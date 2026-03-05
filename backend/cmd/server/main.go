package main

import (
	"context"
	"log"
	"path/filepath"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/ductor/ecommerce_app/backend/internal/config"
	httpapi "github.com/ductor/ecommerce_app/backend/internal/http"
	"github.com/ductor/ecommerce_app/backend/internal/metrics"
	"github.com/ductor/ecommerce_app/backend/internal/migration"
	"github.com/ductor/ecommerce_app/backend/internal/repository"
	"github.com/ductor/ecommerce_app/backend/internal/service"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := config.Load()
	metrics.Init()
	ctx := context.Background()

	db, err := pgxpool.Connect(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db connect failed: %v", err)
	}
	defer db.Close()
	if err := migration.Run(ctx, db, filepath.Join("migrations")); err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	var rdb *redis.Client
	if cfg.RedisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	}

	var nc *nats.Conn
	if cfg.NATSURL != "" {
		if conn, err := nats.Connect(cfg.NATSURL, nats.MaxReconnects(-1)); err == nil {
			nc = conn
			defer nc.Close()
		}
	}

	store := repository.NewStore(db)
	authSvc := service.NewAuthService(store)
	productSvc := service.NewProductService(store, rdb)
	cartSvc := service.NewCartService(store)
	orderSvc := service.NewOrderService(store)
	orderStatsSvc := service.NewOrderStatsService(store)
	replayJobSvc := service.NewReplayJobService(store)
	outboxPublisher := service.NewOutboxPublisher(store, nc)
	go outboxPublisher.Start(ctx, 2*time.Second)
	go replayJobSvc.Start(ctx, 2*time.Second)
	go orderStatsSvc.Start(ctx, 5*time.Second)
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = authSvc.CleanupExpiredAdminSessions(ctx)
			}
		}
	}()

	h := server.Default(server.WithHostPorts(cfg.HTTPAddr))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replayJobSvc, store, outboxPublisher))
	h.Spin()
}

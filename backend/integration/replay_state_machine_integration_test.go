//go:build integration

package integration

import (
	"context"
	"fmt"
	nethttp "net/http"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app/server"
	httpapi "github.com/ductor/ecommerce_app/backend/internal/http"
	"github.com/ductor/ecommerce_app/backend/internal/repository"
	"github.com/ductor/ecommerce_app/backend/internal/service"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

func TestIntegration_ReplayLeaseExpiredRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, cleanup := setupIsolatedPostgres(t, "replay_lease_recovery")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(email,password_hash,name,role) VALUES ($1,$2,$3,$4)`, "admin-lease@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (310, 'ecom.order.created', '{"event":"order.created","order_id":9310}', 'timeout', 3)`); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: getenv("TEST_REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	nc, err := nats.Connect(getenv("TEST_NATS_URL", "nats://localhost:4222"))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()

	store := repository.NewStore(db)
	authSvc := service.NewAuthService(store)
	productSvc := service.NewProductService(store, rdb)
	cartSvc := service.NewCartService(store)
	orderSvc := service.NewOrderService(store)
	replaySvc := service.NewReplayJobService(store)
	publisher := service.NewOutboxPublisher(store, nc)

	port := 23080 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	go replaySvc.Start(ctx, 150*time.Millisecond)
	time.Sleep(250 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-lease@test.com", "password": "admin123"}, map[string]string{})
	token := login["token"].(string)
	create := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{"Authorization": "Bearer " + token, "X-Command-Id": "lease-job-1"})
	jobID := int64(create["job_id"].(float64))

	if _, err := db.Exec(ctx, `
UPDATE replay_jobs
SET status='running',
    lease_owner='stale-worker',
    lease_expires_at=NOW()-INTERVAL '2 minutes',
    updated_at=NOW()-INTERVAL '2 minutes'
WHERE id=$1`, jobID); err != nil {
		t.Fatalf("force stale lease: %v", err)
	}

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		got := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + token})
		status := got["job"].(map[string]any)["status"].(string)
		if status == "completed" {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("job did not recover from expired lease and complete")
}

func TestIntegration_ReplayPartialToCompletedE2E(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, cleanup := setupIsolatedPostgres(t, "replay_partial_completed")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,$1,$2,$3,$4)`, "admin-p2c@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (410, 'ecom.order.created', '{"event":"order.created","order_id":9410}', 'timeout', 3)`); err != nil {
		t.Fatalf("seed dead letter 1: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (411, 'ecom.order.created', '{"event":"order.created","order_id":9411}', 'timeout', 3)`); err != nil {
		t.Fatalf("seed dead letter 2: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: getenv("TEST_REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	nc, err := nats.Connect(getenv("TEST_NATS_URL", "nats://localhost:4222"))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()

	store := repository.NewStore(db)
	authSvc := service.NewAuthService(store)
	productSvc := service.NewProductService(store, rdb)
	cartSvc := service.NewCartService(store)
	orderSvc := service.NewOrderService(store)
	replaySvc := service.NewReplayJobService(store)
	publisher := service.NewOutboxPublisher(store, nc)

	port := 24080 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	go replaySvc.Start(ctx, 150*time.Millisecond)
	time.Sleep(250 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-p2c@test.com", "password": "admin123"}, map[string]string{})
	token := login["token"].(string)
	create := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{
		"Authorization": "Bearer " + token,
		"X-Command-Id":  "p2c-create-1",
		"X-Trace-Id":    "trace-p2c-001",
	})
	jobID := int64(create["job_id"].(float64))

	if _, err := db.Exec(ctx, `
WITH pick AS (
  SELECT id
  FROM replay_job_items
  WHERE job_id=$1
  ORDER BY id
  LIMIT 1
)
UPDATE replay_job_items
SET status='failed', terminal=TRUE, attempts=max_attempts, error_group='timeout', last_error='timeout'
WHERE id IN (SELECT id FROM pick)`, jobID); err != nil {
		t.Fatalf("mark one item failed: %v", err)
	}
	if _, err := db.Exec(ctx, `
UPDATE replay_job_items
SET status='success', terminal=FALSE
WHERE job_id=$1
AND status <> 'failed'`, jobID); err != nil {
		t.Fatalf("mark one item success: %v", err)
	}
	if _, err := db.Exec(ctx, `
UPDATE replay_jobs
SET status='partial', processed_items=2, success_items=1, failed_items=1, last_error='timeout'
WHERE id=$1`, jobID); err != nil {
		t.Fatalf("set partial: %v", err)
	}

	_ = doJSONMap(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, jobID), map[string]any{"error_group": "timeout", "limit": 10}, map[string]string{"Authorization": "Bearer " + token, "X-Command-Id": "p2c-retry-1"})

	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		got := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + token})
		job := got["job"].(map[string]any)
		if job["status"].(string) == "completed" {
			if job["trace_id"].(string) != "trace-p2c-001" {
				t.Fatalf("expected trace_id trace-p2c-001, got %v", job["trace_id"])
			}
			if strings.TrimSpace(job["command_id"].(string)) == "" {
				t.Fatalf("expected non-empty command_id in replay job metadata")
			}
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatalf("job did not transition partial->completed after retry")
}

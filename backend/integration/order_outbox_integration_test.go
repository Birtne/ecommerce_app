//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ductor/ecommerce_app/backend/internal/repository"
	"github.com/ductor/ecommerce_app/backend/internal/service"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
)

func setupIntegration(t *testing.T) (*repository.Store, func(), *nats.Conn, *redis.Client) {
	t.Helper()
	ctx := context.Background()
	raddr := getenv("TEST_REDIS_ADDR", "localhost:6379")
	nurl := getenv("TEST_NATS_URL", "nats://localhost:4222")

	db, cleanup := setupIsolatedPostgres(t, "order_outbox")
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (1,'Phone',299.00,100) ON CONFLICT (id) DO UPDATE SET title=EXCLUDED.title, price=EXCLUDED.price, stock=EXCLUDED.stock`); err != nil {
		t.Fatalf("seed products: %v", err)
	}

	nc, err := nats.Connect(nurl)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: raddr})
	if err := rdb.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("redis flush: %v", err)
	}

	return repository.NewStore(db), cleanup, nc, rdb
}

func createUserAndCart(t *testing.T, store *repository.Store, uid *int64) {
	t.Helper()
	ctx := context.Background()
	id, err := store.CreateUser(ctx, "itest@example.com", "hash_pw", "Itest")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	*uid = id
	if err := store.UpsertCartItem(ctx, id, 1, 2); err != nil {
		t.Fatalf("add cart item: %v", err)
	}
}

func TestIntegration_OrderIdempotencyCreatesSingleOrderAndOutbox(t *testing.T) {
	store, cleanup, nc, rdb := setupIntegration(t)
	defer cleanup()
	defer nc.Close()
	defer rdb.Close()

	var uid int64
	createUserAndCart(t, store, &uid)
	orderSvc := service.NewOrderService(store)

	order1, replay1, err := orderSvc.PlaceOrder(context.Background(), uid, "Shanghai Road", "idem-001")
	if err != nil {
		t.Fatalf("place order first: %v", err)
	}
	if replay1 {
		t.Fatalf("first place order should not be replay")
	}

	order2, replay2, err := orderSvc.PlaceOrder(context.Background(), uid, "Shanghai Road", "idem-001")
	if err != nil {
		t.Fatalf("place order second: %v", err)
	}
	if !replay2 {
		t.Fatalf("second place order should be replay")
	}
	if order1.ID != order2.ID {
		t.Fatalf("expected same order id for idem replay")
	}

	var orderCount int
	if err := store.DB.QueryRow(context.Background(), `SELECT COUNT(*) FROM orders`).Scan(&orderCount); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if orderCount != 1 {
		t.Fatalf("expected 1 order, got %d", orderCount)
	}

	var outboxCount int
	if err := store.DB.QueryRow(context.Background(), `SELECT COUNT(*) FROM outbox_events WHERE status='pending'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected 1 pending outbox event, got %d", outboxCount)
	}
}

func TestIntegration_OutboxPublisherPublishesNATSEvent(t *testing.T) {
	store, cleanup, nc, rdb := setupIntegration(t)
	defer cleanup()
	defer nc.Close()
	defer rdb.Close()

	var uid int64
	createUserAndCart(t, store, &uid)
	orderSvc := service.NewOrderService(store)
	order, _, err := orderSvc.PlaceOrder(context.Background(), uid, "Pudong", "idem-evt-1")
	if err != nil {
		t.Fatalf("place order: %v", err)
	}

	sub, err := nc.SubscribeSync("ecom.order.created")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	publisher := service.NewOutboxPublisher(store, nc)
	if err := publisher.RunOnce(context.Background()); err != nil {
		t.Fatalf("run outbox: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("wait nats message: %v", err)
	}
	if !strings.Contains(string(msg.Data), "order.created") {
		t.Fatalf("unexpected event payload: %s", string(msg.Data))
	}

	var payload []byte
	if err := store.DB.QueryRow(context.Background(), `SELECT payload FROM outbox_events ORDER BY id DESC LIMIT 1`).Scan(&payload); err != nil {
		t.Fatalf("fetch payload: %v", err)
	}
	m := map[string]any{}
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if int64(m["order_id"].(float64)) != order.ID {
		t.Fatalf("event order_id mismatch")
	}

	var status string
	if err := store.DB.QueryRow(context.Background(), `SELECT status FROM outbox_events ORDER BY id DESC LIMIT 1`).Scan(&status); err != nil {
		t.Fatalf("fetch outbox status: %v", err)
	}
	if status != "sent" {
		t.Fatalf("expected outbox status sent, got %s", status)
	}
}

func TestIntegration_ProductListRedisCache(t *testing.T) {
	store, cleanup, nc, rdb := setupIntegration(t)
	defer cleanup()
	defer nc.Close()
	defer rdb.Close()

	productSvc := service.NewProductService(store, rdb)
	ctx := context.Background()

	items1, err := productSvc.ListProducts(ctx)
	if err != nil {
		t.Fatalf("list products first: %v", err)
	}
	if len(items1) == 0 {
		t.Fatalf("expected products from db")
	}

	if _, err := store.DB.Exec(ctx, `DELETE FROM products`); err != nil {
		t.Fatalf("delete products: %v", err)
	}

	items2, err := productSvc.ListProducts(ctx)
	if err != nil {
		t.Fatalf("list products second: %v", err)
	}
	if len(items2) == 0 {
		t.Fatalf("expected products from redis cache")
	}
}

func TestIntegration_OutboxMovesToDeadLetterAfterMaxRetry(t *testing.T) {
	store, cleanup, nc, rdb := setupIntegration(t)
	defer cleanup()
	defer rdb.Close()

	var uid int64
	createUserAndCart(t, store, &uid)
	orderSvc := service.NewOrderService(store)
	if _, _, err := orderSvc.PlaceOrder(context.Background(), uid, "Pudong", "idem-dlq-1"); err != nil {
		t.Fatalf("place order: %v", err)
	}

	if _, err := store.DB.Exec(context.Background(), `UPDATE outbox_events SET max_retries=1 WHERE status='pending'`); err != nil {
		t.Fatalf("update max retries: %v", err)
	}

	nc.Close()
	publisher := service.NewOutboxPublisher(store, nc)
	if err := publisher.RunOnce(context.Background()); err != nil {
		t.Fatalf("run outbox: %v", err)
	}

	var outboxStatus string
	if err := store.DB.QueryRow(context.Background(), `SELECT status FROM outbox_events ORDER BY id DESC LIMIT 1`).Scan(&outboxStatus); err != nil {
		t.Fatalf("query outbox status: %v", err)
	}
	if outboxStatus != "dead_letter" {
		t.Fatalf("expected outbox status dead_letter, got %s", outboxStatus)
	}

	var deadLetterCount int
	if err := store.DB.QueryRow(context.Background(), `SELECT COUNT(*) FROM dead_letter_events`).Scan(&deadLetterCount); err != nil {
		t.Fatalf("count dead letters: %v", err)
	}
	if deadLetterCount != 1 {
		t.Fatalf("expected 1 dead letter row, got %d", deadLetterCount)
	}
}

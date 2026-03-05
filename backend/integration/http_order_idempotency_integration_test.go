//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
)

func TestIntegration_HTTPOrderIdempotencyReplay(t *testing.T) {
	ctx := context.Background()
	raddr := getenv("TEST_REDIS_ADDR", "localhost:6379")
	nurl := getenv("TEST_NATS_URL", "nats://localhost:4222")

	db, cleanup := setupIsolatedPostgres(t, "http_order_idem")
	defer cleanup()
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (1001,'Desk Lamp',129.00,50) ON CONFLICT (id) DO NOTHING`); err != nil {
		t.Fatalf("seed product: %v", err)
	}

	rdb := redis.NewClient(&redis.Options{Addr: raddr})
	defer rdb.Close()
	nc, err := nats.Connect(nurl)
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

	port := 18080 + int(time.Now().UnixNano()%2000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	time.Sleep(300 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	regBody := map[string]any{"email": fmt.Sprintf("u%d@test.com", port), "password": "123456", "name": "itest"}
	regRes := doJSON(t, client, nethttp.MethodPost, base+"/auth/register", regBody, "", "")
	token := regRes["token"].(string)

	_ = doJSON(t, client, nethttp.MethodPost, base+"/cart/items", map[string]any{"product_id": 1001, "quantity": 2}, token, "")

	idemKey := fmt.Sprintf("idem-http-%d", time.Now().UnixNano())
	orderA := doJSON(t, client, nethttp.MethodPost, base+"/orders", map[string]any{"address": "Shanghai Huangpu"}, token, idemKey)
	orderB := doJSON(t, client, nethttp.MethodPost, base+"/orders", map[string]any{"address": "Shanghai Huangpu"}, token, idemKey)

	if orderA["order_id"].(float64) != orderB["order_id"].(float64) {
		t.Fatalf("expected same order id in replay")
	}
	if !orderB["idempotent_replay"].(bool) {
		t.Fatalf("expected second request idempotent_replay=true")
	}

	var orderCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM orders`).Scan(&orderCount); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if orderCount != 1 {
		t.Fatalf("expected one order, got %d", orderCount)
	}

	orderID := int(orderA["order_id"].(float64))
	detail := doJSON(t, client, nethttp.MethodGet, fmt.Sprintf("%s/orders/%d", base, orderID), nil, token, "")
	if int(detail["order_id"].(float64)) != orderID {
		t.Fatalf("order detail id mismatch")
	}
	if detail["status"].(string) == "" {
		t.Fatalf("order detail status should not be empty")
	}

	history := doJSON(t, client, nethttp.MethodGet, base+"/orders?page=1&page_size=10", nil, token, "")
	itemsRaw, ok := history["items"].([]any)
	if !ok {
		t.Fatalf("history contract missing items")
	}
	if len(itemsRaw) != 1 {
		t.Fatalf("expected order history size 1, got %d", len(itemsRaw))
	}
	if int(history["page"].(float64)) != 1 || int(history["page_size"].(float64)) != 10 {
		t.Fatalf("history pagination mismatch: %+v", history)
	}

	outboxHealth := doJSON(t, client, nethttp.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/health/outbox", port), nil, "", "")
	if outboxHealth["db_stats"] == nil || outboxHealth["runtime_stats"] == nil {
		t.Fatalf("outbox health contract mismatch: %+v", outboxHealth)
	}

	metrics := doText(t, client, nethttp.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", port))
	if !strings.Contains(metrics, "ecommerce_outbox_events") {
		t.Fatalf("metrics contract missing ecommerce_outbox_events")
	}
	if !strings.Contains(metrics, "ecommerce_outbox_runtime") {
		t.Fatalf("metrics contract missing ecommerce_outbox_runtime")
	}
}

func doJSON(t *testing.T, client *nethttp.Client, method, url string, payload map[string]any, token, idemKey string) map[string]any {
	t.Helper()
	b, _ := json.Marshal(payload)
	req, err := nethttp.NewRequest(method, url, bytes.NewReader(b))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("request failed %d: %s", resp.StatusCode, string(body))
	}
	m := map[string]any{}
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode json: %v body=%s", err, string(body))
	}
	return m
}

func doText(t *testing.T, client *nethttp.Client, method, url string) string {
	t.Helper()
	req, err := nethttp.NewRequest(method, url, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("request failed %d: %s", resp.StatusCode, string(body))
	}
	return string(body)
}

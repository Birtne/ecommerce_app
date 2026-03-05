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

func TestIntegration_OrderHistoryCursorConsistencyAndExplain(t *testing.T) {
	ctx := context.Background()
	raddr := getenv("TEST_REDIS_ADDR", "localhost:6379")
	nurl := getenv("TEST_NATS_URL", "nats://localhost:4222")

	db, cleanup := setupIsolatedPostgres(t, "order_history")
	defer cleanup()
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (3001,'Cursor Product',88.00,500) ON CONFLICT (id) DO NOTHING`); err != nil {
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

	port := 21080 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	time.Sleep(250 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	reg := doJSONWithHeaders(t, client, nethttp.MethodPost, base+"/auth/register", map[string]any{"email": fmt.Sprintf("cursor-%d@test.com", port), "password": "123456", "name": "cursor"}, map[string]string{})
	token := reg["token"].(string)

	for i := 0; i < 8; i++ {
		doJSONWithHeaders(t, client, nethttp.MethodPost, base+"/cart/items", map[string]any{"product_id": 3001, "quantity": 1}, map[string]string{"Authorization": "Bearer " + token})
		doJSONWithHeaders(t, client, nethttp.MethodPost, base+"/orders", map[string]any{"address": fmt.Sprintf("Address %d", i)}, map[string]string{"Authorization": "Bearer " + token, "Idempotency-Key": fmt.Sprintf("cursor-%d-%d", port, i)})
	}

	first := doJSONWithHeaders(t, client, nethttp.MethodGet, base+"/orders?page_size=3", nil, map[string]string{"Authorization": "Bearer " + token})
	items1 := first["items"].([]any)
	if len(items1) != 3 {
		t.Fatalf("expected first page 3 items, got %d", len(items1))
	}
	cursor, ok := first["next_cursor"].(string)
	if !ok || cursor == "" {
		t.Fatalf("missing next_cursor in first page: %+v", first)
	}

	second := doJSONWithHeaders(t, client, nethttp.MethodGet, base+"/orders?page_size=3&cursor="+cursor, nil, map[string]string{"Authorization": "Bearer " + token})
	items2 := second["items"].([]any)
	if len(items2) == 0 {
		t.Fatalf("expected second page items")
	}

	firstIDs := map[int]bool{}
	for _, raw := range items1 {
		m := raw.(map[string]any)
		firstIDs[int(m["order_id"].(float64))] = true
	}
	for _, raw := range items2 {
		m := raw.(map[string]any)
		id := int(m["order_id"].(float64))
		if firstIDs[id] {
			t.Fatalf("cursor pagination returned duplicated order id: %d", id)
		}
	}

	rows, err := db.Query(ctx, `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id,user_id,address,amount,status,created_at
FROM orders
WHERE user_id=$1
ORDER BY created_at DESC, id DESC
LIMIT 20`, 1)
	if err != nil {
		t.Fatalf("explain analyze: %v", err)
	}
	defer rows.Close()
	planLines := make([]string, 0)
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain line: %v", err)
		}
		planLines = append(planLines, line)
	}
	if len(planLines) == 0 {
		t.Fatalf("empty explain plan")
	}
	t.Logf("EXPLAIN plan:\n%s", strings.Join(planLines, "\n"))
}

func doJSONWithHeaders(t *testing.T, client *nethttp.Client, method, url string, payload map[string]any, headers map[string]string) map[string]any {
	t.Helper()
	var body io.Reader
	if payload != nil {
		b, _ := json.Marshal(payload)
		body = bytes.NewReader(b)
	}
	req, err := nethttp.NewRequest(method, url, body)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do req: %v", err)
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("http %d: %s", resp.StatusCode, string(bodyBytes))
	}
	m := map[string]any{}
	if err := json.Unmarshal(bodyBytes, &m); err != nil {
		t.Fatalf("json decode: %v body=%s", err, string(bodyBytes))
	}
	return m
}

//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"os"
	"path/filepath"
	"sort"
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

type contractSnapshot struct {
	PostOrderKeys            []string `json:"post_order_keys"`
	ListOrdersRootKeys       []string `json:"list_orders_root_keys"`
	OrderDetailKeys          []string `json:"order_detail_keys"`
	OutboxHealthKeys         []string `json:"outbox_health_keys"`
	MetricsRequiredSubstring []string `json:"metrics_required_substrings"`
}

func TestIntegration_APIContractSnapshot(t *testing.T) {
	ctx := context.Background()
	raddr := getenv("TEST_REDIS_ADDR", "localhost:6379")
	nurl := getenv("TEST_NATS_URL", "nats://localhost:4222")

	db, cleanup := setupIsolatedPostgres(t, "api_contract")
	defer cleanup()
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (2001,'Contract Product',88.00,50) ON CONFLICT (id) DO NOTHING`); err != nil {
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

	port := 20080 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	time.Sleep(300 * time.Millisecond)

	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	client := &nethttp.Client{Timeout: 4 * time.Second}

	reg := doJSON(t, client, "POST", base+"/auth/register", map[string]any{"email": fmt.Sprintf("snapshot-%d@test.com", port), "password": "123456", "name": "snapshot"}, "", "")
	token := reg["token"].(string)

	doJSON(t, client, "POST", base+"/cart/items", map[string]any{"product_id": 2001, "quantity": 1}, token, "")
	order := doJSON(t, client, "POST", base+"/orders", map[string]any{"address": "Contract Lane"}, token, fmt.Sprintf("snapshot-%d", port))
	orderID := int(order["order_id"].(float64))
	if err := publisher.RunOnce(ctx); err != nil {
		t.Fatalf("run outbox once: %v", err)
	}
	listResp := doJSON(t, client, "GET", base+"/orders?page=1&page_size=10", nil, token, "")
	detail := doJSON(t, client, "GET", fmt.Sprintf("%s/orders/%d", base, orderID), nil, token, "")
	outboxHealth := doJSON(t, client, "GET", fmt.Sprintf("http://127.0.0.1:%d/health/outbox", port), nil, "", "")
	metrics := doText(t, client, "GET", fmt.Sprintf("http://127.0.0.1:%d/metrics", port))

	snapshotPath := filepath.Join("..", "contract", "api_contract_snapshot.json")
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	var snapshot contractSnapshot
	if err := json.Unmarshal(content, &snapshot); err != nil {
		t.Fatalf("decode snapshot: %v", err)
	}

	assertSameKeys(t, snapshot.PostOrderKeys, mapKeys(order), "post /orders")
	assertSameKeys(t, snapshot.ListOrdersRootKeys, mapKeys(listResp), "get /orders")
	assertSameKeys(t, snapshot.OrderDetailKeys, mapKeys(detail), "get /orders/:id")
	assertSameKeys(t, snapshot.OutboxHealthKeys, mapKeys(outboxHealth), "get /health/outbox")

	for _, needle := range snapshot.MetricsRequiredSubstring {
		if !strings.Contains(metrics, needle) {
			t.Fatalf("metrics missing %s", needle)
		}
	}
}

func mapKeys(v map[string]any) []string {
	keys := make([]string, 0, len(v))
	for k := range v {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func assertSameKeys(t *testing.T, expected, actual []string, scope string) {
	t.Helper()
	sort.Strings(expected)
	sort.Strings(actual)
	if len(expected) != len(actual) {
		t.Fatalf("%s key count mismatch exp=%v act=%v", scope, expected, actual)
	}
	for i := range expected {
		if expected[i] != actual[i] {
			t.Fatalf("%s key mismatch exp=%v act=%v", scope, expected, actual)
		}
	}
}

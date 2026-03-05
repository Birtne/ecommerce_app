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
	"sync"
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

func TestIntegration_AdminReplayJobFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	raddr := getenv("TEST_REDIS_ADDR", "localhost:6379")
	nurl := getenv("TEST_NATS_URL", "nats://localhost:4222")

	db, cleanup := setupIsolatedPostgres(t, "admin_replay")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(email,password_hash,name,role) VALUES ($1,$2,$3,$4)`, "admin@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (100, 'ecom.order.created', '{"event":"order.created","order_id":9001}', 'publish timeout', 6)`); err != nil {
		t.Fatalf("seed dead letter: %v", err)
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

	port := 19080 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	go replaySvc.Start(ctx, 500*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	forbiddenStatus, _ := doJSONStatus(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{})
	if forbiddenStatus != nethttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized without admin session, got %d", forbiddenStatus)
	}

	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin@test.com", "password": "admin123"}, map[string]string{})
	adminToken, ok := login["token"].(string)
	if !ok || adminToken == "" {
		t.Fatalf("admin login token missing: %+v", login)
	}

	createResp := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{"Authorization": "Bearer " + adminToken, "X-Command-Id": "cmd-create-1"})
	jobID := int(createResp["job_id"].(float64))
	if jobID <= 0 {
		t.Fatalf("invalid job id: %+v", createResp)
	}

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		jobResp := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + adminToken})
		jobData := jobResp["job"].(map[string]any)
		status := jobData["status"].(string)
		if status == "completed" || status == "partial" || status == "failed" {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}

	var pendingCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE status='pending' AND topic='ecom.order.created'`).Scan(&pendingCount); err != nil {
		t.Fatalf("count replayed outbox: %v", err)
	}
	if pendingCount != 1 {
		t.Fatalf("expected 1 replayed outbox row, got %d", pendingCount)
	}

	createRespReplay := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{"Authorization": "Bearer " + adminToken, "X-Command-Id": "cmd-create-1"})
	if int(createRespReplay["job_id"].(float64)) != jobID {
		t.Fatalf("idempotent create replay should return same job id")
	}

	auditResp := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/audit-logs?action=replay_job_create&limit=5", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if _, ok := auditResp["items"].([]any); !ok {
		t.Fatalf("expected audit logs items, got %+v", auditResp)
	}
	exportStatus, exportBody := doJSONStatus(t, client, nethttp.MethodGet, base+"/admin/audit-logs/export?action=replay_job_create", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if exportStatus != nethttp.StatusOK {
		t.Fatalf("expected audit export 200, got %d", exportStatus)
	}
	if !strings.Contains(exportBody, "id,actor_user_id,action,target_type,target_id,payload,created_at") {
		t.Fatalf("audit export csv header missing: %s", exportBody)
	}

	var auditCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM audit_logs WHERE action IN ('admin_login','replay_job_create')`).Scan(&auditCount); err != nil {
		t.Fatalf("count audit logs: %v", err)
	}
	if auditCount < 2 {
		t.Fatalf("expected at least 2 audit logs, got %d", auditCount)
	}

	logout := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/logout", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if ok, _ := logout["ok"].(bool); !ok {
		t.Fatalf("expected logout ok, got %+v", logout)
	}
	afterLogoutStatus, _ := doJSONStatus(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if afterLogoutStatus != nethttp.StatusUnauthorized {
		t.Fatalf("expected unauthorized after logout, got %d", afterLogoutStatus)
	}
}

func TestIntegration_AdminTraceCorrelationSearch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, cleanup := setupIsolatedPostgres(t, "admin_trace_search")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(email,password_hash,name,role) VALUES ($1,$2,$3,$4)`, "admin-trace@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (120, 'ecom.order.created', '{"event":"order.created","order_id":9012}', 'publish timeout', 6)`); err != nil {
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

	port := 19180 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	go replaySvc.Start(ctx, 350*time.Millisecond)
	time.Sleep(280 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-trace@test.com", "password": "admin123"}, map[string]string{})
	adminToken := login["token"].(string)

	traceID := "trace-search-001"
	commandID := "cmd-search-001"
	createResp := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{
		"Authorization": "Bearer " + adminToken,
		"X-Command-Id":  commandID,
		"X-Trace-Id":    traceID,
	})
	jobID := int64(createResp["job_id"].(float64))

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE trace_id=$1 AND command_id=$2`, traceID, commandID).Scan(&count); err == nil && count > 0 {
			break
		}
		time.Sleep(220 * time.Millisecond)
	}

	replaySearch := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/replay-jobs?trace_id="+traceID+"&command_id="+commandID+"&limit=10", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(replaySearch["count"].(float64)) < 1 {
		t.Fatalf("expected replay trace search result, got %+v", replaySearch)
	}
	replayItems := replaySearch["items"].([]any)
	firstReplay := replayItems[0].(map[string]any)
	if int64(firstReplay["job_id"].(float64)) != jobID {
		t.Fatalf("trace replay search unexpected job id %+v", firstReplay)
	}

	outboxSearch := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/outbox-events?trace_id="+traceID+"&command_id="+commandID+"&limit=10", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(outboxSearch["count"].(float64)) < 1 {
		t.Fatalf("expected outbox trace search result, got %+v", outboxSearch)
	}
	firstOutbox := outboxSearch["items"].([]any)[0].(map[string]any)
	if firstOutbox["trace_id"].(string) != traceID || firstOutbox["command_id"].(string) != commandID {
		t.Fatalf("trace outbox search mismatch %+v", firstOutbox)
	}

	for i := 0; i < 3; i++ {
		if _, err := db.Exec(ctx, `
INSERT INTO replay_jobs(created_by,status,topic_filter,trace_id,command_id,total_items,processed_items,success_items,failed_items,last_error)
VALUES (1,'completed','ecom.order.created',$1,$2,1,1,1,0,'')`, traceID, commandID); err != nil {
			t.Fatalf("seed replay trace pagination %d: %v", i, err)
		}
		if _, err := db.Exec(ctx, `
INSERT INTO outbox_events(topic,payload,status,retry_count,max_retries,trace_id,command_id,correlation_source,replay_job_id)
VALUES ('ecom.order.created','{}','sent',0,6,$1,$2,'seed',0)`, traceID, commandID); err != nil {
			t.Fatalf("seed outbox trace pagination %d: %v", i, err)
		}
	}

	replayPage1 := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/replay-jobs?trace_id="+traceID+"&command_id="+commandID+"&limit=1", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(replayPage1["count"].(float64)) != 1 {
		t.Fatalf("expected replay page1 count=1, got %+v", replayPage1)
	}
	replayCursor := int64(replayPage1["next_cursor_id"].(float64))
	if replayCursor <= 0 {
		t.Fatalf("expected replay next_cursor_id > 0, got %+v", replayPage1)
	}
	replayPage2 := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/trace/replay-jobs?trace_id=%s&command_id=%s&limit=1&cursor_id=%d", base, traceID, commandID, replayCursor), nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(replayPage2["count"].(float64)) != 1 {
		t.Fatalf("expected replay page2 count=1, got %+v", replayPage2)
	}
	replayFirstID := int64(replayPage1["items"].([]any)[0].(map[string]any)["job_id"].(float64))
	replaySecondID := int64(replayPage2["items"].([]any)[0].(map[string]any)["job_id"].(float64))
	if replaySecondID >= replayFirstID {
		t.Fatalf("expected replay cursor paging to move backward, page1=%d page2=%d", replayFirstID, replaySecondID)
	}

	outboxPage1 := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/outbox-events?trace_id="+traceID+"&command_id="+commandID+"&limit=1", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(outboxPage1["count"].(float64)) != 1 {
		t.Fatalf("expected outbox page1 count=1, got %+v", outboxPage1)
	}
	outboxCursor := int64(outboxPage1["next_cursor_id"].(float64))
	if outboxCursor <= 0 {
		t.Fatalf("expected outbox next_cursor_id > 0, got %+v", outboxPage1)
	}
	outboxPage2 := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/trace/outbox-events?trace_id=%s&command_id=%s&limit=1&cursor_id=%d", base, traceID, commandID, outboxCursor), nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(outboxPage2["count"].(float64)) != 1 {
		t.Fatalf("expected outbox page2 count=1, got %+v", outboxPage2)
	}
	outboxFirstID := int64(outboxPage1["items"].([]any)[0].(map[string]any)["id"].(float64))
	outboxSecondID := int64(outboxPage2["items"].([]any)[0].(map[string]any)["id"].(float64))
	if outboxSecondID >= outboxFirstID {
		t.Fatalf("expected outbox cursor paging to move backward, page1=%d page2=%d", outboxFirstID, outboxSecondID)
	}

	invalidReplayCursorStatus, _ := doJSONStatus(t, client, nethttp.MethodGet, base+"/admin/trace/replay-jobs?trace_id="+traceID+"&cursor_id=bad", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if invalidReplayCursorStatus != nethttp.StatusBadRequest {
		t.Fatalf("expected replay invalid cursor status 400, got %d", invalidReplayCursorStatus)
	}
	invalidOutboxCursorStatus, _ := doJSONStatus(t, client, nethttp.MethodGet, base+"/admin/trace/outbox-events?trace_id="+traceID+"&cursor_id=oops", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if invalidOutboxCursorStatus != nethttp.StatusBadRequest {
		t.Fatalf("expected outbox invalid cursor status 400, got %d", invalidOutboxCursorStatus)
	}

	replayBoundary := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/trace/replay-jobs?trace_id=%s&command_id=%s&limit=2&cursor_id=1", base, traceID, commandID), nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(replayBoundary["count"].(float64)) != 0 || int64(replayBoundary["next_cursor_id"].(float64)) != 0 {
		t.Fatalf("expected replay boundary empty result with zero cursor, got %+v", replayBoundary)
	}
	outboxBoundary := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/trace/outbox-events?trace_id=%s&command_id=%s&limit=2&cursor_id=1", base, traceID, commandID), nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(outboxBoundary["count"].(float64)) != 0 || int64(outboxBoundary["next_cursor_id"].(float64)) != 0 {
		t.Fatalf("expected outbox boundary empty result with zero cursor, got %+v", outboxBoundary)
	}

	emptyReplay := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/replay-jobs?trace_id=trace-not-exists&command_id=cmd-not-exists&limit=5", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(emptyReplay["count"].(float64)) != 0 || len(emptyReplay["items"].([]any)) != 0 || int64(emptyReplay["next_cursor_id"].(float64)) != 0 {
		t.Fatalf("expected empty replay result consistency, got %+v", emptyReplay)
	}
	emptyOutbox := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/outbox-events?trace_id=trace-not-exists&command_id=cmd-not-exists&limit=5", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if int(emptyOutbox["count"].(float64)) != 0 || len(emptyOutbox["items"].([]any)) != 0 || int64(emptyOutbox["next_cursor_id"].(float64)) != 0 {
		t.Fatalf("expected empty outbox result consistency, got %+v", emptyOutbox)
	}
}

func TestIntegration_AdminTraceSearchHighConcurrency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, cleanup := setupIsolatedPostgres(t, "admin_trace_concurrency")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(email,password_hash,name,role) VALUES ($1,$2,$3,$4)`, "admin-trace-concurrency@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	for i := 0; i < 64; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES ($1, 'ecom.order.created', $2, 'publish timeout', 6)`, 8000+i, fmt.Sprintf(`{"event":"order.created","order_id":%d}`, 91000+i)); err != nil {
			t.Fatalf("seed dead letter %d: %v", i, err)
		}
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

	port := 19580 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	go replaySvc.Start(ctx, 200*time.Millisecond)
	time.Sleep(300 * time.Millisecond)

	client := &nethttp.Client{Timeout: 5 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-trace-concurrency@test.com", "password": "admin123"}, map[string]string{})
	adminToken := login["token"].(string)

	traceID := "trace-concurrency-001"
	commandID := "cmd-concurrency-001"
	for i := 0; i < 20; i++ {
		_ = doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 2}, map[string]string{
			"Authorization": "Bearer " + adminToken,
			"X-Command-Id":  fmt.Sprintf("%s-%d", commandID, i),
			"X-Trace-Id":    traceID,
		})
	}

	var searchWG sync.WaitGroup
	errCh := make(chan error, 80)
	for i := 0; i < 40; i++ {
		searchWG.Add(2)
		go func() {
			defer searchWG.Done()
			res := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/replay-jobs?trace_id="+traceID+"&limit=5", nil, map[string]string{"Authorization": "Bearer " + adminToken})
			items, ok := res["items"].([]any)
			if !ok {
				errCh <- fmt.Errorf("replay items type mismatch: %+v", res)
				return
			}
			if len(items) > 5 {
				errCh <- fmt.Errorf("replay items exceeds limit: %d", len(items))
				return
			}
			prev := int64(1 << 62)
			for _, it := range items {
				row := it.(map[string]any)
				id := int64(row["job_id"].(float64))
				if id >= prev {
					errCh <- fmt.Errorf("replay ids not descending: prev=%d current=%d", prev, id)
					return
				}
				prev = id
			}
		}()
		go func() {
			defer searchWG.Done()
			res := doJSONMap(t, client, nethttp.MethodGet, base+"/admin/trace/outbox-events?trace_id="+traceID+"&limit=5", nil, map[string]string{"Authorization": "Bearer " + adminToken})
			items, ok := res["items"].([]any)
			if !ok {
				errCh <- fmt.Errorf("outbox items type mismatch: %+v", res)
				return
			}
			if len(items) > 5 {
				errCh <- fmt.Errorf("outbox items exceeds limit: %d", len(items))
				return
			}
			prev := int64(1 << 62)
			for _, it := range items {
				row := it.(map[string]any)
				id := int64(row["id"].(float64))
				if id >= prev {
					errCh <- fmt.Errorf("outbox ids not descending: prev=%d current=%d", prev, id)
					return
				}
				prev = id
			}
		}()
	}
	searchWG.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegration_AdminTraceCursorPaginationSoak(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(t, "admin_trace_cursor_soak")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(email,password_hash,name,role) VALUES ($1,$2,$3,$4)`, "admin-trace-soak@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}

	traceID := "trace-cursor-soak-001"
	commandID := "cmd-cursor-soak-001"
	for i := 0; i < 140; i++ {
		if _, err := db.Exec(ctx, `
INSERT INTO replay_jobs(created_by,status,topic_filter,trace_id,command_id,total_items,processed_items,success_items,failed_items,last_error)
VALUES (1,'completed','ecom.order.created',$1,$2,1,1,1,0,'')`, traceID, commandID); err != nil {
			t.Fatalf("seed replay job %d: %v", i, err)
		}
		if _, err := db.Exec(ctx, `
INSERT INTO outbox_events(topic,payload,status,retry_count,max_retries,trace_id,command_id,correlation_source,replay_job_id)
VALUES ('ecom.order.created','{}','sent',0,6,$1,$2,'seed',0)`, traceID, commandID); err != nil {
			t.Fatalf("seed outbox event %d: %v", i, err)
		}
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

	port := 19780 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	time.Sleep(260 * time.Millisecond)

	client := &nethttp.Client{Timeout: 5 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-trace-soak@test.com", "password": "admin123"}, map[string]string{})
	adminToken := login["token"].(string)

	collectReplay := func() error {
		cursor := int64(0)
		seen := map[int64]bool{}
		lastID := int64(1 << 62)
		for page := 0; page < 60; page++ {
			url := fmt.Sprintf("%s/admin/trace/replay-jobs?trace_id=%s&command_id=%s&limit=7", base, traceID, commandID)
			if cursor > 0 {
				url = fmt.Sprintf("%s&cursor_id=%d", url, cursor)
			}
			res := doJSONMap(t, client, nethttp.MethodGet, url, nil, map[string]string{"Authorization": "Bearer " + adminToken})
			items := res["items"].([]any)
			if len(items) == 0 {
				return nil
			}
			for _, it := range items {
				row := it.(map[string]any)
				id := int64(row["job_id"].(float64))
				if seen[id] {
					return fmt.Errorf("replay duplicate across cursor pages id=%d", id)
				}
				if id >= lastID {
					return fmt.Errorf("replay non-desc order prev=%d current=%d", lastID, id)
				}
				seen[id] = true
				lastID = id
			}
			next := int64(res["next_cursor_id"].(float64))
			if next == 0 {
				return nil
			}
			if next >= cursor && cursor > 0 {
				return fmt.Errorf("replay cursor did not move backward prev=%d next=%d", cursor, next)
			}
			cursor = next
		}
		return nil
	}
	collectOutbox := func() error {
		cursor := int64(0)
		seen := map[int64]bool{}
		lastID := int64(1 << 62)
		for page := 0; page < 60; page++ {
			url := fmt.Sprintf("%s/admin/trace/outbox-events?trace_id=%s&command_id=%s&limit=7", base, traceID, commandID)
			if cursor > 0 {
				url = fmt.Sprintf("%s&cursor_id=%d", url, cursor)
			}
			res := doJSONMap(t, client, nethttp.MethodGet, url, nil, map[string]string{"Authorization": "Bearer " + adminToken})
			items := res["items"].([]any)
			if len(items) == 0 {
				return nil
			}
			for _, it := range items {
				row := it.(map[string]any)
				id := int64(row["id"].(float64))
				if seen[id] {
					return fmt.Errorf("outbox duplicate across cursor pages id=%d", id)
				}
				if id >= lastID {
					return fmt.Errorf("outbox non-desc order prev=%d current=%d", lastID, id)
				}
				seen[id] = true
				lastID = id
			}
			next := int64(res["next_cursor_id"].(float64))
			if next == 0 {
				return nil
			}
			if next >= cursor && cursor > 0 {
				return fmt.Errorf("outbox cursor did not move backward prev=%d next=%d", cursor, next)
			}
			cursor = next
		}
		return nil
	}

	deadline := time.Now().Add(4 * time.Second)
	var wg sync.WaitGroup
	errCh := make(chan error, 64)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if err := collectReplay(); err != nil {
					errCh <- err
					return
				}
				if err := collectOutbox(); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func doJSONStatus(t *testing.T, client *nethttp.Client, method, url string, payload map[string]any, headers map[string]string) (int, string) {
	t.Helper()
	var bodyReader io.Reader
	if payload != nil {
		b, _ := json.Marshal(payload)
		bodyReader = bytes.NewReader(b)
	}
	req, err := nethttp.NewRequest(method, url, bodyReader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func doJSONMap(t *testing.T, client *nethttp.Client, method, url string, payload map[string]any, headers map[string]string) map[string]any {
	t.Helper()
	status, body := doJSONStatus(t, client, method, url, payload, headers)
	if status >= 300 {
		t.Fatalf("request failed %d: %s", status, body)
	}
	m := map[string]any{}
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("decode body: %v body=%s", err, body)
	}
	return m
}

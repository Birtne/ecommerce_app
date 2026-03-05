//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	nethttp "net/http"
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

func TestIntegration_AdminReplayFailurePaths(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(t, "admin_failures")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(email,password_hash,name,role) VALUES ($1,$2,$3,$4)`, "admin-f@test.com", string(hash), "Admin", "admin"); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (201, 'ecom.order.created', '{"event":"order.created","order_id":9201}', 'publish timeout', 3)`); err != nil {
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

	port := 22080 + int(time.Now().UnixNano()%1000)
	h := server.Default(server.WithHostPorts(fmt.Sprintf("127.0.0.1:%d", port)))
	httpapi.RegisterRoutes(h, httpapi.NewHandler(authSvc, productSvc, cartSvc, orderSvc, replaySvc, store, publisher))
	go h.Spin()
	time.Sleep(250 * time.Millisecond)

	client := &nethttp.Client{Timeout: 4 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)

	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-f@test.com", "password": "admin123"}, map[string]string{})
	adminToken := login["token"].(string)

	stMissingCmd, _ := doJSONStatus(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{"Authorization": "Bearer " + adminToken})
	if stMissingCmd != nethttp.StatusBadRequest {
		t.Fatalf("expected 400 for missing X-Command-Id, got %d", stMissingCmd)
	}

	firstBody := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{"Authorization": "Bearer " + adminToken, "X-Command-Id": "dup-create-1"})
	firstJobID := int64(firstBody["job_id"].(float64))
	dupStatus, dupRaw := doJSONStatus(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"limit": 10}, map[string]string{"Authorization": "Bearer " + adminToken, "X-Command-Id": "dup-create-1"})
	if dupStatus != nethttp.StatusOK {
		t.Fatalf("expected duplicate command id replay 200, got %d", dupStatus)
	}
	dupParsed := map[string]any{}
	if err := json.Unmarshal([]byte(dupRaw), &dupParsed); err != nil {
		t.Fatalf("decode duplicate body: %v body=%s", err, dupRaw)
	}
	if int64(dupParsed["job_id"].(float64)) != firstJobID {
		t.Fatalf("duplicate command id should keep same job_id")
	}

	_ = doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/logout", nil, map[string]string{"Authorization": "Bearer " + adminToken})
	afterLogoutStatus, _ := doJSONStatus(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, firstJobID), nil, map[string]string{"Authorization": "Bearer " + adminToken})
	if afterLogoutStatus != nethttp.StatusUnauthorized {
		t.Fatalf("expected 401 for blacklisted token, got %d", afterLogoutStatus)
	}

	login2 := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-f@test.com", "password": "admin123"}, map[string]string{})
	adminToken2 := login2["token"].(string)
	jobID := firstJobID

	if _, err := db.Exec(ctx, `
UPDATE replay_job_items
SET status='failed', attempts=max_attempts, last_error='publish timeout', error_group='publish timeout', terminal=TRUE
WHERE job_id=$1`, jobID); err != nil {
		t.Fatalf("mark failed item: %v", err)
	}
	if _, err := db.Exec(ctx, `
UPDATE replay_jobs
SET status='failed', processed_items=1, failed_items=1, success_items=0, last_error='publish timeout'
WHERE id=$1`, jobID); err != nil {
		t.Fatalf("mark failed job: %v", err)
	}

	getFailed := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + adminToken2})
	failedJob := getFailed["job"].(map[string]any)
	if failedJob["status"].(string) != "failed" {
		t.Fatalf("expected failed status, got %+v", failedJob)
	}

	retry := doJSONMap(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, jobID), map[string]any{"error_group": "publish timeout", "limit": 10}, map[string]string{"Authorization": "Bearer " + adminToken2, "X-Command-Id": "retry-failed-1"})
	if int64(retry["retried"].(float64)) != 1 {
		t.Fatalf("expected retried=1, got %+v", retry)
	}
	if int64(retry["attempts_before_total"].(float64)) < 1 || int64(retry["attempts_after_total"].(float64)) != 0 {
		t.Fatalf("expected retry response attempts totals before>0 after=0, got %+v", retry)
	}
	grpBefore := retry["error_groups_before"].(map[string]any)
	if int64(grpBefore["publish timeout"].(float64)) != 1 {
		t.Fatalf("expected error_groups_before publish timeout=1, got %+v", grpBefore)
	}
	grpAfter := retry["error_groups_after"].(map[string]any)
	if int64(grpAfter[""].(float64)) != 1 {
		t.Fatalf("expected error_groups_after empty-group=1, got %+v", grpAfter)
	}
	var attempts int32
	var terminal bool
	if err := db.QueryRow(ctx, `SELECT attempts, terminal FROM replay_job_items WHERE job_id=$1 LIMIT 1`, jobID).Scan(&attempts, &terminal); err != nil {
		t.Fatalf("query replay item after retry: %v", err)
	}
	if attempts != 0 || terminal {
		t.Fatalf("expected default reset_attempts to clear attempts and terminal, got attempts=%d terminal=%v", attempts, terminal)
	}

	getQueued := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + adminToken2})
	if getQueued["job"].(map[string]any)["status"].(string) != "queued" {
		t.Fatalf("expected queued status after retry_failed")
	}

	if _, err := db.Exec(ctx, `UPDATE replay_jobs SET status='partial', processed_items=2, success_items=1, failed_items=1 WHERE id=$1`, jobID); err != nil {
		t.Fatalf("mark partial job: %v", err)
	}
	getPartial := doJSONMap(t, client, nethttp.MethodGet, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d", base, jobID), nil, map[string]string{"Authorization": "Bearer " + adminToken2})
	if getPartial["job"].(map[string]any)["status"].(string) != "partial" {
		t.Fatalf("expected partial status")
	}

	if _, err := db.Exec(ctx, `
UPDATE replay_job_items
SET status='failed', attempts=2, max_attempts=5, last_error='publish timeout', error_group='publish timeout', terminal=TRUE
WHERE job_id=$1`, jobID); err != nil {
		t.Fatalf("reseed failed item for non-reset retry: %v", err)
	}
	retryNoReset := doJSONMap(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, jobID), map[string]any{
		"error_group":    "publish timeout",
		"limit":          10,
		"reset_attempts": false,
	}, map[string]string{"Authorization": "Bearer " + adminToken2, "X-Command-Id": "retry-failed-2"})
	if int64(retryNoReset["retried"].(float64)) != 1 {
		t.Fatalf("expected retried=1 for reset_attempts=false, got %+v", retryNoReset)
	}
	if int64(retryNoReset["attempts_before_total"].(float64)) != 2 || int64(retryNoReset["attempts_after_total"].(float64)) != 2 {
		t.Fatalf("expected reset_attempts=false keep attempts totals, got %+v", retryNoReset)
	}
	noResetAfter := retryNoReset["error_groups_after"].(map[string]any)
	if int64(noResetAfter["publish timeout"].(float64)) != 1 {
		t.Fatalf("expected reset_attempts=false keep failure group, got %+v", noResetAfter)
	}
	if err := db.QueryRow(ctx, `SELECT attempts, terminal FROM replay_job_items WHERE job_id=$1 LIMIT 1`, jobID).Scan(&attempts, &terminal); err != nil {
		t.Fatalf("query replay item after non-reset retry: %v", err)
	}
	if attempts != 2 || terminal {
		t.Fatalf("expected reset_attempts=false to keep attempts=2 and set terminal=false, got attempts=%d terminal=%v", attempts, terminal)
	}

	var auditPayload []byte
	if err := db.QueryRow(ctx, `
SELECT payload
FROM audit_logs
WHERE action='replay_job_retry_failed' AND target_id=$1
ORDER BY id DESC
LIMIT 1`, fmt.Sprintf("%d", jobID)).Scan(&auditPayload); err != nil {
		t.Fatalf("query retry_failed audit payload: %v", err)
	}
	auditObj := map[string]any{}
	if err := json.Unmarshal(auditPayload, &auditObj); err != nil {
		t.Fatalf("decode retry_failed audit payload: %v payload=%s", err, string(auditPayload))
	}
	if int64(auditObj["attempts_before_total"].(float64)) != 2 || int64(auditObj["attempts_after_total"].(float64)) != 2 {
		t.Fatalf("audit payload attempts totals mismatch: %+v", auditObj)
	}

	retryNoResetReplayStatus, retryNoResetReplayRaw := doJSONStatus(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, jobID), map[string]any{
		"error_group":    "publish timeout",
		"limit":          10,
		"reset_attempts": false,
	}, map[string]string{"Authorization": "Bearer " + adminToken2, "X-Command-Id": "retry-failed-2"})
	if retryNoResetReplayStatus != nethttp.StatusOK {
		t.Fatalf("expected duplicate retry-failed command replay 200, got %d body=%s", retryNoResetReplayStatus, retryNoResetReplayRaw)
	}
	retryNoResetReplay := map[string]any{}
	if err := json.Unmarshal([]byte(retryNoResetReplayRaw), &retryNoResetReplay); err != nil {
		t.Fatalf("decode retry-failed replay body: %v body=%s", err, retryNoResetReplayRaw)
	}
	if int64(retryNoResetReplay["retried"].(float64)) != int64(retryNoReset["retried"].(float64)) {
		t.Fatalf("duplicate retry-failed replay should keep retried summary, first=%+v replay=%+v", retryNoReset, retryNoResetReplay)
	}
	if int64(retryNoResetReplay["attempts_before_total"].(float64)) != int64(retryNoReset["attempts_before_total"].(float64)) ||
		int64(retryNoResetReplay["attempts_after_total"].(float64)) != int64(retryNoReset["attempts_after_total"].(float64)) {
		t.Fatalf("duplicate retry-failed replay should keep attempts summary, first=%+v replay=%+v", retryNoReset, retryNoResetReplay)
	}
	replayAfterGroups := retryNoResetReplay["error_groups_after"].(map[string]any)
	originAfterGroups := retryNoReset["error_groups_after"].(map[string]any)
	if int64(replayAfterGroups["publish timeout"].(float64)) != int64(originAfterGroups["publish timeout"].(float64)) {
		t.Fatalf("duplicate retry-failed replay should keep error group summary, first=%+v replay=%+v", originAfterGroups, replayAfterGroups)
	}

	if _, err := db.Exec(ctx, `
UPDATE replay_job_items
SET status='failed', attempts=4, max_attempts=6, last_error='publish timeout', error_group='publish timeout', terminal=TRUE
WHERE job_id=$1`, jobID); err != nil {
		t.Fatalf("reseed failed item for concurrent retry replay: %v", err)
	}
	concurrencyPayload := map[string]any{
		"error_group":    "publish timeout",
		"limit":          10,
		"reset_attempts": true,
	}
	const workers = 6
	var wg sync.WaitGroup
	type retryResult struct {
		status int
		body   map[string]any
		raw    string
	}
	results := make([]retryResult, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			st, raw := doJSONStatus(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, jobID), concurrencyPayload, map[string]string{
				"Authorization": "Bearer " + adminToken2,
				"X-Command-Id":  "retry-failed-concurrent-1",
			})
			results[idx] = retryResult{status: st, raw: raw}
			if st < 300 {
				body := map[string]any{}
				if err := json.Unmarshal([]byte(raw), &body); err == nil {
					results[idx].body = body
				}
			}
		}(i)
	}
	wg.Wait()
	var golden map[string]any
	for i, res := range results {
		if res.status != nethttp.StatusOK {
			t.Fatalf("expected concurrent replay status=200 at idx=%d got=%d raw=%s", i, res.status, res.raw)
		}
		if res.body == nil {
			t.Fatalf("expected decodeable response body at idx=%d raw=%s", i, res.raw)
		}
		if golden == nil {
			golden = res.body
			continue
		}
		if int64(res.body["retried"].(float64)) != int64(golden["retried"].(float64)) ||
			int64(res.body["attempts_before_total"].(float64)) != int64(golden["attempts_before_total"].(float64)) ||
			int64(res.body["attempts_after_total"].(float64)) != int64(golden["attempts_after_total"].(float64)) {
			t.Fatalf("expected consistent concurrent retry summary, golden=%+v current=%+v", golden, res.body)
		}
	}
}

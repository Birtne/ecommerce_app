//go:build integration

package integration

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	nethttp "net/http"
	"os"
	"path/filepath"
	"sort"
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

func TestIntegration_ReplayMultiJobCommandConflictSoak(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(t, "replay_admin_conflict")
	defer cleanup()

	hash, err := bcrypt.GenerateFromPassword([]byte("admin123"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatalf("bcrypt: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,$1,$2,'Admin','admin')`, "admin-conflict@test.com", string(hash)); err != nil {
		t.Fatalf("seed admin: %v", err)
	}

	store := repository.NewStore(db)
	seedOutboxID := 50000
	for i := 0; i < 180; i++ {
		payload := []byte(fmt.Sprintf(`{"event":"order.created","order_id":%d}`, 300000+i))
		if _, err := db.Exec(ctx, `
INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count)
VALUES ($1, 'ecom.order.created', $2, 'seed', 2)`, seedOutboxID, payload); err != nil {
			t.Fatalf("seed dead letter: %v", err)
		}
		seedOutboxID++
	}

	rdb := redis.NewClient(&redis.Options{Addr: getenv("TEST_REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	nc, err := nats.Connect(getenv("TEST_NATS_URL", "nats://localhost:4222"))
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	defer nc.Close()

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
	time.Sleep(250 * time.Millisecond)

	client := &nethttp.Client{Timeout: 6 * time.Second}
	base := fmt.Sprintf("http://127.0.0.1:%d/api/v1", port)
	login := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/auth/login", map[string]any{"email": "admin-conflict@test.com", "password": "admin123"}, map[string]string{})
	token := login["token"].(string)

	commandIDs := []string{"multi-cmd-1", "multi-cmd-2", "multi-cmd-3"}
	latencySamples := make([]time.Duration, 0, 128)
	jobByCmd := map[string]int64{}
	var mu sync.Mutex

	type reqResult struct {
		cmd      string
		jobID    int64
		duration time.Duration
	}
	results := make(chan reqResult, len(commandIDs)*5)
	var wg sync.WaitGroup
	for _, cmdID := range commandIDs {
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(commandID string) {
				defer wg.Done()
				start := time.Now()
				resp := doJSONMap(t, client, nethttp.MethodPost, base+"/admin/outbox/replay-jobs", map[string]any{"topic": "ecom.order.created", "limit": 80}, map[string]string{"Authorization": "Bearer " + token, "X-Command-Id": commandID})
				rawJobID, ok := resp["job_id"].(float64)
				if !ok {
					t.Fatalf("replay job response missing job_id, cmd=%s resp=%+v", commandID, resp)
				}
				results <- reqResult{cmd: commandID, jobID: int64(rawJobID), duration: time.Since(start)}
			}(cmdID)
		}
	}
	wg.Wait()
	close(results)

	for r := range results {
		mu.Lock()
		if existing, ok := jobByCmd[r.cmd]; ok {
			if existing != r.jobID {
				mu.Unlock()
				t.Fatalf("command-id conflict should return same job id cmd=%s existing=%d got=%d", r.cmd, existing, r.jobID)
			}
		} else {
			jobByCmd[r.cmd] = r.jobID
		}
		latencySamples = append(latencySamples, r.duration)
		mu.Unlock()
	}
	if len(jobByCmd) != len(commandIDs) {
		t.Fatalf("expected %d unique jobs by command id, got %d", len(commandIDs), len(jobByCmd))
	}

	jobIDs := make([]int64, 0, len(jobByCmd))
	for _, cmd := range commandIDs {
		jobID := jobByCmd[cmd]
		jobIDs = append(jobIDs, jobID)
	}

	// Seed deterministic failure groups across jobs for trend observation.
	failureGroups := []string{"seed.timeout", "seed.cancel", "seed.partial"}
	for i, jobID := range jobIDs {
		rows, err := db.Query(ctx, `SELECT dead_letter_id FROM replay_job_items WHERE job_id=$1 ORDER BY id LIMIT 4`, jobID)
		if err != nil {
			t.Fatalf("query seed failures for job %d: %v", jobID, err)
		}
		targets := make([]int64, 0, 4)
		for rows.Next() {
			var deadID int64
			if err := rows.Scan(&deadID); err != nil {
				rows.Close()
				t.Fatalf("scan dead letter id: %v", err)
			}
			targets = append(targets, deadID)
		}
		rows.Close()
		for j, deadID := range targets {
			group := failureGroups[(i+j)%len(failureGroups)]
			if _, err := db.Exec(ctx, `UPDATE replay_job_items SET max_attempts=1 WHERE job_id=$1 AND dead_letter_id=$2`, jobID, deadID); err != nil {
				t.Fatalf("set replay max_attempts for seed failure job=%d dead=%d: %v", jobID, deadID, err)
			}
			if err := store.MarkReplayItemFailed(ctx, jobID, deadID, group); err != nil {
				t.Fatalf("seed mark failed job=%d dead=%d: %v", jobID, deadID, err)
			}
		}
	}

	retryTarget := jobIDs[0]
	var retryWG sync.WaitGroup
	retryResults := make(chan map[string]any, 4)
	for i := 0; i < 4; i++ {
		retryWG.Add(1)
		go func() {
			defer retryWG.Done()
			resp := doJSONMap(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, retryTarget), map[string]any{"error_group": "seed.timeout", "limit": 20}, map[string]string{"Authorization": "Bearer " + token, "X-Command-Id": "retry-conflict-1"})
			retryResults <- resp
		}()
	}
	retryWG.Wait()
	close(retryResults)
	firstRetried := int64(-1)
	for r := range retryResults {
		cur := int64(r["retried"].(float64))
		if firstRetried == -1 {
			firstRetried = cur
			continue
		}
		if firstRetried != cur {
			t.Fatalf("retry command-id conflict mismatch retried=%d/%d", firstRetried, cur)
		}
	}

	type mixedRetryCase struct {
		jobID       int64
		errorGroup  string
		commandID   string
		reset       bool
		expectedMin int64
	}
	mixedCases := []mixedRetryCase{
		{jobID: jobIDs[0], errorGroup: "seed.timeout", commandID: "mixed-retry-cmd-1", reset: true, expectedMin: 1},
		{jobID: jobIDs[1], errorGroup: "seed.cancel", commandID: "mixed-retry-cmd-2", reset: false, expectedMin: 1},
		{jobID: jobIDs[2], errorGroup: "seed.partial", commandID: "mixed-retry-cmd-3", reset: true, expectedMin: 1},
	}
	for _, tc := range mixedCases {
		tag, err := db.Exec(ctx, `
WITH candidates AS (
	SELECT id
	FROM replay_job_items
	WHERE job_id=$1
	ORDER BY id
	LIMIT 3
)
UPDATE replay_job_items
SET status='failed',
    terminal=TRUE,
    attempts=2,
    max_attempts=5,
    error_group=$2,
    last_error=$2,
    updated_at=NOW()
WHERE id IN (SELECT id FROM candidates)`, tc.jobID, tc.errorGroup)
		if err != nil {
			t.Fatalf("seed mixed retry failed rows job=%d group=%s: %v", tc.jobID, tc.errorGroup, err)
		}
		if tag.RowsAffected() == 0 {
			t.Fatalf("seed mixed retry failed rows affected=0 job=%d group=%s", tc.jobID, tc.errorGroup)
		}

		var wgMixed sync.WaitGroup
		type mixedResp struct {
			status int
			raw    string
			body   map[string]any
		}
		responses := make([]mixedResp, 0, 4)
		var respMu sync.Mutex
		for i := 0; i < 4; i++ {
			wgMixed.Add(1)
			go func(c mixedRetryCase) {
				defer wgMixed.Done()
				status, raw := doJSONStatus(t, client, nethttp.MethodPost, fmt.Sprintf("%s/admin/outbox/replay-jobs/%d/retry-failed", base, c.jobID), map[string]any{
					"error_group":    c.errorGroup,
					"limit":          20,
					"reset_attempts": c.reset,
				}, map[string]string{"Authorization": "Bearer " + token, "X-Command-Id": c.commandID})
				body := map[string]any{}
				if status < 300 {
					_ = json.Unmarshal([]byte(raw), &body)
				}
				respMu.Lock()
				responses = append(responses, mixedResp{status: status, raw: raw, body: body})
				respMu.Unlock()
			}(tc)
		}
		wgMixed.Wait()
		if len(responses) != 4 {
			t.Fatalf("expected 4 mixed conflict responses, got %d", len(responses))
		}
		var golden map[string]any
		for _, res := range responses {
			if res.status != nethttp.StatusOK {
				t.Fatalf("mixed conflict expected 200 command=%s got=%d raw=%s", tc.commandID, res.status, res.raw)
			}
			if golden == nil {
				golden = res.body
				continue
			}
			if int64(res.body["retried"].(float64)) != int64(golden["retried"].(float64)) ||
				int64(res.body["attempts_before_total"].(float64)) != int64(golden["attempts_before_total"].(float64)) ||
				int64(res.body["attempts_after_total"].(float64)) != int64(golden["attempts_after_total"].(float64)) {
				t.Fatalf("mixed conflict summary mismatch command=%s golden=%+v current=%+v", tc.commandID, golden, res.body)
			}
		}
		if int64(golden["retried"].(float64)) < tc.expectedMin {
			t.Fatalf("mixed conflict expected retried >= %d command=%s got=%+v", tc.expectedMin, tc.commandID, golden)
		}
	}

	workers := []*service.ReplayJobService{
		service.NewReplayJobService(store),
		service.NewReplayJobService(store),
		service.NewReplayJobService(store),
		service.NewReplayJobService(store),
	}
	soakDeadline := time.Now().Add(4 * time.Second)
	processLatency := make([]time.Duration, 0, 1024)
	failureTrend := make([]map[string]int32, 0, 8)
	var processMu sync.Mutex

	wg = sync.WaitGroup{}
	for _, worker := range workers {
		wg.Add(1)
		go func(svc *service.ReplayJobService) {
			defer wg.Done()
			for time.Now().Before(soakDeadline) {
				start := time.Now()
				_ = svc.ProcessOnce(ctx)
				cost := time.Since(start)
				processMu.Lock()
				processLatency = append(processLatency, cost)
				processMu.Unlock()
				time.Sleep(15 * time.Millisecond)
			}
		}(worker)
	}

	for time.Now().Before(soakDeadline) {
		rows, err := db.Query(ctx, `
SELECT error_group, COUNT(*)::int
FROM replay_job_items
WHERE job_id = ANY($1) AND status='failed' AND error_group <> ''
GROUP BY error_group
ORDER BY error_group`, jobIDs)
		if err != nil {
			t.Fatalf("query failure trend: %v", err)
		}
		snap := map[string]int32{}
		for rows.Next() {
			var group string
			var c int32
			if err := rows.Scan(&group, &c); err != nil {
				rows.Close()
				t.Fatalf("scan failure trend row: %v", err)
			}
			snap[group] = c
		}
		rows.Close()
		failureTrend = append(failureTrend, snap)
		time.Sleep(700 * time.Millisecond)
	}
	wg.Wait()

	for _, jobID := range jobIDs {
		job, _, err := store.GetReplayJob(ctx, jobID)
		if err != nil {
			t.Fatalf("get replay job %d: %v", jobID, err)
		}
		if !strings.Contains("completed partial failed", job.Status) {
			t.Fatalf("unexpected replay job status for %d: %s", jobID, job.Status)
		}
	}

	var commandRows int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM admin_command_idempotency WHERE action='replay_job:create' AND command_id = ANY($1)`, commandIDs).Scan(&commandRows); err != nil {
		t.Fatalf("count command idempotency rows: %v", err)
	}
	if commandRows != len(commandIDs) {
		t.Fatalf("expected %d command idempotency rows, got %d", len(commandIDs), commandRows)
	}

	p95Request, p99Request := durationQuantiles(latencySamples)
	p95Process, p99Process := durationQuantiles(processLatency)
	t.Logf("replay admin conflict metrics: create_count=%d req_p95_ms=%.2f req_p99_ms=%.2f process_p95_ms=%.2f process_p99_ms=%.2f",
		len(latencySamples), p95Request.Seconds()*1000, p99Request.Seconds()*1000, p95Process.Seconds()*1000, p99Process.Seconds()*1000)
	t.Logf("replay failure group trend snapshots=%v", failureTrend)

	if reportDir := strings.TrimSpace(os.Getenv("REPLAY_SOAK_REPORT_DIR")); reportDir != "" {
		if err := os.MkdirAll(reportDir, 0o755); err != nil {
			t.Fatalf("mkdir replay soak report dir: %v", err)
		}
		type report struct {
			TimestampUTC          string             `json:"timestamp_utc"`
			Scenario              string             `json:"scenario"`
			CreateRequestCount    int                `json:"create_request_count"`
			JobCount              int                `json:"job_count"`
			WorkerCount           int                `json:"worker_count"`
			SoakSeconds           int                `json:"soak_seconds"`
			RequestP95Millis      float64            `json:"request_p95_ms"`
			RequestP99Millis      float64            `json:"request_p99_ms"`
			ProcessP95Millis      float64            `json:"process_p95_ms"`
			ProcessP99Millis      float64            `json:"process_p99_ms"`
			AnomalyTimeout        int32              `json:"anomaly_timeout"`
			AnomalyCancel         int32              `json:"anomaly_cancel"`
			AnomalyPartial        int32              `json:"anomaly_partial"`
			FinalFailureGroups    map[string]int32   `json:"final_failure_groups"`
			FailureTrendSnapshots []map[string]int32 `json:"failure_trend_snapshots"`
		}
		finalGroups := map[string]int32{}
		if len(failureTrend) > 0 {
			finalGroups = failureTrend[len(failureTrend)-1]
		}
		classify := func(keys []string) int32 {
			var total int32
			for group, count := range finalGroups {
				lower := strings.ToLower(group)
				for _, k := range keys {
					if strings.Contains(lower, k) {
						total += count
						break
					}
				}
			}
			return total
		}
		r := report{
			TimestampUTC:          time.Now().UTC().Format(time.RFC3339),
			Scenario:              "replay_multi_job_command_conflict",
			CreateRequestCount:    len(latencySamples),
			JobCount:              len(jobIDs),
			WorkerCount:           len(workers),
			SoakSeconds:           4,
			RequestP95Millis:      p95Request.Seconds() * 1000,
			RequestP99Millis:      p99Request.Seconds() * 1000,
			ProcessP95Millis:      p95Process.Seconds() * 1000,
			ProcessP99Millis:      p99Process.Seconds() * 1000,
			AnomalyTimeout:        classify([]string{"timeout"}),
			AnomalyCancel:         classify([]string{"cancel", "canceled"}),
			AnomalyPartial:        classify([]string{"partial"}),
			FinalFailureGroups:    finalGroups,
			FailureTrendSnapshots: failureTrend,
		}
		raw, err := json.MarshalIndent(r, "", "  ")
		if err != nil {
			t.Fatalf("marshal replay soak report json: %v", err)
		}
		if err := os.WriteFile(filepath.Join(reportDir, "replay-soak-report.json"), raw, 0o644); err != nil {
			t.Fatalf("write replay soak report json: %v", err)
		}

		csvFile, err := os.Create(filepath.Join(reportDir, "replay-soak-report.csv"))
		if err != nil {
			t.Fatalf("create replay soak report csv: %v", err)
		}
		defer csvFile.Close()
		w := csv.NewWriter(csvFile)
		_ = w.Write([]string{"metric", "value"})
		_ = w.Write([]string{"scenario", r.Scenario})
		_ = w.Write([]string{"create_request_count", fmt.Sprintf("%d", r.CreateRequestCount)})
		_ = w.Write([]string{"job_count", fmt.Sprintf("%d", r.JobCount)})
		_ = w.Write([]string{"worker_count", fmt.Sprintf("%d", r.WorkerCount)})
		_ = w.Write([]string{"soak_seconds", fmt.Sprintf("%d", r.SoakSeconds)})
		_ = w.Write([]string{"request_p95_ms", fmt.Sprintf("%.3f", r.RequestP95Millis)})
		_ = w.Write([]string{"request_p99_ms", fmt.Sprintf("%.3f", r.RequestP99Millis)})
		_ = w.Write([]string{"process_p95_ms", fmt.Sprintf("%.3f", r.ProcessP95Millis)})
		_ = w.Write([]string{"process_p99_ms", fmt.Sprintf("%.3f", r.ProcessP99Millis)})
		_ = w.Write([]string{"anomaly_timeout", fmt.Sprintf("%d", r.AnomalyTimeout)})
		_ = w.Write([]string{"anomaly_cancel", fmt.Sprintf("%d", r.AnomalyCancel)})
		_ = w.Write([]string{"anomaly_partial", fmt.Sprintf("%d", r.AnomalyPartial)})
		for k, v := range r.FinalFailureGroups {
			_ = w.Write([]string{"final_failure_group_" + k, fmt.Sprintf("%d", v)})
		}
		w.Flush()
		if err := w.Error(); err != nil {
			t.Fatalf("flush replay soak report csv: %v", err)
		}
	}
}

func durationQuantiles(samples []time.Duration) (time.Duration, time.Duration) {
	if len(samples) == 0 {
		return 0, 0
	}
	cp := make([]time.Duration, len(samples))
	copy(cp, samples)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	p95Idx := int(float64(len(cp)-1) * 0.95)
	p99Idx := int(float64(len(cp)-1) * 0.99)
	return cp[p95Idx], cp[p99Idx]
}

//go:build integration

package repository

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func TestStoreReplayLeaseRecoverySQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_lease")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-lease@test.com','x','repo','admin')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (501,'ecom.order.created','{"event":"order.created","order_id":9501}','timeout',2)`); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}
	jobID, total, err := store.CreateReplayJob(ctx, 1, "ecom.order.created", 10)
	if err != nil {
		t.Fatalf("create replay job: %v", err)
	}
	if total != 1 {
		t.Fatalf("expected total items 1, got %d", total)
	}
	if _, err := db.Exec(ctx, `
UPDATE replay_jobs
SET status='running',
    lease_owner='stale',
    lease_expires_at=NOW()-INTERVAL '2 minutes',
    updated_at=NOW()-INTERVAL '2 minutes'
WHERE id=$1`, jobID); err != nil {
		t.Fatalf("set stale lease: %v", err)
	}
	if err := store.RecoverExpiredReplayLeases(ctx); err != nil {
		t.Fatalf("recover expired leases: %v", err)
	}
	job, err := store.PickQueuedReplayJob(ctx, "repo-worker", 30*time.Second)
	if err != nil {
		t.Fatalf("pick queued replay job: %v", err)
	}
	if job.ID != jobID || job.Status != "running" {
		t.Fatalf("unexpected picked job: %+v", job)
	}
}

func TestStoreRetryFailedReplayItemsCountersSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_retry_counts")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-retry@test.com','x','repo','admin')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES (601,'ecom.order.created','{"event":"order.created","order_id":9601}','timeout',2)`); err != nil {
		t.Fatalf("seed dead letter: %v", err)
	}
	jobID, _, err := store.CreateReplayJob(ctx, 1, "", 10)
	if err != nil {
		t.Fatalf("create replay job: %v", err)
	}
	if _, err := db.Exec(ctx, `
UPDATE replay_job_items
SET status='failed', terminal=TRUE, attempts=max_attempts, error_group='timeout', last_error='timeout'
WHERE job_id=$1`, jobID); err != nil {
		t.Fatalf("set failed replay item: %v", err)
	}
	if _, err := db.Exec(ctx, `
UPDATE replay_jobs
SET status='partial', processed_items=1, success_items=0, failed_items=1, last_error='timeout'
WHERE id=$1`, jobID); err != nil {
		t.Fatalf("set partial counters: %v", err)
	}

	retrySummary, err := store.RetryFailedReplayItems(ctx, jobID, "timeout", 10, true)
	if err != nil {
		t.Fatalf("retry failed replay items: %v", err)
	}
	if retrySummary.Retried != 1 {
		t.Fatalf("expected retried=1 got=%d", retrySummary.Retried)
	}
	if retrySummary.AttemptsBeforeTotal != 5 || retrySummary.AttemptsAfterTotal != 0 {
		t.Fatalf("unexpected attempts summary before=%d after=%d", retrySummary.AttemptsBeforeTotal, retrySummary.AttemptsAfterTotal)
	}

	var status string
	var processed int32
	var failed int32
	if err := db.QueryRow(ctx, `SELECT status, processed_items, failed_items FROM replay_jobs WHERE id=$1`, jobID).Scan(&status, &processed, &failed); err != nil {
		t.Fatalf("query replay job counters: %v", err)
	}
	if status != "queued" || processed != 0 || failed != 0 {
		t.Fatalf("unexpected counters after retry: status=%s processed=%d failed=%d", status, processed, failed)
	}
	var attempts int32
	var terminal bool
	if err := db.QueryRow(ctx, `SELECT attempts, terminal FROM replay_job_items WHERE job_id=$1`, jobID).Scan(&attempts, &terminal); err != nil {
		t.Fatalf("query replay item attempt reset: %v", err)
	}
	if attempts != 0 || terminal {
		t.Fatalf("expected attempts reset to 0 and terminal=false, got attempts=%d terminal=%v", attempts, terminal)
	}
}

func TestStoreListOrdersCursorDeterministicSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_order_cursor")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-orders@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 25; i++ {
		tm := baseTime
		if i%5 == 0 {
			tm = baseTime.Add(-1 * time.Second)
		}
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,99.0,'created',$2)`, fmt.Sprintf("Addr-%02d", i), tm); err != nil {
			t.Fatalf("seed orders: %v", err)
		}
	}

	page1, total, err := store.ListOrders(ctx, 1, OrderListFilter{Page: 1, PageSize: 10, IncludeTotal: true})
	if err != nil {
		t.Fatalf("list orders page1: %v", err)
	}
	if len(page1) != 10 || total != 25 {
		t.Fatalf("unexpected page1 len/total len=%d total=%d", len(page1), total)
	}
	last := page1[len(page1)-1]
	page2, _, err := store.ListOrders(ctx, 1, OrderListFilter{PageSize: 10, CursorAt: &last.CreatedAt, CursorID: last.ID, IncludeTotal: false})
	if err != nil {
		t.Fatalf("list orders page2 with cursor: %v", err)
	}
	if len(page2) == 0 {
		t.Fatalf("expected page2 records")
	}
	seen := map[int64]bool{}
	for _, o := range page1 {
		seen[o.ID] = true
	}
	for _, o := range page2 {
		if seen[o.ID] {
			t.Fatalf("duplicate id across pages: %d", o.ID)
		}
	}
}

func TestStoreListOrdersPaginationBoundarySQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_order_boundary")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-boundary@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 13; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,88.0,'created',$2)`, fmt.Sprintf("Boundary-%02d", i), now.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed order %d: %v", i, err)
		}
	}

	page3, total, err := store.ListOrders(ctx, 1, OrderListFilter{Page: 3, PageSize: 5, IncludeTotal: true})
	if err != nil {
		t.Fatalf("list page3: %v", err)
	}
	if len(page3) != 3 || total != 13 {
		t.Fatalf("unexpected page3/total len=%d total=%d", len(page3), total)
	}

	page4, total4, err := store.ListOrders(ctx, 1, OrderListFilter{Page: 4, PageSize: 5, IncludeTotal: true})
	if err != nil {
		t.Fatalf("list page4: %v", err)
	}
	if len(page4) != 0 || total4 != 13 {
		t.Fatalf("expected empty page4 with stable total, len=%d total=%d", len(page4), total4)
	}
}

func TestStoreListOrdersIncludesItemCountSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_order_item_count")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-itemcount@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	var orderA int64
	if err := db.QueryRow(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,55.0,'created',$2) RETURNING id`, "Count Lane A", createdAt).Scan(&orderA); err != nil {
		t.Fatalf("seed order A: %v", err)
	}
	var orderB int64
	if err := db.QueryRow(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,66.0,'created',$2) RETURNING id`, "Count Lane B", createdAt.Add(-2*time.Second)).Scan(&orderB); err != nil {
		t.Fatalf("seed order B: %v", err)
	}

	if _, err := db.Exec(ctx, `INSERT INTO order_items(order_id,product_id,product_title,price,quantity) VALUES
($1,301,'Count A',11.0,1),
($1,302,'Count B',7.5,3),
($2,401,'Count C',9.9,2)`, orderA, orderB); err != nil {
		t.Fatalf("seed order items: %v", err)
	}

	items, _, err := store.ListOrders(ctx, 1, OrderListFilter{Page: 1, PageSize: 10, IncludeTotal: false})
	if err != nil {
		t.Fatalf("list orders: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 orders, got %d", len(items))
	}

	expected := map[int64]int32{orderA: 4, orderB: 2}
	for _, order := range items {
		want, ok := expected[order.ID]
		if !ok {
			t.Fatalf("unexpected order id %d", order.ID)
		}
		if order.ItemCount != want {
			t.Fatalf("order %d item_count mismatch: got=%d want=%d", order.ID, order.ItemCount, want)
		}
	}
}

func TestStoreListOrdersAmountRangeSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_order_amount_range")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-amount@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	baseTime := time.Now().UTC().Truncate(time.Microsecond)
	amounts := []float64{5.0, 12.5, 30.0, 50.0}
	for i, amount := range amounts {
		createdAt := baseTime.Add(-time.Duration(i) * time.Second)
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,$2,'created',$3)`,
			fmt.Sprintf("Amount-%02d", i), amount, createdAt); err != nil {
			t.Fatalf("seed order %d: %v", i, err)
		}
	}

	minAmount := 10.0
	maxAmount := 40.0
	items, total, err := store.ListOrders(ctx, 1, OrderListFilter{
		Page:         1,
		PageSize:     10,
		IncludeTotal: true,
		MinAmount:    &minAmount,
		MaxAmount:    &maxAmount,
	})
	if err != nil {
		t.Fatalf("list orders amount range: %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("expected 2 orders in range, got len=%d total=%d", len(items), total)
	}
	expectedAmounts := []float64{12.5, 30.0}
	for i, order := range items {
		if math.Abs(order.Amount-expectedAmounts[i]) > 0.001 {
			t.Fatalf("unexpected amount at index %d: got=%.2f want=%.2f", i, order.Amount, expectedAmounts[i])
		}
		if order.Amount < minAmount || order.Amount > maxAmount {
			t.Fatalf("order amount out of range: %.2f", order.Amount)
		}
	}
}

func TestStoreGetOrderDetailSubtotalSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_order_detail_subtotal")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-subtotal@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	createdAt := time.Now().UTC().Truncate(time.Microsecond)
	var orderID int64
	if err := db.QueryRow(ctx, `
INSERT INTO orders(user_id,address,amount,status,created_at)
VALUES (1,$1,$2,'created',$3)
RETURNING id`, "Subtotal Lane 1", 69.98, createdAt).Scan(&orderID); err != nil {
		t.Fatalf("seed order: %v", err)
	}

	if _, err := db.Exec(ctx, `
INSERT INTO order_items(order_id,product_id,product_title,price,quantity)
VALUES
  ($1,$2,$3,$4,$5),
  ($1,$6,$7,$8,$9)`, orderID, 901, "Subtotal A", 19.99, 2, 902, "Subtotal B", 7.50, 4); err != nil {
		t.Fatalf("seed order items: %v", err)
	}

	detail, err := store.GetOrderDetail(ctx, 1, orderID)
	if err != nil {
		t.Fatalf("get order detail: %v", err)
	}
	if len(detail.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(detail.Items))
	}

	expected := map[int64]struct {
		title    string
		price    float64
		quantity int32
		subtotal float64
	}{
		901: {title: "Subtotal A", price: 19.99, quantity: 2, subtotal: 39.98},
		902: {title: "Subtotal B", price: 7.50, quantity: 4, subtotal: 30.00},
	}
	tolerance := 0.0001
	for _, item := range detail.Items {
		exp, ok := expected[item.ProductID]
		if !ok {
			t.Fatalf("unexpected product_id %d", item.ProductID)
		}
		if item.Title != exp.title {
			t.Fatalf("title mismatch for product %d: %s", item.ProductID, item.Title)
		}
		if item.Quantity != exp.quantity {
			t.Fatalf("quantity mismatch for product %d: got %d want %d", item.ProductID, item.Quantity, exp.quantity)
		}
		if diff := math.Abs(item.Price - exp.price); diff > tolerance {
			t.Fatalf("price mismatch for product %d: got %.2f want %.2f", item.ProductID, item.Price, exp.price)
		}
		if diff := math.Abs(item.Subtotal - exp.subtotal); diff > tolerance {
			t.Fatalf("subtotal mismatch for product %d: got %.2f want %.2f", item.ProductID, item.Subtotal, exp.subtotal)
		}
	}
	if diff := math.Abs(detail.Amount - 69.98); diff > tolerance {
		t.Fatalf("amount mismatch: got %.2f want %.2f", detail.Amount, 69.98)
	}
}

func TestStoreListOrdersExplainUsesCompositeIndexSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_order_explain")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-explain@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 200; i++ {
		status := "created"
		if i%2 == 0 {
			status = "paid"
		}
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,123.0,$2,$3)`, fmt.Sprintf("Explain-%03d", i), status, base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed orders: %v", err)
		}
	}
	if _, err := db.Exec(ctx, `SET enable_seqscan = off`); err != nil {
		t.Fatalf("set enable_seqscan off: %v", err)
	}

	rows, err := db.Query(ctx, `
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id,user_id,address,amount,status,created_at
FROM orders
WHERE user_id=$1
ORDER BY created_at DESC, id DESC
LIMIT 20 OFFSET 0`, 1)
	if err != nil {
		t.Fatalf("explain query: %v", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan explain line: %v", err)
		}
		lines = append(lines, line)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "idx_orders_user_created_id") {
		t.Fatalf("expected idx_orders_user_created_id in explain plan:\n%s", plan)
	}
	t.Logf("orders explain plan:\n%s", plan)
}

func TestStoreListOrdersExplainUsesStatusCompositeIndexSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_order_status_explain")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-explain-status@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 220; i++ {
		status := "created"
		if i%3 == 0 {
			status = "paid"
		}
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,77.0,$2,$3)`, fmt.Sprintf("ExplainStatus-%03d", i), status, base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed status orders: %v", err)
		}
	}
	if _, err := db.Exec(ctx, `SET enable_seqscan = off`); err != nil {
		t.Fatalf("set enable_seqscan off: %v", err)
	}

	rows, err := db.Query(ctx, `
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id,user_id,address,amount,status,created_at
FROM orders
WHERE user_id=$1 AND status=$2
ORDER BY created_at DESC, id DESC
LIMIT 15 OFFSET 0`, 1, "paid")
	if err != nil {
		t.Fatalf("explain status query: %v", err)
	}
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("scan status explain line: %v", err)
		}
		lines = append(lines, line)
	}
	plan := strings.Join(lines, "\n")
	if !strings.Contains(plan, "idx_orders_user_status_created_id") {
		t.Fatalf("expected idx_orders_user_status_created_id in explain plan:\n%s", plan)
	}
	t.Logf("orders status explain plan:\n%s", plan)
}

func TestStoreTraceCorrelationExplainUsesCompositeIndexesSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_trace_explain")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-trace-explain@test.com','x','repo','admin')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 60; i++ {
		traceID := fmt.Sprintf("trace-%02d", i%4)
		commandID := fmt.Sprintf("cmd-%02d", i%7)
		if _, err := db.Exec(ctx, `
INSERT INTO replay_jobs(created_by,status,topic_filter,trace_id,command_id,total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at)
VALUES (1,'completed','ecom.order.created',$1,$2,1,1,1,0,'',$3,$3)`, traceID, commandID, base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed replay job %d: %v", i, err)
		}
		if _, err := db.Exec(ctx, `
INSERT INTO outbox_events(topic,payload,status,retry_count,max_retries,trace_id,command_id,created_at,updated_at)
VALUES ('ecom.order.created','{}','sent',0,6,$1,$2,$3,$3)`, traceID, commandID, base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed outbox event %d: %v", i, err)
		}
	}
	if _, err := db.Exec(ctx, `SET enable_seqscan = off`); err != nil {
		t.Fatalf("set enable_seqscan off: %v", err)
	}

	var replayCursor int64
	if err := db.QueryRow(ctx, `SELECT id FROM replay_jobs ORDER BY id DESC OFFSET 10 LIMIT 1`).Scan(&replayCursor); err != nil {
		t.Fatalf("select replay cursor id: %v", err)
	}
	replayRows, err := db.Query(ctx, `
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id,status,topic_filter,trace_id,command_id,total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at
FROM replay_jobs
WHERE trace_id=$1 AND command_id=$2 AND id < $3
ORDER BY id DESC
LIMIT 10`, "trace-01", "cmd-02", replayCursor)
	if err != nil {
		t.Fatalf("explain replay trace query: %v", err)
	}
	defer replayRows.Close()
	replayLines := make([]string, 0, 16)
	for replayRows.Next() {
		var line string
		if err := replayRows.Scan(&line); err != nil {
			t.Fatalf("scan replay explain line: %v", err)
		}
		replayLines = append(replayLines, line)
	}
	replayPlan := strings.Join(replayLines, "\n")
	if !strings.Contains(replayPlan, "idx_replay_jobs_trace_command_id_desc") &&
		!strings.Contains(replayPlan, "idx_replay_jobs_command_id_desc") &&
		!strings.Contains(replayPlan, "idx_replay_jobs_trace_id_desc") {
		t.Fatalf("expected replay trace explain to use one of trace/cmd composite indexes:\n%s", replayPlan)
	}
	t.Logf("trace replay explain plan:\n%s", replayPlan)

	var outboxCursor int64
	if err := db.QueryRow(ctx, `SELECT id FROM outbox_events ORDER BY id DESC OFFSET 10 LIMIT 1`).Scan(&outboxCursor); err != nil {
		t.Fatalf("select outbox cursor id: %v", err)
	}
	outboxRows, err := db.Query(ctx, `
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id,topic,status,retry_count,max_retries,trace_id,command_id,created_at,updated_at
FROM outbox_events
WHERE trace_id=$1 AND command_id=$2 AND id < $3
ORDER BY id DESC
LIMIT 10`, "trace-01", "cmd-02", outboxCursor)
	if err != nil {
		t.Fatalf("explain outbox trace query: %v", err)
	}
	defer outboxRows.Close()
	outboxLines := make([]string, 0, 16)
	for outboxRows.Next() {
		var line string
		if err := outboxRows.Scan(&line); err != nil {
			t.Fatalf("scan outbox explain line: %v", err)
		}
		outboxLines = append(outboxLines, line)
	}
	outboxPlan := strings.Join(outboxLines, "\n")
	if !strings.Contains(outboxPlan, "idx_outbox_trace_command") &&
		!strings.Contains(outboxPlan, "idx_outbox_command_id_desc") &&
		!strings.Contains(outboxPlan, "idx_outbox_trace_id_desc") {
		t.Fatalf("expected outbox trace explain to use one of trace/cmd indexes:\n%s", outboxPlan)
	}
	t.Logf("trace outbox explain plan:\n%s", outboxPlan)

	if dir := strings.TrimSpace(os.Getenv("TRACE_SEARCH_BASELINE_DIR")); dir != "" {
		if err := writeTraceSearchBaselineArtifacts(ctx, db, dir); err != nil {
			t.Fatalf("write trace search baseline artifacts: %v", err)
		}
	}
}

func TestStoreSearchTraceCorrelationLargeCursorSparseIDsSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_trace_sparse")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-trace-sparse@test.com','x','repo','admin')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 1; i <= 160; i++ {
		id := int64(i * 37)
		traceID := "trace-sparse-miss"
		commandID := "cmd-sparse-miss"
		if i%2 == 0 {
			traceID = "trace-sparse-hit"
			commandID = "cmd-sparse-hit"
		}
		if _, err := db.Exec(ctx, `
INSERT INTO replay_jobs(id,created_by,status,topic_filter,trace_id,command_id,total_items,processed_items,success_items,failed_items,last_error,created_at,updated_at)
VALUES ($1,1,'completed','ecom.order.created',$2,$3,1,1,1,0,'',$4,$4)`, id, traceID, commandID, base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed replay sparse row %d: %v", i, err)
		}
	}
	if _, err := db.Exec(ctx, `SELECT setval(pg_get_serial_sequence('replay_jobs','id'), 7000, true)`); err != nil {
		t.Fatalf("set replay_jobs sequence: %v", err)
	}

	cursor := int64(5000)
	page1, err := store.SearchReplayJobsByCorrelation(ctx, "trace-sparse-hit", "cmd-sparse-hit", 25, cursor)
	if err != nil {
		t.Fatalf("search replay sparse page1: %v", err)
	}
	if len(page1) == 0 || len(page1) > 25 {
		t.Fatalf("expected replay page1 size in 1..25, got %d", len(page1))
	}
	lastID := int64(0)
	seen := map[int64]bool{}
	for i, item := range page1 {
		if item.ID >= cursor {
			t.Fatalf("expected replay item id < cursor, got id=%d cursor=%d", item.ID, cursor)
		}
		if item.TraceID != "trace-sparse-hit" || item.CommandID != "cmd-sparse-hit" {
			t.Fatalf("unexpected trace/cmd filter mismatch item=%+v", item)
		}
		if i > 0 && item.ID >= lastID {
			t.Fatalf("expected id-desc order, prev=%d current=%d", lastID, item.ID)
		}
		lastID = item.ID
		seen[item.ID] = true
	}
	page2, err := store.SearchReplayJobsByCorrelation(ctx, "trace-sparse-hit", "cmd-sparse-hit", 25, lastID)
	if err != nil {
		t.Fatalf("search replay sparse page2: %v", err)
	}
	for _, item := range page2 {
		if seen[item.ID] {
			t.Fatalf("unexpected duplicate across pages id=%d", item.ID)
		}
		if item.ID >= lastID {
			t.Fatalf("expected page2 id < page1 last id, got id=%d last=%d", item.ID, lastID)
		}
	}
	empty, err := store.SearchReplayJobsByCorrelation(ctx, "trace-sparse-hit", "cmd-sparse-hit", 25, 1)
	if err != nil {
		t.Fatalf("search replay sparse empty page: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty replay page when cursor at lower bound, got %d", len(empty))
	}
}

func TestStoreSearchOutboxCorrelationLargeCursorSparseIDsSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_outbox_sparse")
	defer cleanup()

	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 1; i <= 220; i++ {
		id := int64(i * 41)
		traceID := "trace-outbox-miss"
		commandID := "cmd-outbox-miss"
		if i%3 == 0 {
			traceID = "trace-outbox-hit"
			commandID = "cmd-outbox-hit"
		}
		if _, err := db.Exec(ctx, `
INSERT INTO outbox_events(id,topic,payload,status,retry_count,max_retries,trace_id,command_id,correlation_source,created_at,updated_at)
VALUES ($1,'ecom.order.created','{}','sent',0,6,$2,$3,'sparse_seed',$4,$4)`, id, traceID, commandID, base.Add(-time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("seed outbox sparse row %d: %v", i, err)
		}
	}
	if _, err := db.Exec(ctx, `SELECT setval(pg_get_serial_sequence('outbox_events','id'), 9000, true)`); err != nil {
		t.Fatalf("set outbox_events sequence: %v", err)
	}

	cursor := int64(7000)
	page1, err := store.SearchOutboxEventsByCorrelation(ctx, "trace-outbox-hit", "cmd-outbox-hit", 30, cursor)
	if err != nil {
		t.Fatalf("search outbox sparse page1: %v", err)
	}
	if len(page1) == 0 || len(page1) > 30 {
		t.Fatalf("expected outbox page1 size in 1..30, got %d", len(page1))
	}
	lastID := int64(0)
	seen := map[int64]bool{}
	for i, item := range page1 {
		if item.ID >= cursor {
			t.Fatalf("expected outbox item id < cursor, got id=%d cursor=%d", item.ID, cursor)
		}
		if item.TraceID != "trace-outbox-hit" || item.CommandID != "cmd-outbox-hit" {
			t.Fatalf("unexpected outbox trace/cmd filter mismatch item=%+v", item)
		}
		if i > 0 && item.ID >= lastID {
			t.Fatalf("expected outbox id-desc order, prev=%d current=%d", lastID, item.ID)
		}
		lastID = item.ID
		seen[item.ID] = true
	}
	page2, err := store.SearchOutboxEventsByCorrelation(ctx, "trace-outbox-hit", "cmd-outbox-hit", 30, lastID)
	if err != nil {
		t.Fatalf("search outbox sparse page2: %v", err)
	}
	for _, item := range page2 {
		if seen[item.ID] {
			t.Fatalf("unexpected outbox duplicate across pages id=%d", item.ID)
		}
		if item.ID >= lastID {
			t.Fatalf("expected outbox page2 id < page1 last id, got id=%d last=%d", item.ID, lastID)
		}
	}
	empty, err := store.SearchOutboxEventsByCorrelation(ctx, "trace-outbox-hit", "cmd-outbox-hit", 30, 1)
	if err != nil {
		t.Fatalf("search outbox sparse empty page: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected empty outbox page when cursor at lower bound, got %d", len(empty))
	}
}

func writeTraceSearchBaselineArtifacts(ctx context.Context, db *pgxpool.Pool, dir string) error {
	type sample struct {
		bucket      string
		queryType   string
		cursor      int64
		iterations  int
		avgMillis   float64
		p95Millis   float64
		explainHint string
	}
	mkBucket := func(name string, cursor int64) (sample, sample, error) {
		const iterations = 12
		replayLat := make([]float64, 0, iterations)
		outboxLat := make([]float64, 0, iterations)
		for i := 0; i < iterations; i++ {
			start := time.Now()
			rows, err := db.Query(ctx, `
SELECT id FROM replay_jobs
WHERE trace_id='trace-sparse-hit' AND command_id='cmd-sparse-hit' AND id < $1
ORDER BY id DESC
LIMIT 25`, cursor)
			if err != nil {
				return sample{}, sample{}, err
			}
			for rows.Next() {
			}
			rows.Close()
			replayLat = append(replayLat, float64(time.Since(start).Microseconds())/1000.0)

			start = time.Now()
			rows2, err := db.Query(ctx, `
SELECT id FROM outbox_events
WHERE trace_id='trace-outbox-hit' AND command_id='cmd-outbox-hit' AND id < $1
ORDER BY id DESC
LIMIT 30`, cursor)
			if err != nil {
				return sample{}, sample{}, err
			}
			for rows2.Next() {
			}
			rows2.Close()
			outboxLat = append(outboxLat, float64(time.Since(start).Microseconds())/1000.0)
		}
		sort.Float64s(replayLat)
		sort.Float64s(outboxLat)
		avg := func(vals []float64) float64 {
			if len(vals) == 0 {
				return 0
			}
			sum := 0.0
			for _, v := range vals {
				sum += v
			}
			return sum / float64(len(vals))
		}
		p95 := func(vals []float64) float64 {
			if len(vals) == 0 {
				return 0
			}
			idx := int(float64(len(vals)-1) * 0.95)
			return vals[idx]
		}
		replay := sample{
			bucket:      name,
			queryType:   "replay_trace_search",
			cursor:      cursor,
			iterations:  iterations,
			avgMillis:   avg(replayLat),
			p95Millis:   p95(replayLat),
			explainHint: "idx_replay_jobs_trace_command_id_desc|idx_replay_jobs_command_id_desc|idx_replay_jobs_trace_id_desc",
		}
		outbox := sample{
			bucket:      name,
			queryType:   "outbox_trace_search",
			cursor:      cursor,
			iterations:  iterations,
			avgMillis:   avg(outboxLat),
			p95Millis:   p95(outboxLat),
			explainHint: "idx_outbox_trace_command|idx_outbox_command_id_desc|idx_outbox_trace_id_desc",
		}
		return replay, outbox, nil
	}

	buckets := []struct {
		name   string
		cursor int64
	}{
		{name: "high", cursor: 1000000},
		{name: "mid", cursor: 5000},
		{name: "low", cursor: 100},
	}
	results := make([]sample, 0, len(buckets)*2)
	for _, b := range buckets {
		replay, outbox, err := mkBucket(b.name, b.cursor)
		if err != nil {
			return err
		}
		results = append(results, replay, outbox)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	jsonPath := filepath.Join(dir, "trace-search-baseline.json")
	csvPath := filepath.Join(dir, "trace-search-baseline.csv")

	var sb strings.Builder
	sb.WriteString("{\n  \"version\": \"iter-21\",\n  \"samples\": [\n")
	for i, r := range results {
		sb.WriteString(fmt.Sprintf("    {\"bucket\":\"%s\",\"query_type\":\"%s\",\"cursor\":%d,\"iterations\":%d,\"avg_ms\":%.3f,\"p95_ms\":%.3f,\"explain_hint\":\"%s\"}", r.bucket, r.queryType, r.cursor, r.iterations, r.avgMillis, r.p95Millis, r.explainHint))
		if i < len(results)-1 {
			sb.WriteString(",")
		}
		sb.WriteString("\n")
	}
	sb.WriteString("  ]\n}\n")
	if err := os.WriteFile(jsonPath, []byte(sb.String()), 0o644); err != nil {
		return err
	}

	f, err := os.Create(csvPath)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"bucket", "query_type", "cursor", "iterations", "avg_ms", "p95_ms", "explain_hint"})
	for _, r := range results {
		_ = w.Write([]string{r.bucket, r.queryType, fmt.Sprintf("%d", r.cursor), fmt.Sprintf("%d", r.iterations), fmt.Sprintf("%.3f", r.avgMillis), fmt.Sprintf("%.3f", r.p95Millis), r.explainHint})
	}
	w.Flush()
	return w.Error()
}

func TestStoreIdempotencyUniqueConflictSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_idem_conflict")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-idem@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO idempotency_keys(user_id, idem_key, status) VALUES (1,'dup-key','processing')`); err != nil {
		t.Fatalf("insert first idem key: %v", err)
	}
	_, err := db.Exec(ctx, `INSERT INTO idempotency_keys(user_id, idem_key, status) VALUES (1,'dup-key','processing')`)
	if err == nil {
		t.Fatalf("expected duplicate key conflict")
	}
	pgErr, ok := err.(*pgconn.PgError)
	if !ok {
		t.Fatalf("expected pg error type, got %T", err)
	}
	if pgErr.Code != "23505" {
		t.Fatalf("expected unique_violation(23505), got %s (%s)", pgErr.Code, pgErr.Message)
	}
}

func TestStoreIdempotencyRowLockContentionSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_idem_lock")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-lock@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO idempotency_keys(user_id, idem_key, status) VALUES (1,'lock-key','processing')`); err != nil {
		t.Fatalf("seed idempotency key: %v", err)
	}

	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	if _, err := tx1.Exec(ctx, `SELECT idem_key FROM idempotency_keys WHERE user_id=$1 AND idem_key=$2 FOR UPDATE`, 1, "lock-key"); err != nil {
		t.Fatalf("tx1 lock idempotency row: %v", err)
	}

	lockErrCh := make(chan error, 1)
	go func() {
		tx2, err := db.Begin(ctx)
		if err != nil {
			lockErrCh <- err
			return
		}
		defer tx2.Rollback(ctx)
		if _, err := tx2.Exec(ctx, `SET LOCAL lock_timeout = '200ms'`); err != nil {
			lockErrCh <- err
			return
		}
		_, err = tx2.Exec(ctx, `UPDATE idempotency_keys SET updated_at=NOW() WHERE user_id=$1 AND idem_key=$2`, 1, "lock-key")
		lockErrCh <- err
	}()

	err = <-lockErrCh
	if err == nil {
		t.Fatalf("expected lock timeout conflict error")
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("expected lock_not_available 55P03, got %v", err)
	}
}

func TestStoreCreateOrderFromCartWithIdempotencyConcurrentConflictSQL(t *testing.T) {
	ctx := context.Background()
	store, db, cleanup := setupStoreFixture(t, "repo_idem_concurrent_order")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-idem-order@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (101,'Concurrent Product',9.9,1000)`); err != nil {
		t.Fatalf("seed product: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO cart_items(user_id,product_id,quantity) VALUES (1,101,2)`); err != nil {
		t.Fatalf("seed cart item: %v", err)
	}

	const workers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	var success, replay, inprogress, otherErr int32
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, isReplay, err := store.CreateOrderFromCartWithIdempotency(ctx, 1, "Concurrent Street", "idem-concurrent-1")
			if err == nil {
				if isReplay {
					atomic.AddInt32(&replay, 1)
				} else {
					atomic.AddInt32(&success, 1)
				}
				return
			}
			if errors.Is(err, ErrIdempotencyInProgress) {
				atomic.AddInt32(&inprogress, 1)
				return
			}
			atomic.AddInt32(&otherErr, 1)
		}()
	}
	close(start)
	wg.Wait()

	if otherErr != 0 {
		t.Fatalf("unexpected non-idempotency errors: %d", otherErr)
	}
	if success != 1 {
		t.Fatalf("expected exactly one success path, got %d", success)
	}
	if replay+inprogress != workers-1 {
		t.Fatalf("expected replay+inprogress=%d got replay=%d inprogress=%d", workers-1, replay, inprogress)
	}

	var orderCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE user_id=1`).Scan(&orderCount); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if orderCount != 1 {
		t.Fatalf("expected one order row, got %d", orderCount)
	}
	var outboxCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic='ecom.order.created'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("expected one outbox row, got %d", outboxCount)
	}

	var idemStatus string
	if err := db.QueryRow(ctx, `SELECT status FROM idempotency_keys WHERE user_id=$1 AND idem_key=$2`, 1, "idem-concurrent-1").Scan(&idemStatus); err != nil {
		t.Fatalf("query idem status: %v", err)
	}
	if idemStatus != "completed" {
		t.Fatalf("expected completed idem status, got %s", idemStatus)
	}

	// A post-check replay request should return the same order.
	o, isReplay, err := store.CreateOrderFromCartWithIdempotency(ctx, 1, "Concurrent Street", "idem-concurrent-1")
	if err != nil {
		t.Fatalf("post-check replay call failed: %v", err)
	}
	if !isReplay {
		t.Fatalf("expected replay=true in post-check call")
	}
	var latestOrderID int64
	if err := db.QueryRow(ctx, `SELECT id FROM orders WHERE user_id=1 ORDER BY id DESC LIMIT 1`).Scan(&latestOrderID); err != nil {
		t.Fatalf("query latest order: %v", err)
	}
	if o.ID != latestOrderID {
		t.Fatalf("expected replay order id %d got %d", latestOrderID, o.ID)
	}
}

func TestStoreReplayItemsForUpdateSkipLockedSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_skip_locked")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-skip@test.com','x','repo','admin')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count) VALUES ($1,'ecom.order.created',$2,'seed',2)`, 701+i, []byte(fmt.Sprintf(`{"event":"order.created","order_id":%d}`, 9700+i))); err != nil {
			t.Fatalf("seed dead letter %d: %v", i, err)
		}
	}
	var jobID int64
	if err := db.QueryRow(ctx, `INSERT INTO replay_jobs(created_by, status, topic_filter) VALUES (1,'running','') RETURNING id`).Scan(&jobID); err != nil {
		t.Fatalf("create replay job: %v", err)
	}
	rows, err := db.Query(ctx, `SELECT id FROM dead_letter_events ORDER BY id LIMIT 2`)
	if err != nil {
		t.Fatalf("query dead letters: %v", err)
	}
	ids := make([]int64, 0, 2)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatalf("scan dead letter id: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	for _, deadID := range ids {
		if _, err := db.Exec(ctx, `INSERT INTO replay_job_items(job_id, dead_letter_id, status) VALUES ($1,$2,'pending')`, jobID, deadID); err != nil {
			t.Fatalf("insert replay item: %v", err)
		}
	}

	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	var lockedID int64
	if err := tx1.QueryRow(ctx, `
SELECT dead_letter_id
FROM replay_job_items
WHERE job_id=$1
ORDER BY id
LIMIT 1
FOR UPDATE`, jobID).Scan(&lockedID); err != nil {
		t.Fatalf("tx1 lock first replay item: %v", err)
	}

	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)
	var pickedID int64
	if err := tx2.QueryRow(ctx, `
SELECT dead_letter_id
FROM replay_job_items
WHERE job_id=$1
ORDER BY id
LIMIT 1
FOR UPDATE SKIP LOCKED`, jobID).Scan(&pickedID); err != nil {
		t.Fatalf("tx2 skip locked query: %v", err)
	}
	if pickedID == lockedID {
		t.Fatalf("expected SKIP LOCKED row to differ; locked=%d picked=%d", lockedID, pickedID)
	}
}

func TestStoreDeadlockRegressionSampleSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_deadlock")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-deadlock@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (111,'DeadlockA',1.0,100),(112,'DeadlockB',1.0,100)`); err != nil {
		t.Fatalf("seed products: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO cart_items(user_id, product_id, quantity) VALUES (1,111,1),(1,112,1)`); err != nil {
		t.Fatalf("seed cart items: %v", err)
	}

	tx1, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback(ctx)
	tx2, err := db.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback(ctx)

	if _, err := tx1.Exec(ctx, `UPDATE cart_items SET quantity=quantity+1 WHERE user_id=1 AND product_id=111`); err != nil {
		t.Fatalf("tx1 update first row: %v", err)
	}
	if _, err := tx2.Exec(ctx, `UPDATE cart_items SET quantity=quantity+1 WHERE user_id=1 AND product_id=112`); err != nil {
		t.Fatalf("tx2 update second row: %v", err)
	}
	if _, err := tx1.Exec(ctx, `SET LOCAL lock_timeout='3s'`); err != nil {
		t.Fatalf("tx1 set lock timeout: %v", err)
	}
	if _, err := tx2.Exec(ctx, `SET LOCAL lock_timeout='3s'`); err != nil {
		t.Fatalf("tx2 set lock timeout: %v", err)
	}

	errCh := make(chan error, 2)
	go func() {
		_, err := tx1.Exec(ctx, `UPDATE cart_items SET quantity=quantity+1 WHERE user_id=1 AND product_id=112`)
		errCh <- err
	}()
	time.Sleep(100 * time.Millisecond)
	go func() {
		_, err := tx2.Exec(ctx, `UPDATE cart_items SET quantity=quantity+1 WHERE user_id=1 AND product_id=111`)
		errCh <- err
	}()

	var gotDeadlock bool
	for i := 0; i < 2; i++ {
		err := <-errCh
		if err == nil {
			continue
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "40P01" {
			gotDeadlock = true
		}
	}
	if !gotDeadlock {
		t.Fatalf("expected deadlock_detected 40P01 from opposing update order")
	}
}

func TestStoreRepeatableReadIsolationSnapshotSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_repeatable_read")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-rr@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status) VALUES (1,$1,9.9,'created')`, fmt.Sprintf("RR-%d", i)); err != nil {
			t.Fatalf("seed order %d: %v", i, err)
		}
	}

	tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatalf("begin repeatable read tx: %v", err)
	}
	defer tx.Rollback(ctx)

	var first int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE user_id=1`).Scan(&first); err != nil {
		t.Fatalf("count orders first read: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status) VALUES (1,'RR-new',9.9,'created')`); err != nil {
		t.Fatalf("insert concurrent order: %v", err)
	}
	var second int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE user_id=1`).Scan(&second); err != nil {
		t.Fatalf("count orders second read: %v", err)
	}
	if first != second {
		t.Fatalf("repeatable read snapshot violated first=%d second=%d", first, second)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit repeatable read tx: %v", err)
	}

	var after int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM orders WHERE user_id=1`).Scan(&after); err != nil {
		t.Fatalf("count orders after commit: %v", err)
	}
	if after != first+1 {
		t.Fatalf("expected visible concurrent insert after commit, first=%d after=%d", first, after)
	}
}

func TestStoreLongTxnRetryStrategySQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_longtxn_retry")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-retry-txn@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (201,'Retry Product',1.0,100)`); err != nil {
		t.Fatalf("seed product: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO cart_items(user_id,product_id,quantity) VALUES (1,201,1)`); err != nil {
		t.Fatalf("seed cart item: %v", err)
	}

	runWithRetry := func(tag string, hold time.Duration) error {
		var lastErr error
		for attempt := 1; attempt <= 4; attempt++ {
			tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='300ms'`); err != nil {
				tx.Rollback(ctx)
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE cart_items SET quantity=quantity+1 WHERE user_id=1 AND product_id=201`); err != nil {
				tx.Rollback(ctx)
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01" || pgErr.Code == "40001") {
					lastErr = err
					time.Sleep(time.Duration(attempt*80) * time.Millisecond)
					continue
				}
				return err
			}
			if hold > 0 {
				time.Sleep(hold)
			}
			if err := tx.Commit(ctx); err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01" || pgErr.Code == "40001") {
					lastErr = err
					time.Sleep(time.Duration(attempt*80) * time.Millisecond)
					continue
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("exhausted retry for %s: %w", tag, lastErr)
	}

	errCh := make(chan error, 2)
	go func() { errCh <- runWithRetry("long", 700*time.Millisecond) }()
	time.Sleep(100 * time.Millisecond)
	go func() { errCh <- runWithRetry("retrying", 0) }()
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("long txn retry worker failed: %v", err)
		}
	}

	var qty int
	if err := db.QueryRow(ctx, `SELECT quantity FROM cart_items WHERE user_id=1 AND product_id=201`).Scan(&qty); err != nil {
		t.Fatalf("query final quantity: %v", err)
	}
	if qty != 3 {
		t.Fatalf("expected quantity=3 after two successful updates with retry, got %d", qty)
	}
}

func runLongTxnRetryScenario(ctx context.Context, db *pgxpool.Pool, workers int, hold time.Duration, maxAttempts int, baseBackoff time.Duration) (int, error) {
	runWithRetry := func(holdFor time.Duration) error {
		var lastErr error
		for attempt := 1; attempt <= maxAttempts; attempt++ {
			tx, err := db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout='280ms'`); err != nil {
				tx.Rollback(ctx)
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE cart_items SET quantity=quantity+1 WHERE user_id=1 AND product_id=201`); err != nil {
				tx.Rollback(ctx)
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01" || pgErr.Code == "40001") {
					lastErr = err
					time.Sleep(time.Duration(attempt) * baseBackoff)
					continue
				}
				return err
			}
			if holdFor > 0 {
				time.Sleep(holdFor)
			}
			if err := tx.Commit(ctx); err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && (pgErr.Code == "55P03" || pgErr.Code == "40P01" || pgErr.Code == "40001") {
					lastErr = err
					time.Sleep(time.Duration(attempt) * baseBackoff)
					continue
				}
				return err
			}
			return nil
		}
		return fmt.Errorf("retry attempts exhausted: %w", lastErr)
	}

	errCh := make(chan error, workers)
	go func() { errCh <- runWithRetry(hold) }()
	time.Sleep(120 * time.Millisecond)
	for i := 1; i < workers; i++ {
		go func() { errCh <- runWithRetry(0) }()
	}
	for i := 0; i < workers; i++ {
		if err := <-errCh; err != nil {
			return 0, err
		}
	}
	var qty int
	if err := db.QueryRow(ctx, `SELECT quantity FROM cart_items WHERE user_id=1 AND product_id=201`).Scan(&qty); err != nil {
		return 0, err
	}
	return qty, nil
}

func TestStoreLongTxnRetryHigherConcurrencyWindowSQL(t *testing.T) {
	ctx := context.Background()
	_, db, cleanup := setupStoreFixture(t, "repo_longtxn_window")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-window@test.com','x','repo','customer')`); err != nil {
		t.Fatalf("seed user: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (201,'Retry Product',1.0,100)`); err != nil {
		t.Fatalf("seed product: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO cart_items(user_id,product_id,quantity) VALUES (1,201,1)`); err != nil {
		t.Fatalf("seed cart item: %v", err)
	}

	qty, err := runLongTxnRetryScenario(ctx, db, 8, 1300*time.Millisecond, 8, 45*time.Millisecond)
	if err != nil {
		t.Fatalf("higher concurrency retry scenario failed: %v", err)
	}
	if qty != 9 {
		t.Fatalf("expected quantity=9 after 8 workers, got %d", qty)
	}
}

func BenchmarkStoreLongTxnRetryBackoffParameterizedSQL(b *testing.B) {
	cases := []struct {
		name       string
		workers    int
		hold       time.Duration
		maxAttempt int
		backoff    time.Duration
	}{
		{name: "w6_backoff30", workers: 6, hold: 900 * time.Millisecond, maxAttempt: 6, backoff: 30 * time.Millisecond},
		{name: "w10_backoff60", workers: 10, hold: 1200 * time.Millisecond, maxAttempt: 8, backoff: 60 * time.Millisecond},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				_, db, cleanup := setupStoreFixture(b, "repo_retry_bench_"+strings.ReplaceAll(tc.name, "-", "_"))
				if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'repo-bench@test.com','x','repo','customer')`); err != nil {
					b.Fatalf("seed user: %v", err)
				}
				if _, err := db.Exec(ctx, `INSERT INTO products(id,title,price,stock) VALUES (201,'Retry Product',1.0,100)`); err != nil {
					b.Fatalf("seed product: %v", err)
				}
				if _, err := db.Exec(ctx, `INSERT INTO cart_items(user_id,product_id,quantity) VALUES (1,201,1)`); err != nil {
					b.Fatalf("seed cart item: %v", err)
				}
				start := time.Now()
				qty, err := runLongTxnRetryScenario(ctx, db, tc.workers, tc.hold, tc.maxAttempt, tc.backoff)
				if err != nil {
					cleanup()
					b.Fatalf("run scenario: %v", err)
				}
				if qty != tc.workers+1 {
					cleanup()
					b.Fatalf("expected quantity=%d got %d", tc.workers+1, qty)
				}
				b.ReportMetric(float64(time.Since(start).Milliseconds()), "scenario_ms")
				cleanup()
			}
		})
	}
}

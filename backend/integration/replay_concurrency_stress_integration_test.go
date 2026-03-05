//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ductor/ecommerce_app/backend/internal/repository"
	"github.com/ductor/ecommerce_app/backend/internal/service"
)

func TestIntegration_ReplayConcurrentPreemptionAndReentryStress(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(t, "replay_concurrent_stress")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'stress-admin@test.com','x','admin','admin')`); err != nil {
		t.Fatalf("seed admin: %v", err)
	}
	for i := 0; i < 40; i++ {
		if _, err := db.Exec(ctx, `
INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count)
VALUES ($1, 'ecom.order.created', $2, 'timeout', 2)`, 10000+i, []byte(`{"event":"order.created","order_id":100}`)); err != nil {
			t.Fatalf("seed dead letter %d: %v", i, err)
		}
	}

	store := repository.NewStore(db)
	jobID, total, err := store.CreateReplayJob(ctx, 1, "ecom.order.created", 100)
	if err != nil {
		t.Fatalf("create replay job: %v", err)
	}
	if total != 40 {
		t.Fatalf("expected total 40, got %d", total)
	}

	svc1 := service.NewReplayJobService(store)
	svc2 := service.NewReplayJobService(store)

	var wg sync.WaitGroup
	runWorker := func(svc *service.ReplayJobService) {
		defer wg.Done()
		for i := 0; i < 15; i++ {
			_ = svc.ProcessOnce(ctx)
			time.Sleep(20 * time.Millisecond)
		}
	}
	wg.Add(2)
	go runWorker(svc1)
	go runWorker(svc2)
	wg.Wait()

	job, _, err := store.GetReplayJob(ctx, jobID)
	if err != nil {
		t.Fatalf("get replay job after stress: %v", err)
	}
	if job.Status != "completed" {
		t.Fatalf("expected completed status, got %s", job.Status)
	}

	var outboxCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic='ecom.order.created'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox events: %v", err)
	}
	if outboxCount != 40 {
		t.Fatalf("expected exactly 40 replayed outbox events, got %d", outboxCount)
	}

	beforeReentry := outboxCount
	wg = sync.WaitGroup{}
	reentry := func() {
		defer wg.Done()
		for i := 0; i < 8; i++ {
			_ = svc1.ProcessOnce(ctx)
			_ = svc2.ProcessOnce(ctx)
		}
	}
	wg.Add(3)
	go reentry()
	go reentry()
	go reentry()
	wg.Wait()

	var afterReentry int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic='ecom.order.created'`).Scan(&afterReentry); err != nil {
		t.Fatalf("count outbox events after reentry: %v", err)
	}
	if afterReentry != beforeReentry {
		t.Fatalf("idempotent reentry violated: before=%d after=%d", beforeReentry, afterReentry)
	}
}

func TestIntegration_ReplayMultiWorkerSoakWithFailureGroups(t *testing.T) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(t, "replay_multiworker_soak")
	defer cleanup()

	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'soak-admin@test.com','x','admin','admin')`); err != nil {
		t.Fatalf("seed admin: %v", err)
	}

	store := repository.NewStore(db)
	const (
		jobCount        = 4
		itemsPerJob     = 120
		missingPerJob   = 15
		workerCount     = 8
		processInterval = 15 * time.Millisecond
	)
	soakSeconds := getenvIntDefaultReplay("REPLAY_SOAK_SECONDS", 6)
	if soakSeconds < 2 {
		soakSeconds = 2
	}

	totalSeeded := 0
	totalFailedSeed := 0
	expectedFailureGroups := map[string]int32{
		"seed.not_found": 0,
		"seed.conflict":  0,
	}
	jobIDs := make([]int64, 0, jobCount)
	seedOutboxID := 30000
	for j := 0; j < jobCount; j++ {
		for i := 0; i < itemsPerJob; i++ {
			payload := []byte(fmt.Sprintf(`{"event":"order.created","order_id":%d}`, 800000+j*itemsPerJob+i))
			if _, err := db.Exec(ctx, `
INSERT INTO dead_letter_events(source_outbox_id, topic, payload, last_error, retry_count)
VALUES ($1, 'ecom.order.created', $2, 'timeout', 2)`, seedOutboxID, payload); err != nil {
				t.Fatalf("seed dead letter (job=%d idx=%d): %v", j, i, err)
			}
			seedOutboxID++
		}
		jobID, total, err := store.CreateReplayJob(ctx, 1, "ecom.order.created", itemsPerJob)
		if err != nil {
			t.Fatalf("create replay job %d: %v", j, err)
		}
		if total != itemsPerJob {
			t.Fatalf("expected total %d, got %d", itemsPerJob, total)
		}
		jobIDs = append(jobIDs, jobID)
		totalSeeded += int(total)

		if _, err := db.Exec(ctx, `UPDATE replay_job_items SET max_attempts=1 WHERE job_id=$1`, jobID); err != nil {
			t.Fatalf("set max_attempts=1 for job %d: %v", jobID, err)
		}
		rows, err := db.Query(ctx, `SELECT dead_letter_id FROM replay_job_items WHERE job_id=$1 ORDER BY id LIMIT $2`, jobID, missingPerJob)
		if err != nil {
			t.Fatalf("query replay job items to seed-fail for job %d: %v", jobID, err)
		}
		toFail := make([]int64, 0, missingPerJob)
		for rows.Next() {
			var deadID int64
			if err := rows.Scan(&deadID); err != nil {
				rows.Close()
				t.Fatalf("scan seed-fail dead letter id for job %d: %v", jobID, err)
			}
			toFail = append(toFail, deadID)
		}
		rows.Close()
		for idx, deadID := range toFail {
			errMsg := "seed.not_found"
			if idx%2 == 1 {
				errMsg = "seed.conflict"
			}
			if err := store.MarkReplayItemFailed(ctx, jobID, deadID, errMsg); err != nil {
				t.Fatalf("seed mark replay item failed (job=%d dead_id=%d): %v", jobID, deadID, err)
			}
			expectedFailureGroups[errMsg]++
		}
		totalFailedSeed += len(toFail)
	}

	workers := make([]*service.ReplayJobService, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		workers = append(workers, service.NewReplayJobService(store))
	}
	deadline := time.Now().Add(time.Duration(soakSeconds) * time.Second)
	var wg sync.WaitGroup
	for _, worker := range workers {
		wg.Add(1)
		go func(svc *service.ReplayJobService) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				_ = svc.ProcessOnce(ctx)
				time.Sleep(processInterval)
			}
		}(worker)
	}
	wg.Wait()

	for i := 0; i < 30; i++ {
		for _, worker := range workers {
			_ = worker.ProcessOnce(ctx)
		}
	}

	failureGroups := map[string]int32{}
	var completed, partial, failed int
	for _, jobID := range jobIDs {
		job, groups, err := store.GetReplayJob(ctx, jobID)
		if err != nil {
			t.Fatalf("get replay job %d: %v", jobID, err)
		}
		switch job.Status {
		case "completed":
			completed++
		case "partial":
			partial++
		case "failed":
			failed++
		default:
			t.Fatalf("unexpected replay status for job %d: %s", jobID, job.Status)
		}
		for _, g := range groups {
			failureGroups[g.ErrorGroup] += g.Count
		}
	}
	if partial != jobCount {
		t.Fatalf("expected all jobs partial due to seeded failures: partial=%d completed=%d failed=%d", partial, completed, failed)
	}
	for group, expected := range expectedFailureGroups {
		if got := failureGroups[group]; got != expected {
			t.Fatalf("failure group mismatch group=%s expected=%d got=%d groups=%v", group, expected, got, failureGroups)
		}
	}
	var groupedTotal int32
	for _, c := range failureGroups {
		groupedTotal += c
	}
	if groupedTotal != int32(totalFailedSeed) {
		t.Fatalf("failure group total mismatch expected=%d got=%d groups=%v", totalFailedSeed, groupedTotal, failureGroups)
	}

	var outboxCount int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic='ecom.order.created'`).Scan(&outboxCount); err != nil {
		t.Fatalf("count replay outbox: %v", err)
	}
	expectedOutbox := totalSeeded - totalFailedSeed
	if outboxCount != expectedOutbox {
		t.Fatalf("unexpected outbox replay count expected=%d got=%d", expectedOutbox, outboxCount)
	}

	before := outboxCount
	for i := 0; i < 12; i++ {
		var inner sync.WaitGroup
		for _, worker := range workers {
			inner.Add(1)
			go func(svc *service.ReplayJobService) {
				defer inner.Done()
				_ = svc.ProcessOnce(ctx)
			}(worker)
		}
		inner.Wait()
	}
	var after int
	if err := db.QueryRow(ctx, `SELECT COUNT(*) FROM outbox_events WHERE topic='ecom.order.created'`).Scan(&after); err != nil {
		t.Fatalf("count outbox after idempotent reentry: %v", err)
	}
	if before != after {
		t.Fatalf("idempotent reentry broken before=%d after=%d", before, after)
	}

	t.Logf("replay soak stats: jobs=%d workers=%d soak_seconds=%d seeded=%d missing=%d outbox=%d failure_groups=%v",
		jobCount, workerCount, soakSeconds, totalSeeded, totalFailedSeed, outboxCount, failureGroups)
}

func getenvIntDefaultReplay(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

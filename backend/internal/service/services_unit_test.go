package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGenerateTokenFormatAndUniqueness(t *testing.T) {
	tok1, err := generateToken()
	if err != nil {
		t.Fatalf("generate token 1: %v", err)
	}
	tok2, err := generateToken()
	if err != nil {
		t.Fatalf("generate token 2: %v", err)
	}
	if !strings.HasPrefix(tok1, "tok_") || !strings.HasPrefix(tok2, "tok_") {
		t.Fatalf("token prefix mismatch: %q %q", tok1, tok2)
	}
	if len(tok1) != 36 || len(tok2) != 36 {
		t.Fatalf("unexpected token len: %d %d", len(tok1), len(tok2))
	}
	if tok1 == tok2 {
		t.Fatalf("expected different tokens, got identical value %q", tok1)
	}
}

func TestOutboxPublisherRuntimeMetricsSnapshot(t *testing.T) {
	p := &OutboxPublisher{}
	atomic.StoreInt64(&p.m.Runs, 7)
	atomic.StoreInt64(&p.m.Attempts, 13)
	atomic.StoreInt64(&p.m.Sent, 11)
	atomic.StoreInt64(&p.m.Retried, 2)
	atomic.StoreInt64(&p.m.DeadLettered, 1)
	atomic.StoreInt64(&p.m.LastRunUnix, 1700000000)

	got := p.RuntimeMetrics()
	if got.Runs != 7 || got.Attempts != 13 || got.Sent != 11 || got.Retried != 2 || got.DeadLettered != 1 || got.LastRunUnix != 1700000000 {
		t.Fatalf("unexpected metrics snapshot: %+v", got)
	}
}

func TestNewReplayJobServiceOwner(t *testing.T) {
	svc := NewReplayJobService(nil)
	if svc.owner == "" {
		t.Fatal("owner should not be empty")
	}
	if !strings.Contains(svc.owner, "-") {
		t.Fatalf("owner should contain host-time separator: %q", svc.owner)
	}
}

func TestOrderStatsServiceStartStopsOnContextCancel(t *testing.T) {
	svc := NewOrderStatsService(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		svc.Start(ctx, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expected stats service to stop after context cancellation")
	}
}

func TestOutboxPublisherRunOnceWithoutNATS(t *testing.T) {
	p := &OutboxPublisher{}
	if err := p.RunOnce(context.Background()); err != nil {
		t.Fatalf("run once without nats should be noop: %v", err)
	}
	got := p.RuntimeMetrics()
	if got.Runs != 1 {
		t.Fatalf("expected one run, got %+v", got)
	}
}

func TestOutboxPublisherStartStopsOnContextCancel(t *testing.T) {
	p := &OutboxPublisher{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Start(ctx, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expected outbox publisher to stop after context cancellation")
	}
}

func TestReplayJobServiceStartStopsOnContextCancel(t *testing.T) {
	svc := NewReplayJobService(nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		svc.Start(ctx, 10*time.Millisecond)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("expected replay job service to stop after context cancellation")
	}
}

func TestFormatErrWrapsOriginal(t *testing.T) {
	root := errors.New("root cause")
	err := FormatErr("wrap", root)
	if err == nil || !errors.Is(err, root) {
		t.Fatalf("expected wrapped error preserving root: %v", err)
	}
}

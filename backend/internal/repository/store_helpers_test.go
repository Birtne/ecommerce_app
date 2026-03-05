package repository

import (
	"testing"
	"time"
)

func TestMinInt32(t *testing.T) {
	if got := minInt32(1, 2); got != 1 {
		t.Fatalf("minInt32 mismatch: %d", got)
	}
	if got := minInt32(5, -1); got != -1 {
		t.Fatalf("minInt32 mismatch: %d", got)
	}
	if got := minInt32(3, 3); got != 3 {
		t.Fatalf("minInt32 mismatch: %d", got)
	}
}

func TestNormalizeOrderListFilterOffset(t *testing.T) {
	now := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	filter, offset := normalizeOrderListFilter(OrderListFilter{
		Page:     3,
		PageSize: 10,
		CursorAt: &now,
		CursorID: 42,
	})
	if offset != 0 {
		t.Fatalf("expected offset 0 with cursor, got %d", offset)
	}
	if filter.Page != 3 || filter.PageSize != 10 {
		t.Fatalf("filter should retain page settings with cursor, got page=%d size=%d", filter.Page, filter.PageSize)
	}

	_, offset = normalizeOrderListFilter(OrderListFilter{Page: 3, PageSize: 10})
	if offset != 20 {
		t.Fatalf("expected offset 20 for page 3 size 10, got %d", offset)
	}

	filter, offset = normalizeOrderListFilter(OrderListFilter{})
	if filter.Page != 1 || filter.PageSize != 20 || offset != 0 {
		t.Fatalf("unexpected defaults: page=%d size=%d offset=%d", filter.Page, filter.PageSize, offset)
	}
}

package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ductor/ecommerce_app/backend/internal/domain"
	"github.com/ductor/ecommerce_app/backend/internal/metrics"
	"github.com/ductor/ecommerce_app/backend/internal/repository"
)

type orderStoreStub struct {
	listOrdersFn         func(ctx context.Context, uid int64, f repository.OrderListFilter) ([]domain.Order, int64, error)
	cachedOrderTotalFn   func(ctx context.Context, uid int64, status string) (int64, error)
	listOrdersCalls      int
	cachedTotalCallCount int
}

func (s *orderStoreStub) CreateOrderFromCartWithIdempotency(ctx context.Context, uid int64, address, idemKey string) (*domain.Order, bool, error) {
	return nil, false, errors.New("unexpected call")
}

func (s *orderStoreStub) GetOrderDetail(ctx context.Context, uid, orderID int64) (*domain.OrderDetail, error) {
	return nil, errors.New("unexpected call")
}

func (s *orderStoreStub) ListOrders(ctx context.Context, uid int64, f repository.OrderListFilter) ([]domain.Order, int64, error) {
	s.listOrdersCalls++
	if s.listOrdersFn != nil {
		return s.listOrdersFn(ctx, uid, f)
	}
	return nil, -1, nil
}

func (s *orderStoreStub) GetCachedOrderTotal(ctx context.Context, uid int64, status string) (int64, error) {
	s.cachedTotalCallCount++
	if s.cachedOrderTotalFn != nil {
		return s.cachedOrderTotalFn(ctx, uid, status)
	}
	return 0, nil
}

func TestOrderServiceListOrdersSkipsCacheForTimeRange(t *testing.T) {
	metrics.Init()
	ctx := context.Background()
	stub := &orderStoreStub{}
	stub.listOrdersFn = func(ctx context.Context, uid int64, f repository.OrderListFilter) ([]domain.Order, int64, error) {
		if f.IncludeTotal {
			return nil, 10, nil
		}
		return nil, -1, nil
	}
	stub.cachedOrderTotalFn = func(ctx context.Context, uid int64, status string) (int64, error) {
		return 42, nil
	}

	svc := &OrderService{store: stub}
	if _, err := svc.ListOrders(ctx, 7, OrderListFilter{IncludeTotal: true, Page: 1, PageSize: 20}); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	if stub.cachedTotalCallCount != 1 {
		t.Fatalf("expected cached total lookup once, got %d", stub.cachedTotalCallCount)
	}

	from := time.Now().Add(-time.Hour)
	res, err := svc.ListOrders(ctx, 7, OrderListFilter{IncludeTotal: false, Page: 1, PageSize: 20, FromTime: &from})
	if err != nil {
		t.Fatalf("list with time range: %v", err)
	}
	if res.Total != -1 {
		t.Fatalf("expected total -1 without cache for time range, got %d", res.Total)
	}
	if stub.cachedTotalCallCount != 1 {
		t.Fatalf("expected no extra cached total lookup, got %d", stub.cachedTotalCallCount)
	}
}

func TestOrderServiceListOrdersSkipsCacheForCursor(t *testing.T) {
	metrics.Init()
	ctx := context.Background()
	stub := &orderStoreStub{}
	stub.listOrdersFn = func(ctx context.Context, uid int64, f repository.OrderListFilter) ([]domain.Order, int64, error) {
		if f.IncludeTotal {
			return nil, 12, nil
		}
		return nil, -1, nil
	}
	stub.cachedOrderTotalFn = func(ctx context.Context, uid int64, status string) (int64, error) {
		return 55, nil
	}

	svc := &OrderService{store: stub}
	if _, err := svc.ListOrders(ctx, 9, OrderListFilter{IncludeTotal: true, Page: 1, PageSize: 10}); err != nil {
		t.Fatalf("prime cache: %v", err)
	}
	if stub.cachedTotalCallCount != 1 {
		t.Fatalf("expected cached total lookup once, got %d", stub.cachedTotalCallCount)
	}

	cursorAt := time.Now().Add(-5 * time.Minute)
	res, err := svc.ListOrders(ctx, 9, OrderListFilter{IncludeTotal: false, Page: 1, PageSize: 10, CursorAt: &cursorAt, CursorID: 99})
	if err != nil {
		t.Fatalf("list with cursor: %v", err)
	}
	if res.Total != -1 {
		t.Fatalf("expected total -1 without cache for cursor, got %d", res.Total)
	}
	if stub.cachedTotalCallCount != 1 {
		t.Fatalf("expected no extra cached total lookup, got %d", stub.cachedTotalCallCount)
	}
}

func TestOrderServiceListOrdersSkipsCacheForOrderIDs(t *testing.T) {
	metrics.Init()
	ctx := context.Background()
	stub := &orderStoreStub{}
	stub.listOrdersFn = func(ctx context.Context, uid int64, f repository.OrderListFilter) ([]domain.Order, int64, error) {
		if f.IncludeTotal {
			return nil, 8, nil
		}
		return nil, -1, nil
	}

	svc := &OrderService{store: stub}
	res, err := svc.ListOrders(ctx, 7, OrderListFilter{IncludeTotal: true, Page: 1, PageSize: 20, OrderIDs: []int64{101, 202}})
	if err != nil {
		t.Fatalf("list with order ids: %v", err)
	}
	if res.Total != 8 {
		t.Fatalf("expected total 8, got %d", res.Total)
	}
	if stub.cachedTotalCallCount != 0 {
		t.Fatalf("expected no cached total lookup, got %d", stub.cachedTotalCallCount)
	}
}

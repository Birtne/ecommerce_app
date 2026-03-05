package http

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/ductor/ecommerce_app/backend/internal/domain"
	"github.com/ductor/ecommerce_app/backend/internal/service"
)

type stubOrderService struct {
	lastFilter service.OrderListFilter
	lastUID    int64
	items      []domain.Order
	total      int64
	err        error
}

func (s *stubOrderService) PlaceOrder(ctx context.Context, uid int64, address, idemKey string) (*domain.Order, bool, error) {
	s.lastUID = uid
	return nil, false, nil
}

func (s *stubOrderService) GetOrderDetail(ctx context.Context, uid, orderID int64) (*domain.OrderDetail, error) {
	s.lastUID = uid
	return &domain.OrderDetail{}, nil
}

func (s *stubOrderService) ListOrders(ctx context.Context, uid int64, f service.OrderListFilter) (*service.OrderListResult, error) {
	s.lastUID = uid
	s.lastFilter = f
	if s.err != nil {
		return nil, s.err
	}
	return &service.OrderListResult{Items: s.items, Total: s.total, Page: f.Page, PageSize: f.PageSize}, nil
}

func TestListOrdersIncludeTotalHandling(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	cursor := encodeOrderCursor(now, 55)

	cases := []struct {
		name             string
		query            string
		wantIncludeTotal bool
		wantCursor       bool
	}{
		{name: "default", query: "", wantIncludeTotal: true, wantCursor: false},
		{name: "explicit false", query: "include_total=no", wantIncludeTotal: false, wantCursor: false},
		{name: "explicit true", query: "include_total=on", wantIncludeTotal: true, wantCursor: false},
		{name: "cursor disables total", query: "cursor=" + cursor, wantIncludeTotal: false, wantCursor: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubOrderService{}
			h := &Handler{orderSvc: stub}
			c := app.NewContext(0)
			c.Set("user_id", int64(11))
			if tc.query != "" {
				c.Request.SetQueryString(tc.query)
			}

			h.listOrders(context.Background(), c)

			if got := c.Response.StatusCode(); got != 200 {
				t.Fatalf("status mismatch: got=%d want=200", got)
			}
			if stub.lastFilter.IncludeTotal != tc.wantIncludeTotal {
				t.Fatalf("include_total mismatch: got=%v want=%v", stub.lastFilter.IncludeTotal, tc.wantIncludeTotal)
			}
			if tc.wantCursor {
				if stub.lastFilter.CursorAt == nil {
					t.Fatal("expected cursor time to be set")
				}
				if stub.lastFilter.CursorID != 55 {
					t.Fatalf("expected cursor id 55, got %d", stub.lastFilter.CursorID)
				}
			} else if stub.lastFilter.CursorAt != nil {
				t.Fatalf("expected cursor to be empty, got %v", stub.lastFilter.CursorAt)
			}
		})
	}
}

func TestListOrdersStatusNormalization(t *testing.T) {
	stub := &stubOrderService{}
	h := &Handler{orderSvc: stub}
	c := app.NewContext(0)
	c.Set("user_id", int64(11))
	c.Request.SetQueryString("status=Created")

	h.listOrders(context.Background(), c)

	if got := c.Response.StatusCode(); got != 200 {
		t.Fatalf("status mismatch: got=%d want=200", got)
	}
	if stub.lastFilter.Status != "created" {
		t.Fatalf("status normalization mismatch: got=%q want=%q", stub.lastFilter.Status, "created")
	}
	if len(stub.lastFilter.Statuses) != 0 {
		t.Fatalf("expected no status list, got %v", stub.lastFilter.Statuses)
	}
}

func TestListOrdersStatusListParsing(t *testing.T) {
	stub := &stubOrderService{}
	h := &Handler{orderSvc: stub}
	c := app.NewContext(0)
	c.Set("user_id", int64(11))
	c.Request.SetQueryString("status=created,processing shipped")

	h.listOrders(context.Background(), c)

	if got := c.Response.StatusCode(); got != 200 {
		t.Fatalf("status mismatch: got=%d want=200", got)
	}
	want := []string{"created", "processing", "shipped"}
	if len(stub.lastFilter.Statuses) != len(want) {
		t.Fatalf("status list length mismatch: got=%v want=%v", stub.lastFilter.Statuses, want)
	}
	for i, status := range want {
		if stub.lastFilter.Statuses[i] != status {
			t.Fatalf("status list mismatch at %d: got=%v want=%v", i, stub.lastFilter.Statuses, want)
		}
	}
	if stub.lastFilter.Status != "" {
		t.Fatalf("expected status to be empty, got %q", stub.lastFilter.Status)
	}
}

func TestListOrdersOrderIDsParsing(t *testing.T) {
	stub := &stubOrderService{}
	h := &Handler{orderSvc: stub}
	c := app.NewContext(0)
	c.Set("user_id", int64(11))
	c.Request.SetQueryString("order_ids=101, 202 303")

	h.listOrders(context.Background(), c)

	if got := c.Response.StatusCode(); got != 200 {
		t.Fatalf("status mismatch: got=%d want=200", got)
	}
	want := []int64{101, 202, 303}
	if len(stub.lastFilter.OrderIDs) != len(want) {
		t.Fatalf("order_ids length mismatch: got=%v want=%v", stub.lastFilter.OrderIDs, want)
	}
	for i, id := range want {
		if stub.lastFilter.OrderIDs[i] != id {
			t.Fatalf("order_ids mismatch at %d: got=%v want=%v", i, stub.lastFilter.OrderIDs, want)
		}
	}
}

func TestListOrdersAmountRangeParsing(t *testing.T) {
	stub := &stubOrderService{}
	h := &Handler{orderSvc: stub}
	c := app.NewContext(0)
	c.Set("user_id", int64(11))
	c.Request.SetQueryString("min_amount=10.5&max_amount=99.9")

	h.listOrders(context.Background(), c)

	if got := c.Response.StatusCode(); got != 200 {
		t.Fatalf("status mismatch: got=%d want=200", got)
	}
	if stub.lastFilter.MinAmount == nil {
		t.Fatal("expected min_amount to be set")
	}
	if got := *stub.lastFilter.MinAmount; got != 10.5 {
		t.Fatalf("min_amount mismatch: got=%v want=%v", got, 10.5)
	}
	if stub.lastFilter.MaxAmount == nil {
		t.Fatal("expected max_amount to be set")
	}
	if got := *stub.lastFilter.MaxAmount; got != 99.9 {
		t.Fatalf("max_amount mismatch: got=%v want=%v", got, 99.9)
	}
}

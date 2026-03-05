package http

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
)

func TestOrderCursorRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	raw := encodeOrderCursor(now, 42)
	at, id, err := parseOrderCursor(raw)
	if err != nil {
		t.Fatalf("parse cursor: %v", err)
	}
	if at == nil || !at.Equal(now) {
		t.Fatalf("unexpected cursor time: got=%v want=%v", at, now)
	}
	if id != 42 {
		t.Fatalf("unexpected cursor id: %d", id)
	}
}

func TestParseOrderCursorInvalid(t *testing.T) {
	cases := []string{
		"!!!",
		base64.StdEncoding.EncodeToString([]byte("bad")),
		base64.StdEncoding.EncodeToString([]byte("x:y")),
		base64.StdEncoding.EncodeToString([]byte("0:1")),
		base64.StdEncoding.EncodeToString([]byte("1:0")),
		base64.StdEncoding.EncodeToString([]byte("-1:2")),
		base64.StdEncoding.EncodeToString([]byte("10:-2")),
	}
	for _, raw := range cases {
		if _, _, err := parseOrderCursor(raw); err == nil {
			t.Fatalf("expected parse error for %q", raw)
		}
	}
}

func TestParseBoolQueryParam(t *testing.T) {
	trueCases := []string{"1", "true", "TRUE", " yes ", "on"}
	falseCases := []string{"0", "false", "FALSE", " no ", "off"}
	for _, raw := range trueCases {
		val, ok := parseBoolQueryParam(raw)
		if !ok || !val {
			t.Fatalf("expected true for %q", raw)
		}
	}
	for _, raw := range falseCases {
		val, ok := parseBoolQueryParam(raw)
		if !ok || val {
			t.Fatalf("expected false for %q", raw)
		}
	}
	if _, ok := parseBoolQueryParam("maybe"); ok {
		t.Fatal("expected invalid parse for maybe")
	}
}

func TestParseOrderIDList(t *testing.T) {
	ids, err := parseOrderIDList("101, 202 303", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int64{101, 202, 303}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("order_ids mismatch: got=%v want=%v", ids, want)
	}

	ids, err = parseOrderIDList("101,101, 202", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want = []int64{101, 202}
	if !reflect.DeepEqual(ids, want) {
		t.Fatalf("order_ids dedupe mismatch: got=%v want=%v", ids, want)
	}

	if _, err := parseOrderIDList("bad", 0); !errors.Is(err, errInvalidOrderIDs) {
		t.Fatalf("expected invalid order_ids error, got %v", err)
	}
	if _, err := parseOrderIDList("1,2,3", 2); !errors.Is(err, errTooManyOrderIDs) {
		t.Fatalf("expected too many order_ids error, got %v", err)
	}
}

func TestParseOrderStatusList(t *testing.T) {
	statuses, err := parseOrderStatusList("Created, processing shipped", allowedOrderStatuses)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"created", "processing", "shipped"}
	if !reflect.DeepEqual(statuses, want) {
		t.Fatalf("status list mismatch: got=%v want=%v", statuses, want)
	}

	statuses, err = parseOrderStatusList("created,created", allowedOrderStatuses)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want = []string{"created"}
	if !reflect.DeepEqual(statuses, want) {
		t.Fatalf("status dedupe mismatch: got=%v want=%v", statuses, want)
	}

	if _, err := parseOrderStatusList("created,bad", allowedOrderStatuses); !errors.Is(err, errInvalidOrderStatus) {
		t.Fatalf("expected invalid status error, got %v", err)
	}
}

func TestContextHelperFunctions(t *testing.T) {
	c := app.NewContext(0)
	if _, err := userIDFromContext(c); err == nil {
		t.Fatal("expected missing user id error")
	}
	if _, err := adminUserIDFromContext(c); err == nil {
		t.Fatal("expected missing admin user id error")
	}
	if role := adminRoleFromContext(c); role != "" {
		t.Fatalf("expected empty role, got %q", role)
	}

	c.Set("user_id", int64(101))
	c.Set("admin_user_id", int64(201))
	c.Set("admin_role", "admin")
	c.Request.Header.Set("X-Command-Id", "  cmd-001  ")

	uid, err := userIDFromContext(c)
	if err != nil || uid != 101 {
		t.Fatalf("userIDFromContext mismatch uid=%d err=%v", uid, err)
	}
	auid, err := adminUserIDFromContext(c)
	if err != nil || auid != 201 {
		t.Fatalf("adminUserIDFromContext mismatch uid=%d err=%v", auid, err)
	}
	if role := adminRoleFromContext(c); role != "admin" {
		t.Fatalf("unexpected role %q", role)
	}
	if cid := commandIDFromHeader(c); cid != "cmd-001" {
		t.Fatalf("unexpected command id %q", cid)
	}
	c.Request.Header.Set("X-Trace-Id", " trace-001 ")
	if tid := traceIDFromHeader(c); tid != "trace-001" {
		t.Fatalf("unexpected trace id %q", tid)
	}
	c.Request.Header.Del("X-Trace-Id")
	c.Request.Header.Set("X-Request-Id", " req-002 ")
	if tid := traceIDFromHeader(c); tid != "req-002" {
		t.Fatalf("unexpected fallback trace id %q", tid)
	}
}

func TestMiddlewareMissingBearer(t *testing.T) {
	h := &Handler{}
	c1 := app.NewContext(0)
	h.authMiddleware(context.Background(), c1)
	if c1.Response.StatusCode() != 401 {
		t.Fatalf("auth middleware status=%d", c1.Response.StatusCode())
	}

	c2 := app.NewContext(0)
	h.adminSessionMiddleware(context.Background(), c2)
	if c2.Response.StatusCode() != 401 {
		t.Fatalf("admin middleware status=%d", c2.Response.StatusCode())
	}
}

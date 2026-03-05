package http

import (
	"strings"
	"testing"
)

func TestValidateCartItemInput(t *testing.T) {
	cases := []struct {
		name string
		pid  int64
		qty  int32
		ok   bool
	}{
		{name: "missing pid", pid: 0, qty: 1, ok: false},
		{name: "missing qty", pid: 1, qty: 0, ok: false},
		{name: "valid", pid: 2, qty: 3, ok: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateCartItemInput(tc.pid, tc.qty)
			if tc.ok && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	cases := []struct {
		name string
		key  string
		ok   bool
	}{
		{name: "empty", key: "", ok: false},
		{name: "has spaces", key: "idem key", ok: false},
		{name: "too long", key: strings.Repeat("a", maxIdempotencyKeyLen+1), ok: false},
		{name: "valid", key: "idem-001", ok: true},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := validateIdempotencyKey(tc.key)
			if tc.ok && err != nil {
				t.Fatalf("expected ok, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("expected error, got nil")
			}
		})
	}
}

func TestNormalizeAddress(t *testing.T) {
	address := " 123 Main St "
	trimmed, err := normalizeAddress(address)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trimmed != "123 Main St" {
		t.Fatalf("expected trimmed address, got %q", trimmed)
	}

	if _, err := normalizeAddress(" "); err == nil {
		t.Fatalf("expected error for empty address")
	}

	long := strings.Repeat("x", maxAddressLen+1)
	if _, err := normalizeAddress(long); err == nil {
		t.Fatalf("expected error for long address")
	}
}

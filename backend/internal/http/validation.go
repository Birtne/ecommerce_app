package http

import (
	"errors"
	"strconv"
	"strings"
	"unicode"
)

const (
	maxIdempotencyKeyLen = 64
	maxAddressLen        = 200
)

var (
	errInvalidOrderIDs    = errors.New("invalid order_ids")
	errTooManyOrderIDs    = errors.New("too many order_ids")
	errInvalidOrderStatus = errors.New("invalid status")
)

func validateIdempotencyKey(key string) error {
	if key == "" {
		return errors.New("missing Idempotency-Key header")
	}
	if len(key) > maxIdempotencyKeyLen {
		return errors.New("idempotency key too long")
	}
	if strings.ContainsAny(key, " \t\n\r") {
		return errors.New("idempotency key must not contain spaces")
	}
	return nil
}

func normalizeAddress(address string) (string, error) {
	trimmed := strings.TrimSpace(address)
	if trimmed == "" {
		return "", errors.New("address is required")
	}
	if len(trimmed) > maxAddressLen {
		return "", errors.New("address too long")
	}
	return trimmed, nil
}

func validateCartItemInput(pid int64, qty int32) error {
	if pid <= 0 {
		return errors.New("product_id must be > 0")
	}
	if qty <= 0 {
		return errors.New("quantity must be > 0")
	}
	return nil
}

func parseBoolQueryParam(raw string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, true
	case "0", "false", "no", "off":
		return false, true
	default:
		return false, false
	}
}

func parseOrderIDList(raw string, max int) ([]int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	if len(parts) == 0 {
		return nil, errInvalidOrderIDs
	}
	ids := make([]int64, 0, len(parts))
	seen := make(map[int64]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseInt(part, 10, 64)
		if err != nil || id <= 0 {
			return nil, errInvalidOrderIDs
		}
		if _, ok := seen[id]; ok {
			continue
		}
		ids = append(ids, id)
		seen[id] = struct{}{}
		if max > 0 && len(ids) > max {
			return nil, errTooManyOrderIDs
		}
	}
	if len(ids) == 0 {
		return nil, errInvalidOrderIDs
	}
	return ids, nil
}

func parseOrderStatusList(raw string, allowed map[string]struct{}) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.FieldsFunc(trimmed, func(r rune) bool {
		return r == ',' || unicode.IsSpace(r)
	})
	if len(parts) == 0 {
		return nil, errInvalidOrderStatus
	}
	statuses := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		normalized := strings.ToLower(part)
		if _, ok := allowed[normalized]; !ok {
			return nil, errInvalidOrderStatus
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		statuses = append(statuses, normalized)
		seen[normalized] = struct{}{}
	}
	if len(statuses) == 0 {
		return nil, errInvalidOrderStatus
	}
	return statuses, nil
}

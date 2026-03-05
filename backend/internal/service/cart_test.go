package service

import (
	"context"
	"testing"

	"github.com/ductor/ecommerce_app/backend/internal/repository"
)

func TestCartValidateQuantity(t *testing.T) {
	svc := NewCartService(&repository.Store{})
	if err := svc.AddOrUpdateItem(context.Background(), 1, 1, 0); err == nil {
		t.Fatalf("expected validation error")
	}
}

//go:build integration

package repository

import (
	"os"
	"testing"

	"github.com/ductor/ecommerce_app/backend/internal/testutil"
	"github.com/jackc/pgx/v4/pgxpool"
)

func setupStoreFixture(tb testing.TB, prefix string) (*Store, *pgxpool.Pool, func()) {
	tb.Helper()
	baseDSN := getenvFixture("TEST_POSTGRES_DSN", "postgres://ecom:ecom@localhost:5432/ecommerce?sslmode=disable")
	db, cleanup := testutil.SetupIsolatedDatabase(tb, baseDSN, prefix, "../../migrations")
	return NewStore(db), db, cleanup
}

func getenvFixture(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}

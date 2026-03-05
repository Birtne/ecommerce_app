//go:build integration

package integration

import (
	"os"
	"strings"
	"testing"

	"github.com/ductor/ecommerce_app/backend/internal/testutil"
	"github.com/jackc/pgx/v4/pgxpool"
)

func setupIsolatedPostgres(tb testing.TB, prefix string) (*pgxpool.Pool, func()) {
	tb.Helper()
	baseDSN := getenv("TEST_POSTGRES_DSN", "postgres://ecom:ecom@localhost:5432/ecommerce?sslmode=disable")
	return testutil.SetupIsolatedDatabase(tb, baseDSN, prefix, "../migrations")
}

func getenv(k, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return fallback
}

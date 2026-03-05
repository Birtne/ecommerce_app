//go:build integration

package testutil

import (
	"context"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ductor/ecommerce_app/backend/internal/migration"
	"github.com/jackc/pgx/v4/pgxpool"
)

var identPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func SetupIsolatedDatabase(tb testing.TB, baseDSN, prefix, migrationsDir string) (*pgxpool.Pool, func()) {
	tb.Helper()
	ctx := context.Background()
	dbName := fmt.Sprintf("%s_%d", sanitizeIdent(prefix), time.Now().UnixNano())
	if !identPattern.MatchString(dbName) {
		tb.Fatalf("invalid db identifier %q", dbName)
	}

	adminDSN, err := withDBName(baseDSN, "postgres")
	if err != nil {
		tb.Fatalf("build admin dsn: %v", err)
	}
	adminPool, err := pgxpool.Connect(ctx, adminDSN)
	if err != nil {
		tb.Fatalf("connect admin pg: %v", err)
	}

	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
		adminPool.Close()
		tb.Fatalf("create isolated db: %v", err)
	}

	testDSN, err := withDBName(baseDSN, dbName)
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
		adminPool.Close()
		tb.Fatalf("build test dsn: %v", err)
	}
	testPool, err := pgxpool.Connect(ctx, testDSN)
	if err != nil {
		_, _ = adminPool.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
		adminPool.Close()
		tb.Fatalf("connect isolated db: %v", err)
	}
	if err := migration.Run(ctx, testPool, filepath.Clean(migrationsDir)); err != nil {
		testPool.Close()
		_, _ = adminPool.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
		adminPool.Close()
		tb.Fatalf("migrate isolated db: %v", err)
	}

	cleanup := func() {
		testPool.Close()
		_, _ = adminPool.Exec(ctx, `
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname=$1 AND pid <> pg_backend_pid()`, dbName)
		_, _ = adminPool.Exec(ctx, "DROP DATABASE IF EXISTS "+dbName)
		adminPool.Close()
	}
	return testPool, cleanup
}

func withDBName(baseDSN, dbName string) (string, error) {
	u, err := url.Parse(baseDSN)
	if err != nil {
		return "", err
	}
	u.Path = "/" + dbName
	return u.String(), nil
}

func sanitizeIdent(in string) string {
	out := strings.ToLower(strings.ReplaceAll(in, "-", "_"))
	if out == "" {
		out = "itest"
	}
	if out[0] >= '0' && out[0] <= '9' {
		out = "i_" + out
	}
	var b strings.Builder
	for _, r := range out {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	clean := b.String()
	if clean == "" {
		return "itest"
	}
	return clean
}

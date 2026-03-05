//go:build integration

package integration

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func seedBenchmarkOrders(b *testing.B, db *pgxpool.Pool) {
	ctx := context.Background()
	if _, err := db.Exec(ctx, `INSERT INTO users(id,email,password_hash,name,role) VALUES (1,'bench@test.com','x','bench','customer')`); err != nil {
		b.Fatalf("seed user: %v", err)
	}
	for i := 0; i < 5000; i++ {
		if _, err := db.Exec(ctx, `INSERT INTO orders(user_id,address,amount,status,created_at) VALUES (1,$1,100.0,'created',$2)`, fmt.Sprintf("A-%d", i), time.Now().Add(-time.Duration(i)*time.Second)); err != nil {
			b.Fatalf("seed orders: %v", err)
		}
	}
	if _, err := db.Exec(ctx, `ANALYZE orders`); err != nil {
		b.Fatalf("analyze orders: %v", err)
	}
}

func BenchmarkIntegration_OrderHistoryQuery(b *testing.B) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(b, "bench_order_history")
	defer cleanup()
	seedBenchmarkOrders(b, db)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query(ctx, `
SELECT id,user_id,address,amount,status,created_at
FROM orders
WHERE user_id=$1
ORDER BY created_at DESC, id DESC
LIMIT 20`, 1)
		if err != nil {
			b.Fatalf("query: %v", err)
		}
		for rows.Next() {
			var (
				id        int64
				uid       int64
				address   string
				amount    float64
				status    string
				createdAt time.Time
			)
			if err := rows.Scan(&id, &uid, &address, &amount, &status, &createdAt); err != nil {
				b.Fatalf("scan: %v", err)
			}
		}
		rows.Close()
	}
}

func BenchmarkIntegration_OrderHistoryQueryParallel(b *testing.B) {
	ctx := context.Background()
	db, cleanup := setupIsolatedPostgres(b, "bench_order_history_parallel")
	defer cleanup()
	seedBenchmarkOrders(b, db)

	var uidSeed uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			uid := atomic.AddUint64(&uidSeed, 1)
			rows, err := db.Query(ctx, `
SELECT id,user_id,address,amount,status,created_at
FROM orders
WHERE user_id=$1
ORDER BY created_at DESC, id DESC
LIMIT 20`, 1)
			if err != nil {
				b.Fatalf("parallel query[%d]: %v", uid, err)
			}
			for rows.Next() {
				var (
					id        int64
					userID    int64
					address   string
					amount    float64
					status    string
					createdAt time.Time
				)
				if err := rows.Scan(&id, &userID, &address, &amount, &status, &createdAt); err != nil {
					b.Fatalf("parallel scan[%d]: %v", uid, err)
				}
			}
			rows.Close()
		}
	})
}

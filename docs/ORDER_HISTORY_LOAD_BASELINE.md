# Order History Load Baseline (iter-07)

Benchmark command:
```bash
cd ecommerce_app/backend
TEST_POSTGRES_DSN='postgres://ecom:ecom@localhost:5432/ecommerce?sslmode=disable' \
go test -tags=integration ./integration -run '^$' -bench BenchmarkIntegration_OrderHistoryQuery -benchtime=2s -count=1
```

Result snapshot (2026-03-04):
```text
BenchmarkIntegration_OrderHistoryQuery-2            9351    117840 ns/op
BenchmarkIntegration_OrderHistoryQueryParallel-2   11247     98585 ns/op
```

Notes:
- Dataset seeded with 5,000 orders for one user.
- Integration/benchmark now runs on per-job isolated databases to avoid concurrent CI interference.
- Query shape: `WHERE user_id = ? ORDER BY created_at DESC, id DESC LIMIT 20`.
- Supports cursor consistency with `(created_at, id)` keyset.
- Total count strategy is split:
  - cursor mode defaults to `include_total=false`;
  - total can be served from async summary table `user_order_totals` refreshed by background worker.

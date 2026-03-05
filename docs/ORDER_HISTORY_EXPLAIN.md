# Order History Query Explain Benchmark (iter-06)

Environment:
- PostgreSQL 16 (docker compose)
- Query path: `GET /api/v1/orders` keyset ordering by `(created_at DESC, id DESC)`
- Indexes:
  - `idx_orders_user_status_created_id`
  - `idx_orders_user_created_id`

Benchmark SQL:
```sql
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT id,user_id,address,amount,status,created_at
FROM orders
WHERE user_id = 1
ORDER BY created_at DESC, id DESC
LIMIT 20;
```

Sample output (integration run, 2026-03-04):
```text
Limit  (cost=11.31..11.31 rows=3 width=104) (actual time=0.012..0.013 rows=8 loops=1)
  Buffers: shared hit=2
  ->  Sort  (cost=11.31..11.31 rows=3 width=104) (actual time=0.011..0.012 rows=8 loops=1)
        Sort Key: created_at DESC, id DESC
        Sort Method: quicksort  Memory: 25kB
        Buffers: shared hit=2
        ->  Bitmap Heap Scan on orders  (cost=4.17..11.28 rows=3 width=104) (actual time=0.004..0.005 rows=8 loops=1)
              Recheck Cond: (user_id = '1'::bigint)
              Heap Blocks: exact=1
              Buffers: shared hit=2
              ->  Bitmap Index Scan on idx_orders_user_created_id  (cost=0.00..4.17 rows=3 width=0) (actual time=0.003..0.003 rows=8 loops=1)
                    Index Cond: (user_id = '1'::bigint)
                    Buffers: shared hit=1
Planning Time: 0.032 ms
Execution Time: 0.038 ms
```

Notes:
- Planner already uses `idx_orders_user_created_id` for user filter.
- With larger cardinality, keyset cursor (`cursor` query param) avoids offset drift and duplicate/skip behavior under concurrent writes.

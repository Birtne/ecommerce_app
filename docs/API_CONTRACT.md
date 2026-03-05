# API Contract (iter-06)

Base URL: `http://localhost:8888/api/v1`

## Auth

### POST `/auth/register`
Request:
```json
{"email":"user@test.com","password":"123456","name":"Alice"}
```
Response:
```json
{"user_id":1,"token":"tok_xxx"}
```

### POST `/auth/login`
Request:
```json
{"email":"user@test.com","password":"123456"}
```
Response:
```json
{"user_id":1,"token":"tok_xxx"}
```

## Product

### GET `/products`
Response:
```json
[{"id":1,"title":"Wireless Earbuds Pro","price":199,"stock":120}]
```

## Cart (Bearer token)

### GET `/cart`
```json
{"items":[{"product_id":1,"title":"Wireless Earbuds Pro","quantity":2,"price":199}],"total_amount":398}
```

### POST `/cart/items`
```json
{"product_id":1,"quantity":2}
```

### DELETE `/cart/items/:product_id`
```json
{"ok":true}
```

## Orders (Bearer token)

### POST `/orders`
Headers:
- `Idempotency-Key: <string>` (required, max 64, no spaces)

Request:
```json
{"address":"Shanghai Pudong Road 1"}
```

Response:
```json
{"order_id":1001,"user_id":1,"address":"Shanghai Pudong Road 1","amount":398,"status":"created","idempotent_replay":false}
```

#### 订单状态说明
- 当前后端固定返回 `created`。
- 其余状态（`processing`/`shipped`/`completed`/`cancelled`/`failed`）为前端预留展示，尚未由后端产生。

### GET `/orders`
Query:
- `status` optional, allowed: `created`, `processing`, `shipped`, `completed`, `cancelled`, `failed` (case-insensitive, supports comma/space-separated list; duplicates ignored)
- `order_ids` optional, comma/space separated positive integers (max 50; duplicates ignored)
- `from` optional RFC3339
- `to` optional RFC3339
- `from` and `to` must satisfy `from <= to` when both provided
- `min_amount` optional, non-negative number (filters orders with amount >= min_amount)
- `max_amount` optional, non-negative number (filters orders with amount <= max_amount)
- `min_amount` and `max_amount` must satisfy `min_amount <= max_amount` when both provided
- `page` optional (default `1`, must be `>= 1`)
- `page_size` optional (default `20`, must be `1-100`)
- `cursor` optional (opaque base64 cursor for consistent keyset pagination; decodes to positive unix-nano + order id; cursor mode ignores offset pagination and treats `page` as informational)
- `include_total` optional (`true/false`, `yes/no`, `on/off`, or `1/0`), default: `false` when cursor mode, otherwise `true` (other values return 400; cursor mode rejects `include_total=true`)
- `total` 当不计算总数时返回 `-1`（`include_total=false` 或使用 `cursor`/时间范围/`order_ids`）。对无过滤查询，服务端可能返回缓存总数。

Response:
```json
{
  "items":[{"order_id":1001,"user_id":1,"address":"A","amount":398,"status":"created","item_count":2,"created_at":"2026-03-04T03:00:00Z"}],
  "page":1,
  "page_size":20,
  "total":1,
  "next_cursor":"MTcwOTUy..."
}
```

- `total` 为 `-1` 表示未计算总数。
- `item_count` 为订单内商品件数（数量求和）。

### GET `/orders/:order_id`
```json
{
  "order_id":1001,
  "user_id":1,
  "address":"A",
  "amount":398,
  "status":"created",
  "item_count":2,
  "created_at":"2026-03-04T03:00:00Z",
  "items":[{"product_id":1,"title":"Wireless Earbuds Pro","price":199,"quantity":2,"subtotal":398}],
  "idempotency_key":"web-123",
  "idempotency_created_at":"2026-03-04T03:00:01Z"
}
```

## Admin Session / Replay Jobs

### POST `/admin/auth/login`
Request:
```json
{"email":"admin@test.com","password":"admin123"}
```
Response:
```json
{"admin_user_id":1,"token":"tok_admin_session"}
```

All admin endpoints require `Authorization: Bearer <admin-session-token>`.

### POST `/admin/auth/logout`
Header:
- `Authorization: Bearer <admin-session-token>`

Response:
```json
{"ok":true}
```

### POST `/admin/outbox/replay-jobs`
Headers:
- `Authorization: Bearer <admin-session-token>`
- `X-Command-Id: <client-generated-id>` (required for idempotent command replay)

Request:
```json
{"limit":50,"topic":"ecom.order.created"}
```
Response (`202 Accepted`):
```json
{"job_id":12,"queued_items":7}
```

### GET `/admin/outbox/replay-jobs/:job_id`
```json
{
  "job":{"job_id":12,"status":"running","topic_filter":"ecom.order.created","total_items":7,"processed_items":3,"success_items":2,"failed_items":1,"last_error":"...","created_at":"...","updated_at":"..."},
  "failed_groups":[{"error_group":"duplicate key value","count":1}]
}
```

### POST `/admin/outbox/replay-jobs/:job_id/retry-failed`
Headers:
- `Authorization: Bearer <admin-session-token>`
- `X-Command-Id: <client-generated-id>` (required for idempotent command replay)

Request:
```json
{"error_group":"duplicate key value","limit":100,"reset_attempts":true}
```
Response:
```json
{
  "job_id":12,
  "retried":1,
  "reset_attempts":true,
  "attempts_before_total":6,
  "attempts_after_total":0,
  "error_groups_before":{"publish timeout":1},
  "error_groups_after":{"":1}
}
```

字段说明：
- `reset_attempts=true`（默认）: 将失败项 `attempts` 重置为 `0`，并重置 backoff（`next_attempt_at=NOW()`）。
- `reset_attempts=false`: 保留现有 `attempts`，仅重置状态与调度窗口。
- `attempts_before_total`/`attempts_after_total`: 本次重试选中失败项的 attempts 聚合前后值。
- `error_groups_before`/`error_groups_after`: 本次重试选中失败项的失败分组聚合变化。
- 使用同一 `X-Command-Id` 重放请求时返回相同摘要（幂等回放）。

### GET `/admin/trace/replay-jobs`
Query:
- `trace_id` optional
- `command_id` optional
- `limit` optional (default `20`, max `200`)
- `cursor_id` optional (id-desc 游标，翻页时传上一页 `next_cursor_id`)
- `trace_id` 与 `command_id` 至少提供一个
- `cursor_id` 非法时返回 `400`。

Response:
```json
{"items":[{"job_id":12,"status":"completed","topic_filter":"ecom.order.created","trace_id":"trace-001","command_id":"cmd-001","total_items":7,"processed_items":7,"success_items":7,"failed_items":0,"last_error":"","created_at":"...","updated_at":"..."}],"count":1,"next_cursor_id":12}
```

### GET `/admin/trace/outbox-events`
Query:
- `trace_id` optional
- `command_id` optional
- `limit` optional (default `20`, max `200`)
- `cursor_id` optional (id-desc 游标，翻页时传上一页 `next_cursor_id`)
- `trace_id` 与 `command_id` 至少提供一个
- `cursor_id` 非法时返回 `400`。

Response:
```json
{"items":[{"id":101,"topic":"ecom.order.created","status":"pending","retry_count":0,"max_retries":6,"trace_id":"trace-001","command_id":"cmd-001","correlation_source":"replay_job","replay_job_id":12,"dead_letter_id":88,"created_at":"...","updated_at":"..."}],"count":1,"next_cursor_id":101}
```

### GET `/admin/audit-logs`
Query:
- `action` optional
- `q` optional full-text search
- `limit` optional, default `50`, max `200`
- `cursor_id` optional (for id-desc cursor pagination)

Response:
```json
{"items":[{"id":10,"actor_user_id":1,"action":"replay_job_create","target_type":"replay_job","target_id":"12","payload":"{\"limit\":50}","created_at":"2026-03-04T00:00:00Z"}],"next_cursor_id":10}
```

### GET `/admin/audit-logs/export`
Query:
- `action` optional
- `q` optional full-text search

Response:
- `text/csv` attachment (`audit_logs.csv`)

## Health / Metrics

### GET `/health/outbox`
```json
{"db_stats":{"pending":0,"sent":1,"failed":0,"dead_letter":0,"total_dead_letter_events":0,"total_retries":0},"runtime_stats":{"runs":1,"attempts":1,"sent":1,"retried":0,"dead_lettered":0,"last_run_unix":1700000000}}
```

### GET `/metrics`
Prometheus text exposition, includes:
- `ecommerce_order_place_requests_total{result=...,replay=...}`
- `ecommerce_order_placed_total{replay=...}`
- `ecommerce_order_place_duration_seconds_*`
- `ecommerce_outbox_publish_total{topic=...,result=...}`
- `ecommerce_outbox_publish_duration_seconds_*`
- `ecommerce_outbox_events{status=...}`
- `ecommerce_outbox_runtime{metric="oldest_pending_age_seconds"}`

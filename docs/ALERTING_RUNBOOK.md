# Alerting Runbook (iter-07)

## Rule Files
- `infra/monitoring/prometheus-rules.yaml`

## Severity Mapping
- `warning`: send to on-duty chat channel, 30-minute acknowledgement target.
- `critical`: page on-call immediately, 10-minute acknowledgement target.

## Escalation Flow
1. `EcommerceOrderSuccessRateLow` (`warning`):
   - Verify `/metrics` endpoint health.
   - Check recent deployment and auth/order error spikes.
2. `EcommerceOutboxDelayHigh` (`warning`):
   - Check NATS connectivity and DB latency.
   - Verify replay jobs/backoff queue growth.
3. `EcommerceDLQBacklogHigh` (`critical`):
   - Trigger admin replay job by topic.
   - Inspect failed groups via `GET /api/v1/admin/outbox/replay-jobs/:job_id`.
   - If repeated same error group, apply targeted fix then `retry-failed`.

## Suggested Alertmanager Routing
- `service=ecommerce-backend,severity=critical` -> pager receiver.
- `service=ecommerce-backend,severity=warning` -> team IM receiver.

## Admin Command Wait Alerts
1. `EcommerceAdminCommandWaitTimeoutSpike` (`warning`):
   - Query backend logs with `component=admin_command_wait` and `wait_result=timeout`.
   - Correlate by `trace_id` / `command_id` and check `admin_command_idempotency` lock contention.
2. `EcommerceAdminCommandWaitCanceledSpike` (`warning`):
   - Query backend logs with `wait_result=canceled`.
   - Confirm upstream timeout/cancel behavior and duplicate admin retries.

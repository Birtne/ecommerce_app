# Monitoring Setup (iter-08)

## Files
- Prometheus rules: `infra/monitoring/prometheus-rules.yaml`
- Alertmanager sample route: `infra/monitoring/alertmanager/alertmanager.sample.yml`
- Grafana datasource provisioning: `infra/monitoring/grafana/provisioning/datasources/prometheus.yaml`
- Grafana dashboard provisioning: `infra/monitoring/grafana/provisioning/dashboards/ecommerce.yaml`
- Grafana dashboard JSON: `infra/monitoring/grafana/dashboards/ecommerce-backend-slo.json`

## Expected Container Mounts
- Mount `infra/monitoring/grafana/provisioning` to `/etc/grafana/provisioning`
- Mount `infra/monitoring/grafana/dashboards` to `/var/lib/grafana/dashboards`
- Mount `infra/monitoring/prometheus-rules.yaml` into Prometheus rule path
- Mount `infra/monitoring/alertmanager/alertmanager.sample.yml` as Alertmanager config baseline

## Alerting Linkage
- Rule severities: `warning` and `critical`
- Recommended routing is defined in Alertmanager sample config
- Operational procedure template: `docs/ONCALL_TEMPLATE.md`

## Drill Runbook (Grafana + Alertmanager)
1. Start stack and verify metric endpoint:
   - `curl -s http://localhost:8080/metrics | grep ecommerce_outbox_events`
2. Open Grafana dashboard `Ecommerce Backend SLO` and validate:
   - Panels for order success rate, outbox latency, DLQ backlog are non-empty.
   - Panel time range set to last 15m and datasource points to Prometheus.
3. Trigger warning alert drill (non-destructive):
   - Temporarily publish a pending outbox backlog by stopping NATS and placing orders.
   - Confirm Prometheus rule enters `pending` then `firing`.
4. Verify Alertmanager route behavior using sample config:
   - `amtool --alertmanager.url=http://localhost:9093 config routes test severity=warning service=ecommerce-backend`
   - `amtool --alertmanager.url=http://localhost:9093 config routes test severity=critical service=ecommerce-backend`
5. Recovery validation:
   - Restart NATS, run publisher loop, ensure backlog panel drops and alert resolves.
6. Evidence checklist:
   - Grafana panel screenshot timestamp.
   - Alertmanager route test outputs (warning + critical).
   - Resolved alert timestamp and incident ticket link.

## Admin Command Wait Metrics
- `ecommerce_admin_command_wait_total{action,result}`: counter of admin command idempotency waits.
- `ecommerce_admin_command_wait_duration_seconds{action,result}`: wait latency histogram.
- Typical results: `ok`, `timeout`, `error`, `canceled`.
- Structured logs: `component=admin_command_wait` with `trace_id`, `command_id`, `wait_result`, `wait_ms`.
- Replay/outbox correlation logs include `trace_id`, `command_id`, `replay_job_id`, `dead_letter_id`, `correlation_source` for end-to-end trace joins.
- Alert rules:
  - `EcommerceAdminCommandWaitTimeoutSpike`
  - `EcommerceAdminCommandWaitCanceledSpike`

### Admin Command Wait Tuning
- `ADMIN_COMMAND_WAIT_TIMEOUT_MS` (default 5000, min 200).
- `ADMIN_COMMAND_WAIT_INITIAL_BACKOFF_MS` (default 20, min 5).
- `ADMIN_COMMAND_WAIT_MAX_BACKOFF_MS` (default 320, min 20).

## Replay Soak Reporting
- Integration test `replay_admin_concurrency_integration_test.go` writes `replay-soak-report.json`/`.csv` when `REPLAY_SOAK_REPORT_DIR` is set.
- Baseline snapshot lives at `docs/REPLAY_SOAK_BASELINE.json` for regression comparison.
- Layered gate thresholds are isolated at `docs/REPLAY_SOAK_THRESHOLDS.json`.
- Threshold scenario template: `docs/REPLAY_SOAK_THRESHOLDS.template.json`.
- CI writeback draft artifacts (no auto-write to repo):
  - `/tmp/ecom-ci/trend/replay-soak-thresholds-draft.json`
  - `/tmp/ecom-ci/trend/replay-soak-thresholds-draft.md`
  - `/tmp/ecom-ci/trend/replay-soak-thresholds-pr-summary.md` (PR summary compare template)

## Cross-Job Scoring Configuration
- Versioned scoring config: `docs/CROSS_JOB_SCORING_CONFIG.json`
- Supports:
  - environment profile overlay (`profiles.ci`, `profiles.nightly`, ...)
  - branch regex override (`branch_overrides[].pattern`)
- CI/Nightly gate should pass:
  - `--config .../CROSS_JOB_SCORING_CONFIG.json`
  - `--env <ci|nightly>`
  - `--branch <GITHUB_REF_NAME>`
- Summary output includes threshold compare explanation via `threshold_explain`:
  - profile/branch override match
  - defaults penalty vs effective penalty
  - config version/path and legacy migration flag
- Migration compatibility checker:
  - `scripts/cross_job_config_migration_check.py`
  - validates legacy config shape is still loadable and produces compatible domain scoring output

## Automated Drill Validation Script
- Script path: `scripts/monitoring_drill_check.sh`
- Purpose:
  - validate metrics endpoint availability;
  - trigger synthetic alert to Alertmanager;
  - verify route matching (via `amtool` when available, else Alertmanager groups API);
  - resolve alert and report recovery latency seconds.
- Modes:
  - `DRILL_MODE=mock`: start embedded mock Alertmanager+metrics service (CI-friendly, no external dependency).
  - `DRILL_MODE=container`: start local Alertmanager container with sample config + metrics mock endpoint.
  - `DRILL_MODE=external`: use externally provided `METRICS_URL` and `ALERTMANAGER_URL`.
- Scenarios (`DRILL_SCENARIO`):
  - `success`: expected pass path, includes recovery latency assertion.
  - `missing_metric`: expected fail path, asserts missing metrics are detected.
  - `route_miss`: expected fail path, asserts route mismatch detection.
  - `recovery_timeout`: expected fail path, asserts unresolved alert timeout detection.
- Example:
```bash
cd ecommerce_app
DRILL_MODE=mock MAX_RECOVERY_SECONDS=20 ./scripts/monitoring_drill_check.sh
```
- CI failure assertion examples:
```bash
cd ecommerce_app
DRILL_MODE=mock DRILL_SCENARIO=missing_metric ./scripts/monitoring_drill_check.sh
DRILL_MODE=mock DRILL_SCENARIO=route_miss ./scripts/monitoring_drill_check.sh
DRILL_MODE=mock DRILL_SCENARIO=recovery_timeout MAX_RECOVERY_SECONDS=2 ./scripts/monitoring_drill_check.sh
```
- Container mode failure assertion examples:
```bash
cd ecommerce_app
DRILL_MODE=container DRILL_SCENARIO=missing_metric ./scripts/monitoring_drill_check.sh
DRILL_MODE=container DRILL_SCENARIO=route_miss ./scripts/monitoring_drill_check.sh
```
- Nightly external smoke (optional, ENV gated):
  - Workflow: `.github/workflows/nightly-monitoring-drill.yml`
  - Enable with workflow_dispatch input `enable_external_smoke=true` or repo variable `ECOM_EXTERNAL_SMOKE_ENABLED=true`.
  - Provide URLs via dispatch inputs or secrets:
    - `ECOM_EXTERNAL_METRICS_URL`
    - `ECOM_EXTERNAL_ALERTMANAGER_URL`
  - Trend delta/escalation artifact:
    - `/tmp/ecom-nightly/history/external-smoke-trend-analysis.json`
  - Cross-job gate artifact (nightly + linked CI signal):
    - `/tmp/ecom-nightly/history/cross-job-signal-nightly.json`
  - Cross-job gate history:
    - `/tmp/ecom-nightly/history/cross-job-history-nightly.json`
  - Escalation state cache:
    - `/tmp/ecom-nightly/history/external-smoke-escalation-state.json`
  - Aggregated health scoring fields:
    - `health_score` (0-100)
    - `aggregate_penalty`
    - `penalties` (细粒度扣分来源: consecutive/latest_exhausted/ci_linked)
    - `denoise_notes` (自动降噪说明)
    - `ci_signal_status` / `ci_signal_level` (CI 联动信号状态)
  - Continuous escalation controls:
    - `EXTERNAL_SMOKE_ESCALATION_CONSECUTIVE_N` (default `3`)
    - `EXTERNAL_SMOKE_ESCALATION_SUPPRESS_RUNS` (default `2`)
  - Cross-job trend degradation:
    - 连续恶化通过 `trend.consecutive_worsening` 与 `trend.consecutive_threshold` 判定
    - 达到阈值时提升 cross-job `overall_level`（warning/critical）
  - 分域评分:
    - `domain_levels`: `coverage/bench/soak/nightly`
    - `domain_scores`: 各域 0-100
    - `total_score`: 分域聚合总分
- Output is one-line JSON:
  - success mode: `alert`, `trigger_unix`, `recovery_latency_seconds`
  - failure assertion mode: `scenario`, `expected_failure`, `assertion`, `reason`

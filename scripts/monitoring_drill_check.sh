#!/usr/bin/env bash
set -euo pipefail

DRILL_MODE="${DRILL_MODE:-mock}" # mock|container|external
DRILL_SCENARIO="${DRILL_SCENARIO:-success}" # success|missing_metric|route_miss|recovery_timeout
METRICS_URL="${METRICS_URL:-http://localhost:8080/metrics}"
ALERTMANAGER_URL="${ALERTMANAGER_URL:-http://localhost:9093}"
ALERTMANAGER_CONFIG="${ALERTMANAGER_CONFIG:-infra/monitoring/alertmanager/alertmanager.sample.yml}"

ALERT_NAME="${ALERT_NAME:-DrillSyntheticEcommerceAlert}"
SERVICE_LABEL="${SERVICE_LABEL:-ecommerce-backend}"
SEVERITY_LABEL="${SEVERITY_LABEL:-warning}"
EXPECTED_RECEIVER="${EXPECTED_RECEIVER:-default-chat}"
MAX_RECOVERY_SECONDS="${MAX_RECOVERY_SECONDS:-60}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"
POLL_TIMEOUT_SECONDS="${POLL_TIMEOUT_SECONDS:-60}"

MOCK_PORT="${MOCK_PORT:-19093}"
MOCK_RECEIVER="${MOCK_RECEIVER:-default-chat}"
MOCK_METRICS_PROFILE="${MOCK_METRICS_PROFILE:-ok}" # ok|missing
MOCK_RESOLVE_DELAY_SECONDS="${MOCK_RESOLVE_DELAY_SECONDS:-0}"
METRICS_MOCK_PROFILE="${METRICS_MOCK_PROFILE:-ok}" # ok|missing, for container mode metrics mock
FORCE_ROUTE_MISS="${FORCE_ROUTE_MISS:-0}"

MOCK_PID=""
METRICS_MOCK_PID=""
ALERTMANAGER_CONTAINER_NAME="ecom-alertmanager-drill"
FAIL_REASON=""
TRIGGER_TS="0"
RECOVERY_LATENCY="0"

log() {
  printf '[monitoring-drill] %s\n' "$*"
}

fail() {
  FAIL_REASON="$1"
  log "failure: $2"
  return 1
}

cleanup() {
  if [[ -n "${MOCK_PID}" ]]; then
    kill "${MOCK_PID}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${METRICS_MOCK_PID}" ]]; then
    kill "${METRICS_MOCK_PID}" >/dev/null 2>&1 || true
  fi
  if [[ "${DRILL_MODE}" == "container" ]]; then
    docker rm -f "${ALERTMANAGER_CONTAINER_NAME}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

now_rfc3339() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

assert_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    log "required command not found: $1"
    exit 1
  fi
}

start_mock_server() {
  local tmp
  tmp="$(mktemp /tmp/monitoring-drill-mock.XXXXXX.py)"
  cat >"${tmp}" <<'PY'
import json
import os
import urllib.parse
from datetime import datetime, timezone, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

alerts = {}
receiver = os.environ.get("MOCK_RECEIVER", "default-chat")
metrics_profile = os.environ.get("MOCK_METRICS_PROFILE", "ok")
resolve_delay_seconds = int(os.environ.get("MOCK_RESOLVE_DELAY_SECONDS", "0"))

def now():
    return datetime.now(timezone.utc)

def parse_ts(v):
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return now()

def is_active(obj):
    expires = parse_ts(obj.get("endsAt", "")) + timedelta(seconds=resolve_delay_seconds)
    return expires > now()

class Handler(BaseHTTPRequestHandler):
    def _write(self, code, body, ctype="application/json"):
        raw = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/metrics":
            rows = [
                "ecommerce_outbox_events{status=\"pending\"} 0",
                "ecommerce_order_place_requests_total{result=\"ok\",replay=\"false\"} 1",
            ]
            if metrics_profile != "missing":
                rows.append("ecommerce_outbox_runtime{metric=\"runs\"} 1")
            body = "\n".join(rows) + "\n"
            self._write(200, body, "text/plain; version=0.0.4; charset=utf-8")
            return
        if parsed.path == "/api/v2/alerts":
            qs = urllib.parse.parse_qs(parsed.query)
            flt = qs.get("filter", [""])[0]
            want = ""
            if flt.startswith("alertname="):
                want = flt.split("=", 1)[1]
            out = []
            for name, obj in list(alerts.items()):
                if not is_active(obj):
                    continue
                if want and name != want:
                    continue
                out.append(obj)
            self._write(200, json.dumps(out))
            return
        if parsed.path == "/api/v2/alerts/groups":
            active = []
            for _, obj in list(alerts.items()):
                if is_active(obj):
                    active.append(obj)
            self._write(200, json.dumps([{"receiver": receiver, "alerts": active}]))
            return
        self._write(404, json.dumps({"error": "not found"}))

    def do_POST(self):
        if self.path != "/api/v2/alerts":
            self._write(404, json.dumps({"error": "not found"}))
            return
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length).decode("utf-8")
        payload = json.loads(raw) if raw else []
        for item in payload:
            labels = item.get("labels", {})
            name = labels.get("alertname", "")
            if not name:
                continue
            alerts[name] = item
        self._write(200, "[]")

HTTPServer(("127.0.0.1", int(os.environ.get("MOCK_PORT", "19093"))), Handler).serve_forever()
PY
  MOCK_PORT="${MOCK_PORT}" MOCK_RECEIVER="${MOCK_RECEIVER}" MOCK_METRICS_PROFILE="${MOCK_METRICS_PROFILE}" MOCK_RESOLVE_DELAY_SECONDS="${MOCK_RESOLVE_DELAY_SECONDS}" python3 "${tmp}" >/tmp/monitoring-drill-mock.log 2>&1 &
  MOCK_PID=$!
  sleep 1
  METRICS_URL="http://127.0.0.1:${MOCK_PORT}/metrics"
  ALERTMANAGER_URL="http://127.0.0.1:${MOCK_PORT}"
}

start_metrics_mock() {
  local tmp
  tmp="$(mktemp /tmp/monitoring-metrics-mock.XXXXXX.py)"
  cat >"${tmp}" <<'PY'
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

metrics_profile = os.environ.get("METRICS_MOCK_PROFILE", "ok")

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/metrics":
            rows = [
                "ecommerce_outbox_events{status=\"pending\"} 0",
                "ecommerce_order_place_requests_total{result=\"ok\",replay=\"false\"} 1",
            ]
            if metrics_profile != "missing":
                rows.append("ecommerce_outbox_runtime{metric=\"runs\"} 1")
            body = "\n".join(rows) + "\n"
            data = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        self.send_response(404)
        self.end_headers()

HTTPServer(("127.0.0.1", int(__import__("os").environ.get("METRICS_MOCK_PORT", "18080"))), Handler).serve_forever()
PY
  METRICS_MOCK_PORT=18080 METRICS_MOCK_PROFILE="${METRICS_MOCK_PROFILE}" python3 "${tmp}" >/tmp/monitoring-metrics-mock.log 2>&1 &
  METRICS_MOCK_PID=$!
  sleep 1
  METRICS_URL="http://127.0.0.1:18080/metrics"
}

start_container_alertmanager() {
  assert_cmd docker
  docker rm -f "${ALERTMANAGER_CONTAINER_NAME}" >/dev/null 2>&1 || true
  docker run -d --rm \
    --name "${ALERTMANAGER_CONTAINER_NAME}" \
    -p 9093:9093 \
    -v "$(pwd)/${ALERTMANAGER_CONFIG}:/etc/alertmanager/alertmanager.yml:ro" \
    prom/alertmanager:v0.28.0 \
    --config.file=/etc/alertmanager/alertmanager.yml >/tmp/monitoring-drill-alertmanager.log
  ALERTMANAGER_URL="http://127.0.0.1:9093"
  for _ in $(seq 1 20); do
    if curl -fsS "${ALERTMANAGER_URL}/-/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  fail "alertmanager_not_ready" "alertmanager failed to become ready"
}

alert_exists() {
  local body
  body="$(curl -fsS "${ALERTMANAGER_URL}/api/v2/alerts?filter=alertname%3D${ALERT_NAME}")" || return 1
  [[ "${body}" != "[]" ]]
}

wait_for_alert_state() {
  local target="$1"
  local deadline=$(( $(date +%s) + POLL_TIMEOUT_SECONDS ))
  while [[ $(date +%s) -lt ${deadline} ]]; do
    if alert_exists; then
      [[ "${target}" == "present" ]] && return 0
    else
      [[ "${target}" == "absent" ]] && return 0
    fi
    sleep "${POLL_INTERVAL_SECONDS}"
  done
  if [[ "${target}" == "present" ]]; then
    fail "alert_absent_timeout" "alert never became active"
  else
    fail "recovery_timeout" "alert did not resolve in time"
  fi
}

validate_metrics_endpoint() {
  local payload
  payload="$(curl -fsS "${METRICS_URL}")" || return 1
  local required
  required=(
    "ecommerce_outbox_events"
    "ecommerce_outbox_runtime"
    "ecommerce_order_place_requests_total"
  )
  for metric in "${required[@]}"; do
    if ! grep -q "${metric}" <<<"${payload}"; then
      fail "missing_metric" "required metric missing: ${metric}"
      return 1
    fi
  done
  return 0
}

trigger_alert() {
  local starts_at ends_at
  starts_at="$(now_rfc3339)"
  ends_at="$(date -u -d '+10 minutes' +"%Y-%m-%dT%H:%M:%SZ")"
  curl -fsS -X POST "${ALERTMANAGER_URL}/api/v2/alerts" \
    -H 'Content-Type: application/json' \
    -d "[
      {
        \"labels\": {
          \"alertname\": \"${ALERT_NAME}\",
          \"severity\": \"${SEVERITY_LABEL}\",
          \"service\": \"${SERVICE_LABEL}\"
        },
        \"annotations\": {
          \"summary\": \"Synthetic drill alert\",
          \"description\": \"Triggered by monitoring_drill_check.sh\"
        },
        \"startsAt\": \"${starts_at}\",
        \"endsAt\": \"${ends_at}\"
      }
    ]" >/dev/null
}

resolve_alert() {
  local now
  now="$(now_rfc3339)"
  curl -fsS -X POST "${ALERTMANAGER_URL}/api/v2/alerts" \
    -H 'Content-Type: application/json' \
    -d "[
      {
        \"labels\": {
          \"alertname\": \"${ALERT_NAME}\",
          \"severity\": \"${SEVERITY_LABEL}\",
          \"service\": \"${SERVICE_LABEL}\"
        },
        \"annotations\": {
          \"summary\": \"Synthetic drill alert\",
          \"description\": \"Resolved by monitoring_drill_check.sh\"
        },
        \"startsAt\": \"${now}\",
        \"endsAt\": \"${now}\"
      }
    ]" >/dev/null
}

validate_route_match() {
  if [[ "${FORCE_ROUTE_MISS}" == "1" ]]; then
    fail "route_miss" "forced route mismatch for failure drill"
    return 1
  fi

  if command -v amtool >/dev/null 2>&1; then
    amtool --alertmanager.url="${ALERTMANAGER_URL}" config routes test "severity=${SEVERITY_LABEL}" "service=${SERVICE_LABEL}" > /tmp/monitoring-route-test.log
    if ! grep -q "${EXPECTED_RECEIVER}" /tmp/monitoring-route-test.log; then
      fail "route_miss" "amtool route test missing expected receiver ${EXPECTED_RECEIVER}"
      return 1
    fi
    return 0
  fi

  local groups
  groups="$(curl -fsS "${ALERTMANAGER_URL}/api/v2/alerts/groups")"
  if ! grep -q "${ALERT_NAME}" <<<"${groups}"; then
    fail "route_miss" "alert group does not contain alert ${ALERT_NAME}"
    return 1
  fi
  if [[ "${DRILL_MODE}" == "mock" ]] && ! grep -q "\"receiver\": \"${EXPECTED_RECEIVER}\"" <<<"${groups}"; then
    fail "route_miss" "mock route receiver mismatch, expected ${EXPECTED_RECEIVER}"
    return 1
  fi
  return 0
}

setup_mode() {
  case "${DRILL_MODE}" in
    mock)
      start_mock_server
      ;;
    container)
      start_metrics_mock
      start_container_alertmanager
      ;;
    external)
      ;;
    *)
      fail "invalid_mode" "invalid DRILL_MODE: ${DRILL_MODE}"
      ;;
  esac
}

configure_scenario() {
  case "${DRILL_SCENARIO}" in
    success)
      ;;
    missing_metric)
      MOCK_METRICS_PROFILE="missing"
      METRICS_MOCK_PROFILE="missing"
      ;;
    route_miss)
      FORCE_ROUTE_MISS="1"
      ;;
    recovery_timeout)
      MOCK_RESOLVE_DELAY_SECONDS="15"
      if (( MAX_RECOVERY_SECONDS > 2 )); then
        MAX_RECOVERY_SECONDS=2
      fi
      ;;
    *)
      fail "invalid_scenario" "invalid DRILL_SCENARIO: ${DRILL_SCENARIO}"
      ;;
  esac
}

run_drill_once() {
  setup_mode || return 1
  validate_metrics_endpoint || return 1

  trigger_alert
  TRIGGER_TS="$(date +%s)"
  wait_for_alert_state "present" || return 1
  validate_route_match || return 1

  local resolve_start_ts resolve_done_ts
  resolve_alert
  resolve_start_ts="$(date +%s)"
  wait_for_alert_state "absent" || return 1
  resolve_done_ts="$(date +%s)"
  RECOVERY_LATENCY=$(( resolve_done_ts - resolve_start_ts ))

  if (( RECOVERY_LATENCY > MAX_RECOVERY_SECONDS )); then
    fail "recovery_timeout" "recovery latency ${RECOVERY_LATENCY}s exceeds threshold ${MAX_RECOVERY_SECONDS}s"
    return 1
  fi
  return 0
}

main() {
  configure_scenario || exit 1

  local expected_failure=""
  if [[ "${DRILL_SCENARIO}" != "success" ]]; then
    expected_failure="${DRILL_SCENARIO}"
  fi

  if run_drill_once; then
    if [[ -n "${expected_failure}" ]]; then
      log "expected failure scenario '${expected_failure}' but drill passed"
      exit 1
    fi
    printf '{"mode":"%s","scenario":"%s","alert":"%s","trigger_unix":%s,"recovery_latency_seconds":%s,"max_recovery_seconds":%s}\n' \
      "${DRILL_MODE}" "${DRILL_SCENARIO}" "${ALERT_NAME}" "${TRIGGER_TS}" "${RECOVERY_LATENCY}" "${MAX_RECOVERY_SECONDS}"
    exit 0
  fi

  if [[ -n "${expected_failure}" ]] && [[ "${FAIL_REASON}" == "${expected_failure}" ]]; then
    printf '{"mode":"%s","scenario":"%s","expected_failure":"%s","assertion":"passed","reason":"%s"}\n' \
      "${DRILL_MODE}" "${DRILL_SCENARIO}" "${expected_failure}" "${FAIL_REASON}"
    exit 0
  fi

  log "drill failed with reason='${FAIL_REASON}', expected='${expected_failure:-none}'"
  exit 1
}

main "$@"

#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def load_json(path: str):
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def main():
    ap = argparse.ArgumentParser(description="Generate rollback baseline PR draft markdown artifact.")
    ap.add_argument("--gate", required=True)
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--soak", required=True)
    ap.add_argument("--ci-baseline", required=False, default="")
    ap.add_argument("--replay-baseline", required=False, default="")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    gate = load_json(args.gate)
    snap = load_json(args.snapshot)
    soak = load_json(args.soak)

    thresholds = gate.get("thresholds", {})
    errors = gate.get("errors", [])
    warnings = gate.get("warnings", [])
    rb = snap.get("recommended_baseline", {})

    ci_baseline_path = Path(args.ci_baseline) if args.ci_baseline else None
    replay_baseline_path = Path(args.replay_baseline) if args.replay_baseline else None
    ci_current = load_json(args.ci_baseline) if args.ci_baseline else {}
    replay_current = load_json(args.replay_baseline) if args.replay_baseline else {}

    ci_suggested = {
        "backend_coverage_pct": rb.get("backend_coverage_pct", ci_current.get("backend_coverage_pct", 0)),
        "integration_coverage_pct": rb.get("integration_coverage_pct", ci_current.get("integration_coverage_pct", 0)),
        "order_history_bench_ns_op": rb.get("order_history_bench_ns_op", ci_current.get("order_history_bench_ns_op", 0)),
        "order_history_parallel_bench_ns_op": rb.get("order_history_parallel_bench_ns_op", ci_current.get("order_history_parallel_bench_ns_op", 0)),
    }
    soak_checks = soak.get("checks") or {}
    replay_suggested = {
        "scenario": replay_current.get("scenario", "replay_multi_job_command_conflict"),
        "request_p95_ms": soak_checks.get("request_p95_ms", {}).get("current", replay_current.get("request_p95_ms", 0)),
        "request_p99_ms": soak_checks.get("request_p99_ms", {}).get("current", replay_current.get("request_p99_ms", 0)),
        "process_p95_ms": soak_checks.get("process_p95_ms", {}).get("current", replay_current.get("process_p95_ms", 0)),
        "process_p99_ms": soak_checks.get("process_p99_ms", {}).get("current", replay_current.get("process_p99_ms", 0)),
    }

    lines = []
    lines.append("# Draft: Baseline Rollback Update")
    lines.append("")
    lines.append("## Purpose")
    lines.append("- Auto-generated draft only. No branch/PR push is performed.")
    lines.append("- Use this draft if quality gates stay unstable and baseline refresh is required.")
    lines.append("")
    lines.append("## Trigger Context")
    lines.append(f"- gate errors: {len(errors)}")
    lines.append(f"- gate warnings: {len(warnings)}")
    lines.append(f"- snapshot reason: {snap.get('reason', 'unknown')}")
    lines.append(f"- consecutive_n: {gate.get('consecutive_n', 'n/a')}")
    lines.append("")

    lines.append("## Suggested Baseline Values")
    lines.append(f"- backend_coverage_pct: {rb.get('backend_coverage_pct', 0)}")
    lines.append(f"- integration_coverage_pct: {rb.get('integration_coverage_pct', 0)}")
    lines.append(f"- order_history_bench_ns_op: {rb.get('order_history_bench_ns_op', 0)}")
    lines.append(f"- order_history_parallel_bench_ns_op: {rb.get('order_history_parallel_bench_ns_op', 0)}")
    lines.append("")

    lines.append("## Gate Threshold Snapshot")
    for k in sorted(thresholds.keys()):
        lines.append(f"- {k}: {thresholds[k]}")
    lines.append("")

    lines.append("## Replay Soak Compare")
    lines.append(f"- status: {soak.get('status', 'unknown')}")
    for key, row in (soak.get("checks") or {}).items():
        lines.append(f"- {key}: current={row.get('current')} baseline={row.get('baseline')} delta={row.get('delta')}")
    lines.append("")

    lines.append("## Error Details")
    if errors:
        for e in errors:
            lines.append(f"- {e}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Warning Details")
    if warnings:
        for w in warnings:
            lines.append(f"- {w}")
    else:
        lines.append("- none")
    lines.append("")

    lines.append("## Manual Steps")
    lines.append("1. Review artifacts: gate-result, rollback-snapshot, replay-soak-compare.")
    lines.append("2. If accepted, update `ecommerce_app/docs/CI_TREND_BASELINE.json` and `ecommerce_app/docs/REPLAY_SOAK_BASELINE.json` in one PR.")
    lines.append("3. Re-run CI and confirm no new hard-fail regression trend.")

    lines.append("")
    lines.append("## Diff Template: CI_TREND_BASELINE.json")
    if ci_baseline_path and ci_baseline_path.exists():
        lines.append("```diff")
        for key in ["backend_coverage_pct", "integration_coverage_pct", "order_history_bench_ns_op", "order_history_parallel_bench_ns_op"]:
            old = ci_current.get(key, 0)
            new = ci_suggested.get(key, old)
            lines.append(f"-  \"{key}\": {old}")
            lines.append(f"+  \"{key}\": {new}")
        lines.append("```")
    else:
        lines.append("- baseline file not found")

    lines.append("")
    lines.append("## Diff Template: REPLAY_SOAK_BASELINE.json")
    if replay_baseline_path and replay_baseline_path.exists():
        lines.append("```diff")
        for key in ["request_p95_ms", "request_p99_ms", "process_p95_ms", "process_p99_ms"]:
            old = replay_current.get(key, 0)
            new = replay_suggested.get(key, old)
            lines.append(f"-  \"{key}\": {old}")
            lines.append(f"+  \"{key}\": {new}")
        lines.append("```")
    else:
        lines.append("- replay soak baseline file not found")

    out = Path(args.output)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

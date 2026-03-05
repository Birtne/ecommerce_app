#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def count_by_category(items: list[dict]) -> dict[str, int]:
    result: dict[str, int] = {}
    for it in items:
        key = str(it.get("failure_category", "unknown"))
        result[key] = result.get(key, 0) + 1
    return result


def compute_level(score: int, warning: int, critical: int) -> str:
    if score <= critical:
        return "critical"
    if score <= warning:
        return "warning"
    return "ok"


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze nightly external smoke trend")
    ap.add_argument("--history", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--state", required=True)
    ap.add_argument("--run-number", type=int, default=0)
    ap.add_argument("--consecutive-n", type=int, default=3)
    ap.add_argument("--suppression-window", type=int, default=2)
    ap.add_argument("--health-warning", type=int, default=70)
    ap.add_argument("--health-critical", type=int, default=50)
    ap.add_argument("--ci-signal", default="")
    args = ap.parse_args()

    history_path = Path(args.history)
    out_path = Path(args.output)
    state_path = Path(args.state)
    if not history_path.exists():
        out_path.write_text(json.dumps({"status": "missing_history"}, ensure_ascii=False, indent=2), encoding="utf-8")
        return

    rows = json.loads(history_path.read_text(encoding="utf-8"))
    latest = rows[-1] if rows else {}
    current = rows[-5:]
    previous = rows[-10:-5]
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        state = {"last_escalated_run_number": {}, "last_levels": {}}

    cur_counts = count_by_category(current)
    prev_counts = count_by_category(previous)
    categories = sorted(set(cur_counts.keys()) | set(prev_counts.keys()))
    deltas: dict[str, dict] = {}
    escalations: list[str] = []
    escalation_levels: dict[str, dict] = {}
    denoise_notes: list[str] = []
    penalties: dict[str, int] = {"consecutive_failures": 0, "latest_exhausted": 0, "ci_linked": 0}

    for c in categories:
        cur = int(cur_counts.get(c, 0))
        prev = int(prev_counts.get(c, 0))
        ratio = round((cur / prev), 3) if prev > 0 else (999.0 if cur > 0 else 1.0)
        if cur < 2 and prev < 2:
            denoise_notes.append(f"{c}: ignored low-volume noise (current={cur}, previous={prev})")
            continue
        if prev == 0 and cur == 1:
            denoise_notes.append(f"{c}: ignored single-spike with zero baseline")
            continue
        deltas[c] = {
            "current_window": cur,
            "previous_window": prev,
            "delta": cur - prev,
            "ratio": ratio,
        }
        consec = 0
        for row in reversed(rows):
            if row.get("failure_category", "unknown") == c and not bool(row.get("ok", False)):
                consec += 1
                continue
            break
        level = ""
        if consec >= max(args.consecutive_n + 1, 4):
            level = "critical"
        elif consec >= args.consecutive_n:
            level = "warning"
        if level:
            last_run = int((state.get("last_escalated_run_number", {}) or {}).get(c, 0))
            suppressed = args.run_number > 0 and last_run > 0 and (args.run_number - last_run) <= args.suppression_window
            escalation_levels[c] = {
                "level": level,
                "consecutive_failures": consec,
                "suppressed": suppressed,
                "suppression_window_runs": args.suppression_window,
                "last_escalated_run_number": last_run,
            }
            if not suppressed:
                escalations.append(f"{c}: level={level}, consecutive={consec}, current={cur}, previous={prev}, ratio={ratio}")
                state.setdefault("last_escalated_run_number", {})[c] = args.run_number
                state.setdefault("last_levels", {})[c] = level
                penalties["consecutive_failures"] += 30 if level == "critical" else 15

    if latest.get("ok") is False and int(latest.get("attempts", 0)) >= 3:
        escalations.append("latest_run_exhausted_retries")
        escalation_levels["latest_run"] = {
            "level": "critical",
            "consecutive_failures": 1,
            "suppressed": False,
            "suppression_window_runs": args.suppression_window,
            "last_escalated_run_number": args.run_number,
        }
        penalties["latest_exhausted"] = 25

    ci_signal_obj: dict = {}
    ci_signal_status = "missing"
    if args.ci_signal:
        p = Path(args.ci_signal)
        if p.exists():
            ci_signal_obj = json.loads(p.read_text(encoding="utf-8"))
            ci_signal_status = "loaded"
            ci_level = str(ci_signal_obj.get("overall_level", "ok"))
            if ci_level == "critical":
                penalties["ci_linked"] = 20
            elif ci_level == "warning":
                penalties["ci_linked"] = 8

    aggregate_penalty = sum(int(v) for v in penalties.values())
    health_score = max(0, 100 - aggregate_penalty)
    health_level = compute_level(health_score, args.health_warning, args.health_critical)
    out = {
        "status": "ok",
        "window_current_size": len(current),
        "window_previous_size": len(previous),
        "consecutive_n": args.consecutive_n,
        "suppression_window_runs": args.suppression_window,
        "deltas": deltas,
        "escalation_levels": escalation_levels,
        "escalations": escalations,
        "escalated": bool(escalations),
        "health_score": health_score,
        "health_level": health_level,
        "health_warning_threshold": args.health_warning,
        "health_critical_threshold": args.health_critical,
        "aggregate_penalty": aggregate_penalty,
        "penalties": penalties,
        "denoise_notes": denoise_notes,
        "latest": latest,
        "ci_signal_status": ci_signal_status,
        "ci_signal_level": ci_signal_obj.get("overall_level", ""),
    }
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()

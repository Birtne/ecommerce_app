#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def load_json(path: str):
    p = Path(path)
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def safe_avg(values):
    nums = [float(v) for v in values if float(v) > 0]
    if not nums:
        return 0.0
    return round(sum(nums) / len(nums), 3)


def main():
    ap = argparse.ArgumentParser(description="Generate replay soak threshold writeback draft artifact.")
    ap.add_argument("--thresholds", required=True)
    ap.add_argument("--history", required=True)
    ap.add_argument("--suggestion", required=True)
    ap.add_argument("--output-json", required=True)
    ap.add_argument("--output-md", required=True)
    ap.add_argument("--output-pr-summary", required=False, default="")
    args = ap.parse_args()

    thresholds = load_json(args.thresholds)
    suggestion = load_json(args.suggestion)
    history = load_json(args.history)
    rows = history[-10:] if isinstance(history, list) else []

    scenario_rows = {}
    for row in rows:
        scenario = row.get("scenario", "default")
        scenario_rows.setdefault(scenario, []).append(row)

    scenario_patch = {}
    for scenario, items in scenario_rows.items():
        p95 = safe_avg([r.get("request_p95_ms", 0) for r in items])
        p99 = safe_avg([r.get("request_p99_ms", 0) for r in items])
        proc95 = safe_avg([r.get("process_p95_ms", 0) for r in items])
        proc99 = safe_avg([r.get("process_p99_ms", 0) for r in items])
        scenario_patch[scenario] = {
            "latency_multiplier": {
                "request_p95_ms": round(max(1.1, min(2.0, p95 / max(1, p95-10))), 3) if p95 > 10 else 1.25,
                "request_p99_ms": round(max(1.1, min(2.2, p99 / max(1, p99-15))), 3) if p99 > 15 else 1.35,
                "process_p95_ms": round(max(1.1, min(2.0, proc95 / max(1, proc95-5))), 3) if proc95 > 5 else 1.35,
                "process_p99_ms": round(max(1.1, min(2.2, proc99 / max(1, proc99-8))), 3) if proc99 > 8 else 1.45,
            }
        }

    recommended_anomaly = suggestion.get("recommended_anomaly_thresholds", {})
    current_scenario_thresholds = thresholds.get("scenario_thresholds", {})
    out = {
        "status": "ok" if rows else "insufficient_history",
        "window_size": len(rows),
        "source_thresholds": args.thresholds,
        "current_thresholds": thresholds,
        "recommended_patch": {
            "anomaly_thresholds": {
                "timeout": int(recommended_anomaly.get("timeout", thresholds.get("anomaly_thresholds", {}).get("timeout", 12))),
                "cancel": int(recommended_anomaly.get("cancel", thresholds.get("anomaly_thresholds", {}).get("cancel", 12))),
                "partial": int(recommended_anomaly.get("partial", thresholds.get("anomaly_thresholds", {}).get("partial", 24))),
            },
            "scenario_thresholds": scenario_patch if scenario_patch else thresholds.get("scenario_thresholds", {}),
        },
    }
    comparison = {
        "anomaly_thresholds": {},
        "scenario_thresholds": {},
    }
    for key in ["timeout", "cancel", "partial"]:
        current_val = int(thresholds.get("anomaly_thresholds", {}).get(key, 0))
        new_val = int(out["recommended_patch"]["anomaly_thresholds"].get(key, current_val))
        comparison["anomaly_thresholds"][key] = {"current": current_val, "suggested": new_val, "delta": new_val - current_val}
    for scenario, cfg in out["recommended_patch"]["scenario_thresholds"].items():
        cur_cfg = (current_scenario_thresholds.get(scenario, {}) or {}).get("latency_multiplier", {})
        new_cfg = (cfg or {}).get("latency_multiplier", {})
        scenario_cmp = {}
        for metric in ["request_p95_ms", "request_p99_ms", "process_p95_ms", "process_p99_ms"]:
            cur_val = float(cur_cfg.get(metric, 0))
            new_val = float(new_cfg.get(metric, cur_val))
            scenario_cmp[metric] = {"current": cur_val, "suggested": new_val, "delta": round(new_val - cur_val, 3)}
        comparison["scenario_thresholds"][scenario] = scenario_cmp
    out["comparison"] = comparison

    out_json = Path(args.output_json)
    out_json.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = []
    lines.append("# Draft: Replay Soak Threshold Writeback")
    lines.append("")
    lines.append("- auto-generated artifact only; no file is modified automatically.")
    lines.append(f"- source thresholds: `{args.thresholds}`")
    lines.append(f"- window size: {out['window_size']}")
    lines.append(f"- status: {out['status']}")
    lines.append("")
    lines.append("## Suggested Patch Snippet")
    lines.append("```json")
    lines.append(json.dumps(out["recommended_patch"], ensure_ascii=False, indent=2))
    lines.append("```")
    lines.append("")
    lines.append("## PR Summary Compare Template")
    lines.append("")
    lines.append("| Key | Current | Suggested | Delta |")
    lines.append("|---|---:|---:|---:|")
    for key, row in comparison["anomaly_thresholds"].items():
        lines.append(f"| anomaly.{key} | {row['current']} | {row['suggested']} | {row['delta']:+d} |")
    for scenario, metrics in comparison["scenario_thresholds"].items():
        for metric, row in metrics.items():
            lines.append(f"| {scenario}.{metric} | {row['current']:.3f} | {row['suggested']:.3f} | {row['delta']:+.3f} |")
    lines.append("")
    lines.append("- note: template is advisory only; no repository file is auto-updated.")

    out_md = Path(args.output_md)
    out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if args.output_pr_summary:
        pr_lines = []
        pr_lines.append("### Replay Soak Threshold Draft Compare")
        pr_lines.append("| Key | Current | Suggested | Delta |")
        pr_lines.append("|---|---:|---:|---:|")
        for key, row in comparison["anomaly_thresholds"].items():
            pr_lines.append(f"| anomaly.{key} | {row['current']} | {row['suggested']} | {row['delta']:+d} |")
        for scenario, metrics in comparison["scenario_thresholds"].items():
            for metric, row in metrics.items():
                pr_lines.append(f"| {scenario}.{metric} | {row['current']:.3f} | {row['suggested']:.3f} | {row['delta']:+.3f} |")
        pr_lines.append("")
        pr_lines.append("- advisory artifact only, no automatic threshold writeback.")
        Path(args.output_pr_summary).write_text("\n".join(pr_lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()

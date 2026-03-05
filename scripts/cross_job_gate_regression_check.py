#!/usr/bin/env python3
import json
import subprocess
import tempfile
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent


def run_cmd(cmd: list[str]) -> str:
    cp = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return cp.stdout.strip()


def case_ci_warning(tmp: Path) -> None:
    q = tmp / "q.json"
    g = tmp / "g.json"
    s = tmp / "s.json"
    o = tmp / "o.json"
    q.write_text(json.dumps({"baseline": {}, "current": {}, "delta": {}}), encoding="utf-8")
    g.write_text(json.dumps({"thresholds": {}, "warnings": ["backend coverage warn floor"], "errors": [], "jitter": {}}), encoding="utf-8")
    s.write_text(json.dumps({"status": "ok", "checks": {}, "anomaly_checks": {}, "warnings": [], "errors": []}), encoding="utf-8")
    run_cmd([
        "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
        "--mode", "ci",
        "--quality", str(q),
        "--gate", str(g),
        "--soak", str(s),
        "--config", str(SCRIPTS_DIR.parent / "docs" / "CROSS_JOB_SCORING_CONFIG.json"),
        "--env", "ci",
        "--branch", "feature/regression-warning",
        "--out", str(o),
    ])
    out = json.loads(o.read_text(encoding="utf-8"))
    if out.get("overall_level") != "warning":
        raise SystemExit(f"ci warning sample failed: {out}")
    if int((out.get("domain_scores") or {}).get("coverage", 0)) >= 100:
        raise SystemExit(f"ci warning domain score missing: {out}")


def case_ci_critical_by_history(tmp: Path) -> None:
    q = tmp / "q2.json"
    g = tmp / "g2.json"
    s = tmp / "s2.json"
    o = tmp / "o2.json"
    h = tmp / "h2.json"
    q.write_text(json.dumps({"baseline": {}, "current": {}, "delta": {}}), encoding="utf-8")
    g.write_text(json.dumps({"thresholds": {}, "warnings": ["backend coverage warn floor"], "errors": [], "jitter": {}}), encoding="utf-8")
    s.write_text(json.dumps({"status": "ok", "checks": {}, "anomaly_checks": {}, "warnings": ["soak p95 warning"], "errors": []}), encoding="utf-8")
    history = [
        {"overall_level": "warning", "health_score": 99, "total_score": 99},
        {"overall_level": "warning", "health_score": 98, "total_score": 98},
        {"overall_level": "warning", "health_score": 97, "total_score": 97},
        {"overall_level": "warning", "health_score": 96, "total_score": 96},
    ]
    h.write_text(json.dumps(history), encoding="utf-8")
    cfg = tmp / "critical-trend-config.json"
    cfg.write_text(json.dumps({
        "version": 2,
        "defaults": {
            "domain_penalties": {
                "coverage": {"warning_penalty": 12, "critical_penalty": 30},
                "bench": {"warning_penalty": 10, "critical_penalty": 28},
                "soak": {"warning_penalty": 10, "critical_penalty": 30},
                "nightly": {"warning_penalty": 8, "critical_penalty": 22}
            },
            "trend": {"history_limit": 30, "consecutive_n": 3, "score_step": 1, "domain_score_step": 1}
        }
    }), encoding="utf-8")
    run_cmd([
        "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
        "--mode", "ci",
        "--quality", str(q),
        "--gate", str(g),
        "--soak", str(s),
        "--history", str(h),
        "--history-out", str(h),
        "--config", str(cfg),
        "--env", "ci",
        "--branch", "feature/regression-history-critical",
        "--out", str(o),
    ])
    out = json.loads(o.read_text(encoding="utf-8"))
    if out.get("overall_level") != "critical":
        raise SystemExit(f"ci critical trend sample failed: {out}")
    trend = out.get("trend", {})
    if int(trend.get("consecutive_worsening", 0)) < 3:
        raise SystemExit(f"ci critical trend streak failed: {out}")


def case_ci_mixed_domain_conflict(tmp: Path) -> None:
    q = tmp / "q3.json"
    g = tmp / "g3.json"
    s = tmp / "s3.json"
    n = tmp / "n3.json"
    o = tmp / "o3.json"
    q.write_text(json.dumps({"baseline": {}, "current": {}, "delta": {}}), encoding="utf-8")
    g.write_text(json.dumps({
        "thresholds": {},
        "warnings": ["backend coverage warn floor breached", "benchmark jitter window high"],
        "errors": ["order bench fail ceiling exceeded"],
        "jitter": {},
    }), encoding="utf-8")
    s.write_text(json.dumps({"status": "ok", "checks": {}, "anomaly_checks": {}, "warnings": ["soak request p95 above baseline"], "errors": []}), encoding="utf-8")
    n.write_text(json.dumps({"overall_level": "critical"}), encoding="utf-8")
    run_cmd([
        "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
        "--mode", "ci",
        "--quality", str(q),
        "--gate", str(g),
        "--soak", str(s),
        "--nightly-signal", str(n),
        "--config", str(SCRIPTS_DIR.parent / "docs" / "CROSS_JOB_SCORING_CONFIG.json"),
        "--env", "ci",
        "--branch", "release/2026-03",
        "--out", str(o),
    ])
    out = json.loads(o.read_text(encoding="utf-8"))
    if out.get("overall_level") != "critical":
        raise SystemExit(f"mixed domain conflict expected critical: {out}")
    domains = out.get("domain_levels") or {}
    if domains.get("bench") != "critical" or domains.get("nightly") != "critical":
        raise SystemExit(f"mixed domain conflict domain levels mismatch: {out}")


def case_nightly_denoise(tmp: Path) -> None:
    hist = tmp / "night_hist.json"
    state = tmp / "state.json"
    out = tmp / "analysis.json"
    ci = tmp / "ci.json"
    hist.write_text(json.dumps([
        {"ok": True, "attempts": 1, "failure_category": "none", "failure_reason": ""},
        {"ok": False, "attempts": 1, "failure_category": "routing", "failure_reason": "x"},
    ]), encoding="utf-8")
    ci.write_text(json.dumps({"overall_level": "ok"}), encoding="utf-8")
    run_cmd([
        "python3", str(SCRIPTS_DIR / "nightly_trend_analysis.py"),
        "--history", str(hist),
        "--output", str(out),
        "--state", str(state),
        "--run-number", "10",
        "--ci-signal", str(ci),
    ])
    analysis = json.loads(out.read_text(encoding="utf-8"))
    notes = analysis.get("denoise_notes", [])
    if not any("ignored low-volume noise" in n for n in notes):
        raise SystemExit(f"nightly denoise sample failed: {analysis}")


def case_threshold_change_override(tmp: Path) -> None:
    q = tmp / "q4.json"
    g = tmp / "g4.json"
    s = tmp / "s4.json"
    o_main = tmp / "o4-main.json"
    o_release = tmp / "o4-release.json"
    cfg = tmp / "override-config.json"
    q.write_text(json.dumps({"baseline": {}, "current": {}, "delta": {}}), encoding="utf-8")
    g.write_text(json.dumps({"thresholds": {}, "warnings": ["backend coverage warn floor"], "errors": [], "jitter": {}}), encoding="utf-8")
    s.write_text(json.dumps({"status": "ok", "checks": {}, "anomaly_checks": {}, "warnings": [], "errors": []}), encoding="utf-8")
    cfg.write_text(json.dumps({
        "version": 2,
        "defaults": {
            "domain_penalties": {
                "coverage": {"warning_penalty": 12, "critical_penalty": 30},
                "bench": {"warning_penalty": 10, "critical_penalty": 28},
                "soak": {"warning_penalty": 10, "critical_penalty": 30},
                "nightly": {"warning_penalty": 8, "critical_penalty": 22}
            },
            "trend": {"history_limit": 30, "consecutive_n": 3, "score_step": 5, "domain_score_step": 5}
        },
        "branch_overrides": [
            {
                "pattern": "^release/",
                "overrides": {
                    "domain_penalties": {
                        "coverage": {"warning_penalty": 18, "critical_penalty": 36}
                    }
                }
            }
        ]
    }), encoding="utf-8")
    run_cmd([
        "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
        "--mode", "ci",
        "--quality", str(q),
        "--gate", str(g),
        "--soak", str(s),
        "--config", str(cfg),
        "--env", "ci",
        "--branch", "main",
        "--out", str(o_main),
    ])
    run_cmd([
        "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
        "--mode", "ci",
        "--quality", str(q),
        "--gate", str(g),
        "--soak", str(s),
        "--config", str(cfg),
        "--env", "ci",
        "--branch", "release/2026-03",
        "--out", str(o_release),
    ])
    out_main = json.loads(o_main.read_text(encoding="utf-8"))
    out_release = json.loads(o_release.read_text(encoding="utf-8"))
    main_cov = int((out_main.get("domain_scores") or {}).get("coverage", 0))
    rel_cov = int((out_release.get("domain_scores") or {}).get("coverage", 0))
    if rel_cov >= main_cov:
        raise SystemExit(f"threshold override sample failed: release coverage score should be lower: main={main_cov} release={rel_cov}")
    rel_explain = out_release.get("threshold_explain") or {}
    if "^release/" not in (rel_explain.get("branch_override_matches") or []):
        raise SystemExit(f"threshold override explain missing match: {out_release}")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="cross-job-regression-") as td:
        tmp = Path(td)
        case_ci_warning(tmp)
        case_ci_critical_by_history(tmp)
        case_ci_mixed_domain_conflict(tmp)
        case_threshold_change_override(tmp)
        case_nightly_denoise(tmp)
    print("cross-job gate regression samples passed")


if __name__ == "__main__":
    main()

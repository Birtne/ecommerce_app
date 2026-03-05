#!/usr/bin/env python3
import argparse
import json
from pathlib import Path


def require_fields(obj: dict, fields: list[str], label: str) -> None:
    missing = [f for f in fields if f not in obj]
    if missing:
        raise SystemExit(f"{label} missing fields: {', '.join(missing)}")


def validate_ci(args: argparse.Namespace) -> None:
    quality = json.loads(Path(args.quality).read_text(encoding="utf-8"))
    gate = json.loads(Path(args.gate).read_text(encoding="utf-8"))
    soak = json.loads(Path(args.soak).read_text(encoding="utf-8"))
    require_fields(quality, ["baseline", "current", "delta"], "quality trend snapshot")
    require_fields(gate, ["thresholds", "warnings", "errors", "jitter"], "gate result snapshot")
    require_fields(soak, ["status", "checks", "anomaly_checks"], "replay soak snapshot")
    if args.cross:
        cross = json.loads(Path(args.cross).read_text(encoding="utf-8"))
        require_fields(cross, [
            "status", "mode", "overall_level", "health_score", "total_score",
            "levels", "domain_levels", "domain_scores", "scoring_thresholds", "threshold_explain", "trend",
        ], "cross-job ci snapshot")
    print("ci trend snapshot compatibility check passed")


def validate_nightly(args: argparse.Namespace) -> None:
    analysis = json.loads(Path(args.analysis).read_text(encoding="utf-8"))
    state = json.loads(Path(args.state).read_text(encoding="utf-8"))
    require_fields(analysis, [
        "status", "deltas", "escalations", "health_score", "health_level",
        "health_warning_threshold", "health_critical_threshold", "denoise_notes", "penalties",
    ], "nightly trend analysis snapshot")
    require_fields(state, ["last_escalated_run_number", "last_levels"], "nightly state snapshot")
    if args.cross:
        cross = json.loads(Path(args.cross).read_text(encoding="utf-8"))
        require_fields(cross, [
            "status", "mode", "overall_level", "health_score", "total_score",
            "levels", "domain_levels", "domain_scores", "scoring_thresholds", "threshold_explain", "trend",
        ], "cross-job nightly snapshot")
    print("nightly trend snapshot compatibility check passed")


def main() -> None:
    ap = argparse.ArgumentParser(description="Trend snapshot compatibility checker")
    ap.add_argument("--mode", choices=["ci", "nightly"], required=True)
    ap.add_argument("--quality", default="")
    ap.add_argument("--gate", default="")
    ap.add_argument("--soak", default="")
    ap.add_argument("--analysis", default="")
    ap.add_argument("--state", default="")
    ap.add_argument("--cross", default="")
    args = ap.parse_args()

    if args.mode == "ci":
        for name in ["quality", "gate", "soak"]:
            if not getattr(args, name):
                raise SystemExit(f"--{name} is required for mode=ci")
        validate_ci(args)
        return

    for name in ["analysis", "state"]:
        if not getattr(args, name):
            raise SystemExit(f"--{name} is required for mode=nightly")
    validate_nightly(args)


if __name__ == "__main__":
    main()

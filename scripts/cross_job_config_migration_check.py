#!/usr/bin/env python3
import json
import subprocess
import tempfile
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent
DOCS_DIR = SCRIPTS_DIR.parent / "docs"


def run(cmd: list[str]) -> str:
    cp = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return cp.stdout.strip()


def main() -> None:
    current_cfg = DOCS_DIR / "CROSS_JOB_SCORING_CONFIG.json"
    if not current_cfg.exists():
        raise SystemExit(f"missing config: {current_cfg}")
    cfg = json.loads(current_cfg.read_text(encoding="utf-8"))
    if int(cfg.get("version", 0)) < 2:
        raise SystemExit("cross job config version must be >= 2")
    if "defaults" not in cfg:
        raise SystemExit("cross job config missing defaults")
    if "domain_penalties" not in cfg["defaults"] or "trend" not in cfg["defaults"]:
        raise SystemExit("cross job config defaults missing domain_penalties/trend")

    with tempfile.TemporaryDirectory(prefix="cross-job-config-migration-") as td:
        tmp = Path(td)
        quality = tmp / "quality.json"
        gate = tmp / "gate.json"
        soak = tmp / "soak.json"
        out_new = tmp / "out-new.json"
        out_legacy = tmp / "out-legacy.json"
        legacy_cfg_path = tmp / "legacy-config.json"

        quality.write_text(json.dumps({"baseline": {}, "current": {}, "delta": {}}), encoding="utf-8")
        gate.write_text(json.dumps({"thresholds": {}, "warnings": ["backend coverage warn floor"], "errors": [], "jitter": {}}), encoding="utf-8")
        soak.write_text(json.dumps({"status": "ok", "checks": {}, "anomaly_checks": {}, "warnings": [], "errors": []}), encoding="utf-8")

        # Legacy shape intentionally omits `defaults`.
        legacy_cfg = {
            "version": 1,
            "thresholds": cfg["defaults"]["domain_penalties"],
            "trend": cfg["defaults"]["trend"],
            "profiles": cfg.get("profiles", {}),
            "branch_overrides": cfg.get("branch_overrides", []),
        }
        legacy_cfg_path.write_text(json.dumps(legacy_cfg), encoding="utf-8")

        run([
            "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
            "--mode", "ci",
            "--quality", str(quality),
            "--gate", str(gate),
            "--soak", str(soak),
            "--config", str(current_cfg),
            "--env", "ci",
            "--branch", "main",
            "--out", str(out_new),
        ])
        run([
            "python3", str(SCRIPTS_DIR / "cross_job_trend_gate.py"),
            "--mode", "ci",
            "--quality", str(quality),
            "--gate", str(gate),
            "--soak", str(soak),
            "--config", str(legacy_cfg_path),
            "--env", "ci",
            "--branch", "main",
            "--out", str(out_legacy),
        ])

        new = json.loads(out_new.read_text(encoding="utf-8"))
        legacy = json.loads(out_legacy.read_text(encoding="utf-8"))

        required = ["overall_level", "health_score", "total_score", "domain_levels", "domain_scores", "scoring_thresholds", "threshold_explain", "trend"]
        for name in required:
            if name not in new:
                raise SystemExit(f"new config output missing: {name}")
            if name not in legacy:
                raise SystemExit(f"legacy config output missing: {name}")

        if (new["domain_levels"] != legacy["domain_levels"]) or (new["domain_scores"] != legacy["domain_scores"]):
            raise SystemExit(f"legacy migration mismatch: new={new['domain_scores']} legacy={legacy['domain_scores']}")
        if not bool((legacy.get("threshold_explain") or {}).get("legacy_migrated", False)):
            raise SystemExit("legacy migration flag not set")

    print("cross-job config migration compatibility check passed")


if __name__ == "__main__":
    main()

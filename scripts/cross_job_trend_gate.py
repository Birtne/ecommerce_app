#!/usr/bin/env python3
import argparse
import copy
import json
import re
from datetime import datetime, timezone
from pathlib import Path

DOMAINS = ["coverage", "bench", "soak", "nightly"]


def level_rank(level: str) -> int:
    return {"ok": 0, "warning": 1, "critical": 2}.get(level, 2)


def max_level(levels: list[str]) -> str:
    best = "ok"
    for lv in levels:
        if level_rank(lv) > level_rank(best):
            best = lv
    return best


def deep_merge(base: dict, override: dict) -> dict:
    out = copy.deepcopy(base)
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def normalize_domain_penalties(raw: dict) -> dict[str, dict[str, int]]:
    defaults = {
        "coverage": {"warning_penalty": 12, "critical_penalty": 30},
        "bench": {"warning_penalty": 10, "critical_penalty": 28},
        "soak": {"warning_penalty": 10, "critical_penalty": 30},
        "nightly": {"warning_penalty": 8, "critical_penalty": 22},
    }
    out = copy.deepcopy(defaults)
    for d in DOMAINS:
        cfg = (raw or {}).get(d, {})
        if "warning_penalty" in cfg:
            out[d]["warning_penalty"] = int(cfg["warning_penalty"])
        elif "warning" in cfg:
            out[d]["warning_penalty"] = int(cfg["warning"])
        if "critical_penalty" in cfg:
            out[d]["critical_penalty"] = int(cfg["critical_penalty"])
        elif "critical" in cfg:
            out[d]["critical_penalty"] = int(cfg["critical"])
    return out


def load_config(path: str, env_name: str, branch: str) -> tuple[dict, dict]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))

    # migration compatibility: old format
    if "defaults" not in raw:
        legacy_penalties = raw.get("domain_penalties") or raw.get("thresholds") or {}
        legacy_trend = raw.get("trend") or {}
        raw = {
            "version": int(raw.get("version", 1)),
            "defaults": {
                "domain_penalties": legacy_penalties,
                "trend": legacy_trend,
            },
            "profiles": raw.get("profiles", {}),
            "branch_overrides": raw.get("branch_overrides", []),
        }

    defaults = raw.get("defaults") or {}
    effective = copy.deepcopy(defaults)
    explain = {
        "config_version": int(raw.get("version", 1)),
        "config_path": path,
        "profile": "",
        "branch_override_matches": [],
        "legacy_migrated": bool("defaults" not in json.loads(Path(path).read_text(encoding="utf-8"))),
    }

    profile_cfg = (raw.get("profiles") or {}).get(env_name, {})
    if profile_cfg:
        effective = deep_merge(effective, profile_cfg)
        explain["profile"] = env_name

    for it in raw.get("branch_overrides") or []:
        pattern = str(it.get("pattern", ""))
        if not pattern:
            continue
        if re.search(pattern, branch or ""):
            effective = deep_merge(effective, it.get("overrides") or {})
            explain["branch_override_matches"].append(pattern)

    effective["domain_penalties"] = normalize_domain_penalties(effective.get("domain_penalties") or {})
    trend = effective.get("trend") or {}
    effective["trend"] = {
        "history_limit": int(trend.get("history_limit", 30)),
        "consecutive_n": int(trend.get("consecutive_n", 3)),
        "score_step": int(trend.get("score_step", 5)),
        "domain_score_step": int(trend.get("domain_score_step", 5)),
    }
    explain["defaults_domain_penalties"] = normalize_domain_penalties((defaults.get("domain_penalties") or {}))
    explain["effective_domain_penalties"] = effective["domain_penalties"]
    return effective, explain


def domain_score(domain: str, level: str, thresholds: dict[str, dict[str, int]]) -> int:
    cfg = thresholds.get(domain, {"warning_penalty": 10, "critical_penalty": 25})
    if level == "critical":
        return max(0, 100 - int(cfg["critical_penalty"]))
    if level == "warning":
        return max(0, 100 - int(cfg["warning_penalty"]))
    return 100


def infer_level(messages: list[str], keywords: list[str]) -> str:
    low = [str(m).lower() for m in messages]
    for msg in low:
        if any(k in msg for k in keywords):
            return "critical"
    return "ok"


def infer_warning_level(messages: list[str], keywords: list[str]) -> str:
    low = [str(m).lower() for m in messages]
    for msg in low:
        if any(k in msg for k in keywords):
            return "warning"
    return "ok"


def merge_level(base: str, incoming: str) -> str:
    return incoming if level_rank(incoming) > level_rank(base) else base


def ci_mode(args: argparse.Namespace, cfg: dict, explain: dict) -> dict:
    quality = json.loads(Path(args.quality).read_text(encoding="utf-8"))
    gate = json.loads(Path(args.gate).read_text(encoding="utf-8"))
    soak = json.loads(Path(args.soak).read_text(encoding="utf-8"))
    errors = [str(x) for x in (gate.get("errors") or [])]
    warns = [str(x) for x in (gate.get("warnings") or [])]

    coverage_level = "ok"
    coverage_level = merge_level(coverage_level, infer_warning_level(warns, ["coverage"]))
    coverage_level = merge_level(coverage_level, infer_level(errors, ["coverage"]))

    bench_level = "ok"
    bench_level = merge_level(bench_level, infer_warning_level(warns, ["bench", "benchmark", "ns/op", "jitter"]))
    bench_level = merge_level(bench_level, infer_level(errors, ["bench", "benchmark", "ns/op"]))

    soak_level = "ok"
    if soak.get("warnings"):
        soak_level = "warning"
    if soak.get("errors"):
        soak_level = "critical"

    nightly_level = "ok"
    nightly_signal_status = "missing"
    if args.nightly_signal:
        p = Path(args.nightly_signal)
        if p.exists():
            nightly = json.loads(p.read_text(encoding="utf-8"))
            nightly_level = str(nightly.get("overall_level", "ok"))
            nightly_signal_status = "loaded"

    thresholds = cfg["domain_penalties"]
    domain_levels = {
        "coverage": coverage_level,
        "bench": bench_level,
        "soak": soak_level,
        "nightly": nightly_level,
    }
    domain_scores = {d: domain_score(d, lv, thresholds) for d, lv in domain_levels.items()}
    aggregate_penalty = 0
    for d, lv in domain_levels.items():
        if lv == "warning":
            aggregate_penalty += int(thresholds[d]["warning_penalty"])
        elif lv == "critical":
            aggregate_penalty += int(thresholds[d]["critical_penalty"])
    total_score = int(round(sum(domain_scores.values()) / len(DOMAINS)))
    health_score = max(0, 100 - aggregate_penalty)
    return {
        "status": "ok",
        "mode": "ci",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_level": max_level(list(domain_levels.values())),
        "health_score": health_score,
        "total_score": total_score,
        "domain_levels": domain_levels,
        "domain_scores": domain_scores,
        "scoring_thresholds": thresholds,
        "threshold_explain": explain,
        "levels": {
            "coverage_benchmark": max_level([coverage_level, bench_level]),
            "soak": soak_level,
            "nightly_link": nightly_level,
        },
        "quality_current": quality.get("current", {}),
        "nightly_signal_status": nightly_signal_status,
    }


def nightly_mode(args: argparse.Namespace, cfg: dict, explain: dict) -> dict:
    analysis = json.loads(Path(args.analysis).read_text(encoding="utf-8"))
    ci_signal_status = "missing"
    domain_levels = {
        "coverage": "ok",
        "bench": "ok",
        "soak": "ok",
        "nightly": str(analysis.get("health_level", "critical")),
    }
    if args.ci_signal:
        p = Path(args.ci_signal)
        if p.exists():
            ci = json.loads(p.read_text(encoding="utf-8"))
            ci_signal_status = "loaded"
            ci_domains = ci.get("domain_levels") or {}
            for d in ["coverage", "bench", "soak"]:
                if d in ci_domains:
                    domain_levels[d] = str(ci_domains.get(d, "ok"))
            if not ci_domains:
                linked = str(ci.get("overall_level", "ok"))
                for d in ["coverage", "bench", "soak"]:
                    domain_levels[d] = linked

    thresholds = cfg["domain_penalties"]
    domain_scores = {d: domain_score(d, lv, thresholds) for d, lv in domain_levels.items()}
    aggregate_penalty = 0
    for d, lv in domain_levels.items():
        if lv == "warning":
            aggregate_penalty += int(thresholds[d]["warning_penalty"])
        elif lv == "critical":
            aggregate_penalty += int(thresholds[d]["critical_penalty"])
    total_score = int(round(sum(domain_scores.values()) / len(DOMAINS)))
    health_score = max(0, 100 - aggregate_penalty)
    return {
        "status": "ok",
        "mode": "nightly",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_level": max_level(list(domain_levels.values())),
        "health_score": health_score,
        "total_score": total_score,
        "domain_levels": domain_levels,
        "domain_scores": domain_scores,
        "scoring_thresholds": thresholds,
        "threshold_explain": explain,
        "levels": {"nightly_external": domain_levels["nightly"]},
        "ci_signal_status": ci_signal_status,
    }


def domain_is_worse(prev: dict, cur: dict, domain: str, score_step: int) -> bool:
    pl = str((prev.get("domain_levels") or {}).get(domain, "ok"))
    cl = str((cur.get("domain_levels") or {}).get(domain, "ok"))
    if level_rank(cl) > level_rank(pl):
        return True
    if level_rank(cl) < level_rank(pl):
        return False
    ps = int((prev.get("domain_scores") or {}).get(domain, 100))
    cs = int((cur.get("domain_scores") or {}).get(domain, 100))
    return (ps - cs) >= score_step


def is_worse(prev: dict, cur: dict, score_step: int) -> bool:
    pl = str(prev.get("overall_level", "ok"))
    cl = str(cur.get("overall_level", "ok"))
    if level_rank(cl) > level_rank(pl):
        return True
    if level_rank(cl) < level_rank(pl):
        return False
    ps = int(prev.get("total_score", prev.get("health_score", 100)))
    cs = int(cur.get("total_score", cur.get("health_score", 100)))
    return (ps - cs) >= score_step


def apply_history(current: dict, args: argparse.Namespace, trend_cfg: dict) -> dict:
    history_path = Path(args.history) if args.history else None
    history = []
    if history_path and history_path.exists():
        history = json.loads(history_path.read_text(encoding="utf-8"))
    history.append({
        "timestamp_utc": current.get("generated_at_utc"),
        "mode": current.get("mode"),
        "overall_level": current.get("overall_level"),
        "health_score": current.get("health_score"),
        "total_score": current.get("total_score"),
        "domain_levels": current.get("domain_levels", {}),
        "domain_scores": current.get("domain_scores", {}),
    })
    history = history[-max(1, int(trend_cfg["history_limit"])):]

    worsening = 0
    if len(history) >= 2:
        for i in range(len(history) - 1, 0, -1):
            if is_worse(history[i-1], history[i], int(trend_cfg["score_step"])):
                worsening += 1
                continue
            break

    domain_worsening = {d: 0 for d in DOMAINS}
    if len(history) >= 2:
        for d in DOMAINS:
            cnt = 0
            for i in range(len(history) - 1, 0, -1):
                if domain_is_worse(history[i-1], history[i], d, int(trend_cfg["domain_score_step"])):
                    cnt += 1
                    continue
                break
            domain_worsening[d] = cnt

    trend_level = "ok"
    c_n = int(trend_cfg["consecutive_n"])
    if worsening >= c_n:
        trend_level = "critical"
    elif worsening >= max(1, c_n - 1):
        trend_level = "warning"
    for d in DOMAINS:
        if domain_worsening[d] >= c_n:
            trend_level = "critical"
        elif domain_worsening[d] >= max(1, c_n - 1):
            trend_level = merge_level(trend_level, "warning")

    current["trend"] = {
        "history_size": len(history),
        "consecutive_worsening": worsening,
        "domain_consecutive_worsening": domain_worsening,
        "consecutive_threshold": c_n,
        "score_step": int(trend_cfg["score_step"]),
        "domain_score_step": int(trend_cfg["domain_score_step"]),
        "trend_level": trend_level,
    }
    current["overall_level"] = max_level([current.get("overall_level", "ok"), trend_level])

    if history_path:
        out_path = Path(args.history_out) if args.history_out else history_path
        out_path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")
    return current


def main() -> None:
    ap = argparse.ArgumentParser(description="Cross-job trend regression gate")
    ap.add_argument("--mode", choices=["ci", "nightly"], required=True)
    ap.add_argument("--quality", default="")
    ap.add_argument("--gate", default="")
    ap.add_argument("--soak", default="")
    ap.add_argument("--analysis", default="")
    ap.add_argument("--ci-signal", default="")
    ap.add_argument("--nightly-signal", default="")
    ap.add_argument("--history", default="")
    ap.add_argument("--history-out", default="")
    ap.add_argument("--config", default="ecommerce_app/docs/CROSS_JOB_SCORING_CONFIG.json")
    ap.add_argument("--env", default="ci")
    ap.add_argument("--branch", default="")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    cfg, explain = load_config(args.config, args.env, args.branch)
    if args.mode == "ci":
        for req in [args.quality, args.gate, args.soak]:
            if not req:
                raise SystemExit("ci mode requires --quality --gate --soak")
        out = ci_mode(args, cfg, explain)
    else:
        if not args.analysis:
            raise SystemExit("nightly mode requires --analysis")
        out = nightly_mode(args, cfg, explain)
    out = apply_history(out, args, cfg["trend"])
    Path(args.out).write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()

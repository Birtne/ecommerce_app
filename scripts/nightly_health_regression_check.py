#!/usr/bin/env python3
from dataclasses import dataclass


@dataclass
class Sample:
    name: str
    score: int
    warning: int
    critical: int
    expected: str


def classify(score: int, warning: int, critical: int) -> str:
    if score <= critical:
        return "critical"
    if score <= warning:
        return "warning"
    return "ok"


def main() -> None:
    samples = [
        Sample("healthy", 92, 70, 50, "ok"),
        Sample("warning-border", 70, 70, 50, "warning"),
        Sample("critical-border", 50, 70, 50, "critical"),
        Sample("critical-low", 21, 70, 50, "critical"),
        Sample("warning-middle", 63, 70, 50, "warning"),
    ]
    for s in samples:
        got = classify(s.score, s.warning, s.critical)
        if got != s.expected:
            raise SystemExit(f"nightly health regression sample failed: {s.name} expected={s.expected} got={got}")
    print("nightly health regression samples passed")


if __name__ == "__main__":
    main()

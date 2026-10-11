#!/usr/bin/env python3
"""Enforce the constitution's per-file coverage floors."""

from collections import defaultdict
from pathlib import Path
import sys


PURE = {"config.go", "option.go", "errors.go"}
WRAPPERS = {"client.go", "tx.go", "gorm_logger.go"}


def main() -> int:
    profile = Path(sys.argv[1])
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for line in profile.read_text().splitlines()[1:]:
        location, statements, count = line.rsplit(" ", 2)
        name = Path(location.split(":", 1)[0]).name
        if name not in PURE | WRAPPERS:
            continue
        amount = int(statements)
        totals[name][0] += amount
        if int(count):
            totals[name][1] += amount

    failed = False
    for name in sorted(PURE | WRAPPERS):
        total, covered = totals[name]
        threshold = 85 if name in PURE else 70
        percent = 100 * covered / total if total else 0
        print(f"{name}: {percent:.1f}% (minimum {threshold}%)")
        if percent < threshold:
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Enforce the constitution's per-file coverage floors."""

from collections import defaultdict
from pathlib import Path
import sys


STORE_PURE = {"config.go", "option.go", "errors.go"}
STORE_WRAPPERS = {"client.go", "tx.go", "gorm_logger.go"}
EVENT_PURE = {"envelope.go", "publisher.go", "subscriber.go", "message.go", "errors.go", "middleware.go", "naming.go", "fake_publisher.go", "fake_subscriber.go"}
EVENT_WRAPPERS = {"config.go", "publisher.go", "subscriber.go", "mapper.go"}


def main() -> int:
    profile = Path(sys.argv[1])
    event_manager = profile.parent.name == "event-manager"
    pure = EVENT_PURE if event_manager else STORE_PURE
    wrappers = EVENT_WRAPPERS if event_manager else STORE_WRAPPERS
    totals: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for line in profile.read_text().splitlines()[1:]:
        location, statements, count = line.rsplit(" ", 2)
        source = location.split(":", 1)[0]
        name = Path(source).name
        if event_manager:
            relative = source.split("/event-manager/", 1)[-1]
            kind = "wrapper" if relative.startswith("kafka/") else "pure"
            if name not in (wrappers if kind == "wrapper" else pure):
                continue
            key = relative
        else:
            if name not in pure | wrappers:
                continue
            key = name
        if key == "":
            continue
        amount = int(statements)
        totals[key][0] += amount
        if int(count):
            totals[key][1] += amount

    failed = False
    expected = ([*("eventtest/" + n if n.startswith("fake_") else n for n in pure), *("kafka/" + n for n in wrappers)] if event_manager else list(pure | wrappers))
    for name in sorted(expected):
        total, covered = totals[name]
        threshold = 85 if (name in pure or name.startswith("eventtest/")) else 70
        percent = 100 * covered / total if total else 100
        print(f"{name}: {percent:.1f}% (minimum {threshold}%)")
        if percent < threshold:
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

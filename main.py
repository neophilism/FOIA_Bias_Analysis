"""Offline, import-safe entry point for the read-only FOIA corpus consumer.

The historical source collectors remain in legacy modules for migration only.
No default command imports or invokes them.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="FOIA Bias Analysis: read-only Aggregator corpus workflow (development)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("inspect", "run"):
        command = commands.add_parser(
            name,
            help=("Validate a corpus configuration offline" if name == "inspect"
                  else "Verify the complete read-only snapshot (no labeling or analysis)"),
        )
        command.add_argument(
            "--config", default="corpus-config.json",
            help="Read-only corpus JSON config; YAML requires optional PyYAML",
        )
    args = parser.parse_args(argv)
    try:
        from foia_bias.utils.corpus_config import load_config
        config = load_config(Path(args.config))
    except (FileNotFoundError, OSError, ValueError, ImportError) as exc:
        print(f"Invalid corpus configuration: {exc}", file=sys.stderr)
        return 2

    corpus = config["corpus"]
    cohort = config["cohort"]
    if args.command == "inspect":
        print(json.dumps({
            "mode": corpus["mode"],
            "manifest_path": corpus["manifest_path"],
            "fixture_mode": corpus["fixture_mode"],
            "batch_size": corpus["batch_size"],
            "cohort_start": cohort["start_date"],
            "cohort_end": cohort["end_date"],
            "status": "configuration-valid; snapshot-not-verified",
        }, sort_keys=True))
        return 0

    try:
        from foia_bias.corpus import verify_snapshot
        summary = verify_snapshot(config, args.config)
    except (OSError, ValueError, RecursionError) as exc:
        print(f"Invalid corpus snapshot: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


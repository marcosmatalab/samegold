"""CLI entry point for the crash campaign, so `make faults` is one command."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from pathlib import Path

from samegold.faults.harness import run_campaign
from samegold.generator.events import CI, FAST, generate
from samegold.generator.seeds import seeds_from_commit


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="samegold-faults")
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--profile", choices=("fast", "ci"), default="fast")
    parser.add_argument("--out", default=None)
    args = parser.parse_args(argv)

    work = Path(args.out or tempfile.mkdtemp(prefix="samegold-faults-"))
    profile = FAST if args.profile == "fast" else CI
    seed = seeds_from_commit(1, purpose="faults")[0]
    generate(work / "data", seed=seed, profile=profile)
    result = run_campaign(work / "data" / "bronze", work / "runs", repetitions=args.repetitions)
    print(json.dumps(result.to_json(), indent=2))
    if not args.out:
        shutil.rmtree(work, ignore_errors=True)
    return 1 if result.divergences or result.missed_injections else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

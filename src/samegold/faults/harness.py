"""Runs the crash campaign and turns it into a verdict.

For each reachable crash point, and for each repetition, the harness:

  1. runs the worker with the barrier armed, and asserts that it died with the barrier's
     exit code - a run that finished normally is a run that did not test anything, and it is
     reported as a MISSED INJECTION rather than as a pass;
  2. runs the worker again with the barrier disarmed, letting it resume from the checkpoint;
  3. digests the silver output and compares it with the digest of a clean run.

The bound published is a rule-of-three upper bound on the divergence rate per point, not the
word "always": zero divergences in n runs means the rate is at most -ln(0.05)/n, and with
the numbers this project can afford that is a percentage, not a proof.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from samegold.faults.barrier import EXIT_CODE
from samegold.faults.points import SILVER_POINTS
from samegold.verify.digest import Projection
from samegold.verify.stats import rule_of_three_upper

SILVER_PROJECTION = Projection(
    table="silver",
    columns=("event_id", "event_type", "event_ts", "quarantine_reason"),
    order_by=("event_id",),
)


@dataclass
class CampaignResult:
    runs: int = 0
    divergences: list[dict[str, Any]] = field(default_factory=list)
    missed_injections: list[dict[str, Any]] = field(default_factory=list)
    per_point: dict[str, dict[str, int]] = field(default_factory=dict)
    clean_digest: str = ""
    duration_s: float = 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "runs": self.runs,
            "points_covered": len(self.per_point),
            "points_total": len(SILVER_POINTS),
            "divergences": self.divergences,
            "missed_injections": self.missed_injections,
            "per_point": self.per_point,
            "clean_digest": self.clean_digest,
            "divergence_rate_upper95": (
                round(rule_of_three_upper(self.runs), 6)
                if self.runs and not self.divergences
                else None
            ),
            "duration_s": round(self.duration_s, 2),
        }


def _worker(bronze: Path, out: Path, env: dict[str, str], reset: bool) -> int:
    command = [
        sys.executable,
        "-m",
        "samegold.faults.worker",
        "--bronze",
        str(bronze),
        "--out",
        str(out),
    ]
    if reset:
        command.append("--reset")
    result = subprocess.run(
        command, env={**os.environ, **env}, capture_output=True, text=True, check=False
    )
    if result.returncode not in (0, EXIT_CODE):
        raise RuntimeError(
            f"the worker failed for a reason that is not the injected crash "
            f"(exit {result.returncode}):\n{result.stderr[-2000:]}"
        )
    return result.returncode


def _digest_silver(out: Path) -> str:
    """Read the silver output with a second engine, deduplicate it, and digest it.

    Two decisions worth stating.

    DuckDB rather than Spark reads it back, because the run that produced the data has just
    been killed and reading it with the engine that wrote it would share any assumption the
    writer made about what a complete file looks like.

    The deduplication on read is not cosmetic, and it is how a real property of the design
    was found: ``silver()`` deduplicates WITHIN a micro-batch, so two copies of one event
    that land in different batches both survive into silver. The first version of this
    function digested silver directly and the digest refused to be taken - "order_by
    ('event_id',) is not a total order" - which is the type system reporting a genuine
    modelling fact rather than a nuisance. Silver is append-only and may hold duplicates;
    uniqueness is a property of gold. The comparison after a crash therefore has to be made
    on the deduplicated view, which is exactly what the close consumes.

    The size of that cross-batch duplicate effect is not swept under the carpet: it is
    measured and published on its own as claim SG-08.
    """
    import duckdb

    from samegold.verify.digest import CanonicalDigest

    pattern = str(out / "silver" / "**" / "*.parquet")
    con = duckdb.connect()
    try:
        rows = con.execute(
            """
            SELECT event_id, event_type, event_ts, quarantine_reason FROM (
                SELECT event_id, event_type, event_ts, quarantine_reason,
                       row_number() OVER (PARTITION BY event_id
                                          ORDER BY event_ts, quarantine_reason) AS rn
                FROM read_parquet(?, union_by_name := true)
            ) WHERE rn = 1
            """,
            [pattern],
        ).fetchall()
    finally:
        con.close()
    mapped = [
        {"event_id": r[0], "event_type": r[1], "event_ts": r[2], "quarantine_reason": r[3]}
        for r in rows
    ]
    return CanonicalDigest.of(mapped, SILVER_PROJECTION).hexdigest


def run_campaign(
    bronze: Path, workdir: Path, repetitions: int = 3, points: tuple[str, ...] | None = None
) -> CampaignResult:
    started = time.monotonic()
    result = CampaignResult()

    clean_out = workdir / "clean"
    shutil.rmtree(clean_out, ignore_errors=True)
    _worker(bronze, clean_out, {"SAMEGOLD_CRASH_POINT": ""}, reset=True)
    result.clean_digest = _digest_silver(clean_out)

    selected = [p for p in SILVER_POINTS if points is None or p.name in points]
    for point in selected:
        stats = {"attempts": 0, "injected": 0, "converged": 0}
        for repetition in range(repetitions):
            out = workdir / f"{point.name}-{repetition}"
            shutil.rmtree(out, ignore_errors=True)
            stats["attempts"] += 1
            result.runs += 1
            code = _worker(
                bronze,
                out,
                {"SAMEGOLD_CRASH_POINT": point.name, "SAMEGOLD_CRASH_BATCH": str(repetition + 1)},
                reset=True,
            )
            if code != EXIT_CODE:
                result.missed_injections.append(
                    {
                        "point": point.name,
                        "repetition": repetition,
                        "detail": "the run finished without reaching the crash point; this "
                        "repetition tested nothing and is not counted as a pass",
                    }
                )
                continue
            stats["injected"] += 1
            _worker(bronze, out, {"SAMEGOLD_CRASH_POINT": ""}, reset=False)
            digest = _digest_silver(out)
            if digest == result.clean_digest:
                stats["converged"] += 1
            else:
                result.divergences.append(
                    {
                        "point": point.name,
                        "repetition": repetition,
                        "clean": result.clean_digest,
                        "after_crash": digest,
                    }
                )
        result.per_point[point.name] = stats
    result.duration_s = time.monotonic() - started
    return result

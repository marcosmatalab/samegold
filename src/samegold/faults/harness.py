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
from samegold.faults.points import GOLD_POINTS, REACHABLE, SILVER_POINTS
from samegold.verify.digest import Projection
from samegold.verify.stats import rule_of_three_upper

SILVER_PROJECTION = Projection(
    table="silver",
    columns=("event_id", "event_type", "event_ts", "quarantine_reason"),
    order_by=("event_id",),
)

# The second digest is what makes the experiment able to fail. Deduplicating before hashing
# answers "does the close still get the same answer", and that question is blind to a writer
# that wrote every row twice - an adversarial review copied a whole batch directory and the
# digest did not move. This one counts the copies of each event, so a non-idempotent writer
# changes it even when the business result is unchanged.
MULTISET_PROJECTION = Projection(
    table="silver_multiset",
    columns=("event_id", "copies"),
    order_by=("event_id",),
)


def _bound(trials: int, saw_divergence: bool) -> float | None:
    """Rule-of-three upper bound, or None when it would say nothing.

    With fewer than three injections the bound exceeds 1 and stops being a probability at
    all: the first version printed 1.4979 as a "rate", which is not a number anyone should
    put in a README.
    """
    if saw_divergence or trials < 3:
        return None
    return round(min(1.0, rule_of_three_upper(trials)), 6)


@dataclass
class CampaignResult:
    runs: int = 0
    injected: int = 0
    divergences: list[dict[str, Any]] = field(default_factory=list)
    missed_injections: list[dict[str, Any]] = field(default_factory=list)
    per_point: dict[str, dict[str, int]] = field(default_factory=dict)
    clean_digest: dict[str, str] = field(default_factory=dict)
    negative_control: dict[str, Any] = field(default_factory=dict)
    batches: int = 0
    duration_s: float = 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "runs": self.runs,
            "points_covered": len(self.per_point),
            "points_total": len(SILVER_POINTS),
            # The campaign injects at the points the SILVER writer owns. The gold-stage
            # points are reachable in principle and are not exercised, and saying so here is
            # the difference between "each structural point" (which the claim used to say,
            # and which was false: two of the four reachable points were never touched) and
            # a bound that means what it says.
            "reachable_points_total": len(REACHABLE),
            "reachable_points_not_covered": [point.name for point in GOLD_POINTS],
            "divergences": self.divergences,
            "missed_injections": self.missed_injections,
            "per_point": self.per_point,
            "clean_digest": self.clean_digest,
            "negative_control": self.negative_control,
            # Only INJECTED runs count towards the bound. Counting attempts that never
            # reached their crash point would publish a tighter interval for having tested
            # less, which is the wrong direction for a number to move.
            "injected_runs": self.injected,
            "divergence_rate_upper95_per_run": _bound(self.injected, bool(self.divergences)),
            "divergence_rate_upper95_per_point": {
                point: _bound(stats["injected"], any(d["point"] == point for d in self.divergences))
                for point, stats in self.per_point.items()
            },
            "batches_available": self.batches,
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


def _digest_silver(out: Path) -> dict[str, str]:
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
    con.execute("SET TimeZone='UTC'")
    try:
        content_rows = con.execute(
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
        multiset_rows = con.execute(
            "SELECT event_id, count(*) AS copies FROM read_parquet(?, union_by_name := true) "
            "GROUP BY event_id",
            [pattern],
        ).fetchall()
    finally:
        con.close()
    content = CanonicalDigest.of(
        [
            {"event_id": r[0], "event_type": r[1], "event_ts": r[2], "quarantine_reason": r[3]}
            for r in content_rows
        ],
        SILVER_PROJECTION,
    )
    multiset = CanonicalDigest.of(
        [{"event_id": r[0], "copies": int(r[1])} for r in multiset_rows], MULTISET_PROJECTION
    )
    return {"content": content.hexdigest, "multiset": multiset.hexdigest}


def _negative_control(bronze: Path, workdir: Path, clean: dict[str, str]) -> dict[str, Any]:
    """Crash a deliberately non-idempotent writer and check that the harness notices."""
    shutil.rmtree(workdir, ignore_errors=True)
    code = _worker(
        bronze,
        workdir,
        {
            "SAMEGOLD_CRASH_POINT": "after_batch_write_before_commit",
            # Batch 1 exists in every profile this runs on (the clean run produces eight),
            # and batches are numbered from zero, so this is the second one.
            "SAMEGOLD_CRASH_BATCH": "1",
            "SAMEGOLD_WRITER": "append",
        },
        reset=True,
    )
    if code != EXIT_CODE:
        return {"status": "inconclusive", "detail": "the control run never reached its crash point"}
    _worker(bronze, workdir, {"SAMEGOLD_CRASH_POINT": "", "SAMEGOLD_WRITER": "append"}, reset=False)
    digest = _digest_silver(workdir)
    detected_by = [key for key in ("content", "multiset") if digest[key] != clean[key]]
    return {
        "status": "detected" if detected_by else "NOT DETECTED",
        "writer": "append (non-idempotent on replay)",
        "detected_by": detected_by,
        "digest": digest,
        "note": (
            "the content digest deduplicates by event_id and is blind to double writes; the "
            "multiset digest is the one that has to catch this"
        ),
    }


def run_campaign(
    bronze: Path, workdir: Path, repetitions: int = 3, points: tuple[str, ...] | None = None
) -> CampaignResult:
    started = time.monotonic()
    result = CampaignResult()

    clean_out = workdir / "clean"
    shutil.rmtree(clean_out, ignore_errors=True)
    _worker(bronze, clean_out, {"SAMEGOLD_CRASH_POINT": ""}, reset=True)
    result.clean_digest = _digest_silver(clean_out)

    # The negative control: the same campaign against a writer that appends instead of
    # overwriting. A crash after a partial write leaves rows behind, the replay appends them
    # again, and the multiset digest has to notice. If this control ever comes back clean,
    # the campaign above is measuring nothing and the whole claim is void.
    # How many micro-batches the clean run actually produced: the crash schedule cannot ask
    # for a batch that does not exist.
    batches = len(list((clean_out / "silver").glob("batch_id=*")))
    result.batches = batches
    result.negative_control = _negative_control(bronze, workdir / "negative", result.clean_digest)
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
                {
                    "SAMEGOLD_CRASH_POINT": point.name,
                    # Spark numbers micro-batches from ZERO, and the campaign asked for
                    # batch `repetition + 1`. With eight batches and ten repetitions that
                    # requested batches 8, 9 and 10, none of which exist: six of twenty runs
                    # reported "missed injection" and the claim failed for a reason that had
                    # nothing to do with the pipeline. The schedule now cycles over the
                    # batches the clean run actually produced.
                    "SAMEGOLD_CRASH_BATCH": str(repetition % max(1, batches)),
                },
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
            result.injected += 1
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

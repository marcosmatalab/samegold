"""Every published claim, as a function that returns evidence.

One function per claim, each returning an ``EvidenceRecord``. Nothing here prints; nothing
here writes to the README. The CLI runs them, the store appends them, the renderer draws
them. That separation is what makes ``make readme`` unable to invent a number.

Claim ids are stable and are cited from the README, from EXAM_MAP.md and from PARITY.md.
A claim that cannot run in a given runtime is not silently skipped: it returns a record with
``outcome = fail`` and a counterexample explaining what was missing, so an unrunnable claim
looks worse than a failing one rather than disappearing.
"""

from __future__ import annotations

import datetime as dt
import json
import shutil
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from samegold.evidence.record import EvidenceRecord
from samegold.generator.events import CI, FAST, FULL, Profile, generate
from samegold.generator.seeds import current_commit_sha, seed_source, seeds_from_commit
from samegold.mutation.runner import run_mutation_campaign
from samegold.oracle.duckdb_gold import DuckDBWitness, reference_counts, scd2_as_of
from samegold.verify.digest import REVENUE_PROJECTION, SCD2_PROJECTION, CanonicalDigest
from samegold.verify.invariants import (
    conservation,
    net_identity,
    restatement_monotonic,
    returns_never_exceed_sales,
    scd2_well_formed,
)
from samegold.verify.verdict import Counterexample, Fail, Pass, Rate, RunSet, now_iso

PROFILES: dict[str, Profile] = {"fast": FAST, "ci": CI, "full": FULL}
REFERENCE_SQL = Path(__file__).parent / "oracle" / "gold_revenue.sql"


def _runset(seeds: Sequence[int], profile: str, started: float, runtime: str) -> RunSet:
    return RunSet(
        n=len(seeds),
        seeds=tuple(seeds),
        commit_sha=current_commit_sha(),
        seed_source=seed_source(),  # type: ignore[arg-type]
        profile=profile,
        started_at=now_iso(),
        duration_s=time.monotonic() - started,
        runtime=runtime,  # type: ignore[arg-type]
    )


def _revenue_rows(witness: DuckDBWitness, bronze: Path, as_of: dt.datetime) -> list[dict[str, Any]]:
    return [
        {"accounting_month": month, "close_version": 0, **values}
        for month, values in sorted(witness.revenue(bronze, as_of).items())
    ]


# --------------------------------------------------------------------- SG-01


def claim_witness_agreement(
    work: Path, profile_name: str = "fast", runs: int = 3
) -> EvidenceRecord:
    """SG-01. Two derivations of the close agree, at every close, on every seed.

    What this shows: the DuckDB reference and the generator's ledger, which share the
    contract and share no code, compute the same close.

    What this does NOT show, and the record says so in ``not_claimed``: that either of them
    is right. Both are written by the same person from the same understanding, so a
    misreading of the contract lands in both. The specification mutants (SG-03) are the
    experiment that puts a number on that blind spot.
    """
    started = time.monotonic()
    seeds = seeds_from_commit(runs, purpose="witness")
    witness = DuckDBWitness()
    comparisons = agreements = 0
    counter: Counterexample | None = None
    for index, seed in enumerate(seeds):
        root = work / f"witness-{index}"
        shutil.rmtree(root, ignore_errors=True)
        result = generate(root, seed=seed, profile=PROFILES[profile_name])
        for close in result.ledger.closes:
            as_of = dt.datetime.fromisoformat(close)
            got = witness.revenue(root / "bronze", as_of)
            expected = {m: v for (m, a), v in result.ledger.revenue.items() if a == close}
            comparisons += 1
            if got == expected:
                agreements += 1
            elif counter is None:
                months = sorted(set(got) | set(expected))
                first = next(m for m in months if got.get(m) != expected.get(m))
                counter = Counterexample(
                    "SG-01",
                    seed,
                    f"witness and ledger disagree at close {close} for month {first}",
                    {"duckdb": got.get(first), "ledger": expected.get(first)},
                )
    rate = Rate(agreements, comparisons)
    runset = _runset(seeds, profile_name, started, "oss-local")
    verdict = (
        Pass("SG-01", runset, rate, "every close, every seed")
        if counter is None
        else Fail("SG-01", runset, counter, rate)
    )
    return EvidenceRecord(
        claim_id="SG-01",
        title="two implementations agree on the close",
        verdict=verdict,
        runtime="oss-local",
        artifacts={"comparisons": comparisons, "closes_per_run": comparisons // max(1, len(seeds))},
        not_claimed=(
            "that either implementation is correct: they share an author and a contract",
            "that agreement transfers to the Spark implementation (see SG-07)",
        ),
    )


# --------------------------------------------------------------------- SG-02


def claim_redelivery_is_a_noop(
    work: Path, profile_name: str = "fast", runs: int = 3
) -> EvidenceRecord:
    """SG-02. Re-delivering every input file under a new path does not move the close.

    The realistic incident: a producer replays a day, or a copy job runs twice, and the same
    events land again under new names. A pipeline that deduplicates on the file path (a very
    common shortcut, and specification mutant SPEC-03) reports the day twice.

    The experiment compares the canonical digest of gold before and after re-delivery.
    It is a statement about content-keyed deduplication, not about exactly-once delivery:
    nothing here says the pipeline processes a record once, only that processing it twice
    leaves the same answer.
    """
    started = time.monotonic()
    seeds = seeds_from_commit(runs, purpose="redelivery")
    witness = DuckDBWitness()
    same = total = 0
    counter: Counterexample | None = None
    for index, seed in enumerate(seeds):
        root = work / f"redelivery-{index}"
        shutil.rmtree(root, ignore_errors=True)
        result = generate(root, seed=seed, profile=PROFILES[profile_name])
        as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
        before = CanonicalDigest.of(
            _revenue_rows(witness, root / "bronze", as_of), REVENUE_PROJECTION
        )
        for path in sorted((root / "bronze").rglob("*.json")):
            shutil.copyfile(path, path.with_name("replay-" + path.name))
        after = CanonicalDigest.of(
            _revenue_rows(witness, root / "bronze", as_of), REVENUE_PROJECTION
        )
        total += 1
        if before.agrees_with(after):
            same += 1
        elif counter is None:
            counter = Counterexample(
                "SG-02",
                seed,
                "the close moved after re-delivering identical content",
                {
                    "before": before.hexdigest,
                    "after": after.hexdigest,
                    "rows_before": before.row_count,
                    "rows_after": after.row_count,
                },
            )
    rate = Rate(same, total)
    runset = _runset(seeds, profile_name, started, "oss-local")
    verdict = (
        Pass("SG-02", runset, rate, "digest unchanged after full re-delivery")
        if counter is None
        else Fail("SG-02", runset, counter, rate)
    )
    return EvidenceRecord(
        claim_id="SG-02",
        title="re-delivery under a new path is a no-op",
        verdict=verdict,
        runtime="oss-local",
        artifacts={"files_duplicated": "all"},
        not_claimed=(
            "exactly-once processing: this is at-least-once plus content-keyed dedup",
            "that a duplicate arriving beyond the streaming watermark is caught; that is "
            "measured separately in SG-07 on the Spark lane, where the state can expire",
        ),
    )


# --------------------------------------------------------------------- SG-03


def claim_mutation_campaign(work: Path, profile_name: str = "fast") -> EvidenceRecord:
    """SG-03. Mechanically generated mutants, plus specification mutants, past every witness.

    Published twice on purpose: with the equivalence classification accepted, and with it
    refused. Survivors are listed by id, and the per-witness marginal contribution says
    whether the second witness is earning its keep.
    """
    started = time.monotonic()
    seeds = seeds_from_commit(1, purpose="mutation")
    root = work / "mutation"
    shutil.rmtree(root, ignore_errors=True)
    result = generate(root, seed=seeds[0], profile=PROFILES[profile_name])
    ledger = json.loads((root / "truth" / "ledger.json").read_text(encoding="utf-8"))
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    run = run_mutation_campaign(
        REFERENCE_SQL.read_text(encoding="utf-8"), root / "bronze", ledger, closes
    )
    matrix = run.matrix.to_json()
    scored, killed = int(matrix["mutants_scored"]), int(matrix["killed"])
    total = int(matrix["mutants_total"])
    rate = Rate(killed, scored)
    runset = _runset(seeds, profile_name, started, "oss-local")
    survivors = list(matrix["survivors"])
    verdict = (
        Pass("SG-03", runset, rate, f"{len(matrix['equivalent'])} classified equivalent")
        if not survivors
        else Fail(
            "SG-03",
            runset,
            Counterexample(
                "SG-03",
                seeds[0],
                f"{len(survivors)} mutants survive every witness",
                {"survivors": survivors},
            ),
            rate,
        )
    )
    return EvidenceRecord(
        claim_id="SG-03",
        title="mutation campaign",
        verdict=verdict,
        runtime="oss-local",
        artifacts={
            "strict_score": round(killed / total, 4),
            "mutants_total": total,
            "equivalent": len(matrix["equivalent"]),
            "kappa": matrix["kappa"],
            "per_witness": {k: v["killed"] for k, v in matrix["per_witness"].items()},
            "marginal": {k: len(v["marginal"]) for k, v in matrix["per_witness"].items()},
            "matrix": matrix,
        },
        not_claimed=(
            "that a high score means the pipeline is correct: mutants are a lower bound on "
            "the faults a suite can see, never a proof of absence",
            "that the equivalence classification is above suspicion: the strict score, which "
            "refuses it entirely, is published next to it",
        ),
    )


# --------------------------------------------------------------------- SG-04


def claim_restatement_magnitude(work: Path, profile_name: str = "fast") -> EvidenceRecord:
    """SG-04. How much of a closed month moves after it is closed. A business number.

    This is the claim a finance stakeholder actually cares about, and it is a measurement
    rather than a pass/fail: the value is the share of net revenue that changed between the
    first close of a month and its final state. A pipeline that cannot restate would report
    the first number for ever and be wrong by exactly this much.
    """
    started = time.monotonic()
    seeds = seeds_from_commit(1, purpose="restatement")
    root = work / "restatement"
    shutil.rmtree(root, ignore_errors=True)
    result = generate(root, seed=seeds[0], profile=PROFILES[profile_name])
    by_month: dict[str, list[tuple[str, dict[str, int]]]] = {}
    for (month, as_of), values in sorted(result.ledger.revenue.items()):
        # A month's baseline is its OWN close, not the first close in which it happens to
        # appear. At the close of January, February exists with one day of data in it;
        # measuring how much February "moved" from that partial figure produces percentages
        # over 100% that mean nothing. The baseline is the first close after the month ends.
        if as_of[:7] <= month:
            continue
        by_month.setdefault(month, []).append((as_of, values))
    by_month = {m: series for m, series in by_month.items() if len(series) >= 2}
    moved: list[dict[str, Any]] = []
    for month, series in by_month.items():
        first, last = series[0][1], series[-1][1]
        if first["net_cents"] != last["net_cents"]:
            delta = last["net_cents"] - first["net_cents"]
            moved.append(
                {
                    "accounting_month": month,
                    "first_close_net_cents": first["net_cents"],
                    "final_net_cents": last["net_cents"],
                    "delta_cents": delta,
                    "delta_pct": round(100.0 * delta / first["net_cents"], 4)
                    if first["net_cents"]
                    else None,
                    "versions": len({json.dumps(v, sort_keys=True) for _, v in series}),
                }
            )
    if not by_month:
        raise RuntimeError(
            "no month has been closed twice in this profile, so there is nothing to measure; "
            "use a profile with at least two closes after the first month"
        )
    rate = Rate(len(moved), len(by_month))
    runset = _runset(seeds, profile_name, started, "oss-local")
    worst = max((abs(m["delta_pct"] or 0.0) for m in moved), default=0.0)
    return EvidenceRecord(
        claim_id="SG-04",
        title="a closed month moves after it is closed",
        verdict=Pass("SG-04", runset, rate, f"largest move {worst:.2f}% of the first close"),
        runtime="oss-local",
        artifacts={"months_that_moved": moved, "worst_move_pct": round(worst, 4)},
        not_claimed=(
            "that these percentages describe real retail: they describe this simulation, "
            "whose return rate is deliberately higher than a real one",
        ),
    )


# --------------------------------------------------------------------- SG-05


def claim_dimension_invariants(
    work: Path, profile_name: str = "fast", runs: int = 3
) -> EvidenceRecord:
    """SG-05. The SCD2 dimension is well formed on every seed, with no oracle involved.

    The only family of checks in the project that needs neither a second implementation nor
    the generator's ledger: intervals disjoint and contiguous, exactly one open row per key,
    version numbering dense, net = gross - returns. A reader who trusts nothing else in this
    repository can still run these.
    """
    started = time.monotonic()
    seeds = seeds_from_commit(runs, purpose="invariants")
    witness = DuckDBWitness()
    checks = clean = 0
    counter: Counterexample | None = None
    digests: list[str] = []
    for index, seed in enumerate(seeds):
        root = work / f"invariants-{index}"
        shutil.rmtree(root, ignore_errors=True)
        result = generate(root, seed=seed, profile=PROFILES[profile_name])
        as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
        scd2 = scd2_as_of(root / "bronze", as_of)
        revenue = _revenue_rows(witness, root / "bronze", as_of)
        counts = reference_counts(root / "bronze")
        violations = (
            scd2_well_formed(scd2)
            + net_identity(revenue)
            + restatement_monotonic(revenue)
            + returns_never_exceed_sales(revenue)
            + conservation(
                ingested=counts["raw_lines"],
                accepted=counts["accepted"],
                quarantined=counts["rejected_by_rule"]
                + counts["unparseable"]
                + counts["no_event_id"],
                rescued=0,
                deduplicated=counts["duplicates"],
            )
        )
        checks += 1
        if not violations:
            clean += 1
        elif counter is None:
            counter = Counterexample(
                "SG-05",
                seed,
                "invariant violated",
                {"first": violations[0], "count": len(violations)},
            )
        digests.append(CanonicalDigest.of(scd2, SCD2_PROJECTION).hexdigest)
    rate = Rate(clean, checks)
    runset = _runset(seeds, profile_name, started, "oss-local")
    verdict = (
        Pass("SG-05", runset, rate, "no oracle involved")
        if counter is None
        else Fail("SG-05", runset, counter, rate)
    )
    return EvidenceRecord(
        claim_id="SG-05",
        title="dimension and conservation invariants hold without an oracle",
        verdict=verdict,
        runtime="oss-local",
        artifacts={"scd2_digests": digests},
        not_claimed=(
            "that the dimension carries the right attribute values: an invariant sees shape, "
            "not truth",
        ),
    )


# --------------------------------------------------------------------- SG-06


def claim_seed_provenance() -> EvidenceRecord:
    """SG-06. The seeds behind every number are derived from the commit, not chosen.

    A meta-claim, and the cheapest one to verify: it recomputes the seeds from the commit
    SHA and checks they match what the other claims used. It exists because every other
    number in this repository is worthless if the author can choose the seed.
    """
    started = time.monotonic()
    sha = current_commit_sha()
    seeds = seeds_from_commit(3, purpose="witness")
    recomputed = seeds_from_commit(3, purpose="witness", sha=sha)
    ok = seeds == recomputed and seed_source() == "commit"
    runset = _runset(seeds, "n/a", started, "oss-local")
    rate = Rate(1 if ok else 0, 1)
    verdict = (
        Pass("SG-06", runset, rate, f"seeds derived from {sha[:12]}")
        if ok
        else Fail(
            "SG-06",
            runset,
            Counterexample(
                "SG-06",
                seeds[0],
                "seeds do not derive from the commit; this run used an override",
                {"seed_source": seed_source()},
            ),
            rate,
        )
    )
    return EvidenceRecord(
        claim_id="SG-06",
        title="seeds are derived from the commit",
        verdict=verdict,
        runtime="oss-local",
        artifacts={"commit_sha": sha, "seed_source": seed_source()},
    )


ALL_CLAIMS = ("SG-01", "SG-02", "SG-03", "SG-04", "SG-05", "SG-06")

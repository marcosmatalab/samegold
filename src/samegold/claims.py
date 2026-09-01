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
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from samegold.domain.bitemporal import accounting_month_of
from samegold.evidence.record import EvidenceRecord
from samegold.generator.events import CI, FAST, FULL, Profile, generate
from samegold.generator.seeds import current_commit_sha, seed_source, seeds_from_commit
from samegold.mutation.assumption_probe import (
    probe_data_assumption,
    probe_order_free_comparison,
    probe_orphan_returns_are_excluded,
    probe_structural_assumption,
)
from samegold.mutation.runner import run_mutation_campaign
from samegold.oracle.duckdb_gold import DuckDBWitness, reference_counts, scd2_as_of
from samegold.verify.digest import (
    REVENUE_PROJECTION,
    SCD2_PROJECTION,
    CanonicalDigest,
)
from samegold.verify.invariants import (
    conservation,
    conservation_against_ledger,
    net_identity,
    restatement_monotonic,
    returns_never_exceed_sales,
    scd2_well_formed,
)
from samegold.verify.verdict import (
    Counterexample,
    Fail,
    Pass,
    Rate,
    RunSet,
    Verdict,
    now_iso,
)

PROFILES: dict[str, Profile] = {"fast": FAST, "ci": CI, "full": FULL}
REFERENCE_SQL = Path(__file__).parent / "oracle" / "gold_revenue.sql"


def _runset(
    seeds: Sequence[int], profile: str, started: float, runtime: str, purpose: str
) -> RunSet:
    return RunSet(
        n=len(seeds),
        seeds=tuple(seeds),
        commit_sha=current_commit_sha(),
        seed_source=seed_source(),  # type: ignore[arg-type]
        seed_purpose=purpose,
        profile=profile,
        started_at=now_iso(),
        duration_s=time.monotonic() - started,
        runtime=runtime,  # type: ignore[arg-type]
    )


def _versioned_rows(bronze: Path, closes: list[dt.datetime]) -> list[dict[str, Any]]:
    """The real gold table: one row per (accounting_month, close_version)."""
    from samegold.oracle.duckdb_gold import revenue_versions

    return revenue_versions(bronze, closes)


# --------------------------------------------------------------------- SG-00


def claim_repository_facts(repo_root: Path | None = None) -> EvidenceRecord:
    """SG-00. The counts this repository prints about itself, measured rather than typed.

    Every "127 tests" in a README is a number that was true once. This claim collects the
    test count per lane, the module and line counts, and the wall time of the fast lane, and
    the documents render those figures through the evidence anchors, so a stale number is a
    failing test rather than a small embarrassment in front of a reviewer who counted.
    """
    started = time.monotonic()
    root = repo_root or Path(__file__).resolve().parents[2]
    seeds = seeds_from_commit(1, purpose="facts")

    def collected(path: str) -> int:
        out = subprocess.run(
            [sys.executable, "-m", "pytest", path, "--collect-only", "-q", "--no-header"],
            capture_output=True,
            text=True,
            cwd=root,
            check=False,
        )
        for line in reversed(out.stdout.splitlines()):
            if "test" in line and "collected" in line:
                return int(line.split()[0])
            if line.strip().endswith("tests collected") or line.strip().endswith("test collected"):
                return int(line.split()[0])
        return sum(1 for line in out.stdout.splitlines() if "::" in line)

    fast_started = time.monotonic()
    # One test is deselected, and it has to be: it asserts that the documents match the
    # evidence, and this claim runs while that evidence is being written. The CI order is
    # `evidence`, then `readme`, then `fast` with nothing deselected, so the check does run -
    # just not inside the thing it checks.
    deselected = (
        "tests/fast/test_evidence_gate.py::"
        "test_the_repository_documents_are_consistent_with_its_evidence"
    )
    fast_run = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/fast",
            "-q",
            "--no-header",
            "--deselect",
            deselected,
        ],
        capture_output=True,
        text=True,
        cwd=root,
        check=False,
    )
    fast_seconds = time.monotonic() - fast_started

    python_files = sorted(p for p in (root / "src").rglob("*.py") if "__pycache__" not in str(p))
    test_files = sorted(p for p in (root / "tests").rglob("*.py") if "__pycache__" not in str(p))
    source_lines = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in python_files)
    test_lines = sum(len(p.read_text(encoding="utf-8").splitlines()) for p in test_files)
    docs = sorted(root.glob("*.md")) + sorted((root / "docs").rglob("*.md"))

    facts: dict[str, Any] = {
        "tests_fast": collected("tests/fast"),
        "tests_spark": collected("tests/spark"),
        "tests_delta": collected("tests/delta"),
        "fast_lane_seconds": round(fast_seconds, 1),
        "fast_lane_green": fast_run.returncode == 0,
        "python_modules": len(python_files),
        "source_lines": source_lines,
        "test_lines": test_lines,
        "markdown_documents": len(docs),
        "adrs": len(list((root / "docs" / "adr").glob("*.md"))),
        "deselected_in_this_run": deselected,
    }
    runset = _runset(seeds, "n/a", started, "oss-local", "facts")
    verdict: Verdict = (
        Pass(
            "SG-00",
            runset,
            Rate(int(facts["tests_fast"]), int(facts["tests_fast"])),
            "fast lane green",
        )
        if fast_run.returncode == 0
        else Fail(
            "SG-00",
            runset,
            Counterexample(
                "SG-00", seeds[0], "the fast lane is red", {"tail": fast_run.stdout[-600:]}
            ),
        )
    )
    return EvidenceRecord(
        claim_id="SG-00",
        title="what this repository contains, counted",
        verdict=verdict,
        runtime="oss-local",
        artifacts=facts,
        not_claimed=("that line counts measure anything about quality",),
    )


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
    runset = _runset(seeds, profile_name, started, "oss-local", "witness")
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
    same = total = 0
    counter: Counterexample | None = None
    for index, seed in enumerate(seeds):
        root = work / f"redelivery-{index}"
        shutil.rmtree(root, ignore_errors=True)
        result = generate(root, seed=seed, profile=PROFILES[profile_name])
        closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
        before = CanonicalDigest.of(_versioned_rows(root / "bronze", closes), REVENUE_PROJECTION)
        for path in sorted((root / "bronze").rglob("*.json")):
            shutil.copyfile(path, path.with_name("replay-" + path.name))
        after = CanonicalDigest.of(_versioned_rows(root / "bronze", closes), REVENUE_PROJECTION)
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
    runset = _runset(seeds, profile_name, started, "oss-local", "redelivery")
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


def _assert_mutation_shapes_exist(root: Path, result: Any, profile_name: str) -> None:
    """Refuse to score a mutation campaign on data that cannot distinguish its mutants.

    The same discipline as the cost lab's probe-existence guard. A mutant that survives
    because the dataset contains none of the shape it changes is not evidence about the
    witnesses; it is evidence about the generator, and publishing it as a survivor sends a
    reader looking for a bug that is not there. The one shape that has actually bitten is a
    line with two amendments at distinct event times, which is what separates "the last
    amendment wins" from "the first one does".
    """
    amendments: dict[tuple[str, str], set[str]] = {}
    for path in sorted((root / "bronze").rglob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if '"order_line_amended"' not in line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = (str(record.get("order_id")), str(record.get("sku")))
            amendments.setdefault(key, set()).add(str(record.get("event_ts")))
    if not any(len(times) >= 2 for times in amendments.values()):
        raise ValueError(
            f"profile {profile_name!r} produced no order line with two amendments at distinct "
            f"event times, so the amendment-ordering mutants cannot be scored on it. Use a "
            f"larger profile rather than publishing them as survivors."
        )
    if not result.ledger.closes:
        raise ValueError(f"profile {profile_name!r} produced no closes to compare")


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
    # The campaign always runs on at least the CI profile, whatever the caller asked for. On
    # the fast profile the dataset happens to contain no order line with two amendments at
    # distinct event times before the close, so SQL-053 - which flips the amendment window
    # from "the last amendment wins" to "the first one does", a real change of meaning -
    # survives every witness and is reported as a genuine survivor. Nothing is wrong with the
    # mutant or with the witnesses; the DATA cannot tell them apart. A score that depends on
    # which profile a reader happened to pass is not a score.
    campaign_profile = profile_name if profile_name in ("ci", "full") else "ci"
    result = generate(root, seed=seeds[0], profile=PROFILES[campaign_profile])
    _assert_mutation_shapes_exist(root, result, campaign_profile)
    ledger = json.loads((root / "truth" / "ledger.json").read_text(encoding="utf-8"))
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    run = run_mutation_campaign(
        REFERENCE_SQL.read_text(encoding="utf-8"), root / "bronze", ledger, closes
    )
    matrix = run.matrix.to_json()
    # Every equivalence class carries an assumption id, and every assumption gets a control
    # here rather than only inside a unit test. Publishing them is the difference between
    # "these mutants are harmless" and "these mutants are harmless while this named property
    # holds, and here is the run that tries to break it". An adversarial review pointed out
    # that CLAIMS.md promised the second and the record contained neither.
    reference_sql = REFERENCE_SQL.read_text(encoding="utf-8")
    last_close = result.ledger.closes[-1]
    probes = [
        probe_data_assumption(reference_sql),
        probe_structural_assumption(root / "bronze", last_close),
        probe_order_free_comparison(reference_sql, root / "bronze", last_close),
        probe_orphan_returns_are_excluded(reference_sql, root / "bronze", last_close),
    ]
    scored, killed = int(matrix["mutants_scored"]), int(matrix["killed"])
    total = int(matrix["mutants_total"])
    rate = Rate(killed, scored)
    runset = _runset(seeds, profile_name, started, "oss-local", "mutation")
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
            "assumption_probes": probes,
            # Named, not hidden: a mutant whose assumption the probe could not falsify keeps
            # its classification and is published as unfalsified, because "I could not break
            # it" and "it cannot be broken" are different statements.
            "mutants_the_probe_could_not_falsify": sorted(
                {
                    mutant
                    for probe in probes
                    for mutant in probe.get("mutants_the_probe_could_not_falsify", [])
                }
            ),
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
        # In the accounting timezone, like every other month key in this project: a close
        # just after midnight in Madrid is still the previous month in UTC, and the string
        # prefix would drop a real close.
        if accounting_month_of(as_of) <= month:
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
    runset = _runset(seeds, profile_name, started, "oss-local", "restatement")
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
    checks = clean = 0
    counter: Counterexample | None = None
    digests: list[str] = []
    for index, seed in enumerate(seeds):
        root = work / f"invariants-{index}"
        shutil.rmtree(root, ignore_errors=True)
        result = generate(root, seed=seed, profile=PROFILES[profile_name])
        as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
        closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
        scd2 = scd2_as_of(root / "bronze", as_of)
        revenue = _versioned_rows(root / "bronze", closes)
        counts = reference_counts(root / "bronze")
        ledger_counts = json.loads((root / "truth" / "ledger.json").read_text(encoding="utf-8"))[
            "counts"
        ]
        violations = (
            scd2_well_formed(scd2)
            + net_identity(revenue)
            + restatement_monotonic(revenue)
            + returns_never_exceed_sales(revenue)
            + conservation(
                # ingested comes from the GENERATOR's ledger, not from the same query that
                # produces the other three. Taking all of them from `reference_counts` makes
                # the identity algebraic - substitute the SQL definitions and the sum reduces
                # to raw_lines for any input at all - so it passed on every seed the way
                # 1 = 1 passes. A second review pointed out that this repair leaves exactly
                # one independent comparison in it, "the generator wrote as many lines as the
                # reference found", which is true and is why the four-way
                # conservation_against_ledger below is the check that carries the weight.
                ingested=int(ledger_counts["events_written"]),
                accepted=counts["accepted"],
                # `unparseable` already includes the rows the reader turned into an
                # all-NULL record, which is what `no_event_id` counts; adding both counted
                # the same line twice.
                quarantined=counts["rejected_by_rule"] + counts["unparseable"],
                rescued=0,
                deduplicated=counts["duplicates"],
            )
            + conservation_against_ledger(ledger_counts, counts)
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
    runset = _runset(seeds, profile_name, started, "oss-local", "invariants")
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


def claim_seed_provenance(evidence_dir: Path | None = None) -> EvidenceRecord:
    """SG-06. Every record in the history derives its seeds from the commit it names, and the
    chain has not been edited.

    The first version of this claim was circular: it recomputed the seeds and compared them
    with themselves. It passed on a repository whose evidence could be, and was, forged by
    appending a line to a JSON file. This version verifies the artefact instead of the
    function: the hash chain over history.jsonl, and the seed derivation of every record in
    it, including the ones written before this claim ran.

    Why it exists at all: every other number in this repository is worthless if the author
    can choose the seed or edit the record afterwards.
    """
    from samegold.evidence.store import EvidenceStore

    started = time.monotonic()
    sha = current_commit_sha()
    seeds = seeds_from_commit(1, purpose="provenance")
    repo_root = Path(__file__).resolve().parents[2]
    store = EvidenceStore(evidence_dir or (repo_root / "evidence"))
    breaks = store.verify_chain(repo_root)
    records = list(store.read_history())
    runset = _runset(seeds, "n/a", started, "oss-local", "provenance")
    # A history with no records verifies vacuously; saying 0/1 would report a healthy chain
    # as a failure.
    rate = Rate(len(records) - len(breaks), len(records)) if records else Rate(1, 1)
    verdict: Verdict
    if breaks:
        verdict = Fail(
            "SG-06",
            runset,
            Counterexample(
                "SG-06",
                seeds[0],
                f"{len(breaks)} record(s) in the evidence history do not verify",
                {"first": str(breaks[0]), "all": [str(b) for b in breaks[:10]]},
            ),
            rate,
        )
    else:
        verdict = Pass(
            "SG-06", runset, rate, f"{len(records)} records verified against commit {sha[:12]}"
        )
    return EvidenceRecord(
        claim_id="SG-06",
        title="the evidence chain verifies and every seed derives from its commit",
        verdict=verdict,
        runtime="oss-local",
        artifacts={
            "commit_sha": sha,
            "seed_source": seed_source(),
            "records_verified": len(records),
            "chain_breaks": [str(b) for b in breaks],
        },
        not_claimed=(
            "that a record marked as produced in CI really was: nothing offline can check "
            "that a run URL exists. The gate checks the shape and the commit; the renderer "
            "prints anything without one as a local run",
        ),
    )


def claim_crash_campaign(
    work: Path, repetitions: int = 3, profile_name: str = "fast"
) -> EvidenceRecord:
    """SG-07. The pipeline is killed at each structural point and has to converge.

    Two digests are compared, and the second one is why this claim can fail at all: the
    content digest deduplicates by event_id, so it is blind to a writer that wrote every row
    twice, and an adversarial review proved that blindness by copying a whole batch directory
    without moving the number. The multiset digest counts the copies.

    The negative control runs the same campaign against a writer that appends instead of
    overwriting - the hopeful version most pipelines ship - and the claim FAILS if the
    harness does not notice it. A crash test that cannot fail is a screenshot.
    """
    from samegold.faults.harness import run_campaign

    started = time.monotonic()
    seeds = seeds_from_commit(1, purpose="faults")
    root = work / "faults"
    shutil.rmtree(root, ignore_errors=True)
    generate(root / "data", seed=seeds[0], profile=PROFILES[profile_name])
    campaign = run_campaign(root / "data" / "bronze", root / "runs", repetitions=repetitions)
    payload = campaign.to_json()
    runset = _runset(seeds, profile_name, started, "oss-local", "faults")
    rate = Rate(campaign.injected - len(campaign.divergences), max(1, campaign.injected))
    control_ok = payload["negative_control"].get("status") == "detected"
    verdict: Verdict
    if campaign.divergences:
        verdict = Fail(
            "SG-07",
            runset,
            Counterexample(
                "SG-07",
                seeds[0],
                "the gold digest moved after a crash and restart",
                campaign.divergences[0],
            ),
            rate,
        )
    elif not control_ok:
        verdict = Fail(
            "SG-07",
            runset,
            Counterexample(
                "SG-07",
                seeds[0],
                "the negative control was not detected: the harness cannot tell an "
                "idempotent writer from a non-idempotent one, so its passes mean nothing",
                payload["negative_control"],
            ),
            rate,
        )
    elif campaign.missed_injections:
        verdict = Fail(
            "SG-07",
            runset,
            Counterexample(
                "SG-07",
                seeds[0],
                f"{len(campaign.missed_injections)} run(s) never reached their crash "
                f"point and therefore tested nothing",
                campaign.missed_injections[0],
            ),
            rate,
        )
    else:
        verdict = Pass("SG-07", runset, rate, "negative control detected, no divergences")
    return EvidenceRecord(
        claim_id="SG-07",
        title="the silver writer survives a crash at each of its structural points",
        verdict=verdict,
        runtime="oss-local",
        artifacts=payload,
        not_claimed=(
            "crash safety of the engine: the reachable points are the ones the writer owns, "
            "and the points inside a Delta commit or a state-store checkpoint are listed in "
            "faults/points.py as not covered",
            "anything about Databricks: serverless gives you no process to kill",
        ),
    )


def claim_privacy_controls(work: Path, profile_name: str = "fast") -> EvidenceRecord:
    """SG-08. No direct identifier reaches gold, and a purge actually purges.

    Three controls, executed rather than declared, because the platform that would enforce
    them cannot: Free Edition has no account groups, so a row filter based on
    ``is_account_group_member`` is a policy nobody is subject to.

      * the column policy masks every direct identifier on the way into gold;
      * the exposure check refuses gold rows that carry one anyway, including one hiding
        under a different column name;
      * the retention purge deletes the expired rows AND vacuums the files that held them,
        because a DELETE alone leaves them readable through time travel.
    """
    import datetime as dt

    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    from samegold.governance.policy import apply_policy, check_gold_exposure
    from samegold.governance.retention import purge_expired, residual_in_transaction_log

    started = time.monotonic()
    seeds = seeds_from_commit(1, purpose="privacy")
    root = work / "privacy"
    shutil.rmtree(root, ignore_errors=True)
    result = generate(root, seed=seeds[0], profile=PROFILES[profile_name])
    as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
    dimension = scd2_as_of(root / "bronze", as_of)

    masked = [apply_policy(row, salt="samegold-demo-salt") for row in dimension]
    leaks_masked = check_gold_exposure(masked)
    leaks_raw = check_gold_exposure(dimension)

    table_uri = str(root / "retention")
    days = sorted({str(row["valid_from"])[:10] for row in dimension})
    write_deltalake(
        table_uri,
        pa.table(
            {
                "customer_id": [row["customer_id"] for row in dimension],
                "event_day": [str(row["valid_from"])[:10] for row in dimension],
            }
        ),
        mode="overwrite",
        # Statistics are restricted to the column that needs them. VACUUM removes data files
        # and leaves the transaction log alone, and the log carries per-file min/max values:
        # an adversarial review found real customer identifiers sitting in the minValues of a
        # committed log entry AFTER the purge. Keeping the identifier out of the statistics is
        # the fix; residual_in_transaction_log is the check.
        configuration={"delta.dataSkippingStatsColumns": "event_day"},
    )
    # The retention horizon is chosen relative to the data the profile actually produced: a
    # fixed 30 days over a 14-day simulation deletes nothing and the claim passes vacuously,
    # which is how this was found. A quarter of the span keeps rows on both sides of the line.
    span = (dt.date.fromisoformat(days[-1]) - dt.date.fromisoformat(days[0])).days or 1
    horizon = dt.datetime.fromisoformat(days[-1] + "T00:00:00+00:00") + dt.timedelta(days=1)
    purge = purge_expired(table_uri, "event_day", retention_days=max(1, span // 4), now=horizon)
    survivors = DeltaTable(table_uri).to_pyarrow_table().to_pylist()
    still_visible = [row for row in survivors if row["event_day"] < purge["cutoff"]]
    identifiers = sorted(
        {
            str(row["customer_id"])
            for row in dimension
            if str(row["valid_from"])[:10] < purge["cutoff"]
        }
    )
    residual = residual_in_transaction_log(table_uri, identifiers)

    runset = _runset(seeds, profile_name, started, "oss-local", "privacy")
    checks = {
        "raw dimension is refused": bool(leaks_raw),
        "masked dimension passes": not leaks_masked,
        "purge deleted rows": purge["rows_deleted"] > 0,
        "purge vacuumed files": purge["files_removed_by_vacuum"] > 0,
        "no expired row survives": not still_visible,
        "no purged identifier is left in the transaction log": not residual,
    }
    failed = [name for name, ok in checks.items() if not ok]
    rate = Rate(len(checks) - len(failed), len(checks))
    verdict: Verdict = (
        Pass("SG-08", runset, rate, "masking, exposure check and purge")
        if not failed
        else Fail(
            "SG-08",
            runset,
            Counterexample(
                "SG-08",
                seeds[0],
                f"privacy controls failed: {failed}",
                {"leaks": leaks_masked[:3], "purge": purge},
            ),
            rate,
        )
    )
    return EvidenceRecord(
        claim_id="SG-08",
        title="no direct identifier reaches gold, and a purge really purges",
        verdict=verdict,
        runtime="oss-local",
        artifacts={
            "dimension_rows": len(dimension),
            "leaks_in_raw_dimension": len(leaks_raw),
            "leaks_in_masked_dimension": len(leaks_masked),
            "purge": purge,
            "identifiers_left_in_the_log": residual[:5],
        },
        not_claimed=(
            "that these controls are enforced by a platform: they run in code here, and the "
            "Databricks lane declares the equivalent row filter and column mask in SQL for a "
            "workspace that has account groups. Free Edition does not",
            "that the generated data contains real personal data: it does not, which is why "
            "the check looks for the SHAPE of an identifier rather than for a real one",
        ),
    )


def claim_cost_lab(work: Path, repetitions: int = 2) -> EvidenceRecord:
    """SG-09. What file layout costs, measured in files and bytes rather than in seconds.

    Wall time on a laptop measures the laptop. These numbers come from the per-file statistics
    in the Delta log, which is what data skipping actually uses.

    The headline is a SHARE, not a byte ratio. Z-ORDER rewrites and recompresses, so the two
    arms of the clustering experiment do not end up with the same bytes on disk and comparing
    raw byte counts mixes skipping with compression - an adversarial review caught exactly
    that. The share of its own table each arm has to read is comparable; the raw counts are
    published next to it.

    The lab is run more than once because the byte counts are not perfectly reproducible: the
    parquet writer varies by a fraction of a per cent between runs. The file counts are, and
    the spread of the bytes is published rather than rounded away.

    The result the lab likes best is a negative one: clustering by (month, sku) does nothing
    for a sku predicate when the table has three files, because three files cover the whole
    key range. It pays only once there are files to skip.
    """
    from samegold.cost.lab import lab_dataset, run_lab

    started = time.monotonic()
    seeds = seeds_from_commit(1, purpose="cost")
    rows = lab_dataset(seed=seeds[0] % (2**31))
    runs = [
        run_lab(rows, work / f"cost-{index}", month="2026-03", sku="SKU-00042")
        for index in range(max(1, repetitions))
    ]
    measurements = runs[0]

    def small(run: dict[str, Any]) -> dict[str, Any]:
        section: dict[str, Any] = run["COST-02"]["small_files"]
        return section

    shares = [(small(run)["share_unclustered"], small(run)["share_clustered"]) for run in runs]
    reduction = [round(100.0 * (before - after) / before, 2) for before, after in shares if before]
    file_counts = [
        (
            run["COST-01"]["before"]["files_total"],
            run["COST-01"]["after"]["files_total"],
            small(run)["clustered"]["files_not_skippable"],
        )
        for run in runs
    ]
    runset = _runset(seeds, "lab", started, "oss-local", "cost")
    checks = {
        "compaction removed files": measurements["COST-01"]["files_removed_pct"] > 0,
        "clustering reduced the share read at small file sizes": min(reduction) > 0,
        "clustering did nothing at large file sizes (the negative result)": (
            measurements["COST-02"]["large_files"]["share_clustered"] >= 0.99
        ),
        "partitioning served the month predicate": measurements["COST-03"][
            "partitioned_files_for_month"
        ]
        < measurements["COST-03"]["partitioned_files_total"],
        "file counts are reproducible across runs": len(set(file_counts)) == 1,
    }
    failed = [name for name, ok in checks.items() if not ok]
    rate = Rate(len(checks) - len(failed), len(checks))
    verdict: Verdict = (
        Pass("SG-09", runset, rate, f"clustering cut the share read by {min(reduction)}%")
        if not failed
        else Fail(
            "SG-09",
            runset,
            Counterexample(
                "SG-09", seeds[0], f"a layout experiment did not behave: {failed}", measurements
            ),
            rate,
        )
    )
    return EvidenceRecord(
        claim_id="SG-09",
        title="what layout costs, in files and bytes",
        verdict=verdict,
        runtime="oss-local",
        artifacts={
            "share_read_reduction_pct": min(reduction),
            "share_read_reduction_pct_range": [min(reduction), max(reduction)],
            "files_removed_by_compaction_pct": measurements["COST-01"]["files_removed_pct"],
            "rows_copied_per_row_deleted": measurements["COST-04"]["rows_copied_per_row_deleted"],
            "repetitions": len(runs),
            "file_counts_identical_across_runs": len(set(file_counts)) == 1,
            "measurements": measurements,
        },
        not_claimed=(
            "anything about query latency: no timing is measured, on purpose",
            "anything about DBU cost: system.billing needs an account console that Free "
            "Edition does not have, and wall time is not a substitute",
            "that clustering always helps: at large file sizes it did nothing here, and the "
            "measurement that says so is a passing check rather than a footnote",
            "that the byte counts are bit-for-bit reproducible: the parquet writer varies by "
            "about a tenth of a per cent between runs, and the range is published",
        ),
    )


# Order matters, and it is not alphabetical. SG-00 runs the fast lane, which contains the
# tests that read the evidence directory, so it has to come after the claims that write it.
# SG-06 verifies the whole chain, so it comes last of all.
ALL_CLAIMS = (
    "SG-01",
    "SG-02",
    "SG-03",
    "SG-04",
    "SG-05",
    "SG-08",
    "SG-09",
    "SG-00",
    "SG-06",
)
# SG-07 needs a JVM and about fifteen minutes, so it is not in the default set. `make faults`
# and the CI evidence workflow run it explicitly.
SLOW_CLAIMS = ("SG-07",)

# What a refutation run covers. SG-00 counts the repository and SG-06 verifies the evidence
# chain: neither is a statement about the data, and running them under a seed override
# produced a "failure" that meant nothing except that the override was working.
REFUTABLE_CLAIMS = ("SG-01", "SG-02", "SG-03", "SG-04", "SG-05", "SG-08", "SG-09")

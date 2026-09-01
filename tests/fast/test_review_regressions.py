"""One test per defect found by the third adversarial review, in the order they were found.

These are not unit tests of features. Each one is a bug that was live in a committed,
documented, green-suite repository, and each one is here so that the same class of mistake
fails loudly rather than being found again by a reader. The comments say what the wrong
behaviour was, because a regression test whose name is the only record of the bug decays into
a test nobody dares delete and nobody understands.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

import pytest

from samegold.domain.bitemporal import accounting_month_of, versions_from_snapshots
from samegold.evidence.registry import CLAIM_TITLES
from samegold.generator.events import FAST, generate
from samegold.governance.anonymise import generalize_timestamp
from samegold.mutation.operators import mutate_sql
from samegold.oracle.duckdb_gold import reference_counts
from samegold.serve.freshness import close_deadline, evaluate_freshness, overdue_months
from samegold.serve.report import render_report
from samegold.verify.invariants import conservation_against_ledger

REPO = Path(__file__).resolve().parents[2]

VALUES = {
    "gross_cents": 100,
    "returns_cents": 0,
    "net_cents": 100,
    "line_count": 1,
    "return_count": 0,
    "returns_rejected_count": 0,
}


def _values(net: int) -> dict[str, int]:
    return {**VALUES, "gross_cents": net, "net_cents": net}


# ------------------------------------------------------------------ the accounting month


def test_a_close_just_after_midnight_in_madrid_closes_the_previous_month() -> None:
    """`as_of[:7]` read the month in UTC, so this close vanished entirely.

    2026-02-01 00:30 Europe/Madrid is 2026-01-31T23:30:00+00:00. The string prefix says
    "2026-01", so January was judged still open and produced no version at all. The same
    instant written as a Madrid-local string produced one. An answer that depends on how a
    timestamp was spelled is not an answer.
    """
    as_of_utc = "2026-01-31T23:30:00+00:00"
    as_of_madrid = "2026-02-01T00:30:00+01:00"
    assert accounting_month_of(as_of_utc) == accounting_month_of(as_of_madrid) == "2026-02"
    snapshot = [("2026-01", _values(100))]
    for spelling in (as_of_utc, as_of_madrid):
        versions = versions_from_snapshots([(spelling, dict(snapshot))])
        assert [v["accounting_month"] for v in versions] == ["2026-01"], spelling


def test_the_version_history_does_not_depend_on_the_order_of_the_closes() -> None:
    """The function iterated the caller's list. The Spark twin sorts by as_of.

    Passing the same two closes in the other order produced a history running backwards,
    with both digests describing themselves as the same bookkeeping.
    """
    closes = [
        ("2026-02-05T22:59:59+00:00", {"2026-01": _values(100)}),
        ("2026-03-05T22:59:59+00:00", {"2026-01": _values(200)}),
    ]
    forward = versions_from_snapshots(closes)
    backward = versions_from_snapshots(list(reversed(closes)))
    assert forward == backward
    assert [v["net_cents"] for v in forward] == [100, 200]


def test_anonymised_periods_use_the_accounting_timezone() -> None:
    """It read the calendar fields off the raw value, so it disagreed with the close.

    2026-03-31T22:30Z is April in Madrid. Generalising it to "2026-03" makes every anonymised
    aggregate fail to reconcile with the close at every month boundary.
    """
    boundary = dt.datetime(2026, 3, 31, 22, 30, tzinfo=dt.UTC)
    assert generalize_timestamp(boundary) == "2026-04"
    assert generalize_timestamp(boundary, "day") == "2026-04-01"


# ------------------------------------------------------------------ the freshness rule


def test_every_missing_close_is_reported_not_only_the_most_recent() -> None:
    """The rule derived exactly one month key, so a backlog reported one gap and hid the rest.

    With January closed and April running, February's missing close could not be reported by
    any call to this function, ever.
    """
    now = dt.datetime(2026, 4, 10, 12, tzinfo=dt.UTC)
    breaches = evaluate_freshness(now - dt.timedelta(minutes=1), ["2026-01"], now)
    months = [b.detail.split()[0] for b in breaches if b.kind == "close_overdue"]
    assert months == ["2026-02", "2026-03"]


def test_the_close_deadline_is_an_instant_in_madrid_not_in_the_caller_s_timezone() -> None:
    """It was midnight in `now`'s timezone, which overstated every lag by the Madrid offset.

    The close of January falls at the end of 5 February, Europe/Madrid: 23:00Z in winter.
    """
    assert close_deadline("2026-01", close_day=5) == dt.datetime(2026, 2, 5, 23, tzinfo=dt.UTC)
    # And in summer, where the offset is two hours rather than one.
    assert close_deadline("2026-06", close_day=5) == dt.datetime(2026, 7, 5, 22, tzinfo=dt.UTC)


def test_a_deployment_with_no_close_on_record_does_not_report_a_backlog() -> None:
    """A bounded, useful alert rather than three years of noise nobody reads."""
    now = dt.datetime(2026, 4, 10, 12, tzinfo=dt.UTC)
    assert len(overdue_months(now, [])) == 1


# ------------------------------------------------------------------ the report


def test_the_report_counts_restated_months_not_months_whose_net_moved() -> None:
    """It highlighted rows by version and counted months by net, so the two disagreed.

    A restatement where gross and returns both rise by the same amount leaves net unchanged:
    the table showed an orange restated row with a change of 0,00 while the sentence above it
    said "0 of 1 months moved after they were signed off".
    """
    versions = [
        {
            "accounting_month": "2026-01",
            "close_version": 0,
            "gross_cents": 1000,
            "returns_cents": 0,
            "net_cents": 1000,
            "restatement_reason": "first close",
            "restated_at": "2026-02-05T22:59:59+00:00",
        },
        {
            "accounting_month": "2026-01",
            "close_version": 1,
            "gross_cents": 1500,
            "returns_cents": 500,
            "net_cents": 1000,
            "restatement_reason": "late arrivals after close",
            "restated_at": "2026-03-05T22:59:59+00:00",
        },
    ]
    page = render_report(versions, dt.datetime(2026, 3, 6, 9, tzinfo=dt.UTC))
    assert "1 of 1 months were restated" in page
    assert 'class="restated"' in page


# ------------------------------------------------------------------ the invariants


def test_the_conservation_cross_check_compares_two_derivations(tmp_path: Path) -> None:
    """The old call took all five counts from one query, making the identity algebraic.

    Substituting the definitions in `_COUNTS_SQL` reduces the sum to `raw_lines` for any
    input at all, so it passed on every seed the way 1 = 1 passes. This version compares the
    generator's own record of what it wrote with the reference's recount of it, and the
    first time it ran it found a real discrepancy of 38 events.
    """
    result = generate(tmp_path / "g", seed=7, profile=FAST)
    counts = json.loads((tmp_path / "g" / "truth" / "ledger.json").read_text(encoding="utf-8"))[
        "counts"
    ]
    assert conservation_against_ledger(counts, reference_counts(tmp_path / "g" / "bronze")) == []
    # And it can fail: a ledger that under-counts by one is a violation, not a rounding.
    broken = dict(counts, unique_events=int(counts["unique_events"]) - 1)
    assert conservation_against_ledger(broken, reference_counts(tmp_path / "g" / "bronze"))
    assert result.ledger.closes


# ------------------------------------------------------------------ the mutation operators


def test_the_hash_width_is_not_a_mutation_target() -> None:
    """sqlglot parses sha256(x) as SHA2(x, 256); bumping it yields an invalid program.

    DuckDB answers "DuckDB only supports SHA256 hashing algorithm", which is the parser
    refusing a query, not a fault the pipeline could have. It appeared as a phantom surviving
    mutant the moment the tie-break moved from md5 to sha256.
    """
    sql = (REPO / "src" / "samegold" / "oracle" / "gold_revenue.sql").read_text(encoding="utf-8")
    assert "sha256(" in sql, "the anchor for this test moved"
    assert [m.mutant_id for m in mutate_sql(sql) if m.original == "256"] == []


# ------------------------------------------------------------------ the Databricks lane


@pytest.mark.parametrize(
    "name,fragment",
    [
        ("gold_close.py", "returns_rejected_count"),
        ("close_month.py", "returns_rejected_count"),
        ("close_month.py", "from_utc_timestamp"),
    ],
)
def test_the_databricks_lane_produces_the_contract_columns(name: str, fragment: str) -> None:
    """Its close emitted six of the seven columns and had no as-of rule at all.

    The statements themselves are parsed by a real Spark in tests/spark; this fast test only
    guards the two contract properties that a parser cannot see.
    """
    source = (REPO / "databricks" / "src" / name).read_text(encoding="utf-8")
    assert fragment in source


# ------------------------------------------------------------------ the fourth review
#
# Everything below is a defect introduced or missed by the FIXES above. A review of a repair
# is worth more than a review of the original, because a repair is written by someone who has
# just convinced themselves they understand the problem.


def test_the_close_history_sorts_by_instant_not_by_the_text_of_the_timestamp() -> None:
    """Sorting the ISO strings fixed the wrong half of the ordering bug.

    "2026-02-01T00:30:00+01:00" is EARLIER than "2026-01-31T23:45:00+00:00" as an instant and
    later as text, so version 0 was the later close and the history ran backwards. The
    monotonic invariant could not see it because it sorted the same way.
    """
    early = "2026-02-01T00:30:00+01:00"  # 2026-01-31T23:30Z
    late = "2026-01-31T23:45:00+00:00"
    assert early > late, "the premise of this test is that text order and time order differ"
    closes = [(early, {"2026-01": _values(100)}), (late, {"2026-01": _values(200)})]
    versions = versions_from_snapshots(closes)
    assert [v["restated_at"] for v in versions] == [early, late]
    assert [v["net_cents"] for v in versions] == [100, 200]


def test_restatement_monotonic_compares_instants() -> None:
    """Same bug, in the invariant that was supposed to notice it."""
    from samegold.verify.invariants import restatement_monotonic

    # Ascending as TEXT, descending as instants: 23:45Z then 00:30+01:00 (= 23:30Z).
    backwards = [
        {
            "accounting_month": "2026-01",
            "close_version": 0,
            "restated_at": "2026-01-31T23:45:00+00:00",
        },
        {
            "accounting_month": "2026-01",
            "close_version": 1,
            "restated_at": "2026-02-01T00:30:00+01:00",
        },
    ]
    assert [r["restated_at"] for r in backwards] == sorted(r["restated_at"] for r in backwards)
    assert restatement_monotonic(backwards), "a history running backwards must be a violation"


def test_a_month_end_close_day_does_not_crash_the_freshness_rule() -> None:
    """`datetime(2026, 2, 31)` raises, and it took the whole evaluation down with it.

    "The month closes at month end" is an ordinary choice, and 29, 30 and 31 all name months
    that do not have that day.
    """
    for day in (28, 29, 30, 31):
        assert close_deadline("2026-01", close_day=day).month == 2
    now = dt.datetime(2026, 4, 10, 12, tzinfo=dt.UTC)
    assert evaluate_freshness(now - dt.timedelta(minutes=1), ["2026-03"], now, close_day=31) == []


def test_one_malformed_month_key_does_not_silence_the_overdue_alert() -> None:
    """`min()` over unvalidated strings: '2026-03' < '2026-1', so the walk stopped at once.

    The failure direction is the dangerous one. The rule returned an empty list and the alert
    reported healthy while a close was missing.
    """
    now = dt.datetime(2026, 4, 10, 12, tzinfo=dt.UTC)
    assert [m for m, _ in overdue_months(now, ["2026-1", "2026-02"])] == ["2026-03"]
    assert [m for m, _ in overdue_months(now, ["nonsense"])] == ["2026-03"]


def test_the_reference_survives_an_integer_larger_than_bigint(tmp_path: Path) -> None:
    """json_type reports 2^63 as UBIGINT, which a plain CAST then dies on.

    The `typed` CTE was added to remove a divergence and reintroduced the "one record with no
    door" failure it had just fixed, one commit later.
    """
    from samegold.oracle.duckdb_gold import revenue_by_month_as_of

    bronze = tmp_path / "bronze" / "batch=1"
    bronze.mkdir(parents=True)
    good = {
        "event_id": "op-1",
        "event_type": "order_placed",
        "event_ts": "2026-01-10T10:00:00+00:00",
        "arrival_ts": "2026-01-10T10:05:00+00:00",
        "order_id": "O1",
        "customer_id": "C1",
        "sku": "S1",
        "qty": 2,
        "unit_price_cents": 1000,
        "currency": "EUR",
    }
    overflowing = dict(good, event_id="op-2", order_id="O2", qty=2**63)
    (bronze / "part-00000.json").write_text(
        json.dumps(good) + "\n" + json.dumps(overflowing) + "\n", encoding="utf-8"
    )
    rows = revenue_by_month_as_of(tmp_path / "bronze", dt.datetime(2026, 12, 31, tzinfo=dt.UTC))
    assert [(r.accounting_month, r.gross_cents, r.line_count) for r in rows] == [
        ("2026-01", 2000, 1)
    ]
    assert reference_counts(tmp_path / "bronze")["accepted"] == 1


def test_the_counts_query_breaks_ties_the_same_way_the_close_does(tmp_path: Path) -> None:
    """The tie-break was applied to two of the three queries, which is worse than to none.

    With only event_ts in its ORDER BY, swapping two lines in one file flipped this query's
    buckets while the revenue query did not move: the accounting reported zero accepted
    records for a close that booked two thousand cents.
    """
    base = {
        "event_id": "op-1",
        "event_type": "order_placed",
        "event_ts": "2026-01-10T10:00:00+00:00",
        "arrival_ts": "2026-01-10T10:05:00+00:00",
        "order_id": "O1",
        "customer_id": "C1",
        "sku": "S1",
        "unit_price_cents": 1000,
        "currency": "EUR",
    }
    good, bad = dict(base, qty=2), dict(base, qty=-1)
    buckets = []
    for order in ([good, bad], [bad, good]):
        root = tmp_path / f"b{len(buckets)}" / "bronze" / "batch=1"
        root.mkdir(parents=True)
        (root / "part-00000.json").write_text(
            "\n".join(json.dumps(row) for row in order) + "\n", encoding="utf-8"
        )
        counts = reference_counts(root.parent)
        buckets.append((counts["accepted"], counts["rejected_by_rule"]))
    assert buckets[0] == buckets[1], f"the accounting depends on the order of the lines: {buckets}"


def test_the_unparseable_counter_counts_unparseable_lines(tmp_path: Path) -> None:
    """It was structurally always zero, for the one case it exists to count.

    With the columns declared, DuckDB reads a truncated line into an all-NULL row instead of
    dropping it, so `raw_lines - parsed_rows` is zero and the record was reported through the
    `no_event_id` door, which the Spark pipeline does not have.
    """
    bronze = tmp_path / "bronze" / "batch=1"
    bronze.mkdir(parents=True)
    (bronze / "part-00000.json").write_text(
        '{"event_id": "op-1", "event_type": "order_pl\n', encoding="utf-8"
    )
    assert reference_counts(tmp_path / "bronze")["unparseable"] == 1


def test_the_databricks_expectations_use_the_contract_reasons() -> None:
    """Its rules used that lane's own vocabulary rather than the contract's closed enum.

    The NULL-safety half of this check used to be `"IS NULL OR" not in block`, a substring
    grep that never evaluated a predicate: it passed while `event_type IN (...)` silently
    accepted a NULL event_type, and then failed on the CORRECT fix
    (`event_type IS NULL OR ...`), which is the signature of a test measuring text rather
    than behaviour. The behaviour is now compared against `quarantine_reason()` over a matrix
    of records in tests/spark, where a Spark session can evaluate both. This test keeps only
    the part that needs no engine: the names.
    """
    from samegold.domain.contract import QuarantineReason

    source = (REPO / "databricks" / "src" / "silver_expectations.py").read_text(encoding="utf-8")
    block = source[source.index("RULES = {") : source.index("@dp.table")]
    names = set(re.findall(r'^    "([a-z_]+)":', block, flags=re.MULTILINE))
    declared = {str(reason) for reason in QuarantineReason}
    unexpected = names - declared
    assert not unexpected, f"rule names that are not quarantine reasons: {sorted(unexpected)}"
    assert len(names) >= 6, f"only {sorted(names)} are declared as expectations"


# ------------------------------------------------------------------ the sixth review


def test_the_ledgers_dimension_is_the_dimension_the_rule_produces(tmp_path: Path) -> None:
    """`Ledger.dim_customer` is documented as "known by construction". It was not.

    The ledger collapsed customer versions that shared a valid_from and stopped there; the
    project's own rule (`domain.bitemporal.scd2_from_versions`) also collapses ADJACENT
    versions with identical attributes, because a Type 2 dimension records changes and not
    heartbeats. So the ledger claimed a few more rows than a correct implementation produces,
    on every seed - 80 against 77 on one, 88 against 78 on another.

    It survived six review rounds because nothing read it: the SCD2 claims compare the two
    ENGINES with each other and never with the ledger, which is the one artefact that is
    supposed to say what the answer is rather than compute it. This test reads it.
    """
    from samegold.domain.bitemporal import scd2_from_versions

    result = generate(tmp_path / "g", seed=11, profile=FAST)
    ledger = result.ledger.dim_customer
    assert ledger, "the generator produced no customer versions to compare"
    for customer_id, versions in ledger.items():
        expected = scd2_from_versions(
            [
                {
                    "customer_id": customer_id,
                    "valid_from": str(version["valid_from"]),
                    "segment": version["segment"],
                    "country": version["country"],
                    "event_id": str(version.get("event_id", "")),
                }
                for version in versions
            ]
        )
        assert [str(v["valid_from"]) for v in versions] == [
            str(row["valid_from"]) for row in expected
        ], f"{customer_id}: the ledger and the rule disagree about the version history"


def test_the_digest_refuses_a_type_it_cannot_encode_unambiguously() -> None:
    """The type tag only helped for types that had one; everything else fell to str().

    Which is the tag `str` uses, so the fallback recreated the collision the tags exist to
    prevent: a UUID and its own string form digested identically while comparing unequal, and
    two equal dicts with different insertion orders digested differently.
    """
    import uuid

    from samegold.verify.digest import Projection, ProjectionError, digest_rows

    projection = Projection(table="t", columns=("k", "v"), order_by=("k",))
    identifier = uuid.UUID("00000000-0000-0000-0000-000000000001")
    with pytest.raises(ProjectionError, match="unambiguously"):
        digest_rows([{"k": "a", "v": identifier}], projection)
    with pytest.raises(ProjectionError, match="unambiguously"):
        digest_rows([{"k": "a", "v": {"x": 1}}], projection)
    # And the string form, which used to collide with the UUID, is still perfectly fine.
    assert digest_rows([{"k": "a", "v": str(identifier)}], projection).row_count == 1


def test_the_digest_refuses_a_non_finite_decimal() -> None:
    """float('nan') was refused and Decimal('NaN') was digested silently; sNaN raised
    InvalidOperation out of the encoder rather than a ProjectionError."""
    from decimal import Decimal

    from samegold.verify.digest import Projection, ProjectionError, digest_rows

    projection = Projection(table="t", columns=("k", "v"), order_by=("k",))
    for value in (Decimal("NaN"), Decimal("Infinity"), Decimal("sNaN")):
        with pytest.raises(ProjectionError, match="non-finite"):
            digest_rows([{"k": "a", "v": value}], projection)


def test_the_evidence_store_refuses_a_claim_it_does_not_define(tmp_path: Path) -> None:
    """A record for a claim that does not exist was accepted and rendered as a table row."""
    from samegold.evidence.record import EvidenceRecord
    from samegold.evidence.store import EvidenceRejected, EvidenceStore
    from samegold.generator.seeds import current_commit_sha, current_tree, seeds_from_commit
    from samegold.verify.verdict import Pass, Rate, RunSet

    sha = current_commit_sha()

    def record(claim_id: str, title: str, purpose: str = "witness") -> EvidenceRecord:
        seeds = tuple(seeds_from_commit(1, purpose, sha=sha))
        runs = RunSet(
            n=1,
            seeds=seeds,
            commit_sha=sha,
            tree_sha=current_tree()[0],
            tree_dirty=current_tree()[1],
            seed_source="commit",
            seed_purpose=purpose,
            profile="fast",
            started_at="2026-09-01T00:00:00+00:00",
            duration_s=1.0,
            runtime="oss-local",
        )
        return EvidenceRecord(claim_id, title, Pass(claim_id, runs, Rate(1, 1)), "oss-local")

    store = EvidenceStore(tmp_path)
    with pytest.raises(EvidenceRejected, match="not a claim this repository defines"):
        store.append(record("SG-DOES-NOT-EXIST", "anything"))
    with pytest.raises(EvidenceRejected, match="renames its own claim"):
        store.append(record("SG-01", "a much better sounding title"))
    with pytest.raises(EvidenceRejected, match="not one of the purposes"):
        store.append(record("SG-01", CLAIM_TITLES["SG-01"], purpose="witness-attempt-0"))


def test_the_evidence_store_refuses_a_number_that_is_not_json(tmp_path: Path) -> None:
    """json.dumps writes NaN and Infinity as bare tokens, which no other JSON reader accepts.

    The chain verified while the append-only file the whole argument rests on had become
    unreadable to `jq`, `JSON.parse`, `serde_json` and Go's `encoding/json`.
    """
    from samegold.evidence.record import EvidenceRecord
    from samegold.evidence.store import EvidenceRejected, EvidenceStore
    from samegold.generator.seeds import current_commit_sha, current_tree, seeds_from_commit
    from samegold.verify.verdict import Pass, Rate, RunSet

    sha = current_commit_sha()
    runs = RunSet(
        n=1,
        seeds=tuple(seeds_from_commit(1, "witness", sha=sha)),
        commit_sha=sha,
        tree_sha=current_tree()[0],
        tree_dirty=current_tree()[1],
        seed_source="commit",
        seed_purpose="witness",
        profile="fast",
        started_at="2026-09-01T00:00:00+00:00",
        duration_s=1.0,
        runtime="oss-local",
    )
    bad = EvidenceRecord(
        "SG-01",
        CLAIM_TITLES["SG-01"],
        Pass("SG-01", runs, Rate(1, 1)),
        "oss-local",
        artifacts={"share": float("nan")},
    )
    with pytest.raises(EvidenceRejected, match="non-finite"):
        EvidenceStore(tmp_path).append(bad)


def test_the_renderer_refuses_a_value_that_would_break_the_document(tmp_path: Path) -> None:
    """An artifact value containing the anchor terminator closed the anchor early.

    The rest of the value landed outside it, the next render appended it again, the document
    grew on every `make readme`, and the drift gate could then only be satisfied by hand
    editing the document, which is the exact act the module exists to prevent.
    """
    from samegold.evidence.render import render_readme

    latest = {
        "SG-01": {
            "claim_id": "SG-01",
            "title": "two implementations agree on the close",
            "runtime": "oss-local",
            "artifacts": {"x": "99.9%<!--/sg--> and INJECTED TEXT"},
            "verdict": {"outcome": "pass", "runs": {}, "rate": None},
        }
    }
    with pytest.raises(ValueError, match="comment delimiter"):
        render_readme("<!--sg:SG-01.artifact.x-->?<!--/sg-->\n", latest)


def test_the_hardcoded_number_check_reads_the_document(tmp_path: Path) -> None:
    """It used to look for a marker the author had to volunteer, which appeared nowhere."""
    from samegold.evidence.render import check_readme

    latest = {
        "SG-01": {
            "claim_id": "SG-01",
            "title": "two implementations agree on the close",
            "runtime": "oss-local",
            "artifacts": {},
            "verdict": {"outcome": "pass", "runs": {}, "rate": None},
        }
    }
    document = tmp_path / "DOC.md"
    document.write_text("`SG-01` agreed on 15/15 runs.\n", encoding="utf-8")
    kinds = [drift.kind for drift in check_readme(document, latest)]
    assert "hardcoded" in kinds
    document.write_text("`SG-01` agreed on every run.\n", encoding="utf-8")
    assert [d.kind for d in check_readme(document, latest)] == []


# ------------------------------------------------------------------ the seventh review


def test_a_record_names_the_tree_it_ran_on_not_only_the_commit() -> None:
    """The commit anchors the SEEDS. Until now nothing anchored the CODE.

    Three examples out of this repository's own committed history, each at a single commit
    sha: SG-05 recorded 0/3 and then 3/3 thirty seconds later; SG-03's denominator moved from
    49 scored mutants to 48; SG-07 went from fail to pass. All three are honest
    re-measurements after a fix, and all three were indistinguishable from retry-until-green,
    because the record said which seeds had run and not which program.
    """
    from samegold.evidence.record import EvidenceRecord
    from samegold.evidence.store import EvidenceRejected, EvidenceStore
    from samegold.generator.seeds import current_commit_sha, current_tree, seeds_from_commit
    from samegold.verify.verdict import Pass, Rate, RunSet

    sha = current_commit_sha()
    tree, dirty = current_tree()
    assert len(tree) == 40, "the working tree has no hash, so a record cannot name it"

    def record(**overrides: object) -> EvidenceRecord:
        fields: dict[str, object] = {
            "n": 1,
            "seeds": tuple(seeds_from_commit(1, "witness", sha=sha)),
            "commit_sha": sha,
            "tree_sha": tree,
            "tree_dirty": dirty,
            "seed_source": "commit",
            "seed_purpose": "witness",
            "profile": "fast",
            "started_at": "2026-09-01T00:00:00+00:00",
            "duration_s": 1.0,
            "runtime": "oss-local",
        }
        fields.update(overrides)
        runs = RunSet(**fields)  # type: ignore[arg-type]
        return EvidenceRecord(
            "SG-01", CLAIM_TITLES["SG-01"], Pass("SG-01", runs, Rate(1, 1)), "oss-local"
        )

    import tempfile

    with (
        tempfile.TemporaryDirectory(prefix="samegold-tree-") as tmp,
        pytest.raises(EvidenceRejected, match="git tree it ran on"),
    ):
        EvidenceStore(Path(tmp)).append(record(tree_sha=""))


def test_a_dirty_tree_is_published_as_such() -> None:
    """Not forbidden - every honest re-measurement after a fix is a dirty tree - but labelled."""
    from samegold.evidence.render import render_claims_block

    latest = {
        "SG-01": {
            "claim_id": "SG-01",
            "title": CLAIM_TITLES["SG-01"],
            "runtime": "oss-local",
            "artifacts": {},
            "verdict": {
                "outcome": "pass",
                "runs": {"tree_dirty": True},
                "rate": {"successes": 1, "trials": 1, "wilson95": [0.2, 1.0], "point": 1.0},
            },
        }
    }
    assert "on an uncommitted tree" in render_claims_block(latest)


def test_the_results_table_prints_the_registry_title() -> None:
    """A record written before a claim was renamed keeps the old title, legitimately.

    The chain is a history and re-validating it must not turn a legitimate past into a
    forgery. The TABLE is not a history: it describes the claims as they are, and it was
    publishing "the close survives a crash at each structural point" forty lines above a
    section explaining that this was false by half.
    """
    from samegold.evidence.render import render_claims_block

    latest = {
        "SG-07": {
            "claim_id": "SG-07",
            "title": "the close survives a crash at each structural point",
            "runtime": "oss-local",
            "artifacts": {},
            "verdict": {"outcome": "pass", "runs": {}, "rate": None},
        }
    }
    rendered = render_claims_block(latest)
    assert CLAIM_TITLES["SG-07"] in rendered
    assert "the close survives a crash at each structural point" not in rendered


def test_the_hardcoded_check_sees_the_shapes_the_documents_actually_use(tmp_path: Path) -> None:
    """The previous version required a BACKTICKED id and skipped lines starting with | or #.

    Measured over the eight documents that inspected zero lines: backticked ids appear only
    inside the generated table, which it skipped, and the prose writes them bare. It also
    fired on "SG-03 runs the reference on DuckDB 1.5", because a version number is a number.
    """
    from samegold.evidence.render import check_readme

    latest = {
        "SG-03": {
            "claim_id": "SG-03",
            "title": CLAIM_TITLES["SG-03"],
            "runtime": "oss-local",
            "artifacts": {},
            "verdict": {"outcome": "pass", "runs": {}, "rate": None},
        }
    }
    document = tmp_path / "D.md"

    def kinds(text: str) -> list[str]:
        document.write_text(text + "\n", encoding="utf-8")
        return [drift.kind for drift in check_readme(document, latest)]

    for stated in (
        "SG-03 moved 99.9% of closed months.",
        "| `SG-03` mutation | PASS | 999/999 | oss-local | CI |",
        "## `SG-03` cut reads by 99.9%",
    ):
        assert "hardcoded" in kinds(stated), stated
    for innocent in (
        "SG-03 runs the reference on DuckDB 1.5.",
        "SG-03 was first published in 2026.",
        "SG-03 mutates the reference SQL and the specification itself.",
    ):
        assert "hardcoded" not in kinds(innocent), innocent


def test_every_seed_purpose_the_code_draws_is_in_the_registry() -> None:
    """The registry calls itself "the seed streams that exist" and omitted three of them.

    `samegold demo`, `samegold generate` and `samegold report` each draw a seed. None of them
    writes a record, so the gate never noticed; the docstring was wrong about its own file.
    """
    from samegold.evidence.registry import SEED_PURPOSES

    drawn = set()
    for module in ("cli.py", "claims.py"):
        source = (REPO / "src" / "samegold" / module).read_text(encoding="utf-8")
        drawn |= set(re.findall(r'purpose="([a-z_]+)"', source))
    missing = drawn - set(SEED_PURPOSES)
    assert not missing, f"seed streams the code draws and the registry does not list: {missing}"


def test_the_ledger_collapse_re_compares_after_a_replacement() -> None:
    """One loop doing both collapses leaves an adjacent identical pair behind.

    A(t1,X), B(t2,Y), C(t2,X): C replaces B in place and is never compared with A, so the
    result is [A(X), C(X)] - exactly the pair the collapse exists to remove.

    The first version of this test copied the two-pass loop into its own body and asserted on
    that copy. A review reverted `generator/events.py` to the single-loop version the commit
    calls wrong, ran the suite, and this test passed: it was protecting a transcription of the
    code rather than the code. The rule is a function now and the test calls it.
    """
    from samegold.domain.bitemporal import collapse_versions

    versions = [
        {
            "valid_from": "2026-01-01T00:00:00+00:00",
            "segment": "X",
            "country": "ES",
            "event_id": "a",
        },
        {
            "valid_from": "2026-02-01T00:00:00+00:00",
            "segment": "Y",
            "country": "ES",
            "event_id": "b",
        },
        {
            "valid_from": "2026-02-01T00:00:00+00:00",
            "segment": "X",
            "country": "ES",
            "event_id": "c",
        },
    ]
    assert [row["valid_from"] for row in collapse_versions(versions)] == [
        "2026-01-01T00:00:00+00:00"
    ]
    # And the two rules it is made of, separately, so a regression in either is named.
    heartbeat = [
        {"valid_from": "2026-01-01T00:00:00+00:00", "segment": "X", "country": "ES"},
        {"valid_from": "2026-02-01T00:00:00+00:00", "segment": "X", "country": "ES"},
    ]
    assert len(collapse_versions(heartbeat)) == 1
    same_instant = [
        {
            "valid_from": "2026-01-01T00:00:00+00:00",
            "segment": "X",
            "country": "ES",
            "event_id": "a",
        },
        {
            "valid_from": "2026-01-01T00:00:00+00:00",
            "segment": "Y",
            "country": "ES",
            "event_id": "b",
        },
    ]
    assert [row["segment"] for row in collapse_versions(same_instant)] == ["Y"]
    # Order-free: it sorts by instant, so the caller's order cannot change the answer.
    assert collapse_versions(versions) == collapse_versions(list(reversed(versions)))


def test_sg04_can_fail() -> None:
    """It had one unconditional Pass and was listed as refutable, so nothing could refute it.

    The claim the README puts in its opening pull-quote was the one claim with no failure
    condition, in a repository that says elsewhere "a crash test that cannot fail is a
    screenshot".
    """
    source = (REPO / "src" / "samegold" / "claims.py").read_text(encoding="utf-8")
    body = source[source.index("def claim_restatement_magnitude") :]
    body = body[: body.index("\ndef ")]
    assert 'Fail(\n            "SG-04"' in body, "SG-04 has no failure branch"
    from samegold.claims import REFUTABLE_CLAIMS

    assert "SG-04" in REFUTABLE_CLAIMS


def test_current_tree_reports_a_dirty_tree_as_dirty(tmp_path: Path) -> None:
    """The headline mechanism of the seventh round had no test at all.

    A review deleted the `git stash create` call, `current_tree()` went on reporting a clean
    committed tree with a modified file in it, and the suite stayed green. It also found that
    the original implementation reported CLEAN for a tree whose only change was an untracked
    file - which is how five new tests can enter the published count with the record saying
    the code was committed.
    """
    import os
    import subprocess

    from samegold.generator.seeds import current_tree

    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-c", "user.email=a@b", "-c", "user.name=a", "-C", str(repo), *args],
            check=True,
            capture_output=True,
        )

    git("init", "-q")
    (repo / "a.py").write_text("x\n", encoding="utf-8")
    git("add", "a.py")
    git("commit", "-qm", "first")

    cwd = os.getcwd()
    try:
        os.chdir(repo)
        clean_tree, clean_dirty = current_tree()
        assert len(clean_tree) == 40 and clean_dirty is False

        (repo / "untracked.py").write_text("y\n", encoding="utf-8")
        untracked_tree, untracked_dirty = current_tree()
        assert untracked_dirty is True, "an untracked file is code that is in no commit"
        assert untracked_tree == clean_tree, "an untracked file changes no tree hash"

        (repo / "a.py").write_text("z\n", encoding="utf-8")
        modified_tree, modified_dirty = current_tree()
        assert modified_dirty is True
        assert modified_tree != clean_tree, "a modified file must change the recorded tree"
    finally:
        os.chdir(cwd)


def test_current_tree_returns_nothing_outside_a_checkout(tmp_path: Path) -> None:
    """And the gate then refuses the record rather than accepting forty zeros.

    Everything in this repository runs from a downloaded tarball. Publishing evidence from
    one does not, because a number whose provenance is "some files, somewhere" is not
    evidence, and the previous fallback was a valid-looking 40-character tree of zeros.
    """
    import os

    from samegold.generator.seeds import current_tree

    cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        assert current_tree() == ("", False)
    finally:
        os.chdir(cwd)

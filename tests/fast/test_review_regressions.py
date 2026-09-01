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


def test_the_databricks_expectations_use_the_contract_reasons_and_are_null_safe() -> None:
    """Its rules were `qty IS NULL OR qty > 0`, which PASSES a record with no quantity.

    So the shape this whole review round started from - an order line with no `currency` -
    was accepted on the Databricks lane while both OSS engines quarantined it. The reason
    names were also that lane's own vocabulary rather than the contract's closed enum.
    """
    from samegold.domain.contract import QuarantineReason

    source = (REPO / "databricks" / "src" / "silver_expectations.py").read_text(encoding="utf-8")
    block = source[source.index("RULES = {") : source.index("@dp.table")]
    names = set(re.findall(r'^    "([a-z_]+)":', block, flags=re.MULTILINE))
    assert names <= {str(reason) for reason in QuarantineReason}, (
        f"rule names that are not contract quarantine reasons: {sorted(names)}"
    )
    assert "IS NULL OR" not in block, "a NULL-tolerant rule passes the record it should catch"

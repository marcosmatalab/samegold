"""The Spark implementation and the DuckDB reference compute the same close.

This is the test the whole design is built around: two engines, two derivations of the same
contract, one canonical digest. It is marked ``spark`` because it needs a JVM; it does NOT
need Delta, so it runs even where Maven Central is unreachable (SAMEGOLD_STORAGE=parquet).
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from samegold.generator.events import FAST, generate
from samegold.oracle.duckdb_gold import revenue_versions, scd2_as_of
from samegold.pipelines.transform import (
    as_of_cut,
    classify_returns,
    dim_customer_scd2,
    effective_lines,
    read_bronze,
    revenue_by_month,
    silver,
)
from samegold.pipelines.transform import revenue_versions as spark_revenue_versions
from samegold.verify.digest import (
    REVENUE_PROJECTION,
    SCD2_PROJECTION,
    CanonicalDigest,
    Projection,
)
from samegold.verify.invariants import restatement_monotonic, scd2_well_formed

pytestmark = pytest.mark.spark

SNAPSHOT_PROJECTION = Projection(
    table="revenue_snapshot",
    columns=(
        "accounting_month",
        "gross_cents",
        "returns_cents",
        "net_cents",
        "line_count",
        "return_count",
        "returns_rejected_count",
    ),
    order_by=("accounting_month",),
)


def _snapshot(spark, bronze: Path, as_of: dt.datetime):  # type: ignore[no-untyped-def]
    raw = as_of_cut(read_bronze(spark, str(bronze)), as_of.isoformat())
    clean = silver(raw)
    lines = effective_lines(clean)
    return revenue_by_month(lines, classify_returns(clean, lines))


def test_the_two_engines_agree_on_the_versioned_close(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """The thesis in one assertion: two engines, one bitemporal close table.

    The comparison is over the VERSIONED table. An earlier version of this test compared a
    single snapshot with a literal close_version of zero, which meant the digest projection
    declared a column no implementation actually produced.
    """
    result = generate(tmp_path / "g", seed=42, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    snapshots = [(c.isoformat(), _snapshot(spark, bronze, c)) for c in closes]
    spark_rows = [row.asDict() for row in spark_revenue_versions(snapshots).collect()]
    duck_rows = revenue_versions(bronze, closes)
    spark_digest = CanonicalDigest.of(spark_rows, REVENUE_PROJECTION)
    duck_digest = CanonicalDigest.of(duck_rows, REVENUE_PROJECTION)
    assert spark_digest.agrees_with(duck_digest), (
        f"engines disagree on the version history: spark={spark_digest} duckdb={duck_digest}\n"
        f"spark={spark_rows[:3]}\nduckdb={duck_rows[:3]}"
    )
    assert restatement_monotonic(spark_rows) == []


def test_every_close_agrees_snapshot_by_snapshot(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """The finer-grained version: if a single close disagrees, this says which one."""
    from samegold.oracle.duckdb_gold import DuckDBWitness

    result = generate(tmp_path / "g", seed=7, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    witness = DuckDBWitness()
    for close in result.ledger.closes:
        as_of = dt.datetime.fromisoformat(close)
        spark_rows = [row.asDict() for row in _snapshot(spark, bronze, as_of).collect()]
        duck_rows = [
            {"accounting_month": month, **values}
            for month, values in sorted(witness.revenue(bronze, as_of).items())
        ]
        assert CanonicalDigest.of(spark_rows, SNAPSHOT_PROJECTION).agrees_with(
            CanonicalDigest.of(duck_rows, SNAPSHOT_PROJECTION)
        ), f"engines disagree at close {close}"


def test_the_dimension_matches_and_is_well_formed(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    result = generate(tmp_path / "g", seed=7, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
    raw = as_of_cut(read_bronze(spark, str(bronze)), as_of.isoformat())
    spark_rows = [row.asDict() for row in dim_customer_scd2(silver(raw)).collect()]
    duck_rows = scd2_as_of(bronze, as_of)
    assert scd2_well_formed(spark_rows) == []
    assert CanonicalDigest.of(spark_rows, SCD2_PROJECTION).agrees_with(
        CanonicalDigest.of(duck_rows, SCD2_PROJECTION)
    )


def test_the_digest_does_not_depend_on_the_shuffle(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Same input, different parallelism: the published number must not move.

    Without this, every "the digests matched" statement in this repository would be a
    statement about one particular partitioning rather than about the data.
    """
    result = generate(tmp_path / "g", seed=3, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
    digests = []
    for partitions in ("2", "16"):
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        rows = [row.asDict() for row in _snapshot(spark, bronze, as_of).collect()]
        digests.append(CanonicalDigest.of(rows, SNAPSHOT_PROJECTION))
    assert digests[0].agrees_with(digests[1])


def test_the_dedup_tie_break_is_a_total_order(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """Feeding the same rows in a different physical order must not change the answer.

    The deduplication window used to order only by (event_ts, arrival_ts), which leaves ties
    unbroken and lets the shuffle decide which copy of an event survives.
    """
    generate(tmp_path / "g", seed=5, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    as_of = dt.datetime(2026, 3, 5, 22, 59, 59, tzinfo=dt.UTC)
    first = _snapshot(spark, bronze, as_of)
    reordered = as_of_cut(
        read_bronze(spark, str(bronze)).repartition(7, "event_id"), as_of.isoformat()
    )
    clean = silver(reordered)
    lines = effective_lines(clean)
    second = revenue_by_month(lines, classify_returns(clean, lines))
    assert CanonicalDigest.of(
        [row.asDict() for row in first.collect()], SNAPSHOT_PROJECTION
    ).agrees_with(
        CanonicalDigest.of([row.asDict() for row in second.collect()], SNAPSHOT_PROJECTION)
    )

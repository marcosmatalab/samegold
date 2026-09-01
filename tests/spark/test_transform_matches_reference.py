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
from samegold.oracle.duckdb_gold import DuckDBWitness, scd2_as_of
from samegold.pipelines.transform import (
    as_of_cut,
    dim_customer_scd2,
    effective_lines,
    read_bronze,
    revenue_by_month,
    silver,
    valid_returns,
)
from samegold.verify.digest import (
    REVENUE_PROJECTION,
    SCD2_PROJECTION,
    CanonicalDigest,
)
from samegold.verify.invariants import scd2_well_formed

pytestmark = pytest.mark.spark


def _spark_revenue(spark, bronze: Path, as_of: dt.datetime) -> list[dict]:  # type: ignore[no-untyped-def]
    raw = as_of_cut(read_bronze(spark, str(bronze)), as_of.isoformat())
    clean = silver(raw)
    lines = effective_lines(clean)
    returns = valid_returns(clean, lines)
    return [
        {
            "accounting_month": r["accounting_month"],
            "close_version": 0,
            "gross_cents": r["gross_cents"],
            "returns_cents": r["returns_cents"],
            "net_cents": r["net_cents"],
        }
        for r in (row.asDict() for row in revenue_by_month(lines, returns).collect())
    ]


def _duckdb_revenue(bronze: Path, as_of: dt.datetime) -> list[dict]:
    return [
        {
            "accounting_month": month,
            "close_version": 0,
            "gross_cents": v["gross_cents"],
            "returns_cents": v["returns_cents"],
            "net_cents": v["net_cents"],
        }
        for month, v in sorted(DuckDBWitness().revenue(bronze, as_of).items())
    ]


def test_the_two_engines_agree_at_every_close(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    result = generate(tmp_path / "g", seed=42, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    for close in result.ledger.closes:
        as_of = dt.datetime.fromisoformat(close)
        spark_digest = CanonicalDigest.of(_spark_revenue(spark, bronze, as_of), REVENUE_PROJECTION)
        duck_digest = CanonicalDigest.of(_duckdb_revenue(bronze, as_of), REVENUE_PROJECTION)
        assert spark_digest.agrees_with(duck_digest), (
            f"engines disagree at close {close}: spark={spark_digest} duckdb={duck_digest}"
        )


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

    Without this, every 'the digest matched' claim in the repository would be a claim about
    one particular partitioning rather than about the data.
    """
    result = generate(tmp_path / "g", seed=3, profile=FAST)
    bronze = tmp_path / "g" / "bronze"
    as_of = dt.datetime.fromisoformat(result.ledger.closes[-1])
    digests = []
    for partitions in ("2", "16"):
        spark.conf.set("spark.sql.shuffle.partitions", partitions)
        digests.append(CanonicalDigest.of(_spark_revenue(spark, bronze, as_of), REVENUE_PROJECTION))
    assert digests[0].agrees_with(digests[1])

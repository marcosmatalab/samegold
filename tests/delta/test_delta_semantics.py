"""The claims that only Delta can carry.

Each test here is one exam objective made executable: time travel, change data feed, an
idempotent MERGE, and the file-count effect that the cost lab measures properly later.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.delta


def test_the_pinned_coordinate_is_the_one_that_exists() -> None:
    """Delta moved to a Spark-qualified artefact name; the old one resolves to nothing."""
    from samegold.pipelines.session import DELTA_COORDINATE

    assert DELTA_COORDINATE == "io.delta:delta-spark_4.2_2.13:4.4.0"


def test_time_travel_returns_the_version_that_was_written(delta_spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    table = str(tmp_path / "tt")
    delta_spark.range(0, 10).write.format("delta").save(table)
    delta_spark.range(10, 20).write.format("delta").mode("append").save(table)
    v0 = delta_spark.read.format("delta").option("versionAsOf", 0).load(table).count()
    v1 = delta_spark.read.format("delta").load(table).count()
    assert (v0, v1) == (10, 20)


def test_change_data_feed_reports_the_rows_that_changed(delta_spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    table = str(tmp_path / "cdf")
    (
        delta_spark.range(0, 5)
        .write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .save(table)
    )
    delta_spark.sql(f"UPDATE delta.`{table}` SET id = id + 100 WHERE id = 1")
    changes = (
        delta_spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 1)
        .load(table)
    )
    kinds = {row["_change_type"] for row in changes.collect()}
    assert {"update_preimage", "update_postimage"} <= kinds


def test_merge_is_idempotent_on_a_repeated_batch(delta_spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """The property the crash campaign depends on: replaying a batch must not duplicate rows."""
    from delta.tables import DeltaTable

    table = str(tmp_path / "merge")
    delta_spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING").write.format(
        "delta"
    ).save(table)
    source = delta_spark.createDataFrame([(2, "B"), (3, "c")], "id INT, v STRING")
    for _ in range(2):
        (
            DeltaTable.forPath(delta_spark, table)
            .alias("t")
            .merge(source.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    rows = {(r["id"], r["v"]) for r in delta_spark.read.format("delta").load(table).collect()}
    assert rows == {(1, "a"), (2, "B"), (3, "c")}


def test_optimize_reduces_the_file_count(delta_spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """A measurement, not a claim about speed: file count is what OPTIMIZE actually changes."""
    table = str(tmp_path / "opt")
    for i in range(12):
        delta_spark.range(i * 10, i * 10 + 10).write.format("delta").mode("append").save(table)
    before = len(list(Path(table).glob("*.parquet")))
    delta_spark.sql(f"OPTIMIZE delta.`{table}`")
    after = len(
        [
            path
            for path in Path(table).glob("*.parquet")
            if path.stat().st_mtime >= max(p.stat().st_mtime for p in Path(table).glob("*.parquet"))
        ]
    )
    assert before >= 12
    assert after < before


def test_the_scd2_merge_produces_a_well_formed_dimension(delta_spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    """The incremental dimension must satisfy the same invariant as the recomputed one."""
    from samegold.pipelines.gold_scd2_merge import upsert_scd2
    from samegold.verify.invariants import scd2_well_formed

    delta_spark.sql("CREATE DATABASE IF NOT EXISTS samegold_test")
    table = "samegold_test.dim_customer_scd2"
    delta_spark.sql(f"DROP TABLE IF EXISTS {table}")
    batch1 = delta_spark.createDataFrame(
        [("C1", "2026-01-01T00:00:00.000000Z", "retail", "ES", "e1")],
        "customer_id STRING, valid_from STRING, segment STRING, country STRING, event_id STRING",
    )
    batch2 = delta_spark.createDataFrame(
        [("C1", "2026-02-01T00:00:00.000000Z", "vip", "ES", "e2")],
        "customer_id STRING, valid_from STRING, segment STRING, country STRING, event_id STRING",
    )
    upsert_scd2(delta_spark, batch1, table)
    upsert_scd2(delta_spark, batch2, table)
    rows = [row.asDict() for row in delta_spark.table(table).collect()]
    assert len(rows) == 2
    assert scd2_well_formed(rows) == []

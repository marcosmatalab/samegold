"""The claims that only Delta can carry.

Each test here is one exam objective made executable: time travel, the change data feed read
AS A FEED, a MERGE that runs both of its branches and then deletes by absence, and OPTIMIZE
with ZORDER measured where the effect actually is.

These tests were written eleven rounds before anything ran them. The machine they were written
on had no route to Maven Central, so the Delta jars could not be resolved, and the lane skipped
with an explicit message on every run - which is the honest behaviour and is also how six tests
stay green-adjacent for eleven rounds while being wrong. The first execution found two defects,
both recorded at the site that carried them:

  * `upsert_scd2` could not complete its FIRST call on any input, because it built the MERGE
    source with an inferred schema and the open row of a Type 2 dimension has a NULL
    `valid_to` (see pipelines/gold_scd2_merge.py). Its only caller is this file;
  * the lane passed once and failed on the second run, because it wrote a metastore-managed
    table into the repository's own `spark-warehouse/` and dropped one of the two tables it
    created. Fixed below by giving the test a database of its own, in `tmp_path`.

And one test was measuring nothing at all: see the OPTIMIZE test.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

pytestmark = pytest.mark.delta


def _latest_commit(spark, table_sql: str) -> dict:  # type: ignore[no-untyped-def]
    """The newest row of the transaction log, as a dict.

    Every measurement in this file that could have been taken with a stopwatch is taken from
    here instead. `DESCRIBE HISTORY` is Delta's own record of what a command did: it is exact,
    it is reproducible on a busy machine, and it is the artefact the exam asks about.
    """
    row = (
        spark.sql(f"DESCRIBE HISTORY {table_sql}").orderBy("version", ascending=False).collect()[0]
    )
    return row.asDict()


def test_the_pinned_coordinate_is_the_one_that_exists() -> None:
    """Delta moved to a Spark-qualified artefact name; the old one resolves to nothing."""
    from samegold.pipelines.session import DELTA_COORDINATE

    assert DELTA_COORDINATE == "io.delta:delta-spark_4.2_2.13:4.4.0"


def test_time_travel_reads_a_row_the_current_table_no_longer_has(  # type: ignore[no-untyped-def]
    delta_spark, tmp_path: Path
) -> None:
    """Version 0 still answers after the rows in it have been deleted.

    The previous version of this test compared two COUNTS, 10 against 20, which an append
    alone explains: nothing in it distinguished "an older version is readable" from "the table
    grew". Deleting the rows that version 0 contains is what makes the two answers
    contradictory, so only real time travel can produce both.
    """
    table = str(tmp_path / "tt")
    delta_spark.range(0, 10).write.format("delta").save(table)
    delta_spark.range(10, 20).write.format("delta").mode("append").save(table)
    delta_spark.sql(f"DELETE FROM delta.`{table}` WHERE id < 5")

    at_zero = delta_spark.read.format("delta").option("versionAsOf", 0).load(table)
    now = delta_spark.read.format("delta").load(table)
    ids_then = {row["id"] for row in at_zero.collect()}
    ids_now = {row["id"] for row in now.collect()}

    assert ids_then == set(range(0, 10))
    assert ids_now == set(range(5, 20))
    # The point of the claim: the old version serves rows the current table cannot.
    assert ids_then - ids_now == set(range(0, 5))
    assert _latest_commit(delta_spark, f"delta.`{table}`")["version"] == 2


def test_the_change_feed_carries_what_the_table_cannot(  # type: ignore[no-untyped-def]
    delta_spark, tmp_path: Path
) -> None:
    """Read as a FEED, not as a table, and check the difference is the whole point.

    A change data feed is worth having only where the table is not enough. So this asserts
    both halves: the feed reports the four change types with the commit that produced each,
    and the two rows that no longer exist in the table - the value before an update, and a
    deleted row - are readable in the feed and absent from the table.
    """
    table = str(tmp_path / "cdf")
    (
        delta_spark.range(0, 5)
        .write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .save(table)
    )
    delta_spark.sql(f"UPDATE delta.`{table}` SET id = id + 100 WHERE id = 1")
    delta_spark.sql(f"DELETE FROM delta.`{table}` WHERE id = 2")

    feed = (
        delta_spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", 0)
        .load(table)
    )
    assert {"_change_type", "_commit_version", "_commit_timestamp"} <= set(feed.columns)
    rows = [row.asDict() for row in feed.collect()]
    assert {row["_change_type"] for row in rows} == {
        "insert",
        "update_preimage",
        "update_postimage",
        "delete",
    }
    # Each change is attributed to the commit that made it, which is what makes the feed
    # replayable from a checkpoint rather than merely descriptive.
    by_kind = {row["_change_type"]: row for row in rows if row["_change_type"] != "insert"}
    assert by_kind["update_preimage"]["_commit_version"] == 1
    assert by_kind["update_postimage"]["_commit_version"] == 1
    assert by_kind["delete"]["_commit_version"] == 2

    in_table = {row["id"] for row in delta_spark.read.format("delta").load(table).collect()}
    in_feed = {row["id"] for row in rows}
    # 1 was updated away and 2 was deleted: the table has neither, the feed has both.
    assert {1, 2} & in_table == set()
    assert {1, 2} <= in_feed


def test_merge_runs_both_branches_and_then_repeats_without_effect(  # type: ignore[no-untyped-def]
    delta_spark, tmp_path: Path
) -> None:
    """The MERGE claim, taken from the transaction log rather than from the final rows.

    The final rows are the same whether the MERGE inserted a row and updated a row or simply
    rewrote the table, so the previous version of this test could not tell an upsert from a
    replace. `operationMetrics` names the branches: `numTargetRowsInserted` is the
    whenNotMatched arm and `numTargetRowsMatchedUpdated` is the whenMatched one, and the
    second run must show the insert arm firing zero times - that is what idempotent means,
    and it is the property the crash campaign leans on.
    """
    from delta.tables import DeltaTable

    table = str(tmp_path / "merge")
    delta_spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING").write.format(
        "delta"
    ).save(table)
    source = delta_spark.createDataFrame([(2, "B"), (3, "c")], "id INT, v STRING")

    metrics = []
    for _ in range(2):
        (
            DeltaTable.forPath(delta_spark, table)
            .alias("t")
            .merge(source.alias("s"), "t.id = s.id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        commit = _latest_commit(delta_spark, f"delta.`{table}`")
        assert commit["operation"] == "MERGE"
        metrics.append({k: int(v) for k, v in commit["operationMetrics"].items() if v.isdigit()})

    assert metrics[0]["numTargetRowsInserted"] == 1, "the whenNotMatched branch did not fire"
    assert metrics[0]["numTargetRowsMatchedUpdated"] == 1, "the whenMatched branch did not fire"
    assert metrics[1]["numTargetRowsInserted"] == 0, "the replay inserted a row again"

    rows = {(r["id"], r["v"]) for r in delta_spark.read.format("delta").load(table).collect()}
    assert rows == {(1, "a"), (2, "B"), (3, "c")}


def test_optimize_with_zorder_is_visible_in_the_transaction_log(  # type: ignore[no-untyped-def]
    delta_spark, tmp_path: Path
) -> None:
    """OPTIMIZE ... ZORDER BY, measured in the Delta log, on a table with something to sort.

    The previous version of this test measured nothing. It counted `*.parquet` files on disk
    before, then counted the files whose mtime was `>= max(mtime)` after - which is the newest
    file or files, so `after` was 1 or 2 whatever OPTIMIZE had done, and `after < before` was
    arithmetic rather than a result. It also never passed ZORDER BY, so the clustering half of
    the objective was not exercised at all.

    Both halves are read from `DESCRIBE HISTORY` now: `operationParameters.zOrderBy` says the
    command really was a Z-ORDER and on which column, and `operationMetrics` says how many
    files it removed and added. That is Delta's own account of the work, which is exact where
    a directory listing is a guess and a stopwatch is a different measurement altogether.
    """
    table = str(tmp_path / "zorder")
    for i in range(12):
        (
            delta_spark.range(i * 100, i * 100 + 100)
            .selectExpr("id", "CAST(id % 7 AS INT) AS bucket")
            .write.format("delta")
            .mode("append")
            .save(table)
        )
    detail_before = delta_spark.sql(f"DESCRIBE DETAIL delta.`{table}`").collect()[0].asDict()
    rows_before = delta_spark.read.format("delta").load(table).count()

    delta_spark.sql(f"OPTIMIZE delta.`{table}` ZORDER BY (bucket)")

    commit = _latest_commit(delta_spark, f"delta.`{table}`")
    assert commit["operation"] == "OPTIMIZE"
    assert json.loads(commit["operationParameters"]["zOrderBy"]) == ["bucket"]
    metrics = {k: int(v) for k, v in commit["operationMetrics"].items() if v.isdigit()}
    assert metrics["numRemovedFiles"] >= 12
    assert metrics["numAddedFiles"] < metrics["numRemovedFiles"]

    detail_after = delta_spark.sql(f"DESCRIBE DETAIL delta.`{table}`").collect()[0].asDict()
    assert detail_after["numFiles"] < detail_before["numFiles"]
    # Compaction is not allowed to change the answer, which is the half of this that a file
    # count cannot see.
    assert delta_spark.read.format("delta").load(table).count() == rows_before


def test_the_scd2_merge_produces_a_well_formed_dimension(  # type: ignore[no-untyped-def]
    delta_spark, tmp_path: Path
) -> None:
    """The incremental dimension must satisfy the same invariant as the recomputed one.

    The database is created in `tmp_path` and dropped at the end. It used to be a fixed name
    in the repository's own `spark-warehouse/`, and the test dropped `dim_customer_scd2` while
    `upsert_scd2` also creates `dim_customer_scd2_versions`: the lane passed on a clean
    machine and failed on the second run, with a Delta error about a location that is not
    empty. A test that only passes once is a test that fails for whoever runs it next.
    """
    from samegold.pipelines.gold_scd2_merge import upsert_scd2
    from samegold.verify.invariants import scd2_well_formed

    database = f"samegold_delta_{uuid4().hex[:12]}"
    delta_spark.sql(f"CREATE DATABASE {database} LOCATION '{(tmp_path / 'warehouse').as_posix()}'")
    table = f"{database}.dim_customer_scd2"
    schema = (
        "customer_id STRING, valid_from STRING, segment STRING, country STRING, event_id STRING"
    )
    try:
        batch1 = delta_spark.createDataFrame(
            [("C1", "2026-01-01T00:00:00.000000Z", "retail", "ES", "e1")], schema
        )
        batch2 = delta_spark.createDataFrame(
            [("C1", "2026-02-01T00:00:00.000000Z", "vip", "ES", "e2")], schema
        )
        # A LATE CORRECTION, which is the batch that broke it. The version arriving third has
        # an earlier valid_from than the second, so the recomputed dimension re-splits the
        # interval the second batch opened. An upsert-only MERGE cannot express that: it
        # updated the rows it recognised and left the superseded one behind for ever, and the
        # table then had two rows with is_current = true and a closed row whose valid_to
        # pointed at an interval that no longer existed. Every earlier test applied purely
        # additive batches, so the delete path had no coverage at all.
        batch3 = delta_spark.createDataFrame(
            [("C1", "2026-01-15T00:00:00.000000Z", "vip", "ES", "e3")], schema
        )

        first = upsert_scd2(delta_spark, batch1, table)
        # The whenNotMatched branch, on an empty target: every row is an insert.
        assert first["rows_written"] == 1 and first["deleted"] == 0
        commit = _latest_commit(delta_spark, table)
        assert commit["operation"] == "MERGE"
        assert int(commit["operationMetrics"]["numTargetRowsInserted"]) == 1

        upsert_scd2(delta_spark, batch2, table)
        rows = [row.asDict() for row in delta_spark.table(table).collect()]
        assert len(rows) == 2
        assert scd2_well_formed(rows) == []

        stats = upsert_scd2(delta_spark, batch3, table)
        rows = [row.asDict() for row in delta_spark.table(table).collect()]
        assert scd2_well_formed(rows) == [], rows
        # retail until 15 January, vip from then on: the 1 February version is a heartbeat once
        # the correction exists, and the row it used to open has to be gone, not merely closed.
        assert sorted(row["valid_from"] for row in rows) == [
            "2026-01-01T00:00:00.000000Z",
            "2026-01-15T00:00:00.000000Z",
        ]
        assert stats["deleted"] == 1
        assert sum(1 for row in rows if row["is_current"]) == 1
        # The delete is one commit over a predicate, not one commit per row: a dimension that
        # is malformed between two commits is a dimension a reader can catch malformed.
        delete_commit = _latest_commit(delta_spark, table)
        assert delete_commit["operation"] == "DELETE"
        assert int(delete_commit["operationMetrics"]["numDeletedRows"]) == 1
    finally:
        delta_spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")

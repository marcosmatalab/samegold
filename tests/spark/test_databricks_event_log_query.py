"""The update-state query, run against a synthetic event log.

`publish_evidence.py` reports whether the lane worked. It did so with

    MAX(details:update_progress.state) AS last_state

and `MAX` on a string is the ALPHABETICAL maximum. Over the states an update passes through -
CREATED, WAITING_FOR_RESOURCES, INITIALIZING, SETTING_UP_TABLES, RUNNING, COMPLETED, FAILED,
CANCELED - `WAITING_FOR_RESOURCES` wins every time, whatever happened. The first successful run
published `last_state: WAITING_FOR_RESOURCES` for an update `databricks pipelines get` reports
as COMPLETED, and the failed run that morning would have published the same word. A constant
with the shape of a measurement, in the field that decides whether the carriage worked, and the
`dbx:update.last_state` anchor would have accepted it.

The query is EXTRACTED from the lane's own source rather than restated here, for the reason
round 17 wrote down: a test that rebuilds the expression it is checking agrees with the defect.
`event_log('<id>')` is a Databricks-only table function, so the only thing substituted is that
name, for a view holding the sequence a real update actually emits.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.spark

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks" / "src" / "publish_evidence.py"

# The real sequence, in the order a Lakeflow update emits it.
HAPPY = (
    "CREATED",
    "WAITING_FOR_RESOURCES",
    "INITIALIZING",
    "SETTING_UP_TABLES",
    "RUNNING",
    "COMPLETED",
)
UNHAPPY = (
    "CREATED",
    "WAITING_FOR_RESOURCES",
    "INITIALIZING",
    "SETTING_UP_TABLES",
    "RUNNING",
    "FAILED",
)


def _lane_statements() -> list[str]:
    """Every SQL string in publish_evidence.py, read with the parse test's own extractor."""
    path = REPO / "tests" / "spark" / "test_databricks_lane_parses.py"
    spec = importlib.util.spec_from_file_location("_lane_parses", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return list(module._sql_calls(LANE.read_text(encoding="utf-8")))


def _statement(marker: str) -> str:
    matches = [s for s in _lane_statements() if marker in s]
    assert len(matches) == 1, (
        f"expected exactly one statement in {LANE.name} containing {marker!r}, found {len(matches)}"
    )
    # The one Databricks-only name in it. Everything else is evaluated exactly as written.
    return matches[0].replace("event_log('{pipeline_id}')", "event_log_probe")


def _write_log(spark: Any, updates: dict[str, tuple[str, ...]]) -> None:
    """A view shaped like the pipeline event log: one row per state transition."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("level", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("update_id", StringType(), True),
            StructField("details", StringType(), True),
        ]
    )
    start = dt.datetime(2026, 9, 3, 12, 0, 0, tzinfo=dt.UTC)
    rows = []
    minute = 0
    for update_id, states in updates.items():
        for state in states:
            rows.append(
                (
                    start + dt.timedelta(minutes=minute),
                    "ERROR" if state == "FAILED" else "INFO",
                    "update_progress",
                    update_id,
                    f'{{"update_progress": {{"state": "{state}"}}}}',
                )
            )
            minute += 1
    # `origin` is a struct in the real log, so it is a struct here: the query reads
    # `origin.update_id`, and a fixture that flattened it would be testing a different query.
    spark.createDataFrame(rows, schema).withColumn(
        "origin", F.struct(F.col("update_id").alias("update_id"))
    ).drop("update_id").createOrReplaceTempView("event_log_probe")


def test_a_completed_update_is_reported_as_completed(spark) -> None:  # type: ignore[no-untyped-def]
    """The happy path, which the old query got wrong in the safest-looking direction."""
    _write_log(spark, {"44a237b3": HAPPY})
    row = spark.sql(_statement("last_state")).collect()[0]
    assert row["last_state"] == "COMPLETED", (
        f"the update ended COMPLETED and the record says {row['last_state']!r}. If this is "
        f"WAITING_FOR_RESOURCES, the query is taking the alphabetical maximum of a string "
        f"again."
    )
    assert row["update_id"] == "44a237b3"


def test_a_failed_update_is_reported_as_failed(spark) -> None:  # type: ignore[no-untyped-def]
    """The symmetric half, and the one that matters.

    A field that always says WAITING_FOR_RESOURCES is wrong twice: it hides a success AND it
    hides a failure, and only the second one costs anything.
    """
    _write_log(spark, {"deadbeef": UNHAPPY})
    row = spark.sql(_statement("last_state")).collect()[0]
    assert row["last_state"] == "FAILED", row["last_state"]
    assert row["error_events"] == 1


def test_the_update_described_is_the_last_one_that_finished(spark) -> None:  # type: ignore[no-untyped-def]
    """Which update the record is about, when a retry loop left several.

    One `bundle run` produced six updates in fourteen minutes. The CTE used to take the most
    recent update to leave ANY event, which during a retry is one that has not finished; it now
    takes the most recent to reach a terminal state, because what this record describes is a
    set of tables and an update that has not ended has not produced them.
    """
    _write_log(
        spark,
        {
            "first-failed": UNHAPPY,
            "second-failed": UNHAPPY,
            "third-succeeded": HAPPY,
            # Started after the others and still going: it has produced nothing.
            "fourth-running": ("CREATED", "WAITING_FOR_RESOURCES", "RUNNING"),
        },
    )
    row = spark.sql(_statement("last_state")).collect()[0]
    assert row["update_id"] == "third-succeeded", row["update_id"]
    assert row["last_state"] == "COMPLETED", row["last_state"]


def test_the_record_carries_every_terminal_update_so_a_retry_loop_is_visible(spark) -> None:  # type: ignore[no-untyped-def]
    """Six failed updates from one launch were invisible in a record describing one update."""
    _write_log(
        spark,
        {"one": UNHAPPY, "two": UNHAPPY, "three": UNHAPPY, "four": HAPPY},
    )
    rows = spark.sql(_statement("final_state")).collect()
    assert [r["update_id"] for r in rows] == ["four", "three", "two", "one"], rows
    assert [r["final_state"] for r in rows] == ["COMPLETED", "FAILED", "FAILED", "FAILED"]


def test_no_statement_takes_a_max_or_min_of_a_non_ordinal_column() -> None:
    """The CLASS, swept, rather than the one line that published a constant.

    `MAX`/`MIN` are meaningful on a timestamp and on a number. On a state, a reason, a name or
    an id they return the alphabetical extreme, which is a well-defined answer to a question
    nobody asked. `max_by(value, timestamp)` is the one that means "the last one".

    This reads the lane's SQL and refuses `MAX(` or `MIN(` applied to anything that is not a
    timestamp or a plainly numeric expression. It is deliberately crude: the point is that a
    new one has to be looked at, not that this list is complete.
    """
    import re

    allowed = re.compile(r"^(timestamp|__START_AT|__END_AT|.*_records|.*count.*|\d+)$", re.I)
    offenders: list[str] = []
    for lane in sorted((REPO / "databricks" / "src").glob("*.py")):
        for statement in _sql_calls_of(lane):
            # Comments stripped FIRST. The SQL in these files explains the defect this test
            # exists for, and the explanation contains the word it is looking for - so the
            # first version of this sweep failed on the comment documenting the fix.
            code = "\n".join(line.split("--", 1)[0] for line in statement.splitlines())
            for match in re.finditer(r"\b(MAX|MIN)\s*\(([^()]*)\)", code, re.I):
                argument = match.group(2).strip()
                if not allowed.match(argument):
                    offenders.append(f"{lane.name}: {match.group(0)}")
    assert not offenders, (
        "MAX/MIN over a column that is not ordered by value. If what is wanted is the latest "
        "or the earliest, that is max_by(value, timestamp):\n  " + "\n  ".join(offenders)
    )


def _sql_calls_of(path: Path) -> list[str]:
    module_path = REPO / "tests" / "spark" / "test_databricks_lane_parses.py"
    spec = importlib.util.spec_from_file_location("_lane_parses2", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return list(module._sql_calls(path.read_text(encoding="utf-8")))

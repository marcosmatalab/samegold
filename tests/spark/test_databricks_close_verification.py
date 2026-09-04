"""The two verification tasks, executed against real tables, one falsification per claim.

`verify_month` and `verify_no_restatement` are the bodies of the two branches of the close's
condition task. Between them they make seven claims about what the close did or did not do, and
a claim that has never been made to FAIL is a claim nobody has checked - which is the whole
argument of this repository, applied here to the checks themselves.

So each one is falsified: the fixture is corrupted in exactly the way that claim exists to
catch, and the test asserts that claim - and, where the corruption is isolable, only that
claim - goes false. A check that stays green under its own defect is decoration with a good
name on it.

The statements are EXTRACTED from the notebooks with the parse test's own reader rather than
restated here, for the reason round 17 wrote down: a test that rebuilds the expression it is
checking agrees with the defect. The only rewrite is Unity Catalog's three-part name, exactly
as `test_databricks_lane_parses.py` does it.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.spark

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks" / "src"

MONTH = "2026-01"
RUN, TASK = "job-1", "task-1"
# The close instant: February, so January is eligible to close and January is not the month in
# progress. `close_month` derives the same boundary and publishes it as a task value.
CLOSED_AT = dt.datetime(2026, 2, 1, 3, 0, 0)
EARLIER = dt.datetime(2026, 1, 1, 3, 0, 0)

CLOSED_COLUMNS = (
    "accounting_month STRING, close_version INT, gross_cents BIGINT, returns_cents BIGINT, "
    "net_cents BIGINT, line_count BIGINT, return_count BIGINT, returns_rejected_count BIGINT, "
    "restated_at TIMESTAMP, restatement_reason STRING"
)
SOURCE_COLUMNS = (
    "accounting_month STRING, gross_cents BIGINT, returns_cents BIGINT, net_cents BIGINT, "
    "line_count BIGINT, return_count BIGINT, returns_rejected_count BIGINT"
)


def _lane_parses() -> Any:
    spec = importlib.util.spec_from_file_location(
        "_lane_parses", REPO / "tests" / "spark" / "test_databricks_lane_parses.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _statement(notebook: str, marker: str, **substitutions: str) -> str:
    """The one SELECT in `notebook` that computes verification rows, ready to execute."""
    module = _lane_parses()
    source = (LANE / notebook).read_text(encoding="utf-8")
    matches = [s for s in module._sql_calls(source) if marker in s]
    assert len(matches) == 1, f"expected one statement with {marker!r} in {notebook}"
    statement = matches[0]
    for name, value in substitutions.items():
        statement = statement.replace("{" + name + "}", value)
    # The placeholders are filled BEFORE the three-part name is flattened, because the
    # flattener matches `catalog.schema.table` and would not recognise `{catalog}.main.x`.
    # The parse test does the two in the same order for the same reason.
    return module._single_part(statement)


def _closed(spark: Any, rows: list[tuple[Any, ...]]) -> None:
    spark.createDataFrame(rows, CLOSED_COLUMNS).createOrReplaceTempView("revenue_closed")


def _source(spark: Any, rows: list[tuple[Any, ...]]) -> None:
    spark.createDataFrame(rows, SOURCE_COLUMNS).createOrReplaceTempView("revenue_by_month")


# A January closed twice: version 0, then version 1 restating it after late arrivals. The
# figures of version 1 are the ones the source now holds, which is what a correct close does.
def _version(
    version: int,
    *,
    gross: int = 1000,
    returns: int = 100,
    net: int | None = None,
    lines: int = 10,
    at: dt.datetime = CLOSED_AT,
) -> tuple[Any, ...]:
    return (
        MONTH,
        version,
        gross,
        returns,
        gross - returns if net is None else net,
        lines,
        2,
        1,
        at,
        "late arrivals after close" if version else "first close",
    )


def _good_close(spark: Any) -> None:
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])


def _verdicts(spark: Any) -> dict[str, bool]:
    statement = _statement(
        "verify_month.py",
        "net_is_gross_minus_returns",
        catalog="samegold",
        accounting_month=MONTH,
        job_run_id=RUN,
        task_run_id=TASK,
    )
    return {row["check_name"]: row["ok"] for row in spark.sql(statement).collect()}


def test_a_correct_restatement_passes_every_check(spark) -> None:  # type: ignore[no-untyped-def]
    """The green case, first, because five checks that are always red prove nothing either."""
    _good_close(spark)
    verdicts = _verdicts(spark)
    assert len(verdicts) == 5, verdicts
    assert all(verdicts.values()), verdicts


def test_a_net_that_is_not_gross_minus_returns_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """Money that does not add up, in the row a person signs."""
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1, net=999)])
    _source(spark, [(MONTH, 1000, 100, 999, 10, 2, 1)])
    verdicts = _verdicts(spark)
    assert verdicts["net_is_gross_minus_returns"] is False, verdicts


def test_a_gap_in_the_version_numbers_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """Versions 0 and 2 with no 1: a restatement that was written and then lost.

    The count check and the distinctness check are both here because either alone passes on a
    shape the other catches - two rows numbered 0 and 2 have the right count and the wrong
    maximum, and two rows both numbered 1 have the right maximum and a duplicate.
    """
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(2)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    verdicts = _verdicts(spark)
    assert verdicts["versions_have_no_gaps"] is False, verdicts


def test_a_duplicated_version_number_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """The other half of the same claim: the same version number twice."""
    _closed(spark, [_version(1, gross=800, returns=80, lines=8, at=EARLIER), _version(1)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    verdicts = _verdicts(spark)
    assert verdicts["versions_have_no_gaps"] is False, verdicts


def test_an_earlier_version_stamped_after_the_newest_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """What a rewritten signed-off version looks like from inside one run.

    Version 0 carries a timestamp LATER than version 1's, which cannot happen if versions are
    appended in order and never touched again. This is `test_a_version_that_was_signed_off_is
    _never_rewritten` asked of the workspace instead of the committed record.
    """
    later = CLOSED_AT + dt.timedelta(days=1)
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=later), _version(1)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    verdicts = _verdicts(spark)
    assert verdicts["earlier_versions_are_older"] is False, verdicts


def test_a_written_figure_that_is_not_the_sources_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """The close copied a number the source does not have.

    `line_count` is the one moved, deliberately: changing gross would also break
    `net_is_gross_minus_returns` and the corruption would no longer be isolated to the claim
    under test.
    """
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1, lines=11)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    verdicts = _verdicts(spark)
    assert verdicts["written_equals_the_source_month"] is False, verdicts
    # And ONLY that one: an isolated corruption must produce an isolated verdict, or the
    # check names are not telling a reader where to look.
    assert [name for name, ok in verdicts.items() if not ok] == [
        "written_equals_the_source_month"
    ], verdicts


def test_a_month_closed_before_it_ended_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """The bitemporal rule that stops the month in progress being signed off.

    Without it, January is closed on 5 January against a partial month and every later close
    restates it, for ever. The instant checked is the one RECORDED on the row, not one this
    task was handed.
    """
    inside = dt.datetime(2026, 1, 20, 3, 0, 0)
    _closed(
        spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1, at=inside)]
    )
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    verdicts = _verdicts(spark)
    assert verdicts["month_was_eligible_to_close"] is False, verdicts


# ------------------------------------------------------------------ the other branch


def _no_op_verdicts(spark: Any, as_of_month: str = "2026-03") -> list[dict[str, Any]]:
    statement = _statement(
        "verify_no_restatement.py",
        "no_eligible_month_drifted",
        catalog="samegold",
        as_of_month=as_of_month,
        job_run_id=RUN,
        task_run_id=TASK,
    )
    return [row.asDict() for row in spark.sql(statement).collect()]


def test_a_close_that_correctly_wrote_nothing_passes(spark) -> None:  # type: ignore[no-untyped-def]
    """Every eligible month has a version and none of them has drifted."""
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    rows = _no_op_verdicts(spark)
    assert rows, "no eligible months were examined at all, so nothing was checked"
    assert all(row["ok"] for row in rows), rows


def test_an_eligible_month_that_was_never_closed_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """Not a restatement that did not happen - a CLOSE that did not happen.

    The close's own MERGE cannot tell the two apart: it writes nothing either way. This is the
    check that can, and it is the reason the false branch is a task and not an absence.
    """
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1)])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1), ("2026-02", 50, 0, 50, 1, 0, 0)])
    rows = _no_op_verdicts(spark)
    missing = [r for r in rows if r["check_name"] == "every_eligible_month_has_a_version"]
    assert {r["accounting_month"]: r["ok"] for r in missing} == {MONTH: True, "2026-02": False}


def test_a_month_that_drifted_since_its_last_version_is_caught(spark) -> None:  # type: ignore[no-untyped-def]
    """The silent half: the close should have restated and decided it had nothing to do."""
    _closed(spark, [_version(0, gross=800, returns=80, lines=8, at=EARLIER), _version(1)])
    _source(spark, [(MONTH, 1234, 100, 1134, 12, 2, 1)])
    rows = _no_op_verdicts(spark)
    drifted = [r for r in rows if r["check_name"] == "no_eligible_month_drifted"]
    assert drifted and all(r["ok"] is False for r in drifted), rows


def test_the_month_in_progress_is_not_examined(spark) -> None:  # type: ignore[no-untyped-def]
    """The eligibility boundary is the same one the close applies, or the branch contradicts it.

    With the close instant inside January, January is not yet closable - so a January with no
    version is correct, and a check that flagged it would be telling the operator to fix
    something the close is right about.
    """
    _closed(spark, [])
    _source(spark, [(MONTH, 1000, 100, 900, 10, 2, 1)])
    assert _no_op_verdicts(spark, as_of_month=MONTH) == []

"""Every version of the cloud close, against the OSS lane over the population that made it.

This is SG-04 - "a closed month moves after it is closed" - executed on Databricks rather than
argued about. The workspace closed January at 14 198 046 cents, 573 events for January arrived
afterwards, and a second close restated it to 25 582 615 without touching the first version.

The check is not that the numbers are plausible. It is that a lane with no shared code
recomputes each version to the cent from the same events:

  version 0  the base population,             755 events
  version 1  base plus one late arrival,      1328 events
  version 2  base plus two late arrivals,     1883 events

MEASURED, over the population `samegold generate-late` reproduces:

  2026-01 v0  gross 14 198 046  net 12 911 212   425 lines   71 returns  22 rejected
  2026-01 v1  gross 25 582 615  net 23 268 535   793 lines  126 returns  32 rejected
  2026-02 v0  gross    199 379  net    199 379     3 lines    0 returns   0 rejected

COMPUTED HERE AND NOT YET PUBLISHED BY ANY RUN, for the third close:

  2026-01 v2  gross 37 622 605  net 33 763 943  1158 lines  191 returns  49 rejected

That row is a prediction until a run writes it - `docs/predictions-2026-09-05.md` says so and
can be scored - and this file compares only versions the record actually carries, so declaring
it early asserts nothing. What it does is make the third population a thing this repository
knows how to build before the workspace is asked to build it.

February gained no version in either close, because its aggregate did not change - the MERGE's
`<>` guard doing what a restatement policy is for: a new version is a CHANGE, not a re-run. It
will not gain one in the third either, and the reason is the same rule for the third time: a
return books into the month of the SALE it refers to, so the 36 February and 8 March returns in
the third arrival move JANUARY. Every one of the 408 new orders has a January sale timestamp.
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from pathlib import Path

import pytest

from samegold.generator.events import FAST
from samegold.generator.late import population_for
from samegold.oracle.duckdb_gold import revenue_by_month_as_of

REPO = Path(__file__).resolve().parents[2]
RECORD = REPO / "evidence" / "databricks" / "SG-DBX-01.json"
BASE_SEED, LATE_SEED, THIRD_SEED, PROFILE = 20260901, 20260904, 20260905, FAST
AS_OF = dt.datetime(2030, 1, 1, tzinfo=dt.UTC)

# Which population each close version was cut over, as the SEQUENCE of late arrivals that
# produced it. A sequence rather than one optional seed because the third close is where "the
# late population" stopped being a single thing: version 2 rests on both arrivals, in order,
# and an arrival is filtered against every arrival before it.
#
# The test fails by name on a version it does not know rather than comparing against whichever
# population happens to be last.
CLOSE_POPULATIONS: dict[int, tuple[int, ...]] = {
    0: (),
    1: (LATE_SEED,),
    2: (LATE_SEED, THIRD_SEED),
}

# The record's column names against the reference row's attributes.
FIELDS = (
    ("gross_cents", "gross_cents"),
    ("net_cents", "net_cents"),
    ("line_count", "line_count"),
    ("return_count", "return_count"),
    ("returns_rejected_count", "returns_rejected_count"),
)


@pytest.fixture(scope="module")
def record() -> dict:
    assert RECORD.exists(), f"no record at {RECORD.relative_to(REPO)}"
    return json.loads(RECORD.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def reference() -> dict[int, dict[str, object]]:
    """The OSS close over each documented population, keyed by close version."""
    root = Path(tempfile.mkdtemp(prefix="closeparity-"))
    out: dict[int, dict[str, object]] = {}
    for version, late_seeds in CLOSE_POPULATIONS.items():
        bronze = population_for(
            root / str(version), base_seed=BASE_SEED, late_seeds=late_seeds, profile=PROFILE
        )
        out[version] = {row.accounting_month: row for row in revenue_by_month_as_of(bronze, AS_OF)}
    return out


def test_every_published_version_is_what_the_oss_lane_computes(record, reference) -> None:  # type: ignore[no-untyped-def]
    """The cloud close, version by version, against a lane that shares no code with it.

    A restatement is the easiest number in this project to get wrong quietly: it is produced
    once, it supersedes a figure somebody signed off, and nothing recomputes it. So it is
    recomputed here, from the same events, by the DuckDB reference.
    """
    rows = record.get("revenue_closed")
    assert isinstance(rows, list) and rows, "the record publishes no closed months"
    unknown = sorted({int(r["close_version"]) for r in rows} - set(CLOSE_POPULATIONS))
    assert not unknown, (
        f"the record carries close version(s) {unknown} and this file does not know which "
        f"population they were cut over. A third close needs its seed added to "
        f"CLOSE_POPULATIONS; comparing against the wrong population is how a difference of "
        f"data gets reported as a difference of implementation."
    )

    for row in rows:
        version = int(row["close_version"])
        month = str(row["accounting_month"])
        expected = reference[version].get(month)
        assert expected is not None, (
            f"the workspace published {month} v{version} and the OSS lane computes no such "
            f"month over the population that version was cut on"
        )
        for column, attribute in FIELDS:
            assert row[column] == getattr(expected, attribute), (
                f"{month} v{version} {column}: workspace {row[column]}, "
                f"OSS lane {getattr(expected, attribute)}"
            )


def test_a_version_that_was_signed_off_is_never_rewritten(record) -> None:  # type: ignore[no-untyped-def]
    """What "bitemporal" has to mean, checked on the record rather than on the design.

    Version 0 of January is the figure finance signed off. The second close may add a version;
    it may not change that one, and it may not change its `restated_at` either - a version
    whose timestamp moves is a version that was rewritten with the evidence of it removed.
    """
    rows = [r for r in (record.get("revenue_closed") or []) if isinstance(r, dict)]
    by_month: dict[str, list[dict]] = {}
    for row in rows:
        by_month.setdefault(str(row["accounting_month"]), []).append(row)

    for month, versions in by_month.items():
        numbers = sorted(int(v["close_version"]) for v in versions)
        assert numbers == list(range(len(numbers))), (
            f"{month} has close versions {numbers}, which is not a sequence from zero: a "
            f"version was removed, or one was written without its predecessor"
        )
        stamps = [v.get("restated_at") for v in sorted(versions, key=lambda v: v["close_version"])]
        assert len(set(filter(None, stamps))) == len([s for s in stamps if s]), (
            f"{month} has two versions with the same restated_at: {stamps}"
        )
        first = next(v for v in versions if int(v["close_version"]) == 0)
        assert first.get("restatement_reason") == "first close", first


def test_the_reference_agrees_that_february_had_nothing_to_restate(reference) -> None:
    """The negative half, and the one a restatement policy is actually judged on.

    Sixteen returns for February and four for March arrived late, and February gained no
    version. That is correct and it is not an omission: `gold_close.py` groups by the month of
    the SALE, those returns are against January sales, and February's aggregate is unchanged.
    A close that wrote a version anyway would be restating a month that did not move.
    """
    before, after = reference[0]["2026-02"], reference[1]["2026-02"]
    for _, attribute in FIELDS:
        assert getattr(before, attribute) == getattr(after, attribute), attribute
    assert after.gross_cents == 199379

"""The two Type 2 dimensions, compared - which is what `gold_close.py` says it exists for.

The Databricks lane maintains `dim_customer_scd2` with AUTO CDC, a Databricks-only primitive;
the OSS lane maintains the same dimension with a hand-written two-pass MERGE, and a third
implementation recomputes it in DuckDB. PARITY.md has said for rounds that comparing them is
the point of having both.

Nothing compared them until the lane ran. It ran on 3 September 2026 and they disagreed:
**78 versions and 18 closed rows on Databricks, 75 and 15 on the OSS side.** Sixty customers
and sixty open rows on both, so `open_rows = customers` held and the difference was exactly
three versions.

It ran again the same day with `track_history_column_list=["segment", "country"]` set, and the
workspace produced **75 / 60 / 60 / 15** - the OSS lane's shape exactly. That record is in this
repository (`evidence/databricks/SG-DBX-01.json`), so the comparison below is no longer a
description of a divergence: it is a check that runs.

The cause is measured below rather than asserted: three of the population's 78
`customer_upserted` events are HEARTBEATS - an upsert that repeats the segment and country the
customer already had - and AUTO CDC's default is a new version whenever ANY column changes,
while the source view carries `event_ts` and `event_id`, which change on every upsert by
construction. So the default was guaranteed to produce one version per event.

Which is right is a contract question, and the contract answers it in
`samegold.pipelines.transform.dim_customer_scd2`: "A Type 2 dimension records CHANGES, not
heartbeats." `track_history_column_list=["segment", "country"]` is now set on the lane, and the
next run is what tells us whether it took.
"""

from __future__ import annotations

import datetime as dt
import json
import tempfile
from itertools import pairwise
from pathlib import Path

import pytest

from samegold.generator.events import FAST, generate
from samegold.oracle.duckdb_gold import scd2_as_of

REPO = Path(__file__).resolve().parents[2]
# What the seed step writes: `samegold generate --profile fast --seed 20260901`.
SEED, PROFILE = 20260901, FAST
TRACKED = ("segment", "country")
# Captured from the workspace. Absent until somebody runs the query in
# docs/databricks-run.md; the comparison below says so by name rather than passing quietly.
CAPTURED = REPO / "evidence" / "databricks" / "dim_customer_scd2.json"
# The record the workspace produced. It carries the dimension's SHAPE - versions, customers,
# open and closed rows - which is what the divergence showed up in, and not its rows.
RECORD = REPO / "evidence" / "databricks" / "SG-DBX-01.json"


@pytest.fixture(scope="module")
def population() -> tuple[list[dict], list[dict]]:
    """The generated upserts, deduplicated, and the reference dimension over them."""
    root = Path(tempfile.mkdtemp(prefix="dimparity-"))
    generate(root / "g", seed=SEED, profile=PROFILE)
    bronze = root / "g" / "bronze"

    by_id: dict[str, dict] = {}
    for path in sorted(bronze.rglob("*.json")):
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or not line.lstrip().startswith("{"):
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("event_type") == "customer_upserted" and record.get("customer_id"):
                by_id.setdefault(record["event_id"], record)
    upserts = sorted(by_id.values(), key=lambda r: (r["customer_id"], r["event_ts"], r["event_id"]))
    dimension = list(scd2_as_of(bronze, dt.datetime(2030, 1, 1, tzinfo=dt.UTC)))
    return upserts, dimension


def _heartbeats(upserts: list[dict]) -> list[dict]:
    """Upserts whose tracked attributes are identical to the customer's previous one."""
    out, previous = [], {}
    for record in upserts:
        key = record["customer_id"]
        now = tuple(record.get(a) for a in TRACKED)
        if key in previous and previous[key] == now:
            out.append(record)
        previous[key] = now
    return out


def test_the_reference_dimension_records_changes_and_not_heartbeats(population) -> None:  # type: ignore[no-untyped-def]
    """The arithmetic that explains the divergence, pinned so it cannot drift silently.

    75 + 3 = 78 and 15 + 3 = 18. If any of these three numbers moves, the explanation moves
    with it and the two lanes have to be compared again rather than assumed to still differ by
    the same three rows.
    """
    upserts, dimension = population
    heartbeats = _heartbeats(upserts)
    assert len(upserts) == 78, len(upserts)
    assert len(heartbeats) == 3, [r["event_id"] for r in heartbeats]
    assert len(dimension) == 75, len(dimension)

    open_rows = [r for r in dimension if r["valid_to"] is None]
    assert len(open_rows) == 60
    assert len({r["customer_id"] for r in dimension}) == 60
    assert len(dimension) - len(open_rows) == 15

    # One version per EVENT is what AUTO CDC produced; one per CHANGE is what the contract
    # asks for. The two differ by exactly the heartbeats, and that is the whole finding.
    assert len(dimension) + len(heartbeats) == len(upserts)

    # And they are nameable, which is what makes the divergence a finding rather than a delta.
    assert sorted(r["event_id"] for r in heartbeats) == [
        "cu-C000028-1",
        "cu-C000038-1",
        "cu-C000043-1",
    ]


def test_no_two_consecutive_versions_of_a_customer_are_identical(population) -> None:  # type: ignore[no-untyped-def]
    """The PROPERTY, which is what actually has to hold whatever the counts are.

    A version that repeats its predecessor's attributes is not a version; it is the same fact
    with a second row. This is the rule stated as a property rather than as a number, so it
    keeps meaning something on a population these fixtures do not describe.
    """
    _, dimension = population
    by_customer: dict[str, list[dict]] = {}
    for row in dimension:
        by_customer.setdefault(row["customer_id"], []).append(row)
    duplicates = []
    for customer, rows in by_customer.items():
        ordered = sorted(rows, key=lambda r: str(r["valid_from"]))
        for previous, nxt in pairwise(ordered):
            if all(previous[a] == nxt[a] for a in TRACKED):
                duplicates.append((customer, previous["valid_from"], nxt["valid_from"]))
    assert not duplicates, (
        f"consecutive versions with identical {TRACKED}: {duplicates}. A Type 2 dimension "
        f"records changes, not heartbeats."
    )


def test_the_two_dimensions_agree_row_by_row(population) -> None:  # type: ignore[no-untyped-def]
    """The comparison `gold_close.py` declares as its reason for existing.

    One half comes from a workspace, so it has to be CAPTURED - there is no way to compute
    AUTO CDC's output without Databricks, and inventing an expected shape here would be a
    second implementation of the primitive rather than a comparison with it.

    When the capture is absent this skips, and the skip names the file and the query. That is
    weaker than a failure and it is honest about being weaker: the alternative is a red suite
    on every clone that has never touched a workspace, which trains people to ignore it.
    """
    if not CAPTURED.exists():
        pytest.skip(
            f"no capture at {CAPTURED.relative_to(REPO)}. It is produced by running, in the "
            f"workspace after `scripts/databricks_run.sh run-full-refresh`:\n\n"
            f"  SELECT customer_id, segment, country, __START_AT, __END_AT\n"
            f"  FROM samegold.main.dim_customer_scd2 ORDER BY customer_id, __START_AT;\n\n"
            f"saved as a JSON array. docs/databricks-run.md carries the same instruction "
            f"beside the two anchors this comparison decides."
        )
    _, dimension = population
    captured = json.loads(CAPTURED.read_text(encoding="utf-8"))

    def key(row: dict, start: str, end: str) -> tuple:
        return (
            str(row["customer_id"]),
            str(row[start])[:19],
            "open" if row[end] is None else str(row[end])[:19],
            row["segment"],
            row["country"],
        )

    ours = sorted(key(r, "valid_from", "valid_to") for r in dimension)
    theirs = sorted(key(r, "__START_AT", "__END_AT") for r in captured)
    only_ours = [r for r in ours if r not in theirs]
    only_theirs = [r for r in theirs if r not in ours]
    assert not only_ours and not only_theirs, (
        f"the hand-written MERGE and AUTO CDC produced different dimensions.\n"
        f"  only in the OSS lane   ({len(only_ours)}): {only_ours[:5]}\n"
        f"  only on Databricks     ({len(only_theirs)}): {only_theirs[:5]}\n"
        f"If the Databricks side has extra rows whose attributes repeat their predecessor's, "
        f"`track_history_column_list` did not take effect on that run."
    )


def test_the_workspace_dimension_has_the_shape_the_oss_lane_computes(population) -> None:  # type: ignore[no-untyped-def]
    """The cross-runtime comparison, running for real against a committed record.

    This is the check that did not exist when it mattered. AUTO CDC and the hand-written MERGE
    disagreed by three versions on the first run and nothing in the repository noticed; the
    difference showed up in a terminal, in four aggregates, which is exactly what this asserts.

    It is WEAKER than the row-by-row comparison below, and the difference is worth naming: four
    matching totals do not prove the same sixty customers have the same seventy-five intervals.
    Two dimensions could agree on every count and disagree on which customer changed when. The
    row-level capture is what closes that, and until it exists this is the half that can run.
    """
    import json

    _, dimension = population
    record = json.loads(RECORD.read_text(encoding="utf-8"))
    shape = (record.get("dim_customer_scd2") or [{}])[0]
    assert shape, "the record carries no dim_customer_scd2 section"

    open_rows = [r for r in dimension if r["valid_to"] is None]
    ours = {
        "versions": len(dimension),
        "customers": len({r["customer_id"] for r in dimension}),
        "open_rows": len(open_rows),
        "closed_rows": len(dimension) - len(open_rows),
    }
    theirs = {k: shape[k] for k in ours}
    assert ours == theirs, (
        f"the hand-written MERGE and AUTO CDC produced differently shaped dimensions.\n"
        f"  OSS lane   {ours}\n"
        f"  Databricks {theirs}\n"
        f"If Databricks has MORE versions, `track_history_column_list` is not in force on the "
        f"run that wrote the record: AUTO CDC's default is a new version whenever any column "
        f"changes, and the source view carries `event_ts` and `event_id`, which change on "
        f"every upsert."
    )

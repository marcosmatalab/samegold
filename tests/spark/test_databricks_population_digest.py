"""The workspace's fingerprint of its population, executed against the one this repo computes.

`tests/fast/test_databricks_dimension_parity.py` compares two implementations of a Type 2
dimension. For that comparison to mean anything the two halves must have read the same events,
and the only tie was a COUNT: the fixture generated each documented population and picked the
one whose bronze line count matched `rows.bronze_events`.

MEASURED, which is why this file exists. Reordering the `countries` list in
`samegold/generator/events.py` - a list literal, the same number of events, the same rng
consumption - leaves every published number identical (1328 rows, 96 upserts, 4 heartbeats, 92
versions, 60 customers, 60 open, 32 closed; the close unchanged to the cent) and gives thirty
customers a different history. Renaming the skus changes 1216 values and **all nineteen**
existing parity tests still pass, because gross is `qty * unit_price_cents` and no dimension
carries a sku. So the count is not a tie, and the money is not one either.

The tie is a digest over the events themselves, computed on both sides:

  * `databricks/src/publish_evidence.py`, in SQL, over `bronze_events` in the workspace;
  * `samegold.generator.late.population_digest`, in Python, over the generated files.

Two implementations of one rule agree only if something makes them. THIS is that something:
the statement is EXTRACTED from the notebook - not restated, for the reason round 17 wrote
down - and run in local Spark over a bronze table built from the real generated population,
including the three lines that are not JSON. The only name substituted is the table's.

Writing it found two things that reading it would not have:

  * the three corrupt lines are TRUNCATED objects - `{"event_id": "bad-0000009", "event_type":
    "order_placed",` - and the domain has to say what becomes of them. Measured here: Python
    cannot parse them at all, and local Spark nulls the whole row, so both halves exclude the
    same three. They agree by the behaviour of ONE reader, though, and the reader that fills
    the real table is Auto Loader in rescue mode, which nothing here can run; whether a
    partially parsed record keeps its leading fields is a setting. So the domain asks for
    `arrival_ts` as well as `event_id`, which makes it independent of that question, and
    `test_the_corrupt_lines_fall_outside_the_domain_on_both_sides` pins the numbers rather
    than a story about them.
  * the generator emits 9223372036854775808 - one past the top of a BIGINT - for two events,
    so the workspace holds NULL where Python holds an integer. Without that rule the two
    digests differ on two rows out of 1325, which is exactly the near-miss that gets explained
    away as "the digest is broken". The record's own `bad_events` section reports
    `unit_price_cents: null` for precisely those two ids.
"""

from __future__ import annotations

import importlib.util
import tempfile
from pathlib import Path

import pytest

from samegold.generator.events import FAST
from samegold.generator.late import BRONZE_DIGEST_COLUMNS, population_digest, population_for

pytestmark = pytest.mark.spark

REPO = Path(__file__).resolve().parents[2]
LANE = REPO / "databricks" / "src" / "publish_evidence.py"
BASE_SEED, LATE_SEED = 20260901, 20260904

# What the population is, so a fixture that silently stopped producing the corrupt lines would
# fail here rather than leave the domain rule untested.
EXPECTED_LINES = 1328
EXPECTED_OUTSIDE = 3


def _statement() -> str:
    """The digest statement, read out of the notebook with the parse test's own extractor."""
    path = REPO / "tests" / "spark" / "test_databricks_lane_parses.py"
    spec = importlib.util.spec_from_file_location("_lane_parses", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    matches = [s for s in module._sql_calls(LANE.read_text(encoding="utf-8")) if "chr(31)" in s]
    assert len(matches) == 1, f"expected one digest statement in {LANE.name}, found {len(matches)}"
    # The one name that cannot exist outside a workspace. Every clause is evaluated as written.
    return matches[0].replace("{catalog}.main.bronze_events", "bronze_probe")


@pytest.fixture(scope="module")
def bronze() -> Path:
    """The population the second close read, generated here."""
    root = Path(tempfile.mkdtemp(prefix="digestprobe-"))
    return population_for(root / "full", base_seed=BASE_SEED, late_seed=LATE_SEED, profile=FAST)


@pytest.fixture(scope="module")
def probe(spark, bronze) -> Path:  # type: ignore[no-untyped-def]
    """`bronze_events` as a JSON reader produces it: the declared schema, corrupt lines kept.

    The corrupt lines have to arrive as ROWS rather than be dropped, because the whole domain
    rule is about what happens to them. A reader that dropped them would test a population the
    lane never had, and the row count below is what says which happened.
    """
    from samegold.pipelines.schema import bronze_schema

    (
        spark.read.schema(bronze_schema())
        .option("mode", "PERMISSIVE")
        .json([str(path) for path in sorted(bronze.rglob("part-*.json"))])
        .createOrReplaceTempView("bronze_probe")
    )
    return bronze


def test_the_probe_is_the_population_the_lane_reads(spark, probe) -> None:  # type: ignore[no-untyped-def]
    """Before comparing digests: does the fixture reproduce the shape the workspace had?

    A digest test over a table with no corrupt rows in it would agree for the wrong reason and
    say nothing about the only interesting part of the definition.
    """
    total = spark.sql("SELECT COUNT(*) AS n FROM bronze_probe").collect()[0]["n"]
    assert total == EXPECTED_LINES, (
        f"the probe holds {total} rows and the lane's population is {EXPECTED_LINES}. If this "
        f"is {EXPECTED_LINES - EXPECTED_OUTSIDE}, the reader DROPPED the corrupt lines instead "
        f"of keeping them, and the domain rule below is being tested against a population the "
        f"workspace never had."
    )
    outside = spark.sql(
        "SELECT COUNT(*) AS n FROM bronze_probe WHERE event_id IS NULL OR arrival_ts IS NULL"
    ).collect()[0]["n"]
    assert outside == EXPECTED_OUTSIDE, outside


def test_the_corrupt_lines_fall_outside_the_domain_on_both_sides(spark, probe) -> None:  # type: ignore[no-untyped-def]
    """The domain rule, as numbers rather than as a story about readers.

    Three of the 1328 lines are truncated objects. This asserts what each side does with them,
    because the digest agreeing is only meaningful if both sides excluded the SAME rows - two
    halves that each dropped a different three would agree on the count and hash different
    populations.

    Note what is NOT asserted: that Spark returns, or does not return, the fields it parsed
    before a truncation. It does not here, which means the two halves would agree on
    `event_id` alone - but that is one reader's behaviour and the workspace's is Auto Loader
    in rescue mode. The domain asks for `arrival_ts` too so the answer does not matter.
    """
    total = spark.sql("SELECT COUNT(*) AS n FROM bronze_probe").collect()[0]["n"]
    outside = spark.sql(
        "SELECT COUNT(*) AS n FROM bronze_probe WHERE event_id IS NULL OR arrival_ts IS NULL"
    ).collect()[0]["n"]
    ours = population_digest(probe)

    assert total == EXPECTED_LINES, total
    assert outside == EXPECTED_OUTSIDE, outside
    assert ours.rows_outside_the_digest == outside, (
        f"the OSS half puts {ours.rows_outside_the_digest} rows outside the digest and the "
        f"table puts {outside} outside. The two halves are hashing different populations and "
        f"the digest below would be comparing them anyway."
    )
    assert ours.digest_rows == total - outside == EXPECTED_LINES - EXPECTED_OUTSIDE, (
        ours.digest_rows,
        total - outside,
    )


def test_the_two_implementations_of_the_digest_agree(spark, probe) -> None:  # type: ignore[no-untyped-def]
    """The tie itself: the notebook's SQL and this repository's Python, on the same events.

    Neither is a reimplementation of the other checked by reading. They are two programs, and
    this runs both.
    """
    row = spark.sql(_statement()).collect()[0].asDict()
    ours = population_digest(probe, row["columns"].split(","))

    assert row["columns"].split(",") == list(BRONZE_DIGEST_COLUMNS), (
        f"the notebook publishes the projection {row['columns']} and this repository renders "
        f"{','.join(BRONZE_DIGEST_COLUMNS)}. The order is part of what is hashed."
    )
    assert row["digest_rows"] == ours.digest_rows, (row["digest_rows"], ours.digest_rows)
    assert row["rows_outside_the_digest"] == ours.rows_outside_the_digest, (
        row["rows_outside_the_digest"],
        ours.rows_outside_the_digest,
    )
    assert row["digest"] == ours.digest, (
        f"the workspace's SQL and this repository's Python hashed the same rows differently.\n"
        f"  SQL:    {row['digest']}\n"
        f"  Python: {ours.digest}\n"
        f"That is a difference of RULE, not of data - the two ran over one table. The places "
        f"they can disagree are the coalesce (concat_ws skips nulls without it), the "
        f"separators, the BIGINT range, and the sort."
    )
    # The arithmetic the record publishes, checked where both halves are in hand.
    assert ours.digest_rows + ours.rows_outside_the_digest == EXPECTED_LINES


def test_a_value_the_table_cannot_hold_is_rendered_as_the_table_holds_it(spark, probe) -> None:  # type: ignore[no-untyped-def]
    """The BIGINT range, which is a property of the TABLE and not of the JSON text.

    The generator emits 9223372036854775808 - two to the sixty-third - as `unit_price_cents`
    for two events. Python reads it as an integer; a BIGINT column cannot hold it and the
    workspace holds NULL. `bad_events` in the committed record reports `unit_price_cents:
    null` for exactly those two ids, which is where this rule comes from rather than from a
    guess about overflow.

    Without the rule the digests differ on two of 1325 rows, which is precisely the kind of
    near-miss that gets explained away as "the digest is broken".
    """
    over = spark.sql(
        "SELECT COUNT(*) AS n FROM bronze_probe "
        "WHERE arrival_ts IS NOT NULL AND unit_price_cents IS NULL"
    ).collect()[0]["n"]
    assert over >= 2, (
        f"only {over} rows have a null unit_price_cents. The two events carrying 2**63 are "
        f"what makes the BIGINT range part of the rendering rule; if they are gone, the rule "
        f"is untested rather than unnecessary."
    )


def test_the_digest_moves_when_one_value_moves(spark, probe) -> None:  # type: ignore[no-untyped-def]
    """Falsification, and the reason this file is not a formality.

    A digest that agrees is worth nothing until it is shown to disagree. One country on one
    row of 1325 is the smallest version of the drift that started this. `sku` is here because
    it is the field NO other test in this repository looks at: renaming the skus leaves all
    nineteen parity tests green.
    """
    before = spark.sql(_statement()).collect()[0].asDict()

    for column, replacement in (("country", "'ZZ'"), ("sku", "'MOVED'")):
        spark.sql(f"""
            SELECT * EXCEPT ({column}),
                   CASE WHEN event_id = (SELECT MIN(event_id) FROM bronze_probe
                                         WHERE {column} IS NOT NULL)
                        THEN {replacement} ELSE {column} END AS {column}
            FROM bronze_probe
        """).createOrReplaceTempView("bronze_probe_mutated")
        after = (
            spark.sql(_statement().replace("bronze_probe", "bronze_probe_mutated"))
            .collect()[0]
            .asDict()
        )
        assert after["digest"] != before["digest"], (
            f"one row's {column} was changed and the digest did not move. A fingerprint that "
            f"cannot see a single field is not tying anything."
        )
        # And it is the VALUE that moved it, not the size of the domain.
        assert after["digest_rows"] == before["digest_rows"], (
            after["digest_rows"],
            before["digest_rows"],
        )

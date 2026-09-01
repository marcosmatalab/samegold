"""The DuckDB witness: gold recomputed by a different engine.

Runs in-process, needs no JVM, and is fast enough to be part of the fast lane. It reads the
same bronze files the Spark pipeline reads, so a disagreement is about the computation and
not about the input.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path

import duckdb

_SQL_PATH = Path(__file__).with_name("gold_revenue.sql")
_SCD2_PATH = Path(__file__).with_name("gold_scd2.sql")


@dataclass(frozen=True, slots=True)
class RevenueRow:
    accounting_month: str
    gross_cents: int
    returns_cents: int
    net_cents: int
    line_count: int
    return_count: int


def revenue_by_month_as_of(bronze_dir: Path, as_of: dt.datetime) -> list[RevenueRow]:
    """gold.revenue_by_month as it would have been reported at ``as_of``."""
    glob = str(Path(bronze_dir) / "**" / "*.json")
    sql = _SQL_PATH.read_text(encoding="utf-8")
    con = duckdb.connect()
    try:
        rows = con.execute(sql, {"glob": glob, "as_of": as_of.isoformat()}).fetchall()
    finally:
        con.close()
    return [
        RevenueRow(str(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5])) for r in rows
    ]


def scd2_as_of(bronze_dir: Path, as_of: dt.datetime) -> list[dict[str, object]]:
    """gold.dim_customer_scd2 as it would have stood at ``as_of``."""
    glob = str(Path(bronze_dir) / "**" / "*.json")
    con = duckdb.connect()
    try:
        rows = con.execute(
            _SCD2_PATH.read_text(encoding="utf-8"),
            {"glob": glob, "as_of": as_of.isoformat()},
        ).fetchall()
    finally:
        con.close()
    return [
        {
            "customer_id": str(r[0]),
            "valid_from": str(r[1]),
            "valid_to": None if r[2] is None else str(r[2]),
            "segment": r[3],
            "country": r[4],
            "is_current": bool(r[5]),
        }
        for r in rows
    ]


class DuckDBWitness:
    """Named wrapper so the witness matrix can talk about witnesses uniformly."""

    name = "duckdb"
    independence = "engine"  # different engine, same author, same contract

    def scd2(self, bronze_dir: Path, as_of: dt.datetime) -> list[dict[str, object]]:
        return scd2_as_of(bronze_dir, as_of)

    def revenue(self, bronze_dir: Path, as_of: dt.datetime) -> dict[str, dict[str, int]]:
        return {
            r.accounting_month: {
                "gross_cents": r.gross_cents,
                "returns_cents": r.returns_cents,
                "net_cents": r.net_cents,
                "line_count": r.line_count,
                "return_count": r.return_count,
            }
            for r in revenue_by_month_as_of(bronze_dir, as_of)
        }


_COUNTS_SQL = """
WITH raw AS (
    SELECT * FROM read_json($glob, format = 'newline_delimited', union_by_name = true,
                            ignore_errors = true, filename = true)
),
tagged AS (
    SELECT *,
           row_number() OVER (PARTITION BY event_id ORDER BY CAST(event_ts AS TIMESTAMPTZ)) AS rn
    FROM raw WHERE event_id IS NOT NULL
),
unique_events AS (SELECT * FROM tagged WHERE rn = 1),
classified AS (
    SELECT
        CASE
            WHEN event_type NOT IN ('order_placed','order_line_amended',
                                    'return_registered','customer_upserted')
                THEN 'unknown_event_type'
            WHEN event_type = 'order_placed' AND (order_id IS NULL OR sku IS NULL
                                                  OR customer_id IS NULL OR qty IS NULL)
                THEN 'missing_required_field'
            WHEN event_type = 'order_placed' AND CAST(qty AS BIGINT) <= 0
                THEN 'non_positive_quantity'
            WHEN event_type = 'order_placed' AND CAST(unit_price_cents AS BIGINT) < 0
                THEN 'negative_price'
            WHEN event_type = 'order_placed' AND currency <> 'EUR'
                THEN 'unknown_currency'
            WHEN event_type = 'return_registered' AND CAST(qty AS BIGINT) <= 0
                THEN 'non_positive_quantity'
            ELSE 'accepted'
        END AS bucket
    FROM unique_events
)
SELECT
    (SELECT count(*) FROM raw)                                   AS parsed_rows,
    (SELECT count(*) FROM raw WHERE event_id IS NULL)            AS no_event_id,
    (SELECT count(*) FROM tagged WHERE rn > 1)                   AS duplicates,
    (SELECT count(*) FROM unique_events)                         AS unique_events,
    (SELECT count(*) FROM classified WHERE bucket = 'accepted')  AS accepted,
    (SELECT count(*) FROM classified WHERE bucket <> 'accepted') AS rejected_by_rule
"""


def reference_counts(bronze_dir: Path) -> dict[str, int]:
    """Row-level accounting of the whole input, for the conservation invariant.

    ``raw_lines`` is counted in Python rather than in SQL on purpose: DuckDB's
    ``ignore_errors`` silently drops an unparseable line, so asking SQL how many lines there
    were would ask the component that already lost them. Counting the file bytes is the only
    way the unparseable ones show up at all - which is precisely the class of record that
    disappears without trace in most pipelines.
    """
    bronze_dir = Path(bronze_dir)
    raw_lines = 0
    for path in sorted(bronze_dir.rglob("*.json")):
        with path.open("rb") as handle:
            raw_lines += sum(1 for line in handle if line.strip())
    con = duckdb.connect()
    try:
        row = con.execute(_COUNTS_SQL, {"glob": str(bronze_dir / "**" / "*.json")}).fetchone()
    finally:
        con.close()
    assert row is not None
    parsed, no_id, duplicates, unique_events, accepted, rejected = (int(x) for x in row)
    # "unparseable_json" is the reason name from the contract; DuckDB does not report it, so
    # it is derived as (lines in the files) - (rows the parser produced).
    return {
        "raw_lines": raw_lines,
        "parsed_rows": parsed,
        "unparseable": raw_lines - parsed,
        "no_event_id": no_id,
        "duplicates": duplicates,
        "unique_events": unique_events,
        "accepted": accepted,
        "rejected_by_rule": rejected,
    }

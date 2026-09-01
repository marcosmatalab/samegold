"""The DuckDB witness: gold recomputed by a different engine.

Runs in-process, needs no JVM, and is fast enough to be part of the fast lane. It reads the
same bronze files the Spark pipeline reads, so a disagreement is about the computation and
not about the input.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from samegold.domain.bitemporal import versions_from_snapshots

_SQL_PATH = Path(__file__).with_name("gold_revenue.sql")
_SCD2_PATH = Path(__file__).with_name("gold_scd2.sql")


def _connect() -> duckdb.DuckDBPyConnection:
    """A connection whose session timezone is pinned to UTC.

    Not a detail. DuckDB resolves TIMESTAMPTZ rendering and interval arithmetic against the
    session timezone, which defaults to the machine's. Leaving it unset means the close is
    computed differently on a developer laptop in Madrid than on a CI runner in UTC, and the
    difference only appears across a daylight-saving boundary, which is to say once or twice
    a year and never in the demo. Every explicit conversion to the accounting timezone is
    written out in the SQL instead.
    """
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    return con


@dataclass(frozen=True, slots=True)
class RevenueRow:
    accounting_month: str
    gross_cents: int
    returns_cents: int
    net_cents: int
    line_count: int
    return_count: int
    returns_rejected_count: int


def revenue_by_month_as_of(bronze_dir: Path, as_of: dt.datetime) -> list[RevenueRow]:
    """gold.revenue_by_month as it would have been reported at ``as_of``."""
    glob = str(Path(bronze_dir) / "**" / "*.json")
    sql = _SQL_PATH.read_text(encoding="utf-8")
    con = _connect()
    try:
        rows = con.execute(sql, {"glob": glob, "as_of": as_of.isoformat()}).fetchall()
    finally:
        con.close()
    return [
        RevenueRow(str(r[0]), int(r[1]), int(r[2]), int(r[3]), int(r[4]), int(r[5]), int(r[6]))
        for r in rows
    ]


def scd2_as_of(bronze_dir: Path, as_of: dt.datetime) -> list[dict[str, object]]:
    """gold.dim_customer_scd2 as it would have stood at ``as_of``."""
    glob = str(Path(bronze_dir) / "**" / "*.json")
    con = _connect()
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


def revenue_versions(bronze_dir: Path, closes: list[dt.datetime]) -> list[dict[str, Any]]:
    """The versioned close table: one row per (accounting_month, close_version).

    This is the real shape of gold. Until an adversarial review pointed it out, the projection
    used for digests declared a ``close_version`` column that no implementation produced: the
    tests injected a literal zero. A column that only exists in the test is not a column.
    """
    snapshots = [
        (
            close.isoformat(),
            {
                row.accounting_month: {
                    "gross_cents": row.gross_cents,
                    "returns_cents": row.returns_cents,
                    "net_cents": row.net_cents,
                    "line_count": row.line_count,
                    "return_count": row.return_count,
                    "returns_rejected_count": row.returns_rejected_count,
                }
                for row in revenue_by_month_as_of(bronze_dir, close)
            },
        )
        for close in closes
    ]
    return versions_from_snapshots(snapshots)


class DuckDBWitness:
    """Named wrapper so the witness matrix can talk about witnesses uniformly."""

    name = "duckdb"
    independence = "engine"  # different engine, same author, same contract

    def versions(self, bronze_dir: Path, closes: list[dt.datetime]) -> list[dict[str, Any]]:
        return revenue_versions(bronze_dir, closes)

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
                "returns_rejected_count": r.returns_rejected_count,
            }
            for r in revenue_by_month_as_of(bronze_dir, as_of)
        }


_COUNTS_SQL = """
-- The columns are DECLARED here for the same reason they are declared in gold_revenue.sql:
-- with union_by_name the schema depends on which files happened to arrive, so a batch with
-- no amendment in it has no `new_qty` column and every query that mentions one fails with a
-- binder error. A counting query that only works on some inputs cannot back an invariant.
WITH raw AS (
    SELECT * FROM read_json($glob, format = 'newline_delimited',
                            ignore_errors = true, filename = true,
                            columns = {
                                'event_id': 'VARCHAR', 'event_type': 'VARCHAR',
                                'event_ts': 'VARCHAR', 'arrival_ts': 'VARCHAR',
                                'order_id': 'VARCHAR', 'customer_id': 'VARCHAR',
                                'sku': 'VARCHAR', 'qty': 'JSON', 'new_qty': 'JSON',
                                'unit_price_cents': 'JSON', 'currency': 'VARCHAR',
                                'return_id': 'VARCHAR', 'reason': 'VARCHAR',
                                'segment': 'VARCHAR', 'country': 'VARCHAR',
                                'boundary': 'VARCHAR'
                            })
),
-- Same JSON-typed conversion as gold_revenue.sql: a float or a quoted number is not an
-- integer, and this query's buckets have to match the Spark pipeline's, which nulls both.
typed AS (
    SELECT * EXCLUDE (qty, new_qty, unit_price_cents),
           CASE WHEN json_type(qty) IN ('BIGINT', 'UBIGINT')
                THEN TRY_CAST(qty AS BIGINT) END AS qty,
           CASE WHEN json_type(new_qty) IN ('BIGINT', 'UBIGINT')
                THEN TRY_CAST(new_qty AS BIGINT) END AS new_qty,
           CASE WHEN json_type(unit_price_cents) IN ('BIGINT', 'UBIGINT')
                THEN TRY_CAST(unit_price_cents AS BIGINT) END AS unit_price_cents
    FROM raw
),
-- The SAME total order as gold_revenue.sql, and for the same reason. With only event_ts in
-- the ORDER BY, two copies of one event_id with different payloads left the winner to the
-- physical order of the rows: swapping two lines in one file flipped this query's buckets
-- while the revenue query, which does have the tie-break, did not move. The accounting then
-- reported zero accepted records for a close that booked two thousand cents. Applying the
-- fix to two of the three queries was worse than not applying it at all, because the two
-- that agreed made the third look verified.
stamped AS (
    SELECT * EXCLUDE (event_ts, arrival_ts),
           CASE WHEN regexp_full_match(event_ts,
                    '\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?\s*(Z|[+-]\d{2}:?\d{2})?')
                THEN TRY_CAST(event_ts AS TIMESTAMPTZ) END AS event_ts,
           CASE WHEN regexp_full_match(arrival_ts,
                    '\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?\s*(Z|[+-]\d{2}:?\d{2})?')
                THEN TRY_CAST(arrival_ts AS TIMESTAMPTZ) END AS arrival_ts
    FROM typed
),
tagged AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY event_id
               ORDER BY event_ts, arrival_ts,
                        sha256(COALESCE(event_type, '') || '|' || COALESCE(order_id, '') || '|'
                            || COALESCE(customer_id, '') || '|' || COALESCE(sku, '') || '|'
                            || COALESCE(CAST(qty AS VARCHAR), '') || '|'
                            || COALESCE(CAST(new_qty AS VARCHAR), '') || '|'
                            || COALESCE(CAST(unit_price_cents AS VARCHAR), '') || '|'
                            || COALESCE(currency, '') || '|' || COALESCE(return_id, '') || '|'
                            || COALESCE(reason, '') || '|' || COALESCE(segment, '') || '|'
                            || COALESCE(country, ''))
           ) AS rn
    FROM stamped WHERE event_id IS NOT NULL
),
unique_events AS (SELECT * FROM tagged WHERE rn = 1),
-- The SAME closed enum as the quarantine_reason() expression in
-- src/samegold/pipelines/transform.py, branch for branch and in the same order, because the
-- conservation invariant compares this accounting with that pipeline's.
--
-- Both are NULL-safe on purpose: a comparison against a missing column is NULL, not false,
-- and the first version of the Spark side let a record with no `currency` fall through every
-- branch to 'accepted' while this query excluded it. Two implementations that disagree about
-- which bucket a record is in are two implementations that will disagree about the close as
-- soon as such a record exists.
classified AS (
    SELECT
        CASE
            WHEN event_type IS NULL
                 OR event_type NOT IN ('order_placed','order_line_amended',
                                       'return_registered','customer_upserted')
                THEN 'unknown_event_type'
            WHEN event_ts IS NULL OR arrival_ts IS NULL
                THEN 'missing_required_field'
            WHEN event_type = 'order_placed' AND (order_id IS NULL OR sku IS NULL
                                                  OR customer_id IS NULL OR qty IS NULL
                                                  OR unit_price_cents IS NULL
                                                  OR currency IS NULL)
                THEN 'missing_required_field'
            WHEN event_type = 'order_line_amended' AND (order_id IS NULL OR sku IS NULL
                                                        OR new_qty IS NULL)
                THEN 'missing_required_field'
            WHEN event_type = 'return_registered' AND (order_id IS NULL OR sku IS NULL
                                                       OR qty IS NULL)
                THEN 'missing_required_field'
            WHEN event_type = 'customer_upserted' AND customer_id IS NULL
                THEN 'missing_required_field'
            WHEN event_type IN ('order_placed','return_registered')
                 AND CAST(qty AS BIGINT) <= 0
                THEN 'non_positive_quantity'
            WHEN event_type = 'order_line_amended' AND CAST(new_qty AS BIGINT) <= 0
                THEN 'non_positive_quantity'
            WHEN event_type = 'order_placed' AND CAST(unit_price_cents AS BIGINT) < 0
                THEN 'negative_price'
            WHEN event_type = 'order_placed' AND currency <> 'EUR'
                THEN 'unknown_currency'
            ELSE 'accepted'
        END AS bucket
    FROM unique_events
)
SELECT
    (SELECT count(*) FROM raw)                                   AS parsed_rows,
    -- A line the parser could not read has no event_id, and with the columns DECLARED the
    -- reader emits an all-NULL row for it rather than dropping it. So `raw_lines - parsed`
    -- is zero for exactly the case it was written to count, and the unparseable records were
    -- being reported through the `no_event_id` door instead. Both are the same door in the
    -- Spark pipeline (`unparseable_json`), and this is now the same count.
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
    con = _connect()
    try:
        row = con.execute(_COUNTS_SQL, {"glob": str(bronze_dir / "**" / "*.json")}).fetchone()
    finally:
        con.close()
    assert row is not None
    parsed, no_id, duplicates, unique_events, accepted, rejected = (int(x) for x in row)
    # "unparseable_json" is the reason name from the contract, and it covers two shapes that
    # DuckDB reports differently: a line the reader dropped entirely (raw_lines - parsed) and
    # a line it read into an all-NULL row because the columns are declared (counted as
    # no_event_id). The Spark pipeline sends both through one door, so they are added here.
    # Reporting them separately is what made the published `unparseable` figure zero on data
    # that contained unparseable lines.
    return {
        "raw_lines": raw_lines,
        "parsed_rows": parsed,
        "unparseable": (raw_lines - parsed) + no_id,
        "no_event_id": no_id,
        "duplicates": duplicates,
        "unique_events": unique_events,
        "accepted": accepted,
        "rejected_by_rule": rejected,
    }

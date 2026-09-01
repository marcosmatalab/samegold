"""Negative controls for the equivalence classification.

Classifying a surviving mutant as "equivalent" is the one place in this project where the
author gets to decide that a failure is not a failure. So each classification carries an
assumption id, and this module tries to break it:

  * for an assumption about the DATA, it builds a dataset that violates the assumption and
    checks that the mutants covered by it stop being equivalent. A mutant that stays
    equivalent even when its assumption is false was classified for the wrong reason.
  * for an assumption about the DERIVATION, it asserts the structural property directly over
    the real data, on every seed, because a structural argument that nobody re-checks after a
    refactor is a comment.

The published claim is therefore not "these mutants are harmless" but "these mutants are
harmless while this named property holds, and here is the run where it does not hold and
they all diverge".
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import duckdb

from samegold.mutation.equivalents import ASSUMPTIONS
from samegold.mutation.operators import Mutant, mutate_sql

_AS_OF = "2026-03-05T22:59:59+00:00"


PAYLOAD_FIELDS = ("event_type", "order_id", "sku", "qty", "new_qty", "unit_price_cents")


def _write_violating_dataset(root: Path, differing_field: str, mode: str = "differs") -> Path:
    """Two records sharing an event_id whose payloads differ in exactly one field.

    One dataset per field, because the tie-break is a hash over all of them and a mutant that
    drops one field from the hash only shows up when THAT field is what distinguishes the two
    copies. A single probe dataset found 2 of 9 mutants; the per-field sweep is what makes the
    negative control mean anything.

    This is a contract violation, not a supported input: the producer promises that an
    event_id identifies one fact. The point of writing it is to show what the tie-break
    protects against once that promise breaks.
    """
    bronze = root / "bronze" / "batch=202601010000"
    bronze.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = [
        {
            "event_id": "op-Y-1",
            "event_type": "order_placed",
            "event_ts": "2026-01-11T10:00:00+00:00",
            "arrival_ts": "2026-01-11T10:05:00+00:00",
            "order_id": "Y",
            "customer_id": "C1",
            "sku": "S1",
            "qty": 2,
            "unit_price_cents": 5000,
            "currency": "EUR",
        },
        {
            "event_id": "rt-Y-1",
            "event_type": "return_registered",
            "event_ts": "2026-01-20T10:00:00+00:00",
            "arrival_ts": "2026-01-20T10:05:00+00:00",
            "order_id": "Y",
            "sku": "S1",
            "qty": 1,
            "return_id": "R1",
            "reason": "size",
        },
    ]
    # Twelve colliding pairs, not one. Which of the two copies wins is decided by comparing
    # two hashes, so a mutant that changes the hash flips a given pair with probability about
    # one half: with a single pair the probe was a coin toss and reported 2 of 9 mutants as
    # affected. Twelve pairs make a false negative a one-in-four-thousand event.
    for index in range(12):
        base = {
            "event_id": f"op-X{index}-1",
            "event_type": "order_placed",
            "event_ts": "2026-01-10T10:00:00+00:00",
            "arrival_ts": "2026-01-10T10:05:00+00:00",
            "order_id": f"X{index}",
            "customer_id": "C1",
            "sku": f"S{index}",
            "qty": 1,
            "unit_price_cents": 10000,
            "currency": "EUR",
        }
        twin = dict(base)
        if mode == "null":
            if differing_field == "new_qty":
                # new_qty only exists on an amendment, so the pair has to be amendments or
                # the "missing field" case is a no-op that probes nothing.
                base = dict(
                    base, event_type="order_line_amended", new_qty=2, event_id=f"am-X{index}-1"
                )
                twin = dict(base)
            # The twin is MISSING the field entirely. This is the shape that separates
            # "COALESCE(x, '')" from a bare "x": with a NULL operand the concatenation
            # collapses the whole tie-break hash to NULL, and the row it belonged to stops
            # being comparable. Without this mode the probe only exercised non-NULL values,
            # where dropping a COALESCE is genuinely a no-op, and it reported 2 of 9.
            twin.pop(differing_field, None)
            rows.extend([base, twin])
            continue
        if differing_field == "event_type":
            twin["event_type"] = "order_line_amended"
            twin["new_qty"] = 4
        elif differing_field == "new_qty":
            base = dict(base, event_type="order_line_amended", new_qty=2, event_id=f"am-X{index}-1")
            twin = dict(base, new_qty=8)
        elif differing_field in ("qty", "unit_price_cents"):
            twin[differing_field] = int(str(base[differing_field])) + 7
        else:
            twin[differing_field] = str(base[differing_field]) + "-other"
        rows.extend([base, twin])
    (bronze / "part-00000.json").write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8"
    )
    return root / "bronze"


def _run(sql: str, bronze: Path) -> list[tuple[Any, ...]]:
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    try:
        return con.execute(sql, {"glob": str(bronze / "**" / "*.json"), "as_of": _AS_OF}).fetchall()
    finally:
        con.close()


def probe_data_assumption(
    reference_sql: str, assumption: str = "unique-event-payload"
) -> dict[str, Any]:
    """Run every mutant covered by a data assumption against data that violates it."""
    from samegold.mutation.equivalents import EQUIVALENCES

    covered = {(e.operator, e.context) for e in EQUIVALENCES if e.assumption == assumption}
    candidates = [m for m in mutate_sql(reference_sql) if (m.operator, m.context) in covered]
    checked = [m.mutant_id for m in candidates]
    diverged: set[str] = set()
    for field, mode in [(f, m) for f in PAYLOAD_FIELDS for m in ("differs", "null")]:
        with tempfile.TemporaryDirectory(prefix="samegold-probe-") as tmp:
            bronze = _write_violating_dataset(Path(tmp), field, mode)
            baseline = _run(reference_sql, bronze)
            for mutant in candidates:
                try:
                    result = _run(mutant.source, bronze)
                except duckdb.Error:
                    diverged.add(mutant.mutant_id)
                    continue
                if result != baseline:
                    diverged.add(mutant.mutant_id)
    return {
        "assumption": assumption,
        "statement": ASSUMPTIONS[assumption],
        "mutants_checked": checked,
        "fields_probed": list(PAYLOAD_FIELDS),
        "modes_probed": ["differs", "null"],
        "mutants_that_diverge_when_it_is_false": sorted(diverged),
        # Named, not hidden: a mutant the probe could not falsify keeps its classification
        # but is published as unfalsified, because "I could not break it" and "it cannot be
        # broken" are different statements.
        "mutants_the_probe_could_not_falsify": sorted(set(checked) - diverged),
        "verdict": (
            f"conditional: {len(diverged)} of {len(checked)} covered mutants change their "
            f"answer once the assumption is false, which is what makes the classification "
            f"conditional rather than convenient"
            if diverged
            else "UNCONDITIONAL OR MISCLASSIFIED: no covered mutant changed its answer even "
            "with the assumption violated, so the stated reason is not the reason"
        ),
    }


def probe_structural_assumption(bronze_dir: Path, as_of: str) -> dict[str, Any]:
    """Assert that the refunds month keys are a subset of the gross month keys."""
    sql = """
    WITH raw AS (
        SELECT * FROM read_json($glob, format='newline_delimited', ignore_errors=true,
            columns={'event_id':'VARCHAR','event_type':'VARCHAR','event_ts':'VARCHAR',
                     'arrival_ts':'VARCHAR','order_id':'VARCHAR','customer_id':'VARCHAR',
                     'sku':'VARCHAR','qty':'BIGINT','new_qty':'BIGINT',
                     'unit_price_cents':'BIGINT','currency':'VARCHAR','return_id':'VARCHAR',
                     'reason':'VARCHAR','segment':'VARCHAR','country':'VARCHAR',
                     'boundary':'VARCHAR'})
    ),
    arrived AS (SELECT * FROM raw WHERE event_id IS NOT NULL AND arrival_ts IS NOT NULL
                AND CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)),
    sales AS (
        SELECT DISTINCT strftime(CAST(event_ts AS TIMESTAMPTZ) AT TIME ZONE 'Europe/Madrid',
                                 '%Y-%m') AS m
        FROM arrived WHERE event_type = 'order_placed' AND qty > 0 AND unit_price_cents >= 0
          AND currency = 'EUR'
    ),
    refund_months AS (
        SELECT DISTINCT strftime(s.sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m') AS m
        FROM arrived r
        JOIN (SELECT order_id, sku, CAST(event_ts AS TIMESTAMPTZ) AS sale_ts FROM arrived
              WHERE event_type = 'order_placed') s
          ON s.order_id = r.order_id AND s.sku = r.sku
        WHERE r.event_type = 'return_registered'
    )
    SELECT (SELECT count(*) FROM refund_months WHERE m NOT IN (SELECT m FROM sales))
               AS orphan_months
    """
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    try:
        row = con.execute(
            sql, {"glob": str(bronze_dir / "**" / "*.json"), "as_of": as_of}
        ).fetchone()
    finally:
        con.close()
    orphans = int(row[0]) if row else -1
    return {
        "assumption": "refunds-months-are-a-subset-of-gross-months",
        "statement": ASSUMPTIONS["refunds-months-are-a-subset-of-gross-months"],
        "orphan_months": orphans,
        "verdict": "holds" if orphans == 0 else "VIOLATED: the equivalence class is void",
    }


def unused(_: Mutant) -> None:  # pragma: no cover - keeps the Mutant import meaningful
    return None

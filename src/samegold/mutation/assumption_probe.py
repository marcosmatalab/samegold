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


def probe_order_free_comparison(reference_sql: str, bronze_dir: Path, as_of: str) -> dict[str, Any]:
    """Two mutants are classified equivalent because "the comparison sorts anyway". Check it.

    The argument is that the final ORDER BY is presentation: every published comparison goes
    through a canonical digest that sorts by the projection's total order first. That is a
    claim about `verify/digest.py`, not about the SQL, and it is checkable directly: digest
    the reference's rows, digest the SAME rows in reverse, and require the two to be equal.

    A digest that did not sort would fail this, and the two mutants would immediately stop
    being equivalent. That is what makes the classification conditional rather than
    convenient.
    """
    from samegold.verify.digest import REVENUE_PROJECTION, CanonicalDigest

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    try:
        cursor = con.execute(
            reference_sql, {"glob": str(bronze_dir / "**" / "*.json"), "as_of": as_of}
        )
        names = [description[0] for description in cursor.description or []]
        rows = [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
    finally:
        con.close()
    # The reference emits one close, so close_version and restated_at are constants here; the
    # projection needs them present to hash the row at all.
    stamped = [
        dict(row, close_version=0, restated_at=as_of, restatement_reason="first close")
        for row in rows
    ]
    forward = CanonicalDigest.of(stamped, REVENUE_PROJECTION)
    backward = CanonicalDigest.of(list(reversed(stamped)), REVENUE_PROJECTION)
    return {
        "assumption": "comparison-is-order-free",
        "statement": ASSUMPTIONS["comparison-is-order-free"],
        "rows": len(stamped),
        "digest_forward": forward.hexdigest,
        "digest_reversed": backward.hexdigest,
        "verdict": (
            "holds: the digest of a permuted result is identical, so the final ORDER BY "
            "cannot change a published answer"
            if forward.hexdigest == backward.hexdigest
            else "VIOLATED: the comparison depends on row order and the equivalence class is void"
        ),
    }


def probe_orphan_returns_are_excluded(
    reference_sql: str, bronze_dir: Path, as_of: str
) -> dict[str, Any]:
    """One mutant is equivalent because orphan returns contribute to nothing. Check it.

    The argument is that turning a LEFT JOIN into an INNER JOIN is harmless because the rows
    it drops are exactly the ones the classification labels ``return_without_order`` and
    every aggregate then excludes. So: compute the close, ADD an orphan return to the input,
    compute it again, and require the two to be identical.

    If an orphan ever reached an output column, this diverges and the mutant is no longer
    equivalent - which is the point of writing a control rather than an argument.
    """
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")

    def close(glob: str) -> list[tuple[Any, ...]]:
        return con.execute(reference_sql, {"glob": glob, "as_of": as_of}).fetchall()

    try:
        before = close(str(bronze_dir / "**" / "*.json"))
        with tempfile.TemporaryDirectory(prefix="samegold-orphan-") as tmp:
            root = Path(tmp) / "bronze"
            (root / "batch=orphan").mkdir(parents=True)
            for source in sorted(bronze_dir.rglob("*.json")):
                target = root / source.relative_to(bronze_dir)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(source.read_bytes())
            orphan = {
                "event_id": "rt-orphan-0",
                "event_type": "return_registered",
                "event_ts": "2026-02-01T10:00:00+00:00",
                "arrival_ts": "2026-02-01T10:05:00+00:00",
                "order_id": "ORDER-THAT-DOES-NOT-EXIST",
                "sku": "SKU-THAT-DOES-NOT-EXIST",
                "qty": 3,
                "return_id": "R-orphan",
                "reason": "size",
            }
            (root / "batch=orphan" / "part-00000.json").write_text(
                json.dumps(orphan) + "\n", encoding="utf-8"
            )
            after = close(str(root / "**" / "*.json"))
    finally:
        con.close()
    return {
        "assumption": "orphan-returns-are-excluded-downstream",
        "statement": ASSUMPTIONS["orphan-returns-are-excluded-downstream"],
        "rows_before": len(before),
        "rows_after": len(after),
        "verdict": (
            "holds: adding a return that matches no sale changes no output column"
            if before == after
            else "VIOLATED: an orphan return reached the close and the equivalence class is void"
        ),
    }


def unused(_: Mutant) -> None:  # pragma: no cover - keeps the Mutant import meaningful
    return None

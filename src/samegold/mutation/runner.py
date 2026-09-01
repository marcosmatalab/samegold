"""Run every mutant past every witness and build the matrix.

The runner is deliberately dumb: it applies one mutation, recomputes gold, and asks each
witness whether it noticed. All the judgement lives in the witnesses and in the equivalence
classification, where it can be read.

A mutant is EQUIVALENT when the mutated program computes the same thing as the original on
every input, not merely on this input - for example flipping the direction of an ORDER BY
inside a window whose partition has one row. Those are classified by running the mutant over
several independent seeds and, when it survives all of them, by a written justification.
Anything that survives without a justification is a SURVIVOR and is published as such.
"""

from __future__ import annotations

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from samegold.mutation.equivalents import classify
from samegold.mutation.operators import Mutant, mutate_sql
from samegold.mutation.spec_mutants import SPEC_MUTANTS
from samegold.mutation.witness_matrix import WitnessMatrix
from samegold.verify.invariants import net_identity, returns_never_exceed_sales

# "runtime" is a witness like any other: a mutant that does not even run is killed, and
# saying so under its own name keeps the mutation score from borrowing credit that belongs
# to the SQL parser rather than to the harness.
WITNESSES = ("ledger", "invariants", "runtime")


@dataclass(frozen=True, slots=True)
class MutationRun:
    matrix: WitnessMatrix
    mutants: list[Mutant]
    detail: dict[str, dict[str, Any]]


def classify_equivalent(mutant: Mutant) -> str | None:
    """Why a surviving mutant is equivalent, or None if it is a genuine survivor.

    Delegates to mutation/equivalents.py, where each class is written out and defended.
    Nothing is classified automatically by heuristic: a survivor stays a survivor until a
    human writes down why it cannot change the answer on ANY input, and the README prints
    the score both ways so a reader can refuse the classification wholesale.
    """
    return classify(mutant.operator, mutant.original)


def _run_sql(sql: str, glob: str, as_of: dt.datetime) -> list[dict[str, Any]]:
    con = duckdb.connect()
    try:
        rows = con.execute(sql, {"glob": glob, "as_of": as_of.isoformat()}).fetchall()
    finally:
        con.close()
    return [
        {
            "accounting_month": str(r[0]),
            "gross_cents": int(r[1]),
            "returns_cents": int(r[2]),
            "net_cents": int(r[3]),
            "line_count": int(r[4]),
            "return_count": int(r[5]),
        }
        for r in rows
    ]


def _ledger_expectation(ledger: dict[str, Any], as_of: str) -> dict[str, dict[str, int]]:
    return {
        row["accounting_month"]: {
            k: row[k]
            for k in ("gross_cents", "returns_cents", "net_cents", "line_count", "return_count")
        }
        for row in ledger["revenue"]
        if row["as_of"] == as_of
    }


def run_mutation_campaign(
    reference_sql: str,
    bronze_dir: Path,
    ledger_json: dict[str, Any],
    as_of: dt.datetime | list[dt.datetime],
    extra_mutants: list[Mutant] | None = None,
    on_progress: Callable[[str], None] | None = None,
) -> MutationRun:
    # A mutant is evaluated at EVERY close, not at one. Two of the specification mutants
    # (SPEC-04 and SPEC-06, both about what a close knew at the time) are invisible at a
    # late close, where everything has already arrived, and only show up at the close where
    # the data was still in flight. Evaluating at a single as-of made them survive, which is
    # how this was found; keeping the note here so it does not get "simplified" back.
    as_ofs = [as_of] if isinstance(as_of, dt.datetime) else list(as_of)
    expectations = {a.isoformat(): _ledger_expectation(ledger_json, a.isoformat()) for a in as_ofs}
    glob = str(Path(bronze_dir) / "**" / "*.json")
    matrix = WitnessMatrix(witnesses=WITNESSES)
    detail: dict[str, dict[str, Any]] = {}

    mutants = mutate_sql(reference_sql) + list(extra_mutants or [])
    for spec in SPEC_MUTANTS:
        try:
            source = spec.apply(reference_sql)
        except ValueError as exc:  # an anchor that no longer matches is a real failure
            detail[spec.mutant_id] = {"error": str(exc)}
            continue
        mutants.append(
            Mutant(
                mutant_id=spec.mutant_id,
                kind="spec",
                operator="specification",
                location=spec.rule,
                original=spec.find[:120],
                mutated=spec.replace[:120],
                source=source,
            )
        )

    for mutant in mutants:
        if on_progress:
            on_progress(mutant.mutant_id)
        killed_by: list[str] = []
        failed_at: list[str] = []
        error: str | None = None
        for a in as_ofs:
            try:
                rows = _run_sql(mutant.source, glob, a)
            except Exception as exc:
                error = f"error: {exc}"[:200]
                break
            got = {
                r["accounting_month"]: {
                    k: r[k]
                    for k in (
                        "gross_cents",
                        "returns_cents",
                        "net_cents",
                        "line_count",
                        "return_count",
                    )
                }
                for r in rows
            }
            if got != expectations[a.isoformat()] and "ledger" not in killed_by:
                killed_by.append("ledger")
                failed_at.append(a.isoformat())
            if (net_identity(rows) or returns_never_exceed_sales(rows)) and (
                "invariants" not in killed_by
            ):
                killed_by.append("invariants")
        if error is not None:
            detail[mutant.mutant_id] = {"killed_by": ["runtime"], "reason": error}
            matrix.record(mutant.mutant_id, ["runtime"])
            continue

        equivalent = classify_equivalent(mutant) if not killed_by else None
        matrix.record(mutant.mutant_id, killed_by, equivalent_reason=equivalent)
        detail[mutant.mutant_id] = {
            "killed_by": killed_by,
            "first_divergence_at": failed_at[0] if failed_at else None,
            "operator": mutant.operator,
            "location": mutant.location,
            "kind": mutant.kind,
            "equivalent_reason": equivalent,
        }
    return MutationRun(matrix=matrix, mutants=mutants, detail=detail)

"""Equivalence classification, written by hand and defended in writing.

A mutant that no witness kills is one of three things: a hole in the harness, a hole in the
generator, or a mutant that cannot change the answer on any input. Only the third is
equivalent, and deciding which is which is a judgement call, so it is made here, in the
open, one entry at a time, with the reason attached and keyed by what the mutation actually
did rather than by an id that moves when the SQL is edited.

The README publishes both scores: the one that accepts this file's classification and the
one that refuses it. A reader who thinks an entry below is wishful thinking can use the
second number without having to re-run anything.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Equivalence:
    operator: str
    anchor: str
    reason: str


EQUIVALENCES: tuple[Equivalence, ...] = (
    Equivalence(
        operator="order:flip",
        anchor="",
        reason=(
            "order-only. The canonical digest sorts rows by the projection's total order "
            "before hashing, and every comparison in the harness is over a keyed mapping, "
            "so a change of row order cannot change any published value. This is equivalence "
            "with respect to the OBSERVATION, and it holds only because the digest refuses "
            "to be taken without a total order (verify/digest.py)."
        ),
    ),
    Equivalence(
        operator="join:kind-swap",
        anchor="FULL OUTER JOIN refunds",
        reason=(
            "unreachable branch. The right-only side of this join is a month with returns "
            "and no sales, which cannot exist while a return is imputed to the month of its "
            "sale: the sale is in that month by construction. Note the dependency - this "
            "equivalence is conditional on SPEC-01 being the correct rule. Under the other "
            "imputation rule the mutant is NOT equivalent, which is why SPEC-01 is a mutant "
            "and not a comment."
        ),
    ),
    Equivalence(
        operator="coalesce:drop-default",
        anchor="COALESCE(g.",
        reason=(
            "unreachable branch, same cause as the FULL OUTER JOIN above: the default only "
            "fires for a month that has returns and no sales."
        ),
    ),
    Equivalence(
        operator="join:kind-swap",
        anchor="JOIN effective AS e ON e.order_id",
        reason=(
            "neutralised by a predicate. Turning this INNER join into a LEFT join admits "
            "orphan returns with NULL on the right, and the very next predicates "
            "(event_ts >= sale_ts, qty <= e.qty) are NULL for those rows, so they are "
            "filtered out again. A classic, and the reason 'change the join type' alone is "
            "a weak test: the WHERE clause was already doing the work."
        ),
    ),
)


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def classify(operator: str, original: str) -> str | None:
    """Return the written reason a mutant is equivalent, or None if it is a survivor."""
    haystack = _normalise(original)
    for entry in EQUIVALENCES:
        if entry.operator != operator:
            continue
        if not entry.anchor or _normalise(entry.anchor) in haystack:
            return entry.reason
    return None

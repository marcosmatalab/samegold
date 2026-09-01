"""Equivalence classification, written by hand, keyed by CTE, and defended in writing.

A mutant that no witness kills is one of three things: a hole in the harness, a hole in the
generator, or a mutant that cannot change the answer on any input. Only the third is
equivalent, and deciding which is which is a judgement call, so it is made here, in the
open, one entry at a time.

Two design rules, both of them scars:

  * **No wildcards.** An earlier version had one entry that matched every ``order:flip``
    mutant regardless of where it was, and through it four mutants that decide WHICH ROW
    SURVIVES a deduplication were filed as "row order does not matter". An adversarial
    review reproduced all four with a counterexample. Entries are now keyed by the CTE the
    mutation lives in, so "the presentation ORDER BY" and "the deduplication window ORDER BY"
    can never be confused again.
  * **A contract-conditional equivalence must be checked against the data.** Some mutants are
    equivalent only because the contract guarantees something about the input, for example
    that two events sharing an ``event_id`` carry identical payloads. Those entries carry an
    ``assumption`` id, and tests/fast/test_mutation.py asserts that the generated data
    actually satisfies each assumption. An unverified assumption is an excuse.

The README publishes both scores: the one that accepts this file's classification and the
one that refuses it.
"""

from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Equivalence:
    operator: str
    context: str
    reason: str
    anchor: str = ""
    assumption: str | None = None


# Assumptions that make a contract-conditional equivalence valid. Each id is asserted against
# the generated data by a test; if the data ever violates one, the equivalence is void and the
# mutants it covers go back to being survivors.
ASSUMPTIONS: dict[str, str] = {
    "unique-event-payload": (
        "two records sharing an event_id carry identical payloads, because event_id is the "
        "producer's idempotency key and a re-delivery is a copy, not a new fact"
    ),
    "refunds-months-are-a-subset-of-gross-months": (
        "every month that appears on the refunds side of the final join also appears on the "
        "gross side. This is structural, not statistical: refunds derives from returns, "
        "returns derives from effective by an inner join on the sale, and gross groups "
        "effective. It is asserted over every generated dataset anyway, because a structural "
        "argument that nobody re-checks after a refactor is a comment"
    ),
    "comparison-is-order-free": (
        "every comparison in this project is over a keyed mapping or a canonical digest that "
        "sorts by the projection's total order before hashing, so the order rows come out of "
        "the reference in cannot change any published answer. Probed by permuting a result "
        "and re-digesting it"
    ),
    "orphan-returns-are-excluded-downstream": (
        "a return whose (order_id, sku) matches no accepted sale contributes to no output "
        "column: the classification labels it return_without_order and every aggregate "
        "filters it out. Probed by computing a close with and without an orphan return "
        "present and requiring the two to be identical"
    ),
}


EQUIVALENCES: tuple[Equivalence, ...] = (
    Equivalence(
        operator="order:flip",
        context="final",
        reason=(
            "presentation only. The final ORDER BY decides the order rows are printed in, and "
            "every comparison in this project is over a keyed mapping or a canonical digest "
            "that sorts by the projection's total order before hashing."
        ),
        assumption="comparison-is-order-free",
    ),
    Equivalence(
        operator="number:+1",
        context="final",
        anchor="1",
        reason=(
            "presentation only: ORDER BY 1 becomes ORDER BY 2. Same reason as the ORDER BY "
            "flip above."
        ),
        assumption="comparison-is-order-free",
    ),
    Equivalence(
        operator="number:+1",
        context="final",
        anchor="0",
        reason=(
            "unreachable branch. The literal is the default of a COALESCE over the right side "
            "of the FULL OUTER JOIN, which only fires for a month that has returns and no "
            "sales. That month cannot exist: refunds is derived from effective through an "
            "inner join on the sale, and gross groups effective, so the refunds month keys "
            "are a subset of the gross month keys by construction."
        ),
        assumption="refunds-months-are-a-subset-of-gross-months",
    ),
    Equivalence(
        operator="coalesce:drop-default",
        context="final",
        reason=(
            "unreachable branch, same cause: the default only fires for a month with returns "
            "and no sales, which the derivation cannot produce."
        ),
        assumption="refunds-months-are-a-subset-of-gross-months",
    ),
    Equivalence(
        operator="join:kind-swap",
        context="final",
        reason=(
            "unreachable branch, same cause. Turning the FULL OUTER JOIN into a LEFT JOIN "
            "drops months that exist only on the refunds side, and the derivation cannot "
            "produce one."
        ),
        assumption="refunds-months-are-a-subset-of-gross-months",
    ),
    Equivalence(
        operator="order:flip",
        context="dedup",
        reason=(
            "the deduplication window picks one row out of a set of IDENTICAL rows. Under the "
            "contract, two records sharing an event_id are copies of the same fact, so every "
            "ordering of that partition selects the same values. This is equivalence "
            "CONDITIONAL on the input, not on the code, and the assumption is asserted "
            "against the generated data by a test rather than trusted."
        ),
        assumption="unique-event-payload",
    ),
    Equivalence(
        operator="coalesce:drop-default",
        context="dedup",
        reason=(
            "the COALESCE is inside the payload hash that breaks ties in the deduplication "
            "window. Dropping it makes the hash NULL when a field is NULL, which changes the "
            "tie-break - and the tie-break is only consulted between identical rows. Same "
            "assumption as above, asserted by the same test."
        ),
        assumption="unique-event-payload",
    ),
    Equivalence(
        operator="join:kind-swap",
        context="return_candidates",
        reason=(
            "neutralised by the classification that follows. Turning this LEFT join into an "
            "INNER join drops orphan returns, and the CASE immediately after labels exactly "
            "those rows 'return_without_order' and excludes them from every output column. "
            "A classic: the WHERE clause was already doing the join's work."
        ),
        assumption="orphan-returns-are-excluded-downstream",
    ),
)


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def classify(operator: str, original: str, context: str = "final") -> Equivalence | None:
    """Return the equivalence entry that covers a mutant, or None if it is a survivor."""
    haystack = _normalise(original)
    for entry in EQUIVALENCES:
        if entry.operator != operator or entry.context != context:
            continue
        if not entry.anchor or _normalise(entry.anchor) == haystack:
            return entry
    return None

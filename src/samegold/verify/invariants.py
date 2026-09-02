"""Properties that must hold whatever the input, checked without any oracle.

These are the only checks in the project that do not depend on a second implementation or
on the generator's ledger, which makes them the part a reader has to trust least. Each one
returns the offending rows rather than a boolean, because "SCD2 is wrong" is not actionable
and "customer C000317 has two rows with is_current=true, versions 3 and 4" is.

Deliberately NOT here, because it is false: "reordering the arrival of events does not change
gold". With a watermark it does - an event that arrives after the watermark has passed its
event time is treated differently from the same event arriving early. The version that IS
true is invariance under a reordering that preserves the watermark order, and the test that
exercises it is
`tests/spark/test_transform_matches_reference.py::test_the_dedup_tie_break_is_a_total_order`,
which repartitions the input and asserts the same digest.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from itertools import pairwise
from typing import Any

from samegold.domain.bitemporal import instant_of

Row = Mapping[str, Any]
Violation = dict[str, Any]


def scd2_well_formed(rows: Iterable[Row], key: str = "customer_id") -> list[Violation]:
    """Per key: intervals sorted, disjoint, contiguous, exactly one open row.

    Contiguity is checked as ``valid_to == next.valid_from`` (closed-open intervals). A gap
    means a period in which the dimension cannot answer "what did this customer look like",
    and an overlap means a join can multiply facts. Both are silent in production and both
    are caught here.
    """
    by_key: dict[Any, list[Row]] = {}
    for row in rows:
        by_key.setdefault(row[key], []).append(row)
    violations: list[Violation] = []
    for k, group in by_key.items():
        ordered = sorted(group, key=lambda r: str(r["valid_from"]))
        open_rows = [r for r in ordered if r.get("is_current")]
        if len(open_rows) != 1:
            violations.append(
                {
                    "kind": "open_rows",
                    key: k,
                    "count": len(open_rows),
                    "detail": f"expected exactly one is_current row, found {len(open_rows)}",
                }
            )
        for previous, nxt in pairwise(ordered):
            if previous.get("valid_to") is None:
                violations.append(
                    {
                        "kind": "closed_row_without_valid_to",
                        key: k,
                        "valid_from": previous["valid_from"],
                    }
                )
            elif str(previous["valid_to"]) != str(nxt["valid_from"]):
                violations.append(
                    {
                        "kind": "gap_or_overlap",
                        key: k,
                        "left_valid_to": str(previous["valid_to"]),
                        "right_valid_from": str(nxt["valid_from"]),
                    }
                )
        if ordered and ordered[-1].get("valid_to") is not None:
            violations.append(
                {"kind": "last_row_closed", key: k, "valid_to": str(ordered[-1]["valid_to"])}
            )
    return violations


def conservation(
    ingested: int, accepted: int, quarantined: int, rescued: int, deduplicated: int
) -> list[Violation]:
    """Every record that entered left through exactly one door.

    ingested = accepted + quarantined + rescued + deduplicated. This is the invariant that
    catches the failure nobody notices: records that vanish because a filter dropped them
    and no counter moved.

    A WARNING about how to use it, learned from an adversarial review that pointed out this
    function was doing nothing. If all five arguments come from the same query, the identity
    is algebraic and it can never fail: substitute the definitions in ``_COUNTS_SQL`` and the
    sum reduces to ``raw_lines`` for any input whatsoever. It was being called exactly that
    way, and it passed on every seed for the same reason 1 = 1 passes.

    It means something only when the two sides come from DIFFERENT derivations. The caller in
    claims.py now passes the ingested count from the GENERATOR's ledger (which knows how many
    lines it wrote, because it wrote them) against the reference's accounting of what it
    found. That is a real cross-check, and see ``conservation_against_ledger`` below for the
    stronger one it is paired with.
    """
    total = accepted + quarantined + rescued + deduplicated
    if total != ingested:
        return [
            {
                "kind": "conservation",
                "ingested": ingested,
                "accepted": accepted,
                "quarantined": quarantined,
                "rescued": rescued,
                "deduplicated": deduplicated,
                "missing": ingested - total,
            }
        ]
    return []


def net_identity(rows: Iterable[Row]) -> list[Violation]:
    """net = gross - returns, on every row. Cheap, and it catches a wrong join direction."""
    return [
        {
            "kind": "net_identity",
            **{k: r[k] for k in ("accounting_month", "close_version") if k in r},
            "gross_cents": r["gross_cents"],
            "returns_cents": r["returns_cents"],
            "net_cents": r["net_cents"],
        }
        for r in rows
        if r["net_cents"] != r["gross_cents"] - r["returns_cents"]
    ]


def restatement_monotonic(rows: Sequence[Row]) -> list[Violation]:
    """Within a month, close versions increase and restated_at never goes backwards.

    A restatement that lands out of order means the accounting history has been rewritten
    rather than appended to, which is exactly what a bitemporal model exists to prevent.
    """
    by_month: dict[Any, list[Row]] = {}
    for row in rows:
        by_month.setdefault(row["accounting_month"], []).append(row)
    violations: list[Violation] = []
    for month, group in by_month.items():
        ordered = sorted(group, key=lambda r: int(r["close_version"]))
        versions = [int(r["close_version"]) for r in ordered]
        if versions != list(range(len(versions))):
            violations.append(
                {"kind": "version_sequence", "accounting_month": month, "versions": versions}
            )
        # Compared as INSTANTS, not as text. The strings come from different closes and may
        # carry different UTC offsets, and "2026-02-01T00:30:00+01:00" sorts after
        # "2026-01-31T23:45:00+00:00" as text while being earlier as an instant. An invariant
        # that sorts the same way the thing it checks sorts cannot see the bug.
        stamps = [str(r["restated_at"]) for r in ordered if r.get("restated_at") is not None]
        if [instant_of(s) for s in stamps] != sorted(instant_of(s) for s in stamps):
            violations.append(
                {
                    "kind": "restated_at_not_monotonic",
                    "accounting_month": month,
                    "restated_at": stamps,
                }
            )
    return violations


def returns_never_exceed_sales(rows: Iterable[Row]) -> list[Violation]:
    """A weak but useful sanity property: a month cannot refund more than it ever sold.

    Weak on purpose. It is true in this domain only because a return is imputed to the month
    of the sale; under the other imputation rule it would be false, which is why it doubles
    as a detector for specification mutant SPEC-01.
    """
    return [
        {
            "kind": "returns_exceed_gross",
            "accounting_month": r["accounting_month"],
            "gross_cents": r["gross_cents"],
            "returns_cents": r["returns_cents"],
        }
        for r in rows
        if r["returns_cents"] > r["gross_cents"]
    ]


ALL_INVARIANTS = (
    "scd2_well_formed",
    "conservation",
    "net_identity",
    "restatement_monotonic",
    "returns_never_exceed_sales",
)


def conservation_against_ledger(
    ledger_counts: Mapping[str, int], reference: Mapping[str, int]
) -> list[Violation]:
    """The generator's own record of what it wrote, against the reference's accounting of it.

    Three quantities, each known independently on both sides:

      * how many lines exist in the files. The generator counted them as it wrote; the
        reference counts the bytes back. A mismatch means a line was lost between writing
        and reading, which is the one failure a self-consistent SQL query cannot see.
      * how many DISTINCT event_ids there are. The generator knows because it built them;
        the reference derives it by deduplicating. A mismatch means the deduplication key is
        wrong, in either direction.
      * how many duplicate copies were written. Same two independent sources.
      * how many lines are not readable as JSON at all. The generator wrote them broken on
        purpose; the reference counts them as the lines it could not turn into a record plus
        the ones it turned into an all-NULL row. That figure was published as zero on data
        that contained them, for a whole review cycle.
      * how many carry a NUMBER THE COLUMN CANNOT HOLD. The generator emits them (a price of
        2^63, one past the largest BIGINT); the reference recounts them by asking which values
        need more than 64 bits. This one is here because it is the only fault in the set that
        erases itself: every reader in the project puts such a value in its rescue column and
        leaves the real column NULL, after which the record is indistinguishable from one whose
        producer never sent the field. It still leaves through a named door -
        ``missing_required_field``, since after the rescue the field is missing - so the four-
        way conservation identity holds without it. What it adds is that the LOSS is counted:
        the record is accounted for, and so is the value.

        There was no such count, and the gap was stated as a fact in the code. ``claims.py``
        passed ``rescued=0`` into ``conservation`` above with a comment explaining that the
        rescue door "is one this pipeline never uses" - true when it was written, and false as
        soon as any producer sent a number too wide for its column, which the deployed lane did
        on its first run. A term that cannot move is not a check; a term that quietly starts
        moving while the comment says it cannot is worse.

    This is the invariant the previous ``conservation`` call was supposed to be and was not.
    """
    violations: list[Violation] = []
    pairs = [
        ("lines_written", int(ledger_counts["events_written"]), int(reference["raw_lines"])),
        ("unique_events", int(ledger_counts["unique_events"]), int(reference["unique_events"])),
        ("duplicates", int(ledger_counts["duplicates"]), int(reference["duplicates"])),
        (
            "unparseable_lines",
            int(ledger_counts["unparseable_lines"]),
            int(reference["unparseable"]),
        ),
        (
            "values_beyond_bigint",
            int(ledger_counts["values_beyond_bigint"]),
            int(reference["beyond_bigint"]),
        ),
    ]
    for name, expected, found in pairs:
        if expected != found:
            violations.append(
                {
                    "kind": "conservation_against_ledger",
                    "quantity": name,
                    "ledger": expected,
                    "reference": found,
                    "difference": found - expected,
                }
            )
    return violations


def returns_accounted_by_reason(
    ledger_quarantine: Mapping[str, int], reference: Mapping[str, int]
) -> list[Violation]:
    """The generator's per-reason record of the returns it planted, against the reference's.

    The return-stage reasons - `return_without_order`, `return_outside_window`,
    `return_exceeds_sold_qty` - are decided in gold, by questions about the SALE, so the
    ingest-stage accounting cannot see them and for a long time nothing counted them at all:
    CONTRACT.md described a quarantine counter that did not exist, and `ledger.quarantine`,
    the generator's own by-construction record, was read by no test and no claim.

    Both sides here are independent: the generator counted as it wrote the events, the
    reference recounts by classifying them. It is the same argument as
    `conservation_against_ledger`, applied to the half of the classification that happens a
    stage later.
    """
    reasons = {
        "return_without_order",
        "return_outside_window",
        "return_exceeds_sold_qty",
    }
    violations: list[Violation] = []
    for reason in sorted(reasons):
        expected = int(ledger_quarantine.get(reason, 0))
        found = int(reference.get(reason, 0))
        if expected != found:
            violations.append(
                {
                    "kind": "returns_by_reason",
                    "reason": reason,
                    "ledger": expected,
                    "reference": found,
                    "difference": found - expected,
                }
            )
    return violations

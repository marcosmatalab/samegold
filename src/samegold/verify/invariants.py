"""Properties that must hold whatever the input, checked without any oracle.

These are the only checks in the project that do not depend on a second implementation or
on the generator's ledger, which makes them the part a reader has to trust least. Each one
returns the offending rows rather than a boolean, because "SCD2 is wrong" is not actionable
and "customer C000317 has two rows with is_current=true, versions 3 and 4" is.

Deliberately NOT here, because it is false: "reordering the arrival of events does not
change gold". With a watermark it does - an event that arrives after the watermark has
passed its event time is treated differently from the same event arriving early. The
version that IS true, and that lives in faults/permutations.py, is invariance under
permutations that preserve the watermark order.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from itertools import pairwise
from typing import Any

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
        stamps = [str(r["restated_at"]) for r in ordered if r.get("restated_at") is not None]
        if stamps != sorted(stamps):
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

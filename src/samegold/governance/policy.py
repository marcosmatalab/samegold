"""Column classification as code, and the check that gold does not leak it.

The declaration below is the contract's privacy half: which columns identify a person,
which are attributes about them, and which are plain business facts. Two things use it: the
transformation that masks the data on its way into gold, and a drift check that reads the
gold rows and refuses any column carrying a direct identifier in the clear.

Why a check and not just a masking function: the masking function is applied by someone who
remembers to apply it. The check is applied to the OUTPUT, so a new column that quietly
carries an identifier fails the build even if nobody remembered anything.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


class Classification(StrEnum):
    DIRECT_IDENTIFIER = "direct_identifier"  # identifies a person on its own
    QUASI_IDENTIFIER = "quasi_identifier"  # identifies in combination
    ATTRIBUTE = "attribute"  # about a person, not identifying
    BUSINESS = "business"  # not about a person at all


@dataclass(frozen=True, slots=True)
class ColumnPolicy:
    column: str
    classification: Classification
    treatment: str
    rationale: str


COLUMN_POLICY: tuple[ColumnPolicy, ...] = (
    ColumnPolicy(
        "customer_id",
        Classification.DIRECT_IDENTIFIER,
        "pseudonymise",
        "the join key to every other system that knows who this person is",
    ),
    ColumnPolicy(
        "country",
        Classification.QUASI_IDENTIFIER,
        "generalize",
        "harmless alone, identifying together with a segment and a purchase history",
    ),
    ColumnPolicy(
        "segment",
        Classification.ATTRIBUTE,
        "keep",
        "a commercial label, not a personal attribute; kept because the close is reported by it",
    ),
    ColumnPolicy("order_id", Classification.BUSINESS, "keep", "identifies an order, not a person"),
    ColumnPolicy("sku", Classification.BUSINESS, "keep", "a product"),
    ColumnPolicy("unit_price_cents", Classification.BUSINESS, "keep", "money"),
)

BY_COLUMN = {policy.column: policy for policy in COLUMN_POLICY}


class PolicyViolation(ValueError):
    pass


def apply_policy(row: dict[str, Any], salt: str) -> dict[str, Any]:
    """Return the row as gold is allowed to hold it."""
    from samegold.governance.anonymise import generalize_country, pseudonymise, suppress

    out: dict[str, Any] = {}
    for column, value in row.items():
        policy = BY_COLUMN.get(column)
        if policy is None or policy.treatment == "keep":
            out[column] = value
        elif policy.treatment == "pseudonymise":
            out[f"{column}_pseudonym"] = pseudonymise(str(value), salt)
        elif policy.treatment == "generalize":
            out[f"{column}_region"] = generalize_country(value)
        elif policy.treatment == "suppress":
            out[column] = suppress(value)
        else:  # pragma: no cover - the enum of treatments is closed
            raise PolicyViolation(f"unknown treatment {policy.treatment!r} for {column}")
    return out


def check_gold_exposure(
    rows: list[dict[str, Any]], pattern: str = r"(?i)\bC\d{6,}\b"
) -> list[dict[str, str]]:
    """Refuse any direct identifier that reached gold in the clear.

    Four false negatives an adversarial review found in the first version, all fixed here and
    all obvious in hindsight:

      * it read the column names from ``rows[0]`` only, so an identifier that appeared from
        the second row on was invisible;
      * it sampled the first 200 rows, so a leak at row 240 passed;
      * it skipped any column that appeared in the policy, so parking a raw customer id in
        ``sku`` or ``order_id`` passed;
      * it matched exactly ``C`` plus six digits, so ``C0000123``, ``c000123`` and
        ``CUST-C000123`` all passed.

    The check now scans every value of every column against a pattern, and knows only one
    exception: a column whose policy treatment produced it (a pseudonym or a region).
    """
    violations: list[dict[str, str]] = []
    if not rows:
        return violations
    identifier = re.compile(pattern)
    derived = {f"{p.column}_pseudonym" for p in COLUMN_POLICY} | {
        f"{p.column}_region" for p in COLUMN_POLICY
    }

    columns: set[str] = set()
    for row in rows:
        columns.update(row)

    for policy in COLUMN_POLICY:
        if policy.classification is Classification.DIRECT_IDENTIFIER and policy.column in columns:
            violations.append(
                {
                    "kind": "direct_identifier_in_gold",
                    "column": policy.column,
                    "detail": f"{policy.column} is classified {policy.classification} and its "
                    f"treatment is {policy.treatment}, but gold carries it unmasked",
                }
            )

    flagged: set[str] = {v["column"] for v in violations}
    for column in sorted(columns):
        if column in derived or column in flagged:
            continue
        for index, row in enumerate(rows):
            value = row.get(column)
            if value is None:
                continue
            if identifier.search(str(value)):
                violations.append(
                    {
                        "kind": "identifier_shaped_value_in_gold",
                        "column": column,
                        "detail": f"row {index} of column {column!r} holds {value!r}, which "
                        f"has the shape of a customer identifier",
                    }
                )
                break
    return violations

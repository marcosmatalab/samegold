"""The data contract.

This module is the single source of truth for the shape and the semantics of the data.
Everything else (generator, Spark pipeline, DuckDB reference, Databricks bundle) reads
its constants from here, so that a change to the contract cannot silently apply to one
implementation and not the other.

Why a contract module and not just schemas in the pipeline: the project's central claim is
that two implementations agree. If each implementation carried its own copy of "a return is
imputed to the month of the sale", agreement would only mean the copies were in sync.
Sharing the *rules* would be worse (it would make the two implementations one). The split
we chose: share the CONTRACT (names, types, windows, timezone), duplicate the COMPUTATION.
See docs/adr/0004-what-is-shared-between-implementations.md.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

# 1.3.0: `amount_out_of_range` joins the closed enum, and the two bounds below join the
# contract. A change to the set of quarantine reasons is a contract change; calling it a
# patch would be the kind of quiet reinterpretation this file exists to prevent.
#
# 2.0.0: the two bounds are NARROWED to numbers a retail data owner would sign. A MAJOR bump
# and not a minor one, because the direction is what matters: 1.3.0 opened a door, and a
# producer that was compliant with it saw nothing change. This version refuses records 1.3.0
# accepted - a unit price of 50 000 euros was legal yesterday and is quarantined today - and a
# version number that cannot tell "we added a door" from "we now turn people away" is the
# quiet reinterpretation again, one field further along.
CONTRACT_VERSION = "2.0.0"

# All accounting periods are computed in this timezone, never in UTC and never in the
# session timezone. A close is a legal artefact of a Spanish entity; UTC would move
# midnight sales of the 1st and the 31st into the wrong month for 2 hours a year.
ACCOUNTING_TIMEZONE = "Europe/Madrid"

# Business lateness: a customer may return an item up to 45 days after the sale.
# This is NOT a watermark. See docs/adr/0003-watermark-is-not-the-return-window.md.
RETURN_WINDOW_DAYS = 45

# Streaming lateness: how long the ingestion layer waits for out-of-order *arrival* of
# events before it may drop state. Two hours covers the retry policy of the upstream
# producers (max 3 retries with 30 min backoff) with 30 minutes of headroom.
WATERMARK_DELAY = dt.timedelta(hours=2)

# Freshness SLA for the operational returns dashboard, from event_ts to gold visibility.
FRESHNESS_SLA = dt.timedelta(minutes=15)

# Currency. Money is integer cents everywhere; there is no float in this pipeline.
# A DECIMAL(18,2) would also work, but cents as BIGINT makes the digest exact by
# construction and removes an entire class of engine-dependent rounding differences.
CURRENCY = "EUR"

# Bounds on the money arithmetic, and the reason they exist rather than being obvious.
#
# `qty * unit_price_cents` is a BIGINT multiplication, and BIGINT overflows. Three order
# lines at the maximum legal price produced a `gross_cents` DuckDB happily returned as
# 18 446 744 073 709 553 614 - a number that does not fit the column it is published in -
# while Spark, under ANSI mode, refused to produce ANY close for ANY month with an
# ARITHMETIC_OVERFLOW. Every value involved was a legal BIGINT that both readers accept, and
# no rule bounded either factor, so this was the last record shape in the pipeline with no
# door: not quarantined, not counted, just the end of the close.
#
# The bounds are BUSINESS bounds: the largest line a retail data owner would put a name to.
# Ten thousand units of one SKU on one line is a pallet order; ten thousand euros a unit is
# the most expensive single item a shop like this sells. Both are generous, and both are
# numbers somebody could defend in a review, which is what a contract term has to be.
#
# The first version of them was not, in two ways that turned out to be the same way.
#
# It was ten million units and a hundred million euros a unit, chosen to be "deliberately far
# above anything real", and the comment defending them did the arithmetic wrong: it claimed a
# close would need a hundred billion maximum-value lines before the SUM overflowed. The true
# figure was NINETY-TWO. A line was 10^7 * 10^10 = 10^17 cents, and (2^63 - 1) // 10^17 is 92,
# so ninety-three such lines re-created the exact incident the bounds were introduced to
# prevent. The bounds moved the threshold from three lines to ninety-three and the comment
# claimed nine orders of magnitude of headroom that were not there.
#
# And a bound has to be MEASURED against, which means a record has to sit exactly on it (see
# boundary case 11 in generator/events.py), which means the bound's size is the size of a
# fixture that lands in a published month. At a hundred million euros a unit that single line
# was a hundred and sixty-eight times the entire simulated business of the month it fell in.
# It moved the restatement figure on the front page of the README from 6.48% to 3.38% and
# moved which month was worst - not because the pipeline changed, but because scaffolding
# entered the denominator. A bound nobody would sign is also a bound nothing can afford to
# test, and the two failures are one failure: the numbers were picked to be safely enormous
# instead of being picked and then checked.
#
# The margin now, with the arithmetic done out loud rather than asserted:
#     largest legal line = 10 000 * 1 000 000                   =            10 000 000 000
#     BIGINT maximum     = 2^63 - 1                             = 9 223 372 036 854 775 807
#     maximum-value lines a month can hold before the SUM wraps =               922 337 203
# Nine hundred million of them, against a simulation that emits a few thousand a month and a
# real shop that would emit fewer. `tests/fast/test_contract_documents.py` recomputes that
# division rather than trusting this comment, which is the lesson of the first version.
MAX_LINE_QUANTITY = 10_000
MAX_UNIT_PRICE_CENTS = 1_000_000


class EventType(StrEnum):
    ORDER_PLACED = "order_placed"
    ORDER_LINE_AMENDED = "order_line_amended"
    RETURN_REGISTERED = "return_registered"
    CUSTOMER_UPSERTED = "customer_upserted"


class QuarantineReason(StrEnum):
    """Why a record did not reach silver. Closed enum: a record is accepted, quarantined
    with one of these reasons, or rescued. There is no fourth outcome, and the conservation
    invariant in verify/invariants.py depends on that being true."""

    UNPARSEABLE_JSON = "unparseable_json"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    NON_POSITIVE_QUANTITY = "non_positive_quantity"
    NEGATIVE_PRICE = "negative_price"
    RETURN_WITHOUT_ORDER = "return_without_order"
    RETURN_OUTSIDE_WINDOW = "return_outside_window"
    RETURN_EXCEEDS_SOLD_QTY = "return_exceeds_sold_qty"
    # There is deliberately no DUPLICATE_EVENT_ID here. A duplicate is not quarantined, it is
    # deduplicated, and the conservation invariant counts it through its own door
    # (ingested = accepted + quarantined + rescued + deduplicated). Giving it a quarantine
    # reason as well would double-count it, and a test in tests/fast/test_contract_documents.py
    # refuses any reason that no implementation can actually emit.
    UNKNOWN_CURRENCY = "unknown_currency"
    # A quantity or a price outside the contract's bounds. Not a type error - every value
    # that triggered it was a legal BIGINT - but an arithmetic overflow further down, which
    # is the only record shape that used to end a close instead of leaving through a door.
    AMOUNT_OUT_OF_RANGE = "amount_out_of_range"


@dataclass(frozen=True, slots=True)
class Event:
    """One raw event as it lands in bronze.

    ``event_id`` is the business idempotency key: the producer guarantees it is stable
    across retries, which is what lets us deduplicate content that arrives twice under
    two different file paths. ``arrival_ts`` is assigned by the ingestion layer, never
    by the producer, and is the only clock-like column allowed downstream (and it is
    excluded from every digest projection - see verify/digest.py).
    """

    event_id: str
    event_type: EventType
    event_ts: dt.datetime
    arrival_ts: dt.datetime
    payload: dict[str, Any] = field(default_factory=dict)

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": str(self.event_type),
            "event_ts": self.event_ts.isoformat(),
            "arrival_ts": self.arrival_ts.isoformat(),
            **self.payload,
        }


# Column sets that must never take part in a digest, because their value depends on the
# wall clock, on the physical layout, or on the engine. Enforced by verify/digest.py and
# by tests/fast/test_digest.py, not by convention.
NON_DETERMINISTIC_COLUMNS: frozenset[str] = frozenset(
    {
        "arrival_ts",
        "ingest_ts",
        "processed_at",
        "restated_at",
        "_metadata",
        "_rescued_data",
        "_commit_version",
        "_commit_timestamp",
        "_change_type",
        "_file_path",
        "run_id",
        "pipeline_id",
    }
)

# Gold tables, with the total order that makes their digest well defined.
GOLD_KEYS: dict[str, tuple[str, ...]] = {
    "dim_customer_scd2": ("customer_id", "valid_from"),
    "fct_order_line": ("order_id", "sku"),
    "revenue_by_month": ("accounting_month", "close_version"),
}

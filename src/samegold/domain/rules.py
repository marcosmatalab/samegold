"""The business rules, as pure functions over plain Python values.

Kept free of Spark, DuckDB, pandas and Arrow on purpose: these are the rules a finance
person would recognise, and they are the part of the system a specification mutant attacks
(mutation/operators.py). Because they are pure, the property tests over them run in
milliseconds and the whole fast lane needs no JVM.

Trade-off accepted: this module is *shared* by the Spark pipeline and by the analytic
oracle, so a bug here is invisible to their comparison. It is not shared with the DuckDB
reference implementation, which re-derives the same rules in SQL. That is the only reason
the DuckDB witness has any independent value at all, and it is why the witness matrix in
mutation/witness_matrix.py reports per-witness kill rates instead of a single number.
"""

from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

from samegold.domain.contract import ACCOUNTING_TIMEZONE, RETURN_WINDOW_DAYS

_TZ = ZoneInfo(ACCOUNTING_TIMEZONE)


def accounting_month(ts: dt.datetime) -> str:
    """The accounting period a timestamp belongs to, as 'YYYY-MM'.

    Converts to the accounting timezone first. A naive timestamp is treated as UTC,
    which is what the producers emit; treating it as local time would silently shift
    two hours of sales per day into the previous period during summer time.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.UTC)
    local = ts.astimezone(_TZ)
    return f"{local.year:04d}-{local.month:02d}"


def is_return_within_window(sale_ts: dt.datetime, return_ts: dt.datetime) -> bool:
    """True when a return is inside the 45-day commercial window.

    Half-open on the right: a return exactly 45 days later is accepted, 45 days plus one
    microsecond is not. The boundary is asserted in tests/fast/test_rules.py because it is
    the single most common off-by-one in this domain and it is worth one euro per unit.
    """
    if sale_ts.tzinfo is None:
        sale_ts = sale_ts.replace(tzinfo=dt.UTC)
    if return_ts.tzinfo is None:
        return_ts = return_ts.replace(tzinfo=dt.UTC)
    if return_ts < sale_ts:
        return False
    return (return_ts - sale_ts) <= dt.timedelta(days=RETURN_WINDOW_DAYS)


def line_amount_cents(qty: int, unit_price_cents: int) -> int:
    """Gross amount of an order line, in cents. Integer arithmetic, no rounding."""
    return qty * unit_price_cents


def imputation_month(sale_ts: dt.datetime, return_ts: dt.datetime | None = None) -> str:
    """The period a movement is imputed to.

    THE rule of this project: a return is imputed to the month of the ORIGINAL SALE, not
    to the month in which the return happened. That is what makes a closed month reopen,
    and therefore what makes the bitemporal model in gold necessary. Changing the argument
    used here is specification mutant SPEC-01.
    """
    return accounting_month(sale_ts)


def net_cents(gross_cents: int, returns_cents: int) -> int:
    """Net revenue of a period. Returns are subtracted, never clamped at zero:
    a month with more returns than sales is a real and reportable outcome."""
    return gross_cents - returns_cents

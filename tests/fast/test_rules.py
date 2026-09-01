"""Boundaries of the business rules. Every assertion here is worth money in cents."""

from __future__ import annotations

import datetime as dt

import pytest

from samegold.domain.rules import (
    accounting_month,
    imputation_month,
    is_return_within_window,
    line_amount_cents,
    net_cents,
)


def test_return_exactly_on_the_45th_day_is_inside_the_window() -> None:
    sale = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC)
    assert is_return_within_window(sale, sale + dt.timedelta(days=45)) is True


def test_return_one_microsecond_past_the_window_is_outside() -> None:
    sale = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC)
    assert is_return_within_window(sale, sale + dt.timedelta(days=45, microseconds=1)) is False


def test_return_before_the_sale_is_never_valid() -> None:
    sale = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC)
    assert is_return_within_window(sale, sale - dt.timedelta(seconds=1)) is False


def test_return_at_the_instant_of_sale_is_valid() -> None:
    sale = dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC)
    assert is_return_within_window(sale, sale) is True


@pytest.mark.parametrize(
    ("utc", "expected"),
    [
        # 23:30 UTC on the last day of the month is already the 1st in Madrid (UTC+1/+2).
        (dt.datetime(2026, 1, 31, 23, 30, tzinfo=dt.UTC), "2026-02"),
        (dt.datetime(2026, 2, 1, 0, 30, tzinfo=dt.UTC), "2026-02"),
        # The night the clocks go forward in Spain (29 March 2026, 02:00 -> 03:00).
        (dt.datetime(2026, 3, 31, 22, 30, tzinfo=dt.UTC), "2026-04"),
        (dt.datetime(2026, 6, 30, 21, 59, tzinfo=dt.UTC), "2026-06"),
        (dt.datetime(2026, 6, 30, 22, 1, tzinfo=dt.UTC), "2026-07"),
    ],
)
def test_accounting_month_uses_the_accounting_timezone(utc: dt.datetime, expected: str) -> None:
    assert accounting_month(utc) == expected


def test_naive_timestamps_are_treated_as_utc() -> None:
    naive = dt.datetime(2026, 1, 31, 23, 30)
    assert accounting_month(naive) == accounting_month(naive.replace(tzinfo=dt.UTC))


def test_a_return_is_imputed_to_the_month_of_the_sale() -> None:
    sale = dt.datetime(2026, 1, 20, 10, 0, tzinfo=dt.UTC)
    ret = dt.datetime(2026, 2, 25, 10, 0, tzinfo=dt.UTC)
    assert imputation_month(sale, ret) == "2026-01"


def test_money_is_integer_cents() -> None:
    assert line_amount_cents(3, 1999) == 5997
    assert net_cents(5997, 1999) == 3998
    # A month can go negative. Clamping would hide a real and reportable outcome.
    assert net_cents(100, 500) == -400

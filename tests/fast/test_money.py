"""How this project writes money. A module of two functions, and it needed tests.

`domain/money.py` was created to consolidate three copies of a cents-to-euros formatter, two
of which had been wrong: one divided by 100 as a float in a codebase whose contract says there
is no float in the pipeline, and one printed en-US separators for a figure the evidence and
the post-mortem write in es-ES. The consolidation then dropped the minus sign from the close
report, so a restatement of -321,45 EUR rendered as a positive 321,45 in red, and the module
shipped with no tests at all.
"""

from __future__ import annotations

import pytest

from samegold.domain.money import euros, signed_euros


@pytest.mark.parametrize(
    "cents,expected",
    [
        (0, "0,00"),
        (1, "0,01"),
        (99, "0,99"),
        (100, "1,00"),
        (67269342, "672 693,42"),
        (-67269342, "672 693,42"),  # unsigned by contract; the caller adds the sign
        (1234567890123456789, "12 345 678 901 234 567,89"),
    ],
)
def test_euros_is_exact_and_spanish(cents: int, expected: str) -> None:
    assert euros(cents) == expected


def test_the_largest_figure_is_exact() -> None:
    """Integer arithmetic, not float division.

    `f"{cents / 100:,.2f}"` on this value prints ...568,00 for a number ending 567,89: the
    figure is beyond a float's 53 bits of mantissa. Money is cents by contract precisely so
    that this cannot happen, and the first version of this formatter did it anyway.
    """
    assert euros(1234567890123456789).endswith("567,89")


@pytest.mark.parametrize(
    "cents,expected",
    [(0, "+0,00"), (-1, "-0,01"), (-3214500, "-32 145,00"), (3214500, "+32 145,00")],
)
def test_signed_euros_keeps_the_sign(cents: int, expected: str) -> None:
    assert signed_euros(cents) == expected


def test_the_separator_is_a_plain_space() -> None:
    """A non-breaking space renders identically and would make every comparison against a
    document compare two things a reader cannot tell apart."""
    assert "\u00a0" not in euros(67269342)
    assert " " in euros(67269342)


def test_the_close_report_shows_a_negative_restatement_as_negative() -> None:
    """The regression this file exists for."""
    import datetime as dt

    from samegold.serve.report import render_report

    versions = [
        {
            "accounting_month": "2026-01",
            "close_version": 0,
            "gross_cents": 100000,
            "returns_cents": 0,
            "net_cents": 100000,
            "restatement_reason": "first close",
            "restated_at": "2026-02-05T22:59:59+00:00",
        },
        {
            "accounting_month": "2026-01",
            "close_version": 1,
            "gross_cents": 100000,
            "returns_cents": 32145,
            "net_cents": 67855,
            "restatement_reason": "late arrivals after close",
            "restated_at": "2026-03-05T22:59:59+00:00",
        },
    ]
    page = render_report(versions, dt.datetime(2026, 3, 6, 9, tzinfo=dt.UTC))
    assert "-321,45" in page, "a fall of 321,45 EUR is shown without its sign"

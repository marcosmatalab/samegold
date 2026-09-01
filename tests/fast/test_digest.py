"""The digest refuses to mean nothing."""

from __future__ import annotations

import pytest

from samegold.verify.digest import (
    REVENUE_PROJECTION,
    CanonicalDigest,
    Projection,
    ProjectionError,
)


def _row(month: str, net: int) -> dict[str, object]:
    """One row of the real gold projection, which is the point: the projection declares the
    columns gold actually has, not a convenient subset the tests can produce."""
    return {
        "accounting_month": month,
        "close_version": 0,
        "gross_cents": net + 3,
        "returns_cents": 3,
        "net_cents": net,
        "line_count": 2,
        "return_count": 1,
        "returns_rejected_count": 0,
        "restated_at": "2026-02-05T22:59:59+00:00",
        "restatement_reason": "first close",
    }


ROWS = [_row("2026-01", 7), _row("2026-02", 20)]


def test_projection_refuses_non_deterministic_columns() -> None:
    with pytest.raises(ProjectionError, match="non-deterministic"):
        Projection(table="t", columns=("id", "restated_at"), order_by=("id",))


def test_projection_allows_a_declared_exception() -> None:
    projection = Projection(
        table="t",
        columns=("id", "restated_at"),
        order_by=("id",),
        allow_non_deterministic=("restated_at",),
    )
    assert "restated_at" in projection.spec["allow_non_deterministic"]


def test_projection_requires_an_explicit_total_order() -> None:
    with pytest.raises(ProjectionError, match="total order"):
        Projection(table="t", columns=("id",), order_by=())


def test_digest_cannot_be_constructed_from_a_string() -> None:
    with pytest.raises(ProjectionError, match="cannot be constructed directly"):
        CanonicalDigest("deadbeef", REVENUE_PROJECTION, 1)


def test_digest_is_independent_of_row_order() -> None:
    a = CanonicalDigest.of(ROWS, REVENUE_PROJECTION)
    b = CanonicalDigest.of(list(reversed(ROWS)), REVENUE_PROJECTION)
    assert a.agrees_with(b)


def test_digest_changes_when_a_cent_changes() -> None:
    a = CanonicalDigest.of(ROWS, REVENUE_PROJECTION)
    moved = [dict(ROWS[0], net_cents=8), ROWS[1]]
    assert not a.agrees_with(CanonicalDigest.of(moved, REVENUE_PROJECTION))


def test_digest_refuses_a_key_that_is_not_a_total_order() -> None:
    duplicated = [ROWS[0], dict(ROWS[0], gross_cents=99)]
    with pytest.raises(ProjectionError, match="not a total order"):
        CanonicalDigest.of(duplicated, REVENUE_PROJECTION)


def test_comparing_across_projections_raises_rather_than_returning_false() -> None:
    other = Projection(
        table="revenue_by_month",
        columns=("accounting_month", "net_cents"),
        order_by=("accounting_month",),
    )
    a = CanonicalDigest.of(ROWS, REVENUE_PROJECTION)
    b = CanonicalDigest.of(ROWS, other)
    with pytest.raises(ProjectionError, match="different projections"):
        a.agrees_with(b)


def test_missing_column_is_an_error_not_a_null() -> None:
    with pytest.raises(ProjectionError, match="missing projected columns"):
        CanonicalDigest.of([{"accounting_month": "2026-01"}], REVENUE_PROJECTION)

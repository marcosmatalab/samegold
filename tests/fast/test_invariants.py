"""The invariants catch what they are for, on data built to break them."""

from __future__ import annotations

from samegold.verify.invariants import (
    conservation,
    net_identity,
    restatement_monotonic,
    returns_never_exceed_sales,
    scd2_well_formed,
)

GOOD_SCD2 = [
    {
        "customer_id": "C1",
        "valid_from": "2026-01-01",
        "valid_to": "2026-02-01",
        "segment": "retail",
        "country": "ES",
        "is_current": False,
    },
    {
        "customer_id": "C1",
        "valid_from": "2026-02-01",
        "valid_to": None,
        "segment": "vip",
        "country": "ES",
        "is_current": True,
    },
]


def test_a_well_formed_dimension_has_no_violations() -> None:
    assert scd2_well_formed(GOOD_SCD2) == []


def test_two_open_rows_are_caught() -> None:
    broken = [dict(GOOD_SCD2[0], is_current=True), GOOD_SCD2[1]]
    kinds = {v["kind"] for v in scd2_well_formed(broken)}
    assert "open_rows" in kinds


def test_a_gap_between_versions_is_caught() -> None:
    broken = [dict(GOOD_SCD2[0], valid_to="2026-01-15"), GOOD_SCD2[1]]
    assert any(v["kind"] == "gap_or_overlap" for v in scd2_well_formed(broken))


def test_an_overlap_is_caught() -> None:
    broken = [dict(GOOD_SCD2[0], valid_to="2026-03-01"), GOOD_SCD2[1]]
    assert any(v["kind"] == "gap_or_overlap" for v in scd2_well_formed(broken))


def test_conservation_reports_what_went_missing() -> None:
    violations = conservation(ingested=100, accepted=80, quarantined=10, rescued=0, deduplicated=5)
    assert violations and violations[0]["missing"] == 5


def test_net_identity_catches_a_wrong_subtraction() -> None:
    rows = [
        {
            "accounting_month": "2026-01",
            "close_version": 0,
            "gross_cents": 10,
            "returns_cents": 3,
            "net_cents": 10,
        }
    ]
    assert net_identity(rows)


def test_restatement_versions_must_be_dense_and_ordered() -> None:
    rows = [
        {"accounting_month": "2026-01", "close_version": 0, "restated_at": "2026-02-05"},
        {"accounting_month": "2026-01", "close_version": 2, "restated_at": "2026-03-05"},
    ]
    assert any(v["kind"] == "version_sequence" for v in restatement_monotonic(rows))


def test_restated_at_going_backwards_is_caught() -> None:
    rows = [
        {"accounting_month": "2026-01", "close_version": 0, "restated_at": "2026-03-05"},
        {"accounting_month": "2026-01", "close_version": 1, "restated_at": "2026-02-05"},
    ]
    assert any(v["kind"] == "restated_at_not_monotonic" for v in restatement_monotonic(rows))


def test_a_month_cannot_refund_more_than_it_sold() -> None:
    rows = [{"accounting_month": "2026-01", "gross_cents": 100, "returns_cents": 101}]
    assert returns_never_exceed_sales(rows)

"""The Type 2 dimension: a function of the SET of source versions.

The first version of this module folded a batch into a materialised dimension, and an
adversarial review showed the result depended on how the input had been cut into batches: the
same three versions, applied one at a time or all at once, produced different dimensions, and
`scd2_well_formed` passed on both. The property tests at the bottom are the ones that would
have caught it, and they are the reason the signature changed.
"""

from __future__ import annotations

import itertools
from itertools import pairwise
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from samegold.domain.bitemporal import scd2_apply, scd2_from_versions
from samegold.verify.invariants import scd2_well_formed


def version(
    valid_from: str, segment: str = "retail", country: str = "ES", event_id: str = "e"
) -> dict[str, Any]:
    return {
        "customer_id": "C1",
        "valid_from": valid_from,
        "segment": segment,
        "country": country,
        "event_id": event_id,
    }


def test_the_first_version_opens_the_dimension() -> None:
    assert scd2_from_versions([version("2026-01-01")]) == [
        {
            "customer_id": "C1",
            "valid_from": "2026-01-01",
            "valid_to": None,
            "segment": "retail",
            "country": "ES",
            "is_current": True,
        }
    ]


def test_a_later_version_closes_the_open_row() -> None:
    rows = scd2_from_versions([version("2026-01-01"), version("2026-02-01", segment="vip")])
    assert [r["valid_to"] for r in rows] == ["2026-02-01", None]
    assert [r["is_current"] for r in rows] == [False, True]
    assert scd2_well_formed(rows) == []


def test_two_versions_in_one_batch_keep_the_middle_period() -> None:
    rows = scd2_apply(
        [version("2026-01-01")],
        [
            version("2026-02-01", segment="vip", event_id="e2"),
            version("2026-03-01", segment="pro", event_id="e3"),
        ],
    )
    assert [r["valid_from"] for r in rows] == ["2026-01-01", "2026-02-01", "2026-03-01"]
    assert [r["segment"] for r in rows] == ["retail", "vip", "pro"]
    assert scd2_well_formed(rows) == []


def test_a_version_at_the_same_instant_is_the_same_fact_arriving_twice() -> None:
    rows = scd2_from_versions(
        [
            version("2026-01-01", segment="retail", event_id="e1"),
            version("2026-01-01", segment="vip", event_id="e2"),
        ]
    )
    assert len(rows) == 1
    assert rows[0]["segment"] == "vip"  # last by event_id wins, as the contract says
    assert rows[0]["is_current"] is True


def test_a_late_correction_splits_the_interval_it_lands_in() -> None:
    rows = scd2_from_versions(
        [
            version("2026-01-01"),
            version("2026-03-01", segment="pro", event_id="e3"),
            version("2026-02-01", segment="vip", event_id="e2"),
        ]
    )
    assert [r["valid_from"] for r in rows] == ["2026-01-01", "2026-02-01", "2026-03-01"]
    assert [r["valid_to"] for r in rows] == ["2026-02-01", "2026-03-01", None]
    assert scd2_well_formed(rows) == []


def test_a_version_that_changes_nothing_is_not_recorded() -> None:
    rows = scd2_from_versions(
        [version("2026-01-01", segment="vip"), version("2026-02-01", segment="vip", event_id="e2")]
    )
    assert len(rows) == 1


def test_no_two_adjacent_rows_carry_the_same_attributes() -> None:
    """A restatement where nothing changed is not a restatement.

    The previous implementation could produce two contiguous rows with identical attributes
    after a late correction, which is a row that says "on this date, nothing happened".
    """
    rows = scd2_from_versions(
        [
            version("2026-01-01", segment="A", event_id="e1"),
            version("2026-02-01", segment="B", event_id="e2"),
            version("2026-01-15", segment="B", event_id="e3"),
        ]
    )
    for left, right in pairwise(rows):
        assert (left["segment"], left["country"]) != (right["segment"], right["country"])


def test_the_result_does_not_depend_on_the_order_the_versions_arrive_in() -> None:
    """The bug that changed this module's signature, as a test over every permutation."""
    versions = [
        version("2026-01-01", segment="A", country="A", event_id="e1"),
        version("2026-02-01", segment="A", country="A", event_id="e2"),
        version("2026-01-15", segment="A", country="B", event_id="e3"),
    ]
    results = {
        tuple(tuple(sorted(row.items(), key=str)) for row in scd2_from_versions(list(order)))
        for order in itertools.permutations(versions)
    }
    assert len(results) == 1


def test_the_result_does_not_depend_on_how_the_input_is_batched() -> None:
    versions = [
        version("2026-01-01", segment="A", country="A", event_id="e1"),
        version("2026-02-01", segment="A", country="A", event_id="e2"),
        version("2026-01-15", segment="A", country="B", event_id="e3"),
    ]
    all_at_once = scd2_from_versions(versions)
    accumulated: list[dict[str, Any]] = []
    incremental: list[dict[str, Any]] = []
    for one in versions:
        accumulated.append(one)
        incremental = scd2_from_versions(accumulated)
    assert all_at_once == incremental


_VERSIONS = st.lists(
    st.tuples(
        st.integers(min_value=1, max_value=28),
        st.sampled_from(["retail", "vip", "pro"]),
        st.sampled_from(["ES", "PT", "FR"]),
    ),
    min_size=1,
    max_size=12,
)


def _as_versions(raw: list[tuple[int, str, str]]) -> list[dict[str, Any]]:
    return [
        {
            "customer_id": "C1",
            "valid_from": f"2026-01-{day:02d}",
            "segment": segment,
            "country": country,
            "event_id": f"e{index:03d}",
        }
        for index, (day, segment, country) in enumerate(raw)
    ]


@settings(max_examples=300, deadline=None)
@given(_VERSIONS)
def test_the_dimension_is_always_well_formed(raw: list[tuple[int, str, str]]) -> None:
    rows = scd2_from_versions(_as_versions(raw))
    assert scd2_well_formed(rows) == []


@settings(max_examples=300, deadline=None)
@given(_VERSIONS, st.integers(min_value=1, max_value=5))
def test_any_batching_gives_the_same_dimension(
    raw: list[tuple[int, str, str]], batch_size: int
) -> None:
    versions = _as_versions(raw)
    once = scd2_from_versions(versions)
    accumulated: list[dict[str, Any]] = []
    stepwise: list[dict[str, Any]] = []
    for start in range(0, len(versions), batch_size):
        accumulated.extend(versions[start : start + batch_size])
        stepwise = scd2_from_versions(accumulated)
    assert once == stepwise


@settings(max_examples=200, deadline=None)
@given(_VERSIONS)
def test_no_two_adjacent_rows_are_identical(raw: list[tuple[int, str, str]]) -> None:
    rows = scd2_from_versions(_as_versions(raw))
    for left, right in pairwise(rows):
        assert (left["segment"], left["country"]) != (right["segment"], right["country"])

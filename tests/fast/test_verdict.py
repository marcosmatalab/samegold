"""No number without its experiment."""

from __future__ import annotations

import math

import pytest

from samegold.verify.stats import cohen_kappa, rule_of_three_upper, wilson_interval
from samegold.verify.verdict import Rate, RunSet


def _runset(n: int = 2) -> RunSet:
    return RunSet(
        n=n,
        seeds=tuple(range(n)),
        commit_sha="a" * 40,
        seed_source="commit",
        profile="fast",
        started_at="2026-09-01T00:00:00+00:00",
        duration_s=1.0,
        runtime="oss-local",
    )


def test_a_runset_must_name_one_seed_per_run() -> None:
    with pytest.raises(ValueError, match="every run must name the seed"):
        RunSet(
            n=3,
            seeds=(1, 2),
            commit_sha="a" * 40,
            seed_source="commit",
            seed_purpose="witness",
            profile="fast",
            started_at="x",
            duration_s=0.0,
            runtime="oss-local",
        )


def test_a_rate_cannot_be_built_from_a_float() -> None:
    with pytest.raises(TypeError):
        Rate(0.93)  # type: ignore[call-arg]


def test_a_rate_over_zero_trials_is_not_a_rate() -> None:
    with pytest.raises(ValueError, match="not a rate"):
        Rate(0, 0)


def test_rule_of_three_is_only_offered_when_nothing_was_observed() -> None:
    assert Rate(140, 140).upper_bound_if_zero is None
    zero = Rate(0, 140)
    assert zero.upper_bound_if_zero == pytest.approx(rule_of_three_upper(140))
    assert zero.upper_bound_if_zero == pytest.approx(0.0214, abs=1e-4)


def test_wilson_stays_inside_the_unit_interval_at_the_edges() -> None:
    low, high = wilson_interval(15, 15)
    assert 0.0 <= low <= high <= 1.0
    assert high == 1.0
    assert low == pytest.approx(0.796, abs=1e-3)


def test_rendered_rate_carries_the_interval() -> None:
    assert "95% CI" in Rate(13, 15).render()


def test_kappa_is_one_for_perfect_agreement_and_zero_for_chance() -> None:
    assert cohen_kappa(both=10, only_a=0, only_b=0, neither=10) == pytest.approx(1.0)
    assert cohen_kappa(both=25, only_a=25, only_b=25, neither=25) == pytest.approx(0.0)


def test_kappa_is_undefined_when_there_is_nothing_to_agree_about() -> None:
    """Two witnesses that both killed everything, or both killed nothing, have no variation.

    The first version returned 1.0 there, which reported a pair that had told us nothing as
    perfect agreement - and the witness matrix uses exactly that number to warn about "one
    witness wearing two hats".
    """
    assert math.isnan(cohen_kappa(both=0, only_a=0, only_b=0, neither=10))
    assert math.isnan(cohen_kappa(both=10, only_a=0, only_b=0, neither=0))

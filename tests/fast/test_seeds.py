"""Seeds come from the commit, and an override says so."""

from __future__ import annotations

import pytest

from samegold.generator.seeds import seed_for, seed_source, seeds_from_commit


def test_seeds_are_a_function_of_the_commit() -> None:
    sha = "b" * 40
    assert seeds_from_commit(3, sha=sha) == seeds_from_commit(3, sha=sha)
    assert seeds_from_commit(3, sha=sha) != seeds_from_commit(3, sha="c" * 40)


def test_purpose_separates_the_streams() -> None:
    sha = "d" * 40
    assert seed_for(sha, 0, "generator") != seed_for(sha, 0, "faults")


def test_an_override_is_marked_as_such(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SAMEGOLD_SEED_OVERRIDE", "hello")
    assert seed_source() == "override"
    assert seeds_from_commit(2) == seeds_from_commit(2)
    monkeypatch.delenv("SAMEGOLD_SEED_OVERRIDE")
    assert seed_source() == "commit"


def test_seeds_fit_in_64_bits() -> None:
    assert all(0 <= s < 2**64 for s in seeds_from_commit(5, sha="e" * 40))

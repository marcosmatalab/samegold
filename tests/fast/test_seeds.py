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


def test_a_runs_own_output_does_not_make_the_tree_look_uncommitted() -> None:
    """`tree_dirty` was true on nine of the ten published records, for no reason a reader
    could guess.

    `samegold evidence` appends a record per claim as it runs. So the first claim of a sweep
    saw a clean tree, and every claim after it saw `evidence/history.jsonl` modified - by the
    sweep itself, seconds earlier - and recorded "on an uncommitted tree". The words say the
    code that ran was in no commit. What had actually happened is that the run had written down
    its own answer.

    A caveat that is always on carries no information, and this one is the field the evidence
    policy leans on hardest: the documents quote the head of the chain and name the commit that
    produced it, and the honest half of that sentence is saying when the commit is NOT what ran.

    The output is excluded. Everything else still counts, and the untracked case in particular:
    an untracked module is the shape of "code that is in no commit" that a repository under
    review actually has, and one moved a published test count by five.
    """
    from samegold.generator.seeds import _code_changes

    assert _code_changes(" M evidence/history.jsonl\n M evidence/runs/SG-07.json") == []
    assert _code_changes(" M evidence/history.jsonl\n M src/samegold/cli.py") == [
        "src/samegold/cli.py"
    ]
    # Untracked files still count, which is the case `git stash create` used to miss.
    assert _code_changes("?? tests/fast/test_something_new.py") == [
        "tests/fast/test_something_new.py"
    ]
    # And a rename is reported by its destination, not by the arrow.
    assert _code_changes("R  src/a.py -> src/b.py") == ["src/b.py"]
    # A directory that merely starts with the same letters is not the evidence directory.
    assert _code_changes(" M evidence_notes.md") == ["evidence_notes.md"]
    assert _code_changes("") == []

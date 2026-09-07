"""Seeds come from the commit, and an override says so."""

from __future__ import annotations

import subprocess
from pathlib import Path

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
    # THE SHAPE THAT ACTUALLY ARRIVES. `current_tree` reads
    # `subprocess.run(...).stdout.strip()`, so the first line has lost its leading space and a
    # parser that slices at a fixed offset eats the first character of the path. The version of
    # this helper that sliced passed the assertion above - written by hand, with both spaces -
    # and published nine records claiming an uncommitted tree.
    stripped = " M evidence/history.jsonl\n M evidence/runs/SG-00.json".strip()
    assert _code_changes(stripped) == []
    assert _code_changes("M evidence/history.jsonl\n M src/samegold/cli.py") == [
        "src/samegold/cli.py"
    ]
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


# ------------------------------------------------ a tool's scratch space is not a tracked file
#
# `.coverage` was committed on 7 September 2026, by the commit that added the coverage gate, and
# `make fast` rewrites it on every run. Two costs, both measured rather than imagined:
#
#   * `git status --porcelain` reports " M .coverage" after the first command in the README.
#     `_code_changes` above excludes only `evidence/`, so `current_tree()` returned dirty and
#     every evidence record written after `make fast` published `tree_dirty: true`. That is the
#     condition the long comment in `generator/seeds.py` describes as having emptied that field
#     of meaning once already - nine records out of ten - recreated by a coverage gate;
#   * in a second clone it aborted `git checkout main` with "Your local changes would be
#     overwritten by checkout". A tool's scratch space that blocks a branch change is a cost
#     paid by whoever clones this, not by the author who committed it.
#
# The artifact names are DERIVED, not typed: coverage is asked where it writes, from this
# repository's own configuration, and pytest is asked for its cache directory. Only the tool
# list is written down, and a tool that starts writing somewhere new is caught by the same
# assertion because the path comes from the tool.


def _tracked(repo: Path, path: str) -> list[str]:
    """What git tracks at or under `path`. Empty for a file it has never been told about."""
    out = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--", path],
        capture_output=True,
        text=True,
        check=False,
    )
    return [line for line in out.stdout.splitlines() if line.strip()]


def test_nothing_the_fast_lane_writes_is_tracked() -> None:
    """The fast lane's own output must not be part of the repository.

    Asked of the tools rather than listed by hand. `coverage.Coverage()` reads this repository's
    `pyproject.toml` and reports the data file it will write; pytest reports its cache
    directory. A tracked file among them is a file the next `make fast` will modify, and the
    two costs of that are in the comment above.
    """
    repo = Path(__file__).resolve().parents[2]

    import coverage

    # The data file coverage will actually use here, including any `data_file` this repository
    # sets in `[tool.coverage.run]`. Typing ".coverage" would miss a configuration change.
    data_file = Path(coverage.Coverage(config_file=str(repo / "pyproject.toml")).config.data_file)
    written = {
        data_file.name: "coverage's data file, from coverage's own configuration",
        f"{data_file.name}.*": "coverage's per-process data files under -p/--parallel",
        ".pytest_cache": "pytest's cache directory",
        ".hypothesis": "hypothesis's example database, written by the property tests",
    }

    offenders = {
        name: (why, found) for name, why in written.items() if (found := _tracked(repo, name))
    }
    assert not offenders, (
        "the fast lane writes these and git tracks them, so every run of the lane dirties the "
        "tree and can block a checkout in somebody else's clone:\n"
        + "\n".join(f"  {name} ({why}): {found}" for name, (why, found) in offenders.items())
        + "\n\nRemove them with `git rm --cached` and ignore them; they are output, not source."
    )

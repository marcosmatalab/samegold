"""The gate that recomputes, and the attack it was written for.

Section 1.3 of the September audit: four defences held and the fifth attack walked through
them. Appending one well-formed record - real seeds, real commit, fresh timestamp, chained
correctly, hashed with this repository's own hasher - published ``SG-03 | PASS | 999/999`` on
the front page with ``samegold check`` at exit 0 and the whole fast lane green. Every check in
the repository asked whether the record was well formed. None asked whether the number was
true.

The tests below are that attack, plus the cases that decide whether the new gate is a gate:
what it does when it cannot recompute (say so, never pass silently), and what it does when the
code under a record has moved (say so, never raise a false alarm).
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import pytest

from samegold.evidence.registry import CLAIM_TITLES
from samegold.evidence.reproduce import (
    RATE_RULES,
    Reproduction,
    arithmetic_mismatches,
    code_moved_since,
    rate_against_artifacts,
    reproduce_latest,
)
from samegold.generator.seeds import seed_for, seeds_from_commit

REPO = Path(__file__).resolve().parents[2]
SHA = "a" * 40
HEAD = SHA


def _record(
    claim_id: str = "SG-03",
    successes: int = 67,
    trials: int = 67,
    artifacts: dict[str, Any] | None = None,
    purpose: str = "mutation",
    sha: str = SHA,
    n: int = 1,
    profile: str = "ci",
) -> dict[str, Any]:
    return {
        "claim_id": claim_id,
        "title": CLAIM_TITLES.get(claim_id, ""),
        "verdict": {
            "outcome": "pass",
            "rate": {"successes": successes, "trials": trials, "point": 1.0},
            "runs": {
                "n": n,
                "seeds": [seed_for(sha, i, purpose) for i in range(n)],
                "seed_purpose": purpose,
                "seed_source": "commit",
                "commit_sha": sha,
                "profile": profile,
            },
        },
        "artifacts": artifacts
        if artifacts is not None
        else {"mutants_total": 94, "equivalent": 27, "per_witness": {"ledger": 67}},
    }


def _never_runs(claim_id: str, record: dict[str, Any]) -> dict[str, Any]:
    raise AssertionError(f"{claim_id} should not have been re-run: {record!r}")


# ------------------------------------------------------------- the arithmetic half


def test_the_forged_rate_is_caught_without_running_anything() -> None:
    """The attack, against the half of the gate that costs nothing.

    The forgery moved `verdict.rate` and left the artifacts alone, because rewriting
    `mutants_total`, `equivalent` and `per_witness` consistently is a great deal more work
    than rewriting one number - and the prose in CLAIMS.md is written from those artifacts,
    so a forger who does rewrite them has a document to rewrite too.
    """
    forged = _record(successes=999, trials=999)
    mismatch, checked = rate_against_artifacts(forged)
    assert checked
    assert mismatch is not None
    assert mismatch.claim_id == "SG-03"
    assert mismatch.published == "999/999"
    assert mismatch.recomputed == "67/67"


def test_the_genuine_rate_passes_the_same_check() -> None:
    mismatch, checked = rate_against_artifacts(_record())
    assert checked
    assert mismatch is None


def test_a_record_whose_artifacts_cannot_answer_is_reported_unchecked_not_agreed() -> None:
    """ "Nothing was compared" and "they agree" must never arrive as the same answer.

    This is the defect the whole module was written against, one level up: a check that
    inspects nothing and reports success. `rate_against_artifacts` returns the two separately
    so the caller cannot collapse them by accident.
    """
    thin = _record(artifacts={})
    mismatch, checked = rate_against_artifacts(thin)
    assert mismatch is None
    assert not checked

    mismatches, unchecked = arithmetic_mismatches({"SG-03": thin})
    assert mismatches == ()
    assert unchecked == ("SG-03",)


def test_every_claim_has_a_rate_rule() -> None:
    """A new claim with no rule would be covered by nothing, and nothing would say so."""
    missing = sorted(set(CLAIM_TITLES) - set(RATE_RULES))
    assert not missing, (
        f"these claims publish a rate that no rule in reproduce.RATE_RULES can check: "
        f"{missing}. A gate whose coverage is believed rather than computed is the failure "
        f"mode this file exists for."
    )


@pytest.mark.parametrize(
    ("claim_id", "artifacts", "expected"),
    [
        ("SG-00", {"tests_passed": 585, "tests_failed": 0}, (585, 585)),
        ("SG-00", {"tests_passed": 580, "tests_failed": 5}, (580, 585)),
        ("SG-01", {"comparisons": 15}, (15, 15)),
        ("SG-03", {"mutants_total": 94, "equivalent": 27, "per_witness": {"ledger": 67}}, (67, 67)),
        ("SG-04", {"months_that_moved": [{}, {}]}, (2, 2)),
        ("SG-05", {"scd2_digests": ["a", "b", "c"]}, (3, 3)),
        ("SG-06", {"records_verified": 193, "chain_breaks": []}, (193, 193)),
        ("SG-06", {"records_verified": 193, "chain_breaks": ["x"]}, (192, 193)),
        ("SG-07", {"injected_runs": 20, "divergences": []}, (20, 20)),
        ("SG-08", {"checks": ["a", "b"], "checks_failed": []}, (2, 2)),
        ("SG-09", {"checks": ["a"], "checks_failed": ["a"]}, (0, 1)),
    ],
)
def test_each_rule_reads_the_number_it_says_it_reads(
    claim_id: str, artifacts: dict[str, Any], expected: tuple[int, int]
) -> None:
    assert RATE_RULES[claim_id](_record(claim_id=claim_id, artifacts=artifacts)) == expected


# ------------------------------------------------------------- the recompute half


def test_the_forged_rate_is_caught_by_recomputing_it() -> None:
    """The attack, against the half that re-runs the claim.

    The fake runner returns the genuine 67/67 for SG-03, which is what re-running it gives;
    the record says 999/999. One mismatch, named, with both numbers in it.
    """
    forged = _record(successes=999, trials=999)

    def runner(claim_id: str, record: dict[str, Any]) -> dict[str, Any]:
        runs = record["verdict"]["runs"]
        assert runs["commit_sha"] == SHA, "the seeds must be pinned to the record's commit"
        assert runs["profile"] == "ci", "the profile must be the record's, not a default"
        return _record(claim_id=claim_id)

    result = reproduce_latest(
        {"SG-03": forged}, runner, repo_root=REPO, claim_ids=["SG-03"], head_sha=HEAD
    )
    assert not result.ok
    assert result.reproduced == ()
    assert [m.claim_id for m in result.mismatches] == ["SG-03"]
    assert result.mismatches[0].published == "999/999"
    assert result.mismatches[0].recomputed == "67/67"
    assert "DID NOT REPRODUCE" in result.summary()


def test_a_genuine_record_reproduces() -> None:
    genuine = _record()
    result = reproduce_latest(
        {"SG-03": genuine},
        lambda claim_id, record: _record(claim_id=claim_id),
        repo_root=REPO,
        claim_ids=["SG-03"],
        head_sha=HEAD,
    )
    assert result.ok
    assert result.reproduced == ("SG-03",)
    assert "every figure recomputed matches what is published" in result.summary()


def test_the_profile_the_record_names_is_the_one_it_is_recomputed_at() -> None:
    """A record written at the `fast` profile describes a different population from one
    written at `ci`, so recomputing it at the caller's default is not recomputing it.

    This is not hypothetical. The first run of `samegold verify-latest` against the real
    evidence reported SG-01 as 9/9 against 15/15 and SG-04 as 1/1 against 2/2: both records
    were written at `fast` and both were recomputed at `ci`. Two MISMATCHes about evidence
    that was perfectly good, from a gate whose whole subject is figures that were not measured
    the way they claim.
    """
    seen: list[str] = []

    def runner(claim_id: str, record: dict[str, Any]) -> dict[str, Any]:
        profile = record["verdict"]["runs"]["profile"]
        seen.append(profile)
        return _record(claim_id=claim_id, profile=profile)

    reproduce_latest(
        {"SG-03": _record(profile="fast")},
        runner,
        repo_root=REPO,
        claim_ids=["SG-03"],
        head_sha=HEAD,
    )
    assert seen == ["fast"]


def test_seeds_that_do_not_derive_from_the_commit_are_a_mismatch_not_a_run() -> None:
    """Defence in depth. The store checks this on append; a file edited afterwards never
    passes through append again, and this gate reads the file."""
    tampered = _record()
    tampered["verdict"]["runs"]["seeds"] = [1234567890]
    result = reproduce_latest(
        {"SG-03": tampered}, _never_runs, repo_root=REPO, claim_ids=["SG-03"], head_sha=HEAD
    )
    assert not result.ok
    assert result.mismatches[0].what == "seed list"


def test_a_claim_that_is_not_recomputed_is_named_and_is_not_counted_as_reproduced() -> None:
    """The summary must never be able to say "every figure reproduces" having run nothing."""
    result = reproduce_latest(
        {"SG-00": _record(claim_id="SG-00", purpose="facts")},
        _never_runs,
        repo_root=REPO,
        head_sha=HEAD,
    )
    assert result.ok  # nothing disagreed
    assert result.reproduced == ()
    assert [s.claim_id for s in result.not_recomputed] == ["SG-00"]
    assert "0 claims recomputed" in result.summary()
    assert "1 not recomputed" in result.summary()
    assert "every figure" not in result.summary()


def test_naming_a_claim_explicitly_overrides_the_default_policy() -> None:
    """`--claims SG-00` is a reasonable thing to want; the policy is a default, not a ban."""
    record = _record(claim_id="SG-00", successes=5, trials=5, purpose="facts", artifacts={})
    result = reproduce_latest(
        {"SG-00": record},
        lambda claim_id, published: record,
        repo_root=REPO,
        claim_ids=["SG-00"],
        head_sha=HEAD,
    )
    assert result.reproduced == ("SG-00",)


def test_a_record_whose_code_has_moved_is_not_recomputed_and_says_so() -> None:
    """A different answer after the code changed is a different measurement, not a forgery.

    Without this the gate would go red on every commit that touches `src/`, which is a gate
    nobody leaves switched on. With it, the run says which files moved and which command
    re-measures - the "not run" versus "failed" distinction this repository argues for in
    `scripts/preflight.sh`.
    """
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=REPO, capture_output=True, text=True, check=True
    ).stdout.strip()
    first = subprocess.run(
        ["git", "rev-list", "--max-parents=0", "HEAD"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()[0]
    assert code_moved_since(REPO, head, head) == []
    moved = code_moved_since(REPO, first, head)
    assert moved, "src/samegold cannot be identical between the first commit and HEAD"

    record = _record(sha=first)
    record["verdict"]["runs"]["seeds"] = seeds_from_commit(1, "mutation", sha=first)
    result = reproduce_latest(
        {"SG-03": record}, _never_runs, repo_root=REPO, claim_ids=["SG-03"], head_sha=head
    )
    assert result.ok
    assert result.reproduced == ()
    assert "the code under it moved" in result.not_recomputed[0].why
    assert "make evidence" in result.not_recomputed[0].why


def test_a_commit_this_checkout_does_not_have_is_not_recomputed() -> None:
    """A shallow clone must not silently recompute against seeds it cannot verify."""
    result = reproduce_latest(
        {"SG-03": _record(sha="0" * 39 + "1")},
        _never_runs,
        repo_root=REPO,
        claim_ids=["SG-03"],
        head_sha=HEAD,
    )
    assert result.reproduced == ()
    assert result.not_recomputed


def test_the_summary_of_an_empty_run_promises_nothing() -> None:
    empty = Reproduction((), (), ())
    assert empty.summary() == "0 claims recomputed from their own seeds"


# ------------------------------------------------------------- the pin


def test_pinning_the_seed_commit_changes_the_seeds_and_the_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mechanism that makes recomputation possible, and the guard that keeps it unusable
    for publishing: a pinned run reports a `seed_source` the store refuses."""
    from samegold.evidence.store import EvidenceRejected, _validate
    from samegold.generator import seeds as seeds_module

    monkeypatch.setenv(seeds_module.SEED_COMMIT_ENV, SHA)
    assert seeds_from_commit(2, "mutation") == [seed_for(SHA, i, "mutation") for i in range(2)]
    assert seeds_module.seed_source() == "pinned"

    payload = json.loads(json.dumps(_record()))
    payload["verdict"]["runs"]["seed_source"] = "pinned"
    payload["verdict"]["runs"]["tree_sha"] = "b" * 40
    with pytest.raises(EvidenceRejected, match="unknown seed_source"):
        _validate(payload)


def test_an_unset_pin_leaves_the_seeds_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    from samegold.generator import seeds as seeds_module

    monkeypatch.delenv(seeds_module.SEED_COMMIT_ENV, raising=False)
    monkeypatch.delenv("SAMEGOLD_SEED_OVERRIDE", raising=False)
    assert seeds_module.seed_source() == "commit"
    assert seeds_module.pinned_commit() is None

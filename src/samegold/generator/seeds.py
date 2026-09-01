"""Seeds are derived from the git commit, never chosen by hand.

The attack this defends against is the obvious one: an author who can pick seeds can pick
the seeds under which the harness is green. Deriving them from the commit SHA means that
choosing a favourable seed requires changing the code, which changes the SHA, which changes
the seed.

The rule is enforced, not merely stated: ``EvidenceStore.append`` recomputes the seeds from
the commit and the purpose recorded in the record and refuses to write it when they differ,
and ``verify_chain`` re-checks every record already in the history. An earlier version of
this docstring claimed that gate existed before it did, and an adversarial review wrote two
records by hand with chosen seeds and a fabricated CI link; both are rejected now, and the
test that reproduces the attack is tests/fast/test_evidence_gate.py.

``SAMEGOLD_SEED_OVERRIDE`` exists for one purpose only: letting a third party run
``make refute SEED=...`` with a seed the author never saw. Runs made with an override are
marked ``seed_source="override"`` in the evidence and are never counted towards a published
claim, only towards the refutation log.
"""

from __future__ import annotations

import hashlib
import os
import subprocess


def current_tree(default: str = "0" * 40) -> tuple[str, bool]:
    """The hash of the tree that is actually about to run, and whether it is committed.

    ``git stash create`` writes a commit object for the working tree WITHOUT touching the
    index, the stash or the checkout, and its tree is exactly what a run will execute. On a
    clean tree it prints nothing, and the answer is HEAD's tree.

    Why a record needs this and not only the commit: the commit anchors the SEEDS, and an
    adversarial review pointed out that it anchors nothing else. Two records at one commit
    can disagree about their own denominators, because the code between them changed and was
    not committed. That is what fixing a bug and re-measuring looks like, and it is also what
    retry-until-green looks like, and the record could not tell them apart.
    """

    def git(*args: str) -> str:
        try:
            out = subprocess.run(
                ["git", *args], capture_output=True, text=True, timeout=15, check=False
            )
        except (OSError, subprocess.SubprocessError):
            return ""
        return out.stdout.strip() if out.returncode == 0 else ""

    stashed = git("stash", "create")
    if stashed:
        tree = git("rev-parse", f"{stashed}^{{tree}}")
        return (tree or default, True)
    tree = git("rev-parse", "HEAD^{tree}")
    return (tree or default, False)


def current_commit_sha(default: str = "0" * 40) -> str:
    """The commit the working tree is on, or ``default`` outside a git checkout.

    We do not fail when git is absent (a downloaded tarball is a legitimate way to run
    this), but the evidence record keeps ``sha_source`` so that a reader can tell a
    CI-produced number from a tarball-produced one.
    """
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return default
    sha = out.stdout.strip()
    return sha if out.returncode == 0 and len(sha) == 40 else default


def seed_for(sha: str, index: int, purpose: str = "generator") -> int:
    """A 64-bit seed bound to (commit, index, purpose).

    ``purpose`` keeps the generator stream, the arrival-permutation stream and the fault
    scheduler from sharing a sequence: two of them consuming the same numbers would make
    the "same input under a different arrival order" experiment secretly change the input.
    """
    digest = hashlib.blake2b(f"{sha}:{purpose}:{index}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def seeds_from_commit(n: int, purpose: str = "generator", sha: str | None = None) -> list[int]:
    override = os.environ.get("SAMEGOLD_SEED_OVERRIDE")
    if override:
        base = hashlib.blake2b(override.encode(), digest_size=20).hexdigest()
        return [seed_for(base, i, purpose) for i in range(n)]
    sha = sha or current_commit_sha()
    return [seed_for(sha, i, purpose) for i in range(n)]


def seed_source() -> str:
    return "override" if os.environ.get("SAMEGOLD_SEED_OVERRIDE") else "commit"

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

# The evidence directory is the OUTPUT of a run, not an input to it, and leaving it in the
# dirtiness check made this field say the same thing forever.
#
# `samegold evidence` appends a record per claim as it goes. So the first claim of a sweep saw
# a clean tree and every claim after it saw `evidence/history.jsonl` modified - by the sweep
# itself, three seconds earlier - and recorded `tree_dirty: true`. Nine of the ten records
# published on the front page say "on an uncommitted tree" for that reason and not for any
# reason a reader would guess from the words. A caveat that is always on carries no
# information, and this one was being read as "the code was not committed".
#
# What the field is FOR is in `current_tree`'s docstring: two records at one commit can
# disagree about their denominators because the code between them changed and was not
# committed. A record already written cannot change what the next claim computes. The one
# claim that reads the chain at all is SG-06, whose job is to verify it, and whose defence
# against an edited chain is the hash chain rather than this flag.
#
# Everything else still counts, untracked files included - that is the shape of "code that is
# in no commit" a repository under review is most likely to have, and it is what caught an
# untracked test module moving a published test count by five.
_EVIDENCE_PREFIX = "evidence/"


def _code_changes(status: str) -> list[str]:
    """The lines of `git status --porcelain` that are not this repository's own output."""
    out: list[str] = []
    for line in status.splitlines():
        if not line.strip():
            continue
        # "XY path", and for a rename "XY old -> new". The destination is what exists now.
        path = line[3:].strip().strip('"')
        if " -> " in path:
            path = path.split(" -> ", 1)[1].strip().strip('"')
        if path.replace("\\", "/").startswith(_EVIDENCE_PREFIX):
            continue
        out.append(path)
    return out


def current_tree(default: str = "") -> tuple[str, bool]:
    """The hash of the tree that is about to run, and whether it is exactly a commit's tree.

    Why a record needs this and not only the commit: the commit anchors the SEEDS, and an
    adversarial review pointed out that it anchors nothing else. Two records at one commit can
    disagree about their own denominators, because the code between them changed and was not
    committed. That is what fixing a bug and re-measuring looks like, and it is also what
    retry-until-green looks like, and the record could not tell them apart.

    Dirtiness comes from ``git status --porcelain`` and not from ``git stash create``, which
    was the first implementation and which IGNORES UNTRACKED FILES. Adding one untracked test
    module moved the published test count by five while this function reported a clean tree,
    and an untracked file is the shape of "code that is in no commit" a repository under
    active review is most likely to have.

    Outside a git checkout it returns ``("", False)`` and the evidence gate refuses the
    record. Everything in this repository runs from a tarball; publishing evidence from one
    does not, because a number whose provenance is "some files, somewhere" is not evidence.
    """

    def git(*args: str) -> tuple[str, bool]:
        try:
            out = subprocess.run(
                ["git", *args], capture_output=True, text=True, timeout=15, check=False
            )
        except (OSError, subprocess.SubprocessError):
            return ("", False)
        return (out.stdout.strip(), out.returncode == 0)

    status, ok = git("status", "--porcelain")
    if not ok:
        return (default, False)
    dirty = bool(_code_changes(status))
    if dirty:
        # A commit object for the working tree, written without touching the index, the stash
        # or the checkout. It captures tracked modifications; an untracked file changes no
        # tree hash anywhere, which is why `dirty` is decided above and not here.
        stashed, ok = git("stash", "create")
        if ok and stashed:
            tree, ok = git("rev-parse", f"{stashed}^{{tree}}")
            if ok and tree:
                return (tree, True)
    tree, ok = git("rev-parse", "HEAD^{tree}")
    return ((tree if ok and tree else default), dirty)


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

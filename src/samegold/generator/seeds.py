"""Seeds are derived from the git commit, never chosen by hand.

The attack this defends against is the obvious one: an author who can pick seeds can pick
the seeds under which the harness is green. Deriving them from the commit SHA means that
choosing a favourable seed requires changing the code, which changes the SHA, which changes
the seed. The evidence gate (evidence/store.py) rejects any run whose seeds do not match
the SHA recorded in the same evidence record.

``SAMEGOLD_SEED_OVERRIDE`` exists for one purpose only: letting a third party run
``make refute SEED=...`` with a seed the author never saw. Runs made with an override are
marked ``seed_source="override"`` in the evidence and are never counted towards a published
claim, only towards the refutation log.
"""

from __future__ import annotations

import hashlib
import os
import subprocess


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

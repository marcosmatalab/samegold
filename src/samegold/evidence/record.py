"""The evidence record: the only currency this repository publishes numbers in.

Every claim in the README is rendered from one of these, and a test in the fast lane
re-renders the README and fails on drift. That alone would be theatre - the author can
regenerate favourable evidence - so a record also carries the things that make a third
party able to disprove it:

  * the commit SHA the seeds were derived from (seeds are not chosen, see generator/seeds.py);
  * the CI run that produced it, when there was one. A record with ``ci_run_url = None`` was
    produced on someone's laptop and the renderer marks it as such, in the README, in words;
  * the exact versions of the engines, so that "green" cannot quietly mean "green on a
    version nobody runs any more";
  * the git TREE the run executed and whether it was committed, because the commit SHA
    anchors the seeds and nothing else: two records at one commit can disagree about their
    own denominators, and until the tree was recorded nothing could tell an honest
    re-measurement from retry-until-green;
  * the digest of the pipeline sources, recorded by the crash campaign so that a reader can
    check the injected runs and the clean runs against the same code. RECORDED, not enforced,
    and it says so where the function is defined - an earlier version of this list claimed it
    "proved" something, while the field was null in every record in the history.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
from dataclasses import dataclass, field
from importlib import metadata
from pathlib import Path
from typing import Any

from samegold.verify.verdict import Verdict


def environment_fingerprint() -> dict[str, str]:
    versions: dict[str, str] = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }
    # ASKED OF THE INSTALLED DISTRIBUTION, not by importing the package, and the difference is
    # a gate that was red on every machine that can run the Spark lane.
    #
    # This used to be `__import__(module)` and `mod.__version__`. It worked, and it left
    # `pyspark`, `delta` and `py4j` in `sys.modules` - so the round-17 session hook in
    # `tests/fast/conftest.py`, which fails the fast lane if Spark was ever imported, fired on
    # any run that built an evidence record. Two whole test files, sixteen tests, exit status 1,
    # on both of this project's development machines. Nobody saw it: pytest still printed
    # `57 passed`, the hook's message went to stdout among the dots, and the number is what gets
    # read. It is the round-15 finding again - a gate whose result is not what its summary line
    # says - with the sign flipped: red locally, green in CI, because CI installs `.[dev]` and
    # has no Spark to import.
    #
    # `importlib.metadata` reads the installed distribution's metadata and does not execute the
    # package, which is the right question anyway: this field records WHICH VERSION IS
    # INSTALLED, and importing was only ever a way of finding that out. It also answers better -
    # `delta.__version__` does not exist, so delta-spark was recorded as "unknown" in every
    # evidence record ever written, and is recorded as its real version now.
    #
    # `samegold doctor` in cli.py keeps the import, deliberately: that command answers "can this
    # environment actually load it", which is a different question and worth the import.
    for distribution, label in (
        ("pyspark", "pyspark"),
        ("delta-spark", "delta_spark"),
        ("duckdb", "duckdb"),
        ("deltalake", "delta_rs"),
        ("sqlglot", "sqlglot"),
        ("pyarrow", "pyarrow"),
    ):
        try:
            versions[label] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[label] = "absent"
    return versions


def artifact_digest(paths: list[Path]) -> str:
    """Digest of the deployable artefact (pipeline code + bundle + SQL).

    Recorded so that a reader can check that the fault-injection runs and the clean runs
    executed the same program: if instrumenting the pipeline required editing it, this digest
    changes.

    RECORDED, not enforced. An earlier version of this docstring said the corresponding claim
    was "refused at render time", and the renderer's own docstring said such a record would be
    printed as UNVERIFIABLE. Neither was true: the word appeared in one docstring and nowhere
    else in the repository, and this field was written into records and read by nothing. Both
    sentences are corrected rather than implemented, because a field that is written and never
    read is a comment wearing a check's clothes, and this repository is about the difference.
    """
    hasher = hashlib.blake2b(digest_size=16)
    for path in sorted(paths):
        if not path.is_file():
            continue
        # LENGTH-FRAMED, and the path rather than the basename. Concatenating name and
        # contents with no framing collides: ("ab.py", b"c") and ("a.py", b"bc") hash
        # identically, which is the same mistake verify/digest.py documents at length about
        # its own encoding. Using only the basename made two files with one name in different
        # packages indistinguishable.
        name = str(path).encode()
        body = path.read_bytes()
        hasher.update(f"{len(name)}:".encode())
        hasher.update(name)
        hasher.update(f"{len(body)}:".encode())
        hasher.update(body)
    return hasher.hexdigest()


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    claim_id: str
    title: str
    verdict: Verdict
    runtime: str
    artifacts: dict[str, Any] = field(default_factory=dict)
    environment: dict[str, str] = field(default_factory=environment_fingerprint)
    ci_run_url: str | None = field(default_factory=lambda: _ci_run_url())
    ci_commit_sha: str | None = field(default_factory=lambda: os.environ.get("GITHUB_SHA"))
    artifact_digest: str | None = None
    not_claimed: tuple[str, ...] = ()

    def to_json(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "title": self.title,
            "runtime": self.runtime,
            "verdict": self.verdict.to_json(),
            "artifacts": self.artifacts,
            "environment": self.environment,
            "ci_run_url": self.ci_run_url,
            "ci_commit_sha": self.ci_commit_sha,
            "artifact_digest": self.artifact_digest,
            "not_claimed": list(self.not_claimed),
        }

    def to_line(self) -> str:
        return json.dumps(self.to_json(), sort_keys=True)


def _ci_run_url() -> str | None:
    server = os.environ.get("GITHUB_SERVER_URL")
    repo = os.environ.get("GITHUB_REPOSITORY")
    run_id = os.environ.get("GITHUB_RUN_ID")
    if server and repo and run_id:
        return f"{server}/{repo}/actions/runs/{run_id}"
    return None

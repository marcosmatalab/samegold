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
  * the digest of the deployable artefact, which is how the fault-injection runs prove they
    exercised the same program that gets deployed rather than an instrumented copy.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from samegold.verify.verdict import Verdict


def environment_fingerprint() -> dict[str, str]:
    versions: dict[str, str] = {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }
    for module, label in (
        ("pyspark", "pyspark"),
        ("delta", "delta_spark"),
        ("duckdb", "duckdb"),
        ("deltalake", "delta_rs"),
        ("sqlglot", "sqlglot"),
        ("pyarrow", "pyarrow"),
    ):
        try:  # pragma: no cover - import side effects only
            mod = __import__(module)
            versions[label] = str(getattr(mod, "__version__", "unknown"))
        except Exception:
            versions[label] = "absent"
    return versions


def artifact_digest(paths: list[Path]) -> str:
    """Digest of the deployable artefact (pipeline code + bundle + SQL).

    Used to prove that the fault-injection runs and the clean runs executed the same
    program. If instrumenting the pipeline required editing it, this digest changes and the
    corresponding claim is refused at render time.
    """
    hasher = hashlib.blake2b(digest_size=16)
    for path in sorted(paths):
        if path.is_file():
            hasher.update(path.name.encode())
            hasher.update(path.read_bytes())
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

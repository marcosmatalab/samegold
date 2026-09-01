"""Append-only evidence with a hash chain, and a gate that actually rejects things.

The first version of this file was a sink: it wrote whatever it was handed and the renderer
printed it. An adversarial review appended two lines by hand claiming 999/999 agreements and
a 100% mutation score, pointed the record at a CI run that does not exist, ran the whole test
suite, and got 152 passed. Everything below exists because of that.

Three defences, in the order a forger meets them:

  1. **The chain.** Every record carries the hash of the previous one and its own hash over a
     canonical serialisation. Editing, inserting or deleting a line anywhere in
     ``history.jsonl`` breaks every hash after it, and ``verify_chain`` says which line.
  2. **Seed derivation.** A record names the commit its seeds came from and the purpose they
     were drawn for. The gate recomputes them and refuses the record if they do not match,
     and the purpose must be one of a closed set, so neither the numbers nor the label of the
     stream is the author's to choose: "I ran it with 333 lucky seeds" cannot be written down
     and neither can "I ran it 333 times under 333 names".
  2b. **Identity.** The claim id must be one claims.py defines, the title must be the title
     that claim declares, and the verdict inside the record must be about the same claim. A
     gate that checks only how a record was produced will happily publish "SG-DOES-NOT-EXIST:
     999/999".
  3. **Provenance shape.** ``ci_run_url`` must be a real GitHub Actions run URL for the
     repository the record names, and a record that claims CI must also carry the commit the
     workflow ran on. It cannot prove the run exists - nothing offline can - but it can stop
     an arbitrary string from being printed as "CI", and the renderer labels anything without
     it as a local run.

None of this makes forgery impossible; it makes forgery require rewriting the chain, which
is a visible act in git history rather than an invisible one in a JSON file.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from samegold.evidence.record import EvidenceRecord

GENESIS = "0" * 32
CI_RUN_URL = re.compile(r"^https://github\.com/[\w.\-]+/[\w.\-]+/actions/runs/\d+$")


class EvidenceRejected(ValueError):
    """Raised when a record cannot be written. The message says which rule refused it."""


@dataclass(frozen=True, slots=True)
class ChainBreak:
    line: int
    claim_id: str
    problem: str

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"line {self.line} ({self.claim_id}): {self.problem}"


def record_hash(payload: dict[str, Any]) -> str:
    """Hash over the record's canonical JSON, excluding its own hash field."""
    body = {k: v for k, v in payload.items() if k != "hash"}
    return hashlib.blake2b(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode(), digest_size=16
    ).hexdigest()


def _known_claims() -> dict[str, str]:
    """The claim ids and titles this repository defines, from claims.py.

    Imported lazily so the store stays importable without the claim implementations (the
    Databricks lane writes records from a notebook that has none of them).
    """
    from samegold.evidence.registry import CLAIM_TITLES

    return dict(CLAIM_TITLES)


def _seed_purposes() -> set[str]:
    from samegold.evidence.registry import SEED_PURPOSES

    return set(SEED_PURPOSES)


def _finite(value: Any, path: str) -> None:
    """Refuse NaN and Infinity anywhere in a record.

    ``json.dumps`` writes them as the bare tokens ``NaN`` and ``Infinity``, which are not
    JSON. A record with one of them was accepted, the chain verified, and the append-only
    file the whole argument rests on could no longer be read by `jq`, `JSON.parse`,
    `serde_json` or Go's `encoding/json`: intact according to itself and unreadable to
    everyone else. `verify/digest.py` had refused non-finite floats since its first review;
    the store had not.
    """
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        raise EvidenceRejected(f"non-finite number at {path}: {value!r}. Evidence must be JSON.")
    if isinstance(value, dict):
        for key, item in value.items():
            _finite(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _finite(item, f"{path}[{index}]")


def _validate(payload: dict[str, Any], *, historical: bool = False) -> None:
    """Refuse a record. ``historical`` relaxes the checks that are about PUBLISHING now.

    The chain is a history: a record written before a claim was renamed still names the
    title it had, and re-validating the whole file must not turn a legitimate past into a
    forgery. So the title comparison applies when appending and not when verifying, while
    everything about identity, seeds and provenance applies to both.
    """
    from samegold.generator.seeds import seeds_from_commit

    claim_id = str(payload.get("claim_id", ""))
    # A record for a claim that does not exist used to be accepted, given its own runs/ file
    # and rendered as a table row: the gate checked how a record was produced and never what
    # it was about, so "SG-DOES-NOT-EXIST: 999/999" was a valid publication. The registry is
    # claims.py, which is also what the documents cite.
    known = _known_claims()
    if claim_id not in known:
        raise EvidenceRejected(
            f"{claim_id!r} is not a claim this repository defines. The claims are "
            f"{sorted(known)}; add it to claims.py before publishing evidence for it."
        )
    if not historical and payload.get("title") != known[claim_id]:
        raise EvidenceRejected(
            f"{claim_id}: the record's title is {payload.get('title')!r} and claims.py says "
            f"{known[claim_id]!r}. A record that renames its own claim is a record about "
            f"something else."
        )
    verdict_claim = str(payload.get("verdict", {}).get("claim_id", claim_id))
    if verdict_claim != claim_id:
        raise EvidenceRejected(
            f"{claim_id}: the verdict inside this record is for {verdict_claim!r}. The record "
            f"and its verdict must be about the same claim."
        )
    _finite(payload.get("artifacts"), f"{claim_id}.artifacts")
    _finite(payload.get("verdict"), f"{claim_id}.verdict")

    runs = payload.get("verdict", {}).get("runs", {})
    sha = str(runs.get("commit_sha", ""))
    seeds = list(runs.get("seeds", []))
    purpose = str(runs.get("seed_purpose", ""))
    source = str(runs.get("seed_source", ""))

    if len(sha) != 40 or any(ch not in "0123456789abcdef" for ch in sha):
        raise EvidenceRejected(
            f"{payload.get('claim_id')}: commit_sha is not a git object id: {sha!r}"
        )
    # And the TREE. The commit anchors the seeds; it does not anchor the code, and two
    # records at one commit can disagree about their own denominators because the code
    # between them changed and was not committed. Historical records predate the field, so
    # they are read rather than refused; a new record without it is refused.
    tree = str(runs.get("tree_sha", ""))
    if not historical and (len(tree) != 40 or any(ch not in "0123456789abcdef" for ch in tree)):
        raise EvidenceRejected(
            f"{payload.get('claim_id')}: the record does not name the git tree it ran on "
            f"({tree!r}). A commit anchors the seeds; only the tree anchors the code. "
            f"Everything in this repository runs from a downloaded tarball; publishing "
            f"evidence from one does not, because a number whose provenance is 'some files, "
            f"somewhere' is not evidence."
        )
    if source == "override":
        # An override run is a refutation, not evidence. It is written to a separate log and
        # never into the history the documents render, because its seeds are by design not
        # derived from the commit and nothing can recompute them. An adversarial review wrote
        # an "override" record with invented seeds straight into the main history and it was
        # accepted, which made every other check in this file decorative.
        raise EvidenceRejected(
            f"{payload.get('claim_id')}: this record was produced with SAMEGOLD_SEED_OVERRIDE. "
            f"Refutation runs go to evidence/refutations.jsonl (created on demand) and "
            f"never back a published number."
        )
    if source == "commit":
        if not purpose:
            raise EvidenceRejected(
                f"{payload.get('claim_id')}: the record does not say what purpose its seeds "
                f"were drawn for, so they cannot be recomputed"
            )
        # And the purpose comes from a CLOSED SET. Recomputing the seeds from (commit,
        # purpose) stops an author choosing the numbers; it does not stop them choosing the
        # LABEL, and each label is a fresh gate-approved draw at a fixed commit. A review
        # appended two hundred records at one commit under the purposes
        # "witness-attempt-0" ... "witness-attempt-199" and the gate accepted all two
        # hundred. "I ran it with 333 lucky seeds" was still writable, one rename at a time.
        if purpose not in _seed_purposes():
            raise EvidenceRejected(
                f"{payload.get('claim_id')}: seed purpose {purpose!r} is not one of the "
                f"purposes this repository draws seeds for. Each purpose is a distinct seed "
                f"stream, so an unlisted one is a fresh draw at a fixed commit, which is "
                f"exactly what deriving the seeds is supposed to prevent."
            )
        # An override run must never be recomputed against the commit: its seeds are, by
        # design, not derived from it. Those records are kept but never back a published
        # number, which the renderer states in the provenance column.
        expected = _expected_seeds(len(seeds), purpose, sha)
        if seeds != expected:
            raise EvidenceRejected(
                f"{payload.get('claim_id')}: the seeds in this record do not derive from "
                f"commit {sha[:12]} for purpose {purpose!r}. Seeds are not chosen; they are "
                f"computed by generator/seeds.py from the commit."
            )
    else:
        raise EvidenceRejected(f"{payload.get('claim_id')}: unknown seed_source {source!r}")

    url = payload.get("ci_run_url")
    if url is not None:
        if not CI_RUN_URL.match(str(url)):
            raise EvidenceRejected(
                f"{payload.get('claim_id')}: ci_run_url is not a GitHub Actions run URL: {url!r}"
            )
        # A URL without the commit it ran on is a URL to anywhere. Pointing a record at
        # `github.com/torvalds/linux/actions/runs/1` used to be enough to have it printed as
        # "CI" in the results table.
        if payload.get("ci_commit_sha") != sha:
            raise EvidenceRejected(
                f"{payload.get('claim_id')}: the record claims a CI run but its ci_commit_sha "
                f"is {payload.get('ci_commit_sha')!r} while its seeds come from {sha[:12]}"
            )
        repository = current_repository()
        if repository and f"/{repository}/actions/runs/" not in str(url):
            raise EvidenceRejected(
                f"{payload.get('claim_id')}: ci_run_url points at another repository: {url!r}"
            )
    del seeds_from_commit  # imported for the reference in the docstring above


def _expected_seeds(n: int, purpose: str, sha: str) -> list[int]:
    from samegold.generator.seeds import seed_for

    return [seed_for(sha, i, purpose) for i in range(n)]


class EvidenceStore:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.runs_dir = self.root / "runs"
        self.history = self.root / "history.jsonl"
        self.runs_dir.mkdir(parents=True, exist_ok=True)
        self.history.touch(exist_ok=True)

    # ---------------------------------------------------------------- writing

    def append(self, record: EvidenceRecord) -> None:
        payload = record.to_json()
        _validate(payload)
        payload["prev"] = self.head_hash()
        payload["hash"] = record_hash(payload)
        with self.history.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True, allow_nan=False) + "\n")
        latest = self.runs_dir / f"{record.claim_id}.json"
        latest.write_text(
            json.dumps(payload, indent=2, sort_keys=True, allow_nan=False), encoding="utf-8"
        )

    def head_hash(self) -> str:
        last = None
        for entry in self.read_history():
            last = entry
        return GENESIS if last is None else str(last.get("hash", GENESIS))

    # ---------------------------------------------------------------- reading

    def read_history(self) -> Iterator[dict[str, Any]]:
        if not self.history.exists():
            return iter(())
        return (
            json.loads(line)
            for line in self.history.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )

    def latest(self) -> dict[str, dict[str, Any]]:
        """The most recent record per claim, which is what the documents render."""
        out: dict[str, dict[str, Any]] = {}
        for entry in self.read_history():
            out[str(entry["claim_id"])] = entry
        return out

    def counts(self) -> dict[str, int]:
        green = red = 0
        for entry in self.read_history():
            verdict = entry.get("verdict", {})
            if isinstance(verdict, dict) and verdict.get("outcome") == "pass":
                green += 1
            else:
                red += 1
        return {"pass": green, "fail": red, "total": green + red}

    # ------------------------------------------------------------ verifying

    def verify_chain(self, repo_root: Path | None = None) -> list[ChainBreak]:
        """Recompute the whole chain and anchor it to this repository.

        Internal consistency alone is not enough, and an adversarial review proved it by
        rewriting the whole file from scratch with correct hashes and invented figures. So the
        chain is also checked against things outside it:

          * every ``commit_sha`` must be a commit that exists in this repository, so a record
            cannot name a made-up commit whose derived seeds happen to be convenient;
          * records must be in non-decreasing time order, so they cannot be reshuffled to make
            an older PASS the latest word on a claim that later failed;
          * the per-claim files in ``runs/`` must match the latest record in the history, so
            editing one of them changes nothing.

        What remains possible, and is stated in the README rather than hidden: anyone who can
        run the code can rewrite the entire chain. What the chain buys is that a SINGLE record
        cannot be edited, inserted, reordered or removed without rewriting everything after
        it, and that every published record names a real commit of this repository.
        """
        breaks: list[ChainBreak] = []
        previous = GENESIS
        last_started = ""
        seen_claims: dict[str, dict[str, Any]] = {}
        entries = list(self.read_history())
        # The commit anchor is only meaningful in the checkout that produced the evidence. In
        # a fork, a shallow clone or a downloaded tarball the recorded commits are genuinely
        # unknown, and failing there would tell every reader the repository is broken. The
        # anchor therefore applies when at least one recorded commit is present in this
        # checkout, which is what makes it the lineage that produced these records.
        anchored = any(
            _commit_exists(
                str(entry.get("verdict", {}).get("runs", {}).get("commit_sha")), repo_root
            )
            for entry in entries
        )
        for number, entry in enumerate(entries, start=1):
            claim = str(entry.get("claim_id", "?"))
            if entry.get("prev") != previous:
                breaks.append(
                    ChainBreak(
                        number,
                        claim,
                        f"prev is {entry.get('prev')!r} but the previous record hashes to "
                        f"{previous!r}: a line was edited, inserted or removed",
                    )
                )
            recomputed = record_hash(entry)
            if entry.get("hash") != recomputed:
                breaks.append(
                    ChainBreak(
                        number,
                        claim,
                        f"hash is {entry.get('hash')!r} but the content hashes to "
                        f"{recomputed!r}: this record was modified after it was written",
                    )
                )
            try:
                _validate(entry, historical=True)
            except EvidenceRejected as exc:
                breaks.append(ChainBreak(number, claim, str(exc)))
            started = str(entry.get("verdict", {}).get("runs", {}).get("started_at", ""))
            if started and started < last_started:
                breaks.append(
                    ChainBreak(
                        number,
                        claim,
                        f"this record started at {started}, before the previous one "
                        f"({last_started}): the history has been reordered",
                    )
                )
            last_started = max(last_started, started)
            named_commit = str(entry.get("verdict", {}).get("runs", {}).get("commit_sha"))
            if anchored and not _commit_exists(named_commit, repo_root):
                breaks.append(
                    ChainBreak(
                        number,
                        claim,
                        "the commit this record names does not exist in this repository",
                    )
                )
            seen_claims[claim] = entry
            previous = str(entry.get("hash", GENESIS))

        for claim_id, entry in seen_claims.items():
            path = self.runs_dir / f"{claim_id}.json"
            if not path.exists():
                breaks.append(ChainBreak(0, claim_id, f"runs/{claim_id}.json is missing"))
                continue
            stored = json.loads(path.read_text(encoding="utf-8"))
            # Recompute from the CONTENT. Comparing the stored `hash` field with the history's
            # would pass for a file whose numbers were edited and whose hash field was left
            # alone, which is exactly how the first version of this check was defeated.
            recomputed_stored = record_hash(stored)
            if entry.get("hash") not in (recomputed_stored, stored.get("hash")) or (
                recomputed_stored != stored.get("hash")
            ):
                breaks.append(
                    ChainBreak(
                        0,
                        claim_id,
                        f"runs/{claim_id}.json does not match the latest record in the "
                        f"history: it was edited, or an older run was left behind",
                    )
                )
        return breaks


def current_repository() -> str | None:
    return os.environ.get("GITHUB_REPOSITORY")


def _commit_exists(sha: str | None, repo_root: Path | None = None) -> bool:
    """True when git knows this object. Outside a checkout, everything passes.

    The all-zero sha is the documented stand-in for "not a git checkout" and is accepted, so
    that running from a downloaded tarball produces evidence rather than an error. The
    renderer already labels such records as local runs.
    """
    import subprocess

    if not sha or sha == "0" * 40:
        return True
    try:
        result = subprocess.run(
            ["git", "cat-file", "-e", f"{sha}^{{commit}}"],
            cwd=str(repo_root) if repo_root else None,
            capture_output=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return True
    return result.returncode == 0

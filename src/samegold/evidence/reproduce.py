"""The gate that recomputes a published figure instead of validating its paperwork.

WHY THIS EXISTS, measured on 20 September 2026 against HEAD ``9e52f15``. The chain had four
defences and an adversarial review walked past all four with fifteen lines of Python:

    latest = ...                                   # the REAL SG-03 record
    forged = dict(latest["SG-03"])
    forged["verdict"]["rate"] = {"successes": 999, "trials": 999, ...}
    forged["verdict"]["runs"]["started_at"] = <a fresh timestamp>
    forged["prev"] = rows[-1]["hash"]              # chained correctly
    forged["hash"] = record_hash(forged)           # hashed with this repository's own hasher
    open("evidence/history.jsonl", "a").write(json.dumps(forged, sort_keys=True) + "\\n")

Nothing was edited. Every hash held, every seed derived from its commit, the timestamps stayed
monotonic and the provenance was real, because all of it was copied from a genuine record. The
front page then said ``SG-03 mutation campaign | PASS | 999/999``, ``samegold check`` exited 0
with "evidence chain verified", and ``make fast`` reported 594 passed.

``store.py`` said, in its own docstring, that forging "requires rewriting the chain, which is a
visible act in the git history". Measured: appending is enough. Every existing check asks
whether the record is WELL FORMED. None of them asks whether the number in it is TRUE.

So this module asks the only question that separates the two: run the claim again, from the
seeds the record itself names, and see whether the same number comes out.

Two gates, not one, because they fail differently:

  * `reproduce_latest` re-runs the claims. It is exact and it costs minutes, so it is a step
    in CI and in `make preflight`, not a test in the fast lane.
  * `rate_against_artifacts` reads one record and needs no run at all. The forgery above moved
    `rate` and left the artifacts alone, so `mutants_total: 94, equivalent: 27` sat two lines
    under a rate of 999/999. That is arithmetic, it costs nothing, and it is what makes
    `make fast` go red on the attack rather than only the slower lane.

Neither is allowed to import `samegold.claims`: the layer map in tests/fast/test_architecture.py
lets `evidence` reach `verify` and `generator` and nothing else. The caller injects a runner.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from samegold.generator.seeds import current_commit_sha, seeds_from_commit

#: The paths whose contents decide what a claim computes. If none of them moved between the
#: commit a record names and HEAD, re-running the claim answers the same question the record
#: answered. If any of them moved, a different answer is a different measurement rather than a
#: disagreement, and this module says so instead of raising a false alarm.
#:
#: `evidence/` is deliberately NOT here: it is the output of a run, never an input to one.
CODE_PATHS = ("src/samegold", "pyproject.toml")

#: Claims this gate does not re-run by default, each with the reason a reader can check.
#: Naming them here rather than leaving them out is the whole difference between a gate that
#: covers eight of ten claims and one that is believed to cover ten.
NOT_BY_DEFAULT = {
    "SG-00": (
        "recomputing it runs the entire fast lane a second time inside the lane that is "
        "already running it. Its figures are checked against the repository instead, by "
        "tests/fast/test_documentation.py. "
        "`samegold verify-latest --claims SG-00` runs it anyway."
    ),
    "SG-07": (
        "the crash campaign needs a JVM and about ten minutes, so it belongs to the Spark "
        "lane rather than to this one. `samegold verify-latest --claims SG-07` runs it where "
        "a JVM exists."
    ),
}


@dataclass(frozen=True, slots=True)
class Mismatch:
    """A published figure that did not come back."""

    claim_id: str
    what: str
    published: str
    recomputed: str
    detail: str

    def __str__(self) -> str:
        return (
            f"{self.claim_id}: the published {self.what} is {self.published} and recomputing "
            f"it gives {self.recomputed}. {self.detail}"
        )


@dataclass(frozen=True, slots=True)
class NotRecomputed:
    """A claim this run did not check, and why. Never silent, never counted as reproduced."""

    claim_id: str
    why: str

    def __str__(self) -> str:
        return f"{self.claim_id}: not recomputed. {self.why}"


@dataclass(frozen=True, slots=True)
class Reproduction:
    reproduced: tuple[str, ...]
    mismatches: tuple[Mismatch, ...]
    not_recomputed: tuple[NotRecomputed, ...]

    @property
    def ok(self) -> bool:
        return not self.mismatches

    def summary(self) -> str:
        """One line that says what was checked, never one that implies more than was.

        "every published figure reproduces" after recomputing nothing is the shape of sentence
        this repository exists to argue against, so the counts come first and the reassuring
        clause only appears when something was actually recomputed.
        """
        n = len(self.reproduced)
        parts = [f"{n} claim{'' if n == 1 else 's'} recomputed from their own seeds"]
        if self.mismatches:
            parts.append(f"{len(self.mismatches)} DID NOT REPRODUCE")
        elif n:
            parts.append("every figure recomputed matches what is published")
        if self.not_recomputed:
            parts.append(f"{len(self.not_recomputed)} not recomputed")
        return "; ".join(parts)


def _rate(record: Mapping[str, Any]) -> dict[str, Any] | None:
    rate = record.get("verdict", {}).get("rate")
    return rate if isinstance(rate, dict) else None


def _shape(rate: Mapping[str, Any] | None) -> str:
    if not rate:
        return "no rate"
    return f"{rate.get('successes')}/{rate.get('trials')}"


def _git(repo_root: Path, *args: str) -> tuple[int, str]:
    out = subprocess.run(
        ["git", *args], cwd=repo_root, capture_output=True, text=True, timeout=60, check=False
    )
    return out.returncode, out.stdout.strip()


def code_moved_since(repo_root: Path, sha: str, head: str) -> list[str]:
    """The files under `CODE_PATHS` that differ between `sha` and `head`.

    An empty list means re-running a claim asks the same question the record answered. A
    non-empty one means the record is a measurement of code that is no longer here, which is
    not a disagreement - it is a reason to run `make evidence`.
    """
    if sha == head:
        return []
    code, _ = _git(repo_root, "cat-file", "-e", f"{sha}^{{commit}}")
    if code != 0:
        return ["<the commit this record names is not in this checkout>"]
    code, out = _git(repo_root, "diff", "--name-only", sha, head, "--", *CODE_PATHS)
    if code != 0:
        return ["<git could not compare the two commits>"]
    return [line for line in out.splitlines() if line.strip()]


def reproduce_latest(
    latest: Mapping[str, Mapping[str, Any]],
    run_claim: Callable[[str, Mapping[str, Any]], Mapping[str, Any]],
    *,
    repo_root: Path,
    claim_ids: Sequence[str] | None = None,
    head_sha: str | None = None,
) -> Reproduction:
    """Re-run each claim from the seeds its own most recent record names, and compare.

    `run_claim` takes the claim id and THE RECORD to reproduce, and returns a fresh record
    as a dict. It is injected rather than imported so that this module stays inside the
    `evidence` layer; `cli.py` is the composition root and supplies it.

    The whole record rather than a handful of fields, because "the seeds its own record names"
    turned out to mean more than the seeds. It means the commit, so the derivation matches; the
    profile, so the population matches; and for SG-06 the chain head, so the denominator
    matches. Each of those was discovered by this gate reporting a mismatch about evidence that
    was perfectly good, and a signature that has to grow every time is a signature that should
    have been the record.

    The profile is part of "from the seeds its own record names" and the first version of this
    left it out. SG-01 was recorded at the `fast` profile and recomputed at `ci`, which is a
    different population: 9 of 9 against 15 of 15, reported as a MISMATCH on evidence that was
    perfectly good. A gate whose first three findings are about itself is a gate that was
    measuring the wrong thing, which is the defect this whole module exists to catch one level
    down.

    `claim_ids` given explicitly overrides `NOT_BY_DEFAULT`: asking for SG-07 by name on a
    machine with a JVM is a reasonable thing to want, and the policy is a default rather than
    a prohibition.
    """
    head = head_sha or current_commit_sha()
    explicit = claim_ids is not None
    wanted = list(claim_ids) if claim_ids is not None else sorted(latest)
    reproduced: list[str] = []
    mismatches: list[Mismatch] = []
    skipped: list[NotRecomputed] = []

    for claim_id in wanted:
        record = latest.get(claim_id)
        if record is None:
            skipped.append(NotRecomputed(claim_id, "there is no evidence for it at all"))
            continue
        if not explicit and claim_id in NOT_BY_DEFAULT:
            skipped.append(NotRecomputed(claim_id, NOT_BY_DEFAULT[claim_id]))
            continue

        runs = record.get("verdict", {}).get("runs", {})
        sha = str(runs.get("commit_sha", ""))
        purpose = str(runs.get("seed_purpose", ""))
        seeds = list(runs.get("seeds", []))
        source = str(runs.get("seed_source", ""))
        published = _rate(record)

        if source != "commit":
            skipped.append(
                NotRecomputed(claim_id, f"its seeds are {source!r}, so nothing can redraw them")
            )
            continue
        if published is None:
            skipped.append(
                NotRecomputed(claim_id, "it publishes a bound rather than a rate; see CLAIMS.md")
            )
            continue

        # Defence in depth, and it costs a hash: the store checks this on append, and a check
        # that only ever runs on append is a check that a file edited afterwards walks past.
        if seeds != seeds_from_commit(len(seeds), purpose, sha=sha):
            mismatches.append(
                Mismatch(
                    claim_id,
                    "seed list",
                    str(seeds),
                    str(seeds_from_commit(len(seeds), purpose, sha=sha)),
                    f"the record's seeds do not derive from commit {sha[:12]} for purpose "
                    f"{purpose!r}, so it names a run nobody can reproduce.",
                )
            )
            continue

        moved = code_moved_since(repo_root, sha, head)
        if moved:
            shown = ", ".join(moved[:4]) + (" ..." if len(moved) > 4 else "")
            skipped.append(
                NotRecomputed(
                    claim_id,
                    f"the code under it moved since {sha[:12]} ({shown}), so a different "
                    f"answer would be a different measurement rather than a disagreement. "
                    f"Run `make evidence` to re-measure it.",
                )
            )
            continue

        fresh = run_claim(claim_id, record)
        got = _rate(fresh)
        if got is None:
            mismatches.append(
                Mismatch(
                    claim_id,
                    "rate",
                    _shape(published),
                    "no rate at all",
                    "re-running the claim produced no rate, so the published one came from "
                    "somewhere this command cannot reach.",
                )
            )
            continue
        if (got.get("successes"), got.get("trials")) != (
            published.get("successes"),
            published.get("trials"),
        ):
            mismatches.append(
                Mismatch(
                    claim_id,
                    "rate",
                    _shape(published),
                    _shape(got),
                    f"re-run from the seeds the record names ({seeds[0]}...) at commit "
                    f"{sha[:12]}, with the code in this tree unchanged since then.",
                )
            )
            continue
        reproduced.append(claim_id)

    return Reproduction(tuple(reproduced), tuple(mismatches), tuple(skipped))


# ------------------------------------------------------------------ the cheap half


def _positive_int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _from_counted_list(record: Mapping[str, Any], key: str) -> tuple[int, int] | None:
    values = record.get("artifacts", {}).get(key)
    if not isinstance(values, list):
        return None
    return (len(values), len(values))


def _sg00(record: Mapping[str, Any]) -> tuple[int, int] | None:
    a = record.get("artifacts", {})
    passed, failed = _positive_int(a.get("tests_passed")), _positive_int(a.get("tests_failed"))
    if passed is None or failed is None:
        return None
    return (passed, passed + failed)


def _sg01(record: Mapping[str, Any]) -> tuple[int, int] | None:
    comparisons = _positive_int(record.get("artifacts", {}).get("comparisons"))
    return None if comparisons is None else (comparisons, comparisons)


def _sg03(record: Mapping[str, Any]) -> tuple[int, int] | None:
    """The mutation score, from the three numbers the campaign publishes beside it.

    `trials` is every mutant that was SCORED, which is the total minus the ones classified
    equivalent, and `successes` is how many the ledger witness killed. The forgery that
    prompted all of this set the rate to 999/999 and left `mutants_total: 94`,
    `equivalent: 27` and `per_witness.ledger: 67` exactly where they were.
    """
    a = record.get("artifacts", {})
    total = _positive_int(a.get("mutants_total"))
    equivalent = _positive_int(a.get("equivalent"))
    per_witness = a.get("per_witness")
    killed = _positive_int(per_witness.get("ledger")) if isinstance(per_witness, dict) else None
    if total is None or equivalent is None or killed is None:
        return None
    return (killed, total - equivalent)


def _sg04(record: Mapping[str, Any]) -> tuple[int, int] | None:
    moved = record.get("artifacts", {}).get("months_that_moved")
    return None if not isinstance(moved, list) else (len(moved), len(moved))


def _sg06(record: Mapping[str, Any]) -> tuple[int, int] | None:
    a = record.get("artifacts", {})
    verified, breaks = _positive_int(a.get("records_verified")), a.get("chain_breaks")
    if verified is None or not isinstance(breaks, list):
        return None
    return (verified - len(breaks), verified)


def _sg07(record: Mapping[str, Any]) -> tuple[int, int] | None:
    a = record.get("artifacts", {})
    injected, divergences = _positive_int(a.get("injected_runs")), a.get("divergences")
    if injected is None or not isinstance(divergences, list):
        return None
    return (injected - len(divergences), max(1, injected))


def _from_checks(record: Mapping[str, Any]) -> tuple[int, int] | None:
    a = record.get("artifacts", {})
    checks, failed = a.get("checks"), a.get("checks_failed")
    if not isinstance(checks, list) or not isinstance(failed, list):
        return None
    return (len(checks) - len(failed), len(checks))


def _runs_n(record: Mapping[str, Any]) -> tuple[int, int] | None:
    n = _positive_int(record.get("verdict", {}).get("runs", {}).get("n"))
    return None if n is None else (n, n)


#: One rule per claim: what the record's own artifacts say its rate must be.
#:
#: Every claim id has an entry, and `tests/fast/test_reproduce.py` fails when a new claim
#: appears without one - because the failure mode this table is built against is a check whose
#: coverage is believed rather than computed. An entry may legitimately answer None when the
#: record is too old to carry the artifact the rule reads; a None is reported as unchecked,
#: never as agreement.
RATE_RULES: dict[str, Callable[[Mapping[str, Any]], tuple[int, int] | None]] = {
    "SG-00": _sg00,
    "SG-01": _sg01,
    # SG-02 publishes only "files_duplicated": the trial count is the number of runs, which is
    # what the claim iterates. Weaker than the others and worth saying so.
    "SG-02": _runs_n,
    "SG-03": _sg03,
    "SG-04": _sg04,
    "SG-05": lambda record: _from_counted_list(record, "scd2_digests"),
    "SG-06": _sg06,
    "SG-07": _sg07,
    "SG-08": _from_checks,
    "SG-09": _from_checks,
}


def rate_against_artifacts(record: Mapping[str, Any]) -> tuple[Mismatch | None, bool]:
    """Whether a record's rate is the one its own artifacts imply. No run, no network.

    This is the half of the gate that catches the forgery in the fast lane. The attack moved
    `verdict.rate` to 999/999 and left `mutants_total: 94` and `equivalent: 27` in place two
    lines below it, because rewriting the artifacts consistently is a great deal more work
    than rewriting one number - and because the artifacts are what the prose in CLAIMS.md and
    FINDINGS.md is written from, so a forger who changes them has to change the prose too.

    Returns `(mismatch_or_None, checked)`. The second value exists because "no mismatch" and
    "nothing was compared" are different answers, and collapsing them is the exact defect this
    module was written against: a check that inspects nothing and reports agreement.
    """
    claim_id = str(record.get("claim_id", ""))
    rule = RATE_RULES.get(claim_id)
    published = _rate(record)
    if rule is None or published is None:
        return (None, False)
    expected = rule(record)
    if expected is None:
        return (None, False)
    if (published.get("successes"), published.get("trials")) != expected:
        return (
            Mismatch(
                claim_id,
                "rate",
                _shape(published),
                f"{expected[0]}/{expected[1]}",
                "the record's own artifacts imply a different number from the one it "
                "publishes. Either the rate was written by something other than the run that "
                "produced the artifacts, or the artifacts are from a different run.",
            ),
            True,
        )
    return (None, True)


def arithmetic_mismatches(
    latest: Mapping[str, Mapping[str, Any]], claim_ids: Iterable[str] | None = None
) -> tuple[tuple[Mismatch, ...], tuple[str, ...]]:
    """Every rate that disagrees with its own artifacts, and the claims no rule could check."""
    mismatches: list[Mismatch] = []
    unchecked: list[str] = []
    for claim_id in sorted(claim_ids if claim_ids is not None else latest):
        record = latest.get(claim_id)
        if record is None:
            continue
        found, checked = rate_against_artifacts(record)
        if found is not None:
            mismatches.append(found)
        if not checked:
            unchecked.append(claim_id)
    return tuple(mismatches), tuple(unchecked)

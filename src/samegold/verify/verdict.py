"""Typed verdicts. There is no way to state a result without stating how it was obtained.

The rule the whole project hangs on: **no number reaches the README except through one of
these types**. A ``Rate`` cannot be built from a float, only from counts; a ``Pass`` cannot
be built without the ``RunSet`` that produced it; a ``Fail`` cannot be built without a
counterexample. An agent - or a tired author at 2am - cannot improve a figure by editing
the report, because the report is rendered from these objects and a test recomputes them.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Literal

from samegold.verify.stats import rule_of_three_upper, wilson_interval


@dataclass(frozen=True, slots=True)
class RunSet:
    """The experiment behind a number."""

    n: int
    seeds: tuple[int, ...]
    commit_sha: str
    seed_source: Literal["commit", "override"]
    seed_purpose: str
    profile: str
    started_at: str
    duration_s: float
    runtime: Literal["oss-local", "oss-ci", "databricks-free"]
    # The git TREE the run actually executed, and whether it differed from the commit's.
    #
    # The commit sha alone anchors the seeds and nothing else, and an adversarial review
    # showed what that leaves open with three examples out of this repository's own history:
    # SG-05 recorded 0/3 and then 3/3 thirty seconds later at one commit; SG-03's DENOMINATOR
    # moved from 49 scored mutants to 48 at one commit; SG-07 went from fail to pass at one
    # commit by being re-run with a different --repetitions. All three are legitimate - they
    # are what fixing a bug and re-measuring looks like - and all three were indistinguishable
    # from retry-until-green, because nothing in the record said which CODE had run.
    #
    # The tree hash says. It is computed from the working tree at run time, so a record made
    # on a dirty tree names a tree that is in no commit, and the renderer labels it. That does
    # not stop anyone re-running until green; it stops them doing it invisibly.
    tree_sha: str = ""
    tree_dirty: bool = False

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError("a RunSet with no runs is not evidence")
        if len(self.seeds) != self.n:
            raise ValueError(
                f"RunSet claims n={self.n} but carries {len(self.seeds)} seeds; "
                f"every run must name the seed it used"
            )

    def to_json(self) -> dict[str, Any]:
        return {
            "n": self.n,
            "seeds": list(self.seeds),
            "commit_sha": self.commit_sha,
            "tree_sha": self.tree_sha,
            "tree_dirty": self.tree_dirty,
            "seed_source": self.seed_source,
            "seed_purpose": self.seed_purpose,
            "profile": self.profile,
            "started_at": self.started_at,
            "duration_s": round(self.duration_s, 3),
            "runtime": self.runtime,
        }


@dataclass(frozen=True, slots=True)
class Rate:
    """A proportion that carries its counts and its interval, or it does not exist."""

    successes: int
    trials: int

    def __post_init__(self) -> None:
        if self.trials <= 0:
            raise ValueError("a rate over zero trials is not a rate")
        if not 0 <= self.successes <= self.trials:
            raise ValueError(f"successes={self.successes} outside 0..{self.trials}")

    @property
    def point(self) -> float:
        return self.successes / self.trials

    @property
    def interval(self) -> tuple[float, float]:
        return wilson_interval(self.successes, self.trials)

    @property
    def upper_bound_if_zero(self) -> float | None:
        """Rule-of-three bound, defined only when nothing was observed."""
        if self.successes != 0:
            return None
        return rule_of_three_upper(self.trials)

    def to_json(self) -> dict[str, Any]:
        lo, hi = self.interval
        out: dict[str, Any] = {
            "successes": self.successes,
            "trials": self.trials,
            "point": round(self.point, 6),
            "wilson95": [round(lo, 6), round(hi, 6)],
        }
        if self.successes == 0:
            out["rule_of_three_upper95"] = round(rule_of_three_upper(self.trials), 6)
        return out

    def render(self) -> str:
        lo, hi = self.interval
        return f"{self.successes}/{self.trials} (95% CI {lo:.1%}-{hi:.1%})"


@dataclass(frozen=True, slots=True)
class Counterexample:
    """What broke, small enough to act on."""

    claim_id: str
    seed: int
    description: str
    payload: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "seed": self.seed,
            "description": self.description,
            "payload": self.payload,
        }


@dataclass(frozen=True, slots=True)
class Pass:
    claim_id: str
    runs: RunSet
    rate: Rate | None = None
    note: str = ""

    ok: bool = field(default=True, init=False)

    def to_json(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "outcome": "pass",
            "runs": self.runs.to_json(),
            "rate": self.rate.to_json() if self.rate else None,
            "note": self.note,
        }


@dataclass(frozen=True, slots=True)
class Fail:
    claim_id: str
    runs: RunSet
    counterexample: Counterexample
    rate: Rate | None = None

    ok: bool = field(default=False, init=False)

    def to_json(self) -> dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "outcome": "fail",
            "runs": self.runs.to_json(),
            "counterexample": self.counterexample.to_json(),
            "rate": self.rate.to_json() if self.rate else None,
        }


Verdict = Pass | Fail


def now_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat(timespec="seconds")

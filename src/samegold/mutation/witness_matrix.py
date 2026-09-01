"""Which witness killed which mutant, and what that says about their independence.

The number a project like this is tempted to publish is a single mutation score. That number
hides the only interesting question: is the second implementation adding anything, or is it
agreeing with the first for the same reasons?

So the matrix reports, per witness: mutants killed, mutants killed *only* by it (its
marginal value), and pairwise Cohen's kappa. A kappa above 0.8 between two witnesses is
reported as "one witness wearing two hats", in the README, in those words.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from samegold.verify.stats import cohen_kappa, wilson_interval


@dataclass
class WitnessMatrix:
    witnesses: tuple[str, ...]
    killed: dict[str, set[str]] = field(default_factory=dict)
    mutants: list[str] = field(default_factory=list)
    equivalent: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for witness in self.witnesses:
            self.killed.setdefault(witness, set())

    def record(
        self, mutant_id: str, killed_by: list[str], equivalent_reason: str | None = None
    ) -> None:
        if mutant_id not in self.mutants:
            self.mutants.append(mutant_id)
        if equivalent_reason:
            self.equivalent[mutant_id] = equivalent_reason
            return
        for witness in killed_by:
            self.killed.setdefault(witness, set()).add(mutant_id)

    @property
    def scored_mutants(self) -> list[str]:
        """Equivalent mutants are excluded from the score and listed separately.

        Counting them as survivors would understate the gate; counting them as kills would
        be a lie. Naming them is the only honest option, and it is also the only one that
        lets a reader check the classification.
        """
        return [m for m in self.mutants if m not in self.equivalent]

    def survivors(self) -> list[str]:
        killed_any = set().union(*self.killed.values()) if self.killed else set()
        return [m for m in self.scored_mutants if m not in killed_any]

    def marginal(self, witness: str) -> list[str]:
        others = (
            set().union(*(v for k, v in self.killed.items() if k != witness))
            if self.killed
            else set()
        )
        return sorted(self.killed.get(witness, set()) - others)

    def kappa(self, a: str, b: str) -> float:
        scored = set(self.scored_mutants)
        ka, kb = self.killed.get(a, set()) & scored, self.killed.get(b, set()) & scored
        both = len(ka & kb)
        only_a = len(ka - kb)
        only_b = len(kb - ka)
        neither = len(scored) - both - only_a - only_b
        return cohen_kappa(both, only_a, only_b, neither)

    def to_json(self) -> dict[str, Any]:
        scored = self.scored_mutants
        killed_any = set().union(*self.killed.values()) if self.killed else set()
        killed_count = len([m for m in scored if m in killed_any])
        lo, hi = wilson_interval(killed_count, len(scored)) if scored else (0.0, 0.0)
        pairs = {}
        for i, a in enumerate(self.witnesses):
            for b in self.witnesses[i + 1 :]:
                pairs[f"{a}|{b}"] = round(self.kappa(a, b), 4)
        return {
            "mutants_total": len(self.mutants),
            "mutants_scored": len(scored),
            "equivalent": self.equivalent,
            "killed": killed_count,
            "score": round(killed_count / len(scored), 4) if scored else 0.0,
            "wilson95": [round(lo, 4), round(hi, 4)],
            "survivors": self.survivors(),
            "per_witness": {
                w: {
                    "killed": len(self.killed.get(w, set()) & set(scored)),
                    "marginal": self.marginal(w),
                }
                for w in self.witnesses
            },
            "kappa": pairs,
        }

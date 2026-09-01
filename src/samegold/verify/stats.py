"""The two intervals this project is allowed to publish, and nothing else.

Both are here because the alternative - a bare percentage - is the single most common way
a portfolio project overclaims. "The gate catches 13 of 15" is not a number you can act on;
[0.64, 0.96] is.
"""

from __future__ import annotations

import math


def wilson_interval(
    successes: int, trials: int, z: float = 1.959963984540054
) -> tuple[float, float]:
    """Wilson score interval for a binomial proportion (95% by default).

    Wilson rather than the normal approximation because the counts here are small and the
    proportions are near 1, exactly where the normal interval leaves the unit interval and
    stops meaning anything.
    """
    if trials <= 0:
        raise ValueError("wilson_interval needs at least one trial")
    if not 0 <= successes <= trials:
        raise ValueError(f"successes={successes} outside 0..{trials}")
    p = successes / trials
    denom = 1 + z * z / trials
    centre = (p + z * z / (2 * trials)) / denom
    half = z * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials)) / denom
    return (max(0.0, centre - half), min(1.0, centre + half))


def rule_of_three_upper(trials: int, confidence: float = 0.95) -> float:
    """Upper bound on an event rate after ``trials`` trials with zero occurrences.

    Zero failures in n runs does not mean the failure rate is zero; it means it is at most
    -ln(1-c)/n. With 140 runs and 95% confidence that is 2.1%, and that is the number this
    project publishes instead of the word "always".
    """
    if trials <= 0:
        raise ValueError("rule_of_three_upper needs at least one trial")
    if not 0 < confidence < 1:
        raise ValueError("confidence must be in (0, 1)")
    return -math.log(1 - confidence) / trials


def cohen_kappa(both: int, only_a: int, only_b: int, neither: int) -> float:
    """Agreement between two witnesses beyond chance.

    Used by the witness matrix: if two witnesses agree at kappa > 0.8 on which mutants they
    kill, the project has one witness wearing two hats and says so.
    """
    n = both + only_a + only_b + neither
    if n == 0:
        raise ValueError("cohen_kappa needs at least one observation")
    po = (both + neither) / n
    pa1, pb1 = (both + only_a) / n, (both + only_b) / n
    pe = pa1 * pb1 + (1 - pa1) * (1 - pb1)
    if pe == 1.0:
        return 1.0
    return (po - pe) / (1 - pe)

"""What may be published: the claim ids, their titles, and the seed streams that exist.

This lives in the evidence layer rather than in ``claims.py`` for two reasons, one structural
and one about what the file is for.

Structural: ``evidence/store.py`` has to check a record against it, and the architecture test
in tests/fast forbids the evidence package from importing the claim IMPLEMENTATIONS. A
registry is not an implementation.

And about what it is for: this is the list a gate consults, not a convenience for the code
that produces records. Keeping it here makes the direction of the dependency say the right
thing - a claim is published because the registry admits it, not because a function exists.

"""

from __future__ import annotations

# Two things live here rather than only inside the functions that build the records: the
# TITLE, so a record cannot rename its own claim on the way to the results table, and the
# SEED PURPOSE, so the label of a seed stream is not the author's to invent. A review
# appended two hundred accepted records at one commit by renaming the purpose two hundred
# times; deriving the numbers from the commit only stops the NUMBERS being chosen.
CLAIM_TITLES: dict[str, str] = {
    "SG-00": "what this repository contains, counted",
    "SG-01": "two implementations agree on the close",
    "SG-02": "re-delivery under a new path is a no-op",
    "SG-03": "mutation campaign",
    "SG-04": "a closed month moves after it is closed",
    "SG-05": "dimension and conservation invariants hold without an oracle",
    "SG-06": "the evidence chain verifies and every seed derives from its commit",
    "SG-07": "the silver writer survives a crash at each of its structural points",
    "SG-08": "no direct identifier reaches gold, and a purge really purges",
    "SG-09": "what layout costs, in files and bytes",
}

# Every stream this repository draws from, including the three that never reach a published
# record: `samegold demo`, `samegold generate` and `samegold report` each draw a seed, and the
# module docstring calls this "the seed streams that exist". A list that omits three of them
# is a list that says something false about itself.
SEED_PURPOSES: tuple[str, ...] = (
    "demo",
    "generator",
    "report",
    "facts",
    "witness",
    "redelivery",
    "mutation",
    "restatement",
    "invariants",
    "provenance",
    "faults",
    "privacy",
    "cost",
)

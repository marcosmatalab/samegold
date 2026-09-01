"""The cost lab: the experiments, their controls, and what each one is allowed to conclude.

Written before the measurements exist, on purpose. A performance experiment designed after
seeing the numbers is a story; designed before, it is a measurement. Each entry names what
changes, what must stay fixed, how many repetitions, and - the part that is usually missing -
what the result may NOT be attributed to.

The metric is deliberately not "it got faster". Wall time on a laptop measures the laptop.
The measured quantities are physical and reproducible: bytes read, files read, output file
count, shuffle bytes spilled. DBU cost is not measurable for free (``system.billing`` needs
account-admin), so it is not claimed at all rather than approximated with wall time.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Experiment:
    experiment_id: str
    question: str
    treatment: str
    control: str
    metric: str
    repetitions: int
    not_attributable: str


EXPERIMENTS: tuple[Experiment, ...] = (
    Experiment(
        "COST-01",
        "How much does compacting the ~1 200 tiny bronze files cost and save?",
        "OPTIMIZE the bronze table",
        "same data, same query, same cluster, cold cache before every run",
        "files read and bytes read for a fixed query",
        5,
        "any improvement is file sizing, not clustering: no clustering column changes here",
    ),
    Experiment(
        "COST-02",
        "Does liquid clustering on (accounting_month, sku) beat partitioning by month?",
        "CLUSTER BY (accounting_month, sku) versus PARTITIONED BY (accounting_month)",
        "identical row content, identical target file size, identical query set",
        "bytes read per query, and file count",
        5,
        "the comparison is confounded by file size unless target file size is pinned in both "
        "arms; it is, and the pinned value is recorded with the result",
    ),
    Experiment(
        "COST-03",
        "What do deletion vectors cost on the read side after a large DELETE?",
        "deletion vectors enabled versus a rewrite",
        "same deleted row set, same query",
        "bytes read, plus the row-level filtering time from the query plan",
        5,
        "not a statement about write cost, which moves the other way",
    ),
    Experiment(
        "COST-04",
        "How much does the as-of cut benefit from data skipping?",
        "z-order / cluster on arrival_ts versus no ordering",
        "same query, same file size target",
        "bytes read for a close at a given as-of",
        5,
        "not attributable to statistics collection alone: both arms collect statistics",
    ),
)

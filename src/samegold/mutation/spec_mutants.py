"""Specification mutants: the ones a generator cannot invent.

Each of these changes what the pipeline is *supposed* to do, not how it does it. They are
the only experiment in the repository capable of falsifying its own independence claim:
a specification mutant that the DuckDB witness survives is a mutant where the witness
inherited the author's misunderstanding, and the README says so with the number attached.

They are written as textual substitutions over the reference SQL because that is the form
in which they can be applied identically to both implementations - the Spark pipeline runs
the same substitutions over its own SQL, see pipelines/gold_revenue.py.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SpecMutant:
    mutant_id: str
    rule: str
    rationale: str
    find: str
    replace: str

    def apply(self, sql: str) -> str:
        if self.find not in sql:
            raise ValueError(
                f"{self.mutant_id}: anchor not found in the SQL; the reference changed and "
                f"this mutant no longer applies. Fix the anchor rather than deleting the mutant."
            )
        return sql.replace(self.find, self.replace, 1)


SPEC_MUTANTS: tuple[SpecMutant, ...] = (
    SpecMutant(
        "SPEC-01",
        "A return is imputed to the month of the return, not of the sale",
        "The single most consequential rule in the domain. If both implementations get it "
        "wrong the same way, every diff is green and the close is wrong by the same amount "
        "in both. This mutant measures exactly that.",
        # The anchor is the shortest text that appears exactly once: the refunds CTE is the
        # only place that groups the RETURNS table by the month of the sale.
        find="""SUM(qty * unit_price_cents) AS returns_cents,
           COUNT(*)                     AS return_count
    FROM returns""",
        replace="""SUM(qty * unit_price_cents) AS returns_cents,
           COUNT(*)                     AS return_count
    FROM (SELECT * REPLACE (return_ts AS sale_ts) FROM returns)""",
    ),
    SpecMutant(
        "SPEC-02",
        "The return window is 60 days instead of 45",
        "A contract change that looks like a tuning parameter. Catches whether the window "
        "is actually enforced anywhere rather than being documentation.",
        find="INTERVAL 45 DAY",
        replace="INTERVAL 60 DAY",
    ),
    SpecMutant(
        "SPEC-03",
        "Deduplication keys on (event_id, file path) instead of event_id",
        "Turns re-delivery of identical content under a new path into double counting. "
        "This is the bug that shows up as a 12% revenue overstatement after a producer "
        "replays a day, and the one an idempotency test is supposed to exist for.",
        find="SELECT *, row_number() OVER (PARTITION BY event_id",
        replace="SELECT *, row_number() OVER (PARTITION BY event_id, filename",
    ),
    SpecMutant(
        "SPEC-04",
        "The as-of cut uses event time instead of arrival time",
        "Makes the close depend on when things happened rather than on when they were "
        "known, which quietly gives the close perfect foresight and erases every "
        "restatement. A month would never reopen and the bitemporal model would look "
        "unnecessary - a very comfortable bug.",
        find="AND CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)",
        replace="AND CAST(event_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)",
    ),
    SpecMutant(
        "SPEC-05",
        "An amendment does not replace the quantity, it adds to it",
        "Plausible reading of 'amended', catastrophic in cents.",
        find="COALESCE(a.qty, l.qty0) AS qty",
        replace="COALESCE(a.qty + l.qty0, l.qty0) AS qty",
    ),
    SpecMutant(
        "SPEC-06",
        "Quantity reported at a close is the final quantity, not the one known then",
        "Assumes perfect foresight about amendments still in flight. Half the restatements "
        "disappear and the close looks stable.",
        find="""      AND CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ)""",
        replace="""      AND (event_type = 'order_line_amended'
           OR CAST(arrival_ts AS TIMESTAMPTZ) <= CAST($as_of AS TIMESTAMPTZ))""",
    ),
)

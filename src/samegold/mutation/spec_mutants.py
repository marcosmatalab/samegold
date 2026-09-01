"""Specification mutants: the ones a generator cannot invent.

Each of these changes what the pipeline is *supposed* to do, not how it does it. They are
the only experiment in the repository capable of falsifying its own independence claim:
a specification mutant that the DuckDB witness survives is a mutant where the witness
inherited the author's misunderstanding, and the README says so with the number attached.

They are written as textual substitutions over the reference SQL because a change of meaning
cannot be generated: no operator knows that "a return belongs to the month of the sale" is a
rule. The Spark implementation carries the same rules in `src/samegold/pipelines/transform.py`,
and SG-01 is what would notice if the two ever stopped agreeing.
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
        """Apply the substitution, refusing to mutate a comment.

        The refusal is not theoretical. When the return window moved from `INTERVAL 45 DAY`
        to a comparison in seconds, the old anchor survived only inside the comment that
        explained the change, so the mutant applied cleanly, changed nothing executable, and
        was reported as a SURVIVING specification mutant. A mutant that edits prose is worse
        than no mutant: it looks like a finding.
        """
        if self.find not in sql:
            raise ValueError(
                f"{self.mutant_id}: anchor not found in the SQL; the reference changed and "
                f"this mutant no longer applies. Fix the anchor rather than deleting the mutant."
            )
        code = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))
        if self.find not in code:
            raise ValueError(
                f"{self.mutant_id}: the anchor only appears inside a SQL comment, so this "
                f"mutant would change nothing executable. Re-anchor it on real code."
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
        find=(
            "    SELECT strftime(sale_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m')"
            " AS accounting_month,\n           SUM(return_qty * unit_price_cents) AS returns_cents,"
        ),
        replace=(
            "    SELECT strftime(return_ts AT TIME ZONE 'Europe/Madrid', '%Y-%m')"
            " AS accounting_month,\n           SUM(return_qty * unit_price_cents) AS returns_cents,"
        ),
    ),
    SpecMutant(
        "SPEC-02",
        "The return window is 60 days instead of 45",
        "A contract change that looks like a tuning parameter. Catches whether the window "
        "is actually enforced anywhere rather than being documentation.",
        find="45 * 86400",
        replace="60 * 86400",
    ),
    SpecMutant(
        "SPEC-03",
        "Deduplication keys on (event_id, file path) instead of event_id",
        "Turns re-delivery of identical content under a new path into double counting. "
        "This is the bug that shows up as a 12% revenue overstatement after a producer "
        "replays a day, and the one an idempotency test is supposed to exist for.",
        find="PARTITION BY event_id",
        replace="PARTITION BY event_id, filename",
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

# Claims

One section per claim: what was measured, how, and — the part that matters — what the result
does **not** show. Numbers are rendered from `evidence/history.jsonl`, whose records are
hash-chained and seed-derived; a hand-edited figure fails a test.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 304/304 (95% CI 98.8%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-03` mutation campaign | PASS | 48/48 (95% CI 92.6%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-06` the evidence chain verifies and every seed derives from its commit | PASS | 8/8 (95% CI 67.6%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-07` the close survives a crash at each structural point | PASS | 20/20 (95% CI 83.9%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-08` no direct identifier reaches gold, and a purge really purges | PASS | 6/6 (95% CI 61.0%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-09` what layout costs, in files and bytes | PASS | 5/5 (95% CI 56.6%-100.0%) | oss-local | local run, not reproduced in CI |

<!-- samegold:end claims -->

---

## SG-00 — what this repository contains, counted

**Experiment.** Collect the tests per lane, run the fast lane, count modules and lines.
Every count printed in a document renders through an evidence anchor, so "127 tests" cannot
survive the day it stops being true. It stopped being true within a week of being written.

**Does not show** anything about quality. Lines of code are a fact about a repository, not a
virtue.

## SG-01 — two implementations agree on the versioned close

**Experiment.** For each commit-derived seed, the generator writes a dataset and its ledger.
At every close, `gold.revenue_by_month` is computed by the DuckDB reference and compared with
the ledger, key by key, in cents. Result: <!--sg:SG-01.rate-->15/15 (95% CI 79.6%-100.0%)<!--/sg-->.
The Spark lane runs the same comparison over the whole **version history**, not over a single
snapshot: `tests/spark/test_transform_matches_reference.py`.

**What it found.** Two real defects, both invisible to any single-implementation test: a
window measured with `unix_timestamp` that truncated to whole seconds and accepted a return a
microsecond outside it, and a window measured with `INTERVAL 45 DAY` that changes length
across a daylight-saving boundary in the accounting timezone.

**Does not show** that either is correct. They share an author and a contract, so a misreading
lands in both. Knight and Leveson measured that effect in 1986 with 27 independently written
versions and found failure coincidence far above chance; two versions by one author are not
more independent than that. The number that prices this blind spot is SG-03, not this one.

## SG-02 — re-delivery under a new path is a no-op

**Experiment.** Compute the close, copy every bronze file to a second path, recompute, compare
canonical digests. Result: <!--sg:SG-02.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg-->.

**Does not show** exactly-once processing. This is at-least-once delivery plus content-keyed
deduplication, a different and weaker property. It also says nothing about a duplicate
arriving after a streaming watermark has expired the state that would recognise it: silver is
append-only and may hold duplicates, uniqueness is enforced at the gold boundary, and the size
of the effect is a milestone (M14), not a claim.

## SG-03 — mutation campaign

**Experiment.** Mutants generated mechanically from the SQL AST (comparison swaps, join kind
swaps, numeric literal bumps, aggregate swaps, coalesce removal, order flips) and run past
three witnesses: the ledger, the invariants and the runtime. Six **specification** mutants are
added by hand and labelled as such, because no generator knows that "a return belongs to the
month of the sale" is a rule.
Result: <!--sg:SG-03.rate-->48/48 (95% CI 92.6%-100.0%)<!--/sg--> of the scored mutants killed; strict score
<!--sg:SG-03.artifact.strict_score-->0.7059<!--/sg--> if the equivalence classification is refused
wholesale.

**Three things this campaign changed about the project:**

1. **Eight mutants were being killed by the SQL parser.** The comparison operator inside
   `read_json(format = 'newline_delimited')` parses as an equality; mutating it produced a
   binder error that the campaign counted as a kill. Named arguments are no longer mutated.
2. **Three more were being killed by the harness crashing.** Removing a `COALESCE` made a
   column NULL and the result mapper called `int(None)`. A NULL is a value now, and the ledger
   is what kills those mutants, for the right reason.
3. **Four row-SELECTING mutants were classified as harmless.** One equivalence entry matched
   every `order:flip` mutant regardless of where it lived, and filed flips inside the
   deduplication window under "row order does not matter". Equivalence is now keyed by the CTE
   the mutation lives in, and there are no wildcards.

**The negative control for the classification.** Each equivalence carries an assumption id,
and `mutation/assumption_probe.py` builds datasets that VIOLATE the assumption and checks that
the covered mutants stop being equivalent. Mutants the probe cannot falsify are published as
unfalsified rather than quietly kept.

**Does not show** correctness. A mutation score is a lower bound on what a suite can see.

## SG-04 — a closed month moves after it is closed

**Experiment.** For every month closed at least twice, compare the net revenue at its own
close (day 5 of the following month) with its final value.
Result: <!--sg:SG-04.rate-->2/2 (95% CI 34.2%-100.0%)<!--/sg--> of closed months moved, worst
<!--sg:SG-04.artifact.worst_move_pct-->6.3652<!--/sg-->%.

**Does not show** anything about real retail: the rates are set high so the rare paths appear.

## SG-05 — invariants hold with no oracle involved

**Experiment.** SCD2 intervals disjoint, contiguous and with exactly one open row per
customer; `net = gross − returns`; close versions dense and `restated_at` monotonic; no month
refunding more than it sold; conservation of every ingested row.
Result: <!--sg:SG-05.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg-->.

**Does not show** that the values are right — an invariant sees shape. And on the published
campaign the invariants killed **zero** mutants the ledger had not already killed. They are
kept because in production there is no ledger and shape is all there is.

## SG-06 — the evidence chain verifies and every seed derives from its commit

**Experiment.** Recompute the hash chain over `evidence/history.jsonl` and re-derive the seeds
of every record from the commit it names. The count is the records present **at the moment
this claim ran**: SG-06 is ordered last in `ALL_CLAIMS` so a full `samegold evidence` covers
everything before it, but a later single-claim run (`make faults`, a re-run of SG-00) appends
after it and is therefore not in this number. Result: <!--sg:SG-06.rate-->8/8 (95% CI 67.6%-100.0%)<!--/sg-->.

**Why it exists.** The first version of this claim recomputed the seeds and compared them with
themselves; it passed on a repository whose evidence had been forged by appending two lines to
a JSON file. This version verifies the artefact rather than the function.

**Does not show** that a record marked as produced in CI really was. Nothing offline can check
that a run URL exists; the gate checks the shape and the commit, and the renderer prints
anything without one as a local run.

## SG-07 — the silver writer survives a crash at each of its structural points

**Experiment.** For each structural point of the SILVER writer, kill the process with
`os._exit` inside `foreachBatch`, restart from the checkpoint, and compare two digests of
silver: the deduplicated content, and the multiset of copies per event.

**Scope, exactly.** `faults/points.py` enumerates four reachable points, two in the silver
stage and two in the gold stage. The campaign injects at the **two silver ones**; the record
carries `reachable_points_not_covered` so the gap is in the evidence rather than in a reader's
assumption. The claim used to say "each structural point", which was false by half.

**The negative control is what makes the claim falsifiable.** The same campaign runs against a writer that appends
instead of overwriting — the hopeful version most pipelines ship. The content digest does
**not** move (it deduplicates, so it is blind to a double write) and the multiset digest does.
If the control is ever undetected, the claim fails: a crash test that cannot fail is a
screenshot. That blindness was found by an adversarial review copying a batch directory and
watching the number stay still.

**Does not show** crash safety of the engine, and does not cover the gold writer. The points
inside a Delta commit, a state-store checkpoint or a multi-part object-storage commit are
listed with `reachable=False`: reaching them means instrumenting the engine, at which point
the program under test is not the program that gets deployed. The two gold-stage points are
reachable and simply not yet exercised, which is a smaller and more embarrassing gap, and is
published as one.

## SG-08 — no direct identifier reaches gold, and a purge really purges

**Experiment.** Three controls, executed: the column policy masks every direct identifier on
the way into gold; the exposure check refuses gold rows carrying one anyway, including one
hiding under a different column name; the retention purge deletes expired rows **and** vacuums
the files that held them. Result: <!--sg:SG-08.rate-->6/6 (95% CI 61.0%-100.0%)<!--/sg-->.

**The part worth reading twice.** On a lakehouse a `DELETE` does not delete: the rows remain
in the previous version and time travel returns them until `VACUUM` removes the files. A purge
that stops at the `DELETE` does not meet a retention policy, and the test fails if it does.

**Does not show** platform enforcement. Free Edition has no account groups, so a row filter on
`is_account_group_member` is a policy nobody is subject to. The controls run in code here, and
the Databricks lane declares the equivalent in SQL for a workspace that has groups.

## SG-09 — what layout costs, in files and bytes

**Experiment.** Four experiments on real Delta tables through delta-rs, measured from the
per-file statistics in the Delta log rather than from a query plan or a clock:
compaction, clustering at two file sizes, partitioning versus clustering on two predicates,
and the copy cost of a delete. Result: <!--sg:SG-09.rate-->5/5 (95% CI 56.6%-100.0%)<!--/sg-->.

**One of the five checks is a negative result, and it has to pass.** Clustering by
(month, sku) does nothing for a sku predicate when the clustered table has two files, because two
files cover the whole key range; at a smaller target size it cuts the share of the table that
must be read by <!--sg:SG-09.artifact.share_read_reduction_pct-->78.25<!--/sg-->%. The headline is
a share and not a byte ratio because Z-ORDER rewrites and recompresses, and a byte ratio takes
credit for that too: an adversarial review caught exactly that arithmetic.

The file counts are reproducible run to run; the byte counts are not, because the parquet
writer is not. The published figure is therefore the SHARE, measured over repetitions, and
`share_read_reduction_pct_range` in the record carries its range across those repetitions
(currently a single value, because the share does not move even though the bytes do, which is
the reason the share is the published quantity). The raw byte counts under `measurements` are
from the first repetition and are there to be inspected, not to be quoted.

**Does not show** latency or money. No timing is measured, on purpose, and DBU cost needs an
account console Free Edition does not have.

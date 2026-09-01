# Claims

One section per claim. Each says what was measured, how, and - the part that matters - what
the result does **not** show. Numbers are rendered from `evidence/history.jsonl`; a test
fails if this file and the evidence disagree.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-03` mutation campaign | PASS | 37/37 (95% CI 90.6%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | local run, not reproduced in CI |
| `SG-06` seeds are derived from the commit | PASS | 1/1 (95% CI 20.7%-100.0%) | oss-local | local run, not reproduced in CI |

<!-- samegold:end claims -->

---

## SG-01 - two implementations agree on the close

**Experiment.** For each of 3 commit-derived seeds, the generator writes a full dataset and
its ledger. At every close instant, `gold.revenue_by_month` is computed by the DuckDB
reference and compared with the ledger, key by key, in cents.
Result: <!--sg:SG-01.rate-->15/15 (95% CI 79.6%-100.0%)<!--/sg-->.

**Does not show** that either is correct. They share an author and a contract, so a
misreading of the contract lands in both. Knight and Leveson measured exactly this in 1986
with 27 independently written versions and found failure coincidence far above chance; two
versions by one author are not more independent than that. The number that describes this
blind spot is the specification-mutant result in SG-03, not this one.

## SG-02 - re-delivery under a new path is a no-op

**Experiment.** Compute the close, copy every bronze file to a second path, recompute, and
compare canonical digests. Result: <!--sg:SG-02.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg-->.

**Does not show** exactly-once processing. This is at-least-once delivery plus content-keyed
deduplication, which is a different and weaker property. It also says nothing about a
duplicate that arrives after a streaming watermark has expired the state that would have
recognised it: that is a separate measurement on the Spark lane, and it is not zero.

## SG-03 - mutation campaign

**Experiment.** Mutants are generated mechanically from the SQL AST (comparison swaps, join
kind swaps, interval bumps, aggregate swaps, coalesce removal, order flips) and run past
three witnesses: the ledger, the invariants, and the runtime. Six **specification** mutants
are added by hand, because a generator cannot invent a change of meaning.
Result: <!--sg:SG-03.rate-->37/37 (95% CI 90.6%-100.0%)<!--/sg--> of the scored mutants killed, strict score
<!--sg:SG-03.artifact.strict_score-->0.7708<!--/sg--> if the equivalence classification in
`mutation/equivalents.py` is refused wholesale.

**What the campaign changed about the project.** The first run killed 71% and the survivors
were not harness holes, they were **generator** holes: the data never contained a line sold
for zero cents, a return exactly on the 45th day, a return at the instant of the sale, or a
sale that happened before a close and arrived after it, and no amendment that arrived after
one. Nine boundary cases were added and the score moved to its published value. The mutants
were measuring the generator rather than the pipeline, and saying so is more useful than the
number itself.

**Does not show** correctness. A mutation score is a lower bound on what a suite can see.
Two survivors classes remain and are named in `mutation/equivalents.py` with written
reasons; one of them is equivalent *only because* the imputation rule is what the contract
says it is, which is a dependency worth knowing about.

## SG-04 - a closed month moves after it is closed

**Experiment.** For every month that has been closed at least twice, compare the net revenue
at its own close (day 5 of the following month) with its final value.
Result: <!--sg:SG-04.rate-->2/2 (95% CI 34.2%-100.0%)<!--/sg--> of closed months moved, worst
<!--sg:SG-04.artifact.worst_move_pct-->4.9264<!--/sg-->%.

**Does not show** anything about real retail. The return rate and the lateness distribution
are set high on purpose so the rare paths appear often enough to measure.

## SG-05 - invariants hold with no oracle involved

**Experiment.** On each seed: SCD2 intervals disjoint, contiguous and with exactly one open
row per customer; `net = gross - returns`; close versions dense and `restated_at` monotonic;
no month refunding more than it sold; and conservation of every ingested row.
Result: <!--sg:SG-05.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg-->.

**Does not show** that the values are right. An invariant sees shape, not truth. On the
published campaign the invariants killed **zero** mutants that the ledger did not already
kill: their marginal contribution here is nil, and they are kept because in production there
is no ledger and shape is all there is.

## SG-06 - the seeds are derived from the commit

**Experiment.** Recompute the seeds from `git rev-parse HEAD` and compare them with the ones
the other claims used. Result: <!--sg:SG-06.rate-->1/1 (95% CI 20.7%-100.0%)<!--/sg-->.

**Why it exists.** Every other number here is worthless if the author can choose the seed.
`make refute SEED=...` deliberately fails this claim, and the evidence marks such runs
`seed_source=override` so they can never back a published number.

## SG-07 - the crash campaign (Spark lane)

**Experiment.** For each structural crash point, the writer is killed with `os._exit` inside
`foreachBatch`, the run is restarted from its checkpoint, and the deduplicated silver output
is digested and compared with a clean run's digest. A run that finishes without reaching its
crash point is reported as a MISSED INJECTION, not as a pass.

**Status.** Runs in the Spark lane (`make faults`), two silver-stage points, converging. It
is not in the published table above because it has not yet been reproduced in CI; see
`docs/milestones.md`.

**Does not show** crash safety of the engine. The reachable points are the ones a writer
owns. The points inside a Delta commit, inside a state-store checkpoint and inside a
multi-part object-storage commit are listed in `faults/points.py` with `reachable=False` and
are reported as NOT COVERED. Reaching them would require instrumenting the engine, at which
point the program under test is no longer the program that gets deployed.

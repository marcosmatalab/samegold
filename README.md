# samegold

**A month-end close that survives late returns, mid-write crashes and reprocessing — and a
harness whose whole job is to prove that it doesn't.**

A retail lakehouse (orders, amendments, returns) on Delta Lake and Spark, plus `samegold`: a
differential harness that generates the data *and* the ledger of what the answer must be,
computes the close twice in two engines, kills the pipeline at named structural points,
mutates the transformation code, measures what file layout costs, and publishes what it could
**not** catch.

Every number below is rendered from `evidence/history.jsonl`. That file is hash-chained and
its records are refused unless their seeds derive from the commit they name, so editing a
figure by hand breaks a test rather than improving a README.

> A return may arrive up to 45 days after the sale, and it is imputed to the month of the
> **sale**. So a month finance has already closed can move. In the published run it moved in
> <!--sg:SG-04.rate-->2/2 (95% CI 34.2%-100.0%)<!--/sg--> of the closed months, the worst by
> <!--sg:SG-04.artifact.worst_move_pct-->6.3652<!--/sg-->% of the figure that had been signed off.
> A pipeline that cannot restate would keep reporting the first number for ever, and be wrong
> by exactly that much.

## Sixty seconds

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make demo      # ~10 s, no account, no credentials, no JVM
make fast      # the whole fast lane: <!--sg:SG-00.artifact.tests_fast-->288<!--/sg--> tests in <!--sg:SG-00.artifact.fast_lane_seconds-->37.1<!--/sg--> s
make evidence  # regenerates every number except SG-07's (that one needs a JVM: make faults)
```

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 288/288 (95% CI 98.7%-100.0%) | oss-local | local run, not reproduced in CI |
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

## What it does

```
                      generator (seeded from the commit SHA)
                               │
                 ┌─────────────┴──────────────┐
                 │                            │
        bronze events (JSONL)          ledger of truth
        duplicates, corrupt records,   what the close must say
        late arrivals, restatements,   at every close instant
        ten boundary cases             (by construction, not recomputed)
                 │                            │
    ┌────────────┴─────────────┐              │
    │                          │              │
  SPARK implementation    DUCKDB reference    │
  bronze → silver →       the same contract,  │
  SCD2 → bitemporal       a different engine, │
  close                   no shared code      │
    │                          │              │
    └────────┬─────────────────┴──────────────┘
             │
     canonical digest over a declared projection
     (typed, length-prefixed, explicit total order)
             │
   ┌─────────┼──────────┬─────────────┬──────────────┬────────────┐
invariants  mutation  crash campaign  cost lab   privacy      evidence
no oracle   generated  named points   files and  masking and  hash-chained,
needed      + spec     + a negative   bytes, not purge that   seed-derived,
            mutants    control        seconds    really purges red runs kept
```

Three witnesses, deliberately unequal, and the repository says what each is worth:

| witness | catches | blind to |
|---|---|---|
| invariants | shape: SCD2 gaps and overlaps, conservation, `net = gross − returns` | values. On the published campaign their marginal contribution was **zero** kills the ledger had not already made, and that is printed rather than hidden |
| DuckDB reference | mistakes in the Spark implementation: dedup semantics, join direction, null handling, truncation | a misreading of the contract, which lands in both implementations identically |
| generator ledger | what was actually emitted, so a wrong close is visible in cents | the same blind spot: same author, same understanding |

The experiment that measures that blind spot is the set of **specification mutants**: six
changes to what the pipeline is *supposed* to do (which month a return belongs to, how long
the window is, what the dedup key is, whether the close cut is on arrival or event time). They
are the only mutants that can falsify the independence claim, and every one of them is killed
by name.

## Two bugs the two-implementation design caught

**One return per run, five thousand cents.** The Spark implementation used
`unix_timestamp()` to measure the 45-day window. It truncates to whole seconds, so a return
one microsecond outside the window came back as exactly 45 days and was accepted, while the
DuckDB reference rejected it. Nothing else in the repository would have found it: the totals
looked plausible, every invariant passed, and the only reason the case existed at all is that
a surviving mutant had asked for a boundary at exactly 45 days.

**A window that changes length twice a year.** The reference measured the same window with
`INTERVAL 45 DAY` over a `TIMESTAMPTZ`, which is calendar arithmetic in the session timezone.
Under `Europe/Madrid` — the accounting timezone this project declares — the window is 44h23 or
45h01 long across a daylight-saving boundary. Both implementations now compare seconds, and a
test runs the reference under three timezones and asserts the same answer.

## What is NOT claimed

Written before the results, because it is the part most portfolio projects leave out.

- **Not exactly-once.** Re-delivering identical content under a new path leaves the close
  unchanged (SG-02). That is at-least-once delivery plus content-keyed deduplication.
- **Not "the pipeline is correct".** Two implementations agreeing means they agree. Both were
  written by one person from one contract, and the specification mutants exist to price that.
- **Not "crash-safe".** The campaign reaches the points a writer owns. The points inside a
  Delta commit or a state-store checkpoint belong to the engine, are listed in
  `faults/points.py` with `reachable=False`, and are reported as NOT COVERED.
- **Not a proof from a mutation score.** Mutants are a lower bound on what a suite can see.
  The score is published twice: accepting the equivalence classification in
  `mutation/equivalents.py`, and refusing it entirely (strict score
  **<!--sg:SG-03.artifact.strict_score-->0.7059<!--/sg-->**).
- **Not a cost claim in money.** The cost lab measures files and bytes, never seconds and
  never DBUs. `system.billing` needs an account console that Free Edition does not have, and
  wall time in a container is not a substitute.
- **Not industry figures.** The return rate and the lateness distribution are set high on
  purpose so the rare paths appear often enough to measure.

## The evidence gate: what it stops, and what it does not

An adversarial reviewer appended records to `evidence/history.jsonl` by hand claiming 999/999
agreements and a 100% mutation score, pointed one at a CI run that does not exist, and ran the
suite. Everything passed. A second review, after the first round of fixes, got through five
more ways: an "override" run written straight into the history, an edited `runs/*.json`, a
reordered history, a record naming an invented commit, and a run URL pointing at somebody
else's repository. Each of those is now a test that fails, and the defences are:

1. **A hash chain.** Every record carries the hash of the previous one and its own. Editing,
   inserting, reordering or deleting a line breaks every hash after it.
2. **Seed derivation.** Seeds come from the commit SHA and the record names the purpose they
   were drawn for; the store recomputes them and refuses records whose seeds were chosen.
   Runs made with `SAMEGOLD_SEED_OVERRIDE` are refused outright and go to a separate
   refutation log.
3. **Anchors outside the file.** Every record must name a commit that exists in this
   repository, records must be in time order, and each `runs/<claim>.json` must hash to the
   record it claims to be.

**What it does not stop, stated plainly:** anyone who can run this code can regenerate the
whole chain, and a chain regenerated from scratch with invented figures verifies. There is no
key here to sign with, and pretending otherwise would be the same kind of overclaim the rest
of the repository exists to avoid. What the chain buys is that a *single* number cannot be
touched without rewriting everything after it, that every record is tied to a real commit, and
that the rewrite is visible in git history rather than invisible in a JSON file.

## What layout costs, measured

From the per-file statistics in the Delta log, not from a stopwatch, so the numbers are the
same on any machine:

- compaction removed **<!--sg:SG-09.artifact.files_removed_by_compaction_pct-->92.5<!--/sg-->%**
  of the files;
- clustering by (month, sku) cut the share of the table a sku predicate has to read by
  **<!--sg:SG-09.artifact.share_read_reduction_pct-->78.25<!--/sg-->%** — **and by nothing at all**
  at large file sizes, where three files cover the whole key range. Both measurements are
  published, and the headline is a share rather than a raw byte ratio because Z-ORDER also
  rewrites and recompresses, which a byte ratio would quietly take credit for;
- deleting one month copied
  **<!--sg:SG-09.artifact.rows_copied_per_row_deleted-->10.98<!--/sg--> surviving rows per deleted
  row**, which is the argument for deletion vectors in one number.

## Two runtimes, one parity matrix

| | OSS lane (this repo, free) | Databricks Free Edition |
|---|---|---|
| ingestion | Structured Streaming file source | Auto Loader (`cloudFiles`) on a UC Volume |
| pipelines | Spark 4.2.0 + Spark Declarative Pipelines | Lakeflow Spark Declarative Pipelines |
| storage | Delta Lake 4.4.0 (`io.delta:delta-spark_4.2_2.13:4.4.0`) and delta-rs 1.6.3 | Delta, managed by Unity Catalog |
| only here | crash injection (there is a process to kill), mutation, the cost lab, the purge | expectations, AUTO CDC **Type 2** (Spark 4.2 has Type 1), `CLUSTER BY AUTO`, UC governance, event log, Jobs, AI/BI |
| cost | 0 € — GitHub Actions is free and unlimited on public repositories | 0 € — Free Edition has no 14-day limit; it does have quotas |

`PARITY.md` says, claim by claim, which lane verifies what. Auto Loader is proprietary and has
**no** open-source equivalent: ingestion is an adapter with two implementations, one contract
test, and the differing guarantees written into the code rather than into a README nobody
re-reads.

## Refute it

```bash
make refute SEED=<anything>     # every claim, with a seed the author never saw
```

Seeds are derived from the commit SHA, so choosing a favourable one means changing the code,
which changes the seed. A run with `SEED=` is marked `seed_source=override` and can never back
a published number. If a claim fails under your seed, that is a refutation: open an issue with
the seed.

## Repository state

| lane | status |
|---|---|
| fast lane: generator, reference, digests, invariants, mutation, governance, evidence gate | done, <!--sg:SG-00.artifact.tests_fast-->288<!--/sg--> tests, <!--sg:SG-00.artifact.fast_lane_seconds-->37.1<!--/sg--> s |
| Spark lane without Delta | done, <!--sg:SG-00.artifact.tests_spark-->10<!--/sg--> tests: both engines agree on the versioned close |
| crash campaign, silver stage | done, with a negative control that a non-idempotent writer fails |
| cost lab on real Delta tables (delta-rs) | done, four experiments, one of them a negative result |
| privacy: masking, exposure check, retention purge | done |
| Delta on Spark (MERGE, CDF, OPTIMIZE, time travel) | written, <!--sg:SG-00.artifact.tests_delta-->6<!--/sg--> tests, needs Maven Central; see `docs/limits.md` |
| Databricks Free Edition lane (bundle, UC, expectations, AUTO CDC, dashboard) | written, needs a workspace |

## Documents

- `CLAIMS.md` — every claim, its experiment, and what it does not show
- `CONTRACT.md` — the data contract, the SLA, the restatement policy and the column classification
- `PARITY.md` — OSS versus Databricks, claim by claim
- `EXAM_MAP.md` — the Databricks Data Engineer Professional guide (3 July 2026), objective by objective
- `docs/adr/` — the decisions, with what was given up
- `docs/limits.md` — what this repository could not verify, and why

Apache-2.0.

# samegold

**A month-end close that survives late returns, mid-write crashes and reprocessing — and a
harness that tries to prove it doesn't.**

A retail lakehouse (orders, amendments, returns) built on Delta Lake and Spark Declarative
Pipelines, plus `samegold`: a differential harness that generates the data *and* the ledger
of what the answer must be, kills the pipeline at named structural points, re-delivers the
input, mutates the transformation code, and publishes what it could **not** catch.

Every number below is rendered from `evidence/history.jsonl` by `make readme`. A test fails
if a number in this file and the evidence behind it disagree.

> A return may arrive up to 45 days after the sale, and it is imputed to the month of the
> **sale**. So a month finance has already closed can move. In the published run it moved in
> <!--sg:SG-04.rate-->2/2 (95% CI 34.2%-100.0%)<!--/sg--> of the closed months, the worst by
> <!--sg:SG-04.artifact.worst_move_pct-->4.9264<!--/sg-->% of the figure that had been signed
> off. A pipeline that cannot restate would keep reporting the first number for ever, and be
> wrong by exactly that much.

## Sixty seconds

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make demo     # ~10 s, no account, no credentials, no JVM
make fast     # ~15 s, the whole fast lane: 127 tests
make evidence # ~90 s, regenerates every number in this file
```

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

## What it does

```
                      generator (seeded from the commit SHA)
                               │
                 ┌─────────────┴──────────────┐
                 │                            │
        bronze events (JSONL)          ledger of truth
        duplicates, corrupt,           what the close must say
        late arrivals, restatements    at every close instant
                 │                            │
    ┌────────────┴─────────────┐              │
    │                          │              │
  SPARK implementation    DUCKDB reference    │
  Auto Loader / file src  the same contract,  │
  → silver → SCD2 MERGE   different engine,   │
  → revenue_by_month      no shared code      │
    │                          │              │
    └────────┬─────────────────┴──────────────┘
             │
     canonical digest over a declared projection
     (no clock columns, explicit total order)
             │
     ┌───────┴────────┬──────────────┬────────────────┐
   invariants     fault campaign   mutation        evidence
   no oracle      named crash      generated +     append-only,
   needed         points           specification   red runs kept
```

Three witnesses, deliberately unequal, and the repository says which is worth what:

| witness | what it can catch | what it is blind to |
|---|---|---|
| invariants | shape: SCD2 gaps and overlaps, conservation, `net = gross - returns` | values; on this campaign its marginal contribution was **zero** kills the ledger did not already catch, and that is published rather than hidden |
| DuckDB reference | mistakes in the Spark implementation: dedup semantics, join direction, null handling, rounding | a misreading of the contract, which lands in both implementations identically |
| generator ledger | what was actually emitted, so a wrong close is visible in cents | the same blind spot: same author, same understanding |

The experiment that measures that blind spot is the set of **specification mutants**: six
changes to what the pipeline is *supposed* to do (which month a return belongs to, how long
the window is, what the dedup key is, whether the close cut is on arrival or event time).
They are the only mutants that can falsify the independence claim, and the campaign reports
each one by name.

## What is NOT claimed

Written first, before the results, because it is the part most portfolio projects leave out.

- **Not exactly-once.** Re-delivering identical content under a new path leaves the close
  unchanged (SG-02). That is at-least-once delivery plus content-keyed deduplication. Nothing
  here shows a record is processed once.
- **Not "the pipeline is correct".** Two implementations agreeing means they agree. Both were
  written by one person from one contract.
- **Not "crash-safe".** The crash campaign reaches the points a writer owns. The points
  inside a Delta commit or inside a state-store checkpoint belong to the engine, are listed
  in `faults/points.py` with `reachable=False`, and are reported as NOT COVERED.
- **Not a proof from a mutation score.** Mutants are a lower bound on what a suite can see.
  The score is published twice: accepting the equivalence classification in
  `mutation/equivalents.py`, and refusing it entirely (strict score
  **<!--sg:SG-03.artifact.strict_score-->0.7708<!--/sg-->**).
- **Not industry figures.** The return rate and the lateness distribution are set high on
  purpose so the rare paths appear often enough to measure. They describe the simulation.

## Two runtimes, one parity matrix

| | OSS lane (this repo, free) | Databricks Free Edition |
|---|---|---|
| ingestion | Structured Streaming file source | Auto Loader (`cloudFiles`) on a UC Volume |
| pipelines | Spark Declarative Pipelines (Apache Spark 4.2.0) | Lakeflow Spark Declarative Pipelines |
| storage | Delta Lake 4.4.0 (`io.delta:delta-spark_4.2_2.13:4.4.0`) | Delta, managed by Unity Catalog |
| what only runs here | crash injection (there is a process to kill), 140-run campaigns, mutation | expectations, AUTO CDC / SCD2, liquid clustering, UC governance, pipeline event log, Jobs, AI/BI dashboard |
| cost | 0 € — GitHub Actions is free and unlimited on public repositories | 0 € — Free Edition has no 14-day limit; it does have quotas |

`PARITY.md` says, claim by claim, which lane verifies it and which does not. Auto Loader is
proprietary and has **no** open-source equivalent: the ingestion layer is an adapter with two
implementations and one contract test, and the guarantees that differ are written down rather
than glossed over.

## Refute it

```bash
make refute SEED=<anything>     # runs every claim with a seed the author never saw
```

Seeds are derived from the commit SHA (`generator/seeds.py`), so the author cannot choose a
favourable one without changing the code and therefore the seed. A run with `SEED=` is marked
`seed_source=override` in the evidence and never counts towards a published number. If a claim
fails under your seed, that is a refutation: open an issue with the seed.

## Repository state

| lane | status |
|---|---|
| fast lane (generator, reference, digests, invariants, mutation, evidence) | done, 127 tests, ~15 s |
| Spark lane without Delta | done, 3 tests, agrees with the reference at every close |
| crash campaign, silver stage | done, 2 structural points, converges |
| Delta lane (MERGE, time travel, CDF, OPTIMIZE, liquid clustering) | scaffolded, needs Maven Central; see `docs/milestones.md` |
| Databricks Free Edition lane (bundle, UC, expectations, AUTO CDC, dashboard) | scaffolded, see `databricks/` |
| cost lab | designed, see `docs/milestones.md` |

## Documents

- `CLAIMS.md` — every claim, its experiment, and what it does not show
- `CONTRACT.md` — the data contract, the SLA, and the restatement policy
- `PARITY.md` — OSS versus Databricks, claim by claim
- `EXAM_MAP.md` — the Databricks Data Engineer Professional exam guide (3 July 2026), objective by objective, mapped to files and evidence
- `docs/adr/` — the decisions, with what was given up
- `docs/limits.md` — what this repository could not verify, and why

Apache-2.0.

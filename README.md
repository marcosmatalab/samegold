**English** | [Español](README.es.md)

<div align="center">

# 🥇 samegold

**A month-end revenue close on Spark and Delta Lake that backs every number it publishes with
evidence anyone can recompute.**

[![fast](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml)
[![spark](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml)
[![evidence](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml)
[![databricks evidence](https://github.com/marcosmatalab/samegold/actions/workflows/databricks-evidence.yml/badge.svg?branch=main)](https://github.com/marcosmatalab/samegold/actions/workflows/databricks-evidence.yml)
[![release](https://img.shields.io/github/v/release/marcosmatalab/samegold)](https://github.com/marcosmatalab/samegold/releases/latest)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue)](LICENSE)

<!-- samegold:begin stack -->
![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.2.0-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-4.4.0-00ADD4)
![Databricks](https://img.shields.io/badge/Databricks-Asset%20Bundles-FF3621?logo=databricks&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-reference%20engine-FFF000?logo=duckdb&logoColor=black)
![mypy](https://img.shields.io/badge/mypy-strict-2A6DB2)
![ruff](https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black)
<!-- samegold:end stack -->

</div>

> [!TIP]
> **In one sentence:** samegold closes the monthly revenue of a synthetic business on Spark
> and Delta Lake, keeps every version finance signed off, and publishes only claims a machine
> can re-measure.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/pipeline-dark.svg">
  <img alt="bronze_events to silver_classified, which splits into silver_events and silver_quarantine; gold reads silver_classified to build revenue_by_month and dim_customer_scd2; the bitemporal close_month writes one immutable version per close into revenue_closed. The Databricks lane is checked against the DuckDB reference to the cent." src="docs/img/pipeline-light.svg">
</picture>

## 💡 The problem, in plain words

Every month, finance **closes the books**: it adds up the month's sales, subtracts its returns
and signs the result off. Customers can return an item up to <!--repo:contract.return_window_days-->45<!--/repo--> days after buying it, so returns
keep arriving after that signature, and each one belongs to the month of the original sale. A
month that is already closed keeps changing.

That leaves a data team with two bad options. Overwrite the figure, and the number finance signed
off disappears. Freeze it, and the reported month stops being true. **samegold keeps both figures:** every
close is an immutable version, and each correction is added beside it as a new one.

**Why the rest of the repository exists.** A revenue figure that is wrong while every check is
green is an expensive failure, because nobody goes looking for it. So the pipeline is the smaller
part of the repository, and most of the rest tries to break it: a second implementation written
separately, generated mutants of its code, crashes injected mid-write and seeds that nobody
chose. Every published claim is measured again on each run.

## 🎯 What it does

- 🧾 **Closes the month.** Sales, returns and amendments flow bronze → silver → gold on Delta
  Lake and land in a versioned, immutable monthly revenue close.
- ⏳ **Keeps history exact.** Every closed version is kept beside the one that replaced it, so the
  figure finance signed off never disappears.
- ⚖️ **Checks it against an independent reference.** The Spark and Delta Lake pipeline and an
  independent DuckDB reference must produce one canonical digest, and the Databricks deployment
  is checked against that reference to the cent, version by version.
- 🔬 **Re-measures its own claims.** Each claim, `SG-00` to `SG-09`, is re-measured from seeds derived
  from the commit sha, appended to a hash-chained evidence log and rendered onto this page.

## 📊 Key metrics

**Every metric in this table is rendered from `evidence/`, not typed by hand.** `make readme`
writes them and `samegold check` fails the build if one drifts from its record.

| | What is measured | Result | Claim |
|---|---|---|---|
| ✅ | Fast lane, no JVM and no credentials | <!--sg:SG-00.artifact.tests_passed-->761<!--/sg--> tests pass in <!--sg:SG-00.artifact.fast_lane_seconds-->59.3<!--/sg--> s | `SG-00` |
| ⚖️ | DuckDB reference and by-construction ledger agree at every close | <!--sg:SG-01.rate-->15/15 (95% CI 79.6%-100.0%)<!--/sg--> | `SG-01` |
| 🔁 | Re-delivering every file under a new path changes nothing | <!--sg:SG-02.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg--> | `SG-02` |
| 🧬 | Non-equivalent generated SQL mutants killed | <!--sg:SG-03.rate-->67/67 (95% CI 94.6%-100.0%)<!--/sg--> | `SG-03` |
| ⏳ | Closed months that moved after sign-off, every version matching the reference | <!--sg:SG-04.rate-->2/2 (95% CI 34.2%-100.0%)<!--/sg--> | `SG-04` |
| 🧮 | Dimension and conservation invariants, no oracle needed | <!--sg:SG-05.rate-->3/3 (95% CI 43.9%-100.0%)<!--/sg--> | `SG-05` |
| 🔗 | Evidence records verified in the hash chain | <!--sg:SG-06.artifact.records_verified-->261<!--/sg--> | `SG-06` |
| 💥 | Injected crashes the silver writer survives | <!--sg:SG-07.rate-->20/20 (95% CI 83.9%-100.0%)<!--/sg--> | `SG-07` |
| 🔒 | Direct identifiers kept out of gold, purge verified | <!--sg:SG-08.rate-->6/6 (95% CI 61.0%-100.0%)<!--/sg--> | `SG-08` |
| 📦 | Files removed by compaction | <!--sg:SG-09.artifact.files_removed_by_compaction_pct-->92.5<!--/sg-->% | `SG-09` |
| 📉 | Cut in the share of the table a sku query reads, from clustering | <!--sg:SG-09.artifact.share_read_reduction_pct-->76.96<!--/sg-->% | `SG-09` |
| ☁️ | Events the Databricks lane closed, every version matching the open-source lane to the cent | <!--dbx:rows.bronze_events-->1883<!--/dbx--> events | `SG-DBX-01` |
| 🧱 | Verification harness against platform code, in lines | <!--sg:SG-00.artifact.harness_lines-->24 884<!--/sg--> against <!--sg:SG-00.artifact.platform_lines-->7 567<!--/sg--> | `SG-00` |

## 🚀 Try it in minutes

No account, no credentials, no network beyond PyPI.

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make install
make demo                 # the close, and the month that moved after it was signed off
make fast                 # the whole fast lane, no JVM
make refute SEED=424242   # the data claims again, on a seed nobody chose
```

What `make demo` prints:

<!-- samegold:begin demo -->
```text
samegold demo - 772 events, 301 files, seed 3606824677207351751

  Month 2026-01 was closed at 2026-02-05 reporting 150 658,58 EUR of net revenue.
  By 2026-04-05, late returns and late amendments had moved it to 147 227,51 EUR.
  That is -3 431,07 EUR, -2.28% of a month that finance had already signed off.

  The customer dimension is well formed: yes.
  Two implementations of that number are compared on this data by `samegold evidence`.

  No account, no credentials, nothing installed beyond this package.
```
<!-- samegold:end demo -->

**That block is produced by running the demo, not pasted,** and `samegold check` runs it
again and fails if one byte differs. The demo uses a fixed seed, so it prints the same
thing on every commit until the code changes; the claims below keep seeds derived from
the commit sha.

**The last command is the point.** Seeds derive from the commit sha, so a favourable seed cannot
be picked without making a new commit, which the history shows. `make refute` lets anyone choose
their own and runs every claim about the data on it, which turns each of them into an
invitation to falsify it.

![make refute on a seed nobody chose: the claims about the data run again, each printed as it passes](docs/img/refute.gif)

**A real recording, not an animation:** played at <!--repo:gif.refute.speed-->4x<!--/repo-->, over a real run of <!--repo:gif.refute.real_seconds-->73,1 s<!--/repo-->.
[`docs/refute.tape`](docs/refute.tape) is the script `vhs` executes, and `make gif` re-records
it from a fresh run.

`make preflight` is the gate before a push and `make doctor` reports what this machine can run.

## 🏗️ Architecture

**A medallion pipeline with a contract at the door.** Raw events land in `bronze_events`,
`silver_classified` applies the data contract, invalid records go to `silver_quarantine`, and
gold holds `revenue_by_month`, a Type 2 customer dimension `dim_customer_scd2` and the
versioned close `revenue_closed`.

**The pipeline diagram at the top of this page is tested against the code.**
`tests/fast/test_documentation.py::test_the_figures_agree_with_the_repository` derives the table
names and the reads between them by parsing `databricks/src/`, and the money figures from
`evidence/databricks/SG-DBX-01.json`, so a renamed table or a reversed arrow fails the build.

## ⏳ The bitemporal close

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/restatement-dark.svg">
  <img alt="Timeline: a January sale, the first close of January, a return for that sale arriving after the close, and January restated as a new version while the first close stays unchanged." src="docs/img/restatement-light.svg">
</picture>

A return may arrive up to <!--repo:contract.return_window_days-->45<!--/repo--> days after the sale and books into the month of the **sale**. The close
therefore tracks two time axes: when something happened and when the business learned about it.
The version finance signed off stays exactly as it was, next to the one that replaced it, and
`SG-04` measures how far each closed month moves and checks every version against the DuckDB
reference on every run.

**January, closed three times on Databricks, with no version rewritten:** gross <!--dbx:closed.2026_01.v0.gross_cents-->14 198 046<!--/dbx--> cents
at sign-off, <!--dbx:closed.2026_01.v1.gross_cents-->25 582 615<!--/dbx--> after the first late arrivals, and
<!--dbx:closed.2026_01.v2.gross_cents-->37 622 605<!--/dbx--> after the second, each matching the
open-source lane to the cent. The lane ran end to end on Databricks Free Edition.
[`docs/postmortem-2026-03-06.md`](docs/postmortem-2026-03-06.md) writes the restatement up as an
incident report.

## 🔬 How every number is proven

```mermaid
flowchart TD
    SHA["commit sha"] --> SEEDS["deterministic seeds"]
    SEEDS --> GEN["generator: sales, returns, amendments"]
    GEN --> LEDGER["by-construction ledger"]
    GEN --> DUCK["DuckDB reference close"]
    GEN --> SPARK["Spark + Delta Lake<br/>bronze → silver → gold"]
    GEN --> ATTACK["mutation · crash injection<br/>privacy purge · layout cost"]
    LEDGER --> AGREE{"SG-01: agree to the cent<br/>at every close"}
    DUCK --> AGREE
    DUCK --> DIGEST{"same canonical digest<br/>spark + delta lanes"}
    SPARK --> DIGEST
    AGREE --> CLAIMS["claims SG-00 … SG-09"]
    ATTACK --> CLAIMS
    CLAIMS --> CHAIN[("evidence/history.jsonl<br/>hash chain")]
    CHAIN --> PAGE["README + CLAIMS.md<br/>make readme"]
    CHAIN --> VERIFY["samegold verify-latest<br/>recomputes each record"]
    classDef input fill:#1f6feb,stroke:#1f6feb,color:#ffffff
    classDef engine fill:#e25a1c,stroke:#e25a1c,color:#ffffff
    classDef proof fill:#2da44e,stroke:#2da44e,color:#ffffff
    class SHA,SEEDS,GEN input
    class SPARK,DUCK,LEDGER,ATTACK engine
    class AGREE,DIGEST,CLAIMS,CHAIN,PAGE,VERIFY proof
```

The claims below are rendered from `evidence/history.jsonl`, an append-only hash chain, using the
most recent record for each claim. When the population moves, the claims run again and a new
record is appended, and the ones already in the chain are kept as they are:
**never edit or replace** a record. [ADR 0010](docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md) is
the full policy.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 761/761 (95% CI 99.5%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-03` mutation campaign | PASS | 67/67 (95% CI 94.6%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-06` the evidence chain verifies and every seed derives from its commit | PASS | 261/261 (95% CI 98.5%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-07` the silver writer survives a crash at each of its structural points | PASS | 20/20 (95% CI 83.9%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-08` no direct identifier reaches gold, and a purge really purges | PASS | 6/6 (95% CI 61.0%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |
| `SG-09` what layout costs, in files and bytes | PASS | 5/5 (95% CI 56.6%-100.0%) | oss-local | [CI, 7b1ef2ed2](https://github.com/marcosmatalab/samegold/actions/runs/35900042512) |

<!-- samegold:end claims -->

**Every row links the CI run that produced it, and `samegold verify-latest` recomputes each one
from the seeds its own record names.** A well-formed record is not enough: the number has to
reproduce. [ADR 0011](docs/adr/0011-the-gate-recomputes-the-record.md) documents the recompute
gate.

## ☁️ The Databricks lane

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/job-graph-dark.svg">
  <img alt="The samegold monthly close job: ingest_and_transform runs the Lakeflow pipeline, close_month writes the versioned close, and the condition task did_the_close_restate sends a close that restated a month to verify_each_restated_month, once per month, and a close that restated nothing to verify_no_restatement. publish_evidence runs after either of them." src="docs/img/job-graph-light.svg">
</picture>

**Deployed and run end to end on a real Databricks workspace.** A Databricks Asset Bundle
deploys a Lakeflow pipeline and a monthly close job whose condition task branches on whether the
close restated a month. The run records are committed under
[`evidence/databricks/`](evidence/databricks/) with their job run, pipeline and update ids, and
[`docs/databricks-run-evidence.md`](docs/databricks-run-evidence.md) renders what the workspace
measured: the Type 2 dimension row by row with its `__START_AT` and `__END_AT`, the closed
versions, the expectations with their pass and fail counts, and every event the contract refused.

**Verified from a clone, with no account:**
<!--sg:SG-00.artifact.tests_databricks_bundle-->135<!--/sg--> tests drive `databricks/` and
`scripts/databricks_run.sh` against a stub CLI on `PATH`: every notebook path, widget and job
parameter, the concurrent-task ceiling computed from the dependency graph above, and the guard
that refuses to run a job deployed from a commit that is not `HEAD`.

**Secure by design.** `.github/workflows/databricks.yml` takes `workflow_dispatch` only, defaults
to `validate`, starts no compute, pins every action to a commit sha and reads its token from a
GitHub environment rather than a repository secret; with no `pull_request` trigger, a fork's pull
request cannot reach it.

## ⚖️ Design decisions and trade-offs

Each decision is written up as an architecture decision record, with the alternatives it
rejected and the reason.

| Decision | Why | Trade-off accepted | ADR |
|---|---|---|---|
| **A second implementation, not more assertions** | Unit tests are blind in the same places as the code they test; an independent computation is not | Two implementations to maintain, and they share an author, so agreement is strong evidence rather than proof | [0001](docs/adr/0001-a-second-implementation-instead-of-more-tests.md) |
| **Share the contract, duplicate the computation** | Column names, the <!--repo:contract.return_window_days-->45<!--/repo-->-day window, the timezone and the currency are defined once; every derivation is written twice, so a misunderstanding surfaces as a disagreement | Every business rule exists twice, in DataFrame code and in SQL | [0004](docs/adr/0004-what-is-shared-between-implementations.md) |
| **Adaptive query execution stays on** | The production configuration is the one under test | Parity is checked on a sorted digest, never byte for byte on the files, so every projection must declare a total order | [0005](docs/adr/0005-adaptive-execution-stays-on.md) |
| **Seeds derive from the commit sha** | A favourable seed cannot be chosen quietly | Each commit changes the synthetic population, so figures move between commits; that is why they are rendered rather than typed | [0007](docs/adr/0007-the-evidence-gate.md) |
| **Evidence is append-only** | A stale figure is fixed by adding a measurement, so every past one stays inspectable | The history only grows, and the page quotes the latest record, which can predate the latest commit | [0010](docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md) |
| **Cost is measured in files and bytes, not seconds** | The figures come from the per-file statistics in the Delta log, so they are identical on any machine | They say nothing about wall-clock latency | [0008](docs/adr/0008-cost-is-measured-in-files-and-bytes.md) |
| **Privacy controls run in code** | They execute and are tested on every run, and the exposure check reads the output instead of trusting the masking step | A control in code can be bypassed by a different pipeline; the platform grants are only declared, for a workspace with groups | [0009](docs/adr/0009-governance-in-code.md) |
| **The Delta lane fails when it cannot verify** | A lane that could not run its checks must not report success | Behind a proxy that blocks Maven Central, the lane fails rather than skipping | [0013](docs/adr/0013-the-delta-lane-fails-when-it-cannot-verify.md) |

## 🧭 Where to look

| To see | Go to |
|---|---|
| The Spark and Delta Lake pipeline | `src/samegold/pipelines/` |
| The independent SQL reference | `src/samegold/oracle/gold_revenue.sql` |
| The data contract shared by both | `src/samegold/domain/contract.py` |
| The synthetic data generator and its ledger | `src/samegold/generator/` |
| The Databricks bundle, pipeline and close job | `databricks/` |
| Mutation testing | `src/samegold/mutation/` |
| Crash injection | `src/samegold/faults/` |
| The evidence chain and the README renderer | `src/samegold/evidence/` |
| The Spark and Delta test lanes | `tests/spark/` · `tests/delta/` |
| CI | `.github/workflows/` |

## 🧰 Tech stack

| Layer | Technology |
|---|---|
| ⚙️ Processing | PySpark · Delta Lake · delta-rs · medallion architecture · SCD Type 2 |
| ☁️ Cloud | Databricks Asset Bundles · Lakeflow pipelines · Jobs with condition tasks · Unity Catalog |
| 🦆 Reference engine | DuckDB, computing the same close independently |
| 🧪 Verification | pytest · Hypothesis · SQL mutants generated with sqlglot · crash injection · Wilson score intervals |
| 🔗 Evidence | append-only JSONL hash chain · seeds derived from the commit sha |
| 🛠️ Quality | Python · ruff · mypy strict · GitHub Actions: fast, spark, evidence, databricks |

## 🗺️ Documentation

- [`docs/how-it-works.md`](docs/how-it-works.md) - the design: three witnesses, the digest, the evidence gate, what layout costs
- [`CLAIMS.md`](CLAIMS.md) - every claim, its experiment and its scope
- [`FINDINGS.md`](FINDINGS.md) - the adversarial review log: every defect the harness caught, by what it teaches
- [`CHANGELOG.md`](CHANGELOG.md) - what each release changed
- [`docs/adr/`](docs/adr/) - the architecture decisions, each with the alternative it rejected and why
- [`docs/databricks-run.md`](docs/databricks-run.md) - what the cloud lane deploys and what it ran
- [`docs/databricks-run-evidence.md`](docs/databricks-run-evidence.md) - what the workspace measured, rendered from the records it left
- [`docs/runbook.md`](docs/runbook.md) - on-call runbook: what an alert means, data problem or platform problem, and how to repair a run
- [`docs/findings/`](docs/findings/) - write-ups of what the recompute gate found
- [`docs/join-skew.md`](docs/join-skew.md) - one key takes a large share of the rows: what happens to the join, measured
- [`EXAM_MAP.md`](EXAM_MAP.md) - the Databricks Professional guide, objective by objective
- [`PARITY.md`](PARITY.md) - open-source lane against Databricks, claim by claim
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - how to contribute: `make preflight` is the gate
- [`docs/limits.md`](docs/limits.md) - known limitations and residual risk

Apache-2.0.

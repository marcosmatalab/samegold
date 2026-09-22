**English** | [Español](README.es.md)

# samegold

**How you prove a data pipeline does what it says.** The pipeline is a bitemporal month-end
close on Delta Lake and Spark, computed three times by three engines. The rest is a harness
whose whole job is to prove it wrong, and a record of every time it did.

[![fast](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml)
[![spark](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml)
[![evidence](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml)
[![release](https://img.shields.io/github/v/release/marcosmatalab/samegold)](https://github.com/marcosmatalab/samegold/releases/latest)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue)](LICENSE)

**It exists because a green pipeline published 2.767e19 cents of January revenue,** six and a
half million times what its own contract allows for the lines involved, **and none of the three
engines saw it.** Two implementations agreed with each other and with a by-construction ledger.
A mutation campaign killed every mutant it had not classified as equivalent. Sixteen rounds of
adversarial review had found nothing. Three events the generator emits *in order to be rejected*
were booked as revenue, because the classification read "I cannot answer" as "accept".

So this is not a pipeline that works. It is one whose claims about itself are checkable, with a
record of every time one of them turned out to be false.

## Check it yourself, in seven minutes

No account, no credentials, no network beyond PyPI.

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make install
make demo                 # the close, and the month that moved after it was signed off
make fast                 # the whole fast lane, no JVM
make refute SEED=424242   # every claim again, on a seed nobody chose
```

About seven minutes end to end on a clean clone, most of it the last command. No duration is
typed on this page: `make doctor` prints what your machine took, and the lane's own time is
the anchored figure below. What `make demo` prints:

<!-- samegold:begin demo -->
```text
samegold demo - 732 events, 292 files, seed 6732031791646218452

  Month 2026-01 was closed at 2026-02-05 reporting 146 468,35 EUR of net revenue.
  By 2026-04-05, late returns and late amendments had moved it to 142 298,95 EUR.
  That is -4 169,40 EUR, -2.85% of a month that finance had already signed off.

  The customer dimension is well formed: yes.
  Two implementations of that number are compared on this data by `samegold evidence`.

  0.2s, no account, no credentials, nothing installed beyond this package.
```
<!-- samegold:end demo -->

**That block is rendered from evidence, not pasted.** It used to be pasted, and it was the one
defect here a reviewer could find by running the command this section tells them to run: it
announced 780 events where the program printed 694, and had been wrong for eighteen commits,
because the seeds derive from the commit sha and a transcript is stale the moment it is copied.

**The last command is the point.** Seeds deriving from the commit sha means picking a
favourable one requires changing the code, which changes the seed. `make refute` lets you pick
one anyway, and the chain refuses the result as evidence. **A claim that fails under your seed
is the most useful issue anyone can open here.**

![make refute on seed 424242: the seven claims about the data run again, and printed as each one passes](docs/img/refute.gif)

**That GIF is a recording, not a drawing.** [`docs/refute.tape`](docs/refute.tape) is the
script `vhs` executes, `make gif` regenerates it, and what scrolls past is the program's own
output at the speed it came out. A drawn one would be the transcript this page already removed
once, in colour.

`make fast` is the whole fast lane (<!--sg:SG-00.artifact.tests_fast-->694<!--/sg--> tests in
<!--sg:SG-00.artifact.fast_lane_seconds-->78.5<!--/sg--> s, no JVM, no credentials), `make
preflight` the gate before a push, `make doctor` what this machine can run.

## What it computes

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/pipeline-dark.svg">
  <img alt="bronze_events to silver_classified, which splits into silver_events and silver_quarantine; silver_events feeds revenue_by_month and dim_customer_scd2; the bitemporal close_month writes one immutable version per close into revenue_closed. The Databricks lane and the DuckDB reference compute it twice and must agree on one canonical digest." src="docs/img/pipeline-light.svg">
</picture>

**That diagram is checked against the code, in three ways.**
`tests/fast/test_documentation.py::test_the_figures_agree_with_the_repository` derives the
table names and the reads between them from `databricks/src/` by parsing it, and the money
figures from `evidence/databricks/SG-DBX-01.json`. A renamed table, a reversed arrow or a
changed digit each turns it red, and each turns red on its own.

It earned that on the first run. The picture drew the tidy chain bronze to classified to events
to gold, and the lane does not do that: gold reads `silver_classified`, which the code says in
one line of docstring and the drawing contradicted. A diagram that drifts is the same defect
class as a sentence that drifts.

## Why a close needs two time axes, in one picture

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/restatement-dark.svg">
  <img alt="Timeline: a sale on 5 January, January closes on 5 February at gross 14198046, a return for that January sale arrives on 18 February, and on 5 March January is restated as version 1 while version 0 stays unchanged." src="docs/img/restatement-light.svg">
</picture>

A return books into the month of the **sale**, not the month it arrived. So a month that
finance has already signed off can move afterwards, and the version they signed off has to
still be there, unchanged, next to the one that replaced it. That is what "bitemporal" buys,
and it is the property `SG-04` measures.

## What this repository is mostly made of, since somebody was going to count it

**<!--sg:SG-00.artifact.platform_share_pct-->24.8<!--/sg-->% of the code is Spark, Delta and
Databricks. <!--sg:SG-00.artifact.harness_share_pct-->75.2<!--/sg-->% is the harness that tries
to break it.** In lines: <!--sg:SG-00.artifact.platform_lines-->7 567<!--/sg--> against
<!--sg:SG-00.artifact.harness_lines-->22 911<!--/sg-->. Of the fast lane's
<!--sg:SG-00.artifact.tests_fast-->694<!--/sg--> tests,
<!--sg:SG-00.artifact.fast_lane_domain_tests-->175<!--/sg--> exercise the pipeline and
<!--sg:SG-00.artifact.fast_lane_repository_tests-->519<!--/sg--> check this repository's own
claims about itself - a test of the close, against a test of this repository's own paperwork.

Those figures are measured by `make evidence` and rendered here, like every other number on
this page. The split is an addition over a classification that is a JUDGEMENT, so the
classification is written out in `src/samegold/evidence/lane_split.py` where it can be argued
with, and <!--sg:SG-00.artifact.fast_lane_unclassified_files-->0<!--/sg--> test files are in
neither class - a file nobody has classified fails the fast lane rather than being assigned a
side, because the published sum is that addition.

**That ratio is the point rather than an accident.** Anybody can write a month-end close. What
is hard, and what this is about, is knowing whether the one you wrote is right, and being able
to hand somebody else a command that answers it. The pipeline is the subject. The harness is
the work.

If you are here for Spark and Delta specifically: `src/samegold/pipelines/`, `databricks/src/`,
`tests/spark/` and `tests/delta/`, and the Databricks lane below ran against a real workspace.

## The claims

Rendered from `evidence/history.jsonl`, an append-only hash chain, from the most recent record for
each claim. When the population moves the rule is: run the claims again and append a new record;
**never edit or replace** the ones already in the chain.
[ADR 0010](docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md) is the
full policy.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 678/678 (95% CI 99.4%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-03` mutation campaign | PASS | 67/67 (95% CI 94.6%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-06` the evidence chain verifies and every seed derives from its commit | PASS | 221/221 (95% CI 98.3%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-07` the silver writer survives a crash at each of its structural points | PASS | 20/20 (95% CI 83.9%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-08` no direct identifier reaches gold, and a purge really purges | PASS | 6/6 (95% CI 61.0%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |
| `SG-09` what layout costs, in files and bytes | PASS | 5/5 (95% CI 56.6%-100.0%) | oss-local | [CI, 9368e9fc4](https://github.com/marcosmatalab/samegold/actions/runs/35736791197) |

<!-- samegold:end claims -->

Every row links the workflow run that produced it, and `samegold verify-latest` recomputes each
one from the seeds its own record names. That command exists because the four defences before
it all answered "is this record well formed" and none of them answered "is this number true":
an adversarial review put a perfect mutation score on this page by appending one well-formed
record, with real seeds and a hash computed by this repository's own hasher, and `samegold
check` exited 0 on it with the whole fast lane green.
[ADR 0011](docs/adr/0011-the-gate-recomputes-the-record.md) is the fix, the two claims it does
not recompute by default, and the hole it still leaves.

## The month that closed twice

A return may arrive 45 days after the sale and is imputed to the month of the **sale**, so a month
finance signed off can move. January did: 14 198 046 cents at signature, restated to 25 582 615
when late events arrived, and to <!--dbx:revenue.2026_01.gross_cents-->37 622 605<!--/dbx--> when
more did - three versions, none of them rewritten. That last one is the Databricks lane, deployed
and run end to end on Free Edition over <!--dbx:rows.bronze_events-->1883<!--/dbx--> events, and
it agrees with the open-source lane **to the cent** - which computes it with no workspace at all.
[`docs/postmortem-2026-03-06.md`](docs/postmortem-2026-03-06.md) writes the restatement up as an
incident; the cloud figures are anchored to `evidence/databricks/SG-DBX-01.json` and checked on
every run of the fast lane.

## The Databricks lane: what you can check, and what you have to take on trust

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/img/job-graph-dark.svg">
  <img alt="The samegold monthly close job: ingest_and_transform runs the Lakeflow pipeline, close_month writes the versioned close, and the condition task did_the_close_restate sends a close that restated a month to verify_each_restated_month, once per month, and a close that restated nothing to verify_no_restatement. publish_evidence runs after either of them." src="docs/img/job-graph-light.svg">
</picture>

**The bundle is checked from a clone; the workspace is not.** Those are different claims and
mixing them is how a repository ends up sounding better than it is.

**Checkable here, with no account:**
<!--sg:SG-00.artifact.tests_databricks_bundle-->126<!--/sg--> tests drive `databricks/` and
`scripts/databricks_run.sh` against a stub CLI on `PATH` - every notebook path, every widget,
every job parameter, the concurrent-task ceiling computed as the width of the dependency graph
above, and the guard that refuses to run a job deployed from a commit that is not `HEAD`. The
figure above is checked against `databricks/resources/jobs.yml` by
`tests/fast/test_documentation.py`: a task renamed in the YAML turns it red.

**Not checkable here:** the workspace. The lane ran four times between 3 and 6 September 2026
against a Databricks Free Edition workspace, and the records are committed under
[`evidence/databricks/`](evidence/databricks/) with their job run, pipeline and update ids.
**This repository holds no credentials for it** - measured: zero repository secrets, zero
environments, and `databricks.yml` is `workflow_dispatch` only - so nobody with a clone can
re-run it. Those records are the one thing here you have to take on trust, and they say so
themselves: `"chain": {"chained": false}`.

What that buys instead of a screenshot:
[**`docs/databricks-run-evidence.md`**](docs/databricks-run-evidence.md) renders what the
workspace measured out of those records - the Type 2 dimension row by row with its `__START_AT`
and `__END_AT`, the four closed versions of two months, the expectations with their pass and
fail counts, the four events the contract refused with the value that did it. `samegold check`
fails if one figure on it stops matching the records.

## Where to go next

- [`FINDINGS.md`](FINDINGS.md) - every defect this repository found in itself, by what it teaches
- [`CLAIMS.md`](CLAIMS.md) - every claim, its experiment, and what it does **not** show
- [`CHANGELOG.md`](CHANGELOG.md) - what each release changed
- [`docs/how-it-works.md`](docs/how-it-works.md) - the design: three witnesses, the digest, the evidence gate, what layout costs
- [`docs/adr/`](docs/adr/) - the decisions, each with the alternative it rejected and why
- [`docs/databricks-run.md`](docs/databricks-run.md) - what the cloud lane deploys and what it ran
- [`docs/databricks-run-evidence.md`](docs/databricks-run-evidence.md) - what the workspace measured, rendered from the records it left
- [`docs/runbook.md`](docs/runbook.md) - the alert has fired at three in the morning: what it means, data problem or platform problem, and how to repair a run without spending the day's quota
- [`docs/findings/`](docs/findings/) - the write-ups: what the recompute gate found on its first run, and how two of the three were the gate
- [`docs/limits.md`](docs/limits.md) - what this repository could not verify, and why
- [`EXAM_MAP.md`](EXAM_MAP.md) - the Databricks Professional guide, objective by objective
- [`PARITY.md`](PARITY.md) - open-source lane versus Databricks, claim by claim
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - `make preflight` is the gate, and why it refuses to exit 0 on a machine that cannot run the Spark lanes

Apache-2.0.

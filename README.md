# samegold

**A bitemporal month-end close on Delta Lake and Spark, and a harness whose whole job is to prove
it wrong.** Three engines compute the same close from the same events; what they disagree about,
and what they all miss, is written down.

[![fast](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/fast.yml)
[![spark](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/spark.yml)
[![evidence](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml/badge.svg)](https://github.com/marcosmatalab/samegold/actions/workflows/evidence.yml)
[![licence](https://img.shields.io/badge/licence-Apache--2.0-blue)](LICENSE)

**It exists because a green pipeline published 2.767e19 cents of January revenue** - six and a
half million times what its own contract allows for the lines involved - **and none of the three
engines saw it.** Two implementations agreed with each other and with a by-construction ledger, a
mutation campaign killed every mutant it had not classified as equivalent, and sixteen rounds of
adversarial review had found nothing. Three events the generator emits *in order to be rejected*
were booked as revenue, because the classification read "I cannot answer" as "accept". So this is
not a pipeline that works: it is one whose claims about itself are checkable, with a record of
every time one of them turned out to be false.

## Refute it

```bash
make refute SEED=<anything>      # every claim, on a seed the author never saw
```

Seeds derive from the commit sha, so picking a favourable one means changing the code, which
changes the seed. Override runs are refused and written to `evidence/refutations.jsonl`, which is
committed. **A claim that fails under your seed is the most useful issue anyone can open here.**

## Sixty seconds

```bash
git clone https://github.com/marcosmatalab/samegold && cd samegold
make demo
```

```text
samegold demo - 780 events, 284 files, seed 6569293562773694097

  Month 2026-01 was closed at 2026-02-05 reporting 149 864,69 EUR of net revenue.
  By 2026-04-05, late returns and late amendments had moved it to 147 674,52 EUR.
  That is -2 190,17 EUR, -1.46% of a month that finance had already signed off.

  The customer dimension is well formed: yes.
  2.8s, no account, no credentials, nothing installed beyond this package.
```

`make fast` is the whole fast lane (<!--sg:SG-00.artifact.tests_fast-->564<!--/sg--> tests in
<!--sg:SG-00.artifact.fast_lane_seconds-->196.2<!--/sg--> s, no JVM, no credentials), `make
preflight` the gate before a push, `make doctor` what this machine can run.

## The claims

Rendered from `evidence/history.jsonl`, an append-only hash chain, from the most recent record for
each claim. When the population moves the rule is: run the claims again and append a new record;
**never edit or replace** the ones already in the chain.
[ADR 0010](docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md) is the
full policy.

<!-- samegold:begin claims -->

| claim | result | experiment | runtime | provenance |
|---|---|---|---|---|
| `SG-00` what this repository contains, counted | PASS | 520/520 (95% CI 99.3%-100.0%) | oss-local | local run, not reproduced in CI, 389bf7f64 |
| `SG-01` two implementations agree on the close | PASS | 15/15 (95% CI 79.6%-100.0%) | oss-local | CI, 4200be34b |
| `SG-02` re-delivery under a new path is a no-op | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | CI, 4200be34b |
| `SG-03` mutation campaign | PASS | 67/67 (95% CI 94.6%-100.0%) | oss-local | CI, 4200be34b |
| `SG-04` a closed month moves after it is closed | PASS | 2/2 (95% CI 34.2%-100.0%) | oss-local | CI, 4200be34b |
| `SG-05` dimension and conservation invariants hold without an oracle | PASS | 3/3 (95% CI 43.9%-100.0%) | oss-local | CI, 4200be34b |
| `SG-06` the evidence chain verifies and every seed derives from its commit | PASS | 180/180 (95% CI 97.9%-100.0%) | oss-local | CI, 4200be34b |
| `SG-07` the silver writer survives a crash at each of its structural points | PASS | 20/20 (95% CI 83.9%-100.0%) | oss-local | CI, 4200be34b |
| `SG-08` no direct identifier reaches gold, and a purge really purges | PASS | 6/6 (95% CI 61.0%-100.0%) | oss-local | CI, 4200be34b |
| `SG-09` what layout costs, in files and bytes | PASS | 5/5 (95% CI 56.6%-100.0%) | oss-local | CI, 4200be34b |

<!-- samegold:end claims -->

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

## Built with an AI assistant

Written by one person working with Claude, and the git history says so: nine commits authored by
it, fourteen more carrying a `Co-Authored-By` trailer, several bodies linking the session. The
direction was mine - what to build, what to distrust, which findings were worth a round - and the
adversarial reviews that produced most of [`FINDINGS.md`](FINDINGS.md) were often aimed at the
tooling itself. None of that changes what is checkable: the numbers are regenerated by `make
evidence` from seeds derived from the commit sha, the chain refuses records it cannot tie to a
commit, and `make refute SEED=...` runs the lot on a seed nobody chose. Evidence that holds only
because of who typed it was never evidence.

## Where to go next

- [`FINDINGS.md`](FINDINGS.md) - every defect this repository found in itself, by what it teaches
- [`CLAIMS.md`](CLAIMS.md) - every claim, its experiment, and what it does **not** show
- [`docs/how-it-works.md`](docs/how-it-works.md) - the design: three witnesses, the digest, the evidence gate, what layout costs
- [`docs/databricks-run.md`](docs/databricks-run.md) - what the cloud lane deploys and what it ran
- [`docs/limits.md`](docs/limits.md) - what this repository could not verify, and why
- [`EXAM_MAP.md`](EXAM_MAP.md) - the Databricks Professional guide, objective by objective
- [`PARITY.md`](PARITY.md) - open-source lane versus Databricks, claim by claim
- [`CONTRIBUTING.md`](CONTRIBUTING.md) - `make preflight` is the gate, and why it refuses to exit 0 on a machine that cannot run the Spark lanes

Apache-2.0.

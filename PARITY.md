# Parity: the open-source lane versus Databricks

Two runtimes run the same transformations. They are not the same platform, and the
differences are the part of this repository that took the most reading to get right.

## The one that cannot be papered over: Auto Loader

Auto Loader (`cloudFiles`) is **proprietary**. There is no open-source equivalent, in Spark
4.2 or anywhere else. The ingestion layer is therefore an adapter with two implementations
behind one contract test, and the guarantees that differ are these:

| | Auto Loader (Databricks) | Structured Streaming file source (OSS) |
|---|---|---|
| discovering new files | directory listing **or** cloud file notifications | directory listing only, every trigger |
| cost as the directory grows | roughly constant with notifications | O(number of objects) per trigger |
| seen-file state | RocksDB-backed, scales to millions | in the checkpoint, degrades with volume |
| schema evolution | `cloudFiles.schemaEvolutionMode`, schema hints, `_rescued_data` | declared schema plus `columnNameOfCorruptRecord`; no evolution modes |
| malformed records | rescued into `_rescued_data` | corrupt record column, and the semantics differ per format |

The claims that depend on ingestion semantics are therefore made **per lane**, never once and
transferred.

## The three lanes do NOT currently agree, and here is the measurement

Everything below was written on the assumption that the Databricks lane computes the same
close as the other two. **On 2 September 2026 that assumption was tested against a real
workspace for the first time and it is false.** The correction goes at the top rather than in
a footnote, because the claim it corrects is the headline claim of this repository.

What the run produced: `revenue_by_month` for 2026-01 with a gross of **2.767e19 cents** from
428 lines. The contract caps a single line at 10 000 x 1 000 000 = 1e10 cents, so that month's
revenue was six and a half million times the ceiling of one line. Three events did all of it -
`bad-0000007`, `bad-0000015`, `bad-0000023`, each with `unit_price_cents =
9223372036854775807` and `quarantine_reason = 'accepted'`. They are events the generator emits
in order to be REJECTED, and the other two lanes reject them.

Two causes, both now fixed and both covered by tests:

1. `gross_cents` was DOUBLE, not BIGINT, because Auto Loader inferred every bronze column as
   STRING and `qty * unit_price_cents` on two strings promotes to double. In a project whose
   thesis is that money is an integer number of cents, this lane's money was floating point.
2. On a STRING column, `unit_price_cents > 1000000` coerces the string to the INT32 literal's
   type; 9223372036854775807 overflows INT32 and non-ANSI Spark returns NULL. A NULL predicate
   does not match a `WHEN`, and the classification's `ELSE` was `accepted`.

So the honest statement of parity today is: **the rules agree, and the lanes did not, because
they ran on different types.** `docs/databricks-run.md` has the full account and
`docs/adr/0006-mutants-are-generated-not-planted.md` has what generalises from it. Nothing in
the table below should be read as "verified on all three lanes" until a run says so.

## Feature parity table

| capability | OSS lane | Databricks Free Edition | note |
|---|---|---|---|
| Spark Declarative Pipelines | yes, Apache Spark 4.2 (`spark-pipelines run`) | yes, Lakeflow | the spec file is `spark-pipeline.yml`; the OSS CLI has `init`, `run`, `dry-run` |
| expectations / data quality in the pipeline | **no** - not in the OSS SDP | yes | the OSS lane enforces the same rules as a `CASE` in `transform.quarantine_reason()` |
| AUTO CDC | **partial**: Spark 4.2 added declarative SCD **Type 1** upserts to open-source SDP; **Type 2 is Databricks-only** | yes, Type 1 and Type 2, with bitemporal tracking | the OSS lane builds Type 2 from the source versions and writes the `MERGE` by hand, which is what the exam asks you to be able to do anyway |
| pipeline event log | **no** | yes | the OSS lane records its own metrics into the evidence store |
| Delta Lake | 4.4.0 OSS | managed by Unity Catalog | `io.delta:delta-spark_4.2_2.13:4.4.0` |
| liquid clustering | `CLUSTER BY` yes | `CLUSTER BY AUTO` also | automatic clustering needs predictive optimization, which is Databricks-only |
| deletion vectors, row tracking, CDF, type widening | yes | yes | |
| catalog-managed tables / commit coordination | preview | yes | |
| Unity Catalog | the open-source UC server | full, one metastore | Free Edition has no external locations, so volumes are the only storage |
| system tables (`system.billing`, `system.access`) | n/a | **no** - they need account-admin, which Free Edition does not grant | cost is measured from Spark metrics and the Delta log instead |
| Delta Sharing | OSS server, as provider and recipient | recipient only | |
| Jobs / orchestration | `make` and the CI workflow | Lakeflow Jobs via a bundle | |
| bundles (Declarative Automation Bundles, formerly Asset Bundles) | n/a | supported, from outside the workspace with a PAT - **this repository has never deployed one**, see `docs/databricks-run.md` | Free Edition restricts outbound traffic, so deploying *from inside* the workspace is unreliable; deploy from outside |
| continuous streaming | yes, locally | **no** - time-based triggers are rejected on serverless (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`); one active pipeline per type, and quota exhaustion stops compute for the day | everything is designed around `Trigger.AvailableNow` plus a scheduled job |
| killing the process mid-write | yes | **no** - serverless gives you no process to kill | the whole crash campaign lives in the OSS lane, and that is stated rather than implied |
| Delta through a second implementation | yes: delta-rs 1.6.3 reads and writes the same tables, and the cost lab and the purge run on it | n/a | multi-engine interoperability is what the format is for, and it is also how the Delta-protocol claims get executed on a machine with no route to Maven |

### The three Databricks-only primitives, checked rather than asserted

The three rows above that say "Databricks-only" were prose for eleven rounds. They are now
each a signature in the open-source API that the Databricks sources fail against, and mypy
checks `databricks/src/` since round 11, so the boundary is enforced where it used to be
described. Against `pyspark 4.2.0`:

| what `databricks/src/` calls | what open-source Spark 4.2.0 offers | how it shows up |
|---|---|---|
| `dp.expect_all_or_drop(RULES)` | `pyspark.pipelines.api` has no such attribute | `error: Module has no attribute "expect_all_or_drop"` |
| `create_streaming_table(cluster_by_auto=True)` | `cluster_by` only, an explicit column list | `error: Unexpected keyword argument "cluster_by_auto"; did you mean "cluster_by"?` |
| `create_auto_cdc_flow(stored_as_scd_type=2)` | typed `Literal[1, "1"] \| None` | `error: incompatible type "Literal[2]"; expected "Literal[1, '1'] \| None"` |

Each call keeps a narrow `# type: ignore` with the reason written beside it. They are not
worked around and not simulated: the code is right for the runtime it is deployed to, and the
open-source lane answers the same three objectives another way - a `CASE` expression for the
rules, `CLUSTER BY (customer_id)` with named columns, and a hand-written two-pass `MERGE` for
the Type 2 dimension, which `tests/delta` now executes.

What the open-source Delta lane **does** have, measured on the run described in
`docs/limits.md` rather than assumed: `CLUSTER BY (customer_id)` really clusters
(`DESCRIBE DETAIL` reports `clusteringColumns: [customer_id]`), deletion vectors really apply
(a single-row `DELETE` reports `numDeletionVectorsAdded: 1` and `numCopiedRows: 0`, and writes
a `deletion_vector_*.bin`), and `OPTIMIZE ... ZORDER BY` really Z-orders
(`operationParameters.zOrderBy` names the column). Only the **automatic** half of clustering
needs predictive optimization, and `CLUSTER BY AUTO` is a parse error outside Databricks.

## Claim by claim

| claim | verified in | not verified in |
|---|---|---|
| SG-00 repository facts | OSS | |
| SG-01 two implementations agree on the versioned close | OSS | Databricks: the reference cannot run there |
| SG-02 re-delivery is a no-op | OSS | Databricks: same input, different ingestion semantics; a separate run is needed |
| SG-03 mutation campaign | OSS | Databricks: mutating a deployed pipeline is not something to do to a workspace |
| SG-04 a closed month moves | OSS | reproduced on Databricks as a dashboard, as illustration, not as evidence |
| SG-05 invariants | both | |
| SG-06 evidence chain and seed provenance | OSS | |
| SG-07 crash campaign | OSS | Databricks: serverless gives you no process to kill, by design |
| SG-08 masking, exposure check, retention purge | OSS | Databricks: row filters and column masks are declared there, and unenforceable without account groups |
| SG-09 cost lab | OSS (delta-rs) | Databricks: the same experiments would be more interesting with predictive optimization and `CLUSTER BY AUTO`, and neither exists outside it |

## Cost

Zero euros, and the two reasons it is zero are worth stating because they are load-bearing:
GitHub Actions is free and unlimited on **public** repositories (standard runners: 4 vCPU,
16 GB RAM, 14 GB disk), and Databricks Free Edition has no 14-day limit. Free Edition does
have quotas that stop compute for the rest of the day when exceeded, its accounts may be
deleted after long inactivity, and its terms prohibit commercial use.

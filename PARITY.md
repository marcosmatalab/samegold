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

## Feature parity table

| capability | OSS lane | Databricks Free Edition | note |
|---|---|---|---|
| Spark Declarative Pipelines | yes, Apache Spark 4.2 (`spark-pipelines run`) | yes, Lakeflow | the spec file is `spark-pipeline.yml`; the OSS CLI has `init`, `run`, `dry-run` |
| expectations / data quality in the pipeline | **no** - not in the OSS SDP | yes | the OSS lane enforces the same rules as a `CASE` in `transform.quarantine_reason()` |
| AUTO CDC / SCD Type 2 as a primitive | **no** | yes, including bitemporal tracking | the OSS lane writes the `MERGE` by hand, which is what the exam asks you to be able to do anyway |
| pipeline event log | **no** | yes | the OSS lane records its own metrics into the evidence store |
| Delta Lake | 4.4.0 OSS | managed by Unity Catalog | `io.delta:delta-spark_4.2_2.13:4.4.0` |
| liquid clustering | `CLUSTER BY` yes | `CLUSTER BY AUTO` also | automatic clustering needs predictive optimization, which is Databricks-only |
| deletion vectors, row tracking, CDF, type widening | yes | yes | |
| catalog-managed tables / commit coordination | preview | yes | |
| Unity Catalog | the open-source UC server | full, one metastore | Free Edition has no external locations, so volumes are the only storage |
| system tables (`system.billing`, `system.access`) | n/a | **no** - they need account-admin, which Free Edition does not grant | cost is measured from Spark metrics and the Delta log instead |
| Delta Sharing | OSS server, as provider and recipient | recipient only | |
| Jobs / orchestration | `make` and the CI workflow | Lakeflow Jobs via a bundle | |
| bundles (Declarative Automation Bundles, formerly Asset Bundles) | n/a | yes, deployed from the CI runner with a PAT | Free Edition restricts outbound traffic, so deploying *from inside* the workspace is unreliable; deploy from outside |
| continuous streaming | yes, locally | **no** - time-based triggers are rejected on serverless (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`); one active pipeline per type, and quota exhaustion stops compute for the day | everything is designed around `Trigger.AvailableNow` plus a scheduled job |
| killing the process mid-write | yes | **no** - serverless gives you no process to kill | the whole crash campaign lives in the OSS lane, and that is stated rather than implied |

## Claim by claim

| claim | verified in | not verified in |
|---|---|---|
| SG-01 two implementations agree | OSS | Databricks: the reference cannot run there |
| SG-02 re-delivery is a no-op | OSS | Databricks: same input, different ingestion semantics; a separate run is needed |
| SG-03 mutation campaign | OSS | Databricks: mutating a deployed pipeline is not a thing you should do to a workspace |
| SG-04 a closed month moves | OSS | reproduced on Databricks as a dashboard, as illustration, not as evidence |
| SG-05 invariants | both | |
| SG-06 seed provenance | OSS | |
| SG-07 crash campaign | OSS | Databricks: no process to kill, by design |

## Cost

Zero euros, and the two reasons it is zero are worth stating because they are load-bearing:
GitHub Actions is free and unlimited on **public** repositories (standard runners: 4 vCPU,
16 GB RAM, 14 GB disk), and Databricks Free Edition has no 14-day limit. Free Edition does
have quotas that stop compute for the rest of the day when exceeded, its accounts may be
deleted after long inactivity, and its terms prohibit commercial use.

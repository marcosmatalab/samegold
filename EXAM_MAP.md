# Exam map — Databricks Certified Data Engineer Professional

Against the official exam guide dated **3 July 2026**
([PDF](https://www.databricks.com/sites/default/files/2026-07/databricks-certified-data-engineer-professional-exam-guide-july-3-2026.pdf)),
whose objectives are quoted below. The section weights are the ones published on the
[certification page](https://www.databricks.com/learn/certification/data-engineer-professional);
the PDF itself lists the objectives without percentages, and saying so is cheaper than being
caught rounding someone else's blog post into a fact.

Exam shape, from the guide: **59 scored multiple-choice questions, 120 minutes**.

For each objective: where the work is, and — where the answer is "nowhere" — why. A map that
lists only hits is not a map.

| # | section | weight | state |
|---|---|---|---|
| 1 | Developing code for data processing using Python and SQL | 22% | covered, except the Databricks-only APIs |
| 2 | Data ingestion and acquisition | 7% | covered, with the Auto Loader gap stated |
| 3 | Data transformation, cleansing and quality | 10% | covered |
| 4 | Data sharing and federation | 5% | not covered: needs two workspaces or an external database |
| 5 | Monitoring and alerting | 10% | partial: run-level metrics yes, system tables no |
| 6 | Cost and performance optimisation | 13% | covered in files and bytes; not in DBUs |
| 7 | Data security and compliance | 10% | covered in code; not enforced by a platform |
| 8 | Data governance | 7% | partial: contract and classification yes, Unity Catalog needs a workspace |
| 9 | Debugging and deploying | 10% | covered |
| 10 | Data modelling | 6% | covered |

---

## 1. Developing code for data processing (22%)

| objective (guide) | where | evidence |
|---|---|---|
| "Design and implement a scalable Python project structure optimized for Declarative Automation Bundles" | `src/samegold/` with an enforced layering test, `databricks/` as the bundle root | `tests/fast/test_architecture.py` |
| "Manage and troubleshoot external third-party library installations and dependencies" | `src/samegold/pipelines/session.py` and ADR 0002: the exact Maven coordinate, the sdist trap, the Ivy cache path | SG-00 |
| "Develop User-Defined Functions (UDFs) using Pandas/Python UDF" | **gap.** The pipeline deliberately has none: every transformation is expressible in the SQL/DataFrame API, which is faster and analysable. A UDF example with its cost measured is milestone M15 | — |
| "Build and manage reliable, production-ready data pipelines for batch and streaming data" | `src/samegold/pipelines/transform.py`, `faults/worker.py` (streaming with `foreachBatch` + `availableNow`) | SG-01, SG-07 |
| "Create and Automate ETL workloads using Jobs via UI/APIs/CLI" | `databricks/resources/jobs.yml`, deployed by the CLI from CI; the schedule is deployed PAUSED because a Free Edition quota overrun stops all compute for the day | needs a workspace |
| "Explain the advantages and disadvantages of streaming tables compared to materialized views" | `pipelines/transformations/bronze_silver.py` and `databricks/src/gold_close.py`: streaming tables for append-only silver, a materialized view for the close | PARITY.md |
| "Use AUTO CDC APIs to simplify CDC in Lakeflow Spark Declarative Pipelines" | `databricks/src/gold_close.py` (`create_auto_cdc_flow`, SCD type 2) versus the hand-written equivalent in `src/samegold/pipelines/gold_scd2_merge.py` | needs a workspace |
| "Compare Spark Structured Streaming and Lakeflow Spark Declarative Pipelines" | the streaming path is exercised by the crash campaign; the declarative spec shares the same `classify` function and its configuration is checked by a test, but it is **not executed here** (milestone M11) | SG-07, PARITY.md |
| "Create a pipeline component that uses control flow operators" | `databricks/resources/jobs.yml` task dependencies; `faults/harness.py` for the local equivalent | SG-07 |
| "Choose the appropriate configs for environments and dependencies" | `src/samegold/pipelines/session.py`, bundle targets | ADR 0002 |
| "Develop unit and integration tests using assertDataFrameEqual, assertSchemaEqual" | `tests/spark/test_assertions.py` uses both, next to the canonical-digest comparison, and the file explains when each one is the right tool | SG-00 |

## 2. Data ingestion and acquisition (7%)

| objective | where | evidence |
|---|---|---|
| "Design and implement data ingestion pipelines to efficiently ingest a variety of data formats" | `ingest/adapter.py`: two implementations, one contract | `tests/spark/test_ingest_contract.py` |
| "Create an append-only data pipeline capable of handling both batch and streaming data" | silver is append-only by design; uniqueness belongs to gold | SG-02, SG-07 |
| Auto Loader specifics (listing versus notification, schema evolution, rescued data) | `PARITY.md` and `databricks/src/bronze_autoloader.py`; **no open-source equivalent exists** | stated, not faked |

## 3. Data transformation, cleansing and quality (10%)

| objective | where | evidence |
|---|---|---|
| "Write efficient Spark SQL and PySpark code to apply advanced data transformations" | `src/samegold/pipelines/transform.py`, `oracle/gold_revenue.sql` | SG-01 |
| "Develop a quarantining process for bad data with Lakeflow Spark Declarative Pipelines" | `transform.quarantine_reason` (closed enum, one door per record) and `databricks/src/silver_expectations.py` (`expect_all_or_drop` plus a quarantine table) | SG-05 |
| proving the quality gate detects real faults | the mutation campaign | SG-03 |

## 4. Data sharing and federation (5%)

| objective | where |
|---|---|
| "Demonstrate delta sharing securely between Databricks deployments" | **not covered.** It needs two workspaces; Free Edition can be a recipient, not a provider |
| "Configure Lakehouse Federation with proper governance" | **not covered.** It needs an external database and a connector |
| "Use Delta Share to share live data from Lakehouse to any computing platform" | **not covered.** An open-source sharing server is not on the milestone list; it is out of scope and said so |

This is the weakest section of the project and the honest reason is money, not effort.

## 5. Monitoring and alerting (10%)

| objective | where | evidence |
|---|---|---|
| "Use Lakeflow Spark Declarative Pipelines Event Logs to monitor pipelines" | `databricks/src/publish_evidence.py` reads the event log and exports expectation counts | needs a workspace |
| "Use the Databricks REST APIs/Databricks CLI for monitoring jobs and pipelines" | `.github/workflows/databricks.yml` | needs a workspace |
| "Use SQL Alerts to monitor data quality" | the rule is in `src/samegold/serve/freshness.py`: ingestion lag and an overdue close are separate alerts because they have different causes and different responders. Wiring it into a Databricks SQL Alert is milestone M12, whose lane has now run (3 September 2026) | fast lane |
| run-level metrics without a platform | every evidence record carries duration, counts, digests and provenance | all claims |
| a consumption layer someone actually reads | `samegold report`: one self-contained HTML page with every version of the close and what moved after signature | `tests/fast/test_serve.py` |
| an incident written up like an incident | `docs/postmortem-2026-03-06.md`, with the numbers taken from SG-04 | SG-04 |
| "Use system tables for observability over resource utilization, cost, auditing and workload" | **not available on Free Edition** (no account console, no metastore-admin role) | `docs/limits.md` |

## 6. Cost and performance optimisation (13%)

| objective | where | evidence |
|---|---|---|
| "Understand delta optimization techniques, such as deletion vectors and liquid clustering" | the cost lab: compaction, Z-ORDER clustering at two file sizes, the copy cost of a delete | SG-09 |
| "Recognizing optimization techniques for large dataset queries, including data skipping and file pruning" | files-not-skippable computed from the per-file min/max in the Delta log | SG-09 |
| "Simplify data layout decisions ... liquid clustering over Partitioning and ZOrder" | COST-03 measures **partitioning versus Z-ORDER clustering** against two predicates and reports which one each serves. Liquid clustering itself is Databricks-only and is declared, not measured | SG-09 |
| "Applying Change Data Feed to address streaming table limitations" | `tests/delta/test_delta_semantics.py` reads it AS A FEED - the four `_change_type` values, each attributed to the commit that made it - and asserts the two rows the table no longer has are in it; the Databricks lane enables it on the dimension | delta lane, run |
| "Using query profiles to identify performance bottlenecks" | **gap**: the query profile is a Databricks UI. `docs/limits.md` says what is measured instead | — |
| "Unity Catalog managed tables reduce operational overhead" | discussed in PARITY.md; not demonstrable without a metastore | — |

## 7. Data security and compliance (10%)

| objective | where | evidence |
|---|---|---|
| "Applying anonymization methods including hashing, tokenization, suppression, and generalization" | `governance/anonymise.py`, all four, with the failure mode of each | SG-08 |
| "Implementing batch and streaming pipelines that detect and apply PII masking" | `governance/policy.py`: classification per column, masking on the way into gold, and an exposure check over the OUTPUT that catches an identifier hiding under a new column name | SG-08 |
| "Developing data purging solutions for data retention policy compliance" | `governance/retention.py`: delete **and** vacuum, because time travel returns purged rows until the files are gone | SG-08 |
| "Use row filters and column masks to filter and mask sensitive table data" | `databricks/sql/policies.sql` declares them, and **nothing executes that file**: applying it needs a SQL warehouse id a Free Edition bundle can neither create nor learn, and `is_account_group_member` is false for everyone on an account with no groups. Declared, parsed, never applied | `docs/databricks-run.md` |
| "Using ACLs to secure workspace objects with least privilege" | `databricks/resources/grants.yml` | needs a workspace |

## 8. Data governance (7%)

| objective | where | evidence |
|---|---|---|
| "Create and add descriptions/metadata about enterprise data to make it more discoverable" | `CONTRACT.md` plus the test that keeps it in step with `domain/contract.py`; table comments in the bundle | fast lane |
| "Demonstrate understanding of Unity Catalog permission inheritance model" | `databricks/resources/grants.yml` and PARITY.md | needs a workspace |

## 9. Debugging and deploying (10%)

| objective | where | evidence |
|---|---|---|
| "Build and deploy Databricks resources using Declarative Automation Bundles" | `databricks/databricks.yml`, `make databricks` (one command: catalog, validate, deploy, seed, run, fetch) and the CI job that calls the same script. The bundle is checked against the Free Edition limits, and against the fields its create APIs require, by `tests/fast/test_databricks_bundle.py`. First real deploy attempted 2 September 2026: validate passed, deploy failed on a missing pipeline `name`, both fixed; **nothing has run yet** | `docs/databricks-run.md` |
| "Configure and integrate with Git-based CI/CD workflows" | four workflows; the evidence workflow opens a pull request rather than pushing to a protected branch | SG-00 |
| "Analyze the errors and remediate the failed job runs with job repairs and parameter overrides" | `faults/harness.py` keeps every failed run's output and names the injection that produced it | SG-07 |
| reproducing a run exactly | commit-derived seeds and a hash-chained history | SG-06 |

## 10. Data modelling (6%)

| objective | where | evidence |
|---|---|---|
| "Design Dimensional Models for analytical workloads" | SCD Type 2 by full recomputation and incrementally, plus a bitemporal fact table | SG-01, SG-05 |
| "Design and implement scalable data models using Delta Lake" | `gold_scd2_merge.py` (clustered, CDF on, deletion vectors on), executed against a real Delta table: `DESCRIBE DETAIL` reports the clustering column, and the MERGE's two branches and its delete-by-absence are counted in the transaction log | delta lane, run |
| "Identify the benefits of using liquid Clustering over Partitioning and ZOrder" | Z-ORDER versus partitioning is measured; `CLUSTER BY (customer_id)` is applied and confirmed in `DESCRIBE DETAIL` on the open-source lane, and only `CLUSTER BY AUTO` needs Databricks - it is a parse error outside it, which PARITY.md records | SG-09, delta lane |
| bitemporal modelling: system time versus valid time | `domain/bitemporal.py`, `revenue_by_month(accounting_month, close_version, restated_at)` | SG-04 |

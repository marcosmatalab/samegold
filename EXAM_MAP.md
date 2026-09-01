# Exam map - Databricks Certified Data Engineer Professional

Against the exam guide dated **3 July 2026**, ten sections. For each: where in this
repository the work is, what evidence backs it, and - where the answer is "nowhere" - why,
because a map that only lists hits is not a map.

| # | section | weight | state |
|---|---|---|---|
| 1 | Developing code for data processing using Python and SQL | 22% | covered |
| 2 | Data ingestion and acquisition | 7% | covered, with the Auto Loader gap stated |
| 3 | Data transformation, cleansing and quality | 10% | covered |
| 4 | Data sharing and federation | 5% | partial: sharing yes, federation no |
| 5 | Monitoring and alerting | 10% | partial: pipeline metrics yes, system tables no |
| 6 | Cost and performance optimisation | 13% | partial: physical metrics yes, DBU cost no |
| 7 | Data security and compliance | 10% | partial: declared and drift-tested, not enforced at account level |
| 8 | Data governance | 7% | covered on the Databricks lane |
| 9 | Debugging and deploying | 10% | covered |
| 10 | Data modelling | 6% | covered |

---

## 1. Developing code for data processing using Python and SQL (22%)

| objective | where | evidence |
|---|---|---|
| DataFrame transformations, joins, windows | `pipelines/transform.py` | SG-01, `tests/spark/` |
| the same logic in SQL | `oracle/gold_revenue.sql`, `oracle/gold_scd2.sql` | SG-01 |
| structured streaming, `foreachBatch`, checkpoints, triggers | `faults/worker.py` | SG-07 |
| watermarks and what they do and do not solve | ADR 0003, `docs/limits.md` | SG-04 |
| deduplication semantics and idempotent writes | `transform.deduplicate`, `faults/worker.write_batch` | SG-02, SG-07 |
| deterministic code: no wall clock in a computed column | `verify/digest.py` refuses to digest one | `tests/fast/test_digest.py` |
| testing Spark code | `tests/spark/`, and the digest-based comparison | SG-01 |

## 2. Data ingestion and acquisition (7%)

| objective | where | evidence |
|---|---|---|
| incremental file ingestion | `ingest/` adapter, two implementations | contract test |
| Auto Loader: listing versus notification, schema evolution, rescued data | `PARITY.md`, `databricks/src/bronze_autoloader.py` | Databricks lane |
| declared schema instead of inference, and why | `pipelines/schema.py` | `tests/spark/` |
| handling malformed records without losing them | `transform.quarantine_reason`, conservation invariant | SG-05 |

## 3. Data transformation, cleansing and quality (10%)

| objective | where | evidence |
|---|---|---|
| quarantine with a closed set of reasons | `domain/contract.QuarantineReason` | SG-05 |
| expectations in a declarative pipeline | `databricks/src/silver_expectations.py` | Databricks lane |
| the equivalent where expectations do not exist | `transform.quarantine_reason()` | SG-05 |
| conservation: nothing disappears silently | `verify/invariants.conservation` | SG-05 |
| proving the quality gate detects real faults | the mutation campaign | SG-03 |

## 4. Data sharing and federation (5%)

| objective | where | evidence |
|---|---|---|
| Delta Sharing, provider side | `docs/milestones.md` M12, open-source sharing server | planned |
| recipient side | notebook in `databricks/` | Databricks lane |
| Lakehouse Federation | **not covered.** It needs an external database and a paid connector. Stated rather than faked | - |

## 5. Monitoring and alerting (10%)

| objective | where | evidence |
|---|---|---|
| pipeline metrics per run | evidence records carry duration, rows and digests | every claim |
| detecting a failed or partial run | MISSED INJECTION reporting in `faults/harness.py` | SG-07 |
| freshness against an SLA | `CONTRACT.md`, operational table | M9 |
| pipeline event log, alerts | Databricks lane, `databricks/resources/` | M9 |
| `system.lakeflow` / `system.access` | **not available on Free Edition** (no account console). `docs/limits.md` | - |

## 6. Cost and performance optimisation (13%)

| objective | where | evidence |
|---|---|---|
| file sizing and the small-file problem | the generator produces ~1 200 tiny files on purpose; the cost lab measures compaction | M8 |
| liquid clustering, `CLUSTER BY` versus partitioning | cost lab | M8 |
| deletion vectors and their read-side cost | cost lab | M8 |
| partition pruning and data skipping, measured in bytes read | cost lab, from the Delta log statistics | M8 |
| shuffle partitions and AQE | ADR 0005, and the shuffle-independence test | `tests/spark/` |
| DBU cost | **not measurable for free**: `system.billing` needs account-admin. Physical proxies are used and labelled as proxies | `docs/limits.md` |

## 7. Data security and compliance (10%)

| objective | where | evidence |
|---|---|---|
| grants declared as code | `databricks/resources/grants.yml` | M10 |
| row filters and column masks | `databricks/sql/policies.sql` | M10 |
| drift between declared and deployed | drift test in M10 | M10 |
| PII handling and minimisation | `CONTRACT.md`: the generated data carries no personal data by construction, which is stated rather than claimed as a control | - |
| account-level identity (SSO, SCIM, OAuth M2M) | **not available on Free Edition** | `docs/limits.md` |

## 8. Data governance (7%)

| objective | where | evidence |
|---|---|---|
| Unity Catalog: catalogs, schemas, volumes, managed tables | `databricks/databricks.yml` | M9 |
| lineage | captured from the Databricks lane as evidence screenshots | M9 |
| tags and ownership | bundle resources | M9 |
| a contract that is enforced rather than described | `CONTRACT.md` plus the test that compares it to `domain/contract.py` | fast lane |

## 9. Debugging and deploying (10%)

| objective | where | evidence |
|---|---|---|
| Declarative Automation Bundles (formerly Asset Bundles) | `databricks/databricks.yml`, `.github/workflows/databricks.yml` | M9 |
| deploying from CI with a token, and why not from inside the workspace | `PARITY.md` | M9 |
| environments (dev / prod targets) | bundle targets | M9 |
| debugging a failed run from its artefacts | `faults/harness.py` keeps every failed run's output | SG-07 |
| reproducing a run exactly | commit-derived seeds | SG-06 |

## 10. Data modelling (6%)

| objective | where | evidence |
|---|---|---|
| SCD Type 2, by full recomputation and by `MERGE` | `transform.dim_customer_scd2`, `pipelines/gold_scd2_merge.py` | SG-01, SG-05 |
| bitemporal modelling: system time versus valid time | `revenue_by_month(accounting_month, close_version, restated_at)` | SG-04 |
| medallion layering and what belongs in each layer | the design, and the fact that silver is allowed to contain duplicates while gold is not | SG-07 |
| keys, grain and the total order a digest needs | `verify/digest.Projection` | fast lane |

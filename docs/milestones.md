# Milestones

Where the repository is and what each remaining step buys. Hours are the author's estimate at
the point of writing; the ones already done carry their measured cost.

| id | milestone | state | hours |
|---|---|---|---|
| M0 | domain, contract, seeded generator with a by-construction ledger | done | 14 |
| M1 | DuckDB reference, canonical digest, typed verdicts, invariants | done | 12 |
| M2 | mutation engine (SQL AST + Python AST), specification mutants, witness matrix, equivalence classification | done | 16 |
| M3 | evidence store, README rendering, drift gate, `demo` in under 10 s | done | 8 |
| M4 | Spark implementation, agreement with the reference at every close, shuffle-independence | done | 10 |
| M5 | crash campaign on the silver stage with structural points | done | 10 |
| M6 | Delta lane: SCD2 by `MERGE`, CDF, time travel, `OPTIMIZE`, the delta CI job green | next | 14 |
| M7 | Spark Declarative Pipelines: the same transformations as a declarative pipeline, running locally and on Databricks | next | 10 |
| M8 | cost lab: file sizing, liquid clustering, deletion vectors, partition pruning, each measured before and after with repetitions and intervals | | 16 |
| M9 | Databricks Free Edition lane: bundle deploy from CI, Unity Catalog, expectations, AUTO CDC, event log, AI/BI dashboard, screenshots as evidence | | 18 |
| M10 | governance as code: declared grants, row filters and column masks, plus a drift test | | 10 |
| M11 | duplicate-escape measurement: stateful streaming dedup versus stateless dedup at the gold boundary, with the escape rate published | | 8 |
| M12 | Delta Sharing (open-source server) and a consumer notebook | | 6 |
| M13 | operational documentation: runbook, post-mortem of the restatement incident, on-call notes | | 6 |

Total remaining: about 88 hours.

## Order and why

M6 before M7 because a declarative pipeline that writes Parquet instead of Delta is a
different program, and finding that out after M7 would waste M7. M8 before M9 because a cost
experiment is worth more on a machine whose configuration is known than on a serverless
workspace whose sizing is invisible. M9 before M10 because a grant cannot drift before it has
been deployed once. M11 last of the measurement milestones because it is the only one whose
result may be uncomfortable, and it should not be able to delay the rest.

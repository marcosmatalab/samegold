# Milestones

Where the repository is and what each remaining step buys. Hours are the author's estimate;
the ones already done carry their measured cost.

| id | milestone | state | hours |
|---|---|---|---|
| M0 | domain, contract, seeded generator with a by-construction ledger and ten boundary cases | done | 16 |
| M1 | DuckDB reference, canonical digest, typed verdicts, invariants | done | 12 |
| M2 | mutation engine (SQL and Python AST), specification mutants, witness matrix, equivalence classification with assumption probes | done | 20 |
| M3 | evidence store with a hash chain and seed derivation, README rendering, drift gate, `demo` under 10 s | done | 12 |
| M4 | Spark implementation, agreement with the reference over the whole version history, shuffle and order independence | done | 12 |
| M5 | crash campaign with structural points and a negative control | done | 12 |
| M6 | bitemporal close: versions, `restated_at`, restatement reasons, in both implementations | done | 8 |
| M7 | SCD2 as a pure function with a property test, plus the thin Delta MERGE over it | done | 8 |
| M8 | cost lab on real Delta tables: compaction, clustering at two file sizes, partitioning, delete cost | done | 10 |
| M9 | governance: anonymisation, column classification, exposure check, retention purge | done | 8 |
| M9b | consumption layer and freshness alerting: `samegold report`, `serve/freshness.py`, the post-mortem | done | 4 |
| M10 | Delta on Spark green: `MERGE` with both branches and a delete by absence, CDF read as a feed, `OPTIMIZE ... ZORDER BY` measured in the transaction log, time travel, the delta CI job | done | 10 |
| M11 | Spark Declarative Pipelines running locally and on Databricks | next | 10 |
| M12 | Databricks Free Edition: bundle deploy from CI, Unity Catalog, expectations, AUTO CDC, event log, AI/BI dashboard, screenshots as evidence | **run, 3 September 2026**: correct close, two findings open | 18 |
| M13 | grants and masks deployed, with a drift test comparing deployed to declared | | 8 |
| M14 | the duplicate-escape measurement: stateful streaming dedup versus the stateless dedup at the gold boundary | | 8 |
| M15 | a pandas UDF and a Python UDF where they are genuinely the right tool, with the cost measured | | 4 |
| M16 | runbook, on-call notes, and the alert for a restatement larger than a declared threshold (the post-mortem itself is written: `docs/postmortem-2026-03-06.md`) | | 5 |

Remaining: about 53 hours.

## Order and why

M10 before M11 because a declarative pipeline that writes Parquet instead of Delta is a
different program, and finding that out after M11 would waste M11. That ordering earned its
keep: running M10 found a `MERGE` that could not complete its first call on any input, and it
would have been M11's problem to diagnose through a pipeline instead of through six tests. M12 before M13 because a
grant cannot drift before it has been deployed once. M14 late because it is the only one whose
result may be uncomfortable, and it should not be able to delay the rest.

M12 said "runnable, never run" for six rounds, and the distinction was the whole lesson of
round 12: the bundle deploying in one command is not the milestone, the milestone is a run.

**There has been one.** It ran end to end on 3 September 2026 and produced a correct close: `revenue_by_month` 2026-01 gross 14 198 046 from 425 lines and 2026-02 gross 199 379 from 3, to the cent against the values the OSS lane predicts; 727 accepted and 28 quarantined across seven reasons out of 755, conservation closed; the Long.MaxValue events out through `amount_out_of_range` and the 2^63 ones through `missing_required_field` with the column NULL.

The hours stay at 18, and M12 is not closed, because a run that produces a correct close is not
the same as a lane that is right. It produced two findings, and both are open:

- `publish_evidence.py` could not report the outcome of an update. `MAX` over a state string is
  the alphabetical maximum, so `last_state` published `WAITING_FOR_RESOURCES` for an update
  that COMPLETED, and would have published the same word for the one that FAILED that morning.
  Fixed with `max_by(state, timestamp)`, and now tested against a synthetic event log - but the
  record in the workspace was written by the old query.
- the two Type 2 dimensions disagree: 78 versions and 18 closed rows from AUTO CDC against 75
  and 15 from the hand-written MERGE. The cause is measured (three heartbeat upserts, which the
  contract says are not versions) and the fix is set
  (`track_history_column_list=["segment", "country"]`), and neither has been near a workspace.

So the honest state is: deployed, run, correct on the numbers it was built to get right, and
carrying two defects its first run exposed. The next run is what closes it.

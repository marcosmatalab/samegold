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
| M12 | Databricks Free Edition: bundle deploy from CI, Unity Catalog, expectations, AUTO CDC, event log, AI/BI dashboard, screenshots as evidence | **6 of 7 done**, verified against a committed record. The dashboard and one SQL alert are declared as bundle resources with their queries parsed and their widgets checked; what is left is deploy-from-CI and the screenshots, which need a workspace to be taken from | 18 |
| M13 | grants and masks deployed, with a drift test comparing deployed to declared | | 8 |
| M14 | the duplicate-escape measurement: stateful streaming dedup versus the stateless dedup at the gold boundary | | 8 |
| M15 | a pandas UDF and a Python UDF where they are genuinely the right tool, with the cost measured | | 4 |
| M16 | runbook, on-call notes, and the alert for a restatement larger than a declared threshold (the post-mortem itself is written: `docs/postmortem-2026-03-06.md`) | | 5 |

Remaining: about 53 hours.

## What v1.0.0 requires, and what it deliberately does not

**The criterion changed on 8 September 2026, and this section is the change rather than a
quiet edit.** It previously read as "every remaining milestone", which made the tag a function
of the backlog's length instead of the project's readiness. Three milestones are now explicitly
**out of scope for v1.0.0**, and they stay listed above with their hours because dropping them
from the table would be deleting the estimate rather than deferring the work.

**In scope for the tag:**

| id | why it gates the tag |
|---|---|
| M11 | the second implementation has to RUN. Until 8 September the open-source declarative lane was linted, type-checked and executed by nothing, and when it was finally run it did not start - `FINDINGS.md` carries it. A project whose thesis is that two implementations agree cannot tag while one of them has never produced a table |
| M12 | the cloud lane deploys from CI and carries screenshots. Deploy-from-CI is the last mechanical claim the repository makes about itself that nothing checks |
| M16 | the runbook, the on-call notes and the restatement alert. `docs/runbook.md` landed on 8 September; the alert is declared and paused, and the threshold work is what remains |

**Out of scope for the tag, and why:**

| id | hours | why it is not a tag blocker |
|---|---|---|
| M13 | 8 | grants and masks deployed with a drift test. Real work, and what it buys is **exam-syllabus coverage**: `databricks/sql/policies.sql` is already written, parsed and explained, and a reviewer reading this repository learns nothing new from it being applied to a workspace that has one account group containing one person |
| M14 | 8 | the duplicate-escape measurement. The most interesting of the three, and still not a tag blocker: the property it measures is already **stated** as a limit (silver is append-only and may hold duplicates; uniqueness is enforced at the gold boundary), and the size of the effect changes a number in `docs/limits.md` rather than anything a reviewer sees about the project |
| M15 | 4 | a pandas UDF and a Python UDF where they are genuinely the right tool. Syllabus coverage by construction - the phrase "where they are genuinely the right tool" is doing the work, and nothing in this close needs one |

**The criterion, stated so it can be argued with:** a milestone gates v1.0.0 if leaving it
undone would make something this repository *claims* unchecked, or would leave a reviewer
reading a sentence the code does not support. It does not gate the tag merely because it
appears on an exam syllabus. M13, M14 and M15 buy breadth of topic coverage; M11, M12 and M16
buy the difference between a claim and a checked claim.

That is **20 of the 53 hours declared as future work**, and the three remain in the table above
with their estimates so the total is still honest about what is unbuilt.

## Order and why

M10 before M11 because a declarative pipeline that writes Parquet instead of Delta is a
different program, and finding that out after M11 would waste M11. That ordering earned its
keep: running M10 found a `MERGE` that could not complete its first call on any input, and it
would have been M11's problem to diagnose through a pipeline instead of through six tests. M12 before M13 because a
grant cannot drift before it has been deployed once. M14 late because it is the only one whose
result may be uncomfortable, and it should not be able to delay the rest.

M12 said "runnable, never run" for six rounds, and the distinction was the whole lesson of
round 12: the bundle deploying in one command is not the milestone, the milestone is a run.

**There has been one, and it is right.** On 3 September 2026 the lane produced
`revenue_by_month` 2026-01 gross 14 198 046 from 425 lines and 2026-02 gross 199 379 from 3 -
to the cent against what the OSS lane computes on the same seed - with 727 accepted and 28
quarantined across seven reasons out of 755, conservation closed, `undecided_rules` empty, and
a Type 2 dimension of the same shape as the hand-written MERGE's. The record is committed at
`evidence/databricks/SG-DBX-01.json`, and the 21 anchored figures in `docs/databricks-run.md`
are rendered from it.

That sentence used to read "every figure in `docs/databricks-run.md` is rendered from it". The
document was 1117 lines holding 21 anchors, so the claim was wrong by a factor of about fifty -
and it was the sentence that justified nobody reading the other 1096. It is now 333 lines: what
was a finding moved to `FINDINGS.md`, what was a figure stayed and is rendered, and what was
neither was deleted.

**M12 IS NOT CLOSED**, and the reason is not a judgement about quality. Its own row lists seven
things. Four are done and checkable against that record; three have not been started:

| item | state | how that is known |
|---|---|---|
| Unity Catalog (catalog, schemas, volumes, grants) | done | the catalog step creates it with SQL; the record names the tables it read |
| expectations, per rule, from the event log | done | seven rules, fourteen numbers, in the record - and every one matches what the OSS predicates give on the same population |
| AUTO CDC Type 2 | done | 75 / 60 / 60 / 15, and equal to the hand-written MERGE's dimension ROW BY ROW - same customers, same intervals, same instants - against the workspace's own rows, committed |
| the event log read for the update's state | done | `update.last_state = COMPLETED`, `error_events = 0`, plus ten terminal updates in `update_history` |
| **bundle deploy from CI** | **not started** | `gh run list --workflow databricks.yml` returns nothing. The workflow exists and can deploy, seed, run and fetch on a `workflow_dispatch`; it has never been dispatched. Every deploy so far was from a laptop |
| **AI/BI dashboard** | **done** | `databricks/resources/dashboards.yml` declares the dashboard and one SQL alert, and `databricks/dashboards/samegold_close.lvdash.json` is the page. Deployed 6 September 2026: `Created dashboards.samegold_close_dashboard`, `Created alerts.samegold_close_not_sound` |
| **screenshots as evidence** | **not started** | none exist |

Two more things are DEPLOYED AND UNVERIFIED, which is a different state again and is why the
next run still has work to do:

- `pipelines.numUpdateRetryAttempts: "0"`. It landed after the retry loop it was written for,
  and every update since has succeeded. An update that succeeds does not exercise a retry
  setting; the next FAILED one tests it.
- ~~the four sections `publish_evidence.py` captures for the checklist items the record could
  not answer~~. `column_types`, `money_types`, `bad_events` and `rescued_rows` are all in the
  committed record now; this line described them as unrun for two rounds after they had run.
- the run writing the row-level dimension capture itself, with the deploy's commit carried in
  as a bundle variable. The capture in the repository was exported by hand and its header says
  so; the next fetch replaces it with one the workspace measured. Until then, what ties it to
  the record is the aggregates recomputing from its rows, which is a real tie and a weaker one
  than the header it is standing in for.

**The hours stay at 18 and that figure is an estimate nobody has measured against.** This
document has no record of hours spent, so changing the number would be inventing one. What can
be said with a number is the state above: five of seven done, two not started - bundle deploy from CI, and screenshots as evidence.

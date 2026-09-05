# Three runs, predicted before they ran

Written on 5 September 2026, before any of the three was launched, against the record from
`ad936aa` in `evidence/databricks/SG-DBX-01.json`. It exists to be **scored**, the way the
prediction in `docs/databricks-run.md` was scored and lost: a list written afterwards is a
description, and a description cannot be wrong.

Every figure below is either copied from that committed record, or computed here by the OSS
lane over the population the run will read. Nothing in it was estimated. Where a field cannot
be predicted, it is named as unpredictable and both outcomes are given a meaning in advance -
which is the only way an unpredicted field can still be evidence of something afterwards.

**The order is fixed and does not change while the runs are happening:** run 1 is the whole job
on unchanged data, run 2 is the deliberate failure and its repair, run 3 is the third close.
Run 1 is the cheapest and it validates the plumbing before a population is spent on the
interesting case.

---

## What all three share

These come out of the deploy rather than out of the data, and a deviation in any of them means
the run is not the run this document is about.

| field | value | a deviation means |
|---|---|---|
| `claim_id` | `SG-DBX-01` | the notebook that ran is not this one |
| `catalog` | `samegold` | the bundle was deployed against another catalog |
| `deploy.commit` | the sha the run was deployed from, 40 hex characters | `unknown` means a hand deploy; a `${var...}` means the bundle variable did not resolve |
| `deploy.tree_dirty` | `false`, as a JSON boolean | `true` means the evidence must not be committed; `null` means the record cannot say, which is the same thing |
| `chain.chained` | `false` | nothing about this lane belongs in `evidence/history.jsonl` |
| `column_types` | `{"qty": "bigint", "new_qty": "bigint", "unit_price_cents": "bigint"}` | a STRING here is the defect the whole lane was rebuilt around, returned |
| `money_types` | `{"gross_cents": "bigint", "returns_cents": "bigint", "net_cents": "bigint"}` | same |
| `rescued_rows` | `2` | the two events carrying 2^63 are in the base population and cannot change; another number means the reader changed |
| `bad_events` | the four ids `bad-0000007`, `bad-0000008`, `bad-0000016`, `bad-0000017`, quarantined as `amount_out_of_range` / `missing_required_field` | an `accepted` among them is `ELSE 'accepted'` returning |
| `undecided_rules` | `[]` | any row means a predicate could not answer, which bronze's declared types are supposed to make impossible |
| `update.last_state` | `COMPLETED` | `FAILED` is a pipeline problem and stops the run; `WAITING_FOR_RESOURCES` means `max_by` was replaced by `MAX` again |
| `update.error_events` | `0` | non-zero with a COMPLETED update is worth reading before anything else |
| `update_history` | one more terminal update than the run before, most recent first, capped at ten | two or more new ids from one launch is the retry loop that `pipelines.numUpdateRetryAttempts: 0` is supposed to have ended |

---

## Run 1 - the whole job, on data nothing has added to

Launched with no `fail_task` and no upload. Auto Loader has already ingested everything in the
landing volume, so the update writes no rows, the close finds nothing eligible that moved, the
condition takes the FALSE branch, and `verify_no_restatement` is the task that reports.

**What this run is for:** the branch nobody has executed. The false branch, `run_if: ALL_DONE`
with a skipped sibling, and the timeouts, all on the cheapest possible input.

### What must not move by one character

The population is unchanged, so every figure the committed record carries about the DATA has to
come back identical. This is the whole value of run 1: it is the one run where the answer is
already known, so anything that moves is the machinery moving, not the data.

| field | value that must come back |
|---|---|
| `rows.bronze_events` | `1328` |
| `rows.silver_classified` | `1328` |
| `rows.silver_events` | `1300` |
| `rows.silver_quarantine` | `28` |
| `rows.dim_customer_scd2` | `92` |
| `rows.revenue_by_month` | `2` |
| `rows.revenue_closed` | `3` |
| `population.digest` | `03a7b4b0d5251eedabdc58f8a13ba1a8377ea63f94713274db8516785f3a410d` |
| `population.digest_rows` | `1325` |
| `population.rows_outside_the_digest` | `3` |
| `dim_customer_scd2` | `versions 92, customers 60, open_rows 60, closed_rows 32` |
| `revenue_closed` | the same **three** rows, unchanged to the cent: 2026-01 v0 (14 198 046 / 12 911 212 / 425 / 71 / 22), 2026-01 v1 (25 582 615 / 23 268 535 / 793 / 126 / 32), 2026-02 v0 (199 379 / 199 379 / 3 / 0 / 0) |
| `quarantine_by_reason` | accepted 1300, amount_out_of_range 6, missing_required_field 5, negative_price 3, non_positive_quantity 6, unknown_currency 2, unknown_event_type 3, unparseable_json 3 |
| `gross_within_contract_bounds` | 2026-01 gross 25 582 615 over 793 lines, 2026-02 gross 199 379 over 3 lines, `above_contract_ceiling` false for both |

A change in `population.digest` with `rows.bronze_events` still 1328 is the worst outcome
available here and the one this field exists for: same count, different events. It means
something re-seeded the volume, or a duplicate arrival was ingested and something else was
dropped. It is not a parity failure and must not be read as one.

### What is new, field by field

| field | predicted value | why |
|---|---|---|
| `orchestration[0].decision` | `"no_op"` | nothing eligible moved, so `close_month` writes no version |
| `orchestration[0].versions_written` | `0` | |
| `orchestration[0].months_written` | `[]` | |
| `orchestration[0].branch` | `"verify_no_restatement"` | derived from the decision, not from a fourth task value |
| `orchestration[0].unresolved_task_values` | `[]` | `close_month` succeeded, so all three references resolve |
| `orchestration[0].expected_checks` | `["every_eligible_month_has_a_version", "no_eligible_month_drifted"]` | what the false branch owes |
| `orchestration[0].missing_checks` | `[]` | |
| `orchestration[0].task_states` | `close_month: success`, `verify_each_restated_month: excluded`, `verify_no_restatement: success` | `excluded` is the vocabulary the runtime uses for the branch that did not run |
| `close_verification` | **4 rows**, all `ok: true`: `every_eligible_month_has_a_version` and `no_eligible_month_drifted`, each for 2026-01 and for 2026-02 | two eligible months against an as-of month of 2026-09 |
| `incomplete` | `[]` | |
| the job's own state | `SUCCEEDED` | |

`verify_each_restated_month` produces **no task run at all** - a skipped branch is not a task
that ran and failed. If the run page shows it as failed rather than skipped, the condition's
outcome wiring is wrong and the for_each is being entered on the false branch.

### The one field that cannot be predicted

`expectations`. The seven rules reported `passed: 573, failed: 0` on the run of 4 September,
and 573 is the number of events that update INGESTED, not the number in the table - the
streaming tables are incremental, so the counts are per update. This update ingests nothing.

- **`expectations` comes back as `[]`** - the update produced no `flow_progress` event carrying
  data quality, because no flow processed a row. Correct, and it is why this run's record does
  not replace the canonical one: `expectations.table` is in the required anchor set, and
  rendering a document from a record with no expectations turns a measured table into
  `NOT RUN` - a regression whose diff reads like an update.
- **`expectations` comes back with seven rules at `passed: 0`** - the flows reported with
  nothing in them. Equally correct, and then the record could answer every anchor.

Either is a fact about how Lakeflow reports an empty update, which is worth learning and is not
worth guessing at in advance. What would be a defect is a third outcome: 573 again, or 1300,
which would mean the query lost its scope to the last update and is reporting the pipeline's
whole history the way it did before `57e2a13`.

### Where this run's record goes

Not over `evidence/databricks/SG-DBX-01.json`. That file is the one every document is rendered
from and every parity test compares against, and the rule for replacing it is written in
`evidence/databricks/README.md` and checked by
`tests/fast/test_databricks_bundle.py::test_the_committed_record_answers_every_anchor_the_documents_require`:
a run replaces it only if its record can answer every anchor the documents require. This run is
the case that rule was written for.

    scripts/databricks_run.sh fetch run-1

writes `SG-DBX-01.run-1.json` beside it, with its own capture and its own `fetch.run-1.json`,
and leaves the canonical record alone. The run is read, scored against this document by its job
run id, and replaces nothing.

### Also produced, and worth reading before run 2

Per-task durations, for the first time, on the run page. Every `timeout_seconds` in
`databricks/resources/jobs.yml` is currently a CEILING chosen against a cold start and says so.
This run is what turns them into measurements. That work is after run 1, not during it.

---

## Run 2 - the deliberate failure, and the repair

Launched with `--params fail_task=verify_no_restatement`, on the same unchanged data. The close
decides `no_op` again, the false branch runs and raises, and `publish_evidence` runs anyway
because `run_if: ALL_DONE`.

**What this run is for:** the claim that ALL_DONE keeps the evidence alive through a failure.
Which is only worth demonstrating if the evidence it keeps alive says the run failed.

### The failed record, field by field

Everything in "what must not move" above still holds - the data is untouched - and everything
in run 1's new-fields table holds except these:

| field | predicted value | why |
|---|---|---|
| `orchestration[0].branch` | `"verify_no_restatement"` | the close still succeeded, so the branch is still derivable |
| `orchestration[0].unresolved_task_values` | `[]` | **this is the point**: every task value resolves, and until this round that meant the record had nothing to say |
| `orchestration[0].expected_checks` | `["every_eligible_month_has_a_version", "no_eligible_month_drifted"]` | |
| `orchestration[0].missing_checks` | the same two names | the task died before writing a row |
| `orchestration[0].task_states` | `close_month: success`, `verify_each_restated_month: excluded`, `verify_no_restatement: failed` | |
| `close_verification` | `[]` - an empty list, not an error object | the table is readable and holds no rows for this job run |
| `incomplete` | `["verify_no_restatement"]` | named by the TASK that owed the rows; `"close_verification"` already means "the table could not be read" |
| the job's own state | `FAILED` | ALL_DONE decides what is recorded, not what is reported |

**A record identical to run 1's but for `close_verification: []` is the failure this round
fixed, arriving again.** If `missing_checks` is empty, or `incomplete` is empty, the derivation
in `databricks/src/publish_evidence.py` did not run - most likely because the deployment is
older than it.

### Two things that will go wrong if they are not done in this order

1. **Fetch before repairing, and fetch it under a label.** The record is written to one path
   in the volume and the repair overwrites it there; the failed record is the artefact this run
   exists to produce, and a repair before a fetch destroys it with the compute already spent.
   It also must not land on the canonical record - the artefact of a deliberately failed run
   cannot be the description of the lane that every page renders from, which is the same
   argument as the dirty-tree guard one step earlier in the same pipeline. So:

       scripts/databricks_run.sh fetch run-2-failed

   which writes `SG-DBX-01.run-2-failed.json` and leaves `SG-DBX-01.json` untouched.
2. **The repair has to turn the switch off.** `fail_task` is a job PARAMETER, and a repair run
   re-uses the run's parameters unless they are overridden. Repairing without overriding it
   fails in exactly the same place.

### The repaired record

A repair keeps `{{job.run_id}}` and issues a new `{{task.run_id}}`, which is what makes the
repair legible in the evidence:

| field | predicted value |
|---|---|
| `job_run_id` | the same value as the failed record |
| `task_run_id` | different from the failed record's |
| `close_verification` | the 4 rows run 1 predicted, all `ok: true` |
| `orchestration[0].missing_checks` | `[]` |
| `incomplete` | `[]` |

If `close_verification` comes back empty after a successful repair, the rows were written under
a different `job_run_id` - which would mean a repair is a new job run, and the record's filter
is looking at the wrong one. That is a finding about the platform, not a bug in the close, and
it is worth more than the run cost.

The repaired record is fetched under its own label too (`scripts/databricks_run.sh fetch
run-2-repaired`). It is a green record of a run that had failed, on unchanged data, so it has
nothing to say that run 3's will not say better; it is kept because the pair - failed, then
repaired, same job run id - is the demonstration, and half of it is not.

---

## Run 3 - the third close, and the true branch

The third close needs a third population: the base seed, the late seed already documented, and
a third late seed. **20260905** is proposed, for the same reason the other two are dates.

Everything below was computed by the OSS lane over that population before the run, with
`revenue_by_month_as_of` and `population_digest` from `src/samegold/generator/late.py`. These
are not estimates and there is no reason for the workspace to differ from them by a cent - a
difference IS the finding.

### The two things that had to be built first, and now are

1. **The batch prefix separated two populations and not three.** `src/samegold/generator/late.py`
   wrote every late batch as `batch=late-<stamp>`, and the stamp comes from the generating
   population - so two late seeds collide with each other exactly as freely as a late seed once
   collided with the base. Measured: of the **278** directories the second late arrival writes,
   **112** are names the first arrival already occupies, and in the volume those 112 replace
   files Auto Loader has already ingested. `late_batch_prefix(n)` generalises it to N arrivals
   (`late-`, `late2-`, `late3-`), the first staying unnumbered because its 269 directories are
   in the workspace and the committed record describes what was made of them. `FINDINGS.md`
   carries it as a class: a fix verified against the case that motivated it and never against
   the case after that.
2. **`population_for` now takes a sequence of late seeds**, and the three declarations name
   their populations that way: `DOCUMENTED_POPULATIONS` and `POPULATION_FACTS` in
   `tests/fast/test_databricks_dimension_parity.py`, `CLOSE_POPULATIONS` in
   `tests/fast/test_databricks_close_parity.py`. All three fail by name on a population they do
   not know, which is what they are for; none of them should ever be made to pass by being
   taught to accept whatever arrived.

The command that writes the arrival to upload, with the earlier one as already delivered:

    samegold generate-late --out /tmp/third --seed 20260901 \
        --late-seed 20260904 --late-seed 20260905

What lands under `/tmp/third/bronze` is arrival 2 alone - 278 directories, all `batch=late2-*` -
because the volume already holds everything before it.

### The third arrival, measured

| field | value |
|---|---|
| events kept | 555 |
| batch directories | 278 |
| by event type | order_placed 408, return_registered 91, order_line_amended 45, customer_upserted 11 |
| by event month | 2026-01 511, 2026-02 36, 2026-03 8 |
| already present in the base plus the second arrival | 236 |
| dropped without an id | 3 - the deliberately corrupt lines, which the filter cannot dedupe and does not re-deliver |

**Every one of the 408 new orders has a January sale timestamp.** The 36 February and 8 March
events are all `return_registered`, and a return books into the month of the SALE it refers to.
This is what decides the shape of the whole run, below.

### What the record will say

| field | predicted value | what it was |
|---|---|---|
| `rows.bronze_events` | **1883** | 1328 |
| `rows.silver_classified` | 1883 | 1328 |
| `rows.silver_events` | 1855 | 1300 |
| `rows.silver_quarantine` | **28, unchanged** | 28 |
| `rows.dim_customer_scd2` | 102 | 92 |
| `rows.revenue_by_month` | **2, unchanged** | 2 |
| `rows.revenue_closed` | **4** | 3 |
| `population.digest` | `f5415ee2f90c77a88a7e3a0e185ef458e20932bfee226a207419bbc16deeebac` | `03a7b4b0...` |
| `population.digest_rows` | 1880 | 1325 |
| `population.rows_outside_the_digest` | **3, unchanged** | 3 |
| `expectations` | seven rules, `passed: 555`, `failed: 0` | 573 / 0 |
| `quarantine_by_reason` | accepted **1855**; every other reason unchanged (6, 5, 3, 6, 2, 3, 3) | accepted 1300 |
| `rescued_rows` | **2, unchanged** | 2 |
| `dim_customer_scd2` | versions **102**, customers **60**, open_rows **60**, closed_rows **42** | 92 / 60 / 60 / 32 |
| `gross_within_contract_bounds` | 2026-01: gross **37 622 605** over **1158** lines; 2026-02: **199 379 over 3 lines, unchanged** | 25 582 615 / 793 |

The dimension arithmetic, which is the claim rather than the numbers: 106 distinct customer
upserts, 4 of them heartbeats (`cu-C000028-1`, `cu-C000038-1`, `cu-C000039-1`, `cu-C000043-1` -
**the same four**, because the third arrival adds ten upserts and every one of them changes a
tracked attribute), and 102 + 4 = 106. `closed_rows` is `versions - customers`: 102 - 60 = 42.

### The close, and the orchestration

**One month moves, not two.** February gains no version, for the third time and for the same
reason: no new February SALE arrived, so its aggregate is identical and the MERGE's `<>` guard
does what a restatement policy is for.

| field | predicted value |
|---|---|
| `orchestration[0].decision` | `"restated"` |
| `orchestration[0].versions_written` | `1` |
| `orchestration[0].months_written` | `["2026-01"]` |
| `orchestration[0].branch` | `"verify_each_restated_month"` |
| `orchestration[0].expected_checks` | the five checks, each suffixed `:2026-01` |
| `orchestration[0].missing_checks` | `[]` |
| `orchestration[0].task_states` | `close_month: success`, `verify_each_restated_month: success`, `verify_no_restatement: excluded` |
| `close_verification` | **5 rows**, all `ok: true`, all for 2026-01, `close_version` 2 |
| `incomplete` | `[]` |
| the for_each | **one iteration**, because the input list has one element |

**One iteration is what the data gives, and no other seed was looked for.** A third seed chosen
because of the number of months it happens to restate would be the data fitted to the
demonstration, which is the one thing this repository does not forgive anywhere else; 20260905
is a date, like 20260901 and 20260904, and the close is whatever it is. So the fan-out is one
iteration wide, and that is not a weaker demonstration: the width is set by
`{{tasks.close_month.values.months_written}}`, and a `for_each` that ran two iterations because
two months were written into the bundle would be the decoration this job was rebuilt to stop
being. The construct is correct or not for reasons that have nothing to do with the count - the
input is a task value, the concurrency is capped at 1, a failed iteration is separately
repairable - and one iteration exercises every one of them.

What is worth saying plainly, in the run document as well as here, is that **no close in this
repository has yet fanned out over two months**, and that the reason is the same rule deciding
the same way for the third time: a return books into the month of the SALE it refers to, so
late returns move January however late they arrive. A reader should not have to infer breadth
from the presence of a construct.

### The new row, to the cent

`revenue_closed` gains exactly one row, and the OSS lane says what is in it:

| accounting_month | close_version | gross_cents | returns_cents | net_cents | line_count | return_count | returns_rejected_count |
|---|---|---|---|---|---|---|---|
| 2026-01 | 2 | 37 622 605 | 3 858 662 | 33 763 943 | 1158 | 191 | 49 |

`restatement_reason` is whatever `close_month` writes for a restatement; the five checks include
`net_is_gross_minus_returns`, and 37 622 605 - 3 858 662 = 33 763 943 holds.

### The three rows that already exist

**These must come back byte-identical, and if any one of them moves the project is wrong.**
This is the claim the whole repository is built on - a version that has been signed off does not
change, a restatement is a NEW version - and it is checked here rather than assumed:

| accounting_month | close_version | gross_cents | returns_cents | net_cents | line_count | return_count | returns_rejected_count |
|---|---|---|---|---|---|---|---|
| 2026-01 | 0 | 14 198 046 | 1 286 834 | 12 911 212 | 425 | 71 | 22 |
| 2026-01 | 1 | 25 582 615 | 2 314 080 | 23 268 535 | 793 | 126 | 32 |
| 2026-02 | 0 | 199 379 | 0 | 199 379 | 3 | 0 | 0 |

A moved January v0 or v1 means the close is rewriting history in place, and every claim in
`CLAIMS.md` about bitemporality is false. A moved February v0 means a month with no new sales
was restated anyway, which is the `<>` guard gone. Neither is a test failure to be repaired;
both are the project being wrong.

### The committed capture

`evidence/databricks/dim_customer_scd2.json` holds 92 rows and is tied to the record by
`update_id`. Run 3 replaces both, together, or `tests/fast/test_databricks_dimension_parity.py`
fails by name - which is what it is for. The new capture holds **102** rows.

**Run 3 is the only one of the three that replaces the canonical record**, and it may because
it ingests 555 events: its `expectations` reports, every required anchor can be answered, and
the check in `tests/fast/test_databricks_bundle.py` that enforces the rule passes. It is fetched
without a label, the documents are re-rendered from it, and the three closes are then a chain a
reader can follow in one file.

---

## What would make me stop

If run 1 or run 2 shows any sign of Free Edition quota pressure - a task queuing for minutes on
serverless, a `bundle run` that will not start, compute refused - run 3 waits for tomorrow. It
costs nothing to postpone and the whole day's compute to get wrong.

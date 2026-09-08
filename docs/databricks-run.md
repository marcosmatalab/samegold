# The Databricks lane: what was deployed, and what it returned

> **State: run four times between 3 and 6 September 2026 against a real Free Edition
> workspace.** The records are committed under `evidence/databricks/` with their job run ids,
> and every figure below is rendered from the canonical one by `samegold readme`.
>
> **What this document is, after 8 September 2026.** It was 1117 lines, of which 21 were
> anchored - so `docs/milestones.md` could say "every figure here is rendered from the record"
> while fifty lines of prose sat between each pair of rendered ones, unchecked by anything.
> `docs/prose-audit-2026-09-07.md` found 38 of its 67 confirmed false statements pointing at
> this file. Fixing them one at a time would have put them back a round later, so the document
> was split three ways instead: what was a FINDING moved to `FINDINGS.md` with its class, what
> was a FIGURE stayed and is rendered from the record, and what was neither - narrative written
> before a run about what it would show, checklists scored against runs since superseded, and
> paragraphs describing the state of a round that ended - was deleted. That last kind is a
> diary, and the diary is already in the record and in the git history.

## What `make databricks` does

Two environment variables, one command:

```sh
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
make databricks
```

`scripts/databricks_run.sh` runs six steps, and any of them can be run alone
(`scripts/databricks_run.sh deploy`):

| step | what it does | why it is a step |
|---|---|---|
| `catalog` | creates the Unity Catalog catalog if missing, **with SQL** | a bundle cannot: there is no `catalogs` resource type, and on Default Storage `databricks catalogs create` fails outright |
| `validate` | `databricks bundle validate -t free` | the only step that needs no compute |
| `deploy` | `databricks bundle deploy -t free` | schemas, volumes, the pipeline, the job |
| `seed` | generates events with the OSS generator and uploads them to the landing volume | a pipeline over an empty directory reports nothing, and "no expectation failed" would arrive looking exactly like "no row was read" |
| `run` | `databricks bundle run samegold_close -t free` | the schedule is deployed **paused**; this is how it starts |
| `fetch` | copies `SG-DBX-01.json` out of the workspace into `evidence/databricks/` | a record that cannot leave the workspace is not evidence anyone can check |

`run-full-refresh` is the variant that re-reads the landing zone from scratch. It is needed
after a `schemaHints` change, because `cloudFiles.schemaLocation` caches the inferred schema,
and the cache is a path in the landing volume rather than pipeline-managed state - so it must
be deleted together with the refresh, never on its own.

## What is deployed

From `databricks/databricks.yml` and `databricks/resources/`:

- **one pipeline**, `samegold_pipeline`: serverless, triggered, `channel: CURRENT`,
  `development: true`. Three sources: `bronze_autoloader.py` (Auto Loader into `bronze_events`),
  `silver_expectations.py` (the quarantine rules as pipeline expectations plus the classified
  table) and `gold_close.py` (AUTO CDC Type 2 dimension, and the close as a materialized view).
- **one job**, `samegold monthly close`: the pipeline update, then `close_month.py`, then
  `publish_evidence.py`.
- **two schemas** and **two volumes**, with their grants.
- **not deployed**: `databricks/sql/policies.sql`. See "What Free Edition cannot show".

## What the run returned

Every figure in this section is inside a `dbx:` anchor and filled from
`evidence/databricks/SG-DBX-01.json` by `samegold readme`.
`tests/fast/test_databricks_bundle.py` fails if any of them holds a number while that record is
absent, and fails if any disagrees with it once present.

### The pipeline update

| | |
|---|---|
| last state | <!--dbx:update.last_state-->COMPLETED<!--/dbx--> |
| ERROR-level events | <!--dbx:update.error_events-->0<!--/dbx--> |

### Rows per table

| table | rows |
|---|---|
| `bronze_events` | <!--dbx:rows.bronze_events-->1883<!--/dbx--> |
| `silver_classified` | <!--dbx:rows.silver_classified-->1883<!--/dbx--> |
| `silver_events` | <!--dbx:rows.silver_events-->1853<!--/dbx--> |
| `silver_quarantine` | <!--dbx:rows.silver_quarantine-->30<!--/dbx--> |
| `dim_customer_scd2` | <!--dbx:rows.dim_customer_scd2-->102<!--/dbx--> |
| `revenue_by_month` | <!--dbx:rows.revenue_by_month-->2<!--/dbx--> |
| `revenue_closed` | <!--dbx:rows.revenue_closed-->4<!--/dbx--> |

`silver_events` is the expectation-filtered table and `silver_classified` is every row with a
reason attached, so `silver_classified = silver_events + silver_quarantine` is the conservation
identity this lane can be checked on without leaving it.

### Expectations, per rule

This is the piece open-source Spark Declarative Pipelines does not have, and the reason this
lane exists: the rules in `databricks/src/silver_expectations.py` are *declared*, so the event
log reports pass and fail counts for each one by name. The names are the contract's quarantine
reasons, not this lane's own vocabulary - `tests/fast/test_review_regressions.py` fails if they
drift, and `tests/spark/test_adversarial_records.py` compares these predicates against the OSS
`CASE` expression record by record.

`scripts/databricks_run.sh fetch` prints this table ready to paste.

<!--dbx:expectations.table-->| rule | dataset | passed | failed |
|---|---|---|---|
| amount_out_of_range | samegold.main.silver_events | 554 | 1 |
| missing_required_field | samegold.main.silver_events | 555 | 0 |
| negative_price | samegold.main.silver_events | 555 | 0 |
| non_positive_quantity | samegold.main.silver_events | 555 | 0 |
| unknown_currency | samegold.main.silver_events | 554 | 1 |
| unknown_event_type | samegold.main.silver_events | 555 | 0 |
| unparseable_json | samegold.main.silver_events | 555 | 0 |<!--/dbx-->

### Quarantine reasons, from the classified table

The same population counted the other way. Every reason here is a member of the contract's
closed enum; a reason that appears here and not in the table above is a rule the expectations
never reached.

<!--dbx:quarantine.table-->| quarantine reason | rows |
|---|---|
| accepted | 1853 |
| amount_out_of_range | 7 |
| missing_required_field | 5 |
| negative_price | 3 |
| non_positive_quantity | 6 |
| unknown_currency | 3 |
| unknown_event_type | 3 |
| unparseable_json | 3 |<!--/dbx-->

### AUTO CDC: the Type 2 dimension

`dp.create_auto_cdc_flow(..., stored_as_scd_type=2)` is one of the four Databricks-only calls
`PARITY.md` pins against the open-source signatures they fail on. The OSS lane maintains the
same dimension by hand with a two-pass `MERGE` in `src/samegold/pipelines/gold_scd2_merge.py`;
that the two agree is the point of having both.

| | |
|---|---|
| version rows | <!--dbx:dim.versions-->102<!--/dbx--> |
| distinct customers | <!--dbx:dim.customers-->60<!--/dbx--> |
| open rows (`__END_AT IS NULL`) | <!--dbx:dim.open_rows-->60<!--/dbx--> |
| closed rows | <!--dbx:dim.closed_rows-->42<!--/dbx--> |

Open rows must equal distinct customers: one current version per key is what Type 2 means, and
a dimension with two open rows for one customer is the defect the hand-written `MERGE` on the
OSS lane has a delete-by-absence branch for.


## The close, recomputed rather than transcribed

Computed by the DuckDB reference over the reproduced population -
`tests/fast/test_databricks_close_parity.py` recomputes it on every run of the fast lane and
compares five columns against the record:

| month | version | gross_cents | net_cents | line_count | return_count | rejected |
|---|---|---|---|---|---|---|
| 2026-01 | 0 | 14 198 046 | 12 911 212 | 425 | 71 | 22 |
| 2026-01 | 1 | 25 582 615 | 23 268 535 | 793 | 126 | 32 |
| 2026-02 | 0 | 199 379 | 199 379 | 3 | 0 | 0 |

Three properties, each a different thing that could have gone wrong:

- **version 0 is untouched**, figures and `restated_at` both. A restatement that rewrites the
  signed-off version has destroyed the evidence that it moved.
- **February gains no version.** The late returns fall in February and March but are returns
  against JANUARY sales; `gold_close.py` groups by the month of the sale, so February's
  aggregate is unchanged and the MERGE's `<>` guard stops a close restating a month that did
  not move.
- **conservation closes over the whole population**: `bronze_events` = `silver_classified` =
  1328 = 755 + 573, and `silver_events` 1300 + `silver_quarantine` 28 = 1328.

The late population is reproducible with one command, which is what makes the figures above
checkable by a reader:

```sh
samegold generate-late --out /tmp/late --seed 20260901 --late-seed 20260904
```

`tests/fast/test_late_arrivals.py` fails if it does not print 573 late events in 269 batch
directories.

## The three runs of 5 September, and which record is canonical

| run | job run id | what it was for | what it left |
|---|---|---|---|
| 1 | `592180158314216` | the false branch, on data nothing had added to | `SG-DBX-01.run-1-no-op.json` |
| 2 | `44869473800771` | a deliberate failure, and its repair | `SG-DBX-01.run-2-failed.json` |
| 3 | `517089489320521` | the third close, and the true branch | `SG-DBX-01.json` - the canonical record |

Runs 1 and 2 do not replace the canonical record and are not rendered from.
`evidence/databricks/README.md` is why: run 1 ingested nothing, so it reported no expectations
at all, and committing it would have turned a measured table on this page into `NOT RUN`.

### What the job decided, from the record

| | |
|---|---|
| the close's decision | <!--dbx:orch.decision-->restated<!--/dbx--> |
| the branch it took | <!--dbx:orch.branch-->verify_each_restated_month<!--/dbx--> |
| versions written | <!--dbx:orch.versions_written-->1<!--/dbx--> |
| months restated | <!--dbx:orch.months_written-->1<!--/dbx--> |
| verification checks run | <!--dbx:orch.checks_run-->5<!--/dbx--> |
| of which failed | <!--dbx:orch.checks_failed-->0<!--/dbx--> |

Those six are rendered from `orchestration` and `close_verification` in the record, and the last
two are only offered at all when the record positively shows that the branch wrote what it owed.
A run whose verification never reported renders them as `NOT RUN` rather than as two zeros -
which is the trap run 2 was built to spring, and did.


### The durations, which turn the ceilings into measurements

Read off the run page rather than out of the record - the Jobs API knows them and the notebook
does not:

| task | measured | ceiling | margin |
|---|---|---|---|
| `ingest_and_transform` | 81 s | 600 s | 7.4x |
| `close_month` | 51 s | 600 s | 11.8x |
| `did_the_close_restate` | 0 s | none (starts no compute) | |
| `verify_no_restatement` | 6 s | 600 s | 100x |
| `publish_evidence` | 34 s | 900 s | 26.5x |

`databricks/resources/jobs.yml` carries each of these beside the timeout it justifies. They stay
loose on purpose: a ceiling on this account has to end a hang before it spends the day's
compute, and must never kill a healthy run on a cold allocation - a serverless cold start alone
is one to two minutes.

## The dashboard and the alert

Declared in `databricks/resources/dashboards.yml`, with the page in
`databricks/dashboards/samegold_close.lvdash.json` - a file rather than a blob folded into YAML,
so it is reviewable and diffable.

`publish_evidence.py` writes two tables per run, `job_run_status` and `job_run_expectations`,
because Free Edition has no account console and therefore no `system.lakeflow`: nothing in SQL
can otherwise answer "how did the last run of this job end?". A file in a volume can be neither
charted nor alerted on.

`samegold_close_not_sound` fires when the most recent `job_run_status` row has `ok = false`, or
when there is no row at all. It deliberately does not watch the job's terminal state - see the
`SUCCESS_WITH_FAILURES` finding below. It is deployed **PAUSED** (the warehouse it wakes is the
compute the close needs, on a shared daily quota) and declares **no notification destination**,
which is a refusal rather than an omission: an email address in a public repository is worse
than no destination at all.

Checked in this repository on every push: that the bundle declares both with every field their
create APIs require; that every widget reads a dataset the file declares and every dataset is
read by some widget; that every encoded field is one its query selects; that the table names use
the catalog the bundle deploys to; and that **the six dataset queries go through Spark's parser
and resolve against views with the real column names**
(`tests/spark/test_databricks_lane_parses.py`). A dashboard is SQL that nothing compiles, and
its failure mode is an empty widget that looks like an answer.

Not checkable here, and only a deploy can: whether the alert's evaluation binds to the `not_ok`
column as written, and **what it looks like** - there are no screenshots in `docs/` yet, because
a screenshot has to be taken from a browser signed in to the workspace.

## What Free Edition cannot show, and what was done instead


| limit | what it costs this lane | what is here instead |
|---|---|---|
| one active pipeline per type | no separate dev and prod pipelines | one pipeline, `development: true` so a failed update does not retry into the quota |
| 5 concurrent job tasks | no fan-out | three tasks in a chain, `max_concurrent_runs: 1` |
| one SQL warehouse, 2X-Small | a bundle can neither create one nor learn its id | no `sql_task` anywhere - that is a JOB task and would need an id at run time, on the one thing that must deploy and run from a clean account. The dashboard and the alert do need one, and the id is resolved one layer out: `scripts/databricks_run.sh deploy` asks `warehouses list` - the same call the catalog step makes - and passes it as a bundle variable. The bundle carries a variable, never an id. **`databricks/sql/policies.sql` is still declared and not applied**, and it is worth being exact about why now that the id is reachable: applying it means EXECUTING SQL against a warehouse, which is a different act from attaching a resource to one, and nothing in this lane does it yet |
| no account console, no account APIs | no `system.billing`, no `system.lakeflow`, no DBUs | pipeline-level counts from the event log, labelled as counts and never as cost |
| no SSO, no SCIM | no service principals, no account groups | a PAT for authentication; `account users` is the only principal a grant can name, and it contains exactly one person - the deployer |
| no external locations | nowhere to put files but a volume | two managed volumes; Auto Loader runs in directory-listing mode, not file notification |
| quota exhaustion stops compute for the day | an unattended nightly schedule can take the account down | the schedule is deployed `PAUSED`; runs are started by hand |
| Default Storage, and therefore no metastore storage root | `databricks catalogs create` fails with `Metastore storage root URL does not exist` ([databricks/cli#4513](https://github.com/databricks/cli/issues/4513)) | the catalog is created with `CREATE CATALOG IF NOT EXISTS` through `POST /api/2.0/sql/statements` on the one 2X-Small warehouse, which resolves its location through Default Storage. The script waits 30s, cancels on timeout, and refuses to continue unless the statement reports `SUCCEEDED` |

Three consequences worth stating plainly, because they are the ones that would otherwise be read

Three consequences worth stating plainly, because they would otherwise read as achievements:

- **The row filter and the column mask are not enforced here.** `databricks/sql/policies.sql`
  declares both, and `is_account_group_member('finance_all')` resolves to false for everyone on
  an account with no groups. Nothing in `make databricks` runs that file; it is checked by a
  parser and by nothing else.
- **Cost is not measured on this lane at all.** The figures above are rows and events. The cost
  work lives in the OSS lane, where files and bytes come out of the Delta log and are labelled
  as a proxy for DBUs rather than converted into one.
- **No job health rule is declared, and that is a refusal.** A `health:` block pairs a threshold
  with a NOTIFICATION, and notifying is all it does. This account has no destination, so the
  rule would fire into nothing - and would be indistinguishable, in a bundle, from one that
  works. What it would have watched is watched by something that ACTS instead:
  `timeout_seconds` on the job and on every task.

## What this lane found, and where each one is written

The findings this lane produced are in `FINDINGS.md` with their defect class, and the platform
behaviours they rest on are in `docs/limits.md` with their measurements. They are NOT restated
here; a finding narrated in two places drifts in one of them.

| what happened | where it is |
|---|---|
| One workspace evaluated the same predicate two ways: the pipeline as `ansi=false`, the SQL warehouse as `ansi=true`, so `unit_price_cents > 1000000` was NULL in one and `true` in the other. An INT32 literal, a STRING column, and a row that fell through to `ELSE 'accepted'` | `docs/limits.md`, "One workspace, two ANSI modes" - with the four-row measurement table |
| A job whose close verification FAILED reported `SUCCESS_WITH_FAILURES`, because the evidence task runs `run_if: ALL_DONE`, is last in the graph, and succeeded. Alerting on terminal state is unsafe here | `FINDINGS.md`; the alert above is the answer to it |
| A task declared `max_retries: 0` and ran twice anyway, 57 seconds apart - serverless auto-optimization retried it, and the declaration never reached the API | `FINDINGS.md`, "A declaration that does not govern" |
| The late population was first produced by a script in `/tmp`, so every figure the second close published rested on data no reader could regenerate | `FINDINGS.md`; `samegold generate-late` is the fix |
| `databricks bundle validate` answered `Validation OK!` on a bundle the API rejects at the first POST, because `resources.pipelines.samegold_pipeline` carried no `name` | `FINDINGS.md`, this round |
| `MAX(state)` is the alphabetical maximum, so a completed update published `WAITING_FOR_RESOURCES` | `FINDINGS.md`; `max_by(state, timestamp)` is the fix |
| AUTO CDC produced 78 versions against the hand-written MERGE's 75, because its default is a new version whenever ANY column changes | `FINDINGS.md`; `track_history_column_list` is the fix |
| `warehouse_id` defaulted to the empty string, and the comment justifying it cited a CI behaviour that had not once occurred | `FINDINGS.md`, "A written reason is a claim with no test behind it" |

## If the next command fails

Moved to `docs/runbook.md`, which is the page for it: this document describes what a run
returned, and a symptom table is read while something is on fire. It is not copied - there is
one of it, in the runbook, beside what fires the alert, how to tell a data problem from a
platform one, and how to repair a run without spending the day's quota.

## What to distrust in this document

- Everything above was produced by one person with one workspace, and none of it is in the
  hash chain. `evidence/databricks/README.md` sets out exactly what that costs.
- The pipeline sources use `from pyspark import pipelines as dp`, the Spark 4 declarative API.
  If the runtime on `channel: CURRENT` does not expose it, the first update fails at import
  and every count above stays `NOT RUN` - which is the correct outcome, not a bug in the
  document.
- An `incomplete` list in `evidence/databricks/SG-DBX-01.json` names any section the notebook
  could not read. A section that failed is a hole, not a zero, and the anchors above will hold
  the error message rather than a number.

# The Databricks lane: what was deployed, and what it returned

> **State: deployed zero times.** Every figure below sits inside an HTML-comment anchor named
> `dbx:<field>`, and every one of them currently reads `NOT RUN`.
> `tests/fast/test_databricks_lane.py` fails if any of them holds a
> number while `evidence/databricks/SG-DBX-01.json` is absent, and fails if any of them
> disagrees with that record once it is present. So this document cannot get ahead of the run
> by hand, which is the failure mode the whole repository is about: for eleven rounds
> `docs/limits.md` said the Delta lane was "not executed here" while CI had been running it,
> red, for two days.

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
| `catalog` | creates the Unity Catalog catalog if missing | a bundle **cannot**: there is no `catalogs` resource type, and a schema whose catalog does not exist fails at deploy time |
| `validate` | `databricks bundle validate -t free` | the only step that needs no compute |
| `deploy` | `databricks bundle deploy -t free` | schemas, volumes, the pipeline, the job |
| `seed` | generates events with the OSS generator and uploads them to the landing volume | a pipeline over an empty directory reports nothing, and "no expectation failed" would arrive looking exactly like "no row was read" |
| `run` | `databricks bundle run samegold_close -t free` | the schedule is deployed **paused**; this is how it starts |
| `fetch` | copies `SG-DBX-01.json` out of the workspace into `evidence/databricks/` | a record that cannot leave the workspace is not evidence anyone can check |

## What is deployed

From `databricks/databricks.yml` and `databricks/resources/`:

- **one pipeline**, `samegold_pipeline`: serverless, triggered (never continuous), `channel:
  CURRENT`, `development: true`. Three sources: `databricks/src/bronze_autoloader.py` (Auto
  Loader, directory listing, into `bronze_events`), `databricks/src/silver_expectations.py`
  (the quarantine rules as pipeline expectations plus the classified table) and
  `databricks/src/gold_close.py` (AUTO CDC Type 2 dimension, and the close as a materialized
  view).
- **one job**, `samegold monthly close`, three tasks in a chain: the pipeline update, then
  `databricks/src/close_month.py` (the bitemporal close, as a notebook task), then
  `databricks/src/publish_evidence.py` (the record).
- **two schemas**, `main` and `raw`, with their grants, from `databricks/resources/grants.yml`.
- **two volumes**, `raw.landing` and `raw.evidence`, from `databricks/resources/volumes.yml`.
- **not deployed**: `databricks/sql/policies.sql`. See the last section.

## What the run returned

### The pipeline update

| | |
|---|---|
| last state | <!--dbx:update.last_state-->NOT RUN<!--/dbx--> |
| ERROR-level events | <!--dbx:update.error_events-->NOT RUN<!--/dbx--> |

### Rows per table

| table | rows |
|---|---|
| `bronze_events` | <!--dbx:rows.bronze_events-->NOT RUN<!--/dbx--> |
| `silver_classified` | <!--dbx:rows.silver_classified-->NOT RUN<!--/dbx--> |
| `silver_events` | <!--dbx:rows.silver_events-->NOT RUN<!--/dbx--> |
| `silver_quarantine` | <!--dbx:rows.silver_quarantine-->NOT RUN<!--/dbx--> |
| `dim_customer_scd2` | <!--dbx:rows.dim_customer_scd2-->NOT RUN<!--/dbx--> |
| `revenue_by_month` | <!--dbx:rows.revenue_by_month-->NOT RUN<!--/dbx--> |
| `revenue_closed` | <!--dbx:rows.revenue_closed-->NOT RUN<!--/dbx--> |

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

<!--dbx:expectations.table-->
NOT RUN
<!--/dbx-->

### Quarantine reasons, from the classified table

The same population counted the other way. Every reason here is a member of the contract's
closed enum; a reason that appears here and not in the table above is a rule the expectations
never reached.

<!--dbx:quarantine.table-->
NOT RUN
<!--/dbx-->

### AUTO CDC: the Type 2 dimension

`dp.create_auto_cdc_flow(..., stored_as_scd_type=2)` is one of the three Databricks-only calls
`PARITY.md` pins against the open-source signatures they fail on. The OSS lane maintains the
same dimension by hand with a two-pass `MERGE` in `src/samegold/pipelines/gold_scd2_merge.py`;
that the two agree is the point of having both.

| | |
|---|---|
| version rows | <!--dbx:dim.versions-->NOT RUN<!--/dbx--> |
| distinct customers | <!--dbx:dim.customers-->NOT RUN<!--/dbx--> |
| open rows (`__END_AT IS NULL`) | <!--dbx:dim.open_rows-->NOT RUN<!--/dbx--> |
| closed rows | <!--dbx:dim.closed_rows-->NOT RUN<!--/dbx--> |

Open rows must equal distinct customers: one current version per key is what Type 2 means, and
a dimension with two open rows for one customer is the defect the hand-written `MERGE` on the
OSS lane has a delete-by-absence branch for.

## What Free Edition cannot show, and what was done instead

The limits are taken as given rather than worked around. Where one bites, it is declared here
and in `docs/limits.md` rather than papered over.

| limit | what it costs this lane | what is here instead |
|---|---|---|
| one active pipeline per type | no separate dev and prod pipelines | one pipeline, `development: true` so a failed update does not retry into the quota |
| 5 concurrent job tasks | no fan-out | three tasks in a chain, `max_concurrent_runs: 1` |
| one SQL warehouse, 2X-Small | no warehouse id at bundle time | no `sql_task` anywhere; `databricks/sql/policies.sql` is therefore **declared and not applied** |
| no account console, no account APIs | no `system.billing`, no `system.lakeflow`, no DBUs | pipeline-level counts from the event log, labelled as counts and never as cost |
| no SSO, no SCIM | no service principals, no account groups | a PAT for authentication; `account users` is the only principal a grant can name, and it contains exactly one person - the deployer |
| no external locations | nowhere to put files but a volume | two managed volumes; Auto Loader runs in directory-listing mode, not file notification |
| quota exhaustion stops compute for the day | an unattended nightly schedule can take the account down | the schedule is deployed `PAUSED`; runs are started by hand |

Two consequences worth stating plainly, because they are the ones that would otherwise be read
as achievements:

- **The row filter and the column mask are not enforced here.** `databricks/sql/policies.sql`
  declares both, and `is_account_group_member('finance_all')` resolves to false for everyone on
  an account with no groups. Applying them would need a SQL warehouse id that the bundle does
  not have and cannot create, so nothing in `make databricks` runs that file. It is checked by
  a parser (`tests/spark/test_databricks_lane_parses.py`) and by nothing else.
- **Cost is not measured on this lane at all.** The numbers above are rows and events. The
  cost work lives in the OSS lane, where files and bytes come out of the Delta log and are
  labelled as a proxy for DBUs rather than converted into one.

## If the first command fails

This bundle has never been handed to a `databricks` CLI, and neither has the script. The
statements in it are parsed by a real Spark parser in `tests/spark`, and the bundle is checked
against the Free Edition limits in `tests/fast/test_databricks_lane.py`, but neither of those
is `databricks bundle validate`. These are the fields most likely to be the first thing that
goes wrong, and what each one means - written before the run rather than after it, so that the
list is a prediction and can be scored:

| symptom | field | what it means |
|---|---|---|
| `validate` rejects an unknown field | `development: true` on the pipeline, or `resources.volumes` | the CLI is older than the field. Upgrade the CLI rather than deleting the field: `development` is what stops a failed update retrying into the quota |
| `deploy` fails on a missing catalog | none | `scripts/databricks_run.sh catalog` did not run, or the token cannot create a catalog. A bundle cannot declare one |
| the pipeline fails at import with `ModuleNotFoundError: pyspark.pipelines` | the three `libraries.file` sources | the runtime on `channel: CURRENT` does not expose the Spark 4 declarative API under that name. The lane is written against it deliberately - `PARITY.md` explains why - and this is the honest way to find out |
| `cluster_by_auto` is rejected | `gold_close.py` | automatic liquid clustering needs predictive optimization on the metastore. If Free Edition does not enable it, that belongs in the table above as a limit, not as a workaround |
| the run finishes and every count is 0 | the `seed` step | nothing was in the landing volume. `databricks fs ls dbfs:/Volumes/<catalog>/raw/landing` says whether the upload happened |
| `job_run_id` in the record reads `{{job.run_id}}` | `resources/jobs.yml` | the runtime did not recognise that dynamic value reference and passed the text through. The record shows it rather than hiding it behind a blank |

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

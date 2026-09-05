# The Databricks lane: what was deployed, and what it returned

> **State: deploy attempted once, on 2 September 2026, against a real Free Edition workspace.
> It failed, so every figure below still reads `NOT RUN`.** `databricks bundle validate -t
> free` answered `Validation OK!` and `databricks bundle deploy -t free` then died on the
> first POST with `cannot create resources.pipelines.samegold_pipeline: name must be set (400
> INVALID_PARAMETER_VALUE)`, taking the job with it as a failed dependency. Ten files had
> already uploaded. What that cost and what was done about it is in the last two sections.
>
> Every figure below sits inside an HTML-comment anchor named `dbx:<field>`, and every one of
> them currently reads `NOT RUN`. `tests/fast/test_databricks_bundle.py` fails if any of them
> holds a number while `evidence/databricks/SG-DBX-01.json` is absent, and fails if any of
> them disagrees with that record once it is present. So this document cannot get ahead of the
> run by hand, which is the failure mode the whole repository is about: for eleven rounds
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
| `catalog` | creates the Unity Catalog catalog if missing, **with SQL** | a bundle cannot: there is no `catalogs` resource type. Nor can the Unity Catalog API on Free Edition - see below |
| `validate` | `databricks bundle validate -t free` | the only step that needs no compute |
| `deploy` | `databricks bundle deploy -t free` | schemas, volumes, the pipeline, the job |
| `seed` | generates events with the OSS generator and uploads them to the landing volume | a pipeline over an empty directory reports nothing, and "no expectation failed" would arrive looking exactly like "no row was read" |
| `run` | `databricks bundle run samegold_close -t free` | the schedule is deployed **paused**; this is how it starts |
| `fetch` | copies `SG-DBX-01.json` out of the workspace into `evidence/databricks/` | a record that cannot leave the workspace is not evidence anyone can check |

## The re-ingestion, in order, and what each step must print

`make databricks` is `all`: catalog, validate, deploy, seed, run. **It does not full-refresh
and it does not re-seed**, so it is the wrong command for the run that has to prove the type
fix. The sequence below is the right one, and it is written down rather than assembled at the
keyboard because on Free Edition a wrong update costs the whole day's compute quota.

Three decisions are baked into the order, and each one is a question that has a wrong answer.

**Re-seed? Yes, and it is not optional.** The landing volume holds events written by the
generator as it was before round 18, which emitted eight kinds of corrupt record. It now emits
nine (`beyond_bigint`, a price of 2^63). Every expected number in the checklist below is for
the nine-kind population, so a run over the old bytes would disagree with all of them and the
disagreement would say nothing.

**Delete before seeding? Yes.** `step_seed` uses `databricks fs cp -r --overwrite`, which is a
COPY and not a sync: it replaces files whose names collide and leaves behind any file the new
generation no longer produces. Batch directory names come from arrival timestamps, so a stale
file from the old population can survive and be ingested beside the new one. Emptying the
landing directory is what makes the population definite. It also makes `SAMEGOLD_RESEED=1`
redundant - `step_seed` skips only when the volume is non-empty - which is why it is passed
anyway: it is a guard against a deletion that half-worked, not the mechanism.

**Delete `_schema` too?** `cloudFiles.schemaLocation` is `{landing}/_schema`, an explicit path
in the landing volume rather than pipeline-managed state, and it CACHES the schema Auto Loader
inferred on the first run - the all-STRING one. `--full-refresh-all` resets the pipeline's
tables and its own checkpoints. **Whether it also clears a schema location the source names
explicitly has not been tested here**, and this document does not get to assume it: the failure
mode is bronze coming back as STRING, the type fix looking as though it did not work, and a
correct fix getting reverted. Deleting the directory costs one command and removes the
question. It must be deleted TOGETHER with the full refresh and never on its own - a
re-inferred schema under an existing checkpoint is how a streaming table fails on a schema
change instead of on a type.

**Seed first, then refresh.** The refresh re-reads whatever is in the landing zone at the
instant it starts. Refreshing before seeding reprocesses the old population, produces a wrong
close, and spends an update - and the second one is the part that matters here, because two
updates is what the daily quota does not stretch to.

```sh
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
CATALOG=samegold                       # SAMEGOLD_CATALOG overrides it everywhere below
```

**1. Deploy the fixed sources.** `deploy` runs catalog, validate and deploy.

```sh
scripts/databricks_run.sh deploy
```

Must print `==> catalog samegold` then `  exists`; `==> bundle validate -t free` ending in
`Validation OK!`; and `==> bundle deploy -t free` ending in `Deployment complete!`. If validate
says OK and deploy dies on a 400, that is the round-13 finding repeating and the message names
the field.

**2. Empty the landing zone, schema cache included.**

```sh
databricks fs rm -r  "dbfs:/Volumes/$CATALOG/raw/landing"
databricks fs mkdir  "dbfs:/Volumes/$CATALOG/raw/landing"
databricks fs ls     "dbfs:/Volumes/$CATALOG/raw/landing"
```

The third command must print **nothing at all**. If it lists `_schema`, the delete did not
reach it and the run that follows will be inferring nothing: stop and delete it by name. The
volume itself is a Unity Catalog object the bundle owns and survives the `rm`; the `mkdir` only
puts the directory back, and `step_seed` would do it anyway.

**3. Seed the new population.**

```sh
SAMEGOLD_RESEED=1 scripts/databricks_run.sh seed
```

Must print `==> seed dbfs:/Volumes/samegold/raw/landing`, then from the generator

```
755 events in 298 files under /tmp/...
ledger: /tmp/.../truth/ledger.json
```

and then `  uploaded 298 files`. **If it prints `the landing volume already has files; not
seeding again`, step 2 did not empty it** - the run would be over the old bytes. If the counts
are not 755 and 298, the seed or the profile is not the default pair (`SAMEGOLD_SEED=20260901`,
`SAMEGOLD_PROFILE=fast`) and every expected value below is for that pair and no other.

**4. The one update.**

```sh
scripts/databricks_run.sh run-full-refresh
```

Must print `==> FULL REFRESH: the pipeline will re-read the landing zone from scratch`, then
`  needed after a schemaHints change, because the inferred schema is cached`, then
`==> bundle run samegold_close -t free` and the CLI's link to the run. This is the command that
can cost the afternoon; everything above it is cheap and everything below it is read-only.

**5. Fetch the record.**

```sh
scripts/databricks_run.sh fetch
```

Must print the two paths under `evidence/databricks/` and then the two tables ready to paste
into the anchors in this document. If it prints a `SECTIONS THAT COULD NOT BE READ` line, those
sections are holes and the anchors take the error message, not a zero.

Then run the checklist below **before** pasting anything. A record that reports numbers is not
the same as a lane that is right.

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

## The first real run, and why its numbers are not in the anchors below

The lane was deployed and run against a Free Edition workspace on 2 September 2026. `deploy`,
`seed` and the whole pipeline succeeded. `close_month` then died with
`DELTA_CAST_OVERFLOW_IN_TABLE_WRITE`, writing a DOUBLE into `gross_cents BIGINT`.

**The run produced figures. They are wrong, so they are written here and NOT pasted into the
anchors below**, which still read `NOT RUN`. An anchor is for a number the lane earned; this
section is for a number it did not.

| measured in the workspace | value |
|---|---|
| `DESCRIBE revenue_by_month` | `gross_cents`, `returns_cents`, `net_cents` = **double** |
| `DESCRIBE silver_classified` | 21 columns, **all STRING** except `_ingested_at` |
| `revenue_by_month` 2026-01 | gross **2.767e19** from 428 lines |
| `revenue_by_month` 2026-02 | gross 199 379 from 3 lines |
| `silver_classified` | `bad-0000007`, `bad-0000015`, `bad-0000023`: `order_placed`, `qty='1'`, `unit_price_cents='9223372036854775807'`, `quarantine_reason='accepted'` |
| `'9223372036854775807' > 1000000` in the SQL warehouse | `true` |
| the same records through the OSS `CASE` | `amount_out_of_range` |
| `silver_events` for those three ids | zero rows - the EXPECTATIONS dropped them |
| the deployed file vs the repository | identical |

The contract caps one line at 10 000 x 1 000 000 = 1e10 cents. Those three events are worth
9.22e18 each and account for the whole of January's gross: six and a half million times the
ceiling of a single line, from events the generator emits **in order to be rejected**.

### The mechanism, measured rather than deduced

The expectations dropped those rows and the classification accepted them, from the same rules
in the same file. The only shape consistent with all of the above is that
`unit_price_cents > 1000000` returned NULL inside the pipeline while returning `true` in the
warehouse. That was checked on pyspark 4.2.0 rather than argued:

| expression, `v` a STRING column holding `9223372036854775807` | `spark.sql.ansi.enabled=false` | `=true` |
|---|---|---|
| `v > 1000000` | **NULL** | `true` |
| `v > 1000000L` | `true` | `true` |
| `CAST(v AS INT)` | NULL | raises `CAST_INVALID_INPUT` |
| `CAST(v AS BIGINT)` | 9223372036854775807 | 9223372036854775807 |

The literal `1000000` is INT32. The STRING is coerced to **the literal's** type, the value
overflows INT32, and non-ANSI Spark yields NULL for that cast. The pipeline behaved as
non-ANSI, the warehouse as ANSI - which is exactly the pair of answers observed. The defect is
the WIDTH of the literal, and it is unreachable the moment the column is a BIGINT.

Then a NULL predicate does not match a `WHEN`, so the row fell through to `ELSE 'accepted'`.
The expectations got it right for free: `expect_all_or_drop` treats "not TRUE" as "did not
pass". The same NULL meant *drop* in one rendering and *accept* in the other.

### What was changed

- `bronze_autoloader.py` declares `cloudFiles.schemaHints` from the same schema the OSS lane
  uses, so `qty`, `new_qty` and `unit_price_cents` arrive as BIGINT. **This needs a full
  refresh** to take effect, because `cloudFiles.schemaLocation` caches the inferred schema:
  `scripts/databricks_run.sh run-full-refresh`.
- `silver_expectations.py` GENERATES the classification from `RULES` instead of restating it,
  and acceptance is positive: every branch is `COALESCE(predicate, false)`, so a row is
  `accepted` only when every rule said TRUE. A new `undecided_rules` column names any rule
  that returned NULL, so if this ever happens again the run says so.
- The three events now land in `amount_out_of_range`, which is where the other two lanes send
  them, and needs no new quarantine reason.

### What a review of that fix changed again, still without a re-run

The fix above was read back a round later and three things in it were not finished. All three
are in the repository and none of them has been near a workspace, which is the same sentence
as the one at the top of this section and is why it is repeated rather than assumed.

- **The literal still had no width.** Making a NULL predicate fail closed fixed the
  consequence; the predicate was still NULL. Every bound literal in the rules now carries `L`
  (`1000000L`, `10000L`, `0L`), which is the cheaper fix and the one that survives a pipeline
  running before its schema is re-inferred. Re-measured on the reproduction - STRING columns,
  ANSI off - no rule is undecidable and the Long.MaxValue record leaves through
  `amount_out_of_range` rather than through the first rule that could not answer.
- **`accepted` was still the `ELSE`.** Correct, because the branches are total, and correct by
  an argument rather than by construction. It is now `WHEN <every rule holds> THEN 'accepted'`
  over the same `RULES`, and the `ELSE` that remains is unreachable and raises: a record the
  classification cannot classify is a pipeline fault, and is deliberately NOT a new member of
  the closed enum.
- **The rules agreed and their ORDER did not.** The OSS lane tested the bounds before the
  currency; `RULES` declares the currency first. Measured on a record that breaks both: two
  different doors for one record, on rules that are identical. The OSS branches follow `RULES`
  now.

`silver_expectations.py` is therefore not byte-identical to the file that produced the numbers
below, and the next run has to be a `run-full-refresh` for the type hints to take effect at
all.

## The run that worked, and the checklist scored against it

The lane ran end to end on 3 September 2026. The first run of the day was correct on the close
and exposed two defects of its own; both were fixed and the lane was run again from commit
`8c9faa7`. That is the run this checklist was scored against, and its `update` was 58d3de6f,
COMPLETED, 0 ERROR-level events, `incomplete: []`.

**The record in this repository is no longer that one.** It is the SECOND close's - update
`289286cc`, 4 September 2026, over 1328 events - because the lane ran again and the record is
whatever the last fetch brought down. Every anchor in this document is filled from it by
`samegold readme`, mechanically, and
`tests/fast/test_databricks_bundle.py::test_the_run_document_agrees_with_the_record` fails if
the document and the record ever disagree. That test is what caught the two being out of step
in the first place.

### The first close's checklist, scored

Everything from here to the end of the checklist is about the FIRST close, over 755 events. It
is kept as it was scored rather than rewritten, because the point of it was that the expected
column had been written before the run - editing it now to describe a later population would
destroy exactly the property that made it worth anything. The second close is the section above,
and the anchored figures in "What the run returned" describe the CURRENT record, which is the
second close's.


Every expected value in the section below it was written **before** any of this ran, from the
generator and the OSS reference on the same seed. Here is each one against what the workspace
produced.

| # | check | expected | workspace | |
|---|---|---|---|---|
| 1 | money columns are `bigint` | `bigint` throughout | **not in the record** | see below |
| 2 | the four `bad-*` verdicts | `amount_out_of_range` x2, `missing_required_field` x2 | **not in the record** | see below |
| 3 | quarantine by reason | 727 / 6 / 5 / 3 / 6 / 2 / 3 / 3 = 755 | identical, all eight | ✅ |
| 3 | conservation | 755 / 727 / 28 / 755 | identical | ✅ |
| 3 | `undecided_rules` | 0 rows | `[]` | ✅ |
| 3 | rescued rows | at least 2 | **not in the record** | see below |
| 4 | expectations, per rule | 752/3, 749/6, 747/8, 743/12, 744/11, 747/8, 741/14 | **all fourteen identical** | ✅ |
| 5 | `revenue_by_month` 2026-01 | 14 198 046, 425 lines | 14 198 046, 425 | ✅ |
| 5 | `revenue_by_month` 2026-02 | 199 379, 3 lines | 199 379, 3 | ✅ |
| 5 | `above_contract_ceiling` | false everywhere | false, both months | ✅ |
| 5 | `revenue_closed` | 2 rows, version 0, "first close" | exactly that, and the returns figures too | ✅ |
| 6 | dimension | 75 / 60 / 60 / 15 | 75 / 60 / 60 / 15 | ✅ |

**Nothing disagreed.** Not one figure. The per-rule expectation counts are worth pausing on:
those seven pairs were predicted by evaluating the lane's own predicates on local Spark over the
generated population, and the Databricks event log's own accounting agrees on all fourteen
numbers - which retires the caveat that stood beside them ("the check that survives even if the
runtime's per-rule accounting turns out to differ").

**And three of the six items could not be checked against the record at all**, because nothing
in it spoke to them: the column types, the four named `bad-*` rows, and the rescued count. They
were read off a terminal by the person who ran it, which is the same standing as prose - and a
checklist and a record that do not cover the same ground is a gap somebody fills by remembering.
`publish_evidence.py` now captures all three (`column_types`, `money_types`, `bad_events`,
`rescued_rows`), so the next run closes it. Until then those three rows say "not in the record"
rather than a tick, which is what they are.

### The two findings from the earlier run, closed

**The outcome field.** `MAX(details:update_progress.state)` is the alphabetical maximum, so
`last_state` published `WAITING_FOR_RESOURCES` for update `44a237b3`, which completed. With
`max_by(state, timestamp)` the record now reads `COMPLETED` for `58d3de6f`, and the CTE picks
the most recent update to reach a TERMINAL state rather than the most recent to leave any event.
`tests/spark/test_databricks_event_log_query.py` runs the lane's own query over a synthetic
event log and requires COMPLETED for one that completes and FAILED for one that fails; with
`MAX` restored, both report `WAITING_FOR_RESOURCES`.

**The dimension.** AUTO CDC produced 78 versions and 18 closed rows against the hand-written
MERGE's 75 and 15, because its default is a new version whenever ANY column changes and the
source view carries `event_ts` and `event_id`. With
`track_history_column_list=["segment", "country"]` the workspace produced **75 / 60 / 60 / 15**
- the OSS lane's shape exactly. The workspace's own rows were then captured to
`evidence/databricks/dim_customer_scd2.json`, and
`tests/fast/test_databricks_dimension_parity.py` compares the two **row by row** against it:
they agree on all seventy-five, as multisets and as per-customer histories.

### What the record now shows that no record showed before

`update_history` carries the ten most recent terminal updates, and the first one it ever wrote
makes the retry loop visible:

    2026-09-03 18:01:18  COMPLETED  58d3de6f      <- this record
    2026-09-03 13:24:36  COMPLETED  44a237b3
    2026-09-03 13:15:22  COMPLETED  b0cf0443
    2026-09-03 12:54:57  FAILED     79bf353a
    2026-09-03 12:49:22  FAILED     865c9dcf
    2026-09-03 12:46:26  FAILED     c0322e9d
    2026-09-03 12:44:50  FAILED     f3e72640
    2026-09-03 12:43:53  FAILED     1adb8985
    2026-09-03 12:43:18  FAILED     d56d5da0
    2026-09-03 12:31:13  FAILED     78785ff2

Seven consecutive failed updates in twenty-four minutes, from launches nobody made seven times.
That is the shape a record describing one update cannot have.

**It does not verify the fix.** `pipelines.numUpdateRetryAttempts: "0"` landed in `e002f29`,
which was pushed after those failures; every update since has succeeded, and an update that
succeeds does not exercise a retry setting. The setting is deployed and untested, and the next
FAILED update is what tests it.

## What the run returned

### The pipeline update

| | |
|---|---|
| last state | <!--dbx:update.last_state-->COMPLETED<!--/dbx--> |
| ERROR-level events | <!--dbx:update.error_events-->0<!--/dbx--> |

### Rows per table

| table | rows |
|---|---|
| `bronze_events` | <!--dbx:rows.bronze_events-->1328<!--/dbx--> |
| `silver_classified` | <!--dbx:rows.silver_classified-->1328<!--/dbx--> |
| `silver_events` | <!--dbx:rows.silver_events-->1300<!--/dbx--> |
| `silver_quarantine` | <!--dbx:rows.silver_quarantine-->28<!--/dbx--> |
| `dim_customer_scd2` | <!--dbx:rows.dim_customer_scd2-->92<!--/dbx--> |
| `revenue_by_month` | <!--dbx:rows.revenue_by_month-->2<!--/dbx--> |
| `revenue_closed` | <!--dbx:rows.revenue_closed-->3<!--/dbx--> |

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
| amount_out_of_range | samegold.main.silver_events | 573 | 0 |
| missing_required_field | samegold.main.silver_events | 573 | 0 |
| negative_price | samegold.main.silver_events | 573 | 0 |
| non_positive_quantity | samegold.main.silver_events | 573 | 0 |
| unknown_currency | samegold.main.silver_events | 573 | 0 |
| unknown_event_type | samegold.main.silver_events | 573 | 0 |
| unparseable_json | samegold.main.silver_events | 573 | 0 |<!--/dbx-->

### Quarantine reasons, from the classified table

The same population counted the other way. Every reason here is a member of the contract's
closed enum; a reason that appears here and not in the table above is a rule the expectations
never reached.

<!--dbx:quarantine.table-->| quarantine reason | rows |
|---|---|
| accepted | 1300 |
| amount_out_of_range | 6 |
| missing_required_field | 5 |
| negative_price | 3 |
| non_positive_quantity | 6 |
| unknown_currency | 2 |
| unknown_event_type | 3 |
| unparseable_json | 3 |<!--/dbx-->

### AUTO CDC: the Type 2 dimension

`dp.create_auto_cdc_flow(..., stored_as_scd_type=2)` is one of the three Databricks-only calls
`PARITY.md` pins against the open-source signatures they fail on. The OSS lane maintains the
same dimension by hand with a two-pass `MERGE` in `src/samegold/pipelines/gold_scd2_merge.py`;
that the two agree is the point of having both.

| | |
|---|---|
| version rows | <!--dbx:dim.versions-->92<!--/dbx--> |
| distinct customers | <!--dbx:dim.customers-->60<!--/dbx--> |
| open rows (`__END_AT IS NULL`) | <!--dbx:dim.open_rows-->60<!--/dbx--> |
| closed rows | <!--dbx:dim.closed_rows-->32<!--/dbx--> |

Open rows must equal distinct customers: one current version per key is what Type 2 means, and
a dimension with two open rows for one customer is the defect the hand-written `MERGE` on the
OSS lane has a delete-by-absence branch for.

## The second close: a month that had already been signed off moved

This is what the project is for, and until 4 September 2026 it had only ever happened on a
laptop. January was closed at 14 198 046 cents. 573 events the first close had never seen were
then ingested, 553 of them for January, and a second close restated it - without touching the
version finance had signed off.

### The population, reproduced

The first time this was done, the 573 events were produced by a script in `/tmp` on one
machine. That made every figure the second close published rest on data no reader could
regenerate, which is this repository's premise inverted, and it is now a command:

```sh
samegold generate-late --out /tmp/late --seed 20260901 --late-seed 20260904
```

It generates the base population, generates a second one from the late seed, keeps the events
whose `event_id` the base did not have, and writes them under `batch=late-<stamp>` - the prefix
matters, because both generations bucket arrivals into the same instants and Auto Loader lists
one directory: uploaded under the base names, the second batch would replace the first.

What it must print, and what `tests/fast/test_late_arrivals.py` fails on if it does not:

```
573 late events in 269 batch directories (269 files)
  from 761 generated, of which 185 had already been delivered by the 755 events before them, and 3 carried no event_id
  by type : customer_upserted 21, order_line_amended 63, order_placed 420, return_registered 69
  by month: 2026-01 553, 2026-02 16, 2026-03 4
```

The three dropped lines are the ones with no `event_id`. That is a decision, not an accident:
"not already present" cannot be decided for a record with no id, and keeping them would
re-deliver a corrupt line the base population already carries - the quarantine counts would
charge one fault twice. So the late batch carries no corrupt records, and the run's arithmetic
shows it: quarantine stays at 28, all from the base population, and 727 + 573 = 1300 accepted.

### Uploading and running it

```sh
databricks fs cp -r /tmp/late/bronze dbfs:/Volumes/samegold/raw/landing
scripts/databricks_run.sh run          # NOT run-full-refresh: this is an incremental update
scripts/databricks_run.sh fetch
```

`run`, not `run-full-refresh`. A full refresh re-reads the whole landing volume and recomputes
the close from scratch, which would produce one correct close over 1328 events and no
restatement at all - the second version exists because the first one was already there.

**Deploy first if anything in `databricks/` has changed since the last deploy.**
`databricks bundle run` runs what was DEPLOYED, not what is in the tree, and on 4 September that
cost two things silently: `publish_evidence.py` did not write `dim_customer_scd2.json` and the
record carried no `deploy` key, both because the commits that added them had never been
deployed. The task ended SUCCESS. `FINDINGS.md` carries it; `scripts/databricks_run.sh all`
deploys before it runs.

### What the close has to say

Computed by the DuckDB reference over the reproduced population, so this table is derived and
not transcribed - `tests/fast/test_databricks_close_parity.py` recomputes it on every run of the
fast lane and compares five columns against the record:

| month | version | gross_cents | net_cents | line_count | return_count | rejected |
|---|---|---|---|---|---|---|
| 2026-01 | 0 | 14 198 046 | 12 911 212 | 425 | 71 | 22 |
| 2026-01 | 1 | 25 582 615 | 23 268 535 | 793 | 126 | 32 |
| 2026-02 | 0 | 199 379 | 199 379 | 3 | 0 | 0 |

Three properties, and each is a different thing that could have gone wrong:

- **version 0 is untouched**, figures and `restated_at` both. A restatement that rewrites the
  signed-off version has destroyed the evidence that it moved.
- **February gains no version.** Sixteen of the late returns fall in February and four in March,
  and they are returns against JANUARY sales: `gold_close.py` groups by the month of the sale,
  February's aggregate is unchanged, and the MERGE's `<>` guard is what stops a close from
  restating a month that did not move. March never appears at all, for the same reason.
- **conservation closes over the whole population**: `bronze_events` = `silver_classified` =
  1328 = 755 + 573, and `silver_events` 1300 + `silver_quarantine` 28 = 1328.

And `close_month` was run a second time over the same data: `revenue_closed` stayed at three
rows. The close's idempotence is executed rather than asserted.

### What the workspace produced, and how it got into this document

`evidence/databricks/SG-DBX-01.json` is the second close's record: update `289286cc`,
COMPLETED, 0 ERROR-level events, `incomplete: []`. The capture beside it holds the workspace's
own 92 dimension rows with `measured_in_the_workspace: true`, written by the notebook in the
same session rather than exported by hand.

**Every anchored figure in this document was re-rendered from that record by a command.**

```sh
scripts/databricks_run.sh fetch        # brings the record and the capture down
samegold readme                        # fills every sg: and dbx: anchor from both
```

That is new this round and it is the fix for how this document went wrong: the anchors were
filled by hand the first time and by a script in a scratch directory the second, so when the
lane ran again the document went on describing a run that no longer existed until a test caught
it. `src/samegold/evidence/databricks_doc.py` derives every anchor name from a field the record
carries, and an anchor the record cannot answer renders as `NOT RUN` rather than as a blank.

### What the comparison says now

The row-by-row dimension comparison ran against the workspace's 92 rows and **they agree**:
same sixty customers, same ninety-two intervals, same attributes, same instants, as multisets
and as per-customer histories. The parity did not merely survive the second close, it got
wider - it now covers late arrivals, which is the case a Type 2 dimension is hardest on.

The arithmetic is measured rather than inferred from the difference. The late population adds
eighteen distinct customer upserts; seventeen change a tracked attribute and one,
`cu-C000039-1`, repeats what the customer already had. So 75 + 17 = 92 versions and
15 + 17 = 32 closed rows, and the heartbeat that changed nothing produced no version, which is
what `track_history_column_list` is for.

## The checklist: what to run afterwards, and what each answer has to be

Six queries. Every expected value on the right was **measured** on the same generator the seed
step runs, at `--profile fast --seed 20260901`, on commit 0bfcff1 - not remembered, and not
inferred from the previous run, whose figures are wrong on purpose and kept above as a record
of being wrong.

They are reproducible without a workspace, which is the point of writing the number down rather
than the impression:

```sh
samegold generate --out /tmp/expect --profile fast --seed 20260901   # 755 events, 298 files
# then read /tmp/expect/truth/ledger.json, and for the close and the dimension run the DuckDB
# reference over /tmp/expect/bronze - the same functions tests/spark compares this lane against.
```

**These numbers are a property of that seed, that profile and that commit.** Change any of the
three and re-measure; a checklist compared against something somebody remembers is not a check,
and a checklist that has quietly stopped describing the generator is worse.

### 1. The money columns are integers

The defect that started all of this: Auto Loader inferred every column as STRING, `qty *
unit_price_cents` promoted to DOUBLE, and the close died writing a double into a BIGINT.

```sql
SELECT table_name, column_name, data_type
FROM samegold.information_schema.columns
WHERE table_schema = 'main'
  AND column_name IN ('qty', 'new_qty', 'unit_price_cents',
                      'gross_cents', 'returns_cents', 'net_cents')
ORDER BY table_name, column_name;
```

| expected | |
|---|---|
| every row's `data_type` | `bigint` |
| any `string` | the schema cache was not cleared - step 2 or step 4 did not take. **Stop here**: every number below is void |
| any `double` | the same, one stage further on |

### 2. The four deliberately-bad money events

The generator emits these to be REJECTED. Two carry a price of `Long.MaxValue`, which fits a
BIGINT and breaks the contract's bound; two carry 2^63, which does not fit the column at all and
is rescued, leaving the column NULL.

```sql
SELECT event_id, qty, unit_price_cents, quarantine_reason, undecided_rules
FROM samegold.main.silver_classified
WHERE event_id IN ('bad-0000007', 'bad-0000008', 'bad-0000016', 'bad-0000017')
ORDER BY event_id;
```

| event_id | qty | unit_price_cents | quarantine_reason | undecided_rules |
|---|---|---|---|---|
| `bad-0000007` | 1 | 9223372036854775807 | `amount_out_of_range` | *(empty)* |
| `bad-0000008` | 1 | `NULL` | `missing_required_field` | *(empty)* |
| `bad-0000016` | 1 | 9223372036854775807 | `amount_out_of_range` | *(empty)* |
| `bad-0000017` | 1 | `NULL` | `missing_required_field` | *(empty)* |

Four rows, exactly these verdicts. **`accepted` on any of them is the round-17 defect back**,
and it is worth checking on its own rather than trusting the totals: two of these four were
`accepted` on the run this document records, and the totals looked plausible right up until
`close_month` overflowed.

`undecided_rules` must be empty on all four. A rule name there means the classification decided
by a predicate that could not answer - fail-closed, so not revenue, but a reason nothing
established.

And they must not be in the validated table:

```sql
SELECT count(*) AS must_be_zero FROM samegold.main.silver_events
WHERE event_id IN ('bad-0000007', 'bad-0000008', 'bad-0000016', 'bad-0000017');
```

Expected `0`. This is the expectations and the classification agreeing about the same four rows,
which is the whole parity claim of this lane on four records a human can read.

### 3. Quarantine reasons, and the conservation identity

```sql
SELECT quarantine_reason, count(*) AS n
FROM samegold.main.silver_classified GROUP BY 1 ORDER BY 1;
```

| quarantine_reason | expected n |
|---|---|
| `accepted` | 727 |
| `amount_out_of_range` | 6 |
| `missing_required_field` | 5 |
| `negative_price` | 3 |
| `non_positive_quantity` | 6 |
| `unknown_currency` | 2 |
| `unknown_event_type` | 3 |
| `unparseable_json` | 3 |
| **total** | **755** |

No other reason may appear. The three return-stage reasons (`return_without_order`,
`return_outside_window`, `return_exceeds_sold_qty`) are decided in gold from questions about the
SALE, so those events are `accepted` here and are inside the 727 - that is the honest split, not
a gap.

```sql
SELECT (SELECT count(*) FROM samegold.main.silver_classified) AS classified,
       (SELECT count(*) FROM samegold.main.silver_events)     AS accepted,
       (SELECT count(*) FROM samegold.main.silver_quarantine) AS quarantined,
       (SELECT count(*) FROM samegold.main.bronze_events)     AS bronze;
```

Expected `755, 727, 28, 755` **for the first close**; the second close makes it
`1328, 1300, 28, 1328`, and `classified = accepted + quarantined` holds on both. If
`bronze` is 752 rather than 755, Auto Loader dropped the three unreadable lines instead of
rescuing them - which the OSS reader does not do, so it is a divergence to record here and not
a rounding difference to wave through.

```sql
SELECT count(*) AS must_be_zero FROM samegold.main.silver_classified
WHERE undecided_rules IS NOT NULL AND undecided_rules <> '';
```

Expected `0`, over the whole table. With bronze typed and every bound literal carrying its
width, no rule can be undecidable; a non-zero here is the type fix not having taken, reported by
the classification rather than by the close.

```sql
SELECT count(*) AS rescued FROM samegold.main.bronze_events WHERE _rescued_data IS NOT NULL;
```

Expected **at least 2** - the two 2^63 prices, whose column is NULL precisely because the value
was rescued. The OSS reader rescues five rows (those two plus the three unreadable lines); Auto
Loader's rescue semantics are its own, so the number to insist on is that
`bad-0000008` and `bad-0000017` are among them. A value that vanished without appearing here is
the one failure shape this lane has no counter for.

### 4. Expectations, per rule

An expectation is evaluated on every row independently, so these do NOT sum to the table above:
a row that breaks two rules is failed by both expectations and quarantined under the first.

| rule | expected passed | expected failed |
|---|---|---|
| `unparseable_json` | 752 | 3 |
| `unknown_event_type` | 749 | 6 |
| `missing_required_field` | 747 | 8 |
| `non_positive_quantity` | 743 | 12 |
| `negative_price` | 744 | 11 |
| `unknown_currency` | 747 | 8 |
| `amount_out_of_range` | 741 | 14 |

`passed + failed = 755` on every row of that table, which is the check that survives even if the
runtime's per-rule accounting turns out to differ from evaluating the same predicates in Spark
(these were produced the second way, over the same bytes). A rule reporting 0 failed is the one
to distrust: every one of these seven has records planted against it on purpose.

**The total is the update's, not the table's**, and reading it the other way is the easiest
mistake this section invites. The silver tables are incremental, so these counts cover the rows
THAT UPDATE ingested: 755 on the first close, 573 on the second - the late arrivals, not the
1328 rows the table then held. An update that ingests nothing therefore reports **no
expectations at all**, which is what run 592180158314216 published on 5 September 2026: an empty
`expectations` list with an empty `incomplete` beside it. That is correct and, on its own, it is
ambiguous - "no rows were processed" and "no rules are declared" render as the same empty list.
So the record now carries `update_output_rows` next to it, and the pair is readable as
arithmetic: no rows written, no rule reported.

### 5. The close, and its size

```sql
SELECT accounting_month, gross_cents, returns_cents, net_cents,
       line_count, return_count, returns_rejected_count
FROM samegold.main.revenue_by_month ORDER BY accounting_month;
```

| accounting_month | gross_cents | returns_cents | net_cents | line_count | return_count | returns_rejected_count |
|---|---|---|---|---|---|---|
| `2026-01` | 14 198 046 | 1 286 834 | 12 911 212 | 425 | 71 | 22 |
| `2026-02` | 199 379 | 0 | 199 379 | 3 | 0 | 0 |

Two rows and no others. **January's gross is 14 198 046 cents - €141 980.46 - from 425 lines.**
The run this document records published 2.767e19 from 428 lines for the same month, which is
six and a half million times the contract's ceiling for a single line, so the order of magnitude
is the check even before the digits are: a January that is not in the tens of millions of cents
is not this population.

```sql
SELECT accounting_month, gross_cents, line_count,
       gross_cents > line_count * 10000L * 1000000L AS above_contract_ceiling
FROM samegold.main.revenue_by_month ORDER BY accounting_month;
```

`above_contract_ceiling` must be `false` on every row. It is the same query
`publish_evidence.py` puts in the record, run by hand because a record that reports a `true`
here is a record nobody should paste into this document.

```sql
SELECT accounting_month, close_version, gross_cents, net_cents, restatement_reason
FROM samegold.main.revenue_closed ORDER BY accounting_month, close_version;
```

Two rows, both `close_version = 0` and `restatement_reason = 'first close'`, with the same
figures as the table above. `close_month` closes every month STRICTLY EARLIER than the month of
`as_of`, and `as_of` is the job's start time - so this holds for any run after February 2026 and
would produce fewer rows for a run inside the data's own months.

### 6. The Type 2 dimension

```sql
SELECT count(*)                                          AS versions,
       count(DISTINCT customer_id)                       AS customers,
       sum(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) AS open_rows,
       sum(CASE WHEN __END_AT IS NOT NULL THEN 1 ELSE 0 END) AS closed_rows
FROM samegold.main.dim_customer_scd2;
```

| expected | |
|---|---|
| versions | 75 |
| customers | 60 |
| open_rows | 60 |
| closed_rows | 15 |

Those four numbers are the FIRST close's; the second close makes them 92 / 60 / 60 / 32, and
`open_rows = customers` holds on both - which is the point. That identity is what Type 2 means,
and it is the property rather than the count: a dimension with two open rows for one customer is
broken whatever the totals say. Both sets are AUTO CDC agreeing with the hand-written `MERGE`
the OSS lane uses, which is the only reason this lane keeps both.

The rows themselves are captured by the run, not by you. `publish_evidence.py` reads the same
table a second time -

```sql
SELECT customer_id, segment, country, __START_AT, __END_AT
FROM samegold.main.dim_customer_scd2 ORDER BY customer_id, __START_AT;
```

- and writes them to the evidence volume with a header naming the update that produced them;
`scripts/databricks_run.sh fetch` brings the file down beside the record. That is a change from
the first time round, when this document asked you to paste the query in and save the result:
a capture exported by hand cannot say which run it came from, so a later run replacing the
record left it comparing green against rows the workspace no longer held.

`tests/fast/test_databricks_dimension_parity.py` compares it against the OSS lane's dimension on
the same seed - row by row, as instants and as multisets - on every run of the fast lane, FAILS
rather than skipping if the file is not there, and fails NAMING THE QUERY if its update id and
the record's disagree. The capture from 3 September 2026 agrees on all seventy-five rows.

### If every one of those holds

Then paste the record's figures into the anchors above, run `make check`, and the sentence at
the top of this document changes from "it failed" to what it did. Until then it stands.

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
| Default Storage, and therefore no metastore storage root | `databricks catalogs create` fails with `Metastore storage root URL does not exist` ([databricks/cli#4513](https://github.com/databricks/cli/issues/4513)) | the catalog is created with `CREATE CATALOG IF NOT EXISTS` through `POST /api/2.0/sql/statements` on the one 2X-Small warehouse, which resolves its location through Default Storage. The script waits 30s, cancels on timeout, and refuses to continue unless the statement reports `SUCCEEDED` |

Three consequences worth stating plainly, because they are the ones that would otherwise be read
as achievements:

- **The row filter and the column mask are not enforced here.** `databricks/sql/policies.sql`
  declares both, and `is_account_group_member('finance_all')` resolves to false for everyone on
  an account with no groups. Applying them would need a SQL warehouse id that the bundle does
  not have and cannot create, so nothing in `make databricks` runs that file. It is checked by
  a parser (`tests/spark/test_databricks_lane_parses.py`) and by nothing else.
- **Cost is not measured on this lane at all.** The numbers above are rows and events. The
  cost work lives in the OSS lane, where files and bytes come out of the Delta log and are
  labelled as a proxy for DBUs rather than converted into one.
- **No job health rule is declared, and that is a refusal rather than an omission.** A
  `health:` block on a job or a task pairs a metric threshold - `RUN_DURATION_SECONDS` is the
  one this lane would have used - with a NOTIFICATION, and notifying is the only thing it does.
  This account has no notification destination: no Slack app, no webhook, no on-call rota. The
  rule would therefore fire into nothing, or into a personal email address committed to a
  public repository, and the second is worse than the first. It would also have been
  indistinguishable, in a bundle, from a rule that works.

  A construct whose only effect is an announcement nobody receives is the decorative case this
  round set out to remove, and it is the same argument that deleted `taskValues.set("evidence",
  ...)` from `publish_evidence.py`: a value written for a reader that cannot exist. What the
  rule would have watched is watched by something that ACTS instead - `timeout_seconds: 1800`
  on the job and `600`/`900` on every task, which kills a stuck run rather than describing one,
  and does so on an account where a stuck run spends the day's compute. When a destination
  exists, the rule becomes worth declaring; until then this paragraph is the honest version of
  it. `docs/milestones.md` M16 is where the alerting work lives.

## The prediction, scored

The section below was written before the lane had ever been deployed, as a list of the fields
most likely to break first, "so that the list is a prediction and can be scored". It has now
been scored, and it lost.

The three runs that follow this round are predicted the same way and field by field, before
they are launched, in `docs/predictions-2026-09-05.md` - including the figures the OSS lane can
compute in advance, which is most of them.

**What actually broke was not on the list.** `resources.pipelines.samegold_pipeline` carried no
`name`. The key a resource is declared under is the bundle's id for it, not the pipeline's
name, and `POST /api/2.0/pipelines` requires `name`. Every prediction below is about a field
whose VALUE might be refused; the defect was a field that was not there at all, and the list
did not contain the idea that a required field could be missing.

**And the thing that should have caught it did not exist.** `databricks bundle validate -t
free` answered `Validation OK!` on this bundle, and `.github/workflows/databricks.yml` runs
validate as its default action, so that job had been green on a bundle the API rejects at the
first request. Validate checks syntax, `include:` resolution and variable substitution, and
warns about properties it does not recognise. It does not check that the request body it is
about to send is one the API will accept - the bundle reference says a resource declaration
"uses the corresponding object's create operation's request payload", and what that payload
requires lives in the REST API reference. `tests/fast/test_databricks_bundle.py` now asserts
the required fields for every resource type in the bundle, from that reference: `name` for all
four, `catalog_name` for schemas and volumes, `schema_name` for volumes, `tasks` for jobs, and
exactly one of `schema`/`target` for the pipeline.

## If the next command fails

These were the fields predicted to break, kept as written. One of them has since been made
explicit rather than left to be inferred: `volume_type: MANAGED` is now spelled out, because
the reference does not mark it required or optional and "the documentation is ambiguous" is
not a reason to find out at POST time on someone else's workspace.

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

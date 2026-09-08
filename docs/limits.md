# What this repository has not verified

The residual risk, listed before the results rather than after them.

## Verified by running it, in this repository, on this commit

- The fast lane: the generator, the reference, the digest's refusals, the invariants, the
  statistics, the mutation engine and its assumption probes, the SCD2 logic (including a
  property test over out-of-order arrivals), the governance controls, and the evidence gate
  with the eleven forgery attacks that used to work.
- The Spark lane **without Delta**: the Spark implementation and the DuckDB reference agree on
  the whole versioned close and on the customer dimension; the digest is unchanged at 2 and at
  16 shuffle partitions, and unchanged when the input is repartitioned so the rows arrive in a
  different physical order.
- The Spark lane **with Delta**, since 2 September 2026: time travel, the change data feed read
  as a feed, a `MERGE` whose two branches are counted separately in the transaction log, an
  `OPTIMIZE ... ZORDER BY` whose effect is read from that log, and the hand-written Type 2
  `MERGE` including its delete-by-absence branch. Run on WSL2 Ubuntu 24.04 under Windows 11,
  Temurin 21, pyspark 4.2.0 with delta-spark 4.4.0, jars resolved from Maven Central at the
  coordinate ADR 0002 pins. And on `ubuntu-latest` in CI: the `delta` job of
  `.github/workflows/spark.yml` runs the same two commands and went green for the first time
  on run 33628100076, `51 passed, 1 skipped` then `6 passed`, having been red on the defect
  below since its first run.
- The crash campaign on the silver stage: injection confirmed by exit code, convergence after
  restart, and a negative control that a non-idempotent writer fails.
- The cost lab on real Delta tables through delta-rs: compaction, clustering, partitioning and
  the copy cost of a delete.
- The privacy controls: masking, the exposure check, and a purge that deletes and vacuums.

## What this section used to say, and what it cost

For eleven rounds this heading read "Written, tested, and not yet executed here", and under it:
everything needing the Delta jars from Maven Central, "because the machine this was built on has
no route to it". Every word of that was true about the machine. It was false about the
repository, and the gap between those two is the finding of round twelve.

The `delta` job in `.github/workflows/spark.yml` had been resolving those jars and running that
lane on every push that touched it. It had never passed. `gh run list` showed two runs and two
failures, both on `test_the_scd2_merge_produces_a_well_formed_dimension`, both
`CANNOT_DETERMINE_TYPE` - the same defect the first local run found. So the lane was not
"written and not executed"; it was executed, red, and described in these documents as unknown.
"Not executed **here**" was doing the work, and a reader has no reason to read "here" as
"anywhere the author can see".

Two defects came out of running it, and neither could have been found by reading:

- `upsert_scd2` could not complete its first call on any input. It built the `MERGE` source
  with an inferred schema, and the open row of a Type 2 dimension has a NULL `valid_to`, so on
  the first batch every value in the column was None and Spark refused to type it. The only
  caller of that function is `tests/delta`.
- the lane passed once and failed on the second run, because it wrote a metastore-managed table
  into the repository's own `spark-warehouse/` and dropped one of the two tables it created.

A third came from installing the Spark extras at all: with `pyspark` present, mypy type-checks
the Spark-facing code for the first time, and the fast lane that runs mypy does not install it.
That found the three Databricks-only primitives now recorded in `PARITY.md`, and ten places
where a `SparkSession | None` was used as a `SparkSession`.

## Executed here now, and this section said otherwise for four days

The Databricks lane needs a workspace: the bundle, Unity Catalog, expectations, AUTO CDC, the
event log and the dashboard. **It has one, and it has run four times between 3 and 6 September
2026**; the records are under `evidence/databricks/`, every figure in `docs/databricks-run.md`
is rendered from the canonical one, and the dashboard and its alert were deployed on the 6th.

**This heading used to read "Still not executed here"**, and the paragraph under it said
"Nothing has yet run the pipeline, so every figure in `docs/databricks-run.md` still reads
`NOT RUN`" - while that document held twenty rendered figures. It was wrong from 3 September to
7 September, which is the finding this round is named for: the drift gate checked the anchored
numbers on every push and no gate had ever read a sentence. `samegold.evidence.prose` reads
them now, and `FINDINGS.md` carries the class.

The first deploy did fail, on 2 September 2026. `databricks bundle validate -t free` passed;
`databricks bundle deploy -t free` died on the first POST because the pipeline resource carried
no `name`, which that API requires and validate does not check. The catalog step failed before
that, for a different reason: `databricks catalogs create` cannot work on a Default Storage
metastore. Both are fixed and both now have tests.

What changed in round 13 is that the lane is now deployable in one command and produces a
record when it is, so the sentence above has a date on it rather than being open-ended:

- `make databricks` reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` and runs the whole lane -
  catalog, validate, deploy, seed, run, fetch. `scripts/databricks_run.sh` is the script.
- `docs/databricks-run.md` holds the results, with every run-produced figure inside an anchor
  rendered from `evidence/databricks/SG-DBX-01.json`. `tests/fast/test_databricks_bundle.py`
  fails if any of them holds a number while that record does not exist, and fails if any of them
  disagrees with it once it does. A document cannot get ahead of its run by hand any more; that
  is the round-12 finding turned into a test.
- `tests/fast/test_databricks_bundle.py` also checks the bundle against the Free Edition limits
  it has to live inside, which is how four defects that would have failed the first deploy or
  the first run were found: a landing volume nothing created, two notebook tasks reading their
  catalog from a `spark.conf` key only a pipeline populates, an `event_log('')` built from a
  pipeline id that does not exist outside a pipeline, and a nightly schedule deployed
  UNPAUSED on an account where the daily quota is a hard stop.
- Whatever that run produces is **not** appended to `evidence/history.jsonl`. It cannot be
  recomputed by a reader with a clone, so it goes to `evidence/databricks/` and says why in
  its own `chain` field. `evidence/databricks/README.md` is the comparison in full.

## One workspace, two ANSI modes, and why a bound literal is spelled `1000000L`

This is not a limit of the free tier. It is a property of the platform that cost this project a
published month, and it is written here because the alternative is that it lives in a commit
message and the next reader finds it the same way this one did.

**The pipeline engine and the SQL warehouse in the same workspace do not evaluate the same
expression the same way.** The declarative pipeline that runs `silver_expectations.py` behaved
as `spark.sql.ansi.enabled=false`; the SQL warehouse that the same rules were checked from
answered as if it were true. Both answers were observed in the workspace on 2 September 2026,
on the same table, for the same predicate, which is how the defect was found at all - the rule
said one thing when a human asked it and another when the pipeline did.

The mechanism, measured locally on pyspark 4.2.0 under WSL2 (Temurin 21) rather than deduced,
with `v` a STRING column holding `9223372036854775807`:

| expression | `ansi=false` | `ansi=true` |
|---|---|---|
| `v > 1000000` | **NULL** | `true` |
| `v > 1000000L` | `true` | `true` |
| `v >= 0` | **NULL** | `true` |
| `v >= 0L` | `true` | `true` |
| `v <= 10000` | **NULL** | `false` |
| `CAST(v AS INT)` | **NULL** | raises `CAST_INVALID_INPUT` |
| `CAST(v AS BIGINT)` | `9223372036854775807` | `9223372036854775807` |

Read the second row against the first. `1000000` is an **INT32** literal, and Spark coerces the
*other operand* to the literal's type rather than widening the literal: the string is cast to
INT32, the value overflows it, and with ANSI off that cast is NULL. It is the WIDTH of the
literal, not string-versus-numeric, and it disappears the moment either the column is a BIGINT
or the literal says it is one.

What that NULL did: `NOT(NULL)` is NULL, a `WHEN` does not match on NULL, and the classification
of the day ended in `ELSE 'accepted'`. Three events the generator emits **in order to be
rejected** were booked as 2.767e19 cents of January revenue - six and a half million times the
contract's ceiling for a single line - out of 428 lines.

Three consequences are carried in the code, and each has a test:

- bronze is typed at ingest (`cloudFiles.schemaHints`), so the columns are BIGINT and the
  coercion has nothing to do;
- every bound literal in Spark-dialect SQL carries `L`, and every bound in the PySpark lane goes
  through `_bound()`, which is `lit(value).cast("bigint")`. Belt and braces on purpose: the
  hints only take effect after a full refresh re-infers the cached schema, and the same rules
  are read by the warehouse, whose mode is not the pipeline's;
- acceptance is a positive conjunction over the rules rather than the `ELSE` branch, so a rule
  that cannot answer quarantines rather than paying out. `tests/fast/test_contract_documents.py`
  enforces the spelling, `tests/spark/test_adversarial_records.py` evaluates the rules on STRING
  columns with ANSI pinned off - which is the reproduction, since Spark 4 defaults ANSI on and
  the test passes vacuously otherwise.

**The reference does not have this hazard, and that is measured too**, on duckdb 1.5.5: comparing
a VARCHAR column against an INTEGER literal is a *binder error* - `Cannot compare values of type
VARCHAR and type INTEGER_LITERAL` - so the reference refuses to run rather than quietly answering
"unknown", and its numeric columns are JSON converted through an explicit `json_type` guard and
`TRY_CAST(... AS BIGINT)` before any comparison. So `gold_revenue.sql` and `duckdb_gold.py` are
exempt from the `L` policy by measurement rather than by omission.

A related asymmetry, same root, recorded here because it is what the failed run actually died
of: a value that does not fit its column is **rescued**, not rejected. Spark reading a declared
schema in PERMISSIVE mode, and Auto Loader with the hints, both null that one column and copy the
raw line into the rescue column; DuckDB's `TRY_CAST(... AS BIGINT)` gives NULL for the same value.
The record survives and leaves through `missing_required_field`, because after the rescue the
field is missing - fail-closed and correct, and completely silent about the fact that a value was
LOST rather than never sent. The generator emits one of these deliberately (corrupt kind
`beyond_bigint`, a price of 2^63) and `values_beyond_bigint` is counted in its ledger and
recounted independently by the reference, so the loss is a number somebody can read.

## Not verifiable for free, at all

| exam area | why the free lanes cannot show it | what is done instead |
|---|---|---|
| cost in DBUs | `system.billing` needs an account console and a metastore-admin role; Free Edition has neither | files and bytes that a predicate cannot skip, read from the Delta log: deterministic, and labelled as a proxy |
| query profiles | a Databricks UI | Spark plans and the same file-level measurements |
| account-level security | no SSO, no SCIM, no account groups, no OAuth machine-to-machine | the controls are implemented in code and tested; the platform equivalents are declared in SQL and marked as declared |
| row filters and column masks, enforced | `databricks/sql/policies.sql` needs a SQL warehouse id to be applied, and a bundle on Free Edition can neither create a warehouse nor learn its id; `is_account_group_member` is false for everyone anyway | the file is deployed with the bundle, parsed by `tests/spark/test_databricks_lane_parses.py`, and **not applied by `make databricks`** - stated here rather than left to be assumed from its presence |
| a grant that keeps anyone out | `account users` is the only principal that exists, and it has one member: whoever deployed | the grants in `databricks/resources/grants.yml` show that the privilege was applied and can be read back, which is a different and much smaller claim |
| Delta Sharing as a provider | provider registration is not available on Free Edition | out of scope, stated rather than faked |
| a failed pipeline update retries itself | **measured**: on 2 September 2026 one `databricks bundle run samegold_close` produced **six failed updates between 12:17 and 12:31 UTC** - one launch, five automatic retries, fourteen minutes of quota - with `databricks pipelines get` confirming `development: true`, `serverless: true`, `continuous: false` on the deployed spec. The reference ties retry behaviour to how the update was TRIGGERED: the UI's Run now "disables pipeline retries", while updates through Jobs or the API get "automatic retry and restart behavior". This lane is started by a job | the claim beside `development:` in `databricks/databricks.yml` is corrected to say what that field actually does. `max_retries: 0` is declared on every job task, which stops the JOB retrying and does not stop the pipeline system. The setting that DOES is `pipelines.numUpdateRetryAttempts`, a pipeline configuration property whose documented default is "Five for triggered pipelines" - five, which is exactly the number of retries measured - and it is now set to `0` in the pipeline's `configuration:` block, alongside `pipelines.maxFlowRetryAttempts`. Neither override has been exercised against a workspace; the default they override is what was measured. The record now makes the loop VISIBLE, which it never was: `update_history` in `evidence/databricks/SG-DBX-01.json` carries the ten most recent terminal updates, and its first ever run shows seven consecutive `FAILED` between 12:31 and 12:54 on 3 September 2026 from launches nobody made seven times. **The override is still unverified**: it landed in `e002f29`, which was pushed after those failures, and every update since has succeeded - an update that succeeds does not exercise a retry setting. The next FAILED update is what tests it. Until then the practical mitigation is to watch the run: a failing update on this lane costs roughly 2.5 minutes per retry, so the first sign of trouble is worth acting on rather than waiting out. A job `timeout_seconds` bounds it, and is now SET: successful runs exist to size it from. The job ceiling is 1800s and every task that starts compute carries its own - 600s for the pipeline task, against a **measured** update of 115 seconds and against the fourteen minutes that retry loop actually took, so it would have ended it at ten. The notebook ceilings are 600s and 900s and they are CEILINGS rather than estimates: no per-task duration has been measured for a notebook in this lane, a serverless cold start alone is one to two minutes, and a bound tight enough to be an estimate would kill a healthy run. `tests/fast/test_databricks_bundle.py` fails if any task that runs compute has none |
| a task that failed retries itself on serverless | **measured**, run 44869473800771 on 5 September 2026: `verify_no_restatement` was failed on purpose with `--params fail_task=...` and ran **twice inside the original run** - attempt 0 failed after 6s, attempt 1 started 57 seconds later and failed after 12s. Every task in this job declares `max_retries: 0`. Two separate things were wrong. FIRST, the declaration did not arrive: `databricks jobs get` on the deployed job reports max_retries **not declared** on any of the six tasks, while `timeout_seconds` survives as 600 and 900. SECOND, and this is the part that decides the behaviour, it would not have mattered - the Jobs API documents `max_retries` **default 0**, "the value 0 means to never retry", so declaring 0 asks for what already applies. What retried the task is **serverless auto-optimization**, which is ON by default, which the UI calls "Enable serverless auto-optimization (may include additional retries)", and whose API field is `disable_auto_optimization` (boolean, default false) | `disable_auto_optimization: true` is now declared on every task that runs compute, beside the `max_retries: 0` that only ever restated a default. `tests/fast/test_databricks_bundle.py::test_no_task_relies_on_max_retries_to_stop_a_serverless_retry` refuses a task that declares one without the other. Neither has been exercised against a workspace: **the next deploy is what tests them**, and `scripts/databricks_run.sh deploy` now reads the deployed job back and prints the retry and timeout settings per task, so a declaration that does not arrive is visible at deploy time instead of on the day something fails. **THE SERIALIZER QUESTION IS CLOSED, and it was falsified**: the deploy read-back of 6 September 2026 reports `max_retries=0` arriving on all four notebook tasks, so a bundle does NOT drop a field whose value equals its default. Zeros survive. Why the 5 September read showed it absent everywhere is a separate open question; the plausible answer is that the field now travels with a sibling retry field on the same task, and that is a hypothesis rather than a finding. The falsification that was written here as costing no compute was never needed: set `max_retries: 2` on one task, `scripts/databricks_run.sh deploy`, and read the line this step now prints. If 2 appears, a bundle drops fields whose value equals their default and every zero in this bundle is decoration; if it does not, something else is eating the field |
| the one task whose retries have ever cost anything, protected | **measured**, 6 September 2026, by the deploy read-back on its first run against a workspace: `disable_auto_optimization` arrives on the four NOTEBOOK tasks and **not** on `ingest_and_transform`. It is a serverless-workflows task setting and a pipeline task runs on the compute of the pipeline, so the one task in this job whose retry loop has actually cost quota - six failed updates from one launch, fourteen minutes, 2 September 2026 - was the one the new protection did not reach | the field is removed where it cannot arrive rather than left looking applied, and the lever that does govern it is `pipelines.numUpdateRetryAttempts` in the `configuration:` block of the pipeline, declared `"0"` since 3 September and **first read back off the deployed pipeline on 6 September**. `scripts/databricks_run.sh deploy` now prints the deployed pipeline spec beside the job tasks and warns by name if either retry property is missing; `tests/fast/test_databricks_bundle.py` refuses the field on a pipeline task and requires the pipeline configuration. `FINDINGS.md` carries it as a class: a correct diagnosis written down is not a correction applied |
| a failed close that reports itself as a failed job | **measured**, same run: it finished **SUCCESS_WITH_FAILURES**, not FAILED. `publish_evidence` runs under `run_if: ALL_DONE` so that a failure still leaves a record, it is the last task in the graph, and it succeeded - so the job as a whole did not fail. This is the price of the record, not a bug in it: **alerting on this job's terminal state is unsafe**, because "not FAILED" includes a run whose close is broken | the failure signal lives in the record instead: `incomplete` names the task that owed rows and `missing_checks` names the checks it never wrote, both non-empty exactly when a verification did not report. `scripts/databricks_run.sh fetch` prints them as `SECTIONS THAT COULD NOT BE READ`, which is what it printed for this run. A monitor for this lane reads the record, or it reads nothing useful |
| a repair that leaves the evidence of the failure alone | **measured**, same run: `databricks jobs repair-run --rerun-tasks verify_no_restatement` re-ran that task and only that task. `publish_evidence` stayed at `attempt_number: 0` and did not run again, so the record in the volume was **not** overwritten by the repair - the opposite of what `docs/predictions-2026-09-05.md` predicted, and it is scored there as a falsified reason | the advice it produced still holds on other grounds: fetch a failed record before repairing, because a repair that re-runs `publish_evidence` (`--rerun-all-failed-tasks` once the evidence task has itself failed, or simply running the job again) does overwrite it - the notebook writes to one path. `scripts/databricks_run.sh fetch <label>` is how a kept run stops being at the mercy of the next one |
| a SQL warehouse that is ready when you are | on Free Edition the warehouse **stops itself after a few minutes idle**, so a cold start is the NORMAL case for anything that runs SQL here, not the exception. A serverless 2X-Small takes 40s to 2 minutes to come up, and `wait_timeout` on the Statement Execution API accepts at most `50s` - so no value of that parameter covers it | every wait in this lane is designed against the cold start rather than against the query: the statement is submitted with `on_wait_timeout: CONTINUE` and then POLLED to a terminal state, with a ceiling on the whole wait (5 minutes by default) and the warehouse started explicitly when it is not already running. The first version asked for 30s with CANCEL and reported a failure that had in fact created the catalog |
| creating a catalog through the Unity Catalog API | Free Edition uses **Default Storage**, so the metastore has no storage root and `databricks catalogs create` fails with `Metastore storage root URL does not exist` ([databricks/cli#4513](https://github.com/databricks/cli/issues/4513)) | `scripts/databricks_run.sh` issues `CREATE CATALOG IF NOT EXISTS` through the SQL Statement Execution API instead, which resolves the location through Default Storage; it checks the statement reached `SUCCEEDED` rather than assuming |
| `databricks bundle validate` as a deploy gate | it checks syntax, includes and variable resolution, and warns about unknown properties - it does **not** check that the request body it will send is one the API accepts | the required fields for every resource type are asserted from the REST API reference in `tests/fast/test_databricks_bundle.py`. This was found by a deploy dying on `name must be set` after validate said `Validation OK!` |
| Lakehouse Federation | needs an external database and a connector | out of scope |

## Written and not executed here

**The open-source Spark Declarative Pipelines lane, as of 8 September 2026.**
`pipelines/spark-pipeline.yml` and `pipelines/transformations/bronze_silver.py` are linted by
`ruff`, type-checked by `mypy` and parsed by `tests/fast/test_contract_documents.py`, and are
executed by no Makefile target, no preflight step and no test. The only `spark-pipelines run`
in this repository is inside a comment in the YAML's own header.

**It was tried, on 8 September 2026, and it does not run.** WSL2 Ubuntu, Temurin 21,
pyspark 4.2.0, Delta jars resolved from Maven Central. Three failures, in order:

| attempt | result |
|---|---|
| `spark-pipelines run` | `Could not find valid SPARK_HOME` - a pip install sets neither `SPARK_HOME` nor a `python` on PATH that has pyspark |
| with both set | `[SCHEMA_NOT_FOUND] The schema spark_catalog.samegold cannot be found` - the spec declares `database: samegold` and nothing creates it |
| with the schema created first | the same error: the Connect server the CLI starts does not see the schema created beside it |

And the spec's `configuration:` block is refused at runtime as a **warning**:
`spark.sql.extensions` fails `[CANNOT_MODIFY_STATIC_CONFIG]`, `spark.jars.packages` fails
`[CANNOT_MODIFY_CONFIG]`. The comment above that block says it is what stops the lane writing
Parquet instead of Delta. `FINDINGS.md` carries the whole of it with its class.

This section was **wrong about itself** until today. It read "Nothing, as of 4 September 2026"
while that lane sat in the repository, unexecuted, and closed with the sentence below about
what belongs in the list. The list had one member and did not name it - which is the failure
this section exists to prevent, committed by the section itself.

`.devcontainer/Dockerfile` was the previous entry and has been built and run: 160.4 s cold,
2.74 GB, and 0.4 s for `docker run --rm samegold make demo`, on Windows 11 with Docker
Desktop's WSL2 backend and Ubuntu 24.04. The Dockerfile's header carries the breakdown.

The section stays, dated, because it is the one this repository has been wrong in most often:
the Delta lane sat here while it was running red in CI, the Databricks lane sat here for six
rounds, and the entry above sat OUTSIDE it while the heading claimed the list was empty. A list
is a claim like any other, and the next thing written and not run belongs in it.

What the build corrected is worth keeping: the header used to say ~7 min and ~2 GB. The time
was 2.6x too pessimistic, the size 37% too optimistic, and the line labelled
"~4 min building the pyspark 4.2.0 wheel. MEASURED" was measured on a GitHub runner in
`.github/workflows/spark.yml` - the whole venv step took 82.9 s here. **A number measured
somewhere else is an estimate here.**

## The prose gate is escaped by rewording, measured

`samegold.evidence.prose` fails the build when a document asserts an absence of runs while
`evidence/databricks/` holds run records. It matches the phrasings this repository actually
uses, and the module comment says that narrowness is deliberate: a pattern that matched every
negation would flag the sentences that *correct* old claims, and FINDINGS.md is a history by
construction.

That trade has a price, and it is written here rather than left for a reviewer to find, because
this is the page a reviewer reads. **The gate is a spell-checker for one family of sentences,
not a proof that no false absence claim is in these documents.** Measured against the three run
records committed under `evidence/databricks/` on 7 September 2026, with the gate green:

| sentence | gate |
| --- | --- |
| "The Databricks job has never been executed." | fails, correctly |
| "The Databricks job has not been executed even once." | **passes**, and is false |
| "The Databricks job has not yet run." | **passes**, and is false |
| "No Databricks job run has taken place." | **passes**, and is false |

Three of those four contradict `evidence/databricks/` and the gate is silent on all three. The
first is caught because `never ... executed` is one of the phrasings in `NEVER_RUN`; the others
are the same claim written by somebody who was not thinking about the pattern - which is the
ordinary case, not an adversarial one.

So the gate raises the cost of one specific way of being wrong. It does not lower the cost of
reading the documents, and a green fast lane is not evidence that these pages are true.

## The credential in CI is a personal token, because the free tier has no other kind

Stated here rather than in a commit message, because it is a standing property of how this
repository is deployed and a reviewer is entitled to see it without reading git history.

**What is in CI.** `.github/workflows/databricks.yml` authenticates with a Databricks
**personal access token**, held in a GitHub *environment* named `databricks` together with the
workspace host. It is dispatch-only, it never starts compute, and the token is issued with an
expiry.

**Why a personal one.** Free Edition has no account console, and therefore:

| what a workspace would normally use | why it is unavailable here |
|---|---|
| a service principal with a scoped role | service principals are an account-level object; Free Edition exposes no account API |
| OAuth machine-to-machine credentials | same reason - they are issued against an account |
| a token scoped to one catalog or one job | Free Edition's PATs carry the user's own authority, whole |

So the credential in CI has **the same authority over the workspace as the person who issued
it**. That is not a design decision and it is not defended as one: it is the only kind of
credential this tier can produce, and the alternative is a lane that cannot be deployed from CI
at all.

**What is done about it, given that.** Four narrowings, none of which changes the paragraph
above:

- it lives in a GitHub **environment**, not in repository secrets. A repository secret is
  readable by every workflow in the repository, including one added later by a change nobody
  read closely; an environment secret is readable only by a job that names that environment;
- the workflow is `workflow_dispatch` only - no `push`, no `schedule` - and there is no
  `pull_request_target` anywhere in this repository, which is the trigger that would run a
  fork's code against these secrets;
- its actions are pinned to **commit SHAs**. A tag is a mutable pointer, and moving one is the
  realistic way a token leaves a repository;
- it can `validate` and `deploy`, and there is no input that makes it `run`. A deploy uploads
  definitions; a run spends the day's compute quota.

**The residual, plainly.** A workspace-wide token exists, in a public repository's CI
configuration, guarded by GitHub's environment protection and by an expiry date. If it leaks,
the workspace is compromised, and rotating it is the only remedy. `docs/runbook.md` carries
what to do the day it expires - which will happen, and is the expected end of its life rather
than an incident.

## Things a reader should distrust

- The three witnesses share an author. That is measured through the specification mutants, not
  denied.
- The percentages describe a simulation whose return rate is deliberately high.
- A claim rendered as "local run, not reproduced in CI" was produced on a laptop. The
  renderer labels it; treat it as weaker evidence than a CI-produced one.
- The equivalence classification in `mutation/equivalents.py` is a judgement call. The strict
  score, which refuses it entirely, is published next to the one that accepts it, and the
  assumption probe tries to falsify each entry.

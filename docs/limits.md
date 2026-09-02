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

## Still not executed here

The Databricks lane needs a workspace: the bundle, Unity Catalog, expectations, AUTO CDC, the
event log and the dashboard. Its sources are parsed, its rules are compared record by record
against the OSS implementation in `tests/spark`, and since round 12 its three Databricks-only
API calls are pinned by the open-source signatures they fail against.

**A deploy was attempted for the first time on 2 September 2026, against a real Free Edition
workspace, and it failed.** `databricks bundle validate -t free` passed; `databricks bundle
deploy -t free` died on the first POST because the pipeline resource carried no `name`, which
that API requires and validate does not check. The catalog step failed before that, for a
different reason: `databricks catalogs create` cannot work on a Default Storage metastore.
Both are fixed and both now have tests. **Nothing has yet run the pipeline**, so every figure
in `docs/databricks-run.md` still reads `NOT RUN`, and this section is worth exactly as much
as the identical sentence about the Delta lane was worth for eleven rounds: check it.

What changed in round 13 is that the lane is now deployable in one command and produces a
record when it is, so the sentence above has a date on it rather than being open-ended:

- `make databricks` reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` and runs the whole lane -
  catalog, validate, deploy, seed, run, fetch. `scripts/databricks_run.sh` is the script.
- `docs/databricks-run.md` holds the results, with every run-produced figure inside an anchor
  that currently reads `NOT RUN`. `tests/fast/test_databricks_bundle.py` fails if any of them
  holds a number while `evidence/databricks/SG-DBX-01.json` does not exist. A document cannot
  get ahead of its run by hand any more; that is the round-12 finding turned into a test.
- `tests/fast/test_databricks_bundle.py` also checks the bundle against the Free Edition limits
  it has to live inside, which is how four defects that would have failed the first deploy or
  the first run were found: a landing volume nothing created, two notebook tasks reading their
  catalog from a `spark.conf` key only a pipeline populates, an `event_log('')` built from a
  pipeline id that does not exist outside a pipeline, and a nightly schedule deployed
  UNPAUSED on an account where the daily quota is a hard stop.
- Whatever that run produces is **not** appended to `evidence/history.jsonl`. It cannot be
  recomputed by a reader with a clone, so it goes to `evidence/databricks/` and says why in
  its own `chain` field. `evidence/databricks/README.md` is the comparison in full.

## Not verifiable for free, at all

| exam area | why the free lanes cannot show it | what is done instead |
|---|---|---|
| cost in DBUs | `system.billing` needs an account console and a metastore-admin role; Free Edition has neither | files and bytes that a predicate cannot skip, read from the Delta log: deterministic, and labelled as a proxy |
| query profiles | a Databricks UI | Spark plans and the same file-level measurements |
| account-level security | no SSO, no SCIM, no account groups, no OAuth machine-to-machine | the controls are implemented in code and tested; the platform equivalents are declared in SQL and marked as declared |
| row filters and column masks, enforced | `databricks/sql/policies.sql` needs a SQL warehouse id to be applied, and a bundle on Free Edition can neither create a warehouse nor learn its id; `is_account_group_member` is false for everyone anyway | the file is deployed with the bundle, parsed by `tests/spark/test_databricks_lane_parses.py`, and **not applied by `make databricks`** - stated here rather than left to be assumed from its presence |
| a grant that keeps anyone out | `account users` is the only principal that exists, and it has one member: whoever deployed | the grants in `databricks/resources/grants.yml` show that the privilege was applied and can be read back, which is a different and much smaller claim |
| Delta Sharing as a provider | provider registration is not available on Free Edition | out of scope, stated rather than faked |
| a SQL warehouse that is ready when you are | on Free Edition the warehouse **stops itself after a few minutes idle**, so a cold start is the NORMAL case for anything that runs SQL here, not the exception. A serverless 2X-Small takes 40s to 2 minutes to come up, and `wait_timeout` on the Statement Execution API accepts at most `50s` - so no value of that parameter covers it | every wait in this lane is designed against the cold start rather than against the query: the statement is submitted with `on_wait_timeout: CONTINUE` and then POLLED to a terminal state, with a ceiling on the whole wait (5 minutes by default) and the warehouse started explicitly when it is not already running. The first version asked for 30s with CANCEL and reported a failure that had in fact created the catalog |
| creating a catalog through the Unity Catalog API | Free Edition uses **Default Storage**, so the metastore has no storage root and `databricks catalogs create` fails with `Metastore storage root URL does not exist` ([databricks/cli#4513](https://github.com/databricks/cli/issues/4513)) | `scripts/databricks_run.sh` issues `CREATE CATALOG IF NOT EXISTS` through the SQL Statement Execution API instead, which resolves the location through Default Storage; it checks the statement reached `SUCCEEDED` rather than assuming |
| `databricks bundle validate` as a deploy gate | it checks syntax, includes and variable resolution, and warns about unknown properties - it does **not** check that the request body it will send is one the API accepts | the required fields for every resource type are asserted from the REST API reference in `tests/fast/test_databricks_bundle.py`. This was found by a deploy dying on `name must be set` after validate said `Validation OK!` |
| Lakehouse Federation | needs an external database and a connector | out of scope |

## Things a reader should distrust

- The three witnesses share an author. That is measured through the specification mutants, not
  denied.
- The percentages describe a simulation whose return rate is deliberately high.
- A claim rendered as "local run, not reproduced in CI" was produced on a laptop. The
  renderer labels it; treat it as weaker evidence than a CI-produced one.
- The equivalence classification in `mutation/equivalents.py` is a judgement call. The strict
  score, which refuses it entirely, is published next to the one that accepts it, and the
  assumption probe tries to falsify each entry.

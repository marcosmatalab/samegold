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
  coordinate ADR 0002 pins. The `delta` job of `.github/workflows/spark.yml` runs the same two
  commands on `ubuntu-latest`; it had been red on the defect below since its first run, and
  this commit is the first that can pass it.
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
API calls are pinned by the open-source signatures they fail against - but nothing here has
deployed it or watched it run.

## Not verifiable for free, at all

| exam area | why the free lanes cannot show it | what is done instead |
|---|---|---|
| cost in DBUs | `system.billing` needs an account console and a metastore-admin role; Free Edition has neither | files and bytes that a predicate cannot skip, read from the Delta log: deterministic, and labelled as a proxy |
| query profiles | a Databricks UI | Spark plans and the same file-level measurements |
| account-level security | no SSO, no SCIM, no account groups, no OAuth machine-to-machine | the controls are implemented in code and tested; the platform equivalents are declared in SQL and marked as declared |
| Delta Sharing as a provider | provider registration is not available on Free Edition | out of scope, stated rather than faked |
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

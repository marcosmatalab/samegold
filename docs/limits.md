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
- The crash campaign on the silver stage: injection confirmed by exit code, convergence after
  restart, and a negative control that a non-idempotent writer fails.
- The cost lab on real Delta tables through delta-rs: compaction, clustering, partitioning and
  the copy cost of a delete.
- The privacy controls: masking, the exposure check, and a purge that deletes and vacuums.

## Written, tested, and not yet executed here

Everything that needs the Delta **jars** from Maven Central, because the machine this was
built on has no route to it: the Spark-side `MERGE`, change data feed through Spark, `OPTIMIZE`
through Spark, and the Spark reader over a Delta table. The code is written against the pinned
coordinate in ADR 0002 and the tests exist and SKIP with an explicit message rather than
passing silently. Note that the Delta *protocol* behaviour those tests cover is exercised here
by the cost lab and the purge through delta-rs, which is a different implementation of the same
format.

The Databricks lane needs a workspace: the bundle, Unity Catalog, expectations, AUTO CDC, the
event log and the dashboard.

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

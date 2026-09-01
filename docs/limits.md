# What this repository has not verified

A short document that a reviewer should read before believing anything else here.

## Verified by running it

- The fast lane (127 tests, ~15 s): generator determinism, the digest's refusals, the
  invariants, the statistics, the mutation engine, the evidence gate, the layering rules.
- The Spark lane **without Delta**: the Spark implementation and the DuckDB reference agree on
  `revenue_by_month` and on `dim_customer_scd2` at every close, and the digest is unchanged at
  2 and at 16 shuffle partitions.
- The crash campaign on the silver stage: two structural points, injection confirmed by exit
  code, convergence to the clean digest after restart.

## Written but not yet executed here

Everything that needs the Delta jars from Maven Central, because the machine this was built on
had no route to it: `MERGE`-based SCD2, time travel, change data feed, `OPTIMIZE`, liquid
clustering, and the crash points inside a Delta commit. The code is written against the pinned
coordinate in ADR 0002 and runs in the `delta` CI job; until that job is green, no claim in
the README depends on it.

## Not verifiable for free, at all

| exam area | why the free lanes cannot show it | what is done instead |
|---|---|---|
| cost attribution in DBUs | `system.billing` needs account-admin on a paid workspace; Free Edition has no account console | bytes read, files read, output file count and wall time from the Spark metrics and the Delta log, measured with repetitions |
| account-level security | no SSO, no SCIM, no account groups, no OAuth machine-to-machine on Free Edition | grants, row filters and column masks declared in the bundle, with a drift test that compares deployed to declared |
| Delta Sharing as a provider | provider registration is not available on Free Edition | the open-source Delta Sharing server, run locally, sharing the gold tables |
| Lakehouse Federation | needs an external database and a paid connector | out of scope, stated rather than faked |

## Things a reader should distrust

- The three witnesses share an author. That is measured (specification mutants), not denied.
- The percentages describe a simulation whose return rate is deliberately high.
- Provenance: a claim rendered as "local run, not reproduced in CI" was produced on a laptop.
  Treat it as weaker evidence than a CI-produced one, which is exactly how the renderer
  labels it.

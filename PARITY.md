# Parity: the open-source lane versus Databricks

Two runtimes run the same transformations. They are not the same platform, and the
differences are the part of this repository that took the most reading to get right.

## The one that cannot be papered over: Auto Loader

Auto Loader (`cloudFiles`) is **proprietary**. There is no open-source equivalent, in Spark
4.2 or anywhere else. The ingestion layer is therefore an adapter with two implementations
behind one contract test, and the guarantees that differ are these:

| | Auto Loader (Databricks) | Structured Streaming file source (OSS) |
|---|---|---|
| discovering new files | directory listing **or** cloud file notifications | directory listing only, every trigger |
| cost as the directory grows | roughly constant with notifications | O(number of objects) per trigger |
| seen-file state | RocksDB-backed, scales to millions | in the checkpoint, degrades with volume |
| schema evolution | `cloudFiles.schemaEvolutionMode`, schema hints, `_rescued_data` | declared schema plus `columnNameOfCorruptRecord`; no evolution modes |
| malformed records | rescued into `_rescued_data` | corrupt record column, and the semantics differ per format - but for the shape that matters here, a value too wide for its column, the two agree: one column nulled, the record kept, the raw line in the rescue column. Measured, not assumed |

The claims that depend on ingestion semantics are therefore made **per lane**, never once and
transferred.

## The three lanes agree, as of 3 September 2026 - and here is what it took

Everything below was written on the assumption that the Databricks lane computes the same
close as the other two. **On 2 September 2026 that assumption was tested against a real
workspace for the first time and it was false.** The correction stays at the top rather than
moving to a footnote, because the claim it corrects is the headline claim of this repository and
because what it cost is the useful part.

**It is true now, and by measurement rather than by repair-and-hope.** On 3 September 2026 the
lane produced `revenue_by_month` 2026-01 gross 14 198 046 from 425 lines and 2026-02 gross
199 379 from 3 - to the cent against what the OSS lane computes on the same seed - with 727
accepted and 28 quarantined across seven reasons out of 755, conservation closed, and a Type 2
dimension the hand-written MERGE's equal row by row, not merely in its totals. The record is
in this repository and every figure in `docs/databricks-run.md` is rendered from it. The account of the failure below
is kept in full.

What the run produced: `revenue_by_month` for 2026-01 with a gross of **2.767e19 cents** from
428 lines. The contract caps a single line at 10 000 x 1 000 000 = 1e10 cents, so that month's
revenue was six and a half million times the ceiling of one line. Three events did all of it -
`bad-0000007`, `bad-0000015`, `bad-0000023`, each with `unit_price_cents =
9223372036854775807` and `quarantine_reason = 'accepted'`. They are events the generator emits
in order to be REJECTED, and the other two lanes reject them.

Two causes, both now fixed and both covered by tests:

1. `gross_cents` was DOUBLE, not BIGINT, because Auto Loader inferred every bronze column as
   STRING and `qty * unit_price_cents` on two strings promotes to double. In a project whose
   thesis is that money is an integer number of cents, this lane's money was floating point.
2. On a STRING column, `unit_price_cents > 1000000` coerces the string to the INT32 literal's
   type; 9223372036854775807 overflows INT32 and non-ANSI Spark returns NULL. A NULL predicate
   does not match a `WHEN`, and the classification's `ELSE` was `accepted`.

So the honest statement of parity today is: **the rules agree, and the lanes did not, because
they ran on different types.** `docs/databricks-run.md` has the full account and
`docs/adr/0006-mutants-are-generated-not-planted.md` has what generalises from it. Nothing in
the table below should be read as "verified on all three lanes" until a run says so.

### What a second pass over that fix found

Fixing a defect and then re-reading the fix is not ceremony here; it produced three more, and
two of them were in the repair itself.

- **The literal's WIDTH, not just its value.** `1000000` is an INT32 literal, and the round
  above fixed the consequence (a NULL predicate can no longer be accepted) without fixing the
  cause (the predicate is still NULL). Every bound literal in Spark-dialect SQL now carries
  `L` and every bound in the PySpark lane goes through `_bound()`, which casts to bigint. The
  effect is measurable and is measured: re-run on STRING columns with ANSI pinned off - the
  exact reproduction - **no rule is undecidable any more**, and the record priced at
  Long.MaxValue leaves through `amount_out_of_range`, the door the contract has for it, rather
  than through the first rule that could not answer. `docs/limits.md` carries the table for
  all three engines, including the measurement that DuckDB does not have this hazard at all:
  it raises a binder error rather than answering NULL.
- **The rules agreed; their ORDER did not.** The OSS `CASE` tested the bounds before the
  currency and the Databricks `RULES` declared the currency before the bounds. Every record in
  the parity matrix broke exactly one rule, so the sequence decided nothing and twenty records
  agreed. Measured on the record that breaks two: an `order_placed` priced past the bound AND
  denominated in dollars was `amount_out_of_range` on the OSS lane and `unknown_currency` on
  the Databricks one. Same rules, different door, and a quarantine report is grouped by door.
  The OSS branches are built in the order `RULES` declares them now, and the matrix has the
  record that holds them there.
- **Acceptance is a conjunction, not the `ELSE`.** `WHEN NOT COALESCE(rule, false) ... ELSE
  'accepted'` is correct - the branches are total - and it is correct by an argument the
  reader has to reconstruct, in the exact place the previous version was wrong for want of it.
  Both lanes now say what they mean: `WHEN <every rule holds> THEN 'accepted'`, over the same
  declaration the rejection branches are generated from. The `ELSE` that remains is
  unreachable and `raise_error`s, which is the ruling on "the classification could not
  classify": a fault in the pipeline, not a member of the contract's closed enum.

**A parity improvement, measured rather than assumed**: a value too wide for its column
behaves the SAME on all three lanes. Spark reading a declared schema in PERMISSIVE mode nulls
that one column and keeps the rest of the record in `_rescued_data`; Auto Loader with the
schema hints does the same; DuckDB's `json_type` says UBIGINT and `TRY_CAST(... AS BIGINT)`
returns NULL. All three then quarantine it as `missing_required_field`. The generator emits
one deliberately (corrupt kind `beyond_bigint`) so that the agreement is exercised on every
seed rather than argued for here.

## The first thing the two Type 2 dimensions said to each other, and how it ended

`gold_close.py` has said for rounds that comparing AUTO CDC against the hand-written `MERGE` is
the point of maintaining both. The lane ran on 3 September 2026 and they disagreed.

| | AUTO CDC (Databricks) | hand-written MERGE (OSS) |
|---|---|---|
| version rows | **78** | **75** |
| closed rows | **18** | **15** |
| distinct customers | 60 | 60 |
| open rows | 60 | 60 |

`open_rows = customers` held on both, so the Type 2 property itself was intact; the difference
was exactly three versions, twice.

**The cause, measured rather than suspected.** The population contains 78 distinct
`customer_upserted` event ids and exactly three of them are HEARTBEATS - an upsert repeating
the segment and country the customer already had:

| customer | event | event_ts | segment / country |
|---|---|---|---|
| `C000028` | `cu-C000028-1` | 2026-01-14T15:00:00Z | vip / PT |
| `C000038` | `cu-C000038-1` | 2026-01-05T19:00:00Z | vip / IT |
| `C000043` | `cu-C000043-1` | 2026-01-13T14:00:00Z | pro / FR |

75 + 3 = 78 and 15 + 3 = 18. AUTO CDC produced one version per EVENT; the OSS lane produces one
per CHANGE. And it could not have done otherwise: AUTO CDC's default for SCD Type 2 is a new
version whenever ANY column changes, and the source view carries `event_ts` and `event_id`,
which change on every upsert by construction.

**Which is right is a contract question, and the contract had already answered it** - in
`samegold.pipelines.transform.dim_customer_scd2`: "A Type 2 dimension records CHANGES, not
heartbeats. An upsert that repeats the attributes the customer already had is not a new
version." Three OSS implementations agree on that (the domain rule in
`domain/bitemporal.scd2_from_versions`, the full recomputation, and the DuckDB reference), and
an earlier round was spent making them agree. So this lane was the one that was wrong.

The fix is `track_history_column_list=["segment", "country"]` - the same pair the OSS lane
compares with `lag()` - which makes a change to `event_ts` or `event_id` alone update the
current row instead of opening a version.

**IT RAN, the same day, and the divergence is closed.** From commit `8c9faa7` the workspace
produced **75 versions, 60 customers, 60 open rows and 15 closed** - the OSS lane's shape
exactly, in every one of the four numbers. That record is in this repository at
`evidence/databricks/SG-DBX-01.json`, so this is no longer a claim about a terminal:

| | AUTO CDC, first run | AUTO CDC, with `track_history_column_list` | hand-written MERGE |
|---|---|---|---|
| version rows | 78 | **75** | 75 |
| closed rows | 18 | **15** | 15 |
| customers | 60 | 60 | 60 |
| open rows | 60 | 60 | 60 |

The three heartbeat events above are the case that revealed it: they are the whole of the
difference, they are named, and they are what the fixture in
`tests/fast/test_databricks_dimension_parity.py` pins so that the explanation cannot drift away
from the numbers it explains.

And the comparison now EXISTS as a test, which it did not when it mattered.
`tests/fast/test_databricks_dimension_parity.py` pins the arithmetic (78 upserts, 3 heartbeats,
75 versions), asserts the property that no two consecutive versions of a customer are identical,
and **compares the OSS dimension against the workspace record's, for real, on every run of the
fast lane**. Falsified: doctored back to 78/18, it fails and prints both shapes.

### Row by row, which is what four aggregates could not tell you

The capture exists. `evidence/databricks/dim_customer_scd2.json` holds the workspace's own
seventy-five rows - `customer_id`, `segment`, `country`, `__START_AT`, `__END_AT` - and the
comparison is no longer a shape comparison and no longer a skip.

**They agree on every row.** The same sixty customers, the same seventy-five intervals, the same
attributes, the same instants - as multisets and as per-customer histories. Nothing appeared row
by row that the aggregates could not see: no displaced `__START_AT`, no different order in a
tie, no gap or overlap on either side. That is a result rather than a formality, because two
dimensions CAN agree on every total and disagree about which customer changed when, and until
this ran nobody knew which of those two was true here.

The finding of this round is therefore in the comparison and not in the data. Its first version
reduced each timestamp with `str(value)[:19]` - which is what made a workspace's
`2026-01-01T00:00:00+00:00` and the generator's `2026-01-01T00:00:00.000000Z` comparable, by
cutting off the part where they differ. It cut off the part where a WRONG one would differ too.
And it asked `row not in theirs`, which is a set question on a list. Both were falsified against
the committed capture:

| the capture, doctored | the old comparison | now |
|---|---|---|
| every timestamp moved to `+01:00` - an hour out, every row | **passed** | fails, in two tests |
| two versions repeated, 77 rows against 75 | **passed** | fails, in three |
| one `__END_AT` and the next `__START_AT` moved an hour, totals unchanged | untested | fails, in two |
| the zone dropped, a capture in local time | untested | refused by name |
| two versions swapped onto each other's customers | untested | fails, in three |

A comparison written about THREE EXTRA VERSIONS could not see extra versions. It compares
instants now, parsed and zone-bearing, and multisets rather than sets, and per-customer
histories in order.

And the captured half now says which run it came from. It was exported by hand, which produced
a file that could not: replace the record with a later run's and the comparison would have gone
on passing against rows the workspace no longer held - green, against a dataset that had stopped
describing the system. `publish_evidence.py` writes the capture itself now, in the same task and
session as the record, with a header naming the update; the commit reaches the workspace as a
bundle variable at deploy time rather than being written afterwards by whoever fetched the
files. Two ties are checked on every run of the fast lane: the two update ids, and the record's
six dimension aggregates recomputed from the captured rows - the second needs no header at all,
which is what covers the one capture whose header was typed rather than measured.

### One more dialect difference, measured on the way

`details:update_progress.state` - the `:` JSON-path operator on a STRING column - is Databricks
SQL. On pyspark 4.2.0 it raises; `get_json_object(details, '$.update_progress.state')` returns
the value on both engines, and `parse_json(details):path` returns a variant. `publish_evidence.py`
now uses `get_json_object`, and that is not tidying: it is what lets
`tests/spark/test_databricks_event_log_query.py` EXECUTE the lane's own update-state query
against a synthetic event log instead of merely parsing it - which is how the `MAX`-over-a-state
defect was caught in a test rather than in a record.

## Feature parity table

| capability | OSS lane | Databricks Free Edition | note |
|---|---|---|---|
| Spark Declarative Pipelines | yes, Apache Spark 4.2 (`spark-pipelines run`) | yes, Lakeflow | the spec file is `spark-pipeline.yml`; the OSS CLI has `init`, `run`, `dry-run` |
| expectations / data quality in the pipeline | **no** - not in the OSS SDP | yes | the OSS lane enforces the same rules as a `CASE` in `transform.quarantine_reason()` |
| AUTO CDC | **partial**: Spark 4.2 added declarative SCD **Type 1** upserts to open-source SDP; **Type 2 is Databricks-only** | yes, Type 1 and Type 2, with bitemporal tracking | the OSS lane builds Type 2 from the source versions and writes the `MERGE` by hand, which is what the exam asks you to be able to do anyway |
| pipeline event log | **no** | yes | the OSS lane records its own metrics into the evidence store |
| Delta Lake | 4.4.0 OSS | managed by Unity Catalog | `io.delta:delta-spark_4.2_2.13:4.4.0` |
| liquid clustering | `CLUSTER BY` yes | `CLUSTER BY AUTO` also | automatic clustering needs predictive optimization, which is Databricks-only |
| deletion vectors, row tracking, CDF, type widening | yes | yes | |
| catalog-managed tables / commit coordination | preview | yes | |
| Unity Catalog | the open-source UC server | full, one metastore | Free Edition has no external locations, so volumes are the only storage |
| system tables (`system.billing`, `system.access`) | n/a | **no** - they need account-admin, which Free Edition does not grant | cost is measured from Spark metrics and the Delta log instead |
| Delta Sharing | OSS server, as provider and recipient | recipient only | |
| Jobs / orchestration | `make` and the CI workflow | Lakeflow Jobs via a bundle | |
| bundles (Declarative Automation Bundles, formerly Asset Bundles) | n/a | supported, from outside the workspace with a PAT - **this repository has never deployed one**, see `docs/databricks-run.md` | Free Edition restricts outbound traffic, so deploying *from inside* the workspace is unreliable; deploy from outside |
| continuous streaming | yes, locally | **no** - time-based triggers are rejected on serverless (`INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`); one active pipeline per type, and quota exhaustion stops compute for the day | everything is designed around `Trigger.AvailableNow` plus a scheduled job |
| killing the process mid-write | yes | **no** - serverless gives you no process to kill | the whole crash campaign lives in the OSS lane, and that is stated rather than implied |
| Delta through a second implementation | yes: delta-rs 1.6.3 reads and writes the same tables, and the cost lab and the purge run on it | n/a | multi-engine interoperability is what the format is for, and it is also how the Delta-protocol claims get executed on a machine with no route to Maven |

### The four Databricks-only primitives, checked rather than asserted

The three rows above that say "Databricks-only" were prose for eleven rounds. They are now
each a signature in the open-source API that the Databricks sources fail against, and mypy
checks `databricks/src/` since round 11, so the boundary is enforced where it used to be
described. Against `pyspark 4.2.0`:

| what `databricks/src/` calls | what open-source Spark 4.2.0 offers | how it shows up |
|---|---|---|
| `dp.expect_all_or_drop(RULES)` | `pyspark.pipelines.api` has no such attribute | `error: Module has no attribute "expect_all_or_drop"` |
| `create_streaming_table(cluster_by_auto=True)` | `cluster_by` only, an explicit column list | `error: Unexpected keyword argument "cluster_by_auto"; did you mean "cluster_by"?` |
| `create_auto_cdc_flow(stored_as_scd_type=2)` | typed `Literal[1, "1"] \| None` | `error: incompatible type "Literal[2]"; expected "Literal[1, '1'] \| None"` |
| `create_auto_cdc_flow(track_history_column_list=...)` | the open-source signature has `column_list` and `except_column_list` and no history-tracking parameter at all | `error: Unexpected keyword argument "track_history_column_list" for "create_auto_cdc_flow"  [call-arg]` |

Each call keeps a narrow `# type: ignore` with the reason written beside it. They are not
worked around and not simulated: the code is right for the runtime it is deployed to, and the
open-source lane answers the same three objectives another way - a `CASE` expression for the
rules, `CLUSTER BY (customer_id)` with named columns, and a hand-written two-pass `MERGE` for
the Type 2 dimension, which `tests/delta` now executes.

What the open-source Delta lane **does** have, measured on the run described in
`docs/limits.md` rather than assumed: `CLUSTER BY (customer_id)` really clusters
(`DESCRIBE DETAIL` reports `clusteringColumns: [customer_id]`), deletion vectors really apply
(a single-row `DELETE` reports `numDeletionVectorsAdded: 1` and `numCopiedRows: 0`, and writes
a `deletion_vector_*.bin`), and `OPTIMIZE ... ZORDER BY` really Z-orders
(`operationParameters.zOrderBy` names the column). Only the **automatic** half of clustering
needs predictive optimization, and `CLUSTER BY AUTO` is a parse error outside Databricks.

## Claim by claim

| claim | verified in | not verified in |
|---|---|---|
| SG-00 repository facts | OSS | |
| SG-01 two implementations agree on the versioned close | OSS | Databricks: the reference cannot run there |
| SG-02 re-delivery is a no-op | OSS | Databricks: same input, different ingestion semantics; a separate run is needed |
| SG-03 mutation campaign | OSS | Databricks: mutating a deployed pipeline is not something to do to a workspace |
| SG-04 a closed month moves | OSS | reproduced on Databricks as a dashboard, as illustration, not as evidence |
| SG-05 invariants | both | |
| SG-06 evidence chain and seed provenance | OSS | |
| SG-07 crash campaign | OSS | Databricks: serverless gives you no process to kill, by design |
| SG-08 masking, exposure check, retention purge | OSS | Databricks: row filters and column masks are declared there, and unenforceable without account groups |
| SG-09 cost lab | OSS (delta-rs) | Databricks: the same experiments would be more interesting with predictive optimization and `CLUSTER BY AUTO`, and neither exists outside it |

## Cost

Zero euros, and the two reasons it is zero are worth stating because they are load-bearing:
GitHub Actions is free and unlimited on **public** repositories (standard runners: 4 vCPU,
16 GB RAM, 14 GB disk), and Databricks Free Edition has no 14-day limit. Free Edition does
have quotas that stop compute for the rest of the day when exceeded, its accounts may be
deleted after long inactivity, and its terms prohibit commercial use.

# What the Databricks lane measured, from the records it left behind

Every other figure in this repository can be recomputed from a clone with no account and no
credentials. **This page is the exception, and it is the only one.** The numbers below were
measured inside a Databricks Free Edition workspace on 5 September 2026 and written out by the
`publish_evidence` task of the `samegold monthly close` job. Nobody with a clone can re-run
them, and the records say so about themselves:

```json
"chain": {"chained": false,
          "why": "produced inside a Databricks workspace by a deploy, not by
                  `samegold evidence` on a commit of this repository (...)
                  nobody with a clone can recompute it"}
```

So this page exists to make the one unreproducible part of the repository at least **readable**
without an account. It is rendered by `samegold readme` from
[`evidence/databricks/SG-DBX-01.json`](../evidence/databricks/SG-DBX-01.json) and
[`evidence/databricks/dim_customer_scd2.json`](../evidence/databricks/dim_customer_scd2.json),
and `samegold check` fails if a single figure on it stops matching those files. Not one number
here was typed.

## What produced it

| | |
|---|---|
| job run | `517089489320521` |
| task run | `451890332619879` |
| pipeline | `f640de65-59ba-4dfc-838d-c6a569dad44c` |
| pipeline update | `0566092c-af8d-4126-967e-d014569442d1` |
| deployed from commit | `b131010988bd6552453501e697e3ad71ad1d08de`, clean tree |
| catalog | `samegold`, Unity Catalog, Free Edition |
| final state | <!--dbx:update.last_state-->COMPLETED<!--/dbx--> with <!--dbx:update.error_events-->0<!--/dbx--> error events |
| the job's branch | <!--dbx:orch.branch-->verify_each_restated_month<!--/dbx-->, because it decided `<!--dbx:orch.decision-->restated<!--/dbx-->` |

The ids are here so that they can be looked up by somebody who does have access to that
workspace, which is the only form of verification this page can offer. A record naming no run
would be a paragraph.

## The pipeline update, and the nine before it

<!--dbx:update_history.table-->| pipeline update | final state | ended |
|---|---|---|
| 0566092c-af8d-4126-967e-d014569442d1 | COMPLETED | 2026-09-05 12:57:16.315000 |
| 804d865e-53be-4678-bfcb-e761d1fc3f97 | COMPLETED | 2026-09-05 11:45:03.154000 |
| 0a9d9093-0afb-4e55-9205-d5c3da9855ca | COMPLETED | 2026-09-05 10:12:34.760000 |
| 289286cc-ca60-4ebf-8a71-6a996e71e0f1 | COMPLETED | 2026-09-04 08:43:49.773000 |
| 58d3de6f-311b-4204-a70d-3ba47ec78c31 | COMPLETED | 2026-09-03 18:01:18.026000 |
| 44a237b3-7742-4aba-a0bb-70c2993f98a3 | COMPLETED | 2026-09-03 13:24:36.214000 |
| b0cf0443-3166-45a7-8d4f-efb026886124 | COMPLETED | 2026-09-03 13:15:22.432000 |
| 79bf353a-1a4d-4fcd-be78-f9f314cfdcec | FAILED | 2026-09-03 12:54:57.046000 |
| 865c9dcf-21d3-4ae1-8dd1-ec32e1292756 | FAILED | 2026-09-03 12:49:22.740000 |
| c0322e9d-e2d7-4a2f-9932-6f193b258fdd | FAILED | 2026-09-03 12:46:26.553000 |<!--/dbx-->

## What the pipeline read and wrote

| table | rows |
|---|---|
| `bronze_events` | <!--dbx:rows.bronze_events-->1883<!--/dbx--> |
| `silver_classified` | <!--dbx:rows.silver_classified-->1883<!--/dbx--> |
| `silver_events` | <!--dbx:rows.silver_events-->1853<!--/dbx--> |
| `silver_quarantine` | <!--dbx:rows.silver_quarantine-->30<!--/dbx--> |
| `dim_customer_scd2` | <!--dbx:rows.dim_customer_scd2-->102<!--/dbx--> |
| `revenue_by_month` | <!--dbx:rows.revenue_by_month-->2<!--/dbx--> |
| `revenue_closed` | <!--dbx:rows.revenue_closed-->4<!--/dbx--> |

`silver_classified` holds every bronze row; `silver_events` and `silver_quarantine` partition
it, which is why the last two add up to the first. Nothing is dropped anywhere in the lane: a
row that fails a rule is moved, and the move is counted.

## The expectations the pipeline declared, and what they reported

These are Lakeflow expectations declared in `databricks/src/`, evaluated by the runtime, and
read back out of the event log. A rule with `failed > 0` is not a broken run: it is the rule
doing its job on data generated to reach it.

<!--dbx:expectations.table-->| rule | dataset | passed | failed |
|---|---|---|---|
| amount_out_of_range | samegold.main.silver_events | 554 | 1 |
| missing_required_field | samegold.main.silver_events | 555 | 0 |
| negative_price | samegold.main.silver_events | 555 | 0 |
| non_positive_quantity | samegold.main.silver_events | 555 | 0 |
| unknown_currency | samegold.main.silver_events | 554 | 1 |
| unknown_event_type | samegold.main.silver_events | 555 | 0 |
| unparseable_json | samegold.main.silver_events | 555 | 0 |<!--/dbx-->

## Where every bronze row ended up

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

## The four events the contract refused, with the value that did it

<!--dbx:bad_events.table-->| event id | why it was refused | qty | unit_price_cents |
|---|---|---|---|
| bad-0000007 | amount_out_of_range | 1 | 9223372036854775807 |
| bad-0000008 | missing_required_field | 1 | null |
| bad-0000016 | amount_out_of_range | 1 | 9223372036854775807 |
| bad-0000017 | missing_required_field | 1 | null |<!--/dbx-->

`9223372036854775807` is `Long.MaxValue`: a legal `BIGINT` that the contract's bound refuses.
The row beside it carrying `null` is `2^63`, one past the top of the column, which every
reader in this project drops into a NULL - so the value is gone and the count of the drop is
the only trace it leaves.

## The month that closed twice, as the workspace wrote it

One row per `(accounting_month, close_version)`. Earlier versions are never rewritten, which
is the whole of what "bitemporal" buys and the property `SG-04` measures in the open-source
lane.

<!--dbx:revenue_closed.table-->| month | version | gross | returns | net | lines | why this version exists |
|---|---|---|---|---|---|---|
| 2026-01 | 0 | 14198046 | 1286834 | 12911212 | 425 | first close |
| 2026-01 | 1 | 25582615 | 2314080 | 23268535 | 793 | late arrivals after close |
| 2026-01 | 2 | 37622605 | 3858662 | 33763943 | 1158 | late arrivals after close |
| 2026-02 | 0 | 199379 | 0 | 199379 | 3 | first close |<!--/dbx-->

The job verified each restated month before publishing it, and the checks it ran are its own
record of what it owed:

<!--dbx:close_verification.table-->| month | version | check | ok | detail |
|---|---|---|---|---|
| 2026-01 | 2 | earlier_versions_are_older | True | newest 2026-09-05 12:55:34, latest earlier 2026-09-04 09:04:41 |
| 2026-01 | 2 | month_was_eligible_to_close | True | recorded close instant 2026-09-05 12:55:34 against month 2026-01 |
| 2026-01 | 2 | net_is_gross_minus_returns | True | gross 37622605 - returns 3858662 = 33763943, net says 33763943 |
| 2026-01 | 2 | versions_have_no_gaps | True | 3 rows for versions 0..2 |
| 2026-01 | 2 | written_equals_the_source_month | True | written gross 37622605 lines 1158 / source gross 37622605 lines 1158 |<!--/dbx-->

<!--dbx:orch.checks_run-->5<!--/dbx--> checks ran and
<!--dbx:orch.checks_failed-->0<!--/dbx--> failed, over
<!--dbx:orch.months_written-->1<!--/dbx--> months and
<!--dbx:orch.versions_written-->1<!--/dbx--> versions.

## The Type 2 dimension, row by row

The dimension holds <!--dbx:dim.versions-->102<!--/dbx--> versions of
<!--dbx:dim.customers-->60<!--/dbx--> customers:
<!--dbx:dim.open_rows-->60<!--/dbx--> rows are open and
<!--dbx:dim.closed_rows-->42<!--/dbx--> are closed. A closed row is one with an `__END_AT`,
and a customer with a closed row and an open one has a HISTORY rather than a current value.
That is the difference between a Type 2 dimension and a snapshot, and it is the thing a
screenshot of this table would have been taken to show.

<!--dbx:scd2.customers_with_history-->36<!--/dbx--> of the
<!--dbx:dim.customers-->60<!--/dbx--> customers have more than one version.
<!--dbx:scd2.customers_shown-->6<!--/dbx--> of them are printed here in full, out of
<!--dbx:scd2.rows_captured-->102<!--/dbx--> rows captured; the rest are in
[`dim_customer_scd2.json`](../evidence/databricks/dim_customer_scd2.json), which carries the
`SELECT` that produced them.

<!--dbx:scd2.table-->| customer_id | segment | country | __START_AT | __END_AT |
|---|---|---|---|---|
| C000001 | pro | IT | 2026-01-01T00:00:00+00:00 | 2026-01-13T00:00:00+00:00 |
| C000001 | vip | IT | 2026-01-13T00:00:00+00:00 | (open) |
| C000002 | pro | FR | 2026-01-01T00:00:00+00:00 | 2026-01-03T09:00:00+00:00 |
| C000002 | retail | FR | 2026-01-03T09:00:00+00:00 | 2026-01-08T04:00:00+00:00 |
| C000002 | pro | IT | 2026-01-08T04:00:00+00:00 | (open) |
| C000003 | vip | PT | 2026-01-01T00:00:00+00:00 | 2026-01-10T16:00:00+00:00 |
| C000003 | vip | IT | 2026-01-10T16:00:00+00:00 | (open) |
| C000005 | pro | ES | 2026-01-01T00:00:00+00:00 | 2026-01-04T06:00:00+00:00 |
| C000005 | pro | IT | 2026-01-04T06:00:00+00:00 | (open) |
| C000008 | pro | IT | 2026-01-01T00:00:00+00:00 | 2026-01-09T14:00:00+00:00 |
| C000008 | vip | PT | 2026-01-09T14:00:00+00:00 | (open) |
| C000009 | pro | FR | 2026-01-01T00:00:00+00:00 | 2026-01-11T12:00:00+00:00 |
| C000009 | pro | IT | 2026-01-11T12:00:00+00:00 | (open) |<!--/dbx-->

## What this page is not

It is not reproducible, and that is the point of saying so here rather than leaving a reader to
work it out. Three things follow from it, and all three are checkable:

1. **These figures back no claim.** `SG-DBX-01` is not in `evidence/history.jsonl` and no row
   of the claims table on the front page rests on it. The chain refuses records it cannot tie
   to a commit of this repository, and refuses these.
2. **The agreement with the open-source lane is still checkable.** `PARITY.md` compares the two
   lanes claim by claim, and `tests/spark/test_databricks_population_digest.py` executes the
   notebook's own statement - extracted from the notebook, not restated - against the Python
   one over the real population, with no workspace involved.
3. **The bundle is checked without a workspace.** 121 tests in `tests/fast/` drive
   `databricks/` and `scripts/databricks_run.sh` against a stub CLI on PATH: every notebook
   path, every widget, every job parameter, the concurrent-task ceiling, and the guard that
   refuses to run a job deployed from a commit that is not HEAD.

What is left over is this page: a run that happened, in a workspace that this repository has no
credentials for, with its ids written down so somebody who does have them can look it up.

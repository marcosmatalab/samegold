# One key takes 30% of the rows. What happens to the join, measured

This is the question every Spark interview asks, and it is worth a page here because the answer
this repository found is not the one it went looking for: **on a machine, with adaptive
execution left on, the skew does not happen.** Not at two hundred thousand rows, and not at two
million either. The measurement is below, and so is the reason this is a page rather than a
claim.

## The experiment

A fact table where the key `C-HOT` holds 30% of the rows and five hundred other keys share the
rest, joined to a dimension on that key. Broadcast is turned off, so it is a shuffle join and
not a lookup - the shuffle is the thing the question is about. Adaptive execution is on, its
skew-join handling is on, and sixteen shuffle partitions are requested.

What is measured is the **skew factor**: rows in the largest partition after the join, over the
mean. One means the rows are spread evenly; the 5.62 in the table below means the busiest task
has five and a half times the rows of the average one, which is what "a skewed join" means in
practice.

No duration is published here, and that is
[ADR 0008](adr/0008-cost-is-measured-in-files-and-bytes.md): this repository does not make
latency claims, because a second measures the machine. A ratio of rows does not.

## The table

Produced by [`scripts/measure_join_skew.py`](../scripts/measure_join_skew.py), which is in this
repository and which `make skew` runs. It needs pyspark and a JVM, so it belongs on the same
machine as the Spark lanes.

| rows joined | AQE coalescing | partitions | max rows | mean rows | **skew factor** |
|---:|---|---:|---:|---:|---:|
| 200 000 | on (the default) | 1 | 200 000 | 200 000.0 | **1.00** |
| 200 000 | OFF | 16 | 70 200 | 12 500.0 | **5.62** |
| 2 000 000 | on (the default) | 1 | 2 000 000 | 2 000 000.0 | **1.00** |
| 8 000 000 | on (the default) | 2 | 4 752 000 | 4 000 000.0 | **1.19** |

Read the first and third rows first. **At two hundred thousand rows and at two million, the
join produces ONE partition and the skew factor is exactly 1.00.** Adaptive execution coalesces
the sixteen requested shuffle partitions into one, because the advisory partition size is 64 MB
and two million narrow rows are a fraction of that. A single partition cannot be skewed: there
is nothing for the hot key to be unbalanced against.

The second row is the same two hundred thousand rows with coalescing turned off. Now there are
sixteen partitions, the hot key's partition holds 70 200 rows against a mean of 12 500, and the
skew factor is **5.62**. The skew was always in the data. What decides whether it exists as a
problem is a runtime setting.

The fourth row is where it starts to appear on its own: eight million rows produce two
partitions and a factor of 1.19. Extrapolating the byte count, the hot key needs somewhere near
a hundred million rows before a laptop's local Spark gives it a partition of its own.

## Why this is not a claim in `evidence/history.jsonl`

Every figure this repository publishes as a claim is recomputed from the seeds its own record
names, by `samegold verify-latest`, on every push and on two architectures. This one cannot be,
and not for a reason of budget.

The only row of that table in which the measured quantity is interesting is the second, and it
requires `spark.sql.adaptive.coalescePartitions.enabled=false`.
[ADR 0005](adr/0005-adaptive-execution-stays-on.md) decided, three weeks before this page was
written, that adaptive execution stays on:

> AQE stays enabled, because turning it off would be tuning the experiment to fit the claim,
> and because the production configuration is the one that should be under test.

A claim whose number only exists under a configuration that an accepted ADR calls tuning the
experiment is not a claim this repository can publish. The honest options were to publish it
anyway with a footnote, to quietly loosen ADR 0005, or to write this page. The first two are
the shape [`FINDINGS.md`](../FINDINGS.md) is full of.

There is a second reason, smaller and also structural: **no claim in this chain imports
pyspark.** `SG-01`'s two implementations are the DuckDB reference and the generator's ledger;
Spark is compared in `tests/spark/`. That is why `verify-latest` can run in a lane with no JVM.
A join claim would be the first to need one, and would join `SG-07` among the claims that are
published without being recomputed by default - which is a cost, and it is not one worth paying
for a number that reads 1.00.

## The mitigation half, in prose, because prose is what this project can honestly offer

The interview asks what you do about it. The three answers, and what each costs:

**Broadcast the small side.** If the dimension fits in memory, there is no shuffle, so there is
no skew to have. It is the first thing to try and the reason this measurement had to disable it
explicitly: left on, Spark broadcasts a five-hundred-row dimension and the question evaporates.
It stops being available when the small side is not small - a customer dimension of tens of
millions of rows is not broadcastable, and that is the case the question is really about.

**Adaptive skew-join handling.** `spark.sql.adaptive.skewJoin.enabled`, on by default, splits a
partition that is both larger than `skewedPartitionFactor` times the median and larger than
`skewedPartitionThresholdInBytes`, and replicates the matching side. It is the answer for most
real cases, and it is worth knowing that it did **not** engage in any row of that table -
`plan_mentions_skew` is false in all four - because the partitions never got big enough in
bytes to cross the threshold. A setting that is on and never fires looks identical, from a
green run, to one that is doing its job.

**Salting.** Add a random bucket to the hot key on the fact side, explode the dimension across
the same buckets, join on the pair. It works when the other two do not, and it costs: the
dimension is replicated N times, the join key is no longer the business key, and every
downstream reader has to know. It is the answer you give when asked what to do when AQE is not
enough, and it is the one to reach for last.

**What this project cannot tell you** is which of the three is right for a given cluster,
because a cluster is exactly what it does not have. Everything here runs on one machine with no
network shuffle, and the cost of a skewed partition on one machine is not the cost of one
across a shuffle boundary between executors. [`docs/limits.md`](limits.md) says this about the
project generally; this page says it about the one question where it matters most.

## What the measurement is actually good for

Two things that are not the interview answer.

**It says what size the question starts at.** Anybody proposing to demonstrate join skew on a
laptop should know that two million rows will show them a skew factor of 1.00, and spending an
afternoon on it will produce a number that says nothing. That is a useful thing to have
measured, and it was going to be six hours of somebody's time.

**And it is a worked example of the rule this repository keeps.** The measurement exists, the
script is here, the table is printed, and none of it became a claim - because the configuration
that makes the number interesting is one an accepted decision forbids. A repository that
publishes only what survives its own rules is the whole argument; this is what it looks like
when the rules say no.

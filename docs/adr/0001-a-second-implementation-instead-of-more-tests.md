# ADR 0001 - a second implementation instead of more assertions

**Status** accepted, 2026-09-01

## Context

The pipeline computes a number that someone signs. Unit tests over transformations catch
mistakes the author thought of. The failure mode that matters here is the one nobody thought
of: a join that multiplies rows on one particular day, a deduplication key that is right
until a producer replays a file, a timezone that is right until the last day of a month.

## Decision

Compute gold twice, in two engines, from one contract: Spark (the deliverable) and DuckDB
(the reference). Compare canonical digests. Add a third witness, the generator's ledger,
which is a record of what was emitted rather than a recomputation of it.

## What we gave up

- **Cost.** Two implementations to keep in step. Every contract change is two edits.
- **False confidence.** Agreement between two versions by the same author is much weaker
  evidence than it feels. This is measured rather than assumed: the specification mutants
  exist precisely to show which misunderstandings both versions share.

## Alternatives considered

- **More unit tests.** Cheaper, and blind in the same places as the code.
- **Great Expectations / DQX / Soda.** Excellent at shape and at data quality rules; they do
  not compute the number a second time, which is the thing being checked here.
- **data-diff.** Closest tool in spirit, and its open-source version was sunset in 2024,
  which is itself informative: table diffing as a product did not stand on its own.

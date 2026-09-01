# ADR 0008 - the cost lab measures files and bytes, never seconds

**Status** accepted, 2026-09-01

## Context

Thirteen per cent of the exam is cost and performance, and the honest constraint is that this
project cannot measure cost: `system.billing` needs an account console that Free Edition does
not have. The tempting substitute is wall time, and wall time in a container measures the
container: it moves with the machine, with the page cache, with whatever else is running, and
a reviewer discounts it on sight.

## Decision

Measure what the layout determines and what the engine actually uses to skip work:

- **files a predicate cannot skip**, computed from the per-file min/max statistics recorded in
  the Delta log - the same statistics data skipping uses;
- **bytes in those files**;
- **rows copied** to rewrite the survivors of a delete.

All three are deterministic: the same input gives the same number on any machine, which is
what makes them publishable as evidence rather than as anecdote.

The lab runs on delta-rs, so it needs no JVM and no Maven, and it doubles as a second
implementation reading tables in the format the Spark lane writes.

## What we gave up

- Any statement about latency. The README says so.
- Any statement about money. It says that too.

## What it produced

A negative result, which the claim treats as a check that has to pass rather than as a
footnote: clustering by (month, sku) does nothing
for a sku predicate when the table has three files, because three files cover the whole key
range. At a smaller target file size the same clustering skips 85% of the bytes. Publishing
both numbers is what stops the second one from being a lie by omission.

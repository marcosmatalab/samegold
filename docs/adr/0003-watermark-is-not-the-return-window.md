# ADR 0003 - the 45-day return window is not a watermark

**Status** accepted, 2026-09-01

## Context

Two kinds of lateness look alike and behave nothing alike:

- **Arrival lateness**: an event happened at 10:00 and reached us at 10:06, or at 15:00 after
  a producer retry. Bounded by the producer's retry policy, measured in minutes to hours.
- **Business lateness**: a sale in January is returned in February. Bounded by the contract,
  45 days.

The tempting design is one watermark that covers both. It cannot: a 45-day watermark means
keeping 45 days of streaming state and delaying every output by 45 days, and a 2-hour
watermark silently drops the returns that are the entire point of the domain.

## Decision

- The **watermark is 2 hours** and governs arrival lateness only: streaming deduplication
  state and nothing else.
- **Business lateness is not a streaming concern.** A late return is a normal fact with an old
  event time. It is joined to its sale by key, imputed to the sale's month, and produces a new
  **version** of an already-closed month.
- Therefore gold is **bitemporal**: `revenue_by_month` is keyed by
  `(accounting_month, close_version)` with `restated_at`, and no version is ever rewritten.

## What we gave up

A "reordering the input does not change the output" invariant, which is false under any
watermark and is not claimed anywhere in this repository. What is claimed is the weaker and
true version: invariance under permutations that preserve watermark order.

## Consequence worth knowing

A duplicate that arrives after the watermark has expired the state that would recognise it
escapes streaming deduplication. That escape rate is a measurable quantity, not zero, and the
close protects itself from it with a second, stateless deduplication by `event_id` at the
gold boundary.

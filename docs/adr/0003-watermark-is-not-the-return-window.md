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

- The **watermark is 2 hours** and would govern arrival lateness only: streaming
  deduplication state and nothing else. It is declared as `WATERMARK_DELAY` in
  `src/samegold/domain/contract.py` and, as of this writing, **is read by no code**: the
  pipeline in this repository deduplicates statelessly at the gold boundary and has no
  `withWatermark` anywhere. An adversarial review found this ADR describing a mechanism the
  repository does not contain, which is the failure mode a design document invites. The
  constant stays because the number is part of the contract and the Databricks lane's
  streaming tables are where it would be applied; the claim that it is applied has been
  removed.
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

A duplicate that arrives after a watermark has expired the state that would recognise it
escapes streaming deduplication. That escape rate is a measurable quantity and it is not
zero, which is why the close does not depend on streaming deduplication at all: it
deduplicates by `event_id` at the gold boundary, statelessly, over the whole history. That is
more expensive and it is correct regardless of arrival order, which is the trade this project
takes. SG-02 measures it: a full re-delivery of every file leaves the digest unchanged.

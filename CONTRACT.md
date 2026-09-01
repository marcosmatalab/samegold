# Data contract

Version 1.2.0. The machine-readable half lives in `src/samegold/domain/contract.py`; a test
fails if this document and that module disagree about the window, the timezone or the
quarantine reasons.

## Producers and events

| event | key | meaning |
|---|---|---|
| `order_placed` | `event_id`, one per order line | a line was sold |
| `order_line_amended` | `event_id` | the quantity of a line changed before shipment. Last amendment wins, ordered by event time then event id |
| `return_registered` | `event_id` | a customer returned units of a line |
| `customer_upserted` | `event_id` | a new version of a customer's attributes, valid from its event time |

`event_id` is the producer's **idempotency key**: stable across retries. This is what lets
the pipeline deduplicate re-delivered content, and it is the reason the deduplication key
must never include the file path (specification mutant SPEC-03).

`arrival_ts` is written by the ingestion layer, never by the producer. It is the only
clock-like column allowed downstream and it is excluded from every digest.

## Rules

1. **Money is integer cents.** No floats, no decimals, no rounding, anywhere.
2. **The accounting timezone is Europe/Madrid.** Periods are computed after converting from
   UTC. Using UTC would move two hours of sales per day into the wrong month.
3. **The return window is 45 days**, half-open on the right: a return exactly 45 days after
   the sale is inside, 45 days and one microsecond is outside.
4. **A return is imputed to the month of the SALE**, not of the return. This is the rule that
   makes a closed month reopen, and therefore the reason gold is bitemporal.
5. **A return cannot exceed the effective quantity** of its line.
6. **A close is a version.** At each close, `revenue_by_month` records what was known at that
   instant. Later arrivals never rewrite a version; they add one, with `restated_at` and a
   reason.
7. **A record leaves through exactly one door**: accepted, quarantined with one of the
   reasons in the closed enum, rescued, or deduplicated. `ingested = accepted + quarantined +
   rescued + deduplicated`, per batch and cumulatively.

## Service levels

| consumer | table | freshness | note |
|---|---|---|---|
| finance, monthly close | `gold.revenue_by_month` | closed at 23:59:59 on day 5 of the following month, Europe/Madrid | a close is signed off and never rewritten; restatements are new versions |
| returns operations | `gold.returns_operational` | < 15 minutes from `event_ts` | may be incomplete; never used for the close |

## What breaks the contract

Adding a column is compatible. Changing the meaning of `accounting_month`, the length of the
window, the deduplication key or the imputation rule is **not**: each of those is a
specification mutant in `src/samegold/mutation/spec_mutants.py`, and each is expected to
change the published close. If one of them ever stops changing it, the pipeline has stopped
implementing this document.

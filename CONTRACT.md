# Data contract

Version 1.2.0. The machine-readable half lives in `src/samegold/domain/contract.py`; a test
fails if this document and that module disagree about the version, the window, the timezone
or the set of quarantine reasons: the reasons are enumerated below and
`tests/fast/test_contract_documents.py` compares that list with the enum in both directions.

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
3. **The return window is 45 days**, CLOSED on the right: a return exactly 45 days after the
   sale is inside, 45 days and one microsecond is outside. (An earlier version of this line
   said "half-open on the right" while describing a closed interval. The wording is worth
   getting right: the two implementations disagreed at exactly this boundary because one of
   them measured it with a function that truncates to whole seconds.)
4. **A return is imputed to the month of the SALE**, not of the return. This is the rule that
   makes a closed month reopen, and therefore the reason gold is bitemporal.
5. **A return cannot exceed the effective quantity** of its line.
6. **A close is a version.** At each close, `revenue_by_month` records what was known at that
   instant. Later arrivals never rewrite a version; they add one, with `restated_at` and a
   reason.
7. **A record leaves through exactly one door**: accepted, quarantined with one of the
   reasons in the closed enum, rescued, or deduplicated. `ingested = accepted + quarantined +
   rescued + deduplicated`, cumulatively over the whole input. It is checked over the input,
   not per batch: there is no per-batch accounting in this repository and claiming one would
   be describing a mechanism that does not exist.
8. **A refused return is reported, not dropped.** Returns outside the window and for more
   units than were sold are counted in `returns_rejected_count` on the month of the sale. A
   return **without an order** has no sale month to be counted on, so it is classified
   `return_without_order` and appears in the quarantine accounting rather than in gold; that
   asymmetry is deliberate and it is the one gap in "a record that leaves the pipeline
   without a counter is the failure nobody detects".

## Quarantine reasons

The closed enum. A record that is not accepted leaves through exactly one of these doors, and
every one of them is reachable by at least one implementation (a test refuses a reason that
nothing can emit, and refuses an implementation that can emit one that is not here).

| reason | meaning |
|---|---|
| `unparseable_json` | the line is not JSON, or carries no `event_id`: nothing can be routed |
| `unknown_event_type` | the `event_type` is missing or is not one of the four in the contract |
| `missing_required_field` | a field the event type requires is absent, or a timestamp is not one |
| `non_positive_quantity` | a sale or a return of zero or fewer units |
| `negative_price` | a unit price below zero |
| `unknown_currency` | a currency other than EUR |
| `return_without_order` | a return whose `(order_id, sku)` matches no accepted sale |
| `return_outside_window` | a return before the sale, or more than 45 days after it |
| `return_exceeds_sold_qty` | a return for more units than the line's effective quantity |

There is deliberately **no** `duplicate_event_id`. A duplicate is not quarantined, it is
deduplicated, and it is counted through its own term in the conservation identity; giving it
a quarantine reason as well would count it twice.

## Service levels

| consumer | table | freshness | note |
|---|---|---|---|
| finance, monthly close | `gold.revenue_by_month` | closed at 23:59:59 on day 5 of the following month, Europe/Madrid | a close is signed off and never rewritten; restatements are new versions |
| returns operations | `gold.revenue_by_month` at its newest version | < 15 minutes from `arrival_ts` to gold visibility; the rule is `src/samegold/serve/freshness.py` and wiring it to a platform alert is milestone M16 | may be incomplete; never used for the signed close |

## Column classification

The privacy half of the contract, enforced by `samegold.governance.policy` and checked over
the OUTPUT rather than trusted at the point of masking.

| column | classification | treatment | why |
|---|---|---|---|
| `customer_id` | direct identifier | pseudonymise (salted SHA-256) | the join key to every system that knows who this is |
| `country` | quasi-identifier | generalise to a region | harmless alone, identifying together with a segment and a purchase history |
| `segment` | attribute | keep | a commercial label; the close is reported by it |
| `order_id`, `sku`, money | business | keep | not about a person |

Retention: the purge deletes rows past the horizon AND vacuums the files that held them,
because time travel returns purged rows until the files are gone. The horizon is a parameter
of `governance.retention.purge_expired`; the demonstration in SG-08 uses a quarter of the
simulated span so that rows fall on both sides of it. There is no production horizon to
declare here because there is no production.

## What breaks the contract

Adding a column is compatible. Changing the meaning of `accounting_month`, the length of the
window, the deduplication key or the imputation rule is **not**: each of those is a
specification mutant in `src/samegold/mutation/spec_mutants.py`, and each is expected to
change the published close. If one of them ever stops changing it, the pipeline has stopped
implementing this document.

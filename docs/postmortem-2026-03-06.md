# Post-mortem: January closed twice, and the second time was right

**Date of incident** 5 March 2026, 23:59 CET (the February close)
**Detected** by the close report, before anyone in finance opened it
**Impact** the signed January close was overstated by <!--sg:SG-04.artifact.worst_delta_eur-->26 838,60<!--/sg--> EUR, <!--sg:SG-04.artifact.worst_move_pct-->4.4851<!--/sg-->% of net revenue
**Data loss** none
**Status** closed; two actions done, one open

## What happened

January was closed on 5 February with net revenue of <!--sg:SG-04.artifact.worst_first_close_eur-->598 396,86<!--/sg--> EUR. Between that close and
the February one, returns arrived for January sales, along with order-line amendments that had
been in flight when January closed. Under the contract a return is imputed to the month of the
**sale**, so all of them belonged to January: a month that finance had already signed.

At the February close the pipeline recorded a new version of January at <!--sg:SG-04.artifact.worst_final_eur-->571 558,26<!--/sg--> EUR, with
`restatement_reason = "late arrivals after close"`. Version 0 was not touched. Both versions
are in `gold.revenue_by_month`, and the close report shows them side by side.

## Why this is not a bug

It is the contract working. The 45-day return window is a commercial commitment, and the
imputation rule is an accounting one; together they mean a January close can move until
mid-March. The alternative designs were considered and rejected:

- **Impute returns to the month of the return.** January would never move, and February would
  carry refunds for goods it never sold. Finance would reconcile it by hand for ever. This is
  specification mutant SPEC-01, and the harness kills it.
- **Wait 45 days before closing.** The close would be six weeks stale, which fails the reason
  the close exists.
- **Rewrite version 0.** The number finance signed would silently change. This is the one that
  gets people fired.

## Timeline

| when | what |
|---|---|
| 5 Feb 23:59 | January closed, version 0, <!--sg:SG-04.artifact.worst_first_close_eur-->598 396,86<!--/sg--> EUR |
| 6 Feb - 4 Mar | late returns and amendments arrive for January sales |
| 5 Mar 23:59 | February close runs; a new January version is recorded, <!--sg:SG-04.artifact.worst_final_eur-->571 558,26<!--/sg--> EUR |
| 6 Mar 09:10 | the close report shows the restatement; finance notified before they asked |

## What went well

- The restatement was **recorded**, not applied in place: version 0 still says what was signed.
- The report showed it, so the conversation started with the number rather than with a query.
- The invariant that close versions are dense and `restated_at` is monotonic held, so the
  history is a history rather than a set of overwrites.

## What did not

- **Nobody was told automatically.** The restatement was visible; nothing pushed it. Finance
  found out because someone opened the report. That is luck, not a process.
- **The size of a restatement is not bounded anywhere.** A <!--sg:SG-04.artifact.worst_move_pct-->4.4851<!--/sg-->% move is normal for this
  business; a 40% move would mean something is broken upstream, and nothing would have said so.

## Actions

| # | action | state |
|---|---|---|
| 1 | publish every version in the close report, with the change against version 0 | done (`samegold report`) |
| 2 | alert when a close is overdue, separately from a stale pipeline: different causes, different responders | done (`serve/freshness.py`) |
| 3 | alert when a restatement moves a signed month by more than a declared threshold | open, milestone M16 |

## Where the numbers come from, and what is narrative

The **figures** are the published SG-04 evidence for the worst month, at the seed derived
from the commit that produced it, and they are RENDERED into this file by `make readme` from
`evidence/runs/SG-04.json` rather than typed. Every seed derives from the commit SHA, so the
figures change with every commit; the first draft of this document invented them, the second
corrected them by hand, and two commits later they were stale again. A number that appears in
prose has to be rendered or it will drift, and `tests/fast/test_documentation.py` fails if
this file and that record disagree.

The **narrative** - the dates, the detection at 09:10, who told whom - is written around
those figures, because the generated dataset has closes and versions and no organisation.
The first draft of this document also invented the euro amounts, which is exactly the habit
it now exists to demonstrate against; an adversarial review checked them against the evidence
and none of them matched. That correction is the reason for the test.

## What this incident is doing in a portfolio repository

Because it is the shape of the work. A pipeline that has never restated a closed month has
never met a real return policy, and an engineer who has not had this conversation with
finance does not know what the bitemporal model is for.

# ADR 0004 - what the two implementations share, and what they must not

**Status** accepted, 2026-09-01

## Context

If the Spark pipeline and the DuckDB reference share code, their agreement means nothing. If
they share nothing, they will disagree about spellings and units for ever and the harness
will be noise.

## Decision

They share the **contract** and duplicate the **computation**.

Shared (`domain/contract.py`): column names, the 45-day window, the accounting timezone, the
currency, the closed enum of quarantine reasons, the digest projections.

Duplicated: every derivation. `src/samegold/pipelines/transform.py` is Python DataFrame code;
`oracle/gold_revenue.sql` is SQL written against the same document, not translated from the
Python.

`domain/rules.py` is shared by the Spark implementation and by the generator's ledger, and is
**not** shared with the DuckDB reference. That asymmetry is the whole source of the
reference's independent value, and it is why the witness matrix reports a per-witness
marginal kill count instead of one number.

## What this bought, twice

Two defects that no single-implementation test could have found, both at the same boundary:

1. The Spark side measured the 45-day window with `unix_timestamp()`, which truncates to whole
   seconds, so a return one microsecond outside the window came back as exactly 45 days and was
   accepted. One return per run, five thousand cents, every invariant green.
2. The DuckDB side measured the same window with `INTERVAL 45 DAY` over a `TIMESTAMPTZ`, which
   is calendar arithmetic in the session timezone: under `Europe/Madrid` the window is 44h23 or
   45h01 long across a daylight-saving boundary. It only bites twice a year, and the accounting
   timezone this project declares is exactly the one where it bites.

Both are now compared in seconds with sub-second precision, and a test runs the reference under
three timezones.

## Also duplicated, and deliberately

The bitemporal version bookkeeping (which closes produce a new version of a month) is written
twice: once as a pure function over snapshots for the reference, once as a window function for
Spark. Sharing it would have been easy and would have made their agreement on the version
history mean only that the import worked.

## What we gave up

Duplicated effort, and a class of divergence that is pure noise (a rename in one and not the
other). The mitigation is that the shared contract carries every name.

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

Duplicated: every derivation. `pipelines/transform.py` is Python DataFrame code;
`oracle/gold_revenue.sql` is SQL written against the same document, not translated from the
Python.

`domain/rules.py` is shared by the Spark implementation and by the generator's ledger, and is
**not** shared with the DuckDB reference. That asymmetry is the whole source of the
reference's independent value, and it is why the witness matrix reports a per-witness
marginal kill count instead of one number.

## What we gave up

Duplicated effort, and a class of divergence that is pure noise (a rename in one and not the
other). The mitigation is that the shared contract carries every name.

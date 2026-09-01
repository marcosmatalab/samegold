# ADR 0005 - adaptive execution stays on; the digest absorbs it

**Status** accepted, 2026-09-01

## Context

Adaptive query execution changes partition counts and join strategies between runs. That
changes file layout, row order and the number of output files, so a naive digest over a
table is not stable across runs, and the tempting fix is `spark.sql.adaptive.enabled=false`.

## Decision

AQE stays enabled, because turning it off would be tuning the experiment to fit the claim,
and because the production configuration is the one that should be under test. Stability
comes from the digest instead: a `Projection` refuses to exist without an explicit total
order, and the digest sorts by it before hashing.

`tests/spark/test_transform_matches_reference.py::test_the_digest_does_not_depend_on_the_shuffle`
runs the same computation at 2 and at 16 shuffle partitions and asserts the digest is
identical. Without that test, every "the digests matched" statement in this repository would
be a statement about one particular partitioning.

## What we gave up

Byte-level comparison of the physical files, which would have been a stronger-sounding and
much weaker claim: it would fail for reasons that have nothing to do with the data.

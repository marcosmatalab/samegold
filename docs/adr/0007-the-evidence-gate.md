# ADR 0007 - the evidence is hash-chained and its seeds are derived, not chosen

**Status** accepted, 2026-09-01

## Context

The first version of the evidence store was a sink: `append` wrote whatever it was handed,
`latest` returned the last record per claim, and the renderer printed it. An adversarial
reviewer appended two records by hand claiming 999/999 agreements and a 100% mutation score,
pointed one at a CI run that does not exist, regenerated the documents and ran the suite. All
152 tests passed.

Worse, `generator/seeds.py` already claimed the gate existed: "The evidence gate rejects any
run whose seeds do not match the SHA recorded in the same evidence record". It did not.

## Decision

Three defences, in the order a forger meets them.

1. **A hash chain.** Every record carries `prev` (the previous record's hash) and `hash` (over
   its own canonical JSON). Editing, inserting or deleting a line breaks every hash after it.
2. **Seed derivation.** A record names the commit and the purpose its seeds were drawn for;
   the store recomputes them with `seed_for(sha, i, purpose)` and refuses the record if they
   differ. Choosing a favourable seed now requires changing the code, which changes the SHA,
   which changes the seed.
3. **Provenance shape.** `ci_run_url` must match a GitHub Actions run URL, and a record that
   claims CI must carry the commit the workflow ran on.

SG-06 verifies the whole chain rather than recomputing a function and comparing it with
itself, which is what the first version of that claim did.

## What we gave up

- Rewriting history is now a visible act. Regenerating the evidence after a schema change
  starts a new chain, and the commit that does it is in the git log where a reader can see it.
- The gate cannot prove a CI run exists. Nothing offline can. It checks the shape, and the
  renderer prints every record without one as "local run, not reproduced in CI", which is the
  honest label rather than a green tick.

## Alternatives considered

- **Signing the records.** Stronger, and it moves the problem to key custody: whoever holds
  the key can sign anything, and in a solo repository that is the same person.
- **Only trusting CI-produced records.** Tempting, and it would make the repository unusable
  by anyone who forks it and runs it locally, which is exactly the audience the refutation
  path is for.

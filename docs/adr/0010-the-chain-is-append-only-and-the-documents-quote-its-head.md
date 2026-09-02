# ADR 0010 - the chain is append-only, and the documents quote its head

**Status** accepted, 2026-09-02

## Context

ADR 0007 made the evidence chain hard to forge: hashes link the records, seeds are derived from
a commit rather than chosen, and a record claiming CI has to carry the run it claims. What it
did not say is what to do when the repository changes underneath a chain that is already
correct.

Round eighteen produced exactly that situation. The generator gained a ninth corrupt kind
(`beyond_bigint`), which redistributes how many records land under each quarantine reason. The
chain still verified, every hash still linked, and the documents still matched the chain -
**and the population they describe is one the code no longer produces.** `samegold check` was
green on a repository whose front page described a past.

That is not a small gap. The whole offer this repository makes to a reader is "clone it and
re-run the claim yourself". A reader who did that would get different per-reason counts from
the ones on the front page and would have no way to tell whether they had found a defect, a
different seed, or a different commit. Two numbers, no rule for choosing between them, is
worse than one number with a caveat.

## Decision

**One: the history is append-only. A stale figure is fixed by adding a measurement, never by
editing one.**

`evidence/history.jsonl` is never rewritten, reordered or truncated. Re-running a claim appends
a record; the older record stays, hashed, with its own commit on it. This is not a convention
that survives on care - `EvidenceStore.verify_chain` refuses an edited, inserted, reordered or
deleted record, and SG-06 runs it over the whole file. The property is the point: a chain
anybody may rewrite when the numbers become inconvenient records nothing at all.

The corollary is the one that takes discipline: **a number that is out of date is not a defect
in the chain.** The temptation, when a document says 6 and the code now produces 7, is to fix
the 6. The answer is to run the claim and let the head move.

**Two: the documents quote the HEAD of the chain, and name it.**

`EvidenceStore.latest()` returns the most recent record per claim, `render_readme` fills every
anchored number from it, and `check_readme` fails if a document and that record disagree. So a
figure in a document is never hand-written and never chosen: it is the last measurement of that
claim, whatever it says.

Since round eighteen the provenance column also **names the commit** that produced it, and the
tree hash as well when the run was on an uncommitted tree - because a commit that does not
describe the code that ran is not provenance, it is a timestamp. A reader comparing the front
page with their own `make evidence` can now see, in the table, whether they are looking at the
same version of the repository.

**Three: regenerate in the same commit as the change, or say the figures are behind.**

A change to the generator, the contract, or a claim's method changes the population. The
obligation is to re-run the claims and commit the new records; where that cannot happen in the
same commit - SG-07 needs a JVM and fifteen minutes, and the Databricks lane needs a workspace
nobody has for free - the commit message says which claims are behind and why, and the
provenance column shows their older commit.

**Four: a change to the RECORD FORMAT is the one case that starts a new chain, and it is a
separate, announced commit.**

ADR 0007 already allows this and it is the only exception. Old records cannot be rehashed under
a new schema without rewriting them, which is the thing rule one forbids. So the chain is
closed and a new one begun, in a commit that does nothing else and says so in its subject line,
with the old file kept in git history where a reader can still verify it end to end. Adding a
FIELD is not a format change and must not start a new chain.

## What we gave up

- **The front page can lag.** A commit that changes the generator and does not re-run the
  claims leaves figures that are honest about their commit and older than `HEAD`. We prefer a
  visibly old number to a freshly typed one, and the provenance column is what makes the age
  visible rather than a matter of trust.
- **`samegold check` still passes on a lagging document.** It compares the documents with the
  chain, not the chain with the code, and it cannot do the second: knowing that a figure would
  come out differently today requires running the claim, which is the fifteen minutes the check
  exists to avoid. This is a stated limit rather than a fixable one, and it is why rule three
  puts the obligation on the commit that changes the population.
- **Chains get long.** 149 records and growing, most of them superseded. That is what an
  append-only log costs, and it is cheap: the file is text, the verification is linear, and the
  superseded records are the only evidence that the current number was not always the number.

## Alternatives considered

- **Rewrite the chain on every regeneration.** One record per claim, always current, nothing
  stale. It also makes every past measurement unfalsifiable-by-inspection and removes the only
  defence against quietly re-running until a number improves - which ADR 0007 exists to
  prevent, and which is exactly what a solo repository needs protecting from.
- **Refuse to render from a record older than HEAD.** Tempting, and it would turn every ordinary
  commit into a fifteen-minute evidence run or a red gate. It also cannot distinguish a commit
  that changed the population from one that changed a docstring, so it would train people to
  bypass it.
- **Put the figures in the documents by hand and check them in review.** This is what the
  repository did before ADR 0007, and an adversarial reviewer forged the lot in one sitting.

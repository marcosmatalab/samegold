# ADR 0017 - the demo has a fixed seed, and its block is rendered by running it

**Status** accepted, 2026-09-23

## Context

The README's demo block said it "always matches what the program prints on the current commit".
At `9bd66c1` the program printed 802 events, 292 files, 157 581,20 EUR and -3,03 %; the block
showed 772 events, 150 658,58 EUR and -2,28 %, which is the output for the seed of commit
`d69164100`.

Two mechanisms were fighting. The demo drew its seed from the commit sha, like every claim, so
its output moved on every commit. The block was rendered from SG-00's record, which is written by
an evidence sweep on some commit and then stays put. Between two sweeps the block was always an
older commit's output. It also carried the run's duration, which is different on every run, so
not even a same-commit comparison could have matched it byte for byte.

## Decision

**The demo draws a fixed seed**, `claims.DEMO_SEED`. It is the seed the block showed when it was
last right, and it still produces that output on the current code.

**`samegold readme` renders the block by running the demo**, through the same function that
`samegold demo` prints, `cli.demo_output`. **`samegold check` runs it again** and fails if the
block differs by a byte.

**The duration goes to stderr**, so stdout is exactly the block. `make demo` is silent about its
own recipe, so what it prints is what the program prints.

`tests/fast/test_demo_block.py` runs the real command and compares its stdout with the block on
both front pages.

## Alternatives rejected

- **Keep the commit-derived seed and render the block on every commit.** A committed file cannot
  contain the output for its own commit, because writing it changes the sha that picks the seed.
  The block would be one commit stale by construction, which is the defect.
- **Keep rendering from SG-00's record and soften the sentence.** That is declaring the defect
  instead of fixing it.

## Consequences

**The demo's population is chosen, and this ADR says so.** Seeds derived from the commit exist so
that nobody can pick a favourable population for a CLAIM, and the claims keep them: SG-00 to SG-09,
`make evidence` and `make refute` are unchanged. The demo proves nothing and was never evidence.
It is the first thing a reader runs, and an illustration that changes on every commit cannot be
quoted.

**A change to the generator or to the demo's wording now fails `samegold check`** until
`make readme` re-renders the block. That is the check doing its job.

**`samegold check` now runs the demo**, which costs well under a second on Linux and a few seconds
on Windows.

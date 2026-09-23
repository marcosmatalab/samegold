# The decisions

Each file is one decision, with the alternative it rejected and why. They are kept because the
alternative is the part that is hard to reconstruct later: a decision reads as obvious once it
is made, and the record of what it was competing with is what makes it arguable.

This index exists because seven of them were reachable from nothing. Measured on 22 September
2026 across the 33 tracked markdown files, ADRs 0001, 0003, 0004, 0005, 0007, 0008 and 0009 -
2 398 words - were linked by no document in the repository, including this directory, which had
no index at all. The front page links `docs/adr/`, GitHub renders this file when you open it,
and that is the whole fix.

One line each, saying what was decided rather than what the file is about.

| | decision | what it rejected |
|---|---|---|
| [0001](0001-a-second-implementation-instead-of-more-tests.md) | a second implementation instead of more assertions | more unit tests, blind in the same places as the code; and the data-quality tools, which check shape and never compute the number twice |
| [0002](0002-version-pinning.md) | one pinned version combination, in one place | version agility. Moving to a new Spark means changing one constant and re-running the Delta lane, which is the point |
| [0003](0003-watermark-is-not-the-return-window.md) | the 45-day return window is not a watermark | a "reordering the input does not change the output" invariant, which is false under any watermark; what is claimed is the weaker true version |
| [0004](0004-what-is-shared-between-implementations.md) | what the two implementations share, and what they must not | sharing the derivations: they share the CONTRACT and duplicate every computation, at the price of divergences that are pure noise |
| [0005](0005-adaptive-execution-stays-on.md) | adaptive execution stays on; the digest absorbs it | turning AQE off, which tunes the experiment to fit the claim; and byte-level file comparison, stronger-sounding and much weaker |
| [0006](0006-mutants-are-generated-not-planted.md) | mutants are generated; the specification ones are not, and say so | one family. Generated mutants alone cannot express "a return belongs to the month of the sale"; hand-written ones alone measure the author's imagination. Six specification mutants are labelled as such |
| [0007](0007-the-evidence-gate.md) | the evidence is hash-chained and its seeds are derived, not chosen | signing the records, which moves the problem to key custody; and trusting only CI records, which would break every fork |
| [0008](0008-cost-is-measured-in-files-and-bytes.md) | the cost lab measures files and bytes, never seconds | any statement about latency, and any statement about money |
| [0009](0009-governance-in-code.md) | the privacy controls run in code, because the platform cannot enforce them | enforcement. A control in the pipeline can be bypassed by anyone who writes another pipeline; this says so rather than implying otherwise |
| [0010](0010-the-chain-is-append-only-and-the-documents-quote-its-head.md) | the chain is append-only, and the documents quote its head | a front page that is never out of date. A visibly old number is preferred to a freshly typed one, and the provenance column is what makes the age visible |
| [0011](0011-the-gate-recomputes-the-record.md) | the gate recomputes the record, it does not only validate it | signing the records - the forgery was written by this repository's own hasher, so a signature would have been valid - and relying on the build attestation, which answers "who" when the question was "is it true" |
| [0012](0012-what-this-repository-is-mostly-made-of.md) | the repository says what it is mostly made of, on its front page | rebalancing by writing more pipeline until it is 50/50: two hundred hours for a worse repository, since the harness is the differentiated part |
| [0013](0013-the-delta-lane-fails-when-it-cannot-verify.md) | the Delta lane fails when it cannot reach the jars, instead of skipping | vendoring the Delta jars: about 30 MB of binaries in a 4.7 MB repository, stale the moment the version moves |
| [0014](0014-the-evidence-pull-request-merges-itself.md) | the evidence pull request is merged by the job that opened it | `--auto`, measured impossible here, and a required-check ruleset, measured worse |
| [0015](0015-the-version-has-one-source.md) | the version has one source, `pyproject.toml`, and the changelog is held to it by a test | deriving the version from git tags, which makes the number depend on how the repository was fetched |
| [0016](0016-every-front-page-figure-has-a-source.md) | every figure on a front page is an anchor, a generated block or an identifier, `samegold check` compares all of them, and the Databricks record is pinned by digest | chaining the Databricks record, which its own `chain.why` argues against; and pinning raw bytes, which fails on a line-ending change |
| [0017](0017-the-demo-has-a-fixed-seed.md) | the demo draws a fixed seed and its README block is rendered by running it, so `samegold check` can compare it byte for byte | a commit-derived seed for the demo, which makes a committed transcript one commit stale by construction |

All of them are in `Accepted` status and none has ever been superseded by another file, so
this index does not describe a supersession policy it has not used. What has happened once is
a reversal INSIDE a file: 0014 describes a mechanism that replaced the one its own first
version described, and its status line says so - `accepted, 2026-09-22 (superseding the
version of this ADR that described --auto)`. The rejected alternative in the last row is that
first version, and it is in the file rather than in a deleted one.

And an ADR cannot describe something the tree does not do: `samegold.evidence.prose` fails the
fast lane when an ADR in `Accepted` status quotes, in the sections where it asserts, a command
the implementation does not run. That rule was written after 0014 sat on the default branch for
a day describing an auto-merge that was in no workflow, and 0011 turned out to be lying beside
it. [`../findings/the-gate-found-three-things-and-two-were-its-own.md`](../findings/the-gate-found-three-things-and-two-were-its-own.md)
is the write-up.

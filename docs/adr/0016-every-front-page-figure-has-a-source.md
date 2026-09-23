# ADR 0016 - every figure on a front page has a source that `samegold check` compares

**Status** accepted, 2026-09-23

## Context

`samegold check` compared the `sg:` anchors with the evidence chain and nothing else. On
23 September 2026 the Databricks row of the README was edited by hand ("1883 events") and the
check passed; one fast-lane test caught it. With nothing watching them at all, the same page
carried:

- the gross figures of January's first two closed versions, typed beside a rendered third;
- the return window;
- the GIF's acceleration and its real duration;
- the package versions in the badges;
- a skew percentage.

`evidence/databricks/SG-DBX-01.json` itself could be edited, the documents re-rendered from the
edit, and every gate would have agreed with it.

That record is outside the chain on purpose, and its `chain.why` field is right: it was
produced inside a workspace, it derives no seed from a commit, and nobody with a clone can
recompute it. A link for it would be the one unverifiable link in a chain whose value is that
every link verifies.

## Decision

**A figure on a front page is one of four things, or it is a finding.**

1. An `sg:` anchor, rendered from the chain.
2. A `dbx:` anchor, rendered from the Databricks record.
3. A `repo:` anchor, rendered from the one file that defines the fact: the contract's
   `RETURN_WINDOW_DAYS`, and the GIF's own frame delays times the Makefile's `setpts=PTS/N`.
4. A generated block: the claims table, the demo, and the stack badges read from
   `pyproject.toml`.

Numbers that NAME something are not figures: claim ids, ADR numbers, "Type 2", the licence.
They are a closed list in `samegold.evidence.front_page.IDENTIFIERS`. Anything else that looks
like a number in the prose of either front page is reported with its file and line.

**`samegold check` compares all of it.** Every `dbx:` anchor in every document that quotes the
record is checked against the record, every `repo:` anchor against its source, and the stack
block against `pyproject.toml`.

**The Databricks files are pinned, not chained.** `PINNED_DIGESTS` in
`samegold.evidence.databricks_doc` holds a SHA-256 of the canonical JSON of the record and of
the capture beside it. `samegold check` recomputes both. The pin lives in source that the fast
lane tests and type-checks. Editing the record therefore means editing that line too, in the
same diff, where it is visible. The chain does not change length.

The digest is taken over the parsed content, with sorted keys and no insignificant whitespace.
A checkout that rewrites line endings, or a formatter that re-indents the file, does not move
it; any change to a value does.

## Alternatives rejected

- **Chain the Databricks record.** This is the one fix the record's own `chain.why` argues
  against, and the argument holds.
- **Keep the figures and check them in a fast-lane test only.** That was the state that let the
  edit through `samegold check`, and a test that runs in one command while the published
  statement is checked by another is two gates that each assume the other.
- **Pin the raw bytes.** Stricter, and it fails on a line-ending conversion that changes no
  value. That is the kind of false alarm that teaches people to update a pin without looking.

## Consequences

**Fetching a new Databricks run means moving the pin.** `samegold check` fails naming the file
and printing the digest it measured, so the pin moves in the same commit as the record, in a
diff a reviewer reads.

**A new figure on a front page has to arrive with its source.** If it has none, it is removed
or turned into prose. That is why the skew percentage and the alt-text dates went.

**The versions left the stack table.** The badges above the table carry them, rendered from
`pyproject.toml`, so the table would only have been a second, unchecked copy.

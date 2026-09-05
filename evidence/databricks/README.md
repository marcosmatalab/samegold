# Databricks evidence, kept apart from the chain

Everything in `evidence/runs/` and `evidence/history.jsonl` is hash-chained, derives its seeds
from a commit of this repository, and can be recomputed by anyone with a clone. That is the
whole value of that chain: a single record cannot be edited, inserted, reordered or removed
without rewriting everything after it, and every record names a real commit.

The files in this directory are none of those things, so they are not in it.

| | the OSS chain | this directory |
|---|---|---|
| produced by | `samegold evidence` on a commit | a job run inside a Databricks workspace |
| reproducible by a reader | yes, `make refute SEED=...` | no: it needs an account and a deploy |
| seeds derived from a commit sha | yes | no |
| hash-chained | yes | no |
| what it can be trusted for | as much as the code you can read | as much as you trust the person who ran it |

Appending `SG-DBX-01` to `evidence/history.jsonl` would put one unverifiable link into a chain
whose only property is that every link is verifiable. The claim id says so too - it is
`SG-DBX-01`, not a two-digit `SG-nn`, and `samegold check` does not read this directory.

## What is here after a run

- `SG-DBX-01.json` - written by `databricks/src/publish_evidence.py` **inside the workspace**
  and copied down verbatim. Expectation pass and fail counts per rule for the last pipeline
  update, row counts per table, the state of that update, the shape of the AUTO CDC dimension,
  and the rows of the signed-off close. Its `incomplete` list names any section that could not
  be read; a section that failed is a hole, not a zero.
- `dim_customer_scd2.json` - written by the same notebook, in the same run, from the same
  table the record's six dimension aggregates were read from. The workspace's Type 2 dimension
  row by row, under a `provenance` header naming the update that produced it. Half of a
  cross-runtime comparison has to come FROM the runtime: `tests/fast/test_databricks_dimension_parity.py`
  compares these rows against the OSS lane's hand-written `MERGE` on the same seed.
- `fetch.json` - written by `scripts/databricks_run.sh` **on the machine that deployed**: the
  commit it deployed from, whether that tree was dirty, the CLI version and the timestamp.
  Kept in a separate file on purpose. Nothing a laptop asserts about a deploy belongs in the
  file the workspace produced.
- `SG-DBX-01.<label>.json`, if any - a run kept beside the canonical record rather than over
  it. See the section below for which runs those are and why they are not interchangeable.

## Which record is canonical, and which runs may replace it

`SG-DBX-01.json` is **the** record: every `<!--dbx:...-->` anchor in every document in this
repository is rendered from it, and `tests/fast/test_databricks_dimension_parity.py` and
`tests/fast/test_databricks_close_parity.py` compare the OSS lane against it. There is one of
it, and a fetch overwrites it.

So not every run may replace it, and the rule is a rule rather than a judgement someone makes
again each time:

**A run replaces `SG-DBX-01.json` only if its record can answer every anchor the documents
require.** `tests/fast/test_databricks_bundle.py::test_the_committed_record_answers_every_anchor_the_documents_require`
is that sentence as a check, so committing the wrong record fails rather than being noticed.

Two runs that cannot, and they are not hypothetical - they are the next two:

- **a run that ingests nothing.** The lane starts with the landing volume already ingested, the
  update processes no rows, and no `flow_progress` event carrying data quality is produced: the
  record's `expectations` comes back empty. Rendering from it would replace a measured table
  with `NOT RUN`, which is a regression whose diff reads like an update.
- **a run that failed on purpose.** The record of a deliberately failed verification is worth
  keeping - it is the evidence that `run_if: ALL_DONE` keeps the evidence alive through a
  failure, and that the record names the hole instead of publishing zeros. It is exactly not the
  description of the lane that the documents should render from.

Both are kept, neither replaces anything:

    scripts/databricks_run.sh fetch run-1
    scripts/databricks_run.sh fetch run-2-failed

writes `SG-DBX-01.<label>.json` beside the canonical record, with its own capture and its own
`fetch.<label>.json`, and leaves `SG-DBX-01.json` alone. Nothing renders or compares a labelled
record; what it is for is being read, and being cited by run id in
`docs/predictions-2026-09-05.md` when the prediction is scored. A labelled record that cannot
name its own job run is refused by
`tests/fast/test_databricks_bundle.py::test_a_record_kept_beside_the_canonical_one_can_still_name_its_run`
- the reason to keep one at all is that it describes a run somebody can look up.

## Why the capture carries a header

It was exported by hand for one run - a query pasted into the workspace, the result saved -
which produced a file that could not say which run it came from. That is a comparison waiting
to go quietly wrong: a later run replaces `SG-DBX-01.json`, nothing replaces the capture, and
the row-by-row comparison goes on passing against rows the workspace no longer holds. Green,
against a dataset that no longer describes the system.

So the run writes it, and the header names the update. The tie is checked two ways, and only
one of them needs the header:

- `provenance.update_id` must equal the record's `update[0].update_id`. Both are the same read
  of the same event log in the same session, so they agree when the two files come from one run
  and differ the moment they do not.
- the record's six dimension aggregates must RECOMPUTE from the captured rows. Those are two
  different queries over one table in one run, so this is not circular - and it holds without
  any header at all, which is what protects the one capture whose header was typed rather than
  measured. `first_start` and `last_start` are the sharp ones: no two populations agree on a
  MIN and a MAX by accident.

`provenance.measured_in_the_workspace` says which kind of header it is. The capture committed
on 3 September 2026 says `false`, because it predates the run writing it; the next fetch
replaces it with one that says `true` and carries the job run, the task run and the workspace's
own clock - three things a header written afterwards cannot have.

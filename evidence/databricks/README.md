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
- `fetch.json` - written by `scripts/databricks_run.sh` **on the machine that deployed**: the
  commit it deployed from, whether that tree was dirty, the CLI version and the timestamp.
  Kept in a separate file on purpose. Nothing a laptop asserts about a deploy belongs in the
  file the workspace produced.

Neither file is here yet. `docs/databricks-run.md` is written with the run's numbers left as
marked blanks, and it stays that way until a run fills them in.

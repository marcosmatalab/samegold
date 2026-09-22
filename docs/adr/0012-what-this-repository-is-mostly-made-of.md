# ADR 0012 - the repository says what it is mostly made of, on its front page

**Status** accepted, 2026-09-22

## Context

Measured, and the classification is in `src/samegold/evidence/lane_split.py` so it can be
argued with rather than inferred:

| | lines |
|---|---|
| `src/samegold/pipelines` + `src/samegold/ingest` (PySpark) | 1 197 |
| `databricks/src/*.py` (the notebooks the workspace runs) | 2 205 |
| `databricks/**` yml, json and sql (the bundle) | 1 031 |
| `pipelines/` (the declarative pipeline) | 92 |
| `tests/spark` + `tests/delta` | 3 042 |
| **Spark, Delta and Databricks** | **7 567** |
| the rest of `src/samegold` (pure Python and DuckDB) | 11 686 |
| `tests/fast` | 10 092 |
| **the harness** | **21 778** |

About a quarter against three quarters. Inside the fast lane the same shape: of its tests, 175
exercise the pipeline and the rest check this repository's own claims about itself.

A reviewer who counts finds that in ten minutes. The question this ADR answers is what the
front page should have said before they did.

The figures above are already out of date, which is the second half of the context. Writing
them into this document is the thing this repository exists to argue against, and they moved
during the round that wrote the section: it added three test modules to the harness and took
the split from 27.9/72.1 to 25.4/74.6. The ones on the README are anchors, measured by
`make evidence`; the ones in this table are a snapshot of the day the decision was made, which
is what an ADR is for.

## Decision

**The README says the ratio, with measured figures, in a section of its own, and defends it.**

The headline stops promising a pipeline and starts promising what the repository actually is:
"How you prove a data pipeline does what it says." The pipeline is named in the next sentence,
because it is the subject and it is real; the harness is named as the work, because it is.

**And the harness stays in this repository.** It is not extracted to a package of its own.

## Why

**On saying the ratio.** If the front page promises a pipeline and a reviewer counts 175
pipeline tests out of 596, the repository loses for being misleading. If the front page
promises verifiability applied to a Databricks pipeline and the reviewer counts exactly that,
it wins for being accurate. The figure is the same either way; the only variable is whether the
reader learns it from the author or finds it themselves. There is also a job argument: for a
platform or data-quality role the harness **is** the skill being bought, and for a data
engineering role the Databricks lane has to be visible and executed, which is why it has a
section of its own with its limits in the first line.

**On keeping the harness here.** Extracted, it becomes a library with no users, which is a
worse signal than a large repository. And here it has a SUBJECT: it verifies a bitemporal close
on Delta. Outside, it verifies nothing. The demonstration that the harness is worth anything is
precisely that it found a 2.767e19-cent bug in this pipeline, and that story needs both halves
in one place.

## Alternatives rejected

**Rebalance by writing more pipeline until it is 50/50.** Two hundred hours or more, and the
result would be a worse repository: the harness is the differentiated part, and a month-end
close is something anybody can write. The ratio is not an accident to be corrected.

**Say nothing and let the reader work it out.** They do work it out, and then every other
sentence on the page is read in the light of the one that was quiet.

**Extract `samegold-evidence` to PyPI.** Eight hours, a second repository with no stars, and
the narrative argument breaks. If somebody ever asks for the library, it can be extracted then,
with a real user behind it.

**Type the figures into the README.** Rejected on this repository's own grounds, and it would
have been wrong within the same round: they moved by two points while the section was being
written. They are anchors, measured by SG-00 out of a classification that lives in `src/`.

## Consequences

The repository wins the platform reviewer and the data-quality reviewer, and loses the one
looking only for years of production PySpark. That is accepted: the second is not won by this
repository and optimising for them would cost the first.

`scripts/` and the markdown are counted on NEITHER side, which is a decision the code says out
loud: `scripts/databricks_run.sh` is 1 212 lines of bash that would move the ratio by about
four points on its own, and which side it belongs to is a real argument nobody has had. A
figure that depends on an unargued call is a figure with a thumb on it.

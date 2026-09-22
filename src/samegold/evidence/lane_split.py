"""Which half of the fast lane each test file belongs to, and the sum the documents publish.

WHY THIS IS IN `src/` AND NOT IN THE TEST THAT ASSERTS IT. A reviewer counted the fast lane and
asked the obvious question: how much of it tests the PIPELINE, and how much of it tests the
repository's own paperwork? The answer was published in CLAIMS.md and TYPED there - "389 of them
check the repository" - beside figures that render through evidence anchors. It drifted the way
every typed number in this repository has drifted: the next commit that added tests moved the
sum and the sentence stayed where it was.

So the classification lives here, where the SG-00 collector can import it and write the sum into
the evidence record, and the documents render that sum through an anchor like every other figure.
The two halves are deliberately different in kind:

  * the CLASSIFICATION is a judgement, and it stays one. A new test file belongs to neither set
    until somebody decides, and `tests/fast/test_prose_gate.py` fails the run until somebody does.
    That is a decision this module cannot make and does not try to;
  * the SUM is a measurement, and it stops being typed. It comes out of pytest's own collection
    in the run that writes the record.

THE CRITERION, written down so the split can be argued with:

  REPOSITORY - the test's verdict is decided by this repository's own text, configuration,
  structure, scripts or evidence plumbing. It reads or executes something committed here and
  compares it against something else committed here. It would survive, unchanged in kind, if the
  domain were payroll instead of retail revenue.

  DOMAIN - the test's verdict is decided by RUNNING a computation over data: the generator, the
  close, the dimension, the digests, the invariants, the mutation engine, the parity comparisons
  against a reference.

Two calls worth naming because a reader may disagree:

  * `test_seeds` is REPOSITORY. Seeds are derived from the commit sha and the evidence store
    refuses records whose seeds were chosen; that is provenance plumbing, not arithmetic about
    revenue.
  * `test_review_regressions` is DOMAIN as a file, and it is the one genuinely mixed one: roughly
    a third of its tests are regressions in documents and lane declarations rather than in the
    close. Moving those would shift the split by about ten tests in the repository direction. It
    is counted whole, on the side its majority sits.

Two more, added on 22 September 2026, both REPOSITORY, and the reasoning is the same one each
time - none of them would change if the domain were payroll:

  * `test_reproduce` is about the evidence gate that recomputes a record (ADR 0011). It runs no
    close and reads no data: its subject is whether a published number was measured;
  * `test_faults` is the one worth arguing about. The crash harness kills a run of the SILVER
    stage, which is domain machinery, so a reader could reasonably put it on the other side.
    It is REPOSITORY because what these tests decide is the harness's own accounting - a missed
    injection is not a pass, the bound is withheld below three trials, the schedule cannot ask
    for a batch that does not exist. The part that computes over data is `worker.run`, which
    needs a JVM and is not in this lane at all.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPOSITORY_TESTS = {
    "test_architecture.py",
    "test_contract_documents.py",
    "test_databricks_bundle.py",
    "test_databricks_catalog_step.py",
    # Which CLI CI installs against which CLI produced the records: paperwork, not the close.
    "test_databricks_cli_pin.py",
    # Whether a deploy came from a clean tree is paperwork about this repository.
    "test_deploy_tree_dirty.py",
    "test_documentation.py",
    "test_evidence_gate.py",
    "test_faults.py",
    "test_preflight.py",
    "test_prose_gate.py",
    # The two front pages agreeing is paperwork about this repository, not about the close.
    "test_readme_parity.py",
    "test_reproduce.py",
    "test_seeds.py",
}
DOMAIN_TESTS = {
    "test_databricks_close_parity.py",
    "test_databricks_dimension_parity.py",
    "test_digest.py",
    "test_generator.py",
    "test_governance.py",
    "test_invariants.py",
    "test_late_arrivals.py",
    "test_money.py",
    "test_mutation.py",
    "test_review_regressions.py",
    "test_rules.py",
    "test_scd2.py",
    "test_serve.py",
    "test_timezone.py",
    "test_verdict.py",
}


def collected_by_file(repo: Path) -> dict[str, int]:
    """How many tests each file contributes, from pytest's own collection.

    COUNTED BY COLLECTING, never by counting `def test_`: more than half of this suite is
    parametrised, and `test_architecture.py` alone has seven functions and a hundred and thirteen
    tests. A split computed from function definitions would be describing a different suite.
    """
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "tests/fast",
            "--collect-only",
            "-q",
            "-p",
            "no:cacheprovider",
        ],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    counts: dict[str, int] = {}
    for line in result.stdout.splitlines():
        if "::" not in line:
            continue
        name = Path(line.split("::", 1)[0]).name
        counts[name] = counts.get(name, 0) + 1
    if not counts:
        raise RuntimeError(
            "pytest collected nothing from tests/fast, so the split cannot be measured:\n"
            + result.stdout[-2000:]
            + result.stderr[-2000:]
        )
    return counts


def split(repo: Path) -> tuple[int, int, list[str]]:
    """(repository tests, domain tests, files in neither class).

    The third element is the one that must stay empty. A file nobody has classified is not
    silently assigned a side - it fails the run, because the published sum is this addition and
    an unclassified file would change it without anybody deciding anything.
    """
    counts = collected_by_file(repo)
    unclassified = sorted(set(counts) - REPOSITORY_TESTS - DOMAIN_TESTS)
    repository = sum(n for name, n in counts.items() if name in REPOSITORY_TESTS)
    domain = sum(n for name, n in counts.items() if name in DOMAIN_TESTS)
    return repository, domain, unclassified


# ---------------------------------------------------------------- the same question, in lines
#
# The README says in its own section that 26% of this repository is Spark, Delta and Databricks
# and 74% is the harness that tries to break it. That is the sentence a reviewer is most likely
# to check by counting, and it was going to be a hand-typed pair of numbers in the one
# repository whose thesis is that those rot. It rotted before it was even published: the round
# that wrote the section added three test modules to the harness and moved the ratio from
# 27.9/72.1 to what it is now.
#
# The classification is a JUDGEMENT, exactly like the one above, and it is written out here so
# it can be argued with rather than inferred from a number.

#: Spark, Delta, Databricks: the pipeline, the notebooks that run in a workspace, the bundle
#: that deploys them, and the tests that need a JVM to say anything.
PLATFORM_PATHS = (
    "src/samegold/pipelines",
    "src/samegold/ingest",
    "databricks",
    "pipelines",
    "tests/spark",
    "tests/delta",
)

#: Everything else under `src/` and `tests/`: the contract, the generator, the DuckDB
#: reference, the mutation engine, the evidence chain, the crash harness, and the fast lane.
HARNESS_PATHS = ("src/samegold", "tests/fast")

#: What counts as code. `scripts/` and the markdown are deliberately outside BOTH sides rather
#: than assigned to one: `scripts/databricks_run.sh` is 1 212 lines of bash that would move the
#: ratio by four points on its own, and which side it belongs to is a real argument. A figure
#: that depends on an unargued call is a figure with a thumb on it.
CODE_SUFFIXES = (".py", ".yml", ".yaml", ".json", ".sql")


def _code_files(repo: Path, roots: tuple[str, ...]) -> set[Path]:
    out: set[Path] = set()
    for root in roots:
        base = repo / root
        if not base.exists():
            continue
        for path in base.rglob("*"):
            if path.is_file() and path.suffix in CODE_SUFFIXES and "__pycache__" not in str(path):
                out.add(path.resolve())
    return out


def code_split(repo: Path) -> tuple[int, int]:
    """(platform lines, harness lines) over the files each side declares.

    A file under a platform path is platform even when it also sits under `src/samegold`, which
    is why the harness set is the difference rather than a second walk: `src/samegold/pipelines`
    is inside `src/samegold`, and counting it on both sides would publish a total larger than
    the tree.
    """
    platform = _code_files(repo, PLATFORM_PATHS)
    harness = _code_files(repo, HARNESS_PATHS) - platform

    def lines(paths: set[Path]) -> int:
        return sum(len(p.read_text(encoding="utf-8", errors="replace").splitlines()) for p in paths)

    return lines(platform), lines(harness)

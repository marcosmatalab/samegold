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
    "test_documentation.py",
    "test_evidence_gate.py",
    "test_preflight.py",
    "test_prose_gate.py",
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

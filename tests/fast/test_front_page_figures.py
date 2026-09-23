"""Every figure on the front pages is checked by `samegold check`, and editing one by hand fails it.

WHAT THIS EXISTS BECAUSE OF. On 23 September 2026 the Databricks row of the README was edited
by hand ("1883 events") and `samegold check` passed; only a fast-lane test noticed. Nothing at
all watched the gross figures of January's first two closed versions, the return window, the
two numbers beside the GIF, the versions in the badges, or the Databricks record itself - each
could be changed in an editor and every gate stayed green.

Each case below makes one such edit to a COPY of the files the check reads, and requires
`front_page_findings` - the function `samegold check` runs - to report it and to name the file
and line. Restoring the copy must bring it back to no findings. The last test runs the real
command against the real repository and shows the chain is read, never written.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from samegold import cli

REPO = Path(__file__).resolve().parents[2]

#: Every file `front_page_findings` reads.
READ = (
    "README.md",
    "README.es.md",
    "docs/databricks-run.md",
    "docs/databricks-run-evidence.md",
    "evidence/databricks/SG-DBX-01.json",
    "evidence/databricks/dim_customer_scd2.json",
    "docs/img/refute.gif",
    "Makefile",
    "pyproject.toml",
)


@pytest.fixture
def copy(tmp_path: Path) -> Path:
    for name in READ:
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / name, target)
    return tmp_path


def _edit(root: Path, name: str, before: str, after: str) -> None:
    path = root / name
    text = path.read_text(encoding="utf-8")
    assert before in text, f"{before!r} is not in {name}; the page moved under this test"
    path.write_text(text.replace(before, after, 1), encoding="utf-8", newline="\n")


# (what was edited, file, before, after, what the finding must name)
EDITS = (
    (
        "the Databricks events row, English",
        "README.md",
        "<!--dbx:rows.bronze_events-->1883<",
        "<!--dbx:rows.bronze_events-->1884<",
        "README.md:",
    ),
    (
        "the Databricks events row, Spanish",
        "README.es.md",
        "<!--dbx:rows.bronze_events-->1883<",
        "<!--dbx:rows.bronze_events-->1884<",
        "README.es.md:",
    ),
    (
        "January's first closed version",
        "README.md",
        "v0.gross_cents-->14 198 046<",
        "v0.gross_cents-->14 198 047<",
        "dbx:closed.2026_01.v0.gross_cents",
    ),
    (
        "January's second closed version",
        "README.es.md",
        "v1.gross_cents-->25 582 615<",
        "v1.gross_cents-->25 582 616<",
        "dbx:closed.2026_01.v1.gross_cents",
    ),
    (
        "January's third closed version",
        "README.md",
        "v2.gross_cents-->37 622 605<",
        "v2.gross_cents-->37 622 606<",
        "dbx:closed.2026_01.v2.gross_cents",
    ),
    (
        "the GIF's real duration",
        "README.md",
        "real_seconds-->73,1 s<",
        "real_seconds-->71,3 s<",
        "repo:gif.refute.real_seconds",
    ),
    (
        "the GIF's acceleration",
        "README.es.md",
        "speed-->4x<",
        "speed-->3x<",
        "repo:gif.refute.speed",
    ),
    (
        "the return window",
        "README.md",
        "return_window_days-->45<",
        "return_window_days-->60<",
        "repo:contract.return_window_days",
    ),
    (
        "a version in the stack badges",
        "README.md",
        "PySpark-4.2.0-",
        "PySpark-4.3.0-",
        "stack badges",
    ),
    (
        "a figure typed into the prose",
        "README.es.md",
        "Sin cuenta, sin credenciales",
        "Sin cuenta, en 7 minutos, sin credenciales",
        "hand-typed figure",
    ),
    (
        "an anchor removed, leaving its figure as plain text",
        "README.md",
        "<!--dbx:rows.bronze_events-->1883<!--/dbx-->",
        "1883",
        "hand-typed figure",
    ),
    (
        "the Databricks record itself",
        "evidence/databricks/SG-DBX-01.json",
        '"gross_cents": 25582615',
        '"gross_cents": 25582616',
        "evidence/databricks/SG-DBX-01.json: content digest",
    ),
    (
        "the SCD2 capture beside it",
        "evidence/databricks/dim_customer_scd2.json",
        '"job_run_id": "517089489320521"',
        '"job_run_id": "517089489320522"',
        "evidence/databricks/dim_customer_scd2.json: content digest",
    ),
)


@pytest.mark.evidence_dependent
def test_the_repository_as_committed_has_no_findings(copy: Path) -> None:
    assert cli.front_page_findings(copy) == []


@pytest.mark.evidence_dependent
@pytest.mark.parametrize(
    ("what", "name", "before", "after", "named"), EDITS, ids=[e[0] for e in EDITS]
)
def test_a_hand_edit_is_reported_and_named(
    copy: Path, what: str, name: str, before: str, after: str, named: str
) -> None:
    pristine = (copy / name).read_bytes()
    _edit(copy, name, before, after)
    findings = cli.front_page_findings(copy)
    assert findings, f"editing {what} by hand was not noticed"
    if named:
        assert any(named in finding for finding in findings), (what, findings)
    (copy / name).write_bytes(pristine)
    assert cli.front_page_findings(copy) == [], f"restoring {what} did not clear the finding"


@pytest.mark.evidence_dependent
def test_editing_the_json_without_touching_a_document_still_fails(copy: Path) -> None:
    """The pin, alone: a change to a value no document quotes is still a change to the record."""
    _edit(
        copy,
        "evidence/databricks/SG-DBX-01.json",
        '"returns_rejected_count": 22',
        '"returns_rejected_count": 23',
    )
    findings = cli.front_page_findings(copy)
    assert findings == [
        f for f in findings if f.startswith("evidence/databricks/SG-DBX-01.json: content digest")
    ]
    assert len(findings) == 1


@pytest.mark.evidence_dependent
def test_samegold_check_passes_and_leaves_the_chain_as_it_found_it() -> None:
    """The real command, on the real tree. The Databricks record stays out of the chain."""
    history = REPO / "evidence" / "history.jsonl"
    before = history.read_bytes()
    out = subprocess.run(
        [sys.executable, "-m", "samegold.cli", "check"],
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert history.read_bytes() == before
    assert b"SG-DBX-01" not in before, "the Databricks record must stay out of the chain"

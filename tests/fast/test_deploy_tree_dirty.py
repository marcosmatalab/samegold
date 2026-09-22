"""`tree_dirty` in the deploy script, checked by RUNNING the script's own awk program.

The rule lives twice: `samegold.generator.seeds._code_changes` in Python, and one `awk` line
in `scripts/databricks_run.sh`. Two copies of a rule are two rules until something compares
them, and this file is that something - for the case that made them diverge and for the case
that must survive the fix.

The awk program is EXTRACTED from the shell script rather than restated here. A test that
retyped the pattern would pass against a script that no longer contains it, which is the shape
`docs/findings/` already carries twice: a check whose name describes the right thing and whose
measurement describes something next to it.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from samegold.generator.seeds import _code_changes

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "scripts" / "databricks_run.sh"

#: The one line of `code_changes()` that decides. Captured, not copied.
AWK_LINE = re.compile(r"\|\s*awk\s+'(?P<program>\{ p = substr.*?\})'")

pytestmark = pytest.mark.skipif(
    shutil.which("awk") is None,
    reason="no awk on PATH, so the script's own program cannot be executed",
)

# (what git would print, what the rule must return). Porcelain is "XY path", and the deploy
# script slices at a fixed offset because it reads git's output directly - no upstream strip.
CASES: tuple[tuple[str, list[str]], ...] = (
    ("?? .databricks/", []),
    ("?? .databricks/bundle/free/terraform/bundle.tf.json", []),
    (" M evidence/history.jsonl", []),
    ("?? evidence/runs/SG-07.json", []),
    (" M src/samegold/cli.py", ["src/samegold/cli.py"]),
    ("?? src/samegold/new_module.py", ["src/samegold/new_module.py"]),
    # The one that matters most: a tool's scratch beside a real change must not hide it.
    (
        "?? .databricks/\n M src/samegold/cli.py",
        ["src/samegold/cli.py"],
    ),
)


def awk_program() -> str:
    found = AWK_LINE.search(SCRIPT.read_text(encoding="utf-8"))
    assert found, "code_changes() in scripts/databricks_run.sh no longer holds one awk program"
    return found.group("program")


def run_rule(porcelain: str) -> list[str]:
    done = subprocess.run(
        ["awk", awk_program()],
        input=porcelain + "\n",
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in done.stdout.splitlines() if line]


@pytest.mark.parametrize(("porcelain", "expected"), CASES, ids=[c[0][:40] for c in CASES])
def test_the_shell_rule(porcelain: str, expected: list[str]) -> None:
    assert run_rule(porcelain) == expected


@pytest.mark.parametrize(("porcelain", "expected"), CASES, ids=[c[0][:40] for c in CASES])
def test_the_python_rule_agrees_with_it(porcelain: str, expected: list[str]) -> None:
    """Same inputs, same answers. Two copies of a rule that disagree is the whole problem.

    They are not one function because one of them runs inside a shell script that must work on
    a machine with no Python of this repository installed. That is a real constraint and this
    is the price of it.
    """
    assert _code_changes(porcelain) == expected
    assert _code_changes(porcelain) == run_rule(porcelain)


def test_the_exclusion_is_not_left_to_gitignore() -> None:
    """`.databricks/` is ignored AND excluded, and the second one is not redundant.

    Adding it to `.gitignore` alone would fix the symptom: git stops listing it, so the rule
    stops seeing it. It would also make the correctness of a provenance field depend on a line
    in a file the rule does not name, in a repository whose whole argument is that a claim
    nobody re-measures is true by accident. Both are here, and this test is why.
    """
    assert ".databricks/" in (REPO / ".gitignore").read_text(encoding="utf-8")
    assert ".databricks" in awk_program()
    from samegold.generator.seeds import _OUTPUT_PREFIXES

    assert ".databricks/" in _OUTPUT_PREFIXES

"""What the front page says `make demo` prints is, byte for byte, what it prints on this commit.

WHAT THIS EXISTS BECAUSE OF. The README said its demo block "always matches what the program
prints on the current commit". At `cf7bf5a` the program printed 802 events, 292 files,
157 581,20 EUR and -3,03 %, and the block showed 772 events, 150 658,58 EUR and -2,28 % - the
output for the seed of commit `0305621dc`, copied into the evidence and from there onto the
page. The demo's seed was derived from the commit sha, so the block was stale on every commit
after the one it was rendered on, and nothing compared the two.

The fix is in the mechanism (ADR 0017): the demo draws a fixed seed, `samegold readme` renders
the block by RUNNING the demo, and `samegold check` runs it again and compares. This test is the
acceptance: it runs the real command the way a reader does and compares the bytes.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

BLOCK = re.compile(
    r"<!-- samegold:begin demo -->\n```text\n(?P<body>.*?)\n```\n<!-- samegold:end demo -->",
    re.DOTALL,
)


@pytest.fixture(scope="module")
def printed() -> str:
    out = subprocess.run(
        [sys.executable, "-m", "samegold.cli", "demo"],
        cwd=REPO,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
        check=False,
    )
    assert out.returncode == 0, out.stderr
    return out.stdout


@pytest.mark.parametrize("page", ["README.md", "README.es.md"])
def test_the_block_is_what_the_demo_prints(printed: str, page: str) -> None:
    text = (REPO / page).read_text(encoding="utf-8")
    match = BLOCK.search(text)
    assert match, f"{page} has no rendered demo block"
    assert printed == match.group("body") + "\n", (
        f"{page}'s demo block is not what `samegold demo` prints on this commit.\n"
        f"--- the page\n{match.group('body')}\n--- the program\n{printed}"
    )

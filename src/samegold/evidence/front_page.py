"""No figure on a front page that nothing can check.

WHAT THIS EXISTS BECAUSE OF. On 23 September 2026 the Databricks row of the README was edited
by hand ("1883 events") and `samegold check` passed; only a fast-lane test caught it. The same
page carried, with nothing behind them at all, the three gross figures of January's closed
versions, the return window, the GIF's acceleration and its real duration, the package pins in
the badges and a skew percentage. Every one of them could be changed in an editor and every gate
this repository has would have stayed green.

So a front page may hold a figure in exactly one of these forms, and `samegold check` enforces
it:

  * an `sg:` anchor, rendered from the evidence chain (`evidence.render`);
  * a `dbx:` anchor, rendered from the Databricks record, which is pinned by digest
    (`evidence.databricks_doc`);
  * a `repo:` anchor, rendered from the one place in the repository that defines the fact: the
    contract for the return window, the GIF's own frame delays and the Makefile's speed-up for
    the recording (this module);
  * a generated block: the claims table, the demo transcript, the stack badges read from
    `pyproject.toml` (this module);
  * an IDENTIFIER, which names something rather than measuring it: a claim id, an ADR number,
    "Type 2", a licence. `IDENTIFIERS` below is the closed list, and anything else that looks
    like a number is reported as a hand-typed figure by `hand_typed_figures`.

The evidence layer may not import the domain (tests/fast/test_architecture.py), so the FACTS
are collected by the CLI and handed in; this module only renders and compares text.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: A repository fact, rendered from the file that defines it. Group 1 is the name.
REPO_ANCHOR = re.compile(r"<!--repo:([\w.]+)-->(.*?)<!--/repo-->", re.DOTALL)

STACK_BEGIN = "<!-- samegold:begin stack -->"
STACK_END = "<!-- samegold:end stack -->"

#: The speed-up `make gif` applies to the recording, as the Makefile writes it.
SETPTS = re.compile(r"setpts=PTS/(\d+)")


@dataclass(frozen=True)
class Finding:
    """One place where a front page states something nothing checks, or states it wrong."""

    where: str
    kind: str
    detail: str

    def __str__(self) -> str:
        return f"{self.where}: {self.kind}: {self.detail}"


# ------------------------------------------------------------------ repo: anchors


def render_repo_facts(text: str, facts: dict[str, str]) -> tuple[str, list[str]]:
    """Fill every `repo:` anchor from `facts`; return the text and the names left unanswered."""
    unknown: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in facts:
            unknown.append(name)
            return f"<!--repo:{name}-->UNKNOWN FACT<!--/repo-->"
        return f"<!--repo:{name}-->{facts[name]}<!--/repo-->"

    return REPO_ANCHOR.sub(replace, text), unknown


def gif_playback_seconds(raw: bytes) -> float:
    """How long a GIF plays, from the delay in each frame's Graphic Control Extension.

    A GCE is 0x21 0xF9 0x04, four bytes of body, then a 0x00 terminator, and then the image or
    another extension. Requiring that shape keeps the scan from matching the same three bytes
    inside compressed image data. Delays are in hundredths of a second.
    """
    delays: list[int] = []
    at = 0
    while True:
        at = raw.find(b"\x21\xf9\x04", at)
        if at == -1:
            break
        tail = raw[at + 7 : at + 9]
        if len(tail) == 2 and tail[0] == 0 and tail[1] in (0x2C, 0x21):
            delays.append(int.from_bytes(raw[at + 4 : at + 6], "little"))
        at += 3
    if not delays:
        raise ValueError("no frame delays found; this is not a GIF this function understands")
    return sum(delays) / 100


def gif_speedup(makefile: str) -> int:
    """The factor `make gif` divides the presentation timestamps by: `setpts=PTS/4` is 4."""
    found = SETPTS.findall(makefile)
    if len(found) != 1:
        raise ValueError(f"expected one `setpts=PTS/N` in the Makefile, found {len(found)}")
    return int(found[0])


def recording_facts(gif: bytes, makefile: str) -> dict[str, str]:
    """The two numbers printed beside the GIF, derived from the GIF and the Makefile.

    The real duration is the playback times the speed-up, written the way the page writes a
    decimal - `73,1 s` - so the rendered value is the sentence's own form.
    """
    speed = gif_speedup(makefile)
    real = gif_playback_seconds(gif) * speed
    return {
        "gif.refute.speed": f"{speed}x",
        "gif.refute.real_seconds": f"{real:.1f}".replace(".", ",") + " s",
    }


# ------------------------------------------------------------------ the stack badges


def render_stack_block(pins: dict[str, str]) -> str:
    """The technology badges, with every version read from `pyproject.toml`.

    `pins` carries `python` (the floor of `requires-python`), `pyspark` and `delta-spark` (the
    exact pins of the Spark extra). The badges that state no version are part of the block too,
    so the row is one generated unit rather than half typed and half rendered.
    """
    python, pyspark, delta = pins["python"], pins["pyspark"], pins["delta-spark"]
    badges = [
        f"![Python](https://img.shields.io/badge/Python-{python}%2B-3776AB?logo=python&logoColor=white)",
        f"![PySpark](https://img.shields.io/badge/PySpark-{pyspark}-E25A1C?logo=apachespark&logoColor=white)",
        f"![Delta Lake](https://img.shields.io/badge/Delta%20Lake-{delta}-00ADD4)",
        "![Databricks](https://img.shields.io/badge/Databricks-Asset%20Bundles-FF3621?logo=databricks&logoColor=white)",
        "![DuckDB](https://img.shields.io/badge/DuckDB-reference%20engine-FFF000?logo=duckdb&logoColor=black)",
        "![mypy](https://img.shields.io/badge/mypy-strict-2A6DB2)",
        "![ruff](https://img.shields.io/badge/lint-ruff-D7FF64?logo=ruff&logoColor=black)",
    ]
    return STACK_BEGIN + "\n" + "\n".join(badges) + "\n" + STACK_END


def render_stack(text: str, pins: dict[str, str]) -> str:
    if STACK_BEGIN not in text or STACK_END not in text:
        return text
    start, rest = text.split(STACK_BEGIN, 1)
    _, tail = rest.split(STACK_END, 1)
    return start + render_stack_block(pins) + tail


# ------------------------------------------------------------------ hand-typed figures

#: Everything that is not prose: the parts of a front page a figure may legitimately sit in
#: because something else renders or compares them. Removed before looking for numbers.
_GENERATED = re.compile(r"<!--\s*samegold:begin (\w+)\s*-->.*?<!--\s*samegold:end \1\s*-->", re.S)
_ANCHORS = re.compile(r"<!--(sg|dbx|repo):[\w.\-]+-->.*?<!--/(?:sg|dbx|repo)-->", re.S)
_FENCE = re.compile(r"^```.*?^```", re.S | re.M)
_INLINE = re.compile(r"`[^`]+`")
_LINK_TARGET = re.compile(r"\]\([^)\s]*\)")
_ATTRIBUTE = re.compile(r"\b(?:src|srcset|href|media)=\"[^\"]*\"")

#: Numbers that NAME something. Closed on purpose: an entry here is an argument that the
#: number cannot go stale, and each one says what it names.
IDENTIFIERS = (
    re.compile(r"\bSG-(?:DBX-)?\d\d\b"),  # a claim id
    re.compile(r"\bADR \d{4}\b"),  # an architecture decision record, in prose
    re.compile(r"\[\d{4}\]"),  # the same, as the text of the ADR column's link
    re.compile(r"\b(?:Type|Tipo) 2\b"),  # the slowly-changing-dimension type
    re.compile(r"\bApache-2\.0\b"),  # the licence
)

#: A number not glued to a word: `scd2` and `0010-the-chain` are names, `45` and `4x` are not.
#: A thousands group is part of its number, so `14 198 046` is reported once, not three times.
_NUMBER = re.compile(r"(?<![\w\-])\d+(?:[.,]\d+| \d{3}(?!\d))*")


def hand_typed_figures(text: str, document: str) -> list[Finding]:
    """Every number in the prose of a front page that no renderer owns and no rule names.

    The first line is the language banner and is skipped. Line numbers are the document's own,
    because a finding has to say where to look.
    """
    lines = text.split("\n")
    body = "\n".join(["", *lines[1:]])
    for pattern in (_GENERATED, _ANCHORS, _FENCE):
        # Blank the span but keep its newlines, so what follows keeps its line number.
        body = pattern.sub(lambda m: re.sub(r"[^\n]", " ", m.group(0)), body)
    found: list[Finding] = []
    for number, line in enumerate(body.split("\n"), start=1):
        prose = _ATTRIBUTE.sub(" ", _LINK_TARGET.sub("]", _INLINE.sub(" ", line)))
        for pattern in IDENTIFIERS:
            prose = pattern.sub(" ", prose)
        for match in _NUMBER.finditer(prose):
            found.append(
                Finding(
                    f"{document}:{number}",
                    "hand-typed figure",
                    f"{match.group(0)!r} is written by hand; render it from an anchor or remove it",
                )
            )
    return found

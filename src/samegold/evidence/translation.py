"""The two front pages say the same thing, or the fast lane says which section does not.

WHAT THIS EXISTS BECAUSE OF. `README.es.md` is a translation, and a translation is the oldest
shape of the defect this repository keeps finding: two documents that agreed on the day one was
copied from the other, and drifted apart on every commit afterwards. The English page is
rendered from evidence, so its figures cannot rot. Nothing stops the Spanish page from keeping
the figure it was translated with, and nothing stops a paragraph added to one from never
arriving at the other - which is worse than not having the translation, because a reader who
picks the Spanish page has no way to know it is a commit behind.

So the pair gets a gate, of the same kind as the four in `samegold.evidence.prose`: the parts
of a front page that a translation MUST NOT change are extracted from both and compared.

WHAT IS COMPARED, and the reasoning is the whole design. A translation changes the prose and
must not change:

  * the EVIDENCE ANCHORS, by name and by value. Both documents are in `RENDERED_FILES`, so a
    figure in the Spanish page is written by the same renderer from the same record. An anchor
    present in one and missing from the other is a figure that one page publishes and the
    other does not;
  * the NUMBERS written outside an anchor, as they are written. This repository already uses
    European forms on the English page - `135 036,87`, `7 567` - so "as they are written" is
    achievable rather than a normalisation problem, and the identity is the stricter rule;
  * the COMMANDS and IDENTIFIERS in backticks, and the fenced blocks BYTE FOR BYTE. `make
    refute SEED=424242` is not translated, and a fenced block that differs is a reader being
    given a command that the other half of the repository does not run;
  * the LINKS, by target. A section that links `docs/limits.md` in one language and nowhere in
    the other is the residual-risk page being quietly dropped for half the readers;
  * the CLAIM IDS, so `SG-04` is discussed in the same section in both;
  * the SHAPE: how many sections, and per section how many paragraphs, how many list items and
    how many **bold spans**. The bold span is this page's own mark for a load-bearing sentence,
    which makes counting them a usable proxy for "an assertion is in one and not the other" -
    the part of the request that no extractor can do exactly.

WHAT IT CANNOT DO. It cannot tell whether the Spanish sentence MEANS what the English one says;
nothing short of a reader can. It checks that the two pages carry the same figures, the same
commands, the same links and the same number of assertions in the same order, which is the part
that rots by itself. A mistranslation that keeps all of those is invisible to it, and
`docs/limits.md` carries that.

THE FIRST LINE OF EACH FILE IS EXCLUDED, by name rather than by accident: it is the language
banner, and it is the one line that MUST differ - each points at the other.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

#: An anchor, of any of the three flavours: `sg:` from the chain, `dbx:` from the Databricks
#: record, `repo:` from the file that defines the fact. Group 2 is the name, group 3 the value.
ANCHOR = re.compile(r"<!--(sg|dbx|repo):(.+?)-->(.*?)<!--/(?:sg|dbx|repo)-->", re.DOTALL)
#: A fenced block. The info string is kept: ```bash and ```text are different blocks.
FENCE = re.compile(r"^```(\w*)\n(.*?)^```", re.MULTILINE | re.DOTALL)
#: Inline code. Never spans a blank line, but DOES span a single newline, because both pages
#: are hard-wrapped and `make\npreflight` is one token written over two lines.
INLINE = re.compile(r"`([^`]+?)`", re.DOTALL)
#: A markdown link target, and an image's too.
LINK = re.compile(r"\]\(([^)\s]+)")
#: An html attribute that carries a path: <img src=...>, <source srcset=...>.
ATTR = re.compile(r"(?:src|srcset|href)=\"([^\"]+)\"")
#: A run of digits, with the separators this repository writes inside numbers: the thin and
#: non-breaking spaces a thousands group uses, the decimal comma, the decimal point, and the
#: colon and hyphen of a time or a date.
#: The space characters that appear INSIDE a number. `7 567` may be written with an
#: ordinary space, a non-breaking space or a narrow one; the difference is invisible on
#: the page, so it must not be a finding, and every other difference in a number must be.
#: Written by NAME rather than as a code point: a literal non-breaking space in source is
#: unreadable and one editor away from being silently replaced.
NBSP = "\N{NO-BREAK SPACE}"
NARROW_NBSP = "\N{NARROW NO-BREAK SPACE}"
#: What an anchor is replaced by before the other rules read the text.
MARKER = "\N{INVISIBLE SEPARATOR}"
NUMBER = re.compile(r"\d+(?:[.,:\-" + NBSP + NARROW_NBSP + r" ]\d+)*")
#: A claim id, of either register.
CLAIM_ID = re.compile(r"\bSG-(?:DBX-)?\d\d\b")
#: A bold span. Non-greedy and newline-crossing: every assertion on the front page is one.
BOLD = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
#: A markdown list item at the start of a line.
LIST_ITEM = re.compile(r"^\s*[-*]\s+\S", re.MULTILINE)
#: A section heading. The front page has no `###`, and if it grows one this splits on it too,
#: which is what we want: a subsection added to one page and not the other is the defect.
HEADING = re.compile(r"^(#{2,})\s+(.*)$", re.MULTILINE)
#: An html comment that is not an anchor: the generated-block fences.
BLOCK_FENCE = re.compile(r"<!--\s*samegold:(begin|end)\s+(\w+)\s*-->")


@dataclass(frozen=True)
class ParityBreak:
    """One thing the two pages do not agree on."""

    section: str
    kind: str
    detail: str

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"{self.section}: {self.kind}: {self.detail}"


def _normalise_space(text: str) -> str:
    """Collapse whitespace, so a token wrapped differently is the same token.

    The two pages are hard-wrapped at 96 columns and the wrap points fall in different places,
    because Spanish is longer. A rule that treated `make\\npreflight` as different from `make
    preflight` would report the wrap rather than the content.
    """
    return " ".join(text.split())


def _fold_digits(text: str) -> str:
    """Normalise the space characters INSIDE a number, and nothing else.

    `7 567` may be written with an ordinary space, a non-breaking space or a narrow one, and a
    renderer or an editor may swap them. That difference is invisible on the page, so it must
    not be a finding; every other difference in a number must be.
    """
    return unicodedata.normalize("NFKC", text).replace(NBSP, " ").replace(NARROW_NBSP, " ")


def _strip_anchors(text: str) -> str:
    """Replace each anchor, name and value alike, with one opaque marker.

    Anchors are compared whole by the `anchors` rule. Left in the text, every anchored figure
    would ALSO be a bare number and every anchor name ALSO a claim id, so one stale anchor
    would be reported three times by three rules and two of those reports would name a rule
    that did not find it. The marker is kept rather than deleted, so an anchor dropped from
    one page still changes the shape of the text around it.
    """
    return ANCHOR.sub(MARKER, text)


def anchors(text: str) -> list[tuple[str, str]]:
    """Every evidence anchor, in order, as (name, rendered value)."""
    return [(m.group(2).strip(), _normalise_space(m.group(3))) for m in ANCHOR.finditer(text)]


def fences(text: str) -> list[tuple[str, str]]:
    """Every fenced block, as (info string, body). Compared byte for byte, less the anchors."""
    return [
        (m.group(1), _fold_digits(_strip_anchors(m.group(2))).rstrip())
        for m in FENCE.finditer(text)
    ]


def code_spans(text: str) -> list[str]:
    """Every inline-code token, whitespace-folded. Commands and identifiers are not translated."""
    return sorted(_normalise_space(m.group(1)) for m in INLINE.finditer(_fenceless(text)))


def links(text: str) -> list[str]:
    """Every link and media target, sorted. Prose moves; what a section points at must not."""
    found = [m.group(1) for m in LINK.finditer(text)]
    found += [m.group(1) for m in ATTR.finditer(text)]
    return sorted(found)


def numbers(text: str) -> list[str]:
    """Every number written outside an anchor and outside a fence, sorted.

    Fences are excluded because they are compared whole, byte for byte, which is strictly
    stronger; counting their digits again would report one divergence as two.
    """
    bare = _fold_digits(_strip_anchors(_fenceless(text)))
    return sorted(m.group(0) for m in NUMBER.finditer(bare))


def claim_ids(text: str) -> list[str]:
    """Every claim id mentioned in the prose, sorted. Anchor names are the `anchors` rule."""
    return sorted(m.group(0) for m in CLAIM_ID.finditer(_strip_anchors(text)))


def _fenceless(text: str) -> str:
    return FENCE.sub("", text)


def _paragraphs(body: str) -> int:
    """Blank-line-separated blocks of prose, not counting fences, pictures or generated blocks."""
    stripped = _fenceless(body)
    stripped = BLOCK_FENCE.sub("", stripped)
    stripped = re.sub(r"<picture>.*?</picture>", "", stripped, flags=re.DOTALL)
    stripped = LIST_ITEM.sub("", stripped)
    stripped = re.sub(r"^\|.*$", "", stripped, flags=re.MULTILINE)
    return len([block for block in re.split(r"\n\s*\n", stripped) if block.strip()])


@dataclass(frozen=True)
class Section:
    """One `##` section of a front page, with the heading kept only for the error message."""

    index: int
    heading: str
    body: str

    @property
    def where(self) -> str:
        return f"section {self.index} ({self.heading!r})"


def sections(text: str) -> list[Section]:
    """Split a front page into its sections, with everything above the first heading as 0."""
    marks = list(HEADING.finditer(text))
    found = [Section(0, "(preamble)", text[: marks[0].start()] if marks else text)]
    for position, mark in enumerate(marks, start=1):
        end = marks[position].start() if position < len(marks) else len(text)
        found.append(Section(position, mark.group(2).strip(), text[mark.end() : end]))
    return found


def _drop_banner(text: str) -> str:
    """Remove the first line, which is the language banner and is REQUIRED to differ."""
    _, _, rest = text.partition("\n")
    return rest


#: Each rule as (name, extractor). A rule is a function of the section body to a comparable
#: value; the comparison is equality, and the message is built from the two values. Adding a
#: rule is adding a line here, which is the reason the rules are data rather than a method
#: per shape.
RULES: tuple[tuple[str, object], ...] = (
    ("anchors", anchors),
    ("fenced blocks", fences),
    ("commands and identifiers in backticks", code_spans),
    ("links", links),
    ("numbers outside an anchor", numbers),
    ("claim ids", claim_ids),
    ("bold assertions", lambda body: len(BOLD.findall(body))),
    ("list items", lambda body: len(LIST_ITEM.findall(body))),
    ("paragraphs", _paragraphs),
    ("generated blocks", lambda body: sorted(m.group(2) for m in BLOCK_FENCE.finditer(body))),
)


def _describe(value: object, other: object) -> str:
    """The difference between two extracted values, as few words as carry the finding."""
    if isinstance(value, int) and isinstance(other, int):
        return f"{value} against {other}"
    if isinstance(value, list) and isinstance(other, list):
        only_here = [item for item in value if item not in other]
        only_there = [item for item in other if item not in value]
        parts = []
        if only_here:
            parts.append(f"only in the English page: {only_here!r}")
        if only_there:
            parts.append(f"only in the Spanish page: {only_there!r}")
        if not parts:
            parts.append(f"same items, different order: {value!r} against {other!r}")
        return "; ".join(parts)
    return f"{value!r} against {other!r}"  # pragma: no cover - no rule returns anything else


def compare(english: str, spanish: str) -> list[ParityBreak]:
    """Return everything the two front pages disagree on. Empty means they are the same page."""
    left = sections(_drop_banner(english))
    right = sections(_drop_banner(spanish))
    if len(left) != len(right):
        return [
            ParityBreak(
                "the whole page",
                "sections",
                f"{len(left)} in the English page and {len(right)} in the Spanish one: "
                f"{[s.heading for s in left]!r} against {[s.heading for s in right]!r}",
            )
        ]
    found: list[ParityBreak] = []
    for here, there in zip(left, right, strict=True):
        for name, extract in RULES:
            mine = extract(here.body)  # type: ignore[operator]
            theirs = extract(there.body)  # type: ignore[operator]
            if mine != theirs:
                found.append(ParityBreak(here.where, name, _describe(mine, theirs)))
    return found


#: The two pages, in the order `compare` takes them.
PAIR = ("README.md", "README.es.md")


def compare_files(repo: Path) -> list[ParityBreak]:
    """`compare`, on the pair this repository publishes."""
    english, spanish = (repo / name for name in PAIR)
    return compare(english.read_text(encoding="utf-8"), spanish.read_text(encoding="utf-8"))


def banner_of(text: str) -> str:
    """The first line, which must link the other page."""
    return text.partition("\n")[0].strip()

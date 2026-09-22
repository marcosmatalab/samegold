"""The two front pages carry the same figures, commands, links and assertions.

The gate itself is `samegold.evidence.translation`. What is here is the one test that points
it at the repository, and fourteen that point it at a page deliberately broken in each of the
fourteen ways a translation rots - because a gate whose first run finds nothing has not been
shown to work, and this one found nothing on its first run.

That is not a rhetorical flourish. It is what happened twice to the ADR rule in
`samegold.evidence.prose`, whose first two versions passed against a document that was lying,
and it is written up in `docs/findings/`. A rule is worth what its falsifications are worth.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from samegold.evidence import translation

REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def pages() -> tuple[str, str]:
    english, spanish = (REPO / name for name in translation.PAIR)
    return english.read_text(encoding="utf-8"), spanish.read_text(encoding="utf-8")


def test_the_two_front_pages_agree(pages: tuple[str, str]) -> None:
    """THE TEST. Everything below exists to show that this one can fail."""
    breaks = translation.compare(*pages)
    assert not breaks, "the front pages have drifted apart:\n" + "\n".join(
        f"  {item}" for item in breaks
    )


def test_each_page_links_the_other_on_its_first_line(pages: tuple[str, str]) -> None:
    """The one line that MUST differ, and the reason the comparison drops it.

    Without this, excluding the first line would be an exclusion with nothing behind it: a
    reader who lands on the Spanish page has no other way to get to the English one, and the
    rule that skips the line would happily skip it being empty.
    """
    english, spanish = (translation.banner_of(text) for text in pages)
    assert "(README.es.md)" in english, english
    assert "(README.md)" in spanish, spanish
    assert english != spanish


def test_both_pages_are_rendered_from_the_evidence(pages: tuple[str, str]) -> None:
    """Not just compared: BOTH are written by `make readme` from the same record.

    Comparing two hand-maintained pages would only report the drift after it happened. The
    figures cannot drift at all, because neither page holds one that a person typed - and
    that is a property of `RENDERED_FILES`, not of this test file.
    """
    from samegold.cli import DBX_DOCUMENTS, RENDERED_FILES

    for name in translation.PAIR:
        assert name in RENDERED_FILES, f"{name} is not rendered from the evidence"
    assert set(translation.PAIR) <= set(DBX_DOCUMENTS)
    english, spanish = pages
    assert translation.anchors(english) == translation.anchors(spanish)


# Each entry is (what a translator did wrong, the rule that must catch it, the edit). The edit
# is applied to the SPANISH page, and the test fails if the named rule stays quiet. Written as
# data because the interesting content is the list, not fourteen near-identical functions.
MUTATIONS: tuple[tuple[str, str, str, str], ...] = (
    (
        "a number in the prose drifting",
        "numbers",
        "anunciaba 780 eventos",
        "anunciaba 781 eventos",
    ),
    (
        "a number inside an image alt text drifting",
        "numbers",
        "bruto de 14198046",
        "bruto de 14198047",
    ),
    (
        "a command translated, so the reader is given one that does not exist",
        "backticks",
        "`make refute` te deja",
        "`make refutar` te deja",
    ),
    (
        "a link pointing somewhere else",
        "links",
        "](docs/limits.md)",
        "](docs/limites.md)",
    ),
    (
        "a picture pointing at a file that is not there",
        "links",
        'src="docs/img/job-graph-light.svg"',
        'src="docs/img/job-graph.svg"',
    ),
    (
        "an assertion quietly losing its emphasis",
        "bold",
        "**El bundle se comprueba desde un clon; el workspace no.**",
        "El bundle se comprueba desde un clon; el workspace no.",
    ),
    (
        "a bullet dropped from the index",
        "list",
        "- [`docs/limits.md`](docs/limits.md) - lo que este repositorio no pudo verificar, y "
        "por qu\N{LATIN SMALL LETTER E WITH ACUTE}\n",
        "",
    ),
    (
        "a whole paragraph never translated",
        "paragraphs",
        "El pipeline es el sujeto. El arn\N{LATIN SMALL LETTER E WITH ACUTE}s es el trabajo.\n\n",
        "",
    ),
    (
        "a section added to one page only",
        "sections",
        "## Ad\N{LATIN SMALL LETTER O WITH ACUTE}nde ir despu\N{LATIN SMALL LETTER E WITH ACUTE}s",
        "## Una secci\N{LATIN SMALL LETTER O WITH ACUTE}n de m\N{LATIN SMALL LETTER A WITH ACUTE}s"
        "\n\nque el otro no tiene.\n\n## Ad\N{LATIN SMALL LETTER O WITH ACUTE}nde ir "
        "despu\N{LATIN SMALL LETTER E WITH ACUTE}s",
    ),
    (
        "a fenced command block edited, so the two pages tell you to run different things",
        "fenced",
        "make refute SEED=424242   # every claim",
        "make refute SEED=424243   # every claim",
    ),
    (
        "a claim discussed under the wrong id",
        "claim ids",
        "propiedad que mide `SG-04`",
        "propiedad que mide `SG-05`",
    ),
    (
        "a generated block missing, so one page has a table the other does not",
        "generated",
        "<!-- samegold:begin demo -->\n",
        "",
    ),
)


@pytest.mark.parametrize(
    ("what", "rule", "before", "after"),
    MUTATIONS,
    ids=[entry[0] for entry in MUTATIONS],
)
def test_the_gate_catches(
    pages: tuple[str, str], what: str, rule: str, before: str, after: str
) -> None:
    english, spanish = pages
    assert before in spanish, f"the mutation no longer applies; the page moved under it: {what}"
    broken = spanish.replace(before, after, 1)
    breaks = translation.compare(english, broken)
    assert breaks, f"the gate said nothing about: {what}"
    assert any(rule in item.kind for item in breaks), (
        f"caught, but by the wrong rule: expected {rule!r}, got "
        f"{sorted({item.kind for item in breaks})} for: {what}"
    )


def test_a_difference_in_wrapping_is_not_a_difference(pages: tuple[str, str]) -> None:
    """The one thing a translation is ALLOWED to change about a compared token.

    Spanish is longer than English, so the two pages wrap in different places and `make
    preflight` is written over two lines on one of them. A rule that reported the wrap would
    be a rule nobody could keep green, and the first thing anyone would do is delete it.
    """
    english, spanish = pages
    rewrapped = re.sub(r"`make\s+preflight`", "`make\npreflight`", spanish)
    assert rewrapped != spanish
    assert not translation.compare(english, rewrapped)


def test_a_thousands_separator_is_a_finding_and_this_is_the_cost_of_that() -> None:
    """The rule's price, measured and written down rather than discovered by a contributor.

    `14.198.046` is how that figure is written in Spanish, and the rule refuses it: the
    English page writes `14 198 046` and identity is the whole rule. This is the one class of
    legitimate difference the gate forbids, `docs/limits.md` carries it, and the trade is
    deliberate - a normalising comparison would also accept `14.198.046` where the record says
    `14 198 046`, which is a published figure differing between the two front pages.
    """
    english = "## x\n\nit moved to 14 198 046 cents.\n"
    spanish = (
        "## x\n\nse movi\N{LATIN SMALL LETTER O WITH ACUTE} a 14.198.046 "
        "c\N{LATIN SMALL LETTER E WITH ACUTE}ntimos.\n"
    )
    breaks = translation.compare("banner\n" + english, "banner\n" + spanish)
    assert [item.kind for item in breaks] == ["numbers outside an anchor"]


def test_an_invisible_space_inside_a_number_is_not_a_finding() -> None:
    """And the other side of that trade: what the rule deliberately does NOT report.

    A non-breaking space and an ordinary one are the same character to a reader. Reporting
    that difference would be the gate finding a defect in the font.
    """
    english = "## x\n\n7\N{NO-BREAK SPACE}567 lines.\n"
    spanish = "## x\n\n7 567 l\N{LATIN SMALL LETTER I WITH ACUTE}neas.\n"
    assert not translation.compare("banner\n" + english, "banner\n" + spanish)


# The two anchor mutations are computed rather than written out, and that is a finding rather
# than a style. They were literals first - `tests_fast-->694` and `fast_lane_seconds-->234.9` -
# and the first evidence run in CI rendered different values into both, so the tests failed
# saying the page had moved under them. A test that hardcodes a RENDERED figure is the same
# defect as a document that does: it is a hand-typed copy of something a renderer owns.


def _first_anchor(text: str) -> re.Match[str]:
    match = translation.ANCHOR.search(text)
    assert match is not None, "the page has no evidence anchors left"
    return match


def test_the_gate_catches_an_anchored_figure_edited_by_hand(pages: tuple[str, str]) -> None:
    english, spanish = pages
    match = _first_anchor(spanish)
    broken = spanish[: match.start(3)] + match.group(3) + "9" + spanish[match.end(3) :]
    breaks = translation.compare(english, broken)
    assert [item.kind for item in breaks] == ["anchors"], breaks


def test_the_gate_catches_an_anchor_deleted_leaving_the_figure_as_plain_text(
    pages: tuple[str, str],
) -> None:
    """The subtle one: the page still SHOWS the right number, and it has stopped being live."""
    english, spanish = pages
    match = _first_anchor(spanish)
    broken = spanish[: match.start()] + match.group(3) + spanish[match.end() :]
    breaks = translation.compare(english, broken)
    assert any(item.kind == "anchors" for item in breaks), breaks

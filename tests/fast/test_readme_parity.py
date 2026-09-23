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
        "hasta 45 d\N{LATIN SMALL LETTER I WITH ACUTE}as",
        "hasta 46 d\N{LATIN SMALL LETTER I WITH ACUTE}as",
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
        "`make refute` deja que",
        "`make refutar` deja que",
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
        "**Seguro por dise\N{LATIN SMALL LETTER N WITH TILDE}o.**",
        "Seguro por dise\N{LATIN SMALL LETTER N WITH TILDE}o.",
    ),
    (
        "a bullet dropped from the index",
        "list",
        "- [`docs/limits.md`](docs/limits.md) - limitaciones conocidas y riesgo residual\n",
        "",
    ),
    (
        "a whole paragraph never translated",
        "paragraphs",
        "Sin cuenta, sin credenciales, sin m\N{LATIN SMALL LETTER A WITH ACUTE}s red que PyPI.\n\n",
        "",
    ),
    (
        "a section added to one page only",
        "sections",
        "\nApache-2.0.",
        "\n## Una secci\N{LATIN SMALL LETTER O WITH ACUTE}n de "
        "m\N{LATIN SMALL LETTER A WITH ACUTE}s\n\nque el otro no tiene.\n\nApache-2.0.",
    ),
    (
        "a fenced command block edited, so the two pages tell you to run different things",
        "fenced",
        "make refute SEED=424242   # the seven",
        "make refute SEED=424243   # the seven",
    ),
    (
        "a claim discussed under the wrong id",
        "claim ids",
        "`SG-04` mide cu\N{LATIN SMALL LETTER A WITH ACUTE}nto",
        "`SG-05` mide cu\N{LATIN SMALL LETTER A WITH ACUTE}nto",
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


# The GIF's two hand-typed numbers, and why they are allowed to be hand-typed.
#
# Everything else on the front page goes through an evidence anchor. The acceleration factor
# and the real duration cannot: they describe a binary artifact that no claim produces, and a
# claim that re-recorded a terminal session on every `make evidence` would be absurd. So they
# are checked against the FILE instead, here, from a clone, with no dependency: a GIF carries
# the delay of each frame in its own Graphic Control Extension blocks, in hundredths of a
# second, and they add up to how long the thing plays.
#
# What that ties together is the honest part of the claim. `make gif` records the run at real
# speed and then divides the frame delays by four; if somebody changes the divisor and not the
# sentence, or re-records a run of a different length and leaves `73,1` where it was, the
# product stops matching and the fast lane says so.

GIF = REPO / "docs" / "img" / "refute.gif"
#: The sentence is found by the file it names rather than by its own wording, so that
#: rewriting the prose around these numbers does not break the test that checks them.
DECLARATION = "docs/refute.tape"


def gif_playback_seconds(raw: bytes) -> float:
    """How long a GIF plays, from its own frame delays. No decoder, no dependency."""
    delays: list[int] = []
    at = 0
    while True:
        at = raw.find(b"\x21\xf9\x04", at)
        if at < 0:
            break
        # A Graphic Control Extension is 0x21 0xF9 0x04, four bytes, then a 0x00 terminator,
        # and then the image or another extension. Requiring that shape keeps the scan from
        # matching the same three bytes inside compressed image data.
        tail = raw[at + 7 : at + 9]
        if len(tail) == 2 and tail[0] == 0 and tail[1] in (0x2C, 0x21):
            delays.append(int.from_bytes(raw[at + 4 : at + 6], "little"))
        at += 3
    assert delays, "no frame delays found; this is not a GIF this function understands"
    return sum(delays) / 100


def declared_speed_and_duration(text: str) -> tuple[int, float]:
    paragraph = next(block for block in text.split(chr(10) + chr(10)) if DECLARATION in block)
    factor = re.search(r"(\d+)x", paragraph)
    duration = re.search(r"(\d+),(\d+) s", paragraph)
    assert factor and duration, f"the GIF paragraph declares neither: {paragraph!r}"
    return int(factor.group(1)), float(f"{duration.group(1)}.{duration.group(2)}")


def test_the_gif_plays_for_as_long_as_the_page_says_it_does() -> None:
    factor, real_seconds = declared_speed_and_duration(
        (REPO / "README.md").read_text(encoding="utf-8")
    )
    plays_for = gif_playback_seconds(GIF.read_bytes())
    expected = real_seconds / factor
    # One per cent, measured rather than picked: the parse is exact (18.27 s against the 18.275
    # the sentence implies, 0.03% apart), so the tolerance is there for rounding in the printed
    # figure and nothing else. At 2% a run re-recorded at 73,9 s and left declared as 73,1
    # slipped through; at 1% it does not. A lie smaller than that - 73,1 for 73,2 - is below
    # the threshold and this says so rather than implying the check is exact.
    assert abs(plays_for - expected) / expected < 0.01, (
        f"the front page says the run took {real_seconds} s played at {factor}x, which is "
        f"{expected:.2f} s of GIF, and docs/img/refute.gif plays for {plays_for:.2f} s. "
        f"Either the sentence or the artifact is stale; `make gif` regenerates the artifact."
    )


def test_both_pages_declare_the_same_speed_and_the_same_run(pages: tuple[str, str]) -> None:
    """Covered by the numbers rule too; stated separately because it is the claim, not a token."""
    english, spanish = pages
    assert declared_speed_and_duration(english) == declared_speed_and_duration(spanish)

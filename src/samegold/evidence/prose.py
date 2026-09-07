"""The drift gate, extended from anchored figures to the sentences around them.

WHAT THIS EXISTS BECAUSE OF. Every figure this repository publishes goes through an evidence
anchor, and `samegold check` fails when a document and the record disagree - which is the whole
argument of the project: a number nobody re-measures is a number that is only true by accident.

The PROSE around those figures had no such gate, and on 6 September 2026 three sentences in the
repository were false:

  * `docs/milestones.md` said `databricks/resources/` holds `grants.yml`, `jobs.yml` and
    `volumes.yml`, "and nothing else". `dashboards.yml` was added in the same merge that shipped
    that sentence - false the moment it was written, by the commit that wrote it;
  * `EXAM_MAP.md` said "nothing has run yet" of a lane that had been running since 3 September
    and had four job run ids committed under `evidence/databricks/`;
  * the `Makefile` said the same of `make databricks`.

A repository whose thesis is that unmeasured numbers rot, with prose that had been rotting for
days. So the sentences get a gate too.

WHAT IT CAN AND CANNOT DO, said here rather than discovered later. It checks the three shapes of
claim that the repository can falsify from its own files, and nothing else:

  1. an EXHAUSTIVE ENUMERATION of a directory - "holds a, b and c, and nothing else";
  2. a claim that something has NEVER RUN, against the run records under `evidence/`;
  3. a claim that a PATH DOES NOT EXIST, against the filesystem.

It cannot read intent, and it does not try. What it does instead is make the exceptions
explicit: a sentence that is genuinely true and matches one of these shapes has to be declared
in `EXEMPTIONS` with its reason, and an exemption that stops matching anything is itself a
failure - so a declared exception cannot outlive the sentence it was written for.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

# `dir/` holds `a`, `b` and `c`, and nothing else.
#
# The tail is what makes it checkable: without "nothing else" the sentence is a sample and not a
# claim. With it, the listing is a closed set and a directory is a closed set, and the two can be
# compared.
ENUMERATION = re.compile(
    r"`(?P<directory>[\w./-]+/)`\s+holds\s+(?P<items>.{0,200}?),?\s+and\s+nothing\s+else",
    re.IGNORECASE | re.DOTALL,
)
BACKTICKED = re.compile(r"`([^`]+)`")

# "has never been run", and the family around it. Deliberately narrow: these are the phrasings
# this repository actually uses, and a gate that matched every negation would be a gate people
# route around by rewording.
NEVER_RUN = re.compile(
    r"(?:has\s+)?never\s+(?:been\s+)?(?:run|executed|dispatched|deployed)"
    r"|nothing\s+has\s+run(?:\s+yet)?"
    r"|no\s+run\s+exists",
    re.IGNORECASE,
)
# THREE SHAPES THAT ARE NARRATION AND NOT CLAIMS, skipped before the exemption list is even
# consulted - because an exemption is a promise somebody has to keep up to date, and these do
# not need one. Each was measured against the seven matches this pattern found on the day it was
# written: four were false statements about the present, and these three were not statements
# about the present at all.
#
#   * PAST PERFECT. "the commits that added them had never been deployed" is a sentence about
#     what was true before something happened, in a document describing what happened;
#   * QUOTED. `M12 said "runnable, never run" for six rounds` quotes a sentence in order to say
#     it is no longer true. Flagging it would be flagging the correction;
#   * A HEADING. `### The comparison a file declared as its reason for existing, never executed`
#     titles a finding about a past defect. FINDINGS.md is a history by construction.
PAST_PERFECT = re.compile(r"\bhad\s+never\b", re.IGNORECASE)
QUOTATION = re.compile(r'"([^"\n]{0,200})"')

# "there is no `path`" / "no `path` exists".
NO_SUCH_PATH = re.compile(
    r"(?:there\s+is\s+no|no)\s+`(?P<path>[\w./-]+)`(?:\s+exists)?",
    re.IGNORECASE,
)


class Kind(Enum):
    """WHY a sentence is exempt, because there are two reasons and they expire differently.

    The list used to hold both under one word, and "it is true, here is the measurement" and
    "this gate has nothing to say about this sentence" are not the same promise:

      * MEASURED_TRUE is a claim about the PRESENT that happens to be true today. It is exactly
        the kind of sentence this gate exists to catch, spared only because the repository cannot
        check it offline. It has an expiry: the day the Databricks job runs, every MEASURED_TRUE
        exemption about a job that has not run becomes a false sentence with a note attached
        saying somebody once checked. Re-reading these is work that has to happen.
      * OUT_OF_SCOPE is not a claim about the present at all, and no future run can make it
        false. The gate's evidence is the run records under `evidence/databricks/`, and those
        give it standing over sentences about the Databricks lane and none whatsoever over a
        sentence about, say, a shell guard that was fixed in the same round it was written.
        Nothing about this one expires, and re-reading it when the lane runs is wasted effort.

    The distinction is recorded rather than enforced. A test that tried to decide which kind a
    sentence deserved would be reading intent, which is the thing this module opens by saying it
    does not do.
    """

    MEASURED_TRUE = "measured true"
    OUT_OF_SCOPE = "out of scope"


@dataclass(frozen=True)
class Exemption:
    """A sentence that matches one of the shapes above and is nevertheless not a defect.

    `fragment` has to appear in the document, or this exemption is stale and the check fails on
    the exemption rather than on the document. That is the property that stops this list from
    becoming the place inconvenient sentences go to be forgotten.

    MATCHED BY FRAGMENT AND NOT BY LINE, which is what lets an exemption survive a merge that
    moves the sentence down the file. The cost of that choice is the other half of the same
    property: reword the sentence and the fragment is orphaned, and `stale_exemptions` fails
    rather than letting the exemption quietly cover nothing.
    """

    document: str
    fragment: str
    kind: Kind
    reason: str


EXEMPTIONS: tuple[Exemption, ...] = (
    Exemption(
        document="docs/milestones.md",
        fragment="it has never been dispatched",
        kind=Kind.MEASURED_TRUE,
        reason=(
            "TRUE, and measured on 6 September 2026: the `databricks` workflow is "
            "workflow_dispatch only and the GitHub API reports total_count 0 for its entire "
            "history. This repository cannot check that offline - it is a fact about a remote "
            "service - so it is exempted by name rather than by a pattern that would also "
            "exempt the two false sentences beside it."
        ),
    ),
)


def _tracked_files(repo: Path) -> frozenset[str]:
    """Every path this checkout tracks, as git itself lists them.

    THE REPOSITORY IS ASKED, because the shape of a name does not say whether it is a path. The
    first version of the absence check decided that by looking at the candidate: a `/` in it, or
    one of four extensions on the end. That kept prose words out of the gate - "there is no
    `deploy` step" is a sentence about a word - and it also made the check blind to every file at
    the top of the repository. `Makefile`, `pyproject.toml` and `LICENSE` have no `/` and none of
    those four extensions, and they are among the most-cited paths in these documents.

    A TRACKED FILE, and not merely a name git recognises. `git ls-files --error-unmatch evidence`
    SUCCEEDS, because a directory is a pathspec that matches the twenty-two files under it, so
    that command alone would turn "there is no `evidence`" red - the exact sentence the shape
    filter was protecting. The predicate is exact membership in git's own list, which is true for
    `Makefile` and false for `evidence`, `docs` and every bare word.

    Outside a git checkout this is empty and the absence check does nothing, which is the honest
    answer: a tree with no index cannot be asked what it tracks.
    """
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), "ls-files", "-z"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return frozenset()
    if out.returncode != 0:
        return frozenset()
    return frozenset(path for path in out.stdout.split("\x00") if path)


def _run_records(repo: Path) -> list[Path]:
    """The committed evidence that the Databricks lane has run: one file per fetched run."""
    return sorted((repo / "evidence" / "databricks").glob("SG-DBX-01*.json"))


def _sentence_at(text: str, index: int) -> str:
    """The sentence the match sits in, so an exemption can be recognised by its own words."""
    start = max(text.rfind(".", 0, index), text.rfind("\n\n", 0, index)) + 1
    end = text.find(".", index)
    return text[start : end if end != -1 else len(text)].strip()


@dataclass(frozen=True)
class Drift:
    document: str
    line: int
    quote: str
    why: str

    def __str__(self) -> str:
        return f"{self.document}:{self.line}: {self.quote}\n    {self.why}"


def check_document(path: Path, repo: Path) -> list[Drift]:
    """Every falsifiable sentence in one document that the repository contradicts today."""
    text = path.read_text(encoding="utf-8")
    name = path.relative_to(repo).as_posix()
    exempted = [e.fragment for e in EXEMPTIONS if e.document == name]
    out: list[Drift] = []

    def line_of(index: int) -> int:
        return text.count("\n", 0, index) + 1

    def is_exempt(index: int) -> bool:
        sentence = _sentence_at(text, index)
        return any(fragment in sentence for fragment in exempted)

    for match in ENUMERATION.finditer(text):
        if is_exempt(match.start()):
            continue
        directory = repo / match.group("directory")
        if not directory.is_dir():
            continue
        claimed = {item.split("/")[-1] for item in BACKTICKED.findall(match.group("items"))}
        actual = {child.name for child in directory.iterdir() if not child.name.startswith(".")}
        missing = sorted(actual - claimed)
        if missing:
            out.append(
                Drift(
                    name,
                    line_of(match.start()),
                    match.group(0).replace("\n", " ")[:120],
                    f"{match.group('directory')} also holds {missing}. A sentence that says "
                    f"'and nothing else' is a closed set, and the directory has moved under it.",
                )
            )

    lines = text.splitlines()

    def is_narration(index: int) -> bool:
        sentence = _sentence_at(text, index)
        line = lines[line_of(index) - 1] if line_of(index) <= len(lines) else ""
        if path.suffix == ".md" and line.lstrip().startswith("#"):
            return True  # a markdown heading titles a section rather than asserting anything
        if PAST_PERFECT.search(sentence):
            return True
        # INSIDE a quotation, by position rather than by matching text back: a sentence that
        # quotes an old claim in order to correct it must not be flagged as making it.
        return any(
            quotation.start(1) <= index < quotation.end(1) for quotation in QUOTATION.finditer(text)
        )

    records = _run_records(repo)
    if records:
        for match in NEVER_RUN.finditer(text):
            if is_exempt(match.start()) or is_narration(match.start()):
                continue
            out.append(
                Drift(
                    name,
                    line_of(match.start()),
                    _sentence_at(text, match.start())[:160],
                    f"{len(records)} run record(s) are committed under evidence/databricks/ "
                    f"({', '.join(r.name for r in records)}). If this sentence is about "
                    f"something that genuinely has not run, say so in "
                    f"samegold.evidence.prose.EXEMPTIONS with the measurement behind it.",
                )
            )

    # Computed once per document and only when there is something to test it against, so a
    # document with no absence claims in it costs no subprocess.
    tracked: frozenset[str] | None = None
    for match in NO_SUCH_PATH.finditer(text):
        if is_exempt(match.start()):
            continue
        candidate = match.group("path")
        if tracked is None:
            tracked = _tracked_files(repo)
        if candidate not in tracked:
            continue  # a word in backticks, a directory, or a file no commit knows about
        if (repo / candidate).exists():
            out.append(
                Drift(
                    name,
                    line_of(match.start()),
                    _sentence_at(text, match.start())[:160],
                    f"{candidate} exists.",
                )
            )
    return out


def check_documents(repo: Path, documents: list[Path]) -> list[Drift]:
    """Every drifted sentence across the documents, in document order."""
    return [drift for path in documents if path.exists() for drift in check_document(path, repo)]


def stale_exemptions(repo: Path) -> list[str]:
    """Exemptions whose sentence is no longer in the document they name.

    An exemption is a promise about a matching sentence. When the sentence goes, the promise is
    about nothing - and a list of exceptions nobody prunes is how the next false sentence gets
    waved through.

    The KIND is named in the failure, because it says what the reader has to do next. An orphaned
    MEASURED_TRUE exemption means a claim about the present lost its measurement; an orphaned
    OUT_OF_SCOPE one usually means the sentence was simply rewritten, and the fragment needs
    updating rather than the fact re-checking.
    """
    out = []
    for exemption in EXEMPTIONS:
        path = repo / exemption.document
        if not path.exists():
            out.append(f"{exemption.document} does not exist ({exemption.kind.value})")
        elif exemption.fragment not in path.read_text(encoding="utf-8"):
            out.append(
                f"{exemption.document} no longer contains {exemption.fragment!r} "
                f"({exemption.kind.value})"
            )
    return out

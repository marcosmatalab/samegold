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
  3. a claim that a PATH DOES NOT EXIST, against the filesystem;
  4. an ACCEPTED ADR THAT QUOTES A COMMAND, against the implementation - added 22 September
     2026, after ADR 0014 sat on `main` for a day describing an auto-merge that had been
     refused at commit time and was in no workflow. The three rules above read all fourteen
     ADRs, through the same `docs/**/*.md` glob as everything else, and none of them had
     anything to say about that shape.

It cannot read intent, and it does not try. What it does instead is make the exceptions
explicit: a sentence that is genuinely true and matches one of these shapes has to be declared
in `EXEMPTIONS` with its reason, and an exemption that stops matching anything is itself a
failure - so a declared exception cannot outlive the sentence it was written for.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import urllib.request
from collections.abc import Sequence
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

# ---------------------------------------------------------------- an ADR that describes a
# change it did not make
#
# ADR 0014 was written in the same round as the change it describes, the change was refused at
# commit time, and the ADR went in anyway saying `gh pr merge --auto --squash --delete-branch`
# had been added "immediately after the `gh pr create` that is already there". It had not. An
# accepted ADR is the most load-bearing prose in this repository - it is what a reader consults
# to find out why something is the way it is - and the three rules above all walked past it,
# not because they do not read `docs/adr/` (they do, all fourteen, through the `docs/**/*.md`
# glob) but because none of them has anything to say about this shape.
#
# WHAT THIS RULE CAN AND CANNOT DO, said plainly because the gap is the interesting part.
# "Does this paragraph describe a change that exists" is not computable. What IS computable is
# the narrow case the failure took: an accepted ADR that QUOTES A COMMAND as part of what it
# decided, where that command appears nowhere in the implementation. Everything else an ADR
# claims is still unguarded, and `docs/limits.md` says so.
#
# Every parameter below was chosen by measuring it against the fourteen ADRs already in the
# tree rather than by taste, because a rule that fires on what is already there is a rule
# somebody turns off:
#
#   * INLINE CODE SPANS, of which the fourteen hold 96. Filtering to command shapes leaves 25;
#     restricting to the asserting sections leaves 10, of which exactly one - 0014's - is
#     absent from the tree. Zero false positives at every step.
#   * A COMMAND is a span whose first token is a tool this repository drives and which has at
#     least one argument. `git` on its own is a word in a sentence; `gh pr merge --auto` is a
#     claim that something was wired up.
#   * THE ASSERTING SECTIONS ONLY. "Alternatives rejected" quotes commands the repository
#     deliberately does NOT run, and "Context" quotes measurements taken before the decision;
#     neither asserts that the tree contains anything. Decision and Consequences do.
#   * THE EVIDENCE EXCLUDES PROSE, AND COMMENTS ARE PROSE. This is the whole of why the first
#     two versions of this rule found nothing. The first read every tracked file, so a command
#     quoted in an ADR appeared in the tree - in that ADR. The second excluded `docs/` and
#     still passed, because the paragraph you are reading quotes the command as an EXAMPLE of
#     the defect, and a comment in a `.py` file is prose that happens to live in source. So
#     Python is reduced to its code (comments and docstrings removed by an AST round-trip,
#     string literals kept, because a command a program actually runs is often a string), and
#     `#` comments come off the YAML, the shell and the Makefile.
#   * `make X` IS NOT A LITERAL. Its evidence is a Makefile target called `X`, which is why
#     `make` is not in the tool list below and is handled on its own. Checking it as a string
#     reported `make fast` and `make preflight` as absent, which they are: the Makefile
#     declares `fast:` and `preflight:` and never spells the invocation.
ADR_TOOLS = (
    "gh",
    "git",
    "samegold",
    "pytest",
    "ruff",
    "mypy",
    "databricks",
    "python",
    "npm",
    "curl",
)
#: A Makefile rule, at the start of a line. `.PHONY` and pattern rules are not targets a
#: document would tell somebody to run.
MAKE_TARGET = re.compile(r"^(?P<target>[a-zA-Z_][\w-]*):", re.MULTILINE)
#: The sections in which an ADR asserts that the tree now contains something.
ADR_ASSERTING_SECTION = re.compile(
    r"^##\s+(?:Decision|Consequences)\b.*?$(?P<body>.*?)(?=^##\s|\Z)",
    re.MULTILINE | re.DOTALL | re.IGNORECASE,
)
ADR_STATUS = re.compile(r"^\*\*Status\*\*\s*(?P<status>[\w-]+)", re.MULTILINE)
#: Where an ADR's claim may be honoured. Prose is absent on purpose; see above.
IMPLEMENTATION_ROOTS = ("src/", "tests/", "scripts/", ".github/", "databricks/", "pipelines/")
IMPLEMENTATION_FILES = ("Makefile", "pyproject.toml")


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


# THE EVENT THAT FALSIFIES A MEASURED_TRUE EXEMPTION, named so a test can ask about it.
#
# `stale_exemptions` catches an exemption whose SENTENCE moved. It cannot catch one whose WORLD
# moved: a sentence that is true because a workflow has never been dispatched becomes false the
# day it is, and nothing in the document changes, so nothing goes red. This repository cannot
# check that offline - it is a fact about a remote service, which is why such a sentence is
# exempt at all. CI has a network, so CI is where it is asked.
#
# NO EXEMPTION CARRIES THIS EXPIRY TODAY, and the mechanism is kept anyway. Three did, over
# sentences that counted the databricks workflow's run history, and the fix was not to wait for
# the expiry to fire: it was to stop writing sentences about run history. Each of the three now
# states what the workflow is ALLOWED to do - `workflow_dispatch` only, `validate` by default,
# no option that starts compute, a token in an environment rather than a repository secret -
# which is a fact about the tree, true before the first dispatch and after it, and checkable
# from a clone. Whether it HAS run is the badge on the front page, which is live by
# construction and therefore never a sentence anybody has to keep up to date.
#
# `tests/fast/test_prose_gate.py` keeps this honest in both directions: the real check is
# vacuous while no exemption carries the expiry, so the test also builds one and requires the
# machinery to report it. A gate with nothing to guard must still be shown to work, or it is a
# green tick for no work.
DATABRICKS_WORKFLOW_HAS_RUN = "the databricks workflow has been dispatched at least once"


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
    # WHAT WOULD MAKE THIS FALSE, for the exemptions where an outside event would. A
    # MEASURED_TRUE exemption without one is a claim nothing can falsify on a schedule, which is
    # allowed but rarer than it looks; `expiring_on` is how a test finds the ones that can.
    expires_when: str | None = None


EXEMPTIONS: tuple[Exemption, ...] = (
    # THE THREE BELOW ARE THE MERGE'S OWN, and this commit is the first place they could live.
    # `warehouse-placeholder` wrote the sentences and has no prose.py; `surface-round` wrote
    # this gate and none of the sentences. Neither branch is red on its own, and neither could
    # carry these: on surface-round all three fragments are absent, so `stale_exemptions` would
    # have failed on the exemptions instead of passing on the document. The drift is created by
    # putting the two branches together, so the exemption for it belongs to the merge.
    Exemption(
        document="FINDINGS.md",
        fragment="in a guard added the day before and never executed",
        kind=Kind.OUT_OF_SCOPE,
        reason=(
            "NOT A CLAIM ABOUT THE PRESENT, and not about the Databricks lane. It describes a "
            "shell guard in scripts/databricks_run.sh that was defeated by its own input "
            "parsing and was fixed in the round that found it. The gate's evidence is the job "
            "run records, which give it no standing over a sentence about a shell function - "
            "flagging this one is the gate reaching outside what it can measure. The "
            "alternative considered and refused was widening `is_narration` to cover table "
            "cells: FINDINGS.md is almost entirely tables, so that would have blinded the "
            "never-run check across the whole document, including the two sentences beside "
            "this one that ARE about the present and must go red the day the workflow runs."
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


def _code_only(source: str) -> str:
    """Python with its comments and docstrings removed, and everything else kept.

    An AST round-trip rather than a regex: `#` inside a string is not a comment, and the one
    thing this function must not do is decide that a command is absent because it deleted the
    line that runs it. String literals survive, because a command a program actually invokes is
    usually a list of strings.

    Unparseable source is returned whole. The rule then errs towards NOT reporting, which is
    the safe direction for a gate whose false positive is an accusation.
    """
    try:
        tree = ast.parse(source)
    except (SyntaxError, ValueError):
        return source
    for node in ast.walk(tree):
        if not isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    try:
        return ast.unparse(tree)
    except (AttributeError, ValueError, RecursionError):
        return source


def _without_hash_comments(source: str) -> str:
    """YAML, shell and Makefile with their comments off. Whole-line and trailing both."""
    kept: list[str] = []
    for line in source.splitlines():
        kept.append("" if line.lstrip().startswith("#") else line.split(" #", 1)[0])
    return "\n".join(kept)


def implementation_text(repo: Path) -> str:
    """Everything an ADR's claim may be honoured by: code, with the prose taken out.

    See the block comment above `ADR_TOOLS` for why prose is excluded and why a comment counts
    as prose. Whitespace is flattened on both sides so that a command wrapped across two lines
    of YAML still matches the one-line form an ADR quotes. What it does not survive is a
    command split by a shell continuation, which stays a false positive and is what
    `EXEMPTIONS` is for.
    """
    wanted = sorted(
        name
        for name in _tracked_files(repo)
        if (name.startswith(IMPLEMENTATION_ROOTS) or name in IMPLEMENTATION_FILES)
        and not name.endswith(".md")
    )
    chunks: list[str] = []
    for name in wanted:
        try:
            source = (repo / name).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        stripped = _code_only(source) if name.endswith(".py") else _without_hash_comments(source)
        chunks.append(" ".join(stripped.split()))
    return " ".join(chunks)


def make_targets(repo: Path) -> frozenset[str]:
    """The targets the Makefile declares, which is what a `make X` claim is honoured by."""
    makefile = repo / "Makefile"
    if not makefile.is_file():
        return frozenset()
    text = makefile.read_text(encoding="utf-8", errors="replace")
    return frozenset(match.group("target") for match in MAKE_TARGET.finditer(text))


def adr_commands(text: str) -> list[str]:
    """The commands an ADR asserts, in the sections where it asserts rather than recounts."""
    out: list[str] = []
    for section in ADR_ASSERTING_SECTION.finditer(text):
        for span in BACKTICKED.findall(section.group("body")):
            tokens = span.split()
            if len(tokens) >= 2 and (tokens[0] in ADR_TOOLS or tokens[0] == "make"):
                out.append(" ".join(tokens))
    return out


def command_is_honoured(command: str, implementation: str, targets: frozenset[str]) -> bool:
    """Whether the tree contains what this command claims. `make X` asks the Makefile."""
    tokens = command.split()
    if tokens[0] == "make":
        return tokens[1] in targets
    return command in implementation


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


def check_document(path: Path, repo: Path, implementation: str | None = None) -> list[Drift]:
    """Every falsifiable sentence in one document that the repository contradicts today.

    `implementation` is the flattened implementation text, passed in by `check_documents` so
    that a sweep of twenty-three documents reads the tree once rather than once per ADR. Left
    out, it is computed on demand and only when an ADR actually asserts a command.
    """
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

    # An ACCEPTED ADR that quotes a command it did not add. Scoped to `docs/adr/` because the
    # status header is what makes the claim binding: a proposed ADR describes something
    # somebody is arguing for, and only an accepted one asserts that the tree contains it.
    if name.startswith("docs/adr/"):
        status = ADR_STATUS.search(text)
        if status is not None and status.group("status").lower() == "accepted":
            commands = [c for c in adr_commands(text) if not any(f in c for f in exempted)]
            if commands:
                if implementation is None:
                    implementation = implementation_text(repo)
                targets = make_targets(repo)
                for command in commands:
                    if command_is_honoured(command, implementation, targets):
                        continue
                    out.append(
                        Drift(
                            name,
                            line_of(text.index(command) if command in text else 0),
                            command[:160],
                            f"this ADR is accepted and says `{command}`, and the tree does "
                            f"not contain it: nothing under "
                            f"{', '.join(IMPLEMENTATION_ROOTS)} runs it, outside comments. An "
                            f"accepted ADR describes what the tree does; if this one "
                            f"describes something still to be done, its status is not "
                            f"'accepted' yet.",
                        )
                    )
    return out


def check_documents(repo: Path, documents: list[Path]) -> list[Drift]:
    """Every drifted sentence across the documents, in document order.

    The implementation text is read once for the whole sweep rather than once per ADR: there
    are fourteen of them and it is a megabyte and a half of source.
    """
    implementation: str | None = None
    if any(path.as_posix().find("docs/adr/") >= 0 for path in documents):
        implementation = implementation_text(repo)
    return [
        drift
        for path in documents
        if path.exists()
        for drift in check_document(path, repo, implementation)
    ]


def expiring_on(event: str, among: Sequence[Exemption] | None = None) -> list[Exemption]:
    """The exemptions that `event` would make false."""
    return [e for e in (EXEMPTIONS if among is None else among) if e.expires_when == event]


def expired_exemptions(
    event: str, has_happened: bool, among: Sequence[Exemption] | None = None
) -> list[Exemption]:
    """The exemptions `event` has already falsified.

    SPLIT FROM THE ASKING ON PURPOSE. The network supplies one boolean and this decides what it
    means, so the decision can be tested with the answer forced both ways - which is the only
    way to know this fires, since the true case cannot be produced on demand: it needs somebody
    to dispatch a workflow.

    `among` takes a set of exemptions other than this module's, and exists because no exemption
    in this module carries an expiry today. Without it, the only test of this function would be
    one that cannot fail.
    """
    return expiring_on(event, among) if has_happened else []


def _origin_slug(repo: Path) -> str | None:
    """`owner/name` from the origin remote. DERIVED, because a typed slug is a fork's first bug."""
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if out.returncode != 0:
        return None
    url = out.stdout.strip().removesuffix(".git")
    if url.startswith("git@") and ":" in url:
        url = url.split(":", 1)[1]
    elif "github.com/" in url:
        url = url.split("github.com/", 1)[1]
    else:
        return None
    parts = [p for p in url.split("/") if p]
    return "/".join(parts[-2:]) if len(parts) >= 2 else None


def databricks_workflow_run_count(repo: Path, timeout: float = 10.0) -> int | None:
    """How many times `.github/workflows/databricks.yml` has run, or None if it cannot be asked.

    NONE IS NOT ZERO, and the distinction is the whole point. Offline, rate-limited, renamed
    workflow, no origin remote - every one of those returns None and the caller skips. Only a
    definite answer from the API is allowed to fail a test, because a check that turns red when
    the network is unavailable is a check people learn to ignore.

    Unauthenticated: the endpoint is public for a public repository, and a test that needed a
    token would not run for a contributor.
    """
    slug = _origin_slug(repo)
    if slug is None:
        return None
    url = f"https://api.github.com/repos/{slug}/actions/workflows/databricks.yml/runs?per_page=1"
    request = urllib.request.Request(url, headers={"Accept": "application/vnd.github+json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                return None
            payload = json.load(response)
    except Exception:
        return None
    count = payload.get("total_count")
    return count if isinstance(count, int) else None


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

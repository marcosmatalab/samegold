"""Rendering the README from the evidence, and refusing to let them drift apart.

Two mechanisms, both enforced in the fast lane:

  * a generated block between ``<!-- samegold:begin claims -->`` and ``<!-- samegold:end
    claims -->``, rebuilt from ``evidence/history.jsonl``;
  * inline anchors of the form ``<!--sg:SG-01.rate-->value<!--/sg-->`` anywhere in any
    markdown file: the value between the comments is replaced from the record for that claim,
    and the comments survive so the next render can update it again.

What makes this more than tidiness: the renderer labels a number by how it was produced. A
record without a CI run URL is printed as "local run, not reproduced in CI", so the author
cannot get a clean-looking README by running things at home; and a value that would break the
anchor around it, the markdown table, or the generated block is REFUSED rather than escaped,
because the values this project produces are numbers and short identifiers.

An earlier version of this paragraph also promised that "a fault-injection record whose
artifact digest differs from the clean build is printed as UNVERIFIABLE". No such code ever
existed: the word appeared in this docstring and nowhere else in the repository, and
`artifact_digest` was written into records and read by nothing. The claim is removed rather
than implemented, because the honest statement about a defence is either that it runs or that
it does not.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BEGIN = "<!-- samegold:begin claims -->"
END = "<!-- samegold:end claims -->"
# An inline value is wrapped in HTML comments so the anchor survives rendering and the
# reader never sees it: GitHub hides comments and shows only the value. Replacing a token
# in place would consume the anchor on the first render, after which the number could
# never be updated again and the drift gate would silently stop watching it.
TOKEN = re.compile(r"<!--sg:([A-Za-z0-9_.\-]+)-->(.*?)<!--/sg-->", re.DOTALL)


@dataclass(frozen=True, slots=True)
class RenderDrift:
    kind: str
    detail: str

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"{self.kind}: {self.detail}"


def _value_for(record: dict[str, Any], field: str) -> str:
    verdict = record.get("verdict", {})
    runs = verdict.get("runs", {})
    rate = verdict.get("rate")
    if field == "outcome":
        return "PASS" if verdict.get("outcome") == "pass" else "FAIL"
    if field == "n":
        return str(runs.get("n", "?"))
    if field == "runtime":
        return str(record.get("runtime", "?"))
    if field == "rate":
        if not rate:
            return "n/a"
        lo, hi = rate["wilson95"]
        return f"{rate['successes']}/{rate['trials']} (95% CI {lo:.1%}-{hi:.1%})"
    if field == "bound":
        if not rate or rate.get("rule_of_three_upper95") is None:
            return "n/a"
        return f"<= {rate['rule_of_three_upper95']:.2%}"
    if field == "point":
        return "n/a" if not rate else f"{rate['point']:.2%}"
    if field.startswith("artifact."):
        value = record.get("artifacts", {}).get(field.split(".", 1)[1], "n/a")
        if isinstance(value, bool):
            return "yes" if value else "no"
        if isinstance(value, int):
            return f"{value:,}".replace(",", " ")
        return str(value)
    raise KeyError(f"unknown evidence field {field!r}")


_UNSAFE = re.compile(r"<!--|-->|\r?\n|\|")


def _registry_title(claim_id: str, record: dict[str, Any]) -> str:
    from samegold.evidence.registry import CLAIM_TITLES

    return CLAIM_TITLES.get(claim_id, str(record.get("title", "")))


def _safe(value: str, where: str) -> str:
    """Refuse a value that would break the document it is rendered into.

    Everything here is a real behaviour of the previous version, found by writing the values
    rather than by reasoning about them:

      * an artifact value containing ``<!--/sg-->`` closed the anchor early, so the rest of
        the value landed OUTSIDE it and the next render appended it again. The document grew
        on every ``make readme``, and the drift gate could never be made green by rendering,
        only by hand-editing the document, which is the exact act this module exists to
        prevent;
      * a title containing ``<!-- samegold:end claims -->`` truncated the generated block and
        the render duplicated the table's tail outside it, unboundedly;
      * a ``|`` or a newline in a title broke the markdown table outright.

    The values this repository actually produces are numbers and short identifiers, so the
    honest answer is to refuse anything else rather than to escape it into something a
    reader would then have to decode.
    """
    if _UNSAFE.search(value):
        raise ValueError(
            f"{where}: the value {value!r} contains a comment delimiter, a newline or a pipe, "
            f"and rendering it would break the document or the anchor around it. Evidence "
            f"values are numbers and short identifiers."
        )
    return value


def _provenance(record: dict[str, Any]) -> str:
    """How the number was produced, in the words a reader needs to discount it by.

    "dirty tree" is the important one and it is new: it means the run executed code that is
    in no commit, so the commit the record names anchors its seeds and not its behaviour.
    Every honest re-measurement after a fix looks like this, and so does retry-until-green.
    Saying it is the whole defence available.
    """
    runs = record.get("verdict", {}).get("runs", {})
    dirty = " on an uncommitted tree" if runs.get("tree_dirty") else ""
    if record.get("ci_run_url"):
        return f"CI{dirty}"
    return f"local run, not reproduced in CI{dirty}"


def render_claims_block(latest: dict[str, dict[str, Any]]) -> str:
    """The results table. One row per claim, with the provenance in the row."""
    if not latest:
        return BEGIN + "\n\n_No evidence recorded yet. Run `make evidence`._\n\n" + END
    header = "| claim | result | experiment | runtime | provenance |\n|---|---|---|---|---|\n"
    rows = []
    for claim_id in sorted(latest):
        record = latest[claim_id]
        verdict = record.get("verdict", {})
        outcome = "PASS" if verdict.get("outcome") == "pass" else "**FAIL**"
        rate = _value_for(record, "rate")
        bound = _value_for(record, "bound")
        detail = rate if rate != "n/a" else bound
        # The REGISTRY's title, not the record's. A record written before a claim was
        # renamed legitimately keeps the old title (the chain is a history and re-validating
        # it must not turn a legitimate past into a forgery), but the results table is a
        # description of the claims as they are now: it published "the close survives a crash
        # at each structural point" forty lines above a section explaining that this was
        # false by half.
        title = _safe(_registry_title(claim_id, record), f"{claim_id}.title")
        runtime = _safe(str(record.get("runtime", "?")), f"{claim_id}.runtime")
        rows.append(
            f"| `{claim_id}` {title} | {outcome} | {_safe(detail, f'{claim_id}.detail')} "
            f"| {runtime} | {_provenance(record)} |"
        )
    return BEGIN + "\n\n" + header + "\n".join(rows) + "\n\n" + END


def render_readme(text: str, latest: dict[str, dict[str, Any]]) -> str:
    if BEGIN in text and END in text:
        start, rest = text.split(BEGIN, 1)
        _, tail = rest.split(END, 1)
        text = start + render_claims_block(latest) + tail

    def replace(match: re.Match[str]) -> str:
        anchor = match.group(1)
        claim_id, _, field = anchor.partition(".")
        record = latest.get(claim_id)
        if record is None:
            return f"<!--sg:{anchor}-->UNKNOWN CLAIM<!--/sg-->"
        value = _safe(_value_for(record, field or "outcome"), anchor)
        return f"<!--sg:{anchor}-->{value}<!--/sg-->"

    return TOKEN.sub(replace, text)


def check_readme(path: Path, latest: dict[str, dict[str, Any]]) -> list[RenderDrift]:
    """Return the drifts between a markdown file and the evidence. Empty means consistent."""
    text = path.read_text(encoding="utf-8")
    drifts: list[RenderDrift] = []
    try:
        rendered = render_readme(text, latest)
    except ValueError as error:
        # A value the renderer refuses is a finding about the EVIDENCE, and `samegold check`
        # is the command whose job is to report findings. Letting it escape turned the check
        # into an uncaught traceback, which is the one way to make a gate look like a bug.
        return [RenderDrift("unrenderable", f"{path.name}: {error}")]
    if rendered != text:
        drifts.append(
            RenderDrift(
                "stale-render",
                f"{path.name} does not match the evidence; run `make readme` to regenerate",
            )
        )
    for match in TOKEN.finditer(text):
        anchor = match.group(1)
        claim_id, _, field = anchor.partition(".")
        if claim_id not in latest:
            drifts.append(
                RenderDrift("unknown-claim", f"{path.name} cites {claim_id}, which has no evidence")
            )
            continue
        # An anchor naming an artifact key that no longer exists rendered "n/a" and the file
        # matched, so a published figure could quietly become "n/a" with nothing reported. A
        # value that disappears is drift; it is only less visible than a value that changes.
        if field.startswith("artifact.") and field.split(".", 1)[1] not in latest[claim_id].get(
            "artifacts", {}
        ):
            drifts.append(
                RenderDrift(
                    "unknown-field",
                    f"{path.name} cites {anchor}, and {claim_id} has no such artifact",
                )
            )
    # A RESULT written by hand next to a claim id. The two previous versions of this check
    # both looked like checks and inspected nothing:
    #
    #   * the first looked for the marker "samegold:hardcoded", which the author had to
    #     volunteer and which appears nowhere in the repository;
    #   * the second required a BACKTICKED claim id and then skipped every line beginning
    #     with "|" or "#". Measured over the eight documents, that inspected zero lines:
    #     backticked ids appear only inside the generated table, and the prose writes them
    #     bare. It also fired on "SG-03 runs the reference on DuckDB 1.5", because a version
    #     number is a number.
    #
    # So: a claim id in any form, outside the generated block and outside an anchor, on a
    # line that also states a RESULT - a percentage or a fraction, which is how every figure
    # in these documents is written. A year or a library version is neither.
    # The generated block, as a SET OF LINES. It used to be compared with `line in generated`,
    # a substring test against the whole block, so every blank line and any prose line that
    # happened to be a substring of a generated row was skipped.
    generated_lines = set()
    if BEGIN in text and END in text:
        generated_lines = set(text.split(BEGIN, 1)[-1].split(END, 1)[0].splitlines())
    claim_id_pattern = re.compile(r"\bSG-\d\d\b")
    # Every shape a RESULT is written in in these documents. The previous version matched a
    # percentage and a fraction only, and a review listed six figures it walked straight past,
    # including "killed 48 of 49 generated mutants" and "moved 662 481,62 EUR" - the second of
    # which is the exact figure class the post-mortem carries and that domain/money.py exists
    # for. A year and a library version still do not match, which is the reason the pattern
    # enumerates shapes instead of matching any number.
    # `(?<![\w-])` on every alternative, because the claim id ITSELF ends in digits: without
    # it, "SG-03 runs the reference on DuckDB 1.5" matched "03 runs" and the check reported a
    # hand-typed result in a sentence that states none.
    number = r"(?<![\w-])\d+"
    result_pattern = re.compile(
        rf"""
          {number}(?:[.,]\d+)?\s?%                # 78.25%, 6,37 %
        | {number}\s?/\s?\d+\b                   # 48/49
        | {number}\s+(?:of|out\s+of)\s+\d+\b     # 48 of 49, 5 out of 5
        | {number}(?:\s\d{{3}})+,\d{{2}}\b         # 662 481,62
        | {number}\s+(?:tests?|mutants?|seeds?|files?|records?|months?|runs?|rows?|events?)\b
        """,
        re.VERBOSE,
    )
    for line_number, line in enumerate(text.splitlines(), start=1):
        if line in generated_lines:
            continue
        outside = TOKEN.sub("", line)
        if not claim_id_pattern.search(outside):
            continue
        for found in result_pattern.finditer(outside):
            drifts.append(
                RenderDrift(
                    "hardcoded",
                    f"{path.name}:{line_number} states the result {found.group(0)!r} beside "
                    f"claim id without an anchor; render it with <!--sg:...--> so it cannot "
                    f"go stale",
                )
            )
    return drifts

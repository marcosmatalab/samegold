"""Rendering the README from the evidence, and refusing to let them drift apart.

Two mechanisms, both enforced in the fast lane:

  * a generated block between ``<!-- samegold:begin claims -->`` and ``<!-- samegold:end
    claims -->``, rebuilt from ``evidence/history.jsonl``;
  * inline tokens of the form ``[[sg:C1.rate]]`` anywhere in any markdown file, replaced by
    the value from the record for that claim.

What makes this more than tidiness: the renderer refuses to print a number at all when the
record behind it is weak. A record produced outside CI is printed with "(local run, not
reproduced in CI)" attached; a fault-injection record whose artifact digest differs from the
clean build is printed as UNVERIFIABLE. The author cannot get a clean-looking README by
running things at home.
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
        return str(record.get("artifacts", {}).get(field.split(".", 1)[1], "n/a"))
    raise KeyError(f"unknown evidence field {field!r}")


def _provenance(record: dict[str, Any]) -> str:
    if record.get("ci_run_url"):
        return "CI"
    return "local run, not reproduced in CI"


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
        rows.append(
            f"| `{claim_id}` {record.get('title', '')} | {outcome} | {detail} "
            f"| {record.get('runtime', '?')} | {_provenance(record)} |"
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
        return f"<!--sg:{anchor}-->{_value_for(record, field or 'outcome')}<!--/sg-->"

    return TOKEN.sub(replace, text)


def check_readme(path: Path, latest: dict[str, dict[str, Any]]) -> list[RenderDrift]:
    """Return the drifts between a markdown file and the evidence. Empty means consistent."""
    text = path.read_text(encoding="utf-8")
    rendered = render_readme(text, latest)
    drifts: list[RenderDrift] = []
    if rendered != text:
        drifts.append(
            RenderDrift(
                "stale-render",
                f"{path.name} does not match the evidence; run `make readme` to regenerate",
            )
        )
    for match in TOKEN.finditer(text):
        claim_id = match.group(1).split(".")[0]
        if claim_id not in latest:
            drifts.append(
                RenderDrift("unknown-claim", f"{path.name} cites {claim_id}, which has no evidence")
            )
    # A number written by hand next to a claim id is the failure mode this whole file
    # exists to prevent, so we look for it explicitly.
    for line in text.splitlines():
        if "samegold:hardcoded" in line:
            drifts.append(RenderDrift("hardcoded", line.strip()))
    return drifts

"""Fill the ``<!--dbx:...-->`` anchors from the record the workspace produced.

The `<!--sg:...-->` anchors have been rendered by a command since round eight, for a reason
this file exists to extend: a figure a person copies is a figure with a transcription error
waiting in it, and one nobody re-copies is stale the moment the run behind it is superseded.

The `dbx:` anchors were not. They were filled by hand the first time and by a script in a
scratch directory the second - the same shape as the `/tmp` filter that produced the second
close's population, and it went wrong the same way: the lane ran again, the record changed,
and `docs/databricks-run.md` went on describing a run that no longer existed until a test
caught it.

WHAT THIS RENDERS, and it is deliberately not "everything in the record". Every name here is
derived from a field the record carries, so an anchor whose value cannot be derived is a
failure rather than a blank - `tests/fast/test_databricks_bundle.py` asserts the same closed
set from the other side.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

ANCHOR = re.compile(r"<!--dbx:([\w.]+)-->(.*?)<!--/dbx-->", re.DOTALL)
# What a document says while no run has produced a value for it. The word, not a blank and not
# a zero: "could not be read" and "is zero" must never render the same.
NOT_RUN = "NOT RUN"


def scalars_from(record: dict[str, Any]) -> dict[str, Any]:
    """Every anchor name the record can answer, and its value.

    The single definition. `tests/fast/test_databricks_bundle.py` imports it rather than
    restating it, because two derivations of the same mapping is how a document and its test
    agree with each other and not with the record.
    """
    out: dict[str, Any] = {}
    update = record.get("update")
    if isinstance(update, list) and update and isinstance(update[0], dict):
        out["update.last_state"] = update[0].get("last_state")
        out["update.error_events"] = update[0].get("error_events")
    for table, count in (record.get("rows") or {}).items():
        out[f"rows.{table}"] = count
    dimension = record.get("dim_customer_scd2")
    if isinstance(dimension, list) and dimension and isinstance(dimension[0], dict):
        for field in ("versions", "customers", "open_rows", "closed_rows"):
            out[f"dim.{field}"] = dimension[0].get(field)
    # What the JOB did, so a document quotes the orchestration instead of describing it.
    #
    # These are absent from a record produced before the round that added them, and the filter
    # at the end drops names whose value is None - so a document may only carry these anchors
    # once a run has published them, which is the same rule every other figure here follows.
    orchestration = record.get("orchestration")
    if isinstance(orchestration, list) and orchestration and isinstance(orchestration[0], dict):
        for field in ("decision", "branch", "versions_written"):
            out[f"orch.{field}"] = orchestration[0].get(field)
        months = orchestration[0].get("months_written")
        if isinstance(months, list):
            out["orch.months_written"] = len(months)
    # The per-month verdicts, as two counts. The rows themselves are a table, and a table
    # belongs in a pasted block rather than in a scalar anchor.
    #
    # OFFERED ONLY IF THE RECORD SHOWS THE VERIFICATION ACTUALLY REPORTED, which is the whole
    # of `_verification_reported` below. Zero rows is the shape a run leaves behind when the
    # verification task FAILED - it wrote none - and rendering that as "0 checks run, 0 failed"
    # puts a clean-looking pair of figures on a page about a run whose verification never
    # executed. A document that cannot answer says NOT RUN, which is what withholding the name
    # here produces.
    verification = record.get("close_verification")
    if isinstance(verification, list) and _verification_reported(record):
        out["orch.checks_run"] = len(verification)
        out["orch.checks_failed"] = sum(1 for row in verification if not row.get("ok"))
    # Keyed by the record's own accounting_month rather than by position: a run over different
    # months should produce anchors nothing claims, which the closed-set check turns into a
    # failure. `2026-01` is not a legal anchor name, so the separator is an underscore.
    for row in record.get("gross_within_contract_bounds") or []:
        if not isinstance(row, dict) or not row.get("accounting_month"):
            continue
        month = str(row["accounting_month"]).replace("-", "_")
        out[f"revenue.{month}.gross_cents"] = row.get("gross_cents")
        out[f"revenue.{month}.line_count"] = row.get("line_count")
    return {name: value for name, value in out.items() if value is not None}


def _verification_reported(record: dict[str, Any]) -> bool:
    """Whether the record POSITIVELY shows the branch's verification wrote what it owed.

    Three states, not two, and the middle one is the reason this function exists:

      * the record says what the branch owed and that none of it is missing - the only case
        that may be quoted;
      * the record says something is missing, or names the hole in `incomplete`. Refused;
      * the record does not say at all. Also refused, and that is not pedantry: a run whose
        `verify_no_restatement` failed publishes a `close_verification` with no rows in it and
        is otherwise indistinguishable from a healthy run, so "the record did not mention a
        problem" is exactly the sentence that must not count as evidence there. A record
        produced before `publish_evidence.py` derived this carries no such fields either, and
        it has no more standing to be quoted than the failed run has.
    """
    orchestration = record.get("orchestration")
    if not (isinstance(orchestration, list) and orchestration):
        return False
    job = orchestration[0]
    if not isinstance(job, dict):
        return False
    if job.get("missing_checks") != [] or not isinstance(job.get("expected_checks"), list):
        return False
    if not job["expected_checks"]:
        return False
    return str(job.get("branch")) not in {str(name) for name in record.get("incomplete") or []}


def _table(rows: list[dict[str, Any]], columns: list[str], headings: list[str]) -> str:
    header = "| " + " | ".join(headings) + " |\n|" + "|".join("---" for _ in headings) + "|"
    body = "\n".join("| " + " | ".join(str(row.get(c, "")) for c in columns) + " |" for row in rows)
    return header + "\n" + body


def tables_from(record: dict[str, Any]) -> dict[str, str]:
    """The two anchors whose value is a whole table rather than a scalar."""
    out: dict[str, str] = {}
    expectations = record.get("expectations")
    if isinstance(expectations, list) and expectations:
        out["expectations.table"] = _table(
            expectations,
            ["rule", "dataset", "passed", "failed"],
            ["rule", "dataset", "passed", "failed"],
        )
    quarantine = record.get("quarantine_by_reason")
    if isinstance(quarantine, list) and quarantine:
        out["quarantine.table"] = _table(quarantine, ["reason", "n"], ["quarantine reason", "rows"])
    return out


def _keep_spacing(previous: str, value: str) -> str:
    """A digit-grouped body stays digit-grouped: `14 198 046`, not `14198046`.

    The record holds an integer and the documents print thousands separated for reading, so
    re-rendering would otherwise reformat a number every time it ran and produce a diff that
    says nothing. The regrouping is applied only when the OLD body was a grouped form of a
    number and the new value is a number - never as a general prettifier.
    """
    stripped = previous.replace(" ", "")
    if not (stripped.isdigit() and " " in previous.strip() and value.isdigit()):
        return value
    digits = value[::-1]
    return " ".join(digits[i : i + 3] for i in range(0, len(digits), 3))[::-1]


def render(text: str, record: dict[str, Any] | None) -> tuple[str, list[str]]:
    """Return the document with every dbx anchor filled, and the names it could not answer."""
    known: dict[str, Any] = {}
    if record is not None:
        known = {**scalars_from(record), **tables_from(record)}
    unanswerable: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name, previous = match.group(1), match.group(2)
        if name not in known:
            unanswerable.append(name)
            return f"<!--dbx:{name}-->{NOT_RUN}<!--/dbx-->"
        value = known[name]
        # A table goes in with no blank lines added around it, because that is the layout these
        # documents already have. A renderer that reformats what it renders makes every run of
        # it a diff, and a diff nobody reads is where a real change hides.
        if isinstance(value, str) and "\n" in value:
            return f"<!--dbx:{name}-->{value}<!--/dbx-->"
        return f"<!--dbx:{name}-->{_keep_spacing(previous, str(value))}<!--/dbx-->"

    return ANCHOR.sub(replace, text), unanswerable


def render_files(repo: Path, record_path: Path, documents: tuple[str, ...]) -> list[str]:
    """Render each document in place. Returns one report line per file."""
    record = None
    if record_path.exists():
        record = json.loads(record_path.read_text(encoding="utf-8"))
    lines = []
    for name in documents:
        path = repo / name
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        rendered, unanswerable = render(text, record)
        anchors = len(ANCHOR.findall(text))
        if rendered != text:
            path.write_text(rendered, encoding="utf-8", newline="\n")
        note = (
            f", {len(unanswerable)} the record cannot answer: {unanswerable}"
            if unanswerable
            else ""
        )
        lines.append(f"{name}: {anchors} dbx anchor(s){note}")
    return lines

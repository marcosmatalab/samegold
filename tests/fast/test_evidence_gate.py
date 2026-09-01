"""The gate that keeps the documents and the evidence from drifting apart."""

from __future__ import annotations

import json
from pathlib import Path

from samegold.evidence.record import EvidenceRecord
from samegold.evidence.render import BEGIN, END, check_readme, render_readme
from samegold.evidence.store import EvidenceStore
from samegold.verify.verdict import Pass, Rate, RunSet

REPO = Path(__file__).resolve().parents[2]


def _record(claim_id: str = "SG-99", successes: int = 9, trials: int = 10) -> EvidenceRecord:
    runs = RunSet(
        n=2,
        seeds=(1, 2),
        commit_sha="a" * 40,
        seed_source="commit",
        profile="fast",
        started_at="2026-09-01T00:00:00+00:00",
        duration_s=1.0,
        runtime="oss-local",
    )
    return EvidenceRecord(
        claim_id=claim_id,
        title="a test claim",
        verdict=Pass(claim_id, runs, Rate(successes, trials)),
        runtime="oss-local",
        ci_run_url=None,
    )


def test_a_value_anchor_is_replaced_and_survives_rendering(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record())
    text = "score: <!--sg:SG-99.rate-->?<!--/sg--> done\n"
    once = render_readme(text, store.latest())
    assert "9/10" in once
    # Idempotence is the property that matters: the anchor must still be there so the next
    # run can update the number. A token that is consumed on first render cannot drift, but
    # it also cannot ever be corrected.
    assert render_readme(once, store.latest()) == once
    assert "<!--sg:SG-99.rate-->" in once


def test_a_changed_number_is_detected_as_drift(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record())
    document = tmp_path / "DOC.md"
    document.write_text(render_readme("score: <!--sg:SG-99.rate-->?<!--/sg-->\n", store.latest()))
    assert check_readme(document, store.latest()) == []
    document.write_text(document.read_text().replace("9/10", "10/10"))
    drifts = check_readme(document, store.latest())
    assert drifts and drifts[0].kind == "stale-render"


def test_a_claim_with_no_evidence_is_reported(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    document = tmp_path / "DOC.md"
    document.write_text("see <!--sg:SG-42.rate-->x<!--/sg-->\n")
    assert any(d.kind == "unknown-claim" for d in check_readme(document, store.latest()))


def test_provenance_is_printed_for_a_local_run(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record())
    block = render_readme(f"{BEGIN}\n{END}\n", store.latest())
    assert "local run, not reproduced in CI" in block


def test_history_is_append_only_and_keeps_failures(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record("SG-98"))
    store.append(_record("SG-98", successes=8))
    lines = store.history.read_text().splitlines()
    assert len(lines) == 2, "history.jsonl must never be rewritten in place"
    assert store.latest()["SG-98"]["verdict"]["rate"]["successes"] == 8


def test_the_repository_documents_are_consistent_with_its_evidence() -> None:
    """The real gate, on the real files. This is the test that fails on a hand-edited number."""
    store = EvidenceStore(REPO / "evidence")
    for name in ("README.md", "CLAIMS.md"):
        assert check_readme(REPO / name, store.latest()) == [], f"{name} drifted"


def test_every_claim_cited_in_the_documents_exists() -> None:
    from samegold.claims import ALL_CLAIMS

    known = set(ALL_CLAIMS) | {"SG-07", "SG-08", "SG-09"}
    for name in ("README.md", "CLAIMS.md", "EXAM_MAP.md", "PARITY.md"):
        for line in (REPO / name).read_text().splitlines():
            for token in line.replace("`", " ").replace(",", " ").split():
                if token.startswith("SG-") and token[:5].endswith(tuple("0123456789")):
                    assert token[:5] in known, f"{name} cites unknown claim {token}"


def test_the_evidence_directory_is_committed() -> None:
    """A repository whose evidence is gitignored is a repository with no evidence."""
    gitignore = (REPO / ".gitignore").read_text()
    assert "evidence/" not in gitignore.replace("# evidence/", "")
    assert (REPO / "evidence" / "history.jsonl").exists()
    assert (
        json.loads((REPO / "evidence" / "runs" / "SG-01.json").read_text())["claim_id"] == "SG-01"
    )

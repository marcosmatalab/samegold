"""The gate that stops the evidence and the documents from lying.

Every test below the first three is a reproduction of an attack that WORKED against the
first version of this repository: an adversarial reviewer appended two records by hand
claiming 999/999 agreements and a 100% mutation score, pointed one at a fabricated CI run,
regenerated the documents and ran the whole suite. 152 tests passed. These are the tests
that fail now.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from samegold.evidence.record import EvidenceRecord
from samegold.evidence.registry import CLAIM_TITLES
from samegold.evidence.render import BEGIN, END, check_readme, render_readme
from samegold.evidence.store import EvidenceRejected, EvidenceStore, record_hash
from samegold.generator.seeds import current_commit_sha, current_tree, seeds_from_commit
from samegold.verify.verdict import Pass, Rate, RunSet

REPO = Path(__file__).resolve().parents[2]


def _record(
    # A REAL claim id and its real title. The gate now checks that a record is about a claim
    # this repository defines and that it has not renamed it, so a fixture using "SG-01" and
    # "a test claim" would be refused by the identity check before it ever reached the rule
    # each test below is actually about.
    claim_id: str = "SG-01",
    successes: int = 9,
    trials: int = 10,
    seeds: tuple[int, ...] | None = None,
    ci_run_url: str | None = None,
) -> EvidenceRecord:
    sha = current_commit_sha()
    real_seeds = seeds if seeds is not None else tuple(seeds_from_commit(2, "witness", sha=sha))
    runs = RunSet(
        n=len(real_seeds),
        seeds=real_seeds,
        commit_sha=sha,
        tree_sha=current_tree()[0],
        tree_dirty=current_tree()[1],
        seed_source="commit",
        seed_purpose="witness",
        profile="fast",
        started_at="2026-09-01T00:00:00+00:00",
        duration_s=1.0,
        runtime="oss-local",
    )
    return EvidenceRecord(
        claim_id=claim_id,
        title=CLAIM_TITLES[claim_id],
        verdict=Pass(claim_id, runs, Rate(successes, trials)),
        runtime="oss-local",
        ci_run_url=ci_run_url,
        ci_commit_sha=None,
    )


# --------------------------------------------------------------- rendering


def test_a_value_anchor_is_replaced_and_survives_rendering(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record())
    text = "score: <!--sg:SG-01.rate-->?<!--/sg--> done\n"
    once = render_readme(text, store.latest())
    assert "9/10" in once
    # Idempotence is the property that matters: the anchor must still be there so the next
    # run can update the number. A token consumed on first render cannot drift, but it can
    # never be corrected either.
    assert render_readme(once, store.latest()) == once
    assert "<!--sg:SG-01.rate-->" in once


def test_a_changed_number_is_detected_as_drift(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record())
    document = tmp_path / "DOC.md"
    document.write_text(render_readme("score: <!--sg:SG-01.rate-->?<!--/sg-->\n", store.latest()))
    assert check_readme(document, store.latest()) == []
    document.write_text(document.read_text().replace("9/10", "10/10"))
    drifts = check_readme(document, store.latest())
    assert drifts and drifts[0].kind == "stale-render"


def test_provenance_is_printed_for_a_local_run(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record())
    block = render_readme(f"{BEGIN}\n{END}\n", store.latest())
    assert "local run, not reproduced in CI" in block


# --------------------------------------------------------------- the attacks


def test_attack_chosen_seeds_is_rejected(tmp_path: Path) -> None:
    """Attack 2: a record whose seeds were picked by hand rather than derived."""
    store = EvidenceStore(tmp_path)
    with pytest.raises(EvidenceRejected, match="do not derive from commit"):
        store.append(_record(seeds=(7, 7, 7)))


def test_attack_fabricated_ci_url_is_rejected(tmp_path: Path) -> None:
    """A run URL has to look like a GitHub Actions run, and match the record's commit."""
    store = EvidenceStore(tmp_path)
    with pytest.raises(EvidenceRejected, match="not a GitHub Actions run URL"):
        store.append(_record(ci_run_url="https://example.com/looks-official"))


def test_attack_editing_the_history_by_hand_breaks_the_chain(tmp_path: Path) -> None:
    """Attack 1: change a number in history.jsonl and re-render."""
    store = EvidenceStore(tmp_path)
    store.append(_record())
    store.append(_record("SG-02"))
    assert store.verify_chain() == []
    forged = store.history.read_text().replace('"successes": 9', '"successes": 999')
    store.history.write_text(forged)
    breaks = store.verify_chain()
    assert breaks, "a hand-edited record must not verify"
    assert "modified after it was written" in breaks[0].problem


def test_attack_inserting_a_record_breaks_the_chain(tmp_path: Path) -> None:
    """Appending a well-formed record produced elsewhere still breaks the chain."""
    store = EvidenceStore(tmp_path)
    store.append(_record())
    smuggled = _record("SG-03", successes=999, trials=999).to_json()
    smuggled["prev"] = "0" * 32
    smuggled["hash"] = record_hash(smuggled)
    with store.history.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(smuggled, sort_keys=True) + "\n")
    breaks = store.verify_chain()
    assert breaks and "a line was edited, inserted or removed" in breaks[0].problem


def test_attack_deleting_a_record_breaks_the_chain(tmp_path: Path) -> None:
    """A failing run cannot be quietly removed from the history."""
    store = EvidenceStore(tmp_path)
    store.append(_record())
    store.append(_record("SG-02"))
    store.append(_record("SG-03"))
    lines = store.history.read_text().splitlines()
    store.history.write_text("\n".join([lines[0], lines[2]]) + "\n")
    assert store.verify_chain(), "removing the middle record must be visible"


def test_attack_an_override_run_cannot_enter_the_history(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """A refutation run's seeds are not derived from the commit, so nothing can recompute
    them. One used to be accepted straight into the history, and SG-06 then reported it under
    the title "every seed derives from its commit"."""
    monkeypatch.setenv("SAMEGOLD_SEED_OVERRIDE", "lucky-777")
    record = _record()
    object.__setattr__(record.verdict.runs, "seed_source", "override")
    store = EvidenceStore(tmp_path)
    with pytest.raises(EvidenceRejected, match="SAMEGOLD_SEED_OVERRIDE"):
        store.append(record)


def test_attack_a_foreign_ci_url_is_rejected(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    """Pointing a record at somebody else's Actions run used to print it as CI."""
    monkeypatch.setenv("GITHUB_REPOSITORY", "marcosmatalab/samegold")
    store = EvidenceStore(tmp_path)
    record = _record(ci_run_url="https://github.com/torvalds/linux/actions/runs/1")
    object.__setattr__(record, "ci_commit_sha", record.verdict.runs.commit_sha)
    with pytest.raises(EvidenceRejected, match="another repository"):
        store.append(record)


def test_attack_a_ci_url_without_its_commit_is_rejected(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    with pytest.raises(EvidenceRejected, match="ci_commit_sha"):
        store.append(_record(ci_run_url="https://github.com/a/b/actions/runs/12"))


def test_attack_editing_a_runs_file_is_detected(tmp_path: Path) -> None:
    """runs/<claim>.json is what a reader opens. It used to be unchecked."""
    store = EvidenceStore(tmp_path)
    store.append(_record())
    path = store.runs_dir / "SG-01.json"
    payload = json.loads(path.read_text())
    payload["verdict"]["rate"]["successes"] = 999
    path.write_text(json.dumps(payload))
    breaks = store.verify_chain()
    assert breaks and "does not match the latest record" in breaks[0].problem


def test_attack_reordering_the_history_is_detected(tmp_path: Path) -> None:
    """Moving an old PASS after a later FAIL used to make the PASS the latest word."""
    store = EvidenceStore(tmp_path)
    first = _record("SG-03")
    object.__setattr__(first.verdict.runs, "started_at", "2026-01-01T00:00:00+00:00")
    second = _record("SG-03", successes=1)
    object.__setattr__(second.verdict.runs, "started_at", "2026-02-01T00:00:00+00:00")
    store.append(first)
    store.append(second)
    lines = store.history.read_text().splitlines()
    rebuilt = []
    previous = "0" * 32
    for line in reversed(lines):
        payload = json.loads(line)
        payload["prev"] = previous
        payload["hash"] = record_hash(payload)
        previous = payload["hash"]
        rebuilt.append(json.dumps(payload, sort_keys=True))
    store.history.write_text("\n".join(rebuilt) + "\n")
    breaks = store.verify_chain()
    assert any("reordered" in b.problem for b in breaks)


def test_attack_a_record_naming_a_commit_that_does_not_exist(tmp_path: Path) -> None:
    """Nothing stopped a record from naming an invented commit whose derived seeds happened
    to be convenient."""
    from samegold.generator.seeds import seed_for

    store = EvidenceStore(tmp_path)
    # A real record first: the commit anchor only applies to a history that belongs to this
    # checkout, so that a fork or a downloaded tarball is not told its evidence is forged.
    store.append(_record("SG-04"))
    fake = "dead" * 10
    record = _record(seeds=tuple(seed_for(fake, i, "witness") for i in range(2)))
    object.__setattr__(record.verdict.runs, "commit_sha", fake)
    store.append(record)  # the shape is valid; the anchor is what catches it
    breaks = store.verify_chain(REPO)
    assert any("does not exist in this repository" in b.problem for b in breaks)


def test_a_fork_is_not_told_its_evidence_is_forged(tmp_path: Path) -> None:
    """Someone who clones this repository into a fresh history has commits nobody here knows.

    A clean-room check caught exactly that: `samegold check` reported all eleven records as
    naming commits that do not exist, which reads as "this repository is a fraud" and means
    "you are not in the checkout that produced it".
    """
    from samegold.generator.seeds import seed_for

    store = EvidenceStore(tmp_path)
    unknown = "beef" * 10
    record = _record(seeds=tuple(seed_for(unknown, i, "witness") for i in range(2)))
    object.__setattr__(record.verdict.runs, "commit_sha", unknown)
    store.append(record)
    assert store.verify_chain(REPO) == []


# --------------------------------------------------------------- the real files


@pytest.mark.evidence_dependent
def test_the_repository_evidence_chain_verifies() -> None:
    store = EvidenceStore(REPO / "evidence")
    assert store.verify_chain(REPO) == []


@pytest.mark.evidence_dependent
def test_the_repository_documents_are_consistent_with_its_evidence() -> None:
    store = EvidenceStore(REPO / "evidence")
    for name in ("README.md", "CLAIMS.md"):
        assert check_readme(REPO / name, store.latest()) == [], f"{name} drifted"


def test_history_is_append_only_and_keeps_failures(tmp_path: Path) -> None:
    store = EvidenceStore(tmp_path)
    store.append(_record("SG-02"))
    store.append(_record("SG-02", successes=8))
    lines = store.history.read_text().splitlines()
    assert len(lines) == 2, "history.jsonl must never be rewritten in place"
    assert store.latest()["SG-02"]["verdict"]["rate"]["successes"] == 8


def test_every_claim_cited_in_the_documents_exists() -> None:
    from samegold.claims import ALL_CLAIMS

    known = set(ALL_CLAIMS) | {"SG-07", "SG-08", "SG-09"}
    for name in ("README.md", "CLAIMS.md", "EXAM_MAP.md", "PARITY.md"):
        for line in (REPO / name).read_text().splitlines():
            for token in line.replace("`", " ").replace(",", " ").split():
                if token.startswith("SG-") and token[:5].endswith(tuple("0123456789")):
                    assert token[:5] in known, f"{name} cites unknown claim {token}"


@pytest.mark.evidence_dependent
def test_the_evidence_directory_is_committed() -> None:
    """A repository whose evidence is gitignored is a repository with no evidence."""
    gitignore = (REPO / ".gitignore").read_text()
    assert "evidence/" not in gitignore.replace("# evidence/", "")
    assert (REPO / "evidence" / "history.jsonl").exists()
    assert (
        json.loads((REPO / "evidence" / "runs" / "SG-01.json").read_text())["claim_id"] == "SG-01"
    )

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

from samegold.claims import claim_seed_provenance
from samegold.evidence.record import EvidenceRecord
from samegold.evidence.registry import CLAIM_TITLES
from samegold.evidence.render import BEGIN, END, check_readme, render_readme
from samegold.evidence.reproduce import arithmetic_mismatches
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


def test_the_provenance_names_the_commit_that_produced_the_number() -> None:
    """A figure on the front page is a measurement of SOME version of this repository.

    The provenance column said "CI" or "local run, not reproduced in CI" and stopped there, so
    a reader who re-ran a claim and got a different answer could not tell whether they had
    found a defect, a different seed, or simply a later commit. The documents quote the HEAD of
    an append-only chain and the head moves - most visibly whenever the generator changes,
    which changes the population every count describes.

    On a dirty tree the commit is not enough on its own, because it names code that is not what
    ran; the tree hash goes beside it. ADR 0010 is the policy, this is the half a reader sees.
    """
    from samegold.evidence.render import _provenance

    clean = {"verdict": {"runs": {"commit_sha": "a" * 40, "tree_dirty": False}}}
    assert "aaaaaaaaa" in _provenance(clean)
    assert "uncommitted" not in _provenance(clean)

    dirty = {
        "verdict": {"runs": {"commit_sha": "a" * 40, "tree_sha": "b" * 40, "tree_dirty": True}}
    }
    rendered = _provenance(dirty)
    assert "aaaaaaaaa" in rendered and "bbbbbbbbb" in rendered
    assert "uncommitted tree" in rendered

    # AND IT LINKS THE RUN when the record names one. "CI" as bare text asked the reader to
    # take on trust that a run existed: `grep -rno 'actions/runs/[0-9]*' --include=*.md .`
    # over this repository answered with nothing, so the word "CI" on the front page was the
    # least checkable thing on it. The url is in the record, which the chain hashes.
    in_ci = {
        "ci_run_url": "https://github.com/x/y/actions/runs/1",
        "verdict": {"runs": {"commit_sha": "c" * 40}},
    }
    assert _provenance(in_ci) == "[CI, ccccccccc](https://github.com/x/y/actions/runs/1)"

    # A url that is not a run url is rendered as the plain text it used to be, not refused and
    # not interpolated: a value that lands inside `](...)` can close the link early and put
    # the rest of itself into the table as prose, and one odd record is no reason to stop
    # publishing the other nine rows.
    odd = {
        "ci_run_url": "https://example.invalid/runs/1) [click here](https://evil.invalid",
        "verdict": {"runs": {"commit_sha": "c" * 40}},
    }
    assert _provenance(odd) == "CI, ccccccccc"

    # A record with no commit at all says so rather than rendering an empty cell, which would
    # read as "no caveat" instead of "no provenance".
    assert "no commit" in _provenance({"verdict": {"runs": {}}})


def test_the_append_only_policy_is_stated_before_the_numbers() -> None:
    """The rule has to be readable by someone who has not opened the ADRs.

    The chain describes a population, the code that generates it changes, and the two can be
    out of step while every hash still verifies - which is the state this repository was in
    when the policy was written. A reader comparing the front page against their own
    `make evidence` will find two different numbers, and the document has to say which one is
    the good one BEFORE they meet either.
    """
    from samegold.evidence.render import BEGIN

    readme = (REPO / "README.md").read_text(encoding="utf-8")
    policy = readme[: readme.index(BEGIN)]
    for phrase in ("append-only", "most recent record", "never edit or replace"):
        assert phrase in policy, (
            f"README.md does not state {phrase!r} above the claims table; the policy has to "
            f"come before the numbers it governs"
        )
    adr = (
        REPO / "docs" / "adr" / "0010-the-chain-is-append-only-and-the-documents-quote-its-head.md"
    )
    assert adr.exists(), "the policy names an ADR that does not exist"
    assert adr.name in policy, "README does not point at the ADR that holds the full policy"


def test_the_chain_is_written_with_one_kind_of_line_ending(tmp_path: Path) -> None:
    """A record's bytes must not depend on which shell appended it.

    Python's text mode translates "\n" to "\r\n" on Windows, so `evidence/history.jsonl` grew
    CRLF lines when a claim ran from the Windows shell and LF lines when the same claim ran
    under WSL - 67 of one and 83 of the other, measured, in one append-only chain. Nothing
    verified wrongly: the hashes are over each record's JSON, not over the file's bytes.

    The damage was one level out. A git that normalises line endings on read (the Windows
    checkout's) called the tree clean; a git that does not (WSL's, on the same directory)
    called thirty-seven files modified - so `current_tree()` reported DIRTY on a tree that was
    byte-for-byte a commit, and every record produced under WSL carried "on an uncommitted
    tree" in its provenance. SG-07 is the claim that has to run there, because it needs a JVM.
    A false caveat on good evidence is still a wrong label, and provenance is the one field
    this chain exists to make trustworthy.
    """
    store = EvidenceStore(tmp_path)
    store.append(_record("SG-01"))
    store.append(_record("SG-02"))
    written = (tmp_path / "history.jsonl").read_bytes()
    assert written.count(b"\r\n") == 0, "the chain was written with CRLF line endings"
    assert written.count(b"\n") == 2
    assert (tmp_path / "runs" / "SG-01.json").read_bytes().count(b"\r\n") == 0


@pytest.mark.evidence_dependent
def test_this_repositorys_own_chain_has_no_crlf() -> None:
    """And the artifact itself, because the fix above only governs records written after it."""
    written = (REPO / "evidence" / "history.jsonl").read_bytes()
    crlf = written.count(b"\r\n")
    assert crlf == 0, (
        f"{crlf} of the {written.count(chr(10).encode())} records in the chain are CRLF-"
        f"terminated. They verify - the hashes are over the JSON - but the file's bytes now "
        f"depend on which shell wrote each line, and a second git on the same checkout reads "
        f"that as an uncommitted tree."
    )


@pytest.mark.evidence_dependent
def test_no_published_rate_disagrees_with_its_own_record() -> None:
    """The fifth attack, in the lane a reviewer runs first.

    The four checks above stop a record being EDITED. None of them stopped one being
    APPENDED: a copy of a genuine record with `verdict.rate` set to 999/999, chained to the
    real head, hashed with `record_hash`, published `SG-03 | PASS | 999/999` on the front page
    with `samegold check` at exit 0 and 594 tests passing.

    What the forgery did not do - because it is a great deal more work, and because the prose
    in CLAIMS.md and FINDINGS.md is written from these numbers - is move the artifacts under
    the rate. `mutants_total: 94`, `equivalent: 27` and `per_witness.ledger: 67` stayed where
    they were, two lines below a rate of 999/999, and 94 - 27 is not 999.

    So this is arithmetic over one file, it costs nothing, and it is the half of the gate that
    runs here. `samegold verify-latest` is the other half: it re-runs the claims from the
    seeds their records name, which is exact and costs minutes, and it runs in CI and in
    `make preflight` rather than in the fast lane.
    """
    latest = EvidenceStore(REPO / "evidence").latest()
    mismatches, unchecked = arithmetic_mismatches(latest)
    assert not mismatches, (
        "a published figure disagrees with the artifacts of its own record, which means it "
        "was not produced by the run those artifacts came from: "
        + "; ".join(str(m) for m in mismatches)
    )
    # Not an assertion that everything was checked - older records legitimately predate the
    # artifacts some rules read, and `samegold verify-latest` reports the same set. What would
    # be wrong is this test reporting agreement having compared nothing at all.
    assert len(unchecked) < len(latest), (
        f"no rule could check any published rate ({unchecked}), so this test compared "
        f"nothing and passed, which is the defect it exists to catch"
    )


# ------------------------------------------------- SG-06 counts a chain that names its head
#
# SG-06's rate was `records - breaks` over `records`, measured over the chain AS IT STOOD. The
# chain grows - this claim's own record is appended the moment it finishes - so re-running it
# verified a longer history and published a bigger number. `samegold verify-latest` reported
# 203/203 against 204/204, and always would have.
#
# That was called a limitation for about an hour. It is not one: a measurement whose
# denominator is "now" cannot be checked by anybody, including whoever took it. The claim names
# the head it verified, and re-running it against that head asks the same question.


def _chain_of(tmp_path: Path, claims: tuple[str, ...]) -> EvidenceStore:
    store = EvidenceStore(tmp_path)
    for claim_id in claims:
        store.append(_record(claim_id))
    return store


@pytest.mark.evidence_dependent
def test_sg06_publishes_the_head_it_verified() -> None:
    """Without the head, the number is about a moving target."""
    record = EvidenceStore(REPO / "evidence").latest().get("SG-06")
    assert record is not None, "SG-06 has no evidence; run `make evidence`"
    head = record.get("artifacts", {}).get("chain_head")
    assert head, (
        "SG-06 publishes no chain_head, so its record count is about the chain at some "
        "unrecorded moment and nothing can reproduce it. Run `make evidence`."
    )
    hashes = [row.get("hash") for row in EvidenceStore(REPO / "evidence").read_history()]
    assert head in hashes, f"SG-06 names a head that is not in the chain: {head}"


def test_sg06_verified_against_its_own_head_gives_the_same_answer(tmp_path: Path) -> None:
    """The property the redefinition buys, over a chain that then grows underneath it."""
    store = _chain_of(tmp_path, ("SG-01", "SG-02", "SG-03"))
    first = claim_seed_provenance(tmp_path)
    head = first.artifacts["chain_head"]
    assert first.verdict.rate is not None
    measured = (first.verdict.rate.successes, first.verdict.rate.trials)
    assert measured == (3, 3), measured

    # The chain grows, exactly as it does when this claim's own record is appended.
    store.append(_record("SG-05"))
    store.append(_record("SG-08"))

    today = claim_seed_provenance(tmp_path)
    assert today.verdict.rate is not None
    assert (today.verdict.rate.successes, today.verdict.rate.trials) == (5, 5), (
        "with no head named, the claim is about the chain now - which is the behaviour "
        "`make evidence` wants and the reason the old definition could not reproduce"
    )

    again = claim_seed_provenance(tmp_path, up_to=head)
    assert again.verdict.rate is not None
    assert (again.verdict.rate.successes, again.verdict.rate.trials) == measured, (
        "re-running SG-06 against the head its record names must ask the same question and "
        "get the same answer, however much the chain has grown since"
    )


def test_sg06_fails_when_the_head_it_named_is_no_longer_in_the_chain(tmp_path: Path) -> None:
    """A head that has vanished is not a missing input, it is the rewrite the chain exists to
    make visible: the history no longer contains the record this claim was measured over."""
    _chain_of(tmp_path, ("SG-01", "SG-02"))
    verdict = claim_seed_provenance(tmp_path, up_to="0" * 32).verdict
    assert not verdict.ok
    assert "rewritten rather than appended" in str(verdict.to_json())


def test_sg06_over_an_empty_history_names_no_head_and_does_not_pretend_to(
    tmp_path: Path,
) -> None:
    (tmp_path / "runs").mkdir(parents=True, exist_ok=True)
    record = claim_seed_provenance(tmp_path)
    assert record.artifacts["chain_head"] == ""
    assert record.verdict.ok, "an empty history verifies vacuously"

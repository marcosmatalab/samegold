"""The crash harness, tested without a JVM.

``src/samegold/faults/`` was **198 statements at 0%** on 20 September 2026: no test in
``tests/fast``, none in ``tests/spark``, none in ``tests/delta``. The only thing that ever ran
it was ``samegold evidence --claims SG-07``, which no pytest lane launches and which needs a
JVM and about ten minutes. The hardest code in this repository, the arm whose whole job is to
kill a process at a named instant and decide whether what came back is the same data, had
nothing checking it at all.

What is and is not covered here, stated rather than implied. The part that needs Spark is one
function, ``worker.run``, and it stays uncovered: a test of it would be a test of Spark. What
IS covered is everything around it - the bound arithmetic, the aggregation that turns a
campaign into a published number, the schedule that decides which batch to crash, the
accounting that separates a missed injection from a pass, the barrier's arming and its exit,
and the negative control's verdict. Those are decisions this repository makes, and every one
of them has been wrong at least once: the batch schedule asked for batches that did not
exist, the bound printed 1.4979 as a probability, and the campaign counted attempts that never
crashed towards a tighter interval.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from samegold.faults import harness
from samegold.faults.barrier import EXIT_CODE, CrashBarrier
from samegold.faults.points import (
    CRASH_POINTS,
    GOLD_POINTS,
    REACHABLE,
    SILVER_POINTS,
)

# --------------------------------------------------------------------------- the barrier


def test_a_barrier_with_no_point_is_disarmed_and_never_fires() -> None:
    barrier = CrashBarrier()
    assert not barrier.armed
    barrier.reach("before_batch_write", 1)  # returns, which is the whole assertion


def test_the_barrier_is_built_from_the_environment_so_arming_it_edits_no_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The honesty constraint from barrier.py's docstring: the program under test must be the
    same program. If arming the crash required editing the writer, the fault run and the
    clean run would not be running the same code and the digest comparison would mean
    nothing."""
    monkeypatch.delenv("SAMEGOLD_CRASH_POINT", raising=False)
    monkeypatch.delenv("SAMEGOLD_CRASH_BATCH", raising=False)
    assert not CrashBarrier.from_env().armed

    monkeypatch.setenv("SAMEGOLD_CRASH_POINT", "mid_merge")
    monkeypatch.setenv("SAMEGOLD_CRASH_BATCH", "3")
    armed = CrashBarrier.from_env()
    assert armed.armed
    assert (armed.point, armed.batch) == ("mid_merge", 3)


def test_an_empty_crash_point_disarms_rather_than_naming_a_point_called_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`SAMEGOLD_CRASH_POINT=""` is how every clean run in the harness is spelled: the
    campaign sets it to the empty string to resume after a crash, and a barrier that read
    that as the point `""` would be armed for a point no writer ever reaches."""
    monkeypatch.setenv("SAMEGOLD_CRASH_POINT", "")
    assert not CrashBarrier.from_env().armed


def test_the_barrier_fires_only_at_its_own_point_and_its_own_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both halves of the condition, because either one alone would crash the wrong run.

    `os._exit` is replaced: the real one takes the test session with it, which is exactly why
    the worker is a subprocess.
    """
    exits: list[int] = []
    monkeypatch.setattr("os._exit", lambda code: exits.append(code))

    barrier = CrashBarrier(point="after_batch_write_before_commit", batch=2)
    barrier.reach("before_batch_write", 2)
    barrier.reach("after_batch_write_before_commit", 1)
    assert exits == [], "the barrier fired at a point or a batch that is not its own"

    barrier.reach("after_batch_write_before_commit", 2)
    assert exits == [EXIT_CODE]


def test_the_exit_code_is_not_a_code_a_crash_could_produce_by_accident() -> None:
    """The harness reads this number as "the injection happened". 0 and 1 are what a normal
    run and a normal failure use, and the harness raises on anything else."""
    assert EXIT_CODE not in (0, 1)


# --------------------------------------------------------------------------- the points


def test_every_crash_point_has_a_unique_name() -> None:
    names = [point.name for point in CRASH_POINTS]
    assert len(names) == len(set(names))


def test_the_unreachable_points_are_listed_rather_than_left_out() -> None:
    """A coverage number over the points you happened to write down is not a coverage number.

    Three of the seven are inside somebody else's transaction log, and the list carries them
    with `reachable=False` so the published record can report them as NOT COVERED instead of
    the reader inferring that four points is all there are.
    """
    unreachable = [point for point in CRASH_POINTS if not point.reachable]
    assert len(unreachable) == 3
    for point in unreachable:
        assert point.expectation, f"{point.name} says it is unreachable and not why"
    assert set(REACHABLE) | set(unreachable) == set(CRASH_POINTS)


def test_the_stages_partition_the_reachable_points() -> None:
    assert set(SILVER_POINTS) | set(GOLD_POINTS) == set(REACHABLE)
    assert not set(SILVER_POINTS) & set(GOLD_POINTS)
    assert SILVER_POINTS and GOLD_POINTS


def test_a_point_serialises_every_field_it_carries() -> None:
    payload = CRASH_POINTS[0].to_json()
    assert set(payload) == {"name", "description", "reachable", "expectation", "stage"}
    json.dumps(payload)  # it goes into an evidence record, so it has to be JSON


# --------------------------------------------------------------------------- the bound


def test_the_bound_refuses_to_be_a_probability_when_it_cannot_be_one() -> None:
    """`-ln(0.05)/n` is 1.4979 at n=2, and the first version published that as a "rate".

    A number above one is not a probability, and a README that prints one has stopped
    measuring and started decorating.
    """
    assert harness._bound(2, saw_divergence=False) is None
    assert harness._bound(1, saw_divergence=False) is None
    bound = harness._bound(3, saw_divergence=False)
    assert bound is not None and 0.0 < bound <= 1.0


def test_the_bound_is_withheld_once_something_has_diverged() -> None:
    """The rule of three bounds the rate of an event that has NOT been seen. Once it has been
    seen the bound is the wrong instrument, and the divergence itself is the finding."""
    assert harness._bound(100, saw_divergence=True) is None


def test_more_runs_give_a_tighter_bound() -> None:
    loose, tight = harness._bound(10, False), harness._bound(100, False)
    assert loose is not None and tight is not None and tight < loose


# --------------------------------------------------------------------------- the aggregation


def _result(**kwargs: Any) -> harness.CampaignResult:
    base: dict[str, Any] = {
        "runs": 20,
        "injected": 20,
        "per_point": {
            "before_batch_write": {"attempts": 10, "injected": 10, "converged": 10},
            "after_batch_write_before_commit": {"attempts": 10, "injected": 10, "converged": 10},
        },
        "clean_digest": {"content": "aaa", "multiset": "bbb"},
        "batches": 8,
        "duration_s": 12.3456,
    }
    base.update(kwargs)
    return harness.CampaignResult(**base)


def test_the_published_record_reports_the_points_it_did_not_reach() -> None:
    """The claim used to say "each of its structural points" while touching two of four.

    The record names the gold-stage points it never injected at, so the sentence on the front
    page can be about the points covered rather than about the points that exist.
    """
    payload = _result().to_json()
    assert payload["points_covered"] == 2
    assert payload["points_total"] == len(SILVER_POINTS)
    assert payload["reachable_points_total"] == len(REACHABLE)
    assert payload["reachable_points_not_covered"] == [p.name for p in GOLD_POINTS]


def test_only_injected_runs_count_towards_the_bound() -> None:
    """Counting attempts that never crashed would publish a TIGHTER interval for having
    tested LESS, which is the wrong direction for a number to move."""
    twenty_attempts_five_crashes = _result(
        runs=20,
        injected=5,
        missed_injections=[{"point": "before_batch_write", "repetition": i} for i in range(15)],
        per_point={"before_batch_write": {"attempts": 20, "injected": 5, "converged": 5}},
    )
    payload = twenty_attempts_five_crashes.to_json()
    assert payload["injected_runs"] == 5
    assert payload["divergence_rate_upper95_per_run"] == harness._bound(5, False)
    assert payload["divergence_rate_upper95_per_run"] != harness._bound(20, False)
    assert len(payload["missed_injections"]) == 15


def test_a_divergence_withholds_the_bound_for_its_own_point_only() -> None:
    diverged = _result(
        divergences=[{"point": "before_batch_write", "repetition": 0, "clean": {}, "after": {}}]
    )
    per_point = diverged.to_json()["divergence_rate_upper95_per_point"]
    assert per_point["before_batch_write"] is None
    assert per_point["after_batch_write_before_commit"] is not None


def test_the_whole_record_is_json() -> None:
    json.dumps(_result().to_json())


# --------------------------------------------------------------------------- the campaign


class _FakeWorker:
    """Stands in for the subprocess. Records every call, answers with a chosen exit code."""

    def __init__(self, batches: int = 4) -> None:
        self.calls: list[tuple[Path, dict[str, str], bool]] = []
        self.batches = batches

    def __call__(self, bronze: Path, out: Path, env: dict[str, str], reset: bool) -> int:
        self.calls.append((out, dict(env), reset))
        # It writes the batch directories a real run would write, because `run_campaign`
        # COUNTS them to build the crash schedule and rmtrees the clean output first. A
        # fixture that made them up front would be measuring nothing, and that count is
        # precisely what the schedule got wrong in the field.
        for batch in range(self.batches):
            (out / "silver" / f"batch_id={batch}").mkdir(parents=True, exist_ok=True)
        return EXIT_CODE if env.get("SAMEGOLD_CRASH_POINT") else 0

    @property
    def crash_batches(self) -> list[str]:
        return [
            env["SAMEGOLD_CRASH_BATCH"]
            for _, env, _ in self.calls
            if env.get("SAMEGOLD_CRASH_POINT")
        ]


@pytest.fixture
def campaign(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Any:
    """A campaign whose worker never starts a JVM and whose digests are decided here."""
    clean = {"content": "clean-content", "multiset": "clean-multiset"}
    worker = _FakeWorker(batches=4)
    monkeypatch.setattr(harness, "_worker", worker)
    monkeypatch.setattr(harness, "_digest_silver", lambda out: clean)
    return worker, clean, tmp_path


def test_a_campaign_where_nothing_diverges_converges(campaign: Any) -> None:
    _worker, clean, tmp_path = campaign
    result = harness.run_campaign(tmp_path / "bronze", tmp_path / "work", repetitions=2)
    assert result.divergences == []
    assert result.missed_injections == []
    assert result.injected == result.runs == 2 * len(SILVER_POINTS)
    assert result.clean_digest == clean
    assert result.batches == 4
    for stats in result.per_point.values():
        assert stats == {"attempts": 2, "injected": 2, "converged": 2}


def test_the_crash_schedule_never_asks_for_a_batch_the_clean_run_did_not_produce(
    campaign: Any,
) -> None:
    """The defect that failed SG-07 for a reason that had nothing to do with the pipeline.

    Spark numbers micro-batches from ZERO and the campaign asked for `repetition + 1`. With
    eight batches and ten repetitions it requested 8, 9 and 10, none of which exist: six of
    twenty runs reported "missed injection" and the claim went red. The schedule cycles over
    the batches the clean run actually produced.
    """
    worker, _, tmp_path = campaign
    harness.run_campaign(tmp_path / "bronze", tmp_path / "work", repetitions=10)
    requested = {int(batch) for batch in worker.crash_batches}
    assert requested <= set(range(4)), f"the schedule asked for batches {sorted(requested)}"
    assert 0 in requested, "batches are numbered from zero and the schedule never asked for one"


def test_a_run_that_never_crashed_is_a_missed_injection_and_not_a_pass(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A run that finished normally tested nothing. Counting it as converged would be the
    purest form of the defect this repository keeps finding: a green that measured nothing."""
    clean = {"content": "c", "multiset": "m"}
    monkeypatch.setattr(harness, "_digest_silver", lambda out: clean)

    def never_crashes(bronze: Path, out: Path, env: dict[str, str], reset: bool) -> int:
        for batch in range(2):
            (out / "silver" / f"batch_id={batch}").mkdir(parents=True, exist_ok=True)
        # The negative control has to crash, or `run_campaign` reports it inconclusive and
        # the point of the test is lost; only the campaign's own injections miss.
        if env.get("SAMEGOLD_WRITER") == "append":
            return EXIT_CODE
        return 0

    monkeypatch.setattr(harness, "_worker", never_crashes)
    result = harness.run_campaign(tmp_path / "bronze", tmp_path / "work", repetitions=3)
    assert result.injected == 0
    assert len(result.missed_injections) == 3 * len(SILVER_POINTS)
    assert all(stats["converged"] == 0 for stats in result.per_point.values())
    assert result.to_json()["divergence_rate_upper95_per_run"] is None


def test_a_digest_that_moved_after_a_crash_is_recorded_as_a_divergence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    clean = {"content": "clean", "multiset": "clean"}
    dirty = {"content": "clean", "multiset": "DOUBLED"}
    seen: list[Path] = []

    def digest(out: Path) -> dict[str, str]:
        seen.append(out)
        # The clean run and the negative control first, then the injected runs.
        return clean if len(seen) <= 2 else dirty

    monkeypatch.setattr(harness, "_worker", _FakeWorker(batches=2))
    monkeypatch.setattr(harness, "_digest_silver", digest)

    result = harness.run_campaign(
        tmp_path / "bronze", tmp_path / "work", repetitions=1, points=("before_batch_write",)
    )
    assert len(result.divergences) == 1
    assert result.divergences[0]["point"] == "before_batch_write"
    assert result.divergences[0]["after_crash"] == dirty
    assert result.to_json()["divergence_rate_upper95_per_run"] is None


def test_selecting_points_runs_only_those_points(campaign: Any) -> None:
    _, _, tmp_path = campaign
    result = harness.run_campaign(
        tmp_path / "bronze", tmp_path / "work", repetitions=1, points=("before_batch_write",)
    )
    assert list(result.per_point) == ["before_batch_write"]


# --------------------------------------------------------------------------- the control


def test_the_negative_control_says_which_digest_caught_the_non_idempotent_writer(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If the control ever comes back clean, the campaign is measuring nothing.

    The content digest deduplicates by event_id and is blind to a writer that wrote every row
    twice - an adversarial review copied a whole batch directory and it did not move. The
    multiset digest is the one that has to catch it, and the control records WHICH of them
    did, so a future change that quietly drops the multiset digest shows up here.
    """
    clean = {"content": "same", "multiset": "one-copy"}
    monkeypatch.setattr(harness, "_worker", _FakeWorker())
    monkeypatch.setattr(
        harness, "_digest_silver", lambda out: {"content": "same", "multiset": "two-copies"}
    )
    control = harness._negative_control(tmp_path / "bronze", tmp_path / "neg", clean)
    assert control["status"] == "detected"
    assert control["detected_by"] == ["multiset"]


def test_a_control_that_never_crashed_is_inconclusive_rather_than_detected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(harness, "_worker", lambda bronze, out, env, reset: 0)
    control = harness._negative_control(tmp_path / "bronze", tmp_path / "neg", {})
    assert control["status"] == "inconclusive"
    assert "never reached its crash point" in control["detail"]


def test_a_control_the_digests_do_not_notice_is_reported_not_detected(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The one outcome that voids the whole claim, and it has to be spelled loudly."""
    clean = {"content": "same", "multiset": "same"}
    monkeypatch.setattr(harness, "_worker", _FakeWorker())
    monkeypatch.setattr(harness, "_digest_silver", lambda out: clean)
    control = harness._negative_control(tmp_path / "bronze", tmp_path / "neg", clean)
    assert control["status"] == "NOT DETECTED"
    assert control["detected_by"] == []


# --------------------------------------------------------------------------- the subprocess


def test_the_worker_call_passes_the_barrier_through_the_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`_worker` is the seam between this process and the one that dies. It must pass the
    crash point in the environment (so no code is edited to arm it) and must accept exactly
    two exit codes."""
    import subprocess

    captured: dict[str, Any] = {}

    class _Completed:
        returncode = EXIT_CODE
        stderr = ""

    def fake_run(command: list[str], **kwargs: Any) -> Any:
        captured["command"] = command
        captured["env"] = kwargs["env"]
        return _Completed()

    monkeypatch.setattr(subprocess, "run", fake_run)
    code = harness._worker(
        tmp_path / "bronze", tmp_path / "out", {"SAMEGOLD_CRASH_POINT": "mid_merge"}, reset=True
    )
    assert code == EXIT_CODE
    assert "samegold.faults.worker" in captured["command"]
    assert "--reset" in captured["command"]
    assert captured["env"]["SAMEGOLD_CRASH_POINT"] == "mid_merge"


def test_a_worker_that_failed_for_another_reason_is_an_error_not_a_missed_injection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Exit 1 means the pipeline broke. Folding that into "missed injection" would report a
    broken pipeline as a repetition that tested nothing, and the campaign would go green."""
    import subprocess

    class _Failed:
        returncode = 1
        stderr = "AnalysisException: cannot resolve 'event_id'"

    monkeypatch.setattr(subprocess, "run", lambda command, **kwargs: _Failed())
    with pytest.raises(RuntimeError, match="not the injected crash"):
        harness._worker(tmp_path / "bronze", tmp_path / "out", {}, reset=False)


# --------------------------------------------------------------------------- the entry points


def test_the_faults_command_fails_when_the_campaign_found_something(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`make faults` has to exit non-zero on a divergence, or the campaign is a report nobody
    reads rather than a gate."""
    from samegold.faults import campaign as campaign_module

    monkeypatch.setattr(campaign_module, "generate", lambda *a, **k: None)
    monkeypatch.setattr(
        campaign_module,
        "run_campaign",
        lambda *a, **k: _result(divergences=[{"point": "before_batch_write"}]),
    )
    assert campaign_module.main(["--out", str(tmp_path), "--repetitions", "1"]) == 1

    monkeypatch.setattr(campaign_module, "run_campaign", lambda *a, **k: _result())
    assert campaign_module.main(["--out", str(tmp_path), "--repetitions", "1"]) == 0


def test_a_missed_injection_alone_fails_the_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A campaign that crashed nothing has measured nothing, and must not exit 0."""
    from samegold.faults import campaign as campaign_module

    monkeypatch.setattr(campaign_module, "generate", lambda *a, **k: None)
    monkeypatch.setattr(
        campaign_module,
        "run_campaign",
        lambda *a, **k: _result(missed_injections=[{"point": "before_batch_write"}]),
    )
    assert campaign_module.main(["--out", str(tmp_path), "--repetitions", "1"]) == 1


def test_the_worker_module_imports_without_a_jvm() -> None:
    """The property that lets this file exist at all.

    `worker.py` used to import pyspark at module scope, so nothing in the fast lane could
    import it and `main` - which takes a `--reset` that deletes a directory - had no test.
    The Spark imports are inside `run` now, and `tests/fast/conftest.py` fails the session if
    pyspark reaches `sys.modules`, so this assertion is checked twice over.
    """
    import sys

    from samegold.faults import worker

    assert worker.run is not None
    assert "pyspark" not in sys.modules


def test_the_worker_resets_its_output_only_when_asked(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`--reset` deletes the output AND the checkpoint, which is what makes a repetition a
    fresh experiment rather than a resume. Getting it backwards would make every injected run
    resume from the previous one's checkpoint and converge for the wrong reason."""
    from samegold.faults import worker

    monkeypatch.setattr(worker, "run", lambda bronze, out, files_per_trigger: 0)
    out = tmp_path / "out"
    (out / "silver").mkdir(parents=True)
    (out / "silver" / "stale.parquet").write_text("old")

    worker.main(["--bronze", str(tmp_path / "bronze"), "--out", str(out)])
    assert (out / "silver" / "stale.parquet").exists(), "it deleted the output without --reset"

    worker.main(["--bronze", str(tmp_path / "bronze"), "--out", str(out), "--reset"])
    assert not (out / "silver").exists()
    assert out.exists(), "--reset must leave the output directory ready to be written into"

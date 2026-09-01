"""Command line interface.

Design rules, taken from having watched people bounce off other people's repositories:

  * ``samegold demo`` must produce something interesting in under ten seconds, with no
    account, no credentials and no arguments. It is the second thing anyone does after
    reading the first line of the README, and if it is slow or asks for a token they leave.
  * every error names the exact command that fixes it;
  * nothing prints a stack trace unless ``--debug`` is passed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from pathlib import Path

from samegold import claims as claim_module
from samegold.evidence.record import EvidenceRecord
from samegold.evidence.render import check_readme, render_readme
from samegold.evidence.store import EvidenceStore
from samegold.generator.events import CI, FAST, FULL
from samegold.generator.seeds import current_commit_sha, seeds_from_commit
from samegold.oracle.duckdb_gold import DuckDBWitness
from samegold.verify.invariants import scd2_well_formed

PROFILES = {"fast": FAST, "ci": CI, "full": FULL}
REPO_ROOT = Path(__file__).resolve().parents[2]
RENDERED_FILES = (
    "README.md",
    "CLAIMS.md",
    # The post-mortem quotes SG-04's euro figures. Seeds derive from the commit SHA, so those
    # figures change on every commit, and hand-typed ones are stale by the next one: they
    # were invented in the first draft and stale again two commits after being corrected.
    "docs/postmortem-2026-03-06.md",
)


class UserError(Exception):
    """An error with a fix attached. Printed without a traceback."""

    def __init__(self, message: str, fix: str) -> None:
        super().__init__(message)
        self.fix = fix


def _work_dir(explicit: str | None) -> Path:
    if explicit:
        path = Path(explicit)
        path.mkdir(parents=True, exist_ok=True)
        return path
    return Path(tempfile.mkdtemp(prefix="samegold-"))


def cmd_demo(args: argparse.Namespace) -> int:
    started = time.monotonic()
    work = _work_dir(args.work)
    seed = seeds_from_commit(1, purpose="demo")[0]
    from samegold.generator.events import generate

    result = generate(work / "demo", seed=seed, profile=FAST)
    witness = DuckDBWitness()
    closes = result.ledger.closes
    first, last = dt.datetime.fromisoformat(closes[0]), dt.datetime.fromisoformat(closes[-1])
    at_close = witness.revenue(work / "demo" / "bronze", first)
    final = witness.revenue(work / "demo" / "bronze", last)
    month = sorted(at_close)[0]
    before, after = at_close[month]["net_cents"], final[month]["net_cents"]
    delta = after - before
    scd2_ok = not scd2_well_formed(witness.scd2(work / "demo" / "bronze", last))
    print(f"samegold demo - {result.event_count} events, {len(result.files)} files, seed {seed}")
    print()
    print(
        f"  Month {month} was closed at {first:%Y-%m-%d} reporting "
        f"{before / 100:,.2f} EUR of net revenue."
    )
    print(
        f"  By {last:%Y-%m-%d}, late returns and late amendments had moved it to "
        f"{after / 100:,.2f} EUR."
    )
    pct = (100.0 * delta / before) if before else 0.0
    print(
        f"  That is {delta / 100:+,.2f} EUR, {pct:+.2f}% of a month that finance had "
        f"already signed off."
    )
    print()
    print(f"  The customer dimension is well formed: {'yes' if scd2_ok else 'NO'}.")
    print("  Two implementations of that number are compared on this data by `samegold evidence`.")
    print()
    print(
        f"  {time.monotonic() - started:.1f}s, no account, no credentials, nothing installed "
        f"beyond this package."
    )
    if not args.work:
        shutil.rmtree(work, ignore_errors=True)
    return 0


def cmd_generate(args: argparse.Namespace) -> int:
    from samegold.generator.events import generate

    out = Path(args.out)
    seed = args.seed if args.seed is not None else seeds_from_commit(1, purpose="generator")[0]
    result = generate(out, seed=seed, profile=PROFILES[args.profile])
    print(f"{result.event_count} events in {len(result.files)} files under {out}")
    print(f"ledger: {out / 'truth' / 'ledger.json'}")
    return 0


def _run_claims(
    names: list[str],
    profile: str,
    work: Path,
    evidence_dir: str | Path = "",
    repetitions: int = 3,
) -> Iterator[EvidenceRecord]:
    """Yields one record at a time.

    A generator, not a list, and the difference matters: SG-06 verifies the history, so it
    has to see the records the earlier claims wrote. Building the whole list before appending
    anything made SG-06 verify an empty file and report 0 of 1, which looked like a bug in
    the chain and was a bug in the loop.
    """
    for name in names:
        records: list[EvidenceRecord] = []
        if name == "SG-00":
            records.append(claim_module.claim_repository_facts())
        elif name == "SG-01":
            records.append(claim_module.claim_witness_agreement(work, profile))
        elif name == "SG-02":
            records.append(claim_module.claim_redelivery_is_a_noop(work, profile))
        elif name == "SG-03":
            records.append(claim_module.claim_mutation_campaign(work, profile))
        elif name == "SG-04":
            records.append(claim_module.claim_restatement_magnitude(work, profile))
        elif name == "SG-05":
            records.append(claim_module.claim_dimension_invariants(work, profile))
        elif name == "SG-08":
            records.append(claim_module.claim_privacy_controls(work, profile))
        elif name == "SG-09":
            records.append(claim_module.claim_cost_lab(work))
        elif name == "SG-07":
            records.append(claim_module.claim_crash_campaign(work, repetitions=repetitions))
        elif name == "SG-06":
            records.append(claim_module.claim_seed_provenance(Path(evidence_dir)))
        else:
            raise UserError(
                f"unknown claim {name}",
                f"pick from: {', '.join(claim_module.ALL_CLAIMS + claim_module.SLOW_CLAIMS)}",
            )
        yield records.pop()


def cmd_evidence(args: argparse.Namespace) -> int:
    work = _work_dir(args.work)
    names = args.claims or list(claim_module.ALL_CLAIMS)
    store = EvidenceStore(Path(args.evidence_dir))
    failures = 0
    for record in _run_claims(names, args.profile, work, args.evidence_dir, args.repetitions):
        store.append(record)
        verdict = record.verdict
        mark = "PASS" if verdict.ok else "FAIL"
        rate = getattr(verdict, "rate", None)
        detail = rate.render() if rate else ""
        print(f"{mark:>4}  {record.claim_id}  {record.title:<52} {detail}")
        if not verdict.ok:
            failures += 1
            print(
                f"        counterexample: {verdict.counterexample.description}"  # type: ignore[union-attr]
            )
    if not args.work:
        shutil.rmtree(work, ignore_errors=True)
    print(f"\nevidence written to {args.evidence_dir}/ ({store.counts()})")
    return 1 if failures and not args.allow_failures else 0


def cmd_readme(args: argparse.Namespace) -> int:
    store = EvidenceStore(Path(args.evidence_dir))
    latest = store.latest()
    if not latest:
        raise UserError(
            "there is no evidence to render from",
            "run `make evidence` first (about 60 seconds, no credentials needed)",
        )
    for name in RENDERED_FILES:
        path = REPO_ROOT / name
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        path.write_text(render_readme(text, latest), encoding="utf-8")
        print(f"rendered {name}")
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    store = EvidenceStore(Path(args.evidence_dir))
    breaks = store.verify_chain(REPO_ROOT)
    if breaks:
        for chain_break in breaks:
            print(f"CHAIN {chain_break}")
        raise UserError(
            f"{len(breaks)} evidence record(s) do not verify",
            "the history is append-only and hash-chained: restore it with "
            "`git checkout evidence/history.jsonl`, or re-run `make evidence` to append a "
            "new, verifiable record",
        )
    latest = store.latest()
    drifts = []
    for name in RENDERED_FILES:
        path = REPO_ROOT / name
        if path.exists():
            drifts.extend(check_readme(path, latest))
    if drifts:
        for drift in drifts:
            print(f"DRIFT {drift}")
        raise UserError(
            f"{len(drifts)} places where the documents and the evidence disagree",
            "run `make readme` to regenerate them from evidence/history.jsonl",
        )
    print(
        f"evidence chain verified ({store.counts()['total']} records) and the documents "
        f"match it ({len(latest)} claims)"
    )
    return 0


def cmd_refute(args: argparse.Namespace) -> int:
    """Run every claim with a seed the author never saw.

    The results go to ``evidence/refutations.jsonl`` (created on the first refutation run) and
    never into the history the documents render. An override run's seeds are, by design, not
    derived from the commit, so nothing can recompute them, and a record nobody can recompute
    has no business backing a published number. The store refuses them outright; before it
    did, an adversarial review wrote one straight into the main history and SG-06 reported it
    under the title "every seed derives from its commit".
    """
    os.environ["SAMEGOLD_SEED_OVERRIDE"] = str(args.seed)
    work = _work_dir(args.work)
    log = REPO_ROOT / "evidence" / "refutations.jsonl"
    print(
        f"refutation run with seed override {args.seed!r}. The results go to "
        f"evidence/refutations.jsonl and never back a published number.\n"
        f"SG-00 (which counts the repository) and SG-06 (which verifies the evidence chain) "
        f"are not part of a refutation: neither is a statement about the data.\n"
    )
    failures = 0
    for record in _run_claims(
        list(claim_module.REFUTABLE_CLAIMS), args.profile, work, REPO_ROOT / "evidence"
    ):
        with log.open("a", encoding="utf-8") as handle:
            handle.write(record.to_line() + "\n")
        ok = record.verdict.ok
        print(f"{'PASS' if ok else 'FAIL'}  {record.claim_id}  {record.title}")
        if not ok:
            failures += 1
    shutil.rmtree(work, ignore_errors=True)
    if failures:
        print(
            f"\n{failures} claim(s) failed under your seed. That is a refutation: please "
            f"open an issue with the seed and the output."
        )
    return 1 if failures else 0


def cmd_report(args: argparse.Namespace) -> int:
    """Render the close as a single self-contained HTML page."""
    from samegold.generator.events import generate
    from samegold.oracle.duckdb_gold import revenue_versions
    from samegold.serve.report import render_report

    work = _work_dir(args.work)
    seed = seeds_from_commit(1, purpose="report")[0]
    result = generate(work / "report", seed=seed, profile=PROFILES[args.profile])
    closes = [dt.datetime.fromisoformat(c) for c in result.ledger.closes]
    versions = revenue_versions(work / "report" / "bronze", closes)
    page = render_report(versions, dt.datetime.now(dt.UTC))
    out = Path(args.out)
    out.write_text(page, encoding="utf-8")
    months = len({version["accounting_month"] for version in versions})
    print(f"{out} ({len(versions)} versions of {months} months)")
    if not args.work:
        shutil.rmtree(work, ignore_errors=True)
    return 0


def cmd_doctor(_: argparse.Namespace) -> int:
    print(f"commit            {current_commit_sha()[:12]}")
    print(f"python            {sys.version.split()[0]}")
    for module, label in (
        ("duckdb", "duckdb"),
        ("pyarrow", "pyarrow"),
        ("sqlglot", "sqlglot"),
        ("pyspark", "pyspark"),
        ("delta", "delta-spark"),
        ("deltalake", "delta-rs"),
    ):
        try:
            mod = __import__(module)
            print(f"{label:<17} {getattr(mod, '__version__', 'unknown')}")
        except Exception:
            print(f"{label:<17} absent    (fast lane does not need it)")
    java = shutil.which("java")
    if java:
        out = subprocess.run([java, "-version"], capture_output=True, text=True, check=False)
        # Some environments prepend a JAVA_TOOL_OPTIONS banner that is longer than the
        # version itself; the first line that mentions "version" is the one worth printing.
        lines = [
            line for line in (out.stderr or out.stdout).splitlines() if "version" in line.lower()
        ]
        first = lines[0] if lines else "unknown"
        print(f"java              {first}")
    else:
        print("java              absent    (needed only by the Spark lane)")
    print(f"databricks cli    {shutil.which('databricks') or 'absent'}")
    print(
        "\nThe fast lane needs none of the optional entries above: no JVM, no network, no "
        "credentials. Run `make fast` to confirm; SG-00 publishes how long it took."
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="samegold",
        description="A month-end close you can falsify.",
    )
    parser.add_argument("--debug", action="store_true", help="show tracebacks")
    sub = parser.add_subparsers(dest="command", required=True)

    demo = sub.add_parser("demo", help="ten seconds, no credentials, one business finding")
    demo.add_argument("--work", default=None)
    demo.set_defaults(func=cmd_demo)

    gen = sub.add_parser("generate", help="write bronze events and the ledger")
    gen.add_argument("--out", required=True)
    gen.add_argument("--profile", choices=sorted(PROFILES), default="fast")
    gen.add_argument("--seed", type=int, default=None)
    gen.set_defaults(func=cmd_generate)

    ev = sub.add_parser("evidence", help="run the claims and append evidence records")
    ev.add_argument("--profile", choices=sorted(PROFILES), default="fast")
    ev.add_argument("--claims", nargs="*", default=None)
    ev.add_argument("--evidence-dir", default=str(REPO_ROOT / "evidence"))
    ev.add_argument("--work", default=None)
    ev.add_argument(
        "--repetitions",
        type=int,
        default=3,
        help="repetitions per crash point for SG-07; the published bound is a function of it",
    )
    ev.add_argument(
        "--allow-failures",
        action="store_true",
        help="record failing claims without failing the command",
    )
    ev.set_defaults(func=cmd_evidence)

    rd = sub.add_parser("readme", help="render the documents from the evidence")
    rd.add_argument("--evidence-dir", default=str(REPO_ROOT / "evidence"))
    rd.set_defaults(func=cmd_readme)

    ck = sub.add_parser("check", help="fail if the documents and the evidence disagree")
    ck.add_argument("--evidence-dir", default=str(REPO_ROOT / "evidence"))
    ck.set_defaults(func=cmd_check)

    rf = sub.add_parser("refute", help="run every claim with a seed of your choosing")
    rf.add_argument("--seed", required=True)
    rf.add_argument("--profile", choices=sorted(PROFILES), default="ci")
    rf.add_argument("--work", default=None)
    rf.set_defaults(func=cmd_refute)

    rp = sub.add_parser("report", help="render the close as one self-contained HTML page")
    rp.add_argument("--out", default="close-report.html")
    rp.add_argument("--profile", choices=sorted(PROFILES), default="ci")
    rp.add_argument("--work", default=None)
    rp.set_defaults(func=cmd_report)

    dr = sub.add_parser("doctor", help="what is installed and what each lane needs")
    dr.set_defaults(func=cmd_doctor)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except UserError as exc:
        print(f"\nerror: {exc}\n  fix: {exc.fix}\n", file=sys.stderr)
        return 2
    except Exception as exc:
        if args.debug:
            raise
        print(
            f"\nerror: {type(exc).__name__}: {exc}\n"
            f"  fix: re-run with --debug for the traceback, and open an issue with it\n",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

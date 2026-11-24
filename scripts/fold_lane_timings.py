# -*- coding: utf-8 -*-
"""Fold CI's measured test timings into tests/lane_durations.json.

The public CI's `tests` job runs scripts/run_local_tests.py with `--timings-out`
and uploads one `lane-timings-<OS>-shard-<K>` artifact per shard: each file's
measured seconds and the tests that skipped there. This script folds the
Windows shards (the CI's longest jobs) into the balance file, so the shards are
dealt by what the CI runner measured, without a local full run.

    python scripts/fold_lane_timings.py --run 45318218900        # download with gh, fold
    python scripts/fold_lane_timings.py --run 45318218900 --dry-run
    python scripts/fold_lane_timings.py some/dir lane-timings.json  # files already on disk
    python scripts/fold_lane_timings.py --run <id> --skips        # what skipped on CI, unmarked

What it refuses, and why:
- timings measured under another marker expression than the CI's: a file's
  seconds under `-m needs_local_data` say nothing about its CI share;
- a run missing one of its shards: a third of the files would keep old numbers
  and the report would not say which (--allow-partial folds it anyway);
- a timings file of an unknown format.

Files measured replace their old value; files no longer in tests/ are dropped;
files the run did not measure keep theirs. Balance data only: a wrong number
costs packing quality, never correctness.

`--skips` lists the tests that skipped on CI and do not carry the
`needs_local_data` marker (tests/needs_local_data.py). A test that skips on CI
and is not marked runs in no release lane at all unless the reason is one the
development machine shares (Linux only, say). Run it at each release.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import subprocess
import sys
import tempfile
from collections import defaultdict

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import run_local_tests as runner  # noqa: E402

PUBLIC_REPO = "gershuni/GenizahSearch"
ARTIFACT_PATTERN = "lane-timings-*"


def _download(run_id, repo, dest):
    """gh run download the run's timing artifacts into *dest*; the JSON files found."""
    cmd = ["gh", "run", "download", str(run_id), "--repo", repo,
           "--pattern", ARTIFACT_PATTERN, "--dir", dest]
    pr = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8",
                        errors="replace")
    if pr.returncode != 0:
        raise SystemExit("gh run download failed (exit %d): %s"
                         % (pr.returncode, (pr.stderr or pr.stdout).strip()))
    return sorted(glob.glob(os.path.join(dest, "**", "*.json"), recursive=True))


def _expand(paths):
    found = []
    for p in paths:
        if os.path.isdir(p):
            found += sorted(glob.glob(os.path.join(p, "**", "*.json"), recursive=True))
        else:
            found.append(p)
    return found


def load_timings(paths):
    """Each timings file as a dict, checked; SystemExit names a bad one."""
    out = []
    for path in paths:
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError) as exc:
            raise SystemExit("%s: not a readable timings file (%s)" % (path, exc))
        if not isinstance(data, dict) or data.get("format") != runner.TIMINGS_FORMAT:
            raise SystemExit("%s: timings format %r, this script reads %r"
                             % (path, data.get("format") if isinstance(data, dict)
                                else None, runner.TIMINGS_FORMAT))
        data["_path"] = path
        out.append(data)
    if not out:
        raise SystemExit("no timings files found")
    return out


def select(timings, platform, allow_partial=False):
    """The timings to fold: CI's marker expression, *platform*, every shard present.

    Returns (chosen, notes). SystemExit when a file was measured under another
    selection, or when shards are missing and *allow_partial* is false."""
    chosen, notes = [], []
    for t in timings:
        if t.get("markers") != runner.DEFAULT_MARKERS:
            raise SystemExit(
                "%s was measured under -m %r, not the CI's %r: its seconds are not "
                "a file's share of a CI shard. Fold only the CI's own timings."
                % (t["_path"], t.get("markers"), runner.DEFAULT_MARKERS))
        if platform != "any" and t.get("platform") != platform:
            notes.append("left out %s (platform %s)"
                         % (os.path.basename(os.path.dirname(t["_path"]))
                            or t["_path"], t.get("platform")))
            continue
        chosen.append(t)
    if not chosen:
        raise SystemExit("no timings for platform %r among the files given" % platform)
    by_n = defaultdict(set)
    for t in chosen:
        if t.get("shard"):
            k, n = runner._parse_shard(t["shard"])
            by_n[n].add(k)
    for n, ks in sorted(by_n.items()):
        missing = sorted(set(range(1, n + 1)) - ks)
        if missing:
            msg = ("shard(s) %s of %d missing: their files would keep their old "
                   "numbers" % (", ".join(map(str, missing)), n))
            if not allow_partial:
                raise SystemExit(msg + " (--allow-partial folds the rest anyway)")
            notes.append(msg)
    return chosen, notes


def measured_modules(chosen):
    """file -> seconds over the chosen timings (the mean where a file is measured twice)."""
    sums, counts = defaultdict(float), defaultdict(int)
    for t in chosen:
        for f, s in (t.get("modules") or {}).items():
            sums[f] += float(s)
            counts[f] += 1
    return {f: round(sums[f] / counts[f], 2) for f in sums}


def fold(old, measured, existing):
    """The new balance table, and what changed: (new, report).

    *existing* is the set of test files in tests/ now. Measured files replace
    their value; files no longer there are dropped; the rest keep theirs."""
    new = {f: s for f, s in old.items() if f in existing}
    dropped = sorted(set(old) - set(new))
    added = sorted(f for f in measured if f not in old and f in existing)
    stale = sorted(f for f in measured if f not in existing)
    for f, s in measured.items():
        if f in existing:
            new[f] = s
    kept = sorted(f for f in new if f not in measured)
    return new, {"updated": len([f for f in measured if f in existing]),
                 "added": added, "dropped": dropped, "kept_unmeasured": kept,
                 "measured_not_in_tests": stale}


def predicted_shards(durs, n):
    """The predicted minutes of each of N shards for the CI's selection under *durs*."""
    files, _skipped, isolated = runner._test_files(runner.DEFAULT_MARKERS)
    plan = runner._plan(files, durs, 2, runner.MAX_FILES_PER_CHUNK, isolated)
    return [sum(load for _c, load in runner._shard(plan, k, n)) / 60
            for k in range(1, n + 1)]


def unmarked_skips(timings):
    """CI's skipped tests that tests/needs_local_data.py neither marks nor lists as
    running nowhere: ({(file, test): {reasons}}, number listed as running nowhere)."""
    registry = runner._local_data_registry()
    out, nowhere = defaultdict(set), set()
    for t in timings:
        for f, test, reason in t.get("skipped") or []:
            if registry.is_marked(f, test):
                continue
            if registry.runs_nowhere(f, test):
                nowhere.add((f, test))
                continue
            out[(f, test)].add(reason)
    return out, len(nowhere)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="*", help="timings files or folders holding them")
    ap.add_argument("--run", help="a public CI run id: download its timing artifacts with gh")
    ap.add_argument("--repo", default=PUBLIC_REPO, help="the repository of --run")
    ap.add_argument("--platform", default="win32",
                    help="fold timings measured on this sys.platform (default win32, "
                         "the CI's longest shards; 'any' folds every OS, averaged)")
    ap.add_argument("--allow-partial", action="store_true",
                    help="fold even when a shard's timings are missing")
    ap.add_argument("--dry-run", action="store_true", help="report, write nothing")
    ap.add_argument("--skips", action="store_true",
                    help="list the tests that skipped on CI and are not marked "
                         "needs_local_data, then exit")
    args = ap.parse_args(argv)
    if not args.run and not args.paths:
        ap.error("give --run <id> or timings files")

    with tempfile.TemporaryDirectory(prefix="lane-timings-") as tmp:
        paths = _download(args.run, args.repo, tmp) if args.run else _expand(args.paths)
        timings = load_timings(paths)

    if args.skips:
        skips, nowhere = unmarked_skips(timings)
        for (f, test), reasons in sorted(skips.items()):
            print("%s::%s\n    %s" % (f, test or "(whole file)",
                                      " | ".join(sorted(reasons))[:300]))
        print("%d skipped on CI without the needs_local_data marker "
              "(and %d listed in RUNS_NOWHERE)" % (len(skips), nowhere))
        return 0

    chosen, notes = select(timings, args.platform, args.allow_partial)
    for n in notes:
        print("note: %s" % n)
    measured = measured_modules(chosen)
    old = runner._durations()
    existing = set(runner._all_test_files())
    new, rep = fold(old, measured, existing)

    shards = sorted({runner._parse_shard(t["shard"])[1] for t in chosen if t.get("shard")}) or [3]
    n = shards[-1]
    walls = sorted((t.get("shard") or "-", t.get("wall_seconds") or 0) for t in chosen)
    print("folding %d timings file(s), platform %s: %s"
          % (len(chosen), args.platform,
             ", ".join("shard %s %.1f min" % (s, w / 60) for s, w in walls)))
    print("  %d files measured, %d new, %d dropped (no longer in tests/), "
          "%d kept unmeasured" % (rep["updated"], len(rep["added"]),
                                  len(rep["dropped"]), len(rep["kept_unmeasured"])))
    if rep["measured_not_in_tests"]:
        print("  %d measured file(s) are not in tests/ now (ignored): %s"
              % (len(rep["measured_not_in_tests"]),
                 ", ".join(rep["measured_not_in_tests"][:5])))
    before = predicted_shards(old, n)
    after = predicted_shards(new, n)
    # Work, not wall time: a CI shard runs its chunks two at a time (-j 2).
    print("  predicted work per shard (chunk-minutes), old table: %s"
          % " / ".join("%.1f" % m for m in before))
    print("  predicted work per shard (chunk-minutes), new table: %s"
          % " / ".join("%.1f" % m for m in after))
    if args.dry_run:
        print("dry run: %s not written" % os.path.relpath(runner.DURATIONS, REPO_ROOT))
        return 0
    runner.write_durations(new)
    return 0


if __name__ == "__main__":
    runner._tolerant_console()
    sys.exit(main())

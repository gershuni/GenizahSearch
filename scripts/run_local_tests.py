# -*- coding: utf-8 -*-
"""Run the test suite in balanced, disposable, parallel chunks.

WHY NOT JUST `pytest tests/`
----------------------------
Measured 2026-09-09: `pytest tests/ -m "not gui and not render_smoke and not
atlas_bake"` as ONE process was killed after 2h35m without finishing. At that
point it held 38.4 GB resident and **91.6 GB of private commit** on a 63 GB
machine -- system commit 129.2 of 130.7 GB, 106 million page faults -- and was
using 45% of ONE core out of 24. It was not computing; it was paging.

The same selection, split into chunks of <=30 files with each chunk its own
process, finished in 90.9 minutes with no chunk exceeding 5.6 GB. Process
lifetime is the variable that mattered: nothing in this suite frees what it
allocates, so the fix is to let processes die.

WHY NOT xdist
-------------
xdist workers collect the whole selection and then persist across many files,
and `--dist loadfile` keeps a file's tests together without recycling the
worker. That is the shape that failed. These chunks are bounded in BOTH file
count and process lifetime, which is the property that made the run complete.
(`_tmp/fullsuite_xdist.log` also records 12 failures from an older xdist
attempt, including Tantivy lock contention -- historical, but not encouraging.)

HOW IT FAILS
------------
Loudly. A non-zero chunk exit, a chunk whose output cannot be parsed, a missing
result, a chunk that reports failures while exiting 0, or zero tests passed
overall all fail the run.

Pytest exit 5 (NO_TESTS_COLLECTED) is the subtle one, and it is split in two by
what the chunk's summary accounts for. "43 deselected in 0.29s" or "3 skipped
in 0.22s" means the tests were found and deliberately not run: legitimate, and
counted and printed rather than hidden. "no tests ran in 0.16s" accounts for
NOTHING, which is what a renamed module, a mistyped path or a swallowed
collection error looks like: that fails the run. Exempting every exit 5 -- as
this script did until Codex flagged it -- let one passing test in any other
chunk print "All chunks passed." over a chunk that ran nothing.

    python scripts/run_local_tests.py                 # the routine run
    python scripts/run_local_tests.py -j 6            # more lanes
    python scripts/run_local_tests.py --record        # refresh the balance file
    python scripts/run_local_tests.py -m "not gui"    # a different selection
    python scripts/run_local_tests.py --shard 2/3     # one CI job's share
    python scripts/run_local_tests.py --local-data    # the release's local lane

LOCAL-DATA LANE (RELEASES)
--------------------------
The public CI runs the full suite on every published commit. What it cannot run
is what needs the local data: tests that skip there for want of a sidecar,
the Tantivy index or the private repository, and the files tests/conftest.py
does not collect on CI. Those carry the `needs_local_data` marker
(tests/needs_local_data.py), and `--local-data` runs only them. It runs in the
main checkout -- a worktree has no data -- and FAILS when any of its tests
skips, because a test that skips here as well ran nowhere.

TIMINGS
-------
Each chunk writes a JUnit report; the runner reads each file's own test time
from it. `--record` writes those into tests/lane_durations.json; `--timings-out
FILE` writes them (and the skipped tests) as JSON, which the CI uploads and
scripts/fold_lane_timings.py folds into the same table.

SHARDS (CI)
-----------
`--shard K/N` runs only the K-th of N shares of the same plan, the chunks dealt
longest-first to the lightest share. Every job computes the identical plan from
the same files and `tests/lane_durations.json`, so the N shares cover every
chunk exactly once. CI runs the `tests` job as N such jobs per OS: one process
over the whole suite took 41-52 min on windows-latest and ~25 on Ubuntu, and
was the wall time of every PR.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DURATIONS = os.path.join(REPO_ROOT, "tests", "lane_durations.json")
# The pytest plugin that removes a passing test's tmp_path (_chunk_cmd), in a folder
# of its own: on PYTHONPATH, it makes only its own two modules importable.
LANE_PLUGIN_DIR = os.path.join(REPO_ROOT, "scripts", "lane_plugin")
LANE_PLUGIN = "genizah_lane_tmp_retention"


def _load_lane_rmtree():
    """genizah_lane_rmtree, by path: the removal function the plugin uses too."""
    spec = importlib.util.spec_from_file_location(
        "genizah_lane_rmtree", os.path.join(LANE_PLUGIN_DIR, "genizah_lane_rmtree.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_lane_rmtree = _load_lane_rmtree()
_rmtree = _lane_rmtree.rmtree
_extended_length = _lane_rmtree.extended_length

# CI's own selection (.github/workflows/ci.yml, the `tests` job), so a green
# local run means the same thing a green CI run does. `slow` is deliberately NOT
# excluded here: this repo leaves it in the default selection, and the one
# genuinely expensive slow test carries its own explicit opt-in instead.
DEFAULT_MARKERS = "not gui and not render_smoke and not atlas_bake"

TIMINGS_FORMAT = 1              # --timings-out's JSON; fold_lane_timings.py checks it

# --local-data: the release's local lane. Only what CI cannot run: the tests that
# skip there for want of local data and the files it does not collect
# (tests/needs_local_data.py lists them; tests/conftest.py marks them). The
# full suite is the public CI's job. Still within DEFAULT_MARKERS, because the
# gui and render-smoke lanes cannot share a process with these.
LOCAL_DATA_MARKER = "needs_local_data"
LOCAL_DATA_MARKERS = "%s and %s" % (LOCAL_DATA_MARKER, DEFAULT_MARKERS)
LOCAL_DATA_REGISTRY = os.path.join(REPO_ROOT, "tests", "needs_local_data.py")
MAX_FILES_PER_CHUNK = 30        # process lifetime cap -- the thing that fixed it
ISOLATE_ABOVE_SECONDS = 45.0    # a heavy module gets a lane to itself
DEFAULT_DURATION = 1.0          # unmeasured module: assume cheap, not free

# Lanes' temp files, under the system temp dir: one folder per run (_lanes_root).
LANES_DIR_NAME = "genizah-test-lanes"
_RUN_DIR_RE = re.compile(r"run-(\d+)-\d+$")
# No run lasts a day: a run folder this old without FINISHED_MARKER is a run that was
# killed, and the next run removes it. A finished failed run goes sooner (_clean_lanes).
_STALE_RUN_SECONDS = 24 * 3600
# Written by a failed run when it ends, after its last chunk: only a run folder holding
# it may be removed by another run before _STALE_RUN_SECONDS (2026-09-20 rule: a run
# never deletes a run that is still going).
FINISHED_MARKER = "FINISHED.txt"

# One INDEPENDENT search per category, because pytest does not emit them in a
# fixed order. A real chunk line from this suite's own failing run reads
#   "426 passed, 2 skipped, 53 deselected, 2 warnings, 200 errors in 24.15s"
# -- errors LAST -- and a failing chunk puts "failed" FIRST, before "passed".
# The previous single ordered pattern returned 0 for every category that
# appeared out of its expected position: that line parsed as 426/0/200/0/0,
# losing skipped and deselected, and "3 failed, 426 passed" lost the 426
# entirely -- which matters, because a zero `passed` total is one of the
# run-level failure conditions below.
_CATEGORIES = ("passed", "failed", "error", "skipped", "deselected")
_COUNT_RES = {c: re.compile(r"(\d+) %ss?\b" % c) for c in _CATEGORIES}

# pytest's counts line is the one ending in a duration -- "... in 24.15s".
# "no tests ran in 0.16s" has that shape too and MUST be recognised: it is a
# summary accounting for zero tests, not unparseable output.
#
# The trailing "(0:01:06)" is not optional decoration to skip: pytest appends a
# human-readable duration once a run exceeds 60 seconds, so a strict `...s$`
# anchor matches only the chunks that finished fast. That is not a hypothetical
# -- the first run after this anchor was introduced reported 9 of 22 chunks as
# "no parseable summary" for exactly this reason, every one of them a chunk that
# had passed. The stricter exit-5 rule is what surfaced it loudly instead of
# quietly dropping their counts, which is the whole point of it.
_SUMMARY_TAIL_RE = re.compile(r"\bin \d+\.\d+s(?:\s*\(\d+:\d\d:\d\d\))?\s*$")


# Directories whose tests CI runs in their own dedicated jobs, and which must
# not share a process with anything else, because pytest IMPORTS the modules at
# collection and that import alone is enough to break other tests in the same
# process.
#
# Measured: putting tests/render_smoke/test_start_render_smoke.py in the same
# process as the private findings-page test fails 7 findings-page tests, with every
# render_smoke test deselected. Alone, findings_page is 282 passed. The
# render_smoke lifespan teardown closes the event loop and drops NiceGUI's
# auto-index client, which is exactly why ci.yml gives these their own jobs.
#
# What happens to them depends on the ACTIVE marker expression, which is asked
# of pytest rather than parsed. If it deselects every test in the directory,
# skipping the directory changes nothing about which tests run, and it is
# skipped and REPORTED (never silently dropped). If it selects any of them,
# they are PLANNED IN THEIR OWN CHUNKS -- their tests run, in their own
# process, poisoning nobody.
#
# That distinction is Codex's second finding on this file and it was a real
# false green: `-m "not gui"`, advertised in this module's own docstring,
# selects 632 tests under these two directories, and the runner dropped all 632
# while printing "their tests are deselected by the markers anyway". A green
# run that silently omits 632 selected tests is the exact failure this script
# is supposed to be incapable of.
#
# tests/e2e/ WAS in this tuple and should not have been. It has no dedicated
# job in ci.yml -- the main `tests` job runs it, and the marker expression does
# not deselect `e2e` or `slow` -- so excluding it made this runner quietly
# narrower than the CI command it claims to mirror. The stated reason (import
# poisoning) was generalised from the render_smoke measurement and never
# checked; measured now, it is false: the private findings-page test is 282 passed both
# alone and sharing a process with tests/e2e/test_search_flow.py. The e2e
# modules importorskip selenium, which is in no requirements file, so today
# they skip in both places -- but a runner that silently drops them would stop
# matching CI the moment anyone installs selenium, with no signal. Codex
# flagged this; tests/test_run_local_tests_gate.py now pins it.
_DEDICATED_JOB_DIRS = ("tests/render_smoke/", "tests/atlas_bake/")


def _all_test_files():
    """Every test module in tests/, as repo-relative posix paths."""
    found = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(REPO_ROOT, "tests")):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in filenames:
            if name.startswith("test_") and name.endswith(".py"):
                p = os.path.join(dirpath, name)
                found.append(os.path.relpath(p, REPO_ROOT).replace("\\", "/"))
    return sorted(found)


_SELECTS_CACHE = {}


def _selects_anything(path, markers):
    """Does `markers` select any test under `path`? Ask pytest, don't parse it.

    A marker expression is arbitrary boolean text -- "not gui",
    "not (gui or render_smoke)", "render_smoke and not slow" -- so answering
    this by string-matching the expression is a guess, and a guess here decides
    whether tests run at all. pytest's own collector is the authority, costs
    about a second per directory, and is the whole difference between a runner
    that honours its -m flag and one that quietly overrides it.

    A collection ERROR raises rather than returning False: "I could not tell"
    must never resolve to "safe to skip".

    Memoised because the answer cannot change within one process, and because
    collecting tests/render_smoke/ costs real seconds -- the runner asks once
    per directory, but its own gate asks repeatedly.
    """
    key = (path, markers)
    if key in _SELECTS_CACHE:
        return _SELECTS_CACHE[key]
    pr = subprocess.run(
        [sys.executable, "-m", "pytest", path, "-m", markers,
         "--collect-only", "-q", "-p", "no:cacheprovider"],
        cwd=REPO_ROOT, env=dict(os.environ, PYTHONUTF8="1"),
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    if pr.returncode in (0, 5):
        # 5 is NO_TESTS_COLLECTED: the expression deselects every test here.
        _SELECTS_CACHE[key] = pr.returncode == 0
        return _SELECTS_CACHE[key]
    raise SystemExit(
        "cannot tell whether %r selects anything under %s: pytest "
        "--collect-only exited %d. Refusing to guess, because guessing wrong "
        "means silently not running those tests.\n%s"
        % (markers, path, pr.returncode,
           "\n".join((pr.stdout or "").splitlines()[-15:])))


def _test_files(markers):
    """(included, skipped, isolated) for the ACTIVE marker expression.

    `included` share chunks normally. `skipped` are the dedicated-job files the
    expression deselects entirely, so omitting them changes nothing. `isolated`
    is a list of file-lists -- one per dedicated directory whose tests this
    expression DOES select -- each of which gets its own chunk, so they run
    without sharing a process with anything they could poison.
    """
    found = _all_test_files()
    included = [f for f in found if not f.startswith(_DEDICATED_JOB_DIRS)]
    skipped, isolated = [], []
    for d in _DEDICATED_JOB_DIRS:
        group = [f for f in found if f.startswith(d)]
        if not group:
            continue
        if markers == DEFAULT_MARKERS:
            # The routine run pays no probe: under this exact expression the
            # answer is a constant, and the gate is what verifies it (see
            # tests/test_run_local_tests_gate.py, which asks pytest for real).
            # Measured: collecting tests/render_smoke/ costs 34 s, which would
            # be 10% of a 6-minute run spent re-learning a known fact. Any
            # OTHER expression is probed, because then it is genuinely unknown.
            selects = False
        else:
            selects = _selects_anything(d, markers)
        (isolated.append(group) if selects else skipped.extend(group))
    return included, sorted(skipped), isolated


def _local_data_registry():
    """tests/needs_local_data.py, loaded by path (importing it as `tests.` would
    run nothing else, but by path the runner needs no package on sys.path)."""
    spec = importlib.util.spec_from_file_location("needs_local_data", LOCAL_DATA_REGISTRY)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _local_data_files():
    """The test files holding a needs_local_data test: the --local-data plan.

    Only these are collected -- collecting all ~900 files to deselect nearly every
    test would cost more than the lane's own tests. A listed file that does not
    exist, or one under a dedicated-job directory, is an error, not a skip: a
    stale list would quietly run less than it claims."""
    files = sorted(_local_data_registry().files())
    on_disk = set(_all_test_files())
    missing = [f for f in files if f not in on_disk]
    dedicated = [f for f in files if f.startswith(_DEDICATED_JOB_DIRS)]
    if missing or dedicated:
        raise SystemExit(
            "tests/needs_local_data.py is out of date: %s%s"
            % ("not in tests/: %s. " % ", ".join(missing) if missing else "",
               "in a dedicated-job directory: %s" % ", ".join(dedicated)
               if dedicated else ""))
    return files


def _in_worktree():
    """Is REPO_ROOT a session worktree (.claude/worktrees/<name>)? Those have none
    of the gitignored data folders (CLAUDE.md)."""
    parts = os.path.normcase(os.path.abspath(REPO_ROOT)).replace("\\", "/").split("/")
    return any(parts[i:i + 2] == [".claude", "worktrees"] for i in range(len(parts)))


def _local_data_problems(results, allow_skips=False, registry=None):
    """The --local-data lane's own failures: (problems, skipped).

    - A test in it that skipped. Every test this lane selects skips on CI, so a
      test that skips HERE too ran nowhere -- and in a worktree, which has none of
      the data, that is nearly all of them. A green lane that ran nothing would be
      the false green this runner exists to prevent, so skips fail it, listed by
      reason. --allow-skips is for a deliberate partial run (a machine without one
      sidecar), and says so.
    - A chunk without a readable JUnit report: its skips cannot be seen, so the
      lane cannot vouch for it (Codex on PR #123).
    - A registry entry (*registry*: tests/needs_local_data.py) that matched no test
      in the run: a test renamed or removed while its file stayed marks nothing,
      and the lane would pass without it (Codex on PR #123)."""
    problems = []
    blind = sorted(idx for idx, r in results.items() if r.get("junit") is None)
    if blind:
        problems.append("%d chunk(s) wrote no readable JUnit report (chunks %s): "
                        "their skips cannot be checked, so the lane cannot pass"
                        % (len(blind), ", ".join(map(str, blind))))
    if registry is not None and not blind:
        ran = [t for r in results.values() for t in r["junit"]["tests"]]
        stale = [e for e in sorted(set(registry.TESTS) | set(registry.WHOLE_FILES))
                 if not any(registry._listed([e], f, t) for f, t in ran)]
        if stale:
            problems.append("%d entr(y/ies) of tests/needs_local_data.py matched no "
                            "test in this run -- renamed or removed? %s"
                            % (len(stale), "; ".join(stale)))
    skipped = sorted(s for r in results.values() if r.get("junit")
                     for s in r["junit"]["skipped"])
    if not skipped or allow_skips:
        return problems, skipped
    by_reason = {}
    for f, test, reason in skipped:
        by_reason.setdefault(reason, []).append("%s::%s" % (f, test) if test else f)
    lines = ["%d test(s) of the local-data lane skipped here, so they ran nowhere "
             "(CI skips them too). Run the lane where the data is -- the main "
             "checkout, not a worktree -- or pass --allow-skips for a deliberate "
             "partial run:" % len(skipped)]
    for reason, tests in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
        lines.append("    %4d  %s  (e.g. %s)" % (len(tests), reason[:110], tests[0]))
    return problems + ["\n".join(lines)], skipped


def _durations():
    try:
        with open(DURATIONS, encoding="utf-8") as fh:
            return json.load(fh).get("modules") or {}
    except (OSError, ValueError):
        print("! %s unreadable -- lanes will be balanced by file count only"
              % os.path.relpath(DURATIONS, REPO_ROOT))
        return {}


def _plan(files, durs, lanes, max_files, isolated=()):
    """Chunks of <=MAX_FILES_PER_CHUNK, balanced by measured seconds.

    Heavy modules are isolated first (a 124-second module sharing a chunk sets
    that chunk's floor), then the rest are packed longest-first into the
    currently-lightest chunk -- ordinary LPT scheduling.

    `isolated` is a list of file-groups that must not share a process with
    anything outside their own group. Each becomes its own chunk (split when
    larger than the file cap) and is never merged into the balanced set, so a
    render-smoke module cannot land beside a NiceGUI test.
    """
    dedicated = []
    for group in isolated:
        group = sorted(group)
        for i in range(0, len(group), max_files):
            part = group[i:i + max_files]
            dedicated.append((part, sum(durs.get(f, DEFAULT_DURATION)
                                        for f in part)))

    weighted = sorted(((durs.get(f, DEFAULT_DURATION), f) for f in files),
                      reverse=True)
    solo = [(d, f) for d, f in weighted if d >= ISOLATE_ABOVE_SECONDS]
    rest = [(d, f) for d, f in weighted if d < ISOLATE_ABOVE_SECONDS]

    chunks = [[f] for _d, f in solo]
    loads = [d for d, _f in solo]

    # Enough chunks that no chunk exceeds the file cap, and at least one per
    # lane so every lane has work.
    n = max(lanes, -(-len(rest) // max_files))
    for _ in range(n):
        chunks.append([])
        loads.append(0.0)
    base = len(solo)

    for d, f in rest:
        # lightest chunk that still has room
        best, best_load = None, None
        for i in range(base, len(chunks)):
            if len(chunks[i]) >= max_files:
                continue
            if best is None or loads[i] < best_load:
                best, best_load = i, loads[i]
        if best is None:                      # every chunk full: open another
            chunks.append([])
            loads.append(0.0)
            best = len(chunks) - 1
        chunks[best].append(f)
        loads[best] += d

    keep = [(c, loads[i]) for i, c in enumerate(chunks) if c]
    keep.sort(key=lambda cl: -cl[1])          # longest first: better packing
    # Dedicated groups go FIRST so the slowest lane starts on them rather than
    # picking them up last; they are already their own chunks either way.
    return dedicated + keep


def _parse_shard(spec):
    """(K, N) from "K/N", 1 <= K <= N; anything else is a usage error."""
    m = re.fullmatch(r"(\d+)/(\d+)", (spec or "").strip())
    if not m or not 1 <= int(m.group(1)) <= int(m.group(2)):
        raise SystemExit("--shard takes K/N with 1 <= K <= N, e.g. 2/3 (got %r)"
                         % spec)
    return int(m.group(1)), int(m.group(2))


def _shard(plan, k, n):
    """The K-th (1-based) of N shares of `plan`: chunks dealt longest-first,
    each to the share with the least predicted work so far (ties: the lowest
    share). Deterministic, so N jobs computing the same plan split it exactly."""
    loads = [0.0] * n
    shares = [[] for _ in range(n)]
    for chunk, load in sorted(plan, key=lambda cl: -cl[1]):
        i = min(range(n), key=lambda j: (loads[j], j))
        shares[i].append((chunk, load))
        loads[i] += load
    return shares[k - 1]


def _parse_counts(text):
    """(passed, failed, errors, skipped, deselected) from pytest's tail.

    None ONLY when pytest produced no summary line at all -- which the caller
    treats as a failure, because an unread result is not a passing one. A
    summary that accounts for zero tests ("no tests ran in 0.16s") parses to
    all zeros, so the caller can tell "every test here was deliberately
    deselected or skipped" apart from "this chunk collected nothing".
    """
    for line in reversed([ln for ln in text.splitlines() if ln.strip()]):
        if not _SUMMARY_TAIL_RE.search(line):
            continue
        counts = []
        for cat in _CATEGORIES:
            m = _COUNT_RES[cat].search(line)
            counts.append(int(m.group(1)) if m else 0)
        return tuple(counts)
    return None


def _verdict(n_chunks, results):
    """Decide the run. Pure, so the gate can test THIS and not a copy of it.

    Returns ((passed, failed, errors, skipped), empty, bad, problems). The run
    is green if and only if `problems` is empty, so every path to a green exit
    is enumerable by reading this one function -- which is the property Codex
    asked for and the reason it lives apart from the process plumbing.

    `empty` holds chunks that legitimately ran nothing (all deselected or all
    skipped); `bad` holds per-chunk failures; `problems` holds run-level ones.
    """
    passed = failed = errors = skipped = 0
    bad, unparsed, empty = [], [], []
    for idx in range(1, n_chunks + 1):
        r = results.get(idx)
        if r is None:
            bad.append((idx, "produced NO RESULT"))
            continue
        if r["counts"] is None:
            # No summary line at all: an OS-killed chunk, a crashed
            # interpreter, a pytest usage error (exit 4). Never a pass.
            unparsed.append(idx)
        else:
            p, f, e, s, d = r["counts"]
            passed += p
            failed += f
            errors += e
            skipped += s
            # pytest exit 5 is NO_TESTS_COLLECTED, and it is legitimate ONLY
            # when the chunk's tests were accounted for and deliberately not
            # run: "43 deselected in 0.29s" (the marker expression excluded
            # them) or "3 skipped in 0.22s" (a module-level importorskip).
            # It is a FAILURE when nothing is accounted for at all ("no tests
            # ran"), because that is what a renamed module, a mistyped path or
            # a swallowed collection error looks like. This used to exempt
            # EVERY exit 5 from both the count check and the non-zero-exit
            # check, so one passing test in any other chunk was enough to
            # print "All chunks passed." over a chunk that ran nothing --
            # reachable with --max-files 1, where a single entirely-deselected
            # module becomes its own chunk.
            if r["rc"] == 5:
                if p + f + e + s + d > 0:
                    empty.append(idx)
                else:
                    bad.append((idx, "exit 5 and accounted for NO tests at "
                                     "all -- the chunk collected nothing"))
                continue
        if r["rc"] != 0:
            bad.append((idx, "exit %d" % r["rc"]))

    problems = []
    if bad:
        problems.append("%d chunk(s) did not pass: %s"
                        % (len(bad), "; ".join("chunk %d %s" % b for b in bad)))
    if unparsed:
        problems.append("%d chunk(s) produced no parseable summary (chunks "
                        "%s) -- treated as a failure, because an unread "
                        "result is not a passing one"
                        % (len(unparsed), ", ".join(map(str, unparsed))))
    if passed == 0:
        problems.append("zero tests passed overall -- the selection collected "
                        "nothing, which is a failure and not a fast run")
    # Defence in depth: every failing chunk should already have a non-zero
    # exit, so this can only fire if a chunk reported failures and still
    # exited 0. That contradiction is a failure either way, and cheap to catch.
    if failed or errors:
        problems.append("%d failed and %d error(s) appear in the chunk "
                        "summaries -- reported as a failure even though the "
                        "exit codes did not say so" % (failed, errors))
    return (passed, failed, errors, skipped), empty, bad, problems


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-j", "--lanes", type=int, default=4,
                    help="concurrent pytest processes (default 4; NOT the core "
                         "count -- these are memory- and disk-bound)")
    ap.add_argument("-m", "--markers", default=DEFAULT_MARKERS)
    ap.add_argument("--max-files", type=int, default=MAX_FILES_PER_CHUNK)
    ap.add_argument("--record", action="store_true",
                    help="rewrite tests/lane_durations.json from this run")
    ap.add_argument("--timings-out", metavar="FILE",
                    help="write this run's per-file seconds and skipped tests "
                         "to FILE as JSON (CI uploads it; "
                         "scripts/fold_lane_timings.py folds it in)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan and exit")
    ap.add_argument("--shard", metavar="K/N",
                    help="run only the K-th of N balanced shares of the chunks "
                         "(CI runs one share per job)")
    ap.add_argument("--local-data", action="store_true",
                    help="the release's local lane: only the needs_local_data tests "
                         "(what CI skips or does not collect); fails when any of "
                         "them skips here")
    ap.add_argument("--allow-skips", action="store_true",
                    help="with --local-data: report skipped tests instead of failing")
    args = ap.parse_args(argv)
    shard = _parse_shard(args.shard) if args.shard else None
    if args.allow_skips and not args.local_data:
        ap.error("--allow-skips goes with --local-data")

    if args.local_data:
        if args.markers != DEFAULT_MARKERS:
            ap.error("--local-data sets its own marker expression (%s)"
                     % LOCAL_DATA_MARKERS)
        args.markers = LOCAL_DATA_MARKERS
        files, skipped, isolated = _local_data_files(), [], []
    else:
        files, skipped, isolated = _test_files(args.markers)
    durs = _durations()
    plan = _plan(files, durs, args.lanes, args.max_files, isolated)
    measured = sum(1 for f in files if f in durs)
    whole = plan
    if shard:
        plan = _shard(plan, *shard)

    n_isolated = sum(len(g) for g in isolated)
    print("=" * 78)
    print(" %d test files -> %d chunks over %d lanes (<=%d files/chunk)"
          % (len(files) + n_isolated, len(whole), args.lanes, args.max_files))
    print(" balance data: %d/%d modules measured, %.1f min of known work"
          % (measured, len(files), sum(durs.get(f, 0) for f in files) / 60))
    print(" markers: %s" % args.markers)
    if args.local_data and _in_worktree():
        print(" ! this is a worktree: it has none of the data these tests need, so")
        print("   they will skip and the lane will fail. Run it in the main checkout.")
    if shard:
        print(" shard %d/%d: %d of %d chunks, %d files, %.1f of %.1f min predicted"
              % (shard[0], shard[1], len(plan), len(whole),
                 sum(len(c) for c, _l in plan),
                 sum(l for _c, l in plan) / 60, sum(l for _c, l in whole) / 60))
    if isolated:
        print(" isolated: %d file(s) under %s -- this marker expression DOES"
              % (n_isolated, ", ".join(sorted(
                  set(d.rstrip("/") for g in isolated for f in g
                      for d in _DEDICATED_JOB_DIRS if f.startswith(d))))))
        print("           select them, so they run in chunk(s) of their own"
              " rather than being")
        print("           dropped -- importing them breaks other tests sharing"
              " the process")
        print("           (measured: 7 findings-page failures).")
    if skipped:
        print(" skipped: %d file(s) under %s -- this marker expression"
              " deselects EVERY"
              % (len(skipped), ", ".join(sorted(
                  set(d.rstrip("/") for f in skipped
                      for d in _DEDICATED_JOB_DIRS if f.startswith(d))))))
        print("          test in them (verified with pytest --collect-only),"
              " so omitting them")
        print("          changes nothing about which tests run. Run them with:")
        print("            pytest tests/render_smoke/ -m render_smoke")
        print("            pytest tests/atlas_bake/ -m atlas_bake")
    print("=" * 78)
    if args.dry_run:
        for i, (chunk, load) in enumerate(plan, 1):
            print(" chunk %2d  %2d files  %6.1fs predicted  %s"
                  % (i, len(chunk), load,
                     chunk[0] if len(chunk) == 1 else ""))
        return 0

    # OUTSIDE the repo, deliberately. A basetemp under REPO_ROOT makes every
    # `tmp_path` a directory INSIDE a git working tree, and two repository-scan
    # tests assert behaviour in a NON-git directory -- they failed on the first
    # run of this script for exactly that reason. pytest's own default lives in
    # the system temp dir; lanes live beside it, in a folder of this run's own
    # (_lanes_root). What stays there is decided in three places: pytest removes
    # each passing test's tmp_path (_chunk_cmd), a chunk that passed has its folder
    # removed when it ends (_chunk_passed), and the run's end removes the rest or
    # keeps the failing chunks' folders (_clean_lanes).
    tmp_root = _lanes_root()

    work = queue.Queue()
    for i, (chunk, load) in enumerate(plan, 1):
        work.put((i, chunk, load))
    results = {}
    leaves = {}
    lock = threading.Lock()

    def lane(lane_no):
        # A private basetemp per lane: pytest's tmp_path is otherwise rooted in
        # one shared directory, and concurrent processes would race on it.
        basetemp = os.path.join(tmp_root, "lane%d" % lane_no)
        # pytest's --basetemp is created with mkdir(parents=False): it does NOT
        # make intermediate directories, and a missing parent turns every
        # tmp_path fixture in the chunk into a FileNotFoundError at setup (370
        # errors, first time round). So the lane directory is created here and
        # pytest creates only the per-chunk leaf inside it.
        os.makedirs(basetemp, exist_ok=True)
        env = _chunk_env()
        while True:
            try:
                idx, chunk, load = work.get_nowait()
            except queue.Empty:
                return
            leaf = os.path.join(basetemp, "c%d" % idx)
            # Beside the leaf, not in it: pytest empties --basetemp when it starts.
            junit = leaf + ".junit.xml"
            with lock:
                leaves[idx] = leaf
            _rmtree(leaf)
            cmd = _chunk_cmd(leaf, args.markers, chunk, junit=junit)
            t0 = time.time()
            pr = subprocess.run(cmd, cwd=REPO_ROOT, env=env,
                                capture_output=True, text=True,
                                encoding="utf-8", errors="replace")
            dur = time.time() - t0
            counts = _parse_counts(pr.stdout or "")
            report = _read_junit(junit)
            try:
                os.remove(junit)
            except OSError:
                pass
            result = {
                "files": chunk, "predicted": load, "seconds": dur,
                "rc": pr.returncode, "counts": counts, "junit": report,
                "stdout": pr.stdout or "", "stderr": pr.stderr or "",
            }
            if _chunk_passed(result):
                # Its files are nobody's evidence: free the disk now, not at the
                # end of a 13-minute run (a run used to peak at ~17.5 GB).
                _rmtree(leaf)
            with lock:
                results[idx] = result
                tail = [ln for ln in (pr.stdout or "").splitlines()
                        if ln.strip()][-1:]
                state = ("ok" if pr.returncode == 0 else
                         "NO TESTS" if pr.returncode == 5 else "FAIL")
                print(" chunk %2d/%d  %2d files  %6.1fs (pred %5.1f)  %-8s %s"
                      % (idx, len(plan), len(chunk), dur, load, state,
                         (tail[0] if tail else "(no output)")[:52]),
                      flush=True)

    t_all = time.time()
    threads = [threading.Thread(target=lane, args=(i,), daemon=True)
               for i in range(args.lanes)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    wall = time.time() - t_all

    totals, empty, bad, problems = _verdict(len(plan), results)
    passed, failed, errors, skipped = totals
    if args.local_data:
        lane_problems, lane_skips = _local_data_problems(
            results, args.allow_skips, registry=_local_data_registry())
        problems += lane_problems
        if lane_skips and args.allow_skips:
            print(" --allow-skips: %d test(s) of the lane skipped and ran nowhere"
                  % len(lane_skips))

    print("\n" + "=" * 78)
    print(" wall %.1f min  |  %d passed, %d failed, %d errors, %d skipped"
          % (wall / 60, passed, failed, errors, skipped))
    if empty:
        print(" %d chunk(s) ran no tests because every test in them was "
              "deselected or skipped (chunks %s)"
              % (len(empty), ", ".join(map(str, empty))))
    print("=" * 78)

    if args.record:
        _record_durations(results)
    if args.timings_out:
        _write_timings(args.timings_out, results, args, wall)

    if problems:
        print("\nFAILED:")
        for p in problems:
            print("  - %s" % p)
        for idx, _why in bad:
            r = results.get(idx)
            if not r:
                continue
            print("\n--- chunk %d output (tail) ---" % idx)
            print("\n".join((r["stdout"] or "").splitlines()[-40:]))
            if (r["stderr"] or "").strip():
                print("--- stderr ---")
                print("\n".join(r["stderr"].splitlines()[-20:]))
        _clean_lanes(tmp_root, passed=False,
                     keep=[leaves[i] for i in sorted(leaves)
                           if not _chunk_passed(results.get(i))])
        return 1

    print("\nAll chunks passed.")
    _clean_lanes(tmp_root, passed=True)
    return 0


def _chunk_cmd(leaf, markers, chunk, junit=None):
    """The pytest command for one chunk, its tmp_path files under *leaf*; run it
    with _chunk_env().

    LANE_PLUGIN removes each test's tmp_path as soon as the test has passed setup,
    call and teardown, so a chunk keeps only its failing and erroring tests' folders
    (and what fixtures made with tmp_path_factory). Without it every test's folder
    stayed until the run ended: ~17.5 GB per full run, measured 2026-11-11, four of
    which filled drive C: in one morning.

    *junit*, when given, is where pytest writes the chunk's JUnit report (_read_junit):
    xunit1, the family whose test cases carry their file."""
    cmd = [sys.executable, "-m", "pytest", "-q", "--no-header",
           "-p", "no:cacheprovider",
           "-p", LANE_PLUGIN,
           "--basetemp", leaf,
           "-m", markers]
    if junit:
        cmd += ["--junitxml", junit, "-o", "junit_family=xunit1"]
    return cmd + list(chunk)


def _junit_file(case):
    """The repo-relative test file of one JUnit <testcase>, or None.

    xunit1 gives it as `file`. A module skipped as a whole at collection (a
    module-level importorskip) has no `file`: its dotted module path is the name
    or the classname, and that is mapped back to a file here."""
    path = case.get("file")
    if path:
        return path.replace("\\", "/")
    for dotted in (case.get("classname") or "", case.get("name") or ""):
        parts = dotted.split(".")
        for n in range(len(parts), 0, -1):
            cand = "/".join(parts[:n]) + ".py"
            if os.path.isfile(os.path.join(REPO_ROOT, cand)):
                return cand
    return None


def _read_junit(path):
    """Per-file test seconds and the skipped tests of one chunk's JUnit report.

    {"seconds": {file: s}, "tests": [[file, test], ...],
    "skipped": [[file, test, reason], ...]}, or None when the report is missing or
    unreadable (a chunk that crashed before pytest wrote it). `test` is the node id
    after the file (`name[param]`, `Class::name`; "" for a module skipped whole at
    collection). A test's time is pytest's `total`: setup, call and teardown. What
    a chunk spends outside its tests -- interpreter start, collection, imports --
    is not in here; _module_seconds spreads it over the chunk's files."""
    import xml.etree.ElementTree as ET
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError):
        return None
    seconds, tests, skipped = {}, [], []
    for case in root.iter("testcase"):
        f = _junit_file(case)
        if f is None:
            continue
        try:
            t = float(case.get("time") or 0.0)
        except ValueError:
            t = 0.0
        seconds[f] = seconds.get(f, 0.0) + t
        module = f[:-3].replace("/", ".")
        cls = case.get("classname") or ""
        name = case.get("name") or ""
        test = name
        if cls.startswith(module + "."):
            test = cls[len(module) + 1:] + "::" + name
        elif not cls or name == module or name == cls:
            test = ""                           # the whole module, at collection
        tests.append([f, test])
        skip = case.find("skipped")
        # JUnit files an expected failure (xfail) under <skipped> too; it ran.
        if skip is not None and skip.get("type") != "pytest.xfail":
            reason = skip.get("message") or ""
            if not reason or reason == "collection skipped":
                reason = (skip.text or "").strip() or reason
            skipped.append([f, test, reason])
    return {"seconds": seconds, "tests": tests, "skipped": skipped}


def _module_seconds(results):
    """Measured seconds per test file, from each chunk's JUnit report.

    A file's share is its tests' own time plus an equal part of what its chunk
    spent outside any test (start-up, collection, imports): the chunk's wall time
    is the number the lanes are balanced against. A chunk without a report (it
    crashed) is spread over its files by their previous weight, as --record did
    before the reports existed. Balance data only."""
    durs = _durations()
    out = {}
    for r in results.values():
        chunk, secs = r["files"], r["seconds"]
        junit = r.get("junit")
        if junit is None:
            prev = [durs.get(f, DEFAULT_DURATION) for f in chunk]
            total_prev = sum(prev) or float(len(chunk))
            for f, p in zip(chunk, prev):
                out[f] = round(secs * (p / total_prev), 2)
            continue
        own = {f: junit["seconds"].get(f, 0.0) for f in chunk}
        overhead = max(0.0, secs - sum(own.values())) / len(chunk)
        for f in chunk:
            out[f] = round(own[f] + overhead, 2)
    return out


def _write_timings(path, results, args, wall):
    """The run's per-file seconds and skipped tests, as JSON, for CI to upload.

    scripts/fold_lane_timings.py folds these into tests/lane_durations.json, so the
    CI shards are balanced by what CI itself measured, without a local full run."""
    skipped = sorted(s for r in results.values() if r.get("junit")
                     for s in r["junit"]["skipped"])
    payload = {
        "_comment": "Written by scripts/run_local_tests.py --timings-out; folded "
                    "into tests/lane_durations.json by scripts/fold_lane_timings.py.",
        "format": TIMINGS_FORMAT,
        "measured": time.strftime("%Y-%m-%d %H:%M:%S"),
        "platform": sys.platform,
        "markers": args.markers,
        "shard": args.shard,
        "lanes": args.lanes,
        "wall_seconds": round(wall, 1),
        "chunks_without_report": sum(1 for r in results.values()
                                     if r.get("junit") is None),
        "modules": dict(sorted(_module_seconds(results).items())),
        "skipped": skipped,
    }
    folder = os.path.dirname(os.path.abspath(path))
    os.makedirs(folder, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    print("wrote %d module timings and %d skipped tests -> %s"
          % (len(payload["modules"]), len(skipped), path))


def _chunk_env(base=None):
    """The environment a chunk runs in: UTF-8, no PostHog, LANE_PLUGIN importable."""
    env = dict(os.environ if base is None else base, PYTHONUTF8="1", POSTHOG_API_KEY="")
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (LANE_PLUGIN_DIR, env.get("PYTHONPATH")) if p)
    return env


def _chunk_passed(r):
    """Did this chunk pass cleanly, so its temp files are evidence of nothing?

    The same per-chunk test _verdict applies: a result exists and parsed, nothing
    failed or errored, and the exit is 0 -- or 5 with every test accounted for as
    deselected or skipped. Anything else keeps its folder."""
    if r is None or r["counts"] is None:
        return False
    p, f, e, s, d = r["counts"]
    if f or e:
        return False
    if r["rc"] == 5:
        return p + s + d > 0
    return r["rc"] == 0


def _lanes_root(now=None):
    """A temp folder of this run's own, `genizah-test-lanes/run-<start>-<pid>`.

    The folder used to be shared, and every run began by deleting it, so a second
    run started meanwhile destroyed the first one's live `tmp_path` folders: 133
    errors, measured 2026-09-20. Now a run starts by deleting only runs that started
    more than _STALE_RUN_SECONDS ago (by then every run has ended: one without
    FINISHED_MARKER was killed, or kept by the runner before 2026-11-11), and anything
    not named like a run (the old shared layout's `lane<N>`). A finished failed run is
    removed sooner, by the next failed run's end (_clean_lanes)."""
    now = time.time() if now is None else now
    parent = os.path.join(tempfile.gettempdir(), LANES_DIR_NAME)
    os.makedirs(parent, exist_ok=True)
    for name in os.listdir(parent):
        m = _RUN_DIR_RE.match(name)
        if m is None or now - int(m.group(1)) > _STALE_RUN_SECONDS:
            _rmtree(os.path.join(parent, name))
    root = os.path.join(parent, "run-%d-%d" % (int(now), os.getpid()))
    os.makedirs(root)
    return root


def _clean_lanes(tmp_root, passed, keep=()):
    """Remove the lanes' temp files at the end of a run, except a failed run's evidence.

    A run that passed removes its whole folder. A run that failed keeps only the chunk
    folders in *keep* (the failing chunks', in which pytest has already removed every
    passing test's tmp_path), marks itself finished, and removes every earlier finished
    run: the latest failure is what one inspects next.

    Measured 2026-11-11: a failed run kept every chunk's folder, ~17.5 GB, for a day, and
    with 11 tests failing on master every run failed; four in one morning filled drive C:
    and broke the fifth with hundreds of tmp_path errors. A run never removes a folder
    without FINISHED_MARKER before it is a day old, so a run still going is safe."""
    if passed:
        _rmtree(tmp_root)
        if os.path.exists(tmp_root):
            print(" ! some lane temp files could not be removed (a file still open?): %s"
                  % tmp_root)
        return
    keep = {os.path.normcase(os.path.abspath(p)) for p in keep}
    for lane in os.listdir(tmp_root):
        lane_dir = os.path.join(tmp_root, lane)
        if not os.path.isdir(lane_dir):
            continue
        for leaf in os.listdir(lane_dir):
            path = os.path.join(lane_dir, leaf)
            if os.path.normcase(os.path.abspath(path)) not in keep:
                _rmtree(path)
        try:
            os.rmdir(lane_dir)             # only if no kept chunk is in it
        except OSError:
            pass
    with open(os.path.join(tmp_root, FINISHED_MARKER), "w", encoding="utf-8",
              newline="\n") as fh:
        fh.write("This run failed and ended %s. Kept: the folders of its failing "
                 "chunks.\nThe next failed run removes this folder.\n"
                 % time.strftime("%Y-%m-%d %H:%M:%S"))
    _remove_earlier_finished_runs(tmp_root)
    print(" lane temp files of the failing chunk(s) kept for inspection (%.1f MB): %s"
          % (_folder_bytes(tmp_root) / 1e6, tmp_root))


def _remove_earlier_finished_runs(tmp_root):
    """Remove the run folders that started before *tmp_root*'s and hold FINISHED_MARKER.

    Earlier, not every: two failed runs ending at once would otherwise delete each
    other. A folder without the marker is a run still going (or killed), left to
    _lanes_root's day-old sweep."""
    parent, own = os.path.split(tmp_root)
    m = _RUN_DIR_RE.match(own)
    if m is None:
        return
    own_start = int(m.group(1))
    for name in os.listdir(parent):
        other = _RUN_DIR_RE.match(name)
        path = os.path.join(parent, name)
        if (other is not None and name != own and int(other.group(1)) < own_start
                and os.path.isfile(os.path.join(path, FINISHED_MARKER))):
            _rmtree(path)


def _folder_bytes(path):
    total = 0
    for dirpath, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(dirpath, name))
            except OSError:
                pass
    return total


def _record_durations(results):
    """Rewrite the balance file from this run's measured per-file time.

    Each file's own tests' time from the chunk's JUnit report, plus its share of
    the chunk's start-up (_module_seconds). Until 2026-11-24 the runner saw only
    per-CHUNK time and spread it by the files' previous weight, so a module first
    recorded at the 1-second default stayed light however long it took. Balance
    data only -- a wrong number here costs packing quality, never correctness.
    """
    updated = dict(_durations())
    updated.update(_module_seconds(results))
    write_durations(updated)


def write_durations(modules):
    """Write tests/lane_durations.json (also scripts/fold_lane_timings.py's writer)."""
    payload = {
        "_comment": "Measured per-module pytest seconds, used ONLY to balance "
                    "lanes in scripts/run_local_tests.py. Stale entries cost "
                    "balance, never correctness; refresh with --record.",
        "_measured": time.strftime("%Y-%m-%d"),
        "modules": dict(sorted(modules.items())),
    }
    with open(DURATIONS, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    print("recorded %d module timings -> %s"
          % (len(modules), os.path.relpath(DURATIONS, REPO_ROOT)))


def _tolerant_console(streams=None):
    """Make *streams* (stdout and stderr) replace what their encoding cannot show.

    A failing chunk's output is Hebrew-heavy; on a Windows console (cp1252 on the CI
    runner) printing it raised UnicodeEncodeError, which ended the run without the
    name of the test that failed (PR #428, 2026-10-21)."""
    for stream in streams if streams is not None else (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass


if __name__ == "__main__":
    _tolerant_console()
    sys.exit(main())

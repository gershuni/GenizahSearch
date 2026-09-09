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
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DURATIONS = os.path.join(REPO_ROOT, "tests", "lane_durations.json")

# CI's own selection (.github/workflows/ci.yml, the `tests` job), so a green
# local run means the same thing a green CI run does. `slow` is deliberately NOT
# excluded here: this repo leaves it in the default selection, and the one
# genuinely expensive slow test carries its own explicit opt-in instead.
DEFAULT_MARKERS = "not gui and not render_smoke and not atlas_bake"

MAX_FILES_PER_CHUNK = 30        # process lifetime cap -- the thing that fixed it
ISOLATE_ABOVE_SECONDS = 45.0    # a heavy module gets a lane to itself
DEFAULT_DURATION = 1.0          # unmeasured module: assume cheap, not free

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
# not share a process with anything else. The marker expression already
# deselects their tests -- but pytest still IMPORTS the modules at collection,
# and that import alone is enough to break other tests in the same process.
#
# Measured: putting tests/render_smoke/test_start_render_smoke.py in the same
# process as tests/test_findings_page.py fails 7 findings-page tests, with every
# render_smoke test deselected. Alone, findings_page is 282 passed. The
# render_smoke lifespan teardown closes the event loop and drops NiceGUI's
# auto-index client, which is exactly why ci.yml gives these their own jobs.
#
# So they are excluded here and REPORTED, never silently dropped -- the header
# prints the count and the command that runs them.
#
# tests/e2e/ WAS in this tuple and should not have been. It has no dedicated
# job in ci.yml -- the main `tests` job runs it, and the marker expression does
# not deselect `e2e` or `slow` -- so excluding it made this runner quietly
# narrower than the CI command it claims to mirror. The stated reason (import
# poisoning) was generalised from the render_smoke measurement and never
# checked; measured now, it is false: test_findings_page.py is 282 passed both
# alone and sharing a process with tests/e2e/test_search_flow.py. The e2e
# modules importorskip selenium, which is in no requirements file, so today
# they skip in both places -- but a runner that silently drops them would stop
# matching CI the moment anyone installs selenium, with no signal. Codex
# flagged this; tests/test_run_local_tests_gate.py now pins it.
_DEDICATED_JOB_DIRS = ("tests/render_smoke/", "tests/atlas_bake/")


def _test_files():
    """(included, excluded) test modules, as repo-relative posix paths."""
    found = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(REPO_ROOT, "tests")):
        dirnames[:] = [d for d in dirnames if d != "__pycache__"]
        for name in filenames:
            if name.startswith("test_") and name.endswith(".py"):
                p = os.path.join(dirpath, name)
                found.append(os.path.relpath(p, REPO_ROOT).replace("\\", "/"))
    included = sorted(f for f in found
                      if not f.startswith(_DEDICATED_JOB_DIRS))
    excluded = sorted(f for f in found if f.startswith(_DEDICATED_JOB_DIRS))
    return included, excluded


def _durations():
    try:
        with open(DURATIONS, encoding="utf-8") as fh:
            return json.load(fh).get("modules") or {}
    except (OSError, ValueError):
        print("! %s unreadable -- lanes will be balanced by file count only"
              % os.path.relpath(DURATIONS, REPO_ROOT))
        return {}


def _plan(files, durs, lanes, max_files):
    """Chunks of <=MAX_FILES_PER_CHUNK, balanced by measured seconds.

    Heavy modules are isolated first (a 124-second module sharing a chunk sets
    that chunk's floor), then the rest are packed longest-first into the
    currently-lightest chunk -- ordinary LPT scheduling.
    """
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
    return keep


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
    ap.add_argument("--dry-run", action="store_true",
                    help="print the plan and exit")
    args = ap.parse_args(argv)

    files, excluded = _test_files()
    durs = _durations()
    plan = _plan(files, durs, args.lanes, args.max_files)
    measured = sum(1 for f in files if f in durs)

    print("=" * 78)
    print(" %d test files -> %d chunks over %d lanes (<=%d files/chunk)"
          % (len(files), len(plan), args.lanes, args.max_files))
    print(" balance data: %d/%d modules measured, %.1f min of known work"
          % (measured, len(files), sum(durs.get(f, 0) for f in files) / 60))
    print(" markers: %s" % args.markers)
    if excluded:
        print(" excluded: %d file(s) under %s -- their tests are deselected by"
              % (len(excluded), ", ".join(d.rstrip("/") for d in _DEDICATED_JOB_DIRS)))
        print("           the markers anyway, but IMPORTING them breaks other"
              " tests in the")
        print("           same process (measured: 7 findings-page failures)."
              " Run them with:")
        print("             pytest tests/render_smoke/ -m render_smoke")
        print("             pytest tests/atlas_bake/ -m atlas_bake")
    print("=" * 78)
    if args.dry_run:
        for i, (chunk, load) in enumerate(plan, 1):
            print(" chunk %2d  %2d files  %6.1fs predicted  %s"
                  % (i, len(chunk), load,
                     chunk[0] if len(chunk) == 1 else ""))
        return 0

    # OUTSIDE the repo, deliberately. A basetemp under REPO_ROOT makes every
    # `tmp_path` a directory INSIDE a git working tree, and two masking-scan
    # tests assert behaviour in a NON-git directory -- they failed on the first
    # run of this script for exactly that reason. pytest's own default lives in
    # the system temp dir; lanes live beside it.
    tmp_root = os.path.join(tempfile.gettempdir(), "genizah-test-lanes")
    shutil.rmtree(tmp_root, ignore_errors=True)
    os.makedirs(tmp_root, exist_ok=True)

    work = queue.Queue()
    for i, (chunk, load) in enumerate(plan, 1):
        work.put((i, chunk, load))
    results = {}
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
        env = dict(os.environ, PYTHONUTF8="1")
        while True:
            try:
                idx, chunk, load = work.get_nowait()
            except queue.Empty:
                return
            leaf = os.path.join(basetemp, "c%d" % idx)
            shutil.rmtree(leaf, ignore_errors=True)
            cmd = [sys.executable, "-m", "pytest", "-q", "--no-header",
                   "-p", "no:cacheprovider",
                   "--basetemp", leaf,
                   "-m", args.markers] + chunk
            t0 = time.time()
            pr = subprocess.run(cmd, cwd=REPO_ROOT, env=env,
                                capture_output=True, text=True,
                                encoding="utf-8", errors="replace")
            dur = time.time() - t0
            counts = _parse_counts(pr.stdout or "")
            with lock:
                results[idx] = {
                    "files": chunk, "predicted": load, "seconds": dur,
                    "rc": pr.returncode, "counts": counts,
                    "stdout": pr.stdout or "", "stderr": pr.stderr or "",
                }
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
        return 1

    print("\nAll chunks passed.")
    return 0


def _record_durations(results):
    """Rewrite the balance file from measured per-chunk time.

    Per-CHUNK, spread over its files by their previous weight: this runner does
    not see per-module timings, and a chunk's own total is the honest thing it
    does know. Balance data only -- a wrong number here costs packing quality,
    never correctness.
    """
    durs = _durations()
    updated = dict(durs)
    for r in results.values():
        chunk, secs = r["files"], r["seconds"]
        prev = [durs.get(f, DEFAULT_DURATION) for f in chunk]
        total_prev = sum(prev) or float(len(chunk))
        for f, p in zip(chunk, prev):
            updated[f] = round(secs * (p / total_prev), 2)
    payload = {
        "_comment": "Measured per-module pytest seconds, used ONLY to balance "
                    "lanes in scripts/run_local_tests.py. Stale entries cost "
                    "balance, never correctness; refresh with --record.",
        "_measured": time.strftime("%Y-%m-%d"),
        "modules": dict(sorted(updated.items())),
    }
    with open(DURATIONS, "w", encoding="utf-8", newline="\n") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=1)
    print("recorded %d module timings -> %s"
          % (len(updated), os.path.relpath(DURATIONS, REPO_ROOT)))


if __name__ == "__main__":
    sys.exit(main())

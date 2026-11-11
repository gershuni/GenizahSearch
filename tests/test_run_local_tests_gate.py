# -*- coding: utf-8 -*-
"""Can `scripts/run_local_tests.py` report success while tests did not run?

That is the only question this file asks, and it asks it exhaustively, because
the runner is now the advertised way to run this suite: if it can print "All
chunks passed." over a run that executed nothing, every other gate in the repo
is reporting through a broken speaker.

Two of the paths tested here were real. Codex found both on PR #337:

  1. `tests/e2e/` was excluded unconditionally, on the stated grounds that CI
     ran it in a dedicated job. There is no e2e job in ci.yml -- the main
     `tests` job runs it -- and the marker expression does not deselect `e2e`,
     so the runner was quietly narrower than the command it claims to mirror.
  2. pytest exit 5 (NO_TESTS_COLLECTED) was exempted from BOTH the count check
     and the non-zero-exit check, so a chunk that ran nothing was forgiven as
     long as one test passed in some other chunk.

`_verdict` is a pure function precisely so this file can drive the real
decision logic rather than re-implement it. Every path to a green run is a path
through that one function, so "enumerate every way this can go green" is a
finite reading task and not a guess.
"""
from __future__ import annotations

import importlib.util
import os
import re
import subprocess
import time

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUNNER_PATH = os.path.join(REPO_ROOT, "scripts", "run_local_tests.py")
CI_YML = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")


def _load_runner():
    """Import scripts/run_local_tests.py by path -- `scripts/` is no package.

    Named uniquely so it cannot shadow, or be shadowed by, anything on
    sys.path; this suite has been bitten by a stub module surviving in
    sys.modules and being imported by a later test.
    """
    spec = importlib.util.spec_from_file_location(
        "_genizah_run_local_tests_under_test", RUNNER_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


runner = _load_runner()


def _chunk(rc, summary):
    """One chunk result shaped exactly as the lane threads record it."""
    return {
        "files": ["tests/test_example.py"],
        "predicted": 1.0,
        "seconds": 1.0,
        "rc": rc,
        "counts": runner._parse_counts(summary),
        "stdout": summary,
        "stderr": "",
    }


@pytest.fixture
def probe_says_selected():
    """Seed the runner's probe cache so the branching can be tested cheaply.

    `_selects_anything` shells out to `pytest --collect-only`, and collecting
    tests/render_smoke/ is 34 seconds. The tests below are about what
    `_test_files` DOES with the answer, not about the answer -- which
    test_a_directory_is_only_skipped_when_the_markers_really_deselect_it
    verifies for real. Seeding the cache keeps them at milliseconds.

    The cache is restored afterwards: a seeded entry left behind would make a
    later test's real probe return this fixture's answer instead of pytest's.
    """
    before = dict(runner._SELECTS_CACHE)

    def seed(markers, selected=True):
        for d in runner._DEDICATED_JOB_DIRS:
            runner._SELECTS_CACHE[(d, markers)] = selected

    yield seed
    runner._SELECTS_CACHE.clear()
    runner._SELECTS_CACHE.update(before)


def _is_green(results, n_chunks=None):
    _totals, _empty, _bad, problems = runner._verdict(
        n_chunks if n_chunks is not None else len(results), results)
    return not problems


# ---------------------------------------------------------------------------
# The summary parser. It feeds every count the verdict reasons about, so a
# mis-read here is a mis-read everywhere downstream.
# ---------------------------------------------------------------------------

# Every line below is a real pytest summary shape, and the first two are
# verbatim from this suite's own runs. Note the ORDER: pytest puts `errors`
# after `warnings` at the END, and `failed` FIRST, before `passed`. An ordered
# single-pattern parser (which this script had) silently returns 0 for every
# category out of its expected position -- it read line 1 as 426/0/200/0/0,
# losing skipped and deselected, and line 2 as 0/3/0/2/0, losing all 426
# passed. A lost `passed` matters: a zero total is a run-level failure
# condition, so the parser's ordering assumption could have turned a real
# result into a misleading diagnosis of the wrong problem.
@pytest.mark.parametrize("line,expected", [
    ("426 passed, 2 skipped, 53 deselected, 2 warnings, 200 errors in 24.15s",
     (426, 0, 200, 2, 53)),
    ("3 failed, 426 passed, 2 skipped in 10.00s", (426, 3, 0, 2, 0)),
    ("282 passed, 1 skipped, 7 deselected, 2 warnings in 23.91s",
     (282, 0, 0, 1, 7)),
    ("11406 passed, 40 skipped in 419.62s", (11406, 0, 0, 40, 0)),
    ("43 deselected in 0.29s", (0, 0, 0, 0, 43)),
    ("3 skipped in 0.22s", (0, 0, 0, 3, 0)),
    ("1 error in 0.05s", (0, 0, 1, 0, 0)),
    # Over 60 seconds pytest appends a human-readable duration, and MOST
    # chunks in a real run are over 60 seconds. Omitting this shape from the
    # test data is how the first version of the tail anchor shipped: 9 of 22
    # passing chunks were reported as having no parseable summary.
    ("740 passed, 23 deselected, 2 warnings in 67.31s (0:01:07)",
     (740, 0, 0, 0, 23)),
    ("816 passed, 3 skipped, 117 deselected in 101.74s (0:01:41)",
     (816, 0, 0, 3, 117)),
    # The whole point of the exit-5 split: this accounts for ZERO tests.
    ("no tests ran in 0.16s", (0, 0, 0, 0, 0)),
])
def test_the_summary_parser_reads_every_category_whatever_the_order(
        line, expected):
    assert runner._parse_counts(line) == expected


def test_the_summary_parser_reads_the_last_summary_not_an_earlier_line():
    """A chunk prints progress, a short summary, then its counts line last."""
    out = ("tests/test_a.py .F\n"
           "=== short test summary info ===\n"
           "FAILED tests/test_a.py::test_b - assert 1 == 2\n"
           "1 failed, 1 passed in 3.21s\n")
    assert runner._parse_counts(out) == (1, 1, 0, 0, 0)


@pytest.mark.parametrize("text", [
    "",                                   # an OS-killed chunk: no output
    "Traceback (most recent call last):\nMemoryError\n",
    "ERROR: file or directory not found: tests/test_typo.py\n",
])
def test_the_summary_parser_returns_none_when_pytest_never_summarised(text):
    """None is the signal for 'unread', which the verdict treats as failure."""
    assert runner._parse_counts(text) is None


# ---------------------------------------------------------------------------
# The verdict. One green case, then every way to reach a red one.
# ---------------------------------------------------------------------------

def test_a_normal_run_is_green():
    """The control. Without this, every red assertion below proves nothing."""
    assert _is_green({
        1: _chunk(0, "500 passed, 3 skipped in 60.00s"),
        2: _chunk(0, "480 passed, 12 deselected in 55.00s"),
    })


def test_a_chunk_that_collected_nothing_at_all_fails_the_run():
    """Codex finding 2, and the exact shape it named.

    Chunk 2 exited 5 having accounted for no tests whatsoever. Before the fix
    this `continue`d past both the count check and the non-zero-exit check, so
    chunk 1's 500 passing tests carried the run to "All chunks passed."
    """
    results = {
        1: _chunk(0, "500 passed in 60.00s"),
        2: _chunk(5, "no tests ran in 0.16s"),
    }
    _totals, _empty, bad, problems = runner._verdict(2, results)
    assert problems, "a chunk that ran nothing must fail the run"
    assert any(idx == 2 for idx, _why in bad)
    assert "collected nothing" in " ".join(why for _idx, why in bad)


@pytest.mark.parametrize("summary", [
    "43 deselected in 0.29s",   # the marker expression excluded every test
    "3 skipped in 0.22s",       # a module-level importorskip
])
def test_a_chunk_whose_tests_were_all_accounted_for_is_allowed_and_counted(
        summary):
    """The legitimate exit 5: the tests were found and deliberately not run.

    This is why the fix is a split and not a blanket "exit 5 fails" -- that
    would redden a correct run, and a gate that cries wolf gets switched off.
    """
    results = {
        1: _chunk(0, "500 passed in 60.00s"),
        2: _chunk(5, summary),
    }
    _totals, empty, _bad, problems = runner._verdict(2, results)
    assert not problems
    assert empty == [2], "it must be reported, not silently forgiven"


def test_a_chunk_killed_by_the_os_with_no_output_at_all_fails():
    """No stdout, so no summary, so no counts -- and never a pass."""
    results = {
        1: _chunk(0, "500 passed in 60.00s"),
        2: _chunk(-9, ""),
    }
    assert runner._parse_counts("") is None
    _totals, _empty, _bad, problems = runner._verdict(2, results)
    assert problems


def test_a_chunk_that_exited_zero_but_printed_no_summary_fails():
    """An unread result is not a passing one, whatever the exit code says."""
    results = {
        1: _chunk(0, "500 passed in 60.00s"),
        2: _chunk(0, "collecting ...\n"),
    }
    _totals, _empty, _bad, problems = runner._verdict(2, results)
    assert problems
    assert any("no parseable summary" in p for p in problems)


def test_a_chunk_that_produced_no_result_at_all_fails():
    """A lane thread that died before recording leaves a hole in `results`."""
    results = {1: _chunk(0, "500 passed in 60.00s")}
    _totals, _empty, bad, problems = runner._verdict(2, results)
    assert problems
    assert any("NO RESULT" in why for _idx, why in bad)


def test_failures_in_a_summary_fail_the_run_even_when_the_exit_code_is_zero():
    """Defence in depth against an exit code that disagrees with the output."""
    results = {1: _chunk(0, "3 failed, 500 passed in 60.00s")}
    _totals, _empty, _bad, problems = runner._verdict(1, results)
    assert problems
    assert any("did not say so" in p for p in problems)


def test_a_run_where_nothing_passed_anywhere_fails():
    """Every chunk legitimately empty is still a run that proved nothing."""
    results = {
        1: _chunk(5, "20 deselected in 0.20s"),
        2: _chunk(5, "18 deselected in 0.18s"),
    }
    _totals, _empty, _bad, problems = runner._verdict(2, results)
    assert problems
    assert any("zero tests passed overall" in p for p in problems)


def test_a_nonzero_exit_fails_even_with_a_healthy_looking_summary():
    """The ordinary case, kept explicit so a refactor cannot lose it."""
    results = {1: _chunk(1, "3 failed, 500 passed in 60.00s")}
    _totals, _empty, _bad, problems = runner._verdict(1, results)
    assert problems


# ---------------------------------------------------------------------------
# Selection. A green verdict over the wrong file set is the quieter failure.
# ---------------------------------------------------------------------------

def test_the_e2e_directory_is_in_the_default_selection():
    """Codex finding 1, pinned so the exclusion cannot come back.

    It was excluded on the stated grounds that CI ran it separately. It does
    not: there is no e2e job in ci.yml, and `not e2e` is not in the marker
    expression. Measured at the time: sharing a process with
    tests/e2e/test_search_flow.py leaves the findings-page suite at 282
    passed, exactly as it is alone -- so the import-poisoning reason given for
    the exclusion was not true either.
    """
    included, skipped, isolated = runner._test_files(runner.DEFAULT_MARKERS)
    everything = list(included) + list(skipped) + [f for g in isolated for f in g]
    e2e = [f for f in everything if f.startswith("tests/e2e/")]
    assert e2e, "expected tests/e2e/ test modules to exist"
    assert set(e2e) <= set(included), (
        "tests/e2e/ is run by CI's main `tests` job and is not deselected by "
        "the marker expression, so excluding it makes this runner narrower "
        "than the command it claims to mirror")


def test_a_directory_is_only_skipped_when_the_markers_really_deselect_it():
    """The invariant whose absence let tests/e2e/ be dropped.

    A dedicated directory may be skipped ONLY if the active expression
    deselects every test in it -- then skipping changes nothing about which
    tests run, and only prevents a poisoning import.

    This is the ONE test that asks pytest for real, and it is why the runner is
    allowed its no-probe fast path for the default expression: the constant the
    runner assumes is asserted here against the collector, so the assumption is
    checked once per suite run rather than trusted forever or re-measured on
    every run. It costs about 40 seconds -- collecting tests/render_smoke/
    alone is 34 -- which is the price of not silently dropping 632 tests.
    """
    _included, skipped, isolated = runner._test_files(runner.DEFAULT_MARKERS)
    assert not isolated, (
        "under the default expression nothing should need isolating; the "
        "runner's fast path assumes exactly that")
    for path in runner._DEDICATED_JOB_DIRS:
        assert any(f.startswith(path) for f in skipped), (
            "%s should be in the skipped set under the default expression"
            % path)
        assert not runner._selects_anything(path, runner.DEFAULT_MARKERS), (
            "%s is skipped on the runner's fast path, but pytest "
            "--collect-only says %r SELECTS tests in it -- the fast path is "
            "now dropping tests that would otherwise run"
            % (path, runner.DEFAULT_MARKERS))


def test_a_custom_marker_expression_cannot_silently_drop_a_whole_directory(
        probe_says_selected):
    """Codex finding 3, and it was a live false green.

    `-m "not gui"` is advertised in the runner's own docstring, and it selects
    632 tests under tests/render_smoke/ and tests/atlas_bake/. `_test_files`
    excluded those directories unconditionally, so the runner dropped all 632
    and printed "their tests are deselected by the markers anyway" -- then
    exited green. Under a selecting expression they must now be ISOLATED (their
    own chunks) rather than skipped, and every one must reach the plan.
    """
    probe_says_selected("not gui", selected=True)
    included, skipped, isolated = runner._test_files("not gui")
    assert not skipped, (
        "'not gui' selects these files, so none may be skipped: %s" % skipped)
    assert isolated, "expected the dedicated directories to be isolated"

    isolated_files = [f for g in isolated for f in g]
    for path in runner._DEDICATED_JOB_DIRS:
        assert any(f.startswith(path) for f in isolated_files), (
            "%s selects tests under 'not gui' but reached neither the "
            "isolated set nor the plan" % path)

    plan = runner._plan(included, runner._durations(), 4,
                        runner.MAX_FILES_PER_CHUNK, isolated)
    planned = [f for chunk, _load in plan for f in chunk]
    assert sorted(planned) == sorted(list(included) + isolated_files)


def test_an_isolated_group_never_shares_a_chunk_with_anything_else(
        probe_says_selected):
    """Isolation is the reason they were excluded; it must survive inclusion.

    Measured: one render_smoke module in the same process as
    the findings-page suite fails 7 findings-page tests. Running these
    files instead of dropping them is only correct if they still get their own
    process.
    """
    probe_says_selected("not gui", selected=True)
    included, _skipped, isolated = runner._test_files("not gui")
    plan = runner._plan(included, runner._durations(), 4,
                        runner.MAX_FILES_PER_CHUNK, isolated)
    isolated_files = {f for g in isolated for f in g}
    for chunk, _load in plan:
        touching = [f for f in chunk if f in isolated_files]
        if touching:
            assert set(chunk) == set(touching), (
                "chunk mixes isolated files with ordinary ones: %s" % chunk)
            dirs = {d for f in chunk for d in runner._DEDICATED_JOB_DIRS
                    if f.startswith(d)}
            assert len(dirs) == 1, (
                "one chunk spans two dedicated directories: %s" % chunk)


def test_an_unanswerable_collection_probe_raises_instead_of_skipping():
    """"I could not tell" must never resolve to "safe to omit those tests".

    A malformed marker expression makes pytest exit 4 (usage error), not 5. If
    that were read as "nothing selected", a typo in -m would silently drop a
    whole dedicated directory -- the same failure this probe exists to prevent.
    """
    with pytest.raises(SystemExit) as exc:
        runner._selects_anything("tests/atlas_bake/", "not (gui")
    assert "Refusing to guess" in str(exc.value)


def test_the_default_markers_are_the_expression_ci_actually_uses():
    """The runner's whole claim is CI equivalence. Read ci.yml, don't trust it.
    CI's `tests` job runs this runner itself, one shard per job."""
    with open(CI_YML, encoding="utf-8") as fh:
        ci = fh.read()
    commands = re.findall(
        r"(?:pytest tests/|scripts/run_local_tests\.py) -m \"([^\"]+)\"", ci)
    assert commands, "could not find CI's test command and its -m expression"
    assert runner.DEFAULT_MARKERS in commands, (
        "DEFAULT_MARKERS=%r is not one of CI's marker expressions %r"
        % (runner.DEFAULT_MARKERS, commands))


def test_ci_runs_every_shard_of_the_split_it_names():
    """`--shard K/N` in ci.yml must come with a matrix listing 1..N, or a share
    of the suite silently never runs."""
    with open(CI_YML, encoding="utf-8") as fh:
        ci = fh.read()
    m = re.search(r"--shard \$\{\{ matrix\.shard \}\}/(\d+)", ci)
    assert m, "ci.yml's tests job no longer runs `--shard ${{ matrix.shard }}/N`"
    n = int(m.group(1))
    listed = re.search(r"shard: \[([0-9, ]+)\]", ci)
    assert listed, "ci.yml has no `shard: [...]` matrix"
    assert sorted(int(x) for x in listed.group(1).split(",")) == list(range(1, n + 1))


def test_the_combined_tests_check_skips_a_cancelled_run():
    """`tests-all` must not run when a newer push cancels the run (`always()` did,
    and reported a red "tests" on the superseded commit), and must need the shards."""
    with open(CI_YML, encoding="utf-8") as fh:
        ci = fh.read()
    job = ci[ci.index("\n  tests-all:\n"):]
    job = job[:job.index("\n\n")]
    # The public-repository condition may follow (ci.yml runs only lint-and-docs elsewhere).
    assert "if: ${{ !cancelled()" in job and "always()" not in job
    assert "needs: tests" in job


@pytest.mark.parametrize("n", [1, 2, 3, 4])
def test_the_shards_split_the_plan_exactly(n):
    """N jobs, one share each, run every chunk of the plan exactly once."""
    included, _skipped, isolated = runner._test_files(runner.DEFAULT_MARKERS)
    plan = runner._plan(included, runner._durations(), 2,
                        runner.MAX_FILES_PER_CHUNK, isolated)
    shares = [runner._shard(plan, k, n) for k in range(1, n + 1)]
    dealt = [tuple(chunk) for share in shares for chunk, _load in share]
    assert sorted(dealt) == sorted(tuple(chunk) for chunk, _load in plan)
    assert all(shares), "a shard got no chunks"


def test_the_shards_are_balanced_by_predicted_work():
    included, _skipped, isolated = runner._test_files(runner.DEFAULT_MARKERS)
    plan = runner._plan(included, runner._durations(), 2,
                        runner.MAX_FILES_PER_CHUNK, isolated)
    loads = [sum(load for _c, load in runner._shard(plan, k, 3)) for k in (1, 2, 3)]
    heaviest_chunk = max(load for _c, load in plan)
    assert max(loads) - min(loads) <= heaviest_chunk


@pytest.mark.parametrize("spec", ["0/3", "4/3", "3", "a/b", "", "1/0"])
def test_a_bad_shard_is_a_usage_error(spec):
    with pytest.raises(SystemExit):
        runner._parse_shard(spec)


def test_the_plan_covers_every_included_file_exactly_once():
    """Balance must never become coverage: no file dropped, none duplicated."""
    included, _skipped, _isolated = runner._test_files(runner.DEFAULT_MARKERS)
    plan = runner._plan(included, runner._durations(), 4,
                        runner.MAX_FILES_PER_CHUNK)
    planned = [f for chunk, _load in plan for f in chunk]
    assert sorted(planned) == sorted(included)
    assert len(planned) == len(set(planned))


def test_no_chunk_exceeds_the_file_cap_that_made_the_run_finish():
    """Bounded process lifetime is the property; the cap is how it is bounded."""
    included, _skipped, _isolated = runner._test_files(runner.DEFAULT_MARKERS)
    plan = runner._plan(included, runner._durations(), 4,
                        runner.MAX_FILES_PER_CHUNK)
    assert plan
    for chunk, _load in plan:
        assert 0 < len(chunk) <= runner.MAX_FILES_PER_CHUNK


def test_a_cp1252_console_shows_a_hebrew_failure_instead_of_crashing():
    """The Windows CI console is cp1252: printing a failing chunk's Hebrew output
    raised UnicodeEncodeError and hid the failing test's name (PR #428)."""
    import io
    raw = io.BytesIO()
    console = io.TextIOWrapper(raw, encoding="cp1252")
    runner._tolerant_console([console])
    print("FAILED tests/test_x.py::test_y - assert 'שלום' == 'עולם'", file=console)
    console.flush()
    assert b"FAILED tests/test_x.py::test_y" in raw.getvalue()


# --- the lanes' temp files (2026-10-22: a full run left ~18 GB under %TEMP%) ---

def _run_main(monkeypatch, tmp_path, rc, summary, files=("tests/test_a.py",),
              seen=None):
    """main() end to end, one file per chunk, each chunk's fake pytest writing a file
    named after its test file into its basetemp.

    *rc* and *summary* are the chunk's exit code and output, or functions of the
    chunk's first file returning them. *seen*, a list, gets one (test file, files
    under the lanes folder) pair per chunk, taken as the chunk starts. Returns main's
    exit code and the shared lanes folder under the fake temp dir."""
    parent = os.path.join(str(tmp_path), runner.LANES_DIR_NAME)
    monkeypatch.setattr(runner.tempfile, "gettempdir", lambda: str(tmp_path))
    monkeypatch.setattr(runner, "_test_files",
                        lambda markers: (list(files), [], []))
    monkeypatch.setattr(runner, "_durations", lambda: {})

    def fake_pytest(cmd, **kwargs):
        leaf = cmd[cmd.index("--basetemp") + 1]
        test_file = cmd[-1]
        if seen is not None:
            seen.append((test_file, [os.path.basename(f) for f in _files_under(parent)]))
        os.makedirs(leaf, exist_ok=True)
        name = "left_by_%s.txt" % os.path.basename(test_file)[:-3]
        with open(os.path.join(leaf, name), "w") as fh:
            fh.write("x")
        return subprocess.CompletedProcess(
            cmd, rc(test_file) if callable(rc) else rc,
            stdout=summary(test_file) if callable(summary) else summary, stderr="")

    monkeypatch.setattr(runner.subprocess, "run", fake_pytest)
    code = runner.main(["-j", "1", "--max-files", "1"])
    return code, parent


def _files_under(path):
    return sorted(os.path.relpath(os.path.join(d, f), path)
                  for d, _dirs, files in os.walk(path) for f in files)


def test_a_passing_run_removes_its_temp_files(monkeypatch, tmp_path):
    code, parent = _run_main(monkeypatch, tmp_path, 0, "1 passed in 0.10s")
    assert code == 0
    assert os.listdir(parent) == []


def test_a_failing_run_keeps_its_temp_files_for_inspection(monkeypatch, tmp_path):
    code, parent = _run_main(monkeypatch, tmp_path, 1, "1 failed in 0.10s")
    assert code == 1
    [run_dir] = os.listdir(parent)
    assert run_dir.startswith("run-")
    assert sorted(os.path.basename(f) for f in _files_under(parent)) == [
        runner.FINISHED_MARKER, "left_by_test_a.txt"]


# 2026-11-11: a failed run kept EVERY chunk's folder (~17.5 GB) for a day. With 11
# tests failing on master every run failed, and four runs in one morning filled
# drive C: -- the fifth broke with hundreds of tmp_path errors.

def test_a_failing_run_keeps_only_its_failing_chunks_files(monkeypatch, tmp_path):
    files = ("tests/test_a.py", "tests/test_b.py", "tests/test_c.py")
    code, parent = _run_main(
        monkeypatch, tmp_path,
        rc=lambda f: 1 if f.endswith("test_b.py") else 0,
        summary=lambda f: ("1 failed in 0.10s" if f.endswith("test_b.py")
                           else "1 passed in 0.10s"),
        files=files)
    assert code == 1
    assert sorted(os.path.basename(f) for f in _files_under(parent)) == [
        runner.FINISHED_MARKER, "left_by_test_b.txt"]


def test_a_passing_chunks_files_go_when_it_ends_not_when_the_run_ends(monkeypatch, tmp_path):
    """A run used to peak at ~17.5 GB before its end could remove anything."""
    seen = []
    files = ("tests/test_a.py", "tests/test_b.py", "tests/test_c.py")
    _run_main(monkeypatch, tmp_path,
              rc=lambda f: 1 if f.endswith("test_b.py") else 0,
              summary=lambda f: ("1 failed in 0.10s" if f.endswith("test_b.py")
                                 else "1 passed in 0.10s"),
              files=files, seen=seen)
    order = [os.path.basename(f)[:-3] for f, _present in seen]
    assert sorted(order) == ["test_a", "test_b", "test_c"]
    for i, (_f, present) in enumerate(seen):
        assert sorted(present) == sorted("left_by_%s.txt" % name for name in order[:i]
                                         if name == "test_b"), (order, seen)


def test_the_end_of_a_failed_run_removes_every_chunk_folder_it_does_not_keep(tmp_path):
    """Whatever a passing chunk left (a file still open when it ended), the run's end
    removes; a lane left with no kept chunk goes too."""
    root = tmp_path / "run-1000-1"
    for leaf in ("lane0/c1", "lane0/c2", "lane1/c3"):
        (root / leaf).mkdir(parents=True)
        (root / leaf / "f.txt").write_text("x")
    runner._clean_lanes(str(root), passed=False, keep=[str(root / "lane0" / "c2")])
    assert sorted(os.listdir(root)) == [runner.FINISHED_MARKER, "lane0"]
    assert os.listdir(root / "lane0") == ["c2"]


def test_a_run_folder_goes_whole_with_a_long_path_and_a_read_only_file(tmp_path):
    """Measured 2026-11-11: plain rmtree left a >260-character path (a long Hebrew path
    test) and read-only git objects, so a swept run folder never fully went."""
    root = tmp_path / "run-1000-1"
    leaf = root / "lane0" / "c1"
    leaf.mkdir(parents=True)
    read_only = leaf / "object"
    read_only.write_text("x")
    os.chmod(read_only, 0o444)
    deep = str(leaf)
    while len(deep) < 300:
        deep = os.path.join(deep, "longpath_שלום_" + "a" * 30)
    long_file = os.path.join(deep, "f.pdf")
    prefix = "\\\\?\\" if os.name == "nt" else ""
    os.makedirs(prefix + deep)
    with open(prefix + long_file, "w") as fh:
        fh.write("x")
    assert len(long_file) > 260

    runner._clean_lanes(str(root), passed=True)

    assert not os.path.exists(prefix + str(root))


def test_a_failed_run_removes_earlier_finished_runs_and_no_live_one(monkeypatch, tmp_path):
    """Only the latest failure is kept, and a run still going is never touched
    (2026-09-20: runs deleting each other's live files gave 133 errors)."""
    parent = os.path.join(str(tmp_path), runner.LANES_DIR_NAME)
    now = int(time.time())
    earlier_failed = os.path.join(parent, "run-%d-1111" % (now - 3600))
    live = os.path.join(parent, "run-%d-2222" % (now - 600))
    for run_dir in (earlier_failed, live):
        os.makedirs(os.path.join(run_dir, "lane0", "c1"))
        with open(os.path.join(run_dir, "lane0", "c1", "f.txt"), "w") as fh:
            fh.write("x")
    with open(os.path.join(earlier_failed, runner.FINISHED_MARKER), "w") as fh:
        fh.write("x")

    code, _parent = _run_main(monkeypatch, tmp_path, 1, "1 failed in 0.10s")

    assert code == 1
    names = sorted(os.listdir(parent))
    assert "run-%d-1111" % (now - 3600) not in names
    assert os.path.exists(os.path.join(live, "lane0", "c1", "f.txt"))
    [own] = [n for n in names if n != "run-%d-2222" % (now - 600)]
    assert os.path.isfile(os.path.join(parent, own, runner.FINISHED_MARKER))


def test_a_finished_run_spares_a_finished_run_that_started_after_it(tmp_path):
    """Two failed runs ending at once must not delete each other: a run removes
    only finished runs that STARTED before it."""
    parent = str(tmp_path)
    own = os.path.join(parent, "run-1000-1")
    later = os.path.join(parent, "run-2000-2")
    for run_dir in (own, later):
        os.makedirs(run_dir)
        with open(os.path.join(run_dir, runner.FINISHED_MARKER), "w") as fh:
            fh.write("x")
    runner._remove_earlier_finished_runs(own)
    assert sorted(os.listdir(parent)) == ["run-1000-1", "run-2000-2"]
    runner._remove_earlier_finished_runs(later)
    assert os.listdir(parent) == ["run-2000-2"]


@pytest.mark.parametrize("rc, summary, passed", [
    (0, "3 passed in 0.10s", True),
    (5, "43 deselected in 0.29s", True),
    (5, "no tests ran in 0.16s", False),     # collected nothing: kept, it is a failure
    (1, "1 failed, 2 passed in 0.10s", False),
    (0, "1 failed, 2 passed in 0.10s", False),  # failures behind exit 0: still kept
    (1, "3 passed, 2 errors in 0.10s", False),
    (-9, "", False),                          # killed: no summary
])
def test_which_chunks_count_as_passed(rc, summary, passed):
    assert runner._chunk_passed(_chunk(rc, summary)) is passed
    assert runner._chunk_passed(None) is False


def test_a_chunk_keeps_exactly_its_failing_and_erroring_tests_tmp_paths(tmp_path):
    """The chunk command itself, run by real pytest: a test that passed setup, call and
    teardown has no tmp_path left when the chunk ends, even one holding a path over 260
    characters and a read-only file; a test that failed, or whose fixture failed in
    setup or teardown, keeps its own. pytest's built-in
    `tmp_path_retention_policy=failed` lost the setup and teardown errors' folders
    (Codex on PR #7)."""
    test_file = tmp_path / "test_outcomes.py"
    test_file.write_text(
        "import pytest\n"
        "\n"
        "@pytest.fixture\n"
        "def breaks_in_setup(tmp_path):\n"
        "    (tmp_path / 'evidence.txt').write_text('x')\n"
        "    raise RuntimeError('setup')\n"
        "\n"
        "@pytest.fixture\n"
        "def breaks_in_teardown(tmp_path):\n"
        "    yield\n"
        "    raise RuntimeError('teardown')\n"
        "\n"
        "def test_passes(tmp_path):\n"
        "    (tmp_path / 'evidence.txt').write_bytes(b'x' * 1000)\n"
        "\n"
        "def test_skips(tmp_path):\n"
        "    (tmp_path / 'evidence.txt').write_text('x')\n"
        "    pytest.skip('not here')\n"
        "\n"
        "def test_fails(tmp_path):\n"
        "    (tmp_path / 'evidence.txt').write_text('x')\n"
        "    assert False\n"
        "\n"
        "def test_passes_with_hard_files(tmp_path):\n"
        "    import os\n"
        "    read_only = tmp_path / 'object'\n"
        "    read_only.write_text('x')\n"
        "    os.chmod(read_only, 0o444)\n"
        "    deep = str(tmp_path)\n"
        "    while len(deep) < 300:\n"
        "        deep = os.path.join(deep, 'd' * 40)\n"
        "    b = chr(92)\n"
        "    prefix = b + b + '?' + b if os.name == 'nt' else ''\n"
        "    os.makedirs(prefix + deep)\n"
        "    with open(prefix + os.path.join(deep, 'f.txt'), 'w') as fh:\n"
        "        fh.write('x')\n"
        "\n"
        "def test_passes_dynamically(request):\n"
        "    path = request.getfixturevalue('tmp_path')\n"
        "    (path / 'evidence.txt').write_text('x')\n"
        "\n"
        "def test_fails_dynamically(request):\n"
        "    path = request.getfixturevalue('tmp_path')\n"
        "    (path / 'evidence.txt').write_text('x')\n"
        "    assert False\n"
        "\n"
        "def test_setup_errs(breaks_in_setup):\n"
        "    pass\n"
        "\n"
        "def test_teardown_errs(breaks_in_teardown, tmp_path):\n"
        "    (tmp_path / 'evidence.txt').write_text('x')\n", encoding="utf-8")
    leaf = tmp_path / "lane0" / "c1"
    leaf.parent.mkdir()
    cmd = runner._chunk_cmd(str(leaf), runner.DEFAULT_MARKERS, [str(test_file)])
    pr = subprocess.run(cmd, cwd=str(tmp_path), capture_output=True, text=True,
                        encoding="utf-8", errors="replace", env=runner._chunk_env())
    counts = runner._parse_counts(pr.stdout)
    assert counts is not None and counts[1:4] == (2, 2, 1), pr.stdout[-3000:]
    # pytest also leaves a `<name>current` symlink beside each kept folder where the
    # OS lets it make one (Linux); it is bookkeeping, not a test's files.
    kept = sorted(n for n in os.listdir(leaf) if not os.path.islink(leaf / n))
    assert kept == ["test_fails0", "test_fails_dynamically0", "test_setup_errs0",
                    "test_teardown_errs0"]
    for name in kept:
        assert os.listdir(leaf / name) == ["evidence.txt"]


@pytest.mark.parametrize("path, extended", [
    ("C:\\Users\\x\\Temp\\run", "\\\\?\\C:\\Users\\x\\Temp\\run"),
    ("\\\\server\\share\\Temp\\run", "\\\\?\\UNC\\server\\share\\Temp\\run"),
    ("\\\\?\\C:\\already", "\\\\?\\C:\\already"),
    ("\\\\?\\UNC\\server\\share\\x", "\\\\?\\UNC\\server\\share\\x"),
    ("\\\\.\\device", "\\\\.\\device"),
])
def test_the_extended_length_form_of_a_windows_path(path, extended):
    """A UNC temp folder needs `\\\\?\\UNC\\`, not `\\\\?\\` before the path (Codex on PR #7)."""
    assert runner._extended_length(path) == extended


def test_a_run_leaves_a_concurrent_run_alone_and_sweeps_day_old_ones(monkeypatch, tmp_path):
    """Two runs at once used to delete each other's live tmp_path folders (133
    errors, 2026-09-20): every run began by wiping the one shared folder."""
    parent = os.path.join(str(tmp_path), runner.LANES_DIR_NAME)
    now = int(time.time())
    live = os.path.join(parent, "run-%d-4242" % now, "lane0", "c1")
    stale = os.path.join(parent, "run-%d-1111" % (now - 2 * 24 * 3600), "lane0", "c1")
    old_layout = os.path.join(parent, "lane0", "c1")
    for d in (live, stale, old_layout):
        os.makedirs(d)
        with open(os.path.join(d, "f.txt"), "w") as fh:
            fh.write("x")

    code, _parent = _run_main(monkeypatch, tmp_path, 0, "1 passed in 0.10s")

    assert code == 0
    assert os.listdir(parent) == ["run-%d-4242" % now]
    assert os.path.exists(os.path.join(live, "f.txt"))

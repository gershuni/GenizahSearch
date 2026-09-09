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
    tests/e2e/test_search_flow.py leaves tests/test_findings_page.py at 282
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
    tests/test_findings_page.py fails 7 findings-page tests. Running these
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
    """The runner's whole claim is CI equivalence. Read ci.yml, don't trust it."""
    with open(CI_YML, encoding="utf-8") as fh:
        ci = fh.read()
    commands = re.findall(r"pytest tests/ -m \"([^\"]+)\"", ci)
    assert commands, "could not find CI's `pytest tests/ -m \"...\"` command"
    assert runner.DEFAULT_MARKERS in commands, (
        "DEFAULT_MARKERS=%r is not one of CI's marker expressions %r"
        % (runner.DEFAULT_MARKERS, commands))


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

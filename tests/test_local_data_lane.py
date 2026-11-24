# -*- coding: utf-8 -*-
"""The release's local lane and the CI's timing record (health assessment 2026-11-23, 1.3 and 1.4).

Two things replaced a ~15,000-test local run at release time:

- `scripts/run_local_tests.py --local-data` runs only the `needs_local_data` tests:
  what the public CI skips for want of local data, or does not collect. It
  must FAIL when its own tests skip (in a worktree, say), or a release would be
  green over tests that ran nowhere.
- each chunk writes a JUnit report, the runner reads each file's own seconds from
  it, the CI uploads them (`--timings-out`), and `scripts/fold_lane_timings.py`
  folds them into tests/lane_durations.json.

These tests drive the real functions: the runner and the fold script by path, the
marker through a real pytest collection.
"""
from __future__ import annotations

import fnmatch
import importlib.util
import json
import os
import subprocess
import sys
import types

import pytest
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CI_YML = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(REPO_ROOT, rel))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


runner = _load("_genizah_run_local_tests_lane", "scripts/run_local_tests.py")
registry = _load("_genizah_needs_local_data", "tests/needs_local_data.py")
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))
fold = _load("_genizah_fold_lane_timings", "scripts/fold_lane_timings.py")


# --- the JUnit report a chunk writes -------------------------------------------

def test_a_chunk_report_gives_each_files_own_seconds_and_its_skips(tmp_path):
    """The chunk command itself, run by real pytest with a report: the file's time is
    there, and each skip with its reason, a class method under `Class::name` and a
    module skipped at collection as the whole file."""
    (tmp_path / "test_mixed.py").write_text(
        "import time, pytest\n"
        "def test_slow():\n"
        "    time.sleep(0.3)\n"
        "@pytest.mark.skipif(True, reason='the FJMS sidecar is absent')\n"
        "def test_needs_data():\n"
        "    pass\n"
        "class TestGroup:\n"
        "    @pytest.mark.parametrize('n', [1, 2])\n"
        "    def test_case(self, n):\n"
        "        pytest.skip('no index here')\n"
        "@pytest.mark.xfail(reason='known bug')\n"
        "def test_expected_failure():\n"
        "    assert False\n", encoding="utf-8")
    (tmp_path / "test_gone.py").write_text(
        "import pytest\n"
        "pytest.importorskip('a_module_that_is_not_installed_anywhere')\n"
        "def test_x():\n"
        "    pass\n", encoding="utf-8")
    leaf = tmp_path / "lane0" / "c1"
    leaf.parent.mkdir()
    junit = str(leaf) + ".junit.xml"
    cmd = runner._chunk_cmd(str(leaf), runner.DEFAULT_MARKERS,
                            ["test_mixed.py", "test_gone.py"], junit=junit)
    pr = subprocess.run(cmd, cwd=str(tmp_path), capture_output=True, text=True,
                        encoding="utf-8", errors="replace", env=runner._chunk_env())
    assert pr.returncode == 0, pr.stdout[-2000:]
    report = runner._read_junit(junit)
    assert report is not None
    assert report["seconds"]["test_mixed.py"] >= 0.3
    assert ["test_mixed.py", "test_slow"] in report["tests"]
    assert ["test_mixed.py", "TestGroup::test_case[1]"] in report["tests"]
    assert ["test_mixed.py", "test_expected_failure"] in report["tests"]
    skipped = {(f, t): reason for f, t, reason in report["skipped"]}
    assert skipped[("test_mixed.py", "test_needs_data")] == "the FJMS sidecar is absent"
    assert skipped[("test_mixed.py", "TestGroup::test_case[1]")] == "no index here"
    assert ("test_mixed.py", "TestGroup::test_case[2]") in skipped
    # an xfail is filed under <skipped> in JUnit, but it ran: not a skip
    assert ("test_mixed.py", "test_expected_failure") not in skipped
    whole = [(f, t) for (f, t) in skipped if f == "test_gone.py"]
    assert whole == [("test_gone.py", "")]
    assert "a_module_that_is_not_installed_anywhere" in skipped[("test_gone.py", "")]


def test_a_missing_or_broken_report_reads_as_none(tmp_path):
    assert runner._read_junit(str(tmp_path / "absent.xml")) is None
    broken = tmp_path / "broken.xml"
    broken.write_text("<testsuites><testcase", encoding="utf-8")
    assert runner._read_junit(str(broken)) is None


def _result(files, seconds, junit):
    return {"files": files, "seconds": seconds, "junit": junit}


def test_a_files_seconds_are_its_tests_plus_an_equal_share_of_the_chunk_start_up(monkeypatch):
    monkeypatch.setattr(runner, "_durations", lambda: {})
    results = {1: _result(["a.py", "b.py", "c.py"], 30.0,
                          {"seconds": {"a.py": 12.0, "b.py": 3.0}, "skipped": []})}
    # 30 s wall, 15 s inside tests: 5 s each on top of their own time.
    assert runner._module_seconds(results) == {"a.py": 17.0, "b.py": 8.0, "c.py": 5.0}


def test_a_chunk_without_a_report_is_spread_by_the_previous_weights(monkeypatch):
    monkeypatch.setattr(runner, "_durations", lambda: {"a.py": 3.0, "b.py": 1.0})
    results = {1: _result(["a.py", "b.py"], 20.0, None)}
    assert runner._module_seconds(results) == {"a.py": 15.0, "b.py": 5.0}


def test_timings_out_writes_what_the_fold_script_reads(monkeypatch, tmp_path):
    monkeypatch.setattr(runner, "_durations", lambda: {})
    results = {1: _result(["tests/test_a.py"], 4.0,
                          {"seconds": {"tests/test_a.py": 3.0},
                           "skipped": [["tests/test_a.py", "test_x", "sidecar absent"]]})}

    class Args:
        markers = runner.DEFAULT_MARKERS
        shard = "2/3"
        lanes = 2
    out = tmp_path / "sub" / "lane-timings.json"
    runner._write_timings(str(out), results, Args, 4.0)
    [data] = fold.load_timings([str(out)])
    assert data["modules"] == {"tests/test_a.py": 4.0}
    assert data["skipped"] == [["tests/test_a.py", "test_x", "sidecar absent"]]
    assert data["shard"] == "2/3" and data["platform"] == sys.platform


# --- the local-data lane --------------------------------------------------------

def _skips(*triples):
    return {1: _result(["f.py"], 1.0, {"seconds": {}, "skipped": [list(t) for t in triples]})}


def test_the_local_data_lane_fails_when_any_of_its_tests_skips():
    problems, skipped = runner._local_data_problems(
        _skips(("tests/test_a.py", "test_x", "the FJMS sidecar is absent"),
               ("tests/test_b.py", "", "no index")))
    assert len(skipped) == 2
    assert len(problems) == 1
    assert "ran nowhere" in problems[0] and "the FJMS sidecar is absent" in problems[0]
    assert "tests/test_a.py::test_x" in problems[0]


def test_the_local_data_lane_passes_when_nothing_skipped_or_skips_are_allowed():
    assert runner._local_data_problems(_skips()) == ([], [])
    problems, skipped = runner._local_data_problems(
        _skips(("tests/test_a.py", "test_x", "absent")), allow_skips=True)
    assert problems == [] and len(skipped) == 1


def test_the_local_data_lane_fails_when_a_chunk_has_no_report():
    """A chunk without its JUnit report hides its skips: the lane cannot pass."""
    results = {1: _result(["f.py"], 1.0, {"seconds": {}, "tests": [], "skipped": []}),
               2: _result(["g.py"], 1.0, None)}
    problems, _skipped = runner._local_data_problems(results)
    assert len(problems) == 1 and "chunks 2" in problems[0]
    # --allow-skips does not excuse it: it is not a skip, it is a blind spot
    problems, _skipped = runner._local_data_problems(results, allow_skips=True)
    assert len(problems) == 1


def _registry(tests=(), whole=()):
    return types.SimpleNamespace(TESTS=dict.fromkeys(tests, "why"),
                                 WHOLE_FILES=dict.fromkeys(whole, "why"),
                                 _listed=registry._listed)


def test_the_local_data_lane_fails_on_an_entry_that_matched_no_test():
    """A test renamed while its file stayed: its entry marks nothing (Codex, PR #123)."""
    ran = {1: _result(["tests/test_a.py"], 1.0, {
        "seconds": {}, "skipped": [],
        "tests": [["tests/test_a.py", "test_x[1]"], ["tests/test_a.py", "TestK::test_y"],
                  ["tests/test_w.py", "test_z"]]})}
    ok = _registry(tests=["tests/test_a.py::test_x", "tests/test_a.py::TestK"],
                   whole=["tests/test_w.py"])
    assert runner._local_data_problems(ran, registry=ok) == ([], [])
    stale = _registry(tests=["tests/test_a.py::test_x", "tests/test_a.py::test_renamed"],
                      whole=["tests/test_gone.py"])
    problems, _skipped = runner._local_data_problems(ran, registry=stale)
    assert len(problems) == 1
    assert "tests/test_a.py::test_renamed" in problems[0]
    assert "tests/test_gone.py" in problems[0] and "test_x" not in problems[0]


def test_every_listed_test_still_exists():
    """The registry by node, not by file: each `file::test` entry, marked or listed as
    running nowhere, must name a test pytest still collects. Collection only, so it
    runs on CI too, where the tests themselves skip."""
    entries = [e for e in list(registry.TESTS) + list(registry.RUNS_NOWHERE) if "::" in e]
    files = sorted({e.partition("::")[0] for e in entries})
    env = dict(os.environ, PYTHONUTF8="1")
    env.pop("GITHUB_ACTIONS", None)
    pr = subprocess.run(
        [sys.executable, "-m", "pytest", *files, "--collect-only", "-q",
         "-p", "no:cacheprovider"], cwd=REPO_ROOT, env=env,
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    assert pr.returncode == 0, pr.stdout[-3000:]
    nodes = [ln.strip().partition("::") for ln in pr.stdout.splitlines() if "::" in ln]
    for entry in entries:
        f, _, test = entry.partition("::")
        assert any(registry._listed([entry], nf, nt) for nf, _s, nt in nodes), (
            "%s names no collected test: renamed or removed?" % entry)
    assert any(f == "tests/test_discovery_suppression.py" for f, _s, _t in nodes)


@pytest.mark.parametrize("argv", [
    ["--allow-skips"],
    ["--local-data", "-m", "not gui"],
])
def test_the_lane_flags_refuse_what_would_change_their_meaning(argv):
    with pytest.raises(SystemExit) as exc:
        runner.main(argv + ["--dry-run"])
    assert exc.value.code == 2


def test_the_lane_plans_exactly_the_registry_files_and_they_exist():
    files = runner._local_data_files()
    assert files == registry.files()
    on_disk = set(runner._all_test_files())
    assert files and set(files) <= on_disk
    for entry in list(registry.WHOLE_FILES) + list(registry.TESTS):
        assert entry.partition("::")[0] in on_disk, entry
    # every CI-uncollected glob still names a file, or it is dead weight
    for glob in registry.CI_UNCOLLECTED:
        assert any(fnmatch.fnmatch(os.path.basename(f), glob) for f in on_disk), glob


def test_the_lane_never_plans_a_dedicated_job_directory():
    assert not [f for f in registry.files() if f.startswith(runner._DEDICATED_JOB_DIRS)]


def test_the_lane_selects_the_marker_within_the_ci_selection():
    assert runner.LOCAL_DATA_MARKERS == "needs_local_data and " + runner.DEFAULT_MARKERS


def test_a_worktree_is_recognised(monkeypatch):
    monkeypatch.setattr(runner, "REPO_ROOT", os.path.join("C:\\", "repo"))
    assert runner._in_worktree() is False
    monkeypatch.setattr(runner, "REPO_ROOT",
                        os.path.join("C:\\", "repo", ".claude", "worktrees", "x"))
    assert runner._in_worktree() is True


# --- the registry and the marker -----------------------------------------------

def test_an_entry_covers_its_parameters_and_methods_but_not_a_longer_name(monkeypatch):
    monkeypatch.setattr(registry, "TESTS", {"tests/test_a.py::test_x": "why",
                                            "tests/test_a.py::TestK": "why"})
    monkeypatch.setattr(registry, "WHOLE_FILES", {"tests/test_w.py": "why"})
    assert registry.is_marked("tests/test_a.py", "test_x")
    assert registry.is_marked("tests/test_a.py", "test_x[3-hebrew]")
    assert registry.is_marked("tests/test_a.py", "TestK::test_y")
    assert not registry.is_marked("tests/test_a.py", "test_x_other")
    assert not registry.is_marked("tests/test_a.py", "")
    assert not registry.is_marked("tests/test_b.py", "test_x")
    assert registry.is_marked("tests/test_w.py", "anything")
    assert registry.is_marked("tests\\test_w.py", "")


def test_a_test_that_runs_nowhere_is_listed_apart_and_never_marked():
    on_disk = set(runner._all_test_files())
    for entry in registry.RUNS_NOWHERE:
        f, _, test = entry.partition("::")
        assert f in on_disk, entry
        assert registry.runs_nowhere(f, test)
        assert not registry.is_marked(f, test), entry
    assert not set(registry.RUNS_NOWHERE) & (set(registry.TESTS) | set(registry.WHOLE_FILES))


def test_the_skips_report_leaves_out_marked_and_runs_nowhere_tests(monkeypatch):
    # RUNS_NOWHERE may be empty (every entry made to run): a stand-in entry, in the
    # registry the fold script itself loads.
    monkeypatch.setattr(registry, "RUNS_NOWHERE", {"tests/test_gone.py::test_nowhere": "why"})
    monkeypatch.setattr(fold.runner, "_local_data_registry", lambda: registry)
    marked = next(iter(registry.TESTS)).partition("::")
    nowhere = next(iter(registry.RUNS_NOWHERE)).partition("::")
    timing = _timing("1/1")
    timing["skipped"] = [[marked[0], marked[2], "sidecar absent"],
                         [nowhere[0], nowhere[2], "absent everywhere"],
                         ["tests/test_x.py", "test_linux", "Linux only"]]
    skips, n_nowhere = fold.unmarked_skips([timing])
    assert set(skips) == {("tests/test_x.py", "test_linux")} and n_nowhere == 1


def test_the_ci_uncollected_files_are_marked():
    assert registry.is_marked("tests/test_my_library_tab_optout_cascade.py", "test_x")
    assert registry.is_marked("tests/test_folder_walk_worker.py", "")
    assert not registry.is_marked("tests/test_local_data_lane.py", "test_x")


def _collected(marker, path):
    env = dict(os.environ, PYTHONUTF8="1")
    env.pop("GITHUB_ACTIONS", None)
    pr = subprocess.run(
        [sys.executable, "-m", "pytest", path, "-m", marker, "--collect-only", "-q",
         "-p", "no:cacheprovider"], cwd=REPO_ROOT, env=env,
        capture_output=True, text=True, encoding="utf-8", errors="replace")
    assert pr.returncode in (0, 5), pr.stdout[-2000:]
    return [ln for ln in pr.stdout.splitlines() if "::" in ln]


def test_conftest_marks_exactly_the_listed_tests_of_a_file():
    """The call site, not the helper: a real collection under -m needs_local_data
    picks the listed tests of a file and none of its others."""
    path = "tests/test_discovery_suppression.py"
    listed = {e for e in registry.TESTS if e.startswith(path + "::")}
    assert len(listed) >= 2
    assert set(_collected("needs_local_data", path)) == listed
    others = _collected("not needs_local_data", path)
    assert others and not set(others) & listed
    assert _collected("needs_local_data", "tests/test_local_data_lane.py") == []


# --- CI and the fold script ----------------------------------------------------

def _tests_job_steps():
    with open(CI_YML, encoding="utf-8") as fh:
        return yaml.safe_load(fh)["jobs"]["tests"]["steps"]


def test_ci_records_each_shards_timings_and_uploads_them_even_after_a_failure():
    steps = _tests_job_steps()
    runs = [s for s in steps if "scripts/run_local_tests.py" in str(s.get("run", ""))]
    assert len(runs) == 1 and "--timings-out lane-timings.json" in runs[0]["run"]
    uploads = [s for s in steps if str(s.get("uses", "")).startswith("actions/upload-artifact@")]
    assert len(uploads) == 1
    up = uploads[0]
    assert up["with"]["path"] == "lane-timings.json"
    assert up["with"]["name"].startswith(fold.ARTIFACT_PATTERN.rstrip("*"))
    assert "${{ matrix.shard }}" in up["with"]["name"] and "${{ runner.os }}" in up["with"]["name"]
    assert "!cancelled()" in up["if"]
    assert steps.index(up) > steps.index(runs[0])


def _timing(shard, markers=None, platform="win32", modules=None):
    return {"format": runner.TIMINGS_FORMAT, "markers": markers or runner.DEFAULT_MARKERS,
            "platform": platform, "shard": shard, "modules": modules or {},
            "skipped": [], "_path": "x/%s.json" % shard}


def test_the_fold_refuses_timings_of_another_selection():
    with pytest.raises(SystemExit, match="measured under"):
        fold.select([_timing("1/3", markers=runner.LOCAL_DATA_MARKERS)], "win32")


def test_the_fold_refuses_a_run_with_a_shard_missing_unless_told():
    partial = [_timing("1/3"), _timing("3/3")]
    with pytest.raises(SystemExit, match="shard\\(s\\) 2 of 3 missing"):
        fold.select(partial, "win32")
    chosen, notes = fold.select(partial, "win32", allow_partial=True)
    assert len(chosen) == 2 and any("2 of 3" in n for n in notes)


def test_the_fold_takes_only_the_platform_asked_for():
    timings = [_timing("%d/3" % k) for k in (1, 2, 3)] + [_timing("1/3", platform="linux")]
    chosen, _notes = fold.select(timings, "win32")
    assert [t["platform"] for t in chosen] == ["win32"] * 3


def test_the_fold_replaces_measured_files_drops_gone_ones_and_keeps_the_rest():
    old = {"a.py": 1.0, "b.py": 2.0, "gone.py": 9.0}
    measured = fold.measured_modules([_timing("1/2", modules={"a.py": 5.0, "new.py": 3.0}),
                                      _timing("2/2", modules={"a.py": 7.0, "x.py": 1.0})])
    assert measured == {"a.py": 6.0, "new.py": 3.0, "x.py": 1.0}
    new, rep = fold.fold(old, measured, existing={"a.py", "b.py", "new.py"})
    assert new == {"a.py": 6.0, "b.py": 2.0, "new.py": 3.0}
    assert rep["dropped"] == ["gone.py"] and rep["added"] == ["new.py"]
    assert rep["kept_unmeasured"] == ["b.py"] and rep["measured_not_in_tests"] == ["x.py"]


def test_the_fold_reads_only_its_own_format(tmp_path):
    bad = tmp_path / "t.json"
    bad.write_text(json.dumps({"format": 99}), encoding="utf-8")
    with pytest.raises(SystemExit, match="format"):
        fold.load_timings([str(bad)])

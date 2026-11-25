# -*- coding: utf-8 -*-
"""The development repository's labelled lanes stay the public CI's lanes.

`.github/workflows/dev-gui-tests.yml` and `dev-render-smoke.yml` copy the steps of
ci.yml's `gui-tests` and `render-smoke-tests`, so a PR labelled `gui-tests` or
`render-smoke` in the development repository runs what the public CI will run. A copy
drifts silently: these tests fail the moment the two differ, and pin the properties the
copies exist for (Linux only, never in the public repository, and ci.yml itself never
started by a label).
"""
from __future__ import annotations

import os

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOWS = os.path.join(REPO, ".github", "workflows")

# (workflow file, its job, ci.yml's job it copies, the label that starts it)
LANES = [
    ("dev-gui-tests.yml", "gui-tests-labelled", "gui-tests", "gui-tests"),
    ("dev-render-smoke.yml", "render-smoke-labelled", "render-smoke-tests", "render-smoke"),
]
every_lane = pytest.mark.parametrize("name, job, public_job, label", LANES, ids=[lane[3] for lane in LANES])


def _load(name):
    with open(os.path.join(WORKFLOWS, name), encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def _on(workflow):
    # PyYAML reads the bare key `on` as the boolean True.
    return workflow.get("on", workflow.get(True))


def _as_linux(job):
    """The public job's steps as its Linux matrix entry runs them: every
    `${{ matrix.<key> }}` replaced by that entry's value, everything else kept."""
    entry = next(e for e in job["strategy"]["matrix"]["include"] if e["os"] == "ubuntu-latest")

    def resolve(value):
        if isinstance(value, dict):
            return {k: resolve(v) for k, v in value.items()}
        if isinstance(value, str):
            for key, val in entry.items():
                value = value.replace("${{ matrix.%s }}" % key, str(val))
        return value

    return [resolve(step) for step in job["steps"]]


@every_lane
def test_the_labelled_lane_runs_the_public_steps(name, job, public_job, label):
    """Whole steps, inputs (`with:`) included: a checkout depth or a setup-python cache
    added to one job and not the other fails here."""
    public = _load("ci.yml")["jobs"][public_job]
    dev = _load(name)["jobs"][job]
    assert dev["steps"] == _as_linux(public)
    assert dev.get("env") == public.get("env")
    assert dev["timeout-minutes"] == public["timeout-minutes"]


@every_lane
def test_a_labelled_pr_never_gets_a_skipped_lane(name, job, public_job, label):
    """A skipped job still leaves a check under its name, and a skipped check counts as
    passing: on a labelled PR it could stand over a failed run on the same commit. So the
    job may skip only for the repository or a missing label, never for the kind of event."""
    cond = " ".join(_load(name)["jobs"][job]["if"].split())
    assert "github.event" not in cond.replace("github.event.pull_request.labels", "")


@every_lane
def test_the_labelled_lane_is_linux_only_and_never_public(name, job, public_job, label):
    dev = _load(name)["jobs"][job]
    assert dev["runs-on"] == "ubuntu-latest"
    cond = " ".join(dev["if"].split())
    assert "github.repository != 'gershuni/GenizahSearch'" in cond
    assert f"contains(github.event.pull_request.labels.*.name, '{label}')" in cond


@every_lane
def test_the_lane_starts_for_a_push_and_for_a_label(name, job, public_job, label):
    types = _on(_load(name))["pull_request"]["types"]
    assert set(types) == {"opened", "synchronize", "reopened", "labeled"}


def test_the_lanes_have_names_of_their_own():
    """One name per check, and one concurrency group per workflow (its name is the group's):
    two lanes sharing either would hide or cancel each other."""
    loaded = [_load(name) for name, *_ in LANES]
    names = [w["jobs"][lane[1]]["name"] for w, lane in zip(loaded, LANES)]
    assert names == [f"{lane[3]} (labelled, ubuntu-latest)" for lane in LANES]
    assert len({w["name"] for w in loaded}) == len(LANES)
    assert all("github.workflow" in w["concurrency"]["group"] for w in loaded)


def test_the_render_smoke_lane_runs_its_folder():
    """The lane collects `tests/render_smoke` alone, and tests/conftest.py marks by the folder's
    name: a second folder of that name would be marked and run nowhere."""
    runs = [s.get("run") for s in _load("ci.yml")["jobs"]["render-smoke-tests"]["steps"]]
    assert runs[-1] == "pytest tests/render_smoke -m render_smoke"
    folders = [os.path.relpath(os.path.join(folder, d), REPO).replace(os.sep, "/")
               for folder, dirs, _files in os.walk(os.path.join(REPO, "tests"))
               for d in dirs if d == "render_smoke"]
    assert folders == ["tests/render_smoke"]


class _Item:
    """What tests/conftest.py's collection hook reads of a collected test."""

    def __init__(self, path, *markers):
        self.fspath = os.path.join(REPO, *path.split("/"))
        self.nodeid = path + "::test_x"
        self.keywords = {}
        self.markers = list(markers)

    def get_closest_marker(self, name):
        return next((m for m in self.markers if m == name), None)

    def add_marker(self, marker):
        self.markers.append(marker if isinstance(marker, str) else marker.name)


def test_a_render_smoke_test_outside_its_folder_stops_the_run(request):
    """A test marked render_smoke anywhere else would run nowhere (the other lanes deselect the
    marker), so collecting it is an error. The marker itself is read, however it was put there."""
    conftest = os.path.join(REPO, "tests", "conftest.py")
    [hook] = [p.pytest_collection_modifyitems for p in request.config.pluginmanager.get_plugins()
              if os.path.normcase(getattr(p, "__file__", None) or "") == os.path.normcase(conftest)]
    inside = _Item("tests/render_smoke/test_a.py")
    plain = _Item("tests/test_b.py")
    hook(request.config, [inside, plain])
    assert "render_smoke" in inside.markers and "render_smoke" not in plain.markers

    with pytest.raises(pytest.UsageError, match="tests/test_c.py::test_x"):
        hook(request.config, [inside, plain, _Item("tests/test_c.py", "render_smoke")])


def test_ci_yml_is_never_started_by_a_label():
    """A ci.yml run started by a label would leave its skipped jobs behind under real
    check names, and a skipped `tests` counts as passing."""
    types = (_on(_load("ci.yml")).get("pull_request") or {}).get("types")
    assert types is None or "labeled" not in types

# -*- coding: utf-8 -*-
"""The development repository's labelled GUI lane stays the public CI's GUI lane.

`.github/workflows/dev-gui-tests.yml` copies ci.yml's `gui-tests` steps, so a PR
labelled `gui-tests` in the development repository runs what the public CI will
run. A copy drifts silently: these tests fail the moment the two differ, and pin
the properties the copy exists for (Linux only, never in the public repository,
and ci.yml itself never started by a label).
"""
from __future__ import annotations

import os

import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOWS = os.path.join(REPO, ".github", "workflows")


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


def test_the_labelled_lane_runs_the_public_gui_steps():
    """Whole steps, inputs (`with:`) included: a checkout depth or a setup-python cache
    added to one job and not the other fails here."""
    public = _load("ci.yml")["jobs"]["gui-tests"]
    dev = _load("dev-gui-tests.yml")["jobs"]["gui-tests-labelled"]
    assert dev["steps"] == _as_linux(public)
    assert dev["env"] == public["env"]
    assert dev["timeout-minutes"] == public["timeout-minutes"]


def test_a_labelled_pr_never_gets_a_skipped_lane():
    """A skipped job still leaves a check under its name, and a skipped check counts as
    passing: on a labelled PR it could stand over a failed run on the same commit. So the
    job may skip only for the repository or a missing label, never for the kind of event."""
    cond = " ".join(_load("dev-gui-tests.yml")["jobs"]["gui-tests-labelled"]["if"].split())
    assert "github.event" not in cond.replace("github.event.pull_request.labels", "")


def test_the_labelled_lane_is_linux_only_and_never_public():
    dev = _load("dev-gui-tests.yml")["jobs"]["gui-tests-labelled"]
    assert dev["runs-on"] == "ubuntu-latest"
    cond = " ".join(dev["if"].split())
    assert "github.repository != 'gershuni/GenizahSearch'" in cond
    assert "contains(github.event.pull_request.labels.*.name, 'gui-tests')" in cond


def test_ci_yml_is_never_started_by_a_label():
    """A ci.yml run started by a label would leave its skipped jobs behind under real
    check names, and a skipped `tests` counts as passing."""
    types = (_on(_load("ci.yml")).get("pull_request") or {}).get("types")
    assert types is None or "labeled" not in types

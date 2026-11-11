# -*- coding: utf-8 -*-
"""pytest plugin for scripts/run_local_tests.py: remove a test's tmp_path once it passed.

"Passed" means setup, call AND teardown: a test whose fixture failed in setup or in
teardown keeps its tmp_path, because that is the evidence one opens next. pytest's own
`tmp_path_retention_policy=failed` decides on the call alone, so it removed the folder of
a test that errored in setup or teardown (Codex on PR #7, 2026-11-11).

Without any removal a full run's chunks kept ~17.5 GB of passing tests' folders
(2026-11-11). The runner loads this with `-p genizah_lane_tmp_retention`, from a folder
holding only this module and genizah_lane_rmtree (on PYTHONPATH), so nothing else becomes
importable."""
from __future__ import annotations

import pytest
from genizah_lane_rmtree import rmtree

_PASSED_PHASES = pytest.StashKey[dict]()
_TMP_PATH = pytest.StashKey[object]()


@pytest.hookimpl(wrapper=True)
def pytest_fixture_setup(fixturedef, request):
    # The value tmp_path gave this test, however it was asked for: `item.funcargs` misses
    # one obtained with `request.getfixturevalue("tmp_path")` (Codex on PR #7).
    value = yield
    if fixturedef.argname == "tmp_path":
        request.node.stash[_TMP_PATH] = value
    return value


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(item, call):
    rep = yield
    phases = item.stash.setdefault(_PASSED_PHASES, {})
    phases[rep.when] = not rep.failed
    if rep.when == "teardown":
        # Every phase that ran: a skip in setup has no call, and is no failure.
        path = item.stash.get(_TMP_PATH, None)
        if path is not None and all(phases.values()):
            rmtree(path)        # long paths and read-only files too (Codex on PR #7)
    return rep

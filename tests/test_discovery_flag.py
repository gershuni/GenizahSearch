# -*- coding: utf-8 -*-
"""Flag-off clean-hide proof for the Discovery Data Spine (Phase 134,
plan 134-05, Task 3, DATA-07).

Mirrors ``tests/test_atlas_flag_gating.py``'s flag-off / asset-not-ready
clean-hide tests: even with a fully READY sidecar loaded, ``DISCOVERY_ENABLED``
OFF must make ``discovery_available()`` False so every future discovery read
no-ops. A companion test proves the module-level startup wiring in
``web/main.py`` never raises when ``discovery_data/`` is absent (fail-open
startup, mirroring the atlas loader's own startup posture).
"""
from __future__ import annotations

import ast
import pathlib

import pytest

import web.discovery_assets as da
import web.main as wm
from tests.fixtures.discovery_v2_fixture import materialize_sidecar

MAIN_PY = pathlib.Path(wm.__file__)
MAIN_SRC = MAIN_PY.read_text(encoding="utf-8")

FIXTURE_DIR = pathlib.Path(__file__).resolve().parent / "fixtures" / "discovery"
FIXTURE_DB = FIXTURE_DIR / "discovery-v1-fixture.db"
FIXTURE_MANIFEST = FIXTURE_DIR / "manifest.json"


@pytest.fixture(autouse=True)
def _restore_state():
    yield
    da.load_discovery_state()


def test_flag_off_hides(tmp_path, monkeypatch):
    """DATA-07: with the flag OFF, discovery_available() is False EVEN WHEN
    the sidecar loaded successfully -- flag AND readiness, never flag alone."""
    # Post-136-20 the readiness contract requires the Amendment 2026-08-02
    # shape, so the ready state is built from an UPGRADED copy of the golden
    # (pre-rebuild) fixture.
    materialize_sidecar(tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))

    assert da.load_discovery_state() is True  # sidecar itself IS ready

    monkeypatch.setattr(da, "DISCOVERY_ENABLED", False)
    assert da.discovery_available() is False

    # Flip the flag back ON with the SAME loaded state -- proves the gate is
    # a live AND of (flag, readiness), not a one-shot snapshot at load time.
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.discovery_available() is True


def test_flag_off_and_sidecar_absent_hides_cleanly(tmp_path, monkeypatch):
    """Flag OFF + no sidecar at all is the default-deploy state this phase
    ships in -- still a clean False, no exception."""
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))  # empty dir, no manifest.json
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", False)

    assert da.load_discovery_state() is False
    assert da.discovery_available() is False


def test_startup_does_not_raise_when_discovery_data_absent(tmp_path, monkeypatch):
    """Fail-open startup: calling load_discovery_state() (as web/main.py does
    at module level) must never raise when discovery_data/ doesn't exist."""
    absent_dir = tmp_path / "discovery_data_does_not_exist"
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(absent_dir))

    result = da.load_discovery_state()  # must not raise

    assert result is False
    assert da.discovery_available() is False


def test_main_calls_load_discovery_state_at_module_level():
    """web/main.py wires load_discovery_state() at module level, mirroring
    the load_atlas_state() wiring point (AST-level: a bare call statement at
    module scope, not nested inside a function/route body)."""
    tree = ast.parse(MAIN_SRC, filename=str(MAIN_PY))
    module_level_calls = set()
    for node in tree.body:  # module top-level statements ONLY
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            func = node.value.func
            if isinstance(func, ast.Name):
                module_level_calls.add(func.id)
    assert "load_discovery_state" in module_level_calls
    assert "load_atlas_state" in module_level_calls  # sanity: same wiring pattern


def test_no_discovery_route_or_nav_added():
    """NO discovery UI/route/nav ships in Phase 134 (134-CONTEXT.md scope)."""
    assert "/discovery" not in MAIN_SRC
    assert "discovery-data" not in MAIN_SRC  # no /discovery-data/* route prefix (mirrors /atlas-data/*)

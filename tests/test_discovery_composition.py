# -*- coding: utf-8 -*-
"""Composition tests for web/discovery.py (Phase 134, plan 134-06, Task 3).

Proves: (1) import-before-load safety (DC12/F1) -- importing web/discovery.py
while the sidecar is NOT yet loaded (or the flag is off) still resolves
correctly once web.discovery_assets.load_discovery_state() runs later, with
NO import-time capture of the path/version; (2) discovery_available() ==
False makes every pass-through a clean no-op ([] / None, never a raised
DiscoveryUnavailable escaping to the caller); (3) no discovery route/page/nav
is added this phase.

Mirrors tests/test_discovery_loader.py / tests/test_discovery_flag.py's own
fixture-building + autouse-restore idiom so module state never leaks across
tests in this file or others.
"""
from __future__ import annotations

import asyncio
import importlib
import inspect

import pytest

import web.discovery_assets as da
from tests.fixtures.discovery_v2_fixture import materialize_sidecar

FIXTURE_VERSION = "discovery-v1-synthetic-fixture"


@pytest.fixture(autouse=True)
def _restore_state():
    """Every test in this module ends with a bare load_discovery_state()
    restore call so module state never leaks across tests (atlas/discovery
    test idiom).

    Module identity needs no restore: `_reimport_web_discovery` reloads in
    place rather than popping `sys.modules`, so only one object ever exists.
    """
    yield
    da.load_discovery_state()


def _reimport_web_discovery():
    """Force a FRESH import of web.discovery -- needed so the
    import-before-load test can control precisely WHEN the module (and its
    module-level DiscoveryService construction) first runs, relative to
    load_discovery_state().

    RELOAD, never `sys.modules.pop()` + re-import: popping builds a SECOND
    module object, and restoring `sys.modules` does not undo it because the
    import machinery also rebinds the submodule as an attribute of the parent
    package, which is what `import web.discovery as x` actually reads. Two live
    objects split from-imported references (notably `web/pages/findings.py`)
    from the object a test patches. See the same helper in
    tests/test_discovery_assets_audience.py for the concrete failure.
    """
    import web.discovery as disc  # noqa: PLC0415 -- reloaded deliberately below

    return importlib.reload(disc)


def test_import_before_load_then_load_then_query_resolves_and_version_correct(monkeypatch, tmp_path):
    """DC12/F1: import web/discovery.py while discovery is NOT ready, THEN
    load the real fixture, THEN query -- must return fixture rows and the
    correct sidecar version. This proves the module-level DiscoveryService
    never captured a stale/empty path at import time."""
    # 1. Start from a genuinely NOT-ready state (empty dir, flag on).
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path / "empty"))
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is False
    assert da.discovery_available() is False

    # 2. Import (or re-import) web.discovery NOW, while still not ready.
    disc = _reimport_web_discovery()

    async def _query_before_load():
        assert await disc.get_version() is None
        assert await disc.get_claims_for_page("p001") == []

    asyncio.run(_query_before_load())

    # 3. NOW load the real committed fixture (simulating a later
    # rebuild+restart) -- the ALREADY-IMPORTED module must pick this up
    # purely because its providers are lazy/call-time, not import-time.
    # Post-136-20 the readiness contract requires the Amendment 2026-08-02
    # shape, so the fixture is materialized as an UPGRADED copy rather than
    # read in place from the committed (pre-rebuild) directory.
    ready_dir = tmp_path / "ready"
    materialize_sidecar(ready_dir)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(ready_dir))
    assert da.load_discovery_state() is True
    assert da.discovery_available() is True

    async def _query_after_load():
        version = await disc.get_version()
        assert version == FIXTURE_VERSION
        claims = await disc.get_claims_for_page("p001")
        assert len(claims) == 1
        assert claims[0]["confidence_band"] == "expert_verified"

    asyncio.run(_query_after_load())


def test_query_returns_fixture_rows_and_correct_version(monkeypatch, tmp_path):
    materialize_sidecar(tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is True

    disc = _reimport_web_discovery()

    async def _run():
        assert await disc.get_version() == FIXTURE_VERSION
        claims = await disc.get_claims_for_page("p012")
        assert len(claims) == 2
        related = await disc.get_pages_related_to_page("p004")
        assert len(related) == 1
        witnesses = await disc.get_work_witnesses("w000005")
        assert len(witnesses) == 2
        evidence = await disc.get_evidence(claims[0]["claim_id"])
        assert len(evidence) >= 1

    asyncio.run(_run())


def test_discovery_available_false_makes_every_passthrough_a_noop(monkeypatch, tmp_path):
    """T-134-failopen: with discovery_available() False (flag off, even
    though the sidecar itself is fully loaded/ready), every pass-through
    must no-op to an empty/None result -- never raise, never touch the DB."""
    materialize_sidecar(tmp_path)
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is True  # sidecar itself IS ready

    monkeypatch.setattr(da, "DISCOVERY_ENABLED", False)  # but the flag is OFF
    assert da.discovery_available() is False

    disc = _reimport_web_discovery()

    async def _run():
        assert await disc.get_version() is None
        assert await disc.get_claims_for_page("p001") == []
        assert await disc.get_pages_related_to_page("p004") == []
        assert await disc.get_evidence("whatever") == []
        assert await disc.get_work_witnesses("w000001") == []

    asyncio.run(_run())


def test_discovery_available_false_when_sidecar_absent(monkeypatch, tmp_path):
    monkeypatch.setattr(da, "DISCOVERY_DATA_DIR", str(tmp_path / "absent"))
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", True)
    assert da.load_discovery_state() is False
    assert da.discovery_available() is False

    disc = _reimport_web_discovery()

    async def _run():
        assert await disc.get_work_witnesses("w000001") == []

    asyncio.run(_run())


def test_no_ui_or_route_added_in_web_discovery_module():
    """No discovery route/page/nav ships in Phase 134 (134-CONTEXT.md scope)."""
    disc = _reimport_web_discovery()
    src = inspect.getsource(disc)
    forbidden_markers = ("ui.page(", "@app.get(", "@app.post(", "app.add_route", "@ui.page")
    for marker in forbidden_markers:
        assert marker not in src, f"web/discovery.py must not add UI/routes this phase (found {marker!r})"
    assert "/discovery" not in src

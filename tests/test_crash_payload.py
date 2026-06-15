# -*- coding: utf-8 -*-
"""Phase 113 Plan 01 — Wave 0 scaffold for CRASH-04 payload tests.

Tests in this file are STUBS that will be filled by Plan 02.
Collection succeeds and all stubs skip, giving Plan 02 clear verify targets.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import pytest


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# CRASH-04 — payload key allowlist enforcement
# ---------------------------------------------------------------------------
def test_payload_keys_allowlisted():
    """CRASH-04: all _make_crash_props keys are in _ALLOWED_PROPS."""
    pytest.skip("filled by plan 02")


def test_no_forbidden_keys_in_payload():
    """CRASH-04: 'traceback_scrubbed', 'thread_name', 'message' never appear."""
    pytest.skip("filled by plan 02")


def test_no_path_in_crash_props():
    """CRASH-04: no file path leaks into crash props (D-07 frame-walk only)."""
    pytest.skip("filled by plan 02")


def test_external_module_fallback():
    """CRASH-04 D-07: stdlib frame -> error_module='external'."""
    pytest.skip("filled by plan 02")


def test_no_str_exc_in_emit_crash():
    """CRASH-04: str(exc_value) is never read (static/AST check on _emit_crash_direct)."""
    pytest.skip("filled by plan 02")


def test_generic_basename_not_in_app():
    """CRASH-04 D-07: arbitrary user/plugin basename is not treated as in-app."""
    pytest.skip("filled by plan 02")

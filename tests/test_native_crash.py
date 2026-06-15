# -*- coding: utf-8 -*-
"""Phase 113 Plan 01 — Wave 0 scaffold for CRASH-03/07 native crash tests.

Tests in this file are STUBS that will be filled by Plan 03.
Collection succeeds and all stubs skip, giving Plan 03 clear verify targets.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import pytest

import desktop.telemetry as tel
import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# CRASH-07 — prior native crash emit (consent True path)
# ---------------------------------------------------------------------------
def test_prior_crash_emitted_on_consent():
    """CRASH-07: prior native crash + consent True → desktop_prior_crash emitted once."""
    pytest.skip("filled by plan 03")


def test_pending_emit_after_consent():
    """CRASH-07 D-03: prior crash held pending, emitted exactly once when consent becomes True."""
    pytest.skip("filled by plan 03")


def test_no_emit_without_consent():
    """CRASH-07: user never consents → prior native crash never emitted."""
    pytest.skip("filled by plan 03")


# ---------------------------------------------------------------------------
# CRASH-07 D-02 — native crash classification (fixed enum, never raw text)
# ---------------------------------------------------------------------------
def test_classify_all_prefixes():
    """CRASH-07 D-02: all _NATIVE_CRASH_LABELS prefixes map to known enum labels."""
    pytest.skip("filled by plan 03")


def test_classify_unknown_maps_to_unknown_native():
    """CRASH-07 D-02: unrecognized prefix → 'unknown_native'."""
    pytest.skip("filled by plan 03")


# ---------------------------------------------------------------------------
# CRASH-03 — faulthandler read-before-enable ordering (D-03)
# ---------------------------------------------------------------------------
def test_read_before_enable_ordering():
    """CRASH-03 D-03: previous dump is READ before faulthandler.enable() opens the file."""
    pytest.skip("filled by plan 03")


def test_persisted_consent_populates_crash_distinct_id():
    """CRASH-07 D-03: after set_consent(True), _crash_distinct_id is populated for pending emit."""
    pytest.skip("filled by plan 03")

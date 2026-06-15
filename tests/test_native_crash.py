# -*- coding: utf-8 -*-
"""Phase 113 Plan 01/02 — Native crash detection + persisted consent tests.

Plan 01 stubs (filled by Plan 03): test_prior_crash_emitted_on_consent,
test_pending_emit_after_consent, test_no_emit_without_consent,
test_classify_all_prefixes, test_classify_unknown_maps_to_unknown_native,
test_read_before_enable_ordering.

Plan 02 (filled here): test_persisted_consent_populates_crash_distinct_id.

No `qtbot` parameter is used anywhere in this file (repo is pytest-qt-FREE;
REVIEWS MEDIUM-6).
"""

import pytest

import desktop.telemetry as tel


# ---------------------------------------------------------------------------
# Module-level autouse wrapper — opt-in to crash_telemetry_state fixture.
# Scoped to this file only (never project-wide).
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _use(crash_telemetry_state):
    yield


# ---------------------------------------------------------------------------
# CRASH-07 — prior native crash emit (consent True path)
# (These stubs will be filled by Plan 03)
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
# (These stubs will be filled by Plan 03)
# ---------------------------------------------------------------------------
def test_classify_all_prefixes():
    """CRASH-07 D-02: all _NATIVE_CRASH_LABELS prefixes map to known enum labels."""
    pytest.skip("filled by plan 03")


def test_classify_unknown_maps_to_unknown_native():
    """CRASH-07 D-02: unrecognized prefix → 'unknown_native'."""
    pytest.skip("filled by plan 03")


# ---------------------------------------------------------------------------
# CRASH-03 — faulthandler read-before-enable ordering (D-03)
# (These stubs will be filled by Plan 03)
# ---------------------------------------------------------------------------
def test_read_before_enable_ordering():
    """CRASH-03 D-03: previous dump is READ before faulthandler.enable() opens the file."""
    pytest.skip("filled by plan 03")


# ---------------------------------------------------------------------------
# REVIEWS HIGH-3 — persisted-consent startup populates _crash_distinct_id
# ---------------------------------------------------------------------------
def test_persisted_consent_populates_crash_distinct_id(crash_telemetry_state):
    """REVIEWS HIGH-3: _load_consent_state() sets _crash_distinct_id when enabled=True.

    Simulates a persisted-consent launch by populating the fake config with
    enabled=True + an install_id, then calling _load_consent_state().
    Asserts that _crash_distinct_id is set to that id (NOT None), so a crash
    before any set_consent() call emits with the correct identity.
    """
    fake_config = crash_telemetry_state
    fake_install_id = 'aaaa1111bbbb2222cccc3333dddd4444'

    # Populate fake persisted config (simulating what set_consent(True) writes on a prior launch)
    fake_config[tel.TELEMETRY_ENABLED_KEY] = True
    fake_config[tel.TELEMETRY_INSTALL_ID_KEY] = fake_install_id

    # Reset and reload from fake config (simulates startup)
    tel._reset_for_tests()
    tel._load_consent_state()

    # _crash_distinct_id must now be set to the persisted install_id
    # (NOT None — a crash before any set_consent() call must not emit as 'system')
    assert tel._crash_distinct_id is not None, (
        "_crash_distinct_id is None after _load_consent_state() with enabled=True — "
        "persisted-consent users would emit crashes as 'system' (REVIEWS HIGH-3)"
    )
    assert tel._crash_distinct_id == fake_install_id, (
        f"_crash_distinct_id ({tel._crash_distinct_id!r}) != install_id ({fake_install_id!r}) "
        "after persisted-consent startup"
    )

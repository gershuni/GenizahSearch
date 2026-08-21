# -*- coding: utf-8 -*-
"""Quick task 260714-k56 (SEED-031) — telemetry re-ask consent tests.

Two sections:
  - HEADLESS: pure should_reask_consent()/record_consent_ask()/set_never_ask()
    gate-logic coverage (Tests A-H) + the done()-finalizer decline path. The
    decline-path test constructs a real ConsentDialog, so it needs the
    Qt-offscreen bootstrap below (same pattern as the other sections).
  - Qt-OFFSCREEN (Task 2): TelemetryConsentBar construct/signal smoke test.

Run with:
    GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest \
        tests/test_telemetry_reask_consent.py -q

Autouse fixture pattern copied from tests/test_telemetry_consent_gate.py /
tests/test_telemetry_consent_ux.py.
"""

from __future__ import annotations

import queue
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only -- Qt in the mixed non-GUI run
# segfaults after thousands of NiceGUI/asyncio tests share the process (2026-08-21).

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — resets module-level state before/after each test.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    """Reset desktop.telemetry + posthog_server state before/after each test."""
    fake_config: dict = {}

    def fake_load_app_config():
        return dict(fake_config)

    def fake_save_app_config(new_data: dict):
        fake_config.update(new_data)

    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    tel._reset_for_tests()
    ph._reset_for_tests()


# ---------------------------------------------------------------------------
# Qt bootstrap (shared by the decline-path test below and the Task 2
# TelemetryConsentBar section appended at the bottom of this file).
# ---------------------------------------------------------------------------
_qt_app = None
_qt_skip_reason: str | None = None

try:
    from PyQt6.QtWidgets import QApplication
    _qt_app = QApplication.instance() or QApplication([])
except Exception as _qt_exc:  # noqa: BLE001
    _qt_skip_reason = f"QApplication unavailable: {_qt_exc}"


def _skip_if_no_qt():
    if _qt_skip_reason is not None:
        pytest.skip(_qt_skip_reason, allow_module_level=False)


_FIXED_NOW = datetime(2026, 7, 14, 12, 0, 0, tzinfo=timezone.utc)
_CURRENT_VERSION = "9.9.9"
_OTHER_VERSION = "8.0.0"


# ===========================================================================
# should_reask_consent gate tests (Tests A-H)
# ===========================================================================

def test_reask_gate_a_opted_in_short_circuits_false(_reset_telemetry_state):
    """Test A: an already-opted-in user is never re-asked."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    assert tel.should_reask_consent(_CURRENT_VERSION) is False


def test_reask_gate_b_first_run_not_shown_false(_reset_telemetry_state):
    """Test B: first-run owns the initial ask — FIRST_RUN_SHOWN_KEY absent -> False."""
    import desktop.telemetry as tel
    # fake_config starts empty -> FIRST_RUN_SHOWN_KEY absent
    assert tel.should_reask_consent(_CURRENT_VERSION) is False


def test_reask_gate_c_never_ask_false(_reset_telemetry_state):
    """Test C: hard opt-out (Don't ask again) is never overridden."""
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    fake_config[tel.TELEMETRY_NEVER_ASK_KEY] = True
    assert tel.should_reask_consent(_CURRENT_VERSION) is False


def test_reask_gate_d_lifetime_cap_false(_reset_telemetry_state):
    """Test D: the ~3 lifetime cap blocks further re-asks regardless of version/cooldown."""
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    fake_config[tel.TELEMETRY_ASK_COUNT_KEY] = tel.REASK_MAX_ASKS
    # No last_version/last_ts set -> would otherwise pass version+cooldown checks
    assert tel.should_reask_consent(_CURRENT_VERSION) is False


def test_reask_gate_e_same_version_false(_reset_telemetry_state):
    """Test E: never re-ask twice within the same version."""
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    fake_config[tel.TELEMETRY_LAST_ASKED_VERSION_KEY] = _CURRENT_VERSION
    # No last_ts set -> would otherwise pass cooldown
    assert tel.should_reask_consent(_CURRENT_VERSION) is False


def test_reask_gate_f_cooldown_active_false(_reset_telemetry_state):
    """Test F: does not fire again within 30 days of the last ask."""
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    fake_config[tel.TELEMETRY_LAST_ASKED_VERSION_KEY] = _OTHER_VERSION
    fake_config[tel.TELEMETRY_LAST_ASKED_TS_KEY] = (
        _FIXED_NOW - timedelta(days=10)
    ).isoformat()
    assert tel.should_reask_consent(_CURRENT_VERSION, now=_FIXED_NOW) is False


def test_reask_gate_g_allowed_true(_reset_telemetry_state):
    """Test G: all conditions hold -> re-ask is allowed."""
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    fake_config[tel.TELEMETRY_LAST_ASKED_VERSION_KEY] = _OTHER_VERSION
    fake_config[tel.TELEMETRY_LAST_ASKED_TS_KEY] = (
        _FIXED_NOW - timedelta(days=31)
    ).isoformat()
    fake_config[tel.TELEMETRY_ASK_COUNT_KEY] = 1
    assert tel.should_reask_consent(_CURRENT_VERSION, now=_FIXED_NOW) is True


def test_reask_gate_h_migration_case_true(_reset_telemetry_state):
    """Test H: a pre-feature decliner (no re-ask bookkeeping yet) is allowed.

    FIRST_RUN_SHOWN_KEY is True but none of last_ts/last_version/count exist --
    absent timestamp counts as cooldown-satisfied.
    """
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    assert tel.should_reask_consent(_CURRENT_VERSION) is True


def test_reask_gate_never_raises_on_malformed_config(_reset_telemetry_state):
    """should_reask_consent must never raise, even on unparseable stored data."""
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    fake_config[tel.TELEMETRY_ASK_COUNT_KEY] = "not-an-int"
    fake_config[tel.TELEMETRY_LAST_ASKED_TS_KEY] = "not-a-timestamp"
    # Should not raise; unparseable count falls back to 0, unparseable ts
    # is treated as cooldown-satisfied -> True (version differs, count<cap).
    assert tel.should_reask_consent(_CURRENT_VERSION) is True


# ===========================================================================
# record_consent_ask / set_never_ask persistence tests
# ===========================================================================

def test_record_consent_ask_increments_and_stamps(_reset_telemetry_state):
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state

    tel.record_consent_ask(_CURRENT_VERSION, now=_FIXED_NOW)
    assert fake_config[tel.TELEMETRY_ASK_COUNT_KEY] == 1
    assert fake_config[tel.TELEMETRY_LAST_ASKED_VERSION_KEY] == _CURRENT_VERSION
    assert fake_config[tel.TELEMETRY_LAST_ASKED_TS_KEY] == _FIXED_NOW.isoformat()

    later = _FIXED_NOW + timedelta(days=40)
    tel.record_consent_ask(_CURRENT_VERSION, now=later)
    assert fake_config[tel.TELEMETRY_ASK_COUNT_KEY] == 2
    assert fake_config[tel.TELEMETRY_LAST_ASKED_TS_KEY] == later.isoformat()


def test_record_consent_ask_never_raises(_reset_telemetry_state, monkeypatch):
    import desktop.telemetry as tel

    def _raise(*a, **kw):
        raise RuntimeError("simulated config failure")

    monkeypatch.setattr(tel, 'load_app_config', _raise)
    # Must not raise
    tel.record_consent_ask(_CURRENT_VERSION)


def test_set_never_ask_persists(_reset_telemetry_state):
    import desktop.telemetry as tel
    fake_config = _reset_telemetry_state
    tel.set_never_ask()
    assert fake_config.get(tel.TELEMETRY_NEVER_ASK_KEY) is True

    # And should_reask_consent honors it immediately (Test C, integration check)
    fake_config[tel.FIRST_RUN_SHOWN_KEY] = True
    assert tel.should_reask_consent(_CURRENT_VERSION) is False


# ===========================================================================
# done()-finalizer decline path (drives ConsentDialog directly — needs Qt)
# ===========================================================================

def test_first_run_decline_records_ask_via_done_finalizer(monkeypatch, _reset_telemetry_state):
    """First-run decline calls record_consent_ask(APP_VERSION) -- starts the clock."""
    _skip_if_no_qt()
    import desktop.telemetry as tel
    from desktop.consent_dialog import ConsentDialog
    from PyQt6.QtWidgets import QDialog
    from version import APP_VERSION

    fake_config = _reset_telemetry_state

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    record_calls = []
    original_record = tel.record_consent_ask

    def _recording_record(version, now=None):
        record_calls.append(version)
        return original_record(version, now=now)

    monkeypatch.setattr(tel, 'record_consent_ask', _recording_record)

    dlg = ConsentDialog()
    # _accepted_telemetry stays False (default) — simulates "Not now"
    dlg.done(QDialog.DialogCode.Rejected)

    assert consent_calls == [False], f"set_consent(False) expected; got {consent_calls}"
    assert record_calls == [APP_VERSION], (
        f"record_consent_ask(APP_VERSION) expected exactly once; got {record_calls}"
    )
    assert fake_config.get(tel.TELEMETRY_ASK_COUNT_KEY) == 1
    assert fake_config.get(tel.TELEMETRY_LAST_ASKED_VERSION_KEY) == APP_VERSION
    assert tel.TELEMETRY_LAST_ASKED_TS_KEY in fake_config


def test_accept_path_does_not_record_ask(monkeypatch, _reset_telemetry_state):
    """Accept ('Enable') must NOT call record_consent_ask (is_enabled() short-circuits anyway)."""
    _skip_if_no_qt()
    import desktop.telemetry as tel
    from desktop.consent_dialog import ConsentDialog
    from PyQt6.QtWidgets import QDialog

    fake_config = _reset_telemetry_state

    monkeypatch.setattr(tel, 'set_consent', lambda v: None)

    record_calls = []
    monkeypatch.setattr(tel, 'record_consent_ask', lambda *a, **kw: record_calls.append(a))

    dlg = ConsentDialog()
    dlg._accepted_telemetry = True  # simulate explicit Enable click
    dlg.done(QDialog.DialogCode.Accepted)

    assert record_calls == [], f"record_consent_ask must not fire on accept; got {record_calls}"
    assert tel.TELEMETRY_ASK_COUNT_KEY not in fake_config


# ===========================================================================
# Qt-OFFSCREEN SECTION (Task 2) — TelemetryConsentBar construct + signals
# Run with: GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen \
#           python -m pytest tests/test_telemetry_reask_consent.py -q
# Deep render/interaction is covered by the Task 3 live smoke.
# ===========================================================================

def test_telemetry_consent_bar_constructs_with_four_signals():
    """TelemetryConsentBar exposes the four signals; show_reask()/hide() do not raise."""
    _skip_if_no_qt()
    from desktop.update_ui import TelemetryConsentBar

    bar = TelemetryConsentBar()
    # Hidden by default (bars mount hidden, shown by the startup gate).
    assert bar.isVisible() is False

    # The four pyqtSignals must exist and be connectable.
    for sig_name in ('enable_requested', 'learn_more', 'never_ask_requested', 'dismissed'):
        assert hasattr(bar, sig_name), f"TelemetryConsentBar must expose {sig_name}"
        getattr(bar, sig_name).connect(lambda *a: None)

    # show_reask()/hide() must not raise and must set the invite label text.
    bar.show_reask()
    assert bar.lbl_msg.text(), "show_reask() must populate the invite label"
    bar.hide()
    assert bar.isVisible() is False


def test_telemetry_consent_bar_button_slots_emit_signals():
    """The Enable / Learn more / Don't-ask-again / dismiss slots emit their signals."""
    _skip_if_no_qt()
    from desktop.update_ui import TelemetryConsentBar

    bar = TelemetryConsentBar()
    fired: dict = {'enable': 0, 'learn': 0, 'never': 0, 'dismiss': 0}
    bar.enable_requested.connect(lambda: fired.__setitem__('enable', fired['enable'] + 1))
    bar.learn_more.connect(lambda: fired.__setitem__('learn', fired['learn'] + 1))
    bar.never_ask_requested.connect(lambda: fired.__setitem__('never', fired['never'] + 1))
    bar.dismissed.connect(lambda: fired.__setitem__('dismiss', fired['dismiss'] + 1))

    bar.on_enable()
    bar.on_learn_more()
    bar.on_never_ask()
    bar.on_dismiss()

    assert fired == {'enable': 1, 'learn': 1, 'never': 1, 'dismiss': 1}

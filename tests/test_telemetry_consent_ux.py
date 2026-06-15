# -*- coding: utf-8 -*-
"""Phase 112 Plan 01 — Consent UX test suite (Wave 0).

Two clearly-separated sections:
  - HEADLESS: no QApplication required; exercises gate logic, done()-finalizer,
    shown-flag writes, and opt-out queue drain.
  - Qt-OFFSCREEN: requires QApplication; exercises dialog widget behaviour
    (no-default buttons, focused-Enter, Escape, X-close, PrivacyDialog content).

Notes:
  (a) Windows test isolation: full `pytest tests/` aborts on a non-deterministic
      PyQt6 headless segfault; run this subset directly:
        GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py
  (b) Pytest config + markers live in pyproject.toml (no pytest.ini).
      Registered markers: slow, e2e, packaging, scale.  `qt` is NOT registered.
  (c) Qt gating uses the conftest offscreen pattern, NOT a `qt` marker:
      a module-level `app = QApplication.instance() or QApplication([])` inside
      try/except skips the whole offscreen section if no platform plugin is available.

Critical design note (save_app_config binding):
  desktop/consent_dialog.py calls `genizah_core.save_app_config(...)` via
  MODULE-ATTRIBUTE access (NOT `from genizah_core import save_app_config`).
  This means the _reset_telemetry_state fixture's monkeypatch of
  `genizah_core.save_app_config` DOES intercept dialog writes — the patched name
  is the one the dialog resolves at call time.
"""

from __future__ import annotations

import queue
import pytest
import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — copied verbatim from tests/test_telemetry_consent_gate.py
# Provides an in-memory fake_config dict and patches load/save_app_config on
# both genizah_core (source) and desktop.telemetry (imported-into names).
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


# ===========================================================================
# HEADLESS SECTION
# Tests in this section do NOT require QApplication.
# They drive the dialog's helper methods directly or test engine behaviour.
# ===========================================================================

def test_first_run_gate_skips_if_shown(monkeypatch, _reset_telemetry_state):
    """CONSENT-03: show_first_run_prompt() is a no-op when FIRST_RUN_SHOWN_KEY=True.

    NOTE: show_first_run_prompt is a no-op stub until Plan 02 fills the gate.
    This test passes trivially now and remains meaningful after Plan 02 wires it.
    The sentinel class raises if instantiated — any invocation is a failure.
    """
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY
    _reset_telemetry_state[FIRST_RUN_SHOWN_KEY] = True  # fake_config

    class _ShouldNotInstantiate:
        def __init__(self, *args, **kwargs):
            raise AssertionError("ConsentDialog must NOT be constructed when already shown")

    import desktop.consent_dialog
    monkeypatch.setattr(desktop.consent_dialog, 'ConsentDialog', _ShouldNotInstantiate)

    import desktop.telemetry as tel
    # Must not raise
    tel.show_first_run_prompt()


def test_first_run_constructs_and_execs_once(monkeypatch, _reset_telemetry_state):
    """CONSENT-02: show_first_run_prompt() constructs ConsentDialog exactly once and calls exec().

    NOTE: FIRST_RUN_SHOWN_KEY absent => gate should pass through and construct.
    This test is authored in Wave 0 but goes GREEN only after Plan 02 fills the stub.
    Plan 02 Task 1 acceptance criteria re-runs it with `-k constructs_and_execs_once`.
    """
    construct_count = [0]
    exec_count = [0]

    from PyQt6.QtWidgets import QDialog

    class _RecordingStub:
        def __init__(self, parent=None):
            construct_count[0] += 1

        def exec(self):
            exec_count[0] += 1
            return QDialog.DialogCode.Rejected

    import desktop.consent_dialog
    monkeypatch.setattr(desktop.consent_dialog, 'ConsentDialog', _RecordingStub)

    import desktop.telemetry as tel
    tel.show_first_run_prompt()

    assert construct_count[0] == 1, "ConsentDialog must be constructed exactly once"
    assert exec_count[0] == 1, "ConsentDialog.exec() must be called exactly once"


# ---------------------------------------------------------------------------
# done()-finalizer tests (REVIEWS HIGH-1)
# Each path drives ConsentDialog.done(result) and asserts:
#   (a) FIRST_RUN_SHOWN_KEY=True in the fake config
#   (b) set_consent called with the correct bool
# ---------------------------------------------------------------------------

def _make_consent_dialog():
    """Construct a ConsentDialog instance under the offscreen QApplication."""
    from desktop.consent_dialog import ConsentDialog
    return ConsentDialog()


def test_done_finalizer_writes_flag_on_accept(monkeypatch, _reset_telemetry_state):
    """Accept path: done(Accepted) writes FIRST_RUN_SHOWN_KEY=True and calls set_consent(True)."""
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = _make_consent_dialog()
    dlg._accepted_telemetry = True  # simulate explicit Enable click
    from PyQt6.QtWidgets import QDialog
    dlg.done(QDialog.DialogCode.Accepted)

    import genizah_core
    cfg = genizah_core.load_app_config()
    assert cfg.get(FIRST_RUN_SHOWN_KEY) is True, "FIRST_RUN_SHOWN_KEY must be True after accept"
    assert consent_calls == [True], f"set_consent(True) expected; got {consent_calls}"


def test_done_finalizer_writes_flag_on_decline(monkeypatch, _reset_telemetry_state):
    """Decline path: done(Rejected) writes FIRST_RUN_SHOWN_KEY=True and calls set_consent(False)."""
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = _make_consent_dialog()
    # _accepted_telemetry stays False (default) — decline
    from PyQt6.QtWidgets import QDialog
    dlg.done(QDialog.DialogCode.Rejected)

    import genizah_core
    cfg = genizah_core.load_app_config()
    assert cfg.get(FIRST_RUN_SHOWN_KEY) is True, "FIRST_RUN_SHOWN_KEY must be True after decline"
    assert consent_calls == [False], f"set_consent(False) expected; got {consent_calls}"


def test_done_finalizer_writes_flag_on_escape(monkeypatch, _reset_telemetry_state):
    """Escape path: keyPressEvent(Escape) routes through reject()→done() and writes flag + opt-out."""
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = _make_consent_dialog()
    # Simulate Escape: _accepted_telemetry stays False; call reject() which calls done(Rejected)
    from PyQt6.QtWidgets import QDialog
    dlg.reject()  # Escape routes through reject()

    import genizah_core
    cfg = genizah_core.load_app_config()
    assert cfg.get(FIRST_RUN_SHOWN_KEY) is True, "FIRST_RUN_SHOWN_KEY must be True after Escape"
    assert consent_calls == [False], f"set_consent(False) expected on Escape; got {consent_calls}"


def test_done_finalizer_writes_flag_on_close(monkeypatch, _reset_telemetry_state):
    """Close (X-button) path: done(Rejected) writes FIRST_RUN_SHOWN_KEY=True and calls set_consent(False).

    X-close routes through reject()→done(Rejected) in Qt's close sequence.
    We drive done() directly here (headless mode — dialog not shown, so close()
    does not trigger the full Qt event chain on a hidden widget; done() is the
    canonical finalizer we test).
    """
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = _make_consent_dialog()
    # Drive done() directly simulating X-close (Rejected, _accepted_telemetry=False)
    from PyQt6.QtWidgets import QDialog
    dlg.done(QDialog.DialogCode.Rejected)

    import genizah_core
    cfg = genizah_core.load_app_config()
    assert cfg.get(FIRST_RUN_SHOWN_KEY) is True, "FIRST_RUN_SHOWN_KEY must be True after X-close"
    assert consent_calls == [False], f"set_consent(False) expected on X-close; got {consent_calls}"


def test_settings_cancel_does_not_desync_telemetry(_reset_telemetry_state):
    """D-07b / T-112-CancelDesync: Cancel in SettingsDialog must NOT overwrite
    telemetry keys that set_consent() already wrote to config.pkl.

    Simulates the exact fix in SettingsDialog.__init__: the snapshot is built by
    stripping the 7 telemetry key constants from the full config dict, then
    save_app_config(snapshot) is called (as _on_cancel does). Since save_app_config
    is additive-merge, the telemetry keys NOT in the snapshot are left untouched.

    After opt-in followed by a snapshot-restore that omits telemetry keys,
    load_app_config()[TELEMETRY_ENABLED_KEY] must still be True (no desync).
    """
    import desktop.telemetry as tel
    from desktop.telemetry import (
        TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY,
        TELEMETRY_INSTALL_ID_KEY, CONSENT_TIMESTAMP_KEY,
        CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
    )
    import genizah_core

    _TELEMETRY_SNAPSHOT_EXCLUDE = frozenset({
        TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY, TELEMETRY_INSTALL_ID_KEY,
        CONSENT_TIMESTAMP_KEY, CONSENT_APP_VERSION_KEY,
        CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY,
    })

    # Step 1: opt in (simulates user flipping toggle in Settings)
    tel.set_consent(True)
    cfg_after_consent = genizah_core.load_app_config()
    assert cfg_after_consent.get(TELEMETRY_ENABLED_KEY) is True, (
        "Precondition: set_consent(True) must write TELEMETRY_ENABLED_KEY=True"
    )

    # Step 2: build a snapshot as SettingsDialog.__init__ does (strip telemetry keys)
    full_cfg = genizah_core.load_app_config()
    snapshot = {k: v for k, v in full_cfg.items() if k not in _TELEMETRY_SNAPSHOT_EXCLUDE}
    assert TELEMETRY_ENABLED_KEY not in snapshot, (
        "Snapshot must not contain TELEMETRY_ENABLED_KEY (D-07b strip)"
    )

    # Step 3: simulate _on_cancel restoring the snapshot (additive-merge)
    genizah_core.save_app_config(snapshot)

    # Step 4: verify telemetry state is NOT desynced — engine wrote True; disk must still be True
    cfg_after_cancel = genizah_core.load_app_config()
    assert cfg_after_cancel.get(TELEMETRY_ENABLED_KEY) is True, (
        "After snapshot restore, TELEMETRY_ENABLED_KEY must remain True "
        "(save_app_config is additive-merge — omitted keys are preserved). "
        "T-112-CancelDesync."
    )
    assert tel.is_enabled() is True, (
        "is_enabled() must still return True after snapshot restore (no engine desync)"
    )


def test_optout_drains_queue(_reset_telemetry_state):
    """CONSENT-08 verification: set_consent(False) drains any queued events.

    Phase 111 already implemented _drain_and_discard inside set_consent(False).
    This test proves the drain via the engine's normal opt-in + track + opt-out flow.
    No new drain logic is re-implemented here.
    """
    import desktop.telemetry as tel
    from desktop.telemetry import DesktopEvent

    # First opt in so we can enqueue an event
    tel.set_consent(True)
    tel.track(DesktopEvent.SELFTEST)

    # Queue should be non-empty
    assert not ph._event_queue.empty(), "Precondition: event should be enqueued after consent+track"

    # Now opt out — should drain queue
    tel.set_consent(False)
    assert ph._event_queue.empty(), "Queue must be drained after set_consent(False)"


# ===========================================================================
# Qt-OFFSCREEN SECTION
# Requires QApplication + offscreen platform.
# Gate: construct QApplication at module level; skip entire section if unavailable.
# Run with: GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py
# ===========================================================================

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


def test_consent_dialog_no_default_button():
    """T-112-EnterOptIn SC#1: both buttons must NOT be default or auto-default."""
    _skip_if_no_qt()
    from desktop.consent_dialog import ConsentDialog
    dlg = ConsentDialog()
    assert dlg.btn_enable.isDefault() is False, "btn_enable must not be the default button"
    assert dlg.btn_enable.autoDefault() is False, "btn_enable must not be auto-default"
    assert dlg.btn_decline.isDefault() is False, "btn_decline must not be the default button"
    assert dlg.btn_decline.autoDefault() is False, "btn_decline must not be auto-default"
    dlg.reject()  # clean up — routes through done() which will call set_consent(False)


def test_consent_dialog_enter_is_decline(monkeypatch):
    """REVIEWS HIGH-2: focused Enter on btn_enable must NOT opt in.

    Explicitly focuses btn_enable (worst case: a focused QPushButton could consume
    Return before the dialog's keyPressEvent), then posts a Key_Return event.
    Asserts set_consent was NOT called with True.
    """
    _skip_if_no_qt()
    import desktop.telemetry as tel
    from desktop.consent_dialog import ConsentDialog
    from PyQt6.QtCore import Qt, QEvent
    from PyQt6.QtGui import QKeyEvent

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = ConsentDialog()
    dlg.btn_enable.setFocus()  # Focus the Enable button — worst case

    # Post Key_Return directly to the focused widget (btn_enable) and to the dialog
    key_event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Return, Qt.KeyboardModifier.NoModifier)
    QApplication.sendEvent(dlg.btn_enable, key_event)
    QApplication.processEvents()

    # Postcondition: set_consent must NOT have been called with True
    assert True not in consent_calls, (
        f"Enter/Return must NOT opt in even when btn_enable is focused; "
        f"set_consent calls: {consent_calls}"
    )
    # Clean up
    try:
        dlg.close()
    except Exception:
        pass


def test_consent_dialog_escape_opts_out(monkeypatch):
    """REVIEWS HIGH-1: Escape routes through done() — writes flag + set_consent(False)."""
    _skip_if_no_qt()
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY
    from desktop.consent_dialog import ConsentDialog
    from PyQt6.QtCore import Qt, QEvent
    from PyQt6.QtGui import QKeyEvent

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = ConsentDialog()

    # Post Escape key to dialog
    escape_event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Escape, Qt.KeyboardModifier.NoModifier)
    QApplication.sendEvent(dlg, escape_event)
    QApplication.processEvents()

    import genizah_core
    cfg = genizah_core.load_app_config()
    assert cfg.get(FIRST_RUN_SHOWN_KEY) is True, "FIRST_RUN_SHOWN_KEY must be True after Escape"
    assert False in consent_calls, f"set_consent(False) expected; got {consent_calls}"
    assert True not in consent_calls, f"set_consent(True) must NOT occur on Escape; got {consent_calls}"


def test_consent_dialog_close_opts_out(monkeypatch):
    """REVIEWS HIGH-1: X-close routes through reject()→done() — writes flag + set_consent(False).

    Qt's X-button (window close) sends a closeEvent which calls reject() on a QDialog.
    reject() in turn calls done(Rejected). We drive reject() directly here because
    close() on a non-visible dialog is a Qt no-op (the close event is only sent when the
    window is visible). The actual close-event→reject()→done() chain is the production
    path; this test verifies the done() finalizer fires correctly via reject().
    """
    _skip_if_no_qt()
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY
    from desktop.consent_dialog import ConsentDialog

    consent_calls = []
    monkeypatch.setattr(tel, 'set_consent', lambda v: consent_calls.append(v))

    dlg = ConsentDialog()
    dlg.reject()  # X-close → closeEvent → reject() → done(Rejected)
    QApplication.processEvents()

    import genizah_core
    cfg = genizah_core.load_app_config()
    assert cfg.get(FIRST_RUN_SHOWN_KEY) is True, "FIRST_RUN_SHOWN_KEY must be True after X-close"
    assert False in consent_calls, f"set_consent(False) expected; got {consent_calls}"
    assert True not in consent_calls, f"set_consent(True) must NOT occur on X-close; got {consent_calls}"


def test_privacy_dialog_constructs_bilingual():
    """PRIV-05: PrivacyDialog must contain bilingual EN+HE content covering D-10 points.

    Assertions:
    - Contains 'PostHog' (data processor)
    - Contains 'My Library' or 'file paths' or 'filenames' (what-is-NOT-collected)
    - Contains 'privacy-preserving' (or HE equivalent — NOT bare 'anonymous')
    - Contains a dir='rtl' block (Hebrew)
    - Contains a dir='ltr' block (English)
    - Does NOT use 'anonymous' as the headline data descriptor
    """
    _skip_if_no_qt()
    from desktop.consent_dialog import PrivacyDialog

    dlg = PrivacyDialog()
    html = dlg._build_html()

    assert 'PostHog' in html, "PrivacyDialog must mention PostHog as data processor"
    assert any(term in html for term in ('My Library', 'file paths', 'filenames')), (
        "PrivacyDialog must state My Library paths/filenames are NOT collected"
    )
    assert any(term in html for term in ('privacy-preserving', 'privacy_preserving', 'שומרי-פרטיות', 'שומרי פרטיות')), (
        "PrivacyDialog must use 'privacy-preserving' wording (not bare 'anonymous')"
    )
    assert "dir='rtl'" in html or 'dir="rtl"' in html, "PrivacyDialog must have a dir=rtl block for Hebrew"
    assert "dir='ltr'" in html or 'dir="ltr"' in html, "PrivacyDialog must have a dir=ltr block for English"
    # Ensure 'anonymous' is not used as THE headline descriptor
    # (it may appear in compound phrases but not as the sole qualifier before 'data' or 'usage')
    import re
    bare_anonymous = re.search(r'\banonymous\s+(?:usage|data|identifier|install)\b', html, re.IGNORECASE)
    assert bare_anonymous is None, (
        f"PrivacyDialog must not use 'anonymous' as headline descriptor; "
        f"found: {bare_anonymous.group() if bare_anonymous else ''}"
    )
    dlg.accept()

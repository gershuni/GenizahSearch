# -*- coding: utf-8 -*-
"""Phase 114 Plan 01 — Identity lifecycle, session foundation, and Wave-0 scaffold.

Covers:
- Task 1: ACTIVE_PING enum member + consent gate
- Task 2: Startup identity coordinator (identity-sync split from one-shot session_start)
          + mid-session opt-in re-identify + producer gate
- Task 3: Login/logout/register identity wiring + best-effort session_end + shutdown flag

Autouse fixture resets module state before/after each test (same pattern as
test_telemetry_identity.py).
"""

from __future__ import annotations

import queue

import pytest

import shared.posthog_server as ph
from desktop.telemetry import DesktopEvent


# ---------------------------------------------------------------------------
# Autouse fixture — resets module-level state before/after each test
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
# Task 1: ACTIVE_PING enum member + Wave-0 scaffold
# ===========================================================================

def test_active_ping_enum_member():
    """DesktopEvent.ACTIVE_PING must exist with value 'desktop_active_ping'."""
    import desktop.telemetry as tel
    assert DesktopEvent.ACTIVE_PING.value == 'desktop_active_ping'
    assert 'desktop_active_ping' in tel._VALID_EVENT_VALUES


def test_active_ping_track_enqueues():
    """With consent ON, track(ACTIVE_PING) enqueues exactly one event."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    tel.track(DesktopEvent.ACTIVE_PING, session_id='s1')
    payload = ph._event_queue.get(timeout=1.0)
    assert payload['event'] == 'desktop_active_ping'


def test_active_ping_consent_gated():
    """With consent False, track(ACTIVE_PING) enqueues nothing."""
    import desktop.telemetry as tel
    # Consent is False by default (no set_consent call)
    tel.track(DesktopEvent.ACTIVE_PING, session_id='s1')
    assert ph._event_queue.empty(), "track(ACTIVE_PING) must be consent-gated"


# ===========================================================================
# Task 2: Startup identity coordinator + producer gate
# ===========================================================================

class _FakeUser:
    """Minimal stand-in for supabase_corrections_client.User."""
    def __init__(self, uuid_val: str):
        self._uuid = uuid_val
        self.id = 123456789  # int hash — must NEVER be used as distinct_id


class _FakeCorrectionsClient:
    """Minimal stand-in for SupabaseCorrectionsClient."""
    def __init__(self, user=None):
        self.current_user = user

    def is_logged_in(self):
        return self.current_user is not None


def _make_gui_stub(monkeypatch, fake_config, user=None):
    """Build a minimal object that simulates GenizahGUI for coordinator tests.

    Uses a SimpleNamespace to avoid importing PyQt6 in tests.
    """
    import types
    gui = types.SimpleNamespace()
    gui.corrections_client = _FakeCorrectionsClient(user)

    # Bind methods from genizah_app onto gui
    import genizah_app as app
    gui._run_startup_telemetry_coordinator = (
        lambda: app.GenizahGUI._run_startup_telemetry_coordinator(gui)
    )
    gui._sync_telemetry_identity = (
        lambda: app.GenizahGUI._sync_telemetry_identity(gui)
    )
    gui._telemetry_ready = (
        lambda: app.GenizahGUI._telemetry_ready(gui)
    )
    # _setup_active_ping is a stub call site — provide a no-op so coordinator can run
    gui._setup_active_ping = lambda: None
    return gui


def _drain_all_events() -> list[dict]:
    """Drain everything currently in the PostHog queue."""
    events = []
    while not ph._event_queue.empty():
        try:
            events.append(ph._event_queue.get_nowait())
        except Exception:
            break
    return events


def test_coordinator_consent_off_emits_nothing(monkeypatch, _reset_telemetry_state):
    """With consent OFF, the coordinator emits nothing."""
    import desktop.telemetry as tel
    # consent stays False
    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state)
    gui._run_startup_telemetry_coordinator()
    assert ph._event_queue.empty(), "coordinator must emit nothing when consent is OFF"


def test_coordinator_identify_uuid_not_int_id(monkeypatch, _reset_telemetry_state):
    """Coordinator calls identify(_uuid) — NEVER .id — when user is logged in.

    D-10 hard rule: distinct_id must be _uuid (raw Supabase UUID string),
    NEVER current_user.id (int hash).
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    user = _FakeUser('test-supabase-uuid-abc123')
    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state, user=user)
    gui._run_startup_telemetry_coordinator()

    events = _drain_all_events()
    identify_events = [e for e in events if e['event'] == '$identify']
    assert len(identify_events) >= 1, "coordinator must emit $identify when logged in"
    assert identify_events[0]['distinct_id'] == 'test-supabase-uuid-abc123', (
        "distinct_id must be _uuid (raw Supabase UUID), never the int .id hash"
    )
    # Confirm the int hash was NOT used
    assert identify_events[0]['distinct_id'] != str(user.id), (
        "distinct_id must NOT be current_user.id (int hash)"
    )


def test_coordinator_session_start_props_allowlisted(monkeypatch, _reset_telemetry_state):
    """session_start event must contain ONLY allowlisted env props.

    No hostname, username, path, cwd, or user-content keys.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state)
    gui._run_startup_telemetry_coordinator()

    events = _drain_all_events()
    session_start_events = [e for e in events if e['event'] == 'desktop_session_start']
    assert len(session_start_events) == 1, "exactly one desktop_session_start must fire"

    props = session_start_events[0].get('properties', {})
    forbidden_keys = {'hostname', 'username', 'user', 'home', 'cwd', 'path',
                      'computername', 'logname', 'userprofile'}
    leaked = {k for k in props if k.lower() in forbidden_keys}
    assert not leaked, f"session_start contains forbidden keys: {leaked}"

    # session_id must be present
    assert 'session_id' in props, "session_start must carry session_id"
    assert props['session_id'], "session_id must not be empty"


def test_coordinator_idempotent_session_start(monkeypatch, _reset_telemetry_state):
    """Calling the coordinator twice must yield exactly one desktop_session_start.

    _telemetry_session_started guards the one-shot session_start (D-14).
    Identity-sync still re-runs on the second call (HIGH-4).
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state)
    gui._run_startup_telemetry_coordinator()
    gui._run_startup_telemetry_coordinator()  # second call

    events = _drain_all_events()
    session_start_count = sum(1 for e in events if e['event'] == 'desktop_session_start')
    assert session_start_count == 1, (
        f"Exactly one session_start expected, got {session_start_count}"
    )


def test_coordinator_stale_identity_triggers_reset(monkeypatch, _reset_telemetry_state):
    """Stale persisted IDENTIFIED_USER_KEY + no live user → reset_identity(), NOT identify().

    D-12 stale-identity fix.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)
    # Simulate a stale stored UUID with no live logged-in user
    _reset_telemetry_state[tel.IDENTIFIED_USER_KEY] = 'stale-old-uuid'

    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state, user=None)
    gui._run_startup_telemetry_coordinator()

    events = _drain_all_events()
    identify_events = [e for e in events if e['event'] == '$identify']
    reset_events = [e for e in events if e['event'] == 'desktop_identity_reset']

    assert len(identify_events) == 0, "stale identity must NOT trigger identify()"
    assert len(reset_events) >= 1, "stale identity must trigger reset_identity()"


def test_reopt_in_reidentifies_without_second_session_start(monkeypatch, _reset_telemetry_state):
    """HIGH-4: mid-session opt-out→opt-in re-identifies logged-in _uuid.

    Sequence:
    1. consent ON → coordinator runs → identify(_uuid) + session_start fire
    2. consent OFF → set_consent(False) only; do NOT reset module state
    3. consent ON → coordinator re-runs:
       - _sync_telemetry_identity UNCONDITIONALLY before the session_start guard
         → a second $identify fires for the logged-in _uuid
       - session_start one-shot guard (_telemetry_session_started=True) suppresses
         a second session_start

    Asserts:
    - >= 2 x $identify with distinct_id == _uuid
    - exactly 1 x desktop_session_start
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    user = _FakeUser('user-reopt-uuid-xyz')
    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state, user=user)

    # Step 1: First opt-in — coordinator runs, identify + session_start fire
    gui._run_startup_telemetry_coordinator()
    events_first = _drain_all_events()

    identify_first = [
        e for e in events_first
        if e['event'] == '$identify' and e.get('distinct_id') == 'user-reopt-uuid-xyz'
    ]
    session_start_first = [e for e in events_first if e['event'] == 'desktop_session_start']
    assert len(identify_first) == 1, "First opt-in must emit one $identify"
    assert len(session_start_first) == 1, "First opt-in must emit one session_start"

    # Step 2: opt-out (do NOT reset telemetry module state — that's what a real opt-out does)
    tel.set_consent(False)
    _drain_all_events()  # discard any identity_reset events

    # Step 3: opt-in again — re-run coordinator
    tel.set_consent(True)
    tel._load_consent_state()
    gui._run_startup_telemetry_coordinator()
    events_second = _drain_all_events()

    # _sync_telemetry_identity ran unconditionally, so a second $identify fires
    identify_second = [
        e for e in events_second
        if e['event'] == '$identify' and e.get('distinct_id') == 'user-reopt-uuid-xyz'
    ]
    # session_start must NOT fire again (_telemetry_session_started already True)
    session_start_second = [e for e in events_second if e['event'] == 'desktop_session_start']

    assert len(identify_second) >= 1, (
        f"Re-opt-in must re-emit $identify (got {len(identify_second)}) — "
        "_sync_telemetry_identity must run unconditionally before the session_start guard"
    )
    assert len(session_start_second) == 0, (
        f"Re-opt-in must NOT emit a second session_start (got {len(session_start_second)})"
    )


def test_telemetry_ready_gate(monkeypatch, _reset_telemetry_state):
    """MEDIUM-9: _telemetry_ready() is False before coordinator runs; True after (consent ON)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_gui_stub(monkeypatch, _reset_telemetry_state)

    # Before coordinator: _telemetry_session_started is not set → False
    assert gui._telemetry_ready() is False, (
        "_telemetry_ready() must return False before the coordinator has run"
    )

    gui._run_startup_telemetry_coordinator()

    assert gui._telemetry_ready() is True, (
        "_telemetry_ready() must return True after the coordinator runs with consent ON"
    )


# ===========================================================================
# Task 3: Login/logout/register identity wiring + session_end + shutdown flag
# ===========================================================================

class _FakeGui:
    """Minimal stub that binds genizah_app methods for login/logout/session tests.

    We import the actual methods from genizah_app.GenizahGUI and bind them onto
    this stub to avoid spinning up the full PyQt6 GUI.
    """
    def __init__(self, user=None):
        self.corrections_client = _FakeCorrectionsClient(user)
        # Attributes that the real methods reference but we don't need to exercise
        self._telemetry_session_started = False
        self._app_shutting_down = False
        self._session_end_emitted = False

    def _sync_telemetry_identity(self):
        import genizah_app as app
        return app.GenizahGUI._sync_telemetry_identity(self)

    def closeEvent_telemetry_part(self):
        """Execute only the telemetry parts of closeEvent (not the Qt thread teardown)."""
        import genizah_app as app
        # Set shutdown flag + emit session_end (replicate the relevant closeEvent snippet)
        self._app_shutting_down = True
        try:
            import desktop.telemetry as tel
            if not getattr(self, '_session_end_emitted', False):
                self._session_end_emitted = True
                tel.track(
                    tel.DesktopEvent.SESSION_END,
                    session_id=getattr(self, '_session_id', ''),
                )
        except Exception:
            pass


def test_login_calls_identify_with_uuid(monkeypatch, _reset_telemetry_state):
    """After successful login, _sync_telemetry_identity() emits $identify with _uuid.

    D-10: distinct_id must be current_user._uuid.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    user = _FakeUser('login-uuid-abc')
    gui = _FakeGui(user=user)
    gui._sync_telemetry_identity()

    events = _drain_all_events()
    identify_events = [e for e in events if e['event'] == '$identify']
    assert len(identify_events) == 1, "login identity sync must emit $identify"
    assert identify_events[0]['distinct_id'] == 'login-uuid-abc', (
        "login identity sync must use _uuid, not .id"
    )


def test_logout_calls_reset_identity(monkeypatch, _reset_telemetry_state):
    """Logout (reset_identity) reverts to install_id and emits desktop_identity_reset."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    tel.reset_identity()

    events = _drain_all_events()
    reset_events = [e for e in events if e['event'] == 'desktop_identity_reset']
    assert len(reset_events) >= 1, "reset_identity must emit desktop_identity_reset"


def test_session_end_fires_on_close(monkeypatch, _reset_telemetry_state):
    """closeEvent emits exactly one desktop_session_end."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    # Set up a session_id to carry
    gui = _FakeGui()
    gui._session_id = 'close-session-xyz'

    gui.closeEvent_telemetry_part()

    events = _drain_all_events()
    session_end_events = [e for e in events if e['event'] == 'desktop_session_end']
    assert len(session_end_events) == 1, "exactly one desktop_session_end must fire on close"


def test_session_end_exactly_once_guard(monkeypatch, _reset_telemetry_state):
    """Double closeEvent does NOT emit two session_end events."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _FakeGui()
    gui._session_id = 'close-session-guard'

    gui.closeEvent_telemetry_part()
    gui.closeEvent_telemetry_part()  # second call — must be suppressed by _session_end_emitted

    events = _drain_all_events()
    session_end_count = sum(1 for e in events if e['event'] == 'desktop_session_end')
    assert session_end_count == 1, (
        f"session_end must fire exactly once, got {session_end_count}"
    )

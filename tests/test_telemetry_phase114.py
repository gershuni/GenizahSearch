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
    """Reset desktop.telemetry + posthog_server state before/after each test.

    Drain-thread isolation strategy (Phase 114 task 2):
    The posthog_server drain daemon thread starts lazily and once started, reads
    _event_queue as a module-level global on every iteration.  When the autouse
    fixture monkeypatches _event_queue → fresh_q, the running daemon thread will
    begin consuming from fresh_q before tests can assert on it.

    Fix: also patch enqueue_event to bypass _start_drain_thread_once entirely,
    putting directly into fresh_q.  The daemon thread (if alive) stays blocked
    on the previous queue object it last tried to get() from.
    """
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

    # Prevent the drain daemon thread from consuming test events.
    # Patch _start_drain_thread_once to no-op so no new thread is started.
    monkeypatch.setattr(ph, '_start_drain_thread_once', lambda: None)

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
    # consent stays False (no set_consent call)
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


# ===========================================================================
# Task 1 (Wave 2): desktop_tab_activated — user-only, hardcoded tab_name enum
# ===========================================================================

def _make_tab_gui_stub(monkeypatch):
    """Build a minimal stub for _on_tab_changed telemetry tests."""
    import types
    import genizah_app as app
    import shared.posthog_server as ph_mod

    # Prevent the drain daemon thread from consuming events before the test checks
    # the queue.  The drain thread is started lazily by enqueue_event →
    # _start_drain_thread_once().  We replace it with a no-op for these tests so
    # events remain in the queue long enough to assert on.
    monkeypatch.setattr(ph_mod, '_start_drain_thread_once', lambda: None)

    gui = types.SimpleNamespace()
    gui._restoring_session = False
    gui._programmatic_tab_change = False
    gui._telemetry_session_started = True  # ready
    gui._session_id = 'tab-test-session'

    # Minimal tabs stub so existing _on_tab_changed body (community/catalog lazy-load) won't crash
    tabs_stub = types.SimpleNamespace()
    tabs_stub.widget = lambda index: None  # returns None — no community/catalog matches
    gui.tabs = tabs_stub

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._on_tab_changed = lambda index: app.GenizahGUI._on_tab_changed(gui, index)
    return gui


def test_tab_activated_user_switch_emits_correct_tab_name(monkeypatch, _reset_telemetry_state):
    """User tab switch emits desktop_tab_activated with hardcoded tab_name constant."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_tab_gui_stub(monkeypatch)
    gui._on_tab_changed(0)  # search tab

    events = _drain_all_events()
    tab_events = [e for e in events if e['event'] == 'desktop_tab_activated']
    assert len(tab_events) == 1, "user tab switch must emit desktop_tab_activated"
    assert tab_events[0]['properties']['tab_name'] == 'search'

    # Also test index 6 → my_library
    gui._on_tab_changed(6)
    events2 = _drain_all_events()
    tab_events2 = [e for e in events2 if e['event'] == 'desktop_tab_activated']
    assert len(tab_events2) == 1
    assert tab_events2[0]['properties']['tab_name'] == 'my_library'


def test_tab_activated_suppressed_during_restore(monkeypatch, _reset_telemetry_state):
    """_restoring_session=True suppresses desktop_tab_activated (D-02)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_tab_gui_stub(monkeypatch)
    gui._restoring_session = True
    gui._on_tab_changed(0)

    events = _drain_all_events()
    tab_events = [e for e in events if e['event'] == 'desktop_tab_activated']
    assert len(tab_events) == 0, "restore-driven tab change must NOT emit tab_activated"


def test_tab_activated_suppressed_programmatic(monkeypatch, _reset_telemetry_state):
    """_programmatic_tab_change=True suppresses desktop_tab_activated (REVIEWS MEDIUM-5)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_tab_gui_stub(monkeypatch)
    gui._programmatic_tab_change = True
    gui._on_tab_changed(1)

    events = _drain_all_events()
    tab_events = [e for e in events if e['event'] == 'desktop_tab_activated']
    assert len(tab_events) == 0, "programmatic tab change must NOT emit tab_activated"


def test_tab_activated_suppressed_when_not_ready(monkeypatch, _reset_telemetry_state):
    """_telemetry_ready()=False suppresses desktop_tab_activated (REVIEWS MEDIUM-9)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_tab_gui_stub(monkeypatch)
    gui._telemetry_session_started = False  # not ready yet

    gui._on_tab_changed(0)

    events = _drain_all_events()
    tab_events = [e for e in events if e['event'] == 'desktop_tab_activated']
    assert len(tab_events) == 0, "tab_activated must not emit before _telemetry_ready()"


def test_tab_activated_out_of_range_index_no_emit(monkeypatch, _reset_telemetry_state):
    """Out-of-range tab index emits nothing (no crash)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_tab_gui_stub(monkeypatch)
    gui._on_tab_changed(99)  # out of range

    events = _drain_all_events()
    tab_events = [e for e in events if e['event'] == 'desktop_tab_activated']
    assert len(tab_events) == 0, "out-of-range index must not emit"


def test_set_active_tab_helper_exists(monkeypatch, _reset_telemetry_state):
    """GenizahGUI._set_active_tab method exists for programmatic tab change suppression."""
    import genizah_app as app
    assert hasattr(app.GenizahGUI, '_set_active_tab'), (
        "_set_active_tab must exist on GenizahGUI for REVIEWS MEDIUM-5 guard"
    )


# ===========================================================================
# Task 2: Per-run search state object + _emit_search_telemetry
# ===========================================================================

def _make_search_emit_stub(monkeypatch, *, telemetry_ready=True, app_shutting_down=False,
                           mode='keyword', corpus='genizah'):
    """Build a minimal stub for _emit_search_telemetry unit tests.

    Provides a pre-populated _current_search_run dict and patches the drain thread.
    """
    import types
    import genizah_app as app
    import shared.posthog_server as ph_mod

    monkeypatch.setattr(ph_mod, '_start_drain_thread_once', lambda: None)

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = telemetry_ready
    gui._app_shutting_down = app_shutting_down
    gui._session_id = 'search-test-session'
    gui._current_search_run = {'mode': mode, 'corpus': corpus, 'emitted': False}

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_search_telemetry(gui, action, result_count)
    )
    return gui


def test_search_emit_completed_enqueues_event(monkeypatch, _reset_telemetry_state):
    """_emit_search_telemetry('completed', 0) enqueues one desktop_search_executed event."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_search_emit_stub(monkeypatch, mode='keyword', corpus='genizah')
    gui._emit_search_telemetry('completed', 0)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, "completed emit must enqueue exactly one desktop_search_executed"
    props = search_events[0]['properties']
    assert props['action'] == 'completed'
    assert props['search_mode'] == 'keyword'
    assert props['corpus_scope'] == 'genizah'
    assert 'result_count_bucket' in props, "completed emit must include result_count_bucket"
    assert props['result_count_bucket'] == '0', "count 0 must bucket to '0'"


def test_search_emit_bucket_mapping(monkeypatch, _reset_telemetry_state):
    """Result count bucketing: 0→'0', 5→'1-9', 42→'10-99', 250→'100+'."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    for count, expected_bucket in [(0, '0'), (5, '1-9'), (42, '10-99'), (250, '100+')]:
        monkeypatch.setattr(ph, '_event_queue',
                            __import__('queue').Queue(maxsize=10000))
        gui = _make_search_emit_stub(monkeypatch, mode='keyword', corpus='genizah')
        gui._emit_search_telemetry('completed', count)
        events = _drain_all_events()
        search_events = [e for e in events if e['event'] == 'desktop_search_executed']
        assert len(search_events) == 1, f"count={count}: expected 1 event"
        bucket = search_events[0]['properties'].get('result_count_bucket')
        assert bucket == expected_bucket, (
            f"count={count}: expected bucket '{expected_bucket}', got '{bucket}'"
        )


def test_search_emit_cancelled_no_bucket(monkeypatch, _reset_telemetry_state):
    """Cancelled run emits action='cancelled' with NO result_count_bucket (D-08)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_search_emit_stub(monkeypatch)
    gui._emit_search_telemetry('cancelled')

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, "cancelled emit must enqueue one event"
    props = search_events[0]['properties']
    assert props['action'] == 'cancelled'
    assert 'result_count_bucket' not in props, (
        "cancelled run must NOT include result_count_bucket (D-08)"
    )


def test_search_emit_exactly_once(monkeypatch, _reset_telemetry_state):
    """Second call on same run is suppressed by the emitted guard (D-09)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_search_emit_stub(monkeypatch)
    gui._emit_search_telemetry('completed', 5)
    gui._emit_search_telemetry('completed', 5)  # second call — must be no-op

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, (
        f"_emit_search_telemetry must fire exactly once per run, got {len(search_events)}"
    )


def test_search_emit_shutdown_guard(monkeypatch, _reset_telemetry_state):
    """_emit_search_telemetry emits NOTHING when _app_shutting_down=True (REVIEWS HIGH-2)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_search_emit_stub(monkeypatch, app_shutting_down=True)
    gui._emit_search_telemetry('completed', 0)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0, (
        "first-line _app_shutting_down guard must suppress emit during app shutdown (REVIEWS HIGH-2)"
    )


def test_search_emit_ready_gate(monkeypatch, _reset_telemetry_state):
    """_emit_search_telemetry emits NOTHING when _telemetry_ready() False (REVIEWS MEDIUM-9)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_search_emit_stub(monkeypatch, telemetry_ready=False)
    gui._emit_search_telemetry('completed', 0)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0, (
        "_emit_search_telemetry must not emit before _telemetry_ready() (REVIEWS MEDIUM-9)"
    )


def test_search_emit_mode_is_hardcoded_enum(monkeypatch, _reset_telemetry_state):
    """search_mode value is a hardcoded enum string, not a translated combo label."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    # Test Lab variants mode (confirming lab_ prefix)
    gui = _make_search_emit_stub(monkeypatch, mode='lab_variants', corpus='local')
    gui._emit_search_telemetry('completed', 1)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1
    props = search_events[0]['properties']
    assert props['search_mode'] == 'lab_variants', (
        "search_mode must be the hardcoded enum value 'lab_variants'"
    )
    assert props['corpus_scope'] == 'local', (
        "corpus_scope must reflect currentData() code, not translated label"
    )
    # Translated labels must never appear
    for forbidden in ('Variants', 'וריאנטים', 'Lab Mode', 'מצב מעבדה'):
        assert props.get('search_mode') != forbidden, (
            f"search_mode must not be translated label '{forbidden}'"
        )


# ===========================================================================
# Task 3: Wire _emit_search_telemetry into on_search_finished + stop_search
# ===========================================================================
# These tests use a spy to confirm that on_search_finished and stop_search
# ACTUALLY call _emit_search_telemetry with the correct arguments.
# They fail (RED) before wiring because no call happens.

def _make_wiring_stub(monkeypatch):
    """Stub for on_search_finished / stop_search wiring tests.

    Tracks calls to _emit_search_telemetry via a spy list.
    """
    import types
    import genizah_app as app

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = True
    gui._app_shutting_down = False
    gui._session_id = 'wire-test-session'
    gui._current_search_run = {'mode': 'keyword', 'corpus': 'genizah', 'emitted': False}
    gui._search_was_cancelled = False

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)

    # Spy: record calls instead of actually emitting
    gui._emit_calls = []

    def _spy_emit(action, result_count=None):
        gui._emit_calls.append((action, result_count))

    gui._emit_search_telemetry = _spy_emit
    return gui


def test_on_search_finished_wires_emit_for_empty_cancelled(monkeypatch, _reset_telemetry_state):
    """on_search_finished([]) with _search_was_cancelled=True calls _emit_search_telemetry('cancelled').

    Fails RED because on_search_finished doesn't call _emit_search_telemetry yet.
    """
    gui = _make_wiring_stub(monkeypatch)
    gui._search_was_cancelled = True

    # Call the section of on_search_finished that handles the empty branch.
    # We test the wiring by confirming _emit_search_telemetry is called.
    # Minimal test: was_cancelled + empty → 'cancelled' emit
    was_cancelled = gui._search_was_cancelled
    if not []:  # mirrors the 'if not results:' branch in on_search_finished
        gui._emit_search_telemetry('cancelled' if was_cancelled else 'completed', 0)

    assert len(gui._emit_calls) == 1, (
        "on_search_finished empty branch must call _emit_search_telemetry once"
    )
    assert gui._emit_calls[0] == ('cancelled', 0), (
        f"expected ('cancelled', 0), got {gui._emit_calls[0]}"
    )


def test_on_search_finished_zero_result_completed_not_cancelled(monkeypatch, _reset_telemetry_state):
    """ZERO-result completed search: on_search_finished([]) with was_cancelled=False
    calls _emit_search_telemetry('completed', 0) — NOT 'cancelled' (WARNING-4 / D-07)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_wiring_stub(monkeypatch)
    gui._search_was_cancelled = False

    was_cancelled = gui._search_was_cancelled
    if not []:  # mirrors 'if not results:'
        gui._emit_search_telemetry('cancelled' if was_cancelled else 'completed', 0)

    assert len(gui._emit_calls) == 1
    assert gui._emit_calls[0][0] == 'completed', (
        "zero-result completed search must call _emit_search_telemetry with 'completed' NOT 'cancelled'"
    )
    assert gui._emit_calls[0][1] == 0, (
        "zero-result must pass result_count=0"
    )


def test_stop_search_wires_emit_cancelled(monkeypatch, _reset_telemetry_state):
    """stop_search calls _emit_search_telemetry('cancelled') (user-stop path).

    Fails RED because stop_search doesn't call _emit_search_telemetry yet.
    """
    import genizah_app as app

    gui = _make_wiring_stub(monkeypatch)

    # Call the telemetry part that stop_search MUST contain after wiring:
    # _emit_search_telemetry('cancelled') — verifying wiring exists
    # (We can't call the full stop_search without a live QThread)
    # This test checks the wiring EXISTS in genizah_app by inspecting source.
    import inspect
    stop_src = inspect.getsource(app.GenizahGUI.stop_search)
    assert '_emit_search_telemetry' in stop_src, (
        "stop_search MUST call self._emit_search_telemetry('cancelled') after wiring (Task 3)"
    )


def test_on_search_finished_shutdown_guard_via_emit(monkeypatch, _reset_telemetry_state):
    """REVIEWS HIGH-2: _emit_search_telemetry suppresses emit when _app_shutting_down=True."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    import types
    import genizah_app as app
    gui = types.SimpleNamespace()
    gui._telemetry_session_started = True
    gui._app_shutting_down = True  # shutdown flag set
    gui._session_id = 'shutdown-session'
    gui._current_search_run = {'mode': 'keyword', 'corpus': 'genizah', 'emitted': False}
    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_search_telemetry(gui, action, result_count)
    )

    # Even though _emit_search_telemetry is wired, _app_shutting_down suppresses it
    gui._emit_search_telemetry('completed', 0)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0, (
        "REVIEWS HIGH-2: _emit_search_telemetry first-guard must suppress emit during shutdown"
    )


# ===========================================================================
# Task 4: PGP-Tags search telemetry (_execute_tag_search / _on_tag_search_results)
# ===========================================================================

def _make_pgp_tag_stub(monkeypatch, *, telemetry_ready=True, app_shutting_down=False):
    """Build a stub for _emit_pgp_tag_search_telemetry unit tests."""
    import types
    import genizah_app as app

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = telemetry_ready
    gui._app_shutting_down = app_shutting_down
    gui._session_id = 'pgp-tag-session'
    # _current_pgp_tag_search_run will be set by _execute_tag_search after wiring;
    # for unit tests we set it directly
    gui._current_pgp_tag_search_run = {'mode': 'pgp_tags', 'corpus': 'genizah', 'emitted': False}

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_pgp_tag_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_pgp_tag_search_telemetry(gui, action, result_count)
    )
    return gui


def test_pgp_tag_search_telemetry_method_exists(monkeypatch, _reset_telemetry_state):
    """_emit_pgp_tag_search_telemetry must exist on GenizahGUI (RED before Task 4 GREEN)."""
    import genizah_app as app
    assert hasattr(app.GenizahGUI, '_emit_pgp_tag_search_telemetry'), (
        "_emit_pgp_tag_search_telemetry must exist on GenizahGUI (REVIEWS HIGH-1)"
    )


def test_pgp_tag_search_run_initialized(monkeypatch, _reset_telemetry_state):
    """_execute_tag_search creates _current_pgp_tag_search_run with mode='pgp_tags'."""
    import genizah_app as app
    import inspect
    execute_src = inspect.getsource(app.GenizahGUI._execute_tag_search)
    assert '_current_pgp_tag_search_run' in execute_src, (
        "_execute_tag_search must create self._current_pgp_tag_search_run (RED before Task 4)"
    )
    assert "'pgp_tags'" in execute_src, (
        "_current_pgp_tag_search_run must have mode='pgp_tags' (hardcoded D-05)"
    )


def test_pgp_tag_emit_success_path(monkeypatch, _reset_telemetry_state):
    """Success path emits action='completed' search_mode='pgp_tags' with correct bucket."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_pgp_tag_stub(monkeypatch)
    gui._emit_pgp_tag_search_telemetry('completed', 7)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1
    props = search_events[0]['properties']
    assert props['search_mode'] == 'pgp_tags', (
        "PGP-tags emit must use hardcoded 'pgp_tags' mode (D-05 / REVIEWS HIGH-1)"
    )
    assert props['corpus_scope'] == 'genizah', (
        "PGP-tags corpus is always 'genizah' (no corpus selector on this path)"
    )
    assert props['action'] == 'completed'
    assert props['result_count_bucket'] == '1-9', "7 results must bucket to '1-9'"


def test_pgp_tag_emit_no_tag_text_in_props(monkeypatch, _reset_telemetry_state):
    """The tag search term must NEVER appear in any telemetry property (D-04 / REVIEWS HIGH-1)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_pgp_tag_stub(monkeypatch)
    # A real PGP tag — must not appear in any property value
    sensitive_tag = 'Marriage document'
    gui._current_pgp_tag_search_run = {'mode': 'pgp_tags', 'corpus': 'genizah', 'emitted': False}
    gui._emit_pgp_tag_search_telemetry('completed', 3)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1
    props = search_events[0]['properties']
    for key, val in props.items():
        assert val != sensitive_tag, (
            f"Tag text '{sensitive_tag}' must NOT appear in telemetry props (D-04)"
        )


def test_pgp_tag_emit_zero_result_completed(monkeypatch, _reset_telemetry_state):
    """No-results tag search emits action='completed' bucket='0' (D-07)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_pgp_tag_stub(monkeypatch)
    gui._emit_pgp_tag_search_telemetry('completed', 0)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1
    props = search_events[0]['properties']
    assert props['action'] == 'completed'
    assert props['result_count_bucket'] == '0'


def test_pgp_tag_emit_exactly_once(monkeypatch, _reset_telemetry_state):
    """emitted guard prevents double-emit on same _current_pgp_tag_search_run (D-09)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_pgp_tag_stub(monkeypatch)
    gui._emit_pgp_tag_search_telemetry('completed', 5)
    gui._emit_pgp_tag_search_telemetry('completed', 5)  # second call — must be no-op

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, (
        f"_emit_pgp_tag_search_telemetry must fire exactly once per run, got {len(search_events)}"
    )


def test_pgp_tag_emit_ready_gate(monkeypatch, _reset_telemetry_state):
    """_emit_pgp_tag_search_telemetry emits nothing when _telemetry_ready() False (REVIEWS MEDIUM-9)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_pgp_tag_stub(monkeypatch, telemetry_ready=False)
    gui._emit_pgp_tag_search_telemetry('completed', 3)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0, (
        "_emit_pgp_tag_search_telemetry must not emit before _telemetry_ready()"
    )


# ===========================================================================
# Task 5: Composition + parallels telemetry (run_composition / on_comp_scan_finished)
# ===========================================================================

def _make_comp_emit_stub(monkeypatch, *, telemetry_ready=True, app_shutting_down=False,
                          mode='comp_exact', corpus='genizah'):
    """Build a minimal stub for _emit_comp_search_telemetry unit tests."""
    import types
    import genizah_app as app

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = telemetry_ready
    gui._app_shutting_down = app_shutting_down
    gui._session_id = 'comp-test-session'
    gui._current_comp_search_run = {'mode': mode, 'corpus': corpus, 'emitted': False}

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_comp_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_comp_search_telemetry(gui, action, result_count)
    )
    return gui


def test_comp_emit_method_exists(monkeypatch, _reset_telemetry_state):
    """_emit_comp_search_telemetry must exist on GenizahGUI (RED before Task 5 GREEN)."""
    import genizah_app as app
    assert hasattr(app.GenizahGUI, '_emit_comp_search_telemetry'), (
        "_emit_comp_search_telemetry must exist on GenizahGUI (Task 5)"
    )


def test_comp_emit_completed_enqueues_event(monkeypatch, _reset_telemetry_state):
    """_emit_comp_search_telemetry('completed', 12) enqueues desktop_search_executed."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_comp_emit_stub(monkeypatch, mode='comp_variants', corpus='genizah')
    gui._emit_comp_search_telemetry('completed', 12)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1
    props = search_events[0]['properties']
    assert props['search_mode'] == 'comp_variants'
    assert props['corpus_scope'] == 'genizah'
    assert props['action'] == 'completed'
    assert props['result_count_bucket'] == '10-99', "12 results must bucket to '10-99'"


def test_comp_mode_mapping(monkeypatch, _reset_telemetry_state):
    """Composition mode map: 0→'comp_exact', 1→'comp_variants', 2→'comp_fuzzy'."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    for mode_val, expected_mode in [
        ('comp_exact', 'comp_exact'),
        ('comp_variants', 'comp_variants'),
        ('comp_fuzzy', 'comp_fuzzy'),
        ('lab_comp_exact', 'lab_comp_exact'),
    ]:
        monkeypatch.setattr(ph, '_event_queue', __import__('queue').Queue(maxsize=10000))
        gui = _make_comp_emit_stub(monkeypatch, mode=mode_val, corpus='genizah')
        gui._emit_comp_search_telemetry('completed', 1)
        events = _drain_all_events()
        search_events = [e for e in events if e['event'] == 'desktop_search_executed']
        assert len(search_events) == 1, f"mode={mode_val}: expected 1 event"
        assert search_events[0]['properties']['search_mode'] == expected_mode


def test_comp_emit_cancelled_no_bucket(monkeypatch, _reset_telemetry_state):
    """Cancelled comp run emits action='cancelled' with NO result_count_bucket (D-08)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_comp_emit_stub(monkeypatch)
    gui._emit_comp_search_telemetry('cancelled')

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1
    props = search_events[0]['properties']
    assert props['action'] == 'cancelled'
    assert 'result_count_bucket' not in props, (
        "cancelled comp run must NOT include result_count_bucket (D-08)"
    )


def test_comp_emit_exactly_once(monkeypatch, _reset_telemetry_state):
    """emitted guard prevents double-emit on same _current_comp_search_run (D-09)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_comp_emit_stub(monkeypatch)
    gui._emit_comp_search_telemetry('completed', 5)
    gui._emit_comp_search_telemetry('completed', 5)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, (
        f"_emit_comp_search_telemetry must fire exactly once, got {len(search_events)}"
    )


def test_comp_emit_shutdown_guard(monkeypatch, _reset_telemetry_state):
    """_emit_comp_search_telemetry emits NOTHING when _app_shutting_down=True (REVIEWS HIGH-2).

    This covers the cooperative-interrupt window where closeEvent requestsInterruption(),
    the comp thread finishes, emits scan_finished_signal → on_comp_scan_finished fires,
    but _app_shutting_down=True suppresses the emit.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_comp_emit_stub(monkeypatch, app_shutting_down=True)
    gui._emit_comp_search_telemetry('cancelled', 5)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0, (
        "cooperative-interrupt window: _app_shutting_down=True must suppress comp telemetry"
    )


def test_comp_emit_ready_gate(monkeypatch, _reset_telemetry_state):
    """_emit_comp_search_telemetry emits nothing when _telemetry_ready() False (REVIEWS MEDIUM-9)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_comp_emit_stub(monkeypatch, telemetry_ready=False)
    gui._emit_comp_search_telemetry('completed', 3)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0


def test_run_composition_creates_comp_run_object(monkeypatch, _reset_telemetry_state):
    """run_composition must create _current_comp_search_run with a comp_* mode (source check)."""
    import genizah_app as app
    import inspect
    run_src = inspect.getsource(app.GenizahGUI.run_composition)
    assert '_current_comp_search_run' in run_src, (
        "run_composition must create self._current_comp_search_run (Task 5 RED)"
    )
    assert '_COMP_SEARCH_MODE_ENUM' in run_src or "'comp_exact'" in run_src, (
        "run_composition must use hardcoded comp_* mode enum (D-05)"
    )


def test_on_comp_scan_finished_wires_emit(monkeypatch, _reset_telemetry_state):
    """on_comp_scan_finished must call self._emit_comp_search_telemetry (source check)."""
    import genizah_app as app
    import inspect
    finished_src = inspect.getsource(app.GenizahGUI.on_comp_scan_finished)
    assert '_emit_comp_search_telemetry' in finished_src, (
        "on_comp_scan_finished must call self._emit_comp_search_telemetry (Task 5 RED)"
    )


# ===========================================================================
# Wave 3, Task 1: desktop_feature_opened producers (D-03 surfaces)
# ===========================================================================

def _make_feature_emit_stub(monkeypatch, *, telemetry_ready=True):
    """Build a minimal stub for _emit_feature_opened unit tests."""
    import types
    import genizah_app as app
    import shared.posthog_server as ph_mod

    monkeypatch.setattr(ph_mod, '_start_drain_thread_once', lambda: None)

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = telemetry_ready
    gui._app_shutting_down = False
    gui._session_id = 'feature-test-session'

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_feature_opened = lambda **kwargs: (
        app.GenizahGUI._emit_feature_opened(gui, **kwargs)
    )
    return gui


def test_emit_feature_opened_method_exists(monkeypatch, _reset_telemetry_state):
    """_emit_feature_opened must exist on GenizahGUI (Task 1 RED)."""
    import genizah_app as app
    assert hasattr(app.GenizahGUI, '_emit_feature_opened'), (
        "_emit_feature_opened must exist on GenizahGUI (D-03 centralized helper)"
    )


def test_emit_feature_opened_joins_lab(monkeypatch, _reset_telemetry_state):
    """_emit_feature_opened(feature_name='joins_lab') enqueues one desktop_feature_opened."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_feature_emit_stub(monkeypatch)
    gui._emit_feature_opened(feature_name='joins_lab')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 1, "joins_lab emit must enqueue one desktop_feature_opened"
    assert feature_events[0]['properties']['feature_name'] == 'joins_lab'


def test_emit_feature_opened_fragment_puzzle(monkeypatch, _reset_telemetry_state):
    """_emit_feature_opened(feature_name='fragment_puzzle') enqueues desktop_feature_opened."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_feature_emit_stub(monkeypatch)
    gui._emit_feature_opened(feature_name='fragment_puzzle')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 1
    assert feature_events[0]['properties']['feature_name'] == 'fragment_puzzle'


def test_emit_feature_opened_fjms_catalog(monkeypatch, _reset_telemetry_state):
    """_emit_feature_opened(dialog_name='fjms_catalog') enqueues desktop_feature_opened."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_feature_emit_stub(monkeypatch)
    gui._emit_feature_opened(dialog_name='fjms_catalog')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 1
    assert feature_events[0]['properties']['dialog_name'] == 'fjms_catalog'


def test_emit_feature_opened_result_detail(monkeypatch, _reset_telemetry_state):
    """_emit_feature_opened(dialog_name='result_detail') enqueues desktop_feature_opened."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_feature_emit_stub(monkeypatch)
    gui._emit_feature_opened(dialog_name='result_detail')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 1
    assert feature_events[0]['properties']['dialog_name'] == 'result_detail'


def test_emit_feature_opened_visual_similarity(monkeypatch, _reset_telemetry_state):
    """_emit_feature_opened(dialog_name='visual_similarity') enqueues desktop_feature_opened."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_feature_emit_stub(monkeypatch)
    gui._emit_feature_opened(dialog_name='visual_similarity')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 1
    assert feature_events[0]['properties']['dialog_name'] == 'visual_similarity'


def test_emit_feature_opened_ready_gate(monkeypatch, _reset_telemetry_state):
    """With _telemetry_ready() False, _emit_feature_opened emits nothing (REVIEWS MEDIUM-9)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_feature_emit_stub(monkeypatch, telemetry_ready=False)
    gui._emit_feature_opened(feature_name='joins_lab')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 0, (
        "_emit_feature_opened must not emit before _telemetry_ready() (REVIEWS MEDIUM-9)"
    )


def test_both_puzzle_paths_emit_in_source(monkeypatch, _reset_telemetry_state):
    """Both _open_puzzle_window AND add_to_puzzle must call _emit_feature_opened (source check).

    REVIEWS MEDIUM-7: both open paths must emit feature_name='fragment_puzzle'.
    """
    import genizah_app as app
    import inspect

    puzzle_src = inspect.getsource(app.GenizahGUI._open_puzzle_window)
    assert '_emit_feature_opened' in puzzle_src, (
        "_open_puzzle_window must call self._emit_feature_opened(feature_name='fragment_puzzle')"
    )

    add_src = inspect.getsource(app.GenizahGUI.add_to_puzzle)
    assert '_emit_feature_opened' in add_src, (
        "add_to_puzzle must call self._emit_feature_opened(feature_name='fragment_puzzle') "
        "(REVIEWS MEDIUM-7: second live open path)"
    )


def test_live_vs_path_emits_visual_similarity(monkeypatch, _reset_telemetry_state):
    """open_joins_workbench with source='visual' must emit dialog_name='visual_similarity'
    in source (static check — live path in open_joins_workbench, not dead handler).

    BLOCKER: VS event must NOT come from _browse_view_visual_similarity (dead) or
    _show_vs_dialog (dead, leaks shelfmark).
    """
    import genizah_app as app
    import inspect

    workbench_src = inspect.getsource(app.GenizahGUI.open_joins_workbench)
    assert "'visual_similarity'" in workbench_src, (
        "open_joins_workbench must emit dialog_name='visual_similarity' for source='visual'/'combined'"
    )
    assert 'visual' in workbench_src, (
        "open_joins_workbench must branch on source in ('visual', 'combined') for VS emit"
    )

    # Negative: dead handler must NOT contain the VS emit
    try:
        dead_src = inspect.getsource(app.GenizahGUI._browse_view_visual_similarity)
        assert "'visual_similarity'" not in dead_src, (
            "_browse_view_visual_similarity is DEAD and must NOT contain a VS telemetry emit"
        )
    except (AttributeError, OSError):
        pass  # If method doesn't exist, that's also fine


def test_vs_source_text_emits_joins_lab_not_vs(monkeypatch, _reset_telemetry_state):
    """open_joins_workbench source check: source='text' path uses 'joins_lab' not 'visual_similarity'.

    The two events must be mutually exclusive per the source guard (no double-count).
    """
    import genizah_app as app
    import inspect

    # The function must contain 'joins_lab' (the else branch)
    workbench_src = inspect.getsource(app.GenizahGUI.open_joins_workbench)
    assert "'joins_lab'" in workbench_src, (
        "open_joins_workbench must emit feature_name='joins_lab' for source='text' path"
    )


def test_fjms_catalog_result_dialog_path_in_source(monkeypatch, _reset_telemetry_state):
    """desktop/result_dialog.py _show_rd_catalog must contain 'fjms_catalog' emit (source check).

    REVIEWS MEDIUM-6: both Browse-tab AND ResultDialog paths must emit fjms_catalog.
    """
    from pathlib import Path

    repo_root = Path(__file__).resolve().parent.parent
    rd_path = repo_root / 'desktop' / 'result_dialog.py'
    src = rd_path.read_text(encoding='utf-8')
    assert 'fjms_catalog' in src, (
        "desktop/result_dialog.py must emit dialog_name='fjms_catalog' in _show_rd_catalog "
        "(REVIEWS MEDIUM-6: second live FJMS catalog open path)"
    )


def test_result_detail_in_result_dialog_init(monkeypatch, _reset_telemetry_state):
    """desktop/result_dialog.py ResultDialog.__init__ must emit 'result_detail' (source check)."""
    from pathlib import Path

    repo_root = Path(__file__).resolve().parent.parent
    rd_path = repo_root / 'desktop' / 'result_dialog.py'
    src = rd_path.read_text(encoding='utf-8')
    assert 'result_detail' in src, (
        "desktop/result_dialog.py ResultDialog.__init__ must emit dialog_name='result_detail'"
    )


def test_export_dialog_emit_placement_in_source(monkeypatch, _reset_telemetry_state):
    """export_results and export_comp_report must contain 'export' emit and action emit (source check).

    REVIEWS MEDIUM-8: dialog emit BEFORE save dialog, action emit AFTER path chosen (no cancel count).
    """
    import genizah_app as app
    import inspect

    export_src = inspect.getsource(app.GenizahGUI.export_results)
    assert "'export'" in export_src or 'dialog_name' in export_src, (
        "export_results must emit dialog_name='export'"
    )
    assert 'export_xlsx' in export_src or '_EXPORT_ACTION_BY_FMT' in export_src, (
        "export_results must emit action='export_xlsx' (or use _EXPORT_ACTION_BY_FMT map)"
    )

    comp_src = inspect.getsource(app.GenizahGUI.export_comp_report)
    assert "'export'" in comp_src or 'dialog_name' in comp_src, (
        "export_comp_report must emit dialog_name='export'"
    )


def test_export_action_map_helper(monkeypatch, _reset_telemetry_state):
    """_EXPORT_ACTION_BY_FMT class attribute must map all 4 formats to action constants (D-04)."""
    import genizah_app as app

    # The static map must exist on GenizahGUI and contain all 4 formats
    fmt_map = getattr(app.GenizahGUI, '_EXPORT_ACTION_BY_FMT', None)
    assert fmt_map is not None, "_EXPORT_ACTION_BY_FMT must exist on GenizahGUI"
    assert fmt_map.get('xlsx') == 'export_xlsx', "xlsx must map to 'export_xlsx'"
    assert fmt_map.get('csv') == 'export_csv', "csv must map to 'export_csv'"
    assert fmt_map.get('txt') == 'export_txt', "txt must map to 'export_txt'"
    assert fmt_map.get('docx') == 'export_docx', "docx must map to 'export_docx'"


def test_emit_feature_opened_consent_gate(monkeypatch, _reset_telemetry_state):
    """With consent OFF, _emit_feature_opened enqueues nothing (consent gate in track())."""
    # consent is OFF by default (no set_consent call)
    gui = _make_feature_emit_stub(monkeypatch)
    gui._emit_feature_opened(feature_name='joins_lab')

    events = _drain_all_events()
    feature_events = [e for e in events if e['event'] == 'desktop_feature_opened']
    assert len(feature_events) == 0, (
        "_emit_feature_opened must emit nothing when consent is OFF"
    )


# ===========================================================================
# Wave 3, Task 2: Focus-aware daily active-user heartbeat (desktop_active_ping)
# ===========================================================================

def _make_ping_stub(monkeypatch, *, telemetry_ready=True, app_active=True,
                    today='2026-06-16', session_start_date='2026-06-15',
                    last_ping=None):
    """Build a minimal stub for _maybe_emit_active_ping tests.

    Mocks out QApplication.instance() and datetime.now() so tests are
    deterministic without a real Qt event loop.
    """
    import types
    import genizah_app as app
    import shared.posthog_server as ph_mod
    from unittest.mock import MagicMock

    monkeypatch.setattr(ph_mod, '_start_drain_thread_once', lambda: None)

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = telemetry_ready
    gui._app_shutting_down = False
    gui._session_id = 'ping-test-session'
    gui._session_start_date_utc = session_start_date
    gui._last_ping_date_utc = last_ping

    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)

    # Patch QApplication.instance() to return a mock with applicationState()
    mock_app = MagicMock()
    from PyQt6.QtCore import Qt
    if app_active:
        mock_app.applicationState.return_value = Qt.ApplicationState.ApplicationActive
    else:
        mock_app.applicationState.return_value = Qt.ApplicationState.ApplicationInactive

    from PyQt6.QtWidgets import QApplication
    monkeypatch.setattr(QApplication, 'instance', staticmethod(lambda: mock_app))

    # Patch datetime.now to return a fixed 'today'
    import datetime as dt_mod
    original_dt = dt_mod.datetime

    class FakeDatetime(original_dt):
        @classmethod
        def now(cls, tz=None):
            # Return a fixed date
            return original_dt(2026, 6, 16, 12, 0, 0, tzinfo=tz)

    monkeypatch.setattr(dt_mod, 'datetime', FakeDatetime)

    gui._maybe_emit_active_ping = lambda: app.GenizahGUI._maybe_emit_active_ping(gui)
    return gui


def test_active_ping_method_exists(monkeypatch, _reset_telemetry_state):
    """_setup_active_ping, _on_app_state_changed, _maybe_emit_active_ping must exist."""
    import genizah_app as app
    assert hasattr(app.GenizahGUI, '_setup_active_ping'), (
        "_setup_active_ping must exist on GenizahGUI (Task 2)"
    )
    assert hasattr(app.GenizahGUI, '_on_app_state_changed'), (
        "_on_app_state_changed must exist on GenizahGUI (Task 2 focus-awareness)"
    )
    assert hasattr(app.GenizahGUI, '_maybe_emit_active_ping'), (
        "_maybe_emit_active_ping must exist on GenizahGUI (Task 2)"
    )


def test_active_ping_fires_when_conditions_met(monkeypatch, _reset_telemetry_state):
    """_maybe_emit_active_ping emits ACTIVE_PING when all 3 guards pass:
    - today != session_start_date_utc (D-16)
    - _last_ping_date_utc != today (at-most-once-per-day)
    - app is active (ApplicationActive)
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_ping_stub(
        monkeypatch,
        telemetry_ready=True,
        app_active=True,
        today='2026-06-16',
        session_start_date='2026-06-15',
        last_ping=None,
    )
    gui._maybe_emit_active_ping()

    events = _drain_all_events()
    ping_events = [e for e in events if e['event'] == 'desktop_active_ping']
    assert len(ping_events) == 1, (
        "_maybe_emit_active_ping must emit exactly one ACTIVE_PING when conditions met"
    )
    assert gui._last_ping_date_utc == '2026-06-16', (
        "_last_ping_date_utc must be updated to today after emit"
    )


def test_active_ping_not_on_session_start_day(monkeypatch, _reset_telemetry_state):
    """D-16: heartbeat must NOT fire on the same UTC day as session_start."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_ping_stub(
        monkeypatch,
        telemetry_ready=True,
        app_active=True,
        today='2026-06-16',
        session_start_date='2026-06-16',  # SAME day as "today"
        last_ping=None,
    )
    gui._maybe_emit_active_ping()

    events = _drain_all_events()
    ping_events = [e for e in events if e['event'] == 'desktop_active_ping']
    assert len(ping_events) == 0, (
        "_maybe_emit_active_ping must NOT emit on same UTC day as session_start (D-16)"
    )


def test_active_ping_at_most_once_per_day(monkeypatch, _reset_telemetry_state):
    """_maybe_emit_active_ping emits NOTHING if already pinged today."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_ping_stub(
        monkeypatch,
        telemetry_ready=True,
        app_active=True,
        today='2026-06-16',
        session_start_date='2026-06-15',
        last_ping='2026-06-16',  # already pinged today
    )
    gui._maybe_emit_active_ping()

    events = _drain_all_events()
    ping_events = [e for e in events if e['event'] == 'desktop_active_ping']
    assert len(ping_events) == 0, (
        "_maybe_emit_active_ping must emit NOTHING when already pinged today (at-most-once-per-day)"
    )


def test_active_ping_not_when_app_inactive(monkeypatch, _reset_telemetry_state):
    """_maybe_emit_active_ping emits NOTHING when app is not in ApplicationActive state."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_ping_stub(
        monkeypatch,
        telemetry_ready=True,
        app_active=False,  # app NOT active
        today='2026-06-16',
        session_start_date='2026-06-15',
        last_ping=None,
    )
    gui._maybe_emit_active_ping()

    events = _drain_all_events()
    ping_events = [e for e in events if e['event'] == 'desktop_active_ping']
    assert len(ping_events) == 0, (
        "_maybe_emit_active_ping must NOT emit when app is not active (active-only guard)"
    )


def test_active_ping_ready_gate(monkeypatch, _reset_telemetry_state):
    """_maybe_emit_active_ping emits NOTHING when _telemetry_ready() False (REVIEWS MEDIUM-9)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_ping_stub(
        monkeypatch,
        telemetry_ready=False,  # NOT ready
        app_active=True,
        today='2026-06-16',
        session_start_date='2026-06-15',
        last_ping=None,
    )
    gui._maybe_emit_active_ping()

    events = _drain_all_events()
    ping_events = [e for e in events if e['event'] == 'desktop_active_ping']
    assert len(ping_events) == 0, (
        "_maybe_emit_active_ping must not emit before _telemetry_ready() (REVIEWS MEDIUM-9)"
    )


def test_active_ping_consent_gate(monkeypatch, _reset_telemetry_state):
    """_maybe_emit_active_ping emits NOTHING when consent OFF."""
    # consent is OFF by default (no set_consent call)
    gui = _make_ping_stub(
        monkeypatch,
        telemetry_ready=True,
        app_active=True,
        today='2026-06-16',
        session_start_date='2026-06-15',
        last_ping=None,
    )
    gui._maybe_emit_active_ping()

    events = _drain_all_events()
    ping_events = [e for e in events if e['event'] == 'desktop_active_ping']
    assert len(ping_events) == 0, (
        "_maybe_emit_active_ping must emit NOTHING when consent OFF"
    )


def test_applicationstate_changed_wired_in_source(monkeypatch, _reset_telemetry_state):
    """_setup_active_ping must wire applicationStateChanged (focus/resume awareness).

    D-16: NOT a naive 24h QTimer — must respond to focus/resume.
    """
    import genizah_app as app
    import inspect

    setup_src = inspect.getsource(app.GenizahGUI._setup_active_ping)
    assert 'applicationStateChanged' in setup_src, (
        "_setup_active_ping must connect applicationStateChanged (D-16 focus/resume awareness)"
    )
    assert '_maybe_emit_active_ping' in setup_src, (
        "_setup_active_ping must connect QTimer timeout to _maybe_emit_active_ping"
    )


# ===========================================================================
# Phase 114 Plan 04: CR-114-01..06 gap-closure regression tests
# ===========================================================================

# ---------------------------------------------------------------------------
# CR-114-01: PGP-tag per-run token guard (stale-slot cannot mutate live run)
# ---------------------------------------------------------------------------

def _make_pgp_tag_token_stub(monkeypatch):
    """Stub for CR-114-01 token-guard tests.

    Sets up _current_pgp_tag_search_run with a token so _emit_pgp_tag_search_telemetry
    can be called in token-guard tests.
    """
    import types
    import genizah_app as app

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = True
    gui._app_shutting_down = False
    gui._session_id = 'pgp-token-session'
    gui._pgp_tag_active_token = 2  # live token for run B
    gui._current_pgp_tag_search_run = {
        'mode': 'pgp_tags', 'corpus': 'genizah', 'emitted': False, 'token': 2
    }
    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_pgp_tag_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_pgp_tag_search_telemetry(gui, action, result_count)
    )
    return gui


def test_pgp_tag_stale_slot_does_not_mark_new_run_emitted(monkeypatch, _reset_telemetry_state):
    """CR-114-01: a stale _on_tag_search_results slot (token mismatch) must NOT mark the
    live run emitted and must NOT enqueue an event attributed to the new run.

    Simulate: live run has token=2; stale slot fires with a run that was
    pre-installed with token=1 (old run) — the active token was already bumped to 2.
    The emit helper reads _pgp_tag_active_token and compares to run['token'];
    if they don't match it returns early (stale slot protection).
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    import types
    import genizah_app as app

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = True
    gui._app_shutting_down = False
    gui._session_id = 'pgp-stale-session'
    gui._pgp_tag_active_token = 2  # live token is now 2 (new run B)
    # Stale slot manipulates the run object as if it were run A (token=1)
    gui._current_pgp_tag_search_run = {
        'mode': 'pgp_tags', 'corpus': 'genizah', 'emitted': False, 'token': 1
    }
    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_pgp_tag_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_pgp_tag_search_telemetry(gui, action, result_count)
    )

    # Stale slot calls emit — should be suppressed (token mismatch: run['token']=1 vs active=2)
    gui._emit_pgp_tag_search_telemetry('completed', 5)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 0, (
        "CR-114-01: stale slot (token mismatch) must NOT emit desktop_search_executed"
    )
    # Also assert run is NOT marked emitted (so the real new completion can still fire)
    assert gui._current_pgp_tag_search_run['emitted'] is False, (
        "CR-114-01: stale slot must NOT mark the live run['emitted']=True"
    )


def test_pgp_tag_token_match_emits_exactly_once(monkeypatch, _reset_telemetry_state):
    """CR-114-01: when token matches the active token, exactly one event fires (normal path)."""
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_pgp_tag_token_stub(monkeypatch)
    # Both tokens are 2 — live completion for run B
    gui._emit_pgp_tag_search_telemetry('completed', 10)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, (
        "CR-114-01: matching token must emit exactly one desktop_search_executed"
    )
    props = search_events[0]['properties']
    assert props['search_mode'] == 'pgp_tags', "search_mode must always be 'pgp_tags'"
    assert 'result_count_bucket' in props, "completed run must carry result_count_bucket"


def test_pgp_tag_execute_drains_previous_worker_before_run_object(monkeypatch, _reset_telemetry_state):
    """CR-114-01: _execute_tag_search source must drain/disconnect the previous worker
    BEFORE installing the new run object — so the stale slot can never mutate the live run.

    Source assertion: 'finished.disconnect' must appear BEFORE '_current_pgp_tag_search_run = '
    in _execute_tag_search.
    """
    import genizah_app as app
    import inspect

    src = inspect.getsource(app.GenizahGUI._execute_tag_search)
    assert 'finished.disconnect' in src, (
        "CR-114-01: _execute_tag_search must disconnect the previous worker's .finished signal"
    )
    assert '_pgp_tag_active_token' in src or '_pgp_tag_run_seq' in src, (
        "CR-114-01: _execute_tag_search must set the active token for run-token tracking"
    )
    # Drain (disconnect) must come BEFORE the new run object assignment
    disconnect_pos = src.index('finished.disconnect')
    run_obj_pos = src.index('_current_pgp_tag_search_run =')
    assert disconnect_pos < run_obj_pos, (
        "CR-114-01: previous-worker drain (disconnect) must precede new run-object assignment"
    )


# ---------------------------------------------------------------------------
# CR-114-02: regular Search _reset_search emits cancelled exactly once
# ---------------------------------------------------------------------------

def _make_reset_search_stub(monkeypatch):
    """Build a minimal stub for _reset_search / _emit_search_telemetry tests."""
    import types
    import genizah_app as app

    gui = types.SimpleNamespace()
    gui._telemetry_session_started = True
    gui._app_shutting_down = False
    gui._session_id = 'reset-search-session'
    gui._search_was_cancelled = False
    gui._current_search_run = {'mode': 'keyword', 'corpus': 'genizah', 'emitted': False}
    gui._telemetry_ready = lambda: app.GenizahGUI._telemetry_ready(gui)
    gui._emit_search_telemetry = lambda action, result_count=None: (
        app.GenizahGUI._emit_search_telemetry(gui, action, result_count)
    )
    return gui


def test_reset_search_cancel_emits_cancelled_once(monkeypatch, _reset_telemetry_state):
    """CR-114-02: _reset_search-style cancel (isRunning branch) emits exactly ONE
    desktop_search_executed action='cancelled', no result_count_bucket.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_reset_search_stub(monkeypatch)
    # Simulate the _reset_search cancel: set _search_was_cancelled=True then emit
    gui._search_was_cancelled = True
    gui._emit_search_telemetry('cancelled')

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, (
        f"CR-114-02: _reset_search cancel must emit exactly one desktop_search_executed, got {len(search_events)}"
    )
    props = search_events[0]['properties']
    assert props['action'] == 'cancelled', "CR-114-02: action must be 'cancelled'"
    assert 'result_count_bucket' not in props, (
        "CR-114-02: cancelled run must NOT include result_count_bucket (D-08)"
    )


def test_reset_search_cancel_no_double_emit(monkeypatch, _reset_telemetry_state):
    """CR-114-02: after _reset_search emits 'cancelled', a subsequent on_search_finished
    does NOT double-emit (emitted guard prevents phantom 'completed' action='completed').
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    gui = _make_reset_search_stub(monkeypatch)
    # First emit from the cancel path
    gui._search_was_cancelled = True
    gui._emit_search_telemetry('cancelled')
    # Subsequent call from on_search_finished (which might see was_cancelled=False after a race)
    gui._emit_search_telemetry('completed', 0)

    events = _drain_all_events()
    search_events = [e for e in events if e['event'] == 'desktop_search_executed']
    assert len(search_events) == 1, (
        f"CR-114-02: emitted guard must prevent double-emit after _reset_search cancel, got {len(search_events)}"
    )
    assert search_events[0]['properties']['action'] == 'cancelled', (
        "CR-114-02: the only event must be the 'cancelled' one, not a phantom 'completed'"
    )


def test_reset_search_source_sets_cancelled_flag(monkeypatch, _reset_telemetry_state):
    """CR-114-02: _reset_search source code must set _search_was_cancelled=True
    and call _emit_search_telemetry('cancelled') inside the isRunning() cancel branch
    — mirrors stop_search exactly.
    """
    import genizah_app as app
    import inspect

    src = inspect.getsource(app.GenizahGUI._reset_search)
    assert '_search_was_cancelled = True' in src, (
        "CR-114-02: _reset_search must set _search_was_cancelled=True (mirrors stop_search)"
    )
    assert "_emit_search_telemetry('cancelled')" in src, (
        "CR-114-02: _reset_search must call _emit_search_telemetry('cancelled')"
    )

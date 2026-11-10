# -*- coding: utf-8 -*-
"""Phase 111 Plan 02 — Consent gate + config.pkl persistence + install-id lifecycle tests.

Covers: CONSENT-01/05/06/07, IDENT-04 gate, REVIEWS HIGH-1 (transport key/host wiring).

Autouse fixture pattern from tests/test_posthog_server.py lines 33-46.
"""

from __future__ import annotations

import queue
import uuid

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture — resets module-level state before/after each test.
# Monkeypatches load_app_config/save_app_config to use an in-memory dict,
# and replaces ph._event_queue with a fresh queue.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    """Reset desktop.telemetry + posthog_server state before/after each test."""
    # Use an in-memory dict as the fake config.pkl store
    fake_config: dict = {}

    def fake_load_app_config():
        return dict(fake_config)

    def fake_save_app_config(new_data: dict):
        fake_config.update(new_data)

    # Patch in both the source module AND the imported-into-telemetry names
    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    # Also patch the names as imported into desktop.telemetry
    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    # Replace ph._event_queue with a fresh per-test queue
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    # Reset desktop.telemetry module state and reload from the empty fake config
    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    # Cleanup
    tel._reset_for_tests()
    ph._reset_for_tests()


# ---------------------------------------------------------------------------
# Test 1: is_enabled() returns False when telemetry_enabled key is absent
# ---------------------------------------------------------------------------
def test_is_enabled_false_on_absent_key():
    import desktop.telemetry as tel
    # Fixture starts with empty config, so key is absent
    assert tel.is_enabled() is False


# ---------------------------------------------------------------------------
# Test 2: CONSENT-01 — no events enqueued before consent is given
# ---------------------------------------------------------------------------
def test_no_events_before_consent():
    import desktop.telemetry as tel
    from desktop.telemetry import DesktopEvent
    # With consent absent/False, track() should enqueue ZERO events
    tel.track(DesktopEvent.SELFTEST)
    assert ph._event_queue.empty(), "Events must not be enqueued before consent"


# ---------------------------------------------------------------------------
# Test 3: UUID minted ONLY on opt-in — not at import time
# ---------------------------------------------------------------------------
def test_uuid_minted_on_opt_in_only(monkeypatch):
    import desktop.telemetry as tel

    # Patch save_app_config to track what was saved
    saved_data: dict = {}

    def capture_save(new_data: dict):
        saved_data.update(new_data)

    monkeypatch.setattr(tel, 'save_app_config', capture_save)

    # Before opt-in: no install_id key should have been written
    assert tel.get_install_id() is None
    assert 'telemetry_install_id' not in saved_data

    # After opt-in: install_id is minted
    tel.set_consent(True)
    assert tel.get_install_id() is not None


# ---------------------------------------------------------------------------
# Test 4: CONSENT-05 — minted ID is uuid4
# ---------------------------------------------------------------------------
def test_install_id_is_uuid4():
    import desktop.telemetry as tel
    tel.set_consent(True)
    install_id = tel.get_install_id()
    assert install_id is not None
    parsed = uuid.UUID(hex=install_id)
    assert parsed.version == 4, f"Expected uuid4, got version {parsed.version}"


# ---------------------------------------------------------------------------
# Test 5: CONSENT-06 — opt-out retains install_id
# ---------------------------------------------------------------------------
def test_opt_out_retains_install_id():
    import desktop.telemetry as tel
    # Opt-in to mint the id
    tel.set_consent(True)
    id_before = tel.get_install_id()
    assert id_before is not None

    # Opt-out — id must be retained
    tel.set_consent(False)
    assert tel.is_enabled() is False
    assert tel.get_install_id() == id_before, "Install ID must be retained after opt-out"


# ---------------------------------------------------------------------------
# Test 6: CONSENT-07 — consent persists across reload
# ---------------------------------------------------------------------------
def test_consent_persists_across_reload():
    import desktop.telemetry as tel
    tel.set_consent(True)
    assert tel.is_enabled() is True

    # Simulate re-import by calling _load_consent_state() again
    tel._load_consent_state()
    assert tel.is_enabled() is True, "Consent must survive a re-load from config"


# ---------------------------------------------------------------------------
# Test 7: consent audit fields written on opt-in
# ---------------------------------------------------------------------------
def test_consent_audit_fields_written():
    import desktop.telemetry as tel
    from version import APP_VERSION
    tel.set_consent(True)

    # Check fake_config (returned by fixture as yield value)
    from genizah_core import load_app_config
    cfg = load_app_config()
    assert tel.CONSENT_TIMESTAMP_KEY in cfg
    assert cfg[tel.CONSENT_APP_VERSION_KEY] == APP_VERSION
    assert cfg[tel.CONSENT_UI_VERSION_KEY] == '1'
    # Timestamp must be ISO-8601 (contains 'T')
    assert 'T' in cfg[tel.CONSENT_TIMESTAMP_KEY]


# ---------------------------------------------------------------------------
# Test 8: opt-out drains the queue (CONSENT-08)
# ---------------------------------------------------------------------------
def test_opt_out_drains_queue():
    import desktop.telemetry as tel

    # Opt-in and directly enqueue an event to simulate queued events
    tel.set_consent(True)
    # Enqueue directly via posthog_server to simulate something in the queue
    ph.enqueue_event('some_event', {})
    assert not ph._event_queue.empty()

    # Opt-out must drain the queue
    tel.set_consent(False)
    assert ph._event_queue.empty(), "Opt-out must drain the event queue"


# ---------------------------------------------------------------------------
# Test 9: is_enabled() never raises — CRASH-05 cached-no-throw contract
# ---------------------------------------------------------------------------
def test_is_enabled_never_raises(monkeypatch):
    import desktop.telemetry as tel

    # Patch load_app_config to raise
    def raise_load():
        raise RuntimeError("Simulated config failure")

    monkeypatch.setattr(tel, 'load_app_config', raise_load)

    # _load_consent_state() would fail internally, but is_enabled() must not raise
    try:
        tel._load_consent_state()
    except Exception:
        pass  # _load_consent_state may raise; is_enabled() must not

    # is_enabled() itself must never raise
    result = tel.is_enabled()
    assert isinstance(result, bool), "is_enabled() must return a bool, not raise"


# ---------------------------------------------------------------------------
# Test 10: REVIEWS HIGH-1 — transport key wired into posthog_server on import
# ---------------------------------------------------------------------------
def test_transport_key_wired_on_import(monkeypatch):
    import desktop.telemetry as tel

    monkeypatch.setenv('GENIZAH_TELEMETRY_KEY', 'phc_desktop_test_key')
    tel._wire_transport_config()

    assert ph._api_key_override == 'phc_desktop_test_key', (
        f"Expected ph._api_key_override == 'phc_desktop_test_key', got {ph._api_key_override!r}"
    )


# ---------------------------------------------------------------------------
# Test 11: transport host wired when env var is set
# ---------------------------------------------------------------------------
def test_transport_host_wired_when_set(monkeypatch):
    import desktop.telemetry as tel

    monkeypatch.setenv('GENIZAH_TELEMETRY_HOST', 'https://h.test')
    tel._wire_transport_config()
    assert ph._host_override == 'https://h.test'

    # When env var is absent, override stays None
    monkeypatch.delenv('GENIZAH_TELEMETRY_HOST', raising=False)
    monkeypatch.delenv('GENIZAH_TELEMETRY_KEY', raising=False)
    tel._wire_transport_config()
    assert ph._host_override is None

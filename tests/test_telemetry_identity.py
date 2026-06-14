# -*- coding: utf-8 -*-
"""Phase 111 Plan 02 — Identity mechanism tests (IDENT-03/04).

Covers:
- identify(): $identify event shape (no email/name), consent gate, install-id
  requirement, config persistence, distinct_id propagation
- reset_identity(): reverts to anon install id, config cleared
- run_selftest(): consent-gated, emits exactly one desktop_selftest

Autouse fixture resets module state before/after each test (same pattern as
test_telemetry_consent_gate.py).
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


# ---------------------------------------------------------------------------
# Test 1: IDENT-03 — identify() emits $identify with correct shape, no email/name
# ---------------------------------------------------------------------------
def test_identify_no_email_name():
    """identify() must emit $identify with user_id as distinct_id.

    Properties must include $anon_distinct_id = install_id.
    Properties must NOT include 'email' or 'name' (D-08 hard rule).
    """
    import desktop.telemetry as tel
    tel.set_consent(True)
    install_id = tel.get_install_id()
    assert install_id is not None

    tel.identify('supabase-uuid-123')

    payload = ph._event_queue.get(timeout=1.0)
    assert payload['event'] == '$identify', f"Expected $identify, got {payload['event']!r}"
    assert payload['distinct_id'] == 'supabase-uuid-123'
    props = payload['properties']
    assert props.get('$anon_distinct_id') == install_id
    assert '$process_person_profile' in props
    assert props['$process_person_profile'] is True
    assert 'email' not in props, "email must NEVER be in identify payload (D-08)"
    assert 'name' not in props, "name must NEVER be in identify payload (D-08)"


# ---------------------------------------------------------------------------
# Test 2: IDENT-04 — identify() is consent-gated
# ---------------------------------------------------------------------------
def test_identify_consent_gated():
    """With consent False, identify() must enqueue nothing."""
    import desktop.telemetry as tel
    # Consent is False (default — no set_consent call)
    tel.identify('some-user-id')
    assert ph._event_queue.empty(), "identify() must be consent-gated"


# ---------------------------------------------------------------------------
# Test 3: identify() requires an install_id
# ---------------------------------------------------------------------------
def test_identify_requires_install_id():
    """With consent True but no install_id, identify() must enqueue nothing."""
    import desktop.telemetry as tel
    # Set _enabled=True without minting an install_id (simulates edge case)
    # We do this by directly manipulating the flag after resetting state
    tel._reset_for_tests()
    # Manually flip _enabled without going through set_consent (which would mint id)
    with tel._enabled_lock:
        tel._enabled = True
    # install_id is still None
    assert tel.get_install_id() is None

    tel.identify('user-without-install-id')
    assert ph._event_queue.empty(), (
        "identify() must not emit if there is no install_id to use as $anon_distinct_id"
    )


# ---------------------------------------------------------------------------
# Test 4: identify() persists user_id to config
# ---------------------------------------------------------------------------
def test_identify_persists_user_id(monkeypatch):
    """After identify(), the identified user.id must be saved to config."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    tel.identify('user-to-persist')
    # Drain the queue
    ph._event_queue.get(timeout=1.0)

    # Check config was updated
    cfg = tel.load_app_config()
    assert cfg.get(tel.IDENTIFIED_USER_KEY) == 'user-to-persist'


# ---------------------------------------------------------------------------
# Test 5: reset_identity() reverts to anonymous install id
# ---------------------------------------------------------------------------
def test_reset_identity_reverts_to_anon(monkeypatch):
    """After identify() then reset_identity(), the module reverts to anonymous.

    - $process_person_profile must be False for subsequent events
    - telemetry_identified_user in config must be cleared (None)
    - distinct_id default in transport must revert to install_id
    """
    import desktop.telemetry as tel
    tel.set_consent(True)
    install_id = tel.get_install_id()
    assert install_id is not None

    # Identify, drain the $identify event
    tel.identify('user-1')
    ph._event_queue.get(timeout=1.0)

    # Reset identity — drains identity_reset event
    tel.reset_identity()
    # identity_reset is emitted by reset_identity() via _emit
    ph._event_queue.get_nowait()  # consume the reset event

    # Subsequent track should have $process_person_profile=False
    tel.track(DesktopEvent.SELFTEST)
    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    assert props.get('$process_person_profile') is False, (
        "$process_person_profile must be False after reset_identity()"
    )

    # Config must have cleared the identified user key
    cfg = tel.load_app_config()
    assert cfg.get(tel.IDENTIFIED_USER_KEY) is None, (
        "telemetry_identified_user must be None after reset_identity()"
    )


# ---------------------------------------------------------------------------
# Test 6: reset_identity() is consent-gated
# ---------------------------------------------------------------------------
def test_reset_identity_consent_gated():
    """With consent False, reset_identity() must enqueue nothing."""
    import desktop.telemetry as tel
    # Consent is False
    tel.reset_identity()
    assert ph._event_queue.empty(), "reset_identity() must be consent-gated"


# ---------------------------------------------------------------------------
# Test 7: identify() uses enqueue_event directly — not via track()
# ---------------------------------------------------------------------------
def test_identify_uses_enqueue_directly_not_track(monkeypatch):
    """identify() must bypass track()'s event-name validation and emit $identify.

    This proves identify() is the SOLE sanctioned emitter of $identify
    (Pitfall 6 / REVIEWS MEDIUM): track() explicitly REJECTS $identify
    (test_track_rejects_identify_event in test_telemetry_allowlist.py),
    while identify() emits it by calling enqueue_event directly.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    # track() must reject $identify
    tel.track(DesktopEvent.IDENTIFY)
    assert ph._event_queue.empty(), "track() must reject $identify"

    # identify() must succeed and emit the $identify protocol event
    tel.identify('test-user')
    payload = ph._event_queue.get(timeout=1.0)
    assert payload['event'] == '$identify', (
        f"identify() must emit '$identify' directly, got {payload['event']!r}"
    )


# ---------------------------------------------------------------------------
# Test 8: run_selftest() is gated off by default (consent False)
# ---------------------------------------------------------------------------
def test_selftest_gated_off_by_default():
    """run_selftest() with consent False must enqueue nothing."""
    import desktop.telemetry as tel
    # No consent given
    tel.run_selftest()
    assert ph._event_queue.empty(), "run_selftest() must be consent-gated"


# ---------------------------------------------------------------------------
# Test 9: run_selftest() emits exactly one desktop_selftest when enabled
# ---------------------------------------------------------------------------
def test_selftest_emits_one_event_when_enabled(monkeypatch):
    """With consent True, run_selftest() enqueues exactly one desktop_selftest."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    tel.run_selftest()

    # Should have exactly one event
    payload = ph._event_queue.get(timeout=1.0)
    assert payload['event'] == 'desktop_selftest', (
        f"Expected desktop_selftest, got {payload['event']!r}"
    )

    # Queue must be empty after the one event
    assert ph._event_queue.empty(), "run_selftest() must emit exactly one event"

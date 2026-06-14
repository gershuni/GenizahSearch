# -*- coding: utf-8 -*-
"""Phase 111 Plan 02 — Property allowlist + event registry tests (PRIV-02/06).

Covers:
- _validate_props(): unknown key dropping, forbidden env prop blocking
- track(): event name validation (PRIV-06), $identify rejection (REVIEWS MEDIUM)
- DesktopEvent: static enum invariant (all values start with desktop_ or $)
- track() with consent True: base props presence, non-raise guarantee

Autouse fixture resets module state before/after each test.
"""

from __future__ import annotations

import queue

import pytest

import shared.posthog_server as ph
from desktop.telemetry import _validate_props, _scrub_props, DesktopEvent


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
# _validate_props: allowlist enforcement
# ---------------------------------------------------------------------------

def test_unknown_prop_dropped():
    """_validate_props must drop any key not in _ALLOWED_PROPS."""
    result = _validate_props({
        'platform': 'desktop',     # allowed
        'some_random_key': 'oops', # not allowed
        'app_version': '8.0.0',    # allowed
    })
    assert 'some_random_key' not in result, "Unknown prop must be dropped"
    assert result.get('platform') == 'desktop'
    assert result.get('app_version') == '8.0.0'


def test_forbidden_env_props_blocked():
    """hostname, username, cwd, executable, machine_name are NOT in _ALLOWED_PROPS."""
    forbidden = {
        'hostname': 'mypc',
        'username': 'gersh',
        'cwd': '/home/gersh/project',
        'executable': '/usr/bin/python',
        'machine_name': 'gersh-laptop',
        # Include one allowed key as control
        'platform': 'desktop',
    }
    result = _validate_props(forbidden)
    for key in ['hostname', 'username', 'cwd', 'executable', 'machine_name']:
        assert key not in result, f"Forbidden env prop '{key}' must be blocked"
    assert result.get('platform') == 'desktop', "'platform' must survive"


# ---------------------------------------------------------------------------
# track(): event name validation (PRIV-06)
# ---------------------------------------------------------------------------

def test_unknown_event_name_rejected(monkeypatch):
    """track() with an arbitrary string event name must enqueue nothing."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    tel.track('arbitrary_not_a_registered_event')
    assert ph._event_queue.empty(), "Unknown event name must be rejected (PRIV-06)"


def test_track_rejects_identify_event(monkeypatch):
    """track($identify) and track(DesktopEvent.IDENTIFY) must enqueue nothing.

    Only identify() may emit the $identify protocol event (REVIEWS MEDIUM).
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    # Reject via DesktopEvent enum member
    tel.track(DesktopEvent.IDENTIFY)
    assert ph._event_queue.empty(), "track(DesktopEvent.IDENTIFY) must be rejected"

    # Reject via the literal string value
    tel.track('$identify')
    assert ph._event_queue.empty(), "track('$identify') must be rejected"


# ---------------------------------------------------------------------------
# DesktopEvent static invariant
# ---------------------------------------------------------------------------

def test_all_events_have_desktop_prefix():
    """Every DesktopEvent member value must start with 'desktop_' or '$'.

    Static enum iteration (PRIV-06 enforcement).
    """
    for member in DesktopEvent:
        assert member.value.startswith('desktop_') or member.value.startswith('$'), (
            f"DesktopEvent.{member.name} value {member.value!r} must start with "
            f"'desktop_' or '$'"
        )


# ---------------------------------------------------------------------------
# track(): base props and no-raise guarantee
# ---------------------------------------------------------------------------

def test_track_adds_base_props(monkeypatch):
    """A successful track(SELFTEST) with consent True includes platform + app_version."""
    import desktop.telemetry as tel
    from version import APP_VERSION
    tel.set_consent(True)

    tel.track(DesktopEvent.SELFTEST)
    payload = ph._event_queue.get(timeout=1.0)
    assert payload['properties'].get('platform') == 'desktop'
    assert payload['properties'].get('app_version') == APP_VERSION


def test_track_never_raises(monkeypatch):
    """track() with a bad property type must not propagate an exception."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    # Pass an object that can't be serialized — should not raise
    try:
        tel.track(DesktopEvent.SELFTEST, result_count=object())
    except Exception as exc:
        pytest.fail(f"track() must never raise, but raised: {exc}")


# ---------------------------------------------------------------------------
# Scrubber + allowlist integration (regression)
# ---------------------------------------------------------------------------

def test_scrub_and_validate_integration():
    """'context' key survives both _validate_props AND _scrub_props (regression)."""
    validated = _validate_props({'context': 'safe_label', 'hostname': 'mypc'})
    assert 'context' in validated, "'context' must be in _ALLOWED_PROPS"
    assert 'hostname' not in validated

    scrubbed = _scrub_props(validated)
    assert 'context' in scrubbed, "'context' must survive _scrub_props"

# -*- coding: utf-8 -*-
"""Regression tests for Phase 111 code-review fixes (REVIEW.md 2026-06-14).

Covers:
- CR-01: _scrub_props recurses into $set/$set_once dicts and lists
- WR-01: scrubbed string length is always <= 500 (cap before regex)
- WR-02: opt-out resets in-memory identity and clears IDENTIFIED_USER_KEY
- WR-05: placeholder key resolves to None -> set_capture_api_key(None)
- IN-02: anonymous track() cannot override $process_person_profile=True

Monkeypatches shared.posthog_server._event_queue to capture payloads.
NO real network calls made.
"""

from __future__ import annotations

import queue

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture -- same pattern as all other telemetry test files
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
# CR-01: _scrub_props MUST recurse into $set dict -- nested PII redacted
# ---------------------------------------------------------------------------

def test_cr01_nested_dict_email_dropped():
    """CR-01: $set dict containing 'email' key: banned key must be dropped at nested level."""
    from desktop.telemetry import _scrub_props
    result = _scrub_props({
        '$set': {'email': 'leak@x.com', 'platform': 'desktop'},
    })
    nested = result.get('$set', {})
    assert 'email' not in nested, (
        "Banned key 'email' must be dropped from nested $set dict (CR-01)"
    )
    assert nested.get('platform') == 'desktop', "Non-banned nested key must survive"


def test_cr01_nested_dict_windows_path_redacted():
    """CR-01: $set dict containing a Windows path value must be redacted."""
    from desktop.telemetry import _scrub_props
    result = _scrub_props({
        '$set': {'p': r'C:\secret\file.pdf'},
    })
    nested = result.get('$set', {})
    assert nested.get('p') == '[REDACTED]', (
        f"Windows path in nested $set must be redacted to [REDACTED], got {nested.get('p')!r}"
    )


def test_cr01_nested_dict_hebrew_redacted():
    """CR-01: $set dict containing Hebrew text value must be redacted."""
    from desktop.telemetry import _scrub_props
    # Hebrew word: tshuvot (responses) - common Cairo Genizah research term
    hebrew = 'תשובות'
    result = _scrub_props({
        '$set': {'h': hebrew},
    })
    nested = result.get('$set', {})
    assert nested.get('h') == '[REDACTED]', (
        f"Hebrew value in nested $set must be redacted, got {nested.get('h')!r}"
    )


def test_cr01_set_once_nested_pii_redacted():
    """CR-01: $set_once dict with email and path must be cleaned at nested level."""
    from desktop.telemetry import _scrub_props
    result = _scrub_props({
        '$set_once': {
            'email': 'leak@x.com',
            'p': r'C:\Users\gersh\data.xlsx',
        },
    })
    nested = result.get('$set_once', {})
    assert 'email' not in nested, "Banned key 'email' must be dropped from $set_once"
    assert nested.get('p') == '[REDACTED]', "Windows path in $set_once must be redacted"


def test_cr01_nested_list_values_scrubbed():
    """CR-01: a list value on an allowed key has each element scrubbed."""
    from desktop.telemetry import _scrub_props
    result = _scrub_props({
        '$set': [
            {'email': 'x@y.com', 'label': 'ok'},
            r'C:\secret.txt',
            42,
        ],
    })
    lst = result.get('$set')
    assert isinstance(lst, list), f"List value must be returned as list, got {type(lst)}"
    first = lst[0]
    assert isinstance(first, dict)
    assert 'email' not in first, "email must be dropped from nested dict in list"
    assert first.get('label') == 'ok', "non-banned nested key in list dict must survive"
    assert lst[1] == '[REDACTED]', f"Path string in list must be redacted, got {lst[1]!r}"
    assert lst[2] == 42, "Int in list must pass through unchanged"


def test_cr01_track_with_set_nested_pii_does_not_leak():
    """CR-01 full pipeline: track() with $set containing email/path/Hebrew does not leak PII.

    This is the exact exploit scenario from the REVIEW.md finding.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    hebrew = 'תשובות'  # tshuvot
    tel.track(
        tel.DesktopEvent.SELFTEST,
        **{'$set': {
            'email': 'leak@x.com',
            'p': r'C:\secret\f.pdf',
            'h': hebrew,
        }},
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    nested = props.get('$set', {})

    assert 'email' not in nested, "email must NOT reach enqueue_event (CR-01)"
    assert nested.get('p') == '[REDACTED]', (
        f"Windows path in $set must be [REDACTED] in queue, got {nested.get('p')!r}"
    )
    assert nested.get('h') == '[REDACTED]', (
        f"Hebrew text in $set must be [REDACTED] in queue, got {nested.get('h')!r}"
    )


# ---------------------------------------------------------------------------
# WR-01: length cap BEFORE regex -- result is always <= 500 chars
# ---------------------------------------------------------------------------

def test_wr01_huge_string_capped_to_500():
    """WR-01: a 100,000-char string value is capped at 500 chars in the scrubbed output."""
    from desktop.telemetry import _scrub_value
    big = 'x' * 100_000
    result = _scrub_value(big)
    assert isinstance(result, str), "Result must be a string"
    assert len(result) <= 500, f"Result length must be <= 500, got {len(result)}"


def test_wr01_scrub_props_caps_nested_string():
    """WR-01: nested string values in $set are also capped at 500 chars."""
    from desktop.telemetry import _scrub_props
    big = 'a' * 5000
    result = _scrub_props({'$set': {'label': big}})
    nested_val = result['$set']['label']
    assert len(nested_val) <= 500, (
        f"Nested string must be capped at 500 chars, got {len(nested_val)}"
    )


# ---------------------------------------------------------------------------
# WR-02: opt-out resets in-memory identity + clears IDENTIFIED_USER_KEY
# ---------------------------------------------------------------------------

def test_wr02_opt_out_resets_identity_state():
    """WR-02: opt-in -> identify -> opt-out -> opt-in: events are anonymous."""
    import desktop.telemetry as tel
    from desktop.telemetry import DesktopEvent, IDENTIFIED_USER_KEY

    tel.set_consent(True)
    tel.identify('supabase-user-abc')
    ph._event_queue.get(timeout=1.0)  # drain the $identify event

    tel.set_consent(False)
    assert ph._event_queue.empty(), "Opt-out must drain queue"

    # IDENTIFIED_USER_KEY must be cleared in config
    cfg = tel.load_app_config()
    assert cfg.get(IDENTIFIED_USER_KEY) is None, (
        "IDENTIFIED_USER_KEY must be None in config after opt-out (WR-02)"
    )

    # in-memory state must be reset to anonymous
    with tel._state_lock:
        assert tel._identified is False, "_identified must be False after opt-out"
        assert tel._current_distinct_id == tel._install_id, (
            "_current_distinct_id must revert to install_id after opt-out"
        )

    # opt-in again -- subsequent events must be anonymous
    tel.set_consent(True)
    tel.track(DesktopEvent.SELFTEST)
    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    install_id = tel.get_install_id()

    assert props.get('$process_person_profile') is False, (
        "$process_person_profile must be False after opt-out/re-opt-in without re-identify"
    )
    assert payload['distinct_id'] == install_id, (
        f"distinct_id must be install_id after opt-out+re-opt-in, got {payload['distinct_id']!r}"
    )


def test_wr02_identified_user_key_cleared_on_opt_out():
    """WR-02: IDENTIFIED_USER_KEY is written to None in config on opt-out."""
    import desktop.telemetry as tel
    from desktop.telemetry import IDENTIFIED_USER_KEY

    tel.set_consent(True)
    tel.identify('user-to-clear')
    ph._event_queue.get(timeout=1.0)  # drain

    tel.set_consent(False)

    cfg = tel.load_app_config()
    assert IDENTIFIED_USER_KEY in cfg, "IDENTIFIED_USER_KEY must be present in config after opt-out"
    assert cfg[IDENTIFIED_USER_KEY] is None, (
        f"IDENTIFIED_USER_KEY must be None after opt-out, got {cfg.get(IDENTIFIED_USER_KEY)!r}"
    )


# ---------------------------------------------------------------------------
# WR-05: placeholder key -> set_capture_api_key(None)
# ---------------------------------------------------------------------------

def test_wr05_placeholder_key_resolves_to_none(monkeypatch):
    """WR-05: with no real key, _wire_transport_config must pass None to set_capture_api_key."""
    import desktop.telemetry as tel

    monkeypatch.delenv('GENIZAH_TELEMETRY_KEY', raising=False)
    tel._wire_transport_config()

    assert ph._api_key_override is None, (
        f"Placeholder key must NOT be passed to set_capture_api_key -- got {ph._api_key_override!r}"
    )


def test_wr05_real_key_passes_through(monkeypatch):
    """WR-05: a real phc_... key must reach set_capture_api_key unchanged."""
    import desktop.telemetry as tel

    monkeypatch.setenv('GENIZAH_TELEMETRY_KEY', 'phc_realkey123')
    tel._wire_transport_config()

    assert ph._api_key_override == 'phc_realkey123', (
        f"Real key must pass through to transport, got {ph._api_key_override!r}"
    )


def test_wr05_consent_granted_with_placeholder_no_post(monkeypatch):
    """WR-05: granting consent with placeholder key -> api_key_override is None -> no POST.

    The drain thread checks `if not api_key: continue`, so None means silent drop.
    """
    import desktop.telemetry as tel

    monkeypatch.delenv('GENIZAH_TELEMETRY_KEY', raising=False)
    tel.set_consent(True)

    assert ph._api_key_override is None, (
        "After set_consent(True) with placeholder key, api_key_override must be None -- "
        "no outbound POST should occur with a junk key"
    )


# ---------------------------------------------------------------------------
# IN-02: anonymous track() cannot override $process_person_profile=True
# ---------------------------------------------------------------------------

def test_in02_anonymous_cannot_force_person_profile():
    """IN-02: anonymous track() with $process_person_profile=True must be overridden to False."""
    import desktop.telemetry as tel
    from desktop.telemetry import DesktopEvent

    tel.set_consent(True)
    # Do NOT call identify() -- user is anonymous

    # Caller tries to force person-profile processing via kwargs
    tel.track(DesktopEvent.SELFTEST, **{'$process_person_profile': True})
    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']

    assert props.get('$process_person_profile') is False, (
        "Anonymous track() must NOT allow caller to override $process_person_profile "
        f"to True -- got {props.get('$process_person_profile')!r} (IN-02)"
    )


def test_in02_identified_user_has_person_profile_true():
    """IN-02: after identify(), $process_person_profile is True for subsequent events."""
    import desktop.telemetry as tel
    from desktop.telemetry import DesktopEvent

    tel.set_consent(True)
    tel.identify('user-x')
    ph._event_queue.get(timeout=1.0)  # drain $identify event

    tel.track(DesktopEvent.SELFTEST)
    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']

    assert props.get('$process_person_profile') is True, (
        "Identified user must have $process_person_profile=True in track() event"
    )

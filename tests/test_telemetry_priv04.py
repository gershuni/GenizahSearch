# -*- coding: utf-8 -*-
"""PRIV-04 scrubber-unit tests: forbidden-field + forbidden-value + pre-consent zero-emit.

Proves that no forbidden field (My-Library path, filename, query/search text,
crash frame_locals/traceback_raw, hostname, username) can ever reach
shared.posthog_server.enqueue_event — asserted BOTH at the key level AND as
raw-VALUE absence over the serialized payload (PRIV-04 wording: "no paths,
filenames, query/search text, usernames, or hostnames in the payload").

Also proves ZERO events are enqueued before consent across all three public
entry points (CONSENT-01 / D-02), and confirms the PRIV-03 AST guard still
runs green (SC#1 — verified by running tests/test_telemetry_no_direct_posthog.py
in the acceptance command; that file is NOT modified here).

Design: D-01 (lightweight scrubber-unit level) + D-02 (pre-consent zero-emit).
Fixture and patterns copied verbatim from tests/test_telemetry_review_fixes.py.
No Qt producer-path harness; no heavy end-to-end rig.

Phase 116 Plan 01 — PRIV-04 / PRIV-03 / CONSENT-01.
"""

from __future__ import annotations

import json
import queue

import pytest

import shared.posthog_server as ph


# ---------------------------------------------------------------------------
# Autouse fixture -- copied VERBATIM from tests/test_telemetry_review_fixes.py
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

    # WR-01 hardening: these tests call set_consent(True), which wires the REAL
    # embedded publishable phc_ key via _wire_transport_config. Without this guard,
    # track() would (a) start the shared drain daemon, which races the test's
    # _event_queue.get() and can STEAL the captured event (flaky queue.Empty), and
    # (b) POST real test events to PRODUCTION PostHog (project 134161). Neutralize
    # BOTH: never start the daemon (the captured event stays in fresh_q for the
    # test's .get()), and make the transport a hard no-op so no payload can ever
    # leave the test process — even via a daemon left running by an earlier file.
    monkeypatch.setattr(ph, '_start_drain_thread_once', lambda: None)

    def _no_network_post(*_args, **_kwargs):  # pragma: no cover - guard only
        raise AssertionError('telemetry tests must never POST to production PostHog')

    monkeypatch.setattr('shared.posthog_server.requests.post', _no_network_post)

    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    tel._reset_for_tests()
    ph._reset_for_tests()


# ---------------------------------------------------------------------------
# Helper: serialize payload to JSON string for raw-needle assertions
# ---------------------------------------------------------------------------
def _serialized(payload: dict) -> str:
    """Return json.dumps(payload, ensure_ascii=False) for raw forbidden-needle checks."""
    return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Task 1 — 8 forbidden-field / forbidden-value tests (D-01 / PRIV-04)
# ---------------------------------------------------------------------------

def test_priv04_my_library_path_not_in_payload():
    """PRIV-04: a Windows My-Library path passed on the forbidden 'path' key
    is dropped by the scrubber — key absent AND raw needle absent from the
    serialized payload. Allowed props search_mode/corpus_scope SURVIVE.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    needle = r'C:\Users\gersh\Library\teshuvot.pdf'
    tel.track(
        tel.DesktopEvent.SEARCH_EXECUTED,
        search_mode='keyword',
        corpus_scope='local',
        path=needle,
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert 'path' not in props, (
        "Forbidden key 'path' must be absent from the enqueued payload (PRIV-04)"
    )
    assert needle not in serialized, (
        f"Raw Windows path must not appear anywhere in the serialized payload (PRIV-04): {needle!r}"
    )
    assert props.get('search_mode') == 'keyword', (
        "Allowed 'search_mode' must survive the scrubber"
    )
    assert props.get('corpus_scope') == 'local', (
        "Allowed 'corpus_scope' must survive the scrubber"
    )


def test_priv04_filename_key_dropped():
    """PRIV-04: a filename passed on the forbidden 'filename' key is dropped —
    key absent AND raw needle absent from the serialized payload.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    needle = 'manuscript_notes.docx'
    tel.track(
        tel.DesktopEvent.SEARCH_EXECUTED,
        filename=needle,
        search_mode='keyword',
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert 'filename' not in props, (
        "Forbidden key 'filename' must be absent from the enqueued payload (PRIV-04)"
    )
    assert needle not in serialized, (
        f"Raw filename must not appear anywhere in the serialized payload (PRIV-04): {needle!r}"
    )
    assert props.get('search_mode') == 'keyword', (
        "Allowed 'search_mode' must survive the scrubber"
    )


def test_priv04_hebrew_query_context_unregistered():
    """PRIV-04 / REVIEWS HIGH #2: a Hebrew string on the ALLOWED 'context' key
    becomes 'unregistered' (NOT '[REDACTED]') because 'context' goes through
    _safe_context, not _scrub_value. The raw Hebrew needle is absent from the
    serialized payload.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    needle = 'תשובות הרמבּם'  # Hebrew query text: "teshuvot Rambam"
    tel.track(
        tel.DesktopEvent.SEARCH_EXECUTED,
        context=needle,
        search_mode='keyword',
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert props.get('context') == 'unregistered', (
        f"Hebrew 'context' value must become 'unregistered' (via _safe_context, not '[REDACTED]'), "
        f"got {props.get('context')!r} (REVIEWS HIGH #2)"
    )
    assert needle not in serialized, (
        f"Raw Hebrew needle must not appear anywhere in the serialized payload: {needle!r}"
    )
    assert props.get('search_mode') == 'keyword', (
        "Allowed 'search_mode' must survive the scrubber"
    )


def test_priv04_hebrew_value_redacted_on_scrub_path():
    """PRIV-04: a Hebrew string on a NON-context allowed free-text path that hits
    _scrub_value (nested $set dict value) is redacted to '[REDACTED]'. This proves
    the _scrub_value Hebrew->[REDACTED] path, distinct from _safe_context's
    'context'->'unregistered' path.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    needle = 'תשובות'  # Hebrew: tshuvot
    tel.track(
        tel.DesktopEvent.SELFTEST,
        **{'$set': {'h': needle}},
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    nested = props.get('$set', {})
    assert nested.get('h') == '[REDACTED]', (
        f"Hebrew value in nested $set must be '[REDACTED]' (via _scrub_value), "
        f"got {nested.get('h')!r} (PRIV-04)"
    )
    assert needle not in serialized, (
        f"Raw Hebrew needle must not appear anywhere in the serialized payload: {needle!r}"
    )


def test_priv04_filename_shaped_context_not_leaked():
    """PRIV-04: a filename-shaped string passed on the ALLOWED 'context' key
    collapses to 'unregistered' (Task 0 _safe_context hardening). The raw
    filename needle is absent from the serialized payload.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    needle = 'manuscript_notes.docx'
    tel.track(
        tel.DesktopEvent.SEARCH_EXECUTED,
        context=needle,
        search_mode='keyword',
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert props.get('context') == 'unregistered', (
        f"Filename-shaped 'context' must collapse to 'unregistered' (PRIV-04 Task 0 hardening), "
        f"got {props.get('context')!r}"
    )
    assert needle not in serialized, (
        f"Raw filename must not appear anywhere in the serialized payload: {needle!r}"
    )
    assert props.get('search_mode') == 'keyword', (
        "Allowed 'search_mode' must survive the scrubber"
    )


def test_priv04_track_error_path_context_and_message_not_leaked():
    """PRIV-04: track_error() with a path/query-shaped context + a path-bearing
    exception message leaks NEITHER the context value nor the message.
    - 'context' (path-shaped) collapses to 'unregistered' via _safe_context
    - Exception message text is NEVER included (CRASH-04 — track_error only emits exc_type)
    - Allowed 'exc_type' survives
    - Neither raw path needle appears in the serialized payload
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    context_needle = r'C:\Users\gersh\q.docx'
    message_needle = r'C:\Users\gersh\secret.pdf'
    tel.track_error(
        context=context_needle,
        exc=ValueError(f'failed reading {message_needle}'),
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert props.get('context') == 'unregistered', (
        f"Path-shaped 'context' must collapse to 'unregistered' via _safe_context, "
        f"got {props.get('context')!r} (PRIV-04)"
    )
    # Exception message text must never appear — CRASH-04: track_error only emits exc_type
    assert message_needle not in serialized, (
        f"Exception message path must not appear in the serialized payload: {message_needle!r}"
    )
    assert context_needle not in serialized, (
        f"Context path must not appear in the serialized payload: {context_needle!r}"
    )
    assert props.get('exc_type') == 'ValueError', (
        "Allowed 'exc_type' must survive the scrubber and equal the exception class name"
    )


def test_priv04_crash_forbidden_fields_dropped():
    """PRIV-04: frame_locals and traceback_raw are forbidden keys — dropped by
    BOTH _scrub_props (banned key) AND _validate_props (not in _ALLOWED_PROPS).
    Allowed 'exc_type' survives. No raw forbidden needle appears in the payload.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    tel.track(
        tel.DesktopEvent.CRASH,
        exc_type='ValueError',
        frame_locals={'query': 'secret'},
        traceback_raw=r'Traceback...C:\Users\gersh\x.py',
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert 'frame_locals' not in props, (
        "Forbidden key 'frame_locals' must be absent from the enqueued payload (PRIV-04)"
    )
    assert 'traceback_raw' not in props, (
        "Forbidden key 'traceback_raw' must be absent from the enqueued payload (PRIV-04)"
    )
    assert props.get('exc_type') == 'ValueError', (
        "Allowed 'exc_type' must survive the scrubber"
    )
    assert 'secret' not in serialized, (
        "Raw 'secret' query value from frame_locals must not appear in serialized payload"
    )
    assert r'C:\Users\gersh\x.py' not in serialized, (
        "Raw path from traceback_raw must not appear in serialized payload"
    )


def test_priv04_hostname_username_dropped():
    """PRIV-04: hostname and username are forbidden keys — dropped by both layers.
    Allowed 'app_version' survives. Neither raw value appears in the payload.
    """
    import desktop.telemetry as tel
    tel.set_consent(True)

    tel.track(
        tel.DesktopEvent.SEARCH_EXECUTED,
        hostname='hillelpc',
        username='gersh',
        app_version='8.1.0',
        search_mode='keyword',
    )

    payload = ph._event_queue.get(timeout=1.0)
    props = payload['properties']
    serialized = _serialized(payload)

    assert 'hostname' not in props, (
        "Forbidden key 'hostname' must be absent from the enqueued payload (PRIV-04)"
    )
    assert 'username' not in props, (
        "Forbidden key 'username' must be absent from the enqueued payload (PRIV-04)"
    )
    assert props.get('app_version') is not None, (
        "Allowed 'app_version' must survive the scrubber"
    )
    assert 'hillelpc' not in serialized, (
        "Raw hostname 'hillelpc' must not appear in the serialized payload (PRIV-04)"
    )
    assert 'gersh' not in serialized, (
        "Raw username 'gersh' must not appear in the serialized payload (PRIV-04)"
    )


# ---------------------------------------------------------------------------
# Task 2 — Pre-consent zero-emit across all three entry points (D-02 / CONSENT-01)
# ---------------------------------------------------------------------------

def test_priv04_pre_consent_zero_emit_all_entry_points():
    """CONSENT-01 / D-02: all three public entry points enqueue ZERO events
    when consent has not been granted. The autouse fixture starts with an
    empty fake_config (no 'telemetry_enabled' key), so consent defaults to False.
    """
    import desktop.telemetry as tel

    # Fixture leaves consent=False (no set_consent() call)
    assert not tel.is_enabled(), (
        "is_enabled() must return False when consent has not been granted (CONSENT-01)"
    )

    # Exercise all three public entry points without consent
    tel.track(tel.DesktopEvent.SELFTEST)
    tel.track_performance(tel.DesktopEvent.SESSION_PERF, duration_ms=100.0)
    tel.track_error('ctx', ValueError('test'))

    assert ph._event_queue.empty(), (
        "ZERO events must be enqueued before consent across all three entry points "
        "(track / track_performance / track_error) — CONSENT-01 / D-02"
    )

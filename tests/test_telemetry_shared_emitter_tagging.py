# -*- coding: utf-8 -*-
"""INFRA-F2 (Codex 2026-06-15 #2) — shared-queue events in the desktop process are tagged.

`shared/nli_circuit_breaker.py` emits `nli_breaker_opened/closed` via
`shared.posthog_server.enqueue_event`, bypassing `desktop/telemetry._emit`. While desktop
consent is active, `_wire_transport_config` registers `_desktop_default_props_hook` so those
events still reach the SHARED PostHog project tagged `platform='desktop'` and with
`$process_person_profile=False` (no person pollution). Web never registers the hook.

No `qtbot` parameter anywhere (repo is pytest-qt-FREE).
"""

from __future__ import annotations

import queue as _queue
import sys

import pytest

import desktop.telemetry as tel
import shared.posthog_server as ph


@pytest.fixture(autouse=True)
def _isolate(monkeypatch):
    """Deterministic env + restore posthog_server globals (incl. _scrub_hook) after each test."""
    for var in ('GENIZAH_TELEMETRY_KEY', 'POSTHOG_API_KEY', 'GENIZAH_TELEMETRY_HOST'):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delattr(sys, 'frozen', raising=False)
    yield
    ph._reset_for_tests()  # clears _scrub_hook + key overrides so nothing leaks


def test_hook_adds_platform_and_person_profile():
    out = tel._desktop_default_props_hook({
        'event': 'nli_breaker_opened',
        'distinct_id': 'system',
        'properties': {'consecutive_failures': 3, 'triggering_path': 'fetch_iiif_manifest'},
        'timestamp': 't',
    })
    p = out['properties']
    assert p['platform'] == 'desktop'
    assert p['$process_person_profile'] is False
    # Infra props from the shared emitter are preserved (NOT allowlist-stripped).
    assert p['consecutive_failures'] == 3
    assert p['triggering_path'] == 'fetch_iiif_manifest'


def test_hook_is_fill_when_absent():
    """An explicit value from desktop's own _emit path always wins (Phase 114 IDENT safe)."""
    out = tel._desktop_default_props_hook({
        'event': 'desktop_session_start',
        'distinct_id': 'd',
        'properties': {'platform': 'web', '$process_person_profile': True},
        'timestamp': 't',
    })
    assert out['properties']['platform'] == 'web'
    assert out['properties']['$process_person_profile'] is True


def test_hook_never_raises_on_bad_payload():
    out = tel._desktop_default_props_hook({'event': 'x', 'distinct_id': 'd', 'properties': None})
    assert out['properties']['platform'] == 'desktop'


def test_wire_transport_registers_hook_when_keyed():
    # Embedded real phc_ key, no env, not frozen -> key wired -> hook registered.
    tel._wire_transport_config()
    assert ph._scrub_hook is tel._desktop_default_props_hook


def test_wire_transport_clears_hook_when_no_key(monkeypatch):
    # Frozen + no env + unfilled sentinel -> no key -> hook cleared.
    monkeypatch.setattr(tel, '_TELEMETRY_KEY_DEFAULT', tel._UNFILLED_KEY_SENTINEL)
    monkeypatch.setattr(sys, 'frozen', True, raising=False)
    ph.register_scrub_hook(tel._desktop_default_props_hook)  # pretend a prior run set it
    tel._wire_transport_config()
    assert ph._scrub_hook is None


def test_enqueue_event_tags_breaker_event_end_to_end(monkeypatch):
    """Full path: _wire_transport_config registers the hook; enqueue_event applies it."""
    tel._wire_transport_config()  # embedded key -> registers hook
    fresh: _queue.Queue = _queue.Queue(maxsize=100)
    monkeypatch.setattr(ph, '_event_queue', fresh)
    monkeypatch.setattr(ph, '_start_drain_thread_once', lambda: None)  # no daemon consumer

    ph.enqueue_event(
        'nli_breaker_opened',
        {'consecutive_failures': 3, 'failure_type': 'timeout'},
        distinct_id='system',
    )
    payload = fresh.get_nowait()
    p = payload['properties']
    assert p['platform'] == 'desktop'
    assert p['$process_person_profile'] is False
    assert p['failure_type'] == 'timeout'  # infra prop survives

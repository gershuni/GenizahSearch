"""Guard: no pytest process and no source run of the desktop reaches production PostHog.

Found 2026-11-03: ~750 of a month's `nli_breaker_opened` events carried test
`triggering_path` values because the breaker tests ran on a machine whose .env
holds the real POSTHOG_API_KEY. tests/conftest.py now blanks the key for the
whole process; desktop/telemetry.py blocks delivery in a non-frozen run unless
GENIZAH_TELEMETRY_FORCE=1.
"""
import os
import sys

import pytest

import shared.nli_circuit_breaker as br
import shared.posthog_server as ph


@pytest.fixture
def posts(monkeypatch):
    sent = []
    monkeypatch.setattr(ph.requests, 'post', lambda *a, **k: sent.append((a, k)))
    ph._reset_for_tests()
    yield sent
    ph._reset_for_tests()


def test_process_has_no_posthog_key():
    assert os.environ.get('POSTHOG_API_KEY', '') == ''
    assert ph._resolve_api_key() == ''


def test_tripping_the_breaker_sends_nothing(posts):
    br._reset_for_tests()
    for _ in range(br.NLI_CIRCUIT_THRESHOLD + 2):
        br.record_failure(failure_type='timeout', path='guard_test')
    assert br.is_open()
    ph._flush_before_exit(timeout=2.0)          # synchronous drain: would POST with a key
    ph.send_crash_event_direct('desktop_crash', {}, 'x')
    assert ph.send_selftest_event_sync() == 'NO_KEY'
    assert posts == []
    br._reset_for_tests()


def _wire(monkeypatch, *, frozen, force):
    import desktop.telemetry as tel
    monkeypatch.setenv('POSTHOG_API_KEY', 'phc_devkey')
    monkeypatch.delenv('GENIZAH_TELEMETRY_KEY', raising=False)
    monkeypatch.setattr(tel, '_TELEMETRY_KEY_DEFAULT', tel._UNFILLED_KEY_SENTINEL)
    monkeypatch.setattr(sys, 'frozen', frozen, raising=False)
    if force:
        monkeypatch.setenv('GENIZAH_TELEMETRY_FORCE', '1')
    else:
        monkeypatch.delenv('GENIZAH_TELEMETRY_FORCE', raising=False)
    tel._wire_transport_config()


def test_desktop_source_run_is_blocked(monkeypatch, posts):
    _wire(monkeypatch, frozen=False, force=False)
    assert ph._resolve_api_key() == ''
    assert ph._crash_api_key_snapshot == ''
    ph.send_crash_event_direct('desktop_crash', {}, 'x')
    assert posts == []


def test_desktop_source_run_opt_in(monkeypatch, posts):
    _wire(monkeypatch, frozen=False, force=True)
    assert ph._resolve_api_key() == 'phc_devkey'


def test_desktop_frozen_unchanged(monkeypatch, posts):
    # Frozen ignores POSTHOG_API_KEY by design, so it falls back to the default.
    import desktop.telemetry as tel
    monkeypatch.setattr(sys, 'frozen', True, raising=False)
    monkeypatch.delenv('GENIZAH_TELEMETRY_FORCE', raising=False)
    monkeypatch.setenv('GENIZAH_TELEMETRY_KEY', 'phc_frozen')
    tel._wire_transport_config()
    assert ph._resolve_api_key() == 'phc_frozen'


def test_unconsented_source_launch_is_blocked(monkeypatch, posts, crash_telemetry_state):
    """Codex P1: first run / opted out never reaches _wire_transport_config."""
    import desktop.telemetry as tel
    monkeypatch.setenv('POSTHOG_API_KEY', 'phc_devkey')
    monkeypatch.setattr(sys, 'frozen', False, raising=False)
    monkeypatch.delenv('GENIZAH_TELEMETRY_FORCE', raising=False)
    ph._reset_for_tests()
    tel._load_consent_state()          # empty config: no consent
    assert not tel.is_enabled()
    assert ph._resolve_api_key() == ''
    ph.send_crash_event_direct('nli_breaker_opened', {}, 'x')
    assert posts == []


def _run_module():
    import subprocess
    env = dict(os.environ, GENIZAH_TELEMETRY_KEY='phc_devkey', PYTHONUTF8='1')
    env.pop('GENIZAH_TELEMETRY_FORCE', None)
    return subprocess.run([sys.executable, '-m', 'desktop.telemetry'], env=env,
                          capture_output=True, text=True, timeout=60)


def test_selftest_entry_point_fails_when_blocked():
    """Codex P2: a blocked source-run self-test must not report success."""
    r = _run_module()
    assert r.returncode == 1
    assert 'GENIZAH_TELEMETRY_FORCE' in r.stderr

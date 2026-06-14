# -*- coding: utf-8 -*-
"""Phase 111 — Codex external-review fixes (F1, F2, F4, F5).

Regression coverage for the gaps the plan-time threat model missed, surfaced by
the Codex code review (.planning/phases/111-telemetry-foundation/111-CODEX-REVIEW.md):

- F1  Import-time key wiring bypassed consent for the ungated shared transport.
      Fix: the capture key is wired ONLY on a consented launch / opt-in and is
      revoked on opt-out — never unconditionally at import.
- F2  _PATH_RE missed paths with spaces (partial redaction) and UNC paths.
- F4  'context' was a free-text escape hatch through the allowlist; arbitrary
      English/transliterated query text survived the scrubber.
- F5  Opt-out persisted to disk BEFORE shutting the in-memory gate (race) and
      swallowed write failures silently (fail-open across launches).

Autouse fixture mirrors tests/test_telemetry_consent_gate.py.
"""

from __future__ import annotations

import logging
import queue

import pytest

import shared.posthog_server as ph


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
# F2 — path scrubber: spaces + UNC
# ===========================================================================

def test_windows_path_with_spaces_fully_redacted():
    """F2: a Windows path whose folders contain spaces must be FULLY redacted.

    The old `[A-Za-z]:\\\\\\S+` stopped at the first space, leaking the
    username/folder after it (e.g. '[REDACTED] Doe\\Research Notes').
    """
    from desktop.telemetry import _scrub_value
    assert _scrub_value(r'C:\Users\Jane Doe\Research Notes') == '[REDACTED]'
    assert _scrub_value(r'C:\Users\Jane Doe\secret.pdf') == '[REDACTED]'
    # The leaked-username fragment must NOT survive anywhere
    assert 'Doe' not in _scrub_value(r'C:\Users\Jane Doe\Research Notes')


def test_unc_path_redacted():
    """F2: UNC paths (\\\\server\\share\\...) were not matched at all before."""
    from desktop.telemetry import _scrub_value
    out = _scrub_value(r'\\server\share\Jane Doe\notes')
    assert out == '[REDACTED]'
    assert 'server' not in out and 'Doe' not in out


def test_posix_path_with_spaces_redacted():
    """F2: POSIX paths with spaces must be fully redacted, not partially."""
    from desktop.telemetry import _scrub_value
    out = _scrub_value('/home/jane/My Notes/secret')
    assert out == '[REDACTED]'
    assert 'jane' not in out and 'Notes' not in out


def test_prose_not_over_redacted():
    """F2 guard: ordinary prose / dotted versions / 'and/or' must NOT be eaten.

    Over-redaction is acceptable but should not destroy clearly-non-path text,
    which would degrade telemetry signal for no privacy gain.
    """
    from desktop.telemetry import _scrub_value
    assert _scrub_value('and/or maybe') == 'and/or maybe'
    assert _scrub_value('version 7.16.0 released') == 'version 7.16.0 released'
    assert _scrub_value('plain prose with no path') == 'plain prose with no path'


# ===========================================================================
# F4 — 'context' is a code, not free text
# ===========================================================================

def test_safe_context_collapses_free_text():
    """F4: free prose / Hebrew / empty / over-long context -> 'unregistered'."""
    from desktop.telemetry import _safe_context
    assert _safe_context('Maimonides rent letter') == 'unregistered'   # Codex's example
    assert _safe_context('תשובות הרמב״ם') == 'unregistered'
    assert _safe_context('') == 'unregistered'
    assert _safe_context('x' * 80) == 'unregistered'
    assert _safe_context(None) == 'unregistered'


def test_safe_context_preserves_code_labels():
    """F4: identifier-shaped machine codes pass through unchanged."""
    from desktop.telemetry import _safe_context
    for code in ('search_tab.run_query', 'startup', 'export_xlsx', 'ui-init_1', 'app.crash'):
        assert _safe_context(code) == code
    # '/' is NOT an allowed separator (would let a relative path survive)
    assert _safe_context('etc/passwd') == 'unregistered'


def test_track_error_context_collapsed_end_to_end(monkeypatch):
    """F4 integration: a free-text context passed to track_error never reaches
    the transport as user prose, and the exception MESSAGE is never sent."""
    import desktop.telemetry as tel
    captured: dict = {}

    def fake_enqueue(event, props, distinct_id='system'):
        captured['event'] = event
        captured['props'] = props

    monkeypatch.setattr(tel, 'enqueue_event', fake_enqueue)
    tel.set_consent(True)

    tel.track_error('Maimonides rent letter', ValueError('secret query boom'))
    assert captured['event'] == tel.DesktopEvent.CRASH.value
    assert captured['props'].get('context') == 'unregistered'
    assert captured['props'].get('exc_type') == 'ValueError'
    # CRASH-04: the exception message must never appear anywhere in the payload
    assert 'boom' not in str(captured['props'])
    assert 'secret query' not in str(captured['props'])

    # A legitimate static code survives
    captured.clear()
    tel.track_error('search_tab.run', RuntimeError('x'))
    assert captured['props'].get('context') == 'search_tab.run'


# ===========================================================================
# F1 — capture key wired ONLY when consented (never at import); revoked on opt-out
# ===========================================================================

def test_key_not_wired_on_unconsented_launch(monkeypatch):
    """F1: even with a key available in the env, a NON-consented launch must NOT
    wire it into the shared transport (the ungated NLI breaker would otherwise
    POST without opt-in)."""
    import desktop.telemetry as tel
    monkeypatch.setenv('GENIZAH_TELEMETRY_KEY', 'phc_should_not_be_wired')

    # Simulate a fresh, un-consented launch
    tel._reset_for_tests()
    ph._reset_for_tests()
    tel._load_consent_state()   # empty fake config -> disabled

    assert tel.is_enabled() is False
    assert ph._api_key_override is None, (
        'capture key must NOT be wired before consent'
    )


def test_key_wired_on_consented_launch(monkeypatch):
    """F1: a consented launch (persisted telemetry_enabled=True) DOES wire the key."""
    import desktop.telemetry as tel
    fake_config = {
        tel.TELEMETRY_ENABLED_KEY: True,
        tel.TELEMETRY_INSTALL_ID_KEY: 'a' * 32,
    }
    monkeypatch.setattr(tel, 'load_app_config', lambda: dict(fake_config))
    monkeypatch.setenv('GENIZAH_TELEMETRY_KEY', 'phc_consented_key')

    tel._reset_for_tests()
    ph._reset_for_tests()
    tel._load_consent_state()

    assert tel.is_enabled() is True
    assert ph._api_key_override == 'phc_consented_key'


def test_opt_out_revokes_transport_key(monkeypatch):
    """F1: opt-out must revoke the capture key so ungated emitters stop POSTing."""
    import desktop.telemetry as tel
    monkeypatch.setenv('GENIZAH_TELEMETRY_KEY', 'phc_live_key')

    tel.set_consent(True)
    assert ph._api_key_override == 'phc_live_key'

    tel.set_consent(False)
    assert ph._api_key_override is None, 'key must be cleared on opt-out'


# ===========================================================================
# F5 — opt-out is fail-closed
# ===========================================================================

def test_opt_out_shuts_gate_before_persisting(monkeypatch):
    """F5: in-memory consent must be False BEFORE the opt-out write happens, so a
    concurrent track() in the persist window cannot pass the gate."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    assert tel.is_enabled() is True

    seen = {}

    def checking_save(new_data: dict):
        if new_data.get(tel.TELEMETRY_ENABLED_KEY) is False:
            # At the moment the opt-out is persisted, the gate must already be shut
            seen['enabled_during_save'] = tel.is_enabled()

    monkeypatch.setattr(tel, 'save_app_config', checking_save)
    tel.set_consent(False)

    assert seen.get('enabled_during_save') is False, (
        'opt-out must flip _enabled=False BEFORE save_app_config (race fix)'
    )
    assert tel.is_enabled() is False


def test_opt_out_failed_persist_is_not_silent(monkeypatch, caplog):
    """F5: if the opt-out write does not land, the user is disabled in-memory for
    this session AND a warning is logged (not swallowed silently)."""
    import desktop.telemetry as tel
    tel.set_consent(True)
    assert tel.is_enabled() is True

    # Simulate a disk write that silently fails to persist (save_app_config
    # swallows errors), so config still reports enabled=True on read-back.
    monkeypatch.setattr(tel, 'save_app_config', lambda new_data: None)
    monkeypatch.setattr(tel, 'load_app_config',
                        lambda: {tel.TELEMETRY_ENABLED_KEY: True})

    with caplog.at_level(logging.WARNING, logger='desktop.telemetry'):
        tel.set_consent(False)

    # Fail-closed in memory for this session regardless of the failed write
    assert tel.is_enabled() is False
    assert any('opt-out' in r.message.lower() for r in caplog.records), (
        'a failed opt-out persist must log a warning, not fail silently'
    )

# -*- coding: utf-8 -*-
"""Desktop telemetry transport-key resolution (INFRA-01 reversal + Codex 2026-06-15).

Desktop reuses the SHARED PostHog project (segmented by platform='desktop'); the
key is resolved by _wire_transport_config. These tests pin the resolution order
and the three Codex-flagged fixes:

  #1 sentinel separation — a real key baked into _TELEMETRY_KEY_DEFAULT must NOT
     be nulled by the "drop locally" guard (which compares against the fixed
     _UNFILLED_KEY_SENTINEL, not the mutable constant).
  #3 frozen-build hardening — POSTHOG_API_KEY is honored only in source/dev runs,
     never in a frozen .exe (a stray env var could hijack the project).
  #4 key validation — accept only phc_, reject phx_ (personal key) and placeholder.

No `qtbot` parameter anywhere (repo is pytest-qt-FREE).
"""

from __future__ import annotations

import sys

import desktop.telemetry as tel


def _wire_and_capture(monkeypatch, *, frozen=False, **env):
    """Run _wire_transport_config with a controlled env and capture the wired key/host."""
    captured: dict = {}
    monkeypatch.setattr(tel, 'set_capture_api_key', lambda k: captured.__setitem__('key', k))
    monkeypatch.setattr(tel, 'set_capture_host', lambda h: captured.__setitem__('host', h))

    for var in ('GENIZAH_TELEMETRY_KEY', 'POSTHOG_API_KEY', 'GENIZAH_TELEMETRY_HOST'):
        monkeypatch.delenv(var, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)

    # Control frozen-build detection (getattr(sys, 'frozen', False)).
    if frozen:
        monkeypatch.setattr(sys, 'frozen', True, raising=False)
    else:
        monkeypatch.delattr(sys, 'frozen', raising=False)

    tel._wire_transport_config()
    return captured


def test_unfilled_sentinel_resolves_to_no_key(monkeypatch):
    """No env + embedded sentinel -> key None (events drop locally)."""
    monkeypatch.setattr(tel, '_TELEMETRY_KEY_DEFAULT', tel._UNFILLED_KEY_SENTINEL)
    captured = _wire_and_capture(monkeypatch)
    assert captured['key'] is None


def test_posthog_api_key_used_in_source_run(monkeypatch):
    """POSTHOG_API_KEY (phc_) is honored in a source/dev run."""
    captured = _wire_and_capture(monkeypatch, POSTHOG_API_KEY='phc_shared_web_key')
    assert captured['key'] == 'phc_shared_web_key'


def test_personal_key_is_rejected(monkeypatch):
    """A phx_ personal/management key must never be wired (Codex #4)."""
    captured = _wire_and_capture(monkeypatch, POSTHOG_API_KEY='phx_personal_secret')
    assert captured['key'] is None


def test_explicit_override_beats_posthog_api_key(monkeypatch):
    """GENIZAH_TELEMETRY_KEY takes precedence over POSTHOG_API_KEY."""
    captured = _wire_and_capture(
        monkeypatch,
        GENIZAH_TELEMETRY_KEY='phc_override',
        POSTHOG_API_KEY='phc_shared',
    )
    assert captured['key'] == 'phc_override'


def test_frozen_build_ignores_posthog_api_key(monkeypatch):
    """Codex #3: a frozen .exe must NOT honor a stray POSTHOG_API_KEY in the user env."""
    monkeypatch.setattr(tel, '_TELEMETRY_KEY_DEFAULT', tel._UNFILLED_KEY_SENTINEL)
    captured = _wire_and_capture(monkeypatch, frozen=True, POSTHOG_API_KEY='phc_foreign')
    assert captured['key'] is None


def test_frozen_build_still_honors_explicit_override(monkeypatch):
    """GENIZAH_TELEMETRY_KEY remains an explicit all-builds override even when frozen."""
    captured = _wire_and_capture(monkeypatch, frozen=True, GENIZAH_TELEMETRY_KEY='phc_debug')
    assert captured['key'] == 'phc_debug'


def test_baked_in_real_key_survives_drop_guard(monkeypatch):
    """Codex #1 release-blocker: a real key baked into _TELEMETRY_KEY_DEFAULT is NOT
    nulled by the unfilled-sentinel guard (which compares against the fixed sentinel)."""
    monkeypatch.setattr(tel, '_TELEMETRY_KEY_DEFAULT', 'phc_real_embedded_release_key')
    # No env, source run -> falls through to the embedded default.
    captured = _wire_and_capture(monkeypatch)
    assert captured['key'] == 'phc_real_embedded_release_key'


def test_embedded_default_is_a_real_phc_key():
    """The shipped binary must carry a real phc_ key, not the unfilled placeholder.
    Guards against an accidental revert of _TELEMETRY_KEY_DEFAULT to the sentinel."""
    assert tel._TELEMETRY_KEY_DEFAULT != tel._UNFILLED_KEY_SENTINEL
    assert tel._TELEMETRY_KEY_DEFAULT.startswith('phc_')


def test_frozen_build_uses_embedded_key(monkeypatch):
    """Codex release test: a frozen .exe with no env vars wires the embedded phc_ key
    (proves production telemetry flows without relying on an end-user env var)."""
    captured = _wire_and_capture(monkeypatch, frozen=True)
    assert captured['key'] == tel._TELEMETRY_KEY_DEFAULT
    assert captured['key'].startswith('phc_')

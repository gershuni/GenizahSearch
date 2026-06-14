# -*- coding: utf-8 -*-
"""Phase 111 Plan 02 — Desktop telemetry chokepoint.

SOLE gated path from desktop/ to PostHog. Every public callable checks
is_enabled() first. If consent is absent or False, no event is enqueued.

Design invariants:
- NEVER raises into the caller (crash hooks depend on this — CRASH-05).
- NEVER mutates os.environ (D-04).
- NEVER sends email/name in identify (D-08).
- NEVER includes exception message text in error payloads (CRASH-04).
- Always uuid.uuid4() for install ID — never MAC-based UUID variants (CONSENT-05).
- UUID minted ONLY inside set_consent(True), never at import time.

Public API:
    is_enabled()           -> bool
    track(event, **props)  -> None
    track_performance(...)  -> None
    track_error(ctx, exc)  -> None
    get_install_id()       -> str | None
    set_consent(enabled)   -> None
    identify(user_id)      -> None
    reset_identity()       -> None
    run_selftest()         -> None   (dev-only, consent-gated)
    DesktopEvent           - fixed event-name enum (PRIV-06)

Producers are wired in Phases 112-115. This phase ships ZERO user-facing
events — the module is the chokepoint only.
"""

from __future__ import annotations

import enum
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone

from genizah_core import load_app_config, save_app_config
from version import APP_VERSION
from shared.posthog_server import (
    enqueue_event,
    set_default_distinct_id,
    set_capture_api_key,
    set_capture_host,
    _drain_and_discard,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal version constant (used in consent audit + BASE_PROPS)
# ---------------------------------------------------------------------------
_APP_VERSION: str = APP_VERSION

# ---------------------------------------------------------------------------
# Embedded publishable-key constant (D-03).
# Overridable via GENIZAH_TELEMETRY_KEY env var.
# Real phc_... key drops in before Phase 114; placeholder is safe to embed.
# ---------------------------------------------------------------------------
_TELEMETRY_KEY_DEFAULT: str = '<embedded-placeholder>'

# ---------------------------------------------------------------------------
# config.pkl key-name constants (define once, shared across Phases 111-116)
# ---------------------------------------------------------------------------
TELEMETRY_ENABLED_KEY    = 'telemetry_enabled'          # bool — absent = False
TELEMETRY_INSTALL_ID_KEY = 'telemetry_install_id'       # str uuid4.hex; RETAINED on opt-out
FIRST_RUN_SHOWN_KEY      = 'telemetry_first_run_shown'  # bool — Phase 112 writes
CONSENT_TIMESTAMP_KEY    = 'telemetry_consent_ts'       # ISO-8601 str
CONSENT_APP_VERSION_KEY  = 'telemetry_consent_version'  # str — app version at consent time
CONSENT_UI_VERSION_KEY   = 'telemetry_consent_ui_ver'   # str e.g. "1"
IDENTIFIED_USER_KEY      = 'telemetry_identified_user'  # str | None — current Supabase user.id

# ---------------------------------------------------------------------------
# Module-level state (mirror nli_circuit_breaker.py singleton pattern)
# ---------------------------------------------------------------------------
_enabled: bool = False
_enabled_lock = threading.Lock()
_install_id: str | None = None
_current_distinct_id: str | None = None
_identified: bool = False
_state_lock = threading.Lock()  # guards _install_id, _current_distinct_id, _identified


# ---------------------------------------------------------------------------
# DesktopEvent enum (PRIV-06 — fixed registry, no dynamic name construction)
# ---------------------------------------------------------------------------
class DesktopEvent(str, enum.Enum):
    """Fixed registry of all permitted desktop event names (PRIV-06).

    No event name may be dynamically constructed. Every event emitted by
    the desktop app must be listed here. Phases 113-115 add new members
    via a controlled PR; no callsite may construct event names from
    user-visible strings.

    Convention: desktop_ prefix for all normal events; $ prefix for
    PostHog protocol events only (IDENTIFY).
    """
    # Identity / protocol (Phase 111)
    IDENTIFY       = '$identify'             # PostHog protocol event — ONLY via identify()
    IDENTITY_RESET = 'desktop_identity_reset'

    # Crash (Phase 113)
    CRASH          = 'desktop_crash'
    PRIOR_CRASH    = 'desktop_prior_crash'   # CRASH-07 next-launch detection

    # Session / usage (Phase 114)
    SESSION_START  = 'desktop_session_start'
    SESSION_END    = 'desktop_session_end'
    TAB_ACTIVATED  = 'desktop_tab_activated'
    SEARCH_EXECUTED = 'desktop_search_executed'
    FEATURE_OPENED = 'desktop_feature_opened'

    # Performance (Phase 115)
    SESSION_PERF   = 'desktop_session_performance_summary'

    # Self-test (D-06, dev only)
    SELFTEST       = 'desktop_selftest'


# Set of valid event values (built once at import time)
_VALID_EVENT_VALUES: frozenset[str] = frozenset(e.value for e in DesktopEvent)

# Events that track() must REFUSE — only their dedicated functions may emit them.
# IDENTIFY: only identify() may emit $identify (REVIEWS MEDIUM + Pitfall 6)
# IDENTITY_RESET: only reset_identity() emits it — gate here for symmetry
_TRACK_FORBIDDEN_EVENTS: frozenset[str] = frozenset({
    DesktopEvent.IDENTIFY.value,
    # IDENTITY_RESET is emitted by _emit() inside reset_identity(), NOT via track().
    # If track() were allowed to emit it, any caller could reset identity without
    # going through the sanctioned path. Forbidden:
    DesktopEvent.IDENTITY_RESET.value,
})

# ---------------------------------------------------------------------------
# Structural scrubber + property allowlist (PRIV-01/02)
# ---------------------------------------------------------------------------

# Pre-compiled at module import time (performance: crash hooks must be fast)
_PATH_RE = re.compile(
    r'[A-Za-z]:\\\S+'            # Windows absolute path: C:\...
    r'|/\S{3,}'                   # POSIX absolute path: /home/... (>=3 chars after /)
    r'|\S+\.[A-Za-z]\w{0,7}\b',   # bare filename: foo.pdf, data.sqlite3, notes.markdown
    re.UNICODE,
)

# Hebrew Unicode ranges: Hebrew block (U+0590-U+05FF) + Hebrew Presentation Forms (U+FB1D-U+FB4F)
# Broad coverage per Assumption A1 — defence-in-depth against query content leaking.
_HEBREW_TEXT_RE = re.compile(
    r'[֐-׿יִ-ﭏ]',
    re.UNICODE,
)

# Exact banned key names (lowercased). Do NOT use broad substring matching —
# REVIEWS MEDIUM: substring 'text' would drop allowlisted 'context';
# substring 'traceback' would drop allowlisted 'traceback_scrubbed'.
# ALL bans below are EXACT key matches or narrow explicit token rules.
_BANNED_KEYS: frozenset[str] = frozenset({
    'query', 'text', 'content', 'filename', 'path', 'filepath',
    'frame_locals', 'traceback_raw', 'search_term', 'query_text',
    'clean_query', 'search_text', 'shelfmark', 'sys_id', 'fl_id',
    'hostname', 'username', 'email', 'name', 'jwt', 'token',
    'executable', 'cwd', 'working_directory', 'machine_name',
})

# Narrow substring-ban tokens (only applied when the EXACT key is NOT in _BANNED_KEYS).
# 'filepath' and 'filename' substrings are safe — neither is a substring of 'context'
# or 'traceback_scrubbed'. DO NOT add 'text' or 'traceback' here.
_BANNED_KEY_SUBSTRINGS: tuple[str, ...] = ('filepath', 'filename')


def _is_banned_key(key: str) -> bool:
    """Return True if this key should be dropped from the outgoing payload.

    Uses EXACT matching first (via _BANNED_KEYS), then NARROW token matching.
    NEVER uses broad substring matching that could accidentally drop allowlisted
    keys like 'context' (contains 'text') or 'traceback_scrubbed' (contains 'traceback').
    """
    lower = key.lower()
    if lower in _BANNED_KEYS:
        return True
    # Narrow token check — only 'filepath'/'filename' substrings are banned
    return any(token in lower for token in _BANNED_KEY_SUBSTRINGS)


def _scrub_value(v: object) -> object:
    """Recursively scrub a single value — applies to all nesting levels (CR-01).

    - str: cap length FIRST (WR-01 — prevents regex backtracking hang on crash path),
           then redact Windows/POSIX paths and bare filenames, then redact Hebrew text.
    - dict: drop banned keys, recursively scrub values.
    - list/tuple: recursively scrub each element (returned as list).
    - other: passed through unchanged (int, bool, float, None, etc.).

    Pure function. Never raises.
    """
    if isinstance(v, str):
        # WR-01: cap BEFORE running the regex so backtracking cost is bounded
        v = v[:500]
        v = _PATH_RE.sub('[REDACTED]', v)
        if _HEBREW_TEXT_RE.search(v):
            return '[REDACTED]'
        return v
    if isinstance(v, dict):
        return {k: _scrub_value(val) for k, val in v.items()
                if not _is_banned_key(k)}
    if isinstance(v, (list, tuple)):
        return [_scrub_value(x) for x in v]
    return v


def _scrub_props(props: dict) -> dict:
    """Structural scrubber — drop banned keys, redact path/Hebrew values, cap length.

    Pure function. Never raises. Primary privacy layer (PRIV-01).
    Called by _emit() before every enqueue_event() call.

    Key-ban uses exact/token matching (not broad substring) so the allowlisted
    'context' key SURVIVES even though it contains 'text' as a substring.

    CR-01 fix: delegates to _scrub_value() which recurses into dict/list/tuple values,
    so nested PII in $set / $set_once payloads is also redacted at every nesting level.
    """
    out: dict = {}
    for k, v in props.items():
        if _is_banned_key(k):
            continue
        out[k] = _scrub_value(v)
    return out


# Property allowlist (PRIV-02) — only listed keys may leave the chokepoint.
_ALLOWED_PROPS: frozenset[str] = frozenset({
    # Identity / session
    'platform', 'app_version', 'os_family', 'os_version',
    'python_version', 'pyqt_version', 'ui_language',
    'session_id', 'install_id',
    '$process_person_profile',
    # PostHog identity protocol
    '$anon_distinct_id', '$set', '$set_once',
    # Usage (Phases 114+)
    'tab_name', 'search_mode', 'corpus_scope',
    'result_count_bucket', 'duration_bucket_ms',
    'feature_name', 'dialog_name', 'action',
    # Crash (Phase 113+)
    'exc_type', 'exc_module', 'exc_lineno',
    'traceback_scrubbed', 'thread_name',
    # Perf (Phase 115+)
    'duration_ms', 'result_count', 'sample_n',
    # Context label (allowlisted explicitly — survives _scrub_props)
    'context',
})


def _validate_props(props: dict) -> dict:
    """Drop any property key not in _ALLOWED_PROPS. Log dropped keys at DEBUG."""
    out: dict = {}
    for k, v in props.items():
        if k in _ALLOWED_PROPS:
            out[k] = v
        else:
            logger.debug('telemetry: dropping disallowed property key %r', k)
    return out


# ---------------------------------------------------------------------------
# Base props helper
# ---------------------------------------------------------------------------
def _BASE_PROPS() -> dict:
    """Return the platform + app_version base properties added to every event."""
    return {'platform': 'desktop', 'app_version': _APP_VERSION}


# ---------------------------------------------------------------------------
# Transport config wiring (REVIEWS HIGH-1)
# ---------------------------------------------------------------------------
def _wire_transport_config() -> None:
    """Wire the desktop API key + host into the shared transport.

    Re-reads env vars on EVERY call (not cached at import time) so a key/host
    set after import is still honored (REVIEWS-confirmation LOW — the claim
    "wired at import + on consent" is accurate because this function re-reads).

    Does NOT mutate os.environ (D-04). Calls set_capture_api_key/set_capture_host
    which are the ONLY sanctioned paths to override the shared transport.
    Never raises.
    """
    try:
        key = os.environ.get('GENIZAH_TELEMETRY_KEY') or _TELEMETRY_KEY_DEFAULT
        # WR-05: treat placeholder as no key so events drop locally, not POSTed
        # with a junk key until the real phc_... key lands before Phase 114.
        if key == _TELEMETRY_KEY_DEFAULT:
            key = None
        host = os.environ.get('GENIZAH_TELEMETRY_HOST') or None
        set_capture_api_key(key)
        set_capture_host(host)
    except Exception:
        logger.debug('telemetry: _wire_transport_config silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Consent state loading
# ---------------------------------------------------------------------------
def _load_consent_state() -> None:
    """Populate module-level cache from config.pkl. Called at import time.

    On first run config is empty; _enabled stays False and _install_id stays None.
    Never raises — all exceptions are swallowed per CRASH-05 contract.
    """
    global _enabled, _install_id, _current_distinct_id, _identified
    try:
        cfg = load_app_config()
        with _enabled_lock:
            _enabled = bool(cfg.get(TELEMETRY_ENABLED_KEY, False))
        with _state_lock:
            _install_id = cfg.get(TELEMETRY_INSTALL_ID_KEY)
            _identified_user = cfg.get(IDENTIFIED_USER_KEY)
            _identified = bool(_identified_user)
            _current_distinct_id = _identified_user or _install_id
        # Wire default distinct_id into the transport if we have one and are enabled
        if _install_id:
            with _enabled_lock:
                enabled = _enabled
            if enabled:
                set_default_distinct_id(_current_distinct_id)
    except Exception:
        logger.debug('telemetry: _load_consent_state silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Public API — 8 callables + identity hooks
# ---------------------------------------------------------------------------

def is_enabled() -> bool:
    """Cached no-throw consent check.

    Returns False when telemetry_enabled key is absent from config.pkl.
    MUST never raise — called from crash hooks in Phase 113 (CRASH-05).
    """
    try:
        with _enabled_lock:
            return _enabled
    except Exception:
        return False


def get_install_id() -> str | None:
    """Return the persisted anonymous UUID hex, or None if not yet opted in."""
    try:
        with _state_lock:
            return _install_id
    except Exception:
        return None


def set_consent(enabled: bool) -> None:
    """Persist the consent flag to config.pkl and update the in-memory cache.

    On opt-in:
        - Mints uuid4 install_id (only when absent — idempotent)
        - Writes consent audit fields (timestamp, app_version, ui_ver)
        - Wires the transport key/host (REVIEWS HIGH-1)
        - Sets the default distinct_id in the transport

    On opt-out (CONSENT-06):
        - Sets telemetry_enabled=False
        - RETAINS telemetry_install_id (never deletes it)
        - Clears IDENTIFIED_USER_KEY in config (WR-02 clean privacy boundary)
        - Resets in-memory identity to anonymous (WR-02)
        - Drains the in-memory queue (CONSENT-08)
        - Clears the default distinct_id from the transport

    Never raises.
    """
    global _enabled, _install_id, _current_distinct_id, _identified
    try:
        updates: dict = {TELEMETRY_ENABLED_KEY: enabled}
        if enabled:
            # Wire transport first so any test that checks ph._api_key_override
            # after set_consent(True) sees the key.
            _wire_transport_config()
            with _state_lock:
                local_install_id = _install_id
            if not local_install_id:
                local_install_id = uuid.uuid4().hex
                updates[TELEMETRY_INSTALL_ID_KEY] = local_install_id
                with _state_lock:
                    _install_id = local_install_id
                    if not _current_distinct_id:
                        _current_distinct_id = local_install_id
            updates[CONSENT_TIMESTAMP_KEY] = datetime.now(timezone.utc).isoformat()
            updates[CONSENT_APP_VERSION_KEY] = _APP_VERSION
            updates[CONSENT_UI_VERSION_KEY] = '1'
        else:
            # WR-02: clear identified user from config on opt-out.
            # CONSENT-06 only requires retaining telemetry_install_id, not the
            # identified user -- opt-out is a clean privacy boundary.
            updates[IDENTIFIED_USER_KEY] = None
        # CONSENT-06: do NOT include TELEMETRY_INSTALL_ID_KEY in updates on opt-out
        save_app_config(updates)
        with _enabled_lock:
            _enabled = enabled
        if enabled:
            with _state_lock:
                distinct_id = _current_distinct_id or _install_id
            set_default_distinct_id(distinct_id)
        else:
            # Opt-out: drain queued events (CONSENT-08) and clear default distinct_id
            _drain_and_discard()
            set_default_distinct_id(None)
            # WR-02: reset in-memory identity to anonymous so re-opt-in starts clean.
            # _install_id is retained (CONSENT-06) but identity is wiped.
            with _state_lock:
                _identified = False
                _current_distinct_id = _install_id
    except Exception:
        logger.debug('telemetry: set_consent silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Internal emit path
# ---------------------------------------------------------------------------
def _set_current_distinct_id(distinct_id: str, anonymous: bool) -> None:
    """Update module-level identity state and the transport default."""
    global _current_distinct_id, _identified
    with _state_lock:
        _current_distinct_id = distinct_id
        _identified = not anonymous
    set_default_distinct_id(distinct_id)


def _emit(event_value: str, props: dict, distinct_id: str | None = None) -> None:
    """Internal emission path — merge base props, validate, scrub, enqueue.

    Never raises. Called by track(), identify(), reset_identity().
    """
    try:
        merged = dict(_BASE_PROPS())
        # WR-04: read _identified AND _current_distinct_id in a SINGLE lock acquisition
        # so an identify/reset on another thread cannot mislabel an event
        # (e.g. identified=True paired with the old anonymous distinct_id).
        with _state_lock:
            identified = _identified
            effective_id = distinct_id or _current_distinct_id or 'system'
        merged['$process_person_profile'] = identified
        merged.update(props)
        # IN-02: re-apply the computed $process_person_profile AFTER merged.update(props)
        # so a caller-supplied value cannot override it -- anonymous users must not
        # force person-profile processing via track() kwargs.
        merged['$process_person_profile'] = identified
        validated = _validate_props(merged)
        scrubbed = _scrub_props(validated)
        enqueue_event(event_value, scrubbed, distinct_id=effective_id)
    except Exception:
        logger.debug('telemetry: _emit silently failed for %r', event_value, exc_info=True)


# ---------------------------------------------------------------------------
# Public track / track_performance / track_error
# ---------------------------------------------------------------------------

def track(event: 'str | DesktopEvent', **props) -> None:
    """Gate-checked, scrubbed event emission for desktop events.

    Rejects:
    - Events not in DesktopEvent enum (PRIV-06)
    - Events in _TRACK_FORBIDDEN_EVENTS (e.g. $identify — REVIEWS MEDIUM)
    - Calls when consent is False (CONSENT-01)

    Never raises.
    """
    try:
        if not is_enabled():
            return
        # Resolve event value
        if isinstance(event, DesktopEvent):
            event_value = event.value
        else:
            # IN-03: str(event) on an arbitrary object could produce surprising values,
            # but the result is IMMEDIATELY re-validated against _VALID_EVENT_VALUES
            # (the fixed DesktopEvent registry, PRIV-06). Only exact enum string values
            # pass. Phase 113-115 authors must NOT bypass this check.
            event_value = str(event)
        # Reject if not a valid DesktopEvent value (PRIV-06)
        if event_value not in _VALID_EVENT_VALUES:
            logger.debug('telemetry: track() rejected unknown event %r', event_value)
            return
        # Reject forbidden events (e.g. $identify — REVIEWS MEDIUM)
        if event_value in _TRACK_FORBIDDEN_EVENTS:
            logger.debug('telemetry: track() rejected forbidden event %r', event_value)
            return
        _emit(event_value, props)
    except Exception:
        logger.debug('telemetry: track() silently failed', exc_info=True)


def track_performance(
    event: 'str | DesktopEvent',
    duration_ms: float,
    result_count: int | None = None,
    **extra,
) -> None:
    """Gate-checked performance event. Phase 115 wires producers.

    Validates the event is a DesktopEvent (and not forbidden) and routes
    through _emit with duration_ms/result_count props. Never raises.
    """
    try:
        if not is_enabled():
            return
        if isinstance(event, DesktopEvent):
            event_value = event.value
        else:
            # IN-03: str() result is immediately re-validated against the fixed registry
            event_value = str(event)
        if event_value not in _VALID_EVENT_VALUES:
            logger.debug('telemetry: track_performance() rejected unknown event %r', event_value)
            return
        if event_value in _TRACK_FORBIDDEN_EVENTS:
            logger.debug('telemetry: track_performance() rejected forbidden event %r', event_value)
            return
        props: dict = {'duration_ms': duration_ms}
        if result_count is not None:
            props['result_count'] = result_count
        props.update(extra)
        _emit(event_value, props)
    except Exception:
        logger.debug('telemetry: track_performance() silently failed', exc_info=True)


def track_error(context: str, exc: Exception) -> None:
    """Gate-checked error event. Phase 113 wires producers.

    Emits DesktopEvent.CRASH with context + exc_type ONLY.
    NEVER includes exception message text (CRASH-04 — messages may contain
    query text, file paths, or Hebrew content).
    Never raises.
    """
    try:
        if not is_enabled():
            return
        props: dict = {
            'context': context,             # allowlisted key — survives scrubber
            'exc_type': type(exc).__name__,  # safe: class name only, never message
        }
        _emit(DesktopEvent.CRASH.value, props)
    except Exception:
        logger.debug('telemetry: track_error() silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Identity mechanism (IDENT-03/04)
# ---------------------------------------------------------------------------

def identify(user_id: str) -> None:
    """Emit $identify event to merge pre-login anonymous history with logged-in person.

    Sends ONLY: distinct_id=user_id, $anon_distinct_id=install_id, base props.
    NEVER sends email or name (D-08 hard rule).
    This is the SOLE sanctioned emitter of $identify (Pitfall 6 / REVIEWS MEDIUM).
    Consent-gated. Never raises.
    """
    try:
        if not is_enabled():
            return
        install_id = get_install_id()
        if not install_id:
            return  # no anon id to alias — do not emit
        props: dict = {
            '$process_person_profile': True,   # D-09: identified events use profiles
            '$anon_distinct_id': install_id,   # aliases the per-install uuid into the person
            # NOTE: no email, no name — D-08 hard rule
        }
        merged = dict(_BASE_PROPS())
        merged.update(props)
        validated = _validate_props(merged)
        scrubbed = _scrub_props(validated)
        # Emit $identify directly — bypasses track()'s _TRACK_FORBIDDEN_EVENTS check
        # because identify() IS the sanctioned emitter (Pitfall 6 / Open Question 3).
        enqueue_event(DesktopEvent.IDENTIFY.value, scrubbed, distinct_id=user_id)
        _set_current_distinct_id(user_id, anonymous=False)
        save_app_config({IDENTIFIED_USER_KEY: user_id})
    except Exception:
        logger.debug('telemetry: identify() silently failed', exc_info=True)


def reset_identity() -> None:
    """Reset distinct_id back to per-install uuid4 (mirrors web posthog.reset()).

    Sets $process_person_profile=False for subsequent anonymous events.
    Emits DesktopEvent.IDENTITY_RESET via _emit (not via track() — IDENTITY_RESET
    is in _TRACK_FORBIDDEN_EVENTS to ensure only reset_identity() emits it).
    Consent-gated. Never raises.
    """
    try:
        if not is_enabled():
            return
        install_id = get_install_id()
        # Emit the reset event before reverting identity state
        _emit(DesktopEvent.IDENTITY_RESET.value, {})
        if install_id:
            _set_current_distinct_id(install_id, anonymous=True)
        save_app_config({IDENTIFIED_USER_KEY: None})
    except Exception:
        logger.debug('telemetry: reset_identity() silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Dev-only self-test (D-06)
# ---------------------------------------------------------------------------

def run_selftest() -> None:
    """Dev-only pipeline probe — emits one desktop_selftest event.

    Consent-gated. Reachable only via explicit invocation (--telemetry-selftest
    CLI flag wired in genizah_app in a later phase, or via python -m desktop.telemetry).
    Never fires in normal app startup. Never raises.
    """
    try:
        if not is_enabled():
            return
        track(DesktopEvent.SELFTEST)
    except Exception:
        logger.debug('telemetry: run_selftest() silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Phase 112/113 stubs — implemented in later phases.
# Present here so ROADMAP SC#1 import check (8-callable surface) passes.
# ---------------------------------------------------------------------------

def install_exception_hooks() -> None:
    """Install crash-capture exception hooks. Implemented in Phase 113.

    Consent-gated no-op in Phase 111. Never raises.
    """
    # Phase 113 implementation


def show_first_run_prompt() -> None:
    """Display the first-run consent prompt. Implemented in Phase 112.

    No-op in Phase 111. Never raises.
    """
    # Phase 112 implementation


# ---------------------------------------------------------------------------
# Test seam (excluded from PRIV-03 AST guard — internal test infrastructure)
# ---------------------------------------------------------------------------

def _reset_for_tests() -> None:
    """Reset all module-level state to defaults. NOT for production use.

    Called by test fixtures (same convention as posthog_server._reset_for_tests).
    Resets: _enabled, _install_id, _current_distinct_id, _identified.
    """
    global _enabled, _install_id, _current_distinct_id, _identified
    with _enabled_lock:
        _enabled = False
    with _state_lock:
        _install_id = None
        _current_distinct_id = None
        _identified = False


# ---------------------------------------------------------------------------
# Module initialization — wire transport then load consent state
# ---------------------------------------------------------------------------
_wire_transport_config()   # REVIEWS HIGH-1: desktop key reaches transport at import
_load_consent_state()      # Populates _enabled/_install_id/_current_distinct_id


# ---------------------------------------------------------------------------
# Public surface declaration
# ---------------------------------------------------------------------------
__all__ = [
    # Public callables
    'is_enabled',
    'track',
    'track_performance',
    'track_error',
    'get_install_id',
    'set_consent',
    'identify',
    'reset_identity',
    'run_selftest',
    # Phase stubs
    'install_exception_hooks',
    'show_first_run_prompt',
    # Enum
    'DesktopEvent',
    # config.pkl key constants (shared across phases)
    'TELEMETRY_ENABLED_KEY',
    'TELEMETRY_INSTALL_ID_KEY',
    'FIRST_RUN_SHOWN_KEY',
    'CONSENT_TIMESTAMP_KEY',
    'CONSENT_APP_VERSION_KEY',
    'CONSENT_UI_VERSION_KEY',
    'IDENTIFIED_USER_KEY',
    # Internal (test seam + helpers — excluded from PRIV-03)
    '_reset_for_tests',
    '_load_consent_state',
    '_wire_transport_config',
    '_scrub_value',
    '_scrub_props',
    '_validate_props',
    '_emit',
    '_BASE_PROPS',
    '_ALLOWED_PROPS',
    '_BANNED_KEYS',
    '_VALID_EVENT_VALUES',
    '_TRACK_FORBIDDEN_EVENTS',
]


# ---------------------------------------------------------------------------
# Dev self-test entry point (D-06)
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import sys
    if os.environ.get('GENIZAH_TELEMETRY_KEY'):
        print('telemetry: running self-test pipeline probe...')
        # WR-03: do NOT persistently mutate real consent -- snapshot and restore
        # so config.pkl is left untouched by a developer running the self-test.
        # In-memory _enabled toggle lets the probe run without writing to disk.
        prior_enabled = is_enabled()
        try:
            with _enabled_lock:
                _enabled = True  # in-memory only, no config.pkl write
            _wire_transport_config()  # apply the env key for this one run
            run_selftest()
            # Allow daemon thread to drain
            import time
            time.sleep(1.0)
            print('telemetry: self-test complete (check PostHog for desktop_selftest event)')
        finally:
            with _enabled_lock:
                _enabled = prior_enabled
    else:
        print('Set GENIZAH_TELEMETRY_KEY env var to run the self-test.', file=sys.stderr)
        sys.exit(1)

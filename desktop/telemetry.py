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
import platform
import re
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone

from genizah_core import load_app_config, save_app_config
from version import APP_VERSION
from shared.posthog_server import (
    enqueue_event,
    set_default_distinct_id,
    set_capture_api_key,
    set_capture_host,
    register_scrub_hook,
    _drain_and_discard,
    send_crash_event_direct,  # Phase 113: module-top import (REVIEWS HIGH-2 — no in-hook import)
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal version constant (used in consent audit + BASE_PROPS)
# ---------------------------------------------------------------------------
_APP_VERSION: str = APP_VERSION

# ---------------------------------------------------------------------------
# OS constants — computed ONCE at import time (lock-free; no per-call syscall).
# Used by _BASE_PROPS() to satisfy CRASH-04/SC#3/D-02 (OS in crash payloads).
# These are module-level constants; _emit_crash_direct and _emit_native_crash
# inherit OS props for free via their dict(_BASE_PROPS()) merge.
# ---------------------------------------------------------------------------
_OS_FAMILY: str = platform.system() or 'unknown'   # e.g. 'Windows', 'Linux', 'Darwin'
_OS_VERSION: str = platform.release() or 'unknown'  # e.g. '10', '11', '5.15.0-73-generic'

# ---------------------------------------------------------------------------
# Embedded publishable-key constant (D-03).
# Overridable via GENIZAH_TELEMETRY_KEY (all builds) or POSTHOG_API_KEY
# (source/dev runs only) — see _wire_transport_config for the resolution order.
# Desktop reuses the SHARED PostHog project (segmented by 'platform': 'desktop'),
# NOT a separate project (INFRA-01 reversed 2026-06-15). The real phc_ ingestion
# key — the same publishable key the web app already exposes in client JS — is
# baked into _TELEMETRY_KEY_DEFAULT at build/release time so the shipped .exe
# sends without any env var.
#
# _UNFILLED_KEY_SENTINEL is a FIXED literal meaning "no real key embedded yet".
# It MUST stay distinct from _TELEMETRY_KEY_DEFAULT's real value: the "drop
# locally" guard in _wire_transport_config compares against the sentinel, so
# comparing against the mutable constant would null out the real key once it is
# baked in (Codex 2026-06-15 #1 — release-blocker).
#
# _TELEMETRY_KEY_DEFAULT is the SHARED project's publishable phc_ ingestion key —
# the same key the web app exposes in client JS (web/main.py _posthog_key). It is
# publishable by design (write-only, abuse-tolerant), so embedding it in the
# distributed binary adds no new exposure. NEVER put a personal phx_ key here.
# ---------------------------------------------------------------------------
_UNFILLED_KEY_SENTINEL: str = '<embedded-placeholder>'
_TELEMETRY_KEY_DEFAULT: str = 'phc_CGTsV72FpOzKDrosXvnSD2t3giKNkjirI2migZCe0j7'

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
# Re-ask config-key constants (SEED-031 / quick-260714-k56)
# Consent-INDEPENDENT bookkeeping — these keys exist and update BEFORE opt-in
# and must never themselves open a data path (T-k56-03). Gated exclusively by
# should_reask_consent() below; the chokepoint / property allowlist / fixed
# event-name registry are untouched.
# ---------------------------------------------------------------------------
TELEMETRY_LAST_ASKED_VERSION_KEY = 'telemetry_last_asked_version'  # str — app version at last ask
TELEMETRY_LAST_ASKED_TS_KEY      = 'telemetry_last_asked_ts'       # ISO-8601 str — last ask timestamp
TELEMETRY_ASK_COUNT_KEY          = 'telemetry_ask_count'           # int — lifetime re-ask counter
TELEMETRY_NEVER_ASK_KEY          = 'telemetry_never_ask'           # bool — hard opt-out from re-asking

REASK_COOLDOWN_DAYS = 30   # minimum days between re-asks
REASK_MAX_ASKS = 3         # lifetime cap on re-ask surfacings

# ---------------------------------------------------------------------------
# Module-level state (mirror nli_circuit_breaker.py singleton pattern)
# ---------------------------------------------------------------------------
_enabled: bool = False
_enabled_lock = threading.Lock()
_install_id: str | None = None
_current_distinct_id: str | None = None
_identified: bool = False
_session_id: str = ''  # per-process session UUID (set by genizah_app at session start — WR-01)
_state_lock = threading.Lock()  # guards _install_id, _current_distinct_id, _identified, _session_id

# ---------------------------------------------------------------------------
# Phase 113 crash-hook globals — read lock-free in the crash hook (D-05)
# All are plain module globals; CPython GIL ensures atomic bool/str reads.
# ---------------------------------------------------------------------------
_crash_distinct_id: str | None = None   # snapshot; written by set_consent/identity, read without lock
_in_crash_hook: bool = False            # recursion guard; plain bool is GIL-safe for single-thread re-entrancy
_hooks_installed: bool = False          # idempotency guard for install_exception_hooks()
_faulthandler_handle = None             # kept open for whole process lifetime (D-03)
_pending_native_crash: str | None = None  # held when prior native crash but consent not yet True (D-03)
_last_reported_tb_id: int | None = None   # lock-free traceback dedup (D-08 / REVIEWS PASS2)
_prior_excepthook = None                  # captured at install time; restored by _reset_for_tests (MEDIUM-8)
_prior_threading_hook = None              # captured at install time; restored by _reset_for_tests (MEDIUM-8)


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

    # Active user (Phase 114)
    ACTIVE_PING    = 'desktop_active_ping'

    # Performance (Phase 115)
    SESSION_PERF      = 'desktop_session_performance_summary'
    INDEXING_COMPLETE = 'desktop_indexing_complete'

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
# F2/Codex review: the old `\S+` patterns stopped at the first space and had no
# UNC branch, so paths with spaces ("C:\Users\Jane Doe\Notes") were only
# PARTIALLY redacted (the username/folder after the space leaked) and
# "\\server\share\..." survived whole. A path TAIL now consumes single internal
# spaces (folder/file names like "Jane Doe") but stops at a DOUBLE space,
# control char, or end-of-string. Over-redacting a little trailing prose after a
# path is acceptable; leaking a username/folder is not. The alternation is
# unambiguous (\S vs a single space) so there is no catastrophic backtracking,
# and values are length-capped before this runs (WR-01).
_PATH_TAIL = r'(?:\S| (?! ))*'
_PATH_RE = re.compile(
    r'[A-Za-z]:[\\/]' + _PATH_TAIL              # Windows drive: C:\Users\Jane Doe\...
    + r'|\\\\[^\\/\s]+[\\/]' + _PATH_TAIL        # UNC: \\server\share\...
    + r'|(?<![\w.])/[^\s/]{2,}/' + _PATH_TAIL    # POSIX dir: /home/jane/My Notes/...
    + r'|/\S{3,}'                                # POSIX no-space fallback (>=3 chars)
    + r'|\S+\.[A-Za-z]\w{0,7}\b',                # bare filename: foo.pdf, notes.markdown
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
    'error_fingerprint',    # "{exc_type}:{exc_module}:{exc_lineno}" (D-07)
    'is_background_thread', # bool; True when emitted from threading.excepthook (D-07)
    'fatal_error',          # fixed enum label for native crashes (D-02; desktop_prior_crash only)
    # Perf (Phase 115+) — existing
    'duration_ms', 'result_count', 'sample_n',
    # Context label (allowlisted explicitly — survives _scrub_props)
    'context',
    # Phase 115 NEW additions:
    'perf_summary',       # top-level container key for nested per-mode stats dict (D-07/KQ-8)
    'operation_kind',     # indexing event: enum literal ('initial_scan' etc.) (D-02)
    'doc_count_bucket',   # indexing event: coarse doc count bucket (D-02/KQ-6)
    # Phase 146: which parallels method ran ('chunk' | 'passage'). A fixed
    # two-value enum stamped at dispatch, never free text.
    'method',
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
# Context-code guard (F4 / Codex review)
# ---------------------------------------------------------------------------
# 'context' is the ONE allowlisted free-text property, but _scrub_value only
# redacts paths + Hebrew + length — so English / Judeo-Arabic / transliterated
# private text (e.g. a search query "Maimonides rent letter") would otherwise
# reach PostHog. A legitimate context is a static machine code like
# 'search_tab.run_query'; enforce that shape and collapse anything else to
# 'unregistered'. This guards every emit path (track_error AND track(context=)).
_MAX_CONTEXT_LEN = 64
# Identifier-shaped code: letters/digits with . _ - separators only. No '/' (it
# would let a relative path like 'etc/passwd' survive), no spaces, no Hebrew.
_CONTEXT_RE = re.compile(r'[A-Za-z0-9]+(?:[._\-][A-Za-z0-9]+)*\Z')
# Known file extensions. If a _CONTEXT_RE-shaped value's final dotted segment is a real
# file extension (case-insensitive), the value is a FILENAME, not a code label, and must
# collapse to 'unregistered' (PRIV-04: filenames must not ride through the lone allowlisted
# free-text key). A curated denylist — NOT a broad "any short dotted token" shape — because a
# loose shape also collapses legitimate dotted code labels whose final segment is a
# method-ish word ('search_tab.run', 'app.crash'), and the established
# tests/test_telemetry_codex_review_fixes.py::test_safe_context_preserves_code_labels contract
# REQUIRES those dotted labels to survive verbatim. A pure label allowlist is not viable here:
# `context` has ZERO production producers today (it is forward-looking defense-in-depth; grep
# confirms no track_error()/track(context=) callsite outside this module), so there is no real
# label set to enumerate. DEFENSE-IN-DEPTH POSTURE (Codex 116 review BLOCKER-1): this denylist is
# the LAST line, not the only one — the PRIMARY structural guard against dynamic user content
# (real query text / real My-Library filenames are runtime values) reaching ANY telemetry arg is
# the D-17 producer-layer AST guard tests/test_no_dynamic_telemetry_strings.py (forbids
# text()/selectedFiles()/currentText()/... at telemetry callsites), plus the recursive
# _scrub_value path-redactor on every key EXCEPT context. The residual (a hardcoded-literal
# filename with an extension not listed below, passed on `context` by a future producer) is
# accepted. The set covers the My-Library document types + common document/data/image/archive/
# code/log file extensions a real filename would carry.
_CONTEXT_FILE_EXTENSIONS = frozenset({
    # documents
    'pdf', 'doc', 'docx', 'txt', 'text', 'rtf', 'odt', 'tex', 'epub', 'mobi', 'azw', 'azw3',
    'djvu', 'pages', 'md', 'markdown', 'rst',
    # spreadsheets / presentations
    'xls', 'xlsx', 'xlsm', 'csv', 'tsv', 'ods', 'ppt', 'pptx', 'key', 'numbers',
    # web / markup / config / data
    'html', 'htm', 'xml', 'json', 'jsonl', 'yaml', 'yml', 'toml', 'ini', 'cfg', 'conf',
    'sql', 'parquet', 'bib', 'ris', 'eml', 'msg', 'vcf', 'ics',
    # images
    'png', 'jpg', 'jpeg', 'gif', 'bmp', 'tif', 'tiff', 'webp', 'svg', 'heic', 'ico',
    'jp2', 'j2k', 'jpx', 'psd', 'raw',
    # archives
    'zip', 'rar', '7z', 'gz', 'tar', 'bz2', 'xz', 'tgz',
    # media
    'mp3', 'mp4', 'wav', 'mov', 'avi', 'mkv', 'flac', 'ogg', 'm4a', 'webm',
    # data / binaries / logs
    'db', 'sqlite', 'sqlite3', 'pkl', 'bak', 'dat', 'log', 'exe', 'dll', 'msi', 'bin',
})


def _safe_context(value: object) -> str:
    """Return value if it is an identifier-shaped code, else 'unregistered'.

    A value passes if it:
    - is a str
    - is non-empty and at most _MAX_CONTEXT_LEN chars
    - matches _CONTEXT_RE (identifier-shape: letters/digits with . _ - separators)
    - does NOT look like a filename (PRIV-04 hardening): if the value's final dotted
      segment is a known file extension (e.g. '.docx', '.pdf', '.txt', '.xlsx'), it is
      a filename and collapses to 'unregistered'. Legitimate dotted code labels whose
      final segment is a method-ish word ('search_tab.run', 'app.crash',
      'search_tab.run_query') are NOT file extensions and are preserved.
    """
    if (isinstance(value, str)
            and 0 < len(value) <= _MAX_CONTEXT_LEN
            and _CONTEXT_RE.match(value)):
        # PRIV-04: reject filename-shaped contexts so a bare filename like
        # 'manuscript_notes.docx' cannot ride through the lone allowlisted free-text key.
        if '.' in value and value.rsplit('.', 1)[1].lower() in _CONTEXT_FILE_EXTENSIONS:
            return 'unregistered'
        return value
    return 'unregistered'


# ---------------------------------------------------------------------------
# Base props helper
# ---------------------------------------------------------------------------
def _BASE_PROPS() -> dict:
    """Return the platform + app_version + OS base properties added to every event.

    RESEARCH A1 invariant (lock-free): reads ONLY module-level constants
    (_APP_VERSION, _OS_FAMILY, _OS_VERSION) — no _state_lock acquired.
    If identity state is ever added here, a _crash_base_props() reading only
    constants must replace it in the crash path (_emit_crash_direct).
    """
    return {
        'platform': 'desktop',
        'app_version': _APP_VERSION,
        'os_family': _OS_FAMILY,
        'os_version': _OS_VERSION,
    }


# ---------------------------------------------------------------------------
# Shared-queue scrub hook (INFRA-F2 / Codex 2026-06-15 #2)
# ---------------------------------------------------------------------------
def _desktop_default_props_hook(payload: dict) -> 'dict | None':
    """Tag EVERY event leaving this desktop process via the shared queue.

    Registered into shared.posthog_server (via register_scrub_hook) while desktop
    consent is active. shared-module emitters — notably shared/nli_circuit_breaker's
    nli_breaker_opened/closed — call enqueue_event directly, bypassing _emit(), so
    without this hook they would reach the SHARED project untagged and could attach a
    person profile to the desktop install_id. This hook guarantees:
      - platform='desktop'              (so web vs desktop stays separable)
      - $process_person_profile=False   (infra/anon events create no person)

    Fill-when-absent ONLY: an explicit value set by desktop's own _emit() path always
    wins, so identified usage events (Phase 114 IDENT) are unaffected. Runs before
    _event_queue.put_nowait (raw data never enters the queue). Never raises — on any
    error the payload is returned unchanged (defence-in-depth must not drop telemetry).
    """
    try:
        props = payload.get('properties')
        if not isinstance(props, dict):
            props = {}
        props.setdefault('platform', 'desktop')
        props.setdefault('$process_person_profile', False)
        payload['properties'] = props
    except Exception:
        pass
    return payload


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
        # Key resolution order:
        #   1. GENIZAH_TELEMETRY_KEY  — explicit override, honored in ALL builds
        #      (tests / one-off dev / intentional debug against a chosen project).
        #   2. POSTHOG_API_KEY        — the SHARED project ingestion key (the same phc_
        #      key the web app uses). Honored ONLY in source/dev runs, NEVER in a
        #      frozen .exe: an end-user machine could carry a stray POSTHOG_API_KEY
        #      pointing at a foreign project (Codex 2026-06-15 #3). Desktop reuses the
        #      existing project, segmented by the 'platform': 'desktop' base prop
        #      (INFRA-01 reversed 2026-06-15).
        #   3. _TELEMETRY_KEY_DEFAULT — embedded publishable key for the shipped binary.
        # Use ONLY a phc_ INGESTION key — never POSTHOG_PERSONAL_API_KEY (phx_,
        # account-level read/management scope; must never reach a distributable).
        import sys as _sys
        _frozen = bool(getattr(_sys, 'frozen', False))
        key = os.environ.get('GENIZAH_TELEMETRY_KEY')
        if not key and not _frozen:
            key = os.environ.get('POSTHOG_API_KEY')
        if not key:
            key = _TELEMETRY_KEY_DEFAULT
        # Unfilled sentinel -> no key, so events drop locally instead of being
        # POSTed with a junk key (shipped binary before the real key is baked in).
        if key == _UNFILLED_KEY_SENTINEL:
            key = None
        # Accept only a phc_ project/ingestion token. Reject phx_ (personal key) and
        # any other shape — never POST with a non-ingestion key (Codex 2026-06-15 #4).
        # Do not log key material.
        if key and not key.startswith('phc_'):
            logger.debug('telemetry: refusing non-phc_ capture key — not wiring')
            key = None
        # Default host is eu.i.posthog.com (matches the EU project); override only
        # if the project ever moves regions.
        host = os.environ.get('GENIZAH_TELEMETRY_HOST') or None
        set_capture_api_key(key)
        set_capture_host(host)
        # INFRA-F2: while a real key is wired (consent on), tag every shared-queue
        # event (incl. shared-module emitters that bypass _emit) as platform=desktop.
        # Cleared when no key is wired so nothing tags events that won't be sent.
        register_scrub_hook(_desktop_default_props_hook if key else None)
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
    global _enabled, _install_id, _current_distinct_id, _identified, _crash_distinct_id
    try:
        cfg = load_app_config()
        enabled = bool(cfg.get(TELEMETRY_ENABLED_KEY, False))
        with _enabled_lock:
            _enabled = enabled
        with _state_lock:
            _install_id = cfg.get(TELEMETRY_INSTALL_ID_KEY)
            identified_user = cfg.get(IDENTIFIED_USER_KEY)
            _identified = bool(identified_user)
            _current_distinct_id = identified_user or _install_id
            distinct_id = _current_distinct_id
        # F1/Codex: wire the capture key ONLY on a consented launch (never at
        # import). The shared transport is intentionally ungated, so a key wired
        # before opt-in would let ungated emitters (e.g. the NLI circuit-breaker,
        # reachable from the desktop via genizah_core) POST without consent. The
        # key is revoked again on opt-out (set_consent(False)).
        if enabled:
            _wire_transport_config()
            if distinct_id:
                set_default_distinct_id(distinct_id)
                # REVIEWS HIGH-3: populate crash snapshot so a crash before any
                # set_consent() call on a persisted-consent launch emits with the
                # correct identity (not 'system'). Plain assignment — no lock needed.
                _crash_distinct_id = distinct_id
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
        - Populates _crash_distinct_id snapshot (Phase 113 D-05)

    On opt-out (CONSENT-06):
        - Sets telemetry_enabled=False
        - RETAINS telemetry_install_id (never deletes it)
        - Clears IDENTIFIED_USER_KEY in config (WR-02 clean privacy boundary)
        - Resets in-memory identity to anonymous (WR-02)
        - Drains the in-memory queue (CONSENT-08)
        - Clears the default distinct_id from the transport

    Never raises.
    """
    global _enabled, _install_id, _current_distinct_id, _identified, _crash_distinct_id
    try:
        if enabled:
            # --- OPT-IN ---
            # Wire transport first so a test checking ph._api_key_override after
            # set_consent(True) sees the key.
            _wire_transport_config()
            updates: dict = {TELEMETRY_ENABLED_KEY: True}
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
            # Persist BEFORE flipping the in-memory gate (opt-in fail-safe: do not
            # promise telemetry we cannot honor on the next launch).
            save_app_config(updates)
            with _enabled_lock:
                _enabled = True
            with _state_lock:
                distinct_id = _current_distinct_id or _install_id
            set_default_distinct_id(distinct_id)
            # Phase 113 D-05 / REVIEWS HIGH-3: populate crash distinct_id snapshot so
            # the crash hook can read the identity without acquiring _state_lock.
            # Plain global write — no lock needed (GIL-atomic str assignment).
            _crash_distinct_id = distinct_id
            # Phase 113 Plan 03 D-03: emit any pending native crash now that consent
            # is True and _crash_distinct_id is populated. _emit_pending_native_crash
            # is exactly-once (clears _pending_native_crash before emitting). No-op
            # if no prior native crash was detected at startup.
            _emit_pending_native_crash()
        else:
            # --- OPT-OUT (fail-closed; F5/Codex) ---
            # 1. Shut the gate and cut the transport IN MEMORY first, so a
            #    concurrent track() can no longer pass is_enabled() and the
            #    ungated shared transport (NLI breaker) can no longer POST —
            #    BEFORE any slow/failable disk I/O.
            with _enabled_lock:
                _enabled = False
            set_default_distinct_id(None)
            set_capture_api_key(None)   # F1: revoke the key so ungated emitters stop
            set_capture_host(None)
            register_scrub_hook(None)   # INFRA-F2: drop the desktop tagging hook on opt-out
            # 2. Discard anything already queued (CONSENT-08) — no POST.
            _drain_and_discard()
            # 2b. Clear the in-memory perf accumulator (CONSENT-08 parity with queue drain).
            # Prevents stale pre-opt-out window from being flushed on a later re-opt-in
            # (REVIEWS finding 1 / T-115-PRIV-4).
            _clear_perf_accumulator()
            # 3. Reset in-memory identity to anonymous (WR-02). _install_id is
            #    RETAINED (CONSENT-06) but identity is wiped so re-opt-in is clean.
            with _state_lock:
                _identified = False
                _current_distinct_id = _install_id
                local_install_id_for_crash = _install_id
            # Phase 113: on opt-out, reset crash snapshot to anonymous install_id
            # (mirrors the anonymous reset — no lock needed).
            _crash_distinct_id = local_install_id_for_crash
            # 4. Persist the opt-out LAST. CONSENT-06: do NOT write
            #    TELEMETRY_INSTALL_ID_KEY. save_app_config swallows write errors,
            #    so verify the flag actually landed — a failed opt-out that leaves
            #    config enabled for the next launch must not be silent (F5).
            save_app_config({TELEMETRY_ENABLED_KEY: False, IDENTIFIED_USER_KEY: None})
            try:
                persisted = bool(load_app_config().get(TELEMETRY_ENABLED_KEY, False))
            except Exception:
                persisted = False
            if persisted:
                logger.warning(
                    'telemetry: opt-out may not have persisted to config — '
                    'telemetry is disabled in-memory for this session and will '
                    're-check consent on next launch'
                )
    except Exception:
        logger.debug('telemetry: set_consent silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Re-ask gate + bookkeeping (SEED-031 / quick-260714-k56)
#
# Loosens the too-strict FIRST_RUN_SHOWN_KEY lock: a decliner is re-invited via
# a throttled, NON-MODAL bar (desktop/update_ui.py::TelemetryConsentBar) instead
# of being permanently opted out by a reflexive "Not now". should_reask_consent
# is the SOLE gate — pure, config.pkl-only, never raises. record_consent_ask /
# set_never_ask are the SOLE writers of the four re-ask keys above; they are
# consent-INDEPENDENT bookkeeping and never call set_consent() themselves
# (T-k56-01 / T-k56-03 — no data path opens before an explicit set_consent(True)).
# ---------------------------------------------------------------------------

def should_reask_consent(current_version: str, now: 'datetime | None' = None) -> bool:
    """Pure gate: True ONLY when every re-ask condition holds. Never raises.

    Evaluated in order (short-circuit on the first failing condition):
      A. is_enabled() is True                              -> False (already opted in)
      B. FIRST_RUN_SHOWN_KEY absent/False                  -> False (first-run owns the initial ask)
      C. telemetry_never_ask is True                        -> False (hard opt-out)
      D. telemetry_ask_count >= REASK_MAX_ASKS              -> False (lifetime cap)
      E. telemetry_last_asked_version == current_version    -> False (already asked this version)
      F. telemetry_last_asked_ts within REASK_COOLDOWN_DAYS -> False (cooldown active)
      G. otherwise                                          -> True

    An absent or unparseable last-asked timestamp counts as cooldown-satisfied
    (migration case — a pre-feature decliner has FIRST_RUN_SHOWN_KEY=True but
    none of the re-ask bookkeeping keys yet).

    `now` lets tests inject a fixed instant instead of freezing the real clock.
    """
    try:
        if is_enabled():
            return False
        cfg = load_app_config()
        if not cfg.get(FIRST_RUN_SHOWN_KEY, False):
            return False
        if cfg.get(TELEMETRY_NEVER_ASK_KEY, False):
            return False
        try:
            ask_count = int(cfg.get(TELEMETRY_ASK_COUNT_KEY, 0) or 0)
        except (TypeError, ValueError):
            ask_count = 0
        if ask_count >= REASK_MAX_ASKS:
            return False
        last_version = cfg.get(TELEMETRY_LAST_ASKED_VERSION_KEY)
        if last_version is not None and last_version == current_version:
            return False
        last_ts_raw = cfg.get(TELEMETRY_LAST_ASKED_TS_KEY)
        if last_ts_raw:
            try:
                last_ts = datetime.fromisoformat(last_ts_raw)
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
                now_dt = now if now is not None else datetime.now(timezone.utc)
                if now_dt.tzinfo is None:
                    now_dt = now_dt.replace(tzinfo=timezone.utc)
                if (now_dt - last_ts) < timedelta(days=REASK_COOLDOWN_DAYS):
                    return False
            except (TypeError, ValueError):
                pass  # unparseable timestamp -> treat cooldown as satisfied
        return True
    except Exception:
        return False


def record_consent_ask(current_version: str, now: 'datetime | None' = None) -> None:
    """Stamp one re-ask surfacing: +1 ask_count, last_asked_version, last_asked_ts.

    Called from BOTH the first-run decline path (ConsentDialog.done — starts the
    30-day clock at count=1) and the re-ask bar's own surfacing
    (genizah_app._maybe_show_telemetry_reask). Consent-INDEPENDENT: never
    touches TELEMETRY_ENABLED_KEY or any chokepoint state. Never raises.
    """
    try:
        cfg = load_app_config()
        try:
            ask_count = int(cfg.get(TELEMETRY_ASK_COUNT_KEY, 0) or 0)
        except (TypeError, ValueError):
            ask_count = 0
        now_dt = now if now is not None else datetime.now(timezone.utc)
        save_app_config({
            TELEMETRY_ASK_COUNT_KEY: ask_count + 1,
            TELEMETRY_LAST_ASKED_VERSION_KEY: current_version,
            TELEMETRY_LAST_ASKED_TS_KEY: now_dt.isoformat(),
        })
    except Exception:
        logger.debug('telemetry: record_consent_ask silently failed', exc_info=True)


def set_never_ask() -> None:
    """Persist a hard, permanent opt-out from future re-asks (T-k56-04).

    Checked FIRST-class (Test C) in should_reask_consent — once set, no future
    launch will surface the re-ask bar again. Never raises.
    """
    try:
        save_app_config({TELEMETRY_NEVER_ASK_KEY: True})
    except Exception:
        logger.debug('telemetry: set_never_ask silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Internal emit path
# ---------------------------------------------------------------------------
def _set_current_distinct_id(distinct_id: str, anonymous: bool) -> None:
    """Update module-level identity state and the transport default."""
    global _current_distinct_id, _identified, _crash_distinct_id
    with _state_lock:
        _current_distinct_id = distinct_id
        _identified = not anonymous
    set_default_distinct_id(distinct_id)
    # Phase 113 D-05: mirror write to lock-free crash snapshot.
    # Plain assignment — GIL-atomic, no lock needed.
    _crash_distinct_id = distinct_id


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
        # F4/Codex: 'context' is the lone allowlisted free-text key. _safe_context
        # is its DEDICATED scrubber — it permits only an identifier-shaped code
        # and collapses anything else (prose, Hebrew, paths, over-long) to
        # 'unregistered'. Apply it AFTER _scrub_props, sourced from the pre-scrub
        # value, so a legitimate dotted code like 'search_tab.run' is not mangled
        # into [REDACTED] by the generic path/filename redactor. Covers
        # track_error AND any track(..., context=) callsite in Phases 112-115.
        if 'context' in validated:
            scrubbed['context'] = _safe_context(validated['context'])
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
# Phase 113 crash-path helpers — lock-free consent + payload building (D-05/D-07)
# ---------------------------------------------------------------------------

def _is_enabled_nolock() -> bool:
    """Lock-free consent read for crash hooks (D-05 / SC#4).

    Reads _enabled directly (no lock). Safe: CPython GIL ensures a bool read is
    atomic. threading.excepthook runs on the FAILING thread — if that thread held
    _enabled_lock we would deadlock. Worst case: stale False-negative (miss one
    event); stale True-positive is impossible (opt-out clears the key first).
    """
    return _enabled  # direct global read, GIL-safe


# ---------------------------------------------------------------------------
# In-app frame classifier for crash payload (D-07 / REVIEWS MEDIUM-9 + PASS2)
#
# Classification is by PATH SEGMENT of co_filename, NOT realpath equality.
# Rationale (the frozen-build fix): a PyInstaller build bakes the BUILD host's
# source path into each code object's co_filename (e.g.
# 'C:\\ci\\build\\desktop\\puzzle.py', or a bundle-relative 'desktop/puzzle.py').
# On the end user's machine that string never equals a realpath resolved from
# this module's runtime __file__ (which lives inside the _MEIPASS extraction
# dir), so the old realpath-equality match classified EVERY shipped crash as
# 'external' with a useless line number. Segment matching inspects only the
# co_filename string, so it behaves identically from source and from the frozen
# .exe — and we still transmit ONLY the basename, never a path (D-07 privacy).
#
#   _APP_SOURCE_ROOTS:   in-app package directory NAMES. A frame is in-app if its
#                        normalized co_filename contains '/<root>/' or begins
#                        with '<root>/' (bundle-relative form).
#   _APP_SOURCE_FILES:   in-app top-level module BASENAMES (matched by basename).
#                        These live at the repo/bundle root with no '/desktop/'
#                        or '/shared/' segment, so a segment match can't catch
#                        them — they must be named explicitly. Only DISTINCTIVE
#                        app-owned names are listed; generic names that collide
#                        with third-party packages (server.py, version.py) are
#                        deliberately excluded to avoid false positives.
#   _EXCLUDED_PATH_SEGMENTS: normalized substrings that force 'external'
#                        regardless of the above. Checked before any positive
#                        in-app match, so a third-party package that happens to
#                        be named 'desktop' or 'shared' under site-packages can
#                        never be a false positive.
#   _GENERIC_BASENAMES:  names that recur across many packages and must never be
#                        a distinguishing in-app module.
# ---------------------------------------------------------------------------
_APP_SOURCE_ROOTS: tuple[str, ...] = ('desktop', 'shared')
_APP_SOURCE_FILES: frozenset[str] = frozenset({
    'genizah_app.py', 'genizah_core.py', 'gui_threads.py',
    # Distinctive root-level app modules (imported at runtime; no package segment
    # to match on). Generic basenames (server.py/version.py) intentionally omitted.
    'corrections_client.py', 'corrections_ui.py', 'supabase_corrections_client.py',
    'lists_sync.py', 'filter_text_dialog.py', 'column_filter_dialog.py',
    'list_filter_dialog.py', 'genizah_translations.py', 'pgp_tag_translations.py',
    'sefaria_utils.py', 'shared_export_utils.py', 'unified_variants.py',
})
_EXCLUDED_PATH_SEGMENTS: tuple[str, ...] = (
    'site-packages',
    'dist-packages',
    '/.venv/',
    '/venv/',
)
_GENERIC_BASENAMES: frozenset[str] = frozenset({'__init__.py', '__main__.py'})


def _frame_basename(co_filename: str) -> str:
    """Separator-agnostic basename for crash payloads (D-07 privacy).

    os.path.basename is HOST-dependent: posixpath (Linux CI / a POSIX dev host)
    does NOT split a Windows backslash path, so os.path.basename of
    'C:\\a\\b\\puzzle.py' returns the WHOLE string there. A frozen build's
    co_filename always carries the BUILD host's separators no matter where this
    code later runs, so we normalize both separators ourselves. Guarantees ONLY
    a basename ever leaves the process — never a path. Never raises.
    """
    return co_filename.replace('\\', '/').rstrip('/').rsplit('/', 1)[-1]


def _is_in_app_frame(co_filename: str) -> bool:
    """Return True if co_filename belongs to an in-app source file.

    In-app means ALL of:
    - basename is NOT in _GENERIC_BASENAMES
    - normalized path contains NONE of _EXCLUDED_PATH_SEGMENTS (checked before
      any positive in-app match — so a third-party 'desktop'/'shared' package
      under site-packages stays external)
    - AND EITHER the basename is in _APP_SOURCE_FILES, OR the normalized path
      carries an _APP_SOURCE_ROOTS package segment ('/<root>/' anywhere, or a
      bundle-relative '<root>/' prefix)

    Segment-based, NOT realpath — so it classifies identically whether the app
    runs from source or from a frozen PyInstaller build (whose co_filename is the
    build host's path). Pure function, never raises.
    """
    try:
        # Normalize separators to '/' and lowercase: Windows paths are
        # case-insensitive and every token we match (site-packages, venv,
        # desktop, shared, the module basenames) is lowercase.
        norm = co_filename.replace('\\', '/').lower()
        basename = norm.rsplit('/', 1)[-1]
        if basename in _GENERIC_BASENAMES:
            return False
        # Force-external if the path carries a venv/site-packages segment
        # (before any positive in-app match).
        for seg in _EXCLUDED_PATH_SEGMENTS:
            if seg in norm:
                return False
        # Top-level app modules — matched by basename.
        if basename in _APP_SOURCE_FILES:
            return True
        # In-app package modules — '/desktop/' or '/shared/' anywhere, or a
        # bundle-relative 'desktop/...' / 'shared/...' prefix.
        for root in _APP_SOURCE_ROOTS:
            if ('/' + root + '/') in norm or norm.startswith(root + '/'):
                return True
        return False
    except Exception:
        return False


def _make_crash_props(
    exc_type: type,
    exc_tb,         # TracebackType | None
    is_background: bool,
) -> dict:
    """Build crash payload by walking the traceback — no format_exception, no str(exc).

    Finds the innermost IN-APP frame (co_filename carrying an _APP_SOURCE_ROOTS
    package segment or an _APP_SOURCE_FILES basename, excluding
    _EXCLUDED_PATH_SEGMENTS and _GENERIC_BASENAMES — see _is_in_app_frame).
    Falls back to the deepest frame with error_module='external'.
    Returns exactly five keys: exc_type, exc_module, exc_lineno, error_fingerprint,
    is_background_thread. All five are in _ALLOWED_PROPS (D-07).
    Never raises.
    """
    try:
        in_app_frame = None
        deepest_frame = None
        frame = exc_tb
        while frame is not None:
            deepest_frame = frame
            if _is_in_app_frame(frame.tb_frame.f_code.co_filename):
                in_app_frame = frame  # keep walking — want INNERMOST in-app frame
            frame = frame.tb_next

        error_module: str = 'external'
        error_line: int = 0

        if in_app_frame is not None:
            # In-app frame found: transmit only the basename (never the full path).
            # _frame_basename (NOT os.path.basename) so a Windows-style co_filename
            # is reduced to a basename even on a POSIX host (Linux CI) — D-07.
            error_module = _frame_basename(in_app_frame.tb_frame.f_code.co_filename)
            error_line = in_app_frame.tb_lineno
        elif deepest_frame is not None:
            # No in-app frame: fallback is deepest frame, still classified as 'external'
            error_line = deepest_frame.tb_lineno

        error_type: str = exc_type.__name__ if exc_type else 'UnknownException'
        fingerprint: str = f'{error_type}:{error_module}:{error_line}'

        return {
            'exc_type': error_type,
            'exc_module': error_module,
            'exc_lineno': error_line,
            'error_fingerprint': fingerprint,
            'is_background_thread': is_background,
        }
    except Exception:
        return {
            'exc_type': 'UnknownException',
            'exc_module': 'external',
            'exc_lineno': 0,
            'error_fingerprint': 'UnknownException:external:0',
            'is_background_thread': is_background,
        }


def _emit_crash_direct(
    exc_type: type,
    exc_tb,         # TracebackType | None
    is_background: bool,
) -> None:
    """Lock-free crash emission (D-05, SC#4). Call ONLY from sys/threading excepthook.

    Uses module-top imported send_crash_event_direct (REVIEWS HIGH-2 — no in-
    function import, which could take the import lock on a failing thread).
    Reads consent via _is_enabled_nolock() (GIL-safe bool read, no lock).
    Reads distinct_id from _crash_distinct_id snapshot (plain global, no lock).
    Dedups duplicate reports for the same traceback via _last_reported_tb_id
    (D-08 / REVIEWS PASS2 — lock-free id(exc_tb) guard).

    NOTE: _BASE_PROPS() reads only module-level constants (verified: 'platform' +
    _APP_VERSION + _OS_FAMILY + _OS_VERSION). No _state_lock acquired.
    Confirmed at the _BASE_PROPS() definition above. If identity state is ever
    added there, a _crash_base_props() reading only constants must replace it here.
    """
    global _in_crash_hook, _last_reported_tb_id
    if _in_crash_hook:
        return  # recursion guard (D-05 — crash inside crash handler must not loop)
    _in_crash_hook = True
    try:
        if not _is_enabled_nolock():
            return
        # D-08 / REVIEWS PASS2: lock-free traceback-id dedup.
        # Slot/excepthook double-delivery for the same exception can fire the hook
        # twice with the same traceback object. Check id(exc_tb) to emit exactly once.
        if exc_tb is not None:
            tb_id = id(exc_tb)
            if tb_id == _last_reported_tb_id:
                return  # already reported this exact traceback
            _last_reported_tb_id = tb_id  # record BEFORE sending
        distinct_id: str = _crash_distinct_id or 'system'
        props = _make_crash_props(exc_type, exc_tb, is_background)
        merged = dict(_BASE_PROPS())          # no lock — reads only constants
        merged.update(props)
        validated = _validate_props(merged)
        scrubbed = _scrub_props(validated)
        # CR-01: exc_module / error_fingerprint are produced by trusted code.
        # _make_crash_props emits ONLY a basename (never a path) and a fixed
        # "{exc_type}:{basename}:{lineno}" fingerprint. The generic value scrubber's
        # bare-filename branch would redact every "*.py" basename to [REDACTED],
        # collapsing all in-app crashes to "[REDACTED]:<lineno>" and destroying crash
        # grouping (D-07 — the deliverable). Restore them from the pre-scrub payload,
        # mirroring the 'context' carve-out in _emit().
        for _trusted_key in ('exc_module', 'error_fingerprint'):
            if _trusted_key in validated:
                scrubbed[_trusted_key] = validated[_trusted_key]
        # send_crash_event_direct imported at module top (REVIEWS HIGH-2)
        send_crash_event_direct(
            DesktopEvent.CRASH.value, scrubbed, distinct_id, timeout=0.5
        )
    except Exception:
        pass  # hook body MUST never raise (SC#4)
    finally:
        _in_crash_hook = False


# ---------------------------------------------------------------------------
# Phase 113 Plan 03 — native crash detection helpers (D-02 / D-03)
# ---------------------------------------------------------------------------

# Fixed enum mapping from faulthandler first-line prefixes to safe labels.
# D-02: raw dump text is NEVER transmitted — only these fixed enum values.
# Verified prefixes from RESEARCH Q3 (lowercase — _classify_native_crash lowercases first).
_NATIVE_CRASH_LABELS: dict[str, str] = {
    'windows fatal exception: access violation':    'access_violation',
    'windows fatal exception: stack overflow':      'stack_overflow',
    'windows fatal exception: int divide by zero':  'abort',
    'windows fatal exception: float divide by zero': 'abort',
    'segmentation fault':                           'segmentation_fault',
    'aborted':                                      'abort',
    'floating-point exception':                     'abort',
    'bus error':                                    'abort',
    'fatal python error:':                          'unknown_native',
}


def _classify_native_crash(text: str) -> str:
    """Map faulthandler dump first-line to a fixed enum label.

    Never returns raw text — only one of {segmentation_fault, access_violation,
    abort, stack_overflow, unknown_native} (D-02). Anything unrecognized or
    empty → 'unknown_native'.
    """
    if not text or not text.strip():
        return 'unknown_native'
    first_line = text.splitlines()[0].lower().strip()
    for prefix, label in _NATIVE_CRASH_LABELS.items():
        if first_line.startswith(prefix):
            return label
    return 'unknown_native'


def _emit_native_crash(label: str) -> None:
    """Emit a desktop_prior_crash event with the classified native crash label.

    Lock-free: reads only _crash_distinct_id (plain global), _BASE_PROPS()
    (module-level constants only). Never raises.
    """
    try:
        if not _is_enabled_nolock():
            return
        distinct_id: str = _crash_distinct_id or 'system'
        props: dict = dict(_BASE_PROPS())           # includes os_family/os_version (CRASH-04)
        props['fatal_error'] = label                # fixed enum label, never raw text (D-02)
        validated = _validate_props(props)
        scrubbed = _scrub_props(validated)
        # send_crash_event_direct imported at module top (REVIEWS HIGH-2)
        send_crash_event_direct(
            DesktopEvent.PRIOR_CRASH.value, scrubbed, distinct_id, timeout=0.5
        )
    except Exception:
        pass  # best-effort; never raises


def _emit_pending_native_crash() -> None:
    """Emit held pending native crash exactly once when consent becomes True.

    Called from set_consent(True) AFTER _enabled is flipped True and
    _crash_distinct_id is populated. The exactly-once guarantee:
    clear _pending_native_crash BEFORE emitting, not after (race-safe under GIL).
    Never raises.
    """
    global _pending_native_crash
    label = _pending_native_crash
    if label is None:
        return
    _pending_native_crash = None    # clear BEFORE emit — exactly-once
    _emit_native_crash(label)


def _setup_faulthandler() -> None:
    """Read + classify previous faulthandler dump, then enable for this run. D-03.

    ORDERING INVARIANT: read BEFORE faulthandler.enable() — opening the file
    for write first would erase last-run evidence.

    STEP 1: read prior content before enable
    STEP 2: classify + emit-or-hold-pending
    STEP 3: (re)open the file 'w' for this run's handle, then enable

    The 'w' open in STEP 3 intentionally truncates the prior content. This is
    CORRECT per CONTEXT D-03: the prior label is already captured in memory in
    STEP 2 (or already emitted), and the pending label survives in-memory until
    set_consent(True) fires that session. No file-preservation path is needed
    or added — pending is memory-only by design (REVIEWS PASS2).

    Best-effort: any exception silently drops faulthandler capture for this
    run; startup is never blocked (CRASH-03).
    """
    global _faulthandler_handle, _pending_native_crash
    try:
        import faulthandler
        from genizah_core import Config  # lazy — avoids circular at module level
        dump_path = os.path.join(Config.INDEX_DIR, 'faulthandler_dump.txt')

        # STEP 1: read prior content BEFORE enabling (opening 'w' erases evidence)
        prior_dump_text: str = ''
        try:
            if os.path.exists(dump_path):
                with open(dump_path, 'r', encoding='utf-8', errors='replace') as _f:
                    prior_dump_text = _f.read().strip()
        except OSError:
            pass

        # STEP 2: classify + emit or hold pending
        if prior_dump_text:
            label = _classify_native_crash(prior_dump_text)
            if _is_enabled_nolock():
                _emit_native_crash(label)
            else:
                _pending_native_crash = label   # hold; emit on set_consent(True)

        # STEP 3: (re)open for THIS run — truncates last run's content.
        # The module global keeps the handle alive for the process lifetime
        # (Pitfall 2: GC closes the handle if it's a local variable).
        _faulthandler_handle = open(dump_path, 'w', encoding='utf-8')
        faulthandler.enable(file=_faulthandler_handle, all_threads=True)
    except Exception:
        pass  # faulthandler is best-effort; failure must never block startup


# ---------------------------------------------------------------------------
# Phase 112/113 — install_exception_hooks (filled in Plan 03)
# ---------------------------------------------------------------------------

def install_exception_hooks() -> None:
    """Install crash-capture exception hooks + faulthandler. Idempotent. Never raises.

    Wraps sys.excepthook and threading.excepthook to capture uncaught exceptions.
    KeyboardInterrupt and SystemExit are excluded (SC#2).
    The existing hook chain (crash_log.txt writer set up by _setup_crash_handler) is
    preserved — telemetry is inserted as the outermost wrapper and ALWAYS calls the
    prior hook, even if the telemetry step raises (SC#1).

    Captures the CURRENT sys.excepthook and threading.excepthook (not
    threading.__excepthook__) into the _prior_excepthook / _prior_threading_hook globals
    so _reset_for_tests() can restore them (REVIEWS MEDIUM-7 / MEDIUM-8).

    Registers an atexit flush INSIDE this function — desktop-side only (D-08). The
    web process imports shared.posthog_server (which has no atexit.register) and must
    not trigger a desktop exit-flush on web server restart (T-113-08-WEBEXIT).

    SC#5 / CRASH-06 reconciliation (REVIEWS HIGH-4 — option (a), direct-send supersedes):
    The crash event is delivered by the lock-free send_crash_event_direct inside
    _emit_crash_direct BEFORE the hook returns — no hook-time _flush_before_exit call
    is needed or safe (_flush_before_exit takes _capture_config_lock via _resolve_api_key,
    posthog_server.py:288, violating D-05's lock-free invariant for the crash path).
    The atexit _atexit_flush (~1.5s, clean-exit only) covers QUEUED non-crash events on
    a normal shutdown. This satisfies CRASH-06 ("crash event prioritized over full queue")
    via the direct send, and SC#5 ("bounded flush before exit") via direct-send + atexit.

    Never raises.
    """
    global _hooks_installed, _prior_excepthook, _prior_threading_hook
    import sys as _sys
    import threading as _threading
    try:
        if _hooks_installed:
            return  # idempotency guard (D-08) — also prevents double atexit registration
        _hooks_installed = True

        # Capture prior hooks BEFORE wrapping so _reset_for_tests can restore.
        # Called from genizah_app.py AFTER _setup_crash_handler(), so _prior_excepthook
        # captures the crash-log writer, not bare sys.__excepthook__.
        # REVIEWS MEDIUM-7: capture the CURRENT threading.excepthook, NOT
        # threading.__excepthook__ — so an already-installed non-default hook is chained
        # exactly once and not skipped.
        _prior_excepthook = _sys.excepthook
        _prior_threading_hook = _threading.excepthook  # CURRENT hook, not __excepthook__

        # 1. Wrap sys.excepthook: telemetry → crash_log.txt writer → sys.__excepthook__
        prior_sys_hook = _prior_excepthook

        def _telemetry_excepthook(exc_type, exc_value, exc_tb):
            # Telemetry step in try/except — a failure here MUST NOT suppress the chain
            # (SC#1). The prior hook is called UNCONDITIONALLY after the try/except.
            # Do NOT call _flush_before_exit here — it takes _capture_config_lock via
            # _resolve_api_key (deadlock risk, REVIEWS HIGH-4 / D-05). The crash event
            # is already delivered by send_crash_event_direct inside _emit_crash_direct.
            try:
                if exc_type is not KeyboardInterrupt and exc_type is not SystemExit:
                    _emit_crash_direct(exc_type, exc_tb, is_background=False)
            except Exception:
                pass
            prior_sys_hook(exc_type, exc_value, exc_tb)  # UNCONDITIONAL chain (SC#1)

        _sys.excepthook = _telemetry_excepthook

        # 2. threading.excepthook — covers worker threads (CRASH-02).
        # Captures the CURRENT hook (REVIEWS MEDIUM-7) — an existing non-default hook
        # installed before telemetry (e.g. by a test or a prior framework) is chained
        # exactly once, not replaced by threading.__excepthook__.
        prior_thread_hook = _prior_threading_hook

        def _telemetry_threading_hook(args):
            try:
                if args.exc_type is not KeyboardInterrupt and args.exc_type is not SystemExit:
                    _emit_crash_direct(args.exc_type, args.exc_traceback, is_background=True)
            except Exception:
                pass
            prior_thread_hook(args)  # always chain

        _threading.excepthook = _telemetry_threading_hook

        # 3. faulthandler — native C-extension crash detection (D-02 / D-03)
        _setup_faulthandler()

        # 4. atexit flush for clean exits — registered HERE, NOT in shared/posthog_server
        # (D-08 — the web process imports posthog_server; registering there would fire
        # on every web server restart, T-113-08-WEBEXIT).
        # The _hooks_installed guard above ensures exactly-one registration across
        # repeated install calls (REVIEWS MEDIUM-8, T-113-08-DUPATEXIT).
        import atexit as _atexit

        def _atexit_flush():
            """Flush the in-memory PostHog queue on a clean exit (~1.5s budget).

            This is the ONLY path that calls _flush_before_exit — it is safe on the
            clean-exit atexit path because _capture_config_lock is not held by any
            crash hook at process teardown time. The crash event itself is already
            delivered by the lock-free send_crash_event_direct (REVIEWS HIGH-4).
            """
            try:
                from shared.posthog_server import _flush_before_exit
                _flush_before_exit(1.5)   # ~1.5s clean-exit budget (crash path stays 0.5s)
            except Exception:
                pass

        _atexit.register(_atexit_flush)

    except Exception:
        logger.debug('telemetry: install_exception_hooks failed', exc_info=True)


def show_first_run_prompt(parent=None) -> None:
    """Display the first-run consent prompt. Never raises.

    Gates on FIRST_RUN_SHOWN_KEY so it shows at most once.  The dialog's
    single done() finalizer (Plan 01) writes FIRST_RUN_SHOWN_KEY=True and
    calls set_consent() on every exit path — this function only gates + shows.

    Args:
        parent: Optional QWidget parent for the dialog (passed from the
                startup hook in genizah_app.py).
    """
    try:
        cfg = load_app_config()
        if cfg.get(FIRST_RUN_SHOWN_KEY, False):
            return  # already shown — D-05 gate; never show twice
        from desktop.consent_dialog import ConsentDialog  # lazy import — keeps Qt out of headless tests
        dlg = ConsentDialog(parent)
        dlg.exec()
    except Exception:
        logger.debug('telemetry: show_first_run_prompt failed', exc_info=True)


# ---------------------------------------------------------------------------
# Test seam (excluded from PRIV-03 AST guard — internal test infrastructure)
# ---------------------------------------------------------------------------

def _reset_for_tests() -> None:
    """Reset all module-level state to defaults. NOT for production use.

    Called by test fixtures (same convention as posthog_server._reset_for_tests).
    Resets: _enabled, _install_id, _current_distinct_id, _identified (Phase 111);
    _crash_distinct_id, _in_crash_hook, _hooks_installed, _pending_native_crash,
    _last_reported_tb_id (Phase 113).

    REVIEWS MEDIUM-8: also restores sys.excepthook and threading.excepthook to
    their pre-install values (if _prior_excepthook/_prior_threading_hook were
    captured by install_exception_hooks) so test suites don't accumulate hook
    wrappers across test functions.
    """
    global _enabled, _install_id, _current_distinct_id, _identified, _session_id
    global _crash_distinct_id, _in_crash_hook, _hooks_installed, _pending_native_crash
    global _last_reported_tb_id, _prior_excepthook, _prior_threading_hook
    import sys as _sys
    # Use try/except for the locked sections — during tests that mock the lock
    # objects with FailLock, acquiring the lock would raise; fall back to direct
    # assignment (acceptable since tests run single-threaded).
    try:
        with _enabled_lock:
            _enabled = False
    except Exception:
        _enabled = False  # direct fallback when lock is mocked
    try:
        with _state_lock:
            _install_id = None
            _current_distinct_id = None
            _identified = False
            _session_id = ''
    except Exception:
        _install_id = None           # direct fallback when lock is mocked
        _current_distinct_id = None
        _identified = False
        _session_id = ''
    # Phase 113 globals (no locks — plain bool/str/int)
    _crash_distinct_id = None
    _in_crash_hook = False
    _pending_native_crash = None
    _last_reported_tb_id = None
    # Phase 115 perf-accumulator globals (no locks — UI-thread-only writes).
    # Use _clear_perf_accumulator() as the single source of truth for zeroing all three.
    _clear_perf_accumulator()
    # REVIEWS MEDIUM-8: restore sys.excepthook and threading.excepthook
    # to their pre-install state so hooks don't accumulate across test functions.
    if _prior_excepthook is not None:
        _sys.excepthook = _prior_excepthook
    if _prior_threading_hook is not None:
        import threading as _threading
        _threading.excepthook = _prior_threading_hook
    _prior_excepthook = None
    _prior_threading_hook = None
    _hooks_installed = False  # reset AFTER hook restoration


# ---------------------------------------------------------------------------
# Phase 115 — performance accumulator globals + helpers
# ---------------------------------------------------------------------------
# Written from UI thread only (Qt signal-slot guarantee). No lock needed.
_perf_accumulator: dict = {}   # key: mode_str → {'durations_ms': [], 'result_counts': [], ...}
_perf_last_flush_time: float = 0.0   # time.monotonic() of last flush; 0.0 = never flushed
_perf_sample_counter: int = 0        # incremented per accumulate_performance() call; sample_n gate

def _clear_perf_accumulator() -> None:
    """Zero the three performance-accumulator globals. Never raises (CONSENT-08 parity).

    Called from set_consent(False) alongside _drain_and_discard() to prevent stale
    pre-opt-out window from being flushed on a later re-opt-in (REVIEWS finding 1).
    Also called from _reset_for_tests() as the single source of truth.
    """
    global _perf_accumulator, _perf_last_flush_time, _perf_sample_counter
    try:
        _perf_accumulator = {}
        _perf_last_flush_time = 0.0
        _perf_sample_counter = 0
    except Exception:
        logger.debug('telemetry: _clear_perf_accumulator() silently failed', exc_info=True)


# Fixed allowed sets for normalizer functions (REVIEWS finding 3).
# Normalized mode values from Phase-114 D-05 search-mode map (genizah_app.py:17493-17496)
# + composition modes + lab mode. Any unknown → 'unknown' (never verbatim free-string).
_PERF_ALLOWED_MODES: frozenset[str] = frozenset({
    'keyword', 'variants', 'responsa', 'fuzzy', 'regex', 'title', 'shelfmark', 'pgp_tags',
    'comp_exact', 'comp_variants', 'comp_fuzzy',
    # WR-02: producers prepend 'lab_' to the base mode when the LAB toggle is on
    # (genizah_app.py:17536 / :23031). All 11 lab_* combinations must be allowed or
    # they collapse to 'unknown' and lose per-mode attribution for LAB users.
    'lab_keyword', 'lab_variants', 'lab_responsa', 'lab_fuzzy', 'lab_regex',
    'lab_title', 'lab_shelfmark', 'lab_pgp_tags',
    'lab_comp_exact', 'lab_comp_variants', 'lab_comp_fuzzy',
})
_PERF_ALLOWED_CORPUS: frozenset[str] = frozenset({'genizah', 'local', 'all'})
_PERF_ALLOWED_OPERATION_KIND: frozenset[str] = frozenset({
    'initial_scan', 'incremental_add', 'reindex_all', 'lab_rebuild',
})
_PERF_ALLOWED_FLUSH_REASON: frozenset[str] = frozenset({'periodic', 'close', 'manual'})


def _normalize_mode(v: object) -> str:
    """Normalize mode to a fixed allowed set; unknown → 'unknown'. Never raises."""
    try:
        s = str(v)
        return s if s in _PERF_ALLOWED_MODES else 'unknown'
    except Exception:
        return 'unknown'


def _normalize_corpus(v: object) -> str:
    """Normalize corpus_scope to {genizah, local, all}; unknown → 'unknown'. Never raises."""
    try:
        s = str(v)
        return s if s in _PERF_ALLOWED_CORPUS else 'unknown'
    except Exception:
        return 'unknown'


def _normalize_operation_kind(v: object) -> str:
    """Normalize operation_kind to the fixed indexing set; unknown → 'unknown'. Never raises."""
    try:
        s = str(v)
        return s if s in _PERF_ALLOWED_OPERATION_KIND else 'unknown'
    except Exception:
        return 'unknown'


def _normalize_flush_reason(v: object) -> str:
    """Normalize flush_reason to {periodic, close, manual}; unknown → 'unknown'. Never raises."""
    try:
        s = str(v)
        return s if s in _PERF_ALLOWED_FLUSH_REASON else 'unknown'
    except Exception:
        return 'unknown'


def _perf_env_int(name: str, default: int, minimum: int) -> int:
    """Validated env-int reader for perf knobs (REVIEWS finding 8).

    Returns default when absent or non-numeric (int() raises).
    Clamps the parsed value UP to minimum if below it (sample_n<1 → 1).
    Never raises.
    """
    try:
        raw = os.environ.get(name)
        if raw is None:
            return default
        parsed = int(raw)
        return max(parsed, minimum)
    except Exception:
        return default


def _perf_env_float(name: str, default: float, minimum_exclusive: float) -> float:
    """Validated env-float reader for perf knobs (REVIEWS finding 8).

    Returns default when absent or non-numeric.
    Returns default when value <= minimum_exclusive (flush_interval <= 0 → default).
    Never raises.
    """
    try:
        raw = os.environ.get(name)
        if raw is None:
            return default
        parsed = float(raw)
        if parsed <= minimum_exclusive:
            return default
        return parsed
    except Exception:
        return default


def _percentile(durations: 'list[float]', p: float) -> float:
    """Exact percentile over a list of floats. Returns 0.0 for empty input. Never raises."""
    try:
        if not durations:
            return 0.0
        s = sorted(durations)
        idx = min(int(p * len(s)), len(s) - 1)
        return s[idx]
    except Exception:
        return 0.0


def accumulate_performance(
    elapsed_ms: float,
    result_count: int,
    mode: str,
    corpus_scope: str,
) -> None:
    """Accumulate one completed-search perf record into the in-memory summary.

    Called from the UI thread (Qt perf_signal slot). Never emits — only
    _flush_perf_summary emits. Never raises. Respects GENIZAH_PERF_SAMPLE_N env knob.
    Consent gate: is_enabled() first (Pitfall 7).
    """
    try:
        if not is_enabled():
            return
        # Normalize inputs FIRST — normalized values become accumulator keys (REVIEWS finding 3).
        # An out-of-set value becomes 'unknown' so no free-string can ever become a nested key.
        mode = _normalize_mode(mode)
        corpus_scope = _normalize_corpus(corpus_scope)
        # Sampling gate (REVIEWS finding 8 — validated env reader, never bare int()).
        global _perf_sample_counter
        _perf_sample_counter += 1
        sample_n = _perf_env_int('GENIZAH_PERF_SAMPLE_N', default=1, minimum=1)
        if sample_n > 1 and (_perf_sample_counter % sample_n) != 1:
            return  # skip this sample
        entry = _perf_accumulator.setdefault(mode, {
            'durations_ms': [],
            'result_counts': [],
            'zero_result_count': 0,
            'corpus_counts': {'genizah': 0, 'local': 0, 'all': 0, 'unknown': 0},
        })
        entry['durations_ms'].append(float(elapsed_ms))
        entry['result_counts'].append(int(result_count))
        if result_count == 0:
            entry['zero_result_count'] += 1
        # corpus_scope is already normalized to one of genizah/local/all/unknown
        entry['corpus_counts'][corpus_scope] = entry['corpus_counts'].get(corpus_scope, 0) + 1
    except Exception:
        logger.debug('telemetry: accumulate_performance() silently failed', exc_info=True)


def set_session_id(session_id: str) -> None:
    """Record the per-process session UUID so the perf summary's session_id matches
    session_start/session_end (WR-01). Called by genizah_app at session mint. Never raises.
    """
    global _session_id
    try:
        with _state_lock:
            _session_id = str(session_id) if session_id else ''
    except Exception:
        _session_id = str(session_id) if session_id else ''


def _flush_perf_summary(flush_reason: str = 'periodic') -> None:
    """Build and emit desktop_session_performance_summary, then reset accumulator (D-06).

    Called from genizah_app.py flush helpers (UI thread). Never raises.
    flush_reason: 'periodic' | 'close' | 'manual' — normalized, rides the 'action' key.
    Attaches session_id (from _current_distinct_id or _install_id — REVIEWS finding 4).
    """
    try:
        if not is_enabled():
            return
        if not _perf_accumulator:
            return  # nothing to flush
        flush_reason = _normalize_flush_reason(flush_reason)
        perf_summary: dict = {}
        for mode_key, entry in _perf_accumulator.items():
            durs = entry['durations_ms']
            counts = entry['result_counts']
            if not durs:
                continue
            # Coarse result-count bucket distribution (D-03 scheme — 0/1-9/10-99/100+).
            # Inlined here to avoid importing genizah_app (circular import).
            # Canonical scheme: genizah_app._telemetry_result_bucket(); keep in sync.
            bucket_0 = bucket_1_9 = bucket_10_99 = bucket_100plus = 0
            for rc in counts:
                if rc == 0:
                    bucket_0 += 1
                elif rc < 10:
                    bucket_1_9 += 1
                elif rc < 100:
                    bucket_10_99 += 1
                else:
                    bucket_100plus += 1
            perf_summary[mode_key] = {
                'count':             len(durs),
                'median_ms':         round(_percentile(durs, 0.5), 1),
                'p95_ms':            round(_percentile(durs, 0.95), 1),
                'min_ms':            round(min(durs), 1),
                'max_ms':            round(max(durs), 1),
                'zero_result_count': entry['zero_result_count'],
                'bucket_0':          bucket_0,
                'bucket_1_9':        bucket_1_9,
                'bucket_10_99':      bucket_10_99,
                'bucket_100plus':    bucket_100plus,
                'corpus_genizah':    entry['corpus_counts'].get('genizah', 0),
                'corpus_local':      entry['corpus_counts'].get('local', 0),
                'corpus_all':        entry['corpus_counts'].get('all', 0),
            }
        if not perf_summary:
            _perf_accumulator.clear()
            return
        # Attach the per-process session_id so this summary joins to session_start /
        # session_end (WR-01). Falls back to the distinct/install id if the session id
        # was never set (e.g. a flush before session mint, or in tests).
        try:
            with _state_lock:
                sid = _session_id or _current_distinct_id or _install_id or ''
        except Exception:
            sid = ''
        sample_n = _perf_env_int('GENIZAH_PERF_SAMPLE_N', default=1, minimum=1)
        track_performance(
            DesktopEvent.SESSION_PERF,
            duration_ms=0.0,    # not meaningful for summary; required by track_performance sig
            perf_summary=perf_summary,
            session_id=sid,
            sample_n=sample_n,
            action=flush_reason,   # 'action' already allowlisted; reuse for flush_reason (KQ-8)
        )
        # D-06: reset so next window is independent (no double-count across flush intervals)
        _perf_accumulator.clear()
        global _perf_last_flush_time
        _perf_last_flush_time = time.monotonic()
    except Exception:
        logger.debug('telemetry: _flush_perf_summary() silently failed', exc_info=True)


def flush_perf_if_due(flush_interval_secs: float = 1800.0) -> None:
    """Flush the perf accumulator only if enough time has elapsed (D-04/D-05 periodic).

    Called from GenizahGUI._maybe_flush_perf_summary() on the UI thread.
    Default interval 1800s (30 min); override via GENIZAH_PERF_FLUSH_INTERVAL env var.
    Never raises.
    """
    try:
        interval = _perf_env_float(
            'GENIZAH_PERF_FLUSH_INTERVAL',
            default=flush_interval_secs,
            minimum_exclusive=0.0,
        )
        if time.monotonic() - _perf_last_flush_time >= interval:
            _flush_perf_summary(flush_reason='periodic')
    except Exception:
        logger.debug('telemetry: flush_perf_if_due() silently failed', exc_info=True)


def flush_perf_unconditionally() -> None:
    """Flush the perf accumulator regardless of elapsed time (D-09 close flush).

    Called from GenizahGUI.closeEvent() on the UI thread. Never raises.
    """
    try:
        _flush_perf_summary(flush_reason='close')
    except Exception:
        logger.debug('telemetry: flush_perf_unconditionally() silently failed', exc_info=True)


# ---------------------------------------------------------------------------
# Module initialization — load consent state (which wires the key IFF consented)
# ---------------------------------------------------------------------------
# F1/Codex: do NOT wire the capture key unconditionally at import. The shared
# transport is ungated, so a key present before opt-in would let ungated
# emitters (NLI circuit-breaker) POST without consent. _load_consent_state()
# wires the key only when the persisted consent flag is True.
_load_consent_state()      # Populates _enabled/_install_id/_current_distinct_id (+ wires key iff consented)


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
    # Re-ask gate + bookkeeping (SEED-031)
    'should_reask_consent',
    'record_consent_ask',
    'set_never_ask',
    # Phase 115 public perf API
    'accumulate_performance',
    'flush_perf_if_due',
    'flush_perf_unconditionally',
    '_clear_perf_accumulator',
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
    # Re-ask config-key constants + policy (SEED-031)
    'TELEMETRY_LAST_ASKED_VERSION_KEY',
    'TELEMETRY_LAST_ASKED_TS_KEY',
    'TELEMETRY_ASK_COUNT_KEY',
    'TELEMETRY_NEVER_ASK_KEY',
    'REASK_COOLDOWN_DAYS',
    'REASK_MAX_ASKS',
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
    # Phase 113 crash-path (test seam — internal)
    '_is_enabled_nolock',
    '_make_crash_props',
    '_emit_crash_direct',
    '_is_in_app_frame',
    '_frame_basename',
    '_APP_SOURCE_ROOTS',
    '_APP_SOURCE_FILES',
    '_EXCLUDED_PATH_SEGMENTS',
    '_GENERIC_BASENAMES',
    '_TRACK_FORBIDDEN_EVENTS',
    # Phase 113 Plan 03 — native crash helpers
    '_NATIVE_CRASH_LABELS',
    '_classify_native_crash',
    '_emit_native_crash',
    '_emit_pending_native_crash',
    '_setup_faulthandler',
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

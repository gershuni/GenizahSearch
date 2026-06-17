# Phase 111: Telemetry Foundation — Research

**Researched:** 2026-06-14
**Domain:** Opt-in PostHog telemetry foundation for a PyQt6 frozen-binary desktop app — consent gate, scrubber, allowlist, identity hooks, and backward-compatible `shared/posthog_server.py` additions
**Confidence:** HIGH — all claims synthesized from direct codebase reads (current session), verified line numbers, and the milestone's prior research artifacts (ARCHITECTURE.md, STACK.md, PITFALLS.md, POSTHOG-PROJECT-DECISION.md, REQUIREMENTS-CODEX-CRITIQUE.md). No web searches required — domain is fully characterized by existing research.

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Desktop sends to the EXISTING shared web PostHog project (id 134161, EU). No separate project.
- **D-01a:** Desktop embeds the same publishable key the web app already uses (`web/main.py:794` — `_posthog_key = os.environ.get('POSTHOG_API_KEY', '')`). Web/desktop separation is by `platform=desktop` base property + `desktop_` event-name namespace prefix.
- **D-02:** Phase 111 builds against the env override with a PLACEHOLDER publishable-key constant. The real `phc_...` key drops in before Phase 114.
- **D-03:** The publishable key + host live as MODULE CONSTANTS in `desktop/telemetry.py`, overridable via `GENIZAH_TELEMETRY_KEY` (+ `GENIZAH_TELEMETRY_HOST`). Write-only; safe to embed; never logged.
- **D-04:** Consent gate lives ONLY in `desktop/telemetry.py`. `shared/posthog_server.py` stays UNGATED and gains only neutral, backward-compatible additions (default `distinct_id` setter, `_flush_before_exit`, optional `_scrub_hook`, queue-drain helper). Existing web / NLI-circuit-breaker callers and the 5 `_event_queue` monkeypatches are UNAFFECTED.
- **D-05:** `desktop/telemetry.py` is the ONLY permitted path from `desktop/` to `enqueue_event`. Enforced by a PRIV-03 AST guard mirroring `tests/test_no_raw_storage_access.py`.
- **D-06:** A dev-only `--telemetry-selftest` CLI flag emits one throwaway event to verify the pipeline. Gated so it never fires in normal use.
- **D-07:** Desktop identity is ALIGNED with the web app. Logged-in → `distinct_id = Supabase user.id` (exact match to `web/auth_state.py:160-170`). Logged-out → anonymous per-install `uuid4`. On login, emit `$identify` with `$anon_distinct_id = <per-install uuid>` to alias/merge. On logout, reset to anonymous per-install id.
- **D-08:** Desktop sends ONLY the bare `user.id` for identity — NEVER email/name. Web already attaches those to the shared person profile.
- **D-09:** Person-profile handling is split: anonymous (logged-out) events keep `$process_person_profile=false`; identified (logged-in) events use real PostHog person profiles.
- **D-10:** Every desktop event carries `platform=desktop` + `desktop_` event-name namespace prefix.

### Claude's Discretion

- Exact event-name registry shape, scrubber redaction regexes, precise property-allowlist contents, and `config.pkl` key names — within the locked constraints (allowlist-only for properties; fixed enum for event names; cached no-throw consent in any hook).

### Deferred Ideas (OUT OF SCOPE)

- First-run dialog + Settings toggle (Phase 112)
- Exception hooks (Phase 113)
- Usage events + login/logout wiring (Phase 114)
- Perf events (Phase 115)
- CI privacy gate (Phase 116)
- Web consent gate harmonization
- WEB-F1 (strip web `search_executed` query text)
- CONSENT-F1 (reset telemetry id affordance)
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CONSENT-01 | Telemetry is OFF by default; no event enqueued before consent loaded and true; startup order guarantees gate resolved first | `is_enabled()` reads `config.pkl` key absent → False; UUID lifecycle; test: 0 events on fresh config |
| CONSENT-05 | Anonymous per-install uuid4 minted on opt-in; Supabase user.id used for logged-in distinct_id; no hardware fingerprinting | `uuid.uuid4().hex` inside `set_consent(True)` only; identity section below |
| CONSENT-06 | Opting out stops emission immediately; per-install ID RETAINED on disk | `set_consent(False)` sets `telemetry_enabled=False`, keeps `telemetry_install_id` key |
| CONSENT-07 | Consent + identity state persists in `config.pkl` via `load_app_config`/`save_app_config` — no new file, no QSettings | Verified: `Config.CONFIG_FILE` = `LOCALAPPDATA/GenizahSearchPro/Index/config.pkl` at `genizah_core.py:2377` |
| INFRA-01 | Desktop events go to the shared web project (id 134161, EU); reuse web publishable key; `platform=desktop` + `desktop_` namespace | POSTHOG-PROJECT-DECISION.md; `web/main.py:794`; D-01/D-01a/D-10 |
| INFRA-02 | `desktop/telemetry.py` is the only public API for emitting desktop events; internally consent-gated + scrubbed; delegates to `shared/posthog_server.enqueue_event` | Architecture section; 8-callable API |
| INFRA-03 | `shared/posthog_server.py` gains backward-compatible additions only — no breaking changes for web/breaker consumers or 5 `_event_queue` monkeypatches | Current module read: zero existing gating, minimal addition surface |
| INFRA-04 | Zero new pip dependencies; no PyInstaller spec changes | STACK.md verified: all additions use stdlib + existing `requests` |
| INFRA-05 | Silent degradation when offline/air-gapped; memory-only events; never blocks UI thread | Fire-and-forget queue; `(connect_timeout, read_timeout)` tuple; no disk spool |
| PRIV-01 | Single structural scrubber sanitizes every outgoing payload | `_scrub_props()` design; always called before `enqueue_event` |
| PRIV-02 | Property allowlist: allowlisted props only; explicit exclusions for hostname/username/exe path/cwd/query/content/UI strings | Static allowlist design; `_ALLOWED_PROPS` frozenset |
| PRIV-06 | Event names drawn from a fixed registry/enum; never derived from query text, filenames, corpus labels, or visible UI strings | `DesktopEvent` enum design |
| IDENT-03 | Desktop sends only user.id on identify — never email/name | D-08; identity section; `$identify` event shape |
| IDENT-04 | `$identify`/alias/reset emitted through the SAME desktop chokepoint + raw `shared/posthog_server.py` queue; consent-gated | Hand-rolled `$identify` event shape; `identify()` + `reset_identity()` in the 8-callable API |
</phase_requirements>

---

## Summary

Phase 111 builds the safety infrastructure that makes it structurally impossible for any desktop event to reach PostHog before the consent gate, scrubber, and property/event allowlist exist and are tested. It ships zero user-facing events — all public callables are wired but no producers call them yet.

The codebase already has a production-proven fire-and-forget PostHog queue (`shared/posthog_server.py`, Phase 98) and a robust pickle-based config store (`genizah_core.load_app_config`/`save_app_config`, `Config.CONFIG_FILE`). Phase 111 adds exactly three things: (1) `desktop/telemetry.py`, the sole gated chokepoint with 8 public callables, the structural scrubber, the property allowlist, the event-name registry, and the identity mechanism; (2) backward-compatible neutral additions to `shared/posthog_server.py` that add a default `distinct_id` injection point, a `_scrub_hook` slot, a `_flush_before_exit` helper, and a queue-drain helper, leaving all existing callers and 5 test monkeypatches untouched; (3) tests proving the consent gate, scrubbing rules, allowlist enforcement, identity shapes, and PRIV-03 structural invariant.

The identity design is a significant addition beyond the earlier "anonymous-only" research: because the web app already calls `posthog.identify(user.id, ...)` on login (`web/auth_state.py:159-170`), desktop must emit `$identify` with the same raw Supabase `user.id` as `distinct_id` to merge cross-surface journeys in the shared project. This is hand-rolled over the raw queue (no SDK) as a `$identify` event with `$anon_distinct_id` aliasing.

**Primary recommendation:** Build `desktop/telemetry.py` strictly as described — 8 public callables, all passing through `_scrub_props()` before `enqueue_event`, with a cached no-throw `is_enabled()` gate. Add exactly the neutral additions to `shared/posthog_server.py`. Do not touch `genizah_app.py` wiring (that is Phases 112-114).

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Consent gate logic | `desktop/telemetry.py` | — | D-05: gate must be in `desktop/`; `shared/` stays ungated so web is unaffected |
| Event serialization + HTTP transport | `shared/posthog_server.py` (existing) | — | Reuse Phase 98 fire-and-forget queue; zero new deps |
| Consent + UUID persistence | `genizah_core.load_app_config`/`save_app_config` (existing) | — | `config.pkl` is the established durable pref store; session.json is wrong (cleared by crash recovery) |
| Structural scrubbing (PII removal) | `desktop/telemetry.py::_scrub_props` (internal) | optional `_scrub_hook` in `shared/posthog_server.py` (defence-in-depth) | Scrubbing runs IN the chokepoint before enqueue so no raw data ever enters the queue |
| Property allowlist enforcement | `desktop/telemetry.py::_validate_props` (internal) | Tests (PRIV-03 guard, property-allowlist tests) | Compile-time + runtime dual enforcement |
| Identity / `$identify` emission | `desktop/telemetry.py::identify()` + `reset_identity()` | `shared/posthog_server.enqueue_event` for transport | Hand-rolled `$identify` event; identity API is part of the 8 public callables |
| Distinct-id default injection | `shared/posthog_server.py::set_default_distinct_id()` (new, neutral) | — | Allows the desktop chokepoint to register the install UUID once; existing callers unaffected |
| Queue drain at process exit | `shared/posthog_server.py::_flush_before_exit()` (new, neutral) | — | Crash events die with the daemon thread without this; called from both crash hook (Phase 113) and atexit |
| AST chokepoint enforcement | `tests/test_telemetry_no_direct_posthog.py` (PRIV-03) | — | Mirror of `tests/test_no_raw_storage_access.py` Phase 87 pattern |

---

## Standard Stack

### Core (Phase 111 uses — zero new packages)

| Library | Version | Purpose | Source |
|---------|---------|---------|--------|
| `shared/posthog_server.py` (existing) | — | Fire-and-forget EU PostHog queue | [VERIFIED: direct file read] |
| `uuid` (stdlib) | Python 3.10+ | `uuid.uuid4()` for anonymous install ID | [VERIFIED: Python stdlib] |
| `re` (stdlib) | Python 3.10+ | Path-like string redaction in `_scrub_props` | [VERIFIED: Python stdlib] |
| `genizah_core.load_app_config`/`save_app_config` (existing) | — | `config.pkl` persistence | [VERIFIED: `genizah_core.py:2871-2891`] |
| `genizah_core.Config.CONFIG_FILE` (existing) | — | Path: `LOCALAPPDATA/GenizahSearchPro/Index/config.pkl` | [VERIFIED: `genizah_core.py:2377`] |
| `enum` (stdlib) | Python 3.10+ | `DesktopEvent` fixed event-name registry (PRIV-06) | [VERIFIED: Python stdlib] |

### Supporting (neutral additions to `shared/posthog_server.py`)

| Addition | Purpose | Impact on existing callers |
|----------|---------|---------------------------|
| `set_default_distinct_id(uid: str)` | Injects per-install UUID as default `distinct_id` for all events from desktop chokepoint | Zero — existing callers pass `distinct_id` explicitly; the default only applies when they do not |
| `_flush_before_exit(timeout: float = 0.5)` | Drains queue synchronously before process exit (crash hook + atexit) | Zero — new function, not called by existing code |
| `_scrub_hook: Callable[[dict], dict \| None] \| None` + `register_scrub_hook(fn)` | Optional second scrubbing layer called inside `enqueue_event` before queue put | Zero — hook is None by default; existing callers unaffected |
| `_drain_and_discard()` | Empties queue without sending (for opt-out, Phase 112 CONSENT-08) | Zero — new function, not called by existing code |

**Installation: no changes to requirements.txt, requirements-lock.txt, or GenizahSearchPro.spec.** [VERIFIED: STACK.md + direct spec inspection]

---

## Package Legitimacy Audit

> Phase 111 installs ZERO new packages. All capabilities use stdlib or the existing `shared/posthog_server.py` with backward-compatible additions.

No audit table required.

---

## Architecture Patterns

### System Architecture Diagram

```
genizah_app.py / gui_threads.py / desktop/*.py
        |
        |  (Phase 114+ wiring — NOT in Phase 111)
        |
        v
desktop/telemetry.py  [NEW in Phase 111]
        |
        |  is_enabled() — cached bool, reads config.pkl once
        |      -> False? return immediately (CONSENT-01 gate)
        |      -> True? continue
        |
        |  _validate_props(event_name, props)
        |      -> unknown event_name? raise or log+drop (PRIV-06)
        |      -> unknown prop key? drop key (PRIV-02)
        |
        |  _scrub_props(props)
        |      -> banned key? drop (PRIV-01)
        |      -> value is path-like? redact (PRIV-01)
        |      -> value too long? cap at 500 chars (PRIV-01)
        |
        |  _emit(event, scrubbed_props, distinct_id)
        |      -> add BASE_PROPS (platform=desktop, app_version)
        |      -> set $process_person_profile based on identity state
        |
        v
shared/posthog_server.enqueue_event(event, properties, distinct_id)
        |
        |  [D-04] stays UNGATED — web callers unaffected
        |  optional: _scrub_hook(payload) if registered (defence-in-depth)
        |
        v
_event_queue: Queue(maxsize=10000)  [daemon thread]
        |
        v
POST https://eu.i.posthog.com/capture
    PostHog project id 134161, org "Dicta"
    POSTHOG_API_KEY from env (web) / embedded constant (desktop)
```

### Recommended Project Structure

```
desktop/
    telemetry.py          [NEW] — consent gate, scrubber, allowlist, identity, 8 public callables

shared/
    posthog_server.py     [EXTEND, backward-compat] — add 4 neutral functions

tests/
    test_telemetry_consent_gate.py        [NEW] — CONSENT-01/05/06/07, IDENT-04
    test_telemetry_scrubbing.py           [NEW] — PRIV-01 scrubber rules
    test_telemetry_allowlist.py           [NEW] — PRIV-02/06 allowlist + event registry
    test_telemetry_identity.py            [NEW] — IDENT-03/04 identify/reset shapes
    test_telemetry_posthog_server_ext.py  [NEW] — INFRA-03 neutral additions
    test_telemetry_no_direct_posthog.py   [NEW] — PRIV-03 AST guard
```

### Pattern 1: The 8-Callable Public API

```python
# desktop/telemetry.py — public surface ONLY

def is_enabled() -> bool:
    """Cached no-throw consent check. Reads config.pkl at most once per process;
    re-reads only when set_consent() is called. Returns False when key is absent.
    MUST never raise (called from exception hooks in Phase 113)."""

def track(event: str, **props) -> None:
    """Gate-check consent, validate event name against DesktopEvent enum,
    validate props against _ALLOWED_PROPS, scrub, add BASE_PROPS, enqueue.
    Never raises. Non-blocking."""

def track_performance(
    event: str,
    duration_ms: float,
    result_count: int | None = None,
    **extra_props,
) -> None:
    """Specialised track for timed operations. Applies 1-in-N sampling.
    Phase 115 wires producers; Phase 111 just defines the callable."""

def track_error(context: str, exc: Exception) -> None:
    """Capture a handled (non-fatal) exception. context = short label.
    Never includes str(exc) in payload. Phase 113 wires; 111 defines."""

def get_install_id() -> str | None:
    """Return persisted anonymous UUID hex, or None if not opted in."""

def set_consent(enabled: bool) -> None:
    """Persist consent flag.
    On True:  mint uuid4 install_id if absent; update cached _enabled.
    On False: write telemetry_enabled=False; RETAIN telemetry_install_id;
              update cached _enabled. (CONSENT-06: id retained, not deleted)"""

def identify(user_id: str) -> None:
    """Emit $identify event with distinct_id=user_id, $anon_distinct_id=install_id.
    Switches subsequent _emit() calls to use user_id as distinct_id.
    Sets $process_person_profile=True for identified events (D-09).
    Consent-gated. (IDENT-03/04)"""

def reset_identity() -> None:
    """Reset distinct_id back to per-install uuid4 (mirrors web posthog.reset()).
    Sets $process_person_profile=False for anonymous events.
    Consent-gated. (IDENT-04)"""
```

**Source:** ARCHITECTURE.md + CONTEXT.md decisions; `identify()` and `reset_identity()` added per D-07/D-08/D-09/IDENT-03/IDENT-04. [VERIFIED: CONTEXT.md + REQUIREMENTS.md]

### Pattern 2: `config.pkl` Key Names

```python
# Keys written by desktop/telemetry.py into config.pkl (via save_app_config)

TELEMETRY_ENABLED_KEY   = 'telemetry_enabled'        # bool — absent = not yet asked = False
FIRST_RUN_SHOWN_KEY     = 'telemetry_first_run_shown' # bool — Phase 112 writes this
INSTALL_ID_KEY          = 'telemetry_install_id'      # str (uuid4.hex) — minted on opt-in; RETAINED on opt-out
CONSENT_TIMESTAMP_KEY   = 'telemetry_consent_ts'      # ISO-8601 str — lightweight audit trail
CONSENT_APP_VERSION_KEY = 'telemetry_consent_version' # str — app version at consent time
CONSENT_UI_VERSION_KEY  = 'telemetry_consent_ui_ver'  # str — e.g. "1" — CONSENT-03 audit trail
IDENTIFIED_USER_KEY     = 'telemetry_identified_user' # str | None — current logged-in user.id; None = anon
```

`config.pkl` path: `genizah_core.Config.CONFIG_FILE` = `os.path.join(INDEX_DIR, "config.pkl")` where `INDEX_DIR` is `LOCALAPPDATA/GenizahSearchPro/Index/` on a standard install. [VERIFIED: `genizah_core.py:2375-2377`]

### Pattern 3: Structural Scrubber `_scrub_props`

```python
# desktop/telemetry.py — internal, not exported

import re as _re

# Pre-compiled at module import time (not at call time — performance)
_PATH_RE = _re.compile(
    r'[A-Za-z]:\\[^\s,\"\']+'      # Windows absolute path: C:\...
    r'|/[^\s,\"\']{'r'3,}'          # POSIX absolute path: /home/...
    r'|\S+\.\w{2,4}\b',             # bare filename: foo.pdf
    _re.UNICODE
)
_HEBREW_TEXT_RE = _re.compile(r'[א-תיִ-פֿ]')  # no Hebrew content in values

_BANNED_KEYS = frozenset({
    'query', 'text', 'content', 'filename', 'path', 'filepath',
    'frame_locals', 'traceback_raw', 'search_term', 'query_text',
    'clean_query', 'search_text', 'shelfmark', 'sys_id', 'fl_id',
    'hostname', 'username', 'email', 'user_id', 'jwt', 'token',
    'executable', 'cwd', 'working_directory',
})

def _scrub_props(props: dict) -> dict:
    out = {}
    for k, v in props.items():
        # Drop banned keys (substring match on lowercased key)
        if any(b in k.lower() for b in _BANNED_KEYS):
            continue
        if isinstance(v, str):
            v = _PATH_RE.sub('[REDACTED]', v)
            # If value contains Hebrew text it may be query content — redact
            if _HEBREW_TEXT_RE.search(v):
                v = '[REDACTED]'
            v = v[:500]  # cap length
        out[k] = v
    return out
```

**Note:** The Hebrew content redaction is a defence-in-depth guard for the case where a future callsite accidentally passes a string value that contains query text. [ASSUMED — specific regex; planner should confirm the Hebrew range coverage is appropriate for the corpus]

### Pattern 4: Property Allowlist

```python
# desktop/telemetry.py — internal; defines PRIV-02

_ALLOWED_PROPS = frozenset({
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
    # Identity protocol extra props
    'context',
})

def _validate_props(event_name: str, props: dict) -> dict:
    """Drop any property key not in _ALLOWED_PROPS. Log dropped keys at DEBUG."""
    return {k: v for k, v in props.items() if k in _ALLOWED_PROPS}
```

The allowlist is intentionally permissive for Phase 111 (future event types have slots). Forbidden sources (PRIV-02): hostname, machine name, username, executable path, cwd, any visible UI string (window title, QAction text, tab title), any property derived from search/query content or My Library data. [CITED: REQUIREMENTS.md PRIV-02]

### Pattern 5: Event-Name Registry (PRIV-06)

```python
# desktop/telemetry.py

import enum

class DesktopEvent(str, enum.Enum):
    """Fixed registry of all permitted desktop event names (PRIV-06).

    No event name may be dynamically constructed. Every event emitted by
    the desktop app must be listed here. Phases 113-115 add new members
    via a controlled PR; no callsite may construct event names from
    user-visible strings.
    """
    # Foundation / identity (Phase 111)
    IDENTIFY          = 'desktop_identify'        # $identify alias event
    IDENTITY_RESET    = 'desktop_identity_reset'  # mirrors posthog.reset()

    # Crash (Phase 113)
    CRASH             = 'desktop_crash'
    PRIOR_CRASH       = 'desktop_prior_crash'     # CRASH-07 next-launch detection

    # Session / usage (Phase 114)
    SESSION_START     = 'desktop_session_start'
    SESSION_END       = 'desktop_session_end'
    TAB_ACTIVATED     = 'desktop_tab_activated'
    SEARCH_EXECUTED   = 'desktop_search_executed'
    FEATURE_OPENED    = 'desktop_feature_opened'

    # Performance (Phase 115)
    SESSION_PERF      = 'desktop_session_performance_summary'

    # Self-test (Phase 111 D-06, dev only)
    SELFTEST          = 'desktop_selftest'
```

`track()` rejects any string not in this enum (or raises `ValueError` in debug, drops silently in production). [CITED: REQUIREMENTS.md PRIV-06; CONTEXT.md D-10]

### Pattern 6: Hand-Rolled `$identify` Event Shape

PostHog person-aliasing requires a specific event shape. With no SDK, this is hand-rolled:

```python
# desktop/telemetry.py::identify() — IDENT-03/04

def identify(user_id: str) -> None:
    if not is_enabled():
        return
    install_id = get_install_id()
    if not install_id:
        return
    # Hand-rolled $identify event — merges pre-login anonymous history
    # into the logged-in person. Must match the PostHog /capture $identify spec.
    payload_props = {
        'platform': 'desktop',
        'app_version': _APP_VERSION,
        '$process_person_profile': True,         # D-09: identified events use profiles
        '$anon_distinct_id': install_id,         # aliases the per-install uuid into the person
        # NOTE: no email, no name — D-08 hard rule
    }
    payload_props = _scrub_props(_validate_props(DesktopEvent.IDENTIFY, payload_props))
    enqueue_event(
        '$identify',                  # PostHog's special identify event name
        payload_props,
        distinct_id=user_id,          # the Supabase user.id (IDENT-01 contract)
    )
    # Update module-level identity state
    _set_current_distinct_id(user_id, anonymous=False)
    # Persist identified user.id to config.pkl for crash-hook use (cached no-read in hook)
    save_app_config({IDENTIFIED_USER_KEY: user_id})
```

For logged-out anonymous events, `$process_person_profile=False` ensures PostHog's anonymous tier (cheaper ingestion, no person profile created). [CITED: REQUIREMENTS.md IDENT-03, D-09; PostHog `/capture` $identify spec]

### Pattern 7: Backward-Compatible Additions to `shared/posthog_server.py`

The current module (`shared/posthog_server.py`, 165 lines, read in this session) has no gating, no scrub hook, no default distinct_id override, and no flush helper. The NEUTRAL additions required for Phase 111:

```python
# shared/posthog_server.py — ADDITIONS ONLY (INFRA-03)

# Module-level state additions (after existing _dropped_events_lock)
_default_distinct_id: str | None = None
_default_distinct_id_lock = threading.Lock()

_scrub_hook: 'Callable[[dict], dict | None] | None' = None
_scrub_hook_lock = threading.Lock()


def set_default_distinct_id(uid: str | None) -> None:
    """Set a module-level default distinct_id injected when caller passes 'system'.

    Called once by desktop/telemetry.py after consent is granted. Web callers
    always pass an explicit distinct_id, so this never changes their behavior.
    Existing test monkeypatches target _event_queue, not distinct_id — unaffected.
    """
    global _default_distinct_id
    with _default_distinct_id_lock:
        _default_distinct_id = uid


def register_scrub_hook(fn: 'Callable[[dict], dict | None] | None') -> None:
    """Register an optional scrub hook called inside enqueue_event before queue put.

    fn(payload) -> payload (modified) or None (drop event).
    Defence-in-depth — desktop/telemetry.py's _scrub_props() is the PRIMARY layer.
    Web callers do not register a hook; this is a no-op for them.
    """
    global _scrub_hook
    with _scrub_hook_lock:
        _scrub_hook = fn


def _flush_before_exit(timeout: float = 0.5) -> None:
    """Drain the event queue synchronously before process exit.

    Called from:
    - sys.excepthook wrapper in desktop/telemetry.py (crash events — atexit does NOT
      run on unhandled exceptions in CPython)
    - atexit handler (clean exits)
    NOT called by web code — web is a long-lived process that doesn't need exit flush.
    Respects the hard timeout to avoid hanging at crash time.
    """
    # Implementation: drain queue items and POST them synchronously
    # (bypassing the daemon thread which may be about to die)
    ...


def _drain_and_discard() -> None:
    """Empty the in-memory queue WITHOUT sending events to PostHog.

    Called by desktop/telemetry.py::set_consent(False) to purge already-queued
    events when the user opts out (CONSENT-08, Phase 112).
    Not called by any web code.
    """
    while True:
        try:
            _event_queue.get_nowait()
        except queue.Empty:
            break
```

**Critical constraint:** The `enqueue_event` signature MUST NOT CHANGE. The 5 test monkeypatches target `_event_queue` directly; they do not care about the new functions. [VERIFIED: `shared/posthog_server.py` read; CONTEXT.md D-04]

### Anti-Patterns to Avoid

- **Putting the consent gate inside `shared/posthog_server.py`:** Would suppress web NLI-circuit-breaker telemetry when desktop user opts out. Gate MUST be only in `desktop/telemetry.py`. [D-04]
- **`session.json` for consent state:** Cleared by crash recovery + Reset My Library. Use `config.pkl` only. [VERIFIED: ARCHITECTURE.md; `genizah_core.py:2378`]
- **`uuid.uuid1()` for install ID:** Embeds MAC address — PII. Must be `uuid.uuid4()`. [PITFALLS #6]
- **Minting UUID at import time:** Must be minted only inside `set_consent(True)`. Pre-consent UUID undermines "default OFF" posture. [PITFALLS #4; STACK.md]
- **Including `str(exc_value)` in any payload:** Exception messages commonly contain query text, file paths, Hebrew terms. CRASH-04 explicitly forbids this. [PITFALLS #1]
- **Calling `enqueue_event` from `desktop/` except through `desktop/telemetry.py`:** Bypasses consent gate and scrubber. AST guard enforces this. [D-05, PRIV-03]
- **Sending email/name from desktop identify:** Only the bare Supabase `user.id` — web already attaches profile to the shared person. [D-08, IDENT-03]

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| PostHog HTTP transport | Custom `requests.post` wrapper | `shared/posthog_server.enqueue_event` | Production-proven, daemon-threaded, EU endpoint already configured, handles queue.Full |
| Config persistence | New file / QSettings | `genizah_core.load_app_config`/`save_app_config` | Existing pickle store; survives crashes + updates; already used by all other pref storage |
| Event-name enum | Ad-hoc string constants | `DesktopEvent(str, enum.Enum)` | PRIV-06 — enum prevents dynamic name construction; Python raises `ValueError` on unknown members |
| Scrub regex compilation | Inline `re.compile()` per call | Module-level pre-compiled `_PATH_RE` + `_HEBREW_TEXT_RE` | Exception hooks must complete in < 50ms; per-call regex compilation would add 1-5ms per crash event |
| AST guard | Custom file scanner | Mirror `tests/test_no_raw_storage_access.py` pattern exactly | Pattern is established, proven, and CI-integrated; don't reinvent |

---

## Common Pitfalls

### Pitfall 1: Pre-Consent Emission (CONSENT-01)

**What goes wrong:** `enqueue_event` called from startup code before `config.pkl` is read, or from an async timer that fires before the consent dialog is shown.

**Why it happens:** `GenizahGUI.__init__` has a complex multi-phase startup; session restore, auto-rescan, and tab initialization all happen before any dialog. If any Phase 114+ callsite fires at init time without the gate, it escapes consent.

**How to avoid:** `is_enabled()` must cache the consent value from `config.pkl` once at module import time and return `False` when the key is absent. Every `track()` call — without exception — checks `is_enabled()` first. The `_enabled` cache is only updated by `set_consent()`, never by the caller. Test: 0 events enqueued on a fresh (empty) `config.pkl`.

**Warning signs:** PostHog shows `desktop_session_start` events from a `distinct_id` that was later seen with an opt-out signal.

### Pitfall 2: `_flush_before_exit` Placement (Phase 111 vs Phase 113)

**What goes wrong:** `_flush_before_exit` is deferred to Phase 113 (crash hooks) thinking it only matters for crashes. But the Phase 113 exception hook implementation depends on calling `_flush_before_exit` from inside the hook — and the hook depends on the flush function existing in `shared/posthog_server.py`.

**How to avoid:** `_flush_before_exit` and `_drain_and_discard` are INFRA additions — they belong in Phase 111 as part of the `shared/posthog_server.py` neutral additions, not Phase 113. Phase 113 only calls them; Phase 111 defines them. [ROADMAP Phase 111 SC#5]

### Pitfall 3: `_scrub_hook` Called After Queue Put

**What goes wrong:** The scrub hook is registered in `shared/posthog_server.py` and called in the drain thread (after queue get), meaning raw data lives in the queue between enqueue and scrub.

**How to avoid:** The scrub hook MUST be called inside `enqueue_event` BEFORE `_event_queue.put_nowait(payload)`. Raw data must never enter the queue. `desktop/telemetry.py::_scrub_props()` is the primary layer; the shared-module `_scrub_hook` is defence-in-depth called at the same point. [ARCHITECTURE.md §5]

### Pitfall 4: Opt-Out Leaves Queued Events

**What goes wrong:** User clicks "Opt out" in Settings (Phase 112). The five events queued before the click are drained and sent by the background thread after the opt-out is written.

**How to avoid:** `_drain_and_discard()` must be in `shared/posthog_server.py` by Phase 111 so Phase 112's `set_consent(False)` can call it. The Phase 111 `set_consent(False)` implementation must call `_drain_and_discard()`. [CONSENT-08 note: CONSENT-08 is gated on Phase 112 for the UX, but the mechanism lands in Phase 111]

### Pitfall 5: Identity State Lost After Crash

**What goes wrong:** Phase 113's crash hook calls `is_enabled()` — but if the crash happens mid-login and `_current_distinct_id` lives only in a module-level variable, the hook may use the wrong `distinct_id`.

**How to avoid:** `identify()` must persist `IDENTIFIED_USER_KEY` to `config.pkl` immediately. `is_enabled()` and `_get_current_distinct_id()` read from `config.pkl` cache (updated on write), not just from in-memory state. The in-memory cache is the fast path; `config.pkl` is the ground truth. [PITFALLS #5 opt-out race; CONTEXT.md CONSENT-05]

### Pitfall 6: The `$identify` vs `identify` Event Name Confusion

**What goes wrong:** The PostHog `$identify` protocol event uses the literal string `'$identify'` as the event name — this is a PostHog-internal protocol event, NOT a member of `DesktopEvent`. If `track()` is used to emit it and the name is validated against `DesktopEvent`, it would be rejected.

**How to avoid:** `identify()` calls `enqueue_event` DIRECTLY with `'$identify'` as the event name (bypassing the `track()` name-validation), after performing its own scrub and allowlist validation on the properties. This is the only case where `desktop/telemetry.py` calls `enqueue_event` without going through `track()` — and it is the only such exception. [IDENT-04; PostHog `/capture` spec]

---

## Runtime State Inventory

> Phase 111 is a greenfield new-module phase. No rename/refactor. No existing telemetry state to migrate. This section is SKIPPED — not applicable.

---

## Code Examples

### Consent lifecycle (CONSENT-01/05/06/07)

```python
# desktop/telemetry.py

import threading
import uuid
from genizah_core import load_app_config, save_app_config

_enabled: bool = False
_enabled_lock = threading.Lock()
_install_id: str | None = None

def _load_consent_state() -> None:
    """Called once at module import. Populates module-level cache from config.pkl."""
    global _enabled, _install_id
    cfg = load_app_config()
    with _enabled_lock:
        _enabled = bool(cfg.get('telemetry_enabled', False))
    _install_id = cfg.get('telemetry_install_id')  # None if never opted in

_load_consent_state()

def is_enabled() -> bool:
    with _enabled_lock:
        return _enabled

def set_consent(enabled: bool) -> None:
    global _enabled, _install_id
    updates: dict = {'telemetry_enabled': enabled}
    if enabled:
        if not _install_id:
            _install_id = uuid.uuid4().hex  # mint only on first opt-in
            updates['telemetry_install_id'] = _install_id
        # Consent audit trail (CONSENT-03)
        from datetime import datetime, timezone
        updates['telemetry_consent_ts'] = datetime.now(timezone.utc).isoformat()
        updates['telemetry_consent_version'] = _APP_VERSION
        updates['telemetry_consent_ui_ver'] = '1'
    # CONSENT-06: do NOT delete telemetry_install_id on opt-out — retain it
    save_app_config(updates)
    with _enabled_lock:
        _enabled = enabled
    if not enabled:
        _drain_and_discard()  # purge queued events (CONSENT-08 mechanism)
```

**Source:** ARCHITECTURE.md §2; CONTEXT.md D-07; REQUIREMENTS.md CONSENT-06. [VERIFIED: `genizah_core.py:2871-2891`]

### Neutral addition to `shared/posthog_server.py` (INFRA-03)

```python
# shared/posthog_server.py — modification to enqueue_event
# Source: ARCHITECTURE.md §5; CONTEXT.md D-04

def enqueue_event(
    event: str,
    properties: dict,
    distinct_id: str = 'system',
) -> None:
    global _dropped_events
    # Resolve distinct_id from module default if caller passed 'system'
    if distinct_id == 'system':
        with _default_distinct_id_lock:
            if _default_distinct_id is not None:
                distinct_id = _default_distinct_id
    _start_drain_thread_once()
    try:
        payload = {
            'event': event,
            'distinct_id': distinct_id,
            'properties': dict(properties) if properties else {},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        # Optional scrub hook (defence-in-depth; primary scrubbing in desktop/telemetry.py)
        with _scrub_hook_lock:
            hook = _scrub_hook
        if hook is not None:
            try:
                payload = hook(payload)
                if payload is None:
                    return  # hook elected to drop this event
            except Exception:
                return  # if scrub hook fails, drop event rather than risk sending raw
        try:
            _event_queue.put_nowait(payload)
        except queue.Full:
            with _dropped_events_lock:
                _dropped_events += 1
    except Exception:
        logger.debug('posthog_server.enqueue_event silently dropped', exc_info=True)
```

**The 5 existing test monkeypatches target `_event_queue` directly — this change does not affect them.** [VERIFIED: `shared/posthog_server.py` read; monkeypatches do not inspect the function body]

---

## Validation Architecture

> `workflow.nyquist_validation` is `true` in `.planning/config.json` — section required.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pytest.ini` or `pyproject.toml` (existing) |
| Quick run command | `pytest tests/test_telemetry_*.py -x` |
| Full suite command | `pytest tests/ -x --ignore=tests/test_join_workbench_construct.py` (Qt headless guard) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CONSENT-01 | Zero events enqueued before consent is loaded and true | unit | `pytest tests/test_telemetry_consent_gate.py::test_no_events_before_consent -x` | ❌ Wave 0 |
| CONSENT-01 | `is_enabled()` returns False when `telemetry_enabled` key absent from config.pkl | unit | `pytest tests/test_telemetry_consent_gate.py::test_is_enabled_false_on_absent_key -x` | ❌ Wave 0 |
| CONSENT-05 | UUID minted only inside `set_consent(True)`, not at import time | unit | `pytest tests/test_telemetry_consent_gate.py::test_uuid_minted_on_opt_in_only -x` | ❌ Wave 0 |
| CONSENT-05 | Generated UUID is uuid4 format (version bit `4`) | unit | `pytest tests/test_telemetry_consent_gate.py::test_install_id_is_uuid4 -x` | ❌ Wave 0 |
| CONSENT-06 | `set_consent(False)` sets flag to False; install_id key RETAINED in config.pkl | unit | `pytest tests/test_telemetry_consent_gate.py::test_opt_out_retains_install_id -x` | ❌ Wave 0 |
| CONSENT-07 | Consent state round-trips through config.pkl (survives re-import) | unit | `pytest tests/test_telemetry_consent_gate.py::test_consent_persists_across_reload -x` | ❌ Wave 0 |
| PRIV-01 | `_scrub_props` strips banned keys (query, path, filename...) | unit | `pytest tests/test_telemetry_scrubbing.py::test_banned_keys_stripped -x` | ❌ Wave 0 |
| PRIV-01 | `_scrub_props` redacts Windows absolute paths in string values | unit | `pytest tests/test_telemetry_scrubbing.py::test_windows_path_redacted -x` | ❌ Wave 0 |
| PRIV-01 | `_scrub_props` redacts POSIX absolute paths | unit | `pytest tests/test_telemetry_scrubbing.py::test_posix_path_redacted -x` | ❌ Wave 0 |
| PRIV-01 | `_scrub_props` redacts bare filenames (foo.pdf) | unit | `pytest tests/test_telemetry_scrubbing.py::test_bare_filename_redacted -x` | ❌ Wave 0 |
| PRIV-01 | `_scrub_props` redacts Hebrew text (potential query content) | unit | `pytest tests/test_telemetry_scrubbing.py::test_hebrew_text_redacted -x` | ❌ Wave 0 |
| PRIV-02 | `_validate_props` drops any key not in `_ALLOWED_PROPS` | unit | `pytest tests/test_telemetry_allowlist.py::test_unknown_prop_dropped -x` | ❌ Wave 0 |
| PRIV-02 | `_validate_props` explicitly blocks hostname, username, cwd, exe path | unit | `pytest tests/test_telemetry_allowlist.py::test_forbidden_env_props_blocked -x` | ❌ Wave 0 |
| PRIV-06 | `track()` rejects event names not in DesktopEvent enum | unit | `pytest tests/test_telemetry_allowlist.py::test_unknown_event_name_rejected -x` | ❌ Wave 0 |
| PRIV-06 | All `DesktopEvent` members have `desktop_` prefix | unit | `pytest tests/test_telemetry_allowlist.py::test_all_events_have_desktop_prefix -x` | ❌ Wave 0 |
| IDENT-03 | `identify()` emits `$identify` event with user_id as distinct_id and no email/name | unit | `pytest tests/test_telemetry_identity.py::test_identify_no_email_name -x` | ❌ Wave 0 |
| IDENT-04 | `identify()` is consent-gated (no emission when `is_enabled()` False) | unit | `pytest tests/test_telemetry_identity.py::test_identify_consent_gated -x` | ❌ Wave 0 |
| IDENT-04 | `reset_identity()` resets distinct_id to install_id, sets `$process_person_profile=False` | unit | `pytest tests/test_telemetry_identity.py::test_reset_identity_reverts_to_anon -x` | ❌ Wave 0 |
| INFRA-03 | `set_default_distinct_id` does not break existing `enqueue_event` calls that pass `distinct_id='system'` | unit | `pytest tests/test_telemetry_posthog_server_ext.py::test_existing_callers_unaffected -x` | ❌ Wave 0 |
| INFRA-03 | `_drain_and_discard` empties queue without any POST | unit | `pytest tests/test_telemetry_posthog_server_ext.py::test_drain_and_discard_no_post -x` | ❌ Wave 0 |
| INFRA-03 | `_flush_before_exit` drains and POSTs queued events within timeout | unit | `pytest tests/test_telemetry_posthog_server_ext.py::test_flush_before_exit_drains -x` | ❌ Wave 0 |
| PRIV-03 (Phase 116) | AST guard: no file under `desktop/` except `desktop/telemetry.py` calls `enqueue_event` directly | static | `pytest tests/test_telemetry_no_direct_posthog.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit:** `pytest tests/test_telemetry_*.py -x`
- **Per wave merge:** `pytest tests/ -x`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/test_telemetry_consent_gate.py` — covers CONSENT-01/05/06/07
- [ ] `tests/test_telemetry_scrubbing.py` — covers PRIV-01
- [ ] `tests/test_telemetry_allowlist.py` — covers PRIV-02/06
- [ ] `tests/test_telemetry_identity.py` — covers IDENT-03/04
- [ ] `tests/test_telemetry_posthog_server_ext.py` — covers INFRA-03
- [ ] `tests/test_telemetry_no_direct_posthog.py` — covers PRIV-03 AST guard
- [ ] `desktop/telemetry.py` — the module itself

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Not applicable (telemetry is not an auth surface) |
| V3 Session Management | Partial | Session ID = per-process uuid4; no session fixation risk |
| V4 Access Control | No | Write-only ingest key; no read path |
| V5 Input Validation | Yes | `_scrub_props` + `_validate_props` at every emission point |
| V6 Cryptography | No | No crypto; UUID is not secret; ingest key is write-only public |

### Known Threat Patterns for This Stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| PII leak via frame locals in traceback | Information Disclosure | `_scrub_props` strips banned keys; `capture_locals=False` (never use); `str(exc_value)` never included |
| Query text in crash event via exception message | Information Disclosure | `exc.args` never included; only `type(exc).__name__` + scrubbed module + line |
| My Library file path in crash payload | Information Disclosure | `_PATH_RE` redaction in `_scrub_props`; `filename`/`path` in `_BANNED_KEYS` |
| Pre-consent emission | Repudiation | `is_enabled()` gate on EVERY public callable; cached no-throw read |
| Ingest key abuse (fake events from external actor) | Tampering | Publishable key is write-only by design; PostHog ingestion filters; rotation procedure in INFRA-06 runbook |
| UUID v1 = hardware fingerprint | Privacy | Use `uuid.uuid4()` only; verify with `uuid_str[14] == '4'` assertion in test |
| Hebrew content leaking as corpus label | Information Disclosure | `_HEBREW_TEXT_RE` guard in scrubber redacts any Hebrew string value |

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Separate desktop PostHog project | Shared web project (id 134161, EU) | 2026-06-14 (D-01 reversal) | Cross-surface journey stitching works; no billing friction; web key reused |
| Anonymous no-account-linkage design | Identity-aligned with web app (`user.id` as `distinct_id`) | 2026-06-14 (D-07) | Web + desktop journeys merge in PostHog; desktop sends only bare user.id (no PII beyond that) |
| Anonymous events only, `$process_person_profile=false` everywhere | Split: anonymous = False, identified = True | 2026-06-14 (D-09) | Identified journey works; anonymous events stay on cheaper anonymous tier |
| Install ID deleted on opt-out (earlier recommendation) | Install ID RETAINED on opt-out (CONSENT-06) | 2026-06-14 (user decision) | Re-opt-in preserves install continuity |
| posthog Python SDK | Raw `shared/posthog_server.py` queue (Phase 98) | Phase 98 design | Zero new deps; `capture_exception_code_variables` PII risk eliminated |

**Deprecated/outdated (do not use):**
- `uuid.uuid1()`: encodes MAC address — never use for install ID.
- `QSettings` for consent storage: PITFALLS.md §7 concern was resolved in favour of `config.pkl`; QSettings used only in `desktop/my_library_tab.py` for that component specifically.
- `session.json` for any telemetry state: cleared by crash recovery.
- Separate PostHog project: reversed 2026-06-14.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | Hebrew content in string values (`[א-תיִ-פֿ]`) redaction covers all realistic query content — the specific Unicode range may miss some extended Hebrew characters (Supplementary Hebrew block) | Pattern 3 `_HEBREW_TEXT_RE` | A narrow range means some Hebrew query text could pass through; defence-in-depth via `_BANNED_KEYS` 'query'/'text' still catches the most likely vectors |
| A2 | `_APP_VERSION` constant can be derived from `version.py` at module import time without circular imports | Pattern 6 `identify()` example | If `version.py` imports anything from `desktop/` there would be a circular import; verify at plan time |
| A3 | The `_flush_before_exit` implementation can drain the queue synchronously (direct HTTP POST bypassing daemon thread) within a 0.5s timeout without triggering the existing `requests.post(timeout=2.0)` read timeout | Pattern 7 neutral additions | If the implementation re-uses the same drain thread rather than doing a direct POST, it will fail when the daemon thread is already dead; implementation must do direct synchronous POSTs |

**If this table is empty:** All claims in this research were verified or cited — no user confirmation needed. The above 3 items are LOW-risk assumptions with strong mitigations.

---

## Open Questions (RESOLVED)

1. **`_APP_VERSION` import path** — **RESOLVED:** `from version import APP_VERSION` is safe. `version.py` has no imports (PATTERNS.md verified — two-line file), so there is no circular-import risk at frozen-binary startup; `genizah_app.py:28` already imports it the same way. No fallback needed.
   - What we knew: `version.py` exists and `scripts/bump_version.py` updates it; `genizah_app.py` imports it.

2. **`_drain_and_discard` vs CONSENT-08 phasing** — **RESOLVED:** Phase 111 defines `_drain_and_discard` in `shared/posthog_server.py` (Plan 01) AND wires the `set_consent(False)` → `_drain_and_discard()` call in `desktop/telemetry.py` (Plan 02). Safe because Phase 112 is the first time a real user can opt out (no consent UX exists until 112), and a drain on an empty queue is a no-op.
   - What we knew: CONSENT-08 is assigned to Phase 112 in REQUIREMENTS.md traceability, but the drain mechanism must exist in `shared/posthog_server.py` by Phase 111 for Phase 112 to call it.

3. **`$identify` event in `DesktopEvent` enum** — **RESOLVED:** `DesktopEvent` includes `IDENTIFY = '$identify'` as a special-case member (the `$` prefix is PostHog's protocol-event convention, intentionally NOT a `desktop_`-prefixed name). `track()` enforces the enum; `identify()` uses `DesktopEvent.IDENTIFY.value` directly so the protocol event stays traceable rather than a free-floating string (Plan 02 Task 3).
   - What we knew: `identify()` must emit PostHog's protocol event `'$identify'` (literal string) while PRIV-06 requires all event names come from the fixed registry.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `shared/posthog_server.py` | INFRA-02/03 | ✓ | existing, 165 lines | — |
| `genizah_core.load_app_config` / `save_app_config` | CONSENT-07 | ✓ | existing (`genizah_core.py:2871-2891`) | — |
| `genizah_core.Config.CONFIG_FILE` | CONSENT-07 | ✓ | `LOCALAPPDATA/GenizahSearchPro/Index/config.pkl` | — |
| `uuid` stdlib | CONSENT-05 | ✓ | Python 3.10+ | — |
| `enum` stdlib | PRIV-06 | ✓ | Python 3.10+ | — |
| `re` stdlib | PRIV-01 | ✓ | Python 3.10+ | — |
| PostHog EU endpoint | INFRA-01 | ✓ | `https://eu.i.posthog.com/capture` (hardcoded in posthog_server.py:44) | — |
| `POSTHOG_API_KEY` env var | INFRA-01 | set on server (web); placeholder in Phase 111 desktop | — | Phase 111 uses placeholder constant; real key before Phase 114 |

**Missing dependencies with no fallback:** None.

**Step 2.6: No blocking external dependencies. Phase 111 is pure Python + stdlib + existing project modules.**

---

## Sources

### Primary (HIGH confidence — direct codebase reads in this session)

- `shared/posthog_server.py` — full 165-line read: Queue(maxsize=10000) at L47; daemon thread at L132; EU endpoint at L44; `enqueue_event` signature at L65; `_event_queue` module var at L47; `_reset_for_tests` at L140; `__all__` at L158
- `genizah_core.py:2374-2377` — `Config.CONFIG_FILE = os.path.join(INDEX_DIR, "config.pkl")`; `Config.SESSION_FILE = os.path.join(INDEX_DIR, "session.json")`
- `genizah_core.py:2871-2891` — `load_app_config()` / `save_app_config()` exact implementation
- `genizah_app.py:148-170` — `_setup_crash_handler()` exact implementation: writes `crash_log.txt`, chains to `sys.__excepthook__`; installed at module-level (L170)
- `web/auth_state.py:159-170` — `_posthog_identify(cls, user, profile)`: emits `posthog.identify(uid, {email, name})` via `ui.run_javascript`; called from `update_user()` at L156
- `web/main.py:794-808` — `_posthog_key = os.environ.get('POSTHOG_API_KEY', '')` at L794; PostHog init with `eu.i.posthog.com` and `person_profiles: 'identified_only'` at L801-802
- `tests/test_no_raw_storage_access.py` — full read: AST scanner pattern for PRIV-03 guard to mirror
- `.planning/config.json` — `workflow.nyquist_validation: true` at root
- `genizah_app.py` grep for `_build_general_tab` — found at L2210 (confirmed line number current)

### Primary (HIGH confidence — existing milestone research artifacts)

- `.planning/research/ARCHITECTURE.md` — 8-callable API design, scrubber design, consent state patterns, exception hook chain design, integration points
- `.planning/research/STACK.md` — SDK vs raw queue verdict, PyInstaller zero-dep confirmation, `uuid.uuid4` mandate, key embedding safety
- `.planning/research/PITFALLS.md` — 13 pitfalls with phase mapping; all verified against codebase line numbers
- `.planning/research/POSTHOG-PROJECT-DECISION.md` — ONE shared project decision rationale; web identity contract confirmation
- `.planning/research/REQUIREMENTS-CODEX-CRITIQUE.md` — 13 Codex gap findings; 5 new requirements; PRIV-06 / CONSENT-08 / USAGE-06 / CRASH-07 / INFRA-06 provenance
- `.planning/research/SUMMARY.md` — cross-document synthesis; QSettings vs config.pkl resolution

### Primary (HIGH confidence — CONTEXT.md locked decisions)

- `.planning/phases/111-telemetry-foundation/111-CONTEXT.md` — D-01 through D-10; canonical references; code context

### Primary (HIGH confidence — REQUIREMENTS.md)

- `.planning/REQUIREMENTS.md` — all 40 v8.1.0 requirements; Phase 111 requirement traceability table

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — zero new packages; all capabilities verified in existing codebase
- Architecture: HIGH — all integration points verified by direct file reads (current session); line numbers confirmed current
- Pitfalls: HIGH — 13 pitfalls from prior research, all cross-referenced to real codebase lines; identity pitfalls (6) added for the D-07 reversal
- Identity mechanism: HIGH — web contract verified at `web/auth_state.py:159-170`; PostHog `$identify` spec is well-documented
- `shared/posthog_server.py` additions: HIGH — full module read; existing `__all__` and `_reset_for_tests` confirm the expected extension points

**Research date:** 2026-06-14
**Valid until:** 2026-08-14 (stable Python/PostHog patterns; `shared/posthog_server.py` line numbers may drift if Phase 98 follow-ups land)

**Note on prior research:** This RESEARCH.md SYNTHESIZES the existing milestone research artifacts (SUMMARY.md, ARCHITECTURE.md, STACK.md, PITFALLS.md, POSTHOG-PROJECT-DECISION.md, REQUIREMENTS-CODEX-CRITIQUE.md) rather than re-deriving from scratch. All line numbers in those artifacts have been verified against the current codebase in this session. The identity-alignment decisions (D-07/D-08/D-09) are NEW relative to the prior research and are fully incorporated here.

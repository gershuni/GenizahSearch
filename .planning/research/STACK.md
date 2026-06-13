# Stack Research: Desktop Telemetry (v8.1.0)

**Domain:** Opt-in PostHog telemetry + crash/error reporting for a PyQt6 frozen-binary desktop app
**Researched:** 2026-06-13
**Confidence:** HIGH (PostHog SDK verified via Context7 + PyPI; packaging verified via direct download + spec inspection; key safety verified via PostHog docs; exception hooks verified via official Python docs + PyQt-specific sources)

---

## Decision 1: PostHog Python SDK vs. Raw `shared/posthog_server.py` Queue

**Recommendation: DO NOT add the `posthog` SDK. Wrap and extend `shared/posthog_server.py` instead.**

### What the posthog SDK provides (version 7.18.3)

| Capability | SDK | Raw queue |
|-----------|-----|-----------|
| Internal batching (default: flush every 0.5s, max 100 events/batch) | YES | NO — one HTTP POST per event |
| Automatic retry with exponential backoff (via `backoff` dep) | YES | NO — silent drop on error |
| `capture_exception()` + `enable_exception_autocapture=True` | YES | NO |
| `before_send` hook (callable → event dict or None) | YES | NO |
| `posthog.disabled = True` / `posthog.disabled = False` runtime toggle | YES | Requires wrapper |
| EU host config (`host="https://eu.posthog.com"`) | YES | YES — already hardcoded to EU |
| Anonymous events (no `distinct_id` context) | YES | YES — uses `distinct_id='system'` |
| Feature flags + local evaluation | YES | NO |
| `shutdown()` / `flush()` for process-exit drain | YES | NO — daemon thread silently drops in-flight queue on exit |
| `super_properties` (appended to every event) | YES | NO — must include manually |
| `capture_exception_code_variables=True` (local variable values in tracebacks) | YES | NO |

### Why the raw queue is the right choice for THIS project

**Four reasons the SDK is overkill and introduces risk:**

1. **`capture_exception_code_variables=True` is a privacy hazard.** The SDK's `capture_exception` sends frame-local variable values to PostHog servers. For this app, local variables in a search-related frame can contain the user's query text, My Library file paths, or transcription snippets. The `before_send` hook scrubs properties dict entries but does NOT suppress the `$exception_list[].frames[].vars` payload unless you explicitly strip it. This is exactly the PII leak the milestone requirements prohibit. The raw queue forces you to construct exception payloads manually — no accidental variable capture.

2. **No feature flags needed.** Feature flags require a `personal_api_key` (a server-side secret) for local evaluation. The desktop app has no secret key. Cloud evaluation would add 100-400ms per flag check. There is no use case for feature flags in this milestone.

3. **One new transitive dependency (`backoff 2.2.1`, ~15 KB wheel).** The rest of the SDK's deps (`requests`, `typing-extensions`, `distro`) are already in requirements-lock.txt. `backoff` is not. The raw queue already uses `requests` directly. Adding `backoff` for retry logic is the only real gap — and it is unnecessary: the fire-and-forget design intentionally accepts silent drops on network failure (the drain loop in `posthog_server.py` swallows exceptions). At this app's event volume (~50 searches/day × 30 desktop users = ~1500 events/day), dropped events are not operationally meaningful.

4. **`shared/posthog_server.py` is already production-proven.** It drains the NLI circuit-breaker telemetry (Phase 98) successfully. Its API (`enqueue_event(event, properties, distinct_id)`) is exactly what is needed. Adding `posthog.disabled`-equivalent behavior requires only a module-level `_telemetry_enabled: bool` flag checked at `enqueue_event` entry.

**Verdict: extend the existing raw queue.** The SDK adds capabilities this project cannot use and introduces opt-out complexity (the SDK's `disabled` flag is set at `Posthog()` construction time and is not trivially changeable at runtime without re-instantiating — `posthog.disabled = True` works at module level but the `Posthog` instance approach used in production requires careful wiring for runtime opt-out toggles). The raw queue's opt-out is one boolean check.

**The one capability gap worth adding to the raw queue: `before_send` equivalent.** A simple `_scrub_hook: Callable | None` at the `enqueue_event` level covers the privacy requirement without the SDK.

---

## Recommended Stack

### Core Technologies

| Technology | Version | Purpose | Why Recommended |
|------------|---------|---------|-----------------|
| `shared/posthog_server.py` (existing) | — | Fire-and-forget event queue to EU PostHog | Already production-proven, web-independent, thread-safe. Needs 3 additions: opt-in gate, scrub hook, desktop-specific UUID distinct_id injection |
| `uuid` (stdlib) | 3.10+ | Generate anonymous per-install UUID | `uuid.uuid4()` is the correct choice — pure random, no MAC address, no PII. Store as text in `%LOCALAPPDATA%\GenizahSearchPro\telemetry_id.txt`. Generate ONLY when user opts in |
| `sys.excepthook` | stdlib | Catch unhandled main-thread exceptions | Standard Python global exception hook. MUST be installed after `QApplication.__init__()` — PyQt6's C++ layer has its own exception handling that can suppress Python exceptions in slot callbacks |
| `threading.excepthook` | stdlib (3.8+) | Catch unhandled exceptions in background threads (QThread excluded — see pitfall below) | Covers `SearchThread`, `LocalIndexerWorker`, and other `threading.Thread` subclasses. Python 3.8+ availability confirmed; project requires Python 3.10+ |
| `faulthandler` | stdlib | Low-level signal/segfault handler for native crashes (C extension crashes, PyMuPDF/Tantivy faults) | `faulthandler.enable()` at startup; writes stack trace to stderr/log file. Does NOT integrate with Python's exception system but captures crashes `sys.excepthook` cannot |
| `pathlib` / `os` (stdlib) | 3.10+ | Persist telemetry opt-in state and install UUID | Write opt-in flag to `%LOCALAPPDATA%\GenizahSearchPro\telemetry_opt_in.json` |

### Supporting Libraries (NEW additions required)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `posthog` SDK | **NOT recommended** | — | Do not add. See Decision 1. |
| `backoff` | **NOT recommended** | Retry logic for telemetry POSTs | Not needed — fire-and-forget accepts silent drops. Adding it adds a new transitive dep for no meaningful benefit at this event volume |

**Net new pip dependencies: ZERO.** All required capabilities use stdlib or the existing `shared/posthog_server.py` queue with extensions.

### Extensions to `shared/posthog_server.py`

Three additions to the existing module. All backward-compatible (existing callers — `shared/nli_circuit_breaker.py` and `web/api_hardening.py` — are unaffected):

| Addition | Purpose | Implementation |
|----------|---------|----------------|
| `_telemetry_enabled: bool = False` module flag + `set_telemetry_enabled(bool)` | Runtime opt-in/opt-out gate; `enqueue_event` no-ops when False | Simple module-level boolean; thread-safe read (GIL protects bool reads) |
| `_scrub_hook: Callable[[dict], dict | None] | None` + `register_scrub_hook(fn)` | `before_send` equivalent; lets the desktop layer strip search query text, file paths, frame vars from exception payloads | Called synchronously inside `enqueue_event` before queue put; if it raises, original event is dropped (not sent) |
| `set_default_distinct_id(uid: str)` | Injects the per-install UUID as default `distinct_id` for all events | Replaces the `'system'` default when user has opted in |

### New Desktop Module: `desktop/telemetry.py`

| Responsibility | Detail |
|----------------|--------|
| Opt-in state persistence | Read/write `%LOCALAPPDATA%\GenizahSearchPro\telemetry_opt_in.json` |
| Install UUID management | Generate `uuid.uuid4()` on first opt-in, persist to `%LOCALAPPDATA%\GenizahSearchPro\telemetry_id.txt`, never regenerate |
| Exception hook installation | `sys.excepthook`, `threading.excepthook`, QThread exception forwarding |
| Scrub hook registration | Strip `$exception_list[].frames[].vars`, redact file paths matching My Library directories, assert no search query text in payload |
| Consent dialog | Bilingual (EN+HE) first-run dialog; default OFF; also surfaced in Settings/About toggle |
| Event helpers | Thin wrappers: `track_tab_switch(tab)`, `track_search_started(mode, corpus)`, `track_crash(exc)` etc. |

---

## PyInstaller Packaging Impact

**Impact: minimal. No spec file changes needed.**

### Analysis

The `posthog` SDK is NOT being added. The raw queue (`shared/posthog_server.py`) is already included via the spec's `datas` tuple:

```python
('shared', 'shared'),  # already in GenizahSearchPro.spec line 9
```

The new `desktop/telemetry.py` module is discovered by PyInstaller's static analysis automatically because it is a pure Python module imported by `genizah_app.py`.

### Dep delta

| Dep | Already bundled? | Action |
|-----|-----------------|--------|
| `requests` | YES (in spec `hiddenimports` + lock file) | None |
| `uuid` | YES (stdlib) | None |
| `sys`, `threading`, `faulthandler` | YES (stdlib) | None |
| `pathlib`, `os`, `json` | YES (stdlib) | None |
| `posthog` SDK | NOT added | None |

**Bundle size delta: ~0 KB.** No new wheels, no new C extensions, no `collect_all(...)` entries.

### PyInstaller gotchas to avoid

1. **`__file__` is unreliable in frozen binaries.** Any telemetry code that tries to determine the installation directory via `__file__` will get a path inside the frozen executable's temp dir, not `%LOCALAPPDATA%`. Use `pathlib.Path(os.environ.get('LOCALAPPDATA', '')) / 'GenizahSearchPro'` for persistent storage. The codebase already handles this pattern correctly for other data (Tantivy index, joins.db).

2. **`faulthandler` log file path.** `faulthandler.enable(file=open(...))` must use an absolute path resolved at runtime, not a compile-time path. Resolve via `LOCALAPPDATA` at startup, before `faulthandler.enable()`.

3. **No `collect_all('posthog')` needed** because the SDK is not used. If the SDK were added, it would need `hiddenimports += ['posthog', 'posthog.client', 'posthog.consumer', 'posthog.request']` because the SDK uses internal imports that PyInstaller's static analysis may miss. Verified: no existing hook for `posthog` exists in `pyinstaller-hooks-contrib==2026.3` (confirmed by filesystem check).

---

## PostHog Project API Key Embedding

**Verdict: SAFE to embed in the distributed binary. This is standard practice.**

### Key type distinction

| Key type | Format | Purpose | Safe to distribute? |
|----------|--------|---------|---------------------|
| **Project API key** ("publishable key") | `phc_...` | Write-only event ingestion (`/capture` endpoint) | YES — by design |
| **Personal API key** | `phx_...` | Admin API, feature flag local eval, data read | NO — never embed |
| **Feature flags secret** (secure mode) | per-project | Server-side flag evaluation | NO — never embed |

The project API key (`phc_...`) is a **write-only ingest token**. It cannot be used to read events, query PostHog data, modify settings, or identify other users. PostHog's own documentation confirms this: "Every project has its own distinct write-only token, which you can use to initialize your integration." Browser-side PostHog JS (already shipped in this project's web app) embeds this same key in public HTML source — that is the intended use model.

This project already embeds `POSTHOG_API_KEY` in the web server's environment. The desktop binary embedding the same key (as a Python string constant, not an env var) is equivalent in security posture to the web app's `<script>posthog.init('phc_...')</script>`.

### Abuse considerations

Someone who extracts the key from the binary can only POST fake ingest events to your PostHog project. Mitigations if this becomes a problem (not needed now at this user scale):

- PostHog project-level ingestion filters (block events with unusual `distinct_id` patterns)
- Rotate the project API key — existing installs simply stop sending until updated (acceptable at this user count)

**Implementation:** embed as a Python string constant in `desktop/telemetry.py`, gated so it is never logged and never included in event properties. Do not use an env var — the frozen binary has no shell environment and `%POSTHOG_API_KEY%` would be empty for end users.

---

## Crash and Error Handling Primitives

### Coverage map

| Hook | What it covers | Limitation for PyQt6 |
|------|---------------|---------------------|
| `sys.excepthook(type, value, tb)` | Unhandled exceptions on the **main thread** that reach Python's top level | PyQt6 wraps slot invocations in C++ try/catch; Python exceptions raised in slots are often printed to stderr rather than propagated to `sys.excepthook`. Must also call `sys.__excepthook__` to preserve default behavior |
| `threading.excepthook(args)` | Unhandled exceptions in `threading.Thread` subclasses (Python threads only) | Does NOT cover `QThread` subclasses — their `run()` is called from C++ and exceptions are suppressed by Qt's C++ layer, not Python's |
| `QThread.run()` wrap | Unhandled exceptions in `QThread` subclasses | Requires explicit `try/except` in each `QThread.run()` override. The codebase already does this in many workers; the telemetry phase should audit and enforce it |
| `faulthandler` | Native crashes: C extension segfaults, stack overflow, `SIGSEGV` from Tantivy/PyMuPDF/PyQt6 | Only writes to a file/stderr; does not call Python code; cannot POST to PostHog; useful for crash log correlation |
| `QApplication.notify()` override | ALL Qt events and slot dispatches | Can catch Python exceptions Qt would otherwise swallow. Override `notify()`, call `super().notify()` in try/except; gives most comprehensive Qt coverage |

### Recommended combination

Install ALL of these at app startup in `desktop/telemetry.py::install_exception_hooks()`:

1. `sys.excepthook` — main thread fallback
2. `threading.excepthook` — background Python threads (`SearchThread`, `LocalIndexerWorker`, `FolderWalkWorker`, etc.)
3. `QApplication.notify()` override — Qt slot exceptions (the most common PyQt6 crash path)
4. `faulthandler.enable(file=<log_path>)` — native crashes only (write to file, cannot POST)

**Do NOT use `enable_exception_autocapture=True` on the posthog SDK** (not added). Implement the exception hooks manually to maintain full control over what gets scrubbed.

### Traceback scrubbing

**Do NOT use `capture_exception_code_variables=True`.** The SDK feature sends frame-local variable values to PostHog servers. This is not configurable via `before_send` because the variable extraction happens inside the SDK before `before_send` is called. (HIGH confidence — confirmed by reviewing SDK source and PostHog error-tracking docs.)

Manual scrubbing approach via the `_scrub_hook` in `shared/posthog_server.py`:

```python
import re
import traceback

_MY_LIBRARY_ROOT = pathlib.Path(os.environ.get('LOCALAPPDATA', '')) / 'GenizahSearchPro'

def scrub_traceback_payload(event: dict) -> dict | None:
    props = event.get('properties', {})
    # Strip any frame-level variable dicts (should not exist in our payloads,
    # but guard defensively)
    for frame in props.get('$exception_list', [{}])[0].get('stacktrace', {}).get('frames', []):
        frame.pop('vars', None)
    # Redact absolute file paths to basename only
    tb_text = props.get('$exception_list', [{}])[0].get('value', '')
    if tb_text:
        # Replace Windows absolute paths with just the filename
        props.setdefault('$exception_list', [{}])[0]['value'] = re.sub(
            r'[A-Za-z]:\\(?:[^\\]+\\)*([^\\]+\.py)', r'...\\\1', tb_text
        )
    return event
```

The actual payload construction for exceptions uses `traceback.format_exception()` output, manually stripped of local variable values (never passed in) and path prefixes.

### Anonymous per-install UUID

**Use `uuid.uuid4()` stored to a local file. Do not use hardware IDs.**

`uuid.uuid1()` encodes the MAC address — it is PII. `uuid4()` is pure random with no hardware linkage.

```python
def get_or_create_install_id() -> str:
    id_path = pathlib.Path(os.environ['LOCALAPPDATA']) / 'GenizahSearchPro' / 'telemetry_id.txt'
    if id_path.exists():
        return id_path.read_text(encoding='utf-8').strip()
    new_id = str(uuid.uuid4())
    id_path.parent.mkdir(parents=True, exist_ok=True)
    id_path.write_text(new_id, encoding='utf-8')
    return new_id
```

Key properties:
- Generated only when user opts in (not at install time or first launch)
- Stable across app restarts and updates
- No linkage to user account, machine, or network address
- Survives app updates (stored in `LOCALAPPDATA`, not in the install dir which Inno Setup may clean)
- Disclosure: bilingual "We generate a random, anonymous identifier for your installation" in consent dialog

---

## Alternatives Considered

| Recommended | Alternative | Why Not |
|-------------|-------------|---------|
| Extend `shared/posthog_server.py` | Add `posthog` SDK 7.18.3 | SDK brings `capture_exception_code_variables` PII risk, feature flags we cannot use, and one new dep (`backoff`). The raw queue is production-proven and fully sufficient |
| Manual `sys.excepthook` + `threading.excepthook` + `QApplication.notify()` | `posthog` SDK `enable_exception_autocapture=True` | SDK's autocapture hooks cannot be fully controlled via `before_send` — local variable capture happens before the hook fires. Manual gives full scrub control |
| `uuid.uuid4()` persisted to file | Hardware ID (MAC, WMIC, Windows machine GUID) | MAC address is PII; hardware IDs change after hardware replacement; `uuid4` is zero-PII and stable enough for analytics |
| `uuid.uuid4()` persisted to file | Windows registry | Registry requires admin rights on some configs; `LOCALAPPDATA` does not. The app already uses `LOCALAPPDATA` for Tantivy index and joins.db |
| Embed key as string constant | `POSTHOG_API_KEY` env var | End users run from a GUI launcher with no shell; env var would be empty in all installs |
| Embed key as string constant | Fetch key from server at runtime | Adds a network dependency at startup; the key is write-only and safe to embed |

---

## What NOT to Add

| Avoid | Why | Use Instead |
|-------|-----|-------------|
| `posthog` SDK (PyPI package) | Brings `capture_exception_code_variables` PII risk; feature flags need a personal API key not appropriate for distribution; `backoff` adds a new dep for no meaningful benefit at this event volume | Extend `shared/posthog_server.py` |
| `capture_exception_code_variables=True` | Sends frame-local variable values to PostHog servers — catastrophic PII leak for a search app (query text, file paths) | Manual traceback serialization with explicit scrubbing |
| Sentry / Rollbar / Bugsnag | Third-party crash reporters with their own SDKs, data retention policies, and EU compliance requirements; PostHog is already the analytics platform | PostHog error tracking via `shared/posthog_server.py` |
| `faulthandler` events to PostHog | Native crashes produce no Python stack; the `faulthandler` output is a raw C stack trace that cannot be safely POST'd | Write `faulthandler` output to a local log file; include the file path in the next session's startup event |
| Per-query search text in telemetry | Privacy requirement: no query text, no My Library content | Track mode (keyword/Responsa/composition), result count, duration — never the query string |
| `psutil` for system metrics | Already in lock file but adds telemetry coupling to OS-level data | Limit to `platform.version()` + `sys.platform` for environment data |

---

## Installation (requirements changes)

```bash
# requirements.txt — NO CHANGES needed
# requirements-lock.txt — NO CHANGES needed
# GenizahSearchPro.spec — NO CHANGES needed

# The ONLY code changes are:
# 1. shared/posthog_server.py  — add opt-in gate + scrub hook + default distinct_id
# 2. desktop/telemetry.py      — new module (consent, UUID, hooks, event helpers)
# 3. genizah_app.py            — wire telemetry.init() at startup, consent dialog
```

---

## Version Compatibility

| Component | Version | Notes |
|-----------|---------|-------|
| `shared/posthog_server.py` (existing) | — | Already uses `requests==2.32.5`, `queue`, `threading` (all available in frozen binary) |
| `threading.excepthook` | Python 3.8+ | Project requires Python 3.10+; no compatibility issue |
| `faulthandler` | Python 3.3+ stdlib | Always available |
| `uuid.uuid4()` | Python 3.x stdlib | Always available |
| PostHog EU ingest (`https://eu.i.posthog.com/capture`) | — | Already hardcoded in `POSTHOG_CAPTURE_URL`; matches project's existing EU PostHog account (`eu.posthog.com`) |
| PyInstaller | 6.19.0 (in lock) | No spec changes needed; `shared/` already in `datas`; `desktop/telemetry.py` auto-discovered |

---

## Sources

- `/posthog/posthog-python` via Context7 CLI — SDK init options, `capture_exception`, `before_send`, `disabled`, lifecycle (HIGH confidence)
- `https://pypi.org/pypi/posthog/json` — Latest version 7.18.3, `install_requires`: `requests<3.0,>=2.7`, `backoff>=1.10.0`, `distro>=1.5.0`, `typing-extensions>=4.2.0` (HIGH confidence — authoritative PyPI API)
- Direct wheel download: `posthog-7.18.3-py3-none-any.whl` = 273 KB; `backoff-2.2.1-py3-none-any.whl` = 15 KB (HIGH confidence — measured)
- PostHog error tracking docs `https://posthog.com/docs/error-tracking/installation/python` — exception autocapture installs `sys.excepthook` + `threading.excepthook`; `capture_exception_code_variables` sends local vars (HIGH confidence — official docs)
- PostHog project API key safety: `https://posthog.com/questions/is-it-ok-to-expose-the-posthog-project-api-key-to-the-public` — confirmed write-only ingest key (HIGH confidence)
- `C:\Genizahsearch\GenizahSearchPro.spec` — direct inspection: `('shared', 'shared')` already in `datas`; no existing posthog hook in `pyinstaller-hooks-contrib==2026.3` confirmed by filesystem search (HIGH confidence)
- `C:\Genizahsearch\requirements-lock.txt` — `backoff` not present; `distro`, `typing-extensions`, `python-dateutil`, `requests` all present (HIGH confidence — direct file read)
- PyQt exception hook patterns: `https://fman.io/blog/pyqt-excepthook/` — PyQt truncates tracebacks in slot callbacks; `sys.excepthook` alone insufficient (MEDIUM confidence — well-known blog, consistent with PyQt6 behavior)
- Python `uuid.uuid1()` MAC address issue: `https://docs.python.org/3/library/uuid.html` — uuid1 uses MAC; uuid4 is random (HIGH confidence — official Python docs)

---

*Stack research for: v8.1.0 Desktop Telemetry — opt-in PostHog + crash reporting for PyQt6 frozen binary*
*Researched: 2026-06-13*

# Phase 111: Telemetry Foundation - Pattern Map

**Mapped:** 2026-06-14
**Files analyzed:** 8 (2 new modules, 1 modified module, 1 modified store, 6 new test files)
**Analogs found:** 8 / 8

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/telemetry.py` | service/chokepoint | event-driven | `web/safe_storage.py` (chokepoint shape) + `shared/nli_circuit_breaker.py` (module-level singleton + state cache) | role-match |
| `shared/posthog_server.py` (modify) | service | event-driven | itself — backward-compatible additions only; shape of new functions mirrors `_reset_for_tests` + `get_dropped_event_count` | exact |
| `genizah_core.py` `load_app_config`/`save_app_config` (modify) | config store | CRUD | itself — new keys added to existing `config.pkl` store | exact |
| `tests/test_telemetry_consent_gate.py` | test | CRUD | `tests/test_posthog_server.py` (module-state reset fixture + autouse + queue monkeypatch) | exact |
| `tests/test_telemetry_scrubbing.py` | test | transform | `tests/test_posthog_server.py` (no-raise, pure-function assertions) | role-match |
| `tests/test_telemetry_allowlist.py` | test | transform | `tests/test_posthog_server.py` (static invariant pattern `TestPublicAPI`) | role-match |
| `tests/test_telemetry_identity.py` | test | event-driven | `tests/test_posthog_server.py` (queue-drain assertion + monkeypatch fixture) | role-match |
| `tests/test_telemetry_posthog_server_ext.py` | test | event-driven | `tests/test_posthog_server.py` (queue assertions, drop-counter, thread safety) | exact |
| `tests/test_telemetry_no_direct_posthog.py` | test/AST guard | static | `tests/test_no_raw_storage_access.py` | exact |

---

## Pattern Assignments

### `desktop/telemetry.py` (service/chokepoint, event-driven)

**Primary analog:** `web/safe_storage.py` for the chokepoint/no-raise wrapper shape  
**Secondary analog:** `shared/nli_circuit_breaker.py` for the module-level singleton + thread-lock pattern

**Imports pattern** — copy from `shared/nli_circuit_breaker.py` lines 48-65 and `web/safe_storage.py` lines 30-38:
```python
from __future__ import annotations

import enum
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from typing import Callable

from genizah_core import load_app_config, save_app_config
from shared.posthog_server import enqueue_event

logger = logging.getLogger(__name__)
```

**Module-constant + cached-state pattern** — copy from `shared/nli_circuit_breaker.py` lines 50-80 (module-level singletons with threading.Lock guards):
```python
# shared/nli_circuit_breaker.py lines 50-80 — the pattern to copy
import threading
import os

_threshold: int = int(os.environ.get('NLI_CIRCUIT_THRESHOLD', '3'))
_window_secs: float = float(os.environ.get('NLI_CIRCUIT_WINDOW', '60'))

_lock = threading.Lock()
_failure_count: int = 0
_open_until: float = 0.0
```
Translate to: `_enabled: bool`, `_enabled_lock`, `_install_id: str | None`, `_current_distinct_id: str | None`, all populated at module import time by `_load_consent_state()`.

**No-raise wrapper shape** — copy from `web/safe_storage.py` lines 46-60:
```python
# web/safe_storage.py lines 46-60 — the no-raise shape every public callable uses
def safe_user_get(key: str, default: Any = None) -> Any:
    try:
        return app.storage.user.get(key, default)
    except AssertionError as e:
        logger.debug("safe_user_get(%r): session storage unavailable: %s", key, e)
        return default
    except Exception as e:
        logger.warning("safe_user_get(%r) unexpected failure: %s", key, e, exc_info=False)
        return default
```
Every public callable in `desktop/telemetry.py` wraps its body in `try/except Exception: pass` or returns silently — never raises into the caller.

**config.pkl load/save pattern** — copy from `genizah_core.py` lines 2871-2891 (see "Shared Patterns" section). The telemetry module calls `load_app_config()` once at import time and `save_app_config({key: value})` on consent/identity changes.

**Enum pattern** — `desktop/telemetry.py` uses `class DesktopEvent(str, enum.Enum)`. No existing enum in `desktop/`; use Python stdlib `enum` as described in RESEARCH.md Pattern 5. The `(str, enum.Enum)` base makes enum members usable directly as strings.

**`is_enabled()` cache pattern** — mirror `shared/nli_circuit_breaker.py`'s `is_open()` (lines shown below) which reads guarded module-level state:
```python
# shared/nli_circuit_breaker.py — is_open() pattern to mirror
def is_open() -> bool:
    """Return True iff the circuit breaker is currently open (short-circuit mode)."""
    with _lock:
        if _failure_count < _threshold and _open_until == 0.0:
            return False
        now = time.monotonic()
        return now < _open_until
```
Translate: `is_enabled()` acquires `_enabled_lock`, returns `_enabled` (which was populated from `config.pkl` at import time).

---

### `shared/posthog_server.py` — backward-compatible additions only (service, event-driven)

**Analog:** itself — new functions follow the exact shape of existing private helpers `_reset_for_tests` (lines 140-155) and `_start_drain_thread_once` (lines 127-137).

**Existing module-state block** (lines 46-51) — additions go IMMEDIATELY AFTER `_dropped_events_lock`:
```python
# shared/posthog_server.py lines 46-51 (existing — do not modify)
_event_queue: queue.Queue = queue.Queue(maxsize=10000)
_drain_thread_started = threading.Event()

_dropped_events: int = 0
_dropped_events_lock = threading.Lock()
```
Add below these lines:
```python
# NEW additions (Phase 111) — guarded by dedicated locks (same pattern as _dropped_events_lock above)
_default_distinct_id: str | None = None
_default_distinct_id_lock = threading.Lock()

_scrub_hook: 'Callable[[dict], dict | None] | None' = None
_scrub_hook_lock = threading.Lock()
```

**`enqueue_event` modification** (lines 65-97) — the ONLY change is:
1. At the top of the function body, resolve `distinct_id` from `_default_distinct_id` when caller passed `'system'`.
2. After building `payload` dict, call `_scrub_hook(payload)` if registered (BEFORE `_event_queue.put_nowait`).

The existing `payload = {...}` block (lines 85-90) and the `try/except queue.Full` block (lines 91-95) remain STRUCTURALLY IDENTICAL. The 5 test monkeypatches in `test_posthog_server.py`, `test_nli_circuit_breaker.py`, `test_api_hardening.py`, `test_search_api_v2.py`, and `test_nli_breaker_cross_module_invariants.py` target `ph._event_queue` only — no function signature changes.

**New helper functions** — copy the shape of `_reset_for_tests` (lines 140-155):
```python
# shared/posthog_server.py lines 140-155 — shape to copy for _drain_and_discard
def _reset_for_tests() -> None:
    global _dropped_events
    while True:
        try:
            _event_queue.get_nowait()
        except queue.Empty:
            break
    with _dropped_events_lock:
        _dropped_events = 0
```
`_drain_and_discard()` is IDENTICAL to the drain-loop portion of `_reset_for_tests` — same `while True / get_nowait / except queue.Empty: break` structure, but WITHOUT resetting `_dropped_events`.

**`_flush_before_exit` shape** — copy `_drain_posthog_queue` (lines 100-124) as a SYNCHRONOUS direct-POST variant, bypassing the daemon thread. Uses `requests.post(POSTHOG_CAPTURE_URL, json=payload, timeout=2.0)` (same call as line 122) inside a time-bounded loop.

**`__all__` addition** (lines 158-164) — add the 4 new public names:
```python
# shared/posthog_server.py lines 158-164 (existing — extend)
__all__ = [
    'POSTHOG_HOST',
    'POSTHOG_CAPTURE_URL',
    'enqueue_event',
    'get_dropped_event_count',
    '_reset_for_tests',
    # Phase 111 additions:
    'set_default_distinct_id',
    'register_scrub_hook',
    '_flush_before_exit',
    '_drain_and_discard',
]
```

---

### `genizah_core.py` — `load_app_config`/`save_app_config` (config store, CRUD)

**Analog:** itself — no code change to these functions; only NEW KEYS are written by `desktop/telemetry.py`. The planner must document the key names so all writers/readers agree.

**Existing store functions** (lines 2871-2891 — read-only reference, no modification):
```python
# genizah_core.py lines 2871-2891
def load_app_config():
    """Load general app configuration."""
    cfg = {}
    if os.path.exists(Config.CONFIG_FILE):
        try:
            with open(Config.CONFIG_FILE, 'rb') as f:
                cfg = pickle.load(f)
        except Exception:
            pass
    return cfg

def save_app_config(new_data):
    """Update general app configuration with new keys."""
    try:
        cfg = load_app_config()
        cfg.update(new_data)
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.CONFIG_FILE, 'wb') as f:
            pickle.dump(cfg, f)
    except Exception as e:
        LOGGER.error("Failed to save config: %s", e)
```

**`Config.CONFIG_FILE` path** (line 2377):
```python
# genizah_core.py line 2377
CONFIG_FILE = os.path.join(INDEX_DIR, "config.pkl")
# INDEX_DIR resolves to LOCALAPPDATA/GenizahSearchPro/Index/ on standard install
```

**New keys written by `desktop/telemetry.py`** — these key-name constants belong in `desktop/telemetry.py` (not in `genizah_core.py`):
```python
TELEMETRY_ENABLED_KEY    = 'telemetry_enabled'         # bool — absent = False
TELEMETRY_INSTALL_ID_KEY = 'telemetry_install_id'      # str (uuid4.hex); RETAINED on opt-out
FIRST_RUN_SHOWN_KEY      = 'telemetry_first_run_shown' # bool — Phase 112 writes this
CONSENT_TIMESTAMP_KEY    = 'telemetry_consent_ts'      # ISO-8601 str
CONSENT_APP_VERSION_KEY  = 'telemetry_consent_version' # str
CONSENT_UI_VERSION_KEY   = 'telemetry_consent_ui_ver'  # str e.g. "1"
IDENTIFIED_USER_KEY      = 'telemetry_identified_user' # str | None — current Supabase user.id
```

---

### `tests/test_telemetry_consent_gate.py` (test, CRUD)

**Analog:** `tests/test_posthog_server.py` lines 33-46 — the autouse `_reset_posthog_server_state` fixture pattern.

**Autouse fixture pattern** (lines 33-46 — copy for telemetry reset):
```python
# tests/test_posthog_server.py lines 33-46
@pytest.fixture(autouse=True)
def _reset_posthog_server_state(monkeypatch):
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)
    yield
    ph._reset_for_tests()
```
Translate: the telemetry test fixture resets module-level `_enabled`, `_install_id`, `_current_distinct_id` using a `monkeypatch` or a dedicated `_reset_for_tests()` function added to `desktop/telemetry.py` (test seam, same pattern as `posthog_server._reset_for_tests`). The fixture also patches `load_app_config` / `save_app_config` to use a temp dict rather than a real `config.pkl` on disk.

**Consent round-trip assertion shape** — copy `TestEnqueueEvent.test_enqueue_event_places_payload_on_queue` (lines 53-64): assert a write then read produces the same value.

**No-raise assertion shape** — `test_enqueue_event_returns_none` (line 78): `result = fn(); assert result is None`.

---

### `tests/test_telemetry_scrubbing.py` (test, transform)

**Analog:** `tests/test_posthog_server.py` `TestPostHogKeyMissing` (lines 130-140) — pure function call + assert, no monkeypatching needed.

The scrubber `_scrub_props` is a pure function (dict-in, dict-out). Tests call it directly:
```python
# Pattern: direct pure-function call + assert
result = _scrub_props({'query': 'some text', 'platform': 'desktop'})
assert 'query' not in result
assert result['platform'] == 'desktop'
```
No fixtures needed. Import `_scrub_props` from `desktop.telemetry` directly (it is an internal function; tests access it directly in the test file — consistent with how `test_posthog_server.py` accesses `ph._event_queue` directly).

---

### `tests/test_telemetry_allowlist.py` (test, transform)

**Analog:** `tests/test_posthog_server.py` `TestPublicAPI` / `TestModuleConstants` (lines 223-271) — static source inspection + enum iteration.

**Static source inspection shape** (lines 236-245):
```python
# tests/test_posthog_server.py lines 236-245
def test_no_web_dependencies(self):
    src = pathlib.Path('shared/posthog_server.py').read_text(encoding='utf-8')
    for line in src.splitlines():
        stripped = line.strip()
        assert not stripped.startswith('from web.'), ...
```
Translate: `test_all_events_have_desktop_prefix` reads `desktop/telemetry.py` source, finds all `DesktopEvent` members, asserts each value starts with `desktop_` (or is `$identify` for the PostHog protocol event).

**Enum iteration shape:**
```python
# Iterate all enum members to assert property
from desktop.telemetry import DesktopEvent
for member in DesktopEvent:
    assert member.value.startswith('desktop_') or member.value.startswith('$'), ...
```

---

### `tests/test_telemetry_identity.py` (test, event-driven)

**Analog:** `tests/test_posthog_server.py` `TestEnqueueEvent` (lines 52-81) — monkeypatch `_event_queue`, call the function, then `ph._event_queue.get(timeout=1.0)` and assert payload shape.

**Queue-drain assertion shape** (lines 53-64 — copy exactly):
```python
ph.enqueue_event('foo', {'k': 'v'})
payload = ph._event_queue.get(timeout=1.0)
assert payload['event'] == 'foo'
assert payload['distinct_id'] == 'system'
```
Translate: after `telemetry.identify('some-user-uuid')`, drain `ph._event_queue`, assert `payload['event'] == '$identify'`, `payload['distinct_id'] == 'some-user-uuid'`, and that `payload['properties']` contains NO `email` or `name` key (IDENT-03).

---

### `tests/test_telemetry_posthog_server_ext.py` (test, event-driven)

**Analog:** `tests/test_posthog_server.py` — full file. Use the SAME autouse fixture (monkeypatch `ph._event_queue`). All 4 new `posthog_server` functions are tested with the same queue-inspection pattern.

**`_drain_and_discard` test shape** — mirrors `TestResetForTests.test_reset_drains_queue` (lines 199-205):
```python
# tests/test_posthog_server.py lines 199-205
def test_reset_drains_queue(self):
    ph.enqueue_event('e1', {})
    ph.enqueue_event('e2', {})
    ph.enqueue_event('e3', {})
    ph._reset_for_tests()
    with pytest.raises(queue.Empty):
        ph._event_queue.get_nowait()
```
Translate: enqueue 3 events, call `ph._drain_and_discard()`, assert `queue.Empty` — and assert that no POST was made (monkeypatch `requests.post` to assert it was never called).

**`set_default_distinct_id` backward-compat test** — mirrors `test_distinct_id_kwarg_override` (lines 64-67): enqueue with explicit `distinct_id='explicit'`, assert payload `distinct_id == 'explicit'` even when a default is set.

---

### `tests/test_telemetry_no_direct_posthog.py` (test/AST guard, static)

**Analog:** `tests/test_no_raw_storage_access.py` — the entire file. This is the PRIV-03 guard.

**Structural shape to copy** (lines 1-387 — the complete pattern):

Key elements to replicate:

1. **REPO_ROOT / DESKTOP_DIR path setup** (lines 32-34):
```python
# tests/test_no_raw_storage_access.py lines 32-34
REPO_ROOT = Path(__file__).resolve().parent.parent
WEB_DIR = REPO_ROOT / 'web'
ALLOWLIST_PATH = REPO_ROOT / '.planning' / 'phase87_storage_allowlist.yaml'
```
Translate: `DESKTOP_DIR = REPO_ROOT / 'desktop'`. No YAML allowlist needed for PRIV-03 — the invariant is absolute (no `desktop/` file except `desktop/telemetry.py` may call `enqueue_event`).

2. **AST scanner** (lines 44-165): scan for `enqueue_event` call nodes in `desktop/` files, using `ast.walk` + `isinstance(node, ast.Call)` + check `node.func` attribute name == `'enqueue_event'`.

3. **Production lint test** (lines 356-386):
```python
# tests/test_no_raw_storage_access.py lines 356-386 — shape to copy
def test_no_raw_storage_access_outside_allowlist():
    violations = []
    for path in WEB_DIR.rglob('*.py'):
        if path.name == 'safe_storage.py':
            continue  # The chokepoint itself
        rel = path.relative_to(REPO_ROOT).as_posix()
        source = path.read_text(encoding='utf-8')
        ...
        for lineno, seg in file_violations:
            ...
            violations.append(f"{rel}:{lineno}: ...")
    if violations:
        pytest.fail(...)
```
Translate: `for path in DESKTOP_DIR.rglob('*.py'): if path.name == 'telemetry.py': continue  # chokepoint itself`.

4. **Synthetic violation test** (lines 235-247):
```python
def test_lint_rejects_synthetic_violation():
    synthetic = textwrap.dedent("""\
        from shared.posthog_server import enqueue_event
        def bad():
            enqueue_event('foo', {})
    """)
    ...
    assert visitor.violations, "Lint visitor failed to detect synthetic raw access"
```

---

## Shared Patterns

### config.pkl Load/Save Pattern
**Source:** `genizah_core.py` lines 2871-2891  
**Apply to:** `desktop/telemetry.py` — all consent reads and writes

The pattern is: `load_app_config()` returns a dict (empty on first run), `save_app_config(new_data)` merges new keys into the existing dict. Both functions are safe (catch all exceptions silently). The telemetry module calls `load_app_config()` once at module import time to populate `_enabled` / `_install_id` / `_current_distinct_id` caches.

```python
# genizah_core.py lines 2871-2891 — the EXACT functions to call
def load_app_config():
    cfg = {}
    if os.path.exists(Config.CONFIG_FILE):
        try:
            with open(Config.CONFIG_FILE, 'rb') as f:
                cfg = pickle.load(f)
        except Exception:
            pass
    return cfg

def save_app_config(new_data):
    try:
        cfg = load_app_config()
        cfg.update(new_data)
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.CONFIG_FILE, 'wb') as f:
            pickle.dump(cfg, f)
    except Exception as e:
        LOGGER.error("Failed to save config: %s", e)
```

### Module-Level Singleton + Lock Pattern
**Source:** `shared/nli_circuit_breaker.py` lines 50-80 and `shared/posthog_server.py` lines 46-51  
**Apply to:** `desktop/telemetry.py` module-level state; `shared/posthog_server.py` new state variables

All shared mutable state uses a dedicated `threading.Lock()` per variable. State is initialized at module import time. Functions that read or write state acquire the lock.

### Fire-and-Forget / Never-Raise Pattern
**Source:** `shared/posthog_server.py` lines 65-97; `web/safe_storage.py` lines 46-86  
**Apply to:** All 8 public callables in `desktop/telemetry.py`

Every public callable wraps its body in a broad `try/except Exception` and returns silently on failure. The POSTHOG_API_KEY being absent or wrong is not an error — events are dropped silently (line 112-113 of `posthog_server.py`).

```python
# shared/posthog_server.py lines 96-97 — the outer catch
    except Exception:
        logger.debug('posthog_server.enqueue_event silently dropped', exc_info=True)
```

### Test Autouse Fixture + Module Reset Pattern
**Source:** `tests/test_posthog_server.py` lines 33-46  
**Apply to:** All 5 new `test_telemetry_*.py` files

Each test file needs an autouse fixture that (a) resets module-level state in `desktop/telemetry.py` before/after each test, and (b) replaces `ph._event_queue` with a fresh drain-free queue so queue-drain assertions are deterministic.

```python
# tests/test_posthog_server.py lines 33-46 — the exact shape to replicate
@pytest.fixture(autouse=True)
def _reset_posthog_server_state(monkeypatch):
    ph._reset_for_tests()
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)
    yield
    ph._reset_for_tests()
```

`desktop/telemetry.py` must expose a `_reset_for_tests()` function (same naming convention as `posthog_server._reset_for_tests`) that resets `_enabled`, `_install_id`, `_current_distinct_id` to their default (False/None) state and is explicitly excluded from the PRIV-03 AST guard.

### `_APP_VERSION` Import Pattern
**Source:** `genizah_app.py` line 28: `from version import APP_VERSION`  
**Apply to:** `desktop/telemetry.py` — constant used in BASE_PROPS and consent audit fields

`version.py` is a two-line file (`APP_VERSION = "8.0.0"`) with no imports — no circular import risk. Use `from version import APP_VERSION` directly.

### web `_posthog_identify` Identity Contract
**Source:** `web/auth_state.py` lines 159-170 — the web-side identity shape  
**Apply to:** `desktop/telemetry.py::identify()` — must use the same `user.id` as `distinct_id`

```python
# web/auth_state.py lines 159-170 — the contract to match
@classmethod
def _posthog_identify(cls, user: Dict, profile: Dict = None):
    try:
        import json
        uid = json.dumps(user.get('id', ''))
        email = json.dumps(user.get('email', ''))
        name = json.dumps((profile or {}).get('full_name', '') or ...)
        js = f"if(window.posthog)posthog.identify({uid},{{email:{email},name:{name}}})"
        ui.run_javascript(js)
    except Exception:
        pass
```
Desktop `identify(user_id: str)` sends the bare `user.id` as `distinct_id` (D-08: no email/name). The web attaches email/name to the shared person profile already.

### PostHog Key Source
**Source:** `web/main.py` lines 794-802 — the key location and init pattern  
**Apply to:** `desktop/telemetry.py` `_TELEMETRY_KEY` constant + env override

```python
# web/main.py line 794
_posthog_key = os.environ.get('POSTHOG_API_KEY', '')
# web/main.py line 801-802
posthog.init('{_posthog_key}', {
    api_host: 'https://eu.i.posthog.com',
    person_profiles: 'identified_only',
```
Desktop mirrors with: `_TELEMETRY_KEY = os.environ.get('GENIZAH_TELEMETRY_KEY', '<PLACEHOLDER_KEY>')`. EU endpoint is already hardcoded in `shared/posthog_server.py` line 43 (`POSTHOG_HOST = 'https://eu.i.posthog.com'`).

### Existing Crash Handler — Chain, Not Replace
**Source:** `genizah_app.py` lines 148-170 — the existing `_setup_crash_handler`  
**Apply to:** Phase 113 (NOT Phase 111 — noted here for awareness only)

```python
# genizah_app.py lines 153-168 — the chain-not-replace pattern (Phase 113 reference)
def exception_hook(exc_type, exc_value, exc_tb):
    ...
    sys.__excepthook__(exc_type, exc_value, exc_tb)  # always chain

sys.excepthook = exception_hook
```
Phase 113's telemetry crash hook MUST chain to the existing handler (which chains to `sys.__excepthook__`). Phase 111 does not touch this.

---

## No Analog Found

All files have analogs. No entry needed.

---

## Metadata

**Analog search scope:** `desktop/`, `shared/`, `web/`, `tests/`  
**Files scanned for patterns:** 9 analog files read in full  
**Pattern extraction date:** 2026-06-14

**Key constraint preserved:** The 5 existing test monkeypatches that target `ph._event_queue` directly are in:
- `tests/test_posthog_server.py` line 44 (`monkeypatch.setattr(ph, '_event_queue', fresh_q)`)
- `tests/test_posthog_server.py` line 103/105 (`monkeypatch.setattr(ph, '_event_queue', tiny_q)`)
- `tests/test_nli_circuit_breaker.py` (imports `shared.posthog_server as ph`)
- `tests/test_api_hardening.py` (separate `web/api_hardening._event_queue`)
- `tests/test_search_api_v2.py` (separate `web/api_hardening._event_queue`)

The `enqueue_event` signature MUST NOT CHANGE. All 4 neutral additions are new functions at module scope, not changes to `enqueue_event`'s public signature.

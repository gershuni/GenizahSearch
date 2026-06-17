# Phase 114: Usage Analytics - Pattern Map

**Mapped:** 2026-06-15
**Files analyzed:** 6 modified files (no new files except the test guard)
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `desktop/telemetry.py` (add `ACTIVE_PING`) | config / registry | event-driven | `desktop/telemetry.py` itself (adding to existing enum) | exact |
| `genizah_app.py` (startup coordinator, session, heartbeat) | event producer / identity coordinator | event-driven | `genizah_app.py:170-179` crash-hook wiring (Phase 113) + `genizah_app.py:3428` `_restoring_session` guard | role-match |
| `genizah_app.py` (tab telemetry, `_on_tab_changed`) | event producer | event-driven | `genizah_app.py:3786-3805` existing `_on_tab_changed` body | exact |
| `genizah_app.py` (search telemetry, per-run object) | event producer / per-run state | event-driven | `desktop/telemetry.py:992-1030` crash hook recursion guard + `emitted` flag pattern | role-match |
| `genizah_app.py` (login/logout identity wiring) | identity coordinator | request-response | `web/auth_state.py:159-170` `_posthog_identify` / `clear_auth` PostHog calls | role-match |
| `tests/test_no_dynamic_telemetry_strings.py` | AST guard test | — | `tests/test_no_raw_storage_access.py` (full template) | exact |

---

## Pattern Assignments

---

### `desktop/telemetry.py` — add `ACTIVE_PING` to `DesktopEvent`

**Analog:** `desktop/telemetry.py:132-163` (the existing `DesktopEvent` enum body)

**Existing enum definition** (`desktop/telemetry.py:132-162`):
```python
class DesktopEvent(str, enum.Enum):
    """Fixed registry of all permitted desktop event names (PRIV-06)."""
    # Identity / protocol (Phase 111)
    IDENTIFY       = '$identify'
    IDENTITY_RESET = 'desktop_identity_reset'

    # Crash (Phase 113)
    CRASH          = 'desktop_crash'
    PRIOR_CRASH    = 'desktop_prior_crash'

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
```

**What to add** — insert between `FEATURE_OPENED` and `SESSION_PERF`:
```python
    # Active user (Phase 114)
    ACTIVE_PING    = 'desktop_active_ping'
```

**Why:** `track()` validates the event value against `_VALID_EVENT_VALUES = frozenset(e.value for e in DesktopEvent)` (line 166). Any call to `track(DesktopEvent.ACTIVE_PING, ...)` will raise `AttributeError` until the member is added. The `_VALID_EVENT_VALUES` frozenset is rebuilt at import time — no other change needed.

---

### `genizah_app.py` — startup identity coordinator + session_start (D-11, D-12, D-14, USAGE-01)

**Analog 1 — crash hook wiring pattern** (`genizah_app.py:170-179`):
```python
# Phase 113: install telemetry crash hooks (chained AFTER _setup_crash_handler so
# _prior_excepthook captures the crash-log writer, not the bare sys.__excepthook__).
# Best-effort: never blocks startup (D-08).
try:
    from desktop import telemetry as _telemetry
    _telemetry.install_exception_hooks()
except Exception:
    pass  # crash hooks are best-effort; never block app startup
```

**What to replicate:** The `try/except Exception: pass` best-effort wrapper, the deferred `from desktop import telemetry` import pattern. Startup telemetry must never block or raise into the caller.

**Analog 2 — QTimer deferred startup chain** (`genizah_app.py:3535-3542`):
```python
# One-time citation reminder (shown once per installation)
if not cfg.get('citation_reminder_seen', False):
    QTimer.singleShot(500, self._show_citation_reminder)
    # consent is chained at the end of _show_citation_reminder
else:
    # Installs that already saw the citation: still show consent if not yet seen
    QTimer.singleShot(500, self._maybe_show_first_run_prompt)

# Restore session state (deferred slightly so all widgets are settled)
QTimer.singleShot(200, self._restore_session)
```

**What to replicate:** Add a `QTimer.singleShot(700, self._run_startup_telemetry_coordinator)` immediately after the two existing defers in `on_startup_finished()`. The 700ms ensures it fires after both the 200ms session-restore and 500ms consent-dialog timers have resolved. The coordinator is idempotent so exact ordering only matters for the first call.

**Coordinator method — what to write** (in `GenizahGUI`):
```python
def _run_startup_telemetry_coordinator(self) -> None:
    """Single boot sequence: consent → identity → session_start.

    D-12: Exactly once. Must be called AFTER _restore_session and
    _maybe_show_first_run_prompt have resolved (700ms defer in on_startup_finished).
    D-14: One session_id per process; crash-restart = fresh session.
    D-11: identify() BEFORE track(SESSION_START).
    """
    try:
        from desktop import telemetry
        if getattr(self, '_telemetry_session_started', False):
            return  # exactly-once guard (D-14)
        if not telemetry.is_enabled():
            return  # consent not granted
        self._telemetry_session_started = True

        import uuid
        from datetime import datetime, timezone
        self._session_id = uuid.uuid4().hex

        # 1. Resolve identity (D-12 stale-identity fix)
        user = getattr(self.corrections_client, 'current_user', None)
        stored_uuid = telemetry.load_app_config().get(telemetry.IDENTIFIED_USER_KEY)
        if user is not None and getattr(user, '_uuid', None):
            telemetry.identify(user._uuid)   # D-10: _uuid, never .id
        elif stored_uuid:
            telemetry.reset_identity()        # stale IDENTIFIED_USER_KEY, session expired

        # 2. Emit session_start (after identify so it attributes to the merged person)
        now_utc = datetime.now(timezone.utc)
        self._session_start_date_utc = now_utc.strftime('%Y-%m-%d')
        import sys
        from PyQt6 import QtCore
        telemetry.track(
            telemetry.DesktopEvent.SESSION_START,
            session_id=self._session_id,
            ui_language='he' if CURRENT_LANG == 'he' else 'en',
            python_version=f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}',
            pyqt_version=QtCore.PYQT_VERSION_STR,
        )

        # 3. Wire daily active-user heartbeat (D-16)
        self._setup_active_ping()
    except Exception:
        pass  # coordinator is best-effort; never raise into caller
```

**Key constraint:** `telemetry.load_app_config()` is the same `load_app_config` that `_load_consent_state` uses at import time (`telemetry.py:466`). The coordinator re-reads `IDENTIFIED_USER_KEY` to detect stale identity, then compares against `corrections_client.current_user` which was populated synchronously by `_load_credentials()` in `SupabaseCorrectionsClient.__init__()` (`supabase_corrections_client.py:335`).

---

### `genizah_app.py` — `_on_tab_changed` telemetry guard (D-01, D-02)

**Analog:** `genizah_app.py:3786-3805` (existing `_on_tab_changed` body):
```python
def _on_tab_changed(self, index):
    """Handle tab change events."""
    logger.debug("_on_tab_changed called with index=%s", index)
    try:
        current_widget = self.tabs.widget(index)
        logger.debug("current_widget=%s", current_widget)
        if hasattr(self, 'community_tab') and current_widget == self.community_tab:
            if not getattr(self, '_community_data_loaded', False):
                self._refresh_community_panels()
                self._community_data_loaded = True
        # Lazy-load catalog browse tree on first tab activation
        if hasattr(self, 'catalog_browse_tab') and current_widget == self.catalog_browse_tab:
            if not getattr(self, '_catalog_tree_loaded', False):
                self._catalog_populate_tree()
    except Exception as e:
        logger.exception("Error in _on_tab_changed: %s", e)
```

**`_restoring_session` flag set** (`genizah_app.py:3428`):
```python
self._restoring_session = True
```

**`_restoring_session` flag reset** (`genizah_app.py:26307`, inside `_restore_session`'s `finally` block):
```python
self._restoring_session = False
```

**What to ADD at the top of the existing `_on_tab_changed` body:**
```python
def _on_tab_changed(self, index):
    """Handle tab change events."""
    logger.debug("_on_tab_changed called with index=%s", index)
    # D-02: skip telemetry for programmatic tab changes during session restore
    if not getattr(self, '_restoring_session', True):
        try:
            from desktop import telemetry
            if telemetry.is_enabled():
                _TAB_NAME_MAP = {
                    0: 'search',
                    1: 'composition',
                    2: 'browse_shelfmark',
                    3: 'browse_catalog',
                    4: 'lists',
                    5: 'community',
                    6: 'my_library',
                }
                tab_name = _TAB_NAME_MAP.get(index)
                if tab_name is not None:
                    telemetry.track(
                        telemetry.DesktopEvent.TAB_ACTIVATED,
                        tab_name=tab_name,   # D-04: hardcoded constant, NEVER tabText()
                        session_id=getattr(self, '_session_id', ''),
                    )
        except Exception:
            pass
    # ... (existing body unchanged)
```

**What to replicate:** The `getattr(self, '_restoring_session', True)` pattern — default to `True` so if the attribute was never set (very early startup race), telemetry is safely suppressed. The `try/except Exception: pass` wrapper mirrors crash-hook best-effort. The `_TAB_NAME_MAP` is a module-level or method-local constant (D-04) — never `self.tabs.tabText(index)`.

---

### `genizah_app.py` — per-run search state object + single-emit (D-08, D-09, USAGE-03)

**Analog — crash hook recursion / exactly-once guard** (`desktop/telemetry.py:992-1030`):
```python
global _in_crash_hook, _last_reported_tb_id
if _in_crash_hook:
    return  # recursion guard (D-05 — crash inside crash handler must not loop)
_in_crash_hook = True
try:
    if not _is_enabled_nolock():
        return
    # D-08 / REVIEWS PASS2: lock-free traceback-id dedup.
    if exc_tb is not None:
        tb_id = id(exc_tb)
        if tb_id == _last_reported_tb_id:
            return  # already reported this exact traceback
        _last_reported_tb_id = tb_id  # record BEFORE sending
    ...
finally:
    _in_crash_hook = False
```

**Pattern to extract:** A module-level (or instance-level) flag is set BEFORE the action and checked at entry to prevent double-emission. The Phase 114 analog is an instance dict with an `'emitted'` key rather than a bool flag — same principle, per-search-run scope.

**`_search_was_cancelled` pattern** (`genizah_app.py:17282`):
```python
def stop_search(self):
    if self.search_thread.isRunning():
        self._search_was_cancelled = True   # set BEFORE cancel_flag
        self.search_thread.cancel_flag = True
        ...
    self.reset_ui()

def on_search_finished(self, results):
    was_cancelled = getattr(self, '_search_was_cancelled', False)   # read here
    if not results:
        self.reset_ui()
        ...
        return
```

**What to ADD — per-run object creation** (at the start of `start_search()`, after effective `mode_idx` is known per D-06):
```python
# D-09: per-run state object — emit exactly once from completion OR stop_search()
_SEARCH_MODE_ENUM = {
    0: 'keyword', 1: 'variants', 2: 'responsa',
    3: 'fuzzy',   4: 'regex',    5: 'title',
    6: 'shelfmark', 7: 'pgp_tags',
}
_mode_key = _SEARCH_MODE_ENUM.get(mode_idx, 'keyword')
_is_lab = getattr(self, 'btn_lab_mode_toggle', None) and self.btn_lab_mode_toggle.isChecked()
_search_mode_enum = f'lab_{_mode_key}' if _is_lab else _mode_key
_corpus = self.corpus_scope_combo.currentData() or 'genizah'  # safe: data(), not text()
self._current_search_run = {
    'mode': _search_mode_enum,
    'corpus': _corpus,
    'emitted': False,
}
```

**What to ADD — helper + wiring in `on_search_finished`:**
```python
def _emit_search_telemetry(self, action: str, result_count: int | None = None) -> None:
    """Emit desktop_search_executed exactly once per run (D-09 idempotency guard)."""
    try:
        run = getattr(self, '_current_search_run', None)
        if run is None or run.get('emitted'):
            return
        run['emitted'] = True
        from desktop import telemetry
        props = {
            'search_mode': run['mode'],       # D-04/D-05: enum constant, never currentText()
            'corpus_scope': run['corpus'],     # safe: currentData() is a fixed code string
            'action': action,                  # 'completed' | 'cancelled'
            'session_id': getattr(self, '_session_id', ''),
        }
        if action == 'completed' and result_count is not None:
            count = result_count
            if count == 0:
                bucket = '0'
            elif count < 10:
                bucket = '1-9'
            elif count < 100:
                bucket = '10-99'
            else:
                bucket = '100+'
            props['result_count_bucket'] = bucket
        telemetry.track(telemetry.DesktopEvent.SEARCH_EXECUTED, **props)
    except Exception:
        pass

# In on_search_finished() — after was_cancelled read, BEFORE early return:
if was_cancelled:
    self._emit_search_telemetry('cancelled')
    # ... (existing early-return body)
    return
# ... process results ...
self._emit_search_telemetry('completed', len(results))

# In stop_search() — BEFORE reset_ui(), AFTER terminate() path (D-09 shutdown guard):
if not getattr(self, '_app_shutting_down', False):  # D-09: skip on clean exit
    self._emit_search_telemetry('cancelled')
```

**`_app_shutting_down` flag** — set in `closeEvent()` BEFORE calling `stop_search()`:
```python
def closeEvent(self, event):
    self._app_shutting_down = True   # D-09: prevent stop_search() cancel from being counted
    # ... existing cleanup including stop_search() ...
```

---

### `genizah_app.py` — login/logout identity wiring (D-10, D-13, IDENT-01/02)

**Analog — web `_posthog_identify`** (`web/auth_state.py:159-170`):
```python
@classmethod
def _posthog_identify(cls, user: Dict, profile: Dict = None):
    """Send posthog.identify() to the browser if PostHog is loaded."""
    try:
        import json
        uid = json.dumps(user.get('id', ''))   # web: user['id'] IS the raw UUID
        email = json.dumps(user.get('email', ''))
        name = json.dumps((profile or {}).get('full_name', '') or ...)
        js = f"if(window.posthog)posthog.identify({uid},{{email:{email},name:{name}}})"
        ui.run_javascript(js)
    except Exception:
        pass
```

**What the desktop mirrors — differences:**
- Web calls `user.get('id')` which IS the raw Supabase UUID (from `user['id']` in the JWT payload).
- Desktop: `corrections_client.current_user._uuid` is the equivalent field (D-10). `current_user.id` is `hash(user_id) % 10**9` (`supabase_corrections_client.py:731`) — NEVER use it.
- Desktop does NOT send email/name (D-08 hard rule in `telemetry.identify()`).

**Analog — web `clear_auth` PostHog reset** (`web/auth_state.py:214-218`):
```python
try:
    ui.run_javascript('if(window.posthog)posthog.reset()')
except Exception:
    pass  # PostHog analytics optional; failure is non-fatal
```

**What to ADD to `_show_login_dialog`** (after `dialog.exec()` succeeds):
```python
def _show_login_dialog(self):
    dialog = LoginDialog(self, self.corrections_client)
    if dialog.exec():
        self._update_corner_login_state()
        self._refresh_community_panels()
        self._enable_lists_cloud_sync()
        # D-13: mid-session login → identify(_uuid) to alias anon→user
        try:
            from desktop import telemetry
            user = self.corrections_client.current_user
            if user is not None and getattr(user, '_uuid', None):
                telemetry.identify(user._uuid)   # D-10: _uuid ONLY
        except Exception:
            pass
```

**What to ADD to `_do_logout`** (`genizah_app.py:3828`):
```python
def _do_logout(self):
    self._disable_lists_cloud_sync()
    self.corrections_client.logout()
    # D-13: reset identity on logout (mirrors web posthog.reset())
    try:
        from desktop import telemetry
        telemetry.reset_identity()
    except Exception:
        pass
    self._update_corner_login_state()
    self._refresh_community_panels()
    QMessageBox.information(self, tr("Logged Out"), tr("You have been logged out."))
```

**What to replicate:** The `try/except Exception: pass` best-effort pattern — identity calls must never raise into login/logout flow. The deferred `from desktop import telemetry` import (matches the crash-hook pattern at line 176).

---

### `genizah_app.py` — daily active-user heartbeat (D-16, USAGE-04)

**Analog — no existing Qt app-state signal wiring** (verified: zero matches for `applicationStateChanged` in `genizah_app.py`). The closest structural analog is the `_ping_check_timer` pattern from QTimer use elsewhere in the file (various `QTimer.singleShot` calls).

**What to ADD** — two methods in `GenizahGUI`:
```python
def _setup_active_ping(self) -> None:
    """Wire daily active-user heartbeat (D-16).

    Fires at most once per UTC day, only when the app is active/resumed,
    and NOT on the same UTC day as session_start (avoids double-count).
    Uses QTimer (5-min check) + applicationStateChanged for sleep/resume.
    """
    self._last_ping_date_utc: str | None = None
    self._ping_check_timer = QTimer(self)
    self._ping_check_timer.setInterval(5 * 60 * 1000)   # 5 minutes
    self._ping_check_timer.timeout.connect(self._maybe_emit_active_ping)
    self._ping_check_timer.start()
    try:
        QApplication.instance().applicationStateChanged.connect(
            self._on_app_state_changed
        )
    except Exception:
        pass  # applicationStateChanged not available on all Qt builds

def _on_app_state_changed(self, state) -> None:
    from PyQt6.QtCore import Qt
    if state == Qt.ApplicationState.ApplicationActive:
        self._maybe_emit_active_ping()

def _maybe_emit_active_ping(self) -> None:
    try:
        from desktop import telemetry
        from datetime import datetime, timezone
        from PyQt6.QtCore import Qt
        if not telemetry.is_enabled():
            return
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        # Not on the same UTC day as session_start (D-16)
        if today == getattr(self, '_session_start_date_utc', today):
            return
        # At most once per UTC day
        if self._last_ping_date_utc == today:
            return
        # Only when app is active (belt-and-braces with state signal)
        app = QApplication.instance()
        if app is not None and app.applicationState() != Qt.ApplicationState.ApplicationActive:
            return
        self._last_ping_date_utc = today
        telemetry.track(
            telemetry.DesktopEvent.ACTIVE_PING,
            session_id=getattr(self, '_session_id', ''),
        )
    except Exception:
        pass
```

**Why not a 24h QTimer:** Sleep/resume makes a 24h timer fire late or skip days. `applicationStateChanged` fires immediately on resume. The 5-min periodic check handles the case where the signal is missed.

---

### `tests/test_no_dynamic_telemetry_strings.py` — AST guard (D-17)

**Analog:** `tests/test_no_raw_storage_access.py` (full template — read above)

**Structure to copy:**
```python
# tests/test_no_dynamic_telemetry_strings.py
"""Lint test: producers in genizah_app.py must not source telemetry property
values from currentText()/windowTitle()/selectedFiles()/query fields (D-04/D-17).

Sibling to tests/test_no_raw_storage_access.py (Phase 87). Same approach:
  1. Define forbidden patterns as AST visitor targets.
  2. Scan the target file(s) (genizah_app.py, gui_threads.py).
  3. Confirm ZERO matches at any call site that also involves telemetry.track().
"""
import ast
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
TARGET_FILES = [
    REPO_ROOT / 'genizah_app.py',
    REPO_ROOT / 'gui_threads.py',
]

# Forbidden method names that must never appear as arguments to track() or
# in the construction of telemetry property values (D-04).
FORBIDDEN_ACCESSORS = frozenset({
    'currentText',       # QComboBox — translated EN/HE (D-05)
    'tabText',           # QTabWidget — translated EN/HE (D-01)
    'windowTitle',       # embeds shelfmark (D-04, genizah_app.py:4970 note)
    'text',              # QLabel/QLineEdit — may carry user content
    'selectedFiles',     # QFileDialog — file paths (D-04)
    'toPlainText',       # QTextEdit — query content
})
```

**Key tests to replicate from `test_no_raw_storage_access.py`:**

1. `test_lint_rejects_synthetic_violation` — confirm the visitor detects a `currentText()` call inside a `telemetry.track(...)` argument.
2. `test_no_dynamic_telemetry_strings_in_producers` — scan `genizah_app.py` for `currentText()`/`windowTitle()`/`tabText()` calls that appear as arguments in `telemetry.track()` or `_emit_search_telemetry()` call sites.

**Visitor approach for D-17** — simpler than the storage guard because we can walk all `ast.Call` nodes and check: (a) the call is to `telemetry.track` or `_emit_search_telemetry`, AND (b) any argument or keyword value contains a `Call` to a forbidden accessor. OR, conservatively: any `Call` to a forbidden accessor within `N` lines of a `telemetry.track()` call in the same function. The conservative approach (AST `FunctionDef` scoping) is easier and sufficient:

```python
class _ForbiddenAccessorInTelemetryVisitor(ast.NodeVisitor):
    """Flag functions that call both telemetry.track() AND a forbidden accessor."""

    def __init__(self):
        self.violations: list[tuple[int, str]] = []

    def visit_FunctionDef(self, node: ast.FunctionDef):
        has_track = False
        forbidden_calls: list[tuple[int, str]] = []
        for child in ast.walk(node):
            if isinstance(child, ast.Call):
                # Detect telemetry.track(...) or _emit_search_telemetry(...)
                if _is_telemetry_track_call(child):
                    has_track = True
                # Detect forbidden accessor calls
                if isinstance(child.func, ast.Attribute):
                    if child.func.attr in FORBIDDEN_ACCESSORS:
                        forbidden_calls.append((child.lineno, child.func.attr))
        if has_track and forbidden_calls:
            for lineno, name in forbidden_calls:
                self.violations.append((lineno, f"{node.name}: {name}()"))
        self.generic_visit(node)
```

**What to replicate from the storage guard test:**
- `REPO_ROOT = Path(__file__).resolve().parent.parent` path convention.
- `pytest.fail(msg)` with a multi-line message listing violations.
- A synthetic-violation test that confirms the visitor catches a known bad pattern.
- The `try/except SyntaxError` around `ast.parse()` per file.

---

## Shared Patterns

### `try/except Exception: pass` best-effort wrapper
**Source:** `genizah_app.py:175-179` (Phase 113 crash hook wiring)
**Apply to:** ALL telemetry call sites in `genizah_app.py` — coordinator, tab emit, search emit, login/logout identity, heartbeat. Telemetry MUST never raise into the calling method.
```python
try:
    from desktop import telemetry as _telemetry
    _telemetry.install_exception_hooks()
except Exception:
    pass  # telemetry is best-effort; never block app startup
```

### Deferred `from desktop import telemetry` import
**Source:** `genizah_app.py:176` and `desktop/consent_dialog.py:26`
**Apply to:** All call sites. Import at the top of the `try:` block, not at module top. This matches the established pattern and avoids import-time cost on startup.

### `telemetry.track()` call shape
**Source:** `desktop/telemetry.py:672-704`
```python
def track(event: 'str | DesktopEvent', **props) -> None:
    """Gate-checked, scrubbed event emission for desktop events."""
    try:
        if not is_enabled():
            return
        ...
        _emit(event_value, props)
    except Exception:
        logger.debug('telemetry: track() silently failed', exc_info=True)
```

**Call pattern for all producers:**
```python
telemetry.track(
    telemetry.DesktopEvent.TAB_ACTIVATED,    # always a DesktopEvent enum member
    tab_name='search',                        # D-04: hardcoded string constant
    session_id=self._session_id,
)
```
`track()` already checks `is_enabled()` internally — producers do NOT need to check `is_enabled()` before calling `track()`. Only `identify()` and `reset_identity()` also check internally; all three are consent-gated.

### `getattr(self, 'flag', safe_default)` defensive flag read
**Source:** `genizah_app.py:17686`, `genizah_app.py:3794`, `genizah_app.py:17298`
```python
was_cancelled = getattr(self, '_search_was_cancelled', False)
if not getattr(self, '_community_data_loaded', False):
if not getattr(self, 'is_searching', False):
```
**Apply to:** All new per-run state and `_restoring_session` reads. Default to the safe/suppressed value (`False`, `True` for `_restoring_session`, etc.) so a missing attribute on a very early call is handled gracefully.

### `corpus_scope_combo.currentData()` — safe corpus value
**Source:** `genizah_app.py:6152-6154`
```python
self.corpus_scope_combo.addItem("Genizah", "local_data='genizah'")
self.corpus_scope_combo.addItem("Local",   "local_data='local'")
self.corpus_scope_combo.addItem("ALL",     "local_data='all'")
```
Actual data values confirmed: `'genizah'`, `'local'`, `'all'` (from RESEARCH). `currentData()` returns the opaque data string, NOT the translated label. This is the ONE combo accessor that is safe to use as a telemetry value because it returns a fixed code, not the display text. `currentText()` would return the translated label — forbidden (D-05).

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| heartbeat `applicationStateChanged` | event producer | event-driven | No existing Qt app-state signal wiring in `genizah_app.py` (verified zero matches). The QTimer + signal pattern is standard Qt; use RESEARCH.md recommendation directly. |

---

## Metadata

**Analog search scope:** `genizah_app.py`, `gui_threads.py`, `desktop/telemetry.py`, `web/auth_state.py`, `supabase_corrections_client.py`, `tests/test_no_raw_storage_access.py`, `desktop/consent_dialog.py`
**Files scanned:** 7 source files read directly
**Pattern extraction date:** 2026-06-15

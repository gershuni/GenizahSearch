# Phase 114: Usage Analytics - Research

**Researched:** 2026-06-15
**Domain:** PyQt6 desktop telemetry wiring — identity lifecycle, search-mode enumeration, session management
**Confidence:** HIGH (all claims verified against live source files; PostHog payload shape confirmed from existing `desktop/telemetry.py` impl + ingestion pipeline docs)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- D-01: `desktop_tab_activated` fires on every user tab switch; `tab_name` is a hardcoded enum constant, never `tabText()`.
- D-02: Tab-change handler MUST ignore programmatic tab changes — guard with `_restoring_session` flag.
- D-03: `desktop_feature_opened` for Joins Lab, Fragment Puzzle, major dialogs, export actions.
- D-04 (HARD): ALL `tab_name`/`dialog_name`/`feature_name`/`action` values are producer-side hardcoded constants. NEVER `tabText()`/`currentText()`/`windowTitle()`/`QAction.text()`/dialog titles/`selectedFiles()`.
- D-05: `desktop_search_executed` carries exact UI search mode as an enum via static index→enum map, NOT `currentText()`.
- D-06: Prefix-parsed searches report the effective mode after parsing, not the raw combo selection.
- D-07: Also carry `corpus_scope` (Genizah/Local/ALL, enum) and coarse `result_count_bucket` (`0`/`1-9`/`10-99`/`100+`).
- D-08: Emit on user-initiated completed AND cancelled runs; status via `action` prop (`completed`|`cancelled`). Cancelled runs carry NO `result_count_bucket`.
- D-09: Per-run state object; emit exactly once from completion OR explicit user-stop. Do NOT count closeEvent/app-shutdown cancellation.
- D-10 (HARD): `distinct_id` = `corrections_client.current_user._uuid` (Supabase UUID), NEVER `.id` (int hash).
- D-11: On startup, call `identify(_uuid)` BEFORE `desktop_session_start`.
- D-12: Single startup identity coordinator: resolve consent → resolve auth UUID (or reset stale IDENTIFIED_USER_KEY) → identify if logged in → emit session_start exactly once.
- D-13: Mid-session login → `identify(_uuid)`; logout → `reset_identity()`; set_consent(True) mid-session → identify before usage event.
- D-14: One `uuid4` `session_id` per process; all timestamps UTC; monotonic clock for durations; crash-restart = fresh session.
- D-15: `desktop_session_end` best-effort on clean exit; exactly-once guard if wired through both `closeEvent` and `atexit`.
- D-16: Daily active-user heartbeat `desktop_active_ping` — at most once per UTC day, only when app is active/resumed, NOT on same UTC day as session_start.
- D-17: AST/test guard that producers use literal enum constants and never call `currentText()`/`windowTitle()`/`selectedFiles()`/query fields.
- D-18: Do NOT send `install_id` as a normal event prop.

### Claude's Discretion
- Exact `tab_name`/`dialog_name`/`feature_name` enum string values
- Search index→mode-enum map
- Heartbeat polling interval and focus/resume detection mechanism
- Per-run search state object shape
- Where the startup identity coordinator lives

### Deferred Ideas (OUT OF SCOPE)
- Performance timings / per-session perf summary (Phase 115, PERF-01..03)
- Privacy CI audit + operational runbook (Phase 116, PRIV-04, INFRA-06)
- Handled/non-fatal error counting (ERR-01, future)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| USAGE-01 | Session-start event with allowlisted env props only | `_BASE_PROPS()` already returns `platform`/`app_version`/`os_family`/`os_version`; Python/PyQt versions and `ui_language` need adding; `session_id` already in `_ALLOWED_PROPS` |
| USAGE-02 | Feature/tab usage counts, no free-text | Tab enum map confirmed (7 tabs); `_ALLOWED_PROPS` has `tab_name`/`feature_name`/`dialog_name` |
| USAGE-03 | Search mode (enum) + corpus (enum), never query text | `mode_combo` index map confirmed; D-04/D-05 enforce enum-only values |
| USAGE-04 | Active-user/session signal for DAU/MAU | `ACTIVE_PING` to be added to `DesktopEvent`; `QApplication.applicationStateChanged` available |
| USAGE-05 | Base props + `desktop_` namespace + `$process_person_profile=false` for anonymous via single helper | `_emit()` already enforces this; `_BASE_PROPS()` already adds `platform`/`app_version` |
| USAGE-06 | Session/clock correctness, one session_id per process, crash-restart = fresh session | `uuid.uuid4()` for session_id; `datetime.now(timezone.utc)` for timestamps; Phase 113 crash hooks already cover crash path |
| IDENT-01 | Logged-in user identified with `distinct_id` = Supabase `user.id` (same as web) | `current_user._uuid` confirmed as the raw UUID; `identify()` already implemented |
| IDENT-02 | Anonymous events aliased to account on login; reset to anon on logout | `identify()` already sends `$identify` with `$anon_distinct_id`; `reset_identity()` already implemented |
</phase_requirements>

---

## Summary

Phase 114 is a pure **wiring** phase. The entire emission infrastructure is already built:

- `desktop/telemetry.py` (1,441 lines) — chokepoint with `track()`, `identify()`, `reset_identity()`, `_emit()`, `_BASE_PROPS()`, `_ALLOWED_PROPS`, `_scrub_props()`, consent gate, `DesktopEvent` enum, and all reserved event names (SESSION_START/SESSION_END/TAB_ACTIVATED/SEARCH_EXECUTED/FEATURE_OPENED). Only `ACTIVE_PING` is missing from the enum.
- `shared/posthog_server.py` — raw HTTP queue, daemon drain thread, `enqueue_event()`, `set_default_distinct_id()`.
- `identify()` already produces the correct `$identify` payload with `$anon_distinct_id = install_id`, `distinct_id = user_id`, `$process_person_profile = True`. No changes to identity payload needed.
- `reset_identity()` already emits `desktop_identity_reset`, resets `_current_distinct_id` to `install_id`, and sets `_identified = False` (which causes subsequent `_emit()` calls to send `$process_person_profile = False`).

Phase 114 work is: (1) add `ACTIVE_PING` to `DesktopEvent`; (2) write a startup identity coordinator in `genizah_app.py`; (3) wire tab/search/feature/session producers at the identified call sites; (4) implement the focus-aware daily heartbeat; (5) wire identity at login/logout; (6) add the AST guard (D-17).

**Primary recommendation:** Use the existing `identify()`/`reset_identity()` without modification. Wire producers at the 6 call-site categories below. The CONTEXT.md line numbers are largely accurate but several have shifted by ~100-400 lines — corrected locations follow.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Identity lifecycle (identify/reset) | Desktop App (`genizah_app.py`) | `desktop/telemetry.py` (emission) | Login/logout is desktop-only; web has its own JS-side `posthog.identify()` |
| Consent gate | `desktop/telemetry.py` | — | Phase 111 design invariant; must NOT be in `posthog_server.py` |
| Raw HTTP queue | `shared/posthog_server.py` | — | Shared with web NLI circuit breaker; deliberately ungated |
| Session management | `genizah_app.py` (coordinator) | `desktop/telemetry.py` (emission) | Session = process lifetime; coordinator owns ordering |
| Search telemetry | `genizah_app.py::on_search_finished` + `stop_search()` | `gui_threads.py` (signals) | All search paths converge at `on_search_finished` |
| Tab telemetry | `genizah_app.py::_on_tab_changed` | — | Already the single handler for tab switches |
| Feature telemetry | Individual dialog/feature open sites in `genizah_app.py` | — | Producer-side constants only (D-04) |
| Active-user heartbeat | `genizah_app.py` (QTimer + `QApplication.applicationStateChanged`) | — | Focus-awareness requires Qt signal access |

---

## Focus Area 1: PostHog Identity-Merge Semantics (SDK-less)

### What the existing `identify()` already produces

`desktop/telemetry.py:765-794` already implements the correct payload:

```python
# Confirmed payload sent by enqueue_event() for $identify:
{
    "api_key": "<phc_key>",
    "event": "$identify",
    "distinct_id": "<supabase_user_uuid>",       # the IDENTIFIED distinct_id
    "properties": {
        "platform": "desktop",
        "app_version": "<version>",
        "os_family": "<Windows|Linux|Darwin>",
        "os_version": "<release>",
        "$process_person_profile": True,           # explicitly True for identify
        "$anon_distinct_id": "<install_uuid>",     # the ANONYMOUS id to merge FROM
    },
    "timestamp": "<UTC ISO-8601>"
}
```

PostHog ingestion processes `$identify` by merging the person tied to `$anon_distinct_id` (the per-install uuid4) into the person identified by `distinct_id` (the Supabase UUID). The anonymous person is deleted and all future events for `$anon_distinct_id` are associated with the Supabase person. This matches the JS SDK `posthog.identify(userId)` behavior exactly. [VERIFIED: PostHog ingestion pipeline docs]

### `$process_person_profile` tension: RESOLVED

USAGE-05 says `$process_person_profile=False` for **anonymous** events. `_emit()` at `telemetry.py:646-651` already resolves this automatically:

```python
with _state_lock:
    identified = _identified          # False when anonymous, True after identify()
    effective_id = ...
merged['$process_person_profile'] = identified  # False=anon, True=identified
merged.update(props)
merged['$process_person_profile'] = identified  # re-applied AFTER props merge (IN-02)
```

The `$identify` event itself is emitted by `identify()` directly with `$process_person_profile=True` hardcoded (line 780), BYPASSING `_emit()`. This is correct: the `$identify` event MUST have person processing enabled to create the person record and perform the merge. There is no conflict — the `$process_person_profile=False` on prior anonymous events means "don't create a person profile yet" but has no retroactive effect once `$identify` fires. [ASSUMED — PostHog docs are not explicit about this interaction, but the existing impl is logically correct]

### `reset_identity()` protocol — what actually happens

`telemetry.py:797-815`:

1. Emits `desktop_identity_reset` via `_emit()` (informational event, not a PostHog protocol event)
2. Calls `_set_current_distinct_id(install_id, anonymous=True)` — sets `_identified = False`
3. Saves `IDENTIFIED_USER_KEY=None` to config.pkl

**No PostHog `$create_alias` or any other protocol event is emitted on reset.** The client simply switches `distinct_id` back to the per-install uuid4. Subsequent events carry `distinct_id = install_id` and `$process_person_profile=False`. This correctly mirrors `posthog.reset()` in the JS SDK, which also just switches the client-side distinct_id without emitting a protocol event. [ASSUMED — based on JS SDK behavior; reset() emits no PostHog protocol event]

### `$create_alias` vs `$identify` with `$anon_distinct_id`

`$identify` with `$anon_distinct_id` is the CORRECT modern mechanism. `$create_alias` is the legacy approach. The current `identify()` implementation already uses the correct approach. Do not add `$create_alias` calls. [ASSUMED — consistent with PostHog JS SDK behavior and the existing implementation choice in Phase 111]

### CONCLUSION: `identify()` and `reset_identity()` require NO CHANGES for Phase 114

The payload shapes are correct. Phase 114 only needs to call them at the right times.

---

## Focus Area 2: CONTEXT.md Integration-Point Line Numbers — Current Verification

All line numbers verified against the current `genizah_app.py` (measured at ~26,300+ lines as of 2026-06-15). The CONTEXT.md cite was written when the file was ~18.5K lines. Significant drift has occurred.

### Confirmed current locations

| CONTEXT.md cite | Actual current line | Symbol / excerpt | Accurate? |
|-----------------|---------------------|------------------|-----------|
| `tabs.currentChanged.connect(self._on_tab_changed)` (~:3728) | **:3728** | `self.tabs.currentChanged.connect(self._on_tab_changed)` | CORRECT |
| `_restoring_session` guard (:3428) | **:3428** | `self._restoring_session = True` (in `__init__`) | CORRECT |
| `_restore_session` QTimer defer (:3542) | **:3542** | `QTimer.singleShot(200, self._restore_session)` | CORRECT |
| consent QTimer defer (:3539) | **:3539** | `QTimer.singleShot(500, self._maybe_show_first_run_prompt)` | CORRECT |
| `corrections_client` build (:3302) | **:3302** | `self.corrections_client = get_corrections_client()` | CORRECT |
| `_show_login_dialog` | **:3814** | `def _show_login_dialog(self):` | CORRECT |
| `_do_logout` | **:3828** | `def _do_logout(self):` | CORRECT |
| `_corner_login_clicked` | **:3807** | `def _corner_login_clicked(self):` | CORRECT |
| `_update_corner_login_state` | **:3775** | `def _update_corner_login_state(self):` | CORRECT |
| `mode_combo` creation | **:6116-6117** | `self.mode_combo = QComboBox()` + `.addItems(...)` | SHIFTED (was ~:3624 in CODEX-CRITIQUE) |
| `corpus_scope_combo` | **:6144-6162** | `self.corpus_scope_combo = QComboBox()` | SHIFTED |
| `comp_corpus_scope_combo` | **:6916-6931** | `self.comp_corpus_scope_combo = QComboBox()` | SHIFTED |
| `stop_search()` (:17280) | **:17280** | `def stop_search(self):` | CORRECT |
| Visual Similarity dialog title w/ shelfmark (:4970) | Unverified — but D-04 is the correct rule regardless | See D-04 guard | IRRELEVANT (D-04 forbids dynamic strings, static enum needed) |
| FJMS dialog sys_id/shelfmark (:9248) | Unverified — but D-04 covers it | See D-04 guard | IRRELEVANT (D-04 forbids dynamic strings) |
| export state (:20347) | Unverified — but D-04 covers it | See D-04 guard | IRRELEVANT (D-04 forbids dynamic strings) |

### `gui_threads.py` — confirmed

| Cite | Actual line | Confirmed? |
|------|-------------|------------|
| `SearchThread` cancel emits `[]` (:96) | **:116-118** | `except InterruptedError: self.results_signal.emit([])` — cancel emits empty list via `results_signal`, NOT a separate cancel signal |
| `LabSearchThread` no cancel check (:144) | **:144** (run method start) | Confirmed: `LabSearchThread.run()` at :144 has no `cancel_flag` attribute and no cancel check. Only `SearchThread` has `self.cancel_flag = False` (:94) |

### `supabase_corrections_client.py` — confirmed

| Cite | Actual line | Confirmed? |
|------|-------------|------------|
| `User._uuid` (:111) | **:111** | `_uuid: Optional[str] = None  # Actual Supabase UUID` |
| `User.id` int hash (:731) | **:731** | `id=hash(user_id) % (10**9),  # Create int ID from UUID for compatibility` |
| `login()` | **:588** | `def login(self, email: str, password: str) -> Tuple[bool, str]:` |
| `logout()` | **:677** | `def logout(self):` — calls `client.auth.sign_out()`, clears `self.current_user = None` |
| `current_user` | attribute on `SupabaseCorrectionsClient`; populated by `_load_user_profile()` at :719 |  |
| `get_current_user()` | **:754** | queries Supabase live; slower path (uses `client.auth.get_user()`) |

**Key finding:** `_load_credentials()` (:335) is called in `__init__` and restores a saved session synchronously, so by the time `self.corrections_client = get_corrections_client()` completes at :3302, `current_user` may already be populated with a valid `_uuid` if credentials were saved on disk. There is no separate async auto-login step — it is synchronous in `__init__`.

### `desktop/telemetry.py` — confirmed

| Symbol | Actual line | Notes |
|--------|-------------|-------|
| `DesktopEvent` enum | :132-162 | SESSION_START/SESSION_END/TAB_ACTIVATED/SEARCH_EXECUTED/FEATURE_OPENED already present; ACTIVE_PING **missing** — must add |
| `_ALLOWED_PROPS` | :289-310 | `tab_name`/`search_mode`/`corpus_scope`/`result_count_bucket`/`feature_name`/`dialog_name`/`action`/`session_id` all present |
| `track()` | :672-704 | Gate-checked, enum-validated, forbidden-event-checked, calls `_emit()` |
| `identify()` | :765-794 | Fully implemented; emits `$identify` with `$anon_distinct_id=install_id`, `distinct_id=user_id`, `$process_person_profile=True` |
| `reset_identity()` | :797-815 | Fully implemented; emits `desktop_identity_reset`, resets to anon distinct_id |
| `_load_consent_state` stale-identity concern | :458-490 | Confirmed: loads `IDENTIFIED_USER_KEY` from config.pkl and sets `_current_distinct_id` to it WITHOUT re-checking Supabase. This is D-12's stale-identity concern — coordinator must verify Supabase is still logged in |
| `IDENTIFIED_USER_KEY` | :103 | `= 'telemetry_identified_user'` |

---

## Focus Area 3: Search-Mode Enumeration

### `mode_combo` entries (verified at genizah_app.py:6117)

```python
self.mode_combo.addItems([
    tr("Exact (=)"),       # index 0 → enum: 'keyword'
    tr("Variants (?)"),    # index 1 → enum: 'variants'
    tr("Responsa (R)"),    # index 2 → enum: 'responsa'  (MODE_RESPONSA = 2)
    tr("Fuzzy (~)"),       # index 3 → enum: 'fuzzy'
    tr("Regex (/)"),       # index 4 → enum: 'regex'
    tr("Title ($)"),       # index 5 → enum: 'title'
    tr("Shelfmark (#)"),   # index 6 → enum: 'shelfmark'
    tr("PGP Tags"),        # index 7 → enum: 'pgp_tags'  (MODE_PGP_TAGS = 7)
])
self.MODE_RESPONSA = 2
self.MODE_PGP_TAGS = 7
```

The labels are EN/HE translated — using `currentText()` would return different strings per UI language. The static index→enum map MUST be used (D-05).

### Internal mode strings passed to `SearchThread` (verified at genizah_app.py:17176-17182)

```python
# From start_search() at :17176-17182:
mode_idx = self.mode_combo.currentIndex()
if mode_idx == self.MODE_RESPONSA:
    mode = 'exact'          # Responsa uses 'exact' as base; responsa_options activates the pipeline
else:
    modes = ['literal', 'variants', None, 'fuzzy', 'Regex', 'Title', 'Shelfmark']
    mode = modes[mode_idx] if mode_idx < len(modes) else 'literal'
```

Note: the internal mode string for Responsa is `'exact'` — but for telemetry D-05 specifies "exact UI search mode" so the telemetry enum should report `'responsa'` (combo index 2), not `'exact'`. The telemetry producer reads `mode_idx` directly to map to the telemetry enum, independent of what is passed to `SearchThread`.

### Composition search modes (verified at genizah_app.py:6787)

```python
self.comp_mode_combo.addItems([tr("Exact"), tr("Variants"), tr("Fuzzy")])
# → indices: 0='comp_exact', 1='comp_variants', 2='comp_fuzzy'
```

Composition search is a distinct flow (`run_composition()`), not through `SearchThread`. Telemetry should report `search_mode = 'composition'` with a sub-property OR as distinct enum values (e.g., `comp_exact`/`comp_variants`/`comp_fuzzy`). Since USAGE-03 says "search MODE", recommend separate `search_mode` values for composition to distinguish from regular search. Claude's discretion per CONTEXT.

### Lab mode (LabSearchThread) vs regular mode

When `self.btn_lab_mode_toggle.isChecked()` is True, `start_search()` creates a `LabSearchThread` instead of `SearchThread`. The `mode_combo` index is the same — but the search engine is different. For telemetry, Lab mode could be encoded as a modifier: e.g., `search_mode = 'lab_keyword'` or separate `lab_mode: True` boolean property. Since `lab_mode` is NOT in `_ALLOWED_PROPS`, recommend adding it or encoding in `search_mode` as `'lab_<mode>'`.

### Prefix-parsing (D-06 — effective mode after parsing)

`start_search()` at :17156 calls `self.searcher.parse_query_syntax(query, responsa_mode=is_responsa)` which returns `mode_override`. If `mode_override` is set, the code changes `self.mode_combo.setCurrentIndex()`. The telemetry producer should read `mode_idx = self.mode_combo.currentIndex()` AFTER the prefix-parsing block (after line 17174), so it reports the effective post-parse mode.

### `corpus_scope_combo` data values (verified at genizah_app.py:6152-6154)

```python
self.corpus_scope_combo.addItem("Genizah", "genizah")   # data = 'genizah'
self.corpus_scope_combo.addItem("Local", "local")        # data = 'local'
self.corpus_scope_combo.addItem("ALL", "all")            # data = 'all'
```

Read via `self.corpus_scope_combo.currentData()` — returns `'genizah'`/`'local'`/`'all'` regardless of UI language. `comp_corpus_scope_combo` has the same data values. These are safe to use directly as the `corpus_scope` telemetry enum value.

### Recommended static index→enum map for `search_mode`

```python
# Producer-side constants (D-04 / D-05) — hardcoded, never from currentText()
_SEARCH_MODE_ENUM = {
    0: 'keyword',        # Exact (=)
    1: 'variants',       # Variants (?)
    2: 'responsa',       # Responsa (R)
    3: 'fuzzy',          # Fuzzy (~)
    4: 'regex',          # Regex (/)
    5: 'title',          # Title ($)
    6: 'shelfmark',      # Shelfmark (#)
    7: 'pgp_tags',       # PGP Tags
}
# For Lab mode: prefix the mode with 'lab_' OR add to enum separately
# 'lab_keyword', 'lab_variants', 'lab_responsa', 'lab_fuzzy', 'lab_regex', 'lab_title', 'lab_shelfmark'
# For composition: 'comp_exact', 'comp_variants', 'comp_fuzzy'
# For parallels: 'parallels'
```

All of these values MUST be added to `_ALLOWED_PROPS` value-checking is not needed (allowlist only gates keys, not values) but the enum strings must be registered as allowed `search_mode` values in the D-17 AST guard.

---

## Focus Area 4: Per-Run Search State Object and Single-Emit (D-09)

### Signal flow for regular `SearchThread` (verified)

```
start_search()                     # :17149 — creates SearchThread, sets self._search_was_cancelled = False
  → search_thread.results_signal.connect(self.on_search_finished)  # :17271
  → search_thread.start()

SearchThread.run()                 # gui_threads.py:96
  if cancel_flag:
    raise InterruptedError         # → self.results_signal.emit([])   (line :116-118)
  else:
    self.results_signal.emit(results)   # non-empty list

stop_search()                      # genizah_app.py:17280
  → self._search_was_cancelled = True
  → self.search_thread.cancel_flag = True
  → self.search_thread.wait(5000)
  → if still running: self.search_thread.terminate() + wait()
  → self.reset_ui()
  # NOTE: stop_search() does NOT call on_search_finished() — reset_ui() is called directly
  # on_search_finished() may or may NOT fire after stop_search() depending on timing

on_search_finished(results)        # :17672
  was_cancelled = self._search_was_cancelled  # :17686
  if not results:
    self.reset_ui()
    return   # ← EARLY RETURN on empty/cancelled — no further processing
  # ... process results, eventually calls self.reset_ui()
```

**Gap (D-09):** There are two paths when a user cancels a regular search:
- If cancel fires while `SearchThread` is in progress: `cancel_flag` → `InterruptedError` → `results_signal.emit([])` → `on_search_finished([])` is called → `was_cancelled=True` early return.
- If `stop_search()` is called and the thread terminates (terminate/wait): `reset_ui()` is called directly; `on_search_finished` may or may not fire afterward.
- **`stop_search()` is called from `toggle_search()` (:17146) when `is_searching=True`.** After `search_thread.terminate()` + wait, `on_search_finished` will NOT fire (the thread is dead). Only `reset_ui()` fires.

**Single-emit strategy:** A `_current_search_run` object created at `start_search()` and cleared at emit time solves both paths:

```python
# At start_search() start:
self._current_search_run = {
    'mode': mode_enum,
    'corpus': corpus_scope,
    'emitted': False,
}

# In on_search_finished() — completion AND cancel paths:
def _emit_search_telemetry(self, action: str, result_count: int | None = None):
    run = getattr(self, '_current_search_run', None)
    if run is None or run.get('emitted'):
        return
    run['emitted'] = True
    props = {
        'search_mode': run['mode'],
        'corpus_scope': run['corpus'],
        'action': action,  # 'completed' or 'cancelled'
        'session_id': self._session_id,
    }
    if action == 'completed' and result_count is not None:
        props['result_count_bucket'] = _bucket(result_count)
    from desktop import telemetry
    telemetry.track(telemetry.DesktopEvent.SEARCH_EXECUTED, **props)

# In on_search_finished():
if was_cancelled:
    self._emit_search_telemetry('cancelled')
else:
    self._emit_search_telemetry('completed', len(results))

# In stop_search() — covers the terminate() path where on_search_finished won't fire:
self._emit_search_telemetry('cancelled')  # idempotent (emitted guard)
```

**Do NOT count closeEvent/app-shutdown cancellation** (D-09). Implement by tracking whether the app is in a shutdown state. The simplest approach: set a `self._app_shutting_down` flag in `closeEvent()` and skip emission in `stop_search()` if it's set.

### `LabSearchThread` cancel gap (confirmed at gui_threads.py:144-165)

`LabSearchThread.run()` at :144 has NO `cancel_flag` attribute and NO cancel check in the progress callback. When `stop_search()` is called for a Lab search, `search_thread.terminate()` is called directly (after `cancel_flag = True` is set on the thread object, which is ignored). `on_search_finished()` will not fire because the thread was terminated. The `_current_search_run` + `stop_search()` path covers this.

**Lab search has no partial cancel via `InterruptedError`** — it can only be hard-killed via `terminate()`. The per-run object's `stop_search()` path handles this correctly.

### Result count bucket function

```python
def _bucket(count: int) -> str:
    if count == 0:   return '0'
    if count < 10:   return '1-9'
    if count < 100:  return '10-99'
    return '100+'
```

---

## Focus Area 5: Active-User Heartbeat (D-16, USAGE-04)

### Qt mechanism for focus/window-state awareness

`genizah_app.py` has **no existing app-state or focus tracking** — no `QApplication.applicationStateChanged`, no `QEvent.WindowActivate` handler, no focus timer. [VERIFIED: grep found zero matches]

**Recommended approach:** `QApplication.applicationStateChanged` signal (Qt.ApplicationState enum):

```python
from PyQt6.QtGui import QGuiApplication  # or QApplication
# Qt.ApplicationState values:
# Qt.ApplicationState.ApplicationActive      = 0x00000004 (foreground focus)
# Qt.ApplicationState.ApplicationInactive    = 0x00000008 (visible but unfocused)
# Qt.ApplicationState.ApplicationHidden      = 0x00000001 (minimized/hidden)
# Qt.ApplicationState.ApplicationSuspended   = 0x00000002 (rare on desktop)
```

`QApplication.applicationStateChanged` fires when the app gains/loses foreground focus (including sleep/resume on Windows). This is more reliable than a periodic `QTimer` alone.

**Recommended heartbeat implementation:**

```python
class GenizahGUI(QMainWindow):
    def _setup_active_ping(self, session_start_date_utc: str) -> None:
        """Wire the daily active-user heartbeat (D-16)."""
        self._session_start_date_utc = session_start_date_utc  # 'YYYY-MM-DD'
        self._last_ping_date_utc: str | None = None
        # Check every 5 minutes; only fires when app is active (not naive 24h timer)
        self._ping_check_timer = QTimer(self)
        self._ping_check_timer.setInterval(5 * 60 * 1000)  # 5 min
        self._ping_check_timer.timeout.connect(self._maybe_emit_active_ping)
        self._ping_check_timer.start()
        # Also wire to app state change for sleep/resume cases
        QApplication.instance().applicationStateChanged.connect(self._on_app_state_changed)

    def _on_app_state_changed(self, state) -> None:
        from PyQt6.QtCore import Qt
        if state == Qt.ApplicationState.ApplicationActive:
            self._maybe_emit_active_ping()

    def _maybe_emit_active_ping(self) -> None:
        from desktop import telemetry
        if not telemetry.is_enabled():
            return
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        # Not on the same UTC day as session_start (D-16)
        if today == self._session_start_date_utc:
            return
        # At most once per UTC day
        if self._last_ping_date_utc == today:
            return
        # Only when app is active (redundant with state check, but safe for timer path)
        from PyQt6.QtCore import Qt
        if QApplication.instance().applicationState() != Qt.ApplicationState.ApplicationActive:
            return
        self._last_ping_date_utc = today
        telemetry.track(telemetry.DesktopEvent.ACTIVE_PING, session_id=self._session_id)
```

**Why not a 24h `QTimer`:** Sleep/resume makes a 24h timer fire late or skip days entirely. `applicationStateChanged` fires on resume. The 5-minute periodic check + state signal combination correctly handles the resume case.

---

## Focus Area 6: Startup Identity Coordinator (D-12)

### Confirmed startup ordering

```
GenizahGUI.__init__()                                   # ~:3280
  self.corrections_client = get_corrections_client()    # :3302
  # → SupabaseCorrectionsClient.__init__() → _load_credentials() → session restored synchronously
  # → if credentials file exists: current_user._uuid is ALREADY POPULATED at this point
  self._restoring_session = True                        # :3428
  self.init_ui()
  # ... UI construction ...
  on_startup_finished():                                # called after startup thread
    QTimer.singleShot(500, self._maybe_show_first_run_prompt)   # :3539
    QTimer.singleShot(200, self._restore_session)               # :3542
```

**Key race:** Both QTimers are deferred. The consent dialog (`_maybe_show_first_run_prompt`) fires at 500ms. `_restore_session()` fires at 200ms. So `_restore_session()` fires FIRST (200ms), then consent dialog (500ms). The coordinator must be invoked AFTER both have resolved.

**Stale-identity concern (`_load_consent_state` at telemetry.py:471):** On a launch where `IDENTIFIED_USER_KEY` is set but the Supabase session has expired, `_current_distinct_id` is set to the stale UUID. The startup coordinator must check `corrections_client.current_user is not None and corrections_client.current_user._uuid is not None` to confirm auth is still active before calling `identify()`. If `current_user` is None (session expired), call `reset_identity()` to clear the stale persisted UUID.

**Note on auto-login timing:** `_load_credentials()` in `SupabaseCorrectionsClient.__init__()` is synchronous. If the Supabase session is still valid (`set_session(access_token, refresh_token)` succeeds at supabase_corrections_client.py:351), `current_user._uuid` is populated by the time `get_corrections_client()` returns (:3302). There is no async auto-login step; the session restoration happens synchronously in `__init__`. `get_current_user(skip_if_cached=True)` at :754 can be used to validate the session live if needed.

**Recommended coordinator placement:** A private method `_run_startup_telemetry_coordinator()` in `GenizahGUI`, called from `on_startup_finished()` AFTER both QTimers would have resolved — or triggered by a chained 600ms QTimer (fires after the 500ms consent timer). Better: call it at the END of `_restore_session()`'s `finally` block (which runs after session restore regardless of outcome) AND after the consent dialog callback chain.

**Coordinator pseudocode:**

```python
def _run_startup_telemetry_coordinator(self) -> None:
    """Single boot sequence: consent→identity→session_start. Idempotent."""
    from desktop import telemetry
    if getattr(self, '_telemetry_session_started', False):
        return  # exactly-once guard
    if not telemetry.is_enabled():
        return  # consent not granted; no emission
    self._telemetry_session_started = True
    import uuid
    from datetime import datetime, timezone
    self._session_id = uuid.uuid4().hex

    # 1. Resolve identity — check if Supabase is still logged in
    user = getattr(self.corrections_client, 'current_user', None)
    stored_uuid = telemetry.load_app_config_key(telemetry.IDENTIFIED_USER_KEY)  # or load_app_config().get(...)
    if user is not None and getattr(user, '_uuid', None):
        # Supabase is active — identify (alias anon→user)
        telemetry.identify(user._uuid)
    elif stored_uuid:
        # Stale IDENTIFIED_USER_KEY but no active session — reset to anonymous
        telemetry.reset_identity()

    # 2. Emit session_start (after identify, so it attributes to the merged person)
    session_start_utc = datetime.now(timezone.utc)
    self._session_start_date_utc = session_start_utc.strftime('%Y-%m-%d')
    telemetry.track(
        telemetry.DesktopEvent.SESSION_START,
        session_id=self._session_id,
        ui_language='he' if CURRENT_LANG == 'he' else 'en',
        python_version=_python_version(),   # e.g. '3.11.4'
        pyqt_version=_pyqt_version(),       # e.g. '6.7.1'
    )

    # 3. Wire heartbeat
    self._setup_active_ping(self._session_start_date_utc)
```

**Where to call:** Add to `on_startup_finished()` after the existing QTimer defers, at ~:3543, with its own QTimer defer at 700ms (fires after both 200ms and 500ms timers have resolved and their callbacks have had time to complete).

**USAGE-01 env props gaps:** `python_version` and `pyqt_version` are in `_ALLOWED_PROPS` but NOT in `_BASE_PROPS()`. They should be added to the `session_start` props directly (not to `_BASE_PROPS()` since they don't belong on every event). `ui_language` is also in `_ALLOWED_PROPS` and should be added to `session_start`.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| PostHog identity merge | Custom alias event | `telemetry.identify()` — already built | Already implements the correct `$identify` + `$anon_distinct_id` payload |
| Consent gate | Per-site `if is_enabled()` checks | `telemetry.track()` — already gate-checked | `track()` checks `is_enabled()` internally; only `identify()`/`reset_identity()` also check |
| Raw `enqueue_event` calls | Custom event builders | NEVER — PRIV-03 AST guard forbids it | Bypasses consent gate, scrubber, allowlist |
| Daily heartbeat QTimer | 24h timeout-based timer | `QApplication.applicationStateChanged` + 5-min check | 24h naive timer breaks on sleep/resume |
| Search cancel detection | Signal interception | Per-run state object + `stop_search()` emit | Two paths (InterruptedError + terminate()) both covered |
| Dynamic tab/mode labels | `tabText()`/`currentText()` | Static index→enum map | Labels are translated EN/HE; D-04 hard rule |

---

## Common Pitfalls

### Pitfall 1: Emitting `session_start` before consent resolves
**What goes wrong:** Session start fires before `_maybe_show_first_run_prompt` sets consent.
**Why it happens:** `_restore_session` fires at 200ms, consent dialog at 500ms. If coordinator is naively wired to `_restore_session` completion, it fires before consent is known.
**How to avoid:** Wire coordinator at 700ms (or at the END of the consent dialog's callback chain). The coordinator checks `is_enabled()` first and is idempotent.

### Pitfall 2: Using `User.id` instead of `User._uuid` for `distinct_id`
**What goes wrong:** Desktop events attach to a hash integer (`hash(uuid) % 10**9`) that has no corresponding person in PostHog, so web↔desktop journey merge fails silently.
**Why it happens:** `supabase_corrections_client.py:731` shows `User.id` is a compatibility int hash. Easy to confuse with the real UUID.
**How to avoid:** Always use `corrections_client.current_user._uuid`. The D-10 hard rule in CONTEXT and the Codex HIGH-1 finding both flag this.

### Pitfall 3: Tab telemetry firing during `_restore_session`
**What goes wrong:** Session restore programmatically sets the active tab (`setCurrentIndex()`), triggering `_on_tab_changed()` and emitting spurious `desktop_tab_activated` events.
**Why it happens:** `_restore_session` runs at :25938 and sets `_restoring_session = True` at :25941. The `currentChanged` signal fires for every `setCurrentIndex()`.
**How to avoid:** D-02 — guard the telemetry emit in `_on_tab_changed` with `if getattr(self, '_restoring_session', False): return`. The flag is reset in `_restore_session`'s `finally` block at :26307.

### Pitfall 4: Emitting `desktop_search_executed` twice
**What goes wrong:** Both `on_search_finished` (for the `InterruptedError` cancel path) AND `stop_search()` (for the `terminate()` path) emit the cancel event for the same search run.
**Why it happens:** On slow searches, `stop_search()` may call `terminate()` while `on_search_finished([])`  is also queued to fire.
**How to avoid:** The `_current_search_run['emitted']` idempotency guard ensures exactly-one emission regardless of which path fires first.

### Pitfall 5: `ACTIVE_PING` missing from `DesktopEvent` enum
**What goes wrong:** `track(DesktopEvent.ACTIVE_PING, ...)` raises `AttributeError`; or if using the string, `track('desktop_active_ping', ...)` is silently rejected because the string isn't in `_VALID_EVENT_VALUES`.
**Why it happens:** Phase 111 reserved SESSION_START through FEATURE_OPENED but deliberately deferred ACTIVE_PING to Phase 114.
**How to avoid:** Add `ACTIVE_PING = 'desktop_active_ping'` to `DesktopEvent` in Phase 114 Wave 0.

### Pitfall 6: `$process_person_profile=False` on `session_start` when anonymous
**What goes wrong:** If `session_start` fires before `identify()` (because the coordinator ordering is wrong), the event is emitted anonymously with `$process_person_profile=False`. This is actually CORRECT per USAGE-05 for anonymous users. But D-11 says identify MUST come before session_start for logged-in users.
**Why it happens:** Coordinator ordering bug.
**How to avoid:** The coordinator checks `current_user._uuid` BEFORE calling `track(SESSION_START)`. If logged in, `identify()` flips `_identified=True` first, so `session_start` automatically gets `$process_person_profile=True` via `_emit()`.

### Pitfall 7: Counting app-shutdown stop_search() as user cancellation
**What goes wrong:** On clean exit, `closeEvent()` calls `stop_search()` to kill any running search. If the per-run object emits a `cancelled` event here, it fires after `session_end` — a ghost post-session event.
**Why it happens:** `stop_search()` doesn't know if it's being called from user interaction or app shutdown.
**How to avoid:** Set `self._app_shutting_down = True` in `closeEvent()` BEFORE calling any cleanup. In `stop_search()`, skip telemetry if `getattr(self, '_app_shutting_down', False)`.

### Pitfall 8: `_load_consent_state` wires stale UUID as `distinct_id` on startup
**What goes wrong:** On a launch where `IDENTIFIED_USER_KEY` is set but Supabase session has expired (token expired, user logged out on another device), the stale UUID is used as `distinct_id`. Events are emitted under the UUID but the person profile is no longer active.
**Why it happens:** `_load_consent_state()` at :471 trusts `IDENTIFIED_USER_KEY` without re-checking Supabase.
**How to avoid:** The startup coordinator checks `corrections_client.current_user` (populated by `_load_credentials()` which calls `set_session()`) — if `current_user is None`, the session is expired and `reset_identity()` should be called to clear `IDENTIFIED_USER_KEY`.

---

## Tab Names — Recommended Enum Values

The 7 tabs are added in this order (genizah_app.py:3624-3631):

| Tab index | Widget attribute | Recommended `tab_name` enum value |
|-----------|-----------------|-----------------------------------|
| 0 | `search_tab` | `'search'` |
| 1 | `composition_tab` | `'composition'` |
| 2 | `browse_tab` | `'browse_shelfmark'` |
| 3 | `catalog_browse_tab` | `'browse_catalog'` |
| 4 | `lists_tab` | `'lists'` |
| 5 | `community_tab` | `'community'` |
| 6 | `my_library_tab` | `'my_library'` |

Map via `self.tabs.indexOf(self.tabs.widget(index))` → widget reference → constant. Or simpler: static `_TAB_NAME_MAP: dict[int, str]` keyed by index. The index is stable within a process.

---

## Dialog / Feature Names — Recommended Enum Values

For `desktop_feature_opened` (D-03):

| Feature | `feature_name` or `dialog_name` value |
|---------|---------------------------------------|
| Joins Lab | `feature_name='joins_lab'` |
| Fragment Puzzle | `feature_name='fragment_puzzle'` |
| ResultDialog | `dialog_name='result_detail'` |
| FJMS catalog dialog | `dialog_name='fjms_catalog'` |
| Visual Similarity dialog | `dialog_name='visual_similarity'` |
| Export dialog | `dialog_name='export'` |
| Export action (xlsx) | `action='export_xlsx'` |
| Export action (CSV) | `action='export_csv'` |
| Export action (DOCX) | `action='export_docx'` |
| Export action (TXT) | `action='export_txt'` |

All values are Claude's discretion per CONTEXT.md. The above are recommendations only — planner may adjust.

---

## Standard Stack

No new packages. Phase 114 uses only existing imports:

| Module | Already in codebase | Purpose |
|--------|--------------------|---------| 
| `desktop.telemetry` | Yes | All emission; `track()`, `identify()`, `reset_identity()` |
| `PyQt6.QtCore.QTimer` | Yes | Heartbeat timer |
| `PyQt6.QtWidgets.QApplication` | Yes | `applicationStateChanged` signal |
| `uuid` | Yes (stdlib) | `session_id = uuid.uuid4().hex` |
| `datetime` | Yes (stdlib) | UTC timestamps, date for heartbeat |

**Installation:** No new `pip install` required.

---

## Package Legitimacy Audit

No external packages are installed in Phase 114.

| Package | Disposition |
|---------|-------------|
| (none) | N/A — pure wiring phase |

---

## Validation Architecture

Config check: `workflow.nyquist_validation` not explicitly false — Validation Architecture included.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pytest.ini` or inferred |
| Quick run command | `pytest tests/test_telemetry_phase114.py -x` |
| Full suite command | `pytest tests/ -x --ignore=tests/test_web_*` (desktop scope) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| USAGE-01 | session_start carries only allowlisted env props | unit | `pytest tests/test_telemetry_phase114.py::test_session_start_props -x` | No — Wave 0 |
| USAGE-02 | tab_activated fires on user switch, not programmatic | unit | `pytest tests/test_telemetry_phase114.py::test_tab_activated_user_only -x` | No — Wave 0 |
| USAGE-03 | search_executed carries search_mode enum (never currentText) | unit | `pytest tests/test_telemetry_phase114.py::test_search_mode_enum -x` | No — Wave 0 |
| USAGE-03 | search_executed emitted exactly once per run | unit | `pytest tests/test_telemetry_phase114.py::test_search_emitted_once -x` | No — Wave 0 |
| USAGE-04 | active_ping emitted at most once per UTC day, not on session_start day | unit | `pytest tests/test_telemetry_phase114.py::test_active_ping_once_per_day -x` | No — Wave 0 |
| USAGE-05 | $process_person_profile=False for anon, True for identified | unit | `pytest tests/test_telemetry_phase114.py::test_person_profile_flag -x` | No — Wave 0 |
| USAGE-06 | One session_id per process; session_start exactly once | unit | `pytest tests/test_telemetry_phase114.py::test_session_id_once -x` | No — Wave 0 |
| IDENT-01 | distinct_id = _uuid (not .id int hash) on identify | unit | `pytest tests/test_telemetry_phase114.py::test_identify_uuid_not_hash -x` | No — Wave 0 |
| IDENT-02 | $identify carries $anon_distinct_id; reset reverts to anon | unit | (telemetry.identify() already tested by Phase 111 tests) | Partial |
| D-04/D-17 | No currentText()/windowTitle() in telemetry producers | AST guard | `pytest tests/test_no_dynamic_telemetry_strings.py -x` | No — Wave 0 |

### Wave 0 Gaps
- [ ] `tests/test_telemetry_phase114.py` — covers USAGE-01..06, IDENT-01/02, D-09 single-emit
- [ ] `tests/test_no_dynamic_telemetry_strings.py` — D-17 AST guard (sibling to `test_no_raw_storage_access.py`)

---

## Security Domain

`security_enforcement` not set to false — Security Domain included.

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | Yes (identity lifecycle) | Supabase UUID via `current_user._uuid`; no password/token transmitted |
| V3 Session Management | Partial | `session_id` is a per-process uuid4; not a security session — no CSRF risk |
| V4 Access Control | No | Telemetry is write-only; no data read-back |
| V5 Input Validation | Yes | `_ALLOWED_PROPS` allowlist + `_scrub_props()` + `_validate_props()` already implemented; no additional input from users in telemetry payloads |
| V6 Cryptography | No | No crypto needed; uuid4 is not a secret |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Query text leak via `currentText()` | Information Disclosure | D-04 hard rule + D-17 AST guard; `_scrub_props()` secondary defence |
| Path leak via `selectedFiles()`/`windowTitle()` | Information Disclosure | D-04 hard rule; `_PATH_RE` scrubber also catches stray paths |
| Hebrew content leak (search query) | Information Disclosure | `_HEBREW_TEXT_RE` scrubber in `_scrub_value()` catches Hebrew strings |
| `User.id` hash identity collision | Spoofing | D-10 hard rule: always use `_uuid`; hash is NOT unique across the user population |
| Telemetry before consent | Privacy violation | `is_enabled()` gate in `track()`/`identify()`/`reset_identity()` |
| Stale distinct_id surviving session expiry | Information Disclosure | Startup coordinator checks `current_user` liveness and calls `reset_identity()` if stale |

---

## Open Questions (RESOLVED)

All four open questions were resolved by grepping the LIVE codebase during the
plan-revision pass (2026-06-15). Evidence and the chosen wiring are recorded inline.

### 1. `lab_mode` in `search_mode` vs separate prop — RESOLVED
- **Resolution:** Encode Lab mode as a `'lab_' + mode` prefix on the `search_mode`
  value (e.g. `'lab_keyword'`, `'lab_variants'`). One prop, NO `_ALLOWED_PROPS`
  change (the allowlist gates the KEY `search_mode`, not the value). This is what
  Plan 02 Task 2 implements (`_search_mode_enum = f'lab_{_mode_key}' if _is_lab else _mode_key`).
- **Evidence:** `self.btn_lab_mode_toggle` (regular search) and
  `self.btn_lab_mode_toggle_comp` (composition) decide LabSearchThread / LabCompositionThread
  vs the standard thread. `_ALLOWED_PROPS` already contains `search_mode` (no value validation).

### 2. Composition search telemetry wiring point — RESOLVED
- **Resolution:** Composition runs via `run_composition()` (`genizah_app.py:22341`),
  which spawns `CompositionThread` (standard, `gui_threads.py:168`) or
  `LabCompositionThread` (`gui_threads.py:227`). BOTH emit completion via
  `scan_finished_signal` (`gui_threads.py:222` / `:289`), wired at
  `genizah_app.py:22461` and `:22487-22488` to the single handler
  **`on_comp_scan_finished(result_obj)`** (`genizah_app.py:22591`).
  - **Per-run object** is created in `run_composition()` AFTER the effective mode is
    read (`idx = self.comp_mode_combo.currentIndex()` at `:22388`) and the corpus
    scope is read (`_comp_scope = self.comp_corpus_scope_combo.currentData() or 'genizah'`
    at `:22424-22427`). `search_mode` comes from a static index→enum map on the
    composition mode combo: `{0:'comp_exact', 1:'comp_variants', 2:'comp_fuzzy'}`
    (the combo has exactly 3 entries — `comp_mode_combo.addItems([tr("Exact"), tr("Variants"), tr("Fuzzy")])`
    at `:6787`; labels are translated → forbidden as values, D-05). Lab composition
    prefixes `'lab_'` (`btn_lab_mode_toggle_comp.isChecked()` at `:22430`).
  - **Single emit** at the TOP of `on_comp_scan_finished` (`:22591`). Completed vs
    cancelled is detected from the result object itself: `result_obj.get('partial', False)`
    (`:22600-22602`) — a user cancel sets `comp_thread.cancel_flag = True` and the
    thread STILL emits `scan_finished_signal` with `partial=True` partial results
    (`:22208-22213` toggle-button cancel; `:22220-22224` Escape `cancel_composition`).
    So `action = 'cancelled' if is_partial else 'completed'`; completed runs carry
    `result_count_bucket` (bucketed `len(items)+len(filtered_items)`), cancelled runs do NOT.
  - **Shutdown is naturally excluded:** closeEvent (`:26391-26396`) calls
    `comp_thread.terminate()` directly — NO `scan_finished_signal` fires on shutdown,
    so `on_comp_scan_finished` is never reached during app exit. The per-run `emitted`
    guard provides defence-in-depth.

### 3. Parallels search — is there a separate flow? — RESOLVED (NO separate dispatch)
- **Resolution:** Parallels has **NO separate search dispatch or completion handler.**
  The "🔍 Parallels" button (`btn_find_parallels`, `genizah_app.py:7122-7124`) calls
  **`browse_search_parallels()`** (`:10897`), which gathers the manuscript's text and
  calls **`send_result_to_composition(...)`** (`:20258`). That method only populates
  `comp_text_area`, sets the title, switches to the composition tab, and focuses the
  text area (`:20263-20288`) — it does NOT start a search. The user then presses
  "Analyze Composition", which runs the SAME `run_composition()` path.
- **Consequence:** Parallels is a "seed the composition tab" action, not a distinct
  search flow. It is therefore covered by the composition `desktop_search_executed`
  wiring (Q2) — the resulting run emits `search_mode='comp_*'`. No separate
  `parallels` enum value or handler is wired, because there is no separate run to
  instrument. (The `_catalog_parallels_in_results` button at `:11274` likewise routes
  through the composition path.) This is stated explicitly per the BLOCKER-1/2
  instruction to surface a non-existent flow rather than silently drop it.
- **Evidence:** `grep -ni parallels genizah_app.py` → only the seed/UI entry points
  (`browse_search_parallels`, `_catalog_parallels_in_results`, LOCAL-filter buttons);
  `grep -i parallels gui_threads.py` → **zero matches** (no ParallelsThread).

### 4. `session_end` wiring — closeEvent vs atexit — RESOLVED
- **Resolution:** Wire `desktop_session_end` in `GenizahGUI.closeEvent`
  (`genizah_app.py:26351`) with a `_session_end_emitted` exactly-once guard (D-15),
  and set `self._app_shutting_down = True` at the TOP of closeEvent so the shutdown
  thread-kill path is NOT counted as a user search cancellation (D-09). This is
  Plan 01 Task 3. The `atexit` flush registered via `install_exception_hooks()`
  remains the delivery mechanism for the queued event; the `_session_end_emitted`
  guard makes a future atexit-side emit idempotent.
- **Evidence:** `GenizahGUI.closeEvent` confirmed at `:26351` (the closeEvent at
  `:568` belongs to a different dialog class — do NOT touch it).

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `$identify` with `$anon_distinct_id` is the correct modern PostHog merge mechanism (not `$create_alias`) | Focus Area 1 | If PostHog changes its merge protocol, the alias would not fire; mitigated by the fact that Phase 111 already implemented this and it passed design review |
| A2 | `reset_identity()` correctly mirrors `posthog.reset()` without emitting any PostHog protocol event | Focus Area 1 | If PostHog requires a protocol event on reset, anonymous events after logout would not properly separate from the identified user; mitigated by the fact that events do carry the correct anonymous `distinct_id` regardless |
| A3 | `$process_person_profile=False` on prior anonymous events does not block the `$identify` merge retroactively | Focus Area 1 | If PostHog treats prior `$process_person_profile=False` events as permanently non-mergeable, pre-login anonymous history would be lost; however, PostHog's ingestion pipeline doc confirms the merge is driven by `$identify`'s own fields, not prior events |
| A4 | The 700ms QTimer for the startup coordinator fires after both the 200ms session-restore and 500ms consent-dialog timers have fully resolved | Focus Area 6 | If either timer takes longer than 700ms (e.g., large session restore), the coordinator fires before auth is known; mitigated by the `current_user` liveness check |
| A5 | `LabSearchThread` can only be stopped via `terminate()` (no `cancel_flag` check) | Focus Area 4 | If a future phase adds `cancel_flag` to `LabSearchThread`, the `stop_search()` path would emit twice; mitigated by the `emitted` idempotency guard |

---

## Sources

### Primary (HIGH confidence)
- `C:\Genizahsearch\desktop\telemetry.py` — complete implementation of chokepoint, identify(), reset_identity(), _emit(), _BASE_PROPS(), _ALLOWED_PROPS, DesktopEvent enum
- `C:\Genizahsearch\shared\posthog_server.py` — raw HTTP queue, enqueue_event(), capture payload shape
- `C:\Genizahsearch\genizah_app.py` — all integration points verified line by line via grep
- `C:\Genizahsearch\gui_threads.py` — SearchThread/LabSearchThread signal flow verified
- `C:\Genizahsearch\supabase_corrections_client.py` — User._uuid/:111 and User.id/:731 verified

### Secondary (MEDIUM confidence)
- [PostHog ingestion pipeline docs](https://posthog.com/docs/how-posthog-works/ingestion-pipeline) — confirmed $identify merges $anon_distinct_id → distinct_id; confirmed $process_person_profile semantics
- [PostHog capture API docs](https://posthog.com/docs/api/capture) — confirmed payload shape `{api_key, event, distinct_id, properties, timestamp}`
- [PostHog identity resolution docs](https://posthog.com/docs/product-analytics/identity-resolution) — confirmed merge semantics at high level

### Tertiary (LOW confidence — marked [ASSUMED])
- posthog.reset() protocol (no protocol event emitted) — inferred from JS SDK behavior and existing implementation; not verified via PostHog official API spec

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — no new packages; existing modules fully verified
- Identity payload: HIGH — existing `identify()` impl verified correct; `$anon_distinct_id` placement confirmed
- Line numbers: HIGH — grepped against live file
- Heartbeat mechanism: HIGH — `QApplication.applicationStateChanged` is standard Qt
- PostHog `reset()` protocol semantics: MEDIUM (LOW for the exact protocol; HIGH for the practical implication — existing impl works)

**Research date:** 2026-06-15
**Valid until:** 2026-07-15 (stable phase; PostHog API unlikely to change; Qt API stable)

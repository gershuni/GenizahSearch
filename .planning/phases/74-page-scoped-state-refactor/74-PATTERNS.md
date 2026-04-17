# Phase 74: Page-Scoped State Refactor - Pattern Map

**Mapped:** 2026-04-17
**Files analyzed:** 9 (2 CREATE, 5 MODIFY-add-helpers, 2 MODIFY-runtime-dataflow-only)
**Analogs found:** 9 / 9

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/browse_bootstrap.py` | pure precedence helper | request-response (pure fn) | `web/search_bootstrap.py` | exact mirror |
| `tests/test_browse_bootstrap.py` | unit test for precedence helper | — | `tests/test_search_bootstrap.py` | exact mirror |
| `web/pages/search_state.py` (add helpers) | page-scoped state + persistence helpers | CRUD (storage hydrate/serialize) | `web/pages/search_state.py:202–251` (existing `add_to_search_history` / `get_search_history` pattern) | role-match (same module, same storage pattern) |
| `web/pages/browse_state.py` (add helpers) | page-scoped state + persistence helpers | CRUD (storage hydrate/serialize) | `web/pages/search_state.py:202–251` | role-match (cross-module mirror) |
| `web/pages/search.py` (Cat-1 sweep + snapshot calls) | page controller, event handlers | request-response | self (existing patterns at lines 95–161 and 805–832) | self-analog |
| `web/pages/browse.py` (Cat-1 sweep + snapshot calls + bootstrap extract) | page controller, event handlers | request-response | self + `web/search_bootstrap.py` | self-analog + exact |
| `web/components/filter_panel.py` (Cat-1 sweep) | component, event handlers | request-response | `web/components/filter_panel.py:449–498` (self — existing `ensure_future` sites) | self-analog |
| `web/pages/browse_enrichment.py` (audit 1 site) | enrichment service | request-response | `web/pages/search_results.py:111` (Cat-2 keep pattern) | role-match |
| `tests/e2e/test_browse_flow.py` (add E2E assertion) | E2E test | — | `tests/e2e/test_browse_flow.py:68–121` (existing TestBrowseNavigation class) | exact (extend) |

---

## Pattern Assignments

### `web/browse_bootstrap.py` (CREATE — pure precedence helper)

**Analog:** `web/search_bootstrap.py` (full file, 68 lines)

**Module header + imports pattern** (`web/search_bootstrap.py` lines 1–6):
```python
"""Helpers for deterministic search-page bootstrap state."""

from __future__ import annotations

from typing import Any, Dict
```
Browse equivalent:
```python
"""Helpers for deterministic browse-page bootstrap state."""

from __future__ import annotations

from typing import Any, Dict
```

**Function signature pattern** (`web/search_bootstrap.py` lines 22–33):
```python
def resolve_search_bootstrap(
    *,
    initial_query: str | None,
    initial_tag: str | None,
    initial_mode: str | None,
    initial_domain: str | None,
    from_browse: int | None,
    saved_mode: str | None,
    saved_query: str | None,
    use_slider: bool,
) -> Dict[str, Any]:
    """Resolve whether persisted search UI state should be reused for this request."""
```
Browse equivalent — all inputs are **pure data** (no `app.storage.user` reads inside the function):
```python
def resolve_browse_bootstrap(
    *,
    initial_fl_id: str | None,
    initial_sys_id: str | None,
    initial_page: int,
    pending_shelfmark: str | None,
    saved_reading_desk: dict | None,   # caller passes app.storage.user.get('reading_desk_state')
    saved_position: dict | None,        # caller passes app.storage.user.get('browse_position')
) -> Dict[str, Any]:
    """Resolve browse bootstrap action without scheduling async tasks.

    Returns a dict describing what action to take:
    {
        'action': 'fl_id' | 'sys_id' | 'shelfmark' | 'restore_desk' | 'restore_position' | 'none',
        'restore_desk': bool,
        'clear_desk': bool,
        'p_num': int,
        'fl_id': str | None,
        'sys_id': str | None,
        'shelfmark': str | None,
    }
    Callers use the returned dict to dispatch the correct load_page() call.
    """
```

**Return dict pattern** (`web/search_bootstrap.py` lines 60–67):
```python
    return {
        'mode': resolved_mode,
        'query': resolved_query,
        'restore_saved_results': restore_saved_state,
        'restore_saved_filters': restore_saved_state and not bool(from_browse),
        'restore_saved_exclusions': restore_saved_state,
    }
```
Browse: return a flat dict with `'action'` key + all fields needed by the caller to dispatch `load_page()`. No side effects. No async. No `app.storage` reads.

**Three precedence cases to implement** (extracted from `web/pages/browse.py` lines 4466–4532):

Case 1 — `initial_fl_id` wins all:
```python
# browse.py:4467–4471 (source of truth for fl_id precedence)
if initial_fl_id_value:
    state.is_loading = True
    update_content()  # Show spinner synchronously before async kicks in
    asyncio.ensure_future(load_page(fl_id=initial_fl_id_value))
```

Case 2 — `initial_sys_id` with reading-desk collision detection:
```python
# browse.py:4472–4505 (source of truth for sys_id + desk precedence)
elif initial_sys_id:
    saved_rd = app.storage.user.get('reading_desk_state')
    if saved_rd and saved_rd.get('entries'):
        persisted_sids = {e.get('sys_id', '') for e in saved_rd['entries']}
        if initial_sys_id in persisted_sids:
            # Language-switch: restore the full reading desk
            ...
        else:
            # Cross-page navigation: clear stale desk, load requested manuscript
            app.storage.user.pop('reading_desk_state', None)
            asyncio.ensure_future(load_page(p_num=initial_page))
    else:
        asyncio.ensure_future(load_page(p_num=initial_page))
```

Case 3 — blank `/browse` with saved desk or position:
```python
# browse.py:4510–4532 (source of truth for restore precedence)
else:
    if _restore_reading_desk_state():
        pass  # Reading desk restored
    else:
        saved_position = app.storage.user.get('browse_position')
        if saved_position and saved_position.get('sys_id'):
            state.sys_id = saved_position['sys_id']
            asyncio.ensure_future(load_page(p_num=saved_position.get('p_num', 1)))
        else:
            update_content()
```

**Key design rule:** `resolve_browse_bootstrap` encodes ONLY the logic branches above (returning a dict), never the `asyncio.ensure_future` calls themselves. The caller in `browse.py` remains responsible for scheduling.

---

### `tests/test_browse_bootstrap.py` (CREATE — unit tests)

**Analog:** `tests/test_search_bootstrap.py` (full file, 103 lines)

**Import + test structure pattern** (lines 1–22):
```python
from web.search_bootstrap import resolve_search_bootstrap


def test_restores_saved_state_without_route_context():
    state = resolve_search_bootstrap(
        initial_query=None,
        initial_tag=None,
        initial_mode=None,
        initial_domain=None,
        from_browse=None,
        saved_mode='Title',
        saved_query='ישן',
        use_slider=False,
    )

    assert state == {
        'mode': 'Title',
        'query': 'ישן',
        'restore_saved_results': True,
        'restore_saved_filters': True,
        'restore_saved_exclusions': True,
    }
```
Browse equivalent:
```python
from web.browse_bootstrap import resolve_browse_bootstrap


def test_explicit_sys_id_beats_saved_position():
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id='003750',
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position={'sys_id': '000001', 'p_num': 3, 'shelfmark': 'T-S 1.1', 'volume_ie': None},
    )
    assert result['action'] == 'sys_id'
    assert result['sys_id'] == '003750'
```

**Three required test cases** (D-19):

(a) `test_explicit_sys_id_beats_saved_position` — `initial_sys_id='003750'` with `saved_position` set to a different manuscript; assert `result['action'] == 'sys_id'`.

(b) `test_blank_browse_restores_saved_position` — all URL params `None`, `saved_position={'sys_id': 'X', 'p_num': 2, ...}`, no desk; assert `result['action'] == 'restore_position'` and `result['p_num'] == 2`.

(c) `test_blank_browse_desk_wins_over_position` — all URL params `None`, `saved_reading_desk={'entries': [...]}`, `saved_position` also set; assert `result['action'] == 'restore_desk'`.

**No fixtures, no mocking.** Each test constructs plain dicts and asserts on the returned dict, same as `test_search_bootstrap.py`. Zero NiceGUI / storage imports.

---

### `web/pages/search_state.py` (MODIFY — add snapshot helper triple)

**Analog:** existing `web/pages/search_state.py` lines 198–251 (`add_to_search_history` / `get_search_history`)

**Existing storage helper pattern to mirror** (lines 202–211):
```python
def get_search_history() -> list:
    """Get search history from storage."""
    return app.storage.user.get('search_history', [])


def add_to_search_history(query: str, result_count: int, mode: str, params: dict, state_snapshot: dict):
    """Add or update a search history entry. Deduplicates by query+mode."""
    if not app.storage.user.get('session_persistence_enabled', True):
        return
    limit = app.storage.user.get('search_history_limit', 20)
    ...
    app.storage.user['search_history'] = history
```

**Existing restore block to centralize** (`web/pages/search.py` lines 95–161, the direct pattern for `restore_search_snapshot`):
```python
# search.py:95–99 (reads snapshot keys into locals before passing to bootstrap)
raw_saved_mode = app.storage.user.get('search_mode', 'exact')
raw_saved_query = app.storage.user.get('search_query', '')
saved_preset = app.storage.user.get('search_preset', 30)
saved_max_changes = app.storage.user.get('search_max_changes', 2)
saved_gap = app.storage.user.get('search_gap', 0)

# search.py:122–161 (actual hydration into state object)
if restore_saved_exclusions:
    _de = app.storage.user.get('domain_exclusions')
    search_state.domain_exclusions = set(_de) if _de is not None else set()
    search_state.printed_filter = app.storage.user.get('search_printed_filter', 'all')
...
if restore_saved_filters and not _filters_from_browse:
    load_filter_state(search_state, 'search')
...
_saved_refinement_chain = app.storage.user.get('search_refinement_chain', [])
if _saved_refinement_chain and restore_saved_results:
    try:
        search_state.refinement_chain = [RefinementStep.from_dict(d) for d in _saved_refinement_chain]
    except Exception:
        search_state.refinement_chain = []
```

**Existing clear block to centralize** (`web/pages/search.py` lines 805–832 and 2019–2025):
```python
# search.py:806–832 (filter keys reset)
app.storage.user['search_filter_domains'] = []
app.storage.user['search_filter_authors'] = []
...
for _mk in ['width_min', 'width_max', ...]:
    app.storage.user[f'search_filter_{_mk}'] = None

# search.py:2019–2025 (main snapshot keys reset)
app.storage.user['search_results'] = []
app.storage.user['search_query'] = ''
app.storage.user['search_mode'] = 'exact'
app.storage.user['domain_exclusions'] = []
app.storage.user['search_printed_filter'] = 'all'
app.storage.user['word_search_excluded_ids'] = []
app.storage.user['search_exclusion_sources'] = []
```

**New helper signatures to add** (D-06, from RESEARCH §3.2):
```python
_SEARCH_SNAPSHOT_VERSION = 1

def restore_search_snapshot(state: SearchUIState) -> None:
    """Hydrate page-scoped state from app.storage.user snapshot.
    Called once at page mount. After this call, SearchUIState is authoritative —
    direct app.storage.user reads for snapshot keys are forbidden (D-03).
    Silently discards snapshot if version stamp is missing or stale (D-04).
    """
    stored_version = app.storage.user.get('search_snapshot_schema_version', 0)
    if stored_version != _SEARCH_SNAPSHOT_VERSION:
        clear_search_snapshot()
        return
    # ... read all restorable_page_snapshot keys into state fields ...
    # ... call load_filter_state(state, 'search') for filter keys ...

def persist_search_snapshot(state: SearchUIState) -> None:
    """Serialize restorable fields of SearchUIState to app.storage.user.
    runtime_only and cross_page_preference fields are NOT written.
    """
    if not app.storage.user.get('session_persistence_enabled', True):
        return
    app.storage.user['search_snapshot_schema_version'] = _SEARCH_SNAPSHOT_VERSION
    # ... write all restorable_page_snapshot keys from state fields ...

def clear_search_snapshot() -> None:
    """Wipe all search snapshot keys from app.storage.user.
    Replaces the scattered blocks at search.py:806–832 and search.py:2019–2025.
    """
    ...
```

**Important:** `filter_panel.py`'s `persist_value()` already handles per-change filter writes. `clear_search_snapshot()` must clear the same `search_filter_*` keys currently cleared at search.py:806–832. No duplication — `restore_search_snapshot` calls `load_filter_state(state, 'search')` (the existing function) rather than re-implementing reads.

---

### `web/pages/browse_state.py` (MODIFY — add snapshot helper triple)

**Analog:** `web/pages/search_state.py` new snapshot helpers (the pattern just described above). This module is the browse mirror.

**Existing `_persist_reading_desk_state` pattern** (the current inline save — `web/pages/browse.py` lines 1056–1074):
```python
def _persist_reading_desk_state():
    """Save reading desk state to app.storage.user for language-switch persistence."""
    try:
        if state.view_joined and state.reading_desk_entries:
            rd_data = []
            for entry in state.reading_desk_entries:
                rd_data.append({
                    'sys_id': entry.get('sys_id', ''),
                    'shelfmark': entry.get('shelfmark', '')
                })
            app.storage.user['reading_desk_state'] = {
                'entries': rd_data,
                'pgpid': state.joined_pgpid,
                'selected_sources': state.reading_desk_selected_sources or {}
            }
        else:
            app.storage.user.pop('reading_desk_state', None)
    except Exception as e:
        logger.error(f"[ReadingDesk] Error persisting state: {e}")
```

**Existing `browse_position` write pattern** (`web/pages/browse.py` lines 777–785):
```python
try:
    app.storage.user['browse_position'] = {
        'sys_id': state.sys_id,
        'p_num': page.p_num,
        'shelfmark': page.shelfmark,
        'volume_ie': state.volume_ie,
    }
except Exception:
    pass  # Browser storage operation failed; preference not persisted
```

**New helper signatures to add** (D-07, from RESEARCH §3.2):
```python
_BROWSE_SNAPSHOT_VERSION = 1

def restore_browse_snapshot(state: BrowseState) -> dict | None:
    """Hydrate browse position from app.storage.user.

    Returns the browse_position dict (or None) so the bootstrap caller
    knows what p_num to pass to load_page(). Does NOT schedule async work.
    Also restores reading_desk_state into state.reading_desk_entries.

    NOTE: Tab stomping limitation — version stamp prevents cross-version
    corruption but does not prevent Tab B overwriting Tab A's position.
    Full tab isolation requires per-tab keys (deferred as Codex W3).
    """
    stored_version = app.storage.user.get('browse_snapshot_schema_version', 0)
    if stored_version != _BROWSE_SNAPSHOT_VERSION:
        clear_browse_snapshot()
        return None
    ...

def persist_browse_snapshot(state: BrowseState, page) -> None:
    """Serialize browse position and reading desk state.
    page: the BrowsePage object (for shelfmark/p_num extraction).
    Replaces the inline writes at browse.py:777–785 and browse.py:1056–1074.
    """
    ...

def clear_browse_snapshot() -> None:
    """Wipe browse_position and reading_desk_state keys."""
    ...
```

**Notable divergence from search:** `restore_browse_snapshot` returns `dict | None` (position dict) because the bootstrap caller needs `p_num` to schedule `load_page(p_num=...)`. Search equivalent returns `None` (state fully hydrated in-place). Also, `BrowseState` has `volume_ie` (multi-IE manuscript concept) that has no search analog — `persist_browse_snapshot` must include it in the position dict and `restore_browse_snapshot` must validate it (use the existing volume validation logic from browse.py:4521–4527).

---

### `web/pages/search.py` (MODIFY — Cat-1 sweep + replace snapshot writes)

**Cat-1 conversion pattern.** Current (12 sites):
```python
# search.py:885 — typical Cat-1 inside a sync event handler
asyncio.ensure_future(_recompute_filter_count())
```
After:
```python
return _recompute_filter_count()  # NiceGUI 3.8 schedules awaitable returns via handle_event
```
Or when called at end of a void handler with no return needed:
```python
# If the on_change handler has multiple statements, convert to async def:
async def on_text_filter_add():
    ...
    await _recompute_filter_count()
    _update_chip_bar()
    _rebuild_text_chips()
```

**Cat-1 lambda form** (`web/pages/browse.py:1589` — same pattern in search.py call sites):
```python
# Before:
on_click=lambda: asyncio.ensure_future(load_page())
# After:
on_click=lambda: load_page()
```

**Snapshot write replacement pattern.** Current inline block at search.py:2019–2025:
```python
app.storage.user['search_results'] = []
app.storage.user['search_query'] = ''
app.storage.user['search_mode'] = 'exact'
app.storage.user['domain_exclusions'] = []
app.storage.user['search_printed_filter'] = 'all'
app.storage.user['word_search_excluded_ids'] = []
app.storage.user['search_exclusion_sources'] = []
```
After:
```python
from web.pages.search_state import clear_search_snapshot
clear_search_snapshot()
```

Similarly, `persist_search_snapshot(search_state)` replaces all individual `app.storage.user[snapshot_key] = ...` writes for restorable fields.

**Cat-2 keep pattern** (8 sites in search.py — add this comment):
```python
# Cat-2: deferred to next event loop tick to allow [container/select/DOM] to mount.
# Cannot convert to bare return — NiceGUI's awaitable path does not restore slot/client context.
asyncio.ensure_future(_after_delay(0.1, load_pgp_tags))
```

**Cat-3 keep pattern** (1 site):
```python
# Cat-3: long-running owned task handle — intentionally detached.
search_state.update_timer = asyncio.ensure_future(_progress_update_loop())
```

---

### `web/pages/browse.py` (MODIFY — Cat-1 sweep + snapshot calls + bootstrap extract)

**Cat-1 lambda conversion** (10 sites). Current forms:
```python
# browse.py:1589
on_click=lambda: asyncio.ensure_future(load_page())
# browse.py:1629
on_click=lambda: asyncio.ensure_future(navigate_shelfmark(-1))
# browse.py:1822
on_click=lambda: asyncio.ensure_future(navigate_shelfmark(1))
# browse.py:3711
on_click=lambda: asyncio.ensure_future(load_page(direction=-1))
# browse.py:3764
on_click=lambda: asyncio.ensure_future(load_page(direction=1))
```
After:
```python
on_click=lambda: load_page()
on_click=lambda: navigate_shelfmark(-1)
on_click=lambda: navigate_shelfmark(1)
on_click=lambda: load_page(direction=-1)
on_click=lambda: load_page(direction=1)
```

**Cat-1 non-lambda forms** (5 remaining Cat-1 in browse.py — same treatment):
```python
# browse.py:1397 (inside save_correction callback)
asyncio.ensure_future(load_page(direction=0))
# After:
return load_page(direction=0)  # or await if caller is already async
```

**Cat-2 bootstrap sites** (6 sites — all at lines 4471–4530, add comment):
```python
# Cat-2: bootstrap deferred init — update_content() must render spinner before load_page fires.
asyncio.ensure_future(load_page(p_num=initial_page))
```

**Bootstrap extraction call site** (replace lines 4466–4532 with):
```python
bootstrap = resolve_browse_bootstrap(
    initial_fl_id=initial_fl_id_value,
    initial_sys_id=initial_sys_id,
    initial_page=initial_page,
    pending_shelfmark=_pending_shelfmark,
    saved_reading_desk=app.storage.user.get('reading_desk_state'),
    saved_position=app.storage.user.get('browse_position'),
)
# Dispatch based on bootstrap action (Cat-2 ensure_future calls survive here)
if bootstrap['action'] == 'fl_id':
    state.is_loading = True
    update_content()
    asyncio.ensure_future(load_page(fl_id=bootstrap['fl_id']))
elif bootstrap['action'] in ('sys_id', 'restore_position'):
    ...
```

**Snapshot call replacements:**

browse.py:777–785 (inside `load_page`):
```python
# Before:
app.storage.user['browse_position'] = {'sys_id': ..., 'p_num': ..., ...}
# After:
from web.pages.browse_state import persist_browse_snapshot
persist_browse_snapshot(state, page)
```

browse.py:982, 1066–1072 (reading desk):
```python
# Before: _persist_reading_desk_state() (inline local function)
# After: persist_browse_snapshot(state, page) / clear_browse_snapshot()
# The local _persist_reading_desk_state() is eliminated; its body moves into persist_browse_snapshot.
```

---

### `web/components/filter_panel.py` (MODIFY — Cat-1 sweep, multi-coroutine handlers)

**Current Cat-1 pattern** (`filter_panel.py` lines 445–498):
```python
def on_domain_change(e=None):
    val = filter_refs['domain'].value or []
    state.filter_domains = val if isinstance(val, list) else [val] if val else []
    persist_value(f'{pfx}_filter_domains', state.filter_domains)
    asyncio.ensure_future(refresh_author_fn())   # Cat-1: three coroutines
    asyncio.ensure_future(refresh_work_fn())
    asyncio.ensure_future(recompute_fn())
    update_chip_fn()
```

**Pitfall 1 (from RESEARCH §2.3):** A sync `def on_*` handler can only return ONE awaitable for NiceGUI's awaitable path. `on_domain_change` calls THREE coroutines. Resolution: convert to `async def`:
```python
async def on_domain_change(e=None):
    val = filter_refs['domain'].value or []
    state.filter_domains = val if isinstance(val, list) else [val] if val else []
    persist_value(f'{pfx}_filter_domains', state.filter_domains)
    await refresh_author_fn()
    await refresh_work_fn()
    await recompute_fn()
    update_chip_fn()
```

**Single-coroutine handlers** (e.g., `on_work_change`, `on_mode_change`, `on_date_from_change`, `on_date_to_change`, `on_exclude_printed_change`):
```python
# Before:
def on_work_change(e=None):
    ...
    asyncio.ensure_future(recompute_fn())
    update_chip_fn()

# After: two options —
# Option A: return the coroutine (NiceGUI picks it up; update_chip_fn() must fire after)
def on_work_change(e=None):
    ...
    update_chip_fn()
    return recompute_fn()  # last statement — NiceGUI schedules it

# Option B: convert to async def (consistent with on_domain_change)
async def on_work_change(e=None):
    ...
    await recompute_fn()
    update_chip_fn()
```
**Prefer Option B** (async def) for all handlers — consistent with `on_domain_change` pattern, avoids subtle ordering issues.

**`persist_value` stays as-is** (lines 220–223) — it is already a write gateway that satisfies D-05:
```python
def persist_value(key, value):
    """Save to storage if session persistence is enabled."""
    if app.storage.user.get('session_persistence_enabled', True):
        app.storage.user[key] = value
```

---

### `web/pages/browse_enrichment.py` (MODIFY — audit 1 site)

**Analog for Cat-2 keep pattern:** `web/pages/search_results.py` lines 111 and 738.

**search_results.py Cat-2 pattern** (lines 109–113):
```python
# Cat-2: client context re-entry required — must use with refs.page_client to render into correct tab.
asyncio.ensure_future(_run_lazy())
```
Full context (lines 107–115):
```python
def toggle_expansion(idx, result, refs):
    ...
    async def _run_lazy():
        with refs.page_client:  # ← re-enter client context explicitly
            ...
    asyncio.ensure_future(_run_lazy())
```

**browse_enrichment.py** — verify the one flagged `ensure_future` call. If it uses an explicit `with client:` or `with container:` block (the Cat-2 fingerprint), add the comment and leave as-is. If not (bare scheduling without context re-entry), it is Cat-1 and should be converted.

---

### `tests/e2e/test_browse_flow.py` (MODIFY — add URL-bar E2E assertion)

**Analog:** existing `TestBrowseNavigation` class in same file (lines 67–121).

**Existing test structure to mirror** (lines 67–89):
```python
@pytest.mark.skipif(not _has_tantivy_index(), reason="Tantivy index not available")
class TestBrowseNavigation:
    """Test browse page with actual manuscript data."""

    def test_browse_with_sys_id(self, screen):
        """Browse page loads a specific manuscript by sys_id."""
        screen.open('/browse?sys_id=003750')
        screen.wait(8.0)

        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        page_text = body.text
        has_content = (
            len(page_text) > 100 or
            'T-S' in page_text or
            'CUL' in page_text or
            'Cambridge' in page_text
        )
        assert has_content, \
            f"Browse page should show manuscript content, got {len(page_text)} chars"
```

**New test to add** (D-20 — add inside `TestBrowseNavigation`):
```python
    def test_shelfmark_navigation_updates_url(self, screen):
        """Shelfmark navigation (Prev/Next) updates the browser URL bar.

        This is the regression test for the Cat-1 ensure_future fix:
        before the fix, on_click=lambda: asyncio.ensure_future(navigate_shelfmark(...))
        returned a Task, bypassing NiceGUI's context-preserving awaitable path,
        and history.replaceState was called outside the client context — silently
        dropped by NiceGUI, leaving the URL bar stale.
        """
        screen.open('/browse?sys_id=003750')
        screen.wait(8.0)

        initial_url = screen.selenium.current_url

        # Click the Next Shelfmark button
        # Adjust selector to match actual rendered button (skip_next icon for RTL, skip_previous for LTR)
        next_btns = screen.selenium.find_elements(
            By.CSS_SELECTOR, 'button[aria-label*="next"], button[aria-label*="Next"]'
        )
        if next_btns:
            next_btns[0].click()
            screen.wait(5.0)
            updated_url = screen.selenium.current_url
            assert updated_url != initial_url, (
                "URL bar should update after shelfmark navigation "
                "(Cat-1 ensure_future fix: NiceGUI's awaitable path must be used)"
            )
```

**Note:** The test must be inside `@pytest.mark.skipif(not _has_tantivy_index(), ...)` class (real navigation requires index). If the Next button is not found (no next shelfmark), the test should skip gracefully rather than fail.

---

## Shared Patterns

### Cat-1 Conversion Rule
**Source:** CONTEXT.md D-10, verified from `nicegui/events.py` (RESEARCH §4)
**Apply to:** All 22 Cat-1 sites across search.py (12), browse.py (10), filter_panel.py (10)

```python
# NiceGUI 3.8.0 handle_event excerpt (events.py):
result = handler(arguments) if expects_arguments else handler()
if isinstance(result, Awaitable) and not isinstance(result, AwaitableResponse):
    async def wait_for_result():
        with parent_slot:          # preserves slot context
            try:
                await result
            except Exception as e:
                core.app.handle_exception(e)
    background_tasks.create(wait_for_result(), name=str(handler))
# asyncio.ensure_future(coro()) returns a Task, NOT an Awaitable that passes isinstance check.
# Bare `return coro()` returns the coroutine object itself — passes the check.
```

Rule: when a NiceGUI event handler (`on_click`, `on_change`) directly calls a coroutine, return the coroutine or use `async def`. Do NOT wrap in `asyncio.ensure_future`.

### Cat-2 Comment Template
**Apply to:** 8 Cat-2 sites in search.py, 6 Cat-2 sites in browse.py, 2 Cat-2 sites in search_results.py
```python
# Cat-2: deferred to next event loop tick — [specific reason: container must mount /
#         client context re-entry required / _after_delay pattern for JS DOM readiness].
# Cannot convert to bare return: NiceGUI's awaitable path does not re-enter slot/client context.
asyncio.ensure_future(...)
```

### Version Stamp Pattern (D-04 tab collision hardening)
**Apply to:** both `restore_search_snapshot` and `restore_browse_snapshot`
```python
_SEARCH_SNAPSHOT_VERSION = 1  # increment on schema-breaking changes

def restore_search_snapshot(state: SearchUIState) -> None:
    stored_version = app.storage.user.get('search_snapshot_schema_version', 0)
    if stored_version != _SEARCH_SNAPSHOT_VERSION:
        clear_search_snapshot()
        return  # start fresh; page mounts with default state
    ...
```

### Persistence Gate Pattern
**Source:** `web/components/filter_panel.py` lines 220–223 (existing `persist_value`)
**Apply to:** `persist_search_snapshot`, `persist_browse_snapshot`
```python
def persist_search_snapshot(state: SearchUIState) -> None:
    if not app.storage.user.get('session_persistence_enabled', True):
        return
    ...
```

### Exception Swallow Pattern
**Source:** `web/pages/browse.py` lines 777–785 (existing browse_position write)
**Apply to:** all snapshot helper bodies
```python
try:
    app.storage.user['browse_position'] = {...}
except Exception:
    pass  # Browser storage operation failed; preference not persisted
```
Use `pass` (not logger.error) for writes; use `logger.error` for reads (reading desk restore already does this at browse.py:1094).

---

## No Analog Found

None — every file has a clear analog or self-analog.

---

## Metadata

**Analog search scope:** `web/`, `tests/`, `shared/`, `web/pages/`, `web/components/`
**Files read:** `web/search_bootstrap.py`, `tests/test_search_bootstrap.py`, `web/pages/search_state.py`, `web/pages/browse_state.py`, `web/pages/browse.py` (bootstrap block + position write + desk write), `web/pages/search.py` (restore block + clear blocks + Cat-1 sites), `web/components/filter_panel.py` (persist_value + load_filter_state + create_filter_handlers), `tests/e2e/test_browse_flow.py`, `tests/e2e/conftest.py`
**Pattern extraction date:** 2026-04-17

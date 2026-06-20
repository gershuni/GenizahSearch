# Phase 120: Actions & Persistence - Pattern Map

**Mapped:** 2026-06-20
**Files analyzed:** 10 new/modified files
**Analogs found:** 10 / 10

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/joins_lab_storage.py` | utility | CRUD | self (extend) + `web/safe_storage.py` | exact extension |
| `web/pages/joins_lab.py` | page/controller | event-driven | self (extend) + `web/pages/search.py` (Stop pattern) | exact extension |
| `web/components/known_joins_group.py` | component | request-response | self (extend) | exact extension |
| `web/components/compare_modal.py` | component | request-response | `web/components/anchor_viewer.py` + `desktop/join_workbench.py:5061` | role-match |
| `web/components/candidate_grid.py` | component | request-response | self (extend) | exact extension |
| `web/pages/puzzle.py` | page/controller | event-driven | self (extend, `create_puzzle_page`) | exact extension |
| `web/pages/lists.py` | page/controller | CRUD | self (extend, Browse/Add-to-Puzzle row) | exact extension |
| `tests/test_joins_lab_storage.py` | test | CRUD | `tests/test_joins_lab_page.py` | role-match |
| `tests/test_joins_lab.py` | test | event-driven | self (extend) | exact extension |
| `tests/render_smoke/test_joins_lab_render_smoke.py` | test | request-response | self (extend, SEED-008 guard tests) | exact extension |

---

## Pattern Assignments

---

### `web/joins_lab_storage.py` (utility, CRUD — extend existing)

**Analog:** self (Phase 117 stub, explicitly forward-compatible) + `web/safe_storage.py`

**Existing structure** (lines 1–112 — read in full):
```python
_JOINS_LAB_KEY = 'joins_lab'
_SCHEMA_VERSION = 1   # KEEP AT 1 for Phase 120 (additive keys; bump only on remove/retype)

def read_joins_lab_state() -> Optional[dict]:
    data = safe_user_get(_JOINS_LAB_KEY, default=None)
    if not isinstance(data, dict):
        return None
    if data.get('schema_version') != _SCHEMA_VERSION:
        return None  # stale schema — discard
    return data

def write_anchor(anchor_sys_id, anchor_fl_id=None, anchor_volume_ie=None) -> bool:
    payload = {
        'schema_version': _SCHEMA_VERSION,
        'anchor_sys_id': ...,
        'anchor_fl_id': ...,
        'anchor_volume_ie': ...,
    }
    return safe_user_set(_JOINS_LAB_KEY, payload)

def clear_joins_lab_state():
    return safe_user_pop(_JOINS_LAB_KEY, None)
```

**Phase 120 extension — add `write_full_state()` and `read_full_state()`:**

The schema version **STAYS at 1** (CONTEXT D-16: "extend v1; bump only on remove/retype"). Phase-120
keys are additive; existing v1 blobs are read with `.get(key, default)` so they restore cleanly. Do
NOT bump to 2 — `read_joins_lab_state()`'s exact-match check would DISCARD existing users' anchors.

New payload shape (all Phase-120 keys under the same `_JOINS_LAB_KEY`):
```python
{
    'schema_version': 1,              # UNCHANGED — additive keys only (do NOT bump; see note below)

    # Phase 117 fields (unchanged):
    'anchor_sys_id': str | None,
    'anchor_fl_id': str | None,
    'anchor_volume_ie': str | None,

    # Phase 120 additions:
    'builder_rows': list,             # [{'term': str, 'gap_to_next': int, 'modifiers': dict}]
    'builder_mode': str,              # 'exact' | 'variants' | 'fuzzy'
    'text_position': str,             # 'anywhere' | 'start' | 'end' | 'line_start' | 'line_end'
    'flex_spacing': bool,
    'bidirectional': bool,
    'other_side_enabled': bool,
    'other_side_rows': list,          # same shape as builder_rows
    'other_side_combine': str,        # 'narrow' | 'widen'
    'triage': dict,                   # {sys_id: 'yes'|'maybe'|'no'} max 500 entries
    'active_filter': dict,            # compact filter discriminants only
    'view_mode': str,                 # 'grid' | 'table'
}
```

**Size discipline (must enforce at write time):**
- `builder_rows`: each `term` capped at 200 chars; max 20 rows
- `triage`: max 500 entries; LRU-evict oldest when overflowing; Y/?/N entries preserved
- `active_filter`: JSON-serialized; must stay < 4 KB
- NEVER write `full_text`, image bytes, or candidate lists (778 MB incident guard)

**`clear_joins_lab_state()` extension (D-16):**
```python
def clear_joins_lab_state() -> None:
    safe_user_pop(_JOINS_LAB_KEY, None)
    safe_user_pop('puzzle_staging', None)  # also wipe puzzle staging key
```

**`safe_user_*` import — all persistence goes through these, NEVER `app.storage.user` directly:**
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
```

---

### `web/pages/joins_lab.py` (page/controller, event-driven — extend existing)

This is the largest change surface. It hosts: Stop button (D-11), PST restore (D-13/D-14/D-15),
SEED-007 Make-an-anchor (D-07), ACT-01 Add-as-Join login gate (D-01/D-03), D-12 VS hide,
D-17/D-18 list picker + sign-in fix, SEED-008 hardening.

**Primary analog:** `web/pages/search.py` (Stop + progress_cb pattern)

#### Pattern A: Stop-with-partials (D-11)

Source: `web/pages/search.py:2354-2358` (cancel), `:4055-4058` (progress_cb), `:557-563` (UI)

The existing `_make_progress_cb` in `joins_lab.py:253-287` already raises `InterruptedError`
when the generation is superseded. D-11 adds a SECOND flag `_stop_requested` that raises
`InterruptedError` on an EXPLICIT user stop WITHOUT bumping the generation — so
`_should_apply_results` still returns True and partials are applied.

**Stop flag pattern** (mirror of `search_state.is_cancelled` at `search.py:2354`):
```python
# New module-level mutable flag alongside _search_generation and _is_running:
_stop_requested: dict = {'value': False}  # dict for closure capture (not a bare bool)

# Stop button handler (replaces Run Search slot while in-flight):
def _on_stop_click() -> None:
    _stop_requested['value'] = True
    # Do NOT bump _search_generation here — _should_apply_results must still return True.
    search_state.status = tr('Stopping…')
```

**Progress_cb extension** (add stop check BEFORE generation check):
```python
# In _make_progress_cb (joins_lab.py:276), add BEFORE the generation check:
def progress_cb(arg1, arg2=None):
    if _stop_requested['value']:               # <-- NEW: user clicked Stop
        raise InterruptedError('joins-lab search stopped by user')
    if my_gen != gen_ref['value']:             # existing: superseded run
        raise InterruptedError('joins-lab search superseded')
    if isinstance(arg1, str):
        return
```

**`_should_apply_results` remains unchanged** (joins_lab.py:234-250):
```python
def _should_apply_results(my_gen: int, gen_ref: dict) -> bool:
    return my_gen == gen_ref['value']
```

**Stop button UI** (mirror of `search.py:557-563`):
```python
# In the Run Search button row — same slot, swapped on in-flight state:
search_btn = ui.button(tr('Run Search'), icon='search', on_click=execute_joins_search)
stop_btn = ui.button(
    tr('Stop'),
    icon='stop_circle',
    on_click=_on_stop_click,
).props('outline color=negative').style('display: none;')
stop_btn.tooltip(tr('Stop search and show partial results'))
# On search start: search_btn.style('display: none;') + stop_btn.style('display: inline;')
# On search end:  stop_btn.style('display: none;') + search_btn.style('display: inline;')
```

**Reset `_stop_requested` at the START of every new search (before bumping generation):**
```python
_stop_requested['value'] = False
```

#### Pattern B: Sign-in fix (D-18)

Source: `web/auth_state.py:358` (`create_login_dialog`), `web/pages/joins_lab.py:1571-1574`

Current bug (joins_lab.py:1573) — navigates AWAY from page, losing state:
```python
on_click=lambda: ui.navigate.to('/settings')   # BUG — do NOT use
```

Fix — opens an overlay dialog, page stays mounted:
```python
from web.auth_state import GlobalAuthState, create_login_dialog

on_click=lambda: create_login_dialog().open()  # CORRECT — in-page dialog
```

`create_login_dialog()` (auth_state.py:358) handles email/password + Google OAuth + Remember Me.
It returns a `ui.dialog` object; calling `.open()` shows it as an overlay.

#### Pattern C: VS toggle hide (D-12)

Source: `web/pages/joins_lab.py:372-407` (`_check_vs_service_available` + `_fetch_vs_candidates`)

Existing probe:
```python
def _check_vs_service_available() -> bool:
    """Sync, blocking I/O — dispatch via run.io_bound only."""
    try:
        return get_vs_service(thread_safe=True).is_available()
    except Exception:
        return False
```

D-12 extends this to also probe the anchor's actual data count:
```python
# In _do_vs_fetch_and_update (after the existing availability check):
# If get_suggestions(anchor_sid, 1) returns empty list → no VS data for this anchor
# → hide (not disable) the VS toggle widget:
vs_toggle_widget.set_visibility(False)
```

Anchor-generation guard is required (same as `_do_vs_fetch_and_update` already does):
```python
if anchor_gen != _anchor_generation['value']:
    return  # stale — anchor changed before probe completed
```

#### Pattern D: SEED-008 fire-and-forget guard (D-20)

Source: `web/pages/joins_lab.py:2119-2123` (existing `_runner` guard),
`web/components/joins_panel.py:507-513` (existing `_safe_load_count` guard)

Existing precedent at joins_lab.py:2119-2123:
```python
async def _runner() -> None:
    try:
        with _page_client:
            await _bootstrap_anchor()
    except RuntimeError as exc:
        if 'slot' not in str(exc) and 'deleted' not in str(exc):
            logger.error('joins-lab bootstrap error: %s', exc)
```

Existing precedent at joins_panel.py:507-513:
```python
async def _runner():
    try:
        with _btn_client:
            await load_count()
    except RuntimeError:
        pass  # Parent element deleted / client torn down
```

**Apply to ALL fire-and-forget tasks that mutate UI after `await`:**

Dispatch sites that must carry the guard (verified from RESEARCH.md §V8):
- `joins_lab.py:1309` — `asyncio.ensure_future(_load_known_joins(...))`
- `joins_lab.py:1257` — `asyncio.ensure_future(_do_vs_fetch_and_update(...))` (re-anchor)
- `joins_lab.py:1515` — `asyncio.ensure_future(_do_vs_fetch_and_update(...))` (toggle ON)
- `joins_lab.py:2046/2050` — `asyncio.ensure_future(_do_enrich_and_update(...))`
- `joins_lab.py:1742` — `asyncio.ensure_future(_do_enrich_and_update(...))`
- ALL new Phase-120 fire-and-forget tasks (D-10 prefetch, D-12 VS probe callback,
  ACT-01 known-joins re-render)

Pattern for EVERY new dispatch site:
```python
_page_client = ui.context.client   # captured at page build time

async def _my_fire_and_forget_task():
    try:
        with _page_client:
            data = await run.io_bound(some_blocking_fn, ...)
            # --- any UI mutation here ---
            container.clear()
            with container:
                ui.label('result')
    except RuntimeError:
        return  # client/tab deleted mid-fetch — standard SEED-008 guard

asyncio.ensure_future(_my_fire_and_forget_task())
```

#### Pattern E: Make-an-anchor (D-07)

Source: `web/pages/joins_lab.py:1167-1169` (`_on_reanchor` inside `_load_known_joins`)

Existing re-anchor mechanism:
```python
def _on_reanchor(member_sys_id: str, member_shelfmark: str) -> None:
    asyncio.ensure_future(load_anchor(member_sys_id, show_restored_toast=False))
```

D-07 wires the same `load_anchor()` call from candidate cards/rows. Triage resets on re-anchor
(per 119 D-11 contract — `_triage_state.clear()` before the new search runs).

**Button on each candidate card/row:**
```python
ui.button(
    tr('Set as Anchor'), icon='push_pin',
    on_click=lambda sid=cand.sys_id: asyncio.ensure_future(load_anchor(sid))
).props('flat dense').tooltip(tr('Pivot the workbench: make this fragment the new anchor'))
```

#### Pattern F: ACT-01 Add-as-Join login gate + D-03 remove join

Source: `web/supabase_client.py:1625-1665`, `web/auth_state.py:GlobalAuthState`

Login gate pattern (used throughout web/ — e.g. `joins_panel.py`, `corrections.py`):
```python
from web.auth_state import GlobalAuthState, create_login_dialog

if GlobalAuthState.is_logged_in():
    user = GlobalAuthState.get_user()
    user_id = user['id']
    # ... proceed with write
else:
    create_login_dialog().open()
```

ACT-01 insert — **[USER OVERRIDE 2026-06-20 — supersedes the old `status='confirmed'` finding]**
keep the inserted status as the `'proposed'` default (a user-added join is an *unmoderated* claim).
Make the join show in the Lab by setting the Lab known-joins fetch to `confirmed_only=False` (see the
known-joins fetch pattern below), NOT by marking it confirmed:
```python
# In run.io_bound closure (off the event loop):
result = create_fragment_join(
    user_id=user_id,
    fragment_a_sys_id=anchor_sys_id,
    fragment_a_shelfmark=anchor_shelfmark,
    fragment_b_sys_id=candidate_sys_id,
    fragment_b_shelfmark=candidate_shelfmark,
    # join_type, confidence, notes, AND status all use defaults → status='proposed'
)
# Do NOT patch status to 'confirmed'. Instead the Lab known-joins group must fetch with
# confirmed_only=False so proposed joins show (parity with /browse). See CONTEXT D-02.
```

After successful insert — force-refresh the known-joins group (bypass 30-second cache):
```python
asyncio.ensure_future(_load_known_joins(anchor_sys_id, anchor_shelfmark, force_refresh=True))
```

D-03 delete (own-joins-only, RLS enforces self-scope):
```python
result = await run.io_bound(delete_fragment_join, join_id)
# RLS DELETE policy: USING (auth.uid() = user_id) — self-scoped automatically
```

Own-join detection in `render_known_joins_group`: pass `current_user_id` as an optional
parameter; render `link_off` button only when `join.get('user_id') == current_user_id`.

#### Pattern G: PST restore on page load (D-14)

Source: `web/pages/joins_lab.py:2092-2129` (`_bootstrap_anchor` + `_schedule_bootstrap`)

Existing bootstrap reads `read_anchor()` and calls `load_anchor()`:
```python
async def _bootstrap_anchor() -> None:
    stored = read_anchor()  # returns None on cold start
    if stored is None:
        return  # cold start — show empty state
    await load_anchor(stored['anchor_sys_id'], ...)
```

D-14 extend: when stored state has builder rows too, restore builder and auto-re-run:
```python
async def _bootstrap_anchor() -> None:
    stored = read_joins_lab_state()   # Phase 120: full state, not just anchor
    if stored is None or not stored.get('anchor_sys_id'):
        return

    # Show "restoring…" indicator before loading
    restoring_bar.set_visibility(True)

    await load_anchor(stored['anchor_sys_id'], ...)

    # Restore builder UI from stored rows
    _restore_builder_from_state(stored)

    # Auto-re-run
    await execute_joins_search()

    # After results: re-attach triage/filter/view by sys_id
    _restore_triage_filter_view(stored)

    # Hide indicator
    restoring_bar.set_visibility(False)
```

---

### `web/components/known_joins_group.py` (component, request-response — extend existing)

**Analog:** self (57-204 lines — read in full)

**Current signature** (lines 54-59):
```python
def render_known_joins_group(
    data: dict,
    current_shelfmark: str,
    current_sys_id: str,
    on_reanchor: Callable[[str, str], None],
    on_open_browse: Callable[[str], None],
) -> None:
```

**Phase 120 extension — add `on_remove_join` + `current_user_id`:**
```python
def render_known_joins_group(
    data: dict,
    current_shelfmark: str,
    current_sys_id: str,
    on_reanchor: Callable[[str, str], None],
    on_open_browse: Callable[[str], None],
    on_remove_join: Callable[[int], None] | None = None,   # D-03: new
    current_user_id: str | None = None,                    # D-03: new
) -> None:
```

**`_render_member_row` extension** — add third trailing icon for own joins:

Current trailing icon block (lines 191-203):
```python
if member_sys_id:
    ui.button(icon='push_pin', on_click=_make_reanchor()).props('flat dense')...
ui.button(icon='open_in_new', on_click=_make_browse()).props('flat dense')...
```

Phase 120 addition — third icon only when `is_mine=True`:
```python
join_user_id = join.get('user_id', '')  # must be passed into _render_member_row
is_mine = bool(current_user_id and join_user_id == current_user_id)

if is_mine and on_remove_join is not None:
    ui.button(
        icon='link_off',
        on_click=lambda jid=join.get('id'): on_remove_join(jid),
    ).props('flat dense color=negative').tooltip(
        tr('Remove this join (only your own joins can be removed)')
    )
```

The `join.get('user_id')` field comes from the Supabase `fragment_joins` row returned by
`get_fragment_joins`. The fetch already returns all columns; `user_id` is present in the schema.

---

### `web/components/compare_modal.py` (component, request-response — extend existing)

**Analog:** `desktop/join_workbench.py:5061-5101` (`_pump_images` — parity model for D-10),
`web/components/candidate_grid.py:442-450` (`build_browse_url` — for D-08),
`web/pages/joins_lab.py:372-407` (`_check_vs_service_available` — off-loop pattern)

#### D-08: Browse-in-Compare buttons

Source: `web/components/candidate_grid.py:442-450`

```python
from web.components.candidate_grid import build_browse_url

# In Compare modal header — one button per pane:
ui.button(
    icon='open_in_new',
    on_click=lambda: ui.navigate.to(build_browse_url(anchor_cand), new_tab=True),
).props('flat dense aria-label="Open anchor in Browse"').style('color: white;').tooltip(
    tr('Open anchor in Browse (new tab)')
)
```

#### D-09: Compare info buttons

Source: Existing Browse/ResultDialog info dialog open pattern (reuse — do NOT re-implement).

```python
# Below each pane's image viewer, before transcription:
with ui.row().classes('gap-2 py-2'):
    fjms_btn = ui.button(
        tr('FJMS Catalog'), icon='info_outline',
    ).props('flat dense')
    if not has_fjms_data:
        fjms_btn.props('disable')
        fjms_btn.tooltip(tr('No FJMS catalog data for this fragment'))
    else:
        fjms_btn.tooltip(tr('View FJMS catalog data for this fragment'))
        fjms_btn.on('click', lambda sid=sys_id: open_fjms_dialog(sid))

    pgp_btn = ui.button(
        tr('PGP / Bibliography'), icon='menu_book',
    ).props('flat dense')
    # same pattern as fjms_btn for disabled/enabled state
```

#### D-10: Compare image prefetch (bounded 5-slot pool)

Parity model: `desktop/join_workbench.py:5061-5101` (`_pump_images`)

Desktop pattern (lines 5061-5101):
```python
_img_threads = []
_img_queue = []
_MAX_CONCURRENT_IMG = 5

def _pump_images(self):
    self._img_threads = [t for t in self._img_threads if t.isRunning()]
    while self._img_queue and len(self._img_threads) < _MAX_CONCURRENT_IMG:
        label, url, target, on_pix = self._img_queue.pop(0)
        loader = ImageLoaderThread(url)
        loader.image_loaded.connect(...)
        loader.finished.connect(self._pump_images)  # drain more on completion
        loader.start()
        self._img_threads.append(loader)
```

**Web equivalent (asyncio-based, not Qt threads):**
```python
_prefetch_cache: dict = {}        # {sys_id: resolved_url} — populated by prefetch
_prefetch_running: set = set()    # sys_ids currently being prefetched
_PREFETCH_SLOTS = 5               # mirror desktop _MAX_CONCURRENT_IMG

async def _prefetch_adjacent_images(candidate_list: list, current_idx: int) -> None:
    """Preload images for adjacent candidates (D-10). Silent — no UI affordance."""
    # Determine next/prev candidates to prefetch
    targets = []
    for offset in (-2, -1, 1, 2):
        idx = current_idx + offset
        if 0 <= idx < len(candidate_list):
            targets.append(candidate_list[idx].sys_id)

    for sys_id in targets:
        if sys_id in _prefetch_cache or sys_id in _prefetch_running:
            continue
        if len(_prefetch_running) >= _PREFETCH_SLOTS:
            break
        _prefetch_running.add(sys_id)
        asyncio.ensure_future(_fetch_one_image(sys_id))

async def _fetch_one_image(sys_id: str) -> None:
    """Fetch and cache image URL for sys_id (fire-and-forget)."""
    try:
        def _resolve():
            # Use same proxy URL construction as _add_fragment_by_sys_id (puzzle.py:2159)
            return executor.get_browse_page(sys_id)   # returns dict with image metadata
        result = await run.io_bound(_resolve)
        if result:
            _prefetch_cache[sys_id] = result
    except RuntimeError:
        return  # SEED-008 guard: client deleted
    except Exception:
        pass
    finally:
        _prefetch_running.discard(sys_id)
```

**Generation token per anchor** — discard prefetch results from prior anchor:
```python
_prefetch_anchor_gen: dict = {'value': 0}
# On re-anchor: _prefetch_anchor_gen['value'] += 1; _prefetch_cache.clear(); _prefetch_running.clear()
```

---

### `web/components/candidate_grid.py` (component, request-response — extend existing)

**Analog:** self (existing card action row pattern)

**Existing card action row** (after lines 442-450):
```python
with ui.row().classes('gap-1'):
    ui.button('View in Browse', icon='open_in_new', ...).props('flat dense')
    ui.button('Compare', icon='compare', ...).props('flat dense')
```

**Phase 120 extension — two new card-level buttons (D-07/ACT-01):**
```python
# Add after existing Compare button:
ui.button(
    tr('Set as Anchor'), icon='push_pin',
    on_click=lambda: on_set_as_anchor(cand.sys_id),
).props('flat dense').tooltip(tr('Pivot the workbench: make this fragment the new anchor'))

# Add-as-Join — login-aware rendering:
if GlobalAuthState.is_logged_in():
    ui.button(
        tr('Add as Join'), icon='add_link',
        on_click=lambda: on_add_as_join(cand),
    ).props('flat dense color=primary').tooltip(tr('Add as scholarly join'))
else:
    ui.button(
        icon='lock',
        on_click=lambda: create_login_dialog().open(),
    ).props('flat dense').tooltip(tr('Sign in to add this join to the community record'))
```

**Callbacks** pass through from the page; the component stays pure-render (no auth calls inside it).

**Bulk action bar extension** (D-04/D-05/D-06) — appended to existing Phase-119 bulk triage bar:
```python
# Extend existing bar (appears when ≥1 candidate checked):
with ui.row().classes('items-center gap-2 ml-auto'):
    # Count badge on Add-to-Puzzle
    with ui.row():
        add_puzzle_btn = ui.button(
            tr('Add to Puzzle'), icon='extension',
            on_click=_on_add_to_puzzle,
        ).props('flat').tooltip(tr('Add anchor + selected candidates to the Fragment Puzzle'))
        ui.badge(str(1 + len(selected))).props('color=primary floating')

    ui.button(
        tr('Add to List'), icon='playlist_add',
        on_click=_on_add_to_list,
    ).props('flat icon-right=lock').tooltip(...)

    with ui.button(tr('Export'), icon='download').props('flat icon-right=arrow_drop_down'):
        with ui.menu():
            ui.menu_item(tr('CSV'), icon='table_view', on_click=lambda: _on_export('csv'))
            ui.menu_item(tr('Excel (XLSX)'), icon='grid_on', on_click=lambda: _on_export('xlsx'))
```

---

### `web/pages/puzzle.py` (page/controller, event-driven — extend `create_puzzle_page`)

**Analog:** self (existing `initial_add` single-fragment handoff at lines 2202-2258)

**Existing single-fragment pattern** (lines 2202-2220):
```python
def create_puzzle_page(initial_add: str = None, initial_doc: str = None):
    # ...
    # At line 2218: initial_add = 'sys_id,fl_id' or 'sys_id'
```

**Phase 120 extension — bulk staging key (ACT-02/D-04):**

Add immediately after the existing `initial_add` check at the top of `create_puzzle_page`:
```python
from web.safe_storage import safe_user_get, safe_user_pop

# Read and clear bulk staging payload (one-shot — must clear immediately to avoid stale data)
bulk = safe_user_pop('puzzle_staging', None)   # safe_user_pop reads + deletes atomically
if bulk and isinstance(bulk, dict) and bulk.get('schema_version') == 1:
    fragments = bulk.get('fragments', [])[:21]  # anchor + max 20 candidates (cap)
    for sys_id in fragments:
        if sys_id:
            # Resolve shelfmark for this sys_id (best-effort)
            shelfmark = ''
            try:
                shelfmark, _ = state.meta_mgr.get_meta_for_id(sys_id)
            except Exception:
                pass
            await _add_fragment_by_sys_id(sys_id, shelfmark, puzzle_meta,
                                          pending_fragment_meta, threshold_slider)
```

**Staging payload written by Joins Lab before `ui.navigate.to('/puzzle')`:**
```python
safe_user_set('puzzle_staging', {
    'schema_version': 1,
    'fragments': [anchor_sys_id] + [c.sys_id for c in selected_candidates[:20]],
    'source': 'joins_lab',
    'created_at': datetime.utcnow().isoformat(),
})
ui.navigate.to('/puzzle')
```

**Multitenant safety:** `safe_user_pop` is per-session (Phase 87 chokepoint). The pop is atomic
(read + delete in one call) — no stale data on next puzzle visit.

**Allowlist:** `puzzle_staging` must be added to `.planning/phase87_storage_allowlist.yaml`
(currently `[]`; CI guard `tests/test_no_raw_storage_access.py` will fail otherwise).

**`_add_fragment_by_sys_id` signature** (lines 2110-2199 — already correct):
```python
async def _add_fragment_by_sys_id(sys_id, shelfmark, puzzle_meta, pending_fragment_meta, threshold_slider):
```

---

### `web/pages/lists.py` (page/controller, CRUD — extend list-item action row)

**Analog:** self (existing Browse/Add-to-Puzzle icon buttons at lines 693-703)

**Existing action row** (lines 690-703):
```python
with ui.column().classes('gap-1'):
    # Browse button
    ui.button(
        icon='menu_book',
        on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
    ).props('flat round dense').tooltip(tr('Browse'))

    if WEB_PUZZLE_ENABLED:
        # Add to Puzzle button
        ui.button(
            icon='extension',
            on_click=lambda sid=sys_id: ui.navigate.to(f'/puzzle?add={sid}')
        ).props('flat round dense').tooltip(tr('Add to Puzzle'))
```

**Phase 120 — insert "Open in Joins Lab" button BETWEEN Browse and Add-to-Puzzle:**
```python
with ui.column().classes('gap-1'):
    # Browse button (unchanged)
    ui.button(
        icon='menu_book',
        on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
    ).props('flat round dense').tooltip(tr('Browse'))

    # NEW: Open in Joins Lab button (D-19)
    ui.button(
        icon='science',
        on_click=lambda sid=sys_id: ui.navigate.to(f'/joins-lab?sys_id={sid}', new_tab=True),
    ).props('flat round dense aria-label="Open in Joins Lab"').tooltip(tr('Open in Joins Lab'))

    if WEB_PUZZLE_ENABLED:
        # Add to Puzzle button (unchanged)
        ui.button(...)
```

Note: `icon='science'` matches the Phase-118 Joins Lab entry points in `search.py` (consistent
visual language; see UI-SPEC §11). The button is always enabled — Joins Lab requires no login
for anonymous browsing.

---

### ACT-03 Export (flat CSV + XLSX) — pattern for the export flow in `joins_lab.py`

**Analog:** `shared/export_dossier.py` (column/header helpers), `web/joins_executor.py:76-106`
(`get_browse_page`), `web/pages/search.py` (off-loop progress pattern)

**Text fetch pattern** (D-06 — off-loop batched, with progress + cancel):
```python
import io
import csv
import openpyxl

async def _export_candidates(candidates: list, fmt: str) -> None:
    """Flat CSV or XLSX export of the candidate set (ACT-03/D-06). Anonymous-OK."""
    cap = 500
    export_set = candidates[:cap]
    if len(candidates) > cap:
        ui.notify(tr('Exporting the first 500 candidates.'), type='info')

    # Show progress indicator
    export_progress_container.set_visibility(True)
    total = len(export_set)
    _export_cancel = {'value': False}

    rows = []
    TEXT_CAP = 4000   # mirror SEARCH_API_BROWSE_TEXT_CAP convention

    def fetch_text_batch(batch):
        """Sync closure — dispatched via run.io_bound."""
        results = []
        for cand in batch:
            page = cand.page  # matched page for text hits; None = first page (VS-only)
            browse = executor.get_browse_page(cand.sys_id, p_num=page)
            text = ''
            if browse:
                text = (browse.get('text') or '')[:TEXT_CAP]
            results.append(text)
        return results

    BATCH_SIZE = 10
    for i in range(0, total, BATCH_SIZE):
        if _export_cancel['value']:
            export_progress_container.set_visibility(False)
            return
        batch = export_set[i:i + BATCH_SIZE]
        texts = await run.io_bound(fetch_text_batch, batch)
        rows.extend(zip(batch, texts))
        # Update progress
        progress_bar.value = min(i + BATCH_SIZE, total) / total

    export_progress_container.set_visibility(False)

    # Build output (flat single sheet)
    headers = [
        tr('Shelfmark'), tr('Library'), tr('Title'), tr('Triage'), tr('Score'),
        tr('Material'), tr('Dimensions'), tr('Page'), tr('Transcription (page)'), tr('Image URL'),
    ]
    # ... assemble rows + trigger ui.download()
```

**Image URL column** — reuse `shared/export_dossier.build_image_url_for_row` (Phase 94 helper):
```python
from shared.export_dossier import build_image_url_for_row
image_url = build_image_url_for_row(sys_id=cand.sys_id, page=cand.page or 0, library=cand.library)
```

**`ui.download` pattern** (existing in `web/pages/search.py`, `web/export_service.py`):
```python
content = io.StringIO()
writer = csv.writer(content)
writer.writerow(headers)
for cand, text in rows:
    writer.writerow([cand.shelfmark, cand.library, cand.title, triage_for(cand), ...])
ui.download(content=content.getvalue().encode('utf-8-sig'), filename='joins-export.csv',
            media_type='text/csv')
```

---

### PST — schema version note [CORRECTED 2026-06-20 — stay at v1, additive]

**Keep `_SCHEMA_VERSION = 1`. Do NOT bump to 2.** CONTEXT D-16 mandates "extend v1; bump only on
remove/retype", and `web/joins_lab_storage.py`'s own docstring (lines 19–22) says Phase 120 adds keys
**under the same `schema_version: 1`**. Critically, the EXISTING `read_joins_lab_state()` does an
**exact-match** check (`if data.get('schema_version') != _SCHEMA_VERSION: return None`) — so bumping
to 2 would **discard every existing user's persisted Phase-117 anchor blob** on next load (a
regression). Phase 120 only ADDS keys (builder rows, triage, filters, view mode), which are
non-breaking: old v1 blobs simply lack them and the reader defaults via `.get(key, default)`.

```python
_SCHEMA_VERSION = 1  # UNCHANGED — additions are non-breaking; bump only on key removal/retype

def read_joins_lab_state() -> Optional[dict]:
    data = safe_user_get(_JOINS_LAB_KEY, default=None)
    if not isinstance(data, dict):
        return None
    if data.get('schema_version') != _SCHEMA_VERSION:   # unchanged exact-match check
        return None
    return data
    # New Phase-120 keys are read by callers with .get(key, <default>) so v1 blobs
    # (which lack them) restore cleanly without any version migration.
```

---

## Shared Patterns

### Authentication gate
**Source:** `web/auth_state.py:358` (`create_login_dialog`) + `web/pages/joins_lab.py:1550-1551`
**Apply to:** ACT-01 Add-as-Join, ACT-03 Add-to-List, D-17 list picker, D-18 sign-in fix
```python
from web.auth_state import GlobalAuthState, create_login_dialog

if GlobalAuthState.is_logged_in():
    user = GlobalAuthState.get_user()
    user_id = user['id']
    # ... proceed with authenticated operation
else:
    create_login_dialog().open()  # overlay dialog, page stays mounted
```

### Off-loop I/O discipline
**Source:** `web/pages/joins_lab.py:396-407` (`_fetch_vs_candidates` / `run_vs_core` pattern)
**Apply to:** ALL new I/O: community writes, list writes, text fetch, image prefetch, VS probe
```python
def run_my_core():
    # sync blocking function — dispatched via run.io_bound
    return some_blocking_operation(...)

result = await run.io_bound(run_my_core)
```

Naming convention: sync closure named `run_*_core` (e.g. `run_vs_core`, `run_search_core`).
This is enforced by the `tests/test_joins_lab_off_loop.py` AST scanner.

**Never call `safe_user_get`/`safe_user_set` inside the sync closure** — storage requires UI
context (web/safe_storage.py:63-66); reads inside `run.io_bound` silently return defaults.

### SEED-008 fire-and-forget guard
**Source:** `web/pages/joins_lab.py:2119-2123` + `web/components/joins_panel.py:507-513`
**Apply to:** ALL `asyncio.ensure_future(...)` calls that mutate UI after any `await`
```python
_page_client = ui.context.client  # captured once at page build time

async def _my_task():
    try:
        with _page_client:
            data = await run.io_bound(...)
            # UI mutations here
    except RuntimeError:
        return  # client/tab deleted — standard SEED-008 guard

asyncio.ensure_future(_my_task())
```

### Event propagation (stop_propagation)
**Source:** `tests/test_no_server_side_stop_propagation.py`
**Apply to:** Any nested clickable inside a card/row that should not bubble
```python
# CORRECT — JS handler (not Python-side):
ui.element(...).props("js_handler='(e) => e.stopPropagation()'")

# FORBIDDEN — will fail CI:
on_click=lambda e: e.stop_propagation()
```

### Supabase community write pattern
**Source:** `web/supabase_client.py:1625-1665`
**Apply to:** ACT-01 (create_fragment_join), D-03 (delete_fragment_join), ACT-03 (add_list_item)
```python
# Always uses get_user_client() internally (RLS-enforced)
# Always returns {'success': True, ...} or {'error': str}
result = create_fragment_join(user_id=..., fragment_a_sys_id=..., ...)
if 'error' in result:
    ui.notify(tr('Could not add join. Check your connection.'), type='negative')
else:
    # success path
```

### `tr()` bilingual strings
**Source:** `web/translations.py` (used in every web UI file)
**Apply to:** Every user-visible string in Phase 120
```python
from web.translations import tr, is_rtl
ui.label(tr('Add as Join'))
ui.label(tr('Restoring your search…'))
```

All new Phase-120 string keys are listed in the UI-SPEC Copywriting Contract table.

---

## No Analog Found

None — all new/modified files have strong analogs in the existing codebase.

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| n/a | n/a | n/a | All 10 files have close analogs |

---

## Invariant Guards (Must Stay Green)

| Guard | File | What It Checks | Risk if Broken |
|-------|------|----------------|----------------|
| `test_no_raw_storage_access.py` | all `web/*.py` | no direct `app.storage.user` | Phase-87 chokepoint bypass |
| `test_joins_lab_off_loop.py` | `web/pages/joins_lab.py` | `execute_search` only inside `run.io_bound` | event-loop blockage |
| `test_no_server_side_stop_propagation.py` | all `web/*.py` | no `.stop_propagation()` in Python | broken click isolation |
| `puzzle_staging` allowlist | `.planning/phase87_storage_allowlist.yaml` | new `safe_user_*` key declared | CI scan failure |

---

## Metadata

**Analog search scope:** `web/pages/`, `web/components/`, `web/`, `shared/`, `desktop/join_workbench.py`
**Files scanned:** ~20 (targeted reads; all sources listed in RESEARCH.md §Sources)
**Pattern extraction date:** 2026-06-20

# Phase 118: Joins, Entry & Full Builders — Pattern Map

**Mapped:** 2026-06-18
**Files analyzed:** 8 new/modified files
**Analogs found:** 8 / 8

All CONTEXT.md line references verified against live code — all accurate as of 2026-06-18
(see RESEARCH.md verification table). One note: `compose()` def is at line 695, not 741;
the `:741` reference in CONTEXT.md points to the `ro` dict block inside compose, which is
correct for its purpose.

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/pages/joins_lab.py` (modify) | page/controller | request-response + event-driven | `web/pages/joins_lab.py` itself (Phase 117 base) | self-extension |
| `web/components/joins_builder.py` (new) | component | transform (BuilderRow → SideQuery) | `desktop/join_workbench.py` JoinQueryBuilder (:797–1466) | role-match |
| `web/components/joins_panel.py` (modify) | component | CRUD + request-response | `web/components/joins_panel.py` itself | self-extension |
| `web/supabase_client.py` (modify) | service | CRUD | `web/supabase_client.py:1574-1623` | self-extension |
| `web/pages/search_results.py` (modify) | component/view | request-response | `web/pages/search_results.py:581-629` action row | self-extension |
| `web/pages/browse.py` (modify) | page/controller | request-response | `web/pages/browse.py:3895-3910` joins button | self-extension |
| `tests/test_merge_globals_web.py` (new) | test | transform | `tests/test_joins_lab_off_loop.py` | role-match |
| `tests/test_other_side_page_contract.py` (new) | test | transform | `tests/test_joins_lab_off_loop.py` | role-match |
| `tests/test_builder_modifier_hoist.py` (new) | test | transform | `tests/test_joins_lab_off_loop.py` | role-match |
| `tests/test_known_joins_group.py` (new) | test | CRUD | `tests/test_joins_lab_off_loop.py` | role-match |
| `tests/test_joins_anc05_rls.py` (new) | test | CRUD | `tests/test_joins_lab_off_loop.py` | role-match |

---

## Pattern Assignments

### `web/pages/joins_lab.py` (modify — Phase 117 base extended in-place)

**Analog:** `web/pages/joins_lab.py` itself (Phase 117 spine)

#### Imports pattern (lines 1–62):
```python
from __future__ import annotations
import asyncio
import logging
from typing import Optional
from nicegui import run, ui
from shared.joins_lab import BuilderRow, SideQuery, compose, dedup_candidates
# Phase 118 adds:
from shared.joins_lab import resolve_other_side_pages, apply_cross_side
from web.components.anchor_viewer import AnchorViewer, inject_viewer_assets
from web.components.candidate_grid import create_candidate_grid
# Phase 118 adds:
from web.components.joins_builder import create_joins_builder
from web.components.joins_panel import fetch_connected_fragments
from web.joins_executor import WebSearchExecutor
from web.joins_lab_storage import read_anchor, write_anchor
from web.services import get_service
from web.state import state
from web.translations import is_rtl, tr
```

#### Off-loop search pattern — anchor side (lines 500–621):

The Phase-117 `execute_joins_search` is the exact template. Phase 118 replaces
`lines_to_side_query(search_textarea.value)` with `builder_widget.build_side_query()`,
adds `_merge_globals_web(ro, _global_opts)` after `compose(side)`, reads mode from
the mode selector, and appends the cross-side block after the dedup step.

**Critical function name requirement (CI guard):** The sync closure passed to
`run.io_bound` MUST use the literal name `run_search_core` (anchor side) and
`run_cross_side_core` (other side). The AST scanner in
`tests/test_joins_lab_off_loop.py:68-93` checks for these exact names as the first
positional arg to `run.io_bound(...)`.

```python
# ANCHOR SIDE (replaces lines 550-560; same structural pattern):
def run_search_core():
    return executor.execute_search(
        query_str,
        mode=_mode_str,          # 'exact' | 'variants' | 'fuzzy' from mode selector
        gap=0,
        progress_callback=_make_progress_cb(my_gen, _search_generation),
        responsa_options=ro,     # AFTER _merge_globals_web(ro, _global_opts)
        text_position=page_position,   # may be 'line_start' | 'line_end' directly
        corpus_scope='genizah',
    )
search_coro = run.io_bound(run_search_core)
_current_task['task'] = asyncio.ensure_future(
    asyncio.wait_for(search_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
)

# CROSS-SIDE BLOCK (appended after dedup step, before create_candidate_grid):
if other_side_enabled and other_side_query.rows:
    def run_cross_side_core():
        b_query, b_ro, _ = compose(other_side_query)
        _merge_globals_web(b_ro, _global_opts)   # BLD-04: must apply here too
        return apply_cross_side(
            executor, list(base_candidates), b_query, b_ro, combine_mode
        )
    cross_coro = run.io_bound(run_cross_side_core)
    cross_task = asyncio.ensure_future(
        asyncio.wait_for(cross_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
    )
    merge_result = await cross_task
    final_candidates = list(merge_result.candidates)
else:
    final_candidates = base_candidates
```

#### Stale-generation + cancellation pattern (lines 516-605):

Copy the exact three-step pattern from Phase 117:
1. `_search_generation['value'] += 1; my_gen = _search_generation['value']`
2. `prev = _current_task['task']; if prev and not prev.done(): prev.cancel()`
3. `if not _should_apply_results(my_gen, _search_generation): return` (after await)

The `_make_progress_cb`, `_should_apply_results`, and `_is_running` dict patterns
at lines 133–186 are unchanged — copy verbatim into Phase 118's extended module.

#### `_merge_globals_web` helper (new, module-level):

```python
# NEW module-level helper — pure, importable for tests
def _merge_globals_web(ro: dict, global_opts: dict) -> dict:
    """Re-inject flex_spacing + bidirectional into a compose()-produced ro.

    compose() at shared/joins_lab.py:741-749 hardcodes ja/flex/bidir=False.
    This step pulls the actual UI-toggle state back in (RR-14 parity, D-11).
    ja intentionally excluded — stays False per D-10 (user decision).
    variants flows correctly via SideQuery.variants — NOT re-merged here.
    """
    ro['flex_spacing'] = global_opts.get('flex_spacing', False)
    ro['bidirectional'] = global_opts.get('bidirectional', False)
    return ro
```

**Apply to BOTH sides:** `_merge_globals_web(ro, _global_opts)` after anchor
`compose()`, and `_merge_globals_web(b_ro, _global_opts)` after other-side
`compose()` inside `run_cross_side_core`. Mirror of
`desktop/join_workbench.py:2519` (anchor) and `:2580` (other-side).

#### Known-joins async load pattern (new async helper in page):

```python
async def _load_known_joins(sys_id: str, shelfmark: str,
                            pgpid: Optional[int] = None) -> None:
    """Fetch confirmed-only joins for the anchor; render in known_joins_container."""
    known_joins_spinner.set_visibility(True)
    try:
        data = await run.io_bound(
            fetch_connected_fragments,
            shelfmark=shelfmark,
            document_id=sys_id,
            pgpid=pgpid,
            confirmed_only=True,          # ANC-05: confirmed-only cache key
            force_refresh=False
        )
        _render_known_joins(data)
    except Exception:
        _render_known_joins_error()
    finally:
        known_joins_spinner.set_visibility(False)
```

Call this from `load_anchor()` after the AnchorViewer is updated (replacing the
`write_anchor` tail of the existing `load_anchor` at line 416).

#### Text Position + mode in the search closure:

The Text Position value from the builder control must be split into two paths
(SideQuery.page_position only accepts None/'start'/'end' — ValueError on
'line_start'/'line_end', verified `shared/joins_lab.py:67-71`):

```python
# After collecting text_position_val from the UI select:
_SIDEQUERY_PAGE_POSITIONS = {None, 'start', 'end'}
tp_val = text_position_select.value   # 'anywhere'|'start'|'end'|'line_start'|'line_end'

if tp_val == 'anywhere' or tp_val is None:
    page_position_for_sidequery = None
    direct_text_position = None
elif tp_val in ('start', 'end'):
    page_position_for_sidequery = tp_val
    direct_text_position = None      # compose() will return it via page_position
elif tp_val in ('line_start', 'line_end'):
    page_position_for_sidequery = None   # do NOT put in SideQuery
    direct_text_position = tp_val        # pass directly to execute_search
```

In `run_search_core`, `text_position` = `direct_text_position or page_position`
(where `page_position` is the compose() return value).

---

### `web/components/joins_builder.py` (new — the line builder widget)

**Analog:** `desktop/join_workbench.py` JoinQueryBuilder (lines 797–1466)

This is a new NiceGUI component file, NOT a port of the desktop Qt widget class.
It is a factory function `create_joins_builder(allow_page_position=True)` returning
a widget object with a `build_side_query()` method. The builder state is managed
via mutable-dict closures (matching the Phase 117 page pattern), not a class
`__init__`.

#### Imports pattern:
```python
from __future__ import annotations
from typing import Optional, List
from nicegui import ui
from shared.joins_lab import BuilderRow, SideQuery
from web.translations import tr
```

#### Per-row hoist rules (from `desktop/join_workbench.py:1272-1347`):

The web builder uses D-04 (user types raw Responsa text in one field; `a/b` = OR-alts).
Modifiers are stored as booleans per row and applied to the user-typed term BEFORE
constructing `BuilderRow`. The hoist mirrors the desktop exactly:

```python
def _apply_modifiers_to_term(term: str, mods: dict) -> str:
    """Apply per-row modifiers to the user-typed term (desktop parity RR-13).

    D-04: user types 'word1/word2' for OR-alts; space = sequence.
    Modifier application order matches desktop/join_workbench.py:1300-1341.
    """
    t = term.strip()
    if not t:
        return t

    # Determine if multi-token (slash-group) — user may have typed 'a/b'
    # The web builder wraps modifiers around the full typed text:
    is_group = '/' in t and not t.startswith('(')
    wrapped = f'({t})' if is_group else t

    if mods.get('negation'):
        return f'-{wrapped}'
    if mods.get('plene'):
        wrapped = f'%{wrapped}'
    if mods.get('prefix'):
        wrapped = f'#{wrapped}'
    if mods.get('suffix'):
        wrapped = f'{wrapped}#'
    # wildcard_prefix NOT supported on slash-groups (RR-13 parity):
    if mods.get('wildcard_prefix') and not is_group:
        wrapped = f'*{wrapped}'
    if mods.get('wildcard_suffix'):
        wrapped = f'{wrapped}*'
    return wrapped
```

#### Builder row UI (from UI-SPEC §Component 1):

```python
def _create_builder_row(row_idx: int, rows_state: list,
                        on_change_callback) -> None:
    """Render one builder row: input + tune popover + remove btn + row number."""
    row_state = rows_state[row_idx]  # dict: {'term': '', 'mods': {}, 'gap_to_next': 0}

    with ui.row().classes('w-full items-center gap-2') as row_el:
        # Row number (RTL right-aligned, 12px muted)
        ui.label(str(row_idx + 1)).classes('text-xs w-5 text-right shrink-0').style(
            'color: var(--text-muted);'
        )

        # Main text input (RTL, Hebrew serif, outlined)
        term_input = ui.input(
            placeholder=tr('Words on this line (space = sequence, a/b = alternatives)')
        ).props('outlined dense').classes('flex-grow').style(
            'direction: rtl; text-align: right;'
            ' font-family: "Noto Sans Hebrew", "SBL Hebrew", serif; font-size: 1rem;'
        )

        # Per-row "more" (tune icon + popover menu)
        with ui.button(icon='tune').props('flat dense size=sm').tooltip(tr('Line options')) as tune_btn:
            with ui.menu() as mods_menu:
                for mod_key, mod_label in [
                    ('line_start', tr('Line start (⊢)')),
                    ('line_end',   tr('Line end (⊣)')),
                    ('plene',      tr('Plene / defective')),
                    ('prefix',     tr('Prefix')),
                    ('suffix',     tr('Suffix')),
                    ('wildcard_prefix', tr('Wildcard prefix')),
                    ('wildcard_suffix', tr('Wildcard suffix')),
                    ('negation',   tr('Negation')),
                ]:
                    cb = ui.checkbox(mod_label,
                                     value=row_state['mods'].get(mod_key, False))
                    cb.on('update:model-value', lambda v, k=mod_key: _set_mod(row_idx, k, v))

        # Remove button (hidden on the last remaining row)
        remove_btn = ui.button(icon='close').props('flat dense size=sm color=negative')
        # Shown/hidden logic based on len(rows_state) > 1
```

#### `build_side_query()` method:

```python
def build_side_query(variants: bool, page_position_override: Optional[str]) -> Optional[SideQuery]:
    """Build SideQuery from current builder rows.

    page_position_override: pre-validated value (None, 'start', or 'end' only —
    callers handle the 'line_start'/'line_end' bypass path before calling this).
    """
    builder_rows = []
    for rs in rows_state:
        term = _apply_modifiers_to_term(rs['term'], rs['mods'])
        builder_rows.append(BuilderRow(
            term=term,
            line_start=rs['mods'].get('line_start', False),
            line_end=rs['mods'].get('line_end', False),
            gap_to_next=rs.get('gap_to_next', 0),
        ))
    non_empty = [r for r in builder_rows if r.term.strip()]
    if not non_empty:
        return None
    return SideQuery(
        rows=tuple(builder_rows),
        variants=variants,
        page_position=page_position_override,
    )
```

#### Gap control between rows (UI-SPEC §Component 1, D-07):

```python
def _create_gap_control(row_idx: int, rows_state: list) -> None:
    """Render the gap control between row_idx and row_idx+1."""
    with ui.row().classes('items-center gap-2 py-1') as gap_row:
        gap_label = ui.label(tr('↕ gap')).classes('text-xs').style(
            'color: var(--text-tertiary);'
        )
        gap_input = ui.number(
            value=rows_state[row_idx].get('gap_to_next', 0),
            min=0, max=20, step=1
        ).props('outlined dense').style('width: 56px;')
        gap_input.on('update:model-value',
                     lambda v, i=row_idx: _set_gap(i, int(v or 0)))
        # When gap > 0: show --border-focus; when 0: show --neutral-300
        # (handled via a reactive style update on gap_input)
```

---

### `web/components/joins_panel.py` (modify — ANC-04/ANC-05 + FND entry)

**Analog:** `web/components/joins_panel.py` itself (lines 24–766)

#### ANC-05 fix — `fetch_connected_fragments` signature extension (lines 32–267):

Add a `confirmed_only: bool = False` parameter. When `True`, use a separate cache
key (`doc:{document_id}:pgp:{pgpid}:confirmed`) and pass `status='confirmed'` to
`get_fragment_joins`. This prevents the confirmed-only cache from poisoning the
full-joins cache used by the browse joins dialog.

```python
# Current line 51 cache key:
cache_key = f"doc:{document_id}:pgp:{pgpid}" if document_id else f"shelf:{shelfmark}:pgp:{pgpid}"

# Extended with confirmed_only suffix:
cache_key = (
    f"doc:{document_id}:pgp:{pgpid}:confirmed"
    if (document_id and confirmed_only)
    else f"doc:{document_id}:pgp:{pgpid}"
    if document_id
    else f"shelf:{shelfmark}:pgp:{pgpid}"
)

# Current line 64 (the Supabase fetch call):
joins = get_fragment_joins(fragment_sys_id=document_id)

# When confirmed_only=True:
joins = get_fragment_joins(
    fragment_sys_id=document_id,
    status='confirmed' if confirmed_only else None,
)
```

**Pre-condition:** Verify the `status` column exists in the live `fragment_joins`
schema before deploying. The RESEARCH.md flagged this as Assumption A1 (LOW confidence).
If the column does not exist, fall back to filtering `formatted_joins` by join type.

#### "Find more joins" button — insertion into `create_joins_dialog` (lines 652–655):

The existing "View All Fragments" button is at lines 652–655. Insert the "Find more
joins" button immediately below it:

```python
# Analog: existing "View All Fragments" button at lines 652-655:
ui.button(
    tr('View All Fragments'), icon='auto_stories',
    on_click=handle_view_all
).props('outline color=green').classes('w-full')

# NEW — "Find more joins" flat button (Phase 118, FND-04/05):
if find_joins_url:   # passed as new optional param to create_joins_dialog
    def _open_lab():
        dialog.close()
        ui.navigate.to(find_joins_url, new_tab=True)

    ui.button(
        tr('Find more joins'), icon='science',
        on_click=_open_lab
    ).props('flat color=primary').classes('w-full').tooltip(
        tr('Go to Joins Lab to find more joins')
    )
```

**Signature change (backward-compatible):** Add `find_joins_url: Optional[str] = None`
to `create_joins_dialog()` (line 377) and `create_joins_button()` (line 310).
All existing call sites omit it and get the same behavior.

#### No-joins "Find Joins" button recolor (inside `create_joins_button`, lines 354–372):

```python
# Current button creation (line 354-358):
btn = ui.button(
    icon='link',
    on_click=open_joins_panel
).props(f'flat dense size={size}').classes('text-green-700').tooltip(tr('Joined Fragments'))

# Phase 118 extension: when find_joins_url is provided AND no joins exist:
def load_count():
    data = fetch_connected_fragments(shelfmark=shelfmark, document_id=document_id, pgpid=pgpid)
    join_count['value'] = data.get('total_joins', 0)
    if button_ref['btn']:
        if join_count['value'] > 0:
            button_ref['btn'].props('color=green').classes('bg-green-100 ring-2 ring-green-500',
                                                           remove='text-green-700')
        elif find_joins_url:
            # D-19: no-joins path → recolor + redirect click to Lab
            button_ref['btn'].props(remove='color=green')
            button_ref['btn'].classes('text-neutral-500', remove='text-green-700')
            button_ref['btn'].tooltip(tr('Find Joins in the Joins Lab'))
            button_ref['btn'].on('click', lambda: ui.navigate.to(find_joins_url, new_tab=True))
```

---

### `web/supabase_client.py` (modify — ANC-05 `status` filter)

**Analog:** `web/supabase_client.py:1574-1623` `get_fragment_joins`

#### Current fetch pattern (lines 1587–1596):

```python
# Current (no status filter):
response = (
    client.table('fragment_joins')
    .select('*')
    .or_(f'fragment_a_sys_id.eq.{fragment_sys_id},'
         f'fragment_b_sys_id.eq.{fragment_sys_id}')
    .execute()
)
```

The `status` parameter is already in the function signature. The ANC-05 fix verifies
it is actually applied when provided (existing code may have the parameter but skip
the filter). Ensure the filter is applied:

```python
query = client.table('fragment_joins').select('*').or_(...)
if status:
    query = query.eq('status', status)
response = query.execute()
```

This is a minimal surgical change — no new Supabase schema required.

---

### `web/pages/search_results.py` (modify — FND-04 joins icon on cards)

**Analog:** `web/pages/search_results.py:581-629` action buttons row

#### Insertion point (after line 628 — Catalog Records button):

```python
# After the existing Catalog Records button block (lines 620-629):
# NEW — Joins icon (D-21, FND-04):
if sys_id:
    _joins_url = f'/joins-lab?sys_id={sys_id}'
    if _card_ie_id:
        _joins_url += f'&volume_ie={_card_ie_id}'

    joins_icon_ref = {'btn': None, 'has_joins': False}

    def _open_joins_for_card(s=sys_id, sm=shelfmark, ie=_card_ie_id,
                             url=_joins_url):
        """D-21: joins exist → open dialog; none → open Lab in new tab."""
        if joins_icon_ref['has_joins']:
            create_joins_dialog(
                shelfmark=sm,
                document_id=s,
                find_joins_url=url,
            )
        else:
            ui.navigate.to(url, new_tab=True)

    joins_btn = ui.button(
        icon='link',
        on_click=_open_joins_for_card
    ).props('flat round dense size=sm').style(
        'color: var(--neutral-400);'   # neutral until joins loaded
    ).tooltip(tr('Joins'))
    joins_icon_ref['btn'] = joins_btn

    def _load_joins_count_for_card(s=sys_id, sm=shelfmark, ie=_card_ie_id):
        try:
            data = fetch_connected_fragments(shelfmark=sm, document_id=s)
            has = data.get('total_joins', 0) > 0
            joins_icon_ref['has_joins'] = has
            if joins_btn and has:
                joins_btn.style('color: var(--primary-600);')  # green
        except Exception:
            pass

    asyncio.get_event_loop().call_later(0.15, _load_joins_count_for_card)
```

**Import additions** needed at top of `search_results.py`:
```python
from web.components.joins_panel import fetch_connected_fragments, create_joins_dialog
```

---

### `web/pages/browse.py` (modify — FND-05 "Find more joins" + recolor)

**Analog:** `web/pages/browse.py:3895-3910` existing `create_joins_button` call

#### Modification (lines 3903–3910):

Pass the new `find_joins_url` parameter to `create_joins_button`:

```python
# Current (lines 3904-3910):
create_joins_button(
    shelfmark=page.shelfmark,
    document_id=page.sys_id,
    pgpid=pgpid_for_joins,
    on_navigate=navigate_to_shelfmark,
    on_view_all=enter_joined_view
)

# Phase 118 (add find_joins_url):
_joins_lab_url = f'/joins-lab?sys_id={page.sys_id}'
if page.volume_ie:
    _joins_lab_url += f'&volume_ie={page.volume_ie}'

create_joins_button(
    shelfmark=page.shelfmark,
    document_id=page.sys_id,
    pgpid=pgpid_for_joins,
    on_navigate=navigate_to_shelfmark,
    on_view_all=enter_joined_view,
    find_joins_url=_joins_lab_url,   # FND-05
)
```

#### New-tab navigation pattern (from `web/pages/download.py:37`):

```python
ui.navigate.to(url, new_tab=True)
```

Use this over `ui.run_javascript('window.open(...)')` for internal SPA routes
(NiceGUI-native form). The `window.open` JS form at `browse.py:3660,3663` is
for EXTERNAL URLs (Cambridge Digital Library, etc.).

---

## New Test Files — Wave 0 Scaffolding

### `tests/test_merge_globals_web.py` (new)

**Analog:** `tests/test_joins_lab_off_loop.py` — pure AST/headless unit test structure

```python
# Pattern: headless, no NiceGUI imports, pure function tests
from shared.joins_lab import BuilderRow, SideQuery, compose
# Import the helper to test (will be in web/pages/joins_lab.py after Phase 118):
# from web.pages.joins_lab import _merge_globals_web

def test_flex_spacing_injected_into_ro():
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, ro, _ = compose(side)
    assert ro['flex_spacing'] is False   # compose hardcodes False
    _merge_globals_web(ro, {'flex_spacing': True, 'bidirectional': False})
    assert ro['flex_spacing'] is True

def test_bidirectional_injected_into_ro():
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=False)
    _, ro, _ = compose(side)
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': True})
    assert ro['bidirectional'] is True

def test_ja_not_injected():
    """D-10: ja stays False regardless of any opts passed."""
    side = SideQuery(rows=(BuilderRow(term='אמת'),), variants=False)
    _, ro, _ = compose(side)
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': False})
    assert ro.get('ja') is False   # ja must never become True

def test_variants_not_touched_by_merge():
    """variants flows via SideQuery.variants; _merge_globals_web must not override it."""
    side = SideQuery(rows=(BuilderRow(term='שלום'),), variants=True)
    _, ro, _ = compose(side)
    _merge_globals_web(ro, {'flex_spacing': False, 'bidirectional': False})
    assert ro['variants'] is True   # still True from SideQuery
```

### `tests/test_other_side_page_contract.py` (new)

**Analog:** `tests/test_joins_lab_off_loop.py` — headless, no engine instantiation

Key test cases to scaffold:
- `test_p_num_used_not_internal_index` — mock `get_browse_page` returning a dict with
  `p_num=3, internal_index=2, total_pages=5`; assert `resolve_other_side_pages(3, 5)` returns `{2, 4}`
- `test_total_pages_zero_treated_as_none` — `total_pages=0` → `resolve_other_side_pages(1, None)` → `{2}`
- `test_metadata_only_returns_none` — `get_browse_page` returns dict with `total_pages=0` → `total_pages=None` passed
- `test_multi_ie_total_pages_scoped` — multi-IE volume_ie passed to `get_browse_page`

### `tests/test_builder_modifier_hoist.py` (new)

Key test cases:
- Each modifier applied singly to a single-token term (`'שלום'` → `'-שלום'` for negation, etc.)
- Multi-token (`a/b`) produces group before modifier: `negation` → `'-(a/b)'`
- `wildcard_prefix` NOT applied to multi-token (RR-13)
- `line_start=True` → `BuilderRow(term='שלום', line_start=True)` → `compose()` output has leading `|`
- `gap_to_next=2` → `compose()` output includes `[|2]` marker

### `tests/test_known_joins_group.py` (new)

Key test cases:
- `fetch_connected_fragments` returns correctly structured dict with `fragment_details`
- Source attribution: PGP joins have `sources=['PGP']`, FJMS have `sources=['FJMS']`
- Multi-source dedup: same pair from PGP + FJMS → `sources=['PGP', 'FJMS']`
- Empty result: `total_joins=0` → `{"fragments": [], "joins": [], ...}`

### `tests/test_joins_anc05_rls.py` (new)

Key test cases:
- Cache key isolation: `confirmed_only=True` uses `doc:{id}:pgp:{pgpid}:confirmed` key
- `confirmed_only=False` uses `doc:{id}:pgp:{pgpid}` key (no cross-contamination)
- The `status='confirmed'` filter is passed to `get_fragment_joins` when `confirmed_only=True`

---

## Shared Patterns

### Off-loop search dispatch (CI-guarded)
**Source:** `web/pages/joins_lab.py:549-565` (Phase 117 `run_search_core` pattern)
**Apply to:** All new search paths in Phase 118 (`run_cross_side_core`, known-joins async load)
```python
def run_search_core():          # sync closure — execute_search ONLY here
    return executor.execute_search(...)
search_coro = run.io_bound(run_search_core)   # function NAME as first arg (CI scanned)
_current_task['task'] = asyncio.ensure_future(
    asyncio.wait_for(search_coro, timeout=_SEARCH_TIMEOUT_SECONDS)
)
```

### Stale-generation guard (all three legs)
**Source:** `web/pages/joins_lab.py:516-604`
**Apply to:** The extended `execute_joins_search` and the cross-side block
```python
_search_generation['value'] += 1
my_gen = _search_generation['value']
# ... after await ...
if not _should_apply_results(my_gen, _search_generation):
    return
```

### `safe_user_*` chokepoint (Phase 87 invariant)
**Source:** `web/joins_lab_storage.py:34` and `web/safe_storage.py`
**Apply to:** Any new per-user state in Phase 118 (builder-state persistence is Phase 120;
for Phase 118 the only new storage touch is extending `write_anchor` if volume_ie needs
updating — same pattern as lines 85-91 in `joins_lab_storage.py`)
```python
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop
# Never: app.storage.user['...'] = ...   ← CI guard blocks this
```

### Bilingual `tr()` from line one
**Source:** `web/pages/joins_lab.py:270-342` (all user-facing strings use `tr()`)
**Apply to:** Every new string in Phase 118 — builder labels, gap control, known-joins
section, button tooltips. Pattern:
```python
ui.label(tr('Known Joins')).classes('text-sm font-semibold')
ui.button(tr('Find more joins'), icon='science').tooltip(tr('Go to Joins Lab to find more joins'))
```

### New-tab navigation for "Find joins"
**Source:** `web/pages/download.py:37` and `web/pages/browse.py:3660`
**Apply to:** All "Find joins" → Joins Lab navigations (D-18/FND-04/FND-05)
```python
# Internal SPA routes — NiceGUI-native (preferred):
ui.navigate.to(f'/joins-lab?sys_id={sys_id}', new_tab=True)
# External URLs only — JS form:
ui.run_javascript(f'window.open("{url}", "_blank")')
```

### `ui.expansion_item` collapsible disclosure
**Source:** Used throughout `web/pages/search.py` advanced options
**Apply to:** Advanced search options disclosure (D-12) and known-joins group (D-15)
```python
with ui.expansion_item(tr('Advanced search options'), icon='tune').classes('w-full') \
        .style('background: var(--bg-tertiary); border: 1px solid var(--border-light); border-radius: 8px;'):
    # Global toggles + other-side builder inside here
    flex_cb = ui.checkbox(tr('Flexible spacing'))
    bidir_cb = ui.checkbox(tr('Bidirectional'))
```

---

## No Analog Found

No files in Phase 118 are entirely without an analog. The new `web/components/joins_builder.py`
has no existing web analog but has a strong desktop analog (`desktop/join_workbench.py`
`JoinQueryBuilder`). The builder widget pattern (factory function returning a widget-state
object with a `build_side_query()` method) is the web-idiomatic equivalent of the Qt class.

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `web/components/joins_builder.py` | component | transform | No existing web builder widget; closest is the desktop JoinQueryBuilder (Qt class — cannot port directly; must re-express as NiceGUI factory + mutable-dict state) |

---

## Line Reference Drift Report

All CONTEXT.md and RESEARCH.md line references verified against live code. Status:

| Reference | Claimed Lines | Verified | Note |
|-----------|--------------|----------|------|
| `lines_to_side_query` | joins_lab.py:116-130 | 116-130 | Verified |
| Phase-117 textarea seam | joins_lab.py:333-340 | 333-340 | Verified |
| `execute_joins_search` | joins_lab.py:500-621 | 500-621 | Verified |
| `compose()` ro dict | joins_lab.py:741-749 | 742-749 | `def compose` is at 695; `:741` = ro dict start |
| `resolve_other_side_pages` | joins_lab.py:283-303 | 283-303 | Verified |
| `_merge_globals` | desktop:2475-2489 | 2475-2489 | Verified |
| Applied to anchor+other-side | desktop:2519, :2580 | 2519, 2580 | Verified |
| `get_fragment_joins` | supabase_client.py:1574-1623 | 1574-1623 | Verified |
| Global cache | joins_panel.py:24-29 | 24-29 | `_joins_cache` at line 25 |
| Text Position control | search.py:646-655 | 646-655 | Verified |
| Action row | search_results.py:581-629 | 581-630 | Verified |
| Joins button in browse | browse.py:3904 | 3904 | Verified |
| `SideQuery.page_position` validates None/'start'/'end' | joins_lab.py:67-71 | 67-71 | Verified — 'line_start'/'line_end' will raise ValueError |
| `badge_for_source` | desktop:166-179 | 166-179 | Verified |
| `build_side_query` hoist | desktop:1272-1347 | 1272-1347 | Verified |

---

## Metadata

**Analog search scope:** `web/pages/`, `web/components/`, `desktop/`, `shared/`, `tests/`
**Files scanned:** 12 source files read in full or targeted sections
**Pattern extraction date:** 2026-06-18

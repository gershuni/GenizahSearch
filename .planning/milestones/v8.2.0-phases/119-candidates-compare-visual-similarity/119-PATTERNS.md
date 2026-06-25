# Phase 119: Candidates, Compare & Visual Similarity — Pattern Map

**Mapped:** 2026-06-19
**Files analyzed:** 5 new/modified files
**Analogs found:** 5 / 5

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/components/candidate_grid.py` (extend) | component | CRUD + event-driven | `web/components/candidate_grid.py` (itself — Phase 117) | exact (self-extension) |
| `web/components/compare_modal.py` (NEW) | component | request-response | `web/components/text_editor.py` + `web/components/visual_similarity_dialog.py` | role-match (full-screen maximized dialog pattern) |
| `web/pages/joins_lab.py` (extend) | page | event-driven + async | `web/pages/joins_lab.py` (itself — Phase 117/118) | exact (self-extension) |
| `shared/joins_lab.py` (extend — Wave 0 gap) | utility | transform | `shared/joins_lab.py` (itself) + `desktop/join_workbench.py:452-457` | exact analog in desktop |
| `tests/test_joins_lab_off_loop.py` (extend) | test | — | `tests/test_joins_lab_off_loop.py` (itself) | exact (same AST guard, extend scope) |

---

## Pattern Assignments

### `web/components/candidate_grid.py` (extend — triage / table / filters / pagination / VS badge)

**Primary analog:** `web/components/candidate_grid.py` lines 1–309 (the Phase-117 read-only grid being grown)
**Secondary analog (table + filter dialog):** `desktop/join_workbench.py:2935-2984` (`apply_filters`) + `:3373` (`_open_filter_dialog`)
**Secondary analog (triage restyle):** `desktop/join_workbench.py:3344-3351` (`_restyle_card`)

**Imports pattern** (lines 26–36):
```python
from __future__ import annotations

import json
from typing import Optional, Callable

from nicegui import ui

from shared.synthetic_sys_id import is_synthetic_sys_id
from web.services import is_oxford_manuscript, get_oxford_direct_image_url
from web.translations import tr, get_language
```

Phase 119 adds:
```python
from shared.joins_lab import Candidate, badge_and_tooltip
from shared.fjms_service import get_fjms_service
```

**Thumbnail URL pattern — proxy guard (Oxford direct-Bodleian path preserved)** (lines 63–106):
```python
def build_thumbnail_url(
    sys_id: str,
    page: Optional[int],
    shelfmark: str = "",
    library_code: str = "",
) -> Optional[str]:
    page_idx = max(0, (page or 1) - 1)
    if is_synthetic_sys_id(sys_id):
        return None
    try:
        is_oxford = is_oxford_manuscript(shelfmark, library_code)
    except Exception:
        is_oxford = False
    if is_oxford:
        try:
            ox_url = get_oxford_direct_image_url(shelfmark, page_idx)
        except Exception:
            ox_url = ""
        if ox_url:
            return ox_url
        return f"/api/oxford_image/{sys_id}?page={page_idx}"
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300"
```
Phase 119 changes ONLY the width param: `?page={page_idx}&width=300` → still correct for
160×160 thumbnails (the proxy serves max-width).

**Pagination pattern (D-08 — replaces `_MAX_RENDERED_CANDIDATES = 200`):**
```python
# Page-level closure state (NOT safe_storage — Phase 120 adds persistence)
_PAGE_SIZE = 24
_current_page: dict = {'value': 0}   # 0-indexed

def _paginate(filtered: list) -> tuple[list, int, int]:
    """Return (page_slice, current_page_0indexed, total_pages)."""
    total = len(filtered)
    total_pages = max(1, (total + _PAGE_SIZE - 1) // _PAGE_SIZE)
    page = min(_current_page['value'], total_pages - 1)
    _current_page['value'] = page
    start = page * _PAGE_SIZE
    return filtered[start:start + _PAGE_SIZE], page, total_pages
```
Filter changes MUST reset `_current_page['value'] = 0` before re-rendering.
Triage changes MUST NOT reset the page (dict mutation only → `_restyle_all`).

**Triage state pattern (D-11, desktop parity `wb.triage[sys_id]`):**
```python
# Page-level closure — single source of truth; shared by grid, table, Compare
_triage: dict[str, str] = {}   # 'yes' | 'maybe' | 'no'

def _set_triage(sys_id: str, verdict: str) -> None:
    _triage[sys_id] = verdict
    _restyle_all(sys_id)   # update every card/row for this sys_id

def _set_triage_bulk(sys_ids: list[str], verdict: str) -> None:
    for sid in sys_ids:
        _triage[sid] = verdict
    for sid in sys_ids:
        _restyle_all(sid)
```
**Never write to `safe_storage` in Phase 119.** Persistence is Phase 120 (PST-01).

**Desktop `_restyle_card` parity** (`:3344-3351`):
```python
def _restyle_all(sys_id: str) -> None:
    """Update every visible card and table row whose sys_id matches."""
    # Card borders: green/amber/red when triaged; default when not
    verdict = _triage.get(sys_id)
    color = {'yes': '#15803d', 'maybe': '#a16207', 'no': '#b91c1c'}.get(verdict)
    for ref in _card_refs.get(sys_id, []):
        # ref is the ui.card() element captured at render time
        border = f'border: 2px solid {color}' if color else 'border: 1px solid var(--border-light)'
        try:
            ref.style(f'border-radius:8px; {border};')
        except Exception:
            pass
    # Also update table row color and Compare verdict bar — same pattern
```

**Grid card layout (D-09 large thumbnails, 160×160):**
```python
with ui.card().classes("w-full p-2").style(
    "border-radius: 8px; border: 1px solid var(--border-light);"
) as card_el:
    _card_refs.setdefault(cand.sys_id, []).append(card_el)

    # Thumbnail: 160×160, object-fit:cover, full width, rounded top
    if thumb_url:
        img_el = ui.image(thumb_url).style(
            "width:100%; height:160px; object-fit:cover;"
            "border-radius:8px 8px 0 0; flex-shrink:0;"
        )
        img_el.on("error", js_handler=(
            "(e) => {"
            " e.target.style.display='none';"
            " const ph=document.createElement('div');"
            " ph.innerHTML='&#128196;';"
            " ph.setAttribute('style','" + _PLACEHOLDER_STYLE_160.replace("'","\\'") + "');"
            " e.target.parentNode.insertBefore(ph,e.target);"
            "}"
        ))
    else:
        ui.element("div").style(_PLACEHOLDER_STYLE_160).html("&#128196;")

    with ui.column().classes("flex-grow min-w-0 gap-2 p-2"):
        # ... library chip + shelfmark + title (unchanged from Phase 117)

        # 👁 badge (Phase 119 new) — badge_and_tooltip() precedence
        icon_name, tooltip_text = badge_and_tooltip(cand)
        if icon_name:
            ui.icon(icon_name).classes("text-sm").style(
                "color: #f59e0b;"
            ).tooltip(tooltip_text)

        # Triage row (Phase 119 new)
        with ui.row().classes("gap-1 items-center"):
            for verdict, label, color in [
                ('yes', tr('Yes'), '#15803d'),
                ('maybe', tr('Maybe'), '#a16207'),
                ('no', tr('No'), '#b91c1c'),
            ]:
                _v = verdict  # closure capture
                btn = ui.button(label).props('flat dense').style(
                    f'min-height:44px; font-size:0.75rem;'
                ).on('click', lambda v=_v, sid=cand.sys_id: _set_triage(sid, v))
                if _triage.get(cand.sys_id) == verdict:
                    btn.style(f'background:{color}; color:#fff;')

        # Compare button (Phase 119 new)
        ui.button(tr('Compare'), icon='compare_arrows').props('flat dense').tooltip(
            tr('Compare fragment')
        ).on('click', lambda sid=cand.sys_id: _open_compare(sid))
```

**Grid responsive layout (D-09, extends Phase-117's `sm:grid-cols-2`):**
```python
# Phase 117 was: grid grid-cols-1 sm:grid-cols-2
# Phase 119: add lg:grid-cols-3 (more cards fit at ≥1024px with smaller viewport)
with ui.grid().classes("w-full gap-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3"):
    for cand in page_slice:
        _create_candidate_card(cand, ...)
```

**Table view pattern (D-10, `ui.table` with sortable columns + multi-select):**
```python
# NiceGUI/Quasar ui.table — all columns except checkbox and triage sortable
columns = [
    {'name': 'select', 'label': '', 'field': 'sys_id', 'sortable': False},
    {'name': 'shelfmark', 'label': tr('Shelfmark'), 'field': 'shelfmark', 'sortable': True},
    {'name': 'score', 'label': tr('Score'), 'field': 'score', 'sortable': True},
    {'name': 'snippet', 'label': tr('Snippet'), 'field': 'snippet', 'sortable': False},
    {'name': 'material', 'label': tr('Material'), 'field': 'material', 'sortable': True},
    {'name': 'dimensions', 'label': tr('Dimensions'), 'field': 'dimensions', 'sortable': True},
    {'name': 'page', 'label': tr('Page'), 'field': 'page', 'sortable': True},
    {'name': 'triage', 'label': tr('Triage'), 'field': 'triage', 'sortable': False},
]
table = ui.table(
    columns=columns,
    rows=rows_data,
    row_key='uid',
    selection='multiple',
).classes('w-full')
```
Desktop `setSortingEnabled(False)` is NOT ported — web table is deliberately sortable (D-10 divergence).
Default sort: score descending. When VS toggle ON: sort switches to VS rank ascending.

**Filter dialog pattern (D-14, parity `_open_filter_dialog:3373`):**
```python
# Filters button opens a ui.dialog (not inline) — keeps the surface clean
def _open_filter_dialog() -> None:
    """Build a fresh filter dialog each time (parity desktop _open_filter_dialog:3373)."""
    with ui.dialog() as dlg, ui.card().classes('p-4 min-w-[360px] gap-4'):
        ui.label(tr('Filters')).classes('text-base font-semibold')

        # 1. Material (multi-select from enrichment values)
        # Options populated after enrichment completes (_enrichment_ready)
        mat_options = sorted({v.get('material') for v in _enrichment.values()
                               if v.get('material')})
        mat_sel = ui.select(
            options=mat_options, value=_filter_state['materials'],
            label=tr('Material'), multiple=True,
        ).props('dense outlined use-chips')

        # 2. Has dimensions
        dims_sw = ui.switch(tr('Has dimensions data')).bind_value(
            _filter_state, 'has_dims'
        )

        # 3. Size mismatch
        mismatch_sw = ui.switch(tr('Exclude size mismatch')).bind_value(
            _filter_state, 'exclude_mismatch'
        )

        # 4. Triage state (multi-select)
        triage_opts = ['All', 'Not triaged', 'Yes', 'Maybe', 'No']
        tri_sel = ui.select(
            options=triage_opts, value=_filter_state['triage_states'],
            label=tr('Triage state'), multiple=True,
        ).props('dense outlined use-chips')

        # 5. Text filter (optional — discretion; included per UI-SPEC)
        text_inp = ui.input(
            placeholder=tr('Filter by shelfmark…'),
            value=_filter_state.get('text_q', ''),
        ).props('dense outlined')

        with ui.row().classes('justify-end gap-2 mt-2'):
            ui.button(tr('Reset'), on_click=lambda: _reset_filters(dlg)).props('flat')
            ui.button(tr('Apply'), on_click=lambda: _apply_filters(dlg, mat_sel, dims_sw, mismatch_sw, tri_sel, text_inp)
            ).props('color=primary unelevated')

    dlg.open()
```

**Size-mismatch predicate (D-15, parity `:1687-1695`):**
```python
def _is_size_mismatch(candidate_width_cm, anchor_width_cm, threshold=1.4) -> bool:
    if candidate_width_cm is None or anchor_width_cm is None:
        return False
    if min(candidate_width_cm, anchor_width_cm) == 0:
        return False
    ratio = max(candidate_width_cm, anchor_width_cm) / min(candidate_width_cm, anchor_width_cm)
    return ratio > threshold
```

**Apply-filters predicate pattern (parity `apply_filters:2935-2984`):**
```python
def _compute_filtered(all_candidates: list) -> list:
    """Pure function — applies _filter_state to all_candidates, returns filtered list."""
    text_q = _filter_state.get('text_q', '').strip().lower()
    materials = set(_filter_state.get('materials', []))
    has_dims = _filter_state.get('has_dims', False)
    exclude_mismatch = _filter_state.get('exclude_mismatch', False)
    triage_states = set(_filter_state.get('triage_states', []))

    out = []
    anchor_w = _enrichment.get(_anchor_sys_id, {}).get('width_cm')
    for c in all_candidates:
        if text_q and text_q not in (c.shelfmark or '').lower() and text_q not in (c.title or '').lower():
            continue
        m = _enrichment.get(c.sys_id, {})
        if materials and m.get('material') not in materials:
            continue
        if has_dims and not (m.get('width_cm') and m.get('height_cm')):
            continue
        if exclude_mismatch and _is_size_mismatch(m.get('width_cm'), anchor_w):
            continue
        if triage_states and 'All' not in triage_states:
            verdict = _triage.get(c.sys_id)
            if 'Not triaged' in triage_states and verdict is None:
                pass   # passes
            elif verdict and verdict.capitalize() not in triage_states:
                continue
        out.append(c)
    return out
```

---

### `web/components/compare_modal.py` (NEW — full-screen Compare modal)

**Analog:** `web/components/text_editor.py` lines 112–163 (maximized dialog + header bar + close button)
**Analog:** `web/components/anchor_viewer.py` lines 190–310 (`AnchorViewer` two-pane reuse)
**Analog:** `desktop/join_workbench.py:3724` (Compare dialog), `:4051` (`_fill_anchor`), `:4086` (`_fill_candidate`), `:3741` (`step`), `:4202` (`_mark`)

**Imports pattern:**
```python
from __future__ import annotations

import asyncio
import logging
from typing import Optional, Callable

from nicegui import ui

from shared.joins_lab import Candidate, badge_and_tooltip
from web.components.anchor_viewer import AnchorViewer
from web.translations import tr

logger = logging.getLogger(__name__)
```

**Full-screen dialog pattern** (from `text_editor.py:112`):
```python
def create_compare_modal(
    anchor_cand: Candidate,
    initial_candidate: Candidate,
    filtered_candidates: list,
    triage: dict,
    on_verdict: Callable,
    on_close: Optional[Callable] = None,
) -> ui.dialog:
    """Full-screen Compare modal. anchor_cand|candidate pane with flip-through + verdict.

    Phase 119 D-01/D-02/D-03 parity with desktop join_workbench.py:3724.
    Both panes use fresh AnchorViewer instances (independent page state — Pitfall 3).
    """
    dialog = ui.dialog().props('maximized persistent')

    # Compare state — mutable dict so closures see current values
    _state = {
        'idx': _find_candidate_idx(initial_candidate, filtered_candidates),
        'candidates': filtered_candidates,
    }

    with dialog:
        with ui.card().classes('w-full h-full').style(
            'display:flex; flex-direction:column; overflow:hidden;'
        ):
            # ── Header bar ────────────────────────────────────────────────
            with ui.row().classes('w-full items-center justify-between px-4 py-2').style(
                'background: var(--bg-header); color: white; flex-shrink:0;'
            ):
                ui.label(tr('Compare')).classes('text-lg font-semibold')
                counter_label = ui.label('').classes('text-sm').style('color:rgba(255,255,255,0.8)')
                ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

            # ── Two-pane body ─────────────────────────────────────────────
            with ui.row().classes('w-full flex-grow min-h-0').style('overflow:hidden;'):
                # Anchor pane (left)
                with ui.column().classes('flex-1 gap-4 p-4 overflow-y-auto').style(
                    'border-right: 2px solid var(--border-light);'
                ):
                    ui.label(tr('Anchor')).classes('text-xs font-bold uppercase').style(
                        'color: var(--text-muted);'
                    )
                    anchor_shelfmark_label = ui.label(anchor_cand.shelfmark or '?').classes(
                        'text-sm font-semibold'
                    ).style('color: var(--primary-700);')
                    # Fresh AnchorViewer for the anchor pane — do NOT reuse the sticky pane viewer
                    anchor_viewer = AnchorViewer(
                        sys_id=anchor_cand.sys_id,
                        p_num=anchor_cand.page,
                        volume_ie=anchor_cand.volume_ie,
                    )

                # Candidate pane (right)
                with ui.column().classes('flex-1 gap-4 p-4 overflow-y-auto'):
                    ui.label(tr('Candidate')).classes('text-xs font-bold uppercase').style(
                        'color: var(--text-muted);'
                    )
                    cand_shelfmark_label = ui.label('').classes('text-sm font-semibold').style(
                        'color: var(--primary-700);'
                    )
                    cand_badge_row = ui.row().classes('gap-1 items-center')
                    cand_viewer_container = ui.column().classes('w-full')
                    # cand_viewer populated by _fill_candidate()

            # ── Verdict bar (sticky bottom) ────────────────────────────────
            with ui.row().classes(
                'w-full items-center justify-between px-4 py-2 flex-wrap gap-2'
            ).style(
                'background: var(--bg-tertiary); position:sticky; bottom:0; flex-shrink:0;'
            ):
                prev_btn = ui.button(tr('‹ Prev'), icon='chevron_left').props('flat dense')
                with ui.row().classes('gap-2 items-center'):
                    for verdict, label, color in [
                        ('yes', tr('Yes'), 'positive'),
                        ('maybe', tr('Maybe'), 'warning'),
                        ('no', tr('No'), 'negative'),
                    ]:
                        _v = verdict
                        ui.button(label).props(f'color={color} unelevated size=md').on(
                            'click', lambda v=_v: _record_verdict(v)
                        )
                next_btn = ui.button(tr('Next ›'), icon='chevron_right').props('flat dense').style(
                    'flex-direction:row-reverse;'
                )
```

**Flip-through pattern (D-02, parity `step(delta)` at `:3741/3753`):**
```python
    def _step(delta: int) -> None:
        """Advance/retreat through filtered_candidates (parity desktop step(delta))."""
        cands = _state['candidates']
        if not cands:
            return
        _state['idx'] = (_state['idx'] + delta) % len(cands)
        _fill_candidate(cands[_state['idx']])
        _update_counter()
        _update_nav_buttons()

    def _record_verdict(verdict: str) -> None:
        """Record verdict and AUTO-ADVANCE (D-03, parity _mark → wb.mark → triage)."""
        cand = _state['candidates'][_state['idx']]
        on_verdict(cand.sys_id, verdict)   # updates _triage dict in the parent closure
        _step(1)   # auto-advance to next candidate

    def _fill_candidate(cand: Candidate) -> None:
        """Populate the candidate pane (parity _fill_candidate:4086)."""
        cand_shelfmark_label.set_text(cand.shelfmark or '?')
        # 👁 badge via badge_and_tooltip() (D-07)
        cand_badge_row.clear()
        with cand_badge_row:
            icon_name, tooltip_text = badge_and_tooltip(cand)
            if icon_name:
                ui.icon(icon_name).style('color:#f59e0b;').tooltip(tooltip_text)
            # Size-mismatch warning badge
            m = _enrichment.get(cand.sys_id, {}) if _enrichment else {}
            anchor_w = _enrichment.get(anchor_cand.sys_id, {}).get('width_cm') if _enrichment else None
            if _is_size_mismatch(m.get('width_cm'), anchor_w):
                ui.badge(tr('Size mismatch'), icon='warning').props('color=warning')
        # Fresh AnchorViewer for candidate — independent from anchor pane (Pitfall 3)
        cand_viewer_container.clear()
        with cand_viewer_container:
            _ = AnchorViewer(
                sys_id=cand.sys_id,
                p_num=cand.page,
                volume_ie=cand.volume_ie,
            )

    prev_btn.on('click', lambda: _step(-1))
    next_btn.on('click', lambda: _step(1))
    _fill_candidate(initial_candidate)
    _update_counter()
    _update_nav_buttons()

    return dialog
```

**AnchorViewer idempotency note (Pitfall 3):**
`inject_viewer_assets()` is called ONCE in `create_joins_lab_page()`. The `window._msViewerLoaded`
guard in `_VIEWER_HEAD` (`anchor_viewer.py:59`) means a second (and third) call is a no-op.
**Do NOT call `inject_viewer_assets()` inside `create_compare_modal`** — it runs on user click,
not at page-build time. The guard only prevents double-init; it does NOT retroactively execute
scripts that were injected into an already-live SPA page.

---

### `web/pages/joins_lab.py` (extend — VS toggle state + enrichment dispatch + pipeline extension)

**Primary analog:** `web/pages/joins_lab.py` itself (the Phase 117/118 search pipeline)
**Secondary analog:** `web/components/visual_similarity_dialog.py:160-176` (VS service `run.io_bound` pattern)

**Additional page-level state (closure variables after existing `_search_generation`, `_is_running` etc.):**
```python
# Phase 119 additions to page-level transient state
_triage: dict = {}                   # sys_id → 'yes'|'maybe'|'no' (D-11)
_selected: set = set()               # table multi-select (D-12)
_filter_state: dict = {              # filter dialog state (D-14)
    'materials': [],
    'has_dims': False,
    'exclude_mismatch': False,
    'triage_states': [],
    'text_q': '',
}
_enrichment: dict = {}               # sys_id → {width_cm, height_cm, material, ...} (D-16)
_enrichment_ready: dict = {'value': False}
_vs_candidates: list = []            # last fetched VS Candidate list (D-05)
_vs_on: dict = {'value': False}      # 👁 toggle state (D-04)
_vs_anchor_sid: dict = {'value': None}  # anchor sid the VS candidates were fetched for (D-06)
_current_page: dict = {'value': 0}   # pagination (D-08)
_view_mode: dict = {'value': 'grid'} # 'grid' | 'table' (D-09/D-10)
_all_candidates: list = []           # full deduped+merged candidate list (before filter)
_filtered_candidates: list = []      # after apply_filters() (before pagination)
```
**Never write any of these to `safe_storage`.** All are in-memory for Phase 119.

**VS lookup adapter pattern (D-05, mirrors `visual_similarity_dialog.py:176`):**
```python
async def _fetch_vs_candidates(anchor_sid: str) -> list:
    """Fetch VS look-alikes off the event loop (D-05).

    Mirrors visual_similarity_dialog.py:176 run.io_bound pattern.
    LOCAL visual_similarity.db SQLite read — no NLI circuit breaker needed.
    """
    from shared.visual_similarity_service import get_vs_service
    from shared.joins_lab import Candidate

    def run_vs_core():  # sync closure — passed to run.io_bound (CI guard)
        vs_svc = get_vs_service(thread_safe=True)
        if not vs_svc.is_available():
            return []
        return vs_svc.get_suggestions(anchor_sid, 200)

    try:
        raw = await run.io_bound(run_vs_core)
    except Exception:
        raw = []

    # Field mapping: alma_id→sys_id, svm_score→vs_score, rank→vs_rank (Pitfall 4 — do NOT swap)
    return [
        Candidate(
            sys_id=r['alma_id'],
            page=None,
            uid=f"{r['alma_id']}|vs",
            via_vs=True,
            vs_rank=r['rank'],
            vs_score=r['svm_score'],   # NOT swapped with rank (Pitfall 4)
        )
        for r in raw
    ]
```

**Enrichment batch pattern (D-16, mirrors desktop `_EnrichWorker:1671`):**
```python
async def _enrich_candidates(sys_ids: list) -> dict:
    """Batch enrichment off the event loop (D-16).

    LOCAL fjms_enrichment.db SQLite read — no NLI circuit breaker needed.
    """
    if not sys_ids:
        return {}

    def run_enrich_core():  # sync closure — passed to run.io_bound (CI guard)
        from shared.fjms_service import get_fjms_service
        fjms = get_fjms_service(thread_safe=True)
        if not fjms.is_available():
            return {}
        return fjms.get_measurement_summaries_batch(sys_ids)
        # Returns {sys_id: {'width_cm': ..., 'height_cm': ..., 'material': ..., ...}}

    try:
        return await run.io_bound(run_enrich_core)
    except Exception:
        return {}
```

**VS conditional merge (D-04, parity desktop `:2788-2802`):**
```python
def _apply_vs_merge(
    text_candidates: list,
    vs_candidates: list,
    vs_on: bool,
    builder_has_query: bool,
) -> list:
    """Compute display candidates from text+VS lists using the conditional model.

    D-04 / desktop parity join_workbench.py:2788-2802.
    Pure function — no I/O (testable headlessly).
    """
    from shared.joins_lab import merge_candidates

    if vs_on and not builder_has_query:
        # UNION: pure VS browse
        return merge_candidates([], vs_candidates)
    elif vs_on and builder_has_query:
        # INTERSECTION: keep only tier0 (both via_text AND via_vs)
        merged = merge_candidates(text_candidates, vs_candidates)
        return [c for c in merged if c.via_text and c.via_vs]
    else:
        # OFF: text-only; tier0+tier1 (keep via_text), drop VS-only (tier2)
        merged = merge_candidates(text_candidates, vs_candidates)
        return [c for c in merged if c.via_text]
```

**Extension to `execute_joins_search` pipeline (Step 9 replacement, D-08/D-04):**

The existing pipeline in `execute_joins_search` at lines 1151-1245 ends with:
```python
# Step 8: Dedup anchor results
base_candidates, anchor_matched = dedup_candidates(raw_results, anchor_sid)
# ... cross-side block ...
# Step 9: Render final candidates
candidates_container.clear()
with candidates_container:
    if final_candidates:
        create_candidate_grid(final_candidates)
```

Phase 119 replaces Step 9 with:
```python
# Step 9 (Phase 119): Store + VS merge + enrich + render
_all_candidates = final_candidates
_triage.clear()   # reset triage on every new search (D-11: resets on re-anchor/re-search)

# VS merge (if toggle already ON from a prior search for the same anchor)
if _vs_on['value'] and _vs_anchor_sid['value'] == anchor_sid:
    display_candidates = _apply_vs_merge(
        _all_candidates, _vs_candidates,
        vs_on=True, builder_has_query=True,
    )
else:
    display_candidates = list(_all_candidates)

_filtered_candidates = _compute_filtered(display_candidates)
_current_page['value'] = 0

# Fire enrichment off-loop (D-16) — batch covers full filtered set
asyncio.ensure_future(_do_enrich_and_update(_filtered_candidates))

candidates_container.clear()
with candidates_container:
    _render_candidate_surface()
```

**Re-anchor invalidation of triage + VS state (D-06/D-11):**
```python
# Inside load_anchor(), after setting _anchor_state:
_triage.clear()          # D-11: triage resets on re-anchor
_all_candidates = []
_filtered_candidates = []
_current_page['value'] = 0
# If VS toggle ON: re-fetch for new anchor (D-06)
if _vs_on['value']:
    _vs_candidates = []
    _vs_anchor_sid['value'] = None
    asyncio.ensure_future(_do_vs_fetch_and_update(sys_id))
```

---

### `shared/joins_lab.py` (Wave 0 addition — `badge_and_tooltip`)

**Analog:** `desktop/join_workbench.py:452-457` (the parity source)

This function does NOT yet exist in `shared/joins_lab.py` (confirmed by grep). It must be added in Wave 0 before any component that renders badges.

**Function to add after `detect_self_match` at line 624:**
```python
def badge_and_tooltip(cand: "Candidate") -> tuple:
    """Return (icon_name | None, tooltip_text) for a candidate badge.

    Precedence (desktop parity join_workbench.py:452-457):
      ⚓ is_anchor_self  → ('anchor', 'Anchor fragment')
      ⇄ via_other_side  → ('swap_horiz', 'Found via other side')
      👁 via_vs          → ('visibility', 'Visually similar')
      (none)            → (None, '')

    Pure function — no I/O.
    """
    if cand.is_anchor_self:
        return ("anchor", "Anchor fragment")
    if cand.via_other_side:
        return ("swap_horiz", "Found via other side")
    if cand.via_vs:
        return ("visibility", "Visually similar")
    return (None, "")
```

**Test to add to `tests/test_joins_lab.py`:**
```python
def test_badge_and_tooltip_precedence():
    from shared.joins_lab import badge_and_tooltip, Candidate

    base = Candidate(sys_id='123', page=1, via_vs=True, via_other_side=True, is_anchor_self=True)
    # ⚓ wins over ⇄ and 👁
    icon, tip = badge_and_tooltip(base)
    assert icon == 'anchor'

    # ⇄ wins over 👁 when not anchor_self
    c2 = Candidate(sys_id='123', page=1, via_vs=True, via_other_side=True)
    icon, tip = badge_and_tooltip(c2)
    assert icon == 'swap_horiz'

    # 👁 only
    c3 = Candidate(sys_id='123', page=1, via_vs=True)
    icon, tip = badge_and_tooltip(c3)
    assert icon == 'visibility'

    # No badge
    c4 = Candidate(sys_id='123', page=1)
    icon, tip = badge_and_tooltip(c4)
    assert icon is None
    assert tip == ''
```

---

### `tests/test_joins_lab_off_loop.py` (extend — cover VS + enrichment call sites)

**Analog:** the existing test itself (lines 1–331) — same AST-guard pattern, extended to new call sites.

The existing guard checks only `execute_search` attribute calls. Phase 119 must extend it to also
guard `get_suggestions` and `get_measurement_summaries_batch` calls in `joins_lab.py`.

**Pattern to replicate from existing guard (lines 96–172):**
```python
# The existing _find_execute_search_violations pattern:
# 1. ast.parse(source)
# 2. Walk all Call nodes for Attribute calls with attr == 'execute_search'
# 3. For each, find enclosing FunctionDef/AsyncFunctionDef
# 4. FAIL if enclosing is AsyncFunctionDef (V1)
# 5. FAIL if enclosing sync def name not in run.io_bound first-arg set (V2)

# Phase 119 MUST add the same guard for:
def _find_blocking_call_violations(source: str, blocking_attrs: list[str]) -> list:
    """Generic version of the existing detector for any list of blocking method names."""
    # Same logic as _find_execute_search_violations but parameterized on method name
    # blocking_attrs: e.g. ['get_suggestions', 'get_measurement_summaries_batch']
    ...

def test_vs_lookup_not_on_event_loop():
    """Phase 119: assert joins_lab.py never calls get_suggestions on the event loop."""
    source = JOINS_LAB_PATH.read_text(encoding='utf-8')
    violations = _find_blocking_call_violations(source, ['get_suggestions'])
    assert not violations, ...

def test_enrichment_batch_not_on_event_loop():
    """Phase 119: assert joins_lab.py never calls get_measurement_summaries_batch on event loop."""
    source = JOINS_LAB_PATH.read_text(encoding='utf-8')
    violations = _find_blocking_call_violations(source, ['get_measurement_summaries_batch'])
    assert not violations, ...
```

---

## Shared Patterns

### Off-Loop Discipline (applies to ALL new I/O in Phase 119)

**Source:** `web/pages/joins_lab.py:1100-1112` (`run_search_core` + `run.io_bound` pattern)
**Also:** `web/components/visual_similarity_dialog.py:176` (VS `run.io_bound`)
**Apply to:** VS lookup, enrichment batch, any new SQLite calls in `joins_lab.py`

```python
# THE PATTERN — always: sync closure → run.io_bound → await
def run_<name>_core():           # literal name matters for CI guard
    return <blocking_call>(...)  # only blocking call; no async, no UI

result = await run.io_bound(run_<name>_core)
```

**Must NOT call `get_suggestions` or `get_measurement_summaries_batch` directly in async context:**
```python
# WRONG — blocks event loop
async def bad():
    data = vs_svc.get_suggestions(sid, 200)   # ← blocks

# CORRECT — off-loop
async def good():
    def run_vs_core():
        return vs_svc.get_suggestions(sid, 200)
    data = await run.io_bound(run_vs_core)
```

### Multitenant Invariant — Zero Raw Storage Access

**Source:** `web/safe_storage.py` + `tests/test_no_raw_storage_access.py`
**Apply to:** ALL new code in `web/` files
**Rule:** Never access `app.storage.user` directly. Phase 119 triage/filter/VS state is in-memory
(page-level Python dicts), NOT `safe_storage`. Phase 120 will add persistence.

```python
# WRONG — Phase-87 violation; caught by CI
app.storage.user['triage'] = _triage

# CORRECT — in-memory page state (Phase 119)
_triage: dict = {}   # closure variable, never written to storage
```

### Bilingual Strings via `tr()`

**Source:** `web/translations.py:31-44`
**Apply to:** ALL new UI strings in Phase 119

```python
from web.translations import tr, get_language

# Every visible string is wrapped:
ui.label(tr('Visual Similarity'))
ui.button(tr('Compare'), ...)
ui.label(tr('Filters'))

# Hebrew keys defined in genizah_translations.TRANSLATIONS
# If a key is missing, tr() returns the English string (graceful fallback)
```

### Image URL — Proxy-Only Rule

**Source:** `web/components/candidate_grid.py:63-106` (`build_thumbnail_url`)
**Apply to:** ALL image URLs in grid cards, table rows, Compare modal

All thumbnail and preview image URLs MUST go through existing proxy endpoints:
- `/api/nli_image_by_sysid/{sys_id}?page={N}&width=300` — NLI + CUL
- `/api/oxford_image/{sys_id}?page={N}` — Bodleian
- Never a direct `iiif.nli.org.il` URL

Reuse `build_thumbnail_url(sys_id, page, shelfmark, library_code)` from `candidate_grid.py`.
The Phase-98 NLI circuit breaker is wired server-side in the proxy endpoints — the UI does
NOT need to know about the breaker; it just uses the proxy URL.

### `inject_viewer_assets()` — Once Per Page, Not Per Dialog

**Source:** `web/components/anchor_viewer.py:171-183`
**Apply to:** `create_joins_lab_page()` — the single call site

```python
# CORRECT — call at page-build time in create_joins_lab_page()
inject_viewer_assets()   # at the top of create_joins_lab_page()

# The Compare modal constructs two AnchorViewer instances dynamically (user click)
# This is fine ONLY because inject_viewer_assets() was called at page-build time.
# Do NOT call inject_viewer_assets() inside create_compare_modal().
```

### Dialog Pattern — `ui.dialog` maximized

**Source:** `web/components/text_editor.py:112-163`

```python
dialog = ui.dialog().props('maximized persistent')

with dialog:
    with ui.card().classes('w-full h-full').style(
        'display:flex; flex-direction:column; overflow:hidden;'
    ):
        # Header bar
        with ui.row().classes('w-full items-center justify-between px-4 py-2').style(
            'background: var(--bg-header); color: white; flex-shrink:0;'
        ):
            ui.label(title).classes('text-lg font-semibold')
            ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        # Scrollable body
        ...

dialog.open()
```

### `stop_propagation` — Client-Side Only (existing AST guard)

**Source:** `web/components/visual_similarity_dialog.py:576` + `tests/test_no_server_side_stop_propagation.py`
**Apply to:** Any nested link inside Compare modal or candidate grid

```python
# WRONG — server-side stop_propagation does not exist on GenericEventArguments
row_el.on('click', lambda e: e.stop_propagation())   # ← crashes

# CORRECT — client-side js_handler
ui.link(shelfmark, browse_url).on('click', js_handler='(e) => e.stopPropagation()')
```

---

## No Analog Found

No files are in this category. All Phase 119 files either extend existing ones or have strong
desktop analogs and web pattern matches.

---

## Metadata

**Analog search scope:** `web/components/`, `web/pages/`, `shared/`, `desktop/`, `tests/`
**Files scanned:** 9 (candidate_grid.py, joins_lab.py, visual_similarity_dialog.py, anchor_viewer.py, joins_lab.py shared, text_editor.py, join_workbench.py desktop, test_joins_lab_off_loop.py, test_no_raw_storage_access.py)
**Pattern extraction date:** 2026-06-19

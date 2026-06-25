# Phase 117: Vertical Spine - Pattern Map

**Mapped:** 2026-06-17
**Files analyzed:** 7 new/modified files
**Analogs found:** 7 / 7

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/pages/joins_lab.py` | page | request-response + event-driven | `web/pages/browse.py` + `web/pages/search.py` | role-match |
| `web/joins_executor.py` | adapter/service | request-response | `desktop/join_workbench.py:1473` (`_DesktopSearchExecutor`) | exact |
| `web/components/anchor_viewer.py` | component | request-response | `web/pages/browse.py:532–621, 1388–1418, 3600–3722` | extract |
| `web/components/candidate_grid.py` | component | CRUD (read) | `web/pages/search_results.py:350–429` | role-match |
| `web/main.py` | config/route | request-response | `web/main.py:1689–1799` (browse_page_route) | exact |
| `tests/test_web_search_executor.py` | test | — | `tests/test_joins_lab.py` (FakeSearchExecutor at :57) | role-match |
| `tests/test_joins_lab_storage.py` | test | — | `tests/test_no_raw_storage_access.py` (AST pattern) | role-match |

---

## Pattern Assignments

### `web/pages/joins_lab.py` (page, request-response + event-driven)

**Analogs:** `web/pages/search.py` (off-loop search pattern) + `web/pages/browse.py` (image viewer, shelfmark resolve, safe_storage)

**Imports pattern** — copy from `web/pages/search.py:1–20` and `web/pages/browse.py:1–22`:
```python
# -*- coding: utf-8 -*-
import logging
from nicegui import ui, run
from web.safe_storage import safe_user_get, safe_user_set
from web.state import state
from web.translations import tr, get_language, is_rtl
```

**Page-level transient state** — copy mutable-container pattern from `web/pages/search.py:3979–3983` (NOT safe_storage — these are ephemeral per-render closures):
```python
# Source: web/pages/search.py:3979 guard pattern
_search_generation = {'value': 0}
_is_running = {'value': False}
_anchor_state = {'sys_id': None, 'fl_id': None, 'volume_ie': None}
```

**safe_storage read/write** — copy key-name + schema-version invalidation from the pattern below:
```python
# Source: web/safe_storage.py safe_user_get/safe_user_set (verified full file)
from web.safe_storage import safe_user_get, safe_user_set

_JOINS_LAB_KEY = 'joins_lab'
_SCHEMA_VERSION = 1

def _read_joins_lab_state() -> dict | None:
    data = safe_user_get(_JOINS_LAB_KEY, default=None)
    if not isinstance(data, dict):
        return None
    if data.get('schema_version') != _SCHEMA_VERSION:
        return None          # discard stale schema; treat as cold start
    return data

def _write_joins_lab_state(anchor_sys_id: str, anchor_fl_id=None, anchor_volume_ie=None):
    safe_user_set(_JOINS_LAB_KEY, {
        'schema_version': _SCHEMA_VERSION,
        'anchor_sys_id': anchor_sys_id,
        'anchor_fl_id': anchor_fl_id,
        'anchor_volume_ie': anchor_volume_ie,
    })
```

**Shelfmark/sys_id resolution** — copy directly from `web/pages/browse.py:714–762` (`search_shelfmark` pattern):
```python
# Source: web/pages/browse.py:728-742
async def resolve_anchor_input(query: str) -> str | None:
    query = query.strip()
    if not query:
        return None
    # sys_id fast path (browse.py:729-738 pattern)
    if query.isdigit() and query.startswith('99'):
        return query
    # Shelfmark resolution — always off-loop (I/O-bound SQLite)
    results, exact_match = await run.io_bound(
        lambda: service.search_by_shelfmark(query, limit=20)
    )
    if results:
        return results[0].sys_id
    return None
```

**Off-loop search with latest-wins guard (FND-01 / D-16)** — copy the `run_core_search` + `search_generation` + `is_running` triple from `web/pages/search.py:3979–4189`. The complete validated template is:
```python
# Source: web/pages/search.py:3979-4189 (execute_search + run_core_search)

async def execute_joins_search():
    # Re-entrancy guard — mirrors search.py:3981
    if _is_running['value']:
        return

    lines = [l.strip() for l in builder_textarea.value.splitlines() if l.strip()]
    if not lines:
        return

    if not state.is_ready():
        ui.notify(tr("Engine not ready."), type='warning')
        return

    _is_running['value'] = True
    _search_generation['value'] += 1       # mirrors search.py:4036
    my_gen = _search_generation['value']

    run_btn.props('loading=true disabled=true')
    candidates_container.clear()

    try:
        from shared.joins_lab import BuilderRow, SideQuery, compose, dedup_candidates
        rows = tuple(BuilderRow(term=line) for line in lines)
        side_query = SideQuery(rows=rows, variants=False, page_position=None)
        query_str, responsa_options, page_position = compose(side_query)
        if not query_str:
            return

        def run_search_core():
            """Runs in io_bound thread — must not touch NiceGUI UI tree."""
            try:
                return state.searcher.execute_search(
                    query_str,
                    mode='exact',
                    gap=0,
                    progress_callback=_make_progress_cb(),
                    responsa_options=responsa_options,
                    text_position=page_position,
                    corpus_scope='genizah',
                ) or []
            except Exception:
                return []

        raw_results = await run.io_bound(run_search_core)   # mirrors search.py:4189

        # Stale-generation check — mirrors search.py:4036 invalidation logic
        if my_gen != _search_generation['value']:
            return

        anchor_sid = _anchor_state['sys_id'] or ''
        candidates, _ = dedup_candidates(raw_results, anchor_sid)
        _render_candidates(candidates)

    finally:
        _is_running['value'] = False
        run_btn.props(remove='loading disabled')
```

**Progress callback dual-protocol guard** — copy from `web/pages/parallels.py:2140–2154` (production fix 2026-06-12):
```python
# Source: web/pages/parallels.py:2140-2154
def _make_progress_cb():
    def progress_cb(arg1, arg2=None):
        if isinstance(arg1, str):
            # Text status call from _execute_batched_search — ignore content.
            # Two-arg-only signature raises TypeError on this branch (prod bug 2026-06-12).
            return
        current, total = arg1, arg2
        # update progress UI if desired
    return progress_cb
```

**RTL numbered transcription** — import directly from `web/pages/browse.py:41` (no extraction needed in Phase 117; Phase 119 may promote it):
```python
# Source: web/pages/browse.py:41 (_render_line_numbered_html — pure function)
from web.pages.browse import _render_line_numbered_html

html_content = _render_line_numbered_html(
    text=page_data.get('text', ''),
    highlight_html=None,
    line_height="2.2",
    font_size="1.4rem",
    show_line_numbers=True,
)
transcription_element.content = html_content
```

**Direction-aware layout (D-01/D-02)** — copy RTL convention from `web/main.py:1127–1153` (`rtl_mode` + `flex-row-reverse`):
```python
# Source: web/main.py (create_layout rtl_mode pattern)
from web.translations import is_rtl
direction_class = 'flex-row-reverse' if is_rtl() else 'flex-row'
with ui.row().classes(f'w-full {direction_class}'):
    # anchor pane (left in EN, right in HE — direction-aware, not hardcoded)
    ...
```

---

### `web/joins_executor.py` (adapter, request-response)

**Analog:** `desktop/join_workbench.py:1473–1538` (`_DesktopSearchExecutor`)

This file is the riskiest new seam — an exact mirror of the desktop executor, adapted for web state. BLOCKER 1 from `v8.2.0-REQ-CODEX-CRITIQUE.md`: must wrap `state.searcher` directly, NOT `/api/search`.

**Imports pattern** (lines 1–5 of the new file):
```python
# Source: desktop/join_workbench.py:1469 + web/state.py:1
from shared.joins_lab import SearchExecutor
from web.state import state
```

**Core adapter pattern** — copy `_DesktopSearchExecutor:1473–1538` verbatim, replacing `self._searcher` → `state.searcher` and `self._meta_mgr` → `state.meta_mgr` (since there is no `__init__` constructor in the web singleton variant):
```python
# Source: desktop/join_workbench.py:1473-1538 (_DesktopSearchExecutor)

class WebSearchExecutor:
    """Satisfies shared/joins_lab.py SearchExecutor Protocol for the web.

    Thin passthrough — wraps state.searcher + state.meta_mgr.
    Returns [] / None / ('','') / '' on any failure (mirrors _DesktopSearchExecutor:1510).

    IMPORTANT: All execute_search calls MUST be made inside run.io_bound().
    This class is synchronous; calling it on the event loop blocks all sessions.
    """

    def execute_search(
        self,
        query_str: str,
        mode: str,
        gap: int,
        progress_callback=None,
        exclude_words=None,
        responsa_options: dict | None = None,
        restrict_sys_ids: set | None = None,
        text_position: str | None = None,
        corpus_scope: str = "all",
    ) -> list[dict]:
        # Source: desktop/join_workbench.py:1497-1511
        try:
            return state.searcher.execute_search(
                query_str,
                mode,
                gap,
                progress_callback=progress_callback,
                exclude_words=exclude_words,
                responsa_options=responsa_options,
                restrict_sys_ids=restrict_sys_ids,
                text_position=text_position,
                corpus_scope=corpus_scope,
            ) or []
        except Exception:
            return []

    def get_browse_page(
        self,
        sys_id: str,
        p_num: int | None = None,
        next_prev: int = 0,
        absolute_index: int | None = None,
        allow_cross: bool = False,
        volume_ie: str | None = None,
    ) -> dict | None:
        # Source: desktop/join_workbench.py:1513-1530
        try:
            return state.searcher.get_browse_page(
                sys_id,
                p_num=p_num,
                next_prev=next_prev,
                absolute_index=absolute_index,
                allow_cross=allow_cross,
                volume_ie=volume_ie,
            )
        except Exception:
            return None

    def get_meta_for_id(self, sys_id: str) -> tuple[str, str]:
        # Source: desktop/join_workbench.py:1532-1534
        try:
            return state.meta_mgr.get_meta_for_id(sys_id)
        except Exception:
            return ('', '')

    def get_library_for_id(self, sys_id: str) -> str:
        # Source: desktop/join_workbench.py:1536-1538
        try:
            return state.meta_mgr.get_library_for_id(sys_id) or ''
        except Exception:
            return ''
```

**Protocol compliance check** — `WebSearchExecutor` must satisfy `isinstance(executor, SearchExecutor)` because `SearchExecutor` is `@runtime_checkable` (`shared/joins_lab.py:149`). All four method signatures must match `:162–192` exactly.

**Wave 0 verification needed:** Before finalizing, grep `genizah_core.py` for `def get_browse_page`, `def get_meta_for_id`, `def get_library_for_id` to confirm which object (`searcher` vs `meta_mgr`) owns each method. Research doc `117-RESEARCH.md` marks these as A1–A3 LOW confidence.

---

### `web/components/anchor_viewer.py` (component, request-response)

**Analog:** `web/pages/browse.py` — three non-overlapping sections:
- CSS + JS head block: `browse.py:532–621`
- Zoom/pan functions: `browse.py:1388–1418`
- Per-provider proxy URL resolution: `browse.py:3600–3722`

**Head HTML injection pattern** — extract the `<style>` + `<script>` block from `browse.py:532–621`. The block ends at line 621. In the component, call `ui.add_head_html()` from within `__init__` or the constructor function. Add a JS-side idempotency guard to support Phase 119 (two viewers per Compare page):
```python
# Source: browse.py:610-621 (manuscriptViewer JS initialization)
_VIEWER_HEAD = '''
<style>
    /* ... CSS from browse.py:400-609 ... */
    .zoomable-image {
        transform-origin: center center;
        transition: transform 0.1s ease-out;
        will-change: transform;
        max-width: 100%;
        max-height: 100%;
        object-fit: contain;
        user-select: none;
    }
</style>
<script>
    if (!window._msViewerLoaded) {
        window._msViewerLoaded = true;
        window.manuscriptViewer = createManuscriptViewer({
            imageSelector: '.zoomable-image',
            containerSelector: '.image-container',
            zoomLabelSelector: '.zoom-level-label',
            gammaFilterId: 'gamma-main'
        });
    }
</script>
'''
# Call in component constructor:
ui.add_head_html(_VIEWER_HEAD)
```
Note: The idempotency guard (`window._msViewerLoaded`) prevents double-injection when Phase 119 instantiates two `AnchorViewer` components on the Compare page.

**Zoom controls pattern** — copy from `browse.py:1388–1418`:
```python
# Source: browse.py:1388-1418 (zoom_in / zoom_out / zoom_reset)
def zoom_in():
    state.zoom_level = min(state.zoom_level + 0.25, 4.0)
    update_image_transform()

def zoom_out():
    state.zoom_level = max(state.zoom_level - 0.25, 0.25)
    update_image_transform()

def zoom_reset():
    state.zoom_level = 1.0
    state.rotation = 0
    ui.run_javascript('''
        if(window.manuscriptViewer) window.manuscriptViewer.reset();
    ''')
    update_image_transform()
```
The anchor viewer needs only `zoom_in`, `zoom_out`, `zoom_reset` — not `rotate_left/right`, `fit_width`, `fit_height` (those are browse extras).

**Per-provider proxy URL resolution** — copy the conditional block from `browse.py:3612–3722`. For the anchor viewer, only the READ path matters (no active_source user toggle needed in Phase 117 — use first available). The critical lines establishing each provider's URL format are:
```python
# Source: browse.py:3625, 3645, 3698, 3714, 3720 — the proxy URL patterns per provider

# NLI (default):
img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"

# Oxford:
img_url = f"/api/oxford_image/{sys_id}?page={page_idx}"

# Cambridge:
img_url = f"/api/cambridge_image/{sys_id}?page={page_idx}"

# Manchester:
img_url = f"/api/manchester_image/{sys_id}?page={page_idx}"

# JTS:
img_url = f"/api/jts_image/{sys_id}?page={page_idx}"
```
Copy the full provider-selection logic (`is_oxford`, `is_synthetic_sys_id`, `active_source`, `cambridge_images`, etc.) from `browse.py:3600–3722`. Do NOT simplify — the Oxford and Cambridge special-casing is required for ANC-02 parity.

**Key pitfall:** `browse.py`'s `update_content()` is ~400 lines including corrections, metadata panels, enrichment workers, nav state, and source toggles. Extract ONLY: image `<img>` tag update, folio nav (prev/next `p_num`), and transcription update. Discard all enrichment, community, and metadata-panel logic.

---

### `web/components/candidate_grid.py` (component, CRUD read)

**Analog:** `web/pages/search_results.py:350–429` (`create_result_card`)

The existing `create_result_card` is too feature-heavy (checkboxes, PGP badges, domain badges, VS expansion). Phase 117 needs a thin read-only card. Use the library-chip pattern from `search_results.py:390–395` and the card shell from `:361–363`:

**Library badge pattern** (lines 390–395):
```python
# Source: web/pages/search_results.py:390-395
if library_code:
    from genizah_core import get_library_display
    full_name = get_library_display(library_code, short=False, lang=get_language())
    ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
        'background: var(--primary-100); color: var(--primary-700);'
    ).tooltip(full_name)
```

**Card shell** (lines 361–363):
```python
# Source: web/pages/search_results.py:361-363
with ui.card().classes(
    'w-full p-4 cursor-pointer transition-all hover:shadow-md'
).style('border-radius: 10px;') as card:
    ...
```

**New thin card for Phase 117** — fewer features than `create_result_card`, but same CSS variable tokens and library-badge pattern:
```python
# web/components/candidate_grid.py (new)
def _create_candidate_card(cand):
    """Read-only candidate card. Phase 119 adds triage Y/?/N."""
    shelfmark = cand.shelfmark or '?'
    library_code = cand.library_code or ''
    title = cand.title or ''
    sys_id = cand.sys_id
    page = cand.page

    with ui.card().classes('w-full p-4').style(
        'border-radius: 8px; border: 1px solid var(--border-light);'
    ):
        with ui.row().classes('items-start gap-2 w-full flex-wrap'):
            if library_code:
                from genizah_core import get_library_display
                full_name = get_library_display(library_code, short=False, lang=get_language())
                ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                    'background: var(--primary-100); color: var(--primary-700);'
                ).tooltip(full_name)
            ui.label(shelfmark).classes('text-sm font-semibold min-w-0 truncate flex-1')
        if title:
            ui.label(title[:80] + ('...' if len(title) > 80 else '')).classes('text-xs').style(
                'color: var(--text-secondary); direction: rtl;'
            )
        browse_url = f'/browse?sys_id={sys_id}' + (f'&page={page}' if page else '')
        ui.link(tr('View in Browse'), browse_url).classes('text-xs mt-1').style(
            'color: var(--primary-700);'
        )
```

---

### `web/main.py` — route + sidebar nav (modification)

**Analog:** `web/main.py:1689–1799` (browse_page_route, existing `nav_items` list at `:1133–1144`)

**Route registration pattern** — copy `browse_page_route` shape (lines 1689–1691):
```python
# Source: web/main.py:1689-1691 (browse_page_route pattern)
@ui.page('/joins-lab', title='Joins Lab | Dicta Genizah Search')
def joins_lab_page_route(
    sys_id: str = None,
    shelfmark: str = None,
    fl_id: str = None,
    page: int = None,
    volume_ie: str = None,
):
    safe_user_set('current_page', '/joins-lab')
    content = create_layout()
    with content:
        from web.pages.joins_lab import create_joins_lab_page
        create_joins_lab_page(
            initial_sys_id=sys_id,
            initial_shelfmark=shelfmark,
            initial_fl_id=fl_id,
            initial_page=page,
            initial_volume_ie=volume_ie,
        )
```

**Sidebar nav entry** — insert into the `nav_items` list at `web/main.py:1133–1144`, following the `/puzzle` pattern (conditionally gated by env var if desired, or unconditional):
```python
# Source: web/main.py:1143-1144 (WEB_PUZZLE_ENABLED gate pattern)
nav_items.append(('/joins-lab', 'join_inner', tr('Joins Lab'), None))
# Or: if WEB_JOINS_LAB_ENABLED: nav_items.append(...)
```
Icon `'join_inner'` is a Material Icons symbol. Confirm it renders before shipping; fallback `'link'`.

---

### `tests/test_web_search_executor.py` (test)

**Analog:** `tests/test_joins_lab.py:57` (existing `FakeSearchExecutor` test double + Protocol tests)

**Protocol compliance test pattern:**
```python
# Source: tests/test_joins_lab.py (FakeSearchExecutor + isinstance check pattern)
from shared.joins_lab import SearchExecutor
from web.joins_executor import WebSearchExecutor

def test_web_executor_satisfies_protocol():
    """WebSearchExecutor must be runtime-checkable as SearchExecutor."""
    executor = WebSearchExecutor()
    assert isinstance(executor, SearchExecutor)
```

**Failure-return test pattern:**
```python
def test_execute_search_returns_empty_list_on_failure(monkeypatch):
    from web.joins_executor import WebSearchExecutor
    from web import state as _state_mod

    class _BrokenSearcher:
        def execute_search(self, *args, **kwargs):
            raise RuntimeError("engine down")

    monkeypatch.setattr(_state_mod.state, 'searcher', _BrokenSearcher())
    executor = WebSearchExecutor()
    result = executor.execute_search('test query', mode='exact', gap=0)
    assert result == []
```

---

### `tests/test_joins_lab_storage.py` (test)

**Analog:** `tests/test_no_raw_storage_access.py` (monkeypatch + guard-assertion pattern)

**Schema-version invalidation test pattern** (from RESEARCH.md code example):
```python
# Source: inferred from web/safe_storage.py round-trip pattern
def test_schema_version_mismatch_treated_as_cold_start(monkeypatch):
    monkeypatch.setattr(
        'web.safe_storage.safe_user_get',
        lambda key, default=None: (
            {'schema_version': 0, 'anchor_sys_id': '990001234'}
            if key == 'joins_lab' else default
        )
    )
    from web.pages.joins_lab import _read_joins_lab_state
    assert _read_joins_lab_state() is None

def test_valid_schema_version_returns_data(monkeypatch):
    monkeypatch.setattr(
        'web.safe_storage.safe_user_get',
        lambda key, default=None: (
            {'schema_version': 1, 'anchor_sys_id': '990001234'}
            if key == 'joins_lab' else default
        )
    )
    from web.pages.joins_lab import _read_joins_lab_state
    data = _read_joins_lab_state()
    assert data is not None
    assert data['anchor_sys_id'] == '990001234'
```

---

## Shared Patterns

### safe_user_* chokepoint (FND-06 / Phase 87 invariant)
**Source:** `web/safe_storage.py` (full file, verified)
**Apply to:** ALL new `web/` files — `web/pages/joins_lab.py`, `web/components/anchor_viewer.py`, `web/components/candidate_grid.py`, `web/joins_executor.py`
**Rule:** Zero raw `app.storage.user` access in any `web/` file except `web/safe_storage.py` itself. CI guard: `tests/test_no_raw_storage_access.py` (allowlist stays `[]`).
```python
# Correct — always use these helpers:
from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop

# Forbidden — CI will reject this in any web/ file:
# app.storage.user['joins_lab'] = ...  # ← AST scanner catches this
```

### RTL layout direction-flip
**Source:** `web/main.py:1127–1153` + `web/translations.py` (`is_rtl()`)
**Apply to:** `web/pages/joins_lab.py` (anchor pane placement), `web/components/anchor_viewer.py`
```python
# Source: web/main.py create_layout rtl_mode convention
from web.translations import is_rtl
direction_class = 'flex-row-reverse' if is_rtl() else 'flex-row'
```

### Bilingual strings via tr()
**Source:** `web/translations.py` (`tr()` function)
**Apply to:** ALL new files — every visible string uses `tr('English copy')`.
**Rule:** Phase 121 is the completeness audit but Phase 117 must have a `tr()` key for every string from line one.

### Off-loop pattern (run.io_bound)
**Source:** `web/pages/search.py:4157–4189` (`run_core_search` + `await run.io_bound(run_search_core)`)
**Apply to:** `web/pages/joins_lab.py` (search call) and shelfmark resolution
**Rule:** Any call to `state.searcher.execute_search` or `service.search_by_shelfmark` must be wrapped in `await run.io_bound(fn)` where `fn` is a plain synchronous function. Never call sync I/O directly in an `async def` NiceGUI handler.

### Progress callback dual-protocol guard
**Source:** `web/pages/parallels.py:2140–2154`
**Apply to:** `web/pages/joins_lab.py` (the `_make_progress_cb()` function)
**Rule:** Always check `isinstance(arg1, str)` and return early. The core's `_execute_batched_search` passes a string status call; a two-required-arg callback raises `TypeError` (production bug 2026-06-12).

### Image proxy routing (ANC-02)
**Source:** `web/pages/browse.py:3600–3722`
**Apply to:** `web/components/anchor_viewer.py`
**Rule:** All image URLs must go through `/api/<provider>_image_by_sysid/{sys_id}?page=N` proxies. Direct `iiif.nli.org.il` URLs are forbidden — they bypass the Phase-98 circuit breaker.

---

## Analog Source Line Ranges (Planner Reference)

| Pattern | File | Lines |
|---------|------|-------|
| `_render_line_numbered_html` (pure function) | `web/pages/browse.py` | 41–157 |
| Viewer CSS + JS head block | `web/pages/browse.py` | 532–621 |
| `zoom_in` / `zoom_out` / `zoom_reset` | `web/pages/browse.py` | 1388–1418 |
| Per-provider proxy URL construction | `web/pages/browse.py` | 3600–3722 |
| Shelfmark / sys_id resolution | `web/pages/browse.py` | 714–762 |
| `execute_search` + `is_running` guard | `web/pages/search.py` | 3979–3983 |
| `search_generation` invalidation | `web/pages/search.py` | 4036 |
| `run_core_search` + `run.io_bound` | `web/pages/search.py` | 4157–4189 |
| Progress callback dual-protocol guard | `web/pages/parallels.py` | 2140–2154 |
| `_DesktopSearchExecutor` (exact mirror) | `desktop/join_workbench.py` | 1473–1538 |
| `create_result_card` (card shell + library badge) | `web/pages/search_results.py` | 350–429 |
| `safe_user_get` / `safe_user_set` / `safe_user_pop` | `web/safe_storage.py` | 46–85 |
| Route registration + `safe_user_set('current_page')` | `web/main.py` | 1689–1691 |
| `nav_items` list + conditional puzzle guard | `web/main.py` | 1133–1144 |
| `SearchExecutor` Protocol (4 method signatures) | `shared/joins_lab.py` | 149–193 |
| `BuilderRow` / `SideQuery` / `Candidate` dataclasses | `shared/joins_lab.py` | 28–128 |
| `AppState.is_ready()` / `state.searcher` | `web/state.py` | 70–75 |

---

## No Analog Found

All Phase 117 files have close analogs. No files require research-only patterns.

---

## Metadata

**Analog search scope:** `web/pages/`, `web/components/`, `web/`, `desktop/`, `shared/`, `tests/`
**Files scanned:** 12 analog files read (browse.py, search.py, parallels.py, search_results.py, safe_storage.py, state.py, main.py, join_workbench.py, joins_lab.py, typography.py, joins_panel.py, test_no_raw_storage_access.py)
**Pattern extraction date:** 2026-06-17

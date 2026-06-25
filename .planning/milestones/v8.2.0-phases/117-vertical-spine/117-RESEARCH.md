# Phase 117: Vertical Spine - Research

**Researched:** 2026-06-17
**Domain:** NiceGUI web app — adapter seam, page composition, safe_storage schema, image viewer extraction
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** Wide-screen layout: anchor pane sticky on reading-start side, work column scrolls.
- **D-02:** Direction-aware side: EN=left, HE=right, flips with `rtl_mode`.
- **D-03:** Narrow screens stack — anchor collapses to top strip, single scroll column below.
- **D-04:** Layout must leave structural room for Phase 118/119 additions (no forced re-layout).
- **D-05:** Cold-start: single smart box (shelfmark OR sys_id) — resolved via existing `search_by_shelfmark` path.
- **D-06:** Always-visible "choose from list" button; anonymous click prompts login, never hidden or deferred.
- **D-07:** Empty state: centered "pin an anchor" panel with smart box + list button + one-line description.
- **D-08:** Phase-117 builder is a single multi-line textarea; each non-empty line → `BuilderRow(term=line)`.
- **D-09:** Spine search runs in exact mode only (`SideQuery.variants=False`). No toggles in 117.
- **D-10:** Reuse `/browse`'s `manuscriptViewer` JS by extracting it into a reusable component.
- **D-11:** All image fetches go through existing per-provider proxy + Phase-98 NLI circuit breaker.
- **D-12:** Versioned `safe_storage` schema now; key `joins_lab`, explicit `schema_version` field.
- **D-13:** Write anchor `sys_id` to `safe_storage`. Bare `/joins-lab` restores last anchor; URL param wins.
- **D-14:** Phase-117 candidate grid is read-only: thumbnail + shelfmark + library + title. No triage.
- **D-15:** `WebSearchExecutor` wraps `state.searcher.execute_search` directly — NOT `/api/search`.
- **D-16:** Call made off-loop via `await run.io_bound(...)` with timeout, cancellation, stale-generation (latest-wins) handling modeled on `web/pages/search.py`.

### Claude's Discretion
- Exact column widths / sticky offsets / breakpoint px — match existing web app conventions.
- Precise `safe_storage` key name and dict shape (beyond requiring `schema_version`).
- Whether "open in /browse" link per candidate card — include if cheap.
- Deep-link param set for Phase 117: implement `sys_id` minimally; optional `shelfmark`, `fl_id`, `page`, `volume_ie` where the anchor viewer already accepts them.

### Deferred Ideas (OUT OF SCOPE)
- Typeahead/autocomplete on cold-start box.
- Builder modes / global toggles (Phase 118).
- Candidate triage / actions / table / Compare / VS (Phase 119–120).
- Full builder/triage/filter persistence + re-run-on-restore (Phase 120).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| FND-01 | Web `SearchExecutor` adapter wraps `state.searcher.execute_search` directly, off the event loop, with timeout/cancel/stale-generation | Section: WebSearchExecutor Adapter Pattern |
| FND-02 | `/joins-lab` web route | Section: Page Registration Pattern |
| FND-03 | Cold-start by shelfmark or sys_id | Section: Anchor Resolution |
| FND-06 | No login wall; all state via `safe_user_*` | Section: safe_storage Schema |
| FND-08 | Deep-link URL contract explicit | Section: Deep-Link URL Contract |
| ANC-01 | Anchor image with zoom/pan + folio nav | Section: Image Viewer Extraction |
| ANC-02 | Images via existing per-provider proxies + Phase-98 breaker | Section: Image Proxy Resolution |
| ANC-03 | RTL numbered transcription | Section: RTL Numbered Transcription |
| BLD-01 | Anchor-side line builder | Section: Builder — Textarea Pattern |
| BLD-05 | compose → execute → candidates pipeline wired end-to-end | Section: WebSearchExecutor Adapter Pattern |
| CND-01 | Dedup one-per-image (`dedup_candidates`) | Section: Candidate Pipeline |
| CND-02 | Candidate grid (thumbnail + key metadata) | Section: Candidate Grid Component |
</phase_requirements>

---

## Summary

Phase 117 is fundamentally a **UI composition + one missing adapter** phase. The heavy lifting is already done: `shared/joins_lab.py` (Phase 106) provides the complete domain model, `compose()`, `dedup_candidates`, and the `SearchExecutor` Protocol. The desktop Joins Lab (`desktop/join_workbench.py`) provides a UAT-approved parity reference. This phase writes the thin glue that connects all existing pieces for the web: the `WebSearchExecutor` adapter (the riskiest seam), the `/joins-lab` page shell, the anchor pane (extracted from `/browse`), a minimal textarea builder, and the candidate grid.

The key research findings are: (1) the `run.io_bound` + `search_generation` + `is_running` pattern from `web/pages/search.py:3979–4189` is the exact off-loop template to copy for FND-01; (2) `_render_line_numbered_html()` from `browse.py:41` is already fully reusable as-is (no extraction needed — just import it); (3) `manuscriptViewer` JS is initialized in a head HTML block with CSS and must be extracted carefully as a component that injects its own `add_head_html` snippet; (4) the progress callback has a **dual protocol** (numeric `(current, total)` AND single-string `status` call) — the web adapter's inner callback must handle both branches or silently suppress the string call; (5) the `tests/test_no_raw_storage_access.py` AST scanner operates on all `web/**/*.py` files, so even the new `WebSearchExecutor` file, if placed under `web/`, must use zero raw `app.storage.user`.

**Primary recommendation:** Copy the `run_core_search` + `search_generation` pattern from `search.py` verbatim into the new `WebSearchExecutor`'s async call site; extract `manuscriptViewer` as `web/components/anchor_viewer.py` injecting its own head HTML; define the `safe_storage` schema with `schema_version: 1` now so Phase 120 can extend it without migration.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Anchor fragment load (shelfmark resolve) | Frontend Server (NiceGUI page handler) | API / Backend (via `service.search_by_shelfmark`) | Resolution is I/O-bound; runs off loop via `run.io_bound` |
| Image display | Browser / Client | Frontend Server (proxy) | `manuscriptViewer` JS runs in browser; images fetched via server-side proxy endpoints |
| Search execution (adapter) | Frontend Server (NiceGUI) | Search Engine (`state.searcher`) | Must run off event loop; wraps `state.searcher` directly |
| Per-user state (anchor, schema) | Frontend Server | — | `app.storage.user` is server-side per-session in NiceGUI |
| Candidate dedup + display | Frontend Server (NiceGUI) | — | `dedup_candidates` is pure; rendering is NiceGUI UI tree |
| Deep-link URL params | Browser / Client | Frontend Server | URL params parsed by `@ui.page` route function signature |
| RTL transcription rendering | Frontend Server | Browser / Client | `_render_line_numbered_html()` generates HTML on server; browser renders it |

---

## Standard Stack

### Core (no new packages — all existing)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | installed | Web UI framework + `run.io_bound` | Project standard; all web pages use it |
| `shared/joins_lab.py` | v8.0.0 (Phase 106) | Domain model, Protocol, compose, dedup | Complete, tested, web-reusable |
| `web/safe_storage.py` | Phase 87 | Per-session state chokepoint | CI-guarded invariant |
| `web/translations.py` `tr()` | — | i18n | Project standard |
| `web/services.py` `search_by_shelfmark` | — | Shelfmark resolution | Existing, off-loop compatible |
| `web/state.py` `state.searcher` | — | Search engine access | Singleton AppState |

### No new Python packages required for Phase 117.

[VERIFIED: codebase grep] All required capabilities exist in the current codebase. No `pip install` needed for this phase.

---

## Package Legitimacy Audit

No external packages are installed in this phase. All functionality is implemented using existing project code and the NiceGUI framework already installed.

**Packages removed due to slopcheck [SLOP] verdict:** none
**Packages flagged as suspicious [SUS]:** none

---

## Architecture Patterns

### System Architecture Diagram

```
User browser
    │
    │  GET /joins-lab?sys_id=...
    ▼
@ui.page('/joins-lab') route (web/main.py)
    │  reads URL params (sys_id, shelfmark, fl_id, page, volume_ie)
    │  calls create_layout() → sidebar shell
    ▼
web/pages/joins_lab.py :: create_joins_lab_page()
    │
    ├── [Cold start — no anchor]
    │       centered empty-state panel
    │       smart box (shelfmark/sys_id input)
    │       → on submit: run.io_bound(service.search_by_shelfmark) → load anchor
    │
    ├── [Anchor loaded]
    │   ├── AnchorViewer component (web/components/anchor_viewer.py)
    │   │       manuscriptViewer JS (extracted from browse.py)
    │   │       per-provider proxy URL resolution
    │   │       → /api/nli_image_by_sysid/{sys_id}?page=N
    │   │       → /api/oxford_image/{sys_id}?page=N
    │   │       → /api/cambridge_image/{sys_id}?page=N
    │   │       RTL numbered transcription (_render_line_numbered_html)
    │   │       Phase-98 NLI circuit breaker (shared/nli_circuit_breaker.py)
    │   │
    │   └── Work column (scrolls)
    │           textarea builder (one line per BuilderRow)
    │           "Run Search" button → execute_joins_search()
    │               search_generation += 1  [stale-gen guard]
    │               await run.io_bound(run_search_core)
    │                   → WebSearchExecutor.execute_search(query_str, mode='exact', ...)
    │                       → state.searcher.execute_search(...)
    │               dedup_candidates(raw_results, anchor_sid)
    │               CandidateGrid component (web/components/candidate_grid.py)
    │
    └── safe_storage
            key: 'joins_lab'
            {'schema_version': 1, 'anchor_sys_id': ..., 'anchor_fl_id': ..., 'anchor_volume_ie': ...}
            read/write via safe_user_get / safe_user_set
```

### Recommended Project Structure

```
web/
├── pages/
│   └── joins_lab.py          # new — @ui.page('/joins-lab'), create_joins_lab_page()
└── components/
    ├── anchor_viewer.py       # new — AnchorViewer (extracted from browse.py)
    └── candidate_grid.py      # new — CandidateGrid (read-only Phase 117)

(web/main.py — add route + sidebar nav entry)
(tests/ — two new CI tests: off-loop assertion + no-state-bleed)
```

### Pattern 1: Off-Loop Search with Latest-Wins Guard (FND-01 / D-16)

This is the **single most important pattern** to get right. Copied directly from `web/pages/search.py:3979–4189` [VERIFIED: codebase read].

```python
# Source: web/pages/search.py:3979 (run_core_search + search_generation pattern)

# State on the page closure (not safe_storage — these are transient UI state)
_search_generation = {'value': 0}
_is_running = {'value': False}

async def execute_joins_search():
    # Re-entrancy guard
    if _is_running['value']:
        return

    # Assemble BuilderRows from textarea
    lines = [l.strip() for l in builder_textarea.value.splitlines() if l.strip()]
    if not lines:
        return

    if not state.is_ready():
        ui.notify(tr("Engine not ready."), type='warning')
        return

    _is_running['value'] = True
    _search_generation['value'] += 1   # Stale-generation invalidation
    my_gen = _search_generation['value']

    # Immediate UI feedback
    run_btn.props('loading=true disabled=true')
    candidates_container.clear()

    try:
        from shared.joins_lab import BuilderRow, SideQuery, compose, dedup_candidates
        rows = tuple(BuilderRow(term=line) for line in lines)
        side_query = SideQuery(rows=rows, variants=False, page_position=None)
        query_str, responsa_options, page_position = compose(side_query)

        if not query_str:
            return  # All lines were blank after strip

        def run_search_core():
            """Runs in io_bound thread — must not touch NiceGUI UI."""
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

        raw_results = await run.io_bound(run_search_core)

        # Stale-generation check: discard if a newer search was triggered
        if my_gen != _search_generation['value']:
            return

        anchor_sid = _anchor_state['sys_id'] or ''
        candidates, _ = dedup_candidates(raw_results, anchor_sid)
        _render_candidates(candidates)

    finally:
        _is_running['value'] = False
        run_btn.props(remove='loading disabled')
```

**Critical: progress_callback dual protocol.** The core's `_execute_batched_search` calls the callback TWICE per batch: once as `progress_cb(i, total_hits)` (numeric) and once as `progress_cb("Scanning items ...")` (single string). The web callback from `search.py:4055` only handles numeric `(current, total)` and will raise `TypeError` on the string call. The fix (2026-06-12 hotfix, grounded in `genizah_core.py:1064–1073`) is already in `_execute_batched_search`: it wraps the string call in a bare `except Exception: pass`. **For the Joins Lab adapter's progress callback, use the same guard as `web/pages/parallels.py:2145–2148`** — check `isinstance(arg1, str)` and return early: [VERIFIED: codebase grep `web/pages/parallels.py:2144-2148`]

```python
def _make_progress_cb():
    def progress_cb(arg1, arg2=None):
        if isinstance(arg1, str):
            # String status call from _execute_batched_search — ignore content
            return
        current, total = arg1, arg2
        # update progress UI if desired
    return progress_cb
```

### Pattern 2: Page Registration (FND-02)

[VERIFIED: codebase read `web/main.py:1689-1799`]

```python
# Source: web/main.py — follows the browse_page_route pattern exactly

@ui.page('/joins-lab', title='Joins Lab | Dicta Genizah Search')
def joins_lab_page_route(
    sys_id: str = None,
    shelfmark: str = None,
    fl_id: str = None,
    page: int = None,
    volume_ie: str = None,
):
    safe_user_set('current_page', '/joins-lab')
    ui.add_head_html(apply_theme_immediately())

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

**Sidebar nav entry** in `create_layout()`'s `nav_items` list (guarded by `WEB_PUZZLE_ENABLED`-style flag if desired, or unconditional). Icon: `'join_inner'` or `'link'` (Material Icons). Label: `tr('Joins Lab')`.

### Pattern 3: safe_storage Schema (D-12 / D-13)

[VERIFIED: codebase read `web/safe_storage.py`]

```python
# Source: web/safe_storage.py (safe_user_get / safe_user_set helpers)
from web.safe_storage import safe_user_get, safe_user_set

_JOINS_LAB_KEY = 'joins_lab'
_SCHEMA_VERSION = 1

def _read_joins_lab_state() -> dict | None:
    """Read and validate safe_storage. Returns None on version mismatch or absence."""
    data = safe_user_get(_JOINS_LAB_KEY, default=None)
    if not isinstance(data, dict):
        return None
    if data.get('schema_version') != _SCHEMA_VERSION:
        return None  # discard stale schema
    return data

def _write_joins_lab_state(anchor_sys_id: str, anchor_fl_id=None, anchor_volume_ie=None):
    safe_user_set(_JOINS_LAB_KEY, {
        'schema_version': _SCHEMA_VERSION,
        'anchor_sys_id': anchor_sys_id,
        'anchor_fl_id': anchor_fl_id,
        'anchor_volume_ie': anchor_volume_ie,
    })
```

**Schema contract for Phase 120 extension:** The dict shape above has room for Phase 120's builder rows, triage verdicts, filter state without a version bump — add keys under the same `schema_version: 1`. Only bump `_SCHEMA_VERSION` when breaking the shape (removing a key or changing a key's type).

### Pattern 4: Shelfmark / sys_id Resolution (FND-03)

[VERIFIED: codebase read `web/pages/browse.py:714–743` + `web/services.py:237`]

```python
# Source: web/pages/browse.py:729-743 (search_shelfmark logic)

async def resolve_anchor_input(query: str) -> str | None:
    """Resolve shelfmark or sys_id string to a sys_id. Returns None on failure."""
    query = query.strip()
    if not query:
        return None

    # Numeric sys_id fast path (matches browse.py:729-738 guard)
    if query.isdigit() and query.startswith('99'):
        return query

    # Shelfmark resolution via existing service (run.io_bound — I/O-bound)
    results, exact_match = await run.io_bound(
        lambda: service.search_by_shelfmark(query, limit=20)
    )
    if results:
        # browse.py uses results[0] for exact match or first result
        return results[0].sys_id
    return None
```

`service.search_by_shelfmark` is in `web/services.py:237`. It uses `state.meta_mgr.resolve_system_by_shelfmark()` which is I/O-bound (SQLite lookup). Always call via `run.io_bound`. [VERIFIED: codebase read]

### Pattern 5: Image Viewer Extraction (ANC-01 / D-10)

The `manuscriptViewer` JS lives in a head HTML string at `browse.py:609–621`. It is initialized via a `<script>` block that calls `createManuscriptViewer(config)` which references `window.manuscript_viewer.js` (loaded with `defer`). The zoom/pan/rotate JS functions (`zoom_in`, `zoom_out`, `zoom_reset`) call `ui.run_javascript(...)` to update the viewer state. [VERIFIED: codebase read `browse.py:580–1505`]

**Extraction approach for `web/components/anchor_viewer.py`:**

The component must:
1. Call `ui.add_head_html(VIEWER_STYLES + VIEWER_SCRIPT)` — extract the CSS+JS block from `browse.py:532–621` to a module-level constant in `anchor_viewer.py`. NiceGUI deduplicates `add_head_html` calls by content hash within a page, so instantiating two `AnchorViewer`s on the same page (Phase 119 Compare) will not double-inject.
2. Expose Python-level `zoom_in()`, `zoom_out()`, `zoom_reset()` methods that call `ui.run_javascript(...)` — copied from `browse.py:1388–1418`.
3. Accept `sys_id`, `fl_id`, `p_num`, `volume_ie` and drive `update_content()` — a stripped-down version of browse's `load_page()`.
4. Per-provider proxy URL resolution logic from `browse.py:3609–3699` — this is ~100 lines but **must be copied faithfully**, not rewritten, to ensure NLI/Oxford/Cambridge/Manchester/JTS parity (ANC-02).

**Key pitfall:** Browse's `update_content()` is a large function that also handles transcription, metadata panels, enrichment workers, etc. The extractor must take ONLY the image viewer + folio nav + RTL transcription sections, not the entire browse page logic. The anchor viewer in Phase 117 shows: (a) image with zoom/pan, (b) folio navigation (prev/next), (c) RTL numbered transcription. Everything else (corrections, metadata panels, community features) stays in browse.

### Pattern 6: RTL Numbered Transcription (ANC-03)

[VERIFIED: codebase read `browse.py:41–157`]

`_render_line_numbered_html()` is already a pure function at `browse.py:41`. It accepts `text: str`, `highlight_html: str | None`, `line_height: str`, `font_size: str`, `show_line_numbers: bool`. For the anchor pane:

```python
# Source: web/pages/browse.py:41 (_render_line_numbered_html)
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

Use `ui.html(html_content)` or `element.content = html_content` to render. The function is importable without changes — no extraction needed, just import from `web.pages.browse`.

**Note:** `_render_line_numbered_html` is a private helper (`_` prefix). The planner should note this and ensure the import is stable, or consider moving it to `web/components/` as a public helper during Phase 117.

### Pattern 7: Candidate Grid (CND-01 / CND-02)

[VERIFIED: codebase read `web/pages/search_results.py:350–429`]

The existing `create_result_card` in `search_results.py` is too feature-heavy for the candidate grid (it includes selection checkboxes, PGP badges, domain badges, accordion expansion, etc.). Phase 117 needs a simpler card. `web/components/candidate_grid.py` should be a new, thin component:

```python
# web/components/candidate_grid.py (new)
from web.translations import tr, get_language
from nicegui import ui

def create_candidate_grid(candidates: list, *, on_browse_click=None):
    """Render a read-only grid of dedup'd candidates (Phase 117).

    Each card: thumbnail, shelfmark, library chip, title.
    Phase 119 adds triage Y/?/N — this component is extended, not replaced.
    """
    with ui.grid(columns=2).classes('w-full gap-3') as grid:
        for cand in candidates:
            _create_candidate_card(cand, on_browse_click=on_browse_click)
    return grid

def _create_candidate_card(cand, *, on_browse_click=None):
    from genizah_core import get_library_display
    sys_id = cand.sys_id
    shelfmark = cand.shelfmark or '?'
    library_code = cand.library_code or ''
    title = cand.title or ''
    page = cand.page

    with ui.card().classes('w-full p-4 cursor-default').style('border-radius: 8px; border: 1px solid var(--border-light);'):
        # Thumbnail (future Phase 119 will enrich; Phase 117: placeholder or proxy URL)
        with ui.row().classes('items-start gap-3 w-full'):
            # Library chip
            if library_code:
                full_name = get_library_display(library_code, short=False, lang=get_language())
                ui.label(library_code).classes('text-xs px-2 py-0.5 rounded shrink-0').style(
                    'background: var(--primary-100); color: var(--primary-700);'
                ).tooltip(full_name)
            # Shelfmark
            ui.label(shelfmark).classes('text-sm font-semibold min-w-0 truncate flex-1')
        # Title (2 lines max)
        if title:
            ui.label(title[:80] + ('...' if len(title) > 80 else '')).classes('text-xs').style(
                'color: var(--text-secondary); direction: rtl;'
            )
        # "View in Browse" link (D-14 optional polish)
        browse_url = f'/browse?sys_id={sys_id}' + (f'&page={page}' if page else '')
        ui.link(tr('View in Browse'), browse_url).classes('text-xs mt-1').style(
            'color: var(--primary-700);'
        )
```

### Anti-Patterns to Avoid

- **Calling `state.searcher.execute_search` on the event loop:** Must always be inside `await run.io_bound(...)`. The event loop IS the NiceGUI request thread; blocking it for even 100ms degrades all sessions on the server.
- **Ignoring the progress callback dual protocol:** The core calls the progress_cb with a SINGLE STRING argument (e.g. `progress_cb("Scanning items 0-5000...")`). A two-arg-only callback (`def progress_cb(current, total)`) will raise `TypeError` inside `_execute_batched_search`. Handle or suppress this via the `isinstance(arg1, str)` guard pattern from `parallels.py:2145`.
- **Raw `app.storage.user` access:** ANY direct access in `web/` triggers `tests/test_no_raw_storage_access.py` failure. Use ONLY `safe_user_get` / `safe_user_set` / `safe_user_pop`.
- **Using the `/api/search` endpoint for the adapter:** It omits `text_position`/`corpus_scope`, caps modes, and goes through HTTP serialization. Must wrap `state.searcher.execute_search` directly.
- **Reinventing the image proxy:** Browse already has the per-provider resolution logic. Any new code that constructs direct NLI IIIF URLs will bypass the Phase-98 circuit breaker and re-introduce the 2026-05-25 hang pattern.
- **Hardcoding physical side for anchor pane:** The anchor must be LEFT in EN (`flex-row`), RIGHT in HE (`flex-row-reverse`). Check `is_rtl()` from `web/translations.py`, not a hardcoded side.
- **Putting the `manuscriptViewer` head HTML in page handler:** It must go in the component constructor (`ui.add_head_html(...)` called from within the component), so Phase 119 can instantiate two viewers per Compare pane.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Line-break query composition | Custom query builder | `shared/joins_lab.compose(SideQuery)` | Already handles RTL pipe tokens, gap markers, line_start/end, responsa_options, page_position — 750 lines of tested logic |
| Candidate dedup | Custom dedup | `shared/joins_lab.dedup_candidates(raw, anchor_sid)` | Handles (sys_id, page) key, self-match detection, anchor exclusion — don't reinvent |
| Shelfmark normalization | New resolver | `service.search_by_shelfmark()` in `web/services.py:237` | Existing, battle-tested, off-loop compatible |
| Image zoom/pan | Custom JS | `manuscriptViewer` (extracted from browse.py) | Works for NLI, Oxford, Cambridge, Manchester, JTS; handles mobile touch |
| Per-provider image proxy | Direct IIIF URLs | Existing proxy endpoints (`/api/nli_image_by_sysid/`, `/api/oxford_image/`, etc.) | Phase-98 circuit breaker, NLI blocking protection |
| RTL numbered transcription | Custom HTML | `_render_line_numbered_html()` from `browse.py:41` | Pure function, tested, handles gutter alignment, `user-select: none` |
| Safe per-user state | `app.storage.user` directly | `safe_user_get` / `safe_user_set` | Prune-race protection, CI-guarded |
| Off-loop executor | Thread pool directly | `await run.io_bound(fn)` | NiceGUI's own abstraction; handles context, cancellation |

**Key insight:** This phase builds almost NOTHING new in terms of logic. Every hard problem is already solved. The phase is 80% wiring and 20% new UI layout.

---

## Common Pitfalls

### Pitfall 1: Progress Callback TypeError (PRODUCTION BUG 2026-06-12)
**What goes wrong:** `_execute_batched_search` in `genizah_core.py:1069` calls `progress_callback("Scanning items 0-5000...")` (a single string arg). A standard `def progress_cb(current, total)` raises `TypeError: takes 2 positional arguments but 1 was given`.
**Why it happens:** The core's Lab search engine added a status-text call on top of the numeric call. The web `search.py` callback only handles numeric. The batched_search added a bare `except Exception: pass` guard (2026-06-12 fix), but the Joins Lab adapter should also guard proactively in its own callback.
**How to avoid:** Use the `isinstance(arg1, str)` pattern from `parallels.py:2145–2148`:
```python
def progress_cb(arg1, arg2=None):
    if isinstance(arg1, str):
        return  # text status — ignore
    current, total = arg1, arg2
```
**Warning signs:** `TypeError: takes 2 positional arguments but 1 was given` in server log; candidate grid never renders after search.

### Pitfall 2: Event Loop Blocking
**What goes wrong:** Calling `state.searcher.execute_search(...)` synchronously in an `async def` page handler (without `run.io_bound`) blocks the NiceGUI event loop for the duration of the search (seconds to minutes). All other sessions on the server freeze.
**Why it happens:** NiceGUI's async handlers run on the event loop. Sync I/O in async context is blocking.
**How to avoid:** Always wrap the search call in `await run.io_bound(run_search_core)` where `run_search_core` is a plain synchronous function. The `run.io_bound` dispatcher runs it in a thread pool.
**Warning signs:** Server becomes unresponsive during search; CI test for off-loop execution fails.

### Pitfall 3: Stale-Generation Interference
**What goes wrong:** User runs search twice quickly. Second search completes first; first search completes and overwrites results with stale data.
**Why it happens:** Both searches complete asynchronously; the slower first search renders its (older) results after the newer search already rendered.
**How to avoid:** Increment `search_generation` before each search; check `my_gen != search_generation` before rendering results (exactly as `search.py:4036`).
**Warning signs:** Candidate grid shows wrong results when clicking "Run Search" rapidly.

### Pitfall 4: Raw app.storage.user in New Files
**What goes wrong:** Adding `app.storage.user[...]` in any new `web/*.py` file causes `tests/test_no_raw_storage_access.py` to fail CI.
**Why it happens:** The AST scanner checks ALL `*.py` under `web/` except `safe_storage.py`.
**How to avoid:** Every per-user read/write in `web/pages/joins_lab.py`, `web/components/anchor_viewer.py`, `web/components/candidate_grid.py` — and any new `WebSearchExecutor` file under `web/` — must use `safe_user_get` / `safe_user_set` exclusively.
**Warning signs:** CI fails with `Raw app.storage.user access found outside allowlist`.

### Pitfall 5: Importing _render_line_numbered_html from browse.py
**What goes wrong:** `_render_line_numbered_html` has a leading underscore (private). If browse.py is refactored, this import breaks silently.
**Why it happens:** The function was designed as browse-internal.
**How to avoid:** Either (a) move `_render_line_numbered_html` to `web/components/typography.py` (it's a pure function with no browse state), or (b) import it from `web.pages.browse` and document that the underscore signals "not yet promoted", to be resolved in Phase 119 when Compare reuses it. Option (a) is cleaner but touches a test that may assert the function location.
**Warning signs:** `ImportError` on anchor pane load.

### Pitfall 6: anchor_viewer.py instantiated twice per page injects duplicate head HTML
**What goes wrong:** Phase 119 Compare instantiates two `AnchorViewer` components on the same page. If `add_head_html` is called twice, the `manuscriptViewer` JS block is injected twice.
**Why it happens:** NiceGUI `add_head_html` deduplicates by content hash only within the SAME `add_head_html` call string — but only if the content is identical.
**How to avoid:** Use a module-level `_VIEWER_HEAD_INJECTED` flag or check via a unique CSS class sentinel before injecting. The anchor_viewer component should inject its head HTML exactly once per page, regardless of how many instances are created. Pattern: `if not _viewer_head_injected: ui.add_head_html(VIEWER_HEAD); _viewer_head_injected = True` (stored in a `threading.local` or NiceGUI client context). The safest approach: make the VIEWER_HEAD content idempotent (re-running it is safe) by checking `if (!window.manuscriptViewerLoaded) { ... window.manuscriptViewerLoaded = true; }`.
**Warning signs:** Phase 119 Compare shows two viewers but only one image loads; JS errors about duplicate `createManuscriptViewer` calls.

### Pitfall 7: Direction mismatch on narrow screens
**What goes wrong:** On narrow screens (< 768px), the anchor collapses to a strip at the top. If the collapse logic hardcodes "top" as absolute position, RTL pages may show it at the wrong edge.
**Why it happens:** Narrow-screen layout is a stacked single column — direction is irrelevant for the stack order itself, but the "expand/collapse" button should still respect RTL.
**How to avoid:** Use `flex-row-reverse` for the header row on narrow HE screens (same as browse.py's existing RTL handling). The collapsed strip itself is full-width so side positioning doesn't apply.
**Warning signs:** HE interface shows anchor strip controls on wrong side of screen.

---

## Code Examples

### WebSearchExecutor — complete skeleton

```python
# web/joins_executor.py (new file, or nested class in joins_lab.py)
# Source: desktop/join_workbench.py:1473 (_DesktopSearchExecutor) + web/state.py

from shared.joins_lab import SearchExecutor
from web.state import state


class WebSearchExecutor:
    """Concrete adapter satisfying the Phase-106 SearchExecutor Protocol for the web.

    Thin passthrough — wraps state.searcher + state.meta_mgr.
    Returns [] on any failure (D-15 / mirrors _DesktopSearchExecutor:1510).

    IMPORTANT: All execute_search calls must be made INSIDE run.io_bound
    (called by the async NiceGUI handler). This class is synchronous —
    it must never be called directly on the event loop.
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
        try:
            return state.searcher.execute_search(
                query_str,
                mode=mode,
                gap=gap,
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
        try:
            return state.meta_mgr.get_meta_for_id(sys_id)
        except Exception:
            return ('', '')

    def get_library_for_id(self, sys_id: str) -> str:
        try:
            return state.meta_mgr.get_library_for_id(sys_id) or ''
        except Exception:
            return ''
```

**Protocol check:** `WebSearchExecutor` satisfies `isinstance(executor, SearchExecutor)` at runtime because `SearchExecutor` is `@runtime_checkable`. All four methods match the Protocol signatures at `shared/joins_lab.py:150–193`. [VERIFIED: codebase read]

### safe_storage read/write with schema-version invalidation

```python
# Source: inferred from web/safe_storage.py pattern + UI-SPEC.md schema
from web.safe_storage import safe_user_get, safe_user_set

_KEY = 'joins_lab'
_VER = 1

def load_joins_lab_storage() -> dict | None:
    data = safe_user_get(_KEY, default=None)
    if not isinstance(data, dict) or data.get('schema_version') != _VER:
        return None
    return data

def save_anchor_to_storage(sys_id: str, fl_id=None, volume_ie=None):
    safe_user_set(_KEY, {
        'schema_version': _VER,
        'anchor_sys_id': str(sys_id) if sys_id else None,
        'anchor_fl_id': fl_id,
        'anchor_volume_ie': volume_ie,
    })
```

### Two-anonymous-session no-state-bleed test skeleton

```python
# tests/test_joins_lab_storage.py (new)
# Tests: (1) Two sessions do not share safe_storage state
#        (2) schema_version mismatch returns None (cold start)

def test_schema_version_mismatch_treated_as_cold_start(monkeypatch):
    from web.pages.joins_lab import load_joins_lab_storage
    # Simulate stale schema in storage
    monkeypatch.setattr(
        'web.safe_storage.safe_user_get',
        lambda key, default=None: {'schema_version': 0, 'anchor_sys_id': '990001234'} if key == 'joins_lab' else default
    )
    assert load_joins_lab_storage() is None

def test_valid_schema_returns_data(monkeypatch):
    from web.pages.joins_lab import load_joins_lab_storage
    monkeypatch.setattr(
        'web.safe_storage.safe_user_get',
        lambda key, default=None: {'schema_version': 1, 'anchor_sys_id': '990001234'} if key == 'joins_lab' else default
    )
    data = load_joins_lab_storage()
    assert data is not None
    assert data['anchor_sys_id'] == '990001234'
```

The two-anonymous-session no-state-bleed test for the full NiceGUI context requires `TestClient` from `nicegui.testing` — see the Validation Architecture section for the test strategy.

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Direct `app.storage.user` access | `safe_user_*` helpers via `web/safe_storage.py` | Phase 87 (2026-05-14) | CI-guarded; 0 raw access in `web/` |
| Separate backend process (port 8000) | NiceGUI FastAPI app serves all `/api/*` | Jan 2026 | `state.searcher` is the only search engine |
| NLI IIIF direct fetches | Server-side proxy + Phase-98 circuit breaker | Phase 98 (2026-05-25) | 3 consecutive failures trip breaker; fetch returns empty in microseconds |
| Single progress callback signature | Dual protocol: numeric `(i, total)` + string `status` | 2026-06-12 hotfix | Adapters must handle or suppress string call |
| Desktop `_DesktopSearchExecutor` (Phase 107) | Web `WebSearchExecutor` (Phase 117) | This phase | Same Protocol, different app context |

**Deprecated/outdated:**
- Direct NLI IIIF URL construction: replaced by `/api/nli_image_by_sysid/{sys_id}?page=N`. Do NOT put `iiif.nli.org.il` URLs in new code.
- `app.storage.user[key]` in `web/` files: replaced by `safe_user_get`/`safe_user_set`. CI will reject it.

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong | Resolution |
|---|-------|---------|---------------|------------|
| A1 | `state.searcher.get_browse_page` signature matches Protocol (has `volume_ie`, `allow_cross` params) | WebSearchExecutor adapter | adapter compiles but Protocol compliance check fails at runtime | RESOLVED (planning, 2026-06-17): `SearchEngine.get_browse_page` at `genizah_core.py:9869` has signature `(sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None)` — matches Protocol exactly. Owner: `state.searcher`. |
| A2 | `state.meta_mgr.get_meta_for_id(sys_id)` returns `(shelfmark, title)` tuple | WebSearchExecutor adapter | method may not exist or return different shape | RESOLVED (planning, 2026-06-17): `MetadataManager.get_meta_for_id` at `genizah_core.py:3968` returns `(shelf, title)`. Owner: `state.meta_mgr`. |
| A3 | `state.meta_mgr.get_library_for_id(sys_id)` exists | WebSearchExecutor adapter | method may be named differently | RESOLVED (planning, 2026-06-17): `MetadataManager.get_library_for_id` at `genizah_core.py:4004` returns library code or `''`. Owner: `state.meta_mgr`. `execute_search` is at `genizah_core.py:8600` on `state.searcher`. |
| A4 | NiceGUI `add_head_html` deduplicates identical content within a single page render | anchor_viewer.py extraction | if it doesn't deduplicate, Phase 119 Compare will double-inject viewer JS | RESOLVED (planning): not relying on NiceGUI dedup — Plan 06 adds an explicit JS `window._msViewerLoaded` idempotency guard (Open Question 2). |
| A5 | `_render_line_numbered_html` has no hidden dependency on `BrowseState` or other browse globals | RTL transcription import | if it imports browse-internal state, the function cannot be cleanly imported | RESOLVED (planning): verified pure (internal `import html` escape, no browse globals); Plan 03 promotes it to `web/components/typography.py` (Open Question 1). |

**Verification (A1–A3):** COMPLETE — inspected `genizah_core.py` during planning (`get_meta_for_id`:3968, `get_library_for_id`:4004 on MetadataManager; `execute_search`:8600, `get_browse_page`:9869 on SearchEngine). Recorded in Plan 117-01 `<interfaces>` block.

---

## Open Questions (RESOLVED)

1. **`_render_line_numbered_html` placement** — RESOLVED in Plan 117-03 Task 1.
   - What we know: it is at `browse.py:41`, pure function, no external dependencies visible in the function body.
   - Resolution: Plan 117-03 Task 1 moves the function to `web/components/typography.py` as a public `render_line_numbered_html` and re-exports the old private name from `browse.py` (keeps `tests/test_line_numbers_web.py` green). Verified pure during planning — no browse-global dependency.

2. **`manuscriptViewer` head HTML duplication guard** — RESOLVED in Plan 117-06 Task 1.
   - What we know: Phase 119 will instantiate two `AnchorViewer` components on the same page.
   - Resolution: Plan 117-06 Task 1 adds a JS-side idempotency guard (`if (!window._msViewerLoaded) { ...; window._msViewerLoaded = true; }`) in the head HTML block; does not rely on NiceGUI's content-hash dedup.

3. **`get_browse_page` / `get_meta_for_id` / `get_library_for_id` ownership** — RESOLVED during planning (A1–A3).
   - What we know: the Protocol declares 4 methods; the desktop `_DesktopSearchExecutor` wraps `self._searcher` and `self._meta_mgr`.
   - Resolution: grepped `genizah_core.py` — `execute_search` (8600) + `get_browse_page` (9869) on `SearchEngine` (`state.searcher`); `get_meta_for_id` (3968) + `get_library_for_id` (4004) on `MetadataManager` (`state.meta_mgr`). Recorded in Plan 117-01 `<interfaces>` block; the adapter wires accordingly.

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| `shared/joins_lab.py` | FND-01 / BLD-05 | Yes | Phase 106 | — |
| `web/state.py` `state.searcher` | FND-01 | Yes (runtime) | AppState singleton | Returns `[]` if not ready (guard with `state.is_ready()`) |
| `web/safe_storage.py` | FND-06 | Yes | Phase 87 | — |
| `web/translations.py` `tr()` | i18n | Yes | — | — |
| `web/services.py` `search_by_shelfmark` | FND-03 | Yes | — | — |
| `manuscriptViewer` JS (in browse.py head HTML) | ANC-01 | Yes (needs extraction) | — | — |
| `_render_line_numbered_html` (browse.py:41) | ANC-03 | Yes | — | — |
| NiceGUI `run.io_bound` | FND-01 / D-16 | Yes | NiceGUI installed | — |
| Phase-98 NLI circuit breaker (`shared/nli_circuit_breaker.py`) | ANC-02 | Yes | Phase 98 | — |

**Missing dependencies with no fallback:** None.

---

## Validation Architecture

> `nyquist_validation: true` in `.planning/config.json`.

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | (project root pytest.ini or pyproject.toml) |
| Quick run command | `pytest tests/test_joins_lab.py tests/test_joins_lab_storage.py tests/test_no_raw_storage_access.py -x -q` |
| Full suite command | `pytest tests/ -x -q --ignore=tests/e2e` (or with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` per conftest) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| FND-01 | `WebSearchExecutor` satisfies `SearchExecutor` Protocol | unit | `pytest tests/test_web_search_executor.py -x` | No — Wave 0 |
| FND-01 | Search call not made on event loop | static/unit | `pytest tests/test_joins_lab_off_loop.py -x` | No — Wave 0 |
| FND-06 | No raw `app.storage.user` in new `web/` files | AST lint | `pytest tests/test_no_raw_storage_access.py -x` | Yes (existing) |
| FND-06 | Two anonymous sessions do not share state | unit | `pytest tests/test_joins_lab_storage.py::test_schema_version_mismatch_treated_as_cold_start -x` | No — Wave 0 |
| FND-08 | URL contract documented; sys_id param resolves | manual smoke | `/joins-lab?sys_id=990001234` in browser | N/A |
| BLD-05 | compose + execute + dedup pipeline wired | unit | `pytest tests/test_joins_lab.py -k compose -x` | Partial (joins_lab.py tests exist) |
| CND-01 | `dedup_candidates` called with correct anchor_sid | unit | `pytest tests/test_joins_lab.py -k dedup -x` | Partial |
| SC#3 (ROADMAP) | Search runs off event loop | assert in test | `pytest tests/test_joins_lab_off_loop.py -x` | No — Wave 0 |
| SC#5 (ROADMAP) | Two anon sessions no state bleed | unit with monkeypatch | `pytest tests/test_joins_lab_storage.py -x` | No — Wave 0 |

### Two Required CI Tests (from ROADMAP Success Criterion 3)

**Test 1 — No raw storage access (existing test must stay green):**
`tests/test_no_raw_storage_access.py` — allowlist stays `[]`. Any new `web/*.py` file for Phase 117 must use zero raw `app.storage.user`. This test already passes; it must CONTINUE to pass after Phase 117 adds `web/pages/joins_lab.py`, `web/components/anchor_viewer.py`, `web/components/candidate_grid.py`.

**Test 2 — Search not on the event loop (new test):**
The test strategy: use a `FakeSearchExecutor` (already exists in `tests/test_joins_lab.py:57`) and a mock of `run.io_bound` that records whether the search was called from the thread pool vs. the event loop. Alternatively, a static AST test on `web/pages/joins_lab.py` that asserts `state.searcher.execute_search` never appears as a direct call (only inside a closure passed to `run.io_bound`). The static AST approach is simpler and mirrors the `test_no_raw_storage_access.py` pattern.

### Sampling Rate
- **Per task commit:** `pytest tests/test_no_raw_storage_access.py tests/test_joins_lab.py -x -q`
- **Per wave merge:** Full phase test suite + full `tests/` suite
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps (test files to create before implementation)
- [ ] `tests/test_web_search_executor.py` — Protocol compliance, `execute_search` → `[]` on failure, all 4 methods covered
- [ ] `tests/test_joins_lab_storage.py` — schema_version invalidation, round-trip write/read, URL-wins-over-storage
- [ ] `tests/test_joins_lab_off_loop.py` — asserts search is not called directly on event loop (static AST or mock-based)

---

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | No login wall for read-only search; login-gated actions are Phase 120 |
| V3 Session Management | Yes | `safe_user_*` helpers; keyed by NiceGUI session cookie |
| V4 Access Control | No | No per-user data written to Supabase in Phase 117 |
| V5 Input Validation | Yes | Shelfmark/sys_id input: existing `service.search_by_shelfmark` handles normalization; builder textarea: passed to `compose()` which processes as plain text |
| V6 Cryptography | No | No new cryptographic operations |

### Known Threat Patterns

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| XSS via transcription text in `_render_line_numbered_html` | Tampering | `html.escape()` in the function (verified at `browse.py:112`); caller must NOT pass raw user input as `highlight_html` |
| Session state bleed (cross-user candidate leak) | Information Disclosure | `safe_user_*` is per-session (NiceGUI session cookie); sessions are isolated by design |
| Indirect object reference (arbitrary sys_id in URL) | Tampering | `state.searcher.get_browse_page` returns None for non-existent sys_ids; anchor pane shows "not found" state |
| XSS via sys_id/shelfmark URL param | Tampering | URL params are strings; `search_by_shelfmark` does not render them as HTML |
| NLI IIIF hang pattern (Phase 98 incident) | Denial of Service | Phase-98 circuit breaker in `shared/nli_circuit_breaker.py` — the `AnchorViewer` must route all NLI fetches through existing proxy endpoints |

---

## Project Constraints (from CLAUDE.md)

| Directive | Impact on Phase 117 |
|-----------|---------------------|
| All per-user state through `web/safe_storage.py` (zero raw `app.storage.user`) | Every joins_lab storage read/write uses `safe_user_get`/`safe_user_set` |
| NLI/IIIF image fetches through per-provider proxies + Phase-98 circuit breaker | `AnchorViewer` proxies all images; no direct `iiif.nli.org.il` URLs |
| Search always off event loop | `WebSearchExecutor` called only inside `run.io_bound` |
| Bilingual EN/HE with correct RTL layout | All strings via `tr()`; `is_rtl()` checked for `flex-row-reverse` |
| No new Supabase schema | `safe_storage` only (server-side, per-session) |
| FastAPI still live at `/api/*` | `WebSearchExecutor` must NOT call `/api/search` (it is an HTTP endpoint, not a direct adapter) |
| Hebrew RTL conventions | Builder textarea direction: `rtl`; transcription: RTL |
| `bump_version.py` for releases | No version bump needed for this internal milestone phase |
| `check_docs.py` before committing | Run `PYTHONUTF8=1 python scripts/check_docs.py` before final commit |

---

## Sources

### Primary (HIGH confidence)
- `shared/joins_lab.py` (Phase 106) — SearchExecutor Protocol, BuilderRow/SideQuery, compose(), dedup_candidates [VERIFIED: codebase read, full file]
- `web/pages/search.py:3979–4207` — off-loop pattern, search_generation, is_running, progress_cb [VERIFIED: codebase read]
- `web/pages/browse.py:41–157, 580–621, 1388–1418, 3600–3699` — _render_line_numbered_html, manuscriptViewer JS, zoom functions, per-provider proxy resolution [VERIFIED: codebase read]
- `web/safe_storage.py` — safe_user_get/set/pop, session UUID [VERIFIED: codebase read, full file]
- `web/state.py` — AppState singleton, state.searcher, state.meta_mgr, state.is_ready() [VERIFIED: codebase read]
- `web/main.py:1127–1199` — create_layout() sidebar nav items, drawer, nav_to() [VERIFIED: codebase read]
- `desktop/join_workbench.py:1469–1511` — _DesktopSearchExecutor passthrough shape [VERIFIED: codebase read]
- `tests/test_no_raw_storage_access.py` — allowlist [] invariant, AST scanner [VERIFIED: codebase read, full file]
- `web/pages/parallels.py:2140–2152` — progress_cb dual-protocol guard [VERIFIED: codebase grep]
- `genizah_core.py:1055–1073` — _execute_batched_search dual progress_callback protocol [VERIFIED: codebase read]
- `genizah_core.py:3968, 4004, 8600, 9869` — get_meta_for_id / get_library_for_id (MetadataManager) + execute_search / get_browse_page (SearchEngine) ownership + signatures [VERIFIED: codebase read during planning, A1–A3 resolved]
- `.planning/phases/117-vertical-spine/117-CONTEXT.md` — 16 locked decisions [VERIFIED: file read]
- `.planning/phases/117-vertical-spine/117-UI-SPEC.md` — layout architecture, component inventory, copywriting, color/typography tokens [VERIFIED: file read]
- `.planning/REQUIREMENTS.md` — FND/ANC/BLD/CND requirements [VERIFIED: file read]
- `.planning/ROADMAP.md` — Phase 117 success criteria [VERIFIED: file read]
- `.planning/v8.2.0-REQ-CODEX-CRITIQUE.md` — BLOCKERS 1 and 2 [VERIFIED: file read]

### Secondary (MEDIUM confidence)
- `web/services.py:237` — search_by_shelfmark signature and delegation [VERIFIED: codebase read]
- `web/pages/search_results.py:350–429` — create_result_card pattern for candidate grid [VERIFIED: codebase read]
- `tests/test_joins_lab.py:57` — FakeSearchExecutor test double [VERIFIED: codebase read]
- `web/components/` directory listing — confirms no `anchor_viewer.py` or `candidate_grid.py` yet exists [VERIFIED: bash ls]

### Tertiary (LOW confidence)
- (none remaining — A1–A3 promoted to HIGH after planning-time verification of `genizah_core.py` method signatures)

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all libraries are existing codebase code, no new packages
- Architecture: HIGH — verified patterns from production code; off-loop model confirmed
- Pitfalls: HIGH — Pitfalls 1 and 4 are verified from production bugs/CI guards; others are reasoned from codebase structure
- Adapter Protocol compliance (A1–A3): HIGH (was LOW) — method names/signatures/ownership confirmed in `genizah_core.py` during planning

**Research date:** 2026-06-17
**Valid until:** 2026-07-17 (stable codebase; safe_storage and search patterns are load-bearing and unlikely to change)
**Updated:** 2026-06-17 — A1–A3 + Open Questions 1–3 marked RESOLVED after plan-phase verification (gsd-checker documentation-hygiene warning).

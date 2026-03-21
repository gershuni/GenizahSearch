---
phase: quick
plan: 260321-tiv
type: execute
wave: 1
depends_on: []
files_modified:
  - web/components/filter_panel.py
  - web/static/manuscript_viewer.js
  - web/pages/search.py
  - web/pages/parallels.py
  - web/pages/browse.py
autonomous: true
requirements: [REFACTOR-FILTER-PANEL, REFACTOR-IMAGE-VIEWER-JS]
must_haves:
  truths:
    - "Search page filter panel works identically to before extraction (domain/author/work/date/material/text filters)"
    - "Parallels page filter panel works identically to before extraction"
    - "Browse page image viewer (standard, fullscreen, reading desk) works identically"
    - "Search advanced view image viewer works identically"
    - "Session persistence of filter state preserved for both search and parallels"
    - "~1050 lines of duplication eliminated"
  artifacts:
    - path: "web/components/filter_panel.py"
      provides: "Shared filter panel builder, state loader, summary builder, option builders"
    - path: "web/static/manuscript_viewer.js"
      provides: "Shared manuscript viewer factory (image error fallback, zoom/pan/rotate, brightness/contrast/gamma/invert)"
  key_links:
    - from: "web/pages/search.py"
      to: "web/components/filter_panel.py"
      via: "import and call create_filter_panel, load_filter_state, build_filter_summary, has_active_filters"
      pattern: "from web.components.filter_panel import"
    - from: "web/pages/parallels.py"
      to: "web/components/filter_panel.py"
      via: "import and call same functions with parallels_ prefix"
      pattern: "from web.components.filter_panel import"
    - from: "web/pages/browse.py"
      to: "web/static/manuscript_viewer.js"
      via: "script tag reference replacing inline JS"
      pattern: "manuscript_viewer.js"
    - from: "web/pages/search.py"
      to: "web/static/manuscript_viewer.js"
      via: "script tag reference replacing inline advViewer JS"
      pattern: "manuscript_viewer.js"
---

<objective>
Extract ~1050 lines of duplicated code into two shared modules: a Python filter panel component and a JavaScript manuscript viewer module.

Purpose: Reduce maintenance burden of identical filter logic in search.py (5708 lines) and parallels.py (3702 lines), and identical image viewer JS in browse.py (4984 lines) and search.py. Pure refactor -- zero behavior changes.

Output: web/components/filter_panel.py, web/static/manuscript_viewer.js, updated search.py/parallels.py/browse.py
</objective>

<execution_context>
@~/.claude/get-shit-done/workflows/execute-plan.md
@~/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@web/pages/search.py
@web/pages/parallels.py
@web/pages/browse.py
@web/components/__init__.py
</context>

<tasks>

<task type="auto">
  <name>Task 1: Extract shared filter panel component</name>
  <files>web/components/filter_panel.py, web/pages/search.py, web/pages/parallels.py</files>
  <action>
Create `web/components/filter_panel.py` extracting the duplicated filter logic from search.py and parallels.py. The ONLY differences between the two copies are: (1) state variable name (search_state vs p_state), (2) storage key prefix ("search_" vs "parallels_"), (3) UI element variable names (domain_select vs p_domain_select), (4) chip bar update function name (_update_chip_bar vs _update_p_chip_bar).

**Module-level pure functions (no NiceGUI dependency):**

1. `build_domain_options(lang: str) -> dict` -- Extract from search.py:924-962 / parallels.py:785-823. Calls get_fjms_service, get_domain_hierarchy, qualify_domain_name. Returns {key: display_label} dict. Takes `lang` parameter instead of calling get_language() internally (REVIEW P2-5: get_language() is process-global, calling it inside run.io_bound() causes cross-client label drift).

2. `build_author_options(lang: str, domain=None) -> dict` -- Extract from search.py:964-984 / parallels.py:838-858. Same lang parameter pattern.

3. `build_work_options(lang: str, domain=None, author=None) -> dict` -- Extract from search.py:986-1007 / parallels.py:873-900. Same lang parameter pattern.

4. `build_filter_summary(filters: dict, tr_func, get_language_func, max_len: int = 50) -> str` -- Extract from search.py:3000-3053 / parallels.py:2025-2078. Pass `tr` and `get_language` as parameters (they're page-context-dependent in NiceGUI).

5. `has_active_filters(state) -> bool` -- Extract from search.py:189-200 / parallels.py:253-264. Takes any object with filter_domains, filter_authors, etc. attributes.

6. `persist_value(key, value)` -- Extract from search.py:129-132 / parallels.py:193-196. Wraps app.storage.user write with session_persistence_enabled check.

**Session state functions:**

7. `load_filter_state(state, storage_prefix: str)` -- Extract from search.py:162-183 / parallels.py:226-247. Reads app.storage.user with the given prefix (e.g., "search_filter_" or "parallels_filter_"). Handles legacy single-value migration. Sets filter_domains, filter_authors, filter_works, filter_include_mode, filter_date_from, filter_date_to, filter_material_exclude, filter_text_all, filter_text_any, filter_text_not on the state object.

8. `consume_incoming_filters(state, storage_prefix: str, require_from_browse: bool = False) -> bool` -- Extract from search.py:136-159 / parallels.py:198-223. Reads incoming_filters from storage, applies to state, persists with prefix, clears incoming_filters. Returns True if filters were consumed. REVIEW P2-3: search.py only consumes when `from_browse` flag is set, parallels consumes whenever incoming_filters exist. The `require_from_browse` parameter preserves this behavioral difference: search passes True, parallels passes False. Must also preserve legacy single-value-to-list migration and return the `_filters_from_browse` flag used to auto-expand the filter panel.

**Async recompute function:**

9. `async recompute_filter_count(state, update_chip_bar_fn)` -- Extract from search.py:1375-1417 / parallels.py:1255-1297. Takes state object and a callback for chip bar update. Uses run.io_bound for the FJMS query. REVIEW P2-4: Add a generation/sequence guard to prevent out-of-order completion races — increment a generation counter before the async call, check it after await, skip update if stale. This fixes a latent bug where a slower older query could overwrite filter_manuscript_count and restrict_sys_ids from a newer filter selection. REVIEW P2-5: Only the pure FJMS query goes inside run.io_bound(); widget mutation and app.storage.user access stay outside the io_bound boundary in the caller's client context.

**Change handler factory:**

10. `create_filter_handlers(state, storage_prefix, filter_refs, refresh_author_fn, refresh_work_fn, recompute_fn, update_chip_fn) -> dict` -- Returns a dict of handler functions {on_domain_change, on_author_change, on_work_change, on_mode_change, on_date_from_change, on_date_to_change, on_exclude_printed_change}. Each handler reads from the appropriate filter_refs element, updates state, persists with prefix, and triggers async refreshes. Extract from search.py:1419-1482 / parallels.py:1299-1362.

**Then update search.py:**
- Remove _build_domain_options (lines ~924-962), _build_author_options (~964-984), _build_work_options (~986-1007)
- Replace with: `from web.components.filter_panel import build_domain_options, build_author_options, build_work_options`
- Remove _persist helper (line ~129), replace with import
- Remove session restore block (~162-183), replace with: `consume_incoming_filters(search_state, 'search')` then `if not _filters_from_browse: load_filter_state(search_state, 'search')`
- Remove _has_active_filters (~189-200), replace with import
- Remove _recompute_filter_count (~1375-1417), replace with: `async def _recompute_filter_count(): await recompute_filter_count(search_state, _update_chip_bar)`
- Remove _build_web_filter_summary (~3000-3053), replace with import call
- Remove filter change handlers (~1419-1482), replace with factory call, wire to same UI elements

**Then update parallels.py identically** with "parallels" prefix and p_state.

IMPORTANT: The UI construction code (expansion panel layout, select widgets, date inputs) is NOT extracted -- it stays in each page because each page's layout context differs. Only the logic/data functions are shared.
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -c "from web.components.filter_panel import build_domain_options, build_author_options, build_work_options, build_filter_summary, has_active_filters, persist_value, load_filter_state, consume_incoming_filters, recompute_filter_count, create_filter_handlers; print('All imports OK')"</automated>
  </verify>
  <done>
- web/components/filter_panel.py exists with all 10 functions
- search.py has ~350 fewer lines (removed duplicated functions, replaced with imports)
- parallels.py has ~350 fewer lines (same)
- `python -m web.main` starts without import errors
- Filter panel behavior unchanged in both search and parallels pages
  </done>
</task>

<task type="auto">
  <name>Task 2: Extract shared manuscript viewer JavaScript</name>
  <files>web/static/manuscript_viewer.js, web/pages/browse.py, web/pages/search.py</files>
  <action>
Create `web/static/manuscript_viewer.js` that provides a factory function to create viewer instances, replacing the 4 duplicated inline viewer objects.

**The four current viewer instances are:**
1. `window.manuscriptViewer` in browse.py VIEWER_STYLES (lines 592-745) -- selectors: `.zoomable-image`, `.image-container`, `.zoom-level-label`, gamma filter `#gamma-main`
2. `window.advViewer` in search.py ADVANCED_VIEWER_STYLES (lines 364-438) -- selectors: `.adv-zoomable-image`, `.adv-image-container`, `.adv-zoom-label`, gamma filter `#gamma-adv`
3. `fsViewer` in browse.py fullscreen dialog (lines 4706-4765+) -- created inline in dialog JS, uses element ref directly, gamma filter `#gamma-fs`, `.fullscreen-image-toolbar .zoom-level-label`
4. `window.rdViewers[viewerId]` in browse.py reading desk (lines 3683-3746) -- per-fragment viewer map, different structure (no factory, inline state objects)

**manuscript_viewer.js contents:**

```javascript
// Shared NLI IIIF utilities
const NLI_IIIF_BASE = 'https://iiif.nli.org.il/IIIFv21';
const _flIdCache = {};

async function fetchFlIdsFromManifest(sysId) { ... }
// Extract from browse.py:88-134 (the verbose version with console.log)

async function handleImageError(img, sysId, pageIdx, isOxford, viewerName) { ... }
// Extract from browse.py:140-205. REVIEW P1-1: Use lazy resolver pattern instead of
// passing viewer object directly. viewerName is a string (e.g., 'manuscriptViewer', 'advViewer')
// resolved lazily via window[viewerName] at callback time, NOT at error handler registration time.
// This prevents stale undefined refs when the viewer doesn't exist yet at onload time.
// Fullscreen browse at browse.py:4638 still calls this with its own viewer lifecycle.

function createManuscriptViewer(options) { ... }
// Factory that returns a viewer object. Options:
//   imageSelector: string (e.g., '.zoomable-image')
//   containerSelector: string (e.g., '.image-container')
//   zoomLabelSelector: string (e.g., '.zoom-level-label')
//   gammaFilterId: string (e.g., 'gamma-main')
//   zoomStep: number (default 0.25, fullscreen uses 0.15)
//   maxZoom: number (default 4, fullscreen uses 5)
// Returns object with: init, update, setTransform, applyTransform, _applyFilters,
//   setBrightness, setContrast, setGamma, toggleInvert, resetAdjustments,
//   zoomIn, zoomOut, rotateLeft, rotateRight, reset, updateLabel, onWheel, etc.
// REVIEW P1-2: The returned object MUST preserve the FULL public API of both current viewers.
// Browse relies on: init(), update(), setTransform(), applyTransform(), _applyFilters(),
//   setBrightness(), setContrast(), setGamma(), toggleInvert(), resetAdjustments(),
//   zoomIn(), zoomOut(), rotateLeft(), rotateRight(), reset(), updateLabel(), onWheel(),
//   and state properties: scale, rotation, x, y, brightness, contrast, gamma, invert.
// Search relies on: zoomIn(), zoomOut(), rotateLeft(), rotateRight(), reset(),
//   resetAdjustments(), setBrightness(), setContrast(), setGamma(), toggleInvert(),
//   init(), updateLabel().
// Do NOT normalize or omit any of these — `... is not a function` regressions are P0.
```

**Update browse.py VIEWER_STYLES:**
- Keep the `<style>` CSS block (image-viewer-container, image-container, image-controls, etc.) -- it's browse-specific
- Keep `progressiveLoad` and `initProgressiveImages` functions -- they're browse-specific
- Remove `fetchFlIdsFromManifest` (lines 88-134), `handleImageError` (lines 140-205), and `window.manuscriptViewer = {...}` (lines 592-745)
- Add `<script src="/static/manuscript_viewer.js"></script>` before the remaining script block
- Replace with: `window.manuscriptViewer = createManuscriptViewer({imageSelector: '.zoomable-image', containerSelector: '.image-container', zoomLabelSelector: '.zoom-level-label', gammaFilterId: 'gamma-main'});`
- REVIEW P3-6: Remove `DOMContentLoaded` auto-init from the shared JS file. The file should define functions only and be idempotent. Each page calls `viewer.init()` explicitly after rendering (browse does this via setTimeout at ~4392, search at ~4797). Load the shared JS with `defer` attribute.

**Update search.py ADVANCED_VIEWER_STYLES:**
- Remove `NLI_IIIF_BASE`, `advFlIdCache`, `advFetchFlIdsFromManifest` (lines 302-327), `advHandleImageError` (lines 329-362), `window.advViewer = {...}` (lines 364-438)
- Add `<script src="/static/manuscript_viewer.js"></script>` before remaining style block
- Replace with: `window.advViewer = createManuscriptViewer({imageSelector: '.adv-zoomable-image', containerSelector: '.adv-image-container', zoomLabelSelector: '.adv-zoom-label', gammaFilterId: 'gamma-adv'});`
- Update `advHandleImageError` calls in search.py HTML templates (lines ~4795, ~5460) to use `handleImageError(this, '...', idx, isOxford, 'advViewer')` instead of `advHandleImageError(this, ...)` — REVIEW P1-1: pass viewer NAME string, not object ref
- Update `handleImageError` calls in browse.py HTML templates (lines ~4388, ~4638) to pass `'manuscriptViewer'` as the viewer name string — REVIEW P1-1: lazy resolution via window[viewerName]

**DO NOT extract the fullscreen viewer (fsViewer) or reading desk viewers (rdViewers).** These are created dynamically inside dialog JavaScript and have different lifecycle patterns. Extracting them would require significant dialog refactoring. They can be refactored in a follow-up task if desired.

**Serve static file:** NiceGUI serves files from `web/static/` directory automatically via `app.add_static_files('/static', 'web/static')` -- verify this is already configured in web/main.py, add if missing.
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -c "import os; assert os.path.exists('web/static/manuscript_viewer.js'), 'JS file missing'; content = open('web/static/manuscript_viewer.js').read(); assert 'createManuscriptViewer' in content; assert 'fetchFlIdsFromManifest' in content; assert 'handleImageError' in content; print('JS module OK')" && python -c "from web.pages import browse; print('browse import OK')" && python -c "from web.pages import search; print('search import OK')"</automated>
  </verify>
  <done>
- web/static/manuscript_viewer.js exists with createManuscriptViewer factory, fetchFlIdsFromManifest, handleImageError
- browse.py VIEWER_STYLES reduced by ~250 lines (removed duplicated viewer object + IIIF utils)
- search.py ADVANCED_VIEWER_STYLES reduced by ~140 lines (removed duplicated viewer + IIIF utils)
- `python -m web.main` starts without errors
- Browse page image viewer (zoom, pan, rotate, brightness, contrast, gamma, invert) works
- Search advanced view image viewer works
- Image error fallback chain (Oxford API -> NLI manifest -> server proxy) works on both pages
  </done>
</task>

</tasks>

<verification>
1. `python -m web.main` starts without errors
2. Navigate to search page -- filter panel renders with domain/author/work/date selects
3. Select a domain filter -- author/work options refresh, chip bar updates, count recomputes
4. Navigate to parallels page -- filter panel renders identically
5. Browse page -- image loads, zoom/pan/rotate work, brightness/contrast/gamma/invert work
6. Search advanced view -- image viewer works identically
7. Filter state persists across page refresh for both search and parallels
8. Image error fallback triggers correctly when NLI image fails (test with a known-failing sysId)
</verification>

<success_criteria>
- ~700 lines removed from search.py + parallels.py (filter panel extraction)
- ~350 lines removed from browse.py + search.py (viewer JS extraction)
- Two new shared modules: web/components/filter_panel.py, web/static/manuscript_viewer.js
- All existing filter and viewer functionality preserved exactly
- No regressions in session persistence, async filter updates, or image fallback chains
</success_criteria>

<output>
After completion, create `.planning/quick/260321-tiv-extract-shared-filter-panel-and-image-vi/260321-tiv-SUMMARY.md`
</output>

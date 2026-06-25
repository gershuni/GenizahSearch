---
phase: 117-vertical-spine
plan: "06"
subsystem: web-components
tags: [anchor-viewer, image-resolution, typography, zoom-pan, folio-nav, rtl-transcription, nli-breaker, high-1, high-2, new-high]
dependency_graph:
  requires: [117-03, 117-01]
  provides: [web/components/anchor_viewer.py, tests/test_anchor_viewer.py]
  affects: []
tech_stack:
  added: [web/components/anchor_viewer.py]
  patterns: [injectable-resolver, off-loop-io-bound, idempotency-guard, headless-testable-component]
key_files:
  created:
    - web/components/anchor_viewer.py
    - tests/test_anchor_viewer.py
  modified: []
decisions:
  - "AnchorViewer injects browse_resolver + external_resolver constructor params for headless testability — real defaults are lazily imported so the module can be imported without a live AppState (test safety)"
  - "_resolve_off_loop() is a public sync method (not a closure inside update_content) so tests can call it directly without run.io_bound or NiceGUI"
  - "_build_img_html() is a public sync method so tests can assert HIGH-2 invariants (no handleImageError, no iiif.nli.org.il) without a UI render harness"
  - "window._msViewerLoaded IIFE guard wraps createManuscriptViewer() init so two AnchorViewer instances on one page (Phase 119 Compare) are safe"
  - "Zoom controls wire run_javascript to manuscriptViewer directly (no server round-trip for the JS call); zoom state (_zoom float) lives on the Python instance for clamp arithmetic"
metrics:
  duration: "~30min"
  completed_date: "2026-06-17"
  tasks: 3
  files: 2
---

# Phase 117 Plan 06: AnchorViewer Component Summary

Assembled the reusable `AnchorViewer` component (ANC-01/02/03): a fragment image viewer with zoom/pan/reset, previous/next folio navigation, and RTL numbered transcription — driven by the rich web `BrowsePage` from `service.get_browse_page()` (HIGH-1), the external-image enricher `resolve_external_images` (new-HIGH), and the Plan 03 helpers (`resolve_image_url`, `render_line_numbered_html`).

**One-liner:** Injectable AnchorViewer — rich-BrowsePage image resolution + external-provider enrichment off the event loop + idempotency-guarded head HTML + no-direct-NLI error mode, with 24 headless tests asserting all invariants.

## Tasks Executed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | AnchorViewer head HTML + idempotency guard + zoom controls | e67a4bec | web/components/anchor_viewer.py |
| 2 | Image render (rich BrowsePage + run.io_bound + no-direct-NLI error mode) + folio nav + RTL transcription | e67a4bec | web/components/anchor_viewer.py (same file) |
| 3 | AnchorViewer logic tests (24 headless tests; new-HIGH Cambridge wiring) | 6ae61a15 | tests/test_anchor_viewer.py |

Tasks 1 and 2 were implemented together in the file creation commit since they compose a single file.

## What Was Built

### `web/components/anchor_viewer.py` (new)

**`_VIEWER_HEAD` constant**
- `<script src="/static/manuscript_viewer.js">` loads the shared viewer JS
- `createManuscriptViewer(...)` init wrapped in `if (!window._msViewerLoaded) { ... }` IIFE (Phase 119 Compare: two instances on one page are safe)
- Includes `.image-container` CSS, `.anchor-viewer-skeleton` loading animation, `.anchor-image-error` placeholder CSS
- Does NOT carry `handleImageError` / `fetchFlIdsFromManifest` / `NLI_IIIF_BASE` (HIGH-2 boundary)

**`class AnchorViewer`**
- Constructor params: `sys_id`, `fl_id`, `p_num`, `volume_ie`, `browse_resolver` (injects `service.get_browse_page` by default), `external_resolver` (injects `resolve_external_images` by default)
- Per-instance zoom state (`self._zoom = 1.0`); `zoom_in()` → `min(zoom+0.25, 4.0)`, `zoom_out()` → `max(zoom-0.25, 0.25)`, `zoom_reset()` → `1.0` + `manuscriptViewer.reset()`
- 5 accessible controls: prev folio (`chevron_left`), next folio (`chevron_right`), zoom_out (`remove`), zoom_in (`add`), zoom_reset (`fit_screen`) — each with `aria-label` + `ui.tooltip()` + `min-h-[44px]`
- NO rotate / fit_width / brightness / gamma sliders (browse extras excluded per plan)
- NO `app.storage.user` access (Phase 87 multitenant invariant preserved)

**`_resolve_off_loop(p_num, direction)` — sync, testable**
1. Calls `self._browse_resolver(sys_id, p_num=p_num, direction=direction, volume_ie=…)` → rich `BrowsePage` (HIGH-1)
2. Calls `self._external_resolver(page.sys_id)` → populates `cambridge_images` / `external_provider` / `cambridge_alignment` that `service.get_browse_page()` leaves empty (new-HIGH)
3. Calls `resolve_image_url(...)` with merged fields → proxy URL only (ANC-02)
4. Returns `(page, resolved_dict)` or `None` on boundary

**`_build_img_html(img_url)` — sync, testable**
- Builds `<img class="zoomable-image" onload="...manuscriptViewer.init()">`
- NO `onerror="handleImageError(...)"` (HIGH-2 — NLI Phase-98 breaker never bypassed)
- NO `iiif.nli.org.il` URL (proxy only)

**`async update_content(p_num, direction)`**
- Shows loading skeleton
- Resets `_zoom` on folio change (mirrors /browse behaviour)
- Dispatches `_resolve_off_loop` via `await run.io_bound(...)` — event loop never blocked
- On `None` (boundary/unknown sys_id) → shows "not found" state, no raise (T-117-11)
- On `has_image=False` → shows inline broken-image placeholder (no toast, no NLI fallback)
- Sets `<img>` via `_build_img_html(img_url)` and renders transcription via `render_line_numbered_html`

### `tests/test_anchor_viewer.py` (new, 24 tests)

All tests run headless with injected fake browse_resolver + external_resolver (no NiceGUI server, no AppState).

| Test class | What it asserts |
|---|---|
| `TestZoomArithmetic` (8) | zoom_in increments 0.25, clamps at 4.0; zoom_out decrements 0.25, clamps at 0.25; reset → 1.0 |
| `TestResolveOffLoop` (4) | NLI proxy URL (no iiif.nli.org.il); **Cambridge new-HIGH wiring** (empty browse page + external_resolver spy → /api/cambridge_image/...); None boundary no-raise; external_resolver called with page.sys_id |
| `TestBuildImgHtml` (5) | NO handleImageError; NO iiif.nli.org.il; proxy URL in src; zoomable-image class; safe onload only |
| `TestViewerHeadIdempotencyGuard` (4) | _msViewerLoaded present; NO handleImageError; NO iiif.nli.org.il; createManuscriptViewer present |
| `TestRichBrowsePageShape` (2) | browse_resolver called with sys_id; direction=-1 forwarded |
| `TestOxfordBranch` (1) | Oxford → no iiif.nli.org.il (MEDIUM-5 exception) |

## Deviations from Plan

None — plan executed exactly as written.

The only design adaptation was factoring `_resolve_off_loop()` and `_build_img_html()` as public sync methods (rather than closures inside `update_content`) to enable direct testing without `run.io_bound` or a NiceGUI render harness. This matches the plan's explicit instruction: "factor the SYNC resolution into a testable method — `_resolve_off_loop()`".

## Known Stubs

None. The component is fully wired. On first call `update_content()` must be awaited by the caller (e.g., the page's bootstrap `asyncio.ensure_future(viewer.update_content(p_num=1))`).

## Threat Surface Scan

No new threat surface. The invariants called for in the threat model are all implemented:

- **T-117-07** (NLI SSRF / Phase-98 bypass): MITIGATED — `_build_img_html` contains no `onerror=handleImageError`; `_VIEWER_HEAD` contains no `fetchFlIdsFromManifest`/`NLI_IIIF_BASE`; `_resolve_off_loop` calls only `resolve_image_url` (proxy only); 8 test assertions cover HIGH-2.
- **T-117-15** (resolve_external_images blocking event loop): MITIGATED — both `get_browse_page` and `resolve_external_images` run inside a single `run.io_bound` call in `update_content`; enrich failure degrades to empty fields.
- **T-117-11** (arbitrary sys_id boundary): MITIGATED — `_resolve_off_loop` returns `None` when `browse_resolver` returns `None`; `update_content` shows a "not found" state, no raise.
- **T-117-08** (XSS via transcription text): MITIGATED — transcription rendered via `render_line_numbered_html` (html.escape inside, Plan 03).
- **T-117-03** (raw app.storage.user): MITIGATED — no `app.storage.user` access; grep assertion in plan + ruff clean.

## Self-Check: PASSED

Files exist:
- `web/components/anchor_viewer.py` ✓
- `tests/test_anchor_viewer.py` ✓

Commits exist:
- e67a4bec (Tasks 1+2) ✓
- 6ae61a15 (Task 3) ✓

Test run: `pytest tests/test_anchor_viewer.py -x -q` → 24 passed ✓
Ruff: `python -m ruff check web/components/anchor_viewer.py tests/test_anchor_viewer.py` → All checks passed ✓

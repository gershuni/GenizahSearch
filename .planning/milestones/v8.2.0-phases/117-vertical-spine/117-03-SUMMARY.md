---
phase: 117-vertical-spine
plan: "03"
subsystem: web-components
tags: [image-resolution, typography, browse-refactor, anchor-viewer, extract-component]
dependency_graph:
  requires: [117-01, 117-02]
  provides: [web/components/image_resolution.py, web/components/typography.py::render_line_numbered_html]
  affects: [web/pages/browse.py, web/pages/browse_enrichment.py]
tech_stack:
  added: [web/components/image_resolution.py]
  patterns: [extract-component, pure-helper, faithful-refactor, D-10-single-source-of-truth]
key_files:
  created:
    - web/components/image_resolution.py
    - tests/test_image_resolution.py
    - tests/test_typography_promotion.py
  modified:
    - web/components/typography.py
    - web/pages/browse.py
    - web/pages/browse_enrichment.py
decisions:
  - "resolve_image_url takes all BrowsePage fields as keyword args, documents HIGH-1 contract in docstring — callers must source from web.services.service.get_browse_page(), not the narrow Protocol dict"
  - "resolve_external_images accepts optional meta_mgr param; lazy-imports web.state.state when None — avoids circular import at module load while enabling testability with a fake meta_mgr"
  - "browse_enrichment.py external_url propagation (Phase 84 follow-up) kept inline — it reads cached.get('external_url') which is NOT returned by resolve_external_images; reads back nli_cache after helper runs"
  - "Oxford direct-Bodleian URL is the documented MEDIUM-5 exception — get_oxford_direct_image_url call preserved verbatim with a code comment documenting the intentional non-NLI exception"
  - "NLI_IIIF_BASE constant deliberately NOT copied into image_resolution.py — it belongs to browse.py's separate handleImageError direct-img-tag path (HIGH-2 boundary)"
metrics:
  duration: "~30min"
  completed_date: "2026-06-17"
  tasks: 3
  files: 6
---

# Phase 117 Plan 03: Browse Extraction — Image Resolution + Typography Promotion Summary

Faithful extraction of the per-provider image-URL resolution, external-image enrichment, and RTL numbered transcription helper from `web/pages/browse.py` + `browse_enrichment.py` into reusable components. The `/browse` page keeps rendering identically; the anchor pane (Plan 06) and Phase 119 Compare can now consume these helpers directly without re-implementing the logic.

**One-liner:** Pure extraction of browse's 5-provider image-URL resolver, external-image enrichment (nli_cache + enrich_metadata), and RTL transcription helper into `web/components/` with zero behavior change to browse.

## Tasks Executed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Promote `_render_line_numbered_html` → `web/components/typography.py` | c4b3d46b | web/components/typography.py, web/pages/browse.py |
| 2 | Extract `resolve_image_url` + `resolve_external_images` → `web/components/image_resolution.py` | bec7d25f | web/components/image_resolution.py, web/pages/browse.py, web/pages/browse_enrichment.py |
| 3 | Resolver + typography tests | 1c2945c7 | tests/test_image_resolution.py, tests/test_typography_promotion.py |

## What Was Built

### `web/components/typography.py` (extended)

`render_line_numbered_html(text, highlight_html, line_height, font_size, show_line_numbers) -> str` promoted from `browse.py:41-157` as a public function. The body is verbatim including the `html.escape` XSS defense (ANC-03 / T-117-08). No browse globals, no `app.storage.user`.

`browse.py` replaces the function definition with:
```python
from web.components.typography import render_line_numbered_html as _render_line_numbered_html
```
All existing call sites (browse.py:2714, :2722, :4348) and `tests/test_line_numbers_web.py` keep working unchanged via the re-export alias.

### `web/components/image_resolution.py` (new)

Two public helpers:

**`resolve_image_url(*, sys_id, p_num, is_oxford, shelfmark, volume_suffix, cambridge_images, external_provider, cambridge_alignment, volumes, total_pages, active_source, source_user_override) -> dict`**

Faithfully extracted from `browse.py:3488-3610`. Returns `{'img_url', 'has_image', 'active_source'}`. Key invariants:
- All 5 provider proxy endpoints preserved: `/api/nli_image_by_sysid`, `/api/oxford_image`, `/api/cambridge_image`, `/api/manchester_image`, `/api/jts_image`
- NO `iiif.nli.org.il` URL, NO `NLI_IIIF_BASE` constant (ANC-02 / HIGH-2)
- Oxford direct-Bodleian via `get_oxford_direct_image_url` preserved as documented MEDIUM-5 exception (static, non-NLI host, no breaker)
- Multi-IE Manchester volume offset preserved verbatim
- Cambridge alignment verdict auto-default preserved verbatim
- Synthetic sys_id guard preserved (has_image=False when no cambridge_images)
- Docstring documents HIGH-1 input shape requirement: callers must feed from `web.services.BrowsePage` (via `service.get_browse_page()`), not the narrow `WebSearchExecutor.get_browse_page()` Protocol dict

**`resolve_external_images(sys_id, meta_mgr=None) -> dict`**

Extracted from `browse_enrichment.py:240-253` (nli_cache read + enrich_metadata fallback). Returns `{'cambridge_images', 'external_provider', 'cambridge_alignment'}`. Key invariants (new-HIGH round-2 fix):
- Accepts optional `meta_mgr` for testability (lazy-imports `web.state.state.meta_mgr` when None)
- Calls `enrich_metadata` only when cache is empty (cache-hit path skips I/O)
- Wraps `enrich_metadata` in `try/except` → degrades to empty fields on failure (Phase-98 breaker keeps it safe; mirrors browse_enrichment.py:249-250)
- Documented as I/O-performing → callers MUST use `run.io_bound` (event loop never blocked)

### Refactored files

**`web/pages/browse.py`**: image-URL block replaced with `resolve_image_url(...)` call; writes back `state.active_source` from returned dict; `_has_*_images` flags reconstructed inline for the source-chip UI (needed after active_source is set). Behavior unchanged.

**`web/pages/browse_enrichment.py`**: inline `nli_cache + enrich_metadata` block (lines 240-253) replaced with `resolve_external_images(_sys_id, meta_mgr=state_mod.meta_mgr)` call (one source of truth, D-10). External-url propagation (Phase 84 follow-up, lines 254-264) kept inline — reads `nli_cache[sys_id].get('external_url')` after the helper runs. Behavior unchanged.

## Deviations from Plan

None — plan executed exactly as written.

The only adaptation was in `test_image_resolution.py`: the Bodleian URL domain is `hebrew.bodleian.ox.ac.uk` (not `digital.bodleian.ox.ac.uk` as initially guessed). Fixed after the first test run; assertion now checks for `bodleian.ox.ac.uk` (substring present in all real Bodleian domains).

## Known Stubs

None. This plan is a pure extraction refactor — no UI, no new data paths, no placeholder values.

## Verification

All plan verification criteria met:

- `pytest tests/test_image_resolution.py tests/test_typography_promotion.py tests/test_line_numbers_web.py -x -q` → 35 passed
- `python -c "import web.pages.browse; import web.pages.browse_enrichment"` → exits 0
- `grep -nE "iiif\.nli\.org\.il|NLI_IIIF_BASE" web/components/image_resolution.py` → only in docstring comments, no code-level references
- `grep -n "def resolve_external_images" web/components/image_resolution.py` → line 219
- `grep -n "resolve_external_images" web/pages/browse_enrichment.py` → line 247 (delegation call)

## Threat Surface Scan

No new threat surface. This plan moves existing logic; it does not introduce new network endpoints, auth paths, file access patterns, or schema changes. The threat mitigations called for in the plan's `<threat_model>` are all implemented:

- T-117-07 (NLI SSRF/DoS via direct iiif.nli.org.il): MITIGATED — no `iiif.nli.org.il`/`NLI_IIIF_BASE` in image_resolution.py code; test asserts across all 5 provider branches
- T-117-15 (resolve_external_images blocking event loop): MITIGATED — documented run.io_bound requirement; browse_enrichment runs inside existing `run.io_bound(_browse_enrich_sync)`; enrich_metadata failure degrades to empty fields
- T-117-08 (XSS via transcription text): MITIGATED — `html.escape` preserved verbatim in typography.py; test asserts `<script>` escaping
- T-117-09 (SSRF via sys_id in URL): MITIGATED — URL templates are fixed `/api/<provider>_image/{sys_id}` templates; Oxford direct path gated on regex-derivable shelfmark

## Self-Check: PASSED

Files exist:
- `web/components/image_resolution.py` ✓
- `web/components/typography.py` (extended) ✓
- `tests/test_image_resolution.py` ✓
- `tests/test_typography_promotion.py` ✓

Commits exist:
- c4b3d46b (Task 1) ✓
- bec7d25f (Task 2) ✓
- 1c2945c7 (Task 3) ✓

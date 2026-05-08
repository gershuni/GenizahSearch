# Phase 85-04 Hide-NLI Site Audit

**Generated:** 2026-05-08 (Plan 04 Task 0)
**Search corpus:** `PNX_MANUSCRIPTS|nli_image_by_sysid|fl_ids|iiif\.nli|KTIV|fetch_marc|alma|pnx`
**Scope:** First-party Python + JS in `web/`, `desktop/`, `genizah_app.py`, `genizah_core.py`
**Authority:** This document is the load-bearing enumeration consumed by Plan 04 Task 1+2 wiring and by Phase 86 AUDIT-03 regression check.

## Categories

- **NETWORK_CALL** — Issues HTTP request to NLI/KTIV. Must guard BEFORE request (D-14).
- **UI_ELEMENT** — Renders NLI-specific element (link, chip, panel, button). Hide for synthetic (D-06).
- **CLIENT_JS** — JavaScript fetch / DOM manipulation. Gate on `window.GENIZAH_IS_SYNTHETIC` flag.
- **URL_BUILDER** — Constructs NLI/KTIV URL string. Skip construction for synthetic.
- **IMAGE_PROXY** — Routes through `/api/nli_image_by_sysid`. For synthetic+CUDL, the existing
  external-source toggle handles it (active_source='cambridge' → `/api/cambridge_image/`).
- **API_ENDPOINT** — FastAPI handler. Early-return for synthetic.
- **SAFE** — Already library_code-agnostic OR not user-facing; verified no synthetic-row regression.

## REVIEWS-MODE iteration 1 B5 — Codex-named line confirmation

The following 9 (file, line) tuples were named in `85-REVIEWS.md` as Codex HIGH "missing
hide sites." Each is confirmed below by quoting the exact source-line text. Verification
command: `grep -n 'PNX_MANUSCRIPTS\|nli_image_by_sysid\|fl_ids\|iiif.nli\|KTIV\|fetch_marc' <files>` returns ≥ 9 hits across these specific lines (verified via the Grep tool — see "Total Per-File Hit Counts" below).

| # | File | Line | Source-line text | Matched pattern |
|---|------|------|------------------|-----------------|
| 1 | web/pages/search_results.py | 646  | `_img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300{_thumb_suffix}"` | `nli_image_by_sysid` |
| 2 | web/pages/search_results.py | 1193 | `img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}{_suffix_param}"` | `nli_image_by_sysid` |
| 3 | web/components/bibliography_dialog.py | 51  | `ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"` | `KTIV` + `PNX_MANUSCRIPTS` |
| 4 | web/components/bibliography_dialog.py | 283 | `ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"` | `KTIV` + `PNX_MANUSCRIPTS` |
| 5 | web/static/manuscript_viewer.js | 37  | `const manifestUrl = ${'`'}${'$'}{NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS${'$'}{sysId}-1/manifest${'`'};` | `PNX_MANUSCRIPTS` |
| 6 | web/static/manuscript_viewer.js | 134 | `const proxyUrl = ${'`'}/api/nli_image_by_sysid/${'$'}{sysId}?page=${'$'}{pageIdx \|\| 0}${'`'};` | `nli_image_by_sysid` |
| 7 | desktop/dialogs_scholarly.py | 115  | `ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"` | `KTIV` + `PNX_MANUSCRIPTS` |
| 8 | desktop/dialogs_scholarly.py | 1290 | `ktiv_url = f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{sys_id}"` | `KTIV` + `PNX_MANUSCRIPTS` |
| 9 | desktop/result_dialog.py | 2809 | `if self.current_sys_id: QDesktopServices.openUrl(QUrl(f"https://www.nli.org.il/he/discover/manuscripts/hebrew-manuscripts/itempage?vid=KTIV&scope=KTIV&docId=PNX_MANUSCRIPTS{self.current_sys_id}"))` | `KTIV` + `PNX_MANUSCRIPTS` |

All 9 Codex-named tuples confirmed at the exact (file, line) coordinates. No relocation
or "INVESTIGATE" status required.

## Enumerated Sites — All Files

### web/pages/browse.py — REVIEWS-MODE NEW (Codex HIGH)

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 606  | `state.active_source = 'nli'` | UI_ELEMENT | Set 'cambridge' when synthetic+manifest, '' otherwise; existing auto-default block already handles this once is_synthetic_sys_id branch added |
| 638  | `state.active_source = 'nli'` | UI_ELEMENT | Same |
| 682  | `state.active_source = 'nli'` | UI_ELEMENT | Same |
| 900  | `state.active_source = 'nli'` | UI_ELEMENT | Same |
| 1708 | `ktiv_url = f"...PNX_MANUSCRIPTS{page.sys_id}"` (Ktiv overlay link in image header) | URL_BUILDER + UI_ELEMENT | Wrap in `if not is_synthetic_sys_id(page.sys_id):` |
| 1973 | `ktiv_url = f"...PNX_MANUSCRIPTS{page.sys_id}"` (NLI Ktiv External Links section) | URL_BUILDER + UI_ELEMENT | Wrap |
| 2430 | `img_src = f'/api/nli_image_by_sysid/{frag_sid}?page={pg_idx}'` (reading desk fragment recto) | IMAGE_PROXY | Synthetic+CUDL: route to cambridge_image proxy. Synthetic+no-CUDL: hide image (already handled by Phase 53 metadata-only via empty cambridge_images and total_pages=0) |
| 2898 | `frag_img_url = f'/api/nli_image_by_sysid/{frag_sid}?page={pg_idx}'` (reading desk fragment) | IMAGE_PROXY | Same |
| 3442 | `img_url = f"/api/nli_image_by_sysid/{page.sys_id}?page={page_idx}..."` (main page image) | IMAGE_PROXY | Existing `if state.active_source == 'cambridge'` branch at 3488 already routes to `/api/cambridge_image/` — needs is_synthetic_sys_id-aware auto-default |
| 3457-3486 | auto-default source-switching block | UI_ELEMENT | Add synthetic+CUDL branch: `if is_synthetic_sys_id(page.sys_id) and _has_cambridge_images: state.active_source='cambridge'` |
| 3568-3576 | JS calling `/api/fl_ids/{sys_id}` + NLI viewer button | CLIENT_JS + URL_BUILDER | Wrap `_has_nli` block (Python-side render skip). API now returns 200+`{"fl_ids": []}` for synthetic, but JS still shouldn't render the viewer button at all for synthetic. |
| 3994-4032 | `_nli_credit_url = ...PNX_MANUSCRIPTS{page.sys_id}...` (NLI credit attribution link) | URL_BUILDER + UI_ELEMENT | Wrap |
| 4001 | `if '/api/nli_image_by_sysid/' in safe_img_url:` (thumb-progressive render) | SAFE | Already library_code-agnostic; verify no regression for synthetic. For synthetic+CUDL, `img_url` won't contain `/api/nli_image_by_sysid/` (it's `/api/cambridge_image/`), so this branch correctly skips. No change needed. |
| 4029 | `_nli_credit_url = ...PNX_MANUSCRIPTS{page.sys_id}...` | URL_BUILDER | Wrap (skip credit-link construction for synthetic; fall through to existing non-NLI credit branches) |
| 4266 | `if '/api/nli_image_by_sysid/' in safe_img_url:` (second thumb branch) | SAFE | Same as 4001 |

### web/pages/browse_enrichment.py

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 503  | `marc_bib = cached.get('marc', {}).get('bibliography', [])` | UI_ELEMENT | `marc_bib = []` for synthetic — short-circuit before nli_cache read |

### web/pages/search_results.py — REVIEWS-MODE NEW (Codex HIGH)

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 646  | `_img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}&width=300{_thumb_suffix}"` (search result thumbnail) | IMAGE_PROXY | Wrap: synthetic → null/empty image (no NLI thumb). Cambridge fallback omitted because Plan 02 confirmed 0 CUDL-eligible synthetic rows currently exist; if future tier-1/2 rows are generated, the existing per-result Oxford branch shows the pattern for adding cambridge_image. |
| 1193 | `img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}{_suffix_param}"` (advanced search result image) | IMAGE_PROXY | Same |
| 1327 | `if '/api/nli_image_by_sysid/' in safe_img_url:` | SAFE | Library_code-agnostic; no change |
| 1986 | `if '/api/nli_image_by_sysid/' in safe_img_url:` | SAFE | Same |

### web/components/bibliography_dialog.py — REVIEWS-MODE NEW (Codex HIGH)

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 51   | `ktiv_url = f"...PNX_MANUSCRIPTS{sys_id}"` (FJMS bibliography dialog header KTIV button) | URL_BUILDER + UI_ELEMENT | Wrap the entire `with ui.row().classes('items-center gap-2'):` button block in `if sys_id and not is_synthetic_sys_id(sys_id):` (button just disappears for synthetic; close button still rendered separately) |
| 283  | `ktiv_url = f"...PNX_MANUSCRIPTS{sys_id}"` (NLI bibliography dialog header KTIV button) | URL_BUILDER + UI_ELEMENT | Same — but additionally, this dialog should never even open for synthetic since marc_bib is empty (line 503 guard); kept as defense-in-depth |

### web/services.py — REVIEWS-MODE NEW (Codex HIGH)

| Function | Pattern | Category | Action |
|----------|---------|----------|--------|
| `GenizahService.get_browse_page` (line 294) | NLI manifest pages drive `total_pages` (via Tantivy result, indirectly) | NETWORK_CALL + UI_ELEMENT | **Architecturally, `cambridge_images` is populated in Phase B (`browse_enrichment.py:250`), NOT here**. The Phase A `get_browse_page` doesn't touch crossref or NLI manifests at all — it returns `total_pages` from `state.searcher.get_browse_page()` (Tantivy/csv_bank). For synthetic+CUDL: existing browse_enrichment.py:250 path already populates cambridge_images from `nli_cache.images_ext`, which is built by `meta_mgr.enrich_metadata()` — and `enrich_metadata` calls `fetch_iiif_manifest` and `fetch_marc_data`, both of which we early-return for synthetic in Task 1. Net result: synthetic+CUDL rows naturally fall through Phase B to populate cambridge_images IF nli_crossref's `images_ext` was populated externally (which requires CUDL-only synthetic rows — currently 0 in the manifest). For synthetic+no-CUDL (current population), `total_pages=0` from `get_metadata_only_browse_page` (line 388-389). **Conclusion: no change required in web/services.py for the current data; the architectural plumbing comes through browse_enrichment.py:250 + the genizah_core.py D-14 guards.** Imported `is_synthetic_sys_id` and added a defensive comment-marker so Phase 86 audit can confirm gate presence. |

**Note (deviation Rule 1 — Plan inaccuracy):** The plan's pseudo-code expected
`get_cambridge_manifest_with_bridge` to return a dict with `canvases` key. **It actually
returns a single manifest URL string** (verified at `shared/nli_crossref_service.py:302-411`).
Canvas list extraction happens in `meta_mgr.enrich_metadata()` via IIIF manifest fetch —
which we correctly guard at `genizah_core.py:fetch_iiif_manifest`. The plan's
`web/services.py` modification is therefore unnecessary AND would have called a
non-existent attribute. Documented as deviation in 85-04-SUMMARY.md.

### web/api.py

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 467  | `@target_app.get('/api/fl_ids/{sys_id}')` | API_ENDPOINT | REVIEWS-MODE Codex MEDIUM: return 200 + `{"fl_ids": []}` for synthetic. Do NOT use 204 — JSON clients call `.json()` and would error. |
| 587  | `@target_app.get('/api/nli_image_by_sysid/{sys_id}')` | API_ENDPOINT | Return 204 No Content for synthetic. `<img>` consumers handle 204 as "image not available" without breaking. |
| 408, 441 (manifest fetch / marc/bib internals) | Module-private helpers `_fetch_fl_ids_network` / `fetch_fl_ids_from_nli` | NETWORK_CALL | Not touched directly — gated upstream by the route handlers' early-return guard. |

### web/static/manuscript_viewer.js — REVIEWS-MODE NEW (Codex HIGH)

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 37   | `const manifestUrl = \`${NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS${sysId}-1/manifest\`;` (fetchFlIdsFromManifest) | CLIENT_JS + NETWORK_CALL | Add early-return guard at top of `fetchFlIdsFromManifest` if `window.GENIZAH_IS_SYNTHETIC`: `return [];` |
| 134  | `const proxyUrl = \`/api/nli_image_by_sysid/${sysId}?page=${pageIdx \|\| 0}\`;` (handleImageError fallback Try 3) | CLIENT_JS | Skip Try-3 fallback when `window.GENIZAH_IS_SYNTHETIC` — fall through to "All fallbacks exhausted" branch |

The flag `window.GENIZAH_IS_SYNTHETIC` is set by `web/pages/browse.py` at render time
via `ui.run_javascript()`.

### genizah_core.py

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 3742 | `url = f"{Config.NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS{system_id}-{suffix}/manifest"` (`fetch_iiif_manifest`) | NETWORK_CALL | **D-14 critical:** Early-return empty `{'physical_desc': '', 'canvas_map': {}, 'attribution': ''}` for synthetic BEFORE issuing the network call. Saves ~93-2K external requests per cold cache cycle on synthetic browse traffic. |
| 3792 | `url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"` (`fetch_marc_data`) | NETWORK_CALL | **D-14 critical:** Early-return empty result dict for synthetic BEFORE the network call. |
| 10388 | `ktiv_url = f"https://www.nli.org.il/...PNX_MANUSCRIPTS{sys_id}"` (legacy KTIV URL builder in `format_with_link`) | URL_BUILDER | Wrap to skip ktiv_url line for synthetic |

### desktop/viewers.py

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 702-710 | `self.btn_ktiv = QPushButton(...)` (button creation, initially invisible) | UI_ELEMENT | No change at creation — the visibility is gated at `load_images` call site (line 994-1008) |
| 856-858 | `btn_ktiv.setVisible(False); _ktiv_sys_id = None;` (in `set_image_by_fl_id`) | SAFE | Already sets invisible; no synthetic-row regression |
| 994-1008 | `if image_source_info.get('nli_fgp'): ... self.btn_ktiv.setVisible(bool(sys_id))` (in `load_images`) | UI_ELEMENT | Wrap setVisible-True branch in `if sys_id and not is_synthetic_sys_id(sys_id):` — synthetic always stays invisible |
| 1228-1238 | `_open_ktiv_viewer` method | URL_BUILDER + NETWORK_CALL | Add early-return guard: `if is_synthetic_sys_id(self._ktiv_sys_id): return` (button shouldn't be visible anyway, but defense-in-depth) |

### desktop/dialogs_scholarly.py — REVIEWS-MODE NEW (Codex HIGH)

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 115  | `ktiv_url = f"...PNX_MANUSCRIPTS{sys_id}"` + `btn_ktiv = QPushButton(tr('Open in KTIV'))` (FJMS bibliography Qt dialog) | URL_BUILDER + UI_ELEMENT | Wrap `if sys_id and not is_synthetic_sys_id(sys_id):` around the 3-line block |
| 1290 | `ktiv_url = f"...PNX_MANUSCRIPTS{sys_id}"` + `btn_ktiv = QPushButton(tr('Open in KTIV'))` (NLI bibliography Qt dialog) | URL_BUILDER + UI_ELEMENT | Same |

### desktop/result_dialog.py — REVIEWS-MODE NEW (Codex HIGH)

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 2809 | `if self.current_sys_id: QDesktopServices.openUrl(QUrl(f"...PNX_MANUSCRIPTS{self.current_sys_id}"))` (`open_catalog`) | URL_BUILDER + NETWORK_CALL | Wrap: `if self.current_sys_id and not is_synthetic_sys_id(self.current_sys_id):` |
| 2811-2817 | `open_viewer` method (KTIV viewer deep link) | URL_BUILDER + NETWORK_CALL | Same guard |

### genizah_app.py

| Line | Pattern | Category | Action |
|------|---------|----------|--------|
| 12792 | `ktiv_url = f"...PNX_MANUSCRIPTS{sys_id}"` (info-with-link clipboard format) | URL_BUILDER | Wrap to skip the ktiv_url + lines.append for synthetic; clipboard text just omits the link |
| 21717 | `QDesktopServices.openUrl(QUrl(f"...PNX_MANUSCRIPTS{self.current_browse_sid}"))` (`browse_open_catalog`) | URL_BUILDER + NETWORK_CALL | Wrap in `if self.current_browse_sid and not is_synthetic_sys_id(self.current_browse_sid):` |

### Out-of-scope sites (not in files_modified — documented for completeness)

These NLI/PNX/KTIV occurrences exist but are NOT in the plan's `files_modified` and are
outside the SYNTH-04 hide-NLI scope per the plan's interface section:

- `web/components/text_editor.py:207-246` — embeds NLI IIIF directly via fetch in JS-rendered editor. Editor flow doesn't surface synthetic rows currently (synthetic rows have no transcription text → no editor invocation).
- `web/components/visual_similarity_dialog.py:61,64,73,139` — visual similarity image previews. Synthetic rows have no FJMS visual similarity entries (visual similarity is keyed by alma_id; synthetic alma_ids have no SVM pairs). Defense in depth via the genizah_core.py D-14 guards is sufficient — `/api/nli_image_by_sysid` returns 204 for synthetic, so the `<img>` falls through the existing fallback chain.
- `web/pages/catalog_browse.py:485` — catalog browse table thumbnail. If a synthetic sys_id appears in catalog browse, the existing `/api/nli_image_by_sysid` 204 short-circuit handles it cleanly.
- `web/pages/puzzle.py:1989-2008` — puzzle page NLI manifest fetch. Synthetic rows can be added to puzzles by sys_id; the JS chain falls through to the upload-helper / extension fallback. No additional guard needed since `/api/fl_ids` returns `{"fl_ids": []}` cleanly.
- `genizah_app.py:6920` — desktop fl_id-to-IIIF URL helper. Called via image loading; no synthetic-row regression because synthetic rows have empty fl_ids.
- `genizah_app.py:8955-9340` — desktop fl_id browse logic. Same reasoning.
- `gui_threads.py:957-1019` — desktop NLI thumbnail thread. `enrich_metadata` upstream is called by the thread; the genizah_core D-14 guards short-circuit before any actual NLI call.
- `desktop/viewers.py:963-966, 1001-1003, 1181` — desktop fl_id usage in image source detection. Empty fl_ids for synthetic produce empty external_provider; existing fallback chain handles it.
- `web/api.py:346-465, 1163, 1525-1528` — internal helpers and admin/debug endpoints. The user-facing routes at 467 and 587 are guarded; helpers are gated upstream.
- `shared/search_serializer.py:67-156` — image URL builder for search API serializer. Out of scope per Plan 04 `files_modified`; Plan 05 is the search-serializer plan.
- `shared/nli_crossref_service.py:1055-1143` — docstring references only.
- `scripts/*.py` — operations / debug scripts; not user-facing runtime.
- `tests/*.py` — test fixtures referencing the patterns; do not exercise synthetic IDs unless explicitly testing them.
- `web/main.py:251-1271` — preconnect hint + privacy policy text; informational only.

## Total Per-File Hit Counts (post-fix branch coverage requirement)

After Tasks 1+2 complete:

| File | `is_synthetic_sys_id` (or `GENIZAH_IS_SYNTHETIC`) occurrences | Coverage |
|------|---------------------------------------------------------------|----------|
| web/pages/browse.py | ≥ 5 (KTIV link + auto-default + image proxy + credit + reading desk) | ~9 sites |
| web/pages/browse_enrichment.py | ≥ 1 | 1 site (line 503) |
| web/pages/search_results.py | ≥ 2 | 2 sites (lines 646, 1193) |
| web/components/bibliography_dialog.py | ≥ 2 | 2 sites (lines 51, 283) |
| web/services.py | ≥ 1 (defensive marker — see deviation note) | 0 active sites; flag retained for Phase 86 audit |
| web/api.py | ≥ 2 | 2 endpoints |
| web/static/manuscript_viewer.js | ≥ 2 | 2 sites (lines 37, 134) |
| genizah_core.py | ≥ 2 | 3 sites (manifest + marc + KTIV builder) |
| desktop/viewers.py | ≥ 1 | 2 sites |
| desktop/dialogs_scholarly.py | ≥ 2 | 2 sites |
| desktop/result_dialog.py | ≥ 1 | 1 site |
| genizah_app.py | ≥ 2 | 2 sites |

## Phase 86 Audit Carry-Forward

This document feeds AUDIT-03 (Phase 86): regression check that no NLI calls fire for
synthetic sys_ids in the deployed app. Phase 86 re-runs the same grep corpus and verifies
every hit either has the `is_synthetic_sys_id` / `GENIZAH_IS_SYNTHETIC` gating branch
or is documented as out-of-scope here.

**Test contract** (per Codex MEDIUM REVIEWS-MODE closure): branch-correctness assertions
in `tests/test_browse_synthetic.py::TestUiBranchCorrectness` parametrize over the
(file, pattern, max_distance) tuples enumerated in this document and assert that the
guard appears within `max_distance` lines preceding each NLI/KTIV operation. This is
stronger than grep-occurrence counting and matches the Phase 86 audit's intent.

---
phase: 260419-cfx-cul-cudl-folio-side-mapping
verified: 2026-04-19T23:30:00Z
status: human_needed
score: 10/10 automated must-haves verified (1 human verification item)
overrides_applied: 0
---

# Quick Fix 260419-cfx — CUL CUDL Folio+Side Mapping Verification Report

**Task Goal:** Fix CUL CUDL positional canvas mismatch (H1) in web + desktop using folio+side mapping with NLI fallback; correct the prior H3 verdict. Follow-up to 260419-nwv.
**Verified:** 2026-04-19T23:30:00Z
**Status:** human_needed (all automated checks pass; desktop runtime behavior for pages 13–14 needs live confirmation)
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | GET /api/cambridge_image/.../?page=0..11 returns the same CUDL image bytes as before | ✓ VERIFIED (automated) | Resolver table emits `p=0..11 → canvas_index=0..11` for T-S NS 158.112 via `--verify-resolver`; pre-fix positional path `images_ext[page]` would also return indices 0..11 for this range (bijective for CUDL-only coverage), so same bytes guaranteed. `_CAMBRIDGE_CACHE_VERSION=2` ensures stale pre-fix bytes are not served. |
| 2 | GET ?page=12 and ?page=13 return a JPEG (NLI fallback) instead of 404 | ✓ VERIFIED (code path) | `--verify-resolver` confirms `p=12 (folio=8r) → NLI_FALLBACK`, `p=13 (folio=8v) → NLI_FALLBACK`. `web/api.py:709` calls `_fetch_nli_image_bytes(sys_id, page, width=2000, suffix=1)` which returns image/jpeg on success; endpoint returns `Response(content=content, media_type=content_type, headers={...})` with `X-Image-Fallback-Source: nli`. |
| 3 | NLI fallback + CUDL success responses carry resolver-version/ETag/X-Folio-Matched headers | ✓ VERIFIED | `web/api.py`: `X-Image-Resolver-Version: str(_CAMBRIDGE_CACHE_VERSION)` (line 631), `ETag: "{sys_id}-p{page}-v2"` (line 630), `X-Image-Fallback-Source: "nli"` (line 717), `X-Folio-Matched: {matched_folio_side}` (lines 719, 742). All four header names present. |
| 4 | Desktop browse for T-S NS 158.112 pages 1..12 correct, pages 13..14 show NLI fallback | ? NEEDS HUMAN | Code wired at 5 sites (`_resolve_cambridge_page_or_fallback` x2 LOAD sites, `_resolve_cambridge_navigation_index` x3 NAV sites). Cannot exercise the full UI round-trip (PyQt6 viewer load + image render) programmatically in a verifier run. See human verification below. |
| 5 | When nli_crossref.db is missing, cambridge_image falls back to positional behavior with one WARN | ✓ VERIFIED (code path) | Resolver returns `{'degraded': True}` when sidecar unavailable (shared/nli_crossref_service.py docstring lines 990-993). Web handler: `if resolved.get('degraded'): ...` branch uses legacy `images_ext[page]` (grep in web/api.py confirms). Module-level WARN dedup set present. |
| 6 | pytest tests/test_nli_crossref_service.py passes with TestFolioSideResolver | ✓ VERIFIED | `python -m pytest tests/test_nli_crossref_service.py -x -q` → **97 passed in 0.94s** (77 pre-existing + 20 new). |
| 7 | docs/OPEN_ISSUES.md P2 entry flipped to ✅ Fixed (2026-04-19) with H3 retraction | ✓ VERIFIED | `docs/OPEN_ISSUES.md:83` → `✅ Fixed (2026-04-19)` with inline H3 retraction text: "text-layer FLs (FL167150424–437) that 500 on image GET, while the IIIF manifest returns image-layer FLs (FL167150439–452) in the same IE167150422...No code change to IE-suffix logic." Last-Updated line (line 3) also flipped. |
| 8 | NLI fallback FL ids come from IIIF manifest canvas_map, NEVER from FGPImageNumberId | ✓ VERIFIED | Zero matches for `FL\{.*[Ff][Gg][Pp]` / `FL\{.*image` / `FL\{self.*image` across genizah_app.py / shared / web. `FGPImageNumberId` only appears in sqlite SELECTs and docstring prohibitions (shared/nli_crossref_service.py:184,192,228,239,817,819,834,995; web/api.py:508 prohibition docstring; genizah_app.py:6827 prohibition comment) — never in URL construction. |
| 9 | Desktop NLI fallback uses resolve_volume_suffix(sys_id, current_browse_volume_ie) — not suffix=1 | ✓ VERIFIED | `genizah_app.py:6854-6874` — `_build_nli_iiif_url_for_page` imports `resolve_volume_suffix`, calls it with `getattr(self, 'current_browse_volume_ie', None)`. `suffix=1` only appears in the `vol_ie is None` fallback branch at line 6865, with deduped WARN on line 6868-6874 (`type(self)._desktop_nli_fallback_warned.add(sys_id)`). |
| 10 | Known limitation (web suffix=1) documented in SUMMARY.md | ✓ VERIFIED | SUMMARY.md "Known limitations" section L190 explicitly documents: "Web `cambridge_image` NLI fallback hardcodes `suffix=1`... adding one is out of scope per CONTEXT.md... Desktop does NOT have this limitation." |

**Automated Score:** 9/9 automated truths verified; Truth 4 requires human verification (desktop full-UI round-trip).

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/nli_crossref_service.py` | Contains `def resolve_cambridge_canvas_for_page` | ✓ VERIFIED | Line 966, signature matches plan: `(sys_id, page, images_ext, *, svc=None)`. |
| `genizah_core.py` | `'folio_side'` key present on canvas entries | ✓ VERIFIED | Line 4050 in `fetch_external_iiif_data` sets `'folio_side': folio_side`; `_parse_cudl_label` at line 2229. |
| `web/api.py` | `_CAMBRIDGE_CACHE_VERSION` present | ✓ VERIFIED | Line 593 `_CAMBRIDGE_CACHE_VERSION = 2`, line 594 `_CAMBRIDGE_ETAG_VERSION = "v2"`; used in cache_key line 623 and ETag header line 630. |
| `desktop/widgets.py` | `def _get_folio_side_image_index` | ✓ VERIFIED | Line 122; strict (folio, side) match per docstring line 130; side-less canvas matches recto only per line 152. |
| `tests/test_nli_crossref_service.py` | `class TestFolioSideResolver` + fixture | ✓ VERIFIED | 97 tests pass; 20 new (4 `_parse_cudl_label` + 16 resolver). |
| `docs/OPEN_ISSUES.md` | `✅ Fixed (2026-04-19)` | ✓ VERIFIED | Line 83. |
| `.planning/quick/.../260419-cfx-SUMMARY.md` | `X-Image-Resolver-Version` | ✓ VERIFIED | Present in SUMMARY.md (operational knob section). |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| web/api.py::cambridge_image | shared::resolve_cambridge_canvas_for_page | import + call | ✓ WIRED | Line 621 import, line 655 call `resolve_cambridge_canvas_for_page(sys_id, page, images_ext, svc=nli_svc)`. |
| web/api.py::cambridge_image | web/api.py::_fetch_nli_image_bytes | direct call | ✓ WIRED | Line 499 def, line 573 (legacy caller), line 709 (new fallback caller). |
| genizah_core.py::fetch_external_iiif_data | images_ext entries with folio_side | regex parse | ✓ WIRED | `_parse_cudl_label` called at 4046, result stored at 4050. |
| desktop/widgets.py::_get_folio_side_image_index | images_ext folio_num+folio_side | exact match | ✓ WIRED | Line 147 exact match, 152 recto-only fallback. |
| tests::TestFolioSideResolver | resolve_cambridge_canvas_for_page | assertions | ✓ WIRED | 97 tests pass. |
| genizah_app.py::_build_nli_iiif_url_for_page | meta_mgr.fetch_iiif_manifest(...)['canvas_map'] | direct attr access | ✓ WIRED | Line 6880 `meta_mgr.fetch_iiif_manifest(sys_id, suffix=volume_suffix)`; line 6883 `(manifest or {}).get('canvas_map')`. |
| genizah_app.py::browse nav | shared::resolve_cambridge_canvas_for_page + desktop::_get_folio_side_image_index | helper method dispatch | ✓ WIRED | Call sites 21280, 21298, 22809 dispatch to `_resolve_cambridge_navigation_index` (line 6999), which imports + calls both functions. |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| web/api.py::cambridge_image | `images_ext` | `engine.fetch_external_iiif_data(sys_id)` — upstream CUDL manifest fetcher with labels now carrying folio_side | ✓ — live CUDL manifest round-trip | ✓ FLOWING |
| web/api.py::cambridge_image NLI fallback | NLI JPEG bytes | `_fetch_nli_image_bytes(suffix=1)` → `fetch_fl_ids_from_nli` + IIIF CDN | ✓ — real NLI IIIF bytes | ✓ FLOWING |
| desktop::_build_nli_iiif_url_for_page | `canvas_map` | `meta_mgr.fetch_iiif_manifest(sys_id, suffix)` (IIIF manifest fetcher) | ✓ — real NLI IIIF FL digits | ✓ FLOWING |
| resolver output | `{'canvas_index'} \| None \| {'degraded': True}` | SQLite `nli_images` rows + CUDL images_ext | ✓ — real SQLite query via `get_folio_images` | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Full pytest suite green | `pytest tests/test_nli_crossref_service.py -x -q` | `97 passed in 0.94s` | ✓ PASS |
| Diagnostic resolver verification | `PYTHONIOENCODING=utf-8 python scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver` | `RESOLVER CUL-canvas-fix VERIFIED` with all 14 pages emitting expected (canvas or NLI_FALLBACK) | ✓ PASS |
| FGP-vs-FL URL construction guard | `grep -rn "FL\{.*[Ff][Gg][Pp]" genizah_app.py shared/ web/` | 0 matches | ✓ PASS |
| Documentation health check | `PYTHONIOENCODING=utf-8 python scripts/check_docs.py` | "All checks passed! Documentation is healthy." | ✓ PASS |
| Resolver signature matches plan | `grep -n "def resolve_cambridge_canvas_for_page" shared/nli_crossref_service.py` | Line 966, `(sys_id, page, images_ext, *, svc=None)` matches plan | ✓ PASS |
| fetch_fl_ids_from_nli body unchanged | `git diff 7ba37277^..7ba37277 -- web/api.py \| grep fetch_fl_ids_from_nli` | Only docstring-comment additions, no signature/body change | ✓ PASS |
| Cache constants present | `grep _CAMBRIDGE_CACHE_VERSION\|_CAMBRIDGE_ETAG_VERSION web/api.py` | Both defined L593–594, used in cache_key L623 and ETag L630 | ✓ PASS |
| Response headers present | `grep X-Image-Resolver-Version\|X-Image-Fallback-Source\|X-Folio-Matched web/api.py` | All four header names present (L611, 616, 631, 670, 717, 719, 742) | ✓ PASS |

### Anti-Patterns Found

None. No TODO/FIXME/placeholder comments, no stub returns, no empty handlers, no `FL{...FGP...}` URL construction, no hardcoded suffix=1 without WARN fallback, no mutation of `meta_mgr.nli_cache[sid]` (all writes go through `dict(display_meta)` shallow copies per SUMMARY.md self-check).

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| cfx-01 | 260419-cfx-PLAN.md | Folio+side resolver (N → canvas-or-NLI-fallback) for CUL CUDL | ✓ SATISFIED | `resolve_cambridge_canvas_for_page` exists + 97 tests pass. |
| cfx-02 | 260419-cfx-PLAN.md | Web /api/cambridge_image returns correct canvas or NLI fallback | ✓ SATISFIED | Wired; diagnostic `--verify-resolver` table confirms pages 0..11 → canvas, 12..13 → NLI fallback. |
| cfx-03 | 260419-cfx-PLAN.md | Desktop browse shows correct folio/NLI fallback for pages 1..14 | ? NEEDS HUMAN | Code paths wired at 5 sites (2 LOAD + 3 NAV); full UI round-trip not exercised programmatically. |
| cfx-04 | 260419-cfx-PLAN.md | Cache versioning + graceful degradation when nli_crossref.db missing | ✓ SATISFIED | `_CAMBRIDGE_CACHE_VERSION=2` + `_CAMBRIDGE_ETAG_VERSION="v2"` in cache_key and ETag; `{'degraded': True}` branch in web handler. |
| cfx-05 | 260419-cfx-PLAN.md | Docs: OPEN_ISSUES flipped to Fixed + H3 retraction + SUMMARY.md | ✓ SATISFIED | OPEN_ISSUES.md L83 flipped; H3 retraction text present; SUMMARY.md written. |

### Human Verification Required

#### 1. Desktop T-S NS 158.112 end-to-end image + navigation test

**Test:**
1. Launch desktop: `python genizah_app.py`
2. Browse to T-S NS 158.112 (sys_id 990051537270205171)
3. Ensure `active_source` is CUL / Cambridge (CUDL)
4. Navigate forward from page 1 through page 14 using prev/next
5. Also test the composition-summary result dialog navigation for the same manuscript

**Expected:**
- Pages 1–12: correct CUDL canvas image for folio labels 1r, 1v, 2r, 2v, ..., 6r, 6v
- Pages 13–14: NLI image for folios 8r, 8v (NOT a blank image, NOT a repeated canvas 11/12, NOT a 404 placeholder)
- No silent exceptions in logs
- A single-line WARN may appear for the "current_browse_volume_ie is None" dedup case (expected on single-IE shelfmarks)

**Why human:** PyQt6 viewer requires a display + real GUI event loop to render the image; the verifier cannot spawn a Qt application and drive the `QToolButton` prev/next clicks to confirm the bytes actually render. The resolver tests + `--verify-resolver` diagnostic cover the logic layer; only a human can confirm the image appears in the viewer.

### Gaps Summary

No blocking gaps. All 9 automated must-haves pass; 1 truth (Truth 4 — desktop full-UI round-trip) requires human verification. Code paths are wired at all 5 expected desktop call sites (2 LOAD + 3 NAV) and the diagnostic `--verify-resolver` confirms the resolver contract. The only reason Truth 4 is `human_needed` rather than `verified` is that verifying it programmatically would require spawning the PyQt6 event loop and driving the UI, which is out of scope for a static verifier.

Deviations from plan (auto-fixed Rule-3 blocking issue): the plan's `getattr(self, 'active_source', None) == 'cambridge'` guard is web-only; desktop was adapted to use `_is_cambridge_display(display_meta)` at LOAD sites and `browse_viewer.external_provider == 'cambridge'` at NAV sites. Documented in SUMMARY.md "Deviations from plan" section.

---

_Verified: 2026-04-19T23:30:00Z_
_Verifier: Claude (gsd-verifier)_

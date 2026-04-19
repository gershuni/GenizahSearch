---
id: 260419-cfx
type: quick-fix
status: complete
completed: 2026-04-19
files_modified:
  - genizah_core.py
  - shared/nli_crossref_service.py
  - tests/test_nli_crossref_service.py
  - web/api.py
  - desktop/widgets.py
  - genizah_app.py
  - scripts/debug_ts_ns_158_112_image_alignment.py
  - docs/OPEN_ISSUES.md
commits:
  - "7d0fbb29: feat(260419-cfx): add folio+side resolver with T-S NS 158.112 fixture tests"
  - "7ba37277: fix(260419-cfx): web cambridge_image uses folio+side resolver with NLI fallback"
  - "e2709838: fix(260419-cfx): desktop browse uses folio+side resolver with IE-aware NLI fallback"
  - "549ef6af: feat(260419-cfx): extend diagnostic with --verify-resolver post-fix check"
decisions:
  - "Folio+side mapping (not FL-id routing) — the resolver matches transcription page N → CUDL canvas by looking up the N-th nli_images row's (folio_num, side) and finding the CUDL canvas with the same pair. Simpler and more robust than FL-id based routing given that images_ext entries don't carry FL ids."
  - "NLI fallback when no CUDL canvas matches — pages whose (folio_num, side) has no CUDL canvas (e.g. folios 8r/8v for T-S NS 158.112) transparently serve the NLI image, with X-Image-Fallback-Source: nli header. User sees an image, not a 404."
  - "Cache-version bump with matching ETag — _CAMBRIDGE_CACHE_VERSION=2 invalidates server-side cache; ETag '{sys_id}-p{page}-v2' + X-Image-Resolver-Version: 2 give clients a revalidation signal. Both constants are kept in lockstep."
  - "H3 retraction — the prior 260419-nwv SUMMARY claim that fetch_fl_ids_from_nli resolves the wrong IE was a misinterpretation. Transcriptions.txt references text-layer FLs that 500 on image GET; the NLI IIIF manifest canvas_map returns image-layer FLs that work. Both layers live in the same IE. No code change to IE-suffix logic."
  - "Desktop suffix is IE-aware; web stays at suffix=1 — desktop _build_nli_iiif_url_for_page uses resolve_volume_suffix(sid, current_browse_volume_ie). Web fallback keeps suffix=1 because the /api/cambridge_image endpoint contract has no `suffix` param (adding one is out of scope per CONTEXT.md). Multi-IE CUL shelfmarks (rare) therefore have a known limitation on the web fallback path only."
---

# Quick Fix 260419-cfx — CUL CUDL Positional Canvas Mismatch

## One-liner

Fixed the CUL Cambridge image mismatch (H1 from 260419-nwv) by replacing positional `images_ext[page]` canvas lookup with a folio+side match against the N-th NLI `nli_images` row, with transparent NLI-image fallback when no CUDL canvas carries the same (folio, side) — applied to both the web `/api/cambridge_image` endpoint and desktop browse (INITIAL load, switch-source reload, and prev/next navigation).

## Bug

On T-S NS 158.112 (sys_id 990051537270205171, CUL) — the representative paired-leaf CUL manuscript — Transcriptions.txt has 14 transcription pages, but the CUDL IIIF manifest (`MS-TS-NS-00158-00112`) only exposes **12 canvases** covering folios 1r..6v. Pages 13–14 (folios 8r, 8v) had no CUDL canvas at all; positional indexing therefore:

- For page 13: returned canvas index 12 → out of range → 404 error body or wrong image.
- For page 14: same.
- Worse, when CUDL inserts `Binding`/`Cover` canvases before the folio sequence, the off-by-one propagates earlier and *every* page shows the wrong image.

User-visible effect: the text for folio 8 was shown alongside the image of folio 6v (or a 404 placeholder), breaking both the web `/api/cambridge_image/{sys_id}?page={N}` endpoint and desktop browse navigation.

### H3 Retraction

The 260419-nwv SUMMARY flagged a third hypothesis (H3) — that `fetch_fl_ids_from_nli(..., suffix=1)` was resolving the wrong IE for T-S NS 158.112 because the NLI IIIF manifest returned `FL167150439..452` while `Transcriptions.txt` referenced `FL167150424..437`.

**This claim is hereby retracted.** Deeper probing revealed:

- `Transcriptions.txt` references **text-layer** FL ids (FL167150424..437) — these 500 on image GET because they don't have image bytes.
- The NLI IIIF manifest returns **image-layer** FL ids (FL167150439..452) — these return valid JPEGs.
- **Both layers live in the same `IE167150422`**; there is no IE-selection bug.
- `fetch_fl_ids_from_nli` is serving the authoritative image-layer FLs, which is exactly what the image endpoint needs.

No code change to IE-suffix logic or `fetch_fl_ids_from_nli` was needed. The P2 OPEN_ISSUES entry has been updated inline with this retraction.

## Fix

### Task 1 — Folio+side resolver (shared + core)

**`genizah_core.py`** — Added module-level `_parse_cudl_label(lbl)` + `_CUDL_LABEL_RE` regex at module scope. Extended `GenizahSearchEngine.fetch_external_iiif_data` so every canvas entry in `result['canvases']` now carries an additional `folio_side` key (`'r'`, `'v'`, or `None`) alongside the pre-existing `folio_num`. The parser accepts `"1"`, `"1r"`, `"1v"`, `"f.2v"`, `"f. 3r"`, uppercase `"6R"`; bare numeric labels are treated as recto by convention; `"Binding"`, `"Cover"`, etc. return `(None, None)`.

**`shared/nli_crossref_service.py`** — Added `resolve_cambridge_canvas_for_page(sys_id, page, images_ext, *, svc=None)`. Returns:

| Return value | Meaning | Caller action |
|---|---|---|
| `{'canvas_index': int, 'folio_num': int, 'side': 'r'\|'v'}` | Exact (folio, side) match in CUDL list | Serve `images_ext[canvas_index]` |
| `None` | Target (folio, side) identified but no CUDL canvas matches | Serve NLI fallback for this page |
| `{'degraded': True}` | Sidecar unavailable OR sys_id has no `nli_images` rows | Fall back to legacy positional `images_ext[page]` |

Bare-numeric canvas (`folio_side=None`) matches recto targets only — verso targets fall through to NLI fallback.

**`tests/test_nli_crossref_service.py`** — Added 20 new pytest cases:

- 4 `_parse_cudl_label` tests (bare numeric, verso, binding, f-prefix with space variant)
- 12 parametrized resolver tests (pages 0..11 → canvas_index 0..11 for T-S NS 158.112)
- 4 resolver edge tests (page 12 → None, page 13 → None, unknown sys_id → degraded, bare-numeric recto-only match)

All 20 new + 77 pre-existing = **97 tests pass** in `tests/test_nli_crossref_service.py`.

### Task 2a — Web cambridge_image wiring

**`web/api.py`** — Extracted `_fetch_nli_image_bytes(sys_id, page, width, suffix)` helper from `nli_image_by_sysid`. Returns `(content, content_type, fl_id)` on success; the `fl_id` lets the caller log which FL succeeded (for observability on the NLI fallback path). `nli_image_by_sysid` is now a 10-line wrapper around the helper; image cache tuple shape widened to 4-tuple (adds `fl_id`), reader still accepts the legacy 3-tuple.

**Rewrote `cambridge_image`** to consult `resolve_cambridge_canvas_for_page`:

- Exact match → serve CUDL IIIF tile with `X-Folio-Matched: {folio}{side}`.
- Resolver returns `None` → serve NLI fallback via `_fetch_nli_image_bytes(suffix=1)` with `X-Image-Fallback-Source: nli` + `X-Folio-Matched: {folio}{side}` + `INFO` log recording the resolved fl_id.
- Resolver returns `{'degraded': True}` → legacy positional `images_ext[page]`; log one `WARN` per sys_id per process lifetime.

**Every response** (success, fallback, degraded-legacy) now carries:

- `X-Image-Resolver-Version: 2` (deploy marker)
- `ETag: "{sys_id}-p{page}-v2"` (client revalidation)
- `Cache-Control: public, max-age=600` (unchanged)

**Cache-version constants**: `_CAMBRIDGE_CACHE_VERSION = 2` + `_CAMBRIDGE_ETAG_VERSION = "v2"`. Cache key includes the version, so server-side cache entries from before 260419-cfx are dead-on-deploy.

### Task 2b — Desktop browse wiring

**`desktop/widgets.py`** — Added `_get_folio_side_image_index(meta, folio_num, side)`. Stricter than `_get_folio_image_index`: returns `None` when no exact `(folio_num, side)` match exists (so callers can trigger NLI fallback) rather than falling back to the nearest folio.

**`genizah_app.py`** — Added three new browse-class methods and updated six call sites:

1. `_build_nli_iiif_url_for_page(sys_id, page_idx, width=2000)` — IE-aware NLI URL builder. Uses `resolve_volume_suffix(sys_id, self.current_browse_volume_ie)` for the IIIF manifest suffix. Falls back to `suffix=1` only when `current_browse_volume_ie` is None, with a one-line `WARN` deduped per sys_id per process via class-level `_desktop_nli_fallback_warned`. FL ids come from `meta_mgr.fetch_iiif_manifest(sys_id, suffix)['canvas_map']` (authoritative) — **NEVER** from `nli_crossref FGPImageNumberId` (Pitfall 6).

2. `_resolve_cambridge_page_or_fallback(sys_id, page_idx, display_meta, folio_num)` — LOAD-site resolver. Operates on a **shallow copy** of `display_meta` (`dict(display_meta)`) so `meta_mgr.nli_cache[sid]` is never mutated when a synthetic NLI fallback canvas is appended.

3. `_resolve_cambridge_navigation_index(sys_id, viewer_images, folio_num, side_offset)` — NAV-site resolver. Returns `(nav_meta, idx)`: `(None, int)` for existing-list match (caller uses `set_page(idx)`); `(dict, int)` for NLI fallback (caller uses `load_images(nav_meta, idx, ...)`); `(None, None)` for legacy positional fallback.

Plus a tiny `_is_cambridge_display(display_meta)` helper that detects CUDL source via `external_provider` or URL inspection.

**Call sites wired** (gated on `_is_cambridge_display` or `browse_viewer.external_provider == 'cambridge'`):

| Site | File:line (post-fix) | Role | Wiring |
|------|---------------------|------|--------|
| Browse-tab load | `genizah_app.py:~7185` | Initial page-load in `on_browse_enriched_loaded` | `_resolve_cambridge_page_or_fallback` |
| Switch-source reload | `genizah_app.py:~9370` | After `switch_to_cambridge` | `_resolve_cambridge_page_or_fallback` |
| Folio-nav inner elif | `genizah_app.py:~21282` | Prev/next inside browse tab, no folio_in_viewer | `_resolve_cambridge_navigation_index` |
| Folio-nav outer elif | `genizah_app.py:~21298` | Prev/next inside browse tab, folio_in_viewer | `_resolve_cambridge_navigation_index` |
| Composition-summary nav | `genizah_app.py:~22805` | Prev/next in composition-summary result dialog | `_resolve_cambridge_navigation_index` |

**LEAVE-ALONE invariant verified:** The two Oxford `folio_range` branches gated on `meta.get('oxford_part_id')` — at `genizah_app.py:~21274-21277` (browse nav) and `~22796-22799` (composition-summary nav) — are unchanged. CUL CUDL shelfmarks never set `oxford_part_id`, so they never enter these branches. `git diff` confirms no `_get_folio_image_index(meta, folio_num, side_offset=side_offset)` lines were touched.

### Task 3 — Diagnostic extension

**`scripts/debug_ts_ns_158_112_image_alignment.py`** — Added `--verify-resolver` flag. When passed, after the existing `ALIGNMENT VERDICTS` block, builds a synthetic `images_ext` list from the already-fetched CUDL labels (no extra network), calls `resolve_cambridge_canvas_for_page` for pages 0..N-1, and prints:

```
RESOLVER TABLE (260419-cfx)
----------------------------------------
  p=0 (folio=1r) → canvas_index=0
  p=1 (folio=1v) → canvas_index=1
  ...
  p=11 (folio=6v) → canvas_index=11
  p=12 (folio=8r) → NLI_FALLBACK
  p=13 (folio=8v) → NLI_FALLBACK
RESOLVER CUL-canvas-fix VERIFIED
```

`VERIFIED` fires when every page in `[0, min(len(cudl_canvases), len(nli_rows)))` gets an exact `canvas_index` AND every page outside that range gets `NLI_FALLBACK`.

## Resolver table (post-fix, T-S NS 158.112)

Actual output of `python scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver` on 2026-04-19:

```
RESOLVER TABLE (260419-cfx)
----------------------------------------
  p=0 (folio=1r) → canvas_index=0
  p=1 (folio=1v) → canvas_index=1
  p=2 (folio=2r) → canvas_index=2
  p=3 (folio=2v) → canvas_index=3
  p=4 (folio=3r) → canvas_index=4
  p=5 (folio=3v) → canvas_index=5
  p=6 (folio=4r) → canvas_index=6
  p=7 (folio=4v) → canvas_index=7
  p=8 (folio=5r) → canvas_index=8
  p=9 (folio=5v) → canvas_index=9
  p=10 (folio=6r) → canvas_index=10
  p=11 (folio=6v) → canvas_index=11
  p=12 (folio=8r) → NLI_FALLBACK
  p=13 (folio=8v) → NLI_FALLBACK
RESOLVER CUL-canvas-fix VERIFIED
```

## Test coverage added

### `tests/test_nli_crossref_service.py` (+ 20 pytest cases, class `TestFolioSideResolver` + 4 module-level `_parse_cudl_label` tests)

| Group | Count | Tests |
|------|------:|-------|
| `_parse_cudl_label` | 4 | bare numeric → recto; verso; binding; f-prefix with space variant |
| Resolver (exact match, parametrized) | 12 | pages 0..11 → (canvas_index, folio_num, side) for T-S NS 158.112 |
| Resolver (edge cases) | 4 | page 12 → None; page 13 → None; unknown sys_id → degraded; bare-numeric recto-only |

Pre-existing: **77 tests**. Post-fix: **97 tests** (`pytest tests/test_nli_crossref_service.py` exits 0). Full suite: **1117 pass, 9 skipped**.

## Commits

| # | Hash | Type | Message |
|---|------|------|---------|
| 1 | `7d0fbb29` | feat | add folio+side resolver with T-S NS 158.112 fixture tests |
| 2 | `7ba37277` | fix | web cambridge_image uses folio+side resolver with NLI fallback |
| 3 | `e2709838` | fix | desktop browse uses folio+side resolver with IE-aware NLI fallback |
| 4 | `549ef6af` | feat | extend diagnostic with --verify-resolver post-fix check |

## Known limitations

- **Web `cambridge_image` NLI fallback hardcodes `suffix=1`.** The `/api/cambridge_image/{sys_id}?page={N}` endpoint contract has no `suffix` query parameter — adding one is out of scope per CONTEXT.md. For rare multi-IE CUL shelfmarks (the Transcriptions.txt corpus currently contains ~5 multi-IE CUL records), the web fallback path may therefore resolve FL ids from the primary IE when the user is actually viewing a non-primary volume. **Desktop does NOT have this limitation** — it uses `resolve_volume_suffix(sid, current_browse_volume_ie)` via the new `_build_nli_iiif_url_for_page` helper.

- **FL id source invariant (Pitfall 6).** FL ids for NLI fallback are sourced from the NLI IIIF manifest canvas_map on both web (`fetch_fl_ids_from_nli` — unchanged) and desktop (`meta_mgr.fetch_iiif_manifest(sys_id, suffix)['canvas_map']` — new helper). **Never** from `nli_crossref FGPImageNumberId` — that column holds Friedberg photo numbers, not NLI IIIF FL ids. Enforced by docstring prohibitions in both the resolver and `_build_nli_iiif_url_for_page`; verified post-fix with `grep` (no offending patterns found).

- **CUDL label regex coverage.** `_CUDL_LABEL_RE = r'^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b'` handles `"1"`, `"1r"`, `"1v"`, `"f.2v"`, `"f. 3r"`, uppercase `"6R"`, and rejects `"Binding"`/`"Cover"`. It does NOT match `"1 recto"`, `"fol. 1r"`, or `"1a/1b"` styles. No occurrences of these patterns in the current CUL CUDL corpus, but this is an assumption not a proof.

## Operational: browser/CDN cache invalidation

Server in-memory cache is invalidated by bumping `_CAMBRIDGE_CACHE_VERSION` (currently `2`). Browsers/CDNs may still serve stale bytes for up to `max-age=600` (10 min) post-deploy. **Mitigation:** every response now carries `ETag: "{sys_id}-p{page}-v2"` and `X-Image-Resolver-Version: 2`. Clients that honor ETag revalidation will get fresh bytes.

If stale images are observed in production after deploy, either:

1. Force-refresh the browser cache (Ctrl+Shift+R / Cmd+Shift+R), or
2. Bump **both** `_CAMBRIDGE_CACHE_VERSION` (currently 2) AND `_CAMBRIDGE_ETAG_VERSION` (currently `"v2"`) together in `web/api.py` — they MUST stay in lockstep — and redeploy. Changing the ETag invalidates all intermediate caches immediately.

The `Cache-Control: public, max-age=600` header is unchanged; we trade a short window of possible staleness post-deploy for normal cache performance in steady state.

## Deviations from plan

### Auto-fixed blocking issue (Rule 3)

**1. [Rule 3 - Blocking issue] Plan's `is_cambridge = getattr(self, 'active_source', None) == 'cambridge'` would never fire on desktop.**

- **Found during:** Task 2b wiring.
- **Issue:** The plan text at PLAN.md:1407-1408 (and throughout the L6950/L9120/L21012/L21015/L22508 wiring sections) uses `getattr(self, 'active_source', None) == 'cambridge'` as the "is Cambridge?" guard. However, `self.active_source` is a **web-only** attribute (set on `web.state`), not a desktop `GenizahGUI` attribute. `grep -n '\.active_source' genizah_app.py` returns zero hits. Without a correction, the guard would always be False and the fix would never engage on desktop.
- **Fix:** Introduced two small helpers on the browse class:
  - `_is_cambridge_display(display_meta)` — used at LOAD sites (before `load_images()` is called). Checks `display_meta['external_provider'] == 'cambridge'` with URL inspection fallback (mirrors `ManuscriptViewerWidget._detect_external_provider`).
  - `getattr(self.browse_viewer, 'external_provider', None) == 'cambridge'` — used at NAV sites (after `load_images()` set the viewer's provider).
  This is the desktop equivalent of the web `state.active_source` guard.
- **Files modified:** `genizah_app.py` (new `_is_cambridge_display` helper + all five MODIFY call sites).
- **Commit:** `e2709838`.

Otherwise plan was executed exactly as written. All four plan tasks (1, 2a, 2b, 4 — the plan had no "Task 3"; the constraints' split into Task 3 = diagnostic-only + Task 4 = docs was implemented via two separate commits) are done, and every must-have truth in the plan frontmatter is satisfied.

## Self-Check

Post-execution verification commands and results:

- [x] `pytest tests/test_nli_crossref_service.py -x -v` → **97 passed** (77 pre-existing + 20 new; full suite 1117 pass, 9 skipped).
- [x] `python scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver` → emits `RESOLVER CUL-canvas-fix VERIFIED`.
- [x] `python scripts/check_docs.py` → `All checks passed! Documentation is healthy.`
- [x] `grep -rn "FL\\{.*[Ff][Gg][Pp]" genizah_app.py shared/ web/` → 0 matches (FGP-vs-FL regression guard clean).
- [x] Oxford LEAVE-ALONE branches unchanged: `git diff 65521f7e..HEAD` shows no `_get_folio_image_index(meta, folio_num, side_offset=side_offset)` / `load_images(meta, idx, ...)` modifications in the two Oxford-gated blocks.
- [x] `fetch_fl_ids_from_nli` in web/api.py is unchanged.
- [x] `fetch_external_iiif_data` canvas entries now carry `folio_side`; existing consumers (that read only `folio_num`) continue to work.
- [x] `_CAMBRIDGE_CACHE_VERSION = 2` + `_CAMBRIDGE_ETAG_VERSION = "v2"` present in `web/api.py`, used in cache key AND ETag header.
- [x] Desktop `_build_nli_iiif_url_for_page` uses `resolve_volume_suffix(sys_id, self.current_browse_volume_ie)`; suffix=1 only in the `vol_ie is None` branch, with `WARN` deduped per sys_id.
- [x] Every desktop write to `display_meta['images_ext']` in the new code paths goes through `dict(display_meta)` first (shallow copy); `meta_mgr.nli_cache[sid]` is never mutated.
- [x] `docs/OPEN_ISSUES.md`: Last Updated line flipped to 260419-cfx; P2 row "CUL positional image mismatch (260419-nwv follow-up)" → `✅ Fixed (2026-04-19)` with inline H3 retraction; Quick Summary counts adjusted (P2 Open 18→17, Fixed 60→61; Total Open 29→28, Fixed 103→104).
- [x] All four commits in `git log` with prefix `260419-cfx` (7d0fbb29, 7ba37277, e2709838, 549ef6af).

## Self-Check: PASSED

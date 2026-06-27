---
status: fix_implemented_pending_uat
trigger: "SEED-010 — Web Joins Lab: per-provider image resolution diverges across anchor / grid / Compare; zoom dead when image fails to load. Surfaced during 2026-06-21 live HE-mode RTL UAT while NLI's image API was DOWN. Language-independent (reproduces in English)."
created: 2026-06-21T00:00:00Z
updated: 2026-06-21T00:00:00Z
seed: SEED-010
branch: master-main (fix applied here per user; no feature branch)
fix_scope: Option B (unified breaker-aware resolver) + Compare per-instance zoom — Codex-reviewed
---

## Current Focus

hypothesis: CONFIRMED — three divergent image-resolution paths (grid / anchor / Compare) with inconsistent provider coverage, plus a zoom-init coupling. Grid hardcodes the NLI proxy for ALL non-Oxford providers; anchor/Compare gate Cambridge auto-default behind an "aligned" verdict. When NLI is down, non-Oxford grid thumbs 404 and non-aligned CUDL anchors stay on NLI and fail. Zoom is dead because manuscriptViewer.init() is wired only to <img onload>, which never fires on a 404.
test: Verified seed file:line claims against current code (candidate_grid.py:440 NLI-proxy hardcode; image_resolution.py:156-185 Cambridge verdict gate while JTS/Manchester/Oxford auto-default unconditionally). Both confirmed exact.
expecting: Root cause is established; remaining work is the FIX (unify resolution + decouple zoom-init) and validation with NLI UP (live now) and DOWN (simulate via Phase-98 breaker open).
next_action: Decide fix scope (minimal targeted patches vs single shared breaker-aware resolver), implement, then validate UP + DOWN.

## Symptoms

expected: In the Web Joins Lab, candidate images load consistently across the candidate grid, the main anchor pane, and the Compare modal, for every provider (NLI, Oxford, Cambridge/CUDL, Manchester, JTS) — and zoom/pan controls work — including when NLI's image API is down (other providers should still resolve).
actual (observed 2026-06-21, web Joins Lab, NLI image API DOWN):
  - Oxford: thumbnails shown in the candidate GRID, but full images NOT shown in the main ANCHOR pane nor in COMPARE.
  - CUDL/Cambridge: images shown in COMPARE, but NOT in the GRID nor the main ANCHOR.
  - Zoom controls do NOT work, at least for CUDL images.
errors: No exception surfaced to the user; images silently fail (404 from the NLI proxy when NLI is down), and zoom buttons click but do nothing.
reproduction: Open the Web Joins Lab, pin an anchor, view candidates from Oxford and CUDL/Cambridge while NLI's image API is unavailable (or with the Phase-98 NLI circuit breaker open). Compare across grid / anchor / Compare surfaces. Try zoom on a CUDL image.
started: Pre-existing latent bug; only manifests when NLI is down. Masked when NLI is up because the NLI proxy itself falls back to other providers server-side and /api/cambridge_image has its own NLI fallback (web/api.py:1156-1177).

## Eliminated

- hypothesis: "ALL zoom failures are downstream of the image failing to load (seed's framing)."
  reason: PARTIALLY TRUE but INCOMPLETE — REVISED 2026-06-21 by evidence :07Z. For the single anchor pane, _show_image() already calls manuscriptViewer.init() after render (anchor_viewer.py:761-763), not only via <img onload>, so when an image DOES load zoom works there. BUT the live user report "can't zoom in Compare" reproduces WITH images loading — a SEPARATE structural root cause: the single global manuscriptViewer + first-match querySelector('.zoomable-image') wires only the first (background) image, never the Compare modal's panes. So zoom-in-Compare is NOT downstream of a 404 and IS a standalone bug. See evidence :07Z.

## Evidence

- timestamp: 2026-06-21T00:00:00Z
  checked: web/components/candidate_grid.py:397-440 build_thumbnail_url()
  found: Synthetic→None; Oxford→Bodleian direct or /api/oxford_image (own fork, :424-436); ALL other providers (Cambridge/CUDL, Manchester, JTS) hardcoded to /api/nli_image_by_sysid/{sys_id}?page=...&width=300 (:440). Docstring explicitly says non-Oxford providers "defer to the NLI proxy."
  implication: GRID thumbnails for CUDL/Manchester/JTS 404 when NLI is down; Oxford survives. Matches observed Oxford-grid-OK / CUDL-grid-fail.

- timestamp: 2026-06-21T00:00:01Z
  checked: web/components/image_resolution.py:151-216 resolve_image_url() auto-default + override block
  found: JTS (:158), Manchester (:160), Oxford (:162) auto-default to their own source unconditionally when active_source=='nli' and no user override. Cambridge auto-default (:184) is GATED behind _cam_safe_default (:173-183) which requires verdict=='aligned' OR (verdict is None AND cambridge count == total_pages). A missing/'misaligned' verdict keeps active_source='nli'.
  implication: ANCHOR pane: Oxford auto-defaults → works; CUDL with no/misaligned verdict stays on NLI → fails when NLI down. Matches observed Oxford-anchor-fail-too (see next) / CUDL-anchor-fail.

- timestamp: 2026-06-21T00:00:02Z
  checked: Seed cross-reference for Oxford anchor/Compare failure
  found: Seed reports Oxford full images NOT shown in anchor nor Compare even though Oxford auto-defaults at image_resolution.py:162. Likely the Oxford direct/proxy URL path itself failing at render or the onload coupling — needs live confirmation with NLI UP (now reachable). Flag for the debugger to reproduce, since image_resolution.py auto-defaults Oxford but the observed anchor still failed.
  implication: There may be a second Oxford-specific anchor failure distinct from the Cambridge verdict gate; do not assume items 1-3 fully cover it. Verify on the live anchor path.

- timestamp: 2026-06-21T00:00:03Z
  checked: web/components/compare_modal.py:394-520 _fill_candidate()
  found: Builds a FRESH AnchorViewer per candidate, re-running resolve_external_images() + resolve_image_url(). Same resolver as anchor, but re-evaluated per flip, and it does NOT carry forward the grid/user-chosen active_source (no source_user_override) → can re-default to NLI.
  implication: COMPARE shows CUDL when the verdict is aligned (why CUDL appeared in Compare but not grid/anchor), but still risks re-defaulting to NLI without override preservation.

- timestamp: 2026-06-21T00:00:04Z
  checked: web/components/anchor_viewer.py:449-470 _build_img_html() ; web/static/manuscript_viewer.js:205-238 init()
  found: <img ... onload="if(window.manuscriptViewer) window.manuscriptViewer.init()">; pan/zoom/wheel handlers wired only inside init(). Server-side zoom buttons (anchor_viewer.py:347-384) push _zoom via ui.run_javascript.
  implication: On a 404, onload never fires → init() never runs → zoom buttons do nothing. Decouple init from onload (or also call on placeholder/error path).

- timestamp: 2026-06-21T00:00:05Z
  checked: Live reachability probe (2026-06-21)
  found: genizahsearch.com/api/fl_ids/990001458630205171 → HTTP 200 in 3.6s (real NLI MARC fetch; breaker closed). iiif.nli.org.il bogus manifest → fast 403 (not a hang).
  implication: NLI image API is UP now. The seed's "cannot validate during the outage" blocker is LIFTED. UP path is testable live; DOWN path must be simulated by forcing the Phase-98 breaker open (shared/nli_circuit_breaker.py).

- timestamp: 2026-06-21T00:00:06Z
  checked: Codex scope review (_tmp/codex-seed010-scope-output.md) + verification of image_resolution.py:102/139/162/188-210
  found: NEW root cause Codex surfaced and I confirmed — OXFORD ORDERING BUG. The Oxford primary-URL branch (:102) only runs when active_source != 'nli'. On the first resolve active_source defaults to 'nli', so :102 is SKIPPED and the NLI URL is built at :139. Then :162 flips active_source to 'oxford'. The override block (:188-210) has cambridge/manchester/jts branches but NO oxford branch, so img_url stays the NLI URL while active_source reports 'oxford'. Result: {img_url: NLI, active_source: 'oxford'} → Oxford anchor renders an NLI URL → 404s when NLI is down.
  implication: This is the Oxford anchor/Compare failure the seed flagged as an open question. It is NOT covered by any of the seed's 4 proposed patches — decisive evidence that minimal patches (Option A) are insufficient. The clean fix is to decide active_source FIRST, then build exactly one URL (Codex's unified-resolver shape).

- timestamp: 2026-06-21T00:00:07Z
  checked: web/static/manuscript_viewer.js:180-231,281-288 (createManuscriptViewer/init/applyTransform); web/components/anchor_viewer.py:50-68 (single global viewer, .zoomable-image), :752-763 (_show_image post-render init); web/components/compare_modal.py:482-491,723-728 (two FRESH AnchorViewer panes)
  found: CONFIRMED ROOT CAUSE for live user report "can't zoom in Compare." window.manuscriptViewer is a SINGLE global viewer per page (window._msViewerLoaded guard) bound to imageSelector='.zoomable-image'. init()/applyTransform() use document.querySelector('.zoomable-image') = FIRST match in DOM. The Compare modal renders TWO .zoomable-image elements (anchor + candidate panes) on top of the sticky page anchor's .zoomable-image. init() therefore wires the FIRST (background page) image; both Compare panes get no zoom handlers.
  implication: Zoom-in-Compare is a STRUCTURAL single-instance bug, independent of NLI up/down and independent of image 404s — it reproduces with images loading fine (matches the live report). Fix: per-pane viewer instances bound to a unique selector/id per AnchorViewer, or scope the querySelector to the viewer's own container. This REFUTES the seed's "zoom dead = downstream of 404" framing AND Codex's "zoom already fine, deprioritize" (Codex only inspected the single-pane anchor _show_image path). Zoom IS in scope.

## Per-surface × per-provider resolution matrix (current behavior)

| Surface | NLI | Oxford | CUDL/Cambridge | Manchester | JTS |
|---------|-----|--------|----------------|------------|-----|
| Anchor | /api/nli_image_by_sysid | direct Bodleian / /api/oxford_image | /api/cambridge_image ONLY if verdict aligned, else stays NLI | /api/manchester_image | /api/jts_image |
| Grid thumb | /api/nli_image_by_sysid&width=300 | direct Bodleian / /api/oxford_image | /api/nli_image_by_sysid (WRONG) | /api/nli_image_by_sysid (WRONG) | /api/nli_image_by_sysid (WRONG) |
| Compare | via fresh AnchorViewer (= Anchor row) | = Anchor | = Anchor (re-evaluated per flip, no override carry-forward) | = Anchor | = Anchor |

## Proposed fix scope (from seed; debugger to confirm + sequence)

1. Grid provider-awareness (PRIMARY): build_thumbnail_url() (candidate_grid.py:440) must route Cambridge/Manchester/JTS to their own proxies (/api/cambridge_image etc.) instead of the NLI proxy. Either thread external_provider into card rendering or call resolve_external_images() during card creation (heavier; mirrors Compare). Ideally route ALL three surfaces through ONE shared resolver.
2. Cambridge auto-default when NLI is down: relax the verdict gate (image_resolution.py:156-185) — when the Phase-98 NLI breaker is OPEN, auto-default CUDL to cambridge even without an aligned verdict (pass breaker state into resolve_image_url()).
3. Compare provider preservation: store the resolved active_source in Compare state and pass source_user_override=True to the fresh AnchorViewer (compare_modal.py:394-520).
4. Zoom-init robustness: decouple manuscriptViewer.init() from <img> onload (or also invoke on placeholder/error path) so controls are live even when an image fails. (Items 1-3 largely subsume the symptom by ensuring a working URL is chosen.)

DESIGN NOTE: the durable fix is a SINGLE shared, breaker-aware image-URL resolver used by grid, anchor, and Compare. Minimal targeted patches (items 1-4 in place) are lower-risk and faster; the unified resolver is the seed's recommended durable design but a larger refactor. This scope choice is the main fix-checkpoint decision.

## Best fix locations (file:line — verified current 2026-06-21)

- web/components/candidate_grid.py:397-440 — build_thumbnail_url() (NLI-proxy hardcode at :440)
- web/components/image_resolution.py:151-216 — resolve_image_url() router; auto-default gate :156-185; provider overrides :188-210
- web/components/anchor_viewer.py:390-443 (_resolve_off_loop), :449-470 (_build_img_html onload coupling), :347-384 (server-side zoom state)
- web/components/compare_modal.py:394-520 — _fill_candidate() fresh-viewer creation
- web/static/manuscript_viewer.js:205-238 (init()), :281-288 (applyTransform)
- web/api.py proxies: /api/nli_image_by_sysid (:1012-1035, breaker check :1027-1029), /api/cambridge_image (:1054-1211, NLI-fallback :1156-1177), /api/manchester_image (:1216-1268), /api/jts_image (:1273-1325), /api/oxford_image (:1335-1452)
- shared/nli_circuit_breaker.py — Phase-98 breaker (use to detect NLI-down state and to simulate DOWN in tests)

## Validation plan

- NLI UP (live now): confirm all five providers resolve in grid / anchor / Compare and zoom works.
- NLI DOWN (simulate breaker open): confirm CUDL/Manchester/JTS grid thumbs no longer 404, CUDL anchor auto-defaults to cambridge, Compare preserves source, and zoom controls remain live.
- Regression: ensure the masked-by-NLI-up behavior still works (don't break the server-side NLI→other fallback in /api/cambridge_image:1156-1177).
- Add render-smoke coverage (memory feedback_nicegui_render_smoke_gap — headless pytest misses the async render path that produced Phase-119 criticals).

## Implementation (2026-06-21) — all 5 steps applied on master-main

1. resolve_image_url() (web/components/image_resolution.py) — RESTRUCTURED to source-first:
   compute provider flags → decide final active_source (auto-default priority preserved) →
   build EXACTLY ONE URL via dispatch (now includes an Oxford branch, fixing the ordering bug
   where active_source='oxford' came back paired with an NLI URL). New params: width,
   surface ('viewer'|'thumbnail'), nli_circuit_open (read lazily from shared.nli_circuit_breaker
   when None). Cambridge auto-defaults when verdict-safe OR NLI breaker OPEN OR surface=thumbnail.
   Viewer-surface behavior byte-for-byte unchanged except the intended breaker relaxation.
2. build_thumbnail_url() (web/components/candidate_grid.py) — breaker-aware: NLI UP → NLI proxy
   (unchanged, fast); NLI DOWN → route by library_code to the provider proxy via the canonical
   shared.search_serializer._BROWSE_PROXY_BY_LIBRARY table. Oxford fork unchanged.
   web/api.py — cambridge/manchester/jts endpoints now enrich-on-demand on a cold nli_cache
   (_ensure_images_ext; enrich resolves CUDL/Manchester/JTS from the LOCAL crossref sidecar +
   non-NLI manifest fetches, so it works during an NLI outage); cambridge falls back to NLI when
   no CUDL (was a hard 404 → also fixes a latent cold-cache 404 in the public /api/browse).
3. AnchorViewer (anchor_viewer.py) — added active_source/source_user_override params, threads
   them into _resolve_off_loop→resolve_image_url, and PERSISTS resolved active_source across
   folio nav. Compare's fresh panes now auto-default deterministically (did NOT pin
   source_user_override=True — that would wrongly pin NLI). Codex "minimum" satisfied.
4. Per-instance zoom — each AnchorViewer gets a UNIQUE container class (avcN); a per-instance
   manuscriptViewer is created/inited via window.__msInitViewer(vid) scoped to '.avcN
   .zoomable-image' (registry in _VIEWER_HEAD). zoom_in/out/reset/_apply_zoom drive that instance.
   Fixes dead zoom in Compare (was one global viewer + first-match querySelector wiring only the
   first image). manuscript_viewer.js: drag move/up now bound to document per-drag (in
   onMouseDown, removed in onMouseUp) instead of overwriting window.onmousemove globally — so
   Compare's two panes pan independently. Backward-compatible for /browse + the VS advViewer.
5. Tests + checks — new SEED-010 cases in test_image_resolution.py (8: Oxford-first-load,
   breaker-closed-stays-NLI, breaker-open→CUDL, thumbnail→CUDL+width, no-width-on-Bodleian,
   user-override, lazy-breaker-read), test_candidate_grid.py (5: up/down/manchester+jts/unknown/
   oxford), test_anchor_viewer.py (unique-id-per-pane, head registry, updated onload assertion).
   ruff clean; py_compile clean; node --check manuscript_viewer.js OK.

## Test results (2026-06-21)
- 565 passed / 2 skipped across joins_lab/compare/browse_api/api_legacy/search_serializer/
  parallels/openapi suites + 163 in image_resolution/candidate_grid/anchor_viewer (incl. new).
- Pre-existing unrelated failure: test_joins_lab_render::test_cold_start... (Windows cross-test
  asyncio event-loop pollution; reproduces identically on the stashed baseline — NOT this change).

## REMAINING — live HUMAN UAT (cannot be unit-verified; matches the seed's UP/DOWN mandate)
- [ ] Browser: open Web Joins Lab, pin an anchor, open Compare → ZOOM works in BOTH panes
      (wheel + +/-/reset buttons), and pan works in both. (The per-instance JS wiring can only be
      confirmed in a real browser.)
- [ ] NLI UP (reachable now): Oxford full image shows in anchor + Compare; CUDL/Manchester/JTS
      show in grid + anchor + Compare; no broken thumbnails.
- [ ] NLI DOWN (simulate: trip the Phase-98 breaker, e.g. point NLI at a bad host or force
      is_open()): grid CUDL/Manchester/JTS thumbnails still load (provider proxy + enrich-on-demand),
      CUDL anchor auto-defaults to cambridge, Oxford still loads (Bodleian), plain-NLI shows placeholder.
- [ ] Regression: /browse Oxford + CUDL pages still render correctly (shared resolver); /browse
      zoom/pan still work (shared manuscript_viewer.js drag change).
- [ ] Public API: GET /api/cambridge_image/{cold CUL sys_id} now serves (CUDL or NLI fallback)
      instead of 404 — confirm no surprise for existing API consumers.

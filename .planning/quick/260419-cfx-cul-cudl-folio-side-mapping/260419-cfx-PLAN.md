---
id: 260419-cfx
type: quick-fix
phase: 260419-cfx
plan: 01
wave: 1
depends_on: []
autonomous: true
requirements:
  - cfx-01  # Folio+side resolver (N → canvas-or-NLI-fallback) for CUL CUDL
  - cfx-02  # Web /api/cambridge_image returns correct canvas or NLI fallback for T-S NS 158.112 pages 1..14
  - cfx-03  # Desktop browse with active_source='cambridge' shows correct folio or NLI fallback for T-S NS 158.112 pages 1..14
  - cfx-04  # Cache versioning + graceful degradation when nli_crossref.db missing
  - cfx-05  # Docs: OPEN_ISSUES flipped to Fixed + H3 retraction note + SUMMARY.md
files_modified:
  - genizah_core.py
  - shared/nli_crossref_service.py
  - tests/test_nli_crossref_service.py
  - web/api.py
  - desktop/widgets.py
  - genizah_app.py
  - scripts/debug_ts_ns_158_112_image_alignment.py
  - docs/OPEN_ISSUES.md
  - .planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-SUMMARY.md

must_haves:
  truths:
    - "GET /api/cambridge_image/990051537270205171?page=0..11 returns the same CUDL image bytes as before the fix (folios 1r..6v)."
    - "GET /api/cambridge_image/990051537270205171?page=12 and ?page=13 return a JPEG (NLI fallback for folios 8r/8v) instead of a 404 error body."
    - "The NLI-fallback response has Content-Type 'image/jpeg' and headers 'X-Image-Fallback-Source: nli', 'X-Image-Resolver-Version: 2', and an ETag of shape '\"{sys_id}-p{page}-v2\"'. Successful CUDL canvas responses also carry 'X-Image-Resolver-Version: 2' and the same ETag shape, and a successful fallback also sets 'X-Folio-Matched: {folio}{side}' (e.g. '8r') when the resolver produced a (folio, side) pair."
    - "Desktop browse for T-S NS 158.112 (sys_id 990051537270205171) with active_source='cambridge' shows the correct folio image for pages 1..12 and shows the NLI image (not a repeated last CUDL canvas and not a blank) for pages 13..14, on INITIAL page-load AND during subsequent next/prev navigation."
    - "When nli_crossref.db is missing, cambridge_image falls back to the previous positional behavior with a single logged WARNING, not an HTTP 500."
    - "pytest tests/test_nli_crossref_service.py passes, including the new TestFolioSideResolver class that asserts the expected N→canvas-or-NLI-fallback indices for T-S NS 158.112 (14 nli_images rows, 12 CUDL canvases, 14 transcription pages)."
    - "docs/OPEN_ISSUES.md P2 entry 'CUL positional image mismatch (260419-nwv follow-up)' is flipped from ❌ Open to ✅ Fixed (2026-04-19) with a short NOTE retracting the H3 IE-suffix claim (text-layer FL ids vs image-layer FL ids — not an IE-selection bug)."
    - "NLI fallback FL ids are sourced from the NLI IIIF manifest canvas_map (via fetch_fl_ids_from_nli on web, or meta_mgr.fetch_iiif_manifest(sys_id, suffix)['canvas_map'] on desktop), NEVER from nli_crossref FGPImageNumberId. FGPImageNumberId is a Friedberg photo number, not an NLI FL id (see .planning/research/PITFALLS.md Pitfall 6)."
    - "Desktop NLI fallback uses the active volume's IE via `resolve_volume_suffix(sys_id, current_browse_volume_ie)` — NOT a hardcoded suffix=1. When current_browse_volume_ie is None (single-IE shelfmark or no volume selected), suffix=1 is used and a one-line WARNING is logged for the sys_id."
    - "Known limitation (documented in SUMMARY.md): the WEB cambridge_image NLI fallback still uses suffix=1 because the existing /api/cambridge_image endpoint contract has no `suffix` query param (adding one is out of scope per CONTEXT.md). Multi-IE CUL shelfmarks (rare) may therefore get the wrong volume's NLI image on the web fallback path. Desktop does NOT have this limitation."
  artifacts:
    - path: "shared/nli_crossref_service.py"
      provides: "Public resolver function resolve_cambridge_canvas_for_page(sys_id, page, images_ext) -> dict|None (None == use NLI fallback)"
      contains: "def resolve_cambridge_canvas_for_page"
    - path: "genizah_core.py"
      provides: "fetch_external_iiif_data canvas entries carry folio_side in addition to folio_num"
      contains: "'folio_side'"
    - path: "web/api.py"
      provides: "Private _fetch_nli_image_bytes helper + cambridge_image folio+side lookup with NLI fallback + cache version bump + resolver-version response headers"
      contains: "_CAMBRIDGE_CACHE_VERSION"
    - path: "desktop/widgets.py"
      provides: "Side-aware folio resolver helper (mirrors web behavior) used by desktop browse CUDL path"
      contains: "def _get_folio_side_image_index"
    - path: "tests/test_nli_crossref_service.py"
      provides: "Unit test class TestFolioSideResolver with T-S NS 158.112 fixture"
      contains: "class TestFolioSideResolver"
    - path: "docs/OPEN_ISSUES.md"
      provides: "P2 'CUL positional image mismatch' flipped to Fixed; H3 retraction note"
      contains: "✅ Fixed (2026-04-19)"
    - path: ".planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-SUMMARY.md"
      provides: "Quick-fix summary with diagnostic re-run output, retraction of H3 verdict, and operational note on browser cache"
      contains: "X-Image-Resolver-Version"
  key_links:
    - from: "web/api.py::cambridge_image"
      to: "shared/nli_crossref_service.py::resolve_cambridge_canvas_for_page"
      via: "direct import + call with (sys_id, page, images_ext)"
      pattern: "resolve_cambridge_canvas_for_page"
    - from: "web/api.py::cambridge_image"
      to: "web/api.py::_fetch_nli_image_bytes (extracted from nli_image_by_sysid)"
      via: "direct function call for NLI fallback when resolver returns None"
      pattern: "_fetch_nli_image_bytes"
    - from: "genizah_core.py::fetch_external_iiif_data"
      to: "images_ext canvas entries (folio_num, folio_side, label, url)"
      via: "CUDL manifest label regex now captures side (r|v)"
      pattern: "folio_side"
    - from: "desktop/widgets.py::_get_folio_side_image_index"
      to: "images_ext entries' folio_num + folio_side"
      via: "exact match on (folio_num, side) pair derived from nli_images ImageName"
      pattern: "folio_side"
    - from: "tests/test_nli_crossref_service.py::TestFolioSideResolver"
      to: "resolve_cambridge_canvas_for_page"
      via: "assert expected indices for pages 1..14 of T-S NS 158.112 fixture"
      pattern: "resolve_cambridge_canvas_for_page"
    - from: "genizah_app.py::_build_nli_iiif_url_for_page"
      to: "genizah_core.GenizahSearchEngine.fetch_iiif_manifest (canvas_map)"
      via: "meta_mgr.fetch_iiif_manifest(sys_id, suffix=resolve_volume_suffix(sys_id, current_browse_volume_ie))['canvas_map']"
      pattern: "fetch_iiif_manifest.*canvas_map"
    - from: "genizah_app.py::browse navigation (L21013, L21016, L22509)"
      to: "shared/nli_crossref_service.py::resolve_cambridge_canvas_for_page + desktop/widgets.py::_get_folio_side_image_index"
      via: "side-aware index lookup when active_source='cambridge' (instead of _get_folio_image_index)"
      pattern: "_get_folio_side_image_index|resolve_cambridge_canvas_for_page"

threat_model:
  trust_boundaries:
    - "Web client → /api/cambridge_image/{sys_id}?page={N}: untrusted sys_id + page param; server reads local SQLite (nli_crossref.db) and fetches external IIIF (cudl.lib.cam.ac.uk, iiif.nli.org.il)."
    - "Server → external IIIF (CUDL + NLI): outbound; treat response bytes as untrusted image data, never execute, always cap content size implicitly via IIIF URL."
  threats:
    - id: "T-260419-cfx-01"
      category: "Tampering (stale cache)"
      component: "web/api.py::_cambridge_image_cache + browser/CDN cache"
      disposition: "mitigate"
      mitigation: "Add `_CAMBRIDGE_CACHE_VERSION = 2` constant and include it in the cache key tuple AND in the response headers (`X-Image-Resolver-Version: 2`, `ETag: \"{sys_id}-p{page}-v2\"`). Server-side caches are invalidated by the key bump; browser/CDN caches revalidate via ETag. Cache-Control max-age=600 is unchanged (retain normal performance), but clients that honor ETag revalidation will get fresh bytes after deploy. SUMMARY.md documents the operational knob: bump `_CAMBRIDGE_CACHE_VERSION` and ETag version together if stale bytes are observed."
    - id: "T-260419-cfx-02"
      category: "Information Disclosure (wrong content-type)"
      component: "web/api.py::cambridge_image NLI fallback"
      disposition: "mitigate"
      mitigation: "NLI fallback reuses the extracted helper `_fetch_nli_image_bytes`, which only returns content when Content-Type contains 'image' AND payload size exceeds the min-size threshold. On helper failure the endpoint returns a 404 with a plain-text body, never a falsy image."
    - id: "T-260419-cfx-03"
      category: "Denial of Service (missing sidecar → 500)"
      component: "web/api.py::cambridge_image"
      disposition: "mitigate"
      mitigation: "If NliCrossrefService.is_available() is False OR get_folio_images(sys_id) returns [], fall back to the legacy positional lookup (existing code path). Log at WARNING exactly once per sys_id per process lifetime via a module-level guard set. Never raise to the request handler."
    - id: "T-260419-cfx-04"
      category: "Tampering (label-side ambiguity)"
      component: "shared/nli_crossref_service.py::resolve_cambridge_canvas_for_page"
      disposition: "mitigate"
      mitigation: "Side-detection regex requires trailing 'r' or 'v' (case-insensitive) on the CUDL label. A bare numeric label like '1' (no side) is treated as recto by convention (documented in docstring). Labels like 'Binding', 'Cover', 'f.1r', 'f. 1v' parse consistently via the canonical `^\\s*(?:f\\.?\\s*)?(\\d+)\\s*([rv])?\\b` pattern."
    - id: "T-260419-cfx-05"
      category: "Spoofing (sys_id collision)"
      component: "resolve_cambridge_canvas_for_page"
      disposition: "accept"
      mitigation: "sys_id is already trusted by the NLI image endpoint; no new attack surface is introduced. Low-value, low-risk."
---

<objective>
Fix the CUL CUDL positional canvas mismatch (H1) in web + desktop by
replacing positional `images_ext[page]` lookup with a folio+side match
against the N-th NLI ImageName for that sys_id, falling back to the NLI
image when no CUDL canvas carries the same (folio_num, side). Keep the
URL shape `/api/cambridge_image/{sys_id}?page={N}` and desktop browse
navigation unchanged. Retract the prior H3 verdict in docs.

Purpose: T-S NS 158.112 (sys_id 990051537270205171) — and any CUL
manuscript where CUDL canvas count < transcription page count, and the
paired-leaf shift means canvas ordering no longer matches transcription
page ordering — currently shows the wrong image past canvas 12 (off-by-one
or trailing-canvas repeat). This is a real UX bug: users see text for
folio 8 with the image from folio 6v still on screen.

Output: deterministic page-N → canvas-or-NLI-fallback resolver, a
refactored `cambridge_image` endpoint that uses it, a mirrored desktop
fix (including navigation-time index recompute), a unit test with the
T-S NS 158.112 fixture, and docs that correct the earlier misdiagnosis.

> **Hard rule (do not violate):** NEVER construct NLI IIIF URLs from
> `FGPImageNumberId`. That column is a Friedberg photo number, NOT an
> NLI IIIF FL id — different numbering systems (see
> `.planning/research/PITFALLS.md` Pitfall 6 and `MEMORY.md` Phase 30
> lesson). The canonical FL-id source is the NLI IIIF manifest's
> `canvas_map` (web: `fetch_fl_ids_from_nli(sys_id, suffix)`; desktop:
> `meta_mgr.fetch_iiif_manifest(sys_id, suffix)['canvas_map']`).
</objective>

<context>
@.planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-CONTEXT.md
@.planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-REVIEWS.md
@.planning/quick/260419-nwv-bug-with-some-shelfmarks-images-esp-cul-/260419-nwv-SUMMARY.md
@.planning/research/PITFALLS.md
@./CLAUDE.md
@shared/nli_crossref_service.py
@web/api.py
@genizah_core.py
@desktop/widgets.py
@scripts/debug_ts_ns_158_112_image_alignment.py

<interfaces>
<!-- Existing contracts the executor will use directly. No codebase
     exploration required — these are extracted from the code that is
     already in the repo. -->

From shared/nli_crossref_service.py:
```python
_FOLIO_PATTERN = re.compile(r'L(\d+)(?:_\d+)?F\d+B\d+S(\d+)')

def parse_folio_label(image_name: str) -> str:
    """Returns e.g. '1r', '8v', or '' if no match."""

class NliCrossrefService:
    # __init__(db_path, thread_safe=False) opens sqlite in ro mode; sets
    # self._conn to a connection OR None on failure/missing-file. The
    # constructor does NOT read the `meta` table. `is_available()` simply
    # returns `self._conn is not None` — it does NOT require any rows in
    # `meta` (get_version() is the only meta reader, and it's not on the
    # resolver path). Bare-schema fixtures with empty `meta` and
    # `nli_images` tables are therefore sufficient; seeding a
    # `meta(key='version', value='0.0.0-test')` row is optional and only
    # needed if your test happens to call get_version().
    def is_available(self) -> bool: ...
    def get_images(self, sys_id: str) -> list[dict]:
        """Each dict has: fgp_image_number_id, fgp_number, image_name,
           image_source_name, shelfmark."""
    def get_folio_images(self, sys_id: str) -> list[dict]:
        """Same as get_images() but sorted by (leaf_number, side),
           with a 'folio_label' key added. Post-260419-nwv this returns
           the correct order for T-S NS 158.112:
             [{'folio_label': '1r', ...}, {'folio_label': '1v', ...},
              {'folio_label': '2r', ...}, ..., {'folio_label': '8v', ...}]"""

def get_nli_crossref_service(thread_safe: bool = False) -> NliCrossrefService:
    """Singleton accessor. web/api.py already calls this at module init
       (web/api.py:304-305) and stores `nli_svc`."""
```

From genizah_core.py::GenizahSearchEngine.fetch_iiif_manifest (L3517-3569):
```python
def fetch_iiif_manifest(self, system_id, suffix=1):
    """Fetch and parse NLI IIIF manifest.

    Returns dict with:
      - 'physical_desc': str
      - 'attribution': str
      - 'canvas_map': dict[str, str]  -- {fl_digits: label, ...}
                                         insertion order preserved (Python
                                         3.7+), which IS the manifest's
                                         canvas order.

    The canvas_map keys are FL digit strings (e.g. '167150439'), NOT
    prefixed with 'FL'. To build an IIIF URL, prepend 'FL':
        url = f'https://iiif.nli.org.il/IIIFv21/FL{fl_digits}/full/{w},/0/default.jpg'

    This is the AUTHORITATIVE source of FL ids for NLI images — nli_crossref
    FGPImageNumberId is a Friedberg photo number, not an FL id.
    """
```

From genizah_core.py (volume suffix resolver — already imported elsewhere
in genizah_app.py at L6804, L6931, L9096):
```python
def resolve_volume_suffix(sys_id: str, ie_id: Optional[str]) -> int:
    """Map (sys_id, ie_id) → integer suffix for the NLI IIIF manifest URL.
    Returns 0 when ie_id is not found in the volume map; returns 1 as
    default for single-IE shelfmarks when ie_id is None/1.
    Canonical usage: suffix = resolve_volume_suffix(sid, vol_ie) if vol_ie else 1
    """
```

From web/api.py (pre-existing, line numbers from 2026-04-19 tree):
```python
# L304-309: nli_svc already initialized at module scope
from shared.nli_crossref_service import get_nli_crossref_service
nli_svc = get_nli_crossref_service(thread_safe=True)

# L497-568: nli_image_by_sysid(sys_id, page, width=2000, suffix=1) ->
# Response. The image-fetch body (IIIF GET + min-size check + cache
# write + Response build) is what we need to extract. Internally this
# uses fetch_fl_ids_from_nli(sys_id, suffix) which resolves FL ids from
# the NLI IIIF manifest (the SAME canvas_map-derived source as
# genizah_core.fetch_iiif_manifest — they share the cache file
# nli_fl_ids_cache.json).

# L573-634: cambridge_image(sys_id, page=0) -> Response.
# Currently does: cached = state.meta_mgr.nli_cache.get(sys_id, {})
#                 images_ext = cached.get('images_ext', [])
#                 canvas_entry = images_ext[page]  # positional — the bug
#                 img_url = f"{canvas_entry['url']}/full/2000,/0/default.jpg"

_cambridge_image_cache: dict  # keyed by (sys_id, page) — needs version bump
```

From genizah_core.py::fetch_external_iiif_data (L3934-4010):
```python
# Already parses folio_num from canvas label:
#   lbl_match = re.match(r'^(\d+)', str(lbl).strip())
#   folio_num = int(lbl_match.group(1)) if lbl_match else None
# Canvas entry shape: {'label': lbl, 'url': img_id, 'folio_num': folio_num}
# We extend to also capture folio_side ('r'|'v'|None).
```

From desktop/widgets.py (L94-152):
```python
def _get_folio_image_index(meta, folio_num, side_offset=0) -> int: ...
def _get_initial_image_index(meta, page_num) -> int: ...
# These return an integer index into meta['images_ext'] for a given
# folio_num. They do NOT currently match on side. We add a sibling
# helper _get_folio_side_image_index(meta, folio_num, side) that
# returns None when no exact (folio_num, side) canvas exists.
```

Desktop call-site inventory (genizah_app.py, evidence-based post-Codex
review). Each site below is verified against the actual code at the cited
line range (reads done 2026-04-19). Executor MUST follow the MODIFY or
LEAVE-ALONE verdict; if behavior deviates during implementation, note in
SUMMARY.md "Deviations from plan" — do not silently expand scope.

```python
# ===================================================================
# MODIFY — L6950-6952 (browse-tab page-load, active_source='cambridge'):
# ===================================================================
# Evidence (verified 2026-04-19):
#   6950:     folio_num = _get_folio_number_from_shelfmark(shelf)
#   6951:     idx = _get_initial_image_index(display_meta, folio_num if folio_num is not None else self.current_browse_p)
#   6952:     self.browse_viewer.load_images(display_meta, idx, target_folio=folio_num)
# This is the primary CUDL positional-lookup site on INITIAL page-load.
# When active_source='cambridge', display_meta['images_ext'] is the CUDL
# canvas list (filtered to active volume at L6928-6948). Replace `idx`
# computation with the resolver + synthetic-NLI-entry injection (see
# Task 2b).

# ===================================================================
# MODIFY — L9120-9124 (switch-source reload, reached via switch_to_cambridge):
# ===================================================================
# Evidence (verified 2026-04-19):
#   9120:     if hasattr(self, 'browse_viewer') and not self.browse_reading_desk_active:
#   9121:         shelfmark, _ = self.meta_mgr.get_meta_for_id(sid)
#   9122:         folio_num = _get_folio_number_from_shelfmark(shelfmark)
#   9123:         idx = _get_initial_image_index(display_meta, folio_num if folio_num is not None else self.current_browse_p)
#   9124:         self.browse_viewer.load_images(display_meta, idx, target_folio=folio_num)
# After switching active_source (user clicks Cambridge source button),
# this re-invokes load_images with a positional _get_initial_image_index
# call. Scope: ONLY when the new active source is 'cambridge'. For 'nli'
# or other sources, keep the existing _get_initial_image_index path
# untouched.

# ===================================================================
# MODIFY — L21010-21011 (folio-nav Oxford folio_range load_images):
# ===================================================================
# Evidence (verified 2026-04-19):
#   21004:    if not folio_in_viewer and folio_num is not None and meta:
#   21005:        # Check if this is Oxford with folio_range that includes this folio
#   21006:        oxford_part_meta = meta.get('oxford_part_metadata', {})
#   21007:        folio_range = oxford_part_meta.get('folio_range', [])
#   21008:        if meta.get('oxford_part_id') and len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
#   21009:            # Need dynamic images - call load_images
#   21010:            idx = _get_folio_image_index(meta, folio_num, side_offset=side_offset)
#   21011:            self.browse_viewer.load_images(meta, idx, target_folio=folio_num)
# VERDICT (unchanged from prior plan): LEAVE ALONE for CUDL purposes.
# This branch is gated on `meta.get('oxford_part_id') and len(folio_range)
# >= 2 and folio_range[0] <= folio_num <= folio_range[1]`. Oxford part
# ids are not set on CUL CUDL shelfmarks (they come from Oxford Bodleian
# manifests). A CUL manuscript like T-S NS 158.112 does not reach this
# branch. Add a one-line comment:
#   # TODO(260419-cfx follow-up): evaluate whether CUDL sys_ids ever reach
#   # this branch; currently gated on oxford_part_id + folio_range.

# ===================================================================
# MODIFY — L21012-21014 (folio-nav generic elif, active_list set_page):
# ===================================================================
# Evidence (verified 2026-04-19, Codex concern H-R1 CONFIRMED):
#   21000:    # Use active_list (includes both images_ext and images_nli depending on source)
#   21001:    viewer_images = getattr(self.browse_viewer, 'active_list', []) or getattr(self.browse_viewer, 'images_ext', [])
#   21002:    folio_in_viewer = any(img.get('folio_num') == folio_num for img in viewer_images) if folio_num and viewer_images else False
#   ...
#   21012:        elif viewer_images:
#   21013:            idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
#   21014:            self.browse_viewer.set_page(idx)
# REVISED VERDICT: MODIFY. Codex was right — this path IS reachable for
# CUL CUDL. `viewer_images = active_list` is the currently-displayed
# image list; when active_source='cambridge' it is the CUDL canvas list
# (12 canvases for T-S NS 158.112). Prev/next navigation to folio 8 sets
# folio_num=8, folio_in_viewer=False (CUDL only has 1-6), then hits this
# elif branch and calls _get_folio_image_index which has no canvas for
# folio 8 → returns a fallback (last prior folio) and set_page displays
# the WRONG canvas. This MUST be wired to the resolver when
# active_source='cambridge'. See Task 2b wiring.

# ===================================================================
# MODIFY — L21015-21017 (folio-nav outer elif, active_list set_page):
# ===================================================================
# Evidence (verified 2026-04-19, Codex concern H-R1 CONFIRMED):
#   21015:    elif viewer_images:
#   21016:        idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
#   21017:        self.browse_viewer.set_page(idx)
# REVISED VERDICT: MODIFY. Same reasoning as L21012-21014: this is the
# outer-else branch when folio_in_viewer=True and folio_num might still
# be outside the CUDL canvas coverage (edge case: the viewer has SOME
# canvases for the folio but not the correct side). Wire to the resolver
# when active_source='cambridge'. See Task 2b.

# ===================================================================
# MODIFY — L22500-22505 (composition-summary Oxford folio_range load_images):
# ===================================================================
# Evidence (verified 2026-04-19):
#   22500:    if not folio_in_viewer and folio_num is not None and meta:
#   22501:        oxford_part_meta = meta.get('oxford_part_metadata', {})
#   22502:        folio_range = oxford_part_meta.get('folio_range', [])
#   22503:        if meta.get('oxford_part_id') and len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
#   22504:            image_idx = _get_folio_image_index(meta, folio_num, side_offset=side_offset)
#   22505:            self.browse_viewer.load_images(meta, image_idx, target_folio=folio_num)
#   22506:            return
# VERDICT (unchanged): LEAVE ALONE. Same gating as L21008 — Oxford
# folio_range branch, not reachable for CUL CUDL. Out of scope.

# ===================================================================
# MODIFY — L22508-22510 (composition-summary generic elif, active_list set_page):
# ===================================================================
# Evidence (verified 2026-04-19, Codex concern H-R1 CONFIRMED):
#   22508:    if viewer_images and self.browse_viewer.active_list:
#   22509:        image_idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num, side_offset=side_offset)
#   22510:        self.browse_viewer.set_page(image_idx)
# REVISED VERDICT: MODIFY. This is the composition-summary navigation
# equivalent of L21015-21017. Same reasoning: when active_source='cambridge'
# and viewer_images is the CUDL canvas list, set_page(idx) from
# _get_folio_image_index can land on the wrong canvas for folios outside
# CUDL coverage. Wire to the resolver. See Task 2b.

# ===================================================================
# Summary of verdicts (REVISED from prior plan per H-R1):
#   MODIFY  — L6950-6952   (browse load, initial)
#   MODIFY  — L9120-9124   (switch-source reload)
#   MODIFY  — L21012-21014 (folio-nav elif, set_page)   ← flipped from LEAVE-ALONE
#   MODIFY  — L21015-21017 (folio-nav outer elif, set_page) ← flipped from LEAVE-ALONE
#   MODIFY  — L22508-22510 (composition-summary elif, set_page) ← flipped from LEAVE-ALONE
#   LEAVE-ALONE — L21010-21011 (Oxford folio_range load_images — gated on oxford_part_id)
#   LEAVE-ALONE — L22504-22505 (Oxford folio_range load_images — gated on oxford_part_id)
# ===================================================================
```

From scripts/debug_ts_ns_158_112_image_alignment.py:
```python
# Already prints the alignment table and verdicts. We ADD one block that
# calls resolve_cambridge_canvas_for_page for pages 1..14 and prints the
# resolved canvas index or 'NLI_FALLBACK', which post-fix should be:
#   N=1..12 → canvas_index = N-1
#   N=13    → NLI_FALLBACK
#   N=14    → NLI_FALLBACK
```

From docs/OPEN_ISSUES.md (L82-83, Last Updated L3):
```
| **CUL positional image mismatch: `/api/cambridge_image/{sys_id}?page={p-1}`
  indexes a CUDL canvas list whose length does not match transcription
  page count (260419-nwv follow-up)** | web/api.py (cambridge_image
  handler), web/pages/browse.py:3440-3441 | ❌ Open | ... |
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add folio+side parser and (sys_id, page) → canvas resolver + T-S NS 158.112 unit tests</name>
  <files>
    genizah_core.py,
    shared/nli_crossref_service.py,
    tests/test_nli_crossref_service.py
  </files>
  <behavior>
    - Canvas side extraction (genizah_core.fetch_external_iiif_data):
      - Label '1r'  → {'folio_num': 1, 'folio_side': 'r'}
      - Label '1v'  → {'folio_num': 1, 'folio_side': 'v'}
      - Label '1'   → {'folio_num': 1, 'folio_side': 'r'}   # bare-numeric convention (recto)
      - Label 'f.2r' → {'folio_num': 2, 'folio_side': 'r'}
      - Label 'f. 2v' → {'folio_num': 2, 'folio_side': 'v'}
      - Label 'Binding' → {'folio_num': None, 'folio_side': None}
      - Label '6R' (uppercase) → {'folio_num': 6, 'folio_side': 'r'}
    - resolve_cambridge_canvas_for_page(sys_id, page, images_ext, *, svc=None):
      - Given the T-S NS 158.112 fixture (14 nli_images rows sorted
        1r,1v,...,8r,8v + 12 CUDL canvases 1r..6v):
          page=0 → {'canvas_index': 0, 'folio_num': 1, 'side': 'r'}
          page=1 → {'canvas_index': 1, 'folio_num': 1, 'side': 'v'}
          page=2 → {'canvas_index': 2, 'folio_num': 2, 'side': 'r'}
          ...
          page=11 → {'canvas_index': 11, 'folio_num': 6, 'side': 'v'}
          page=12 → None  (folio 8r — no CUDL canvas; callers use NLI fallback)
          page=13 → None  (folio 8v — no CUDL canvas; callers use NLI fallback)
      - If get_folio_images(sys_id) returns [] OR svc.is_available() is
        False: return sentinel {'degraded': True} so callers can choose
        legacy positional behavior (not None, which means "NLI fallback").
      - If page < 0 or page >= len(nli_images_sorted): return None.
      - If side matching fails but a canvas exists with the same
        folio_num and NO side (bare numeric label like '1'): match it
        when the target side is 'r'; do NOT match for 'v'.
  </behavior>
  <action>
    Step 0 — Verify the fixture-schema baseline BEFORE writing resolver
    tests.

    `NliCrossrefService.__init__` only reads the sqlite file (opens it in
    read-only mode) and sets `self._conn`. It does NOT query the `meta`
    table during construction. `is_available()` just checks
    `self._conn is not None`. Therefore a bare schema with an empty
    `meta` table and a populated `nli_images` table is enough.

    Sanity-check this once before writing `TestFolioSideResolver`:
    1. Create the fixture sqlite DB with:
       ```
       CREATE TABLE meta (key TEXT, value TEXT);
       CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT,
           FGPNumber TEXT, ImageName TEXT, ImageSourceName TEXT,
           Shelfmark TEXT, Material TEXT DEFAULT '', NumFolio TEXT
           DEFAULT '', NumBifolio TEXT DEFAULT '', Size TEXT DEFAULT '',
           LibraryAbbrev TEXT DEFAULT '', LibraryNameEng TEXT DEFAULT '',
           CatalogEntry TEXT DEFAULT '', CollectionName TEXT DEFAULT '',
           OBBox TEXT DEFAULT '', OBVolume TEXT DEFAULT '', OBFolio TEXT
           DEFAULT '', PartOf TEXT DEFAULT '', See TEXT DEFAULT '',
           BifolioWith TEXT DEFAULT '');
       ```
    2. Insert the 14 TS_NS_158_112 rows.
    3. In an ad-hoc REPL run: `svc = NliCrossrefService(db_path=...);
       assert svc.is_available(); assert svc.get_folio_images(
       '990051537270205171')` returns 14 rows with `folio_label` set.

    If `is_available()` returns False against this bare schema (it
    should not, per current __init__ code), seed a version row:
    `INSERT INTO meta VALUES ('version', '0.0.0-test')` and note the
    change in a comment on the fixture. Then proceed.

    Step 1 — Extend `images_ext` canvas schema (genizah_core.py L3934-4010):

    In `fetch_external_iiif_data`, replace the current label-parsing
    block (the one that sets only `folio_num`) with a small helper that
    also extracts `folio_side`. Keep the existing public behavior: every
    existing key on canvas entries remains, only ADD a `folio_side` key.

    > **DO NOT MODIFY `fetch_fl_ids_from_nli`** (web/api.py).
    > The resolver extraction is a NEW module-level helper in
    > shared/nli_crossref_service.py. The existing FL-id resolution
    > function in web/api.py MUST NOT be refactored, inlined, or renamed
    > as part of this task — CONTEXT.md explicitly drops H3 and
    > `fetch_fl_ids_from_nli` is known-correct (it reads from the NLI
    > IIIF manifest canvas_map, which is the authoritative FL-id source).

    ```python
    # Replace the existing lbl_match block with a single helper call.
    # Add this helper near the top of fetch_external_iiif_data (or at
    # module scope as a private _parse_cudl_label).
    _CUDL_LABEL_RE = re.compile(r'^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b',
                                re.IGNORECASE)

    def _parse_cudl_label(lbl):
        """Return (folio_num:int|None, folio_side:'r'|'v'|None).
        Convention: bare numeric label (no 'r'/'v' suffix) is treated as
        recto. Non-numeric labels ('Binding', 'Cover') return (None, None).
        """
        if not lbl:
            return (None, None)
        m = _CUDL_LABEL_RE.match(str(lbl).strip())
        if not m:
            return (None, None)
        try:
            folio_num = int(m.group(1))
        except (TypeError, ValueError):
            return (None, None)
        side_raw = m.group(2)
        side = side_raw.lower() if side_raw else 'r'  # bare numeric → recto
        return (folio_num, side)
    ```

    Then inside the canvas loop replace:
    ```python
    folio_num = None
    lbl_match = re.match(r'^(\d+)', str(lbl).strip())
    if lbl_match:
        try:
            folio_num = int(lbl_match.group(1))
        except (TypeError, ValueError):
            pass
    result['canvases'].append({'label': lbl, 'url': img_id, 'folio_num': folio_num})
    ```
    with:
    ```python
    folio_num, folio_side = _parse_cudl_label(lbl)
    result['canvases'].append({
        'label': lbl, 'url': img_id,
        'folio_num': folio_num, 'folio_side': folio_side,
    })
    ```

    Downstream code that already reads `folio_num` continues to work;
    `folio_side` is new and additive. No other callsites need to change
    in this task.

    Step 2 — Add `resolve_cambridge_canvas_for_page` to
    shared/nli_crossref_service.py (module-level function, NOT a method —
    it takes the service as an optional arg so web/api.py can pass the
    module-level singleton it already holds):

    ```python
    # Module-level constant. Callers check via `result.get('degraded')`,
    # NOT by importing or identity-comparing this sentinel.
    _DEGRADED = {'degraded': True}

    def _extract_side_from_nli_label(folio_label: str) -> Optional[str]:
        """Return 'r'|'v' from a label like '1r'/'8v'; None if neither."""
        if not folio_label:
            return None
        last = folio_label[-1].lower()
        if last in ('r', 'v'):
            return last
        return None

    def resolve_cambridge_canvas_for_page(
        sys_id: str,
        page: int,
        images_ext: list[dict],
        *,
        svc: Optional['NliCrossrefService'] = None,
    ) -> Optional[dict]:
        """Map transcription page index → CUDL canvas index using
        folio+side, with NLI fallback when no canvas matches.

        Returns:
          - {'canvas_index': int, 'folio_num': int, 'side': 'r'|'v'} on match
          - None when no canvas matches for this page's (folio, side)
            (caller should serve the NLI image for that page)
          - {'degraded': True} when sidecar unavailable or sys_id has no
            nli_images rows (caller should fall back to legacy positional
            behavior, i.e. images_ext[page])
        """
        if svc is None:
            svc = get_nli_crossref_service(thread_safe=True)
        if svc is None or not svc.is_available():
            return dict(_DEGRADED)
        folio_rows = svc.get_folio_images(sys_id)
        if not folio_rows:
            return dict(_DEGRADED)
        if page < 0 or page >= len(folio_rows):
            return None  # Out of nli_images range → NLI fallback will also 404; that is fine
        target = folio_rows[page]
        target_folio_str = target.get('folio_label', '')
        # Parse '1r' → (1, 'r'); 'auxiliary' fallback label like '7' → (7, None)
        m = re.match(r'^(\d+)([rv])?$', target_folio_str, re.IGNORECASE)
        if not m:
            return None
        try:
            target_folio = int(m.group(1))
        except (TypeError, ValueError):
            return None
        target_side = m.group(2).lower() if m.group(2) else 'r'

        # Exact (folio_num, side) match first
        for idx, c in enumerate(images_ext or []):
            if c.get('folio_num') == target_folio and c.get('folio_side') == target_side:
                return {'canvas_index': idx, 'folio_num': target_folio, 'side': target_side}

        # Legacy-shaped entries with no folio_side: match only when
        # target_side is 'r' AND canvas label has no side letter. Skip
        # verso targets against side-less canvases.
        if target_side == 'r':
            for idx, c in enumerate(images_ext or []):
                if c.get('folio_num') == target_folio and not c.get('folio_side'):
                    return {'canvas_index': idx, 'folio_num': target_folio, 'side': 'r'}

        return None
    ```

    Step 3 — Add tests to tests/test_nli_crossref_service.py
    (append at end of file, before the last module-level blank line):

    ```python
    class TestFolioSideResolver:
        """T-S NS 158.112 fixture: 14 nli_images rows (incl. paired-leaf
        bifolio ImageNames), 12 CUDL canvases 1r..6v.
        Validates resolve_cambridge_canvas_for_page for all 14 pages."""

        TS_NS_158_112_IMAGE_NAMES = [
            'T_S_NS_158_112__L1_12F0B0S1',  # 1r
            'T_S_NS_158_112__L1_12F0B0S2',  # 1v
            'T_S_NS_158_112__L2_11F0B0S1',  # 2r
            'T_S_NS_158_112__L2_11F0B0S2',  # 2v
            'T_S_NS_158_112__L3_10F0B0S1',  # 3r
            'T_S_NS_158_112__L3_10F0B0S2',  # 3v
            'T_S_NS_158_112__L4_9F0B0S1',   # 4r
            'T_S_NS_158_112__L4_9F0B0S2',   # 4v
            'T_S_NS_158_112__L5F0B0S1',     # 5r
            'T_S_NS_158_112__L5F0B0S2',     # 5v
            'T_S_NS_158_112__L6_7F0B0S1',   # 6r
            'T_S_NS_158_112__L6_7F0B0S2',   # 6v
            'T_S_NS_158_112__L8F0B0S1',     # 8r — NO CUDL canvas
            'T_S_NS_158_112__L8F0B0S2',     # 8v — NO CUDL canvas
        ]

        TS_NS_158_112_CUDL_CANVASES = [
            {'label': '1r', 'url': 'https://x/1r', 'folio_num': 1, 'folio_side': 'r'},
            {'label': '1v', 'url': 'https://x/1v', 'folio_num': 1, 'folio_side': 'v'},
            {'label': '2r', 'url': 'https://x/2r', 'folio_num': 2, 'folio_side': 'r'},
            {'label': '2v', 'url': 'https://x/2v', 'folio_num': 2, 'folio_side': 'v'},
            {'label': '3r', 'url': 'https://x/3r', 'folio_num': 3, 'folio_side': 'r'},
            {'label': '3v', 'url': 'https://x/3v', 'folio_num': 3, 'folio_side': 'v'},
            {'label': '4r', 'url': 'https://x/4r', 'folio_num': 4, 'folio_side': 'r'},
            {'label': '4v', 'url': 'https://x/4v', 'folio_num': 4, 'folio_side': 'v'},
            {'label': '5r', 'url': 'https://x/5r', 'folio_num': 5, 'folio_side': 'r'},
            {'label': '5v', 'url': 'https://x/5v', 'folio_num': 5, 'folio_side': 'v'},
            {'label': '6r', 'url': 'https://x/6r', 'folio_num': 6, 'folio_side': 'r'},
            {'label': '6v', 'url': 'https://x/6v', 'folio_num': 6, 'folio_side': 'v'},
        ]

        @pytest.fixture
        def ts_ns_158_112_svc(self, tmp_path):
            # Build an in-memory-like sqlite DB with just the 14 rows needed.
            # Bare schema — NliCrossrefService.__init__ does NOT require any
            # rows in `meta` (see Task 1 Step 0). If that changes in future,
            # seed a ('version','0.0.0-test') row here.
            import sqlite3
            db_path = tmp_path / "nli_crossref.db"
            conn = sqlite3.connect(str(db_path))
            conn.execute("CREATE TABLE meta (key TEXT, value TEXT)")
            conn.execute("CREATE TABLE nli_images (NLI_AlmaId TEXT, FGPImageNumberId TEXT, FGPNumber TEXT, ImageName TEXT, ImageSourceName TEXT, Shelfmark TEXT, Material TEXT DEFAULT '', NumFolio TEXT DEFAULT '', NumBifolio TEXT DEFAULT '', Size TEXT DEFAULT '', LibraryAbbrev TEXT DEFAULT '', LibraryNameEng TEXT DEFAULT '', CatalogEntry TEXT DEFAULT '', CollectionName TEXT DEFAULT '', OBBox TEXT DEFAULT '', OBVolume TEXT DEFAULT '', OBFolio TEXT DEFAULT '', PartOf TEXT DEFAULT '', See TEXT DEFAULT '', BifolioWith TEXT DEFAULT '')")
            for idx, img_name in enumerate(self.TS_NS_158_112_IMAGE_NAMES):
                conn.execute(
                    "INSERT INTO nli_images (NLI_AlmaId, FGPImageNumberId, FGPNumber, ImageName, ImageSourceName, Shelfmark) VALUES (?, ?, ?, ?, ?, ?)",
                    ("990051537270205171", f"FGP{idx}", str(idx), img_name, "", "T-S NS 158.112"),
                )
            conn.commit()
            conn.close()
            svc = NliCrossrefService(db_path=str(db_path))
            # Smoke-check before any resolver test runs — if this fails,
            # the resolver tests below would all return {'degraded': True}
            # and silently pass wrong assertions.
            assert svc.is_available(), (
                "Fixture sqlite is_available() must be True. "
                "If False, seed a meta.version row or inspect __init__."
            )
            assert len(svc.get_folio_images("990051537270205171")) == 14
            yield svc
            svc.close()

        @pytest.mark.parametrize("page,expected_canvas_idx,expected_folio,expected_side", [
            (0, 0, 1, 'r'),
            (1, 1, 1, 'v'),
            (2, 2, 2, 'r'),
            (3, 3, 2, 'v'),
            (4, 4, 3, 'r'),
            (5, 5, 3, 'v'),
            (6, 6, 4, 'r'),
            (7, 7, 4, 'v'),
            (8, 8, 5, 'r'),
            (9, 9, 5, 'v'),
            (10, 10, 6, 'r'),
            (11, 11, 6, 'v'),
        ])
        def test_resolves_exact_canvas_for_pages_0_through_11(
            self, ts_ns_158_112_svc, page, expected_canvas_idx,
            expected_folio, expected_side,
        ):
            from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
            out = resolve_cambridge_canvas_for_page(
                "990051537270205171", page,
                self.TS_NS_158_112_CUDL_CANVASES,
                svc=ts_ns_158_112_svc,
            )
            assert out == {
                'canvas_index': expected_canvas_idx,
                'folio_num': expected_folio,
                'side': expected_side,
            }

        def test_returns_none_for_page_12_folio_8r_no_canvas(self, ts_ns_158_112_svc):
            from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
            out = resolve_cambridge_canvas_for_page(
                "990051537270205171", 12,
                self.TS_NS_158_112_CUDL_CANVASES,
                svc=ts_ns_158_112_svc,
            )
            assert out is None

        def test_returns_none_for_page_13_folio_8v_no_canvas(self, ts_ns_158_112_svc):
            from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
            out = resolve_cambridge_canvas_for_page(
                "990051537270205171", 13,
                self.TS_NS_158_112_CUDL_CANVASES,
                svc=ts_ns_158_112_svc,
            )
            assert out is None

        def test_degraded_when_sys_id_unknown(self, ts_ns_158_112_svc):
            from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
            out = resolve_cambridge_canvas_for_page(
                "UNKNOWN_SYS_ID", 0,
                self.TS_NS_158_112_CUDL_CANVASES,
                svc=ts_ns_158_112_svc,
            )
            assert out == {'degraded': True}

        def test_bare_numeric_label_matches_recto_only(self, ts_ns_158_112_svc):
            """A canvas with folio_num=1 and folio_side=None (bare '1'
            label) should match target (1, 'r') but NOT (1, 'v')."""
            from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
            side_less_canvases = [
                {'label': '1', 'url': 'https://x/1', 'folio_num': 1, 'folio_side': None},
            ]
            # Page 0 = 1r → should match
            out_recto = resolve_cambridge_canvas_for_page(
                "990051537270205171", 0, side_less_canvases, svc=ts_ns_158_112_svc,
            )
            # Page 1 = 1v → should NOT match → None (NLI fallback)
            out_verso = resolve_cambridge_canvas_for_page(
                "990051537270205171", 1, side_less_canvases, svc=ts_ns_158_112_svc,
            )
            assert out_recto == {'canvas_index': 0, 'folio_num': 1, 'side': 'r'}
            assert out_verso is None
    ```

    Also add four small label-parser tests using _parse_cudl_label
    (import from genizah_core, or if it's nested inside the function,
    promote it to module scope so it is importable). If promoting, put
    it as `def _parse_cudl_label(lbl)` at module scope in genizah_core.py
    alongside the existing regex constants.

    Note on import shape: `_parse_cudl_label` is a private helper, but
    exposing it at module scope is fine — it is not in __all__ and it is
    documented as internal. The test imports it as
    `from genizah_core import _parse_cudl_label`.

    ```python
    def test_parse_cudl_label_bare_numeric_is_recto():
        from genizah_core import _parse_cudl_label
        assert _parse_cudl_label('1') == (1, 'r')

    def test_parse_cudl_label_verso():
        from genizah_core import _parse_cudl_label
        assert _parse_cudl_label('1v') == (1, 'v')

    def test_parse_cudl_label_binding():
        from genizah_core import _parse_cudl_label
        assert _parse_cudl_label('Binding') == (None, None)

    def test_parse_cudl_label_with_f_prefix():
        from genizah_core import _parse_cudl_label
        assert _parse_cudl_label('f.2v') == (2, 'v')
        assert _parse_cudl_label('f. 3r') == (3, 'r')
    ```

    Test count for this task: **20 new pytest cases**
      - 12 parametrized resolver tests (pages 0..11 of T-S NS 158.112)
      - 4 resolver edge tests (page 12 None, page 13 None, unknown sys_id
        degraded, bare-numeric recto-only match)
      - 4 _parse_cudl_label tests (bare numeric, verso, binding, f-prefix
        — the f-prefix test has two asserts but is one test function)
  </action>
  <verify>
    <automated>pytest tests/test_nli_crossref_service.py -x -q</automated>
    <!-- Must pass: 4 new _parse_cudl_label tests + 12 parametrized
         resolver tests + 4 edge cases = 20 new tests + pre-existing
         77 tests. Total expected: 97 pass, 0 fail. The fixture's
         `is_available()` smoke-assert in the fixture setup is the
         Step 0 verification. -->
  </verify>
  <done>
    (1) `genizah_core.py` has module-level `_parse_cudl_label(lbl)` and
    `fetch_external_iiif_data` returns canvas entries with a `folio_side`
    key. (2) `shared/nli_crossref_service.py` exports
    `resolve_cambridge_canvas_for_page(sys_id, page, images_ext, *, svc=None)`.
    (3) `tests/test_nli_crossref_service.py` has `TestFolioSideResolver`
    + four module-level `_parse_cudl_label` tests (20 new cases total).
    (4) `pytest tests/test_nli_crossref_service.py -x -q` exits 0.
    (5) `fetch_fl_ids_from_nli` in web/api.py is UNCHANGED.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2a: Wire folio+side resolver into web cambridge_image (NLI fallback + cache bump + resolver-version headers)</name>
  <files>
    web/api.py
  </files>
  <behavior>
    - GET /api/cambridge_image/990051537270205171?page=N for N in 0..11 returns a CUDL JPEG identical to today's response (verified via Content-Length within ±5% and Content-Type=image/jpeg; exact bytes match when network/cache are warm). Response carries X-Image-Resolver-Version: 2 and ETag "{sys_id}-p{page}-v2".
    - GET /api/cambridge_image/990051537270205171?page=12 and ?page=13 return a JPEG (NLI-fetched) with Content-Type=image/jpeg and response headers:
        X-Image-Fallback-Source: nli
        X-Image-Resolver-Version: 2
        X-Folio-Matched: 8r (for page=12) or 8v (for page=13)
        ETag: "{sys_id}-p{page}-v2"
    - GET /api/cambridge_image/OTHER_SYS_ID?page=K where nli_crossref sidecar has no rows for OTHER_SYS_ID returns the legacy positional canvas response (no regression for JTS/Oxford/other libraries — they already use different endpoints, but shelfmarks with images_ext and no nli_images rows must still work). Still carries X-Image-Resolver-Version and ETag.
    - When the sidecar file is absent at startup, `cambridge_image` logs one WARNING per sys_id and returns the legacy positional canvas response. It does not raise HTTP 500.
    - Cache: `_CAMBRIDGE_CACHE_VERSION = 2` is included in the cache key. After deploy, old entries keyed without the version are never served.
    - NLI fallback logs the resolved fl_id (or the page index if fl_id is unknown) at INFO level, prefixed with "cambridge_image NLI fallback".
  </behavior>
  <action>
    Step 1 — Extract `_fetch_nli_image_bytes` from `nli_image_by_sysid`
    in web/api.py. The helper takes the same args and returns either a
    `(bytes, content_type, fl_id)` tuple on success or `None` on failure.
    Returning the fl_id lets the caller log it (per Gemini L-R3 nice-to-have).

    > **DO NOT MODIFY `fetch_fl_ids_from_nli` in web/api.py.** This is
    > the function that resolves FL ids from the NLI IIIF manifest and is
    > known-correct (CONTEXT.md H3 drop). The extraction below moves ONLY
    > the image-fetch body (IIIF GET + min-size check + cache write) out
    > of `nli_image_by_sysid`; it does not touch FL id resolution.

    ```python
    # Insert just above `nli_image_by_sysid` (around line 497).
    def _fetch_nli_image_bytes(sys_id: str, page: int, width: int = 2000, suffix: int = 1):
        """Internal helper: return (bytes, content_type, fl_id) for the NLI
        image at (sys_id, page, width, suffix), or None if nothing was
        retrievable. fl_id is the FL digit string that succeeded (for
        logging/observability). Uses the existing `fetch_fl_ids_from_nli`
        cache + the existing `_image_cache` (via same cache_key shape).

        FL ids come from fetch_fl_ids_from_nli (NLI IIIF manifest
        canvas_map). NEVER from nli_crossref FGPImageNumberId (Friedberg
        photo number, not an FL id; see PITFALLS.md Pitfall 6).
        """
        import time as _time
        width = max(100, min(width, 2000))
        cache_key = (sys_id, page, width, suffix)
        if cache_key in _image_cache:
            entry = _image_cache[cache_key]
            # Pre-existing cache entries are 3-tuples (content, ct, ts);
            # new ones are 4-tuples with fl_id. Support both.
            if len(entry) == 4:
                content, content_type, fl_id, cached_at = entry
            else:
                content, content_type, cached_at = entry
                fl_id = None
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                return (content, content_type, fl_id)

        fl_ids = fetch_fl_ids_from_nli(sys_id, suffix=suffix)
        if not fl_ids:
            return None

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://www.nli.org.il/',
        }

        def _try_fl(fl_id):
            iiif_url = f"https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/{width},/0/default.jpg"
            try:
                resp = requests.get(iiif_url, headers=headers, timeout=15, verify=True)
                min_size = 1000 if width < 500 else 5000
                ct = resp.headers.get('Content-Type', '')
                if resp.status_code == 200 and 'image' in ct and len(resp.content) > min_size:
                    return (resp.content, ct or 'image/jpeg')
            except Exception:
                return None
            return None

        if 0 <= page < len(fl_ids):
            got = _try_fl(fl_ids[page])
            if got is not None:
                _image_cache[cache_key] = (got[0], got[1], fl_ids[page], _time.time())
                return (got[0], got[1], fl_ids[page])

        for fl_id in fl_ids:
            got = _try_fl(fl_id)
            if got is not None:
                _image_cache[cache_key] = (got[0], got[1], fl_id, _time.time())
                return (got[0], got[1], fl_id)

        return None
    ```

    Then rewrite `nli_image_by_sysid` to call the helper (preserves
    existing behavior; now ~15 lines instead of ~70):

    ```python
    @app.get('/api/nli_image_by_sysid/{sys_id}')
    def nli_image_by_sysid(sys_id: str, page: int = 0, width: int = 2000, suffix: int = 1):
        got = _fetch_nli_image_bytes(sys_id, page, width=width, suffix=suffix)
        if got is None:
            return Response(content="Image not found", status_code=404)
        content, ct, _fl_id = got
        return Response(
            content=content,
            media_type=ct,
            headers={"Cache-Control": "public, max-age=600"},
        )
    ```

    Sanity check: manually diff behavior — the extraction removes the
    in-function cache short-circuit at the top of the public endpoint,
    but since `_fetch_nli_image_bytes` itself consults `_image_cache`
    first, the observable caching behavior is identical.

    Step 2 — Rewrite `cambridge_image` in web/api.py (replaces L576-634).

    ```python
    # Bump this AND the ETag version suffix (below) together when the
    # resolver contract changes (e.g. when the N→canvas resolution rule
    # changes). Clients revalidate via ETag, server-side cache resets via
    # key mismatch. Both must change in lockstep.
    _CAMBRIDGE_CACHE_VERSION = 2
    _CAMBRIDGE_ETAG_VERSION = "v2"  # keep in sync with _CAMBRIDGE_CACHE_VERSION
    _cambridge_degraded_warned = set()  # sys_ids we have already warned about

    @app.get('/api/cambridge_image/{sys_id}')
    def cambridge_image(sys_id: str, page: int = 0):
        """Fetch Cambridge IIIF image by System ID with folio+side
        matching against nli_crossref. Falls back to NLI image when no
        CUDL canvas matches the target (folio_num, side).
        """
        import time as _time
        # Import ONLY the resolver. The {'degraded': True} sentinel is
        # checked via `resolved.get('degraded')` — do NOT import the
        # _DEGRADED module constant; identity-comparing a dict sentinel
        # across `dict(_DEGRADED)` copies is fragile.
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        cache_key = (_CAMBRIDGE_CACHE_VERSION, sys_id, page)

        def _base_headers():
            """Headers attached to every non-error response. ETag lets
            clients revalidate after a deploy; X-Image-Resolver-Version
            is a human-readable deploy marker."""
            return {
                "Cache-Control": "public, max-age=600",
                "ETag": f'"{sys_id}-p{page}-{_CAMBRIDGE_ETAG_VERSION}"',
                "X-Image-Resolver-Version": str(_CAMBRIDGE_CACHE_VERSION),
            }

        if cache_key in _cambridge_image_cache:
            content, content_type, headers_extra, cached_at = _cambridge_image_cache[cache_key]
            if _time.time() - cached_at < IMAGE_CACHE_TTL:
                resp_headers = _base_headers()
                resp_headers.update(headers_extra or {})
                return Response(content=content, media_type=content_type, headers=resp_headers)

        if not state.meta_mgr or not hasattr(state.meta_mgr, 'nli_cache'):
            return Response(content="Metadata not available", status_code=503)

        cached = state.meta_mgr.nli_cache.get(sys_id, {})
        images_ext = cached.get('images_ext', [])
        if not images_ext:
            return Response(content="No Cambridge images available", status_code=404)

        # Resolve the canvas via folio+side. May return None (→ NLI
        # fallback) or {'degraded': True} (→ legacy positional).
        resolved = resolve_cambridge_canvas_for_page(sys_id, page, images_ext, svc=nli_svc)

        canvas_entry = None
        fallback_to_nli = False
        matched_folio_side = None  # e.g. '8r' — set when resolver produced a (folio, side)
        if resolved is None:
            fallback_to_nli = True
            # Even though CUDL has no matching canvas, we may still know
            # the folio/side the page maps to (from nli_images). Read it
            # from the service directly so the X-Folio-Matched header
            # can be set on the NLI fallback response.
            try:
                folio_rows = nli_svc.get_folio_images(sys_id) if nli_svc else []
                if 0 <= page < len(folio_rows):
                    matched_folio_side = folio_rows[page].get('folio_label') or None
            except Exception:
                matched_folio_side = None
        elif resolved.get('degraded'):
            # Sidecar missing or sys_id not in nli_crossref — preserve
            # legacy positional behavior and warn once.
            if sys_id not in _cambridge_degraded_warned:
                logger.warning(
                    "cambridge_image: nli_crossref unavailable for sys_id=%s — "
                    "using legacy positional canvas lookup", sys_id,
                )
                _cambridge_degraded_warned.add(sys_id)
            if 0 <= page < len(images_ext):
                canvas_entry = images_ext[page]
            else:
                return Response(content="Page out of range", status_code=404)
        else:
            idx = resolved['canvas_index']
            matched_folio_side = f"{resolved['folio_num']}{resolved['side']}"
            if 0 <= idx < len(images_ext):
                canvas_entry = images_ext[idx]
            else:
                fallback_to_nli = True

        if fallback_to_nli:
            # KNOWN LIMITATION: suffix=1 is hardcoded here. The
            # /api/cambridge_image endpoint contract has no `suffix` query
            # param (adding one is out of scope per CONTEXT.md). For
            # multi-IE CUL shelfmarks (rare) this will resolve FL ids
            # from the primary IE, which may be the wrong volume.
            # Documented in SUMMARY.md known-limitations.
            got = _fetch_nli_image_bytes(sys_id, page, width=2000, suffix=1)
            if got is None:
                return Response(content="Image not found", status_code=404)
            content, ct, resolved_fl_id = got
            logger.info(
                "cambridge_image NLI fallback: sys_id=%s page=%s folio=%s fl_id=%s",
                sys_id, page, matched_folio_side or "?", resolved_fl_id or "?",
            )
            extra_headers = {"X-Image-Fallback-Source": "nli"}
            if matched_folio_side:
                extra_headers["X-Folio-Matched"] = matched_folio_side
            _cambridge_image_cache[cache_key] = (content, ct, extra_headers, _time.time())
            resp_headers = _base_headers()
            resp_headers.update(extra_headers)
            return Response(content=content, media_type=ct, headers=resp_headers)

        # Normal CUDL fetch path.
        canvas_url = (canvas_entry or {}).get('url', '')
        if not canvas_url:
            return Response(content="No canvas URL for this page", status_code=404)
        img_url = f"{canvas_url}/full/2000,/0/default.jpg"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Referer': 'https://cudl.lib.cam.ac.uk/',
        }
        try:
            resp = requests.get(img_url, headers=headers, timeout=30, verify=True)
            if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
                content_type = resp.headers.get('Content-Type', 'image/jpeg')
                extra_headers = {}
                if matched_folio_side:
                    extra_headers["X-Folio-Matched"] = matched_folio_side
                _cambridge_image_cache[cache_key] = (resp.content, content_type, extra_headers, _time.time())
                resp_headers = _base_headers()
                resp_headers.update(extra_headers)
                return Response(
                    content=resp.content,
                    media_type=content_type,
                    headers=resp_headers,
                )
            return Response(
                content=f"Failed to fetch Cambridge image: {resp.status_code}",
                status_code=resp.status_code,
            )
        except Exception as e:
            return Response(content=f"Error fetching Cambridge image: {e}", status_code=500)
    ```

    Note: the cache value tuple changed from
    `(content, content_type, cached_at)` to
    `(content, content_type, headers_extra, cached_at)`. Since the cache
    key also changed (now includes _CAMBRIDGE_CACHE_VERSION), any legacy
    shapes are dead entries — no compatibility code required.

    Note: the `_image_cache` tuple (used by `_fetch_nli_image_bytes`)
    changed from 3-tuple to 4-tuple (added fl_id). The reader supports
    both shapes for backward compatibility with warm caches.
  </action>
  <verify>
    <automated>pytest tests/test_nli_crossref_service.py -x -q 2>&1 | tail -20</automated>
    <!-- Resolver tests from Task 1 stay green. -->

    <automated>pytest tests/ -x -q -k "cambridge_image or nli_image" 2>&1 | tail -40</automated>
    <!-- If tests/test_web_api_cambridge_image.py exists from a prior
         iteration, run it. If not, create a minimal new test file at
         tests/test_web_api_cambridge_image.py with:

         1. Mock state.meta_mgr.nli_cache with the T-S NS 158.112
            images_ext fixture (12 canvases with folio_num + folio_side).
         2. Monkeypatch nli_svc with a stub that returns the 14-row
            folio list from Task 1.
         3. Monkeypatch requests.get so CUDL URLs return a synthetic
            JPEG with Content-Type image/jpeg and 10KB content, and NLI
            IIIF URLs return a different synthetic JPEG.
         4. Use FastAPI TestClient to assert:
            - GET /api/cambridge_image/990051537270205171?page=0 → 200,
              image/jpeg, body == CUDL synthetic bytes, NO
              X-Image-Fallback-Source header, HAS X-Image-Resolver-Version=2,
              HAS ETag header, HAS X-Folio-Matched=1r.
            - GET /api/cambridge_image/990051537270205171?page=12 → 200,
              image/jpeg, body == NLI synthetic bytes,
              X-Image-Fallback-Source == 'nli', X-Image-Resolver-Version=2,
              X-Folio-Matched=8r.
            - GET /api/cambridge_image/OTHER_UNKNOWN_SYS_ID?page=0 with
              state.meta_mgr.nli_cache configured for OTHER_UNKNOWN_SYS_ID
              (images_ext present but nli_crossref returns []) →
              200, image/jpeg, body == CUDL synthetic bytes (legacy
              positional), HAS X-Image-Resolver-Version=2.
         If the web api is hard to exercise via TestClient in the
         existing test scaffolding, fall back to unit-testing the
         resolver integration directly (import the handler function and
         call it with a mock state). -->
  </verify>
  <done>
    (1) `_fetch_nli_image_bytes` exists in web/api.py and returns a
    3-tuple (bytes, content_type, fl_id). Both `nli_image_by_sysid` and
    the NLI-fallback branch of `cambridge_image` call it.
    (2) `_CAMBRIDGE_CACHE_VERSION = 2` and `_CAMBRIDGE_ETAG_VERSION = "v2"`
    are both in web/api.py and used in the cambridge cache key + ETag.
    (3) Every successful cambridge_image response carries
    `X-Image-Resolver-Version: 2`, an `ETag: "{sys_id}-p{page}-v2"`, and
    (when applicable) `X-Folio-Matched: {folio}{side}`.
    (4) cambridge_image serves NLI image bytes with `X-Image-Fallback-Source: nli`
    header when resolver returns None, logs fl_id at INFO level.
    (5) When nli_crossref is unavailable, cambridge_image logs one
    WARNING per sys_id and serves the legacy positional canvas.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2b: Wire desktop browse CUDL path to resolver (load + navigation sites) with IE-aware NLI fallback</name>
  <files>
    desktop/widgets.py,
    genizah_app.py
  </files>
  <behavior>
    - Opening T-S NS 158.112 (sys_id 990051537270205171) in desktop browse and navigating pages 1..14 with active_source='cambridge' shows 12 correct CUDL folios (1r..6v) for pages 1..12 and NLI images (not repeated 6v, not a black frame) for pages 13..14, on INITIAL page-load, switch-source reload, AND prev/next/folio navigation. No console exception.
    - `_build_nli_iiif_url_for_page` sources the suffix from `resolve_volume_suffix(sys_id, self.current_browse_volume_ie)`. When `current_browse_volume_ie` is None (single-IE shelfmark or not selected), suffix defaults to 1 and a one-line WARNING is logged. It does NOT hardcode suffix=1 unconditionally.
    - When the navigation helpers at L21012-21014, L21015-21017, and L22508-22510 see `active_source='cambridge'` AND the target folio+side is not in the CUDL canvas list, they inject a synthetic NLI fallback entry (same pattern as the load sites) instead of calling `set_page` on a wrong canvas index.
    - Calls to `display_meta['images_ext'] = ...` always operate on a dict copy (`dict(display_meta)`), never on a reference that could mutate the canonical `meta_mgr.nli_cache[sid]` entry.
  </behavior>
  <action>
    Step 1 — Desktop side-aware helper in desktop/widgets.py.

    Add a new function below `_get_folio_image_index` (around L120,
    before `ShelfmarkCompleter`):

    ```python
    def _get_folio_side_image_index(meta, folio_num, side):
        """Return the index of the images_ext entry matching both
        (folio_num, side), or None if no exact match exists.

        'side' is 'r' or 'v'. A canvas with folio_side=None (bare
        numeric label, e.g. just '1') matches only when side='r'.

        This is stricter than _get_folio_image_index: it returns None
        (not a fallback index) when no exact match exists, so callers
        can trigger an NLI fallback.
        """
        if not meta or folio_num is None or side not in ('r', 'v'):
            return None
        images = (meta or {}).get('images_ext') or (meta or {}).get('images') or []
        for idx, img in enumerate(images):
            if img.get('folio_num') == folio_num and img.get('folio_side') == side:
                return idx
        if side == 'r':
            for idx, img in enumerate(images):
                if img.get('folio_num') == folio_num and not img.get('folio_side'):
                    return idx
        return None
    ```

    Step 2 — Add a shared helper method `_resolve_cambridge_page_or_fallback`
    on the browse class in genizah_app.py. This is the single source of
    truth for "given a display_meta and a transcription page, produce
    either a canvas index into display_meta['images_ext'] OR a new
    display_meta with a synthetic NLI-fallback entry appended plus the
    index of that appended entry". Both load-time and navigation-time
    call sites use it.

    Place the helper near the existing browse-tab page-load method (near
    L6920). It must guard against missing services and fall back to
    legacy behavior on any error:

    ```python
    def _resolve_cambridge_page_or_fallback(self, sys_id, page_idx, display_meta, folio_num):
        """Compute (display_meta, idx) for a Cambridge CUDL page using
        folio+side resolution, injecting a synthetic NLI-fallback entry
        when no CUDL canvas matches.

        Args:
            sys_id: Manuscript system id.
            page_idx: 0-based transcription page index.
            display_meta: dict (already volume-filtered if multi-IE).
            folio_num: int|None — for logging/fallback only.

        Returns:
            (display_meta, idx)  — display_meta may be a MUTATED COPY
            (dict(display_meta)) with an appended synthetic entry. The
            caller should use the returned display_meta (not the input).

        Always returns a valid (display_meta, idx); on any error, falls
        back to legacy _get_initial_image_index behavior.
        """
        from shared.nli_crossref_service import (
            resolve_cambridge_canvas_for_page,
            get_nli_crossref_service,
        )
        # Cache the service on self to avoid repeated singleton lookups.
        _nli_svc = getattr(self, '_nli_crossref_service', None)
        if _nli_svc is None:
            try:
                _nli_svc = get_nli_crossref_service(thread_safe=False)
                self._nli_crossref_service = _nli_svc
            except Exception:
                _nli_svc = None

        images_ext = (display_meta or {}).get('images_ext') or []
        resolved = None
        if _nli_svc is not None and images_ext:
            try:
                resolved = resolve_cambridge_canvas_for_page(
                    sys_id, page_idx, images_ext, svc=_nli_svc,
                )
            except Exception:
                resolved = None

        # Path A: exact canvas match.
        if resolved and resolved.get('canvas_index') is not None:
            return (display_meta, resolved['canvas_index'])

        # Path B: resolver returned None → NLI fallback.
        if resolved is None:
            nli_url = self._build_nli_iiif_url_for_page(sys_id, page_idx)
            if nli_url:
                # Defensive COPY — never mutate meta_mgr.nli_cache[sid].
                display_meta_copy = dict(display_meta)
                display_meta_copy['images_ext'] = list(display_meta_copy.get('images_ext') or []) + [{
                    'label': 'NLI', 'url': nli_url,
                    'folio_num': None, 'folio_side': None,
                    'is_nli_fallback': True,
                }]
                # Keep 'images' alias in sync (same pattern as L6948, L9117).
                display_meta_copy['images'] = display_meta_copy['images_ext']
                return (display_meta_copy, len(display_meta_copy['images_ext']) - 1)
            # Fallback-fallback: legacy positional.
            idx = _get_initial_image_index(
                display_meta,
                folio_num if folio_num is not None else page_idx,
            )
            return (display_meta, idx)

        # Path C: resolver returned {'degraded': True} OR _nli_svc is
        # None → legacy positional behavior.
        idx = _get_initial_image_index(
            display_meta,
            folio_num if folio_num is not None else page_idx,
        )
        return (display_meta, idx)
    ```

    Step 3 — Add `_build_nli_iiif_url_for_page` on the browse class.

    **HARD RULE: suffix MUST come from the active volume IE via
    `resolve_volume_suffix(sys_id, self.current_browse_volume_ie)`.**
    Do NOT hardcode suffix=1 (per H-R2 from Codex review). Only fall
    back to suffix=1 when `current_browse_volume_ie` is None AND log a
    WARNING once per sys_id so the condition is observable.

    Before adding this helper, grep for any existing equivalent. If one
    already exists, REUSE it — do not add a duplicate:

    ```bash
    grep -n "fetch_iiif_manifest.*suffix" genizah_app.py | head -20
    grep -n "iiif.nli.org.il/IIIFv21/FL" genizah_app.py | head -20
    ```

    Assuming none exists, add:

    ```python
    _desktop_nli_fallback_warned = set()  # sys_ids warned about (module or class attr)

    def _build_nli_iiif_url_for_page(self, sys_id, page_idx, width=2000):
        """Resolve the FL id for (sys_id, page_idx) via the NLI IIIF
        manifest's canvas_map and return a direct IIIF URL.

        Returns None if the manifest cannot be fetched or page_idx is
        out of range.

        Suffix policy (per 260419-cfx review H-R2):
          - When self.current_browse_volume_ie is set (multi-volume
            browse), suffix = resolve_volume_suffix(sys_id, vol_ie).
            This uses the active volume's IE, matching what
            switch_to_cambridge() already does at L9098.
          - When self.current_browse_volume_ie is None (single-IE
            shelfmark OR no volume selector active), suffix=1 and a
            WARNING is logged ONCE per sys_id per process lifetime.

        HARD RULE: DO NOT construct FL ids from
        NliCrossrefService.get_images()['fgp_image_number_id']. That
        column is a Friedberg photo number, not an NLI IIIF FL id; they
        are different numbering systems (see PITFALLS.md Pitfall 6 and
        PROJECT.md Phase 30 lesson).
        """
        from genizah_core import resolve_volume_suffix

        vol_ie = getattr(self, 'current_browse_volume_ie', None)
        if vol_ie:
            try:
                volume_suffix = resolve_volume_suffix(sys_id, vol_ie)
                if not volume_suffix or volume_suffix < 1:
                    volume_suffix = 1
            except Exception:
                volume_suffix = 1
        else:
            volume_suffix = 1
            # One-line warning so operators can see when suffix=1 is used
            # without an explicit IE (single-IE shelfmarks are fine; the
            # warning catches bugs where volume_ie should be set but isn't).
            if sys_id not in type(self)._desktop_nli_fallback_warned:
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "_build_nli_iiif_url_for_page: current_browse_volume_ie is None "
                    "for sys_id=%s — using suffix=1 (single-IE default). If this "
                    "shelfmark is multi-IE, the NLI fallback may show the wrong volume.",
                    sys_id,
                )
                type(self)._desktop_nli_fallback_warned.add(sys_id)

        meta_mgr = getattr(self, 'meta_mgr', None)
        if meta_mgr is None:
            return None
        try:
            manifest = meta_mgr.fetch_iiif_manifest(sys_id, suffix=volume_suffix)
        except Exception:
            return None
        canvas_map = (manifest or {}).get('canvas_map') or {}
        if not canvas_map:
            return None

        # canvas_map is {fl_digits: label, ...}. Python dicts preserve
        # insertion order, which IS the NLI manifest's canvas order. For
        # determinism across Python versions / platforms, prefer
        # numerically-sorted FL digits (matches genizah_core.py:3836
        # which sorts canvas_map items by key). Both orderings agree for
        # well-formed NLI manifests, but numeric sort is more robust.
        try:
            fl_keys = sorted(canvas_map.keys(), key=lambda k: int(k))
        except (TypeError, ValueError):
            fl_keys = list(canvas_map.keys())  # fallback: insertion order

        if page_idx < 0 or page_idx >= len(fl_keys):
            return None
        fl_digits = fl_keys[page_idx]
        return f"https://iiif.nli.org.il/IIIFv21/FL{fl_digits}/full/{width},/0/default.jpg"
    ```

    Note: `_desktop_nli_fallback_warned` is a class-level attribute
    (declared once at the top of the class body: `_desktop_nli_fallback_warned = set()`)
    so warnings are deduplicated across all instances of the app window
    (normally only one exists).

    Step 4 — Wire the LOAD sites (L6950-6952 and L9120-9124) to the
    shared helper.

    **L6950-6952 replacement** (browse-tab page-load). Current code:

    ```python
    folio_num = _get_folio_number_from_shelfmark(shelf)
    idx = _get_initial_image_index(display_meta, folio_num if folio_num is not None else self.current_browse_p)
    self.browse_viewer.load_images(display_meta, idx, target_folio=folio_num)
    ```

    Replace with:

    ```python
    folio_num = _get_folio_number_from_shelfmark(shelf)
    is_cambridge = (getattr(self, 'active_source', None) == 'cambridge')
    if is_cambridge and display_meta.get('images_ext'):
        display_meta, idx = self._resolve_cambridge_page_or_fallback(
            self.current_browse_sid, self.current_browse_p, display_meta, folio_num,
        )
    else:
        idx = _get_initial_image_index(
            display_meta, folio_num if folio_num is not None else self.current_browse_p,
        )
    self.browse_viewer.load_images(display_meta, idx, target_folio=folio_num)
    ```

    **L9120-9124 replacement** (switch-source reload). Current code:

    ```python
    if hasattr(self, 'browse_viewer') and not self.browse_reading_desk_active:
        shelfmark, _ = self.meta_mgr.get_meta_for_id(sid)
        folio_num = _get_folio_number_from_shelfmark(shelfmark)
        idx = _get_initial_image_index(display_meta, folio_num if folio_num is not None else self.current_browse_p)
        self.browse_viewer.load_images(display_meta, idx, target_folio=folio_num)
    ```

    Replace with:

    ```python
    if hasattr(self, 'browse_viewer') and not self.browse_reading_desk_active:
        shelfmark, _ = self.meta_mgr.get_meta_for_id(sid)
        folio_num = _get_folio_number_from_shelfmark(shelfmark)
        is_cambridge = (getattr(self, 'active_source', None) == 'cambridge')
        if is_cambridge and display_meta.get('images_ext'):
            display_meta, idx = self._resolve_cambridge_page_or_fallback(
                sid, self.current_browse_p, display_meta, folio_num,
            )
        else:
            idx = _get_initial_image_index(
                display_meta, folio_num if folio_num is not None else self.current_browse_p,
            )
        self.browse_viewer.load_images(display_meta, idx, target_folio=folio_num)
    ```

    Step 5 — Wire the NAVIGATION sites (L21012-21017 and L22508-22510).
    These are the sites that Codex H-R1 identified: they compute image
    indices via `_get_folio_image_index` + `set_page` during prev/next
    navigation AFTER an initial load. When active_source='cambridge' and
    the target folio falls outside CUDL canvas coverage, the resolver
    must be consulted and — if it returns None — the viewer must be
    re-loaded (`load_images`, not `set_page`) with a synthetic NLI
    fallback, because `set_page` cannot inject new entries.

    **L21012-21017 replacement** (folio-nav elif branches). Current code:

    ```python
    if not folio_in_viewer and folio_num is not None and meta:
        oxford_part_meta = meta.get('oxford_part_metadata', {})
        folio_range = oxford_part_meta.get('folio_range', [])
        if meta.get('oxford_part_id') and len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
            # Need dynamic images - call load_images
            idx = _get_folio_image_index(meta, folio_num, side_offset=side_offset)
            self.browse_viewer.load_images(meta, idx, target_folio=folio_num)
        elif viewer_images:
            idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
            self.browse_viewer.set_page(idx)
    elif viewer_images:
        idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
        self.browse_viewer.set_page(idx)
    ```

    Replace the TWO `elif viewer_images:` branches (NOT the Oxford
    `if meta.get('oxford_part_id')` branch) with the wiring below. The
    Oxford branch at L21008-21011 stays untouched.

    ```python
    if not folio_in_viewer and folio_num is not None and meta:
        oxford_part_meta = meta.get('oxford_part_metadata', {})
        folio_range = oxford_part_meta.get('folio_range', [])
        if meta.get('oxford_part_id') and len(folio_range) >= 2 and folio_range[0] <= folio_num <= folio_range[1]:
            # LEAVE ALONE — Oxford folio_range branch. See plan.
            idx = _get_folio_image_index(meta, folio_num, side_offset=side_offset)
            self.browse_viewer.load_images(meta, idx, target_folio=folio_num)
        elif viewer_images:
            # MODIFY (260419-cfx H-R1) — consult resolver when Cambridge.
            is_cambridge = (getattr(self, 'active_source', None) == 'cambridge')
            if is_cambridge:
                nav_meta, idx = self._resolve_cambridge_navigation_index(
                    self.current_browse_sid, viewer_images, folio_num, side_offset,
                )
                if nav_meta is None:
                    idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
                    self.browse_viewer.set_page(idx)
                else:
                    self.browse_viewer.load_images(nav_meta, idx, target_folio=folio_num)
            else:
                idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
                self.browse_viewer.set_page(idx)
    elif viewer_images:
        # MODIFY (260419-cfx H-R1) — consult resolver when Cambridge.
        is_cambridge = (getattr(self, 'active_source', None) == 'cambridge')
        if is_cambridge:
            nav_meta, idx = self._resolve_cambridge_navigation_index(
                self.current_browse_sid, viewer_images, folio_num, side_offset,
            )
            if nav_meta is None:
                idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
                self.browse_viewer.set_page(idx)
            else:
                self.browse_viewer.load_images(nav_meta, idx, target_folio=folio_num)
        else:
            idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num if folio_num is not None else self.current_browse_p, side_offset=side_offset)
            self.browse_viewer.set_page(idx)
    ```

    **L22508-22510 replacement** (composition-summary nav). Current:

    ```python
    if viewer_images and self.browse_viewer.active_list:
        image_idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num, side_offset=side_offset)
        self.browse_viewer.set_page(image_idx)
    ```

    Replace with:

    ```python
    if viewer_images and self.browse_viewer.active_list:
        # MODIFY (260419-cfx H-R1) — consult resolver when Cambridge.
        is_cambridge = (getattr(self, 'active_source', None) == 'cambridge')
        if is_cambridge:
            nav_meta, image_idx = self._resolve_cambridge_navigation_index(
                self.current_browse_sid, viewer_images, folio_num, side_offset,
            )
            if nav_meta is None:
                image_idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num, side_offset=side_offset)
                self.browse_viewer.set_page(image_idx)
            else:
                self.browse_viewer.load_images(nav_meta, image_idx, target_folio=folio_num)
        else:
            image_idx = _get_folio_image_index({'images_ext': viewer_images}, folio_num, side_offset=side_offset)
            self.browse_viewer.set_page(image_idx)
    ```

    Step 6 — Add the navigation helper `_resolve_cambridge_navigation_index`
    on the browse class (next to `_resolve_cambridge_page_or_fallback`
    added in Step 2):

    ```python
    def _resolve_cambridge_navigation_index(self, sys_id, viewer_images, folio_num, side_offset):
        """Side-aware index lookup for prev/next navigation with CUDL
        canvases. Returns (nav_meta, idx):
          - If (folio_num, side) exists in viewer_images (exact match or
            bare-numeric recto): (None, idx) — caller uses set_page(idx)
            on the existing viewer (no reload needed).
          - If no match AND we can build an NLI fallback URL:
            (dict_with_appended_synthetic, len-1) — caller calls
            load_images(nav_meta, idx, ...) with the synthetic meta.
          - If no match AND no NLI URL available: (None, None) — caller
            falls back to legacy _get_folio_image_index + set_page.

        'folio_num' is the target folio number. 'side_offset' follows the
        same semantics as _get_folio_image_index: 0 = recto, 1 = verso.
        If folio_num is None, return (None, None) — caller uses legacy.
        """
        if folio_num is None or not viewer_images:
            return (None, None)
        target_side = 'v' if side_offset == 1 else 'r'

        # First: does the viewer already have this (folio, side)?
        idx = _get_folio_side_image_index(
            {'images_ext': viewer_images}, folio_num, target_side,
        )
        if idx is not None:
            return (None, idx)

        # No match: try NLI fallback. We need a transcription page index
        # to resolve the FL. Use the current browse page (self.current_browse_p)
        # which is set by the prev/next navigation before this helper runs.
        page_idx = getattr(self, 'current_browse_p', None)
        if page_idx is None:
            return (None, None)
        nli_url = self._build_nli_iiif_url_for_page(sys_id, page_idx)
        if not nli_url:
            return (None, None)

        # Build a synthetic nav_meta — defensive COPY, never mutate
        # the viewer's list in place.
        synthetic = {
            'label': 'NLI', 'url': nli_url,
            'folio_num': None, 'folio_side': None,
            'is_nli_fallback': True,
        }
        new_images = list(viewer_images) + [synthetic]
        nav_meta = {'images_ext': new_images, 'images': new_images}
        return (nav_meta, len(new_images) - 1)
    ```

    Note: this helper imports `_get_folio_side_image_index` from
    `desktop.widgets` — ensure that import is present at the top of
    genizah_app.py (grep first; it may already be there).

    Verification that this path is correct for T-S NS 158.112: the NLI
    IIIF manifest for sys_id 990051537270205171 (suffix=1) should return
    a canvas_map with 14 entries whose FL digits are FL167150439..
    FL167150452 (image-layer FLs, per CONTEXT.md H3 retraction note).
    Page 12 (0-indexed) → FL167150451 → folio 8r; page 13 → FL167150452
    → folio 8v. If your local cache of `fetch_iiif_manifest` doesn't
    have this sys_id warm, a first call will populate it.

    Step 7 — Nothing else in genizah_app.py changes. The LEAVE-ALONE
    Oxford branches at L21008-21011 and L22500-22505 stay untouched —
    gated on `meta.get('oxford_part_id')` which is never set on CUL
    CUDL manuscripts.
  </action>
  <verify>
    <automated>pytest tests/ -x -q -k "widgets or folio_side" 2>&1 | tail -20</automated>
    <!-- If tests/test_desktop_widgets.py exists, the new
         _get_folio_side_image_index should have unit coverage. If the
         existing test file doesn't include widget tests, write a small
         new test for _get_folio_side_image_index asserting:
            - exact (folio_num, side) match returns the expected index
            - bare-numeric canvas matches 'r' but not 'v'
            - empty images returns None
            - side='x' (invalid) returns None -->

    <manual>
      (Smoke) Launch desktop, open T-S NS 158.112 in browse, verify:
        - Page 1 → shows folio 1r (CUDL)
        - Page 6 → shows folio 3v (CUDL)
        - Page 12 → shows folio 6v (CUDL)
        - Page 13 → shows folio 8r (NLI fallback — different image from page 12!)
        - Page 14 → shows folio 8v (NLI fallback)
        - Click prev from page 14 → shows correct page 13 image (8r NLI fallback)
        - Click next from page 12 → shows correct page 13 image (8r NLI fallback)
      If console shows:
        "_build_nli_iiif_url_for_page: current_browse_volume_ie is None ..."
        once (for this single-IE shelfmark), that is expected and correct.
    </manual>

    <automated>python scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver 2>&1 | grep -E "RESOLVER CUL-canvas-fix (VERIFIED|BROKEN)"</automated>
    <!-- Extension of the existing diagnostic is delivered in Task 4;
         run after Task 4 lands. -->
  </verify>
  <done>
    (1) `desktop/widgets.py` has `_get_folio_side_image_index` and it is
    imported in genizah_app.py.
    (2) genizah_app.py has both `_resolve_cambridge_page_or_fallback`
    (used at load sites) and `_resolve_cambridge_navigation_index` (used
    at nav sites).
    (3) `_build_nli_iiif_url_for_page` uses `resolve_volume_suffix(
    sys_id, self.current_browse_volume_ie)` for suffix — NOT hardcoded
    suffix=1. One-line WARNING logged once per sys_id when vol_ie is None.
    (4) L6950-6952 and L9120-9124 (LOAD sites) are wired via
    `_resolve_cambridge_page_or_fallback`.
    (5) L21012-21014, L21015-21017, and L22508-22510 (NAV sites) are
    wired via `_resolve_cambridge_navigation_index` when active_source='cambridge'.
    (6) L21010-21011 (Oxford load_images) and L22504-22505 (Oxford
    load_images) are UNCHANGED.
    (7) Every mutation of `display_meta['images_ext']` in the new code
    paths operates on a `dict(display_meta)` COPY (never mutates
    meta_mgr.nli_cache[sid]).
    (8) Smoke test verifies T-S NS 158.112 pages 1..12 serve CUDL
    JPEGs, pages 13..14 serve NLI JPEGs on initial load AND on prev/next
    nav.
  </done>
</task>

<task type="auto">
  <name>Task 4: Extend diagnostic, flip OPEN_ISSUES entry, retract H3 verdict, write SUMMARY.md, commit</name>
  <files>
    scripts/debug_ts_ns_158_112_image_alignment.py,
    docs/OPEN_ISSUES.md,
    .planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-SUMMARY.md
  </files>
  <action>
    Step 1 — Extend `scripts/debug_ts_ns_158_112_image_alignment.py`
    with a `--verify-resolver` flag. When passed, after the existing
    alignment table, print a "RESOLVER TABLE" section that calls
    `resolve_cambridge_canvas_for_page(sys_id, page, images_ext)` for
    pages 0..N-1 and prints one of:
       "p=N (folio=Xr|v) → canvas_index=K"
       "p=N (folio=Xr|v) → NLI_FALLBACK"
       "p=N (folio=?)    → DEGRADED (sidecar unavailable)"
    Also print a single-line verdict:
       "RESOLVER CUL-canvas-fix VERIFIED" if (a) every page in
       [0, min(len(cudl_canvases), len(nli_images))) resolves to a
       canvas with exact (folio_num, side) match AND (b) every page
       outside that range resolves to NLI_FALLBACK.
    Otherwise:
       "RESOLVER CUL-canvas-fix BROKEN — <reason>".

    Keep the existing 3-verdict block untouched. Both blocks run in a
    single script invocation when `--verify-resolver` is passed. Add the
    flag to argparse, default False.

    Step 2 — Update `docs/OPEN_ISSUES.md`:

    a. Change "Last Updated" at line 3 to:
       `> **Last Updated:** 2026-04-19 (260419-cfx: CUL CUDL positional canvas mismatch fixed via folio+side resolver with NLI fallback; H3 IE-suffix claim retracted)`

    b. In the P2 row that starts
       `| **CUL positional image mismatch:` (matches on line 83), change:
       `❌ Open` → `✅ Fixed (2026-04-19)`
       AND append to the Notes cell (inside the same cell, no newlines
       — keep the table well-formed):
       ` **Fixed by 260419-cfx:** /api/cambridge_image now resolves transcription page N → CUDL canvas by matching (folio_num, side) against the N-th nli_images row (via resolve_cambridge_canvas_for_page in shared/nli_crossref_service.py); when no CUDL canvas matches, the endpoint falls back to the NLI image with X-Image-Fallback-Source: nli, X-Folio-Matched: {folio}{side}, ETag "{sys_id}-p{page}-v2", and X-Image-Resolver-Version: 2. Desktop mirrors the same logic on both initial load AND prev/next navigation; desktop NLI fallback uses resolve_volume_suffix(sid, current_browse_volume_ie) for multi-IE correctness. **H3 retraction:** the prior SUMMARY for 260419-nwv claimed NLI IIIF manifest was resolving the wrong IE for T-S NS 158.112; deeper probing shows the manifest is correct — Transcriptions.txt references text-layer FLs (FL167150424–437) that 500 on image GET, while the IIIF manifest returns image-layer FLs (FL167150439–452) in the same IE167150422. Both layers are valid; fetch_fl_ids_from_nli is serving the right one. No code change to IE-suffix logic.`

    c. Update the Quick Summary counts at the top of the file:
       If previous line was "P2 Open 18, Fixed 60", change to
       "P2 Open 17, Fixed 61". (Verify exact prior counts by reading the
       file; adjust both by ±1.) Do NOT invent the counts — read
       OPEN_ISSUES.md line 5-10 block before editing.

    Step 3 — Write `.planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-SUMMARY.md`.

    Use the same frontmatter shape as 260419-nwv-SUMMARY.md. Sections:
    - One-liner
    - Bug (retracting H3)
    - Fix (what changed in web/desktop/shared)
    - Resolver table (the --verify-resolver output from Task 4 Step 1
      for T-S NS 158.112)
    - Test coverage added (TestFolioSideResolver; 20 new pytest cases:
      12 parametrized resolver + 4 resolver edge + 4 label parser)
    - **Known limitations** — MUST include the following bullets:
      - "The WEB `cambridge_image` NLI fallback hardcodes suffix=1 when
        resolving FL ids. This is because the existing
        `/api/cambridge_image/{sys_id}?page={N}` URL contract has no
        `suffix` query param — adding one is out of scope per CONTEXT.md.
        Multi-IE CUL shelfmarks (rare; Transcriptions.txt currently has
        ~5 known multi-IE CUL records) may therefore receive the wrong
        volume's NLI image on the web fallback path. Desktop does NOT
        have this limitation — it uses `resolve_volume_suffix(sid,
        current_browse_volume_ie)`."
      - "FL ids are sourced from the NLI IIIF manifest canvas_map on
        both web (fetch_fl_ids_from_nli) and desktop
        (meta_mgr.fetch_iiif_manifest[canvas_map]) — NEVER from
        nli_crossref FGPImageNumberId (see PITFALLS.md Pitfall 6)."
    - **Operational: browser/CDN cache invalidation** — MUST include:
      "Server in-memory cache is invalidated by bumping
      `_CAMBRIDGE_CACHE_VERSION` (currently 2). Browsers/CDNs may still
      serve stale bytes for up to max-age=600 (10 min) post-deploy.
      **Mitigation:** every response carries
      `ETag: \"{sys_id}-p{page}-v2\"` and `X-Image-Resolver-Version: 2`.
      Clients that honor ETag revalidation get fresh bytes. If stale
      images are observed after deploy, either (a) force-refresh browser
      cache, or (b) bump BOTH `_CAMBRIDGE_CACHE_VERSION` AND
      `_CAMBRIDGE_ETAG_VERSION` together in web/api.py (they must stay
      in lockstep) and redeploy. The ETag version change invalidates all
      intermediate caches immediately."
    - Commits (populate from actual git log at commit time)
    - Deviations from plan (if any — MUST note if any LEAVE-ALONE site
      from the inventory was unexpectedly reached)
    - Self-Check (see done criteria)

    Step 4 — Commit. Create three commits:

    1. `feat(260419-cfx): add folio+side canvas resolver for CUL CUDL`
       — Task 1 changes (genizah_core.py, shared/nli_crossref_service.py,
       tests/test_nli_crossref_service.py).

    2. `fix(260419-cfx): serve NLI fallback when CUDL canvas missing for transcription page`
       — Task 2a + Task 2b changes (web/api.py, desktop/widgets.py,
       genizah_app.py). If the executor committed 2a and 2b separately
       for atomicity, keep two commits here.

    3. `docs(260419-cfx): flip OPEN_ISSUES entry and retract H3 verdict; add SUMMARY`
       — Task 4 changes (scripts/debug_*, docs/OPEN_ISSUES.md, SUMMARY.md).

    Each commit message body should include the Co-Authored-By trailer
    per the project commit convention.
  </action>
  <verify>
    <automated>python scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver 2>&1 | grep -E "RESOLVER CUL-canvas-fix (VERIFIED|BROKEN)"</automated>
    <!-- Must emit "RESOLVER CUL-canvas-fix VERIFIED". If the dev
         environment lacks network access to iiif.nli.org.il /
         cudl.lib.cam.ac.uk, the existing script already degrades
         gracefully — and the resolver verification does NOT require
         network (it reads nli_crossref.db and images_ext from meta_mgr
         + local fixture). If meta_mgr isn't available in this script's
         minimal context, the resolver check should still be runnable
         by constructing the T-S NS 158.112 images_ext inline. Keep
         the check purely local. -->

    <automated>python scripts/check_docs.py</automated>
    <!-- Must report "All checks passed! Documentation is healthy."
         No new docs-hygiene regressions. -->
  </verify>
  <done>
    (1) `scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver`
    prints the RESOLVER TABLE with N=1..12 → canvas_index=N-1 and N=13,14
    → NLI_FALLBACK, and emits "RESOLVER CUL-canvas-fix VERIFIED". (2)
    `docs/OPEN_ISSUES.md` P2 row "CUL positional image mismatch (260419-nwv
    follow-up)" is flipped to `✅ Fixed (2026-04-19)` with an inline
    retraction of the H3 IE-suffix claim. (3) Quick Summary counts are
    adjusted (±1). (4) SUMMARY.md exists at the expected path with all
    required sections including the "Known limitations" bullets (web
    suffix=1 + FL id source) AND the "Operational: browser/CDN cache
    invalidation" section. (5) `python scripts/check_docs.py` passes.
    (6) Commits are in git log with message prefix `260419-cfx`
    (Task 2a and 2b may share one or two commits per executor preference).
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Web client → `/api/cambridge_image/{sys_id}` | `sys_id` + `page` are untrusted; server reads local SQLite + fetches external IIIF. |
| Server → cudl.lib.cam.ac.uk + iiif.nli.org.il | Outbound; response bytes treated as opaque image data, length-checked, Content-Type validated. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-260419-cfx-01 | Tampering (stale cache) | `web/api.py::_cambridge_image_cache` + browser/CDN cache | mitigate | Include `_CAMBRIDGE_CACHE_VERSION = 2` in the cache key tuple AND `ETag: "{sys_id}-p{page}-v2"` + `X-Image-Resolver-Version: 2` in every response. Bumping the version constants (in lockstep) invalidates server-side cache AND signals clients to revalidate. Max-age=600 is unchanged; operational knob documented in SUMMARY.md. |
| T-260419-cfx-02 | Information Disclosure (wrong content-type) | `cambridge_image` NLI fallback | mitigate | NLI fallback uses `_fetch_nli_image_bytes`, which only returns on (status 200 AND Content-Type contains 'image' AND body > min_size). On helper failure the endpoint returns 404 plain-text, never a falsy image. |
| T-260419-cfx-03 | Denial of Service (missing sidecar → 500) | `cambridge_image` when `nli_crossref.db` absent | mitigate | Degrade to legacy positional lookup when `svc.is_available()` is False OR `get_folio_images(sys_id)` returns []. Log WARNING once per sys_id per process. Never raise. |
| T-260419-cfx-04 | Tampering (label-side ambiguity) | `_parse_cudl_label` | mitigate | Canonical regex `^\s*(?:f\.?\s*)?(\d+)\s*([rv])?\b` with case-insensitive side group. Bare numeric labels treated as recto by documented convention. Non-matching labels return (None, None) which excludes them from folio+side matching. |
| T-260419-cfx-05 | Spoofing (sys_id collision) | resolver input | accept | sys_id is already trusted by all other NLI endpoints; no new attack surface. Low value, low risk. |

</threat_model>

<verification>

Phase-level checks:

1. **Observable behavior on T-S NS 158.112** (the representative case):
   - `GET /api/cambridge_image/990051537270205171?page=N` for N=0..11:
     returns CUDL JPEGs identical in shape to pre-fix responses (same
     Content-Type, similar Content-Length). Headers include
     `X-Image-Resolver-Version: 2`, `ETag: "{sys_id}-p{page}-v2"`,
     `X-Folio-Matched: {folio}{side}` (e.g. `1r` for page=0), and NO
     `X-Image-Fallback-Source` header.
   - `GET /api/cambridge_image/990051537270205171?page=12` and
     `?page=13`: returns JPEGs with Content-Type `image/jpeg`, header
     `X-Image-Fallback-Source: nli`, `X-Folio-Matched: 8r` (page=12) or
     `8v` (page=13), `X-Image-Resolver-Version: 2`, and ETag as above.
     Neither is a 404 body.
   - Desktop: open sys_id 990051537270205171 in browse tab, active
     source Cambridge (auto-defaulted), step through pages 1..14 AND
     navigate back and forth with prev/next. Each page shows the correct
     image (not a repeated canvas, not blank). Initial load AND
     navigation both honor the resolver.

2. **Regression safety on other CUL shelfmarks**:
   - Open a known-working CUL manuscript where CUDL canvas count equals
     transcription page count (any sys_id from recent browse traffic
     except T-S NS 158.112). Confirm: images still load for every page,
     zero `X-Image-Fallback-Source: nli` headers observed.
   - Open a non-CUL manuscript (JTS / Manchester / Oxford). Confirm:
     no crash, no unexpected `cambridge_image` calls in server logs.

3. **Graceful degradation**:
   - With `nli_crossref.db` temporarily renamed to `nli_crossref.db.bak`
     (or via env pointing to a missing path), `GET /api/cambridge_image/{any_CUL_sys_id}?page=0`
     returns a valid CUDL image (legacy positional behavior). Server
     log contains one WARNING line per sys_id. Response still carries
     `X-Image-Resolver-Version: 2`.

4. **FL-id source invariant (regression guard)**:
   - `grep -n "FGPImageNumberId" genizah_app.py web/api.py` must show
     ZERO lines where `FGPImageNumberId` is interpolated into an
     `iiif.nli.org.il/IIIFv21/FL...` URL. If any are found, the plan is
     violated (see PITFALLS.md Pitfall 6).
   - `_build_nli_iiif_url_for_page` in genizah_app.py calls
     `meta_mgr.fetch_iiif_manifest(...)['canvas_map']` with suffix
     derived from `resolve_volume_suffix(sid, current_browse_volume_ie)`.
     Both can be grep-verified:
       `grep -n "canvas_map" genizah_app.py`
       `grep -n "resolve_volume_suffix" genizah_app.py`
     The latter must show at least one NEW call site in
     `_build_nli_iiif_url_for_page` (in addition to the pre-existing
     calls at L6804, L6933, L9098).

5. **LEAVE-ALONE invariant**:
   - `git diff HEAD~3 -- genizah_app.py | grep -E "^[-+].*_get_folio_image_index"`
     must ONLY show CHANGES at line ranges corresponding to
     L21012-21017 and L22508-22510 (the navigation MODIFY sites). The
     Oxford branches at L21008-21011 and L22500-22505 must be
     UNCHANGED (their `_get_folio_image_index(meta, folio_num,
     side_offset=side_offset)` + `load_images(meta, idx, ...)` calls
     still present and identical).

6. **Test suite**:
   - `pytest tests/test_nli_crossref_service.py` — all tests pass
     (77 pre-existing + 20 new = 97 total). 20 new cases enumerate as:
     12 parametrized resolver tests + 4 resolver edge tests + 4
     `_parse_cudl_label` tests.
   - `python scripts/debug_ts_ns_158_112_image_alignment.py --verify-resolver`
     exits 0 and prints `RESOLVER CUL-canvas-fix VERIFIED`.
   - `python scripts/check_docs.py` reports all checks passed.

7. **Docs state**:
   - `docs/OPEN_ISSUES.md` P2 entry "CUL positional image mismatch
     (260419-nwv follow-up)" is `✅ Fixed (2026-04-19)`.
   - H3 retraction note is present inline in that cell.
   - `.planning/quick/260419-cfx-.../260419-cfx-SUMMARY.md` exists with
     the "Known limitations" section (web suffix=1 + FL id source
     invariant) AND the "Operational: browser/CDN cache invalidation"
     section documenting the ETag + X-Image-Resolver-Version mechanism.

</verification>

<success_criteria>

Quick fix is complete when:

- [ ] T-S NS 158.112 pages 13 and 14 show a correct image (NLI
      fallback) instead of wrong/missing canvas content in both web and
      desktop, both on INITIAL load and during prev/next navigation.
- [ ] T-S NS 158.112 pages 1..12 continue to show the correct CUDL
      canvases (1r..6v) with no observable change in latency or image
      quality.
- [ ] `/api/cambridge_image/{sys_id}?page={N}` URL contract is
      unchanged. Callers in browse.py are not touched.
- [ ] Every cambridge_image response carries
      `X-Image-Resolver-Version: 2` and
      `ETag: "{sys_id}-p{page}-v2"`. Fallback responses additionally
      carry `X-Image-Fallback-Source: nli` and (when known)
      `X-Folio-Matched: {folio}{side}`.
- [ ] `pytest tests/test_nli_crossref_service.py` passes with 20 new
      pytest cases added (12 parametrized resolver + 4 resolver edge + 4
      label parser).
- [ ] `_CAMBRIDGE_CACHE_VERSION = 2` and `_CAMBRIDGE_ETAG_VERSION = "v2"`
      are both present in web/api.py and used consistently (cache key +
      response header).
- [ ] `nli_crossref.db` absent → endpoint degrades to legacy positional
      behavior with one WARNING log per sys_id; no 500s. Response still
      carries resolver-version headers.
- [ ] FL ids for NLI fallback are sourced from the NLI IIIF manifest
      canvas_map on both web (fetch_fl_ids_from_nli, unchanged) and
      desktop (meta_mgr.fetch_iiif_manifest[canvas_map], new helper).
      ZERO uses of FGPImageNumberId to construct IIIF FL URLs in either
      codebase.
- [ ] Desktop `_build_nli_iiif_url_for_page` derives suffix via
      `resolve_volume_suffix(sys_id, current_browse_volume_ie)`. When
      vol_ie is None, suffix=1 AND a WARNING is logged once per sys_id.
      Hardcoded `suffix=1` without the WARNING path does NOT appear
      (grep -n "suffix=1" desktop paths to confirm — only the WARNING
      fallback branch uses 1).
- [ ] Desktop LEAVE-ALONE sites (L21010-21011 Oxford load + L22504-22505
      Oxford load) are unchanged. MODIFY sites (L6950-6952, L9120-9124,
      L21012-21017, L22508-22510) are wired to the resolver /
      `_resolve_cambridge_navigation_index` per Task 2b.
- [ ] `fetch_fl_ids_from_nli` in web/api.py is not modified.
- [ ] Every code path in Task 2b that writes to `display_meta['images_ext']`
      operates on `dict(display_meta)` (defensive copy — never mutates
      `meta_mgr.nli_cache[sid]`).
- [ ] `docs/OPEN_ISSUES.md` flipped; H3 retraction documented inline.
- [ ] `scripts/check_docs.py` passes.
- [ ] Commits in the log with prefix `260419-cfx` for Task 1, Task 2a/2b
      (one or two commits per executor preference), and Task 4.
- [ ] `SUMMARY.md` is written at the expected path with a "Known
      limitations" section documenting the WEB suffix=1 multi-IE
      limitation AND an "Operational: browser/CDN cache invalidation"
      section.

</success_criteria>

<output>
After completion, create
`.planning/quick/260419-cfx-cul-cudl-folio-side-mapping/260419-cfx-SUMMARY.md`
with the retraction of the H3 verdict from 260419-nwv, the observable
post-fix resolver table for T-S NS 158.112, and the operational note
on browser/CDN cache invalidation via ETag.
</output>

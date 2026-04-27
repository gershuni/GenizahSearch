# Review Request: Auto-detect blue mat background removal in Fragment Puzzle

## Your Role

You are reviewing an implementation plan for a Cairo Genizah manuscript research application. The app has a "Fragment Puzzle" feature where users arrange manuscript fragment images on a canvas to reconstruct joins between fragments held in different libraries. Background removal strips the scanning mat so fragments can be overlaid cleanly.

Please review the plan for correctness, risks, and missed edge cases. Flag anything that seems wrong or incomplete.

---

## Context

### What the app does

GenizahSearch is a research tool for Cairo Genizah manuscripts (~255K records across 20+ libraries). The Fragment Puzzle lets users:
1. Add manuscript fragment images to a canvas
2. Remove scanning backgrounds (mats) to isolate the parchment
3. Arrange, rotate, flip, and crop fragments to reconstruct joins
4. Save/export composite images

Images come from NLI (National Library of Israel) IIIF servers, or library-specific proxies (Cambridge CUDL, Manchester LUNA, Oxford, JTS).

### Current background removal architecture

**`shared/background_removal.py`** — the core engine:
- `detect_background_color()` — samples four corners of the image in HSV space to find the dominant background color
- `create_mask()` — creates a foreground/background mask using HSV distance. Three modes based on background saturation:
  - Low saturation (S<30): gray/cream/white backgrounds → Value-channel distance
  - High saturation (S>100): colored backgrounds → hue-dominant weighted distance
  - Medium saturation: standard Euclidean HSV distance
- `create_cul_blue_mask()` — a separate, deterministic mask that targets blue pixels by hue range (H 135-185, S>=60 on Pillow's 0-255 scale). Does NOT sample corners — checks every pixel.
- `remove_background(image_bytes, threshold, is_cul)` — entry point. If `is_cul=True`, uses `create_cul_blue_mask()`. Otherwise uses `create_mask()` with corner-sampled background color.

**`shared/puzzle_image_service.py`** — fetches images, applies background removal, caches to disk:
- Cache key format: `{fl_id}_{size}_{threshold}[_cul]_{PROCESSING_VERSION}.png`
- `PROCESSING_VERSION = 'v3'` — bumped when algorithm changes to auto-invalidate cache
- `is_cul` parameter flows through to `remove_background()` and affects the cache key

**`web/pages/puzzle.py`** — web UI (4,000+ lines, NiceGUI + Fabric.js canvas):
- Three call sites determine `is_cul` by checking library code and shelfmark prefixes
- `is_cul` is stored in fragment metadata dict and passed through API URLs
- JavaScript `_loadImageWithFallbacks()` passes `is_cul` through the fallback chain (server cache → browser extension → localhost helper → direct NLI)

**`web/api.py`** — HTTP endpoints:
- `GET /api/puzzle_image` — serves NLI images (accepts `is_cul` query param)
- `POST /api/puzzle_process` — browser extension uploads raw image bytes for server-side BG removal (accepts `is_cul`)
- `GET /api/puzzle_ext_image` — fetches from library proxies, applies BG removal server-side. Currently does NOT pass `is_cul` to `remove_background()`

**`genizah_app.py`** — desktop app (PyQt6):
- `_is_cul_fragment(pf)` static method checks shelfmark/provider
- Used at 5+ call sites for image loading, threshold selection, export

### The problem

The `is_cul` flag is determined by **library code heuristics**:
- `library_code == 'CUL'` (Cambridge University Library)
- Shelfmark prefix: `T-S`, `OR.`, `ADD.` (CUL shelfmark patterns)
- `external_provider == 'cambridge'` (Cambridge CUDL)

This **misses British Library (BL) images** that use the exact same blue conservation mat. A user noticed BL shelfmark OR 5557O.30 has a blue mat identical to CUL's. Testing confirmed BL images have 21-28% blue pixels — solidly in the same range as CUL (5-59%).

Any other library that adopts blue mats in the future would also be missed.

### Test results

We analyzed 30 images for blue pixel percentage (pixels matching H[135-185] & S>=60):

| Category | Blue % Range | Count | Notes |
|----------|-------------|-------|-------|
| CUL (blue mat) | 5.2% – 59.4% | 25 | Clear blue mat |
| CUL (faded) | 0.83% | 1 | Very desaturated blue (S~16), below S>=60 threshold |
| BL (blue mat) | 21.4% – 27.9% | 4 | sys_ids: 990053506030205171, 990053506130205171, 990053505780205171, 990053505760205171 |

The gap between blue (>=5.2%) and non-blue (<=0.83%) is enormous. Non-blue libraries (JTS, RNL, AIU, Manchester) could not be live-tested (NLI blocks server IP), but their images have cream/gray/dark backgrounds with essentially 0% blue.

Blue is not a natural color for genizah manuscripts (parchment/paper, brown/black ink), so any significant blue presence is a reliable mat signal.

---

## The Plan

### Core change: Auto-detection in `remove_background()`

Add `detect_blue_mat(hsv_array) -> bool` to `background_removal.py`:
- Count pixels matching H[135-185] & S>=60 (same range as existing `create_cul_blue_mask`)
- Return True if >= 2.0% of pixels are blue
- Constant: `BLUE_MAT_DETECTION_THRESHOLD = 0.02`

Modify `remove_background()`:
- If `is_cul=False` but `detect_blue_mat()` returns True → auto-apply blue mask
- If `is_cul=True` → apply blue mask as before (no change)
- Log when auto-detection triggers

**The `is_cul` parameter stays** (not removed) because:
1. It's part of the cache key (`_cul` suffix in filenames)
2. Callers use it to select threshold=150 (hue-dominant distance works better with higher threshold for blue backgrounds)
3. Removing it would be a breaking API change across web endpoints, JS, desktop, HMAC tokens

### Caller-side changes

- Add `lib_code == 'BL'` to the `is_cul` checks in puzzle.py (3 call sites) and genizah_app.py
- Rename `_is_cul_fragment()` → `_has_blue_mat()` in desktop app
- Rename CUL-specific constants/functions to generic names (`CUL_BLUE_*` → `BLUE_MAT_*`, `create_cul_blue_mask` → `create_blue_mat_mask`)

### Cache invalidation

Bump `PROCESSING_VERSION = 'v3'` → `'v4'` in `puzzle_image_service.py` to force re-processing of all cached images.

### Files changed

| File | Change |
|------|--------|
| `shared/background_removal.py` | Add `detect_blue_mat()`, rename CUL→blue_mat constants/functions, auto-detect in `remove_background()` |
| `web/pages/puzzle.py` | Add BL to `is_cul` checks at 3 call sites |
| `genizah_app.py` | Rename `_is_cul_fragment` → `_has_blue_mat`, add BL |
| `shared/puzzle_image_service.py` | Bump PROCESSING_VERSION to v4 |
| `web/api.py` | Add comment at `puzzle_ext_image` (auto-detection covers it) |

### What's NOT changed (and why)

- `web/puzzle_tokens.py` — `is_cul` stays in HMAC token generation
- `shared/puzzle_model.py` — no `is_cul` field (derived at load time)
- JS code in puzzle.py — `is_cul` stays in meta dict as cache key hint
- API signatures — `is_cul` parameter kept for backward compatibility

---

## Questions for Reviewer

1. **Threshold of 2%** — is this too low (risk of false positives from blue ink/illumination) or too high (risk of missing faded blue mats)? The data shows a gap from 0.83% to 5.2%.

2. **Auto-detect vs. explicit** — the plan keeps `is_cul` as a hint AND adds auto-detection as a fallback. Is this dual approach sound, or should we go fully one way?

3. **Cache key concern** — when auto-detection triggers for an image that wasn't flagged `is_cul`, the cache key won't have the `_cul` suffix, so it gets cached under a different key than it would if the caller had known. Is this a problem? (The image content will be correct either way — it's just a cache key mismatch if the caller later learns to pass `is_cul=True`.)

4. **`puzzle_ext_image` endpoint** — currently calls `remove_background(raw_bytes, threshold=threshold)` without `is_cul`. The plan says auto-detection covers it. But threshold is still 30 (not 150). Does the lower threshold matter for auto-detected blue mats, or is the hue-range mask independent of threshold?

5. **Performance** — `detect_blue_mat()` scans all pixels. This adds ~2-5ms to every `remove_background()` call. Acceptable?

6. **Any missed call sites or edge cases?**

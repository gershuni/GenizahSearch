# Quick Task 260322-efy: Auto-detect blue mat background instead of hardcoding CUL library

**Created:** 2026-03-22
**Status:** Ready for implementation (post-review revision)

## Problem

Background removal in the puzzle currently uses a hardcoded `is_cul` flag to decide whether to apply the blue conservation mat removal algorithm. This flag is set by checking:
- `library_code == 'CUL'`
- Shelfmark prefix: `T-S`, `OR.`, `ADD.`
- `external_provider == 'cambridge'`

This misses **British Library images** (and potentially others) that also use identical blue mats. User observed BL shelfmark OR 5557O.30 with the same blue mat as CUL.

## Evidence

Tested blue pixel percentage (H[135-185] S>=60 on Pillow 0-255 HSV scale) across 30 images:

| Library | Blue % Range | Count |
|---------|-------------|-------|
| CUL (blue mat) | 5.2% - 59.4% | 25 images |
| CUL (faded/desaturated) | 0.83% | 1 image |
| **BL (blue mat)** | **21.4% - 27.9%** | **4 images** |

The gap between blue (>=5.2%) and non-blue (<=0.83%) is enormous. A **2% threshold** cleanly separates them. Blue is not a natural color for genizah fragments (no blue inks, no blue illumination), so any significant blue presence is a reliable mat signal.

## Review Findings (Codex)

Key feedback incorporated:

1. **[P2] Don't blanket-map BL into `is_cul=True`.** `is_cul=True` means "skip normal bg removal, ONLY target blue" (confirmed by test at `test_background_removal.py:335`). Setting it for all BL images would break non-blue BL scans. **Fix: rely on auto-detection, don't add BL to heuristics.**

2. **[P2] Whole-image detection is fine for this domain.** Blue doesn't occur naturally in genizah fragments. Even blue visible only through a hole in the manuscript (not at edges) should be removed. Perimeter-only detection would miss that case.

3. **[P2] When auto-detected, apply blue mask AND normal mask as union.** Don't switch exclusively to blue-only mode. This preserves normal bg removal for gray/cream borders while also catching the blue mat.

4. **[P3] Faded mats (0.83%) are an intentional limit.** They have very low saturation (S~16) so the general `create_mask()` handles them via its medium-saturation path.

5. **Threshold inconsistency: desktop uses 115, web uses 150.** Standardize to **150** everywhere.

## Approach (Revised)

**Auto-detect blue mats inside `remove_background()` from the image pixels themselves.** The `is_cul` parameter stays as a caller hint for cache keys but auto-detection is the primary mechanism.

**Critical behavioral change:** When auto-detected (not explicit `is_cul`), apply blue mask **combined with** normal mask (union), not blue-only. This is safer than the `is_cul=True` path which skips normal removal entirely.

---

## Task 1: Add `detect_blue_mat()` and union mode to `background_removal.py`

**File:** `shared/background_removal.py`

### 1a. Rename CUL-specific names to generic blue-mat names

- `CUL_BLUE_HUE_MIN` -> `BLUE_MAT_HUE_MIN`
- `CUL_BLUE_HUE_MAX` -> `BLUE_MAT_HUE_MAX`
- `CUL_BLUE_SAT_MIN` -> `BLUE_MAT_SAT_MIN`
- `create_cul_blue_mask()` -> `create_blue_mat_mask()`

Update all internal references (only used within this file).

### 1b. Add `detect_blue_mat(hsv_array) -> float`

```python
BLUE_MAT_DETECT_THRESHOLD = 0.02  # 2% of pixels

def detect_blue_mat(hsv_array: np.ndarray) -> float:
    """Return fraction of pixels matching blue mat HSV range."""
    h = hsv_array[:, :, 0].astype(float)
    s = hsv_array[:, :, 1].astype(float)
    is_blue = (h >= BLUE_MAT_HUE_MIN) & (h <= BLUE_MAT_HUE_MAX) & (s >= BLUE_MAT_SAT_MIN)
    return np.count_nonzero(is_blue) / is_blue.size
```

Returns float (not bool) so callers can log the percentage.

### 1c. Modify `remove_background()` — three paths

```
if is_cul:
    # Explicit hint: blue-only mask (existing behavior, unchanged)
    mask = create_blue_mat_mask(hsv_array)
elif detect_blue_mat(hsv_array) >= BLUE_MAT_DETECT_THRESHOLD:
    # Auto-detected: UNION of blue mask + normal mask
    blue_mask = create_blue_mat_mask(hsv_array)
    normal_mask = create_mask(hsv_array, bg_color, threshold)
    mask = ImageChops.multiply(blue_mask, normal_mask)  # intersection=0 means either removes it
    # Actually: union of removal = pixel is background if EITHER mask says 0
    # So: mask = min(blue_mask, normal_mask) per pixel
    mask = ImageChops.darker(blue_mask, normal_mask)
    log: "Auto-detected blue mat ({pct:.1%} blue pixels)"
else:
    # No blue: normal mask only (existing behavior, unchanged)
    mask = create_mask(hsv_array, bg_color, threshold)
```

**Performance note (from review):** In the `is_cul` path, skip `detect_background_color()` — it's not needed. Move `bg_color = detect_background_color(hsv_array)` inside the branches that need it (auto-detect and normal paths).

### 1d. Update existing tests

`test_is_cul_only_targets_blue` (line 335): behavior unchanged for explicit `is_cul=True`.

Add new tests:
- Auto-detect triggers on image with blue background + `is_cul=False`
- Auto-detect does NOT trigger on gray background image
- Auto-detect union mode: gray border frame around blue mat both get removed
- Blue content <2% does NOT trigger auto-detect

**Verify:** `pytest tests/test_background_removal.py`

---

## Task 2: Simplify caller-side logic in `puzzle.py` (web) — NO BL addition

**File:** `web/pages/puzzle.py`

**Key change from original plan: Do NOT add BL to the is_cul heuristics.** Auto-detection handles BL (and any future library with blue mats).

### 2a. `_add_fragment_by_sys_id()` (line ~2100)
- Keep existing CUL/Cambridge/T-S heuristics for `is_cul` (cache key hint)
- **Change threshold from 150 to 150** (already 150 on web, no change needed)
- No BL addition — auto-detection covers it

### 2b. `_load_document()` fragment loop (line ~2474)
- No change — existing heuristics stay, auto-detection covers BL

### 2c. Add-from-browse handler (line ~3803)
- No change — same reasoning

### 2d. External provider path (lines ~2130, ~3831)
- No change — `puzzle_ext_image` calls `remove_background()` which now auto-detects

**Net effect on web callers:** Zero changes to `is_cul` logic. All the work happens inside `remove_background()`.

---

## Task 3: Fix desktop threshold inconsistency + rename

**File:** `genizah_app.py`

### 3a. Fix threshold: 115 -> 150

At `_load_puzzle_fragment()` (line ~4090):
```python
# Before:
threshold = 115.0
# After:
threshold = 150.0
```

And the shelfmark fallback (line ~4096):
```python
# Before:
threshold = 115.0
# After:
threshold = 150.0
```

### 3b. Rename `_is_cul_fragment()` -> `_has_blue_mat()` (line ~4528)
- Keep existing CUL/Cambridge checks (these are still valid as cache key hints)
- Do NOT add BL — auto-detection handles it
- Update all 5+ callers to use new name

**Verify:** `pytest tests/`

---

## Task 4: Bump `PROCESSING_VERSION`

**File:** `shared/puzzle_image_service.py`

**Change:** `PROCESSING_VERSION = 'v3'` -> `PROCESSING_VERSION = 'v4'`

Forces re-processing of all cached images with new auto-detection logic.

---

## Task 5: Update `test_bg_removal_samples.py` and cleanup test script

**File:** `scripts/test_bg_removal_samples.py`
- Update import: `create_cul_blue_mask` -> `create_blue_mat_mask`
- Update constant imports: `CUL_BLUE_*` -> `BLUE_MAT_*`

**File:** `scripts/test_blue_mat_detection.py`
- Can be deleted or kept as a manual verification tool

---

## Files Changed Summary

| File | Change |
|------|--------|
| `shared/background_removal.py` | Add `detect_blue_mat()`, union mode in `remove_background()`, rename CUL->blue_mat |
| `shared/puzzle_image_service.py` | Bump PROCESSING_VERSION to v4 |
| `genizah_app.py` | Fix threshold 115->150, rename `_is_cul_fragment`->`_has_blue_mat` |
| `tests/test_background_removal.py` | Add auto-detection tests |
| `scripts/test_bg_removal_samples.py` | Update renamed imports |

## Files NOT Changed (and why)

| File | Why |
|------|-----|
| `web/pages/puzzle.py` | No changes needed — auto-detection in remove_background() handles everything |
| `web/api.py` | Auto-detection covers puzzle_ext_image path |
| `web/puzzle_tokens.py` | `is_cul` param unchanged |
| `shared/puzzle_model.py` | No `is_cul` field stored |

## Risks

1. **Cache invalidation**: PROCESSING_VERSION bump forces one-time re-processing. Intended.
2. **False positives**: Theoretical for genizah domain — no blue inks/illumination in these manuscripts.
3. **Faded blue mats (0.83%)**: Intentional limit. General `create_mask()` handles them via medium-saturation path.
4. **Union mask slightly more aggressive**: Auto-detected images get both blue AND normal removal. Could remove a few extra pixels at mask boundaries. Acceptable — better than missing blue mat entirely.

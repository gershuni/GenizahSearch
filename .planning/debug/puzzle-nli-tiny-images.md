# Debug Handoff: NLI Images Appear Tiny in Desktop Puzzle

**Date:** 2026-03-18
**Status:** OPEN — investigation incomplete, user asked for handoff
**Reporter:** User observed during Mosseri CUDL testing session
**Symptom:** NLI images load in desktop puzzle but appear as very small thumbnails
**Screenshot:** User provided screenshot showing Austrian National Library Rainer Collection ("Ms. 1") with 2-3 tiny fragments on puzzle canvas alongside a large ResultDialog image

## Context: What Was Done This Session

### Completed: Mosseri CUDL Image Source (quick-260318-tkj)
Added CUDL as image source for Mosseri collection. 4 commits:
- `398e44f7` feat: `construct_mosseri_cudl_label()` + wire into `enrich_metadata`
- `3263db96` docs: planning artifacts
- `b0efd91e` fix(web puzzle.py): route Mosseri CUDL through external provider path
- `c4f8fc1a` fix(desktop genizah_app.py + gui_threads.py): same for desktop

### Changes made in this session (potential regression sources)

#### `gui_threads.py:964-966` (PuzzleMetaLoaderThread)
```python
# BEFORE:
use_ext = (images_ext and external_provider
           and external_provider != 'cambridge')
# AFTER:
use_ext = (images_ext and external_provider
           and (external_provider != 'cambridge' or lib_code != 'CUL'))
```
**Impact analysis:** For Vienna/Rainer records: `external_provider=''`, `lib_code='Vienna'`. `use_ext = ([] and '' and ...)` = False. **No change in behavior.** The condition only matters when `external_provider='cambridge'`.

#### `genizah_app.py:3607` (_add_fragment threshold)
```python
# BEFORE:
if lib_code == 'CUL':
# AFTER:
if lib_code == 'CUL' or external_provider == 'cambridge':
```
**Impact analysis:** For Vienna records: `lib_code='Vienna'`, `external_provider=''`. Falls through to default threshold=30. **No change.**

#### `genizah_app.py:3641` (_add_fragment is_cul)
```python
# BEFORE:
is_cul = (lib_code == 'CUL') or (shelfmark and shelfmark.upper().startswith(('T-S', 'OR.', 'ADD.')))
# AFTER:
is_cul = (lib_code == 'CUL') or (external_provider == 'cambridge') or (shelfmark and shelfmark.upper().startswith(('T-S', 'OR.', 'ADD.')))
```
**Impact analysis:** For Vienna: `external_provider=''`. **No change.**

#### `genizah_app.py:4045-4051` (_is_cul_fragment)
Added `getattr(pf, 'external_provider', '') == 'cambridge'` check. **No change for non-Cambridge.**

#### `web/pages/puzzle.py:1951-1957` (_resolve_folios)
Same pattern as gui_threads.py. **No change for non-Cambridge records.**

#### `web/pages/puzzle.py:2113-2121` (_add_fragment_to_puzzle external threshold)
Added `ext_is_cul = external_provider == 'cambridge'`. **No change for non-Cambridge.**

**Conclusion: None of the session's changes affect Vienna/Rainer/NLI records.** The tiny-image issue is either pre-existing or caused by something else.

## Investigation Performed

### 1. NLI IIIF returns correct 800px images
Tested FL ID 168830869 (Rainer Collection H 120, sys_id 990001697030205171):
```
curl → status=200, size=174395
PIL.Image.open → 800x1065 pixels
```
The NLI IIIF endpoint returns proper 800px images for this collection. Not a server-side issue.

### 2. Image loading path is correct
- `PuzzleMetaLoaderThread.run()` calls `enrich_metadata()` → gets `images_nli` with FL IDs
- For Vienna: `use_ext = False` → emits `images_nli` via `meta_ready` signal
- `_on_meta_resolved()` calls `add_fragment(sys_id, shelfmark, label, fl_id, ...)`
- `PuzzleImageLoaderThread(fl_id, threshold=30, size=800, processed=True, is_cul=False)`
- `resolve_fragment_image()` → `_fetch_iiif_image(fl_id, 800)` → fetches `{NLI_IIIF_BASE}/FL{digits}/full/800,/0/default.jpg`
- `remove_background()` does NOT resize — works on original dimensions
- Image bytes emitted back to `_on_image_loaded()` → creates `QPixmap` from bytes

### 3. Background removal preserves dimensions
`remove_background()` in `shared/background_removal.py` opens image, creates mask, applies alpha channel, saves as PNG. No resize anywhere in the pipeline.

### 4. `_fit_all_fragments()` might explain visual appearance
After each fragment loads, `_fit_all_fragments()` calls `self.canvas_view.fitInView(rect, KeepAspectRatio)`. If fragments are spread across a large scene area, the view zooms out significantly, making 800px images appear small on screen. The Scale slider shows fragment scale (1.0x), NOT view zoom.

## Leading Hypothesis

**`_fit_all_fragments()` view zoom, NOT actual tiny images.** The screenshot shows fragments with visible background-removal detail (transparency grid, color swatches) suggesting real data was processed. If images were genuinely ~100px, background removal would look different. The QGraphicsView `fitInView` zooms the view to fit all items, making large images appear small when fragments are spread apart.

### How to verify:
1. In the puzzle, scroll-wheel zoom INTO one of the "tiny" fragments. If it reveals 800px of detail, the image is full-size but the view is zoomed out.
2. If zooming in shows blocky/pixelated content, the source image is genuinely small.

### Alternative hypothesis:
NLI returns small images for specific FL IDs despite advertising large canvas dimensions in the manifest. This would mean the IIIF endpoint serves different quality levels for different collections. Unlikely given the curl test above returned 800x1065.

## Possible Fixes (if genuinely tiny)

### A. Increase puzzle image size from 800 to 2000
Match ResultDialog's 2000px request. Change in `PuzzleImageLoaderThread.__init__` default or pass `size=2000` from `_add_fragment`.
- **Pro:** Simple, matches working code
- **Con:** Larger images = more memory, slower bg removal

### B. Don't call `_fit_all_fragments` after every load
Only fit on initial document load, not individual fragment adds.
- **Pro:** User controls zoom; fragments appear at natural size
- **Con:** User may lose fragments off-screen

### C. Add minimum zoom level
In `_fit_all_fragments`, clamp the zoom so fragments never appear smaller than a threshold.

## Files to Examine

| File | Lines | What |
|------|-------|------|
| `genizah_app.py` | 3937-3949 | `_fit_all_fragments()` — fitInView logic |
| `genizah_app.py` | 3866-3936 | `_on_image_loaded()` — fragment creation from bytes |
| `genizah_app.py` | 3583-3648 | `add_fragment()` — PuzzleImageLoaderThread creation |
| `gui_threads.py` | 904-934 | `PuzzleImageLoaderThread` — size=800 default |
| `shared/puzzle_image_service.py` | 94-155 | `resolve_fragment_image()` — fetch + BG removal |
| `shared/puzzle_image_service.py` | 230-262 | `_fetch_iiif_image()` — NLI IIIF URL construction |
| `shared/background_removal.py` | 129-177 | `remove_background()` — no resize in pipeline |

## Key Test Cases

- **Vienna/Rainer:** sys_id `990001697030205171`, FL `168830869` — confirmed 800x1065 from NLI
- **Mosseri (CUDL):** sys_id `990053803330205171` — should now route through Cambridge external path
- **CUL T-S (control):** Any T-S shelfmark — should still use NLI FL IDs with full-size images

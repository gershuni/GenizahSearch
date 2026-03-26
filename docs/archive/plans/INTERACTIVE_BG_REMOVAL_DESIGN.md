# Interactive Click-to-Remove Background Removal — Design Document

**Status:** Deferred (2026-03-22)
**Issue:** Brown backing page not removed on glued manuscripts (BL, Oxford)
**Approach:** Interactive click-to-remove with additive steps and per-step undo

---

## 1. Problem Statement

Some manuscripts are glued onto backing pages whose color is too close to parchment for automatic HSV-based removal:

- **BL (British Library)**: Brown backing page glued OVER a blue conservation mat. Blue mat auto-detection removes the blue, but a brown rectangular backing page survives between the blue and the parchment.
- **Oxford (Bodleian)**: Full brown background — parchment sits directly on brown, no blue mat involved. Corner-sampled HSV distance is too small to distinguish brown from parchment.

Auto-detection was considered and rejected: brown is too close to parchment in all color spaces (HSV, Lab, RGB). A Gemini-authored analysis (`docs/plans/BROWN_BG_REMOVAL.md`) recommended deep learning (U-Net), which is impractical given the project's Pillow+NumPy-only constraint.

## 2. Proposed UX

**Interactive "eraser" tool in the puzzle page:**

1. User clicks a toolbar button to enter eraser mode (cursor becomes crosshair)
2. User clicks on a background area of a fragment image
3. Server applies edge-aware flood fill from the clicked pixel, making connected same-color pixels transparent
4. Multiple clicks are additive — each removes another area/color
5. Each step is individually undoable
6. A tolerance slider (10-80, default 32) controls how aggressively colors spread from the seed

This approach is general-purpose: works for brown, gray, cream, blue, or any other scanning background. The user, not the algorithm, decides what's background.

## 3. Algorithm: BFS Flood Fill

New function `flood_fill_remove(image_bytes, seed_x, seed_y, tolerance=32) -> bytes`:

- Open image as RGBA (preserving existing alpha from prior removals)
- Sample RGB color at (seed_x, seed_y) — this is the target color
- BFS from seed pixel: for each pixel, if RGB Euclidean distance from target <= tolerance AND alpha > 0, mark for removal and add 4-connected neighbors to queue
- Set alpha=0 for all marked pixels
- Optional: 1px edge feather (alpha=128 on boundary) — start without blur, tune later
- Return RGBA PNG bytes
- Pillow + NumPy only. `collections.deque` for BFS. 800px images are fast enough without optimization.

## 4. Architecture Considerations (from Code Review)

A Codex code review identified 5 critical issues with the initial plan. All must be addressed before implementation.

### 4.1 Persistence (Critical)

**Problem:** The puzzle page saves fragment metadata (fl_id, transforms, crop state) and rebuilds images from source on load. A purely client-side click-removal would be lost on:
- Save / reload document
- Publish / fork document
- Export to PNG
- Toggle processed/original
- Change threshold slider
- Navigate folios

**Solution — Hybrid seed-point persistence:**
- Store eraser steps in fragment metadata: `eraser_steps: [{x, y, tolerance}, ...]`
- On save, `eraser_steps` is included in `fragments_json` in joins.db
- On load, after fetching the base processed image, replay eraser steps sequentially
- Server caches the final derivative keyed by `{fl_id}_{size}_{threshold}_{steps_hash}.png`
- On export (`shared/puzzle_export.py`), use the derivative image

**Files affected:**
- `shared/puzzle_model.py` — add `eraser_steps` field to `PuzzleFragment` dataclass
- `shared/puzzle_service.py` — serialize/deserialize eraser_steps in fragments_json
- `shared/puzzle_export.py` (~line 217) — use derivative image for export
- `web/pages/puzzle.py` (~line 2226) — include eraser_steps in save metadata
- `web/pages/puzzle.py` (~line 1688, 2692) — replay steps on image rebuild

### 4.2 Authentication (Critical)

**Problem:** The initial plan proposed an unauthenticated `POST /api/puzzle_click_remove` accepting arbitrary image bytes — a security regression. The existing `puzzle_process` endpoint requires an HMAC upload token.

**Solution:** Require the same upload token (`web/puzzle_tokens.py`) on the click-remove endpoint. Use `_check_puzzle_rate_limit` for rate limiting.

### 4.3 Coordinate Transform (Critical)

**Problem:** Click coordinates in canvas space must be mapped to pixel coordinates in the source image. `toLocalPoint() / scaleX / scaleY` alone does NOT account for:
- `cropX` / `cropY` (crop offset)
- `flipX` / `flipY` (mirror state)
- Rotation angle
- Viewport zoom/pan transform

**Solution:** Full transform chain:
```javascript
// Canvas point -> object local point
let local = obj.toLocalPoint(new fabric.Point(canvasX, canvasY), 'left', 'top');
// Account for scale
let pixelX = local.x / obj.scaleX;
let pixelY = local.y / obj.scaleY;
// Account for crop offset
pixelX += (obj.cropX || 0);
pixelY += (obj.cropY || 0);
// Account for flip
if (obj.flipX) pixelX = obj.width - pixelX;
if (obj.flipY) pixelY = obj.height - pixelY;
// Clamp
pixelX = Math.max(0, Math.min(pixelX, sourceWidth - 1));
pixelY = Math.max(0, Math.min(pixelY, sourceHeight - 1));
```

Existing crop/flip/rotation code is at:
- `web/pages/puzzle.py:1224` (crop)
- `web/pages/puzzle.py:1599` (flip)
- `web/pages/puzzle.py:1774` (restoration)

### 4.4 Fabric.js Event Model (Design)

**Problem:** The initial plan set `selectable=false` and `evented=false` on objects to prevent drag, then tried to detect click targets — contradictory since non-evented objects are excluded from Fabric hit-testing.

**Solution:** Use a single interaction mode enum (`'select' | 'crop' | 'eraser'`) integrated into the existing `mouse:down` handler at `web/pages/puzzle.py:774`. The handler checks the current mode and routes the event:
- `'select'`: existing drag/selection behavior
- `'crop'`: existing crop behavior
- `'eraser'`: seed-point extraction → server call → image update

Objects stay `evented=true` at all times. The mode determines what happens on click, not whether objects receive events. Eraser and crop modes are mutually exclusive (enforced by the enum).

### 4.5 Undo Stack (Design)

**Problem:** Full-image data URLs are memory-heavy and unbounded. Autosave hooks listen for specific events, not a generic `puzzle-change`.

**Solution:**
- Cap undo stack at **10 steps per fragment**
- Use `Blob` / object URLs instead of base64 data URLs (smaller memory footprint)
- Alternatively, since seed points are stored in metadata, undo = pop last seed point + replay remaining (no image snapshots needed, but slower for many steps)
- Wire autosave to the specific eraser completion event, following the pattern at `web/pages/puzzle.py:3437` and `web/pages/puzzle.py:3528`

## 5. Implementation Estimate

### Files to modify
| File | Changes |
|------|---------|
| `shared/background_removal.py` | Add `flood_fill_remove()` function |
| `shared/puzzle_model.py` | Add `eraser_steps` to PuzzleFragment |
| `shared/puzzle_service.py` | Serialize eraser_steps |
| `shared/puzzle_export.py` | Use derivative image in export |
| `shared/puzzle_image_service.py` | Derivative cache support, bump PROCESSING_VERSION |
| `web/api.py` | New authenticated endpoint |
| `web/pages/puzzle.py` | Eraser mode, click handler, undo, persistence, replay |

### Complexity
This is a medium-sized feature touching 7 files across 3 layers (algorithm, service, UI). The algorithm itself is straightforward (BFS flood fill). The complexity is in:
- Persistence plumbing (saving/loading/replaying eraser steps)
- Coordinate transform correctness across all canvas states
- Event model integration without breaking existing tools
- Export pipeline integration

Recommend implementing as a full GSD phase rather than a quick task.

## 6. References

- `docs/plans/BROWN_BG_REMOVAL.md` — Gemini analysis of the general problem (deep learning approach, impractical for this project)
- `shared/background_removal.py` — Current HSV-based bg removal engine
- `shared/puzzle_image_service.py` — Image fetch + cache + bg removal orchestration
- `web/pages/puzzle.py` — Puzzle page with Fabric.js canvas (~3500 lines)
- `web/puzzle_tokens.py` — HMAC upload token generation

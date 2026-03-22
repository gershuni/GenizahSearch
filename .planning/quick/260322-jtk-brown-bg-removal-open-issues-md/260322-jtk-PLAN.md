---
phase: quick-260322-jtk
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - shared/background_removal.py
  - shared/puzzle_image_service.py
  - scripts/test_brown_bg_removal.py
  - docs/OPEN_ISSUES.md
autonomous: false
requirements: [BROWN-BG-01]

must_haves:
  truths:
    - "BL images with brown backing page over blue mat have brown rectangle removed after blue mat removal"
    - "Oxford images with full brown background have brown background removed without destroying parchment"
    - "Existing blue mat removal for CUL and other libraries is not regressed"
    - "Normal (non-brown, non-blue) background removal is not regressed"
    - "Safety fallback still prevents over-removal (foreground < 5%)"
  artifacts:
    - path: "shared/background_removal.py"
      provides: "Brown background detection and removal functions"
      contains: "detect_brown_backing"
    - path: "shared/puzzle_image_service.py"
      provides: "Bumped PROCESSING_VERSION to v5"
      contains: "PROCESSING_VERSION = 'v5'"
    - path: "scripts/test_brown_bg_removal.py"
      provides: "Visual test script for brown bg removal validation"
  key_links:
    - from: "shared/background_removal.py"
      to: "shared/puzzle_image_service.py"
      via: "remove_background import"
      pattern: "from shared.background_removal import remove_background"
    - from: "shared/background_removal.py"
      to: "remove_background()"
      via: "brown detection integrated into main entry point"
      pattern: "detect_brown_backing"
---

<objective>
Add brown background removal for two distinct manuscript scanning patterns:
1. BL (British Library): brown backing page glued over blue mat -- after blue removal, a brown rectangle survives
2. Oxford (Bodleian): full brown background with parchment sitting directly on brown, no blue mat

Purpose: These two libraries produce images where current bg removal either leaves brown rectangles (BL) or fails entirely (Oxford, brown too close to parchment in HSV). The edge-based approach using Pillow's built-in filters detects fragment boundaries by physical edge/shadow transitions rather than relying solely on color distance.

Output: Updated background_removal.py with brown backing detection and removal, bumped cache version, test script.
</objective>

<execution_context>
@~/.claude/get-shit-done/workflows/execute-plan.md
@~/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@shared/background_removal.py
@shared/puzzle_image_service.py
@scripts/test_blue_mat_detection.py (pattern reference for test script)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Implement brown background detection and removal in background_removal.py</name>
  <files>shared/background_removal.py</files>
  <action>
Add brown background removal to background_removal.py using an edge-aware flood-fill approach with Pillow+NumPy only:

**1. Brown detection function `detect_brown_backing(hsv_array) -> float`:**
- Brown in Pillow HSV (0-255): Hue roughly 10-30 (warm orange-brown range), Saturation 30-120 (clearly colored but not vivid), Value 60-180 (medium brightness, not white/black)
- Return fraction of pixels matching brown range
- Detection threshold: ~8% of pixels (similar logic to blue mat's 2% but higher because brown can be a smaller portion for BL)
- Constants: BROWN_HUE_MIN=8, BROWN_HUE_MAX=35, BROWN_SAT_MIN=25, BROWN_SAT_MAX=130, BROWN_VAL_MIN=50, BROWN_VAL_MAX=190, BROWN_DETECT_THRESHOLD=0.08

**2. Brown removal function `create_brown_removal_mask(hsv_array, rgb_array) -> Image.Image`:**
- This is the hard part because brown overlaps parchment in HSV.
- Strategy: edge-detection + border flood fill (NOT pure color segmentation)
- Step A: Convert RGB to grayscale numpy array (0.299R + 0.587G + 0.114B)
- Step B: Apply Pillow's ImageFilter.FIND_EDGES to get edge map, then threshold edge map at value 30 to get binary edge array (strong edges = True)
- Step C: Dilate edge array using MaxFilter(3) to thicken edge boundaries
- Step D: Flood fill from all 4 borders — mark every pixel reachable from the image border without crossing a strong edge as "background". Use a BFS/queue approach with NumPy: start with border pixels, expand to 4-connected neighbors, stop at edge pixels. This captures the brown backing/background because the physical fragment boundary (torn parchment edge = shadow/texture change) creates a strong edge barrier.
- Step E: Also include a relaxed brown color mask — pixels that are clearly brown (tight HSV range: within Euclidean distance 25 of the median brown color sampled from detected brown pixels) AND are connected to the flood-filled background region. This catches brown areas that the flood fill might miss due to weak edges.
- Step F: Combine flood-fill mask with the connected brown color mask
- Step G: Morphological cleanup — MinFilter(5) erode then MaxFilter(7) dilate (slightly more aggressive than standard to clean flood-fill artifacts)
- Return mask (foreground=255, background=0)

**3. Integrate into `remove_background()` entry point:**
- After the existing blue mat detection block (line 217-227), add brown detection:
- When blue mat IS detected AND brown backing is detected (BL pattern):
  - First apply blue mat mask, then detect if remaining non-blue area has brown backing
  - Apply brown removal mask to the non-blue region
  - Combine: pixel is background if blue mask says background OR brown mask says background
- When NO blue mat detected AND brown backing is detected (Oxford pattern):
  - Use `create_brown_removal_mask()` directly as the primary mask
- When neither: existing behavior unchanged
- Add import for `collections.deque` (for BFS flood fill)
- Log detection: `logger.info(f"Detected brown backing ({brown_frac:.1%} brown pixels), applying edge-aware removal")`

**4. Important edge cases and safety:**
- The flood fill MUST NOT traverse the entire image if edges are sparse — add a max-fill limit (if filled area > 85% of image, fall back to color-only mask with conservative threshold)
- The existing MIN_FOREGROUND_RATIO (5%) safety check still applies AFTER brown removal
- Do NOT modify any existing function signatures — only add new functions and extend `remove_background()` logic
- Keep all existing constants and legacy aliases intact

**5. RGB array access:**
- The function already has `img` (RGB PIL Image) available. Convert to numpy: `rgb_array = np.array(img)`
- Pass both hsv_array and rgb_array to the brown removal function (edges work better on luminance from RGB than on HSV channels)
  </action>
  <verify>
    <automated>python -c "from shared.background_removal import remove_background, detect_brown_backing; print('Import OK')"</automated>
  </verify>
  <done>
- `detect_brown_backing()` function exists and returns float fraction
- `create_brown_removal_mask()` function exists using edge-aware flood fill
- `remove_background()` handles BL (blue+brown) and Oxford (brown-only) patterns
- All existing functions unchanged in signature
- No new dependencies beyond Pillow+NumPy+collections.deque
  </done>
</task>

<task type="auto">
  <name>Task 2: Bump PROCESSING_VERSION and create brown bg removal test script</name>
  <files>shared/puzzle_image_service.py, scripts/test_brown_bg_removal.py</files>
  <action>
**Part A: Bump PROCESSING_VERSION in puzzle_image_service.py**
- Change `PROCESSING_VERSION = 'v4'` to `PROCESSING_VERSION = 'v5'` (line 29)
- This invalidates all cached processed images, forcing re-processing with the new algorithm

**Part B: Create test script scripts/test_brown_bg_removal.py**
Model after scripts/test_blue_mat_detection.py pattern but for brown backgrounds:

1. Define test image sources:
   - BL images (blue mat + brown backing): fetch 3-5 BL fragment images via IIIF
     - Use known BL shelfmarks from nli_crossref.db or hardcode FL IDs
     - Example BL library_code = 'BL' in libraries.csv
   - Oxford images (full brown background): fetch 3-5 Oxford fragment images
     - Example Oxford library_code = 'Oxford'
   - Control images (should NOT trigger brown detection):
     - CUL blue mat images (2-3, from existing cache in scripts/bg_removal_test/cache/)
     - Plain white/cream background images (2-3)

2. For each test image:
   - Fetch via IIIF (with fallback to cached)
   - Run `detect_brown_backing()` and report percentage
   - Run full `remove_background()` and save result PNG
   - Save side-by-side comparison (original | processed) as HTML report

3. Output: `scripts/bg_removal_test/brown_bg_report.html` with:
   - For each image: original thumbnail, processed thumbnail, brown %, blue %, detection verdict
   - Summary: how many BL/Oxford correctly detected, how many controls correctly skipped

4. Also run existing blue mat images through to verify NO regression:
   - Blue detection percentage should be unchanged
   - CUL images should still have blue removed correctly

5. The script should be runnable standalone: `python scripts/test_brown_bg_removal.py`
   - Accept --cached-only flag to skip live IIIF fetches (use only what's in cache)
   - If IIIF fetch fails, skip that image with a warning (don't fail the whole script)

To find BL and Oxford FL IDs for testing, query nli_crossref.db:
```python
import sqlite3
conn = sqlite3.connect('nli_crossref.db')
# BL images
bl_ids = conn.execute("""
    SELECT DISTINCT fgp_image_number_id FROM nli_crossref
    WHERE call_number LIKE 'Or %' AND source_library = 'BL'
    LIMIT 5
""").fetchall()
# Oxford images
ox_ids = conn.execute("""
    SELECT DISTINCT fgp_image_number_id FROM nli_crossref
    WHERE source_library = 'Oxford'
    LIMIT 5
""").fetchall()
```
NOTE: fgp_image_number_id is a Friedberg photo number, NOT an NLI FL ID. To get actual IIIF-fetchable FL IDs, check if nli_crossref has an fl_id or nli_id column. If not, hardcode a few known FL IDs from manual inspection, or use the image_url column if available. Document this clearly in the script.
  </action>
  <verify>
    <automated>python -c "from shared.puzzle_image_service import PROCESSING_VERSION; assert PROCESSING_VERSION == 'v5', f'Expected v5, got {PROCESSING_VERSION}'; print('Version bump OK')"</automated>
  </verify>
  <done>
- PROCESSING_VERSION is 'v5' in puzzle_image_service.py
- scripts/test_brown_bg_removal.py exists and runs without import errors
- Script produces visual comparison output for manual inspection
- Script tests both BL (blue+brown) and Oxford (brown-only) patterns
- Script includes control images to verify no regression on CUL/normal backgrounds
  </done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <name>Task 3: Visual verification of brown background removal</name>
  <files>scripts/bg_removal_test/brown_bg_report.html</files>
  <action>
Run the test script and visually verify brown background removal quality:
1. Run: `python scripts/test_brown_bg_removal.py`
2. Open: `scripts/bg_removal_test/brown_bg_report.html`
3. Verify BL images: brown backing rectangle removed, parchment preserved
4. Verify Oxford images: brown background removed, parchment preserved (some edge artifacts acceptable)
5. Verify CUL controls: no regression in blue mat removal
6. Optionally test in puzzle UI with real BL and Oxford fragments
  </action>
  <verify>
    <automated>python -c "from pathlib import Path; assert Path('scripts/bg_removal_test/brown_bg_report.html').exists(), 'Report not generated'; print('Report exists')"</automated>
  </verify>
  <done>User has visually confirmed brown background removal quality is acceptable for both BL and Oxford patterns, with no regression on existing libraries</done>
</task>

</tasks>

<verification>
- `python -c "from shared.background_removal import remove_background, detect_brown_backing, create_brown_removal_mask; print('All imports OK')"` succeeds
- `python -c "from shared.puzzle_image_service import PROCESSING_VERSION; assert PROCESSING_VERSION == 'v5'"` succeeds
- `python scripts/test_brown_bg_removal.py --cached-only` runs without errors (if cached images available)
- Existing tests still pass: `pytest tests/ -x -q` (if any bg removal tests exist)
</verification>

<success_criteria>
- BL images: brown backing page removed after blue mat removal, parchment intact
- Oxford images: brown background removed with acceptable edge quality, parchment intact
- CUL/normal images: zero regression in existing background removal
- PROCESSING_VERSION bumped to v5, forcing cache invalidation
- Safety fallback (5% minimum foreground) still prevents catastrophic removal
</success_criteria>

<output>
After completion, create `.planning/quick/260322-jtk-brown-bg-removal-open-issues-md/260322-jtk-SUMMARY.md`
</output>

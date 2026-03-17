---
phase: quick
plan: 260317-aru
type: execute
wave: 1
depends_on: []
files_modified:
  - shared/background_removal.py
  - tests/test_background_removal.py
autonomous: true
requirements: []
must_haves:
  truths:
    - "CUL images with gray border + blue mat have BOTH backgrounds removed"
    - "Images with only one background color still work identically"
    - "Edge midpoint sampling detects the inner mat color when corners show the outer border"
  artifacts:
    - path: "shared/background_removal.py"
      provides: "Two-pass background removal with edge midpoint secondary detection"
      contains: "detect_edge_midpoint_color"
    - path: "tests/test_background_removal.py"
      provides: "Tests for two-layer background scenario"
      contains: "test_two_layer"
  key_links:
    - from: "remove_background"
      to: "detect_edge_midpoint_color"
      via: "called after first-pass mask to check edge midpoints for secondary bg"
      pattern: "detect_edge_midpoint_color"
---

<objective>
Fix background removal for CUL manuscript images that have a gray/white border frame surrounding a blue conservation mat, with the parchment fragment on the blue mat.

Problem: Corner sampling detects the gray border as background and removes only that, leaving the blue mat as "foreground."

Solution: After the first-pass mask, sample edge midpoints of the image. If those midpoints show a high-saturation color (S > 100) that differs from the corner-detected color, create a second mask for that color and combine both masks (pixel is foreground only if it differs from BOTH backgrounds).

Purpose: CUL images are a major collection (~128K records) and their IIIF images consistently use this border+mat layout.
Output: Updated background_removal.py with two-pass detection, updated tests.
</objective>

<execution_context>
@C:/Users/gersh/.claude/get-shit-done/workflows/execute-plan.md
@C:/Users/gersh/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@shared/background_removal.py
@tests/test_background_removal.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add two-pass background removal with edge midpoint detection</name>
  <files>shared/background_removal.py, tests/test_background_removal.py</files>
  <behavior>
    - Test: Two-layer image (gray border, blue inner mat, white center square) -- corners detect gray, edge midpoints detect blue, BOTH are removed, center stays opaque
    - Test: Single-layer image (solid blue bg, white center) -- no secondary detection triggered, behavior unchanged
    - Test: Single-layer low-saturation image (gray bg, white center) -- no secondary detection triggered, behavior unchanged
    - Test: detect_edge_midpoint_color returns median HSV from 4 edge midpoint sample patches
    - Test: Edge midpoints that match corner color (same single bg) do NOT trigger second pass
  </behavior>
  <action>
1. Add `detect_edge_midpoint_color(hsv_array)` function:
   - Sample 4 patches at edge midpoints: top-center, bottom-center, left-center, right-center
   - Each patch is CORNER_SAMPLE_SIZE x CORNER_SAMPLE_SIZE pixels
   - Return median HSV as numpy array (3,), same format as detect_background_color

2. Add `_colors_are_different(color_a, color_b, min_distance=40.0)` helper:
   - Returns True if two HSV colors are sufficiently different
   - Use HSV Euclidean distance. 40.0 threshold distinguishes gray border from blue mat.

3. Modify `remove_background()` after the first-pass mask creation:
   - Call `detect_edge_midpoint_color(hsv_array)` to get midpoint color
   - Check if midpoint color has high saturation (S > HIGH_SATURATION_THRESHOLD=100) AND is different from the corner-detected bg_color (using _colors_are_different)
   - If yes: create a second mask via `create_mask(hsv_array, midpoint_color, threshold)`
   - Combine masks: final_mask = pixel-wise minimum of both masks (foreground only where BOTH masks say foreground)
   - Use this combined mask for the alpha channel
   - If no secondary color detected: behavior is unchanged (single mask as before)

4. Add tests in a new `TestTwoLayerBackground` class:
   - `make_two_layer_image(outer_color, inner_color, fg_color, size=300, inner_margin=40, fg_size=60)` helper:
     Creates image with outer border of outer_color, inner rectangle of inner_color, centered fg square of fg_color
   - Test that gray-border + blue-mat + white-center removes both backgrounds
   - Test that single-layer images are unaffected
   - Test detect_edge_midpoint_color directly on a two-layer image
   - Test that same-color edge midpoints do not trigger second pass

5. Export the new function in the import list in test file.
  </action>
  <verify>
    <automated>cd C:/GenizahSearch && python -m pytest tests/test_background_removal.py -x -v</automated>
  </verify>
  <done>
    - All existing tests still pass (no regression)
    - New two-layer tests pass: gray border + blue mat both removed, white center stays opaque
    - Single-layer images produce identical results (no secondary pass triggered)
    - detect_edge_midpoint_color correctly samples edge midpoints
  </done>
</task>

</tasks>

<verification>
All tests in tests/test_background_removal.py pass with no regressions.
</verification>

<success_criteria>
- CUL-style two-layer images (gray border + blue mat) have both backgrounds removed to transparent
- Single-background images behave identically to before (no regression)
- All tests pass
</success_criteria>

<output>
After completion, create `.planning/quick/260317-aru-fix-background-removal-for-cul-blue-cons/260317-aru-SUMMARY.md`
</output>

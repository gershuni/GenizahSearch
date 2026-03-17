# Quick Task 260317-aru: Fix background removal for CUL blue conservation mat

## Problem
CUL manuscript images have a gray/white border frame with rulers around a blue conservation mat. The background removal algorithm only sampled corners, detecting the gray border as background but missing the blue mat entirely.

## Solution
Two-pass background detection in `shared/background_removal.py`:
1. **Edge midpoint sampling** (`detect_edge_midpoint_color`): Samples 4 patches inset ~20% from each edge to detect inner mat colors
2. **Secondary background check**: If edge midpoints show a high-saturation color (S > 100) with different hue from corners, creates a second mask
3. **Combined mask**: Pixel is foreground only if different from BOTH backgrounds (min of both masks)
4. **Force Euclidean**: When secondary bg detected, primary mask uses full HSV Euclidean instead of V-only, preventing false removal of parchment with similar brightness to the border

## Files Changed
- `shared/background_removal.py` — Added `detect_edge_midpoint_color()`, `force_euclidean` param to `create_mask()`, two-pass logic in `remove_background()`
- `tests/test_background_removal.py` — 6 new tests in `TestTwoLayerBackground` class
- `CHANGELOG.md` — Bug fix entry, known issue resolved

## Test Results
17/17 tests pass (11 existing + 6 new), no regressions.

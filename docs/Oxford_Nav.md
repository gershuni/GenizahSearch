# Oxford Browse Navigation Issue

**Date:** January 2025
**Status:** Open - Needs Investigation

---

## Problem Description

When navigating in the Browse tab from MS heb. f.21/17 to the previous page:
- **Expected:** MS heb. f.21/16 (text, info, and image)
- **Actual:**
  - Image shows correctly: f.21/16 ✓
  - Text and info show incorrectly: f.2/16 ✗ (missing "1" in "21")

## Analysis

The navigation uses `get_browse_page()` in `genizah_core.py` which returns:
- `sys_id` - Used for image loading → **Correct**
- `text` - Page content from browse_map → **Wrong**
- `full_header` - Header from browse_map → **Wrong**

Since the image is correct but text is wrong, the issue is in the `browse_map.pkl` data file, not the navigation logic.

## Technical Details

### Data Flow
1. `browse_navigate()` calls `searcher.get_browse_page()`
2. `get_browse_page()` uses `get_adjacent_sys_id_by_file_order()` for cross-manuscript navigation
3. Returns `page_data` with `sys_id`, `text`, `full_header` from `browse_map`
4. Image loading uses `self.current_browse_sid` (from `page_data['sys_id']`) - works correctly
5. Text display uses `page_data['text']` and `page_data['full_header']` - shows wrong content

### Relevant Files
- `genizah_core.py`: `get_browse_page()` (line ~4282), `get_adjacent_sys_id_by_file_order()` (line ~4392)
- `browse_map.pkl`: Cached navigation data built from Transcriptions.txt
- `Transcriptions.txt`: Source data (user confirms order is correct here)

## Possible Causes

1. **Corrupted browse_map.pkl** - The pickle file may have wrong text/header stored for some sys_ids
2. **Stale cache** - browse_map.pkl may need to be regenerated from Transcriptions.txt
3. **Build issue** - The process that builds browse_map.pkl may have a bug

## Suggested Fix

1. Verify Transcriptions.txt has correct content for f.21/16 and f.21/17
2. Delete `browse_map.pkl` to force regeneration
3. Restart the application to rebuild the browse_map
4. If issue persists, investigate the browse_map building code

## Notes

- This issue is **not related** to the Oxford image loading fixes
- The image loading uses `self.current_browse_sid` which is correct
- Only the text/header display is affected

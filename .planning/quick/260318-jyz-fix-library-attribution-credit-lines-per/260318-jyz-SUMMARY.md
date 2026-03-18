---
plan: 260318-jyz
status: complete
duration: 5min
tasks_completed: 2
tasks_total: 2
---

# Quick Task 260318-jyz: Fix library attribution credit lines

## What Changed

Previously, all non-Oxford manuscripts showed "מאוסף הספרייה הלאומית" (NLI default) as the
image credit, even for Manchester, BL, RNL, and other libraries.

### Task 1: Library-aware attribution

- **genizah_core.py**: Set `current_meta['attribution']` when Manchester canvases are loaded
  (previously skipped because `fetch_external_iiif_data()` was bypassed)
- **web/services.py**: Added `ATTRIBUTION_BY_LIBRARY` dict (13 entries) mapping library_code
  to proper attribution text. Replaces the hard NLI fallback in both `get_browse_page()` and
  `get_browse_page_by_fl()`.

Attribution fallback chain (new):
1. IIIF manifest attribution from cache (works for Cambridge, JTS)
2. Library-specific override from `ATTRIBUTION_BY_LIBRARY` dict
3. Oxford special case (detected by `is_oxford_manuscript()`)
4. Default: NLI

Libraries with IIIF manifest attribution (CUL, JTS): dict value = `None` → keep manifest text.
Libraries with NLI-digitized images (BL, RNL, AIU, etc.): "[Library Name] · image: הספרייה הלאומית"

### Task 2: Per-provider credit link

- **web/pages/browse.py**: Credit footer now routes links based on image source:
  - Oxford → digital.bodleian.ox.ac.uk
  - Manchester → luna.manchester.ac.uk
  - Cambridge → cudl.lib.cam.ac.uk
  - JTS → dpul.princeton.edu/cairo_geniza
  - BL → searcharchives.bl.uk
  - All others → NLI ktiv

Desktop needs no changes — it displays `meta['attribution']` from the cache, which is now correct.

## Files Modified

| File | Change |
|------|--------|
| genizah_core.py | Manchester attribution set in canvases block |
| web/services.py | ATTRIBUTION_BY_LIBRARY dict + library-aware fallback (2 code paths) |
| web/pages/browse.py | Per-provider credit link routing |

## Verification

- All 3 files compile without errors
- 421 tests pass (1 pre-existing unrelated failure in puzzle cache)
- ATTRIBUTION_BY_LIBRARY dict verified: 13 entries, correct None/string values

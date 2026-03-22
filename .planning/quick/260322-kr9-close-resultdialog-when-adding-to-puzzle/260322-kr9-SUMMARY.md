# Quick Task 260322-kr9: Close ResultDialog when adding to puzzle — Summary

## Changes (3 commits)

1. **Close dialog** — Added `self.close()` after `add_to_puzzle()` call so ResultDialog dismisses automatically
2. **Fix shelfmark** — Read from `display.shelfmark` (where all result dicts store it) instead of missing top-level key
3. **Fix image loading** — Rewrote `_add_to_puzzle` to mirror `_browse_add_to_puzzle`: gets fl_id from the viewer's NLI image list only, leaving it `None` for external sources (Cambridge, Oxford, etc.) so `PuzzleMetaLoaderThread` handles them with proper `image_url`. Also set `current_fl_id` during initial `load_result_by_index`.

## Root cause
The direct `fl_id` path in `add_to_puzzle` doesn't pass `image_url` or `external_provider`. For CUL manuscripts whose images come from Cambridge IIIF (not NLI), passing an NLI fl_id without a Cambridge URL caused "image not available" errors.

## Files changed
- `genizah_app.py` — `ResultDialog._add_to_puzzle()` rewritten, `load_result_by_index` sets `current_fl_id`

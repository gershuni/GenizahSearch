# Quick Task 260321-qjh: Summary

## What Changed

### 1. Fixed session restore for external library fragments
**Bug**: `init_canvas()` session restore at line 3638 had `if not fl_id: continue` which skipped ALL external library fragments (Oxford, Manchester, JTS, Cambridge CUDL). These fragments would appear when first added but vanish on page reload or navigation.

**Fix**: Added dual-path restore — NLI fragments use `/api/puzzle_image`, external fragments use `/api/puzzle_ext_image`. Also fixed `load_pending` counter which got out of sync when fragments were skipped (preventing auto-save from working).

**Impact**: Fragments from all libraries now persist across page reloads and navigations.

### 2. Wired Firefox AMO link into extension banner
**Bug**: Extension install banner only showed Chrome Web Store link regardless of browser.

**Fix**: Added Firefox detection (`InstallTrigger` / `navigator.userAgent`) and route to appropriate store:
- Firefox → https://addons.mozilla.org/addon/genizahsearch-image-helper/ (orange button)
- Chrome/other → Chrome Web Store (blue button)

### 3. Improved error handling in auto_add flow
**Bug**: `_after_delay` wrapper silently swallowed ALL exceptions (`except Exception: pass`), making it impossible to diagnose why fragments failed to add from browse/search "Add to Puzzle" buttons.

**Fix**: Replaced with `logger.error()` so failures appear in server logs.

## Files Changed
- `web/pages/puzzle.py` — all three fixes

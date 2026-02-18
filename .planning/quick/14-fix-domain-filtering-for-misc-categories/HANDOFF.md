# Handoff: Domain Filter Dialog Bugs

## Status
- **Quick task 14 (committed):** Added `qualify_domain_name()` helpers + updated dedup in all 4 locations. Commits: `8ce212a7`, `b5dd8abb`, `9bc50777`, `b522812b`.
- **Session 2 (uncommitted, abandoned):** Three UI approaches tried (ui.tree, checkboxes, HTML+JS). None worked.
- **Session 3 (uncommitted, IN WORKING TREE):** Reverted to ui.checkbox approach. Added pre-cache + merge fix + timing diagnostics. User reports nothing changed — **timing output not appearing in console**, meaning the modified code may not be loading (stale `.pyc`, wrong file being served, or NiceGUI not picking up changes).

## Two Bugs

### Bug 1: Dialog opens very slowly (7-19 seconds)
- **Symptom:** Clicking "Filter by domains" button → 7-19 seconds before dialog appears.
- **User context:** "The lag is NEW." Production v5.9 at genizahsearch.com does NOT have this lag. The user believes recent local development work introduced it.
- **Root cause: UNKNOWN.** Session 3 added `print()` timing diagnostics to the dialog function but NO output appeared in the server console. This means either:
  1. The modified `_open_domain_filter_dialog()` is NOT being called (stale bytecode / import cache)
  2. The web server is serving a cached/compiled version of the page
  3. NiceGUI's auto-reload didn't pick up the changes
  4. The function exits early before reaching the print (but early exit only happens if `has_domain_data` is False, which would also skip the dialog entirely)
- **CRITICAL FIRST STEP:** Verify code changes are actually running. Add `print("DOMAIN FILTER DIALOG CALLED")` as the VERY FIRST line of `_open_domain_filter_dialog()` (before any if-checks). Restart the server fully (`Ctrl+C` and re-run `python -m web.main`). Check console.

### Bug 2: "Other" domains don't toggle with Select All/None
- **Symptom:** "Select All" checks most checkboxes but "Other" entries stay unchecked.
- **User context:** "The 'Other' bug is older — probably from the time the domain filtering was first applied" (Phase 27).
- **Service-layer fix (IN WORKING TREE, VERIFIED):** `shared/fjms_service.py:~639-651` — merges duplicate "Other" children after orphan promotion. Confirmed: 13 unique "Other" entries, zero duplicates, counts are correct (e.g., Documentary: Other = 36).
- **UI-level fix: NOT DONE.** The merge fix prevents duplicate dict keys, but the UI toggle still doesn't work. Possible causes:
  1. **Dict key collision from qualified names:** `checkboxes[child['domain']] = child_cb` — if two "Other" children resolve to the same qualified name, the second overwrites the first checkbox reference. The merge fix should prevent this, but verify by logging `checkboxes.keys()` after building.
  2. **NiceGUI `.set_value()` not updating Quasar component:** MEMORY.md says "NiceGUI programmatic .value changes don't fire Vue events." The `select_all()` function uses `cb.set_value(True)` which should bypass handlers but still update the component. If it doesn't, try `cb.value = True; cb.update()` instead.
  3. **Race condition in batch updates:** Setting ~200 checkbox values sends ~200 WebSocket messages. Some may be dropped or delayed.

## What's in the Working Tree (Uncommitted Session 3 Changes)

### `shared/fjms_service.py` (KEEP THIS)
- **Lines ~639-651:** Duplicate children merge step after orphan promotion loop. Iterates each parent's children, consolidates entries with same domain name by summing counts. **Verified correct.**

### `web/pages/search.py`
- **Line ~60:** `SearchUIState.domain_hierarchy: dict = {}` field added
- **Lines ~2058-2067:** Pre-cache hierarchy during `execute_search()` via `await run.io_bound(fetch_hierarchy)` — runs after domain collection, before transcription lookup
- **Lines ~1683-1860:** `_open_domain_filter_dialog()` — reverted to `ui.checkbox` approach with:
  - Pre-cached hierarchy (no DB call on dialog open)
  - `perf_counter()` timing diagnostics (print to console)
  - `set_value(True/False)` for Select All/None instead of `.value = True` + `.update()`
  - Parent handlers for parent-child propagation
  - No per-child handlers (removed)

### `web/pages/parallels.py`
- **Line ~157:** `ParallelsState.domain_hierarchy: dict = {}` field added
- **Lines ~1310-1323:** Pre-cache hierarchy after parallels domain collection
- **Lines ~1398-1560:** `_open_parallels_domain_filter_dialog()` — same pattern as search.py

## Key Code Locations

| File | Lines | What |
|------|-------|------|
| `web/pages/search.py` | ~1683-1860 | `_open_domain_filter_dialog` (checkbox approach + timing) |
| `web/pages/search.py` | ~60 | `SearchUIState.domain_hierarchy` field |
| `web/pages/search.py` | ~2058-2067 | Pre-cache hierarchy during `execute_search()` |
| `web/pages/search.py` | ~690 | Button: `on_click=lambda: _open_domain_filter_dialog()` |
| `web/pages/parallels.py` | ~1398-1560 | Parallels domain filter dialog |
| `web/pages/parallels.py` | ~157 | `ParallelsState.domain_hierarchy` field |
| `web/pages/parallels.py` | ~1310-1323 | Pre-cache hierarchy for parallels |
| `shared/fjms_service.py` | ~639-651 | Duplicate children merge (**VERIFIED**) |
| `shared/fjms_service.py` | ~549-668 | `get_domain_hierarchy()` full function |
| `genizah_app.py` | ~4780-4995 | Desktop `DomainFilterDialog` (PyQt6, NOT modified) |

## What NOT to Do
- Don't use `ui.html()` with inline `onchange` handlers — tried in session 3, made lag WORSE (19s vs 7-12s)
- Don't use `ui.tree` with `tick_strategy='leaf'` + `tree.tick()` — "Other" doesn't toggle (Quasar bug)
- Don't use `_batch_updating` flags — they didn't help
- Don't use `_domain_hierarchy_cache` as a class variable — instance lifecycle issues
- Don't assume code changes are live — session 3 proved timing output didn't appear, suggesting stale code

## Recommended Next Steps (Priority Order)

### Step 1: Verify code is actually loading
Add `print("=== DOMAIN FILTER DIALOG CALLED ===")` as the absolute first line of `_open_domain_filter_dialog()`. Do a FULL restart of the web server. If this doesn't appear in the console when clicking the button, the problem is environmental (stale cache, wrong file path, etc.). Delete `__pycache__` directories and restart.

### Step 2: Diagnose the lag with timing
Once print output is confirmed working, the existing timing diagnostics will reveal WHERE the time goes:
- `hierarchy` — should be ~0s with pre-cache (or ~1.15s if fallback fires)
- `build_data` — should be <0.01s
- `create_ui` — likely the bottleneck (N checkboxes × ~50ms each)
- Check how many checkboxes are created — if it's 200, that's ~10s of NiceGUI element creation

### Step 3: Compare with production v5.9
The user says production doesn't lag. Check:
- How many checkboxes does production create? (Depends on search query — narrow query = fewer domains = fewer checkboxes)
- Is the production `fjms_enrichment.db` smaller (fewer domains)?
- Is production running a different NiceGUI version?

### Step 4: Fix the speed
If the bottleneck is confirmed as NiceGUI element creation (~200 checkboxes), the only fix is fewer elements. Options:
1. **Show only parent categories (~25 checkboxes):** Excluding a parent excludes all its children. Simpler UX, much faster. Add expandable children later.
2. **`ui.tree` with `tick_strategy='strict'`:** Single Vue component, all nodes independent. Not tried yet. Would need JS-based Select All via `tree._props['ticked'] = [all_ids]` + `tree.update()`.
3. **Client-side rendering via `ui.run_javascript()` AFTER dialog.open():** Create empty dialog with ~5 Python elements, then inject checkbox HTML via JS. Avoids NiceGUI element overhead entirely.

### Step 5: Fix "Other" toggle
With the merge fix committed and code loading confirmed, test Select All/None. If "Other" STILL doesn't toggle:
- Log `list(checkboxes.keys())` to see if "Other (ParentName)" keys exist
- Check if the issue is visual-only (Quasar not re-rendering) vs logical (value not being set)
- Try `ui.run_javascript('...')` to toggle checkboxes client-side instead of Python-side `set_value()`

## Data Context
- `fjms_enrichment.db` domains table: 390K rows, v2.0.0
- 25 parent categories, ~175 unique child domains = ~200 total items
- "Other" appears as child of 13 different parents (never standalone)
- After merge fix: no duplicate children within any parent
- Hierarchy query: `GROUP BY Domain, ParentDomain` → ~200 rows → Python dedup/merge → 1.15s cold

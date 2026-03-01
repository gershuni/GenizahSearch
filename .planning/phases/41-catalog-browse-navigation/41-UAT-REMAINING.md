# Catalog Browse UAT — Remaining Fixes

## Status: ALL DONE (marked complete 2026-03-01 by user)

### Issue 1: Dark mode — table is black text on white background
**File:** `web/pages/catalog_browse.py`
**Problem:** The Quasar table and the expanded detail row use hardcoded light colors:
- Line ~292: `bg-green-1` in the expanded detail div (hardcoded light green)
- The `ui.table` itself doesn't adapt to dark mode
- Hardcoded `text-gray-500`, `text-green-800` etc. throughout

**Fix approach:**
- Remove hardcoded `bg-green-1` from the Vue slot template — use Quasar dark-aware classes instead
- Change `style="border-left: 4px solid #2e7d32;"` to use a CSS variable or Quasar class
- Check how other pages (browse.py, search.py) handle dark mode for tables
- The header `text-green-800` should use a dark-mode-aware class
- Look at `web/main.py` around line 433 for `set_theme()` to understand the theming system
- grep for `dark:` Tailwind prefix usage in other web pages for the pattern

### Issue 2: Expanded detail row needs richer content
**File:** `web/pages/catalog_browse.py` — the `add_slot('body', ...)` Vue template
**Current:** Shows author, title, domain, date, description text + Browse button
**Requested additions:**

#### a) Catalog Records button
- Like the one in Browse by Shelfmark (`web/pages/browse.py`)
- grep for `catalog_records` or `_show_fjms_catalog` in browse.py to find the pattern
- Uses `fjms.get_catalog_records(sys_id)` from `shared/fjms_service.py`
- Shows a dialog/panel with FJMS catalog record details
- Since this is inside a Vue slot, the button needs to emit a NiceGUI event back to Python
- Alternative: render catalog info inline in the expanded row (simpler)

#### b) Thumbnail image
- Need to fetch a thumbnail for the sys_id
- Pattern: use NLI crossref or IIIF manifest to get an image URL
- Check `shared/nli_crossref_service.py` for `get_thumbnail_url` or similar
- Or use the same IIIF approach as browse.py image loading
- Could use `<img>` tag in the Vue template with a dynamically resolved URL
- Simpler: add a `_thumb_url` field to each row dict, resolved during `_resolve_all()`

#### c) Snippet from middle of manuscript
- Get text from the manuscript, but NOT page 1 (often not real text)
- Use `searcher.get_full_manuscript(sys_id)` which returns list of pages
- Pick page from middle: `pages[len(pages)//2]` or similar
- Truncate to ~200 chars for the snippet
- This needs to run in `io_bound` during result building (batch)
- Add as `_snippet` field to each row dict

**Implementation notes:**
- The detail row is a Vue template string (Quasar slot), so Python-side data must be
  passed as fields in the row dict (e.g., `_thumb_url`, `_snippet`, `_catalog_count`)
- The Browse button in the slot currently uses `window.location.href` — same pattern
  can be used for catalog records dialog, or render inline

## Already completed (this session)
- ✅ Translation fix: "Browse by Shelfmark" Hebrew
- ✅ Web: Header font matches browse.py
- ✅ Web: Shelfmark/library resolution (was using `service.mm` which doesn't exist, fixed to `state.meta_mgr`)
- ✅ Web: Table restored with inline expandable detail rows (Quasar slot)
- ✅ Web: Author/Work search via `ui.select(with_input=True)`
- ✅ Web: Hebrew domain chips show Hebrew names
- ✅ Web: Collapsible sidebar with toggle button
- ✅ Desktop: Sorting enabled on results table
- ✅ Desktop: Eye (👁) + Book (📖) action buttons per row with hover
- ✅ Desktop: Double-click → ResultDialog with prev/next
- ✅ Desktop: `_catalog_results_data` stored for ResultDialog access

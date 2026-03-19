# Quick Task 260319-mc4: Search UX Overhaul — Summary

**Completed:** 2026-03-19
**Commits:** a7abc6c9 → 6d5f5817 (12 commits)

## What Was Built

### 1. Hero Search Bar on Home Page
- Large, prominent search input below the "What is the Cairo Genizah?" card
- Hebrew translation: "חיפוש בכתבי יד..."
- Navigates to `/search?q=...` via `ui.navigate.to()`
- Search icon button triggers same navigation

### 2. Splitter Removed → Inline Accordion
- **Removed** the 35/65 splitter that wasted 65% of width on a viewer panel
- Results now take **100% width** of the content area
- Clicking a result card **expands an inline accordion** below it showing:
  - Left: manuscript thumbnail image (300px IIIF via server proxy)
  - Right: full page text with highlighting and line breaks
- Only one accordion open at a time; click again to collapse
- Accordion state preserved across enrichment rerenders

### 3. Result Card Restructure
- **On card:** badges + shelfmark + title + action buttons + snippet
- Action buttons: Browse, Quick View (renamed from Advanced View / מבט מהיר), Add to List, Catalog Records
- **Removed from cards:** Find Parallels, Exclude (not appropriate for this context)
- Cards are scannable; detailed content appears on expand

### 4. Citation Footer Auto-Collapse
- Full citation shows for 10 seconds, then fades to compact single line
- Compact line shows full citation text with CSS `text-overflow: ellipsis`
- Copy button copies the full citation regardless of display state
- Manual dismiss via X button persists in localStorage

### 5. Status Label Merged
- Removed duplicate "Search completed in X — N Results" / "Done. Found N results." lines
- Single merged display: "588 Results · 0:18" in results header
- Timer shows "Searching... · 0:11" during active search

### 6. Thumbnail Images
- Added `width` parameter to `/api/nli_image_by_sysid/` endpoint
- Accordion requests 300px thumbnails (~20x smaller than full 2000px images)
- Uses same fallback chain as Advanced View (server proxy → manifest → direct IIIF)
- Oxford manuscripts handled via dedicated image endpoints

### 7. Lazy Text Loading
- Fresh search: full text renders immediately with highlighting
- Session restore (navigate away and back): text lazy-loads on first accordion expand via `get_service().get_browse_page()`
- Cached in result dict after first load — no re-fetch on subsequent expands

### 8. NiceGUI Client Context Fix
- `_after_delay()` deferred tasks now capture `ui.context.client` at page creation
- Fixes homepage search, scroll collapse, and all deferred UI operations that need slot context

## Files Modified
- `web/pages/home.py` — hero search bar
- `web/pages/search.py` — accordion, card restructure, status merge, lazy loading, client context
- `web/main.py` — citation footer auto-collapse, header search
- `web/api.py` — thumbnail width parameter for IIIF proxy
- `web/static/common.css` — citation compact/full styles, accordion animation
- `genizah_translations.py` — "Search manuscripts...", "Quick View" translations

## Key Technical Decisions
- **Accordion over splitter**: Results-first UX; viewer appears on demand, not permanently
- **Server proxy for images**: Browser can access NLI IIIF directly, but server proxy provides caching + FL ID resolution from local sidecar
- **Lazy text loading**: Avoids persisting megabytes of full text in session storage
- **Client context capture**: NiceGUI's slot stack is per-asyncio-task; deferred callbacks need explicit client context

# GenizahSearch Desktop - Post-Extraction Smoke Test Checklist

**Purpose:** Verify desktop app functionality after v7.9 module decomposition (Phases 67-71).
**Version:** v7.9 (desktop/ extraction milestone)
**Baseline:** pytest 1067 passed, 8 skipped

---

## Legend
- [ ] - Not tested
- [x] - Passed
- [!] - Failed / Needs fix
- [?] - Needs additional testing
- [~] - Skipped (reason noted)

---

# 1. Application Startup

- [ ] App launches without crash: `python genizah_app.py`
- [ ] Main window visible with all tabs (Search, Browse, Composition, Lists, Discoveries)
- [ ] No error dialogs on startup
- [ ] Status bar shows version and manuscript count
- [ ] No console tracebacks during startup

# 2. Basic Search

*Exercises: genizah_app.py (search tab), desktop/result_dialog.py (ResultDialog)*

- [ ] Type a Hebrew query in the search field (e.g. "שלום")
- [ ] Execute search (Enter or button)
- [ ] Results appear in results table with shelfmarks, snippets, scores
- [ ] Click a result -- ResultDialog opens
- [ ] ResultDialog shows manuscript metadata, text, images
- [ ] Image loads in the ResultDialog viewer
- [ ] Close ResultDialog cleanly (no crash, no orphan windows)

# 3. Responsa / Tabular Query Builder

*Exercises: genizah_app.py (TabularQueryBuilderDialog -- remains in coordinator)*

- [ ] Switch to Responsa mode in search settings
- [ ] Open tabular query builder dialog
- [ ] Add a search term row, select field type
- [ ] Builder returns valid Responsa syntax to the search field
- [ ] Close builder dialog cleanly

# 4. Browse Navigation

*Exercises: desktop/viewers.py (ManuscriptViewerWidget, ZoomableScrollArea), desktop/image_loader.py (ImageLoaderThread)*

- [ ] Navigate to Browse tab
- [ ] Select a manuscript by shelfmark
- [ ] Manuscript images load in the viewer
- [ ] Page forward/backward using navigation controls
- [ ] Images update correctly on page change
- [ ] Extended info panel opens (metadata, transcription text)
- [ ] Image adjustment controls work (brightness/contrast sliders)
- [ ] Fullscreen image viewer opens and closes (FullscreenImageWindow)

# 5. Visual Similarity

*Exercises: desktop/vs_cache.py (DesktopVSCache, VSFetchThread, VSDownloadThread)*

> Note: If VS data is not available on your local machine, mark this section as [~] Skipped.

- [ ] Browse a manuscript with VS data available
- [ ] Open VS suggestions dialog from the browse view
- [ ] Suggestions display with partner thumbnails and metadata
- [ ] Click a suggestion -- navigates to partner manuscript
- [ ] Close VS dialog cleanly

# 6. Settings, Help & About Dialogs

*Exercises: genizah_app.py (SettingsDialog, HelpDialog, WhatsNewDialog -- remain in coordinator)*

- [ ] Settings dialog opens from menu/toolbar
- [ ] Settings changes are applied (e.g. language toggle, font size)
- [ ] Settings dialog closes cleanly
- [ ] Help dialog opens from menu/toolbar
- [ ] Help content renders correctly (Hebrew + English sections)
- [ ] Help dialog closes cleanly
- [ ] About / What's New dialog opens
- [ ] Version number and changelog display correctly
- [ ] About dialog closes cleanly

# 7. List Management

*Exercises: genizah_app.py (Lists tab, ListsTreeWidget)*

- [ ] Lists tab shows existing lists (or empty state)
- [ ] Create a new list
- [ ] Add a manuscript to the list (from search results or browse)
- [ ] Rename the list
- [ ] Delete the list
- [ ] Confirm deletion removes the list from the tree

# 8. Puzzle Window

*Exercises: desktop/puzzle.py (PuzzleCanvasWindow, PuzzleFragment, PuzzleToolbar, PuzzleLayerPanel, PuzzleFragmentSelector)*

- [ ] Open puzzle window via toolbar or menu
- [ ] Add a fragment by shelfmark using the fragment selector
- [ ] Fragment image loads on the canvas
- [ ] Drag fragment to reposition
- [ ] Rotate fragment using rotation controls
- [ ] Flip fragment (horizontal/vertical)
- [ ] Zoom in/out on canvas
- [ ] Layer ordering controls work (bring to front / send to back)
- [ ] Close puzzle window cleanly

# 9. Filter Dialogs

*Exercises: desktop/dialogs_filter.py (PreSearchFilterDialog, DomainFilterDialog, ExcludeDialog)*

- [ ] Open pre-search filter dialog before executing a search
- [ ] Filter options display correctly (domain, material, date range)
- [ ] Apply a filter and verify it affects search results
- [ ] Open domain filter dialog from search results
- [ ] Domain categories display with counts
- [ ] Apply domain filter and verify results narrow
- [ ] Open exclude dialog
- [ ] Exclude sources display (lists, pasted shelfmarks, imported files)
- [ ] Close all filter dialogs cleanly

# 10. Scholarly Dialogs

*Exercises: desktop/dialogs_scholarly.py (BibliographyDialog, CatalogDialog, MeasurementsDialog, CatalogRefsDialog)*

- [ ] Browse to a manuscript with FJMS enrichment data
- [ ] Open bibliography dialog -- references display correctly
- [ ] Open catalog dialog -- catalog description and metadata display
- [ ] Open measurements dialog -- physical dimensions display
- [ ] Open catalog references dialog -- cross-references display
- [ ] Close all scholarly dialogs cleanly

---

# Architecture After v7.9

The desktop app has been decomposed from a monolithic `genizah_app.py` into focused modules under `desktop/`. The import graph is strictly one-directional (no cycles).

| Module | Lines | Purpose |
|--------|------:|---------|
| `genizah_app.py` | 22,541 | GenizahGUI orchestrator, app chrome, small coordination helpers |
| `desktop/result_dialog.py` | 2,818 | ResultDialog and related UI |
| `desktop/puzzle.py` | 2,668 | PuzzleCanvasWindow and 4 supporting classes |
| `desktop/dialogs_filter.py` | 1,658 | PreSearchFilterDialog, DomainFilterDialog, ExcludeDialog |
| `desktop/dialogs_scholarly.py` | 1,311 | BibliographyDialog, CatalogDialog, MeasurementsDialog, CatalogRefsDialog |
| `desktop/viewers.py` | 1,228 | ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget |
| `desktop/vs_cache.py` | 215 | DesktopVSCache, VSFetchThread, VSDownloadThread |
| `desktop/widgets.py` | 183 | ShelfmarkCompleter, ActionsHoverWidget, shared helpers |
| `desktop/title_helpers.py` | 168 | Title formatting utilities |
| `desktop/image_loader.py` | 135 | ImageLoaderThread |
| **Total desktop/** | **10,384** | |

**Re-exports in `genizah_app.py`** are intentional back-compat shims (per D-10/D-11). All extracted classes are re-exported at their original import paths so that any external code referencing `from genizah_app import ResultDialog` (etc.) continues to work. These re-exports are removable after an external import audit confirms no consumers.

**Import graph:** All `desktop/` modules import only from `genizah_core`, `shared/`, `gui_threads`, standard library, and other `desktop/` modules. Zero back-edges to `genizah_app.py`.

---

*Checklist created: 2026-04-16*
*Phase 67-71 desktop extraction milestone (v7.9)*

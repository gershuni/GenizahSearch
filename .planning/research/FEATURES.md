# Feature Landscape: Desktop PGP Parity & Transcription Search

**Domain:** PyQt6 desktop app — bringing PGP features from web to desktop
**Researched:** February 7, 2026
**Confidence Level:** HIGH (based on direct analysis of existing web and desktop codebases, not external research)

---

## Executive Summary

GenizahSearch's web app (NiceGUI) already implements the full PGP feature set: transcription display, multi-source version selector, metadata panel, tag-based search, joins panel, and search-result transcription indicators. The desktop app (PyQt6, ~15,800 lines in `genizah_app.py`) has mature community features (corrections, versions, joins, comments) but zero PGP integration.

This research maps every web PGP feature to its desktop equivalent, identifies desktop-specific UX improvements, and categorizes by priority.

**Key constraint:** The web's `document_service.py` currently imports from `web.supabase_client` and cannot be used by the desktop app without extraction to a shared location. This was already identified in project memory as the "shared service layer" decision (Option C).

---

## Table Stakes

Features that must exist for desktop parity. Without these, researchers using the desktop app get a degraded experience compared to the web.

### TS-1: PGP Transcription Display in Manuscript Viewer

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `browse.py` lines 883-950: fetches PGP doc, gets section for page, displays in transcription panel |
| **What it does** | When viewing a manuscript page, show PGP transcription content instead of or alongside V0.8 text |
| **Desktop target** | `ResultDialog.text_ms` (QTextBrowser) and `browse_text` (QTextEdit) in browse tab |
| **Complexity** | Medium |
| **Desktop UX notes** | Text already displays in QTextBrowser with RTL Hebrew. PGP text replaces V0.8 as default when available. Needs visual indicator (green label or badge) showing "PGP" source. |
| **Dependencies** | Shared document_service (TS-7) |

**Implementation approach:** In `ResultDialog.load_versions()` and `browse_tab._browse_load_versions()`, after loading the page text, call `get_document_for_fragment(sys_id, page_num)`. If a PGP edition exists, add it to the version combo as the first entry and auto-select it.

### TS-2: Multi-Source Version Selector (Editions + Translations)

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `web/components/version_selector.py`: dropdown menu with PGP editions grouped by scholar, translations grouped by language |
| **What it does** | Shows all available editions (by scholar) and translations (by language) for a document, plus V0.8 and user corrections |
| **Desktop target** | `rd_version_combo` (QComboBox) in ResultDialog, `browse_version_combo` in browse tab |
| **Complexity** | Medium |
| **Desktop UX notes** | QComboBox with grouped items (using separators). Structure: PGP Edition (Scholar A), PGP Edition (Scholar B), --- separator ---, Hebrew Translation (Scholar C), English Translation (Scholar D), --- separator ---, V0.8, User Corrections. Use `QComboBox.insertSeparator()` for visual grouping. |
| **Dependencies** | Shared document_service (TS-7) |

**Implementation approach:** Extend existing `_rd_refresh_versions()` and `_browse_load_versions()` methods. Before adding V0.8 and user corrections, query `get_all_sources_for_fragment(sys_id)` and add PGP sources to the combo with appropriate labels. Group with separators. When PGP editions exist, auto-select the first edition as default (matching web behavior).

**Desktop-specific improvement:** QComboBox supports `setItemData()` with custom roles, enabling richer tooltips. Show scholar name, source type, and content length in tooltip. This is better than the web's flat menu items.

### TS-3: PGP Metadata Display

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `browse.py` lines 1753-1818: PGP metadata section in sidebar showing document_type, tags, description, dates |
| **What it does** | Shows PGP-sourced metadata: document type, primary/secondary languages, tags, description, date fields, link to PGP website |
| **Desktop target** | New section in `ResultDialog` extended info panel, or new collapsible group in browse tab |
| **Complexity** | Medium |
| **Desktop UX notes** | Use QGroupBox("Princeton Geniza Project") with a green header/border. Inside: QFormLayout with document type, tags as clickable labels, description as QLabel with word wrap, dates, and PGP URL as clickable link. Tags should open tag search (see TS-5). Keep collapsible to not overwhelm the viewer. |
| **Dependencies** | Shared document_service (TS-7) |

**Implementation approach:** Add a collapsible `QGroupBox` below the existing extended info section. Populate with `get_document_metadata(pgpid)`. Style with green border to visually distinguish PGP data from Ktiv/NLI metadata.

### TS-4: Search Result Transcription Indicators

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `search.py` lines 1212-1217: green icon next to shelfmark when sys_id has PGP transcription |
| **What it does** | Batch-checks search results against `document_fragments` table to show which results have PGP transcriptions |
| **Desktop target** | `QTableWidget` in search tab, add icon or colored indicator to result rows |
| **Complexity** | Low-Medium |
| **Desktop UX notes** | Add a new narrow column (20px) in the results table for a green circle/icon. Use `get_sys_ids_with_transcriptions()` after search completes to batch-mark rows. Icon: QIcon or colored QLabel. Alternatively, color the shelfmark text green for PGP-linked results. Prefer icon column -- it is scannable and does not interfere with text selection. |
| **Dependencies** | Shared document_service (TS-7) |

**Implementation approach:** After search results populate the table, collect all sys_ids, call `get_sys_ids_with_transcriptions()` in a background thread (to not freeze the UI), then update the indicator column. Use a QTableWidgetItem with a small green dot icon.

### TS-5: Tag-Based Search

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `search.py` lines 2310-2404 and `browse.py` line 1790: clickable tag badges navigate to `/search?tag=X`, which calls `get_fragments_by_tag(tag)` |
| **What it does** | Search for all fragments linked to PGP documents with a specific tag (e.g., "marriage", "commercial", "letter") |
| **Desktop target** | New search mode or filter in search tab; also, clickable tag labels in PGP metadata (TS-3) |
| **Complexity** | Medium |
| **Desktop UX notes** | Two entry points: (1) clicking a tag in the PGP metadata panel runs a tag search and displays results in the search results table, (2) a dedicated "Tag" search mode in the mode combo (alongside Exact, Variants, Fuzzy, etc.). Results display like normal search results but with document_type and description columns from PGP data. |
| **Dependencies** | Shared document_service (TS-7), PGP metadata display (TS-3) |

**Implementation approach:** Add "Tag (#)" mode to `mode_combo`. When searching in tag mode, call `get_fragments_by_tag(query)` instead of `SearchEngine.search()`. Format results into the same table structure. Tag clicks in the PGP metadata group box switch to search tab and trigger a tag search.

### TS-6: Related Fragments / Joins Panel (PGP Source)

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `web/components/joins_panel.py`: merges user-created joins with PGP multi-fragment document joins, shows Related Fragments panel |
| **What it does** | Shows all fragments that belong to the same PGP document (multi-fragment compositions). Currently, the desktop's `JoinsDialog` only shows user-created joins via the corrections client -- it does not query PGP document_fragments. |
| **Desktop target** | Existing `JoinsDialog` in `corrections_ui.py`, existing `btn_joins` in ResultDialog and browse tab |
| **Complexity** | Medium |
| **Desktop UX notes** | The desktop `JoinsDialog` already has the right UI structure (list of connected fragments with navigate/browse callbacks). It just needs to also query `get_fragments_for_document(pgpid)` and merge those results with user joins. PGP-sourced joins should be labeled with a "PGP" badge and should not have a delete button. |
| **Dependencies** | Shared document_service (TS-7) |

**Implementation approach:** Modify `JoinsDialog.__init__()` to accept an optional `pgpid` parameter. In the data loading method, query `get_fragments_for_document(pgpid)` and merge results with user joins. Add "PGP" badge for PGP-sourced joins. The "View All Fragments" functionality (showing all fragment images side-by-side) maps well to a `QScrollArea` with image thumbnails.

### TS-7: Shared Document Service Layer

| Aspect | Detail |
|--------|--------|
| **Web equivalent** | `web/document_service.py` -- 508 lines of PGP data access functions |
| **What it does** | Provides functions for accessing PGP data from Supabase: `get_document_for_fragment()`, `get_fragments_for_document()`, `get_all_sources_for_fragment()`, `get_sys_ids_with_transcriptions()`, `get_fragments_by_tag()`, etc. |
| **Desktop target** | Extract to shared location (e.g., `shared/document_service.py` or `document_service.py` at root) |
| **Complexity** | Low-Medium |
| **Desktop UX notes** | Not a UI feature, but a prerequisite for all other features. Currently imports `from web.supabase_client import get_client` which ties it to the web app. Desktop uses `supabase_corrections_client.py` with its own Supabase client. Need to either: (a) extract `get_client()` to shared module, or (b) make document_service accept a client parameter. |
| **Dependencies** | None -- this must be done first |

**Implementation approach (recommended):** Copy `web/document_service.py` to root level as `document_service.py`. Modify it to use a shared `get_client()` that works for both web and desktop. The desktop already has Supabase client setup in `supabase_corrections_client.py` -- factor out the client initialization to a shared module. Alternatively, add a `set_client()` function to document_service that lets the desktop inject its own Supabase client instance.

---

## Differentiators

Features where the desktop can improve on the web experience, leveraging PyQt6's native capabilities.

### D-1: Persistent PGP Info Panel (QDockWidget)

| Aspect | Detail |
|--------|--------|
| **What it does** | Dockable, always-visible PGP metadata panel that persists as you navigate between manuscripts. On the web, metadata is in a collapsible sidebar that resets on every page load. |
| **Desktop advantage** | QDockWidget can be floated, docked to any edge, or tabbed with other dock widgets. Researchers can position PGP metadata wherever they want and it stays. |
| **Complexity** | Medium |
| **Value** | High for intensive research workflows -- researchers can keep metadata visible while browsing through many manuscripts |

**Implementation approach:** Create a `PGPInfoDockWidget(QDockWidget)` that displays document type, tags, description, dates, PGP link, and related fragments. Update its content whenever the active manuscript changes. Allow docking to left, right, or floating. Save dock position in app config.

### D-2: Keyboard-Driven Version Switching

| Aspect | Detail |
|--------|--------|
| **What it does** | Switch between PGP edition, V0.8, and user corrections using keyboard shortcuts (e.g., Ctrl+1 for PGP, Ctrl+2 for V0.8, Ctrl+3 for next user correction) |
| **Desktop advantage** | Native keyboard shortcut support. Web requires JavaScript hacks for keyboard shortcuts. Desktop can use QAction with QShortcut natively. |
| **Complexity** | Low |
| **Value** | High for researchers comparing transcription versions side-by-side with manuscript images |

**Implementation approach:** Add QAction shortcuts in ResultDialog and browse tab. Ctrl+Shift+P for PGP, Ctrl+Shift+O for original (V0.8), Ctrl+Shift+1/2/3 for user corrections by index.

### D-3: Transcription Search with Filter Toggles

| Aspect | Detail |
|--------|--------|
| **What it does** | Filter search results to show only manuscripts with PGP transcriptions, or filter by document type, date range, or tags |
| **Desktop advantage** | Persistent filter panel (QGroupBox with QCheckBox toggles) in the search tab. Filters remain active across searches. Web does not currently have this. |
| **Complexity** | Medium |
| **Value** | High -- researchers often want to search only within transcribed texts, or only within legal documents, etc. |

**Implementation approach:** Add a collapsible "Filters" QGroupBox below the search controls:
- `QCheckBox("Has PGP Transcription")` -- post-filter results using `get_sys_ids_with_transcriptions()`
- `QComboBox` for document type filter (Legal, Letter, Commercial, etc.) -- requires querying PGP documents table
- Date range filter (QSpinBox for start/end year) -- requires PGP date fields
- Tag filter (QLineEdit with autocomplete from known tags)

Filter UI pattern: checkboxes for boolean filters, combo boxes for categorical, spin boxes for ranges. All wrapped in a collapsible group that remembers its state.

### D-4: Side-by-Side Edition Comparison

| Aspect | Detail |
|--------|--------|
| **What it does** | Show two transcription versions side-by-side with diff highlighting (e.g., PGP edition vs V0.8, or Scholar A vs Scholar B) |
| **Desktop advantage** | QSplitter with two QTextBrowser panels. Diff highlighting using QTextCharFormat with background colors. Web would need complex DOM manipulation. |
| **Complexity** | Medium-High |
| **Value** | Medium -- useful for scholars verifying transcription accuracy, but not needed by all users |

**Implementation approach:** Add a "Compare" button in the version selector area. Opens a QDialog or uses QSplitter to show two texts side-by-side. Use `difflib.SequenceMatcher` to compute diffs at the word level. Highlight additions in green, deletions in red, changes in yellow using `QTextCharFormat.setBackground()`.

### D-5: Offline PGP Data Cache

| Aspect | Detail |
|--------|--------|
| **What it does** | Cache PGP transcriptions, metadata, and joins data locally so the desktop app works without internet |
| **Desktop advantage** | Desktop apps are expected to work offline. Web cannot do this (depends on server). SQLite cache or JSON files on disk. |
| **Complexity** | Medium-High |
| **Value** | Medium -- researchers sometimes work without internet (travel, archives, conferences) |

**Implementation approach:** Add SQLite cache in `~/.genizahsearch/pgp_cache.db`. Cache `documents`, `document_sources`, and `document_fragments` tables. Populate on first fetch, refresh on demand or time-based expiry. Cache hit path: check local DB first, fall back to Supabase on miss.

### D-6: Tag Cloud / Tag Browser

| Aspect | Detail |
|--------|--------|
| **What it does** | Show all available PGP tags with frequency counts in a browsable list or cloud visualization |
| **Desktop advantage** | QListWidget or custom QWidget with frequency-weighted display. Click a tag to search. Web has no tag browsing -- only click-through from individual manuscripts. |
| **Complexity** | Low-Medium |
| **Value** | Medium -- helps researchers discover content they did not know existed |

**Implementation approach:** Query all distinct tags from documents table (can be cached). Display in a QListWidget sorted by frequency, with count shown next to each tag. Click triggers tag search (TS-5). Could be integrated into the PGP Info dock widget (D-1).

---

## Anti-Features

Things to deliberately NOT build differently for desktop, or to explicitly avoid.

### AF-1: Do NOT Copy NiceGUI Component Structure

| What | Why Avoid |
|------|-----------|
| Replicating NiceGUI's reactive component model (like `version_selector.create_version_selector()`) as a PyQt6 pattern | NiceGUI is declarative/reactive; PyQt6 is signal-slot imperative. Trying to create "components" that build UI on the fly leads to memory leaks and layout bugs in Qt. |
| **What to do instead** | Use traditional PyQt6 patterns: subclass QWidget or QDialog for reusable UI pieces. Connect signals/slots. Update widget state, do not rebuild widgets. |

### AF-2: Do NOT Build a Separate "PGP Mode"

| What | Why Avoid |
|------|-----------|
| Creating a separate tab or mode just for PGP features | Fragments the user experience. Researchers should see PGP data integrated into the existing search, browse, and viewer workflows. |
| **What to do instead** | Integrate PGP features into existing tabs: PGP transcriptions appear in the version selector (which already exists), PGP metadata appears as an additional info section, tag search appears as a new search mode, transcription indicators appear in search results. |

### AF-3: Do NOT Add Web-Style Dialog Chains for Version Selection

| What | Why Avoid |
|------|-----------|
| Opening a dialog with tabs and sub-menus to select a transcription version (like the web's nested menu with sections for editions, translations, corrections) | Desktop users expect a combobox dropdown, not cascading dialogs. The existing `rd_version_combo` pattern is correct -- just extend it with PGP entries. |
| **What to do instead** | Use the existing QComboBox with separators to group PGP editions, translations, and user corrections. This is faster and more native-feeling than a dialog. |

### AF-4: Do NOT Rebuild Image Proxy Logic

| What | Why Avoid |
|------|-----------|
| Recreating the web's `/api/nli_image_by_sysid/` or `/api/oxford_image/` proxy endpoints for the desktop | The desktop already has `ManuscriptViewerWidget` with direct IIIF and external image loading. It does not need a server proxy. |
| **What to do instead** | Reuse existing `ManuscriptViewerWidget.load_images()` for fragment image display. For "View All Fragments" mode, create thumbnail views using the same image loading infrastructure. |

### AF-5: Do NOT Over-Invest in Full-Text Transcription Search (Phase 1)

| What | Why Avoid |
|------|-----------|
| Building full-text search across all PGP transcription content in the desktop app | This requires either a Supabase full-text search (pg_trgm, GIN indexes) or a local Tantivy index of PGP content. Both are significant infrastructure. Tag search and transcription indicators provide 80% of the discovery value at 20% of the cost. |
| **What to do instead** | Phase 1: tag search + transcription indicators. Phase 2 (if demand exists): full-text search within PGP transcriptions, possibly by extending the Tantivy index to include PGP text. |

---

## Feature Dependencies

```
TS-7 (Shared Service Layer)
  |
  +---> TS-1 (Transcription Display)
  |       |
  |       +---> TS-2 (Multi-Source Version Selector)
  |               |
  |               +---> D-2 (Keyboard Version Switching)
  |               +---> D-4 (Side-by-Side Comparison)
  |
  +---> TS-3 (PGP Metadata Display)
  |       |
  |       +---> TS-5 (Tag-Based Search)
  |       +---> D-1 (Dockable PGP Panel)
  |       +---> D-6 (Tag Cloud)
  |
  +---> TS-4 (Search Result Indicators)
  |       |
  |       +---> D-3 (Filter Toggles)
  |
  +---> TS-6 (Related Fragments / Joins)
  |
  +---> D-5 (Offline Cache) -- can be added at any time
```

---

## MVP Recommendation

For the minimum viable milestone, prioritize in this order:

### Phase 1: Foundation + Core Display
1. **TS-7** (Shared Document Service) -- prerequisite, do first
2. **TS-1** (PGP Transcription Display) -- highest user value
3. **TS-2** (Multi-Source Version Selector) -- extends existing pattern

### Phase 2: Discovery + Navigation
4. **TS-4** (Search Result Indicators) -- low effort, high visibility
5. **TS-3** (PGP Metadata Display) -- enriches the viewer
6. **TS-6** (Related Fragments / Joins) -- extends existing JoinsDialog

### Phase 3: Search + Desktop Polish
7. **TS-5** (Tag-Based Search) -- new search mode
8. **D-2** (Keyboard Version Switching) -- low effort polish
9. **D-3** (Filter Toggles) -- desktop differentiation

### Defer to Post-Milestone
- **D-1** (Dockable PGP Panel) -- nice to have, not critical
- **D-4** (Side-by-Side Comparison) -- niche use case
- **D-5** (Offline Cache) -- infrastructure investment
- **D-6** (Tag Cloud) -- discovery feature, not core workflow
- Full-text transcription search (see AF-5)

---

## Complexity Estimates

| Feature | Code Changes | Estimated Effort | Risk |
|---------|-------------|-----------------|------|
| TS-7 (Shared Service) | Extract document_service, modify imports | 2-3 hours | Low -- mechanical refactoring |
| TS-1 (Transcription Display) | Modify ResultDialog + browse tab load methods | 3-4 hours | Low -- extends existing patterns |
| TS-2 (Version Selector) | Extend version combo population | 3-4 hours | Low -- existing combo infrastructure |
| TS-3 (PGP Metadata) | New QGroupBox in viewer | 3-4 hours | Low -- straightforward UI |
| TS-4 (Search Indicators) | Background thread + table column | 2-3 hours | Low -- batch API + UI update |
| TS-5 (Tag Search) | New search mode + results formatting | 4-5 hours | Medium -- new search path |
| TS-6 (Related Fragments) | Extend JoinsDialog | 3-4 hours | Low -- well-scoped extension |
| D-2 (Keyboard Shortcuts) | QAction + QShortcut | 1 hour | Low |
| D-3 (Filter Toggles) | New QGroupBox with checkboxes | 4-5 hours | Medium -- post-filter logic |

**Total estimated effort for all table stakes (TS-1 through TS-7):** 20-27 hours
**Total for MVP (TS + top differentiators D-2, D-3):** 25-33 hours

---

## Web Feature Parity Matrix

Complete mapping of every PGP feature in the web app to its desktop status and plan.

| Web Feature | Web Location | Desktop Status | Plan |
|-------------|-------------|----------------|------|
| PGP transcription as default version | browse.py:883-950 | Not implemented | TS-1 |
| Multi-source edition selector | version_selector.py:86-362 | Not implemented | TS-2 |
| Translation selector (Hebrew/English) | version_selector.py:246-286 | Not implemented | TS-2 |
| Scholar attribution display | version_selector.py:139-141 | Not implemented | TS-2 |
| PGP metadata in sidebar | browse.py:1753-1818 | Not implemented | TS-3 |
| Clickable tag badges | browse.py:1787-1790 | Not implemented | TS-3 + TS-5 |
| Tag-based search (via URL) | search.py:2310-2404 | Not implemented | TS-5 |
| Search result PGP indicator | search.py:1212-1217 | Not implemented | TS-4 |
| PGP joins in Related Fragments | joins_panel.py:105-168 | Not implemented | TS-6 |
| View All Fragments mode | joins_panel.py:519-548 | Not implemented | TS-6 |
| PGP link button in header | browse.py:1616-1624 | Not implemented | TS-3 |
| Date display (original + inferred) | browse.py:1799-1818 | Not implemented | TS-3 |
| Document description display | browse.py:1792-1797 | Not implemented | TS-3 |
| V0.8 original text | browse.py, search.py | Already exists | Keep |
| User corrections (version combo) | genizah_app.py:2416-2424 | Already exists | Extend (TS-2) |
| User-created joins | corrections_ui.py:JoinsDialog | Already exists | Extend (TS-6) |
| Manuscript image viewer | genizah_app.py:ManuscriptViewerWidget | Already exists | Reuse |
| Recto/verso page navigation | genizah_app.py:ResultDialog | Already exists | Keep |

---

## Desktop-Specific UX Considerations

### Information Density
Desktop researchers prefer seeing more data at once. The web's modal/dialog-heavy approach (joins dialog, version menu) should be replaced with persistent panels where possible. The QDockWidget pattern (D-1) serves this need, but even without it, the existing splitter-based layout allows more information density than the web.

### Keyboard Navigation
The desktop app should support keyboard navigation for common PGP workflows:
- Tab/Shift+Tab between transcription and metadata panels
- Ctrl+Shift+P to switch to PGP transcription
- Enter on a tag to trigger tag search
- Arrow keys to navigate between search results (already works)

### State Persistence
Desktop apps should remember user preferences:
- Which version was selected last (PGP vs V0.8)
- Whether PGP metadata panel is expanded or collapsed
- Filter toggle states (has transcription, document type)
- Dock widget positions

### Threading
Desktop must not freeze the UI during Supabase queries. All PGP data fetches should use `QThread` or `threading.Thread` with signal-based result delivery. The existing `EnrichMetadataThread` pattern in `gui_threads.py` provides the correct model for this.

---

## Sources

- Direct codebase analysis of `web/document_service.py` (508 lines, 12 functions)
- Direct codebase analysis of `web/components/version_selector.py` (376 lines)
- Direct codebase analysis of `web/components/joins_panel.py` (884 lines)
- Direct codebase analysis of `web/pages/browse.py` (PGP metadata display, transcription loading)
- Direct codebase analysis of `web/pages/search.py` (tag search, transcription indicators)
- Direct codebase analysis of `genizah_app.py` (15,800 lines, desktop app)
- Direct codebase analysis of `corrections_ui.py` (JoinsDialog, version combo patterns)
- Project memory: shared service layer decision (Option C)
- [Qt QDockWidget documentation](https://doc.qt.io/qt-6/qdockwidget.html)
- [Qt QSplitter documentation](https://doc.qt.io/qtforpython-6/PySide6/QtWidgets/QSplitter.html)
- [Qt Document Viewer Example](https://doc.qt.io/qtforpython-6/examples/example_demos_documentviewer.html)

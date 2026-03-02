# Phase 44: Quick UX Wins - Context

**Gathered:** 2026-03-02
**Status:** Ready for planning

<domain>
## Phase Boundary

Batch of four independent, high-value UX improvements across both apps: desktop search completion notifications, OS sleep prevention during search, Hebrew library names, and copy from compact desktop results. Each is a self-contained feature with no cross-dependencies.

</domain>

<decisions>
## Implementation Decisions

### Desktop Search Notification
- Windows toast notification when a search completes
- Only triggers when the app is **not focused** (user switched to another window)
- Shows **result count + search term** (e.g. "GenizahSearch — 42 results for 'אברהם'")
- Notification can be **disabled in settings** (toggle in desktop settings dialog)
- Applies to all search types: regular, Responsa, composition/parallels, PGP tag search
- Sound: Claude's discretion (default Windows notification sound or silent)

### Sleep Prevention
- Prevent OS sleep while **any search type** is running (regular, Responsa, composition, PGP tags)
- Desktop only — uses Win32 `SetThreadExecutionState` with `ES_SYSTEM_REQUIRED | ES_CONTINUOUS`
- **Prevent sleep only** — screen may dim/turn off per user power settings (no `ES_DISPLAY_REQUIRED`)
- Must reliably clear the flag when search completes, is cancelled, or errors out
- Visibility indicator: Claude's discretion (status bar hint or silent)

### Hebrew Library Names
- **Replace English with Hebrew** when app is in Hebrew mode
- **English names as today** when app is in English mode (no change to English mode behavior)
- Applies **everywhere libraries are shown**: search results, browse pages, lists, exports, reading desk, detail panels — both apps
- Exports (Excel/Word/CSV) **follow app language** — Hebrew names in Hebrew mode, English in English mode
- **Translate all ~90 libraries** in `LIBRARY_CODES`
- Source strategy: check **FIST.db and NLI crossref** for existing Hebrew names first, then manually translate all remaining libraries
- English fallback only if a library code has no entry at all (shouldn't happen after full translation)

### Copy from Compact Results
- **Desktop only** — no web changes needed
- Right-click context menu on result table rows with copy options (Copy Shelfmark, Copy Title, Copy Row, etc.)
- Extends the existing right-click context menu on `results_table`
- User wanted mouse-drag text selection, but QTableWidget constraints make right-click menu the practical approach

### Claude's Discretion
- Notification sound (default Windows ding vs. silent)
- Sleep prevention status bar indicator (show small hint vs. silent)
- Exact copy menu item labels and format of copied text
- Settings UI placement for notification toggle (existing settings dialog section)

</decisions>

<specifics>
## Specific Ideas

- Hebrew library names may already exist in FIST.db (institution records) or NLI's online database — researcher should check these sources before manual translation
- The user's original request (ט) was specifically about seeing Hebrew library names — this is a localization priority
- The copy feature (יז) addresses researchers who need to quickly reference shelfmarks without opening the full reading desk view
- Notification feature (ה) is for the common workflow where a researcher starts a long composition search and switches to other work

</specifics>

<code_context>
## Existing Code Insights

### Reusable Assets
- `genizah_core.LIBRARY_CODES` (dict, ~90 entries): Central English name mapping — extend with parallel Hebrew dict or add Hebrew field
- `genizah_core.get_library_display(code, short)`: Display function — can be extended with language parameter
- `genizah_translations.py`: Existing Hebrew translation system with `tr()` function
- Desktop right-click context menu on `results_table` (genizah_app.py:~8264): Already has context menu infrastructure
- `on_search_finished()` (genizah_app.py:17312): Search completion handler — notification trigger point
- `on_comp_search_finished()` (genizah_app.py:~19840): Composition search completion — second notification trigger point
- `gui_threads.py:SearchThread`: Search thread class — sleep prevention start/stop points

### Established Patterns
- Translation system: `tr('English string')` → Hebrew lookup in `genizah_translations.py`
- Status bar messages: `self.statusBar().showMessage()` used throughout for transient info
- Settings: Desktop settings stored in `QSettings` / config system
- Notification bar: `NotificationBar` class exists (genizah_app.py:239) — in-app notifications, but not OS-level

### Integration Points
- Sleep prevention: wrap `SearchThread.run()` start/end with `SetThreadExecutionState` calls via ctypes
- Toast notification: `on_search_finished` / `on_comp_search_finished` — check `QApplication.activeWindow()` for focus state
- Hebrew library names: `get_library_display()` is the single point to modify — all callers benefit
- Copy menu: extend existing `results_table` context menu handler (~genizah_app.py:8264)

</code_context>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 44-quick-ux-wins*
*Context gathered: 2026-03-02*

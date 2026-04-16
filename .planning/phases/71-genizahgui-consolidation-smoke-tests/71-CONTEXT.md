# Phase 71: GenizahGUI Consolidation & Smoke Tests - Context

**Gathered:** 2026-04-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Final consolidation phase for v7.9 desktop decomposition. Clean up the last architectural back-edge, create a committed desktop smoke-test checklist, verify no import cycles, and confirm the cumulative extraction result is sound.

In scope:
- **`desktop/vs_cache.py`** (new): Extract `DesktopVSCache`, `VSFetchThread`, `VSDownloadThread` from `genizah_app.py` (~120 lines) — kills the last `desktop/ → genizah_app` back-edge
- **`desktop/result_dialog.py`**: Retarget `from genizah_app import DesktopVSCache` to `from desktop.vs_cache import DesktopVSCache`
- **`genizah_app.py`**: Add re-export for DesktopVSCache trio
- **`docs/desktop-smoke-checklist.md`** (new): Committed checklist covering all major desktop code paths post-extraction
- **Import cycle verification**: Confirm zero cycles across all `desktop/` modules
- **Re-export documentation**: Document existing re-exports as intentional back-compat shims

Out of scope:
- Further class extraction (the 18 remaining classes are "small coordination helpers" per SC-2)
- TabularQueryBuilderDialog extraction (614 lines, self-contained, single call site at genizah_app.py:15265 — best candidate for a future cleanup phase)
- Protocol/ABC narrowing of PuzzleCanvasWindow.self.app or other parent surfaces (deferred — see Deferred Ideas)
- Re-export removal (keeping all re-exports for back-compat — see D-08)
- PyQt automation harness for smoke testing

</domain>

<decisions>
## Implementation Decisions

### DesktopVSCache Back-Edge Fix
- **D-01:** Move `DesktopVSCache` (genizah_app.py:2452, ~70 lines), `VSFetchThread` (genizah_app.py:2548, ~21 lines), `VSDownloadThread` (genizah_app.py:2569, ~90 lines) to new **`desktop/vs_cache.py`**. This is the only remaining `desktop/ → genizah_app` import edge (`desktop/result_dialog.py:645`).
- **D-02:** `desktop/result_dialog.py:645` retargets: `from genizah_app import DesktopVSCache` → `from desktop.vs_cache import DesktopVSCache`. Stays function-local (same pattern as Phases 68/69).
- **D-03:** `genizah_app.py` adds re-export: `from desktop.vs_cache import DesktopVSCache, VSFetchThread, VSDownloadThread  # noqa: F401`. Same pattern as prior phases.
- **D-04:** Module name exactly `vs_cache` (lowercase, underscore).

### No Further Extraction
- **D-05:** The 18 remaining pre-GenizahGUI classes (2,538 lines total) stay in `genizah_app.py`. SC-2 explicitly allows "small coordination helpers and thin wrappers." These classes are: app chrome (UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog), settings/help (SearchSettingsDialog, SettingsDialog, HelpDialog, LabScoringDialog), small widgets (ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget), Lab (LabPanel), catalog (_CatalogRefreshWorker), search builder (TabularQueryBuilderDialog).
- **D-06:** `TabularQueryBuilderDialog` (614 lines) is the only sizable remaining class. It's self-contained with a single call site. Best candidate for a future cleanup phase, not Phase 71.

### Smoke Test Approach
- **D-07:** Create **`docs/desktop-smoke-checklist.md`** — a committed manual checklist, NOT a PyQt automation script. PyQt event loop automation is fragile and not worth the maintenance cost.
- **D-08:** Checklist derived from the existing `docs/archive/PRE_LAUNCH_CHECKLIST.md` pattern, focused on post-extraction code paths:
  1. App launches without crash
  2. Basic search executes and returns results
  3. Responsa tabular builder opens and returns syntax
  4. ResultDialog opens from search results, closes cleanly
  5. Visual similarity fetch/cache path works (browse a manuscript, open VS dialog)
  6. Settings, Help, and About dialogs open
  7. Browse navigation changes pages
  8. List management basics work (create/rename/delete list)
  9. Puzzle window opens, loads a fragment, flip/rotate
  10. All filter dialogs open (PreSearch, Domain, Exclude)
- **D-09:** The smoke checklist is the DESK-07 deliverable. After user runs through it and confirms, DESK-07 is satisfied.

### Re-export Strategy
- **D-10:** Keep ALL existing `# noqa: F401` re-exports in `genizah_app.py`. No in-repo consumers besides the now-fixed DesktopVSCache import, but the compatibility risk of removing them outweighs the theoretical benefit. After the vs_cache move, the trio gets a new re-export shim too.
- **D-11:** Document the re-exports as intentional in a comment block or in the smoke checklist, noting they're removable when an external import audit is done.

### Import Cycle Verification
- **D-12:** Run `python -c "import desktop.widgets; import desktop.title_helpers; import desktop.image_loader; import desktop.result_dialog; import desktop.dialogs_scholarly; import desktop.dialogs_filter; import desktop.viewers; import desktop.puzzle; import desktop.vs_cache"` — must succeed with no errors. Additionally verify no `from genizah_app import` lines exist in any `desktop/*.py` file (grep check).

### Verification
- **D-13:** pytest baseline (1067 passed, 8 skipped) must remain green.
- **D-14:** Import smoke: all `desktop/` modules importable, no cycles, no `from genizah_app import` in desktop/.
- **D-15:** Desktop smoke checklist: user runs through all items, confirms no regressions.
- **D-16:** CI green (Ubuntu + Windows matrix).

### Docs Cleanup (minor)
- **D-17:** Fix `docs/OPEN_ISSUES.md` reference to `PRE_LAUNCH_CHECKLIST.md` — update path from repo root to `docs/archive/PRE_LAUNCH_CHECKLIST.md` (Codex spotted this drift).

### Claude's Discretion
- Commit granularity for the vs_cache extraction.
- Exact import set for `desktop/vs_cache.py` — derived via ruff iteration.
- Smoke checklist formatting and exact wording.
- Whether to add a brief "Architecture after v7.9" summary section to the smoke doc.

### Folded Todos
None — matched todos are orthogonal.

</decisions>

<canonical_refs>
## Canonical References

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 71 entry; v7.9 milestone boundaries
- `.planning/REQUIREMENTS.md` — DESK-06, DESK-07
- `.planning/PROJECT.md` — v7.9 Active milestone

### Source — Subject of the Phase
- `genizah_app.py:2452-2568` — `DesktopVSCache` (~70 lines)
- `genizah_app.py:2548-2568` — `VSFetchThread` (~21 lines)
- `genizah_app.py:2569-2658` — `VSDownloadThread` (~90 lines)
- `desktop/result_dialog.py:645` — last `from genizah_app import DesktopVSCache` back-edge

### Prior Phase Context (established pattern)
- `.planning/phases/70-puzzle-extraction/70-CONTEXT.md` — latest extraction pattern
- `.planning/phases/69-image-viewer-extraction/69-CONTEXT.md` — re-export, ruff F401

### Existing desktop/ Modules (verify no cycles)
- `desktop/__init__.py`, `desktop/widgets.py`, `desktop/title_helpers.py`, `desktop/image_loader.py`
- `desktop/result_dialog.py`, `desktop/dialogs_scholarly.py`, `desktop/dialogs_filter.py`
- `desktop/viewers.py`, `desktop/puzzle.py`

### Smoke Checklist Reference
- `docs/archive/PRE_LAUNCH_CHECKLIST.md` — existing pre-launch pattern to derive from

### CI & Verification
- `.github/workflows/ci.yml` — Ubuntu + Windows matrix
- `tests/` — 1067 passed, 8 skipped baseline

</canonical_refs>

<code_context>
## Existing Code Insights

### DesktopVSCache Dependencies
- `DesktopVSCache` uses: `genizah_core` (get_logger), `shared.fjms_service` (FjmsService), standard lib (os, json, threading)
- `VSFetchThread` — QThread wrapping FJMS visual similarity API call
- `VSDownloadThread` — QThread wrapping batch VS data download
- Used by: GenizahGUI (creates instance), `desktop/result_dialog.py` (lazy import for VS dialog)
- No dependency on other desktop/ modules — clean extraction target

### Current Import Graph (post-Phase 70)
```
genizah_app.py (coordinator)
  ├── desktop/widgets.py (shared widgets — ShelfmarkCompleter, ActionsHoverWidget, helpers)
  ├── desktop/title_helpers.py (title formatting)
  ├── desktop/image_loader.py (ImageLoaderThread)
  ├── desktop/result_dialog.py (ResultDialog)
  │     ├── desktop/dialogs_scholarly.py (4 scholarly dialogs)
  │     ├── desktop/viewers.py (ManuscriptViewerWidget — lazy)
  │     └── genizah_app.py:DesktopVSCache (BACK-EDGE — Phase 71 target)
  ├── desktop/dialogs_filter.py (3 filter dialogs)
  │     └── gui_threads.py (FilterCountWorker)
  ├── desktop/viewers.py (ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget)
  │     └── desktop/image_loader.py
  ├── desktop/puzzle.py (5 puzzle classes)
  │     ├── gui_threads.py (PuzzleImageLoaderThread, PuzzleMetaLoaderThread)
  │     └── desktop/widgets.py (ShelfmarkCompleter)
  └── gui_threads.py (all QThread workers)
```

After Phase 71: the `genizah_app.py:DesktopVSCache` back-edge becomes `desktop/vs_cache.py` (one-directional).

</code_context>

<deferred>
## Deferred Ideas

### For a Future Cleanup Phase (not currently scheduled)
- **TabularQueryBuilderDialog extraction** — 614 lines, single call site at genizah_app.py:15265, self-contained. Natural home: `desktop/dialogs_search.py` if Phase 72 (search-page-split) surfaces more search-builder UI.
- **Protocol/ABC narrowing** — PuzzleCanvasWindow.self.app has 20+ hard-coupled attributes. Define a typed contract (`PuzzleHostProtocol`) so the dependency is explicit. Same for ResultDialog's parent surface.
- **Re-export removal** — once external import audit confirms no consumers, remove all `# noqa: F401` re-exports from genizah_app.py.

### For Phase 76 (Documentation Close)
- Record `desktop/vs_cache.py` in `docs/CODE_INDEX.md`
- Update all remaining path references in docs

</deferred>

---

*Phase: 71-genizahgui-consolidation-smoke-tests*
*Context gathered: 2026-04-16*

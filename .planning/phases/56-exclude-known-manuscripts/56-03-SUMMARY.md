---
plan: 56-03
status: complete
started: 2026-03-29
completed: 2026-03-29
---

## Summary

Enhanced the desktop ExcludeDialog with multi-source exclusion:

- **Tabbed ExcludeDialog**: QTabWidget with "From File / Manual" (existing functionality + resolution report table) and "From List" (QListWidget with multi-select from ListsManager)
- **Resolution report (D-04)**: QTableWidget with color-coded rows (green=found, red=not_found, yellow=duplicate), auto-resolves on file load
- **get_exclusion_sources()**: Returns ExclusionSource objects from active tab (file/manual or list selection)
- **CSV support**: Load from File now accepts .txt and .csv, uses shared parse_csv_shelfmarks
- **Multi-source tracking**: `self.exclusion_sources` field, computed `excluded_sys_ids` via compute_excluded_ids
- **Per-source clear (D-06)**: `_remove_exclusion_source()` method
- **Count display (D-07)**: `_update_exclusion_display()` shows per-source breakdown in lbl_exclude_status
- **Session persistence (D-10)**: serialize_sources in _save_session, deserialize_sources in restore with backward compat (old excluded_sys_ids wrapped into legacy ExclusionSource)
- **Clear handlers**: Both regular and composition clear paths reset exclusion_sources

## Key Files

### Modified
- `genizah_app.py` — +266 lines: imports, exclusion_sources field, enhanced ExcludeDialog (tabs, report table, get_exclusion_sources), updated open_exclude_dialog, _update_exclusion_display, _remove_exclusion_source, session save/restore, clear handlers

## Verification

- `python -c "from genizah_app import ExcludeDialog; print('OK')"` — import clean
- `pytest tests/test_exclusion.py -x -q` — 15/15 passed (shared service)
- ExcludeDialog has QTabWidget with 2 tabs, QTableWidget for report, get_exclusion_sources method
- Session save includes `exclusion_sources`, restore handles both new and legacy format

## Deviations

- Second "post-search" button not added as separate widget — the existing toolbar button is always visible in the search tab, making a redundant button unnecessary. The toolbar is the single entry point for desktop (sufficient for D-01).

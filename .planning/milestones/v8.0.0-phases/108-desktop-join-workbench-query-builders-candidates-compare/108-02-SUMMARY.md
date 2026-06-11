---
phase: 108-desktop-join-workbench-query-builders-candidates-compare
plan: "02"
subsystem: desktop-joins-lab
tags: [joins-lab, query-builder, or-groups, per-row-mods, hoist, executor-adapter, i18n]
dependency_graph:
  requires: [108-01, 106-joins-lab-pure-logic, 107-join-workbench-shell]
  provides:
    - JoinQueryBuilder(QWidget) with multi-row OR boxes + PER-ROW mods HOISTED (RR-13)
    - _DesktopSearchExecutor adapter satisfying Phase-106 SearchExecutor Protocol (D-22)
    - QFrame/QSpinBox/QEvent imports added alongside first use (RR-15)
    - 31 new EN->HE translation entries (RR-4)
  affects: [desktop/join_workbench.py, genizah_translations.py]
tech_stack:
  added: []
  patterns:
    - PER-ROW modifier HOIST: single-box decorate token; multi-box group then hoist outside (RR-13)
    - Active-row tracking via eventFilter + _on_row_focus + _on_modifier_changed (transplanted from TabularQueryBuilderDialog)
    - allow_page_position flag gates page-position QComboBox (RR-5)
    - _responsa_opts() exposes ja/flex/bidir globals for Plan 03 to merge into ro (RR-14)
    - Thin passthrough adapter pattern for SearchExecutor Protocol (Phase-106 D-22)
key_files:
  created: []
  modified:
    - desktop/join_workbench.py (JoinQueryBuilder class + _DesktopSearchExecutor + QFrame/QSpinBox/QEvent imports)
    - genizah_translations.py (Phase 108-02 TRANSLATIONS.update block, 31 new EN->HE entries)
decisions:
  - "RR-13: mods dict lives on the ROW entry (not per box); HOISTED group form '#(a/b)' not per-box '(#a/%b)' which the parser treats as literals"
  - "RR-16: active-row reference cleared in both _remove_row and _remove_box handlers so a deleted row/box cannot receive modifier writes"
  - "RR-15: QFrame/QSpinBox added to QtWidgets tuple and QEvent added to QtCore line IN TASK 1 alongside first use — ruff F401 cannot fire on any intermediate state"
  - "RR-14: _responsa_opts() exposes ja/flex_spacing/bidirectional because compose() hardcodes them False and SideQuery cannot carry them; Plan 03 do_search merges these"
  - "RR-5: allow_page_position=False on the other-side builder omits the page_pos QComboBox entirely (self.page_pos = None); _page_position() returns None"
  - "D-22: _DesktopSearchExecutor is a thin passthrough — no per-app normalizer; execute_search returns [] on any exception (safe degradation)"
metrics:
  duration: "~6 minutes"
  completed: "2026-06-05T11:33:55Z"
  tasks_completed: 2
  tasks_total: 2
  files_changed: 2
  tests_added: 0
---

# Phase 108 Plan 02: JoinQueryBuilder Widget + _DesktopSearchExecutor Adapter Summary

**One-liner:** Multi-row OR-word-box query builder widget with PER-ROW modifiers hoisted outside the slash-group and a thin SearchExecutor Protocol adapter over the desktop searcher.

## What Was Built

### Task 1: JoinQueryBuilder widget (RR-1/RR-13/RR-14/RR-15/RR-16)

`desktop/join_workbench.py` — new `class JoinQueryBuilder(QWidget)` (placed inside `if _QT_AVAILABLE:`, above `_DesktopSearchExecutor` and `JoinWorkbenchWindow`).

**Import additions (RR-15 — added in Task 1 alongside first use):**
- `QFrame`, `QSpinBox` added to the `from PyQt6.QtWidgets import (...)` tuple
- `QEvent` added to the `from PyQt6.QtCore import (...)` line
- `QGridLayout`, `QTableWidget`, `QTableWidgetItem`, `SearchThread` NOT added (RR-10)

**Row model (RR-13):**
- Each row = a `dict` with keys `end` (QCheckBox), `boxes` (list of `{"edit": QLineEdit, ...}`), `mods` (dict), `ind` (QLabel), `start` (QCheckBox), `gap` (QSpinBox), `rm` (QPushButton), `widget`, `boxes_strip_layout`
- Each row starts with ONE box; `[+ or]` appends additional OR-alternative boxes
- The `mods` dict lives on the ROW entry — NOT on each box (RR-13)

**Active-row modifier wiring (transplanted from TabularQueryBuilderDialog, scoped to ROW):**
- `eventFilter(obj, event)`: on `QEvent.Type.FocusIn`, identifies which row owns the focused QLineEdit and calls `_on_row_focus(entry)`
- `_on_row_focus(entry)`: sets `self._active_row = entry`, reflects `entry["mods"]` onto the six modifier checkboxes (under `_updating_modifiers` guard)
- `_refresh_modifier_enabled()`: disables `chk_wild_start` (wildcard-prefix) when active row has >1 box (parser cannot strip leading `*` before OR group check, genizah_core.py:6140 — RR-13)
- `_on_modifier_changed()`: reads all six checkbox states into `self._active_row["mods"]`, calls `_update_row_indicator` + `_update_preview`
- `_active_row = None` set in both `_remove_row` and `_remove_box` (RR-16)

**HOISTED group term serialization (build_side_query, RR-13):**
- Single non-empty box → decorate the lone token:
  `negation → '-'+t; else plene → prefix → suffix-append → wildcard_prefix → wildcard_suffix`
- Multiple non-empty boxes → `group = "(" + "/".join(tokens) + ")"` then HOIST row mods outside:
  `negation → '-(group)'; else plene → prefix → suffix-append → wildcard_suffix`
  (wildcard_prefix skipped on multi-box — parser limitation)
- No `|`.join; no per-box `(#a/%b)` form (both forbidden by RR-1/RR-13)

**`_responsa_opts()` (RR-14):** returns `{"responsa_mode": True, "variants": v, "ja": ..., "flex_spacing": ..., "bidirectional": ..., "variant_mode": ...}`. PER-ROW token mods are NOT here — they are baked into the term by the HOIST rule. Plan 03's `do_search` merges `ja`/`flex_spacing`/`bidirectional` into the composed `ro`.

**`allow_page_position` (RR-5):** True for anchor side → builds the `page_pos` QComboBox with items `page: anywhere / page: start of text / page: end of text` (data `None/"start"/"end"`). False for other side → `self.page_pos = None`, `_page_position()` returns `None`.

**Live Preview:** read-only QLineEdit (RTL, color `#94a3b8`) updated on every keystroke / box-add / modifier change via `compose(build_side_query())` inside `try/except ValueError` (Pitfall 7).

**i18n (RR-4):** 31 new EN→HE entries added to `genizah_translations.py` in a `# === Phase 108-02 — JoinQueryBuilder i18n ===` block, covering all new `tr()` keys: row controls (`+ or`, `+ Add Line`, `ends line ⊣`, `⊢ starts line`, `↓ `, ` ln`, `Remove row`, `Remove this OR alternative`), variants, modifier row labels, modifier-row hint, wildcard-prefix-disable tooltip, page-position items and tooltip, Preview label, placeholder strings.

### Task 2: _DesktopSearchExecutor adapter (D-22)

`desktop/join_workbench.py` — new `class _DesktopSearchExecutor` (inside `if _QT_AVAILABLE:`, before `JoinWorkbenchWindow`).

Four Protocol methods (thin passthrough, no normalizer — Phase-106 D-01):
- `execute_search(...)` → `self._searcher.execute_search(...) or []`; returns `[]` on any exception (safe degradation)
- `get_browse_page(...)` → `self._searcher.get_browse_page(...)`
- `get_meta_for_id(sys_id)` → `self._meta_mgr.get_meta_for_id(sys_id)`
- `get_library_for_id(sys_id)` → `self._meta_mgr.get_library_for_id(sys_id) or ""`

Instantiated in `JoinWorkbenchWindow.__init__` as `self._executor = _DesktopSearchExecutor(self.searcher, self.meta_mgr)` immediately after `self.searcher` and `self.meta_mgr` are assigned.

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. All builder fields are wired and functional; the preview composes live queries via `compose()`. The builder does not yet receive real search results — that is Plan 03 (candidate rendering).

## Threat Flags

No new security-relevant surface introduced beyond what is documented in the plan's threat_model.

- T-108-03 (Tampering): The multi-box UI assembles box values into the engine slash-group `(t1/t2)` and applies PER-ROW `#`/`%`/`*`/`-` decoration HOISTED outside the group internally. `compose()` and `build_side_query()` are pure with no I/O. The page-position `ValueError` is caught and surfaced as a benign Preview hint. No new shell/SQL/eval surface.
- T-108-04 (DoS/expansion): Wildcard/variant expansion goes through the existing engine `MAX_EXPANDED_TERMS` guard — unchanged.

## Self-Check: PASSED

Files exist:
- `desktop/join_workbench.py` (JoinQueryBuilder + _DesktopSearchExecutor) — FOUND
- `genizah_translations.py` (Phase 108-02 TRANSLATIONS.update block) — FOUND

Commits exist:
- `7122d93c` (Task 1: JoinQueryBuilder + imports + translations) — FOUND
- `fbdfbd11` (Task 2: _DesktopSearchExecutor adapter) — FOUND

Tests pass: 194/194 green (test_join_workbench_builder + test_join_workbench_i18n + test_join_workbench_no_private + test_join_workbench + test_joins_lab).
ruff: All checks passed on desktop/join_workbench.py and genizah_translations.py.

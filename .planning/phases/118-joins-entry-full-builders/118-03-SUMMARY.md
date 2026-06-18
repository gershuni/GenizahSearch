---
phase: 118-joins-entry-full-builders
plan: "03"
subsystem: web/joins-lab
tags: [tdd, joins-lab, bld-03, modifier-hoist, nicegui, component]
dependency_graph:
  requires:
    - 118-01 (BLD-03 RED test stubs — test_builder_modifier_hoist.py)
  provides:
    - web/components/joins_builder.py (_apply_modifiers_to_term + build_side_query + create_joins_builder)
  affects:
    - plans/118-04 (Plan 04 imports create_joins_builder and mounts it in joins_lab.py)
tech_stack:
  added: []
  patterns:
    - Mutable-dict closure state (no class __init__, no app.storage.user)
    - Factory function returning handle dict (NiceGUI-idiomatic widget pattern)
    - Per-row modifier hoist with desktop parity (RR-13: wildcard_prefix not on slash-groups)
key_files:
  created:
    - web/components/joins_builder.py
  modified: []
decisions:
  - "_apply_modifiers_to_term: negation is terminal (returns immediately, overrides all other mods) — desktop parity desktop/join_workbench.py:1300-1341"
  - "build_side_query: line_start/line_end are BuilderRow flags, not text transforms; page_position None/'start'/'end' only (SideQuery.__post_init__ validates)"
  - "create_joins_builder: returns handle dict (not class instance) — consistent with Phase 117 mutable-dict closure pattern"
  - "Text Position options dictionary is module-level (called at import time via tr()) — matches search.py:646-655"
  - "allow_page_position=False: Text Position control hidden, build_side_query always passes page_position=None (Plan 04 uses for other-side widget)"
  - "Fuzzy hint rendered once, visibility toggled on mode change (avoids re-render cost)"
metrics:
  duration: "~3min"
  completed: "2026-06-18"
  tasks: 2
  files: 1
---

# Phase 118 Plan 03: Builder Widget Factory Summary

**One-liner:** Per-row modifier hoist (`_apply_modifiers_to_term`, RR-13 desktop parity) + `build_side_query` converter + `create_joins_builder` NiceGUI factory widget with Text Position / mode selector / gap controls / tune popover.

---

## What Was Built

### Task 1: Per-row modifier hoist + build_side_query (BLD-03 core logic) — commit `4fa36791`

**`web/components/joins_builder.py`** — pure module-level helper functions:

**`_apply_modifiers_to_term(term, mods)`** — applies per-row modifier flags to a user-typed term before constructing a `BuilderRow`. Desktop parity `desktop/join_workbench.py:1272-1347`:
- Strip term; detect slash-group (`'/' in t and not t.startswith('(')`)
- Wrap slash-groups in parens before modifier application
- Negation is terminal: returns `f'-{wrapped}'` immediately
- Else apply in order: plene (`%`), prefix (`#`), suffix (`#` appended), wildcard_prefix (`*` only when NOT slash-group — RR-13), wildcard_suffix (`*` appended)
- `line_start`/`line_end` are NOT text transforms (handled as `BuilderRow` flags in `build_side_query`)

**`build_side_query(rows_state, variants, page_position)`** — converts a list of row-state dicts to a `SideQuery`:
- For each row: calls `_apply_modifiers_to_term` then constructs `BuilderRow(term, line_start, line_end, gap_to_next)`
- Empty-builder guard: returns `None` when all rows have empty stripped terms
- Returns `SideQuery(rows=tuple(builder_rows), variants=variants, page_position=page_position)`

All 7 RED tests in `tests/test_builder_modifier_hoist.py` (Part 2) turned GREEN. The 4 compose() tests (Part 1) remain GREEN.

### Task 2: Builder UI widget — rows, tune popover, gap control, Text Position, mode selector — commit `4fa36791`

**`create_joins_builder(allow_page_position=True)`** — NiceGUI widget factory returning a handle dict:

**Handle dict keys:**
- `container` — top-level `ui.column` element (mount point for the page)
- `build_side_query()` — closes over live `rows_state` + mode + text position; routes `'start'/'end'` → `SideQuery.page_position`; routes `'line_start'/'line_end'` → `page_position=None` (Plan 04 passes these directly to `execute_search(text_position=...)`)
- `get_mode()` — `'exact'` | `'variants'` | `'fuzzy'`
- `get_text_position()` — one of 5 option values
- `get_summary()` — e.g. `'Variants · 3 lines · Text Position: Line starts'`
- `is_empty()` — `True` when all builder rows are blank

**Widget features per UI-SPEC:**
- Header row: Text Position `ui.select` (5 options, 12px bold uppercase label, prominent above rows) + Mode selector (Exact/Variants/Fuzzy flat buttons with active/inactive state) + Fuzzy hint (12px muted, visibility-toggled)
- Vertical builder row stack: row number label (12px muted, right-aligned) + RTL Hebrew-serif `ui.input` (16px, outlined, direction:rtl) + tune icon button (opens `ui.menu` with 8 modifier checkboxes) + remove button (hidden on last row)
- Active modifier indicator: tune button color switches to `--primary-600` when any modifier is active
- Gap control between rows: `ui.number` (56px, 0–20 step 1) + `↕ gap` label; border color switches to `--border-focus` when gap > 0
- Add line button: flat small below last row; re-renders full rows area
- When `allow_page_position=False`: Text Position control hidden; `build_side_query()` always passes `page_position=None`

**Bilingual:** 24 `tr()` calls — all user-facing strings bilingual from line 1.

---

## Test Status

| File | Tests | Result |
|------|-------|--------|
| test_builder_modifier_hoist.py | 11 | PASS (7 RED→GREEN + 4 already green) |
| test_no_raw_storage_access.py | 6 | PASS (Phase 87 invariant preserved) |

---

## Deviations from Plan

None — plan executed exactly as written.

The file implements both the pure logic layer (Task 1) and the UI widget factory (Task 2) in a single implementation. Both tasks target the same file `web/components/joins_builder.py`, so both were delivered in a single commit `4fa36791`.

---

## Threat Surface Scan

| Threat ID | Check | Result |
|-----------|-------|--------|
| T-118-02 | No `app.storage.user` access | PASS — all state is closure-local; `test_no_raw_storage_access.py` green |
| T-118-04 | Builder text → compose() → engine (not SQL) | PASS — `_apply_modifiers_to_term` produces only Responsa tokens; no SQL |
| T-118-05 | Fuzzy mode DoS | ACCEPT — widget only captures mode string; dispatch is Plan 04's responsibility |

No new network endpoints, auth paths, or schema changes introduced.

## Known Stubs

None. The widget is functionally complete. Plan 04 wires it into `joins_lab.py`.

## Self-Check: PASSED

Files verified to exist:
- web/components/joins_builder.py — FOUND

Commits verified:
- 4fa36791: feat(118-03): implement _apply_modifiers_to_term + build_side_query (BLD-03 GREEN) — FOUND

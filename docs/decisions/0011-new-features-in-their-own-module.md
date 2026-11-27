# 0011 -- A new feature goes in its own module; the big units only wire it in

- **Status:** binding
- **Date:** 2026-11-27
- **Source:** the repository health review of 2026-11-23 (recommendations 8 and 9: extract by
  feature, together with its state, while fixing bugs in that area), and the desktop
  search-run work that followed it (dev #143, #144, #147, #150, #151).

## Decision

New logic for the desktop window or a large web page factory goes in a module of its own,
which holds that feature's state. The big unit (`GenizahGUI` in `genizah_app.py`, or a
`create_*_page` factory under `web/pages/`) keeps only the call or signal that wires the
feature in. The same applies when a bug is fixed in such a unit: move that feature's code and
state out while fixing it, rather than adding to the unit.

Examples: `desktop/reading_desk_text.py` and `shared/reading_desk_model.py` (the Reading Desk),
`desktop/search_run.py` (the search worker's lifetime), `desktop/search_scope.py` (one scope
per tab), `desktop/result_view.py` (filters and counts over the whole result list) and
`desktop/search_export.py` (the background export).

## Why

Extraction once cut `genizah_app.py` by a third, and the file grew back past its old size: a
new desktop feature is easiest to write as methods on `GenizahGUI`, because that class already
holds the state. A feature in its own module can be tested without building the window, and
its state has one owner. `genizah_core.py` shows that a unit with a fixed role and a guard
stays small.

## Consequences

- A new module is registered where the guards look for it: `DESKTOP_MODULES` in
  `tests/test_no_back_edges_desktop.py` for `desktop/`, `EXTRACTED_MODULES` in
  `tests/test_no_back_edges_core.py` for `shared/`, a section in `docs/CODE_INDEX.md`, and a
  `changelog.d/` fragment.
- If the wiring alone needs more lines, the ceiling is raised in the same PR and the PR
  description says why, so the raise is a decision made in review.
- When a unit shrinks by more than the test's slack (100 lines), its ceiling is lowered to the
  new size in the same PR.

## Supersedes

Nothing. It writes down the rule that `tests/test_size_ratchet.py` already enforces.

## Enforced by

[tests/test_size_ratchet.py](../../tests/test_size_ratchet.py): each large unit has a ceiling
in lines; the test fails when a unit grows past it, and when a unit shrinks by more than its
slack until the ceiling is lowered.

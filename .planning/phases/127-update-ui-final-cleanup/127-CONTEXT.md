# Phase 127 — Update UI & Final Cleanup: CONTEXT

**Status:** Discuss-phase **SKIPPED** (no genuine user-facing gray areas). Per the standing v8.3.0
autonomous directive. [[feedback_no_auto_discuss]] holds — this is a *skip*, not an auto-answer.

## Why discuss is skipped
Phase 127 is a **pure internal refactor, ZERO behavior change** — the FINAL phase of v8.3.0. It (a)
extracts the `update_ui` cluster to `desktop/update_ui.py`, (b) retires the Phase-126 (D1) re-export
shims, (c) installs the desktop back-edge AST guard, (d) confirms the `genizah_core` permanent facade,
and (e) signs off the full suite. The architecture is locked in the ROADMAP (Phase 127 section) +
SEED-020 (§Desktop E1). No product/UX choices for the user. The one real unknown — whether the
"sidecar reset/download **coordination**" is entangled GenizahGUI methods (like the D2-D5 panels that
were just deferred) — is a **research/Codex-pre-flight** question, delegated. If research surfaces a
genuine fork (e.g. the coordination is too entangled to extract safely), pause then.

## Locked decisions (from ROADMAP + SEED-020 E1 — do NOT re-litigate)

### DESK-08 — extract `desktop/update_ui.py` (MOVE-and-shim, the genizah_core/D1 recipe)
- Move the update-UI **classes**: `UpdateNotificationBar`, `WhatsNewBar`, `WhatsNewDialog`,
  `UpdateProgressDialog` → `desktop/update_ui.py`; DELETE the originals from `genizah_app.py`, replace
  with a `# noqa: F401` re-export shim so `genizah_app.X is desktop.update_ui.X` identity holds.
- **NEW direct behavioral tests** for the sidecar reset/download coordination methods (SEED-020 §7 C-6
  — these GUI methods have no direct test today). Register any new GUI test in conftest `_GUI_TEST_FILES`.
- `pyqtSignal`-bearing workers stay at module level in the new home.

### Final cleanup
- **Retire the D1 shims:** retarget every `from genizah_app import <D1 class>` / `genizah_app.<D1 class>`
  caller to `desktop.settings_dialogs` / `desktop.ui_widgets`, then DELETE the two D1 shim lines from
  `genizah_app.py` (genizah_app.py:77-78). This is the GUARD-03/04 "flip" deferred from Phase 126.
- **Install `tests/test_no_back_edges_desktop.py`** — AST guard mirroring `test_no_back_edges_core.py`:
  no `desktop/` module imports `genizah_app` at MODULE level (lazy `# noqa: PLC0415` only).
- **Confirm the `genizah_core` permanent facade** stays intact (`tests/test_genizah_core_facade.py`,
  new or updated): `genizah_core.X is shared.Y.X` for the moved core names. The `genizah_core` facade is
  PERMANENT — NEVER removed (contrast: the `genizah_app` D1 shims ARE retired here).
- **Full-suite sign-off** (bulk + gui slices) — the milestone's final zero-behavior-change gate.

## Research directives (the planning-critical investigation — apply the 126 lesson)
1. **Are the 4 update-UI classes top-level CLASSES** (clean move-and-shim like D1) or GenizahGUI methods?
   Pin their exact line ranges in `genizah_app.py` (grep `^class`, do NOT trust line numbers).
2. **THE crux (126 lesson): is the "sidecar reset/download coordination" a set of densely cross-called
   GenizahGUI METHODS** (like the D2-D5 panels we just deferred to SEED-028)? Map the coupling — which
   `self.<method>()`/`self.<widget>` callers stay in GenizahGUI. **If it's entangled, RECOMMEND DEFERRING
   the coordination-method extraction** (extract only the 4 bar/dialog CLASSES + write the new behavioral
   tests AGAINST THE METHODS IN PLACE on GenizahGUI). Do NOT force a risky method-move under zero-behavior-change.
3. **Enumerate ALL callers of the 9 D1 classes** (`from genizah_app import SettingsDialog | SearchSettingsDialog
   | HelpDialog | TabularQueryBuilderDialog | LabScoringDialog | ShelfmarkTableWidgetItem | CheckBoxHeader |
   HiddenScrollArea | ListsTreeWidget`, and `genizah_app.<class>` attribute access) — the full set to
   retarget for the shim retirement. Confirm none break after the shim is deleted.
4. **`test_no_back_edges_desktop.py` shape** — read `test_no_back_edges_core.py` as the template; enumerate
   the current `desktop/*.py` modules it must scan (puzzle, viewers, dialogs_*, vs_cache, join_workbench,
   telemetry, file_actions, my_library_tab, image_loader, result_dialog, widgets, title_helpers,
   settings_dialogs, ui_widgets, + the new update_ui).
5. **`test_genizah_core_facade.py`** — does one exist? What core names must it assert identity for (the ~20
   from search_engine + the 122-124 names)?
6. **GUARD-03:** any source-scan test pinning the update_ui symbols or the D1 classes (for the retarget/flip).

## Out of scope (do NOT attempt here)
- D2-D5 method-based panels (catalog/search/browse/lists) → **SEED-028** (DESK-03..07, deferred).
- Composition tab (DEFER-02/03) + startup/session remainder (DEFER-04).
- The ≥70% `genizah_app.py` shrink — explicitly **NOT a v8.3.0 target** (the bulk is the deferred panels).
- Removing the `genizah_core` facade (it is PERMANENT).
- Any web change / any behavior change.

## Base & drill
- **Base:** HEAD `0f3cbc4d` (code `7a692319`).
- **Full drill:** research → pattern-map → plan(opus) → gsd-plan-checker → **Codex PLAN pre-flight** →
  execute (sequential, USE_WORKTREES=false) + source-integrity gate (identity + base-vs-HEAD name diff NOT
  count + bulk/gui slices, 6-env baseline) → **Codex CODE review** → gsd-verifier → CLOSE v8.3.0. BOTH Codex
  gates must clear. Never repo-wide `ruff --fix`.

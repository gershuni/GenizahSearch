# Phase 67: ResultDialog Extraction - Context

**Gathered:** 2026-04-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract the `ResultDialog` class (QDialog, ~2,818 lines, 68 methods, 2 `pyqtSignal`s) from `genizah_app.py:6045-8862` into a new `desktop/result_dialog.py` module. Zero user-visible behavior changes. This is the first extraction in v7.9 and sets the pattern for Phases 68-71 (dialogs, viewers, puzzle).

In scope:
- Create the `desktop/` package (with `__init__.py`) — no module exists yet
- Move `ResultDialog` into `desktop/result_dialog.py`
- Introduce `desktop/widgets.py` (neutral shared module) for helpers used by both ResultDialog and the rest of `genizah_app.py`
- Mechanical rename inside ResultDialog: `self.parent()` → `self._app` (assigned once in `__init__`)
- Verify no regression via pytest + import smoke + minimal manual desktop smoke

Out of scope:
- Any behavior change, styling change, or feature addition
- Tightening the ResultDialog/GenizahGUI coupling via Protocol/ABC (deferred — see Deferred Ideas)
- Extracting any other dialog / viewer / puzzle class (Phases 68-70)
- Writing new unit tests for ResultDialog (existing pytest baseline is the contract)

</domain>

<decisions>
## Implementation Decisions

### Parent Coupling (Gray Area 1)
- **D-01:** In `ResultDialog.__init__`, after `super().__init__(parent)`, assign `self._app = parent`. Mechanically replace every `self.parent()` usage inside `ResultDialog` with `self._app`. Behavior is identical; coupling becomes explicit and greppable.
- **D-02:** Do NOT introduce a Protocol/ABC for the parent surface in this phase. Narrowing the ~33-method dependency is explicitly deferred to Phase 71 (GenizahGUI Consolidation) so Phase 67 stays low-risk.

### Shared Helpers (Gray Area 2)
- **D-03:** Create `desktop/widgets.py` as a neutral module. Move `_format_add_to_list_label` (7 callers) and `ActionsHoverWidget` (20+ callers, currently at `genizah_app.py:8862`) into it.
- **D-04:** Both `genizah_app.py` AND `desktop/result_dialog.py` import these helpers from `desktop.widgets`. Import direction is: `genizah_app.py` → `desktop/widgets.py` ← `desktop/result_dialog.py`. No circular import between `genizah_app` and `desktop/result_dialog`.

### ResultDialog Module Contents (Gray Area 3)
- **D-05:** `desktop/result_dialog.py` contains `ResultDialog` plus any helper function/class used **exclusively** by ResultDialog. Researcher must scan the 6045-8862 line range for exclusive helpers; shared ones go to `desktop/widgets.py` (see D-03).
- **D-06:** Imports ResultDialog needs from `genizah_app` must be one-directional — `desktop/result_dialog.py` pulls from `genizah_core`, `genizah_translations`, `shared/*`, `gui_threads`, and `desktop/widgets.py`, but NOT from `genizah_app.py`.

### Verification (Gray Area 4)
- **D-07:** pytest baseline (1067 passed, 8 skipped as of 2026-04-14) must remain green — no change to that count (neither lower nor higher for this phase).
- **D-08:** Import smoke executed after extraction: `python -c "from desktop.result_dialog import ResultDialog; from desktop.widgets import ActionsHoverWidget, _format_add_to_list_label; from genizah_app import GenizahGUI"` must succeed with no errors.
- **D-09:** Minimal manual desktop smoke on Windows: launch `python genizah_app.py`, run one basic search, open a single result in ResultDialog, navigate one step (prev or next), close. No crash, no visible regression. Executor documents result in phase summary.
- **D-10:** CI green (Ubuntu + Windows matrix via v7.8 safety net) is the authoritative gate — per-push CI must pass before phase closes.

### Module Layout
- **D-11:** New files: `desktop/__init__.py` (empty or tiny `"""Desktop UI modules (v7.9 decomposition)."""`), `desktop/widgets.py`, `desktop/result_dialog.py`.
- **D-12:** `genizah_app.py` imports `from desktop.result_dialog import ResultDialog` and `from desktop.widgets import ActionsHoverWidget, _format_add_to_list_label` at module top, replacing the inline class definitions.

### Claude's Discretion
- Commit granularity: planner/executor may split into multiple commits (e.g., create package skeleton → move shared helpers → move ResultDialog → rename `self.parent()` → re-import in `genizah_app.py`) or bundle. Whatever keeps each commit pytest-green.
- Exact file header/docstring wording for the new modules.
- Whether `desktop/widgets.py` becomes `desktop/helpers.py` if a clearer name emerges during research (non-blocking — choose whatever reads best).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap & Requirements
- `.planning/ROADMAP.md` — Phase 67 entry (lines 205, 218-229); v7.9 milestone boundaries
- `.planning/REQUIREMENTS.md` — DESK-01 (line 14), DESK-06, DESK-07 (lines 19-20); non-regression expectations
- `.planning/PROJECT.md` — Constraints (dual-app, shared core), v7.9 Active milestone description

### Source — Subject of the Phase
- `genizah_app.py:6045-8862` — `ResultDialog` class (2,818 lines, 68 methods)
- `genizah_app.py:8862-8906` — `ActionsHoverWidget` (shared helper to relocate)
- `genizah_app.py:8907-` (starts at 8907) — `_format_add_to_list_label` (shared helper to relocate)
- `genizah_app.py:12754` — `GenizahGUI(QMainWindow)` (the parent; ~33 members accessed by ResultDialog)

### Parent Surface Used by ResultDialog (inventory for researcher)
Known parent attrs/methods (sampled via grep of `NR>=6045 && NR<=8862`):
`_VS_SERVER_URL`, `_auto_select_pgp_edition`, `_browse_document_by_shelfmark`, `_build_fjms_catalog_html`, `_build_pgp_extended_info_html`, `_domain_display_name`, `_enrich_vs_suggestions`, `_ensure_shelf_map`, `_is_item_in_non_recent_list`, `_normalize_fl_id`, `_normalize_shelfmark`, `_open_document_result_dialog`, `_populate_pgp_combo`, `_result_domain_map`, `_search_by_pgp_tag`, `_shelf_to_sys`, `_show_vs_dialog`, `_start_field_translation`, `_trans_toggle_state`, `_vs_cache`, `add_to_puzzle`, `btn_b_translations`, `btn_search_translations`, `chk_show_translations`, `comp_text_area`, `corrections_client`, `joins_mgr`, `lists_mgr`, `meta_mgr`, `open_result_in_browse`, `send_result_to_composition`, `show_add_to_list_menu`.

### CI & Verification Context
- `.github/workflows/ci.yml` (v7.8 safety net — Ubuntu + Windows matrix)
- `tests/` (1067 passing tests as of 2026-04-14 baseline)
- `docs/OPEN_ISSUES.md` — any new decomposition findings get recorded here during Phase 76

### Prior Patterns (for reference)
- `shared/` (service-layer extraction pattern — 20+ modules) — demonstrates the import-direction discipline for shared modules; `desktop/` should follow the same one-way imports convention

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`shared/` service pattern**: 20+ shared modules (e.g., `shared/document_service.py`, `shared/corrections_service.py`) already show how a neutral module imported by both web and desktop avoids circular imports. `desktop/widgets.py` follows the same direction rule.
- **`gui_threads.py`**: already a neutral module imported by `genizah_app.py` (e.g., `SearchThread`). Demonstrates that breaking classes out of the monolith into a sibling top-level `.py` works.
- **`genizah_translations.py`**: `tr()` function is already imported by `genizah_app.py` — `desktop/result_dialog.py` imports directly from there, not transitively.

### Established Patterns
- **One-directional imports**: `shared/*` NEVER imports from `genizah_app.py` or `web/*`. `desktop/*` must preserve this — `desktop/result_dialog.py` and `desktop/widgets.py` must not import `genizah_app`.
- **Module docstring**: every `shared/*.py` begins with a one-line docstring naming the module's purpose. New `desktop/*` files should follow.
- **Qt lifecycle guards**: v6.0.0 added `sip.isdeleted()` guards across Qt lifecycle sites. ResultDialog currently contains some; keep them in place verbatim during the move.

### Integration Points
- **`genizah_app.py` ResultDialog call sites** (from grep): lines 1753, 6626, 6744, 6784, 6934, 6972, 7367, 7572, 7588, 7680, 8142, 8250, 8286, 8323, 8396, 8409, 8626, 9009, 13914, 17791, 18258, 18259, 18362, 19756, 20076, 20714, 20718, 20726, 20743 — all must continue to work unchanged after the import swap.
- **`parent().`-style references INSIDE ResultDialog**: all stay in the same lines but lexically change to `self._app.` — strict text substitution scoped to lines 6045-8862.

### Constraints from this Codebase
- **Windows is the deploy target** — CI matrix includes Windows; planner must ensure no UNIX-isms leak in.
- **No new dependencies** — v7.9 is structural only; pinned in `requirements-lock.txt`.
- **ruff scoped ruleset (E9/F401/F811/F821)** — unused imports after move will break CI.

</code_context>

<specifics>
## Specific Ideas

From Codex external review (captured during discussion):
- Import smoke should include **three** import lines, not just one:
  1. `from desktop.result_dialog import ResultDialog`
  2. `from desktop.widgets import ActionsHoverWidget, _format_add_to_list_label`
  3. `from genizah_app import GenizahGUI` (proves the main app still imports cleanly after ResultDialog is moved out)
- Manual smoke kept intentionally minimal: app start → one search → open one result → navigate once → close. Not a formal checklist — two minutes of eyeballing.
- Phase 67 is explicitly low-risk by choice. Protocol/ABC coupling cleanup is Phase 71's job.

User preference: ask questions in plain English; defer technical specifics to external AI (Codex / Gemini CLIs). Keep CONTEXT.md technical for downstream agents.

</specifics>

<deferred>
## Deferred Ideas

### For Phase 71 (GenizahGUI Consolidation & Smoke Tests)
- **Protocol/ABC narrowing of the ResultDialog ↔ GenizahGUI interface.** Define a `ResultDialogHost` Protocol in `desktop/result_dialog.py` (or a shared location) enumerating the ~33 parent attrs/methods ResultDialog calls. Replace `self._app: Any` with `self._app: ResultDialogHost`. Lets ruff / mypy (future) catch accidental expansion of the coupling.
- **Rationale for deferral**: Phase 67 must ship low-risk to validate the extraction pattern for Phases 68-70. Narrowing the interface is orthogonal to the move and mixes refactoring concerns.

### For Phase 76 (Documentation Close)
- Record `desktop/result_dialog.py`, `desktop/widgets.py`, `desktop/__init__.py` in `docs/CODE_INDEX.md`.
- Update any path references in docs pointing to `genizah_app.py:6045` (ResultDialog class) to the new location.

### Potentially for Phases 68-71 (pattern propagation)
- Once this phase ships, the `desktop/widgets.py` module is the canonical home for other shared UI helpers surfaced during subsequent extractions. Phases 68-70 may add to it.

### Reviewed Todos (not folded)
None — no pending todos matched Phase 67 scope. The pending todos in STATE.md (desktop corrections fetch migration, CUT-01, date/creation-type filters) are orthogonal feature work.

</deferred>

---

*Phase: 67-resultdialog-extraction*
*Context gathered: 2026-04-15*

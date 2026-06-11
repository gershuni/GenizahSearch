# Plan 110-03 SUMMARY — Composition tab LOCAL corpus UI wiring

**Status:** complete (human-verify checkpoint APPROVED 2026-06-09)
**Plan:** 110-03 (wave 3) · requirements COMP-LOC-01, COMP-LOC-02

## Objective
Wire the desktop composition tab for LOCAL corpus support: the Genizah/Local/ALL selector,
run_composition scope plumbing to both threads, session persistence, Lab decoupling — and (via the
UAT corrections folded in) make standard LOCAL composition actually search the regular My-Library
index and display LOCAL hits correctly.

## What was built (final state, after UAT corrections)
- **Selector:** `comp_corpus_scope_combo` (bilingual hardcoded HE/EN, mirroring the Search-tab
  selector) + `_on_comp_corpus_scope_changed`; orthogonal to Lab Mode (D-06 decoupling — Lab Mode no
  longer hardwired to LOCAL).
- **Routing:** `run_composition` reads the scope and passes `corpus_scope=` to BOTH `CompositionThread`
  and `LabCompositionThread`. Parallels-from-browse inherits the selector (no extra wiring).
- **Persistence (3 paths):** full session restore, the persistent-preferences path
  (`_apply_persistent_session_preferences`, Round-2 #2), and composition history re-run (Round-2 #3) —
  each validates fail-closed and restores via the blockSignals idiom.
- **LOCAL hit display:** LOCAL composition hits render shelfmark=filename, Library=parent/folder
  (helpers `_comp_item_is_local` / `_comp_local_display_fields` / `_prime_comp_local_filepath_cache`),
  mirroring regular search — in both the comp results tree and the ResultDialog. Filepath cache primed
  at the `display_comp_results` entry point (covers the session-restore render path).

## Design correction (vs the original plan)
The original plan routed standard (Lab-off) LOCAL composition through the LAB side-index and added a
weights-hash override + 3-site refresh + a stale-LAB signal. UAT showed standard scope=Local returned
nothing (LAB side-index unbuilt). Per the user's authoritative intent (CONTEXT design-correction
block): **the LAB index is opt-in; the default uses the regular index.** So:
- Standard composition scope=Local/ALL now queries the **regular My-Library index** (engine fix in
  110-02 re-point). Lab Mode keeps the LAB/fingerprint index.
- The weights-hash override, `_refresh_lab_weights_hash_override` (3 sites), the
  `lbl_comp_local_stale` label, and `_refresh_comp_stale_label_for_scope` were **REMOVED** — the
  default path has no staleness concept (empty LOCAL = "no results", like Genizah). The
  `my_library_tab._on_lab_rebuild_finished` override-refresh wiring was reverted.

## Key files
- `genizah_app.py` — selector, handler, run_composition wiring, persistence (3 paths), LOCAL display
  helpers, ResultDialog LOCAL branch.
- `desktop/my_library_tab.py` — (net) unchanged after the revert of the override-refresh hook.

## Verification
- Human-verify checkpoint APPROVED (selector + routing + Lab decoupling + persistence + history +
  parallels + LOCAL display all confirmed live).
- `pytest tests/test_comp_corpus_scope.py` green; `python -c "import genizah_app"` exits 0; ruff clean.

## Deviations
Major: the staleness/weights-hash machinery specified in the original plan was removed (design
correction — see CONTEXT). The selector / persistence / routing / Lab-decoupling deliverables stand.

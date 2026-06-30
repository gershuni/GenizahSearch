# Phase 126 — Codex PLAN Pre-flight Findings (Round 1) + Resolutions

**Verdict:** REVISE (1 BLOCKER + 3 HIGH + 2 MEDIUM). Full output: `scratchpad/126-codex/planflight-r1.txt`.
These are plan↔LIVE-code drifts the internal checker structurally cannot catch (it doesn't grep code).
**Re-plan all affected plans per the resolutions below, then re-run the Codex pre-flight (round 2).**

---

## BLOCKER — the recipe must be MOVE-and-shim, NOT copy-keep-both

**Finding:** The plans relocate each cluster to `desktop/X.py` but KEEP the original class/def in
`genizah_app.py` ("copy-not-move"), and add `from desktop.X import Y  # noqa: F401` at the top shim
block (genizah_app.py:67). In Python the later, kept original definition (`class SettingsDialog`
@2218, `class SearchSettingsDialog` @697, `class LabScoringDialog` @596, the UI widgets @1092/1099/
1336/1520/1583/1605, `class _CatalogRefreshWorker` @1453) **OVERWRITES** the top-of-file import →
`genizah_app.SettingsDialog is desktop.settings_dialogs.SettingsDialog` is **False**. The identity
acceptance criteria can never pass; and the file carries duplicate, drift-prone definitions.

**RESOLUTION (locked): use MOVE-and-shim — exactly what genizah_core Phases 122–125 did** (genizah_core
shrank 12,506→755 ln precisely because code was MOVED OUT, and the `# noqa: F401` shims then gave
`genizah_core.X is shared.Y.X` identity 20/20). For Phase 126:
- For each cluster: **DELETE the cluster's class/worker code from `genizah_app.py`** and replace it
  (in place, or via the shim block) with `from desktop.X import <names>  # noqa: F401`. No kept
  original → no shadowing → identity holds.
- `genizah_app.py` therefore **shrinks in Phase 126** (this is fine and expected — the ROADMAP's
  "≥70% shrink" is satisfied by end of Phase 127 regardless of which phase moves the bytes; ROADMAP
  126 SC#1 only requires the `from genizah_app import …` imports to KEEP WORKING, which the shim
  delivers).
- **Phase 127 then does:** retarget the external `from genizah_app import …` callers to
  `from desktop.X import …`, DELETE the shim lines, install `test_no_back_edges_desktop.py`. (This
  supersedes the roadmap's loosely-worded "delete the copied cluster code in 127" — there is no
  copied duplicate to delete under move-and-shim.)

**Method-based panels (D3/D4/D5) — apply the `desktop/my_library_tab.py` MyLibraryTab model:** the
search-results / browse / lists "panels" are largely `GenizahGUI` METHODS, not standalone classes.
Create `class XPanel(QWidget)` in `desktop/X.py` that OWNS the cluster's widgets + (former) methods;
`GenizahGUI` instantiates `self.x_panel = XPanel(self)` and its former `self.<method>()` call sites
become panel-owned (or delegate via the back-ref). MOVE the methods onto the panel class (delete from
GenizahGUI) — same move-and-shim identity discipline, shim re-exports the panel CLASS.
**Exception — cross-cluster methods stay on GenizahGUI** (do NOT move): see MEDIUM-2.

---

## HIGH-1 — D3 wrongly owns the browse view-all methods (reassign to D4)

`_render_view_all_batch` (genizah_app.py:20854) + `_append_next_view_all_batch` (:20893), called from
`_open_local_browse` (:20648), write via `apply_line_numbered_text(self.browse_text, …)` (:20883) —
they are the **browse/local view-all** flow and use `self.browse_text`. The D3 plan lists them
(126-03-PLAN.md:79, :121). **RESOLUTION:** move them (and their caller chain that belongs to browse)
to **D4**, not D3 — they share `browse_text`, which is exactly why D3→D4 is sequenced. Re-scope D3 to
the true search-results lifecycle methods only.

## HIGH-2 — D3 missing source-scan test `test_local_filter_cascade.py`

D3 claims "no D3 source-scanning tests to retarget" (126-03-PLAN.md:178, :182). FALSE —
`tests/test_local_filter_cascade.py` reads/AST-parses `genizah_app.py` (`GENIZAH_APP_PY` :17, parse
:46) and pins MOVED D3 methods `_apply_results_table_filters`/`_apply_local_filter` (:63) +
`_apply_local_optout_filter` (:200). **RESOLUTION:** add it to D3's ADDITIVE-retarget set (accept old
genizah_app OR new desktop location).

## HIGH-3 — D4 GUARD-03 list materially wrong (true browse scanners)

Plan says "8" but lists 9 (126-04-PLAN.md:174,:177). Codex's live count of genuine browse-cluster
source-scanners = **13 total**; that set INCLUDES `test_local_filter_cascade.py` which is actually a
**D3** scanner (reassign per HIGH-2), and the plan's 9 MISSES five genuine browse scanners:
`test_local_nav_codex_fix7.py:192`, `test_local_nav_codex_fix8.py:338`, `test_my_library_tab.py:538`,
`test_join_workbench_vs.py:622`, `test_synthetic_round_trip.py:509`. **RESOLUTION:** D4's additive-
retarget set = the genuine browse scanners (the 13 minus the one D3 scanner = **12**); fix the count
label to match; `test_browse_state.py` stays EXCLUDED (web-side, scans `web.pages.browse_state`).
**Each plan must enumerate its retarget set by FILENAME (no bare counts) so the count can't drift.**

## MEDIUM-1 — D4 `browse_thumb_resolved` must verify all 4 sites

Plan names 4 sites (126-04-PLAN.md:96, "ALL FOUR" :147) but the success criterion says "all 3
use-sites" (:167). Live sites: declaration :3359, connect :3585, emit :26024, **handler :26039**.
**RESOLUTION:** acceptance must assert all FOUR (incl. the handler) land on the SAME object as the
signal declaration; pin the handler disposition (move onto BrowsePanel with the signal, or keep on
GenizahGUI and connect across — choose one explicitly).

## MEDIUM-2 — D5 `show_add_to_list_menu` is cross-cluster — keep on GenizahGUI

D5 moves `show_add_to_list_menu` (126-05-PLAN.md:77), but browse (:11487) and search (:20347) call it
via `self.`. Under move-and-shim, moving it onto ListsPanel breaks those callers immediately (they're
in browse/search code, not lists). **RESOLUTION:** **keep `show_add_to_list_menu` on `GenizahGUI`**
(do NOT move it in D5) — the kept-in-place pattern (like `_CATALOG_FILTER_SETS`); panels call it via
the back-ref. Note the cross-cluster caller set for the Phase-127 caller-retarget.

---

## Also fold in the internal-checker's 3 warnings (Codex confirmed/subsumed all 3)
- 126-04 "8 vs 9" count → resolved by HIGH-3 (enumerate by filename; true = 12 browse).
- 126-02 undocumented shim-serialization `depends_on` → add the one-line rationale ("serialized due to
  the shared genizah_app.py shim-block edit, not a code dependency; D3→D4 IS a code dep via browse_text").
- 126-04 browse_thumb_resolved 4th site → resolved by MEDIUM-1.

## Unchanged invariants (still correct — keep)
Sequential waves D1→D5 (all edit genizah_app.py); GUARD-01 desktop (no module-level `import
genizah_app`); GUARD-04 base-vs-HEAD `dir(genizah_app)` NAME diff per commit (NOT count); GUARD-02
bulk+gui slices, 6-env baseline; never repo-wide `ruff --fix`; LabPanel→E2; `_CATALOG_FILTER_SETS`
kept-in-place; Community populators stay; D-07b telemetry strip + `_ListsSyncCoordinator` gate verbatim;
NEW `test_search_results_panel.py` + conftest registration.

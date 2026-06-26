---
id: SEED-028
status: dormant
planted: 2026-06-26
planted_during: v8.3.0 God-File Decomposition, Phase 126. The Codex PLAN pre-flight (round 2) proved the method-based desktop panels are too entangled to extract safely under a zero-behavior-change mandate this milestone. User decision (Hillel, 2026-06-26): defer the entangled panels; keep only the clean class extractions (D1) in Phase 126.
trigger_when: A future milestone, AFTER a prerequisite widget-ownership / back-ref refactor (analogous to E2's CompositionState requirement — DEFER-02). Treat like DEFER-03/04: needs the GenizahGUI god-class broken into setter/property-exposed tab state FIRST. Not safe as a copy/move-and-shim until then.
scope: large (4 method-based panel clusters carved out of a 28k-line god class with dense self.method()/self.widget cross-references; needs delegating wrappers + ownership splits + caller retargeting)
---

# SEED-028: Method-based desktop panel extraction (catalog tab, search-results, browse/reading-desk, lists)

> Deferred from v8.3.0 Phase 126. Phase 126 shipped only **D1** (the clean top-level dialog + widget
> CLASS extractions → `desktop/settings_dialogs.py` + `desktop/ui_widgets.py`). The four method-based
> clusters below were proven unsafe-as-is by the Codex PLAN pre-flight and deferred per user decision.

## Why deferred (the core difficulty)
genizah_core (Phases 122–125) extracted **classes** with clean boundaries — move-and-shim gave
identity 20/20 trivially. These desktop "panels" are different: they are **GenizahGUI METHODS**, not
classes, and the god class calls them densely via `self.method()` and references their widgets via
`self.widget`. Codex PLAN pre-flight (round 2, `scratchpad/126-codex/planflight-r2.txt`) found that a
straight move-and-shim **breaks every `self.<moved>()` caller that remains in GenizahGUI** at the wave
it runs:
- **D2 catalog tab:** `_catalog_populate_tree`/`_catalog_start_async_refresh`/`_catalog_update_chips`
  called by `_on_tab_changed` (genizah_app.py:4188), `_navigate_to_catalog_browse` (:10108-10144),
  session restore (:27264-27303).
- **D3 search-results:** `_collect_sorted_results`/`_apply_results_table_filters`/`on_search_finished`/
  `set_results_loading` called by `export_results` (:21320), filter dialogs (:22805/22863/22882),
  startup (:3523/3594), restore+history (:26629/27135). `on_search_finished` ALONE touches **109**
  distinct `self.*` attributes — more coupled than the E3 cluster (~50) SEED-020 already deferred.
- **D4 browse + reading-desk:** `browse_load`/`_open_local_browse`/`_open_local_browse_page` +
  view-all (`_render_view_all_batch`/`_append_next_view_all_batch`) called by `open_result_in_browse`
  (:20446-20527), `lists_browse_by_id` (:14517), community join nav (:16229), restore (:27234).
  `browse_thumb_resolved` signal has 4 load-bearing sites (decl :3359 / connect :3585 / emit :26024 /
  handler :26039).
- **D5 lists:** `_enable_lists_cloud_sync`/`_disable_lists_cloud_sync` called by login/logout
  (:4201/4225). Cross-cluster `show_add_to_list_menu` (def :15132) called by browse (:11494),
  visual-sim (:5802), search-results (:20372/20388/20444).

## Prerequisite (do this FIRST)
A **widget-ownership / state refactor** so each panel owns its widgets and exposes tab state behind
setters/properties, and GenizahGUI reaches panel state through the panel ref — the same prerequisite
E2 (composition tab) needs via a `CompositionState` dataclass (DEFER-02). Without it, the `self.<own>`
vs `self._parent.<shared>` split inside 109-self.* method bodies is error-prone under zero-behavior-change.

## The technique (when it runs)
MyLibraryTab model (panel `QWidget` owns widgets + former methods + GenizahGUI back-ref) PLUS, for the
transition, **delegating wrappers**: leave a thin `def m(self,*a,**k): return self._panel.m(*a,**k)`
on GenizahGUI for every moved method that has an external caller, and `@property` proxies for moved
widgets that other code references — removed only when all callers are retargeted. Cross-cluster
methods (`show_add_to_list_menu`, `open_result_in_browse`) either stay on GenizahGUI (kept-in-place)
or move to a shared home. Sequence so each panel's external callers are handled at its wave.

## Starting point (already produced — reuse)
- `.planning/phases/126-desktop-panels/deferred-method-panels/126-02..05-PLAN.md` — the move-and-shim
  plans (Codex r1-corrected) for D2–D5; need the delegating-wrapper + caller-retargeting layer added.
- `.planning/phases/126-desktop-panels/126-RESEARCH.md` (coupling map, line ranges) + `126-PATTERNS.md`
  (SP-1..5, MyLibraryTab analog) + `126-PREFLIGHT-CODEX.md` (the recipe + reassignment fixes) +
  `scratchpad/126-codex/planflight-r1.txt`/`planflight-r2.txt` (the exact caller evidence).
- GUARD-03 corrected lists: D3 scanner = `test_local_filter_cascade.py`; D4 browse scanners (13, by
  filename) incl. `test_local_nav_codex_fix7/8.py`, `test_my_library_tab.py`, `test_join_workbench_vs.py`,
  `test_synthetic_round_trip.py`; `test_browse_state.py` is web-side (exclude).

## Requirements deferred here
DESK-03 (catalog_browse), DESK-04 (search_results_panel), DESK-05 (browse_panel), DESK-06
(reading_desk_panel), DESK-07 (lists_tab). DESK-01/02 (D1 dialogs/widgets) ship in Phase 126.

## Done when
The four method-based clusters are extracted to their `desktop/` modules behind the MyLibraryTab model
+ delegating wrappers, with all cross-cluster callers handled, GUARD-01..04 held, zero behavior change,
full suite + gui slice green, Codex-reviewed. (Pairs naturally with E2/E3 — DEFER-02/03/04 — as "finish
decomposing GenizahGUI" once the state refactor lands.)

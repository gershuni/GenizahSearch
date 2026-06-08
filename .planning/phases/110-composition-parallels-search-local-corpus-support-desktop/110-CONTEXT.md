# Phase 110: Composition / Parallels Search — LOCAL Corpus Support (desktop) - Context

**Gathered:** 2026-06-08
**Status:** Ready for planning

<domain>
## Phase Boundary

> **⚠ PHASE REFRAMED 2026-06-08.** This phase was originally "Search-Support — Parallels Seeding
> (JSA-01)". Per user decision in this discuss-phase, **all of Component B (the Join Workbench
> search-support algorithms — JSA-01, JSA-02, JSA-03, and the deferred JWB-05 tear-side assist) is
> DEFERRED to a post-v8.0.0 milestone**, and the Phase 110 slot is **repurposed** to the
> LOCAL-composition wiring the user wants before release. ROADMAP.md + REQUIREMENTS.md were amended in
> this session to match (see the amendments list below).

**What this phase delivers (desktop-only):** wire the LOCAL ("My Library") corpus into the existing
**composition / parallels search**, and make the composition-results export LOCAL-aware.

**In scope (COMP-LOC-01, COMP-LOC-02, EXP-F3):**
- A **pre-search `Genizah / Local / ALL` corpus selector** on the composition / parallels tab,
  mirroring the existing Search-tab selector (`genizah_app.py:5953`). It scopes **which corpus
  composition searches**, and is **orthogonal to the composition search MODE** (standard *and* Lab
  both honor it).
- Composition search **executes against the selected corpus**: `Local` = LOCAL only, `ALL` =
  Genizah + LOCAL merged, `Genizah` = unchanged. LOCAL hits render into the existing composition
  results surface; a **stale LOCAL LAB index** surfaces a rebuild/staleness signal (does not silently
  drop LOCAL hits).
- **`export_comp_report` (`genizah_app.py:20447`) made LOCAL-aware (EXP-F3)** — a Local/ALL
  composition run exports LOCAL hits with local-meaningful columns (filename/folder/filepath/page/
  matched-text) via the Phase 103 export helpers.
- After this phase ships and is verified → **`/release` v8.0.0** (the version-file bump happens at
  release time, not in this phase).

**Explicitly OUT of this phase:**
- **All of Component B / Join Workbench search-support** (JSA-01 anchor parallels seeding, JSA-02
  corpus completion, JSA-03 torn-word completion, JWB-05 tear-side assist) — DEFERRED to post-v8.0.0.
- **Post-search LOCAL filter activation on the composition surface** — user chose **pre-search
  scoping only** (the dormant `local_filter_btn_composition` scaffolding is NOT activated this phase;
  see Claude's Discretion re: hiding the misleading dormant control).
- **Web composition/parallels LOCAL** — web has no composition UI; LOCAL is desktop-only.
- Any change to the Join Workbench (Phases 106–109 are complete and untouched here).

</domain>

<decisions>
## Implementation Decisions

> **⚠ DESIGN CORRECTION 2026-06-08 (Plan 110-03 UAT checkpoint).** During human verification of
> the composition corpus selector, standard (Lab-Mode-OFF) composition with scope=Local returned
> **nothing**. Root cause: Plans 110-01/02/03 routed *standard* LOCAL composition through the **LOCAL
> LAB side-index** (`local_lab_searcher`), which only exists once the user has built it via Lab Mode —
> so it was `None`, the `_check_local_lab_freshness()` gate failed, and the hook was silently skipped.
> **User's corrected intent (authoritative):** the LAB index is an **opt-in** ("Lab Mode") for both
> search and composition; **by default both must use the REGULAR index.** So standard composition with
> scope=Local/ALL must query the **regular My-Library index** (`local_searcher`/`local_index`, the same
> index regular search scope=Local uses — its schema carries `content`/`unique_id`/`full_header`/`source`,
> everything the composition hook reads), and **Lab Mode** keeps using the LAB/fingerprint side-index.
> **Consequences (supersede the originals below):**
> - **D-07/RF-2 (amended):** standard `Local`/`ALL` composition merges hits from the regular LOCAL index
>   (`local_searcher`), NOT the LAB side-index. Lab Mode composition (`lab_composition_search`) still uses
>   the LAB side-index — unchanged.
> - **D-08/RF-4 (SUPERSEDED for the default path):** the LAB weights-hash mismatch + the
>   `_lab_weights_hash_override` + the 3-site refresh + the stale-LAB signal were only needed *because*
>   the default path was wrongly routed through the LAB index. The regular LOCAL index has **no weights
>   hash / no staleness concept** → the default path needs none of it. Per user: an empty LOCAL result
>   is treated exactly like an empty Genizah result ("no results"), **no staleness banner.** Any LAB
>   staleness concern is now Lab-Mode-only (pre-existing `lab_composition_search` behavior; out of scope).
> - **Tests (110-01) amended:** the standard-path routing tests assert routing to the **regular**
>   `local_searcher` (not `local_lab_searcher`); Lab-path tests keep asserting LAB routing;
>   `test_stale_lab_sets_flag` is repurposed to the Lab path (or dropped from the default path).

### Scope & deferral
- **D-01:** **Defer ALL of Component B to a post-v8.0.0 milestone** (user decision). JSA-01 / JSA-02 /
  JSA-03 / JWB-05 moved to REQUIREMENTS.md § Future → "Component B — Search-support algorithms". No
  keep/spike/cut disposition is recorded because nothing in Component B ships in v8.0.0. Component A
  (Join Workbench, Phases 106–109) is complete and unaffected.
- **D-02:** **Desktop-only.** Web has no composition/parallels UI and no LOCAL "My Library"; this is
  inherently a desktop change. (Dual-app rule does not apply — there is no web surface to mirror.)
- **D-03:** **This is the last v8.0.0 phase.** After verification → `/release` v8.0.0 (web + desktop
  bundle decision per the release skill; the rebrand is desktop-only).

### Corpus selector (COMP-LOC-01)
- **D-04:** **Pre-search `Genizah / Local / ALL` dropdown on the composition tab**, mirroring the
  Search-tab selector pattern (`genizah_app.py:5953` storage `self._search_corpus_scope` →
  `_on_corpus_scope_changed` → `SearchThread(corpus_scope=…)` → `execute_search(corpus_scope=…)`).
  Add an analogous `comp_corpus_scope_combo` + state field on the composition tab; persist it in the
  session like the Search-tab scope.
- **D-05:** **Pre-search scoping ONLY — do NOT activate the post-search LOCAL filter** on the
  composition surface (user decision). Composition is expensive; scoping *what* is searched up front
  is the right lever. The dormant post-search comp-filter scaffolding
  (`_local_filter_state_composition`, `local_filter_btn_composition`, gated on `has_local`) stays
  inactive. (See Claude's Discretion — consider hiding it to avoid a misleading dead control.)
- **D-06:** **Corpus is ORTHOGONAL to the composition search MODE** (user decision, 2026-06-08): the
  selector governs which corpus is searched for **both** standard composition and **Lab** composition
  — exactly like regular search, where the corpus dropdown is independent of exact/variants/fuzzy.
  **"Lab Mode" must NOT be hardwired to LOCAL.** Today LOCAL composition only runs via the Lab-Mode
  checkbox (`chk_lab_mode_comp` → `LabCompositionThread` → `lab_engine.lab_composition_search`); that
  coupling is removed — Lab Mode searches whichever corpus the dropdown picks. (See **RF-1**: the
  planner must first determine what "Lab Mode" does *beyond* searching LOCAL — `deep_scan` /
  `scan_limit` — and preserve any genuine extra semantics as an orthogonal toggle.)

### Search execution & merge (COMP-LOC-02)
- **D-07:** **`Local`** = LOCAL corpus only; **`ALL`** = Genizah + LOCAL **merged**; **`Genizah`** =
  unchanged from today (zero regression for the default path). The merge approach for `ALL` is a
  research flag (**RF-2**) — lean: mirror regular search's **RRF k=60** post-merge
  (`genizah_core.py:8823`), adapted to composition's Main/Appendix grouping + span-length scores.
- **D-08:** **Stale LOCAL LAB index must not silently drop LOCAL hits.** Reuse the existing
  weights-hash staleness detection (`_current_lab_weights_hash` — `genizah_core.py:7079` /
  `:808`) to surface a rebuild/staleness signal (banner or prompt) when a Local/ALL composition run
  hits a stale LAB index, rather than the current silent-skip behavior
  (`genizah_core.py:9085` `_lab_fresh` gate). (**RF-4**.)
- **D-09:** **Parallels inherits the selector.** "Parallels" is a convenience wrapper
  (`browse_search_parallels` → `send_result_to_composition` → `run_composition`), so it picks up the
  corpus selector automatically. Decide the default scope for a parallels-from-browse launch
  (lean: current selector value, default `Genizah`) — planner detail (**RF-6**).

### LOCAL-aware export (EXP-F3)
- **D-10:** **`export_comp_report` (`genizah_app.py:20447`) becomes LOCAL-aware.** Detect LOCAL hits
  via `item['display']['source'] == 'LOCAL'` (the established search-export pattern) and emit
  local-meaningful columns (filename / folder / filepath / page / matched-text) by reusing the Phase
  103 helpers (`shared/export_dossier.py::build_local_document_row` / `local_documents_header_row`;
  `shared/docx_export.py::write_docx_result_block`). Prime the `_local_filepath_cache` for batch
  filepath resolution. A Genizah-only composition export is **unchanged** (no regression). (**RF-5**.)

### Architecture / constraints (carried forward — locked)
- **D-11:** **i18n from line one** — every new string (`comp_corpus_scope_combo` items, any
  staleness banner, new export headers) is `tr()`-wrapped (EN + HE) with keys added to
  `genizah_translations.TRANSLATIONS`. (Same i18n-guard discipline as Phases 107–109.)
- **D-12:** **No LOCAL data ever reaches the cloud** — the three v7.14 cloud-write gates remain
  inviolate; LOCAL composition hits are desktop-only, never synced/exported to Supabase/web/API.
  (Inherent here, but state it as a non-regression invariant.)
- **D-13:** **Genizah default path is a strict non-regression baseline.** All existing
  Genizah-only composition + parallels + export behavior must be byte-for-byte unchanged when scope =
  `Genizah` (the default). Add a regression test pinning this.

### Claude's Discretion
- Exact placement/label of the `comp_corpus_scope_combo` on the composition tab (lean: next to the
  mode combo / Lab-Mode checkbox, matching the Search-tab layout).
- Whether to **hide/remove the now-misleading dormant post-search comp LOCAL-filter control**
  (`local_filter_btn_composition`) given D-05 chose pre-search-only — small cleanup, planner's call
  (it is shared with the Parallels surface per the memory note; verify before removing).
- Staleness signal styling (banner vs inline note vs toast) for D-08.
- Helper decomposition; how much of `run_composition` / `CompositionThread` /
  `search_composition_logic` is parameterized vs branched for `corpus_scope`.

### Research flags (for gsd-phase-researcher / gsd-planner)
- **RF-1 (load-bearing):** Determine exactly what **"Lab Mode"** does beyond searching LOCAL —
  inspect `LabCompositionThread` (`gui_threads.py:216`, params `deep_scan`/`scan_limit`),
  `LabEngine.lab_composition_search` (`genizah_core.py:640`), and `chk_lab_mode_comp`. Then design
  D-06: route LOCAL/ALL via the corpus selector for **both** standard and Lab modes; preserve any
  genuine Lab-Mode-only scan semantics as an **orthogonal** toggle (not a corpus proxy). If "Lab
  Mode" exists *only* to search LOCAL, the corpus selector subsumes it (retire the checkbox).
- **RF-2:** **`ALL`-scope merge semantics for composition** — `search_composition_logic`
  (`genizah_core.py:8892`, no `corpus_scope` today) needs a `corpus_scope` param + a LOCAL/ALL branch.
  Reconcile the two existing LOCAL paths: the standard path's half-wired `_lab_fresh`-gated LOCAL LAB
  hook (`genizah_core.py:9085`) and the `LabEngine` path. Decide the merge (RRF k=60 like regular
  search, vs sectioned Genizah/LOCAL, vs interleaved by score) given composition's Main/Appendix +
  span-length scoring.
- **RF-3:** Thread the `corpus_scope` parameter end-to-end: `comp_corpus_scope_combo` →
  `run_composition` (`genizah_app.py:21654`) → `CompositionThread.__init__`
  (`gui_threads.py:163`) / `LabCompositionThread` → `search_composition_logic`
  (`genizah_core.py:8892`) — mirroring the proven Search-tab chain.
- **RF-4:** Wire the **LAB staleness signal** (D-08) using `_current_lab_weights_hash`
  (`genizah_core.py:7079` / `:808`) + `local_lab_searcher_stale`. Confirm whether the
  Phase-97 weights-hash mismatch (standard composition omitting LOCAL LAB hits) is still live and, if
  so, fix it so a Local/ALL composition run surfaces the staleness rather than empty LOCAL results.
- **RF-5:** **EXP-F3** — map `export_comp_report` (`genizah_app.py:20447`) to the Phase 103 LOCAL
  export helpers; detect LOCAL via `display['source']=='LOCAL'`; batch filepath resolution via
  `_local_filepath_cache`; verify all four formats (xlsx / csv / txt / docx) and keep the Genizah-only
  path unchanged (cross-parity test).
- **RF-6:** Confirm the **parallels** wrapper (`browse_search_parallels` →
  `send_result_to_composition`, `genizah_app.py:10681`/`:19944`) inherits the selector with no
  separate wiring; pick the default scope for a parallels-from-browse launch.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & roadmap
- `.planning/REQUIREMENTS.md` § "New Build — Composition / Parallels LOCAL Corpus (COMP-LOC)"
  (COMP-LOC-01/02 + EXP-F3 promoted), § "Component B … ⏸ DEFERRED", § Future → "Component B —
  Search-support algorithms (DEFERRED from v8.0.0)", and the updated Traceability table.
- `.planning/ROADMAP.md` § "Phase 110: Composition / Parallels Search — LOCAL Corpus Support" — goal,
  reframe note, dependencies, 4 success criteria.

### The pattern to mirror (Search-tab corpus selector — read first)
- `genizah_app.py:5953` (selector UI in `create_search_tab`), `:3187` (`_search_corpus_scope`
  storage), `:16846` (`_on_corpus_scope_changed`), `:16972` (read combo → `SearchThread(corpus_scope=…)`).
- `gui_threads.py:86` `SearchThread.__init__(..., corpus_scope=…)` → `:112` forwards to `execute_search`.
- `genizah_core.py:8412` `execute_search(corpus_scope=…)` → `:8430` LOCAL-only branch → `:8823` RRF
  k=60 LOCAL merge.

### Composition / parallels code to extend (read before planning)
- `genizah_app.py:6489` `create_composition_tab` (add the selector here), `:21654` `run_composition`
  (read scope, pass to thread), `:18064`/`:18130` dormant post-search comp LOCAL-filter (D-05 — leave
  inactive / consider hiding), `:10681` `browse_search_parallels`, `:19944`
  `send_result_to_composition`.
- `gui_threads.py:163` `CompositionThread`, `:216` `LabCompositionThread` (the two composition
  threads — add `corpus_scope`; reconcile per RF-1).
- `genizah_core.py:8892` `search_composition_logic` (add `corpus_scope`; LOCAL/ALL branch), `:640`
  `LabEngine.lab_composition_search`, `:9085` the `_lab_fresh`-gated LOCAL LAB hook, `:7079`/`:808`
  `_current_lab_weights_hash` (staleness), `:8823` RRF merge reference.
- `shared/local_indexer.py:4285` `build_lab_side_index` + `:4448` `weights_hash` / `.meta.json`.

### EXP-F3 export
- `genizah_app.py:20447` `export_comp_report` (make LOCAL-aware).
- `shared/export_dossier.py` `build_local_document_row` / `local_documents_header_row` (Phase 103
  helpers); `shared/docx_export.py::write_docx_result_block` (LOCAL DOCX block).
- `.planning/phases/103-search-results-local-export-all-formats-bilingual-non-regression/103-CONTEXT.md`
  — the LOCAL-export design + helper contracts EXP-F3 reuses.

### Prior context (for the LOCAL/LAB architecture)
- Project memory `project_composition_no_local_corpus_path.md` — the originating pre-release intention
  (this phase implements it). Memory `feedback_no_auto_reindex_in_init.md` — never trigger bulk
  re-indexing from `__init__`/UI thread (a LAB-rebuild prompt must stay on the background worker).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **The Search-tab corpus selector is a complete, shipping template** (Genizah/Local/ALL → RRF k=60
  LOCAL merge). Phase 110 is largely "do the same thing on the composition tab."
- **`LabEngine.lab_composition_search` already searches the LOCAL LAB side-index** — the LOCAL
  composition engine path exists; the missing piece is a corpus_scope param + UI selector + decoupling
  it from the "Lab Mode" checkbox (RF-1).
- **Phase 103 LOCAL-export helpers** (`shared/export_dossier.py`, `shared/docx_export.py`) make
  EXP-F3 mostly a wiring job, not new export logic.
- **Existing LAB staleness detection** (`_current_lab_weights_hash`, `local_lab_searcher_stale`) is
  reusable for the D-08 rebuild/staleness signal.

### Established Patterns
- Corpus-scope chain: combo `currentData()` → state field → thread `corpus_scope` → engine branch →
  RRF k=60 LOCAL merge.
- LOCAL hit shape: `item['display']['source'] == 'LOCAL'`; LOCAL columns =
  filename/folder/filepath/page/matched-text.
- Composition result shape: `{'main':[…], 'filtered':[…]}` with per-item
  `uid/raw_header/src_lbl/source_ctx/text/score/highlight_pattern`; grouped to manuscript via
  `group_pages_by_manuscript()`.

### Integration Points
- Composition tab (`create_composition_tab`) gains the selector; `run_composition` reads it; the two
  composition threads forward `corpus_scope` to `search_composition_logic` / `lab_composition_search`.
- `export_comp_report` partitions LOCAL vs Genizah rows and delegates LOCAL rows to the Phase 103
  helpers.

</code_context>

<specifics>
## Specific Ideas

- **Mirror the Search tab exactly** — same Genizah/Local/ALL dropdown semantics, same RRF LOCAL merge,
  so the composition tab behaves like the search tab users already know.
- **Lab Mode ≠ LOCAL** (user's explicit framing): "Lab mode should not include LOCAL by default; it
  should search in the same fashion as regular mode — i.e. by choosing in which corpus the search is
  performed." The corpus is a dropdown decision; the mode is a separate decision.
- **Pre-search scoping only** — no post-search LOCAL filter on the composition surface.
- **Ship-focused** — this is the gate to v8.0.0 release; keep the surface area tight (selector +
  search routing + LOCAL-aware export), defer everything algorithmic (Component B).

</specifics>

<deferred>
## Deferred Ideas

- **Component B — Join Workbench search-support algorithms (JSA-01, JSA-02, JSA-03, JWB-05)** —
  DEFERRED in full to a **post-v8.0.0 milestone** (this discussion). JSA-01 = anchor parallels
  seeding inside the Workbench; JSA-02 = corpus-completion; JSA-03 = `[`/`]` torn-word completion;
  JWB-05 = `[`/`]` tear-side assist. All ride the completed Component A.
- **Post-search LOCAL filter on the composition surface** — not activated this phase (D-05);
  the dormant scaffolding remains for a possible future iteration.
- **Web composition/parallels LOCAL** — no web composition UI exists; out of scope.
- **JOINS-F1** relative-offset cross-line positional search, **JOINS-F2** Dicta/Sefaria citation-ID
  completion, **JOINS-F3** batch-export + persisted candidate list, **JOINS-F4** auto-ranked finder —
  all remain Future (unchanged).

### Reviewed Todos (not folded)
- `project_composition_no_local_corpus_path.md` (the originating intention) is now being implemented
  by this phase — folded, not deferred.

</deferred>

---

*Phase: 110-composition-parallels-search-local-corpus-support-desktop*
*Context gathered: 2026-06-08*

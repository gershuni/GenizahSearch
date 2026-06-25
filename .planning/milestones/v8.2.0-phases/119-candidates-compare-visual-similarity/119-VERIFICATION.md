---
phase: 119-candidates-compare-visual-similarity
verified: 2026-06-19T00:00:00Z
status: human_needed
score: 5/6 must-haves verified
overrides_applied: 1
overrides:
  - must_have: "a self-match banner appears when detect_self_match finds the anchor among its own candidates"
    reason: >
      D-13 (user decision documented in 119-CONTEXT.md): self-match is silently excluded via
      dedup_candidates(include_self=False). detect_self_match still runs; result captured as
      _self_matched but not surfaced. This is explicit desktop parity — the desktop never
      shows a banner. CONTEXT.md §D-13 says 'Flag for planner + verifier so the phase is not
      failed for a missing banner — CND-05 is satisfied by correct exclusion, not by a UI surface.'
      Phase 120 will add the UI badge.
    accepted_by: "hillel (documented in 119-CONTEXT.md D-13)"
    accepted_at: "2026-06-19T00:00:00Z"
human_verification:
  - test: "Open /joins-lab in the browser, load an anchor (sys_id), run a search with 1+ result lines, and confirm the candidate grid appears with 160×160 image-first cards, Prev/Next pagination, and triage Y/?/N buttons visible on each card."
    expected: "Grid renders up to 24 cards per page; cards show thumbnail at ≈160×160; each card has three triage buttons (Yes/Maybe/No) and a Compare button; Prev/Next controls appear when more than 24 candidates."
    why_human: "The NiceGUI render path is not exercised by the headless tests; only the headless pure-function layer is tested. Visual card layout and actual browser DOM rendering require a live browser session."
  - test: "Toggle between Grid and Table views — confirm table shows 8 columns (Shelfmark, Score, Snippet, Material, Dimensions, Page, Triage, and a select column), that selecting rows reveals a bulk-triage bar, and that triage state set in grid still shows in table."
    expected: "Table is sortable; multi-select works; bulk 'Mark N selected as: Y/Maybe/No' appears on row selection; switching back to grid shows the same verdict borders/fills."
    why_human: "Table interactivity (selection events, sort, bulk bar visibility) requires a live browser."
  - test: "Open the Filters dialog — confirm material select is disabled until enrichment loads, then enable it; apply a material filter and verify the candidate count changes; apply size-mismatch exclusion and verify mismatched candidates disappear."
    expected: "Material multi-select starts disabled with 'Loading...' note; after enrichment it shows available materials; applying filter re-renders with fewer candidates; page resets to 1."
    why_human: "Filter dialog visibility, enrichment timing, and re-render triggered by enrichment completion require a live page."
  - test: "Click 'Compare fragment' on a candidate card — confirm the full-screen Compare modal opens with the anchor image on the left and the candidate image on the right; navigate the candidate pane to a different folio without moving the anchor pane; record a verdict and confirm the grid card's border color updates."
    expected: "Two independent viewers; folio navigation is per-pane; recording Yes gives the card a green border; auto-advance moves to the next candidate."
    why_human: "Two-pane AnchorViewer rendering, per-pane folio independence, and card restyle after modal verdict require a live browser."
  - test: "Toggle the 👁 Visual Similarity switch ON with a search query active — confirm the displayed candidates are narrowed to the intersection (text AND VS), and the count notice updates. Then clear the builder and toggle VS ON — confirm the pure VS union renders (no 'Enter at least one search line' toast)."
    expected: "Intersection mode: fewer candidates, each with the 👁 badge. Pure VS mode: the builder-empty path renders look-alikes without the empty-builder notify."
    why_human: "VS toggle state transitions, conditional merge model, and the empty-builder F1 branch behavior require a live NiceGUI page with a real anchor loaded."
  - test: "Load a fresh anchor (re-anchor) after triaging some candidates — confirm that all triage verdicts are cleared and the VS look-alikes refetch for the new anchor."
    expected: "After re-anchor: triage dict is empty (no verdict borders on any card); if VS was ON, look-alikes refetch and the loading notice appears briefly."
    why_human: "Re-anchor invalidation + VS refetch sequence requires a live session."
---

# Phase 119: Candidates, Compare & Visual Similarity — Verification Report

**Phase Goal:** Scholars can work a large candidate set efficiently — grid/table with persistent triage, filters, bounded rendering, off-loop enrichment — pull up a side-by-side Compare of anchor vs candidate, and toggle Visual Similarity to merge FIST look-alikes, with look-alikes badged consistently everywhere.
**Verified:** 2026-06-19
**Status:** human_needed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths (from ROADMAP Success Criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| SC-1 | Candidate surface offers grid and table views; triage verdicts and eye badges show consistently in both; switching view never resets or hides per-candidate state | VERIFIED | `web/components/candidate_grid.py` implements both views sharing a single `_triage` dict; `get_table_columns()` returns 8 columns; `get_table_config()` returns `selection='multiple'`, `row_key='uid'`; `_make_table_rows` includes badge_and_tooltip marker; tests/test_candidate_surface.py passes |
| SC-2 | Triage verdicts (Yes/Maybe/No) keyed by sys_id, reflected immediately in grid and table, cleared on re-anchor; self-match banner appears when detect_self_match finds the anchor | VERIFIED (override) | Triage implementation verified: `TriageState` keyed by `sys_id`; `load_anchor` clears `_triage`; `detect_self_match` runs and result is captured (`_self_matched`). **Banner deliberately omitted** per D-13 user decision (desktop parity). Override applied — see frontmatter. |
| SC-3 | Candidate filters (material/dimensions/size-mismatch/triage state) narrow display, persist across grid/table; result cap/pagination prevents unbounded render; neither render loop nor enrichment blocks the event loop | VERIFIED | `compute_filtered` pure function verified with tests; `_PAGE_SIZE = 24` and `paginate()` implemented; enrichment via `run.io_bound(run_enrich_core)` — off-loop guard test `test_enrichment_batch_not_on_event_loop` PASSES (load-bearing) |
| SC-4 | Candidate metadata enriched asynchronously off event loop, in batches, with image/network lookups through Phase-98 NLI circuit breaker — NLI outage degrades thumbnails gracefully without stalling | VERIFIED | `_enrich_candidates` dispatches `run_enrich_core` via `run.io_bound`; `build_thumbnail_url` routes through `/api/*` proxy (NLI) or Oxford direct-Bodleian; no direct `iiif.nli.org.il` URLs; off-loop guard passes |
| SC-5 | Compare from any candidate shows side-by-side anchor↔candidate panel (image + transcription) with independent per-pane zoom and folio navigation; recording verdict syncs to triage with no refresh | VERIFIED | `create_compare_modal` factory implemented with two fresh `AnchorViewer` instances; `_find_candidate_idx` locates by per-image identity (uid/(sys_id,page)); `record_verdict` auto-advances; `_on_compare_verdict` writes `_triage` and calls restyle; tests/test_compare_modal.py passes |
| SC-6 | Single 👁 toggle merges FIST look-alikes (off = text-only; on = merged/intersection); tracks loaded anchor sid; explicit disabled/no-data/empty-intersection states; badges look-alikes consistently across grid, table, and Compare | VERIFIED | `_vs_on`/`_vs_anchor_sid` state in joins_lab.py; `_apply_vs_merge` implements intersection/union/text-only; `_fetch_vs_candidates` dispatches via `run.io_bound(run_vs_core)`; `badge_and_tooltip` is the single badge source used by grid cards, table rows, and Compare modal header; test_vs_adapter.py passes |

**Score:** 5/6 SC truths directly VERIFIED (SC-2 passes via documented override for the banner divergence)

### Deferred Items

None — all gaps are either verified or covered by the accepted override.

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/joins_lab.py` | `badge_and_tooltip(cand) -> tuple` pure helper with ⚓›⇄›👁 precedence | VERIFIED | Function at line 627; precedence: `is_anchor_self` → `via_other_side` → `via_vs` → `(None,'')`. Import test: `python -c "from shared.joins_lab import badge_and_tooltip"` exits 0. |
| `web/components/candidate_grid.py` | Pagination, triage state, grid 160×160 cards, sortable multi-select table, filter dialog, bulk triage, per-image Compare hook | VERIFIED | 1027-line file; contains `_PAGE_SIZE=24`, `paginate()`, `TriageState` with `backing` kwarg, `compute_filtered`, `get_table_columns()` (8 cols), `open_filter_dialog()`, `_make_restyle_fn()`. No module-level `_card_refs`. No `gap-3`/`p-3`. |
| `web/components/compare_modal.py` | Full-screen two-pane Compare dialog; per-image lookup; flip-through + verdict auto-advance + badges | VERIFIED | 433-line file; `create_compare_modal()` factory; `_find_candidate_idx()` locates by `uid` then `(sys_id,page)`; `step_candidate()` with wrap-around; `record_verdict()` calls `on_verdict(sys_id,v)` then advances; no `inject_viewer_assets` call. |
| `web/pages/joins_lab.py` | VS adapter, enrichment batch, conditional merge, VS-only empty-builder branch, surface+Compare+VS-toggle wiring | VERIFIED | 1957-line file; `run_vs_core` and `run_enrich_core` closures dispatched via `run.io_bound`; `_apply_vs_merge` pure function; VS-only F1 branch at line 1520; `create_compare_modal` imported and called in `_open_compare`; `TriageState(backing=_triage)` at line 662. |
| `tests/test_joins_lab_render_contract.py` | 22 introspection tests covering CR-01..05 class of bugs | VERIFIED | 22/22 tests pass; covers `TriageState` backing-dict contract, `open_filter_dialog` caller kwargs, `Candidate` field contract, `_card_refs` module-level absence, `compose()` 3-tuple return. |
| Test suite (12 files) | All Phase 119 tests green | VERIFIED | 169 passed, 25 xpassed in 4.11s. Zero failures, zero errors. |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `web/pages/joins_lab.py` | `shared.visual_similarity_service.get_vs_service` | `run.io_bound(run_vs_core)` | VERIFIED | `run_vs_core` closure calls `get_vs_service(thread_safe=True).get_suggestions(anchor_sid, 200)`; dispatched via `run.io_bound`; off-loop guard `test_vs_lookup_not_on_event_loop` PASSES (load-bearing) |
| `web/pages/joins_lab.py` | `shared.fjms_service.get_fjms_service` | `run.io_bound(run_enrich_core)` | VERIFIED | `run_enrich_core` closure calls `get_fjms_service(thread_safe=True).get_measurement_summaries_batch(sys_ids)`; dispatched via `run.io_bound`; guard `test_enrichment_batch_not_on_event_loop` PASSES (load-bearing) |
| `web/pages/joins_lab.py` | `web.components.compare_modal.create_compare_modal` | `_open_compare(cand)` receiving full candidate | VERIFIED | `_open_compare` constructs `anchor_cand` WITHOUT `fl_id` (CR-03 fix verified: no `fl_id` in Candidate dataclass); passes full clicked `cand` as `initial_candidate`; passes `_filtered_candidates` as `filtered_candidates` |
| `web/components/candidate_grid.py` | `shared.joins_lab.badge_and_tooltip` | Single badge decision per card/row | VERIFIED | Grid cards call `badge_and_tooltip(cand)` at line 650; table rows call it in `_make_table_rows` at line 352; Compare modal imports and calls it in `_fill_candidate` |
| `web/components/compare_modal.py` | `web.components.anchor_viewer.AnchorViewer` | Two fresh independent viewer instances | VERIFIED | File imports `AnchorViewer`; `create_compare_modal` creates two instances — one for anchor pane, one rebuilt per `_fill_candidate`; no `inject_viewer_assets` call in compare_modal.py |
| `web/components/candidate_grid.py` | `open_filter_dialog` call site in joins_lab.py | `on_apply=_on_filter_apply, on_reset=_on_filter_reset` | VERIFIED | CR-02 fix verified: `open_filter_dialog` called with `filter_state, enrichment, enrichment_ready, on_apply=_on_filter_apply, on_reset=_on_filter_reset`; no spurious `candidates=` or `anchor_sys_id=` kwargs; `_on_filter_apply` takes no arguments (matching the dialog's no-arg call contract) |

---

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|--------------------|--------|
| `candidate_grid.py` | `triage` (dict/TriageState) | Page-level `_triage` dict passed as `backing` | Yes — shared object, writes from any path (grid buttons, Compare, bulk bar) are instantly visible | FLOWING |
| `candidate_grid.py` | `enrichment` (dict) | `_enrich_candidates` → `get_measurement_summaries_batch` → fjms_enrichment.db SQLite | Yes — real SQLite query off-loop; gracefully returns `{}` on failure | FLOWING |
| `compare_modal.py` | `filtered_candidates` (list) | `_filtered_candidates` in joins_lab.py, populated by `compute_filtered` after each search/filter | Yes — real search results from `execute_joins_search` pipeline | FLOWING |
| `joins_lab.py` | `_vs_candidates` (list) | `_fetch_vs_candidates` → `get_suggestions` → visual_similarity.db SQLite | Yes — real SQLite query off-loop; returns `[]` gracefully when unavailable | FLOWING |

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| `badge_and_tooltip` importable | `python -c "from shared.joins_lab import badge_and_tooltip; print('OK')"` | OK | PASS |
| `compare_modal` importable headlessly | `python -c "import web.components.compare_modal as m; assert hasattr(m, 'create_compare_modal')"` | OK | PASS |
| Phase-87 invariant | `test_no_raw_storage_access.py` (6 tests) | 6 passed | PASS |
| No module-level `_card_refs` | grep `^_card_refs` in candidate_grid.py | No output | PASS |
| CR-01 fix: `TriageState(backing=_triage)` | grep in joins_lab.py | line 662: `triage_obj = TriageState(backing=_triage)` | PASS |
| CR-02 fix: no spurious kwargs | grep `open_filter_dialog` in joins_lab.py | Lines 707–712: `filter_state, enrichment, enrichment_ready, on_apply, on_reset` | PASS |
| CR-03 fix: no `fl_id` in Candidate build | grep `fl_id=anchor_fl_id` in joins_lab.py | No matches | PASS |
| CR-04 fix: `_make_restyle_fn` present | grep in candidate_grid.py | Line 492: `def _make_restyle_fn(card_refs: dict)` | PASS |
| CR-05 fix: `b_query is None` guard | grep in joins_lab.py | Line 1766: `if b_query is None:` | PASS |
| No `gap-3`/`p-3` in candidate_grid.py | grep `gap-3\|p-3` in candidate_grid.py | No output | PASS |
| No server-side `stop_propagation()` | grep `stop_propagation\(\)` in joins_lab.py | No output | PASS |
| Full test suite | `pytest 12-file suite -q` | 169 passed, 25 xpassed | PASS |
| Render-contract tests | `pytest test_joins_lab_render_contract.py -v` | 22/22 passed | PASS |
| Off-loop guard load-bearing | `test_vs_lookup_not_on_event_loop`, `test_enrichment_batch_not_on_event_loop` | Both PASSED (not skipped) | PASS |

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| CND-03 | 119-02 | Sortable multi-select table view | SATISFIED | `get_table_columns()` 8 cols, `get_table_config()` `selection='multiple'`, `row_key='uid'`; test_candidate_surface.py passes |
| CND-04 | 119-02, 119-04 | Triage Y/Maybe/N keyed by sys_id, reset on re-anchor | SATISFIED | `TriageState` with backing dict; `load_anchor` clears `_triage`; test_candidate_triage.py passes |
| CND-05 | 119-04 | Self-match readout (detect_self_match) | SATISFIED (override) | `detect_self_match` runs; banner intentionally omitted per D-13 user decision (CONTEXT.md). Override accepted. |
| CND-06 | 119-02, 119-04 | Filter candidates (material/dims/mismatch/triage) | SATISFIED | `compute_filtered` with 5 predicates; `open_filter_dialog` wired; test_candidate_filters.py passes |
| CND-07 | 119-02, 119-04 | Bounded rendering (pagination + cap) | SATISFIED | `_PAGE_SIZE=24`; `paginate()` replaces 200-cap as primary bound; test_candidate_pagination.py passes |
| CND-08 | 119-04 | Off-loop batched enrichment, breaker-guarded thumbnails | SATISFIED | `run.io_bound(run_enrich_core)` off-loop; thumbnails via proxy + Oxford fork preserved; off-loop guard passes |
| CMP-01 | 119-03 | Side-by-side Compare modal | SATISFIED | `create_compare_modal` with two AnchorViewer instances; test_compare_modal.py passes |
| CMP-02 | 119-03 | Per-pane zoom and folio navigation (independent) | SATISFIED | `step_pane_page(state, 'anchor', +1)` / `step_pane_page(state, 'candidate', +1)` are independent; test asserts page-6 lookup returns page-6 index |
| CMP-03 | 119-03 | Y/Maybe/N verdict syncs to sys_id-keyed triage | SATISFIED | `record_verdict` writes `triage[cand.sys_id]` and calls `on_verdict(sys_id, v)`; auto-advances via `step_candidate(state, +1)` |
| VSM-01 | 119-04 | 👁 toggle merges look-alikes; tracks anchor sid; explicit states | SATISFIED | `_vs_on`, `_vs_anchor_sid` tracking; `_apply_vs_merge` intersection/union/text-only; re-anchor clears `_vs_candidates`; test_vs_adapter.py passes |
| VSM-02 | 119-01, 119-02, 119-03 | 👁 badge consistent across grid/table/Compare | SATISFIED | `badge_and_tooltip` is the single badge source — imported in card renderer, `_make_table_rows`, and compare_modal `_fill_candidate` |

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `web/components/candidate_grid.py` | 253–256 | Dead `isinstance` branch in `_get_verdict` in `compute_filtered` (both branches call `.get(sys_id)` identically) | Info | No functional impact; a simplified form exists in the card renderer (line 582) but the filter's internal `_get_verdict` retains the dead branch. No behavior change. |

No TBD / FIXME / XXX / blocker-level anti-patterns found in phase-modified files. WR-02 (triage button active-state not updated on Compare-verdict restyle) is documented as accepted partial behavior — border update is the primary feedback mechanism; button fill deferred to Phase 120 full re-render.

---

### Human Verification Required

Six items need live browser testing — see frontmatter `human_verification` section for full detail.

**1. Candidate Grid Visual Layout**
**Test:** Open `/joins-lab`, load anchor, run search — confirm 160×160 image-first cards with triage buttons and Compare button appear.
**Expected:** Grid renders up to 24 cards; each card has large thumbnail, library chip, shelfmark, title, 👁 badge (if via_vs), Y/Maybe/No triage buttons, View in Browse link, Compare fragment button.
**Why human:** NiceGUI render path not exercised by headless tests.

**2. Grid↔Table Triage Consistency and Bulk Triage Bar**
**Test:** Set verdicts in grid, switch to table — verify same verdicts shown; select rows and confirm bulk triage bar appears.
**Expected:** Table shows same verdict glyphs; selecting rows reveals "Mark N selected as: Yes/Maybe/No".
**Why human:** NiceGUI selection events and view-switch rendering require a live browser.

**3. Filter Dialog Enrichment Gate + Apply Behavior**
**Test:** Open Filters — material select disabled initially, then enabled after enrichment; apply filter and verify count changes.
**Expected:** Material multi-select starts disabled with note; after enrichment it populates; Apply re-renders with fewer candidates; page resets to 1.
**Why human:** Enrichment timing and dialog render sequence require a live page.

**4. Compare Modal Per-Pane Independence + Card Restyle After Verdict**
**Test:** Click Compare fragment — anchor and candidate panes navigate independently; record verdict — grid card border updates.
**Expected:** Two independent viewers; folio navigation per-pane; recording Yes updates grid card to green border.
**Why human:** Two-pane AnchorViewer rendering and card restyle after modal verdict require a live browser.

**5. VS Toggle Intersection / Empty-Builder Union Modes**
**Test:** Toggle VS ON with a query — confirm intersection (fewer candidates, each 👁-badged); then clear builder with VS ON — confirm look-alikes render without empty-builder toast.
**Expected:** Intersection: reduced count, 👁 badges. Empty builder + VS ON: pure VS union renders, no "Enter at least one search line" notify.
**Why human:** VS toggle state transitions and F1 empty-builder branch require a live NiceGUI page.

**6. Re-anchor Triage Invalidation + VS Refetch**
**Test:** Triage some candidates, then re-anchor — confirm triage cleared and VS look-alikes refetch.
**Expected:** After re-anchor: no verdict borders; VS loading notice appears briefly if VS was ON.
**Why human:** Re-anchor invalidation + VS refetch sequence requires a live session.

---

### Gaps Summary

No blocker gaps found. All five critical bugs (CR-01..CR-05) have been fixed and verified in the codebase:

- **CR-01 FIXED:** `TriageState(backing=_triage)` confirmed at joins_lab.py:662; render-contract test `TestTriageStateConstructorContract` (6 tests) all pass.
- **CR-02 FIXED:** `open_filter_dialog` call at lines 707–712 has correct signature — no spurious kwargs, includes `on_reset`; `_on_filter_apply` is no-arg; render-contract test `TestOpenFilterDialogCallSiteContract` (5 tests) all pass.
- **CR-03 FIXED:** `_open_compare` builds `anchor_cand = Candidate(sys_id, page, uid, volume_ie, is_anchor_self=True)` — no `fl_id` field; render-contract test `TestCandidateConstructorContract` (4 tests) all pass.
- **CR-04 FIXED:** No module-level `_card_refs` dict; `_make_restyle_fn(card_refs)` factory creates per-render closures; `create_candidate_grid` passes fresh `_render_card_refs={}` per call; render-contract test `TestCardRefsNotModuleGlobal` (4 tests) all pass.
- **CR-05 FIXED:** `if b_query is None:` guard at joins_lab.py:1766 returns `MergeResult(candidates=tuple(_base_snapshot), note='b_query empty…')` before `_merge_globals_web` call; render-contract test `TestComposeEmptyInputContract` (3 tests) all pass.

The only item requiring human judgement is SC-2's self-match banner (accepted via override per D-13 user decision) and the 6 human-verification items above which require a live browser session.

---

_Verified: 2026-06-19_
_Verifier: Claude (gsd-verifier)_

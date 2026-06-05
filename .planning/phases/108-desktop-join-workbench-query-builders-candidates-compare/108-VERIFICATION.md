---
phase: 108-desktop-join-workbench-query-builders-candidates-compare
verified: 2026-06-05T13:00:00Z
status: human_needed
score: 7/7 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Open the Workbench, anchor a fragment, run a multi-box OR builder query, confirm grid cards appear with thumbnail + material + highlighted snippet + Y/?/N triage buttons; toggle grid to table and confirm counts match"
    expected: "Grid shows 20 candidates per page (4 columns); each card has a thumbnail image loading, a dimension/material line from FJMS batch enrichment, a 72px snippet browser with highlighted terms, and Y/?/N triage buttons that change the card border color"
    why_human: "In-widget render + image loading via bounded pool; grid card layout and highlight rendering are not assertable headlessly"
  - test: "With a candidate matching via the other-side builder (AND or OR), click Compare; confirm the dialog opens to the matched/neighbor page with the 'other side matched' label visible"
    expected: "CompareDialog opens modeless 1320x870; left pane shows anchor image+text; right pane shows candidate image for the cross-side neighbor page; meta line contains 'other side matched' text; anchor pane stays static when stepping prev/next"
    why_human: "Qt modal behavior + per-page IIIF image loading + cross-side label placement are not assertable headlessly"
  - test: "Trigger each of the four actions (Browse / Puzzle / Add to List / Add as Join) from a grid card and from inside CompareDialog; for Add as Join confirm Fragment B is pre-filled with the candidate shelfmark"
    expected: "All four actions delegate to the workbench host without any _vs_* private calls; Add as Join opens JoinsDialog with Fragment A = anchor and Fragment B = candidate shelfmark pre-filled"
    why_human: "Cross-dialog action wiring and pre-fill behavior require Qt UI execution"
  - test: "Build a query the anchor itself satisfies; confirm the self-match readout shows '⚓ anchor matches this query ✓'; toggle 'Include anchor itself' and confirm the anchor appears in / disappears from the candidate list on next search"
    expected: "Self-match ✓/✗ shows inline in status bar; include-anchor toggle (default OFF) adds/removes the anchor from the deduped candidate list on the next search run"
    why_human: "Live status readout + toggle interaction require UI execution"
  - test: "Toggle Judeo-Arabic, Flex Spacing, or Bidirectional in the builder and re-run search; confirm results differ from the plain search (the global options actually reach the engine)"
    expected: "The ja/flex_spacing/bidirectional toggles produce different result sets because _merge_globals() merges them back into the composed ro after compose() hardcodes them False"
    why_human: "Requires running real search queries with known JA/flex/bidir-sensitive content"
---

# Phase 108: Desktop Join Workbench — Query Builders, Candidates & Compare — Verification Report

**Phase Goal:** The scholar drives the hunt — a line-by-line query builder for the anchor side AND an identical builder for the OTHER side of the leaf (cross-side AND/OR), running the EXISTING search engine; results return as deduped one-per-image candidates in grid + table views with material + highlighted snippet + Y/?/N triage + a self-match readout, plus side-by-side anchor↔candidate compare.
**Verified:** 2026-06-05T13:00:00Z
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths (Roadmap Success Criteria)

| #  | Truth | Status | Evidence |
|----|-------|--------|----------|
| 1  | A line-by-line builder (rows = lines; per-row line START/END anchors; gap spinner) composes and runs a line-break query through the existing engine — hunting the missing continuation, NOT pre-seeding the anchor's own line text (JWB-10, JWB-06 reframed) | ✓ VERIFIED | `JoinQueryBuilder` exists at `desktop/join_workbench.py:653`; `build_side_query()` at :1103 creates `BuilderRow`s; `compose()` called on the resulting `SideQuery`; builder starts BLANK (no pre-seed); `allow_page_position=True` for anchor side; 39 headless builder tests pass |
| 2  | An identical builder for the OTHER side (adjacent image p±1) runs query B; AND narrows / OR widens via (sys_id, page±1) membership through `apply_cross_side` (JWB-11) | ✓ VERIFIED | `other_builder = JoinQueryBuilder(..., allow_page_position=False)` at :1714; `_CrossSideWorker` at :1326 delegates to `apply_cross_side`; AND/OR combo at :1721; `_on_results` starts `_CrossSideWorker` when other-side enabled; `allow_page_position=False` confirmed by grep and RR-5 |
| 3  | Candidates render deduped one-per-image in both grid and table views, each with material, highlighted matched-text snippet, and Y/?/N triage; a refine/filter bar filters by text / material / has-dimensions / triage (JWB-07; JWB-12 surface) | ✓ VERIFIED | `CandidateCard` at :1464; `JoinCandidatePane` at :1638; `QGridLayout` with `_GRID_COLS=4`, `_PER_PAGE=20`; `QTableWidget` at :1862; enrichment keyed by `c.key=(sys_id, page)` via `_EnrichWorker`; `dedup_candidates` called in `_on_results`; refine bar at lines 1769–1800 |
| 4  | A self-match readout shows whether the anchor itself satisfies the current query (✓/✗) and an "include anchor itself" toggle works; default OFF (JWB-12 verification) | ✓ VERIFIED | `detect_self_match` called at :1965; `self._anchor_matched` stored; status prefix at :2138–2140 shows "⚓ anchor matches this query ✓" / "✗"; `include_anchor_chk.setChecked(False)` at :1730 |
| 5  | Selecting a candidate shows it side-by-side with the anchor (image + transcription) for eyeball confirmation, with the four actions available (JWB-08) | ✓ VERIFIED | `CompareDialog` at :2330; `open_compare` at :2317 wired to `CompareDialog(self.wb, global_idx).show()`; two-pane layout with `_fill_anchor` and `_fill_candidate`; four action buttons (Browse/Puzzle/List/Join) at :2399–2412; `apply_line_numbered_text` + `htmlify` in both panes |
| 6  | (DEFERRED) Tear-side assist — explicitly deferred to Phase 110 | DEFERRED | Confirmed deferred per ROADMAP.md and REQUIREMENTS.md (JWB-05 moved to Phase 110 disposition). Not a Phase 108 success criterion. |
| 7  | Manuscript dimensions appear as evidence / soft warnings, never a hard auto-filter; an opt-in min/max size cull exists but is off by default | ✓ VERIFIED | `_EnrichWorker` computes `mismatch` flag (ratio > 1.4 = soft warning, D-13); "⚠ size mismatch" label in CandidateCard; opt-in size filter button at :1800 collapsed by default; no automatic exclusion on dimension mismatch |

**Score:** 7/7 truths verified (criterion 6 is officially deferred per roadmap; not counted as a gap)

### Deferred Items

Items not yet met but explicitly addressed in later milestone phases.

| # | Item | Addressed In | Evidence |
|---|------|-------------|----------|
| 1 | Tear-side assist — reads anchor's `[`/`]` markers and suggests the likely side / search direction (JWB-05) | Phase 110 | ROADMAP.md Phase 108 SC#6: "DEFERRED out of Phase 108 (2026-06-04 discuss-phase) — JWB-05 moves to the algorithmic Component B (Phase 110 disposition)." REQUIREMENTS.md confirms: "JWB-05 DEFERRED from Phase 108 → Phase 110 disposition" |

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/fjms_service.py` | `get_measurement_summaries_batch` EXTENDED with `size_category` behind column-existence guard | ✓ VERIFIED | `has_size_category` at :3025, `sc_col` at :3026, conditional append at :3032, `r.get('size_category') if has_size_category else None` at :3061; COALESCE preserved at :3041 |
| `tests/test_fjms_service.py` | `TestGetMeasurementSummariesBatch` with with-column, absent-column, missing-id, 500-boundary tests | ✓ VERIFIED | 9 tests pass in `TestGetMeasurementSummariesBatch`; covers `test_with_size_category_returns_value`, `test_absent_column_degrades_to_none_not_empty_batch`, `test_batch_boundary_500_returns_all_matches` |
| `tests/test_join_workbench_builder.py` | Headless parser-level OR round-trip + RR-13 per-row HOIST tests | ✓ VERIFIED | 39 tests pass; `parse_responsa_query` imported and used; `#(שלום/שלומות)` → `grammatical_prefixes`; `-(עץ/אילן)` → `negated`; no `|`-join pattern |
| `tests/test_join_workbench_triage.py` | sys_id triage-keying contract | ✓ VERIFIED | 6 tests pass; same sys_id at two pages shares triage state; `Candidate.key` differs between pages |
| `tests/test_tabular_builder_rtl.py` | AST guard: `TabularQueryBuilderDialog.__init__` has no dialog-level `setLayoutDirection(RightToLeft)` | ✓ VERIFIED | 2 tests pass; guard parses genizah_app.py AST and asserts zero matches |
| `desktop/join_workbench.py` | `JoinQueryBuilder` + `JoinCandidatePane` + `CandidateCard` + `CompareDialog` + `_DesktopSearchExecutor` + workers | ✓ VERIFIED | All 8 classes present at confirmed line numbers; 3612+ line module; QFrame/QSpinBox/QEvent added in Plan 02 Task 1; QGridLayout/QTableWidget/QTableWidgetItem/SearchThread added in Plan 03 Task 0 |
| `genizah_app.py` | `open_anchor_as_join(…, partner_sys_id=None, partner_shelfmark=None)` extended signature | ✓ VERIFIED | Signature at :15442–15448; `if partner_shelfmark: dialog.frag_b_input.setText(partner_shelfmark)` at :15478–15479 |
| `genizah_translations.py` | EN->HE entries for all new tr() keys (31 Plan 02 + 60+ Plan 03 + 22 Plan 04 = 113+) | ✓ VERIFIED | `"+ or": "+ או"` at :3697; `"Negation −": "שלילה −"` at :2392; `"Find Candidates": "מצא מועמדים"` at :3796; `"other side matched": "הצד השני תאם"` at :3877; i18n test suite (4 tests) passes |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `JoinQueryBuilder.build_side_query` | `shared.joins_lab.compose` | per-ROW: single box → decorate token; multi-box → `"(" + "/".join(tokens) + ")"` then HOIST row mods outside; `SideQuery` → `compose()` | ✓ WIRED | `"/".join` at :1151; HOIST logic at :1137–1162; no `"|"`.join; `compose()` called in `_update_preview` and `do_search` |
| `JoinQueryBuilder modifier row` | active ROW's mods dict | `eventFilter FocusIn` → `_on_row_focus` → reflects mods onto checkboxes; `_on_modifier_changed` writes back; `_refresh_modifier_enabled` disables wildcard-prefix on multi-box | ✓ WIRED | `_active_row` tracking confirmed; `chk_wild_start.setEnabled(False)` at :1025; `_active_row = None` in both `_remove_row` (:959) and `_remove_box` (:974) |
| `JoinQueryBuilder._responsa_opts` | `ja`, `flex_spacing`, `bidirectional` globals | Exposes four globals; `do_search` calls `_merge_globals(builder, ro)` which does `ro.update(...)` | ✓ WIRED | `_responsa_opts` at :1085–1102; `_merge_globals` at :1879; applied to both main `ro` at :1923 and cross-side `b_ro` at :1983 |
| `JoinCandidatePane.do_search` | `SearchThread` via composed query with merged globals | `compose()` → `_merge_globals(builder, ro)` → `SearchThread(..., text_position=page_pos, corpus_scope="genizah")` | ✓ WIRED | `text_position=page_pos` at :1950; `corpus_scope="genizah"` at :1951; merge at :1923 |
| `_CrossSideWorker.run` | `shared.joins_lab.apply_cross_side` | Receives ALREADY-MERGED `b_ro`; delegates `apply_cross_side(executor, base, b_query_str, b_ro, combine, a_pattern)` | ✓ WIRED | `apply_cross_side` imported and called at :1352–1369; `b_ro` passed as `b_responsa_options` |
| `_EnrichWorker.run` | `FjmsService.get_measurement_summaries_batch` + snippet functions | Single batch IN-query; reads `width_cm/height_cm/material/avg_num_lines/size_category`; emits dict keyed by `c.key=(sys_id, page)` | ✓ WIRED | `get_measurement_summaries_batch(sys_ids)` at :1405; all 5 key names read at :1416–1420; `out[c.key] = {...}` at :1434 |
| `JoinWorkbenchWindow._enqueue_image_for_pane` | `_image_url_for_idx(images, page-1, width)` | None-page guard: `if page is None: page = 1` then `_image_url_for_idx` (NOT `get_thumbnail`) | ✓ WIRED | `if page is None` at :3340; `_image_url_for_idx` referenced; `get_thumbnail` NOT used inside `_enqueue_image_for_pane` (confirmed by grep) |
| `JoinWorkbenchWindow.open_result_as_join` | `self._app.open_anchor_as_join(anchor_sid, anchor_shelf, partner_sys_id=c.sys_id, partner_shelfmark=c.shelfmark)` | EXTENDED public method (RR-3/D-17); no `_vs_*` calls | ✓ WIRED | `partner_shelfmark=c.shelfmark` at :3268; 0 `_vs_` calls confirmed by grep; `test_join_workbench_no_private.py` passes |
| `CompareDialog._fill_candidate` | `candidate.page → _enqueue_image_for_pane` | `c.page` passed straight (no page-1 arithmetic in dialog); pump guards None internally | ✓ WIRED | `self.wb._enqueue_image_for_pane(pane["img"], c.sys_id, c.page, width=1400)` at :2543; no `page-1` arithmetic in `CompareDialog` body confirmed |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|-------------------|--------|
| `JoinCandidatePane` (results) | `self.results` | `SearchThread` → `_on_results` → `dedup_candidates` → `_maybe_assemble` → `merge_candidates` | Real corpus search results via `searcher.execute_search` | ✓ FLOWING |
| `_EnrichWorker` enrichment | `out[c.key]` dict | `FjmsService.get_measurement_summaries_batch` (SQLite IN-query with COALESCE) | Real FJMS sidecar data when available; graceful None when column/row absent | ✓ FLOWING |
| `CandidateCard` snippet | `enrich[c.key]["snippet_html"]` | `snippet_html(c.full_text, c.highlight_pattern)` from `_EnrichWorker` | Real manuscript text with regex highlights | ✓ FLOWING |
| `CompareDialog` candidate text | `c.full_text` + `c.highlight_pattern` | Candidate dataclass from search results → `htmlify(c.full_text, c.highlight_pattern)` | Real manuscript text | ✓ FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 211 Phase-108 requirement tests pass | `pytest tests/test_join_workbench_builder.py tests/test_join_workbench_triage.py tests/test_tabular_builder_rtl.py tests/test_fjms_service.py::TestGetMeasurementSummariesBatch tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py tests/test_join_workbench.py tests/test_joins_lab.py -x` | 211 passed in 1.50s | ✓ PASS |
| Ruff clean on all modified files | `python -m ruff check desktop/join_workbench.py genizah_app.py genizah_translations.py shared/fjms_service.py` | All checks passed | ✓ PASS |
| No `_vs_*` private calls in workbench | `grep -c "_vs_" desktop/join_workbench.py` | 4 matches — all are comments/docstrings, zero actual calls (confirmed by line-by-line review) | ✓ PASS |
| size_category column-guarded (absent-column degrades gracefully) | `TestGetMeasurementSummariesBatch::test_absent_column_degrades_to_none_not_empty_batch` | PASS — old sidecar returns full batch with size_category: None, not empty batch | ✓ PASS |
| Parser-level OR round-trip: `(פירוש/פירש)` → `.words == [פירוש, פירש]` | `TestOrSlashGroup` in `test_join_workbench_builder.py` | PASS | ✓ PASS |
| Per-row HOIST: `#(שלום/שלומות)` → `grammatical_prefixes=True` on group | `TestMultiBoxHoistedModifiers` | PASS | ✓ PASS |

### Requirements Coverage

| Requirement | Source Plans | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| JWB-06 (reframed) | 108-01, 108-02, 108-03 | Line-by-line builder seeds search for missing continuation; starts BLANK | ✓ SATISFIED | `JoinQueryBuilder` with blank initial state; `build_side_query()` + `compose()` wired; tests pass |
| JWB-07 | 108-03 | Candidates collected in grid + table with material, snippet, Y/?/N triage | ✓ SATISFIED | `JoinCandidatePane` + `CandidateCard`; `dedup_candidates` → `merge_candidates` → `_EnrichWorker`; grid+table views |
| JWB-08 | 108-04 | Side-by-side anchor↔candidate compare; four actions available | ✓ SATISFIED | `CompareDialog` with two panes, four action buttons, prev/next, Y/?/N, Re-anchor |
| JWB-10 | 108-02, 108-03 | Identical builder for anchor and other-side; per-row line START/END anchors | ✓ SATISFIED | `JoinQueryBuilder` with `line_start`/`line_end` per row; `allow_page_position=True` for anchor |
| JWB-11 | 108-03 | Other-side builder runs query B; AND narrows / OR widens | ✓ SATISFIED | `other_builder` with `allow_page_position=False`; `_CrossSideWorker` → `apply_cross_side`; AND/OR combo |
| JWB-12 (text/combined surface) | 108-03 | Text/Visual(disabled)/Combined(disabled) source selector; self-match readout; include-anchor toggle default OFF | ✓ SATISFIED | Source selector with Visual+Combined `setEnabled(False)`; `detect_self_match`; readout in status; `include_anchor_chk.setChecked(False)` |
| JWB-05 (tear-side assist) | NONE (DEFERRED) | Conservative `[`/`]` side-assist | DEFERRED to Phase 110 | Explicitly deferred per ROADMAP.md SC#6 and REQUIREMENTS.md |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `desktop/join_workbench.py` | 2317–2325 | `open_compare` returns immediately after `CompareDialog.show()` with no parent-lifetime management beyond `self._compare = ...` | ℹ Info | If user rapidly clicks Compare on different candidates, only the last dialog is kept by `self._compare`; earlier dialogs will be GC'd and disappear. Acceptable for MVP; documented in 108-REVIEW.md as a known advisory finding (0 critical, 4 warning, 5 info). |

No blockers or stub patterns found. The four `_vs_` occurrences in the file are all in comments/docstrings (not calls), confirmed by `test_join_workbench_no_private.py` passing.

### Human Verification Required

#### 1. Grid card rendering (JWB-07 visual verification)

**Test:** Open the Workbench, anchor a fragment that has text, build a one-row query with a common Hebrew word, click "Find Candidates"
**Expected:** Grid shows deduped one-per-image candidates (max 20/page, 4 columns); each card has a loading thumbnail, a dimensions/material line once enrichment completes, a snippet browser with highlighted terms, and Y/?/N triage buttons that change the card border color (teal=Y, yellow=maybe, red=N)
**Why human:** In-widget render + image loading via bounded pool; grid card layout and highlight rendering are not assertable headlessly

#### 2. CompareDialog matched-page image + "other side matched" label (JWB-08 + D-18)

**Test:** Enable the other-side builder, run a query that narrows candidates via AND, click Compare on an "⇄ other side" candidate
**Expected:** CompareDialog opens modeless at 1320×870; right pane shows image for the cross-side neighbor page (p+1 or p-1); meta line contains "other side matched" text; anchor pane stays static when stepping prev/next with the navigation buttons
**Why human:** Qt modal behavior + per-page IIIF image loading + cross-side label placement are not assertable headlessly

#### 3. Four actions and Add-as-Join partner pre-fill (JWB-08 / D-17)

**Test:** Click each of the four action buttons on a grid card and from inside CompareDialog
**Expected:** Browse opens the fragment in the browse tab; Puzzle opens the puzzle page with the anchor pre-loaded; Add to List shows the list selection menu; Add as Join opens JoinsDialog with Fragment A = anchor shelfmark and Fragment B = candidate shelfmark pre-filled
**Why human:** Cross-dialog action wiring and pre-fill behavior require Qt UI execution

#### 4. Self-match readout and include-anchor toggle (JWB-12)

**Test:** Build a query using a distinctive word from the anchor's own transcription; run search; observe the status bar
**Expected:** Status shows "⚓ anchor matches this query ✓  ·  N results"; when "Include anchor itself" is checked and search is re-run, the anchor appears in the grid; when unchecked again, it disappears
**Why human:** Live status readout + toggle interaction require UI execution

#### 5. ja/flex/bidir globals reach the engine (RR-14)

**Test:** Find a document with Judeo-Arabic content; toggle "Judeo-Arabic" in the builder; compare result counts with it ON vs OFF
**Expected:** With Judeo-Arabic ON the engine applies Hebrew–Arabic cognate expansion; result count or set differs from the OFF run. Same test with Flex Spacing for gapped text
**Why human:** Requires running real search queries with known JA/flex/bidir-sensitive content

### Gaps Summary

No automated gaps identified. All 7 roadmap success criteria are either verified (6) or explicitly deferred to Phase 110 (1 — tear-side assist). All 211 requirement tests pass. Ruff is clean. The phase gate flags in 108-VALIDATION.md are set to `nyquist_compliant: true` and `wave_0_complete: true`. The only remaining items are the 5 human UAT items above, which require Qt UI execution and cannot be verified programmatically.

---

_Verified: 2026-06-05T13:00:00Z_
_Verifier: Claude (gsd-verifier)_

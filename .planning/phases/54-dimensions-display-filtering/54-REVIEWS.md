---
phase: 54
reviewers: [gemini, codex]
reviewed_at: 2026-03-27T12:40:00Z
plans_reviewed: [54-01-PLAN.md, 54-02-PLAN.md, 54-03-PLAN.md, 54-04-PLAN.md]
rounds: 4
---

# Cross-AI Plan Review — Phase 54

## Round 4: Gemini + Codex Review (Revised Plans 54-03, 54-04)

### Gemini Review (gemini-2.5-pro)

#### Summary

The implementation plans for Wave 1 (Backend) and Wave 2 (UI) are technically sound, highly detailed, and demonstrate a strong commitment to addressing the architectural concerns raised in Round 3. The strategy of separating pre-search and post-search states in both the Web and Desktop applications directly resolves the most critical risk of state loss and coupling. The use of a batch lookup mechanism for post-search measurements is an efficient way to handle "sidecar" data that isn't indexed in the primary search engine (Tantivy). The backend testing plan is comprehensive, covering edge cases like unit normalization and NULL exclusion.

#### Concerns

**Round 3 Resolution Audit:**
- Concerns 1, 2, 3 (State Coupling/Loss): **RESOLVED.** Plan 54-04 explicitly defines separate storage for pre- and post-search filters.
- Concern 6 (Performance): **RESOLVED.** Move from on_change to blur/editingFinished prevents excessive recomputations.
- Concern 7 (Migration Risk): **RESOLVED.** PRAGMA existence checks ensure idempotency and stability.

| # | Severity | Concern | Action |
|---|----------|---------|--------|
| 1 | MEDIUM | Material multi-select query — backend `measurement_material` param must handle `list[str]` with IN clause, not single string | Executor: verify IN clause implementation |
| 2 | MEDIUM | Normalization logic consistency — min > max normalization only specified in UI, backend could receive inverted ranges | Executor: add backend guard clause |
| 3 | LOW | UI automation coverage — Plan 54-04 lacks specific UI verification steps | Noted |
| 4 | LOW | Material label translations — ensure the five material types are translated, not just section header | Executor: verify |

#### Suggestions
- Add backend guard clause for reversed min/max pairs
- Ensure SQL IN clause behavior for material multi-select
- Add visual indicator on Apply button during processing
- Verify unit labels match DB schema (mm vs cm)

#### Execution Readiness
**GO**

---

### Codex Review (GPT-5.4 / o3)

#### Plan 54-03: Backend

##### Summary
Mostly solid and much better scoped than the prior round. Splitting schema/import work from service/query work is the right order. The proposed `get_measurement_summaries_batch()` closes a real gap for DIM-03. Plan addresses several earlier concerns well: wave ordering explicit, batch lookup called out, dedup called out, service-level tests broad enough.

##### Concerns

| # | Severity | Concern | Action |
|---|----------|---------|--------|
| 1 | MEDIUM | Row-factory concern not fully resolved — `dict(row)` insufficient if `row` is already a tuple (intermittent issue) | Executor: fall back to cursor.description or positional unpacking |
| 2 | MEDIUM | Index coverage incomplete — new filters add line count, line height, text density, material but no indexes planned for these | Executor: add indexes for avg_line_height_mm, avg_text_density, material |
| 3 | MEDIUM | D-19 normalization not owned in backend tests — reversed input pairs (min > max) not tested | Executor: add reversed bounds test |
| 4 | LOW | Test fixture schema — adding avg_line_height_mm will break current inserts unless existing fixture updated | Executor: update fixture |
| 5 | LOW | Width/height semantics — should state exactly which summary column drives each bound | Executor: document in code comments |

##### Suggestions
- Add shared normalization helper for numeric ranges
- Make batch method robust to tuple rows via cursor.description fallback
- Add indexes for new filter columns
- Add reversed min/max test + intersection test with non-measurement filter
- Return normalized summary shape from batch method

##### Execution Readiness
**CONDITIONAL GO**

---

#### Plan 54-04: UI

##### Summary
Directionally correct on desktop, partially convincing on web. Desktop replan addresses pre/post coupling with separate `_post_measurement_filters` dict. Web plan underspecified for rerender persistence: current post-search rendering spread across `apply_filters()`, domain exclusion, printed filtering, word-exclusion, history restore, and staged enrichment — "read from state not widgets" only fixes one code path.

##### Concerns

| # | Severity | Concern | Action |
|---|----------|---------|--------|
| 1 | HIGH | Web rerender persistence — "read from state, not widgets" only fixes apply_filters path. Domain exclusions, printed toggles, history restore, staged enrichment all re-render results without composing measurement post-filters | Executor: route ALL rerender paths through shared measurement post-filter |
| 2 | MEDIUM | Desktop persistence path wrong — app uses `_save_session()` + `shared/session_persistence.py`, NOT QSettings for this flow | Executor: use existing session machinery |
| 3 | MEDIUM | Material label ambiguity — existing printed/non-printed controls vs new measurement material; plan separates backend keys but not user-facing labels | Executor: use "Material (measured)" or "Physical Material" label |
| 4 | MEDIUM | Desktop batch fetch race — `_result_measurement_map` populated after search but before rows rendered; missing rows handled as "exclude" could oscillate | Executor: handle missing as "pending" during fetch |
| 5 | LOW | Material values hardcoded — brittle if DB gets new values | Acceptable short-term |
| 6 | LOW | UI-state tests still open — no concrete test for rerender persistence, separate state, Enter-to-apply | Consider adding |

##### Suggestions
- Create one shared post-search filtering pipeline on web; route every rerender through it
- Persist desktop post-search measurement state through existing session/history machinery
- Add shared pure-Python matcher for post-search measurement rules (web + desktop identical semantics)
- Add targeted UI/state tests: rerender persistence, pre/post independence, session restore, Enter-to-apply

##### Execution Readiness
**BLOCK** — Web rerender integration point must be made explicit before execution

---

## Consensus Summary

### Agreed Strengths (both reviewers)
- Backend plan (54-03) is well-structured with correct wave ordering and comprehensive tests
- Pre/post state separation architecture is sound in both apps
- Batch lookup pattern for post-search is efficient and correct
- Round 3 concerns 5-9 are genuinely resolved

### Agreed Concerns (both reviewers — highest priority)

| # | Severity | Concern | Gemini | Codex |
|---|----------|---------|--------|-------|
| 1 | HIGH | Web rerender persistence — measurement post-filters only applied in apply_filters(), not in other rerender paths (domain exclusion, printed toggle, history restore, staged enrichment) | Implicit (noted normalization gap) | Explicit BLOCK |
| 2 | MEDIUM | Material multi-select contract — backend `measurement_material` param must accept list and use IN clause | Both flagged |
| 3 | MEDIUM | Min > max normalization ownership — not tested in backend, only assumed in UI | Both flagged |
| 4 | MEDIUM | Missing indexes for new filter columns (line height, text density, material) | Codex only but valid |
| 5 | MEDIUM | Desktop session persistence path — plan says QSettings but app uses _save_session() | Codex only but valid |

### Divergent Views

| Topic | Gemini | Codex |
|-------|--------|-------|
| Overall readiness | GO (all concerns are executor-level) | BLOCK on UI (web rerender integration), CONDITIONAL GO on backend |
| Severity of web rerender gap | Not flagged as blocking | Flagged as HIGH/BLOCK — multiple code paths skip measurement filters |
| Row factory resilience | Not mentioned | MEDIUM — dict(row) insufficient if row is tuple |

### Remaining Action Items for Planner

1. **Web rerender integration (HIGH)** — Plan 54-04 Task 1 must specify that ALL result-rendering code paths (domain exclusion toggle, printed filter toggle, history restore, staged enrichment completion) compose measurement post-filters from `search_state.post_filter_*`. Not just `apply_filters()`.
2. **Material IN clause (MEDIUM)** — Plan 54-03 Task 2 already has `IN ({ph})` in the action block. Verify this is reflected in tests (test_filter_measurement_material should test multi-value).
3. **Backend min>max normalization (MEDIUM)** — Add guard clause in `get_filter_sys_ids` or add explicit test that reversed bounds still work.
4. **New column indexes (MEDIUM)** — Add indexes for `avg_line_height_mm`, `avg_text_density`, `material` in import script.
5. **Desktop session persistence (MEDIUM)** — Plan 54-04 Task 2 should reference `_save_session()` / `session_persistence.py`, not QSettings.

---

## Round 3: Codex Review (Plan 54-03 — Dimension Filtering)

<details>
<summary>Expand Round 3 review (superseded by Round 4)</summary>

### Summary

The plan is directionally strong: it extends the shared filtering contract in shared/fjms_service.py, keeps pre-search filtering in the service layer, and uses batch lookup for post-search filtering instead of row-by-row queries. That architecture is the right shape for DIM-02 and DIM-03. The main problem is that the plan is more complete for backend and web than for desktop post-search, and a few state/performance details are underspecified enough that the implementation could ship with parity gaps or filters that silently disappear after re-render.

### Concerns

| # | Severity | Concern | Action |
|---|----------|---------|--------|
| 1 | HIGH | Desktop DIM-03 underspecified — reuses pre-search values for post-search instead of separate post-search UI | Executor: add explicit desktop post-search measurement state, separate from pre_search_filters |
| 2 | HIGH | Web post-search filters may be lost on re-render (enrichment/pagination) — only applied inside apply_filters() | Executor: persist post-filter state and fold into normal render path |
| 3 | HIGH | Desktop uses shared pre_search_filters for post-search, coupling two concepts | Executor: split pre/post measurement state |
| 4 | MEDIUM | Material parity gap — web multi-select vs desktop single QComboBox; hardcoded "Vellum" may not match DB "Parchment" | Executor: use multi-select in both, source options from DB or shared list |
| 5 | MEDIUM | Tasks called autonomous but Task 2/3 depend on Task 1's API | Executor: execute Task 1 first, then Task 2/3 |
| 6 | MEDIUM | Pre-search count recomputation on every numeric input change — DB churn with 10 inputs | Executor: debounce or apply on blur/Enter |
| 7 | MEDIUM | Migration ordering risk — UI ships before avg_line_height_mm exists | Executor: graceful no-op when column missing |
| 8 | MEDIUM | Row factory instability in fjms_service.py may affect batch method | Executor: defensive dict(row) access |
| 9 | LOW | Batch lookup should deduplicate sys_ids | Executor: dedupe before query |
| 10 | LOW | No UI-state tests (chip removal, clear, session restore, Enter-to-apply) | Consider adding |

### Execution Readiness
**CONDITIONAL GO** — Ready with clarifications addressed by executor

</details>

---

## Round 2: Codex Review (Revised Plans 54-01, 54-02)

<details>
<summary>Expand Round 2 review (superseded by Round 3)</summary>

### Concern Resolution Audit

| # | Original Concern | Verdict | Notes |
|---|---|---|---|
| 1 | Build path discipline | PARTIALLY RESOLVED | One canonical script named, but ordering enforced by comments/process rather than code. Old exporter still exists as fallback. |
| 2 | Summary table semantics | PARTIALLY RESOLVED | Computed summary fixed with min/max pairs. Catalog summary still uses separate MAX(SizeX_cm)/MAX(SizeY_cm) — can synthesize phantom catalog size across catalogers. |
| 3 | Flag exclusion at aggregation time | RESOLVED | Flagged rows excluded in aggregation SQL with tests. |
| 4 | Catalog_Sizes audit | RESOLVED | Audit covers match rate, unmatched samples, duplicates, final distinct AlmaId counts. |
| 5 | DIM-01 surface coverage | PARTIALLY RESOLVED | Scope narrowed to browse-only, search results excluded. Desktop task still doesn't clearly name browse-tab button/wiring. |
| 6 | AlmaId float validation | PARTIALLY RESOLVED | Validation step exists, but "abort if 0/10 match" is weak guard — catches total failure, not partial precision drift. |
| 7 | Performance design | RESOLVED | Streaming reads, batching, per-table transactions. |
| 8 | InnerSizeX backward compat | RESOLVED | Schema and dict keys address existing consumers. |
| 9 | Old-sidecar graceful degradation | RESOLVED | Try/except per table + tests. |
| 10 | has_measurements() semantics | RESOLVED | Checks 3 tables with short-circuit. |
| 11 | Blank_Images in dialog | RESOLVED | Surfaced in get_measurements() + dedicated dialog section. |
| 12 | Lazy loading / async | RESOLVED | Web: run.io_bound. Desktop: lazy fetch + cache. |
| 13 | Desktop translations | RESOLVED | tr() for all labels. |
| 14 | HTML escaping | RESOLVED | html.escape() on QTextBrowser output. |
| 15 | blank_images in schema summary | RESOLVED | has_blank_images + blank_image_count columns. |

**Score: 11/15 RESOLVED, 4/15 PARTIALLY RESOLVED, 0/15 UNRESOLVED**

### Execution Readiness
**CONDITIONAL GO** — Ready with 3 clarifications

</details>

---

## Round 1: Codex Review (Original Plans)

<details>
<summary>Expand Round 1 review (superseded by Round 2)</summary>

### Round 1 Risk: MEDIUM-HIGH

Plan 54-01: 9 concerns (4 HIGH, 4 MEDIUM, 1 LOW)
Plan 54-02: 8 concerns (3 HIGH, 4 MEDIUM, 1 LOW)

</details>

---

*Phase: 54-dimensions-display-filtering*
*Review rounds: 4 (R1: codex original, R2: codex revised 01/02, R3: codex 03, R4: gemini+codex revised 03/04)*

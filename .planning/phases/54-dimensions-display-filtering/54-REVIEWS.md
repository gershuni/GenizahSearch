---
phase: 54
reviewers: [codex]
reviewed_at: 2026-03-27T10:30:00Z
plans_reviewed: [54-01-PLAN.md, 54-02-PLAN.md, 54-03-PLAN.md]
rounds: 3
---

# Cross-AI Plan Review — Phase 54

## Round 3: Codex Review (Plan 54-03 — Dimension Filtering)

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

### Suggestions

- Split pre-search and post-search measurement state explicitly in both apps
- Normalize material options from DB values or shared canonical list
- Integrate web measurement post-filters into persistent render pipeline
- Make sidecar schema update a hard prerequisite or graceful no-op
- Cache measurement summaries per search generation
- Add UI-state tests for clear/reset/session restore

### Risk Assessment

**HIGH** — Core architecture sound but desktop DIM-03 and web post-filter state need tightening before execution.

### Execution Readiness

**CONDITIONAL GO** — Ready with these clarifications addressed by executor:
1. Separate pre/post measurement state in both apps
2. Desktop post-search: explicit controls or clear reuse contract
3. Web post-filters survive re-renders
4. Material options from DB, not hardcoded
5. Graceful no-op for missing avg_line_height_mm column

---

## Round 2: Codex Review (Revised Plans 54-01, 54-02)

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

### New Concerns (Round 2)

- **HIGH:** Import writes directly into fjms_enrichment.db in place. No temp-DB build-and-swap or rollback plan — interrupted import could leave sidecar half-migrated.
- **MEDIUM:** No sidecar meta version/build metadata update after adding measurement tables. Harder to detect stale sidecars.
- **MEDIUM:** Desktop implementation details skew toward reading-desk controls (btn_rd_measurements, btn_compact_measurements). Browse-tab button should be named just as concretely.
- **MEDIUM:** AlmaId validation (10-row sample, "0/10 fail" threshold) gives limited confidence for a large migration.

### Risk Assessment

**MEDIUM** (down from MEDIUM-HIGH in Round 1)

Major architectural and UX gaps are mostly closed. Remaining risk is operational: migration safety, sidecar versioning, one desktop browse ambiguity.

### Execution Readiness

**CONDITIONAL GO** — Ready with 3 clarifications:
1. Make import safe: build to temp tables and swap on success
2. Update meta/sidecar versioning for measurement-capable build
3. Explicitly name desktop browse-surface button wiring

---

## Round 1: Codex Review (Original Plans)

<details>
<summary>Expand Round 1 review (superseded by Round 2)</summary>

### Plan 54-01 Concerns (Round 1)

- **HIGH:** Build path — two scripts with undefined ordering
- **HIGH:** Summary table MAX creates phantom dimensions
- **HIGH:** Flag exclusion described as display-only rule
- **HIGH:** Catalog_Sizes shelfmark→AlmaId needs audit plan
- **MEDIUM:** str(int()) may not catch rounded floats
- **MEDIUM:** No explicit performance design for 1.5M rows
- **MEDIUM:** InnerSizeX backward compat unclear
- **MEDIUM:** No old-sidecar graceful degradation
- **LOW:** blank_images inconsistency

### Plan 54-02 Concerns (Round 1)

- **HIGH:** DIM-01 coverage incomplete — no search result surfaces
- **HIGH:** Desktop scope internally inconsistent
- **HIGH:** Lazy loading not explicit
- **MEDIUM:** Blank_Images not in dialog
- **MEDIUM:** has_measurements() semantics undefined
- **MEDIUM:** Empty/error states underspecified
- **MEDIUM:** Dialog scroll/performance for large datasets
- **MEDIUM:** Desktop translations not mentioned
- **LOW:** HTML escaping discipline

### Round 1 Risk: MEDIUM-HIGH

</details>

---

## Consensus Summary

### Resolution Progress

Round 1 → Round 2: 15 concerns raised, 11 fully resolved, 4 partially resolved, 4 new (lower severity).

### Remaining Action Items for Executor

1. **Import safety** (HIGH) — Use DROP TABLE IF EXISTS + CREATE TABLE pattern (already implied by plan). Consider wrapping all 5 table imports in a single outer transaction.
2. **Catalog summary phantom sizes** (MEDIUM) — Document that catalog_width_cm/catalog_height_cm are MAX across catalogers, not from a single source. Acceptable for Phase 55 filtering (ranges are still valid bounds).
3. **Sidecar versioning** (MEDIUM) — Add a version comment or meta row after measurement import completes.
4. **Desktop browse clarity** (MEDIUM) — The btn_rd_measurements IS the browse-tab button (reading desk = browse tab in desktop architecture). Reviewer may not know this.

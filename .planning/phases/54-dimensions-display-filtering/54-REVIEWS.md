---
phase: 54
reviewers: [codex]
reviewed_at: 2026-03-26T20:00:00Z
plans_reviewed: [54-01-PLAN.md, 54-02-PLAN.md]
---

# Cross-AI Plan Review — Phase 54

## Codex Review

### Plan 54-01: Import FIST Measurement Data

**Summary**

This is the stronger of the two plans. It identifies the right source sheets, the right join keys, and the right long-term foundation for Phase 55. It also fits the current architecture well: `fjms_enrichment.db` is already the sidecar for browse/search enrichment, and `shared/fjms_service.py` already exposes structured, backward-compatible getters. The main weaknesses are around migration discipline, summary-table semantics, and making sure the "foundation for filtering" is based on trustworthy data rather than just convenient aggregates.

**Strengths**

- Uses the user-approved source of truth and replaces the known-bad mixed-unit `catalog_sizes` data with normalized cm values.
- Separates image-level and manuscript-level storage, which is the right model for both UI display and later filtering.
- Recognizes the two real implementation traps up front: `Catalog_Sizes` needing a join chain to `AlmaId`, and Excel numeric `AlmaId` handling.
- Keeps compatibility in mind by preserving existing `get_catalog_detail()` output keys even if the DB column names change.
- Plans indexes on summary columns, which is exactly what Phase 55 will need.
- Includes tests early instead of treating the import as a one-off script.

**Concerns**

- **HIGH:** The rebuild/migration path is underspecified. The repo already has a single sidecar export pipeline in `scripts/export_fist_enrichment.py`; this plan mentions both a new `import_measurements.py` and edits to the export script, but it does not define one canonical build path. That risks a sidecar that is only correct if two scripts are run in the right order.
- **HIGH:** `manuscript_measurements` needs tighter semantics. "MAX dimensions" can synthesize a width from one image and a height from another, producing a size no physical fragment ever had. That is risky both for DIM-01 summary display and especially for Phase 55 filtering.
- **HIGH:** Flag exclusion is described mainly as a display rule. If flagged rows still flow into `manuscript_measurements` or indexed filter columns, Phase 55's foundation will be contaminated even if the dialog hides them.
- **HIGH:** `Catalog_Sizes` resolution from shelfmark to `AlmaId` needs an audit plan, not just a join plan. Shelfmarks can be ambiguous or variant-heavy; without match-rate reporting and duplicate handling, silent drops/duplications are likely.
- **MEDIUM:** `str(int(value))` may not be sufficient protection for 18-digit Excel IDs unless the source values are validated against another trustworthy join key. It prevents `.0` artifacts, but it does not by itself prove the float was not already rounded.
- **MEDIUM:** The import is large enough to need explicit performance design. Reading ~1.5M spreadsheet rows without streaming/batching/transaction discipline could be slow or memory-heavy.
- **MEDIUM:** Backward compatibility is only partially covered. The current service/tests and catalog dialog still assume `catalog_sizes` has `InnerSizeX/InnerSizeY`; the plan mentions new outer-dimension columns but does not say whether the inner-dimension fields remain, become nullable, or are retired cleanly.
- **MEDIUM:** Old-sidecar graceful degradation is not mentioned. `shared/fjms_service.py` is intentionally tolerant of missing tables; the new measurements APIs should follow that pattern.
- **LOW:** The table list is slightly inconsistent: `blank_images` is described in the task steps but not in the earlier schema summary.

**Suggestions**

- Define one reproducible build path: either fold measurements export into `scripts/export_fist_enrichment.py` or make that script call the new importer as a final step. Avoid a "run A, then maybe B" workflow.
- Make the summary table explicit and filter-safe. Prefer fields like `max_page_width_cm`, `max_page_height_cm`, `min_page_width_cm`, `min_page_height_cm`, `catalog_width_cm`, `catalog_height_cm`, and a `summary_basis`/`source_count`, rather than a generic "width/height".
- Apply flag exclusion at summary-generation time, not just at render time.
- Add import audit outputs and tests for:
  - unmatched `Catalog_Sizes` rows
  - duplicated shelfmark matches
  - `FGP` rows missing `AlmaId`
  - manuscripts with only blank-image measurements
  - old sidecars where measurement tables do not exist
- Build into temp tables or a temp DB and swap only after success, so a failed import does not leave a half-migrated sidecar.
- Call out required indexes explicitly: `FGP`, `AlmaId`, and the summary filter columns.
- Validate a sample of Excel-derived `AlmaId` values against the `FGP -> Extra_Info -> AlmaId` path or the FIST join path before trusting them broadly.

**Risk Assessment**

**MEDIUM-HIGH.** The design direction is good, but this is a foundational data migration touching a shared sidecar and future filtering behavior. If the build path, summary semantics, and exclusion rules are tightened, risk drops a lot; without that, Phase 55 could inherit hard-to-see data integrity bugs.

---

### Plan 54-02: Web + Desktop Measurements Dialog

**Summary**

This plan is well aligned with existing UI patterns: the web already uses chip buttons plus dialogs in browse, and the desktop already uses lazy-loaded FJMS dialogs. The problem is completeness. As written, it looks likely to deliver a browse-only feature, while DIM-01 explicitly says dimensions should appear in browse and result views across web and desktop. It also needs more precision around lazy loading, empty states, and how large per-image datasets are rendered without blocking.

**Strengths**

- Reuses established dialog patterns instead of inventing a new UI architecture.
- Keeps the button conditional, which matches the current browse enrichment model and avoids clutter.
- Uses a blocking visual checkpoint, which is the right call for a feature whose value is mostly presentation and clarity.
- Separates summary, catalog sizes, and computed measurements, which mirrors the data model well.
- Keeps the heavy detail behind a dialog instead of expanding the main browse panel.

**Concerns**

- **HIGH:** DIM-01 coverage appears incomplete. The plan does not address search-result surfaces on web or desktop, even though the requirement says "browse and result views".
- **HIGH:** Desktop scope is internally inconsistent. The prose says "desktop dialog + browse button", but the concrete task bullets only mention `btn_rd_measurements` and `btn_compact_measurements`; they do not clearly include the browse-tab button equivalent to the existing browse catalog button.
- **HIGH:** Lazy loading strategy is not explicit enough. The current browse flow only fetches lightweight FJMS metadata in background enrichment; if `get_measurements()` is called synchronously during dialog creation, large manuscripts could stall the UI.
- **MEDIUM:** `Blank_Images` is not accounted for in the dialog structure. Those rows seem important for fragments without text blocks, but the plan only names summary, catalog sizes, and computed measurements.
- **MEDIUM:** `has_measurements()` semantics are not defined. If it only checks `manuscript_measurements`, manuscripts with catalog-only or blank-image-only data may incorrectly lose the button.
- **MEDIUM:** Empty/error states are underspecified. There will be manuscripts with partial measurement coverage, all-computed rows excluded by flags, or only material/DPI data.
- **MEDIUM:** The dialog could become very large for manuscripts with many image rows; there is no mention of truncation, grouping collapse, or a scroll/performance strategy beyond following the current pattern.
- **MEDIUM:** Translation scope is incomplete. Web translations are mentioned explicitly, but the desktop translation surface is not.
- **LOW:** New measurement strings will flow into HTML/QTextBrowser surfaces; the plan should explicitly preserve escaping discipline, especially since this project recently fixed FJMS HTML injection issues.

**Suggestions**

- Expand the plan to explicitly cover all DIM-01 surfaces:
  - web browse
  - web search results
  - desktop browse
  - desktop result dialog / result view
- Define lazy loading per surface:
  - use `has_measurements()` only for button visibility
  - fetch full measurement payload only on click
  - keep the fetch off the UI thread / event loop
- Add a dedicated `Blank Images` section or merge it into computed/image-level records with a clear "no text block" label.
- Define `has_measurements()` as "any usable catalog, computed, blank-image, or summary measurement exists after quality rules," not just "summary row exists".
- Add visual checkpoint cases for:
  - only catalog sizes
  - only blank images
  - only flagged computed rows
  - mixed multi-source catalog sizes
  - Hebrew UI
  - a manuscript with many image rows
- Be explicit about desktop parity: browse tab, reading desk, and compact mode if all are in scope.

**Risk Assessment**

**MEDIUM-HIGH.** The implementation style is sensible, but the plan currently looks under-scoped against the stated requirement and could easily miss one or more UI surfaces. The biggest risk is shipping a solid browse dialog while still not actually satisfying DIM-01.

---

## Consensus Summary

*(Single reviewer — consensus analysis not applicable. Key themes below.)*

### Key Concerns (Priority Order)

1. **Summary table semantics** (HIGH) — MAX(width) from one image + MAX(height) from another creates phantom dimensions. Needs min/max pairs or explicit source tracking.
2. **Flag exclusion at summary-generation time** (HIGH) — Currently described as display-only; must be enforced during manuscript_measurements aggregation or Phase 55 filtering inherits bad data.
3. **DIM-01 surface coverage** (HIGH) — Plans only cover browse dialog; requirement says "browse and result views". Search results need at minimum a compact dimension display.
4. **Build path discipline** (HIGH) — Two scripts (export_fist_enrichment.py + import_measurements.py) with no defined ordering creates fragile sidecar builds.
5. **Catalog_Sizes shelfmark→AlmaId audit** (HIGH) — No match-rate reporting or duplicate handling for ambiguous shelfmarks.
6. **Lazy loading / async** (MEDIUM) — Dialog fetch must not block UI thread, especially for manuscripts with many images.
7. **has_measurements() semantics** (MEDIUM) — Should check all data sources, not just summary table.
8. **Blank_Images in dialog** (MEDIUM) — Imported but not displayed.

### Divergent Views

N/A — single reviewer.

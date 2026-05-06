---
gsd_state_version: 1.0
milestone: v7.11
milestone_name: CUDL Coverage & Synthetic Inventories
status: executing
last_updated: "2026-05-06T12:30:00.000Z"
last_activity: 2026-05-06 -- Phase 84 execution complete (5/5 plans, VERIFICATION human_needed 4/5)
progress:
  total_phases: 85
  completed_phases: 82
  total_plans: 269
  completed_plans: 268
  percent: 99
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-05)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.11 Phase 85 — Synthetic FJMS Inventory Rows (next)

## Current Position

Phase: 84 (cudl-shelfmark-normalization) — COMPLETE (5/5 plans)
Plan: —
Status: Phase 84 complete; VERIFICATION 4/5 (1 deferred to Phase 86, 1 human_needed Mosseri 98% rate vs nli_crossref.db)
Last activity: 2026-05-06 -- Phase 84 execution complete

Progress: [###.......] 33% (1/3 phases)

**Phase queue (v7.11):**

1. ✅ **Phase 84** — CUDL Shelfmark Normalization (NORM-01..04). Bridge-layer normalizers shipped: shared/shelfmark_bridge.py, alias index, leading-zero audit, NORM-04 regression guard.
2. ⏭ **Phase 85** — Synthetic FJMS Inventory Rows (SYNTH-01..06). Option-2 18-digit numeric sys_id format. Touches search index, browse, lists, exclusions, parallels, FJMS enrichment fallback.
3. ⏭ **Phase 86** — CUDL Coverage Audit (AUDIT-01..03). Final scan, report, regression check.

## Investigation Summary (pre-milestone)

**User-reported case:** `T-S NS 329.96` missing from app despite existing in CUDL. Investigation produced `scripts/scan_cudl_orphans.py` and surfaced 6,052 CUDL-vs-libraries.csv classmark gaps.

**Key findings driving this milestone:**

- 4 normalization bugs in the libraries.csv ↔ CUDL bridge accounted for ~13K false orphans (slash, comma, letter-adjacent dot, leading zeros).
- Mosseri-aware normalization recovers 3,828 of 3,883 (98.6%) — rows already exist under `library_code=Mosseri` in `Moss. III,27O` form.
- Cambridge Or. normalization recovers 584 of 1,421 so far; deeper letter-suffix pattern work in scope.
- NLI gap file (`Inventory ID no exact match to Alma.xlsx`) confirms ~93 T-S sub-series classmarks are FJMS-only — no Alma record at all. Need synthetic rows.
- 0 cases of "missing alias on existing libraries.csv row" — the original "merge 329.96 into 329.97" hypothesis is wrong; NLI doesn't have a single Alma covering both.

**Reports produced:**

- `reports/cudl_orphans_all.csv` — 6,052 rows
- `reports/cudl_orphans_with_neighbor.csv` — 104 rows (heuristic candidates, mostly disconfirmed by gap file)

**External data references:**

- `FIST_DB_BACKUP/gap_files/Inventory ID no exact match to Alma.xlsx` (NLI Chico/Tzippora, Feb 2026)
- `FIST_DB_BACKUP/gap_files/Alma records - no Inventory ID.xlsx` (12,647 Alma rows lacking FIST link, mostly BL/RNL/JTS — out of scope)
- `nli_data/nli_crossref.db` `cambridge_manifests` table (141,368 CUDL classmarks)

## Accumulated Context

### Architectural Constraints (carry-over from prior milestones)

- **Dual app maintenance:** All shared logic lives in `genizah_core.py` and `shared/*`. UI is app-specific (web/, desktop/).
- **sys_id contract:** Browse module identifies sys_id by "starts with 99, all digits" (web/pages/browse.py:584-585). Synthetic IDs MUST satisfy this.
- **Tantivy index:** Local, includes a row per libraries.csv entry. Synthetic rows require index rebuild path.
- **fjms_enrichment.db keys on AlmaId:** All catalogue/bib/measurement tables key by AlmaId, not InventoryId. Phase 85 needs an InventoryId-fallback resolver.

### Key References

- `shared/nli_crossref_service.py` — current bridge layer for CUDL classmark resolution
- `web/pages/browse.py` — sys_id detection logic (line 584-585)
- `scripts/scan_cudl_orphans.py` — investigation script (will be re-run for AUDIT-01)
- `docs/FJMS_API_REFERENCE.md` — FJMS WCF API docs (used during investigation)
- `genizah_core.py` `LIBRARY_CODES` — library_code taxonomy (synthetic rows use existing codes)

## Next Step

`/gsd-discuss-phase 85` (Synthetic FJMS Inventory Rows) or `/gsd-plan-phase 85` to start the next phase.

Optional: with `nli_crossref.db` available locally, run `pytest tests/test_shelfmark_bridge.py::TestScanDiffBaselineStillResolves -v` to confirm the human-verification item (Mosseri 98% end-to-end resolution rate).

---
status: partial
phase: 85-synthetic-fjms-inventory-rows
source: [85-VERIFICATION.md]
started: 2026-05-08T11:34:44Z
updated: 2026-05-08T19:30:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Open browse for synthetic sys_id (e.g. /browse?sys_id=990000002099000000) on web app
expected: Page renders with FJMS Bibliography panel populated (5,034 of 5,035 synthetic IDs have bibliography); catalogue/free-desc/full-text panels intentionally empty (data reality — these inventories only have bibliography in FJMS); NLI elements hidden; metadata-only Phase 53 fallback with no broken-image placeholder; no console errors; no 404 noise in NLI logs
result: [pending]
notes: G-01 resolved 2026-05-08 — sidecar regenerated to 895MB with 5,034 synthetic AlmaIds in bibliography table. Deploy regenerated sidecar to web server + bundle in next desktop installer.

### 2. Search 'T-S NS 329.96' (or equivalent FJMS-only shelfmark from manifest) in Shelfmark mode
expected: Returns synthetic row with FJMS-derived title and matching call_numbers; clicking through to browse opens the synthetic-row page successfully
result: [pending]

### 3. Add synthetic sys_id to a saved list, then reload the list
expected: Round-trip preserves synthetic sys_id without crash; list item displays shelfmark; remove operation succeeds
result: [pending]

### 4. Click Edit/correction button on synthetic browse page
expected: Web — button hidden; cannot bypass via UI. Desktop — btn_b_edit hidden; Ctrl+Shift+S programmatic shortcut shows "Corrections not available" QMessageBox without crashing
result: [pending]

### 5. Run scripts/scan_cudl_orphans.py after Phase 85 to gauge orphan-classmark reduction (Phase 86 input)
expected: Synthetic rows reduce orphan count; Phase 86 input artifacts ready
result: [pending]

### 6. Open desktop app v7.11 (post-build): browse synthetic sys_id, attempt all features (lists/exclusions/corrections/external links/parallels)
expected: Web+desktop parity preserved; no QMessageBox surprises beyond the corrections-disabled message; no Qt warnings; no crashes
result: [pending]

## Summary

total: 6
passed: 0
issues: 0
pending: 6
skipped: 0
blocked: 0

## Gaps

### G-01 Operational: regenerate fjms_enrichment.db
status: resolved
resolved: 2026-05-08T19:30:00Z
debug_session: null
notes: Sidecar regenerated 2026-05-08 against real FIST.db. Final size 895MB. Synthetic AlmaId coverage:
- bibliography: 5,034 distinct synthetic AlmaIds (full manifest coverage minus 1 CSV-injection-leader exclusion)
- catalog: 1 synthetic (only 1 InventoryId has UnitCatalogRec entry)
- domains: 1 synthetic (same InventoryId)
- catalog_fields / sizes / running_titles / free_desc / full_texts / textual_frames / mentions / refs / joins: 0 synthetic each

This is the data reality. Plan 02 qualified InventoryIds via "any FJMS signal" (catalog title OR genizah title OR bib OR free_desc OR full_text OR size). 5,034 of 5,035 qualified via bibliography only — they don't have entries in dbo_UnitCatalogRec or other catalog-keyed tables. Browse pages for synthetic rows will populate the Bibliography panel and leave catalogue/scholarly-description/measurements panels empty. Acceptable per Plan 02's inclusive-coverage stance ("if any external system holds something useful, GenizahSearch should let researchers find and view it").

**Side effect of regen — script optimization:** Original UNION-ALL outer ORDER BY (Codex HIGH "determinism" review) caused indefinite hang on real FIST.db (catalog_fields stuck at 0 rows for 2+ hours). Codex consultation rewrote to drop outer ORDER BY entirely; semantic-determinism test invariant added. Tests went 33→45 passing. Commit `74fa6beb`.

**Sidecar deployment still pending:** Deploy regenerated `fist_data/fjms_enrichment.db` to web server + bundle in next desktop installer.

### G-02 Decision: SYNTH-03 narrowing
status: needs_decision
debug_session: null
notes: ROADMAP success criterion #2 says "all standard search modes (text/title/shelfmark/Responsa)" but implementation supports Title+Shelfmark only. Text/Responsa use Tantivy chunks; synthetic rows have no transcription text. Both Codex and Gemini reviewers (MEDIUM) flagged this; `reports/synthetic_coverage.md` §"SYNTH-03 Search Mode Coverage" recommends REQUIREMENTS amendment.

**User decision needed:** accept narrowing (REQUIREMENTS amendment) OR schedule Tantivy-stub-rows infrastructure follow-up phase.

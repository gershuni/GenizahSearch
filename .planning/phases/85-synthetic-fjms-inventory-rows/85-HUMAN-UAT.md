---
status: partial
phase: 85-synthetic-fjms-inventory-rows
source: [85-VERIFICATION.md]
started: 2026-05-08T11:34:44Z
updated: 2026-05-08T11:34:44Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Open browse for synthetic sys_id (e.g. /browse?sys_id=990000002099000000) on web app
expected: Page renders with FJMS catalogue/bibliography/measurements panels populated; NLI elements (KTIV button, NLI source toggle, Alma metadata) hidden; if CUDL manifest available, Cambridge IIIF image loads; if not, metadata-only Phase 53 fallback with no broken-image placeholder; no console errors; no 404 noise in NLI logs
result: [pending]
notes: Blocked on operational gap (G-01) — fjms_enrichment.db must be regenerated against real FIST.db with the new manifest UNION ALL injection. Run `python scripts/export_fist_enrichment.py` then deploy to web server.

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
blocked: 1

## Gaps

### G-01 Operational: regenerate fjms_enrichment.db
status: failed
debug_session: null
notes: Plan 03 wired UNION ALL synthetic injection in scripts/export_fist_enrichment.py and tests pass, but the actual fist_data/fjms_enrichment.db on disk has NOT been regenerated since 2026-04-21 (pre-Phase-85). Currently 0 synthetic AlmaIds in catalog table. Until the sidecar is rebuilt against real FIST.db with the new manifest, opening a browse page for any synthetic sys_id will not show FJMS catalogue/bibliography/measurements data — defeating SYNTH-04 criterion 4.

**Required steps:**
1. Run `python scripts/export_fist_enrichment.py` against real `fist_data/FIST.db` to regenerate `fist_data/fjms_enrichment.db` with the 5,035 synthetic-AlmaId UNION ALL rows
2. Verify post-export catalog table contains 5,035 synthetic AlmaIds matching manifest
3. Deploy regenerated sidecar to web server + bundle in next desktop installer

### G-02 Decision: SYNTH-03 narrowing
status: needs_decision
debug_session: null
notes: ROADMAP success criterion #2 says "all standard search modes (text/title/shelfmark/Responsa)" but implementation supports Title+Shelfmark only. Text/Responsa use Tantivy chunks; synthetic rows have no transcription text. Both Codex and Gemini reviewers (MEDIUM) flagged this; `reports/synthetic_coverage.md` §"SYNTH-03 Search Mode Coverage" recommends REQUIREMENTS amendment.

**User decision needed:** accept narrowing (REQUIREMENTS amendment) OR schedule Tantivy-stub-rows infrastructure follow-up phase.

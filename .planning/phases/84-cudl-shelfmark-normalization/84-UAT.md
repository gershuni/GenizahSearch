---
status: partial
phase: 84-cudl-shelfmark-normalization
source:
  - 84-01-SUMMARY.md
  - 84-02-SUMMARY.md
  - 84-03-SUMMARY.md
  - 84-04-SUMMARY.md
  - 84-05-SUMMARY.md
started: 2026-05-06T12:35:00Z
updated: 2026-05-06T15:30:00Z
notes: |
  4/4 tests reached a definitive result. Test 1 surfaced 3 sub-issues; sub-issue 1b
  was fixed in-session (commit 2b1a1b2e), 1a and 1c are pre-existing data/nav
  problems unrelated to Phase 84 and are deferred to the v7.11 backlog. Tests
  2/3/4 pass. Phase 84 contract (NORM-01..04) is honored for the bridge layer.
---

## Current Test

[testing complete]

# Tests 1, 1c, 2 closed in this session (multiple commits — see 84-UAT.md
# Tests section for severity/diagnosis trail).
# Resolved gaps:
#   - 670058c7: bridge wired into get_image_sources() (5th call site)
#   - 5b96868a: bridge wired into web/pages/browse_enrichment.py (6th call site)
#   - b458f665: iiif → view URL transform + propagate external_url to web
#   - 2b1a1b2e: bridge-resolved sys_id promoted to exact-match in
#               resolve_system_by_shelfmark — closes sub-issue 1b
# Remaining open from Test 1:
#   - 1a (header shows '8/002'): pre-existing data quality (libraries.csv row
#     stored only in CUDL classmark form since first commit 68dc0e99). Logged
#     as v7.11 backlog item — out of Phase 84 scope.
#   - 1c (Prev/Next nav unrelated): pre-existing nav design (Transcriptions.txt
#     file order). Out of Phase 84 scope.

## Tests

### 1. Shelfmark search resolves CUDL slash/leading-zero form
expected: |
  Searching shelfmark `T-S F 8/002` resolves to the same record as `T-S F 8.2`
  (libraries.csv canonical form). Pre-Phase-84 this returned 0 hits.
result: issue
reported: |
  Searching `T-S F 8/002` resolves to the right manuscript (T-S F 8.2 — content
  matches Talmud Bavli Sanhedrin halakhic) BUT three problems surfaced:
    (a) Browse header shows `Ms. T-S F 8/002` literally — should be the canonical
        `Ms. T-S F 8.2`. The user's input is being shown as the manuscript label.
    (b) Searching `T-S F 8.2` or `T-S F 8/2` opens a "Select manuscript" picker
        listing T-S F 8.20, 8.21, 8.22 etc. but NOT T-S F 8.2 itself — exact-match
        is hidden behind prefix matches.
    (c) After landing on the bridge-resolved page, Prev/Next manuscript navigation
        goes to "completely unrelated mss" — sibling sys_id ordering is broken.
severity: major
sub_issues:
  - id: 1a
    name: header-label-shows-input-form
    severity: cosmetic
    diagnosis: |
      Pre-existing data. Row 990026242400205171 in libraries.csv stores BOTH call_numbers
      as `Ms. T-S F 8/002` (CUDL form). No `T-S F 8.2` alias exists. Phase 84 made the
      row reachable for the first time, exposing the data issue. Not a regression.
    fix_scope: out-of-phase-84 (data update or render-side canonicalize)
  - id: 1b
    name: bridge-fallback-suppressed-when-prefix-matches-exist
    severity: major
    diagnosis: |
      genizah_core.py:4613 only fires bridge when `not results`. Canonical search for
      `T-S F 8.2` returns 8.20, 8.21, 8.22 (substring matches), so bridge never runs
      for the user-typed exact form. Bridge should also run when no EXACT match exists
      among canonical results, and bridge hit should be ranked above substring hits.
    fix_scope: in-phase-84-followup (fix gate condition + result ordering)
  - id: 1c
    name: prev-next-nav-unrelated-for-metadata-only-rows
    severity: major
    diagnosis: |
      Pre-existing. service.get_adjacent_shelfmark uses Transcriptions.txt file order
      via state.searcher.get_adjacent_sys_id_by_file_order. Metadata-only CUL/Mosseri
      rows (no transcription) sit next to unrelated mss in that file. Phase 84 made
      them reachable; nav logic unchanged. Not a regression but a real exposure.
    fix_scope: out-of-phase-84 (separate nav phase — natural-sort by shelfmark)

### 2. Browse Mosseri manuscript loads Cambridge IIIF images
expected: |
  Open a Mosseri manuscript (e.g. browse to a sys_id with library_code=Mosseri,
  or search shelfmark `Moss. III,27`). Cambridge CUDL embedded images load (not
  just NLI). External CUDL link button works. Pre-Phase-84 these images were
  missing because the bridge couldn't translate `Moss. III,27O` ↔ `mosseriiii27o`.
result: pass
fix_chain:
  - 670058c7: get_image_sources() bridge fallback (5th call site)
  - 5b96868a: browse_enrichment.py bridge wiring (6th call site)
  - b458f665: iiif → view URL transform + external_url propagation to web

### 3. Browse Or.-letter-suffix manuscript generates correct CUDL slug
expected: |
  Open an `Or. 1080 J 15`-style manuscript (Cambridge Or. classmark with letter
  suffix). "View on CUDL" external link works (lands on a real CUDL gallery page).
result: pass

### 4. Regression: canonical T-S shelfmark unchanged
expected: |
  Search shelfmark `T-S 12.123` (or any standard canonical form). Behavior is
  identical to v7.10 — same record, same browse page, same Cambridge images.
  No regression on the 140K already-matching CUL rows.
result: pass

## Summary

total: 4
passed: 3
issues: 1
pending: 0
skipped: 0
in_session_fixes:
  - 670058c7: get_image_sources() bridge fallback (5th call site)
  - 5b96868a: browse_enrichment.py bridge wiring (6th call site)
  - b458f665: iiif → view URL transform + external_url propagation to web
  - 2b1a1b2e: bridge-resolved sys_id promoted to exact-match in
              resolve_system_by_shelfmark (closes 1b)

## Gaps

# Sub-issue 1b RESOLVED in 2b1a1b2e — `T-S F 8.2` now resolves directly to
# the bridge-resolved row 990026242400205171 (single-exact-match, no picker).
# 4 new tests in TestResolveSystemBridgeExactMatch lock the contract.

# Remaining open (out-of-Phase-84 scope, logged for v7.11 backlog):
- truth: "Browse header displays canonical NLI/CUDL form, not the user's input or stored slash form"
  status: deferred
  reason: "Pre-existing data quality. libraries.csv row 990026242400205171 has stored Ms. T-S F 8/002 since first commit (68dc0e99). NLI / CUDL display title is 'T-S F 8.2', but the row was sourced from CUDL classmark (URL slug) form, not display title. Phase 84 made the row reachable; the data issue itself predates Phase 84."
  severity: cosmetic
  test: 1
  fix_scope: out-of-phase-84
  next_step: v7.11 backlog item — data refresh OR render-time canonicalize CUL classmark variants

- truth: "Prev/Next manuscript navigation lands on semantically-related siblings, not arbitrary file-order neighbors"
  status: deferred
  reason: "Pre-existing nav design. service.get_adjacent_shelfmark uses Transcriptions.txt file order. Metadata-only CUL/Mosseri rows (no transcription) sit next to unrelated mss in that file. Phase 84 made these rows reachable but did not change nav. Out of scope."
  severity: major
  test: 1
  fix_scope: out-of-phase-84
  next_step: separate phase — natural-sort by shelfmark for prev/next nav


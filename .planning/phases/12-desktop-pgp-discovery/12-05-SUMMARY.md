---
phase: 12-desktop-pgp-discovery
plan: 05
subsystem: desktop-ui, web-ui
tags: [filters, i18n, pgp-joins, enrichment, browse]
depends_on:
  requires: [12-01, 12-02, 12-03]
  provides: [web-filters-toggle-fix, hebrew-translations, pgp-joins-dropdown, browse-enrichment]
  affects: [13-transcription-search]
tech-stack:
  added: []
  patterns: [boolean-state-toggle, race-condition-merge, enrichment-html-builder]
key-files:
  created: []
  modified:
    - web/pages/search.py
    - genizah_translations.py
    - genizah_app.py
decisions: []
metrics:
  duration: 4min
  completed: 2026-02-08
---

# Phase 12 Plan 05: UAT Gap Closure - Filters, Joins, i18n, Enrichment

**One-liner:** Fixed web filters toggle with boolean state, added PGP joins to dropdown menus, Hebrew translations for Phase 12, and KTI/Oxford/Cambridge data in Browse extended info.

## Task Commits

| # | Task | Commit | Key Changes |
|---|------|--------|-------------|
| 1 | Fix web filters toggle + Hebrew translations | `07fb363` | Boolean state variable for toggle, 9 new Hebrew entries |
| 2 | PGP joins in dropdown menus + Browse enrichment | `431a320` | PGP fallback in both dropdowns, enrichment HTML builder |

## What Was Done

### Task 1: Fix web filters toggle + add Hebrew translations

**Web filters toggle (Test 9):**
- The `toggle_filters()` function was reading `filters_panel.style` as a string, but NiceGUI's `.style` returns a Style object, not a plain string
- Replaced style read-back with a `filters_visible = {'value': False}` boolean state dictionary
- Toggle now reliably flips the boolean and applies the correct style each time

**Hebrew translations (Test 15):**
- Added 9 new translation entries to `genizah_translations.py` covering all Phase 12 UI strings
- Fixed key mismatches where `tr()` calls used format placeholders (`{}`) but dict keys didn't include them
- New keys: `PGP Only`, `Show only manuscripts with PGP transcriptions`, `Search Tag`, `PGP Tag:`, `Search by PGP Tag...`, `Searching tag: {}...`, `No results for tag: {}`, `No local results for tag: {}`, `Tag: {} - {} results`

### Task 2: PGP joins in dropdown menus + Browse tab enrichment

**PGP joins in dropdowns (Test 10):**
- Added PGP multi-fragment joins fallback to `_update_joins_dropdown()` (Browse tab)
- Added same fallback to `_rd_update_joins_menu()` (Reading Desk)
- After user-joins check fails, queries `get_document_for_fragment` and `get_fragments_for_document` from shared service
- PGP join entries display with `[PGP]` prefix and navigate to the fragment on click
- Button turns green when PGP joins found, grey when none

**Browse tab enrichment (Test 16):**
- Created `_build_browse_enriched_html()` helper method mirroring ResultDialog's enrichment builder
- Builds KTI metadata (date, physical description, English title, subjects, notes, people, bibliography)
- Builds Oxford part metadata (codicological part, folio range, Oxford title, contents)
- Builds Cambridge/external metadata
- Handles two-column layout (KTI + Oxford/Cambridge) with RTL support
- Race condition handling: enrichment and PGP workers can arrive in either order
  - `on_browse_enriched_loaded`: stores enriched HTML in `_browse_enriched_html`, combines with PGP if already loaded
  - `_on_browse_pgp_loaded`: combines with enriched HTML if already stored
- `_browse_enriched_html` reset alongside `_browse_pgp_doc` when manuscript changes

## Deviations from Plan

None - plan executed exactly as written.

## Verification

1. Web filters toggle uses `filters_visible['value']` boolean - VERIFIED
2. All 9 new translation keys exist with Hebrew values - VERIFIED
3. PGP joins fallback in `_update_joins_dropdown` using `get_document_for_fragment` - VERIFIED
4. PGP joins fallback in `_rd_update_joins_menu` with same pattern - VERIFIED
5. `_build_browse_enriched_html` helper method exists - VERIFIED
6. `_browse_enriched_html` is reset at both initialization points - VERIFIED
7. All three files compile without syntax errors - VERIFIED

## Self-Check: PASSED

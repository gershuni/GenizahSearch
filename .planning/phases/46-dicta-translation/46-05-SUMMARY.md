---
phase: 46-dicta-translation
plan: 05
subsystem: translation
tags: [dicta, translation, desktop-integration, extraction-fix, subtitle, batch-translation]

# Dependency graph
requires:
  - phase: 46-01
    provides: "Dicta API client, TranslationService, few-shot templates"
  - phase: 46-02
    provides: "PGP batch translations in pgp_translations table"
  - phase: 46-03
    provides: "FJMS batch translations in fjms_translations table"
  - phase: 46-04
    provides: "Web translation integration, toggle, badges, translate buttons"
provides:
  - "Desktop translation toggle, translated text display in search/browse, translated match badges"
  - "english_title_he subtitle display (HE — EN subtitle when Hebrew title < 15 chars)"
  - "Extraction fix: MARC semicolon split (87K records fixed, 58K Hebrew values improved)"
  - "Search-in-translation removed from main search (belongs in browse filter only)"
  - "EN→HE title batch (7,635 translations), FJMS Round 3 RunningTitle (111K) + FullText (71K)"
  - "RunningTitle/FullText translation wiring in FjmsCatalogDialog (web + desktop)"
affects: []

# Tech tracking
tech-stack:
  added: []
  patterns: [subtitle-display, marc-semicolon-split, translation-toggle-desktop, clickable-toggle-badge-desktop]

key-files:
  created:
    - scripts/translate_fjms_catalog_text.py
    - scripts/translate_libraries_en2he.py
  modified:
    - genizah_app.py
    - gui_threads.py
    - scripts/extract_libraries_english.py
    - shared/translation_service.py
    - web/pages/search.py
    - web/components/catalog_dialog.py

key-decisions:
  - "Extraction fix: split on ` ; ` (MARC field separator) not `\\s*;\\s*` — preserves semicolons within Hebrew text"
  - "Longest pure-Hebrew part preferred over mixed content — 58K records improved"
  - "english_title_he shown as subtitle when Hebrew title < 15 chars (em dash separator)"
  - "Search-in-translation removed from main search — translation search belongs only in browse catalog FTS5 filter"
  - "Shelfmark-sysid fix: same-series conflicts only (not cross-series aliases like BL Or/Gaster)"

patterns-established:
  - "MARC semicolon split pattern: ` ; ` preserves internal punctuation"
  - "Subtitle display: short Hebrew title gets EN→HE translation as subtitle"
  - "RunningTitle/FullText translation wiring via fjms_translations table lookup"

requirements-completed: [TRANS-05, TRANS-01, TRANS-02]

# Metrics
duration: ~3 sessions (code + batch + fixes)
completed: 2026-03-10
---

# Phase 46 Plan 05: Desktop Translation Wiring & Batch Round 2-3 Summary

**Desktop translation integration, extraction fix, EN→HE title batch, FJMS Round 3 translations, subtitle display, and search-in-translation correction**

## Performance

- **Duration:** ~3 sessions across 2026-03-08 to 2026-03-10
- **Tasks:** Code changes + server batch runs + extraction fix + corrections
- **Files modified:** 6
- **Files created:** 2 (batch scripts)

## Accomplishments

- Desktop translation toggle (show_translations setting), translated text display in search results and browse views, translated match badges — full parity with web
- TranslationService extended with english_title_he column support and cached _titles_has_en_he flag
- Subtitle display: when Hebrew title < 15 chars, shows "HE — EN_HE" subtitle in both web and desktop
- Extraction fix: semicolon split changed from `\s*;\s*` to ` ; ` (MARC separator), longest pure-Hebrew part preferred — 87K records fixed, 58K Hebrew values improved
- Removed translated-match badges from main search (gui_threads.py, genizah_app.py, web/pages/search.py) — translation search belongs only in browse catalog text filter
- EN→HE title batch: 7,635 translated, 9 failed, 31.2 min
- FJMS Round 3: RunningTitle (111K) + FullText (71K) EN→HE translations complete
- RunningTitle/FullText translations wired into FjmsCatalogDialog display (web + desktop)
- libraries_translations.db rebuilt with all fixes

## Task Commits

1. **Extraction fix + search-in-translation removal** — `a57bf68c`
2. **english_title_he subtitle wiring (web + desktop)** — `f0dcf21a`

## Files Created/Modified

- `scripts/extract_libraries_english.py` — MARC semicolon split fix, longest pure-Hebrew selection
- `scripts/translate_fjms_catalog_text.py` — NEW: EN→HE batch script for RunningTitle + FullText
- `scripts/translate_libraries_en2he.py` — NEW: EN→HE title batch script
- `genizah_app.py` — Desktop translation toggle, subtitle display (_resolve_display_title)
- `gui_threads.py` — Removed search-in-translation from SearchThread
- `shared/translation_service.py` — english_title_he support, _titles_has_en_he cache flag
- `web/pages/search.py` — Subtitle display, removed translated-match from main search
- `web/components/catalog_dialog.py` — RunningTitle translation wiring

## Deviations from Plan

### Intentional Scope Changes

**1. Search-in-translation removed from main search**
- **Reason:** Translation search in main search results created noise — translated match badges appeared on nearly every result. Translation search belongs only in browse catalog FTS5 text filter, not in full-text search results.
- **Impact:** Cleaner search UX. TranslationService search methods retained for future browse integration.

**2. Extraction fix added to scope**
- **Reason:** Discovered during testing that `\s*;\s*` split was breaking Hebrew text containing semicolons (not MARC separators). 87K records affected.
- **Impact:** Significant data quality improvement. Zero data loss.

**3. Batch translations expanded (Round 2-3)**
- **Reason:** EN→HE titles and FJMS RunningTitle/FullText needed translation for complete bilingual coverage.
- **Impact:** +118K additional translations across 2 DBs.

## Issues Encountered
- Server batch runs required ~11h total for FJMS Round 3 (RunningTitle + FullText)
- Shelfmark-sysid conflict discovered and fixed separately (cherry-picked to master-main)

## Next Phase Readiness
- Phase 46 complete. All 5 plans done.
- v6.5.0 milestone at 100% phase completion.
- Remaining: download updated DBs from server, verify locally, then milestone wrap-up.

## Self-Check: PASSED

All modified files verified present. All task commits verified in git log.

---
*Phase: 46-dicta-translation*
*Completed: 2026-03-10*

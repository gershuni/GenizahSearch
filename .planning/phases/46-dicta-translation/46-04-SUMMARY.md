---
phase: 46-dicta-translation
plan: 04
subsystem: translation
tags: [dicta, translation, web-integration, toggle, badge, translate-button, sqlite, nicegui]

# Dependency graph
requires:
  - phase: 46-01
    provides: "Dicta API client (shared/dicta_client.py), TranslationService (shared/translation_service.py), few-shot templates"
  - phase: 46-02
    provides: "PGP batch translation script populating pgp_translations table"
  - phase: 46-03
    provides: "FJMS batch translation scripts populating fjms_translations table"
provides:
  - "Global translation toggle in web sidebar (show_translations user preference)"
  - "Translated match badge (light blue) on search results found via translation"
  - "Clickable Translated/Original badge toggling between translated and original text"
  - "TranslationService search methods: search_pgp_by_translation, search_fjms_by_translation, get_pgp_translations_by_sys_ids, get_translated_match_sys_ids"
  - "Dicta-powered translate buttons replacing MyMemory API for community content"
  - "Browse page shelfmark URL param + sys_id direct load support"
affects: [46-05]

# Tech tracking
tech-stack:
  added: []
  patterns: [translation-toggle-user-preference, translated-match-badge, clickable-toggle-badge, sys-id-based-translation-lookup, lazy-few-shot-singleton]

key-files:
  created: []
  modified:
    - shared/translation_service.py
    - web/components/translate_button.py
    - web/main.py
    - web/pages/search.py
    - web/pages/browse.py
    - web/pages/catalog_browse.py
    - genizah_translations.py
    - tests/test_translation_service.py

key-decisions:
  - "Translation enrichment keyed by sys_id (not pgpid) using document_fragments JOIN — avoids needing pgpid in search result objects"
  - "Translated badge is a clickable button (click.stop) that toggles between translated and original text inline — better UX than tooltip-only"
  - "Browse page now accepts shelfmark URL param and detects sys_id input (starts with 99, all digits) for direct load"
  - "Few-shot templates lazy-loaded as singletons in translate_button.py — loaded once on first use, cached for session"
  - "Translation toggle default is OFF — users opt in to translated descriptions per CONTEXT.md"

patterns-established:
  - "sys_id-based translation lookup: JOIN document_fragments + pgp_translations in batched queries (400 per batch)"
  - "Clickable toggle badge pattern: ui.button with click.stop, toggling label text and direction style between translated/original"
  - "Translation enrichment in parallel gather: 5th enrichment query alongside domains, transcriptions, catalog_counts, printed_ids"
  - "Lazy few-shot singleton: _few_shot_cache dict loaded on first translate_text call, reused for session"

requirements-completed: [TRANS-05, TRANS-01, TRANS-02]

# Metrics
duration: 14min
completed: 2026-03-04
---

# Phase 46 Plan 04: Web Search Integration & Translation Toggle Summary

**Web translation integration with global toggle, clickable Translated/Original badges, translated match detection, Dicta-powered translate buttons, and browse shelfmark/sys_id URL support**

## Performance

- **Duration:** ~14 min (execution time, excluding human verification wait)
- **Started:** 2026-03-04T07:00:24Z
- **Completed:** 2026-03-04T13:36:00Z
- **Tasks:** 3
- **Files modified:** 8

## Accomplishments
- Global translation toggle in web sidebar (between language and theme toggles) with persistent show_translations preference
- Translated match badge (light blue) on search results whose PGP description translations match the search query
- Clickable Translated/Original badges that toggle inline between Hebrew translated text (RTL) and English original (LTR) in compact results, advanced view, and browse metadata
- TranslationService extended with 4 new methods for search and sys_id-based lookups (10 new tests, 35 total)
- MyMemory API fully replaced with Dicta Translation API in translate_button.py with lazy-loaded scholarly few-shot templates
- Browse page enhanced with shelfmark URL param support and sys_id direct detection/loading
- 12 new translation UI strings in genizah_translations.py

## Task Commits

Each task was committed atomically:

1. **Task 1: Web search/browse translation integration + translated match badge** - `da769e93` (feat: TranslationService search methods, enrichment pipeline, toggle, badges, 10 new tests)
2. **Task 2: Replace MyMemory with Dicta for community content** - `810d0737` (feat: Dicta API in translate_button.py, lazy few-shot loading)
3. **Task 3: Verified web translation UX + refinements** - `0ea3f022` (feat: clickable toggle badges, browse shelfmark/sys_id URL support)

## Files Created/Modified
- `shared/translation_service.py` - Added search_pgp_by_translation, search_fjms_by_translation, get_pgp_translations_by_sys_ids, get_translated_match_sys_ids methods
- `web/components/translate_button.py` - Complete rewrite: MyMemory replaced with Dicta API, lazy singleton few-shot loading, 2000-char truncation for on-demand UX
- `web/main.py` - Translation toggle in sidebar footer, browse route shelfmark param + sys_id detection
- `web/pages/search.py` - Translation enrichment in parallel gather, translated match detection, clickable toggle badges in compact cards + advanced view
- `web/pages/browse.py` - Translated PGP description with clickable toggle badge, shelfmark URL param support, sys_id direct load in search box
- `web/pages/catalog_browse.py` - FJMS translation gap-fill for empty titles when toggle is on
- `genizah_translations.py` - 12 Phase 46 translation strings (Show translations, Translated, Original, Translated match, etc.)
- `tests/test_translation_service.py` - 10 new tests: search methods (3), sys_id mapping (2), translated match detection (3), edge cases (2)

## Decisions Made
1. **sys_id-based translation lookup:** Search results carry sys_id but not pgpid. Used document_fragments JOIN pgp_translations to map sys_id -> translation data. Batched in groups of 400 to avoid SQLite param limits.
2. **Clickable badge over tooltip:** Changed from static "Translated" label with tooltip to a clickable button that toggles text inline between translated and original. Uses click.stop to prevent card click propagation. Better UX for quick comparison.
3. **Parallel enrichment:** Translation batch lookup added as 5th parallel enrichment query in asyncio.gather, alongside domains, transcriptions, catalog_counts, and printed_ids. No additional latency.
4. **Graceful degradation:** When pgp_translations/fjms_translations tables don't exist (batch scripts not yet run), all translation features degrade silently -- toggle is visible but no translations appear.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Search results lack pgpid in display dict**
- **Found during:** Task 1 (translation enrichment)
- **Issue:** Plan assumed pgpid would be available in search result display dict. Actual display dict only has sys_id, shelfmark, title, library_code, source.
- **Fix:** Added get_pgp_translations_by_sys_ids() and get_translated_match_sys_ids() methods that JOIN document_fragments with pgp_translations to map sys_id -> translation data
- **Files modified:** shared/translation_service.py, web/pages/search.py
- **Verification:** 10 new tests covering the mapping logic
- **Committed in:** da769e93 (Task 1 commit)

---

**Total deviations:** 1 auto-fixed (1 bug -- data model mismatch between plan and reality)
**Impact on plan:** Essential fix for correct data access pattern. No scope creep.

## Issues Encountered
- Translation display requires batch scripts (46-02, 46-03) to have been run to populate translation tables. Without data, the toggle is visible but translations don't appear (by design -- graceful degradation).

## User Setup Required
None - no external service configuration required. Translation data populated by running batch scripts from Plans 02 and 03.

## Next Phase Readiness
- Web translation integration complete. Ready for 46-05 (desktop translation toggle and display)
- TranslationService search methods available for desktop integration
- Dicta translate_button.py ready (shared by both apps via web components)
- All 35 tests passing

## Self-Check: PASSED

All 8 modified files verified present. All 3 task commits verified in git log.

---
*Phase: 46-dicta-translation*
*Completed: 2026-03-04*

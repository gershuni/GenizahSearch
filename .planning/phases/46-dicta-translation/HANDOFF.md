# Phase 46 Handoff — Batch Translation & Desktop Toggle

**Last updated:** 2026-03-05 (session 2)

## Status
- **46-01 through 46-04**: COMPLETE and committed
- **46-05 Task 1**: Committed. Desktop translation toggle, badges, settings.
- **46-05 Task 2 (verification)**: NEEDS TESTING — ResultDialog toggle refactored but not yet verified.

---

## What Changed This Session

### ResultDialog Toggle Fix (commit `1194629d`)
- Extracted `_rd_build_extended_html()` from `on_enriched_data_loaded()` — reusable method
- `_rd_refresh_extended_info()` now calls `_rd_build_extended_html()` directly using stored `_rd_enrichment_meta`
- `_rd_update_extended_info_with_pgp()` simplified to use same rebuild path (no more fragile `toHtml()` string surgery)
- Removed stale `_rd_enriched_html_prefix` approach

### Translation Infrastructure
- **libraries_translations.db created** (56.7MB): 112,361 extracted English + 19,787 Dicta translations = 130K+ with English
- **Dicta rate limit discovered**: 100 requests per 900s (15min). `dicta_client.py` updated: `RateLimit-Reset` header, 120s max retry, 5 retries
- **translate_libraries_titles.py**: ready to run, 9.5s delay, checkpoint/resume. ~34,700 unique strings remaining.
- **Translation Master Plan**: `docs/plans/TRANSLATION_MASTER_PLAN.md` — full inventory of all data sources

### Translation Master Plan Progress

| Phase | Status | Details |
|-------|--------|---------|
| A: Extract bilingual English | **DONE** | 112,361 titles, zero API |
| B: FJMS catalog fields | Partial (1,260 done) | Resume: `python scripts/translate_fjms_catalog.py` |
| C: Hebrew-only titles | **Script ready** | Run: `python scripts/translate_libraries_titles.py` (~91h) |
| D: PGP descriptions | Needs script fix | parallel→sequential refactor |
| E: Service + UI integration | Not started | Wire TranslationService to libraries_translations.db |
| F: FJMS free desc | Needs script fix | parallel→sequential refactor |

## What Needs Testing
1. **Desktop ResultDialog**: Launch app, open a result, click "מתורגם" badge → should toggle text
2. **Desktop ResultDialog**: Click "הצג תרגומים"/"אל תציג תרגומים" → should refresh
3. **Desktop Browse tab**: Translation toggle should still work (was working before)

## What Needs Doing Next
1. **Test the ResultDialog toggle fix** (Task 2 of 46-05)
2. **Run batch translations** (user runs scripts manually — can't background from Claude Code on Windows)
3. **Post-translation polish** (Piyyut→Poem normalization, terminology alignment — see Master Plan)
4. **Phase E**: Wire TranslationService to read libraries_translations.db, integrate into both apps

## Key Code Locations
- `_rd_build_extended_html()`: genizah_app.py:~3728 — NEW, builds full ResultDialog HTML
- `_rd_refresh_extended_info()`: genizah_app.py:~3885 — simplified, calls _rd_build_extended_html
- `_rd_update_extended_info_with_pgp()`: genizah_app.py:~3696 — simplified, uses same rebuild
- `on_enriched_data_loaded()`: genizah_app.py:~4477 — now delegates HTML to _rd_build_extended_html
- `_build_pgp_extended_info_html()`: genizah_app.py:~11446 — PGP section builder (unchanged)

## Test Data
- 8 test translations in pgp.db `pgp_translations` for pgpids 444-453
- libraries_translations.db: 130K+ records with English titles
- Shelfmarks for testing: T-S 13J35.3 (sys_id 990051250670205171)

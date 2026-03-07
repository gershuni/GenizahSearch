# Phase 46 Handoff — Batch Translation & Desktop Toggle

**Last updated:** 2026-03-06 (session 5)

## Status
- **46-01 through 46-04**: COMPLETE and committed
- **46-05 Task 1**: Committed. Desktop translation toggle, badges, settings.
- **46-05 Task 2**: Committed (`6f5fdc38`) — auto-translate-all + phys_desc
- **46-05 title wiring**: Committed (`7fb59c4c`) — libraries_translations.db title lookup + stale cache fix. **NOT FULLY TESTED — needs UAT**

---

## What Changed This Session (session 5, 2026-03-06)

### Batch Translation — Server Status Update
- Verified all batch jobs on server (`ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com`, tmux `translations`)
- Libraries titles, PGP descriptions, and FJMS catalog all COMPLETE
- FJMS free descriptions running (~40% done, ~21h remaining)
- Downloaded current DBs to local machine for testing

### Prior Session (session 4) Changes
- Title Translation Wiring (`7fb59c4c`): `_resolve_display_title()`, `_get_title_svc()`, 4 desktop title paths
- Stale Translation Cache Fix (`7fb59c4c`): clear `_field_translation_cache` on navigate
- Joins Sync (`7fb59c4c`): startup-only (was 5-min polling)

## What Still Needs Testing / Doing

### UAT Required (from session 4 changes)
1. **Title display correctness**: verify titles show clean Hebrew (not raw bilingual) across CUL, Oxford, JTS, RNL manuscripts
2. **Oxford parts**: verify different parts show their own part-specific titles (not stale cached)
3. **Translation toggle**: verify Hebrew | English appears when ON, Hebrew-only when OFF
4. **Fallback**: verify manuscripts NOT in libraries_translations.db still show raw title correctly
5. **Browse tab**: same verifications as above
6. **Cache clearing**: verify navigating between results clears stale translations

### Search Results Translation (IMPORTANT — user request)
Translations should also work on:
1. **Desktop search results** (search tab result list items) — show translated titles
2. **Desktop composition/parallels search results** — same
3. **Web search results** (web/pages/search.py) — translated titles and metadata
4. **Web composition results** (web/pages/parallels.py or similar)

### Batch Translation Progress (updated 2026-03-07)
| Phase | Status | Details |
|-------|--------|---------|
| A: Extract bilingual English | **DONE** | 112,361 titles, zero API |
| B: FJMS catalog fields | **DONE** | 3,830 rows (6 categories) |
| C: Hebrew-only titles | **DONE** | 184,514 total (all pending_dicta resolved) |
| D: PGP descriptions | **DONE** | 34,954 descriptions EN->HE |
| E: Service + UI integration | In progress | Title wiring done, search results needed |
| F: FJMS free desc | **DONE** | 254,835/254,835 (100%, 0 failures) |

All batch translation jobs complete. All DBs downloaded locally (2026-03-07).

### Post-Translation Polish
- Piyyut->Poem normalization, terminology alignment (see Translation Master Plan)
- ResultDialog toggle end-to-end test still needed
- Browse tab auto-translate-all (parity with ResultDialog)

## Key Code Locations
- `_resolve_display_title()`: genizah_app.py — resolves title from libraries_translations.db
- `_get_title_svc()`: genizah_app.py — singleton TranslationService for title lookups
- `_rd_auto_translate_all()`: genizah_app.py — fires all pending translations on toggle ON
- `_rd_refresh_title()`: genizah_app.py — rebuilds title on toggle (now uses _resolve_display_title)
- `_rd_build_extended_html()`: genizah_app.py:~3713 — builds full ResultDialog HTML
- `_rd_toggle_translations()`: genizah_app.py — toggle handler, calls auto-translate
- `_start_field_translation()`: genizah_app.py:~12133 — direction-aware (he2en for phys_desc)
- `_get_field_original_text()`: genizah_app.py:~12183 — extracts source text, handles phys_desc
- `TranslationService.get_title_translation()`: shared/translation_service.py — single sys_id lookup
- `TranslationService.get_title_translations_batch()`: shared/translation_service.py — batch lookup
- `dicta_client.py`: GOD_MODE, MAX_WORKERS, _split_by_words, _sanitize_text

## Test Data
- 34,954 translations in pgp.db `pgp_translations` (full corpus)
- libraries_translations.db: 184,514 records (112K extracted + 72K dicta, 0 pending)
- fjms_enrichment.db `fjms_translations`: 258,665 (3,830 catalog + 254,835 free desc) — COMPLETE
- sys_id 990053401060205171: bilingual title with extracted English "Mishnah: Avot 2:9 - 16"

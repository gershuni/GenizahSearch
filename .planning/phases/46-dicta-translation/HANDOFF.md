# Phase 46 Handoff — Batch Translation & Desktop Toggle

**Last updated:** 2026-03-05 (session 4)

## Status
- **46-01 through 46-04**: COMPLETE and committed
- **46-05 Task 1**: Committed. Desktop translation toggle, badges, settings.
- **46-05 Task 2**: Partially done — auto-translate-all + phys_desc committed (`6f5fdc38`)
- **46-05 title wiring**: Committed (`7fb59c4c`) — libraries_translations.db title lookup + stale cache fix. **NOT FULLY TESTED — needs UAT**

---

## What Changed This Session (session 4)

### Title Translation Wiring (`7fb59c4c`)
- `TranslationService` gains `_titles_conn` for `libraries_translations.db` with `get_title_translation()` and `get_title_translations_batch()` methods
- `_resolve_display_title()` helper: looks up clean `hebrew_title`/`english_title` from DB, replaces raw bilingual strings from libraries.csv
- Translations OFF: shows `hebrew_title` only (clean Hebrew)
- Translations ON: shows `hebrew_title | english_title`
- Wired into 4 desktop title paths: `apply_metadata`, `on_enriched_data_loaded`, `_rd_refresh_title`, `on_browse_enriched_loaded`
- Singleton `_title_svc_singleton` avoids repeated DB connections

### Stale Translation Cache Fix (`7fb59c4c`)
- `_field_translation_cache` (global dict in gui_threads.py) was never cleared on manuscript navigation
- Caused Oxford Part titles/contents to show PREVIOUS part's cached translation when navigating between parts
- ResultDialog: clear entire cache + `_trans_toggle_state` on navigate (in `load_result_by_index` reset block)
- Browse tab: clear `br_*` prefixed keys on new `on_browse_enriched_loaded`

### Joins Sync (`7fb59c4c`)
- Changed from 5-minute polling loop to startup-only sync in `_sync_loop()`

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

### Batch Translation Progress
| Phase | Status | Details |
|-------|--------|---------|
| A: Extract bilingual English | **DONE** | 112,361 titles, zero API |
| B: FJMS catalog fields | **DONE** | 3,835 complete |
| C: Hebrew-only titles | **RUNNING ON SERVER** | 7,859 done, ~27K remaining (~1.5h) |
| D: PGP descriptions | **QUEUED ON SERVER** | 100 done, ~35K remaining (runs after C) |
| E: Service + UI integration | In progress | Title wiring done, search results needed |
| F: FJMS free desc | **QUEUED ON SERVER** | ~255K (runs after D, ~14h est) |

All 4 scripts running sequentially on server in tmux session `translations`.
Server: `ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com`, then `tmux attach -t translations`.
When done, download updated DBs back to dev machine (see bottom of this file).

### Downloading Results
After all scripts complete, from PowerShell:
```powershell
$SERVER = "ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com"
$REMOTE = "/home/ubuntu/GenizahSearch"
scp ${SERVER}:${REMOTE}/pgp_data/pgp.db pgp_data/pgp.db
scp ${SERVER}:${REMOTE}/fist_data/fjms_enrichment.db fist_data/fjms_enrichment.db
scp ${SERVER}:${REMOTE}/libraries_translations.db libraries_translations.db
```

### Post-Translation Polish
- Piyyut->Poem normalization, terminology alignment (see Translation Master Plan)
- ResultDialog toggle end-to-end test still needed
- Browse tab auto-translate-all (parity with ResultDialog)

## Key Code Locations
- `_resolve_display_title()`: genizah_app.py — NEW, resolves title from libraries_translations.db
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
- 8 test translations in pgp.db `pgp_translations` for pgpids 444-453
- libraries_translations.db: 184K+ records (112K extracted, 38K dicta, 34K pending)
- sys_id 990053401060205171: bilingual title with extracted English "Mishnah: Avot 2:9 - 16"

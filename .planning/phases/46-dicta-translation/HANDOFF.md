# Phase 46 Handoff — Batch Translation & Desktop Toggle

**Last updated:** 2026-03-05 (session 3)

## Status
- **46-01 through 46-04**: COMPLETE and committed
- **46-05 Task 1**: Committed. Desktop translation toggle, badges, settings.
- **46-05 Task 2**: Partially done — auto-translate-all + phys_desc committed (`6f5fdc38`)
- **46-05 remaining**: Title separation from libraries_translations.db, search results translations, composition search, web parity

---

## What Changed This Session

### God Mode (`b190b6ec`)
- `.env` (gitignored): `DICTA_GOD_MODE=bagatz` — adds `x-god-mode` header, bypasses rate limits
- `dicta_client.py`: loads from .env, text splitting (>100 words at sentence boundaries), strips line breaks, caps workers at 5
- All 4 translation scripts: parallel with ThreadPoolExecutor, zero throttle in god mode
- Estimated speedup: ~20x for sequential scripts (catalog, libraries_titles)

### Auto-Translate on Toggle (`6f5fdc38`)
- `_rd_toggle_translations()`: now calls `_rd_auto_translate_all()` when toggled ON — fires translation for all pending fields at once
- `_rd_refresh_title()`: rebuilds title label with/without English complement on toggle
- Physical Description (Hebrew Ktiv info) now translatable field (HE→EN direction)
- Direction-aware badge display: `_he_fields` set determines RTL/LTR for original vs translated
- Browse tab parity: same phys_desc and direction fixes in `_trans_or_badge_b`
- `_get_field_original_text`: handles `phys_desc` suffix with fallback across meta keys

## What Still Needs Doing

### Title Separation (HIGH PRIORITY)
The title display still reads raw bilingual strings from libraries.csv (e.g., "משנה [טקסט]. ; Mishnah: Avot 2:9 – 16").
`libraries_translations.db` already has the split: `original_title` (Hebrew) + `english_title` (English).

**Fix needed:**
- Wire `TranslationService` into title display paths:
  - `apply_metadata()` (~line 4471): look up split title from libraries_translations.db
  - `on_enriched_data_loaded()` (~line 4794): same
  - Browse tab title display (~line 11164)
- When translations OFF: show Hebrew part only
- When translations ON: show Hebrew + English complement
- Same for `_rd_refresh_title()` — use DB lookup instead of marc.english_title

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
| B: FJMS catalog fields | Partial (1,576 done) | Resume: `python scripts/translate_fjms_catalog.py` |
| C: Hebrew-only titles | Partial (23,715 done, 48K pending) | Run: `python scripts/translate_libraries_titles.py` |
| D: PGP descriptions | 8 done, 35,830 pending | Run: `python scripts/translate_pgp_descriptions.py` |
| E: Service + UI integration | In progress | Auto-translate done, title separation needed |
| F: FJMS free desc | Not started (255K) | Run: `python scripts/translate_fjms_free_desc.py` |

### Post-Translation Polish
- Piyyut→Poem normalization, terminology alignment (see Translation Master Plan)
- ResultDialog toggle end-to-end test still needed
- Browse tab auto-translate-all (parity with ResultDialog)

## Key Code Locations
- `_rd_auto_translate_all()`: genizah_app.py — NEW, fires all pending translations
- `_rd_refresh_title()`: genizah_app.py — NEW, rebuilds title on toggle
- `_rd_build_extended_html()`: genizah_app.py:~3713 — builds full ResultDialog HTML
- `_rd_toggle_translations()`: genizah_app.py — toggle handler, now calls auto-translate
- `_start_field_translation()`: genizah_app.py:~12133 — direction-aware (he2en for phys_desc)
- `_get_field_original_text()`: genizah_app.py:~12183 — extracts source text, handles phys_desc
- `dicta_client.py`: GOD_MODE, MAX_WORKERS, _split_by_words, _sanitize_text

## Test Data
- 8 test translations in pgp.db `pgp_translations` for pgpids 444-453
- libraries_translations.db: 136K+ records with English titles
- sys_id 990053401060205171: bilingual title with extracted English "Mishnah: Avot 2:9 – 16"

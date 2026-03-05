# Phase 46 Handoff — Batch Translation & Desktop Toggle

**Last updated:** 2026-03-05

## Status
- **46-01 through 46-04**: COMPLETE and committed
- **46-03 re-execution (batch translation)**: IN PROGRESS — catalog gap-fill 1,210/3,835 rows done
- **46-05**: Partially complete. Task 1 committed (`c8115f33`). Task 2 (verification) NOT passed.
- **Additional web fixes**: Committed in `0ea3f022` (clickable badges, browse sys_id support)

---

## Batch Translation Progress (2026-03-05 session)

### Changes Made
- **HE→EN few-shot** (`data/few_shot_he2en_scholarly.json`): 16 real scholarly pairs from `genizah_titles`. JA titles now transliterated (Kitab, Tafsir, Sharh, Qissat). Added מהר"ם → Maharam.
- **EN→HE few-shot** (`data/few_shot_en2he_scholarly.json`): Rebuilt with real genizah_titles pairs.
- **`shared/dicta_client.py`**: 429 retry (3 attempts, max 30s), caps Retry-After header.
- **`scripts/translate_fjms_catalog.py`**: Dedup (unique strings only), sequential + 3s throttle, EN→HE Latin-char filter (1,720→9), removed ThreadPoolExecutor.

### Translation State

| Category | Dir | Rows | Unique | Status |
|----------|-----|------|--------|--------|
| Title | he2en | 1,152 | 261 | **Done** |
| TitleHeb | en2he | 8 | 6 | **Done** |
| AuthorText | he2en | 50 | 50 | 50/178 |
| GenizahTitleEngTitle | he2en | 0 | 0 | Not started |
| PersonEngDesc | he2en | 0 | 0 | Not started |
| PersonHebDesc | en2he | 0 | 0 | Not started |

Resume: `python scripts/translate_fjms_catalog.py` (checkpoint auto-resumes)

If 429s persist, increase `REQUEST_DELAY` in script (try 5.0 or 10.0).

### CRITICAL: Other Scripts Need Same Fixes Before Running

1. **`scripts/translate_fjms_free_desc.py`** (~255K items) — Still uses ThreadPoolExecutor + parallel workers. Needs:
   - Sequential execution with `time.sleep(REQUEST_DELAY)` throttle
   - Deduplication of identical descriptions
   - Remove `--workers` / parallel execution
   - Keep existing SIGINT handler

2. **`shared/dicta_client.batch_translate()`** — Still uses ThreadPoolExecutor internally. Any script calling it will hit cascading 429s. Either rewrite to sequential+throttle or don't use it.

3. **Future PGP translation scripts** — Same pattern: sequential + 3s+ delay is the only safe approach for Dicta's free API.

**Key lesson**: Dicta rate-limits after ~90-100 requests regardless of concurrency. Parallel workers cause cascading 429 failures. Sequential + 3s delay is the safe pattern.

---

## What Works
1. **Web app**: All translation features work — toggle, clickable Translated/Original badges, browse by sys_id, Dicta translate buttons
2. **Desktop Browse tab**: Toggle "מתורגם"/"מקור" WORKS (rebuilds HTML via `_refresh_browse_extended_info`)
3. **Desktop Browse tab**: "הצג תרגומים" / "אל תציג תרגומים" links WORK (toggle global setting)
4. **Desktop Settings dialog**: OK/Cancel buttons added and working
5. **Desktop search results**: "Translated match" badge displays

## What Does NOT Work
1. **Desktop ResultDialog**: Clicking "מתורגם" badge does NOT toggle text
2. **Desktop ResultDialog**: "הצג תרגומים"/"אל תציג תרגומים" link does NOT refresh

## Root Cause Analysis
The ResultDialog's extended info HTML is built across multiple async stages:
- `on_enriched_data_loaded()` builds Ktiv/Oxford/Cambridge/FJMS sections
- PGP section appended either inline or via `_rd_update_extended_info_with_pgp()` (race handler)
- The HTML stored in QTextBrowser gets rewritten by Qt (different quotes, attributes, etc.)

The current approach stores `_rd_enriched_html_prefix` (pre-PGP HTML) and rebuilds by concatenating prefix + fresh PGP HTML. This doesn't work, likely because:
- The prefix is stored BEFORE the closing `</div>` wrapper but the full HTML wraps everything in a styled outer div
- Or `_rd_enriched_html_prefix` is not set when PGP arrives via the late-arrival path (`_rd_update_extended_info_with_pgp`)

## Failed Approaches
1. **Regex replacement on `toHtml()`**: Qt rewrites HTML extensively (attribute quotes, style normalization, added `<html><head><body>` wrapper). String/regex matching on `toHtml()` output is unreliable.
2. **Span ID replacement**: Same problem — Qt changes `id='foo'` to `id="foo"` and adds other attributes.
3. **Stored prefix + rebuild**: Current approach. Conceptually correct but may have timing/path issues.

## Recommended Approaches for Next Session

### Approach A: Store full rebuild data (RECOMMENDED)
Instead of trying to patch HTML, store ALL the data needed to rebuild the entire extended info:
- Store `self._rd_enriched_meta = meta` in `on_enriched_data_loaded()`
- `_rd_refresh_extended_info()` calls the same full rebuild logic as `on_enriched_data_loaded()` but only the HTML part (lines ~4524-4646)
- Extract the HTML-building portion of `on_enriched_data_loaded()` into `_rd_build_extended_html()` that both the original and refresh paths call

### Approach B: Use QTextBrowser cursor manipulation
Instead of setHtml(), use QTextCursor to find and replace specific text blocks. More surgical but complex.

### Approach C: Separate QTextBrowser for PGP section
Add a second small QTextBrowser widget just for the PGP metadata. Then refresh is trivial — just rebuild that one widget. Simplest change but affects layout.

### Approach D: Use a QLabel-based approach for toggle fields
Instead of embedding toggle links in HTML, use a QWidget overlay with QPushButtons for the toggle badges, positioned over the QTextBrowser. More Pythonic but layout-heavy.

## Key Code Locations
- `_build_pgp_extended_info_html()`: genizah_app.py:~11290 — builds PGP HTML with toggle links
- `_handle_toggle_trans()`: genizah_app.py:~11803 — sets toggle state, calls refresh
- `_refresh_browse_extended_info()`: genizah_app.py:~11825 — WORKS for browse tab
- `_rd_refresh_extended_info()`: genizah_app.py:~3700 — BROKEN for ResultDialog
- `on_enriched_data_loaded()`: genizah_app.py:4411 — builds ResultDialog extended info
- `_rd_update_extended_info_with_pgp()`: genizah_app.py:3668 — late PGP arrival handler
- `_on_rd_ext_link_clicked()`: genizah_app.py:4301 — ResultDialog link handler
- `_on_browse_ext_link_clicked()`: genizah_app.py:~11776 — Browse tab link handler
- `_trans_toggle_state`: dict on GenizahGUI, tracks per-field toggle {field: bool}

## Debug prints to remove
- genizah_app.py: `print(f"[DEBUG] Browse link clicked:` in `_on_browse_ext_link_clicked`
- genizah_app.py: `print(f"[DEBUG] toggle-trans:` in `_handle_toggle_trans`
- genizah_app.py: `print(f"[DEBUG] _refresh:` in `_refresh_browse_extended_info`

## Test Data
8 test translations inserted in `pgp_data/pgp.db` table `pgp_translations` for pgpids 444-453.
Shelfmarks: T-S 13J35.3 (sys_id 990051250670205171), AIU VII A 23, VII A 34, VII D 78, etc.

## Other Uncommitted Changes
- Settings dialog OK/Cancel buttons (genizah_app.py SettingsDialog.__init__)
- genizah_translations.py: "Don't show translations", "Original", "Enter shelfmark or NLI system ID"

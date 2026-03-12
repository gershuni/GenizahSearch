# Translation Statistics & Coverage Report

**Date:** 2026-03-12 (post-QC cleanup)
**Status:** Comprehensive audit after Round 1 + Round 2 batch translations

---

## Grand Totals

| Database | Translations | Source Records | Coverage |
|----------|-------------|----------------|----------|
| FJMS (fjms_enrichment.db) | 490,377 | ~1.01M translatable | 48.5% |
| PGP (pgp.db) | 34,797 desc + 31,268 types | 35,839 docs | 97.1% desc |
| Library Titles (libraries_translations.db) | 184,514 EN + 10,328 EN→HE | 216,942 records | 85.1% |
| **GRAND TOTAL** | **~751K translations** | **~1.27M source** | **~59%** |

---

## 1. FJMS Enrichment (fjms_enrichment.db)

### 1.1 Translation Counts (post-cleanup)

| field_name | Direction | Count | Source Table | Source Column |
|------------|-----------|-------|-------------|---------------|
| FreeDesc | HE→EN | 196,314 | catalog_free_desc | FreeDesc |
| RunningTitle | EN→HE | 133,767 | catalog_running_titles | RunningTitle |
| TextualFrame | HE→EN | 84,345 | catalog_textual_frames | TextualFrameHeb |
| FullText | EN→HE | 70,797 | catalog_full_texts | FullText |
| Title | HE→EN | 2,408 | catalog_fields (gap-fill) | TitleHeb→Title |
| PersonEngDesc | HE→EN | 1,163 | genizah_persons | HebDesc→EngDesc |
| PersonHebDesc | EN→HE | 702 | genizah_persons | EngDesc→HebDesc |
| GenizahTitleEngTitle | HE→EN | 624 | genizah_titles | OrgTitle→EngTitle |
| AuthorText | HE→EN | 247 | catalog_fields (gap-fill) | AuthorText |
| TitleHeb | EN→HE | 10 | catalog_fields (gap-fill) | Title→TitleHeb |
| **TOTAL** | | **490,377** | | |

Unique AlmaIds covered: 184,700 / 1,315,501 catalog_fields rows (14% of catalog rows have at least one translation).

### 1.2 Source Data & Coverage

| Source Table | Column | Total Rows | Non-null | len>=10 | len>=20 | Translated | Coverage (of len>=20) |
|-------------|--------|-----------|----------|---------|---------|------------|----------------------|
| catalog_free_desc | FreeDesc | 303,392 | 303,378 | 302,710 | 254,835 | 196,314 | 77.0% |
| catalog_running_titles | RunningTitle | 317,412 | 317,217 | 288,430 | 209,417 | 133,767 | 63.9% |
| catalog_full_texts | FullText | 94,939 | 94,939 | 94,937 | 94,860 | 70,797 | 74.6% |
| catalog_textual_frames | TextualFrameHeb | 298,740 | 298,740 | 263,193 | 221,259 | 84,345 | 38.1% |

### 1.3 What Was NOT Translated (and Why)

#### FreeDesc (58,521 gap from 254,835)

- **~56,700 were English-only source text** — the script translated HE→EN but had no language detection. English FreeDesc entries were sent to Dicta which echoed them back. These copies were deleted in QC cleanup. The source data is already in English so no translation is needed.
- ~1,800 remaining are short texts (under min_length=20) or genuine gaps.
- **Action needed:** None. English source texts serve the user directly. Future batch runs should add `has_hebrew()` filtering to skip English sources.

#### RunningTitle (155,650 gap from 288,430 at len>=10)

- **~183K non-English running titles** — the script uses `has_english(text, min_latin=3)` to select only English titles for EN→HE translation. Running titles in Hebrew were not sent to Dicta (they're already in the user's target language for Hebrew UI).
- ~22K were translated in Round 2 backfill (the gap closed from 183K to 155K).
- **Remaining gap:** Running titles that are pure Hebrew (no English content). These don't need EN→HE translation — they need HE→EN translation instead (not yet implemented for this field).

#### FullText (24,140 gap from 94,937 at len>=10)

- Same as RunningTitle: `has_english(text, min_latin=10)` filters out Hebrew-only full texts.
- **Remaining:** Hebrew-only full texts that need HE→EN translation (not yet implemented).

#### TextualFrame (178,848 gap from 263,193 at len>=10)

- Script uses `has_hebrew(text)` and checks that TextualFrameEng is NULL/empty/identical to Heb.
- ~84K Hebrew textual frames were translated HE→EN in Round 2.
- **Remaining:** ~179K frames where TextualFrameEng already exists and differs from TextualFrameHeb (i.e., an English version already exists in the source data — no translation needed).

#### Catalog Fields Gap-Fill (Title, TitleHeb, AuthorText)

- Only fills gaps: translates TitleHeb when Title is NULL, and vice versa.
- 3,170 total gap-fill translations. The remaining catalog_fields rows already have both Hebrew and English values.

#### Reference Tables (genizah_persons, genizah_titles)

- genizah_persons: 2,286 total. 1,163 HebDesc→EngDesc + 702 EngDesc→HebDesc = 1,865 translations. Remaining ~421 have both fields or neither.
- genizah_titles: 775 total. 624 OrgTitle→EngTitle translated. Remaining ~151 already have EngTitle or OrgTitle is empty.

### 1.4 QC Cleanup Summary (2026-03-12)

| Pass | Reason | Rows Deleted |
|------|--------|-------------|
| Pass 1 | copied_source (English echoed as "translation") | 61,834 |
| Pass 1 | script_mismatch (wrong script in output) | 5,134 |
| Pass 1 | near_copy (>90% character overlap) | 1,615 |
| Pass 1 | score < 0.5 (multiple severe flags) | 4,763 |
| Pass 2 | collapsed (>100 chars source → <15 chars target) | 442 |
| Pass 2 | word stuttering (>60% same word) | 300 |
| Pass 2 | stock hallucination (invented "N manuscripts" summary) | 15 |
| Pass 2 | sentence stuttering (same sentence 3+ times) | 10 |
| **TOTAL** | | **~65,905** (some overlap between passes) |

**Post-cleanup QC scores:**

| Dataset | Total | Flagged | % Flagged | Worst (<0.5) | Mean Score |
|---------|-------|---------|-----------|-------------|------------|
| FJMS | 490,377 | 35,221 | 7.2% | 0 | 0.986 |
| PGP | 34,797 | 1,833 | 5.3% | 41 | 0.993 |
| Titles | 82,481 | 2,788 | 3.4% | 38 | 0.993 |

Remaining flags are mostly minor: numbers_added (scholarly references), parens_dropped, brackets_dropped, length_ratio variations — common in scholarly translation and not hallucinations.

---

## 2. PGP Translations (pgp_data/pgp.db)

### 2.1 Description Translations (EN→HE)

| Metric | Count |
|--------|-------|
| Total documents | 35,839 |
| With English description | 35,832 |
| pgp_translations rows | 34,954 |
| description_he filled | 34,797 |
| description_he null/empty | 157 |
| **Coverage** | **97.1%** |

**157 null/empty description_he:** These were previously hallucinated translations that were manually nulled during Round 1 QA. Can be retranslated with `--retranslate-nulls` flag.

**885 documents with no pgp_translations row at all:** Short descriptions below min_length threshold, or documents added after the batch run.

### 2.2 Document Type Translations (EN→HE)

| Metric | Count |
|--------|-------|
| document_type_he filled | 31,268 |
| document_type_he not filled | 3,686 |
| **Coverage** | **87.2%** |

Document types use a **manual controlled mapping** (9 fixed values in `PGP_DOCUMENT_TYPE_HE` dict), not Dicta API. The 3,686 without translations have document types not in the mapping or NULL source types.

### 2.3 What Was NOT Translated

- 157 nulled hallucinations — retranslatable with improved prompts
- 885 missing rows — short/empty descriptions
- 3,686 missing document types — unmapped type values or NULL source
- **No HE→EN needed:** PGP descriptions are natively English

---

## 3. Library Titles (libraries_translations.db)

### 3.1 Coverage

| Metric | Count |
|--------|-------|
| libraries.csv total records | 216,942 |
| title_translations rows | 184,514 |
| english_title filled | 184,514 |
| hebrew_title filled | 184,514 |
| english_title_he filled | 10,328 |
| **Coverage (any English)** | **85.1%** |

### 3.2 Sources

| Source | Count | Description |
|--------|-------|-------------|
| extracted | 112,361 | Bilingual pairs extracted from libraries.csv (title had both HE+EN) |
| dicta | 72,153 | Hebrew-only titles translated HE→EN via Dicta API |
| **Total** | **184,514** | |

### 3.3 EN→HE Backfill (english_title_he column)

10,328 rows have English→Hebrew backfill translations. These are cases where the English title was the scholarly description (e.g., "Letter to David ha-Kohen he-haver") and the Hebrew title was just a generic label (e.g., "מכתבים"). The backfill provides a proper Hebrew rendering of the English content.

### 3.4 What Was NOT Translated (32,428 gap)

| Reason | Est. Count |
|--------|-----------|
| Records not in title_translations table | 32,428 |
| Likely: placeholder titles, non-Hebrew/English, or recent CSV additions | — |

These 32,428 records from libraries.csv have no entry in title_translations at all. They were likely excluded during the initial extraction because:
- Title field was empty or a placeholder
- Title was in a non-Hebrew/non-English script
- Record was added to libraries.csv after the translation batch

---

## 4. Translation Method Summary

| Method | Description | Scope |
|--------|-------------|-------|
| **Dicta LM 2.0 (batch)** | Few-shot prompted neural translation via API | All batch translations |
| **Bilingual extraction** | Parse existing HE+EN from libraries.csv | 112,361 library titles |
| **Manual mapping** | Fixed dict for small taxonomies | 9 PGP document types |
| **Language detection** | `has_english()`/`has_hebrew()` filters source by script | FJMS catalog_text only |
| **No detection** | Assumes source language from context | PGP (EN), FreeDesc (HE), library titles (HE) |

### Few-Shot Templates

| File | Direction | Used By |
|------|-----------|---------|
| data/few_shot_en2he_scholarly.json | EN→HE | PGP descriptions, FJMS RunningTitle, FJMS FullText |
| data/few_shot_he2en_scholarly.json | HE→EN | FJMS FreeDesc, FJMS TextualFrame, library titles |

---

## 5. Known Issues & Future Work

### Round 3 Gap-Closing Batch (2026-03-12)

**Completed locally:**
- RunningTitle EN→HE: 8,098 rows inserted (295 unique English terms, manual+Dicta mapping)

**Server batches prepared** (CSVs in `scripts/translation_gaps/`, script: `scripts/translate_gaps_server.py`):

| Batch | File | Rows | Direction | Est. Time |
|-------|------|------|-----------|-----------|
| freedesc_en | freedesc_en2he.csv | 23,389 | EN→HE | ~2h |
| freedesc_he | freedesc_he2en.csv | 3,048 | HE→EN | ~20min |
| fulltext_en | fulltext_en2he.csv | 4,971 | EN→HE | ~30min |
| fulltext_he | fulltext_he2en.csv | 14,680 | HE→EN | ~1.5h |
| rt | rt_he2en.csv | 159,910 | HE→EN | ~12h |
| **Total** | | **205,998** | | **~16h** |

**After server batches:** download results CSVs, run `scripts/merge_translation_results.py` to insert into DB, then QC pass.

### Gaps Investigated and Closed (2026-03-12)

| Gap | Original Estimate | Actual | Status |
|-----|-------------------|--------|--------|
| Library title gaps (32K) | 32,428 | 0 | Not a gap — all blank titles, correctly skipped |
| Library EN→HE backfill (102K) | ~102,045 | N/A | Not needed — hebrew_title already serves users |
| TextualFrame (179K) | ~179K | 0 | Not a gap — source table already has both Heb+Eng |
| PGP 885 stubs | 885 | 0 | Not a gap — 1-2 word placeholders |
| PGP 3,686 missing types | 3,686 | 0 | Not a gap — source has no document_type |
| RT EN→HE (295 unique) | 9,112 | 8,098 | Done locally (1,014 already existed) |

### Still Pending

1. **PGP nulled descriptions** (157 rows) — retranslatable with `--retranslate-nulls`, Arabic diacritics may cause issues
2. **Wiring gaps**: RunningTitle/FullText translations not yet displayed in FjmsCatalogDialog; TextualFrame translations not yet wired to browse UI

### Translation Scripts Missing Language Detection

| Script | Issue | Fix |
|--------|-------|-----|
| translate_fjms_free_desc.py | No `has_hebrew()` check — translates English sources too | Add language filter |
| translate_pgp_descriptions.py | No language check — assumes all English | OK (PGP is English-only) |

---

## 6. Database Sizes

| Database | Size | Translations |
|----------|------|-------------|
| fjms_enrichment.db | 1.1 GB | 498,475 (+8,098 RT EN→HE) |
| pgp.db | 165 MB | 34,954 |
| libraries_translations.db | 84 MB | 184,514 |
| **Total** | **~1.35 GB** | **~718K** |

After Round 3 server batches complete: estimated ~924K total translations.

Backups:
- `fist_data/fjms_enrichment_pre_qc_cleanup.db` (556,282 translations pre-cleanup)
- `fist_data/fjms_enrichment_pre_round3.db` (490,377 translations pre-Round 3)

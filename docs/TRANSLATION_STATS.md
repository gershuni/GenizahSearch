# Translation Statistics & Coverage Report

**Date:** 2026-03-13 (final, post-Round 3 + retranslation)
**Status:** All translation rounds complete. Ready for v6.5.0 release.

---

## Grand Totals

| Database | Translations | Coverage |
|----------|-------------|----------|
| FJMS (fjms_enrichment.db) | 704,138 | See per-field below |
| PGP (pgp.db) | 34,954 (34,797 desc + 31,268 types) | 97.1% desc |
| Library Titles (libraries_translations.db) | 184,514 EN + 10,328 EN→HE | 85.1% |
| **GRAND TOTAL** | **923,606** | |

---

## 1. FJMS Enrichment (fjms_enrichment.db)

### 1.1 Translation Counts (final)

| field_name | Direction | Count | Source Table |
|------------|-----------|-------|-------------|
| FreeDesc | HE→EN | 199,349 | catalog_free_desc |
| RunningTitle | HE→EN | 159,845 | catalog_running_titles |
| RunningTitle | EN→HE | 141,865 | catalog_running_titles |
| TextualFrame | HE→EN | 84,345 | catalog_textual_frames |
| FullText | EN→HE | 75,560 | catalog_full_texts |
| FullText | HE→EN | 14,648 | catalog_full_texts |
| FreeDesc | EN→HE | 23,372 | catalog_free_desc |
| Title | HE→EN | 2,408 | catalog_fields |
| PersonEngDesc | HE→EN | 1,163 | genizah_persons |
| PersonHebDesc | EN→HE | 702 | genizah_persons |
| GenizahTitleEngTitle | HE→EN | 624 | genizah_titles |
| AuthorText | HE→EN | 247 | catalog_fields |
| TitleHeb | EN→HE | 10 | catalog_fields |
| **TOTAL** | | **704,138** | |

### 1.2 By Model Version

| Model | Count | Notes |
|-------|-------|-------|
| dictalm2.0 | 490,377 | Rounds 1 + 2 |
| dictalm2.0-round3 | 205,113 | Round 3 gap-closing |
| manual+dictalm2.0 | 8,098 | 295 RT terms, hand-mapped |
| dictalm2.0-retranslate | 550 | Flagged items retranslated |

### 1.3 Coverage by Field

| Field | Translated AlmaIds | Total AlmaIds | Coverage |
|-------|-------------------|---------------|----------|
| RunningTitle | 162,111 | 162,188 | **100.0%** |
| FullText | 85,063 | 85,313 | **99.7%** |
| FreeDesc | 150,431 | 170,327 | **88.3%** |
| TextualFrame | 26,343 | — | Bidirectional (source has both langs) |
| Title | 1,220 | — | Gap-fill only |

### 1.4 What Remains Untranslated

**FreeDesc (19,896 AlmaIds, 11.7%):**
- Mostly mixed-language texts, very short entries (<20 chars), or Arabic-script content that Dicta cannot handle.

**FullText (250 AlmaIds, 0.3%):**
- English-only codicological descriptions where Dicta echoes back the source. These are inherently untranslatable by the current model — the source text is already in English.

**RunningTitle (77 AlmaIds, <0.1%):**
- Edge cases: mixed script, very short, or unusual characters.

### 1.5 QC Summary

**Rounds 1–2 cleanup (2026-03-12):**

| Reason | Rows Deleted |
|--------|-------------|
| copied_source (English echoed as "translation") | 61,834 |
| script_mismatch (wrong script in output) | 5,134 |
| near_copy (>90% character overlap) | 1,615 |
| score < 0.5 (multiple severe flags) | 4,763 |
| collapsed (>100 chars → <15 chars) | 442 |
| word/sentence stuttering | 310 |
| stock hallucination ("N manuscripts" summary) | 15 |
| **Total** | **~65,905** |

**Round 3 cleanup (2026-03-13):**

| Action | Count |
|--------|-------|
| Gibberish rows deleted (post-merge) | 34 |
| Bad translations deleted (length_ratio_low, no_words, copies) | 696 |
| Retranslated via Dicta | 550 |
| QC rejected on retranslation (untranslatable by Dicta) | 89 |
| **Net loss from Round 3 QC** | **-146** |

**Post-cleanup QC scores:**

| Dataset | Total | Flagged | % Flagged | Mean Score |
|---------|-------|---------|-----------|------------|
| FJMS | 704,138 | ~35K | ~5% | 0.99 |
| PGP | 34,797 | 1,833 | 5.3% | 0.993 |
| Titles | 82,481 | 2,788 | 3.4% | 0.993 |

Remaining flags are minor: numbers_added (scholarly references), parens/brackets variation — common in scholarly translation.

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

157 null/empty: previously hallucinated translations manually nulled during QA.
885 documents with no row: short descriptions below min_length or documents added after batch.

### 2.2 Document Type Translations (EN→HE)

| Metric | Count |
|--------|-------|
| document_type_he filled | 31,268 |
| document_type_he not filled | 3,686 |
| **Coverage** | **87.2%** |

Uses manual controlled mapping (9 values in `PGP_DOCUMENT_TYPE_HE` dict). The 3,686 without translations have NULL source types.

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
| extracted | 112,361 | Bilingual pairs extracted from libraries.csv |
| dicta | 72,153 | Hebrew-only titles translated HE→EN via Dicta API |

### 3.3 Untranslated (32,428 records)

Records not in title_translations at all — empty or placeholder titles, non-Hebrew/English script, or recent CSV additions.

---

## 4. Translation Rounds

| Round | Date | Scope | Translations |
|-------|------|-------|-------------|
| **Round 1** | 2026-03-05–07 | Libraries 184K, PGP 35K, FJMS catalog 4K, FJMS FreeDesc 255K | ~478K |
| **Round 2** | 2026-03-08 | FJMS RunningTitle EN→HE 107K, FullText EN→HE 46K | ~153K |
| **Round 3** | 2026-03-12–13 | Gap-closing: FreeDesc both dirs, FullText both dirs, RT HE→EN 160K | ~206K |
| **RT local** | 2026-03-12 | 295 English RunningTitles hand-mapped to Hebrew (8,098 rows) | 8K |
| **Retranslation** | 2026-03-13 | QC-flagged bad rows retranslated via Dicta | 550 |
| **QC cleanup** | 2026-03-12–13 | Hallucinations, copies, gibberish, stuttering deleted | -66K |

### Translation Method Summary

| Method | Description | Scope |
|--------|-------------|-------|
| Dicta LM 2.0 (batch) | Few-shot prompted neural translation | All batch translations |
| Bilingual extraction | Parse existing HE+EN from libraries.csv | 112,361 library titles |
| Manual mapping | Fixed dict for small taxonomies | 9 PGP document types, 295 RT terms |
| Language detection | `has_english()`/`has_hebrew()` per-row | FJMS source filtering |

---

## 5. Database Sizes

| Database | Size | Translations |
|----------|------|-------------|
| fjms_enrichment.db | 1.17 GB | 704,138 |
| pgp.db | 164 MB | 34,954 |
| libraries_translations.db | 84 MB | 184,514 |
| **Total** | **~1.42 GB** | **923,606** |

---

## 6. Remaining Known Issues

1. **PGP nulled descriptions** (157 rows) — retranslatable with improved prompts
2. **89 untranslatable FJMS rows** — English codicological descriptions that Dicta echoes back; would need a different model
3. **FreeDesc 11.7% gap** — mixed-language/Arabic content beyond current model capability

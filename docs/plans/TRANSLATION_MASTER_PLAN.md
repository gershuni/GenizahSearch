# Translation Master Plan — GenizahSearch

> Complete plan for translating all user-facing text fields across all data sources.
> Goal: Full bilingual (Hebrew/English) access to all metadata in both apps.

**Created:** 2026-03-05
**Status:** In Progress (Phase 46 of v6.5.0)

---

## Executive Summary

GenizahSearch has ~242K unique strings across 6 data sources that need translation via Dicta API, plus ~105K bilingual titles where English can be extracted by parsing (no API needed). Total estimated Dicta API time: **103–202 hours** depending on whether FJMS free descriptions are included.

### Priority Order

| Priority | Source | Unique Strings | Direction | API Hours | User Impact |
|----------|--------|---------------|-----------|-----------|-------------|
| **P0** | libraries.csv English extraction | 54,931 | parse only | 0 | Instant English for 57% of search results |
| **P1** | FJMS catalog remaining fields | 2,841 | he2en | 2.4h | Catalog browse completeness |
| **P2** | libraries.csv Hebrew-only titles | 34,709 | he2en | 28.9h | English for remaining 39% of search results |
| **P3** | PGP descriptions | 31,735 | en2he | 26.4h | Hebrew descriptions in browse/search |
| **P4** | libraries.csv English→Hebrew | 54,931 | en2he | 45.8h | Hebrew for bilingual records (optional — already Hebrew) |
| **P5** | FJMS free descriptions | 117,959 | he2en | 98.3h | English for scholarly codicological notes |

P0 is free. P1 is a quick win (~2.4h). P2+P3 are the high-value workhorses (~55h). P4 is low priority (records already have Hebrew). P5 is optional and expensive.

---

## Data Source Inventory

### 1. libraries.csv — Manuscript Titles (216,906 records)

The `titles_non_placeholder` column (MARC 245+246 concatenation) has three categories:

| Category | Rows | Unique Strings | Action |
|----------|------|---------------|--------|
| Bilingual (English extractable via `;`) | 104,626 | 54,931 English parts | **Parse — no API** |
| Mixed-script (English embedded in Hebrew) | 6,876 | ~5,000 | Smarter regex parse |
| Hebrew-only | 71,981 | 34,709 | Dicta he2en |
| English-only | 2,559 | ~2,200 | Already English |
| No title | 32,392 | — | Nothing to translate |

**Library distribution of Hebrew-only titles:**

| Library | Hebrew-Only | % of Library | Notes |
|---------|------------|-------------|-------|
| RNL (Russia) | 16,600 | 99.3% | Never cataloged in English |
| JTS | 16,200 | 53.7% | Partially cataloged |
| CUL | 24,200 | 19.0% | Mostly bilingual already |
| Others | ~14,980 | varies | |

**Top repeated Hebrew-only strings (dedup leverage):**
- `פיוט.` — 8,077 occurrences (1 API call serves 8K rows)
- `ספרות חז"ל.` — 873
- `מקרא [טקסט].` — 603
- 83.6% of unique strings appear only once (long tail of specific titles)

**Storage:** New `libraries_translations` table (or extend libraries.csv with English column).

### 2. FJMS Catalog Fields (fjms_enrichment.db)

| Field | Total Rows | Unique | Already Done | Remaining | Direction |
|-------|-----------|--------|-------------|-----------|-----------|
| catalog.Title | 1,773 | 1,283 | 261 | **1,022** | he2en |
| catalog.AuthorText | 1,435 | 204 | 150 | **54** | he2en |
| genizah_titles.OrgTitle | 775 | 615 | 149 (have EngTitle) | **615** | he2en |
| genizah_persons.HebDesc | 2,286 | 1,150 | 1,121 (have EngDesc) | **1,150** | he2en |
| **Subtotal** | | | | **2,841** | |

**Storage:** `fjms_translations` table (existing, working).

### 3. FJMS Free Descriptions (fjms_enrichment.db)

| Table | Total Rows | Unique | Avg Length | Direction |
|-------|-----------|--------|-----------|-----------|
| catalog_free_desc | 303,392 | 117,959 | 138 chars | he2en |

These are scholarly codicological notes (material, damage, ink, script descriptions). Lower priority — not prominently displayed.

**Storage:** `fjms_translations` table with `field_name='FreeDesc'`.

### 4. PGP Descriptions (pgp.db)

| Field | Total | Unique | Direction |
|-------|-------|--------|-----------|
| documents.description | 35,838 | 31,735 | en2he |
| documents.document_type | 9 types | 1 remaining | en2he (manual mapping) |

PGP descriptions are English scholarly text with transliterated terms. Low dedup factor (1.13x — nearly all unique).

**Storage:** `pgp_translations` table (existing, 8 test rows).

### 5. FJMS Bibliography (fjms_enrichment.db)

| Table | Total Rows | Direction |
|-------|-----------|-----------|
| bibliography | 542,000+ | Mixed — deferred |

Out of scope for now. Bibliography entries are structured (author, title, year) and many are in English already.

### 6. MARC 500 Notes (NLI API — optional future)

English catalog descriptions available via `https://iiif.nli.org.il/IIIFv21/marc/bib/{system_number}` (public, no API key). Richer than title-only text but requires 217K HTTP fetches (~18 hours). Deferred.

---

## Implementation Phases

### Phase A: English Extraction from Bilingual Titles (P0) — No API

**Goal:** Extract English from ~111K bilingual libraries.csv titles via parsing.

**Steps:**
1. Build extraction script `scripts/extract_libraries_english.py`
2. Parse semicolon-delimited titles: split on ` ; ` (space-semicolon-space)
3. For each part, classify as Hebrew (has \u0590-\u05FF), English (has A-Za-z, no Hebrew), or mixed
4. Store results in new `libraries_translations.csv` or SQLite table
5. Handle mixed-script titles (6,876) with smarter regex: extract English runs outside Hebrew quotations
6. Validate: spot-check 50 random extractions

**Output:** ~105K records with English title field. Zero API cost.

**Storage options:**
- Option A: New `libraries_translations.db` SQLite sidecar (consistent with other sidecars)
- Option B: Add column to libraries.csv (simple but changes core file)
- Option C: Store in pgp.db or fjms_enrichment.db (wrong domain)
- **Recommended: Option A** — new sidecar `libraries_translations.db` with schema:
  ```sql
  CREATE TABLE title_translations (
      system_number TEXT PRIMARY KEY,
      english_title TEXT,
      hebrew_title TEXT,  -- original from libraries.csv
      source TEXT,         -- 'extracted' | 'dicta' | 'manual'
      translated_at TEXT
  );
  ```

### Phase B: FJMS Catalog Quick Wins (P1) — 2.4 hours API

**Goal:** Complete remaining FJMS catalog field translations.

**Steps:**
1. Resume `scripts/translate_fjms_catalog.py` (checkpoint auto-resumes)
2. Categories remaining: AuthorText (54), GenizahTitleEngTitle (615), PersonEngDesc (1,150), PersonHebDesc (~remaining)
3. All use existing infrastructure — no new code needed

**Prerequisite:** Phase 46-03 batch script running (currently in progress).

### Phase C: Hebrew-Only Title Translation (P2) — 28.9 hours API

**Goal:** Translate 34,709 unique Hebrew-only titles from libraries.csv via Dicta.

**Steps:**
1. Build `scripts/translate_libraries_titles.py`:
   - Read libraries.csv, filter Hebrew-only titles
   - Deduplicate: group by title string → translate each unique string once
   - Sequential + 3s throttle (established safe pattern)
   - Checkpoint/resume with JSON (same pattern as translate_fjms_catalog.py)
   - Write results to `libraries_translations.db`
2. Few-shot template: Use existing `few_shot_he2en_scholarly.json` (already has genizah vocabulary)
3. Apply: map translated unique strings back to all 71,981 rows

**Dedup leverage:** 71,981 rows → 34,709 unique strings (2.07x). Top string `פיוט.` covers 8,077 rows alone.

**Estimated runtime:** 34,709 × 3s = ~28.9 hours. Can run overnight in 2 sessions.

### Phase D: PGP Description Translation (P3) — 26.4 hours API

**Goal:** Translate 31,735 unique PGP descriptions from English to Hebrew.

**Steps:**
1. Fix `scripts/translate_pgp_descriptions.py`:
   - **Refactor from ThreadPoolExecutor to sequential + 3s throttle** (critical — same fix needed as translate_fjms_free_desc.py)
   - Keep existing checkpoint/resume mechanism
   - Keep existing document_type manual mapping (only 1 type remaining)
2. Deduplicate descriptions before API calls (1.13x factor — minimal savings but still worth it)
3. Run sequentially with checkpoint

**Estimated runtime:** 31,735 × 3s = ~26.4 hours.

### Phase E: Service Layer & UI Integration

**Goal:** Wire translated data into both apps.

**Steps:**
1. Extend `TranslationService` to read from `libraries_translations.db`
   - `get_title_translation(system_number)` → English title
   - `get_title_translations_batch(system_numbers)` → dict
   - `search_by_translated_title(query, lang)` → set of system_numbers
2. Web app integration:
   - Search results: show English title when translation toggle ON
   - Browse page: show translated title
   - Search: include translated titles in search scope
3. Desktop app integration (same as web, pending 46-05 ResultDialog fix)
4. FTS index on translations for search-within-translations

### Phase F: FJMS Free Descriptions (P5) — 98.3 hours API (optional)

**Goal:** Translate 117,959 unique scholarly descriptions.

**Steps:**
1. Refactor `scripts/translate_fjms_free_desc.py`:
   - **Remove ThreadPoolExecutor** (critical — flagged in HANDOFF.md)
   - Convert to sequential + 3s throttle
   - Add deduplication (303K rows → 118K unique)
   - Keep SIGINT handler and checkpoint
2. Run over ~4 days (can be backgrounded)

**Decision point:** These are codicological notes not prominently displayed. Cost-benefit analysis needed.

---

## Cross-Cutting Concerns

### Deduplication Strategy

All scripts must deduplicate BEFORE calling the API:

| Source | Raw Rows | Unique | Dedup Factor | API Calls Saved |
|--------|---------|--------|-------------|----------------|
| libraries.csv Hebrew titles | 71,981 | 34,709 | 2.07x | 37,272 |
| FJMS catalog fields | ~6,269 | 2,841 | 2.21x | 3,428 |
| PGP descriptions | 35,838 | 31,735 | 1.13x | 4,103 |
| FJMS free descriptions | 303,392 | 117,959 | 2.57x | 185,433 |

Cross-source dedup yields negligible savings (only 5 overlapping strings across sources).

### Rate Limiting (Dicta API)

**Established safe pattern:**
- Sequential execution (no ThreadPoolExecutor)
- `time.sleep(3.0)` between requests minimum
- 429 retry: 3 attempts with exponential backoff (3s, 6s, 12s)
- Cap `Retry-After` header to 30s max

**Scripts that need refactoring before use:**
- `translate_fjms_free_desc.py` — still parallel
- `translate_pgp_descriptions.py` — still parallel
- `shared/dicta_client.batch_translate()` — parallel (don't use)

### Checkpoint/Resume Pattern

All scripts use:
```python
# JSON checkpoint: {"completed": {...}, "counts": {...}, "saved_at": "ISO8601"}
# Atomic write: temp file + os.replace()
# SIGINT handler: save checkpoint on Ctrl+C
# Resume: load checkpoint at startup, skip completed IDs
```

### Few-Shot Templates

| File | Direction | Pairs | Domain |
|------|-----------|-------|--------|
| `data/few_shot_he2en_scholarly.json` | he2en | 16 | Genizah titles, JA transliteration |
| `data/few_shot_en2he_scholarly.json` | en2he | ~16 | Genizah titles, scholarly terms |

May need additional templates for:
- Short Hebrew genre labels (P2) — `פיוט.` → "Piyyut"
- English scholarly descriptions (P3) — contains transliterated terms, dates, places
- Codicological notes (P5) — material, damage, script terminology

### Storage Architecture

```
libraries_translations.db (NEW)
  └── title_translations (system_number PK, english_title, hebrew_title, source)

pgp_data/pgp.db (EXISTING)
  └── pgp_translations (pgpid PK, description_he, document_type_he)

fist_data/fjms_enrichment.db (EXISTING)
  └── fjms_translations (id PK, alma_id, field_name, signature_id, original/translated)
```

---

## Execution Timeline

### Wave 1: Quick Wins (Day 1)
- [x] FJMS catalog titles (done — 1,152/1,773)
- [ ] FJMS catalog authors (54 remaining — minutes)
- [ ] Phase A: Extract English from bilingual titles (scripting — hours)

### Wave 2: Core Translation (Days 2–5)
- [ ] Phase B: FJMS genizah_titles + persons (2,841 strings — ~2.4h)
- [ ] Phase C: libraries.csv Hebrew-only titles (34,709 strings — ~29h, 2 overnight runs)
- [ ] Phase D: PGP descriptions (31,735 strings — ~26h, 2 overnight runs)

### Wave 3: Integration (Days 6–7)
- [ ] Phase E: TranslationService extensions + UI wiring
- [ ] Fix 46-05 ResultDialog toggle (desktop)
- [ ] Verification and testing

### Wave 4: Optional (Week 2+)
- [ ] Phase F: FJMS free descriptions (117,959 strings — ~98h, 4+ days)
- [ ] MARC 500 notes harvesting (18h fetch, then translation)

---

## Script Inventory & Status

| Script | Status | Execution Model | Needs Fix? |
|--------|--------|----------------|------------|
| `scripts/extract_libraries_english.py` | **NEW — to build** | Parse only | — |
| `scripts/translate_libraries_titles.py` | **NEW — to build** | Sequential + throttle | — |
| `scripts/translate_fjms_catalog.py` | Working | Sequential + throttle | No |
| `scripts/translate_fjms_free_desc.py` | Broken | Parallel (ThreadPoolExecutor) | **Yes — refactor to sequential** |
| `scripts/translate_pgp_descriptions.py` | Untested | Parallel (ThreadPoolExecutor) | **Yes — refactor to sequential** |

---

## Success Criteria

- [ ] 100% of libraries.csv titled records have an English translation (extracted or Dicta)
- [ ] 100% of FJMS catalog fields (Title, AuthorText, genizah_titles, genizah_persons) translated
- [ ] 100% of PGP descriptions have Hebrew translations
- [ ] TranslationService reads from all 3 translation stores
- [ ] Both web and desktop apps display translated text when toggle is ON
- [ ] Translation toggle persists across sessions
- [ ] All existing tests pass

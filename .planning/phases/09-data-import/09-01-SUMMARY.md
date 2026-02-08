---
phase: 09-data-import
plan: 01
subsystem: data-import
tags: [supabase, migration, sql, csv, import-pipeline, pgp]
depends_on:
  requires: [08-foundation]
  provides: [schema-migrations, import-script]
  affects: [09-02-execute-import]
tech-stack:
  added: []
  patterns: [multi-pass-pipeline, dry-run-execute, batch-upsert, bom-handling]
key-files:
  created:
    - migrations/add_full_pgp_columns.sql
    - migrations/create_footnotes_table.sql
    - scripts/import_pgp_full.py
  modified: []
decisions:
  - id: DEC-09-01-01
    decision: "Footnotes with empty doc_relation (177 records) excluded from import since the unique constraint requires doc_relation NOT NULL"
    context: "footnotes.csv has 177 records with empty doc_relation; these are bibliographic references without a relation type"
  - id: DEC-09-01-02
    decision: "Footnote deduplication by (pgpid, source_slug, doc_relation) removes 1,442 duplicates, yielding 22,764 valid records from 24,383 loaded"
    context: "Raw footnotes have duplicates that would violate the UNIQUE constraint; dedup before upsert is required"
metrics:
  duration: "~7 min"
  completed: "2026-02-08"
---

# Phase 9 Plan 01: Schema Migration & Import Script Summary

**One-liner:** SQL migrations for 13 new columns + footnotes table, and 1,013-line multi-pass import script validated at 35,839 docs / 9,364 sources / 22,764 footnotes / 36,112 fragment links

## What Was Done

### Task 1: Schema Migration SQL Files

Created two idempotent SQL migration files for Supabase SQL Editor:

**`migrations/add_full_pgp_columns.sql`:**
- 8 new columns on `documents` table: scholarship_records, shelfmarks_historic, language_note, doc_date_calendar, inferred_date_notes, has_transcription, has_translation, input_by
- 5 new columns on `document_fragments` table: collection, library, library_abbrev, fragment_url, iiif_url
- All use `ADD COLUMN IF NOT EXISTS` for safe re-runs
- Column comments for all 13 new columns

**`migrations/create_footnotes_table.sql`:**
- Creates `document_footnotes` table with 10 columns (id, pgpid, source, source_slug, doc_relation, location, url, notes, content, content_length, created_at)
- Unique constraint: `(pgpid, source_slug, doc_relation)`
- Indexes: pgpid and (pgpid, doc_relation)
- RLS enabled with public SELECT policy
- Uses `DO $$ IF NOT EXISTS` pattern matching existing migration style

### Task 2: Comprehensive Import Script

Created `scripts/import_pgp_full.py` (1,013 lines) -- a single multi-pass import pipeline replacing the two v1 scripts.

**Dry-run validation results:**

| Table | Records | Notes |
|-------|---------|-------|
| documents | 35,839 | All PGP documents, 7,020 with merged transcription text |
| document_sources | 9,364 | 7,664 editions + 1,696 translations (733 Hebrew, 963 English) |
| document_footnotes | 22,764 | After dedup of 1,442 duplicates and 177 missing-doc_relation |
| document_fragments | 36,112 | 94.3% match rate (2,168 unmatched) |

**Key features:**
- `--dry-run` (default): validates all data, reports statistics, no database writes
- `--execute`: 4-pass FK-safe pipeline (documents -> sources -> footnotes -> fragments)
- Merges transcription text from transcriptions_linked.csv into document records (7,020 docs)
- Full document_sources re-upsert on `pgpid,source_scholar,doc_relation` constraint
- Fragment metadata (collection, library, URLs) included in fragment link records
- Before/after count capture and verification report generation
- Reuses proven v1 patterns: batch upsert (500), BOM handling, shelfmark normalization

## Decisions Made

| ID | Decision | Rationale |
|----|----------|-----------|
| DEC-09-01-01 | Excluded 177 footnotes with empty doc_relation | doc_relation is NOT NULL in schema; these are untyped bibliography entries |
| DEC-09-01-02 | Footnote dedup removes 1,442 duplicates (24,383 -> 22,764) | UNIQUE constraint on (pgpid, source_slug, doc_relation) requires dedup |

## Deviations from Plan

None -- plan executed exactly as written.

## Task Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Create schema migration SQL files | dcc261c | migrations/add_full_pgp_columns.sql, migrations/create_footnotes_table.sql |
| 2 | Build comprehensive import script | 7aa2162 | scripts/import_pgp_full.py |

## Verification Results

All success criteria met:

- [x] `python scripts/import_pgp_full.py --dry-run` completes without errors
- [x] Migration SQL files use idempotent patterns (IF NOT EXISTS / DO blocks)
- [x] All column names and types match between migration SQL and import script
- [x] Document count: 35,839 (matches research exactly)
- [x] Document sources count: 9,364 (matches transcriptions_linked.csv exactly)
- [x] Fragment match rate: 94.3% (consistent with research 94.0%)
- [x] Footnote count: 22,764 (after dedup from 24,383 loaded; consistent with research ~24,388)
- [x] Script includes document_sources pass using transcriptions_linked.csv
- [x] Document records include transcription text merged from transcriptions_linked.csv (7,020 docs)

## Next Phase Readiness

Plan 09-02 can now execute these migrations and the import script:
1. Run `add_full_pgp_columns.sql` in Supabase SQL Editor
2. Run `create_footnotes_table.sql` in Supabase SQL Editor
3. Run `python scripts/import_pgp_full.py --execute` with SUPABASE_SERVICE_KEY set

## Self-Check: PASSED

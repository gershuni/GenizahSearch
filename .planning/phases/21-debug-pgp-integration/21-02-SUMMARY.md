---
phase: 21-debug-pgp-integration
plan: 02
subsystem: data-import
tags: [schema-migration, pgp-text, sections-jsonb, import-script]

# Dependency graph
requires:
  - phase: 21-debug-pgp-integration
    plan: 01
    provides: parse_html_sections function for HTML parsing
---

## Summary

Created SQL migration adding `sections` JSONB, `source_language`, and `source_direction` columns to `document_sources`, plus import script that clones pgp-text repo and populates structured section data.

## Key Results

- **9,068 source records updated** (7,470 editions + 1,599 translations) across 6,894 PGPIDs
- **Zero parse errors** on 9,497 HTML files
- 401 PGPIDs in pgp-text not yet in our DB (expected — different import scope)
- 1 transient connection error (pgpid 6478) — non-blocking

## Artifacts

### key-files

created:
  - migrations/add_sections_column.sql — ALTER TABLE for sections JSONB + metadata columns
  - scripts/import_pgp_sections.py — Clone pgp-text, parse HTML, populate sections column

modified:
  - (none — script and migration are new files)

### Deviations

1. **Windows NTFS clone failure**: pgp-text repo has filenames with colons in annotations/ directory. Fixed with sparse checkout excluding annotations.
2. **Upsert NOT NULL violation**: Original upsert tried to insert new rows with null `content`. Fixed to use `update+match` instead, targeting only sections/language/direction columns on existing records.

## Commits

- `2af0a9c` — feat(21-02): create SQL migration and import script for pgp-text sections
- `b38258e` — fix(21-02): use update+match instead of upsert to avoid NOT NULL violation

## Verification

- SQL migration executed in Supabase Dashboard
- Import completed: 9,068/9,069 records updated
- Sample data verified in report (PGPIDs 444, 445, 446 show correct Hebrew/JA text with canvas structure)

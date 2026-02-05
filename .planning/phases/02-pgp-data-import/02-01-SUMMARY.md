---
phase: 02
plan: 01
subsystem: database
tags: [postgresql, migration, supabase, pgp-data]
depends:
  requires: [01-01]
  provides: [page_info-column]
  affects: [02-02]
tech-stack:
  added: []
  patterns: [idempotent-migration]
key-files:
  created:
    - migrations/add_page_info_column.sql
  modified:
    - docs/guides/SUPABASE_GUIDE.md
decisions:
  - "IF NOT EXISTS pattern via DO block for safe migration re-runs"
metrics:
  duration: 3 min
  completed: 2026-02-05
---

# Phase 2 Plan 1: Add page_info Column Summary

**One-liner:** Idempotent migration adding page_info TEXT column to document_fragments for recto/verso storage

## What Was Built

Added the `page_info` column to the `document_fragments` table to store recto/verso/folio information for each fragment within a PGP document.

### Migration File

**`migrations/add_page_info_column.sql`**
- Uses DO block with IF NOT EXISTS check for safe re-runs
- Adds TEXT column for flexible page info storage
- Includes column comment for self-documentation

### Documentation Update

**`docs/guides/SUPABASE_GUIDE.md`**
- Added page_info column to document_fragments table documentation
- Column description: "Page/folio info (recto, verso, recto and verso)"

## Commits

| Hash | Type | Description |
|------|------|-------------|
| 49c64e2 | feat | Migration file for page_info column |
| d9f2235 | docs | Document page_info in SUPABASE_GUIDE |

## Deviations from Plan

None - plan executed exactly as written.

## Technical Notes

The migration uses a DO block to check for column existence before adding it:

```sql
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'document_fragments' AND column_name = 'page_info'
    ) THEN
        ALTER TABLE document_fragments ADD COLUMN page_info TEXT;
    END IF;
END $$;
```

This pattern ensures the migration can be safely re-run without errors.

## Next Steps

1. **User action required:** Run `migrations/add_page_info_column.sql` in Supabase SQL Editor
2. Plan 02-02 will implement the import script that populates this column

## Next Phase Readiness

**Ready for:** Plan 02-02 (Import script) - blocked only on migration execution
**Blockers:** Migration must be run in Supabase before import can proceed

---
phase: 01-database-schema
plan: 01
subsystem: database
tags: [postgresql, supabase, rls, jsonb, generated-columns]

# Dependency graph
requires: []
provides:
  - documents table with pgpid PK and all metadata columns
  - document_fragments table linking documents to sys_ids
  - RLS policies for public read access
  - Migration file ready for Supabase SQL Editor
affects: [02-data-import, 03-api-integration, 04-ui-updates]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - GENERATED ALWAYS AS for computed columns (pgp_url)
    - JSONB with GIN index for array filtering (tags)
    - Denormalized shelfmark in child table for display efficiency

key-files:
  created:
    - migrations/add_pgp_documents_tables.sql
  modified:
    - supabase_setup.sql
    - docs/guides/SUPABASE_GUIDE.md

key-decisions:
  - "pgpid as natural PRIMARY KEY (not synthetic UUID) - matches PGP data source"
  - "GENERATED pgp_url column - computed URL avoids data duplication"
  - "JSONB tags with GIN index - flexible tag filtering without join table"
  - "Denormalized shelfmark in document_fragments - display optimization"

patterns-established:
  - "System tables (no user_id) use public SELECT policies only"
  - "Multi-fragment linkage via sys_id TEXT matching libraries.csv"

# Metrics
duration: 8min
completed: 2026-02-05
---

# Phase 1 Plan 1: PGP Documents Schema Summary

**PostgreSQL tables for PGP document storage with pgpid natural key, JSONB tags, GENERATED url, and RLS public-read policies**

## Performance

- **Duration:** 8 min
- **Started:** 2026-02-05T17:15:16Z
- **Completed:** 2026-02-05T17:23:XX
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments
- Created `documents` table storing PGP metadata, transcriptions, and computed URL
- Created `document_fragments` table linking PGP documents to GenizahSearch sys_ids
- Established RLS pattern for system data (public read, service role write)
- Documented new tables in SUPABASE_GUIDE.md with column descriptions

## Task Commits

Each task was committed atomically:

1. **Task 1: Create migration file for PGP documents tables** - `d587246` (feat)
2. **Task 2: Update supabase_setup.sql for fresh deployments** - `2b95571` (feat)
3. **Task 3: Update SUPABASE_GUIDE.md documentation** - `09b67fe` (docs)

## Files Created/Modified
- `migrations/add_pgp_documents_tables.sql` - Complete migration with tables, indexes, RLS, comments
- `supabase_setup.sql` - Full schema updated with new tables for fresh deployments
- `docs/guides/SUPABASE_GUIDE.md` - Documentation of documents and document_fragments tables

## Decisions Made
- **pgpid as natural PRIMARY KEY:** Using PGP's document ID directly rather than creating a synthetic UUID. This matches the data source and simplifies import/lookups.
- **GENERATED pgp_url column:** Computed column ensures URL is always consistent with pgpid, avoids data duplication.
- **JSONB tags with GIN index:** Flexible array storage allows filtering by any tag combination without a separate join table.
- **Denormalized shelfmark in document_fragments:** Storing shelfmark redundantly avoids joins when displaying fragment lists.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required. Migration file is ready to run in Supabase SQL Editor but execution is deferred to user action.

## Next Phase Readiness
- Database schema is ready for data import (Phase 2)
- Tables must be created in Supabase before import can proceed
- User should run `migrations/add_pgp_documents_tables.sql` in Supabase SQL Editor

---
*Phase: 01-database-schema*
*Completed: 2026-02-05*

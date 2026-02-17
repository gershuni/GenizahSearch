---
phase: 35-pgp-sidecar-export
plan: 01
subsystem: database
tags: [sqlite, supabase, etl, sidecar, pgp, json, pagination]

# Dependency graph
requires: []
provides:
  - "pgp.db sidecar with documents, document_sources, document_footnotes, document_fragments tables"
  - "scripts/export_pgp_sidecar.py for reproducible sidecar generation"
affects: [36-pgp-service-rewrite, 37-browse-enrichment, 38-desktop-bundling]

# Tech tracking
tech-stack:
  added: []
  patterns: ["Supabase-to-SQLite export with .range() pagination", "JSONB-to-TEXT deterministic serialization with json.dumps(sort_keys=True)"]

key-files:
  created: ["scripts/export_pgp_sidecar.py", "pgp_data/pgp.db"]
  modified: [".gitignore"]

key-decisions:
  - "Hardcoded Supabase URL/anon key defaults matching codebase pattern (not .env-only)"
  - "Compact JSON format with sorted keys for deterministic, space-efficient storage"
  - "pgp_url stored as plain TEXT (from Supabase generated column) rather than recomputed"
  - "Unique index on fragments(document_id, sys_id) with fallback to non-unique if duplicates exist"

patterns-established:
  - "Supabase export pattern: fetch_all_rows with .range() pagination in 1000-row pages"
  - "Built-in validation: row count match + JSON round-trip checks before finalizing sidecar"
  - "Error cleanup: delete partial sidecar on any failure (no corrupt files left behind)"

requirements-completed: [MIGR-01, MIGR-04, MIGR-08]

# Metrics
duration: 6min
completed: 2026-02-17
---

# Phase 35 Plan 01: PGP Sidecar Export Summary

**Supabase PGP tables exported to 146.6 MB local SQLite sidecar (pgp.db) with pagination, JSON serialization, built-in validation, and idempotent rebuild**

## Performance

- **Duration:** 6 min
- **Started:** 2026-02-17T03:37:25Z
- **Completed:** 2026-02-17T03:43:54Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Created export script producing pgp.db with all 4 PGP tables (104,115 total rows)
- Built-in validation confirms exact row count match with Supabase and JSON round-trip integrity
- Idempotent: re-run deletes and recreates with identical results (same counts, same file size)
- Meta table tracks version (1.0.0), source URL, creation timestamp, and per-table row counts

## Task Commits

Each task was committed atomically:

1. **Task 1: Create PGP sidecar export script** - `1a715942` (feat)
2. **Task 2: Run export, validate output, update .gitignore** - `010b4aee` (chore)

## Files Created/Modified
- `scripts/export_pgp_sidecar.py` - Complete Supabase-to-SQLite export with pagination, JSON serialization, validation, error cleanup
- `pgp_data/pgp.db` - SQLite sidecar with 5 tables: documents (35,839), document_sources (9,364), document_footnotes (22,757), document_fragments (36,155), meta (8)
- `.gitignore` - Added `pgp_data/pgp.db` entry

## Decisions Made
- **Hardcoded credentials as defaults:** The .env file does not contain SUPABASE_URL or SUPABASE_ANON_KEY. Following the codebase pattern (lists_sync.py, supabase_corrections_client.py, web/supabase_client.py), hardcoded the public anon key and URL as defaults in os.environ.get(). These are public client-side keys, safe to embed.
- **Stored pgp_url as plain TEXT:** Supabase returns the generated column in SELECT * responses. Storing it saves recomputing and keeps the SQLite schema aligned with Supabase.
- **Validation uses .limit(0) with count='exact':** More efficient than head=True for getting exact counts from Supabase without transferring row data.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added hardcoded Supabase credential defaults**
- **Found during:** Task 2 (Run export)
- **Issue:** .env file lacks SUPABASE_URL and SUPABASE_ANON_KEY; script exited with error
- **Fix:** Added hardcoded defaults matching the pattern used throughout the codebase (lists_sync.py, supabase_corrections_client.py, etc.)
- **Files modified:** scripts/export_pgp_sidecar.py
- **Verification:** Export runs successfully with default credentials
- **Committed in:** 010b4aee (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking)
**Impact on plan:** Essential fix for script to run. No scope creep. Follows established codebase pattern.

## Issues Encountered
None beyond the credential defaults fix documented above.

## User Setup Required
None - no external service configuration required. The script uses the same public Supabase credentials embedded throughout the codebase.

## Next Phase Readiness
- pgp.db sidecar is ready for Phase 36 to rewrite the service layer to read from local SQLite instead of Supabase
- All 4 tables have appropriate indexes for Phase 36 query patterns (document_type, pgpid lookups, sys_id lookups, document_id lookups)
- JSON columns (tags, sections) verified to round-trip correctly via json.loads()

## Self-Check: PASSED

All artifacts verified:
- scripts/export_pgp_sidecar.py: FOUND
- pgp_data/pgp.db: FOUND
- .planning/phases/35-pgp-sidecar-export/35-01-SUMMARY.md: FOUND
- Commit 1a715942: FOUND
- Commit 010b4aee: FOUND

---
*Phase: 35-pgp-sidecar-export*
*Completed: 2026-02-17*

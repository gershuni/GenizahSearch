---
phase: 61-volume-session-community-validation
plan: 01
subsystem: database, api
tags: [supabase, corrections, comments, multi-ie, volume-aware]

requires:
  - phase: 60-desktop-volume-aware-browse
    provides: volume_ie state in web BrowseState and desktop current_browse_volume_ie
provides:
  - ie_id parameter on all correction/comment create functions (web + desktop)
  - ie_id filtering on all correction/comment read functions (OR logic with NULL)
  - Volume-aware community data pipeline for multi-IE manuscripts
affects: [corrections, comments, browse, community]

tech-stack:
  added: []
  patterns:
    - "ie_id OR NULL filter pattern for backward-compatible volume filtering"
    - "Conditional dict insertion (only add ie_id when truthy) to avoid NULL columns in Supabase"

key-files:
  created: []
  modified:
    - web/supabase_client.py
    - supabase_corrections_client.py
    - web/pages/browse.py
    - web/components/comment_dialog.py
    - genizah_app.py
    - docs/guides/SUPABASE_GUIDE.md

key-decisions:
  - "ie_id only included in Supabase insert when non-None (sparse column pattern)"
  - "Read filter uses OR logic: ie_id=active OR ie_id IS NULL (legacy always visible)"
  - "Used getattr(self, 'current_volume_ie', None) in ResultDialog for safe access"

patterns-established:
  - "Volume-aware community filter: or_(f'ie_id.eq.{ie_id},ie_id.is.null') for Supabase PostgREST"

requirements-completed: [CW-01, CW-02]

duration: 5min
completed: 2026-03-31
---

# Phase 61 Plan 01: Volume-Aware Community Data Summary

**ie_id column added to corrections/comments write and read paths so multi-IE manuscript contributions reference the specific volume**

## Performance

- **Duration:** 5 min
- **Started:** 2026-03-31T19:44:36Z
- **Completed:** 2026-03-31T19:49:46Z
- **Tasks:** 3
- **Files modified:** 6

## Accomplishments
- All correction and comment create functions (web + desktop) accept optional ie_id parameter
- All browse-context callers pass active volume_ie as ie_id to both write and read functions
- Read paths filter by ie_id with OR-NULL logic so legacy data remains visible
- Supabase migration SQL documented in SUPABASE_GUIDE.md (must be run manually before testing)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add Supabase ie_id columns and update write functions** - `ce00d876` (feat)
2. **Task 2: Thread ie_id through web and desktop correction/comment callers** - `8a820721` (feat)
3. **Task 3: Add ie_id filtering to correction/comment READ paths** - `3af16f8c` (feat)

## Files Created/Modified
- `web/supabase_client.py` - ie_id param on create_correction, create_comment, get_corrections, get_comments
- `supabase_corrections_client.py` - ie_id param on create_correction, get_corrections_for_document, get_document_comments
- `web/pages/browse.py` - Pass state.volume_ie to all correction/comment create and read calls
- `web/components/comment_dialog.py` - Thread ie_id through create_comment_button -> create_comment_dialog -> create_comment
- `genizah_app.py` - Pass volume_ie to all 4 create_correction calls and all 4 read calls (ResultDialog + browse tab)
- `docs/guides/SUPABASE_GUIDE.md` - Document ie_id columns, migration SQL in "Pending Migrations" section

## Decisions Made
- ie_id only added to Supabase payload when truthy (None/empty string excluded) -- avoids inserting NULL explicitly
- Read filter uses PostgREST `or_` with `ie_id.eq.{value},ie_id.is.null` -- legacy corrections without ie_id always shown
- ResultDialog uses `getattr(self, 'current_volume_ie', None)` for safe access since attribute was added in Phase 60

## Deviations from Plan

None - plan executed exactly as written.

## User Setup Required

**Supabase SQL migration required before testing.** Run in Supabase SQL Editor:

```sql
ALTER TABLE corrections ADD COLUMN IF NOT EXISTS ie_id TEXT;
ALTER TABLE comments ADD COLUMN IF NOT EXISTS ie_id TEXT;
COMMENT ON COLUMN corrections.ie_id IS 'IE identifier for multi-volume manuscripts. NULL = primary IE or pre-volume-awareness.';
COMMENT ON COLUMN comments.ie_id IS 'IE identifier for multi-volume manuscripts. NULL = primary IE or pre-volume-awareness.';
```

## Next Phase Readiness
- Write and read paths are volume-aware; ready for testing with real multi-IE manuscripts
- Phase 61-02 (session/corpus) can proceed independently

---
*Phase: 61-volume-session-community-validation*
*Completed: 2026-03-31*

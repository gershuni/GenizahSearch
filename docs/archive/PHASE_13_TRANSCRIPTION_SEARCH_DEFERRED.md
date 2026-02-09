# Phase 13: Transcription Search — Deferred

> **Status:** Reverted on 2026-02-09. Work preserved for future re-implementation.
> **Reason:** Index build too slow for local desktop use. Will revisit when architecture shifts to server-side index build + client download.

## What Was Built (25 commits, ~536 lines across 4 files)

### Plan 13-01: Index Foundation
- Extended Tantivy schema with `content_type` field (pgp/correction/htr)
- Fetched PGP transcriptions from Supabase during index build (batch paginated)
- Fetched approved user corrections from Supabase
- Preprocessing: stripped recto/verso headers, line numbers, brackets, nikud
- Temp-then-swap rebuild pattern (build to temp path, verify, rename)

### Plan 13-02: Search Engine Filtering
- `content_filter` parameter on `SearchEngine.execute_search()`
- Two-stage filtering: Tantivy-level `boolean_query` for content_type, post-filter for V0.8/V0.7 source
- Priority-based deduplication: PGP > Correction > V0.8 > V0.7
- `SearchThread` (gui_threads.py) passes content_filter through

### Plan 13-03: UI Integration
- 4 content-type checkboxes in both web and desktop (V0.8, V0.7, PGP, Users)
- V0.7 checkbox conditionally shown (only if V0.7 content exists in index)
- Old PGP-only post-filter removed, replaced by search-engine-level filtering
- Clear Filters resets all 4 checkboxes

## Files Changed

| File | Lines Changed | What |
|------|--------------|------|
| genizah_core.py | +418 -27 | Schema, PGP/correction fetch, preprocessing, content_filter, dedup |
| genizah_app.py | +87 -18 | Desktop checkboxes, content_filter wiring |
| web/pages/search.py | +87 -14 | Web checkboxes, content_filter wiring |
| gui_threads.py | +11 -3 | SearchThread content_filter passthrough |

## Post-Execution Bugfixes (9 commits)

1. `_get_field` returns default when Tantivy field is empty list
2. Status text updates during index build phases
3. Retry + fallback for Windows file locking on index swap
4. Index swap moved to main thread (close handles first)
5. Timing/debug logs for build phases
6. Split into 4 intermediate commits (page HTR -> PGP -> system docs -> parts)
7. `import time` moved before first usage
8. `del writer` before `index.writer()` to release Tantivy lock
9. Graceful fallback: reload_index tries tantivy_db_building if swap fails

## Known Issues at Time of Revert

1. **Index swap fails on Windows** — `os.rename` gets Access Denied (antivirus/Windows Search Indexer holds file handles). Workaround: load from build path.
2. **Index build very slow** — acceptable one-time, but not good UX for desktop users.
3. **V0.7 checkbox visibility bug** — `Query.term_query(schema, "source", "v0.7")` returns 0 hits because Tantivy's default tokenizer splits "V0.7" into tokens ["v0", "7"]. Fix: use `parse_query("source:V0.7")`.
4. **Lab index (fingerprints) doesn't include PGP** — `rebuild_lab_index()` only indexes V0.8/V0.7 files, not PGP transcriptions.

## Key Decisions (preserved for re-implementation)

| Decision | Rationale |
|----------|-----------|
| DEC-13-01-01 | PGP transcriptions use first fragment sys_id for full_header |
| DEC-13-01-02 | Graceful fallback on Supabase failure — index builds with HTR-only |
| DEC-13-01-03 | Preprocessing strips recto/verso headers, line numbers, brackets, nikud |
| DEC-13-02-01 | V0.8/V0.7 post-filtered (not Tantivy-level) because both share content_type=htr |
| DEC-13-02-02 | PGP/correction display metadata uses source field, with shelfmark fallback |
| DEC-13-03-01 | PGP result click already handled by existing data flow |
| DEC-13-03-02 | Old PGP-only post-filter removed, replaced by search-engine-level content_filter |

## Diffs Preserved

Full diffs saved in `.planning/phases/13-transcription-search/` directory (plans, verification, context files).

## Future Re-implementation Notes

- When moving to server-side index build + download, this feature should be re-implemented
- The **lab index (fingerprints)** also needs to be included in the server-side build
- Consider using `content_type` as a `keyword` field (not `text`) to avoid tokenizer issues
- The V0.7 visibility check should use `parse_query` not `term_query`
- Windows file locking is a non-issue with server-side builds

---
phase: 39-bug-fixing-cleanup-performance-improving
plan: 08
subsystem: ui
tags: [asyncio, performance, nicegui, fjms, supabase, parallel-queries]

# Dependency graph
requires:
  - phase: 39-07
    provides: Static CSS + lazy dialog for navigation performance baseline
provides:
  - Parallelized search post-processing (3 enrichment queries via asyncio.gather)
  - Batched FJMS metadata pre-fetch in browse load_page
  - Async stats + feed loading on discoveries page
affects: [search, browse, discoveries, page-navigation-speed]

# Tech tracking
tech-stack:
  added: []
  patterns: [asyncio.gather for parallel run.io_bound calls, pre-fetch-then-render for FJMS metadata, pure-UI render helpers separated from I/O]

key-files:
  created: []
  modified:
    - web/pages/search.py
    - web/pages/browse.py
    - web/pages/discoveries.py

key-decisions:
  - "Consolidate result_sys_ids/all_sys_ids into single variable (identical list comprehensions)"
  - "Keep hierarchy fetch sequential after asyncio.gather (depends on domain results)"
  - "Keep fjms service reference in update_content for on-demand catalog dialog queries"
  - "Extract _render_stat_cards/_render_feed_result as pure-UI helpers (no I/O in rendering)"
  - "Keep sync load_stats/load_feed for filter refresh callbacks (only initial load is async)"

patterns-established:
  - "asyncio.gather + run.io_bound: pattern for parallel off-thread queries in NiceGUI async handlers"
  - "Pre-fetch in load_page, read from state in update_content: separates I/O from UI rendering"
  - "Pure-UI render helpers: _render_stat_cards(stats) takes data, no DB calls"

requirements-completed: []

# Metrics
duration: 14min
completed: 2026-02-20
---

# Phase 39 Plan 08: Page Navigation Speed Summary

**Parallelized search enrichment via asyncio.gather, batched FJMS metadata pre-fetch in browse, and async stats+feed loading on discoveries page**

## Performance

- **Duration:** 14 min
- **Started:** 2026-02-20T08:35:44Z
- **Completed:** 2026-02-20T08:49:32Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Search post-processing runs 3 independent enrichment queries (domains, transcriptions, catalog counts) in parallel via asyncio.gather instead of 4 sequential awaits
- Browse update_content reads from pre-fetched state.fjms_data instead of making 5 serial FJMS service calls during UI rendering
- Discoveries page loads stats and feed asynchronously off the UI thread using asyncio.gather + run.io_bound

## Task Commits

Each task was committed atomically:

1. **Task 1: Parallelize search post-processing enrichment queries** - `f549e75e` (perf)
2. **Task 2: Batch FJMS metadata in browse load_page and async-ify discoveries page** - `9964a27a` (perf)

**Plan metadata:** [pending] (docs: complete plan)

## Files Created/Modified
- `web/pages/search.py` - Added import asyncio; replaced 4 sequential run.io_bound calls with asyncio.gather for 3 parallel queries + 1 conditional sequential fetch
- `web/pages/browse.py` - Added fjms_data to BrowseState; pre-fetch all FJMS data in load_page(); replaced 5 service calls in update_content with state.fjms_data reads
- `web/pages/discoveries.py` - Added import asyncio; async initial_load() with asyncio.gather for parallel stats+feed; extracted _render_stat_cards and _render_feed_result helpers

## Decisions Made
- Consolidated `result_sys_ids` and `all_sys_ids` into a single `all_sys_ids` variable (they were identical list comprehensions)
- Kept hierarchy fetch sequential after the gather (it depends on `search_state.has_domain_data` which is computed from the domain results)
- Kept `fjms` service reference available in `update_content` for the catalog dialog click handler (on-demand, not page-load)
- Extracted `_render_stat_cards()` and `_render_feed_result()` as pure-UI functions that accept pre-fetched data
- Kept synchronous `load_stats()` and `load_feed()` intact for the filter-change refresh callbacks (only the initial page load is async)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Fixed indentation after removing FJMS availability guards**
- **Found during:** Task 2 (browse.py FJMS metadata refactoring)
- **Issue:** After replacing `if fjms.is_available():` guards with `if fjms_data:` reads, the inner block bodies retained the old extra indentation level
- **Fix:** De-indented the bodies of domains, catalog refs, and source names sections by one level
- **Files modified:** web/pages/browse.py
- **Verification:** `ast.parse()` confirms syntax is valid
- **Committed in:** 9964a27a (Task 2 commit)

---

**Total deviations:** 1 auto-fixed (1 bug)
**Impact on plan:** Cosmetic indentation fix, no scope creep.

## Issues Encountered
- Pre-existing test failures (desktop folio navigation test and responsa explosion guard test) are unrelated to these changes. 398 tests pass.

## User Setup Required
None - no external service configuration required.

## Next Phase Readiness
- All three key pages (search, browse, discoveries) now use parallel/async loading patterns
- Expected load time improvements: Search ~2-3s (3 queries now overlap), Browse ~2-4s (FJMS calls off render path), Discoveries ~2-3s (stats+feed concurrent)
- Manual UAT recommended to measure actual improvement

---
## Self-Check: PASSED

- [x] web/pages/search.py exists
- [x] web/pages/browse.py exists
- [x] web/pages/discoveries.py exists
- [x] 39-08-SUMMARY.md exists
- [x] Commit f549e75e exists (Task 1)
- [x] Commit 9964a27a exists (Task 2)

---
*Phase: 39-bug-fixing-cleanup-performance-improving*
*Completed: 2026-02-20*

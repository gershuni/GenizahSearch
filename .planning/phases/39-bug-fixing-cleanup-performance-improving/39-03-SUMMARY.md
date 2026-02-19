---
phase: 39-bug-fixing-cleanup-performance-improving
plan: 03
subsystem: analytics
tags: [posthog, analytics, session-recording, real-user-monitoring]

# Dependency graph
requires: []
provides:
  - PostHog JS snippet integration in web app (conditional on env var)
  - Real-user monitoring with autocapture, session recordings, page views
affects: [web-performance, user-analytics]

# Tech tracking
tech-stack:
  added: [posthog-js-cdn]
  patterns: [conditional-analytics-snippet, env-var-gated-feature]

key-files:
  created: []
  modified:
    - web/main.py
    - CLAUDE.md

key-decisions:
  - "maskAllInputs + maskTextSelector for privacy -- researchers' search inputs not recorded in session replays"
  - "person_profiles: identified_only -- no person profiles for anonymous visitors"
  - "Empty string when POSTHOG_API_KEY not set -- graceful degradation, zero cost when disabled"

patterns-established:
  - "Env-var-gated analytics: define script constant conditionally, add to all page handlers alongside existing analytics"

requirements-completed: []

# Metrics
duration: 4min
completed: 2026-02-19
---

# Phase 39 Plan 03: PostHog Analytics Integration Summary

**PostHog JS snippet with autocapture, session recordings (masked inputs), and page view tracking on all 14 web pages, conditional on POSTHOG_API_KEY env var**

## Performance

- **Duration:** 4 min
- **Started:** 2026-02-19T19:48:43Z
- **Completed:** 2026-02-19T19:52:11Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments
- PostHog JS snippet added to web/main.py with full autocapture, session recordings, and page view tracking
- Privacy-first configuration: maskAllInputs and maskTextSelector protect researcher search inputs in session replays
- Graceful degradation: entire snippet is empty string when POSTHOG_API_KEY env var is not set
- All 14 page handlers receive PostHog alongside existing Google Analytics (no conflict)

## Task Commits

Each task was committed atomically:

1. **Task 1: Add PostHog JS snippet to web/main.py alongside Google Analytics** - `181617e3` (feat)

**Plan metadata:** pending (docs: complete plan)

## Files Created/Modified
- `web/main.py` - Added POSTHOG_SCRIPT constant and ui.add_head_html(POSTHOG_SCRIPT) on all 14 page handlers
- `CLAUDE.md` - Added POSTHOG_API_KEY to environment variables section

## Decisions Made
- Used maskAllInputs: true and maskTextSelector for privacy -- researchers' manuscript search inputs are not recorded in session replays
- Set person_profiles to 'identified_only' -- only creates person profiles for identified users, not anonymous visitors
- Empty string when env var not set -- zero performance cost and no errors when PostHog is disabled
- Used PostHog US data center (us.i.posthog.com) for API host

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- 3 pre-existing test failures found (desktop KTIV button style, responsa explosion guard Hebrew warnings) -- all unrelated to PostHog changes, documented as out-of-scope

## User Setup Required

PostHog requires manual account creation and API key configuration:
1. Create a PostHog account at https://posthog.com
2. Create a project in the PostHog dashboard
3. Copy the Project API Key from Settings
4. Set the `POSTHOG_API_KEY` environment variable on the server

Without the env var, the app runs normally with no PostHog analytics (graceful degradation).

## Next Phase Readiness
- PostHog integration complete and ready for deployment
- User needs to create PostHog account and set POSTHOG_API_KEY env var to activate
- Session recordings will provide real-user performance data to guide future optimization

## Self-Check: PASSED

- FOUND: web/main.py
- FOUND: CLAUDE.md
- FOUND: 181617e3 (task 1 commit)

---
*Phase: 39-bug-fixing-cleanup-performance-improving*
*Completed: 2026-02-19*

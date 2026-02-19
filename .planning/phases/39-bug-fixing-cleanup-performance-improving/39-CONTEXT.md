# Phase 39: Bug Fixing, Cleanup, Performance Improving - Context

**Gathered:** 2026-02-19
**Status:** Ready for planning

<domain>
## Phase Boundary

Stabilize and polish the app after 7 milestones of rapid feature development. Fix all known crashes from crash_log.txt, address accumulated technical debt and pending todos, improve web app performance (especially search results and browse loading), integrate PostHog analytics, add Playwright E2E tests, and implement server-side pagination for search results beyond the current 200-item WebSocket cap.

</domain>

<decisions>
## Implementation Decisions

### Bug Triage
- Fix ALL crash types — clean slate approach
- QScrollBar deleted (2,347x) and QGraphicsSimpleTextItem deleted (341x) are the big two — both Qt object lifecycle issues in desktop app
- Also fix rare crashes: KeyError 'uid' (2x), AttributeError list.replace (2x), TypeError sequence item (1x)
- After fixing: archive current crash_log.txt to crash_log_archive.txt, then clear crash_log.txt for a clean baseline

### Cleanup Targets
- Pending todos from STATE.md: Claude assesses which are worth doing based on impact vs effort (JA diacritic dots normalization, desktop corrections migration to shared service, domain click behavior in browse metadata, pre-search domain filtering optimization)
- genizah_app.py (18.5K lines): Claude assesses whether any module extraction would be safe and high-value
- web/pages/search.py (3,200 lines): Claude assesses based on actual code structure
- auth_state.py hardcoded timeouts: Claude assesses if worth the effort

### Performance Focus
- User reports general web slowness: search results rendering, browse page loading, page navigation
- Integrate PostHog for real-user analytics and performance monitoring
- PostHog scope: core analytics (page views, feature usage, performance timings) + session recordings — start lightweight, expand later
- Raise the 200-result WebSocket cap with server-side pagination so users can browse beyond 200 results without loading all at once
- Profile and fix web performance hotspots identified through local investigation

### Test Coverage
- Add Playwright E2E tests for critical user-facing flows (happy paths)
- E2E scope: Search→View→Edit→Submit→Approve and other key user journeys
- Add performance/stress tests: 1000+ results, 100+ list items
- Coverage goal: ensure all critical paths have at least one happy-path test (no arbitrary number target)
- CI setup: Claude decides based on project setup complexity

### Claude's Discretion
- Specific fix approach for QScrollBar and QGraphicsSimpleTextItem crashes (guard checks, signal disconnection, or both)
- Which pending todos are worth addressing (impact vs effort assessment)
- Whether to extract modules from large files (only if safe and clearly high-value)
- auth_state.py timeout configurability (fix or keep deferring)
- PostHog feature depth beyond core analytics + recordings
- CI integration for Playwright tests (now vs later)
- Web performance optimization techniques (lazy loading, component splitting, caching strategies)

</decisions>

<specifics>
## Specific Ideas

- "Most things in web feel slow" — broad web performance is a priority, not just individual features
- An expert recommended PostHog — user wants real-user analytics to guide future optimization
- Crash log is 900KB and obscures new issues — archive-then-clear pattern
- Server-side pagination was explicitly requested to go beyond the 200-result cap

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 39-bug-fixing-cleanup-performance-improving*
*Context gathered: 2026-02-19*

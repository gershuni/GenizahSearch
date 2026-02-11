# Phase 22: Pending Corrections Data Layer - Context

**Gathered:** 2026-02-11
**Status:** Ready for planning

<domain>
## Phase Boundary

Shared service function to fetch a user's own pending (unapproved) corrections for a given manuscript page. Consumed by web (Phase 23) and desktop (Phase 24) display layers. No UI in this phase — purely data retrieval.

</domain>

<decisions>
## Implementation Decisions

### Claude's Discretion

All implementation decisions delegated to Claude. The success criteria are specific enough to guide choices:

- **Correction data shape** — Return whatever fields the display layers need (text, status, submission date, any relevant metadata). Research phase should examine the existing corrections table schema to determine available fields.
- **Status distinction** — The success criteria specify returning `draft`, `pending`, and `under_review` statuses. Whether to expose these individually or group them is up to Claude — choose what makes the display layers simplest.
- **Page-level vs batch scope** — The success criteria say "for a given sys_id + page_number", so single-page is the baseline. Batch support can be added if the display layers would benefit, at Claude's discretion.
- **Service location** — Place in the shared service layer following existing patterns (e.g., `shared/document_service.py` or similar).
- **Auth filtering** — Must filter to authenticated user only; return empty when no user or different user. Implementation approach is Claude's choice.

</decisions>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. Follow existing service layer patterns established in earlier phases.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 22-pending-corrections-data-layer*
*Context gathered: 2026-02-11*

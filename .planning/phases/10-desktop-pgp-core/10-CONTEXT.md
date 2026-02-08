# Phase 10: Desktop PGP Core - Context

**Gathered:** 2026-02-08
**Status:** Ready for planning

<domain>
## Phase Boundary

Desktop users can view PGP transcriptions in the manuscript viewer and switch between scholars' editions and translations via a grouped version selector. The desktop already has transcription display, a version selector, and source switching for user transcriptions and HTR — this phase adds PGP editions and translations as new sources using the existing UI patterns.

New capabilities NOT in scope: metadata panel (Phase 12), search indicators (Phase 12), tag search (Phase 12), reading desk (Phase 11), offline caching (POLISH-04).

</domain>

<decisions>
## Implementation Decisions

### Version selector grouping
- PGP editions appear in their own group with a "PGP Editions" separator/header, placed above HTR entries
- Visual separator line between groups (PGP Editions, HTR, User Corrections)
- Each PGP edition shows scholar name as the label

### Translation placement
- Claude's discretion on whether translations get their own separate group below editions or are combined under one PGP group
- Should follow whatever pattern the web app already uses for consistency
- Show translator name + language in the selector entry

### Auto-selection priority
- When a manuscript has a PGP edition available, it auto-selects as the default (replacing HTR V0.8)
- HTR V0.8 remains available in the selector but is not pre-selected when PGP exists
- If no PGP edition exists, existing behavior unchanged (HTR default)

### Data fetching
- Desktop calls the exact same `shared/document_service.py` functions the web uses
- All Supabase calls wrapped in QThread workers (matching existing desktop patterns with 14+ QThread worker examples)
- No desktop-specific data layer — shared service is the single source

### Claude's Discretion
- Exact QThread worker class design (can follow existing patterns in genizah_app.py)
- How to handle loading states while fetching PGP data
- Error handling for network failures during PGP fetch
- Whether to pre-fetch PGP data or fetch on-demand when user opens a manuscript

</decisions>

<specifics>
## Specific Ideas

- The desktop already has all the UI patterns needed (transcription display, version selector, source switching) — this is about wiring PGP data into existing infrastructure, not building new UI
- Follow web app patterns for how editions and translations are presented for cross-app consistency

</specifics>

<deferred>
## Deferred Ideas

- Offline PGP data cache (SQLite local store) — already tracked as POLISH-04 in future requirements for v5.7.0+
- User raised this during discussion; confirmed it belongs in a future phase

</deferred>

---

*Phase: 10-desktop-pgp-core*
*Context gathered: 2026-02-08*

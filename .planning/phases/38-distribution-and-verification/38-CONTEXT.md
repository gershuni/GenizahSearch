# Phase 38: Distribution and Verification - Context

**Gathered:** 2026-02-18
**Status:** Ready for planning

<domain>
## Phase Boundary

Bundle all three sidecar databases (pgp.db, fjms_enrichment.db, nli_crossref.db) for both desktop installer and web deployment. Verify that desktop PGP browsing works fully offline using local SQLite data. Add an in-app sidecar update mechanism for desktop users.

</domain>

<decisions>
## Implementation Decisions

### Sidecar bundling scope
- Bundle ALL three sidecars: pgp.db, fjms_enrichment.db, nli_crossref.db
- Same set for both desktop installer and web server deployment — consistent architecture
- No size concern — 200MB+ is acceptable for a research tool with local data
- File location on disk: Claude's discretion (choose most practical based on installer mechanics)

### Offline experience
- Silent operation — no "Offline Mode" indicator; local features just work
- Online feature failure handling: Claude's discretion (match existing app patterns)
- Verification method: automated code-path verification that PGP browse paths use local SQLite with no Supabase calls
- Verification scope: all three sidecars, not just PGP — verify FJMS and NLI features also work from local data

### Sidecar update path
- In-app update check: auto-check on startup (non-blocking, silent unless update available)
- Update source: Claude's discretion (GitHub releases or web server manifest — pick most practical)
- Download behavior: ask user first — notification like "New data available (X MB). Download now?"
- User controls bandwidth; no silent large downloads

### Missing sidecar handling
- Graceful degradation: features depending on a missing sidecar simply don't appear
- No error dialogs for missing sidecars — they're optional enhancements
- Sidecar health discovery: Claude's discretion (e.g., About screen or just file system)
- Web app fallback behavior: Claude's discretion (Supabase fallback or require sidecar)
- Integrity check: version check only (read meta table version on startup) — fast, catches stale files

### Claude's Discretion
- Sidecar file location on user's machine (app directory vs AppData)
- Online feature failure UX when offline (error on attempt vs disable controls)
- Update check hosting source (GitHub releases vs web endpoint)
- Web app Supabase fallback when sidecar missing
- Sidecar health display (About screen or implicit)

</decisions>

<specifics>
## Specific Ideas

- Update notification should be non-intrusive — user asked for "ask first" rather than auto-download
- The app should feel seamless offline; researchers shouldn't notice they're working from local data
- Version check on startup serves double duty: integrity validation + update check trigger

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 38-distribution-and-verification*
*Context gathered: 2026-02-18*

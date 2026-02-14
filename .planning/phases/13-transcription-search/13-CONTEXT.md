# Phase 13: Transcription Search - Context

**Gathered:** 2026-02-09
**Status:** Ready for planning

<domain>
## Phase Boundary

Users can search within PGP transcriptions and user-corrected transcriptions via Tantivy, with checkbox filters to scope results by content type. Extends the existing main search index only (not the lab/fingerprint index). Both web and desktop apps get filter controls.

</domain>

<decisions>
## Implementation Decisions

### Search result display
- Single row per manuscript when multiple sources match (deduplicated)
- Source priority order: PGP > Correction > V0.8 > V0.7 (PGP scholarly editions win)
- Snippet/preview text comes from the winning (highest priority) source
- Source type populates the existing Source column in desktop; web uses its existing source display
- PGP-only manuscripts (no HTR) always appear in default "All Content" search results
- Clicking a PGP result opens the manuscript viewer with PGP transcription selected (same flow as HTR results)

### Filter UI design
- **Checkboxes, not a dropdown** — independent toggles for each content type
- Hidden by default (same pattern as existing filter toggles)
- All checked by default
- Checkbox labels:
  - "MiDRASH auto-transcript (V0.8)"
  - "MiDRASH auto-transcript (V0.7)" — only shown if V0.7 content exists in index
  - "PGP"
  - "Users transcriptions"
- Users can combine freely (e.g., PGP + Users but not MiDRASH)
- Filter state persists within session (survives across searches)

### Index rebuild experience
- PGP transcription fetching is automatic if Supabase is reachable — no opt-in needed
- Graceful fallback: if Supabase unreachable, continue with HTR-only indexing + log warning
- If PGP fetch fails mid-rebuild (network drops), continue with whatever PGP data was fetched + all HTR. Show notice that PGP content may be partial.
- Approved user corrections are always fetched (public data, no login required)
- Progress feedback: progress bar + status text during PGP fetch step (e.g., "Fetching PGP transcriptions... 500/9000")

### PGP text preprocessing
- Strip Recto/Verso section headers before indexing
- Strip line numbers (leading digits) before indexing
- Strip bracket characters but keep contents inside — `[word]` becomes `word`, `(note)` becomes `note`
- Scholarly editorial marks removed, actual transcription text preserved

### Claude's Discretion
- Multi-fragment PGP documents: how to represent in search results (one row per PGP doc or per fragment) — follow existing multi-fragment display patterns
- Nikud stripping: follow existing HTR indexing normalization approach
- Filter checkbox placement: inside existing filters panel vs near search box — follow existing layout patterns
- Web index rebuild: whether to add admin-only rebuild capability or keep desktop-only
- Post-rebuild summary: whether to show detailed counts or simple success message
- Dynamic filter visibility: whether to hide checkboxes for content types not in the index

</decisions>

<specifics>
## Specific Ideas

- The desktop already has a "Source" row/column — PGP results populate this naturally
- "MiDRASH auto-transcript" is the user-facing name for HTR content (not "HTR")
- Checkbox filter pattern matches the existing hidden-by-default filter toggle design used in Phase 12
- V0.7 checkbox is conditionally shown — only if V0.7 content exists

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 13-transcription-search*
*Context gathered: 2026-02-09*

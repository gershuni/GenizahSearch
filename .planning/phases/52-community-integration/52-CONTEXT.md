# Phase 52: Community + Integration - Context

**Gathered:** 2026-03-17
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can publish their puzzle join documents for community review, and browse published joins from other researchers. Both web (Discoveries Center feed + joins panel) and desktop (Discoveries tab + joins dialog). Entry points from browse/search/lists (CANV-02) and personal workspace (COMM-01) are already implemented — this phase adds COMM-02 (publish) and COMM-03 (browse published).

Requirements: COMM-02 (publish for review), COMM-03 (published joins browsable). Desktop publishing parity included.

</domain>

<decisions>
## Implementation Decisions

### Publishing Flow
- **One-click publish** — user clicks Publish, join immediately visible to all. No moderation queue.
- Requires **logged-in Supabase account** (consistent with corrections/lists auth model)
- User can **edit and unpublish anytime** — full control. Re-publish updates the existing published record. Unpublish makes it private again.
- Attribution shown as **display name from Supabase profile** (e.g. "Published by Dr. Sarah Cohen")

### What Gets Published (Supabase)
- **Full arrangement JSON** — fragment positions, rotations, scales, flips, shelfmarks, folio labels. Enables forking: other users can open it in their own puzzle canvas.
- **Full-resolution composite PNG** — uploaded to server storage at publish time (~170GB available). Viewers can download directly without server-side re-compositing.
- **Thumbnail** — smaller composite for browsing display
- **Metadata** — title, notes, fragment shelfmarks list, author (Supabase user), publish date

### Published Joins in Discoveries Center (Web + Desktop)
- Published puzzle joins appear **mixed into the activity feed** alongside existing discoveries and questions
- Feed item shows: composite thumbnail, title, author, fragment shelfmarks, date
- Click to view details: full-res image download, notes, fragment list, "Open in Puzzle" (fork) button

### Published Joins in Joins Panel (Browse/ResultDialog)
- **Separate section below FJMS scientific joins** — "Community Puzzle Joins" section
- Shows published joins that contain the current manuscript's fragments
- Composite thumbnail + author + title per entry
- Clear distinction from authoritative FJMS joins

### Desktop Parity
- Desktop Publish button in PuzzleCanvasWindow toolbar
- Desktop Discoveries tab shows published puzzle joins in feed
- Desktop joins dialog (browse/ResultDialog community_row) shows "Community Puzzle Joins" section

### Claude's Discretion
- Supabase table schema design for published joins
- Supabase storage bucket configuration for composite images
- Fork behavior details (copy to local joins.db, rename convention)
- Feed item card design and layout
- Thumbnail size and compression for upload
- Desktop Discoveries tab rendering approach

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Puzzle Implementation (Phases 47-50)
- `shared/puzzle_model.py` — PuzzleFragment/PuzzleDocument dataclasses with JSON roundtrip
- `shared/puzzle_service.py` — joins.db CRUD: save_document, load_document, list_documents, delete_document, list_documents_for_fragment
- `shared/puzzle_export.py` — compose_puzzle_export() for full-res composite PNG generation
- `shared/puzzle_image_service.py` — IIIF fetch, bg removal, disk cache

### Community Infrastructure (Existing)
- `web/pages/discoveries.py` — Discoveries Center page with activity feed (discoveries, questions, corrections)
- `web/supabase_client.py` — Supabase client with get_feed_items, create_discovery, vote, pin, etc.
- `web/auth_state.py` — GlobalAuthState for login checks
- `web/components/joins_panel.py` — Fragment joins panel showing FJMS scientific joins

### Desktop
- `genizah_app.py` lines 3209+ — PuzzleCanvasWindow (toolbar, save/load/export, side panel)
- `genizah_app.py` lines 5209+ — community_row in ResultDialog (joins button, corrections, comments)

### Requirements
- `.planning/REQUIREMENTS.md` — COMM-02, COMM-03 requirements

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `web/supabase_client.py` — Full Supabase CRUD pattern (create_discovery, get_feed_items, vote, delete). Publishing follows identical pattern.
- `web/pages/discoveries.py` — Activity feed renderer with card-based items, translate buttons, responses. Published joins become a new feed item type.
- `web/components/joins_panel.py` — fetch_connected_fragments() queries Supabase fragment_joins + PGP document_fragments. Add a section for community puzzle joins.
- `shared/puzzle_export.py` — compose_puzzle_export() generates full-res PNG from arrangement. Reuse at publish time.
- `PuzzleService.load_document()` / `.to_json()` — Serialize arrangement for upload to Supabase.

### Established Patterns
- Supabase RLS: user-owned rows with auth.uid() checks (corrections, lists, discoveries)
- Feed items: unified query across multiple tables (discoveries + corrections + comments), type badge per item
- Desktop Supabase client: `supabase_corrections_client.py` pattern for desktop-side Supabase operations

### Integration Points
- Web puzzle.py toolbar: Add "Publish" button (next to Export)
- Desktop PuzzleCanvasWindow toolbar: Add "Publish" button
- Web discoveries.py feed: New item type 'puzzle_join' with thumbnail rendering
- Desktop Discoveries tab: Render published puzzle joins in feed
- Web joins_panel.py: New "Community Puzzle Joins" section below FJMS joins
- Desktop joins dialog: New section for community puzzle joins

</code_context>

<specifics>
## Specific Ideas

- Publishing is "share with community" — local save remains the primary workflow, publish is optional
- Published joins in the joins panel connect the scholarly workflow: user sees FJMS joins (authoritative), then community puzzle joins (user-contributed) for the same manuscript
- Fork/open in puzzle lets researchers build on each other's work
- Full-res image stored on server (~170GB available) so viewers can download without server-side re-compositing

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 52-community-integration*
*Context gathered: 2026-03-17*

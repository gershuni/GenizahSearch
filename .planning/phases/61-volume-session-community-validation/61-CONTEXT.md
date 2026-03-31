# Phase 61: Volume Session, Community Context & Corpus Validation - Context

**Gathered:** 2026-03-31
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 61 completes the v7.7 milestone with three polish tasks:
1. Session persistence for volume state (web + desktop)
2. Community writes (corrections, comments) include IE context
3. Automated validation of the 907-to-suffix IIIF mapping

All are infrastructure/plumbing — no new UI surfaces or user-facing design decisions.

</domain>

<decisions>
## Implementation Decisions

### Session Persistence (URL-02)
- **D-01:** Web browse session restore should save/restore `volume_ie` alongside existing `sys_id`, `page`, `fl_id` state in NiceGUI user storage
- **D-02:** Desktop session restore should save/restore `current_browse_volume_ie` in the existing session persistence mechanism (QSettings or JSON)
- **D-03:** If a restored volume_ie refers to an IE that no longer exists in ie_volume_map, silently fall back to primary IE (no error)

### Community Writes (CW-01, CW-02)
- **D-04:** Add `ie_id` column to corrections table in Supabase (nullable TEXT, NULL = primary/unknown IE)
- **D-05:** Pass active `volume_ie` from browse/search state through correction and comment creation functions
- **D-06:** Desktop corrections client gets same `ie_id` parameter
- **D-07:** Existing corrections/comments without `ie_id` remain valid — nullable column, no migration needed for old data

### Corpus Validation (VAL-01)
- **D-08:** Validation script is a standalone `scripts/validate_ie_volume_map.py` that samples N manuscripts and checks live IIIF manifests
- **D-09:** Sample size: ~100 multi-IE manuscripts (stratified by library code) — not all 3,193 (would overwhelm NLI API)
- **D-10:** Validation output is a report file, not a pass/fail gate — informational for manual review

### Claude's Discretion
- Schema details for ie_id column (exact name, constraints)
- Validation script output format (JSON, CSV, or markdown report)
- Sampling strategy details (random vs stratified, seed)
- Whether to add ie_id display to corrections admin view

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Volume Infrastructure
- `.planning/REQUIREMENTS.md` — URL-02, CW-01, CW-02, VAL-01 definitions
- `.planning/phases/60-desktop-volume-aware-browse/60-01-PLAN.md` — Desktop volume implementation
- `ie_volume_map.json` — The mapping being validated (3,193 entries)

### Session Persistence Patterns
- `web/pages/browse.py` — Web browse state (BrowseState class, volume_ie field already exists)
- `genizah_app.py` — Desktop session save/restore (search for `session` and `QSettings`)

### Corrections/Comments
- `web/supabase_client.py:803` — `create_correction()` function signature
- `shared/corrections_service.py` — Shared corrections service
- `supabase_corrections_client.py:763` — Desktop `create_correction()`
- `docs/guides/SUPABASE_GUIDE.md` — Supabase schema documentation

### Validation
- `scripts/build_ie_volume_map.py` — Script that built the mapping (understands data structure)
- `genizah_core.py` — `fetch_iiif_manifest()` with suffix parameter (Task 1 of Phase 60)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `BrowseState.volume_ie` already exists in web browse — just needs session persistence wiring
- `self.current_browse_volume_ie` already exists in desktop browse — needs QSettings save/restore
- `create_correction()` in both web and desktop already has clear parameter signatures — just add `ie_id`
- `fetch_iiif_manifest(sys_id, suffix=N)` already works — validation script can reuse it

### Established Patterns
- Web session: NiceGUI `app.storage.user` for per-user persistent state
- Desktop session: QSettings-based save/restore in `save_session()` / `restore_session()`
- Supabase schema: nullable columns for optional metadata fields (standard pattern)

### Integration Points
- Web browse `create_browse_page()` — session restore entry point
- `browse_render_page()` in desktop — where volume state is applied after restore
- All correction submission paths (web browse, web search, desktop browse, desktop ResultDialog)

</code_context>

<specifics>
## Specific Ideas

No specific requirements — all three tasks are straightforward infrastructure wiring with clear existing patterns.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 61-volume-session-community-validation*
*Context gathered: 2026-03-31*

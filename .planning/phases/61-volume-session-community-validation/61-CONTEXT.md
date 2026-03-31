# Phase 61: Volume Session, Community Context & Corpus Validation - Context

**Gathered:** 2026-03-31
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 61 completes the v7.7 milestone. It has two tiers:

**Correctness (must-do):** Community writes (corrections, comments) must include `ie_id` because different volumes contain different text — a correction without IE context can be semantically wrong. Corpus validation must confirm the 907→suffix mapping is reliable.

**Polish (nice-to-have):** Session restore for active volume across refresh/restart.

</domain>

<decisions>
## Implementation Decisions

### Community Writes — Correctness Tier (CW-01, CW-02)
- **D-01:** Store `ie_id` (stable IE identifier), NOT volume index number. Volume index is display-only and derived from the map.
- **D-02:** Add nullable `ie_id` TEXT column to corrections and comments tables in Supabase. NULL = primary IE or pre-volume-awareness data.
- **D-03:** Pass active `volume_ie` from browse/search state through all correction and comment creation functions (web + desktop).
- **D-04:** Existing corrections/comments without `ie_id` remain valid — no migration needed for old data.
- **D-05:** If a user submits a correction on a multi-IE manuscript without `ie_id` (edge case), accept it but log a warning — never block the user.

### Corpus Validation — Correctness Tier (VAL-01)
- **D-06:** Validation script is standalone `scripts/validate_ie_volume_map.py`.
- **D-07:** Stratified sampling, NOT purely random. Must include:
  - All 16 heuristic-edge-case manuscripts (if any)
  - All known problematic cases
  - Separate strata for: 2-volume, 3-volume, 4+ volume manuscripts
  - Cases with large page-count gaps between volumes
- **D-08:** Sample size: ~200-300 manuscripts (larger than originally proposed 100).
- **D-09:** Output is a structured report (pass/fail per manuscript + summary statistics).

### Session Restore — Polish Tier (URL-02)
- **D-10:** Web browse session saves/restores `volume_ie` alongside existing state in NiceGUI user storage.
- **D-11:** Desktop session saves/restores `current_browse_volume_ie` in existing QSettings mechanism.
- **D-12:** If restored `volume_ie` is no longer valid (IE removed from map), silently fall back to primary IE.
- **D-13:** This is lowest priority — implement only if time permits after correctness items.

### Priority Order
- **D-14:** Correctness first: CW-01/CW-02 (ie_id in writes) → VAL-01 (validation) → URL-02 (session polish).

### Claude's Discretion
- Exact Supabase column name and constraints for ie_id
- Validation report format (JSON, CSV, or markdown)
- Whether to surface ie_id in corrections admin view

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Volume Infrastructure
- `.planning/REQUIREMENTS.md` — URL-02, CW-01, CW-02, VAL-01 definitions
- `.planning/phases/60-desktop-volume-aware-browse/60-01-PLAN.md` — Desktop volume implementation (resolve_volume_suffix, VolumeManifestThread)
- `ie_volume_map.json` — The mapping being validated (3,193 entries)

### Corrections & Comments
- `web/supabase_client.py:803` — `create_correction()` function signature
- `shared/corrections_service.py` — Shared corrections service
- `supabase_corrections_client.py:763` — Desktop `create_correction()`
- `docs/guides/SUPABASE_GUIDE.md` — Supabase schema documentation

### Session Persistence
- `web/pages/browse.py` — BrowseState class with `volume_ie` field
- `genizah_app.py` — Desktop session save/restore (QSettings)

### Validation
- `scripts/build_ie_volume_map.py` — Script that built the mapping
- `genizah_core.py` — `fetch_iiif_manifest(sys_id, suffix=N)` for live manifest checks

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `BrowseState.volume_ie` already exists in web browse — needs session persistence wiring
- `self.current_browse_volume_ie` exists in desktop — needs QSettings save/restore
- `create_correction()` in both web and desktop has clear signatures — add `ie_id` param
- `fetch_iiif_manifest(sys_id, suffix=N)` works — validation script can reuse it
- `resolve_volume_suffix(sys_id, ie_id)` centralized helper from Phase 60

### Established Patterns
- Web session: NiceGUI `app.storage.user` for per-user persistent state
- Desktop session: QSettings-based save/restore
- Supabase schema: nullable columns for optional metadata (standard pattern)

### Integration Points
- All correction submission paths (web browse, web search, desktop browse, desktop ResultDialog)
- Comment creation in browse and discoveries
- Session save/restore entry points in both apps

</code_context>

<specifics>
## Specific Ideas

Key insight from external review (Codex): "Different volume = different text. A correction without IE context can be semantically wrong." This elevates CW-01/CW-02 from polish to correctness requirement.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope.

</deferred>

---

*Phase: 61-volume-session-community-validation*
*Context gathered: 2026-03-31*

# Phase 8: Foundation - Context

**Gathered:** 2026-02-07
**Status:** Ready for planning

<domain>
## Phase Boundary

Extract shared service layer from web-only `web/document_service.py` so both web and desktop apps consume PGP data through `shared/document_service.py`. A single `shared/supabase_provider.py` provides the Supabase client. Zero breakage to existing web functionality.

</domain>

<decisions>
## Implementation Decisions

### Migration safety
- Verification approach: Both automated smoke tests AND a manual walkthrough checklist
- Automated tests should catch import breakage and runtime issues (Claude decides appropriate depth)
- Manual checklist covers: transcriptions load, metadata displays, tags work, joins display

### Claude's Discretion
- **Import path strategy**: Whether to use a re-export shim at `web/document_service.py` or update all 15+ import sites — Claude picks the safest approach based on codebase analysis
- **Smoke test depth**: Import-only checks vs import + live data calls — Claude decides based on what's practical
- **Commit strategy**: Single commit vs incremental commits — Claude picks based on risk level
- **Package naming**: `shared/` vs `core/` vs `services/` — Claude decides based on existing conventions

</decisions>

<specifics>
## Specific Ideas

No specific requirements — open to standard approaches. User trusts Claude's judgment on all technical decisions for this infrastructure extraction.

</specifics>

<deferred>
## Deferred Ideas

None — discussion stayed within phase scope

</deferred>

---

*Phase: 08-foundation*
*Context gathered: 2026-02-07*

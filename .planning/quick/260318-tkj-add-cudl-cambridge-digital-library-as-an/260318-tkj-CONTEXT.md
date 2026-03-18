# Quick Task 260318-tkj: Add CUDL as image source for CUL-hosted private collections - Context

**Gathered:** 2026-03-18
**Status:** Ready for planning

<domain>
## Task Boundary

Add CUDL (Cambridge Digital Library) as an image source for Mosseri and other CUL-hosted private collections in enrich_metadata. Currently enrich_metadata only discovers Cambridge IIIF manifests for T-S shelfmarks via the crossref sidecar. Mosseri, Gaster, and other private collections physically held at CUL have high-res images on CUDL but are never discovered.

</domain>

<decisions>
## Implementation Decisions

### Scope of CUDL collections
- Research phase will determine exactly which library_codes are on CUDL using existing docs and the CUDL collection manifest
- User confirmed: use existing documentation in /docs/ as primary research source

### Claude's Discretion
- Classmark conversion rules (shelfmark → CUDL classmark format)
- Manifest verification strategy (construct URL vs verify first)
- Priority placement within enrich_metadata's image source chain

</decisions>

<specifics>
## Specific Ideas

- CUDL manifest URL pattern: shelfmark "Mosseri VI.108" → classmark "MS-MOSSERI-VI-00108" → `https://cudl.lib.cam.ac.uk/iiif/MS-MOSSERI-VI-00108`
- Test shelfmarks: Mosseri Ms. VI 108 (sys_id 990053803100205171), Mosseri Ms. VI 129.3 (sys_id 990053803470205171)
- Fix must be in genizah_core.py enrich_metadata so it flows to ALL consumers
- Open issue exists in docs/OPEN_ISSUES.md (line 88)

</specifics>

<canonical_refs>
## Canonical References

- `docs/plans/EXTERNAL_DATA_INTEGRATION_EXPLORATION.md` — CUDL manifest format, shelfmark↔classmark conversion functions, cambridge_genizah.json collection details
- `docs/OPEN_ISSUES.md` line 88 — tracked open issue for this exact problem

</canonical_refs>

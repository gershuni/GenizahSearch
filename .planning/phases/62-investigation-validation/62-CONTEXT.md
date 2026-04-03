# Phase 62: Investigation & Validation - Context

**Gathered:** 2026-04-03
**Status:** Ready for planning

<domain>
## Phase Boundary

Confirm that server-side NLI image caching is feasible -- rate limits, storage, filesystem, and TOS all validated before any infrastructure is built. This is an investigation phase: the output is validated data and documented decisions, not shipped product code.

</domain>

<decisions>
## Implementation Decisions

### Rate Limit Testing
- **D-01:** Run rate test from user's home PC (residential IP, already has codebase + nli_crossref.db)
- **D-02:** Conservative ramp-up: start at 1 req/sec, ramp to 2, 4, 8 over 15+ minutes. Stop at first sign of throttling. Target ~100-200 images.
- **D-03:** Block detection: abort on HTTP 429 (rate limit) or 403 (forbidden). Also abort on 3+ consecutive timeouts (>30s).
- **D-04:** Test at **two resolutions** for comparison: 800px width (`/full/800,/0/default.jpg`) and 1200px width (`/full/1200,/0/default.jpg`). This feeds directly into INV-05 (resolution decision) with real size data.

### Storage Sampling
- **D-05:** Sample from **NLI-only subset** -- manuscripts with NLI images but no Cambridge/Oxford/Manchester/JTS alternatives. These are the priority cache targets.
- **D-06:** Determine NLI-only subset by querying `nli_crossref.db` -- cross-reference sys_ids with NLI images against those with Cambridge/Manchester/JTS/DPUL entries.
- **D-07:** Target 1000+ images in the sample, drawn from the NLI-only subset. Rate test images count toward sample.

### Filesystem Structure
- **D-08:** Claude's Discretion -- directory layout on EC2 (2-level hash, library-based, or hybrid). Pick based on inode/performance analysis during investigation.

### TOS Outreach
- **D-09:** Review NLI's published TOS/terms of use for IIIF first. Only email NLI if terms are ambiguous or silent on caching.
- **D-10:** If TOS is ambiguous or doesn't explicitly prohibit: proceed cautiously with conservative rate + academic framing. Document the reasoning.
- **D-11:** INV-04 gate: TOS review is sufficient -- no need to block on NLI email response. If TOS doesn't prohibit, mark as "conditional go" and proceed to Phase 63.

### Deliverables
- **D-12:** Phase produces: investigation report (markdown) + reusable test scripts
- **D-13:** Full report in `.planning/phases/62-investigation-validation/62-REPORT.md`, summary in `docs/specs/image-cache-investigation.md`
- **D-14:** Scripts in `scripts/` directory (e.g., `scripts/nli_rate_test.py`, `scripts/nli_storage_sample.py`) -- consistent with existing project convention

### Claude's Discretion
- EC2 filesystem directory structure (D-08)
- Exact sample selection algorithm (random within NLI-only subset, or stratified by some attribute)
- Report structure and sections

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### NLI Integration
- `shared/nli_crossref_service.py` -- 16 methods for NLI crossref DB queries; use for NLI-only subset determination
- `.planning/codebase/INTEGRATIONS.md` -- NLI IIIF API URLs, caching details, external service inventory

### Image Loading (current state)
- `web/api.py` -- Current image proxy and IIIF fetching (6 codepaths to be unified in Phase 64)
- `shared/puzzle_image_service.py` -- Puzzle image service with IIIF fetch + background removal + cache versioning

### Infrastructure
- `.planning/codebase/STACK.md` -- Current tech stack, IMAGE_CACHE_TTL, infrastructure details
- `.planning/codebase/CONCERNS.md` -- Known NLI IIIF concerns (manifest fetching overhead, concurrency limits, API stability)

### Data Sources
- `nli_crossref.db` -- 815K NLI images, 141K Cambridge manifests, 28K Manchester LUNA, 36K JTS DPUL
- `fist_data/fjms_enrichment.db` -- Library codes and catalog data for determining alternative image sources

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/nli_crossref_service.py` -- Has methods for querying by sys_id, library_code, FL ID lookup. Can be used to determine NLI-only subset.
- `nli_crossref.db` tables -- Already tracks which sys_ids have Cambridge/Manchester/JTS images. Cross-reference query is straightforward.
- `scripts/` directory -- Multiple existing batch scripts (build_ie_volume_map.py, validate_ie_volume_map.py) that can serve as patterns for rate test scripts.

### Established Patterns
- HTTP requests via `requests` library with retry logic (used in genizah_core.py, web/api.py)
- SQLite sidecar pattern for local data (pgp.db, fjms_enrichment.db, nli_crossref.db, joins.db)
- Batch scripts in `scripts/` with progress reporting

### Integration Points
- `nli_crossref.db` -- Query for NLI-only subset determination
- NLI IIIF Image API -- `https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/{width},/0/default.jpg`
- NLI IIIF Manifest API -- `https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest`

</code_context>

<specifics>
## Specific Ideas

- Rate test at both 800px and 1200px to directly compare quality/size tradeoff (user wants to see the numbers before deciding INV-05)
- NLI-only subset is the priority corpus -- size estimate should reflect this subset, not the full 815K
- Scripts should be reusable by Phase 63 batch fetcher (not throwaway)
- TOS review before any outreach -- only contact NLI if terms are unclear

</specifics>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope

</deferred>

---

*Phase: 62-investigation-validation*
*Context gathered: 2026-04-03*

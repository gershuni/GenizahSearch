# Phase 62: Investigation & Validation - Context

**Gathered:** 2026-04-03
**Revised:** 2026-04-03 (post-review: Gemini + Codex feedback incorporated)
**Status:** Ready for planning

<domain>
## Phase Boundary

Confirm that server-side NLI image caching is feasible -- rate limits, storage, filesystem, and TOS all validated before any infrastructure is built. This is an investigation phase: the output is validated data and documented decisions, not shipped product code.

</domain>

<decisions>
## Implementation Decisions

### Execution Order

Investigation should proceed in this order to avoid wasted work:

1. **TOS review** (D-09/D-10/D-11) -- if NLI explicitly prohibits caching, nothing else matters
2. **NLI-only subset determination** (D-05/D-06) -- needed to scope the rate test and storage sample
3. **Rate limit testing** (D-01 through D-04) -- produces images that count toward the storage sample
4. **Storage analysis + resolution decision** (D-07/D-08) -- uses rate test data plus additional sampling
5. **Filesystem validation** (D-15) -- informed by storage estimate
6. **Report + cost projection** (D-12 through D-14, D-16)

### TOS Gate (INV-04)

**Requirement reconciliation:** INV-04's "hard go/no-go gate" is interpreted for this phase as: a documented TOS determination must be recorded within 5 business days, including a "conditional go" outcome if terms are silent. The conditional-go path supersedes the original hard-gate wording -- blocking indefinitely on institutional email response is not practical for an academic project. This interpretation is intentional and must be documented in the investigation report.

- **D-09:** Review NLI's published TOS/terms of use for IIIF **first**, before any rate testing. If TOS explicitly prohibits bulk caching, stop the phase immediately and escalate.
- **D-10:** If TOS is ambiguous or silent on caching: contact NLI immediately with a formal academic request explaining the project and intended use. Do not wait indefinitely -- set a 5 business day window for response.
- **D-11:** INV-04 gate criteria: (a) TOS explicitly permits caching -> **GO**. (b) TOS explicitly prohibits -> **NO-GO**. (c) TOS is silent AND email sent AND 5 business days elapsed with no response -> record as **CONDITIONAL GO** with documented reasoning (public IIIF API, academic use, conservative rate). (d) NLI responds with permission -> **GO**. (e) NLI responds with denial -> **NO-GO**.

### Ingest Topology
- **D-17:** Phase 63 assumes **residential fetching plus rsync transfer to EC2**. NLI blocks datacenter IPs (verified 2026-03-17), so EC2 cannot fetch directly from NLI. The end-to-end path is: home PC fetches images from NLI IIIF -> local staging directory -> rsync/scp to EC2 staging -> atomic promotion to live cache. Unless NLI explicitly grants permission for direct server-side acquisition (which would change the architecture), this is the assumed topology for all downstream planning.

### Rate Limit Testing
- **D-01:** Run rate test from user's home PC (residential IP, already has codebase + nli_crossref.db).
- **D-02:** Conservative ramp-up: start at 1 req/sec, ramp to 2, 4, 8 over 15+ minutes. Stop at first sign of throttling. Target ~100-200 images total.
- **D-02a:** Success criteria for INV-01: identify a **sustained plateau rate** (req/sec maintained for 5+ minutes without errors). Report: plateau rate achieved, total images fetched at plateau, error count during ramp-up, error budget (retries do NOT count as successes). "Sustainable rate" = the highest rate held for 5+ minutes with <1% error rate.
- **D-03:** Block detection: abort on HTTP 429 (rate limit) or 403 (forbidden). Also abort on 3+ consecutive timeouts (>30s).
- **D-04:** Test at **two resolutions** for comparison: 800px width (`/full/800,/0/default.jpg`) and 1200px width (`/full/1200,/0/default.jpg`). This feeds directly into INV-05 (resolution decision) with real size data.

### NLI-Only Subset Definition
- **D-05:** Sample from **NLI-only subset** -- manuscripts with NLI images and **no usable non-NLI image source in the current app architecture**.
- **D-06:** Determine NLI-only subset by querying `nli_crossref.db` using the actual image-provider tables, not library_code alone. The app has four non-NLI image providers with DB-backed lookups:
  - **Cambridge** -- `cambridge_manifests` table (141K entries, covers CUL shelfmarks + some Mosseri via Cambridge label construction in genizah_core.py)
  - **Manchester** -- `manchester_luna` table (28K entries, joined via ImageSourceName)
  - **JTS/Princeton** -- `jts_dpul` table (36K entries, matched by shelfmark)
  - **Oxford/Bodleian** -- separate image path in `web/api.py` (`/api/oxford_image/`), uses `oxford_full_db.json` (~13K records). Not in nli_crossref.db -- must be cross-referenced separately.

  **Important nuances:**
  - `library_code` is NOT a safe proxy for image availability. RNL, BL, AIU, Gaster, Halper are collection owners but have **no alternative image provider** -- their images come from NLI. These are NLI-only.
  - Mosseri is tricky: some Mosseri manuscripts have Cambridge-backed coverage (via label/shelfmark match in `cambridge_manifests`). Must check per-manuscript, not per-library.
  - The NLI-only count should be: (manuscripts in `nli_images` table) MINUS (manuscripts with matches in `cambridge_manifests` OR `manchester_luna` OR `jts_dpul` OR `oxford_full_db.json`).

### Storage Sampling
- **D-07:** Target **1000+ images total** across both resolutions. Approach: sample the same 500+ NLI-only manuscripts at both 800px and 1200px. This gives paired comparison data for INV-05 and sufficient sample size for INV-02 at whichever resolution is chosen. Rate test images count toward the total.
- **D-07a:** Quality review for INV-05: after sampling, user reviews a representative mix of manuscript types (handwritten, printed, microfilm, high-res scan) side-by-side at 800px vs 1200px. User decides "good enough for research use." This is a human judgment call, not automated.

### Filesystem Validation (INV-03)
- **D-08:** Implementation Discretion -- directory layout on EC2 (2-level hash, library-based, or hybrid). Pick based on analysis, but the investigation must answer these specific questions:
  - What filesystem type? (ext4 is the EC2 default -- confirm it handles the file count)
  - Inode budget: does the default ext4 inode ratio support 815K+ files plus directory entries?
  - Per-directory file count target: what's the max files per directory before lookup degrades?
  - EBS volume type and size: gp3 vs gp2, provisioned IOPS needed?
- **D-15:** Validate with a practical test: create a representative directory structure on the target EC2 instance with ~50-100K dummy files and measure `ls`, `stat`, and `find` performance. This turns INV-03 from theoretical to empirical.

### Deliverables
- **D-12:** Phase produces: investigation report (markdown) + reusable test scripts
- **D-13:** Full report in `.planning/phases/62-investigation-validation/62-REPORT.md`, summary in `docs/specs/image-cache-investigation.md`
- **D-14:** Scripts in `scripts/` directory (e.g., `scripts/nli_rate_test.py`, `scripts/nli_storage_sample.py`) -- consistent with existing project convention. Scripts should be functional for validation but not over-engineered for production -- Phase 63 will build the real batch fetcher.
- **D-16:** Report must include **projected monthly EBS cost** based on sample data. Calculate: (NLI-only subset count) x (average image size at chosen resolution) = total storage -> map to EBS gp3 pricing. Include bandwidth estimate for rsync transfers.

### Implementation Discretion
- EC2 filesystem directory structure details (D-08, guided by D-15 test results)
- Exact sample selection algorithm (random within NLI-only subset, or stratified by some attribute)
- Report structure and sections

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### NLI Integration
- `shared/nli_crossref_service.py` -- 16 methods for NLI crossref DB queries; has `get_image_sources()` method that already checks Cambridge/Manchester/JTS availability per sys_id. Use for NLI-only subset determination.
- `.planning/codebase/INTEGRATIONS.md` -- NLI IIIF API URLs, caching details, external service inventory

### Image Providers (current non-NLI sources)
- `web/api.py` -- Current image proxy. Has dedicated endpoints for NLI (`/api/nli_image/`), Oxford (`/api/oxford_image/`), Cambridge, Manchester, JTS. Oxford uses separate `oxford_full_db.json` not in nli_crossref.db.
- `shared/puzzle_image_service.py` -- Puzzle image service with IIIF fetch + background removal + cache versioning
- `genizah_core.py` -- Mosseri-to-Cambridge label construction logic (some Mosseri manuscripts get Cambridge images)

### Infrastructure
- `.planning/codebase/STACK.md` -- Current tech stack, IMAGE_CACHE_TTL, infrastructure details
- `.planning/codebase/CONCERNS.md` -- Known NLI IIIF concerns (manifest fetching overhead, concurrency limits, API stability)

### Data Sources
- `nli_crossref.db` -- 815K NLI images, 141K Cambridge manifests, 28K Manchester LUNA, 36K JTS DPUL
- `oxford_full_db.json` -- ~13K Oxford/Bodleian records (NOT in nli_crossref.db)
- `fist_data/fjms_enrichment.db` -- Library codes and catalog data

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/nli_crossref_service.py` -- Has `get_image_sources()` that already checks Cambridge/Manchester/JTS availability per sys_id. Can be extended or queried in batch for NLI-only subset determination.
- `nli_crossref.db` tables -- `nli_images`, `cambridge_manifests`, `manchester_luna`, `jts_dpul` enable SQL-level cross-referencing.
- `oxford_full_db.json` -- Must be loaded separately (not in nli_crossref.db) to exclude Oxford manuscripts.
- `scripts/` directory -- Multiple existing batch scripts (build_ie_volume_map.py, validate_ie_volume_map.py) that can serve as patterns for rate test scripts.

### Established Patterns
- HTTP requests via `requests` library with retry logic (used in genizah_core.py, web/api.py)
- SQLite sidecar pattern for local data (pgp.db, fjms_enrichment.db, nli_crossref.db, joins.db)
- Batch scripts in `scripts/` with progress reporting

### Integration Points
- `nli_crossref.db` -- Query for NLI-only subset determination (4 provider tables)
- `oxford_full_db.json` -- Cross-reference for Oxford exclusion
- NLI IIIF Image API -- `https://iiif.nli.org.il/IIIFv21/FL{fl_id}/full/{width},/0/default.jpg`
- NLI IIIF Manifest API -- `https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest`

</code_context>

<specifics>
## Specific Ideas

- Rate test at both 800px and 1200px to directly compare quality/size tradeoff (user wants to see the numbers before deciding INV-05)
- NLI-only subset is the priority corpus -- size estimate should reflect this subset, not the full 815K
- End-to-end ingest path: residential fetch -> local staging -> rsync to EC2 -> atomic promotion
- TOS review/outreach should start first or in parallel with technical tests -- reply latency is likely the longest path
- Scripts should be functional for validation but not over-hardened -- Phase 63 builds the real fetcher

</specifics>

<deferred>
## Deferred Ideas

None -- discussion stayed within phase scope

</deferred>

---

*Phase: 62-investigation-validation*
*Context gathered: 2026-04-03*
*Revised: 2026-04-03 (post-review)*

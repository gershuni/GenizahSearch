# Phase 79: /api/browse Drill-Down - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-29
**Phase:** 79-api-browse-drill-down
**Areas discussed:** Locator resolution, Response shape & metadata, Image URLs & graceful degrade, Enrichment latency + external review

---

## Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Locator resolution | uid lookup, multi-IE handling, fl_id form, page indexing | ✓ |
| Response shape & metadata | PGP/FJMS/NLI subset; envelope structure | ✓ |
| Image URLs & graceful degrade | One URL vs many; bifolios; degrade pattern; external viewers | ✓ |
| Enrichment latency + external review | Block vs timeout; text cap; rate limit; Codex review | ✓ |

**User's choice:** All four areas selected.

---

## Locator Resolution

### Q1 — uid-only resolution

| Option | Description | Selected |
|--------|-------------|----------|
| Parse uid into IE/P/FL, build reverse-map | Add helper to resolve uid → {sys_id, volume_ie, p_num}; walks browse_map at startup. True 'no-disambiguation'. | |
| Require sys_id alongside uid | Skill always has sys_id from search response; sys_id required + uid optional disambiguator. | ✓ (via Other) |
| Add core method `get_browse_page_by_uid()` | Push resolution into genizah_core.py. Cleaner architecturally; touches 8K-line file. | |

**User's choice:** "Why not require sys_id, if it's always known?" — captured as **D-01: sys_id REQUIRED in every request**.
**Notes:** Skill workflow always carries sys_id alongside uid in /api/search response items. Requiring sys_id sidesteps reverse-map cost AND avoids touching genizah_core.py.

### Q2 — Multi-IE manuscript with uid only

| Option | Description | Selected |
|--------|-------------|----------|
| uid pins the volume | uid = IE_P_FL; the IE component IS the volume_ie. uid is authoritative. | ✓ |
| Reject as ambiguous | 400 'volume_required' — contradicts goal #2 'no disambiguation step'. | |
| Default to primary IE (suffix=1) | Fall back when both uid and volume_ie missing. | |

**User's choice:** uid pins the volume (recommended).
**Notes:** Captured as D-02. When uid is supplied, volume_ie/p_num/fl_id query params are ignored with a `locator_redundant_fields_ignored` warning if they conflict.

### Q3 — fl_id as third locator form

| Option | Description | Selected |
|--------|-------------|----------|
| No — uid + sys_id+volume_ie+p_num only | Keeps contract minimal. fl_id is implementation detail. | |
| Yes — fl_id as third query-param form | Mirrors existing internal `get_browse_page_by_fl(fl_id, sys_id=...)` helper. Adds one validator branch. | ✓ |

**User's choice:** Yes — fl_id accepted.
**Notes:** Captured as D-02 (third form) and D-03 (validator). Direct folio access for downstream tools that already have fl_id.

### Q4 — Page indexing convention

| Option | Description | Selected |
|--------|-------------|----------|
| Echo p_num + add 'page_indexing': '1-based' | 1-based already locked Phase 77/78. Self-documenting per goal #1. | ✓ |
| Use 'page': {number, indexing, total} nested object | More explicit + extensible. Slight bloat. | |
| Echo 0-based | Conflicts with Phase 77 emission. | |

**User's choice:** Echo p_num + page_indexing field (recommended). Captured as D-05.

---

## Response Shape & Metadata

### Q1 — PGP fields (multiSelect)

| Option | Description | Selected |
|--------|-------------|----------|
| Core: description + tags + dates + languages + pgpid + pgp_url | Highest signal-per-byte. ~6 fields. | ✓ |
| Add: doc_relation + sources list (editions/translations) | Strong scholarly signal but list grows. | |
| Add: full transcription text | Page-scoped already coming via 'text'; duplicates. | |
| Add: pgp_metadata.tags + inferred_date_rationale | Inferred-date rationale is high-signal. | |

**User's choice:** Core only.
**Notes:** Captured as D-07. `metadata.pgp = null` when manuscript has no PGP record. Sources list and full transcription deferred (see deferred ideas).

### Q2 — FJMS fields

| Option | Description | Selected |
|--------|-------------|----------|
| source_names + has_measurements + has_visual_suggestions | Compact. Catalog attribution + flags. | ✓ |
| Full bibliography[] entries | 5–30 entries; bloats. | |
| Skip FJMS entirely | Smallest response. Lose scholarly attribution. | |
| Bare minimum: just source_names | Lighter. Loses measurement/VS flags. | |

**User's choice:** Recommended (source_names + flags). Captured as D-08. Bibliography deferred to opt-in `?include=bibliography`.

### Q3 — NLI fields

| Option | Description | Selected |
|--------|-------------|----------|
| physical_metadata + current page's folio info only | physical_metadata: {material, num_folio, num_bifolio, size}. Active folio: {fl_id, folio_label, thumb_url}. | ✓ |
| Add full folio_images[] list | 20–200 entries per manuscript. Skill can re-call with different p_num. | |
| Skip NLI metadata, keep image_url only | Lose material/dimension context. | |

**User's choice:** Recommended. Captured as D-09. Full folio sequence deferred to opt-in `?include=folios`.

### Q4 — Top-level envelope

| Option | Description | Selected |
|--------|-------------|----------|
| Flat with namespaced groups | `metadata: {pgp, fjms, nli}` provenance grouping. Mirrors search envelope. | ✓ |
| Flatter — metadata fields hoisted | Less nesting; loses provenance. | |
| Match /api/search result-item shape exactly | Search-item is rank-optimized; browse is grounding-optimized. Shoehorning. | |

**User's choice:** Flat with namespaced groups (recommended). Captured as D-06.

---

## Image URLs & Graceful Degrade

### Q1 — Image URL strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Library-aware picked URL + sources[] with all proxies + external viewers | Pick correct proxy per library; expose alternates with role tag. Skill uses image.url, can fall back. | ✓ |
| Single URL only — match search emission | Simplest. Loses Cambridge-quality + external viewer links. | |
| Library-aware single URL, no sources[] | Lighter. Skill can't fall back without re-call. | |

**User's choice:** Library-aware + sources[] (recommended). Captured as D-12 + D-13.

### Q2 — CUDL bifolios

| Option | Description | Selected |
|--------|-------------|----------|
| Single image object — alternates inside sources[] | image: {url, provider} for primary; bifolio companions in sources[] with role: 'companion_folio'. | ✓ |
| image is always a list | Uniform but verbose for 95% case. | |
| Top-level folios[] field always present | Most explicit; biggest payload. | |

**User's choice:** Single image + sources[] alternates (recommended). Captured as D-13.

### Q3 — Graceful degrade

| Option | Description | Selected |
|--------|-------------|----------|
| image.url = null + warnings: ['image_unavailable: ...'] | Mirrors Phase 78 warnings array pattern. 200 still. | ✓ |
| Add image.status: 'available' | 'unavailable' | 'pending' | Explicit field; duplicates info. | |
| 503 if image source fails | Hard fail; goal #3 forbids. | |

**User's choice:** null + warnings (recommended). Captured as D-14.

### Q4 — External direct URLs

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — include in sources[] with role: 'external_viewer' | Skill citations link to authoritative library viewer. | ✓ |
| No — only proxy URLs | Skill never points users at external. Defensible if no endorsement. | |
| Top-level field 'external_viewer_url' | Single dedicated field. | |

**User's choice:** sources[] entry with role tag (recommended). Captured as D-13.

---

## Enrichment Latency + External Review

### Q1 — Blocking strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Block on all sources with per-source timeout | asyncio.gather + asyncio.wait_for; default 2s per source; null + warning on timeout. | ✓ |
| Block on core only; enrichment best-effort with very short timeout | 500ms enrichment; lower latency. | |
| Block on everything, no timeout | Can hang on NLI IIIF stalls. Goal #3 incompatible. | |
| Two modes: ?full=false vs ?full=true | Skill picks. Adds branch logic; overkill for v7.10. | |

**User's choice:** Per-source timeout (recommended). Captured as D-15, D-16, D-17.

### Q2 — Text length cap

| Option | Description | Selected |
|--------|-------------|----------|
| 8000 chars hard cap | ~1500 Hebrew/Arabic words; generous. | |
| Tighter cap: 4000 chars | ~800 words; saves ~50% bandwidth. | ✓ |
| Looser cap: 16000 chars | Almost never truncates. Larger payloads. | |
| No cap | Page-scoped already short. Belt-and-suspenders. | |

**User's choice:** 4000 chars. Captured as D-11.
**Notes:** Configurable via SEARCH_API_BROWSE_TEXT_CAP. Truncate at word boundary; add text_truncated + warnings entry.

### Q3 — Rate limit bucket

| Option | Description | Selected |
|--------|-------------|----------|
| Same SEARCH_API_RATE_LIMIT, separate per-IP bucket per endpoint | Skill workflow (search once + browse N times) doesn't hit search's limit. | ✓ |
| Shared bucket — one limit across search + browse + parallels | Uniform DoS protection. Penalizes realistic skill workflow. | |
| Browse-specific env var SEARCH_API_BROWSE_RATE_LIMIT | More flexibility, more knobs. | |

**User's choice:** Same env var, separate bucket (recommended). Captured as D-18, D-19. Flagged as Codex Q3 for review.

### Q4 — External Codex review

| Option | Description | Selected |
|--------|-------------|----------|
| Yes — run open questions through Codex after this discussion | Phase 78 precedent. | ✓ |
| No — capture as Claude defaults, proceed to plan | Phase 79 is mostly response-shape on top of well-trodden patterns. | |
| Cross-AI review via /gsd-review during plan-phase | Standard post-plan pattern. | |

**User's choice:** Codex review pending (recommended). 6 open questions captured at the bottom of CONTEXT.md.

---

## Final Confirmation

| Option | Description | Selected |
|--------|-------------|----------|
| I'm ready for context | Lock decisions; write CONTEXT.md; Codex review after. | ✓ |
| One more area: error codes for browse-specific failures | uid_not_found, page_not_found, manuscript_not_found, multi_ie_volume_required. | |
| One more area: Hebrew/RTL text handling | Encoding, normalization, bidi marks. | |
| Explore more gray areas | Pick something else. | |

**User's choice:** Ready for context.

---

## Claude's Discretion

Captured at end of CONTEXT.md `<decisions>` section under "Claude's Discretion":
- Service-layer extraction shape (D-23 — `shared/browse_service.py` vs inline)
- Whether `image.sources[]` is populated when `image.url` is null
- Daemon/cleanup hooks (none required)
- `fl_id` validation depth (accept any non-empty string)
- Accept-header content negotiation (skip; respond JSON unconditionally)
- Whether to expose `navigation: {prev_p_num, next_p_num, total_pages}` hint (nice-to-have)

## Deferred Ideas

Captured in CONTEXT.md `<deferred>` section:
- ?include=bibliography (FJMS)
- ?include=full_transcription (PGP)
- ?include=folios (NLI folio sequence)
- ?include=sources (PGP editions/translations)
- navigation hint as nice-to-have during planning
- HEAD method support
- Accept-Language honoring
- CORS
- /api/browse/manuscript/{sys_id} collection endpoint
- Cache-Control headers

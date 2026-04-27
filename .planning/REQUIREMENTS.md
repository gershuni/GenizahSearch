# Requirements: GenizahSearch

**Defined:** 2026-04-27
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.10 Requirements — Search API

Add a thin internal HTTP/JSON surface over the existing search, parallels, and browse pipelines so external automation (first consumer: a Claude skill that sorts/ranks results) can drive GenizahSearch. Helper surface, not platform — narrow endpoints, no public docs, no long-term stability promise.

Build on `web/api.py` (existing FastAPI surface mounted in the NiceGUI app) and `SearchEngine.execute_search(...)` in `genizah_core.py`. Search behavior is split between UI orchestration in `web/pages/search.py` and core execution in `genizah_core.py`; the API surface exposes only the subset the Claude skill actually needs — full search-page parity is explicitly **not** a goal for v7.10.

The three new endpoints below are referred to collectively as the **search-helper endpoints** (`/api/search`, `/api/parallels`, `/api/browse`). Hardening, observability, and access-mode requirements apply to *those three new routes only*; existing `/api/*` routes (image proxies, puzzle uploads, etc.) are out of scope for this milestone and unaffected.

Auth posture: open + rate-limit + capped result count + capped query length. No API keys in v1.

### API Endpoints

- [ ] **API-01**: `POST /api/search` accepts exactly `{query, mode, gap?, limit?, filters?}` where `mode` is an explicit enum (text / Title / Shelfmark / Responsa) and `filters` is the exact supported subset (domain, author, work, date_from, date_to, material — no others in v1). Each response item includes the drill-down locator (see API-05) plus fixed ranking fields: `score`, `shelfmark`, `title`, `snippet`, `excerpt` (short text), key metadata (library, domain, dating).
- [ ] **API-02**: `POST /api/parallels` accepts the v7.10 subset actually needed by the consumer: `text`, `chunk_size`, `mode`, `max_freq?`, optional same filter subset as API-01, optional boundary options. Response defines whether filtered / high-frequency hits are returned separately (under a `filtered` key) or omitted; the chosen behavior is documented in the response shape.
- [ ] **API-03**: `GET /api/browse` resolves a specific manuscript page by `uid` (preferred) or by `sys_id + volume_ie + page` (fallback). Page indexing is explicit (1-based or 0-based, whichever core uses, documented in the response). Response is a minimal fixed shape: text (transcription if available, snippet otherwise), metadata (PGP/FJMS/NLI subset documented in DOC-01), image URLs. Not browse-page parity — the contract is "what the Claude skill needs to rank," not "everything browse renders."
- [ ] **API-04**: The three search-helper endpoints validate inputs (max query length, max result count, allowed enum values, allowed filter keys) and return a consistent error envelope `{error: {code, message}}` rather than raw FastAPI 422 dumps. Existing `/api/*` routes are unaffected.
- [ ] **API-05**: Search and parallels responses include a drill-down locator on every result that is sufficient to call `/api/browse` without follow-up disambiguation. At minimum: `uid`. If `uid` is not exposed by core, the locator is `{sys_id, volume_ie, p_num}`. `sys_id` alone is insufficient — multi-IE manuscripts and page-level hits require the volume + page.
- [ ] **API-06**: The three search-helper endpoints are stateless and request-driven. No dependence on NiceGUI session state (`app.storage.user`), `state.last_results`, refinement chain, or any prior UI action. A request must produce identical output regardless of whether a browser session exists.
- [ ] **API-07**: Filter values submitted to `/api/search` and `/api/parallels` are resolved through the same FJMS `restrict_sys_ids` pipeline that the UI uses; the API does not implement a parallel filter engine. If a filter cannot be resolved through that pipeline, it is rejected at the endpoint, not silently ignored.

### JSON Export

- [ ] **EXPORT-01**: `/search` page has a toolbar button that downloads the current result set as Claude-friendly JSON. Payload uses the same field shape as `POST /api/search` responses.
- [ ] **EXPORT-02**: `/parallels` page has the same export button; payload matches `POST /api/parallels` responses.
- [ ] **EXPORT-03**: Export payload shape and API payload shape share a single source of truth (one serializer module) so they cannot drift.
- [ ] **EXPORT-04**: Export filename includes a timestamp + page identifier (e.g. `genizah-search-2026-04-27T1530.json`) so multiple downloads do not silently overwrite.

### Hardening

- [ ] **HARDEN-01**: Per-IP rate limit on the three search-helper endpoints (configurable via env var, default conservative — e.g. 30 req/min). Limit-exceeded returns 429 with `Retry-After`. Other `/api/*` routes are unaffected.
- [ ] **HARDEN-02**: Capped result count on `/api/search` and `/api/parallels` (default 50, max 200). Requests above the cap return 400 with the cap echoed in the error message.
- [ ] **HARDEN-03**: Capped query length and reuse of the existing Responsa combinatorial cap (`MAX_EXPANDED_TERMS = 500`). When the cascade downgrades the query, the response surfaces this in a top-level `warnings: [...]` array (or equivalent `query_adjustments`) — never hidden inside the first result item.
- [ ] **HARDEN-04**: Access posture for the search-helper endpoints configurable via env var: `SEARCH_API_MODE=open|localhost-only|disabled`. Default `open` for v7.10. Production server can flip without a code change. Other `/api/*` routes unaffected.
- [ ] **HARDEN-05**: PostHog event emitted per search-helper request (or per N requests) capturing endpoint, mode, latency bucket, result-count bucket, IP-hash. No payload contents logged. Existing `/api/*` routes unaffected.

### Skill Consumer

- [ ] **SKILL-01**: Reference consumer skill/harness with configurable base URL (default points at the production deployment). The skill itself is a deliverable of v7.10 — its filesystem location is environment-specific and not a GenizahSearch requirement; this milestone only requires the skill exist and be runnable.
- [ ] **SKILL-02**: Skill demonstrates the end-to-end loop: search → browse for top N candidates → return ranked list with brief justifications grounded in the fetched text. Treated as the v7.10 acceptance harness for the API.
- [ ] **SKILL-03**: Skill handles upstream errors gracefully (rate limit / 429, timeouts, partial `/api/browse` data on NLI outage) — does not crash the conversation; surfaces the failure in plain terms.

### Documentation (internal only)

- [ ] **DOC-01**: One `docs/SEARCH_API.md` page documenting the three endpoints, exact payload shapes, env vars, and the explicit "internal, no stability promise" disclaimer. Not linked from the public site.
- [ ] **DOC-02**: `CLAUDE.md` updated with the new env vars (`SEARCH_API_MODE`, rate-limit knobs). Public-facing `README.md` is intentionally not updated — the API is internal.

## Future Requirements

Deferred to later milestones.

| Requirement | Defer reason |
|-------------|--------------|
| API keys / auth tokens | Defer until a second consumer appears or open + rate-limit proves insufficient |
| Versioned routes (`/api/v1/*`) | Premature; v7.10 explicitly disclaims long-term stability — re-introduce when the contract starts to matter externally |
| OpenAPI / public docs | Out of scope by design — would invite usage we are not committing to support |
| Full search-page filter parity | Expand on demand from the Claude skill experience, not all at once |
| Desktop app exposing the API | Web-only for v7.10; the desktop already calls `genizah_core` directly |
| Streaming responses / SSE | Narrow consumer does not need it |
| Cursor-based pagination | Offset+limit is sufficient for capped result counts |
| Multi-tenant / per-user rate limits | Single shared rate limit is enough at current traffic |
| Refinement chain through API | Refinement chain stays UI-only in v7.10; consumer can replay multi-step search itself if needed |

## Out of Scope

| Feature | Reason |
|---------|--------|
| Public marketing of the API | Helper surface, not platform — public docs invite obligations we are not taking on |
| GraphQL endpoint | Massive scope for the Claude-skill use case; REST + JSON is sufficient |
| WebSocket / streaming search | Search is fast enough to return in one shot at the capped result count |
| Allowing arbitrary SQL / filter DSL through the API | SSRF-style abuse risk and engine misuse risk; only the explicit filter subset is exposed |
| Returning full transcription text without snippet bounds | License and attribution constraints; excerpts only, attribution preserved |
| Bypassing existing exclusion / refinement chain semantics | Not in v1 — keep the API at the core search level |
| Touching existing `/api/*` routes (image proxies, puzzle uploads, NLI proxies) | Out of milestone scope — adding rate-limit / observability there is its own future decision |

## Traceability

Every v7.10 requirement maps to exactly one phase. Phase numbering continues from v7.9 (last phase 76); v7.10 phases are 77-82.

| Requirement | Phase | Status |
|-------------|-------|--------|
| API-01 | Phase 78 | Pending |
| API-02 | Phase 80 | Pending |
| API-03 | Phase 79 | Pending |
| API-04 | Phase 78 | Pending |
| API-05 | Phase 78 | Pending |
| API-06 | Phase 78 | Pending |
| API-07 | Phase 78 | Pending |
| EXPORT-01 | Phase 77 | Pending |
| EXPORT-02 | Phase 77 | Pending |
| EXPORT-03 | Phase 77 | Pending |
| EXPORT-04 | Phase 77 | Pending |
| HARDEN-01 | Phase 78 | Pending |
| HARDEN-02 | Phase 78 | Pending |
| HARDEN-03 | Phase 78 | Pending |
| HARDEN-04 | Phase 78 | Pending |
| HARDEN-05 | Phase 78 | Pending |
| SKILL-01 | Phase 81 | Pending |
| SKILL-02 | Phase 81 | Pending |
| SKILL-03 | Phase 81 | Pending |
| DOC-01 | Phase 82 | Pending |
| DOC-02 | Phase 82 | Pending |

**Per-phase requirement counts:**

| Phase | Requirements | Count |
|-------|--------------|-------|
| Phase 77: Serializer & JSON Export | EXPORT-01, EXPORT-02, EXPORT-03, EXPORT-04 | 4 |
| Phase 78: /api/search + Hardening Shell | API-01, API-04, API-05, API-06, API-07, HARDEN-01, HARDEN-02, HARDEN-03, HARDEN-04, HARDEN-05 | 10 |
| Phase 79: /api/browse Drill-Down | API-03 | 1 |
| Phase 80: /api/parallels | API-02 | 1 |
| Phase 81: Claude Skill Consumer | SKILL-01, SKILL-02, SKILL-03 | 3 |
| Phase 82: Internal Documentation | DOC-01, DOC-02 | 2 |
| **Total** | | **21** |

**Coverage:**
- v7.10 requirements: 21 total (7 API + 4 EXPORT + 5 HARDEN + 3 SKILL + 2 DOC)
- Mapped to phases: 21 ✓
- Unmapped: 0 ✓
- Double-mapped: 0 ✓

**Cross-phase note on API-05 (drill-down locator):** API-05 is *owned* by Phase 78 because that is where the locator's response shape is first established on `/api/search`. The locator obligation actually spans **Phase 77 → 78 → 79 → 80**: Phase 77 (export) embeds it in downloaded JSON; Phase 78 (`/api/search`) emits it; Phase 79 (`/api/browse`) *consumes* it and validates the round-trip end-to-end; Phase 80 (`/api/parallels`) inherits the same locator on its responses. The requirement is mapped once to Phase 78 for traceability; the cross-phase obligations are captured in each downstream phase's success criteria, not by re-mapping. Phase 79 (browse) is sequenced before Phase 80 (parallels) per Codex review so the locator round-trip is validated by a real consumer before a second producer is added.

---
*Requirements defined: 2026-04-27*
*Last updated: 2026-04-27 — v7.10 traceability populated by roadmapper; Codex review pass swapped Phase 79 (browse) ↔ Phase 80 (parallels)*

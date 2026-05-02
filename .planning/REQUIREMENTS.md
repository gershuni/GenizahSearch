# Requirements: GenizahSearch

**Defined:** 2026-04-27
**Core Value:** Researchers can find what they need in the Genizah corpus

## v7.10 Requirements — Search API

Add a thin internal HTTP/JSON surface over the existing search, parallels, and browse pipelines so external automation (first consumer: a Claude skill that sorts/ranks results) can drive GenizahSearch. Helper surface, not platform — narrow endpoints, no public docs, no long-term stability promise.

Build on `web/api.py` (existing FastAPI surface mounted in the NiceGUI app) and `SearchEngine.execute_search(...)` in `genizah_core.py`. Search behavior is split between UI orchestration in `web/pages/search.py` and core execution in `genizah_core.py`; the API surface exposes only the subset the Claude skill actually needs — full search-page parity is explicitly **not** a goal for v7.10.

The three new endpoints below are referred to collectively as the **search-helper endpoints** (`/api/search`, `/api/parallels`, `/api/browse`). Hardening, observability, and access-mode requirements apply to *those three new routes only*; existing `/api/*` routes (image proxies, puzzle uploads, etc.) are out of scope for this milestone and unaffected.

Auth posture: open + rate-limit + capped result count + capped query length. No API keys in v1.

### API Endpoints

- [ ] **API-01** *(SUPERSEDED on the request-shape clauses by API-EXPAND-01..06; response-shape clauses still apply)*: `POST /api/search` request shape is now defined by API-EXPAND-01..06 (UI-aligned `search_mode` enum + `responsa_options`). The original `{query, mode, gap?, limit?, filters?}` shape with `mode: text|Title|Shelfmark|Responsa` is replaced. Response-shape clauses from API-01 still apply: each response item includes the drill-down locator (see API-05) plus fixed ranking fields — `score`, `shelfmark`, `title`, `snippet`, `excerpt` (short text), key metadata (library, `domains: list[str]` *(plural; supersedes prior singular `domain` phrasing — locked at Phase 77 plan time, MED-01)*, dating). `image_url` is server-relative `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` for NLI-resolvable providers, `null` for non-NLI providers (e.g. Oxford-only) per Phase 77 HIGH-07.
- [ ] **API-02**: `POST /api/parallels` accepts the v7.10 subset actually needed by the consumer: `text`, `chunk_size`, `mode`, `max_freq?`, optional same filter subset as API-01, optional boundary options. Response defines whether filtered / high-frequency hits are returned separately (under a `filtered` key) or omitted; the chosen behavior is documented in the response shape.
- [ ] **API-03**: `GET /api/browse` resolves a specific manuscript page by `uid` (preferred) or by `sys_id + volume_ie + page` (fallback). Page indexing is explicit (1-based or 0-based, whichever core uses, documented in the response). Response is a minimal fixed shape: text (transcription if available, snippet otherwise), metadata (PGP/FJMS/NLI subset documented in DOC-01), image URLs. Not browse-page parity — the contract is "what the Claude skill needs to rank," not "everything browse renders."
- [ ] **API-04**: The three search-helper endpoints validate inputs (max query length, max result count, allowed enum values, allowed filter keys) and return a consistent error envelope `{error: {code, message}}` rather than raw FastAPI 422 dumps. Existing `/api/*` routes are unaffected.
- [ ] **API-05**: Search and parallels responses include a drill-down locator on every result that is sufficient to call `/api/browse` without follow-up disambiguation. At minimum: `uid`. If `uid` is not exposed by core, the locator is `{sys_id, volume_ie, p_num}`. `sys_id` alone is insufficient — multi-IE manuscripts and page-level hits require the volume + page.
- [ ] **API-06**: The three search-helper endpoints are stateless and request-driven. No dependence on NiceGUI session state (`app.storage.user`), `state.last_results`, refinement chain, or any prior UI action. A request must produce identical output regardless of whether a browser session exists.
- [ ] **API-07**: Filter values submitted to `/api/search` and `/api/parallels` are resolved through the same FJMS `restrict_sys_ids` pipeline that the UI uses; the API does not implement a parallel filter engine. If a filter cannot be resolved through that pipeline, it is rejected at the endpoint, not silently ignored.

### JSON Export

- [ ] **EXPORT-01**: `/search` page has a toolbar button that downloads the current result set as Claude-friendly JSON. Payload uses the same field shape as `POST /api/search` responses, including `domains: list[str]` (plural; the singular `domain` phrasing in any earlier discussion is superseded — Phase 77 MED-01).
- [ ] **EXPORT-02**: `/parallels` page has the same export button; payload matches `POST /api/parallels` responses.
- [ ] **EXPORT-03**: Export payload shape and API payload shape share a single source of truth (one serializer module) so they cannot drift.
- [ ] **EXPORT-04**: Export filename includes a timestamp + page identifier (e.g. `genizah-search-2026-04-27T1530.json`) so multiple downloads do not silently overwrite. Phase 77 implementation uses millisecond resolution + monotonic counter (HIGH-06) to guarantee distinct filenames on consecutive same-second clicks.

### Hardening

- [ ] **HARDEN-01**: Per-IP rate limit on the three search-helper endpoints (configurable via env var, default conservative — e.g. 30 req/min). Limit-exceeded returns 429 with `Retry-After`. Other `/api/*` routes are unaffected.
- [ ] **HARDEN-02**: Capped result count on `/api/search` and `/api/parallels`. **Updated 2026-05-02 by API-EXPAND-05**: default 50, max 100 (lowered from 200 — conservative posture before widening based on PostHog data). Requests above the cap return 422 (Pydantic constraint).
- [ ] **HARDEN-03**: Capped query length and reuse of the existing Responsa combinatorial cap (`MAX_EXPANDED_TERMS = 500`). When the cascade downgrades the query, the response surfaces this in a top-level `warnings: [...]` array (or equivalent `query_adjustments`) — never hidden inside the first result item.
- [ ] **HARDEN-04**: Access posture for the search-helper endpoints configurable via env var: `SEARCH_API_MODE=open|localhost-only|disabled`. Default `open` for v7.10. Production server can flip without a code change. Other `/api/*` routes unaffected.
- [ ] **HARDEN-05**: PostHog event emitted per search-helper request (or per N requests) capturing endpoint, mode, latency bucket, result-count bucket, IP-hash. No payload contents logged. Existing `/api/*` routes unaffected.

### API Contract Expansion (Phase 81A — added 2026-05-02 via rescope)

Live testing after Phase 80 surfaced that the API as shipped under API-01 was not expressive enough for witness-discovery skills. API-EXPAND-* requirements REPLACE the conflated `mode` field in API-01 with a UI-aligned surface. See `.planning/phases/81B-claude-skill-consumer/81-RESCOPE.md` for full rationale.

- [ ] **API-EXPAND-01**: `POST /api/search` request shape changes (BREAKING; internal API). Old `mode: text|Title|Shelfmark|Responsa` is replaced by `search_mode: exact|variants|regex|responsa|title|shelfmark` (six values). The old `mode` field is rejected (`extra='forbid'`) with `invalid_request`. `'fuzzy'` is intentionally NOT in v7.10 — it would map to deferred `variants_maximum`.
- [ ] **API-EXPAND-02**: `responsa_options: {variants, ja, flex_spacing, bidirectional}` is accepted only when `search_mode='responsa'`. Field names mirror the desktop UI exactly (`genizah_app.py:15788-15797`). Internal `variant_mode` is derived server-side and NOT exposed in the API. `responsa_options` with any non-responsa mode returns 400 `invalid_combination`.
- [ ] **API-EXPAND-03**: All 6 `search_mode` values produce non-empty results on at least one fixture query each. All 4 `responsa_options` flags produce a measurable behavioral change on at least one Responsa fixture query each.
- [ ] **API-EXPAND-04**: Cross-field validation matrix returns 400 `invalid_combination` for: (a) `responsa_options` with non-responsa mode; (b) non-zero `gap` with `search_mode='title'` or `'shelfmark'`; (c) non-empty `responsa_options` with `search_mode='regex'`. Error message identifies both offending fields.
- [ ] **API-EXPAND-05**: `limit` ceiling lowered from 200 to 100 (Pydantic `Field(ge=1, le=100)`). `query` length cap stays at 1000 chars for non-regex modes; `search_mode='regex'` adds a 256-char pattern cap returning 400 `regex_pattern_too_long` above. Existing `query_required` / `query_too_long` codes preserved.
- [ ] **API-EXPAND-06**: Response envelope adds a `request` echo block: `search_mode` (always identical to client request — never silently downgraded), `responsa_options` (when applicable), `responsa_options_effective` (when applicable; reflects Responsa cascade-disabled options), `gap`, `limit`, `limit_effective`, `filters`. Responsa cascade case shows `responsa_options != responsa_options_effective` AND surfaces the disabled options as `tr()` strings in `warnings[]`.
- [ ] **API-EXPAND-07**: `/api/parallels` envelope gains the `request` echo block. The existing `mode: exact|variants|fuzzy` field name is preserved on `/api/parallels` (not renamed to `search_mode`) — the temporary stylistic inconsistency between the two endpoints is documented in DOC-01. Phase 80 tests pass unchanged.
- [ ] **API-EXPAND-08**: All Phase 78/79/80 hardening behaviors continue to hold for the expanded surface (rate limit, mode gate, error envelope shape, PostHog capture, statelessness, per-bucket independence). PostHog event gains properties `search_mode_value` and `responsa_options_count` (count of True flags in ResponsaOptions, 0 if None).

### Skill Consumer

Existing SKILL-01..03 stay. Three new requirements added 2026-05-02 to capture browse-honesty, known-witness policy, and request-pacing behavior surfaced during the rescope.

- [ ] **SKILL-01**: Reference consumer skill/harness with configurable base URL (default points at the production deployment). The skill itself is a deliverable of v7.10 — its filesystem location is environment-specific and not a GenizahSearch requirement; this milestone only requires the skill exist and be runnable.
- [ ] **SKILL-02**: Skill demonstrates the end-to-end loop via staged phrase discovery: extract distinctive phrases from `query` or `base_text`, run multiple `/api/search` calls (using the API-EXPAND surface), merge by `uid`/`sys_id`, drill down via `/api/browse` for top-N candidates, return a ranked candidate list with brief justifications grounded in the fetched browse text. Treated as the v7.10 acceptance harness for the API.
- [ ] **SKILL-03**: Skill handles upstream errors gracefully (rate limit / 429, timeouts, partial `/api/browse` data on NLI outage) — does not crash the conversation; surfaces the failure in plain terms.
- [ ] **SKILL-04**: Browse honesty. When `/api/browse` returns `text_source != 'full'`, the candidate's justification appends `"(full text unavailable; based on snippet of N chars)"`. When `image_url` is null or NLI returns 4xx for the image, the output appends `"(no image available)"`. Researchers always know what evidence the justification is grounded in.
- [ ] **SKILL-05**: Known-witness policy. Skill accepts optional `known_witnesses[]` (shelfmark strings) and `known_witness_policy: 'flag' | 'exclude'` (default `'flag'`). `'flag'` keeps known witnesses in the candidate list with a `known_witness: true` marker; `'exclude'` removes them. Shelfmark normalization is a two-tier strategy: lightweight local normalization (Tier 1) plus `/api/search?search_mode=shelfmark` resolution as fallback (Tier 2). Skill does NOT depend on `genizah_core`.
- [ ] **SKILL-06**: Request-pacing. Skill self-paces using a token-bucket throttle, separate buckets for `/api/search` and `/api/browse`, default ≤24 req/min per bucket (= 0.4 req/s, headroom under the server's 30 req/min limit). Burst capacity 5. Configurable via `GENIZAH_SKILL_REQ_PER_MIN` env var. A single skill run with 15 search + 10 browse calls completes without triggering its own rate limit.

### Documentation (internal only)

- [ ] **DOC-01**: One `docs/SEARCH_API.md` page documenting the three endpoints, exact payload shapes, env vars, and the explicit "internal, no stability promise" disclaimer. Not linked from the public site.
- [ ] **DOC-02**: `CLAUDE.md` updated with the new env vars (`SEARCH_API_MODE`, rate-limit knobs). Public-facing `README.md` is intentionally not updated — the API is internal.

# Phase 79: /api/browse Drill-Down - Context

**Gathered:** 2026-04-29
**Status:** Ready for planning (Codex external review pending — see open questions at bottom)

<domain>
## Phase Boundary

Build `GET /api/browse` — a stateless drill-down endpoint that takes a locator emitted by `/api/search` (Phase 78) and returns one manuscript page's text + metadata + image URLs in a single round-trip, no follow-up calls, no session state. This is the **first real consumer** of the locator contract emitted in Phase 77/78 — it closes the search → browse vertical slice the Claude skill (Phase 81) needs.

**In scope:**
- New route `GET /api/browse` registered in the existing `web/search_api.py` (Phase 78 D-18 already established that module owns search-helper routes; Phase 79 adds the second route).
- Per-IP rate-limit bucket distinct from `/api/search`'s bucket (same SEARCH_API_RATE_LIMIT env-var ceiling, separate counters), inheriting Phase 78's `RateLimiter` + `wrap_endpoint` + `_build_envelope_response` + `capture_api_event` (with `endpoint='browse'`).
- Locator-resolution helper layer: `sys_id` is REQUIRED in every request; `uid` (preferred) or `p_num+volume_ie` or `fl_id` provide the page-pinning signal.
- Pydantic request validation (`extra='forbid'`), echoing Phase 78's `invalid_request` envelope path.
- Enrichment fan-out: PGP transcription (page-section-scoped) + FJMS catalog subset + NLI crossref physical metadata + folio info. Each source guarded by `asyncio.wait_for(timeout)`; partial results surface in `warnings[]`.
- Response shape: flat envelope with namespaced metadata groups (`metadata: {pgp, fjms, nli}`), `image.url` library-aware + `image.sources[]` with all proxies and external viewers tagged by role.
- Two new env vars: `SEARCH_API_BROWSE_TIMEOUT` (per-source enrichment timeout, default 2s) and `SEARCH_API_BROWSE_TEXT_CAP` (transcription char cap, default 4000).
- Test surface: pytest TestClient unit tests for locator round-trip (single-IE + multi-IE); error envelopes; graceful image degrade; transcription truncation; per-source timeout warnings; statelessness.

**Out of scope:**
- `POST /api/parallels` (Phase 80) — the parallels endpoint inherits the same hardening shell built in Phase 78 plus the locator-emission contract; Phase 79 only **consumes** the locator.
- Skill consumer / `docs/SEARCH_API.md` (Phases 81 + 82).
- New genizah_core method `get_browse_page_by_uid()`. Not adding the surface; uid is treated as a verification + IE-pinning token alongside the required `sys_id` (decision D-01 below).
- Authentication / API keys (consistent with Phase 78 D-out-of-scope).
- CORS (consistent with Phase 78).
- Modifying `web/api.py` legacy `/api/*` proxy routes. The new endpoint lives in `web/search_api.py`; legacy image proxies (`/api/nli_image_by_sysid`, `/api/cambridge_image`, etc.) stay byte-identical.
- Changes to `web/pages/browse.py` UI flow. The web UI continues to drive its own enrichment via `web/pages/browse_enrichment.py`; Phase 79 may extract a shared service-layer helper but does NOT alter UI behavior.

</domain>

<decisions>
## Implementation Decisions

### Locator Resolution

- **D-01:** **`sys_id` is REQUIRED in every `/api/browse` request.** The roadmap goal phrases the locator as "uid (preferred) or sys_id+volume_ie+page (fallback)", but practically every `/api/search` response item already carries `{uid, sys_id, volume_ie, p_num}` together (Phase 77 D-04 — both fields always populated). Requiring `sys_id` always means the handler can call `state.searcher.get_browse_page(sys_id, ...)` directly with no reverse-map lookup. **uid is treated as an authoritative IE-pinning + page-pinning token** that, when present, supersedes the `volume_ie` and `p_num` query params. **Skill workflow stays no-disambiguation** because skill copies the full locator object verbatim from `/api/search`.
- **D-02:** Three accepted page-pinning forms (in priority order), all alongside the required `sys_id`:
  - `uid=IE{N}_P{M}_FL{K}` — parse it. The `IE{N}` component pins `volume_ie`; the `P{M}` component pins `p_num`. When `uid` is supplied, any explicit `volume_ie` / `p_num` / `fl_id` query params are ignored (with a `warnings: ['locator_redundant_fields_ignored']` note if they conflict).
  - `p_num=N&volume_ie=IE{X}` (or `p_num=N` alone for single-IE manuscripts) — pure-coordinate access. `volume_ie` defaults to the manuscript's primary IE (suffix=1) when omitted on a multi-IE manuscript; emit `warnings: ['volume_ie_defaulted: <IE>']` so the skill knows the default fired.
  - `fl_id=FL{K}` — direct folio access via existing `state.searcher.get_browse_page_by_fl(fl_id, sys_id=...)`. Mirrors the internal browse-UI affordance.
- **D-03:** Locator validator (Pydantic `BrowseRequest` model with `extra='forbid'`):
  - Required: `sys_id: str`.
  - Optional, mutually exclusive with priority order: `uid: str | None`, `p_num: int | None` (1-based), `volume_ie: str | None`, `fl_id: str | None`.
  - If NONE of `uid` / `p_num` / `fl_id` is supplied → 400 `invalid_request` "locator missing: provide uid, p_num, or fl_id alongside sys_id".
  - If both `uid` and `p_num` are supplied AND they disagree (after parsing uid) → strip the redundant fields and emit `warnings: ['locator_redundant_fields_ignored: p_num contradicts uid']`. Do NOT error — uid wins.
  - `p_num` must parse to int >= 1 (1-based, per page_indexing convention). Negative or zero → 400 `invalid_request`.
- **D-04:** **Multi-IE without `uid` and without `volume_ie`** → default to the manuscript's primary IE (suffix=1). The page resolves via `get_browse_page(sys_id, p_num, volume_ie=None)` which already auto-detects via `get_volumes_for_sys_id`. Surface `warnings: ['volume_ie_defaulted: <resolved IE>']` so the skill knows a default was applied.
- **D-05:** Page indexing in the response — **echo `p_num` as int + add a top-level string field `"page_indexing": "1-based"`** to every response. The roadmap goal explicitly requires the convention be "explicit in the response itself" (#1).

### Response Shape

- **D-06:** Top-level envelope (flat with namespaced metadata groups). Mirrors Phase 77/78 conventions:
  ```json
  {
    "schema_version": 1,
    "source": "browse",
    "generated_at": "2026-04-29T12:34:56Z",
    "locator": {"uid": "...", "sys_id": "...", "volume_ie": "...", "p_num": 3},
    "page_indexing": "1-based",
    "shelfmark": "T-S 12.123",
    "title": "...",
    "library": {"code": "CUL", "name": "Cambridge University Library"},
    "text": "...",
    "text_source": "pgp_transcription" | "snippet" | "none",
    "text_truncated": false,
    "metadata": {
      "pgp": {...},
      "fjms": {...},
      "nli": {...}
    },
    "image": {"url": "...", "provider": "cambridge", "sources": [...]},
    "warnings": []
  }
  ```
- **D-07:** PGP metadata subset (`metadata.pgp`):
  - `description: str | null`
  - `tags: list[str]`
  - `document_type: str | null`
  - `languages_primary: list[str]` and `languages_secondary: list[str]`
  - `doc_date_original: str | null`, `doc_date_standard: str | null`, `inferred_date_display: str | null`
  - `pgpid: int | null`, `pgp_url: str | null`
  - All fields nullable (some manuscripts have no PGP record). When PGP is unavailable for this `sys_id`, `metadata.pgp` is `null` (not an empty dict) so the skill can branch on presence.
  - **Sources list (editions/translations) and full transcription are NOT in `metadata.pgp`** — page-scoped transcription text comes through the top-level `text` field instead (D-09). Sources list is deferred (see deferred ideas).
- **D-08:** FJMS metadata subset (`metadata.fjms`) — minimal:
  - `source_names: list[str]` (catalog handlist sources)
  - `has_measurements: bool`
  - `has_visual_suggestions: bool`
  - `null` when no FJMS enrichment present for this `sys_id`. Bibliography list is **deferred** (D-deferred); too large for a default response, may surface via opt-in `?include=bibliography` later.
- **D-09:** NLI crossref subset (`metadata.nli`) — physical metadata + current page only:
  - `physical_metadata: {material, num_folio, num_bifolio, size}` — verbatim from `crossref_data.physical_metadata` (already a dict from `shared/nli_crossref_service.py`).
  - `folio: {fl_id, folio_label, thumb_url}` for the active page only. NOT the full `folio_images[]` sequence — that's deferred. Skill needs the active folio for citation; navigating around requires re-calling `/api/browse` with a different `p_num`.
  - `null` when NLI crossref doesn't have this `sys_id`.

### Text Source

- **D-10:** Text source priority — **PGP page-scoped transcription > Tantivy snippet > none**. The handler:
  1. Attempts PGP edition transcription scoped to the page section (mirroring `web/pages/browse_enrichment.py:get_section_for_page` logic).
  2. If unavailable, falls back to the page's snippet from `BrowsePage.text` (Tantivy).
  3. If both unavailable, returns `text: ""` with `text_source: "none"`.
  - `text_source` enum: `"pgp_transcription"`, `"snippet"`, `"none"`.
- **D-11:** Transcription char cap — **4000 characters**, configurable via env var `SEARCH_API_BROWSE_TEXT_CAP`. Truncate at last word boundary ≤ cap; append `…` (single character ellipsis, not three dots). When truncated:
  - `text_truncated: true`
  - `warnings[].append({"code": "transcription_truncated", "message": "transcription text truncated at <CAP> chars"})`
  Otherwise `text_truncated: false`. The cap is applied AFTER PGP page-section scoping — most pages already fit comfortably under 4000 chars.

### Image URLs + Graceful Degrade

- **D-12:** `image.url` is **library-aware** — pick the correct proxy based on `BrowsePage.library_code`:
  - CUL → `/api/cambridge_image/{sys_id}?page={p_num-1}`
  - Manchester → `/api/manchester_image/{sys_id}?page={p_num-1}`
  - JTS → `/api/jts_image/{sys_id}?page={p_num-1}`
  - Oxford → `/api/oxford_image/{shelfmark}/{p_num-1}` (existing route shape; planner verifies the exact signature in Plan 03 against `web/api.py`)
  - Default → `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}`
  - Page indexing on the proxy URL stays **0-based** — that's the existing internal convention (Phase 77 D-04 image URL emission). The response's `page_indexing: "1-based"` documents the field; the proxy URL semantics are server-internal.
- **D-13:** `image.sources[]` is a list of `{url, provider, role}` entries. Always populated (≥1 entry — at minimum the canonical `image.url` repeated):
  - `role: "iiif_proxy"` — our proxy routes (NLI, Cambridge, Manchester, JTS, Oxford) — one per available source.
  - `role: "external_viewer"` — direct library viewer URLs (Cambridge CUDL viewer page, Bodleian viewer page, JTS DPUL catalog page, etc.) extracted from `BrowsePage.external_url`.
  - `role: "companion_folio"` — additional CUDL bifolio companions when `cambridge_images[]` has multiple entries for one `p_num`. Each entry pins which folio it represents (`fl_id`, `folio_label`).
  - The `image.provider` field at the top level reflects which provider `image.url` came from (matches one of the `image.sources[]` entries).
- **D-14:** Graceful image degrade — **`image.url = null` + `warnings[].append({"code": "image_unavailable", "message": "<provider> proxy returned <status>"})`**. Mirrors Phase 78's warnings array convention. Response body still 200; metadata + text still returned. Upstream proxy 5xx, timeout, or empty body all map to this path. `image.sources[]` may still be populated with viable alternates (the skill can retry against a different source).

### Enrichment Pipeline

- **D-15:** Enrichment fan-out — single `asyncio.gather` across:
  1. `state.searcher.get_browse_page(sys_id, p_num=..., volume_ie=..., fl_id=...)` (core; not optional — must succeed for the response to be possible).
  2. PGP fetch (`get_pgp_for_sys_id` + page-section scoping).
  3. FJMS fetch (catalog source_names, measurements/visual-suggestions flags).
  4. NLI crossref fetch (physical_metadata + active-page folio info).
  Each non-core source wrapped in `asyncio.wait_for(fetch, timeout=SEARCH_API_BROWSE_TIMEOUT)` (default **2 seconds**, env-overridable).
- **D-16:** Per-source failure mode:
  - **Core fails** (locator unresolvable, manuscript-page-not-found) → 404 `manuscript_page_not_found` envelope; no further enrichment. The skill must see hard 404 here; it cannot rank against a non-existent page.
  - **Enrichment source times out** → that source's slot is `null`; `warnings[].append({"code": "enrichment_timeout", "source": "<pgp|fjms|nli>"})`. Response body still 200.
  - **Enrichment source raises** → that source's slot is `null`; `warnings[].append({"code": "enrichment_failed", "source": "<source>"})`. Logged via `logger.exception` server-side; never includes traceback in response (Phase 78 D-07 `internal_error` posture). Response body still 200.
- **D-17:** New env vars (two new for Phase 79, both inheriting Phase 78's "read on every request" pattern from D-02 of Phase 78):
  - `SEARCH_API_BROWSE_TIMEOUT` (float seconds, default `2.0`) — per-source enrichment timeout.
  - `SEARCH_API_BROWSE_TEXT_CAP` (int chars, default `4000`) — transcription truncation cap.
  - Documented in CLAUDE.md (Phase 79 owns adding them; Phase 82 makes them canonical in `docs/SEARCH_API.md`).

### Rate Limit Bucket Topology

- **D-18:** **Same `SEARCH_API_RATE_LIMIT` env-var ceiling, separate per-IP buckets per endpoint.** Phase 78's `RateLimiter` is per-IP. Phase 79 instantiates its own `RateLimiter(default_limit=int(os.environ.get('SEARCH_API_RATE_LIMIT', 30)))` instance, distinct from the one `/api/search` uses. A single client doing `search() once + browse() 10 times` does NOT exhaust the search bucket. Each endpoint enforces its own counter against the same env-var ceiling.
- **D-19:** Bucket-key resolution — reuse Phase 78's `_resolve_rate_limit_key(request)` (Concern #1 fix). Trusted-proxy XFF resolution stays consistent across endpoints.

### Module Layout + Statelessness

- **D-20:** **New route registered in existing `web/search_api.py`.** Phase 78 D-18 established this module owns search-helper routes; Phase 79 adds `init_search_api`'s registration of `GET /api/browse` alongside `POST /api/search`. NO new module file. NO modification to `web/api.py`.
- **D-21:** Pydantic models for `/api/browse`:
  - `BrowseRequest(BaseModel)` with `extra='forbid'` and the field rules from D-03.
  - Inline in `web/search_api.py` for v7.10 (mirrors Phase 78 D-19; defer extraction to `web/search_api_models.py` if file grows past ~600 lines).
- **D-22:** Statelessness contract enforcement — same as Phase 78 D-20. The handler MAY use `state.searcher` (singleton SearchEngine) and `state.meta_mgr` (singleton MetadataManager). It MUST NOT touch `state.last_results`, `app.storage.user`, `request.cookies`, or any per-session/refinement state. Verified via `! grep -qE "state\.last_results|state\.current_search_query|app\.storage|request\.cookies"` on Plan 03's output. Two identical requests → byte-identical responses (modulo `generated_at`).
- **D-23:** **Service-layer extraction (planner's call):** browse enrichment logic in `web/pages/browse_enrichment.py` is currently page-coupled (mutates `BrowseState`). Phase 79's handler needs a pure data-returning function `fetch_browse_enrichment(sys_id, p_num, volume_ie) -> BrowseEnrichmentBundle`. Planner decides whether to (a) extract a shared helper to `shared/browse_service.py` consumed by both UI and API, or (b) reimplement the pure-data version inside `web/search_api.py`. **Preference (a)** — keeps single source of truth — but only if the extraction is clean. If it pulls UI dependencies, fall back to (b).

### Test Surface

- **D-24:** Pytest unit tests in `tests/test_browse_api.py` (NEW file — keeps `tests/test_search_api.py` focused on `/api/search`):
  - **Locator round-trip:** Skill workflow — POST `/api/search` returns N results; for each, GET `/api/browse` with the locator object; assert 200 + non-empty metadata. At least one single-IE manuscript and one multi-IE manuscript.
  - **Locator forms:** uid alone, p_num+volume_ie, p_num alone (single-IE), fl_id alone — each resolves correctly.
  - **Locator conflicts:** uid + contradicting p_num → 200 + `warnings: ['locator_redundant_fields_ignored']`. Missing all page-pinning forms → 400 `invalid_request`.
  - **Multi-IE default:** sys_id of a multi-IE manuscript with only `p_num` (no volume_ie) → 200 + `warnings: ['volume_ie_defaulted: <IE>']`.
  - **Manuscript not found:** bogus `sys_id` → 404 `manuscript_page_not_found`.
  - **Image graceful degrade:** mock NLI proxy returning 503 → 200 response with `image.url = null` + `warnings: ['image_unavailable']`.
  - **Transcription truncation:** PGP transcription > 4000 chars → 200 with `text_truncated: true` + `warnings: ['transcription_truncated']`.
  - **Enrichment timeout:** mock PGP service to sleep 5s → 200 with `metadata.pgp = null` + `warnings: ['enrichment_timeout: pgp']`.
  - **Statelessness:** two identical requests produce identical bodies (modulo `generated_at`).
  - **Rate-limit independence:** burst 31 requests on `/api/browse` → 30 succeed + 1 returns 429; verify `/api/search` rate counter is unaffected (separate bucket per D-18).
  - **Error envelope:** every non-2xx response is `{error: {code, message}}` per Phase 78 D-07.
- **D-25:** Legacy-route immutability spot check — extend `tests/test_api_legacy_unchanged.py` (Phase 78 created it) with one additional assertion: `/api/nli_image_by_sysid/<known_sys_id>?page=0` still returns the expected status + content-type. `/api/cambridge_image` likewise. Phase 79 must not alter those proxy routes.

### Locator + Serializer Reuse

- **D-26:** Phase 79 does NOT call `serialize_search_payload`. The browse response shape is *different* from the search response (single page vs ranked list of items). However, Phase 79 reuses the **same envelope conventions** — `schema_version`, `source`, `generated_at`, `locator`, `warnings: []` always present, error envelope shape — implemented as a sibling function `serialize_browse_payload(...)` in `shared/search_serializer.py`. Planner places it next to the existing serializers; both expose a clean `source` field (`'search'` vs `'browse'`).
- **D-27:** Cross-phase integrity — Phase 80 (`/api/parallels`) inherits the SAME locator emission from `/api/search` (Phase 77 D-04) and the SAME hardening shell (Phase 78 D-01..D-13). Phase 79 verifies the locator round-trips end-to-end; Phase 80 then knows it can emit the same shape and `/api/browse` will accept it without per-producer adjustment. The Plan 04 verification step explicitly tests "locator from /api/search → /api/browse → 200 with text + metadata" against multiple manuscripts.

### Claude's Discretion

- **Service-layer extraction shape (D-23)** — planner decides whether `fetch_browse_enrichment` lives in `shared/browse_service.py` (preferred) or inline in `web/search_api.py`. Either is acceptable; the tipping factor is whether the extraction is clean.
- **Whether `image.sources[]` is populated when `image.url` is null** — if the chosen primary proxy fails, can the skill use an alternate? Recommendation: yes, populate alternates anyway; skill can retry. Planner flag: don't degrade the alternate proxies' URLs based on the primary's status — they're independent.
- **Daemon/cleanup hooks** — none. The endpoint is fully request-scoped; no background work survives a request beyond Phase 78's existing PostHog drain thread.
- **`fl_id` validation depth** — accept any non-empty string; leave validity to `get_browse_page_by_fl(fl_id, sys_id=...)`. Don't pre-validate the `FL\d+` pattern; core's resolver already returns None for unknown IDs.
- **Should `/api/browse` honor `Accept: application/json` and reject other Accept types?** No. FastAPI's default `JSONResponse` is content-negotiation-agnostic. Skip the header check; respond JSON unconditionally. Document in `docs/SEARCH_API.md` (Phase 82).
- **Whether to expose a "next" / "prev" hint in the response** — i.e. `navigation: {prev_p_num: 2, next_p_num: 4, total_pages: 50}`. Probably yes — adds <30 bytes and lets the skill walk pages without re-doing math. Planner adds if Plan 03 has slack; flag as nice-to-have.

### Folded Todos

None — no pending todos matched Phase 79 scope.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase / milestone specs
- `.planning/ROADMAP.md` §`Phase 79: /api/browse Drill-Down` — full success criteria #1–#4.
- `.planning/ROADMAP.md` §`Phase 80: /api/parallels` and §`Phase 81: Claude Skill Consumer` — confirm Phase 79's locator-consume contract is what Phases 80/81 depend on.
- `.planning/REQUIREMENTS.md` §`API Endpoints` — **API-03** (this phase), **API-04** (error envelope inherited), **API-05** (locator inherited), **API-06** (statelessness inherited), **API-07** (no parallel filter engine — N/A here, browse has no filters).
- `.planning/REQUIREMENTS.md` §`Hardening` (HARDEN-01..05) — inherited from Phase 78 verbatim; planner verifies wrap_endpoint is reused, not reimplemented.
- `.planning/PROJECT.md` §Architecture — `shared/` service-layer convention; FastAPI mount via NiceGUI.
- `.planning/STATE.md` — v7.10 milestone position, watch list (`/api/*` legacy proxies must remain unchanged).

### Phase 77 lock (locator + envelope conventions)
- `.planning/phases/77-serializer-json-export/77-CONTEXT.md` — D-04 (locator both-fields-always-populated), D-14 (serializer module is single source of truth). Phase 79's `serialize_browse_payload` lives next to the existing serializers.
- `shared/search_serializer.py` — Phase 77 module; Phase 79 adds `serialize_browse_payload` here (D-26).

### Phase 78 lock (hardening shell)
- `.planning/phases/78-api-search-hardening-shell/78-CONTEXT.md` — D-01 (sliding-window RateLimiter), D-06+D-07 (APIError envelope + ERROR_CODES), D-10..D-14 (PostHog server-side), D-18 (init_search_api registrar), D-20 (statelessness contract). Phase 79 inherits ALL of these.
- `web/api_hardening.py` — Phase 78 module; reused: `RateLimiter`, `_resolve_rate_limit_key`, `_is_loopback_request`, `wrap_endpoint`, `_build_envelope_response`, `enforce_mode_gate`, `capture_api_event`, `hash_ip`, `latency_bucket`, `result_count_bucket`. Phase 79 adds NO new helpers here.
- `shared/api_errors.py` — Phase 78 module; reused: `class APIError`, `ERROR_CODES`. Phase 79 may need to extend ERROR_CODES with two new codes (see open questions).
- `web/search_api.py` — Phase 78 module; Phase 79 adds the `GET /api/browse` route + `BrowseRequest` Pydantic model + `serialize_browse_payload` invocation here.

### Existing code (single source of truth)
- `web/services.py:88` `class BrowsePage` — the dataclass returned by `get_browse_page`. Phase 79's response is built from this (text, full_header, fl_id, image_url, total_pages, library_code, library_name, folio_images, cambridge_images, physical_metadata, volumes, volume_ie, etc.).
- `web/services.py:294` `WebDataService.get_browse_page(sys_id, p_num, direction, absolute_index, allow_cross, volume_ie)` — Phase A fast path; Phase 79 calls this as the core fetch.
- `web/services.py:408` `WebDataService.get_browse_page_by_fl(fl_id, sys_id)` — Phase 79 calls this when `fl_id` is the locator form.
- `genizah_core.py:8246` `SearchEngine.get_browse_page(sys_id, p_num, next_prev, absolute_index, allow_cross, volume_ie)` — under-the-hood resolver.
- `genizah_core.py:8343` `SearchEngine.get_browse_page_by_fl(fl_id, sys_id)`.
- `web/pages/browse_enrichment.py:240-340` — the enrichment fan-out pattern Phase 79 mirrors. Especially:
  - `fetch_pgp()` / `get_pgp_for_sys_id` — page-section scoping via `get_section_for_page(content, p_num, sections)`.
  - `fetch_fjms()` — catalog source_names + has_measurements + has_visual_suggestions.
  - `fetch_crossref()` — physical_metadata + folio_images.
  - `fetch_browse_enrichment()` — Cambridge alignment + Oxford part metadata.
- `web/pages/browse_enrichment.py:280` `get_section_for_page(content, p_num, sections)` — page-section text extraction. Phase 79 uses this to scope PGP transcription per D-10.
- `shared/nli_crossref_service.py` — `physical_metadata`, `folio_images`, `cambridge_images`, `cambridge_alignment`, `external_provider` fields. Read-side service; Phase 79's NLI metadata subset comes from here (D-09).
- `shared/fjms_service.py` — `get_catalog_source_names(sys_id)`, `has_measurements(sys_id)`, `has_visual_suggestions(sys_id)`. Phase 79's FJMS metadata subset reads these (D-08).
- `shared/document_service.py` — PGP fetch helpers; underlying `pgp.db` queries. Phase 79's PGP metadata + transcription read these (D-07 + D-10).
- `web/api.py:573` `nli_image_by_sysid(sys_id, page, width, suffix)` — image proxy referenced by `image.url` for non-CUL/Manchester/JTS/Oxford manuscripts. Phase 79 does NOT modify; it only emits URLs.
- `web/api.py:611` `cambridge_image(sys_id, page, width)` — CUL proxy.
- `web/api.py:776` `manchester_image(sys_id, page, width)` — Manchester proxy.
- `web/api.py:822` `jts_image(...)` — JTS DPUL proxy. (Planner verifies exact signature.)
- `web/api.py:???` `oxford_image*` — Oxford direct image. (Planner verifies exact signature in Plan 03.)
- `web/main.py:166-169` — `init_api_routes()` then `init_search_api()`. Phase 79 needs no new wiring here; `init_search_api` registers the new route automatically when this phase lands.

### Cross-phase obligations
- Phase 80 (`/api/parallels`) inherits Phase 79's locator-round-trip contract. Phase 79 D-27 spells out the integrity test.
- Phase 81 (skill consumer) is the FIRST real consumer of the search → browse vertical. Phase 79's response shape decisions IS the contract that determines what the skill can do well in Phase 81.
- Phase 82 (`docs/SEARCH_API.md`) captures the as-shipped contract. Phase 79 owns env-var lines in `CLAUDE.md` (`SEARCH_API_BROWSE_TIMEOUT`, `SEARCH_API_BROWSE_TEXT_CAP`); Phase 82 promotes them into the canonical doc.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `state.searcher.get_browse_page(sys_id, p_num=N, volume_ie=...)` — exact signature Phase 79 hits for the core fetch. Returns rich dict with text, full_header, total_pages, etc.
- `state.searcher.get_browse_page_by_fl(fl_id, sys_id=None)` — direct folio access. Phase 79's `fl_id` locator form calls this.
- `state.meta_mgr.parse_full_id_components(header)` — extracts `{sys_id, ie_id, p_num, fl_id}` from page header. Phase 79 uses this to enrich the locator echo if `volume_ie` was inferred from `uid`.
- `state.meta_mgr.get_meta_for_id(sys_id)`, `get_library_for_id(sys_id)` — shelfmark/title/library_code lookups.
- `web/pages/browse_enrichment.py` enrichment fan-out (PGP + FJMS + crossref + Cambridge align) — pattern Phase 79 mirrors. Service-layer extraction is preferred (D-23) but optional.
- `shared/nli_crossref_service.py: NliCrossrefService` — already a thread-safe service-layer module. Phase 79 reads from it directly.
- `shared/fjms_service.py: FjmsService` — already a thread-safe service-layer module (Phase 78 added new helpers here for filter validation). Phase 79 reads catalog data from it.
- `shared/document_service.py: DocumentService` — PGP source. Phase 79 reads transcription + metadata from it.
- `web/api_hardening.py` (Phase 78) — RateLimiter, wrap_endpoint, capture_api_event. Phase 79 instantiates a second RateLimiter with the same env-var ceiling and reuses everything else.
- `shared/search_serializer.py: serialize_search_payload` — sibling function `serialize_browse_payload` lands here (D-26).

### Established Patterns
- Routes registered via `init_search_api(app_override=None)` (Phase 78 D-18). Phase 79 adds `@target_app.get('/api/browse')` to the same registrar function.
- Per-endpoint `wrap_endpoint(...)` decorator owns try/except/finally + envelope + PostHog (Phase 78 R2-#6). Phase 79's handler inherits this directly.
- `asyncio.gather` + `asyncio.wait_for(timeout)` for parallel I/O with per-source deadlines.
- Library-aware image URL emission already exists in `shared/search_serializer._build_image_url` for the NLI default; Phase 79 generalizes it via `_build_browse_image_url(sys_id, p_num, library_code)` returning `(url, provider)`.
- Service-layer modules in `shared/` are stateless + thread-safe + read-only.

### Integration Points
- `web/main.py:166-169` — already wires `init_search_api()` (Phase 78). Phase 79 needs zero changes here; the new route auto-registers.
- `CLAUDE.md` Environment Variables — Phase 79 adds two new lines (`SEARCH_API_BROWSE_TIMEOUT`, `SEARCH_API_BROWSE_TEXT_CAP`) below Phase 78's four (`SEARCH_API_MODE`, `SEARCH_API_RATE_LIMIT`, `POSTHOG_IP_SALT`, `SEARCH_API_POSTHOG_SAMPLE_N`).
- `requirements.txt` — no new direct deps. `asyncio` is stdlib; everything else is already pulled by Phase 78.
- `pyproject.toml` — no marker changes.
- `.github/workflows/ci.yml` — no new CI job. New tests run under the existing default `tests` job.

### Test Surface
- `pytest` with `from fastapi.testclient import TestClient` and a bare `FastAPI()` instance. Pattern matches `tests/test_search_api.py` (Phase 78). Construct fresh app, call `init_search_api(app_override=fresh_app)`, hit `/api/browse` via TestClient.
- `tests/test_browse_api.py` (NEW) — Phase 79's primary test file (D-24).
- `tests/test_api_legacy_unchanged.py` (Phase 78) — extended with one image-proxy assertion (D-25).
- `tests/test_search_api.py` (Phase 78) — must remain GREEN (regression check).
- Mock `state.searcher.get_browse_page` for fast tests; integration test against real searcher for the locator round-trip case (uses cached test data fixture if available, or skips if not).

</code_context>

<specifics>
## Specific Ideas

- **The skill workflow drives shape decisions.** The Claude skill (Phase 81) calls `/api/search` once, then `/api/browse` 5–10 times per query to ground its rankings. So `/api/browse` favors completeness over latency: 2s timeout per source is acceptable; 200ms is too aggressive when PGP can occasionally be slow on a cold cache. Decision D-15 (block on full enrichment with timeout) reflects this.
- **`text` is the most-cited field.** When the skill writes "this manuscript discusses X, see <pgp_url>", the cited evidence comes from `text`. The 4000-char cap (D-11) is tight enough to limit one-page bandwidth but generous enough that a single response carries one full transcription page including the surrounding context.
- **`metadata.pgp` is highest-signal.** PGP description + tags + dates + languages are what the skill uses to rank candidates. FJMS catalog source_names are scholarly attribution (also high-signal but lower density). NLI physical_metadata is interesting for citation but not ranking-decisive. D-07 / D-08 / D-09 reflect this priority.
- **Multi-IE manuscripts are a real failure mode.** Phase 78's locator emission was specifically scoped to handle these (sys_id + volume_ie + p_num — sys_id alone is insufficient). Phase 79 closes the loop: uid IS the IE-pinning token; sys_id-only requests for multi-IE manuscripts get a *defaulted* volume_ie + a warning so the skill knows the default fired (D-04).
- **`sys_id` always required is a deliberate simplification.** The roadmap goal phrases the locator as "uid (preferred)" but every real consumer (search response item, skill extraction) carries `sys_id` next to `uid`. Requiring `sys_id` skips a reverse-map lookup and keeps the handler thin (D-01).
- **Image graceful degrade is a hard requirement.** Goal #3 explicitly says "degrade gracefully when NLI is unavailable rather than failing the whole response." The skill must be able to read text + metadata for a manuscript whose NLI image is currently 503'd (or whose Cambridge IIIF manifest is stale). D-14 is non-negotiable.
- **Codex external review pending.** Per Phase 78 precedent, the user wants infra/contract decisions in this phase reviewed by an external AI (Codex CLI) before locking. Open questions list at the bottom of this file is the input for that review.

</specifics>

<deferred>
## Deferred Ideas

- **`?include=bibliography`** — opt-in flag to expose the full FJMS bibliography list per manuscript. Skipped in v7.10 default response because lists can be 5–30 entries and bloat the payload. Add when a real consumer requests it.
- **`?include=full_transcription`** — opt-in flag to return the entire PGP transcription (not just the page section). Useful for scholarly tools that want the whole document; deferred until a consumer needs it.
- **`?include=folios`** — opt-in flag to return the full `folio_images[]` sequence. Skill currently navigates pages by re-calling `/api/browse` with different `p_num`, so this is unnecessary. Add if a consumer wants to render a thumbnail grid.
- **`?include=sources`** — opt-in flag to expose PGP `sources` list (editions/translations metadata). Useful for citation chaining; defer until a consumer uses it.
- **`navigation` hint in default response** — `{prev_p_num, next_p_num, total_pages}` adds <30 bytes and helps the skill walk pages. Plan 03 may add as a nice-to-have if it has slack; otherwise defer.
- **HEAD method support on `/api/browse`** — for cheap existence checks. Skip in v7.10; GET is sufficient.
- **`Accept-Language` honoring** — return Hebrew-language descriptions when the client requests `he`. UI does this; API stays English-default. Defer until needed.
- **CORS** — same as Phase 78. Internal helper; no browser consumer in v7.10.
- **`/api/browse/manuscript/{sys_id}` collection endpoint** — return a manuscript's full metadata + folio list without picking a single page. Out of scope: skill has no use case yet.
- **Cache-Control headers on `/api/browse` responses** — content rarely changes; could be cacheable with sensible TTL. Defer to performance pass.

</deferred>

<open_questions_for_codex_review>
## Open Questions for Codex External Review

Per Phase 78 precedent, the user requested external review of infra/contract decisions before locking. The decisions above are **provisional**; Codex review may revise any of them. Run via:

```
echo "<question>" | codex --model gpt-5
```

### Q1 — Locator design (D-01, D-02, D-03)

> Phase 79 of GenizahSearch builds `GET /api/browse` consuming a locator emitted by `/api/search` (Phase 78). Locator shape: `{uid, sys_id, volume_ie, p_num}` where uid = `IE{N}_P{M}_FL{K}` and is globally unique within NLI's numbering. Three forms:
> 1. uid alone (preferred per roadmap goal)
> 2. sys_id + volume_ie + p_num (fallback)
> 3. fl_id (third form for direct folio access)
>
> The handler decision is to **require sys_id always** — the search response always carries it, and requiring it skips a reverse-map lookup. Then uid (when present) supersedes volume_ie/p_num/fl_id. fl_id is a third optional field. This sidesteps adding a `get_browse_page_by_uid()` method to genizah_core.py.
>
> Is this the right call? Specifically: does requiring sys_id violate the spirit of "preferred uid" in the roadmap? Should we instead bite the bullet and add `get_browse_page_by_uid()` for a cleaner contract?

### Q2 — Enrichment timeout policy (D-15, D-16, D-17)

> Phase 79's handler does asyncio.gather across 4 sources (core BrowsePage, PGP, FJMS, NLI crossref). Each non-core source is wrapped in `asyncio.wait_for(timeout=2s)`, env-overridable via `SEARCH_API_BROWSE_TIMEOUT`. On timeout: that source returns null + warnings entry; response is still 200. Core failure is hard 404.
>
> Is 2s the right default for a Claude skill workflow that calls /api/browse 5–10x per query? Should the timeout be per-source or shared (one budget for all 3 enrichment sources)? Should the core fetch also have a timeout (currently no — assumed always-fast since it's Tantivy + csv_bank)?

### Q3 — Rate limit topology (D-18)

> /api/browse uses the same `SEARCH_API_RATE_LIMIT` env-var ceiling as /api/search but a separate per-IP bucket. So a client doing search-once + browse-N-times against /api/browse doesn't exhaust the search bucket. Each endpoint enforces its own counter.
>
> Is this right? Or should rate limits be shared across all 3 search-helper endpoints (search + browse + parallels) as one bucket? Trade-off: separate buckets favor the realistic skill workflow (search→browse N times); shared bucket gives uniform DoS protection. The current Phase 78 RateLimiter is per-IP. Reasoning is fine either way; want a recommendation.

### Q4 — Image URL strategy (D-12, D-13, D-14)

> Response shape: `image: {url, provider, sources: [{url, provider, role}]}` where role ∈ {iiif_proxy, external_viewer, companion_folio}. Library-aware url picker: CUL → /api/cambridge_image, Manchester → /api/manchester_image, JTS → /api/jts_image, Oxford → /api/oxford_image, default → /api/nli_image_by_sysid. CUDL bifolios (multiple folios per p_num) surface as role: 'companion_folio' entries. Graceful degrade: image.url=null + warnings: ['image_unavailable']; alternates in sources[] still populated.
>
> Is the role-tagged sources[] worth the complexity? Or should we keep image.url single-string (matches /api/search emission) and require the skill to call additional /api/browse with different parameters if it wants alternates?

### Q5 — Response envelope conventions (D-06, D-26)

> Phase 79's response shape: flat envelope `{schema_version, source: 'browse', generated_at, locator, page_indexing, shelfmark, title, library, text, text_source, text_truncated, metadata: {pgp, fjms, nli}, image: {...}, warnings: []}`. The serializer lives in `shared/search_serializer.py` next to `serialize_search_payload`, exposed as `serialize_browse_payload`.
>
> Is `metadata: {pgp, fjms, nli}` (namespaced groups) the right grouping? Alternative: hoist all fields to top level (`description`, `tags`, `source_names`, `physical_material` directly under the root) — less nesting but loses provenance. Or: keep three top-level objects (`pgp_metadata`, `fjms_metadata`, `nli_metadata`) — same as namespaced but flatter. Which is best for a Claude-consuming skill?

### Q6 — Text length cap (D-11)

> Transcription text is capped at 4000 chars (env-overridable via SEARCH_API_BROWSE_TEXT_CAP). Truncation point: last word boundary ≤ cap. Indication: `text_truncated: true` + `warnings: ['transcription_truncated']`. PGP page-section scoping is applied first; the cap is belt-and-suspenders.
>
> Is 4000 chars the right default? Hebrew/Arabic text averages ~5 chars per word, so 4000 chars ≈ 800 words ≈ 1–2 typical Genizah folios. Skills typically need ~500 chars for grounding citations. Is the default too generous? Too tight? Should there be a way for the skill to request more (?text_cap=N) up to a hard ceiling (?cap=20000 max)?

</open_questions_for_codex_review>

---

*Phase: 79-api-browse-drill-down*
*Context gathered: 2026-04-29*
*External review via Codex CLI: pending — see open questions above. Recommendations applied during plan-phase will produce the locked CONTEXT.md revision.*

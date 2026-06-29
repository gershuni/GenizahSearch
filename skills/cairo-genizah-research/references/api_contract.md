# GenizahSearch API contract (v7.10) — for skill debugging

Locked by Phases 77, 78, 79, 80, 81A. The skill MUST match these shapes exactly.
Load this file via `cat references/api_contract.md` only when debugging an
envelope-shape mismatch.

## Envelope (all endpoints)

Every successful response carries:
- `schema_version: 1` (integer; bump on breaking changes)
- `source: "search" | "browse" | "parallels"`
- `generated_at: ISO-8601 string`
- `request: {...echoed request fields...}` (Phase 81A D-04)
- `count: int` (current page) / `total: int` (full)
- `warnings: [{code, message, ...}]` (top-level — never inside results)
- `results: [...]` (shape varies per endpoint)

Error envelope (any endpoint):

```json
{"error": {"code": "<error_code>", "message": "<human readable>"}}
```

HTTP 429 carries `Retry-After: <seconds>` header.

## POST /api/search request

```json
{
  "search_mode": "exact" | "variants" | "responsa" | "title" | "shelfmark" | "fuzzy",
  "query": "<1-1000 chars>",
  "gap": 0,
  "limit": 10,
  "filters": {"library": ["CUL", "JTS"], "domains": ["Liturgy"]},
  "responsa_options": {"variants": false, "ja": false, "flex_spacing": false, "bidirectional": false}
}
```

Notes:
- Phase 81A REPLACED the old `mode` field. `mode` is hard-rejected with
  `invalid_request` ("unknown field 'mode' — use search_mode instead").
- `search_mode` enum has **6 values** (Phase 81A D-09 dropped `regex`):
  `exact | variants | responsa | title | shelfmark | fuzzy`. `fuzzy` is the
  approximate / maximum-variant tier (slowest; bounded by `SEARCH_API_FUZZY_TIMEOUT` ~300s,
  NOT the 30s interactive baseline). Expect long runtime for multi-word fuzzy queries.
- `responsa_options` only with `search_mode: "responsa"`; otherwise 400 `invalid_combination`.
- `limit` ceiling: **100** for non-fuzzy modes (Phase 81A D-05 unchanged). **500** (default,
  configurable via `SEARCH_API_FUZZY_MAX_LIMIT`, max 2000) for `fuzzy` mode — set a higher
  limit when recall is important (e.g. agent searching for a rare name). Fuzzy with no explicit
  limit widens to 250 automatically.
- `regex_pattern_too_long` error code is NOT in v7.10 (deferred with regex mode).
- `filters` keys (all optional; `extra='forbid'` rejects any other key with
  `invalid_request`): `library` (list of library codes, e.g. `["CUL","JTS","Oxford"]`),
  `domains`, `authors`, `works`, `materials` (all lists), `date_from`, `date_to` (ints).
  Every categorical value is validated server-side — an unknown value (incl. an unknown
  library code) returns 400 `unresolvable_filter_value`. **`library` is an inclusion
  filter applied server-side and intersected with the other filters BEFORE the result
  cap** (SEED-026), so it is more complete than filtering the returned page client-side.
  The `search.py` script also accepts a convenience `--library CUL,JTS` flag.

## POST /api/search response

Each `results[i]` item:

```json
{
  "uid": "<UID>",
  "locator": {"sys_id": "...", "volume_ie": "...", "p_num": 1, "fl_id": "..."},
  "score": 0.8731,
  "shelfmark": "T-S 12.123",
  "title": "...",
  "snippet": "...highlighted text...",
  "excerpt": "...short text...",
  "metadata": {"library": "CUL", "library_name": "...", "domains": [...], "dating": "..."},
  "image_url": "/api/nli_image_by_sysid/<sys_id>?page=<p_num-1>" or null
}
```

Response envelope also includes `request` echo block (Phase 81A D-04):

```json
{
  "request": {
    "search_mode": "exact",
    "responsa_options": null,
    "responsa_options_effective": null,
    "gap": 0,
    "limit": 10,
    "limit_effective": 10,
    "filters": {}
  }
}
```

## GET /api/browse

Query params (one of):
- `?uid=<UID>` (preferred)
- `?sys_id=<S>&p_num=<N>&volume_ie=<IE>` (volume_ie optional)
- `?fl_id=<F>`
- Optional `?text_cap=<100..10000>`

Response:

```json
{
  "schema_version": 1,
  "source": "browse",
  "generated_at": "...",
  "locator": {"uid": "...", "sys_id": "...", "volume_ie": "...", "p_num": 1, "fl_id": "..."},
  "shelfmark": "...",
  "title": "...",
  "library_code": "CUL",
  "library_name": "...",
  "text": "...",
  "text_source": "pgp_transcription" | "snippet" | "none",
  "text_truncated": false,
  "metadata": {"pgp": {...} | null, "fjms": {...} | null, "nli": {...} | null},
  "image": {"url": "..." | null, "provider": "..." | null, "sources": [...]},
  "warnings": []
}
```

**CRITICAL — text_source enum (R2 mapping):** REQUIREMENTS.md SKILL-04 says
`!= 'full'`; the locked enum is `pgp_transcription | snippet | none`. The skill
treats `pgp_transcription` as the equivalent-of-full case. See SKILL.md
"R2 mapping" section.

## POST /api/parallels request

```json
{
  "text": "<1-20000 chars>",
  "chunk_size": 5,
  "mode": "exact" | "variants" | "fuzzy",
  "max_freq": null,
  "boundary_mode": "full" | "boundary" | "combined" | null,
  "filters": {}
}
```

Note: parallels uses `mode` NOT `search_mode` (Phase 81A D-07 kept name as-is).

Response: same envelope; `results` are groups (per sys_id) with `aggregate_score`,
`matches: [{chunk_index, score, source_chunk_text, manuscript_snippet}]`. Top-level
also has `filtered: [...]` (always present, possibly empty per Phase 80 D-04).

## Error code catalogue

See `shared/api_errors.py` (in the GenizahSearch repo) for the canonical list.
Common codes the skill encounters:

- `rate_limited` (HTTP 429 + Retry-After header)
- `heavy_search_busy` (HTTP 503 + Retry-After: 5 — heavy-mode concurrency budget exhausted; retry shortly)
- `core_timeout` (HTTP 504; per-mode ceiling exceeded — exact/title/shelfmark/responsa: 30s, variants: 60s, fuzzy: 300s, parallels: 300s)
- `manuscript_page_not_found` (HTTP 404)
- `locator_conflict` (HTTP 400)
- `invalid_request` (HTTP 400; e.g. unknown field, including legacy `mode`)
- `invalid_combination` (HTTP 400; e.g. responsa_options with non-responsa)
- `limit_too_high` (HTTP 400; non-fuzzy `limit` > 100, or fuzzy `limit` > SEARCH_API_FUZZY_MAX_LIMIT. Note: a non-fuzzy limit in 101..2000 now returns this code — it was `invalid_request` previously; both are HTTP 400 rejections)
- `invalid_filter_value` (HTTP 400; unknown filter token)
- `filter_vocabulary_unavailable` (HTTP 503; FJMS sidecar misloaded)
- `query_required`, `query_too_long`
- `composition_required`, `composition_too_long`

## Rate limits

Per-endpoint independent buckets, server enforces 120 rpm per IP per bucket
(Phase 78/79/80 HARDEN-01, D-05; raised 30->120 in 2026-06). Skill
self-throttles to 96 rpm per bucket (24 rpm headroom; SKILL-06).

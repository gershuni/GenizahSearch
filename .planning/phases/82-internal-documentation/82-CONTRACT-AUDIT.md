# 82-CONTRACT-AUDIT — v7.10 Search-Helper API Contract Audit

> **Internal scratch doc.** Sole consumer is Plan 02 (writes `docs/SEARCH_API.md` from this sheet). Cite line numbers from live source; terse > prose. Source-of-truth is `web/search_api.py` at HEAD of master-main as of phase 82 entry.

---

## 1. Endpoints Inventory

| Endpoint | HTTP | Path | Handler symbol | Rate-limiter instance | Source |
| --- | --- | --- | --- | --- | --- |
| search | POST | `/api/search` | `search_endpoint` | `_rate_limiter` | web/search_api.py:L438-L772 |
| browse | GET | `/api/browse` | `browse_endpoint` | `_browse_rate_limiter` | web/search_api.py:L774-L896 |
| parallels | POST | `/api/parallels` | `parallels_endpoint` | `_parallels_rate_limiter` | web/search_api.py:L898-L1050 |

Module-level rate-limiter declarations: web/search_api.py:L64 (search), L71 (browse), L78 (parallels). All three constructed with `default_limit=30` but read `SEARCH_API_RATE_LIMIT` env on every `check()` call (RateLimiter._current_limit() — ceiling is shared across the three buckets but each bucket counts independently per Phase 80 D-05 and Phase 79 D-18).

Source: web/search_api.py:L60-L78, L438, L774, L898.

---

## 2. POST /api/search — Request

`SearchRequest` Pydantic model: `model_config = ConfigDict(extra='forbid')` — unknown fields produce 400 `invalid_request` envelopes. Source: web/search_api.py:L128-L171.

| Field | Type | Constraint | Default | Source |
| --- | --- | --- | --- | --- |
| `query` | str | 1..1000 chars (`QUERY_LENGTH_CAP=1000`, post-strip empty → `query_required`) | required | web/search_api.py:L144, L175, L543-L556 |
| `search_mode` | Literal | enum: `exact`, `variants`, `responsa`, `title`, `shelfmark` (`regex` intentionally NOT in the enum per 81A D-09; deferred to v7.11) | required | web/search_api.py:L145 |
| `responsa_options` | Optional[ResponsaOptions] | nullable; valid only when `search_mode='responsa'` (cross-field validator `_check_responsa_options_coupling`) | None | web/search_api.py:L146, L151-L160 |
| `gap` | int | must be 0 when `search_mode in {'title','shelfmark'}` (cross-field validator `_check_gap_metadata_coupling`) | 0 | web/search_api.py:L147, L162-L171 |
| `limit` | int | `Field(ge=1, le=100)` (`MAX_LIMIT=100`, lowered from 200 per 81A D-06) | 50 | web/search_api.py:L148, L177 |
| `filters` | Optional[FiltersModel] | nullable; sub-fields all Optional (see FiltersModel below) | None | web/search_api.py:L149 |

### 2.1 ResponsaOptions sub-model

`ResponsaOptions` Pydantic model: `model_config = ConfigDict(extra='forbid')`. Source: web/search_api.py:L112-L125.

| Field | Type | Default |
| --- | --- | --- |
| `variants` | bool | False |
| `ja` | bool | False |
| `flex_spacing` | bool | False |
| `bidirectional` | bool | False |

Field names mirror desktop UI checkboxes (genizah_app.py:L15788-L15797 per the docstring at search_api.py:L112-L114). Internal `variant_mode` is derived server-side: `'variants' if opts.variants else 'exact'` (search_api.py:L609). Extended/maximum variant tiers, `variant_mode`, or any other field name are rejected per D-03.

### 2.2 FiltersModel sub-model

`model_config = ConfigDict(extra='forbid')`. Source: web/search_api.py:L100-L109.

| Field | Type | Default |
| --- | --- | --- |
| `domains` | Optional[List[str]] | None |
| `authors` | Optional[List[str]] | None |
| `works` | Optional[List[str]] | None |
| `materials` | Optional[List[str]] | None |
| `date_from` | Optional[int] | None |
| `date_to` | Optional[int] | None |

### 2.3 Cross-field rejection matrix

| Reject condition | Code | HTTP | Source |
| --- | --- | --- | --- |
| `responsa_options is not None` AND `search_mode != 'responsa'` | invalid_combination | 400 | web/search_api.py:L151-L160 |
| `gap != 0` AND `search_mode in {'title','shelfmark'}` | invalid_combination | 400 | web/search_api.py:L162-L171 |
| `responsa_options + regex` | invalid_combination | 400 | dead code in v7.10 (regex isn't an enum value, structurally rejected first) — documented per 81A-01 SUMMARY |
| client sends Phase 78 old shape (top-level `mode`) | invalid_request | 400 with explicit hint `"unknown field 'mode' — use search_mode instead"` | web/search_api.py:L503-L509 (81A D-13) |

Source: web/search_api.py:L128-L171, L503-L509.

---

## 3. POST /api/search — Response

Envelope skeleton (canonical shape per skills/cairo-genizah-research/references/api_contract.md and Phase 77 serializer `shared/search_serializer.serialize_search_payload`):

```
{
  schema_version: "1.0",
  source: "search",
  generated_at: ISO-8601,
  count: int,
  total: int,
  warnings: [...],
  results: [<item>...],
  request: {<7-key echo>}
}
```

Per-item shape:

```
{
  uid: "IE{N}_P{M}_FL{K}" or null,
  locator: { sys_id, volume_ie, p_num, fl_id },
  score: float,
  shelfmark: str,
  title: str,
  snippet: str,            // pre-snippet
  excerpt: str,            // post-snippet
  metadata: {
    library: str,
    library_name: str,
    domains: [str],
    dating: str
  },
  image_url: str or null
}
```

### 3.1 Seven-key request echo (81A-02)

Built at web/search_api.py:L684-L692 and passed to serializer via `request_echo=` kwarg (L705).

| Echo key | Source | Notes |
| --- | --- | --- |
| `search_mode` | `req.search_mode` | echoed VERBATIM (never silently downgraded — 81A D-04) |
| `responsa_options` | `(req.responsa_options or ResponsaOptions()).model_dump()` for responsa mode, else None | what the CLIENT sent |
| `responsa_options_effective` | cascade-meta thread-local (`_consume_last_responsa_downgrade_meta`); else mirrors `responsa_options` | reflects cascade outcome — e.g. request `ja=true` + cascade-disable → `effective.ja=false`. Both None for non-Responsa modes (D-05). |
| `gap` | `req.gap` | unmodified |
| `limit` | `req.limit` | unmodified |
| `limit_effective` | `min(req.limit, MAX_LIMIT)` | post-cap value actually applied |
| `filters` | `filters_dict` (FiltersModel.model_dump(exclude_none=True)) or None | post-validation snapshot |

The cascade case is the load-bearing nuance: when responsa cascade fires, `responsa_options_effective` differs from `responsa_options`. Source: web/search_api.py:L640-L692, shared/search_serializer.py (per Phase 77 D-14 SOLE producer of envelope shape).

---

## 4. GET /api/browse — Request

`BrowseRequest` Pydantic model: `model_config = ConfigDict(extra='forbid')`. `sys_id` is REQUIRED (Phase 79 D-01). Constructed inside the handler from `dict(request.query_params)` because FastAPI does not auto-bind GET query params to a Pydantic model when the handler takes Request directly. Source: web/search_api.py:L210-L226, L800-L815.

### 4.1 Locator resolution paths (Phase 79 D-03)

At least one of (`uid`, `p_num`, `fl_id`) must be supplied alongside `sys_id`. Three resolution paths:

| Path | Required fields | Effective fields derived from | Source |
| --- | --- | --- | --- |
| uid alone | `sys_id`, `uid` | parsed `uid` (regex `^(IE\d+)_(P\d+)_(FL\d+)$`) → `volume_ie`, `p_num`, `fl_id` | web/search_api.py:L273-L295, L336-L374 |
| sys_id + p_num + volume_ie | `sys_id`, `p_num` (≥1), optional `volume_ie` | mirrors request fields verbatim | web/search_api.py:L376-L384 |
| sys_id + fl_id | `sys_id`, `fl_id` | mirrors request fields verbatim | web/search_api.py:L376-L384 |

### 4.2 Field reference

| Field | Type | Constraint | Default | Source |
| --- | --- | --- | --- | --- |
| `sys_id` | str | required | — | web/search_api.py:L220 |
| `uid` | Optional[str] | format `IE{N}_P{M}_FL{K}` (else 400 `locator_conflict` "uid is malformed") | None | web/search_api.py:L221, L336-L343 |
| `p_num` | Optional[int] | ≥1 | None | web/search_api.py:L222, L319-L324 |
| `volume_ie` | Optional[str] | — | None | web/search_api.py:L223 |
| `fl_id` | Optional[str] | — | None | web/search_api.py:L224 |
| `text_cap` | Optional[int] | `[MIN_BROWSE_TEXT_CAP=100, MAX_BROWSE_TEXT_CAP=10000]`; `?text_cap=` > env > default | None (env `SEARCH_API_BROWSE_TEXT_CAP` else `DEFAULT_BROWSE_TEXT_CAP=4000`) | web/search_api.py:L225, L194-L196, L325-L333, L387-L400 |

### 4.3 `locator_conflict` raise conditions

Raised at HTTP 400 (web/search_api.py:L338-L365):

- `uid` is malformed (regex non-match)
- `uid` parsed `volume_ie` disagrees with request `volume_ie`
- `uid` parsed `p_num` disagrees with request `p_num`
- `uid` parsed `fl_id` disagrees with request `fl_id`

### 4.4 Post-resolution uid verification (Phase 79 D-03b)

After `fetch_browse_bundle` resolves the page, if the request supplied `uid`, the handler compares `bundle.page.uid` to the original `loc.requested_uid`. Mismatch → 404 `manuscript_page_not_found` ("uid resolved to different page; check sys_id + uid pair"). Catches the case where sys_id from manuscript A is paired with uid from manuscript B. Source: web/search_api.py:L856-L864.

Source: web/search_api.py:L210-L400, L774-L896.

---

## 5. GET /api/browse — Response

Envelope shape (locked per 79-03 SUMMARY, produced by `shared/search_serializer.serialize_browse_payload`):

```
{
  schema_version: "1.0",
  source: "browse",
  generated_at: ISO-8601,
  locator: { uid, sys_id, volume_ie, p_num, fl_id },
  shelfmark: str,
  title: str,
  library_code: str,
  library_name: str,
  text: str,                 // capped at effective text_cap chars
  text_source: enum,         // "pgp_transcription" | "snippet" | "none" (Phase 79 D-10)
  text_truncated: bool,
  metadata: {
    pgp:  { ... } | null,    // null on per-source enrichment failure
    fjms: { ... } | null,
    nli:  { ... } | null
  },
  image: {
    url: str or null,
    provider: str or null,
    sources: [str]
  },
  warnings: [...]
}
```

`text_source` enum (Phase 79 D-10):
- `pgp_transcription` — primary path; `text` is from `document_service.fetch_pgp_transcription`
- `snippet` — fallback; `text` is whatever browse_service synthesized
- `none` — no transcription resolvable; `text` is empty

### 5.1 Browse-emitted warnings

| Warning shape | When emitted | Source |
| --- | --- | --- |
| `{code: "volume_ie_defaulted", volume_ie: "IE..."}` | sys_id-only request resolved against a multi-IE manuscript and server auto-picked default volume | web/search_api.py:L867-L877 |
| `enrichment_timeout` | per-source PGP/FJMS/NLI fetch hit `SEARCH_API_BROWSE_TIMEOUT` (default 1.0s) | shared/browse_service.py (fetch_browse_bundle) |
| `enrichment_failed` | per-source PGP/FJMS/NLI fetch raised | shared/browse_service.py (fetch_browse_bundle) |

R-PR-01 / D-14: image URLs are best-effort. The server does NOT probe upstream IIIF availability before emitting them — clients must tolerate dead `image.url` values.

Source: web/search_api.py:L774-L896, shared/search_serializer.serialize_browse_payload, .planning/phases/79-api-browse-drill-down/79-03-SUMMARY.md.

---

## 6. POST /api/parallels — Request

`ParallelsRequest` Pydantic model: `model_config = ConfigDict(extra='forbid')`. Source: web/search_api.py:L228-L253.

| Field | Type | Constraint | Default | Source |
| --- | --- | --- | --- | --- |
| `text` | str | post-strip 1..20000 chars (`COMPOSITION_LENGTH_CAP=20000`); empty → `composition_required`, over cap → `composition_too_long` | required | web/search_api.py:L248, L202, L949-L962 |
| `chunk_size` | int | `Field(ge=2, le=20)` | 5 | web/search_api.py:L249 |
| `mode` | Literal | enum: `exact`, `variants`, `fuzzy` (Lab Engine path OUT OF SCOPE for v7.10 per Phase 80 D-02) | `'exact'` | web/search_api.py:L250 |
| `max_freq` | Optional[float] | None disables high-freq filtering (all hits in `results[]`, `filtered: []`) | None | web/search_api.py:L251 |
| `boundary_mode` | Literal | enum: `full`, `boundary`, `combined` (only boundary knob exposed in v7.10 — Phase 80 D-03) | `'full'` | web/search_api.py:L252 |
| `filters` | Optional[FiltersModel] | reuses Phase 78 FiltersModel verbatim | None | web/search_api.py:L253 |

### 6.1 Naming inconsistency: `mode` vs `search_mode`

`/api/parallels.mode` is INTENTIONALLY named `mode`, NOT `search_mode`. The other search-helper endpoint that ships in v7.10, `/api/search`, uses `search_mode` (Phase 81A D-01). The parallels field name was DELIBERATELY preserved per Phase 81A **D-07** ("rename deferred to v7.11"). The two fields are also semantically disjoint:

- `/api/search.search_mode` enum: `exact | variants | responsa | title | shelfmark`
- `/api/parallels.mode` enum: `exact | variants | fuzzy`

A skill consumer must use the correct field name per endpoint and must not assume the enum values overlap (they share `exact` and `variants` only). Source: web/search_api.py:L250 + .planning/phases/81A-api-contract-expansion/81A-02-SUMMARY.md (D-07).

---

## 7. POST /api/parallels — Response

Envelope produced by `shared/search_serializer.serialize_parallels_payload` (Phase 77 D-14 — SOLE producer of envelope shape).

```
{
  schema_version: "1.0",
  source: "parallels",
  generated_at: ISO-8601,
  count: int,                // len(main_results)
  warnings: [...],
  results: [<group>...],
  filtered: [<group>...],    // ALWAYS PRESENT (possibly empty) per Phase 80 D-04
  request: { <6-key echo> }
}
```

Per-group item:

```
{
  uid: "IE{N}_P{M}_FL{K}" or null,
  locator: { sys_id, volume_ie, p_num, fl_id },
  aggregate_score: float,
  matches: [
    {
      chunk_index: int,
      score: float,
      source_chunk_text: str,
      manuscript_snippet: str,
      ...
    }
  ]
}
```

### 7.1 Six-key request echo (81A-02 / Phase 80)

Built at web/search_api.py:L1024-L1031.

| Echo key | Source | Notes |
| --- | --- | --- |
| `mode` | `req.mode` | NOT `search_mode` (D-07; see §6.1) |
| `chunk_size` | `req.chunk_size` | unmodified |
| `max_freq` | `req.max_freq` | unmodified (None permitted) |
| `boundary_options` | `bundle.boundary_options` (5-key dict — `boundary_mode`, `boundary_delimiter`, `boundary_boost`, `min_boundary_matches`, `min_delimiter_distance`) | server-resolved values incl. defaults from service layer |
| `limit_effective` | `len(bundle.main_results)` | post-truncation group count |
| `filters` | `filters_dict` (FiltersModel.model_dump(exclude_none=True)) or None | — |

Explicitly NO `search_mode`, NO `gap`, NO `responsa_options` keys (parallels never used Responsa; gap is a Phase 78 search-only concept).

Source: web/search_api.py:L1017-L1031.

---

## 8. Error Envelope

Uniform shape across all three search-helper endpoints. Source: web/api_hardening.py `_build_envelope_response`.

```
HTTP <status>
Content-Type: application/json
{
  "error": {
    "code": "<lowercase_snake_case>",
    "message": "<human-readable>"
  }
}
```

- Never raw FastAPI 422 dumps for these three endpoints (Concern #2 — handlers wrap their own bodies; no global exception handlers installed).
- HTTP 429 carries a `Retry-After` header (RateLimiter raises `APIError('rate_limited', http_status=429, headers={'Retry-After': N})`; `_build_envelope_response` propagates the header).

Source: web/search_api.py:L709-L724, shared/api_errors.py:L55-L89.

---

## 9. Error Code Catalogue

From `shared/api_errors.py:L24-L45` (`ERROR_CODES` frozenset) cross-referenced with raise-site evidence in `web/search_api.py`. The codes are part of the public API surface — renaming any is a breaking change.

| Code | HTTP status | Typical raise condition | Originating phase | Source |
| --- | --- | --- | --- | --- |
| `invalid_request` | 400 | malformed JSON, Pydantic structural validation failure (incl. unknown field — produces explicit "unknown field 'mode' — use search_mode instead" hint per 81A D-13), bad query-param int casts in browse, missing required browse locator, bad text_cap bounds, bad p_num | 78 | shared/api_errors.py:L25; web/search_api.py:L472-L475, L497, L505-L509, L545-L568, L808-L814, L313-L333 |
| `invalid_combination` | 400 | cross-field rejection: `responsa_options` with non-Responsa mode; `gap != 0` with title/shelfmark | 81A | shared/api_errors.py:L26; web/search_api.py:L151-L171 |
| `invalid_mode` | 400 | reserved (mode validation) | 78 | shared/api_errors.py:L27 |
| `query_required` | 400 | post-strip empty `query` | 78 | shared/api_errors.py:L28; web/search_api.py:L545-L549 |
| `query_too_long` | 400 | `len(query) > QUERY_LENGTH_CAP=1000` | 78 | shared/api_errors.py:L29; web/search_api.py:L550-L556 |
| `limit_too_high` | 400 | `req.limit > MAX_LIMIT=100` (also enforced via Pydantic `Field(le=100)` which rejects as `invalid_request` first; this branch is defense-in-depth) | 78 (cap lowered 81A D-06) | shared/api_errors.py:L30; web/search_api.py:L563-L568 |
| `unknown_filter_key` | 400 | filter key not in known set | 78 | shared/api_errors.py:L31; shared/fjms_service.validate_filter_values |
| `unresolvable_filter_value` | 400 | filter value not in vocabulary | 78 | shared/api_errors.py:L32; shared/fjms_service.validate_filter_values |
| `filter_vocabulary_unavailable` | 503 | vocabulary loader failed (R2-#3 fail-closed) | 78 | shared/api_errors.py:L33 |
| `rate_limited` | 429 + `Retry-After` | per-IP sliding window exhausted on the endpoint's own bucket | 78 | shared/api_errors.py:L34; web/api_hardening.RateLimiter |
| `disabled` | 503 | `SEARCH_API_MODE=disabled` | 78 | shared/api_errors.py:L35; web/api_hardening.enforce_mode_gate |
| `localhost_only` | 403 | `SEARCH_API_MODE=localhost-only` and request from non-loopback | 78 | shared/api_errors.py:L36; web/api_hardening.enforce_mode_gate |
| `internal_error` | 500 | unhandled exception in handler | 78 | shared/api_errors.py:L37; web/search_api.py:L719-L724 |
| `locator_conflict` | 400 | uid malformed; uid disagrees with sys_id/p_num/fl_id/volume_ie | 79 | shared/api_errors.py:L39; web/search_api.py:L338-L365 |
| `manuscript_page_not_found` | 404 | core fetch returned `bundle.page is None`; or post-resolution `bundle.page.uid != requested_uid` | 79 | shared/api_errors.py:L40; web/search_api.py:L842-L864 |
| `core_timeout` | 504 | core BrowsePage fetch exceeded `SEARCH_API_BROWSE_CORE_TIMEOUT=2.0` | 79 | shared/api_errors.py:L41; shared/browse_service.fetch_browse_bundle |
| `composition_required` | 400 | `text.strip()` empty | 80 | shared/api_errors.py:L43; web/search_api.py:L949-L955 |
| `composition_too_long` | 400 | `len(text.strip()) > COMPOSITION_LENGTH_CAP=20000` | 80 | shared/api_errors.py:L44; web/search_api.py:L956-L962 |

Cross-checked against `shared/api_errors.py:L24-L45`: no missing codes. The full taxonomy is exhausted by the table above.

Source: shared/api_errors.py:L24-L45.

---

## 10. Warnings Vocabulary

Top-level `warnings: [...]` array codes (per HARDEN-03: warnings are TOP-LEVEL, never per-item). Source: `WARNING_CODES` frozenset in shared/api_errors.py:L48-L52 plus phase-specific surfacing.

| Warning | Endpoint | When emitted | Source |
| --- | --- | --- | --- |
| `query_downgraded: <message>` | search | Responsa cascade disabled one or more options (variants, ja, flex_spacing, bidirectional); also surfaced via `responsa_options_effective` cascade meta | shared/api_errors.py:L49; web/search_api.py:L640-L660; genizah_core._consume_last_responsa_downgrade |
| `volume_ie_defaulted` | browse | sys_id-only request resolved against multi-IE manuscript; server auto-picked default IE | web/search_api.py:L867-L877 (Phase 79 D-04) |
| `enrichment_timeout` | browse | per-source PGP/FJMS/NLI fetch hit `SEARCH_API_BROWSE_TIMEOUT` (default 1.0s); soft failure, partial bundle returned | shared/browse_service.fetch_browse_bundle |
| `enrichment_failed` | browse | per-source PGP/FJMS/NLI fetch raised; soft failure, partial bundle returned | shared/browse_service.fetch_browse_bundle |
| `truncated_to_200` | parallels | group count exceeds 200; top 200 returned (Phase 80 D-07) | shared/api_errors.py:L51; web/search_api.py:L1013-L1015 |

Source: shared/api_errors.py:L48-L52, web/search_api.py:L652-L660, L867-L877, L1013-L1015.

---

## 11. Environment Variables

### 11.1 Server-side (already in CLAUDE.md L137-L151)

| Env var | Default | Scope | First introduced | Source |
| --- | --- | --- | --- | --- |
| `SEARCH_API_MODE` | `open` | gate (`open` \| `localhost-only` \| `disabled`); flippable per request without restart; applies to /api/search, /api/browse, /api/parallels | Phase 78 | CLAUDE.md:L143; web/api_hardening.enforce_mode_gate |
| `SEARCH_API_RATE_LIMIT` | `30` | per-IP requests per minute; SHARED ceiling across the three buckets but each endpoint counts independently | Phase 78 (clarified Phase 80 D-05) | CLAUDE.md:L144; web/api_hardening.RateLimiter._current_limit |
| `POSTHOG_IP_SALT` | auto-generated if unset | HMAC salt for hashing client IPs in server-side PostHog events; production should set explicitly | Phase 78 | CLAUDE.md:L145 |
| `SEARCH_API_POSTHOG_SAMPLE_N` | `1` | capture every Nth API request to PostHog; 1 = every request | Phase 78 | CLAUDE.md:L146 |
| `SEARCH_API_BROWSE_TIMEOUT` | `1.0` | per-source PGP/FJMS/NLI enrichment timeout for /api/browse, in seconds | Phase 79 | CLAUDE.md:L147 |
| `SEARCH_API_BROWSE_CORE_TIMEOUT` | `2.0` | core BrowsePage fetch timeout for /api/browse (R-01: prevents executor pinning on hung Tantivy reader) | Phase 79 R-01 | CLAUDE.md:L148-L149 |
| `SEARCH_API_BROWSE_TEXT_CAP` | `4000` | default char cap for transcription text in /api/browse; per-request `?text_cap=N` bounded by [100, 10000] | Phase 79 R-08 | CLAUDE.md:L150-L151; web/search_api.py:L194-L196, L387-L400 |

(`PUZZLE_UPLOAD_SECRET` is unrelated to the search API and is excluded from this audit even though it appears in CLAUDE.md.)

### 11.2 Skill-side (NOT in CLAUDE.md as of phase 82 entry)

| Env var | Default | What it controls | Source |
| --- | --- | --- | --- |
| `GENIZAH_API_BASE` | `https://genizahsearch.com` | Base URL for all skill API calls (Phase 81B D-09 precedence: env var > `--base-url` CLI flag > default — INVERSION of typical CLI convention) | skills/cairo-genizah-research/scripts/_config.py:L3, L19, L23; skills/cairo-genizah-research/SKILL.md:L49, L54 |
| `GENIZAH_SKILL_REQ_PER_MIN` | `24` | skill-side throttle ceiling per endpoint bucket (Phase 78 HARDEN-01 — kept under server `SEARCH_API_RATE_LIMIT=30` to leave headroom) | skills/cairo-genizah-research/scripts/_config.py:L58, L60; skills/cairo-genizah-research/SKILL.md:L51 |

### 11.3 CLAUDE.md DOC-02 deltas

Plan 03 must ADD the following entries to the CLAUDE.md env-var block (currently L137-L151):

- `GENIZAH_SKILL_REQ_PER_MIN` — skill-side throttle ceiling per endpoint bucket; default `24`. Source: skills/cairo-genizah-research/scripts/_config.py:L60.
- `GENIZAH_API_BASE` — skill-side base URL override; default `https://genizahsearch.com`. Precedence: env var > `--base-url` CLI flag > default. Source: skills/cairo-genizah-research/scripts/_config.py:L23.

All seven server-side env vars (§11.1) are already present in CLAUDE.md L137-L151. **No additions are needed for the server-side surface — only the two skill-side vars listed above.**

---

## 12. Rate Limiting Architecture

Three INDEPENDENT per-IP buckets, all reading the same `SEARCH_API_RATE_LIMIT` env-var ceiling:

| Bucket | Limiter instance | Endpoint | Source |
| --- | --- | --- | --- |
| search | `_rate_limiter` | POST /api/search | web/search_api.py:L64, L540 |
| browse | `_browse_rate_limiter` | GET /api/browse | web/search_api.py:L71, L822 |
| parallels | `_parallels_rate_limiter` | POST /api/parallels | web/search_api.py:L78, L946 |

Consequence: a single client doing search+browse+parallels has approximately **3× the per-IP allowance of one endpoint alone** (Phase 80 D-05; Phase 79 D-18 R-10 captures it as a monitoring obligation, not a contract change in v7.10). Independence verified by `tests/test_parallels_api.py::test_parallels_rate_limit_independence`.

429 responses include a `Retry-After` header (see §8).

Source: web/search_api.py:L60-L78, L540, L822, L946.

---

## 13. Statelessness Contract

Per Phase 78 D-22 / D-20 (HARDEN): the three search-helper endpoints have ZERO references to:
- `state.last_results`
- `state.current_search_query`
- `state.parallels_results`
- `app.storage` (any sub-key)
- `request.cookies`

Identical query strings produce identical bodies regardless of session. Verified by grep at acceptance time. Handler docstrings restate this constraint at web/search_api.py:L26-L29 (search), L788-L790 (browse), L913-L915 (parallels).

Source: web/search_api.py:L26-L29, L612, L879-L880, L986.

---

## 14. Mode Gate

`SEARCH_API_MODE` env var, three values:

| Value | Effect | Error code on rejection |
| --- | --- | --- |
| `open` (default) | all callers permitted | — |
| `localhost-only` | only loopback callers permitted | `localhost_only` (403) |
| `disabled` | endpoint returns error envelope unconditionally | `disabled` (503) |

Flippable per request without restart (`enforce_mode_gate(request)` re-reads env every call). Applies ONLY to the three search-helper endpoints (/api/search, /api/browse, /api/parallels); existing /api/* routes (image proxies, puzzle uploads, NLI proxies) are unaffected.

Source: CLAUDE.md:L143; web/search_api.py:L533, L818, L942; web/api_hardening.enforce_mode_gate.

---

## 15. Drill-Down Locator Round-Trip

The `locator` field on /api/search and /api/parallels result items is shaped:

```
{
  sys_id: str,
  volume_ie: str or null,
  p_num: int or null,
  fl_id: str or null
}
```

Plus a top-level `uid: "IE{N}_P{M}_FL{K}"` on each result item when resolvable.

Round-trip into /api/browse:

- `GET /api/browse?uid=IE12345_P3_FL999` (preferred — uniquely resolves)
- `GET /api/browse?sys_id=...&volume_ie=IE12345&p_num=3` (works when uid is null)
- `GET /api/browse?sys_id=...&fl_id=FL999`

Locator fields can be fed verbatim — no normalization needed. uid+sys_id together is the safest combination because /api/browse cross-checks them at D-03b (see §4.4).

Source: shared/search_serializer.py (locator/uid emission), web/search_api.py:L296-L384 (parsing).

---

## 16. Source-File Reference Index

Quick-lookup table for Plan 02 citation hygiene (avoids re-reading source).

| Contract element | Source file:line |
| --- | --- |
| Module docstring + statelessness contract | web/search_api.py:L1-L29 |
| Rate-limiter instances (three) | web/search_api.py:L64, L71, L78 |
| `FiltersModel` | web/search_api.py:L100-L109 |
| `ResponsaOptions` | web/search_api.py:L112-L125 |
| `SearchRequest` + cross-field validators | web/search_api.py:L128-L171 |
| Constants `QUERY_LENGTH_CAP`, `MAX_LIMIT`, `COMPOSITION_LENGTH_CAP` | web/search_api.py:L174-L202 |
| Browse text-cap constants | web/search_api.py:L194-L196 |
| `BrowseRequest` | web/search_api.py:L210-L226 |
| `ParallelsRequest` | web/search_api.py:L228-L253 |
| `NormalizedLocator` + `_parse_uid` + `_validate_locator` | web/search_api.py:L256-L385 |
| `_resolve_text_cap` | web/search_api.py:L387-L400 |
| `init_search_api` (idempotent registrar) | web/search_api.py:L407-L432 |
| `search_endpoint` | web/search_api.py:L438-L772 |
| Search 7-key request echo build site | web/search_api.py:L684-L692 |
| Search envelope serializer call | web/search_api.py:L694-L707 |
| Old-`mode`-field rejection hint | web/search_api.py:L503-L509 |
| `browse_endpoint` | web/search_api.py:L774-L896 |
| Browse `volume_ie_defaulted` warning emission | web/search_api.py:L867-L877 |
| Browse post-resolution uid verification (D-03b) | web/search_api.py:L856-L864 |
| `parallels_endpoint` | web/search_api.py:L898-L1050 |
| Parallels 6-key request echo build site | web/search_api.py:L1024-L1031 |
| Parallels `truncated_to_200` warning emission | web/search_api.py:L1013-L1015 |
| `APIError` + `ERROR_CODES` + `WARNING_CODES` | shared/api_errors.py:L24-L89 |
| Skill `GENIZAH_API_BASE` resolver | skills/cairo-genizah-research/scripts/_config.py:L19-L23 |
| Skill `GENIZAH_SKILL_REQ_PER_MIN` resolver | skills/cairo-genizah-research/scripts/_config.py:L58-L60 |
| Server env-var documentation block | CLAUDE.md:L137-L151 |

Source: this audit.

---

End of audit.

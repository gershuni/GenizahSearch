# GenizahSearch Search-Helper API (v7.10)

> Last updated: 2026-05-05

## ⚠ Internal Helper — No Stability Promise

This page documents an **internal** helper API used by first-party tooling (the
`cairo-genizah-research` Claude skill, deployment soak tests, and ad-hoc maintainer
scripts). It is **not** a public API.

- The contract may change without warning, including breaking changes within a patch release.
- There are no API keys, no published SLAs, no semver guarantees.
- It is not linked from `README.md` or the public site, and external usage is not invited.
- For external research access to the Genizah corpus, use the website search interface at https://genizahsearch.com.

If you are reading this because you are about to build a new internal consumer, prefer the
skill at `skills/cairo-genizah-research/` over hand-rolling — it already handles the
rate-limiter cooperation, retry semantics, and locator round-trip.

## Overview

The v7.10 search-helper API exposes three endpoints — `POST /api/search`, `GET /api/browse`,
and `POST /api/parallels` — that together let an internal consumer execute a Tantivy
keyword/Responsa search, drill down into a single manuscript page (with PGP/FJMS/NLI
enrichment), and run a composition-parallels job over an arbitrary input text. The three
endpoints are consumed primarily by the `cairo-genizah-research` Claude skill, by the
deployment soak/smoke harness, and by occasional maintainer scripts. They share a common
hardening shell: per-IP rate limiting (independent token bucket per endpoint, all reading
the same `SEARCH_API_RATE_LIMIT` env-var ceiling), a mode gate (`SEARCH_API_MODE`) that
permits run-time disabling or loopback-only restriction without restart, a uniform error
envelope, sampled PostHog observability, and a Phase-77-locked JSON envelope shape produced
by the sole serializer at `shared/search_serializer.py`. The endpoints are stateless: the
same query string produces the same body regardless of session.

## Endpoint: POST /api/search

Request body is JSON. The `SearchRequest` Pydantic model has `extra='forbid'` — any unknown
top-level field produces a 400 `invalid_request` envelope, with an explicit cutover hint
when the offending key is the legacy `mode` field (renamed to `search_mode` in Phase 81A).

### Request — non-Responsa example

```json
{
  "query": "rambam",
  "search_mode": "exact",
  "gap": 0,
  "limit": 50,
  "filters": {
    "domains": ["Halakha"],
    "date_from": 1100,
    "date_to": 1300
  }
}
```

### Request — Responsa example

```json
{
  "query": "תשובה",
  "search_mode": "responsa",
  "responsa_options": {
    "variants": true,
    "ja": true,
    "flex_spacing": false,
    "bidirectional": false
  },
  "gap": 5,
  "limit": 25
}
```

### Request fields

| Name | Type | Constraint | Default | Notes |
| ---- | ---- | ---------- | ------- | ----- |
| `query` | string | 1..1000 chars (post-strip; empty → `query_required`; over cap → `query_too_long`) | required | `QUERY_LENGTH_CAP=1000` |
| `search_mode` | enum | `exact \| variants \| responsa \| title \| shelfmark` | required | `regex` was intentionally dropped per Phase 81A D-09; `fuzzy` is intentionally not present (it is a `/api/parallels` mode only) |
| `responsa_options` | object \| null | valid only when `search_mode="responsa"` | `null` | see sub-table below |
| `gap` | integer | must be `0` when `search_mode in {title, shelfmark}` | `0` | proximity slop for keyword search |
| `limit` | integer | `1..100` (`MAX_LIMIT=100`, lowered from 200 per Phase 81A D-06) | `50` | |
| `filters` | object \| null | all sub-fields nullable; unknown filter keys → 400 `unknown_filter_key`; unknown values → 400 `unresolvable_filter_value` | `null` | `domains`, `authors`, `works`, `materials` (string lists); `date_from`, `date_to` (int years) |

### `responsa_options` sub-fields

| Name | Type | Default | Notes |
| ---- | ---- | ------- | ----- |
| `variants` | bool | `false` | morphological/orthographic variants |
| `ja` | bool | `false` | Judeo-Arabic transliteration variants |
| `flex_spacing` | bool | `false` | tolerate variable whitespace within phrase |
| `bidirectional` | bool | `false` | match phrase in either token order |

Field names mirror the desktop UI checkboxes. Server-side derivation: `variant_mode` is
implicit (`'variants' if opts.variants else 'exact'`); do not send it. Other field names
(extended/maximum tiers, `variant_mode`, etc.) are rejected by `extra='forbid'`.

### Responsa query string syntax

In addition to `responsa_options` flags, the **`query` string itself** supports a
Responsa-Project-style mini-syntax (parsed by `genizah_core.parse_responsa_query`).
This syntax is only honored when `search_mode="responsa"`; in other modes the same
characters are matched literally. Tokens are whitespace-separated; modifiers stack.

| Syntax | Example | Meaning |
| ------ | ------- | ------- |
| plain word | `שלום` | exact word match |
| suffix wildcard | `שלום*` | word starts with the prefix |
| prefix wildcard | `*נדר` | word ends with the suffix |
| character pattern | `*פ*ט*ר*פ*` | letters appear in order, any chars between |
| grammatical prefixes | `#שלום` | match plus all Hebrew grammatical prefix expansions (`ה`, `ב`, `ל`, `מ`, `ש`, `ו`, `כ`, plus 2-letter combos `וה`, `שב`, …) |
| grammatical suffixes | `שלום#` | match plus Hebrew grammatical suffix expansions |
| both prefixes + suffixes | `#שלום#` | combine the two above |
| plene/defective | `%שלום` | tolerate plene/defective spelling variants |
| stacked modifiers | `%#שלום#` | plene/defective + prefixes + suffixes (any order of `%` and leading `#`) |
| OR group | `(עץ/אילן)` | match any of the alternatives at that position |
| modifier + OR | `#(שלום/שלומות)` | grammatical prefixes applied to each alternative |
| negation | `-word` | exclude results containing this token (modifier prefix `-` may combine with `%`/`#`) |
| per-pair gap | `word1 [3] word2` | allow up to 3 intervening tokens between the surrounding pair (overrides top-level `gap` for that pair) |
| line constraint | `\|word` / `word\|` | token must be at the start / end of a manuscript line |
| line gap | `word1 [\|2] word2` | allow up to 2 line breaks between the pair |

Notes:
- The leading `#`, `%`, `-` modifiers and the trailing `#` modifier may appear on plain
  words, OR groups, and patterns; combinations are commutative for the leading set.
- `[N]` and `[\|N]` tokens are gap markers, not search tokens — they do not count toward
  the proximity slop and do not appear in `responsa_options_effective`.
- Wildcard expansion can be expensive; the Responsa cascade may downgrade noisy patterns
  and surface a `query_downgraded` warning in `warnings[]`.
- The `query` length cap (`QUERY_LENGTH_CAP=1000`) applies to the raw string, not the
  post-expansion form.

### Cross-field validation rejections

| Input pattern | Error code | HTTP | Notes |
| ------------- | ---------- | ---- | ----- |
| `responsa_options` set AND `search_mode != "responsa"` | `invalid_combination` | 400 | |
| `gap != 0` AND `search_mode in {title, shelfmark}` | `invalid_combination` | 400 | |
| Top-level `mode` key (legacy Phase 78 shape) | `invalid_request` | 400 | message includes hint `"unknown field 'mode' — use search_mode instead"` per Phase 81A D-13 |
| `regex` value for `search_mode` | `invalid_request` | 400 | structurally rejected by Literal enum; not in v7.10 |

### Response example

```json
{
  "schema_version": "1.0",
  "source": "search",
  "generated_at": "2026-05-05T12:34:56Z",
  "count": 1,
  "total": 1,
  "warnings": [],
  "results": [
    {
      "uid": "IE12345_P3_FL999",
      "locator": {
        "sys_id": "990001234560205171",
        "volume_ie": "IE12345",
        "p_num": 3,
        "fl_id": "FL999"
      },
      "score": 12.47,
      "shelfmark": "T-S 12.123",
      "title": "תשובה לרמב\"ם",
      "snippet": "...לפני הכתיבה תשובת הרמב\"ם...",
      "excerpt": "...הרמב\"ם השיב על השאלה הזאת...",
      "metadata": {
        "library": "CUL",
        "library_name": "Cambridge University Library",
        "domains": ["Halakha"],
        "dating": "12th century"
      },
      "image_url": "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123/canvas/1"
    }
  ],
  "request": {
    "search_mode": "responsa",
    "responsa_options": {"variants": true, "ja": true, "flex_spacing": false, "bidirectional": false},
    "responsa_options_effective": {"variants": true, "ja": true, "flex_spacing": false, "bidirectional": false},
    "gap": 5,
    "limit": 25,
    "limit_effective": 25,
    "filters": null
  }
}
```

### Response item fields

| Name | Type | Notes |
| ---- | ---- | ----- |
| `uid` | string \| null | `IE{N}_P{M}_FL{K}` when resolvable; safe to feed verbatim into `/api/browse?uid=...` |
| `locator` | object | always present; `{sys_id, volume_ie, p_num, fl_id}`; any individual field may be null |
| `score` | float | Tantivy raw score |
| `shelfmark` | string | canonical shelfmark for display |
| `title` | string | manuscript title (often Hebrew) |
| `snippet` | string | pre-snippet (text before the hit) |
| `excerpt` | string | post-snippet (text including/after the hit) |
| `metadata` | object | `library`, `library_name`, `domains` (list), `dating` (string) |
| `image_url` | string \| null | best-effort IIIF URL; server does not probe upstream availability |

### 7-key request echo

Each `/api/search` response carries a top-level `request` object with exactly seven keys
(no more, no fewer). Per Phase 81A D-04, `search_mode` is echoed VERBATIM — never silently
downgraded — and the `responsa_options_effective` key reflects the post-cascade outcome,
which may differ from the client-supplied `responsa_options`.

| Echo key | Source | Notes |
| -------- | ------ | ----- |
| `search_mode` | `req.search_mode` | echoed verbatim |
| `responsa_options` | client-supplied (model-dumped); non-Responsa modes → `null` (per D-05) | what the client sent |
| `responsa_options_effective` | post-cascade values; mirrors `responsa_options` when no cascade fired; non-Responsa modes → `null` | reflects what the engine actually applied |
| `gap` | `req.gap` | unmodified |
| `limit` | `req.limit` | unmodified |
| `limit_effective` | `min(req.limit, MAX_LIMIT)` | post-cap value actually applied |
| `filters` | model-dumped FiltersModel (exclude_none) or `null` | post-validation snapshot |

**Worked Responsa cascade case.** Client sends `responsa_options.ja=true`; the server's
Responsa cascade decides the JA expansion is unsafe for this query and disables it. The
echo then reads:

```json
{
  "request": {
    "search_mode": "responsa",
    "responsa_options":           {"variants": true, "ja": true,  "flex_spacing": false, "bidirectional": false},
    "responsa_options_effective": {"variants": true, "ja": false, "flex_spacing": false, "bidirectional": false},
    "gap": 5,
    "limit": 25,
    "limit_effective": 25,
    "filters": null
  },
  "warnings": [
    {"code": "query_downgraded", "message": "Judeo-Arabic expansion disabled for this query."}
  ]
}
```

The cascade signals through both channels: the divergence between `responsa_options` and
`responsa_options_effective`, AND a top-level `warnings[]` entry with the human-readable
reason.

## Endpoint: GET /api/browse

Drill-down into a single manuscript page. `BrowseRequest` is built from query parameters
(FastAPI does not auto-bind GET params to a Pydantic model when the handler takes the raw
request). `extra='forbid'` applies. `sys_id` is required (Phase 79 D-01). At least one of
`uid`, `p_num`, `fl_id` must be supplied alongside it.

### Three resolution paths

1. **uid alone** — `?sys_id=...&uid=IE{N}_P{M}_FL{K}`. The handler parses uid into
   `volume_ie`/`p_num`/`fl_id` (regex `^(IE\d+)_(P\d+)_(FL\d+)$`).
2. **sys_id + p_num + volume_ie** — `?sys_id=...&p_num=3&volume_ie=IE12345`.
3. **sys_id + fl_id** — `?sys_id=...&fl_id=FL999`.

Optional `?text_cap=N` overrides the per-request transcription cap (`[100, 10000]` chars;
overrides env `SEARCH_API_BROWSE_TEXT_CAP`, default `4000`).

### Locator-conflict examples

| Request | Resulting error |
| ------- | --------------- |
| `?sys_id=A&uid=BAD_FORMAT` | 400 `locator_conflict` "uid is malformed" |
| `?sys_id=A&uid=IE1_P3_FL9&p_num=4` | 400 `locator_conflict` (uid p_num disagrees with explicit p_num) |
| `?sys_id=A&uid=IE1_P3_FL9&volume_ie=IE2` | 400 `locator_conflict` (uid volume_ie disagrees) |
| `?sys_id=A&uid=IE1_P3_FL9&fl_id=FL77` | 400 `locator_conflict` (uid fl_id disagrees) |
| `?sys_id=A&uid=IE1_P3_FL9` where uid actually belongs to manuscript B | 404 `manuscript_page_not_found` (post-resolution check; Phase 79 D-03b) |

### Response example

```json
{
  "schema_version": "1.0",
  "source": "browse",
  "generated_at": "2026-05-05T12:35:00Z",
  "locator": {
    "uid": "IE12345_P3_FL999",
    "sys_id": "990001234560205171",
    "volume_ie": "IE12345",
    "p_num": 3,
    "fl_id": "FL999"
  },
  "shelfmark": "T-S 12.123",
  "title": "תשובה לרמב\"ם",
  "library_code": "CUL",
  "library_name": "Cambridge University Library",
  "text": "<full PGP transcription, capped at text_cap chars>",
  "text_source": "pgp_transcription",
  "text_truncated": false,
  "metadata": {
    "pgp":  {"pgpid": 12345, "description": "...", "editions": [], "translations": []},
    "fjms": {"catalog_records": [], "free_descriptions": [], "bibliography": []},
    "nli":  {"manifest_url": "...", "fl_index": 4}
  },
  "image": {
    "url": "https://cudl.lib.cam.ac.uk/iiif/MS-TS-00012-00123/canvas/1",
    "provider": "Cambridge CUDL",
    "sources": ["CUDL", "NLI"]
  },
  "warnings": []
}
```

### Top-level fields

| Name | Type | Notes |
| ---- | ---- | ----- |
| `schema_version` | string | currently `"1.0"` |
| `source` | string | `"browse"` |
| `generated_at` | string | ISO-8601 UTC |
| `locator` | object | resolved locator (all five fields) |
| `shelfmark` | string | canonical shelfmark |
| `title` | string | manuscript title |
| `library_code` | string | e.g. `CUL`, `JTS`, `Oxford` |
| `library_name` | string | full library name |
| `text` | string | transcription, capped at effective `text_cap` |
| `text_source` | enum | see below |
| `text_truncated` | bool | `true` when `text` was clipped to `text_cap` |
| `metadata.pgp` | object \| null | PGP enrichment; `null` on per-source failure |
| `metadata.fjms` | object \| null | FJMS enrichment; `null` on per-source failure |
| `metadata.nli` | object \| null | NLI enrichment; `null` on per-source failure |
| `image` | object | `{url, provider, sources[]}`; `url` may be `null` |
| `warnings` | array | top-level only (see Warnings section) |

### `text_source` enum (verbatim, locked Phase 79 D-10)

- `pgp_transcription` — primary path; `text` came from `document_service.fetch_pgp_transcription`. The skill maps this value to "full text available".
- `snippet` — fallback path; `text` is a synthesized snippet from `browse_service`.
- `none` — no transcription resolvable; `text` is empty.

### Best-effort image URL contract

Per Phase 79 R-PR-01 / D-14, the server emits `image.url` and `image.sources[]` WITHOUT
probing the upstream IIIF endpoint for availability. Clients must tolerate `image.url`
values that 404 or time out at the IIIF host. There is no head-probe; do not request one.

### Per-source enrichment failure modes

`metadata.pgp`, `metadata.fjms`, and `metadata.nli` are independently populated via
soft-failure-tolerant fetches. Any one of them may be `null` while the response is still
HTTP 200. When this happens, the top-level `warnings[]` array carries a corresponding
`enrichment_timeout` or `enrichment_failed` entry naming the source. The browse response
is therefore always partially usable even when one or two enrichers are slow or down.

## Endpoint: POST /api/parallels

Composition parallels: given an arbitrary input text, find manuscripts whose chunks match.
The `ParallelsRequest` Pydantic model has `extra='forbid'`. Note that the field name for
mode is `mode`, NOT `search_mode` — see "Naming Inconsistency" below.

### Request example

```json
{
  "text": "<the composition text — up to 20000 chars>",
  "chunk_size": 5,
  "mode": "variants",
  "max_freq": 0.05,
  "boundary_mode": "combined",
  "filters": {
    "domains": ["Liturgy"]
  }
}
```

### Request fields

| Name | Type | Constraint | Default | Notes |
| ---- | ---- | ---------- | ------- | ----- |
| `text` | string | 1..20000 chars (post-strip; `COMPOSITION_LENGTH_CAP=20000`; empty → `composition_required`; over cap → `composition_too_long`) | required | |
| `chunk_size` | integer | `2..20` | `5` | size of sliding chunks |
| `mode` | enum | `exact \| variants \| fuzzy` | `"exact"` | **field name is `mode`, not `search_mode`** — see "Naming Inconsistency" |
| `max_freq` | float \| null | `null` disables high-frequency filtering (no chunks routed to `filtered`) | `null` | |
| `boundary_mode` | enum | `full \| boundary \| combined` | `"full"` | only boundary knob exposed in v7.10 (Phase 80 D-03) |
| `filters` | object \| null | reuses Phase 78 `FiltersModel` verbatim | `null` | same shape as `/api/search.filters` |

The Lab Engine extended-parallels path is OUT OF SCOPE for v7.10 (Phase 80 D-02).

### Response example

```json
{
  "schema_version": "1.0",
  "source": "parallels",
  "generated_at": "2026-05-05T12:36:00Z",
  "count": 1,
  "warnings": [],
  "results": [
    {
      "uid": "IE12345_P3_FL999",
      "locator": {
        "sys_id": "990001234560205171",
        "volume_ie": "IE12345",
        "p_num": 3,
        "fl_id": "FL999"
      },
      "aggregate_score": 8.42,
      "matches": [
        {
          "chunk_index": 0,
          "score": 4.21,
          "source_chunk_text": "...input text chunk...",
          "manuscript_snippet": "...matched manuscript text..."
        }
      ]
    }
  ],
  "filtered": [],
  "request": {
    "mode": "variants",
    "chunk_size": 5,
    "max_freq": 0.05,
    "boundary_options": {
      "boundary_mode": "combined",
      "boundary_delimiter": "...",
      "boundary_boost": 1.0,
      "min_boundary_matches": 1,
      "min_delimiter_distance": 0
    },
    "limit_effective": 1,
    "filters": {"domains": ["Liturgy"]}
  }
}
```

### Always-present `filtered` array

Per Phase 80 D-04, the `filtered: [...]` top-level array is ALWAYS present (possibly
empty). It contains groups whose chunks were filtered out by the `max_freq` high-frequency
threshold but are still reported back to the client for transparency.

### 200-group cap and `truncated_to_200` warning

The response is hard-capped at 200 result groups (Phase 80 D-07). When a query produces
more than 200 groups, the top 200 are returned and a `{"code": "truncated_to_200", ...}`
entry is added to `warnings[]`.

### 6-key request echo

The parallels response echoes exactly six keys (no more, no fewer). Explicitly NOT echoed:
`search_mode` (parallels uses `mode`), `gap` (a search-only concept), `responsa_options`
(parallels never used Responsa).

| Echo key | Source | Notes |
| -------- | ------ | ----- |
| `mode` | `req.mode` | NOT `search_mode` (per Phase 81A D-07) |
| `chunk_size` | `req.chunk_size` | unmodified |
| `max_freq` | `req.max_freq` | `null` permitted |
| `boundary_options` | server-resolved 5-key dict (`boundary_mode`, `boundary_delimiter`, `boundary_boost`, `min_boundary_matches`, `min_delimiter_distance`) | includes service-layer defaults |
| `limit_effective` | `len(bundle.main_results)` | post-truncation group count |
| `filters` | model-dumped `FiltersModel` (exclude_none) or `null` | |

## Naming Inconsistency: parallels.mode vs search.search_mode

`/api/search` uses `search_mode` (the post-Phase-81A name). `/api/parallels` continues to
use `mode`. This is intentional v7.10 debt locked by Phase 81A D-07: renaming the parallels
field would have broken Phase 80 tests with no consumer-visible benefit, since the
parallels enum is a different set of values (`exact | variants | fuzzy`) than the search
enum (`exact | variants | responsa | title | shelfmark`). Future versions may unify the
field name; consumers should code defensively against both names.

The two enums share only `exact` and `variants`. A consumer must use the correct field
name per endpoint and must not assume value-set equivalence (`responsa`/`title`/`shelfmark`
exist only in `/api/search`; `fuzzy` exists only in `/api/parallels`).

## Drill-Down Locator Round-Trip

The `locator` field on `/api/search` and `/api/parallels` result items is shaped:

```json
{
  "sys_id": "990001234560205171",
  "volume_ie": "IE12345",
  "p_num": 3,
  "fl_id": "FL999"
}
```

…and each result item also carries a top-level `uid: "IE{N}_P{M}_FL{K}"` when resolvable.

**Worked round-trip.** Given the search response shown above, the consumer reads the first
result's `uid` field (`IE12345_P3_FL999`) and issues:

```
GET /api/browse?sys_id=990001234560205171&uid=IE12345_P3_FL999
```

The browse response (shown in the `/api/browse` section above) returns the same
manuscript's full PGP transcription, IIIF image URL, and per-source enrichment in a single
hop — no disambiguation, no follow-up calls. The `uid+sys_id` combination is the safest
because the browse endpoint cross-checks them post-resolution (Phase 79 D-03b) and returns
404 `manuscript_page_not_found` if the pair refers to different manuscripts.

When `uid` is null on the result item (rare: the underlying record lacked a complete
locator triple), use one of the alternate browse paths:

```
GET /api/browse?sys_id=...&volume_ie=IE12345&p_num=3
GET /api/browse?sys_id=...&fl_id=FL999
```

No locator-field normalization is needed — feed values verbatim.

## Error Envelope

The three search-helper endpoints all wrap their errors in a uniform JSON envelope.
Existing legacy `/api/*` routes (image proxies, puzzle uploads, NLI proxies) keep their
original FastAPI default behavior — this envelope applies ONLY to `/api/search`,
`/api/browse`, and `/api/parallels`.

```json
{
  "error": {
    "code": "invalid_combination",
    "message": "responsa_options is only valid when search_mode='responsa'"
  }
}
```

Properties:

- `Content-Type: application/json` on every error response.
- HTTP status varies (see Error Codes table); never raw FastAPI 422 dumps for these three
  endpoints (Phase 78 Concern #2 — handlers wrap their own bodies; no global exception
  handlers installed).
- HTTP 429 carries a `Retry-After: <seconds>` header alongside the `rate_limited` body.

## Error Codes

Full table from `shared/api_errors.py` `ERROR_CODES` frozenset (the codes are part of the
public API surface — renaming any is a breaking change).

| Code | HTTP status | Typical raise condition |
| ---- | ----------- | ----------------------- |
| `invalid_request` | 400 | malformed JSON; Pydantic structural validation failure (incl. unknown field — emits "unknown field 'mode' — use search_mode instead" hint per Phase 81A D-13); bad query-param int casts in browse; missing required browse locator; bad `text_cap` bounds; bad `p_num` |
| `invalid_combination` | 400 | cross-field rejection: `responsa_options` with non-Responsa mode; `gap != 0` with title/shelfmark |
| `invalid_mode` | 400 | reserved (mode validation) |
| `query_required` | 400 | post-strip empty `query` |
| `query_too_long` | 400 | `len(query) > 1000` |
| `limit_too_high` | 400 | `req.limit > 100` (also enforced by Pydantic `Field(le=100)` first; defense-in-depth) |
| `unknown_filter_key` | 400 | filter key not in known set |
| `unresolvable_filter_value` | 400 | filter value not in vocabulary |
| `filter_vocabulary_unavailable` | 503 | vocabulary loader failed (Phase 78 R2-#3 fail-closed) |
| `rate_limited` | 429 + `Retry-After` | per-IP sliding window exhausted on the endpoint's own bucket |
| `disabled` | 503 | `SEARCH_API_MODE=disabled` |
| `localhost_only` | 403 | `SEARCH_API_MODE=localhost-only` and request from non-loopback |
| `internal_error` | 500 | unhandled exception in handler |
| `locator_conflict` | 400 | uid malformed; uid disagrees with sys_id/p_num/fl_id/volume_ie |
| `manuscript_page_not_found` | 404 | core fetch returned `bundle.page is None`; or post-resolution `bundle.page.uid != requested_uid` |
| `core_timeout` | 504 | core BrowsePage fetch exceeded `SEARCH_API_BROWSE_CORE_TIMEOUT` (default 2.0s) |
| `composition_required` | 400 | `text.strip()` empty |
| `composition_too_long` | 400 | `len(text.strip()) > 20000` |

See [shared/api_errors.py](../shared/api_errors.py) for the authoritative frozenset.

## Warnings Array

`warnings: [...]` is ALWAYS top-level on the response envelope, NEVER per-item (Phase 78
HARDEN-03). Items are never the right place: warnings describe the request, the engine
outcome, or a per-source enrichment soft failure — none of which are item-scoped.

| Code | Endpoint | Meaning |
| ---- | -------- | ------- |
| `query_downgraded: <message>` | search | Responsa cascade disabled one or more options (`variants`, `ja`, `flex_spacing`, `bidirectional`); also surfaced via `responsa_options_effective` divergence in the request echo. The `tr()` strings are the canonical signal alongside the echo. |
| `volume_ie_defaulted` | browse | `sys_id`-only request resolved against a multi-IE manuscript; server auto-picked the default IE (Phase 79 D-04). Includes `volume_ie` field naming the picked IE. |
| `enrichment_timeout` | browse | per-source PGP/FJMS/NLI fetch hit `SEARCH_API_BROWSE_TIMEOUT` (default 1.0s); soft failure; partial bundle returned with the corresponding `metadata.<source>` set to `null`. |
| `enrichment_failed` | browse | per-source PGP/FJMS/NLI fetch raised an exception; soft failure; partial bundle returned (same null-out behavior). |
| `truncated_to_200` | parallels | group count exceeded 200; top 200 returned (Phase 80 D-07). |

**Worked Responsa cascade case.** A `/api/search` response showing both signals
simultaneously:

```json
{
  "warnings": [
    {"code": "query_downgraded", "message": "Judeo-Arabic expansion disabled for this query."}
  ],
  "request": {
    "search_mode": "responsa",
    "responsa_options":           {"variants": true, "ja": true,  "flex_spacing": false, "bidirectional": false},
    "responsa_options_effective": {"variants": true, "ja": false, "flex_spacing": false, "bidirectional": false},
    "gap": 0,
    "limit": 50,
    "limit_effective": 50,
    "filters": null
  }
}
```

Both channels carry the same information. Programmatic consumers should branch on the
echo divergence; user-facing display should surface the warning message verbatim.

## Environment Variables

Every server-side var that affects the three endpoints, plus the two skill-side vars.

| Var | Default | Scope | Notes |
| --- | ------- | ----- | ----- |
| `SEARCH_API_MODE` | `open` | server | Values: `open` \| `localhost-only` \| `disabled`. Flippable per request without restart (`enforce_mode_gate` re-reads env every call). Applies to `/api/search`, `/api/browse`, `/api/parallels` only. |
| `SEARCH_API_RATE_LIMIT` | `30` | server | Per-IP requests per minute. **Shared ceiling but each endpoint has an independent bucket** — Phase 80 D-05 makes `/api/search` + `/api/browse` + `/api/parallels` run three separate rate-limiter instances reading the same env var, so a client doing search+browse+parallels gets approximately 3× the per-IP allowance of one endpoint alone. Verified by `tests/test_parallels_api.py::test_parallels_rate_limit_independence`. |
| `SEARCH_API_BROWSE_TIMEOUT` | `1.0` | server | Per-source enrichment timeout for `/api/browse` PGP/FJMS/NLI fetches, in seconds. Hitting it produces an `enrichment_timeout` warning (response is still 200). |
| `SEARCH_API_BROWSE_CORE_TIMEOUT` | `2.0` | server | Core BrowsePage fetch timeout for `/api/browse`, in seconds. Phase 79 R-01 added this to prevent executor pinning on a hung Tantivy reader; hitting it produces a 504 `core_timeout` envelope. |
| `SEARCH_API_BROWSE_TEXT_CAP` | `4000` | server | Default character cap for transcription text on `/api/browse`. Per-request override via `?text_cap=N`, bounded `[100, 10000]`. |
| `SEARCH_API_POSTHOG_SAMPLE_N` | `1` | server | Capture every Nth request to PostHog. `1` = every request. Applies to all three search-helper endpoints. |
| `POSTHOG_IP_SALT` | auto-generated | server | HMAC salt for hashing client IPs in server-side PostHog events. Optional, but production should set explicitly so hashes survive restarts. |
| `GENIZAH_API_BASE` | `https://genizahsearch.com` | skill | Base URL for all skill API calls. Per Phase 81B D-09, precedence is **env var > `--base-url` CLI flag > default** — an inversion of typical CLI convention; the env var ALWAYS wins. |
| `GENIZAH_SKILL_REQ_PER_MIN` | `24` | skill | Per-bucket throttle ceiling for the skill's token-bucket. Default leaves 6 rpm headroom under the server's 30 rpm `SEARCH_API_RATE_LIMIT`. |

The CLAUDE.md env-var block at [CLAUDE.md](../CLAUDE.md) currently documents all seven
server-side vars; the two skill-side vars are documented in
[skills/cairo-genizah-research/SKILL.md](../skills/cairo-genizah-research/SKILL.md).

## Rate Limiting & Buckets

Three INDEPENDENT per-IP buckets, all reading the same `SEARCH_API_RATE_LIMIT` env-var
ceiling on every request:

| Bucket | Limiter instance | Endpoint |
| ------ | ---------------- | -------- |
| search | `_rate_limiter` | `POST /api/search` |
| browse | `_browse_rate_limiter` | `GET /api/browse` |
| parallels | `_parallels_rate_limiter` | `POST /api/parallels` |

Bursting one endpoint's bucket does NOT exhaust the other two. A client making sustained
calls to all three endpoints sees roughly 3× the per-IP allowance compared with hammering
one endpoint alone. This is a deliberate v7.10 contract choice (Phase 80 D-05; Phase 79
D-18 R-10 captures it as a monitoring obligation, not a contract change).

429 responses include a `Retry-After: <seconds>` header.

The `SEARCH_API_MODE` env var gates all three endpoints uniformly:

- `open` (default) — all callers permitted.
- `localhost-only` — non-loopback callers receive a 403 `localhost_only` envelope.
- `disabled` — all callers receive a 503 `disabled` envelope unconditionally.

`enforce_mode_gate(request)` re-reads the env var on every call, so the value can be
flipped at runtime without a restart.

## Statelessness Contract

The three search-helper endpoints have ZERO references to `state.last_results`,
`state.current_search_query`, `state.parallels_results`, `app.storage` (any sub-key), or
`request.cookies`. Identical query strings produce identical bodies regardless of session.
This is verified at acceptance time by grep, and the constraint is restated in the handler
docstrings in [web/search_api.py](../web/search_api.py).

## What This API Is NOT

- Not a public API. No keys, no SLAs, no semver guarantees.
- Not browse-page parity. Only the subset of fields the skill ranks against is exposed; UI-only fields (corrections, comments, lists, puzzles) are not.
- Not a long-lived contract. Internal helper for v7.10 first-party tooling; the contract may change without warning.
- Not linked from `README.md` and not linked from the public site. External usage is not invited.
- For external research access to the Genizah corpus, use https://genizahsearch.com directly.

## See Also

- [skills/cairo-genizah-research/references/api_contract.md](../skills/cairo-genizah-research/references/api_contract.md) — locked consumer-facing envelope shapes used by the Claude skill.
- [CLAUDE.md](../CLAUDE.md) — server-side env-var declarations (the seven `SEARCH_API_*` and `POSTHOG_IP_SALT` vars in the Environment Variables block).
- [web/search_api.py](../web/search_api.py) — the route handlers and Pydantic models in source.
- [shared/api_errors.py](../shared/api_errors.py) — `ERROR_CODES` and `WARNING_CODES` frozensets and the `APIError` exception type.
- [shared/search_serializer.py](../shared/search_serializer.py) — the sole producer of envelope shapes (Phase 77 D-14).

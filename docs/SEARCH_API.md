# GenizahSearch Search-Helper API

> Contract `schema_version: 1` · Verified against site release 9.2.0 · Last updated: 2026-09-08

The **site release** above is the GenizahSearch version this document was last checked
against — it is NOT a separate API version, and it does not gate what is true. The
contract itself is versioned only by `schema_version` (see "Stability" below). Because
this file is edited in place rather than re-issued per release, the body can (and
routinely does) document features that shipped after the site release named above was
current — do not infer that a section is aspirational, unreleased, or newer-than-the-doc
merely because it postdates the header. If something here looks newer than the header,
trust the body: this exact failure mode (a stale header making shipped features read as
speculative) is why this note exists.

## Stability

This is a public research-automation API. We aim to keep this contract stable. Breaking changes (request shape, response envelope shape, error codes) will only ship on major website-version releases and will be announced in `CHANGELOG.md` and the `Changelog` section below. Additive changes (new optional fields, new optional request keys, new endpoints) may ship at any time.

**Interactive docs:** [`/api/docs`](https://genizahsearch.com/api/docs) (Swagger UI) · [`/api/openapi.json`](https://genizahsearch.com/api/openapi.json) (OpenAPI spec)

---

## Quick Start

All four endpoints return JSON, and every successful response carries `schema_version`. Beyond that the four are NOT uniform, so do not write one envelope parser for all of them:

- `/search` and `/parallels` carry `source`, `generated_at`, `count`, `total`, `warnings[]` and `results[]`.
- `/browse` carries `source` and `generated_at`, but returns its payload as top-level fields rather than a `results[]` list, and has no `count`/`total`.
- `/capabilities` is a fixed-shape descriptor: it has **no `source`** and no `generated_at`. Its top-level key set is exhaustively listed in its own section below.

Failures return `{"error": {"code": "...", "message": "..."}}` with an HTTP 4xx/5xx status -- but see "Error Envelope" for the two cases that carry something else.

Two details worth knowing before you write a client, because both have caught integrators out:

- **`request` is not a plain echo, and `/browse` does not have one.** On `/search` and `/parallels` it is the LAST key and reports the *effective* request -- values after defaulting and capping (`limit_effective`, `responsa_options_effective`, the resolved `passage_policy`), which is what you want for reproducing a result but is not what you sent. `/browse` has no `request` key at all; its equivalent is `locator`, the normalized, round-trippable address of the page.
- **A 5xx from the public deployment may not be JSON at all.** See "Edge-Timeout Ceiling" below. Branch on the HTTP status first and parse the envelope only if the body parses.

### Search for manuscripts

```bash
curl -s -X POST https://genizahsearch.com/api/search \
  -H "Content-Type: application/json" \
  -d '{"query": "אחד מי יודע", "search_mode": "variants", "limit": 5}' \
  | python -m json.tool
```

Response shape (truncated):

```json
{
  "schema_version": 1,
  "source": "search",
  "query": "אחד מי יודע",
  "mode": "variants",
  "count": 5,
  "total": 42,
  "warnings": [],
  "generated_at": "2026-09-08T14:25:36Z",
  "results": [
    {
      "uid": "IE167876818_P000001_FL167876820",
      "locator": {"sys_id": "990052326940205171", "volume_ie": "IE167876818", "p_num": "1"},
      "is_synthetic": false,
      "score": 32.2649,
      "shelfmark": "T-S AS 194.244",
      "title": "",
      "library": {"code": "CUL", "name": "Cambridge University Library"},
      "snippet": "...",
      "match_terms": ["..."],
      "image_url": "..."
    }
  ],
  "request": {"search_mode": "variants", "limit": 5, "limit_effective": 5, "filters": null}
}
```

There is no `rank` field -- results arrive in rank order, and `score` is the ranking
quantity. `count` is how many rows this response carries; `total` is how many matched.

Take a result's `uid` and `locator.sys_id` to drill down via `/api/browse`.

### Drill down to a manuscript page

```bash
curl -s "https://genizahsearch.com/api/browse?sys_id=990052326940205171&uid=IE167876818_P000001_FL167876820" \
  | python -m json.tool
```

Response shape (truncated):

```json
{
  "schema_version": 1,
  "source": "browse",
  "generated_at": "2026-09-08T14:26:51Z",
  "locator": {
    "uid": "IE167876818_P000001_FL167876820",
    "sys_id": "990052326940205171",
    "volume_ie": "IE167876818",
    "p_num": 1,
    "fl_id": "167876820"
  },
  "page_indexing": "1-based",
  "is_synthetic": false,
  "shelfmark": "T-S AS 194.244",
  "title": "",
  "library": {"code": "CUL", "name": "Cambridge University Library"},
  "text": "...",
  "text_source": "pgp_transcription",
  "text_truncated": false,
  "metadata": {},
  "image": {},
  "warnings": []
}
```

Returns transcription text (when available), PGP/FJMS/NLI metadata, and image URLs.

**These fields are flat.** There is no `manuscript` object, no `page` object, and no
`request` key -- `shelfmark`, `text` and `text_source` sit at the top level, the library
is `library: {code, name}` (not `library_code`), images are under `image`, and the page
address is `locator`. This document described a nested `manuscript`/`page` shape until
2026-09-08; it was never what the endpoint returned.

### Find composition parallels

```bash
curl -s -X POST https://genizahsearch.com/api/parallels \
  -H "Content-Type: application/json" \
  -d '{"text": "ואם בכי אבכה ומה ילד לי יגון", "chunk_size": 4, "mode": "variants"}' \
  | python -m json.tool
```

Response shape (truncated):

```json
{
  "schema_version": 1,
  "source": "parallels",
  "source_text": "...",
  "chunk_size": 4,
  "mode": "variants",
  "count": 108,
  "total": 108,
  "warnings": [],
  "generated_at": "2026-09-08T14:26:12Z",
  "results": [
    {
      "uid": "IE167876818_P000001_FL167876820",
      "locator": {"sys_id": "990052326940205171", "volume_ie": "IE167876818", "p_num": "1"},
      "is_synthetic": false,
      "score": 55.0,
      "shelfmark": "T-S AS 194.244",
      "library": {"code": "CUL", "name": "Cambridge University Library"},
      "snippet": "...",
      "matches": []
    }
  ],
  "request": {"mode": "variants", "chunk_size": 4, "method": "chunk", "limit_effective": 183}
}
```

Returns a `results[]` of manuscript groups that share sequential phrase-chunks with the
input text. Result items carry the same shape as `/search` results plus `matches`; there
is no top-level `sys_id` (it is inside `locator`) and no `matched_chunks`. With
`witnesses[]`, each group also carries `witness_fusion` -- see the multi-witness section
under `POST /api/parallels`.

Three numbers here are easy to misread, so they are worth stating outright:

- **`count` and `total` are ALWAYS equal on `/api/parallels`.** The serializer assigns
  both from the same expression. They differ on `/api/search` (where `total` is the full
  match count and `count` is how many rows this page carries), and a client that relies
  on `total > count` to detect "there is more" must not do so here.
- **`request.limit_effective` is a ROW count, not a group count**, so it is normally
  LARGER than `count`: 183 matching chunk-hit rows collapsed into 108 manuscript groups
  in the response above. It is not a page size and cannot be compared with `count`.
- **`results[]` is truncated in this example.** A real response carries `count` items.

### Error responses

All endpoints return errors in a single envelope. Example:

```json
{
  "error": {
    "code": "rate_limited",
    "message": "Rate limit exceeded. Try again in 60 seconds."
  }
}
```

See [Error Codes](#error-codes) below for the full list.

---

## Attribution & Citation

If you use GenizahSearch or its API in academic research, please cite the underlying data sources:

- **MiDRASH Transcriptions (primary text corpus):** Stoekl Ben Ezra, D., et al. (2025). *MiDRASH Automatic Transcriptions of the Cairo Geniza Fragments* [Data set]. Zenodo. [doi.org/10.5281/zenodo.17734473](https://doi.org/10.5281/zenodo.17734473)
- **Princeton Geniza Project (PGP):** Curated transcriptions, translations, and metadata — [geniza.princeton.edu](https://geniza.princeton.edu/)
- **Friedberg Jewish Manuscript Studies (FJMS / FGP):** Domain classifications, scholarly joins, bibliography, and catalog records — [fjms.genizah.org](https://fjms.genizah.org/)
- **National Library of Israel (NLI):** Manuscript images served via IIIF manifests

For full credits including hosting and development attribution, see the [Credits & Data section in the main README](../README.md#credits--data).

---

## Overview

This search-helper API exposes four endpoints — `POST /api/search`, `GET /api/browse`,
`POST /api/parallels`, and `GET /api/capabilities` — that together let a research consumer
execute a Tantivy keyword/Responsa search, drill down into a single manuscript page (with
PGP/FJMS/NLI enrichment), run a composition-parallels job over an arbitrary input text
(including the beta letter-level `method='passage'` engine and multi-witness search), and
cheaply discover which flag-gated features are live on a given deployment. A reference
consumer is the [`cairo-genizah-research` Claude skill](../skills/cairo-genizah-research/SKILL.md),
which demonstrates the full search → browse → rank workflow. The first three endpoints share
a common hardening shell: per-IP rate limiting (independent token bucket per endpoint, all
reading the same `SEARCH_API_RATE_LIMIT` env-var ceiling), a mode gate (`SEARCH_API_MODE`)
that permits run-time disabling or loopback-only restriction without restart, a uniform error
envelope, sampled PostHog observability, and a Phase-77-locked JSON envelope shape produced
by the sole serializer at `shared/search_serializer.py`. `/api/capabilities` shares the mode
gate and the uniform envelope but has its own rate-limit bucket and does no index/database
work. All four endpoints are stateless: the same request produces the same body regardless
of session.

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
    "library": ["CUL", "JTS"],
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
| `search_mode` | enum | `exact \| variants \| responsa \| title \| shelfmark \| fuzzy` | required | `regex` was intentionally dropped per Phase 81A D-09. `fuzzy` (added 2026-06) is the approximate / maximum-variant tier — bounded by `SEARCH_API_FUZZY_TIMEOUT` (default 110s as of 2026-09-08, down from 300s — see "Edge-Timeout Ceiling" below), not the interactive 30s baseline |
| `responsa_options` | object \| null | valid only when `search_mode="responsa"` | `null` | see sub-table below |
| `gap` | integer | must be `0` when `search_mode in {title, shelfmark}` | `0` | proximity slop for keyword search |
| `limit` | integer | `1..100` for non-fuzzy modes (`MAX_LIMIT=100`); `1..SEARCH_API_FUZZY_MAX_LIMIT` (default 500, max 2000) for `fuzzy` | `50` (fuzzy with no explicit limit widens to a recall-oriented default of 250) | P9X: fuzzy recall-over-precision — non-fuzzy boundary unchanged |
| `filters` | object \| null | all sub-fields nullable; unknown filter keys → 400 `invalid_request` (Pydantic `extra='forbid'`); unknown values → 400 `unresolvable_filter_value` | `null` | `library` (library codes, e.g. `["CUL","JTS","Oxford"]` — inclusion or exclusion filter depending on `library_filter_mode`; intersected with the other filters BEFORE the result cap; SEED-026); `library_filter_mode` (`"include"` default, omitted≡include — restrict to the given set; `"exclude"` — restrict to the complement, i.e. manuscripts whose `library_code` is NOT in the set; invalid value → 400 `invalid_request`; applies to both `POST /api/search` and `POST /api/parallels`; Phase 132 DMF-11); `domains`, `authors`, `works`, `materials` (string lists); `date_from`, `date_to` (int years) |

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
| `regex` value for `search_mode` | `invalid_request` | 400 | structurally rejected by Literal enum; never part of this API's public surface (dropped before the initial public release, Phase 81A D-09) |

### Response example

```json
{
  "schema_version": 1,
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
        "p_num": "3"
      },
      "score": 12.47,
      "shelfmark": "T-S 12.123",
      "title": "תשובה לרמב\"ם",
      "snippet": "...לפני הכתיבה תשובת הרמב\"ם...",
      "excerpt": "...הרמב\"ם השיב על השאלה הזאת...",
      "library": {"code": "CUL", "name": "Cambridge University Library"},
      "domains": ["Halakha"],
      "dating": "12th century",
      "image_url": "/api/nli_image_by_sysid/990001234560205171?page=2"
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
| `locator` | object | always present; EXACTLY `{sys_id, volume_ie, p_num}` — three keys, any of which may be null. **No `fl_id`**: that key exists only on `/api/browse`'s locator, and a client reading `result["locator"]["fl_id"]` here gets a `KeyError`, not `null`. `p_num` is a **string** (`"3"`), not an int — it is stringified in `parse_full_id_components`. Documented with a 4th `fl_id` key and an integer `p_num` until 2026-09-08; the code was right |
| `is_synthetic` | bool | Phase 85 SYNTH-06 (v7.11): `true` iff the row is a Phase-85 synthetic libraries.csv entry generated for an FJMS-only or CUDL-orphaned inventory; `false` for real NLI Alma records. Top-level (NOT nested under `locator`); additive — schema_version stays 1. Skill consumers should consider showing a "no NLI metadata" annotation when `true`. |
| `score` | float | Tantivy raw score |
| `shelfmark` | string | canonical shelfmark for display |
| `title` | string | manuscript title (often Hebrew) |
| `snippet` | string | pre-snippet (text before the hit) |
| `excerpt` | string | post-snippet (text including/after the hit) |
| `library` | object | `{code, name}` — e.g. `{"code": "CUL", "name": "Cambridge University Library"}`. **Not** a bare string, and **not** nested under a `metadata` object (there is no `metadata` key on a search/parallels result item; that shape was documented here until 2026-09-08 and never existed) |
| `domains` | list of string | possibly empty; top-level, not under `metadata` |
| `dating` | string \| null | top-level, not under `metadata` |
| `image_url` | string \| null | best-effort image URL; the server does not probe upstream availability. **Server-relative, same-origin** (e.g. `/api/nli_image_by_sysid/{sys_id}?page={n}`) — it proxies the upstream IIIF service rather than linking it, so resolve it against the API host and do not expect a `cudl.lib.cam.ac.uk` or NLI hostname |

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
| `limit_effective` | `min(req.limit, MAX_LIMIT)` for every mode EXCEPT `fuzzy`; for `search_mode='fuzzy'` it is the fuzzy-specific effective limit (the client's `limit` when supplied, else `SEARCH_API_FUZZY_MAX_RESULTS`) | post-cap value actually applied |
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
  "warnings": ["query_downgraded: Judeo-Arabic expansion disabled for this query."]
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
  "schema_version": 1,
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
  "library": {"code": "CUL", "name": "Cambridge University Library"},
  "text": "<full PGP transcription, capped at text_cap chars>",
  "text_source": "pgp_transcription",
  "text_truncated": false,
  "metadata": {
    "pgp":  {"description": "...", "tags": [], "document_type": null,
             "languages_primary": [], "languages_secondary": [],
             "doc_date_original": null, "doc_date_standard": null,
             "inferred_date_display": null, "pgpid": 12345, "pgp_url": "..."},
    "fjms": {"source_names": [], "has_measurements": false, "has_visual_suggestions": false},
    "nli":  {"physical_metadata": {"material": "...", "size": "...",
                                   "num_folio": "...", "num_bifolio": "..."},
             "folio": {"fl_id": "167876820", "folio_label": "1r", "thumb_url": null}}
  },
  "image": {
    "url": "/api/cambridge_image/990001234560205171?page=2",
    "provider": "cambridge",
    "sources": [
      {"url": "/api/cambridge_image/990001234560205171?page=2", "provider": "cambridge",
       "role": "iiif_proxy", "kind": "image", "fl_id": "FL999", "folio_label": null}
    ]
  },
  "warnings": []
}
```

### Top-level fields

| Name | Type | Notes |
| ---- | ---- | ----- |
| `schema_version` | integer | currently `1` |
| `source` | string | `"browse"` |
| `generated_at` | string | ISO-8601 UTC |
| `locator` | object | resolved locator (all five fields) |
| `is_synthetic` | bool | Phase 85 SYNTH-06 (v7.11): `true` iff the resolved row is a Phase-85 synthetic libraries.csv entry. Top-level (NOT nested under `locator`); additive — schema_version stays 1. When `true`, `metadata.nli` will typically be `null` and the image will fall back to CUDL when available. |
| `shelfmark` | string | canonical shelfmark |
| `title` | string | manuscript title |
| `library` | object | `{code, name}` — e.g. `{"code": "CUL", "name": "Cambridge University Library"}`. Documented as two FLAT keys `library_code`/`library_name` until 2026-09-08; the endpoint has only ever returned the nested object |
| `text` | string | transcription, capped at effective `text_cap` |
| `text_source` | enum | see below |
| `text_truncated` | bool | `true` when `text` was clipped to `text_cap`. The same event also appends a `transcription_truncated` entry to `warnings[]` — two channels, one fact |
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
  "max_freq": 50,
  "boundary_mode": "combined",
  "filters": {
    "domains": ["Liturgy"]
  }
}
```

### Request fields

| Name | Type | Constraint | Default | Notes |
| ---- | ---- | ---------- | ------- | ----- |
| `text` | string \| null | 1..20000 chars (post-strip; `COMPOSITION_LENGTH_CAP=20000`; empty → `composition_required`; over cap → `composition_too_long`) | required *unless* `witnesses` is sent | Omit it ONLY when sending `witnesses` instead. Sending both → 400 `witnesses_and_text_conflict`; sending neither → 400 `composition_required`. |
| `chunk_size` | integer | `2..20` | `5` | size of sliding chunks |
| `mode` | enum | `exact \| variants \| fuzzy` | `"exact"` | **field name is `mode`, not `search_mode`** — see "Naming Inconsistency" |
| `max_freq` | float \| null | `>= 1`. A **document count**, not a ratio: a chunk matching more than `max_freq` documents is treated as too common. `null` disables high-frequency filtering | `null` | **Effective range is `[1, 50)`.** The engine tests `len(hits) > max_freq` against a per-chunk retrieval hard-capped at 50 hits, so any `max_freq >= 50` can never fire and behaves exactly like `null`. It is therefore not a corpus frequency: it counts hits inside a truncated top-50 and cannot tell a chunk in 51 manuscripts from one in 5,000. A value below 1 would discard every chunk that matches anything, so such values are rejected with `invalid_request` rather than silently returning an empty result set. Documented as a `0.0-1.0` ratio until 2026-08-24 — the docs were wrong, not the code |
| `boundary_mode` | enum | `full \| boundary \| combined` | `"full"` | only boundary knob exposed since the initial public release (Phase 80 D-03) |
| `filters` | object \| null | reuses Phase 78 `FiltersModel` verbatim | `null` | same shape as `/api/search.filters` |
| `method` | enum | `chunk \| passage` | `"chunk"` | Phase 145 (beta). `chunk` is the pre-Phase-145 sliding-window Tantivy engine described above, byte-for-byte unchanged. `passage` is a character-level matching engine, tolerant of OCR/HTR noise and reflowed line breaks — see "`method='passage'` (beta)" below. |
| `witnesses` | array \| null | 1..`SEARCH_API_PASSAGE_MAX_WITNESSES` (default 25) objects; `method='passage'` only | `null` | Several witnesses of ONE work, each searched **separately** and merged by rank fusion — see "Multi-witness search" below. Mutually exclusive with `text`. |
| `sort` | enum \| null | `fused \| best_match \| witness_count`; requires `witnesses` | `null` (→ `fused`) | Group ordering for a multi-witness search. Without `witnesses` → 400 `sort_requires_multi_witness`. |

The Lab Engine extended-parallels path is OUT OF SCOPE for this API (Phase 80 D-02).

### Multi-witness search (`witnesses`, beta)

One work survives in many manuscripts, and no single witness of it retrieves every other.
Measured through the shipped code at policy `max-40+short`, against the 614 Birkat Hamazon
census manuscripts that have any indexed text: the best single witness finds **348 (56.7%)**,
while the same 17 searched **separately and merged** find **455 (74.1%)**. On Megillat Antiochus
(design harness), a seed plus three rounds of promoted witnesses took frontier coverage from 2
to 9 of 20.

Send them as `witnesses` instead of `text`:

```json
{
  "method": "passage",
  "witnesses": [
    {"label": "T-S H6.37", "text": "..."},
    {"label": "Or. 1080", "text": "..."},
    {"label": "promoted from results", "raw_header": "990001234560205171_IE12345_P00001_FL678"}
  ]
}
```

Each entry needs **exactly one** of `text` (a pasted witness, capped at
`COMPOSITION_LENGTH_CAP`) or `raw_header` (a page header exactly as carried on every result
row, resolved server-side to that page's text — which keeps recursive requests small).
`label` is echoed back and never used for matching. Both, or neither, → 400
`invalid_request`.

**Do not concatenate witnesses into `text` yourself.** The passage engine spends a per-query
posting budget, so one long joined query starves: the 17 witnesses joined admit 2.4% of their
own postings and reach **48.2%** of the reachable census — *worse than the best single witness
(56.7%)* — against 74.1% fused, and every concatenated recursion round scored below the seed
alone. This is specific to `method='passage'`; the chunk engine decomposes a query into
independent per-chunk lookups with no shared budget, where concatenation and union were
measured to return the identical manuscript set. `witnesses` with `method='chunk'` is
therefore rejected with 400 `witnesses_require_passage_method` rather than quietly accepted.

**Ranking.** Results are merged by Reciprocal Rank Fusion (k=60), not by score. A passage
score counts matched *query* letters, so a long witness mechanically outscores a short one
for reasons unrelated to match quality; RRF ties sum-of-scores at similar witness lengths and
beats it decisively at mixed ones. Each group in `results[]` gains a `witness_fusion` object:

```json
"witness_fusion": {
  "witness_count": 4,
  "witness_ids": ["w1", "w3", "w5", "w7"],
  "fusion_score": 0.0621,
  "best_witness_score": 880.0
}
```

`witness_count` is the **union** of witnesses across the group's rows — a manuscript found on
three pages by one witness is one witness. Witness ids are assigned positionally (`w1`, `w2`,
…) in request order.

`filter_text` routes a **row**, not a record. When one witness matches a manuscript on text you
declared as a known source and another matches it on text you did not, the manuscript stays in
`results` — suppressing it would make the filter *stricter* the more witnesses you add — and its
`witness_count`, `witness_ids`, `fusion_score` and `best_witness_score` count **both** witnesses.
The rendered row is still supplied by a witness whose match was *not* filtered, so the
highlighted span is never text you asked to discount. Evidence from the eligible contributor,
arithmetic over all of them.

`best_witness_score` is the strongest single match **any** witness made on this manuscript. It
is reported here, and not as a row's `score`, because it may belong to a witness whose evidence
no returned row renders: each row carries the label, highlighted span and `score` of the one
witness that ranked it best, and a score borrowed from another witness would describe text the
response does not contain.

Each `sort` value orders the groups by a **named field of that same response**, so a consumer
can reproduce any of them locally:

| `sort` | orders groups by | ties broken by |
|---|---|---|
| `fused` (default) | `witness_fusion.fusion_score` | the grouping order — this is a no-op, the array already arrives in it |
| `best_match` | `witness_fusion.best_witness_score` — the strongest single match any witness made | summed `score` |
| `witness_count` | `witness_fusion.witness_count` | summed `score` |

`sort` reorders the groups the response already contains — it does not re-select them. The
200-group cap is applied on `fusion_score` first, so `best_match` and `witness_count` rank the
fused top 200 and cannot surface a manuscript the fusion had already cut. The cap has to rank by
something, and fusion is the ranking that chose the rows; widen the result set with more or
better witnesses, not with `sort`.

`best_match` deliberately does **not** read the rows' `score`. That field carries the *rank
winner's* matched letters, so ordering by it reproduces `fused` under a second name rather than
answering "which manuscript holds the strongest match" — a manuscript ranked first by a short
witness and thirty-first by a long one renders the short witness's score.

**`score` and `sort_score` stay matched letters on a multi-witness response**, exactly as on
every other method — one scale everywhere. The array is ordered by
`witness_fusion.fusion_score` instead, because that is the ranking that actually selected the
rows. So a consumer that re-sorts a multi-witness response by `score` will get a *different*
order than the one returned; that is deliberate. Use `witness_fusion.fusion_score` to reproduce
the returned order.

**Partial resolution is normal.** A `raw_header` that does not resolve (or resolves to a page
over the length cap) is **skipped and reported**, never fatal — rejecting a 17-witness
request over one stale reference would waste the sixteen you can still have. The response
carries a `witness_ref_unresolved` warning naming which failed and why, and
`request.witnesses` reports `requested` vs `searched`. The request fails (400
`witnesses_required`) only when *not one* entry resolves.

**Budget.** One HTTP request is one concurrency slot, with the witnesses searched
sequentially inside it, under the same `SEARCH_API_PASSAGE_TIMEOUT` ceiling as a
single-witness request. The witness **cap** — not a raised ceiling — is the control on cost:
on timeout the slot keeps its executor thread until the work really finishes, so a longer
ceiling would let timed-out requests occupy every slot while clients retry. A witness list
whose projected cost could not fit the ceiling is refused up front with 400
`too_many_witnesses`, before any slot is acquired.

Gated by `PASSAGE_MULTI_WITNESS_ENABLED` **and** `passage_available()`; when off, 503
`passage_multi_witness_unavailable`. Witness *texts* are never echoed back — only counts,
ids, labels, kinds and resolution status.

A cheap way to check before spending a request: `GET /api/capabilities` (below) reports
`features.passage_multi_witness` without the cost of a 503 round-trip. It is a convenience,
not the authority — a client must still stay correct against a deployment with no
capabilities endpoint, or one whose gate flips between the two calls, so treat this 503 as
the ground truth and `/api/capabilities` as a way to avoid asking for it needlessly.

### `method='passage'` (beta, Phase 145)

An alternative matching engine over the SAME response shape (`results[]`/`filtered[]`/
`matches[]`/`locator` unchanged) — a client that does not read `request.method` cannot tell
the two apart from the envelope shape alone.

- **Availability.** Requires the deployment to have `PASSAGE_PARALLELS_ENABLED=1` AND a
  successfully-loaded passage index (`web/passage_assets.py::passage_available()`). When
  either is false, `method='passage'` returns 503 `passage_unavailable` — never a silent
  fallback to `chunk`. `GET /api/capabilities` (below) reports `features.passage` and
  `parallels.methods` as a cheap pre-check — but the 503 stays authoritative, since a
  deployment may have no capabilities endpoint, or its gate may change between the two
  calls.
- **Genizah-only scope.** The passage index is built ONLY from the Genizah transcription
  corpus; it holds zero records for `LOCAL` (My-Library) provenance. Requesting
  `method='passage'` together with `filters.library` containing `"LOCAL"` under the default
  `library_filter_mode="include"` returns 400 `passage_scope_unsupported` rather than a
  silently-empty result that would look identical to "no matches found". Excluding `"LOCAL"`
  (`library_filter_mode="exclude"`) is a no-op for passage and is NOT rejected.
- **Display name.** The web GUI presents this method as “Letter-level search” (owner naming, 2026-08-23) and selects it by default when the index is available; `method='passage'` remains the stable wire value — API clients should never parse display names.
- **Span-shaped `matches[]`.** Each accepted contiguous span of matched text on a manuscript
  page is one `matches[]` entry, so the number of spans is `len(item["matches"])` — there is
  **no `chunk_count` field in the response** on either method (it exists inside the engine
  and is not serialized; documented as a response field here until 2026-09-08). Unlike the
  incumbent's Tantivy-hit-derived count, one entry is one accepted span. Each entry is
  exactly `{chunk_index, source_chunk_text, manuscript_snippet, score}`;
  `chunk_index` is the ordinal of the span's position within the
  submitted `text`, comparable across different matched manuscripts the same way the
  `chunk` engine's sliding-window index is). `score` is the span's matched-letter count,
  not a Tantivy relevance score — and it is **NOT comparable to the `chunk` engine's
  score**: passage counts normalized letters on the *query* side of the match (whitespace,
  marks, punctuation and digits removed), while `chunk` counts raw characters of the
  *manuscript* side's merged spans. The two are measured on opposite sides of the match in
  different units, so no conversion factor exists. A client combining the two methods must
  rank *within* each method (by rank or per-method quantile) and never pool or sort a mixed
  list by raw `score`. `manuscript_snippet` / `source_chunk_text` are still `*term*`-marked
  highlight text (same markup the `chunk` engine emits, including sanitizing a literal `*`
  in the source manuscript text so it is never mistaken for that markup), built via a
  bounded re-normalization. The row set this engine renders and the row set it returns are
  always identical — grouped and capped at 200 manuscripts by the same rule `/api/parallels`
  applies to every method, applied internally rather than left to the caller.
- **No silently-ignored knobs.** `chunk_size`, `mode` and `max_freq` have no passage-matching
  equivalent (no sliding-window chunk, no morphological-variant matching, no per-chunk
  frequency signal), and neither does `boundary_mode` other than `"full"` (no cross-
  paragraph/token-boundary concept over a letter stream). Rather than silently ignoring a
  non-default value while the client believes it was applied, `method='passage'` together
  with a non-default `chunk_size` (≠5), `mode` (≠`"exact"`), `max_freq` (≠`null`), or
  `boundary_mode` (≠`"full"`) returns 400 `passage_option_unsupported`. The response envelope
  correspondingly nulls out `chunk_size`/`mode`/`max_freq`/`boundary_options` (both at the
  top level and inside `request`) and adds `request.passage_policy` — the actual policy
  (`policy_id`, `min_span`, `regime`, `posting_budget`, ...) that drove the search — so nothing
  in the envelope reads as "this knob was applied" when it was not.
- **Multi-witness.** One work can be searched with several of its witnesses at once — see
  the next section.
- **Filtering.** `filters` (domains/authors/works/materials/dates/other libraries) applies as
  a plain sys_id restriction, same as `chunk`. `filter_text` (the "known source text" a row's
  match can be checked against, routing it to `filtered` rather than `results[]`) is a
  web-page-only concept (the page's "Filter Sources" panel) that this endpoint never
  populates — but `filtered[]` is **not** therefore always `[]`, as this section claimed
  until 2026-09-08. The mandatory post-verify duplicate-photography pass DEMOTES rows into
  `filtered[]` (each carrying `filter_reason: "duplicate_photography"`) rather than dropping
  them, so a skeptical reader can still inspect what was set aside; the count ships in a
  `duplicate_photography_demoted` warning. A row whose display-text lookup
  fails is DROPPED (never returned in either bucket) and counted in a `passage_text_lookup_
  failed` warning (see Warnings Array) rather than coming back with blank text.
- **Timeout.** Its own ceiling, `SEARCH_API_PASSAGE_TIMEOUT` (default 30s; see Environment
  Variables below) — separate from `SEARCH_API_PARALLELS_TIMEOUT`, since the two engines'
  cost models are unrelated. Exceeding it returns 504 `core_timeout`.
- **Busy envelope.** Passage requests are gated by their OWN bounded concurrency budget
  (`SEARCH_API_PASSAGE_CONCURRENCY`, default 4) — separate from the `chunk`/`variants`/
  `fuzzy` heavy-mode budget (`SEARCH_API_HEAVY_CONCURRENCY`). Exhausting it returns 503
  `passage_search_busy` with a `Retry-After` header, mirroring `heavy_search_busy`'s shape.

### Response example

```json
{
  "schema_version": 1,
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
        "p_num": "3"
      },
      "score": 8.42,
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
    "max_freq": 50,
    "boundary_options": {
      "boundary_mode": "combined",
      "boundary_delimiter": "...",
      "boundary_boost": 1.0,
      "min_boundary_matches": 1,
      "min_delimiter_distance": 0
    },
    "method": "chunk",
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
more than 200 groups, the top 200 are returned and the **bare string** `"truncated_to_200"`
is appended to `warnings[]` — not an object. This section described it as
`{"code": "truncated_to_200", ...}` until 2026-09-08, contradicting the Warnings Array
table, which had it right.

**`warnings[]` is a mixed-type array.** Most entries are objects with a `code` key, but
`truncated_to_200` and `query_downgraded: <message>` are plain strings. A client must
therefore type-check each entry — `w["code"]` over this array raises `TypeError` on the
string entries. See the Warnings Array table, which marks the shape of every code.

### Request echo: 7 keys for `chunk`, 9 for `passage`, 11 with witnesses

The key COUNT is method-dependent — this section claimed a flat "exactly seven keys (no
more, no fewer)" until 2026-09-08, which was true only for `method='chunk'`:

| Configuration | Keys | Added |
| ------------- | ---- | ----- |
| `method='chunk'` (default) | **7** | — the byte-for-byte stable shape; unchanged since Phase 145 added `method` to the original six |
| `method='passage'`, no witnesses | **9** | `passage_policy`, `passage_report` |
| `method='passage'` with `witnesses[]` | **11** | `passage_policy`, `passage_report`, `witnesses`, `sort` |

Both passage keys are added together in one branch, so 8 is not a reachable count (the
source comments in `web/search_api.py` said "8-key" until 2026-09-08 and undercounted by
one). A `chunk` caller's echo is untouched by any of this.

Explicitly NOT echoed on any path: `search_mode` (parallels uses `mode`), `gap` (a
search-only concept), `responsa_options` (parallels never used Responsa).

| Echo key | Source | Notes |
| -------- | ------ | ----- |
| `mode` | `req.mode` | NOT `search_mode` (per Phase 81A D-07) |
| `chunk_size` | `req.chunk_size` | unmodified |
| `max_freq` | `req.max_freq` | `null` permitted |
| `boundary_options` | server-resolved 5-key dict (`boundary_mode`, `boundary_delimiter`, `boundary_boost`, `min_boundary_matches`, `min_delimiter_distance`) | includes service-layer defaults |
| `method` | `req.method` | Phase 145; `"chunk"` when omitted — always present, so a caller never has to guess which engine served the response |
| `limit_effective` | `len(bundle.main_results)` | post-cap **ROW** count — the raw chunk-hit rows behind the ≤200 kept groups. It is NOT the group count (which is `count`/`total`) and is normally LARGER: a live response showed `limit_effective: 183` with `count: 108`. Described as a group count here until 2026-09-08 |
| `filters` | model-dumped `FiltersModel` (exclude_none) or `null` | |
| `passage_policy` | resolved passage policy | `method='passage'` ONLY (a present key, not a null value). The knobs that ACTUALLY drove the search (`policy_id`, `min_span`, `regime`, `posting_budget`, …); `mode`/`chunk_size`/`max_freq`/`boundary_options` are nulled out on this path because the passage engine never reads them |
| `passage_report` | `QueryReport.as_dict()` | `method='passage'` ONLY. Budget/truncation accounting (postings, candidates, verify counts) for evaluation consumers who need more than the truncated-or-not warning. Undocumented in this file until 2026-09-08 |
| `witnesses` | `{requested, searched, labels[]}` | only when `witnesses[]` was sent. Counts and LABELS only — a witness's text is never echoed back |
| `sort` | `req.sort or "fused"` | only when `witnesses[]` was sent; echoes what was ASKED for (see the `sort_not_applied` warning for what was done) |

## Endpoint: GET /api/capabilities

A client that wants to know which flag-gated features are live on a deployment has
historically had to either spend a heavy request and read its 503, or guess from this
document alone (which cannot say whether a given deployment currently has, say,
`PASSAGE_PARALLELS_ENABLED=1`). `GET /api/capabilities` answers that cheaply: **no index
load, no database, no search** — it reads live config and returns a small JSON object.

It shares the same mode gate and error envelope as the other three endpoints — `enforce_mode_gate`
applies, so `SEARCH_API_MODE=disabled` still returns 503 `disabled` and `localhost-only`
still returns 403 `localhost_only` for non-loopback callers — and it is rate-limited through
its own bucket (`SEARCH_API_RATE_LIMIT`, same ceiling as the other three buckets, tracked
independently — see Rate Limiting & Buckets below), so hammering `/api/capabilities` cannot
starve `/api/search`, `/api/browse`, or `/api/parallels` and vice versa.

**This endpoint never advertises a feature whose gate is closed.** If `passage_available()`
is `false` on this deployment, `"passage"` is absent from `parallels.methods`, full stop —
an over-advertising capabilities endpoint would actively mislead a client, which is worse
than not having one. Treat every value below as a snapshot of the moment the request was
served, not a promise: a deployment's flags and env vars can change between this call and
the next request you make.

### Request

No parameters, no body.

```bash
curl -s https://genizahsearch.com/api/capabilities | python -m json.tool
```

### Response example

```json
{
  "schema_version": 1,
  "request": {},
  "api_version": "9.2.0",
  "endpoints": ["/api/search", "/api/browse", "/api/parallels", "/api/capabilities"],
  "search_modes": ["exact", "variants", "responsa", "title", "shelfmark", "fuzzy"],
  "features": {
    "passage": true,
    "passage_multi_witness": true
  },
  "parallels": {
    "methods": ["chunk", "passage"],
    "multi_witness": true,
    "max_witnesses": 25,
    "max_witness_chars": 20000,
    "sorts": ["fused", "best_match", "witness_count"]
  },
  "limits": {
    "search_requests_per_minute": 120,
    "browse_requests_per_minute": 120,
    "parallels_requests_per_minute": 120,
    "capabilities_requests_per_minute": 120,
    "heavy_concurrency": 2,
    "passage_concurrency": 4
  },
  "timeouts": {
    "search_core": 30.0,
    "variants": 60.0,
    "fuzzy": 110.0,
    "parallels": 110.0,
    "passage": 30.0,
    "browse": 1.0,
    "browse_core": 2.0,
    "browse_core_warmup": 45.0
  }
}
```

### Field semantics

| Field | Meaning |
| ----- | ------- |
| `schema_version` | Always `1` — this endpoint is an additive change under the Stability commitment above, so adding it does not move the version. |
| `request` | Always `{}`. Present for uniformity with `/search` and `/parallels`, which carry an effective-request echo here; `/browse` has no `request` key (it uses `locator`). This endpoint takes no input, so there is nothing to echo. |
| `api_version` | The site release this deployment is running (`version.py::APP_VERSION`), read live — NOT a hardcoded string, and NOT the same thing as `schema_version`. |
| `endpoints` | The four public paths as absolute `/api/...` strings — what a client actually calls, regardless of what path prefix this router happens to be mounted under. |
| `search_modes` | The live `search_mode` enum accepted by `POST /api/search`, in the order documented above. |
| `features.passage` | `web.passage_assets.passage_available()` — both `PASSAGE_PARALLELS_ENABLED=1` AND a successfully-loaded passage index. |
| `features.passage_multi_witness` | `web.passage_assets.passage_multi_witness_available()` — both `PASSAGE_MULTI_WITNESS_ENABLED=1` AND `features.passage`. |
| `parallels.methods` | `["chunk"]` when `features.passage` is `false`; `["chunk", "passage"]` when it is `true`. Never lists `"passage"` on a deployment where it would 503. |
| `parallels.multi_witness` | Mirrors `features.passage_multi_witness`. |
| `parallels.max_witnesses` | The **effective** cap — the largest witness count a request would actually be accepted with, which is the lower of `SEARCH_API_PASSAGE_MAX_WITNESSES` (default 25) and what the passage ceiling can afford. Two gates reject a witness list, not one: the handler also refuses on projected cost (`witnesses × 0.75 s > SEARCH_API_PASSAGE_TIMEOUT` → 400 `too_many_witnesses`), so lowering the passage timeout lowers the real cap without touching the cap variable. At defaults the projection binds nothing (30 / 0.75 = 40, above 25); at a 10 s ceiling the real cap is 13. `0` is a truthful answer when the ceiling cannot afford even one witness. |
| `parallels.max_witness_chars` | The per-witness **length** cap (`MAX_WITNESS_CHARS`, 20,000 characters). A separate rejection from the count cap above — one over-long witness is a 400 `witness_too_long` regardless of how many were sent. |
| `parallels.sorts` | `[]` when `parallels.multi_witness` is `false` (advertising sort values that `sort_requires_multi_witness` would reject without `witnesses` would be a lie); the three live `sort` values otherwise. |
| `limits.*` | Live per-bucket rate ceilings (each reads `SEARCH_API_RATE_LIMIT`, tracked independently per endpoint — see Rate Limiting & Buckets) and the two concurrency budgets (`SEARCH_API_HEAVY_CONCURRENCY`, `SEARCH_API_PASSAGE_CONCURRENCY`). The concurrency values are what a request arriving **now** would face, resolved from the environment on each call — not the size the live semaphore was last built with, which lags a configuration change until the next acquisition. |
| `timeouts.*` | Live values of every per-mode timeout documented in Environment Variables below, in seconds (floats). `fuzzy` and `parallels` reflect the 110 s edge-timeout ceiling — see "Edge-Timeout Ceiling" below — not a hardcoded 300 s. |
| `timeouts.browse_core_warmup` | The cold-start ceiling for `/api/browse` (`SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT`, default 45 s). **`browse_core` does not apply while the provider is warming**: a freshly restarted deployment substitutes this larger budget until its browse map is loaded, so a browse can legitimately take far longer than `browse_core` right after a restart. Both are reported because they answer different questions — size a socket timeout from the larger, reason about steady-state latency from the smaller. |

The JSON above shows this deployment's *current defaults*; every value is read from the
live resolvers at request time, not typed in as a literal, specifically so this response
cannot drift out of sync with reality the way the header of this document once did (see
"Edge-Timeout Ceiling" below for that history).

## Naming Inconsistency: parallels.mode vs search.search_mode

`/api/search` uses `search_mode` (the post-Phase-81A name). `/api/parallels` continues to
use `mode`. This is intentional debt locked by Phase 81A D-07 at the initial public release: renaming the parallels
field would have broken Phase 80 tests with no consumer-visible benefit, since the
parallels enum is a different set of values (`exact | variants | fuzzy`) than the search
enum (`exact | variants | responsa | title | shelfmark | fuzzy`). Future versions may unify the
field name; consumers should code defensively against both names.

The two enums share `exact`, `variants` and `fuzzy`. A consumer must use the correct field
name per endpoint and must not assume value-set equivalence: `responsa`/`title`/`shelfmark`
exist only in `/api/search`, and no value exists only in `/api/parallels`. (This paragraph
said the enums "share only `exact` and `variants`" and that `fuzzy` was parallels-only
until 2026-09-08 — contradicting the two enum lists immediately above it.)

## Drill-Down Locator Round-Trip

The `locator` field on `/api/search` and `/api/parallels` result items is shaped:

```json
{
  "sys_id": "990001234560205171",
  "volume_ie": "IE12345",
  "p_num": "3"
}
```

Three keys, and `p_num` is a string. **`fl_id` is not one of them** — it appears only on
`/api/browse`'s locator (which does return five keys, `p_num` there being an int) and as a
browse QUERY parameter. This block showed a 4-key locator with an integer `p_num` until
2026-09-08, which is why the round-trip below deliberately uses `uid` rather than `fl_id`.

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

The four search-helper endpoints all wrap their errors in a uniform JSON envelope.
Existing legacy `/api/*` routes (image proxies, puzzle uploads, NLI proxies) keep their
original FastAPI default behavior — this envelope applies ONLY to `/api/search`,
`/api/browse`, `/api/parallels`, and `/api/capabilities`. This is exactly the failure mode
"Edge-Timeout Ceiling" (below) warns about at the transport level: even where this API's
own handler is guaranteed to emit this envelope, an edge proxy sitting in front of the
deployment is not part of this contract and may return a non-JSON body of its own.

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
- HTTP status varies (see Error Codes table); never raw FastAPI 422 dumps for these four
  endpoints (Phase 78 Concern #2 — handlers wrap their own bodies; no global exception
  handlers installed).
- HTTP 429 carries a `Retry-After: <seconds>` header alongside the `rate_limited` body.
- **A structural validation failure adds a third key.** When the body fails Pydantic
  validation outright (unknown top-level field, wrong type, bad `Literal`), the envelope
  carries `fields` alongside `code` and `message` — a list of the offending dotted paths.
  A live example, from `POST /api/parallels` with an unrecognized `limit` key:
  `{"error": {"code": "invalid_request", "message": "Extra inputs are not permitted",
  "fields": ["limit"]}}`. Treat `fields` as present-sometimes.
- **Two failures never reach the envelope at all,** because they are decided before any
  handler runs. Using the wrong HTTP method returns Starlette's own
  `{"detail": "Method Not Allowed"}` with status 405 — `/api/search` and `/api/parallels`
  are POST-only, `/api/browse` and `/api/capabilities` GET-only — and an edge proxy
  timing out in front of the deployment returns whatever that proxy serves (see
  "Edge-Timeout Ceiling"). Branch on the HTTP status BEFORE parsing the body.

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
| `limit_too_high` | 400 | `req.limit > MAX_LIMIT` (100) for non-fuzzy modes, or `req.limit > SEARCH_API_FUZZY_MAX_LIMIT` for fuzzy. Pydantic still rejects `limit > 2000` (FUZZY_HARD_MAX) with `invalid_request`. **Contract change (heavy-tier release):** a non-fuzzy `limit` in `101..2000` now returns `limit_too_high` (was `invalid_request` when the Pydantic bound was `le=100`). The request is still rejected with HTTP 400; only the error `code` changed (now the more specific `limit_too_high`). Clients that branch on `invalid_request` for over-limit values should also accept `limit_too_high`. |
| `unknown_filter_key` | 400 | reserved in `ERROR_CODES` for future use; in practice, an unknown `filters` key is caught first by Pydantic `extra='forbid'` and returns `invalid_request` (not this code) |
| `unresolvable_filter_value` | 400 | filter value not in vocabulary |
| `filter_vocabulary_unavailable` | 503 | vocabulary loader failed (Phase 78 R2-#3 fail-closed) |
| `rate_limited` | 429 + `Retry-After` | per-IP sliding window exhausted on the endpoint's own bucket |
| `heavy_search_busy` | 503 + `Retry-After` | heavy-mode (variants/fuzzy/parallels) concurrency budget (`SEARCH_API_HEAVY_CONCURRENCY`, default 2) exhausted; fail-fast instead of queueing unboundedly; retry shortly |
| `disabled` | 503 | `SEARCH_API_MODE=disabled` |
| `localhost_only` | 403 | `SEARCH_API_MODE=localhost-only` and request from non-loopback |
| `internal_error` | 500 | unhandled exception in handler |
| `locator_conflict` | 400 | uid malformed; uid disagrees with sys_id/p_num/fl_id/volume_ie |
| `manuscript_page_not_found` | 404 | core fetch returned `bundle.page is None`; or post-resolution `bundle.page.uid != requested_uid` |
| `core_timeout` | 504 | `/api/browse`: BrowsePage exceeded `SEARCH_API_BROWSE_CORE_TIMEOUT` (2.0s). `/api/search`: per-mode ceiling exceeded — exact/title/shelfmark/responsa→30s (`SEARCH_API_CORE_TIMEOUT`), variants→60s (`SEARCH_API_VARIANTS_TIMEOUT`), fuzzy→110s (`SEARCH_API_FUZZY_TIMEOUT`, lowered from 300s 2026-09-08 — see "Edge-Timeout Ceiling" below). `/api/parallels`→110s (`SEARCH_API_PARALLELS_TIMEOUT`, likewise lowered from 300s) for `method='chunk'`, 30s (`SEARCH_API_PASSAGE_TIMEOUT`, unchanged) for `method='passage'`. Message names the ceiling and mode. |
| `composition_required` | 400 | `text.strip()` empty |
| `composition_too_long` | 400 | `len(text.strip()) > 20000` |
| `passage_unavailable` | 503 | Phase 145: `method='passage'` requested but `PASSAGE_PARALLELS_ENABLED` is off, or the passage index did not load |
| `passage_scope_unsupported` | 400 | Phase 145: `method='passage'` + `filters.library` includes `"LOCAL"` in include mode — the passage index holds no Local-corpus records |
| `passage_option_unsupported` | 400 | Phase 145: `method='passage'` + `boundary_mode` other than `"full"` — passage-matching has no cross-paragraph/token-boundary concept over a letter stream |
| `passage_multi_witness_unavailable` | 503 | `witnesses` requested but `PASSAGE_MULTI_WITNESS_ENABLED` is off (or passage itself is unavailable) |
| `witnesses_require_passage_method` | 400 | `witnesses` sent with `method='chunk'` — the chunk engine has no per-query budget to starve, so joining witnesses into `text` there is equivalent and cheaper |
| `witnesses_and_text_conflict` | 400 | both `text` and `witnesses` supplied — never silently pick one |
| `witnesses_required` | 400 | `witnesses` supplied but not one entry resolved to searchable text; the message names each failure and its reason |
| `too_many_witnesses` | 400 | more than `SEARCH_API_PASSAGE_MAX_WITNESSES` entries, OR a list whose projected cost exceeds `SEARCH_API_PASSAGE_TIMEOUT` |
| `witness_too_long` | 400 | a **pasted** witness exceeds `COMPOSITION_LENGTH_CAP`; a resolved `raw_header` over the cap is skipped-and-reported instead |
| `sort_requires_multi_witness` | 400 | `sort` sent without `witnesses` |
| `passage_search_busy` | 503 + `Retry-After` | Phase 145: passage-matching concurrency budget (`SEARCH_API_PASSAGE_CONCURRENCY`, default 4) exhausted; fail-fast; retry shortly |

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
| `passage_text_lookup_failed` | parallels (`method='passage'`) | one or more matched rows were DROPPED (never returned in `results[]`/`filtered[]`) because their display-text lookup failed -- never a silently blank row. Object-shaped (not a bare string, unlike `truncated_to_200`): `{"code": "passage_text_lookup_failed", "count": N}`. |
| `witness_ref_unresolved` | parallels (`witnesses`) | one or more witnesses were SKIPPED because their `raw_header` did not resolve, or resolved to a page over the length cap. Object-shaped: `{"code": "witness_ref_unresolved", "count": N, "witnesses": [{"id", "label", "reason"}]}` where `reason` is `not_found` \| `bad_ref` \| `empty` \| `too_long`. The other witnesses still ran. |
| `witness_duplicate_skipped` | parallels (`witnesses`) | an entry resolved to text an earlier witness had already supplied, and was skipped rather than searched again. Deduplication is on the RESOLVED TEXT, so it also catches two different `raw_header`s naming the same page, and a `text` identical to a resolved reference. Searching it would spend a witness slot to re-derive rows already in hand and then count them twice — `fuse()` counts contributors positionally, so the same witness supplied twice inflates `witness_count` and `fusion_score` and reorders results. Object-shaped: `{"code": "witness_duplicate_skipped", "count": N, "witnesses": [{"id": "...", "label": "...", "duplicate_of": "..."}]}`. NOT reported as `witness_ref_unresolved` — it resolved. |
| `passage_results_truncated` | parallels (`method='passage'`) | the candidate pool or the verify pass hit its budget, so the result set is not exhaustive. Object-shaped and self-describing: `{"code": "passage_results_truncated", "candidates_truncated": bool, "verify_truncated": bool, "verified": N, "candidates": N}`. Undocumented in this table until 2026-09-08 |
| `duplicate_photography_demoted` | parallels (`method='passage'`) | N rows were moved from `results[]` to `filtered[]` because they appear to photograph the same physical page as a higher-scoring row. Object-shaped: `{"code": "duplicate_photography_demoted", "count": N}`. Demoted, never deleted — the rows are in `filtered[]` with `filter_reason: "duplicate_photography"`. Undocumented until 2026-09-08 |
| `transcription_truncated` | browse | the transcription was clipped to the effective `text_cap`; emitted whenever the top-level `text_truncated` flag is `true`, so the same fact reaches you through both channels. Object-shaped: `{"code": "transcription_truncated", "message": "..."}`. Undocumented until 2026-09-08 |
| `sort_not_applied` | parallels (`witnesses`) | fewer than two witnesses resolved, so no fusion happened and there is nothing for `fused` / `witness_count` to order by. The array is ordered by score. Object-shaped: `{"code": "sort_not_applied", "sort": "...", "reason": "..."}`. `request.sort` still echoes what was **asked for** — the echo reflects the request, this warning reports what was done. |

**Worked Responsa cascade case.** A `/api/search` response showing both signals
simultaneously:

```json
{
  "warnings": ["query_downgraded: Judeo-Arabic expansion disabled for this query."],
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

Every server-side var that affects the four endpoints, plus the two skill-side vars.

| Var | Default | Scope | Notes |
| --- | ------- | ----- | ----- |
| `SEARCH_API_MODE` | `open` | server | Values: `open` \| `localhost-only` \| `disabled`. Flippable per request without restart (`enforce_mode_gate` re-reads env every call). Applies to `/api/search`, `/api/browse`, `/api/parallels`, and `/api/capabilities`. |
| `SEARCH_API_RATE_LIMIT` | `120` | server | Per-IP requests per minute (raised from `30` in 2026-06 to support API-driven research). **Shared ceiling but each endpoint has an independent bucket** — Phase 80 D-05 makes `/api/search` + `/api/browse` + `/api/parallels` run three separate rate-limiter instances reading the same env var (a fourth, independent instance was added for `/api/capabilities` — see Rate Limiting & Buckets below), so a client doing search+browse+parallels+capabilities gets approximately 4× the per-IP allowance of one endpoint alone. Verified by `tests/test_parallels_api.py::test_parallels_rate_limit_independence`. |
| `SEARCH_API_BROWSE_TIMEOUT` | `1.0` | server | Per-source enrichment timeout for `/api/browse` PGP/FJMS/NLI fetches, in seconds. Hitting it produces an `enrichment_timeout` warning (response is still 200). |
| `SEARCH_API_BROWSE_CORE_TIMEOUT` | `2.0` | server | Core BrowsePage fetch timeout for `/api/browse`, in seconds. Phase 79 R-01 added this to prevent executor pinning on a hung Tantivy reader; hitting it produces a 504 `core_timeout` envelope. |
| `SEARCH_API_CORE_TIMEOUT` | `30.0` | server | Interactive baseline timeout for `/api/search` (exact/title/shelfmark/responsa modes), in seconds. Re-read per request. |
| `SEARCH_API_VARIANTS_TIMEOUT` | `60.0` | server | Heavy-tier timeout for `/api/search` with `search_mode=variants`, in seconds. Re-read per request. |
| `SEARCH_API_FUZZY_TIMEOUT` | `110.0` (was `300.0` before 2026-09-08) | server | Heavy-tier timeout for `/api/search` with `search_mode=fuzzy`, in seconds. Fuzzy (variants_maximum) is inherently slow. Lowered from 300s so the server-side ceiling sits below the public deployment's edge-proxy origin-response budget — see "Edge-Timeout Ceiling" below. A deployment that sits behind no such proxy can raise it back via this env var. Re-read per request. |
| `SEARCH_API_PARALLELS_TIMEOUT` | `110.0` (was `300.0` before 2026-09-08) | server | Timeout for `/api/parallels` composition search with `method='chunk'` (default), in seconds. Lowered from 300s for the same edge-proxy reason as `SEARCH_API_FUZZY_TIMEOUT` — see "Edge-Timeout Ceiling" below. Re-read per request. |
| `SEARCH_API_PASSAGE_TIMEOUT` | `30.0` | server | Phase 145. Timeout for `/api/parallels` with `method='passage'`, in seconds — its own ceiling, unrelated to `SEARCH_API_PARALLELS_TIMEOUT`. Re-read per request. |
| `SEARCH_API_HEAVY_CONCURRENCY` | `2` | server | Maximum simultaneous in-flight heavy requests (variants/fuzzy/`method='chunk'` parallels). Beyond this, new requests fail fast with 503 `heavy_search_busy` + `Retry-After: 5`. Re-read per request (semaphore rebuilt when config changes and all slots are free). |
| `SEARCH_API_PASSAGE_MAX_WITNESSES` | `25` | server | Maximum `witnesses` entries per request. 25 rather than a rounder number because the flagship case is a 17-witness Birkat Hamazon set; a cap of twelve would reject the workflow the feature exists for. Raising it past what `SEARCH_API_PASSAGE_TIMEOUT` can serve does not extend reach — such requests are refused up front with `too_many_witnesses`. Re-read per request. |
| `SEARCH_API_PASSAGE_CONCURRENCY` | `4` | server | Phase 145. Maximum simultaneous in-flight `method='passage'` requests — its OWN bounded budget (semaphore + its own dedicated `ThreadPoolExecutor(max_workers=4)`, never the default executor `method='chunk'` dispatches into; docs/specs/discovery-budgets.md SS2/SS3's two-budgets lesson). Beyond this, 503 `passage_search_busy` + `Retry-After: 5`. Re-read per request. |
| `SEARCH_API_FUZZY_MAX_LIMIT` | `500` | server | Result-count ceiling for `fuzzy` mode (recall over precision). Bounded `[1, 2000]` (FUZZY_HARD_MAX). Non-fuzzy modes keep MAX_LIMIT=100. Re-read per request. |
| `SEARCH_API_BROWSE_TEXT_CAP` | `4000` | server | Default character cap for transcription text on `/api/browse`. Per-request override via `?text_cap=N`, bounded `[100, 10000]`. |
| `SEARCH_API_POSTHOG_SAMPLE_N` | `1` | server | Capture every Nth request to PostHog. `1` = every request. Applies to all three search-helper endpoints. |
| `POSTHOG_IP_SALT` | auto-generated | server | HMAC salt for hashing client IPs in server-side PostHog events. Optional, but production should set explicitly so hashes survive restarts. |
| `GENIZAH_API_BASE` | `https://genizahsearch.com` | skill | Base URL for all skill API calls. Per Phase 81B D-09, precedence is **env var > `--base-url` CLI flag > default** — an inversion of typical CLI convention; the env var ALWAYS wins. |
| `GENIZAH_SKILL_REQ_PER_MIN` | `96` | skill | Per-bucket throttle ceiling for the skill's token-bucket. Default leaves 24 rpm headroom under the server's 120 rpm `SEARCH_API_RATE_LIMIT`. |

The env-var reference at [docs/guides/ENV_VARS.md](guides/ENV_VARS.md) documents every server-side var
above; the two skill-side vars are documented in
[skills/cairo-genizah-research/SKILL.md](../skills/cairo-genizah-research/SKILL.md).

## Rate Limiting & Buckets

Four INDEPENDENT per-IP buckets, all reading the same `SEARCH_API_RATE_LIMIT` env-var
ceiling on every request:

| Bucket | Limiter instance | Endpoint |
| ------ | ---------------- | -------- |
| search | `_rate_limiter` | `POST /api/search` |
| browse | `_browse_rate_limiter` | `GET /api/browse` |
| parallels | `_parallels_rate_limiter` | `POST /api/parallels` |
| capabilities | its own `RateLimiter` instance | `GET /api/capabilities` |

Bursting one endpoint's bucket does NOT exhaust the other three. A client making sustained
calls to all four endpoints sees roughly 4× the per-IP allowance compared with hammering
one endpoint alone. The three-bucket version of this was a deliberate contract choice made
at the initial public release (Phase 80 D-05; Phase 79 D-18 R-10 captures it as a
monitoring obligation, not a contract change); `/api/capabilities` added a fourth bucket
under the same reasoning when it shipped.

429 responses include a `Retry-After: <seconds>` header.

The `SEARCH_API_MODE` env var gates all four endpoints uniformly:

- `open` (default) — all callers permitted.
- `localhost-only` — non-loopback callers receive a 403 `localhost_only` envelope.
- `disabled` — all callers receive a 503 `disabled` envelope unconditionally.

`enforce_mode_gate(request)` re-reads the env var on every call, so the value can be
flipped at runtime without a restart.

## Heavy-Search Tier

Certain search classes are inherently slow:

- **variants** — morphological expansion (30+ variant pairs)
- **fuzzy** — full Tantivy edit-distance + variants_maximum tier
- **/api/parallels** — multi-minute sliding-window composition matching

These run in a thread-pool worker (one slow query blocks ONE worker thread, not the event loop), so the risk is threadpool starvation rather than event-loop blocking. They are governed by a separate tier:

| Mode | Timeout knob | Default | Budget knob | Default |
| ---- | ------------ | ------- | ----------- | ------- |
| `variants` | `SEARCH_API_VARIANTS_TIMEOUT` | 60 s | `SEARCH_API_HEAVY_CONCURRENCY` | 2 |
| `fuzzy` | `SEARCH_API_FUZZY_TIMEOUT` | 110 s (was 300 s before 2026-09-08) | `SEARCH_API_HEAVY_CONCURRENCY` | 2 |
| parallels | `SEARCH_API_PARALLELS_TIMEOUT` | 110 s (was 300 s before 2026-09-08) | `SEARCH_API_HEAVY_CONCURRENCY` | 2 |
| interactive (exact/title/shelfmark/responsa) | `SEARCH_API_CORE_TIMEOUT` | 30 s | — (no cap) | — |

**Concurrency budget:** A module-level `asyncio.Semaphore` gates heavy-mode requests. When all `SEARCH_API_HEAVY_CONCURRENCY` slots are occupied, a new heavy request fails immediately with **503 `heavy_search_busy` + `Retry-After: 5`** instead of queuing and potentially starving the threadpool. The slot is released from the worker future's **done-callback**, i.e. when the underlying search/composition thread *actually finishes* — not merely when the request's awaiter returns. This matters on the timeout path: `run_in_executor` cannot cancel a running thread, so a 504'd heavy query keeps occupying its worker; holding the slot until true completion (rather than releasing it the moment the timeout fires) prevents re-admitting heavy work past the budget. A timeout or exception therefore cannot strand a slot, and cannot prematurely free one either.

Interactive modes (exact/title/shelfmark/responsa) are NOT gated by this semaphore and always proceed with their own 30 s baseline.

All knobs are re-read per request and can be flipped without a restart.

## Edge-Timeout Ceiling

The public deployment at genizahsearch.com sits behind an edge proxy that abandons the
origin connection somewhere between roughly 100 s and 125 s of wall-clock time,
**independent of any server-side timeout this API documents.** Measured on 2026-09-08:
a `chunk`-method parallels request returned our own 200 at 97.0 s, while a `fuzzy` search
returned a proxy-level 524 at 125.2 s — no origin response involved. As of 2026-09-08,
`SEARCH_API_FUZZY_TIMEOUT` and `SEARCH_API_PARALLELS_TIMEOUT` are therefore **110 s**
(down from 300 s), so that the server-side ceiling sits strictly below the edge's
abandonment window: a client hitting the timeout gets our documented JSON `core_timeout`
504 rather than the proxy's opaque one. 110 s was chosen because it is above the observed
97.0 s success (so no request that currently succeeds starts failing) and below the
observed 125.2 s proxy giveup. A deployment that sits behind no such proxy — or one with a
larger origin-response budget — can raise both env vars back via `SEARCH_API_FUZZY_TIMEOUT`
/ `SEARCH_API_PARALLELS_TIMEOUT`; nothing in the engine itself needs 300 s, that number was
only ever a server-side ceiling.

**A 5xx from the public deployment may not be JSON.** A timeout absorbed by the edge
arrives as a plain-text body (observed: `error code: 504`), not the `{"error": {...}}`
envelope this document specifies elsewhere. Client error handling that assumes every 5xx
is JSON will break on exactly the failures most likely to happen under load. Branch on the
HTTP status first; parse the envelope only if the body actually parses as JSON.

**Two related engine defects, tracked as open, not papered over here:**

- `method='passage'` has been observed to exceed its own 30 s ceiling
  (`SEARCH_API_PASSAGE_TIMEOUT`) on short single-text input — a 100-character text
  returned a 504 `core_timeout` at roughly 34 s on 2026-09-08, twice. This is a
  performance defect against the engine's own cost model (`shared/passage_parallels.py`
  documents ~3,000 postings as comfortably inside the timeout for inputs of this size),
  not a documented design point. `SEARCH_API_PASSAGE_TIMEOUT` is intentionally left at
  30 s rather than raised to hide it — see the Environment Variables table below.
- Multi-witness (`witnesses[]`) latency has been observed to be **non-deterministic**:
  an identical 2-witness request returned 200 with 198 results once, and produced no
  response within 120 s on an immediate identical repeat. Do not assume a multi-witness
  request that succeeded once will succeed again at the same cost.

Neither of these is fixed by the 110 s ceiling change above; both remain open engine-level
work.

## Statelessness Contract

The three search-helper endpoints have ZERO references to `state.last_results`,
`state.current_search_query`, `state.parallels_results`, `app.storage` (any sub-key), or
`request.cookies`. Identical query strings produce identical bodies regardless of session.
This is verified at acceptance time by grep, and the constraint is restated in the handler
docstrings in [web/search_api.py](../web/search_api.py).

## What This API Is NOT

- Not authenticated. There are no API keys today; access is anonymous and rate-limited per-IP. Future versions may add optional keys for higher quotas.
- Not browse-page parity. Only the subset of fields the skill ranks against is exposed; UI-only fields (corrections, comments, lists, puzzles) are not.
- Not a write API. All three endpoints are read-only over the public corpus.
- Not a bulk-export interface. For full-corpus access, use the [interactive search](https://genizahsearch.com) directly or contact the project for the underlying transcription dataset (see [Attribution & Citation](#attribution--citation)).
- Not a long-running job runner. Composition-parallels requests run synchronously within the request timeout; a future async-job API may ship in v7.11+.

## Deferred Follow-Ups (NOT in P9X)

The following improvements are documented here as future work but are NOT implemented in this change:

1. **Async job pattern**: `POST /api/search` (heavy mode) → `202 + job_id` → poll `GET /api/jobs/{id}`. This is the right long-term solution for multi-minute queries but is a larger surface (job store, polling contract, skill client changes). The current per-mode timeout tiering + fail-fast 503 buys correctness now with minimal surface area.
2. **Index-time skeleton / matres-lectionis normalization**: Would improve fuzzy PRECISION (fewer noise hits) and could allow lowering `SEARCH_API_FUZZY_TIMEOUT`, but requires an index rebuild and core search changes — out of scope for an API-layer hardening pass.

## See Also

- [skills/cairo-genizah-research/references/api_contract.md](../skills/cairo-genizah-research/references/api_contract.md) — locked consumer-facing envelope shapes used by the Claude skill.
- [docs/guides/ENV_VARS.md](guides/ENV_VARS.md) — server-side env-var declarations (the seven `SEARCH_API_*` and `POSTHOG_IP_SALT` vars).
- [web/search_api.py](../web/search_api.py) — the route handlers and Pydantic models in source.
- [shared/api_errors.py](../shared/api_errors.py) — `ERROR_CODES` and `WARNING_CODES` frozensets and the `APIError` exception type.
- [shared/search_serializer.py](../shared/search_serializer.py) — the sole producer of envelope shapes (Phase 77 D-14).

---

## Changelog

### 9.2.0 (2026-09-08) — Capabilities endpoint, retroactive Phase 145 changelog entry, and the edge-timeout ceiling

**(a) Retroactively recording shipped-but-never-logged additions.** The following were
documented in the body of this file for some time but were never given a changelog entry —
which is itself part of why this document could read as internally inconsistent (a body
describing features with no corresponding history of when they arrived):

- `search_mode: "fuzzy"` on `POST /api/search` (added 2026-06) — the approximate /
  maximum-variant tier; no feature flag, always available.
- `method: "chunk" | "passage"` on `POST /api/parallels` (Phase 145) — `"passage"` is
  gated by `PASSAGE_PARALLELS_ENABLED` **and** a successfully-loaded passage index
  (`passage_available()`); **both are ON in this production deployment.**
- `witnesses[]` on `POST /api/parallels` (Phase 145, max `SEARCH_API_PASSAGE_MAX_WITNESSES`,
  default 25) — multi-witness search, gated by `PASSAGE_MULTI_WITNESS_ENABLED` **and**
  `passage_available()`; **both are ON in this production deployment.**
- `sort: "fused" | "best_match" | "witness_count"` on `POST /api/parallels` (Phase 145) —
  requires `witnesses[]`; gated the same way as `witnesses[]` above.

This entry does not change behavior — every one of these was already live. The omission
from this Changelog, against a body that documented them in full, was the defect being
fixed here.

**(b) `GET /api/capabilities` (additive).** A new endpoint reporting which flag-gated
features are live on a deployment (which endpoints exist, the current `search_mode`
enum, whether `passage` / `passage_multi_witness` are on, live rate limits, concurrency
budgets, and timeouts) without the cost of a heavy request. See "Endpoint:
GET /api/capabilities" above. New endpoints are explicitly additive per this document's
own Stability section above; `schema_version` stays `1`.

**(c) `SEARCH_API_FUZZY_TIMEOUT` and `SEARCH_API_PARALLELS_TIMEOUT` lowered 300s → 110s
(reduction in a published ceiling).** The public deployment's edge proxy abandons the
origin connection at roughly 100–125s regardless of what this API's own timeout says; a
300s server-side ceiling could therefore never actually deliver our documented JSON
`core_timeout` 504 to a client on a request that ran that long — the proxy would return an
opaque, non-JSON 5xx first. Lowering both ceilings to 110s (still above the 97.0s slowest
observed success, still below the 125.2s observed proxy giveup) means a timing-out request
now gets our envelope instead. See "Edge-Timeout Ceiling" above for the full measurement.
**Judged NOT breaking under this document's own Stability definition**, which names three
specific things as breaking — request shape, response envelope shape, and error codes —
and this change touches none of them: the same `core_timeout` error code, in the same
envelope shape, on the same request shape, now simply fires somewhat sooner for the
slowest requests. For transparency: this is still a reduction in a previously-published
number, and a request that would have taken between 110s and 300s (none were observed in
this measurement) would now time out where it previously would have succeeded — no such
case is known to exist today, but the document should not claim the ceiling change is
consequence-free, only that it does not meet this contract's own definition of breaking.

**(d) Response-shape accuracy sweep (documentation only).**
No API change. Every response example and field table in this file was compared
against the live serializers and against real production responses, and 26 corrections
were applied. The endpoint REFERENCE sections had drifted further than the Quick Start
examples repaired earlier the same day, and in the same direction: they described an
envelope the code has never produced.

**Fields that did not exist** (a client following the doc got a `KeyError` on first
contact, not a `null`):

- `/api/search` and `/api/parallels` result `locator` was documented with a fourth key,
  `fl_id`. It has exactly three: `{sys_id, volume_ie, p_num}`. `fl_id` belongs only to
  `/api/browse`'s locator. The Drill-Down Locator Round-Trip section — the part of this
  document most likely to be copied verbatim — carried the same error.
- `/api/search` result items were documented with a nested `metadata` object holding
  `library`/`library_name`/`domains`/`dating`. There is no `metadata` key on a result
  item; `library` (an OBJECT, `{code, name}`), `domains` and `dating` are top-level.
- `/api/parallels` result items were documented with `aggregate_score`. That is an
  internal group sort key, consumed into `sort_score`, never serialized under that name.
  The field is `score`.
- `/api/browse` was documented with flat `library_code` / `library_name`. It returns
  `library: {code, name}`.
- `method='passage'` was documented as returning a `chunk_count` field. It does not;
  the span count is `len(item["matches"])`.

**Sub-objects whose every key was wrong.** `/api/browse`'s `metadata.pgp` was documented
as `{pgpid, description, editions, translations}` (it has ten keys, and neither
`editions` nor `translations` is among them); `metadata.fjms` as
`{catalog_records, free_descriptions, bibliography}` (it is
`{source_names, has_measurements, has_visual_suggestions}`); `metadata.nli` as
`{manifest_url, fl_index}` (it is `{physical_metadata, folio}`); and `image.sources` as a
list of provider-name strings (it is a list of objects).

**Wrong types and value domains.** `locator.p_num` is a STRING on `/api/search` and
`/api/parallels` (an int on `/api/browse`) and was documented as an int on all three.
`image_url` / `image.url` are server-relative same-origin proxy paths, not the absolute
upstream IIIF URLs the examples showed. `image.provider` is a lowercase provider code.

**`warnings[]` is a mixed-type array, which this document only half-admitted.** Most
entries are objects with a `code` key, but `truncated_to_200` and
`query_downgraded: <message>` are plain strings. The Warnings Array table had this right;
the 200-group-cap section and both worked examples showed the object form, so a client
written from either would raise `TypeError` reading `w["code"]`. Three emitted warning
codes were missing from the table entirely: `passage_results_truncated`,
`duplicate_photography_demoted`, and `transcription_truncated`.

**Claims that contradicted the code or this file itself.** `/api/parallels`' request echo
is 7 keys only for `method='chunk'` — it is 9 for `passage` (`passage_policy` and
`passage_report`, the latter undocumented until now) and 11 with `witnesses[]`, where this
section had asserted "exactly seven keys (no more, no fewer)". `count` and `total` are
ALWAYS equal on `/api/parallels` (both assigned from one expression), and a Quick Start
example showed them differing. `request.limit_effective` is a ROW count, not the
"post-truncation group count" claimed here — a live response returns 183 against a `count`
of 108; the wrong description originated in a source comment, now also corrected.
`filtered[]` was said to be always `[]` for `method='passage'`; the mandatory
duplicate-photography pass demotes rows into it. The two `mode` enums were said to share
only `exact` and `variants` two lines after both were listed as including `fuzzy`. Sending
neither `text` nor `witnesses` returns `composition_required`, not `invalid_request`.

**Two things that are not this envelope at all**, now documented: a Pydantic structural
failure adds a `fields` array to the error object, and a wrong HTTP method never reaches a
handler — `GET /api/search` returns Starlette's `{"detail": "Method Not Allowed"}` at 405.

**`/api/capabilities` carries no `source` key**, so the envelope guarantee added earlier
the same day ("every successful response carries `schema_version` and `source`") was false
for the endpoint added in the same commit. The fixed-shape descriptor is deliberate and
pinned by test; the guarantee is now scoped to the three data endpoints.

**Prevention.** The gate introduced with the Quick Start repair read only the Quick Start,
which is why these sections survived it. It now locates every response example in this
file by an explicit anchor — 7 examples across all four endpoints — compares each against
a real response body, and fails if any fenced JSON block is neither checked nor recorded
as exempt. Pointed at the pre-repair text it reports every error listed above.

### v7.11 (Phase 85 — SYNTH-06) — Synthetic-row API field (additive)

As of v7.11, all `/api/search`, `/api/browse`, and `/api/parallels` response items
include a top-level `is_synthetic: boolean` field (NOT nested under `locator`):

- `false` (default): the result row corresponds to a real NLI Alma record.
- `true`: the result row is a synthetic libraries.csv entry generated for an
  FJMS-only or CUDL-orphaned inventory (Phase 85). Synthetic rows have FJMS
  catalogue / bibliography / measurements but no NLI Alma data; CUDL images are
  served when a Cambridge IIIF manifest is available.

This field is **additive and backward-compatible**: existing consumers can ignore it.
Schema version remains `1` per the Phase 83 stability commitment ("additive changes
any time"). Skill consumers should consider showing a "no NLI metadata" annotation
when `is_synthetic: true`.

**PostHog event tagging (analytics):** `/api/search` and `/api/browse` events carry
an `is_synthetic` property. `/api/parallels` events INTENTIONALLY omit this property
— parallels seeds with composition `text`, not `sys_id`, so there is no canonical
"seed sys_id" to tag. Future analytics needing this signal can derive it from the
response payload's per-item `is_synthetic` field.

**Corrections write deferral (D-10):** Corrections-write is gated CLIENT-SIDE at the
two real write entry points — there is NO `POST /api/corrections` HTTP route in
this codebase:

- `corrections_client.py` `CorrectionsClient.create_correction` returns
  `(None, "synthetic_corrections_disabled: ...")` for synthetic `document_id`.
- `supabase_corrections_client.py` `SupabaseCorrectionsClient.create_correction`
  returns the same shape BEFORE the `client.table('corrections').insert(data).execute()`
  call.

This is a Phase 85 D-10 deferral; a future plan will define proper `page_number`
semantics for image-backed synthetic rows. The web and desktop UIs hide the
"Add correction" / "Edit" button as defense-in-depth.

**Audit deferral note:** AUDIT-01 / AUDIT-02 / AUDIT-03 (re-running
`scan_cudl_orphans.py`, producing `reports/cudl_coverage.md`, regression-checking
the Phase-85 hide-NLI gates) are tracked in **Phase 86** — see `ROADMAP.md §Phase 86`.

### v7.10 (2026-05-05) — Initial public release

- Endpoints `/api/search`, `/api/browse`, `/api/parallels` promoted from internal-undocumented to public API per Phase 83.
- OpenAPI spec at `/api/openapi.json`; interactive Swagger UI at `/api/docs`.
- Stability commitment added (see "Stability" section above).
- Attribution & Citation section added.

Breaking changes announced in `CHANGELOG.md` for all future major-version releases.

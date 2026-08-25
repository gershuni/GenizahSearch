# GenizahSearch Search-Helper API (v7.10)

> Last updated: 2026-05-05

## Stability

This is a public research-automation API. We aim to keep this contract stable. Breaking changes (request shape, response envelope shape, error codes) will only ship on major website-version releases and will be announced in `CHANGELOG.md` and the `Changelog` section below. Additive changes (new optional fields, new optional request keys, new endpoints) may ship at any time.

**Interactive docs:** [`/api/docs`](https://genizahsearch.com/api/docs) (Swagger UI) · [`/api/openapi.json`](https://genizahsearch.com/api/openapi.json) (OpenAPI spec)

---

## Quick Start

All three endpoints return JSON wrapped in a uniform envelope. Successful responses contain `schema_version`, `request` (echo of input), and a result payload (`results[]` for `/search` and `/parallels`, top-level fields for `/browse`). Failures return `{"error": {"code": "...", "message": "..."}}` with an HTTP 4xx/5xx status.

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
  "request": {"query": "אחד מי יודע", "search_mode": "variants", "limit": 5},
  "results": [
    {
      "uid": "IE1_P1_FL1",
      "rank": 1,
      "score": 12.34,
      "locator": {"sys_id": "990025143260205171", "p_num": 1},
      "snippet": "..."
    }
  ],
  "total": 42
}
```

Take a result's `uid` and `locator.sys_id` to drill down via `/api/browse`.

### Drill down to a manuscript page

```bash
curl -s "https://genizahsearch.com/api/browse?sys_id=990025143260205171&uid=IE1_P1_FL1" \
  | python -m json.tool
```

Response shape (truncated):

```json
{
  "schema_version": 1,
  "request": {"sys_id": "990025143260205171", "uid": "IE1_P1_FL1"},
  "manuscript": {"shelfmark": "...", "library_code": "..."},
  "page": {"text": "...", "text_source": "pgp_transcription", "image_url": "..."}
}
```

Returns transcription text (when available), PGP/FJMS/NLI metadata, and image URL.

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
  "request": {"text": "...", "chunk_size": 4, "mode": "variants"},
  "results": [
    {"sys_id": "...", "matched_chunks": [], "score": 0.87}
  ]
}
```

Returns a `results[]` of manuscript groups that share sequential phrase-chunks with the input text.

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

The v7.10 search-helper API exposes three endpoints — `POST /api/search`, `GET /api/browse`,
and `POST /api/parallels` — that together let a research consumer execute a Tantivy
keyword/Responsa search, drill down into a single manuscript page (with PGP/FJMS/NLI
enrichment), and run a composition-parallels job over an arbitrary input text. A reference
consumer is the [`cairo-genizah-research` Claude skill](../skills/cairo-genizah-research/SKILL.md),
which demonstrates the full search → browse → rank workflow. The three endpoints share a common
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
| `search_mode` | enum | `exact \| variants \| responsa \| title \| shelfmark \| fuzzy` | required | `regex` was intentionally dropped per Phase 81A D-09. `fuzzy` (added 2026-06) is the approximate / maximum-variant tier — bounded by `SEARCH_API_FUZZY_TIMEOUT` (~300s), not the interactive 30s baseline |
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
| `regex` value for `search_mode` | `invalid_request` | 400 | structurally rejected by Literal enum; not in v7.10 |

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
| `is_synthetic` | bool | Phase 85 SYNTH-06 (v7.11): `true` iff the row is a Phase-85 synthetic libraries.csv entry generated for an FJMS-only or CUDL-orphaned inventory; `false` for real NLI Alma records. Top-level (NOT nested under `locator`); additive — schema_version stays 1. Skill consumers should consider showing a "no NLI metadata" annotation when `true`. |
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
| `schema_version` | integer | currently `1` |
| `source` | string | `"browse"` |
| `generated_at` | string | ISO-8601 UTC |
| `locator` | object | resolved locator (all five fields) |
| `is_synthetic` | bool | Phase 85 SYNTH-06 (v7.11): `true` iff the resolved row is a Phase-85 synthetic libraries.csv entry. Top-level (NOT nested under `locator`); additive — schema_version stays 1. When `true`, `metadata.nli` will typically be `null` and the image will fall back to CUDL when available. |
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
| `text` | string \| null | 1..20000 chars (post-strip; `COMPOSITION_LENGTH_CAP=20000`; empty → `composition_required`; over cap → `composition_too_long`) | required *unless* `witnesses` is sent | Omit it ONLY when sending `witnesses` instead. Sending both → 400 `witnesses_and_text_conflict`; sending neither → 400 `invalid_request` (unchanged). |
| `chunk_size` | integer | `2..20` | `5` | size of sliding chunks |
| `mode` | enum | `exact \| variants \| fuzzy` | `"exact"` | **field name is `mode`, not `search_mode`** — see "Naming Inconsistency" |
| `max_freq` | float \| null | `>= 1`. A **document count**, not a ratio: a chunk matching more than `max_freq` documents is treated as too common. `null` disables high-frequency filtering | `null` | **Effective range is `[1, 50)`.** The engine tests `len(hits) > max_freq` against a per-chunk retrieval hard-capped at 50 hits, so any `max_freq >= 50` can never fire and behaves exactly like `null`. It is therefore not a corpus frequency: it counts hits inside a truncated top-50 and cannot tell a chunk in 51 manuscripts from one in 5,000. A value below 1 would discard every chunk that matches anything, so such values are rejected with `invalid_request` rather than silently returning an empty result set. Documented as a `0.0-1.0` ratio until 2026-08-24 — the docs were wrong, not the code |
| `boundary_mode` | enum | `full \| boundary \| combined` | `"full"` | only boundary knob exposed in v7.10 (Phase 80 D-03) |
| `filters` | object \| null | reuses Phase 78 `FiltersModel` verbatim | `null` | same shape as `/api/search.filters` |
| `method` | enum | `chunk \| passage` | `"chunk"` | Phase 145 (beta). `chunk` is the pre-Phase-145 sliding-window Tantivy engine described above, byte-for-byte unchanged. `passage` is a character-level matching engine, tolerant of OCR/HTR noise and reflowed line breaks — see "`method='passage'` (beta)" below. |
| `witnesses` | array \| null | 1..`SEARCH_API_PASSAGE_MAX_WITNESSES` (default 25) objects; `method='passage'` only | `null` | Several witnesses of ONE work, each searched **separately** and merged by rank fusion — see "Multi-witness search" below. Mutually exclusive with `text`. |
| `sort` | enum \| null | `fused \| best_match \| witness_count`; requires `witnesses` | `null` (→ `fused`) | Group ordering for a multi-witness search. Without `witnesses` → 400 `sort_requires_multi_witness`. |

The Lab Engine extended-parallels path is OUT OF SCOPE for v7.10 (Phase 80 D-02).

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

### `method='passage'` (beta, Phase 145)

An alternative matching engine over the SAME response shape (`results[]`/`filtered[]`/
`matches[]`/`locator` unchanged) — a client that does not read `request.method` cannot tell
the two apart from the envelope shape alone.

- **Availability.** Requires the deployment to have `PASSAGE_PARALLELS_ENABLED=1` AND a
  successfully-loaded passage index (`web/passage_assets.py::passage_available()`). When
  either is false, `method='passage'` returns 503 `passage_unavailable` — never a silent
  fallback to `chunk`.
- **Genizah-only scope.** The passage index is built ONLY from the Genizah transcription
  corpus; it holds zero records for `LOCAL` (My-Library) provenance. Requesting
  `method='passage'` together with `filters.library` containing `"LOCAL"` under the default
  `library_filter_mode="include"` returns 400 `passage_scope_unsupported` rather than a
  silently-empty result that would look identical to "no matches found". Excluding `"LOCAL"`
  (`library_filter_mode="exclude"`) is a no-op for passage and is NOT rejected.
- **Display name.** The web GUI presents this method as “Letter-level search” (owner naming, 2026-08-23) and selects it by default when the index is available; `method='passage'` remains the stable wire value — API clients should never parse display names.
- **Span-shaped `matches[]`.** Each accepted contiguous span of matched text on a manuscript
  page is one `matches[]` entry (`chunk_count` = number of spans, unlike the incumbent's
  Tantivy-hit-derived count; `chunk_index` is the ordinal of the span's position within the
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
  a plain sys_id restriction, same as `chunk`. `filtered[]` is always `[]` via the public API
  specifically: `filter_text` (the "known source text" a row's match can be checked against,
  routing it to `filtered` rather than `results[]`) is a web-page-only concept (the page's
  "Filter Sources" panel) that this endpoint never populates. A row whose display-text lookup
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
more than 200 groups, the top 200 are returned and a `{"code": "truncated_to_200", ...}`
entry is added to `warnings[]`.

### 7-key request echo

The parallels response echoes exactly seven keys (no more, no fewer; six before Phase 145
added `method`). Explicitly NOT echoed: `search_mode` (parallels uses `mode`), `gap` (a
search-only concept), `responsa_options` (parallels never used Responsa).

| Echo key | Source | Notes |
| -------- | ------ | ----- |
| `mode` | `req.mode` | NOT `search_mode` (per Phase 81A D-07) |
| `chunk_size` | `req.chunk_size` | unmodified |
| `max_freq` | `req.max_freq` | `null` permitted |
| `boundary_options` | server-resolved 5-key dict (`boundary_mode`, `boundary_delimiter`, `boundary_boost`, `min_boundary_matches`, `min_delimiter_distance`) | includes service-layer defaults |
| `method` | `req.method` | Phase 145; `"chunk"` when omitted — always present, so a caller never has to guess which engine served the response |
| `limit_effective` | `len(bundle.main_results)` | post-truncation group count |
| `filters` | model-dumped `FiltersModel` (exclude_none) or `null` | |

## Naming Inconsistency: parallels.mode vs search.search_mode

`/api/search` uses `search_mode` (the post-Phase-81A name). `/api/parallels` continues to
use `mode`. This is intentional v7.10 debt locked by Phase 81A D-07: renaming the parallels
field would have broken Phase 80 tests with no consumer-visible benefit, since the
parallels enum is a different set of values (`exact | variants | fuzzy`) than the search
enum (`exact | variants | responsa | title | shelfmark | fuzzy`). Future versions may unify the
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
| `core_timeout` | 504 | `/api/browse`: BrowsePage exceeded `SEARCH_API_BROWSE_CORE_TIMEOUT` (2.0s). `/api/search`: per-mode ceiling exceeded — exact/title/shelfmark/responsa→30s (`SEARCH_API_CORE_TIMEOUT`), variants→60s (`SEARCH_API_VARIANTS_TIMEOUT`), fuzzy→300s (`SEARCH_API_FUZZY_TIMEOUT`). `/api/parallels`→300s (`SEARCH_API_PARALLELS_TIMEOUT`) for `method='chunk'`, 30s (`SEARCH_API_PASSAGE_TIMEOUT`) for `method='passage'`. Message names the ceiling and mode. |
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
| `sort_not_applied` | parallels (`witnesses` + `sort`) | fewer than two witnesses resolved, so no fusion happened and there is nothing for `fused` / `witness_count` to order by. The array is ordered by score. Object-shaped: `{"code": "sort_not_applied", "sort": "...", "reason": "..."}`. `request.sort` still echoes what was **asked for** — the echo reflects the request, this warning reports what was done. |

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
| `SEARCH_API_RATE_LIMIT` | `120` | server | Per-IP requests per minute (raised from `30` in 2026-06 to support API-driven research). **Shared ceiling but each endpoint has an independent bucket** — Phase 80 D-05 makes `/api/search` + `/api/browse` + `/api/parallels` run three separate rate-limiter instances reading the same env var, so a client doing search+browse+parallels gets approximately 3× the per-IP allowance of one endpoint alone. Verified by `tests/test_parallels_api.py::test_parallels_rate_limit_independence`. |
| `SEARCH_API_BROWSE_TIMEOUT` | `1.0` | server | Per-source enrichment timeout for `/api/browse` PGP/FJMS/NLI fetches, in seconds. Hitting it produces an `enrichment_timeout` warning (response is still 200). |
| `SEARCH_API_BROWSE_CORE_TIMEOUT` | `2.0` | server | Core BrowsePage fetch timeout for `/api/browse`, in seconds. Phase 79 R-01 added this to prevent executor pinning on a hung Tantivy reader; hitting it produces a 504 `core_timeout` envelope. |
| `SEARCH_API_CORE_TIMEOUT` | `30.0` | server | Interactive baseline timeout for `/api/search` (exact/title/shelfmark/responsa modes), in seconds. Re-read per request. |
| `SEARCH_API_VARIANTS_TIMEOUT` | `60.0` | server | Heavy-tier timeout for `/api/search` with `search_mode=variants`, in seconds. Re-read per request. |
| `SEARCH_API_FUZZY_TIMEOUT` | `300.0` | server | Heavy-tier timeout for `/api/search` with `search_mode=fuzzy`, in seconds. Fuzzy (variants_maximum) is inherently slow. Re-read per request. |
| `SEARCH_API_PARALLELS_TIMEOUT` | `300.0` | server | Timeout for `/api/parallels` composition search with `method='chunk'` (default), in seconds. Re-read per request. |
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

The CLAUDE.md env-var block at [CLAUDE.md](../CLAUDE.md) documents every server-side var
above; the two skill-side vars are documented in
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

## Heavy-Search Tier

Certain search classes are inherently slow:

- **variants** — morphological expansion (30+ variant pairs)
- **fuzzy** — full Tantivy edit-distance + variants_maximum tier
- **/api/parallels** — multi-minute sliding-window composition matching

These run in a thread-pool worker (one slow query blocks ONE worker thread, not the event loop), so the risk is threadpool starvation rather than event-loop blocking. They are governed by a separate tier:

| Mode | Timeout knob | Default | Budget knob | Default |
| ---- | ------------ | ------- | ----------- | ------- |
| `variants` | `SEARCH_API_VARIANTS_TIMEOUT` | 60 s | `SEARCH_API_HEAVY_CONCURRENCY` | 2 |
| `fuzzy` | `SEARCH_API_FUZZY_TIMEOUT` | 300 s | `SEARCH_API_HEAVY_CONCURRENCY` | 2 |
| parallels | `SEARCH_API_PARALLELS_TIMEOUT` | 300 s | `SEARCH_API_HEAVY_CONCURRENCY` | 2 |
| interactive (exact/title/shelfmark/responsa) | `SEARCH_API_CORE_TIMEOUT` | 30 s | — (no cap) | — |

**Concurrency budget:** A module-level `asyncio.Semaphore` gates heavy-mode requests. When all `SEARCH_API_HEAVY_CONCURRENCY` slots are occupied, a new heavy request fails immediately with **503 `heavy_search_busy` + `Retry-After: 5`** instead of queuing and potentially starving the threadpool. The slot is released from the worker future's **done-callback**, i.e. when the underlying search/composition thread *actually finishes* — not merely when the request's awaiter returns. This matters on the timeout path: `run_in_executor` cannot cancel a running thread, so a 504'd heavy query keeps occupying its worker; holding the slot until true completion (rather than releasing it the moment the timeout fires) prevents re-admitting heavy work past the budget. A timeout or exception therefore cannot strand a slot, and cannot prematurely free one either.

Interactive modes (exact/title/shelfmark/responsa) are NOT gated by this semaphore and always proceed with their own 30 s baseline.

All knobs are re-read per request and can be flipped without a restart.

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
- [CLAUDE.md](../CLAUDE.md) — server-side env-var declarations (the seven `SEARCH_API_*` and `POSTHOG_IP_SALT` vars in the Environment Variables block).
- [web/search_api.py](../web/search_api.py) — the route handlers and Pydantic models in source.
- [shared/api_errors.py](../shared/api_errors.py) — `ERROR_CODES` and `WARNING_CODES` frozensets and the `APIError` exception type.
- [shared/search_serializer.py](../shared/search_serializer.py) — the sole producer of envelope shapes (Phase 77 D-14).

---

## Changelog

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

# Phase 81A: Minimal API Contract Expansion — Context

**Gathered:** 2026-05-03
**Status:** Ready for planning
**Supersedes:** none (greenfield directory; companion to `.planning/phases/81B-claude-skill-consumer/81-RESCOPE.md`)

<domain>
## Phase Boundary

Replace `/api/search`'s conflated `mode` field with a UI-aligned `search_mode` enum + a Responsa-only `responsa_options` flag bag. Add a `request` echo block to `/api/search` AND `/api/parallels` envelopes so consumers see what the server actually applied. Lower the `limit` ceiling from 200 to 100. Preserve all Phase 78/79/80 hardening (rate limit, mode gate, error envelope, PostHog capture, statelessness, per-bucket independence).

Internal/undocumented API. **Breaking change** — old `mode` field is hard-rejected via `extra='forbid'`. No external clients exist; no deprecation window.

**In scope:**
- New `SearchRequest` / `ResponsaOptions` Pydantic models with cross-field validation
- `request` echo block on both `/api/search` and `/api/parallels` envelopes
- `responsa_options_effective` + `warnings[]` semantics for Responsa cascade visibility
- Lowered `limit` ceiling (max 100)
- New PostHog properties: `search_mode_value`, `responsa_options_count`
- New test file `tests/test_search_api_v2.py` covering the search_mode × responsa_options × invalid-combination matrix
- In-place rewrite of 78-era `tests/test_search_api.py` cases that use the old `mode` field

**Out of scope:**
- The skill consumer itself (Phase 81B)
- Internal documentation (Phase 82 — `docs/SEARCH_API.md`)
- Renaming `/api/parallels.mode` → `search_mode` (deferred to v7.11)
- Long-running parallels job API (Phase 81C — deferred)
- `variants_extended` / `variants_maximum` / `text_position` / global `judeo_arabic` / `plene_defective` / `exclude_words[]` (all deferred to v7.11 per `81-RESCOPE.md` §3.2)

</domain>

<decisions>
## Implementation Decisions

### Locked in 81-RESCOPE.md (carried forward, not re-discussed)

- **D-01:** `search_mode` enum is UI-aligned and flat — NOT split on `field × match_mode`. (`81-RESCOPE.md` §3.1)
- **D-02:** `responsa_options` field names mirror desktop UI exactly: `variants`, `ja`, `flex_spacing`, `bidirectional`. Internal `variant_mode` is derived server-side from `responsa_options.variants`; NOT exposed in the API.
- **D-03:** `extra='forbid'` on both `SearchRequest` and `ResponsaOptions` Pydantic models.
- **D-04:** `request` echo block always preserves `search_mode` identical to client request (never silently downgraded). Responsa cascade is exposed via `responsa_options_effective` + existing `tr()` strings in `warnings[]`. No `search_mode_effective` field.
- **D-05:** `responsa_options` and `responsa_options_effective` are `null` for non-Responsa modes.
- **D-06:** Conservative guardrails: `limit` default 50 / max 100 (lowered from 200), `query` 1000 chars, composition cap 20000, rate limits unchanged (30/min per bucket × 3 buckets).
- **D-07:** `/api/parallels.mode: exact|variants|fuzzy` enum stays as-is in 81A. The temporary stylistic inconsistency with `/api/search.search_mode` is documented in Phase 82 (DOC-01); rename deferred to v7.11. (OQ-2 resolved 2026-05-02.)
- **D-08:** PostHog event gains `search_mode_value` (the literal enum string) and `responsa_options_count` (count of True flags in ResponsaOptions, 0 if None or non-responsa mode).

### New decisions from this discussion (2026-05-03)

- **D-09:** **Drop `regex` from the v7.10 enum.** (Resolves OQ-4; deviates from RESCOPE recommendation.) Final enum has **5 values**: `exact | variants | responsa | title | shelfmark`. Defers regex to v7.11. Rationale: smaller test surface, smaller ReDoS attack surface, and the v7.10 skill (81B) does not use regex.
  - **Cascading effects:**
    - The 256-char regex pattern cap (RESCOPE §3.4) is **not implemented** in 81A. Single 1000-char `query` cap applies uniformly.
    - The `regex_pattern_too_long` error code is **not added** in 81A.
    - The validation matrix row "`regex` + `responsa_options` → 400 `invalid_combination`" is **removed** (not applicable).
    - 81A-AC2 reduces from "all 6 search_mode values" to "all 5 search_mode values."
    - 81A-AC4 invalid-combination set: only (a) `responsa_options` with non-responsa mode and (b) `gap > 0` with title/shelfmark. The `(c) regex + responsa_options` clause is dropped.
    - 81A-AC5 drops the regex pattern-length sub-clause; `query` cap stays at 1000 for all modes.

- **D-10:** **No hashed IP in `request` echo block.** (Resolves OQ-5; matches RESCOPE recommendation.) Echo block contains only what the client sent + what the server applied (`limit_effective`, `responsa_options_effective`). Skills detect rate-limit state from 429 envelopes. Avoids hash-inversion leak surface.

- **D-11:** **`responsa_options.variants` is a plain boolean.** (Resolves OQ-6; matches RESCOPE recommendation.) No tier enum. Server derives internal `variant_mode` ('exact' vs 'variants') from the boolean exactly as the desktop UI does (`genizah_app.py:15796`). Mirrors main-mode deferral of `variants_extended` / `variants_maximum`. Consistent with the desktop UI's single checkbox.

- **D-12:** **New test file `tests/test_search_api_v2.py`** owns the search_mode × responsa_options × invalid-combination matrix (~40–50 cases after the regex drop). Existing `tests/test_search_api.py` is **rewritten in-place** so 78-era hardening tests using the old `mode` field migrate to `search_mode` while preserving git history of the file. Hardening regression coverage stays intact.

- **D-13:** **Hard cutover for old `mode` field.** Pydantic `extra='forbid'` returns 400 `invalid_request` with message `"unknown field 'mode' — use search_mode instead"` (or equivalent). No deprecation grace period. No mapping shim. Internal API; no external clients to break. Simple, atomic, auditable.

### Validation matrix (final, after D-09 cascade)

| Rule | Legal? | Enforcement |
|---|---|---|
| `search_mode='responsa'` + `responsa_options=None` | ✓ | Pydantic default constructor (all-False ResponsaOptions) |
| `search_mode='responsa'` + non-empty `responsa_options` | ✓ | Pass-through |
| Any non-responsa `search_mode` + non-None `responsa_options` | ✗ 400 `invalid_combination` ("responsa_options is only valid when search_mode=responsa") | `@model_validator(mode='after')` |
| `search_mode='title'` or `'shelfmark'` + non-zero `gap` | ✗ 400 `invalid_combination` ("gap has no effect with metadata-only search modes") | `@model_validator(mode='after')` |
| `query` empty after `.strip()` | ✗ 400 `query_required` | Existing |
| `len(query) > 1000` | ✗ 400 `query_too_long` | Existing |
| `limit > 100` or `limit < 1` | ✗ 422 | Pydantic Field constraint |
| `filters.*` value not in FJMS catalog | ✗ 400 `invalid_filter_value` | Inherited Phase 78 D-17 |
| Old `mode` field present | ✗ 400 `invalid_request` ("unknown field 'mode' — use search_mode instead") | `extra='forbid'` (D-03/D-13) |

### Response envelope (final)

`/api/search` and `/api/parallels` both gain a `request` echo block:

```jsonc
{
  "schema_version": 1,
  "source": "search",
  "count": 34,
  "total": 34,
  "warnings": ["Judeo-Arabic expansion disabled (cascade)", ...],
  "generated_at": "...",
  "request": {
    "search_mode": "responsa",                    // /api/search only
    "mode": "variants",                            // /api/parallels only — name preserved per D-07
    "responsa_options": { ... },                   // null for non-responsa or /api/parallels
    "responsa_options_effective": { ... },         // null for non-responsa or /api/parallels
    "gap": 0,
    "limit": 50,
    "limit_effective": 50,
    "filters": null
  },
  "results": [...]
}
```

`/api/parallels` request echo retains the existing `mode` field name (D-07); does NOT expose `responsa_options` (parallels never used Responsa).

### Claude's Discretion (planner picks)

- Pydantic model file location — `web/search_api.py` vs split into `web/search_models.py`. Planner judges based on file size growth.
- Exact wording of `invalid_combination` messages, as long as both offending field names appear.
- Test fixture queries — concrete strings the planner picks for each `search_mode` value (must yield non-empty results per 81A-AC2).
- Whether to introduce a small `_apply_request_echo()` helper in `shared/search_serializer.py` or inline the echo construction at the endpoint sites.

### Folded Todos

None.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase / milestone specs
- `.planning/ROADMAP.md` §`Phase 81A: Minimal API Contract Expansion` — SC-1..SC-6 and the phase gate language.
- `.planning/REQUIREMENTS.md` §`API Contract Expansion (Phase 81A — added 2026-05-02 via rescope)` — `API-EXPAND-01..08` (note: 81A drops regex per D-09, so AC2 reduces to 5 modes; the requirements text still mentions 6 — planner reconciles).
- `.planning/phases/81B-claude-skill-consumer/81-RESCOPE.md` §3 — full design memo for 81A; §3.1 request shape, §3.3 validation matrix, §3.5 echo block, §3.6 guardrails, §3.7 parallels hardening, §3.8 hardening inheritance. **Read first.**
- `.planning/phases/81B-claude-skill-consumer/81-RESCOPE.md` §6 — open-question recommendations (OQ-4/OQ-5/OQ-6 are now resolved here in D-09/D-10/D-11; OQ-1/OQ-2/OQ-7/OQ-8 already resolved by user 2026-05-02).
- `.planning/STATE.md` — v7.10 milestone state; Phases 78/79/80 verified PASSED before 81A starts.

### API contract (locked — 81A modifies request shape, response shape preserved)
- `shared/search_serializer.py` — canonical envelope shape. 81A adds the `request` echo block; everything else preserved.
- `shared/api_errors.py` — error-code taxonomy. 81A reuses `invalid_request`, `invalid_combination`, `invalid_filter_value`, `query_required`, `query_too_long`. Does NOT add `regex_pattern_too_long` (D-09).
- `web/search_api.py` — concrete request/response surface. 81A modifies `SearchRequest` (replace `mode` with `search_mode` + add `responsa_options`); minor changes to `/api/parallels` handler (echo block).

### Endpoint behavior (locked — inherited)
- `.planning/phases/78-api-search-hardening-shell/78-CONTEXT.md` — D-01..D-24 hardening. Especially D-06 (per-route exception handling), D-07 (error-code taxonomy), D-15 (FiltersModel), D-20 (statelessness), D-14 (PostHog event shape — 81A extends with two new properties per D-08).
- `.planning/phases/79-api-browse-drill-down/79-CONTEXT.md` — locator semantics; not modified by 81A.
- `.planning/phases/80-api-parallels/80-CONTEXT.md` — D-01 (request shape), D-04 (filtered-key always present), D-07 (200-group cap + `truncated_to_200`), D-09 (mode property values: `exact|variants|fuzzy` — preserved per D-07 here).

### Core search engine (consumed, not modified)
- `genizah_core.py:2475` — `_MODE_PAIRS_COUNT['variants']=30` (the actual core value referenced in RESCOPE §3.1; replaces incorrect "top-25" wording).
- `genizah_core.py:6048-6090` — Responsa cascade `tr()` warning strings that flow into `warnings[]`.
- `genizah_core.py:7265-7266` — diacritic-stripping skip for regex (irrelevant after D-09 drops regex).
- `genizah_app.py:15788-15797` — desktop UI checkbox bindings that `responsa_options` mirror exactly.
- `web/search_api.py:85` — `_consume_last_responsa_downgrade` signal that feeds `warnings[]`.

### PostHog observability (extended)
- Existing per-request event from Phase 78 HARDEN-05 — 81A adds two properties: `search_mode_value` (str) and `responsa_options_count` (int, 0 when None or non-responsa).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets (consumed)
- **`shared/search_serializer.py`** — canonical envelope. 81A adds the `request` echo block alongside existing `count`, `total`, `warnings`, `results`. Single source of truth for both endpoints.
- **`shared/api_errors.py`** — error envelope shape `{error: {code, message}}` and existing codes. 81A reuses `invalid_request` and `invalid_combination`; no new codes (regex deferred per D-09).
- **`web/search_api.py`** — single source of truth for Pydantic request models (`SearchRequest`, `ParallelsRequest`, `BrowseRequest`, `FiltersModel`). 81A modifies `SearchRequest` and adds `ResponsaOptions`; touches `/api/parallels` handler for the echo block.
- **PostHog wrapper in `web/search_api.py`** — existing per-request event emission. 81A extends the event property dict (no new transport code).

### Established Patterns
- **`extra='forbid'` on every Pydantic request model** (Phase 78 baseline) — 81A relies on this for the hard cutover (D-13).
- **`@model_validator(mode='after')` for cross-field validation** (Phase 78 D-17 set the precedent) — 81A adds two new validators (responsa_options-mode coupling, gap-mode coupling) following the same shape.
- **`tr()` warnings flow into top-level `warnings[]` array** (Phase 78 HARDEN-03) — 81A surfaces Responsa cascade-disabled options through this same channel; `responsa_options_effective` is structured-data parallel.
- **Per-bucket independence** (Phase 78/79/80 D-05) — three rate-limit buckets stay independent; 81A doesn't add a fourth.

### Integration Points
- `web/search_api.py` — model definition site + endpoint handlers for `/api/search` and `/api/parallels`.
- `shared/search_serializer.py` — envelope construction; 81A may add `_apply_request_echo()` helper or inline (Claude's discretion).
- `tests/test_search_api.py` — existing 78-era tests rewritten in-place (D-12).
- `tests/test_search_api_v2.py` — new file for the matrix (D-12).
- `tests/test_search_serializer.py` — extend with `request` echo round-trip cases (per RESCOPE §8).
- `tests/test_parallels_api.py` — extend with `request` echo presence assertion + Phase 80 regression coverage.

</code_context>

<specifics>
## Specific Ideas

- **Echo block fidelity is the key contract.** Skills (81B) compare `responsa_options` vs `responsa_options_effective` (or read `warnings[]`) to detect silent server-side cascade. Both signals must agree on every Responsa cascade case — `tr()` string in `warnings[]` AND boolean diff in `responsa_options_effective`.
- **Hard cutover wording matters.** When the old `mode` field is sent, the 400 message must explicitly name both the old field and the new (`"unknown field 'mode' — use search_mode instead"`). Skill authors who copy-paste old payloads need a one-line debugging hint.
- **Regex absence is a surface contract.** Skill authors will inspect the enum at runtime; an attempt to send `search_mode='regex'` returns 422 (Pydantic enum constraint). Document the v7.11 candidacy in 82.
- **`/api/parallels` keeps `mode`, NOT `search_mode`.** Stylistic inconsistency is intentional (D-07 / OQ-2). Phase 82 documents it.

</specifics>

<deferred>
## Deferred Ideas

- **`search_mode='regex'`** (D-09) — moved from 81A scope to v7.11. UI continues to support regex; only the API surface is gated.
- **`'fuzzy'` re-introduction** under a name that matches the actual variant tier exposed (RESCOPE §3.1 NOTE).
- **`variants_extended` / `variants_maximum`** as either main-mode values or `responsa_options.variants` tiers — v7.11 candidate, gate behind a private flag if needed sooner.
- **`text_position` (start/end of text/line)** — join-finding workflow; v7.11.
- **Global `judeo_arabic` / `plene_defective` / `grammatical_prefix` / `grammatical_suffix` / `exclude_words[]`** — v7.11; would invent semantics that don't match the UI.
- **Rename `/api/parallels.mode` → `search_mode`** (D-07 / OQ-2) — v7.11, or whenever 81C ships.
- **Brief deprecation window for old `mode` field** — explicitly rejected (D-13). Could be revisited only if external integrations appear, which contradicts the internal-API posture.
- **Hashed IP in `request` echo** (D-10) — explicitly rejected. v7.11 could revisit if skill self-debugging becomes a real need.
- **Per-mode request timeouts / regex precompile guards** — moot after D-09 drops regex; revisit when regex returns in v7.11.

</deferred>

---

*Phase: 81A-api-contract-expansion*
*Context gathered: 2026-05-03*

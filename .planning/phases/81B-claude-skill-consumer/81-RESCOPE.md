# Phase 81 — Rescope Memo (Revision 3)

**Date:** 2026-05-02
**Status:** APPROVED 2026-05-02; ROADMAP.md / REQUIREMENTS.md / directory mutations applied.
**Supersedes:** `.planning/phases/81B-claude-skill-consumer/81B-CONTEXT.md` (formerly `81-claude-skill-consumer/81-CONTEXT.md` before the 2026-05-02 rename)
**Revision history:**
- **v1 (2026-05-01)** — initial split into 81A (full UI parity) + 81B (skill) + 81C (job API).
- **v2 (2026-05-02)** — UI-aligned single `search_mode` enum (not `field × match_mode`); defer extended/maximum variants, text_position, and full UI parity; demote 81C to optional/deferred; add browse-`text_source` honesty requirement to 81B; conservative API guardrails.
- **v3 (2026-05-02)** — User-approved with 7 corrections: (1) drop `'fuzzy'` from v7.10 enum (6 values, not 7); (2) variants description uses actual core value (30 pairs) not the wrong "top-25"; (3) Responsa cascade keeps `search_mode_effective='responsa'` — expose `responsa_options_effective` + `warnings` instead; (4) `responsa_options` does NOT expose `variant_mode`; (5) `/api/parallels.mode` documented as a temporary Phase 80 inconsistency, not unified; (6) skill does NOT import `genizah_core` — uses lightweight local normalization OR `/api/search?search_mode=shelfmark`; (7) skill pacing revised — 1.5 req/s was 90/min not 30/min; correct pacing is ≤0.4 req/s per bucket via token-bucket throttle.

---

## 1. Trigger

Live local testing after Phase 80 complete revealed that the v7.10 API as shipped is **useful but not yet expressive enough** to power the research workflows the skill needs to demonstrate.

Concrete findings from local testing:

1. **Phrase discovery works.** `/api/search` for `"את העולם כולו בטוב"` with `limit=100` returned 34 results in ~6s. After filtering titles already containing `ברכת המזון` / `ברכת מזון` / `Grace after meals`, candidate unidentified witnesses remained — including `T-S NS 97.49` (cataloged as `פיוט;שירת חול` but the snippet contains canonical Birkat Hamazon language) and `MS heb. f.7/31` (empty title, strong Birkat Hamazon snippet). **This is the v7.10 success path.**
2. **Mode coverage is wrong-shaped.** `/api/search` exposes `mode: text|Title|Shelfmark|Responsa` — title and shelfmark are properly modes (csv_bank-backed metadata search), but the `text` value collapses `exact|variants|regex|responsa|fuzzy` together. The fix is **not** to split on `field × match_mode`; it's to expose the UI's flat `search_mode` enum directly so the API matches what the user already controls in the UI.
3. **Parallels works for short text but times out on full compositions.** Full Birkat Hamazon × `variants` mode timed out locally. **Phrase-search discovery (finding 1) works well enough that v7.10 does not need long-running parallels.** The skill can do staged phrase mining via many `/api/search` calls. Long-running parallels stays an optional v7.11 deliverable unless the user explicitly promotes it.
4. **The existing `genizah-parallels` skill works from XLSX.** Inspected `C:\Users\gersh\Downloads\genizah-parallels-updated.skill` — it operates on Excel exports of MiDRASH/Genizah Search Pro results, comparing column I (`הקשר חיפוש`) vs column J (`תוכן כתב יד`), with OCR-tolerant matching, distinctive-phrase mining, and known-witness flagging. The XLSX is itself the manual output of the parallels-search UI. The new live skill mirrors the same analysis pipeline but drives it from `/api/search` + `/api/browse` results, not from full-composition parallels.

Building Phase 81 against today's API would yield a skill that can run `exact`-mode searches but cannot drive `variants` mode that surfaces unidentified witnesses behind OCR noise — i.e., the very capability the v7.10 acceptance harness is meant to demonstrate.

## 2. Recommendation

**Split Phase 81 into two required sub-phases plus one optional sub-phase.**

| Sub-phase | Title | Required for v7.10? | Scope summary |
|---|---|---|---|
| **81A** | Minimal UI-aligned API expansion | **Required** | Replace conflated `mode` with UI-aligned `search_mode` enum (7 values). Add `responsa_options` (4 flags) usable only in Responsa mode. Conservative guardrails (limit 50/100, query/text caps, rate limits unchanged). Echo requested/effective mode/options in response. **Breaking change** — internal/undocumented API. |
| **81B** | Claude Skill Consumer | **Required** | Reference Anthropic Skill (SKILL.md + scripts) using **staged phrase discovery** — multiple `/api/search` calls + merge by uid/sys_id + known-witness handling (exclude OR flag) + title/metadata filtering + `/api/browse` drill-down with honest `text_source` reporting + ranked candidates with grounded justifications. May use short synchronous `/api/parallels`; full-composition parallels deferred. |
| **81C** | Long-running parallels job API | **Deferred** to v7.11 unless explicitly promoted | Async job protocol for parallels-against-full-composition: POST → 202 + job_id, GET status, DELETE cancel. Sketched here for completeness only; not on the v7.10 critical path. |

**Phase 82** (Internal Documentation) is unchanged in identity but its scope grows to cover the expanded surface from 81A.

**Why two required + one optional, not three required:**

- **81A is contract work, not feature work.** It must lock and ship before any skill is built against it.
- **81B is the v7.10 milestone gate.** Its acceptance criterion is "user-observed live run on a scholarly query" — that gate fires once 81A exists. The local-testing finding that *phrase discovery works* means a staged-phrase-mining skill is sufficient.
- **81C is genuinely optional.** Long-running compositions are nice-to-have, not gating. Promoting 81C back to required is cheap if you reverse the call later — the design is sketched in §5 and the in-memory job store can be built in ~1–2 weeks.

## 3. Phase 81A — Minimal UI-aligned API Expansion

### 3.1 `/api/search` request shape

Single primary enum, UI-aligned. **No `field × match_mode` split, no extended/maximum variants in v1.**

```python
class SearchRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    query: str
    search_mode: Literal[
        'exact',       # literal match
        'variants',    # UI-equivalent basic variants (?) — top-30 orthographic pairs
                       # (genizah_core.py:2475 _MODE_PAIRS_COUNT['variants']=30, max_changes=1).
                       # Basic only; no extended/maximum in v7.10.
        'regex',       # power-user regex; safely bounded (see §3.4)
        'responsa',    # Responsa pipeline; pairs with responsa_options below
        'title',       # csv_bank metadata search on titles
        'shelfmark',   # csv_bank metadata search on shelfmarks
    ]
    # NOTE: 'fuzzy' is intentionally NOT in v7.10 — it would map to variants_maximum
    # which is deferred per §3.2. v7.11 may reintroduce it under a name that matches
    # the actual variant tier exposed.

    # Responsa-only options. Required when search_mode='responsa' (else ignored or rejected — see §3.5).
    responsa_options: Optional['ResponsaOptions'] = None

    # Common knobs (apply to text-side modes only)
    gap: int = 0                                       # proximity gap; ignored for title/shelfmark/responsa
    limit: int = Field(default=50, ge=1, le=100)       # ⚠ Lowered ceiling: 200 → 100 (§3.6)
    filters: Optional[FiltersModel] = None             # inherited from Phase 78


class ResponsaOptions(BaseModel):
    model_config = ConfigDict(extra='forbid')

    variants: bool = False         # basic variant expansion within Responsa pipeline
    ja: bool = False               # Judeo-Arabic article-form expansion
    flex_spacing: bool = False     # tolerate variable spacing (Hebrew/Arabic word boundaries)
    bidirectional: bool = False    # match terms in either order
```

The internal `variant_mode` ('exact' vs 'variants') is derived from `responsa_options.variants` server-side exactly as the desktop UI does (`genizah_app.py:15796`). It is NOT exposed in the API — clients control the boolean only.

### 3.2 What is intentionally NOT in v1

| Feature | Why deferred |
|---|---|
| `variants_extended` (??) | High Tantivy-pair count (70 pairs); too expensive for an open API. Defer to v7.11; gate behind a private flag if needed sooner. |
| `variants_maximum` (???) | Highest cost (150 pairs + 2-character changes). Same rationale; v7.11. |
| `text_position` (start/end of text/line) | Unique to join-finding workflow. No v7.10 skill needs it. v7.11. |
| Global `judeo_arabic` / `plene_defective` toggles | These are NOT global toggles in the codebase — they are Responsa query-syntax features (`%word`, `#word`, `word#`) parsed inside the Responsa pipeline. Exposing them as global API flags would invent new semantics that don't match the UI. |
| Global `grammatical_prefix` / `grammatical_suffix` toggles | Same: `#word` and `word#` are Responsa-internal token prefixes, not global flags. |
| Global `exclude_words[]` | UI exposes this only as a refinement step (post-search filter). Not needed for the first skill; defer. |
| `field × match_mode` split | UI doesn't model search this way; matching the UI is more important than orthogonal contract elegance. |
| Long-running parallels job API | Deferred to optional 81C; phrase-discovery via `/api/search` covers v7.10. |

### 3.3 Validation matrix

Cross-field validation rules. Violations return 400 `invalid_combination` with a message identifying both offending fields.

| Rule | Legal? | Enforcement |
|---|---|---|
| `search_mode='responsa'` + `responsa_options=None` | ✓ Legal — defaults to all-False ResponsaOptions | Pydantic default constructor when omitted |
| `search_mode='responsa'` + non-empty `responsa_options` | ✓ Legal | Pass-through |
| Any `search_mode != 'responsa'` + non-None `responsa_options` | ✗ 400 `invalid_combination` ("responsa_options is only valid when search_mode=responsa") | Custom Pydantic validator |
| `search_mode='title'` or `'shelfmark'` + non-zero `gap` | ✗ 400 `invalid_combination` ("gap has no effect with metadata-only search modes") | Custom validator |
| `search_mode='regex'` + non-empty `responsa_options` | ✗ 400 `invalid_combination` | Same custom validator |
| `search_mode='regex'` + non-zero `gap` | ✓ Legal but `gap` is silently ignored (regex pattern owns proximity); document in 82 | Pass-through |
| `query` empty after `.strip()` | ✗ 400 `query_required` | Existing |
| `len(query) > QUERY_LENGTH_CAP` (1000 chars) | ✗ 400 `query_too_long` | Existing |
| `limit > 100` | ✗ 422 (Pydantic Field constraint) | Pydantic |
| `limit < 1` | ✗ 422 | Pydantic |
| `filters.*` containing values not in FJMS catalog | ✗ 400 `invalid_filter_value` | Inherited Phase 78 D-17 |
| Old `mode` field present | ✗ 400 `invalid_request` ("unknown field 'mode' — use search_mode instead") | `extra='forbid'` |

### 3.4 Regex safety bounds

Regex mode is power-user; bound it conservatively to prevent ReDoS and excessive scan time.

- Pattern length cap: `len(query) <= 256` for `search_mode='regex'` (vs 1000 for other modes). Above → 400 `regex_pattern_too_long`.
- Pattern compile timeout: skip pattern-compile timing for v1 (Python `re` doesn't expose it cheaply); rely on the existing per-request timeout in `wrap_endpoint`.
- Catastrophic backtracking: rely on the existing executor timeout. Document in 82 that regex queries may be cancelled by the executor and return `partial_results=true` (existing partial-results signaling).
- Diacritic stripping: skipped for regex (matches existing core behavior at `genizah_core.py:7265-7266`).
- Toggles (`responsa_options`): rejected per §3.3.

### 3.5 Response shape: echo requested/effective mode/options

Add a `request` block to the envelope echoing what the server actually applied. **`search_mode` is always preserved** — Responsa cascade-downgrades affect which `responsa_options` were applied, not the mode itself. The mode stays `responsa`; the options that were silently disabled appear in `responsa_options_effective` and are also surfaced in `warnings[]`.

```jsonc
{
  "schema_version": 1,
  "source": "search",
  "count": 34,
  "total": 34,
  "warnings": [
    "Judeo-Arabic expansion disabled (cascade)",   // existing tr() string from genizah_core.py:6048
    "Plene/defective expansion disabled (cascade)"
  ],
  "generated_at": "2026-05-02T13:42:11Z",
  "request": {                              // NEW in 81A
    "search_mode": "responsa",
    "responsa_options": {                    // what the client sent
      "variants": true,
      "ja": true,
      "flex_spacing": false,
      "bidirectional": false
    },
    "responsa_options_effective": {          // what the server actually applied after cascade
      "variants": true,
      "ja": false,                           // disabled by cascade — see warnings[]
      "flex_spacing": false,
      "bidirectional": false
    },
    "gap": 0,
    "limit": 50,
    "limit_effective": 50,
    "filters": null
  },
  "results": [...]
}
```

**Behavior:**
- `search_mode` in the echo is always identical to the `search_mode` the client sent — even when Responsa cascades downgrade some options. `search_mode_effective` is **not** added (correction from v2 — would have been misleading).
- `responsa_options_effective` is present only when `search_mode='responsa'`. When all requested options were applied, `responsa_options == responsa_options_effective` (skills can either compare or read `warnings[]`).
- `warnings[]` carries the existing `tr()` strings from `genizah_core.py:6048-6090` (Judeo-Arabic disabled, plene/defective disabled, prefix/suffix disabled, etc.).
- `limit_effective` reflects any server-side cap (e.g., `MAX_LIMIT` enforcement).
- For non-Responsa modes, `responsa_options` and `responsa_options_effective` are both `null`.

Skills can compare `responsa_options` vs `responsa_options_effective` (or read `warnings[]`) to detect silent server-side cascade changes. The `_consume_last_responsa_downgrade` signal in `web/search_api.py:85` feeds the `warnings[]` array — its existing semantics are preserved.

### 3.6 Conservative guardrails

| Knob | Phase 78 baseline | 81A new |
|---|---|---|
| Default `limit` | 50 | 50 (unchanged) |
| Max `limit` | 200 | **100** (lowered) |
| Query length cap | 1000 | 1000 (unchanged); 256 for regex (new) |
| Composition length cap (parallels) | 20000 | 20000 (unchanged) |
| Per-IP rate limit | 30 req/min per bucket | 30 req/min per bucket (unchanged) |
| Number of rate-limit buckets | 3 (search, browse, parallels-sync) | 3 (unchanged in 81A; 81C would add a 4th) |
| `SEARCH_API_MODE` modes | open / localhost-only / disabled | unchanged |

Rationale: API may be open and skills generate many requests; widen later based on PostHog latency/result-count/error data, not speculatively.

### 3.7 `/api/parallels` minimal hardening (81A scope)

Phase 80 already shipped `/api/parallels` with a usable contract. **81A leaves it largely alone** — only two changes:

1. **Echo block** (mirrors §3.5): add `request` block to the parallels envelope so skills can detect server-side adjustments.
2. **Composition length cap clarity**: existing 20000-char cap stays. Document that compositions over ~5000 chars in `variants` mode may time out; recommend the skill use phrase discovery via `/api/search` for longer texts. Long-running parallels (81C) is deferred.

Phase 80's existing `mode: exact|variants|fuzzy` enum stays as-is in 81A (despite being stylistically inconsistent with `/api/search`'s new `search_mode`). Reasoning: changing it now means breaking Phase 80's tests + serializer that landed yesterday, and the skill won't drive long compositions through it anyway. **OQ-2** flags this for explicit decision.

### 3.8 Hardening inheritance

All hardening primitives from Phase 78/79/80 inherit unchanged:

- Three rate-limit buckets, mode gate (`SEARCH_API_MODE`), error envelope shape, error code taxonomy.
- PostHog event shape — new properties `search_mode_value`, `responsa_options_count` (count of True flags in ResponsaOptions, 0 if None).
- Statelessness, no auth, no CORS (server-to-server skill calls).

## 4. Phase 81B — Skill Consumer (decisions)

### 4.1 Architecture: staged phrase discovery, not full-composition parallels

The skill does **not** drive full-composition `/api/parallels` for the v7.10 acceptance run. Instead, it follows the discovery pattern that worked locally (finding 1):

```
1. User supplies:
   - query (single distinctive phrase) OR base_text (full composition)
   - optional: known_witnesses[] (shelfmark list)
   - optional: known_witness_policy ('exclude' | 'flag', default 'flag')

2. If base_text supplied:
   a. Skill extracts ~5–15 distinctive phrases from base_text (n-gram + Judeo-Arabic
      marker scoring, mirroring genizah-parallels Step 6).

3. For each query phrase:
   a. Call /api/search with search_mode='variants' (the witness-discovery default).
   b. Merge results into a candidate dict keyed by uid (preferred) or sys_id (fallback).
   c. Aggregate per-candidate match-count and best score across phrases.

4. Apply known-witness policy:
   - 'flag' (default): keep all candidates; mark known witnesses with a 'known_witness: true' flag.
   - 'exclude': drop known witnesses from candidate list entirely.

5. Title/metadata pre-filter:
   - Drop candidates whose catalog title is exclusively biblical/liturgical (port the
     loose-filter rules from genizah-parallels SKILL.md Step 1 — never aggressive).

6. Top-N selection (default 10, configurable up to 25):
   a. Rank by: distinct-phrase-hit-count desc, then aggregate score desc, then unidentified-
      catalog-title flag (matches in unidentified manuscripts surface first per
      genizah-parallels Step 4).

7. /api/browse drill-down for top-N:
   a. Fetch full text via /api/browse using locator from /api/search response.
   b. Honor text_source: if 'snippet', skill marks the candidate's justification with
      'full text unavailable; using snippet of N chars' (§4.2).
   c. Note image availability/unavailability in the candidate output.

8. Justification:
   a. For each top-N candidate, compose a 1–2 sentence Hebrew or English justification
      grounded in the fetched browse text + matching phrases.
   b. Tier candidates A/B/C per genizah-parallels Step 5 rules.

9. Output: ranked Markdown list (or JSON, configurable) with per-candidate:
   shelfmark, library, catalog title, tier, known-witness flag, matching phrases,
   justification (with source attribution), browse URL, image URL or 'no image'.

10. Final summary line: "Processed N phrases × M candidates → K top results
    (J failed: rate-limited / browse-snippet-only / NLI-image-missing).”
```

### 4.2 Browse honesty (NEW — addresses user finding §8)

`/api/browse` may return `text_source='snippet'` instead of full text when transcription is incomplete. The skill MUST surface this honestly:

- When `text_source != 'full'`, the skill's per-candidate justification appends a parenthetical: `"(full text unavailable; based on snippet of {len} chars)"`.
- When `image_url` is null or NLI returns 4xx for the image, the skill's output appends: `"(no image available)"`.
- Acceptance criterion **81B-AC7** locks this behavior.

### 4.3 Override of existing `81-CONTEXT.md` decisions

| ID | Original (existing 81-CONTEXT.md) | Override (81B revision 2) |
|---|---|---|
| D-04 | Skill exercises `/api/search` + `/api/browse` + `/api/parallels` | Required: `/api/search` + `/api/browse`. **Optional**: short-text `/api/parallels` (e.g., a 200-char snippet) when the skill judges it useful. Long-composition parallels NOT required. |
| D-05 | Hybrid ranking — API order + Claude justification | Refined: staged phrase discovery (§4.1) merges across many `/api/search` calls; ranking is by distinct-phrase-hit-count then score then unidentified-flag, not raw API order. Justifications grounded in `/api/browse` text per candidate. |
| D-06 | Top N = 10 | Default 10, configurable up to 25 (`GENIZAH_TOP_N` env or `--top-n` flag). Cap dropped from open-ended to bounded for rate-limit hygiene. |
| D-07 | Per-candidate inline error note + continue | Unchanged. Plus: 408/timeout from short `/api/parallels` triggers fallback to "use phrase discovery instead" (skip parallels gracefully, do not promote to 81C). |

### 4.4 New decisions in 81B revision 2

- **D-13:** Skill input shapes:
  - **Live mode (primary)**: `query` OR `base_text` + optional `known_witnesses[]` + optional `known_witness_policy='flag'|'exclude'`.
  - **JSON-input mode** (deferred — 81B Plan 02 or v7.11): ingest a `/api/search` envelope previously saved.
  - **Excel-input mode** (legacy, v7.11): ingest an XLSX export from MiDRASH/Genizah Search Pro.
- **D-14:** Local-data hook from existing `81-CONTEXT.md` D-03 stands — documented but not implemented.
- **D-15:** Distinctive-phrase extractor in step 2a is ported from genizah-parallels Steps 2 and 6 (n-gram + JA marker density). Client-side (in the skill); no server-side helper API in v7.10.
- **D-16:** OCR-tolerant matching (existing skill's Step 3 — whitespace-stripped substring match) runs against the `/api/browse` full-text for top tier-A candidates. Snippet-only (`text_source='snippet'`) candidates skip this pass with a flag in the justification.
- **D-17:** Known-witness policy is user-controlled (D-13). Default is `'flag'` because it's the safer-for-research default — researcher sees both new candidates and known witnesses, can decide.
- **D-18:** Shelfmark normalization for `known_witnesses[]` (resolves OQ-7). The skill does NOT depend on the GenizahSearch repo / `genizah_core`. Two-tier strategy:
  - **Tier 1 (default)**: lightweight local normalization — strip whitespace, normalize Unicode, collapse common library-prefix variants (`T-S`/`TS`/`Cambridge T-S`, `ENA`/`E.N.A.`, `MS heb`/`MS. heb.`, etc.). Skill keys candidate dict on this normalized form.
  - **Tier 2 (fallback)**: when Tier 1 produces no match for a `known_witness` against any candidate, the skill issues a `/api/search` call with `search_mode='shelfmark'` and the witness string as `query` to resolve the canonical `sys_id`, then re-keys on `sys_id`. This costs N additional calls (one per unmatched witness) and is bounded by the `known_witnesses[]` length.
  - SKILL.md documents both tiers and the shelfmark formats Tier 1 handles.
- **D-19:** Skill-side rate-limit pacing (resolves OQ-8). Token-bucket throttle with **separate buckets for `/api/search` and `/api/browse`**, default 24 req/min per bucket (0.4 req/s), burst capacity 5. `GENIZAH_SKILL_REQ_PER_MIN` env var overrides. The 1.5 req/s figure that appeared in earlier drafts was wrong (= 90/min, exceeds the 30/min server cap).

## 5. Phase 81C — Long-running parallels job API (DEFERRED, sketched only)

**Status:** Deferred to v7.11 unless explicitly promoted by the user.

**Sketch retained for design continuity:**

- `POST /api/parallels` with `async=true` returns 202 + `job_id`.
- `GET /api/parallels/jobs/{id}` returns `{status, progress, partial_results?, final_results?, expires_at}`.
- `DELETE /api/parallels/jobs/{id}` cancels.
- In-memory state store with TTL — but multi-worker deployments invalidate this (open question OQ-3 below).
- 4th rate-limit bucket.
- Background worker via `asyncio.create_task` + `run_in_executor`.

**Promotion conditions** (any one triggers reactivation):
- A v7.10 user says "I need full Birkat Hamazon parallels in the skill" and is willing to wait the additional 1–2 weeks of work.
- PostHog data after 81A+81B ship shows users hitting 5000+ char compositions repeatedly.
- A second skill is planned that requires long-running parallels (e.g., a literary-influence mapper).

## 6. Open Questions (post-revision)

Most v1 OQs are now resolved by the user's revision direction. Remaining:

| # | Question | Recommendation | Blocker for |
|---|---|---|---|
| OQ-1 | Should `'fuzzy'` stay in the v1 `search_mode` enum, or be dropped entirely? `fuzzy` internally aliases `variants_maximum` (which is deferred per §3.2), so keeping it would expose maximum-variants under a different name — defeating the deferral. **Recommendation: drop `'fuzzy'` from v1; document as v7.11 candidate.** Keep just 6 values: exact / variants / regex / responsa / title / shelfmark. | Drop fuzzy | 81A planning |
| OQ-2 | **RESOLVED (user, v3):** Phase 80's `/api/parallels` `mode: exact|variants|fuzzy` enum stays as-is in 81A. The temporary inconsistency with 81A's new `search_mode` is documented in Phase 82 as a known v7.10 quirk. Rename deferred to v7.11 (or whenever 81C ships, whichever is sooner). | Leave as-is + document | 81A planning |
| OQ-3 | Production deployment worker count. If Uvicorn runs >1 worker, in-memory job store breaks (only relevant if 81C is promoted). **Recommendation: verify before any 81C work; not blocking 81A or 81B.** | Verify if 81C promoted | 81C only |
| OQ-4 | Is there a v7.10 use case for `regex` mode in 81B? If the skill's primary path is variants-mode phrase discovery, regex may be unused in the first skill — and regex is the riskiest mode for ReDoS. **Recommendation: keep `regex` in 81A for completeness (it's UI-supported), but skill avoids it by default.** | Keep regex | 81A |
| OQ-5 | Should the `request` block (§3.5) include the IP-bucket-key (hashed) for skills to debug their own rate-limit behavior? **Recommendation: no — leak surface for hash inversion attacks. Skills can read 429 envelope when rate-limited.** | No | 81A |
| OQ-6 | Should `responsa_options` expose `'variants_extended'` / `'variants_maximum'` for power-user Responsa? **Recommendation: no — Responsa already cascades-downgrades on its own; mirroring main-mode deferral keeps the API consistent.** Note: `variant_mode` itself is NOT exposed; only the `responsa_options.variants` boolean is exposed (per user correction v3). | No | 81A |
| OQ-7 | **RESOLVED (user, v3):** Skill does NOT import `genizah_core` (external skill, can't depend on the GenizahSearch repo). Two acceptable strategies for `known_witnesses[]` shelfmark normalization: **(a) lightweight local normalization** in the skill — strip whitespace, normalize Unicode, collapse common library-prefix variants (T-S/TS, ENA/E.N.A.) — sufficient for direct-match equality; OR **(b) resolve via API** — for each `known_witness` string, issue a `/api/search` call with `search_mode='shelfmark'` to get the canonical `sys_id`, then key the candidate dict on `sys_id`. Strategy (b) is more accurate but costs N extra API calls. SKILL.md documents both; default is (a) with (b) as a fallback when (a) misses. | Lightweight local OR /api/search?search_mode=shelfmark | 81B |
| OQ-8 | **RESOLVED (user, v3):** Pacing math correction. 1.5 req/s = 90/min (NOT inside 30/min). The 30 req/min server limit is **per-bucket per-IP** — search and browse are separate buckets, so a skill run with 15 search + 10 browse calls fits in 60s only if each bucket stays under 30 req/min. Correct pacing: **≤0.4 req/s per endpoint bucket** (= 24 req/min, headroom for retries) via a token-bucket throttle that tracks `/api/search` and `/api/browse` separately. Concretely: skill maintains two token buckets (one per endpoint), 24 req/min refill, burst capacity 5. Configurable via `GENIZAH_SKILL_REQ_PER_MIN` env. | Token-bucket per endpoint, ≤24/min default | 81B |

## 7. Acceptance Criteria

### Phase 81A acceptance

| ID | Criterion |
|---|---|
| 81A-AC1 | `/api/search` accepts the new shape (`search_mode`, `responsa_options`) and rejects the old `mode` field with `invalid_request`. |
| 81A-AC2 | All 6 `search_mode` values (exact, variants, regex, responsa, title, shelfmark) produce non-empty results on at least one fixture query each. (`'fuzzy'` is NOT in v7.10 per OQ-1 user resolution.) |
| 81A-AC3 | All 4 `responsa_options` flags (`variants`, `ja`, `flex_spacing`, `bidirectional`) produce a measurable behavioral change on at least one Responsa fixture query each. |
| 81A-AC4 | Cross-field invalid combinations (`responsa_options` + non-responsa mode; `gap > 0` + title/shelfmark; `regex` + responsa_options) return 400 `invalid_combination`. |
| 81A-AC5 | `limit > 100` returns 422; `limit < 1` returns 422; `query` length > 1000 returns 400; `query` length > 256 with `search_mode=regex` returns 400 `regex_pattern_too_long`. |
| 81A-AC6 | Response envelope echoes `request` block with `search_mode` (always identical to client request — never downgraded), `responsa_options` (when applicable), `responsa_options_effective` (when applicable), `gap`, `limit`, `limit_effective`, `filters`. Responsa cascade case shows `responsa_options != responsa_options_effective` AND surfaces the disabled options as `tr()` strings in `warnings[]`. |
| 81A-AC7 | Existing Phase 78/79/80 hardening behaviors (rate limit, mode gate, error envelope, PostHog capture, statelessness) hold for the expanded surface. Per-bucket independence test passes. |
| 81A-AC8 | `/api/parallels` envelope gains the `request` echo block; existing Phase 80 tests pass unchanged otherwise. The `mode` field name is preserved on `/api/parallels` (not renamed to `search_mode`) per user OQ-2 resolution; Phase 82 documents the temporary inconsistency. |

### Phase 81B acceptance

| ID | Criterion |
|---|---|
| 81B-AC1 | Skill is runnable from a clean checkout against a configurable base URL (env var + CLI flag, env wins). Defaults to production. |
| 81B-AC2 | Skill accepts `query` OR `base_text`, optional `known_witnesses[]`, optional `known_witness_policy='flag'|'exclude'`. |
| 81B-AC3 | Live mode on a representative scholarly query (user-supplied at run time) produces a ranked candidate list. Each candidate has: shelfmark, library, catalog title, tier (A/B/C), known-witness flag, matching phrases, justification grounded in browse text, browse URL, image URL or "no image" note. |
| 81B-AC4 | Distinctive-phrase extractor (D-15) extracts ≥3 distinct phrases from a typical scholarly base text (~2000 chars). Extracted phrases include at least one author signature or distinctive technical term when present. |
| 81B-AC5 | Known-witness policy works both ways: `'exclude'` removes known witnesses from output; `'flag'` keeps them with `known_witness: true`. Default is `'flag'`. |
| 81B-AC6 | Skill handles 429, timeout, and partial-NLI gracefully (per-candidate inline note + continue; final summary line). |
| 81B-AC7 | Browse honesty: when `/api/browse` returns `text_source='snippet'`, the candidate's justification appends `"(full text unavailable; based on snippet of N chars)"`. When image_url is null/4xx, appends `"(no image available)"`. |
| 81B-AC8 | Skill self-paces using a token-bucket throttle, **separate bucket per endpoint** (search and browse). Default ≤24 req/min per bucket (= 0.4 req/s, headroom under the server's 30 req/min limit). Burst capacity 5. Configurable via `GENIZAH_SKILL_REQ_PER_MIN`. A single skill run with 15 search + 10 browse calls completes without triggering its own rate limit. |
| 81B-AC9 | Live user-observed run on at least one real scholarly query, signed off by the user. |

### Phase 81C acceptance (only if promoted)

Deferred. See §5 for sketch.

## 8. Test Plan

### Unit / integration (pytest)

- `tests/test_search_api_v2.py` (new) — coverage of all 6 (or 7 if `'fuzzy'` kept) `search_mode` values × all 4 `responsa_options` × invalid-combination matrix. ~50–60 cases.
- `tests/test_parallels_api.py` (extend) — `request` echo block presence + Phase 80 regression coverage.
- `tests/test_search_serializer.py` (extend) — `request` block round-trips through `serialize_search_payload` and `serialize_parallels_payload`.

### Skill-pipeline pytest (81B)

- `tests/test_genizah_skill.py` (new, lives in skill repo not main repo) — mocked `/api/search` and `/api/browse` responses; verify staged phrase discovery merges by uid, applies known-witness policy correctly, ranks by distinct-phrase-hit-count, surfaces snippet-only browse responses honestly.

### Spike (before 81A planning lock)

- One-off script: send representative `/api/search` calls in all 6 search_modes against the dev API; capture latency + result counts. Verifies that the deferred extended/maximum modes are indeed slower than basic variants (justifies the deferral). ~1 hour; lives in `_tmp/spike_search_modes/`.

### Live acceptance run (81B phase gate)

- User-observed end-to-end run of the skill against the production deployment with at least one scholarly query the user picks at run time. User signs off on the candidate ranking, tier classification, and browse-honesty annotations.

### Regression

- Existing 1156 pytest passes plus new tests; CI green on Ubuntu and Windows runners.
- Manual smoke: `/search` and `/parallels` UI pages still work (no regression in toolbar JSON download from Phase 77).

## 9. Smallest Shippable Increment

**Recommended v7.10 ship target: 81A (minimal) + 81B (staged phrase discovery).** No 81C.

If milestone scope must compress further, cut order from largest-cut to smallest-cut:

1. **Cut 81B's distinctive-phrase extractor** (D-15) → skill becomes single-query-only; user must supply a phrase, no `base_text` extraction. Loses witness discovery on long compositions but keeps `/api/search` driving and `/api/browse` drilling.
2. **Cut 81B's known-witness policy** → skill always reports all candidates (no exclude/flag distinction). User filters manually.
3. **Cut 81A's `regex` mode** → defers regex to v7.11. Losing it removes power-user pattern queries; main flows unaffected.
4. **Cut 81A's `responsa_options` flag exposure** → `responsa` mode works with default-False flags only. Reduces 81A test surface but loses bidirectional/flex_spacing/ja which are real research levers.

Below cut #2 the milestone fails its core acceptance ("skill demonstrates witness discovery against real OCR noise"). **The realistic v7.10 floor is 81A + 81B with all described features intact.**

## 10. Risks

| ID | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Variants mode is too slow on the production index for live skill use | Low | Medium | Local testing showed 6s for 100 results in variants — acceptable. Spike (test plan §8) confirms before 81A locks. If too slow, add per-mode timeout and surface `partial_results=true` in envelope. |
| R2 | Validation matrix has unforeseen invalid combinations that surface in production | Low | Low | 81A-AC4 covers explicitly listed invalid combos; remaining gaps documented in 82. |
| R3 | Skill's distinctive-phrase extractor produces too many or too few phrases for typical compositions, blowing rate-limit budget | Medium | Medium | Hard cap: max 15 phrases per run. Token-bucket throttle at ≤24 req/min per endpoint (81B-AC8 / D-19). |
| R4 | Skill's browse-honesty annotations clutter the user-visible output | Low | Low | Annotations are opt-out via `--quiet` flag; default is on so the user always knows what's known. |
| R5 | Live acceptance run fails on the first user-supplied scholarly query | Low | High (milestone gate fails) | Dry rehearsal with a query the user has already validated locally. AC says "at least one query" so retry is fine. |
| R6 | Regex mode triggers ReDoS or excessive scan time | Low | Medium | 256-char pattern cap (§3.4); existing executor timeout catches catastrophic backtracking. |
| R7 | API users (the skill or third parties) fan out enough requests to overwhelm the 30 req/min bucket | Medium | Medium | Conservative limits (§3.6): max=100, lowered from 200. Skill self-paces. PostHog monitors. |
| R8 | Phase 80's `/api/parallels` `mode` enum is stylistically inconsistent with 81A's `search_mode` and confuses skill authors | Low | Low | Document the inconsistency in 82; rename in v7.11 if 81C is built (OQ-2). |
| R9 | `text_source='snippet'` is more common than expected, and the skill's justifications become low-confidence | Medium | Medium | Browse-honesty annotations expose this clearly (AC7). PostHog `text_source` distribution monitored. If too high, prioritize transcription completeness as a separate v7.11 effort. |

## 11. Deferred Items (post-v7.10)

- **v7.11 — `variants_extended` / `variants_maximum`** modes on `/api/search`. Gate behind a private API-key flag if needed sooner; otherwise wait for PostHog data showing variants is too coarse.
- **v7.11 — `text_position` / start-of-text/end-of-text/start-of-line/end-of-line** for join-finding skill. The API surface design lives in this memo's git history; recover from `81-RESCOPE.md@v1` if needed.
- **v7.11 — Long-running parallels job API (81C)** per §5.
- **v7.11 — `/api/distinctive_phrases` server-side helper.** Skill's client-side phrase extractor (D-15) is sufficient for v7.10.
- **v7.11 — Excel-input legacy mode in 81B** (D-13).
- **v7.11 — `boundary_*` knob exposure on `/api/parallels` beyond `boundary_mode`.** Phase 80 deferred 4 of 5 boundary knobs; defer the rest.
- **v7.11 — Persistent job store (Redis/SQLite)** if multi-worker production confirmed AND 81C promoted.
- **v7.11 — In-repo CI smoke test for the skill** (existing 81-CONTEXT.md deferred).
- **v7.11 — Skill JSON / NDJSON output formats** (existing 81-CONTEXT.md Claude's Discretion).
- **v7.11 — Local-data hook for skill** (`Genizah_Index/` Tantivy + `transcriptions.txt`).
- **v7.11 — Rename Phase 80's `/api/parallels.mode` → `search_mode`** for stylistic consistency (OQ-2).
- **Future skill — Join-finder** using `text_position`. The 81A surface today does NOT support this; new contract phase needed.

## 12. Approval Status (v3 — APPROVED)

User approved on 2026-05-02 with the seven corrections that drove revision 3 (see revision history at the top of this memo). The following mutations are now authorized and will be performed:

- [x] **Adopt the two-required + one-optional split.** ROADMAP.md Phase 81 entry fans out into 81A + 81B; 81C added to v7.11 backlog; v7.10 milestone phase count: 6 → 7.
- [x] **Add new requirements** (REQUIREMENTS.md): `API-EXPAND-01..08` (one per 81A-AC). Existing `SKILL-01..03` move to 81B. Add `SKILL-04` (browse-honesty), `SKILL-05` (known-witness policy), `SKILL-06` (token-bucket pacing).
- [x] **Mark existing `81-CONTEXT.md` as superseded.** `git mv .planning/phases/81-claude-skill-consumer/ → .planning/phases/81B-claude-skill-consumer/`. New dir: `81A-api-contract-expansion/`. No `81C-` directory in v7.10.
- [x] **OQ-1 resolved**: drop `'fuzzy'` from v7.10 enum. Six values: exact / variants / regex / responsa / title / shelfmark.
- [x] **OQ-2 resolved**: leave `/api/parallels.mode` as-is in 81A; document the temporary inconsistency in 82.
- [x] **OQ-7 resolved**: skill does NOT import `genizah_core`; lightweight local normalization (D-18 Tier 1) with `/api/search?search_mode=shelfmark` fallback (Tier 2).
- [x] **OQ-8 resolved**: token-bucket throttle, separate buckets per endpoint, default 24 req/min per bucket (≤0.4 req/s). Earlier 1.5 req/s figure was wrong.
- [ ] OQ-3, OQ-4, OQ-5, OQ-6 carry recommendations into formal discuss-phase for 81A/B unless re-opened.

Next commands after the mutations land:

1. `/gsd-discuss-phase 81A` — formal discuss-phase for the API contract (this memo's §3 + open questions becomes the input).
2. `/gsd-discuss-phase 81B` — supplemental discuss for the skill (most of existing `81-CONTEXT.md` carries over with overrides from §4).

Or, single pass: `/gsd-discuss-phase 81A --power` and reuse outputs for 81B with the §4 deltas applied.

---

*Memo: 81-RESCOPE.md (revision 3 — APPROVED)*
*Author: Claude Opus 4.7*
*Date: 2026-05-02*

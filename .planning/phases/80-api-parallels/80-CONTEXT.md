# Phase 80: /api/parallels — Context

**Gathered:** 2026-05-01
**Status:** Ready for planning
**Source:** Brief inline discussion (skipped /gsd-discuss-phase) — most decisions inherit from Phase 78; only parallels-specific deltas captured below.

<domain>
## Phase Boundary

`POST /api/parallels` exposes the existing composition/parallels pipeline (`SearchEngine.search_composition_logic` in `genizah_core.py:7675`) through the same payload, locator, error envelope, and hardening conventions Phase 78 established for `/api/search`. Same module (`web/search_api.py`), same hardening primitives (`web/api_hardening.py`), same serializer (`shared.search_serializer.serialize_parallels_payload` from Phase 77).

**Inheritance default:** unless explicitly overridden in the Parallels-Specific Decisions section, every Phase 78 decision (D-01..D-24) applies verbatim — rate limiter pattern, mode-gate semantics, error envelope shape, error code taxonomy, query-validation strategy (Pydantic + per-route handler), filter shape (`FiltersModel`), FJMS validation (`validate_filter_values`), PostHog observability (already pinned to `endpoint='parallels'` in 78-D-14), IP-hash, sampling, statelessness, module location. Phase 79's `/api/browse` rate-limit-bucket precedent extends to Phase 80.

</domain>

<decisions>
## Implementation Decisions

### Inherited from Phase 78 (no change)

- **Module location:** `web/search_api.py` adds `POST /api/parallels` route + a `ParallelsRequest(BaseModel)` Pydantic model. Mounted by the existing `init_search_api(app_override=None)` (78-D-18) — no second registrar.
- **Hardening:** rate limiter, mode gate (`SEARCH_API_MODE` ∈ open|localhost-only|disabled), env-var rate-limit knob (`SEARCH_API_RATE_LIMIT`), error envelope, error codes from 78-D-07, query-cap-style validation, statelessness contract (78-D-20).
- **Validation strategy:** Pydantic `model_config = ConfigDict(extra='forbid')`. Errors route through `wrap_endpoint`'s try/except branches (Phase 78 R2-#6) — NOT global FastAPI handlers. Inherited from Phase 78 Concern #2 lock-in.
- **Filter shape:** reuse `FiltersModel` from `web/search_api.py` verbatim. Same plural keys (`domains`, `authors`, `works`, `materials`, `date_from`, `date_to`); same `extra='forbid'`. Inclusion-only (78-D-16). FJMS resolution via `validate_filter_values` (78-D-17).
- **PostHog event:** `search_api_request` with `endpoint='parallels'` (78-D-14). Properties `latency_bucket`, `result_count_bucket`, `status_code`, `error_code` echoed verbatim. **Add** `mode` = the parallels mode string (`'exact'|'variants'|'fuzzy'`) — same property key as /api/search, semantic differs by endpoint. NEVER log `text`, `filters`, response items, or composition source.
- **Serializer:** call `shared.search_serializer.serialize_parallels_payload(...)` from Phase 77. Result envelope shape (locator-on-every-item, `schema_version=1`, `source='parallels'`, `count`, `total`, `warnings`, `generated_at`, `results`, `filtered`) is owned by that function — Phase 80 contributes only the input arguments (main_results, filtered_results, source_text, chunk_size, mode, max_freq, boundary_options, warnings).

### Parallels-Specific Decisions

#### D-01: Request body shape

```python
class ParallelsRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    text: str                                                # composition body — REQUIRED
    chunk_size: int = 5                                      # n-gram chunk length
    mode: Literal['exact', 'variants', 'fuzzy'] = 'exact'    # locked enum (D-02)
    max_freq: Optional[float] = None                         # filter threshold; None = no filter
    boundary_mode: Literal['full', 'boundary', 'combined'] = 'full'  # D-03
    filters: Optional[FiltersModel] = None                   # reuse Phase 78 FiltersModel
```

Field semantics:
- `text` is the composition source. Maps to `full_text` arg of `search_composition_logic`. Stripped before length validation.
- `chunk_size`: integer, validated bounds `[2, 20]` (Pydantic `Field(ge=2, le=20)`). UI default is 5; below 2 produces no chunks; above 20 saturates index seeks.
- `max_freq`: optional float; when set, chunks whose match frequency exceeds the threshold get diverted to `filtered[]` (existing core behavior). When `None`, no high-freq filtering — all hits in `results[]`, `filtered: []`.

#### D-02: Mode enum locked to `exact | variants | fuzzy`

The Lab Engine path (`lab_composition_search`) is **out of scope** for v7.10. Rationale: LabEngine requires LabSettings + heavyweight setup; not skill-friendly. The Phase 77 Plan 05 follow-on commits already aligned both paths' `chunk_hits` shape, so the skill gets equivalent data through the standard path. Lab mode deferred to v7.11.

#### D-03: Boundary options — single field only

Expose only `boundary_mode: Literal['full','boundary','combined'] = 'full'`. The other 4 core knobs (`boundary_delimiter`, `boundary_boost`, `min_boundary_matches`, `min_delimiter_distance`) use the existing defaults (`'\n'`, `1.5`, `0`, `3`) inside the handler — NOT exposed in the request schema. Skill (Phase 81) won't touch these in v7.10. Future v7.11 may extract a `BoundaryOptions(BaseModel)`.

#### D-04: Filtered-hits surface — always emit `filtered` array

The response **always** includes a `filtered` key, even when empty (`filtered: []`). Mirrors the Phase 77 `serialize_parallels_payload` output exactly. Skill code can rely on the key existing without a `'filtered' in resp` check. SC-2's "documented behavior applied consistently across at least three sample compositions" is satisfied trivially by the always-present key — three sample tests (text/gap/Responsa-equivalent) assert `'filtered' in body` regardless of content.

**Note:** "gap mode" in SC-2 corresponds to `mode='variants'` with non-zero variant tolerance, NOT the `gap` field used by /api/search. Phase 80 has no `gap` field — `mode='variants'` is the parallels-equivalent.

#### D-05: Separate rate-limit bucket `_parallels_rate_limiter`

Mirrors Phase 79's `_browse_rate_limiter` precedent (`tests/test_browse_api.py::test_rate_limit_independence`). Each endpoint has independent 30 req/min budget driven by the same `SEARCH_API_RATE_LIMIT` env var (so the knob is shared even if the buckets aren't). Test enforces independence: 31 bursts on /api/parallels → 30+1×429, /api/search and /api/browse buckets unaffected.

The bucket is stored as a module-level singleton in `web/search_api.py` alongside `_rate_limiter` (Phase 78) and `_browse_rate_limiter` (Phase 79). Three buckets total.

#### D-06: Composition text length cap

Composition text can legitimately be much longer than a search query (whole piyutim, parts of liturgy). Cap at **20000 chars** after `.strip()` (~3000 Hebrew words). Reject with new error code `composition_too_long` (HTTP 400, message echoes cap and submitted length). Add to ERROR_CODES in `shared/api_errors.py`.

Empty `text` after `.strip()` → `composition_required` (HTTP 400). Both codes are parallels-specific; the existing `query_required` / `query_too_long` codes stay search-only to keep error messages honest about which field failed.

#### D-07: Result cap — group cap, not raw-hit cap

The `serialize_parallels_payload` envelope's `count`/`total` count groups (one per `sys_id`), not raw chunk-hit rows. Apply the **same 200 group cap** as /api/search. Default: no per-request `limit` field — return all groups up to 200. If group count exceeds 200, return the top 200 (sorted desc by `aggregate_score` per Phase 77 D-13) and emit a warning `'truncated_to_200'` in `warnings[]`.

Rationale: parallels users typically want every match; a `limit` knob adds surface for v7.11.

#### D-08: Locator round-trip test (SC-4)

A test in `tests/test_parallels_api.py` posts a composition that produces at least one parallels result, extracts the `locator` block from the first result, and issues a `/api/browse` GET with that locator. The browse response status_code must be 200. Mirrors Phase 79's `test_browse_real_round_trip_search_to_browse` env-gated PRIMARY round-trip. Env-gated `@pytest.mark.skipif(no_fixture_corpus)` like Phase 79 — locked-in unit tests do NOT depend on corpus availability.

#### D-09: PostHog `mode` property — parallels-specific values

The shared `mode` property in the PostHog event takes the parallels mode string (`'exact'|'variants'|'fuzzy'`) for /api/parallels requests. /api/search continues to send `'text'|'Title'|'Shelfmark'|'Responsa'`. Distinct value spaces are fine — PostHog dimensions are by-event, and the `endpoint` property already disambiguates.

#### D-10: Test surface (`tests/test_parallels_api.py`)

Mirror the Phase 79 fixture pattern (bare FastAPI app + `init_search_api` + TestClient). Coverage:
- **Happy paths** — exact / variants / fuzzy modes each return Phase 77 envelope; `boundary_mode='full'` and `'boundary'` and `'combined'` each return populated `results[]`
- **Locator** — every result item has `uid` AND `locator: {sys_id, volume_ie, p_num}`
- **Validation** — missing `text`, empty `text`, text > 20000 chars, `chunk_size` < 2, `chunk_size` > 20, unknown mode, unknown filter key, malformed JSON
- **Filtered key** — always present; non-empty when `max_freq` is set and high-freq chunks exist; empty `[]` when `max_freq=None`
- **Hardening parity** — mode-gate (disabled→503, localhost-only→403/200), rate-limit bucket independence (D-05 test), error envelope shape, statelessness (two identical posts diff only in `generated_at`)
- **Locator round-trip** — D-08 env-gated PRIMARY test
- **`@wrap_endpoint`** — uses the same decorator as /api/search and /api/browse so latency/result_count buckets and PostHog capture happen identically

#### D-11: Service-layer extraction

Mirror Phase 79 D-23: extract a `shared/parallels_service.py` (or `shared/composition_service.py`) with a `fetch_parallels_results(...)` async function that calls `search_composition_logic` via the singleton SearchEngine and returns a `ParallelsResultBundle` dataclass. The route handler stays thin and stateless.

**Decision deferred to planner:** if the existing call site in `web/pages/parallels.py:2120-2280` is already clean enough (or extraction risks UI regression), the planner may skip the service layer and let the handler call `state.searcher.search_composition_logic` directly — the same pragmatic boundary Phase 79 honored ("UI continues to drive its own enrichment via the existing BrowseState-mutating path"). Default recommendation: **extract**, because Phase 81 skill testing benefits from a non-UI-coupled async call signature.

### Claude's Discretion

- **Service-layer file name** — `shared/parallels_service.py` vs `shared/composition_service.py` vs inline-in-handler. Default to extraction per D-11 if context budget allows.
- **`limit` request field for v7.11** — out of scope for v7.10 (D-07 cap is hardcoded 200). Folded into deferred ideas.
- **Hardening test layout** — keep `tests/test_api_hardening.py` Phase 78-only (no parallels-specific tests there); put all parallels coverage in `tests/test_parallels_api.py`. Mirrors Phase 79's `tests/test_browse_api.py` discipline.
- **Variant-mode score thresholds** — `search_composition_logic` accepts the same thresholds the UI exposes; pass through whatever the existing call site uses unless skill testing reveals a need to expose them.
- **Empty-results behavior** — composition text with zero matches → `count=0, total=0, results=[], filtered=[]`. NOT an error. Matches /api/search empty-result behavior.
- **`max_freq` value range** — float, no Pydantic bounds. Composition core already handles edge values gracefully.

### Folded Todos

None.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase / milestone specs
- `.planning/ROADMAP.md` §`Phase 80: /api/parallels` — success criteria SC-1..SC-4 and the parity/round-trip phase gate.
- `.planning/REQUIREMENTS.md` §`API Endpoints` — API-02 (parallels endpoint shape).
- `.planning/STATE.md` — v7.10 milestone position; Phase 78 + 79 verified PASSED.

### Phase 77 lock (serializer + envelope shape)
- `.planning/phases/77-serializer-json-export/77-CONTEXT.md` — D-01..D-14 lock the response shape Phase 80 emits. Especially D-13 (`matches[]` per uid from `chunk_hits`) and D-14 (`serialize_parallels_payload` is the SOLE producer of result item shape).
- `shared/search_serializer.py:791` (`serialize_parallels_payload`) — the canonical envelope builder. Phase 80 calls this verbatim; do NOT reimplement grouping or matches[] aggregation.

### Phase 78 lock (hardening + envelope mechanics)
- `.planning/phases/78-api-search-hardening-shell/78-CONTEXT.md` — D-01..D-24 are inherited unchanged unless explicitly overridden above. Especially D-06 (per-route exception handling, NOT global), D-15 (FiltersModel), D-17 (FJMS validation), D-20 (statelessness contract).
- `web/search_api.py` — Phase 78 module. Phase 80 adds the parallels endpoint here; do NOT create a new module file.
- `web/api_hardening.py` — Phase 78 hardening primitives. Phase 80 imports `RateLimiter`, `wrap_endpoint`, `enforce_mode_gate`, `capture_api_event`, `_build_envelope_response` verbatim.
- `shared/api_errors.py` — Phase 80 ADDS two new codes: `composition_required`, `composition_too_long`, `truncated_to_200` (warning code, not error).

### Phase 79 lock (separate-bucket precedent)
- `.planning/phases/79-api-browse-drill-down/79-CONTEXT.md` — D-23 (service-layer extraction precedent for D-11), and the separate-rate-limit-bucket pattern.
- `tests/test_browse_api.py::test_rate_limit_independence` — the test pattern Phase 80 mirrors for D-05.
- `shared/browse_service.py` — file-naming and async-fan-out pattern for D-11 (if extraction is taken).

### Existing core (single source of truth)
- `genizah_core.py:7675` (`SearchEngine.search_composition_logic`) — the function the API calls. Read its signature; do NOT modify it for v7.10.
- `genizah_core.py:1244` (`SearchEngine.lab_composition_search`) — out-of-scope per D-02. Mentioned only so the planner does NOT call it.
- `web/pages/parallels.py:624-632` — confirms the `mode` enum values (`exact|variants|fuzzy`) D-02 locks.
- `web/pages/parallels.py:2120-2280` — existing search-execution call site. Useful as a reference for parameter passing; not a refactoring target.

### Cross-phase obligations
- Phase 78's `_rate_limiter` MUST stay independent of Phase 80's `_parallels_rate_limiter` (D-05) — bucket-independence test is the gate.
- Phase 79's `_browse_rate_limiter` MUST stay independent of both (3-bucket assertion).
- Phase 81 (Claude Skill Consumer) consumes /api/parallels alongside /api/search and /api/browse — the locator on every parallels item must round-trip through /api/browse without per-producer adjustment (SC-4).

</canonical_refs>

<specifics>
## Specific Ideas

- **Round-trip test corpus** — D-08 PRIMARY test needs at least one composition string that reliably produces a parallels result against the test fixture corpus. Phase 79's test corpus (env-gated, skipped when absent) probably has a usable composition; planner verifies and reuses or contributes a new fixture.
- **Re-use Phase 78's `_consume_last_responsa_downgrade` thread-local** — `mode='variants'` may trigger the same cascade-downgrade path in core; if it does, the warning surfaces through the same mechanism and lands in `warnings[]`. Planner verifies whether `search_composition_logic` triggers that signal; if not, no integration needed.
- **`source_text` in serializer** — pass the *trimmed* `text` from the request through to `serialize_parallels_payload`'s `source_text` arg so the envelope echoes what the caller sent (Phase 77 D-06).

</specifics>

<deferred>
## Deferred Ideas

- **Lab Engine composition path** (`lab_composition_search`) — v7.11 candidate. Per D-02, requires LabSettings + LabEngine integration; not skill-friendly.
- **Full boundary-options surface** (5 knobs) — v7.11 candidate. Per D-03, only `boundary_mode` exposed in v7.10.
- **`limit` / `max_results` request field** — v7.11 candidate. Per D-07, the 200-group cap is hardcoded in v7.10.
- **Filter exclusion (`*_exclude` kwargs)** — v7.11 candidate. Inherited from Phase 78 D-16 (inclusion-only).
- **Per-request boundary tuning** — v7.11 candidate (paired with full boundary-options surface).
- **Variant-mode score threshold exposure** — v7.11 candidate; defaults from existing UI call site for v7.10.
- **CORS** — none for v7.10 (inherited from Phase 78). Skill calls server-to-server.

</deferred>

---

*Phase: 80-api-parallels*
*Context gathered: 2026-05-01 via inline brief discussion (4 AskUserQuestion decisions captured: filtered-key always, single boundary_mode field, separate rate bucket, standard-path-only). Phase 78 inheritance default for everything else.*

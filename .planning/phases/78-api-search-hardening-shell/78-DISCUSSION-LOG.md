# Phase 78: /api/search + Hardening Shell - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-04-28
**Phase:** 78-api-search-hardening-shell
**Areas discussed:** Rate limiter + mode flag (transport gate); Error envelope + validation + caps; PostHog server-side observability; Filter shape + route mounting + tests
**External review:** Codex CLI consulted on all four clusters mid-discussion; recommendations adopted in full

---

## Rate limiter + mode flag (transport gate)

### Q1: Rate limiter implementation

| Option | Description | Selected |
|--------|-------------|----------|
| Reuse in-memory pattern | Generalize `_check_puzzle_rate_limit` into a helper. Per-IP dict, sliding 60s window, ~30 lines. Zero new deps. | ✓ (initially) |
| Pull slowapi | Add slowapi (Starlette/FastAPI rate-limit lib). More featureful but adds a dependency. | |

**User's choice:** Reuse in-memory pattern (Recommended).
**Codex pushback (adopted):** Don't *literally* clone `_check_puzzle_rate_limit` — it's fixed-window and returns bare 429. The new helper should use **per-IP `deque[float]` (true sliding 60s window)** and compute `Retry-After` from the oldest in-window timestamp. Locked as D-01.

### Q2: Env var naming for rate-limit knob

| Option | Description | Selected |
|--------|-------------|----------|
| Single `SEARCH_API_RATE_LIMIT` | One int, requests/min, default 30. | ✓ |
| Split: `SEARCH_API_RATE_PER_MIN` + `SEARCH_API_RATE_BURST` | Separate steady-state + burst tolerance. | |
| Family matching puzzle pattern (`SEARCH_API_RATE_LIMIT_PER_MIN`) | Mirror existing puzzle naming. | |

**User's choice:** Single `SEARCH_API_RATE_LIMIT` (Recommended). Locked as D-02.

### Q3: `localhost-only` detection behind nginx

| Option | Description | Selected |
|--------|-------------|----------|
| Trust X-Forwarded-For first hop | XFF first hop; fall back to `request.client.host`. Loopback set 127.0.0.1/::1/RFC1918. | ✓ (initially) |
| Strict `request.client.host == 127.0.0.1 / ::1` | Don't trust XFF at all. | |
| Both: XFF first, fall back to client.host | Functionally equivalent to the recommended option. | |

**User's choice:** Trust X-Forwarded-For first hop (Recommended).
**Codex pushback (adopted):** `localhost-only` should mean **loopback only** (127.0.0.1, ::1) — NOT RFC1918. RFC1918 ranges would be a "private-network" mode, semantically different. Trust XFF first hop only when `request.client.host` is itself loopback (i.e. immediate peer is nginx). Locked as D-03.

### Q4: `disabled` response code

| Option | Description | Selected |
|--------|-------------|----------|
| 503 + error envelope | `{error:{code:'disabled', message:'Search API disabled'}}`. Semantically correct. | ✓ |
| 404 Not Found | Pretends route doesn't exist. Breaks envelope-on-every-error contract. | |
| 403 Forbidden | Implies auth decision; misleading. | |

**User's choice:** 503 + error envelope (Recommended). Locked as D-04.

---

## Error envelope + validation + caps

### Q5: Validation strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Pydantic + global exception handler | `SearchRequest(BaseModel)`; FastAPI exception_handler rewrites 422 → envelope. | ✓ (initially) |
| Hand-rolled validator function | Custom `validate_search_request(body: dict)` raising `HardeningError`. | |
| Pydantic with strict=true and per-field validators | Pydantic + per-field validators that raise with explicit codes. | |

**User's choice:** Pydantic + global exception handler (Recommended).
**Codex pushback (adopted):** Don't blanket-rewrite *every* `RequestValidationError` to `invalid_request`. Use `invalid_request` only for malformed-JSON / type-shape failures. **Raise explicit `APIError` exceptions for semantic codes** (`query_required`, `limit_too_high`, `invalid_mode`, `unresolvable_filter_value`) inside validators or post-validation logic so their fine-grained codes survive. Locked as D-05 + D-06.

### Q6: Error code taxonomy granularity

| Option | Description | Selected |
|--------|-------------|----------|
| Fine-grained, stable snake_case | `invalid_mode`, `query_too_long`, `limit_too_high`, `unknown_filter_key`, `unresolvable_filter_value`, `rate_limited`, `disabled`, `localhost_only`, plus warning `query_downgraded`. | ✓ |
| Coarse: `invalid_request` / `rate_limited` / `disabled` / `server_error` | Four buckets, detail in message. | |
| HTTP-status-derived | `bad_request` / `too_many_requests` / `service_unavailable`. | |

**User's choice:** Fine-grained, stable snake_case names (Recommended). Locked as D-07.

### Q7: Query length cap

| Option | Description | Selected |
|--------|-------------|----------|
| 1000 chars | Real Genizah queries are 30-200 chars; longest known UI input ~500. 1000 leaves 2x headroom. | ✓ |
| 500 chars | Tighter; aligns numerically with `MAX_EXPANDED_TERMS` (different units though). | |
| 4096 chars | Generous; matches typical URL/header limits. | |
| Configurable via env | Add a third env var. | |

**User's choice:** 1000 chars (Recommended). Locked as D-08.

### Q8: Result limit overflow behavior (limit > 200)

| Option | Description | Selected |
|--------|-------------|----------|
| Reject 400 with `limit_too_high` | HARDEN-02 says "return 400 with the cap echoed". Honors literal language. | ✓ |
| Clip to 200 + warning | Friendly but contradicts HARDEN-02. | |
| Reject if explicit, default to 50 if missing | Same as recommended for the explicit-too-high case. | |

**User's choice:** Reject 400 with `limit_too_high` (Recommended). Locked as D-09.

---

## PostHog server-side observability

User flagged Cluster C as outside their expertise: "All questions are above my understanding. I'm offering to consult Codex now". User then asked to consult Codex on **all** gray areas. Codex prompt at `_tmp/codex_phase78_consult.md` summarized requirements, codebase realities, the tentative A/B locks, and the open C/D options.

### Q9: Server-side capture approach

| Option | Description | Selected |
|--------|-------------|----------|
| posthog Python SDK | Add `posthog` to requirements.txt; `posthog.capture(...)`. SDK batches/flushes. | |
| Direct `requests.post()` to `/capture` from daemon thread | No new dep; `requests` already present. Fire-and-forget. | ✓ |
| Structured log line, no PostHog from server | Punts HARDEN-05 to ops. | |

**Codex recommendation (adopted):** Direct `requests.post()` to PostHog `/capture` from a daemon thread. `requests` is already in requirements.txt; traffic is low; keeps the hardening layer dependency-light. Locked as D-10.

### Q10: IP-hash scheme

| Option | Description | Selected |
|--------|-------------|----------|
| `HMAC-SHA256(salt, ip)`, truncated to 16 hex | Stable pseudonymous ID; HMAC prevents salt extraction. | ✓ |
| Plain `SHA256(salt + ip)` full 64 hex chars | Slightly simpler. | |
| /24 subnet truncation, no hash | Simpler but stores partial IP. | |

**Codex recommendation (adopted):** `HMAC-SHA256(salt, ip)` truncated to 16 hex chars. Auto-generate `POSTHOG_IP_SALT` if absent, but **production should set it explicitly** so hashes survive restarts. Locked as D-11.

### Q11: Latency + result-count buckets

| Option | Description | Selected |
|--------|-------------|----------|
| Fixed labels: lt_100ms/lt_500ms/lt_2s/lt_10s/gte_10s + zero/count_1_10/count_11_50/count_51_200 | Low-cardinality strings; HARDEN-05 explicitly wants buckets. | ✓ |
| Raw int values | PostHog UI buckets later. | |
| Coarse: lt_1s/gte_1s; empty/some/many | Crash-detection only. | |

**Codex recommendation (adopted):** Fixed bucket labels. Locked as D-12.

### Q12: Sampling rate

| Option | Description | Selected |
|--------|-------------|----------|
| Every request | v7.10 traffic is low. | |
| 1-in-N from env (default 1) | Future-proof. | ✓ |

**Codex recommendation (adopted):** Sampling N from env, default 1 = every request, can dial down without code change. Locked as D-13.

---

## Filter shape + route mounting + tests

### Q13: Filter input shape

| Option | Description | Selected |
|--------|-------------|----------|
| Singular keys, scalar values | `filters: {domain: 'Bible', author: '...', date_from: '1100', material: 'parchment'}`. UI supports multi; loses that. | |
| Plural keys, list values | `filters: {domains: [...], authors: [...], works: [...], date_from, date_to, materials: [...]}`. Matches `get_filter_sys_ids` signature exactly. | |
| Hybrid: lists for categorical, scalars for dates | Lists for domains/authors/works/materials; scalars for date_from/date_to. | ✓ |

**Codex recommendation (adopted):** Hybrid. Matches the real code path: `get_filter_sys_ids()` and the UI treat domain/author/work as multi-select; dates are true scalar bounds. Locked as D-15.

### Q14: Include/exclude toggle

| Option | Description | Selected |
|--------|-------------|----------|
| Don't expose | Inclusion-only in v7.10. | ✓ |
| Parallel `filters_exclude` | UI parity. | |
| Per-filter prefix `!domain: [...]` | Compact but parser-heavy. | |

**Codex recommendation (adopted):** Don't expose. Locked as D-16. Captured in `<deferred>`.

### Q15: Unresolvable filter values

| Option | Description | Selected |
|--------|-------------|----------|
| Strict reject — `400 unresolvable_filter_value` | Single value can't resolve → reject with key+value in message. | ✓ |
| Reject only if intersection is empty | Resolve all, only fail if combined result is empty. | |
| Best-effort: warn and proceed with resolvable subset | Surface unresolved in `warnings[]`. | |

**Codex recommendation (adopted):** Strict reject. The only honest reading of API-07 ("rejected at the endpoint, never silently ignored") that distinguishes "bad filter token" from "valid filters with empty intersection". Locked as D-17.

### Q16: Route mounting

| Option | Description | Selected |
|--------|-------------|----------|
| Extend `init_api_routes()` in `web/api.py` | Single file, established pattern. `web/api.py` already 2200+ lines. | |
| New `web/search_api.py` with `init_search_api()` registrar | Cleaner separation; hardening helpers go alongside. | ✓ |
| Sub-app: `app.mount('/api', sub_app)` | Cleanest isolation but two FastAPI instances. | |

**Codex recommendation (adopted):** New `web/search_api.py` registrar called from `web/main.py` after `init_api_routes()`. Keeps the single mounted FastAPI app and the existing `app_override` test pattern. **`web/api.py` is not modified by Phase 78** — zero blast radius on legacy routes. Locked as D-18.

### Q17: Pydantic vs raw dict

| Option | Description | Selected |
|--------|-------------|----------|
| Pydantic `SearchRequest(BaseModel)` with `extra='forbid'` | FastAPI binds it; validation flows through global handler. | ✓ |
| Raw dict body parsed manually | Hand-rolled. | |

**Codex recommendation (adopted):** Pydantic with `extra='forbid'`. Locked as D-19.

### Q18: Soak test approach

| Option | Description | Selected |
|--------|-------------|----------|
| pytest with `@pytest.mark.slow` | Synchronous TestClient burst loop; deterministic. | |
| Standalone `scripts/soak_search_api.py` | Real production / nginx XFF / real `Retry-After`. | |
| Both | pytest for handler semantics; standalone for end-to-end through nginx. | ✓ |

**Codex recommendation (adopted):** Both. The slow pytest proves handler semantics deterministically; the standalone soak script is the only realistic check through nginx/X-Forwarded-For and the real `Retry-After` behavior. Locked as D-20 + D-22.

---

## Claude's Discretion

The CONTEXT.md `<decisions>` section lists items where the planner has discretion (module split between `search_api.py` and `api_hardening.py`, exact import boundaries, whether models go inline or in their own file, daemon thread lifecycle, salt persistence path, date filter inclusivity, CORS deferral, etc.). These were not asked of the user during discussion because they are pure implementation mechanics.

## Deferred Ideas

- Filter exclusion in API.
- CORS headers.
- API authentication / keys.
- Additional rate-limit dimensions (per-key, per-route, burst).
- PostHog SDK migration (currently fire-and-forget HTTP).
- Multi-worker rate-limit / counter (would need Redis).
- `docs/SEARCH_API.md` — Phase 82's responsibility.

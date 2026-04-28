# Phase 78: /api/search + Hardening Shell - Context

**Gathered:** 2026-04-28
**Status:** Ready for planning

<domain>
## Phase Boundary

Build `POST /api/search` — the first Claude-friendly search-helper HTTP endpoint — over a **cross-cutting hardening shell** (rate limit, mode flag, query/result caps, uniform error envelope, PostHog observability, locator emission). The hardening shell is built **once** so Phase 79 (`/api/browse`) and Phase 80 (`/api/parallels`) inherit it without per-endpoint reimplementation.

**In scope:**
- New module `web/search_api.py` registering the hardened routes against the existing NiceGUI-mounted FastAPI app.
- New helper module `web/api_hardening.py` (rate limiter, mode-gate, error envelope, PostHog capture, IP-hash) consumed by Phase 78/79/80.
- `POST /api/search` handler: Pydantic-validated request → FJMS filter resolution → `SearchEngine.execute_search` → `shared/search_serializer.serialize_search_payload`.
- Two new env vars: `SEARCH_API_MODE` and `SEARCH_API_RATE_LIMIT`. One auto-generated secret: `POSTHOG_IP_SALT`.
- Test surface: pytest TestClient unit tests for handlers, validation, error envelope, locator round-trip-ready output, mode-gate behavior; `@pytest.mark.slow` soak test for rate limiter; standalone `scripts/soak_search_api.py` for nginx-aware soak.

**Out of scope:**
- `GET /api/browse` (Phase 79).
- `POST /api/parallels` (Phase 80).
- Skill consumer (Phase 81).
- Public documentation `docs/SEARCH_API.md` and CLAUDE.md env-var update (Phase 82 — the as-shipped contract is captured after all three endpoints exist).
- Any change to `/api/export/json`, `/api/export/excel`, `/api/export/parallels/json`, `/api/nli_image_*`, `/api/cambridge_image`, `/api/manchester_image`, `/api/jts_image`, `/api/oxford_image*`, `/api/puzzle_*`, `/api/visual_suggestions*`, sitemap routes, robots, `/api/proxy_image` — these stay byte-identical.
- Authentication / authorization layer. The endpoint is internal-helper-only; no API keys in v7.10.
- CORS configuration. Deferred until a browser consumer needs it.

</domain>

<decisions>
## Implementation Decisions

### Transport Gate (Rate Limiter + Mode Flag)

- **D-01:** Rate limiter — new `web/api_hardening.py` exports a `RateLimiter` class using **per-IP `collections.deque[float]` (true sliding 60s window)**. On each request: append `now`, `popleft` while `oldest < now - 60`, count remaining. If `count > limit` → return 429 with `Retry-After: max(1, ceil(60 - (now - oldest_in_window)))` and envelope `{error:{code:'rate_limited', message:'...'}}`. Process-local state. Production assumption: single uvicorn worker (already true; documented in CLAUDE.md). **Do NOT clone** the existing fixed-window `_check_puzzle_rate_limit` (web/api.py:1209) — that one is fixed-window and returns bare 429. The puzzle endpoint stays on its current pattern; new helper is sliding-window with proper `Retry-After`.
- **D-02:** Env vars (two new):
  - `SEARCH_API_MODE` ∈ {`open`, `localhost-only`, `disabled`}, default `open`.
  - `SEARCH_API_RATE_LIMIT` (int, requests-per-minute, default `30`).
  - Both read at module import time AND on every request (cached with a short TTL or re-read each call — planner's call) so production can flip without restart per HARDEN-04.
- **D-03:** `localhost-only` means **loopback only** (127.0.0.1, ::1). Resolution rule:
  - If `request.client.host` is loopback → trust the **first hop in `X-Forwarded-For`** (immediate peer is nginx, the only proxy in front). The XFF first hop must itself be loopback to pass.
  - If `request.client.host` is NOT loopback → ignore XFF, check `request.client.host` directly against the loopback set.
  - Non-local IP under `localhost-only` mode → **403** + envelope `{error:{code:'localhost_only', message:'Endpoint restricted to localhost'}}`.
  - **Do NOT include RFC1918 (10.*, 192.168.*, 172.16-31.*)** — that would be "private-network", not "localhost-only". The semantic is: only the box itself can hit the endpoint.
- **D-04:** `SEARCH_API_MODE=disabled` → **503** + envelope `{error:{code:'disabled', message:'Search API disabled'}}`. Returned for every request regardless of input. Other `/api/*` routes unaffected (they don't go through the new mode-gate).

### Validation + Error Envelope + Caps

- **D-05:** Validation — **Pydantic + global FastAPI exception handler**. Define `SearchRequest(BaseModel)` with `model_config = {'extra': 'forbid'}`. Fields: `query: str`, `mode: Literal['text','Title','Shelfmark','Responsa']`, `gap: int = 0`, `limit: int = 50`, `filters: Optional[FiltersModel] = None`. FastAPI binds it; structural failures route through the global handler (D-06). Pydantic models live in `web/search_api.py` (inline) or `web/search_api_models.py` — planner's call.
- **D-06:** Two error paths, **distinct codes**:
  - **Path 1 — structural failures** (RequestValidationError from Pydantic): malformed JSON, missing required field, type mismatch, unknown key (because `extra='forbid'`). Global `exception_handler(RequestValidationError)` rewrites these to `{error:{code:'invalid_request', message:<first error>, fields:[<dotted paths>]}}` with HTTP 400. **Do not blanket-rewrite** every validation error to `invalid_request`.
  - **Path 2 — semantic failures**: raised explicitly inside validators or handler code as a custom `class APIError(Exception): code: str; message: str; http_status: int = 400`. A separate `exception_handler(APIError)` renders `{error:{code: e.code, message: e.message}}` with the supplied status. Codes from D-07 stay fine-grained and stable (the skill in Phase 81 branches on `code`, not on `message` strings).
- **D-07:** Error code taxonomy (lowercase snake_case, stable, contractual):
  - `invalid_request` — Pydantic structural (malformed body, type mismatch, unknown key, missing required) — HTTP 400
  - `invalid_mode` — mode value present but not in the enum — HTTP 400 (raised when an enum value is technically a valid string but semantically rejected; in practice Pydantic catches most of these as `invalid_request`, this code stays available for future hand-validations)
  - `query_required` — `query` empty after `.strip()` — HTTP 400
  - `query_too_long` — query exceeds char cap (D-08) — HTTP 400, message echoes cap and submitted length
  - `limit_too_high` — `limit > 200` — HTTP 400, message echoes cap and submitted limit
  - `unknown_filter_key` — filter key not in the allowed set (caught by Pydantic `extra='forbid'`, surfaced as `invalid_request` in practice; code reserved for future)
  - `unresolvable_filter_value` — a filter value cannot be resolved by the FJMS pipeline — HTTP 400, message echoes the offending key=value
  - `rate_limited` — HTTP 429, `Retry-After` header set
  - `disabled` — HTTP 503
  - `localhost_only` — HTTP 403
  - `internal_error` — HTTP 500 catch-all (logged; message: 'Internal error'; no stack trace leaked)
  - Warning code (in top-level `warnings: []`, not an error): `query_downgraded` — Responsa combinatorial cascade reduced expansion below `MAX_EXPANDED_TERMS=500`. Phase 78 surfaces this from the existing core path (already returned by `execute_search` internals); planner threads it through the serializer.
- **D-08:** Query length cap = **1000 chars** (after `.strip()`). Reject with `query_too_long` if exceeded. Independent of `MAX_EXPANDED_TERMS=500` in `genizah_core.py:1927` — that's a post-expansion term count, units are different.
- **D-09:** Result limit — default `50`, max `200`. `limit > 200` → 400 `limit_too_high`. `limit <= 0` → 400 `invalid_request`. Default applies when `limit` field is omitted from the request body.

### PostHog Server-Side Observability

- **D-10:** Server-side capture — **direct `requests.post()` to PostHog `/capture` from a daemon thread**. The existing `posthog_capture()` in `web/analytics.py` uses `ui.run_javascript()` and only works for browser-attached NiceGUI sessions; it stays unchanged for browser events. New helper `web/api_hardening.py: capture_api_event(...)` enqueues events to a `queue.Queue`; a single daemon thread drains and POSTs to `https://eu.i.posthog.com/capture` (host matches existing client-side `web/main.py:246`) with `timeout=2s`. POSTHOG endpoint failures are swallowed silently (best-effort observability — never block or crash a request).
- **D-11:** IP-hash — `hmac.new(salt, ip.encode(), 'sha256').hexdigest()[:16]`. Salt from env `POSTHOG_IP_SALT`. If absent at startup, auto-generate `secrets.token_hex(32)` and persist to a small file (mirroring the `PUZZLE_UPLOAD_SECRET` pattern at `web/puzzle_tokens.py`). CLAUDE.md notes that **production should set `POSTHOG_IP_SALT` explicitly** so hashes remain stable across server restarts (a fresh salt each boot would inflate distinct-user counts in PostHog).
- **D-12:** Bucket labels (low-cardinality strings, sent verbatim to PostHog):
  - `latency_bucket`: `lt_100ms`, `lt_500ms`, `lt_2s`, `lt_10s`, `gte_10s`
  - `result_count_bucket`: `zero`, `count_1_10`, `count_11_50`, `count_51_200`
  - Boundaries inclusive on the lower end: e.g. `lt_500ms` covers 100ms ≤ latency < 500ms.
- **D-13:** Sampling — env var `SEARCH_API_POSTHOG_SAMPLE_N` (int, default `1`). Sample rule: capture iff `request_counter % N == 0` where `request_counter` is a process-local `itertools.count()`-style monotonic counter (atomic increment via `threading.Lock`). Default `1` means every request fires. Future-proofs against a Phase 81 skill or external probe flooding the endpoint.
- **D-14:** Event payload:
  - Event name: `search_api_request`.
  - `distinct_id`: the IP-hash from D-11.
  - Properties: `endpoint` (`'search'` for Phase 78; `'browse'` for Phase 79; `'parallels'` for Phase 80), `mode` (the validated mode string), `latency_bucket`, `result_count_bucket`, `status_code` (HTTP status), `error_code` (the envelope's `error.code` if non-2xx, else `null`).
  - **Never** logged: `query`, `filters`, `gap`, response items, snippets, full text. HARDEN-05 explicitly forbids payload contents.

### Filter Input Shape + FJMS Resolution

- **D-15:** Filter shape — **hybrid: lists for categorical, scalars for dates**. Pydantic `FiltersModel(BaseModel)` with `extra='forbid'`:
  ```python
  class FiltersModel(BaseModel):
      model_config = {'extra': 'forbid'}
      domains: Optional[List[str]] = None
      authors: Optional[List[str]] = None
      works: Optional[List[str]] = None
      materials: Optional[List[str]] = None
      date_from: Optional[int] = None  # Hebrew/CE year as integer
      date_to: Optional[int] = None
  ```
  Plural keys match `shared/fjms_service.get_filter_sys_ids(domains=, authors=, works=, ...)` signature exactly — direct kwargs passthrough, no shape transform inside the handler. Aligns with the EXPORT-01/API-01 phrasing `domains: list[str]` (plural; locked at Phase 77 plan time, MED-01).
- **D-16:** No include/exclude toggle in v7.10. All filters are **inclusion-only**. The UI's `filter_include_mode` flag (and the `domains_exclude=`, `authors_exclude=`, `works_exclude=` kwargs of `get_filter_sys_ids`) are not exposed by the API. Skill (Phase 81) doesn't need exclusion to rank, and exposing it would double the validation/error surface for marginal gain. Captured as a deferred idea below.
- **D-17:** Unresolvable filter values — **strict reject**. After Pydantic validation passes, a thin resolver (new helper, e.g. `shared/fjms_service.validate_filter_values(filters: FiltersModel) -> None`) probes each value against FJMS lookup tables (existing `get_all_domains()`, browse_authors, browse_works, materials list). First unresolvable value → `raise APIError('unresolvable_filter_value', f'filter {key}={value!r} not found in FJMS pipeline')` with HTTP 400. Empty intersection (all values resolvable, but their AND yields 0 manuscripts) is **not** an error — returns the normal envelope with `count=0, total=0, results=[]`. This is the only honest reading of API-07 ("rejected at the endpoint, never silently ignored") that distinguishes "bad filter token" from "valid filters with empty result".

### Route Mounting + Module Layout

- **D-18:** New module `web/search_api.py` exporting `init_search_api(app_override=None)`. Called from `web/main.py` **immediately after** `init_api_routes()` at line 166. Owns:
  - The `POST /api/search` route.
  - The FastAPI exception handlers for `RequestValidationError` and `APIError`.
  - Any module-private helpers specific to the search route.
  - Phase 79 adds `GET /api/browse` to this same module; Phase 80 adds `POST /api/parallels`. The hardening helpers themselves live in `web/api_hardening.py`, imported by `search_api.py`.
  - **`web/api.py` is not modified by Phase 78** — keeps the legacy-routes blast radius zero.
- **D-19:** Pydantic models — define inside `web/search_api.py` for v7.10. If Phase 79/80 push the file past ~600 lines, planner can extract to `web/search_api_models.py` then. Don't pre-split.
- **D-20:** Stateless contract enforcement — the handler **must not** import or reference `web.state.state` for any per-request data. It MAY use `state.searcher` (the singleton `SearchEngine` instance) since that's process-wide search infrastructure, not session state. No `app.storage.user`, no `state.last_results`, no `request.cookies`. Identical request bodies → byte-identical response bodies (modulo `generated_at` timestamp). API-06 verification = a unit test that runs two identical requests through the TestClient and diffs the bodies after stripping `generated_at`.

### Test Surface

- **D-21:** Pytest unit tests in `tests/test_search_api.py` covering:
  - Happy path: text/Title/Shelfmark/Responsa modes each return the Phase 77 serializer envelope shape.
  - Locator: every result item has `uid` (string, may be empty) AND `locator: {sys_id, volume_ie, p_num}` (some fields may be null) per D-04 of Phase 77.
  - Validation: missing `query` → `query_required`; `query` > 1000 chars → `query_too_long`; unknown mode → `invalid_request`; unknown filter key → `invalid_request`; `limit=300` → `limit_too_high`; `limit=0` → `invalid_request`.
  - Error envelope: every non-2xx response is `{error:{code, message, ...}}`, never a raw 422 dump.
  - Filter resolution: known-good domain/author/work resolves; bogus value raises `unresolvable_filter_value`.
  - Mode gate: `SEARCH_API_MODE=disabled` → 503 `disabled`; `localhost-only` with non-loopback IP → 403 `localhost_only`; `localhost-only` with loopback → 200.
  - Stateless: two identical requests produce identical bodies (modulo timestamp).
  - Warnings: a Responsa query that triggers cascade downgrade surfaces `query_downgraded` in `warnings[]`, never inside the first result item.
- **D-22:** Soak test — **both forms**:
  - `tests/test_search_api_soak.py::test_rate_limit_soak` with `@pytest.mark.slow`. With `SEARCH_API_RATE_LIMIT=30` set, fires e.g. 50 requests in 1s via TestClient against the in-process app. Asserts: 429 observed, `Retry-After` header present and integer ≥ 1, envelope code = `rate_limited`. Filtered out of the default `pytest` run via pytest config (`addopts = -m 'not slow'`); CI runs the slow marker explicitly.
  - `scripts/soak_search_api.py` — standalone, args `--url`, `--rate`, `--duration`. Hits the live deployment so the XFF-aware loopback resolution and the actual nginx-mediated `Retry-After` are exercised end-to-end. Run manually as part of phase-gate verification; not in CI.
- **D-23:** Legacy-route immutability spot check — `tests/test_api_legacy_unchanged.py`. Two or three representative existing `/api/*` routes (e.g. `/api/export/json` for non-empty `state.last_results` shape, `/api/puzzle_image` for headers/status) — no behavioral change expected from Phase 78. Light smoke to catch accidental cross-route impact.

### Locator + Serializer (inherited from Phase 77, for reference)

- **D-24:** The handler calls `serialize_search_payload(...)` from `shared/search_serializer.py` (Phase 77, D-14). All response shape obligations (locator on every item, flat envelope, `warnings: []` always present, `source: 'search'`, `schema_version: 1`, `count` + `total`, etc.) are owned by that function. Phase 78 contributes only the populated `warnings` array (cascade downgrades, query adjustments) and the `source='search'` argument.

### Claude's Discretion

- **Module split between `web/search_api.py` and `web/api_hardening.py`** — exact import boundary. Recommendation: `api_hardening.py` is generic infra (RateLimiter class, mode-gate helper, IP-hash, capture_api_event, APIError exception, exception handlers); `search_api.py` is route-specific (Pydantic models, `init_search_api`, the search handler). Planner adjusts if it finds a cleaner cut.
- **Whether `state.searcher` is used directly or wrapped** — singleton already thread-safe; direct use is fine.
- **Daemon thread lifecycle for PostHog queue** — start at `init_search_api()` time; queue is module-level. Process exits on its own; daemon threads die with the process. No explicit shutdown handler required.
- **Salt persistence path** — pick something that mirrors `PUZZLE_UPLOAD_SECRET`. Likely `web/_secrets/posthog_ip_salt` or `~/.genizah_secrets/posthog_ip_salt`. Planner picks based on existing precedent.
- **Whether `gap` and `mode='Responsa'` interact specially** — read existing `SearchEngine.execute_search` signature and pass through; don't reinvent core logic.
- **Date filter semantics** — `date_from` / `date_to` are integer years. Inclusive vs exclusive matches whatever `get_filter_sys_ids` already does — don't redefine.
- **CORS headers** — none for v7.10. Internal helper, no browser consumer. Document as a deferred idea (skill calls server-to-server).

### Folded Todos

None — no pending todos matched Phase 78 scope.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase / milestone specs
- `.planning/ROADMAP.md` §`Phase 78: /api/search + Hardening Shell` — success criteria, including the "Phase 78 hardening shell inherited by 79 and 80" obligation.
- `.planning/ROADMAP.md` §`Phase 79: /api/browse Drill-Down` and §`Phase 80: /api/parallels` — confirm the hardening shell built here must support `GET /api/browse` and `POST /api/parallels` without per-endpoint reimplementation.
- `.planning/REQUIREMENTS.md` §`API Endpoints` (API-01, API-04, API-05, API-06, API-07) — request shape, error envelope, locator, statelessness, FJMS pipeline reuse.
- `.planning/REQUIREMENTS.md` §`Hardening` (HARDEN-01..05) — rate limit, result cap, query cap + cascade, mode flag, PostHog observability.
- `.planning/PROJECT.md` §Architecture — `shared/` service-layer convention; FastAPI mount via NiceGUI.
- `.planning/STATE.md` — v7.10 milestone position, watch list (`/api/*` legacy routes must remain unchanged).

### Phase 77 lock (serializer + envelope shape)
- `.planning/phases/77-serializer-json-export/77-CONTEXT.md` — D-01..D-14 lock the response shape Phase 78 emits. **Especially D-04** (locator both-fields-always-populated) and **D-14** (`serialize_search_payload` / `serialize_parallels_payload` are the SOLE producers of result item shape).
- `shared/search_serializer.py` (Phase 77 output) — actual serializer functions; Phase 78 calls these and never replicates their logic.

### Existing code (single source of truth)
- `web/api.py:174` `init_api_routes(app_override=None)` — the registrar pattern Phase 78 mirrors with its own `init_search_api`. **Phase 78 does NOT modify `web/api.py`.**
- `web/api.py:1207-1225` — existing `_check_puzzle_rate_limit` (fixed-window). Reference only; Phase 78's `RateLimiter` is sliding-window with `Retry-After` (D-01). Puzzle endpoint stays on its current pattern.
- `web/main.py:154-166` — where `init_api_routes()` is called. Phase 78 adds `init_search_api()` immediately after.
- `web/main.py:238-262` — existing `POSTHOG_SCRIPT` and `POSTHOG_API_KEY` env var. Phase 78 reuses `POSTHOG_API_KEY` (no new key); fires server-side via `requests.post()` (D-10).
- `web/analytics.py` — existing client-side `posthog_capture()` using `ui.run_javascript`. **Stays as-is.** Phase 78 adds a separate server-side path.
- `web/puzzle_tokens.py` — `PUZZLE_UPLOAD_SECRET` auto-generation pattern that `POSTHOG_IP_SALT` follows (D-11).
- `web/state.py: state.searcher` — singleton `SearchEngine`, thread-safe, no per-session state. Phase 78 handler calls `state.searcher.execute_search(...)`.
- `genizah_core.py:7211` `SearchEngine.execute_search(query_str, mode, gap, progress_callback=None, exclude_words=None, responsa_options=None, restrict_sys_ids: set = None, text_position: str = None)` — exact signature the handler hits.
- `genizah_core.py:1927` `Config.MAX_EXPANDED_TERMS = 500` — Responsa combinatorial cap; `query_downgraded` warning surfaces when this triggers.
- `genizah_core.py:5935-5965` — `MAX_EXPANDED_TERMS` cascade logic; the planner traces what surface change is needed (if any) so the warning is reachable from the API handler.
- `shared/fjms_service.py: get_filter_sys_ids(...)` — kwargs match Phase 78 D-15 filter shape exactly. Phase 78's `validate_filter_values` is a new sibling that probes each value against `get_all_domains()`, browse_authors, browse_works, etc.

### Cross-phase obligations
- Phase 79 (`/api/browse`) consumes the locator emitted by Phase 78 verbatim. **D-04 of Phase 77** (locator both-fields-always-populated) is the contract Phase 79 inherits.
- Phase 80 (`/api/parallels`) reuses the same `web/search_api.py` and `web/api_hardening.py` modules built here (D-18). The hardening helpers must be generic enough to mount on a parallels handler without per-endpoint reimplementation.
- Phase 81 (skill consumer) branches on the error `code` strings from D-07 — those are contractual, not message text.
- Phase 82 (internal docs) captures the as-shipped contract in `docs/SEARCH_API.md` after all three endpoints exist. Phase 78 does NOT write `docs/SEARCH_API.md`.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/search_serializer.serialize_search_payload(...)` (Phase 77) — owns the entire response body shape including locator, envelope, warnings.
- `state.searcher.execute_search(...)` — singleton entry point; thread-safe; takes a `restrict_sys_ids: set` kwarg the handler populates from FJMS.
- `shared/fjms_service.get_filter_sys_ids(...)` — kwargs map 1:1 to D-15 filter shape; no transform needed.
- `web/main.py: POSTHOG_API_KEY` (env), `web/main.py:246: api_host: 'https://eu.i.posthog.com'` — reuse same key + host for server-side `requests.post()` to `/capture`.
- `web/puzzle_tokens.py: PUZZLE_UPLOAD_SECRET` — auto-generate-and-persist pattern that `POSTHOG_IP_SALT` mirrors.
- `web/api.py:174 init_api_routes(app_override=None)` — registrar pattern that `init_search_api(app_override=None)` mirrors, supporting the same TestClient pattern.

### Established Patterns
- NiceGUI mounts the FastAPI app; `app` from `from nicegui import app` is the FastAPI instance. `init_X_routes(app_override=None)` registers routes on `app` (or a TestClient-supplied override).
- Single uvicorn worker in production (deployment fact, not a constraint Phase 78 needs to break). Process-local rate-limit state is therefore correct.
- Server-side PostHog is currently NOT done (existing `posthog_capture` is JS injection). Phase 78 introduces it.
- Custom env-var-based feature flags (`WEB_PUZZLE_ENABLED` in `web/feature_flags.py`) — `SEARCH_API_MODE` follows the same pattern but reads on every request.

### Integration Points
- `web/main.py:166` — `init_api_routes()` call. Phase 78 adds `init_search_api()` immediately after on a new line.
- `web/state.py: state.searcher` — read-only access to the singleton SearchEngine.
- `requirements.txt` — no new direct deps required (`requests` already present, `pydantic` already a transitive of FastAPI). Verify on planner pass.
- `CLAUDE.md` Environment Variables section — Phase 78 adds the three new env vars (`SEARCH_API_MODE`, `SEARCH_API_RATE_LIMIT`, `POSTHOG_IP_SALT`) and the optional `SEARCH_API_POSTHOG_SAMPLE_N`. **Note:** the larger `docs/SEARCH_API.md` doc is Phase 82's responsibility, but env-var lines in CLAUDE.md are owned by whichever phase introduces them — planner decides whether Phase 78 or Phase 82 writes them. Roadmap puts DOC-02 in Phase 82, so default = Phase 78 writes them as part of phase summary, Phase 82 makes them canonical.

### Test Surface
- `pytest` with `from fastapi.testclient import TestClient`. Construct a fresh `FastAPI()` instance, call `init_search_api(app_override=fresh_app)`, hit it with TestClient. No real server, no real PostHog (mock the daemon thread or stub the queue).
- Soak test runs synchronously with a fast fake clock or a real loop; can use `time.monotonic()`-based rate limiter without actually waiting (the deque comparison is enough).
- Live soak script (`scripts/soak_search_api.py`) — not pytest, hits real production. Documented in phase summary.

</code_context>

<specifics>
## Specific Ideas

- **The user explicitly delegated the infra/security calls** in this phase ("most decisions are not my expertise") and ran the open questions through Codex (external CLI). The recommendations Codex returned (sliding-window rate limiter, loopback-only meaning of `localhost-only`, fine-grained semantic error codes raised explicitly via `APIError` not via blanket Pydantic rewrite, server-side PostHog via `requests.post()` daemon thread, hybrid filter shape, strict reject for unresolvable filters, new `web/search_api.py` module rather than growing `web/api.py`, both pytest-soak and standalone-soak) ARE the locked decisions in `<decisions>` above. Planner should treat these as load-bearing — they are not Claude defaults.
- **`Retry-After` is non-negotiable.** The phase gate explicitly requires "soak check sustaining traffic above the per-IP rate limit until 429 + `Retry-After` are observed". The fixed-window puzzle limiter cannot produce a meaningful `Retry-After` value (it returns the same number until the window flips). The sliding-window deque approach is the smallest design that produces an honest `Retry-After`. Do not regress this to a fixed-window pattern even if it's "shorter".
- **`localhost-only` is loopback-only**, not "private network". A previous draft of A3 included RFC1918 ranges (10.*, 192.168.*, 172.16-31.*); Codex pushed back, and rightly: "localhost-only" semantically means the box itself, not anything on the LAN. RFC1918 inclusion would make the mode misleading. If you ever need "private-network-only", it should be a different mode value.
- **Error code stability matters more than message wording.** The skill (Phase 81) branches on `code` strings; messages are for humans reading logs. Renaming a code is a breaking change for the skill; rewriting a message is not. Plan accordingly.
- **PostHog server-side is genuinely new**, not a port of `posthog_capture`. The client-side function uses `ui.run_javascript` which has no client when no NiceGUI session exists — all API requests have no NiceGUI session. The two paths coexist; do not try to unify them.
- **Phase 78 does NOT touch `web/api.py`.** All new code lives in `web/search_api.py` and `web/api_hardening.py`. The roadmap success criterion that legacy routes are byte-identical is structurally enforced by this split.

</specifics>

<deferred>
## Deferred Ideas

- **Filter exclusion (`!domain:`, `domains_exclude:`, etc.)** — UI has it, API skips it for v7.10. Add when a real consumer requests it; would extend `FiltersModel` with parallel `*_exclude` keys and pass through to `get_filter_sys_ids(domains_exclude=...)`.
- **CORS headers** — no browser consumer in v7.10 (skill calls server-to-server). Add later if a JS consumer appears.
- **Authentication / API keys** — internal helper, no public stability promise. Would compose cleanly on top of the mode gate.
- **Additional rate-limit dimensions** (per-API-key, per-route, burst tolerance) — defer until a use case appears.
- **PostHog event dedupe / batching tuning** — the daemon thread + queue + `requests.post()` is fire-and-forget; if event volume ever justifies the SDK or proper batching, swap then.
- **Multi-worker deployment** — the in-memory rate limit and PostHog counter are process-local. If production ever runs multiple uvicorn workers, swap to Redis-backed state. Out of scope for v7.10.
- **`docs/SEARCH_API.md`** — Phase 82's responsibility; deliberately not written here.

</deferred>

---

*Phase: 78-api-search-hardening-shell*
*Context gathered: 2026-04-28*
*Open questions externally reviewed via Codex CLI 2026-04-28; recommendations adopted in full.*

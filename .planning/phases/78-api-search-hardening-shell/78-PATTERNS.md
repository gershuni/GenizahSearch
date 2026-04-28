# Phase 78: /api/search + Hardening Shell - Pattern Map

**Mapped:** 2026-04-28
**Files analyzed:** 5 new + 2 minimal-edit = 7
**Analogs found:** 7/7 (every new file has a strong existing analog)

Authoritative file list extracted from CONTEXT.md `<integration_points>` and `<decisions>` D-18..D-22. Cross-checked against ROADMAP Phase 78 success criteria and REQUIREMENTS API-01/04/05/06/07 + HARDEN-01..05.

## File Classification

| New / Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---------------------|------|-----------|----------------|---------------|
| `web/api_hardening.py` (new) | infra utility | request-response middleware | `web/api.py:1206-1225` (rate limiter) + `web/puzzle_tokens.py` (HMAC) + `web/analytics.py` (PostHog) | role-match (no single existing module covers all four; assemble from three) |
| `web/search_api.py` (new) | route registrar / controller | request-response | `web/api.py:174 init_api_routes(app_override=None)` | exact (registrar pattern) |
| `tests/test_search_api.py` (new) | test (handler unit) | request-response | `tests/test_api_export_json.py` | exact (TestClient + `app_override=bare_app` fixture) |
| `tests/test_search_api_soak.py` (new) | test (rate-limit soak, `@pytest.mark.slow`) | burst | `tests/test_api_export_json.py` (TestClient harness only) | partial — no existing soak test in repo |
| `scripts/soak_search_api.py` (new) | standalone CLI | burst against live nginx | no analog (new pattern; argparse + `requests` loop) | none — see "No Analog Found" |
| `web/main.py` (modify, 1 line) | bootstrap | startup wire-up | `web/main.py:166 init_api_routes()` | exact (one-line addition immediately after) |
| `CLAUDE.md` (modify, ~4 lines) | docs | static | `CLAUDE.md:137-145 Environment Variables` block | exact (append new env vars to existing block) |

## Pattern Assignments

### `web/api_hardening.py` (infra utility, request-response middleware)

This is a NEW module assembling four sub-patterns from three existing files. Per D-01 it must NOT clone the existing fixed-window puzzle limiter — the existing one is the **counter-example**. Per D-10/D-11 the PostHog server-side path is genuinely new (the existing `posthog_capture` is JS-only and stays untouched).

**Sub-pattern A — Rate limiter (REFERENCE only; do NOT clone). Source: `web/api.py:1206-1225`.**

```python
# In-memory rate limiter for puzzle upload endpoints
_puzzle_rate_limits = {}  # IP -> (count, window_start_epoch)

def _check_puzzle_rate_limit(request: Request, max_per_min: int = 60):
    """Check per-IP rate limit. Returns Response if exceeded, else None."""
    import time as _time
    client_ip = request.client.host if request.client else 'unknown'
    now = _time.time()
    entry = _puzzle_rate_limits.get(client_ip)
    if entry:
        count, window_start = entry
        if now - window_start < 60:
            if count >= max_per_min:
                return Response(content="Rate limit exceeded", status_code=429)
            _puzzle_rate_limits[client_ip] = (count + 1, window_start)
        else:
            _puzzle_rate_limits[client_ip] = (1, now)
    else:
        _puzzle_rate_limits[client_ip] = (1, now)
    return None
```

**Reuse:** the per-IP dict + module-level state convention; `request.client.host if request.client else 'unknown'` IP extraction; bare-`Response(status_code=429)` mechanic.
**Deviate (D-01):** swap `(count, window_start)` tuple for `collections.deque[float]`; on each call append `now`, `popleft` while `oldest < now - 60`, count remaining; if `count > limit` compute `Retry-After = max(1, ceil(60 - (now - oldest_in_window)))`; return JSON envelope `{error:{code:'rate_limited',message:...}}` with `Retry-After` header — NOT the puzzle endpoint's bare 429. The puzzle endpoint stays as-is (D-01 explicit, Phase 78 does not modify `web/api.py`).

**Sub-pattern B — Auto-generate-and-persist secret. Source: `web/puzzle_tokens.py:18`.**

```python
# Secret key for HMAC signing. In production, set PUZZLE_UPLOAD_SECRET env var.
# Falls back to a random key per process (tokens won't survive restarts).
PUZZLE_SECRET = os.environ.get('PUZZLE_UPLOAD_SECRET', os.urandom(32).hex())
```

```python
# Verify pattern (HMAC compare_digest, never bare ==)
sig = hmac.new(PUZZLE_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
if not hmac.compare_digest(sig, expected):
    return False
```

**Reuse:** `os.environ.get('POSTHOG_IP_SALT', os.urandom(32).hex())` — same shape, different env name. `hmac.new(salt.encode(), ip.encode(), hashlib.sha256).hexdigest()[:16]` per D-11.
**Deviate (D-11 / Claude's Discretion):** the Phase 78 salt should ideally **persist to a file** so hashes survive restarts (puzzle one doesn't because token expiry is 5 min anyway; for IP-hash, restart-instability inflates PostHog distinct-user counts). Planner picks path — recommend `web/_secrets/posthog_ip_salt` mirroring the puzzle file convention. CLAUDE.md note: "production should set `POSTHOG_IP_SALT` explicitly" (text from D-11).

**Sub-pattern C — Existing PostHog client-side helper (REFERENCE only; stays unchanged). Source: `web/analytics.py:11-24`.**

```python
def posthog_capture(event: str, properties: dict = None):
    """Send a custom PostHog event from the server side via JS injection.
    Safe to call even if PostHog isn't loaded (no-ops gracefully).
    """
    import json
    props_js = json.dumps(properties or {})
    try:
        ui.run_javascript(
            f"if(window.posthog)posthog.capture('{event}',{props_js})"
        )
    except Exception:
        pass  # No client connection or PostHog not loaded
```

**Reuse:** event-name + properties-dict shape; silent-fail philosophy (`except Exception: pass`).
**Deviate (D-10):** the API path has NO NiceGUI session, so `ui.run_javascript` is a no-op. New `capture_api_event(...)` enqueues to a module-level `queue.Queue`; a single daemon thread (`threading.Thread(target=_drain, daemon=True).start()` at `init_search_api()` time per Claude's-Discretion bullet) pops events and `requests.post('https://eu.i.posthog.com/capture', json={...}, timeout=2)`. Failures swallowed silently.

**Sub-pattern D — PostHog endpoint URL + API key reuse. Source: `web/main.py:238-262`.**

```python
_posthog_key = os.environ.get('POSTHOG_API_KEY', '')
# inside POSTHOG_SCRIPT JS:
posthog.init('{_posthog_key}', {{
    api_host: 'https://eu.i.posthog.com',
    ...
}});
```

**Reuse verbatim:** `POSTHOG_API_KEY` env var name (no new key per D-10), `https://eu.i.posthog.com` host. POST to `https://eu.i.posthog.com/capture`; body shape `{api_key: <key>, event: 'search_api_request', distinct_id: <ip_hash>, properties: {...}, timestamp: <iso>}`.

**Sub-pattern E — `APIError` exception class + FastAPI exception handlers.** No direct existing analog (this is new). Define inside `web/api_hardening.py`:

```python
class APIError(Exception):
    def __init__(self, code: str, message: str, http_status: int = 400):
        self.code = code
        self.message = message
        self.http_status = http_status

# Exception handlers registered against target_app inside init_search_api():
async def _api_error_handler(request, exc: APIError):
    return JSONResponse(status_code=exc.http_status,
                        content={'error': {'code': exc.code, 'message': exc.message}})
async def _validation_error_handler(request, exc: RequestValidationError):
    first = (exc.errors() or [{}])[0]
    fields = ['.'.join(str(p) for p in (e.get('loc') or [])) for e in exc.errors()]
    return JSONResponse(status_code=400,
                        content={'error': {'code': 'invalid_request',
                                            'message': first.get('msg', 'invalid request'),
                                            'fields': fields}})
```

**Why no analog:** existing `/api/*` handlers in `web/api.py` use bare `Response(content=..., status_code=...)` (e.g. line 1219 `return Response(content="Rate limit exceeded", status_code=429)`). Phase 78 introduces structured envelopes via D-06 — explicitly distinct from legacy.

---

### `web/search_api.py` (route registrar / controller, request-response)

**Analog:** `web/api.py:174-188` — the `init_api_routes(app_override=None)` registrar pattern.

**Imports + module-header pattern** (lines 1-18 of `web/api.py`):

```python
import logging
import json
from nicegui import app
from fastapi import Response
from starlette.requests import Request
from web.state import state
import requests, os, threading
from genizah_core import Config

logger = logging.getLogger(__name__)
```

**Reuse:** identical import block, swap `Response` for `JSONResponse` from `fastapi.responses`; add `from pydantic import BaseModel, Field`; `from typing import Literal, Optional, List`; `from web.api_hardening import RateLimiter, APIError, capture_api_event, ...`.

**Registrar shape** (`web/api.py:174-188`):

```python
def init_api_routes(app_override=None):
    """Register API routes for image proxy and exports.
    Args:
        app_override: Optional FastAPI/Starlette app to register routes onto.
                      When None (default, production), registers onto the
                      module-level NiceGUI singleton ``app``. When a bare app
                      is passed (test fixtures), registers onto that instead.
                      See 77-REVIEWS.md HIGH-08.
    """
    target_app = app_override if app_override is not None else app
    logger.info("API routes initialized (Supabase mode)")

    @target_app.get('/robots.txt')
    def robots_txt():
        ...
```

**Reuse verbatim (D-18):** function name `init_search_api(app_override=None)`, identical `target_app = app_override if app_override is not None else app` line, decorator-on-`target_app` pattern. **Critical:** every route definition uses `@target_app.post('/api/search')` (NOT `@app.post(...)`) so `app_override` actually works — see `tests/test_api_export_json.py:192-210` for the regression that catches this.

**Route handler skeleton** (synthesized from D-05 + D-15 + D-17 + D-20 + D-24):

```python
class FiltersModel(BaseModel):
    model_config = {'extra': 'forbid'}
    domains: Optional[List[str]] = None
    authors: Optional[List[str]] = None
    works: Optional[List[str]] = None
    materials: Optional[List[str]] = None
    date_from: Optional[int] = None
    date_to: Optional[int] = None

class SearchRequest(BaseModel):
    model_config = {'extra': 'forbid'}
    query: str
    mode: Literal['text', 'Title', 'Shelfmark', 'Responsa']
    gap: int = 0
    limit: int = 50
    filters: Optional[FiltersModel] = None

@target_app.post('/api/search')
async def search_endpoint(req: SearchRequest, request: Request):
    # 1. Mode gate (re-read env per request per D-02; check loopback per D-03)
    # 2. Rate limit check (sliding-window deque per D-01)
    # 3. Query post-validation: strip, length cap 1000 (D-08), limit cap 200 (D-09)
    # 4. Filter resolution: validate_filter_values(req.filters) raises APIError
    #    on unresolvable; restrict_sys_ids = fjms.get_filter_sys_ids(**req.filters.model_dump())
    # 5. Call state.searcher.execute_search(query, mode, gap, restrict_sys_ids=...)
    #    -- D-20: NEVER read state.last_results, app.storage.user, request.cookies
    # 6. Capture warnings (cascade downgrade -> 'query_downgraded')
    # 7. Return serialize_search_payload(results, meta_mgr=state.meta_mgr,
    #                                    query=..., mode=..., warnings=[...], total=..., source='search')
    # 8. capture_api_event(endpoint='search', mode=..., latency_bucket=..., result_count_bucket=..., status_code=200, error_code=None)
```

**Stateless contract enforcement (D-20):** the handler MAY use `state.searcher` and `state.meta_mgr` (process-wide singletons). It MUST NOT touch `state.last_results`, `state.current_search_query`, etc. — those are session echoes for the legacy export route, not API state.

**Core search call signature** — `genizah_core.py:7211`:

```python
def execute_search(self, query_str, mode, gap, progress_callback=None,
                   exclude_words=None, responsa_options=None,
                   restrict_sys_ids: set = None, text_position: str = None):
```

**Reuse:** pass `query_str=req.query.strip()`, `mode=req.mode`, `gap=req.gap`, `restrict_sys_ids=<set from FJMS>`, `progress_callback=None`. Mode `'Responsa'` requires `responsa_options={'responsa_mode': True, ...}` — planner traces existing call site in `web/pages/search.py` to see exact options shape.

**Serializer call** — `shared/search_serializer.py:313`:

```python
def serialize_search_payload(
    results: list[dict], *, meta_mgr, query='', mode='text',
    gap=None, filters=None, warnings=None, total=None,
) -> dict:
    # Returns: {schema_version, source: 'search', query, mode, gap, filters,
    #          count, total, warnings, generated_at, results: [...]}
```

**Reuse verbatim (D-24):** call exactly this; do not reshape items. Phase 78 supplies `warnings=[...]` (cascade downgrades from `query_downgraded`) and trusts the rest.

---

### `tests/test_search_api.py` (test, handler unit)

**Analog:** `tests/test_api_export_json.py` (Phase 77). Same TestClient + `app_override=bare_app` fixture pattern.

**Bare-app fixture pattern** (lines 20-37):

```python
@pytest.fixture(scope='module')
def bare_app_with_routes():
    """Build a bare FastAPI app and register Phase 77 routes onto it.
    HIGH-08: this fixture does NOT touch nicegui.app.
    """
    from web.api import init_api_routes
    bare = FastAPI()
    init_api_routes(app_override=bare)
    return bare

@pytest.fixture
def client(bare_app_with_routes):
    return TestClient(bare_app_with_routes)
```

**Reuse verbatim:** swap `init_api_routes` for `init_search_api`. Module-scoped fixture so the daemon-thread PostHog drainer starts once. Add a fixture that mocks `state.searcher` and `state.meta_mgr` (the handler reads them; tests should not require a real Tantivy index).

**State-mock fixture pattern** (lines 41-91):

```python
@pytest.fixture
def mock_meta_mgr():
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ("T-S 12.345", "Test Title")
    mgr.parse_full_id_components.return_value = {
        'sys_id': '9912345678901234', 'ie_id': 'IE99', 'p_num': '7', 'fl_id': None,
    }
    return mgr

@pytest.fixture
def populated_search_state(mock_meta_mgr):
    from web.api import state
    saved = {'meta_mgr': state.meta_mgr, ...}
    state.meta_mgr = mock_meta_mgr
    state.last_results = [...]
    yield state
    for k, v in saved.items():
        setattr(state, k, v)
```

**Reuse:** save-set-yield-restore pattern for global `state`. **Deviate (D-20):** Phase 78 handler does NOT read `state.last_results`, so don't bother seeding it. Mock `state.searcher` instead — `MagicMock(spec=SearchEngine)` with `.execute_search.return_value = [<one synthetic result dict>]`.

**Singleton-immutability spot check** (lines 192-210) — copy verbatim, swap function name:

```python
def test_init_search_api_does_not_mutate_nicegui_singleton():
    from web.search_api import init_search_api
    from nicegui import app as nicegui_app
    before = len(nicegui_app.routes) if hasattr(nicegui_app, 'routes') else 0
    bare = FastAPI()
    init_search_api(app_override=bare)
    after = len(nicegui_app.routes) if hasattr(nicegui_app, 'routes') else 0
    assert after == before, f"NiceGUI singleton was mutated: routes {before} -> {after}."
    assert len(bare.routes) > 0, "Bare app got no routes -- app_override dispatch broken."
```

**Test cases per D-21** (8 groups): happy-path-per-mode, locator round-trip-ready, validation (8 sub-cases), error-envelope-shape, filter-resolution, mode-gate (3 sub-cases), stateless-byte-equality (modulo `generated_at`), warnings-not-in-first-result.

---

### `tests/test_search_api_soak.py` (test, rate-limit soak, `@pytest.mark.slow`)

**Analog:** the bare-app fixture from `tests/test_api_export_json.py` (above) is the only reusable harness; the burst-loop logic is new.

**Pattern (synthesized from D-22):**

```python
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

@pytest.mark.slow
def test_rate_limit_soak(monkeypatch):
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    from web.search_api import init_search_api
    bare = FastAPI()
    init_search_api(app_override=bare)
    with TestClient(bare) as client:
        # mock state.searcher to return [] quickly
        responses = [client.post('/api/search', json={'query': 'a', 'mode': 'text'})
                     for _ in range(50)]
        rate_limited = [r for r in responses if r.status_code == 429]
        assert len(rate_limited) > 0, "expected at least one 429"
        for r in rate_limited:
            assert 'Retry-After' in r.headers
            assert int(r.headers['Retry-After']) >= 1
            assert r.json()['error']['code'] == 'rate_limited'
```

**Pytest config note:** `@pytest.mark.slow` filtered out of default runs. Add `addopts = -m 'not slow'` to `pytest.ini` / `pyproject.toml` if not already configured (planner verifies — the project may already have this).

---

### `scripts/soak_search_api.py` (standalone CLI, burst against live nginx)

**Analog:** none in repo — this is genuinely new. Use stdlib `argparse` + `requests` only (already a dep).

**Pattern (synthesized from D-22):**

```python
#!/usr/bin/env python3
"""Live soak test: hit /api/search at a sustained rate, observe 429 + Retry-After.
Run manually as part of phase-gate verification; not in CI.
"""
import argparse, time, sys
import requests

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--url', default='https://genizahsearch.com/api/search')
    p.add_argument('--rate', type=int, default=60, help='requests per minute target')
    p.add_argument('--duration', type=int, default=120, help='seconds to run')
    args = p.parse_args()

    body = {'query': 'soak', 'mode': 'text', 'limit': 1}
    interval = 60.0 / args.rate
    deadline = time.monotonic() + args.duration
    counts = {'2xx': 0, '429': 0, 'other': 0}
    first_429_retry_after = None
    while time.monotonic() < deadline:
        t0 = time.monotonic()
        r = requests.post(args.url, json=body, timeout=10)
        if 200 <= r.status_code < 300:
            counts['2xx'] += 1
        elif r.status_code == 429:
            counts['429'] += 1
            if first_429_retry_after is None:
                first_429_retry_after = r.headers.get('Retry-After')
        else:
            counts['other'] += 1
        elapsed = time.monotonic() - t0
        if elapsed < interval:
            time.sleep(interval - elapsed)
    print(f"counts: {counts}")
    print(f"first 429 Retry-After: {first_429_retry_after}")
    sys.exit(0 if counts['429'] > 0 else 1)

if __name__ == '__main__':
    main()
```

**No analog reason:** no live-deployment soak script exists in the repo. Listed in "No Analog Found" below.

---

### `web/main.py` (modify, 1 line)

**Analog:** `web/main.py:154-166` — exactly where the existing `init_api_routes()` is called.

**Existing code** (lines 154, 165-166):

```python
from web.api import init_api_routes
...
# Initialize API routes (Image Proxy, Export)
init_api_routes()
```

**Phase 78 edit** (per D-18 / `<integration_points>` integration point):

```python
from web.api import init_api_routes
from web.search_api import init_search_api  # new import

# Initialize API routes (Image Proxy, Export)
init_api_routes()
init_search_api()  # new line, immediately after
```

**Reuse:** match the exact one-line `init_X()` call shape so cross-module bootstrap reads consistently.

---

### `CLAUDE.md` (modify, ~4 lines added to existing block)

**Analog:** `CLAUDE.md:137-145` — existing Environment Variables block.

**Existing block** (lines 137-145, rendered as an indented quote to avoid a nested fence):

> ```
> SUPABASE_URL=https://xxxxx.supabase.co
> SUPABASE_ANON_KEY=eyJ...
> POSTHOG_API_KEY=phc_xxxxx (optional - enables PostHog analytics)
> WEB_PUZZLE_ENABLED=true (default: true; set to false to disable web puzzle page)
> PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
> ```

**Phase 78 addition** (mirror the existing one-line-with-parenthesized-default convention):

```
SEARCH_API_MODE=open (one of: open | localhost-only | disabled; default: open; flippable per request without restart)
SEARCH_API_RATE_LIMIT=30 (per-IP requests per minute; default: 30)
POSTHOG_IP_SALT=xxx (optional - HMAC salt for hashing client IPs in server-side PostHog events; auto-generated if unset, but production should set explicitly so hashes survive restarts)
SEARCH_API_POSTHOG_SAMPLE_N=1 (optional - capture every Nth API request to PostHog; default: 1 = every request)
```

**Note (from `<integration_points>`):** DOC-02 ownership belongs to Phase 82 for the canonical write. Phase 78 introduces the env vars and adds them to the block as part of phase summary (the smaller `docs/SEARCH_API.md` is Phase 82's responsibility).

---

## Shared Patterns

### Rate Limiter / Mode Gate

**Source:** new `web/api_hardening.py` (this phase).
**Apply to:** every route registered by `init_search_api` in Phase 78, and inherited by Phase 79 `/api/browse` and Phase 80 `/api/parallels`.

```python
# Pseudocode for the per-handler entry sequence:
def _gate(request: Request, endpoint: str):
    mode = os.environ.get('SEARCH_API_MODE', 'open').strip().lower()  # re-read per D-02
    if mode == 'disabled':
        raise APIError('disabled', 'Search API disabled', http_status=503)
    if mode == 'localhost-only':
        # D-03: trust XFF first hop only when client.host is loopback
        client = (request.client.host if request.client else '')
        if client in {'127.0.0.1', '::1'}:
            xff = (request.headers.get('x-forwarded-for', '') or '').split(',')[0].strip()
            ok = xff in {'127.0.0.1', '::1', ''}
        else:
            ok = False
        if not ok:
            raise APIError('localhost_only', 'Endpoint restricted to localhost', http_status=403)
    # Then rate-limit check on resolved IP (NOT XFF if non-loopback).
    _rate_limiter.check_or_raise(client_ip)
```

### Error Envelope

**Source:** new `web/api_hardening.py` exception handlers.
**Apply to:** all Phase 78/79/80 routes (registered against `target_app` in `init_search_api`).

```python
# Two distinct paths per D-06:
#   RequestValidationError -> 400 {error:{code:'invalid_request', message, fields:[...]}}
#   APIError(code, message, http_status) -> http_status {error:{code, message}}
```

Code taxonomy from D-07 (lowercase snake_case, contractually stable for Phase 81 skill):
`invalid_request`, `invalid_mode`, `query_required`, `query_too_long`, `limit_too_high`, `unknown_filter_key`, `unresolvable_filter_value`, `rate_limited`, `disabled`, `localhost_only`, `internal_error`. Warning code (in `warnings:[]`, not error): `query_downgraded`.

### PostHog Server-Side Capture

**Source:** new `web/api_hardening.py: capture_api_event(...)`.
**Apply to:** every Phase 78/79/80 handler exit (success or error).

Event: `search_api_request`. `distinct_id`: IP-hash. Properties: `endpoint`, `mode`, `latency_bucket` (D-12), `result_count_bucket` (D-12), `status_code`, `error_code`. **Never:** query, filters, gap, response items, snippets, full text (HARDEN-05 explicit).

### Singleton Read-Only Use

**Source:** `web/state.py: state.searcher`, `state.meta_mgr` (existing, process-wide, thread-safe).
**Apply to:** all Phase 78 handler code paths.

```python
# OK (process-wide singletons):
state.searcher.execute_search(...)
state.meta_mgr.parse_full_id_components(...)

# FORBIDDEN (D-20, session/legacy state):
state.last_results            # legacy export-route echo
state.current_search_query    # ditto
app.storage.user              # NiceGUI session
request.cookies               # session
```

### `target_app = app_override or app` Registrar Convention

**Source:** `web/api.py:186` (and the immutability test at `tests/test_api_export_json.py:192-210`).
**Apply to:** `web/search_api.py: init_search_api`. Every route uses `@target_app.<verb>(...)` not `@app.<verb>(...)`.

## No Analog Found

| File | Role | Reason |
|------|------|--------|
| `scripts/soak_search_api.py` | standalone live-deployment CLI burst | No prior soak script exists in the repo. Pattern is straightforward (`argparse` + `requests` loop) and directly described in D-22. Planner uses the synthesized excerpt above. |

## Metadata

**Analog search scope:** `web/`, `shared/`, `tests/`, `genizah_core.py`, `CLAUDE.md`, `.planning/`.
**Files scanned:** `web/api.py` (registrar + rate limiter), `web/main.py` (PostHog init + bootstrap), `web/state.py` (singleton + forbidden fields), `web/puzzle_tokens.py` (HMAC + secret), `web/feature_flags.py` (env-flag), `web/analytics.py` (client-side PostHog), `shared/fjms_service.py` (filter resolution signature), `shared/search_serializer.py` (envelope), `genizah_core.py` (execute_search + cascade), `tests/test_api_export_json.py` (TestClient harness).
**Pattern extraction date:** 2026-04-28.

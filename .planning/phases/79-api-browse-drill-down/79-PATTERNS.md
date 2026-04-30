# Phase 79: /api/browse Drill-Down — Pattern Map

**Mapped:** 2026-04-29
**Files analyzed:** 6 new/modified files
**Analogs found:** 6 / 6

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `web/search_api.py` (MODIFY: add `GET /api/browse` route + `BrowseRequest` model) | route handler + Pydantic model | request-response (async I/O fan-out) | `web/search_api.py:141-373` (existing `POST /api/search` body in same module) | exact — same module, mirror endpoint |
| `shared/search_serializer.py` (MODIFY: add `serialize_browse_payload`) | serializer | transform | `shared/search_serializer.py:313-371` (`serialize_search_payload`) | exact — sibling function in same module |
| `shared/api_errors.py` (MODIFY: extend `ERROR_CODES` with 3 new codes) | config / error taxonomy | constant table | `shared/api_errors.py:24-37` (`ERROR_CODES = frozenset({...})`) | exact — append to existing frozenset |
| `shared/browse_service.py` (CREATE — D-23 preferred path) | service (pure-data enrichment fan-out) | event-driven (asyncio.gather) | `web/pages/browse_enrichment.py:66-264` (`load_enrichment` + 4 inner fetchers) | role-match — UI-coupled analog; planner extracts pure-data version |
| `tests/test_browse_api.py` (CREATE) | test (TestClient unit) | request-response | `tests/test_search_api.py:1-200` (Phase 78 TestClient pattern) | exact — same module under test, mirror harness |
| `tests/test_api_legacy_unchanged.py` (MODIFY: add image-proxy assertions) | test (legacy spot check) | request-response | `tests/test_api_legacy_unchanged.py:36-78` (existing legacy export assertion) | exact — same file extended |
| `CLAUDE.md` (MODIFY: 3 new env vars) | config-doc | constant table | `CLAUDE.md` "Environment Variables" section (Phase 78's 4 lines) | exact — append to existing section |

---

## Pattern Assignments

### `web/search_api.py` — adding `GET /api/browse` (route handler + Pydantic model)

**Analog:** `web/search_api.py:141-373` (existing `POST /api/search` handler) — same module, same registrar function `init_search_api(app_override=None)`. The new route is registered inside the SAME `init_search_api()` body, immediately after the `@target_app.post('/api/search')` decorator block. NO new module file (per D-20).

**Imports pattern** (lines 31-50, mirror verbatim — only `Request` is needed for GET):
```python
import logging
import time
from typing import Literal, Optional, List

from nicegui import app
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, ValidationError as PydanticValidationError

from web.state import state
from web.api_hardening import (
    RateLimiter,
    enforce_mode_gate,
    _resolve_rate_limit_key,
    capture_api_event,
    _build_envelope_response,
)
from shared.api_errors import APIError
```

**Module-level RateLimiter (D-18 — new instance, separate bucket)** — analog at line 55:
```python
# Phase 78 pattern (line 55):
_rate_limiter = RateLimiter(default_limit=30)

# Phase 79 ADDITION (sibling instance, same env-var ceiling, distinct bucket):
_browse_rate_limiter = RateLimiter(default_limit=30)
```
Both instances read `SEARCH_API_RATE_LIMIT` via `RateLimiter._current_limit()` on every `check()`. Per D-18, each endpoint gets its own per-IP counter against the same env ceiling.

**Pydantic model placement** — analog at lines 77-97 (`FiltersModel`, `SearchRequest` defined inline):
```python
class FiltersModel(BaseModel):
    model_config = ConfigDict(extra='forbid')
    domains: Optional[List[str]] = None
    # ...

class SearchRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    query: str
    mode: Literal['text', 'Title', 'Shelfmark', 'Responsa']
    gap: int = 0
    limit: int = 50
    filters: Optional[FiltersModel] = None
```
Phase 79 places `BrowseRequest(BaseModel)` next to `SearchRequest`. Per D-21:
```python
class BrowseRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')
    sys_id: str
    uid: Optional[str] = None
    p_num: Optional[int] = None
    volume_ie: Optional[str] = None
    fl_id: Optional[str] = None
    text_cap: Optional[int] = None  # R-08: per-request override [100, 10000]
```
**NOTE:** GET endpoint — Pydantic doesn't bind GET query params automatically as a single model. Pattern: read query params from `request.query_params` and construct `BrowseRequest(**params)` inside the handler body, mirroring lines 162-183 (`request.json()` → `SearchRequest(**body)` with `PydanticValidationError` caught locally).

**Idempotency marker pattern** (lines 125-135 — re-use as-is, no change):
```python
target_app = app_override if app_override is not None else app
if getattr(target_app.state, 'search_api_initialized', False):
    logger.debug("init_search_api(): app %r already initialized; skipping", target_app)
    return
target_app.state.search_api_initialized = True
```
Phase 79 adds the `@target_app.get('/api/browse')` decorator block ABOVE the closing `logger.info(...)` line at 373; the SAME idempotency marker covers both routes (the registrar function only runs its body once per app).

**Route body skeleton** — adapt lines 141-371 wholesale. Critical differences for browse:

```python
@target_app.get('/api/browse')
async def browse_endpoint(request: Request):
    """GET /api/browse — Phase 79 drill-down endpoint (Concern #2 pattern)."""
    t0 = time.monotonic()
    endpoint_name = 'browse'
    client_ip = _resolve_rate_limit_key(request)
    status_code = 200
    error_code: Optional[str] = None
    result_count: Optional[int] = None
    validated_mode: Optional[str] = None  # always None for browse (no 'mode' field)

    try:
        # 0. Parse query params + Pydantic validation.
        try:
            params = dict(request.query_params)
            # Coerce numeric strings (FastAPI doesn't auto-coerce for non-bound params).
            for k in ('p_num', 'text_cap'):
                if k in params and params[k] != '':
                    params[k] = int(params[k])
            req = BrowseRequest(**params)
        except (ValueError, PydanticValidationError):
            status_code = 400
            error_code = 'invalid_request'
            raise

        # 1. Mode gate (reuse — D-04 disabled / D-03 localhost-only).
        enforce_mode_gate(request)

        # 2. Rate limit (D-18 — DIFFERENT instance from search bucket).
        _browse_rate_limiter.check(client_ip)

        # 3. Locator validation (D-03):
        #    - parse uid → IE/P/FL components
        #    - if uid + (volume_ie | p_num | fl_id) supplied AND mismatch → 400 'locator_conflict'
        #    - if NONE of (uid, p_num, fl_id) supplied → 400 'invalid_request'
        #    - p_num <= 0 → 400 'invalid_request'
        #    - text_cap outside [100, 10000] → 400 'invalid_request'
        # (planner spells the exact comparison logic in PLAN body)

        # 4. Resolve text_cap (R-08 priority: ?text_cap > env > default 4000).
        text_cap = _resolve_text_cap(req.text_cap)

        # 5. Enrichment fan-out — asyncio.gather across 4 sources, EACH wrapped
        #    in asyncio.wait_for() per D-15. Core fetch ALSO timed (R-01).
        #    See `shared/browse_service.py` analog below.
        bundle, warnings_list = await fetch_browse_bundle(
            sys_id=req.sys_id, uid=req.uid, p_num=req.p_num,
            volume_ie=req.volume_ie, fl_id=req.fl_id,
        )

        # 6. Core resolution failure → 404 (D-16).
        if bundle.page is None:
            raise APIError('manuscript_page_not_found', '...', http_status=404)

        # 6b. R-03 — uid round-trip verification: if req.uid was supplied AND
        #     bundle.page.uid != req.uid → 404 'manuscript_page_not_found'.

        # 7. Serialize via sibling serializer (D-26).
        from shared.search_serializer import serialize_browse_payload
        envelope = serialize_browse_payload(
            page=bundle.page,
            pgp=bundle.pgp,
            fjms=bundle.fjms,
            nli=bundle.nli,
            requested_uid=req.uid,
            requested_fl_id=req.fl_id,
            text_cap=text_cap,
            warnings=warnings_list,
        )
        result_count = 1
        return envelope

    except APIError as exc:
        status_code = exc.http_status
        error_code = exc.code
        return _build_envelope_response(request, exc)
    except (RequestValidationError, PydanticValidationError) as exc:
        status_code = 400
        error_code = 'invalid_request'
        return _build_envelope_response(request, exc)
    except Exception:
        logger.exception('browse_endpoint unhandled exception')
        status_code = 500
        error_code = 'internal_error'
        err = APIError('internal_error', 'Internal error', http_status=500)
        return _build_envelope_response(request, err)
    finally:
        try:
            elapsed = time.monotonic() - t0
            capture_api_event(
                endpoint=endpoint_name,
                mode=validated_mode,
                latency_seconds=elapsed,
                result_count=result_count,
                status_code=status_code,
                error_code=error_code,
                client_ip=client_ip,
            )
        except Exception:
            logger.warning('capture_api_event failed in browse finally block')
```

**What Phase 79 PRESERVES vs the analog:**
- **Preserves:** module-level RateLimiter pattern, idempotency marker on `target_app.state`, per-endpoint try/except/finally with `_build_envelope_response`, finally-block `capture_api_event` call, Concern #2 (NO global exception handlers), endpoint_name string for PostHog properties.
- **Changes:** GET method (not POST) — query params parsed from `request.query_params` not JSON body; second `RateLimiter` instance; no `mode` field in PostHog event (always `None` per D-14 schema where `mode` is optional); no responsa downgrade thread-local consume (browse doesn't run search); `serialize_browse_payload` not `serialize_search_payload`.

**Concern #2 reminder** (analog docstring lines 1-29): handler MUST catch Pydantic errors INSIDE its body so finally-block PostHog fires `invalid_request` events. Phase 79 inherits this exactly.

---

### `shared/search_serializer.py` — adding `serialize_browse_payload`

**Analog:** `shared/search_serializer.py:313-371` (`serialize_search_payload`) — sibling function in the same module. Per D-26, Phase 79 places `serialize_browse_payload` next to it; both expose a `source` field (`'search'` vs `'browse'`).

**Existing helpers Phase 79 REUSES verbatim:**
- `SCHEMA_VERSION = 1` (line 52) — constant unchanged.
- `_utc_iso_now()` (lines 269-271) — same `generated_at` format.
- `_safe_library_name(code)` (lines 129-137) — library code → English name with graceful degrade.
- `NLI_RESOLVABLE_LIBRARY_CODES` (lines 61-63) — the whitelist; Phase 79's library-aware picker uses a DIFFERENT set (CUL/Manchester/JTS/Oxford get specialized proxies, all others fall back to NLI by sys_id).

**Existing function shape to mirror** (lines 313-371):
```python
def serialize_search_payload(
    results: list[dict],
    *,
    meta_mgr: Any,
    query: str = '',
    mode: str = 'text',
    gap: Optional[int] = None,
    filters: Optional[dict] = None,
    warnings: Optional[list[str]] = None,
    total: Optional[int] = None,
) -> dict:
    # ... batch FJMS lookup ...
    items = [_serialize_item(r, ...) for r in results]
    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'search',
        'query': query or '',
        'mode': mode or 'text',
        'gap': gap,
        'filters': filters,
        'count': len(items),
        'total': total if total is not None else len(items),
        'warnings': list(warnings) if warnings else [],
        'generated_at': _utc_iso_now(),
        'results': items,
    }
```

**Phase 79's `serialize_browse_payload` signature** (D-06 envelope shape):
```python
def serialize_browse_payload(
    *,
    page: 'BrowsePage',                   # web.services.BrowsePage dataclass
    pgp: Optional[dict],                  # PGP metadata + transcription bundle, or None
    fjms: Optional[dict],                 # FJMS subset, or None
    nli: Optional[dict],                  # NLI crossref subset, or None
    requested_uid: Optional[str],
    requested_fl_id: Optional[str],
    text_cap: int,
    warnings: Optional[list],
) -> dict:
    # Build locator (R-04: fl_id always echoed at top level).
    locator = {
        'uid': page.uid or None,
        'sys_id': page.sys_id or None,
        'volume_ie': page.volume_ie,
        'p_num': page.p_num,
        'fl_id': page.fl_id,
    }
    # Build text + truncation flag (D-10, D-11).
    text, text_source, text_truncated, trunc_warning = _resolve_browse_text(
        page=page, pgp=pgp, text_cap=text_cap,
    )
    if trunc_warning:
        warnings = (warnings or []) + [trunc_warning]
    # Build image (D-12, D-13, D-14) — see _build_browse_image_url helper.
    image = _build_browse_image(page, nli)
    # R-07: each metadata group is None OR fully-populated dict.
    metadata = {
        'pgp': _build_pgp_subset(pgp) if pgp else None,
        'fjms': _build_fjms_subset(fjms) if fjms else None,
        'nli': _build_nli_subset(nli, page) if nli else None,
    }
    return {
        'schema_version': SCHEMA_VERSION,
        'source': 'browse',
        'generated_at': _utc_iso_now(),
        'locator': locator,
        'page_indexing': '1-based',
        'shelfmark': page.shelfmark or '',
        'title': page.title or '',
        'library': {
            'code': page.library_code or '',
            'name': _safe_library_name(page.library_code),
        },
        'text': text,
        'text_source': text_source,
        'text_truncated': text_truncated,
        'metadata': metadata,
        'image': image,
        'warnings': list(warnings) if warnings else [],
    }
```

**Library-aware image picker** — analog at lines 96-126 (`_build_image_url`) handles the NLI-only case for search. Phase 79 generalizes via a NEW sibling `_build_browse_image_url(sys_id, p_num, library_code) -> tuple[str, str]`:

Existing `_build_image_url` (line 96-126, the search/export pattern — Phase 79 does NOT modify this):
```python
def _build_image_url(sys_id, p_num, library_code=None):
    if not sys_id or not p_num:
        return None
    if not library_code or library_code not in NLI_RESOLVABLE_LIBRARY_CODES:
        return None
    try:
        page_idx = max(0, int(p_num) - 1)
    except (ValueError, TypeError):
        return None
    return f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}"
```

Phase 79's `_build_browse_image_url` mapping (D-12 — verified against `web/api.py`):

| `library_code` | URL template | Source line |
|----------------|--------------|-------------|
| `'CUL'` | `/api/cambridge_image/{sys_id}?page={p_num-1}` | `web/api.py:610` |
| `'Manchester'` | `/api/manchester_image/{sys_id}?page={p_num-1}` | `web/api.py:775` |
| `'JTS'` | `/api/jts_image/{sys_id}?page={p_num-1}` | `web/api.py:833` |
| `'Oxford'` (or `is_oxford`) | `/api/oxford_image/{sys_id}?page={p_num-1}` | `web/api.py:896` |
| default (BL/RNL/AIU/Mosseri/Gaster/Halper/etc.) | `/api/nli_image_by_sysid/{sys_id}?page={p_num-1}` | `web/api.py:573` |

**CORRECTION TO CONTEXT D-12:** CONTEXT.md said Oxford uses `/api/oxford_image/{shelfmark}/{p_num-1}` but the actual route signature at `web/api.py:896` is `@target_app.get('/api/oxford_image/{sys_id}')` with `def oxford_image(sys_id: str, page: int = 0, width: int = 2000)`. **Use sys_id+query-param form**, identical to the other three. Page indexing on all proxy URLs is **0-based** (matches existing `web/pages/search_results.py:629-657` UI emission and Phase 77 `_build_image_url`).

**`image.sources[]` shape** (D-13 with R-05/R-06 — kind+fl_id+folio_label fields, may be `[]`):
```python
# Each entry:
{'url': str, 'provider': str, 'role': str, 'kind': 'image' | 'viewer',
 'fl_id': str | None, 'folio_label': str | None}
# role ∈ {'iiif_proxy', 'external_viewer', 'companion_folio'}
# Default mapping: iiif_proxy + companion_folio → kind='image';
#                  external_viewer → kind='viewer'.
```
Sources to populate, in order:
1. The library-aware proxy → `role='iiif_proxy', kind='image'`.
2. Each entry in `BrowsePage.cambridge_images` (lines 115-117 of `web/services.py:88`) → `role='companion_folio', kind='image'`.
3. `BrowsePage.library_viewer_url` if non-null → `role='external_viewer', kind='viewer'`. Read from analog `web/pages/browse_enrichment.py:223`.

---

### `shared/api_errors.py` — extending `ERROR_CODES`

**Analog:** `shared/api_errors.py:24-37` (`ERROR_CODES = frozenset({...})`) — the existing taxonomy that Phase 78 owns.

**Existing pattern:**
```python
ERROR_CODES = frozenset({
    'invalid_request',
    'invalid_mode',
    'query_required',
    'query_too_long',
    'limit_too_high',
    'unknown_filter_key',
    'unresolvable_filter_value',
    'filter_vocabulary_unavailable',
    'rate_limited',
    'disabled',
    'localhost_only',
    'internal_error',
})
```

**Phase 79 addition** — three new codes (referenced in D-03b, D-16):
```python
ERROR_CODES = frozenset({
    # ... existing 12 codes ...
    'locator_conflict',              # D-03 / R-02 — uid + other-fields disagree → HTTP 400
    'manuscript_page_not_found',     # D-16 / D-03b — locator resolves to None or uid mismatch → HTTP 404
    'core_timeout',                  # D-16 / R-01 — core BrowsePage fetch timeout → HTTP 504
})
```

**APIError raising convention** (analog usage in `web/search_api.py:200-223`):
```python
raise APIError(
    'locator_conflict',
    'uid IE99 conflicts with volume_ie=IE100',
    http_status=400,
)
raise APIError(
    'manuscript_page_not_found',
    'no page for sys_id=...,p_num=...',
    http_status=404,
)
raise APIError(
    'core_timeout',
    f'core resolver did not return within {timeout}s',
    http_status=504,
)
```

**No change to:** `WARNING_CODES` (line 40), `class APIError` (lines 43-77). Browse warnings (`enrichment_timeout`, `enrichment_failed`, `transcription_truncated`, `image_unavailable`, `volume_ie_defaulted`) appear in the response `warnings: []` array as free-form objects/strings — they are NOT promoted into `WARNING_CODES` because (a) `WARNING_CODES` is currently informational-only and not validated against, (b) browse warnings carry payload fields (`source`, `message`) that don't fit the bare-string `WARNING_CODES` set. Planner may keep them as plain strings or as `{code, message, ...}` dicts per D-13/D-14 wording — either is consistent with Phase 78 precedent (`['query_downgraded: <msg>']`).

---

### `shared/browse_service.py` — pure-data enrichment fan-out (D-23 PREFERRED PATH)

**Analog:** `web/pages/browse_enrichment.py:66-264` (`load_enrichment` containing inner async fetchers `fetch_pgp`, `fetch_fjms`, `fetch_crossref`, `fetch_browse_enrichment` + `asyncio.gather`).

**Why D-23 marks this path "preferred":** the analog mutates `BrowseState` (`state.enrichment_loading`, `state.all_sources`, `state.pgp_metadata`, `state.fjms_data`, `state.crossref_data`, `state.current_page.attribution`, etc.) — these are NiceGUI per-session UI fields that Phase 79's stateless API handler MUST NOT touch (D-22). A clean extraction returns a pure data bundle.

**Recommended extraction shape:**
```python
# shared/browse_service.py
from dataclasses import dataclass
from typing import Optional, Any
import asyncio
import os
import logging

logger = logging.getLogger(__name__)

DEFAULT_BROWSE_TIMEOUT = 1.0  # D-17, R-01 (lowered 2.0 → 1.0)
DEFAULT_BROWSE_CORE_TIMEOUT = 2.0  # D-17 NEW per R-01


@dataclass
class BrowseEnrichmentBundle:
    page: Optional[Any]               # web.services.BrowsePage or None
    pgp: Optional[dict]               # {transcription_section, full_doc, sources_for_page}
    fjms: Optional[dict]              # {source_names, has_measurements, has_visual_suggestions}
    nli: Optional[dict]               # {physical_metadata, folio_images, library_viewer_url, cambridge_alignment}


async def fetch_browse_bundle(
    *, sys_id: str,
    uid: Optional[str] = None,
    p_num: Optional[int] = None,
    volume_ie: Optional[str] = None,
    fl_id: Optional[str] = None,
) -> tuple[BrowseEnrichmentBundle, list]:
    """Pure-data enrichment fan-out for /api/browse.

    Mirrors the structure of web/pages/browse_enrichment.py:load_enrichment but
    returns data instead of mutating BrowseState. Stateless: re-entrant across
    concurrent requests on the same worker.
    """
    warnings_list: list = []
    timeout = float(os.environ.get('SEARCH_API_BROWSE_TIMEOUT', DEFAULT_BROWSE_TIMEOUT))
    core_timeout = float(os.environ.get('SEARCH_API_BROWSE_CORE_TIMEOUT', DEFAULT_BROWSE_CORE_TIMEOUT))

    # 1. Core fetch — wrapped in wait_for per R-01 (no longer exempt).
    page = await _fetch_core(sys_id, p_num, volume_ie, fl_id, core_timeout)
    if page is None:
        return BrowseEnrichmentBundle(None, None, None, None), warnings_list

    # 2. Three enrichment sources in parallel.
    pgp_task = _wrap(_fetch_pgp(page), 'pgp', timeout, warnings_list)
    fjms_task = _wrap(_fetch_fjms(page.sys_id), 'fjms', timeout, warnings_list)
    nli_task = _wrap(_fetch_nli(page.sys_id, page.p_num), 'nli', timeout, warnings_list)

    pgp, fjms, nli = await asyncio.gather(pgp_task, fjms_task, nli_task)

    return BrowseEnrichmentBundle(page=page, pgp=pgp, fjms=fjms, nli=nli), warnings_list
```

**Inner fetcher patterns to lift verbatim** from the analog:

**Core fetch** (analog calls live in `web/services.py:294` and `:408`):
```python
async def _fetch_core(sys_id, p_num, volume_ie, fl_id, timeout):
    from web.state import state

    def _sync():
        if fl_id:
            return state.searcher.get_browse_page_by_fl(fl_id, sys_id=sys_id)
        return state.searcher.get_browse_page(
            sys_id, p_num=p_num, volume_ie=volume_ie,
        )

    try:
        # Note: state.searcher.get_browse_page is sync — wrap via run_in_executor.
        # asyncio.wait_for cancels the await but NOT the underlying thread (R-09).
        loop = asyncio.get_event_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _sync),
            timeout=timeout,
        )
        # Result is a dict (genizah_core return shape); the WebDataService wraps it
        # into a BrowsePage dataclass. Plan 03 decides which level to return.
        return result
    except asyncio.TimeoutError:
        raise APIError('core_timeout', f'core resolver did not return within {timeout}s', http_status=504)
```

**PGP fetcher** — analog at `web/pages/browse_enrichment.py:70-83`:
```python
async def fetch_pgp():
    _page_sys_id = page.sys_id
    _page_p_num = page.p_num
    def _pgp_sync():
        all_sources = get_all_sources_for_fragment(_page_sys_id)
        pgp_doc = get_document_for_fragment(_page_sys_id, _page_p_num)
        return all_sources, pgp_doc
    try:
        return await run.io_bound(_pgp_sync)
    except Exception as e:
        logger.error(f"Failed to fetch PGP data: {e}")
        return None, None
```
**Phase 79's pure-data version** replaces `nicegui.run.io_bound` with `asyncio.get_event_loop().run_in_executor(None, _pgp_sync)` (NiceGUI's `run.io_bound` is unavailable from `shared/`). PGP transcription is page-section-scoped via `get_section_for_page()` from `shared/document_service.py:659`. Section scoping pattern (analog at line 280, 306):
```python
page_content = get_section_for_page(
    pgp_doc['transcription'],
    page.p_num,
    fragment_page_info=pgp_doc.get('_fragment_page_info'),
) if pgp_doc.get('transcription') else None
```

**FJMS fetcher** — analog at lines 85-122 — Phase 79's slim subset (D-08, three keys only):
```python
def _fjms_sync(_sys_id):
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    if not fjms.is_available():
        return None
    result = {
        'source_names': fjms.get_source_names(_sys_id),  # shared/fjms_service.py:2639
        'has_measurements': fjms.has_measurements(_sys_id),  # :3062
    }
    try:
        from shared.visual_similarity_service import get_vs_service
        vs_svc = get_vs_service(thread_safe=True)
        result['has_visual_suggestions'] = (
            vs_svc.is_available() and vs_svc.get_suggestion_count(_sys_id) > 0
        )
    except Exception:
        result['has_visual_suggestions'] = False
    return result
```

**NLI crossref fetcher** — analog at lines 123-142:
```python
def _nli_sync(_sys_id, _p_num):
    from shared.nli_crossref_service import get_nli_crossref_service
    svc = get_nli_crossref_service(thread_safe=True)
    if not svc.is_available() or not _sys_id:
        return None
    crossref_data = svc.get_crossref_metadata(_sys_id)  # :724
    folio_images = svc.get_folio_images(_sys_id)  # :252
    # D-09: extract physical_metadata + active-page folio only.
    active_folio = None
    if folio_images and 0 < _p_num <= len(folio_images):
        f = folio_images[_p_num - 1]
        active_folio = {
            'fl_id': f.get('fl_id'),
            'folio_label': f.get('folio_label', ''),
            'thumb_url': f.get('thumb_url'),
        }
    return {
        'physical_metadata': crossref_data.get('physical_metadata'),
        'folio': active_folio,
        'library_viewer_url': svc.get_library_viewer_url(_sys_id),
    }
```

**Per-source timeout wrapper** — D-16 partial-failure pattern:
```python
async def _wrap(coro, source_name, timeout, warnings_list):
    try:
        return await asyncio.wait_for(coro, timeout=timeout)
    except asyncio.TimeoutError:
        warnings_list.append({'code': 'enrichment_timeout', 'source': source_name})
        return None
    except Exception:
        logger.exception('Enrichment source %s failed', source_name)
        warnings_list.append({'code': 'enrichment_failed', 'source': source_name})
        return None
```

**What Phase 79 PRESERVES vs the analog:**
- **Preserves:** The 4-way fan-out structure (core + PGP + FJMS + NLI), `asyncio.gather` for parallelism, per-source try/except + log-and-continue, the inner sync-thunk pattern (`def _sync(): ... return await run.io_bound(_sync)`), use of `shared/document_service.get_section_for_page` for page-section text extraction, use of singleton thread-safe services (`get_fjms_service(thread_safe=True)`, `get_nli_crossref_service(thread_safe=True)`).
- **Changes:** Returns a `BrowseEnrichmentBundle` dataclass instead of mutating `BrowseState`; uses `asyncio.wait_for` per source (analog has no per-source timeouts — UI tolerates slow loads); uses `loop.run_in_executor` instead of `nicegui.run.io_bound` (`shared/` cannot import nicegui); core fetch ALSO timed (R-01 — analog has no core timeout); drops UI-specific extras (Oxford translations, Cambridge MARC alignment, attribution cascade, derived_fl_id rewriting — all UI-coupled per analog lines 144-254, deferred per D-08/D-09 to keep payload minimal).

**Operational note (R-09):** `asyncio.wait_for` cancels the await but the executor thread keeps running. If a sidecar SQLite read hangs, the executor pool can starve. CONTEXT.md flags this as a monitoring obligation; Phase 79 may want to bump the default executor `max_workers` (default is `min(32, os.cpu_count()+4)` on Python 3.10+ — likely fine, but document).

---

### `tests/test_browse_api.py` — pytest TestClient unit tests

**Analog:** `tests/test_search_api.py:1-200` — Phase 78's TestClient harness. Phase 79 mirrors the fixture set + happy-path + validation + error-envelope + statelessness patterns.

**Imports + fixtures pattern** (lines 14-108):
```python
import json
import pytest
from unittest.mock import MagicMock
from fastapi import FastAPI
from fastapi.testclient import TestClient

from web.search_api import init_search_api, BrowseRequest  # NEW: BrowseRequest
from web.api_hardening import (
    RateLimiter,
    capture_api_event,
    _build_envelope_response,
    _resolve_rate_limit_key,
)
from shared.api_errors import APIError


@pytest.fixture
def bare_app():
    """Per-test bare app — fresh idempotency marker each test."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    return bare


@pytest.fixture
def client(bare_app):
    return TestClient(bare_app)


@pytest.fixture
def mock_searcher():
    """Replace state.searcher with a MagicMock that returns a synthetic
    BrowsePage-shaped dict from get_browse_page(...).
    """
    from web.state import state
    saved = state.searcher
    fake = MagicMock()
    fake.get_browse_page.return_value = {
        'uid': 'IE99_P7_FL12345',
        'p_num': 7,
        'text': 'Hebrew text snippet...',
        'full_header': 'header_9912345678901234_IE99_P7_FL12345',
        'total_pages': 50,
        'current_idx': 6,
        'sys_id': '9912345678901234',
        'volume_ie': 'IE99',
    }
    fake.get_browse_page_by_fl.return_value = fake.get_browse_page.return_value
    state.searcher = fake
    yield fake
    state.searcher = saved


@pytest.fixture
def clean_env(monkeypatch):
    monkeypatch.setenv('SEARCH_API_MODE', 'open')
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '30')
    monkeypatch.setenv('SEARCH_API_POSTHOG_SAMPLE_N', '999999')
    monkeypatch.setenv('SEARCH_API_BROWSE_TIMEOUT', '1.0')
    monkeypatch.setenv('SEARCH_API_BROWSE_CORE_TIMEOUT', '2.0')
    monkeypatch.setenv('SEARCH_API_BROWSE_TEXT_CAP', '4000')
```

**Idempotency test pattern** (analog at lines 128-136):
```python
def test_init_search_api_registers_browse_endpoint():
    """Phase 79: init_search_api must register both /api/search AND /api/browse."""
    bare = FastAPI()
    init_search_api(app_override=bare)
    paths = {getattr(r, 'path', None) for r in bare.routes}
    assert '/api/search' in paths
    assert '/api/browse' in paths
```

**Happy path pattern** (analog at lines 155-163):
```python
def test_browse_happy_path_uid(client, populated_state, clean_env):
    r = client.get('/api/browse', params={
        'sys_id': '9912345678901234', 'uid': 'IE99_P7_FL12345',
    })
    assert r.status_code == 200, r.json()
    body = r.json()
    assert body['source'] == 'browse'
    assert body['schema_version'] == 1
    assert body['locator']['sys_id'] == '9912345678901234'
    assert body['locator']['uid'] == 'IE99_P7_FL12345'
    assert body['locator']['fl_id'] is not None  # R-04
    assert body['page_indexing'] == '1-based'
    assert isinstance(body.get('warnings'), list)
```

**Locator round-trip test (D-24, D-27)** — chain `/api/search` → `/api/browse`:
```python
def test_locator_round_trip_search_to_browse(client, populated_state, clean_env):
    r1 = client.post('/api/search', json={'query': 'foo', 'mode': 'text'})
    assert r1.status_code == 200
    for item in r1.json()['results']:
        loc = item['locator']
        if not loc['sys_id']:
            continue
        params = {'sys_id': loc['sys_id']}
        if item.get('uid'):
            params['uid'] = item['uid']
        elif loc.get('p_num'):
            params['p_num'] = loc['p_num']
            if loc.get('volume_ie'):
                params['volume_ie'] = loc['volume_ie']
        r2 = client.get('/api/browse', params=params)
        assert r2.status_code == 200, r2.json()
```

**Error-envelope tests** mirror analog lines 280-310 (replace per D-24 cases): conflict, missing locator, bogus sys_id, image degrade (mock proxy 503), transcription truncation, enrichment timeout, statelessness diff (modulo `generated_at`), rate-limit independence (burst 31 on `/api/browse` does NOT exhaust the search bucket).

**Rate-limit independence pattern** (NEW for Phase 79 — verifies D-18):
```python
def test_browse_rate_limit_independent_from_search(client, populated_state, monkeypatch):
    monkeypatch.setenv('SEARCH_API_RATE_LIMIT', '5')
    # Reset both buckets.
    from web.search_api import _rate_limiter, _browse_rate_limiter
    _rate_limiter.reset_for_tests()
    _browse_rate_limiter.reset_for_tests()
    # Hit /api/browse 5x — fills its bucket.
    for _ in range(5):
        client.get('/api/browse', params={'sys_id': '...', 'p_num': 1})
    r6 = client.get('/api/browse', params={'sys_id': '...', 'p_num': 1})
    assert r6.status_code == 429
    # /api/search bucket is independent — should still succeed.
    rs = client.post('/api/search', json={'query': 'foo', 'mode': 'text'})
    assert rs.status_code == 200, 'search bucket leaked into browse counter'
```

---

### `tests/test_api_legacy_unchanged.py` — append image-proxy assertions (D-25)

**Analog:** `tests/test_api_legacy_unchanged.py:36-78` (existing `test_legacy_export_route_shape_unchanged`). Phase 79 adds ONE additional test asserting that legacy image proxies still respond after `init_search_api` runs.

**Existing pattern to mirror** (lines 27-78): `client_with_both_inits` fixture mounts BOTH `init_api_routes` and `init_search_api` on a bare app, then asserts the legacy route's response is byte-identical to its pre-Phase-78 shape. Phase 79 adds assertions for `/api/nli_image_by_sysid` and `/api/cambridge_image`:

```python
def test_legacy_nli_image_route_unchanged(client_with_both_inits):
    """D-25: Phase 79 must not perturb /api/nli_image_by_sysid behavior."""
    # Use a known stable sys_id from test fixtures, OR mock _fetch_nli_image_bytes.
    # Bogus sys_id should return 404 (not 200, not 5xx, not Phase 78 envelope).
    r = client_with_both_inits.get('/api/nli_image_by_sysid/0000?page=0')
    assert r.status_code == 404, r.text
    # CRITICAL: legacy route MUST NOT return a Phase 78 error envelope.
    body_text = r.text
    assert '"error"' not in body_text or '"code"' not in body_text, (
        'legacy /api/nli_image_by_sysid leaked Phase 78 envelope shape'
    )


def test_legacy_cambridge_image_route_unchanged(client_with_both_inits):
    """D-25: same for /api/cambridge_image."""
    r = client_with_both_inits.get('/api/cambridge_image/0000?page=0')
    assert r.status_code in (404, 502, 503), r.text  # Various legitimate failure paths
```

---

### `CLAUDE.md` — Environment Variables section addition

**Analog:** existing `## Environment Variables` block in `CLAUDE.md` (lines around the Phase 78 entries `SEARCH_API_MODE`, `SEARCH_API_RATE_LIMIT`, `POSTHOG_IP_SALT`, `SEARCH_API_POSTHOG_SAMPLE_N`).

**Existing format:**
```
SEARCH_API_MODE=open (one of: open | localhost-only | disabled; default: open; flippable per request without restart)
SEARCH_API_RATE_LIMIT=30 (per-IP requests per minute; default: 30)
```

**Phase 79 additions** (D-17, R-01, R-08):
```
SEARCH_API_BROWSE_TIMEOUT=1.0 (per-source enrichment timeout for /api/browse PGP/FJMS/NLI fetches in seconds; default: 1.0)
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0 (core BrowsePage fetch timeout for /api/browse in seconds; default: 2.0)
SEARCH_API_BROWSE_TEXT_CAP=4000 (default char cap for transcription text in /api/browse; per-request override via ?text_cap=N bounded by [100, 10000]; default: 4000)
```

---

## Shared Patterns

### Authentication / Mode Gate
**Source:** `web/api_hardening.py:251-267` (`enforce_mode_gate`)
**Apply to:** `browse_endpoint` body, immediately after Pydantic validation, BEFORE rate limit check.
```python
mode = (os.environ.get('SEARCH_API_MODE', 'open') or 'open').strip().lower()
if mode == 'disabled':
    raise APIError('disabled', 'Search API disabled', http_status=503)
if mode == 'localhost-only':
    if not _is_loopback_request(request):
        raise APIError('localhost_only', 'Endpoint restricted to localhost', http_status=403)
```
Phase 79 calls `enforce_mode_gate(request)` — same as Phase 78's analog at line 188. No re-implementation; the env-re-read-per-request semantic is preserved automatically.

### Rate Limit Check
**Source:** `web/api_hardening.py:155-244` (`class RateLimiter`)
**Apply to:** `browse_endpoint` — uses a SEPARATE instance (`_browse_rate_limiter`) per D-18.
```python
_browse_rate_limiter.check(client_ip)  # raises APIError('rate_limited', http_status=429, headers={'Retry-After': N})
```
The `Retry-After` header is preserved through `_build_envelope_response` because `APIError(..., headers={...})` is honored at line 304.

### Error Handling
**Source:** `web/api_hardening.py:274-330` (`_build_envelope_response`) + `shared/api_errors.py:43-77` (`class APIError`)
**Apply to:** Every non-2xx exit from `browse_endpoint`. Pattern: raise `APIError(code, message, http_status=N, headers=...)` from anywhere in the body; the outer `except APIError` branch routes it through `_build_envelope_response(request, exc)` which emits `{error: {code, message}}` with the supplied status.

```python
return _build_envelope_response(request, exc)  # → JSONResponse with envelope shape
```

### PostHog Capture
**Source:** `web/api_hardening.py:572-620` (`capture_api_event`)
**Apply to:** `browse_endpoint`'s finally block — fires once per request, success or error, with `endpoint='browse'`.
```python
capture_api_event(
    endpoint='browse',          # D-14 — distinct from 'search'
    mode=None,                  # browse has no 'mode' field
    latency_seconds=elapsed,
    result_count=result_count,  # 1 on success, None on error
    status_code=status_code,
    error_code=error_code,
    client_ip=client_ip,
)
```
Reuses the queue/daemon-thread/sampling/IP-hash machinery built in Phase 78 — no new helpers in `api_hardening.py` per D-15.

### Validation (Pydantic + Concern #2 inline)
**Source:** `web/search_api.py:162-183` (the `try: body = await request.json(); req = SearchRequest(**body); except PydanticValidationError: ...` pattern)
**Apply to:** `browse_endpoint` adapted for query-params:
```python
try:
    params = dict(request.query_params)
    for k in ('p_num', 'text_cap'):
        if k in params and params[k] != '':
            try:
                params[k] = int(params[k])
            except ValueError:
                raise APIError('invalid_request', f'{k} must be int', http_status=400)
    req = BrowseRequest(**params)
except PydanticValidationError:
    status_code = 400
    error_code = 'invalid_request'
    raise
```
Concern #2 reminder: catching Pydantic errors INSIDE the body so finally-block PostHog can fire `invalid_request` events.

### Stateless Contract (D-22)
**Source:** `web/search_api.py:263` (`# 6. Statelessness check (D-20). Forbidden reads — none below.`)
**Apply to:** `browse_endpoint` and `shared/browse_service.py`. Verification (D-22): planner runs `! grep -qE "state\.last_results|state\.current_search_query|app\.storage|request\.cookies"` against Plan 03's output before merge.

Permitted reads: `state.searcher`, `state.meta_mgr`. Forbidden: `state.last_results`, `state.current_search_query`, `app.storage.*`, `request.cookies`, `BrowseState` (UI-only).

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| (none) | — | — | All 6 files have clear analogs in the Phase 77/78 ancestry. |

`shared/browse_service.py` is technically NEW but its analog in `web/pages/browse_enrichment.py` is well-established and the extraction is a known-good refactor pattern from Phase 25 (FjmsService extraction) and Phase 29 (NliCrossrefService extraction) — both shipped successfully via the `web/ → shared/` migration playbook.

---

## Metadata

**Analog search scope:**
- `web/search_api.py`, `web/api_hardening.py`, `shared/api_errors.py`, `shared/search_serializer.py` (Phase 78/77 ancestry)
- `web/pages/browse_enrichment.py` (UI enrichment fan-out)
- `web/services.py:88, 294, 408` (`BrowsePage` + `get_browse_page` + `get_browse_page_by_fl`)
- `shared/document_service.py:659, 950, 1042` (`get_section_for_page`, `get_document_for_fragment`, `get_all_sources_for_fragment`)
- `shared/fjms_service.py:2639, 3062` (`get_source_names`, `has_measurements`)
- `shared/nli_crossref_service.py:252, 349, 724` (`get_folio_images`, `get_physical_metadata`, `get_crossref_metadata`)
- `web/api.py:573, 610, 775, 833, 896` (image proxy routes — verified URL signatures)
- `tests/test_search_api.py`, `tests/test_api_legacy_unchanged.py` (Phase 78 test patterns)

**Files scanned:** 11 source files + 2 test files + 2 context docs

**Pattern extraction date:** 2026-04-29

**Notes for planner:**
1. **D-12 correction confirmed:** Oxford image proxy uses `sys_id` query-param form `/api/oxford_image/{sys_id}?page={N}`, NOT shelfmark-keyed as CONTEXT.md tentatively phrased.
2. **D-23 D-23 preferred path is viable.** The UI analog `web/pages/browse_enrichment.py:144-254` (`fetch_browse_enrichment` inner thunk) IS UI-coupled (reads `state.meta_mgr.nli_cache`, mutates `pg.attribution`, etc.), but the four core fetchers (`fetch_pgp`, `fetch_fjms`, `fetch_crossref`, plus the new core fetch) are clean to extract as pure-data variants. The cleanest extraction lifts the inner sync thunks (`_pgp_sync`, `_fjms_sync`, `_crossref_sync`) verbatim and replaces `nicegui.run.io_bound` with `loop.run_in_executor(None, ...)` so `shared/` doesn't import nicegui.
3. **R-09 executor starvation:** `asyncio.wait_for` does NOT kill the executor thread. Default executor pool size is `min(32, os.cpu_count()+4)` on Python 3.10+ — adequate for v7.10 but flag in PLAN for monitoring.
4. **CONTEXT.md note about `WARNING_CODES`:** the existing frozenset (line 40 of `shared/api_errors.py`) only contains `'query_downgraded'` as a bare string. Phase 79's warning entries are richer (`{'code': 'enrichment_timeout', 'source': 'pgp'}`) and don't fit the frozenset shape. Planner should NOT extend `WARNING_CODES`; instead emit warnings as objects/strings directly into the response `warnings: []` array, matching D-13 (image_unavailable) and D-16 (enrichment_timeout) wording.

## PATTERN MAPPING COMPLETE

**Phase:** 79 — api-browse-drill-down
**Files classified:** 6 (5 code + 1 doc)
**Analogs found:** 6 / 6

### Coverage
- Files with exact analog: 5 (search_api.py, search_serializer.py, api_errors.py, test_search_api.py, test_api_legacy_unchanged.py — all in Phase 78 ancestry)
- Files with role-match analog: 1 (browse_service.py — analog is UI-coupled but extraction is well-precedented)
- Files with no analog: 0

### Key Patterns Identified
- `web/search_api.py` is the single registrar module; Phase 79 adds the second route + `BrowseRequest` model inline; idempotency marker on `target_app.state.search_api_initialized` covers both routes.
- `RateLimiter` is per-instance per-IP-bucket; D-18 calls for a SECOND `_browse_rate_limiter = RateLimiter(default_limit=30)` instance distinct from `_rate_limiter` so search/browse counters are independent.
- Concern #2 lock: NO global FastAPI exception handlers; envelope rewriting via `_build_envelope_response` called from inside per-endpoint try/except. Pydantic errors caught locally so finally-block `capture_api_event` can pin `invalid_request` labels.
- Enrichment fan-out pattern: `asyncio.gather` of `asyncio.wait_for`-wrapped sync thunks executed via `run_in_executor`; per-source failure→`null` + warning, response stays 200; core failure→404 (D-16).
- Library-aware image picker: 5 routes verified at `web/api.py:573/610/775/833/896` — all use sys_id+page query-param form (NOT shelfmark-keyed).
- Page-section text extraction: `shared/document_service.py:659 get_section_for_page(transcription, page_num, sections=None, fragment_page_info=None)` — reused verbatim, source-of-truth.
- Sibling serializers in `shared/search_serializer.py`: existing `serialize_search_payload` shape (envelope keys + `_utc_iso_now()` + `_safe_library_name()`) directly mirrored as `serialize_browse_payload` with `source='browse'` and the namespaced `metadata: {pgp, fjms, nli}` group from D-06.

### File Created
`C:\GenizahSearch\.planning\phases\79-api-browse-drill-down\79-PATTERNS.md`

### Ready for Planning
Pattern mapping complete. Planner can now reference analog patterns + concrete code excerpts in PLAN.md files. Critical decisions surfaced for planner review: (1) D-12 Oxford route is sys_id-keyed (not shelfmark-keyed); (2) D-23 service-extraction path is clean and recommended; (3) R-09 executor starvation flagged for monitoring note in PLAN.

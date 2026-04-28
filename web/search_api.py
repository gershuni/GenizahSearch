"""Phase 78: POST /api/search.

Concern #2 fix (78-REVIEWS.md): init_search_api does NOT install global
exception handlers. The endpoint wraps its own body in try/except and calls
web.api_hardening._build_envelope_response from inside the except clause.
Legacy /api/* routes are unaffected.

Concern #6: Responsa downgrade warnings come from a thread-local meta channel
(genizah_core._consume_last_responsa_downgrade) so they survive empty results.

Concern #10 / R2-#2: init_search_api is idempotent — second call on the same
app is a no-op. The idempotency marker lives on `target_app.state.search_api_initialized`
(per-app, GC-safe), NOT a module-global set (which suffered id(app) GC reuse
hazard).

Concern #12: The endpoint catches PydanticValidationError inside its body so
the finally-block PostHog capture fires `invalid_request` events for
structural errors. Concern #2 forbids global exception handlers, so capturing
inside the handler is the only way to keep observability for those errors.

R2-#1: SearchEngine.execute_search clears the thread-local downgrade signal
on entry. The handler reads it on the success path AND has a defensive
finally-block consume so the thread-local is drained even on the exception
path — no stale-signal leak across requests on the same worker thread.

Stateless contract (D-20): handler MUST NOT read any of the forbidden
session-scoped surfaces (last results, current search query, storage user,
or request cookies).
"""

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
# Concern #3: APIError from neutral location.
from shared.api_errors import APIError

logger = logging.getLogger(__name__)


# Module-level RateLimiter; Phases 79/80 import this same instance.
_rate_limiter = RateLimiter(default_limit=30)


# ---------------------------------------------------------------------------
# Concern #6 — re-export downgrade reader for monkeypatchability.
# ---------------------------------------------------------------------------

def _consume_last_responsa_downgrade() -> Optional[str]:
    """Concern #6: read-and-clear the thread-local downgrade signal.

    Re-exported here so tests can monkeypatch
    `web.search_api._consume_last_responsa_downgrade`. Defers to the actual
    implementation in genizah_core.
    """
    from genizah_core import _consume_last_responsa_downgrade as _impl
    return _impl()


# ---------------------------------------------------------------------------
# Pydantic models (D-05, D-15).
# ---------------------------------------------------------------------------

class FiltersModel(BaseModel):
    """D-15 hybrid: lists for categorical filters, scalars for date bounds."""
    model_config = ConfigDict(extra='forbid')

    domains: Optional[List[str]] = None
    authors: Optional[List[str]] = None
    works: Optional[List[str]] = None
    materials: Optional[List[str]] = None
    date_from: Optional[int] = None
    date_to: Optional[int] = None


class SearchRequest(BaseModel):
    """API-01 / D-05."""
    model_config = ConfigDict(extra='forbid')

    query: str
    mode: Literal['text', 'Title', 'Shelfmark', 'Responsa']
    gap: int = 0
    limit: int = 50
    filters: Optional[FiltersModel] = None


# Constants (D-07, D-08, D-09).
QUERY_LENGTH_CAP = 1000
DEFAULT_LIMIT = 50
MAX_LIMIT = 200


# ---------------------------------------------------------------------------
# Idempotent registrar (Concern #10 / R2-#2).
# ---------------------------------------------------------------------------

def init_search_api(app_override: Optional[FastAPI] = None) -> None:
    """Register Phase 78 search-helper routes onto target_app.

    Concern #10 / R2-#2: idempotent. Re-calling on the same app is a no-op.
    The idempotency marker lives on `target_app.state.search_api_initialized`
    (per-app, GC-safe). Module-global set[int] would suffer id(app) GC reuse
    plus accumulate indefinitely across tests.

    Concern #2: does NOT install global exception handlers. Envelope
    rewriting happens INSIDE the endpoint via `_build_envelope_response`.

    Args:
        app_override: When None, registers onto the NiceGUI singleton. When a
                      bare FastAPI app is passed, registers onto that instead.
    """
    target_app = app_override if app_override is not None else app

    # R2-#2: app-bound idempotency marker (was Concern #10's module-global set
    # in round 1).
    if getattr(target_app.state, 'search_api_initialized', False):
        logger.debug(
            "init_search_api(): app %r already initialized; skipping",
            target_app,
        )
        return
    target_app.state.search_api_initialized = True

    # CRITICAL: Concern #2 — NO global exception handler is installed on
    # target_app. Envelope rewriting happens INSIDE the endpoint via
    # _build_envelope_response, called from per-endpoint try/except branches.

    @target_app.post('/api/search')
    async def search_endpoint(request: Request):
        """POST /api/search — Phase 78 hardened search endpoint.

        Concern #2: parses + validates Pydantic input INSIDE the body so we can
        catch RequestValidationError / PydanticValidationError locally and
        route them through `_build_envelope_response`. If we used FastAPI's
        standard `req: SearchRequest` parameter, Pydantic errors would bubble
        to the FastAPI handler chain — which would either hit FastAPI's
        default 422 handler (legacy behavior — fine) OR a globally-installed
        handler (which Concern #2 explicitly forbids).
        """
        t0 = time.monotonic()
        endpoint_name = 'search'
        client_ip = _resolve_rate_limit_key(request)
        status_code = 200
        error_code: Optional[str] = None
        result_count: Optional[int] = None
        validated_mode: Optional[str] = None

        try:
            # 0. Parse body and validate via Pydantic explicitly so we own
            #    the error path.
            try:
                body = await request.json()
            except Exception as exc:
                raise APIError(
                    'invalid_request',
                    f'request body must be valid JSON: {exc}',
                    http_status=400,
                )
            try:
                if isinstance(body, dict):
                    req = SearchRequest(**body)
                else:
                    req = SearchRequest.model_validate(body)
            except PydanticValidationError:
                # Concern #12: pin status_code/error_code so the finally block
                # fires the PostHog event with correct labels. Re-raise so
                # the outer except branch builds the envelope.
                status_code = 400
                error_code = 'invalid_request'
                raise

            validated_mode = req.mode

            # 1. Mode gate (D-02, D-03, D-04).
            enforce_mode_gate(request)

            # 2. Rate limit check (D-01 sliding window). Key is the trusted
            #    real client IP per Concern #1. On limit hit, RateLimiter.check
            #    raises APIError(rate_limited, http_status=429, headers={'Retry-After': N})
            #    which the outer except APIError branch routes through
            #    _build_envelope_response (preserves the Retry-After header).
            _rate_limiter.check(client_ip)

            # 3. Query post-validation.
            query = (req.query or '').strip()
            if not query:
                raise APIError(
                    'query_required',
                    'query is required and cannot be empty',
                    http_status=400,
                )
            if len(query) > QUERY_LENGTH_CAP:
                raise APIError(
                    'query_too_long',
                    f'query exceeds cap (max {QUERY_LENGTH_CAP} chars; '
                    f'submitted {len(query)})',
                    http_status=400,
                )
            if req.limit <= 0:
                raise APIError(
                    'invalid_request',
                    f'limit must be positive (submitted {req.limit})',
                    http_status=400,
                )
            if req.limit > MAX_LIMIT:
                raise APIError(
                    'limit_too_high',
                    f'limit exceeds max (max {MAX_LIMIT}; submitted {req.limit})',
                    http_status=400,
                )

            # 4. Filter resolution (API-07, D-17).
            restrict_sys_ids: Optional[set] = None
            filters_dict: Optional[dict] = None
            short_circuit_empty = False

            if req.filters is not None:
                filters_dict = req.filters.model_dump(exclude_none=True)
                # Concern #3: validate_filter_values raises APIError from
                # shared.api_errors.
                # Late binding via the module attribute so test fixtures can
                # monkeypatch shared.fjms_service.{validate_filter_values,
                # is_valid_domain_token, get_filter_sys_ids}.
                from shared import fjms_service as _fjms_module
                _fjms_module.validate_filter_values(filters_dict)

                restrict_sys_ids = _fjms_module.get_filter_sys_ids(
                    domains=filters_dict.get('domains'),
                    authors=filters_dict.get('authors'),
                    works=filters_dict.get('works'),
                    material_include=filters_dict.get('materials'),
                    date_from=filters_dict.get('date_from'),
                    date_to=filters_dict.get('date_to'),
                )
                if restrict_sys_ids is not None and len(restrict_sys_ids) == 0:
                    short_circuit_empty = True

            # 5. Build responsa_options if Responsa mode.
            responsa_options = None
            if req.mode == 'Responsa':
                responsa_options = {
                    'responsa_mode': True,
                    'variants': True,
                    'ja': True,
                    'flex_spacing': False,
                    'bidirectional': False,
                    'variant_mode': 'variants',
                }

            # 6. Statelessness check (D-20). Forbidden reads — none below.

            # 7. Execute search OR short-circuit on empty intersection.
            if short_circuit_empty:
                results = []
                total = 0
            else:
                results = state.searcher.execute_search(
                    query_str=query,
                    mode=req.mode,
                    gap=req.gap,
                    progress_callback=None,
                    exclude_words=None,
                    responsa_options=responsa_options,
                    restrict_sys_ids=restrict_sys_ids,
                    text_position=None,
                ) or []
                total = len(results)

            # 8. Cap results.
            results = results[:req.limit]
            result_count = len(results)

            # 8a. R2-#1: consume the thread-local downgrade signal here, on
            # the success path. The defensive consume in `finally` below
            # handles the exception path. Reading on success first means we
            # still surface the downgrade warning correctly.
            downgrade_msg = _consume_last_responsa_downgrade()

            # 9. Lift cascade-downgrade warning.
            #    R2-#1 + Concern #6: read the thread-local meta channel FIRST
            #    (resilient to empty results). The OUTER finally below ensures
            #    the thread-local is drained even on the exception path so it
            #    cannot leak into the next request on this worker thread.
            warnings_list: list = []
            if downgrade_msg:
                warnings_list.append(f'query_downgraded: {downgrade_msg}')
            elif results:
                # Legacy fallback path — only fires if thread-local was unset.
                first = results[0]
                rw = first.pop('responsa_warning', None)
                if rw:
                    warnings_list.append(f'query_downgraded: {rw}')
            # Strip any per-row markers regardless so result rows are clean.
            if results:
                results[0].pop('responsa_warning', None)
                results[0].pop('responsa_expanded_count', None)

            # 10. Serialize (D-24).
            from shared.search_serializer import serialize_search_payload
            envelope = serialize_search_payload(
                results,
                meta_mgr=state.meta_mgr,
                query=query,
                mode=req.mode,
                gap=req.gap if req.gap else None,
                filters=filters_dict,
                warnings=warnings_list,
                total=total,
            )
            return envelope

        except APIError as exc:
            status_code = exc.http_status
            error_code = exc.code
            return _build_envelope_response(request, exc)
        except (RequestValidationError, PydanticValidationError) as exc:
            # Concern #2 + #12: per-endpoint Pydantic-error envelope; the
            # finally block fires the PostHog event with these labels.
            status_code = 400
            error_code = 'invalid_request'
            return _build_envelope_response(request, exc)
        except Exception:
            logger.exception('search_endpoint unhandled exception')
            status_code = 500
            error_code = 'internal_error'
            err = APIError('internal_error', 'Internal error', http_status=500)
            return _build_envelope_response(request, err)
        finally:
            # 11. PostHog event capture (HARDEN-05). Once per request, success
            #     or error. Concern #12: this fires for Pydantic-caught errors
            #     too because we caught them in the body and pinned
            #     status_code/error_code above.
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
                logger.warning(
                    'capture_api_event failed in finally block',
                )
            # 12. R2-#1: defensive thread-local drain. If execute_search
            #     raised after setting _LAST_RESPONSA_DOWNGRADE but before
            #     step 8a's consume call, that stale signal must NOT leak to
            #     the next request on this worker thread. Calling consume here
            #     is a no-op when the success path already drained it.
            try:
                _consume_last_responsa_downgrade()
            except Exception:
                logger.warning(
                    'thread-local downgrade drain failed in finally',
                )

    logger.info("Search API routes initialized: POST /api/search")

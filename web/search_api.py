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
import os
import re as _re
import time
from dataclasses import dataclass
from typing import Literal, Optional, List

from nicegui import app
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError, model_validator

from web.state import state
from web.api_hardening import (
    RateLimiter,
    enforce_mode_gate,
    _resolve_rate_limit_key,
    capture_api_event,
    _build_envelope_response,
    wrap_endpoint,  # Phase 79 R-PR-03: now reused for browse_endpoint.
)
# Concern #3: APIError from neutral location.
from shared.api_errors import APIError
# Phase 79 imports.
from shared.browse_service import fetch_browse_bundle
from shared.search_serializer import serialize_browse_payload, serialize_parallels_payload
# Phase 80 imports.
from shared.parallels_service import fetch_parallels_results, ParallelsResultBundle

logger = logging.getLogger(__name__)


# Module-level RateLimiter; Phases 79/80 import this same instance.
_rate_limiter = RateLimiter(default_limit=30)

# Phase 79 D-18: SEPARATE per-IP bucket from /api/search.
# Same SEARCH_API_RATE_LIMIT env-var ceiling -- RateLimiter._current_limit() reads
# the env on every check(). A client doing search-once + browse-N-times does NOT
# exhaust the search bucket. (R-10: aggregate per-IP allowance is roughly 2x the
# ceiling -- captured as monitoring obligation, no contract change in v7.10.)
_browse_rate_limiter = RateLimiter(default_limit=30)

# Phase 80 D-05: SEPARATE per-IP bucket from /api/search and /api/browse.
# Same SEARCH_API_RATE_LIMIT env-var ceiling — RateLimiter._current_limit()
# reads the env on every check(). A client doing search-once + browse-N-times
# + parallels-once does NOT exhaust the search or browse buckets. Independence
# is verified by tests/test_parallels_api.py::test_parallels_rate_limit_independence.
_parallels_rate_limiter = RateLimiter(default_limit=30)


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


class ResponsaOptions(BaseModel):
    """Phase 81A D-02 — Responsa-only options. Field names mirror the desktop
    UI checkboxes exactly (genizah_app.py:15788-15797).

    D-03: extra='forbid' — extended/maximum variant tiers, variant_mode, and
    any other field name are rejected. D-11: variants is a plain bool; the
    internal variant_mode is derived server-side ('variants' if True else 'exact').
    """
    model_config = ConfigDict(extra='forbid')

    variants: bool = False
    ja: bool = False
    flex_spacing: bool = False
    bidirectional: bool = False


class SearchRequest(BaseModel):
    """Phase 81A — UI-aligned search_mode + Responsa-only options.

    Replaces the Phase 78 `mode` field with `search_mode` (D-01, D-13). Old
    `mode` field is hard-rejected by extra='forbid' — see search_endpoint for
    the 400 invalid_request envelope with the explicit `unknown field 'mode'`
    message.

    Validation matrix (81A-CONTEXT.md):
    - Any non-responsa search_mode + non-None responsa_options → 400 invalid_combination
    - search_mode in {'title', 'shelfmark'} + non-zero gap → 400 invalid_combination
    - limit must be in [1, 100] (Pydantic Field constraint → 400 invalid_request via Phase 78 envelope wrapper)
    - regex is intentionally NOT in the enum (D-09; deferred to v7.11)
    """
    model_config = ConfigDict(extra='forbid')

    query: str
    search_mode: Literal['exact', 'variants', 'responsa', 'title', 'shelfmark']
    responsa_options: Optional[ResponsaOptions] = None
    gap: int = 0
    limit: int = Field(default=50, ge=1, le=100)
    filters: Optional[FiltersModel] = None

    @model_validator(mode='after')
    def _check_responsa_options_coupling(self):
        if self.search_mode != 'responsa' and self.responsa_options is not None:
            raise APIError(
                'invalid_combination',
                f"responsa_options is only valid when search_mode='responsa' "
                f"(got search_mode={self.search_mode!r})",
                http_status=400,
            )
        return self

    @model_validator(mode='after')
    def _check_gap_metadata_coupling(self):
        if self.search_mode in ('title', 'shelfmark') and self.gap and self.gap != 0:
            raise APIError(
                'invalid_combination',
                f"gap has no effect with metadata-only search modes "
                f"(search_mode={self.search_mode!r}, gap={self.gap})",
                http_status=400,
            )
        return self


# Constants (D-07, D-08, D-09).
QUERY_LENGTH_CAP = 1000
DEFAULT_LIMIT = 50
MAX_LIMIT = 100  # 81A D-06 — lowered from 200 (also enforced via Pydantic Field(le=100))

# 81A — translate API search_mode → internal mode value space consumed by
# SearchEngine.execute_search (genizah_core.py:7249). For non-Responsa text
# searches, the internal `mode` argument is THE variant-tier knob:
# genizah_core.py:6467 calls var_mgr.get_variants(term, mode, limit=200),
# so 'exact' → no variant expansion, 'variants' → 30-pair variant expansion.
# Mirrors desktop UI semantics (genizah_app.py:15796 toggles 'variants' vs 'exact').
_SEARCH_MODE_TO_INTERNAL = {
    'exact':     'exact',
    'variants':  'variants',
    'responsa':  'Responsa',
    'title':     'Title',
    'shelfmark': 'Shelfmark',
}

# Phase 79 D-11 / R-08 -- transcription char cap.
DEFAULT_BROWSE_TEXT_CAP = 4000
MIN_BROWSE_TEXT_CAP = 100
MAX_BROWSE_TEXT_CAP = 10000

# Phase 80 D-06 — composition text length cap. Composition body can legitimately
# be much longer than a search query (whole piyutim, parts of liturgy). Cap at
# 20000 chars (~3000 Hebrew words) after .strip(). Above cap → 400
# 'composition_too_long'. Empty after .strip() → 400 'composition_required'.
COMPOSITION_LENGTH_CAP = 20000


# ---------------------------------------------------------------------------
# Phase 79 -- BrowseRequest model + NormalizedLocator + locator helpers.
# ---------------------------------------------------------------------------


class BrowseRequest(BaseModel):
    """Phase 79 D-21 -- query-param model for GET /api/browse.

    `sys_id` is REQUIRED (D-01). At least one of (uid, p_num, fl_id) must
    be supplied (D-03). FastAPI does not auto-bind GET query params to a
    Pydantic model when the route handler takes a Request object directly,
    so the handler constructs `BrowseRequest(**dict(request.query_params))`
    inside the body -- the wrap_endpoint decorator catches PydanticValidationError.
    """
    model_config = ConfigDict(extra='forbid')
    sys_id: str
    uid: Optional[str] = None
    p_num: Optional[int] = None
    volume_ie: Optional[str] = None
    fl_id: Optional[str] = None
    text_cap: Optional[int] = None


class ParallelsRequest(BaseModel):
    """Phase 80 D-01 — POST /api/parallels request body.

    Field semantics:
    - `text` is the composition source. Maps to `full_text` arg of
      search_composition_logic. Stripped before length validation; empty
      → 'composition_required', > COMPOSITION_LENGTH_CAP → 'composition_too_long'
      (both 400).
    - `chunk_size`: integer in [2, 20]; UI default is 5.
    - `mode`: locked enum 'exact' | 'variants' | 'fuzzy' (D-02). Lab Engine
      path is OUT OF SCOPE for v7.10.
    - `max_freq`: optional float; when set, chunks whose match frequency
      exceeds the threshold get diverted to filtered[]. When None, no
      high-freq filtering — all hits in results[], filtered: [].
    - `boundary_mode`: only boundary knob exposed in v7.10 (D-03). Other 4
      core knobs use existing defaults.
    - `filters`: reuse Phase 78 FiltersModel verbatim.
    """
    model_config = ConfigDict(extra='forbid')

    text: str
    chunk_size: int = Field(default=5, ge=2, le=20)
    mode: Literal['exact', 'variants', 'fuzzy'] = 'exact'
    max_freq: Optional[float] = None
    boundary_mode: Literal['full', 'boundary', 'combined'] = 'full'
    filters: Optional[FiltersModel] = None


@dataclass(frozen=True)
class NormalizedLocator:
    """Phase 79 R-PR-04 -- output of _validate_locator(req).

    The handler passes `effective_*` fields to fetch_browse_bundle.
    When the request supplied `uid`, the effective fields are derived
    from parsing the uid (IE{N}_P{M}_FL{K}). When uid was absent, the
    effective fields mirror the request fields directly.
    """
    sys_id: str
    requested_uid: Optional[str]            # original uid string, for D-03b post-resolution check
    effective_p_num: Optional[int]
    effective_volume_ie: Optional[str]
    effective_fl_id: Optional[str]
    text_cap: Optional[int]


_UID_PATTERN = _re.compile(r'^(IE\d+)_(P\d+)_(FL\d+)$')


def _parse_uid(uid: str) -> Optional[dict]:
    """Parse uid='IE{N}_P{M}_FL{K}' into components.

    Returns dict with keys 'volume_ie' (str e.g. 'IE12345'), 'p_num' (int,
    1-based) and 'fl_id' (str e.g. 'FL999'), or None if the uid does not
    match the expected shape.
    """
    if not uid or not isinstance(uid, str):
        return None
    m = _UID_PATTERN.match(uid.strip())
    if not m:
        return None
    ie_part, p_part, fl_part = m.group(1), m.group(2), m.group(3)
    try:
        p_num_val = int(p_part[1:])  # strip 'P' prefix
    except ValueError:
        return None
    if p_num_val < 1:
        return None
    return {'volume_ie': ie_part, 'p_num': p_num_val, 'fl_id': fl_part}


def _validate_locator(req: 'BrowseRequest') -> 'NormalizedLocator':
    """D-03 / R-02 / D-03b -- strict locator semantics. R-PR-04 update: returns
    a NormalizedLocator with effective_* fields derived from uid when present.

    Rules:
    - sys_id always required (Pydantic enforces; this is documentation).
    - At least one of (uid, p_num, fl_id) must be supplied. None -> 400.
    - p_num must be int >= 1.
    - text_cap, when supplied, must be int in [MIN_BROWSE_TEXT_CAP, MAX_BROWSE_TEXT_CAP].
    - If uid AND any of (volume_ie, p_num, fl_id) supplied AND parsed
      components disagree -> 400 `locator_conflict`.
    - When uid is supplied: effective_p_num / effective_volume_ie /
      effective_fl_id are derived from parsing the uid.
    - When uid is absent: effective_* fields mirror the request fields.
    """
    if not (req.uid or req.p_num is not None or req.fl_id):
        raise APIError(
            'invalid_request',
            'locator missing: provide uid, p_num, or fl_id alongside sys_id',
            http_status=400,
        )
    if req.p_num is not None and req.p_num < 1:
        raise APIError(
            'invalid_request',
            f'p_num must be >= 1 (got {req.p_num})',
            http_status=400,
        )
    if req.text_cap is not None and not (
        MIN_BROWSE_TEXT_CAP <= req.text_cap <= MAX_BROWSE_TEXT_CAP
    ):
        raise APIError(
            'invalid_request',
            f'text_cap must be in [{MIN_BROWSE_TEXT_CAP}, {MAX_BROWSE_TEXT_CAP}] '
            f'(got {req.text_cap})',
            http_status=400,
        )

    # uid path: parse + check conflicts + derive effective fields.
    if req.uid:
        parsed = _parse_uid(req.uid)
        if parsed is None:
            raise APIError(
                'locator_conflict',
                f'uid is malformed (expected IE{{N}}_P{{M}}_FL{{K}}; got {req.uid!r})',
                http_status=400,
            )
        # R-02 conflict detection.
        if req.volume_ie is not None and req.volume_ie != parsed['volume_ie']:
            raise APIError(
                'locator_conflict',
                f'uid implies volume_ie={parsed["volume_ie"]!r} but request '
                f'supplied volume_ie={req.volume_ie!r}',
                http_status=400,
            )
        if req.p_num is not None and req.p_num != parsed['p_num']:
            raise APIError(
                'locator_conflict',
                f'uid implies p_num={parsed["p_num"]} but request supplied '
                f'p_num={req.p_num}',
                http_status=400,
            )
        if req.fl_id is not None and req.fl_id != parsed['fl_id']:
            raise APIError(
                'locator_conflict',
                f'uid implies fl_id={parsed["fl_id"]!r} but request supplied '
                f'fl_id={req.fl_id!r}',
                http_status=400,
            )
        # Effective fields derived from uid (R-PR-04).
        return NormalizedLocator(
            sys_id=req.sys_id,
            requested_uid=req.uid,
            effective_p_num=parsed['p_num'],
            effective_volume_ie=parsed['volume_ie'],
            effective_fl_id=parsed['fl_id'],
            text_cap=req.text_cap,
        )

    # No uid: effective fields mirror request fields.
    return NormalizedLocator(
        sys_id=req.sys_id,
        requested_uid=None,
        effective_p_num=req.p_num,
        effective_volume_ie=req.volume_ie,
        effective_fl_id=req.fl_id,
        text_cap=req.text_cap,
    )


def _resolve_text_cap(requested: Optional[int]) -> int:
    """R-08 priority: ?text_cap > env > DEFAULT_BROWSE_TEXT_CAP. Caller has
    already validated bounds via _validate_locator.
    """
    if requested is not None:
        return requested
    raw = os.environ.get('SEARCH_API_BROWSE_TEXT_CAP')
    if raw:
        try:
            v = int(raw)
            return max(MIN_BROWSE_TEXT_CAP, min(MAX_BROWSE_TEXT_CAP, v))
        except (ValueError, TypeError):
            pass
    return DEFAULT_BROWSE_TEXT_CAP


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
            except PydanticValidationError as exc:
                # Concern #12: pin status_code/error_code so the finally block
                # fires the PostHog event with correct labels. Re-raise so
                # the outer except branch builds the envelope.
                status_code = 400
                error_code = 'invalid_request'
                # 81A D-13: when the rejected field is the old `mode`, surface the
                # cutover hint explicitly so skill authors copy-pasting old payloads
                # see the migration path.
                for err in exc.errors():
                    if err.get('type') == 'extra_forbidden' and err.get('loc') == ('mode',):
                        raise APIError(
                            'invalid_request',
                            "unknown field 'mode' — use search_mode instead",
                            http_status=400,
                        )
                raise

            validated_mode = req.search_mode

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

            # 5. Build responsa_options if responsa search_mode (per req.responsa_options).
            responsa_options = None
            if req.search_mode == 'responsa':
                # Default: all-False ResponsaOptions when client omitted the field.
                opts = req.responsa_options or ResponsaOptions()
                # D-11: derive internal variant_mode from the boolean flag exactly as
                # the desktop UI does (genizah_app.py:15796).
                responsa_options = {
                    'responsa_mode': True,
                    'variants': opts.variants,
                    'ja': opts.ja,
                    'flex_spacing': opts.flex_spacing,
                    'bidirectional': opts.bidirectional,
                    'variant_mode': 'variants' if opts.variants else 'exact',
                }

            # 6. Statelessness check (D-20). Forbidden reads — none below.

            # 7. Execute search OR short-circuit on empty intersection.
            if short_circuit_empty:
                results = []
                total = 0
            else:
                internal_mode = _SEARCH_MODE_TO_INTERNAL[req.search_mode]
                results = state.searcher.execute_search(
                    query_str=query,
                    mode=internal_mode,
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
            # 81A — drain the structured-meta channel adjacent to the legacy
            # string channel. Both feed the request-echo block below
            # (cascade_meta populates responsa_options_effective when set).
            from genizah_core import _consume_last_responsa_downgrade_meta as _consume_meta
            cascade_meta = _consume_meta()

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

            # 81A D-04/D-05/D-06 — build request echo for the response envelope.
            # search_mode is ECHOED VERBATIM (never silently downgraded — D-04).
            # responsa_options_effective reflects the cascade outcome (e.g.
            # request.ja=true + cascade disable → effective.ja=false). For
            # non-Responsa modes both responsa_options and
            # responsa_options_effective are None (D-05).
            if req.search_mode == 'responsa':
                opts_dict = (req.responsa_options or ResponsaOptions()).model_dump()
                if cascade_meta is not None:
                    # cascade_meta is shaped {variants, ja, flex_spacing, bidirectional}
                    effective_dict = cascade_meta
                else:
                    # No cascade fired — effective == requested.
                    effective_dict = dict(opts_dict)
            else:
                opts_dict = None
                effective_dict = None

            request_echo = {
                'search_mode': req.search_mode,
                'responsa_options': opts_dict,
                'responsa_options_effective': effective_dict,
                'gap': req.gap,
                'limit': req.limit,
                'limit_effective': min(req.limit, MAX_LIMIT),
                'filters': filters_dict,
            }

            # 10. Serialize (D-24).
            from shared.search_serializer import serialize_search_payload
            envelope = serialize_search_payload(
                results,
                meta_mgr=state.meta_mgr,
                query=query,
                mode=_SEARCH_MODE_TO_INTERNAL[req.search_mode],
                gap=req.gap if req.gap else None,
                filters=filters_dict,
                warnings=warnings_list,
                total=total,
                request_echo=request_echo,
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
            # 81A — symmetric defensive drain for the structured-meta channel
            # so a setter without a matching consumer (e.g. exception between
            # set-site and step 8a's consume) cannot leak meta into the next
            # request on this worker thread.
            try:
                from genizah_core import _consume_last_responsa_downgrade_meta as _drain_meta
                _drain_meta()
            except Exception:
                logger.warning(
                    'thread-local downgrade-meta drain failed in finally',
                )

    @target_app.get('/api/browse')
    @wrap_endpoint(endpoint_name='browse')
    async def browse_endpoint(request: Request, *, captured_state: dict):
        """GET /api/browse -- Phase 79 drill-down endpoint.

        R-PR-03: decorated with @wrap_endpoint(endpoint_name='browse') from
        web/api_hardening.py. The decorator owns try/except/finally + envelope
        rewriting + capture_api_event. The handler body holds ONLY business
        logic -- no hand-rolled boilerplate.

        captured_state contract (set by handler, read by decorator's finally):
        - 'mode': None for browse (no mode field)
        - 'result_count': 1 on success, None on error/early-return paths

        Statelessness D-22: handler MUST NOT touch any per-session/refinement
        surfaces (last results, current query, browser-storage user dict, or
        request cookies).
        """
        # Browse has no mode; pin to None so PostHog event has the right shape.
        captured_state['mode'] = None

        # 0. Parse query params + Pydantic validation. The decorator catches
        #    PydanticValidationError; we just construct the model.
        params = dict(request.query_params)
        # Coerce known int fields explicitly -- bad casts surface as APIError
        # 'invalid_request' which the decorator converts to a 400 envelope.
        for k in ('p_num', 'text_cap'):
            if k in params and params[k] != '':
                try:
                    params[k] = int(params[k])
                except ValueError:
                    raise APIError(
                        'invalid_request',
                        f'{k} must be an integer (got {params[k]!r})',
                        http_status=400,
                    )
        req = BrowseRequest(**params)

        # 1. Mode gate (D-04 disabled / D-03 localhost-only -- same as search).
        enforce_mode_gate(request)

        # 2. Rate limit (D-18 -- DIFFERENT instance from search bucket).
        client_ip = _resolve_rate_limit_key(request)
        _browse_rate_limiter.check(client_ip)

        # 3. Locator validation + normalization (D-01 / D-03 / R-02 / R-PR-04).
        loc = _validate_locator(req)

        # 4. Resolve effective text_cap (R-08 priority).
        effective_text_cap = _resolve_text_cap(loc.text_cap)

        # 5. Enrichment fan-out (Plan 02). Pass NORMALIZED fields -- fetch_browse_bundle
        #    does NOT accept uid (R-PR-04). Core fetch is also timed (R-01).
        #    fetch_browse_bundle raises APIError('core_timeout', 504) on
        #    core timeout; otherwise returns a bundle (page may be None).
        bundle, warnings_list = await fetch_browse_bundle(
            sys_id=loc.sys_id,
            p_num=loc.effective_p_num,
            volume_ie=loc.effective_volume_ie,
            fl_id=loc.effective_fl_id,
        )

        # 6. Core resolution failure -> 404 (D-16).
        if bundle.page is None:
            raise APIError(
                'manuscript_page_not_found',
                f'no page for sys_id={loc.sys_id!r} '
                f'p_num={loc.effective_p_num} volume_ie={loc.effective_volume_ie!r} '
                f'fl_id={loc.effective_fl_id!r}',
                http_status=404,
            )

        # 7. R-03 / D-03b -- post-resolution uid verification.
        #    Compare the resolved BrowsePage.uid to the ORIGINAL requested uid
        #    (not the effective fields -- those agree by construction in
        #    _validate_locator). This catches the case where sys_id from
        #    manuscript A is paired with uid from manuscript B.
        if loc.requested_uid:
            resolved_uid = getattr(bundle.page, 'uid', None) or ''
            if resolved_uid and resolved_uid != loc.requested_uid:
                raise APIError(
                    'manuscript_page_not_found',
                    f'uid resolved to different page (requested {loc.requested_uid!r}, '
                    f'resolved {resolved_uid!r}); check sys_id + uid pair',
                    http_status=404,
                )

        # 8. D-04 -- multi-IE without volume_ie defaulted; surface a warning.
        if (
            loc.requested_uid is None
            and loc.effective_fl_id is None
            and loc.effective_volume_ie is None
            and getattr(bundle.page, 'volume_ie', None)
            and len(getattr(bundle.page, 'volumes', []) or []) > 1
        ):
            warnings_list.append({
                'code': 'volume_ie_defaulted',
                'volume_ie': bundle.page.volume_ie,
            })

        # 9. Statelessness check (D-22). Forbidden reads -- none below.
        #    Verified by grep at acceptance time.

        # 10. Serialize via sibling serializer (D-26). R-PR-09: no
        #     locator-echo parameters (those were dropped from the signature).
        envelope = serialize_browse_payload(
            page=bundle.page,
            pgp=bundle.pgp,
            fjms=bundle.fjms,
            nli=bundle.nli,
            text_cap=effective_text_cap,
            warnings=warnings_list,
        )

        # 11. Tell the decorator's finally block what to log to PostHog.
        captured_state['result_count'] = 1

        return envelope

    @target_app.post('/api/parallels')
    @wrap_endpoint(endpoint_name='parallels')
    async def parallels_endpoint(request: Request, *, captured_state: dict):
        """POST /api/parallels — Phase 80 composition/parallels endpoint.

        R-PR-03 precedent (Phase 79): decorated with @wrap_endpoint(endpoint_name='parallels').
        The decorator owns try/except/finally + envelope rewriting + capture_api_event.
        Handler body holds ONLY business logic.

        captured_state contract (set by handler, read by decorator's finally):
        - 'mode': req.mode for the PostHog event (D-09 — parallels-specific
          mode value space; same property key as /api/search disambiguated by
          endpoint='parallels').
        - 'result_count': len(bundle.main_results) on success.

        Statelessness D-20: handler MUST NOT touch state.last_results /
        state.parallels_results / state.current_search_query / app.storage /
        request.cookies.
        """
        # 0. Manual JSON parse so malformed JSON flows through wrap_endpoint
        #    envelope instead of FastAPI's 422 default. FastAPI body injection
        #    is intentionally NOT used here (would bypass envelope shape).
        try:
            body = await request.json()
        except Exception as exc:
            raise APIError(
                'invalid_request',
                f'request body must be valid JSON: {exc}',
                http_status=400,
            )
        if isinstance(body, dict):
            req = ParallelsRequest(**body)
        else:
            req = ParallelsRequest.model_validate(body)

        # PostHog mode property (D-09) — parallels-specific value space.
        captured_state['mode'] = req.mode

        # 1. Mode gate (same as search/browse).
        enforce_mode_gate(request)

        # 2. Rate limit (D-05 — DIFFERENT instance from search and browse buckets).
        client_ip = _resolve_rate_limit_key(request)
        _parallels_rate_limiter.check(client_ip)

        # 3. Composition text validation (D-06).
        text = (req.text or '').strip()
        if not text:
            raise APIError(
                'composition_required',
                'text is required and cannot be empty after stripping whitespace',
                http_status=400,
            )
        if len(text) > COMPOSITION_LENGTH_CAP:
            raise APIError(
                'composition_too_long',
                f'text exceeds cap (max {COMPOSITION_LENGTH_CAP} chars; '
                f'submitted {len(text)})',
                http_status=400,
            )

        # 4. Filter resolution (API-07, D-15/D-17 inherited from Phase 78).
        #    Late-binding via the module attribute so test fixtures can
        #    monkeypatch shared.fjms_service.{validate_filter_values, get_filter_sys_ids}.
        restrict_sys_ids: Optional[set] = None
        filters_dict: Optional[dict] = None
        short_circuit_empty = False

        if req.filters is not None:
            filters_dict = req.filters.model_dump(exclude_none=True)
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

        # 5. Statelessness check (D-20). Forbidden reads — none below.

        # 6. Execute via service layer OR short-circuit on empty intersection.
        warnings_list: list = []
        if short_circuit_empty:
            bundle = ParallelsResultBundle(
                main_results=[],
                filtered_results=[],
                boundary_options={
                    'boundary_mode': req.boundary_mode,
                    'boundary_delimiter': '\n',
                    'boundary_boost': 1.5,
                    'min_boundary_matches': 0,
                    'min_delimiter_distance': 3,
                },
                truncated_to_200=False,
            )
        else:
            bundle = await fetch_parallels_results(
                text=text,
                chunk_size=req.chunk_size,
                mode=req.mode,
                max_freq=req.max_freq,
                boundary_mode=req.boundary_mode,
                restrict_sys_ids=restrict_sys_ids,
            )

        # 7. Surface group-cap warning (D-07).
        if bundle.truncated_to_200:
            warnings_list.append('truncated_to_200')

        # 81A D-07 — request echo for /api/parallels. Field name `mode` is
        # PRESERVED here (NOT renamed to search_mode); the rename is deferred
        # to v7.11. ParallelsRequest at web/search_api.py has no `gap` field
        # and no `responsa_options` (parallels never used Responsa), so the
        # echo has 6 keys: mode, chunk_size, max_freq, boundary_options,
        # limit_effective, filters. limit_effective mirrors the post-truncation
        # group count (D-07: 200-group cap surfaced via warnings_list).
        parallels_echo = {
            'mode': req.mode,
            'chunk_size': req.chunk_size,
            'max_freq': req.max_freq,
            'boundary_options': bundle.boundary_options,
            'limit_effective': len(bundle.main_results),
            'filters': filters_dict,
        }

        # 8. Serialize — Phase 77 D-14 SOLE producer of envelope shape.
        envelope = serialize_parallels_payload(
            bundle.main_results,
            bundle.filtered_results,
            meta_mgr=state.meta_mgr,
            source_text=text,
            chunk_size=req.chunk_size,
            mode=req.mode,
            max_freq=req.max_freq,
            boundary_options=bundle.boundary_options,
            warnings=warnings_list,
            request_echo=parallels_echo,
        )

        # 9. Tell the decorator's finally block what to log to PostHog.
        captured_state['result_count'] = len(bundle.main_results)

        return envelope

    logger.info("Search API routes initialized: POST /api/search, GET /api/browse, POST /api/parallels")

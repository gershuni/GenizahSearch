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

import asyncio
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
from shared.browse_service import fetch_browse_bundle, _read_timeout
# SEED-016 #3: the browse core-fetch provider is injected by the caller (here),
# inverting the former shared/ -> web/ import. web.services is the web layer's
# own dependency, so importing it from web/search_api.py respects layering.
from web.services import get_service
from shared.search_serializer import serialize_browse_payload, serialize_parallels_payload
# Phase 80 imports.
from shared.parallels_service import fetch_parallels_results, ParallelsResultBundle

logger = logging.getLogger(__name__)


# Module-level RateLimiter; Phases 79/80 import this same instance.
_rate_limiter = RateLimiter(default_limit=120)

# Phase 79 D-18: SEPARATE per-IP bucket from /api/search.
# Same SEARCH_API_RATE_LIMIT env-var ceiling -- RateLimiter._current_limit() reads
# the env on every check(). A client doing search-once + browse-N-times does NOT
# exhaust the search bucket. (R-10: aggregate per-IP allowance is roughly 2x the
# ceiling -- captured as monitoring obligation, no contract change in v7.10.)
_browse_rate_limiter = RateLimiter(default_limit=120)

# Phase 80 D-05: SEPARATE per-IP bucket from /api/search and /api/browse.
# Same SEARCH_API_RATE_LIMIT env-var ceiling — RateLimiter._current_limit()
# reads the env on every check(). A client doing search-once + browse-N-times
# + parallels-once does NOT exhaust the search or browse buckets. Independence
# is verified by tests/test_parallels_api.py::test_parallels_rate_limit_independence.
_parallels_rate_limiter = RateLimiter(default_limit=120)


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

    domains: Optional[List[str]] = Field(
        default=None,
        description="FJMS domain labels (e.g. 'Halakha', 'Piyyut'). Unknown values -> 400 unresolvable_filter_value.",
    )
    authors: Optional[List[str]] = Field(
        default=None,
        description="FJMS genizah_persons author names. Unknown values -> 400 unresolvable_filter_value.",
    )
    works: Optional[List[str]] = Field(
        default=None,
        description="FJMS genizah_titles work names. Unknown values -> 400 unresolvable_filter_value.",
    )
    library: Optional[List[str]] = Field(
        default=None,
        description=(
            "Library codes (e.g. 'CUL', 'JTS', 'Oxford'). Inclusion filter — results "
            "are restricted to manuscripts in these libraries, intersected with any "
            "other filters BEFORE the result cap. Unknown codes -> 400 "
            "unresolvable_filter_value. SEED-026."
        ),
    )
    materials: Optional[List[str]] = Field(
        default=None,
        description="Material type (e.g. 'paper', 'parchment'). Unknown values -> 400 unresolvable_filter_value.",
    )
    date_from: Optional[int] = Field(
        default=None,
        description="Inclusive lower bound on manuscript estimated year (CE). Combine with date_to.",
    )
    date_to: Optional[int] = Field(
        default=None,
        description="Inclusive upper bound on manuscript estimated year (CE). Combine with date_from.",
    )


class ResponsaOptions(BaseModel):
    """Phase 81A D-02 — Responsa-only options. Field names mirror the desktop
    UI checkboxes exactly (genizah_app.py:15788-15797).

    D-03: extra='forbid' — extended/maximum variant tiers, variant_mode, and
    any other field name are rejected. D-11: variants is a plain bool; the
    internal variant_mode is derived server-side ('variants' if True else 'exact').
    """
    model_config = ConfigDict(extra='forbid')

    variants: bool = Field(
        default=False,
        description="Enable morphological variant expansion for Responsa mode.",
    )
    ja: bool = Field(
        default=False,
        description="Enable Judeo-Arabic expansion for Responsa mode.",
    )
    flex_spacing: bool = Field(
        default=False,
        description="Allow flexible spacing between terms in Responsa mode.",
    )
    bidirectional: bool = Field(
        default=False,
        description="Enable bidirectional (RTL+LTR) matching in Responsa mode.",
    )


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

    query: str = Field(
        ...,
        description="Search query string. Max 1000 chars after stripping. Empty after strip -> 400 query_required.",
    )
    search_mode: Literal['exact', 'variants', 'responsa', 'title', 'shelfmark', 'fuzzy'] = Field(
        ...,
        description="Search mode: 'exact' (literal), 'variants' (morphological), 'responsa' (Responsa Project style), 'title' (FJMS title metadata), 'shelfmark' (call number lookup), 'fuzzy' (approximate / maximum variant expansion — slowest mode).",
    )
    responsa_options: Optional[ResponsaOptions] = Field(
        default=None,
        description="Responsa expansion flags. Valid only when search_mode='responsa'; rejected otherwise with 400 invalid_combination.",
    )
    gap: int = Field(
        default=0,
        description="Gap between terms (words). Must be 0 when search_mode is 'title' or 'shelfmark'.",
    )
    limit: int = Field(
        default=50,
        ge=1,
        le=2000,  # Hard max — per-mode ceiling enforced in handler (fuzzy: FUZZY_HARD_MAX=2000; others: MAX_LIMIT=100)
        description=(
            "Max results to return. For non-fuzzy modes bounded [1, 100]. "
            "For fuzzy mode, may be up to SEARCH_API_FUZZY_MAX_LIMIT (default 500, "
            "hard max 2000). Default 50 (fuzzy with no explicit limit applies a "
            "wider recall-oriented default)."
        ),
    )
    filters: Optional[FiltersModel] = Field(
        default=None,
        description="Optional domain/author/work/material/date filter. Unknown values fail-closed with 400.",
    )

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
    'fuzzy':     'fuzzy',
}

# Core-search wall-clock timeout (2026-06): execute_search runs in a thread-pool
# worker wrapped in asyncio.wait_for, so a slow query (especially the
# 'fuzzy'/variants_maximum tier) returns a 504 'core_timeout' instead of pinning
# the event loop for its full duration. Re-read per request via _read_timeout so
# prod can flip SEARCH_API_CORE_TIMEOUT without a restart.
DEFAULT_SEARCH_CORE_TIMEOUT = 30.0

# P9X per-mode timeout ladder: heavy modes get their own ceiling env knobs.
# All are re-read per request (via _read_timeout inside helper functions).
DEFAULT_VARIANTS_TIMEOUT = 60.0
DEFAULT_FUZZY_TIMEOUT = 300.0
DEFAULT_PARALLELS_TIMEOUT = 300.0
DEFAULT_HEAVY_CONCURRENCY = 2
HEAVY_SEARCH_MODES = frozenset({'variants', 'fuzzy'})

# P9X fuzzy result-cap ceiling.  MAX_LIMIT stays 100 for non-fuzzy modes.
DEFAULT_FUZZY_MAX_LIMIT = 500
FUZZY_HARD_MAX = 2000      # absolute cap so payload stays sane


def _resolve_search_timeout(search_mode: str) -> tuple:
    """Return (ceiling_seconds, env_var_name) for the given search_mode.

    Re-reads the env on every call (no caching) so prod can flip timeouts
    without a restart. responsa and interactive modes use the baseline knob.
    """
    if search_mode == 'variants':
        return (
            _read_timeout('SEARCH_API_VARIANTS_TIMEOUT', DEFAULT_VARIANTS_TIMEOUT),
            'SEARCH_API_VARIANTS_TIMEOUT',
        )
    if search_mode == 'fuzzy':
        return (
            _read_timeout('SEARCH_API_FUZZY_TIMEOUT', DEFAULT_FUZZY_TIMEOUT),
            'SEARCH_API_FUZZY_TIMEOUT',
        )
    # exact, title, shelfmark, responsa — all use the interactive baseline.
    return (
        _read_timeout('SEARCH_API_CORE_TIMEOUT', DEFAULT_SEARCH_CORE_TIMEOUT),
        'SEARCH_API_CORE_TIMEOUT',
    )


def _resolve_parallels_timeout() -> float:
    """Return the parallels ceiling (re-read per request)."""
    return _read_timeout('SEARCH_API_PARALLELS_TIMEOUT', DEFAULT_PARALLELS_TIMEOUT)


async def _intersect_library_filter(restrict_sys_ids, filters_dict, meta_mgr):
    """SEED-026 (API library filter): if ``filters_dict`` carries a ``library`` list,
    intersect the resolved library sys_id set into ``restrict_sys_ids``.

    ``resolve_library_sys_ids`` iterates the full csv_bank (~255K rows), so it runs
    off the event loop via ``run_in_executor``. Returns the updated restrict set:
      - no library filter -> ``restrict_sys_ids`` unchanged (may be None);
      - library only -> the resolved library sys_id set;
      - library + other filters -> the intersection (which the caller short-circuits
        to 0 results if empty).

    Late-binds ``resolve_library_sys_ids`` through the module attribute so test
    fixtures can monkeypatch ``shared.fjms_service.resolve_library_sys_ids``.
    Library codes are validated upstream by ``validate_filter_values`` (unknown -> 400),
    so by here every code is a known LIBRARY_CODES key.
    """
    libs = (filters_dict or {}).get('library')
    if not libs:
        return restrict_sys_ids
    from shared import fjms_service as _fjms_module
    loop = asyncio.get_running_loop()
    lib_ids = await loop.run_in_executor(
        None, _fjms_module.resolve_library_sys_ids, libs, meta_mgr
    )
    if restrict_sys_ids is None:
        return lib_ids
    return restrict_sys_ids & lib_ids


def _resolve_fuzzy_max_limit() -> int:
    """Return the fuzzy result-cap ceiling (re-read per request, no import caching)."""
    raw = os.environ.get('SEARCH_API_FUZZY_MAX_LIMIT')
    if raw:
        try:
            val = int(raw)
            return max(1, min(val, FUZZY_HARD_MAX))
        except (ValueError, TypeError):
            pass
    return min(DEFAULT_FUZZY_MAX_LIMIT, FUZZY_HARD_MAX)


class _HeavySemaphoreState:
    """Module-level mutable state for the heavy-mode concurrency semaphore.

    asyncio.Semaphore is fixed-size at construction.  We keep the semaphore
    plus a record of its configured capacity so we can rebuild it when the
    env changes AND the semaphore is fully idle (all slots free).

    All accesses are on the event-loop thread (the semaphore is an asyncio
    primitive), so no thread lock is needed around the rebuild.
    """
    sem: asyncio.Semaphore = asyncio.Semaphore(DEFAULT_HEAVY_CONCURRENCY)
    _capacity: int = DEFAULT_HEAVY_CONCURRENCY

    @classmethod
    def reset(cls, capacity: int) -> None:
        """Rebuild the semaphore to the given capacity.  Only safe to call
        when the semaphore is fully idle (tests use this directly)."""
        cls.sem = asyncio.Semaphore(capacity)
        cls._capacity = capacity


async def _acquire_heavy_slot():
    """Acquire one slot from the heavy-mode semaphore.

    Re-reads SEARCH_API_HEAVY_CONCURRENCY on every call; rebuilds the
    semaphore if the configured size changed AND it is currently fully idle.

    Returns:
        A zero-argument callable that releases the slot.  Callers MUST
        invoke this in a ``finally`` block so a timeout/exception cannot
        strand a slot.

    Raises:
        APIError('heavy_search_busy', ..., 503): if no slot is available
            right now (non-blocking acquire failed).
    """
    # Re-read the desired concurrency from env.
    raw = os.environ.get('SEARCH_API_HEAVY_CONCURRENCY')
    desired = DEFAULT_HEAVY_CONCURRENCY
    if raw:
        try:
            desired = max(1, int(raw))
        except (ValueError, TypeError):
            pass

    # Rebuild semaphore only when the size changed AND it is fully idle
    # (no held slots).  If partially held, keep the current semaphore so we
    # never strand held slots.  "Fully idle" means the counter is back at
    # capacity; asyncio exposes no public API for that, so we read the
    # internal counter defensively via getattr — if the attribute is ever
    # renamed/removed, current_value is None, we skip the rebuild, and the
    # gate keeps working at the old capacity (fail-safe, never raises).
    if desired != _HeavySemaphoreState._capacity:
        current_value = getattr(_HeavySemaphoreState.sem, '_value', None)
        if current_value == _HeavySemaphoreState._capacity:
            _HeavySemaphoreState.reset(desired)

    sem = _HeavySemaphoreState.sem
    # Non-blocking acquire via the public API.  On a single-threaded event
    # loop, sem.locked() (True iff no slot is free) followed by sem.acquire()
    # cannot race: when not locked, acquire() decrements the counter and
    # returns WITHOUT awaiting a Future, so the loop never switches between
    # the check and the acquire.  release() in the returned callable keeps the
    # acquire/release pair symmetric (no manual counter manipulation).
    # We do NOT use asyncio.wait_for(sem.acquire(), timeout=0): in Python 3.11
    # that always raises TimeoutError before the coroutine runs a single step,
    # regardless of slot availability.
    if sem.locked():
        raise APIError(
            'heavy_search_busy',
            'heavy search concurrency limit reached; retry shortly',
            http_status=503,
            headers={'Retry-After': '5'},
        )
    await sem.acquire()

    def _release():
        sem.release()

    return _release

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
    sys_id: str = Field(
        ...,
        description="Manuscript system_number (unique ID from /api/search result locator.sys_id).",
    )
    uid: Optional[str] = Field(
        default=None,
        description="Preferred locator: 'IE{N}_P{M}_FL{K}' format from /api/search result uid field. Uniquely resolves page.",
    )
    p_num: Optional[int] = Field(
        default=None,
        ge=1,
        description="1-based page number (alternative to uid). Combine with optional volume_ie.",
    )
    volume_ie: Optional[str] = Field(
        default=None,
        description="IE identifier string (e.g. 'IE12345') for multi-IE manuscripts. Ignored for single-IE manuscripts.",
    )
    fl_id: Optional[str] = Field(
        default=None,
        description="Fragment/leaf identifier (e.g. 'T-S 12.123.1r'). Alternative to p_num.",
    )
    text_cap: Optional[int] = Field(
        default=None,
        description="Override transcription text cap in chars. Bounded [100, 10000]. Default: env SEARCH_API_BROWSE_TEXT_CAP (4000).",
    )


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

    text: str = Field(
        ...,
        description="Composition text to search for parallels. Max 20000 chars after stripping.",
    )
    chunk_size: int = Field(
        default=5,
        ge=2,
        le=20,
        description="Number of words per chunk for sliding-window matching. Default 5.",
    )
    mode: Literal['exact', 'variants', 'fuzzy'] = Field(
        default='exact',
        description="Matching mode for each chunk: 'exact' (literal), 'variants' (morphological), 'fuzzy' (approximate).",
    )
    max_freq: Optional[float] = Field(
        default=None,
        description="High-frequency cutoff ratio (0.0-1.0). Chunks appearing in more than max_freq fraction of corpus are moved to 'filtered'. None disables high-freq filtering.",
    )
    boundary_mode: Literal['full', 'boundary', 'combined'] = Field(
        default='full',
        description="Chunk boundary strategy: 'full' (any position), 'boundary' (text boundary-aligned), 'combined' (prefer boundaries).",
    )
    filters: Optional[FiltersModel] = Field(
        default=None,
        description="Optional domain/author/work/material/date filter (same as /api/search).",
    )


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
# Phase 83 -- OpenAPI metadata helpers (Codex HIGH concern #2).
#
# Background: the route handlers below use FastAPI's raw `Request` argument
# and parse Pydantic models manually inside the body (search_api.py rationale
# at the original lines 444-448). FastAPI therefore cannot infer the request
# body / query params / response schemas from the handler signature -- without
# explicit OpenAPI metadata the spec at /api/openapi.json renders empty
# requestBody objects and Swagger UI displays buttonless endpoints.
#
# Solution per Codex's "typed wrapper" suggestion (Option B): keep handler
# bodies/signatures byte-identical and declare OpenAPI metadata explicitly via
# `openapi_extra=` on each route decorator. The metadata is built from each
# Pydantic model's `model_json_schema()`. This changes the spec only -- not
# the runtime parsing path -- so Phase 78/79/80/81A behavior is preserved.
# ---------------------------------------------------------------------------

def _inline_schema_refs(schema: dict) -> dict:
    """Inline local Pydantic ``$defs`` references for route-level OpenAPI extras.

    FastAPI only hoists schemas into top-level ``components`` when it owns the
    Pydantic body parameter. These endpoints parse raw ``Request`` objects and
    attach request schemas via ``openapi_extra``, so any generated references to
    ``#/components/schemas/...`` would point at a missing top-level section.
    Keeping the requestBody schema self-contained avoids Swagger UI resolver
    errors while preserving the runtime request contract.
    """
    defs = schema.get("$defs") or {}

    def resolve(node):
        if isinstance(node, list):
            return [resolve(item) for item in node]
        if not isinstance(node, dict):
            return node
        ref = node.get("$ref")
        if isinstance(ref, str):
            marker = "#/$defs/"
            if ref.startswith(marker):
                name = ref[len(marker):]
                target = defs.get(name)
                if isinstance(target, dict):
                    merged = {k: v for k, v in node.items() if k != "$ref"}
                    resolved = resolve(target)
                    if merged:
                        resolved = {**resolved, **resolve(merged)}
                    return resolved
        return {
            key: resolve(value)
            for key, value in node.items()
            if key != "$defs"
        }

    return resolve(schema)


def _openapi_request_body(model_cls) -> dict:
    """Build an OpenAPI requestBody object from a Pydantic model.

    Used for POST /search and POST /parallels.
    """
    schema = _inline_schema_refs(
        model_cls.model_json_schema(ref_template="#/$defs/{model}")
    )
    return {
        "required": True,
        "content": {
            "application/json": {"schema": schema},
        },
    }


def _openapi_query_parameters(model_cls) -> list:
    """Build OpenAPI 'parameters' (in: query) list from a Pydantic model.

    Used for GET /browse which takes query params, not a body.
    """
    schema = model_cls.model_json_schema(ref_template="#/components/schemas/{model}")
    props = schema.get("properties", {})
    required = set(schema.get("required", []))
    params = []
    for name, prop_schema in props.items():
        params.append({
            "name": name,
            "in": "query",
            "required": name in required,
            "description": prop_schema.get("description", ""),
            "schema": {k: v for k, v in prop_schema.items() if k != "description"},
        })
    return params


_ENVELOPE_SUCCESS_SCHEMA = {
    "type": "object",
    "properties": {
        "schema_version": {"type": "string", "example": "1.0"},
        "request": {"type": "object", "description": "Echo of validated request input."},
    },
    "required": ["schema_version", "request"],
    "additionalProperties": True,
}
_ENVELOPE_ERROR_SCHEMA = {
    "type": "object",
    "properties": {
        "error": {
            "type": "object",
            "properties": {
                "code": {"type": "string", "description": "ERROR_CODES key (see shared/api_errors.py)."},
                "message": {"type": "string", "description": "Human-readable, sanitized."},
            },
            "required": ["code", "message"],
        },
    },
    "required": ["error"],
}


def _openapi_responses_for(success_summary: str) -> dict:
    return {
        "200": {
            "description": success_summary,
            "content": {"application/json": {"schema": _ENVELOPE_SUCCESS_SCHEMA}},
        },
        "400": {
            "description": "Validation error (e.g. invalid_request, query_required, invalid_combination, unresolvable_filter_value).",
            "content": {"application/json": {"schema": _ENVELOPE_ERROR_SCHEMA}},
        },
        "429": {
            "description": "Rate limit exceeded (rate_limited).",
            "content": {"application/json": {"schema": _ENVELOPE_ERROR_SCHEMA}},
        },
        "503": {
            "description": "Mode-gate disabled or upstream service unavailable.",
            "content": {"application/json": {"schema": _ENVELOPE_ERROR_SCHEMA}},
        },
    }


# ---------------------------------------------------------------------------
# Idempotent registrar (Concern #10 / R2-#2).
# ---------------------------------------------------------------------------

def init_search_api(app_override: Optional[FastAPI] = None, path_prefix: str = '/api') -> None:
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
        path_prefix: Route prefix prepended to '/search', '/browse',
                     '/parallels'. Default '/api' preserves backward compatibility
                     with tests that hit '/api/search' on a bare app. Pass ''
                     when mounting the sub-app at '/api' in production -- the
                     mount provides the prefix.
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

    @target_app.post(
        f'{path_prefix}/search',
        summary="Search Cairo Geniza manuscripts (keyword / Responsa / title / shelfmark).",
        tags=["search"],
        openapi_extra={
            "requestBody": _openapi_request_body(SearchRequest),
            "responses": _openapi_responses_for("Ranked search results envelope."),
        },
    )
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
        # 81A D-08 + Codex MEDIUM-3 — provisional PostHog capture state.
        # Populated from the raw body BEFORE Pydantic construction so
        # `invalid_combination` rejections (raised by @model_validator AFTER
        # search_mode parsed but DURING cross-field validation) retain the
        # offending mode value in telemetry. Overwritten with the validated
        # value after successful Pydantic construction.
        posthog_search_mode_value: Optional[str] = None
        posthog_responsa_options_count: int = 0
        # Phase 85 SYNTH-06 / D-14 — populated AFTER serialization with
        # `any(item['is_synthetic'] for item in envelope['results'])`. None on
        # error paths (pre-serialization rejection) so PostHog event reflects
        # structural unavailability, not a False signal.
        posthog_is_synthetic: Optional[bool] = None

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
            # 81A D-08 + Codex MEDIUM-3 — provisional capture from raw body
            # BEFORE Pydantic construction. Structural rejections (missing
            # field, wrong type, unknown enum value) leave this as None;
            # cross-field rejections preserve it. Gate on the allowed enum
            # set so unknown values (e.g. 'regex', 'NOT_A_MODE') are treated
            # as structurally invalid and stay None per D-08.
            if isinstance(body, dict):
                _raw_search_mode = body.get('search_mode')
                if isinstance(_raw_search_mode, str) and _raw_search_mode in _SEARCH_MODE_TO_INTERNAL:
                    posthog_search_mode_value = _raw_search_mode
                # responsa_options_count stays 0 provisionally — only computed
                # after Pydantic confirms the responsa_options field shape.
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
            # 81A D-08 — overwrite the provisional PostHog values with the
            # validated parse. Identical content for valid `search_mode`
            # strings, but explicit overwrite documents the contract and
            # gives us a single site to compute responsa_options_count from
            # the parsed Pydantic model.
            posthog_search_mode_value = req.search_mode
            if req.search_mode == 'responsa' and req.responsa_options is not None:
                _opts = req.responsa_options
                posthog_responsa_options_count = sum([
                    bool(_opts.variants),
                    bool(_opts.ja),
                    bool(_opts.flex_spacing),
                    bool(_opts.bidirectional),
                ])
            else:
                # responsa_options omitted → defaults all-False → count 0;
                # non-responsa modes → count 0 by definition (D-08).
                posthog_responsa_options_count = 0

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
            # P9X: fuzzy has a separate (higher) ceiling; all other modes keep MAX_LIMIT=100.
            effective_max = (
                _resolve_fuzzy_max_limit() if req.search_mode == 'fuzzy' else MAX_LIMIT
            )
            if req.limit > effective_max:
                raise APIError(
                    'limit_too_high',
                    f'limit exceeds max for search_mode={req.search_mode!r} '
                    f'(max {effective_max}; submitted {req.limit})',
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
                # SEED-026: intersect the library filter BEFORE the result cap.
                restrict_sys_ids = await _intersect_library_filter(
                    restrict_sys_ids, filters_dict, state.meta_mgr
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
            #
            # execute_search runs in a thread-pool worker wrapped in
            # asyncio.wait_for (SEARCH_API_CORE_TIMEOUT) so a slow query — esp.
            # the 'fuzzy'/variants_maximum tier — returns 504 'core_timeout'
            # instead of pinning the event loop for its full duration.
            #
            # R2-#1 / 81A thread-local note: execute_search sets the responsa
            # downgrade signals on the THREAD it runs on. We consume them INSIDE
            # the worker closure (same thread) and hand them back — reading the
            # thread-locals on the event-loop thread after the executor hop would
            # see an empty (wrong-thread) signal.
            downgrade_msg = None
            cascade_meta = None
            if short_circuit_empty:
                results = []
                total = 0
            else:
                internal_mode = _SEARCH_MODE_TO_INTERNAL[req.search_mode]

                def _run_search_sync():
                    res = state.searcher.execute_search(
                        query_str=query,
                        mode=internal_mode,
                        gap=req.gap,
                        progress_callback=None,
                        exclude_words=None,
                        responsa_options=responsa_options,
                        restrict_sys_ids=restrict_sys_ids,
                        text_position=None,
                    ) or []
                    from genizah_core import (
                        _consume_last_responsa_downgrade_meta as _consume_meta_inner,
                    )
                    return res, _consume_last_responsa_downgrade(), _consume_meta_inner()

                # P9X per-mode timeout: variants/fuzzy get heavier ceilings.
                core_timeout, timeout_env = _resolve_search_timeout(req.search_mode)
                loop = asyncio.get_event_loop()

                # P9X heavy-mode concurrency gate (variants/fuzzy only).
                _heavy_release = None
                if req.search_mode in HEAVY_SEARCH_MODES:
                    _heavy_release = await _acquire_heavy_slot()
                try:
                    # The heavy slot must stay held for the WORKER's TRUE
                    # lifetime, not merely until the awaiter returns.
                    # run_in_executor cannot cancel a running thread, so on a
                    # timeout the search keeps occupying a threadpool worker.
                    # We use asyncio.wait (which, unlike wait_for, does NOT
                    # cancel the future on timeout) and release the slot from
                    # the future's done-callback — so the budget is freed only
                    # when the thread actually finishes. Releasing in a plain
                    # finally would recycle the slot while a timed-out search
                    # still runs, re-admitting heavy work past the budget and
                    # defeating the saturation guard (the NLI-hang lesson).
                    _search_fut = loop.run_in_executor(None, _run_search_sync)
                    if _heavy_release is not None:
                        _search_fut.add_done_callback(
                            lambda _f, _r=_heavy_release: _r()
                        )
                        _heavy_release = None  # ownership -> done-callback
                    _done, _pending = await asyncio.wait(
                        {_search_fut}, timeout=core_timeout,
                    )
                    if _search_fut in _pending:
                        logger.warning(
                            'search core_timeout after %ss (search_mode=%s, env=%s)',
                            core_timeout, req.search_mode, timeout_env,
                        )
                        raise APIError(
                            'core_timeout',
                            f'search did not complete within {core_timeout}s '
                            f'(search_mode={req.search_mode}); '
                            f'try a narrower query or a faster search_mode',
                            http_status=504,
                        )
                    results, downgrade_msg, cascade_meta = _search_fut.result()
                finally:
                    # Safety net: only fires if a slot was acquired but never
                    # handed to a done-callback (e.g. executor dispatch failed).
                    if _heavy_release is not None:
                        _heavy_release()
                total = len(results)

            # 8. Cap results.
            # P9X: for fuzzy, if the client did NOT supply `limit` (Pydantic
            # filled the default of 50), widen to a recall-oriented default so
            # an agent that just sets search_mode=fuzzy gets wider recall.
            # Detect "limit not supplied" by checking the raw body dict.
            _fuzzy_recall_default = 250  # wider default when client omits limit
            if (
                req.search_mode == 'fuzzy'
                and isinstance(body, dict)
                and 'limit' not in body
            ):
                effective_limit = min(_resolve_fuzzy_max_limit(), _fuzzy_recall_default)
            else:
                effective_limit = req.limit
            results = results[:effective_limit]
            result_count = len(results)

            # 8a. R2-#1 / 81A: downgrade_msg + cascade_meta were captured inside
            # the executor worker above (where execute_search set the
            # thread-locals); the short-circuit branch leaves them None. The
            # defensive drains in `finally` below still guard the exception path.

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
                # P9X: reflect the actual effective limit applied (fuzzy may have
                # a wider cap, or a widened recall default when limit was omitted).
                'limit_effective': effective_limit if req.search_mode == 'fuzzy' else min(req.limit, MAX_LIMIT),
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
            # Phase 85 SYNTH-06 / D-14 — derive captured_state['is_synthetic']
            # from response items. True iff at least one result row is a
            # synthetic libraries.csv entry (helper module decides). The
            # property is uniformly shaped per Plan 05 (None for parallels;
            # resolved bool for search/browse).
            try:
                _items = envelope.get('results', []) or []
                if _items:
                    posthog_is_synthetic = any(
                        bool(it.get('is_synthetic')) for it in _items
                    )
                else:
                    posthog_is_synthetic = False
            except Exception:
                posthog_is_synthetic = None
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
                    # 81A D-08 — observability of new contract. Provisional
                    # values (captured pre-Pydantic) survive the cross-field
                    # `invalid_combination` rejection path (Codex MEDIUM-3);
                    # structural rejections leave them as None / 0.
                    search_mode_value=posthog_search_mode_value,
                    responsa_options_count=posthog_responsa_options_count,
                    # Phase 85 SYNTH-06 / D-14 — derived from response items
                    # after serialization. None on error paths (no envelope).
                    is_synthetic=posthog_is_synthetic,
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

    @target_app.get(
        f'{path_prefix}/browse',
        summary="Drill down to a single manuscript page (text + metadata + image).",
        tags=["browse"],
        openapi_extra={
            "parameters": _openapi_query_parameters(BrowseRequest),
            "responses": _openapi_responses_for("Manuscript page envelope."),
        },
    )
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
        # 81A D-08 — browse has no search_mode and no responsa options;
        # pin explicitly so PostHog events from /api/browse carry the
        # uniform property shape (search_mode_value=None, count=0).
        captured_state['search_mode_value'] = None
        captured_state['responsa_options_count'] = 0

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
            service=get_service(),  # SEED-016 #3: inject the browse-page provider.
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
        # Phase 85 SYNTH-06 / D-14 — populate is_synthetic from the resolved
        # sys_id (NOT the requested locator: a uid-keyed lookup may resolve to
        # a different sys_id; the resolved page is the authoritative answer).
        from shared.synthetic_sys_id import is_synthetic_sys_id
        captured_state['is_synthetic'] = is_synthetic_sys_id(
            getattr(bundle.page, 'sys_id', None)
        )

        return envelope

    @target_app.post(
        f'{path_prefix}/parallels',
        summary="Find composition parallels via sliding-window chunk matching.",
        tags=["parallels"],
        openapi_extra={
            "requestBody": _openapi_request_body(ParallelsRequest),
            "responses": _openapi_responses_for("Ranked parallel-witness groups envelope."),
        },
    )
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

        Statelessness D-20: handler MUST NOT touch the per-session export
        state (web.export_state) or app.storage / request.cookies -- handlers
        are stateless and respond purely from request body + corpus indexes.
        Historical note: pre-Phase-88, the rule named the AppState singleton
        mirror fields (deleted in Phase 88 STATE-01) which Phase 88 removed;
        the rule now reads against the per-session payload helper surface
        instead.
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
        # 81A D-08 — parallels does NOT consume search_mode / responsa_options;
        # pin explicitly so PostHog events from /api/parallels carry the
        # uniform property shape (search_mode_value=None, count=0).
        captured_state['search_mode_value'] = None
        captured_state['responsa_options_count'] = 0
        # Phase 85 SYNTH-06 / D-14 (REVIEWS-MODE Codex HIGH) —
        # /api/parallels takes `text`, NOT `sys_id`, so there is no canonical
        # seed sys_id to tag with is_synthetic. Synthetic rows have no Tantivy
        # chunks, so they are naturally absent from main_results regardless of
        # the seed text. We INTENTIONALLY leave captured_state['is_synthetic']
        # at its wrap_endpoint default of None for /api/parallels events.
        # Future analytics needing this signal can derive it from the response
        # payload's per-item is_synthetic field (set by shared serializer).

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
            # SEED-026: intersect the library filter BEFORE the result cap (parity
            # with /api/search; otherwise filters.library would be silently ignored).
            restrict_sys_ids = await _intersect_library_filter(
                restrict_sys_ids, filters_dict, state.meta_mgr
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
            # P9X: parallels are always heavy — gate on the concurrency budget
            # and wrap the fetch in a timeout. As with /api/search, the slot is
            # held for the work's TRUE lifetime: asyncio.wait does not cancel
            # the task on timeout, and the slot is released from the task's
            # done-callback, so a timed-out composition keeps its budget until
            # it actually finishes (releasing early would re-admit heavy work
            # past the budget — the NLI-hang lesson). The done-callback is the
            # SOLE releaser; it fires on success, exception, or post-timeout
            # completion, so no finally release is needed.
            parallels_ceiling = _resolve_parallels_timeout()
            _par_release = await _acquire_heavy_slot()
            try:
                _par_task = asyncio.ensure_future(
                    fetch_parallels_results(
                        # SEED-016 #3: inject the SearchEngine + MetadataManager
                        # singletons (was read off web.state inside shared/).
                        searcher=state.searcher,
                        meta_mgr=state.meta_mgr,
                        text=text,
                        chunk_size=req.chunk_size,
                        mode=req.mode,
                        max_freq=req.max_freq,
                        boundary_mode=req.boundary_mode,
                        restrict_sys_ids=restrict_sys_ids,
                    )
                )
                # Hand the slot to the task's done-callback (sole releaser): it
                # fires on success, exception, or post-timeout completion, so the
                # budget is held for the task's TRUE lifetime.
                _par_task.add_done_callback(lambda _t, _r=_par_release: _r())
                _par_release = None  # ownership -> done-callback
                _done, _pending = await asyncio.wait(
                    {_par_task}, timeout=parallels_ceiling,
                )
                if _par_task in _pending:
                    logger.warning(
                        'parallels core_timeout after %ss', parallels_ceiling,
                    )
                    raise APIError(
                        'core_timeout',
                        f'parallels did not complete within {parallels_ceiling}s; '
                        f'try a shorter text or smaller chunk_size',
                        http_status=504,
                    )
                bundle = _par_task.result()
            finally:
                # Safety net: only fires if the slot was acquired but never handed
                # to the done-callback (e.g. ensure_future raised before transfer).
                if _par_release is not None:
                    _par_release()

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

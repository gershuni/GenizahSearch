# -*- coding: utf-8 -*-
"""Phase 79 D-23 -- pure-data enrichment fan-out for /api/browse.

Mirrors web/pages/browse_enrichment.py:load_enrichment but returns data
instead of mutating per-page UI state. Importable from shared/ -- does NOT import
nicegui or any UI module.

Layering contract (SEED-016 #3):
- This module lives in shared/ and MUST NOT import from web/ at runtime. The
  core-fetch path used to do `from web.services import get_service` inline,
  which inverted the layering (shared -> web). The dependency is now INVERTED:
  the caller (web/search_api.py) constructs the browse-page provider and passes
  it INTO fetch_browse_bundle via the `service` parameter. shared/ stays
  framework- and web-agnostic.
- The only `web.*` symbol referenced here is the `BrowsePage` return type, kept
  under `TYPE_CHECKING` so it never runs at import or call time.

Statelessness contract (D-22):
- The injected provider is the process-singleton WebDataService (a structural
  BrowsePageProvider). MUST NOT touch any per-session/refinement state (last
  results, current query, browser storage, request cookies, or any UI-coupled
  state object).

Per-source policy (D-15, D-16, R-01):
- Core fetch is mandatory; timeout -> APIError('core_timeout', http_status=504).
  Previously timeout-exempt; R-01 added the timeout to prevent executor
  starvation under a hung Tantivy reader (R-09).
- Three enrichment sources (PGP, FJMS, NLI) run in parallel via asyncio.gather.
- Per-source timeout -> null slot + structured warning entry; response stays 200.
- Per-source exception -> null slot + 'enrichment_failed' warning; logged via
  logger.exception. Response stays 200.

R-PR-02 (cross-AI review 2026-04-29): _fetch_core uses the injected
BrowsePageProvider (WebDataService in production) so the returned BrowsePage is
fully hydrated with shelfmark/title/library_code/library_name/fl_id/volume_ie/
volumes -- fields Plan 01's serialize_browse_payload reads via attribute access.
Earlier draft called the raw core resolver directly, which returns a minimal
dict from genizah_core.py:8246 missing all metadata; that path was a guaranteed
500-on-success bug.

R-PR-05 (cross-AI review 2026-04-29): per-source helpers (_pgp_sync,
_fjms_sync, _nli_sync) DO NOT contain try/except blocks that swallow
exceptions. _wrap_with_timeout owns ALL warning emission -- both timeout
(enrichment_timeout) and exception (enrichment_failed). Earlier draft hid
real service errors as silent nulls, breaking D-16's partial-failure
visibility contract.

Bounded executor (SEED-016 #29): the enrichment + core fan-out runs sync sidecar
I/O on a MODULE-LEVEL NAMED ThreadPoolExecutor (`_BROWSE_EXECUTOR`), not the
event loop's default executor. asyncio.wait_for() cancels the awaiting coroutine
but NOT the underlying executor thread doing sync sidecar I/O, so a genuinely
hung sidecar still occupies its worker thread until it returns. To stop one hung
source from starving the others (and from saturating the default loop executor
shared with the rest of the app), this module:
  - owns a dedicated, named, size-bounded pool (`BROWSE_EXECUTOR_MAX_WORKERS`,
    env-overridable via SEARCH_API_BROWSE_EXECUTOR_WORKERS), and
  - caps the number of concurrently-dispatched sync fetches via an
    asyncio.Semaphore (`BROWSE_SOURCE_CONCURRENCY`), so the pool can never be
    fully consumed by a single browse request's fan-out.
A timed-out source returns its worker to the pool only when the underlying sync
call actually finishes; the semaphore slot is likewise held for the work's true
lifetime (released in a finally), so a hung source cannot re-admit more work
past the cap. `shutdown_browse_executor()` provides deterministic teardown for
tests and process exit.
"""
from __future__ import annotations

import asyncio
import atexit
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional, Protocol, TYPE_CHECKING

from shared.api_errors import APIError

if TYPE_CHECKING:
    from web.services import BrowsePage

logger = logging.getLogger(__name__)


# Defaults read on every call so production env-flips take effect without restart.
DEFAULT_BROWSE_TIMEOUT = 1.0       # D-17, R-01: lowered 2.0 -> 1.0
DEFAULT_BROWSE_CORE_TIMEOUT = 2.0  # D-17 NEW per R-01
# 260902: the FIRST core resolution in a fresh process pays the cold browse-map
# load (18 s measured on the dev box: 133 MB pickle + repair scan; the Tantivy
# text fetch itself is 30 ms). A request arriving in that window used to burn the
# 2 s budget and 504 -- and asyncio's cancel does not stop the loading thread, so
# the next requests queued behind the lock 504'd too, one after another, until
# the map was in memory. While the provider reports it is not warm yet, the core
# budget is widened to this value so those requests WAIT for the one-off load.
DEFAULT_BROWSE_CORE_WARMUP_TIMEOUT = 45.0

def _read_int_env(env_var: str, default: int, *, minimum: int = 1) -> int:
    """Read an int env var at import; fall back to `default` on missing/malformed.

    Never raises on a garbage value (e.g. SEARCH_API_BROWSE_EXECUTOR_WORKERS=abc)
    -- a bad env var must not crash module import."""
    raw = os.environ.get(env_var)
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except (ValueError, TypeError):
        return max(minimum, default)


# SEED-016 #29: bounded named executor sizing. Read once at module import for the
# pool (a ThreadPoolExecutor cannot be resized live); the per-request source
# concurrency cap is derived from the same default. Keep workers modest -- one
# browse request fans out to at most core(1) + 3 enrichment sources, and the
# concurrency cap (below) is what actually shields the pool.
BROWSE_EXECUTOR_MAX_WORKERS = _read_int_env('SEARCH_API_BROWSE_EXECUTOR_WORKERS', 8)
# Max sync fetches dispatched concurrently across ALL in-flight browse requests.
# Bounds blast radius so a single request's fan-out (or a stampede) cannot
# consume the whole pool. Defaults below the pool size so headroom always exists.
BROWSE_SOURCE_CONCURRENCY = max(1, BROWSE_EXECUTOR_MAX_WORKERS - 1)


class BrowsePageProvider(Protocol):
    """Structural type for the injected core-fetch provider (SEED-016 #3).

    WebDataService (web/services.py) satisfies this without importing it here.
    Only the two methods _fetch_core needs are declared.
    """

    def get_browse_page(
        self, sys_id: str, *, p_num: Optional[int] = ..., volume_ie: Optional[str] = ...,
    ) -> Optional['BrowsePage']:
        ...

    def get_browse_page_by_fl(
        self, fl_id: str, *, sys_id: Optional[str] = ...,
    ) -> Optional['BrowsePage']:
        ...


# ---------------------------------------------------------------------------
# Module-level named bounded executor (SEED-016 #29)
# ---------------------------------------------------------------------------

_executor_lock = threading.Lock()
_BROWSE_EXECUTOR: Optional[ThreadPoolExecutor] = None
# asyncio.Semaphore is bound to a running loop; build it lazily on first use so
# import-time has no event loop dependency. Guarded by _executor_lock.
_BROWSE_SEMAPHORE: Optional[asyncio.Semaphore] = None


def _get_browse_executor() -> ThreadPoolExecutor:
    """Return the process-wide named browse executor, creating it on first use."""
    global _BROWSE_EXECUTOR
    if _BROWSE_EXECUTOR is None:
        with _executor_lock:
            if _BROWSE_EXECUTOR is None:
                _BROWSE_EXECUTOR = ThreadPoolExecutor(
                    max_workers=BROWSE_EXECUTOR_MAX_WORKERS,
                    thread_name_prefix='browse-enrich',
                )
    return _BROWSE_EXECUTOR


def _get_browse_semaphore() -> asyncio.Semaphore:
    """Return the per-process source-concurrency semaphore, building it lazily.

    asyncio.Semaphore binds to the loop that constructs it; in production a
    single loop runs the server so one semaphore is correct. Tests using
    asyncio.run() get a fresh loop each call -- reset_browse_executor_state()
    drops the cached semaphore so the next loop rebuilds it.
    """
    global _BROWSE_SEMAPHORE
    if _BROWSE_SEMAPHORE is None:
        with _executor_lock:
            if _BROWSE_SEMAPHORE is None:
                _BROWSE_SEMAPHORE = asyncio.Semaphore(BROWSE_SOURCE_CONCURRENCY)
    return _BROWSE_SEMAPHORE


def shutdown_browse_executor(wait: bool = False) -> None:
    """Shut the named browse executor down (tests / process exit).

    Idempotent. After shutdown the next _get_browse_executor() rebuilds a fresh
    pool, so this is safe to call between test cases.
    """
    global _BROWSE_EXECUTOR, _BROWSE_SEMAPHORE
    with _executor_lock:
        ex = _BROWSE_EXECUTOR
        _BROWSE_EXECUTOR = None
        _BROWSE_SEMAPHORE = None
    if ex is not None:
        ex.shutdown(wait=wait)


def reset_browse_executor_state() -> None:
    """Drop the cached semaphore (and pool) WITHOUT waiting -- test helper.

    Use between asyncio.run() calls so a stale loop-bound semaphore is not
    reused on a fresh loop.
    """
    shutdown_browse_executor(wait=False)


# Deterministic teardown at interpreter exit so the named pool's threads do not
# linger (and so test runners that import this module get a clean shutdown).
atexit.register(shutdown_browse_executor)


def _read_timeout(env_var: str, default: float) -> float:
    """Read float timeout from env on every call; fall back on parse error."""
    raw = os.environ.get(env_var)
    if not raw:
        return default
    try:
        return float(raw)
    except (ValueError, TypeError):
        return default


async def _run_sync(func, *args):
    """Run blocking sync work on the NAMED bounded browse executor (SEED-016 #29).

    Replaces the previous run_in_executor(None, ...) which used the event loop's
    default (unbounded-ish, app-shared) ThreadPoolExecutor.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_get_browse_executor(), func, *args)


def _submit_sync(func, *args):
    """Submit blocking sync work to the named bounded pool; return the underlying
    concurrent.futures.Future.

    SEED-016 #29 fix: callers can hang a done-callback on the returned future so
    a resource (e.g. the source-concurrency semaphore slot) is released only when
    the EXECUTOR THREAD actually finishes -- not merely when an asyncio.wait_for
    on the awaiting coroutine times out. asyncio.wait_for cancels the await; it
    cannot stop a thread already running in the pool.
    """
    return _get_browse_executor().submit(func, *args)


def _provider_is_warm(service) -> bool:
    """True unless the provider says its browse map is still cold.

    Providers MAY expose ``is_warm() -> bool`` (web.services.GenizahService does:
    it reports whether the engine's shared browse map is in memory). A provider
    without the method, or one whose probe raises, is treated as warm so the
    normal budget applies -- this hook must never make a request SLOWER to fail
    than before except in the one cold-start case it exists for.
    """
    probe = getattr(service, 'is_warm', None)
    if probe is None or not callable(probe):
        return True
    try:
        return bool(probe())
    except Exception:
        return True


# ---------------------------------------------------------------------------
# Core fetch (R-01: now timed; R-PR-02: uses injected provider for hydration)
# ---------------------------------------------------------------------------

async def _fetch_core(
    service: BrowsePageProvider,
    sys_id: str,
    p_num: Optional[int],
    volume_ie: Optional[str],
    fl_id: Optional[str],
    timeout: float,
) -> Optional['BrowsePage']:
    """Resolve core BrowsePage via the injected provider (R-PR-02 / SEED-016 #3).

    Returns the hydrated `web.services.BrowsePage` dataclass (with shelfmark,
    title, library_code, library_name, fl_id, volume_ie, volumes populated)
    OR None when the resolver cannot pin a page.

    Timeout -> APIError('core_timeout', 504). None return propagates so the
    handler can map to 404 'manuscript_page_not_found'.
    """

    def _sync():
        if fl_id:
            return service.get_browse_page_by_fl(fl_id, sys_id=sys_id)
        return service.get_browse_page(
            sys_id, p_num=p_num, volume_ie=volume_ie,
        )

    if not _provider_is_warm(service):
        warm_budget = _read_timeout(
            'SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT', DEFAULT_BROWSE_CORE_WARMUP_TIMEOUT)
        if warm_budget > timeout:
            logger.info(
                'browse core not warm yet (browse map still loading); widening the core '
                'budget %.1fs -> %.1fs for sys_id=%s', timeout, warm_budget, sys_id)
            timeout = warm_budget
    try:
        result = await asyncio.wait_for(_run_sync(_sync), timeout=timeout)
    except asyncio.TimeoutError:
        # An expected, handled condition (mapped to a 504 below): one WARNING
        # line, not a traceback. The stack here is always the same two frames
        # of asyncio.wait_for and carries no diagnostic value; the identifiers
        # and the budget are what a reader needs.
        logger.warning(
            'core_timeout sys_id=%s p_num=%s volume_ie=%s fl_id=%s (budget %.1fs; '
            'SEARCH_API_BROWSE_CORE_TIMEOUT raises it)',
            sys_id, p_num, volume_ie, fl_id, timeout,
        )
        raise APIError(
            'core_timeout',
            f'core resolver did not return within {timeout}s',
            http_status=504,
        )

    # The provider returns a hydrated BrowsePage dataclass instance OR None.
    # No attribute-access shim wrapping -- Plan 01's serializer reads
    # dataclass attributes directly off the BrowsePage.
    return result


@dataclass
class BrowseEnrichmentBundle:
    """Pure-data result of /api/browse enrichment fan-out.

    `page` is a hydrated `web.services.BrowsePage` dataclass instance with
    shelfmark/title/library_code/library_name/fl_id/volume_ie/volumes
    populated by the provider's get_browse_page (R-PR-02). It is the
    dataclass directly -- not a dict and not an attribute-access shim.
    """
    page: Optional['BrowsePage']  # web.services.BrowsePage | None
    pgp: Optional[dict]   # Plus key 'page_section_text' for serializer D-10.
    fjms: Optional[dict]  # {source_names, has_measurements, has_visual_suggestions}
    nli: Optional[dict]   # {physical_metadata, folio}


# ---------------------------------------------------------------------------
# Per-source enrichment helpers (sync bodies invoked via _run_sync)
#
# R-PR-05: NO inner try/except. Exceptions propagate to _wrap_with_timeout
# which converts them to enrichment_failed warnings. Earlier draft swallowed
# exceptions and returned None silently -- broke D-16 visibility.
# ---------------------------------------------------------------------------

def _pgp_sync(sys_id: str, p_num: int) -> Optional[dict]:
    """Fetch PGP doc + page-section transcription. Returns shaped dict or None."""
    from shared.document_service import (
        get_document_for_fragment, get_section_for_page,
    )
    doc = get_document_for_fragment(sys_id, p_num)
    if not doc:
        return None
    page_section_text: Optional[str] = None
    transcription = doc.get('transcription')
    if transcription:
        page_section_text = get_section_for_page(
            transcription, p_num,
            fragment_page_info=doc.get('_fragment_page_info'),
        )
    return {
        'description':           doc.get('description'),
        'tags':                  list(doc.get('tags') or []),
        'document_type':         doc.get('document_type'),
        'languages_primary':     list(doc.get('languages_primary') or []),
        'languages_secondary':   list(doc.get('languages_secondary') or []),
        'doc_date_original':     doc.get('doc_date_original'),
        'doc_date_standard':     doc.get('doc_date_standard'),
        'inferred_date_display': doc.get('inferred_date_display'),
        'pgpid':                 doc.get('pgpid'),
        'pgp_url':               doc.get('pgp_url'),
        'page_section_text':     page_section_text,
    }


def _fjms_sync(sys_id: str) -> Optional[dict]:
    """Fetch slim FJMS subset (D-08): source_names, has_measurements, has_visual_suggestions."""
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    if not fjms or not fjms.is_available():
        return None
    source_names = fjms.get_source_names(sys_id) or []
    has_measurements = bool(fjms.has_measurements(sys_id))
    has_visual_suggestions = False
    # VS is best-effort and SHOULD let exceptions propagate to the wrapper if
    # it fails; the import / availability check is cheap enough to not warrant
    # a separate timeout guard.
    from shared.visual_similarity_service import get_vs_service
    vs_svc = get_vs_service(thread_safe=True)
    if vs_svc and vs_svc.is_available():
        has_visual_suggestions = vs_svc.get_suggestion_count(sys_id) > 0
    return {
        'source_names':           list(source_names),
        'has_measurements':       has_measurements,
        'has_visual_suggestions': has_visual_suggestions,
    }


def _nli_sync(sys_id: str, p_num: int, fl_id: Optional[str] = None) -> Optional[dict]:
    """Fetch slim NLI crossref subset (D-09): physical_metadata + active-page folio.

    Active folio resolution prefers `fl_id` match (R-05 -- safer for multi-IE
    manuscripts) and falls back to `folio_images[p_num - 1]` index when fl_id
    is not provided or doesn't match.
    """
    from shared.nli_crossref_service import get_nli_crossref_service
    svc = get_nli_crossref_service(thread_safe=True)
    if not svc or not svc.is_available() or not sys_id:
        return None
    crossref_data = svc.get_crossref_metadata(sys_id) or {}
    folio_images = svc.get_folio_images(sys_id) or []
    active_folio = None
    # Prefer fl_id match (R-05).
    if fl_id and folio_images:
        for f in folio_images:
            if isinstance(f, dict) and f.get('fl_id') == fl_id:
                active_folio = {
                    'fl_id':       f.get('fl_id'),
                    'folio_label': f.get('folio_label', '') or '',
                    'thumb_url':   f.get('thumb_url'),
                }
                break
    # Fall back to p_num index.
    if active_folio is None and folio_images and isinstance(p_num, int) and 0 < p_num <= len(folio_images):
        f = folio_images[p_num - 1] or {}
        active_folio = {
            'fl_id':       f.get('fl_id'),
            'folio_label': f.get('folio_label', '') or '',
            'thumb_url':   f.get('thumb_url'),
        }
    return {
        'physical_metadata': crossref_data.get('physical_metadata'),
        'folio':             active_folio,
    }


# ---------------------------------------------------------------------------
# Per-source timeout wrapper (D-16) -- R-PR-05: SOLE owner of warning emission.
# ---------------------------------------------------------------------------

async def _wrap_with_timeout(
    sync_func, args: tuple, source_name: str,
    timeout: float, warnings_list: list,
) -> Optional[Any]:
    """Run sync_func(*args) on the bounded executor under a timeout + concurrency
    cap. On timeout/exception, append a structured warning to warnings_list and
    return None.

    SEED-016 #29: the dispatch is gated by the module-level source-concurrency
    semaphore so a single browse request's fan-out cannot consume the whole
    named pool.

    SEED-016 #29 FIX (Codex REQUEST-CHANGES): the semaphore slot is held until the
    underlying EXECUTOR FUTURE actually completes -- NOT merely until the awaiting
    coroutine's asyncio.wait_for times out. `wait_for` only cancels the await; it
    cannot stop a thread already running in the pool. Releasing the slot on
    timeout would free a concurrency slot while the worker thread is still busy,
    re-admitting work past the cap and defeating the saturation guard. So we:
      - acquire the slot,
      - submit the blocking work and get its concurrent.futures.Future,
      - release the slot from the FUTURE's done-callback (fires exactly once when
        the thread truly finishes; scheduled back onto the loop because
        asyncio.Semaphore is not thread-safe),
      - keep asyncio.wait_for as the CALLER-facing timeout (the coroutine still
        returns promptly on timeout; the slot just stays occupied until the real
        work returns).

    R-PR-05: this is the SOLE place that catches inner sync-helper failures.
    `_pgp_sync` / `_fjms_sync` / `_nli_sync` no longer have inner try/except
    blocks -- they let exceptions propagate so this wrapper can emit
    'enrichment_failed' warnings (instead of silently nulling out the source).
    """
    loop = asyncio.get_event_loop()
    sem = _get_browse_semaphore()

    # SEED-016 #29 FIX-2 (GitHub Codex P1): bound the slot wait itself. Because the
    # slot is held for the worker thread's TRUE lifetime (see below), a plain
    # unbounded `sem.acquire()` here would block this task OUTSIDE any timeout once
    # every slot is occupied by timed-out-but-still-running sidecar calls -- turning
    # a sidecar hang into a stuck /api/browse (the exact saturation this guard is
    # meant to prevent). Cap the wait at the per-source timeout; on expiry skip the
    # source with an enrichment_timeout warning and WITHOUT submitting work, so no
    # slot is consumed. asyncio.Semaphore.acquire releases correctly if cancelled
    # after acquisition (CPython 3.10+). Worst case per source is ~2x timeout
    # (slot wait + work wait) under full saturation -- bounded, degraded, not hung.
    try:
        await asyncio.wait_for(sem.acquire(), timeout=timeout)
    except asyncio.TimeoutError:
        warnings_list.append({'code': 'enrichment_timeout', 'source': source_name})
        return None

    # Release the slot from the executor future's completion path so it is held
    # for the work's TRUE lifetime, even if the caller-facing wait_for below
    # times out and cancels the await. add_done_callback fires exactly once.
    released = False

    def _release_once() -> None:
        nonlocal released
        if not released:
            released = True
            sem.release()

    def _on_future_done(_f) -> None:
        # Runs on the worker thread once the blocking call truly finishes.
        # asyncio.Semaphore is not thread-safe, so hop back to the loop. If the
        # loop is already closed (e.g. a timed-out source's thread finishing
        # after the request's loop tore down -- only happens in short-lived test
        # loops; production runs a single long-lived loop), there is nothing left
        # to release into, so swallow the closed-loop error.
        try:
            loop.call_soon_threadsafe(_release_once)
        except RuntimeError:
            pass

    future = _submit_sync(sync_func, *args)
    future.add_done_callback(_on_future_done)

    try:
        # wrap_future binds the concurrent future to this loop; cancelling the
        # await on timeout does NOT cancel the running thread (so the slot stays
        # held until the thread finishes and the done-callback releases it).
        return await asyncio.wait_for(asyncio.wrap_future(future), timeout=timeout)
    except asyncio.TimeoutError:
        warnings_list.append({'code': 'enrichment_timeout', 'source': source_name})
        return None
    except Exception:
        logger.exception('enrichment source %s failed', source_name)
        warnings_list.append({'code': 'enrichment_failed', 'source': source_name})
        return None


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------

async def fetch_browse_bundle(
    *,
    service: BrowsePageProvider,
    sys_id: str,
    p_num: Optional[int] = None,
    volume_ie: Optional[str] = None,
    fl_id: Optional[str] = None,
) -> tuple[BrowseEnrichmentBundle, list]:
    """Fan out: core + PGP + FJMS + NLI. Pure-data return.

    SEED-016 #3: `service` is an injected BrowsePageProvider (WebDataService in
    production). This module no longer imports web.services -- the caller owns
    the dependency. This keeps shared/ from importing web/.

    R-PR-04: signature does NOT accept a `uid` parameter. Plan 03's
    `_validate_locator` parses uid into effective `{p_num, volume_ie, fl_id}`
    BEFORE calling this function. The handler is the sole owner of uid
    parsing.

    R-PR-09 note: `text_cap` is NOT a parameter here either -- the serializer
    applies the cap at envelope-build time, not at fetch time.

    SEED-016 #29 note: sync sidecar I/O runs on the named, bounded
    _BROWSE_EXECUTOR with a source-concurrency cap. asyncio.wait_for cancels the
    await but NOT the executor thread; the bounded pool + cap stop one hung
    source from starving the others.
    """
    warnings_list: list = []

    timeout = _read_timeout('SEARCH_API_BROWSE_TIMEOUT', DEFAULT_BROWSE_TIMEOUT)
    core_timeout = _read_timeout('SEARCH_API_BROWSE_CORE_TIMEOUT', DEFAULT_BROWSE_CORE_TIMEOUT)

    # 1. Core fetch -- mandatory + timed. Returns hydrated BrowsePage or None.
    page = await _fetch_core(service, sys_id, p_num, volume_ie, fl_id, core_timeout)
    if page is None:
        return BrowseEnrichmentBundle(None, None, None, None), warnings_list

    # Resolve canonical sys_id, p_num, fl_id from the BrowsePage (handles
    # fl_id-only requests where the resolver figures out the active page).
    resolved_sys_id = getattr(page, 'sys_id', None) or sys_id
    resolved_p_num = getattr(page, 'p_num', None) or p_num or 1
    resolved_fl_id = getattr(page, 'fl_id', None) or fl_id

    # 2. Three enrichment sources in parallel; per-source timeout + warnings.
    pgp_task  = _wrap_with_timeout(_pgp_sync,  (resolved_sys_id, resolved_p_num),                    'pgp',  timeout, warnings_list)
    fjms_task = _wrap_with_timeout(_fjms_sync, (resolved_sys_id,),                                   'fjms', timeout, warnings_list)
    nli_task  = _wrap_with_timeout(_nli_sync,  (resolved_sys_id, resolved_p_num, resolved_fl_id),    'nli',  timeout, warnings_list)

    pgp, fjms, nli = await asyncio.gather(pgp_task, fjms_task, nli_task)

    return BrowseEnrichmentBundle(page=page, pgp=pgp, fjms=fjms, nli=nli), warnings_list

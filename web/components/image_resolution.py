# -*- coding: utf-8 -*-
"""
Per-provider image URL resolution and external-image enrichment helpers.

Extracted from web/pages/browse.py (Phase 117 Plan 03) to enable the anchor pane
(Plan 06) and Phase 119 Compare to achieve full /browse image parity without
re-implementing the provider logic.

## HIGH-1 Input Shape

Both helpers consume fields from the RICH ``web.services.BrowsePage`` dataclass
(``web/services.py:89``), NOT the narrow ``SearchEngine.get_browse_page()``
text/nav dict from ``genizah_core.py:9954`` (which lacks
``shelfmark`` / ``is_oxford`` / ``library_code`` / ``cambridge_images`` /
``external_provider`` / ``cambridge_alignment``).

Callers MUST source inputs from ``web.services.service.get_browse_page(...)``
(returns a rich ``BrowsePage``) and MUST call ``resolve_external_images(sys_id)``
(inside ``run.io_bound``) BEFORE ``resolve_image_url`` to populate the
cambridge_images / external_provider / cambridge_alignment fields — those three
fields are NOT set by ``service.get_browse_page()`` alone (new-HIGH round-2 fix).

## ANC-02 / HIGH-2 Boundary

``resolve_image_url`` returns ONLY ``/api/<provider>_image*`` proxy URLs for NLI
fetches. It does NOT construct any direct ``iiif.nli.org.il`` URL and does NOT
carry the ``NLI_IIIF_BASE`` constant (that constant lives in browse.py's separate
direct-img-tag handleImageError path and must NEVER migrate here).

Oxford's direct Bodleian URL (``get_oxford_direct_image_url``) is the documented
intentional exception: Bodleian is a STATIC, non-NLI, non-IIIF-breaker host and
the existing /browse behaviour (MEDIUM-5 in 117-REVIEWS.md). The NLI Phase-98
circuit breaker does not cover Bodleian; only the NLI path is hard-required to
stay on the server-side proxy.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from shared.cudl_identity import RESOLVER_REVISION as CUDL_RESOLVER_REVISION

from web.services import get_oxford_direct_image_url
from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)


def _warn_if_on_event_loop(func_name: str) -> None:
    """Warn (never raise) if ``func_name`` is running on the asyncio event loop.

    Audit #38: ``resolve_external_images`` does synchronous network I/O via
    ``enrich_metadata`` and must run inside ``run.io_bound`` (an executor
    thread), never directly on the NiceGUI/asyncio event loop where it would
    block every other request. Detecting a *running* loop on the current thread
    means we are NOT on an io_bound worker. This is a soft guard: it logs a
    warning so the misuse is observable but does not change behaviour.
    """
    import asyncio
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop on this thread → we are on a worker thread (or sync
        # context). This is the correct place to call. Nothing to warn about.
        return
    logger.warning(
        "%s called on the event-loop thread — it performs blocking network I/O "
        "and must be invoked inside run.io_bound to avoid stalling the UI.",
        func_name,
        stack_info=True,
    )


def _cudl_page_is_nli(sys_id: str, page_idx: int, images: List[Dict]) -> bool:
    """Whether /api/cambridge_image serves NLI's image for this page (web/api.py: the page's (folio, side) has no
    CUDL canvas, answered with 200, so the browser's error fallback never fires). Its credit is then NLI's
    (Codex on #116). An unreadable sidecar is the route's positional case: CUDL's."""
    try:
        from shared.nli_crossref_service import resolve_cambridge_canvas_for_page
        resolved = resolve_cambridge_canvas_for_page(sys_id, page_idx, images or [])
    except Exception:
        return False
    if resolved is None:
        return True
    if resolved.get('degraded'):
        return False
    idx = resolved.get('canvas_index')
    return idx is None or not (0 <= idx < len(images or []))


def resolve_image_url(
    *,
    sys_id: str,
    p_num: int,
    fl_id: Optional[str] = None,
    catalogue_folios: Optional[List[Dict]] = None,
    is_oxford: bool = False,
    shelfmark: str = '',
    oxford_image_url: Optional[str] = None,
    volume_suffix: int = 1,
    cambridge_images: Optional[List[Dict]] = None,
    external_provider: Optional[str] = None,
    cambridge_alignment: Optional[Dict] = None,
    volumes: Optional[List[Dict]] = None,
    total_pages: int = 0,
    active_source: str = 'nli',
    source_user_override: bool = False,
    width: Optional[int] = None,
    surface: str = 'viewer',
    nli_circuit_open: Optional[bool] = None,
) -> Dict[str, Any]:
    """Resolve the image URL for a browse page using the per-provider proxy strategy.

    INPUT SHAPE — HIGH-1:
        All keyword args correspond 1:1 to fields on ``web.services.BrowsePage``
        (``web/services.py:89``). They MUST be sourced from a ``BrowsePage`` returned
        by ``web.services.service.get_browse_page()``, NOT from the narrow
        ``WebSearchExecutor.get_browse_page()`` Protocol dict (which lacks the
        is_oxford / shelfmark / cambridge_images / external_provider /
        cambridge_alignment fields — so all provider branches except NLI default
        would resolve wrong).

    IMPORTANT (new-HIGH round-2):
        ``cambridge_images``, ``external_provider``, and ``cambridge_alignment`` are
        NOT populated by ``service.get_browse_page()`` alone — they default empty
        on the BrowsePage dataclass. Callers MUST call ``resolve_external_images(sys_id)``
        inside ``run.io_bound`` and pass its return values as the respective kwargs.

    Returns:
        dict with keys:
          ``img_url``       — the resolved proxy URL string (empty string if no image)
          ``has_image``     — True when an image is expected at this URL
          ``active_source`` — the (possibly auto-defaulted) active source so callers
                              can persist it (e.g. state.active_source = result['active_source'])
          ``has_oxford_image``, ``has_katz_image`` — source available for this page

    SEED-010 (source-first resolution):
        The final ``active_source`` is decided BEFORE any URL is built, then a single
        URL is constructed from it.  This fixes the Oxford ordering bug where the
        function could return ``active_source='oxford'`` paired with an NLI URL
        (the old override block had no Oxford branch).

        ``width``            — when set, append ``&width=N`` to /api/* proxy URLs
                               (thumbnails pass 300).  The direct Bodleian URL is
                               unaffected (it carries its own sizing).
        ``surface``          — 'viewer' (default) or 'thumbnail'.  Thumbnails prefer
                               the Cambridge proxy whenever CUDL metadata exists (the
                               proxy keeps its own NLI fallback when NLI is up).
        ``nli_circuit_open`` — Phase-98 NLI breaker state.  When None it is read from
                               ``shared.nli_circuit_breaker.is_open()``.  When True
                               (NLI down) Cambridge auto-defaults even without an
                               'aligned' verdict so CUDL still resolves.
    """
    cambridge_images = cambridge_images or []
    external_provider = external_provider or ''
    volumes = volumes or []

    # Page index (0-based) for multi-page manuscripts
    page_idx = max(0, p_num - 1)

    # Cache-buster suffix for API URLs to force image refresh on folio change
    cache_bust_value = f"_cb={p_num}" if p_num else ""
    cache_bust_api = f"&{cache_bust_value}" if cache_bust_value else ""
    cache_bust_direct = f"?{cache_bust_value}" if cache_bust_value else ""

    has_image = False
    img_url = ''

    # ---- External source availability flags (computed BEFORE any URL) ----
    # SEED-010: these MUST precede source selection + URL construction so the
    # final active_source is decided first and exactly one URL is built from it.
    # The old interleaved order built the NLI URL, THEN flipped active_source to
    # 'oxford' with no Oxford branch to rewrite the URL -> NLI URL leaked through.
    _has_ext_images = bool(cambridge_images)
    _has_cambridge_images = _has_ext_images and not is_oxford and external_provider not in ('manchester', 'jts', 'katz')
    _has_manchester_images = _has_ext_images and external_provider == 'manchester'
    _has_jts_images = _has_ext_images and external_provider == 'jts'
    # Penn scans are paired with NLI's individual image IDs. A transcription's
    # page number can differ from its position in the library's image sequence.
    # Only catalogue-only pages are numbered by scans. An indexed page
    # without an image ID must never borrow the scan at that ordinal.
    katz_idx = None
    if external_provider == 'katz' and not fl_id and catalogue_folios is not None:
        from shared.nli_crossref_service import katz_catalogue_page_fl
        fl_id = katz_catalogue_page_fl(cambridge_images, catalogue_folios, p_num)
    if external_provider == 'katz' and fl_id:
        current_fl = str(fl_id).removeprefix('FL')
        katz_idx = next((i for i, img in enumerate(cambridge_images)
                         if img.get('fl_id') == current_fl), None)
    if active_source == 'katz' and katz_idx is None:
        active_source = 'nli'
    # Browse supplies the exact side (empty when unavailable). Callers that
    # omit it retain the historical shelfmark/proxy path.
    _has_oxford_images = bool(is_oxford and sys_id and oxford_image_url != '')
    if active_source == 'oxford' and not _has_oxford_images:
        active_source = 'nli'
    _is_synth = is_synthetic_sys_id(sys_id)

    # ---- NLI circuit-breaker state (SEED-010) ----
    # When NLI's image API is down (Phase-98 breaker OPEN) we relax the Cambridge
    # verdict gate so CUDL still resolves. Read lazily when not passed by caller.
    if nli_circuit_open is None:
        try:
            from shared import nli_circuit_breaker
            nli_circuit_open = nli_circuit_breaker.is_open()
        except Exception:
            nli_circuit_open = False

    # ---- Decide the FINAL active_source first (no URL built yet) ----
    # Preserves the historical auto-default priority (synthetic-cambridge, jts,
    # manchester, oxford, cambridge-safe) — each guarded on active_source=='nli'
    # so the first applicable provider wins.
    # Phase 85 D-08: synthetic sys_ids with a CUDL manifest default to Cambridge
    # (no NLI attempted at all).
    if _is_synth and _has_cambridge_images and active_source == 'nli' and not source_user_override:
        active_source = 'cambridge'
    if _has_jts_images and active_source == 'nli' and not source_user_override:
        active_source = 'jts'
    if katz_idx is not None and active_source == 'nli' and not source_user_override:
        active_source = 'katz'
    if _has_manchester_images and active_source == 'nli' and not source_user_override:
        active_source = 'manchester'
    if _has_oxford_images and active_source == 'nli' and not source_user_override:
        active_source = 'oxford'
    # 260419-cfx / 260421-aln: auto-default to Cambridge CUDL when the per-position
    # (folio,side) verdict from classify_cambridge_alignment says 'aligned' (or the
    # legacy count-match). A 'misaligned'/missing verdict normally keeps NLI as
    # default — BUT SEED-010: when NLI is DOWN (breaker open) we default to CUDL
    # regardless (a possibly-misaligned CUDL image beats no image), and for
    # THUMBNAILS we always prefer the CUDL proxy when it exists (the proxy keeps
    # its own NLI fallback when NLI is up). User can still switch via the toggle.
    _cam_verdict = (cambridge_alignment or {}).get('verdict') if cambridge_alignment else None
    _cam_safe_default = (
        _has_cambridge_images
        and (
            _cam_verdict == 'aligned'
            or (
                _cam_verdict is None
                and total_pages
                and len(cambridge_images) == total_pages
            )
        )
    )
    _cam_should_default = _has_cambridge_images and (
        _cam_safe_default or nli_circuit_open or surface == 'thumbnail'
    )
    if _cam_should_default and active_source == 'nli' and not source_user_override:
        active_source = 'cambridge'

    # ---- Build EXACTLY ONE URL from the final active_source ----
    nli_stand_in = False  # the Cambridge proxy serves NLI's image for this page (its credit NLI's)
    # Width applies to the /api/* proxy URLs (thumbnails pass width=300); the
    # direct Bodleian URL carries its own sizing so width is NOT appended there.
    _width_api = f"&width={int(width)}" if width else ''

    if active_source == 'oxford' and _has_oxford_images:
        has_image = True
        # For multi-IE Oxford manuscripts, each volume = next folio in sequence
        # (d.50/19 Vol 1 = folio 19, Vol 2 = folio 20). Oxford folios have 2 sides
        # (recto 'a' + verso 'b'), so offset by preceding volumes, not pages.
        _ox_folio_offset = max(0, (volume_suffix or 1) - 1)
        # MEDIUM-5 documented exception: prefer the direct Bodleian URL in the
        # browser to avoid production /api proxy failures. Bodleian is a STATIC,
        # non-NLI host with no circuit breaker. The ANC-02 NLI-proxy restriction
        # applies only to NLI/iiif.nli.org.il paths, NOT to Bodleian.
        oxford_direct = oxford_image_url or get_oxford_direct_image_url(shelfmark, page_idx, folio_offset=_ox_folio_offset)
        if oxford_direct:
            img_url = f"{oxford_direct}{cache_bust_direct}"
        else:
            img_url = f"/api/oxford_image/{sys_id}?page={page_idx}{cache_bust_api}{_width_api}"
    elif active_source == 'cambridge' and _has_cambridge_images and not is_oxford:
        has_image = True
        # cv=: browsers keep these a week (IMAGE_HTTP_MAX_AGE) and never ask again in
        # that time, so a new Cambridge decision needs a new URL (cudl_identity).
        img_url = (f"/api/cambridge_image/{sys_id}?page={page_idx}{cache_bust_api}{_width_api}"
                   f"&cv={CUDL_RESOLVER_REVISION}")
        # The viewers' credit follows the image the proxy will serve; not asked per thumbnail.
        if surface == 'viewer':
            nli_stand_in = _cudl_page_is_nli(sys_id, page_idx, cambridge_images)
    elif active_source == 'manchester' and _has_manchester_images and not is_oxford:
        has_image = True
        # For multi-IE manuscripts, Manchester canvases span all volumes
        # sequentially. Compute absolute canvas index by adding preceding
        # volumes' transcription page counts as offset.
        _manch_page_idx = page_idx
        if volume_suffix and volume_suffix > 1 and volumes:
            _vol_offset = 0
            for v in volumes:
                if v.get('suffix', 1) < volume_suffix:
                    _vol_offset += v.get('transcription_pages', 0)
            _manch_page_idx = page_idx + _vol_offset
        img_url = f"/api/manchester_image/{sys_id}?page={_manch_page_idx}{cache_bust_api}{_width_api}"
    elif active_source == 'jts' and _has_jts_images and not is_oxford:
        has_image = True
        img_url = f"/api/jts_image/{sys_id}?page={page_idx}{cache_bust_api}{_width_api}"
    elif active_source == 'katz' and katz_idx is not None:
        has_image = True
        img_url = f"/api/katz_image/{sys_id}?page={katz_idx}&fl={current_fl}{_width_api}"
    elif active_source == 'nli' and katz_idx is not None:
        has_image = True
        img_url = f"/api/nli_image/{current_fl}"
    elif sys_id:
        # NLI default. ANC-02 / HIGH-2: ONLY /api/nli_image_by_sysid — never a
        # direct iiif.nli.org.il URL. Phase 85 D-06/D-08: synthetic sys_ids have
        # no NLI Alma record, so with no external image they render no <img>.
        if _is_synth:
            has_image = False
            img_url = ''
        else:
            has_image = True
            _suffix_param = f'&suffix={volume_suffix}' if volume_suffix > 1 else ''
            img_url = f"/api/nli_image_by_sysid/{sys_id}?page={page_idx}{_suffix_param}{cache_bust_api}{_width_api}"

    return {
        'img_url': img_url,
        'has_image': has_image,
        'active_source': active_source,
        'nli_stand_in': nli_stand_in,
        'has_oxford_image': _has_oxford_images,
        'has_katz_image': katz_idx is not None,
        'katz_fl_id': current_fl if katz_idx is not None else None,
    }


#: The library image proxies that take ``width=`` (web/api.py). Not /api/nli_image/<fl>,
#: which always answers at 2000 px: a "preview" of it would be a second full download.
_PREVIEW_ROUTE_RE = re.compile(
    r'^/api/(?:nli_image_by_sysid|cambridge_image|manchester_image|jts_image|katz_image'
    r'|oxford_image)/'
)
PREVIEW_WIDTH = 400


def preview_url(img_url: str, width: int = PREVIEW_WIDTH) -> Optional[str]:
    """The small preview of a library proxy image, or None when the URL has none.

    Browse draws it first and swaps in the full image when that arrives
    (manuscript_viewer.js ``progressiveLoad``); Quick View does the same. Only NLI's
    route had one on Browse until 2026-11-22, so a Cambridge page waited for its
    2000-px image, 3-4 s from CUDL, before showing anything.
    """
    if not img_url or not _PREVIEW_ROUTE_RE.match(img_url):
        return None
    if re.search(r'[?&]width=', img_url):
        return None
    sep = '&' if '?' in img_url else '?'
    return f"{img_url}{sep}width={int(width)}"


def _external_images_of(cached: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'cambridge_images': cached.get('images_ext', []),
        'folio_images': cached.get('folio_images') or [],
        'external_provider': cached.get('external_provider', ''),
        'cambridge_alignment': cached.get('cambridge_alignment'),
        'cambridge_resolution': cached.get('cambridge_resolution'),
        'external_url': cached.get('external_url', '') or '',
    }


def cached_external_images(sys_id: str, meta_mgr: Any = None) -> Optional[Dict[str, Any]]:
    """``resolve_external_images``'s answer when it is already in memory, else None.

    Never enriches, so it never touches the network (``shared.cudl_identity.
    cached_external_entry``): Browse's Phase A uses it to draw a Cambridge, Manchester,
    JTS or Katz page with its library's image from the first draw. A page turn used to
    draw NLI's image, throw it away when Phase B found the library's, and fetch that one
    after (2026-11-22, IMAGE_LOADING_SPEED_PLAN in the private repository). None when
    Phase B would have to enrich first: the page is then drawn as before.
    """
    if not sys_id:
        return None
    if meta_mgr is None:
        from web.state import state as _state
        meta_mgr = _state.meta_mgr
    if not hasattr(meta_mgr, 'nli_cache'):
        return None
    from shared.cudl_identity import cached_external_entry
    try:
        cached = cached_external_entry(meta_mgr, sys_id)
    except Exception:  # an aid to the first draw: never stop the page
        logger.debug("cached_external_images failed for %s", sys_id, exc_info=True)
        return None
    return _external_images_of(cached) if cached else None


def image_credits(entry: Dict[str, Any], library_code: str, is_oxford: bool,
                  with_katz: bool = True, own_images: bool = True) -> tuple:
    """(credit, NLI credit) for a page's image, from the manuscript's cache entry.

    The one rule for both of Browse's draws: Phase B's enrichment
    (web/pages/browse_enrichment.py) and Phase A when it draws a library image from
    memory (``apply_cached_external_images``). The manifest's credit, a library's own
    where it has one (``web.services.ATTRIBUTION_BY_LIBRARY``), the Bodleian's for
    Oxford; a Katz record's own; Cambridge's and JTS's images, from their own servers,
    their library's name alone. The NLI credit is the catalogue's (the library and
    Ktiv), else the NLI manifest's (shared/metadata_manager.py: the rule both apps keep).
    ``with_katz=False``: Phase B applies the Katz credit itself, after its enrichment,
    from the entry that enrichment answered with. ``own_images=False``: the library's
    images are not in hand (Phase B's first read, before its enrichment says whether there
    are any; or none came back), so NLI's image is the one shown: its credit (a library's
    fixed one, BL's say, still wins).
    """
    from shared.metadata_manager import nli_image_credit, own_server_image_credit
    from web.services import ATTRIBUTION_BY_LIBRARY, _get_library_attribution
    from web.translations import get_language
    entry = entry or {}
    nli_attribution = nli_image_credit(entry)
    if own_images:
        attribution = (own_server_image_credit(entry, library_code=library_code, lang=get_language())
                       or entry.get('attribution', '') or '')
    else:
        attribution = nli_attribution
    if library_code in ATTRIBUTION_BY_LIBRARY:
        lib_attr = _get_library_attribution(library_code)
        if lib_attr is not None:
            attribution = lib_attr
    elif is_oxford:
        attribution = _get_library_attribution('Oxford') or ''
    if with_katz and entry.get('external_provider') == 'katz' and entry.get('attribution'):
        attribution = entry['attribution']
    return attribution, nli_attribution


def apply_cached_external_images(page: Any, meta_mgr: Any = None) -> bool:
    """Put the manuscript's in-memory external images on a freshly fetched ``page``
    (a ``web.services.BrowsePage``) before its first draw; True when it did.

    Only the manuscript-level fields Phase B would set from the same cache entry
    (images, provider, alignment) and the credit that goes with them
    (``image_credits``: a Cambridge image under the NLI credit until Phase B came was
    Codex's finding on #96); page-specific data (a Katz page's FL id, an Oxford side,
    folio labels) is the page's own. Phase B sets them all again.
    """
    if page is None or getattr(page, 'cambridge_images', None):
        return False
    sys_id = getattr(page, 'sys_id', '') or ''
    if not sys_id:
        return False
    if meta_mgr is None:
        from web.state import state as _state
        meta_mgr = _state.meta_mgr
    if not hasattr(meta_mgr, 'nli_cache'):
        return False
    from shared.cudl_identity import cached_external_entry
    try:
        entry = cached_external_entry(meta_mgr, sys_id)
        if not entry:
            return False
        snapshot = _external_images_of(entry)
        credit, nli_credit = image_credits(entry, getattr(page, 'library_code', '') or '',
                                           bool(getattr(page, 'is_oxford', False)))
    except Exception:  # an aid to the first draw: never stop the page
        logger.debug("apply_cached_external_images failed for %s", sys_id, exc_info=True)
        return False
    page.cambridge_images = snapshot['cambridge_images']
    page.external_provider = snapshot['external_provider']
    page.cambridge_alignment = snapshot['cambridge_alignment']
    if credit:
        page.attribution = credit
    if nli_credit:
        page.attribution_nli = nli_credit
    return True


def resolve_external_images(
    sys_id: str,
    meta_mgr: Any = None,
) -> Dict[str, Any]:
    """Fetch external-provider image metadata for a manuscript (new-HIGH round-2 fix).

    Extracted from ``web/pages/browse_enrichment.py:240-253`` (the nli_cache read +
    enrich_metadata fallback block).  Both ``/browse``'s enrichment path AND the
    Plan 06 AnchorViewer call THIS single helper — one source of truth (D-10).

    IMPORTANT — I/O contract:
        This function calls ``meta_mgr.enrich_metadata(sys_id)`` when the nli_cache
        is empty, which performs network I/O to the NLI crossref sidecar / IIIF
        endpoints.  ``enrich_metadata`` is Phase-98-NLI-breaker-guarded in
        ``genizah_core.py``.  Callers MUST invoke this function inside
        ``run.io_bound`` (never on the NiceGUI event loop).

    Args:
        sys_id:   The manuscript system-number to enrich.
        meta_mgr: Optional MetadataManager instance.  When None, falls back to
                  ``web.state.state.meta_mgr`` (lazy import to avoid a circular
                  dependency between web.components and web.state at module load).

    Returns:
        dict with keys:
          ``cambridge_images``    — list of IIIF canvas URLs (empty list if none)
          ``external_provider``   — the library providing the canvases, including 'katz'
          ``cambridge_alignment`` — CUDL↔NLI alignment verdict dict or None
          ``cambridge_resolution`` — the record's Cambridge decision (shared/cudl_identity.py)
          ``external_url``        — the external library link
        All five come from ONE cache entry: the decision vouches for exactly these images.

        On enrich_metadata failure (any Exception), returns the same shape with
        all-empty values — degrades gracefully, mirrors browse_enrichment.py:249-250.
    """
    # Audit #38: the io_bound requirement (docstring above) was comment-only.
    # Add a runtime guard that warns when this is mistakenly invoked on the
    # asyncio event loop thread (i.e. NOT inside run.io_bound), where the
    # synchronous enrich_metadata network call would block the whole UI. This
    # only warns — it never raises — so existing callers that already wrap the
    # call in run.io_bound (anchor_viewer.py, joins_lab.py) are unaffected.
    _warn_if_on_event_loop("resolve_external_images")

    if meta_mgr is None:
        from web.state import state as _state
        meta_mgr = _state.meta_mgr

    if not sys_id or not hasattr(meta_mgr, 'nli_cache'):
        return {
            'cambridge_images': [],
            'folio_images': [],
            'external_provider': '',
            'cambridge_alignment': None,
            'cambridge_resolution': None,
            'external_url': '',
        }

    # nli_cache not yet populated -- or populated before the record's current Cambridge
    # decision, so its images may be another item's: fresh_external_entry enriches it
    # (Manchester/Cambridge/JTS images from crossref sidecar + IIIF) and hands back the
    # one entry everything below is read from. enrich_metadata is Phase-98-NLI-breaker-
    # guarded; failure degrades to empty image fields (no raise), mirroring
    # browse_enrichment.py:249-250.
    from shared.cudl_identity import fresh_external_entry
    try:
        cached = fresh_external_entry(meta_mgr, sys_id)
    except Exception as e:
        # Audit #37: include stack info so the failing call path is
        # diagnosable (the bare warning lost the traceback).
        logger.warning(
            "resolve_external_images enrich_metadata error for %s: %s",
            sys_id, e, exc_info=True,
        )
        cached = {}

    return _external_images_of(cached)

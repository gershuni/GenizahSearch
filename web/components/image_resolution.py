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
from typing import Any, Dict, List, Optional

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


def resolve_image_url(
    *,
    sys_id: str,
    p_num: int,
    is_oxford: bool = False,
    shelfmark: str = '',
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
    _has_cambridge_images = _has_ext_images and external_provider not in ('manchester', 'jts')
    _has_manchester_images = _has_ext_images and external_provider == 'manchester'
    _has_jts_images = _has_ext_images and external_provider == 'jts'
    # Oxford manuscripts always have a Bodleian direct/proxy path — treat
    # it as an external source like Cambridge/JTS so the user can switch
    # between it and the NLI IIIF view of the same manuscript.
    _has_oxford_images = bool(is_oxford and sys_id)
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
        oxford_direct = get_oxford_direct_image_url(shelfmark, page_idx, folio_offset=_ox_folio_offset)
        if oxford_direct:
            img_url = f"{oxford_direct}{cache_bust_direct}"
        else:
            img_url = f"/api/oxford_image/{sys_id}?page={page_idx}{cache_bust_api}{_width_api}"
    elif active_source == 'cambridge' and _has_cambridge_images and not is_oxford:
        has_image = True
        img_url = f"/api/cambridge_image/{sys_id}?page={page_idx}{cache_bust_api}{_width_api}"
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
    }


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
          ``external_provider``   — 'manchester', 'jts', '' (Cambridge), or ''
          ``cambridge_alignment`` — CUDL↔NLI alignment verdict dict or None

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
            'external_provider': '',
            'cambridge_alignment': None,
        }

    cached = getattr(meta_mgr, 'nli_cache', {}).get(sys_id, {})
    if not cached.get('images_ext'):
        # nli_cache not yet populated — call enrich_metadata to resolve
        # Manchester/Cambridge/JTS images from crossref sidecar + IIIF.
        # enrich_metadata is Phase-98-NLI-breaker-guarded; failure degrades
        # to empty image fields (no raise), mirroring browse_enrichment.py:249-250.
        try:
            meta_mgr.enrich_metadata(sys_id)
            cached = meta_mgr.nli_cache.get(sys_id, {})
        except Exception as e:
            # Audit #37: include stack info so the failing call path is
            # diagnosable (the bare warning lost the traceback).
            logger.warning(
                "resolve_external_images enrich_metadata error for %s: %s",
                sys_id, e, exc_info=True,
            )
            cached = {}

    return {
        'cambridge_images': cached.get('images_ext', []),
        'external_provider': cached.get('external_provider', ''),
        'cambridge_alignment': cached.get('cambridge_alignment'),
    }

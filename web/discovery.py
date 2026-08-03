# -*- coding: utf-8 -*-
"""Web composition for the Discovery Data Spine (Phase 134, DATA-06).

Wires a module-level ``shared.discovery_service.DiscoveryService`` instance
to the LIVE ``web.discovery_assets.discovery_available`` callable and the
LAZY path/version providers (``discovery_db_path`` / `discovery_sidecar_version`),
each read at CALL time (never captured at import) -- so importing this
module BEFORE ``load_discovery_state()`` has run still resolves correctly
once the sidecar loads (import-before-load safe; the service's own lazy
``_get_conn()`` means no DB is ever opened at import time either).

NO route/page/nav is added in this phase (134-06) -- Phase 135+ surfaces
will call the thin async pass-throughs below from an actual UI. Every
pass-through fails OPEN to an empty/None result (never lets an uncaught
``DiscoveryUnavailable`` escape) whenever discovery is unavailable or a
query times out -- T-134-failopen.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple

import scripts.discovery_ids as _ids
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_service import DiscoveryService
from shared.discovery_surface_projection import (
    busy_envelope,
    timeout_envelope,
    unavailable_envelope,
)
from web.discovery_assets import (
    discovery_available,
    discovery_db_path,
    discovery_sidecar_version,
)
from web.feature_flags import DISCOVERY_PUBLIC_RELEASED

logger = logging.getLogger(__name__)

# Module-level singleton, wired to LIVE/LAZY providers -- never captured at
# import time (mirrors web/atlas_assets.py's own lazy-provider convention).
# Constructing this does NOT touch the sidecar DB (DiscoveryService.__init__
# never builds a connection -- F15); it is safe to import this module before
# web.discovery_assets.load_discovery_state() has run.
_service = DiscoveryService(
    path_provider=discovery_db_path,
    availability_callable=discovery_available,
    sidecar_version_provider=discovery_sidecar_version,
)


async def get_version() -> Optional[str]:
    """The currently-loaded sidecar version, or None when discovery is
    unavailable."""
    if not discovery_available():
        return None
    try:
        return await _service.get_version_async()
    except DiscoveryUnavailable:
        logger.info("discovery.get_version: temporarily unavailable")
        return None


async def get_claims_for_page(
    page_id: str, *, page: int = 1, page_size: Optional[int] = None
) -> List[Dict[str, Any]]:
    """PANEL-01/02 pass-through: the manuscript's banded claims on this
    page. Fails open to ``[]`` when discovery is unavailable or a query
    times out."""
    if not discovery_available():
        return []
    try:
        return await _service.get_claims_for_page_async(page_id, page=page, page_size=page_size)
    except DiscoveryUnavailable:
        logger.info("discovery.get_claims_for_page: temporarily unavailable for page_id=%s", page_id)
        return []


async def get_claims_for_page_enveloped(
    page_id: str, *, page: int = 1, page_size: Optional[int] = None,
    include_review: bool = False, lang: str = "en",
) -> Dict[str, Any]:
    """PANEL-01/02, the ENVELOPED shape (D-13, plan 136-14).

    Prefer this over `get_claims_for_page` above on any surface that decides
    whether to RENDER: the list-returning wrapper collapses a timeout, an
    overload, an absent sidecar and a genuine zero all into `[]`, so the panel's
    hide-on-zero rule would hide the panel during an outage exactly as though
    the manuscript had nothing on it. Only ~17% of manuscripts carry shipped
    claims, so hiding on a zero is right -- which is precisely why the zero has
    to be a TRUE zero.

    Still fails open (never raises); the failure is simply *named* now.
    """
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_claims_for_page_enveloped_async(
            page_id, page=page, page_size=page_size,
            include_review=include_review, lang=lang,
        )
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        logger.info("discovery.get_claims_for_page_enveloped: busy for page_id=%s", page_id)
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        logger.info("discovery.get_claims_for_page_enveloped: timeout for page_id=%s", page_id)
        return timeout_envelope(meta={"reason": "query_timeout"})


async def get_pages_related_to_page(
    page_id: str, *, page: int = 1, page_size: Optional[int] = None
) -> List[Dict[str, Any]]:
    """PANEL-02 pass-through: shared_text alignments touching this page."""
    if not discovery_available():
        return []
    try:
        return await _service.get_pages_related_to_page_async(page_id, page=page, page_size=page_size)
    except DiscoveryUnavailable:
        logger.info(
            "discovery.get_pages_related_to_page: temporarily unavailable for page_id=%s", page_id
        )
        return []


async def get_evidence(
    claim_id: str, *, page: int = 1, page_size: Optional[int] = None
) -> List[Dict[str, Any]]:
    """PANEL-03 pass-through: every evidence row for a claim, for on-demand
    expansion."""
    if not discovery_available():
        return []
    try:
        return await _service.get_evidence_async(claim_id, page=page, page_size=page_size)
    except DiscoveryUnavailable:
        logger.info("discovery.get_evidence: temporarily unavailable for claim_id=%s", claim_id)
        return []


async def get_work_witnesses(
    work_id: str,
    enabled_bands: Optional[Iterable[str]] = None,
    *,
    page: int = 1,
    page_size: Optional[int] = None,
    anchor_sys_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """DATA-10 unit x work projection pass-through: witnesses of
    ``work_id``, one row per physical-MS witness_unit at its highest member
    band, the anchor's own unit excluded, same-unit members suppressed."""
    if not discovery_available():
        return []
    try:
        return await _service.get_work_witnesses_async(
            work_id,
            enabled_bands,
            page=page,
            page_size=page_size,
            anchor_sys_id=anchor_sys_id,
        )
    except DiscoveryUnavailable:
        logger.info("discovery.get_work_witnesses: temporarily unavailable for work_id=%s", work_id)
        return []


# ---------------------------------------------------------------------------
# BAND-05 methods-page readers (Phase 135, plan 135-02). The SUPPORTED public
# wrappers the /help "Confidence Bands & Methods" section reads its per-band
# numbers through -- all fail OPEN (None / {}) exactly like the pass-throughs
# above, so a flag-ON / sidecar-absent (or query-timeout) window renders the
# section's placeholders rather than crashing the Help page (T-135-02-03).
# ---------------------------------------------------------------------------


async def get_band_precision(
    evidence_source: Optional[str], confidence_band: Optional[str]
) -> Optional[Dict[str, Any]]:
    """BAND-02 pass-through: the ``scope='band'`` ``band_precision`` row for a
    (evidence_source, confidence_band) pair, or None when
    absent/unavailable."""
    if not discovery_available():
        return None
    try:
        return await _service.get_band_precision_async(evidence_source, confidence_band)
    except DiscoveryUnavailable:
        logger.info(
            "discovery.get_band_precision: temporarily unavailable for (%s, %s)",
            evidence_source, confidence_band,
        )
        return None


async def get_band_precision_collection(
    collection_id: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """The ``scope='collection'`` ``band_precision`` row -- the propagated-
    witness COLLECTION-level number (e.g. 0.926) that lives on NO per-band
    row. None when absent/unavailable/ambiguous."""
    if not discovery_available():
        return None
    try:
        return await _service.get_band_precision_collection_async(collection_id)
    except DiscoveryUnavailable:
        logger.info("discovery.get_band_precision_collection: temporarily unavailable")
        return None


async def get_band_claim_counts() -> Dict[Tuple[str, str], int]:
    """Codex #9/#B1: the version-aware, SHIPPED, DISPLAY-DEDUPLICATED per-
    (evidence_source, confidence_band) CLAIM-count population -- the BAND-05
    "population" source (each claim counted ONCE via its single
    display_evidence_id, never raw evidence rows, never
    ``band_precision.denominator``). ``{}`` when unavailable."""
    if not discovery_available():
        return {}
    try:
        return await _service.get_band_claim_counts_async()
    except DiscoveryUnavailable:
        logger.info("discovery.get_band_claim_counts: temporarily unavailable")
        return {}


async def get_all_band_precision() -> Dict[Any, Optional[Dict[str, Any]]]:
    """Convenience aggregate for the /help methods route: a dict keyed by
    ``(evidence_source, confidence_band)`` -> its ``band_precision`` row for
    every stored band pair, PLUS a ``'collection'`` key -> the collection-scope
    row (the 0.926, kept separate so it is NEVER attached to a per-band row).

    Probes every pair in ``scripts.discovery_ids.CONFIDENCE_BANDS_BY_SOURCE``
    (which carries BOTH the v1 ``expert_verified`` and the v2
    ``high_confidence_algorithmic`` keys, so it resolves against either sidecar
    version -- §5 v1-read-compat). Absent rows are simply omitted. Fails open to
    ``{}`` when discovery is unavailable."""
    result: Dict[Any, Optional[Dict[str, Any]]] = {}
    if not discovery_available():
        return result
    for source, bands in _ids.CONFIDENCE_BANDS_BY_SOURCE.items():
        for band in bands:
            row = await get_band_precision(source, band)
            if row is not None:
                result[(source, band)] = row
    collection = await get_band_precision_collection()
    if collection is not None:
        result["collection"] = collection
    return result


def discovery_methods_noindex() -> bool:
    """Codex #18: the dedicated pre-release SEO predicate for the /help methods
    section. Returns True (noindex the page) ONLY while discovery is available
    AND the Phase-139 REL-01 gate has NOT flipped
    (``DISCOVERY_PUBLIC_RELEASED`` False) -- so the pre-release methods copy is
    hidden from crawlers, then FLIPS to indexed at REL-01, never noindexed
    forever. When discovery is unavailable the section is absent, so there is
    nothing to noindex and this returns False (no gratuitous de-index)."""
    return discovery_available() and not DISCOVERY_PUBLIC_RELEASED

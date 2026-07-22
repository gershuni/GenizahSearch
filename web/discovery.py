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
from typing import Any, Dict, Iterable, List, Optional

from shared.discovery_errors import DiscoveryUnavailable
from shared.discovery_service import DiscoveryService
from web.discovery_assets import (
    discovery_available,
    discovery_db_path,
    discovery_sidecar_version,
)

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

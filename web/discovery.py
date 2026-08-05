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
from shared.discovery_service import (
    BUCKET_MAIN,
    FACET_LEVELS,  # noqa: F401 -- exported for surfaces to validate against
    FINDINGS_BUCKETS,  # noqa: F401 -- exported for surfaces to validate against
    FINDINGS_SORT_BAND_RANK,
    FINDINGS_SORTS,  # noqa: F401 -- exported for surfaces to validate against
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNITS,  # noqa: F401 -- exported for surfaces to validate against
    DiscoveryService,
    findings_novelty_offered,  # noqa: F401 -- the axis rule a surface needs BEFORE it calls
)
from shared.discovery_surface_projection import (
    STATUS_OK,
    busy_envelope,
    make_envelope,
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


async def get_manuscript_page_ids(
    sys_id: str, *, volume_ie: Optional[str] = None, limit: Optional[int] = None,
) -> Dict[str, Any]:
    """The manuscript's discovery `page_id` list, resolved OFF the event loop.

    The one blocking read here that is not a sidecar query: it loads the browse
    map, which `BrowsePage` does not carry the page ids of. It runs under the
    same off-loop discipline and the same browse budget as every sidecar read.

    `meta['resolved']` is what the panel branches on. An empty item list with
    `resolved=False` means the manuscript's pages could not be resolved -- NOT
    that the manuscript has no identifications. Querying the empty page set and
    rendering a zero would present a resolution failure as a fact about the
    manuscript (T-136-14-11).
    """
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    from web.services import get_service  # lazy: avoids a heavy import at module load

    try:
        result = await _service.run_off_loop(
            get_service().get_manuscript_page_ids, sys_id, volume_ie, limit)
    except DiscoveryOverload:
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:
        logger.info("discovery.get_manuscript_page_ids: timeout for sys_id=%s", sys_id)
        return timeout_envelope(meta={"reason": "query_timeout"})
    return make_envelope(
        STATUS_OK, list(result.page_ids), result.total,
        meta={
            "sys_id": sys_id,
            "resolved": result.resolved,
            "truncated": result.truncated,
            "volume_ie": volume_ie,
        },
    )


async def get_manuscript_works_enveloped(
    page_ids: Iterable[str], *, page: int = 1, page_size: Optional[int] = None,
    lang: str = "en",
) -> Dict[str, Any]:
    """D-13h: "Elsewhere in this manuscript", as NAMED works."""
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_manuscript_works_enveloped_async(
            page_ids, page=page, page_size=page_size, lang=lang)
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        return timeout_envelope(meta={"reason": "query_timeout"})


async def get_related_page_count_enveloped(
    page_id: str, *, include_review: bool = False,
) -> Dict[str, Any]:
    """D-11/D-11a: the header count -- DISTINCT opposite pages, deduplicated,
    with no rows (the rows come only behind the toggle)."""
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_related_page_count_enveloped_async(
            page_id, include_review=include_review)
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        return timeout_envelope(meta={"reason": "query_timeout"})


async def get_related_pages_enveloped(
    page_id: str, *, page: int = 1, page_size: Optional[int] = None,
    include_review: bool = False,
) -> Dict[str, Any]:
    """D-11: the rows behind the toggle, one per DISTINCT opposite page."""
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_related_pages_enveloped_async(
            page_id, page=page, page_size=page_size, include_review=include_review)
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        return timeout_envelope(meta={"reason": "query_timeout"})


async def get_findings_enveloped(
    unit: str = FINDINGS_UNIT_IDENTIFICATION,
    *,
    bucket: str = BUCKET_MAIN,
    novelty: Optional[Iterable[str]] = None,
    include_divergent: bool = False,
    domain: Optional[str] = None,
    author: Optional[str] = None,
    work_id: Optional[str] = None,
    sort: str = FINDINGS_SORT_BAND_RANK,
    page: int = 1,
    page_size: Optional[int] = None,
) -> Dict[str, Any]:
    """The corpus-wide "Computed Identifications" query (A-6).

    NOTE for callers: an out-of-vocabulary `unit` / `sort` / `bucket` raises
    `ValueError` and is NOT converted into an envelope. Those values come from
    closed enums the surface maps (`FINDINGS_UNITS`, `FINDINGS_SORTS`,
    `FINDINGS_BUCKETS`, all exported here), so an unknown one is a bug to fix,
    not a service state to render. Validate the request against those sets
    before calling.

    `include_divergent` defaults to `False` HERE as well as in the service, so
    a caller that has never heard of ruling F gets the ruling's posture rather
    than its opposite. A surface that wants the divergent rows must ask for
    them, in words, on every call.
    """
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_findings_enveloped_async(
            unit, bucket=bucket, novelty=novelty,
            include_divergent=include_divergent, domain=domain, author=author,
            work_id=work_id, sort=sort, page=page, page_size=page_size)
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        return timeout_envelope(meta={"reason": "query_timeout"})


async def get_launch_stats_enveloped() -> Dict[str, Any]:
    """Ruling U's launch statistics: the main-pool contribution total, its three
    shades, and the context figures beside them (plan 136-22).

    THE ONLY SUPPORTED WAY TO OBTAIN ANY OF THESE NUMBERS. Every figure is
    computed from the artifact being served at request time, on the single basis
    `main_pool = 1`, and carries the sidecar version and audience that produced
    it -- because the same query against the public projection and the private
    rebuild returns two different, both-correct answers while BOTH report the
    identical `sidecar_version` string.

    Not one of these numbers may appear as a literal anywhere in code or in a
    translation. `tests/test_discovery_launch_stats.py` enforces that over a
    glob-derived source set and the translation table, and fails naming the
    file, the line, the figure and this accessor.

    NOTHING re-caches above this wrapper: the service's own cache is keyed on
    `(path, sidecar_version)`, and the outer `_browse_cached_call` LRU carries no
    path at all -- layering it here would serve the previous artifact's headline
    after a rebuild swap, while every reader-level test still passed.

    Fails open like every other wrapper; never raises.
    """
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_launch_stats_enveloped_async()
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        logger.info("discovery.get_launch_stats_enveloped: timeout")
        return timeout_envelope(meta={"reason": "query_timeout"})


async def get_findings_facets_enveloped(
    level: str,
    *,
    bucket: str = BUCKET_MAIN,
    novelty: Optional[Iterable[str]] = None,
    include_divergent: bool = False,
    domain: Optional[str] = None,
    author: Optional[str] = None,
    unit: str = FINDINGS_UNIT_IDENTIFICATION,
) -> Dict[str, Any]:
    """The domain / author / work cascade -- on the IDENTIFIED WORK's domain,
    never the manuscript's catalogue domain.

    Takes `include_divergent` AND `unit` for the same reason the row query does:
    a count beside an option has to describe the set that option produces. The
    default result set excludes ruling F's divergent rows, and the reader's row
    unit decides whether a "row" is an identification, a manuscript or a work --
    so a cascade fixed at one grain reports a population the result bar beside it
    does not."""
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_findings_facets_enveloped_async(
            level, bucket=bucket, novelty=novelty,
            include_divergent=include_divergent, domain=domain, author=author,
            unit=unit)
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
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


async def get_work_expansion_enveloped(
    work_id: str,
    enabled_bands: Optional[Iterable[str]] = None,
    *,
    page: int = 1,
    page_size: Optional[int] = None,
    anchor_sys_id: Optional[str] = None,
    anchor_claim_type: Optional[str] = None,
    anchor_evidence_source: Optional[str] = None,
    anchor_confidence_band: Optional[str] = None,
    lang: str = "en",
) -> Dict[str, Any]:
    """PANEL-02 "Other manuscripts matching <work>", the ENVELOPED shape
    (D-13, plan 136-21) -- `{status, items, total, meta}`.

    Prefer this over `get_work_witnesses` above on any surface that decides
    whether to RENDER, and on any surface that shows a COUNT. The list-returning
    wrapper collapses a timeout, an overload, an absent sidecar and a genuine
    zero all into `[]`, and carries no total at all -- so the section would hide
    itself during an outage exactly as though the work had no other carriers,
    and a page length would be the only number available where a real count
    belongs.

    `total` is the count query's exact result. A count that cannot be produced
    inside its budget surfaces as `timeout`; it is never softened into an
    approximate or capped figure.

    The anchor identity is ALL THREE OR NONE (`anchor_claim_type`,
    `anchor_evidence_source`, `anchor_confidence_band`); a partial set raises
    `ValueError` and is NOT converted into an envelope, because it is a caller
    bug rather than a service state -- the same posture `get_findings_enveloped`
    takes for an out-of-vocabulary unit.
    """
    if not discovery_available():
        return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
    try:
        return await _service.get_work_expansion_enveloped_async(
            work_id, enabled_bands, page=page, page_size=page_size,
            anchor_sys_id=anchor_sys_id, anchor_claim_type=anchor_claim_type,
            anchor_evidence_source=anchor_evidence_source,
            anchor_confidence_band=anchor_confidence_band, lang=lang,
        )
    except DiscoveryOverload:  # pragma: no cover -- the service maps this itself
        return busy_envelope(meta={"reason": "bounded_concurrency"})
    except DiscoveryUnavailable:  # pragma: no cover -- the service maps this itself
        logger.info(
            "discovery.get_work_expansion_enveloped: timeout for work_id=%s", work_id)
        return timeout_envelope(meta={"reason": "query_timeout"})


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

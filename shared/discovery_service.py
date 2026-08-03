# -*- coding: utf-8 -*-
"""The single async, read-only ``DiscoveryService`` chokepoint (Phase 134,
DATA-06) through which all web access to the ``discovery.db`` sidecar flows.

Modeled on the established ``shared/*_service.py`` sidecar-service shape
(``shared/fjms_service.py``: module-level sidecar open + graceful-absent
reads + a ``meta`` version accessor) composed with the off-event-loop async
pattern already proven in ``web/search_api.py`` (``run_in_executor`` +
``asyncio.wait`` -- NEVER ``asyncio.wait_for`` over ``run_in_executor``,
since executor threads are not cancellable -- plus a non-blocking bounded
concurrency semaphore for heavy queries).

Key invariants (134-06 must_haves):
  - ``__init__`` takes INJECTED LAZY providers (``path_provider``,
    ``availability_callable``, ``sidecar_version_provider``) and stores them
    WITHOUT touching the DB (F15) -- importing a module that constructs a
    ``DiscoveryService`` at import time (``web/discovery.py``) before the
    sidecar loader has run must never bind a stale/empty path.
  - The ``ThreadLocalConnection`` is built LAZILY on the first AVAILABLE
    call, and RECREATED whenever the resolved path or sidecar version
    changes -- the PRIOR ``ThreadLocalConnection`` pool is ``.close()``d
    first so no per-thread handle is ever leaked (R8).
  - Every sync read method follows the fjms_service graceful-absent shape:
    unavailable or any exception -> return ``[]``/``None``, NEVER raise.
  - Every async wrapper dispatches its sync method via
    ``loop.run_in_executor(...)`` wrapped in ``asyncio.wait({fut},
    timeout=...)`` (never ``wait_for``); a timeout raises
    ``DiscoveryUnavailable`` WITHOUT awaiting the abandoned future.
  - Heavy reads (``get_work_witnesses``) acquire a non-blocking bounded
    semaphore; the slot is released from the future's ``add_done_callback``
    (never a bare ``finally``) so a timed-out thread cannot re-admit new
    heavy work until it truly finishes (DC6).
  - The browse-enrichment reads (``get_claims_for_page`` /
    ``get_pages_related_to_page``) are wrapped in a small bounded LRU keyed
    INCLUDING the sidecar version, so a version swap never serves stale
    cached rows (F15).
  - ``get_work_witnesses`` implements the DATA-10 unit x work projection: a
    physical-MS witness_unit is shown ONCE at its highest member band; the
    enabled-band filter acts on that DISPLAYED band BEFORE pagination; the
    anchor's own unit is excluded; same-unit members are suppressed from the
    list (the grouping/filtering/sorting/pagination logic itself is a pure,
    DB-free helper -- ``_project_work_witnesses`` -- directly unit-testable).

All timeout/concurrency/LRU/page-size defaults are read from
``docs/specs/discovery-budgets.md``'s ``DISCOVERY_*`` env-var convention,
re-read PER CALL (never baked in at import time) so they can be tuned in
production without a restart.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import threading
from collections import OrderedDict
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from shared.discovery_band_labels import serialize_banded_claim
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_surface_projection import (
    STATUS_OK,
    busy_envelope,
    make_envelope,
    surface_safe_claim,
    surface_safe_related_page,
    surface_safe_work_summary,
    timeout_envelope,
    unavailable_envelope,
)
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Env-var defaults (docs/specs/discovery-budgets.md SS2/SS3) -- re-read per
# call, never cached at import time.
# ---------------------------------------------------------------------------

_DEFAULT_QUERY_TIMEOUT_BROWSE = 2.0
_DEFAULT_QUERY_TIMEOUT_WORK = 5.0
_DEFAULT_MAX_CONCURRENT_QUERIES = 4
_DEFAULT_BROWSE_LRU_MAX_ENTRIES = 5000
_DEFAULT_PAGE_SIZE_DEFAULT = 50
_DEFAULT_PAGE_SIZE_MAX = 200

# M3: the frozen ABSOLUTE row-per-page ceiling (docs/specs/discovery-budgets.md
# SS3: "hard ceiling; never overridable above this"). DISCOVERY_PAGE_SIZE_MAX
# may only ever TIGHTEN this value, never raise it -- see _clamp_page_size.
_ABSOLUTE_PAGE_SIZE_CEILING = 200

# H1: there is NO pre-grouping raw-claim cap here (a previous
# _MAX_RAW_CLAIMS_PER_WORK=5000 / _MAX_UNIT_MEMBER_ROWS=200_000 pair silently
# dropped units on any work with more claims than that -- get_work_witnesses
# now performs the whole DATA-10 unit x work projection IN SQL, so
# LIMIT/OFFSET paginates over UNITS post-grouping, never over a truncated
# pre-grouping raw-claim scan; see get_work_witnesses below).


def _get_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    return default


def _get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    return default


def _get_positive_int_env(name: str, default: int) -> int:
    """Like ``_get_int_env``, but coerces to >= 1 (M3) -- a non-positive or
    unparsable env value falls back to ``default`` rather than propagating
    into ``asyncio.Semaphore(n)`` (which raises ValueError for n < 0 and
    means "always locked" for n == 0, either of which would silently break
    or permanently overload the service from a single bad env var)."""
    value = _get_int_env(name, default)
    return value if value >= 1 else default


def _get_positive_float_env(name: str, default: float) -> float:
    """Like ``_get_float_env``, but coerces to > 0 (M3) -- a non-positive
    timeout would make ``asyncio.wait(..., timeout=X)`` return immediately
    on every call, permanently overloading the service from a single bad
    env var; falls back to ``default`` instead."""
    value = _get_float_env(name, default)
    return value if value > 0 else default


def _parse_json_field(value: Optional[str]) -> Any:
    """Best-effort JSON parse for the seed_spans/seed_ms_ids TEXT columns.
    Returns None for a NULL cell, the parsed object on success, or the raw
    string unchanged if it somehow isn't valid JSON (defensive; never
    raises)."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


# ---------------------------------------------------------------------------
# Frozen global band-rank (docs/specs/discovery-sidecar-schema-v1.md SS6 item
# 2), strongest first. Inlined here (rather than importing
# scripts/discovery_ids.py) to keep this runtime module decoupled from the
# offline-build script tree -- mirrors the same decision already made in
# web/discovery_assets.py (see its own enum-vocab spot-check comment).
# ---------------------------------------------------------------------------

# v1-read-compat window (Codex #8): the strongest track1_direct band is listed
# under BOTH the v2 key `high_confidence_algorithmic` AND the v1 key
# `expert_verified` at the same (top) rank position, so the band-rank lattice
# ranks whichever key the currently-loaded asset carries. A built v2 asset
# uses only the v2 key (the offline verifier's no-mixed-enum-state check
# enforces that); do NOT drop `expert_verified` until the v2 manifest is live
# (135-08) -- the v1 asset + the v1 fixture tests still read it.
_BAND_RANK_ORDER: List[Tuple[str, str]] = [
    ("track1_direct", "high_confidence_algorithmic"),
    ("track1_direct", "expert_verified"),
    ("track1_direct", "tier_a"),
    ("propagated", "corroborated"),
    ("track1_direct", "screening_rb"),
    ("track1_direct", "screening_canon"),
    ("propagated", "weak"),
    ("propagated", "not_evaluated"),
]
_BAND_RANK_INDEX: Dict[Tuple[str, str], int] = {
    pair: i for i, pair in enumerate(_BAND_RANK_ORDER)
}
_UNRANKED_BAND = len(_BAND_RANK_ORDER)


def _band_rank(evidence_source: Optional[str], confidence_band: Optional[str]) -> int:
    """Lower is "stronger" (rank 0 = expert_verified, the strongest band)."""
    return _BAND_RANK_INDEX.get((evidence_source, confidence_band), _UNRANKED_BAND)


def _build_band_rank_case_sql() -> str:
    """Build the frozen band-rank lattice as a SQL CASE expression (H1) --
    ``_BAND_RANK_ORDER`` is a frozen module-level constant (never user
    input), so building SQL via string formatting here is safe. Used to
    perform the DATA-10 unit x work projection's "highest member band"
    selection IN SQL (a ``ROW_NUMBER() OVER (PARTITION BY unit_key ...)``
    window-function query) instead of in Python, so ``LIMIT``/``OFFSET``
    can paginate over UNITS post-grouping rather than a pre-grouping
    raw-claim cap."""
    lines = ["CASE"]
    for i, (source, band) in enumerate(_BAND_RANK_ORDER):
        lines.append(
            f"    WHEN de.evidence_source = '{source}' AND de.confidence_band = '{band}' THEN {i}"
        )
    lines.append(f"    ELSE {_UNRANKED_BAND}")
    lines.append("  END")
    return "\n".join(lines)


_BAND_RANK_CASE_SQL = _build_band_rank_case_sql()


# ---------------------------------------------------------------------------
# D-13g: the default-surface routing predicate.
#
# The bug this replaces: the page query filtered `routing_status = 'shipped'`
# in SQL, while `shared/discovery_band_labels.py::is_default_eligible` returns
# True for `human_confirmed` UNCONDITIONALLY, before it inspects routing --
# and `discovery-band-labels-v1.md` SS4 says the same. So a human-confirmed row
# that routing had demoted was dropped by the query BEFORE the predicate meant
# to protect it ever ran: 19 of the 121 human-confirmed rows across all
# human-confirmed evidence, and 14 of 116 on the DISPLAY evidence this query
# actually reads. Live symptom: on one manuscript, one page's human-confirmed
# row was hidden while another page's human-confirmed row showed -- two rows a
# human confirmed, treated differently.
#
# The fix is in SQL, not in a display-layer patch, and it MIRRORS the
# eligibility rule the build already materializes into
# `discovery_identification` (`scripts/build_discovery_sidecar.py::
# populate_discovery_identification`) -- so the restore cannot be undone one
# layer down by the identification join.
# ---------------------------------------------------------------------------

_CLAIMS_DEFAULT_ROUTING_CLAUSE = (
    "AND (de.routing_status = 'shipped' "
    "OR de.adjudication_status = 'human_confirmed')"
)

# The manuscript scope is served by `page_id IN (...)` over the browse page's
# own page list, NOT by `discovery_evidence.sys_id` -- that column has no
# index, while `ix_discovery_claim_page_id` exists and is confirmed in use by
# EXPLAIN QUERY PLAN (asserted by a test, not assumed). Bounded so an
# unbounded caller cannot build a giant IN list: the largest manuscript in the
# corpus carries 427 claims, and 429 manuscripts carry over 50.
_MAX_MANUSCRIPT_PAGE_IDS = 500


def _build_manuscript_works_sql(n_page_ids: int) -> str:
    """The D-13h manuscript-scope query, as SQL, for `n_page_ids` bound
    parameters.

    A module-level builder rather than an inline f-string so a test can run
    `EXPLAIN QUERY PLAN` over the EXACT statement the service executes -- an
    index assertion against a hand-retyped near-copy proves nothing.

    Only the placeholder COUNT is interpolated; every value is bound.
    """
    placeholders = ",".join("?" for _ in range(n_page_ids))
    return f"""
        SELECT w.canonical_work_id AS canonical_work_id,
               MIN(di.display_work_id) AS display_work_id,
               MIN(dw.neutral_title) AS display_neutral_title,
               MIN(w.neutral_title) AS neutral_title,
               MIN(COALESCE(dw.author, w.author)) AS author,
               MIN(COALESCE(dw.genre, w.genre)) AS genre,
               COUNT(DISTINCT dc.page_id) AS page_count,
               MIN(COALESCE(de.band_rank, {_UNRANKED_BAND})) AS best_band_rank,
               MAX(COALESCE(di.main_pool, 0)) AS any_main_pool,
               MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                                      WHEN 'quotes_this_work' THEN 1
                                      ELSE 2 END) AS _relation_strength,
               CASE MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                                           WHEN 'quotes_this_work' THEN 1
                                           ELSE 2 END)
                    WHEN 0 THEN 'direct_witness'
                    WHEN 1 THEN 'quotes_this_work'
                    ELSE 'shared_text' END AS relation_kind,
               COUNT(*) OVER () AS _total_rows
        FROM discovery_claim dc
        JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
        JOIN works w ON w.work_id = dc.work_id
        LEFT JOIN discovery_identification di
               ON di.sys_id = de.sys_id
              AND di.canonical_work_id = w.canonical_work_id
        LEFT JOIN works dw ON dw.work_id = di.display_work_id
        WHERE dc.page_id IN ({placeholders})
        GROUP BY w.canonical_work_id
        ORDER BY best_band_rank ASC, canonical_work_id ASC
        LIMIT ? OFFSET ?
    """

# The per-work "ranked" CTE body (H1): every witness claim's display
# evidence for `work_id`, its physical-MS unit_key (a real unit_id, or a
# `sys:<sys_id>` singleton discriminator -- unit_id values are always
# 64-hex-char sha256 digests so they can never collide with the "sys:"
# prefix form), and its frozen band_rank. Reused (with the SAME `work_id`
# bind parameter) by both the paginated projection query and the
# member-sys_ids follow-up query below, so the two queries can never drift
# out of sync with each other.
_WORK_WITNESSES_RANKED_CTE_SQL = f"""
  SELECT
    COALESCE(wum.unit_id, 'sys:' || de.sys_id) AS unit_key,
    wum.unit_id AS unit_id,
    dc.page_id AS page_id,
    dc.work_id AS work_id,
    dc.claim_id AS claim_id,
    dc.claim_type AS claim_type,
    de.sys_id AS sys_id,
    de.evidence_source AS evidence_source,
    de.confidence_band AS confidence_band,
    {_BAND_RANK_CASE_SQL} AS band_rank
  FROM discovery_claim dc
  JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
  LEFT JOIN witness_unit_members wum ON wum.sys_id = de.sys_id
  WHERE dc.work_id = ?
    AND dc.claim_type IN ('direct_witness', 'quotes_this_work')
"""


# ---------------------------------------------------------------------------
# Pure, DB-free DATA-10 unit x work projection helper. Kept as the reference
# implementation of the SAME grouping/highest-band/anchor-exclusion/
# same-unit-suppression/pagination rules the SQL projection in
# get_work_witnesses() below implements directly in the database -- used
# for member-claim expansion callers that already hold an in-memory row set
# with no DB handle, and directly unit-tested with fabricated data (no
# fixture DB required) so the rules stay pinned independent of the SQL.
# ---------------------------------------------------------------------------

def _project_work_witnesses(
    claim_rows: Iterable[Dict[str, Any]],
    unit_by_sys: Dict[str, str],
    *,
    enabled_bands: Optional[Iterable[str]] = None,
    anchor_sys_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> List[Dict[str, Any]]:
    """Group ``claim_rows`` (one row per real (page_id, work_id) witness
    claim, each carrying the claim's OWN display-evidence band) into
    physical-MS ``witness_units`` and project ONE row per unit at its
    HIGHEST member band, filtering on that displayed band BEFORE
    pagination, excluding the anchor's own unit, and suppressing same-unit
    members from the returned list.

    Args:
        claim_rows: dicts with keys ``page_id``, ``work_id``, ``claim_id``,
            ``claim_type``, ``sys_id``, ``evidence_source``,
            ``confidence_band`` -- one row per witness claim for a single
            work_id (already restricted to claim_type IN
            ('direct_witness', 'quotes_this_work') by the caller).
        unit_by_sys: sys_id -> unit_id for every sys_id that belongs to a
            merged witness_unit; a sys_id absent from this dict is its own
            singleton (unmerged) unit.
        enabled_bands: optional iterable of confidence_band strings; when
            given (non-empty), only units whose DISPLAYED band is in this
            set are kept. None/empty = no filtering (every band shown).
        anchor_sys_id: when given, the unit containing this sys_id
            (merged or singleton) is excluded entirely from the result.
        page, page_size: 1-indexed pagination applied AFTER filtering.

    Returns:
        A list of dicts, one per surviving unit, each carrying:
        ``work_id``, ``unit_id`` (None for an unmerged singleton),
        ``representative_sys_id``, ``representative_page_id``,
        ``representative_claim_id``, ``claim_type``, ``evidence_source``,
        ``confidence_band`` (the unit's displayed/highest band), and
        ``member_sys_ids`` (sorted list of every claim-bearing sys_id in
        the unit -- the surface a caller uses to retrieve suppressed
        member claims on expansion, via ``get_claims_for_page`` +
        ``witness_unit_members``; there is no ``supporting_page_ids``
        column, G4/R5).
    """
    rows = list(claim_rows)
    if not rows:
        return []

    enabled_bands_set = set(enabled_bands) if enabled_bands else None

    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        sys_id = row["sys_id"]
        unit_id = unit_by_sys.get(sys_id)
        unit_key = ("unit", unit_id) if unit_id is not None else ("sys", sys_id)
        groups.setdefault(unit_key, []).append(row)

    anchor_unit_key = None
    if anchor_sys_id:
        anchor_unit_id = unit_by_sys.get(anchor_sys_id)
        anchor_unit_key = (
            ("unit", anchor_unit_id) if anchor_unit_id is not None else ("sys", anchor_sys_id)
        )

    items: List[Dict[str, Any]] = []
    for unit_key, members in groups.items():
        if anchor_unit_key is not None and unit_key == anchor_unit_key:
            continue  # exclude the anchor's own unit entirely

        # MED (Codex R2): band_rank + sys_id alone is NOT a total order --
        # a unit/sys_id can carry >=2 same-band page claims (2,829 tied
        # units observed in the cited real-corpus large work), leaving the
        # representative dependent on unspecified scan/insertion order.
        # page_id/claim_id are stable, deterministic secondary tie-breakers
        # -- MIRRORED exactly (same key order) in the SQL projection below
        # (_WORK_WITNESSES_RANKED_CTE_SQL's ROW_NUMBER() ORDER BY) so the
        # pure-Python reference implementation and the SQL projection can
        # never disagree on which row wins a tie.
        best_row = min(
            members,
            key=lambda r: (
                _band_rank(r["evidence_source"], r["confidence_band"]),
                r["sys_id"],
                r["page_id"],
                r["claim_id"],
            ),
        )
        displayed_band = best_row["confidence_band"]
        if enabled_bands_set is not None and displayed_band not in enabled_bands_set:
            continue

        member_sys_ids = sorted({m["sys_id"] for m in members})
        items.append({
            "work_id": best_row["work_id"],
            "unit_id": unit_key[1] if unit_key[0] == "unit" else None,
            "representative_sys_id": best_row["sys_id"],
            "representative_page_id": best_row["page_id"],
            "representative_claim_id": best_row["claim_id"],
            "claim_type": best_row["claim_type"],
            "evidence_source": best_row["evidence_source"],
            "confidence_band": displayed_band,
            "member_sys_ids": member_sys_ids,
        })

    items.sort(
        key=lambda it: (
            _band_rank(it["evidence_source"], it["confidence_band"]),
            it["representative_sys_id"],
            it["representative_page_id"],
            it["representative_claim_id"],
        )
    )

    page = page if isinstance(page, int) and page >= 1 else 1
    page_size = page_size if isinstance(page_size, int) and page_size > 0 else 1
    offset = (page - 1) * page_size
    return items[offset: offset + page_size]


class DiscoveryService:
    """The one async read-only chokepoint over ``discovery.db``.

    Constructed with LAZY injected providers -- NOTHING is read from disk
    until the first available call (F15). Safe to construct at MODULE
    import time (as ``web/discovery.py`` does) even before the sidecar
    loader has run.
    """

    def __init__(
        self,
        path_provider: Callable[[], Optional[str]],
        availability_callable: Optional[Callable[[], bool]] = None,
        sidecar_version_provider: Optional[Callable[[], Optional[str]]] = None,
    ) -> None:
        self._path_provider = path_provider
        self._availability_callable = availability_callable
        self._sidecar_version_provider = sidecar_version_provider

        # Lazy connection state (F15) -- deliberately NOT built here.
        self._conn: Optional[ThreadLocalConnection] = None
        self._last_path: Optional[str] = None
        self._last_version: Optional[str] = None
        self._conn_lock = threading.Lock()

        # Browse-enrichment LRU (version-keyed, F15).
        self._browse_lru: "OrderedDict[tuple, Any]" = OrderedDict()
        self._browse_lru_lock = threading.Lock()

        # 136-14: the `scope='band'` measurement lookup, cached per
        # (path, version) -- see _band_measurements().
        self._band_measurement_cache: Optional[Tuple[tuple, Dict]] = None

        # Heavy-query bounded concurrency (mirrors web/search_api.py's
        # _HeavySemaphoreState, kept per-instance since this class -- unlike
        # the module-function shape of search_api.py -- IS the natural
        # single-owner scope for its own concurrency budget).
        capacity = _get_positive_int_env(
            "DISCOVERY_MAX_CONCURRENT_QUERIES", _DEFAULT_MAX_CONCURRENT_QUERIES
        )
        self._heavy_sem = asyncio.Semaphore(capacity)
        self._heavy_capacity = capacity

    # ------------------------------------------------------------------
    # Lazy connection management (F15 / R8)
    # ------------------------------------------------------------------

    def _get_conn(self) -> Optional[ThreadLocalConnection]:
        try:
            path = self._path_provider() if self._path_provider is not None else None
        except Exception as e:
            logger.error("DiscoveryService: path_provider raised: %s", e)
            return None
        if not path:
            return None

        version = None
        if self._sidecar_version_provider is not None:
            try:
                version = self._sidecar_version_provider()
            except Exception as e:
                logger.error("DiscoveryService: sidecar_version_provider raised: %s", e)
                version = None

        with self._conn_lock:
            if self._conn is None or path != self._last_path or version != self._last_version:
                old_conn = self._conn
                try:
                    new_conn = ThreadLocalConnection(f"file:{path}?mode=ro", row_factory=sqlite3.Row)
                except Exception as e:
                    logger.error("DiscoveryService: failed to open %s: %s", path, e)
                    return None
                self._conn = new_conn
                self._last_path = path
                self._last_version = version
                if old_conn is not None:
                    try:
                        old_conn.close()
                    except Exception:
                        logger.debug(
                            "DiscoveryService: error closing prior connection pool", exc_info=True
                        )
            return self._conn

    def is_available(self) -> bool:
        try:
            if self._availability_callable is not None and not self._availability_callable():
                return False
        except Exception as e:
            logger.error("DiscoveryService.is_available: availability_callable raised: %s", e)
            return False
        return self._get_conn() is not None

    # ------------------------------------------------------------------
    # Pagination helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clamp_page(page: Any) -> int:
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        return page if page >= 1 else 1

    @staticmethod
    def _clamp_page_size(page_size: Any) -> int:
        # M3: DISCOVERY_PAGE_SIZE_MAX can only TIGHTEN the frozen absolute
        # ceiling, never raise it -- a misconfigured env var (0, negative,
        # or > _ABSOLUTE_PAGE_SIZE_CEILING) falls back to the ceiling itself
        # rather than being trusted as-is.
        raw_maximum = _get_int_env("DISCOVERY_PAGE_SIZE_MAX", _DEFAULT_PAGE_SIZE_MAX)
        if raw_maximum <= 0 or raw_maximum > _ABSOLUTE_PAGE_SIZE_CEILING:
            maximum = _ABSOLUTE_PAGE_SIZE_CEILING
        else:
            maximum = raw_maximum

        default = _get_int_env("DISCOVERY_PAGE_SIZE_DEFAULT", _DEFAULT_PAGE_SIZE_DEFAULT)
        if default <= 0 or default > maximum:
            default = min(_DEFAULT_PAGE_SIZE_DEFAULT, maximum)

        if page_size is None:
            page_size = default
        try:
            page_size = int(page_size)
        except (TypeError, ValueError):
            page_size = default
        if page_size <= 0:
            page_size = default
        return min(page_size, maximum)

    # ------------------------------------------------------------------
    # Sync read core (fjms_service graceful-absent shape -- never raises)
    # ------------------------------------------------------------------

    def get_version(self) -> Optional[str]:
        if not self.is_available():
            return None
        conn = self._get_conn()
        if conn is None:
            return None
        try:
            cur = conn.execute("SELECT value FROM meta WHERE key = 'sidecar_version'")
            row = cur.fetchone()
            return row["value"] if row else None
        except Exception as e:
            logger.error("DiscoveryService.get_version error: %s", e)
            return None

    def get_band_precision(
        self, evidence_source: Optional[str], confidence_band: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """BAND-02: the matching ``scope='band'`` ``band_precision`` row, or
        None when absent/unavailable (never raises -- mirrors ``get_version``
        exactly).

        Uses ``SELECT *`` (never a fixed column list) so the 135-05-added
        columns (``measurement_status``/``measurement_date``/``grader``/
        ``audit_status``/``report_id``) surface when present and are simply
        absent (via ``dict.get`` downstream) against a v1-shaped fixture."""
        if not self.is_available():
            return None
        conn = self._get_conn()
        if conn is None:
            return None
        try:
            cur = conn.execute(
                "SELECT * FROM band_precision WHERE scope = 'band' "
                "AND evidence_source = ? AND confidence_band = ?",
                (evidence_source, confidence_band),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(
                "DiscoveryService.get_band_precision error for (%s, %s): %s",
                evidence_source, confidence_band, e,
            )
            return None

    def get_band_precision_collection(
        self, collection_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """The ``scope='collection'`` ``band_precision`` row -- the
        propagated-witness COLLECTION-level number (e.g. 0.926) that never
        lives on any band row. With ``collection_id=None``, returns the sole
        collection row when exactly one exists; None when zero or more than
        one (ambiguous -- never raises, mirrors ``get_version``)."""
        if not self.is_available():
            return None
        conn = self._get_conn()
        if conn is None:
            return None
        try:
            if collection_id is not None:
                cur = conn.execute(
                    "SELECT * FROM band_precision WHERE scope = 'collection' AND collection_id = ?",
                    (collection_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
            cur = conn.execute("SELECT * FROM band_precision WHERE scope = 'collection'")
            rows = cur.fetchall()
            if len(rows) != 1:
                return None
            return dict(rows[0])
        except Exception as e:
            logger.error("DiscoveryService.get_band_precision_collection error: %s", e)
            return None

    def get_band_claim_counts(self) -> Dict[Tuple[str, str], int]:
        """Codex #9/#B1: the version-aware, SHIPPED, DISPLAY-DEDUPLICATED
        per-(evidence_source, confidence_band) CLAIM-count population --
        each ``discovery_claim`` counted ONCE via its single
        ``display_evidence_id`` (per docs/specs/discovery-frames.md §4), NOT
        raw ``discovery_evidence`` rows (a claim owning >1 evidence row, e.g.
        the witness + shared_text collision, must not inflate the count).
        ``{}`` on unavailable/error (never raises). This is the BAND-05
        "population" source -- reflects whatever sidecar asset is currently
        loaded (v1 pre-bake, v2 post-deploy), never ``band_precision.
        denominator`` (the graded-sample size, a different number)."""
        if not self.is_available():
            return {}
        conn = self._get_conn()
        if conn is None:
            return {}
        try:
            cur = conn.execute(
                """
                SELECT e.evidence_source, e.confidence_band, COUNT(*) AS n
                FROM discovery_claim c
                JOIN discovery_evidence e ON e.evidence_id = c.display_evidence_id
                WHERE e.routing_status = 'shipped'
                GROUP BY e.evidence_source, e.confidence_band
                """
            )
            return {
                (row["evidence_source"], row["confidence_band"]): row["n"]
                for row in cur.fetchall()
            }
        except Exception as e:
            logger.error("DiscoveryService.get_band_claim_counts error: %s", e)
            return {}

    def get_claims_for_page(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        """PANEL-01/02: the manuscript's banded claims on this page, each at
        its display_evidence_id-selected band.

        Defaults to the DEFAULT-SURFACE population, which is NOT "shipped
        only" -- see ``_CLAIMS_DEFAULT_ROUTING_CLAUSE`` and D-13g. Review-only
        rows still PERSIST in the sidecar (queryable) -- they are only hidden
        from this default read surface, never deleted
        (docs/specs/discovery-sidecar-schema-v1.md SS7); ``include_review=True``
        opts into all of them.

        Signature and return type are UNCHANGED from 134-06 (a list of dicts,
        ``[]`` on every failure path, never an exception). The row now carries
        the panel's display fields as well; the total is available only through
        the enveloped shape below, since adding it here would change the return
        type."""
        rows, _total = self._query_claims_for_page(
            page_id, page=page, page_size=page_size, include_review=include_review)
        return rows

    # ------------------------------------------------------------------
    # 136-14 Task 1: the panel's ONE query.
    # ------------------------------------------------------------------

    def _query_claims_for_page(
        self, page_id: str, *, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """The single page query: claim + display evidence + the materialized
        identification + the DISPLAY work, plus the real total, in ONE
        statement.

        ``COUNT(*) OVER ()`` is what makes the total free: SQLite evaluates a
        window function over the whole result set BEFORE ``LIMIT``, so the
        panel gets its "N identifications" header without a second query and
        without a per-row follow-up (T-136-14-03).

        The identification join is a LEFT join deliberately. Its eligibility
        rule (`shipped` OR `human_confirmed`) matches this query's own default
        predicate, so it is total for the default population -- but under
        ``include_review=True`` a review-only/unreviewed claim legitimately has
        NO identification row, and an inner join would silently drop exactly
        the rows the opt-in flag exists to reveal. A missing identification
        surfaces as a NULL bucket, never as a vanished row.

        ``eligibility_basis`` is derived PER ROW here rather than read from
        ``discovery_identification.eligibility_basis``, for two reasons. It is
        more precise: the stored column is an AGGREGATE over an identification's
        evidence rows, so an identification carrying one shipped row and one
        restored review-only row reports ``shipped`` for both, mislabelling the
        row the surface must annotate. And it is more portable: that column is
        not yet in the schema contract's authorized column list (a dated
        amendment is owed), so a contract-shaped artifact may not carry it at
        all. Three closed values: ``shipped``, ``human_confirmed`` (D-13g's
        restore) and ``review_opt_in`` (present only because the caller passed
        ``include_review``).

        The identity join is on ``display_work_id``, NEVER ``canonical_work_id``:
        the latter is not unique on ``works`` (15 duplicated groups on the live
        asset, three with different titles and mixed source corpora), so joining
        on it FANS OUT the identification grain (64,509 -> 65,587 rows) and
        makes both the displayed title and ``identity_visibility`` a matter of
        which row the join happened to return.
        """
        if not self.is_available():
            return [], 0
        conn = self._get_conn()
        if conn is None:
            return [], 0
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        routing_clause = "" if include_review else _CLAIMS_DEFAULT_ROUTING_CLAUSE
        try:
            cur = conn.execute(
                f"""
                SELECT dc.page_id, dc.work_id, dc.claim_id, dc.claim_type, dc.source_corpus,
                       de.evidence_id, de.evidence_kind, de.evidence_source, de.confidence_band,
                       de.adjudication_status, de.audit_status, de.routing_status, de.routing_reason,
                       de.is_new, de.a_page_id, de.sys_id, de.span_start, de.span_end,
                       de.text_layer, de.snapshot_hash,
                       de.matched_letters, de.n_spans, de.band_rank,
                       de.coverage_ppm, de.coverage_status,
                       de.novelty_status, de.novelty_source_label,
                       w.neutral_title, w.author, w.genre, w.canonical_work_id,
                       di.identification_id, di.display_work_id, di.main_pool,
                       di.main_pool_reason, di.page_count AS identification_page_count,
                       CASE WHEN de.routing_status = 'shipped' THEN 'shipped'
                            WHEN de.adjudication_status = 'human_confirmed'
                                 THEN 'human_confirmed'
                            ELSE 'review_opt_in' END AS eligibility_basis,
                       dw.neutral_title AS display_neutral_title,
                       dw.author AS display_author,
                       dw.genre AS display_genre,
                       CASE WHEN de.routing_status <> 'shipped'
                             AND de.adjudication_status = 'human_confirmed'
                            THEN 1 ELSE 0 END AS restored_by_human_confirmation,
                       CASE WHEN de.routing_status <> 'shipped'
                             AND de.routing_reason = 'low_coverage'
                            THEN 1 ELSE 0 END AS low_coverage_marker,
                       COUNT(*) OVER () AS _total_rows
                FROM discovery_claim dc
                JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
                JOIN works w ON w.work_id = dc.work_id
                LEFT JOIN discovery_identification di
                       ON di.sys_id = de.sys_id
                      AND di.canonical_work_id = w.canonical_work_id
                LEFT JOIN works dw ON dw.work_id = di.display_work_id
                WHERE dc.page_id = ?
                {routing_clause}
                ORDER BY COALESCE(de.band_rank, {_UNRANKED_BAND}) ASC, dc.work_id ASC
                LIMIT ? OFFSET ?
                """,
                (page_id, page_size, offset),
            )
            rows = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("DiscoveryService.get_claims_for_page error for %s: %s", page_id, e)
            return [], 0
        total = rows[0].pop("_total_rows", len(rows)) if rows else 0
        for row in rows[1:]:
            row.pop("_total_rows", None)
        return rows, int(total)

    def _band_measurements(self) -> Dict[Tuple[str, str], Tuple[Optional[str], Optional[float]]]:
        """`(evidence_source, confidence_band) -> (measurement_status, ci_low)`
        for every stored `scope='band'` row, cached per sidecar version.

        Read ONCE per version rather than joined into the page query on
        purpose: `band_precision` carries `precision`/`ci_low`/`ci_high`, and a
        join would put those columns on every raw row the service returns --
        one careless caller away from a D-06 violation. Here they exist only
        inside this module, are consumed by `serialize_banded_claim`, and are
        dropped by `surface_safe_claim` before anything leaves.
        """
        version = None
        if self._sidecar_version_provider is not None:
            try:
                version = self._sidecar_version_provider()
            except Exception:
                version = None
        key = (self._last_path, version)
        cached = self._band_measurement_cache
        if cached is not None and cached[0] == key:
            return cached[1]

        measurements: Dict[Tuple[str, str], Tuple[Optional[str], Optional[float]]] = {}
        conn = self._get_conn()
        if conn is not None:
            try:
                cur = conn.execute(
                    "SELECT evidence_source, confidence_band, measurement_status, ci_low "
                    "FROM band_precision WHERE scope = 'band'"
                )
                for row in cur.fetchall():
                    measurements[(row["evidence_source"], row["confidence_band"])] = (
                        row["measurement_status"], row["ci_low"],
                    )
            except Exception as e:
                logger.error("DiscoveryService._band_measurements error: %s", e)
                return {}
        self._band_measurement_cache = (key, measurements)
        return measurements

    def _present_claim_row(
        self, row: Mapping[str, Any], measurements, lang: str,
    ) -> Dict[str, Any]:
        """One raw query row -> the surface-safe panel row.

        Band presentation goes through `serialize_banded_claim`, which RAISES
        rather than emitting a bandless presentation (SC#1); the result is then
        projected through the allowlist, which drops `review_overlay` and the
        band measurement inputs. Both steps are mandatory: the serializer
        guarantees the band is present, the projection guarantees the badge is
        not.
        """
        measurement_status, ci_low = measurements.get(
            (row.get("evidence_source"), row.get("confidence_band")), (None, None))
        banded = serialize_banded_claim(
            {**row, "measurement_status": measurement_status, "ci_low": ci_low}, lang)

        display_title = row.get("display_neutral_title")
        claim_title = row.get("neutral_title")
        title = display_title if display_title else claim_title
        presented = {
            **row,
            "band_label": banded["band_label"],
            "measurement_status": banded["measurement_status"],
            "default_eligible": banded["default_eligible"],
            "display_work_id": row.get("display_work_id") or row.get("work_id"),
            "neutral_title": title or None,
            "title_missing": not bool(title),
            "author": row.get("display_author") if display_title else row.get("author"),
            "genre": row.get("display_genre") if display_title else row.get("genre"),
            "relation_kind": row.get("claim_type"),
            "main_pool": None if row.get("main_pool") is None else bool(row.get("main_pool")),
            "restored_by_human_confirmation": bool(row.get("restored_by_human_confirmation")),
            "low_coverage_marker": bool(row.get("low_coverage_marker")),
        }
        return surface_safe_claim(presented)

    def get_claims_for_page_enveloped(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False, lang: str = "en",
    ) -> Dict[str, Any]:
        """The SYNC enveloped shape of `get_claims_for_page` (D-13).

        Two shapes, ONE implementation. This sync callable exists because the
        panel crosses into `run.io_bound` with an explicit client, and an async
        function handed to a sync worker silently returns a coroutine nobody
        awaits; the async wrapper below exists because every other caller is on
        the loop. Both go through this method -- never two query paths.

        A failed query returns `unavailable`, never `ok` with an empty list: an
        outage that renders as a genuine zero is the exact defect D-13 exists to
        prevent (the panel hides itself on a SUCCESSFUL zero).
        """
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        try:
            rows, total = self._query_claims_for_page(
                page_id, page=page, page_size=page_size, include_review=include_review)
            measurements = self._band_measurements()
            items = [self._present_claim_row(row, measurements, lang) for row in rows]
        except Exception as e:
            logger.error(
                "DiscoveryService.get_claims_for_page_enveloped error for %s: %s", page_id, e)
            return unavailable_envelope(meta={"reason": "query_failed"})
        return make_envelope(
            STATUS_OK, items, total,
            meta={"page_id": page_id, "include_review": bool(include_review)},
        )

    # ------------------------------------------------------------------
    # 136-14 Task 2: manuscript scope that NAMES the works (D-13h).
    # ------------------------------------------------------------------

    def get_manuscript_works_enveloped(
        self, page_ids: Iterable[str], page: int = 1, page_size: Optional[int] = None,
        lang: str = "en",
    ) -> Dict[str, Any]:
        """"Elsewhere in this manuscript", as NAMED works (D-13h).

        One row per DISTINCT canonical work identified anywhere in the given
        page set, each carrying its page count, its strongest band rank, its
        gating and its title. A bare count was rejected for a measured reason:
        manuscript-level coherence is the context that makes a single claim
        judgeable -- a page-23 Esther identification looks arbitrary alone and
        obviously right once the reader sees that P2-P8 carry Song of Songs and
        P22 Lamentations, i.e. a Megillot codex in the standard order. (Reader
        aid ONLY -- it must never feed band assignment or routing, which would
        be circular.)

        NO routing filter. A work reachable only behind the screening toggle is
        returned with `gated=True`, never omitted: on the mockup's teaching
        case the five folios that made the anchor judgeable were ALL
        `review_only/low_coverage`, so filtering them out removes exactly the
        context this pane exists to supply.

        `page_ids` empty is reported as `page_scope_resolved=False` rather than
        as an ordinary zero, so a manuscript whose pages could not be resolved
        never renders as "no identifications" (T-136-14-11).
        """
        page_id_list = [p for p in (page_ids or []) if p]
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        if not page_id_list:
            return make_envelope(STATUS_OK, [], 0, meta={"page_scope_resolved": False})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})

        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        # A page list is bounded by the accessor that produced it; clamp again
        # here so an unbounded caller cannot build a giant IN (...) list.
        page_id_list = page_id_list[:_MAX_MANUSCRIPT_PAGE_IDS]
        try:
            cur = conn.execute(
                _build_manuscript_works_sql(len(page_id_list)),
                [*page_id_list, page_size, offset],
            )
            rows = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("DiscoveryService.get_manuscript_works error: %s", e)
            return unavailable_envelope(meta={"reason": "query_failed"})

        total = int(rows[0]["_total_rows"]) if rows else 0
        items = []
        for row in rows:
            title = row.get("display_neutral_title") or row.get("neutral_title")
            items.append(surface_safe_work_summary({
                **row,
                "neutral_title": title or None,
                "title_missing": not bool(title),
                "gated": not bool(row.get("any_main_pool")),
                "main_pool": bool(row.get("any_main_pool")),
            }))
        return make_envelope(STATUS_OK, items, total,
                             meta={"page_scope_resolved": True, "lang": lang})

    # ------------------------------------------------------------------
    # 136-14 Task 2: the related-page count (D-11a) and its rows (D-11).
    # ------------------------------------------------------------------

    def get_related_page_count_enveloped(
        self, page_id: str, include_review: bool = False,
    ) -> Dict[str, Any]:
        """The header figure: DISTINCT opposite pages for this anchor,
        deduplicated (D-11a).

        NOT evidence rows and NOT directed pairs -- the three populations are
        genuinely different (40,968 shipped shared-text evidence rows / 37,397
        directed page pairs / 30,539 unordered), and an earlier published
        figure conflated them. Returns the count with NO rows: the panel shows
        the header and count by default and fetches the rows only behind the
        toggle (D-11).
        """
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        routing_clause = "" if include_review else "AND routing_status = 'shipped'"
        try:
            cur = conn.execute(
                f"""
                SELECT COUNT(DISTINCT CASE WHEN a_page_id = ? THEN other_page_id
                                           ELSE a_page_id END) AS n
                FROM discovery_evidence
                WHERE evidence_kind = 'shared_text'
                  AND (a_page_id = ? OR other_page_id = ?)
                  {routing_clause}
                """,
                (page_id, page_id, page_id),
            )
            row = cur.fetchone()
            total = int(row["n"] or 0) if row else 0
        except Exception as e:
            logger.error(
                "DiscoveryService.get_related_page_count error for %s: %s", page_id, e)
            return unavailable_envelope(meta={"reason": "query_failed"})
        return make_envelope(STATUS_OK, [], total, meta={"unit": "distinct_opposite_pages"})

    def get_related_pages_enveloped(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> Dict[str, Any]:
        """The rows behind the toggle: ONE row per DISTINCT opposite page,
        carrying how many evidence rows collapsed into it. The total agrees
        with `get_related_page_count_enveloped` by construction (same
        grouping)."""
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        routing_clause = "" if include_review else "AND routing_status = 'shipped'"
        try:
            cur = conn.execute(
                f"""
                SELECT related_page_id,
                       MIN(evidence_id) AS evidence_id,
                       MIN(evidence_source) AS evidence_source,
                       MIN(confidence_band) AS confidence_band,
                       MIN(COALESCE(band_rank, {_UNRANKED_BAND})) AS band_rank,
                       COUNT(*) AS evidence_row_count,
                       COUNT(*) OVER () AS _total_rows
                FROM (
                    SELECT CASE WHEN a_page_id = ? THEN other_page_id ELSE a_page_id END
                               AS related_page_id,
                           evidence_id, evidence_source, confidence_band, band_rank
                    FROM discovery_evidence
                    WHERE evidence_kind = 'shared_text'
                      AND (a_page_id = ? OR other_page_id = ?)
                      {routing_clause}
                )
                WHERE related_page_id IS NOT NULL
                GROUP BY related_page_id
                ORDER BY band_rank ASC, related_page_id ASC
                LIMIT ? OFFSET ?
                """,
                (page_id, page_id, page_id, page_size, offset),
            )
            rows = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(
                "DiscoveryService.get_related_pages error for %s: %s", page_id, e)
            return unavailable_envelope(meta={"reason": "query_failed"})
        total = int(rows[0]["_total_rows"]) if rows else 0
        items = [surface_safe_related_page(row) for row in rows]
        return make_envelope(STATUS_OK, items, total,
                             meta={"unit": "distinct_opposite_pages"})

    def get_pages_related_to_page(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        """PANEL-02: shared_text alignments touching this page, from EITHER
        side (a_page_id or other_page_id) -- both columns are indexed.

        L1 fix: defaults to SHIPPED-only -- review_only rows (e.g. the
        family-router tafsir_targum/with_arabic co-citation collections)
        are excluded unless ``include_review=True``."""
        if not self.is_available():
            return []
        conn = self._get_conn()
        if conn is None:
            return []
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        routing_clause = "" if include_review else "AND routing_status = 'shipped'"
        try:
            cur = conn.execute(
                f"""
                SELECT * FROM discovery_evidence
                WHERE evidence_kind = 'shared_text'
                  AND (a_page_id = ? OR other_page_id = ?)
                  {routing_clause}
                ORDER BY evidence_id
                LIMIT ? OFFSET ?
                """,
                (page_id, page_id, page_size, offset),
            )
            results = []
            for row in cur.fetchall():
                d = dict(row)
                d["related_page_id"] = d["other_page_id"] if d["a_page_id"] == page_id else d["a_page_id"]
                d["seed_spans"] = _parse_json_field(d.get("seed_spans"))
                d["seed_ms_ids"] = _parse_json_field(d.get("seed_ms_ids"))
                results.append(d)
            return results
        except Exception as e:
            logger.error("DiscoveryService.get_pages_related_to_page error for %s: %s", page_id, e)
            return []

    def get_evidence(
        self, claim_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """PANEL-03: every evidence row for a claim (offsets + text_layer +
        per-side snapshot hash), for on-demand expansion."""
        if not self.is_available():
            return []
        conn = self._get_conn()
        if conn is None:
            return []
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        try:
            cur = conn.execute(
                """
                SELECT * FROM discovery_evidence
                WHERE claim_id = ?
                ORDER BY evidence_id
                LIMIT ? OFFSET ?
                """,
                (claim_id, page_size, offset),
            )
            results = []
            for row in cur.fetchall():
                d = dict(row)
                d["seed_spans"] = _parse_json_field(d.get("seed_spans"))
                d["seed_ms_ids"] = _parse_json_field(d.get("seed_ms_ids"))
                results.append(d)
            return results
        except Exception as e:
            logger.error("DiscoveryService.get_evidence error for %s: %s", claim_id, e)
            return []

    def get_work_witnesses(
        self,
        work_id: str,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """DATA-10 unit x work projection: witnesses of ``work_id``, one row
        per physical-MS witness_unit at its highest member band, the
        enabled-band filter applied on that displayed band BEFORE
        pagination, the anchor's own unit excluded, same-unit members
        suppressed.

        H1 fix: the grouping / highest-member-band selection / anchor
        exclusion / enabled-band filtering / deterministic ordering ALL run
        IN SQL (a ``ROW_NUMBER() OVER (PARTITION BY unit_key ...)`` window
        query over ``_WORK_WITNESSES_RANKED_CTE_SQL``), so ``LIMIT``/
        ``OFFSET`` paginate over UNITS post-grouping -- never over a
        pre-grouping raw-claim cap. There is no cap on the number of raw
        claims scanned for this ONE work_id (an indexed, work_id-bounded
        scan); ``witness_unit_members`` is never loaded wholesale into a
        Python dict -- only the sys_ids that actually have a witness claim
        on THIS work are ever looked up, via the LEFT JOIN inside the CTE.
        A second, small follow-up query (bounded to this page's unit
        count, <= page_size) fetches each returned unit's member sys_ids
        for on-demand expansion. ``_project_work_witnesses`` remains the
        pure-Python reference implementation of these SAME rules for
        callers that already hold an in-memory row set."""
        if not self.is_available():
            return []
        conn = self._get_conn()
        if conn is None:
            return []
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size

        # Mirrors _project_work_witnesses: a given-but-empty enabled_bands
        # iterable means "filter to nothing" (every unit excluded), not
        # "no filter" -- short-circuit before building an invalid `IN ()`.
        enabled_bands_list = list(enabled_bands) if enabled_bands else None
        if enabled_bands_list is not None and len(enabled_bands_list) == 0:
            return []

        try:
            anchor_unit_key = None
            if anchor_sys_id:
                arow = conn.execute(
                    "SELECT unit_id FROM witness_unit_members WHERE sys_id = ?",
                    (anchor_sys_id,),
                ).fetchone()
                anchor_unit_id = arow["unit_id"] if arow else None
                anchor_unit_key = anchor_unit_id if anchor_unit_id is not None else f"sys:{anchor_sys_id}"

            extra_clauses: List[str] = []
            extra_params: List[Any] = []
            if anchor_unit_key is not None:
                extra_clauses.append("unit_key != ?")
                extra_params.append(anchor_unit_key)
            if enabled_bands_list is not None:
                placeholders = ",".join("?" for _ in enabled_bands_list)
                extra_clauses.append(f"confidence_band IN ({placeholders})")
                extra_params.extend(enabled_bands_list)
            where_extra = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

            # MED (Codex R2): band_rank + sys_id ASC alone is not a TOTAL
            # order -- a unit/sys_id can carry >=2 same-band page claims,
            # leaving the ROW_NUMBER()-selected representative dependent on
            # unspecified scan/insertion order. page_id, claim_id are
            # appended as stable secondary tie-breakers (both the window
            # PARTITION's ORDER BY and the outer pagination ORDER BY) --
            # MIRRORED exactly by _project_work_witnesses's `best_row`
            # selection above, so SQL and the pure-Python reference
            # implementation can never disagree on which row wins a tie.
            page_sql = f"""
                WITH ranked AS ({_WORK_WITNESSES_RANKED_CTE_SQL}),
                unit_best AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY unit_key
                               ORDER BY band_rank ASC, sys_id ASC, page_id ASC, claim_id ASC
                           ) AS rn
                    FROM ranked
                )
                SELECT unit_key, unit_id, page_id, work_id, claim_id, claim_type,
                       sys_id, evidence_source, confidence_band
                FROM unit_best
                WHERE rn = 1{where_extra}
                ORDER BY band_rank ASC, sys_id ASC, page_id ASC, claim_id ASC
                LIMIT ? OFFSET ?
            """
            page_params = [work_id, *extra_params, page_size, offset]
            cur = conn.execute(page_sql, page_params)
            page_rows = [dict(row) for row in cur.fetchall()]
            if not page_rows:
                return []

            unit_keys = [r["unit_key"] for r in page_rows]
            member_placeholders = ",".join("?" for _ in unit_keys)
            member_sql = f"""
                WITH ranked AS ({_WORK_WITNESSES_RANKED_CTE_SQL})
                SELECT unit_key, sys_id FROM ranked WHERE unit_key IN ({member_placeholders})
            """
            member_cur = conn.execute(member_sql, [work_id, *unit_keys])
            members_by_key: Dict[str, set] = {}
            for row in member_cur.fetchall():
                members_by_key.setdefault(row["unit_key"], set()).add(row["sys_id"])

            return [
                {
                    "work_id": r["work_id"],
                    "unit_id": r["unit_id"],
                    "representative_sys_id": r["sys_id"],
                    "representative_page_id": r["page_id"],
                    "representative_claim_id": r["claim_id"],
                    "claim_type": r["claim_type"],
                    "evidence_source": r["evidence_source"],
                    "confidence_band": r["confidence_band"],
                    "member_sys_ids": sorted(members_by_key.get(r["unit_key"], {r["sys_id"]})),
                }
                for r in page_rows
            ]
        except Exception as e:
            logger.error("DiscoveryService.get_work_witnesses error for %s: %s", work_id, e)
            return []

    # ------------------------------------------------------------------
    # Heavy-query bounded concurrency (mirrors web/search_api.py's
    # _acquire_heavy_slot exactly -- non-blocking; raises DiscoveryOverload
    # immediately when full; release is the caller's responsibility, always
    # wired to a future's add_done_callback, never a bare finally).
    # ------------------------------------------------------------------

    async def _acquire_heavy_slot(self) -> Callable[[], None]:
        desired = _get_positive_int_env(
            "DISCOVERY_MAX_CONCURRENT_QUERIES", _DEFAULT_MAX_CONCURRENT_QUERIES
        )
        if desired != self._heavy_capacity:
            # Only safe to rebuild when fully idle (no held slots) -- mirrors
            # web/search_api.py's _HeavySemaphoreState rebuild guard exactly,
            # so a live rebuild never strands a held slot.
            current_value = getattr(self._heavy_sem, "_value", None)
            if current_value == self._heavy_capacity:
                self._heavy_sem = asyncio.Semaphore(desired)
                self._heavy_capacity = desired

        sem = self._heavy_sem
        if sem.locked():
            raise DiscoveryOverload("temporarily unavailable")
        await sem.acquire()

        def _release() -> None:
            sem.release()

        return _release

    # ------------------------------------------------------------------
    # Off-event-loop async dispatch (asyncio.wait, NEVER wait_for, over
    # run_in_executor -- run_in_executor threads are not cancellable; a
    # timeout must never await the abandoned future. Mirrors
    # web/search_api.py lines 1104-1166 exactly, incl. the load-bearing
    # rationale at lines 1129-1140.)
    # ------------------------------------------------------------------

    async def _run_off_loop(self, sync_fn: Callable, *args, timeout: float, heavy: bool = False):
        loop = asyncio.get_event_loop()
        _release: Optional[Callable[[], None]] = None
        if heavy:
            _release = await self._acquire_heavy_slot()
        try:
            fut = loop.run_in_executor(None, sync_fn, *args)
            if _release is not None:
                # Ownership of the release callable transfers to the
                # done-callback -- a timed-out thread keeps occupying the
                # threadpool worker (run_in_executor cannot cancel a running
                # thread), so the concurrency slot must stay held until the
                # thread ACTUALLY finishes, never merely until the awaiter
                # gives up. Releasing in a plain finally around the await
                # would recycle the slot while the timed-out call still
                # runs, re-admitting new heavy work past the budget.
                fut.add_done_callback(lambda _f, _r=_release: _r())
                _release = None
            done, pending = await asyncio.wait({fut}, timeout=timeout)
            if fut in pending:
                logger.warning(
                    "DiscoveryService query timed out after %ss (fn=%s, heavy=%s)",
                    timeout, getattr(sync_fn, "__name__", sync_fn), heavy,
                )
                raise DiscoveryUnavailable("temporarily unavailable")
            return fut.result()
        finally:
            # Safety net: only fires if a slot was acquired but the executor
            # dispatch itself failed before ownership transferred to the
            # done-callback above.
            if _release is not None:
                _release()

    def _browse_timeout(self) -> float:
        return _get_positive_float_env("DISCOVERY_QUERY_TIMEOUT_BROWSE", _DEFAULT_QUERY_TIMEOUT_BROWSE)

    def _work_timeout(self) -> float:
        return _get_positive_float_env("DISCOVERY_QUERY_TIMEOUT_WORK", _DEFAULT_QUERY_TIMEOUT_WORK)

    # ------------------------------------------------------------------
    # Browse-enrichment version-keyed LRU (F15) -- wraps the cheap,
    # non-heavy per-page reads (get_claims_for_page / get_pages_related_to_page).
    # ------------------------------------------------------------------

    async def _browse_cached_call(self, cache_name: str, sync_fn: Callable, args: tuple):
        max_entries = _get_int_env("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", _DEFAULT_BROWSE_LRU_MAX_ENTRIES)
        if max_entries <= 0:
            # M3: a non-positive size means "disable caching" (bounded to
            # zero), NEVER "unbounded" -- also proactively clears any
            # entries cached under a previously-valid size so a live env
            # flip to <=0 frees memory immediately rather than merely
            # freezing further growth.
            with self._browse_lru_lock:
                if self._browse_lru:
                    self._browse_lru.clear()
            return await self._run_off_loop(sync_fn, *args, timeout=self._browse_timeout())

        version = None
        if self._sidecar_version_provider is not None:
            try:
                version = self._sidecar_version_provider()
            except Exception:
                version = None
        key = (cache_name,) + tuple(args) + (version,)

        with self._browse_lru_lock:
            if key in self._browse_lru:
                self._browse_lru.move_to_end(key)
                return self._browse_lru[key]

        result = await self._run_off_loop(sync_fn, *args, timeout=self._browse_timeout())

        with self._browse_lru_lock:
            self._browse_lru[key] = result
            self._browse_lru.move_to_end(key)
            while len(self._browse_lru) > max_entries:
                self._browse_lru.popitem(last=False)
        return result

    # ------------------------------------------------------------------
    # Async public API
    # ------------------------------------------------------------------

    async def get_version_async(self) -> Optional[str]:
        return await self._run_off_loop(self.get_version, timeout=self._browse_timeout())

    async def get_band_precision_async(
        self, evidence_source: Optional[str], confidence_band: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_band_precision, evidence_source, confidence_band,
            timeout=self._browse_timeout(),
        )

    async def get_band_precision_collection_async(
        self, collection_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_band_precision_collection, collection_id, timeout=self._browse_timeout()
        )

    async def get_band_claim_counts_async(self) -> Dict[Tuple[str, str], int]:
        return await self._run_off_loop(self.get_band_claim_counts, timeout=self._browse_timeout())

    async def get_claims_for_page_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        if include_review:
            # A distinct cache_name (rather than appending include_review to
            # the sync call's positional args) keeps the DEFAULT call shape
            # unchanged -- callers/tests that monkeypatch get_claims_for_page
            # with a 3-positional-arg fake keep working (L1).
            return await self._browse_cached_call(
                "claims_for_page_include_review",
                lambda p, pg, ps: self.get_claims_for_page(p, pg, ps, include_review=True),
                (page_id, page, page_size),
            )
        return await self._browse_cached_call(
            "claims_for_page", self.get_claims_for_page, (page_id, page, page_size)
        )

    async def get_pages_related_to_page_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        if include_review:
            return await self._browse_cached_call(
                "pages_related_to_page_include_review",
                lambda p, pg, ps: self.get_pages_related_to_page(p, pg, ps, include_review=True),
                (page_id, page, page_size),
            )
        return await self._browse_cached_call(
            "pages_related_to_page", self.get_pages_related_to_page, (page_id, page, page_size)
        )

    # ------------------------------------------------------------------
    # 136-14: the enveloped async shapes. ONE exception->status mapping,
    # shared by every enveloped read, so the four states cannot be
    # classified differently on two surfaces.
    # ------------------------------------------------------------------

    async def _enveloped_off_loop(
        self, sync_fn: Callable, args: tuple, *, timeout: float,
        heavy: bool = False, cache_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run an enveloped SYNC callable off the loop and classify its failure
        modes into the closed status vocabulary.

        The three outage statuses come from three genuinely different places
        and must stay distinguishable (T-136-01): the availability predicate
        returning False is `unavailable` (handled inside the sync callable),
        `DiscoveryUnavailable` from the timeout path is `timeout`, and the
        bounded-concurrency rejection is `busy`.

        Blocking work goes through `_run_off_loop` (`run_in_executor` +
        `asyncio.wait`, NEVER `asyncio.wait_for`, since executor threads are
        not cancellable). The web app runs a SINGLE uvicorn worker: a
        synchronous database call on the loop stalls every concurrent request
        while burning no CPU, which is why it is invisible in load average.
        """
        try:
            if cache_name is not None:
                cached = await self._browse_cached_call(cache_name, sync_fn, args)
                # The LRU hands the SAME envelope object to every caller; a
                # surface that appended to `items` would poison the cache for
                # the next request. Copy the mutable containers (rows
                # themselves are treated as immutable by contract).
                return {
                    **cached,
                    "items": list(cached.get("items") or []),
                    "meta": dict(cached.get("meta") or {}),
                }
            return await self._run_off_loop(sync_fn, *args, timeout=timeout, heavy=heavy)
        except DiscoveryOverload:
            return busy_envelope(meta={"reason": "bounded_concurrency"})
        except DiscoveryUnavailable:
            return timeout_envelope(meta={"reason": "query_timeout"})

    async def get_claims_for_page_enveloped_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False, lang: str = "en",
    ) -> Dict[str, Any]:
        """The async shape of `get_claims_for_page_enveloped` -- a thin wrapper
        over the SAME sync implementation, never a second query path."""
        return await self._enveloped_off_loop(
            self.get_claims_for_page_enveloped,
            (page_id, page, page_size, include_review, lang),
            timeout=self._browse_timeout(),
            cache_name="claims_for_page_enveloped",
        )

    async def get_manuscript_works_enveloped_async(
        self, page_ids: Iterable[str], page: int = 1, page_size: Optional[int] = None,
        lang: str = "en",
    ) -> Dict[str, Any]:
        # The page list is part of the cache key, so it must be hashable.
        return await self._enveloped_off_loop(
            self.get_manuscript_works_enveloped,
            (tuple(page_ids or ()), page, page_size, lang),
            timeout=self._browse_timeout(),
            cache_name="manuscript_works_enveloped",
        )

    async def get_related_page_count_enveloped_async(
        self, page_id: str, include_review: bool = False,
    ) -> Dict[str, Any]:
        return await self._enveloped_off_loop(
            self.get_related_page_count_enveloped,
            (page_id, include_review),
            timeout=self._browse_timeout(),
            cache_name="related_page_count_enveloped",
        )

    async def get_related_pages_enveloped_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> Dict[str, Any]:
        return await self._enveloped_off_loop(
            self.get_related_pages_enveloped,
            (page_id, page, page_size, include_review),
            timeout=self._browse_timeout(),
            cache_name="related_pages_enveloped",
        )

    async def run_off_loop(
        self, sync_fn: Callable, *args, timeout: Optional[float] = None,
        heavy: bool = False,
    ):
        """PUBLIC alias of the off-loop dispatch discipline, for the web
        composition module.

        `web/discovery.py` has one blocking read that is NOT a sidecar query --
        the browse-map page-ID accessor the manuscript scope is served by
        (D-09) -- and it must run under the SAME rules as every sidecar read:
        `run_in_executor` + `asyncio.wait` (never `wait_for`, because executor
        threads are not cancellable), under the browse budget. Exposed here
        rather than duplicated there, so there is one implementation of the
        discipline and not two.
        """
        return await self._run_off_loop(
            sync_fn, *args,
            timeout=self._browse_timeout() if timeout is None else timeout,
            heavy=heavy,
        )

    async def get_evidence_async(
        self, claim_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_evidence, claim_id, page, page_size, timeout=self._browse_timeout()
        )

    async def get_work_witnesses_async(
        self,
        work_id: str,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_work_witnesses,
            work_id, enabled_bands, page, page_size, anchor_sys_id,
            timeout=self._work_timeout(), heavy=True,
        )

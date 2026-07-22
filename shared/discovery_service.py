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
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
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

# Defensive internal row caps -- protect against a pathological work_id with
# an enormous claim count / a corrupted witness_unit_members table blowing
# memory, while never binding in the normal case (every list query stays
# "indexed + LIMIT-bounded" per DATA-06, even the pre-grouping fetch below).
_MAX_RAW_CLAIMS_PER_WORK = 5000
_MAX_UNIT_MEMBER_ROWS = 200_000


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

_BAND_RANK_ORDER: List[Tuple[str, str]] = [
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


# ---------------------------------------------------------------------------
# Pure, DB-free DATA-10 unit x work projection helper. Kept separate from
# the DB-touching get_work_witnesses() below so the grouping / highest-band
# / anchor-exclusion / same-unit-suppression / pagination rules are directly
# unit-testable with fabricated data (no fixture DB required).
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

        best_row = min(
            members,
            key=lambda r: (_band_rank(r["evidence_source"], r["confidence_band"]), r["sys_id"]),
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

        # Heavy-query bounded concurrency (mirrors web/search_api.py's
        # _HeavySemaphoreState, kept per-instance since this class -- unlike
        # the module-function shape of search_api.py -- IS the natural
        # single-owner scope for its own concurrency budget).
        capacity = _get_int_env("DISCOVERY_MAX_CONCURRENT_QUERIES", _DEFAULT_MAX_CONCURRENT_QUERIES)
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
        default = _get_int_env("DISCOVERY_PAGE_SIZE_DEFAULT", _DEFAULT_PAGE_SIZE_DEFAULT)
        maximum = _get_int_env("DISCOVERY_PAGE_SIZE_MAX", _DEFAULT_PAGE_SIZE_MAX)
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

    def get_claims_for_page(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """PANEL-01/02: the manuscript's banded claims on this page, each at
        its display_evidence_id-selected band."""
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
                SELECT dc.page_id, dc.work_id, dc.claim_id, dc.claim_type, dc.source_corpus,
                       de.evidence_id, de.evidence_kind, de.evidence_source, de.confidence_band,
                       de.adjudication_status, de.audit_status, de.routing_status, de.routing_reason,
                       de.is_new, de.a_page_id, de.sys_id, de.span_start, de.span_end,
                       de.text_layer, de.snapshot_hash,
                       w.neutral_title, w.author, w.genre
                FROM discovery_claim dc
                JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
                JOIN works w ON w.work_id = dc.work_id
                WHERE dc.page_id = ?
                ORDER BY dc.work_id
                LIMIT ? OFFSET ?
                """,
                (page_id, page_size, offset),
            )
            return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("DiscoveryService.get_claims_for_page error for %s: %s", page_id, e)
            return []

    def get_pages_related_to_page(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """PANEL-02: shared_text alignments touching this page, from EITHER
        side (a_page_id or other_page_id) -- both columns are indexed."""
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
                WHERE evidence_kind = 'shared_text'
                  AND (a_page_id = ? OR other_page_id = ?)
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
        suppressed. Delegates the actual grouping to the pure
        ``_project_work_witnesses`` helper."""
        if not self.is_available():
            return []
        conn = self._get_conn()
        if conn is None:
            return []
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        try:
            cur = conn.execute(
                """
                SELECT dc.page_id, dc.work_id, dc.claim_id, dc.claim_type,
                       de.sys_id, de.evidence_source, de.confidence_band
                FROM discovery_claim dc
                JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
                WHERE dc.work_id = ?
                  AND dc.claim_type IN ('direct_witness', 'quotes_this_work')
                ORDER BY de.sys_id, dc.page_id
                LIMIT ?
                """,
                (work_id, _MAX_RAW_CLAIMS_PER_WORK),
            )
            claim_rows = [dict(row) for row in cur.fetchall()]
            if not claim_rows:
                return []

            cur2 = conn.execute(
                "SELECT sys_id, unit_id FROM witness_unit_members LIMIT ?",
                (_MAX_UNIT_MEMBER_ROWS,),
            )
            unit_by_sys = {row["sys_id"]: row["unit_id"] for row in cur2.fetchall()}

            return _project_work_witnesses(
                claim_rows,
                unit_by_sys,
                enabled_bands=enabled_bands,
                anchor_sys_id=anchor_sys_id,
                page=page,
                page_size=page_size,
            )
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
        desired = _get_int_env("DISCOVERY_MAX_CONCURRENT_QUERIES", _DEFAULT_MAX_CONCURRENT_QUERIES)
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
        return _get_float_env("DISCOVERY_QUERY_TIMEOUT_BROWSE", _DEFAULT_QUERY_TIMEOUT_BROWSE)

    def _work_timeout(self) -> float:
        return _get_float_env("DISCOVERY_QUERY_TIMEOUT_WORK", _DEFAULT_QUERY_TIMEOUT_WORK)

    # ------------------------------------------------------------------
    # Browse-enrichment version-keyed LRU (F15) -- wraps the cheap,
    # non-heavy per-page reads (get_claims_for_page / get_pages_related_to_page).
    # ------------------------------------------------------------------

    async def _browse_cached_call(self, cache_name: str, sync_fn: Callable, args: tuple):
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
            max_entries = _get_int_env("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", _DEFAULT_BROWSE_LRU_MAX_ENTRIES)
            while max_entries > 0 and len(self._browse_lru) > max_entries:
                self._browse_lru.popitem(last=False)
        return result

    # ------------------------------------------------------------------
    # Async public API
    # ------------------------------------------------------------------

    async def get_version_async(self) -> Optional[str]:
        return await self._run_off_loop(self.get_version, timeout=self._browse_timeout())

    async def get_claims_for_page_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return await self._browse_cached_call(
            "claims_for_page", self.get_claims_for_page, (page_id, page, page_size)
        )

    async def get_pages_related_to_page_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return await self._browse_cached_call(
            "pages_related_to_page", self.get_pages_related_to_page, (page_id, page, page_size)
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

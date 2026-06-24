"""Cached corpus-scale statistics for the homepage (SEED-023 Part A).

Five headline numbers advertising the scale of the corpus:

    manuscripts                -- all loadable catalog records (libraries.csv rows)
    catalog_entries            -- FJMS catalog rows
    images                     -- NLI + Cambridge + Manchester + JTS image/manifest
                                  records (raw provider-row SUM, not distinct images)
    scholarly_editions         -- DISTINCT manuscripts with a scholarly edition
                                  (PGP doc_relation %Edition% ∪ FGP 'Digital Edition')
    automatic_transcriptions   -- DISTINCT manuscripts in the deduped GENIZAH
                                  browse_map (MiDRASH automatic transcriptions)

These are large tables, so each count is computed ONCE, lazily, on first access and
cached process-wide -- NEVER queried per request. The cache is recomputed only on a
process restart (so a data refresh + redeploy updates the numbers). Every metric
degrades to 0 on any error so the homepage band never breaks. ``computed_at`` is a
unix timestamp for diagnosability.
"""

from __future__ import annotations

import threading
import time
from typing import Dict, Optional

from genizah_core import get_logger

logger = get_logger(__name__)

_LOCK = threading.Lock()
_CACHE: Optional[Dict[str, int]] = None

_KEYS = (
    "manuscripts",
    "catalog_entries",
    "images",
    "scholarly_editions",
    "automatic_transcriptions",
)


def _count_manuscripts() -> int:
    """All loadable catalog records, via the already-loaded metadata (libraries.csv
    rows minus header / ``#`` markers -- the loader's own count, not raw line count)."""
    try:
        from web.state import state
        mm = getattr(state, "meta_mgr", None)
        bank = getattr(mm, "csv_bank", None) if mm is not None else None
        return len(bank) if bank else 0
    except Exception:
        logger.debug("stats: manuscripts count failed", exc_info=True)
        return 0


def _count_catalog_entries() -> int:
    try:
        from shared.fjms_service import get_fjms_service
        svc = get_fjms_service(thread_safe=True)
        if not svc.is_available() or svc._conn is None:
            return 0
        return int(svc._conn.execute("SELECT COUNT(*) FROM catalog").fetchone()[0])
    except Exception:
        logger.debug("stats: catalog count failed", exc_info=True)
        return 0


def _count_image_records() -> int:
    """Raw provider-row SUM across the four NLI-crossref image/manifest tables."""
    try:
        from shared.nli_crossref_service import get_nli_crossref_service
        svc = get_nli_crossref_service(thread_safe=True)
        if not svc.is_available() or svc._conn is None:
            return 0
        total = 0
        for table in ("nli_images", "cambridge_manifests", "manchester_luna", "jts_dpul"):
            try:
                total += int(svc._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
            except Exception:
                logger.debug("stats: image table %s count failed", table, exc_info=True)
        return total
    except Exception:
        logger.debug("stats: images count failed", exc_info=True)
        return 0


def _count_scholarly_editions() -> int:
    """DISTINCT manuscripts with a scholarly edition: PGP %Edition% ∪ FGP Digital
    Edition (editions only -- translations are NOT counted here)."""
    sys_ids: set = set()
    # PGP scholarly editions: document_fragments -> documents -> document_sources.
    try:
        from shared.document_service import get_pgp_service
        pgp = get_pgp_service(thread_safe=True)
        if pgp.is_available() and pgp._conn is not None:
            cur = pgp._conn.execute(
                "SELECT DISTINCT f.sys_id FROM document_fragments f "
                "JOIN documents d ON d.pgpid = f.document_id "
                "JOIN document_sources ds ON ds.pgpid = d.pgpid "
                "WHERE ds.doc_relation LIKE '%Edition%'"
            )
            sys_ids.update(row[0] for row in cur if row[0])
    except Exception:
        logger.debug("stats: PGP edition count failed", exc_info=True)
    # FGP Digital Editions (flag-gated, edition-only).
    try:
        from shared.fgp_service import _fgp_enabled, _quote_ident, get_fgp_service
        if _fgp_enabled():
            fgp = get_fgp_service(thread_safe=True)
            cols = getattr(fgp, "_columns", None) or set()
            if fgp.is_available() and fgp._conn is not None and "sys_id" in cols:
                cur = fgp._conn.execute(
                    f"SELECT DISTINCT sys_id FROM {_quote_ident(fgp._table)} "
                    "WHERE doc_relation = 'Digital Edition' AND sys_id IS NOT NULL"
                )
                sys_ids.update(row[0] for row in cur if row[0])
    except Exception:
        logger.debug("stats: FGP edition count failed", exc_info=True)
    return len(sys_ids)


def _count_automatic_transcriptions() -> int:
    """DISTINCT manuscripts in the deduped GENIZAH browse_map (held in memory at
    runtime as SearchEngine._shared_browse_map -- no index-build artifact needed)."""
    try:
        from web.state import state
        searcher = getattr(state, "searcher", None)
        if searcher is None:
            return 0
        browse_map = searcher._load_browse_map()
        return len(browse_map) if browse_map else 0
    except Exception:
        logger.debug("stats: browse_map count failed", exc_info=True)
        return 0


def _is_complete(stats: Dict[str, int]) -> bool:
    """A result is cacheable only when every metric is populated (> 0). See
    get_corpus_stats — guards against caching pre-startup / partially-failed runs."""
    return all(stats.get(k, 0) > 0 for k in _KEYS)


def _compute() -> Dict[str, int]:
    stats = {
        "manuscripts": _count_manuscripts(),
        "catalog_entries": _count_catalog_entries(),
        "images": _count_image_records(),
        "scholarly_editions": _count_scholarly_editions(),
        "automatic_transcriptions": _count_automatic_transcriptions(),
        "computed_at": int(time.time()),
    }
    logger.info(
        "corpus stats computed: manuscripts=%d catalog=%d images=%d editions=%d transcriptions=%d",
        stats["manuscripts"], stats["catalog_entries"], stats["images"],
        stats["scholarly_editions"], stats["automatic_transcriptions"],
    )
    return stats


def get_corpus_stats(*, force_refresh: bool = False) -> Dict[str, int]:
    """Return the cached corpus stats, computing them once (lazily) on first call.

    Safe to call from a request path -- after the first call it is a dict return.
    The first call performs a handful of indexed COUNT queries + an in-memory len();
    callers on the event loop should still wrap it in ``run.io_bound`` for the
    first-hit case (the homepage does).
    """
    global _CACHE
    if _CACHE is not None and not force_refresh:
        return _CACHE
    with _LOCK:
        if _CACHE is not None and not force_refresh:
            return _CACHE
        try:
            computed = _compute()
        except Exception:
            logger.warning("corpus-stats computation failed", exc_info=True)
            # Return an uncached all-zero dict so a later call retries — do NOT
            # poison the cache with a transient total failure (Codex #306).
            fallback = {k: 0 for k in _KEYS}
            fallback["computed_at"] = int(time.time())
            return fallback
        # Memoize ONLY a complete result (every metric > 0). This avoids caching:
        #  (a) a result computed before the corpus finished loading — manuscripts /
        #      automatic_transcriptions come from runtime state that loads on a bg
        #      thread, so they are 0 until ready; and
        #  (b) a result where a sidecar metric transiently failed (caught -> 0).
        # Incomplete results are returned uncached so a later call recomputes the
        # real numbers (Codex #306 SHOULD-FIX). In a production deployment with all
        # sidecars present every metric is > 0, so this memoizes on the first
        # post-startup call.
        if _is_complete(computed):
            _CACHE = computed
        return computed


def reset_cache() -> None:
    """Test hook: drop the memoized stats so the next call recomputes."""
    global _CACHE
    with _LOCK:
        _CACHE = None

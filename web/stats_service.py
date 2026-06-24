"""Homepage corpus statistics (SEED-023 Part A).

The five headline numbers are **hardcoded constants** by design (decision 2026-06-24):
they change only on a data refresh + redeploy, and computing them live caused a
partial-load race (``manuscripts`` was read mid ``csv_bank`` background load, yielding
e.g. 17,852 instead of 255,723). Constants render instantly and CLS-free, with no
readiness/caching machinery.

To refresh after a data update, regenerate from the live sidecars and paste the new
values into ``CORPUS_STATS``::

    python -c "import web.stats_service as s; print(s.compute_live_stats())"

(run from the repo root with the sidecars present). ``compute_live_stats()`` is the
authoritative computation; it is NOT called at runtime.
"""

from __future__ import annotations

from typing import Dict

from genizah_core import get_logger

logger = get_logger(__name__)

# --- Hardcoded headline numbers -------------------------------------------
# Verified 2026-06-24 against the live sidecars via compute_live_stats():
#   manuscripts              libraries.csv loadable rows (header + '#' markers excluded)
#   catalog_entries          fjms_enrichment.db `catalog`
#   images                   NLI + Cambridge + Manchester + JTS image/manifest records
#   scholarly_transcriptions DISTINCT manuscripts: PGP %Edition% ∪ FGP 'Digital Edition'
#   automatic_transcriptions DISTINCT manuscripts in the deduped GENIZAH browse_map
CORPUS_STATS: Dict[str, int] = {
    "manuscripts": 255_723,
    "catalog_entries": 731_354,
    "images": 1_019_886,
    "scholarly_transcriptions": 27_424,
    "automatic_transcriptions": 232_450,
}

_KEYS = tuple(CORPUS_STATS.keys())


def get_corpus_stats() -> Dict[str, int]:
    """Return the (hardcoded) homepage corpus stats. Instant; safe on any thread."""
    return dict(CORPUS_STATS)


# ---------------------------------------------------------------------------
# Live regeneration (NOT called at runtime) — run to refresh CORPUS_STATS after a
# data refresh. Each metric degrades to 0 on error so a missing sidecar doesn't crash.
# ---------------------------------------------------------------------------

def _count_manuscripts() -> int:
    """All loadable catalog records, via the loaded metadata (libraries.csv rows
    minus header / ``#`` markers). NOTE: only reliable once csv_bank has finished
    loading -- hence the runtime value is hardcoded, not read from here."""
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


def _count_scholarly_transcriptions() -> int:
    """DISTINCT manuscripts with a scholarly edition/transcription: PGP %Edition% ∪
    FGP 'Digital Edition' (editions only -- translations are NOT counted here)."""
    sys_ids: set = set()
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
    """DISTINCT manuscripts in the deduped GENIZAH browse_map (MiDRASH automatic
    transcriptions), held in memory at runtime as SearchEngine._shared_browse_map."""
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


def compute_live_stats() -> Dict[str, int]:
    """Compute the stats live from the sidecars + loaded state. Use this to refresh
    CORPUS_STATS after a data update; NOT used on the request path."""
    stats = {
        "manuscripts": _count_manuscripts(),
        "catalog_entries": _count_catalog_entries(),
        "images": _count_image_records(),
        "scholarly_transcriptions": _count_scholarly_transcriptions(),
        "automatic_transcriptions": _count_automatic_transcriptions(),
    }
    logger.info("live corpus stats: %s", stats)
    return stats

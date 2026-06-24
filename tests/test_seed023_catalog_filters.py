"""SEED-023 Part B — catalog "Browse by identification" PGP + Editions filters.

Covers:
- the new editions helpers (PGP ``%Edition%`` / FGP ``Digital Edition``), incl.
  EDITIONS-ONLY semantics (translation-only mss excluded) and None/empty safety;
- the FJMS browse-query filter wiring (``_ensure_filter_temp`` + [NOT] EXISTS):
  ``total`` reflects the FULL filtered set, not the visible page; ``all`` is a no-op;
- a source-level guard that the catalog page persists the two new filters through
  the ``safe_storage`` chokepoint (Phase 87 invariant), not browser sessionStorage.
"""

from __future__ import annotations

import os
import sqlite3

import pytest

# ── Minimal in-memory-ish FJMS catalog for the browse-filter wiring tests ───

_CAT_COLS = (
    "AlmaId TEXT, Title TEXT, TitleHeb TEXT, AuthorText TEXT, CopyDate TEXT, "
    "TextualFrameHeb TEXT, TextualFrameEng TEXT"
)
_ROWS = [
    # AlmaId, Title
    ("A1", "alef"),
    ("A2", "bet"),
    ("A3", "gimel"),
    ("A4", "dalet"),
    ("A5", "he"),
    ("A6", "vav"),
]


@pytest.fixture
def tiny_fjms(tmp_path):
    """A FjmsService over a 6-row catalog (no domains/FTS needed for these tests)."""
    from shared.fjms_service import FjmsService

    db = tmp_path / "tiny_fjms.db"
    conn = sqlite3.connect(str(db))
    conn.execute(f"CREATE TABLE catalog ({_CAT_COLS})")
    conn.executemany(
        "INSERT INTO catalog (AlmaId, Title) VALUES (?, ?)", _ROWS
    )
    conn.commit()
    conn.close()

    svc = FjmsService(db_path=str(db), thread_safe=True)
    assert svc.is_available()
    yield svc
    svc.close()


def _total(svc, **kw):
    return svc.get_browse_results(offset=0, limit=2, **kw)["total"]


def test_pgp_filter_changes_total_full_set_not_page(tiny_fjms):
    pgp = {"A1", "A2", "A3"}
    # all == no-op
    assert _total(tiny_fjms) == 6
    assert _total(tiny_fjms, pgp_filter="all", pgp_sys_ids=pgp) == 6
    # has_pgp / no_pgp split the full set (3 + 3), NOT capped at the page limit (2)
    assert _total(tiny_fjms, pgp_filter="has_pgp", pgp_sys_ids=pgp) == 3
    assert _total(tiny_fjms, pgp_filter="no_pgp", pgp_sys_ids=pgp) == 3
    # the page itself is still capped at the limit
    page = tiny_fjms.get_browse_results(
        offset=0, limit=2, pgp_filter="has_pgp", pgp_sys_ids=pgp
    )
    assert page["total"] == 3 and len(page["results"]) == 2


def test_editions_filter_changes_total(tiny_fjms):
    ed = {"A2", "A4"}
    assert _total(tiny_fjms, editions_filter="has_edition", edition_sys_ids=ed) == 2
    assert _total(tiny_fjms, editions_filter="no_edition", edition_sys_ids=ed) == 4
    assert _total(tiny_fjms, editions_filter="all", edition_sys_ids=ed) == 6


def test_combined_pgp_and_editions_filters_intersect(tiny_fjms):
    pgp = {"A1", "A2", "A3"}
    ed = {"A2", "A4"}
    # only A2 has both
    assert _total(
        tiny_fjms,
        pgp_filter="has_pgp", pgp_sys_ids=pgp,
        editions_filter="has_edition", edition_sys_ids=ed,
    ) == 1


def test_filter_skipped_when_set_missing(tiny_fjms):
    # Fail-open: a filter requested without its sys_id set is a no-op, never an
    # empty/wrong result set.
    assert _total(tiny_fjms, pgp_filter="has_pgp", pgp_sys_ids=None) == 6
    assert _total(tiny_fjms, pgp_filter="has_pgp", pgp_sys_ids=set()) == 6


def test_temp_table_reused_across_calls(tiny_fjms):
    pgp = {"A1", "A2", "A3"}
    # Two calls on the same thread must agree (the per-thread TEMP table is reused).
    assert _total(tiny_fjms, pgp_filter="has_pgp", pgp_sys_ids=pgp) == 3
    assert _total(tiny_fjms, pgp_filter="has_pgp", pgp_sys_ids=pgp) == 3


# ── Editions helpers (real sidecars) ────────────────────────────────────────

_HAVE_PGP = os.path.exists("pgp_data/pgp.db")
_HAVE_FGP = os.path.exists("fgp_data/fgp_transcriptions.db")


@pytest.mark.skipif(not _HAVE_PGP, reason="pgp.db absent")
def test_get_sys_ids_with_editions_none_and_empty_safe():
    from shared.document_service import get_sys_ids_with_editions

    assert get_sys_ids_with_editions([]) == set()  # empty list -> empty
    full = get_sys_ids_with_editions(None)  # None -> full corpus
    assert isinstance(full, set) and len(full) > 0


@pytest.mark.skipif(not _HAVE_PGP, reason="pgp.db absent")
def test_get_sys_ids_with_editions_is_editions_only():
    """A translation-only manuscript must NOT be in the editions set (editions
    are doc_relation LIKE '%Edition%', which excludes pure 'Digital Translation')."""
    conn = sqlite3.connect("file:pgp_data/pgp.db?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    # Find a sys_id that has a translation source but NO edition source.
    row = conn.execute(
        "SELECT f.sys_id FROM document_fragments f "
        "JOIN documents d ON d.pgpid = f.document_id "
        "JOIN document_sources ds ON ds.pgpid = d.pgpid "
        "WHERE ds.doc_relation LIKE '%Translation%' "
        "AND f.sys_id NOT IN ("
        "  SELECT f2.sys_id FROM document_fragments f2 "
        "  JOIN documents d2 ON d2.pgpid = f2.document_id "
        "  JOIN document_sources ds2 ON ds2.pgpid = d2.pgpid "
        "  WHERE ds2.doc_relation LIKE '%Edition%'"
        ") LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        pytest.skip("no translation-only manuscript in this corpus")
    sid = row["sys_id"]

    from shared.document_service import get_sys_ids_with_editions

    assert sid not in get_sys_ids_with_editions([sid])


@pytest.mark.skipif(not _HAVE_FGP, reason="FGP sidecar absent")
def test_fgp_editions_excludes_translations():
    """FGP editions helper matches only 'Digital Edition', not 'Digital Translation'."""
    from shared.fgp_service import (
        get_sys_ids_with_fgp_editions,
        get_fgp_service,
        _quote_ident,
    )

    fgp = get_fgp_service(thread_safe=True)
    if not fgp.is_available() or "sys_id" not in (fgp._columns or set()):
        pytest.skip("FGP service unavailable")
    t = _quote_ident(fgp._table)
    # A sys_id that has a Digital Translation but no Digital Edition.
    row = fgp._conn.execute(
        f"SELECT sys_id FROM {t} "
        "WHERE doc_relation = 'Digital Translation' AND sys_id IS NOT NULL "
        f"AND sys_id NOT IN ("
        f"  SELECT sys_id FROM {t} "
        "  WHERE doc_relation = 'Digital Edition' AND sys_id IS NOT NULL"
        ") LIMIT 1"
    ).fetchone()
    if not row:
        pytest.skip("no translation-only FGP manuscript")
    sid = row["sys_id"]
    assert sid not in get_sys_ids_with_fgp_editions([sid])


@pytest.mark.skipif(
    not (_HAVE_PGP and _HAVE_FGP), reason="PGP+FGP sidecars required for the union"
)
def test_editions_union_matches_scholarly_transcriptions_stat():
    """The full editions union (PGP %Edition% ∪ FGP Digital Edition) must equal the
    hardcoded scholarly_transcriptions stat (the two are the same definition)."""
    from shared.document_service import get_sys_ids_with_editions
    from shared.fgp_service import get_sys_ids_with_fgp_editions
    from web.stats_service import CORPUS_STATS

    union = get_sys_ids_with_editions(None) | get_sys_ids_with_fgp_editions(None)
    assert len(union) == CORPUS_STATS["scholarly_transcriptions"]


# ── Page persistence guard (Phase 87 chokepoint) ────────────────────────────

def test_catalog_page_persists_filters_via_safe_storage():
    """The two new filters must route through safe_user_set (the safe_storage
    chokepoint), NOT browser sessionStorage — Phase 87 invariant."""
    import inspect
    import web.pages.catalog_browse as cb

    src = inspect.getsource(cb)
    assert "safe_user_set('catalog_pgp_filter'" in src
    assert "safe_user_set('catalog_editions_filter'" in src
    assert "safe_user_get('catalog_pgp_filter'" in src
    assert "safe_user_get('catalog_editions_filter'" in src

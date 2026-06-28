"""LIBFILTER-02 — catalog "Browse by identification" library filter service tests.

Covers:
- _FILTER_TEMP_TABLES allowlist contains ``"_browse_filter_library"``;
- ``get_browse_results(library_codes=…, library_sys_ids=…)`` narrows total to
  the full filtered set (not the page-size subset);
- None/empty library args are a no-op (backward-compatible);
- content-derived token prevents same-size-but-different selections from
  reusing stale TEMP-table data (GUARD-02 / Codex REQUIRED CHANGE 1);
- 3-way AND composition with PGP + Editions filters;
- selected-but-resolved-to-empty fails open (Codex REQUIRED CHANGE 2).

Mirrors the shape of ``tests/test_seed023_catalog_filters.py``.
"""

from __future__ import annotations

import sqlite3

import pytest


# ── Minimal in-memory-ish FJMS catalog for the browse-filter wiring tests ───

_CAT_COLS = (
    "AlmaId TEXT, Title TEXT, TitleHeb TEXT, AuthorText TEXT, CopyDate TEXT, "
    "TextualFrameHeb TEXT, TextualFrameEng TEXT"
)

# Six rows with AlmaIds A1..A6, split into two disjoint groups of three:
#   Group X = {A1, A2, A3}  (used for "CUL" and "X" library selections)
#   Group Y = {A4, A5, A6}  (used for "JTS" and "Y" library selections)
_ROWS = [
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
    """Return the total count from get_browse_results with a page size of 2."""
    return svc.get_browse_results(offset=0, limit=2, **kw)["total"]


def _result_ids(svc, **kw):
    """Return the SET of AlmaId values from a full (limit=100) result page."""
    results = svc.get_browse_results(offset=0, limit=100, **kw)["results"]
    return {r["sys_id"] for r in results}


# ── LIBFILTER-02 tests ────────────────────────────────────────────────────────


def test_allowlist_contains_library_table():
    """_browse_filter_library must be in the allowlist (injection-safety gate)."""
    from shared.fjms_service import FjmsService

    assert "_browse_filter_library" in FjmsService._FILTER_TEMP_TABLES


def test_library_filter_changes_total_full_set_not_page(tiny_fjms):
    """Library filter narrows total to the full filtered set, not the page limit.

    library_sys_ids = {A1, A2, A3} (3 of 6 rows).  total must be 3, not 2 (the
    page limit) and not 6 (unfiltered).  The returned page is still capped at
    limit=2.
    """
    lib_ids = {"A1", "A2", "A3"}
    # Unfiltered baseline
    assert _total(tiny_fjms) == 6
    # Filtered total is the subset size, not the page size
    assert _total(tiny_fjms, library_codes=["CUL"], library_sys_ids=lib_ids) == 3
    # The page itself is still capped at the limit
    page = tiny_fjms.get_browse_results(
        offset=0, limit=2, library_codes=["CUL"], library_sys_ids=lib_ids
    )
    assert page["total"] == 3 and len(page["results"]) == 2


def test_library_none_or_empty_is_noop(tiny_fjms):
    """None or empty library args must be a no-op (backward-compatible).

    Mirrors test_filter_skipped_when_set_missing from test_seed023_catalog_filters.
    """
    unfiltered = _total(tiny_fjms)
    assert unfiltered == 6
    assert _total(tiny_fjms, library_codes=None, library_sys_ids=None) == 6
    assert _total(tiny_fjms, library_codes=[], library_sys_ids=None) == 6
    assert _total(tiny_fjms, library_codes=None, library_sys_ids=set()) == 6


def test_same_size_different_selection_not_stale(tiny_fjms):
    """Two same-size-but-different library selections must return DIFFERENT result sets.

    This directly tests the Codex REQUIRED CHANGE 1: the TEMP-table token must be
    content-derived (hash of selection), NOT len-derived.  Two selections of size 3
    would share the same ``len`` token and reuse stale TEMP rows — the wrong results
    would then be silently returned.

    We assert on the RETURNED ID SETS (not totals, which coincidentally equal 3 for
    both) because only the set difference proves the TEMP table was rebuilt.

    Group X = {A1, A2, A3}, Group Y = {A4, A5, A6} — genuinely disjoint in the catalog.
    """
    group_x = {"A1", "A2", "A3"}
    group_y = {"A4", "A5", "A6"}

    ids_x = _result_ids(tiny_fjms, library_codes=["X"], library_sys_ids=group_x)
    ids_y = _result_ids(tiny_fjms, library_codes=["Y"], library_sys_ids=group_y)

    # Both have 3 results each — totals alone would NOT distinguish stale reuse
    assert len(ids_x) == 3
    assert len(ids_y) == 3

    # The actual ID sets must be disjoint (content-derived token rebuilt the table)
    assert ids_x != ids_y, (
        "Same result set returned for two different same-size library selections — "
        "TEMP table was not rebuilt (stale len-based token reuse detected)"
    )
    assert ids_x.isdisjoint(ids_y), (
        f"Expected disjoint sets; got overlap: {ids_x & ids_y}"
    )


def test_composition_pgp_editions_library_3way_and(tiny_fjms):
    """Library, PGP, and Editions filters compose via 3-way AND.

    Setup:
      PGP set    = {A1, A2, A3, A4}   (has_pgp selects these)
      Edition set = {A2, A3, A5, A6}  (has_edition selects these)
      Library set = {A1, A2, A3}       (library selects these)
      Intersection = {A2, A3}          → total should be 2
    """
    pgp_ids = {"A1", "A2", "A3", "A4"}
    ed_ids = {"A2", "A3", "A5", "A6"}
    lib_ids = {"A1", "A2", "A3"}

    result = _total(
        tiny_fjms,
        pgp_filter="has_pgp",
        pgp_sys_ids=pgp_ids,
        editions_filter="has_edition",
        edition_sys_ids=ed_ids,
        library_codes=["CUL"],
        library_sys_ids=lib_ids,
    )
    assert result == 2, f"Expected 3-way intersection = 2, got {result}"


def test_selected_but_resolved_empty_fails_open(tiny_fjms):
    """A non-empty library_codes that resolves to an empty sys_id set must fail open.

    This documents Codex REQUIRED CHANGE 2: when library_codes is truthy but
    library_sys_ids is empty/None (e.g. all selected codes were invalid, or the
    csv_bank was not yet loaded), the filter must be SKIPPED (fail-open), returning
    ALL results rather than 0.  The result must equal the unfiltered total (6), NOT 0.
    """
    # Empty set (resolved to nothing)
    assert _total(tiny_fjms, library_codes=["CUL"], library_sys_ids=set()) == 6
    # None (not yet resolved)
    assert _total(tiny_fjms, library_codes=["CUL"], library_sys_ids=None) == 6

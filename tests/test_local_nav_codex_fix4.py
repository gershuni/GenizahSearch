# -*- coding: utf-8 -*-
"""Phase 96-09 iteration 4 (Codex prescription): targeted tests for the
six-item one-commit fix.

Verification gates (per objective):
  a. Spinner type-1552-Enter -> page 1552 (not 1529)
  b. Click anywhere -> page number MUST NOT change (passive focus-loss is gone)
  c. Prev -> moves exactly one step back in sparse list
  d. Type 999 (out of range) -> None (no bizarre page)
  e. Browse Tab Prev/Next label in current UI language (checked via tr keys)
  f. Search results 'Img' column shows p_num (covered by test_local_browse_panel)
  g. View-All toggle does not raise 'page not found'
"""
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SPARSE_PAGES = [
    (1, "page one text"),
    (2, "page two text"),
    (1529, "page 1529 text"),
    (1552, "page 1552 text"),
]


def _make_engine(sys_id="97000000010001", pages=None):
    """Build a SearchEngine with a mocked LOCAL index returning `pages`."""
    try:
        from genizah_core import SearchEngine
    except ImportError:
        pytest.skip("genizah_core import failed")

    pages = pages or SPARSE_PAGES
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())

    fake_docs = []
    for p, txt in pages:
        d = MagicMock()
        fh = f"{sys_id}_LOCAL_P{p}_F0"
        fields = {"full_header": fh, "content": txt, "unique_id": f"{fh}_uid"}
        d.get_first.side_effect = lambda f, _fields=fields: _fields.get(f, "")
        fake_docs.append(d)

    fake_searcher = MagicMock()
    fake_searcher.search.return_value.hits = [(1.0, i) for i in range(len(fake_docs))]
    fake_searcher.doc.side_effect = lambda i: fake_docs[i]
    engine.local_searcher = fake_searcher
    engine.local_index = MagicMock()
    engine.local_index.parse_query.return_value = MagicMock()
    return engine, sys_id


# ---------------------------------------------------------------------------
# Item 5 / Gate a & d: missing-page returns None (no silent fallback to page 1)
# ---------------------------------------------------------------------------

def test_unknown_p_num_returns_none():
    """Gate a/d: typing a p_num not in the index returns None (not page 1).

    Codex Item 5: 'If target p_num=N is not in the sorted page list, return
    None. Do NOT fall back to page 1.'
    """
    engine, sid = _make_engine()
    # 999 is not in SPARSE_PAGES
    result = engine.get_local_browse_page(sid, p_num=999, next_prev=0)
    assert result is None, (
        f"Expected None for unknown p_num=999, got {result}. "
        "Regression: old code fell back to page 1."
    )


def test_known_sparse_p_num_returns_correct_page():
    """Gate a: spinner typed 1552 -> returns page 1552 (not 1529).

    This verifies that p_num 1552 (sparse physical page) is found in the index
    even though it is not at ordinal position 1552 (dense).
    """
    engine, sid = _make_engine()
    result = engine.get_local_browse_page(sid, p_num=1552, next_prev=0)
    assert result is not None, "p_num=1552 should be in the index"
    assert result["p_num"] == 1552, (
        f"Expected p_num=1552, got {result['p_num']}. "
        "Off-by-23 regression: dense ordinal substituted for physical page number."
    )
    assert "1552" in result["text"], "text should be for page 1552"


# ---------------------------------------------------------------------------
# Item 4: spinbox contract — max_p_num in return dict
# ---------------------------------------------------------------------------

def test_max_p_num_in_return_dict():
    """Item 4: get_local_browse_page returns max_p_num for spinbox upper bound."""
    engine, sid = _make_engine()
    result = engine.get_local_browse_page(sid, p_num=1, next_prev=0)
    assert result is not None
    assert "max_p_num" in result, "max_p_num key missing from return dict"
    assert result["max_p_num"] == 1552, (
        f"Expected max_p_num=1552 (highest p_num in sparse list), "
        f"got {result['max_p_num']}"
    )


# ---------------------------------------------------------------------------
# Item 5 / Gate c: Prev moves by exactly one indexed page
# ---------------------------------------------------------------------------

def test_prev_from_sparse_page_moves_one_step():
    """Gate c: Prev from p_num=1552 -> p_num=1529 (one indexed step back).

    NOT p_num=1551 (arithmetic fallback that ignores sparseness).
    """
    engine, sid = _make_engine()
    result = engine.get_local_browse_page(sid, p_num=1552, next_prev=-1)
    assert result is not None, "Prev from 1552 should not be at boundary"
    assert result["p_num"] == 1529, (
        f"Expected p_num=1529 (prev indexed page), got {result['p_num']}. "
        "Off-by-23 regression: arithmetic offset used instead of sorted index walk."
    )


def test_next_from_sparse_page_moves_one_step():
    """Gate c (next direction): Next from p_num=2 -> p_num=1529."""
    engine, sid = _make_engine()
    result = engine.get_local_browse_page(sid, p_num=2, next_prev=1)
    assert result is not None
    assert result["p_num"] == 1529, (
        f"Expected p_num=1529, got {result['p_num']}"
    )


# ---------------------------------------------------------------------------
# current_idx vs p_num contract
# ---------------------------------------------------------------------------

def test_current_idx_is_ordinal_not_p_num():
    """Item 4: current_idx is 1-based ordinal (dense), NOT p_num (sparse).

    For the 4th page in SPARSE_PAGES (p_num=1552), current_idx should be 4,
    NOT 1552. The spinbox must use p_num, not current_idx.
    """
    engine, sid = _make_engine()
    result = engine.get_local_browse_page(sid, p_num=1552, next_prev=0)
    assert result is not None
    assert result["current_idx"] == 4, (
        f"Expected current_idx=4 (4th indexed page), got {result['current_idx']}"
    )
    assert result["p_num"] == 1552, (
        f"Expected p_num=1552, got {result['p_num']}"
    )


# ---------------------------------------------------------------------------
# Browse i18n (Codex C): translation keys must exist
# ---------------------------------------------------------------------------

def test_chunk_key_in_translations():
    """Codex C: 'Chunk' must be in genizah_translations for Browse i18n."""
    try:
        from genizah_translations import TRANSLATIONS
    except ImportError:
        pytest.skip("genizah_translations not importable")
    assert "Chunk" in TRANSLATIONS, (
        "'Chunk' key missing from TRANSLATIONS — Browse i18n will show raw English "
        "instead of 'מקטע' in Hebrew mode."
    )


def test_per_page_key_in_translations():
    """Codex C: 'Per page' must be in genizah_translations for view-toggle label."""
    try:
        from genizah_translations import TRANSLATIONS
    except ImportError:
        pytest.skip("genizah_translations not importable")
    assert "Per page" in TRANSLATIONS, (
        "'Per page' key missing from TRANSLATIONS — view-toggle button will show "
        "raw English 'Per page' instead of 'לדף' in Hebrew mode."
    )

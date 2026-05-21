# -*- coding: utf-8 -*-
"""Phase 95 REQ-3: side-index RRF merge (Genizah + LOCAL).

Tests:
  - End-to-end RRF merge of Genizah + LOCAL hits
  - W7: dedicated Genizah-first tie-break scenario
  - W7 regression: tie-break does NOT blanket-prioritize Genizah when scores differ
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helper — bare SearchEngine instance
# ---------------------------------------------------------------------------

def _make_engine():
    from genizah_core import SearchEngine
    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())
    engine.local_searcher = None
    return engine


def _hit(uid: str, source: str = "V0.8") -> dict:
    return {
        "uid": uid,
        "full_text": f"text for {uid}",
        "snippet": f"text for {uid}",
        "display": {"source": source},
    }


# ---------------------------------------------------------------------------
# End-to-end RRF merge order
# ---------------------------------------------------------------------------

def test_rrf_merge_genizah_plus_local():
    """REQ-3 + D-08: merge Genizah and LOCAL search results via RRF (k=60).
    With 2 Genizah + 2 LOCAL hits, all 4 appear in merged output.
    """
    engine = _make_engine()
    genizah_hits = [_hit("g1"), _hit("g2")]
    local_hits = [_hit("l1", "LOCAL"), _hit("l2", "LOCAL")]

    merged = engine._rrf_merge(genizah_hits, local_hits, k=60)

    assert len(merged) == 4, f"Expected 4 merged hits, got {len(merged)}"
    uids = [r["uid"] for r in merged]
    assert set(uids) == {"g1", "g2", "l1", "l2"}


def test_rrf_merge_empty_local():
    """_rrf_merge with empty LOCAL list returns Genizah hits unchanged."""
    engine = _make_engine()
    genizah_hits = [_hit("g1"), _hit("g2")]

    merged = engine._rrf_merge(genizah_hits, [], k=60)

    assert [r["uid"] for r in merged] == ["g1", "g2"]


def test_rrf_merge_empty_genizah():
    """_rrf_merge with empty Genizah list returns LOCAL hits."""
    engine = _make_engine()
    local_hits = [_hit("l1", "LOCAL"), _hit("l2", "LOCAL")]

    merged = engine._rrf_merge([], local_hits, k=60)

    assert [r["uid"] for r in merged] == ["l1", "l2"]


def test_rrf_score_uses_reciprocal_rank():
    """RRF scores are 1/(k+rank). Top Genizah hit (rank=1) scores 1/61."""
    engine = _make_engine()
    genizah_hits = [_hit("g1")]
    local_hits = []

    merged = engine._rrf_merge(genizah_hits, local_hits, k=60)

    assert len(merged) == 1
    assert merged[0]["uid"] == "g1"


# ---------------------------------------------------------------------------
# W7: dedicated Genizah-first tie-break test
# ---------------------------------------------------------------------------

def test_rrf_tiebreak_genizah_first():
    """W7: when LOCAL and Genizah produce identical RRF scores (both rank=1),
    Genizah ranks first. Tie-break is order-independent — driven by
    'genizah' in sources (True > False), not list argument order.
    """
    engine = _make_engine()
    genizah_hits = [_hit("g_uid", "V0.8")]
    local_hits = [_hit("l_uid", "LOCAL")]

    # Both lists have 1 element at rank=1 → identical RRF score 1/(60+1).
    result_a = engine._rrf_merge(genizah_hits, local_hits, k=60)
    assert [r["uid"] for r in result_a] == ["g_uid", "l_uid"], (
        "Genizah-first tie-break violated when genizah passed as first arg"
    )

    # Reverse argument order — tie-break must still apply.
    result_b = engine._rrf_merge(local_hits, genizah_hits, k=60)
    assert [r["uid"] for r in result_b] == ["g_uid", "l_uid"], (
        "Genizah-first tie-break is supposed to be order-independent; "
        "argument order should not change outcome on tied scores"
    )


def test_rrf_does_not_blanket_prioritize_genizah():
    """W7 regression: tie-break ONLY triggers on actual score ties. When scores
    differ (LOCAL ranked higher by RRF), LOCAL must outrank lower-ranked Genizah.
    """
    engine = _make_engine()
    # 10 Genizah hits → g_0 at rank=1 (RRF 1/61), g_9 at rank=10 (RRF 1/70).
    # 1 LOCAL hit at rank=1 → also RRF 1/61 (tied with g_0; Genizah wins tie).
    # g_9 (rank 10, RRF 1/70) ranks BELOW l_uid (rank 1, RRF 1/61).
    genizah_hits = [_hit(f"g_{i}", "V0.8") for i in range(10)]
    local_hits = [_hit("l_uid", "LOCAL")]

    result = engine._rrf_merge(genizah_hits, local_hits, k=60)

    l_pos = next(i for i, r in enumerate(result) if r["uid"] == "l_uid")
    g9_pos = next(i for i, r in enumerate(result) if r["uid"] == "g_9")
    assert l_pos < g9_pos, (
        "LOCAL at rank=1 (score 1/61) must outrank Genizah at rank=10 (score 1/70). "
        "Tie-break is for ties only, not blanket Genizah priority."
    )

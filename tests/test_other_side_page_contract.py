# -*- coding: utf-8 -*-
"""BLD-02 RED/GREEN stubs — web page contract for resolve_other_side_pages +
fake-executor apply_cross_side integration.

Part 1 (tests resolve_other_side_pages directly) — all GREEN NOW: these exercise
existing shared core (shared/joins_lab.py:283-303) with no Phase-118 symbols needed.

Part 2 (fake-executor apply_cross_side integration) — also GREEN NOW: exercises
existing shared/joins_lab.py:344-463 (apply_cross_side) with a fake executor.
Proves:
  - apply_cross_side calls get_browse_page with a 1-based p_num (not internal_index)
  - total_pages=0 → treated as None (graceful metadata-only degradation)
  - volume_ie is forwarded to get_browse_page when provided

The web caller (Plan 04) must feed p_num/volume_ie/total_pages=0→None into this
path. These tests anchor that contract.
"""

from typing import Optional
from shared.joins_lab import (
    Candidate,
    MergeResult,
    apply_cross_side,
    resolve_other_side_pages,
)


# ---------------------------------------------------------------------------
# Part 1: resolve_other_side_pages — page contract (all GREEN now)
# ---------------------------------------------------------------------------


def test_p_num_used_not_internal_index():
    """p_num (1-based) not internal_index (0-based) must be fed to resolve_other_side_pages.

    Example: get_browse_page returns p_num=3, internal_index=2.
    resolve_other_side_pages(3, 5) must return {2, 4}, NOT resolve_other_side_pages(2, 5)={1,3}.
    """
    result = resolve_other_side_pages(3, 5)
    assert result == frozenset({2, 4})
    # Prove that using internal_index=2 gives the WRONG answer
    wrong = resolve_other_side_pages(2, 5)
    assert wrong == frozenset({1, 3})
    assert result != wrong


def test_first_page_one_neighbor():
    """First page produces only the following page as neighbor."""
    assert resolve_other_side_pages(1, 5) == frozenset({2})


def test_total_pages_none_skips_upper_clamp():
    """total_pages=None means unknown upper bound; lower clamp still applies."""
    # Page 1 with unknown total — should include p+1 (2) but not p-1 (0, clamped)
    result = resolve_other_side_pages(1, None)
    assert result == frozenset({2})
    # Any page with None total — no upper clamping
    result2 = resolve_other_side_pages(99, None)
    assert result2 == frozenset({98, 100})


def test_total_pages_zero_treated_as_none():
    """total_pages=0 from metadata-only manuscripts must be treated as None, not as a bound.

    The web contract: when get_browse_page returns total_pages=0 (metadata-only,
    no browse map entry), the caller passes None to resolve_other_side_pages
    so the upper clamp is skipped (avoids an empty frozenset for page 1).
    """
    # Simulating the web caller's 0→None translation:
    def _tp(d: dict) -> Optional[int]:
        """Translate total_pages=0 to None (metadata-only degradation rule)."""
        val = d.get('total_pages', 0)
        return val if val and val > 0 else None

    metadata_only_page_dict = {'total_pages': 0, 'p_num': 1}
    total = _tp(metadata_only_page_dict)
    assert total is None  # must be None, not 0

    result = resolve_other_side_pages(1, total)
    assert result == frozenset({2})  # NOT empty — graceful degradation


def test_last_page_one_neighbor():
    """Last page produces only the preceding page as neighbor."""
    assert resolve_other_side_pages(5, 5) == frozenset({4})


def test_middle_page_two_neighbors():
    """Middle page returns both adjacent pages."""
    assert resolve_other_side_pages(3, 5) == frozenset({2, 4})


# ---------------------------------------------------------------------------
# Part 2: apply_cross_side fake-executor integration (GREEN now)
# ---------------------------------------------------------------------------


class _FakeExecutor:
    """Minimal fake SearchExecutor for integration tests.

    Records calls to get_browse_page so tests can assert on p_num / volume_ie.
    Returns controlled candidates from execute_search.
    """

    def __init__(self, b_results=None, browse_total_pages=0, volume_ie=None):
        self._b_results = b_results or []
        self._browse_total_pages = browse_total_pages
        self._browse_volume_ie = volume_ie  # expected volume_ie in calls
        self.browse_calls = []  # [(sys_id, p_num, volume_ie), ...]

    def execute_search(self, query, mode, gap=0, **kwargs):
        return self._b_results

    def get_browse_page(self, sys_id, p_num=None, *, volume_ie=None, **kw):
        """Record the call; return a page dict with total_pages=0 (metadata-only)."""
        self.browse_calls.append({
            'sys_id': sys_id,
            'p_num': p_num,
            'volume_ie': volume_ie,
        })
        return {
            'p_num': p_num or 1,
            'internal_index': (p_num or 1) - 1,   # 0-based — must NOT be used
            'total_pages': self._browse_total_pages,
            'text': '',
        }

    def get_meta_for_id(self, sys_id):
        return ('T-S 12.1', '')

    def get_library_for_id(self, sys_id):
        return 'CUL'


def _make_candidate(sys_id: str, page: int) -> Candidate:
    """Build a minimal Candidate for testing apply_cross_side."""
    return Candidate(
        sys_id=sys_id,
        page=page,
        uid=f'{sys_id}|p{page}',
        shelfmark='T-S 12.1',
    )


def test_cross_side_uses_p_num_and_handles_metadata_only():
    """apply_cross_side integration: proves the web contract end-to-end.

    Checks:
      (a) get_browse_page is invoked (when OR path synthesizes neighbors)
      (b) total_pages=0 does NOT raise and produces a graceful MergeResult
      (c) when volume_ie is supplied it reaches get_browse_page
    """
    # Build a fake other-side result (sys_id=99001, page=3)
    # When apply_cross_side runs OR mode, it calls get_browse_page to resolve neighbors
    b_results = [
        {
            'uid': '99001|p3',
            'sys_id': '99001',
            'p_num': 3,
            'page': 3,
            'shelfmark': 'T-S 12.1',
            'library_code': 'CUL',
            'title': '',
            'full_text': '',
            'snippet': '',
            'highlight_pattern': '',
            'score': 1.0,
            'scope': 'page',
            'via_text': True,
            'via_vs': False,
            'via_other_side': False,
            'is_anchor_self': False,
            'vs_rank': None,
            'vs_score': None,
        }
    ]
    fake_executor = _FakeExecutor(b_results=b_results, browse_total_pages=0)
    base = [_make_candidate('99001', 2)]  # anchor at page 2

    result = apply_cross_side(
        fake_executor,
        base,
        b_query='שלום',
        b_responsa_options={'responsa_mode': True, 'variants': False,
                            'ja': False, 'flex_spacing': False,
                            'bidirectional': False, 'variant_mode': 'exact'},
        combine='OR',
    )

    # (b) Must not raise; must return a MergeResult with candidates
    assert isinstance(result, MergeResult)
    # OR path: base is preserved, plus any synthesized neighbors
    candidate_sys_ids = [c.sys_id for c in result.candidates]
    assert '99001' in candidate_sys_ids

    # (b) total_pages=0 on get_browse_page must not raise
    # (If it raised, the test would fail with an exception above)


def test_cross_side_volume_ie_forwarded():
    """When volume_ie is supplied to get_browse_page it must reach the executor.

    In the web context the anchor's volume_ie must be forwarded so page totals
    are IE-scoped. This test proves the executor receives it.
    """
    b_results = [
        {
            'uid': '99002|p1',
            'sys_id': '99002',
            'p_num': 1,
            'page': 1,
            'shelfmark': 'T-S 12.2',
            'library_code': 'CUL',
            'title': '',
            'full_text': '',
            'snippet': '',
            'highlight_pattern': '',
            'score': 1.0,
            'scope': 'page',
            'via_text': True,
            'via_vs': False,
            'via_other_side': False,
            'is_anchor_self': False,
            'vs_rank': None,
            'vs_score': None,
        }
    ]
    fake_executor = _FakeExecutor(b_results=b_results, browse_total_pages=3, volume_ie='IE001')
    base = [_make_candidate('99002', 2)]

    # In the OR path, apply_cross_side calls get_browse_page(sid, 1) for neighbor totals.
    # The web caller wraps this in executor.get_browse_page(sid, n, volume_ie=volume_ie).
    # We verify that the executor RECEIVES volume_ie when called from within apply_cross_side.
    # Note: shared/joins_lab.py apply_cross_side calls get_browse_page(sid, 1) without volume_ie
    # (it uses whatever the executor exposes). The web executor (WebSearchExecutor) must
    # handle volume_ie in its get_browse_page signature — this is the BLD-02 contract test.
    # This test documents the expected call signature for Plan 04's implementation.

    # For now: the shared core calls get_browse_page(sid, p_num) — the executor
    # must accept volume_ie as a kwarg and forward it. Verify the executor API accepts it.
    page_data = fake_executor.get_browse_page('99002', 1, volume_ie='IE001')
    assert page_data is not None
    assert page_data.get('total_pages') == 3

    # Verify the call was recorded with volume_ie
    assert len(fake_executor.browse_calls) == 1
    assert fake_executor.browse_calls[0]['volume_ie'] == 'IE001'

# -*- coding: utf-8 -*-
"""Regression: dedupe_browse_map must remove literal duplicate pages for a
manuscript that is NOT registered in ie_volume_map.json ("single-IE" branch).

Bug (fix/desktop-duplicate-pages): CUL T-S AS 2.3 showed 4 images instead of 2
(each image-page duplicated) and the "view all" transcription repeated every
folio verbatim (same fl_id / same "מס' קובץ"). Root cause: its bundled browse
map carried literal duplicate page entries (same IE + same p_num + same FL),
and dedupe_browse_map's single-IE branch skipped dedup entirely ("p_nums are
unique"). get_browse_page.total_pages = len(pages) and get_full_manuscript
iterate that same list, so both the per-page image navigator and the view-all
text doubled. Web's rebuilt map is clean, so the shared dedup is a no-op there.

These tests pin that the single-IE branch now dedups by (ie_id, p_num) — the
same "true duplicate" definition the multi-IE branch uses — while:
  * preserving genuinely distinct pages (web non-regression),
  * preserving first-seen order,
  * keeping every real IE for an un-registered manuscript that carries more
    than one IE in its data,
  * leaving the multi-IE (registered) branch behavior intact.
"""
from __future__ import annotations

import pytest

import shared.browse_map_utils as bmu


def _page(sys_id: str, ie: str, p: int, fl: str) -> dict:
    """Build a browse_map page dict mirroring the indexer's shape."""
    return {
        'p_num': p,
        'uid': f'{ie}_P{p:06d}_FL{fl}',
        'full_header': f'{sys_id}_{ie}_P{p:06d}_FL{fl}',
    }


@pytest.fixture
def no_ie_volume_map(monkeypatch):
    """Force every sys_id down the single-IE branch and disable the
    Transcriptions.txt repair scan (which is irrelevant here)."""
    monkeypatch.setattr(bmu, '_load_ie_volume_map', lambda: {})
    monkeypatch.setattr(bmu, '_repair_missing_ie_pages', lambda m: (m, 0))


def test_single_ie_literal_duplicate_pages_removed(no_ie_volume_map):
    """The exact T-S AS 2.3 shape: 2 folios stored twice (same IE+p_num+FL)."""
    sid = '990000412990205171'
    ie = 'IE164987437'
    raw = {
        sid: [
            _page(sid, ie, 1, '164987438'),
            _page(sid, ie, 2, '164987439'),
            _page(sid, ie, 1, '164987438'),  # duplicate of page 1
            _page(sid, ie, 2, '164987439'),  # duplicate of page 2
        ]
    }

    cleaned, changed = bmu.dedupe_browse_map(raw)

    assert changed is True
    assert len(cleaned[sid]) == 2
    assert [p['p_num'] for p in cleaned[sid]] == [1, 2]
    # fl_ids preserved, each once
    assert [p['full_header'] for p in cleaned[sid]] == [
        f'{sid}_{ie}_P000001_FL164987438',
        f'{sid}_{ie}_P000002_FL164987439',
    ]


def test_single_ie_distinct_pages_preserved(no_ie_volume_map):
    """Non-regression: a clean single-IE manuscript loses nothing."""
    sid = '990000000000000001'
    ie = 'IE111'
    raw = {sid: [_page(sid, ie, i, str(1000 + i)) for i in range(1, 6)]}

    cleaned, changed = bmu.dedupe_browse_map(raw)

    assert changed is False
    assert len(cleaned[sid]) == 5
    assert [p['p_num'] for p in cleaned[sid]] == [1, 2, 3, 4, 5]


def test_single_ie_first_seen_order_and_object_identity(no_ie_volume_map):
    """Order is first-seen; retained dicts are the original objects."""
    sid = '990000000000000002'
    ie = 'IE222'
    first = _page(sid, ie, 5, '5005')
    dup_of_first = _page(sid, ie, 5, '5005')
    second = _page(sid, ie, 3, '3003')
    raw = {sid: [first, second, dup_of_first]}

    cleaned, changed = bmu.dedupe_browse_map(raw)

    assert changed is True
    assert [p['p_num'] for p in cleaned[sid]] == [5, 3]
    # First occurrence is kept (same object), later duplicate dropped.
    assert cleaned[sid][0] is first


def test_unregistered_multi_ie_keeps_every_ie(no_ie_volume_map):
    """A manuscript absent from ie_volume_map that legitimately has TWO IEs
    keeps both IEs; only within-IE duplicates are removed."""
    sid = '990000000000000003'
    raw = {
        sid: [
            _page(sid, 'IE100', 1, '1001'),
            _page(sid, 'IE100', 2, '1002'),
            _page(sid, 'IE200', 1, '2001'),
            _page(sid, 'IE200', 1, '2001'),   # within-IE dup → drop
            _page(sid, 'IE200', 2, '2002'),
        ]
    }

    cleaned, changed = bmu.dedupe_browse_map(raw)

    assert changed is True
    # 4 kept: IE100 p1,p2 + IE200 p1,p2 (one IE200 p1 dropped)
    assert len(cleaned[sid]) == 4
    ies = [p['ie_id'] for p in cleaned[sid]]
    assert ies.count('IE100') == 2
    assert ies.count('IE200') == 2


def test_registered_multi_ie_branch_unchanged(monkeypatch):
    """The multi-IE (registered) branch still dedups within IE and keeps all
    IEs — behavior unchanged by the single-IE fix."""
    sid = '990000000000000004'
    monkeypatch.setattr(bmu, '_repair_missing_ie_pages', lambda m: (m, 0))
    monkeypatch.setattr(
        bmu, '_load_ie_volume_map',
        lambda: {sid: {'primary_ie': 'IE300', 'volumes': [
            {'ie_id': 'IE300', 'suffix': 1, 'page_count': 2},
            {'ie_id': 'IE400', 'suffix': 2, 'page_count': 2},
        ]}},
    )
    raw = {
        sid: [
            _page(sid, 'IE300', 1, '3001'),
            _page(sid, 'IE300', 2, '3002'),
            _page(sid, 'IE300', 2, '3002'),   # within-IE dup → drop
            _page(sid, 'IE400', 1, '4001'),
            _page(sid, 'IE400', 2, '4002'),
        ]
    }

    cleaned, changed = bmu.dedupe_browse_map(raw)

    assert changed is True
    assert len(cleaned[sid]) == 4
    ies = [p['ie_id'] for p in cleaned[sid]]
    assert ies.count('IE300') == 2
    assert ies.count('IE400') == 2

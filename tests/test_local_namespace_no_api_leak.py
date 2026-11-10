# -*- coding: utf-8 -*-
"""Phase 95 REQ-9: LOCAL sys_ids must not appear in /api/search payload.

Tests that shared.search_serializer.serialize_search_payload drops LOCAL
items BEFORE _serialize_item runs, as defense-in-depth.
"""
from unittest.mock import MagicMock


LOCAL_SYS_ID = '970012345601234567'
REAL_SYS_ID_1 = '990025143260205171'
REAL_SYS_ID_2 = '990001234560000001'


def _make_meta_mgr():
    """Return a minimal MetadataManager mock for the serializer."""
    meta_mgr = MagicMock()
    # parse_full_id_components returns a dict with sys_id, p_num, fl_id etc.
    meta_mgr.parse_full_id_components.return_value = {
        'sys_id': REAL_SYS_ID_1,
        'ie_id': '',
        'p_num': 1,
        'fl_id': '',
    }
    meta_mgr.get_meta_for_id.return_value = None
    return meta_mgr


def _make_result(sys_id: str, library_code: str = 'CUL') -> dict:
    """Return a minimal result dict resembling SearchEngine output."""
    return {
        'sys_id': sys_id,
        'display': {
            'id': sys_id,
            'library_code': library_code,
            'source': library_code,
            'shelfmark': f'T-S {sys_id[-4:]}',
            'title': 'Test title',
            'img': None,
        },
        'snippet': 'test snippet',
        'full_text': '',
        'sort_score': 1.0,
        'uid': f'uid-{sys_id}',
        'raw_header': sys_id,
        'score': 1.0,
    }


def _make_local_result() -> dict:
    """Return a result dict for a LOCAL hit."""
    return _make_result(LOCAL_SYS_ID, library_code='LOCAL')


# ---------------------------------------------------------------------------
# Test 1: serialize_search_payload drops LOCAL items
# ---------------------------------------------------------------------------

def test_serialize_search_payload_drops_local():
    """REQ-9 defense-in-depth: LOCAL row dropped BEFORE _serialize_item runs."""
    from shared.search_serializer import serialize_search_payload

    results = [
        _make_result(REAL_SYS_ID_1),
        _make_local_result(),
        _make_result(REAL_SYS_ID_2),
    ]

    meta_mgr = _make_meta_mgr()
    # Override parse_full_id_components to return sensible data per call
    def parse_side_effect(header):
        return {
            'sys_id': header if header.isdigit() else REAL_SYS_ID_1,
            'ie_id': '',
            'p_num': 1,
            'fl_id': '',
        }
    meta_mgr.parse_full_id_components.side_effect = parse_side_effect

    envelope = serialize_search_payload(
        results=results,
        meta_mgr=meta_mgr,
        query='test',
        mode='text',
    )

    assert len(envelope['results']) == 2, (
        f"Expected 2 results after LOCAL drop, got {len(envelope['results'])}"
    )
    # Confirm the LOCAL item is absent
    for item in envelope['results']:
        display = item.get('display', {}) or {}
        assert display.get('library_code') != 'LOCAL', (
            "LOCAL library_code found in serialized output — filter failed"
        )
        assert display.get('source') != 'LOCAL', (
            "LOCAL source found in serialized output — filter failed"
        )
        sid = display.get('id', '')
        assert not sid.startswith('97'), (
            f"97-prefix sys_id found in serialized output: {sid}"
        )


# ---------------------------------------------------------------------------
# Test 2: no LOCAL items — output unchanged
# ---------------------------------------------------------------------------

def test_serialize_search_payload_no_local_unchanged():
    """Regression: when no LOCAL items present, output length == input length."""
    from shared.search_serializer import serialize_search_payload

    results = [
        _make_result(REAL_SYS_ID_1),
        _make_result(REAL_SYS_ID_2),
    ]

    meta_mgr = _make_meta_mgr()
    def parse_side_effect(header):
        return {
            'sys_id': header if header.isdigit() else REAL_SYS_ID_1,
            'ie_id': '',
            'p_num': 1,
            'fl_id': '',
        }
    meta_mgr.parse_full_id_components.side_effect = parse_side_effect

    envelope = serialize_search_payload(
        results=results,
        meta_mgr=meta_mgr,
        query='test',
        mode='text',
    )

    assert len(envelope['results']) == 2, (
        f"Expected 2 results (no LOCAL items to drop), got {len(envelope['results'])}"
    )

"""Phase 94 EXPORT-META-07 / D-10 regression test.

Pins the negative invariant: ``_to_parallels_envelope_item`` output MUST NOT
contain ``has_pgp`` / ``is_printed`` keys. The parallels envelope shape is a
public contract via ``/api/parallels`` and ``/api/export/parallels/json``;
silently inheriting search-side additive flags would break skill consumers.

Defense in depth: even though MUST-FIX 94-02-B (opt-in semantics) ensures
``_serialize_item`` does NOT emit these keys when neither enrichment kwarg
is provided, the parallels envelope builder also explicitly strips them
(via .pop(..., None)) so that a future caller that does opt in cannot leak
into the parallels shape.
"""


def test_parallels_envelope_item_does_not_contain_has_pgp():
    from shared.search_serializer import _to_parallels_envelope_item
    # Build a minimal synthetic group dict
    group = {
        'sys_id': '99001234567890',
        'representative': {
            'uid': 'rep_uid',
            'display': {
                'id': '99001234567890',
                'shelfmark': 'T-S 1',
                'title': 'X',
                'library_code': 'CUL',
            },
            'snippet': 'foo',
            'raw_header': '',
            'sort_score': 1.0,
        },
        'aggregate_score': 1.0,
        'items': [],
    }
    item = _to_parallels_envelope_item(
        group,
        meta_mgr=None,
        domain_batch={},
        catalog_batch={},
    )
    assert 'has_pgp' not in item, (
        f"D-10 violation: parallels item leaked has_pgp key: {list(item.keys())}"
    )
    assert 'is_printed' not in item, (
        f"D-10 violation: parallels item leaked is_printed key: {list(item.keys())}"
    )


def test_parallels_envelope_item_keeps_existing_keys():
    """Sanity check -- the strip does not collateral-damage other keys."""
    from shared.search_serializer import _to_parallels_envelope_item
    group = {
        'sys_id': '99001234567890',
        'representative': {
            'uid': 'rep_uid',
            'display': {
                'id': '99001234567890',
                'shelfmark': 'T-S 1',
                'title': 'X',
                'library_code': 'CUL',
            },
            'snippet': '',
            'raw_header': '',
            'sort_score': 1.0,
        },
        'aggregate_score': 1.0,
        'items': [],
    }
    item = _to_parallels_envelope_item(
        group,
        meta_mgr=None,
        domain_batch={},
        catalog_batch={},
    )
    # Pre-existing keys still present:
    assert 'uid' in item
    assert 'shelfmark' in item
    assert 'is_synthetic' in item  # the other additive bool -- kept (Phase 85)
    assert 'matches' in item  # parallels-specific

# -*- coding: utf-8 -*-
"""Tests for shared/exclusion_service.py — exclusion parsing, resolution, and serialization."""

import pytest

from shared.exclusion_service import (
    ExclusionSource,
    ResolvedEntry,
    build_shelf_map,
    compute_excluded_ids,
    deserialize_sources,
    parse_csv_shelfmarks,
    parse_shelfmark_file,
    resolve_shelfmarks,
    serialize_sources,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MOCK_CSV_BANK = {
    '990051564290205171': {
        'shelfmark': 'T-S 12.123',
        'call_numbers_raw': ['T-S 12.123'],
        'title': 'Test 1',
    },
    '990053963680205171': {
        'shelfmark': 'MS Heb a.1',
        'call_numbers_raw': ['MS. Heb. a.1', 'Ms. Heb. a 1'],
        'title': 'Test 2',
    },
    '990044330510205171': {
        'shelfmark': 'EVR II B 1011',
        'call_numbers_raw': ['Yevr. II B 1011', 'Ms. EVR II B 1011'],
        'title': 'Test 3',
    },
}


@pytest.fixture
def shelf_map():
    return build_shelf_map(MOCK_CSV_BANK)


# ---------------------------------------------------------------------------
# parse_shelfmark_file
# ---------------------------------------------------------------------------

def test_parse_shelfmark_file_basic():
    content = "T-S 12.123\nMS Heb a.1\n\n  T-S NS 192.21  "
    result = parse_shelfmark_file(content)
    assert result == ["T-S 12.123", "MS Heb a.1", "T-S NS 192.21"]


def test_parse_shelfmark_file_comments():
    content = "# This is a comment\nT-S 12.123\n# Another comment\nMS Heb a.1"
    result = parse_shelfmark_file(content)
    assert result == ["T-S 12.123", "MS Heb a.1"]


# ---------------------------------------------------------------------------
# parse_csv_shelfmarks
# ---------------------------------------------------------------------------

def test_parse_csv_header_detection():
    content = "shelfmark,title,notes\nT-S 12.123,some title,note\nMS Heb a.1,another,info"
    result = parse_csv_shelfmarks(content)
    assert result == ["T-S 12.123", "MS Heb a.1"]


def test_parse_csv_call_number_keyword():
    content = "id,call_number,desc\n1,T-S 12.123,description\n2,MS Heb a.1,other"
    result = parse_csv_shelfmarks(content)
    assert result == ["T-S 12.123", "MS Heb a.1"]


def test_parse_csv_fallback_column_zero():
    content = "col1,col2\nT-S 12.123,value\nMS Heb a.1,value2"
    result = parse_csv_shelfmarks(content)
    assert result == ["T-S 12.123", "MS Heb a.1"]


def test_parse_csv_bom():
    content = "\ufeffshelfmark,title\nT-S 12.123,test\nMS Heb a.1,test2"
    result = parse_csv_shelfmarks(content)
    assert result == ["T-S 12.123", "MS Heb a.1"]


# ---------------------------------------------------------------------------
# build_shelf_map
# ---------------------------------------------------------------------------

def test_build_shelf_map_variants(shelf_map):
    # Primary shelfmarks
    assert 'ts12.123' in shelf_map
    assert 'heba1' in shelf_map  # "MS Heb a.1" -> normalized "heba1" (ms prefix stripped)
    assert 'evriib1011' in shelf_map

    # Variant shelfmarks from call_numbers_raw
    assert shelf_map.get('evriib1011') == '990044330510205171'


# ---------------------------------------------------------------------------
# resolve_shelfmarks
# ---------------------------------------------------------------------------

def test_resolve_found(shelf_map):
    ids, unresolved, entries = resolve_shelfmarks(["T-S 12.123"], shelf_map)
    assert '990051564290205171' in ids
    assert unresolved == []
    assert len(entries) == 1
    assert entries[0].status == 'found'
    assert entries[0].sys_id == '990051564290205171'


def test_resolve_not_found(shelf_map):
    ids, unresolved, entries = resolve_shelfmarks(["NONEXISTENT"], shelf_map)
    assert ids == set()
    assert unresolved == ["NONEXISTENT"]
    assert len(entries) == 1
    assert entries[0].status == 'not_found'
    assert entries[0].sys_id is None


def test_resolve_duplicate(shelf_map):
    """Second occurrence of same sys_id is marked as duplicate."""
    ids, unresolved, entries = resolve_shelfmarks(
        ["T-S 12.123", "T-S 12.123"], shelf_map
    )
    assert len(ids) == 1
    assert entries[0].status == 'found'
    assert entries[1].status == 'duplicate'


def test_resolve_variant_shelfmarks(shelf_map):
    """Yevr variant resolves via call_numbers_raw in shelf_map."""
    ids, unresolved, entries = resolve_shelfmarks(
        ["Yevr. II B 1011"], shelf_map
    )
    assert '990044330510205171' in ids
    assert entries[0].status == 'found'


# ---------------------------------------------------------------------------
# compute_excluded_ids
# ---------------------------------------------------------------------------

def test_compute_excluded_ids_union():
    s1 = ExclusionSource('a', 'file', 'f1', sys_ids={'id1', 'id2'})
    s2 = ExclusionSource('b', 'file', 'f2', sys_ids={'id2', 'id3'})
    result = compute_excluded_ids([s1, s2])
    assert result == {'id1', 'id2', 'id3'}


def test_compute_excluded_ids_empty():
    assert compute_excluded_ids([]) == set()


# ---------------------------------------------------------------------------
# serialize / deserialize
# ---------------------------------------------------------------------------

def test_serialize_deserialize_roundtrip():
    sources = [
        ExclusionSource(
            label='Test List',
            source_type='list',
            source_id='list-123',
            sys_ids={'id1', 'id2'},
            unresolved=['MISSING'],
            resolved_entries=[ResolvedEntry('x', 'x', 'id1', 'found')],
        ),
    ]
    data = serialize_sources(sources)
    # resolved_entries must NOT be in serialized output
    assert 'resolved_entries' not in data[0]

    restored = deserialize_sources(data)
    assert len(restored) == 1
    assert restored[0].label == 'Test List'
    assert restored[0].source_type == 'list'
    assert restored[0].source_id == 'list-123'
    assert restored[0].sys_ids == {'id1', 'id2'}
    assert restored[0].unresolved == ['MISSING']
    assert restored[0].resolved_entries == []  # transient, not restored


def test_deserialize_malformed():
    """Gracefully handles bad data — skips malformed entries."""
    data = [
        {'label': 'Good', 'source_type': 'file', 'source_id': 'f1'},
        {'bad_key': 'value'},  # missing required fields
        'not_a_dict',
    ]
    result = deserialize_sources(data)
    assert len(result) == 1
    assert result[0].label == 'Good'

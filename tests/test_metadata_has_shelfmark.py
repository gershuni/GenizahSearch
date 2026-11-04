# -*- coding: utf-8 -*-
"""``MetadataManager.has_shelfmark`` -- the go-to boxes' catalogue check (Codex on #476).

The website's homepage and header boxes ask it about Latin text with a digit
that the rule-based recogniser calls text (``Mingana 9``), so a catalogue
shelfmark opens Browse instead of a search. It looks the normalised query up
(with a library name off the front, or whole; exact only -- a prefix of a
shelfmark is not one) in the Shelfmark search index the server builds at
start, and never builds that index itself.
"""
from __future__ import annotations

import pytest

from shared.browse_map_utils import normalize_shelfmark
from shared.metadata_manager import MetadataManager, _ShelfmarkSearchIndex


def _manager(*shelfmarks, built=True, alternates=None):
    mm = MetadataManager.__new__(MetadataManager)   # no csv load
    mm.csv_bank = {f'99{i:016d}': {'shelfmark': shelf} for i, shelf in enumerate(shelfmarks)}
    for sys_id, others in (alternates or {}).items():
        mm.csv_bank[sys_id] = {'shelfmark': others[0], 'call_numbers_raw': list(others)}
    mm.nli_cache = {}
    mm._shelfmark_search_index = _ShelfmarkSearchIndex(mm.csv_bank, normalize_shelfmark) if built else None
    return mm


@pytest.mark.parametrize('typed', ['Mingana 9', 'mingana 9', '  Mingana 9 ', 'Mingana  9', 'MINGANA 9'])
def test_a_catalogue_shelfmark_is_found_as_typed(typed):
    assert _manager('Mingana 9', 'T-S 12.123').has_shelfmark(typed) is True


def test_a_library_name_in_front_is_taken_off():
    assert _manager('T-S 12.123').has_shelfmark('Cambridge T-S 12.123') is True


@pytest.mark.parametrize('typed', ['Mingana', 'Mingana 90', 'ingana 9', 'Psalm 23', '', '   ', None, '!!'])
def test_anything_else_is_not(typed):
    assert _manager('Mingana 9', 'T-S 12.123').has_shelfmark(typed) is False


def test_before_the_index_is_built_nothing_is_found_and_nothing_is_built():
    mm = _manager('Mingana 9', built=False)
    assert mm.has_shelfmark('Mingana 9') is False
    assert mm._shelfmark_search_index is None


def test_an_index_of_another_catalogue_is_not_used():
    mm = _manager('Mingana 9')
    mm.csv_bank = dict(mm.csv_bank, **{'990000000000000099': {'shelfmark': 'T-S 1.1'}})
    assert mm.has_shelfmark('Mingana 9') is False


def test_a_records_other_call_numbers_are_found_too():
    """Codex on #476: libraries.csv 990053779870205171 stores its Gaster number
    second; Browse resolves it, so the go-to boxes must open Browse for it."""
    mm = _manager('T-S 12.123', alternates={
        '990053779870205171': ('Gaster 61', 'Gaster, Moses Collection Printed series 61'),
    })
    assert mm.has_shelfmark('Gaster, Moses Collection Printed series 61') is True
    assert mm.has_shelfmark('gaster moses collection printed series 61') is True
    assert mm.has_shelfmark('Gaster 61') is True
    assert mm.has_shelfmark('Gaster, Moses Collection Printed series 6') is False


def test_a_shelfmark_only_the_nli_cache_holds_is_found():
    """Codex on #476: Browse also resolves the NLI cache's shelfmark and
    shelfmark_alt (tests/test_catalog_shelfmark.py, 'Alt 7' -> NLIONLY)."""
    mm = _manager('T-S 12.123')
    mm.nli_cache = {'NLIONLY': {'shelfmark': 'Prim 3', 'shelfmark_alt': 'Alt 7'}}
    assert mm.has_shelfmark('Alt 7') is True
    assert mm.has_shelfmark('Prim 3') is True
    assert mm.has_shelfmark('Alt 8') is False

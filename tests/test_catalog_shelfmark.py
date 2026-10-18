"""Catalogue-form shelfmark fallback, on a small synthetic corpus (plan gates 5, 6, 7, 8).

docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md. Real-data gates live in
test_catalog_shelfmark_real.py and test_shelfmark_resolve_baseline.py; this file
pins the rules and the plumbing, so it is fast and runs everywhere.

The CUDL bridge is stubbed to "no hit" (its alias index is process-global and may
hold the real corpus from another test), except where a test sets it.
"""
import threading

import pytest

import shared.catalog_shelfmark as cs
import shared.shelfmark_bridge as bridge
from shared.exclusion_service import build_shelf_map, catalog_fallback, resolve_shelfmarks
from shared.metadata_manager import MetadataManager

ROWS = {
    # Cambridge: the head is used >= 3 times, so it is a table head.
    'CUL1': ('CUL', ['Cambridge University Library Ms. T-S K 8.26', 'Ms. T-S K 8.26', 'T-S K8.26']),
    'CUL2': ('CUL', ['Cambridge University Library Ms. T-S 12.123', 'T-S 12.123']),
    'CUL3': ('CUL', ['Cambridge University Library Ms. T-S AS 31.1', 'T-S AS 31.1']),
    # Mosseri: VIII.402 exists only as parts; IV 366 as a unit AND a part.
    'MOS1': ('Mosseri', ['Moss. VIII,402.1']),
    'MOS2': ('Mosseri', ['Moss. VIII,402.2']),
    'MOS3': ('Mosseri', ['Moss. IV,366', 'Mosseri, Jacques Ms. IV 366', 'Ms. IV 366']),
    'MOS4': ('Mosseri', ['Moss. IV,366.1', 'Mosseri, Jacques Ms. IV 366.1']),
    'MOS5': ('Mosseri', ['Moss. III,27O', 'Mosseri, Jacques Ms. III 27O']),
    # Hungarian Academy: one record stores the long form, a stub stores only "6".
    'HAS1': ('HAS', ['Library of the Hungarian Academy of Sciences Ms. 6', 'Ms. 6',
                     'Library of the Hungarian Academy of Sciences Ms. Kaufmann GEN 48']),
    'HAS2': ('HAS', ['6']),
    'HAS3': ('HAS', ['Library of the Hungarian Academy of Sciences Ms. 90']),
    # Zero-padded stored forms vs plain ones.
    'FREER2': ('Freer', ['Freer Gallery of Art, Smithsonian Institution Ms. F 02', 'Ms. F 02']),
    'FREER1': ('Freer', ['Ms. F 01']),
    'RNLF1': ('RNL', ['F 1']),
    # normalize_shelfmark glues "6=4" into "64": the zero strip must happen first.
    'NLI6': ('NLI', ['Ms. Heb. 577.10.6=4', 'The National Library of Israel Ms. Heb. 577.10.6=4']),
    'NLI60': ('NLI', ['Ms. Heb. 577.10.60=4', 'The National Library of Israel Ms. Heb. 577.10.60=4']),
    'NLI1': ('NLI', ['The National Library of Israel Ms. Heb. 1.1']),
    # "72, 1" normalizes like "721": only the numeric check keeps them apart.
    'EVR721': ('RNL', ['Ms. EVR II A 721', 'The National Library of Russia Ms. EVR II A 721']),
    'EVR72_1': ('RNL', ['Ms. EVR II A 72, 1']),
    'RNL1': ('RNL', ['The National Library of Russia Ms. EVR I 1']),
    # Three Sassoon variants stored under CUL must not make every CUL record "Sassoon".
    'SAS1': ('CUL', ['Sassoon, David Solomon Ms. 1']),
    'SAS2': ('CUL', ['Sassoon, David Solomon Ms. 2']),
    'SAS3': ('CUL', ['Sassoon, David Solomon Ms. 3']),
}


def _bank(rows=ROWS):
    return {sid: {'shelfmark': min(v, key=len), 'title': '', 'library_code': code, 'call_numbers_raw': list(v)}
            for sid, (code, v) in rows.items()}


def _manager(rows=ROWS, nli_cache=None):
    mm = MetadataManager.__new__(MetadataManager)
    mm.csv_bank = _bank(rows)
    mm.nli_cache = nli_cache or {}
    mm._shelfmark_index = None

    def _no_load():
        raise AssertionError('unit tests never load libraries.csv')
    mm._load_csv_bank = _no_load
    return mm


@pytest.fixture(autouse=True)
def _no_bridge(monkeypatch):
    monkeypatch.setattr(bridge, 'lookup_cudl', lambda text: None)


@pytest.fixture
def mm():
    return _manager()


def ids(result):
    if result.get('sys_id'):
        return [result['sys_id']]
    return [o['sys_id'] for o in result.get('options') or []]


# ---------------------------------------------------------------- building blocks

def test_zero_strip_happens_on_raw_text_before_normalizing():
    assert cs.zs('T-S K 08.026') == 'T-S K 8.26'
    assert cs.zs('Heb. 577.10.06=04') == 'Heb. 577.10.6=4'
    assert cs.zs('0') == '0' and cs.zs('00') == '0' and cs.zs('a00b') == 'a0b'
    assert cs.zs('Or. 10578R.98') == 'Or. 10578R.98'      # zeros inside a number stay
    assert cs.zs('III 27O') == 'III 27O'                   # a letter O is not a zero
    assert cs.zkey('Heb. 577.10.06=04') == 'heb577.10.64'  # not ...604 (which is 60=4)
    assert cs.numbers('EVR II A 72, 1') == (72, 1) and cs.numbers('EVR II A 0721') == (721,)


def test_catalogue_shape_gate():
    yes = ['Ms. F 002', 'T-S K 08.026', 'Cambridge University Library, Ms. T-S K 8.26', 'MS heb. d. 32']
    no = ['T-S 12.123', 'ENA 2956.1', 'Oxford Ms', '', None]
    assert all(cs.looks_like_catalogue_form(q) for q in yes)
    assert not any(cs.looks_like_catalogue_form(q) for q in no)


def test_heads_come_from_raw_variants_with_three_uses_longest_first():
    rows = {
        'A': ('CUL', ['Abc Library Ms. 1', 'Abc Library Ms. 1.']),   # two raw variants, one record
        'B': ('CUL', ['Abc Library Ms. 2']),
        'C': ('CUL', ['Abc Library Annex Ms. 3', 'Abc Library Annex Ms. 4', 'Abc Library Annex Ms. 5']),
        'D': ('CUL', ['Rare Head Ms. 1', 'Rare Head Ms. 2']),         # only 2 uses
    }
    mm = _manager(rows)
    tables = mm._get_catalog_tables(mm._get_shelfmark_index())
    assert tables.heads == ('abc library annex', 'abc library')
    assert tables.split('Abc Library Annex, Ms. 5') == ('abc library annex', '5')
    assert tables.split('Abc Library, Ms. 5') == ('abc library', '5')
    assert tables.split('Abc Library Paris France Ms. 5') == ('abc library', '5')
    assert tables.split('Abc Library, ms. 5') == ('abc library', '5')     # typed in any case
    assert tables.split('ABC LIBRARY MS 5') == ('abc library', '5')
    assert tables.split('Abc Libraryx, Ms. 5') is None
    assert tables.split('Rare Head, Ms. 1') is None


def test_library_codes_are_authoritative_never_frequency():
    assert cs.head_codes('cambridge university library') == {'CUL'}
    assert cs.head_codes('the national library of israel') == {'NLI'}
    assert cs.head_codes('sassoon, david solomon') == {'Sassoon'}      # not CUL
    assert cs.head_codes('state library of berlin') == {'SBB'}
    assert cs.head_codes('vernadsky national library of ukraine') == frozenset()
    aliases = cs.alias_codes()
    assert aliases['bnf'] == {'BnF', 'BNF'}
    assert aliases['national library of france'] == {'BnF', 'BNF'}
    assert aliases['cul'] == {'CUL'} and 'local' not in aliases


# ---------------------------------------------------------------- answers

@pytest.mark.parametrize('query, expected', [
    ('Cambridge University Library, Ms. T-S K 08.026', ['CUL1']),
    ('Cambridge University Library Ms. T-S K 08.026', ['CUL1']),
    ('T-S K 08.026', ['CUL1']),
    ('CUL T-S K 08.026', ['CUL1']),
    ('Mosseri, Jacques, Ms. VIII.402', ['MOS1', 'MOS2']),
    ('Mosseri, Jacques Paris France Ms. VIII.402', ['MOS1', 'MOS2']),
    ('Mosseri, Jacques, Ms. IV.366', ['MOS3']),            # the unit, not its part
    ('Mosseri, Jacques, Ms. III 270', []),                 # a zero is not the letter O
    ('Ms. F 002', ['FREER2']),
    ('Cambridge University Library, Ms. F 002', []),       # a named library is never dropped
    ('The British Library, Ms. F 002', []),
    ('The National Library of Israel, Ms. Heb. 577.10.06=04', ['NLI6']),
    ('Library of the Hungarian Academy of Sciences, Ms. 06', ['HAS1', 'HAS2']),
    ('The National Library of Russia, Ms. EVR II A 0721', ['EVR721']),
    ('Sassoon, David Solomon, Ms. T-S K 08.026', []),
])
def test_catalogue_forms(mm, query, expected):
    assert sorted(ids(mm.resolve_system_by_shelfmark(query))) == expected
    assert sorted(ids(mm.resolve_catalog_form(query))) == expected


def test_zero_insensitive_on_both_sides_gives_options_never_one_silently(mm):
    r = mm.resolve_catalog_form('F 001')
    assert r['sys_id'] is None and sorted(ids(r)) == ['FREER1', 'RNLF1'] and r['total'] == 2


def test_exact_answers_never_reach_the_fallback(mm, monkeypatch):
    def boom(*a, **k):
        raise AssertionError('T2 ran although T1 had an exact match')
    monkeypatch.setattr(cs, 'resolve_many', boom)
    assert mm.resolve_system_by_shelfmark('F 01')['sys_id'] == 'FREER1'
    assert mm.resolve_system_by_shelfmark('T-S 12.123')['sys_id'] == 'CUL2'
    six = mm.resolve_system_by_shelfmark('6')          # exact OPTIONS are exact too
    assert sorted(ids(six)) == ['HAS1', 'HAS2']
    assert mm.resolve_system_by_shelfmark('6', limit=0) == {'sys_id': None, 'options': [], 'selected_shelfmark': None}


def test_a_broken_fallback_keeps_todays_substring_answer(mm, monkeypatch):
    before = ids(mm.resolve_system_by_shelfmark('T-S AS'))
    monkeypatch.setattr(cs, 'resolve_many', lambda *a, **k: 1 / 0)
    assert ids(mm.resolve_system_by_shelfmark('T-S AS')) == before


def test_uniqueness_is_decided_before_the_limit(mm):
    one = mm.resolve_catalog_form('F 001', limit=1)
    assert one['sys_id'] is None and len(one['options']) == 1 and one['total'] == 2
    zero = mm.resolve_catalog_form('F 001', limit=0)
    assert zero['sys_id'] is None and zero['options'] == [] and zero['total'] == 2
    assert mm.resolve_system_by_shelfmark('F 001', limit=1)['sys_id'] is None


def test_single_and_batch_answers_are_identical(mm):
    queries = [q for q, _ in test_catalogue_forms.pytestmark[0].args[1]] + ['F 001', 'F 01', '6', 'nothing 999']
    batch = mm.resolve_catalog_forms(queries)
    for q in queries:
        assert mm.resolve_catalog_form(q) == batch[q], q


class _CountingList(list):
    def __init__(self, *a):
        super().__init__(*a)
        self.passes = 0

    def __iter__(self):
        self.passes += 1
        return super().__iter__()


def test_the_batch_makes_one_pass_for_many_inputs(mm):
    index = _CountingList(mm._get_shelfmark_index())
    tables = cs.CatalogTables(index, mm.csv_bank)
    index.passes = 0
    cs.resolve_many(['T-S K 08.026', 'Ms. F 002', 'F 001', 'Library of the Hungarian Academy of Sciences, Ms. 06'],
                    tables, mm.csv_bank)
    assert index.passes == 1
    index.passes = 0
    cs.resolve_many(['Mosseri, Jacques, Ms. VIII.402', 'Mosseri, Jacques, Ms. VIII.403', 'T-S K 08.026'],
                    tables, mm.csv_bank)
    assert index.passes == 2          # one more, shared by every input that needs a parent


# ---------------------------------------------------------------- lifecycle (gate 6)

def test_tables_follow_the_index_object_not_its_content(mm):
    index = mm._get_shelfmark_index()
    tables = mm._get_catalog_tables(index)
    assert tables.index is index and mm._get_catalog_tables(index) is tables
    replacement = list(index)                     # equal content, distinct object
    assert replacement == index
    mm._shelfmark_index = replacement
    rebuilt = mm._get_catalog_tables(mm._get_shelfmark_index())
    assert rebuilt is not tables and rebuilt.index is replacement


def test_an_empty_index_then_a_load_rebuilds_the_tables():
    mm = _manager()
    full_bank = mm.csv_bank
    mm.csv_bank = {}
    mm._shelfmark_index = []                      # cached before the background load finished
    assert mm.resolve_system_by_shelfmark('T-S K 08.026')['sys_id'] is None
    mm.csv_bank = full_bank                       # the load completes
    assert mm.resolve_system_by_shelfmark('T-S K 08.026')['sys_id'] == 'CUL1'


def test_cache_only_and_bridge_answers_stay_exact(monkeypatch):
    mm = _manager(nli_cache={'NLIONLY': {'shelfmark': 'Ms. F 002', 'shelfmark_alt': 'Alt 7', 'title': ''}})
    assert mm.resolve_system_by_shelfmark('Ms. F 002')['sys_id'] == 'NLIONLY'   # T1 exact, not Freer
    assert mm.resolve_system_by_shelfmark('Alt 7')['sys_id'] == 'NLIONLY'
    monkeypatch.setattr(bridge, 'lookup_cudl', lambda text: {'sys_id': 'CUL2'} if text == 'T-S K 08.026' else None)
    assert _manager().resolve_system_by_shelfmark('T-S K 08.026')['sys_id'] == 'CUL2'   # bridge wins


def test_two_threads_on_a_cold_manager_agree():
    mm = _manager()
    out = []
    threads = [threading.Thread(target=lambda: out.append(ids(mm.resolve_system_by_shelfmark(
        'Library of the Hungarian Academy of Sciences, Ms. 06')))) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(out[0]) == sorted(out[1]) == ['HAS1', 'HAS2']


# ---------------------------------------------------------------- search_by_meta (gate 7)

def test_search_fallback_only_for_empty_catalogue_shaped_queries(mm, monkeypatch):
    assert sorted(mm.search_by_meta('Cambridge University Library, Ms. T-S K 08.026', 'shelfmark')) == ['CUL1']
    assert sorted(mm.search_by_meta('Mosseri, Jacques, Ms. VIII.402', 'shelfmark')) == ['MOS1', 'MOS2']
    calls = []
    real = MetadataManager._get_shelfmark_index
    monkeypatch.setattr(MetadataManager, '_get_shelfmark_index', lambda self: calls.append(1) or real(self))
    assert mm.search_by_meta('zzz 999', 'shelfmark') == []
    assert calls == []                        # an ordinary empty search builds nothing
    assert mm.search_by_meta('T-S 12.123', 'shelfmark') == ['CUL2']
    assert calls == []                        # nor does one that found something


def test_search_fallback_never_returns_substring_options(mm):
    # Catalogue-shaped, no catalogue match: T1 offers substring options (T-S AS 31.1);
    # the search fallback offers nothing.
    assert set(ids(mm.resolve_system_by_shelfmark('Ms. T-S AS 3'))) == {'CUL3'}   # T1 lists both variants
    assert mm.search_by_meta('Ms. T-S AS 3', 'shelfmark') == []


# ---------------------------------------------------------------- exclusions (gate 8)

def test_exclusions_resolve_unique_catalogue_forms_in_one_batch(mm):
    shelf_map = build_shelf_map(mm.csv_bank)
    calls = []
    fallback = catalog_fallback(mm)

    def spy(lines):
        calls.append(list(lines))
        return fallback(lines)

    lines = ['T-S 12.123', 'Cambridge University Library, Ms. T-S K 08.026',
             'Mosseri, Jacques, Ms. VIII.402', 'T-S K 08.026', 'nothing 999']
    found, unresolved, entries = resolve_shelfmarks(lines, shelf_map, spy)
    assert calls == [lines[1:]]               # one call, only the lines the map missed
    assert found == {'CUL2', 'CUL1'}
    assert [e.status for e in entries] == ['found', 'found', 'not_found', 'duplicate', 'not_found']
    assert unresolved == ['Mosseri, Jacques, Ms. VIII.402', 'nothing 999']   # ambiguous stays out
    assert resolve_shelfmarks(lines, shelf_map)[0] == {'CUL2'}             # no fallback: as before

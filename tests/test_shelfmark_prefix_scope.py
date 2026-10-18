"""A library prefix keeps its library, on a small synthetic corpus.

docs/plans/SHELFMARK_PREFIX_SCOPE_PLAN.md. Each row below is a real corpus shape that the
plan measured as a wrong or lost record; the real-data gates live in
test_shelfmark_prefix_scope_real.py. The CUDL bridge is stubbed to "no hit" except where a
test sets it (its alias index is process-global).
"""
import pytest

import shared.catalog_shelfmark as cs
import shared.shelfmark_bridge as bridge
from shared.metadata_manager import MetadataManager

ROWS = {
    # `Halper 77` is stored on a Katz record; no record has code Halper.
    'KATZ77': ('Katz', ['Halper 77']),
    'KATZ_G77': ('Katz', ['Ms. Genizah 77']),          # normalize_shelfmark: halper == genizah
    'SENCK77': ('Senckenberg', ['77']),
    'KATZ_MS77': ('Katz', ['Ms. 77']),                 # Penn Ms. 77: Katz, but not Halper 77
    'HAS77': ('HAS', ['77']),
    # The Manchester record and two Mosseri records share `P 45`.
    'MAN45': ('Manchester', ['Ms. P 45', 'The University of Manchester Library Ms. P 45']),
    'MOS45': ('Mosseri', ['Ms. P 45']),
    'MOS45B': ('Mosseri', ['P 45']),
    # A stub and a full record carry one shelfmark; only the full one stores the long form.
    'OXSTUB': ('Oxford', ['arch. O.d.8/1']),
    'OXFULL': ('Oxford', ['MS heb. d.32/1', 'The Bodleian Libraries, University of Oxford Arch. O.d. 8.1']),
    # Code Gaster holds no record: the Gaster texts are coded BL.
    'BLFULL': ('BL', ['Gaster, Moses Collection Ms. 1201.17', 'Ms. 1201.17']),
    'BLSTUB': ('BL', ['GASTER 1201.17']),
    'JTS_ENA': ('JTS', ['Ms. ENA 1775.5', 'Ms. 1775.5']),
    'BL1775': ('BL', ['GASTER 1775.5']),
    # Glass 12 vs Glass 1.2: only T1's dotless rule joins them.
    'CULGLASS': ('CUL', ['Ms. L-G Glass 12', 'Westminster College Library Ms. Glass 12']),
    'WESTG12': ('Westminster', ['L-G Glass.1.2']),
    # Ms. 2162 vs Ms. 216.2: the dotless rule again, on the whole input.
    'JTS2162': ('JTS', ['Adler, Elkan Nathan Ms. 2162', 'Ms. 2162']),
    'JTS216_2': ('JTS', ['Adler, Elkan Nathan Ms. 216.2', 'Ms. 216.2']),
    'HAS2162': ('HAS', ['2162']),
    # Same key as `Adler Ms. 2162` once normalized, other numbers: only the numeric check on
    # the whole input keeps it out.
    'ADLERGLUE': ('JTS', ['Adler Ms. 21 62']),
    # Adler 1873 that also stores JTS `Ms. 1512`, and a JTS stub `MS 1512`: neither is Adler 1512.
    'JTS1873': ('JTS', ['Adler, Elkan Nathan Ms. 1873', 'Ms. 1512']),
    'JTS1512': ('JTS', ['MS 1512']),
    # Nothing in the named library: today's answer stays.
    'HAS441': ('HAS', ['441']),
    # BnF and BNF are one library.
    'BNF1': ('BNF', ['Heb 12']),
    'BNF2': ('BnF', ['Heb 12']),
    'CULH12': ('CUL', ['Heb 12']),
    # For the bridge tests.
    'KATZ5': ('Katz', ['Halper 5']),
    'CUL5': ('CUL', ['T-S 5']),
    'CULB': ('CUL', ['T-S 9.009']),
}


def _manager(rows=ROWS):
    mm = MetadataManager.__new__(MetadataManager)
    mm.csv_bank = {sid: {'shelfmark': min(v, key=len), 'title': '', 'library_code': code, 'call_numbers_raw': list(v)}
                   for sid, (code, v) in rows.items()}
    mm.nli_cache = {}
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


# ---------------------------------------------------------------- which library a prefix names

def test_prefix_codes_are_exactly_a_library_code_or_name():
    assert cs.prefix_codes('CUL T-S 1', 'T-S 1') == {'CUL'}
    assert cs.prefix_codes('cul T-S 1', 'T-S 1') == {'CUL'}
    assert cs.prefix_codes('The University of Manchester Library, Ms. P 45', 'Ms. P 45') == {'Manchester'}
    assert cs.prefix_codes('bnf Heb 12', 'Heb 12') == {'BnF', 'BNF'}
    assert cs.prefix_codes('National Library of France Heb 12', 'Heb 12') == {'BnF', 'BNF'}
    assert cs.prefix_codes('Cambridge T-S 1', 'T-S 1') == frozenset()        # an alias, not a name
    assert cs.prefix_codes('British Library Or 1', 'Or 1') == frozenset()
    assert cs.prefix_codes('T-S 1', 'T-S 1') == frozenset()                  # nothing removed
    assert cs.named_prefix('Cambridge T-S 1', 'T-S 1') == (frozenset(), None, None)
    assert cs.named_prefix('Halper 77', '77') == ({'Halper'}, 'genizah77', 'Halper 77')


# ---------------------------------------------------------------- answers

@pytest.mark.parametrize('query, expected, tier', [
    # The named library's record, which today was not even offered -- and only Halper 77:
    # Katz also holds `Ms. 77`, which a Katz-wide reading would wrongly add.
    ('Halper 77', ['KATZ77', 'KATZ_G77'], 'exact'),
    ('The University of Manchester Library, Ms. P 45', ['MAN45'], 'exact'),
    ('Manchester Ms. P 45', ['MAN45'], 'exact'),
    ('GASTER 1775.5', ['BL1775'], 'exact'),
    # Both records that carry the typed shelfmark (no swap of one single record for another).
    ('The Bodleian Libraries, University of Oxford, arch. O.d.8/1', ['OXFULL', 'OXSTUB'], 'exact'),
    ('GASTER 1201.17', ['BLFULL', 'BLSTUB'], 'exact'),
    # Only the Westminster record (a dotless-rule match, Glass.1.2) is in the named library and
    # it does not carry the numbers; the CUL record's Westminster variant is `Glass 12`, not
    # `L-G Glass 12`. Scoping would leave a conflation alone, so today's list stays.
    ('Westminster College Ms. L-G Glass 12', ['CULGLASS', 'WESTG12'], 'exact'),
    # An in-library dotless match stays beside a real match: scoping removes other libraries
    # only (HAS `2162` goes; JTS `216.2` stays, as today).
    ('JTS Ms. 2162', ['JTS2162', 'JTS216_2'], 'exact'),
    # Provenance is per shelfmark: `Adler ... Ms. 216.2` is Adler 216.2, not Adler 2162.
    ('Adler Ms. 2162', ['JTS2162'], 'exact'),
    # ... and the record whose Adler number is 1873 is not Adler 1512: no Adler 1512, today's answer.
    ('adler MS 1512', ['JTS1512', 'JTS1873'], 'exact'),
    # Nothing in the named library and nothing stores the input: today's answer.
    ('Halper 441', ['HAS441'], 'exact'),
    ('Gaster Ms. 1201.17', ['BLFULL'], 'exact'),
    # BnF and BNF are one class; CUL's Heb 12 is dropped.
    ('BnF Heb 12', ['BNF1', 'BNF2'], 'exact'),
    ('National Library of France Heb 12', ['BNF1', 'BNF2'], 'exact'),
    # An alias that names no library, and no prefix at all: unchanged.
    ('Cambridge 77', ['HAS77', 'KATZ_MS77', 'SENCK77'], 'exact'),
    ('77', ['HAS77', 'KATZ_MS77', 'SENCK77'], 'exact'),
    ('P 45', ['MAN45', 'MOS45', 'MOS45B'], 'exact'),
    # No exact match today: the rule does not apply and T2 answers as before (2162, not 216.2).
    ('Adler, Elkan Nathan Ms. 2162', ['JTS2162'], 'direct'),
])
def test_prefixed_inputs(mm, query, expected, tier):
    assert sorted(ids(mm.resolve_system_by_shelfmark(query))) == expected
    got = mm.resolve_catalog_form(query)
    assert sorted(ids(got)) == expected and got['tier'] == tier


def test_a_bare_library_name_finds_nothing_on_both_paths(mm):
    for query in ('Halper', 'CUL', 'The University of Manchester Library'):
        assert ids(mm.resolve_system_by_shelfmark(query)) == []
        assert ids(mm.resolve_catalog_form(query)) == []


def test_a_zero_padded_prefixed_input_reaches_what_its_unpadded_form_reaches(mm):
    got = mm.resolve_catalog_form('Halper 077')
    assert sorted(ids(got)) == ['KATZ77', 'KATZ_G77'] and got['tier'] == 'direct'
    assert sorted(ids(mm.resolve_system_by_shelfmark('Halper 077'))) == ['KATZ77', 'KATZ_G77']


def test_the_shortest_shelfmark_is_still_the_one_shown(mm):
    r = mm.resolve_system_by_shelfmark('The University of Manchester Library, Ms. P 45')
    assert r == {'sys_id': 'MAN45', 'options': [], 'selected_shelfmark': 'Ms. P 45'}
    assert mm.resolve_catalog_form('The University of Manchester Library, Ms. P 45')['selected_shelfmark'] == 'Ms. P 45'


def test_uniqueness_is_decided_before_the_limit(mm):
    one = mm.resolve_system_by_shelfmark('GASTER 1201.17', limit=1)
    assert one['sys_id'] is None and len(one['options']) == 1
    assert mm.resolve_system_by_shelfmark('GASTER 1201.17', limit=0) == {'sys_id': None, 'options': [],
                                                                          'selected_shelfmark': None}
    c = mm.resolve_catalog_form('GASTER 1201.17', limit=1)
    assert c['sys_id'] is None and len(c['options']) == 1 and c['total'] == 2


def test_a_bridge_hit_outside_the_named_library_is_dropped(monkeypatch):
    monkeypatch.setattr(bridge, 'lookup_cudl', lambda text: {'sys_id': 'CUL5'} if text == '5' else None)
    mm = _manager()
    assert ids(mm.resolve_system_by_shelfmark('5')) == ['CUL5']             # no prefix: as today
    assert ids(mm.resolve_system_by_shelfmark('Halper 5')) == ['KATZ5']
    assert ids(mm.resolve_catalog_form('Halper 5')) == ['KATZ5']


def test_a_bridge_hit_inside_the_named_library_is_kept_unchecked(monkeypatch):
    # The bridge maps forms (`X 9`) to records whose shelfmark has other numbers (`9.009`).
    monkeypatch.setattr(bridge, 'lookup_cudl', lambda text: {'sys_id': 'CULB'} if text == 'X 9' else None)
    mm = _manager()
    assert ids(mm.resolve_system_by_shelfmark('CUL X 9')) == ['CULB']
    assert ids(mm.resolve_catalog_form('CUL X 9')) == ['CULB']


def test_the_unscoped_restatement_is_todays_exact_set(mm):
    tables = mm._get_catalog_tables(mm._get_shelfmark_index())
    queries = ['Halper 77', 'The University of Manchester Library, Ms. P 45', 'P 45']
    scoped = cs.t1_exact_entries(queries, tables, mm.csv_bank)
    unscoped = cs.t1_exact_entries(queries, tables, mm.csv_bank, scoped=False)
    # Sets: before deduplication an entry appears twice when its key and dotless key both match.
    assert {h['sys_id'] for h in unscoped['Halper 77']} == {'HAS77', 'KATZ_MS77', 'SENCK77'}
    assert {h['sys_id'] for h in scoped['Halper 77']} == {'KATZ77', 'KATZ_G77'}
    assert {h['sys_id'] for h in unscoped['The University of Manchester Library, Ms. P 45']} == {'MAN45', 'MOS45', 'MOS45B'}
    assert {h['sys_id'] for h in scoped['The University of Manchester Library, Ms. P 45']} == {'MAN45'}
    assert scoped['P 45'] == unscoped['P 45']


def test_single_and_batch_and_resolver_agree(mm):
    queries = [q for q, _, _ in test_prefixed_inputs.pytestmark[0].args[1]] + ['Halper 077', 'Halper', 'nothing 9']
    batch = mm.resolve_catalog_forms(queries)
    for q in queries:
        single = mm.resolve_catalog_form(q)
        assert single == batch[q], q
        if single['tier'] == 'exact':
            r = mm.resolve_system_by_shelfmark(q, limit=10 ** 6)
            assert ids(r) == ids(single) and r['selected_shelfmark'] == single['selected_shelfmark'], q


# ---------------------------------------------------------------- Codex plan review, round 1

def test_a_cache_only_record_is_not_dropped_by_the_named_library():
    """Its library is unknown (no csv row), which is not "outside": it stays (finding 3)."""
    mm = _manager({'CULX7': ('CUL', ['X 7'])})
    mm.nli_cache = {'CACHEX7': {'shelfmark': 'X 7', 'shelfmark_alt': 'Cambridge University Library Ms. X 7', 'title': ''}}
    assert sorted(ids(mm.resolve_system_by_shelfmark('CUL X 7'))) == ['CACHEX7', 'CULX7']
    assert sorted(ids(mm.resolve_catalog_form('CUL X 7'))) == ['CACHEX7', 'CULX7']
    assert sorted(ids(mm.resolve_system_by_shelfmark('Cambridge University Library, Ms. X 7'))) == ['CACHEX7', 'CULX7']


def test_collection_provenance_keeps_a_record_coded_elsewhere():
    """The Schocken record stores `Sofer, David Ms. 111.12`: it is a Sofer record too (finding 4)."""
    mm = _manager({'SCHOCKEN': ('Schocken', ['Ms. 111.12', 'Sofer, David Ms. 111.12']),
                   'SOFER': ('Sofer', ['111/12']),
                   'OTHER': ('HAS', ['111.12'])})
    assert sorted(ids(mm.resolve_system_by_shelfmark('Sofer Ms. 111.12'))) == ['SCHOCKEN', 'SOFER']
    assert sorted(ids(mm.resolve_catalog_form('Sofer Ms. 111.12'))) == ['SCHOCKEN', 'SOFER']


def test_a_padded_prefixed_input_keeps_both_records_its_unpadded_form_reaches(mm):
    """T2 names the library of a record that stores the whole input, as T1 does (finding 2)."""
    got = mm.resolve_catalog_form('GASTER 01201.017')
    assert sorted(ids(got)) == ['BLFULL', 'BLSTUB'] and got['tier'] == 'direct'
    assert sorted(ids(mm.resolve_system_by_shelfmark('GASTER 01201.017'))) == ['BLFULL', 'BLSTUB']


def test_nothing_left_after_the_prefix_finds_nothing_on_both_paths():
    """A record stores `CUL`; `CUL .` leaves nothing to look up, so neither path answers (finding 9)."""
    mm = _manager({'CULWORD': ('CUL', ['CUL']), 'CUL5': ('CUL', ['T-S 5'])})
    assert cs.prefix_codes('CUL', '') == frozenset() and cs.prefix_codes('CUL .', '.') == frozenset()
    for query in ('CUL', 'CUL .', 'CUL -'):
        assert ids(mm.resolve_system_by_shelfmark(query)) == [], query
        assert ids(mm.resolve_catalog_form(query)) == [], query


def test_the_bridge_never_changes_the_shelfmark_shown(monkeypatch):
    """Today the bridge entry is added only for a record not matched already (finding 8)."""
    mm = _manager({'CULX': ('CUL', ['Ms. T-S 12.123', 'X1'])})          # csv shelfmark 'X1'
    monkeypatch.setattr(bridge, 'lookup_cudl', lambda text: {'sys_id': 'CULX'} if text == 'T-S 12.123' else None)
    for query in ('T-S 12.123', 'CUL T-S 12.123'):
        r = mm.resolve_system_by_shelfmark(query)
        assert r['sys_id'] == 'CULX' and r['selected_shelfmark'] == 'Ms. T-S 12.123', query
        assert mm.resolve_catalog_form(query)['selected_shelfmark'] == 'Ms. T-S 12.123', query


# ---------------------------------------------------------------- Codex review, round 2

def test_the_ms_token_in_any_case_keeps_every_candidate_of_the_head():
    """A lowercase `ms.` once defeated the head split, so the whole-input key answered with the
    long-form record alone (finding 1)."""
    mm = _manager({'A': ('Mosseri', ['Mosseri, Jacques Ms. Ch 2']), 'B': ('Mosseri', ['Moss. Ch 2']),
                   'C': ('Mosseri', ['Mosseri, Jacques Ms. X 1']), 'D': ('Mosseri', ['Mosseri, Jacques Ms. X 2'])})
    for token in ('Ms.', 'MS.', 'ms.', 'mS.', 'MS'):
        query = f'Mosseri, Jacques, {token} Ch 2'
        assert sorted(ids(mm.resolve_catalog_form(query))) == ['A', 'B'], query
        assert sorted(ids(mm.resolve_system_by_shelfmark(query))) == ['A', 'B'], query


@pytest.mark.parametrize('provenance', ['Gaster, Moses Collection Ms. 01201.017',    # zero-padded
                                        'Gaster, Moses Collection ms. 1201.17'])     # lowercase ms.
def test_provenance_reads_padded_and_lowercase_stored_forms(provenance):
    """Otherwise the BL stub that stores `GASTER 1201.17` replaced the full record (finding 2)."""
    mm = _manager({'FULL': ('BL', ['Ms. 1201.17', provenance]), 'STUB': ('BL', ['GASTER 1201.17'])})
    assert sorted(ids(mm.resolve_system_by_shelfmark('GASTER 1201.17'))) == ['FULL', 'STUB']
    assert sorted(ids(mm.resolve_catalog_form('GASTER 1201.17'))) == ['FULL', 'STUB']
    assert sorted(ids(mm.resolve_catalog_form('GASTER 01201.017'))) == ['FULL', 'STUB']


def test_prefix_rule_false_answers_as_before_the_rule(mm):
    """The switch the real-data gate uses for its in-process "before"."""
    tables = mm._get_catalog_tables(mm._get_shelfmark_index())
    before = cs.resolve_many(['Halper 77', 'Halper 077', 'P 45'], tables, mm.csv_bank, prefix_rule=False)
    now = cs.resolve_many(['Halper 77', 'Halper 077', 'P 45'], tables, mm.csv_bank)
    assert sorted(ids(before['Halper 77'])) == ['HAS77', 'KATZ_MS77', 'SENCK77']
    assert sorted(ids(now['Halper 77'])) == ['KATZ77', 'KATZ_G77']
    assert before['Halper 077']['total'] == 0 and sorted(ids(now['Halper 077'])) == ['KATZ77', 'KATZ_G77']
    assert before['P 45'] == now['P 45']


# ---------------------------------------------------------------- Codex review, round 3

def test_provenance_also_reads_the_nli_cache_variants_of_a_csv_record():
    """The index holds a record's NLI-cache `shelfmark`/`shelfmark_alt`; provenance must read
    them too, or the Schocken record whose cached alternative is `Sofer, David Ms. 111.12`
    vanished from `Sofer Ms. 111.12` (finding 1)."""
    mm = _manager({'SCHOCKEN': ('Schocken', ['Ms. 111.12']), 'SOFER': ('Sofer', ['111/12']),
                   'OTHER': ('HAS', ['111.12'])})
    mm.nli_cache = {'SCHOCKEN': {'shelfmark': 'Ms. 111.12', 'shelfmark_alt': 'Sofer, David Ms. 111.12', 'title': ''}}
    assert sorted(ids(mm.resolve_system_by_shelfmark('Sofer Ms. 111.12'))) == ['SCHOCKEN', 'SOFER']
    assert sorted(ids(mm.resolve_catalog_form('Sofer Ms. 111.12'))) == ['SCHOCKEN', 'SOFER']


# ---------------------------------------------------------------- Codex review, round 4

@pytest.mark.parametrize('late_change', ['evicted', 'not a string'])
def test_provenance_lives_as_long_as_the_index_not_the_cache(late_change):
    """The index keeps the cached alternative it was built from; evicting the cache entry or a
    bad value arriving later must not take the Schocken record away (findings 1 and 2)."""
    mm = _manager({'SCHOCKEN': ('Schocken', ['Ms. 111.12']), 'SOFER': ('Sofer', ['111/12']),
                   'OTHER': ('HAS', ['111.12'])})
    mm.nli_cache = {'SCHOCKEN': {'shelfmark': 'Ms. 111.12', 'shelfmark_alt': 'Sofer, David Ms. 111.12', 'title': ''}}
    assert sorted(ids(mm.resolve_system_by_shelfmark('Sofer Ms. 111.12'))) == ['SCHOCKEN', 'SOFER']   # builds the index
    if late_change == 'evicted':
        del mm.nli_cache['SCHOCKEN']
    else:
        mm.nli_cache['SCHOCKEN']['shelfmark_alt'] = ['Sofer, David Ms. 111.12']
    for query in ('Sofer Ms. 111.12', 'Sofer Ms. 0111.012'):
        assert sorted(ids(mm.resolve_system_by_shelfmark(query))) == ['SCHOCKEN', 'SOFER'], query
        assert sorted(ids(mm.resolve_catalog_form(query))) == ['SCHOCKEN', 'SOFER'], query


def test_a_bad_cached_value_never_reaches_provenance():
    mm = _manager({'SCHOCKEN': ('Schocken', ['Ms. 111.12']), 'SOFER': ('Sofer', ['111/12'])})
    mm.nli_cache = {'SCHOCKEN': {'shelfmark': 'Ms. 111.12', 'shelfmark_alt': 12, 'title': ''}}
    assert sorted(ids(mm.resolve_system_by_shelfmark('Sofer Ms. 111.12'))) == ['SOFER']
    assert mm._get_shelfmark_index().cached_variants == {'SCHOCKEN': ('Ms. 111.12',)}


# ---------------------------------------------------------------- Codex review, round 5

def test_the_index_and_its_provenance_come_from_one_frozen_copy(monkeypatch):
    """The index's cache entries are built from the very snapshot it carries, and every lookup
    reads the snapshot of the index it holds -- so a cache change during a build, or a rebuild
    while a lookup runs, cannot pair one build's entries with another's provenance."""
    mm = _manager({'SCHOCKEN': ('Schocken', ['Ms. 111.12']), 'SOFER': ('Sofer', ['111/12'])})
    mm.nli_cache = {}
    real = MetadataManager._cached_shelfmarks

    def snapshot_then_change(self):
        frozen = real(self)                  # the build's copy is taken here ...
        self.nli_cache['SCHOCKEN'] = {'shelfmark': 'x', 'shelfmark_alt': 'Sofer, David Ms. 111.12', 'title': ''}
        return frozen                        # ... and the cache changes before the entries are made
    monkeypatch.setattr(MetadataManager, '_cached_shelfmarks', snapshot_then_change)
    index = mm._get_shelfmark_index()
    assert index.cached_variants == {}
    assert not any(e[1] == 'Sofer, David Ms. 111.12' for e in index)   # entries follow the copy
    monkeypatch.setattr(MetadataManager, '_cached_shelfmarks', real)
    mm._shelfmark_index = None                                           # a later rebuild sees the change
    index2 = mm._get_shelfmark_index()
    assert index2.cached_variants == {'SCHOCKEN': ('x', 'Sofer, David Ms. 111.12')}
    assert index.cached_variants == {}                                   # the old index keeps its own
    assert sorted(ids(mm.resolve_system_by_shelfmark('Sofer Ms. 111.12'))) == ['SCHOCKEN', 'SOFER']


# ---------------------------------------------------------------- Codex review, round 6

@pytest.mark.parametrize('variants, cached', [
    (['Ms. 123', 'GASTER 12 3', 'GASTER 123'], None),
    (['Ms. 123', 'GASTER 123', 'GASTER 12 3'], None),
    (['Ms. 123'], {'shelfmark': 'GASTER 12 3', 'shelfmark_alt': 'GASTER 123', 'title': ''}),
])
def test_the_numeric_check_sees_every_variant_not_only_the_one_the_index_kept(variants, cached):
    """`GASTER 12 3` and `GASTER 123` share a normalized key, and the index keeps one of them;
    the full record still stores `GASTER 123` (finding 1)."""
    mm = _manager({'FULL': ('BL', variants), 'STUB': ('BL', ['GASTER 123'])})
    mm.nli_cache = {'FULL': cached} if cached else {}
    assert sorted(ids(mm.resolve_system_by_shelfmark('GASTER 123'))) == ['FULL', 'STUB']
    assert sorted(ids(mm.resolve_catalog_form('GASTER 123'))) == ['FULL', 'STUB']


def test_no_dotless_rule_for_the_whole_input():
    """`GASTER 21 62` must not reach a record that stores `GASTER 21.62`, although both have the
    integers (21, 62): that is the dotless rule, which the whole input does not use (finding 2)."""
    mm = _manager({'DOTTED': ('BL', ['GASTER 21.62']), 'OTHER': ('HAS', ['2162'])})
    assert ids(mm.resolve_system_by_shelfmark('GASTER 21 62')) == ['OTHER']      # today's answer
    assert ids(mm.resolve_catalog_form('GASTER 21 62')) == ['OTHER']

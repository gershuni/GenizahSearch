# -*- coding: utf-8 -*-
"""``shared/shelfmark_tree_rows.py``: the shelf picker's table and thumbnail views (both apps).

* The stored choice is read safely: anything unknown is the list and the medium size.
* The layout rule: the table and the thumbnails keep only the last node column, when a shelfmarks
  pane is shown.
* ``snippet``: the details pane's rule (the first page that reads; a long manuscript's first page
  only as the last resort; the short manuscript's floor), on one line, cut; the pages read one at a
  time, at most ``SNIPPET_PAGES``.
* ``row_details``: every field from its source, in one batch per source; a source that fails leaves
  its field None and the others stand; domains in the language asked.
"""
from __future__ import annotations

import pytest

from shared import shelfmark_tree_rows as rows
from shared.shelfmark_tree_rows import (
    COLUMNS, DEFAULT_SIZE, DEFAULT_VIEW, FIELDS, LINE_MARK, SNIPPET_CHARS, SNIPPET_PAGES, choice_value,
    folds_columns, normal_size, normal_view, read_choice, row_details, shown_columns, snippet, wanted,
)

HEBREW = 'ויאמר משה אל העם אל תיראו התיצבו וראו את ישועת השם אשר יעשה לכם היום כי אשר ראיתם את מצרים '
LONG_TEXT = HEBREW * 4                          # reads at the long floor (120 letters)
SHORT_TEXT = 'ויאמר משה אל העם אל תיראו התיצבו'   # 27 letters... and a few more below
SHORT_TEXT = SHORT_TEXT + ' וראו'                 # 31 letters: reads only at the short floor (30)


# -- the choice --------------------------------------------------------------------------------

@pytest.mark.parametrize('value', [None, '', 'grid', 3, ['table'], 'TABLE'])
def test_an_unknown_view_is_the_list(value):
    assert normal_view(value) == DEFAULT_VIEW == 'list'


@pytest.mark.parametrize('value', ['list', 'table', 'gallery'])
def test_a_known_view_is_kept(value):
    assert normal_view(value) == value


@pytest.mark.parametrize('value', [None, 'xl', 1, 'M'])
def test_an_unknown_size_is_the_medium(value):
    assert normal_size(value) == DEFAULT_SIZE == 'm'


def test_the_stored_choice_round_trips_and_a_broken_one_is_the_default():
    assert read_choice(choice_value('gallery', 'l')) == ('gallery', 'l')
    assert read_choice(choice_value('table', 'nope')) == ('table', 'm')
    for broken in (None, 'table', ['gallery'], {'view': 7}, {}):
        assert read_choice(broken) == ('list', 'm')


def test_the_columns_are_the_search_tables():
    assert [key for key, _ in COLUMNS] == ['shelfmark', 'title', 'snippet', 'domain', 'pages', 'printed',
                                          'pgp', 'edition']
    assert dict(COLUMNS)['domain'] == 'Domain' and dict(COLUMNS)['edition'] == 'Edition'


# -- the layout rule -----------------------------------------------------------------------------

def test_the_table_and_the_thumbnails_keep_the_last_column_alone():
    cols = ['libs', 'collections', 'volumes', 'more volumes']
    assert shown_columns(cols, 'list', True, 3) == ['collections', 'volumes', 'more volumes']
    assert shown_columns(cols, 'table', True, 3) == ['more volumes']
    assert shown_columns(cols, 'gallery', True, 3) == ['more volumes']


def test_without_a_shelfmarks_pane_every_column_is_drawn():
    cols = ['libs', 'collections']
    assert not folds_columns('table', False)
    assert shown_columns(cols, 'table', False, 3) == cols
    assert shown_columns([], 'gallery', True, 3) == []


# -- the snippet ---------------------------------------------------------------------------------

def test_the_snippet_is_the_first_page_that_reads():
    assert snippet(['', '12 / ., ;', LONG_TEXT], pages=3).startswith('ויאמר')


def test_a_long_manuscripts_first_page_is_the_last_resort():
    title_page = 'MICROFILM ' + LONG_TEXT
    assert snippet([title_page, LONG_TEXT], pages=40) == snippet([LONG_TEXT], pages=1)
    # ... and taken when no other page has text
    assert snippet([title_page, '', ''], pages=40).startswith('MICROFILM')
    # a short manuscript's first page is its text
    assert snippet([title_page, LONG_TEXT], pages=2).startswith('MICROFILM')


def test_a_short_manuscript_reads_at_the_short_floor():
    marks = '1 2 3 . , ; 4 5'
    assert snippet([marks, SHORT_TEXT], pages=2).startswith('ויאמר')
    # a long one asks for more: none reads, so the first page with any text (after page one)
    assert snippet(['TITLE', marks, SHORT_TEXT], pages=40) == marks


def test_no_text_is_no_snippet():
    assert snippet(['', '  ', None], pages=3) == ''
    assert snippet([], pages=None) == ''


def test_the_snippet_is_one_line_cut_at_its_length():
    out = snippet(['שורה ראשונה של הטקסט\nשורה שנייה\r\n\nשלישית ' + LONG_TEXT], pages=1)
    assert '\n' not in out and '\r' not in out
    assert out.startswith('שורה ראשונה של הטקסט' + LINE_MARK + 'שורה שנייה' + LINE_MARK + 'שלישית')
    assert len(out) <= SNIPPET_CHARS + 1 and out.endswith('…')


def test_the_pages_are_read_one_at_a_time_and_no_more_than_needed():
    asked = []

    def pages():
        for i in range(50):
            asked.append(i)
            yield LONG_TEXT if i == 2 else ''
    assert snippet(pages(), pages=50)
    assert asked == [0, 1, 2]                  # stops at the page that reads

    asked.clear()

    def blank():
        for i in range(50):
            asked.append(i)
            yield ''
    assert snippet(blank(), pages=50) == ''
    assert asked == list(range(SNIPPET_PAGES))  # never past SNIPPET_PAGES


def test_wanted_batches_what_is_not_known():
    assert wanted(['a', 'b', 'a', '', None, 'c', 'd'], {'b': {}}, size=2) == [['a', 'c'], ['d']]
    assert wanted(['a'], {'a': {}}) == []


# -- row_details ---------------------------------------------------------------------------------

class _Meta:
    csv_bank = {'1': {'title': ' A   title '}, '2': {'title': ''}, '3': {}}


def _domains():
    return {'1': [{'domain': 'Letters', 'domain_heb': 'מכתבים'}, {'domain': 'Bible', 'domain_heb': ''}]}


@pytest.fixture
def sources(monkeypatch):
    calls = {'printed': [], 'domains': [], 'pgp': [], 'pgp_editions': [], 'fgp_editions': []}

    def source(key, answer):
        def fn(ids):
            calls[key].append(list(ids))
            return answer() if callable(answer) else answer
        return fn
    monkeypatch.setattr(rows, '_printed', source('printed', {'2'}))
    monkeypatch.setattr(rows, '_domains', source('domains', _domains))
    monkeypatch.setattr(rows, '_pgp', source('pgp', {'1', '9'}))
    monkeypatch.setattr(rows, '_pgp_editions', source('pgp_editions', {'3'}))
    monkeypatch.setattr(rows, '_fgp_editions', source('fgp_editions', set()))
    return calls


def test_every_field_from_its_source_in_one_batch_each(sources):
    texts = {'1': ['', LONG_TEXT], '2': [], '3': [LONG_TEXT]}
    counts = {'1': 2, '2': 0, '3': 1}
    got = row_details(['1', '2', '1', '3'], 'en', meta_mgr=_Meta(), page_count=counts.get,
                      page_texts=lambda s: iter(texts[s]), thumb_of=lambda s: f'/img/{s}')
    assert list(got) == ['1', '2', '3']
    assert all(set(r) == set(FIELDS) for r in got.values())
    assert got['1']['title'] == 'A title' and got['2']['title'] == '' and got['3']['title'] == ''
    assert got['1']['snippet'].startswith('ויאמר') and got['2']['snippet'] == ''
    assert [got[s]['pages'] for s in '123'] == [2, 0, 1]
    assert [got[s]['printed'] for s in '123'] == [False, True, False]
    assert [got[s]['pgp'] for s in '123'] == [True, False, False]
    assert [got[s]['edition'] for s in '123'] == [False, False, True]
    assert got['1']['domains'] == ['Letters', 'Bible'] and got['2']['domains'] == []
    assert got['3']['thumb'] == '/img/3'
    assert sources == dict.fromkeys(sources, [['1', '2', '3']])   # each source asked once, for the page


def test_the_domains_are_named_in_the_language_asked(sources):
    got = row_details(['1'], 'he')
    assert got['1']['domains'] == ['מכתבים', 'Bible']   # no Hebrew name: the English one


def test_a_manuscript_without_pages_reads_no_text(sources):
    read = []
    got = row_details(['2'], page_count=lambda s: 0, page_texts=lambda s: read.append(s) or iter([LONG_TEXT]))
    assert got['2']['snippet'] == '' and read == []


def test_a_failing_source_leaves_its_field_unknown_and_the_others_stand(monkeypatch, sources):
    monkeypatch.setattr(rows, '_pgp', _boom)
    got = row_details(['1', '3'], meta_mgr=_Meta(), page_count=lambda s: 1 / 0,
                      page_texts=lambda s: iter([LONG_TEXT]), thumb_of=lambda s: 1 / 0)
    assert got['1']['pgp'] is None and got['3']['pgp'] is None
    assert got['1']['pages'] is None and got['1']['thumb'] is None
    assert got['1']['snippet'].startswith('ויאמר')          # the page count unknown: the text still read
    assert got['3']['edition'] is True and got['1']['title'] == 'A title'


def test_a_missing_sidecar_is_unknown_not_none(monkeypatch, sources):
    monkeypatch.setattr(rows, '_printed', lambda ids: None)
    monkeypatch.setattr(rows, '_domains', lambda ids: None)
    monkeypatch.setattr(rows, '_pgp', lambda ids: None)
    got = row_details(['1'])
    assert got['1']['printed'] is None and got['1']['domains'] is None and got['1']['pgp'] is None
    assert got['1']['edition'] is False


def _boom(_ids):
    raise RuntimeError('down')


def test_printed_and_domains_fail_apart(monkeypatch, sources):
    monkeypatch.setattr(rows, '_printed', _boom)
    got = row_details(['1', '2'])
    assert got['2']['printed'] is None and got['1']['domains'] == ['Letters', 'Bible']


@pytest.mark.parametrize('failing, other, expected', [
    ('_pgp_editions', {'1'}, True),       # the other source says yes: yes
    ('_pgp_editions', set(), None),       # the other says no, this one could not answer: not known
    ('_fgp_editions', {'1'}, True),
    ('_fgp_editions', set(), None),
])
def test_an_edition_source_failing_keeps_the_others_yes(monkeypatch, sources, failing, other, expected):
    monkeypatch.setattr(rows, failing, _boom)
    monkeypatch.setattr(rows, ({'_pgp_editions', '_fgp_editions'} - {failing}).pop(), lambda ids: other)
    assert row_details(['1'])['1']['edition'] is expected


def test_either_edition_source_is_enough(monkeypatch, sources):
    monkeypatch.setattr(rows, '_pgp_editions', lambda ids: set())
    monkeypatch.setattr(rows, '_fgp_editions', lambda ids: {'2'})
    got = row_details(['1', '2'])
    assert got['1']['edition'] is False and got['2']['edition'] is True


def test_without_sources_given_the_fields_are_unknown(sources):
    got = row_details(['1'])
    assert got['1']['title'] is None and got['1']['pages'] is None and got['1']['snippet'] is None
    assert got['1']['thumb'] is None
    assert row_details([]) == {}


def test_the_real_sources_answer_without_their_data(monkeypatch):
    """Off the fixture: the real lookups, whatever data this machine has, never raise."""
    got = row_details(['990000000000000000'], 'en')
    assert set(got['990000000000000000']) == set(FIELDS)


class _Service:
    def __init__(self, available: bool):
        self.available = available

    def is_available(self):
        return self.available

    def get_printed_sys_ids(self, ids):
        return {ids[0]}

    def get_domains_for_sys_ids(self, ids):
        return {ids[0]: [{'domain': 'Letters'}]}


@pytest.mark.parametrize('available', [True, False])
def test_the_real_sources_tell_a_missing_sidecar_from_none(monkeypatch, available):
    import shared.document_service as documents
    import shared.fjms_service as fjms
    monkeypatch.setattr(fjms, 'get_fjms_service', lambda thread_safe=False: _Service(available))
    monkeypatch.setattr(documents, 'get_pgp_service', lambda: _Service(available))
    monkeypatch.setattr(documents, 'get_sys_ids_with_transcriptions', lambda ids: {ids[0]})
    got = row_details(['1', '2'])
    if available:
        assert [got[s]['printed'] for s in '12'] == [True, False]
        assert got['1']['domains'] == ['Letters'] and got['2']['domains'] == []
        assert [got[s]['pgp'] for s in '12'] == [True, False]
    else:
        assert all(got[s][k] is None for s in '12' for k in ('printed', 'domains', 'pgp'))

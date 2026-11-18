# -*- coding: utf-8 -*-
"""Near spellings in Title mode (catalogue plan §4.7, PR 2b).

With the option, each word of a Title query is found on its own, anywhere in the title and in
any order, as typed or as one of its near spellings (shared/near_spellings.py: Hebrew plene and
defective, one ו or י; Latin edits from five letters, kept only where a title holds them as a
word). Without it the search is today's phrase match, unchanged. Pinned here: the matcher
(shared/metadata_manager.py), the engine (shared/search_engine.py), the website's in-process
resolution and its worker (web/research_jobs.py), search-within replay (shared/refinement.py)
and the desktop's switch (genizah_app.py, desktop/gui_threads.py). The API is in
tests/test_catalog_text_api.py, the page in tests/render_smoke/test_title_near_spellings_render_smoke.py.
"""
from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from shared.metadata_manager import MetadataManager, _title_matcher, _title_word_spellings
from shared.catalog_text_rows import CATALOG_NEAR_TOOLTIP
from shared.near_spellings import TITLE_NEAR_SPELLINGS_TOOLTIP, near_spellings_tooltip
from shared.refinement import RefinementStep, replay_chain
from shared.search_engine import SearchEngine
from web import research_jobs
from web.research_jobs import INLINE_METADATA_ROWS, IsolatedEngine

TITLES = {
    'T1': 'תפלין של ראש',                  # defective
    'T2': 'תפילין של יד',                  # plene
    'T3': 'הלכות תפילין',
    'T4': 'פירוש תהילים',
    'T5': 'פרוש התורה (תהלים, קטע)',       # both words in other spellings, other order, apart
    'T6': 'תלמוד ירושלמי',                 # ירושלמ- inside a longer word: not ירושלם
    'T7': 'מכתב מירושלם',                  # ירושלם with its final mem
    'T8': 'פסקי הרמב"ם',
    'T9': 'הלכות רמבם',
    'T10': 'רמב״ם, הלכות תשובה',
    'L1': 'Letter of Saadya Gaon',
    'L2': 'Responsum of Saadiah',
    'L3': 'Commentary (Saadia)',
    'L4': 'A line of verse',
}


def _manager(titles=TITLES, nli=None):
    from shared.metadata_manager import _BoundedLRUCache
    mm = MetadataManager.__new__(MetadataManager)
    mm.csv_bank = {sid: {'shelfmark': f'X {sid}', 'title': t} for sid, t in titles.items()}
    mm._csv_bank_loaded = True
    # As the app holds it (MetadataManager.__init__ and the cache load).
    mm.nli_cache = _BoundedLRUCache(data={sid: {'shelfmark': '', 'title': t} for sid, t in (nli or {}).items()})
    return mm


def _near(mm, query):
    return sorted(mm.search_by_meta(query, 'title', near_spellings=True))


def _off(mm, query):
    return sorted(mm.search_by_meta(query, 'title'))


# ---------------------------------------------------------------- the matcher

def test_without_the_option_the_query_is_one_phrase_as_before():
    mm = _manager()
    assert _off(mm, 'פירוש תהלים') == []
    assert _off(mm, 'תפילין') == ['T2', 'T3']
    assert _off(mm, 'saadia') == ['L2', 'L3']


@pytest.mark.parametrize('typed', ['תפילין', 'תפלין'])
def test_plene_and_defective_find_each_other(typed):
    assert _near(_manager(), typed) == ['T1', 'T2', 'T3']


@pytest.mark.parametrize('typed', ['פירוש תהלים', 'תהלים פירוש', 'פרוש תהילים'])
def test_each_word_anywhere_in_any_order(typed):
    assert _near(_manager(), typed) == ['T4', 'T5']


def test_a_near_spelling_keeps_the_words_final_letter():
    """near_spellings works on the catalogue's folded keys; a title writes its finals. Its edits
    are inside the word, so each spelling ends as the word does and is looked for as written:
    ירושלם (final mem) is found, ירושלמי is another word and is not."""
    spellings = _title_word_spellings('תפילין')
    assert spellings[0] == 'תפילין' and 'תפלין' in spellings
    assert all(s.endswith('ן') for s in spellings)
    assert _near(_manager(), 'ירושלים') == ['T7']


def test_an_abbreviation_is_found_by_its_mark_as_without_the_option():
    mm = _manager()
    assert _near(mm, 'רמב"ם') == ['T10', 'T8']                  # the mark, in either spelling
    assert _near(mm, 'רמבם') == ['T10', 'T8', 'T9']             # no mark: the abbreviation too
    assert _near(mm, 'הלכות רמב"ם') == ['T10']                  # each word on its own, any order


def test_a_latin_near_spelling_counts_only_where_a_title_holds_it():
    mm = _manager()
    assert _title_word_spellings('saadia', mm._title_latin_near) == ['saadia', 'saadiah', 'saadya']
    assert _near(mm, 'Saadia') == ['L1', 'L2', 'L3']
    assert _near(mm, 'Gaon Saadia') == ['L1']


def test_a_latin_word_under_five_letters_has_none_and_reads_no_titles():
    mm = _manager()
    assert _title_word_spellings('line', mm._title_latin_near) == ['line']
    assert _near(mm, 'lime') == []
    assert mm._title_latin_words_cache is None


def test_a_latin_words_kept_spellings_are_remembered(monkeypatch):
    """near_spellings keeps nothing (two edits are ~100,000 strings), so the Title search
    remembers what it kept, per catalogue size."""
    import shared.near_spellings as ns
    calls = []
    real = ns.near_spellings
    monkeypatch.setattr(ns, 'near_spellings', lambda w: calls.append(w) or real(w))
    mm = _manager()
    for _ in range(3):
        assert _near(mm, 'saadia') == ['L1', 'L2', 'L3']
    assert calls == ['saadia']
    mm.csv_bank['L5'] = {'shelfmark': 'X L5', 'title': 'Saadja'}     # the catalogue grew
    assert _near(mm, 'saadia') == ['L1', 'L2', 'L3', 'L5']
    assert calls == ['saadia', 'saadia']


def test_the_latin_words_follow_a_growing_catalogue():
    mm = _manager()
    assert _near(mm, 'saadia') == ['L1', 'L2', 'L3']
    mm.csv_bank['L5'] = {'shelfmark': 'X L5', 'title': 'Saadja'}
    assert _near(mm, 'saadia') == ['L1', 'L2', 'L3', 'L5']


def test_a_latin_spelling_a_newly_fetched_nli_title_holds_is_found():
    """The Latin words are read again when the NLI cache changes, not only when the catalogue
    grows: search_by_meta scans the NLI titles as they are now (Codex review of PR #49)."""
    from shared.metadata_manager import _BoundedLRUCache
    mm = _manager({'L3': TITLES['L3']})
    mm.nli_cache = _BoundedLRUCache(data={'N1': {'shelfmark': '', 'title': 'Commentary'}})
    assert _near(mm, 'saadia') == ['L3']
    mm.nli_cache['N2'] = {'shelfmark': '', 'title': 'Letter of Saadya'}         # a new entry
    assert _near(mm, 'saadia') == ['L3', 'N2']
    mm.nli_cache['N1'] = {'shelfmark': '', 'title': 'Saadja on Exodus'}         # same size, new title
    assert _near(mm, 'saadia') == ['L3', 'N1', 'N2']
    mm.nli_cache.merge_fields('N1', {'title': 'Exodus'})                       # and taken out again
    assert _near(mm, 'saadia') == ['L3', 'N2']


def test_a_plain_dict_nli_cache_is_read_every_time():
    """A dict counts no writes, and its size misses a title replaced in place: its words are
    read again for every search (Codex review of PR #49, round 3)."""
    mm = _manager({'L3': TITLES['L3']})
    mm.nli_cache = {'N1': {'shelfmark': '', 'title': 'Commentary'}}
    assert _near(mm, 'saadia') == ['L3']
    mm.nli_cache['N1'] = {'shelfmark': '', 'title': 'Saadya on Exodus'}
    assert _near(mm, 'saadia') == ['L3', 'N1']


def test_the_nli_cache_counts_its_writes():
    from shared.metadata_manager import _BoundedLRUCache
    cache = _BoundedLRUCache(data={'a': {}})
    start = cache.revision
    cache['b'] = {}
    cache.merge_fields('b', {'title': 'x'})
    _ = cache.get('a'), cache['b'], cache.items()                            # reads do not count
    assert cache.revision == start + 2


def test_the_nli_cache_titles_are_searched_with_near_spellings_too():
    mm = _manager({'T2': TITLES['T2']}, nli={'N1': 'תפלין'})
    assert _near(mm, 'תפילין') == ['N1', 'T2']


@pytest.mark.parametrize('typed', ['"', '""', "'", ' ', ''])
def test_quote_marks_alone_find_nothing(typed):
    assert _title_matcher(typed, True) is None
    assert _manager().search_by_meta(typed, 'title', near_spellings=True) == []


def test_without_the_option_the_matcher_reads_no_latin_words():
    asked = []
    match = _title_matcher('saadia', False, lambda word: asked.append(word) or frozenset())
    assert match('Saadia') and asked == []


# ---------------------------------------------------------------- the engine

def _engine(meta_mgr):
    with patch.object(SearchEngine, '__init__', lambda self, *a, **kw: None):
        engine = SearchEngine.__new__(SearchEngine)
    engine.meta_mgr = meta_mgr
    engine.searcher = None
    engine.index = None
    return engine


def _meta_mock():
    meta_mgr = MagicMock()
    meta_mgr.search_by_meta.return_value = ['1']
    meta_mgr.get_meta_for_id.side_effect = lambda sid: (f'X {sid}', 'title')
    meta_mgr.get_library_for_id.return_value = 'CUL'
    return meta_mgr


def test_metadata_search_ids_passes_the_option_for_title_only():
    meta_mgr = _meta_mock()
    engine = _engine(meta_mgr)
    engine.metadata_search_ids('x', 'Title', near_spellings=True)
    meta_mgr.search_by_meta.assert_called_with('x', 'title', near_spellings=True)
    engine.metadata_search_ids('x', 'Title')
    meta_mgr.search_by_meta.assert_called_with('x', 'title')
    engine.metadata_search_ids('T-S 1', 'Shelfmark', near_spellings=True)
    meta_mgr.search_by_meta.assert_called_with('T-S 1', 'shelfmark')


@pytest.mark.parametrize('mode, options, expected', [
    ('Title', {'near_spellings': True}, {'near_spellings': True}),
    ('Title', {'near_spellings': False}, {}),
    ('Title', None, {}),
    ('Shelfmark', {'near_spellings': True}, {}),
])
def test_execute_search_reads_the_option_from_catalog_options(mode, options, expected):
    meta_mgr = _meta_mock()
    rows = _engine(meta_mgr).execute_search('x', mode, 0, catalog_options=options)
    assert [r['display']['id'] for r in rows] == ['1']
    assert meta_mgr.search_by_meta.call_args.kwargs == expected


def test_execute_search_on_the_real_matcher():
    mm = _manager()
    mm.get_meta_for_id = lambda sid: (mm.csv_bank[sid]['shelfmark'], mm.csv_bank[sid]['title'])
    mm.get_library_for_id = lambda sid: ''
    rows = _engine(mm).execute_search('תפילין', 'Title', 0, catalog_options={'near_spellings': True})
    assert sorted(r['display']['id'] for r in rows) == ['T1', 'T2', 'T3']
    rows = _engine(mm).execute_search('תפילין', 'Title', 0)
    assert sorted(r['display']['id'] for r in rows) == ['T2', 'T3']


# ---------------------------------------------------------------- the website: resolved here, or a worker

class _Fake:
    """An engine as IsolatedEngine sees it, recording how it was asked."""

    def __init__(self, ids):
        self.ids = list(ids)
        self.meta_mgr = SimpleNamespace(_csv_bank_loaded=True)
        self.calls = []

    def metadata_search_ids(self, query_str, mode, restrict_sys_ids=None, **kwargs):
        self.calls.append(('resolve', mode, kwargs))
        return self.ids

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None, ids_only=False,
                       metadata_sys_ids=None, sources=None, catalog_options=None):
        self.calls.append(('run', metadata_sys_ids, catalog_options))
        return [{'display': {'id': sid}} for sid in metadata_sys_ids or []]


def test_a_title_lookup_is_resolved_here_with_near_spellings(monkeypatch):
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: pytest.fail('a worker was asked for'))
    fake = _Fake(['1', '2'])
    rows = IsolatedEngine(fake, 'search').execute_search('תפילין', 'Title', 0,
                                                         catalog_options={'near_spellings': True})
    assert [r['display']['id'] for r in rows] == ['1', '2']
    assert fake.calls == [('resolve', 'Title', {'near_spellings': True}),
                          ('run', ['1', '2'], {'near_spellings': True})]


@pytest.mark.parametrize('mode, options', [('Title', None), ('Title', {'near_spellings': False}),
                                           ('Shelfmark', {'near_spellings': True})])
def test_without_the_option_resolution_is_asked_as_before(monkeypatch, mode, options):
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: pytest.fail('a worker was asked for'))
    fake = _Fake(['1'])
    extra = {} if options is None else {'catalog_options': options}
    IsolatedEngine(fake, 'search').execute_search('x', mode, 0, **extra)
    assert fake.calls[0] == ('resolve', mode, {})


def test_a_broad_title_search_goes_to_a_worker_with_the_option(monkeypatch):
    submitted = []

    def submit(payload):
        submitted.append(payload)
        future = Future()
        future.set_result({'value': [], 'cutoff': {}, 'time_limit': False})
        return SimpleNamespace(future=future, status='Searching', progress=(0, 0),
                               preview=None, preview_sequence=0)
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: SimpleNamespace(
        submit=submit, position=lambda job: 0, cancel=lambda job: None))
    ids = [str(i) for i in range(INLINE_METADATA_ROWS + 1)]
    IsolatedEngine(_Fake(ids), 'search').execute_search('x', 'Title', 0, catalog_options={'near_spellings': True})
    arguments = submitted[0]['arguments']
    assert arguments['metadata_sys_ids'] == ids
    # Were the catalogue still loading here, the worker resolves them itself, with the option.
    assert arguments['catalog_options'] == {'near_spellings': True}


# ---------------------------------------------------------------- search within

def test_a_title_step_replays_its_near_spellings():
    step = RefinementStep(query='תפילין', mode='Title', catalog_options={'near_spellings': True})
    step = RefinementStep.from_dict(step.to_dict())
    searcher = MagicMock()
    searcher.execute_search.return_value = [{'uid': '', 'display': {'id': '99123'}}]
    replay_chain([step], searcher, None)
    assert searcher.execute_search.call_args.kwargs['catalog_options'] == {'near_spellings': True}


# ---------------------------------------------------------------- the switch's text, both apps

def test_the_title_tooltip_adds_that_the_words_are_found_in_any_order():
    assert near_spellings_tooltip('Catalog', str) == CATALOG_NEAR_TOOLTIP
    assert near_spellings_tooltip('Title', str) == CATALOG_NEAR_TOOLTIP + '. ' + TITLE_NEAR_SPELLINGS_TOOLTIP


def test_the_switch_strings_are_translated():
    from shared.genizah_translations import TRANSLATIONS
    for key in ('Fuzzy search', CATALOG_NEAR_TOOLTIP, TITLE_NEAR_SPELLINGS_TOOLTIP):
        assert TRANSLATIONS.get(key), key


# ---------------------------------------------------------------- the desktop

@pytest.mark.gui      # a QThread, and genizah_app below: the gui bucket
def test_the_desktop_thread_passes_the_option_only_when_set():
    from desktop.gui_threads import SearchThread
    seen = []

    class _Searcher:
        def execute_search(self, *args, **kwargs):
            seen.append(kwargs)
            return []
    for options in ({'near_spellings': True}, None):
        thread = SearchThread(_Searcher(), 'תפילין', 'Title', 0, catalog_options=options)
        thread.run()
    assert seen[0]['catalog_options'] == {'near_spellings': True}
    assert 'catalog_options' not in seen[1]


@pytest.mark.gui
def test_the_desktop_step_records_the_option():
    from genizah_app import GenizahGUI
    host = SimpleNamespace(_last_search_params=dict(
        query='q', gap=0, exclude_words=[], text_position=None, responsa_options=None,
        corpus_scope='genizah', variant_settings=None, sources=None,
        catalog_options={'near_spellings': True}))
    step = RefinementStep(query='q', mode='Title')
    GenizahGUI._apply_run_params(host, step)
    assert step.catalog_options == {'near_spellings': True}


@pytest.mark.gui
def test_the_desktop_shows_the_catalogues_switch_in_title_mode_without_translations(monkeypatch):
    """One Fuzzy search switch, as on the website: the catalogue's (PR 3), shown in Title mode
    too, without the translations switch, its tooltip saying the words are found in any order."""
    from PyQt6.QtWidgets import QApplication, QCheckBox, QWidget
    import genizah_app
    import genizah_core
    from desktop.catalog_search import MODE_INDEX_CATALOG
    from genizah_app import GenizahGUI
    # The asserted tooltips are English: tr() must not read this machine's language.
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    monkeypatch.setattr(genizah_app, 'CURRENT_LANG', 'en', raising=False)
    QApplication.instance() or QApplication([])
    window = QWidget()
    row = QWidget(window)
    host = SimpleNamespace(MODE_RESPONSA=2, MODE_PGP_TAGS=7, _update_sources_visibility=lambda: None,
                           catalog_sub_row=row, chk_catalog_near=QCheckBox(row),
                           chk_catalog_translations=QCheckBox(row))
    seen = {}
    for name, index in (('title', 5), ('catalog', MODE_INDEX_CATALOG), ('exact', 0)):
        GenizahGUI._on_search_mode_changed(host, index)
        seen[name] = (not row.isHidden(), not host.chk_catalog_translations.isHidden(),
                      host.chk_catalog_near.toolTip())
    assert seen['title'] == (True, False, CATALOG_NEAR_TOOLTIP + '. ' + TITLE_NEAR_SPELLINGS_TOOLTIP), seen
    assert seen['catalog'] == (True, True, CATALOG_NEAR_TOOLTIP), seen
    assert seen['exact'][0] is False, seen


@pytest.mark.gui
def test_a_desktop_title_run_keeps_the_option_in_its_history_entry():
    from genizah_app import GenizahGUI
    host = SimpleNamespace(_last_search_params={'catalog_options': {'near_spellings': True}})
    assert GenizahGUI._history_catalog_options(host) == {'near_spellings': True}


def test_the_desktop_title_run_searches_and_records_the_option():
    """start_search reads the catalogue's switches for Catalog mode only; for Title mode the
    thread and the run's parameters get the near-spellings option after they are made."""
    import ast
    import genizah_app
    tree = ast.parse(open(genizah_app.__file__, encoding='utf-8').read())
    start = ast.unparse(next(f for f in ast.walk(tree)
                             if isinstance(f, ast.FunctionDef) and f.name == 'start_search'))
    made = start.index('self.search_thread = SearchThread(')
    title = start.index("if mode == 'Title' and getattr(self, '_last_search_params', None) is not None")
    assert made < start.index('self._last_search_params = dict(') < title < start.index('self.search_thread.start()')
    assert "self.search_thread.catalog_options = {'near_spellings': True}" in start[title:]
    assert "self._last_search_params['catalog_options'] = {'near_spellings': True}" in start[title:]


def test_title_is_index_5_of_the_desktop_mode_combo():
    """_on_search_mode_changed takes Title mode to be index 5 of the mode combo."""
    import ast
    import genizah_app
    text = open(genizah_app.__file__, encoding='utf-8').read()
    combo = next(n for n in ast.walk(ast.parse(text)) if isinstance(n, ast.Call)
                 and ast.unparse(n.func) == 'self.mode_combo.addItems')
    labels = [ast.unparse(e) for e in combo.args[0].elts]
    assert labels[5] == "tr('Title ($)')"

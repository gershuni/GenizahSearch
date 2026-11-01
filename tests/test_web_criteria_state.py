# -*- coding: utf-8 -*-
"""The web pages' catalogue-criteria state (PR 1b of
docs/plans/CATALOGUE_CRITERIA_PLAN.md): web/components/filter_panel.py and
web/pages/search_state.py.

* saved state: criteria restored, old dates read in as the period (D18), old
  measurement keys and their stored rows retired (R2), a 5.0.0 file dropping
  what it cannot answer (rule 9);
* the browse -> search / parallels handoff carries the criteria unchanged;
* the live count and the search pass them to the service;
* history summaries and runs carry them;
* post-search filters mean what the panel means (D20).
Data: tests/criteria_sidecar.py.
"""
import ast
import asyncio
import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, B, C, D, E, F, UNIVERSE, build_criteria_sidecar  # noqa: E402
from fjms_filter_sidecar import brackets_balanced  # noqa: E402

import web.components.filter_panel as fp  # noqa: E402
import web.pages.search_state as ss  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.fjms_service import FilterUnavailable, FjmsService  # noqa: E402


@pytest.fixture(scope='module')
def sidecar(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('crit') / 'fjms.db')


@pytest.fixture(scope='module')
def old_sidecar(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('crit50') / 'fjms.db', upgrade=False)


@pytest.fixture
def svc(sidecar, monkeypatch):
    service = FjmsService(db_path=sidecar)
    monkeypatch.setattr(fjms_service, '_default_service', service)
    return service


@pytest.fixture
def old_svc(old_sidecar, monkeypatch):
    service = FjmsService(db_path=old_sidecar)
    monkeypatch.setattr(fjms_service, '_default_service', service)
    return service


@pytest.fixture
def storage(monkeypatch):
    """The visitor's storage, as a dict; persist_value writes into it."""
    import web.safe_storage as safe
    store = {}
    monkeypatch.setattr(safe, 'safe_user_get', lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(safe, 'safe_user_set', lambda key, value: store.__setitem__(key, value))
    monkeypatch.setattr(safe, 'safe_user_pop',
                        lambda key, default=None: store.pop(key, default))
    monkeypatch.setattr(ss, 'safe_user_set', lambda key, value: store.__setitem__(key, value))
    return store


def _state(**over):
    st = ss.SearchUIState()
    for key, value in over.items():
        setattr(st, key, value)
    return st


# ── saved state ─────────────────────────────────────────────────────────

def test_saved_criteria_are_restored(svc, storage):
    storage['search_filter_criteria'] = {'style': {'values': ['Square']}}
    st = _state()
    assert fp.load_filter_state(st, 'search') == []
    assert st.filter_criteria == {'style': {'values': ['Square'], 'mode': 'only'}}


def test_saved_dates_become_the_period(svc, storage):
    storage.update({'parallels_filter_date_from': 1000, 'parallels_filter_date_to': 1200})
    st = _state()
    fp.load_filter_state(st, 'parallels')
    assert st.filter_criteria == {'period': {'min': 1000, 'max': 1200}}
    # Read in once: the criteria are saved and the old keys cleared.
    assert storage['parallels_filter_criteria'] == st.filter_criteria
    assert storage['parallels_filter_date_from'] is None


def test_saved_criteria_win_over_saved_dates(svc, storage):
    storage.update({'search_filter_criteria': {'period': {'min': 900}},
                    'search_filter_date_from': 1000})
    st = _state()
    fp.load_filter_state(st, 'search')
    assert st.filter_criteria == {'period': {'min': 900}}


def test_a_5_0_0_file_drops_saved_criteria_it_cannot_answer(old_svc, storage):
    storage['search_filter_criteria'] = {'lines': {'min': 20, 'max': 25, 'source': 'measured'},
                                         'region': {'values': ['Spanish']}}
    st = _state()
    # Lines needs the 5.1.0 table even for its measured half (rule 9).
    assert fp.load_filter_state(st, 'search') == ['lines']
    assert st.filter_criteria == {'region': {'values': ['Spanish'], 'mode': 'only'}}
    assert storage['search_filter_criteria'] == st.filter_criteria


_RESET = {'search_results': [], 'search_last_run': None}


def test_old_measurement_filters_and_their_rows_are_retired(storage):
    storage.update({'search_filter_line_count_min': 20, 'search_filter_script_styles': ['Square'],
                    'search_results': [{'sys_id': A}], 'search_last_run': {'url_stamp': 'k3'}})
    assert fp.retire_old_saved_filters('search', reset=_RESET) is True
    assert storage['search_filter_line_count_min'] is None
    assert storage['search_filter_script_styles'] is None
    # The rows AND the run that would bring them back as a finished search.
    assert storage['search_results'] == [] and storage['search_last_run'] is None


def test_rows_stay_when_no_measurement_filter_was_saved(storage):
    storage.update({'search_filter_script_styles': ['Square'], 'search_results': [{'sys_id': A}]})
    assert fp.retire_old_saved_filters('search', reset=_RESET) is False
    assert storage['search_results'] == [{'sys_id': A}]


def test_a_retired_search_runs_again_from_its_address():
    # Codex round 1: with the run kept, a stamped Back restored "no rows".
    from web.search_bootstrap import resolve_search_bootstrap
    boot = resolve_search_bootstrap(
        initial_query='שבת', initial_tag=None, initial_mode=None, initial_domain=None,
        from_browse=None, saved_mode='exact', saved_query='שבת', saved_results_count=0,
        use_slider=False, initial_responsa={}, initial_run='k3', saved_run=None, live_run=None)
    assert boot['restore_saved_results'] is False and boot['query'] == 'שבת'


def test_an_empty_saved_criteria_is_an_answer_not_a_missing_one(svc, storage):
    # Codex round 1: Clear All, then old dates came back as the period.
    storage.update({'parallels_filter_criteria': {}, 'parallels_filter_date_from': 1000})
    st = _state()
    fp.load_filter_state(st, 'parallels')
    assert st.filter_criteria == {}
    assert fp.saved_criteria_of({'criteria': {}, 'date_from': 1000}) == {}
    assert fp.saved_criteria_of({'date_from': 1000}) == {'period': {'min': 1000}}


def test_clearing_the_filters_clears_the_old_keys_too(storage):
    storage.update({'search_filter_criteria': {'style': {'values': ['Square']}},
                    'search_filter_date_from': 1000, 'search_filter_width_min': 3.0})
    ss.clear_search_filters()
    assert storage['search_filter_criteria'] == {}
    assert storage['search_filter_date_from'] is None and storage['search_filter_width_min'] is None


# ── handoff, count, search ──────────────────────────────────────────────

@pytest.mark.parametrize('prefix', ['search', 'parallels'])
def test_a_browse_list_hands_its_criteria_on_unchanged(svc, storage, prefix):
    wanted = {'material': {'values': ['Vellum'], 'mode': 'except', 'source': 'catalogue'},
              'width': {'min': 10, 'max': 15, 'source': 'either'}}
    storage['incoming_filters'] = {'criteria': wanted}
    st = _state()
    assert fp.consume_incoming_filters(st, prefix)
    assert st.filter_criteria == wanted
    assert storage[f'{prefix}_filter_criteria'] == wanted
    assert 'incoming_filters' not in storage


def test_a_handoff_without_criteria_clears_saved_ones(svc, storage):
    # Codex round 1: a domain-only list must not search under an earlier Square.
    storage.update({'search_filter_criteria': {'style': {'values': ['Square']}},
                    'incoming_filters': {'domain': 'Bible'}})
    st = _state()
    fp.consume_incoming_filters(st, 'search')
    assert st.filter_criteria == {} and storage['search_filter_criteria'] == {}


def test_a_handoff_drops_what_a_5_0_0_file_cannot_answer_and_says_so(old_svc, storage,
                                                                      monkeypatch):
    said = []
    monkeypatch.setattr(fp, 'notify_dropped_filters', lambda: said.append(True))
    storage['incoming_filters'] = {'criteria': {'columns': {'min': 2},
                                                'style': {'values': ['Square']}}}
    st = _state()
    fp.consume_incoming_filters(st, 'search')
    assert st.filter_criteria == {'style': {'values': ['Square'], 'mode': 'only'}}
    assert said == [True]


@pytest.mark.parametrize('saved', [
    {'criteria': {'lines': {'min': 20}, 'region': {'values': ['Spanish']}}},   # a history entry
    {'date_from': 1000, 'domains': ['Bible']},                                 # an old one: period
])
def test_every_restore_path_drops_what_a_5_0_0_file_cannot_answer(old_svc, saved, monkeypatch):
    said = []
    monkeypatch.setattr(fp, 'notify_dropped_filters', lambda: said.append(True))
    criteria = fp.restore_saved_criteria(saved)
    assert not set(criteria) & {'lines', 'period'}, criteria
    assert said == [True]


def test_a_run_restore_reads_old_dates_and_checks_the_file(old_svc, sidecar, monkeypatch):
    monkeypatch.setattr(fp, 'notify_dropped_filters', lambda: None)
    fresh = _state()
    ss.apply_run_filter_state(fresh, {'filter_date_from': 1000, 'filter_date_to': 1100})
    assert fresh.filter_criteria == {}        # the 5.0.0 file cannot answer the period
    monkeypatch.setattr(fjms_service, '_default_service', FjmsService(db_path=sidecar))
    ss.apply_run_filter_state(fresh, {'filter_date_from': 1000, 'filter_date_to': 1100})
    assert fresh.filter_criteria == {'period': {'min': 1000, 'max': 1100}}


def test_an_old_handoff_brings_its_dates_as_the_period(svc, storage):
    storage['incoming_filters'] = {'date_from': 1000, 'date_to': 1100}
    st = _state()
    fp.consume_incoming_filters(st, 'search')
    assert st.filter_criteria == {'period': {'min': 1000, 'max': 1100}}


def test_the_live_count_passes_the_criteria(svc, storage, monkeypatch):
    seen = {}
    real = FjmsService.get_filter_sys_ids

    def spy(self, **kwargs):
        seen.update(kwargs)
        return real(self, **kwargs)
    monkeypatch.setattr(FjmsService, 'get_filter_sys_ids', spy)
    monkeypatch.setattr(fp, 'corpus_universe', lambda: (lambda: set(UNIVERSE)))

    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)
    monkeypatch.setattr(fp.run, 'io_bound', _io_bound)
    st = _state(filter_criteria={'region': {'values': ['_none']}})
    asyncio.run(fp.recompute_filter_count(st, lambda: None))
    assert seen['criteria'] == {'region': {'values': ['_none']}}
    assert st.restrict_sys_ids == {D, E, F} and st.filter_manuscript_count == 3


def test_criteria_alone_are_active_filters():
    assert fp.has_active_filters(_state(filter_criteria={'columns': {'not_recorded': True}}))
    assert not fp.has_active_filters(_state(filter_criteria={'style': {'values': []}}))
    assert not fp.has_active_filters(_state())


def test_the_history_summary_names_the_criteria(svc):
    summary = fp.build_filter_summary(
        {'include_mode': False, 'domains': ['Bible'],
         'criteria': {'region': {'values': ['Oriental'], 'mode': 'except'}}},
        lambda t: t, lambda: 'en', max_len=200)
    # Exclude mode never turns a criterion around (rule 7).
    assert summary == '[exclude: Bible] [include: Region: all except Oriental]', summary


def test_an_old_history_entry_summarises_its_dates_as_the_period(svc):
    assert fp.build_filter_summary({'date_from': 1000, 'date_to': 1200}, lambda t: t,
                                   lambda: 'en', max_len=200) == \
        '[include: Period: 1000–1200]'


def test_a_run_keeps_its_own_copy_of_the_criteria():
    st = _state(filter_criteria={'style': {'values': ['Square'], 'mode': 'only'}})
    saved = ss.run_filter_state(st)
    assert saved['filter_criteria'] == st.filter_criteria
    saved['filter_criteria']['style']['values'].append('Cursive')
    assert st.filter_criteria['style']['values'] == ['Square']
    fresh = _state()
    assert ss.apply_run_filter_state(fresh, {'filter_criteria': {'lines': {'min': 3}}})
    assert fresh.filter_criteria == {'lines': {'min': 3, 'source': 'either'}}
    # A run saved before the criteria: none, never its old measurement fields.
    assert ss.apply_run_filter_state(fresh, {'filter_width_min': 10.0})
    assert fresh.filter_criteria == {} and not hasattr(fresh, 'filter_width_min')


# ── post-search filters (D20) ───────────────────────────────────────────

def _rows(ids):
    return [{'display': {'id': sid}} for sid in ids]


@pytest.mark.parametrize('criteria', [
    {'width': {'min': 10, 'max': 12}},
    {'lines': {'min': 22, 'max': 22}},
    {'lines': {'min': 20, 'max': 20, 'source': 'measured'},
     'width': {'min': 10, 'max': 12, 'source': 'measured'}},
    {'width': {'min': 10, 'max': 12}, 'margin_left': {'min': 1, 'max': 2}},
    {'material': {'values': ['Paper']}},
    {'line_height': {'not_recorded': True}},
])
def test_post_search_filters_mean_what_the_panel_means(svc, criteria):
    result = [A, C, D, E]
    st = _state(post_filter_criteria=criteria)
    panel = svc.get_filter_sys_ids(criteria=criteria, universe=set(UNIVERSE))
    assert ss.post_criteria_matches(st, _rows(result)) == panel & set(result)


def test_post_search_matches_are_kept_per_criteria(svc, monkeypatch):
    calls = []
    real = FjmsService.match_result_criteria

    def spy(self, criteria, sys_ids):
        calls.append(set(sys_ids))
        return real(self, criteria, sys_ids)
    monkeypatch.setattr(FjmsService, 'match_result_criteria', spy)
    st = _state(post_filter_criteria={'width': {'min': 10, 'max': 12}})
    ss.post_criteria_matches(st, _rows([A, B]))
    ss.post_criteria_matches(st, _rows([A, B, D]))     # only D is new
    assert calls == [{A, B}, {D}]
    st.post_filter_criteria = {'width': {'min': 12, 'max': 14}}
    ss.post_criteria_matches(st, _rows([A]))           # new criteria: asked again
    assert calls[-1] == {A}


def test_a_render_never_asks_the_sidecar(svc, monkeypatch):
    # Codex round 1: render paths run on the event loop -- cache only.
    st = _state(post_filter_criteria={'width': {'min': 10, 'max': 12}})
    assert ss.post_criteria_matches(st, _rows([A, B]), fetch=False) is None
    ss.post_criteria_matches(st, _rows([A, B]))
    monkeypatch.setattr(FjmsService, 'match_result_criteria',
                        lambda *a, **k: pytest.fail('asked the sidecar'))
    assert ss.post_criteria_matches(st, _rows([A, B]), fetch=False) == {A, B}


def test_post_search_filters_fail_closed(old_svc):
    st = _state(post_filter_criteria={'lines': {'min': 20, 'max': 25}})
    with pytest.raises(FilterUnavailable):
        ss.post_criteria_matches(st, _rows([A, B]))


def test_post_search_filters_hold_back_the_streamed_preview():
    assert ss.preview_shows_final_rows(_state())
    assert not ss.preview_shows_final_rows(_state(post_filter_criteria={'width': {'min': 3}}))


def test_no_page_state_field_is_left_from_the_old_controls():
    st = ss.SearchUIState()
    for name in ('filter_date_from', 'filter_script_styles', 'filter_width_min',
                 'post_filter_width_min', 'post_filter_measurement_material'):
        assert not hasattr(st, name), name
    assert st.filter_criteria == {} and st.post_filter_criteria == {}


# ── call sites ──────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent


def _filter_kwargs_values(path: Path) -> list:
    """{keyword: the attributes and names its value reads} for every
    ``filter_kwargs = dict(...)`` in a page."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    return [
        {kw.arg: {getattr(n, 'attr', None) or getattr(n, 'id', None) for n in ast.walk(kw.value)
                  if isinstance(n, (ast.Attribute, ast.Name))}
         for kw in node.value.keywords}
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
        and getattr(node.value.func, 'id', None) == 'dict'
        and any(getattr(t, 'id', None) == 'filter_kwargs' for t in node.targets)
    ]


@pytest.mark.parametrize('page', ['web/pages/search.py', 'web/pages/parallels.py'])
def test_the_search_itself_is_restricted_by_the_criteria(page):
    """The count is one call site; the search's own lookup is another. Its
    ``criteria`` keyword must READ the page's criteria -- ``criteria=None``
    would run the search over every manuscript."""
    found = _filter_kwargs_values(ROOT / page)
    assert found, f"{page}: no filter_kwargs = dict(...) found; update this guard"
    for kws in found:
        assert '_dispatch_criteria' in kws.get('criteria', set()), (page, kws.get('criteria'))
        for old in ('date_from', 'script_styles', 'width_min', 'measurement_material'):
            assert old not in kws, (page, old)
    # ...read from the page's criteria once, when the search is pressed; its
    # history entry and export record that same snapshot (Codex round 1).
    src = (ROOT / page).read_text(encoding='utf-8')
    assert re.search(r"_dispatch_criteria = copy\.deepcopy\(\w+\.filter_criteria or \{\}\)", src)
    assert src.count("'criteria': copy.deepcopy(_dispatch_criteria)") >= 2, page
    # ...and nothing copies the live criteria after that (a late copy can record
    # what the panel says now, not what the search ran with).
    assert len(re.findall(r"copy\.deepcopy\((?:getattr\()?\w+[.,] ?'?filter_criteria", src)) == 1, page


def test_exclude_mode_never_turns_a_criterion_around(svc, monkeypatch):
    async def _io_bound(fn, *a, **k):
        return fn(*a, **k)
    monkeypatch.setattr(fp.run, 'io_bound', _io_bound)
    monkeypatch.setattr(fp, 'corpus_universe', lambda: (lambda: set(UNIVERSE)))
    st = _state(filter_include_mode=False, filter_criteria={'style': {'values': ['Square']}})
    asyncio.run(fp.recompute_filter_count(st, lambda: None))
    assert st.restrict_sys_ids == {A}


LONG_EXCLUDE = {'include_mode': False, 'domains': ['Bible', 'Liturgy', 'Halakha'],
                'criteria': {'period': {'min': 1000, 'max': 1300},
                             'region': {'values': ['Spanish']}}}


def test_a_truncated_summary_keeps_its_brackets_balanced():
    for max_len in range(20, 90):  # every cut point, inside either bracket or between them
        s = fp.build_filter_summary(LONG_EXCLUDE, lambda t: t, lambda: 'en', max_len)
        assert len(s) <= max_len and brackets_balanced(s), (max_len, s)


@pytest.mark.parametrize('page', ['web/pages/search.py', 'web/pages/parallels.py'])
def test_every_page_restore_path_checks_the_file(page):
    """History entries, page snapshots and runs restore criteria through
    restore_saved_criteria (rule 9), never the unchecked saved_criteria_of."""
    src = (ROOT / page).read_text(encoding='utf-8')
    assert 'saved_criteria_of(' not in src, page
    assert src.count('restore_saved_criteria(') >= 1, page


def test_the_search_page_renders_post_filters_from_the_cache_only():
    src = (ROOT / 'web/pages/search.py').read_text(encoding='utf-8')
    body = src[src.index('    def _apply_measurement_post_filters('):]
    body = body[:body.index('\n    def ', 10)]
    assert 'post_criteria_matches(state, results, fetch=False)' in body
    assert '_refill_post_matches(results)' in body


# ── Codex round 2 ───────────────────────────────────────────────────────

def test_a_saved_run_with_old_measurement_filters_retires_too(storage):
    """A search with width >= 10, then Clear All without a new search: the
    flat keys are empty, but the rows and the run that ran them remain."""
    storage.update({'search_results': [{'sys_id': A}],
                    'search_last_run': {'pre_search_filters': {'filter_width_min': 10.0}}})
    assert fp.retire_old_saved_filters('search', reset=_RESET, run_key='search_last_run')
    assert storage['search_results'] == [] and storage['search_last_run'] is None
    storage.update({'search_results': [{'sys_id': A}],
                    'search_last_run': {'pre_search_filters': {'filter_criteria': {}}}})
    assert not fp.retire_old_saved_filters('search', reset=_RESET, run_key='search_last_run')
    assert storage['search_results'] == [{'sys_id': A}]


@pytest.mark.parametrize('page', ['web/pages/search.py', 'web/pages/parallels.py'])
def test_whether_a_search_had_filters_is_decided_when_it_is_pressed(page):
    """Codex round 2: the only criterion removed while the search ran left its
    history entry (and parallels' export) with no filters at all."""
    src = (ROOT / page).read_text(encoding='utf-8')
    assert '_dispatch_had_filters = _has_active_filters()' in src
    late = re.findall(r"\} if (_dispatch_had_filters or )?_has_active_filters\(\) else None", src)
    assert late and all(late), (page, late)
    if page.endswith('search.py'):
        assert "retire_old_saved_filters('search', run_key='search_last_run'" in src


class _Gate:
    def __init__(self):
        self.calls, self.events = [], []

    async def prefetch(self, rows):
        self.calls.append(list(rows))
        event = asyncio.Event()
        self.events.append(event)
        await event.wait()


def test_the_refill_keeps_a_request_made_while_it_runs():
    async def go():
        gate, renders = _Gate(), []
        refill = ss.PostMatchRefill(gate.prefetch, lambda: renders.append(True))
        refill.request([A])
        await asyncio.sleep(0)
        refill.request([A, B])              # an exclusion removed meanwhile
        gate.events[0].set()
        await asyncio.sleep(0)
        assert gate.calls == [[A], [A, B]], gate.calls
        assert renders == []                 # not before the last rows are checked
        gate.events[1].set()
        for _ in range(3):
            await asyncio.sleep(0)
        assert renders == [True] and refill.task is None
    asyncio.run(go())


def test_a_miss_met_by_the_rerender_starts_a_new_check():
    async def go():
        gate = _Gate()
        refill = None

        def rerender():
            if len(gate.calls) == 1:
                refill.request([B])          # the re-render met another unchecked row
        refill = ss.PostMatchRefill(gate.prefetch, rerender)
        refill.request([A])
        await asyncio.sleep(0)
        gate.events[0].set()
        for _ in range(3):
            await asyncio.sleep(0)
        assert gate.calls == [[A], [B]] and refill.task is not None
        gate.events[1].set()
        for _ in range(3):
            await asyncio.sleep(0)
        assert refill.task is None
    asyncio.run(go())


def test_the_refill_does_not_render_over_a_running_search():
    async def go():
        renders = []

        async def prefetch(rows):
            return None
        refill = ss.PostMatchRefill(prefetch, lambda: renders.append(True), busy=lambda: True)
        refill.request([A])
        for _ in range(3):
            await asyncio.sleep(0)
        assert renders == []
    asyncio.run(go())


def test_a_worker_whose_criteria_changed_does_not_publish(svc, monkeypatch):
    st = _state(post_filter_criteria={'width': {'min': 10, 'max': 12}})
    real = FjmsService.match_result_criteria

    def changes_meanwhile(self, criteria, sys_ids):
        st.post_filter_criteria = {'width': {'min': 30}}
        return real(self, criteria, sys_ids)
    monkeypatch.setattr(FjmsService, 'match_result_criteria', changes_meanwhile)
    assert ss.post_criteria_matches(st, _rows([A, B])) == {A, B}   # its own answer
    assert getattr(st, '_post_match', None) is None                 # not published

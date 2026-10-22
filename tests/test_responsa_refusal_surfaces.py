"""A refused Responsa query is a message in Joins Lab, never zero results; and the web's
full-text highlight matches before escaping (docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md,
Phase 1). The engine and the API sides are tests/test_responsa_api_refusal.py.
"""
import pytest

from shared.responsa_match import ResponsaQueryRefused
from tests.test_web_io_bound_none_findings import _click, _type_joins_query
from tests.test_web_search_within_cutoff import WORD, _label_texts, _row, _wait_for, page  # noqa: F401
from tests.test_web_variant_settings_per_visitor import (  # noqa: F401  (server is a fixture)
    _element, run, server, submit, wait_for_payloads,
)


class _RefusingSearcher:
    def __init__(self):
        self.calls = []

    def execute_search(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise ResponsaQueryRefused()


def test_the_web_joins_lab_executor_lets_a_refusal_through(monkeypatch):
    from web.state import state
    from web.joins_executor import WebSearchExecutor
    monkeypatch.setattr(state, 'searcher', _RefusingSearcher())
    with pytest.raises(ResponsaQueryRefused):
        WebSearchExecutor().execute_search('כ*נים', 'exact', 0)

    class _Broken:
        def execute_search(self, *a, **k):
            raise RuntimeError('index gone')
    monkeypatch.setattr(state, 'searcher', _Broken())
    assert WebSearchExecutor().execute_search('שלום', 'exact', 0) == []   # unchanged


def test_the_other_side_of_the_leaf_lets_a_refusal_through():
    from shared.joins_lab import apply_cross_side, normalize_candidate
    base = [normalize_candidate({'display': {'id': '990000000010205171', 'img': 3}, 'uid': 'u', 'full_text': ''})]
    with pytest.raises(ResponsaQueryRefused):
        apply_cross_side(_RefusingSearcher(), base, 'כ*נים', {'responsa_mode': True}, 'AND', None)


def test_the_web_full_text_highlight_matches_before_escaping():
    """`"` was `&quot;` when matched, so `ר"ת` was never marked (2026-10-21)."""
    from shared.search_engine import make_mark_tolerant_pattern
    import re
    from web.pages.search_results import highlight_full_text_html
    pattern = make_mark_tolerant_pattern(re.escape('רת'))
    html_text, stopped = highlight_full_text_html('אמר ר"ת <b> כאן', pattern)
    assert not stopped
    assert '<span class="highlight-match">ר&quot;ת</span>' in html_text
    assert '&lt;b&gt;' in html_text                  # the text itself is escaped
    plain, _ = highlight_full_text_html('a\x01b<', '')
    assert plain == 'ab&lt;'                         # no sentinel survives, no pattern


@pytest.mark.gui
def test_the_desktop_other_side_worker_reports_the_refusal():
    from desktop.join_workbench import _CrossSideWorker
    from shared.joins_lab import BuilderRow, SideQuery, normalize_candidate
    base = [normalize_candidate({'display': {'id': '990000000010205171', 'img': 3}, 'uid': 'u', 'full_text': ''})]
    worker = _CrossSideWorker(_RefusingSearcher(), base, SideQuery(rows=(BuilderRow(term='כ*נים'),)), {}, 'AND', None)
    refused, done = [], []
    worker.refused.connect(refused.append)
    worker.done.connect(done.append)
    worker.run()
    assert refused and '*' in refused[0]
    assert [c.page for c in done[0].candidates] == [3]   # this side's rows still show


# --- A refused search leaves no candidates of the previous one (Codex review of PR #433) ---

def _stale_rows(n=3):
    return [{'display': {'id': f'9900000000999900{i:02d}', 'shelfmark': f'T-S STALE {i}', 'title': '',
                         'library_code': 'CUL', 'img': 1},
             'uid': f'9900000000999900{i:02d}_P0001', 'full_text': 'x'} for i in range(n)]


@pytest.mark.gui
def test_a_refused_desktop_search_clears_the_previous_candidates():
    import sys
    from unittest.mock import MagicMock

    from PyQt6.QtWidgets import QApplication
    QApplication.instance() or QApplication(sys.argv)
    from desktop.join_workbench import JoinCandidatePane
    from shared.joins_lab import normalize_candidate
    wb = MagicMock()
    wb.meta_mgr = None          # no ThumbResolver thread
    wb._pick_callback = None
    wb.triage = {}
    wb.filtered = []
    pane = JoinCandidatePane(wb=wb, executor=None)
    pane.results = [normalize_candidate(r) for r in _stale_rows()]
    pane.apply_filters()
    assert len(pane.wb.filtered) == 3 and pane.cards
    pane._search_gen = 7
    pane._on_search_error('refused', 7)
    assert pane.results == [] and pane.wb.filtered == [] and not pane.cards
    assert pane.status.text() == 'refused'


def test_a_refused_web_search_does_not_bring_the_previous_cards_back(server, monkeypatch):  # noqa: F811
    """The previous search's enrichment lands after the refusal and re-renders from the
    page's candidate lists: they must be empty by then."""
    import asyncio
    import threading

    from nicegui import ui

    from web.joins_executor import WebSearchExecutor
    from web.pages import joins_lab
    from web.state import state
    monkeypatch.setattr(state, 'is_ready', lambda: True)
    calls = []

    def search(self, *args, **kwargs):
        calls.append(args[:1])
        if len(calls) == 1:
            return _stale_rows()
        raise ResponsaQueryRefused()
    monkeypatch.setattr(WebSearchExecutor, 'execute_search', search)
    release = threading.Event()
    enriching = []

    async def slow_enrich(sys_ids):
        enriching.append(sys_ids)
        while not release.is_set():
            await asyncio.sleep(0.02)
        return {}
    monkeypatch.setattr(joins_lab, '_enrich_candidates', slow_enrich)
    shown = {}

    def stale(user):
        return [t for t in _label_texts(user) if 'T-S STALE' in t]

    async def driver(a, b):
        try:
            await a.open('/joins-lab')
            await _type_joins_query(a)
            _click(a, _element(a, ui.button, lambda e: e.text == 'Run Search'))
            await _wait_for(lambda: stale(a) and enriching)
            shown['first'] = bool(stale(a))
            _click(a, _element(a, ui.button, lambda e: e.text == 'Run Search'))
            await _wait_for(lambda: any(t.startswith('Responsa search takes a *') for t in _label_texts(a)))
            release.set()
            await asyncio.sleep(0.5)                 # the first search's enrichment re-renders
            shown['after'] = stale(a)
        except Exception as exc:                    # asserted below
            shown['error'] = repr(exc)
        finally:
            release.set()

    run(driver)
    assert shown == {'first': True, 'after': []}, shown


def test_a_superseded_refused_web_search_stays_quiet(server, monkeypatch):  # noqa: F811
    """A refusal that lands after a newer search took the page -- the cancel lost the
    race -- must not clear that search's candidates or show its message (Codex review
    of PR #433). The newer search is simulated: the generation check says superseded."""
    import asyncio

    from nicegui import ui

    from web.joins_executor import WebSearchExecutor
    from web.pages import joins_lab
    from web.state import state
    monkeypatch.setattr(state, 'is_ready', lambda: True)
    calls = []

    def search(self, *args, **kwargs):
        calls.append(args[:1])
        raise ResponsaQueryRefused()
    monkeypatch.setattr(WebSearchExecutor, 'execute_search', search)
    monkeypatch.setattr(joins_lab, '_should_apply_results', lambda my_gen, gen_ref: False)
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/joins-lab')
            await _type_joins_query(a)
            _click(a, _element(a, ui.button, lambda e: e.text == 'Run Search'))
            await _wait_for(lambda: calls)
            await asyncio.sleep(0.5)                 # the refusal has reached its handler
            shown['said'] = [t for t in _label_texts(a) if t.startswith('Responsa search takes a *')]
        except Exception as exc:                    # asserted below
            shown['error'] = repr(exc)

    run(driver)
    assert shown == {'said': []}, shown


def test_a_superseded_refusal_of_the_other_side_stays_quiet(server, monkeypatch):  # noqa: F811
    """The other side's refusal landing after a newer search took the page must not apply
    this run's candidates or say its message (Codex review of PR #433). The newer search
    is simulated: the generation check says superseded once the other side has run."""
    import asyncio

    from nicegui import ui

    from web.joins_executor import WebSearchExecutor
    from web.pages import joins_lab
    from web.state import state
    monkeypatch.setattr(state, 'is_ready', lambda: True)
    calls, superseded = [], []

    def search(self, *args, **kwargs):
        calls.append(args[:1])
        if len(calls) == 1:
            return _stale_rows()
        superseded.append(True)                 # a newer Run Search took the page meanwhile
        raise ResponsaQueryRefused()
    monkeypatch.setattr(WebSearchExecutor, 'execute_search', search)
    monkeypatch.setattr(joins_lab, '_should_apply_results', lambda my_gen, gen_ref: not superseded)
    shown = {}

    async def driver(a, b):
        try:
            await a.open('/joins-lab')
            await _type_joins_query(a)              # both builders, this side and the other
            other_side = _element(a, ui.checkbox, lambda e: e.text == 'Search the other side of the leaf')
            with a._client:
                other_side.value = True
            _click(a, _element(a, ui.button, lambda e: e.text == 'Run Search'))
            await _wait_for(lambda: len(calls) == 2)
            await asyncio.sleep(0.5)                # the refusal has reached its handler
            shown['said'] = [m for m in a.notify.messages if m.startswith('Responsa search takes a *')]
            shown['cards'] = [t for t in _label_texts(a) if 'T-S STALE' in t]
        except Exception as exc:                    # asserted below
            shown['error'] = repr(exc)

    run(driver)
    assert shown == {'said': [], 'cards': []}, shown


def test_a_downgrade_stays_on_the_count_line_after_a_filter(page):  # noqa: F811
    """The guard's downgrade belongs to the results shown: a filter's re-render of the
    count line dropped it (Codex review of PR #433)."""
    from nicegui import ui
    queue = page({(WORD, False): ([dict(_row('M1'), responsa_warning='Spelling variants disabled'),
                                   _row('M2')], None)})
    seen = {}

    def counts(user):
        return [t for t in _label_texts(user) if ' Results' in t]

    async def driver(a, b):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(queue, 1)
        await _wait_for(lambda: any('Spelling variants disabled' in t for t in counts(a)))
        _click(a, _element(a, ui.button, lambda e: e.text == 'Apply Filters'))
        await _wait_for(lambda: any(t.startswith('2 / 2 Results') for t in counts(a)))
        seen['after'] = counts(a)

    run(driver)
    assert any(t.startswith('2 / 2 Results') and t.endswith('· Spelling variants disabled')
               for t in seen['after']), seen

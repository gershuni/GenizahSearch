"""Search budgets must escape hit recovery without changing generated matches."""

import re
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from shared.search_engine import SearchEngine
from shared.search_regex import SearchBudgetExceeded


def _engine():
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = SimpleNamespace(
        get_variants=lambda term, mode, limit=200: [term, term + 'א']
    )
    engine.index = Mock()
    engine.searcher = Mock()
    engine.searcher.search.return_value = SimpleNamespace(hits=[(1.0, 'doc')])
    engine.searcher.doc.return_value = {'content': ['שלום עולם'], 'scope': ['page']}
    engine.local_searcher = None
    return engine


def test_search_timeout_escapes_per_hit_recovery():
    engine = _engine()
    matcher = Mock(pattern='שלום')
    matcher.search.side_effect = SearchBudgetExceeded()
    engine.build_regex_pattern = Mock(return_value=matcher)

    with pytest.raises(SearchBudgetExceeded):
        engine.execute_search('שלום', 'Regex', 0, corpus_scope='genizah')

    matcher.search.assert_called_once_with('שלום עולם')


def test_local_only_timeout_is_not_reported_as_empty_results():
    engine = _engine()
    engine.local_searcher = Mock()
    engine._query_local_index = Mock(side_effect=SearchBudgetExceeded())

    with pytest.raises(SearchBudgetExceeded):
        engine.execute_search('שלום', 'Regex', 0, corpus_scope='local')


@pytest.mark.parametrize('gap', [0, 1, 3])
@pytest.mark.parametrize('text', [
    'שלום עולם', 'שלוםא עולם', 'שלום דבר עולם', 'שלום\nעולם',
    'שָׁלוֹם עולם', ']שלום[ עולם', 'שלום\u0307 עולם',
    'שלום\u200cעולם', 'שלום_עולם', 'שלום²עולם', 'nothing matches',
])
def test_generated_pattern_spans_match_stdlib(gap, text):
    compiled = _engine().build_regex_pattern(['שלום', 'עולם'], 'exact', gap)
    expected = re.search(compiled.pattern, text, re.IGNORECASE)
    actual = compiled.search(text)
    assert (actual.span() if actual else None) == (expected.span() if expected else None)
    assert (actual.groups() if actual else None) == (expected.groups() if expected else None)


@pytest.mark.parametrize('text', ['שלום עולם', 'עולם שלום', 'שלום-עולם', 'שָׁלוֹם עולם'])
def test_bidirectional_responsa_matches_stdlib(text):
    compiled = _engine().build_regex_pattern(
        None, 'exact', 0,
        responsa_components=[{'regex_terms': ['שלום']}, {'regex_terms': ['עולם']}],
        responsa_options={'bidirectional': True, 'flex_spacing': True},
    )
    expected = re.search(compiled.pattern, text, re.IGNORECASE)
    actual = compiled.search(text)
    assert (actual.span() if actual else None) == (expected.span() if expected else None)
    assert (actual.groups() if actual else None) == (expected.groups() if expected else None)

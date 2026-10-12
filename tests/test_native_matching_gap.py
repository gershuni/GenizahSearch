"""The word-gap limit (docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md §3.6, owner ruling
2026-10-12): at most MAX_WORD_GAP in the regular modes, which can match natively;
no policy limit in Responsa and Regex, which keep the interruptible route -- only
re's own repeat-count ceiling, rejected with a message instead of an OverflowError.
"""

import re
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from shared import responsa
from shared.search_engine import (
    MAX_WORD_GAP, SearchEngine, _gap_separator, _validate_word_gap,
)


def _engine():
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = SimpleNamespace(
        get_variants=lambda term, mode, limit=200: [term],
        variant_level=lambda mode: 'basic',
    )
    engine.index = Mock()
    engine.searcher = Mock()
    engine.searcher.search.return_value = SimpleNamespace(hits=[])
    engine.local_searcher = None
    return engine


@pytest.mark.parametrize('mode', ['literal', 'variants', 'variants_extended', 'variants_maximum', 'fuzzy'])
def test_regular_modes_reject_a_gap_over_the_limit(mode):
    _validate_word_gap(mode, MAX_WORD_GAP)
    with pytest.raises(ValueError, match=r'at most 10 .*Responsa and Regex'):
        _validate_word_gap(mode, MAX_WORD_GAP + 1)


@pytest.mark.parametrize('mode', ['Regex', 'Title', 'Shelfmark'])
def test_regex_and_metadata_modes_keep_any_gap(mode):
    _validate_word_gap(mode, 50)
    _validate_word_gap(mode, 100_000)


def test_responsa_keeps_any_gap():
    _validate_word_gap('literal', 50, {'responsa_mode': True})


def test_a_missing_gap_is_not_a_gap():
    _validate_word_gap('variants', None)


def test_execute_search_rejects_at_entry_before_the_local_branch():
    """The LOCAL-only branch swallows exceptions into []; the check runs before it."""
    engine = _engine()
    engine.local_searcher = Mock()
    engine._query_local_index = Mock(return_value=[])
    with pytest.raises(ValueError, match='at most 10'):
        engine.execute_search('שלום עולם', 'literal', 11, corpus_scope='local')
    engine._query_local_index.assert_not_called()


def test_execute_search_rejects_before_the_fuzzy_matcher():
    """Fuzzy never reaches build_regex_pattern; the entry check still fires."""
    with pytest.raises(ValueError, match='at most 10'):
        _engine().execute_search('שלום עולם', 'fuzzy', 11, corpus_scope='genizah')


def test_execute_search_accepts_the_limit_and_responsa_above_it():
    engine = _engine()
    assert engine.execute_search('שלום עולם', 'literal', MAX_WORD_GAP, corpus_scope='genizah') == []
    engine.build_regex_pattern = Mock(return_value=None)
    engine.execute_search('שלום עולם', 'literal', 50, corpus_scope='genizah',
                          responsa_options={'responsa_mode': True})


def test_gap_separator_takes_an_int_and_only_an_int():
    assert _gap_separator('3') == _gap_separator(3)
    assert '{0,3}' in _gap_separator(3)
    with pytest.raises(ValueError):
        _gap_separator('three')


@pytest.mark.parametrize('query, expected', [
    ('אב [50] גד', [50]),
    ('אב [4294967294] גד', [4294967294]),
])
def test_responsa_word_gaps_have_no_policy_limit(query, expected):
    assert responsa.extract_per_pair_gaps(query) == expected


def test_responsa_rejects_only_the_re_ceiling_with_a_message():
    with pytest.raises(ValueError, match=r'\[4294967295\] is too large'):
        responsa.extract_per_pair_gaps('אב [4294967295] גד')
    # The same token in a line group, and the line-gap token: both parse sites.
    with pytest.raises(ValueError, match='too large'):
        responsa._parse_line_break_query('|אב [4294967295] גד|')
    with pytest.raises(ValueError, match=r'\[\|4294967295\] is too large'):
        responsa._parse_line_break_query('|אב| [|4294967295] |גד|')
    # One under the ceiling compiles in re (the reason the ceiling is where it is).
    re.compile('(?:a){0,4294967294}')
    with pytest.raises(OverflowError):
        re.compile('(?:a){0,4294967295}')

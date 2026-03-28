# -*- coding: utf-8 -*-
"""Tests for shared/refinement.py — RefinementStep dataclass and chain helpers."""

import pytest
from shared.refinement import (
    RefinementStep,
    compute_effective_restrict,
    needs_mode_labels,
    truncate_chain,
    replay_chain,
    scope_signature,
    enrich_snippet_with_chain_terms,
    compute_all_terms_filter,
)


# ---------------------------------------------------------------------------
# MockSearcher for replay_chain tests
# ---------------------------------------------------------------------------

class MockSearcher:
    """Mock search engine that returns predetermined results per call."""
    def __init__(self, results_by_call):
        self.results_by_call = results_by_call  # list of list[dict]
        self.call_count = 0
        self.calls = []

    def execute_search(self, query, mode, gap, **kwargs):
        self.calls.append((query, mode, gap, kwargs))
        result = self.results_by_call[self.call_count] if self.call_count < len(self.results_by_call) else []
        self.call_count += 1
        return result


def _make_results(*sys_ids):
    """Helper: create mock result list from sys_id strings."""
    return [{'display': {'id': sid}, 'uid': f'uid_{sid}'} for sid in sys_ids]


# ---------------------------------------------------------------------------
# RefinementStep creation and serialization
# ---------------------------------------------------------------------------

class TestRefinementStepCreation:
    def test_refinement_step_create(self):
        step = RefinementStep('rambam', 'exact')
        assert step.query == 'rambam'
        assert step.mode == 'exact'
        assert step.gap == 0
        assert step.exclude_words == []
        assert step.text_position is None
        assert step.responsa_options is None
        assert step.result_count == 0

    def test_refinement_step_roundtrip(self):
        step = RefinementStep(
            query='test query',
            mode='responsa',
            gap=3,
            exclude_words=['foo', 'bar'],
            text_position='start',
            responsa_options={'plene': True, 'prefixes': True},
            result_count=42,
        )
        d = step.to_dict()
        restored = RefinementStep.from_dict(d)
        assert restored.query == step.query
        assert restored.mode == step.mode
        assert restored.gap == step.gap
        assert restored.exclude_words == step.exclude_words
        assert restored.text_position == step.text_position
        assert restored.responsa_options == step.responsa_options
        assert restored.result_count == step.result_count

    def test_refinement_step_from_dict_extra_keys(self):
        d = {'query': 'x', 'mode': 'exact', 'future_field': 42, 'another': 'val'}
        step = RefinementStep.from_dict(d)
        assert step.query == 'x'
        assert step.mode == 'exact'
        # Should not raise, unknown keys silently ignored

    def test_display_label_simple(self):
        step = RefinementStep(query='rambam', mode='exact')
        assert step.display_label == 'rambam'


# ---------------------------------------------------------------------------
# needs_mode_labels
# ---------------------------------------------------------------------------

class TestNeedsModeLabels:
    def test_needs_mode_labels_single_mode(self):
        chain = [
            RefinementStep('a', 'exact'),
            RefinementStep('b', 'exact'),
        ]
        assert needs_mode_labels(chain) is False

    def test_needs_mode_labels_mixed(self):
        chain = [
            RefinementStep('a', 'exact'),
            RefinementStep('b', 'responsa'),
        ]
        assert needs_mode_labels(chain) is True

    def test_needs_mode_labels_empty(self):
        assert needs_mode_labels([]) is False


# ---------------------------------------------------------------------------
# compute_effective_restrict (None vs empty-set contract)
# ---------------------------------------------------------------------------

class TestComputeEffectiveRestrict:
    def test_effective_restrict_both_none(self):
        result = compute_effective_restrict(None, None)
        assert result is None

    def test_effective_restrict_filter_only(self):
        result = compute_effective_restrict({1, 2, 3}, None)
        assert result == {1, 2, 3}

    def test_effective_restrict_refinement_only(self):
        result = compute_effective_restrict(None, {2, 3})
        assert result == {2, 3}

    def test_effective_restrict_intersection(self):
        result = compute_effective_restrict({1, 2, 3}, {2, 3, 4})
        assert result == {2, 3}

    def test_effective_restrict_empty_intersection(self):
        result = compute_effective_restrict({1, 2}, {3, 4})
        assert result == set()
        assert result is not None  # empty set, NOT None

    def test_effective_restrict_empty_filter(self):
        result = compute_effective_restrict(set(), {1, 2})
        assert result == set()
        assert result is not None

    def test_effective_restrict_empty_refinement(self):
        result = compute_effective_restrict({1, 2}, set())
        assert result == set()
        assert result is not None


# ---------------------------------------------------------------------------
# truncate_chain
# ---------------------------------------------------------------------------

class TestTruncateChain:
    def test_truncate_chain_middle(self):
        a, b, c, d = [RefinementStep(q, 'exact') for q in 'abcd']
        result = truncate_chain([a, b, c, d], index=1)
        assert result == [a]

    def test_truncate_chain_last(self):
        a, b, c = [RefinementStep(q, 'exact') for q in 'abc']
        result = truncate_chain([a, b, c], index=2)
        assert result == [a, b]

    def test_truncate_chain_first(self):
        a, b, c = [RefinementStep(q, 'exact') for q in 'abc']
        result = truncate_chain([a, b, c], index=0)
        assert result == []


# ---------------------------------------------------------------------------
# replay_chain
# ---------------------------------------------------------------------------

class TestReplayChain:
    def test_replay_chain_empty(self):
        searcher = MockSearcher([])
        result = replay_chain([], searcher, None)
        assert result is None

    def test_replay_chain_single_step(self):
        results1 = _make_results('sys_001', 'sys_002')
        searcher = MockSearcher([results1])
        step = RefinementStep('rambam', 'exact')
        result = replay_chain([step], searcher, None)
        assert result == {'sys_001', 'sys_002'}
        # Verify searcher was called with correct params
        assert searcher.call_count == 1
        q, mode, gap, kw = searcher.calls[0]
        assert q == 'rambam'
        assert mode == 'exact'
        assert gap == 0
        assert kw.get('restrict_sys_ids') is None

    def test_replay_chain_multi_step(self):
        results1 = _make_results('sys_001', 'sys_002', 'sys_003')
        results2 = _make_results('sys_001', 'sys_003')
        searcher = MockSearcher([results1, results2])
        step1 = RefinementStep('rambam', 'exact')
        step2 = RefinementStep('torah', 'variants')
        result = replay_chain([step1, step2], searcher, None)
        assert result == {'sys_001', 'sys_003'}
        # Second call should have restrict from first results
        _, _, _, kw2 = searcher.calls[1]
        assert kw2.get('restrict_sys_ids') == {'sys_001', 'sys_002', 'sys_003'}

    def test_replay_chain_with_filter_restrict(self):
        results1 = _make_results('sys_002')
        searcher = MockSearcher([results1])
        step = RefinementStep('test', 'exact')
        result = replay_chain([step], searcher, {1, 2, 3})
        assert result == {'sys_002'}
        # First call should use filter restrict
        _, _, _, kw = searcher.calls[0]
        assert kw.get('restrict_sys_ids') == {1, 2, 3}

    def test_replay_chain_updates_result_counts(self):
        results1 = _make_results('a', 'b', 'c')
        results2 = _make_results('a')
        searcher = MockSearcher([results1, results2])
        step1 = RefinementStep('q1', 'exact')
        step2 = RefinementStep('q2', 'exact')
        replay_chain([step1, step2], searcher, None)
        assert step1.result_count == 3
        assert step2.result_count == 1


# ---------------------------------------------------------------------------
# scope_signature
# ---------------------------------------------------------------------------

class TestScopeSignature:
    def test_scope_signature_none(self):
        sig = scope_signature(None)
        assert isinstance(sig, str)
        assert sig == 'none'

    def test_scope_signature_same_set(self):
        assert scope_signature({1, 2, 3}) == scope_signature({3, 2, 1})

    def test_scope_signature_different_set(self):
        assert scope_signature({1, 2}) != scope_signature({1, 2, 3})


# ---------------------------------------------------------------------------
# chain_to_dicts roundtrip
# ---------------------------------------------------------------------------

class TestChainRoundtrip:
    def test_chain_to_dicts_roundtrip(self):
        chain = [
            RefinementStep('first', 'exact', gap=0, result_count=10),
            RefinementStep('second', 'responsa', gap=2, responsa_options={'plene': True}),
            RefinementStep('third', 'variants', exclude_words=['x']),
        ]
        dicts = [s.to_dict() for s in chain]
        restored = [RefinementStep.from_dict(d) for d in dicts]
        for orig, rest in zip(chain, restored):
            assert orig.query == rest.query
            assert orig.mode == rest.mode
            assert orig.gap == rest.gap
            assert orig.exclude_words == rest.exclude_words
            assert orig.text_position == rest.text_position
            assert orig.responsa_options == rest.responsa_options
            assert orig.result_count == rest.result_count


class TestEnrichSnippet:
    def test_enrich_marks_earlier_terms(self):
        chain = [RefinementStep('שלום', 'exact'), RefinementStep('רמבם', 'exact')]
        snippet = 'בשם שלום ורמבם *כמא* בתורה'
        result = enrich_snippet_with_chain_terms(snippet, chain, 'כמא')
        assert '*שלום*' in result
        assert '*רמבם*' in result
        assert '*כמא*' in result

    def test_enrich_no_double_mark(self):
        chain = [RefinementStep('abc', 'exact')]
        snippet = 'x *abc* y abc z'
        result = enrich_snippet_with_chain_terms(snippet, chain, 'abc')
        # The already-marked *abc* should stay, the unmarked one should be skipped (same as current query)
        assert result.count('*abc*') == 1  # only the original one

    def test_enrich_empty_chain(self):
        result = enrich_snippet_with_chain_terms('hello *world*', [], 'world')
        assert result == 'hello *world*'

    def test_enrich_skips_current_query(self):
        chain = [RefinementStep('old', 'exact'), RefinementStep('current', 'exact')]
        snippet = 'old text *current* here'
        result = enrich_snippet_with_chain_terms(snippet, chain, 'current')
        assert '*old*' in result
        # 'current' is already marked and also the current query — no double


# ---------------------------------------------------------------------------
# _result_uids and compute_all_terms_filter
# ---------------------------------------------------------------------------

class TestResultUids:
    def test_to_dict_excludes_result_uids(self):
        step = RefinementStep('q', 'exact', result_count=5)
        step._result_uids = {'uid_a', 'uid_b'}
        d = step.to_dict()
        assert '_result_uids' not in d
        assert d['query'] == 'q'

    def test_from_dict_ignores_result_uids(self):
        d = {'query': 'q', 'mode': 'exact', '_result_uids': {'a', 'b'}}
        step = RefinementStep.from_dict(d)
        assert step._result_uids == set()  # default, not from dict

    def test_replay_populates_result_uids(self):
        results1 = _make_results('a', 'b', 'c')
        results2 = _make_results('a')
        searcher = MockSearcher([results1, results2])
        step1 = RefinementStep('q1', 'exact')
        step2 = RefinementStep('q2', 'exact')
        replay_chain([step1, step2], searcher, None)
        assert step1._result_uids == {'uid_a', 'uid_b', 'uid_c'}
        assert step2._result_uids == {'uid_a'}

    def test_replay_result_count_is_page_level(self):
        # 3 results, 2 unique sys_ids — count should be 3 (pages), not 2 (manuscripts)
        results = [
            {'display': {'id': 'ms1'}, 'uid': 'uid_ms1_p1'},
            {'display': {'id': 'ms1'}, 'uid': 'uid_ms1_p2'},
            {'display': {'id': 'ms2'}, 'uid': 'uid_ms2_p1'},
        ]
        searcher = MockSearcher([results])
        step = RefinementStep('q', 'exact')
        replay_chain([step], searcher, None)
        assert step.result_count == 3  # pages, not 2 manuscripts


class TestComputeAllTermsFilter:
    def test_single_step_returns_none(self):
        step = RefinementStep('q', 'exact')
        step._result_uids = {'a', 'b'}
        assert compute_all_terms_filter([step]) is None

    def test_two_steps_intersection(self):
        s1 = RefinementStep('q1', 'exact')
        s1._result_uids = {'a', 'b', 'c'}
        s2 = RefinementStep('q2', 'exact')
        s2._result_uids = {'b', 'c', 'd'}
        result = compute_all_terms_filter([s1, s2])
        assert result == {'b', 'c'}

    def test_skips_metadata_modes(self):
        s1 = RefinementStep('T-S', 'Shelfmark')
        s1._result_uids = {'a', 'b'}
        s2 = RefinementStep('q', 'exact')
        s2._result_uids = {'b', 'c'}
        # Only one text-search step (s2), Shelfmark skipped → needs 2+ text steps
        assert compute_all_terms_filter([s1, s2]) is None

    def test_three_steps_intersection(self):
        s1 = RefinementStep('q1', 'exact')
        s1._result_uids = {'a', 'b', 'c', 'd'}
        s2 = RefinementStep('q2', 'exact')
        s2._result_uids = {'b', 'c', 'd', 'e'}
        s3 = RefinementStep('q3', 'exact')
        s3._result_uids = {'c', 'd', 'e', 'f'}
        result = compute_all_terms_filter([s1, s2, s3])
        assert result == {'c', 'd'}

    def test_empty_intersection(self):
        s1 = RefinementStep('q1', 'exact')
        s1._result_uids = {'a', 'b'}
        s2 = RefinementStep('q2', 'exact')
        s2._result_uids = {'c', 'd'}
        result = compute_all_terms_filter([s1, s2])
        assert result == set()

    def test_mixed_modes_only_intersects_text(self):
        s1 = RefinementStep('term1', 'exact')
        s1._result_uids = {'a', 'b', 'c'}
        s2 = RefinementStep('title', 'Title')
        s2._result_uids = {'b', 'c', 'd'}  # skipped (metadata)
        s3 = RefinementStep('term2', 'exact')
        s3._result_uids = {'b', 'c', 'e'}
        result = compute_all_terms_filter([s1, s2, s3])
        assert result == {'b', 'c'}  # intersection of s1 and s3 only

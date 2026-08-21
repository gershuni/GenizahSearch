# -*- coding: utf-8 -*-
"""Tests for `scripts/probe_exclusive_depth.py` (Codex: "no reproducible
depth-probe implementation is named or frozen").

This script is deliberately SLOW when run for real (per-chunk-query cost is
~13s in the prereg's own estimate), so these tests never touch a real index
or `eval_methods.build_retrievers`. Every retriever here is a plain stub
object exposing only `.retrieve(text) -> list[str]`, exercising `probe()`
(the pure core: card selection, grain computation, report shaping) exactly
as `run_probe()` calls it -- just with a fabricated dict of retrievers
instead of a real one built from a real index.
"""
from __future__ import annotations

import json

import pytest

from scripts import probe_exclusive_depth as ped


class StubRetriever:
    """Ignores the query text entirely; always returns the same ranked list."""

    def __init__(self, ranked_list):
        self.ranked_list = ranked_list
        self.calls = 0

    def retrieve(self, text):
        self.calls += 1
        return list(self.ranked_list)


def _card(cid, qid, rid, methods):
    return {'id': cid, 'query_id': qid, 'record_id': rid,
           'methods': list(methods)}


# ---------------------------------------------------------------------------
# select_exclusive_graded
# ---------------------------------------------------------------------------


def test_select_exclusive_graded_keeps_only_single_method_graded_known_ids():
    key = {
        'c1': _card('c1', 'q1', 'sysA_p1', ['passage']),            # keep
        'c2': _card('c2', 'q2', 'sysB_p1', ['passage', 'chunk']),   # multi-method, drop
        'c3': _card('c3', 'q3', 'sysC_p1', ['chunk']),              # keep
    }
    verdicts = [
        {'id': 'c1', 'grade': 'same_text'},
        {'id': 'c2', 'grade': 'same_text'},   # would qualify but multi-method
        {'id': 'c3', 'grade': 'unrelated'},
        {'id': 'orphan', 'grade': 'same_text'},        # not in key
        {'id': 'c1', 'grade': 'paraphrase'},            # duplicate id, dropped
        {'id': 'c3', 'grade': 'not_a_real_grade'},      # would duplicate anyway
    ]
    out = ped.select_exclusive_graded(key, verdicts, ped.sgd.ALL_GRADES)
    assert out == [('c1', 'same_text'), ('c3', 'unrelated')]


def test_select_exclusive_graded_rejects_unknown_grade():
    key = {'c1': _card('c1', 'q1', 'sysA_p1', ['passage'])}
    verdicts = [{'id': 'c1', 'grade': 'bogus'}]
    assert ped.select_exclusive_graded(key, verdicts, ped.sgd.ALL_GRADES) == []


# ---------------------------------------------------------------------------
# rank_or_absent / manuscript_rank_or_absent -- grain computation
# ---------------------------------------------------------------------------


def test_rank_or_absent_found_and_missing():
    ranked = ['r1', 'r2', 'r3']
    assert ped.rank_or_absent('r2', ranked) == 1
    assert ped.rank_or_absent('r1', ranked) == 0
    assert ped.rank_or_absent('rX', ranked) == 'ABSENT'


def test_manuscript_rank_or_absent_takes_the_best_rank_among_matches():
    ranked = ['other_p1', 'sysA_p9', 'sysA_p2']
    # sysA appears at index 1 AND 2 -- best (lowest) is 1.
    assert ped.manuscript_rank_or_absent('sysA', ranked) == 1
    assert ped.manuscript_rank_or_absent('sysZ', ranked) == 'ABSENT'


def test_manuscript_rank_or_absent_single_match():
    ranked = ['sysQ_p1']
    assert ped.manuscript_rank_or_absent('sysQ', ranked) == 0


# ---------------------------------------------------------------------------
# probe_card -- report shaping, the ABSENT wording contract
# ---------------------------------------------------------------------------


def test_probe_card_found_at_both_grains_has_no_note():
    entry = _card('c1', 'q1', 'sysA_p2', ['passage'])
    other = StubRetriever(['sysX_p1', 'sysA_p2', 'sysA_p9'])
    row = ped.probe_card('c1', 'same_text', entry, 'chunk:3:exact:100',
                         other, 'query text')
    assert row['page_rank'] == 1
    assert row['page_note'] is None
    assert row['manuscript_rank'] == 1  # best among sysA hits (index 1)
    assert row['manuscript_note'] is None
    assert row['sys_id'] == 'sysA'
    assert row['own_method'] == 'passage'
    assert row['other_method'] == 'chunk:3:exact:100'
    assert other.calls == 1


def test_probe_card_absent_at_both_grains_carries_the_required_wording():
    entry = _card('c1', 'q1', 'sysA_p2', ['passage'])
    other = StubRetriever(['sysX_p1', 'sysY_p1'])   # no sysA at all
    row = ped.probe_card('c1', 'unrelated', entry, 'chunk:3:exact:100',
                         other, 'query text')
    assert row['page_rank'] == 'ABSENT'
    assert row['page_note'] == ped.NOT_RETURNED_NOTE
    assert row['manuscript_rank'] == 'ABSENT'
    assert row['manuscript_note'] == ped.NOT_RETURNED_NOTE


def test_probe_card_page_absent_but_manuscript_present():
    # A different page of the SAME manuscript is present, but not this
    # exact record -- page grain must ABSENT, manuscript grain must find it.
    entry = _card('c1', 'q1', 'sysA_p2', ['passage'])
    other = StubRetriever(['sysA_p9'])
    row = ped.probe_card('c1', 'same_text', entry, 'chunk:3:exact:100',
                         other, 'query text')
    assert row['page_rank'] == 'ABSENT'
    assert row['manuscript_rank'] == 0


# ---------------------------------------------------------------------------
# build_report -- deterministic grouping/ordering
# ---------------------------------------------------------------------------


def test_build_report_groups_by_grade_alphabetically_and_sorts_cards():
    cards = [
        {'card_id': 'z9', 'grade': 'unrelated', 'page_rank': 'ABSENT',
         'manuscript_rank': 'ABSENT'},
        {'card_id': 'a1', 'grade': 'unrelated', 'page_rank': 0,
         'manuscript_rank': 0},
        {'card_id': 'm5', 'grade': 'same_text', 'page_rank': 'ABSENT',
         'manuscript_rank': 2},
    ]
    report = ped.build_report(cards)
    assert report['n_exclusive_graded'] == 3
    assert list(report['by_grade']) == ['same_text', 'unrelated']  # alpha
    unrelated = report['by_grade']['unrelated']
    assert [c['card_id'] for c in unrelated['cards']] == ['a1', 'z9']
    assert unrelated['n'] == 2
    assert unrelated['page_absent'] == 1
    assert unrelated['manuscript_absent'] == 1
    same_text = report['by_grade']['same_text']
    assert same_text['page_absent'] == 1
    assert same_text['manuscript_absent'] == 0


# ---------------------------------------------------------------------------
# probe() -- the full pure pipeline with stubbed retrievers
# ---------------------------------------------------------------------------


def _fixture():
    key = {
        'c1': _card('c1', 'q1', 'sysA_p1', ['passage:standard-40']),
        'c2': _card('c2', 'q2', 'sysB_p1', ['chunk:3:exact:100']),
        'c3': _card('c3', 'q3', 'sysC_p1',
                   ['passage:standard-40', 'chunk:3:exact:100']),  # excluded
    }
    verdicts = [
        {'id': 'c1', 'grade': 'same_text'},
        {'id': 'c2', 'grade': 'unrelated'},
        {'id': 'c3', 'grade': 'same_text'},
    ]
    qtext = {'q1': 'text one', 'q2': 'text two', 'q3': 'text three'}
    specs = ['passage:standard-40', 'chunk:3:exact:100']
    retrievers_by_spec = {
        'passage:standard-40': StubRetriever(['sysB_p1']),
        'chunk:3:exact:100': StubRetriever(['sysA_p1', 'sysQ_p1']),
    }
    return key, verdicts, qtext, specs, retrievers_by_spec


def test_probe_excludes_multi_method_cards_and_probes_the_other_method():
    key, verdicts, qtext, specs, retrievers_by_spec = _fixture()
    report = ped.probe(key, verdicts, qtext, specs, retrievers_by_spec)
    assert report['n_exclusive_graded'] == 2   # c3 excluded (multi-method)
    all_cards = [c for block in report['by_grade'].values()
                for c in block['cards']]
    ids = {c['card_id'] for c in all_cards}
    assert ids == {'c1', 'c2'}
    c1 = next(c for c in all_cards if c['card_id'] == 'c1')
    assert c1['other_method'] == 'chunk:3:exact:100'
    assert c1['page_rank'] == 0   # chunk's stub list has sysA_p1 at index 0
    c2 = next(c for c in all_cards if c['card_id'] == 'c2')
    assert c2['other_method'] == 'passage:standard-40'
    assert c2['page_rank'] == 0   # passage's stub list has sysB_p1 at index 0
    # Each retriever consulted exactly once per card that needed it.
    assert retrievers_by_spec['chunk:3:exact:100'].calls == 1
    assert retrievers_by_spec['passage:standard-40'].calls == 1


def test_probe_limit_caps_the_number_of_cards_probed():
    key, verdicts, qtext, specs, retrievers_by_spec = _fixture()
    report = ped.probe(key, verdicts, qtext, specs, retrievers_by_spec,
                       limit=1)
    assert report['n_exclusive_graded'] == 1
    # Sorted by card_id: 'c1' < 'c2', so the surviving card is c1 and only
    # the chunk retriever (c1's "other" method) should have been consulted.
    assert retrievers_by_spec['chunk:3:exact:100'].calls == 1
    assert retrievers_by_spec['passage:standard-40'].calls == 0


def test_probe_is_deterministic():
    key, verdicts, qtext, specs, retrievers_by_spec = _fixture()
    r1 = ped.probe(key, verdicts, qtext, specs, retrievers_by_spec)
    # Fresh stub instances (calls-counter reset) but identical data.
    _, _, _, _, retrievers_by_spec2 = _fixture()
    r2 = ped.probe(key, verdicts, qtext, specs, retrievers_by_spec2)
    assert json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)


# ---------------------------------------------------------------------------
# probe() -- fatal, never a silent guess
# ---------------------------------------------------------------------------


def test_probe_rejects_non_pairwise_configs():
    key, verdicts, qtext, _specs, retrievers_by_spec = _fixture()
    with pytest.raises(SystemExit, match='pairwise'):
        ped.probe(key, verdicts, qtext, ['only-one-spec'], retrievers_by_spec)


def test_probe_rejects_a_cards_method_not_in_configs():
    key = {'c1': _card('c1', 'q1', 'sysA_p1', ['some-other-method'])}
    verdicts = [{'id': 'c1', 'grade': 'same_text'}]
    qtext = {'q1': 'text'}
    specs = ['passage:standard-40', 'chunk:3:exact:100']
    retrievers_by_spec = {s: StubRetriever([]) for s in specs}
    with pytest.raises(SystemExit, match='not in --configs'):
        ped.probe(key, verdicts, qtext, specs, retrievers_by_spec)


def test_probe_rejects_a_missing_query_text():
    key = {'c1': _card('c1', 'q1', 'sysA_p1', ['passage:standard-40'])}
    verdicts = [{'id': 'c1', 'grade': 'same_text'}]
    qtext = {}   # q1 missing
    specs = ['passage:standard-40', 'chunk:3:exact:100']
    retrievers_by_spec = {s: StubRetriever([]) for s in specs}
    with pytest.raises(SystemExit, match='not found'):
        ped.probe(key, verdicts, qtext, specs, retrievers_by_spec)


# ---------------------------------------------------------------------------
# Wording pin: the REPORTED note is the required sentence, never the word
# "unretrievable" (the module docstring names that word ONCE, in quotes, to
# explain why it is avoided -- this pins the actual runtime-facing string,
# not blind prose-grepping, so documenting the anti-pattern by name does not
# itself trip the gate).
# ---------------------------------------------------------------------------


def test_not_returned_note_is_the_exact_required_sentence():
    assert ped.NOT_RETURNED_NOTE == (
        'not returned by this configured retriever under its internal caps')
    assert 'unretrievable' not in ped.NOT_RETURNED_NOTE.lower()


def test_probe_card_note_field_never_contains_unretrievable():
    entry = _card('c1', 'q1', 'sysA_p2', ['passage'])
    other = StubRetriever([])   # everything absent
    row = ped.probe_card('c1', 'unrelated', entry, 'chunk:3:exact:100',
                         other, 'query text')
    for field in ('page_note', 'manuscript_note'):
        assert row[field] is not None
        assert 'unretrievable' not in row[field].lower()
        assert row[field] == ped.NOT_RETURNED_NOTE

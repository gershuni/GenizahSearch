# -*- coding: utf-8 -*-
"""Multi-witness passage search at the ENGINE seam
(shared/passage_parallels.py's `witnesses=` parameter).

The unit-level ranking maths lives in tests/test_passage_fusion.py. This file
covers what only the engine can get wrong:

* single-witness parity -- the property that protects every existing caller;
* witnesses are searched SEPARATELY, never joined into one query (the
  measured difference between 85% and 59% of the reachable BH census);
* per-witness render context -- a span offset belongs to ONE witness's text;
* resolution: skip-and-report a bad reference, fail loudly only when nothing
  resolves at all;
* the synthesised query_report, so a truncation on witness 7 still ships.

Built on the same synthetic-index scaffolding as
tests/test_passage_parallels.py: this worktree carries no corpus data.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared import passage_parallels as pp  # noqa: E402
from shared.passage_builder import build_index  # noqa: E402
from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import norm_stream_fast  # noqa: E402
from shared.passage_parallels import (  # noqa: E402
    NoWitnessesResolved, PassageSearcher, _synthesize_query_report,
)

ALEF = 0x05D0


def _aperiodic(n: int, salt: int = 0) -> str:
    """Pseudo-random Hebrew letters with no short period (crib from
    tests/test_passage_search.py -- a periodic fixture collapses under gram
    dedup and is useless for testing ordinary-text retrieval)."""
    out = []
    x = 987_654_321 + salt
    for _ in range(n):
        x = (x * 1_103_515_245 + 12_345) & 0x7FFFFFFF
        out.append(chr(ALEF + (x >> 7) % 22))
    return ''.join(out)


def _rid(tag: int) -> str:
    return f"99{tag:08d}_IE{20_000_000 + tag}_P{tag:07d}_FL1"


class _FakeTextFetcher:
    def __init__(self, mapping: dict):
        self._mapping = mapping
        self.calls: list = []

    def get_full_text_by_header(self, full_header: str):
        self.calls.append(full_header)
        return self._mapping.get(full_header)


# Three records and two witnesses, arranged so every case the fusion has to
# handle is present: a record only witness 1 finds, a record only witness 2
# finds, and a record BOTH find (the collision that last-witness-wins would
# silently reduce to one).
RID_ONLY_1 = _rid(1)
RID_ONLY_2 = _rid(2)
RID_BOTH = _rid(3)


@pytest.fixture(scope='module')
def two_motif_corpus(tmp_path_factory):
    d = str(tmp_path_factory.mktemp('ppmw'))
    motif_1 = _aperiodic(90, salt=11)
    motif_2 = _aperiodic(90, salt=22)

    originals = {
        RID_ONLY_1: _aperiodic(150, salt=101) + ' ' + motif_1 + ' '
                    + _aperiodic(150, salt=102),
        RID_ONLY_2: _aperiodic(150, salt=201) + ' ' + motif_2 + ' '
                    + _aperiodic(150, salt=202),
        # Full motif 1, only PART of motif 2 -- so witness 1 wins this
        # record outright. A fixture where the LAST witness wins it cannot
        # detect a hit map keyed by record instead of by (witness, record):
        # last-witness-wins and winner-wins agree, and the mutation passes.
        RID_BOTH: _aperiodic(80, salt=301) + ' ' + motif_1 + ' '
                  + _aperiodic(80, salt=302) + ' ' + motif_2[:60] + ' '
                  + _aperiodic(80, salt=303),
    }
    # Filler, so the index is not three records wide.
    for r in range(10, 22):
        originals[_rid(r)] = _aperiodic(300, salt=900 + r)

    build_index(list(originals.items()), d, partitions=2, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, originals, motif_1, motif_2


@pytest.fixture
def searcher(two_motif_corpus):
    idx, originals, _m1, _m2 = two_motif_corpus
    return PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals))


def _headers(rows):
    return {r['raw_header'] for r in rows}


def _by_header(rows):
    return {r['raw_header']: r for r in rows}


# ---------------------------------------------------------------------------
# 1. Single-witness parity -- the property every existing caller rests on.
# ---------------------------------------------------------------------------

def test_omitting_witnesses_is_byte_identical_to_before(searcher,
                                                        two_motif_corpus):
    """`witnesses=None` must not change the result shape at all. Every
    existing caller, test and export path binds against this dict."""
    _idx, _o, motif_1, _m2 = two_motif_corpus
    result = searcher.search_composition_logic(full_text=motif_1)
    assert set(result.keys()) == {
        'main', 'filtered', 'dropped_text_lookup_failures',
        'duplicate_photography_demoted', 'query_report', 'truncated_to_200',
    }
    assert 'witness_report' not in result
    for row in result['main']:
        assert 'witness_id' not in row
        assert 'fusion_score' not in row


def test_one_witness_short_circuits_to_the_single_witness_path(
        searcher, two_motif_corpus):
    """RRF over ONE list is a 1/(k+rank) rescale carrying no information, and
    it would silently change `score` from matched letters to ~0.03 -- the
    number the Max:/Avg: badges and every export column read."""
    _idx, _o, motif_1, _m2 = two_motif_corpus
    direct = searcher.search_composition_logic(full_text=motif_1)
    via_witness = searcher.search_composition_logic(
        full_text='', witnesses=[{'text': motif_1, 'label': 'Seed'}])

    # `witness_report` IS present -- the caller asked for witnesses, so it
    # gets told what happened to them. Parity is about the ROWS and about
    # what an untouched caller sees, not about hiding a report someone
    # explicitly opted into.
    assert set(via_witness.keys()) - set(direct.keys()) == {
        'witness_report', 'per_witness_query_reports'}
    assert _headers(via_witness['main']) == _headers(direct['main'])
    a, b = _by_header(direct['main']), _by_header(via_witness['main'])
    for header, row in a.items():
        assert b[header]['score'] == row['score']
        assert b[header]['text'] == row['text']
        assert 'fusion_score' not in b[header]


# ---------------------------------------------------------------------------
# 2. Two witnesses.
# ---------------------------------------------------------------------------

def test_two_witnesses_reach_records_neither_reaches_alone(
        searcher, two_motif_corpus):
    """The whole point of the feature: one text per work is not enough."""
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    only_1 = _headers(searcher.search_composition_logic(
        full_text=motif_1)['main'])
    only_2 = _headers(searcher.search_composition_logic(
        full_text=motif_2)['main'])
    assert RID_ONLY_1 in only_1 and RID_ONLY_2 not in only_1
    assert RID_ONLY_2 in only_2 and RID_ONLY_1 not in only_2

    both = searcher.search_composition_logic(full_text='', witnesses=[
        {'text': motif_1, 'label': 'W1'}, {'text': motif_2, 'label': 'W2'}])
    assert {RID_ONLY_1, RID_ONLY_2, RID_BOTH} <= _headers(both['main'])


def test_a_record_both_witnesses_match_reports_both(searcher,
                                                    two_motif_corpus):
    """`hit_by_header` in the single-witness code was keyed by RECORD and
    collides across witnesses. Last-witness-wins there reduces every count to
    1 while the search still looks entirely healthy."""
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    result = searcher.search_composition_logic(full_text='', witnesses=[
        {'id': 'w1', 'text': motif_1}, {'id': 'w2', 'text': motif_2}])

    row = _by_header(result['main'])[RID_BOTH]
    assert row['witness_count'] == 2
    assert sorted(row['witness_ids'].split(',')) == ['w1', 'w2']
    assert row['fusion_score'] > 0

    solo = _by_header(result['main'])[RID_ONLY_1]
    assert solo['witness_count'] == 1
    assert solo['witness_ids'] == 'w1'


def test_multi_witness_rows_carry_the_winning_witness_and_its_evidence(
        searcher, two_motif_corpus):
    """A span offset is a position in ONE witness's text. Rendering the
    highlight through another witness's offset map points at the wrong
    letters and still looks plausible -- so the row must name its own."""
    _idx, originals, motif_1, motif_2 = two_motif_corpus
    result = searcher.search_composition_logic(full_text='', witnesses=[
        {'id': 'w1', 'label': 'First', 'text': motif_1},
        {'id': 'w2', 'label': 'Second', 'text': motif_2}])

    rows = _by_header(result['main'])
    row = rows[RID_ONLY_2]
    assert row['witness_id'] == 'w2'
    assert row['witness_label'] == 'Second'
    # The highlighted query-side context must come from THAT witness's text.
    plain = row['source_ctx'].replace('*', '')
    assert plain and plain in motif_2
    assert plain not in motif_1

    # The record BOTH witnesses matched is the case that matters: the engine's
    # hit map is keyed by (witness, record) precisely because a record-only
    # key collides here, and the resulting mismatch -- one witness's spans
    # projected through another's offset map -- renders a wrong highlight that
    # still looks entirely plausible.
    shared = rows[RID_BOTH]
    assert shared['witness_id'] == 'w1', (
        'fixture invariant: witness 1 must WIN this record, or the assertions '
        'below cannot distinguish winner-wins from last-witness-wins')
    # "It came from the winner's text" is NOT enough on its own: a span read
    # through the WRONG witness's offset map still lands inside some text and
    # still yields a plausible-looking slice. The check that bites is that the
    # query-side highlight is the SAME LETTERS as the manuscript-side one --
    # a mismatched offset map cannot satisfy that by accident.
    for _idx_, query_text, _len_, ms_snip in shared['chunk_hits']:
        q = norm_stream_fast(query_text)
        m = norm_stream_fast(ms_snip.replace('*', ''))
        assert q and q in m, (
            'the highlighted query text is not the text that matched -- the '
            'span was projected through the wrong offset map')


def test_multi_witness_result_carries_per_witness_reports(searcher,
                                                          two_motif_corpus):
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    result = searcher.search_composition_logic(full_text='', witnesses=[
        {'id': 'w1', 'label': 'First', 'text': motif_1},
        {'id': 'w2', 'label': 'Second', 'text': motif_2}])

    assert result['witness_report']['requested'] == 2
    assert result['witness_report']['searched'] == 2
    assert result['witness_report']['unresolved'] == []
    per = result['per_witness_query_reports']
    assert [p['witness_id'] for p in per] == ['w1', 'w2']
    assert [p['witness_label'] for p in per] == ['First', 'Second']


# ---------------------------------------------------------------------------
# 3. Never concatenate. This is the finding the whole design rests on.
# ---------------------------------------------------------------------------

def test_witnesses_are_searched_separately_never_joined(monkeypatch, searcher,
                                                        two_motif_corpus):
    """The passage engine spends a per-query POSTING BUDGET, so one long
    concatenated query starves -- measured 59% of the reachable BH census
    concatenated against 85% fused, and every concatenated Antiochus
    recursion round scored BELOW the seed alone.

    Pinned as a call-shape assertion because concatenation is the cheap,
    obvious refactor someone will reach for, and its damage is invisible in
    the result shape.
    """
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    seen: list = []
    real = pp.search_passage

    def _spy(index, text, policy, **kw):
        seen.append(text)
        return real(index, text, policy, **kw)

    monkeypatch.setattr(pp, 'search_passage', _spy)
    searcher.search_composition_logic(full_text='', witnesses=[
        {'text': motif_1}, {'text': motif_2}])

    assert seen == [motif_1, motif_2], 'witnesses must be searched one by one'
    for text in seen:
        assert motif_1 not in text or motif_2 not in text, (
            'a query containing BOTH witnesses is a concatenated query')


# ---------------------------------------------------------------------------
# 4. Resolution: refs, skip-and-report, hard failure only when nothing works.
# ---------------------------------------------------------------------------

def test_a_witness_can_be_a_raw_header_reference(searcher, two_motif_corpus):
    """Recursive promotion sends back a `raw_header` the client already has
    on every row, instead of re-uploading the whole page text."""
    _idx, _o, motif_1, _m2 = two_motif_corpus
    result = searcher.search_composition_logic(full_text='', witnesses=[
        {'id': 'w1', 'text': motif_1},
        {'id': 'w2', 'raw_header': RID_ONLY_2, 'label': 'Promoted'}])

    entries = {e['id']: e for e in result['witness_report']['witnesses']}
    assert entries['w2']['kind'] == 'manuscript'
    assert entries['w2']['resolved'] is True
    assert entries['w2']['letters'] > 0
    assert RID_ONLY_2 in _headers(result['main'])


@pytest.mark.parametrize('bad, reason', [
    ({'id': 'wx', 'raw_header': 'NO_SUCH_HEADER_1'}, 'not_found'),
    ({'id': 'wx', 'raw_header': 'has spaces and ../slashes'}, 'bad_ref'),
    ({'id': 'wx'}, 'empty'),
])
def test_an_unresolvable_witness_is_skipped_and_reported(
        searcher, two_motif_corpus, bad, reason):
    """Never fatal. Rejecting a seventeen-witness request over one stale
    reference throws away the sixteen searches the user asked for and can
    still have."""
    _idx, _o, motif_1, _m2 = two_motif_corpus
    result = searcher.search_composition_logic(
        full_text='', witnesses=[{'id': 'w1', 'text': motif_1}, bad])

    report = result['witness_report']
    assert report['requested'] == 2 and report['searched'] == 1
    assert [e['reason'] for e in report['unresolved']] == [reason]
    # ... and the good witness still ran.
    assert RID_ONLY_1 in _headers(result['main'])
    # One resolved witness means the SINGLE-witness path, so no fusion keys
    # on the rows -- but the report still says a witness was dropped.
    assert 'fusion_score' not in result['main'][0]


def test_a_bad_ref_never_reaches_the_text_fetcher(searcher):
    """The character-set check is the authoritative one and runs BEFORE the
    lookup -- web/search_api.py's copy is fail-fast UX, not a boundary."""
    searcher.text_fetcher.calls.clear()
    with pytest.raises(NoWitnessesResolved):
        searcher.search_composition_logic(
            full_text='', witnesses=[{'raw_header': '../../etc/passwd'}])
    assert searcher.text_fetcher.calls == []


def test_all_witnesses_unresolvable_raises_with_the_report(searcher):
    """An empty result set would be indistinguishable from an honest
    "no matches" -- the one thing a search must never be ambiguous about."""
    with pytest.raises(NoWitnessesResolved) as exc:
        searcher.search_composition_logic(full_text='', witnesses=[
            {'raw_header': 'NO_SUCH_A'}, {'raw_header': 'NO_SUCH_B'}])
    assert exc.value.report['searched'] == 0
    assert len(exc.value.report['unresolved']) == 2


def test_text_and_witnesses_together_is_an_error(searcher, two_motif_corpus):
    """Never silently pick one: the query the user believes was searched
    would differ from the one that ran."""
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    with pytest.raises(ValueError, match='EITHER'):
        searcher.search_composition_logic(
            full_text=motif_1, witnesses=[{'text': motif_2}])


def test_the_length_cap_is_re_checked_after_resolution(searcher,
                                                       two_motif_corpus):
    """Twenty-five tiny references can resolve to twenty-five 20,000-char
    pages, so a payload-only cap bounds the REQUEST, not the WORK."""
    _idx, originals, motif_1, _m2 = two_motif_corpus
    cap = len(originals[RID_ONLY_2]) - 1
    result = searcher.search_composition_logic(
        full_text='', witness_text_cap=cap, witnesses=[
            {'id': 'w1', 'text': motif_1},
            {'id': 'w2', 'raw_header': RID_ONLY_2}])
    assert [e['reason'] for e in result['witness_report']['unresolved']] \
        == ['too_long']


# ---------------------------------------------------------------------------
# 5. The synthesised query_report.
# ---------------------------------------------------------------------------

def test_synthesized_report_ors_booleans_and_sums_counters():
    """Passing through only the FIRST witness's report -- the obvious shape --
    under-reports exactly the case the report exists for."""
    a = {'policy_id': 'p', 'candidates': 10, 'verify_truncated': False,
         'seconds': 0.5}
    b = {'policy_id': 'p', 'candidates': 7, 'verify_truncated': True,
         'seconds': 0.25}
    out = _synthesize_query_report([a, b])
    assert out['candidates'] == 17
    assert out['verify_truncated'] is True
    assert out['seconds'] == pytest.approx(0.75)
    assert out['policy_id'] == 'p'


def test_synthesized_report_of_one_is_that_report_unchanged():
    """Identity, not a rebuild -- this is what keeps the single-witness
    result byte-identical."""
    a = {'policy_id': 'p', 'candidates': 10, 'verify_truncated': False}
    assert _synthesize_query_report([a]) is a


def test_real_multi_witness_report_sums_across_witnesses(searcher,
                                                         two_motif_corpus):
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    r1 = searcher.search_composition_logic(full_text=motif_1)['query_report']
    r2 = searcher.search_composition_logic(full_text=motif_2)['query_report']
    both = searcher.search_composition_logic(full_text='', witnesses=[
        {'text': motif_1}, {'text': motif_2}])['query_report']
    assert both['query_letters'] == r1['query_letters'] + r2['query_letters']
    assert both['verified'] == r1['verified'] + r2['verified']


# ---------------------------------------------------------------------------
# 6. The group cap must rank by the key the rows were SELECTED by.
# ---------------------------------------------------------------------------

def test_group_cap_orders_by_fusion_score_when_asked_to():
    """Without this, the cap discards exactly the groups the fusion promoted:
    rows are chosen by RRF and then thrown away by raw matched letters.

    `order_key`, NOT a score key: it decides which groups survive and must
    never reach `aggregate_score`, which becomes the envelope's `sort_score`
    and then the public `score`. Conflating the two turned that field into
    ~0.03 on multi-witness responses (review finding).

    Unit-level on hand-built rows, because making a synthetic index produce a
    fusion/letters disagreement is a fixture puzzle, not a test.
    """
    from shared.parallels_service import _cap_main_results_by_group
    from shared.passage_parallels import _RegexSysIdParser

    # Group A: one huge-scoring row found by ONE witness.
    # Group B: two modest rows found by THREE witnesses between them.
    rows = [
        {'raw_header': '9900000001_IE1_P1_FL1', 'score': 900.0,
         'fusion_score': 1 / 61},
        {'raw_header': '9900000002_IE2_P1_FL1', 'score': 100.0,
         'fusion_score': 3 / 61},
    ]
    by_letters, _ = _cap_main_results_by_group(
        rows, _RegexSysIdParser(), cap=1)
    by_fusion, _ = _cap_main_results_by_group(
        rows, _RegexSysIdParser(), cap=1, order_key='fusion_score')

    assert by_letters[0]['score'] == 900.0        # today's default, unchanged
    assert by_fusion[0]['score'] == 100.0         # the fusion-ranked group


# ---------------------------------------------------------------------------
# 7. filter_text across witnesses.
# ---------------------------------------------------------------------------

def test_a_record_is_filtered_only_when_every_witness_filters_it(
        searcher, two_motif_corpus):
    """Otherwise the "known source text" filter gets STRICTER the more
    witnesses you add -- the opposite of what the control says it does. One
    witness matching a record on text the user did NOT declare as a known
    source is a real result and has to survive."""
    _idx, _o, motif_1, motif_2 = two_motif_corpus
    result = searcher.search_composition_logic(
        full_text='', filter_text=motif_2,
        witnesses=[{'id': 'w1', 'text': motif_1},
                   {'id': 'w2', 'text': motif_2}])

    main, filtered = _headers(result['main']), _headers(result['filtered'])
    # Only witness 2 reaches it, and its match IS the declared source text.
    assert RID_ONLY_2 in filtered and RID_ONLY_2 not in main
    # Both reach it; witness 1's match is not the declared source text.
    assert RID_BOTH in main and RID_BOTH not in filtered
    assert RID_ONLY_1 in main


# ---------------------------------------------------------------------------
# 8. The ENGINE's own choice of cap key (section 6 tests the cap function; a
#    correct function called with the wrong key is still the same bug).
# ---------------------------------------------------------------------------

CAP_RID_STRONG = _rid(41)     # one witness, twice the matched letters
CAP_RID_SHARED = _rid(42)     # two witnesses, half the letters each


@pytest.fixture(scope='module')
def cap_corpus(tmp_path_factory):
    """Arranged so raw letters and rank fusion DISAGREE about which group
    wins: STRONG carries ALL of witness 1's long motif, SHARED carries only
    its first half plus all of witness 2's.

    Note a passage score counts matched QUERY letters, not manuscript ones,
    so repeating a motif inside a manuscript does NOT raise its score -- the
    fixture has to vary how much of the QUERY each record covers."""
    d = str(tmp_path_factory.mktemp('ppmwcap'))
    motif_1 = _aperiodic(180, salt=311)
    motif_2 = _aperiodic(90, salt=322)
    originals = {
        CAP_RID_STRONG: (_aperiodic(60, salt=401) + ' ' + motif_1 + ' '
                         + _aperiodic(60, salt=403)),
        CAP_RID_SHARED: (_aperiodic(60, salt=501) + ' ' + motif_1[:90] + ' '
                         + _aperiodic(60, salt=502) + ' ' + motif_2 + ' '
                         + _aperiodic(60, salt=503)),
    }
    for r in range(60, 72):
        originals[_rid(r)] = _aperiodic(300, salt=1900 + r)
    build_index(list(originals.items()), d, partitions=2, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, originals, motif_1, motif_2


def test_the_cap_keeps_the_fusion_ranked_group_not_the_wordiest(cap_corpus):
    """With render_cap=1 the two rules disagree, and the engine has to hand
    the cap the same key the rows were selected by. Otherwise a multi-witness
    search chooses rows by RRF and then discards them by matched letters."""
    idx, originals, motif_1, motif_2 = cap_corpus
    searcher = PassageSearcher(index=idx,
                               text_fetcher=_FakeTextFetcher(originals),
                               render_cap=1)

    # Sanity: the two rules really do disagree on this fixture.
    uncapped = PassageSearcher(index=idx,
                               text_fetcher=_FakeTextFetcher(originals),
                               render_cap=0).search_composition_logic(
        full_text='', witnesses=[{'id': 'w1', 'text': motif_1},
                                 {'id': 'w2', 'text': motif_2}])
    rows = _by_header(uncapped['main'])
    assert rows[CAP_RID_STRONG]['score'] > rows[CAP_RID_SHARED]['score']
    assert (rows[CAP_RID_SHARED]['fusion_score']
            > rows[CAP_RID_STRONG]['fusion_score'])

    capped = searcher.search_composition_logic(
        full_text='', witnesses=[{'id': 'w1', 'text': motif_1},
                                 {'id': 'w2', 'text': motif_2}])
    assert _headers(capped['main']) == {CAP_RID_SHARED}
    assert capped['truncated_to_200'] is True

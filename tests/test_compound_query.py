"""Composition Search conditions: the model, the masked search, the judgement.

Gates G1-G3 of docs/plans/COMPOSITION_CONDITIONS_PLAN.md:
- G1: page identity and the three places (literal P+-1, IE-aware, whole
  manuscript), and the page map that turns a scope into a record mask;
- G2: every cell of the judgement table, the cut reasons, strict
  (de)serialisation, and the EXHAUSTIVE masked search -- on a fixture built
  like Codex's round-2 counterexample, where the budgeted search cannot find
  the target and the masked one must;
- G3: the display cap applies after judging, to Main and Filtered only.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from shared import compound_query as cq
from shared.compound_query import (
    CAT_DROPPED, CAT_MAIN, CAT_REMOVED, CAT_UNRESOLVED, Condition, PageKey,
    VersionSearch, judge, parse_page_key, route, search_scope,
)
from shared.passage_builder import build_index
from shared.passage_index import open_index
from shared.passage_pages import PageIndex, StaleIndexError, clear_cache
from shared.passage_parallels import ConditionSearch, PassageSearcher, _RegexSysIdParser
from shared.passage_policy import get_preset
from shared.passage_search import search_passage

ALEF = 0x05D0
S1 = '990000000000000001'
S2 = '990000000000000002'


def rid(sys_id, ie, p, fl=1):
    return f'{sys_id}_IE{ie}_P{p:06d}_FL{fl}'


def cond(req, where, *versions, label=''):
    return Condition(req, where, tuple(versions or ('x' * 50,)), label)


def complete(*rids):
    return VersionSearch(tuple(rids), cq.CUT_NONE)


def cut(*rids, reason=cq.CUT_VERIFY):
    return VersionSearch(tuple(rids), reason)


# -- G1: identity and places --------------------------------------------------

def test_parse_page_key_corpus_local_and_v07_paths():
    assert parse_page_key(rid(S1, 77, 12)) == PageKey(S1, 'IE77', 12)
    assert parse_page_key('970012345601234567_LOCAL_P3_F0042') == \
        PageKey('970012345601234567', 'F0042', 3)
    v07 = (f'### Q:\\ERC\\all_version_0.9\\{S1}\\IE104549337\\'
           'IE104549337_P000002_FL104549340\u2014reco.xml - JER NLI 6972=8')
    assert parse_page_key(v07) == PageKey(S1, 'IE104549337', 2)
    for junk in ('', None, 'no ids here', f'{S1}_P000001'):
        assert parse_page_key(junk) is None


def test_search_scope_per_place():
    keys = [PageKey(S1, 'IE1', 5), PageKey(S2, 'IE9', 1)]
    pages, sys_ids = search_scope(cq.WHERE_SAME_IMAGE, keys)
    assert pages == {(S1, 'IE1', 5), (S2, 'IE9', 1)} and not sys_ids
    pages, sys_ids = search_scope(cq.WHERE_ADJACENT, keys)
    assert pages == {(S1, 'IE1', 4), (S1, 'IE1', 6), (S2, 'IE9', 0),
                     (S2, 'IE9', 2)} and not sys_ids
    pages, sys_ids = search_scope(cq.WHERE_MANUSCRIPT, keys)
    assert not pages and sys_ids == {S1, S2}


def _one(where, main, hit, req=cq.REQ_MUST):
    c = cond(req, where)
    return judge([main], [c], [[complete(hit)]])[main]


def test_adjacent_is_literal_p_plus_minus_one_in_the_same_image_set():
    main = rid(S1, 1, 5)
    assert _one(cq.WHERE_ADJACENT, main, rid(S1, 1, 6)).category == CAT_MAIN
    assert _one(cq.WHERE_ADJACENT, main, rid(S1, 1, 4)).category == CAT_MAIN
    # Across a gap (P6 absent from the index): P7 is NOT adjacent.
    assert _one(cq.WHERE_ADJACENT, main, rid(S1, 1, 7)).category == CAT_DROPPED
    # Same P+1 in ANOTHER image set of the same manuscript: not adjacent.
    assert _one(cq.WHERE_ADJACENT, main, rid(S1, 2, 6)).category == CAT_DROPPED
    # The page itself is not its own neighbour.
    assert _one(cq.WHERE_ADJACENT, main, rid(S1, 1, 5)).category == CAT_DROPPED


def test_same_image_and_same_manuscript():
    main = rid(S1, 1, 5)
    assert _one(cq.WHERE_SAME_IMAGE, main, rid(S1, 1, 5)).category == CAT_MAIN
    assert _one(cq.WHERE_SAME_IMAGE, main, rid(S1, 2, 5)).category == CAT_DROPPED
    # Any page of the manuscript counts, including one the main text did
    # not match, in another image set.
    assert _one(cq.WHERE_MANUSCRIPT, main, rid(S1, 3, 40)).category == CAT_MAIN
    assert _one(cq.WHERE_MANUSCRIPT, main, rid(S2, 1, 5)).category == CAT_DROPPED


# -- the page map and the masked search (G1/G2) ---------------------------------

def _aperiodic(n, salt=0):
    out, x = [], 987_654_321 + salt
    for _ in range(n):
        x = (x * 1_103_515_245 + 12_345) & 0x7FFFFFFF
        out.append(chr(ALEF + (x >> 7) % 22))
    return ''.join(out)


def _mutated(text, every):
    """`text` with every `every`-th letter replaced by a different letter."""
    chars = list(text)
    for i in range(every // 2, len(chars), every):
        chars[i] = chr(ALEF + ((ord(chars[i]) - ALEF + 7) % 22))
    return ''.join(chars)


QUERY = _aperiodic(80, salt=4242)
RECORD_TEXT: dict = {}        # record id -> text, filled by the fixture
TARGET = _mutated(QUERY, 10)          # 8 evenly spaced substitutions
N_TARGETS = 100


@pytest.fixture(scope='module')
def counter_index(tmp_path_factory):
    """Codex's round-2 counterexample as an index: 100 pages carrying a
    10%-substituted copy of the query, one page carrying it exactly (the
    decoy), filler pages, and one manuscript with two image sets that share
    page numbers and a gap."""
    d = str(tmp_path_factory.mktemp('compound'))
    records = []
    for n in range(N_TARGETS):
        sys_id = f'99{n + 10:016d}'
        body = _aperiodic(200, salt=n)
        records.append((rid(sys_id, n + 1, 1), body[:60] + TARGET + body[60:]))
    records.append((rid('99' + '9' * 16, 999, 1), _aperiodic(100, 7) + QUERY))
    for n in range(20):
        records.append((rid(f'99{n + 5000:016d}', 5000 + n, 1),
                        _aperiodic(300, salt=900 + n)))
    # A manuscript with IE1 pages 1, 2, 4 (3 left out) and IE2 page 2.
    for ie, p in ((1, 1), (1, 2), (1, 4), (2, 2)):
        records.append((rid(S1, ie, p), _aperiodic(150, salt=ie * 10 + p)))
    build_index(records, d, partitions=2, apply_hygiene=False)
    RECORD_TEXT.clear()
    RECORD_TEXT.update(dict(records))
    idx = open_index(d)
    assert idx is not None
    clear_cache()
    yield idx
    clear_cache()


class _NoText:
    def fetch(self, *a, **k):
        raise AssertionError('conditions must never fetch page text')

    def __getattr__(self, name):
        raise AssertionError('conditions must never fetch page text')


def _searcher(idx, policy=None):
    return PassageSearcher(index=idx, text_fetcher=_NoText(),
                           policy=policy or get_preset('standard-40'), render_cap=0)


def test_page_map_resolves_pages_sys_ids_and_gaps(counter_index):
    pmap = PageIndex.build_from(counter_index)
    assert pmap.unparsed == 0
    ie1_p2 = pmap.records_for_page(S1, 'IE1', 2)
    ie2_p2 = pmap.records_for_page(S1, 'IE2', 2)
    assert len(ie1_p2) == 1 and len(ie2_p2) == 1 and ie1_p2[0] != ie2_p2[0]
    assert len(pmap.records_for_page(S1, 'IE1', 3)) == 0   # left out: nothing
    mask = pmap.mask_for(counter_index, pages=[(S1, 'IE1', 2)])
    assert mask.sum() == 1 and mask[ie1_p2[0]]
    mask = pmap.mask_for(counter_index, sys_ids=[S1])
    assert mask.sum() == 4


def test_page_map_refuses_another_build(counter_index, tmp_path):
    other_dir = str(tmp_path / 'other')
    build_index([(rid(S2, 1, 1), _aperiodic(200, salt=3))], other_dir,
                partitions=1, apply_hygiene=False)
    other = open_index(other_dir)
    pmap = PageIndex.build_from(counter_index)
    with pytest.raises(StaleIndexError):
        pmap.mask_for(other, sys_ids=[S2])


def _target_records(idx):
    return {r for r in range(idx.n_records)
            if idx.record_id(r).startswith('99000000000000')
            and int(idx.record_id(r)[2:18]) >= 10
            and int(idx.record_id(r)[2:18]) < 10 + N_TARGETS}


def test_budgeted_search_misses_the_target_but_the_masked_search_finds_it(counter_index):
    idx = counter_index
    tight = replace(get_preset('standard-40'), posting_budget=80)
    targets = _target_records(idx)
    assert len(targets) == N_TARGETS
    # The ordinary (budgeted) search restricted to the targets: the admitted
    # rare grams live only in the decoy, so nothing forms -- and no flag says so.
    hits, report = search_passage(idx, QUERY, tight,
                                  record_allowed=lambda r: '_IE999_' not in r)
    assert not any(h.record in targets for h in hits)
    assert not report.candidates_truncated and not report.verify_truncated
    # The condition search over the same pages admits every gram.
    sys_ids = {idx.record_id(r).split('_')[0] for r in targets}
    found = _searcher(idx, tight).search_condition(QUERY, sys_ids=sys_ids)
    assert found.report['exhaustive_mask'] is True
    assert {h.split('_')[0] for h in found.hit_record_ids} == sys_ids
    assert found.as_version_search().cut == cq.CUT_NONE


def test_condition_search_never_leaves_its_mask(counter_index):
    idx = counter_index
    one = rid('99' + f'{10:016d}', 1, 1)
    found = _searcher(idx).search_condition(QUERY, pages=[(one.split('_')[0], 'IE1', 1)])
    assert found.hit_record_ids == (one,)


def test_empty_scope_is_complete_and_searches_nothing(counter_index):
    found = _searcher(counter_index).search_condition(
        QUERY, pages=[(S1, 'IE1', 3)])          # a page the index left out
    assert found.hit_record_ids == () and not found.stopped
    assert found.as_version_search().cut == cq.CUT_NONE


def test_a_stop_is_reported_even_though_hits_came_back(counter_index):
    idx = counter_index
    sys_ids = {idx.record_id(r).split('_')[0] for r in _target_records(idx)}
    asked = []

    def should_stop():
        asked.append(1)
        return len(asked) > 3          # let a couple of candidates verify
    found = _searcher(idx).search_condition(QUERY, sys_ids=sys_ids,
                                            should_stop=should_stop)
    assert found.stopped
    assert found.as_version_search().cut == cq.CUT_STOPPED


def test_stop_before_the_search_runs_nothing(counter_index):
    found = _searcher(counter_index).search_condition(
        QUERY, sys_ids=[S1], should_stop=lambda: True)
    assert found.stopped and found.hit_record_ids == ()


def test_record_mask_and_record_allowed_are_exclusive(counter_index):
    import numpy as np
    with pytest.raises(ValueError):
        search_passage(counter_index, QUERY, record_allowed=lambda r: True,
                       record_mask=np.ones(counter_index.n_records, bool))
    with pytest.raises(ValueError):
        search_passage(counter_index, QUERY, record_mask=np.ones(3, bool))


# -- G2: the judgement table ----------------------------------------------------

MAIN = rid(S1, 1, 5)
HIT = rid(S1, 1, 5)


@pytest.mark.parametrize('req, search, state, category', [
    (cq.REQ_MUST, complete(HIT), cq.SATISFIED, CAT_MAIN),
    (cq.REQ_MUST, cut(HIT), cq.SATISFIED, CAT_MAIN),
    (cq.REQ_MUST, complete(), cq.FAILED, CAT_DROPPED),
    (cq.REQ_MUST, cut(), cq.UNRESOLVED, CAT_UNRESOLVED),
    (cq.REQ_MUST_NOT, complete(HIT), cq.REMOVED, CAT_REMOVED),
    (cq.REQ_MUST_NOT, cut(HIT), cq.REMOVED, CAT_REMOVED),
    (cq.REQ_MUST_NOT, complete(), cq.SATISFIED, CAT_MAIN),
    (cq.REQ_MUST_NOT, cut(), cq.UNRESOLVED, CAT_UNRESOLVED),
])
def test_judgement_table(req, search, state, category):
    oc = judge([MAIN], [cond(req, cq.WHERE_SAME_IMAGE)], [[search]])[MAIN]
    assert oc.states == (state,) and oc.category == category
    if state == cq.UNRESOLVED:
        assert oc.reasons == (cq.CUT_VERIFY,)
    if state in (cq.REMOVED,) or (state == cq.SATISFIED and search.hit_record_ids):
        assert oc.evidence == ((HIT,),)


def test_any_version_counts_and_a_cut_version_spoils_absence():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, ('a' * 50, 'b' * 50))
    assert judge([MAIN], [c], [[complete(), complete(HIT)]])[MAIN].category == CAT_REMOVED
    oc = judge([MAIN], [c], [[complete(), cut(reason=cq.CUT_STOPPED)]])[MAIN]
    assert oc.category == CAT_UNRESOLVED and oc.reasons == (cq.CUT_STOPPED,)


def test_precedence_dropped_then_removed_then_unresolved():
    must, mustnot, other = (cond(cq.REQ_MUST, cq.WHERE_SAME_IMAGE),
                            cond(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE),
                            cond(cq.REQ_MUST, cq.WHERE_MANUSCRIPT))
    oc = judge([MAIN], [must, mustnot], [[complete()], [complete(HIT)]])[MAIN]
    assert oc.category == CAT_DROPPED
    oc = judge([MAIN], [mustnot, other], [[complete(HIT)], [cut()]])[MAIN]
    assert oc.category == CAT_REMOVED


def test_an_unidentified_main_page_or_hit_never_proves_anything():
    c = cond(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE)
    oc = judge(['not-a-record'], [c], [[complete()]])['not-a-record']
    assert oc.category == CAT_UNRESOLVED and oc.reasons == (cq.CUT_UNIDENTIFIED,)
    oc = judge([MAIN], [c], [[complete('garbage-hit')]])[MAIN]
    assert oc.category == CAT_UNRESOLVED


def test_judge_requires_every_version_and_condition():
    c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('a' * 50, 'b' * 50))
    with pytest.raises(ValueError):
        judge([MAIN], [c], [[complete()]])
    with pytest.raises(ValueError):
        judge([MAIN], [c], [])


@pytest.mark.parametrize('report, kwargs, expected', [
    ({}, {}, cq.CUT_NONE),
    ({'candidates_truncated': True}, {}, cq.CUT_CANDIDATES),
    ({'verify_truncated': True}, {}, cq.CUT_VERIFY),
    ({'below_min_span_query': True}, {}, cq.CUT_BELOW_FLOOR),
    ({'postings_excluded': 10**6}, {}, cq.CUT_NONE),
    ({}, {'stopped': True}, cq.CUT_STOPPED),
    ({'verify_truncated': True}, {'failed': True}, cq.CUT_FAILED),
])
def test_cut_reason(report, kwargs, expected):
    assert cq.cut_reason(report, **kwargs) == expected


def test_outcome_round_trip_and_strict_restore():
    c = [cond(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT)]
    oc = judge([MAIN], c, [[complete(rid(S1, 2, 9))]])[MAIN]
    assert cq.PageOutcome.from_dict(oc.to_dict(), 1) == oc
    bad = oc.to_dict()
    bad['category'] = CAT_MAIN               # does not follow from REMOVED
    with pytest.raises(ValueError):
        cq.PageOutcome.from_dict(bad, 1)
    with pytest.raises(ValueError):
        cq.PageOutcome.from_dict(oc.to_dict(), 2)
    with pytest.raises(ValueError):
        cq.PageOutcome.from_dict(None, 1)


def test_condition_round_trip_and_strict_restore():
    c = Condition(cq.REQ_MUST, cq.WHERE_ADJACENT, ('one', 'two'), 'HG')
    assert Condition.from_dict(c.to_dict()) == c
    for broken in ({**c.to_dict(), 'requirement': 'maybe'},
                   {**c.to_dict(), 'where': 'nearby'},
                   {**c.to_dict(), 'versions': []},
                   {**c.to_dict(), 'versions': ['ok', '  ']},
                   'not a dict'):
        with pytest.raises(ValueError):
            Condition.from_dict(broken)


def test_validate_limits():
    long_ok = 'א' * 60
    ok = [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (long_ok,))]
    assert cq.validate(ok, 1, 40) == []
    five = ok * 5
    assert {p.code for p in cq.validate(five, 1, 40)} == {'too_many_conditions'}
    short = [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('א' * 39,))]
    assert [p.code for p in cq.validate(short, 1, 40)] == ['version_too_short']
    huge = [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('א' * 10_001,))]
    assert [p.code for p in cq.validate(huge, 1, 40)] == ['version_too_long']
    many = [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (long_ok,) * 10)] * 3
    assert 'too_many_searches' in {p.code for p in cq.validate(many, 1, 40)}
    eleven = [Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (long_ok,) * 11)]
    assert 'too_many_versions' in {p.code for p in cq.validate(eleven, 1, 40)}


def test_fingerprint_changes_with_every_part_of_the_query():
    c = [cond(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, 'a' * 50)]
    base = cq.fingerprint(['main'], c, {'width': 'w'})
    assert base == cq.fingerprint(['main'], c, {'width': 'w'})
    assert base != cq.fingerprint(['main2'], c, {'width': 'w'})
    assert base != cq.fingerprint(['main'], [cond(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE, 'a' * 50)],
                                  {'width': 'w'})
    assert base != cq.fingerprint(['main'], c, {'width': 'x'})


# -- G3: the cap comes after the judgement ------------------------------------------

def _row(n, score):
    return {'raw_header': rid(f'99{n + 1:016d}', 1, 1), 'score': float(score),
            'uid': f'u{n}', 'text': ''}


def test_cap_after_judging_keeps_rank_250_and_every_removed_page():
    rows = [_row(n, 1000 - n) for n in range(460)]          # rank = n + 1
    c = [cond(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE)]
    removed_hits = [r['raw_header'] for r in rows[:230]]     # the leaders
    outcomes = judge([r['raw_header'] for r in rows], c, [[complete(*removed_hits)]])
    routed = route(rows, [], outcomes, 1, cap=200, sys_parser=_RegexSysIdParser())
    kept = {r['uid'] for r in routed.main}
    assert len(routed.main) == 200 and routed.main_truncated
    assert 'u249' in kept                                    # rank 250
    assert len(routed.removed) == 230                        # never capped
    assert all(r['compound']['category'] == CAT_REMOVED for r in routed.removed)


def test_route_caps_filtered_too_and_counts_failures():
    main = [_row(n, 100 - n) for n in range(3)]
    filt = [_row(10 + n, 50 - n) for n in range(250)]
    c = [cond(cq.REQ_MUST, cq.WHERE_SAME_IMAGE)]
    hits = [r['raw_header'] for r in main[:2] + filt]
    outcomes = judge([r['raw_header'] for r in main + filt], c, [[complete(*hits)]])
    routed = route(main, filt, outcomes, 1, cap=200, sys_parser=_RegexSysIdParser())
    assert len(routed.main) == 2 and routed.failed_counts == (1,)
    assert len(routed.filtered) == 200 and routed.filtered_truncated
    assert all(r['compound_bucket'] == 'filtered' for r in routed.filtered)


def test_route_refuses_an_unjudged_row():
    with pytest.raises(ValueError):
        route([_row(1, 5)], [], {}, 1, cap=200, sys_parser=_RegexSysIdParser())


def test_identity_tells_apart_builds_whose_manifests_coincide(tmp_path):
    """Two builds of the same pages in a different record ORDER can carry
    identical manifests (measured: 1 pair in 5 when the build timings round
    alike). Record numbers mean nothing across them, so the identity must
    differ even when the manifests are made equal."""
    from shared.passage_pages import build_identity
    recs = [(rid(S1, 1, 1), _aperiodic(200, 1)), (rid(S1, 1, 2), _aperiodic(200, 2)),
            (rid(S2, 1, 2), _aperiodic(200, 3))]
    a_dir, b_dir = str(tmp_path / 'a'), str(tmp_path / 'b')
    build_index(recs, a_dir, partitions=1, apply_hygiene=False)
    build_index([recs[1], recs[2], recs[0]], b_dir, partitions=1, apply_hygiene=False)
    a, b = open_index(a_dir), open_index(b_dir)
    b.manifest = dict(a.manifest)
    assert build_identity(a) != build_identity(b)
    pmap = PageIndex.build_from(a)
    with pytest.raises(StaleIndexError):
        pmap.mask_for(b, pages=[(S1, 'IE1', 2)])


class _Texts:
    def get_full_text_by_header(self, header):
        return RECORD_TEXT.get(header)


def test_rendered_evidence_rows_carry_text_and_a_highlight(counter_index):
    idx = counter_index
    sys_ids = {idx.record_id(r).split('_')[0] for r in _target_records(idx)}
    searcher = PassageSearcher(index=idx, text_fetcher=_Texts(),
                               policy=get_preset('standard-40'), render_cap=0)
    found = searcher.search_condition(QUERY, sys_ids=sys_ids, render_limit=5)
    assert len(found.hit_record_ids) == N_TARGETS
    assert len(found.rows) == 5
    for row in found.rows:
        assert row['raw_header'] in found.hit_record_ids
        assert row['text'] and row['highlight_pattern']
    # Not asked for: nothing rendered, and no page text is fetched.
    assert _searcher(idx).search_condition(QUERY, sys_ids=sys_ids).rows == ()


# -- own text (owner, 2026-10-14): a must_not condition removes a page only ----
# when the quoting text contributes letters of its OWN -- not merely because
# it also quotes (and therefore matches on) the main text's own manuscripts.

R1, R2 = 'rec-1', 'rec-2'


def _cs(windows, *, stopped=False, report=None):
    """A ConditionSearch carrying only what own_text_runs /
    must_not_version_search read: `windows` is {record_id: ((r0, r1), ...)}."""
    return ConditionSearch((), (), report or {}, stopped, (), dict(windows))


def test_own_text_runs_fully_covered_is_not_found():
    cond = {R1: ((50, 90),)}
    # Widened by EDGE_ALLOWANCE=12 the protection already covers (50, 90)
    # without even needing the margin.
    protect = {R1: ((40, 100),)}
    assert cq.own_text_runs(cond, protect, min_run=40, edge=12) == {}


def test_own_text_runs_39_vs_40():
    cond = {R1: ((0, 39),), R2: ((0, 40),)}
    assert cq.own_text_runs(cond, {}, min_run=40, edge=12) == {R2: 40}


def test_own_text_runs_two_islands_do_not_add_up():
    # One 100-letter quoting match, split by a protected middle (widened to
    # (25, 75)) into two 25-letter islands -- NEITHER alone reaches 40, and
    # the rule never adds scattered islands together.
    cond = {R1: ((0, 100),)}
    protect = {R1: ((37, 63),)}
    assert cq.own_text_runs(cond, protect, min_run=40, edge=12) == {}


def test_own_text_runs_overhang_within_edge_is_swallowed_but_13_is_not_enough_alone():
    protect = {R1: ((100, 200),)}         # widened (edge=12) to (88, 212)
    # Exactly the widened bounds: fully swallowed, nothing left over.
    assert cq.own_text_runs({R1: ((88, 212),)}, protect, min_run=40, edge=12) == {}
    # 13 letters past the widened bound on one side -- real overhang, but
    # far short of OWN_TEXT_MIN on its own.
    assert cq.own_text_runs({R1: ((88, 225),)}, protect, min_run=40, edge=12) == {}
    # The SAME overhang, grown until it alone reaches the floor: counted.
    assert cq.own_text_runs({R1: ((88, 252),)}, protect, min_run=40, edge=12) == {R1: 40}


def test_own_text_runs_edge_widening_changes_the_outcome():
    """Mutation-sensitive: without the edge widening this run would reach
    OWN_TEXT_MIN (50 letters); with it applied (correct), it falls 2 letters
    short."""
    cond = {R1: ((50, 250),)}
    protect = {R1: ((100, 200),)}
    assert cq.own_text_runs(cond, protect, min_run=40, edge=12) == {}


def test_must_not_version_search_applies_own_text_per_record():
    cond_search = _cs({R1: ((0, 100),), R2: ((0, 100),)})
    protection = _cs({R1: ((0, 100),)})     # R1 fully protected, R2 untouched
    vs = cq.must_not_version_search(cond_search, [protection])
    assert vs == VersionSearch((R2,), cq.CUT_NONE)


def test_must_not_version_search_protection_cut_removes_nothing():
    """ANY protection search that does not prove its own completeness blocks
    the whole version -- nothing is "found", regardless of what the quoting
    text's own search turned up."""
    cond_search = _cs({R1: ((0, 100),)})          # plenty of "own text"
    protection = _cs({}, stopped=True)            # the protection search was cut
    vs = cq.must_not_version_search(cond_search, [protection])
    assert vs == VersionSearch((), cq.CUT_STOPPED)
    # The quoting text's OWN cut is still honoured once protection is clean.
    cut_cond = _cs({}, report={'verify_truncated': True})
    clean_protection = _cs({})
    vs2 = cq.must_not_version_search(cut_cond, [clean_protection])
    assert vs2 == VersionSearch((), cq.CUT_VERIFY)


def test_must_not_version_search_seed_windows_protect_too():
    """`seed_windows` (the main search's OWN matched-core windows on its own
    result pages) protects a record EVEN WHEN the protection search itself
    found nothing there -- the union, not a replacement: a page the
    protection search legitimately covers on its own (R2 here) is still
    protected without any seed_windows entry for it."""
    cond_search = _cs({R1: ((0, 100),), R2: ((0, 100),)})
    protection = _cs({R2: ((0, 100),)})           # protection re-found R2 only
    seed_windows = {R1: ((0, 100),)}              # the main search found R1 itself
    vs = cq.must_not_version_search(cond_search, [protection],
                                    seed_windows=seed_windows)
    assert vs == VersionSearch((), cq.CUT_NONE)   # neither record removed


def test_protection_scopes_one_per_distinct_must_not_where():
    keys = [cq.PageKey(S1, 'IE1', 5)]
    conditions = [
        cond(cq.REQ_MUST_NOT, cq.WHERE_SAME_IMAGE),
        cond(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT),
        cond(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT),   # same scope as above
        cond(cq.REQ_MUST, cq.WHERE_ADJACENT),         # not must_not: no scope
    ]
    scopes = cq.protection_scopes(conditions, keys)
    assert set(scopes) == {cq.WHERE_SAME_IMAGE, cq.WHERE_MANUSCRIPT}
    assert scopes[cq.WHERE_SAME_IMAGE] == cq.search_scope(cq.WHERE_SAME_IMAGE, keys)
    assert scopes[cq.WHERE_MANUSCRIPT] == cq.search_scope(cq.WHERE_MANUSCRIPT, keys)


def test_seed_protection_windows_unions_by_record_and_skips_blanks():
    rows = [
        {'raw_header': R1, 'page_windows': ((0, 50),)},
        {'raw_header': R1, 'page_windows': ((60, 100),)},   # a second row, same record
        {'raw_header': R2, 'page_windows': ()},             # present but empty: skipped
        {'raw_header': R2},                                 # missing entirely: skipped
        {'page_windows': ((0, 10),)},                       # no raw_header: skipped
    ]
    assert cq.seed_protection_windows(rows) == {R1: ((0, 50), (60, 100))}
    assert cq.seed_protection_windows([]) == {}


# -- own text on the REAL engine (owner, 2026-10-14) ---------------------------
# Pins the fix itself: PassageHit.windows / ConditionSearch.hit_windows must be
# page-side, PRE-MERGE, MATCHED-CORE bounds -- never the margin-extended
# alignment window `spans` carries -- or OWN_TEXT_MIN stops meaning "40 page
# letters actually matched" in both directions (too easy to clear with padding
# on one side of the comparison, too hard with padding on the other).

S3 = '990000000000000003'
S4 = '990000000000000004'
S5 = '990000000000000005'
QUOTE_TEXT = _aperiodic(100, salt=5151)         # what the main text IS
OWN_TEXT = _aperiodic(60, salt=6262)            # the quoting work's OWN 60 letters
# The embedded quote+own copy's last EDGE_NOISE letters are page noise (HTR
# misread at the tail of the quoted passage), so the true matched span falls
# a FEW letters short of the full 160 -- which is also what makes this fixture
# mutation-sensitive: a verbatim, edge-to-edge match never engages the
# verify-margin extension at all (both q0/q1 clip to the query's own bounds,
# so core and margin-extended windows come out identical regardless of which
# one the code stores), so a fixture without this noise cannot tell the fix
# from the regression it fixes.
EDGE_NOISE = 4


def _edge_noise(s: str, n: int, shift: int = 11) -> str:
    """`s` with its LAST `n` letters replaced by a different, fixed shift --
    just enough that the anchors covering the tail no longer match."""
    if n <= 0:
        return s
    head, tail = s[:-n], s[-n:]
    noisy_tail = ''.join(chr(ALEF + ((ord(c) - ALEF + shift) % 22)) for c in tail)
    return head + noisy_tail


@pytest.fixture(scope='module')
def own_text_index(tmp_path_factory):
    """Two pages: S3 is the quoting work's own page (the quote plus 60
    letters that are its OWN, not the main text's, with the LAST few letters
    of that embedded copy noised so the match is not edge-to-edge -- see
    EDGE_NOISE); S4 is "only the quote" -- a control for a page that
    contributes nothing of its own."""
    d = str(tmp_path_factory.mktemp('own_text_engine'))
    noisy_quote_own = _edge_noise(QUOTE_TEXT + OWN_TEXT, EDGE_NOISE)
    records = [
        (rid(S3, 1, 1),
         _aperiodic(150, salt=1) + noisy_quote_own + _aperiodic(150, salt=2)),
        (rid(S4, 1, 1),
         _aperiodic(150, salt=3) + QUOTE_TEXT + _aperiodic(150, salt=4)),
        # The quote twice, 20 letters apart: two matches on one page.
        (rid(S5, 1, 1),
         _aperiodic(150, salt=7) + QUOTE_TEXT + _aperiodic(20, salt=8)
         + QUOTE_TEXT + _aperiodic(150, salt=9)),
    ]
    build_index(records, d, partitions=1, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    clear_cache()
    yield idx
    clear_cache()


def test_own_text_runs_on_real_engine_output(own_text_index):
    s = _searcher(own_text_index)
    a_id, b_id = rid(S3, 1, 1), rid(S4, 1, 1)
    # Protection: the main text, which IS the quote, searched over both pages.
    protection = s.search_condition(QUOTE_TEXT, sys_ids=(S3, S4))
    # The quoting work's OWN search: the quote plus its own 60 letters.
    quoting = s.search_condition(QUOTE_TEXT + OWN_TEXT, sys_ids=(S3, S4))
    assert a_id in quoting.hit_record_ids
    # Windows are page-side (fit inside the 460-letter S3 record) and
    # PRE-MERGE (own_text_runs does its own merging; a window handed to it
    # already merged into one span would still pass this, so the real
    # assertion is the exact run length below, which a margin-extended
    # window gets WRONG -- not absent, not merely under the floor, but a
    # different, inflated number: 48 instead of 44, measured against the
    # unfixed code).
    for wins in quoting.hit_windows.values():
        for lo, hi in wins:
            assert 0 <= lo < hi <= 460
    runs = cq.own_text_runs(quoting.hit_windows, protection.hit_windows,
                            min_run=cq.OWN_TEXT_MIN, edge=cq.EDGE_ALLOWANCE)
    # S3's matched core falls EDGE_NOISE=4 letters short of its true 60 own
    # letters (the noised tail has no matching anchors), and EDGE_ALLOWANCE=12
    # further widens protection's own core across the quote/own boundary,
    # eating 12 more -- 60 - 4 - 12 = 44, comfortably over OWN_TEXT_MIN=40.
    # S4 -- only the quote, nothing of its own -- contributes no run at all.
    assert runs == {a_id: 44}
    assert b_id not in runs


def test_condition_windows_are_page_offsets_of_each_match_unmerged(own_text_index):
    """The windows are PAGE offsets (query-side offsets would read (0, 100)),
    and two matches on one page stay two windows: the merged span would also
    cover the 20 unmatched letters between them."""
    s = _searcher(own_text_index)
    found = s.search_condition(QUOTE_TEXT, sys_ids=(S4, S5))
    assert found.hit_windows[rid(S4, 1, 1)] == ((150, 250),)
    assert sorted(found.hit_windows[rid(S5, 1, 1)]) == [(150, 250), (270, 370)]

# -*- coding: utf-8 -*-
"""Tests for `scripts/score_display_deck.py`, the IPW scorer for the
stratified display-policy grading deck (external review, Codex 2026-08-21;
stratified-bootstrap rewrite, 2026-08-21, second integration smoke).

Every fixture is FABRICATED: opaque card/query/record/sys ids, never real
research content. deck_key.json / deck_manifest.json / prereg.json are
built by hand with the same hashing scheme scripts/score_grading_deck.py
and scripts/build_grading_deck.py use (cross-checked directly against a
real scripts.score_grading_deck.sha() call below), so the manifest's
key_hash always matches unless a test deliberately corrupts it.

Where the arithmetic can be checked by hand, the test asserts the EXACT
expected point value. `_views_manifest` auto-derives N_display as the sum
of each view's declared stratum N_h values, matching the invariant
validate_structure now enforces -- there is no more per-query padding
concern (the earlier ratio-bootstrap design's padding is gone along with
that design; the stratified bootstrap bounds precision by construction
regardless of how any one query's own cards happen to fall across strata).
"""
from __future__ import annotations

import json
import random

import pytest

from scripts import score_display_deck as sdd
from scripts import score_grading_deck as sgd


def _sel(view, stratum, pi_h, N_h, n_h, rank=1, rank_band='1-3',
        span_band='<60'):
    return {'view': view, 'stratum': stratum, 'pi_h': pi_h, 'N_h': N_h,
           'n_h': n_h, 'rank': rank, 'rank_band': rank_band,
           'span_band': span_band}


def _card(cid, qid, rid, sys_id, selections, is_source=False):
    return {'id': cid, 'query_id': qid, 'record_id': rid, 'sys_id': sys_id,
           'is_source': is_source, 'selections': selections}


def _views_manifest(strata_by_view):
    """{view: {N_display, strata}} -- N_display is always the exact sum of
    that view's declared stratum N_h values, matching validate_structure's
    invariant by construction (so most tests never need to think about it).
    """
    out = {}
    for view, strata in strata_by_view.items():
        out[view] = {'N_display': sum(s['N_h'] for s in strata.values()),
                     'strata': strata}
    return out


def _write_deck(tmp_path, key_list, views_manifest, panel_n_queries=10,
               n_cards=None, name='deck'):
    """Write a self-consistent deck_key.json + deck_manifest.json +
    prereg.json. Returns (deck_dir, deck_id) where deck_id is the
    manifest's cards_hash prefix a verdicts export must declare.
    """
    deck_dir = tmp_path / name
    deck_dir.mkdir(exist_ok=True)
    (deck_dir / 'deck_key.json').write_text(
        json.dumps(key_list, ensure_ascii=False), encoding='utf-8')
    cards_hash = 'ab' * 32  # fabricated 64-hex-char placeholder
    manifest = {
        'cards_hash': cards_hash,
        'key_hash': sdd.sha(key_list),
        'n_cards': n_cards if n_cards is not None else len(key_list),
        'n_queries': len({k['query_id'] for k in key_list}),
        'views': views_manifest,
    }
    (deck_dir / 'deck_manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
    (deck_dir / 'prereg.json').write_text(
        json.dumps({'panel_n_queries': panel_n_queries}, ensure_ascii=False),
        encoding='utf-8')
    return str(deck_dir), cards_hash[:16]


def _write_verdicts(tmp_path, deck_id, rows, name='verdicts.json'):
    p = tmp_path / name
    p.write_text(json.dumps({'deck': deck_id, 'grader': 'test',
                            'verdicts': rows}, ensure_ascii=False),
                encoding='utf-8')
    return str(p)


def _old_query_clustered_ratio_bootstrap(numer_by_query, denom_by_query,
                                         point_denom, resamples, seed):
    """Minimal reconstruction of the SUPERSEDED per-query ratio bootstrap
    (FIX 1, scripts/score_display_deck.py prior to the stratified-
    bootstrap rewrite) -- kept HERE ONLY, as a regression fixture, to
    demonstrate that it escapes [0, 1] on realistic stratum-level weights
    where the CURRENT stratified_bootstrap does not. Not production code;
    resamples query ids (not strata) and recomputes a per-query
    denominator alongside the numerator each replicate.
    """
    keys = sorted(numer_by_query)
    point = sum(numer_by_query.values()) / point_denom
    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        num = den = 0.0
        for _ in range(len(keys)):
            g = keys[rng.randrange(len(keys))]
            num += numer_by_query[g]
            den += denom_by_query[g]
        stats.append(num / den if den else 0.0)
    stats.sort()

    def q_(p):
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    return {'point': point, 'ci95': [q_(0.025), q_(0.975)]}


# ---------------------------------------------------------------------------
# sha() cross-check
# ---------------------------------------------------------------------------


def test_sha_matches_score_grading_deck_sha():
    for obj in ({'a': 1, 'b': [1, 2, 3]}, [{'id': 'x'}, {'id': 'y'}], []):
        assert sdd.sha(obj) == sgd.sha(obj)


# ---------------------------------------------------------------------------
# The IPW arithmetic, hand-computed
# ---------------------------------------------------------------------------


def test_hand_computed_ipw_precision_and_yield(tmp_path):
    """One stratum, N_h=10, n_h=4 -> weight = 10/4 = 2.5. Four cards
    graded: 2 strict (same_text, paraphrase), 2 not (unrelated x2) ->
    strict sum = 2. Point-total = 2.5 * 2 = 5.0. N_display = N_h = 10 ->
    P_hat = 5/10 = 0.500. panel_n_queries=4 -> Y_hat = 5/4 = 1.250.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 0.4, 10, 4)]),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 0.4, 10, 4)]),
        _card('c3', 'q3', 'r3', 's3', [_sel('S', 'A', 0.4, 10, 4)]),
        _card('c4', 'q4', 'r4', 's4', [_sel('S', 'A', 0.4, 10, 4)]),
    ]
    strata_by_view = {'S': {'A': {'N_h': 10, 'n_h': 4}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=4)
    rows = [{'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'paraphrase'},
           {'id': 'c3', 'grade': 'unrelated'},
           {'id': 'c4', 'grade': 'unrelated'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    s = res['views']['S']
    assert s['n_display'] == 10
    assert s['precision']['overall']['point'] == 0.5
    assert s['yield']['overall']['point'] == 1.25


def test_card_selected_in_two_views_contributes_to_both(tmp_path):
    """c1 is drawn into BOTH S (weight 1.0, census) and W (weight 2.0),
    one grade.

    S: strict contribution = 1.0*1 = 1.0, N_display=1 -> P_hat = 1.0.
    W: strict contribution = 2.0*1 = 2.0, N_display=4 -> P_hat = 0.5.
    Same card, same grade, two independent view-scoped contributions --
    each view's OWN stratum weight applies, not the card's own pi_h.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [
            _sel('S', 'A', 1.0, 1, 1),
            _sel('W', 'A', 0.5, 4, 2),
        ]),
    ]
    strata_by_view = {
        'S': {'A': {'N_h': 1, 'n_h': 1}},
        'W': {'A': {'N_h': 4, 'n_h': 2}},
    }
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=1)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['precision']['overall']['point'] == 1.0
    assert res['views']['W']['precision']['overall']['point'] == 0.5


def test_duplicate_photo_lowers_strict_but_reported_separately(tmp_path):
    """5 cards, one census stratum (weight=1.0): 4 strict + 1
    duplicate_photo.

    Strict sum = 4.0 -> P_hat = 4/5 = 0.8 (duplicate_photo contributes
    NOTHING to the strict numerator, but it still occupies one of the 5
    N_display slots). duplicate_photo weighted sum = 1.0 -> rate = 1/5
    = 0.2, reported as its OWN number, not folded into strict.
    """
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 5, 5)]) for i in range(5)]
    strata_by_view = {'S': {'A': {'N_h': 5, 'n_h': 5}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=5)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'paraphrase'},
           {'id': 'c3', 'grade': 'same_text'},
           {'id': 'c4', 'grade': 'duplicate_photo'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    s = res['views']['S']
    assert s['precision']['overall']['point'] == 0.8
    assert s['duplicate_photo_rate']['overall']['point'] == 0.2


def test_non_source_restricted_precision_uses_same_fixed_denominator(tmp_path):
    """D3: 'restricted to non-source' filters the NUMERATOR only.

    c1 (is_source=True) and c2 (is_source=False), one census stratum
    (weight=1.0, N_h=n_h=4 -- more design capacity than the 2 cards
    graded here, matching a realistic partial-grading situation). Both
    strict. Overall = 1.0*(1+1)/4 = 0.5. Non-source = 1.0*1/4 = 0.25
    (only c2's contribution), SAME denominator 4.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 4, 4)],
             is_source=True),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 1.0, 4, 4)],
             is_source=False),
    ]
    strata_by_view = {'S': {'A': {'N_h': 4, 'n_h': 4}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=2)
    rows = [{'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'same_text'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    s = res['views']['S']
    assert s['precision']['overall']['point'] == 0.5
    assert s['precision']['non_source']['point'] == 0.25


def test_relation_mix_by_rank_band(tmp_path):
    key_list = [
        _card('c1', 'q1', 'r1', 's1',
             [_sel('S', 'A', 1.0, 2, 2, rank=1, rank_band='1-3')]),
        _card('c2', 'q2', 'r2', 's2',
             [_sel('S', 'A', 1.0, 2, 2, rank=2, rank_band='1-3')]),
        _card('c3', 'q3', 'r3', 's3',
             [_sel('S', 'B', 1.0, 1, 1, rank=5, rank_band='4-10')]),
    ]
    strata_by_view = {'S': {'A': {'N_h': 2, 'n_h': 2}, 'B': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=3)
    rows = [{'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'unrelated'},
           {'id': 'c3', 'grade': 'paraphrase'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    mix = res['views']['S']['relation_mix_by_rank_band']
    assert mix['1-3']['n'] == 2
    assert mix['1-3']['mix'] == {'same_text': 0.5, 'unrelated': 0.5}
    assert mix['4-10']['n'] == 1
    assert mix['4-10']['mix'] == {'paraphrase': 1.0}


def test_yield_point_and_bootstrap_share_the_same_fixed_denominator(tmp_path):
    """Simplification under the stratified rewrite: yield's point AND
    every bootstrap replicate divide by the SAME fixed panel_n_queries --
    there is no more query resampling to make them diverge (unlike the
    superseded FIX 1, where the bootstrap used len(displaying queries)
    instead). A single census stratum, all strict, makes every replicate
    IDENTICAL to the point, so point == both CI bounds exactly.
    """
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 3, 3)]) for i in range(3)]
    strata_by_view = {'S': {'A': {'N_h': 3, 'n_h': 3}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=3)
    rows = [{'id': f'c{i}', 'grade': 'same_text'} for i in range(3)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    y = res['views']['S']['yield']['overall']
    assert y['point'] == 1.0
    assert y['ci95'] == [1.0, 1.0]


# ---------------------------------------------------------------------------
# Tamper evidence
# ---------------------------------------------------------------------------


def test_key_hash_mismatch_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    key_path = tmp_path / 'deck' / 'deck_key.json'
    tampered = key_list + [_card('c2', 'q1', 'r2', 's2',
                                 [_sel('S', 'A', 1.0, 1, 1)])]
    key_path.write_text(json.dumps(tampered, ensure_ascii=False),
                        encoding='utf-8')
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='key_hash'):
        sdd.score(deck_dir, verdicts)


def test_missing_deck_id_in_verdicts_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, _deck_id = _write_deck(tmp_path, key_list, views_manifest)
    p = tmp_path / 'verdicts.json'
    p.write_text(json.dumps({'verdicts': [{'id': 'c1',
                                          'grade': 'same_text'}]}),
                encoding='utf-8')
    with pytest.raises(SystemExit, match='no deck id declared'):
        sdd.score(deck_dir, str(p))


def test_wrong_deck_id_in_verdicts_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, 'not' + deck_id[3:],
                              [{'id': 'c1', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='REFUSING'):
        sdd.score(deck_dir, verdicts)


# ---------------------------------------------------------------------------
# Duplicate / orphan / unknown-grade rejection
# ---------------------------------------------------------------------------


def test_duplicate_verdict_id_is_fatal(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 5, 5)]) for i in range(5)]
    strata_by_view = {'S': {'A': {'N_h': 5, 'n_h': 5}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c1', 'grade': 'same_text'},
           {'id': 'c0', 'grade': 'unrelated'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='duplicate'):
        sdd.score(deck_dir, verdicts)


def test_orphan_verdict_id_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'DOES-NOT-EXIST', 'grade': 'same_text'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='absent from the deck key'):
        sdd.score(deck_dir, verdicts)


def test_unknown_grade_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'totally_not_real'}])
    with pytest.raises(SystemExit, match='prereg vocabulary'):
        sdd.score(deck_dir, verdicts)


def test_all_three_defects_counted_together_not_first_only(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 5, 5)]),
               _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 5, 5)])]
    strata_by_view = {'S': {'A': {'N_h': 5, 'n_h': 5}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c0', 'grade': 'same_text'},          # duplicate
           {'id': 'nope', 'grade': 'same_text'},         # orphan
           {'id': 'c1', 'grade': 'bogus_grade'}]         # unknown grade
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit) as ei:
        sdd.score(deck_dir, verdicts)
    msg = str(ei.value)
    assert '1 duplicate' in msg
    assert '1 verdict id(s) absent' in msg
    assert 'bogus_grade' in msg


def test_prereg_vocabulary_override(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    (tmp_path / 'deck' / 'prereg.json').write_text(
        json.dumps({'panel_n_queries': 10,
                   'grade_vocabulary': ['exact_match', 'unrelated']}),
        encoding='utf-8')
    verdicts_rejected = _write_verdicts(
        tmp_path, deck_id, [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='prereg vocabulary'):
        sdd.score(deck_dir, verdicts_rejected)
    verdicts_ok = _write_verdicts(
        tmp_path, deck_id, [{'id': 'c0', 'grade': 'exact_match'}],
        name='v2.json')
    res = sdd.score(deck_dir, verdicts_ok)
    assert res['grade_vocabulary_source'] == 'prereg.json'
    assert res['graded'] == 1


# ---------------------------------------------------------------------------
# --min-graded
# ---------------------------------------------------------------------------


def test_min_graded_fatal_when_under_threshold(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 10, 10)]) for i in range(10)]
    strata_by_view = {'S': {'A': {'N_h': 10, 'n_h': 10}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': f'c{i}', 'grade': 'same_text'} for i in range(5)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    assert res['graded'] == 5
    with pytest.raises(SystemExit, match='min-graded'):
        sdd.score(deck_dir, verdicts, min_graded=8)
    res2 = sdd.score(deck_dir, verdicts, min_graded=5)
    assert res2['graded'] == 5


# ---------------------------------------------------------------------------
# Structural validation: selections, pi_h, n_h/N_h, per-stratum caps,
# and the N_h-sums-to-N_display invariant the stratified bound depends on
# ---------------------------------------------------------------------------


def test_card_with_zero_selections_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='zero selections'):
        sdd.score(deck_dir, verdicts)


def test_pi_h_out_of_range_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.5, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='pi_h outside'):
        sdd.score(deck_dir, verdicts)


def test_n_h_greater_than_N_h_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 4, 5)])]
    strata_by_view = {'S': {'A': {'N_h': 4, 'n_h': 5}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='n_h > N_h'):
        sdd.score(deck_dir, verdicts)


def test_selection_count_exceeding_manifest_n_h_is_fatal(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 10, 1)]) for i in range(3)]
    # manifest declares n_h=2 for stratum A, but 3 selections actually
    # exist in deck_key.json -- must not exceed.
    strata_by_view = {'S': {'A': {'N_h': 10, 'n_h': 2}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(
        tmp_path, deck_id,
        [{'id': f'c{i}', 'grade': 'same_text'} for i in range(3)])
    with pytest.raises(SystemExit, match='exceeding manifest n_h'):
        sdd.score(deck_dir, verdicts)


def test_n_display_sum_mismatch_is_fatal(tmp_path):
    """The invariant the stratified bootstrap's max-numerator bound
    depends on: sum(stratum N_h) must equal N_display, exactly.
    """
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 999, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}  # sum(N_h)=1, N_display claims 999
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='do not sum to N_display'):
        sdd.score(deck_dir, verdicts)


def test_missing_panel_n_queries_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=0)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='panel_n_queries'):
        sdd.score(deck_dir, verdicts)


# ---------------------------------------------------------------------------
# Zero-graded view -> INSUFFICIENT, never a fabricated 0/1
# ---------------------------------------------------------------------------


def test_zero_graded_view_reports_insufficient(tmp_path):
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)]),
        _card('c2', 'q1', 'r2', 's2', [_sel('C5', 'A', 1.0, 1, 1)]),
    ]
    strata_by_view = {
        'S': {'A': {'N_h': 1, 'n_h': 1}},
        'C5': {'A': {'N_h': 1, 'n_h': 1}},
    }
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=1)
    # Only c1 (view S) is graded; c2 (view C5) is left ungraded.
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['precision']['overall']['point'] == 1.0
    assert res['views']['C5']['precision']['overall'] == sdd.INSUFFICIENT
    assert res['views']['C5']['yield']['overall'] == sdd.INSUFFICIENT
    assert (res['views']['C5']['duplicate_photo_rate']['overall']
           == sdd.INSUFFICIENT)


def test_zero_graded_non_source_column_is_insufficient_even_if_view_ok(tmp_path):
    """D7: a view can be fine overall but INSUFFICIENT in its non-source
    column specifically -- never a fabricated 0/1 for that column.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)],
             is_source=True),
    ]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=1)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['precision']['overall']['point'] == 1.0
    assert res['views']['S']['precision']['non_source'] == sdd.INSUFFICIENT


# ---------------------------------------------------------------------------
# multi_stratum_queries -- the residual within-query correlation exposure
# ---------------------------------------------------------------------------


def test_multi_stratum_queries_counts_queries_spanning_2plus_strata(tmp_path):
    key_list = [
        # q1 has cards in BOTH strata A and B -> counts.
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 3, 3)]),
        _card('c2', 'q1', 'r2', 's2', [_sel('S', 'B', 1.0, 3, 3)]),
        # q2 has a card only in stratum A -> does not count.
        _card('c3', 'q2', 'r3', 's3', [_sel('S', 'A', 1.0, 3, 3)]),
    ]
    strata_by_view = {'S': {'A': {'N_h': 3, 'n_h': 2},
                            'B': {'N_h': 3, 'n_h': 1}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=2)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['multi_stratum_queries'] == 1


# ---------------------------------------------------------------------------
# The stratified bootstrap: bounded by construction, determinism, census
# ---------------------------------------------------------------------------


def test_old_query_clustered_ratio_bootstrap_would_escape_but_stratified_does_not():
    """Reproduces the SECOND integration smoke's diagnosis directly: the
    real deck's cited stratum weight ratio (pi_h = 4/74, weight 18.5) does
    not decompose by query. If 3 of a stratum's 4 sampled cards happen to
    belong to one query (all graded strict), that query's OWN naive
    numerator is 3*18.5=55.5 against a plausible local display count of
    5 -- the SUPERSEDED per-query ratio bootstrap (reconstructed above
    only for this comparison) escapes far above 1 on this realistic,
    unpadded weight ratio. The CURRENT stratified_bootstrap, drawing from
    the stratum as a whole rather than attributing weight to individual
    queries, does not (confirmed by direct simulation before writing this
    assertion: point=1.0 for both, but old ci95=[1.85, 11.1] while new
    ci95=[1.0, 1.0]).
    """
    numer_by_query = {'fgp:1888': 3 * 18.5, 'other_q': 1 * 18.5}
    denom_by_query = {'fgp:1888': 5.0, 'other_q': 10.0}
    old = _old_query_clustered_ratio_bootstrap(
        numer_by_query, denom_by_query, point_denom=74.0, resamples=2000,
        seed=3)
    assert old['ci95'][1] > 1.0, (
        'the superseded per-query ratio bootstrap should reproduce the '
        'escaped-CI symptom on this realistic weight ratio -- if it no '
        'longer does, this regression test has gone stale')

    strata = {'h': {'weight': 18.5, 'n_h': 4, 'graded': [1, 1, 1, 1]}}
    new = sdd.stratified_bootstrap(strata, denom=74.0, resamples=2000,
                                   seed=3, unit_interval=True)
    assert new['ci95'] == [1.0, 1.0]


def test_max_numerator_equals_n_display_property():
    """Two strata, uneven weights, NON-census (n_h < N_h), MIXED (not
    all-1) graded outcomes -- over many resamples, the replicate total
    never exceeds N_display, and the point is exactly the algebraic
    prediction. Confirmed by direct simulation before writing this
    assertion (point=0.6957, every one of 5000 replicates within [0,1]).
    """
    strata = {
        'h1': {'weight': 5.0, 'n_h': 3, 'graded': [1, 0, 1]},   # N_h1=15
        'h2': {'weight': 2.0, 'n_h': 4, 'graded': [1, 1, 0, 1]},  # N_h2=8
    }
    N_display = 15 + 8
    boot = sdd.stratified_bootstrap(strata, denom=N_display, resamples=5000,
                                    seed=11, unit_interval=True)
    # point_total = 5.0*(1+0+1) + 2.0*(1+1+0+1) = 10 + 6 = 16; /23 = 0.6957
    assert boot['point'] == round(16 / 23, 4)
    assert 0.0 <= boot['ci95'][0] <= boot['ci95'][1] <= 1.0


def test_census_singleton_stratum_contributes_zero_variance():
    """n_h == N_h == 1 (a fully-censused, SINGLE-item stratum): there is
    only one card to ever draw, so every replicate draws it exactly once,
    deterministically -- zero bootstrap variance from this stratum.

    This is NOT a general property of "n_h == N_h" strata: a census
    stratum with n_h > 1 and mixed 0/1 outcomes still varies between
    replicates (which specific units get drawn how many times still
    differs), even though every unit was sampled. The guarantee the
    stratified bootstrap's fix actually relies on is only that the
    replicate MAXIMUM never exceeds N_display (see
    test_max_numerator_equals_n_display_property) -- not that every
    census stratum is deterministic.
    """
    strata = {'h': {'weight': 1.0, 'n_h': 1, 'graded': [1]}}
    boot = sdd.stratified_bootstrap(strata, denom=1.0, resamples=2000,
                                    seed=5, unit_interval=True)
    assert boot['point'] == 1.0
    assert boot['ci95'] == [1.0, 1.0]


def test_stratified_bootstrap_resamples_within_stratum_independently():
    """Stratum A is a census singleton (weight 6.0, ALWAYS contributes
    6.0). Stratum B draws 2 with replacement from [1, 0] (weight 2.0),
    giving a sum of 0, 2, or 4 with probabilities 0.25/0.5/0.25. Total is
    therefore {6, 8, 10} over denom=10 -> {0.6, 0.8, 1.0}, confirmed by
    direct simulation across several seeds before writing this assertion.
    A bug that pooled every stratum's graded cards together before
    resampling (instead of resampling each stratum independently at its
    OWN weight and OWN n_h) would not reliably reproduce this exact,
    seed-stable three-value split.
    """
    strata = {
        'A': {'weight': 6.0, 'n_h': 1, 'graded': [1]},
        'B': {'weight': 2.0, 'n_h': 2, 'graded': [1, 0]},
    }
    boot = sdd.stratified_bootstrap(strata, denom=10.0, resamples=1000,
                                    seed=7, unit_interval=True)
    assert boot['point'] == 0.8
    assert boot['ci95'] == [0.6, 1.0]


def test_stratified_bootstrap_asserts_when_ci_escapes_unit_interval():
    """A deliberately adversarial `denom` (far smaller than the true
    max=50) -- must trip the INTERNAL BUG assertion rather than silently
    clip the escaped bound away.
    """
    strata = {'h': {'weight': 10.0, 'n_h': 5, 'graded': [1, 1, 1, 1, 1]}}
    with pytest.raises(SystemExit, match='INTERNAL BUG'):
        sdd.stratified_bootstrap(strata, denom=1.0, resamples=200, seed=1,
                                 unit_interval=True)


def test_bootstrap_determinism_same_seed_same_bytes(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A' if i % 2 else 'B',
                           1.0 if i % 2 else 0.5, 10, 5)])
               for i in range(10)]
    strata_by_view = {'S': {'A': {'N_h': 10, 'n_h': 5},
                            'B': {'N_h': 10, 'n_h': 5}}}
    views_manifest = _views_manifest(strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=10)
    rows = [{'id': f'c{i}', 'grade': 'same_text' if i % 3 else 'unrelated'}
           for i in range(10)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    r1 = sdd.score(deck_dir, verdicts, cluster_resamples=300)
    r2 = sdd.score(deck_dir, verdicts, cluster_resamples=300)
    assert json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)

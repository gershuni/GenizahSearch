# -*- coding: utf-8 -*-
"""Tests for `scripts/score_display_deck.py`, the IPW scorer for the
stratified display-policy grading deck (external review, Codex 2026-08-21).

Every fixture is FABRICATED: opaque card/query/record/sys ids, never real
research content. deck_key.json / deck_manifest.json / prereg.json are
built by hand with the same hashing scheme scripts/score_grading_deck.py
and scripts/build_grading_deck.py use (cross-checked directly against a
real scripts.score_grading_deck.sha() call below), so the manifest's
key_hash always matches unless a test deliberately corrupts it.

Where the arithmetic can be checked by hand, the test asserts the EXACT
expected point value (not just "is a number" or "is roughly right") --
see test_hand_computed_ipw_precision_and_yield for the worked case the
module docstring's IPW definition maps onto directly.
"""
from __future__ import annotations

import json

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
    """Two strata in view S: pi=1.0 (certainty) and pi=0.5.

    Strict weighted sum = 1/1.0 (c1, same_text) + 0 (c2, unrelated)
    + 1/0.5 (c3, same_text) + 1/0.5 (c4, paraphrase) = 1 + 0 + 2 + 2 = 5.0.
    N_display=10 -> P_hat = 5/10 = 0.500. panel_n_queries=4 ->
    Y_hat = 5/4 = 1.250.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 4, 2)]),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 1.0, 4, 2)]),
        _card('c3', 'q3', 'r3', 's3', [_sel('S', 'B', 0.5, 8, 2)]),
        _card('c4', 'q4', 'r4', 's4', [_sel('S', 'B', 0.5, 8, 2)]),
    ]
    views_manifest = {'S': {'N_display': 10, 'strata': {
        'A': {'N_h': 4, 'n_h': 2}, 'B': {'N_h': 8, 'n_h': 2}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=4)
    rows = [{'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'unrelated'},
           {'id': 'c3', 'grade': 'same_text'},
           {'id': 'c4', 'grade': 'paraphrase'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    s = res['views']['S']
    assert s['precision']['overall']['point'] == 0.5
    assert s['yield']['overall']['point'] == 1.25


def test_card_selected_in_two_views_contributes_to_both(tmp_path):
    """c1 is drawn into BOTH S (pi=1.0) and W (pi=0.5), one grade.

    S: strict sum = 1/1.0 = 1.0, N_display=1 -> P_hat = 1.0.
    W: strict sum = 1/0.5 = 2.0, N_display=4 -> P_hat = 0.5.
    Same card, same grade, two independent view-scoped contributions.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [
            _sel('S', 'A', 1.0, 1, 1),
            _sel('W', 'A', 0.5, 2, 1),
        ]),
    ]
    views_manifest = {
        'S': {'N_display': 1, 'strata': {'A': {'N_h': 1, 'n_h': 1}}},
        'W': {'N_display': 4, 'strata': {'A': {'N_h': 2, 'n_h': 1}}},
    }
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=1)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['precision']['overall']['point'] == 1.0
    assert res['views']['W']['precision']['overall']['point'] == 0.5


def test_duplicate_photo_lowers_strict_but_reported_separately(tmp_path):
    """5 cards, pi=1.0, view S: 4 strict + 1 duplicate_photo.

    Strict sum = 4.0 -> P_hat = 4/5 = 0.8 (duplicate_photo contributes
    NOTHING to the strict numerator, but it still occupies one of the 5
    N_display slots). duplicate_photo weighted sum = 1.0 -> rate = 1/5
    = 0.2, reported as its OWN number, not folded into strict.
    """
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 5, 5)]) for i in range(5)]
    views_manifest = {'S': {'N_display': 5, 'strata': {
        'A': {'N_h': 5, 'n_h': 5}}}}
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

    c1 (is_source=True) and c2 (is_source=False), both pi=1.0, both
    strict, N_display=4 (a fixed population constant unrelated to how many
    of these two cards are source vs not). Overall = (1+1)/4 = 0.5.
    Non-source = 1/4 = 0.25 (only c2's weight), SAME denominator 4 -- not
    2 (which a per-column-denominator reading would require and which the
    manifest schema does not supply).
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 4, 2)],
             is_source=True),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 1.0, 4, 2)],
             is_source=False),
    ]
    views_manifest = {'S': {'N_display': 4, 'strata': {
        'A': {'N_h': 4, 'n_h': 2}}}}
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
    views_manifest = {'S': {'N_display': 3, 'strata': {
        'A': {'N_h': 2, 'n_h': 2}, 'B': {'N_h': 1, 'n_h': 1}}}}
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


# ---------------------------------------------------------------------------
# Tamper evidence
# ---------------------------------------------------------------------------


def test_key_hash_mismatch_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
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
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, _deck_id = _write_deck(tmp_path, key_list, views_manifest)
    p = tmp_path / 'verdicts.json'
    p.write_text(json.dumps({'verdicts': [{'id': 'c1',
                                          'grade': 'same_text'}]}),
                encoding='utf-8')
    with pytest.raises(SystemExit, match='no deck id declared'):
        sdd.score(deck_dir, str(p))


def test_wrong_deck_id_in_verdicts_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
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
    views_manifest = {'S': {'N_display': 5, 'strata': {
        'A': {'N_h': 5, 'n_h': 5}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c1', 'grade': 'same_text'},
           {'id': 'c0', 'grade': 'unrelated'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='duplicate'):
        sdd.score(deck_dir, verdicts)


def test_orphan_verdict_id_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'DOES-NOT-EXIST', 'grade': 'same_text'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='absent from the deck key'):
        sdd.score(deck_dir, verdicts)


def test_unknown_grade_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'totally_not_real'}])
    with pytest.raises(SystemExit, match='prereg vocabulary'):
        sdd.score(deck_dir, verdicts)


def test_all_three_defects_counted_together_not_first_only(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 5, 5)]),
               _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 5, 5)])]
    views_manifest = {'S': {'N_display': 5, 'strata': {
        'A': {'N_h': 5, 'n_h': 5}}}}
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
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
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
    views_manifest = {'S': {'N_display': 10, 'strata': {
        'A': {'N_h': 10, 'n_h': 10}}}}
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
# Structural validation: selections, pi_h, n_h/N_h, per-stratum caps
# ---------------------------------------------------------------------------


def test_card_with_zero_selections_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='zero selections'):
        sdd.score(deck_dir, verdicts)


def test_pi_h_out_of_range_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.5, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='pi_h outside'):
        sdd.score(deck_dir, verdicts)


def test_n_h_greater_than_N_h_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 4, 5)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 4, 'n_h': 5}}}}
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
    views_manifest = {'S': {'N_display': 3, 'strata': {
        'A': {'N_h': 10, 'n_h': 2}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(
        tmp_path, deck_id,
        [{'id': f'c{i}', 'grade': 'same_text'} for i in range(3)])
    with pytest.raises(SystemExit, match='exceeding manifest n_h'):
        sdd.score(deck_dir, verdicts)


def test_missing_panel_n_queries_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
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
    views_manifest = {
        'S': {'N_display': 1, 'strata': {'A': {'N_h': 1, 'n_h': 1}}},
        'C5': {'N_display': 1, 'strata': {'A': {'N_h': 1, 'n_h': 1}}},
    }
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
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=1)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['precision']['overall']['point'] == 1.0
    assert res['views']['S']['precision']['non_source'] == sdd.INSUFFICIENT


# ---------------------------------------------------------------------------
# Bootstrap: determinism, and resampling QUERIES not cards
# ---------------------------------------------------------------------------


def test_bootstrap_determinism_same_seed_same_bytes(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A' if i % 2 else 'B',
                           1.0 if i % 2 else 0.5, 10, 5)])
               for i in range(10)]
    views_manifest = {'S': {'N_display': 10, 'strata': {
        'A': {'N_h': 10, 'n_h': 5}, 'B': {'N_h': 10, 'n_h': 5}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=10)
    rows = [{'id': f'c{i}', 'grade': 'same_text' if i % 3 else 'unrelated'}
           for i in range(10)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    r1 = sdd.score(deck_dir, verdicts, cluster_resamples=300)
    r2 = sdd.score(deck_dir, verdicts, cluster_resamples=300)
    assert json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)


def test_bootstrap_resamples_queries_not_cards():
    """Two query-groups, each contributing SIX weighted units: q1 is
    all-strict (weight 1.0 x6), q2 is all-miss (weight 0.0 x6). Resampling
    at QUERY grain always draws exactly len(keys)=2 group-totals (each
    total is 6.0 or 0.0), so the resampled sum can ONLY be 0.0, 6.0, or
    12.0 over denom=12.0 -> the point value is one of exactly {0.0, 0.5,
    1.0} -- a hard combinatorial fact, true for ANY number of resamples,
    not a statistical coincidence of this particular seed.

    Resampling at CARD grain (12 individual Bernoulli(0.5) draws instead
    of 2 group draws) would spread mass over a Binomial(12, 0.5)-like
    distribution whose 2.5/97.5 percentiles land near 0.17-0.25 and
    0.75-0.83 (confirmed by direct simulation across several seeds before
    writing this assertion) -- values IMPOSSIBLE under correct query-grain
    resampling. If ci95 ever reports such a value, the resampling grain is
    wrong.
    """
    contribs = {'q1': [1.0] * 6, 'q2': [0.0] * 6}
    boot = sdd.weighted_cluster_bootstrap(contribs, denom=12.0,
                                          resamples=500, seed=7)
    assert boot['n_groups'] == 2
    assert boot['point'] == 0.5
    lo, hi = boot['ci95']
    assert lo in (0.0, 0.5, 1.0)
    assert hi in (0.0, 0.5, 1.0)

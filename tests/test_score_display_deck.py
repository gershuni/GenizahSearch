# -*- coding: utf-8 -*-
"""Tests for `scripts/score_display_deck.py`, the IPW scorer for the
stratified display-policy grading deck (external review, Codex 2026-08-21;
ratio-bootstrap fix, 2026-08-21 integration smoke).

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

`display_counts_by_query` fixtures: most tests only care about the OTHER
guards, so `_valid_views_manifest` auto-derives a self-consistent
display_counts_by_query from the natural (view, query) selection counts
already in the fixture's key_list, padding with unsampled/ungraded
"background" queries via `extra_display_counts_by_view` only where a test
needs N_display to be LARGER than what the sampled cards alone display
(e.g. the hand-computed base case, where N_display=10 but only 4 of the
view's queries were ever sampled into the deck). The four
`test_display_counts_*_is_fatal` tests hand-build a raw manifest dict
instead, to inject one specific defect in isolation.
"""
from __future__ import annotations

import collections
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


def _valid_views_manifest(key_list, strata_by_view,
                          extra_display_counts_by_view=None):
    """Auto-derive a self-consistent {view: {N_display, strata,
    display_counts_by_query}} manifest fragment: display_counts_by_query
    starts from the NATURAL per-(view, query) selection counts already
    present in key_list, then adds any padding entries named in
    `extra_display_counts_by_view` (view -> {query_id: count}) --
    representing display slots the deck shows but never drew into the
    grading sample. N_display is always the exact sum, by construction.
    """
    extra_display_counts_by_view = extra_display_counts_by_view or {}
    natural: dict = collections.defaultdict(lambda: collections.defaultdict(int))
    for card in key_list:
        for sel in card.get('selections') or []:
            natural[sel['view']][card['query_id']] += 1

    out = {}
    for view, strata in strata_by_view.items():
        dcbq = dict(natural.get(view, {}))
        for qid, extra in extra_display_counts_by_view.get(view, {}).items():
            dcbq[qid] = dcbq.get(qid, 0) + extra
        out[view] = {
            'N_display': sum(dcbq.values()),
            'strata': strata,
            'display_counts_by_query': dcbq,
        }
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
    N_display=10 (4 sampled queries + 4 padding/background queries this
    view displays but never drew into the deck) -> P_hat = 5/10 = 0.500.
    panel_n_queries=4 -> Y_hat = 5/4 = 1.250 (unaffected by the padding,
    since the point estimate's denominator is the fixed panel size, not
    the view's own displaying-query count).

    q3 and q4 (the pi=0.5 cards) are each padded to a local display count
    of 2, matching their own 1/pi_h=2.0 weight exactly (ratio 1.0, not
    exceeding) -- pi_h is a STRATUM-level rate pooled across every query
    sharing that stratum, so a query whose OWN local display count is
    smaller than its own card's weight would give that query alone a
    per-query ratio > 1, which the ratio bootstrap's unit-interval
    assertion (correctly) refuses to let through. Real decks avoid this
    because a card's pi_h is drawn from a stratum spanning many queries,
    each usually displaying several cards of its own; this fixture pads
    to keep the arithmetic hand-checkable without also being flagged.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 4, 2)]),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 1.0, 4, 2)]),
        _card('c3', 'q3', 'r3', 's3', [_sel('S', 'B', 0.5, 8, 2)]),
        _card('c4', 'q4', 'r4', 's4', [_sel('S', 'B', 0.5, 8, 2)]),
    ]
    strata_by_view = {'S': {'A': {'N_h': 4, 'n_h': 2},
                            'B': {'N_h': 8, 'n_h': 2}}}
    extra = {'S': {'q3': 1, 'q4': 1, 'qx5': 1, 'qx6': 1, 'qx7': 1, 'qx8': 1}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view, extra)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=4)
    rows = [{'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'unrelated'},
           {'id': 'c3', 'grade': 'same_text'},
           {'id': 'c4', 'grade': 'paraphrase'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    s = res['views']['S']
    assert s['n_display'] == 10
    assert s['precision']['overall']['point'] == 0.5
    assert s['yield']['overall']['point'] == 1.25


def test_card_selected_in_two_views_contributes_to_both(tmp_path):
    """c1 is drawn into BOTH S (pi=1.0) and W (pi=0.5), one grade.

    S: strict sum = 1/1.0 = 1.0, N_display=1 -> P_hat = 1.0.
    W: strict sum = 1/0.5 = 2.0, N_display=4 (q1 padded to a local count
    of 2, matching its own weight exactly, plus 2 padding queries) ->
    P_hat = 0.5. Same card, same grade, two independent view-scoped
    contributions.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [
            _sel('S', 'A', 1.0, 1, 1),
            _sel('W', 'A', 0.5, 2, 1),
        ]),
    ]
    strata_by_view = {
        'S': {'A': {'N_h': 1, 'n_h': 1}},
        'W': {'A': {'N_h': 2, 'n_h': 1}},
    }
    extra = {'W': {'q1': 1, 'qx2': 1, 'qx3': 1}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view, extra)
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
    strata_by_view = {'S': {'A': {'N_h': 5, 'n_h': 5}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    strict, N_display=4 (2 sampled + 2 padding queries -- a fixed
    population constant unrelated to how many of the SAMPLED cards are
    source vs not). Overall = (1+1)/4 = 0.5. Non-source = 1/4 = 0.25
    (only c2's weight), SAME denominator 4 -- not 2 (which a
    per-column-denominator reading would require and which the manifest
    schema does not supply).
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 4, 2)],
             is_source=True),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 1.0, 4, 2)],
             is_source=False),
    ]
    strata_by_view = {'S': {'A': {'N_h': 4, 'n_h': 2}}}
    extra = {'S': {'qx3': 1, 'qx4': 1}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view, extra)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'DOES-NOT-EXIST', 'grade': 'same_text'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='absent from the deck key'):
        sdd.score(deck_dir, verdicts)


def test_unknown_grade_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'totally_not_real'}])
    with pytest.raises(SystemExit, match='prereg vocabulary'):
        sdd.score(deck_dir, verdicts)


def test_all_three_defects_counted_together_not_first_only(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 5, 5)]),
               _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 5, 5)])]
    strata_by_view = {'S': {'A': {'N_h': 5, 'n_h': 5}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    extra = {'S': {'q0': 1}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view, extra)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='zero selections'):
        sdd.score(deck_dir, verdicts)


def test_pi_h_out_of_range_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.5, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='pi_h outside'):
        sdd.score(deck_dir, verdicts)


def test_n_h_greater_than_N_h_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 4, 5)])]
    strata_by_view = {'S': {'A': {'N_h': 4, 'n_h': 5}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(
        tmp_path, deck_id,
        [{'id': f'c{i}', 'grade': 'same_text'} for i in range(3)])
    with pytest.raises(SystemExit, match='exceeding manifest n_h'):
        sdd.score(deck_dir, verdicts)


def test_missing_panel_n_queries_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    strata_by_view = {'S': {'A': {'N_h': 1, 'n_h': 1}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=0)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='panel_n_queries'):
        sdd.score(deck_dir, verdicts)


# ---------------------------------------------------------------------------
# display_counts_by_query validation (added with the ratio-bootstrap fix)
# ---------------------------------------------------------------------------


def test_display_counts_missing_field_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    # deliberately no display_counts_by_query at all
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='missing or empty'):
        sdd.score(deck_dir, verdicts)


def test_display_counts_non_positive_value_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}},
        'display_counts_by_query': {'q0': 0}}}
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='non-positive'):
        sdd.score(deck_dir, verdicts)


def test_display_counts_sum_mismatch_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 5, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}},
        'display_counts_by_query': {'q0': 1}}}  # sums to 1, N_display=5
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='does not sum to N_display'):
        sdd.score(deck_dir, verdicts)


def test_display_counts_orphan_query_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', 's0', [_sel('S', 'A', 1.0, 1, 1)])]
    views_manifest = {'S': {'N_display': 1, 'strata': {
        'A': {'N_h': 1, 'n_h': 1}},
        'display_counts_by_query': {'OTHER_QUERY': 1}}}  # q0 missing
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c0', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='absent from display_counts_by_query'):
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
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
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=1)
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    res = sdd.score(deck_dir, verdicts)
    assert res['views']['S']['precision']['overall']['point'] == 1.0
    assert res['views']['S']['precision']['non_source'] == sdd.INSUFFICIENT


# ---------------------------------------------------------------------------
# The ratio-bootstrap fix: precision/duplicate_photo_rate never escape [0,1]
# ---------------------------------------------------------------------------


def test_fixed_denominator_bootstrap_would_escape_one_the_bug_this_fixes():
    """Direct reproduction of the coordinator's reported defect, and proof
    the fix resolves it on the IDENTICAL data.

    Three queries, uneven per-query weights (the realistic scenario): q1
    displays 10 cards total but only 1 was sampled at pi_h=0.1 (weight
    10.0, graded strict) -- its own per-query ratio is exactly 10/10=1.0,
    not inflated. q2 displays 1 card, census-sampled (pi_h=1.0, weight
    1.0, strict) -- ratio 1/1=1.0. q3 displays 1 card that was never
    graded -- ratio 0/1=0.0. Every INDIVIDUAL query's own ratio is <= 1,
    so mathematically the TRUE aggregate ratio can never exceed 1 no
    matter how any subset of queries is combined or repeated.

    BEFORE (the bug): resampling the numerator only against N_display=12
    held fixed for every replicate -- exactly what precision's bootstrap
    used to do -- can still push a replicate that draws q1 repeatedly
    to e.g. (10+10+10)/12 = 2.5, because the fixed denominator does not
    grow to match. This is reproduced directly via
    weighted_cluster_bootstrap(point_denom=bootstrap_denom=N_display).

    AFTER (the fix): weighted_ratio_cluster_bootstrap resamples the
    matching per-query denominator alongside the numerator, so a replicate
    that draws q1 three times sums 30/30=1.0, never more -- the CI stays
    inside [0, 1] on this SAME data.
    """
    numer = {'q1': 10.0, 'q2': 1.0, 'q3': 0.0}
    denom_by_query = {'q1': 10.0, 'q2': 1.0, 'q3': 1.0}
    N_display = 12.0

    old = sdd.weighted_cluster_bootstrap(numer, point_denom=N_display,
                                         bootstrap_denom=N_display,
                                         resamples=1000, seed=3)
    assert old['ci95'][1] > 1.0, (
        'the fixed-denominator bootstrap should reproduce the escaped-CI '
        'bug on this data -- if it no longer does, this regression test '
        'has gone stale and no longer proves anything')

    new = sdd.weighted_ratio_cluster_bootstrap(
        numer, denom_by_query, N_display, resamples=1000, seed=3,
        unit_interval=True)
    assert 0.0 <= new['ci95'][0] <= 1.0
    assert 0.0 <= new['ci95'][1] <= 1.0


def test_precision_ci_upper_bound_is_exactly_one_for_full_census_all_strict_deck(tmp_path):
    """Coordinator's explicit check: a full-census (pi_h=1.0), all-strict
    deck must report an upper bound of EXACTLY 1.0, never above.
    """
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A', 1.0, 3, 3)]) for i in range(3)]
    strata_by_view = {'S': {'A': {'N_h': 3, 'n_h': 3}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=3)
    rows = [{'id': f'c{i}', 'grade': 'same_text'} for i in range(3)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    p = res['views']['S']['precision']['overall']
    assert p['point'] == 1.0
    assert p['ci95'] == [1.0, 1.0]


def test_ratio_cluster_bootstrap_asserts_when_ci_escapes_unit_interval():
    """A genuinely adversarial (fabricated, not naturally occurring) input
    where q1's own ratio is 10.0/1 = 10 -- must trip the INTERNAL BUG
    assertion rather than silently clip the escaped bound away.
    """
    numer = {'q1': 10.0, 'q2': 0.0}
    denom_by_query = {'q1': 1.0, 'q2': 1.0}
    with pytest.raises(SystemExit, match='INTERNAL BUG'):
        sdd.weighted_ratio_cluster_bootstrap(
            numer, denom_by_query, point_denom=2.0, resamples=200, seed=1,
            unit_interval=True)


def test_ratio_cluster_bootstrap_mismatched_query_universes_is_fatal():
    with pytest.raises(SystemExit, match='INTERNAL BUG'):
        sdd.weighted_ratio_cluster_bootstrap(
            {'q1': 1.0}, {'q1': 1.0, 'q2': 1.0}, point_denom=1.0,
            resamples=10, seed=1)


# ---------------------------------------------------------------------------
# Yield: point stays on panel_n_queries, bootstrap uses the displaying-
# query count instead -- a deliberate, documented asymmetry (D1)
# ---------------------------------------------------------------------------


def test_yield_bootstrap_uses_displaying_query_count_not_panel_n_queries(tmp_path):
    """panel_n_queries=100 (the pre-registered panel), but view S only
    displays something for 2 queries. Point = 2.0/100 = 0.02 (fixed panel
    scale). Every possible 2-of-2 resample sums to exactly 2.0 (both
    queries contribute 1.0 each), and the bootstrap divides by
    len(displaying queries)=2, not 100 -- so ci95 is [1.0, 1.0], utterly
    off-scale from the 0.02 point. That gap IS the point of this test:
    it is what the module docstring's D1 explains, not a bug.
    """
    key_list = [
        _card('c1', 'q1', 'r1', 's1', [_sel('S', 'A', 1.0, 2, 2)]),
        _card('c2', 'q2', 'r2', 's2', [_sel('S', 'A', 1.0, 2, 2)]),
    ]
    strata_by_view = {'S': {'A': {'N_h': 2, 'n_h': 2}}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=100)
    rows = [{'id': 'c1', 'grade': 'same_text'},
           {'id': 'c2', 'grade': 'same_text'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sdd.score(deck_dir, verdicts)
    y = res['views']['S']['yield']['overall']
    assert y['point'] == 0.02
    assert y['ci95'] == [1.0, 1.0]
    assert y['n_groups'] == 2


# ---------------------------------------------------------------------------
# Bootstrap: determinism, and resampling QUERIES not cards
# ---------------------------------------------------------------------------


def test_bootstrap_determinism_same_seed_same_bytes(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', f's{i}',
                      [_sel('S', 'A' if i % 2 else 'B',
                           1.0 if i % 2 else 0.5, 10, 5)])
               for i in range(10)]
    strata_by_view = {'S': {'A': {'N_h': 10, 'n_h': 5},
                            'B': {'N_h': 10, 'n_h': 5}}}
    # q0,q2,q4,q6,q8 carry the pi=0.5 cards (weight 2.0); pad each of their
    # own local display counts to 2 so no single query's own ratio exceeds
    # 1 -- see test_hand_computed_ipw_precision_and_yield's docstring for
    # why an unpadded pi=0.5 query with a local count of 1 is flagged.
    extra = {'S': {f'q{i}': 1 for i in range(0, 10, 2)}}
    views_manifest = _valid_views_manifest(key_list, strata_by_view, extra)
    deck_dir, deck_id = _write_deck(tmp_path, key_list, views_manifest,
                                    panel_n_queries=10)
    rows = [{'id': f'c{i}', 'grade': 'same_text' if i % 3 else 'unrelated'}
           for i in range(10)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    r1 = sdd.score(deck_dir, verdicts, cluster_resamples=300)
    r2 = sdd.score(deck_dir, verdicts, cluster_resamples=300)
    assert json.dumps(r1, sort_keys=True) == json.dumps(r2, sort_keys=True)


def test_bootstrap_resamples_queries_not_cards():
    """Two query-groups, q1 (weight 6.0) and q2 (weight 0.0), each drawn
    with 50% probability per slot. QUERY-grain resampling always draws
    exactly len(keys)=2 group-totals, so both draws land on q1 (P=0.25,
    sum=12.0) or both on q2 (P=0.25, sum=0.0) often enough, over 500
    resamples, that the 2.5th/97.5th percentiles are PINNED to the exact
    extremes 0.0 and 1.0 (over bootstrap_denom=12.0) -- not just "some
    value in {0, 0.5, 1}", which a WEAKER, coincidentally-passing
    assertion could not tell apart from a resampler that draws the wrong
    NUMBER of query ids per replicate (e.g. 1 draw instead of len(keys)=2
    would cap the achievable sum at 6.0, giving hi=0.5, never 1.0 --
    confirmed by direct simulation across several seeds before writing
    this assertion, precisely so that regression is caught).
    """
    numer = {'q1': sum([1.0] * 6), 'q2': sum([0.0] * 6)}
    boot = sdd.weighted_cluster_bootstrap(numer, point_denom=12.0,
                                          bootstrap_denom=12.0,
                                          resamples=500, seed=7)
    assert boot['n_groups'] == 2
    assert boot['point'] == 0.5
    assert boot['ci95'] == [0.0, 1.0]


def test_ratio_bootstrap_resamples_queries_not_cards():
    """Proves the numerator and denominator are drawn from the SAME query
    per resampled slot -- the core of the 2026-08-21 fix -- using
    ASYMMETRIC per-query denominators (q1 denom=6.0, q2 denom=2.0) so a
    decoupled implementation (drawing an independent query id for the
    denominator side) is detectable: it can pair q1's numerator (6.0) with
    q2's denominator (2.0) and vice versa, reaching ratios like 3.0 that a
    coupled draw never can (confirmed by direct simulation across several
    seeds before writing this assertion: correct pins ci95 to exactly
    [0.0, 1.0]; a decoupled implementation reaches [0.0, 3.0] instead).
    """
    numer = {'q1': 6.0, 'q2': 0.0}
    denom_by_query = {'q1': 6.0, 'q2': 2.0}
    boot = sdd.weighted_ratio_cluster_bootstrap(
        numer, denom_by_query, point_denom=8.0, resamples=500, seed=7,
        unit_interval=True)
    assert boot['n_groups'] == 2
    assert boot['point'] == 0.75
    assert boot['ci95'] == [0.0, 1.0]

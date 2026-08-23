# -*- coding: utf-8 -*-
"""Tests for `scripts/score_grading_deck.py` (Codex hardening: tamper
evidence, duplicate/orphan/unknown-grade rejection, --min-graded, and a
query-clustered bootstrap CI alongside the existing Wilson interval).

Every fixture is FABRICATED: opaque card/query/record ids, never real
research content. deck_key.json / deck_manifest.json are built by hand with
the same hashing scheme scripts/build_grading_deck.py uses (cross-checked
directly against a real build_grading_deck.sha() call below), so the
manifest's key_hash always matches unless a test deliberately corrupts it.
"""
from __future__ import annotations

import json

import pytest

from scripts import build_grading_deck as bgd
from scripts import score_grading_deck as sgd


def _write_deck(tmp_path, key_list, n_cards=None):
    """Write a self-consistent deck_key.json + deck_manifest.json.

    Returns (deck_dir, deck_id) where deck_id is the manifest's cards_hash
    prefix a verdicts export must declare to be accepted.
    """
    deck_dir = tmp_path / 'deck'
    deck_dir.mkdir(exist_ok=True)
    key_path = deck_dir / 'deck_key.json'
    key_path.write_text(json.dumps(key_list, ensure_ascii=False),
                        encoding='utf-8')
    cards_hash = 'ab' * 32  # fabricated 64-hex-char placeholder
    manifest = {
        'prereg_id': 'fake-prereg',
        'n_cards': n_cards if n_cards is not None else len(key_list),
        'n_queries': len({k['query_id'] for k in key_list}),
        'cards_hash': cards_hash,
        'key_hash': sgd.sha(key_list),
    }
    (deck_dir / 'deck_manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False), encoding='utf-8')
    return str(deck_dir), cards_hash[:16]


def _write_verdicts(tmp_path, deck_id, rows, name='verdicts.json'):
    p = tmp_path / name
    p.write_text(json.dumps({'deck': deck_id, 'verdicts': rows},
                            ensure_ascii=False), encoding='utf-8')
    return str(p)


def _card(cid, qid, rid, methods, is_source=None):
    entry = {'id': cid, 'query_id': qid, 'record_id': rid,
             'methods': sorted(methods)}
    if is_source is not None:
        entry['is_source'] = is_source
    return entry


# ---------------------------------------------------------------------------
# sha() cross-check: score_grading_deck.sha must be BYTE-IDENTICAL to
# build_grading_deck.sha, since the scorer recomputes the manifest's
# key_hash independently (never by importing the builder's heavy deps).
# ---------------------------------------------------------------------------


def test_sha_matches_build_grading_deck_sha():
    for obj in ({'a': 1, 'b': [1, 2, 3]}, [{'id': 'x'}, {'id': 'y'}], []):
        assert sgd.sha(obj) == bgd.sha(obj)


# ---------------------------------------------------------------------------
# Tamper evidence
# ---------------------------------------------------------------------------


def test_key_hash_mismatch_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    # Corrupt deck_key.json AFTER the manifest was baked against the
    # original content -- simulates an edited/regenerated key file.
    key_path = tmp_path / 'deck' / 'deck_key.json'
    tampered = key_list + [_card('c2', 'q1', 'r2', ['chunk:3:exact:100'])]
    key_path.write_text(json.dumps(tampered, ensure_ascii=False),
                        encoding='utf-8')
    verdicts = _write_verdicts(tmp_path, deck_id,
                              [{'id': 'c1', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='key_hash'):
        sgd.score(deck_dir, verdicts)


def test_missing_deck_id_in_verdicts_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    p = tmp_path / 'verdicts.json'
    # No 'deck' key at all.
    p.write_text(json.dumps({'verdicts': [{'id': 'c1', 'grade': 'same_text'}]}),
                encoding='utf-8')
    with pytest.raises(SystemExit, match='no deck id declared'):
        sgd.score(deck_dir, str(p))


def test_wrong_deck_id_in_verdicts_is_fatal(tmp_path):
    key_list = [_card('c1', 'q1', 'r1', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    verdicts = _write_verdicts(tmp_path, 'not' + deck_id[3:],
                              [{'id': 'c1', 'grade': 'same_text'}])
    with pytest.raises(SystemExit, match='REFUSING'):
        sgd.score(deck_dir, verdicts)


# ---------------------------------------------------------------------------
# Duplicate / orphan / unknown-grade rejection -- fatal, counted, never a
# silent skip.
# ---------------------------------------------------------------------------


def test_duplicate_verdict_id_is_fatal(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', ['passage'])
               for i in range(5)]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c1', 'grade': 'same_text'},
           {'id': 'c0', 'grade': 'unrelated'}]  # c0 graded twice
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='duplicate'):
        sgd.score(deck_dir, verdicts)


def test_orphan_verdict_id_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'DOES-NOT-EXIST', 'grade': 'same_text'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='absent from the deck key'):
        sgd.score(deck_dir, verdicts)


def test_unknown_grade_is_fatal(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': 'c0', 'grade': 'totally_not_a_real_grade'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit, match='prereg vocabulary'):
        sgd.score(deck_dir, verdicts)


def test_all_three_defects_counted_together_not_first_only(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', ['passage']),
               _card('c1', 'q1', 'r1', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c0', 'grade': 'same_text'},          # duplicate
           {'id': 'nope', 'grade': 'same_text'},         # orphan
           {'id': 'c1', 'grade': 'bogus_grade'}]         # unknown grade
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    with pytest.raises(SystemExit) as ei:
        sgd.score(deck_dir, verdicts)
    msg = str(ei.value)
    assert '1 duplicate' in msg
    assert '1 verdict id(s) absent' in msg
    assert 'bogus_grade' in msg


# ---------------------------------------------------------------------------
# --min-graded
# ---------------------------------------------------------------------------


def test_min_graded_fatal_when_under_threshold(tmp_path):
    key_list = [_card(f'c{i}', f'q{i}', f'r{i}', ['passage'])
               for i in range(10)]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': f'c{i}', 'grade': 'same_text'} for i in range(5)]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    # default (0 = off): fine with only 5 graded.
    res = sgd.score(deck_dir, verdicts)
    assert res['graded'] == 5
    # min_graded above what's graded: fatal.
    with pytest.raises(SystemExit, match='min-graded'):
        sgd.score(deck_dir, verdicts, min_graded=8)
    # min_graded exactly at the graded count: passes.
    res2 = sgd.score(deck_dir, verdicts, min_graded=5)
    assert res2['graded'] == 5


# ---------------------------------------------------------------------------
# scored.json backward compatibility -- add keys, never remove.
# ---------------------------------------------------------------------------


def test_output_shape_keeps_original_keys(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', ['passage']),
               _card('c1', 'q1', 'r1', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c1', 'grade': 'unrelated'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sgd.score(deck_dir, verdicts)
    assert set(res) >= {'deck', 'graded', 'methods'}
    m = res['methods']['passage']
    assert set(m) >= {'n', 'strict', 'useful', 'not_wrong', 'grades'}
    assert m['n'] == 2
    assert m['grades'] == {'same_text': 1, 'unrelated': 1}
    # additive keys are present too
    assert 'strict_cluster' in m and 'useful_cluster' in m
    assert 'not_wrong_cluster' in m


# ---------------------------------------------------------------------------
# is_source split -- present when the key carries it, absent (never
# fabricated) when it does not.
# ---------------------------------------------------------------------------


def test_is_source_split_computed_when_key_carries_it(tmp_path):
    key_list = []
    rows = []
    # 10 source-manuscript cards, all same_text (perfect strict precision).
    for i in range(10):
        cid = f'src{i}'
        key_list.append(_card(cid, f'q{i}', f'r{i}', ['passage'],
                              is_source=True))
        rows.append({'id': cid, 'grade': 'same_text'})
    # 10 non-source cards, all unrelated (zero strict precision).
    for i in range(10):
        cid = f'oth{i}'
        key_list.append(_card(cid, f'q{i + 10}', f'r{i + 10}', ['passage'],
                              is_source=False))
        rows.append({'id': cid, 'grade': 'unrelated'})
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sgd.score(deck_dir, verdicts)
    split = res['methods']['passage']['strict_by_is_source']
    assert split['true'][0] == 1.0    # 10/10 same_text
    assert split['false'][0] == 0.0   # 0/10 same_text


def test_is_source_split_absent_when_key_lacks_the_field(tmp_path):
    key_list = [_card('c0', 'q0', 'r0', ['passage']),   # no is_source at all
               _card('c1', 'q1', 'r1', ['passage'])]
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    rows = [{'id': 'c0', 'grade': 'same_text'},
           {'id': 'c1', 'grade': 'unrelated'}]
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sgd.score(deck_dir, verdicts)
    assert 'strict_by_is_source' not in res['methods']['passage']


# ---------------------------------------------------------------------------
# Query-clustered bootstrap CI: on data with strong within-query
# correlation, the cluster CI must be wider-or-equal to the iid Wilson CI.
# ---------------------------------------------------------------------------


def test_cluster_ci_wider_or_equal_to_wilson_on_correlated_data(tmp_path):
    # 20 queries x 4 cards each = 80 cards, one method. Odd-indexed queries
    # are ENTIRELY same_text (strict hit), even-indexed ENTIRELY unrelated
    # (strict miss) -- perfect within-query correlation, so the effective
    # sample size for the true uncertainty is ~20 (queries), not 80 (cards).
    key_list, rows = [], []
    for qi in range(20):
        grade = 'same_text' if qi % 2 else 'unrelated'
        for ci in range(4):
            cid = f'q{qi}_c{ci}'
            key_list.append(_card(cid, f'q{qi}', f'r{qi}_{ci}', ['passage']))
            rows.append({'id': cid, 'grade': grade})
    deck_dir, deck_id = _write_deck(tmp_path, key_list)
    verdicts = _write_verdicts(tmp_path, deck_id, rows)
    res = sgd.score(deck_dir, verdicts, cluster_seed=42,
                    cluster_resamples=2000)
    m = res['methods']['passage']
    assert m['n'] == 80
    wilson_width = m['strict'][2] - m['strict'][1]
    cluster_width = m['strict_cluster'][2] - m['strict_cluster'][1]
    assert cluster_width >= wilson_width, (
        f'cluster CI ({m["strict_cluster"]}) must be at least as wide as '
        f'the iid Wilson CI ({m["strict"]}) on perfectly query-correlated '
        f'data')
    # The point estimates should agree (both are exactly 10/20 queries hit
    # at the card level too: 40/80 = 0.5).
    assert m['strict'][0] == 0.5
    assert m['strict_cluster'][0] == 0.5

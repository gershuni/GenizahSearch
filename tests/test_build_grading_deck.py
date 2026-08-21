# -*- coding: utf-8 -*-
"""Tests for `scripts/build_grading_deck.py` (Codex hardening: "the deck's
stated unit is wrong", "not artifact-pinned", "self-retrieval discarded").

Every fixture is FABRICATED: opaque query/record/sys ids, placeholder
manifest/meta bytes -- never real research content or file paths.
"""
from __future__ import annotations

import hashlib

from scripts import build_grading_deck as bgd


# ---------------------------------------------------------------------------
# dedupe_to_grain -- the estimand fix ("the deck's stated unit is wrong")
# ---------------------------------------------------------------------------


def test_sys_grain_collapses_two_records_of_one_sys_keeping_best_rank():
    # Query q1: method 'passage' returns [sysA_p2 (rank0), sysB_p1 (rank1)];
    # method 'chunk' returns [sysA_p9 (rank0)] -- a DIFFERENT record of the
    # SAME manuscript (sysA), at a better rank (0) than passage's own sysA
    # hit (rank0 too, but from a different record id). Under sys grain,
    # sysA must collapse to exactly one card, and BOTH methods must be
    # credited on it (chunk found sysA via a different page than passage
    # did) even though only one record id survives as the card.
    retrieved = {
        ('q1', 'passage'): ['sysA_p2', 'sysB_p1'],
        ('q1', 'chunk'): ['sysA_p9'],
    }
    pairs = bgd.dedupe_to_grain(retrieved, grain='sys')
    sys_a_cards = [(q, rid) for (q, rid) in pairs if rid.split('_')[0] == 'sysA']
    assert len(sys_a_cards) == 1, (
        f'sysA must collapse to exactly one card under --grain sys, got '
        f'{sys_a_cards}')
    (_q, kept_rid) = sys_a_cards[0]
    # rank0 beats rank0 by first-seen-wins is ambiguous only if EQUAL; here
    # passage's sysA_p2 is rank0 in ITS OWN list and chunk's sysA_p9 is also
    # rank0 in ITS OWN list -- both are "best" for their method, so either
    # may legitimately survive, but the credited methods must be the UNION.
    assert kept_rid in ('sysA_p2', 'sysA_p9')
    assert pairs[('q1', kept_rid)] == {'passage', 'chunk'}
    # sysB is untouched (only one record, one method).
    assert pairs[('q1', 'sysB_p1')] == {'passage'}


def test_sys_grain_keeps_the_strictly_better_rank_when_unambiguous():
    # method 'passage' alone returns two pages of sysA: p5 at rank0 (best),
    # p9 at rank1 (worse). Only p5 may survive.
    retrieved = {('q1', 'passage'): ['sysA_p5', 'sysA_p9']}
    pairs = bgd.dedupe_to_grain(retrieved, grain='sys')
    assert list(pairs) == [('q1', 'sysA_p5')]
    assert pairs[('q1', 'sysA_p5')] == {'passage'}


def test_sys_grain_updates_to_a_later_encountered_better_rank():
    # methodX's own list puts sysA at rank1 (not its top hit); methodX is
    # processed FIRST (dict insertion order), so the (q1, sysA) cell is
    # first created with best_rank=1. methodY is processed SECOND and puts
    # sysA at rank0 -- a genuinely BETTER rank arriving after a worse one.
    # If the "is this rank better than what we already kept" comparison
    # were disabled, the cell would wrongly keep the first (worse) hit.
    retrieved = {
        ('q1', 'methodX'): ['other1', 'sysA_pWorse'],
        ('q1', 'methodY'): ['sysA_pBetter'],
    }
    pairs = bgd.dedupe_to_grain(retrieved, grain='sys')
    sys_a_cards = [rid for (_q, rid) in pairs if rid.startswith('sysA')]
    assert sys_a_cards == ['sysA_pBetter']
    assert pairs[('q1', 'sysA_pBetter')] == {'methodX', 'methodY'}


def test_record_grain_does_not_collapse_same_sys_different_records():
    # The old, always-implicit behaviour: --grain record keeps every
    # (query, record_id) pair exactly as returned, even multiple pages of
    # one manuscript for one query.
    retrieved = {
        ('q1', 'passage'): ['sysA_p2'],
        ('q1', 'chunk'): ['sysA_p9'],
    }
    pairs = bgd.dedupe_to_grain(retrieved, grain='record')
    assert set(pairs) == {('q1', 'sysA_p2'), ('q1', 'sysA_p9')}
    assert pairs[('q1', 'sysA_p2')] == {'passage'}
    assert pairs[('q1', 'sysA_p9')] == {'chunk'}


def test_dedupe_to_grain_rejects_unknown_grain():
    import pytest
    with pytest.raises(ValueError):
        bgd.dedupe_to_grain({}, grain='page')


# ---------------------------------------------------------------------------
# compute_is_source -- the self-retrieval fact, kept instead of discarded
# ---------------------------------------------------------------------------


def test_compute_is_source_true_when_record_sys_matches_query_meta():
    row = {'query_id': 'q1', 'meta': {'sys_id': 'sysA'}}
    assert bgd.compute_is_source('sysA_p3', row) is True


def test_compute_is_source_false_when_record_sys_differs():
    row = {'query_id': 'q1', 'meta': {'sys_id': 'sysA'}}
    assert bgd.compute_is_source('sysB_p3', row) is False


def test_compute_is_source_none_when_query_has_no_meta_sys_id():
    assert bgd.compute_is_source('sysA_p3', {'query_id': 'q1'}) is None
    assert bgd.compute_is_source('sysA_p3',
                                 {'query_id': 'q1', 'meta': {}}) is None


# ---------------------------------------------------------------------------
# fingerprint_inputs -- full-length hashes, never a silent omission
# ---------------------------------------------------------------------------


def test_fingerprint_inputs_queries_file_is_full_sha256(tmp_path):
    q = tmp_path / 'queries.jsonl'
    # write_bytes (not write_text) so no platform newline translation can
    # make the on-disk bytes differ from what this test hashes.
    content = b'{"query_id": "q1", "text": "fake"}\n'
    q.write_bytes(content)
    idx_dir = tmp_path / 'idx'
    idx_dir.mkdir()
    fp = bgd.fingerprint_inputs(str(q), str(idx_dir),
                                tantivy_index_dir=str(tmp_path / 'no_tantivy'))
    expected = hashlib.sha256(content).hexdigest()
    assert fp['queries_file']['sha256'] == expected
    assert len(fp['queries_file']['sha256']) == 64  # full sha256, not a prefix


def test_fingerprint_inputs_passage_manifest_hashes_file_bytes(tmp_path):
    q = tmp_path / 'queries.jsonl'
    q.write_text('{}', encoding='utf-8')
    idx_dir = tmp_path / 'idx'
    idx_dir.mkdir()
    manifest_bytes = b'{"layout": 1, "counts": {"n_records": 3}}'
    (idx_dir / 'manifest.json').write_bytes(manifest_bytes)
    fp = bgd.fingerprint_inputs(str(q), str(idx_dir),
                                tantivy_index_dir=str(tmp_path / 'no_tantivy'))
    assert fp['passage_index_manifest']['sha256'] == hashlib.sha256(
        manifest_bytes).hexdigest()
    assert len(fp['passage_index_manifest']['sha256']) == 64


def test_fingerprint_inputs_tantivy_meta_present(tmp_path):
    q = tmp_path / 'queries.jsonl'
    q.write_text('{}', encoding='utf-8')
    idx_dir = tmp_path / 'idx'
    idx_dir.mkdir()
    (idx_dir / 'manifest.json').write_bytes(b'{}')
    tantivy_dir = tmp_path / 'tantivy'
    tantivy_dir.mkdir()
    meta_bytes = b'{"segments": [], "schema": []}'
    (tantivy_dir / 'meta.json').write_bytes(meta_bytes)
    fp = bgd.fingerprint_inputs(str(q), str(idx_dir),
                                tantivy_index_dir=str(tantivy_dir))
    assert fp['tantivy_index']['sha256'] == hashlib.sha256(
        meta_bytes).hexdigest()
    assert 'status' not in fp['tantivy_index']


def test_fingerprint_inputs_tantivy_meta_absent_is_named_not_omitted(tmp_path):
    q = tmp_path / 'queries.jsonl'
    q.write_text('{}', encoding='utf-8')
    idx_dir = tmp_path / 'idx'
    idx_dir.mkdir()
    (idx_dir / 'manifest.json').write_bytes(b'{}')
    missing_tantivy_dir = tmp_path / 'no_such_tantivy_dir'
    fp = bgd.fingerprint_inputs(str(q), str(idx_dir),
                                tantivy_index_dir=str(missing_tantivy_dir))
    # Never silently absent from the dict -- the key exists and says why.
    assert 'tantivy_index' in fp
    assert fp['tantivy_index']['status'] == 'unfingerprinted'
    assert 'sha256' not in fp['tantivy_index']


def test_fingerprint_inputs_default_tantivy_dir_is_config_index_dir(tmp_path):
    # No explicit tantivy_index_dir -- must fall back to shared.config's
    # Config.INDEX_DIR rather than silently skipping the Tantivy entry.
    import os

    from shared.config import Config
    q = tmp_path / 'queries.jsonl'
    q.write_text('{}', encoding='utf-8')
    idx_dir = tmp_path / 'idx'
    idx_dir.mkdir()
    (idx_dir / 'manifest.json').write_bytes(b'{}')
    fp = bgd.fingerprint_inputs(str(q), str(idx_dir))
    assert fp['tantivy_index']['path'] == os.path.join(Config.INDEX_DIR,
                                                       'meta.json')

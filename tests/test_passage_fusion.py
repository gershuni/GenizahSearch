# -*- coding: utf-8 -*-
"""Unit tests for shared/passage_fusion.py -- the single definition of how
multi-witness passage results are ranked.

The module is pure (no engine, no NiceGUI, no I/O), so everything here runs on
hand-built dicts. That is the point: the ranking rule that decides what a user
sees first is testable without an index.
"""
import pytest

from shared import passage_fusion as pf


def _row(rec, score, **extra):
    row = {
        'raw_header': rec,
        'uid': rec,
        'score': float(score),
        'final_score': float(score),
        'chunk_count': 1,
        'text': f'text-of-{rec}',
        'source_ctx': f'ctx-of-{rec}',
        'chunk_hits': [],
    }
    row.update(extra)
    return row


def _witness(wid, label, recs_and_scores):
    """A witness's result list, in descending score order, already tagged."""
    rows = [_row(rec, sc) for rec, sc in recs_and_scores]
    return pf.tag_rows(rows, wid, label)


# --- tag_rows ---------------------------------------------------------------

def test_tag_rows_stamps_identity_and_one_based_rank():
    rows = _witness('w1', 'Seed', [('A', 300), ('B', 200), ('C', 100)])
    assert [r['witness_rank'] for r in rows] == [1, 2, 3]
    assert {r['witness_id'] for r in rows} == {'w1'}
    assert {r['witness_label'] for r in rows} == {'Seed'}


def test_fuse_refuses_untagged_rows():
    """Fail closed rather than invent a rank. An untagged row silently ranked
    1 would hand every un-tagged witness the maximum RRF contribution."""
    with pytest.raises(ValueError, match='witness_rank'):
        pf.fuse({'w1': [_row('A', 100)]})


# --- fuse -------------------------------------------------------------------

def test_two_witnesses_on_one_record_accumulate_rather_than_overwrite():
    """The engine's own `hit_by_header` dict is keyed by record and COLLIDES
    across witnesses; last-witness-wins is how witness_count silently becomes
    1. Pinned here because the collision is invisible in the happy path."""
    w1 = _witness('w1', 'Seed', [('A', 300)])
    w2 = _witness('w2', 'Other', [('A', 500)])

    fused = pf.fuse({'w1': w1, 'w2': w2})

    assert len(fused) == 1
    row = fused[0]
    assert row['witness_count'] == 2
    assert row['witness_ids'] == 'w1,w2'
    # score stays MATCHED LETTERS on the best witness's scale -- not the RRF
    # sum, which would silently turn ~500 into ~0.03 in the Max:/Avg: badges
    # and every export column that reads it.
    assert row['score'] == 500.0
    assert row['final_score'] == 500.0
    assert row['fusion_score'] == pytest.approx(2.0 / (60 + 1))


def test_fusion_score_is_the_hand_computed_rrf_sum():
    w1 = _witness('w1', 'Seed', [('A', 300), ('B', 200)])
    w2 = _witness('w2', 'Other', [('B', 900), ('A', 100)])

    fused = {r['raw_header']: r for r in pf.fuse({'w1': w1, 'w2': w2})}

    # A: rank 1 under w1, rank 2 under w2. B: rank 2, then rank 1.
    assert fused['A']['fusion_score'] == pytest.approx(1 / 61 + 1 / 62)
    assert fused['B']['fusion_score'] == pytest.approx(1 / 62 + 1 / 61)


def test_winner_supplies_the_rendered_evidence():
    """A span offset is a position in ONE witness's text. Rendering witness
    B's highlight beside witness A's label would be a lie about provenance."""
    a = _row('REC', 100, text='from-w1', source_ctx='ctx-w1', chunk_count=2)
    b = _row('REC', 900, text='from-w2', source_ctx='ctx-w2', chunk_count=7)
    pf.tag_rows([a], 'w1', 'Seed')
    pf.tag_rows([b], 'w2', 'Other')

    # Same rank on both, so the higher score wins the tie.
    row = pf.fuse({'w1': [a], 'w2': [b]})[0]
    assert row['witness_id'] == 'w2'
    assert row['witness_label'] == 'Other'
    assert row['text'] == 'from-w2'
    assert row['source_ctx'] == 'ctx-w2'
    assert row['chunk_count'] == 7


def test_best_rank_beats_higher_score_across_witnesses():
    """RRF is rank-based ON PURPOSE. A passage score is matched QUERY letters,
    so a 6,000-letter witness mechanically outscores a 1,200-letter one. This
    is the measured difference between 18/26 and 10/19 positives in the top
    50/100 on the Antiochus instrument."""
    short_w = _witness('w1', 'Short', [('GOOD', 400)])
    long_w = _witness('w2', 'Long', [(f'F{i}', 5000 - i) for i in range(30)]
                      + [('GOOD', 900)])

    fused = pf.fuse({'w1': short_w, 'w2': long_w})

    # GOOD is rank 1 for the short witness and rank 31 for the long one, so
    # its RRF sum beats every long-witness-only row despite far lower scores.
    assert fused[0]['raw_header'] == 'GOOD'
    assert fused[0]['witness_id'] == 'w1'
    # ... and its score is the WINNER'S, not the other witness's 900. This
    # line used to assert 900.0, one line below `witness_id == 'w1'`: the row
    # rendered w1's label and w1's highlighted span beside a number that
    # belonged to w2's text. The ranking claim this test exists for is
    # untouched; the contradiction beside it is gone.
    assert fused[0]['score'] == 400.0
    assert fused[0]['best_witness_score'] == 900.0


def test_fuse_is_order_deterministic_for_equal_contributions():
    w1 = _witness('w1', 'A', [('X', 100)])
    w2 = _witness('w2', 'B', [('X', 100)])
    assert pf.fuse({'w1': w1, 'w2': w2})[0]['witness_id'] == 'w1'
    assert pf.fuse({'w2': w2, 'w1': w1})[0]['witness_id'] == 'w2'


def test_fuse_accepts_pairs_as_well_as_a_mapping():
    w1 = _witness('w1', 'A', [('X', 100)])
    w2 = _witness('w2', 'B', [('Y', 100)])
    assert len(pf.fuse([('w1', w1), ('w2', w2)])) == 2


def test_fuse_does_not_mutate_the_input_rows():
    """The page keeps per-witness lists in session state and re-fuses on every
    addition. A fuse that wrote back onto them would compound each round."""
    w1 = _witness('w1', 'A', [('X', 100)])
    pf.fuse({'w1': w1})
    assert 'fusion_score' not in w1[0]
    assert 'witness_ids' not in w1[0]


# --- group_stats ------------------------------------------------------------

def test_group_witness_count_is_a_union_not_a_sum():
    """Two pages of one manuscript found by the SAME witness is one witness,
    not two. This is the only genuinely novel ranking logic in the feature."""
    items = [
        {'witness_ids': 'w1,w2', 'fusion_score': 0.03},
        {'witness_ids': 'w2,w3', 'fusion_score': 0.02},
    ]
    stats = pf.group_stats(items)
    assert stats['witness_count'] == 3          # union, not 2 + 2
    assert stats['witness_ids'] == ['w1', 'w2', 'w3']
    assert stats['fusion_score'] == pytest.approx(0.05)


def test_group_stats_falls_back_to_witness_id_on_single_witness_rows():
    """Single-witness rows never carry `witness_ids` (the engine short-
    circuits before fusing), so the group must still count them."""
    stats = pf.group_stats([{'witness_id': 'w1', 'fusion_score': 0}])
    assert stats['witness_count'] == 1


def test_group_stats_on_rows_with_no_witness_fields_is_zero():
    assert pf.group_stats([{'score': 5}])['witness_count'] == 0
    assert pf.group_stats([])['witness_count'] == 0


# --- split_pasted -----------------------------------------------------------

def test_split_pasted_splits_on_blank_lines_and_counts_what_it_drops():
    blob = "aleph bet gimel\ndalet\n\nhe vav zayin\n\nshort\n\nchet tet yod kaf"
    texts, skipped = pf.split_pasted(blob)
    assert texts == ["aleph bet gimel\ndalet", "he vav zayin",
                     "chet tet yod kaf"]
    # Never a silent drop: "short" is one word, under the page's own 3-word
    # floor, and the caller must be able to say so.
    assert skipped == 1


def test_split_pasted_tolerates_a_lone_asterisk_separator():
    """The shape of the owner's hand-maintained witness file."""
    texts, skipped = pf.split_pasted("aleph bet gimel\n*\nhe vav zayin")
    assert texts == ["aleph bet gimel", "he vav zayin"]
    assert skipped == 0


def test_split_pasted_on_a_single_witness_returns_it_unchanged():
    texts, skipped = pf.split_pasted("aleph bet gimel dalet")
    assert texts == ["aleph bet gimel dalet"]
    assert skipped == 0


def test_split_pasted_on_empty_input_is_empty():
    assert pf.split_pasted('') == ([], 0)
    assert pf.split_pasted(None) == ([], 0)


# --- ids and digests --------------------------------------------------------

def test_witness_ids_are_short_and_one_based():
    assert pf.witness_id_for(0) == 'w1'
    assert pf.witness_id_for(24) == 'w25'


def test_text_digest_separates_texts_that_share_a_label():
    """The fingerprint must move when the SET OF TEXTS changes; labels are
    user-editable and cannot carry that job."""
    assert pf.text_digest('aleph') != pf.text_digest('bet')
    assert pf.text_digest('aleph') == pf.text_digest('aleph')
    assert len(pf.text_digest('aleph')) == 16


def test_split_ids_round_trips_the_flat_scalar():
    assert pf.split_ids('w1,w2,w3') == ['w1', 'w2', 'w3']
    assert pf.split_ids('') == []
    assert pf.split_ids(None) == []
    assert pf.split_ids(['w1', 'w2']) == ['w1', 'w2']


def test_the_score_and_the_rendered_evidence_come_from_one_witness():
    """A fused row shows ONE witness's label and ONE witness's highlighted
    span -- a span offset is a position in that witness's text and means
    nothing against another's. A score from a different contributor therefore
    describes text the reader cannot see."""
    # The rendered fields are given DIFFERENT values per witness, or the
    # assertion below could not tell whose row survived.
    winner = pf.tag_rows(
        [_row('REC', 120, source_ctx='span-in-w1')], 'w1', 'Winner')
    louder = pf.tag_rows(
        [_row('OTHER', 999), _row('REC', 880, source_ctx='span-in-w2')],
        'w2', 'Louder')

    row = pf.fuse({'w1': winner, 'w2': louder})[0]

    assert row['raw_header'] == 'REC'
    assert row['witness_id'] == 'w1'
    assert row['witness_label'] == 'Winner'
    assert row['source_ctx'] == 'span-in-w1', (
        'sanity: the rendered evidence really is the winner\'s'
    )
    assert row['score'] == 120.0, (
        'the score must describe the span rendered beside it'
    )
    assert row['final_score'] == 120.0, 'both fields, or the exports disagree'


def test_the_best_witness_score_is_still_reported():
    """Fusing must not DISCARD the information, only stop mislabelling it."""
    row = pf.fuse({
        'w1': _witness('w1', 'A', [('REC', 120)]),
        'w2': _witness('w2', 'B', [('X', 1), ('REC', 880)]),
    })[0]
    assert row['best_witness_score'] == 880.0


def test_a_single_witness_row_scores_the_same_either_way():
    """With one contributor the winner IS the maximum, so nothing about the
    common case moves."""
    row = pf.fuse({'w1': _witness('w1', 'A', [('REC', 214)])})[0]
    assert row['score'] == 214.0
    assert row['best_witness_score'] == 214.0


def test_group_stats_carries_the_best_witness_score_up():
    """The group badge needs a number no row explains to be reachable
    SOMEWHERE, or fusing silently loses the strongest match on a manuscript."""
    # The maximum comes FIRST on purpose. Listed ascending, "take the last"
    # and "take the maximum" agree, and a mutation to the former stayed green.
    items = [
        {'witness_ids': 'w1', 'fusion_score': 0.01, 'best_witness_score': 880},
        {'witness_ids': 'w2', 'fusion_score': 0.02, 'best_witness_score': 120},
    ]
    stats = pf.group_stats(items)
    assert stats['best_witness_score'] == 880.0
    assert stats['witness_count'] == 2, 'the union rule is untouched'


def test_group_stats_survives_junk_in_the_best_score():
    stats = pf.group_stats([
        {'witness_ids': 'w1', 'fusion_score': 0.01, 'best_witness_score': None},
        {'witness_ids': 'w2', 'fusion_score': 0.02, 'best_witness_score': 'x'},
        {'witness_ids': 'w3', 'fusion_score': 0.03, 'best_witness_score': 7},
    ])
    assert stats['best_witness_score'] == 7.0

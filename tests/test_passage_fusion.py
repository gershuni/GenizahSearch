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


def test_one_witness_contributes_once_per_record():
    """The RRF sum accumulated per ROW, so two rows carrying the same
    `raw_header` in one witness's list gave that witness two contributions to
    one record. The engine keys its hits by (witness, record) and should never
    emit that -- which is exactly why it would go unnoticed if it did: it does
    not move `witness_count` (keyed by witness position), only inflates one
    witness's share of the ranking, and a ranking that is quietly wrong looks
    like one that is right."""
    dupes = pf.tag_rows([_row('REC', 300), _row('REC', 100)], 'w1', 'Seed')

    row = pf.fuse({'w1': dupes})[0]

    assert row['fusion_score'] == pytest.approx(1 / 61), (
        'the duplicate added a second 1/(60+rank) term'
    )
    assert row['witness_count'] == 1


def test_the_better_ranked_duplicate_is_the_one_kept():
    dupes = pf.tag_rows([_row('OTHER', 500), _row('REC', 300),
                         _row('REC', 100)], 'w1', 'Seed')
    row = [r for r in pf.fuse({'w1': dupes}) if r['raw_header'] == 'REC'][0]
    # 'REC' is rank 2 and rank 3; the better rank wins.
    assert row['fusion_score'] == pytest.approx(1 / 62)
    assert row['score'] == 300.0


def test_duplicates_do_not_break_the_cross_witness_count():
    a = pf.tag_rows([_row('REC', 300), _row('REC', 100)], 'w1', 'A')
    b = pf.tag_rows([_row('REC', 900)], 'w2', 'B')
    row = pf.fuse({'w1': a, 'w2': b})[0]
    assert row['witness_count'] == 2
    assert row['fusion_score'] == pytest.approx(1 / 61 + 1 / 61)


def test_the_witness_length_cap_has_one_definition():
    """The page had no cap at all, so an over-long paste became a witness that
    spent its whole 30s ceiling and failed -- once per witness. The number
    lives here so the page cannot enforce something different from the API."""
    assert pf.MAX_WITNESS_CHARS == 20000


def test_split_by_length_rejects_rather_than_truncates():
    """Half a manuscript searched as if it were the whole one is a worse
    answer than none, and an invisible one."""
    short, long_one = 'x' * 10, 'y' * (pf.MAX_WITNESS_CHARS + 1)
    ok, too_long = pf.split_by_length([short, long_one])
    assert ok == [short]
    assert too_long == [long_one]
    assert too_long[0] == long_one, 'the reject is returned whole, not trimmed'


def test_split_by_length_is_inclusive_at_the_cap():
    exactly = 'x' * pf.MAX_WITNESS_CHARS
    ok, too_long = pf.split_by_length([exactly])
    assert ok == [exactly] and too_long == []


def test_split_by_length_keeps_input_order():
    a, b, c = 'a' * 5, 'b' * 7, 'c' * 3
    ok, _ = pf.split_by_length([a, b, c])
    assert ok == [a, b, c]


def test_split_by_length_takes_an_explicit_cap():
    ok, too_long = pf.split_by_length(['abcdef', 'ab'], cap=3)
    assert ok == ['ab'] and too_long == ['abcdef']


def test_split_by_length_survives_none_and_empty():
    assert pf.split_by_length(None) == ([], [])
    assert pf.split_by_length([None, '']) == ([None, ''], [])


# ---------------------------------------------------------------------------
# PR #329 review round 1: the `best_witness_score` fallback.
# ---------------------------------------------------------------------------


def test_group_stats_falls_back_to_the_row_score_when_unfused():
    """A single-witness or chunk row carries no `best_witness_score`, and its
    own score is exactly "the best any witness made" -- there being one.
    Reporting 0.0 tied every such group together under `best_match`.
    """
    from shared.passage_fusion import group_stats
    stats = group_stats([{'score': 214.0}, {'score': 88.0}])
    assert stats['best_witness_score'] == 214.0


def test_the_fallback_never_overrides_a_real_fused_value():
    """The fallback must not become a second definition: where `fuse()` set
    the field, that value stands even though it is LOWER than a sibling row's
    raw score."""
    from shared.passage_fusion import group_stats
    stats = group_stats([
        {'score': 300.0, 'best_witness_score': 900.0},
        {'score': 500.0, 'best_witness_score': 500.0},
    ])
    assert stats['best_witness_score'] == 900.0


# ---------------------------------------------------------------------------
# PR #329 review round 3: routing vs. the contributor arithmetic.
# ---------------------------------------------------------------------------

def _r(rec, rank, score, **extra):
    row = {'raw_header': rec, 'witness_rank': rank, 'score': float(score),
           'final_score': float(score)}
    row.update(extra)
    return row


def test_a_main_record_counts_contributors_from_the_filtered_bucket_too():
    """`filter_text` routes a ROW. The fusion facts describe a RECORD, and
    fusing each bucket separately conflated the two: a manuscript found by two
    witnesses, one of them on known source text, reported `witness_count` 1 --
    contradicting that field's documented meaning ("how many distinct
    witnesses point at this manuscript") and under-ranking it against records
    whose contributors happened to avoid the filter.
    """
    from shared.passage_fusion import fuse_routed
    main, filtered = fuse_routed(
        [('w1', [_r('A', 1, 500, witness_id='w1')]), ('w2', [])],
        [('w1', []), ('w2', [_r('A', 1, 900, witness_id='w2')])],
    )
    assert len(main) == 1 and not filtered
    assert main[0]['witness_count'] == 2, 'the filtered contributor was lost'
    assert main[0]['witness_ids'] == 'w1,w2'
    assert main[0]['best_witness_score'] == 900.0


def test_the_rendered_row_still_comes_from_an_eligible_contributor():
    """A row shows ONE witness's highlighted span, and a filtered span is
    exactly the text the caller asked to discount. So the arithmetic counts
    every contributor while the evidence stays eligible -- even when the
    filtered contributor ranked the record higher.
    """
    from shared.passage_fusion import fuse_routed
    main, _ = fuse_routed(
        [('w1', [_r('A', 5, 100, witness_id='w1')]), ('w2', [])],
        [('w1', []), ('w2', [_r('A', 1, 900, witness_id='w2')])],
    )
    assert main[0]['witness_id'] == 'w1', (
        'the rendered row came from the filtered bucket, so the highlighted '
        'span is text the caller asked to discount'
    )
    assert main[0]['score'] == 100.0, 'score must describe the rendered span'
    assert main[0]['best_witness_score'] == 900.0


def test_the_overlay_reorders_main():
    """The overlay changes the key `fuse` had just sorted by, so the list has
    to be re-sorted or the returned order contradicts the scores on it.

    A: eligible at rank 50 (1/110) plus a filtered contributor at rank 1
       (1/61) -> complete 0.0255
    B: eligible at rank 2 (1/62) -> complete 0.0161
    Eligible-only arithmetic puts B first; the complete one puts A first.
    """
    from shared.passage_fusion import fuse_routed
    main, _ = fuse_routed(
        [('w1', [_r('B', 2, 300, witness_id='w1'),
                 _r('A', 50, 100, witness_id='w1')]), ('w2', [])],
        [('w1', []), ('w2', [_r('A', 1, 900, witness_id='w2')])],
    )
    assert [r['raw_header'] for r in main] == ['A', 'B'], (
        'main was not re-sorted after the contributor overlay'
    )


def test_a_record_every_witness_filtered_stays_filtered():
    """The routing rule itself, now executed rather than grepped for: a record
    is `filtered` only when EVERY witness that matched it filtered it."""
    from shared.passage_fusion import fuse_routed
    main, filtered = fuse_routed(
        [('w1', []), ('w2', [])],
        [('w1', [_r('A', 1, 500, witness_id='w1')]),
         ('w2', [_r('A', 3, 400, witness_id='w2')])],
    )
    assert not main
    assert len(filtered) == 1
    assert filtered[0]['witness_count'] == 2


def test_one_eligible_witness_is_enough_to_keep_a_record_in_main():
    """Suppressing it would make the filter STRICTER the more witnesses are
    added -- the opposite of what the control says."""
    from shared.passage_fusion import fuse_routed
    main, filtered = fuse_routed(
        [('w1', [_r('A', 2, 500, witness_id='w1')]), ('w2', [])],
        [('w1', []), ('w2', [_r('A', 1, 900, witness_id='w2')])],
    )
    assert [r['raw_header'] for r in main] == ['A']
    assert not filtered, 'the record was returned in BOTH buckets'


def test_without_filter_text_the_result_is_identical_to_a_plain_fuse():
    """The common path: every filtered bucket empty, so the overlay is a
    no-op. Guards the blast radius of the whole change."""
    from shared.passage_fusion import fuse, fuse_routed
    pairs = [('w1', [_r('A', 1, 500, witness_id='w1'),
                     _r('B', 2, 300, witness_id='w1')]),
             ('w2', [_r('B', 1, 400, witness_id='w2')])]
    plain = fuse([(w, [dict(r) for r in rows]) for w, rows in pairs])
    main, filtered = fuse_routed(
        [(w, [dict(r) for r in rows]) for w, rows in pairs],
        [('w1', []), ('w2', [])],
    )
    assert not filtered
    assert [r['raw_header'] for r in main] == [r['raw_header'] for r in plain]
    for a, b in zip(main, plain):
        for field in ('fusion_score', 'witness_count', 'witness_ids',
                      'score', 'final_score', 'best_witness_score'):
            assert a[field] == b[field], (field, a[field], b[field])

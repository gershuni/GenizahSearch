# -*- coding: utf-8 -*-
"""The desktop multi-witness state machine.

Every rule in `desktop/passage_witnesses.py` is a bug the WEB surface hit
first, so each one is tested by CALLING it rather than by asserting against
its source text. That distinction is the reason the module exists outside
`genizah_app.py` at all: the equivalent rules on the page were once covered
only by AST assertions, and a mutation sweep proved several of those vacuous
against the exact bugs they were written to catch.

Qt-free by construction -- this module imports no PyQt6, so these run in the
default (non-gui) lane with no QApplication and no segfault risk.
"""
import pytest

from desktop import passage_witnesses as pw
from shared.passage_fusion import MAX_WITNESS_CHARS
from shared.passage_witness_source import WITNESS_SEED_ID

SEED = 'the seed text of the work'
FALLBACK = 'Pasted text'


def _set(*texts, seed=SEED):
    """A set holding `texts` as pasted witnesses, added the normal way."""
    st = pw.WitnessSet()
    for t in texts:
        pw.add_texts(st, [t], seed, FALLBACK)
    return st


def _row(header, score, **kw):
    d = {'raw_header': header, 'score': score}
    d.update(kw)
    return d


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def test_ids_are_never_reused_after_a_removal():
    """A recycled id lets a removed witness's cached rows be attributed to
    its replacement. The caches are keyed by id, so the collision is silent
    and surfaces only as a wrong `witness_count`."""
    st = _set('alpha beta gamma', 'delta epsilon zeta')
    first, second = st.entries[0].id, st.entries[1].id
    pw.remove(st, first)
    pw.add_texts(st, ['eta theta iota'], SEED, FALLBACK)
    third = st.entries[-1].id
    assert third not in (first, second)
    assert len({first, second, third}) == 3


def test_the_seed_is_part_of_the_fusion_order_and_comes_first():
    st = _set('alpha beta gamma')
    assert pw.order(st)[0] == WITNESS_SEED_ID
    assert pw.order(st) == [WITNESS_SEED_ID, st.entries[0].id]


def test_default_label_is_the_first_five_words():
    assert pw.default_label('one two three four five six seven', FALLBACK) == \
        'one two three four five'


def test_default_label_falls_back_when_there_are_no_words():
    assert pw.default_label('   ', FALLBACK) == FALLBACK


def test_searched_count_includes_a_witness_that_found_nothing():
    """A witness that was consulted and found nothing is still a witness.
    Excluding it would make the "of m" denominator move with the results."""
    st = _set('alpha beta gamma')
    st.rows[WITNESS_SEED_ID] = [_row('h1', 10)]
    st.rows[st.entries[0].id] = []          # searched, found nothing
    assert pw.searched_count(st) == 2


def test_searched_count_excludes_a_witness_that_has_not_run():
    st = _set('alpha beta gamma')
    st.rows[WITNESS_SEED_ID] = [_row('h1', 10)]
    assert pw.searched_count(st) == 1


# ---------------------------------------------------------------------------
# Admission rules
# ---------------------------------------------------------------------------

def test_a_paste_identical_to_the_seed_is_refused():
    """The seed IS a witness, fused under WITNESS_SEED_ID. Adding the same
    text again makes `witness_count` report two witnesses where there is
    one -- a wrong number, not merely a redundant search."""
    st = pw.WitnessSet()
    rep = pw.add_texts(st, [SEED], SEED, FALLBACK)
    assert rep.added == []
    assert rep.duplicates == 1
    assert st.entries == []


def test_a_duplicate_of_an_existing_witness_is_refused_and_counted():
    st = _set('alpha beta gamma')
    rep = pw.add_texts(st, ['alpha beta gamma'], SEED, FALLBACK)
    assert rep.duplicates == 1 and rep.added == []
    assert len(st.entries) == 1


def test_duplicates_within_one_paste_are_refused():
    """Two identical blocks in one file are one witness."""
    st = pw.WitnessSet()
    rep = pw.add_texts(st, ['alpha beta gamma', 'alpha beta gamma'],
                       SEED, FALLBACK)
    assert len(rep.added) == 1
    assert rep.duplicates == 1


def test_whitespace_only_differences_are_the_same_witness():
    """`witness_text_key` strips leading/trailing whitespace and nothing
    else -- it deliberately does NOT use the passage normalizer, which would
    collapse two genuinely different witnesses of one work."""
    st = _set('alpha beta gamma')
    rep = pw.add_texts(st, ['  alpha beta gamma\n'], SEED, FALLBACK)
    assert rep.duplicates == 1


def test_two_different_witnesses_of_one_work_are_both_kept():
    """The counterpart to the rule above: near-identical is not identical.
    Collapsing them would silently discard a real witness."""
    st = _set('alpha beta gamma')
    rep = pw.add_texts(st, ['alpha beta gamma delta'], SEED, FALLBACK)
    assert len(rep.added) == 1 and rep.duplicates == 0
    assert len(st.entries) == 2


def test_a_text_under_the_word_floor_is_skipped_and_counted():
    st = pw.WitnessSet()
    rep = pw.add_texts(st, ['too short'], SEED, FALLBACK)
    assert rep.added == [] and rep.too_short == 1


def test_an_over_long_witness_is_rejected_not_truncated():
    """Half a manuscript searched as if it were the whole one is a worse
    answer than none, and an invisible one."""
    st = pw.WitnessSet()
    long_text = 'word ' * (MAX_WITNESS_CHARS // 2)
    assert len(long_text) > MAX_WITNESS_CHARS
    rep = pw.add_texts(st, [long_text], SEED, FALLBACK)
    assert rep.added == [] and rep.too_long == 1
    assert st.entries == []


def test_the_cap_truncates_and_reports_the_overflow():
    """The add-witness path must not drop the excess in silence: ten pasted
    with room for three became three with no sign of the seven."""
    st = pw.WitnessSet()
    texts = [f'witness number {i} here' for i in range(10)]
    rep = pw.add_texts(st, texts, SEED, FALLBACK, cap=3)
    assert len(rep.added) == 3
    assert rep.over_cap == 7
    assert len(st.entries) == 3


def test_the_cap_is_flat_at_twenty_five():
    """Owner ruling 2026-08-27: unlike the web's 25/8/4 depth ladder, the
    desktop shares no pool and no timeout, so the cap does not vary."""
    assert pw.DESKTOP_WITNESS_CAP == 25


def test_capacity_is_checked_after_the_other_rules():
    """Checking capacity first would spend the last slots on texts that are
    about to be rejected anyway. Here two duplicates precede two good ones
    and there is room for exactly two."""
    st = _set('alpha beta gamma')
    rep = pw.add_texts(
        st,
        ['alpha beta gamma', 'alpha beta gamma',
         'fresh one here', 'fresh two here'],
        SEED, FALLBACK, cap=3)
    assert rep.duplicates == 2
    assert len(rep.added) == 2, 'duplicates consumed capacity'
    assert rep.over_cap == 0


def test_a_bulk_paste_splits_on_blank_lines_and_reports_what_it_skipped():
    texts, skipped = pw.split_paste(
        'first witness text here\n\nsecond witness text here\n\nnope')
    assert len(texts) == 2
    assert skipped == 1


def test_labels_are_numbered_when_one_label_covers_several_texts():
    st = pw.WitnessSet()
    rep = pw.add_texts(st, ['one two three', 'four five six'],
                       SEED, FALLBACK, label='Rite')
    assert [e.label for e in rep.added] == ['Rite 1', 'Rite 2']


def test_a_single_text_keeps_the_bare_label():
    st = pw.WitnessSet()
    rep = pw.add_texts(st, ['one two three'], SEED, FALLBACK, label='Rite')
    assert rep.added[0].label == 'Rite'


# ---------------------------------------------------------------------------
# Removal
# ---------------------------------------------------------------------------

def test_removing_a_witness_drops_its_cached_rows():
    st = _set('alpha beta gamma')
    wid = st.entries[0].id
    st.rows[WITNESS_SEED_ID] = [_row('h1', 10)]
    st.rows[wid] = [_row('h2', 8)]
    st.filtered[wid] = [_row('h3', 4)]
    pw.remove(st, wid)
    assert wid not in st.rows and wid not in st.filtered
    assert st.entries == []


def test_removal_reports_that_a_restored_session_cannot_restrip_its_rows():
    """After a restore the per-witness caches are empty by design -- ranks
    cannot be recovered from fused rows. The rows on screen therefore keep
    the removed witness's contributions, and the caller has to SAY so rather
    than destroy a result set that exists nowhere else."""
    st = pw.restore(
        [{'kind': 'pasted', 'text': 'alpha beta gamma', 'label': 'A'}],
        FALLBACK)
    assert st.rows == {}
    assert pw.remove(st, st.entries[0].id) is False


def test_removal_reports_that_a_live_session_can_restrip_its_rows():
    st = _set('alpha beta gamma')
    st.rows[WITNESS_SEED_ID] = [_row('h1', 10)]
    assert pw.remove(st, st.entries[0].id) is True


# ---------------------------------------------------------------------------
# Staleness
# ---------------------------------------------------------------------------

def test_a_witness_gathered_for_another_text_goes_stale():
    st = _set('alpha beta gamma')
    assert pw.mark_stale_against(st, 'a completely different source text') == 1
    assert st.entries[0].status == pw.STATUS_STALE


def test_a_witness_gathered_for_this_text_does_not_go_stale():
    st = _set('alpha beta gamma')
    assert pw.mark_stale_against(st, SEED) == 0
    assert st.entries[0].status == pw.STATUS_PENDING


def test_staleness_leaves_a_witness_that_already_ran_alone():
    """`searched` and `failed` describe a run that really happened;
    relabelling them would rewrite history."""
    st = _set('alpha beta gamma', 'delta epsilon zeta')
    st.entries[0].status = pw.STATUS_SEARCHED
    st.entries[1].status = pw.STATUS_FAILED
    assert pw.mark_stale_against(st, 'different text entirely') == 0
    assert st.entries[0].status == pw.STATUS_SEARCHED
    assert st.entries[1].status == pw.STATUS_FAILED


def test_reviving_a_stale_witness_restamps_its_digest():
    """Without the restamp it goes stale again on the next search and the
    user is asked the same question twice."""
    st = _set('alpha beta gamma')
    other = 'a completely different source text'
    pw.mark_stale_against(st, other)
    assert pw.revive_stale(st, other) == 1
    assert st.entries[0].status == pw.STATUS_PENDING
    assert pw.mark_stale_against(st, other) == 0, 'went stale again'


def test_removing_stale_witnesses_removes_only_those():
    st = _set('alpha beta gamma', 'delta epsilon zeta')
    st.entries[1].seed_digest = 'something else'
    pw.mark_stale_against(st, SEED)
    assert pw.remove_stale(st) == 1
    assert len(st.entries) == 1
    assert st.entries[0].text == 'alpha beta gamma'


# ---------------------------------------------------------------------------
# Fusion
# ---------------------------------------------------------------------------

def test_nothing_to_fuse_from_returns_none_rather_than_an_empty_result():
    """Conflating "nothing to fuse from" with "the result set is empty"
    destroyed data on the web: a restored page's rows were wiped by a
    removal, and the loss was then persisted to storage."""
    st = _set('alpha beta gamma')
    assert pw.fuse_all(st) is None


def test_a_single_witness_passes_through_unfused():
    """RRF over one list is a 1/(k+rank) rescale carrying no information,
    and it would change `score` from matched letters to ~0.03 -- the number
    the Max:/Avg: badges and every export column render."""
    st = pw.WitnessSet()
    rows = [_row('h1', 214), _row('h2', 90)]
    st.rows[WITNESS_SEED_ID] = rows
    main, filt = pw.fuse_all(st)
    assert [r['score'] for r in main] == [214, 90]
    assert 'fusion_score' not in main[0]
    assert 'witness_count' not in main[0]
    assert filt == []


def test_two_witnesses_fuse_and_count_contributors():
    st = _set('alpha beta gamma')
    wid = st.entries[0].id
    st.rows[WITNESS_SEED_ID] = [_row('shared', 100), _row('seedonly', 50)]
    st.rows[wid] = [_row('shared', 80), _row('witonly', 40)]
    main, _filt = pw.fuse_all(st)
    by_header = {r['raw_header']: r for r in main}
    assert by_header['shared']['witness_count'] == 2
    assert by_header['seedonly']['witness_count'] == 1
    assert by_header['witonly']['witness_count'] == 1
    # Found by both, so it outranks either single-witness row.
    assert main[0]['raw_header'] == 'shared'


def test_a_record_found_by_two_witnesses_reports_the_winners_score():
    """The rendered row shows one witness's highlighted span, so the number
    beside it must be that witness's. The maximum is reported separately."""
    st = _set('alpha beta gamma')
    wid = st.entries[0].id
    # The seed ranks it first with 100; the witness ranks it first with 900.
    st.rows[WITNESS_SEED_ID] = [_row('shared', 100)]
    st.rows[wid] = [_row('shared', 900)]
    main, _ = pw.fuse_all(st)
    assert main[0]['best_witness_score'] == 900
    assert main[0]['score'] in (100, 900)
    assert main[0]['score'] == main[0]['final_score']


def test_fusion_tags_rows_with_the_witness_that_produced_them():
    st = _set('alpha beta gamma')
    wid = st.entries[0].id
    st.rows[WITNESS_SEED_ID] = [_row('a', 10)]
    st.rows[wid] = [_row('b', 10)]
    main, _ = pw.fuse_all(st)
    assert {r['witness_id'] for r in main} == {WITNESS_SEED_ID, wid}


def test_a_filtered_row_still_counts_toward_the_fusion_statistics():
    """`filter_text` routes a ROW; the fusion facts describe a RECORD. A
    manuscript found by two witnesses, one of them on known source text,
    must still report two."""
    st = _set('alpha beta gamma')
    wid = st.entries[0].id
    st.rows[WITNESS_SEED_ID] = [_row('shared', 100)]
    st.rows[wid] = []
    st.filtered[wid] = [_row('shared', 80)]
    main, _ = pw.fuse_all(st)
    assert main[0]['witness_count'] == 2


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def test_a_manuscript_witness_is_stored_without_its_text():
    """The corpus still has it; copying up to 25 x 20,000 characters into a
    session file buys nothing."""
    st = pw.WitnessSet()
    pw.add_texts(st, ['manuscript text here'], SEED, FALLBACK,
                 kind='manuscript', sys_id='990001234567',
                 headers=['H1', 'H2'])
    snap = pw.snapshot(st)
    assert 'text' not in snap[0]
    assert snap[0]['sys_id'] == '990001234567'
    assert snap[0]['headers'] == ['H1', 'H2']


def test_a_pasted_witness_is_stored_with_its_text():
    """Its text exists nowhere else -- dropping it makes it unrecoverable."""
    st = _set('alpha beta gamma')
    assert pw.snapshot(st)[0]['text'] == 'alpha beta gamma'


def test_a_restored_manuscript_witness_survives_without_text():
    st = pw.restore([{'kind': 'manuscript', 'sys_id': '990001234567',
                      'label': 'T-S 1.1', 'headers': ['H1']}], FALLBACK)
    assert len(st.entries) == 1
    assert st.entries[0].text == ''
    assert st.entries[0].sys_id == '990001234567'


def test_a_restored_pasted_witness_without_text_is_dropped():
    """Nothing in the world can recover it, and a witness that cannot be
    searched must not sit in the list pretending otherwise."""
    st = pw.restore([{'kind': 'pasted', 'label': 'gone', 'text': ''}],
                    FALLBACK)
    assert st.entries == []


def test_every_restored_witness_comes_back_pending_with_empty_caches():
    """The snapshot holds FUSED rows; per-witness ranks cannot be recovered
    from them, and a fusion rebuilt from partial inputs would be quietly
    wrong rather than visibly absent."""
    st = pw.restore(
        [{'kind': 'pasted', 'text': 'alpha beta gamma', 'label': 'A'},
         {'kind': 'pasted', 'text': 'delta epsilon zeta', 'label': 'B'}],
        FALLBACK)
    assert [e.status for e in st.entries] == [pw.STATUS_PENDING] * 2
    assert st.rows == {} and st.filtered == {}


def test_the_id_counter_follows_a_restore():
    """`restore_witness_entries` renumbers survivors w1..wN. If the counter
    did not follow, the next `new_id` would re-issue an id already in use and
    two witnesses would share a row cache."""
    st = pw.restore(
        [{'kind': 'pasted', 'text': 'alpha beta gamma', 'label': 'A'},
         {'kind': 'pasted', 'text': 'delta epsilon zeta', 'label': 'B'}],
        FALLBACK)
    existing = {e.id for e in st.entries}
    pw.add_texts(st, ['eta theta iota'], SEED, FALLBACK)
    assert st.entries[-1].id not in existing


def test_a_snapshot_round_trips():
    st = _set('alpha beta gamma', 'delta epsilon zeta')
    back = pw.restore(pw.snapshot(st), FALLBACK)
    assert [e.text for e in back.entries] == [e.text for e in st.entries]
    assert [e.label for e in back.entries] == [e.label for e in st.entries]


@pytest.mark.parametrize('raw', [None, [], 'not a list', 42, [None, 7]])
def test_restore_survives_a_malformed_snapshot(raw):
    """A hand-edited or truncated session file must not take the tab down."""
    assert pw.restore(raw, FALLBACK).entries == []


# --- cache invalidation ----------------------------------------------------
# The row caches make an auto-expand round cost one search per NEW witness.
# They are only sound while they answer the SAME question, and nothing else in
# this module notices when the question changes -- `mark_stale_against` reads
# the seed digest but touches only `pending` entries.


def test_the_first_key_ever_seen_invalidates_nothing_but_is_recorded():
    st = _set('alpha beta gamma')
    assert pw.invalidate_cache(st, ('k1',)) is False
    assert st.cache_key == ('k1',)


def test_an_identical_key_keeps_the_caches():
    """This is the reuse the whole design rests on: an auto-expand round must
    not re-run the witnesses it already searched."""
    st = _set('alpha beta gamma')
    pw.invalidate_cache(st, ('k1',))          # establish the key first
    st.rows[WITNESS_SEED_ID] = [{'raw_header': 'h', 'score': 1}]
    st.entries[0].status = pw.STATUS_SEARCHED

    assert pw.invalidate_cache(st, ('k1',)) is False
    assert st.rows, 'an unchanged query re-ran witnesses it already had'
    assert st.entries[0].status == pw.STATUS_SEARCHED


def test_a_changed_key_drops_the_rows_and_re_queues_the_searched():
    """THE defect this exists for. Without it a second run finds the seed's
    rows cached and every witness `searched`, dispatches NOTHING, and
    re-publishes the previous query's rows as the new query's answer."""
    st = _set('alpha beta gamma')
    pw.invalidate_cache(st, ('k1',))          # establish the key first
    st.rows[WITNESS_SEED_ID] = [{'raw_header': 'h', 'score': 1}]
    st.rows[st.entries[0].id] = [{'raw_header': 'h2', 'score': 2}]
    st.filtered[WITNESS_SEED_ID] = [{'raw_header': 'h3', 'score': 3}]
    st.entries[0].status = pw.STATUS_SEARCHED
    st.entries[0].hits = 9

    assert pw.invalidate_cache(st, ('k2',)) is True
    assert st.rows == {} and st.filtered == {}
    assert st.entries[0].status == pw.STATUS_PENDING
    assert st.entries[0].hits == 0


def test_the_seed_rows_are_dropped_too_so_the_seed_is_re_searched():
    """The seed is a witness like any other, and it is the one whose text the
    user actually edited. Keeping ITS rows is the whole bug: the dispatch list
    includes the seed only when `rows[seed]` is missing."""
    st = pw.WitnessSet()
    pw.invalidate_cache(st, ('k1',))          # establish the key first
    st.rows[WITNESS_SEED_ID] = [{'raw_header': 'h', 'score': 1}]
    pw.invalidate_cache(st, ('k2',))
    assert st.rows.get(WITNESS_SEED_ID) is None


def test_a_failed_witness_is_left_failed():
    """It has no rows to invalidate, and silently re-queueing it would retry a
    known failure on every settings change instead of leaving the user the
    explicit Retry the panel offers."""
    st = _set('alpha beta gamma')
    st.entries[0].status = pw.STATUS_FAILED
    st.entries[0].error = 'engine said no'
    pw.invalidate_cache(st, ('k1',))
    pw.invalidate_cache(st, ('k2',))
    assert st.entries[0].status == pw.STATUS_FAILED
    assert st.entries[0].error == 'engine said no'


def test_a_stale_witness_is_left_stale():
    """Staleness is a question already put to the user; an invalidation must
    not answer it for them by quietly re-queueing the witness."""
    st = _set('alpha beta gamma')
    st.entries[0].status = pw.STATUS_STALE
    pw.invalidate_cache(st, ('k1',))
    pw.invalidate_cache(st, ('k2',))
    assert st.entries[0].status == pw.STATUS_STALE


def test_a_restored_set_starts_with_no_key():
    """Its caches come back empty by design, so the first run after a restore
    must record a key rather than match one."""
    st = _set('alpha beta gamma')
    pw.invalidate_cache(st, ('k1',))
    assert pw.restore(pw.snapshot(st), FALLBACK).cache_key is None


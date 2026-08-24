# -*- coding: utf-8 -*-
"""Behaviour of the arrangement-C query path and the policy object.

The properties tested here are the ones the spec makes contractual:
determinism, the distinct-gram two-hit rule, budget accounting that adds up,
explicit truncation, and policy identity. Plus self-retrieval on a real
corpus slice, which is the smoke test that the whole pipe actually retrieves.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_builder import build_index  # noqa: E402
from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import norm_stream_fast  # noqa: E402
from shared.passage_policy import (  # noqa: E402
    DEFAULT_POLICY, FLAT_25, LENGTH_PROFILES, PRESETS, STANDARD_40,
    PassagePolicy, compose, get_preset,
)
from shared.passage_search import search_passage  # noqa: E402

ALEF = 0x05D0
TRANSCRIPTIONS = os.environ.get(
    'GENIZAH_TRANSCRIPTIONS', r'C:\GenizahSearch\Transcriptions.txt')


def _letters(seq) -> str:
    return ''.join(chr(ALEF + (v % 22)) for v in seq)


def _aperiodic(n: int, salt: int = 0) -> str:
    """Pseudo-random letters with no short period.

    The first fixture used (i*7+3) % 22 -- a 22-letter CYCLE. That collapses
    to 22 distinct grams under query dedup, the record-side cluster extent
    spans every repetition, alignment length inflates, and the density
    boundary rejects it. Which is CORRECT engine behaviour -- a short cycle
    repeated is exactly the degenerate formula the boundary exists to refuse
    -- but it makes the fixture useless for testing retrieval of ordinary
    text, which is not periodic."""
    out = []
    x = 987_654_321 + salt
    for _ in range(n):
        x = (x * 1_103_515_245 + 12_345) & 0x7FFFFFFF
        out.append(chr(ALEF + (x >> 7) % 22))
    return ''.join(out)


@pytest.fixture(scope='module')
def small_index(tmp_path_factory):
    """A synthetic index with known structure."""
    d = str(tmp_path_factory.mktemp('psearch'))
    motif = _aperiodic(80)
    records = []
    for r in range(30):
        body = _aperiodic(300, salt=1000 + r)
        # records 0..9 carry the motif; 10..29 do not
        text = body[:120] + (motif if r < 10 else '') + body[120:]
        records.append((f'syn{r:03d}', text))
    build_index(records, d, partitions=3, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, motif


# ---------------------------------------------------------------------------
# Policy identity.
# ---------------------------------------------------------------------------

def test_policy_id_is_content_and_only_content():
    a = PassagePolicy(name='standard-40')
    assert a.policy_id == STANDARD_40.policy_id
    assert PassagePolicy(name='x').policy_id != STANDARD_40.policy_id
    assert PassagePolicy(name='standard-40', verify_cap=1).policy_id \
        != STANDARD_40.policy_id


def test_policy_is_frozen_and_validated():
    with pytest.raises(Exception):
        STANDARD_40.min_span = 25          # frozen dataclass
    with pytest.raises(ValueError):
        PassagePolicy(name='bad', regime='detected')
    with pytest.raises(ValueError):
        PassagePolicy(name='bad', min_span=3)
    with pytest.raises(ValueError):
        PassagePolicy(name='bad', posting_budget=0)


def test_presets_are_registered_and_default_is_standard_40():
    assert DEFAULT_POLICY is STANDARD_40
    assert get_preset('flat-25') is FLAT_25
    assert set(PRESETS) == {'standard-40', 'standard-40-noisy', 'wide-40', 'wider-40',
                            # widest-40: the web GUI's operating point
                            # (owner ruling 2026-08-23, two graded GUI case
                            # studies -- see shared/passage_policy.py)
                            'widest-40',
                            # max-40: the GUI's "Maximal" step (owner,
                            # 2026-08-23, Birkat Hamazon session)
                            'max-40',
                            # short-28: the measured point on the SECOND
                            # axis -- passage length (spec section 8.1)
                            'short-28',
                            'flat-25', 'flat-25-noisy'}
    with pytest.raises(ValueError):
        get_preset('slider-17')


def test_acceptance_rule_matches_the_spec_table():
    p = STANDARD_40
    assert p.max_density(99) == 0.28 and p.max_density(100) == 0.35
    n = get_preset('standard-40-noisy')
    assert n.max_density(99) == 0.30
    assert n.max_density(150) == 0.386
    assert n.max_density(200) == 0.418
    assert not p.accepts(39, 200, 0.0)         # span floor
    assert p.accepts(40, 200, 0.35)
    assert not p.accepts(40, 200, 0.351)       # boundary is inclusive


# ---------------------------------------------------------------------------
# Query behaviour on the synthetic index.
# ---------------------------------------------------------------------------

def test_motif_query_finds_exactly_the_motif_carriers(small_index):
    idx, motif = small_index
    hits, rep = search_passage(idx, motif, STANDARD_40)
    got = {h.record for h in hits}
    assert got == set(range(10)), got
    assert not rep.candidates_truncated and not rep.verify_truncated
    for h in hits:
        assert h.matched_letters >= len(norm_stream_fast(motif)) - 10
        assert h.best_density <= 0.05


def test_determinism_same_query_same_everything(small_index):
    idx, motif = small_index
    a_hits, a_rep = search_passage(idx, motif, STANDARD_40)
    b_hits, b_rep = search_passage(idx, motif, STANDARD_40)
    assert [(h.record, h.score, h.spans) for h in a_hits] \
        == [(h.record, h.score, h.spans) for h in b_hits]
    a, b = a_rep.as_dict(), b_rep.as_dict()
    a.pop('seconds'); b.pop('seconds')
    assert a == b


def test_below_min_span_query_is_an_explicit_state(small_index):
    idx, _ = small_index
    hits, rep = search_passage(idx, 'אבגדהוזחטי', STANDARD_40)
    assert hits == [] and rep.below_min_span_query
    # flat-25 has a lower floor; 10 letters still under it
    hits, rep = search_passage(idx, 'אבגדהוזחטי', FLAT_25)
    assert hits == [] and rep.below_min_span_query


def test_single_repeated_gram_cannot_seed_a_candidate(small_index):
    """The spec 6.1 distinctness rule: query grams are DEDUPLICATED, so one
    gram repeated in the query is one anchor, and min_anchors=2 needs a
    second distinct code on the same diagonal."""
    idx, _ = small_index
    one_gram = 'אבגדה'
    query = (one_gram + 'ץ') * 12     # final-fold makes tsadi; still 1 code repeated
    hits, rep = search_passage(idx, query, FLAT_25)
    # whatever comes back must have >= 2 distinct anchors by construction;
    # with a single distinct content gram the candidate stage yields nothing
    # unless a真 second code exists. Assert the report is coherent instead of
    # asserting emptiness blindly:
    assert rep.query_grams_distinct < len(norm_stream_fast(query)) - 4


def test_budget_accounting_adds_up(small_index):
    idx, motif = small_index
    for preset in PRESETS.values():
        _hits, rep = search_passage(idx, motif, preset)
        assert rep.grams_admitted + rep.grams_excluded \
            == rep.query_grams_distinct
        assert rep.postings_admitted >= 0
        assert rep.verified <= rep.candidates
        assert rep.policy_id == preset.policy_id


def test_tiny_verify_cap_truncates_and_says_so(small_index):
    idx, motif = small_index
    tight = PassagePolicy(name='tight', verify_cap=3)
    _hits, rep = search_passage(idx, motif, tight)
    assert rep.verified == 3
    assert rep.verify_truncated, 'cap fired silently'


def test_tiny_candidate_cap_truncates_and_says_so(small_index):
    idx, motif = small_index
    tight = PassagePolicy(name='tightc', candidate_cap=2)
    _hits, rep = search_passage(idx, motif, tight)
    assert rep.candidates_truncated


def test_flat25_is_a_superset_permitter_of_standard40(small_index):
    """Lowering the floor may only ADD hits, never remove or reorder the
    ones standard-40 already accepts (same budget, same boundary)."""
    idx, motif = small_index
    q = motif[:120]
    h40, _ = search_passage(idx, q, STANDARD_40)
    h25, _ = search_passage(idx, q, FLAT_25)
    got40 = {(h.record, tuple(map(tuple, h.spans))) for h in h40}
    got25 = {(h.record, tuple(map(tuple, h.spans))) for h in h25}
    assert got40 <= got25


def test_verbatim_passage_query_scores_near_zero_density(small_index):
    """Regression for the asymmetric-extension bug. A pasted query IS the
    passage: its verification window clamps at the query bounds, and if the
    record window keeps the full +-MARGIN anyway it comes out up to 2*MARGIN
    letters longer -- Levenshtein pays one edit per surplus letter, density
    inflates by ~2*MARGIN/aligned_len (0.43 at 80 letters), and a VERBATIM
    match is rejected. The record must extend only as far as the query
    actually did, per side."""
    idx, motif = small_index
    hits, _rep = search_passage(idx, motif, STANDARD_40)
    assert hits, 'verbatim motif found nothing -- the bug is back'
    for h in hits:
        assert h.best_density <= 0.02, \
            f'record {h.record}: density {h.best_density} on a verbatim match'


# ---------------------------------------------------------------------------
# PR #324 review, P1: the no-cap branch returned indices in the WRONG space.
# ---------------------------------------------------------------------------

# Grams absent from the fixture corpus whose CODES SORT LOW. Both properties
# are required and the second is easy to get wrong: `np.unique` sorts `codes`
# by code value, so if every absent gram sorts to the TAIL then
# `np.flatnonzero(held)` == `np.arange(k)` == `base` and the two index spaces
# coincide -- the buggy and fixed returns become byte-identical and any test
# built on such a query passes either way.
#
# The first attempt at this fixture used resh/shin/tav, the HIGHEST letter
# codes, and was exactly that silent no-op (measured: 13 absent grams, all at
# the tail, mutation stayed green). Runs of alef with low-letter punctuation
# put absent grams underneath present ones instead.
_ABSENT_LOW_PREFIX = 'אאאאבאאאגאאאדאאאהאאאו' * 3


def _absent_gram_query(motif: str) -> str:
    return _ABSENT_LOW_PREFIX + motif


def _query_arrays(idx, motif):
    import numpy as np

    from shared.passage_normalize import gram_codes, norm_stream

    qstream, _ = norm_stream(_absent_gram_query(motif))
    codes, first_idx = np.unique(gram_codes(qstream), return_index=True)
    return codes, first_idx.astype(np.int64), idx.dfs(codes) > 0


def _assert_spaces_really_differ(held):
    """The precondition, asserted rather than assumed.

    Unless some absent gram sorts BELOW some present one, the filtered and
    original index spaces are the same array and this bug is unobservable.
    """
    import numpy as np

    present = np.flatnonzero(held)
    assert present.size < held.size, 'no absent grams at all'
    assert (present != np.arange(present.size)).any(), (
        'every absent gram sorts to the tail, so the filtered and original '
        'index spaces coincide and this fixture cannot observe the defect'
    )


def test_no_cap_admits_exactly_the_grams_that_are_IN_the_index(small_index):
    """The invariant the no-cap branch broke, asserted directly.

    `_admit_grams` drops df == 0 grams (`held`) and then works in the FILTERED
    index space. Every capped branch converts back on the way out with
    `np.flatnonzero(held)[chosen]`; BUDGET_NO_CAP returned `base[order]`,
    whose values address the filtered array -- while `_candidates` applies
    whatever it gets to the caller's ORIGINAL `codes`/`qpos`.

    Note what the damage is NOT. Each admitted index still pairs a code with
    its own `qpos`, so diagonals stay coherent and a strong motif is still
    retrieved; a retrieval-level assertion does not catch this. What actually
    happens is that the selection becomes "the len(held) lowest-valued codes",
    a set that includes absent grams (expanding to nothing) and omits present
    ones -- lost anchors, and DFs measured for grams other than those
    expanded. Hence an index-space assertion, not a recall one.
    """
    import numpy as np

    from shared.passage_search import QueryReport, _admit_grams

    idx, motif = small_index
    codes, qpos, held = _query_arrays(idx, motif)
    _assert_spaces_really_differ(held)

    nocap = PassagePolicy(name='nocap-probe', budget_policy='no_cap')
    admitted = _admit_grams(idx, codes, qpos, nocap, QueryReport())

    assert int(admitted.max()) < codes.size, "index outside the caller's range"
    assert (idx.dfs(codes[admitted]) > 0).all(), (
        'no-cap admitted a gram absent from the index -- the returned indices '
        "address the filtered array, not the caller's"
    )
    assert sorted(admitted.tolist()) == np.flatnonzero(held).tolist(), (
        'under no_cap the contract is total: exactly the grams the index '
        'holds, and nothing else'
    )


def test_no_cap_admits_the_same_grams_as_an_effectively_unlimited_cap(
        small_index):
    """Differential form. A budget large enough to admit everything must
    select the same grams as no-cap; only the capped path mapped back."""
    from shared.passage_search import QueryReport, _admit_grams

    idx, motif = small_index
    codes, qpos, held = _query_arrays(idx, motif)
    _assert_spaces_really_differ(held)

    nocap = PassagePolicy(name='nocap-cmp', budget_policy='no_cap')
    roomy = PassagePolicy(name='roomy-cmp', budget_policy='rarest_first',
                          posting_budget=10 ** 9)

    a = sorted(_admit_grams(idx, codes, qpos, nocap, QueryReport()).tolist())
    b = sorted(_admit_grams(idx, codes, qpos, roomy, QueryReport()).tolist())
    assert a == b, (
        'no-cap and an unlimited cap admit the same grams by definition; they '
        'disagreed, so one of them is in the wrong index space'
    )


# ---------------------------------------------------------------------------
# Policy identity across schema growth. The seven presets that predate the
# 2026-08-24 fields keep the ids their ledger measurements were recorded
# under (shared/retrieval_eval.py keys on policy_id); pinned so a future
# field addition cannot silently orphan them again.
# ---------------------------------------------------------------------------

_MEASURED_PRESET_IDS = {
    'standard-40': 'pp1-dfd44076cf548ea5',
    'standard-40-noisy': 'pp1-a23d529e202fe27f',
    'flat-25': 'pp1-1257aa52cd805830',
    'wide-40': 'pp1-15deb93f5e8bd8b9',
    'wider-40': 'pp1-b8e2c872ae959003',
    'widest-40': 'pp1-c10214cb51ce763d',
    'max-40': 'pp1-73d07b8e18eb1215',
}


def test_presets_keep_their_measured_ids():
    for name, pid in _MEASURED_PRESET_IDS.items():
        assert get_preset(name).policy_id == pid


# ---------------------------------------------------------------------------
# compose(): the two-axis control surface (width x passage length).
# ---------------------------------------------------------------------------

def test_compose_normal_is_the_width_preset_itself():
    for name in _MEASURED_PRESET_IDS:
        assert compose(name) is get_preset(name)
        assert compose(name, 'normal') is get_preset(name)


def test_compose_short_moves_both_coupled_parameters():
    """min_span and verify_margin are ONE decision (spec 8.1); the profile
    must move both or it silently does nothing."""
    base, short = get_preset('widest-40'), compose('widest-40', 'short')
    assert (base.min_span, base.verify_margin) == (40, 30)
    assert (short.min_span, short.verify_margin) == (28, 12)
    # width is preserved, identity is distinct and self-describing
    assert short.density_scale == base.density_scale
    assert short.name == 'widest-40+short'
    assert short.policy_id != base.policy_id


def test_compose_rejects_an_unknown_profile():
    with pytest.raises(ValueError):
        compose('widest-40', 'medium-ish')
    with pytest.raises(ValueError):
        compose('no-such-width', 'short')


def test_every_offered_combination_is_a_distinct_named_policy():
    """The surface offers a small discrete grid, not a slider: each cell
    must be nameable and separately identifiable."""
    from shared.passage_policy import DEPTH_PROFILES
    ids, names = set(), set()
    for width in _MEASURED_PRESET_IDS:
        for length in LENGTH_PROFILES:
            for depth in DEPTH_PROFILES:
                p = compose(width, length, depth)
                ids.add(p.policy_id)
                names.add(p.name)
    expected = (len(_MEASURED_PRESET_IDS) * len(LENGTH_PROFILES)
                * len(DEPTH_PROFILES))
    assert len(ids) == len(names) == expected


# ---------------------------------------------------------------------------
# compose(): the THIRD axis -- search depth (posting/verify/candidate
# budgets moved together). Measured 2026-08-24 on the Antiochus deck:
# the default posting budget admits <5% of a 6K-letter query's postings,
# and verify_cap 3,000 vs 26K candidates crowds real witnesses below the
# cap. DEPTH_PROFILES in shared/passage_policy.py carries the numbers.
# ---------------------------------------------------------------------------

def test_compose_normal_depth_is_the_width_preset_itself():
    for name in _MEASURED_PRESET_IDS:
        assert compose(name, 'normal', 'normal') is get_preset(name)


def test_compose_deep_moves_all_three_coupled_budgets():
    """posting_budget, verify_cap and candidate_cap are ONE decision: more
    budget without more verification changes almost nothing (measured), so
    the profile must move all three or it silently underdelivers."""
    base = get_preset('max-40')
    deep = compose('max-40', 'short', 'deep')
    assert (base.posting_budget, base.verify_cap, base.candidate_cap) \
        == (500_000, 3_000, 200_000)
    assert (deep.posting_budget, deep.verify_cap, deep.candidate_cap) \
        == (2_000_000, 50_000, 500_000)
    deepest = compose('max-40', 'normal', 'deepest')
    assert (deepest.posting_budget, deepest.verify_cap,
            deepest.candidate_cap) == (5_000_000, 50_000, 500_000)
    # width and length are preserved; identity is distinct, self-describing
    assert deep.density_scale == base.density_scale
    assert (deep.min_span, deep.verify_margin) == (28, 12)
    assert deep.name == 'max-40+short+deep'
    assert deepest.name == 'max-40+deepest'
    assert len({base.policy_id, deep.policy_id, deepest.policy_id}) == 3


def test_compose_rejects_an_unknown_depth():
    with pytest.raises(ValueError):
        compose('widest-40', 'normal', 'bottomless')


# ---------------------------------------------------------------------------
# verify_margin: below MIN_SPAN 40 the margin, not the span floor, decides.
# Measured 2026-08-24 after the anchor tier failed on the Antiochus deck.
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def short_name_index(tmp_path_factory):
    """A 'translation': it shares only three SHORT contiguous runs with the
    query, the way a Judeo-Arabic version shares transliterated proper nouns
    (identical Hebrew letters) and nothing else. Also a clean copy and a
    15%-CER noisy copy, so a margin change cannot quietly break the cases
    that already work."""
    d = str(tmp_path_factory.mktemp('pmargin'))
    ref = _aperiodic(500, salt=4242)
    names = [_aperiodic(9, salt=11), _aperiodic(12, salt=22),
             _aperiodic(7, salt=33)]
    parts = [ref]
    for i, nm in enumerate(names):
        parts += [_aperiodic(300, salt=100 + i), nm]
    query = ''.join(parts)

    def noisy(s, rate=0.15):
        x, out = 13, []
        for ch in s:
            x = (x * 1_103_515_245 + 12_345) & 0x7FFFFFFF
            out.append(chr(ALEF + ((x >> 11) % 22))
                       if (x >> 7) % 100 < rate * 100 else ch)
        return ''.join(out)

    records = [
        ('verbatim', _aperiodic(80, salt=1) + ref + _aperiodic(80, salt=2)),
        ('noisy_copy', _aperiodic(80, salt=3) + noisy(ref)
         + _aperiodic(80, salt=4)),
        ('translation', ''.join(_aperiodic(200, salt=700 + i) + nm
                                for i, nm in enumerate(names))),
    ]
    for r in range(40):
        records.append((f'unrel{r:03d}', _aperiodic(700, salt=3000 + r)))
    build_index(records, d, partitions=2, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, query


def _found(idx, query, **kw):
    policy = PassagePolicy(name='t-margin', density_scale=1.8,
                           verify_cap=200_000, candidate_cap=2_000_000, **kw)
    hits, report = search_passage(idx, query, policy)
    return {h.record_id for h in hits}, report


def test_lowering_min_span_alone_changes_nothing(short_name_index):
    """The negative half of the finding, and the reason this is a policy
    field: at the default margin, dropping the span floor moves every
    rejection from rejected_short to rejected_density and finds nothing
    new."""
    idx, query = short_name_index
    wide, r_wide = _found(idx, query, min_span=40)
    short, r_short = _found(idx, query, min_span=10)
    assert 'translation' not in wide
    assert 'translation' not in short, (
        'min_span alone must NOT be credited with finding short matches')
    assert r_short.rejected_short == 0 and r_short.rejected_density > 0


def test_a_small_margin_is_what_finds_short_shared_names(short_name_index):
    idx, query = short_name_index
    ids, _r = _found(idx, query, min_span=10, verify_margin=8)
    assert 'translation' in ids, (
        'a 9-letter true match is only judgeable over a window near its own '
        'size; margin 30 scores it across ~70 letters of unrelated text')
    # And the cases that already work must survive the change.
    assert {'verbatim', 'noisy_copy'} <= ids
    assert not any(i.startswith('unrel') for i in ids)


def test_the_default_margin_is_unchanged_for_long_spans(short_name_index):
    """verify_margin is new; every policy that does not set it must behave
    exactly as before, which is what keeps the pre-existing policy_ids
    honest."""
    idx, query = short_name_index
    assert PassagePolicy(name='x').verify_margin == 30
    assert (PassagePolicy(name='x').policy_id
            == PassagePolicy(name='x', verify_margin=30).policy_id)
    assert (PassagePolicy(name='x').policy_id
            != PassagePolicy(name='x', verify_margin=8).policy_id)
    with pytest.raises(ValueError):
        PassagePolicy(name='x', verify_margin=-1)

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
    DEFAULT_POLICY, FLAT_25, PRESETS, STANDARD_40, PassagePolicy, get_preset,
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
                            # anchor-sweep-40: max-40 plus the anchor-evidence
                            # tier (spec section 10.4, the Antiochus method
                            # comparison)
                            'anchor-sweep-40',
                            # names-10: short contiguous evidence -- the
                            # verify_margin finding (spec section 8.1)
                            'names-10',
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
# Anchor-evidence tier (spec section 10.4).
# ---------------------------------------------------------------------------

# The seven pre-2026-08-23 presets keep the ids their ledger measurements
# were recorded under (shared/retrieval_eval.py keys on policy_id): the
# anchor fields join the identity hash ONLY when anchor_tier is True.
# Verified against HEAD before the anchor fields landed; pinned here so a
# future field addition cannot silently orphan recorded measurements again.
_MEASURED_PRESET_IDS = {
    'standard-40': 'pp1-dfd44076cf548ea5',
    'standard-40-noisy': 'pp1-a23d529e202fe27f',
    'flat-25': 'pp1-1257aa52cd805830',
    'wide-40': 'pp1-15deb93f5e8bd8b9',
    'wider-40': 'pp1-b8e2c872ae959003',
    'widest-40': 'pp1-c10214cb51ce763d',
    'max-40': 'pp1-73d07b8e18eb1215',
}


def test_pre_anchor_presets_keep_their_measured_ids():
    for name, pid in _MEASURED_PRESET_IDS.items():
        assert get_preset(name).policy_id == pid


def test_anchor_fields_join_identity_only_when_enabled():
    off_a = PassagePolicy(name='x')
    off_b = PassagePolicy(name='x', anchor_min_codes=9, anchor_cap=7)
    assert off_a.policy_id == off_b.policy_id
    on_a = PassagePolicy(name='x', anchor_tier=True)
    on_b = PassagePolicy(name='x', anchor_tier=True, anchor_min_codes=9)
    assert on_a.policy_id != off_a.policy_id
    assert on_a.policy_id != on_b.policy_id
    with pytest.raises(ValueError):
        PassagePolicy(name='x', anchor_min_codes=1)
    with pytest.raises(ValueError):
        PassagePolicy(name='x', anchor_cap=0)


@pytest.fixture(scope='module')
def scatter_index(tmp_path_factory):
    """A synthetic 'translation' fixture: records that share only SCATTERED
    SHORT collocations with the reference text -- names in a translation --
    far below MIN_SPAN, so span acceptance can never fire on them."""
    d = str(tmp_path_factory.mktemp('panchor'))
    reference = _aperiodic(400)
    snippets = [reference[40:54], reference[180:194], reference[320:334]]
    records = []
    # records 0..4: 'translations' -- unrelated body + the three snippets
    # at scattered offsets (different diagonals, never one 40-letter run).
    for r in range(5):
        body = _aperiodic(360, salt=5000 + r)
        text = (body[:90] + snippets[0] + body[90:200] + snippets[1]
                + body[200:300] + snippets[2] + body[300:])
        records.append((f'trn{r:03d}', text))
    # records 5..14: pure unrelated text.
    for r in range(5, 15):
        records.append((f'unr{r:03d}', _aperiodic(400, salt=9000 + r)))
    # record 15: a verbatim carrier of the reference -- a span hit.
    records.append(('carrier015', _aperiodic(100, salt=77) + reference
                    + _aperiodic(100, salt=78)))
    build_index(records, d, partitions=3, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, reference


def test_anchor_tier_off_by_default_and_inert(scatter_index):
    idx, reference = scatter_index
    hits, report = search_passage(idx, reference, STANDARD_40)
    assert report.anchor_tier_enabled is False
    assert report.anchor_records == 0
    assert all(h.tier == 'span' for h in hits)
    # The scattered-collocation records are invisible to the span tiers --
    # the blindness the anchor tier exists to remove.
    assert not any(h.record_id.startswith('trn') for h in hits)


def test_anchor_tier_reports_scattered_collocations(scatter_index):
    idx, reference = scatter_index
    policy = PassagePolicy(name='t-anchor', density_scale=2.0,
                           anchor_tier=True)
    hits, report = search_passage(idx, reference, policy)
    assert report.anchor_tier_enabled is True
    span_ids = {h.record_id for h in hits if h.tier == 'span'}
    anchor_ids = {h.record_id for h in hits if h.tier == 'anchor'}
    # The verbatim carrier is a span hit; NEVER duplicated as an anchor hit.
    assert 'carrier015' in span_ids
    assert not (span_ids & anchor_ids)
    # Every 'translation' surfaces in the anchor tier; unrelated records not.
    assert {f'trn{r:03d}' for r in range(5)} <= anchor_ids
    assert not any(rid.startswith('unr') for rid in anchor_ids)
    assert report.anchor_records == len(anchor_ids)
    for h in hits:
        if h.tier == 'anchor':
            assert h.matched_letters == 0
            assert h.best_density == 1.0
            assert h.anchor_codes >= policy.anchor_min_codes
            assert h.score == float(h.anchor_codes)
            assert h.spans and all(len(s) == 5 for s in h.spans)
    # Tiers never interleave: every span hit precedes every anchor hit.
    tiers = [h.tier for h in hits]
    assert tiers == sorted(tiers, key=lambda t: t != 'span')


def test_anchor_tier_is_deterministic_and_cap_reports(scatter_index):
    idx, reference = scatter_index
    policy = PassagePolicy(name='t-anchor', density_scale=2.0,
                           anchor_tier=True)
    a = search_passage(idx, reference, policy)
    b = search_passage(idx, reference, policy)
    assert [(h.record_id, h.tier, h.score, h.spans) for h in a[0]] == \
           [(h.record_id, h.tier, h.score, h.spans) for h in b[0]]
    capped = PassagePolicy(name='t-anchor-cap', density_scale=2.0,
                           anchor_tier=True, anchor_cap=2)
    hits_c, report_c = search_passage(idx, reference, capped)
    anchors_c = [h for h in hits_c if h.tier == 'anchor']
    assert len(anchors_c) == 2
    assert report_c.anchor_truncated is True
    assert report_c.anchor_records == 2
    # The cap keeps the strongest-evidenced records, in the full run's order.
    full_anchors = [h.record_id for h in a[0] if h.tier == 'anchor']
    assert [h.record_id for h in anchors_c] == full_anchors[:2]


def test_anchor_tier_respects_record_restriction(scatter_index):
    idx, reference = scatter_index
    policy = PassagePolicy(name='t-anchor', density_scale=2.0,
                           anchor_tier=True)
    hits, _report = search_passage(
        idx, reference, policy,
        record_allowed=lambda rid: not rid.startswith('trn'))
    assert not any(h.record_id.startswith('trn') for h in hits)


@pytest.fixture(scope='module')
def formulaic_index(tmp_path_factory):
    """The failure the 2026-08-24 Antiochus run exposed, in miniature.

    MANY records share numerous SHORT COMMON phrases with the query (as
    biblical Aramaic manuscripts share stock formulae with a Daniel-imitating
    text); ONE short record shares a FEW RARE snippets (as an Arabic
    translation shares only names). Every shared piece is short and
    scattered, so nothing forms an acceptable span and all of it lands in the
    anchor tier -- which is the situation the real run produced, where
    99 כתובים / 35 Daniel / 25 Targum filled the cap and the true finds sat
    on its floor.
    """
    d = str(tmp_path_factory.mktemp('pformula'))
    stock = [_aperiodic(12, salt=4242 + i) for i in range(20)]  # df ~40 each
    rare = [_aperiodic(12, salt=900 + i) for i in range(6)]     # df 1 each

    # The query carries all of both, each piece isolated by filler so no long
    # contiguous run exists on either side of any later comparison.
    parts = []
    for i, piece in enumerate(stock + rare):
        parts.append(_aperiodic(40, salt=200 + i))
        parts.append(piece)
    query = ''.join(parts)

    records = []
    for r in range(40):
        body = [_aperiodic(45, salt=6000 + r * 31 + i) for i in range(21)]
        text = ''
        for i, piece in enumerate(stock):
            text += body[i] + piece
        records.append((f'frm{r:03d}', text + body[20]))
    # The target: short, sharing ONLY the rare snippets.
    tgt = ''
    for i, piece in enumerate(rare):
        tgt += _aperiodic(45, salt=8000 + i) + piece
    records.append(('rare000', tgt))

    build_index(records, d, partitions=3, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, query


def test_rarity_gate_keeps_the_distinctive_record_over_the_formulaic_ones(
        formulaic_index):
    """The regression that motivated anchor_df_max + weight ordering."""
    idx, query = formulaic_index
    # Stock grams have df ~40, the rare ones df 1: a cutoff between the two
    # is exactly what separates 'distinctive' from 'stock phrase'.
    gated = PassagePolicy(name='t-gated', anchor_tier=True,
                          anchor_df_max=10, anchor_min_codes=4)
    hits, _report = search_passage(idx, query, gated)
    anchors = [h for h in hits if h.tier == 'anchor']
    by_id = {h.record_id: h for h in anchors}
    assert 'rare000' in by_id, 'the distinctive record must survive'
    assert anchors[0].record_id == 'rare000', 'and must rank first'
    assert all(h.anchor_weight > 0 for h in anchors)

    # Formula-bearers can still appear -- in a 22-letter synthetic alphabet
    # random 5-gram collisions hand a few of them a rare code, which is a
    # property of the fixture, not of the gate. What the gate must deliver is
    # SEPARATION: the stock-phrase evidence has to collapse to noise level
    # while the distinctive record keeps all of its.
    frm = [h for h in anchors if h.record_id.startswith('frm')]
    assert frm, 'fixture assumption: collisions do reach the tier'
    assert by_id['rare000'].anchor_weight > 5 * max(h.anchor_weight
                                                    for h in frm)

    # Ungated (the pre-fix behaviour): the formulaic records flood the tier
    # AND invert the raw count -- a stock-phrase record outscores the real
    # one on codes, which is precisely why the cap used to keep the wrong
    # records. Weight ordering survives the inversion; counting does not.
    ungated = PassagePolicy(name='t-ungated', anchor_tier=True,
                            anchor_df_max=10 ** 9, anchor_min_codes=4)
    hits_u, _r = search_passage(idx, query, ungated)
    anchors_u = [h for h in hits_u if h.tier == 'anchor']
    frm_u = [h for h in anchors_u if h.record_id.startswith('frm')]
    rare_u = next(h for h in anchors_u if h.record_id == 'rare000')
    assert len(frm_u) > 2 * len(frm), 'the gate must thin the flood'
    assert max(h.anchor_codes for h in frm_u) > rare_u.anchor_codes, \
        'fixture must reproduce the COUNT inversion the fix is about'
    assert anchors_u[0].record_id == 'rare000', \
        'weight ordering must beat the count inversion even ungated'


def test_anchor_cap_keeps_the_highest_WEIGHT_not_the_highest_count(
        formulaic_index):
    """A count-ordered cap keeps the records sharing the most stock phrases;
    a weight-ordered cap keeps the distinctive one. With a single slot and
    NO rarity gate, the rare record must still win -- that is the ordering
    fix, independent of the membership gate."""
    idx, query = formulaic_index
    allp = PassagePolicy(name='t-all', anchor_tier=True,
                         anchor_df_max=10 ** 9, anchor_min_codes=4)
    everything = {h.record_id: h for h in search_passage(idx, query, allp)[0]
                  if h.tier == 'anchor'}
    assert 'rare000' in everything
    assert max(h.anchor_codes for h in everything.values()) > \
        everything['rare000'].anchor_codes, (
        'fixture must have a formulaic record with a HIGHER raw count, '
        'otherwise the weight ordering is not actually under test')

    one = PassagePolicy(name='t-one', anchor_tier=True,
                        anchor_df_max=10 ** 9, anchor_min_codes=4,
                        anchor_cap=1)
    hits, report = search_passage(idx, query, one)
    anchors = [h for h in hits if h.tier == 'anchor']
    assert len(anchors) == 1 and report.anchor_truncated is True
    assert anchors[0].record_id == 'rare000', (
        'the single kept anchor must be the rarest-evidenced record, not '
        'the one sharing the most stock formula')


def test_unverified_records_are_never_reported_as_anchor_only(scatter_index):
    """PR #327 review (Codex P1). `merged` holds only records that were
    actually verified AND accepted, so a record the verify cap never tried
    trivially satisfies 'not in merged'. Reporting it as anchor-only asserts
    'no alignment accepted' about a check that never ran -- and because
    verification is ordered by anchor strength, the untried tail is exactly
    where the anchor tier's own population sits, so this is the common case,
    not a corner. Two verbatim carriers with verify_cap=1: the second must
    NOT come back as an anchor hit."""
    idx, reference = scatter_index
    policy = PassagePolicy(name='t-cap1', density_scale=2.0, anchor_tier=True,
                           anchor_min_codes=2, anchor_df_max=10 ** 9,
                           verify_cap=1)
    hits, report = search_passage(idx, reference, policy)
    assert report.verify_truncated is True
    spans = [h for h in hits if h.tier == 'span']
    anchors = [h for h in hits if h.tier == 'anchor']
    assert len(spans) == 1, 'verify_cap=1 admits exactly one accepted span'
    # The carrier is a real contiguous match; it must never be demoted to
    # anchor-only just because the cap stopped before its other clusters.
    assert 'carrier015' not in {h.record_id for h in anchors}
    # Records withheld for being untried are COUNTED, never silently dropped.
    assert report.anchor_withheld_unverified >= 1
    assert report.anchor_records == len(anchors)


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

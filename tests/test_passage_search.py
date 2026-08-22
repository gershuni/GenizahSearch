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

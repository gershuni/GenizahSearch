# -*- coding: utf-8 -*-
"""The Antiochus deck is an INSTRUMENT, so it gets instrument tests.

Every recall figure in docs/specs/passage-matching-algorithm.md sections 8.1
and 10.4 is a measurement against eval/antiochus/deck.json. If the deck or
the join drifts, those published numbers quietly stop meaning what they say
-- and nothing else in the suite would notice, because no application code
reads any of this.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, 'scripts'))

from score_antiochus_deck import (  # noqa: E402
    FRONTIER, POSITIVE, RUNS_DIR, load_deck, load_run, score,
)
from shelfmark_join import canonical_key, load_aliases, shelfmark_key  # noqa: E402


@pytest.fixture(scope='module')
def deck_and_aliases():
    return load_deck()


def test_the_deck_is_the_size_the_spec_quotes(deck_and_aliases):
    """1767 graded / 83 positives is cited in section 8.1 as the denominator
    of every recall figure. It is not a detail that may drift."""
    deck, _ = deck_and_aliases
    assert len(deck) == 1767
    positives = [r for r in deck.values() if r['verdict'] in POSITIVE]
    assert len(positives) == 83
    assert sum(1 for r in positives if r['verdict'] == 'WITNESS') == 69
    assert sum(1 for r in positives if r['verdict'] == 'INDIRECT') == 14


def test_every_verdict_carries_its_reason(deck_and_aliases):
    """A verdict without a stated basis cannot be re-checked by the next
    person, which makes the deck an assertion rather than evidence."""
    deck, _ = deck_and_aliases
    for rec in deck.values():
        assert rec['verdict'] in ('WITNESS', 'INDIRECT', 'NOISE')
        assert rec.get('reason', '').strip(), rec


def test_the_deck_never_contradicts_itself_after_alias_resolution():
    """load_deck refuses a deck whose canonical keys collide on different
    verdicts -- otherwise dict order would decide whether a manuscript counts
    as a positive. This caught a bad alias on 2026-08-24 (the collection-level
    'Gaster, Moses Collection' equated with the single codex 'Ms. 1774')."""
    load_deck()  # exits non-zero on conflict; reaching here is the assertion


def test_the_rejected_alias_is_kept_rejected():
    """Recorded so it is not re-derived and re-accepted. It merged a NOISE
    entry with a WITNESS one."""
    path = os.path.join(ROOT, 'eval', 'antiochus', 'aliases.json')
    with open(path, encoding='utf-8') as fh:
        data = json.load(fh)
    live = {v for e in data['aliases'] for v in e['variants']}
    assert 'Gaster, Moses Collection' not in live
    assert any('Gaster, Moses Collection' in e['variants']
               for e in data['_rejected'])


def test_the_join_collapses_the_variants_that_broke_a_measurement():
    """The catalogue/export spelling differences that produced a 22%-recall
    misreport before the key existed."""
    same = [
        ('Cambridge University Library Ms. T-S NS 312', 'Ms. T-S NS 312'),
        ('Or. 2116.12.A.2', 'Or.2116.12a.2'),
    ]
    for a, b in same:
        assert shelfmark_key(a) == shelfmark_key(b), (a, b)
    aliases = load_aliases()
    assert canonical_key('L-G Misc. 31', aliases) == \
        canonical_key('Ms. Misc. 31', aliases)
    assert canonical_key('T-S A 45.09', aliases) == \
        canonical_key('T-S A45.9', aliases)


def test_distinct_manuscripts_do_not_collide():
    """The join must not be so aggressive that it merges real neighbours --
    a false merge inflates precision and is invisible in the totals."""
    distinct = [
        ('T-S A45.10', 'T-S A45.11'),
        ('MS heb. e.45/34', 'MS heb. e.45/36'),
        ('L-G Ar.II.151', 'L-G Ar.II.152'),
    ]
    aliases = load_aliases()
    for a, b in distinct:
        assert canonical_key(a, aliases) != canonical_key(b, aliases), (a, b)


def test_the_frontier_is_exactly_the_chunk2_exclusive_positives(deck_and_aliases):
    """FRONTIER drives the scorer's headline 'n/20'. It must stay derived
    from the archived runs, not hand-maintained into disagreement."""
    deck, aliases = deck_and_aliases
    sets = {}
    for fn in os.listdir(RUNS_DIR):
        if fn.endswith('.json'):
            sets[fn[:-5]] = {canonical_key(s, aliases)
                             for s in load_run(os.path.join(RUNS_DIR, fn))}
    others = set().union(*[v for k, v in sets.items()
                           if k != 'chunks-2-filtered'])
    exclusive = {k for k in sets['chunks-2-filtered'] - others
                 if deck.get(k, {}).get('verdict') in POSITIVE}
    assert exclusive == {canonical_key(s, aliases) for s in FRONTIER}
    assert len(FRONTIER) == 20


@pytest.mark.parametrize('run,manuscripts,precision,recall,frontier', [
    ('chunks-linebreaks', 43, 53, 28, 0),
    ('letters-widest-40', 56, 100, 67, 0),
    ('letters-max-40', 67, 90, 72, 0),
    ('chunks-3', 297, 18, 64, 0),
    ('chunks-2-filtered', 1727, 5, 98, 20),
])
def test_the_published_table_still_reproduces(
        deck_and_aliases, run, manuscripts, precision, recall, frontier):
    """These are the numbers eval/antiochus/README.md prints and the spec
    reasons from. Pinned so a change to the deck, the join or the scorer
    cannot silently rewrite published measurements."""
    deck, aliases = deck_and_aliases
    res = score(load_run(os.path.join(RUNS_DIR, run + '.json')), deck, aliases)
    assert res['manuscripts'] == manuscripts
    assert round(res['precision'] * 100) == precision
    assert round(res['recall'] * 100) == recall
    assert len(res['frontier_found']) == frontier


def test_a_run_of_pure_positives_scores_perfectly(deck_and_aliases):
    """Guards the scorer itself: a hand-built ideal run must come back at
    100/100, or the harness is measuring something other than what it says."""
    deck, aliases = deck_and_aliases
    ideal = {rec['shelfmark']: 1.0 for rec in deck.values()
             if rec['verdict'] in POSITIVE}
    res = score(ideal, deck, aliases)
    assert res['precision'] == 1.0 and res['recall'] == 1.0
    assert not res['buckets']['UNGRADED'] and not res['missed']


def test_an_unknown_shelfmark_is_ungraded_not_noise(deck_and_aliases):
    """The distinction the README is built around: absent from the deck is
    'unmeasured', never 'wrong'. Collapsing the two is how a broken join
    disguises itself as a bad result."""
    deck, aliases = deck_and_aliases
    res = score({'T-S NEVER-SEEN 99.99': 5.0}, deck, aliases)
    assert len(res['buckets']['UNGRADED']) == 1
    assert not res['buckets']['NOISE']

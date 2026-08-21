# -*- coding: utf-8 -*-
"""Score a stratified DISPLAY-POLICY grading deck (external review, Codex
2026-08-21) into inverse-probability-weighted (IPW) precision and per-query
yield, per view (S = standard, W = wide, C5 = chunk-5, or whatever set of
view names the deck's own manifest declares).

Why this is a SEPARATE script from `scripts/score_grading_deck.py`. That
scorer answers "of the cards a method produced, how many were real?" with an
unweighted per-card proportion -- correct for a POOLED, method-blinded deck
where every card had an equal (or irrelevant) chance of being drawn. This
deck answers a different question: "if a reader is shown view V's actual
display list, how many genuine same-text manuscripts do they see, and what
share of the real estate is a genuine match?" -- a question about the
DISPLAY POLICY itself, not about a method's raw candidate pool. Because the
deck draws cards from strata (by rank band and span band) at DIFFERENT
sampling rates `pi_h` to keep the deck small while still covering rare-but-
important strata (e.g. rank 4-10), a card sampled from a thin stratum stands
in for many unseen siblings. Treating it as one vote among equals -- what
`score_grading_deck.py` does -- would silently over- or under-weight those
strata. The fix is the standard survey-sampling one: each graded card's
contribution is divided by its OWN inclusion probability (`1/pi_h`, a
Horvitz-Thompson weight) before being summed, so a rarely-sampled card
speaks for the many cards its stratum did not have room to grade.

THE INPUT CONTRACT (a deck directory holds three files):

  deck_key.json      a LIST of cards: {"id", "query_id", "record_id",
                      "sys_id", "is_source", "selections": [...]}.
                      `selections` is a list because ONE card (one
                      manuscript) can be independently drawn into SEVERAL
                      views -- each entry names {"view", "rank",
                      "rank_band", "span_band", "stratum", "N_h", "n_h",
                      "pi_h"} for that one (view, stratum) draw. A card
                      graded once therefore contributes ONCE PER SELECTION.
  deck_manifest.json  {"cards_hash", "key_hash", "n_cards", "n_queries",
                      "views": {view: {"N_display", "strata": {stratum:
                      {"N_h", "n_h"}}}}}. `N_display` is the TRUE
                      denominator: the total number of displayed (query,
                      sys_id) cards in that view across the whole panel,
                      known by design, not estimated from the sample. As of
                      the stratified-bootstrap rewrite below, `N_h`/`n_h`
                      per stratum are the ONLY per-stratum numbers this
                      module reads from the manifest (a selection's own
                      carried `n_h`/`N_h`/`pi_h` are still validated for
                      basic sanity, but the manifest's declared values are
                      the canonical weight source -- see "KEEP THE WEIGHTS
                      FIXED" below).
  prereg.json         metadata; must carry a positive "panel_n_queries"
                      (the yield endpoint's denominator has no meaning
                      without it).

The verdicts export is {"deck": <cards_hash prefix>, "grader", "verdicts":
[{"id", "grade"}, ...]} -- the same shape `score_grading_deck.py` consumes.

GRADE VOCABULARY AND duplicate_photo -- THE ONE DELIBERATE DIVERGENCE FROM
score_grading_deck.py. That scorer excludes `duplicate_photo` from every
denominator, on the reasoning that a re-photographed page is a corpus
artefact, not a retrieval verdict. That reasoning does not carry over here:
this deck measures what a READER actually sees on screen, and a duplicate-
photo card still occupies one of the `N_display` display slots whether or
not it happens to be a physically duplicated page -- excluding it would
under-state the denominator and silently inflate precision. So here,
`duplicate_photo` COUNTS toward the strict denominator (it consumed a
slot) but never toward the strict NUMERATOR (`y_i = 1` only for
same_text/paraphrase), and its own weighted rate is reported as a separate
number precisely so a reader of the report is not left guessing how much
of the "non-strict" mass is corpus artefact versus a genuine miss.

TWO INTEGRATION SMOKES, TWO FIXES -- the full, honest history, because the
first fix was itself found wrong by measuring real output, not just by
reasoning about it:

  SMOKE 1 (2026-08-21, real builder output, synthetic all-same_text
  verdicts): the ORIGINAL bootstrap resampled QUERIES in the numerator
  only, dividing every replicate by `N_display` held FIXED -- correct for
  the point estimate, wrong for the interval: a replicate drawing display-
  heavy queries inflates the numerator against a denominator that cannot
  move with it. Observed: ci95 upper bounds of 1.486, 1.377, 1.502 on
  views whose points were all exactly 1.000. FIX 1: a RATIO bootstrap that
  resampled queries and recomputed a per-query denominator (a new manifest
  field, `display_counts_by_query`) alongside the numerator.

  SMOKE 2 (same day, run against REAL smoke-deck data instead of an
  all-same_text synthetic): FIX 1's own unit-interval guard fired for
  real, "ci95 upper bound of 1.5247 escaped [0, 1]" -- refusing to clip it
  away was correct, but the cause was not fixable inside a per-query ratio
  bootstrap at all. The sampling design is stratified by (view, rank_band,
  span_band) -- strata that cut ACROSS queries -- while FIX 1's variance
  estimator clustered by QUERY. Those two do not compose: a card drawn
  from a thin stratum (measured: pi_h = 4/74, weight 18.5) can belong to a
  query that itself displays only a handful of cards, so that ONE query's
  own naive numerator can exceed its own naive local denominator even
  though the estimator is behaving exactly as designed. Measured on the
  real smoke deck: 6 of 10 graded S queries, 8 of 14 W queries, and 7 of
  12 C5 queries had a weighted numerator exceeding their own display
  count -- this is the NORMAL case for 6-8 of every ~12 graded queries,
  not a rare edge case. `display_counts_by_query` is consequently GONE
  from this module: nothing here reads it any more.

  FIX 2 (current): resample the way the deck was actually DRAWN --
  per STRATUM, not per query. See "KEEP THE WEIGHTS FIXED" below.

KEEP THE WEIGHTS FIXED AT N_h/n_h -- the stratified bootstrap. For each
view, group every GRADED selection by its stratum. A replicate draws `n_h`
cards WITH REPLACEMENT from that stratum's graded cards (not from the whole
design population -- only the graded subset has a known outcome), and every
draw is weighted at the stratum's OWN `N_h/n_h`, the SAME number for every
card in that stratum, taken from the manifest, not from any individual
selection's own carried `pi_h` (which the design intends to equal N_h/n_h
anyway, but the bound below needs one canonical source, not N separately
agreeing copies). This bounds the replicate total by construction:

    max replicate numerator = sum over h of  n_h * (N_h / n_h) * 1
                             = sum over h of N_h
                             = N_display

-- so precision (and duplicate_photo_rate, the other share-of-slots metric)
can NEVER exceed 1 no matter what a replicate happens to draw. The
`unit_interval` guard on `stratified_bootstrap` becomes a true internal-bug
detector again, rather than a tripwire on the design itself. This equality
(`sum of stratum N_h == N_display`) is a real invariant of how
`build_display_deck.py` computes `N_display` -- and `validate_structure`
now verifies it on the read side (never trust, verify) rather than assuming
the builder's own bookkeeping is correct on the specific file it produced.

Yield's denominator handling simplifies under this rewrite: since there is
no more QUERY resampling at all, both the point estimate AND every
bootstrap replicate divide by the SAME fixed `panel_n_queries` -- the
point/bootstrap-denominator asymmetry FIX 1 needed for yield (documented as
D1 below, superseded) is gone; `stratified_bootstrap` takes one `denom`,
used for both.

RESIDUAL LIMITATION, stated with numbers rather than a bare promise: a
stratified bootstrap treats cards as independent WITHIN a stratum. That is
a good approximation here -- measured on the real smoke deck, cards per
(query, stratum) was {1: 14} for view S and {1: 16, 2: 1} for both W and
C5, i.e. almost no query contributes TWO cards to the same stratum, so the
within-stratum-independence assumption costs almost nothing. What the
stratified bootstrap does NOT model is a query contributing cards to
DIFFERENT strata: those draws are resampled independently even though they
describe the same query, so any real correlation between them is missed,
making the reported interval a (typically modest) UNDERSTATEMENT of the
true uncertainty. `score()` computes and reports the exposure directly, per
view, as `multi_stratum_queries` in the JSON output -- "how many queries
have selections in 2+ distinct strata" -- so a reader can judge the size of
the understatement on THIS deck rather than take a promise about deck
design in general.

DESIGN DECISIONS MADE WHERE THE CONTRACT ABOVE WAS SILENT (flagged here
rather than guessed away quietly -- see the task report for the same list):

  D1. SUPERSEDED by FIX 2 above. Previously: yield's bootstrap denominator
      was fixed-but-different-from-its-point-estimate's-denominator (a
      per-query-resampling artefact). There is no query resampling left,
      so there is nothing left to document here beyond FIX 2's own
      simplification note.
  D2. SUPERSEDED by FIX 2 above. Previously described the query-resampling
      universe for FIX 1's per-query ratio bootstrap; moot now that
      resampling is per-stratum, not per-query.
  D3. "Restricted to non-source cards" (item 3 of the spec) filters the
      NUMERATOR only; the denominator (`N_display` / `panel_n_queries`)
      is shared, unfiltered, identical between the "overall" and
      "non_source" columns -- unchanged in spirit from before, now
      implemented by filtering which GRADED cards enter a stratum's pool
      rather than which query's contribution counts. A non-source-only
      denominator (i.e. "accuracy among just the non-source slots") is NOT
      computed, because `N_display` is not broken out by `is_source` in
      the given schema, so a true non-source population size is not
      derivable without fabricating an estimated sub-population (a
      Hajek-type ratio), which would mix estimator types within one
      report. The chosen reading is honest and simple to hand-verify:
      "what share of ALL displayed slots is a genuine non-source strict
      match" rather than "accuracy specifically among non-source slots".
  D4. prereg.json's grade-vocabulary override key name is not given in the
      spec (only "panel_n_queries" is shown in the example). This module
      accepts `prereg["grade_vocabulary"]`, falling back to
      `prereg["vocabulary"]` -- confirmed against the real
      `scripts/build_display_deck.py` output, which writes `vocabulary`
      -- falling back to the same 8-term vocabulary
      `scripts/score_grading_deck.py` uses (duplicated below, not
      imported, for the same reason that module gives for duplicating its
      own copy of `sha()`: no coupling to a sibling script's import graph).
  D5. `--min-graded` is a GLOBAL threshold -- the count of distinct graded
      card ids across the WHOLE verdicts export -- mirroring
      `scripts/score_grading_deck.py`'s convention, since the spec's
      "--min-graded N (fatal if fewer graded)" does not say per-view.
  D6. The relation mix by rank band is computed over all graded selections
      in a view (source and non-source combined), not split further by
      `is_source` -- the spec asks for one such breakdown, not two. It now
      weights by the manifest's canonical `N_h/n_h` (see "KEEP THE WEIGHTS
      FIXED"), not by each selection's own carried `pi_h`, for the same
      single-source-of-truth reason.
  D7. The "zero graded cards -> INSUFFICIENT" rule (spec's validation
      section) is applied independently to EACH (view, column) pair, not
      only at the whole-view grain: a view can have plenty of graded
      overall cards yet zero graded non-source cards, and reporting a
      fabricated 0.000 for that column would be exactly the failure mode
      the rule exists to prevent.
  D8. `is_source` is NOT always a bool, despite the input contract's
      "is_source": bool -- `scripts/build_display_deck.py`'s own
      `compute_is_source()` (imported from build_grading_deck.py) returns
      None when a query row lacks `meta.sys_id`. This module's
      `non_source_only` filters treat both `False` and `None` as
      "include in the non-source column" (`if non_source_only and
      card.get('is_source'):` only excludes an AFFIRMATIVE `True`) --
      i.e. a card of genuinely unknown source-status is folded into
      "non-source" by default rather than excluded outright. MOOT on the
      real FGP panel: 0 of 19,090 FGP queries lack `meta.sys_id`, and
      `build_display_deck.py` now REFUSES to emit a None `is_source`
      rather than filing it as non-source -- so this fallback is a
      defensive default for a case the real builder no longer produces,
      not an active behaviour on production data. Kept (rather than
      deleted) because a synthetic or future deck could still hand this
      module a `None`, and a silent Python-truthiness accident is worse
      than a documented one.

Usage:
  python scripts/score_display_deck.py --deck-dir DIR --verdicts V.json \
      [--min-graded N] [--cluster-seed S] [--cluster-resamples R] \
      [--out scored.json]
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import random

STRICT = frozenset({'same_text', 'paraphrase'})
DUPLICATE_PHOTO = 'duplicate_photo'

# Duplicated from scripts/score_grading_deck.py::ALL_GRADES -- see D4 above
# for why this is a fallback rather than an import.
DEFAULT_GRADE_VOCAB = frozenset({
    'same_text', 'paraphrase', 'canonical', 'shared_formula',
    'duplicate_photo', 'topical', 'unrelated', 'junk',
})

CLUSTER_SEED_DEFAULT = 20260821
CLUSTER_RESAMPLES_DEFAULT = 10_000

INSUFFICIENT = 'INSUFFICIENT'

# Float slack for the [0, 1] share-of-slots bound check -- large enough to
# absorb ordinary floating-point division noise, far too small to mask a
# real escape (both smokes this module survived overshot by tenths, not
# 1e-9).
UNIT_INTERVAL_EPS = 1e-9


def sha(obj) -> str:
    """Byte-identical to scripts/score_grading_deck.py::sha (and, through
    it, scripts/build_grading_deck.py::sha) -- pinned directly by
    tests/test_score_display_deck.py against a real call into that module,
    not just by matching source text.
    """
    blob = json.dumps(obj, sort_keys=True, ensure_ascii=False,
                      separators=(',', ':')).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def load_json(path: str) -> object:
    with open(path, encoding='utf-8') as fh:
        return json.load(fh)


def validate_structure(key_list: list, manifest: dict) -> None:
    """Fatal (SystemExit), never a silent skip, on any structural defect in
    deck_key.json considered against itself and against deck_manifest.json.
    Every defect kind is counted across the WHOLE key before raising, same
    fail-closed reporting style as scripts/score_grading_deck.py.

    Checks:
      - every card carries at least one selection;
      - every selection has 0 < pi_h <= 1;
      - every selection has n_h <= N_h (the selection's OWN carried
        n_h/N_h fields -- a basic sanity/tamper-evidence check, even
        though the stratified bootstrap weights by the MANIFEST's
        declared N_h/n_h, not by these per-selection copies);
      - for every (view, stratum) pair, the number of selections actually
        present in deck_key.json does not exceed that (view, stratum)'s
        n_h as declared in deck_manifest.json (a pair absent from the
        manifest is treated as declaring n_h=0, so any selection against
        an undeclared view/stratum is refused too);
      - for every view, the declared strata's N_h values sum EXACTLY to
        that view's N_display -- the invariant the stratified bootstrap's
        max-numerator-equals-N_display bound depends on, and a real
        property of how build_display_deck.py computes N_display, verified
        here rather than assumed.
    """
    empty_selection_ids = []
    range_violations = []
    nh_violations = []
    actual_counts: collections.Counter = collections.Counter()

    for card in key_list:
        sels = card.get('selections') or []
        if not sels:
            empty_selection_ids.append(card.get('id'))
            continue
        for sel in sels:
            view, stratum = sel.get('view'), sel.get('stratum')
            pi_h = sel.get('pi_h')
            n_h, N_h = sel.get('n_h'), sel.get('N_h')
            if pi_h is None or not (0 < pi_h <= 1):
                range_violations.append((card.get('id'), view, stratum, pi_h))
            if n_h is None or N_h is None or n_h > N_h:
                nh_violations.append((card.get('id'), view, stratum, n_h, N_h))
            actual_counts[(view, stratum)] += 1

    views_manifest = manifest.get('views') or {}
    cap_violations = []
    for (view, stratum), actual in sorted(actual_counts.items()):
        declared = (views_manifest.get(view) or {}).get('strata', {}).get(stratum, {})
        n_h_declared = declared.get('n_h', 0)
        if actual > n_h_declared:
            cap_violations.append((view, stratum, actual, n_h_declared))

    n_display_mismatches = []
    for view, view_manifest in sorted(views_manifest.items()):
        strata_manifest = view_manifest.get('strata') or {}
        total_N_h = sum(s.get('N_h', 0) for s in strata_manifest.values())
        n_display = view_manifest.get('N_display')
        if total_N_h != n_display:
            n_display_mismatches.append((view, total_N_h, n_display))

    if not (empty_selection_ids or range_violations or nh_violations
            or cap_violations or n_display_mismatches):
        return

    total = (len(empty_selection_ids) + len(range_violations)
             + len(nh_violations) + len(cap_violations)
             + len(n_display_mismatches))
    lines = [f'REFUSING to score: {total} structural defect(s) in '
            f'deck_key.json / deck_manifest.json']
    if empty_selection_ids:
        lines.append(f'  {len(empty_selection_ids)} card(s) with zero '
                    f'selections: {empty_selection_ids[:5]}')
    if range_violations:
        lines.append(f'  {len(range_violations)} selection(s) with pi_h '
                    f'outside (0, 1]: {range_violations[:5]}')
    if nh_violations:
        lines.append(f'  {len(nh_violations)} selection(s) with n_h > N_h: '
                    f'{nh_violations[:5]}')
    if cap_violations:
        lines.append(f'  {len(cap_violations)} (view, stratum) '
                    f'combination(s) exceeding manifest n_h totals: '
                    f'{cap_violations[:5]}')
    if n_display_mismatches:
        lines.append(f'  {len(n_display_mismatches)} view(s) whose strata '
                    f"N_h do not sum to N_display: {n_display_mismatches[:5]}")
    raise SystemExit('\n'.join(lines))


def process_verdicts(verdicts: list, key: dict, vocab: frozenset) -> tuple:
    """-> (graded: {card_id: grade}, dup_count, orphan_count,
    unknown_grades: Counter). Never raises here -- the caller decides
    whether to refuse, after counting every defect kind across the WHOLE
    export (same fail-closed style as scripts/score_grading_deck.py).
    """
    graded: dict = {}
    dup_count = orphan_count = 0
    unknown_grades: collections.Counter = collections.Counter()
    seen: set = set()
    for v in verdicts:
        cid, g = v.get('id'), v.get('grade')
        if cid not in key:
            orphan_count += 1
            continue
        if g not in vocab:
            unknown_grades[g] += 1
            continue
        if cid in seen:
            dup_count += 1
            continue
        seen.add(cid)
        graded[cid] = g
    return graded, dup_count, orphan_count, unknown_grades


def graded_selection_count(key: dict, graded: dict, view: str,
                           non_source_only: bool = False) -> int:
    """How many GRADED selections exist for `view` (optionally restricted
    to non-source cards). Zero here is exactly the "view has zero graded
    cards" condition the INSUFFICIENT rule (D7) gates on.
    """
    n = 0
    for cid, card in key.items():
        if cid not in graded:
            continue
        if non_source_only and card.get('is_source'):
            continue
        n += sum(1 for sel in (card.get('selections') or [])
                 if sel.get('view') == view)
    return n


def count_multi_stratum_queries(key: dict, view: str) -> int:
    """How many DISTINCT queries have selections in 2+ distinct strata
    within `view` -- the residual within-query correlation the stratified
    bootstrap does not model (see the module docstring's "RESIDUAL
    LIMITATION"). Counted over ALL cards in the key, not just graded ones,
    because this is a property of the DECK'S DESIGN (which cards were
    drawn for which query), not of grading progress.
    """
    strata_by_query: dict = collections.defaultdict(set)
    for card in key.values():
        for sel in card.get('selections') or []:
            if sel.get('view') != view:
                continue
            strata_by_query[card['query_id']].add(sel.get('stratum'))
    return sum(1 for strata in strata_by_query.values() if len(strata) >= 2)


def stratum_pools(key: dict, graded: dict, view: str, predicate,
                  view_manifest: dict, non_source_only: bool = False) -> dict:
    """{stratum: {'weight': N_h/n_h, 'n_h': n_h, 'graded': [0/1, ...]}} --
    the per-stratum pool `stratified_bootstrap` draws from. `weight` and
    `n_h` come from the MANIFEST's declared per-stratum values (the
    canonical source -- see "KEEP THE WEIGHTS FIXED" in the module
    docstring), not from any individual selection's own carried `pi_h`.
    `graded` holds one 0/1 entry per GRADED selection in this (view,
    stratum) matching `predicate(grade)` (and, if `non_source_only`,
    excluding source cards) -- its length can be LESS than `n_h` when
    grading is incomplete; a stratum with nothing graded yet is simply an
    empty list, which `stratified_bootstrap` treats as a deterministic 0
    contribution.
    """
    strata_manifest = view_manifest.get('strata') or {}
    out = {}
    for st, m in strata_manifest.items():
        n_h = m.get('n_h') or 0
        N_h = m.get('N_h') or 0
        out[st] = {'weight': (N_h / n_h) if n_h else 0.0, 'n_h': n_h,
                  'graded': []}
    for cid, card in key.items():
        if cid not in graded:
            continue
        if non_source_only and card.get('is_source'):
            continue
        grade = graded[cid]
        y = 1 if predicate(grade) else 0
        for sel in card.get('selections') or []:
            if sel.get('view') != view:
                continue
            st = sel.get('stratum')
            if st in out:
                out[st]['graded'].append(y)
    return out


def stratified_bootstrap(strata: dict, denom: float, resamples: int,
                         seed: int, *, unit_interval: bool = False) -> dict:
    """The FIX 2 estimator: point = sum over strata of weight*sum(graded),
    divided by the FIXED `denom` (N_display for precision/
    duplicate_photo_rate, panel_n_queries for yield -- see the module
    docstring's "Yield's denominator handling simplifies"). Each bootstrap
    replicate independently resamples EVERY stratum: for stratum h with a
    non-empty graded pool, draw `n_h` items WITH REPLACEMENT from that
    pool, contributing `weight * (sum of the n_h draws)`; a stratum with
    nothing graded contributes a deterministic 0. The replicate total is
    divided by the SAME fixed `denom` used for the point -- there is no
    query resampling left anywhere in this module, so nothing about the
    denominator varies between the point estimate and any replicate,
    unlike FIX 1's yield asymmetry (superseded, see D1).

    This construction bounds the replicate total by n_h*weight=N_h per
    stratum (at most, when every draw happens to be a 1), so the total can
    never exceed sum(N_h)=N_display -- `validate_structure` verifies that
    equality on the read side. `unit_interval=True` still asserts (fatal,
    never silently clipped) that the point AND both CI bounds fall in
    [0, 1] past a tiny float epsilon, but it is now a genuine internal-bug
    detector: on a correctly-validated deck, this bound is a mathematical
    certainty, not a design tripwire (see the module docstring's two
    integration-smoke fixes for why FIX 1's version of this same guard was
    NOT a tripwire-free certainty, and fired for real).
    """
    contributing = [(s['weight'], s['n_h'], s['graded'])
                   for s in strata.values() if s['graded']]
    point_total = sum(weight * sum(graded_list)
                      for weight, _n_h, graded_list in contributing)
    point = point_total / denom if denom else 0.0

    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        total = 0.0
        for weight, n_h, graded_list in contributing:
            m = len(graded_list)
            for _ in range(n_h):
                total += weight * graded_list[rng.randrange(m)]
        stats.append(total / denom if denom else 0.0)
    stats.sort()

    def q_(p: float) -> float:
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    lo_raw, hi_raw = (q_(0.025), q_(0.975)) if stats else (0.0, 0.0)

    if unit_interval:
        for label, val in (('point', point), ('ci95 lower bound', lo_raw),
                           ('ci95 upper bound', hi_raw)):
            if not (-UNIT_INTERVAL_EPS <= val <= 1 + UNIT_INTERVAL_EPS):
                raise SystemExit(
                    f'INTERNAL BUG: a share-of-display-slots {label} of '
                    f'{val!r} escaped [0, 1] -- refusing to clip it away; '
                    f'this is an estimator or input defect, not a display '
                    f'rounding issue')

    return {
        'point': round(point, 4),
        'ci95': [round(lo_raw, 4), round(hi_raw, 4)],
        'n_groups': len(contributing),
    }


def relation_mix_by_rank_band(key: dict, graded: dict, view: str,
                              view_manifest: dict) -> dict:
    """{rank_band: {"n": int, "mix": {grade: weighted_fraction}}} for every
    GRADED selection in `view`, weighted by the manifest's canonical
    N_h/n_h per stratum (D6) and normalised to sum to 1 within each band
    (source and non-source combined). A band with zero graded selections
    is simply absent -- never a fabricated entry.
    """
    strata_manifest = view_manifest.get('strata') or {}

    def weight_of(stratum):
        m = strata_manifest.get(stratum) or {}
        n_h = m.get('n_h') or 0
        return (m.get('N_h', 0) / n_h) if n_h else 0.0

    weights: dict = collections.defaultdict(lambda: collections.defaultdict(float))
    counts: collections.Counter = collections.Counter()
    for cid, card in key.items():
        if cid not in graded:
            continue
        grade = graded[cid]
        for sel in card.get('selections') or []:
            if sel.get('view') != view:
                continue
            weights[sel.get('rank_band')][grade] += weight_of(sel.get('stratum'))
            counts[sel.get('rank_band')] += 1
    out = {}
    for band, grade_weights in weights.items():
        total_w = sum(grade_weights.values())
        out[band] = {
            'n': counts[band],
            'mix': {g: round(w / total_w, 4)
                   for g, w in sorted(grade_weights.items())} if total_w else {},
        }
    return out


def score(deck_dir: str, verdicts_path: str, *, min_graded: int = 0,
          cluster_seed: int = CLUSTER_SEED_DEFAULT,
          cluster_resamples: int = CLUSTER_RESAMPLES_DEFAULT) -> dict:
    """Validate a verdicts export against its stratified display deck, then
    score every view. Raises SystemExit (fatal, never a silent skip) on any
    tamper-evidence or format defect described in the module docstring.
    """
    key_list = load_json(os.path.join(deck_dir, 'deck_key.json'))
    manifest = load_json(os.path.join(deck_dir, 'deck_manifest.json'))
    prereg_path = os.path.join(deck_dir, 'prereg.json')
    prereg = load_json(prereg_path) if os.path.exists(prereg_path) else {}
    payload = load_json(verdicts_path)
    verdicts = payload.get('verdicts', payload)

    deck_id = manifest['cards_hash'][:16]
    declared = payload.get('deck')
    if not declared:
        raise SystemExit(
            f'REFUSING to score {verdicts_path}: no deck id declared in '
            f'the verdicts export -- cannot verify it belongs to deck '
            f'{deck_id} (tamper-evidence requires a declared id, not just '
            f'a matching one)')
    if declared != deck_id:
        raise SystemExit(
            f'REFUSING: verdicts are for deck {declared}, this deck is '
            f'{deck_id}')

    recomputed_key_hash = sha(key_list)
    if recomputed_key_hash != manifest.get('key_hash'):
        raise SystemExit(
            f'REFUSING: deck_key.json does not match deck_manifest.json -- '
            f'manifest key_hash={manifest.get("key_hash")!r}, recomputed '
            f'from the key file on disk={recomputed_key_hash!r}. The key '
            f'was edited or regenerated without re-baking the manifest.')

    validate_structure(key_list, manifest)

    key = {c['id']: c for c in key_list}

    panel_n_queries = prereg.get('panel_n_queries')
    if not panel_n_queries or panel_n_queries <= 0:
        raise SystemExit(
            'REFUSING: prereg.json must declare a positive '
            '"panel_n_queries" -- the weighted-yield endpoint has no '
            'denominator without it')

    vocab = DEFAULT_GRADE_VOCAB
    vocab_source = 'default (score_grading_deck.ALL_GRADES equivalent)'
    prereg_vocab = prereg.get('grade_vocabulary') or prereg.get('vocabulary')
    if prereg_vocab:
        vocab = frozenset(prereg_vocab)
        vocab_source = 'prereg.json'

    graded, dup_count, orphan_count, unknown_grades = process_verdicts(
        verdicts, key, vocab)

    if dup_count or orphan_count or unknown_grades:
        lines = [f'REFUSING to score {verdicts_path}: '
                f'{dup_count + orphan_count + sum(unknown_grades.values())} '
                f'defective verdict row(s) found']
        if dup_count:
            lines.append(f'  {dup_count} duplicate verdict id(s)')
        if orphan_count:
            lines.append(f'  {orphan_count} verdict id(s) absent from the '
                         f'deck key')
        if unknown_grades:
            lines.append(f'  grade(s) outside the prereg vocabulary: '
                         f'{dict(unknown_grades)}')
        raise SystemExit('\n'.join(lines))

    if min_graded and len(graded) < min_graded:
        raise SystemExit(
            f'REFUSING: only {len(graded)} of {manifest.get("n_cards")} '
            f'cards graded, --min-graded requires {min_graded}')

    views_out = {}
    for view, view_manifest in sorted((manifest.get('views') or {}).items()):
        N_display = view_manifest.get('N_display')
        entry = {
            'n_display': N_display,
            'n_graded_selections': graded_selection_count(key, graded, view),
            'multi_stratum_queries': count_multi_stratum_queries(key, view),
            'precision': {},
            'yield': {},
            'duplicate_photo_rate': {},
        }
        for column, non_source_only in (('overall', False),
                                        ('non_source', True)):
            n_g = graded_selection_count(key, graded, view, non_source_only)
            if not n_g or not N_display:
                entry['precision'][column] = INSUFFICIENT
                entry['yield'][column] = INSUFFICIENT
                entry['duplicate_photo_rate'][column] = INSUFFICIENT
                continue
            strict_strata = stratum_pools(
                key, graded, view, lambda g: g in STRICT, view_manifest,
                non_source_only=non_source_only)
            dup_strata = stratum_pools(
                key, graded, view, lambda g: g == DUPLICATE_PHOTO,
                view_manifest, non_source_only=non_source_only)
            entry['precision'][column] = stratified_bootstrap(
                strict_strata, N_display, cluster_resamples, cluster_seed,
                unit_interval=True)
            entry['yield'][column] = stratified_bootstrap(
                strict_strata, panel_n_queries, cluster_resamples,
                cluster_seed)
            entry['duplicate_photo_rate'][column] = stratified_bootstrap(
                dup_strata, N_display, cluster_resamples, cluster_seed,
                unit_interval=True)
        entry['relation_mix_by_rank_band'] = relation_mix_by_rank_band(
            key, graded, view, view_manifest)
        views_out[view] = entry

    return {
        'deck': deck_id,
        'n_cards': manifest.get('n_cards'),
        'graded': len(graded),
        'min_graded': min_graded,
        'cluster_seed': cluster_seed,
        'cluster_resamples': cluster_resamples,
        'panel_n_queries': panel_n_queries,
        'grade_vocabulary': sorted(vocab),
        'grade_vocabulary_source': vocab_source,
        'views': views_out,
    }


def render_report(res: dict) -> list:
    """Human-readable report lines for `res` (the score() return value)."""
    lines = [f'deck {res["deck"]}  cards {res.get("n_cards")}  '
            f'graded {res["graded"]}  panel_n_queries '
            f'{res["panel_n_queries"]}']
    if res.get('min_graded'):
        lines.append(f'  --min-graded {res["min_graded"]} satisfied')
    lines.append(f'  grade vocabulary source: {res["grade_vocabulary_source"]}')
    lines.append('')

    def fmt(m):
        if m == INSUFFICIENT:
            return 'INSUFFICIENT'
        return f'{m["point"]:.3f} [{m["ci95"][0]:.3f},{m["ci95"][1]:.3f}]'

    views = sorted(res['views'])
    if not views:
        lines.append('(no view found in deck_manifest.json)')
        return lines

    w = max(len(v) for v in views)
    lines.append(f'{"view":<{w}} {"N_display":>9} {"precision(overall)":>26} '
                f'{"precision(non-src)":>26} {"yield(overall)":>20} '
                f'{"yield(non-src)":>20}')
    lines.append(f'  (stratified bootstrap, seed {res["cluster_seed"]}, '
                f'{res["cluster_resamples"]} resamples)')
    lines.append('-' * (w + 106))
    for v in views:
        e = res['views'][v]
        lines.append(f'{v:<{w}} {e["n_display"]!s:>9} '
                     f'{fmt(e["precision"]["overall"]):>26} '
                     f'{fmt(e["precision"]["non_source"]):>26} '
                     f'{fmt(e["yield"]["overall"]):>20} '
                     f'{fmt(e["yield"]["non_source"]):>20}')
    lines.append('')
    for v in views:
        e = res['views'][v]
        lines.append(f'{v}  duplicate_photo_rate: '
                     f'overall={fmt(e["duplicate_photo_rate"]["overall"])}  '
                     f'non_source={fmt(e["duplicate_photo_rate"]["non_source"])}'
                     f'  n_graded_selections={e["n_graded_selections"]}'
                     f'  multi_stratum_queries={e["multi_stratum_queries"]}')
        for band, m in sorted((e.get('relation_mix_by_rank_band') or {}).items(),
                              key=lambda kv: str(kv[0])):
            mix = '  '.join(f'{g}:{frac:.2f}' for g, frac in
                            sorted(m['mix'].items()))
            lines.append(f'    rank_band {band} (n={m["n"]}): {mix}')
    return lines


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--deck-dir', required=True)
    ap.add_argument('--verdicts', required=True)
    ap.add_argument('--min-graded', type=int, default=0,
                    help='fatal if fewer graded cards than this (0 = off)')
    ap.add_argument('--cluster-seed', type=int,
                    default=CLUSTER_SEED_DEFAULT)
    ap.add_argument('--cluster-resamples', type=int,
                    default=CLUSTER_RESAMPLES_DEFAULT)
    ap.add_argument('--out', default=None)
    args = ap.parse_args()

    res = score(args.deck_dir, args.verdicts, min_graded=args.min_graded,
               cluster_seed=args.cluster_seed,
               cluster_resamples=args.cluster_resamples)

    for line in render_report(res):
        print(line)

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(res, fh, ensure_ascii=False, indent=1, sort_keys=True)
        print(f'\nwrote {args.out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

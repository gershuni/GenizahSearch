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
                      graded once therefore contributes ONCE PER SELECTION,
                      each at that selection's own `pi_h`.
  deck_manifest.json  {"cards_hash", "key_hash", "n_cards", "n_queries",
                      "views": {view: {"N_display", "strata": {stratum:
                      {"N_h", "n_h"}}, "display_counts_by_query":
                      {query_id: int}}}}. `N_display` is the TRUE
                      denominator: the total number of displayed (query,
                      sys_id) cards in that view across the whole panel,
                      known by design, not estimated from the sample.
                      `display_counts_by_query` (added 2026-08-21 after the
                      integration smoke described below) is the per-QUERY
                      breakdown of that same total: how many displayed
                      cards THIS view shows for each query_id, summing
                      exactly to `N_display`.
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

THE 2026-08-21 INTEGRATION-SMOKE FIX -- precision/duplicate_photo_rate
intervals escaping [0, 1]. The first version of this module's bootstrap
resampled QUERIES in the numerator only, while dividing every replicate by
the FIXED, unresampled `N_display` -- correct for the POINT estimate (a
known population total never needs resampling), wrong for the interval: a
replicate that happens to draw display-heavy, high-weight queries inflates
the numerator against a denominator that cannot move with it, so the upper
bound can exceed 1 -- impossible for a share of display slots. Confirmed on
a real integration run: `[0.544, 1.486]`, `[0.651, 1.377]`, `[0.581,
1.502]` on three views whose points were all exactly 1.000.

The fix (`weighted_ratio_cluster_bootstrap`, used by precision and
duplicate_photo_rate, both genuine shares of `N_display`): treat it as a
proper RATIO estimator. Every bootstrap replicate resamples query ids and
recomputes BOTH the numerator (weighted strict/duplicate contribution) AND
the matching per-query denominator (that query's own display count, from
the new `display_counts_by_query`) from the SAME drawn queries, then takes
their ratio. A query's weight can never be counted on one side of the
ratio without its own share of the other. The POINT estimate is UNCHANGED
(`sum(y_i/pi_i) / N_display`, per the instruction that fixing the interval
must not move the number the point already reports correctly).

Because a share of display slots can never legitimately fall outside
[0, 1], `weighted_ratio_cluster_bootstrap` treats an escaped bound as an
INTERNAL BUG and refuses (fatal) rather than clip it away -- clipping would
hide a real defect in the estimator or its inputs behind a plausible-looking
number. Yield has NO such bound (more than one strict manuscript per query
is normal and desirable), so it is never clamped or bound-checked; see
`weighted_cluster_bootstrap`'s own docstring for how its point-vs-bootstrap
denominator asymmetry works instead.

DESIGN DECISIONS MADE WHERE THE CONTRACT ABOVE WAS SILENT (flagged here
rather than guessed away quietly -- see the task report for the same list):

  D1. YIELD's bootstrap denominator is FIXED PER REPLICATE but DIFFERENT
      FROM its point estimate's denominator -- see `weighted_cluster_
      bootstrap`'s docstring for the full reasoning. This is now the only
      surviving "fixed denominator" scheme in this module: PRECISION and
      duplicate_photo_rate no longer use it (see the integration-smoke fix
      above) precisely because a share-of-slots ratio cannot tolerate a
      denominator that does not move with a resampled numerator, while a
      per-query RATE (yield) has no such constraint.
  D2. The query-resampling universe for a given (view, column) is now the
      FULL set of queries that view's `display_counts_by_query` names --
      resolved by the 2026-08-21 fix for any query the view displays
      something for (a query with zero GRADED cards in that view still
      resamples correctly, contributing 0 to the numerator and its real
      display count to the denominator, per the coordinator's
      "conservative reading"). What remains genuinely unresolvable from
      the given files: a panel query with LITERALLY ZERO candidates in
      EVERY view (absent from every view's `display_counts_by_query`,
      since neither deck_key.json nor prereg.json enumerates the full
      panel query_id list, only `panel_n_queries`, a count). Such a
      query's contribution is deterministically zero regardless of being
      "drawn", so the point estimate is unaffected; the bootstrap variance
      is very slightly narrower than a hypothetical full-population
      resample would give.
  D3. "Restricted to non-source cards" (item 3 of the spec) filters the
      NUMERATOR only; the denominator -- both the point estimate's fixed
      `N_display` / `panel_n_queries` AND the ratio bootstrap's per-query
      `display_counts_by_query` -- is shared, unfiltered, identical between
      the "overall" and "non_source" columns. A non-source-only
      denominator (i.e. "accuracy among just the non-source slots") is NOT
      computed, because neither `N_display` nor `display_counts_by_query`
      is broken out by `is_source` in the given schema, so a true
      non-source population size is not derivable without fabricating an
      estimated sub-population (a Hajek-type ratio), which would mix
      estimator types within one report. The chosen reading is honest and
      simple to hand-verify: "what share of ALL displayed slots is a
      genuine non-source strict match" rather than "accuracy specifically
      among non-source slots".
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
      `is_source` -- the spec asks for one such breakdown, not two.
  D7. The "zero graded cards -> INSUFFICIENT" rule (spec's validation
      section) is applied independently to EACH (view, column) pair, not
      only at the whole-view grain: a view can have plenty of graded
      overall cards yet zero graded non-source cards, and reporting a
      fabricated 0.000 for that column would be exactly the failure mode
      the rule exists to prevent.
  D8. `is_source` is NOT always a bool, despite the input contract's
      "is_source": bool -- `scripts/build_display_deck.py`'s own
      `compute_is_source()` (imported from build_grading_deck.py) returns
      None when a query row lacks `meta.sys_id`, "so a query file without
      that field degrades honestly instead of crashing or silently
      reporting False" (that function's own docstring). This module's
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
# real escape (the bug this guards against overshot by tenths, not 1e-9).
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
        n_h/N_h fields);
      - for every (view, stratum) pair, the number of selections actually
        present in deck_key.json does not exceed that (view, stratum)'s
        n_h as declared in deck_manifest.json (a pair absent from the
        manifest is treated as declaring n_h=0, so any selection against
        an undeclared view/stratum is refused too).

    See `validate_display_counts` for the SEPARATE, newer set of checks
    over `display_counts_by_query`.
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

    if not (empty_selection_ids or range_violations or nh_violations
            or cap_violations):
        return

    total = (len(empty_selection_ids) + len(range_violations)
             + len(nh_violations) + len(cap_violations))
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
    raise SystemExit('\n'.join(lines))


def validate_display_counts(key_list: list, manifest: dict) -> None:
    """Fatal (SystemExit), never a silent skip, on any defect in
    deck_manifest.json's `display_counts_by_query` (per view: the number of
    displayed (query, sys_id) cards that view shows for each query -- the
    per-query denominator the ratio bootstrap needs, added 2026-08-21 after
    an integration smoke exposed a fixed-denominator bootstrap escaping
    [0, 1]). Every defect kind is counted across the WHOLE manifest before
    raising, same fail-closed style as `validate_structure`.

    `scripts/build_display_deck.py` asserts these invariants itself at
    write time; this is "never trust, verify" on the read side -- this
    module must not silently assume a producer's own internal assertion
    always ran, or ran correctly, on the specific file it was handed.

    Checks, per view declared in deck_manifest.json:
      - `display_counts_by_query` is present and is a mapping;
      - every value in it is a positive number;
      - its values sum EXACTLY to that view's `N_display`;
      - every query_id referenced by ANY selection for that view in
        deck_key.json is a key in that view's display_counts_by_query (a
        graded card's query cannot be a mystery to the very denominator
        its own view's ratio bootstrap needs).
    """
    views_manifest = manifest.get('views') or {}

    missing_field = []
    non_positive = []
    sum_mismatches = []
    orphan_queries = []

    selection_queries_by_view: dict = collections.defaultdict(set)
    for card in key_list:
        qid = card.get('query_id')
        for sel in card.get('selections') or []:
            selection_queries_by_view[sel.get('view')].add(qid)

    for view, view_manifest in sorted(views_manifest.items()):
        dcbq = view_manifest.get('display_counts_by_query')
        if not isinstance(dcbq, dict) or not dcbq:
            missing_field.append(view)
            continue
        for qid, count in dcbq.items():
            if not isinstance(count, (int, float)) or count <= 0:
                non_positive.append((view, qid, count))
        total = sum(v for v in dcbq.values() if isinstance(v, (int, float)))
        n_display = view_manifest.get('N_display')
        if total != n_display:
            sum_mismatches.append((view, total, n_display))
        for qid in selection_queries_by_view.get(view, ()):
            if qid not in dcbq:
                orphan_queries.append((view, qid))

    if not (missing_field or non_positive or sum_mismatches
            or orphan_queries):
        return

    total_defects = (len(missing_field) + len(non_positive)
                    + len(sum_mismatches) + len(orphan_queries))
    lines = [f'REFUSING to score: {total_defects} display_counts_by_query '
            f'defect(s) in deck_manifest.json']
    if missing_field:
        lines.append(f'  {len(missing_field)} view(s) missing or empty '
                    f'display_counts_by_query: {missing_field}')
    if non_positive:
        lines.append(f'  {len(non_positive)} non-positive '
                    f'display_counts_by_query value(s): {non_positive[:5]}')
    if sum_mismatches:
        lines.append(f'  {len(sum_mismatches)} view(s) whose '
                    f'display_counts_by_query does not sum to N_display: '
                    f'{sum_mismatches[:5]}')
    if orphan_queries:
        lines.append(f'  {len(orphan_queries)} (view, query_id) pair(s) '
                    f'with a selection but absent from '
                    f'display_counts_by_query: {orphan_queries[:5]}')
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


def dense_numerator_by_query(key: dict, graded: dict, view: str, predicate,
                             query_universe, non_source_only: bool = False) -> dict:
    """{query_id: weighted_sum} for EVERY query_id in `query_universe` --
    DENSE, never sparse: a query with no matching graded selection gets an
    explicit 0.0 rather than being omitted, so a cluster bootstrap over the
    view's FULL set of displaying queries (`query_universe`, from
    `display_counts_by_query`) can resample it and correctly contribute
    nothing to the numerator while a ratio bootstrap elsewhere still
    charges it its real display count in the denominator (D2/D4: the
    2026-08-21 fix's "conservative reading").

    `query_universe` is trusted to already contain every query any
    matching selection could reference -- `validate_display_counts`
    enforces that upstream, so this function does not re-check it on the
    hot path.
    """
    out = {q: 0.0 for q in query_universe}
    for cid, card in key.items():
        if cid not in graded:
            continue
        if non_source_only and card.get('is_source'):
            continue
        grade = graded[cid]
        if not predicate(grade):
            continue
        for sel in card.get('selections') or []:
            if sel.get('view') != view:
                continue
            out[card['query_id']] += 1.0 / sel['pi_h']
    return out


def weighted_cluster_bootstrap(numer_by_query: dict, point_denom: float,
                               bootstrap_denom: float, resamples: int,
                               seed: int) -> dict:
    """Point estimate = sum(numer)/point_denom; CI resamples query ids
    (query-clustered, mirroring scripts/analyze_paired_outcomes.py's
    seeded random.Random(seed) + rng.randrange(len(keys)) idiom) and
    divides each replicate's resampled numerator sum by `bootstrap_denom`
    -- FIXED per replicate, because every replicate draws the SAME NUMBER
    of query ids (len(keys)). This is the right shape for YIELD, the only
    caller left after the 2026-08-21 fix moved precision and
    duplicate_photo_rate to `weighted_ratio_cluster_bootstrap` instead
    (see the module docstring's integration-smoke section): yield's
    natural bootstrap denominator IS that draw count, a per-query RATE,
    not a resampled display-slot total.

    `point_denom` and `bootstrap_denom` are DELIBERATELY not always equal
    (D1): yield's point divides by the pre-registered `panel_n_queries`
    (the full panel size, a stable cross-deck constant used so the
    headline number is comparable across decks and views regardless of
    how many queries any one view happened to display something for),
    while its bootstrap CI divides by `len(keys)` (the number of queries
    THIS VIEW actually displays anything for, which can be smaller than
    `panel_n_queries` if some panel queries have zero candidates in this
    view). The CI therefore characterises sampling variability in the
    rate among DISPLAYING queries, on a potentially different scale than
    the panel-wide point estimate -- both are correct for what they
    separately claim to measure. Yield has no upper bound (more than one
    strict manuscript per query is normal), so unlike the ratio bootstrap,
    nothing here is ever asserted into [0, 1].
    """
    keys = sorted(numer_by_query)
    point = sum(numer_by_query.values()) / point_denom if point_denom else 0.0

    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        s = 0.0
        for _ in range(len(keys)):
            g = keys[rng.randrange(len(keys))]
            s += numer_by_query[g]
        stats.append(s / bootstrap_denom if bootstrap_denom else 0.0)
    stats.sort()

    def q_(p: float) -> float:
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    return {
        'point': round(point, 4),
        'ci95': [round(q_(0.025), 4), round(q_(0.975), 4)],
        'n_groups': len(keys),
    }


def weighted_ratio_cluster_bootstrap(numer_by_query: dict, denom_by_query: dict,
                                     point_denom: float, resamples: int,
                                     seed: int, *, unit_interval: bool = False) -> dict:
    """A RATIO bootstrap for share-of-display-slots estimators (precision,
    duplicate_photo_rate): each replicate resamples query ids with
    replacement and recomputes BOTH the numerator and the matching
    denominator from the SAME drawn queries, then takes their ratio --
    unlike `weighted_cluster_bootstrap`, whose bootstrap denominator is a
    single FIXED number shared by every replicate.

    Why this exists (the 2026-08-21 integration-smoke bug this function
    fixes -- see the module docstring for the full account): holding
    `N_display` fixed while only resampling the numerator lets a replicate
    that happens to draw display-heavy, high-weight queries inflate the
    numerator against an unchanged denominator, producing a CI bound
    ABOVE 1 -- impossible for a share of display slots. Resampling the
    matching per-query denominator alongside the numerator keeps both
    sides of the ratio drawn from the SAME queries, so a query's weight is
    never counted on one side without its own share of the other.

    `numer_by_query` and `denom_by_query` MUST share the same key set --
    the FULL, dense set of queries this view displays anything for
    (`deck_manifest.json`'s `display_counts_by_query`), not just the
    subset with a graded card: a displaying-but-ungraded query still
    consumes real denominator mass and must be resampleable, contributing
    0 to the numerator and its true display count to the denominator (D2,
    the coordinator's "conservative reading").

    `point_denom` is the FIXED, external population constant (`N_display`)
    the POINT estimate divides by -- UNCHANGED by this fix; only the CI
    changed.

    unit_interval: a share-of-slots point AND its CI bounds can never
    legitimately fall outside [0, 1]. If they do (checked against the
    UNROUNDED bootstrap quantiles, past a tiny float epsilon), that is an
    INTERNAL BUG in the estimator or its inputs -- refuse loudly rather
    than clip the number away, so a real defect is never hidden behind a
    plausible-looking, silently-truncated one.
    """
    keys = sorted(numer_by_query)
    if sorted(denom_by_query) != keys:
        raise SystemExit(
            'INTERNAL BUG: numerator and denominator query universes '
            'differ in a ratio bootstrap -- refusing to resample a '
            'mismatched pair')
    point = sum(numer_by_query.values()) / point_denom if point_denom else 0.0

    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        num = den = 0.0
        for _ in range(len(keys)):
            g = keys[rng.randrange(len(keys))]
            num += numer_by_query[g]
            den += denom_by_query[g]
        stats.append(num / den if den else 0.0)
    stats.sort()

    def q_(p: float) -> float:
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    lo_raw, hi_raw = q_(0.025), q_(0.975)

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
        'n_groups': len(keys),
    }


def relation_mix_by_rank_band(key: dict, graded: dict, view: str) -> dict:
    """{rank_band: {"n": int, "mix": {grade: weighted_fraction}}} for every
    GRADED selection in `view`, weighted by 1/pi_h and normalised to sum to
    1 within each band (D6: source and non-source combined). A band with
    zero graded selections is simply absent -- never a fabricated entry.
    """
    weights: dict = collections.defaultdict(lambda: collections.defaultdict(float))
    counts: collections.Counter = collections.Counter()
    for cid, card in key.items():
        if cid not in graded:
            continue
        grade = graded[cid]
        for sel in card.get('selections') or []:
            if sel.get('view') != view:
                continue
            weights[sel.get('rank_band')][grade] += 1.0 / sel['pi_h']
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
    validate_display_counts(key_list, manifest)

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
        display_counts = view_manifest.get('display_counts_by_query') or {}
        query_universe = set(display_counts)
        entry = {
            'n_display': N_display,
            'n_graded_selections': graded_selection_count(key, graded, view),
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
            strict_numer = dense_numerator_by_query(
                key, graded, view, lambda g: g in STRICT, query_universe,
                non_source_only=non_source_only)
            dup_numer = dense_numerator_by_query(
                key, graded, view, lambda g: g == DUPLICATE_PHOTO,
                query_universe, non_source_only=non_source_only)
            entry['precision'][column] = weighted_ratio_cluster_bootstrap(
                strict_numer, display_counts, N_display,
                cluster_resamples, cluster_seed, unit_interval=True)
            entry['yield'][column] = weighted_cluster_bootstrap(
                strict_numer, panel_n_queries, len(query_universe),
                cluster_resamples, cluster_seed)
            entry['duplicate_photo_rate'][column] = weighted_ratio_cluster_bootstrap(
                dup_numer, display_counts, N_display,
                cluster_resamples, cluster_seed, unit_interval=True)
        entry['relation_mix_by_rank_band'] = relation_mix_by_rank_band(
            key, graded, view)
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
    lines.append(f'  (weighted, query-clustered bootstrap, seed '
                f'{res["cluster_seed"]}, {res["cluster_resamples"]} '
                f'resamples)')
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
                     f'  n_graded_selections={e["n_graded_selections"]}')
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

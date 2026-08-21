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
                      {"N_h", "n_h"}}}}}. `N_display` is the TRUE
                      denominator: the total number of displayed (query,
                      sys_id) cards in that view across the whole panel,
                      known by design, not estimated from the sample.
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

DESIGN DECISIONS MADE WHERE THE CONTRACT ABOVE WAS SILENT (flagged here
rather than guessed away quietly -- see the task report for the same list):

  D1. Bootstrap denominator is FIXED, not resampled. `N_display` and
      `panel_n_queries` are design-known population constants, not counts
      of the graded sample -- unlike `scripts/analyze_paired_outcomes.py`'s
      `cluster_bootstrap_diff`, where the denominator IS a resampled count
      of shared queries (a genuine sample-size quantity there). Each
      bootstrap replicate here resamples the QUERY-GROUPED numerator only
      and divides by the one true, unchanging denominator.
  D2. The query-resampling universe for a given (view, column) is the set
      of query_ids carrying at least one GRADED selection contributing to
      that column's numerator. A panel query with literally zero displayed
      candidates in every view (and therefore absent from deck_key.json
      entirely) cannot be represented as a distinct resampling unit, because
      neither deck_key.json nor prereg.json enumerates the full panel
      query_id list -- only `panel_n_queries`, a count. Such a query's true
      contribution is a deterministic zero regardless of whether it is
      "drawn", so the point estimate is unaffected; the bootstrap variance
      is very slightly narrower than a hypothetical full-population
      resample would give. Flagged as underspecified, not silently assumed.
  D3. "Restricted to non-source cards" (item 3 of the spec) filters the
      NUMERATOR only; the denominator stays the same fixed `N_display` /
      `panel_n_queries` used for the overall figure. A non-source-only
      denominator (i.e. "accuracy among just the non-source slots") is NOT
      computed, because deck_manifest.json's schema reports `N_display` and
      each stratum's `N_h`/`n_h` only at the (view, stratum) grain -- it
      never splits by `is_source` -- so a true non-source population size
      is not derivable from the given files without fabricating an
      estimated sub-population (a Hajek-type ratio), which would mix
      estimator types (Horvitz-Thompson overall, Hajek non-source) within
      one report. The chosen reading is honest and simple to hand-verify:
      "what share of ALL displayed slots is a genuine non-source strict
      match" rather than "accuracy specifically among non-source slots".
  D4. prereg.json's grade-vocabulary override key name is not given in the
      spec (only "panel_n_queries" is shown in the example). This module
      accepts `prereg["grade_vocabulary"]`, falling back to
      `prereg["vocabulary"]`, falling back to the same 8-term vocabulary
      `scripts/score_grading_deck.py` uses (duplicated below, not imported,
      for the same reason that module gives for duplicating its own copy
      of `sha()`: no coupling to a sibling script's import graph).
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


def weighted_sum_by_query(key: dict, graded: dict, view: str, predicate,
                          non_source_only: bool = False) -> dict:
    """{query_id: [weighted 0/1 contributions]} for every GRADED selection
    in `view` (optionally non-source-only) matching `predicate(grade)`.
    A card selected twice in the SAME view contributes twice, each at that
    selection's own `pi_h` -- this loop is keyed on (card, selection), not
    on card alone.
    """
    out: dict = collections.defaultdict(list)
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
            out[card['query_id']].append(y / sel['pi_h'])
    return dict(out)


def weighted_cluster_bootstrap(contribs_by_query: dict, denom: float,
                               resamples: int, seed: int) -> dict:
    """Point estimate + 95% percentile CI for sum(contribs)/denom, where
    `denom` is a FIXED, known population constant (D1) and only the
    query-grouped numerator is resampled (query-clustered, mirroring
    scripts/analyze_paired_outcomes.py::cluster_bootstrap_diff's seeded
    random.Random(seed) + rng.randrange(len(keys)) idiom, adapted to a
    fixed-denominator ratio instead of a resampled-denominator one).

    Caller guarantees `contribs_by_query` is non-empty and `denom` > 0
    (an empty/zero case is the INSUFFICIENT sentinel upstream, not this
    function's concern).
    """
    keys = sorted(contribs_by_query)
    total = sum(sum(v) for v in contribs_by_query.values())
    point = total / denom

    rng = random.Random(seed)
    stats = []
    for _ in range(resamples):
        s = 0.0
        for _ in range(len(keys)):
            g = keys[rng.randrange(len(keys))]
            s += sum(contribs_by_query[g])
        stats.append(s / denom)
    stats.sort()

    def q_(p: float) -> float:
        return stats[min(len(stats) - 1, int(p * len(stats)))]

    return {
        'point': round(point, 4),
        'ci95': [round(q_(0.025), 4), round(q_(0.975), 4)],
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
            strict_c = weighted_sum_by_query(
                key, graded, view, lambda g: g in STRICT,
                non_source_only=non_source_only)
            dup_c = weighted_sum_by_query(
                key, graded, view, lambda g: g == DUPLICATE_PHOTO,
                non_source_only=non_source_only)
            entry['precision'][column] = weighted_cluster_bootstrap(
                strict_c, N_display, cluster_resamples, cluster_seed)
            entry['yield'][column] = weighted_cluster_bootstrap(
                strict_c, panel_n_queries, cluster_resamples, cluster_seed)
            entry['duplicate_photo_rate'][column] = weighted_cluster_bootstrap(
                dup_c, N_display, cluster_resamples, cluster_seed)
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

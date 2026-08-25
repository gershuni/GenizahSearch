# -*- coding: utf-8 -*-
"""Diff two passage-matching policies on ONE query, by manuscript.

Every policy claim is a DELTA -- "this setting finds witnesses that one does
not" -- and a claim of that shape is only checkable if both runs are put side
by side on the same query and the same index, with the added records listed
by shelfmark so a scholar can grade them. Latency and self-retrieval live in
scripts/bench_passage_query.py; this script answers "what changed, and is it
any good". It is what measured the passage-length profiles (spec section 8.1)
and what retired the anchor-evidence tier (section 10.4).

Output columns are deliberately gradeable, not just countable: every row
carries the shelfmark, a normalized join key, the catalogue title, which
run(s) returned it, and its score.

Usage:
  python scripts/compare_passage_policies.py --index passage_index/current \\
      --query-file antiochus.txt

  python scripts/compare_passage_policies.py --index ... --query-file ... \\
      --baseline widest-40 --candidate short-28 --csv delta.csv

The query file is plain UTF-8 text (paste the composition into it). Nothing
here writes to the index or the app's state -- policy is query-side by
design, so no rebuild is ever needed to try a new one.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import replace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.passage_index import (  # noqa: E402
    MANIFEST_NAME, diagnose_index, open_index,
)
from shared.passage_policy import (  # noqa: E402
    DEFAULT_DEPTH, DEFAULT_LENGTH, DEPTH_PROFILES, LENGTH_PROFILES, PRESETS,
    compose,
)
from shelfmark_join import shelfmark_key  # noqa: E402,F401
from shared.passage_search import search_passage  # noqa: E402


def _nearby_index_dirs(index_dir: str) -> list:
    """Sibling directories that DO look like an index, as a next step.

    The likeliest cause of a failed open is simply the wrong directory name
    -- the index lives outside the repo, is gitignored, and different
    machines have used `current/`, `full_v1/` and dated names. Pointing at
    the real ones beats making the operator go hunting.
    """
    hints: list = []
    try:
        parent = os.path.dirname(os.path.abspath(index_dir)) or '.'
        if not os.path.isdir(parent):
            return hints
        for name in sorted(os.listdir(parent)):
            cand = os.path.join(parent, name)
            if (os.path.abspath(cand) != os.path.abspath(index_dir)
                    and os.path.isdir(cand)
                    and os.path.isfile(os.path.join(cand, MANIFEST_NAME))):
                hints.append(f'an index appears to be here: {cand}')
    except Exception:
        pass
    return hints[:5]


def load_shelfmarks(csv_path: str) -> dict:
    """sys_id -> (shelfmark, library_code, title) from libraries.csv.

    Optional enrichment: an unreadable or absent file degrades to bare
    sys_ids with a warning, never a crash -- the delta is still gradeable by
    shelfmark-less record id, just less pleasantly.
    """
    out: dict = {}
    if not csv_path or not os.path.exists(csv_path):
        return out
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='replace',
                  newline='') as fh:
            for row in csv.reader(fh):
                if len(row) < 4 or not row[0].strip():
                    continue
                sys_id = ''.join(ch for ch in row[0] if ch.isdigit())
                if not sys_id:
                    continue
                shelf = (row[2] or '').split('|')[0].strip()
                title = row[7].strip() if len(row) > 7 else ''
                out[sys_id] = (shelf, (row[3] or '').strip(), title)
    except Exception as exc:  # pragma: no cover - operator convenience path
        print(f'  ! libraries.csv unreadable ({exc}); '
              f'continuing without shelfmarks', file=sys.stderr)
    return out


# The shelfmark join lives in ONE place (scripts/shelfmark_join.py), shared
# with scripts/score_antiochus_deck.py: a join rule defined twice is a join
# rule that drifts, and this one failing silently already cost two wrong
# measurements. The normalized key ships as its own CSV column rather than
# being left to each consumer to re-derive.


def sys_id_of(record_id: str) -> str:
    """Leading numeric component of '{sys}_{IE}_{P}_{FL}'."""
    head = record_id.split('_', 1)[0]
    return ''.join(ch for ch in head if ch.isdigit())


def run(idx, text: str, policy):
    """One search -> (by_sys_id, report). Records collapse to MANUSCRIPTS:
    a manuscript counts once, keeping its best row, because that is the unit
    a scholar grades (and the unit the adjudicated decks use)."""
    hits, report = search_passage(idx, text, policy)
    best: dict = {}
    for h in hits:
        sid = sys_id_of(h.record_id)
        prev = best.get(sid)
        if prev is None or h.score > prev.score:
            best[sid] = h
    return best, report


def describe(report) -> str:
    bits = [f"{report.seconds:.2f}s",
            f"cand={report.candidates}",
            f"verified={report.verified}",
            f"spans={report.accepted_spans}"]
    if report.verify_truncated:
        bits.append('VERIFY-CAP HIT')
    if report.candidates_truncated:
        bits.append('CAND-CAP HIT')
    return '  '.join(bits)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--index', required=True, help='passage index directory')
    ap.add_argument('--query-file', required=True,
                    help='UTF-8 text file holding the query composition')
    ap.add_argument('--baseline', default='widest-40', choices=sorted(PRESETS))
    ap.add_argument('--candidate', default='short-28',
                    choices=sorted(PRESETS))
    # The other two policy axes, per side. `--baseline`/`--candidate` name a
    # WIDTH preset only, so before these existed the committed instrument
    # could not express -- let alone reproduce -- the depth measurements this
    # milestone rests on: `--candidate deep` was simply rejected by argparse,
    # because `deep` lives in DEPTH_PROFILES and reaches a policy only through
    # `compose()`. Per side, because the depth table compares depths against
    # each other at a fixed width, which is a baseline-vs-candidate run.
    for _side, _dflt in (('baseline', 'widest-40'), ('candidate', 'short-28')):
        ap.add_argument(f'--{_side}-length', default=DEFAULT_LENGTH,
                        choices=sorted(LENGTH_PROFILES),
                        help=f'passage-length profile for the {_side} '
                             f'(default: {DEFAULT_LENGTH})')
        ap.add_argument(f'--{_side}-depth', default=DEFAULT_DEPTH,
                        choices=sorted(DEPTH_PROFILES),
                        help=f'search-depth profile for the {_side} '
                             f'(default: {DEFAULT_DEPTH}). Deeper costs '
                             f'proportionally more time')
    ap.add_argument('--libraries-csv', default='',
                    help='libraries.csv for shelfmarks (optional; '
                         'default: <index>/../libraries.csv then ./libraries.csv)')
    ap.add_argument('--csv', default='', help='write the full table here')
    ap.add_argument('--show', type=int, default=40,
                    help='rows of the ADDED-by-candidate list to print (0=all)')
    # Probe knobs, so a (min_span, verify_margin) sweep does not need a new
    # preset per cell. A probe is a DIFFERENT policy and is named and hashed
    # as one -- never a preset's id on altered settings.
    ap.add_argument('--min-span', type=int, default=0,
                    help='override the candidate policy min_span (0=keep). '
                         'Sweep together with --verify-margin: below 40 the '
                         'margin, not the span floor, decides the result. '
                         'Applied ON TOP of --candidate-length, for sweeping '
                         'BETWEEN the named points rather than instead of '
                         'them')
    ap.add_argument('--verify-margin', type=int, default=-1,
                    help='override the candidate policy verify_margin '
                         '(-1=keep, 0 is legal and means no extension)')
    ap.add_argument('--rank-of', default='',
                    help='comma-separated shelfmark substrings: report each '
                         'one\'s RANK within the candidate run, or that it '
                         'is genuinely absent')
    args = ap.parse_args()

    with open(args.query_file, 'r', encoding='utf-8-sig') as fh:
        text = fh.read()
    if not text.strip():
        print('query file is empty', file=sys.stderr)
        return 2

    idx = open_index(args.index)
    if idx is None:
        print(f'index failed to open (fail-closed): '
              f'{os.path.abspath(args.index)}', file=sys.stderr)
        print(f'  reason: {diagnose_index(args.index)}', file=sys.stderr)
        for hint in _nearby_index_dirs(args.index):
            print(f'  hint: {hint}', file=sys.stderr)
        return 2

    csv_path = args.libraries_csv
    if not csv_path:
        for guess in (os.path.join(os.path.dirname(os.path.abspath(args.index)),
                                   'libraries.csv'),
                      'libraries.csv'):
            if os.path.exists(guess):
                csv_path = guess
                break
    shelf = load_shelfmarks(csv_path)

    # Through compose(), never a hand-rolled replace(): it is the shared
    # entry point for all three axes, it derives the composed name, and
    # policy_id is a content hash -- so a probe run stays traceable to
    # exactly the settings that produced it.
    base_p = compose(args.baseline, args.baseline_length, args.baseline_depth)
    cand_p = compose(args.candidate, args.candidate_length,
                     args.candidate_depth)
    overrides = {}
    if args.min_span:
        overrides['min_span'] = args.min_span
    if args.verify_margin >= 0:
        overrides['verify_margin'] = args.verify_margin
    if overrides:
        cand_p = replace(cand_p, name=f'{cand_p.name}+probe', **overrides)
        print(f'probe overrides: {overrides}')
    if base_p.name != args.baseline or cand_p.name != args.candidate:
        print(f'policies: baseline={base_p.name}  candidate={cand_p.name}')
    print(f'query: {len(text)} chars from {args.query_file}')
    print(f'index: {args.index}  ({idx.n_records:,} records)')
    if shelf:
        print(f'shelfmarks: {len(shelf):,} from {csv_path}')
    print()

    base, base_rep = run(idx, text, base_p)
    print(f'{args.baseline:<18} {len(base):>5} manuscripts   '
          f'[{base_p.policy_id}]  {describe(base_rep)}')
    cand, cand_rep = run(idx, text, cand_p)
    print(f'{args.candidate:<18} {len(cand):>5} manuscripts   '
          f'[{cand_p.policy_id}]  {describe(cand_rep)}')
    print()

    added = sorted(set(cand) - set(base), key=lambda s: -cand[s].score)
    lost = sorted(set(base) - set(cand))
    both = set(base) & set(cand)

    print(f'shared: {len(both)}   added by {args.candidate}: {len(added)}   '
          f'absent from {args.candidate}: {len(lost)}')
    if lost:
        print('  ! the candidate LOST manuscripts -- expected only if the '
              'policies differ in span width, not by the anchor tier alone')
    print()

    def label(sid: str) -> tuple:
        s, lib, title = shelf.get(sid, ('', '', ''))
        return (s or f'sys {sid}', lib, title)

    print(f'--- added by {args.candidate} ({len(added)}) ---')
    shown = added if args.show <= 0 else added[:args.show]
    for sid in shown:
        h = cand[sid]
        sm, lib, title = label(sid)
        print(f'  {int(h.score):>6} letters  {sm:<26} '
              f'{lib:<12} {str(title)[:52]}')
    if len(added) > len(shown):
        print(f'  ... {len(added) - len(shown)} more (use --show 0 or --csv)')

    if args.rank_of:
        print()
        print('--- rank probe (candidate run, manuscripts in returned order) ---')
        ordered = sorted(cand.items(), key=lambda kv: -kv[1].score)
        for needle in [x.strip() for x in args.rank_of.split(',') if x.strip()]:
            hitrows = [(i, sid, h) for i, (sid, h) in enumerate(ordered, 1)
                       if needle in label(sid)[0] or needle == sid]
            if not hitrows:
                print(f'  {needle:<26} ABSENT -- not returned at all '
                      f'(no cap would recover it)')
            for i, sid, h in hitrows[:3]:
                print(f'  {needle:<26} rank {i} of {len(ordered)} '
                      f'{int(h.score)} letters')

    if args.csv:
        with open(args.csv, 'w', encoding='utf-8-sig', newline='') as fh:
            w = csv.writer(fh)
            # The COMPOSED names and their content-hash ids, not the raw
            # --baseline/--candidate width arguments. Those were unambiguous
            # until width, length and depth became three axes: two runs at
            # different depths, or with different probe overrides, otherwise
            # write identical provenance and their archives cannot be told
            # apart. policy_id is a content hash, so it pins the settings even
            # for a composition nobody has named.
            w.writerow(['sys_id', 'shelfmark', 'shelfmark_key', 'library',
                        'title', 'presence', 'score', 'score_unit',
                        'record_id',
                        'baseline_policy', 'baseline_policy_id',
                        'candidate_policy', 'candidate_policy_id'])
            for sid in sorted(set(base) | set(cand)):
                h = cand.get(sid) or base[sid]
                presence = ('both' if sid in base and sid in cand
                            else ('candidate_only' if sid in cand
                                  else 'baseline_only'))
                sm, lib, title = label(sid)
                w.writerow([sid, sm, shelfmark_key(sm), lib, title, presence,
                            int(h.score), 'matched_letters', h.record_id,
                            base_p.name, base_p.policy_id,
                            cand_p.name, cand_p.policy_id])
        print(f'\nwrote {args.csv}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

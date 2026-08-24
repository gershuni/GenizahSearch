# -*- coding: utf-8 -*-
"""Diff two passage-matching policies on ONE query, by manuscript.

Built for the anchor-evidence tier (spec section 10.4), whose whole claim is
about a DELTA: "anchor-sweep-40 finds witnesses max-40 cannot". A claim of
that shape is only checkable if the two runs are put side by side on the same
query and the same index, and the added records are listed by shelfmark so a
scholar can grade them. Latency and self-retrieval live in
scripts/bench_passage_query.py; this script answers "what changed, and is it
any good".

Output columns are deliberately gradeable, not just countable: every row
carries the shelfmark, the catalogue title, which run(s) returned it, its
tier, and its score. Anchor-tier scores are DISTINCT ANCHOR CODES and span
scores are MATCHED LETTERS -- different units, never comparable across the
tier boundary, so the tier is always printed beside the number.

Usage:
  python scripts/compare_passage_policies.py --index passage_index/current \\
      --query-file antiochus.txt

  python scripts/compare_passage_policies.py --index ... --query-file ... \\
      --baseline widest-40 --candidate anchor-sweep-40 --csv delta.csv

The query file is plain UTF-8 text (paste the composition into it). Nothing
here writes to the index or the app's state -- policy is query-side by
design, so no rebuild is ever needed to try a new one.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import (  # noqa: E402
    MANIFEST_NAME, diagnose_index, open_index,
)
from shared.passage_policy import PRESETS, get_preset  # noqa: E402
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
        # Span beats anchor for the representative row; within a tier, the
        # higher score wins. Never compare a span score to an anchor score.
        if prev is None:
            best[sid] = h
            continue
        if prev.tier == h.tier:
            if h.score > prev.score:
                best[sid] = h
        elif h.tier == 'span':
            best[sid] = h
    return best, report


def describe(report) -> str:
    bits = [f"{report.seconds:.2f}s",
            f"cand={report.candidates}",
            f"verified={report.verified}",
            f"spans={report.accepted_spans}"]
    if report.anchor_tier_enabled:
        bits.append(f"anchor_records={report.anchor_records}")
        if report.anchor_truncated:
            bits.append('ANCHOR-CAP HIT')
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
    ap.add_argument('--candidate', default='anchor-sweep-40',
                    choices=sorted(PRESETS))
    ap.add_argument('--libraries-csv', default='',
                    help='libraries.csv for shelfmarks (optional; '
                         'default: <index>/../libraries.csv then ./libraries.csv)')
    ap.add_argument('--csv', default='', help='write the full table here')
    ap.add_argument('--show', type=int, default=40,
                    help='rows of the ADDED-by-candidate list to print (0=all)')
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

    base_p, cand_p = get_preset(args.baseline), get_preset(args.candidate)
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

    added = sorted(set(cand) - set(base),
                   key=lambda s: (cand[s].tier != 'span', -cand[s].score))
    lost = sorted(set(base) - set(cand))
    both = set(base) & set(cand)
    # A manuscript the baseline found by SPAN must never arrive as anchor-only
    # in the candidate: the candidate's spans are a superset by construction.
    demoted = [s for s in both
               if base[s].tier == 'span' and cand[s].tier == 'anchor']

    print(f'shared: {len(both)}   added by {args.candidate}: {len(added)}   '
          f'absent from {args.candidate}: {len(lost)}')
    if lost:
        print('  ! the candidate LOST manuscripts -- expected only if the '
              'policies differ in span width, not by the anchor tier alone')
    if demoted:
        print(f'  !! {len(demoted)} span->anchor demotions: a contract '
              f'violation, report it ({", ".join(demoted[:5])})')
    print()

    def label(sid: str) -> tuple:
        s, lib, title = shelf.get(sid, ('', '', ''))
        return (s or f'sys {sid}', lib, title)

    n_anchor = sum(1 for s in added if cand[s].tier == 'anchor')
    print(f'--- added by {args.candidate} '
          f'({len(added) - n_anchor} span, {n_anchor} anchor) ---')
    shown = added if args.show <= 0 else added[:args.show]
    for sid in shown:
        h = cand[sid]
        sm, lib, title = label(sid)
        unit = 'codes' if h.tier == 'anchor' else 'letters'
        print(f'  [{h.tier:<6}] {int(h.score):>5} {unit:<7} {sm:<26} '
              f'{lib:<12} {str(title)[:52]}')
    if len(added) > len(shown):
        print(f'  ... {len(added) - len(shown)} more (use --show 0 or --csv)')

    if args.csv:
        with open(args.csv, 'w', encoding='utf-8-sig', newline='') as fh:
            w = csv.writer(fh)
            w.writerow(['sys_id', 'shelfmark', 'library', 'title', 'presence',
                        'tier', 'score', 'score_unit', 'record_id',
                        'baseline_policy', 'candidate_policy'])
            for sid in sorted(set(base) | set(cand)):
                h = cand.get(sid) or base[sid]
                presence = ('both' if sid in base and sid in cand
                            else ('candidate_only' if sid in cand
                                  else 'baseline_only'))
                sm, lib, title = label(sid)
                w.writerow([sid, sm, lib, title, presence, h.tier,
                            int(h.score),
                            'anchor_codes' if h.tier == 'anchor'
                            else 'matched_letters',
                            h.record_id, args.baseline, args.candidate])
        print(f'\nwrote {args.csv}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

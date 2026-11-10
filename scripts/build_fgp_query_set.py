# -*- coding: utf-8 -*-
"""Build the FGP query set for the method comparison.

GROUND TRUTH IS IDENTIFIER-BASED, AT SYS_ID GRAIN -- and that is a deliberate
retreat from the plan's page-grain design, for a reason worth stating.

The research CER script pairs an FGP transcription to a specific HTR page by
sys_id PLUS FUZZY CONTENT MATCHING (rapidfuzz partial_ratio >= 65). Using that
as ground truth here would be circular: a fuzzy content matcher would be
deciding which page a passage-matching engine is supposed to find. The
codebase itself avoids label/positional folio mapping for "structurally
unalignable" manuscripts, and only 18,362 of 45,034 rows carry a folio label
at all -- so a trustworthy page-grain identifier mapping does not exist for
most of the corpus.

What DOES exist is an identifier join with no content involved:
shelfmark -> sys_id, resolved at FGP indexing time (documented in fgp_meta).
So a query's positives are ALL indexed pages of its sys_id. Coarser than the
plan intended, and the report must say so: it measures "does the method find
the right manuscript", not "the right folio". It is non-circular, which the
page-grain version could not be.

Exclusions, each counted:
  * doc_relation='Digital Translation' (3,342 rows) -- those are translations,
    not source-language transcriptions. Including them would have measured
    cross-language retrieval while claiming to measure noise robustness.
  * rows with no sys_id, or whose sys_id has no indexed page
  * content shorter than --min-chars after normalization

Usage:
  python scripts/build_fgp_query_set.py --index PATH --out queries.jsonl
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import sqlite3
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import norm_stream_fast  # noqa: E402

FGP_DB = os.environ.get('GENIZAH_FGP_DB',
                        r'C:\GenizahSearch\fgp_data\fgp_transcriptions.db')


def length_band(n: int) -> str:
    for edge in (100, 200, 400, 800, 1600):
        if n < edge:
            return f'<{edge}'
    return '>=1600'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--index', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--fgp-db', default=FGP_DB)
    ap.add_argument('--min-chars', type=int, default=120,
                    help='minimum NORMALIZED letters for a usable query')
    ap.add_argument('--max-per-sys', type=int, default=1,
                    help='cap queries per sys_id so big manuscripts do not '
                         'dominate the population')
    args = ap.parse_args()

    idx = open_index(args.index)
    if idx is None:
        print(f'index will not open: {args.index}', file=sys.stderr)
        return 2

    # sys_id -> indexed record ids, straight off the artifact.
    by_sys = collections.defaultdict(list)
    for ri in range(idx.n_records):
        rid = idx.record_id(ri)
        by_sys[rid.split('_', 1)[0]].append(rid)
    print(f'index: {idx.n_records:,} records across {len(by_sys):,} sys_ids')

    con = sqlite3.connect(f'file:{args.fgp_db}?mode=ro', uri=True)
    rows = con.execute(
        "SELECT id, sys_id, image_side, content FROM fgp_transcriptions "
        "WHERE doc_relation='Digital Edition' "
        "AND sys_id IS NOT NULL AND sys_id <> '' "
        "ORDER BY id").fetchall()
    con.close()
    print(f'fgp: {len(rows):,} Digital Edition rows with a sys_id')

    drops = collections.Counter()
    per_sys = collections.Counter()
    out = []
    for row_id, sys_id, image_side, content in rows:
        sys_id = str(sys_id)
        positives = by_sys.get(sys_id)
        if not positives:
            drops['sys_id_not_indexed'] += 1
            continue
        stream = norm_stream_fast(content or '')
        if len(stream) < args.min_chars:
            drops['too_short'] += 1
            continue
        if per_sys[sys_id] >= args.max_per_sys:
            drops['per_sys_cap'] += 1
            continue
        per_sys[sys_id] += 1
        out.append({
            'query_id': f'fgp:{row_id}',
            'text': content,
            'positives': positives,
            'strata': {
                'length_band': length_band(len(stream)),
                'pages_in_sys': ('1' if len(positives) == 1 else
                                 '2-5' if len(positives) <= 5 else
                                 '6-20' if len(positives) <= 20 else '>20'),
                'has_folio_label': bool(image_side),
            },
            'meta': {'sys_id': sys_id, 'norm_letters': len(stream),
                     'n_positives': len(positives)},
        })

    with open(args.out, 'w', encoding='utf-8') as fh:
        for q in out:
            fh.write(json.dumps(q, ensure_ascii=False) + '\n')

    print(f'\nqueries written: {len(out):,} -> {args.out}')
    print('dropped:', dict(drops))
    band = collections.Counter(q['strata']['length_band'] for q in out)
    pages = collections.Counter(q['strata']['pages_in_sys'] for q in out)
    lab = collections.Counter(q['strata']['has_folio_label'] for q in out)
    print('by length band :', dict(sorted(band.items())))
    print('by pages in sys:', dict(sorted(pages.items())))
    print('folio label    :', dict(lab))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

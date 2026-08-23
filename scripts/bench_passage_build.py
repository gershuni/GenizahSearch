# -*- coding: utf-8 -*-
"""Measure a passage-index build. Feeds the Phase 142 acceptance table.

The plan's size and build-cost numbers are projections from a measured letter
count, and the two constructions were deliberately NOT ranked in advance. This
produces the evidence to rank them, plus the per-letter rates needed to project
the full corpus from a slice rather than guess it.

Usage:
  python scripts/bench_passage_build.py --records 50000
  python scripts/bench_passage_build.py --records 50000 --compare
  python scripts/bench_passage_build.py --records 200000 --partitions 16

Dev-box / owner-machine only. Never run it on the web server.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import shutil
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_builder import build_index  # noqa: E402
from shared.passage_corpus import iter_records  # noqa: E402
from shared.passage_index import open_index  # noqa: E402

DEFAULT_CORPUS = os.environ.get(
    'GENIZAH_TRANSCRIPTIONS', r'C:\GenizahSearch\Transcriptions.txt')
FULL_CORPUS_LETTERS = 602_598_330   # measured 2026-08-20
FULL_CORPUS_RECORDS = 948_549

ARTIFACTS = ('postings.bin', 'gram_offsets.bin', 'streams.bin',
             'records.bin', 'record_ids.bin', 'excluded_records.tsv')


def peak_rss_bytes() -> int:
    try:
        import psutil
        mi = psutil.Process().memory_info()
        return int(getattr(mi, 'peak_wset', mi.rss))
    except Exception:
        return 0


def digest(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for b in iter(lambda: fh.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def sizes(index_dir: str) -> dict:
    out = {}
    for name in ARTIFACTS:
        p = os.path.join(index_dir, name)
        out[name] = os.path.getsize(p) if os.path.exists(p) else 0
    out['TOTAL'] = sum(out.values())
    return out


def human(n: float) -> str:
    for unit in ('B', 'KB', 'MB', 'GB'):
        if abs(n) < 1024 or unit == 'GB':
            return f'{n:,.1f} {unit}'
        n /= 1024
    return f'{n:.1f} GB'


def _progress(phase, a, b, elapsed):
    print(f'    [{phase}] {a:,}/{b:,}  {elapsed:.0f}s', flush=True)


def run_one(records_factory, out_dir, args, construction) -> dict:
    """`records_factory` returns a FRESH iterator each call.

    The bench must not hold the corpus in a list: 948K records is ~1.5 GB of
    Python strings, which would both fail to scale and pollute the peak-RSS
    measurement the builder is being judged on.
    """
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    t0 = time.time()
    # Fingerprint the corpus into the manifest (PR #324 round 3). This bench
    # is also the shipped way to BUILD a real artifact (--keep on the full
    # corpus is exactly how the production index was made), and without
    # `source_manifest` it wrote `corpus.sources: []` -- an artifact that
    # cannot be tied to the bytes it indexed, so a stale index could be
    # paired with newer Tantivy display text and project spans onto changed
    # content. One extra sha256 pass over the corpus file, paid once per
    # build.
    from shared.passage_corpus import source_manifest
    stats = build_index(
        records_factory(), out_dir, construction=construction,
        partitions=args.partitions, stride=args.stride,
        df_cap=args.df_cap, batch_grams=args.batch_grams,
        apply_hygiene=not args.no_hygiene,
        source_manifest=source_manifest([args.corpus]),
        corpus_label=f'bench:{args.records}',
        progress=(lambda *a: None) if args.quiet else _progress)
    wall = time.time() - t0
    idx = open_index(out_dir)
    row = stats.as_dict()
    row.update({
        'batch_grams': args.batch_grams,
        'wall_seconds': round(wall, 2),
        'peak_rss_bytes': peak_rss_bytes(),
        'opens': idx is not None,
        'sizes': sizes(out_dir),
        'postings_digest': digest(os.path.join(out_dir, 'postings.bin')),
        'offsets_digest': digest(os.path.join(out_dir, 'gram_offsets.bin')),
    })
    return row


# gram_offsets.bin is (GRAM_CODE_SPACE + 1) * 8 bytes REGARDLESS of corpus
# size -- the code space is fixed at 27**5. Scaling it with the corpus is what
# turned a ~3.5 GB artifact into a reported 21.8 GB on the first run.
FIXED_SIZE_FILES = ('gram_offsets.bin',)


def project(row: dict) -> dict:
    """Extrapolate to the full corpus from measured per-letter rates.

    Only the size-proportional files are scaled. Timings are scaled whole:
    pass 1 is linear in letters, and pass 2 is linear in postings, which is
    linear in letters.
    """
    letters = row['n_letters'] or 1
    scale = FULL_CORPUS_LETTERS / letters
    scaled = 0
    fixed = 0
    for name, n in row['sizes'].items():
        if name == 'TOTAL':
            continue
        if name in FIXED_SIZE_FILES:
            fixed += n
        else:
            scaled += n * scale
    return {
        'scale_factor': round(scale, 2),
        'projected_postings': int(row['n_postings'] * scale),
        'projected_artifact_bytes': int(scaled + fixed),
        'projected_fixed_bytes': int(fixed),
        'projected_pass1_seconds': round(row['seconds_pass1'] * scale),
        'projected_pass2_seconds': round(row['seconds_pass2'] * scale),
        'projected_total_seconds': round(row['wall_seconds'] * scale),
    }


def _report(construction, r) -> None:
    print(f'  records {r["n_records_indexed"]:,} indexed / '
          f'{r["n_records_seen"]:,} seen   excluded {r["excluded"]}')
    print(f'  letters {r["n_letters"]:,}   postings {r["n_postings"]:,}   '
          f'distinct codes {r["distinct_codes"]:,}')
    print(f'  pass1 {r["seconds_pass1"]}s   pass2 {r["seconds_pass2"]}s   '
          f'wall {r["wall_seconds"]}s   opens={r["opens"]}')
    print(f'  batch_grams {r.get("batch_grams", 0):,}')
    print(f'  peak RSS {human(r["peak_rss_bytes"])}   '
          f'peak slice {human(r["peak_slice_bytes"])}   '
          f'scratch {human(r["scratch_bytes"])}')
    print(f'  artifact {human(r["sizes"]["TOTAL"])}  '
          f'(postings {human(r["sizes"]["postings.bin"])}, '
          f'offsets {human(r["sizes"]["gram_offsets.bin"])} FIXED, '
          f'streams {human(r["sizes"]["streams.bin"])})')
    if r['df_capped_codes']:
        print(f'  df cap removed {r["df_capped_codes"]:,} codes / '
              f'{r["df_capped_postings"]:,} postings')
    p = project(r)
    print(f'  --> full corpus x{p["scale_factor"]}: '
          f'{p["projected_postings"]:,} postings, '
          f'{human(p["projected_artifact_bytes"])}, '
          f'{p["projected_total_seconds"] / 60:.0f} min '
          f'(pass1 {p["projected_pass1_seconds"] / 60:.0f} + '
          f'pass2 {p["projected_pass2_seconds"] / 60:.0f})')


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--records', type=int, default=50_000)
    ap.add_argument('--corpus', default=DEFAULT_CORPUS)
    ap.add_argument('--out', default=None)
    ap.add_argument('--construction', default='scatter',
                    choices=['scatter', 'spool'])
    ap.add_argument('--compare', action='store_true',
                    help='build BOTH constructions and diff the artifacts')
    ap.add_argument('--partitions', type=int, default=8)
    ap.add_argument('--stride', type=int, default=1)
    ap.add_argument('--batch-grams', type=int, default=4_000_000,
                    help='builder RAM knob: ~24 bytes per gram, plus copies')
    ap.add_argument('--df-cap', type=int, default=None)
    ap.add_argument('--no-hygiene', action='store_true')
    ap.add_argument('--quiet', action='store_true')
    ap.add_argument('--json', default=None, help='append a JSON line here')
    ap.add_argument('--keep', action='store_true',
                    help='leave the built index in place')
    args = ap.parse_args()

    if not os.path.exists(args.corpus):
        print(f'corpus not found: {args.corpus}', file=sys.stderr)
        return 2
    base = args.out or os.path.join(
        os.environ.get('TEMP', '.'), 'passage_bench')

    def records_factory():
        it = iter_records(args.corpus)
        return it if args.records <= 0 else itertools.islice(it, args.records)

    label = 'ALL' if args.records <= 0 else f'{args.records:,}'
    print(f'streaming {label} records from {args.corpus}', flush=True)

    plan = ['scatter', 'spool'] if args.compare else [args.construction]
    rows = {}
    rc = 0
    for construction in plan:
        print(f'\n=== {construction} (partitions={args.partitions}, '
              f'stride={args.stride}, df_cap={args.df_cap}) ===', flush=True)
        rows[construction] = run_one(
            records_factory, os.path.join(base, construction), args,
            construction)
        _report(construction, rows[construction])

    if args.compare:
        a, b = rows['scatter'], rows['spool']
        same = (a['postings_digest'] == b['postings_digest']
                and a['offsets_digest'] == b['offsets_digest'])
        print(f'\nartifacts byte-identical: {same}')
        if not same:
            print('  MISMATCH -- the constructions disagree', file=sys.stderr)
            rc = 1
        faster = 'scatter' if a['wall_seconds'] <= b['wall_seconds'] else 'spool'
        print(f'faster: {faster}  (scatter {a["wall_seconds"]}s vs '
              f'spool {b["wall_seconds"]}s)')
        print(f'scratch: scatter {human(a["scratch_bytes"])} vs '
              f'spool {human(b["scratch_bytes"])}')
        print(f'peak RSS: scatter {human(a["peak_rss_bytes"])} vs '
              f'spool {human(b["peak_rss_bytes"])}')

    if args.json:
        with open(args.json, 'a', encoding='utf-8') as fh:
            for construction, row in rows.items():
                fh.write(json.dumps({
                    'args': vars(args), 'construction': construction,
                    'row': row, 'projection': project(row)}) + '\n')
        print(f'\nappended to {args.json}')
    if not args.keep:
        shutil.rmtree(base, ignore_errors=True)
    return rc


if __name__ == '__main__':
    raise SystemExit(main())

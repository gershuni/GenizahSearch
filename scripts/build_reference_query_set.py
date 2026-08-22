# -*- coding: utf-8 -*-
"""Query set drawn from COMPLETE reference works -- the real search case.

Why this exists (owner ruling, 2026-08-22). Every graded deck so far used FGP
transcriptions as queries, and those are not what a researcher pastes. They
are often partial, and they carry transcriber summaries and skips. Measured
against each transcription's own HTR page on 4,000 rows: only 57.6% are
comparable in length (ratio 0.8-1.25); 8.6% are materially SHORTER (2.1% under
half the page) and 33.7% are LONGER than the page, which is what editorial
notes and apparatus look like. About 42% deviate materially in one direction
or the other.

The real case is a continuous passage of a composition. This builder draws
exactly that: contiguous readable slices from complete works in the reference
corpora, with no witness-oracle restriction (unlike
`scripts/build_witness_query_set.py`, which needs attested manuscripts because
it measures recall). A precision deck needs no oracle -- it shows query text
beside a returned manuscript and asks the grader what the relation is -- so
the draw can span the whole corpus rather than the attested corner of it.

Two corpora, both masked at the identity level and taken as directories on the
command line, never named in this file:

  --m-dir    plain-text works, filename ...--<work-id>.txt-OnlyText.txt, with
             `##...##` metadata blocks that MUST be stripped before slicing:
             they carry the source-manuscript statement, so leaving them in
             pastes the answer into the query.
  --b-dir    a staged corpus with a manifest.json listing {key, body_file};
             bodies are plain text with no metadata blocks.

`positives` is emitted EMPTY and that is deliberate: this set is for grading
relations, not for measuring recall, and an empty positive set makes misuse
as a recall instrument fail loudly rather than silently score zero.

Usage:
  python scripts/build_reference_query_set.py --m-dir <dir> --b-dir <dir> \
      --out ref_queries.jsonl [--per-work 1] [--target-chars 900]
"""
from __future__ import annotations

import argparse
import collections
import hashlib
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_normalize import norm_stream  # noqa: E402

HEADER_RE = re.compile(r'##(?:[^#\n]|#(?!#))*##')
MIN_LETTERS = 400
LEAD_SKIP = 0.05
TAIL_SKIP = 0.10


def read_long_path(path: str) -> str:
    with open('\\\\?\\' + os.path.abspath(path), encoding='utf-8',
              errors='replace') as fh:
        return fh.read()


def slice_of(body: str, key: str, k: int, n: int, target: int) -> str:
    """One deterministic contiguous window, word-aligned at both edges."""
    lo = int(len(body) * LEAD_SKIP)
    hi = int(len(body) * (1.0 - TAIL_SKIP)) - target
    if hi <= lo:
        lo, hi = 0, max(0, len(body) - target)
    span = max(1, hi - lo)
    h = hashlib.blake2b(f'{key}#{k}'.encode(), digest_size=8).digest()
    band = max(1, span // n)
    start = lo + span * k // n + int.from_bytes(h, 'big') % band
    text = body[start:start + target]
    if ' ' in text[:40]:
        text = text[text.index(' ') + 1:]
    if ' ' in text[-40:]:
        text = text[:text.rindex(' ')]
    return text.strip()


def iter_m_works(d: str):
    for fn in sorted(os.listdir(d)):
        if not fn.endswith('.txt'):
            continue
        base = fn.replace('.txt-OnlyText.txt', '')
        parts = base.split('--')
        yield ('M:' + (parts[-1] if parts else fn),
               HEADER_RE.sub(' ', read_long_path(os.path.join(d, fn))))


def iter_b_works(d: str):
    mpath = os.path.join(d, 'manifest.json')
    if not os.path.exists(mpath):
        raise SystemExit(f'no manifest.json in {d}')
    with open(mpath, encoding='utf-8') as fh:
        entries = json.load(fh)['entries']
    for e in entries:
        p = os.path.join(d, e['body_file'])
        if os.path.exists(p):
            yield ('B:' + e['key'], read_long_path(p))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--m-dir')
    ap.add_argument('--b-dir')
    ap.add_argument('--out', required=True)
    ap.add_argument('--per-work', type=int, default=1)
    ap.add_argument('--target-chars', type=int, default=900)
    ap.add_argument('--max-works-per-corpus', type=int, default=0,
                    help='0 = all; otherwise an evenly spaced subsample')
    args = ap.parse_args()
    if not (args.m_dir or args.b_dir):
        raise SystemExit('need --m-dir and/or --b-dir')

    drop = collections.Counter()
    written = 0
    seen_text: set = set()
    with open(args.out, 'w', encoding='utf-8') as out:
        for corpus, it in (('M', iter_m_works(args.m_dir) if args.m_dir else ()),
                           ('B', iter_b_works(args.b_dir) if args.b_dir else ())):
            works = list(it)
            if args.max_works_per_corpus and \
                    len(works) > args.max_works_per_corpus:
                step = len(works) / args.max_works_per_corpus
                works = [works[int(i * step)]
                         for i in range(args.max_works_per_corpus)]
            print(f'corpus {corpus}: {len(works)} works', flush=True)
            for key, body in works:
                if len(body) < args.target_chars:
                    drop[f'{corpus}:body_too_short'] += 1
                    continue
                for k in range(args.per_work):
                    text = slice_of(body, key, k, args.per_work,
                                    args.target_chars)
                    if text in seen_text:
                        drop[f'{corpus}:duplicate_slice'] += 1
                        continue
                    stream, _ = norm_stream(text)
                    if len(stream) < MIN_LETTERS:
                        drop[f'{corpus}:below_min_letters'] += 1
                        continue
                    seen_text.add(text)
                    out.write(json.dumps({
                        'query_id': f'ref:{key}#{k}',
                        'text': text,
                        'positives': [],
                        'strata': {'corpus': corpus,
                                   'length_band': '<800' if len(stream) < 800
                                                  else '>=800'},
                        'meta': {'work_key': key, 'norm_letters': len(stream)},
                    }, ensure_ascii=False) + '\n')
                    written += 1
    print(f'wrote {written:,} queries to {args.out}')
    if drop:
        print('  dropped: ' + '  '.join(f'{k}={v}'
                                        for k, v in sorted(drop.items())))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

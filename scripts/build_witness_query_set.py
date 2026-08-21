# -*- coding: utf-8 -*-
"""Instrument 2: queries whose positives come from a HUMAN witness oracle.

Why this exists. The FGP instrument (scripts/build_fgp_query_set.py) is a
same-folio recognition task: the query is a human transcription OF the page
it must retrieve. That measures the mechanism but not the workload -- a
scholar pastes a passage of a KNOWN WORK and asks which Genizah fragments
carry it. This instrument is that task, and its ground truth is independent
of both search methods: a scholarly witness list mapping work -> manuscript.

The oracle is catalogue-shaped, so it is a RECALL yardstick only, never
acceptance evidence. Precision comes from the graded deck.

Two deflations, both stated because both are large and neither is a defect:

 1. A witness is a FRAGMENT. It attests that the manuscript carries the
    work, not that it carries the passage we pasted. A randomly chosen
    passage of a long work is usually absent from any one fragment, so the
    absolute hit rate is far below the true recall of either method.
 2. The oracle is incomplete. A retrieved page that is not on the list is
    not a false positive -- it may be an unattested witness. Hence recall
    only, and hence "any-positive@k": did the method surface AT LEAST ONE
    manuscript that scholars had already attested for this work.

Both deflations are IDENTICAL for both methods -- same queries, same positive
sets -- so the between-method difference is unaffected. Only the absolute
level is uninterpretable, and it is reported as such.

Sampling is weighted toward works with many attested witnesses, because
deflation (1) falls as the number of independent fragments rises. Witness
count is a stratum so the effect is visible rather than assumed.

The ## metadata blocks are stripped BEFORE the readable slice is taken. They
carry the source-manuscript provenance line, so leaving them in would paste
the answer into the query. This is a correctness requirement, not tidiness.

Inputs are all gitignored: the witness oracle, the reference-work text
directory, and the built passage index. Output is written next to the index
and must stay gitignored -- it embeds reference-corpus text.

Usage:
  python scripts/build_witness_query_set.py \
      --witnesses <oracle.json> --ref-dir <dir> --index <index_dir> \
      --out <queries.jsonl>
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

from shared.passage_index import open_index          # noqa: E402
from shared.passage_normalize import norm_stream     # noqa: E402

# Header body may contain a lone '#' but never '##' and never a newline.
# Ported from the producer's reference builder; the pattern is load-bearing
# (a greedy variant once deleted 30,677 letters of a work's body).
HEADER_RE = re.compile(r'##(?:[^#\n]|#(?!#))*##')

MIN_QUERY_LETTERS = 400     # comparable to the FGP set's median of 601
TARGET_CHARS = 900          # readable chars, before normalization
LEAD_SKIP = 0.05            # skip a work's opening (title/preamble matter)
TAIL_SKIP = 0.10


def witness_map(path: str, min_conf: str = 'high') -> dict:
    """work_id -> {sys_id}. Only the stated confidence tier."""
    with open(path, encoding='utf-8') as fh:
        rows = json.load(fh)
    out = collections.defaultdict(set)
    kept = dropped = 0
    for r in rows:
        if r.get('confidence') != min_conf:
            dropped += 1
            continue
        out[r['work_id']].add(r['sys_id'])
        kept += 1
    print(f'oracle: {len(rows):,} rows -> {kept:,} at confidence={min_conf} '
          f'({dropped:,} dropped), {len(out):,} works', flush=True)
    return dict(out)


def sys_to_records(idx) -> dict:
    """sys_id -> [record_id], over the records the passage index holds.

    This is the eligible set by construction, which is what makes the
    positive sets equal for both methods (see shared/retrieval_adapters.py).
    """
    out = collections.defaultdict(list)
    for i in range(idx.n_records):
        rid = idx.record_id(i)
        out[rid.split('_', 1)[0]].append(rid)
    return dict(out)


def ref_files(ref_dir: str) -> dict:
    """work_id -> path, replicating the producer's id derivation exactly."""
    out = {}
    for fn in os.listdir(ref_dir):
        if not fn.endswith('.txt'):
            continue
        base = fn.replace('.txt-OnlyText.txt', '')
        parts = base.split('--')
        out['M:' + (parts[-1] if parts else fn)] = os.path.join(ref_dir, fn)
    return out


def read_body(path: str) -> str:
    # The \\?\ prefix: some of these filenames exceed Windows MAX_PATH.
    with open('\\\\?\\' + os.path.abspath(path), encoding='utf-8',
              errors='replace') as fh:
        raw = fh.read()
    return HEADER_RE.sub(' ', raw)


def slices(body: str, work_id: str, n: int, target: int) -> list:
    """n deterministic readable windows, spread across the work's body.

    Deterministic in (work_id, k) so a rebuild reproduces the same set and
    the tune/holdout split stays stable.
    """
    lo = int(len(body) * LEAD_SKIP)
    hi = int(len(body) * (1.0 - TAIL_SKIP)) - target
    if hi <= lo:
        lo, hi = 0, max(0, len(body) - target)
    out = []
    span = max(1, hi - lo)
    for k in range(n):
        h = hashlib.blake2b(f'{work_id}#{k}'.encode(), digest_size=8).digest()
        # spread the k windows over k/n-th sub-bands so two slices of one
        # work cannot collide on the same passage
        band_lo = lo + span * k // n
        band = max(1, span // n)
        start = band_lo + int.from_bytes(h, 'big') % band
        text = body[start:start + target]
        # do not cut a word in half at either edge
        if ' ' in text[:40]:
            text = text[text.index(' ') + 1:]
        if ' ' in text[-40:]:
            text = text[:text.rindex(' ')]
        out.append(text.strip())
    return out


def length_band(n: int) -> str:
    for edge in (400, 800, 1600):
        if n < edge:
            return f'<{edge}'
    return '>=1600'


def witness_band(n: int) -> str:
    if n == 1:
        return '1'
    if n <= 3:
        return '2-3'
    if n <= 10:
        return '4-10'
    return '11+'


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--witnesses', required=True)
    ap.add_argument('--ref-dir', required=True)
    ap.add_argument('--index', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--confidence', default='high')
    ap.add_argument('--min-witnesses', type=int, default=4,
                    help='deflation (1) falls with independent fragments; '
                         'below this a random passage is almost never present')
    ap.add_argument('--per-work', type=int, default=4)
    ap.add_argument('--target-chars', type=int, default=TARGET_CHARS)
    args = ap.parse_args()

    wmap = witness_map(args.witnesses, args.confidence)
    idx = open_index(args.index)
    if idx is None:
        print(f'index will not open: {args.index}', file=sys.stderr)
        return 2
    s2r = sys_to_records(idx)
    print(f'index: {idx.n_records:,} records, {len(s2r):,} sys_ids',
          flush=True)
    files = ref_files(args.ref_dir)
    print(f'reference files: {len(files):,}', flush=True)

    # Every exclusion is counted. A bare `continue` once lost 30% of an
    # append at exit 0 in this project.
    drop = collections.Counter()
    chosen = []
    for work_id, sys_ids in sorted(wmap.items()):
        present = {s for s in sys_ids if s in s2r}
        if not present:
            drop['no_eligible_record'] += 1
            continue
        if len(present) < args.min_witnesses:
            drop['too_few_witnesses'] += 1
            continue
        path = files.get(work_id)
        if path is None:
            drop['no_reference_file'] += 1
            continue
        chosen.append((work_id, present, path))
    print(f'works: {len(wmap):,} -> {len(chosen):,} usable   '
          + '  '.join(f'{k}={v:,}' for k, v in sorted(drop.items())),
          flush=True)
    if not chosen:
        print('no usable works', file=sys.stderr)
        return 1

    qdrop = collections.Counter()
    n_written = 0
    with open(args.out, 'w', encoding='utf-8') as out:
        for work_id, present, path in chosen:
            body = read_body(path)
            positives = sorted(r for s in sorted(present) for r in s2r[s])
            seen_texts = set()
            for k, text in enumerate(slices(body, work_id, args.per_work,
                                            args.target_chars)):
                # The short-body fallback in slices() can land 2-4 of a work's
                # windows on the same start position, yielding byte-identical
                # "independent" queries (measured: 94 of 2,258, 4.2%, in v1 of
                # this file). Identical siblings inflate n without adding
                # information; drop them, counted.
                if text in seen_texts:
                    qdrop['duplicate_slice'] += 1
                    continue
                seen_texts.add(text)
                stream, _ = norm_stream(text)
                if len(stream) < MIN_QUERY_LETTERS:
                    qdrop['below_min_letters'] += 1
                    continue
                rec = {
                    'query_id': f'wit:{work_id}#{k}',
                    'text': text,
                    'positives': positives,
                    'strata': {
                        'length_band': length_band(len(stream)),
                        'witness_band': witness_band(len(present)),
                    },
                    'meta': {
                        'work_id': work_id,
                        'norm_letters': len(stream),
                        'n_sys': len(present),
                        'n_positives': len(positives),
                    },
                }
                out.write(json.dumps(rec, ensure_ascii=False) + '\n')
                n_written += 1

    tail = ''
    if qdrop:
        tail = ('   dropped '
                + '  '.join(f'{k}={v:,}' for k, v in sorted(qdrop.items())))
    print(f'wrote {n_written:,} queries to {args.out}{tail}')
    expected = len(chosen) * args.per_work
    accounted = n_written + sum(qdrop.values())
    print(f'expected {expected:,}, wrote {n_written:,}, '
          f'accounted {accounted:,}')
    if accounted != expected:
        print('COUNT DIVERGENCE -- queries were lost silently',
              file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

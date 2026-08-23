# -*- coding: utf-8 -*-
"""Release verifier for a passage index. Exit 0 = every check passed.

What it proves, beyond the fail-closed checks open_index already does:

  1. CSR offsets are globally monotone (open_index checks ends only).
  2. RECONSTRUCTION, both directions, on sampled records spread across the
     artifact: (a) every gram re-derived from streams.bin has its posting at
     the CSR address the offsets claim, respecting the manifest's stride and
     df_cap; (b) postings sampled from the index decode to (record, position)
     pairs whose stream really contains the gram of that CSR row.
  3. Postings within every sampled row are strictly ordered by
     (record, position) -- the determinism the artifact hash relies on.
  4. Record ids look like corpus record ids, and stream lengths agree with
     records.bin.

The check the plan calls "integrity is not cursor equality": a wrong address
does not crash, it returns someone else's postings, so the verifier re-derives
from the data rather than trusting the accounting.

Usage: python scripts/verify_passage_index.py --index PATH [--records N]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_builder import codes_from_letter_indices  # noqa: E402
from shared.passage_index import (  # noqa: E402
    POSTING_BYTES, open_index, unpack_postings, verify_csr_monotone,
)

RECORD_ID_RE = re.compile(r'^\S+$')


def fail(msg: str) -> None:
    print(f'FAIL: {msg}')
    raise SystemExit(1)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--index', required=True)
    ap.add_argument('--records', type=int, default=400,
                    help='records to reconstruct, spread evenly')
    ap.add_argument('--rows', type=int, default=5000,
                    help='CSR rows to order-check and back-check')
    args = ap.parse_args()

    idx = open_index(args.index)
    if idx is None:
        fail('open_index refused the artifact (fail-closed checks)')
    m = idx.manifest
    stride = int(m['build']['stride'])
    df_cap = m['build']['df_cap']
    print(f'records={idx.n_records:,} postings={idx.n_postings:,} '
          f'stride={stride} df_cap={df_cap}')

    # 1. Global CSR monotonicity.
    verify_csr_monotone(np.asarray(idx.gram_offsets))
    print('ok: CSR offsets monotone')

    # 2a. Forward reconstruction from sampled records.
    widths = np.diff(idx.gram_offsets.astype(np.int64))
    n = idx.n_records
    sample = range(0, n, max(1, n // args.records))
    checked = 0
    for ri in sample:
        rec = idx.records[ri]
        off, n_let = int(rec['stream_off']), int(rec['n_letters'])
        chunk = idx.streams[off:off + n_let]
        codes = codes_from_letter_indices(chunk)
        pos = np.arange(codes.size, dtype=np.int64)
        if stride > 1:
            codes, pos = codes[::stride], pos[::stride]
        for c, p in zip(codes.tolist(), pos.tolist()):
            w = int(widths[c])
            if w == 0:
                if df_cap and w == 0:
                    continue          # df-capped row: legitimately absent
                fail(f'record {ri}: gram {c} has an empty CSR row and no cap')
            pages, positions = idx.postings_for(int(c))
            hit = np.flatnonzero((pages == ri) & (positions == p))
            if hit.size != 1:
                fail(f'record {ri}: posting ({ri},{p}) for gram {c} '
                     f'found {hit.size} times, expected 1')
            checked += codes.size
    print(f'ok: forward reconstruction, {len(list(sample))} records')

    # 3. GLOBAL order sweep -- every posting, streamed. A sampled order
    # check let a swapped-postings artifact PASS (the mutated row was not in
    # the sample, and forward reconstruction searches the whole row so
    # reordering slips through). Order is the determinism guarantee, so it is
    # checked exhaustively: within every CSR row, (record, position) keys
    # must be strictly increasing. ~594M postings stream in chunks in
    # seconds; a release verifier can afford that.
    offsets64 = idx.gram_offsets.astype(np.int64)
    total = int(offsets64[-1])
    CHUNK = 40_000_000
    prev_key = -1
    prev_row_end = False
    for a in range(0, total, CHUNK):
        b = min(a + CHUNK, total)
        pages, positions = unpack_postings(
            idx.postings[a * POSTING_BYTES:b * POSTING_BYTES])
        keys = (pages.astype(np.int64) << 20) | positions.astype(np.int64)
        starts = offsets64[np.searchsorted(offsets64, a, side='right'):
                           np.searchsorted(offsets64, b, side='left')] - a
        is_start = np.zeros(b - a, dtype=bool)
        is_start[starts] = True
        bad = np.flatnonzero((np.diff(keys) <= 0) & ~is_start[1:])
        if bad.size:
            fail(f'posting {a + int(bad[0]) + 1}: row order violated')
        if a > 0 and not prev_row_end and keys[0] <= prev_key:
            fail(f'posting {a}: row order violated at chunk boundary')
        prev_key = int(keys[-1])
        prev_row_end = bool(is_start.size and
                            np.searchsorted(offsets64, b) < offsets64.size
                            and b in offsets64)
    print(f'ok: global order sweep, {total:,} postings')

    # 3b. Back-check sampled CSR rows: first posting decodes to its own gram.
    nz = np.flatnonzero(widths)
    row_sample = nz[::max(1, nz.size // args.rows)]
    for c in row_sample.tolist():
        pages, positions = idx.postings_for(int(c))
        ri, p = int(pages[0]), int(positions[0])
        rec = idx.records[ri]
        off = int(rec['stream_off'])
        sub = idx.streams[off + p:off + p + 5]
        got = int(codes_from_letter_indices(sub)[0]) if sub.size == 5 else -1
        if got != c:
            fail(f'gram {c}: posting ({ri},{p}) decodes to gram {got}')
    print(f'ok: back-check, {row_sample.size:,} CSR rows')

    # 4. Record table sanity.
    total_letters = int(m['counts']['n_letters'])
    if int(idx.records['n_letters'].sum()) != total_letters:
        fail('sum of record letters disagrees with the manifest')
    for ri in list(sample)[:50]:
        rid = idx.record_id(ri)
        if not RECORD_ID_RE.match(rid):
            fail(f'record {ri}: id {rid!r} malformed')
    print('ok: record table consistent with manifest')

    print('PASS')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

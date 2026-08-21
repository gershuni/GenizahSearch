"""Is the 25-40 letter band bimodal in rarity? Corpus-derived, not anecdotal.

ANSWER, measured: NO. See docs/specs/passage-matching-algorithm.md, item 11 of
"what is not established". Kept so the negative result is reproducible.

Replaces a table of 10 hand-picked formulas with a deterministic stratified
sample of real windows, scored on TRUE distinct-record document frequency --
which the index does NOT store (it stores posting counts) but which is
derivable in one vectorized pass, because postings within a gram are ordered
by (record, position).
"""
import itertools, os, sys, tempfile
import numpy as np
sys.path.insert(0, '.')
from shared.passage_builder import build_index, codes_from_letter_indices
from shared.passage_corpus import iter_records
from shared.passage_index import open_index, unpack_postings

N_RECORDS = int(os.environ.get('NREC', '60000'))
OUT = os.environ.get('IDXDIR') or os.path.join(tempfile.gettempdir(), 'span_rarity_idx')

if not os.path.exists(os.path.join(OUT, 'manifest.json')):
    print(f'building index over {N_RECORDS:,} records -> {OUT}', flush=True)
    build_index(itertools.islice(iter_records(r'C:\GenizahSearch\Transcriptions.txt'),
                                 N_RECORDS), OUT, partitions=4)
idx = open_index(OUT)
assert idx is not None
print(f'index: {idx.n_records:,} records, {idx.n_postings:,} postings\n', flush=True)

# ---- TRUE record-DF per gram, vectorized ----------------------------------
pages, _pos = unpack_postings(idx.postings)
off = idx.gram_offsets.astype(np.int64)
new_rec = np.empty(pages.size, dtype=bool)
new_rec[0] = True
np.not_equal(pages[1:], pages[:-1], out=new_rec[1:])
nz = np.flatnonzero(np.diff(off))            # codes that hold postings
new_rec[off[nz]] = True                      # force True at each gram start
rec_df = np.zeros(off.size - 1, dtype=np.int64)
rec_df[nz] = np.add.reduceat(new_rec, off[nz])
post_df = np.diff(off)
held = post_df > 0
print(f'distinct gram codes            : {held.sum():,}')
print(f'mean postings per held code    : {post_df[held].mean():.2f}')
print(f'mean RECORDS  per held code    : {rec_df[held].mean():.2f}')
print(f'codes in exactly 1 record      : {(rec_df[held] == 1).sum():,} '
      f'({100*(rec_df[held]==1).mean():.1f}%)')
print(f'max records for one code       : {rec_df.max():,} of {idx.n_records:,}\n', flush=True)

# ---- deterministic stratified sample of real windows ----------------------
rng_stride = 37          # deterministic: every 37th record, fixed offsets
LENGTHS = [25, 30, 35, 40, 60, 100]
print(f'{"len":>5} {"n":>7} | median-DF deciles across sampled windows')
print('-' * 78)
rows = {}
for L in LENGTHS:
    med = []
    for ri in range(0, idx.n_records, rng_stride):
        s = idx.stream(ri)
        if len(s) < L + 20:
            continue
        for frac in (0.15, 0.45, 0.75):
            start = int((len(s) - L) * frac)
            w = s[start:start + L]
            codes = codes_from_letter_indices(
                np.array([ord(c) - 0x05D0 for c in w], dtype=np.uint8))
            if not codes.size:
                continue
            d = rec_df[codes.astype(np.int64)]
            med.append(int(np.median(d)))
    med = np.array(med)
    rows[L] = med
    dec = np.percentile(med, [10, 25, 50, 75, 90, 99]).astype(int)
    print(f'{L:>5} {med.size:>7} | p10={dec[0]:<6} p25={dec[1]:<6} p50={dec[2]:<6} '
          f'p75={dec[3]:<6} p90={dec[4]:<7} p99={dec[5]}')

print()
print('fraction of sampled windows whose MEDIAN gram appears in >N records')
print(f'{"len":>5} | {">10":>8} {">50":>8} {">200":>8} {">1000":>8}')
print('-' * 46)
for L in LENGTHS:
    m = rows[L]
    print(f'{L:>5} | {100*(m>10).mean():7.1f}% {100*(m>50).mean():7.1f}% '
          f'{100*(m>200).mean():7.1f}% {100*(m>1000).mean():7.1f}%')

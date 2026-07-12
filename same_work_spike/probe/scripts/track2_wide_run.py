# -*- coding: utf-8 -*-
"""MAPV2-6 Track-2 WIDE tier: probability spectrum for small/fragmented pages.

Single full-corpus engine pass with INLINE decoy injection. The null (target-
decoy) and the wide tier come from the SAME index, SAME DF statistics, SAME
machinery -> exchangeable by construction (kills Codex design BLOCKER 1: no
separate 60k-page calibration whose DF band differs from production).

Two decoy cohorts are injected together in the one pass:
  D25 (CHUNK=25) + D12 (CHUNK=12), N pages each, from a leak-clean stratified
  pool. Per-bucket null = the CONSERVATIVE ENVELOPE (max over the two cohorts).

Design spec: _tmp/track2_wide_spec.md (revised after Codex design critique
results/overnight/codex_track2_wide_design_r1.log). Code review round 1
(results/overnight/codex_track2_wide_code_r1.log, 7 findings) applied in full;
see results/track2_wide_impl_notes.md.

Modes:
  --smoke   run end-to-end on data/mapv2_smoke.db (read-only) with 600+600
            decoys and relaxed guards; sidecar = data/track2_wide_smoke.db.
  (default) FULL run over data/fullcorpus_v2.db with 12,000+12,000 decoys;
            refuses unless data/mapv2_chain_state.json shows '6-track2' done
            AND track1_matches has shadowed_by AND accepted_pairs_canonmask /
            track1_candidates.p_same_work / pages.provenance exist.
            Sidecar = data/track2_wide.db.
  --resume  skip phases already marked done in the sidecar meta table
            (engine_done/spill_done/null_done/scored/enriched/cleanup);
            refuses if source path/mtime, decoy sizes, or engine params
            differ from what the meta table recorded (Codex code #6).

Usage: python -X utf8 -u track2_wide_run.py [--smoke] [--resume]

WIDE-ONLY PAIR SEMANTICS (Codex code BLOCKER 1)
-----------------------------------------------
A page PAIR with >= 1 strict-class segment is excluded from the sidecar
ENTIRELY: the verify loop keeps a strict_pairs set, pops any pair-best entry,
suppresses later wide segments of that pair, and purges already-spilled raw
rows for strict pairs before the pair-best dedup. strict_pairs_seen is
therefore PAIR-level (comparable to the chain's accepted_pairs_canonmask row
count). Decoy-real null counting stays SEGMENT-level (all wide-not-strict
segments counted, incl. weaker segments of pairs that also have a strict
segment): relative to the pair-best real side this only INFLATES the null
(segments-per-pair multiplicity >= 1) -> higher expected_null -> higher FDR
-> conservative. Documented decision, not an oversight.

NORMALIZATION DERIVATION (opportunity normalization; Codex design BLOCKER 2)
----------------------------------------------------------------------------
Decoys travel the SAME engine (same index, same DF_DROP gram filter, same
two-hit candidate generation, same MARGIN-padded verify), so the null is
exchangeable with the tested real-real pairs BY CONSTRUCTION. What differs is
the number of pairing OPPORTUNITIES each population has.

Let n_real = number of real pages, n_dec = number of decoy pages in a cohort.
Same-sys pairs are dropped by the engine; that removes an identical (tiny)
fraction from both populations -> ignore to first order.

Unordered pairing opportunities:
  N_rr  (real-real)  = C(n_real, 2)   = n_real*(n_real-1)/2
  N_dr  (decoy-real) = n_dec * n_real   (decoys are a disjoint set; every
                                         {decoy, real} combination is exactly
                                         one unordered pair, no symmetry factor)

Under the null, the per-opportunity probability r of a chance alignment landing
in a given (alen,dens,stratum) bucket is the same for a decoy-real pair and for
an UNRELATED real-real pair (both = two noisy streams, no shared long-range
structure). In expectation, per bucket:
  observed_decoy_real       = r * N_dr
  expected_null_real_real   = r * N_rr
Divide:
  expected_null_real_real = observed_decoy_real * N_rr / N_dr
        = observed_decoy_real * [n_real*(n_real-1)/2] / [n_dec*n_real]
        = observed_decoy_real * (n_real-1) / (2*n_dec)
       ~= observed_decoy_real * n_real / (2*n_dec)

  => FACTOR per cohort = n_real / (2 * n_dec).

The 1/2 is exactly the C(n_real,2) vs n_dec*n_real asymmetry: a real-real pair
draws its null opportunity from BOTH real sides (unordered -> halved), whereas a
decoy-real opportunity is already one distinct unordered pair (only the decoy
side is "spent").
  FULL  (n_real~=667,411, n_dec=12,000):  FACTOR ~= 27.8
  smoke (n_real~=139,694, n_dec=600):     FACTOR ~= 116.4

SPARSE-NULL HIERARCHICAL FALLBACK (Codex code BLOCKER 2)
--------------------------------------------------------
Per-bucket decoy draws are sparse even at 12,000/cohort. The bucket-level
local-FDR estimate is used AS-IS only when the envelope's supporting decoy
count >= K_SUPPORT=25 draws (rel. sampling error ~20%). Below that the bucket
gets max(bucket, same-stratum marginal, global) — each level computed with a
"+1" upper-bound on the decoy count (a crude one-sided Poisson/Garwood-style
bound: 0 observed draws are treated as <= 1, so NO bucket ever gets
local_fdr = 0 / p_local_bucket = 1.0 from absence of evidence). The monotone
majorant (non-decreasing in dens, non-increasing in alen) is applied AFTER
the fallback.

CONSERVATIVE ENVELOPE (Codex design #4): expected_null is computed per cohort
and the per-bucket MAX is taken. CHUNK=12 destroys more long-range structure
(more decoy pairs -> higher expected_null -> higher FDR), so the envelope is
conservative w.r.t. the unsettled chunk-size choice.

q_value (Codex code #3): per-row EMPIRICAL NULL-TAIL p-value from the decoy
envelope over the "as-or-more-significant" ordering (alen' >= alen AND
dens' <= dens, within the row's minlen stratum), +1-smoothed:
  p_tail = (1 + null_mass_as_or_more_significant) / (1 + null_mass_stratum),
then GLOBAL Benjamini-Hochberg over all real wide rows (computed exactly on
bucket groups — p_tail is bucket-constant, so ranks are cumulative row counts).
"""
import argparse
import bisect
import datetime
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time

import numpy as np
from rapidfuzz.distance import Levenshtein

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine_np                                            # noqa: E402
from normalize import norm_stream                           # noqa: E402
from stage0 import line_agreement, load_shelf_variants, same_shelf  # noqa: E402

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DATA = PROBE + r"\data"
RESULTS = PROBE + r"\results"
SPILL_DIR = DATA + r"\spill"
CHAIN_STATE = DATA + r"\mapv2_chain_state.json"
CHAIN_STEP = '6-track2'                                     # exact key in chain state

# ---- engine + verify params (IDENTICAL to rehearsal_run.py strict pass) ----
K, BAND, DF_DROP, MIN_ANCHORS = 5, 20, 100, 2
MARGIN, MIN_SPAN, SPAN_CAP = 30, 25, 6000
FLANK = 150
CANON_CATS = ('Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi')
MASKS_VERSION = 'maskcanon-canon5-v1'                       # cat IN CANON_CATS

# ---- wide-tier definition (Codex design #7 semantics: wide-ONLY pairs) ----
WIDE_MIN_ALEN = 35
WIDE_MAX_DENS = 0.45

# ---- decoy cohorts ----
COHORT_TAGS = ['D25', 'D12']
COHORT_CHUNK = {'D25': 25, 'D12': 12}
DECOY_N_FULL = 12000                     # raised 6000->12000 (Codex code #2)
DECOY_N_SMOKE = 600
SEED = 29
RESERVOIR_MAX = 2000

# ---- null buckets ----
ALEN_BINS = [35, 50, 70, 100, 150, 250, 400]   # right-open; last = 400+  (7 bins)
N_ALEN_BINS = len(ALEN_BINS)
N_DENS_BINS = 9                                # 0..0.45 in 0.05 steps
LEN_STRATA = [150, 300, 600]                   # <=150 / 150-300 / 300-600 / >600
N_STRATA = len(LEN_STRATA) + 1
K_SUPPORT = 25                    # min envelope decoy draws for bucket-level FDR

# ---- spill / batching (Codex design #6: spill, never truncate) ----
SPILL_FLUSH = 2_000_000                         # pair-best dict entries before flush
INSERT_BATCH = 200_000
DELETE_P_LOCAL = 0.05                            # prune if p_local_bucket < this ...
DELETE_Q_VALUE = 0.5                             # ... AND q_value > this
_PKEY_BITS = 20                                  # page idx < 2^20 (engine asserts)


# sloped strict acceptance boundary (rehearsal_run.accept_density)
def accept_density(length):
    return 0.30 if length < 100 else (0.386 if length < 200 else 0.418)


def flank_dist(sa, sb, a0, a1, b0, b1):
    """Best (lowest) EQUAL-LENGTH flank normalized distance; None if none.
    Equal-length clipping is load-bearing (rehearsal_run / fix_flanks post-mortem)."""
    best = None
    L = min(FLANK, a0, b0)
    if L >= 60:
        best = Levenshtein.normalized_distance(sa[a0 - L:a0], sb[b0 - L:b0])
    L = min(FLANK, len(sa) - a1, len(sb) - b1)
    if L >= 60:
        d = Levenshtein.normalized_distance(sa[a1:a1 + L], sb[b1:b1 + L])
        best = d if best is None else min(best, d)
    return best


def flank_class_of(fd):
    if fd is None:
        return 'edge'
    if fd <= 0.52:
        return 'continuation'
    return 'ambig' if fd <= 0.58 else 'island'


# ---- bucket index helpers (scalar for verify loop) ----
def alen_bin(a):
    return min(max(bisect.bisect_right(ALEN_BINS, a) - 1, 0), N_ALEN_BINS - 1)


def dens_bin(d):
    return min(int(d / 0.05), N_DENS_BINS - 1)


def stratum(length):
    return bisect.bisect_left(LEN_STRATA, length)


def make_decoy(stream, chunk, rng):
    """Chunk-shuffle (probe_conformal_fdr.make_decoy): preserves gram content /
    DF, destroys long-range diagonal structure."""
    chunks = [stream[i:i + chunk] for i in range(0, len(stream), chunk)]
    rng.shuffle(chunks)
    return ''.join(chunks)


def strip_masks(s, iv):
    """Concatenate the unmasked segments of s (mirrors what the engine indexes
    for a real page once its canonical spans are removed)."""
    if not iv:
        return s
    out = []
    cur = 0
    for m0, m1 in sorted(iv):
        m0 = max(0, min(int(m0), len(s)))
        m1 = max(0, min(int(m1), len(s)))
        if m0 > cur:
            out.append(s[cur:m0])
        if m1 > cur:
            cur = m1
    if cur < len(s):
        out.append(s[cur:])
    return ''.join(out)


def coverage(iv, n):
    """Total masked length (merged intervals clipped to [0,n))."""
    if not iv:
        return 0
    tot = 0
    cur = -1
    for a, b in sorted((max(0, min(int(x), n)), max(0, min(int(y), n))) for x, y in iv):
        if b <= a:
            continue
        if a > cur:
            tot += b - a
            cur = b
        elif b > cur:
            tot += b - cur
            cur = b
    return tot


def overlap_frac(iv, x0, x1):
    """Fraction of window [x0,x1) overlapping canonical spans iv (Codex design #5)."""
    if not iv or x1 <= x0:
        return 0.0
    tot = 0
    for m0, m1 in iv:
        lo = x0 if x0 > m0 else m0
        hi = x1 if x1 < m1 else m1
        if hi > lo:
            tot += hi - lo
    return tot / (x1 - x0)


def largest_remainder(prop, total):
    """Exact integer allocation of `total` proportional to `prop` (Codex #7)."""
    quota = np.asarray(prop, dtype=np.float64) * total
    base = np.floor(quota).astype(np.int64)
    rem = int(total - base.sum())
    if rem > 0:
        order = np.argsort(-(quota - base))
        base[order[:rem]] += 1
    return base


# ---- meta / phase helpers ----
def ensure_meta(con):
    con.execute("CREATE TABLE IF NOT EXISTS track2_wide_meta("
                "key TEXT PRIMARY KEY, value TEXT)")


def meta_set(con, k, v):
    con.execute("INSERT OR REPLACE INTO track2_wide_meta(key,value) VALUES(?,?)",
                (k, json.dumps(v) if not isinstance(v, str) else v))


def meta_get(con, k, default=None):
    try:
        r = con.execute("SELECT value FROM track2_wide_meta WHERE key=?",
                        (k,)).fetchone()
    except sqlite3.OperationalError:
        return default
    return r[0] if r else default


def phase_mark(con, name):
    meta_set(con, f'phase_{name}',
             datetime.datetime.now().isoformat(timespec='seconds'))
    print(f"[phase] {name} done @ {datetime.datetime.now():%H:%M:%S}", flush=True)


def has_phase(con, name):
    return meta_get(con, f'phase_{name}') is not None


def reset_out(con):
    for t in ('track2_wide_raw', 'track2_wide_base', 'track2_wide',
              'track2_wide_new', 'track2_strict_pairs', 'track2_obs_buckets',
              'track2_null_buckets', 'track2_bucket_scores',
              'track2_decoy_counts', 'track2_decoy_reservoir',
              'track2_wide_meta'):
        con.execute(f"DROP TABLE IF EXISTS {t}")


def git_describe():
    try:
        return subprocess.run(['git', '-C', ROOT, 'describe', '--always', '--dirty'],
                              capture_output=True, text=True, timeout=5).stdout.strip()
    except Exception:                                        # noqa: BLE001
        return '?'


def set_below_normal():
    """Drop this process to BelowNormal priority (mapv2_overnight pattern)."""
    try:
        import ctypes
        BELOW_NORMAL = 0x4000
        ctypes.windll.kernel32.SetPriorityClass(
            ctypes.windll.kernel32.GetCurrentProcess(), BELOW_NORMAL)
        print("[prio] process priority set BelowNormal", flush=True)
    except Exception as e:                                   # noqa: BLE001
        print(f"[prio] priority drop failed (continuing): {e!r}", flush=True)


# =====================================================================
# PHASE A: engine pass + inline decoys + verify + spill (+ base build)
# =====================================================================
def phase_engine_verify_spill(con, src, decoy_n, funnel, mode):
    t0 = time.time()

    # ---- load real pages ----
    rows = src.execute(
        "SELECT page_id, sys_id, text FROM pages ORDER BY rowid").fetchall()
    # MAPV2-10: NLI microfilm title-card / copyright-stamp pages are not
    # manuscript content and match EACH OTHER across HTR modes — drop them
    # from the engine input AND the decoy pool entirely
    mf_path = os.path.join(DATA, 'microfilm_title_pages.json')
    if os.path.exists(mf_path):
        mf = set(json.load(open(mf_path, encoding='utf-8'))['pages'])
        n_pre = len(rows)
        rows = [r for r in rows if r[0] not in mf]
        print(f"microfilm title-card pages dropped: {n_pre - len(rows):,} "
              f"(exclude-list {len(mf):,})", flush=True)
    ids = [r[0] for r in rows]
    real_sys = [r[1] for r in rows]
    texts = [r[2] for r in rows]
    n_real = len(rows)
    del rows
    streams = [norm_stream(t)[0] for t in texts]
    del texts
    print(f"real pages: {n_real:,}; letters={sum(map(len, streams)):,} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- canonical masks (maskcanon; keyed by page index) ----
    id_to_idx = {p: i for i, p in enumerate(ids)}
    masks = {}
    q = ("SELECT page_id, spans_json FROM track1_matches WHERE cat IN ("
         + ",".join("'%s'" % c for c in CANON_CATS) + ")")
    for pid, spans_json in src.execute(q):
        pi = id_to_idx.get(pid)
        if pi is None:
            continue
        masks.setdefault(pi, []).extend(
            (int(s[0]), int(s[1])) for s in json.loads(spans_json))
    n_iv = sum(len(v) for v in masks.values())
    print(f"masks[maskcanon]: {len(masks):,} pages, {n_iv:,} Track-1 intervals",
          flush=True)

    # ---- masked-reduced length per real page (stratification variable) ----
    strat_len = np.zeros(n_real, dtype=np.int32)
    for i in range(n_real):
        strat_len[i] = len(streams[i]) - coverage(masks.get(i), len(streams[i]))

    # ---- leak-clean decoy source pool ----
    excluded = set()
    tables = {r[0] for r in src.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if 'accepted_pairs_canonmask' in tables:
        for (pa,) in src.execute("SELECT page_a FROM accepted_pairs_canonmask"):
            excluded.add(pa)
        for (pb,) in src.execute("SELECT page_b FROM accepted_pairs_canonmask"):
            excluded.add(pb)
    elif mode == 'full':
        # belt-and-braces: main() already hard-aborts full mode without it
        print("ABORT: full mode requires accepted_pairs_canonmask", flush=True)
        sys.exit(1)
    else:
        print("  (smoke: no accepted_pairs_canonmask -> strict-exclusion join "
              "skipped)", flush=True)
    if 'track1_matches' in tables:
        for (pid,) in src.execute("SELECT page_id FROM track1_matches"):
            excluded.add(pid)
    if 'track1_candidates' in tables:
        for (pid,) in src.execute(
                "SELECT page_id FROM track1_candidates WHERE p_same_work>=0.5"):
            excluded.add(pid)
    pool = [i for i in range(n_real) if ids[i] not in excluded]
    print(f"leak-clean decoy pool: {len(pool):,} / {n_real:,} pages "
          f"({len(excluded):,} excluded)", flush=True)

    # ---- stratified proportional sampling: largest-remainder + adjacent
    #      refill (Codex code #7); cohorts disjoint by construction ----
    rng = np.random.default_rng(SEED)
    pool_by_stratum = {s: [] for s in range(N_STRATA)}
    for i in pool:
        pool_by_stratum[stratum(int(strat_len[i]))].append(i)
    for s in range(N_STRATA):
        rng.shuffle(pool_by_stratum[s])
    real_strata = np.zeros(N_STRATA, dtype=np.float64)
    for s_val in strat_len:
        real_strata[stratum(int(s_val))] += 1
    real_prop = real_strata / max(1.0, real_strata.sum())
    alloc = largest_remainder(real_prop, decoy_n)      # per-stratum, per cohort
    selections = {c: [] for c in COHORT_TAGS}
    leftover = {}
    deficit = {c: np.zeros(N_STRATA, dtype=np.int64) for c in COHORT_TAGS}
    for s in range(N_STRATA):
        avail = pool_by_stratum[s]
        k = int(alloc[s])
        t25 = avail[:k]
        t12 = avail[k:2 * k]
        selections['D25'].extend(t25)
        selections['D12'].extend(t12)
        leftover[s] = avail[2 * k:]
        deficit['D25'][s] = k - len(t25)
        deficit['D12'][s] = k - len(t12)
    for c in COHORT_TAGS:                    # refill shortfalls from neighbors
        for s in range(N_STRATA):
            need = int(deficit[c][s])
            if need <= 0:
                continue
            for dist in range(1, N_STRATA):
                for nb in (s - dist, s + dist):
                    if need <= 0 or not (0 <= nb < N_STRATA):
                        continue
                    take = leftover[nb][:need]
                    leftover[nb] = leftover[nb][len(take):]
                    selections[c].extend(take)
                    need -= len(take)
            if need > 0:
                print(f"  WARN: cohort {c} stratum {s} short by {need} "
                      f"(pool exhausted)", flush=True)
    for c in COHORT_TAGS:
        print(f"  cohort {c}: {len(selections[c]):,} decoys (target {decoy_n})",
              flush=True)

    # ---- build combined stream list (real first, then decoys) ----
    sys_uniq = {s: i for i, s in enumerate(dict.fromkeys(real_sys))}
    sys_codes = [sys_uniq[s] for s in real_sys]
    ids_all = list(ids)
    sys_all = list(real_sys)
    is_decoy = np.zeros(n_real, dtype=bool)      # grows below
    cohort_code = np.full(n_real, -1, dtype=np.int8)
    dec_is_decoy = []
    dec_cohort = []
    dec_strat = []
    for ci, c in enumerate(COHORT_TAGS):
        chunk = COHORT_CHUNK[c]
        for src_i in selections[c]:
            ms = strip_masks(streams[src_i], masks.get(src_i))
            seed = int.from_bytes(
                hashlib.blake2b((c + '|' + ids[src_i]).encode('utf-8'),
                                digest_size=8).digest(), 'little')
            ds = make_decoy(ms, chunk, np.random.default_rng(seed))
            streams.append(ds)
            sys_codes.append(sys_uniq[real_sys[src_i]])       # inherit source sys
            ids_all.append(f'{c}_{ids[src_i]}')
            sys_all.append(real_sys[src_i])
            dec_is_decoy.append(True)
            dec_cohort.append(ci)
            dec_strat.append(len(ds))
    n_dec_total = len(dec_is_decoy)
    is_decoy = np.concatenate([is_decoy, np.array(dec_is_decoy, dtype=bool)])
    cohort_code = np.concatenate([cohort_code, np.array(dec_cohort, dtype=np.int8)])
    strat_len = np.concatenate([strat_len, np.array(dec_strat, dtype=np.int32)])
    sys_codes = np.array(sys_codes, dtype=np.int32)
    n_total = len(streams)
    n_dec = {ci: int((cohort_code == ci).sum()) for ci in range(len(COHORT_TAGS))}
    print(f"combined index: {n_total:,} pages ({n_real:,} real + "
          f"{n_dec_total:,} decoy: " +
          ", ".join(f"{COHORT_TAGS[ci]}={n_dec[ci]:,}" for ci in n_dec) + ")",
          flush=True)

    # ---- engine ----
    pa, pb, cnt, mina, maxa, minb, maxb, estats = engine_np.build_candidates(
        streams, sys_codes, df_drop=DF_DROP, band=BAND, min_anchors=MIN_ANCHORS,
        masks=masks, spill_dir=SPILL_DIR)
    n_cand = len(pa)
    print(f"candidates: {n_cand:,} ({time.time() - t0:.0f}s)", flush=True)
    phase_mark(con, 'engine_done')

    # ---- raw spill table (bucket columns included; Codex code #5) ----
    con.execute("DROP TABLE IF EXISTS track2_wide_raw")
    con.execute("""
        CREATE TABLE track2_wide_raw (
            page_a TEXT, page_b TEXT, sys_a TEXT, sys_b TEXT,
            a0 INT, a1 INT, b0 INT, b1 INT, n_anchors INT,
            alen INT, dens REAL, minlen INT,
            alen_bin INT, dens_bin INT, stratum INT,
            mask_ov_a REAL, mask_ov_b REAL)""")
    ins_sql = ("INSERT INTO track2_wide_raw VALUES "
               "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    decoy_counts = np.zeros(
        (len(COHORT_TAGS), N_ALEN_BINS, N_DENS_BINS, N_STRATA), dtype=np.int64)
    reservoir = {ci: [] for ci in range(len(COHORT_TAGS))}
    res_seen = {ci: 0 for ci in range(len(COHORT_TAGS))}
    res_rng = np.random.default_rng(SEED + 7)

    best = {}                       # packed (ia<<20|ib) -> pair-best value tuple
    strict_pairs = set()            # packed keys of real-real strict-class pairs
    spilled = 0

    def row_of(k, v):
        ia_, ib_ = k >> _PKEY_BITS, k & ((1 << _PKEY_BITS) - 1)
        return (ids_all[ia_], ids_all[ib_], sys_all[ia_], sys_all[ib_],
                v[2], v[3], v[4], v[5], v[6], v[0], round(v[1], 4), v[7],
                alen_bin(v[0]), dens_bin(v[1]), stratum(v[7]),
                round(v[8], 4), round(v[9], 4))

    def flush(buf):
        nonlocal spilled
        for j in range(0, len(buf), INSERT_BATCH):
            con.execute("BEGIN")
            con.executemany(ins_sql, buf[j:j + INSERT_BATCH])
            con.execute("COMMIT")
        spilled += len(buf)

    n_rej_short = n_wide_seg = 0
    n_decoy_real = n_decoy_decoy = 0
    t1 = time.time()
    _isd = is_decoy
    for i in range(n_cand):
        ia = int(pa[i])
        ib = int(pb[i])
        sa = streams[ia]
        sb = streams[ib]
        a0 = int(mina[i]) - MARGIN
        a0 = a0 if a0 > 0 else 0
        a1 = int(maxa[i]) + K + MARGIN
        a1 = a1 if a1 < len(sa) else len(sa)
        b0 = int(minb[i]) - MARGIN
        b0 = b0 if b0 > 0 else 0
        b1 = int(maxb[i]) + K + MARGIN
        b1 = b1 if b1 < len(sb) else len(sb)
        if a1 - a0 > SPAN_CAP or b1 - b0 > SPAN_CAP:
            a1 = min(a1, a0 + SPAN_CAP)
            b1 = min(b1, b0 + SPAN_CAP)
        la = a1 - a0
        lb = b1 - b0
        if (la if la < lb else lb) < MIN_SPAN:
            n_rej_short += 1
            continue
        alen = la if la > lb else lb
        cutoff = int(0.45 * alen) + 1
        dist = Levenshtein.distance(sa[a0:a1], sb[b0:b1], score_cutoff=cutoff)
        density = dist / alen

        da = _isd[ia]
        db = _isd[ib]
        if da and db:
            n_decoy_decoy += 1
            continue
        if da or db:
            # decoy-real null draw — SEGMENT-level (documented conservative;
            # see module docstring). Counted only in the wide-not-strict class,
            # matching the stored real rows.
            if alen < WIDE_MIN_ALEN or density > WIDE_MAX_DENS:
                continue
            if density <= accept_density(alen):
                continue
            n_decoy_real += 1
            ci = int(cohort_code[ia]) if da else int(cohort_code[ib])
            ml = strat_len[ia] if strat_len[ia] < strat_len[ib] else strat_len[ib]
            decoy_counts[ci, alen_bin(alen), dens_bin(density),
                         stratum(int(ml))] += 1
            res_seen[ci] += 1
            row = (ids_all[ia], ids_all[ib], int(alen), round(density, 4),
                   int(ml), stratum(int(ml)), int(cnt[i]))
            if len(reservoir[ci]) < RESERVOIR_MAX:
                reservoir[ci].append(row)
            else:
                j = int(res_rng.integers(0, res_seen[ci]))
                if j < RESERVOIR_MAX:
                    reservoir[ci][j] = row
            continue

        # real-real
        key = (ia << _PKEY_BITS) | ib
        if density <= accept_density(alen):
            # strict-class segment -> the whole PAIR is excluded from the
            # sidecar (Codex code BLOCKER 1): record, drop any pair-best
            # entry; already-spilled raw rows are purged after the loop.
            strict_pairs.add(key)
            best.pop(key, None)
            continue
        if alen >= WIDE_MIN_ALEN and density <= WIDE_MAX_DENS:
            if key in strict_pairs:
                continue                       # pair already known strict
            n_wide_seg += 1
            ml = int(strat_len[ia] if strat_len[ia] < strat_len[ib]
                     else strat_len[ib])
            mov_a = overlap_frac(masks.get(ia), a0, a1)
            mov_b = overlap_frac(masks.get(ib), b0, b1)
            old = best.get(key)
            if old is None or (alen, -density) > (old[0], -old[1]):
                best[key] = (alen, density, a0, a1, b0, b1, int(cnt[i]),
                             ml, mov_a, mov_b)
            if len(best) >= SPILL_FLUSH:
                buf = [row_of(k, v) for k, v in best.items()]
                print(f"  spill flush: {len(buf):,} rows "
                      f"(total spilled {spilled + len(buf):,})", flush=True)
                flush(buf)
                best.clear()
        if i and i % 20_000_000 == 0:
            print(f"  verify {i:,}/{n_cand:,} wide_dict={len(best):,} "
                  f"spilled={spilled:,} strict_pairs={len(strict_pairs):,} "
                  f"decoy_real={n_decoy_real:,} ({time.time() - t1:.0f}s)",
                  flush=True)

    # ---- final flush ----
    if best:
        flush([row_of(k, v) for k, v in best.items()])
        best.clear()
    n_strict_pairs = len(strict_pairs)
    print(f"verify done: candidates={n_cand:,} spilled_raw={spilled:,} "
          f"strict_pairs={n_strict_pairs:,} wide_segments={n_wide_seg:,} "
          f"decoy_real={n_decoy_real:,} decoy_decoy={n_decoy_decoy:,} "
          f"rej_short={n_rej_short:,} ({time.time() - t1:.0f}s)", flush=True)

    # ---- purge spilled raw rows of strict pairs (Codex code BLOCKER 1) ----
    con.execute("DROP TABLE IF EXISTS track2_strict_pairs")
    con.execute("CREATE TABLE track2_strict_pairs("
                "page_a TEXT, page_b TEXT, PRIMARY KEY(page_a, page_b)) "
                "WITHOUT ROWID")
    sp_rows = [(ids_all[k >> _PKEY_BITS], ids_all[k & ((1 << _PKEY_BITS) - 1)])
               for k in strict_pairs]
    del strict_pairs
    for j in range(0, len(sp_rows), INSERT_BATCH):
        con.execute("BEGIN")
        con.executemany("INSERT OR IGNORE INTO track2_strict_pairs VALUES (?,?)",
                        sp_rows[j:j + INSERT_BATCH])
        con.execute("COMMIT")
    del sp_rows
    cur = con.execute("""
        DELETE FROM track2_wide_raw WHERE EXISTS (
            SELECT 1 FROM track2_strict_pairs sp
            WHERE sp.page_a = track2_wide_raw.page_a
              AND sp.page_b = track2_wide_raw.page_b)""")
    n_purged = cur.rowcount
    print(f"strict-pair purge: {n_purged:,} spilled raw rows removed", flush=True)

    # ---- dedup raw -> immutable track2_wide_base (pair-best); index-backed
    #      (Codex code #5) ----
    con.execute("CREATE INDEX IF NOT EXISTS idx_raw_pair ON track2_wide_raw"
                "(page_a, page_b, alen DESC, dens ASC)")
    con.execute("DROP TABLE IF EXISTS track2_wide_base")
    con.execute("""
        CREATE TABLE track2_wide_base (
            page_a TEXT, page_b TEXT, sys_a TEXT, sys_b TEXT,
            a0 INT, a1 INT, b0 INT, b1 INT, n_anchors INT,
            alen INT, dens REAL, minlen INT,
            alen_bin INT, dens_bin INT, stratum INT,
            mask_ov_a REAL, mask_ov_b REAL)""")
    con.execute("""
        INSERT INTO track2_wide_base
        SELECT page_a,page_b,sys_a,sys_b,a0,a1,b0,b1,n_anchors,alen,dens,
               minlen,alen_bin,dens_bin,stratum,mask_ov_a,mask_ov_b FROM (
          SELECT *, ROW_NUMBER() OVER (
              PARTITION BY page_a,page_b ORDER BY alen DESC, dens ASC) rn
          FROM track2_wide_raw) WHERE rn=1""")
    con.execute("DROP TABLE track2_wide_raw")     # base = the immutable artifact
    n_wide = con.execute("SELECT COUNT(*) FROM track2_wide_base").fetchone()[0]
    print(f"deduped base rows: {n_wide:,} (from {spilled:,} spilled, "
          f"{n_purged:,} strict-purged)", flush=True)

    # ---- persisted pre-prune per-bucket observed histogram (Codex #4) ----
    con.execute("DROP TABLE IF EXISTS track2_obs_buckets")
    con.execute("CREATE TABLE track2_obs_buckets("
                "alen_bin INT, dens_bin INT, stratum INT, observed_real INT, "
                "PRIMARY KEY(alen_bin, dens_bin, stratum)) WITHOUT ROWID")
    con.execute("""
        INSERT INTO track2_obs_buckets
        SELECT alen_bin, dens_bin, stratum, COUNT(*)
        FROM track2_wide_base GROUP BY alen_bin, dens_bin, stratum""")

    # ---- persist decoy counts + reservoir ----
    con.execute("DROP TABLE IF EXISTS track2_decoy_counts")
    con.execute("CREATE TABLE track2_decoy_counts("
                "cohort TEXT, alen_bin INT, dens_bin INT, stratum INT, decoy_n INT)")
    dc_rows = []
    for ci in range(len(COHORT_TAGS)):
        for ab in range(N_ALEN_BINS):
            for dbn in range(N_DENS_BINS):
                for st in range(N_STRATA):
                    v = int(decoy_counts[ci, ab, dbn, st])
                    if v:
                        dc_rows.append((COHORT_TAGS[ci], ab, dbn, st, v))
    con.executemany("INSERT INTO track2_decoy_counts VALUES (?,?,?,?,?)", dc_rows)
    con.execute("DROP TABLE IF EXISTS track2_decoy_reservoir")
    con.execute("CREATE TABLE track2_decoy_reservoir("
                "cohort TEXT, page_a TEXT, page_b TEXT, alen INT, dens REAL, "
                "minlen INT, stratum INT, n_anchors INT)")
    for ci in range(len(COHORT_TAGS)):
        con.executemany(
            "INSERT INTO track2_decoy_reservoir VALUES (?,?,?,?,?,?,?,?)",
            [(COHORT_TAGS[ci],) + r for r in reservoir[ci]])

    # ---- chain step-6 comparison count ----
    if 'accepted_pairs_canonmask' in tables:
        step6 = src.execute(
            "SELECT COUNT(*) FROM accepted_pairs_canonmask").fetchone()[0]
    else:
        step6 = None

    # ---- meta ----
    meta_set(con, 'n_real_pages', n_real)
    meta_set(con, 'n_decoy_total', n_dec_total)
    for ci in range(len(COHORT_TAGS)):
        meta_set(con, f'n_decoy_{COHORT_TAGS[ci]}', n_dec[ci])
    meta_set(con, 'engine_stats', {k: v for k, v in estats.items()
                                   if isinstance(v, (int, float, str))})
    funnel.update(dict(
        n_real=n_real, n_decoy_total=n_dec_total,
        n_decoy_per_cohort={COHORT_TAGS[ci]: n_dec[ci]
                            for ci in range(len(COHORT_TAGS))},
        pool_size=len(pool), excluded=len(excluded),
        candidate_pairs=n_cand, rej_short=n_rej_short,
        decoy_real=n_decoy_real, decoy_decoy=n_decoy_decoy,
        strict_pairs_seen=n_strict_pairs, strict_purged_raw=n_purged,
        wide_spilled_raw=spilled, wide_deduped=n_wide,
        chain_step6_count=step6))
    if step6:
        funnel['strict_vs_step6_delta_pct'] = round(
            (n_strict_pairs - step6) / step6 * 100.0, 2)
    meta_set(con, 'funnel', funnel)
    con.commit()
    phase_mark(con, 'spill_done')


# =====================================================================
# null model — reads ONLY persisted pre-prune tables (Codex code #4)
# =====================================================================
def compute_null_model(con):
    n_real = int(meta_get(con, 'n_real_pages'))
    n_dec = {ci: int(meta_get(con, f'n_decoy_{COHORT_TAGS[ci]}', '0'))
             for ci in range(len(COHORT_TAGS))}
    factor = {ci: n_real / (2.0 * max(1, n_dec[ci]))
              for ci in range(len(COHORT_TAGS))}
    dc = np.zeros((len(COHORT_TAGS), N_ALEN_BINS, N_DENS_BINS, N_STRATA),
                  dtype=np.float64)
    for coh, ab, dbn, st, dn in con.execute(
            "SELECT cohort,alen_bin,dens_bin,stratum,decoy_n "
            "FROM track2_decoy_counts"):
        dc[COHORT_TAGS.index(coh), ab, dbn, st] = dn
    obs = np.zeros((N_ALEN_BINS, N_DENS_BINS, N_STRATA), dtype=np.float64)
    for ab, dbn, st, n in con.execute(
            "SELECT alen_bin,dens_bin,stratum,observed_real "
            "FROM track2_obs_buckets"):
        obs[ab, dbn, st] = n

    # expected null per cohort + conservative envelope (max over cohorts)
    exp = np.stack([dc[ci] * factor[ci] for ci in range(len(COHORT_TAGS))])
    envelope = exp.max(axis=0)
    # "+1" upper-bound variants (no bucket ever gets fdr=0 from 0 draws)
    env_ub = np.stack([(dc[ci] + 1.0) * factor[ci]
                       for ci in range(len(COHORT_TAGS))]).max(axis=0)
    support = dc.max(axis=0)              # draws backing the envelope estimate

    with np.errstate(divide='ignore', invalid='ignore'):
        bucket_ub = np.where(obs > 0, np.minimum(1.0, env_ub / obs), 0.0)
    # hierarchical fallback (Codex code #2): same-stratum marginal + global
    obs_s = obs.sum(axis=(0, 1))                                   # (S,)
    strat_ub = np.zeros(N_STRATA)
    for st in range(N_STRATA):
        if obs_s[st] > 0:
            strat_ub[st] = min(1.0, max(
                (dc[ci, :, :, st].sum() + 1.0) * factor[ci]
                for ci in range(len(COHORT_TAGS))) / obs_s[st])
    obs_g = obs.sum()
    glob_ub = 0.0
    if obs_g > 0:
        glob_ub = min(1.0, max((dc[ci].sum() + 1.0) * factor[ci]
                               for ci in range(len(COHORT_TAGS))) / obs_g)
    fallback = np.maximum(bucket_ub,
                          np.maximum(strat_ub[None, None, :], glob_ub))
    raw = np.where(obs > 0,
                   np.where(support >= K_SUPPORT, bucket_ub, fallback), 0.0)
    smoothed = monotone_majorant(raw)

    # empirical null-tail p per bucket, within stratum (Codex code #3):
    # T = envelope null mass with alen' >= alen AND dens' <= dens
    T = np.cumsum(envelope[::-1, :, :], axis=0)[::-1, :, :]
    T = np.cumsum(T, axis=1)
    n_st = envelope.sum(axis=(0, 1))                                # (S,)
    p_tail = (1.0 + T) / (1.0 + n_st[None, None, :])
    return dict(n_real=n_real, n_dec=n_dec, factor=factor, dc=dc, obs=obs,
                exp=exp, envelope=envelope, support=support, raw=raw,
                smoothed=smoothed, p_tail=p_tail)


def monotone_majorant(raw):
    """Conservative one-sided PAV: the smallest function >= raw that is
    non-decreasing in dens and non-increasing in alen, per stratum. A single
    cummax pass per axis is exact because the alen-cummax of dens-monotone
    slices stays dens-monotone. Only ever RAISES the FDR estimate."""
    out = np.maximum.accumulate(raw, axis=1)               # dens non-decreasing
    out = np.maximum.accumulate(out[::-1, :, :], axis=0)[::-1, :, :]
    return np.minimum(out, 1.0)


# =====================================================================
# PHASE B: null model tables
# =====================================================================
def phase_null(con):
    m = compute_null_model(con)
    con.execute("DROP TABLE IF EXISTS track2_null_buckets")
    con.execute("""
        CREATE TABLE track2_null_buckets (
            cohort TEXT, alen_bin INT, dens_bin INT, stratum INT,
            decoy_n INT, expected_null REAL, observed_real REAL,
            support INT, local_fdr REAL)""")
    rows = []
    for ab in range(N_ALEN_BINS):
        for dbn in range(N_DENS_BINS):
            for st in range(N_STRATA):
                obs = m['obs'][ab, dbn, st]
                env = m['envelope'][ab, dbn, st]
                if obs == 0 and env == 0:
                    continue
                smoothed = m['smoothed'][ab, dbn, st]
                sup = int(m['support'][ab, dbn, st])
                for ci in range(len(COHORT_TAGS)):
                    if m['dc'][ci, ab, dbn, st] == 0 and obs == 0:
                        continue
                    rows.append((COHORT_TAGS[ci], ab, dbn, st,
                                 int(m['dc'][ci, ab, dbn, st]),
                                 float(m['exp'][ci, ab, dbn, st]),
                                 float(obs), sup, float(smoothed)))
                rows.append(('ENVELOPE', ab, dbn, st, -1, float(env),
                             float(obs), sup, float(smoothed)))
    con.executemany("INSERT INTO track2_null_buckets VALUES (?,?,?,?,?,?,?,?,?)",
                    rows)
    con.commit()
    print(f"null buckets: {len(rows):,} rows "
          f"(observed_real total={int(m['obs'].sum()):,}, "
          f"envelope_null total={m['envelope'].sum():.0f}, "
          f"buckets with support>={K_SUPPORT}: "
          f"{int(((m['support'] >= K_SUPPORT) & (m['obs'] > 0)).sum())})",
          flush=True)
    phase_mark(con, 'null_done')


# =====================================================================
# PHASE C: bucket scores + global BH -> new table, atomic swap (Codex #4, #5)
# =====================================================================
def phase_scored(con):
    m = compute_null_model(con)
    mask = m['obs'] > 0
    idx = np.argwhere(mask)                       # (nb, 3) of (ab, db, st)
    if not len(idx):
        print("no wide rows to score", flush=True)
        con.execute("BEGIN")
        con.execute("DROP TABLE IF EXISTS track2_wide")
        con.execute("""
            CREATE TABLE track2_wide AS
            SELECT *, NULL AS p_local_bucket, NULL AS q_value,
                   NULL AS flank_dist, NULL AS flank_class,
                   NULL AS dup_shelf, NULL AS dup_lines
            FROM track2_wide_base WHERE 0""")
        con.execute("COMMIT")
        meta_set(con, 'n_scored', 0)
        meta_set(con, 'n_deleted_lowvalue', 0)
        meta_set(con, 'n_survivors', 0)
        con.commit()
        phase_mark(con, 'scored')
        return
    p = m['p_tail'][mask]
    n_rows = m['obs'][mask]
    p_local = 1.0 - m['smoothed'][mask]           # bucket precision (design #3)

    # global BH over ALL real wide rows, computed exactly on bucket groups
    # (p_tail is bucket-constant): rank = cumulative row count in p order.
    order = np.argsort(p, kind='stable')
    p_o = p[order]
    cum = np.cumsum(n_rows[order])
    n_tot = cum[-1]
    q_o = np.minimum(np.minimum.accumulate((p_o * n_tot / cum)[::-1])[::-1], 1.0)
    q = np.empty(len(p))
    q[order] = q_o

    con.execute("DROP TABLE IF EXISTS track2_bucket_scores")
    con.execute("""
        CREATE TABLE track2_bucket_scores (
            alen_bin INT, dens_bin INT, stratum INT,
            -- p_local = 1 - local_fdr of the bucket. BUCKET-LEVEL PRECISION,
            -- not a pair probability (Codex design #3).
            p_local REAL, p_tail REAL, q_value REAL, support INT, n_rows INT,
            PRIMARY KEY (alen_bin, dens_bin, stratum)) WITHOUT ROWID""")
    con.executemany(
        "INSERT INTO track2_bucket_scores VALUES (?,?,?,?,?,?,?,?)",
        [(int(idx[j, 0]), int(idx[j, 1]), int(idx[j, 2]),
          round(float(p_local[j]), 6), float(p[j]), round(float(q[j]), 6),
          int(m['support'][idx[j, 0], idx[j, 1], idx[j, 2]]), int(n_rows[j]))
         for j in range(len(idx))])

    # build the scored+pruned final table from the IMMUTABLE base, then swap
    # atomically (crash-safe resume: base is untouched until cleanup). The
    # spec's DELETE is realized as prune-at-insert (equivalent, less churn).
    con.execute("DROP TABLE IF EXISTS track2_wide_new")
    con.execute("""
        CREATE TABLE track2_wide_new (
            page_a TEXT, page_b TEXT, sys_a TEXT, sys_b TEXT,
            a0 INT, a1 INT, b0 INT, b1 INT, n_anchors INT,
            alen INT, dens REAL, minlen INT,
            alen_bin INT, dens_bin INT, stratum INT,
            mask_ov_a REAL, mask_ov_b REAL,
            -- p_local_bucket: bucket-level precision, NOT pair probability
            p_local_bucket REAL, q_value REAL,
            flank_dist REAL, flank_class TEXT, dup_shelf INT, dup_lines REAL)""")
    con.execute(f"""
        INSERT INTO track2_wide_new
        SELECT b.page_a, b.page_b, b.sys_a, b.sys_b, b.a0, b.a1, b.b0, b.b1,
               b.n_anchors, b.alen, b.dens, b.minlen,
               b.alen_bin, b.dens_bin, b.stratum, b.mask_ov_a, b.mask_ov_b,
               s.p_local, s.q_value, NULL, NULL, NULL, NULL
        FROM track2_wide_base b
        JOIN track2_bucket_scores s
          ON b.alen_bin = s.alen_bin AND b.dens_bin = s.dens_bin
         AND b.stratum = s.stratum
        WHERE NOT (s.p_local < {DELETE_P_LOCAL} AND s.q_value > {DELETE_Q_VALUE})
        """)
    con.execute("BEGIN")
    con.execute("DROP TABLE IF EXISTS track2_wide")
    con.execute("ALTER TABLE track2_wide_new RENAME TO track2_wide")
    con.execute("COMMIT")
    con.execute("CREATE INDEX IF NOT EXISTS idx_wide_pair "
                "ON track2_wide(page_a, page_b)")
    n_base = con.execute("SELECT COUNT(*) FROM track2_wide_base").fetchone()[0]
    surv = con.execute("SELECT COUNT(*) FROM track2_wide").fetchone()[0]
    meta_set(con, 'n_scored', int(n_base))
    meta_set(con, 'n_deleted_lowvalue', int(n_base - surv))
    meta_set(con, 'n_survivors', int(surv))
    con.commit()
    print(f"scored {n_base:,} rows (bucket-level SQL, no fetchall); pruned "
          f"{n_base - surv:,} low-value; survivors {surv:,}", flush=True)
    phase_mark(con, 'scored')


# =====================================================================
# PHASE D: flank/dup enrichment for survivors only (design R4: joins flagged)
# =====================================================================
def phase_enrich(con, src):
    survivors = con.execute(
        "SELECT rowid,page_a,page_b,sys_a,sys_b,a0,a1,b0,b1 FROM track2_wide"
    ).fetchall()
    if not survivors:
        print("no survivors to enrich", flush=True)
        phase_mark(con, 'enriched')
        return
    needed = set()
    for r in survivors:
        needed.add(r[1])
        needed.add(r[2])
    text_of = {}
    for pid, txt in src.execute("SELECT page_id, text FROM pages"):
        if pid in needed:
            text_of[pid] = txt
    stream_of = {pid: norm_stream(t)[0] for pid, t in text_of.items()}
    shelf = load_shelf_variants()
    print(f"enrich: {len(survivors):,} survivors, {len(text_of):,} pages loaded",
          flush=True)

    updates = []
    for rowid, pa, pb, sa, sb, a0, a1, b0, b1 in survivors:
        strA = stream_of.get(pa, '')
        strB = stream_of.get(pb, '')
        fd = flank_dist(strA, strB, a0, a1, b0, b1) if (strA and strB) else None
        ds = int(same_shelf(sa, sb, shelf))
        dl = line_agreement(text_of.get(pa, ''), text_of.get(pb, ''))
        updates.append((-1.0 if fd is None else round(fd, 4),
                        flank_class_of(fd), ds, dl, rowid))
    for j in range(0, len(updates), INSERT_BATCH):
        con.execute("BEGIN")
        con.executemany(
            "UPDATE track2_wide SET flank_dist=?, flank_class=?, dup_shelf=?, "
            "dup_lines=? WHERE rowid=?", updates[j:j + INSERT_BATCH])
        con.execute("COMMIT")
    from collections import Counter
    fclasses = dict(Counter(u[1] for u in updates))
    meta_set(con, 'flank_classes', fclasses)
    con.commit()
    print(f"enriched flank classes: {fclasses}", flush=True)
    phase_mark(con, 'enriched')


# =====================================================================
# PHASE E: cleanup — drop the immutable base + conditional VACUUM (Codex #5)
# =====================================================================
def phase_cleanup(con, out_path):
    con.execute("DROP TABLE IF EXISTS track2_wide_base")
    con.execute("DROP TABLE IF EXISTS track2_strict_pairs")
    con.commit()
    try:
        free = shutil.disk_usage(os.path.dirname(out_path)).free
        size = os.path.getsize(out_path)
        if free > int(size * 1.2) + (2 << 30):
            print(f"VACUUM (db {size / 2**30:.1f} GB, free {free / 2**30:.1f} GB)",
                  flush=True)
            con.execute("VACUUM")
        else:
            print(f"VACUUM SKIPPED: free disk {free / 2**30:.1f} GB < "
                  f"1.2x db {size / 2**30:.1f} GB + 2 GB headroom", flush=True)
            meta_set(con, 'vacuum_skipped', 'insufficient_disk')
            con.commit()
    except Exception as e:                                   # noqa: BLE001
        print(f"VACUUM check failed (skipping): {e!r}", flush=True)
    phase_mark(con, 'cleanup')


# =====================================================================
# reporting — reads ONLY persisted tables + meta (Codex code #4)
# =====================================================================
def write_report(con, mode, src_path):
    funnel = json.loads(meta_get(con, 'funnel', '{}'))
    m = compute_null_model(con) if has_phase(con, 'spill_done') else None
    md = [f"# Track-2 WIDE tier report ({mode})", ""]
    md.append(f"Generated {datetime.datetime.now():%Y-%m-%d %H:%M}; "
              f"source `{src_path}`; git `{meta_get(con, 'git', '?')}`.")
    md.append("")
    md.append("## Cohort sizes")
    md.append(f"- real pages: {funnel.get('n_real'):,}")
    md.append(f"- decoys: {funnel.get('n_decoy_total'):,} "
              f"{funnel.get('n_decoy_per_cohort')}")
    md.append(f"- leak-clean pool: {funnel.get('pool_size'):,} "
              f"({funnel.get('excluded'):,} excluded)")
    md.append("")
    md.append("## Volume funnel")
    for k in ('candidate_pairs', 'rej_short', 'decoy_decoy', 'decoy_real',
              'strict_pairs_seen', 'strict_purged_raw', 'wide_spilled_raw',
              'wide_deduped'):
        v = funnel.get(k)
        md.append(f"- {k}: {v:,}" if isinstance(v, int) else f"- {k}: {v}")
    md.append(f"- pruned low-value: {meta_get(con, 'n_deleted_lowvalue', 'n/a')}")
    md.append(f"- survivors (post score+prune): {meta_get(con, 'n_survivors', 'n/a')}")
    md.append("")
    md.append("## Strict-pairs vs chain step-6")
    s6 = funnel.get('chain_step6_count')
    md.append(f"- strict-class real-real PAIRS seen this run: "
              f"{funnel.get('strict_pairs_seen'):,} (pair-level; a pair with "
              f"any strict segment is excluded from the sidecar entirely)")
    if s6:
        md.append(f"- chain step-6 `accepted_pairs_canonmask` rows: {s6:,}")
        md.append(f"- delta: {funnel.get('strict_vs_step6_delta_pct')}% "
                  f"(small DF perturbation from injected decoys is expected)")
    else:
        md.append("- chain step-6 count unavailable (no accepted_pairs_canonmask)")
    md.append("")
    md.append("## Normalization derivation")
    md.append("Opportunity normalization -- expected null real-real per bucket =")
    md.append("`decoy_real_count * n_real / (2 * n_dec_cohort)`, conservative")
    md.append("envelope = MAX over the two chunk cohorts. Factors this run:")
    md.append("")
    if m:
        for ci in range(len(COHORT_TAGS)):
            md.append(f"- {COHORT_TAGS[ci]}: factor = {m['n_real']:,} / "
                      f"(2 * {m['n_dec'][ci]:,}) = {m['factor'][ci]:.2f}")
    md.append("")
    md.append("The `/2` is the C(n_real,2) vs n_dec*n_real opportunity asymmetry:")
    md.append("real-real pairs are unordered (both real sides), decoy-real pairs")
    md.append("are already distinct unordered pairs (only the decoy side spent).")
    md.append("See the module docstring for the full step-by-step.")
    md.append("")
    md.append("## Null estimation policy")
    md.append(f"- bucket-level local FDR used AS-IS only when the envelope's "
              f"supporting decoy count >= {K_SUPPORT}; below that: "
              f"max(bucket, same-stratum marginal, global), each with a `+1` "
              f"upper bound on the decoy count (no bucket gets fdr=0 / "
              f"p_local=1.0 from absence of evidence), THEN the monotone "
              f"majorant (non-decreasing in dens, non-increasing in alen).")
    md.append("- decoy-real null counting is SEGMENT-level while the real side "
              "is pair-best + strict-pair-excluded: segments-per-pair "
              "multiplicity >= 1 only INFLATES the null -> conservative.")
    md.append("- q_value = global BH over per-row empirical null-tail "
              "p-values, p = (1 + null mass with alen'>=alen, dens'<=dens in "
              "the row's stratum) / (1 + stratum null mass).")
    md.append("")
    if m is not None:
        md.append("## Null buckets (envelope; top by observed_real)")
        md.append("| alen_bin | dens_bin | stratum | support | expected_null "
                  "| observed_real | local_fdr | p_local |")
        md.append("|---|---|---|---|---|---|---|---|")
        cells = []
        for ab in range(N_ALEN_BINS):
            for dbn in range(N_DENS_BINS):
                for st in range(N_STRATA):
                    obs = m['obs'][ab, dbn, st]
                    if obs <= 0:
                        continue
                    cells.append((obs, ab, dbn, st))
        cells.sort(reverse=True)
        albl = [f"[{ALEN_BINS[i]},{ALEN_BINS[i+1]})" if i + 1 < N_ALEN_BINS
                else f"{ALEN_BINS[i]}+" for i in range(N_ALEN_BINS)]
        for obs, ab, dbn, st in cells[:25]:
            env = m['envelope'][ab, dbn, st]
            fdr = m['smoothed'][ab, dbn, st]
            sup = int(m['support'][ab, dbn, st])
            md.append(f"| {albl[ab]} | [{dbn*0.05:.2f},{(dbn+1)*0.05:.2f}) "
                      f"| {st} | {sup} | {env:.2f} | {int(obs):,} "
                      f"| {fdr:.4f} | {1-fdr:.4f} |")
    md.append("")
    md.append("## Reliability caveats")
    md.append("- `p_local_bucket` is BUCKET-LEVEL empirical precision (1 - local "
              "FDR), NOT a per-pair probability (Codex design #3). It ignores "
              "anchor count, coverage, length asymmetry, shelf/dup signals.")
    md.append("- The chunk-shuffle null assumes decoy-real chance rate == "
              "unrelated real-real chance rate; the CHUNK=12/25 envelope is the "
              "conservative mitigation of the unsettled chunk size (design #4).")
    md.append("- Verification runs on UNMASKED streams, so canonical text can "
              "re-enter a window near an anchor; `mask_ov_a/mask_ov_b` let "
              "consumers drop rows whose evidence is mostly masked (design #5).")
    md.append("- Decoys inflate DF by the injected fraction, mildly tightening "
              "the DF<=100 cap vs a pure production run (reported delta above).")
    md.append("- `dup_shelf`/`dup_lines` flag same-object joins (design R4): "
              "these are FINDS for a scholar, kept and flagged, not dropped.")
    out_md = RESULTS + (r"\track2_wide_smoke_report.md" if mode == 'smoke'
                        else r"\track2_wide_report.md")
    open(out_md, 'w', encoding='utf-8').write('\n'.join(md))

    stats = dict(mode=mode, source=src_path, funnel=funnel,
                 n_survivors=meta_get(con, 'n_survivors'),
                 n_deleted_lowvalue=meta_get(con, 'n_deleted_lowvalue'),
                 flank_classes=json.loads(meta_get(con, 'flank_classes', '{}')),
                 k_support=K_SUPPORT,
                 phases={p: meta_get(con, f'phase_{p}') for p in
                         ('engine_done', 'spill_done', 'null_done', 'scored',
                          'enriched', 'cleanup')})
    if m is not None:
        stats['null_factor_per_cohort'] = {
            COHORT_TAGS[ci]: m['factor'][ci] for ci in range(len(COHORT_TAGS))}
        stats['observed_real_total'] = int(m['obs'].sum())
        stats['envelope_null_total'] = float(m['envelope'].sum())
        stats['buckets_with_support'] = int(
            ((m['support'] >= K_SUPPORT) & (m['obs'] > 0)).sum())
    out_json = RESULTS + (r"\track2_wide_smoke_stats.json" if mode == 'smoke'
                          else r"\track2_wide_stats.json")
    json.dump(stats, open(out_json, 'w', encoding='utf-8'),
              indent=1, ensure_ascii=False)
    print(f"report -> {out_md}\nstats  -> {out_json}", flush=True)


# =====================================================================
def check_resume_compat(con, src_path, decoy_n):
    """Refuse --resume if the sidecar's recorded run parameters differ
    (Codex code #6). No-op for a fresh sidecar (no meta yet)."""
    prior_src = meta_get(con, 'source_db')
    if prior_src is None:
        return
    problems = []
    if prior_src != src_path:
        problems.append(f"source_db {prior_src!r} != {src_path!r}")
    prior_mtime = meta_get(con, 'source_db_mtime')
    try:
        cur_mtime = datetime.datetime.fromtimestamp(
            os.path.getmtime(src_path)).isoformat()
    except OSError:
        cur_mtime = None
    if prior_mtime and cur_mtime and prior_mtime != cur_mtime:
        problems.append(f"source mtime changed {prior_mtime} -> {cur_mtime}")
    prior_target = meta_get(con, 'decoy_target_per_cohort')
    if prior_target is not None and int(prior_target) != decoy_n:
        problems.append(f"decoy target {prior_target} != {decoy_n}")
    prior_eng = meta_get(con, 'engine_params')
    cur_eng = dict(K=K, BAND=BAND, DF_DROP=DF_DROP, MIN_ANCHORS=MIN_ANCHORS,
                   MARGIN=MARGIN, MIN_SPAN=MIN_SPAN, SPAN_CAP=SPAN_CAP)
    if prior_eng is not None and json.loads(prior_eng) != cur_eng:
        problems.append("engine params differ from meta")
    prior_chunk = meta_get(con, 'cohort_chunk')
    if prior_chunk is not None and json.loads(prior_chunk) != COHORT_CHUNK:
        problems.append("cohort chunk sizes differ from meta")
    if problems:
        for p in problems:
            print(f"REFUSE --resume: {p}", flush=True)
        sys.exit(1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--smoke', action='store_true')
    ap.add_argument('--resume', action='store_true')
    args = ap.parse_args()

    set_below_normal()
    t0 = time.time()

    if args.smoke:
        mode = 'smoke'
        src_path = DATA + r"\mapv2_smoke.db"
        out_path = DATA + r"\track2_wide_smoke.db"
        decoy_n = DECOY_N_SMOKE
    else:
        mode = 'full'
        src_path = DATA + r"\fullcorpus_v2.db"
        out_path = DATA + r"\track2_wide.db"
        decoy_n = DECOY_N_FULL
        # ---- FULL-mode guards (chain state checked BEFORE opening the db) ----
        try:
            state = json.load(open(CHAIN_STATE, encoding='utf-8'))
        except Exception as e:                               # noqa: BLE001
            print(f"REFUSE full run: cannot read chain state {CHAIN_STATE}: "
                  f"{e!r}", flush=True)
            sys.exit(1)
        if CHAIN_STEP not in state.get('done', []):
            print(f"REFUSE full run: chain step '{CHAIN_STEP}' not in "
                  f"{CHAIN_STATE} done={state.get('done')}. "
                  f"(strict Track-2 artifact not built yet)", flush=True)
            sys.exit(1)
        # only NOW may we touch the source (read-only): artifact guards
        # (Codex code #6 — hard-abort on ANY missing table/column)
        _c = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)
        tabs = {r[0] for r in _c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        problems = []
        if 'accepted_pairs_canonmask' not in tabs:
            problems.append("missing table accepted_pairs_canonmask")
        t1m = {r[1] for r in _c.execute("PRAGMA table_info(track1_matches)")}
        if 'shadowed_by' not in t1m:
            problems.append("track1_matches lacks shadowed_by "
                            "(shadow step not applied)")
        t1c = {r[1] for r in _c.execute("PRAGMA table_info(track1_candidates)")}
        if 'p_same_work' not in t1c:
            problems.append("track1_candidates lacks p_same_work")
        pg = {r[1] for r in _c.execute("PRAGMA table_info(pages)")}
        if 'provenance' not in pg:
            problems.append("pages lacks provenance")
        _c.close()
        if problems:
            for p in problems:
                print(f"REFUSE full run: {p}", flush=True)
            sys.exit(1)

    print(f"=== track2_wide_run mode={mode} src={src_path} out={out_path} "
          f"resume={args.resume} decoys={decoy_n}/cohort ===", flush=True)

    src = sqlite3.connect(f"file:{src_path}?mode=ro", uri=True)  # ALWAYS read-only
    con = sqlite3.connect(out_path)
    con.isolation_level = None                # autocommit; we drive BEGIN/COMMIT
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    if args.resume:
        ensure_meta(con)
        check_resume_compat(con, src_path, decoy_n)
    else:
        reset_out(con)
    ensure_meta(con)

    funnel = json.loads(meta_get(con, 'funnel', '{}'))
    meta_set(con, 'mode', mode)
    meta_set(con, 'source_db', src_path)
    try:
        meta_set(con, 'source_db_mtime', datetime.datetime.fromtimestamp(
            os.path.getmtime(src_path)).isoformat())
    except Exception:                                        # noqa: BLE001
        pass
    meta_set(con, 'masks_version', MASKS_VERSION)
    meta_set(con, 'engine_params', dict(K=K, BAND=BAND, DF_DROP=DF_DROP,
                                        MIN_ANCHORS=MIN_ANCHORS, MARGIN=MARGIN,
                                        MIN_SPAN=MIN_SPAN, SPAN_CAP=SPAN_CAP))
    meta_set(con, 'cohort_chunk', COHORT_CHUNK)
    meta_set(con, 'decoy_target_per_cohort', decoy_n)
    meta_set(con, 'wide_def', dict(min_alen=WIDE_MIN_ALEN, max_dens=WIDE_MAX_DENS,
                                   pair_semantics='wide-only; any strict-class '
                                   'segment excludes the whole pair',
                                   null_semantics='decoy-real SEGMENT-level '
                                   '(conservative vs pair-best real side)'))
    meta_set(con, 'k_support', K_SUPPORT)
    meta_set(con, 'git', git_describe())
    meta_set(con, 'versions', dict(python=sys.version.split()[0],
                                   numpy=np.__version__,
                                   sqlite=sqlite3.sqlite_version))
    con.commit()

    if not (args.resume and has_phase(con, 'spill_done')):
        phase_engine_verify_spill(con, src, decoy_n, funnel, mode)
    else:
        print("[resume] skip engine/verify/spill", flush=True)

    if not (args.resume and has_phase(con, 'null_done')):
        phase_null(con)
    else:
        print("[resume] skip null", flush=True)

    if not (args.resume and has_phase(con, 'scored')):
        phase_scored(con)
    else:
        print("[resume] skip scored", flush=True)

    if not (args.resume and has_phase(con, 'enriched')):
        phase_enrich(con, src)
    else:
        print("[resume] skip enrich", flush=True)

    write_report(con, mode, src_path)

    if not (args.resume and has_phase(con, 'cleanup')):
        phase_cleanup(con, out_path)
    else:
        print("[resume] skip cleanup", flush=True)

    con.close()
    src.close()
    print(f"=== DONE mode={mode} in {(time.time() - t0)/60:.1f} min ===",
          flush=True)


if __name__ == '__main__':
    main()

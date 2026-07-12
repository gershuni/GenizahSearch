# -*- coding: utf-8 -*-
"""Preview deck v2 — smoke tier-B re-scored with the FINAL model + guards.

Fixes everything Hillel's review of preview v1 surfaced (2026-07-10 eve):
 1. PILOT P (no margin curves) floated junk — rows are RE-SCORED with
    p_calibration_final.json (margin bands + decoy-anchored singleton null).
 2. Canonical verse-chain leak (ויקרא רבה on צידוק הדין, Bavli Pesachim on
    Neilah — pages matching a work THROUGH shared Bible verses):
    **canonical-overlap guard** — a non-Bible row whose best span is covered
    >= GUARD_BIBLE_COV by the union of Bible-match spans on the same page is
    demoted out of the deck.
 3. Verse-chain competition miss (a competitor showing as several small
    spans, each failing the best-hull overlap test): **span-union margin** —
    margin/band recomputed against the UNION of each competitor work's
    spans, then P re-derived.
 4. Liturgy-agglomeration (ספר אהבה = siddur appendix, 2,321 witnesses):
    **rarity gate** — works with > RARITY_MAX strict-tier witnesses in the
    subcorpus are excluded (a discovery list is for few-witness works).
 5. Page-level cards read as duplicates (two pages of one MS):
    aggregation is per (manuscript, work) — best re-scored page shown.
 6. Bidi garbling of mixed Hebrew/Latin lines: shelfmarks/stats isolated
    with <bdi dir=ltr>.

v5 (2026-07-10, from the agent v4 re-grade — results/agent_deck_review_content.md):
the v4 batch guard verified hulls over the anchor extent PADDED +-30 on
both sides; flank mismatch alone drives density ~60/(run+65), so only
quotes >= ~68 contiguous clean letters could clear 0.45 — it caught 1/39
graded leaks and over-killed the correct Onkelos card (self-match vs the
guard's own Onkelos unit). v5 is the validated recipe: TRIMMED hulls (no
padding; window >= 18; dens <= 0.35; per-work UNION coverage >= 0.45;
claimed-work-in-guard-set exempt) + the RESTORED whole-slice Bible
partial_ratio guard at threshold 60 for non-Bible/Targum claims —
measured together ~87% leak catch at zero value-bearing kills on the
68-case graded ground truth.

This script is the PROTOTYPE of the real post-run deck builder (same guards
run over fullcorpus_v2 tomorrow).

Usage: python -X utf8 -u build_smoke_preview2.py
Out:   review/mapv2_smoke_preview_v5.html
"""
import html
import json
import os
import re
import sqlite3
from collections import Counter, defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein
from rapidfuzz.fuzz import partial_ratio_alignment

from engine_np import _gram_codes
from mapv2_track1_run import PModel, margin_band
from normalize import norm_stream
from track1_build_ref import HEADER_RE, JA_DIR, MAAGARIM
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\mapv2_smoke.db"
P_FINAL = PROBE + r"\data\p_calibration_final.json"
STAGING = PROBE + r"\refs_staging"
OUT = PROBE + r"\review\mapv2_smoke_preview_v6.html"

P_RE = re.compile(r'_P(\d+)_')
GUARD_BIBLE_COV = 0.70   # best span covered >= this by Bible spans -> demote
# ---- chain-aware canonical-rendering guard (v5, measured basis) ----
# Guard reference = Bible + Targum + statutory Liturgy + tafsir (60 works,
# 3.55M letters). Every surviving slice is batch-queried against it with
# track1 mechanics (K=5, band=20, min_anchors=2) but hulls are verified
# over the TRIMMED anchor extent — NO +-30 margin padding. v4's padded
# verification needed >= ~68 contiguous clean canonical letters to clear
# its 0.45 cutoff (flank mismatch ~60/(run+65)) and caught only 1/39
# graded leaks; trimmed hulls at the constants below catch 32/47 on the
# same 68-case ground truth, killing nothing value-bearing (agent v4
# re-grade, results/agent_deck_review_content.md).
GUARD_HULL_WMIN = 18     # min trimmed-hull window (query AND ref side);
                         # statutory/verse pieces >= ~18 letters verify,
                         # chance 2-anchor clusters mostly don't
GUARD_HULL_DMAX = 0.35   # trimmed-hull accept density (HTR noise headroom;
                         # 0.30-0.40 measured within 1-2 cases of 0.35)
GUARD_COVER_MIN = 0.45   # max per-guard-work UNION of verified hull query
                         # intervals / slice len >= this -> the evidence is
                         # canonical rendering -> demote. Rows whose CLAIMED
                         # work is itself in the guard set are exempt
                         # (fixes the v4 Onkelos self-match over-kill).
# Restored whole-slice Bible alignment guard (complementary to the trimmed
# hulls: catches noisy CONTIGUOUS quotes whose anchor runs shatter below
# WMIN — nusach variants, HTR-garbled verses). v3 ran this at 70; the
# graded sample showed the 60-70 band is ~100% leak for non-Targum claims
# (correct non-Targum cards all scored < 60), and 60 adds ~9 of the 15
# trimmed-hull misses -> combined ~87% catch. Bible/Targum-claimed rows
# are exempt (Targum legitimately tracks the Bible; Onkelos scored 60.8).
BIBLE_ALIGN_MIN = 60
K, BAND, MIN_ANCHORS, B_OFF = 5, 20, 2, 256  # track1_match mechanics
OVERLAP_FRAC = 0.5       # competitor span-union overlap threshold
RARITY_MAX = 60          # works with more strict-tier witnesses are not
                         # discovery material (liturgy/canon agglomerates)
PER_WORK_CAP = 3
STRATA = [
    ("P ≥ 0.8 — כמעט־ודאי", 0.8, 1.01, 25),
    ("P 0.5–0.8 — סביר", 0.5, 0.8, 20),
    ("P 0.2–0.5 — ספק", 0.2, 0.5, 15),
    ("P < 0.2 — קרוב לרעש (לניגוד)", 0.02, 0.2, 8),
]


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def merge_iv(ivs, gap=0):
    ivs = sorted(ivs)
    out = []
    for a, b in ivs:
        if out and a <= out[-1][1] + gap:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


def ov_len(a0, a1, iv):
    return sum(max(0, min(a1, b) - max(a0, a)) for a, b in iv)


def query_batch_trimmed(streams, ref_tuple, wmin=GUARD_HULL_WMIN,
                        dmax=GUARD_HULL_DMAX):
    """Batch query with TRIMMED hull verification (guard v5).

    Same vectorized anchor/diagonal-cluster pipeline as
    frag1_truncation.query_batch (K=5, band=20, min_anchors=2, adjacent-
    bucket merge), but each cluster is Levenshtein-verified over its anchor
    extent ONLY (no +-30 margin padding — the padding poisoned density for
    short canonical quotes; see module docstring). Returns
    results[qi] = [(work_idx, p0, p1), ...] of VERIFIED hull query
    intervals, for per-work UNION coverage.
    """
    seg_streams, seg_work, seg_off, codes_f, seg_f, pos_f = ref_tuple
    n = len(streams)
    results = [[] for _ in range(n)]
    parts_c, parts_p, parts_pos = [], [], []
    for qi, s in enumerate(streams):
        g = _gram_codes(s)
        if not len(g):
            continue
        parts_c.append(g.astype(np.uint32))
        parts_p.append(np.full(len(g), qi, np.uint32))
        parts_pos.append(np.arange(len(g), dtype=np.uint32))
    if not parts_c:
        return results
    pg_c = np.concatenate(parts_c)
    pg_p = np.concatenate(parts_p)
    pg_pos = np.concatenate(parts_pos)
    del parts_c, parts_p, parts_pos

    lo = np.searchsorted(codes_f, pg_c, 'left')
    hi = np.searchsorted(codes_f, pg_c, 'right')
    cnt = hi - lo
    sel = cnt > 0
    counts = cnt[sel]
    total = int(counts.sum())
    if not total:
        return results

    cum0 = np.cumsum(counts) - counts
    ref_idx = (np.repeat(lo[sel], counts)
               + (np.arange(total, dtype=np.int64) - np.repeat(cum0, counts)))
    page_r = np.repeat(pg_p[sel], counts).astype(np.uint64)
    ppos_r = np.repeat(pg_pos[sel], counts).astype(np.int64)
    seg_h = seg_f[ref_idx].astype(np.uint64)
    rpos_h = pos_f[ref_idx].astype(np.int64)
    del ref_idx
    bucket = ((ppos_r - rpos_h) // BAND + B_OFF).astype(np.uint64)
    key = (page_r << np.uint64(34)) | (seg_h << np.uint64(18)) | bucket
    order = np.argsort(key, kind='stable')
    key = key[order]
    ppos_r, rpos_h = ppos_r[order], rpos_h[order]
    del order, page_r, seg_h, bucket

    s2 = np.flatnonzero(np.r_[True, key[1:] != key[:-1]])
    cnt2 = np.diff(np.r_[s2, len(key)])
    minp = np.minimum.reduceat(ppos_r, s2)
    maxp = np.maximum.reduceat(ppos_r, s2)
    minr = np.minimum.reduceat(rpos_h, s2)
    maxr = np.maximum.reduceat(rpos_h, s2)
    k2 = key[s2]
    pair = k2 >> np.uint64(18)
    buck = (k2 & np.uint64((1 << 18) - 1)).astype(np.int64)
    new_seg = np.r_[True, (pair[1:] != pair[:-1]) | (buck[1:] - buck[:-1] > 1)]
    s3 = np.flatnonzero(new_seg)
    seg_cnt = np.add.reduceat(cnt2, s3).astype(np.int64)

    hit = seg_cnt >= MIN_ANCHORS
    c_pair = pair[s3][hit]
    c_minp = np.minimum.reduceat(minp, s3)[hit]
    c_maxp = np.maximum.reduceat(maxp, s3)[hit]
    c_minr = np.minimum.reduceat(minr, s3)[hit]
    c_maxr = np.maximum.reduceat(maxr, s3)[hit]
    for i in range(len(c_pair)):
        qi = int(c_pair[i] >> np.uint64(16))
        si = int(c_pair[i] & np.uint64(0xFFFF))
        sp, sr = streams[qi], seg_streams[si]
        # TRIMMED extents — the v5 change: no +-MARGIN padding
        p0 = int(c_minp[i])
        p1 = min(len(sp), int(c_maxp[i]) + K)
        r0 = int(c_minr[i])
        r1 = min(len(sr), int(c_maxr[i]) + K)
        if min(p1 - p0, r1 - r0) < wmin:
            continue
        alen = max(p1 - p0, r1 - r0)
        cutoff = int(dmax * alen) + 1
        dist = Levenshtein.distance(sp[p0:p1], sr[r0:r1], score_cutoff=cutoff)
        if dist > cutoff:
            continue
        results[qi].append((int(seg_work[si]), p0, p1))
    return results


def snippet(text, spans, pad=80):
    """(highlighted page-snippet html, matched stream slice) — the slice is
    used to locate the SAME passage in the reference edition."""
    stream, offs = norm_stream(text)
    p0, p1 = max(spans, key=lambda s: s[1] - s[0])[:2]
    p0 = max(0, min(int(p0), len(offs) - 1))
    p1 = min(int(p1), len(offs))
    if not len(offs) or p1 <= 0:
        return '', ''
    a = offs[p0]
    z = offs[p1 - 1] + 1
    htm = (f"<span class='ctx'>{html.escape(text[max(0, a - pad):a])}</span>"
           f"<mark>{html.escape(text[a:z][:600])}</mark>"
           f"<span class='ctx'>{html.escape(text[z:z + pad])}</span>")
    return htm, stream[p0:p1]


# ---------------------------------------------------------------------
# reference-side passage (Hillel: "show the two compared texts 1 vs
# another"). The rows store only page-side spans, so the edition passage
# is RECOVERED per displayed card: align the page's matched stream slice
# inside the work's stream, then map the aligned window back to the raw
# (readable) source text via norm_stream offsets — exactly the page-
# snippet mechanism, applied to the edition side.
# ---------------------------------------------------------------------

class RefText:
    def __init__(self):
        self.path_of = {}
        for fn in os.listdir(MAAGARIM):
            if fn.endswith('.txt'):
                base = fn.replace('.txt-OnlyText.txt', '')
                self.path_of['M:' + base.split('--')[-1]] = \
                    ('maagarim', os.path.join(MAAGARIM, fn))
        for fn in os.listdir(JA_DIR):
            if fn.endswith('.txt'):
                self.path_of['J:' + fn[:-4]] = \
                    ('ja', os.path.join(JA_DIR, fn))
        man = os.path.join(STAGING, 'manifest.json')
        if os.path.exists(man):
            for e in json.load(open(man, encoding='utf-8'))['entries']:
                self.path_of['REF2:' + e['key']] = \
                    ('ref2', os.path.join(STAGING, e['body_file']))
        self.cache = {}

    def _prepped(self, wid):
        if wid in self.cache:
            return self.cache[wid]
        kind_path = self.path_of.get(wid)
        out = None
        if kind_path:
            kind, path = kind_path
            raw = open('\\\\?\\' + path if kind == 'maagarim' else path,
                       encoding='utf-8', errors='replace').read()
            if kind == 'maagarim':
                raw = HEADER_RE.sub(' ', raw)
            stream, offs = norm_stream(raw)
            out = (raw, stream, offs)
        self.cache[wid] = out
        return out

    def passage(self, wid, page_slice, pad=40):
        """Readable edition passage matching the page's stream slice."""
        prep = self._prepped(wid)
        if not prep or len(page_slice) < 20:
            return ''
        raw, stream, offs = prep
        if len(stream) < len(page_slice):
            return ''
        res = partial_ratio_alignment(page_slice, stream, score_cutoff=30)
        if res is None:
            return ''
        w0 = max(0, res.dest_start - pad)
        w1 = min(len(stream), res.dest_end + pad)
        if w1 <= w0:
            return ''
        return html.escape(raw[offs[w0]:offs[w1 - 1] + 1][:800])


def main():
    import pickle
    pm = PModel(P_FINAL)
    print(f"final model loaded: margin bands {sorted(pm.margin)}, "
          f"singleton bins {sorted(pm.singleton_null)}")
    ref_all = pickle.load(open(PROBE + r"\data\ref_corpus_v2.pkl", 'rb'))
    # v6: rabbinic canon ADDED to the guard reference (Hillel directive +
    # agent v5 re-grade: 2 new-class leaks were works claimed via shared
    # Bavli dicta — רשב"ח מבוא התלמוד, מדרש אגור). The claimed-work-in-
    # guard-set exemption already protects legitimate Bavli/Mishnah claims
    # (a real Talmud fragment is exempt from its own tractate AND from
    # parallel-sugya demotion, since all tractates are guard works and the
    # exemption is id-based on the CLAIMED work — cross-tractate ambiguity
    # stays visible via the margin band instead).
    guard_works = [
        w for w in ref_all
        if w['cat'] in ('Bible', 'Targum', 'Liturgy', 'Mishnah', 'Bavli',
                        'Yerushalmi', 'Tosefta')
        or (w['cat'] == 'JA' and 'תפסיר' in (w['title'] or ''))]
    guard_ids = {w['id'] for w in guard_works}
    bible_stream = ''.join(w['stream'] for w in ref_all
                           if w['cat'] == 'Bible')
    del ref_all
    print(f"guard reference: {len(guard_works)} works "
          f"({sum(len(w['stream']) for w in guard_works):,} letters) — "
          f"cats {sorted({w['cat'] for w in guard_works})}; "
          f"bible stream {len(bible_stream):,}")
    guard_ref = build_ref_index(guard_works)[:6]
    con = sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro',
                          uri=True)

    # ---- all rows per page (both tiers) for guards + competition ----
    page_rows = defaultdict(list)   # pid -> [(wid, cat, spans, best_dens)]
    a_ms = defaultdict(set)
    for pid, sid, wid, cat, dens, spans_json in con.execute(
            "SELECT page_id, sys_id, work_id, cat, best_density, spans_json "
            "FROM track1_matches"):
        spans = [(int(s[0]), int(s[1])) for s in json.loads(spans_json)]
        page_rows[pid].append((wid, cat, spans, dens))
        a_ms[wid].add(sid)
    cands = con.execute("""
        SELECT page_id, sys_id, work_id, cat, author, title, best_alen,
               best_density, matched_letters, spans_json
        FROM track1_candidates WHERE flag=''""").fetchall()
    for pid, sid, wid, cat, author, title, alen, dens, letters, sj in cands:
        spans = [(int(s[0]), int(s[1])) for s in json.loads(sj)]
        page_rows[pid].append((wid, cat, spans, dens))
    print(f"pages with rows: {len(page_rows):,}; tier-B rows: {len(cands):,}")

    # ---- re-score every tier-B row with guards ----
    pstream_cache = {}

    def pstream(pid):
        if pid not in pstream_cache:
            t = con.execute("SELECT text FROM pages WHERE page_id=?",
                            (pid,)).fetchone()
            pstream_cache[pid] = norm_stream(t[0] or '')[0] if t else ''
        return pstream_cache[pid]

    kept = []       # (p_final, guard, row-tuple, band, margin)
    n_guard_bible = n_rare = 0
    stats = Counter()
    for pid, sid, wid, cat, author, title, alen, dens, letters, sj in cands:
        spans = [(int(s[0]), int(s[1])) for s in json.loads(sj)]
        b0, b1 = max(spans, key=lambda s: s[1] - s[0])
        blen = max(1, b1 - b0)
        others = [r for r in page_rows[pid] if r[0] != wid]
        # guard 3 first (cheapest): rarity — agglomerated works are not
        # discovery material
        if len(a_ms.get(wid, ())) > RARITY_MAX:
            n_rare += 1
            stats['guard_rarity'] += 1
            continue
        # guard 1: canonical(Bible) coverage of the best span
        if cat != 'Bible':
            bible_iv = merge_iv([(a, b) for _w, c, sp, _d in others
                                 if c == 'Bible' for a, b in sp])
            if bible_iv and ov_len(b0, b1, bible_iv) / blen >= GUARD_BIBLE_COV:
                n_guard_bible += 1
                stats['guard_bible'] += 1
                continue
        # guard 2: span-union margin
        comp_d = []
        for owid, _oc, osp, od in others:
            iv = merge_iv(osp)
            if ov_len(b0, b1, iv) >= OVERLAP_FRAC * blen:
                comp_d.append(od)
        margin = (min(comp_d) - dens) if comp_d else None
        band = ('not_best' if (margin is not None and margin <= 0)
                else margin_band(margin if margin is not None else 1.0,
                                 len(comp_d)))
        if band == 'not_best':
            stats['not_best'] += 1
            continue
        p = pm.p(alen, dens, band)
        stats[f'band_{band}'] += 1
        kept.append((p, pid, sid, wid, cat, author, title, alen, dens,
                     margin, len(comp_d), band, letters, spans))
        if len(kept) % 10000 == 0:
            print(f"  cheap-guard survivors {len(kept):,} ...", flush=True)
    print(f"cheap guards: bible-cover {n_guard_bible:,}, rarity {n_rare:,}, "
          f"stats {dict(stats)}; survivors {len(kept):,}", flush=True)

    # ---- v5 guard stage 1: chain-aware canonical-rendering guard
    # (batch, TRIMMED hulls). Bible rows are exempt (a Bible page matching
    # מקרא is a Bible witness, not a leak), and so are rows whose CLAIMED
    # work is itself in the guard set (Targum/tafsir/liturgy-unit claims
    # would self-match their own edition — the v4 Onkelos over-kill).
    # Everything else must NOT be explainable as canonical rendering
    # (verse chains, targum/tafsir renderings, statutory liturgy). ----
    test_idx = [i for i, r in enumerate(kept)
                if r[4] != 'Bible' and r[3] not in guard_ids]
    slices = []
    for i in test_idx:
        r = kept[i]
        sp = max(r[13], key=lambda s: s[1] - s[0])
        slices.append(pstream(r[1])[sp[0]:sp[1]])
    print(f"guard query: {len(slices):,} slices vs guard reference "
          f"(trimmed hulls) ...", flush=True)
    results = query_batch_trimmed(slices, guard_ref)
    drop = set()
    for k, (i, hulls) in enumerate(zip(test_idx, results)):
        per_work = defaultdict(list)
        for wi_g, hp0, hp1 in hulls:
            per_work[wi_g].append((hp0, hp1))
        slen = max(1, len(slices[k]))
        for ivs in per_work.values():
            cov = sum(b - a for a, b in merge_iv(ivs)) / slen
            if cov >= GUARD_COVER_MIN:
                drop.add(i)
                break
    stats['guard_canonical_rendering'] = len(drop)
    kept = [r for i, r in enumerate(kept) if i not in drop]
    print(f"canonical-rendering guard (trimmed) dropped {len(drop):,}; "
          f"kept {len(kept):,}", flush=True)

    # ---- v5 guard stage 2: restored whole-slice Bible alignment at 60.
    # Complementary: catches noisy CONTIGUOUS quotes whose anchor runs
    # shatter below GUARD_HULL_WMIN (nusach variants, HTR-garbled verses).
    # Bible/Targum claims exempt (Targum legitimately tracks the Bible). ----
    kept2 = []
    n_verse = 0
    for r in kept:
        if r[4] not in ('Bible', 'Targum'):
            sp = max(r[13], key=lambda s: s[1] - s[0])
            sl = pstream(r[1])[sp[0]:sp[1]]
            if len(sl) >= 30:
                res = partial_ratio_alignment(sl, bible_stream,
                                              score_cutoff=BIBLE_ALIGN_MIN)
                if res is not None and res.score >= BIBLE_ALIGN_MIN:
                    n_verse += 1
                    continue
        kept2.append(r)
    kept = kept2
    stats['guard_verse_align60'] = n_verse
    print(f"whole-slice Bible-align guard (>= {BIBLE_ALIGN_MIN}) dropped "
          f"{n_verse:,}; kept {len(kept):,}", flush=True)

    # ---- aggregate per (manuscript, work): best re-scored page ----
    best = {}
    for row in kept:
        key = (row[2], row[3])
        if key not in best or row[0] > best[key][0]:
            best[key] = row
    ms_rows = sorted(best.values(), key=lambda r: -r[0])
    print(f"(manuscript, work) rows after aggregation: {len(ms_rows):,}")

    # shelfmarks
    import csv
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for r in rd:
            if len(r) >= 4 and r[0]:
                v = [x.strip() for x in (r[2] or '').split('|') if x.strip()]
                title_nli = r[7].strip() if len(r) >= 8 else ''
                meta[r[0]] = (v[0] if v else r[0], r[3].strip() or '?',
                              title_nli)

    band_he = {
        'singleton': 'התאמה בודדת',
        'm_ge_010': 'מוביל בפער גדול',
        'm_003_010': 'מוביל בפער בינוני',
        'm_0_003': 'מוביל בפער קטן',
    }
    reftext = RefText()
    sections = []
    for label, lo, hi, n_cards in STRATA:
        per_work = Counter()
        cards = []
        for (p, pid, sid, wid, cat, author, title, alen, dens, margin,
             ncomp, band, letters, spans) in ms_rows:
            if not (lo <= p < hi) or per_work[wid] >= PER_WORK_CAP:
                continue
            trow = con.execute("SELECT text FROM pages WHERE page_id=?",
                               (pid,)).fetchone()
            if not trow or not trow[0]:
                continue
            per_work[wid] += 1
            sm, lib, title_nli = meta.get(sid, (sid, '?', ''))
            url = (f"https://genizahsearch.com/browse?sys_id={sid}"
                   f"&page={pnum(pid)}")
            name = f"{author} — {title}" if author else title
            n_wit = len(a_ms.get(wid, ()))
            page_htm, page_slice = snippet(trow[0], spans)
            ref_htm = reftext.passage(wid, page_slice)
            ref_pane = (f"<div class='pane'><div class='lbl'>המקבילה "
                        f"במהדורה:</div><div class='ev ref'>{ref_htm}"
                        f"</div></div>" if ref_htm else
                        "<div class='pane'><div class='lbl'>(המקבילה "
                        "במהדורה לא אותרה לתצוגה)</div></div>")
            nli_t = (f" <span class='nli'>· קטלוג NLI: "
                     f"{html.escape(title_nli[:60])}</span>"
                     if title_nli else "")
            cards.append(f"""
<div class='card'>
 <div class='head'>
  <span class='p'>P {p:.2f}</span>
  <a href='{url}' target='_blank'><bdi dir='ltr'><b>{html.escape(sm)}</b>
  ({lib})</bdi></a>
  <span class='work'>[{cat}] {html.escape(name[:75])}</span>{nli_t}
 </div>
 <div class='stats'>אורך התאמה <bdi dir='ltr'>{alen}</bdi> אות · מרחק
  <bdi dir='ltr'>{dens:.2f}</bdi> · {band_he.get(band, band)}{
  f" (פער {margin:.2f})" if margin is not None and ncomp else ""} ·
  עדים מחמירים לחיבור: <bdi dir='ltr'>{n_wit}</bdi></div>
 <div class='panes'>
  <div class='pane'><div class='lbl'>קטע העמוד (גניזה, מודגש =
   ההתאמה):</div><div class='ev'>{page_htm}</div></div>
  {ref_pane}
 </div>
</div>""")
            if len(cards) >= n_cards:
                break
        sections.append((label, cards))
        print(f"  {label}: {len(cards)} cards")

    hist = Counter(min(9, int(r[0] * 10)) / 10 for r in ms_rows)
    hist_html = " · ".join(f"{k:.1f}: {v:,}" for k, v in sorted(hist.items()))
    n_a = con.execute("SELECT COUNT(*) FROM track1_matches").fetchone()[0]
    body = []
    for label, cards in sections:
        body.append(f"<h2>{label}</h2>")
        body.extend(cards or ["<p>(אין כרטיסים ברצועה זו)</p>"])
    doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'><head>
<meta charset='utf-8'><title>MAPV2 preview v5 — ציון סופי + מסננים</title>
<style>
 body{{font-family:Segoe UI,Arial;max-width:1100px;margin:20px auto;
 padding:0 12px;background:#fafaf7;color:#222}}
 .card{{background:#fff;border:1px solid #ddd;border-radius:8px;
 margin:10px 0;padding:8px 14px}}
 .head{{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}}
 .p{{background:#1a5da6;color:#fff;border-radius:6px;padding:2px 8px;
 font-weight:bold}} .work{{color:#444}}
 .stats{{font-size:12.5px;color:#666;margin:4px 0}}
 .ev{{direction:rtl;text-align:right;font-size:14.5px;line-height:1.7;
 white-space:pre-wrap;background:#fcfcf9;border:1px solid #eee;
 border-radius:6px;padding:6px 8px}}
 .ev mark{{background:#ffe58a}} .ev .ctx{{color:#aaa}}
 .ev.ref{{background:#f4f9f4;border-color:#d8e8d8}}
 .lbl{{font-size:12px;color:#777;margin-top:6px}}
 .nli{{font-size:12.5px;color:#8a6d3b}}
 .panes{{display:flex;gap:12px;flex-wrap:wrap}}
 .pane{{flex:1 1 320px;min-width:0}}
 h2{{border-bottom:2px solid #1a5da6;padding-bottom:4px;margin-top:28px}}
 .note{{background:#eef4fb;border:1px solid #cfe0f5;border-radius:8px;
 padding:10px 14px;font-size:14px}}
</style></head><body>
<h1>תצוגה מקדימה v5 — ציון סופי + מסנני v5 (ליטורגיה)</h1>
<div class='note'>
<b>מה במסנן v5</b> (מכויל על 68 מקרים שדורגו ידנית בביקורת הסוכן):
(1) הציונים חושבו מחדש עם הכיול הסופי; (2) <b>מסנן עיבודים קנוניים —
גבעות חתוכות</b>: כל קטע ראיה נבדק מול רפרנס קנוני של {len(guard_works)}
חיבורים (מקרא + תרגומים + תפסיר + יחידות תפילת קבע) באימות ללא ריפוד־שוליים
(חלון ≥ {GUARD_HULL_WMIN}, צפיפות ≤ {GUARD_HULL_DMAX}); כיסוי־איחוד
לחיבור קנוני אחד ≥ {int(GUARD_COVER_MIN*100)}% ⇐ הסרה. גרסת v4
(אימות מרופד) תפסה רק 1/39 דליפות מדורגות — הגרסה החתוכה תופסת 32/47,
כולל דף הסליחות תה' מד שיוחס ל"לאן?" של פיירברג ב־P=1.00; (3) <b>הוחזר
המסנן הגלובלי מול נוסח המקרא</b> בסף {BIBLE_ALIGN_MIN} (במקום 70) לכל
מועמד שאינו מקרא/תרגום — יחד ≈87% תפיסת דליפות במדגם המדורג; חיבור
נטען שנמצא בעצמו ברפרנס הקנוני פטור מהמסנן (תיקון הסרת־היתר של
אונקלוס); (4) הוסר כל מועמד שקטעו מכוסה בפסוקי מקרא שזוהו באותו עמוד;
(5) חיבורים עם יותר מ־{RARITY_MAX} עדים מחמירים הוצאו; (6) כרטיס אחד
לכל (כתב־יד, חיבור); (7) שני הטקסטים זה לצד זה + כותרת קטלוג NLI.
<b>חדש ב־v6:</b> גם משנה/בבלי/ירושלמי/תוספתא נכנסו לרפרנס־המסנן (הערתך:
ציטוטי חז"ל שכיחים לא פחות מפסוקים) — חיבור שנתלה במימרא תלמודית משותפת
(רשב"ח מבוא התלמוד, מדרש אגור) יוסר; טענות אמיתיות על בבלי/משנה עצמם
פטורות מהמסנן. <b>שיירי רעש ידועים</b> (יטופלו בהמשך): נוסחי קבע שחסרים
ברפרנס — כל חמירא, קדיש, המפיל, ברכות נישואין — יתווספו כיחידות; נוסחי
קבע בגרסת־נוסח שונה שמנתצת את רצפי העוגנים; שרשראות של קטעים קצרצרים
(&lt;18 אות); ודו־כיווניות ציטוט (מי מצטט את מי).
<br><b>המספרים:</b> מתוך 657,205 שורות גולמיות נשארו {len(ms_rows):,}
(כתב־יד, חיבור) אחרי הציון הסופי והמסננים. הרובד המחמיר: {n_a:,} זיהויים
(ללא שינוי). התפלגות P אחרי הכל: {hist_html}
</div>
{''.join(body)}
</body></html>"""
    open(OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT}")
    con.close()


if __name__ == '__main__':
    main()

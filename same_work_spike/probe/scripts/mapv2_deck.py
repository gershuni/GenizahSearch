# -*- coding: utf-8 -*-
"""MAPV2-5 — REAL full-corpus discovery deck + blinded grading deck + report.

Successor of build_smoke_preview2.py (v6 recipe), hardened per the Codex
deck-guard review (results/overnight/codex_deck_guard_r1.log):
  #1 BLOCKER rarity gate derived at runtime as a quantile of the tier-A
     witness-count distribution (bounded), recorded in the report.
  #2 HIGH    streaming page-grouped pass (no global page_rows dict), LRU-free
     locality (ORDER BY page_id), canonical guard + Bible-align run in
     bounded chunks with NDJSON checkpoints (resume-safe).
  #3 HIGH    Bible-coverage gate measured on the row's span UNION.
  #4 HIGH    flag!='' rows (merge_page etc.) participate in page CONTEXT
     (Bible spans, competition) but are excluded from display.
  #5 MED     (sys_id, work_id) pairs already in live tier A are dropped from
     the discovery stream (counted; a discovery deck shows NEW witnesses).
  #6 MED     span-union margin recompute uses per-span densities from
     spans_json (competitor density = best density among its spans that
     overlap the candidate's best span; union-overlap >= OVERLAP_FRAC gates).
  #7 LOW     CLI args; no hardcoded corpus labels/counts; pnum() failure ->
     no page param.

Also folds the graded-deck review items: (ms, work) aggregation, side-by-side
panes with edition passage + NLI title, bdi isolation, P display cap, singleton
cap/relabel (data/deck_params.json overridable), flank-contrast chip
(continues/island/edge vs the claimed work's edition — evidence only, never a
demotion), blinded stratified deck for unbiased grading.

Run ONLY when the overnight chain is done with the DB (chain state json).
Usage: python -X utf8 -u mapv2_deck.py [--db PATH] [--outdir PATH]
       [--label TEXT] [--fresh] [--allow-noshadow]
"""
import argparse
import html
import json
import os
import random
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict

from rapidfuzz.distance import Levenshtein
from rapidfuzz.fuzz import partial_ratio_alignment

from build_smoke_preview2 import (
    BIBLE_ALIGN_MIN, GUARD_BIBLE_COV, GUARD_COVER_MIN, OVERLAP_FRAC,
    RefText, merge_iv, ov_len, query_batch_trimmed, snippet)
from mapv2_track1_run import PModel, margin_band
from normalize import norm_stream
from track1_match import build_ref_index

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
P_FINAL = PROBE + r"\data\p_calibration_final.json"
# final2 = alen-binned refit (results/cal1_final2_refit.md): holdout mean
# |gap| 0.161 -> 0.082; preferred when present + Codex-approved
P_FINAL2 = PROBE + r"\data\p_calibration_final2.json"
PARAMS_PATH = PROBE + r"\data\deck_params.json"
P_RE = re.compile(r'_P(\d+)_')

DEFAULT_PARAMS = {
    # rarity gate: works whose live tier-A witness count exceeds
    # quantile(counts, q) (bounded) are agglomerates, not discovery material
    "rarity_quantile": 0.95,
    "rarity_bounds": [30, 400],
    # singleton-band P handling (stats-audit calibrated; deck_params.json
    # overrides): cap P for singleton rows below alen thresholds
    # cap sits BELOW the 0.8 top-stratum threshold (Codex R2 #2): a capped
    # short singleton must not land in the "almost certain" section
    "singleton_caps": [[80, 0.799]],   # [max_alen, cap]
    # singleton rows at chance-level density (decoy p50 ~0.52) -> weak
    "singleton_dens_floor": [0.52, 0.30],
    # display honesty (stats audit: mid-range decimals off by up to +-0.4):
    "hide_decimal_singleton_below_alen": 80,
    "coarse_grade_bands": ["m_003_010", "m_0_003"],
    "coarse_grade_window": [0.2, 0.8],
    "display_p_cap": 0.99,
    "per_work_cap": 3,
    # v6-verify agent: one commentary codex held 2 top-25 slots -> cap cards
    # per manuscript per section
    "per_ms_cap": 2,
    # guard v7 (v6-verify agent: aggadic midrash sharing leaks below the top
    # band): rabbinic-genre works attested ABOVE the rarity cutoff join the
    # guard reference — they are rarity-excluded from discovery anyway, and
    # they are exactly the shared quotation sources (Tanhuma, Rabbot...).
    # Whole-guard exemption semantics preserved for claims ON guard works.
    "guard_midrash_by_attestation": True,
    # v7-verify agent: 6/7 remaining top-band leaks were CANONICAL-CLAIM
    # EXEMPTION leaks (a siddur page quoting פיטום הקטורת claimed as Bavli
    # Keritot). The census's own coverage semantics applied to tier-B
    # canonical claims: a claim on a guard-cat work must cover the page at
    # testimony grade, else it is a citation, not a witness.
    "canon_claim_min_coverage": 0.45,
    "guard_chunk": 4000,
    "strata": [
        ["P ≥ 0.8 — כמעט־ודאי", 0.8, 1.01, 40],
        ["P 0.5–0.8 — סביר", 0.5, 0.8, 25],
        ["P 0.2–0.5 — ספק", 0.2, 0.5, 15],
        ["P < 0.2 — קרוב לרעש (לניגוד)", 0.02, 0.2, 8],
    ],
    "blinded_n": 60,
}

FLANK_LEN, FLANK_MIN = 150, 60


def load_params():
    p = dict(DEFAULT_PARAMS)
    if os.path.exists(PARAMS_PATH):
        p.update(json.load(open(PARAMS_PATH, encoding='utf-8')))
        print(f"params overridden from {PARAMS_PATH}")
    return p


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else None


def quantile(sorted_vals, q):
    if not sorted_vals:
        return 0
    i = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
    return sorted_vals[i]


def apply_singleton_cap(p, band, alen, dens, prm):
    if band != 'singleton':
        return p
    for max_alen, cap in prm['singleton_caps']:
        if alen <= max_alen:
            p = min(p, cap)
            break
    d_thr, d_cap = prm.get('singleton_dens_floor', (1.1, 1.0))
    if dens >= d_thr:
        p = min(p, d_cap)
    return p


def p_chip(p_show, band, alen, prm):
    """Honest P display (stats audit): no decimal where the model is not
    calibrated — singleton short rows show the band, small-margin mid-range
    rows show the range bucket."""
    if band == 'singleton' and alen < prm['hide_decimal_singleton_below_alen']:
        return "התאמה בודדת"
    lo, hi = prm['coarse_grade_window']
    if band in prm['coarse_grade_bands'] and lo <= p_show < hi:
        return f"P {'0.5–0.8' if p_show >= 0.5 else '0.2–0.5'}"
    return f"P {p_show:.2f}"


def flank_class(pstream, p0, p1, wstream):
    """Hillel's flank test vs the claimed work's edition stream.

    Locates the matched slice in the work stream, then compares equal-length
    flanks on both sides. Returns (class, best_dist|None). Evidence only."""
    sl = pstream[p0:p1]
    if len(sl) < 25 or len(wstream) < len(sl):
        return 'edge', None
    res = partial_ratio_alignment(sl, wstream, score_cutoff=30)
    if res is None:
        return 'edge', None
    d0, d1 = res.dest_start, res.dest_end
    best = None
    L = min(FLANK_LEN, p0, d0)
    if L >= FLANK_MIN:
        best = Levenshtein.normalized_distance(
            pstream[p0 - L:p0], wstream[d0 - L:d0])
    L = min(FLANK_LEN, len(pstream) - p1, len(wstream) - d1)
    if L >= FLANK_MIN:
        d = Levenshtein.normalized_distance(
            pstream[p1:p1 + L], wstream[d1:d1 + L])
        best = d if best is None else min(best, d)
    if best is None:
        return 'edge', None
    if best <= 0.52:
        return 'continuation', best
    return ('ambig', best) if best <= 0.58 else ('island', best)


# v7-verify note: 'continuation' fired 0/88 (alignment-boundary slop inflates
# flank distances) — the chip is an INDICATION, never a verdict; wording +
# color softened accordingly.
FLANK_HE = {
    'continuation': ('ההקשר ממשיך במהדורה ✓', '#2e7d32'),
    'island': ('השוליים שונים — אפשרי ציטוט (אינדיקציה בלבד)', '#b26a00'),
    'ambig': ('הקשר גבולי', '#8a6d3b'),
    'edge': ('קצה קטע — אין הקשר לבדיקה', '#777'),
}
CANON_CATS = ('Bible', 'Targum', 'Liturgy', 'Mishnah', 'Bavli',
              'Yerushalmi', 'Tosefta')
# citation-formula demoter (full-deck grading: 9+ top-band leaks were
# anthologies/commentaries QUOTING the claimed work with an explicit
# citation formula right before the matched span — one literally reads
# "וגרסי' במד' תהלים". This mechanizes Hillel's read-the-introduction test.
# HIGH-PRECISION markers only (verbs of quotation/naming, not words that
# appear freely inside running rabbinic prose).
CITE_MARKERS = ('וגרסי', 'גרסינן', 'דגרסינן', 'כדגרסינן', 'כדאיתא',
                'דאיתא', 'כדתנן', 'כדתניא', 'ובמדרש', 'שאמרו במדרש',
                'וזה לשון', 'וז"ל', 'לשון המדרש', 'ואמרו ז"ל', 'אמרו ז"ל',
                'כמו שאמרו', 'וכן אמרו', 'כמו שכתוב ב', 'כדאמרינן',
                'מוכח התם', 'כדמוכח', 'איתא התם',
                # blind-grading additions (Hillel): #11 ואמרו במדרש; #23 the
                # Judeo-Arabic citation family לקו' (=לקולה) / כמא קאל; the
                # HTR-garbled וגדסי' (=וגרסי')
                'ואמרו במדרש', 'אמרו במדרש', 'וגדסי', 'כמא קאל', 'לקולה',
                "לקו'", 'לקו׳', 'קאל אבו')
CITE_WINDOW = 38   # raw chars BEFORE the matched span's raw start
# v9 re-grade: in the real leak cards the formula sits INSIDE the highlight
# head ("כד מוכח התם פר' התקבל" BEGINS the match) — scan the span head too
CITE_HEAD = 30     # raw chars INTO the matched span
BAND_HE = {
    'singleton': 'התאמה בודדת',
    'm_ge_010': 'מוביל בפער גדול',
    'm_003_010': 'מוביל בפער בינוני',
    # honesty chip (full-deck grading: geonic Talmud-digest family cards sit
    # at margins 0.00-0.02 — the leading work is one of several close
    # parallels excerpting the same passage)
    'm_0_003': 'מוביל בפער זעיר ⚠ ייתכן חיבור מקביל קרוב',
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--db', default=PROBE + r"\data\fullcorpus_v2.db")
    ap.add_argument('--outdir', default=PROBE + r"\review")
    ap.add_argument('--label', default='הקורפוס המלא')
    ap.add_argument('--fresh', action='store_true',
                    help='ignore existing guard checkpoints')
    ap.add_argument('--allow-noshadow', action='store_true',
                    help='smoke validation: accept a DB without shadowed_by '
                         '(all tier-A rows treated as live)')
    ap.add_argument('--pmodel', default=None,
                    help='P model json (default: final2 if present, else '
                         'final)')
    args = ap.parse_args()
    prm = load_params()
    t0 = time.time()

    os.makedirs(args.outdir, exist_ok=True)
    ckpt_path = os.path.join(args.outdir, 'mapv2_deck_guard_ckpt.ndjson')
    out_main = os.path.join(args.outdir, 'mapv2_discovery_deck.html')
    out_blind = os.path.join(args.outdir, 'mapv2_blinded_deck.html')
    out_key = os.path.join(args.outdir, 'mapv2_blinded_key.json')
    out_report = os.path.join(args.outdir, 'mapv2_deck_report.md')
    if args.fresh and os.path.exists(ckpt_path):
        os.remove(ckpt_path)

    pmodel_path = args.pmodel or (
        P_FINAL2 if os.path.exists(P_FINAL2) else P_FINAL)
    pm = PModel(pmodel_path)
    print(f"P model: {pmodel_path}", flush=True)
    con = sqlite3.connect('file:' + args.db.replace('\\', '/') + '?mode=ro',
                          uri=True)
    pcols = {r[1] for r in con.execute("PRAGMA table_info(pages)")}
    prov_expr = "provenance" if 'provenance' in pcols else "'htr'"
    # substitution-risk exclude-list (MAPV2-8 F-SQL-1 recompute): pages where
    # a partial/divergent human transcription displaced fuller HTR — their
    # text is gappy, so candidate evidence on them is unreliable
    risk_pages = set()
    risk_p = PROBE + r"\data\substitution_risk_pages.json"
    if os.path.exists(risk_p):
        risk_pages = set(
            json.load(open(risk_p, encoding='utf-8'))['page_ids'])
        print(f"substitution-risk exclude-list: {len(risk_pages)} pages",
              flush=True)
    # MAPV2-10: NLI microfilm title-card / copyright-stamp pages — pure
    # film-leader boilerplate, never manuscript content; never map them
    mf_pages = set()
    mf_p = PROBE + r"\data\microfilm_title_pages.json"
    if os.path.exists(mf_p):
        mf_pages = set(json.load(open(mf_p, encoding='utf-8'))['pages'])
        print(f"microfilm title-card exclude-list: {len(mf_pages):,} pages",
              flush=True)
    cols = {r[1] for r in con.execute("PRAGMA table_info(track1_matches)")}
    has_shadow = 'shadowed_by' in cols
    if not has_shadow and not args.allow_noshadow:
        sys.exit("ABORT: track1_matches has no shadowed_by column — "
                 "run track1_shadow first (chain step 3).")

    # ---- tier-A maps (small table) ----
    a_ms = defaultdict(set)        # live witnesses per work
    a_pairs = set()                # live (sys, work) known pairs
    sh_expr = "shadowed_by" if has_shadow else "NULL"
    for wid, sid, sh in con.execute(
            f"SELECT work_id, sys_id, {sh_expr} FROM track1_matches"):
        if sh is None:
            a_ms[wid].add(sid)
            a_pairs.add((sid, wid))
    counts = sorted(len(v) for v in a_ms.values())
    lo_b, hi_b = prm['rarity_bounds']
    rarity_max = max(lo_b, min(hi_b, quantile(counts, prm['rarity_quantile'])))
    print(f"tier A: {sum(counts):,} live (ms,work) links, {len(a_ms):,} works; "
          f"witness-count q{int(prm['rarity_quantile']*100)}="
          f"{quantile(counts, prm['rarity_quantile'])} -> rarity_max="
          f"{rarity_max}", flush=True)

    # ---- streaming page-grouped cheap-guard pass ----
    # Both cursors ordered by page_id; context = tier-A rows (incl. shadowed)
    # + ALL tier-B rows (incl. flagged/not_best); display = flag='' rows.
    cur_a = con.execute(
        "SELECT page_id, work_id, cat, best_density, spans_json "
        "FROM track1_matches ORDER BY page_id")
    cur_b = con.execute(
        "SELECT page_id, sys_id, work_id, cat, author, title, best_alen, "
        "best_density, margin, n_competitors, margin_band, p_same_work, "
        "flag, matched_letters, spans_json "
        "FROM track1_candidates ORDER BY page_id")
    row_a = next(cur_a, None)
    row_b = next(cur_b, None)
    stats = Counter()
    survivors = []   # rows past cheap guards, pre canonical guard
    n_pages = 0

    def parse_spans(sj):
        return [(int(s[0]), int(s[1]), float(s[2]) if len(s) > 2 else 0.0)
                for s in json.loads(sj)]

    while row_a is not None or row_b is not None:
        pids = [r[0] for r in (row_a, row_b) if r is not None]
        pid = min(pids)
        ctx = []          # (wid, cat, spans, best_dens) all rows on page
        disp = []         # displayable tier-B rows
        while row_a is not None and row_a[0] == pid:
            ctx.append((row_a[1], row_a[2], parse_spans(row_a[4]), row_a[3]))
            row_a = next(cur_a, None)
        while row_b is not None and row_b[0] == pid:
            (_, sid, wid, cat, author, title, alen, dens, margin, ncomp,
             band, p, flag, letters, sj) = row_b
            spans = parse_spans(sj)
            ctx.append((wid, cat, spans, dens))
            if flag == '' and band != 'not_best':
                disp.append((sid, wid, cat, author, title, alen, dens,
                             band, p, letters, spans))
            else:
                stats['skip_flag' if flag else 'skip_not_best'] += 1
            row_b = next(cur_b, None)
        n_pages += 1

        if not disp:
            continue
        if pid in risk_pages:
            stats['guard_substitution_risk'] += len(disp)
            continue
        if pid in mf_pages:
            stats['guard_microfilm'] += len(disp)
            continue
        bible_iv = merge_iv([(a, b) for _w, c, sp, _d in ctx if c == 'Bible'
                             for a, b, *_ in sp])
        plen = None   # page stream length, fetched once per page on demand
        ptext, pstream_, poffs_ = '', None, None
        for (sid, wid, cat, author, title, alen, dens, band, p,
             letters, spans) in disp:
            if len(a_ms.get(wid, ())) > rarity_max:
                stats['guard_rarity'] += 1
                continue
            if (sid, wid) in a_pairs:
                stats['known_tierA_pair'] += 1
                continue
            row_iv = merge_iv([(a, b) for a, b, *_ in spans])
            row_len = max(1, sum(b - a for a, b in row_iv))
            # canonical-claim citation gate (v7-verify): a tier-B claim ON a
            # canonical work must cover the page at testimony grade — a
            # siddur page quoting פיטום הקטורת is not a Bavli witness. The
            # guard exemption protects these claims from the canonical
            # guard, so coverage is their only citation test. Fragment-safe:
            # coverage is relative to the PAGE, so a small torn Bavli scrap
            # fully covered by its match passes.
            if plen is None:
                trow_ = con.execute("SELECT text FROM pages WHERE page_id=?",
                                    (pid,)).fetchone()
                ptext = (trow_[0] or '') if trow_ else ''
                pstream_, poffs_ = norm_stream(ptext)
                plen = max(1, len(pstream_))
            if cat in CANON_CATS or (cat == 'JA' and 'תפסיר' in (title or '')):
                if row_len / plen < prm['canon_claim_min_coverage']:
                    stats['guard_canon_citation'] += 1
                    continue
            # Codex #3: Bible coverage over the row's span UNION
            if cat != 'Bible' and bible_iv and \
                    sum(ov_len(a, b, bible_iv) for a, b in row_iv) / row_len \
                    >= GUARD_BIBLE_COV:
                stats['guard_bible'] += 1
                continue
            # Codex #6: span-union margin, competitor density from
            # OVERLAPPING competitor spans only
            b0, b1, _ = max(spans, key=lambda s: s[1] - s[0])
            blen = max(1, b1 - b0)
            comp_d = []
            for owid, _oc, osp, od in ctx:
                if owid == wid:
                    continue
                iv = merge_iv([(a, b) for a, b, *_ in osp])
                if ov_len(b0, b1, iv) >= OVERLAP_FRAC * blen:
                    over = [sd for a, b, sd in osp
                            if min(b1, b) - max(b0, a) > 0]
                    comp_d.append(min(over) if over else od)
            margin2 = (min(comp_d) - dens) if comp_d else None
            band2 = ('not_best' if (margin2 is not None and margin2 <= 0)
                     else margin_band(margin2 if margin2 is not None else 1.0,
                                      len(comp_d)))
            if band2 == 'not_best':
                stats['not_best_union'] += 1
                continue
            p2 = pm.p(alen, dens, band2)
            p2 = apply_singleton_cap(p2, band2, alen, dens, prm)
            stats[f'band_{band2}'] += 1
            survivors.append((p2, pid, sid, wid, cat, author or '', title or '',
                              alen, dens, margin2, len(comp_d), band2,
                              letters, [(a, b) for a, b, *_ in spans]))
        if n_pages % 50000 == 0:
            print(f"  pages {n_pages:,} · survivors {len(survivors):,}",
                  flush=True)
    print(f"cheap guards done: {n_pages:,} pages, survivors "
          f"{len(survivors):,}; stats {dict(stats)} "
          f"({time.time()-t0:.0f}s)", flush=True)

    # ---- canonical-rendering guard (chunked, checkpointed) ----
    import pickle
    ref_all = pickle.load(open(PROBE + r"\data\ref_corpus_v2.pkl", 'rb'))
    # known-sibling suppression (v7-verify: excluding a tier-A-known Onkelos
    # pair let Pseudo-Jonathan surface on the same manuscript at P 0.99):
    # a (ms, work) claim whose work shares a VERSION GROUP with a work the
    # ms already witnesses in tier A is a known-sibling, not a discovery.
    vg_of = {w['id']: w.get('vgroup') for w in ref_all if w.get('vgroup')}
    known_vg = {(sid, vg_of[wid]) for sid, wid in a_pairs if wid in vg_of}
    if known_vg:
        before = len(survivors)
        survivors = [r for r in survivors
                     if (r[2], vg_of.get(r[3])) not in known_vg
                     or vg_of.get(r[3]) is None]
        stats['known_sibling_vgroup'] = before - len(survivors)
        print(f"known-sibling (version-group) suppression: dropped "
              f"{before - len(survivors):,}", flush=True)
    # Targum sibling gate (full-deck grading: 4 top-band leaks were
    # Pseudo-Jonathan claims on manuscripts already tier-A-known as Onkelos
    # of the SAME BOOK — vgroups don't bind cross-work siblings). A ms with
    # a live tier-A Targum row on a book is not a discovery for ANOTHER
    # Targum of that book.
    targum_book = {w['id']: w['id'].rsplit('_', 1)[-1] for w in ref_all
                   if w['cat'] == 'Targum'}
    known_tg = {(sid, targum_book[wid]) for sid, wid in a_pairs
                if wid in targum_book}
    if known_tg:
        before = len(survivors)
        survivors = [r for r in survivors
                     if r[4] != 'Targum'
                     or (r[2], targum_book.get(r[3])) not in known_tg]
        stats['known_sibling_targum'] = before - len(survivors)
        print(f"Targum same-book sibling suppression: dropped "
              f"{before - len(survivors):,}", flush=True)
    # era gate (full-deck grading: Gottlober's 1880s memoirs surfaced at
    # P 0.80): a work dated to the print era is not Genizah discovery
    # material — a match to it is formulaic/late-deposit noise.
    modern_ids = {w['id'] for w in ref_all
                  if re.search(r'\b(1[5-9]\d\d|20\d\d)\b',
                               w.get('date') or '')}
    before = len(survivors)
    survivors = [r for r in survivors if r[3] not in modern_ids]
    stats['guard_modern_era'] = before - len(survivors)
    print(f"modern-era (>=1500) work suppression: dropped "
          f"{before - len(survivors):,} (of {len(modern_ids)} modern works)",
          flush=True)
    # citation-formula demoter v11 (MAPV2-A + blind grading): the v10
    # exemption was keyed on the WRONG side — it exempted claims on works
    # that use citation formulas ANYWHERE in their own text, re-admitting
    # the geonic digest family. v11 keys on the ALIGNED POSITION: a page-
    # side formula near the span head demotes UNLESS the claimed work's own
    # stream carries a formula at the aligned position too (then the
    # formula is part of the work itself — e.g. R. Hananel lemmata opening
    # גרסינן, and the page is copying the work INCLUDING its citation).
    # length >= 4 letters only: shorter normalized forms ('וזל','לקו')
    # match inside ordinary words (גוזל, לקוח). Markers normalized through
    # norm_stream so final-letter folding matches the work streams
    # (Codex v11 R1 HIGH: re.sub kept final letters -> גרסינן never matched)
    wstreams = {w['id']: w['stream'] for w in ref_all}
    norm_markers = tuple({norm_stream(m)[0] for m in CITE_MARKERS
                          if len(norm_stream(m)[0]) >= 4})
    cand_rows = [r for r in survivors
                 if r[4] not in CANON_CATS and r[4] != 'Liturgy']
    cand_rows.sort(key=lambda r: r[1])
    drop_cite = set()
    exempt_cite = set()   # page formula matched by a work-side formula
    cur_pid, ptext_c, poffs_c, pstream_cc = None, '', None, ''
    for r in cand_rows:
        if r[1] != cur_pid:
            cur_pid = r[1]
            t = con.execute("SELECT text FROM pages WHERE page_id=?",
                            (cur_pid,)).fetchone()
            ptext_c = (t[0] or '') if t else ''
            pstream_cc, poffs_c = norm_stream(ptext_c)
        b0_, b1_ = max(r[13], key=lambda s: s[1] - s[0])[:2]
        if poffs_c is None or not len(poffs_c) or b0_ >= len(poffs_c):
            continue
        raw0 = poffs_c[b0_]
        win = ptext_c[max(0, raw0 - CITE_WINDOW):raw0 + CITE_HEAD]
        if not any(m in win for m in CITE_MARKERS):
            continue
        ws = wstreams.get(r[3], '')
        sl = pstream_cc[b0_:b1_]
        keep = False
        if ws and len(sl) >= 25:
            res = partial_ratio_alignment(sl, ws, score_cutoff=45)
            if res is not None:
                d0 = res.dest_start
                wwin = ws[max(0, d0 - 45):d0 + 35]
                keep = any(nm in wwin for nm in norm_markers)
        if keep:
            exempt_cite.add((r[1], r[3]))
        else:
            drop_cite.add((r[1], r[3]))
    if drop_cite:
        before = len(survivors)
        survivors = [r for r in survivors if (r[1], r[3]) not in drop_cite]
        stats['guard_cite_formula'] = before - len(survivors)
    print(f"cite-formula gate v11 (aligned host-side exemption): dropped "
          f"{len(drop_cite):,}, exempted-aligned {len(exempt_cite):,}",
          flush=True)
    guard_works = [w for w in ref_all
                   if w['cat'] in ('Bible', 'Targum', 'Liturgy', 'Mishnah',
                                   'Bavli', 'Yerushalmi', 'Tosefta')
                   or (w['cat'] == 'JA' and 'תפסיר' in (w['title'] or ''))]
    guard_ids = {w['id'] for w in guard_works}
    n_midrash_guard = 0
    if prm.get('guard_midrash_by_attestation'):
        for w in ref_all:
            if w['id'] not in guard_ids \
                    and (w.get('genre') or '') == 'תלמוד ומדרש' \
                    and len(a_ms.get(w['id'], ())) > rarity_max:
                guard_works.append(w)
                guard_ids.add(w['id'])
                n_midrash_guard += 1
        print(f"guard v7: +{n_midrash_guard} well-attested rabbinic-genre "
              f"works (witnesses > {rarity_max})", flush=True)
    # guard-only statutory units (refs_staging manifest, guard_only: true —
    # kaddish, kol chamira, hamapil, sheva berachot...; NEVER in the census
    # reference; their GRD: ids never equal a claimed work id, so the
    # claimed-work exemption is unaffected)
    n_grd = 0
    man_p = os.path.join(PROBE, 'refs_staging', 'manifest.json')
    if os.path.exists(man_p):
        for e in json.load(open(man_p, encoding='utf-8'))['entries']:
            if not e.get('guard_only'):
                continue
            bp = os.path.join(PROBE, 'refs_staging', e['body_file'])
            if not os.path.exists(bp):
                continue
            stream = norm_stream(
                open(bp, encoding='utf-8', errors='replace').read())[0]
            if len(stream) < 20:
                continue
            guard_works.append({'id': 'GRD:' + e['key'], 'stream': stream,
                                'cat': 'Liturgy',
                                'title': e.get('title', e['key'])})
            n_grd += 1
        if n_grd:
            print(f"guard: +{n_grd} guard-only statutory units "
                  f"(refs_staging)", flush=True)
    bible_stream = ''.join(w['stream'] for w in ref_all if w['cat'] == 'Bible')
    # wstreams already built at the cite gate (flank chips + reversed check)
    del ref_all
    guard_ref = build_ref_index(guard_works)[:6]
    print(f"guard reference: {len(guard_works)} works; "
          f"bible stream {len(bible_stream):,}", flush=True)

    # checkpoint fingerprint (Codex R2 #1): stale verdicts from a different
    # DB / ref / guard-param combination must never bypass the guards
    fp = {'hdr': 1,
          'db': args.db, 'db_mtime': int(os.path.getmtime(args.db)),
          'ref_mtime': int(os.path.getmtime(
              PROBE + r"\data\ref_corpus_v2.pkl")),
          'guard': [GUARD_COVER_MIN, BIBLE_ALIGN_MIN, rarity_max,
                    n_midrash_guard, n_grd]}
    done = {}
    stale = False
    if os.path.exists(ckpt_path):
        with open(ckpt_path, encoding='utf-8') as f:
            first = f.readline()
            try:
                ok = json.loads(first) == fp
            except Exception:
                ok = False
            if ok:
                for line in f:
                    try:
                        r = json.loads(line)
                        done[(r['pid'], r['wid'])] = r['v']
                    except Exception:
                        pass
                print(f"checkpoint: {len(done):,} guard verdicts loaded")
            else:
                stale = True
        if stale:
            # remove OUTSIDE the with-block (Windows locks open files)
            print("checkpoint fingerprint MISMATCH — discarding old verdicts")
            os.remove(ckpt_path)
    if not os.path.exists(ckpt_path):
        with open(ckpt_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(fp) + "\n")
    ck = open(ckpt_path, 'a', encoding='utf-8')

    survivors.sort(key=lambda r: r[1])   # page locality for stream fetch
    pcache = {}

    def pstream(pid_):
        if pid_ not in pcache:
            if len(pcache) > 8000:
                pcache.clear()
            t = con.execute("SELECT text FROM pages WHERE page_id=?",
                            (pid_,)).fetchone()
            pcache[pid_] = norm_stream(t[0] or '')[0] if t else ''
        return pcache[pid_]

    # verdicts: 0 keep, 1 canonical-rendering drop, 2 bible-align drop
    todo = [(i, r) for i, r in enumerate(survivors)
            if (r[1], r[3]) not in done]
    print(f"guard todo: {len(todo):,} of {len(survivors):,}", flush=True)
    CH = prm['guard_chunk']
    for c0 in range(0, len(todo), CH):
        chunk = todo[c0:c0 + CH]
        slices, meta_c = [], []
        for i, r in chunk:
            sp = max(zip([a for a, b in r[13]], [b for a, b in r[13]]),
                     key=lambda s: s[1] - s[0]) if r[13] else (0, 0)
            sl = pstream(r[1])[sp[0]:sp[1]]
            slices.append(sl)
            meta_c.append((i, r, sl))
        test_pos = [k for k, (i, r, sl) in enumerate(meta_c)
                    if r[4] != 'Bible' and r[3] not in guard_ids]
        results = query_batch_trimmed([slices[k] for k in test_pos], guard_ref)
        verdict = {}
        for k, hulls in zip(test_pos, results):
            i, r, sl = meta_c[k]
            per_work = defaultdict(list)
            for wi_g, hp0, hp1 in hulls:
                per_work[wi_g].append((hp0, hp1))
            slen = max(1, len(sl))
            for ivs in per_work.values():
                if sum(b - a for a, b in merge_iv(ivs)) / slen \
                        >= GUARD_COVER_MIN:
                    verdict[i] = 1
                    break
        for k, (i, r, sl) in enumerate(meta_c):
            if i in verdict:
                continue
            v = 0
            if r[4] not in ('Bible', 'Targum') and len(sl) >= 30:
                res = partial_ratio_alignment(sl, bible_stream,
                                              score_cutoff=BIBLE_ALIGN_MIN)
                if res is not None and res.score >= BIBLE_ALIGN_MIN:
                    v = 2
            verdict[i] = v
        for i, r, _sl in meta_c:
            v = verdict.get(i, 0)
            done[(r[1], r[3])] = v
            ck.write(json.dumps({'pid': r[1], 'wid': r[3], 'v': v}) + "\n")
        ck.flush()
        print(f"  guard chunk {c0//CH + 1}/{(len(todo)+CH-1)//CH} "
              f"({time.time()-t0:.0f}s)", flush=True)
    ck.close()

    kept = []
    for r in survivors:
        v = done.get((r[1], r[3]), 0)
        if v == 1:
            stats['guard_canonical_rendering'] += 1
        elif v == 2:
            stats['guard_verse_align'] += 1
        else:
            kept.append(r)
    print(f"guards final: kept {len(kept):,} "
          f"(canonical {stats['guard_canonical_rendering']:,}, "
          f"verse-align {stats['guard_verse_align']:,})", flush=True)
    # free guard memory before rendering (Codex R2 #3); keep only the
    # work streams the flank chips can actually need
    del guard_ref, guard_works, bible_stream
    kept_wids = {r[3] for r in kept}
    wstreams = {w: s for w, s in wstreams.items() if w in kept_wids}
    import gc
    gc.collect()

    # ---- aggregate per (manuscript, work) ----
    best = {}
    pages_of = defaultdict(list)
    for row in kept:
        key = (row[2], row[3])
        pages_of[key].append((row[0], row[1]))
        if key not in best or row[0] > best[key][0]:
            best[key] = row
    ms_rows = sorted(best.values(), key=lambda r: -r[0])
    print(f"(manuscript, work) rows: {len(ms_rows):,}")

    # ---- shelfmark/NLI metadata ----
    import csv
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for r in rd:
            if len(r) >= 4 and r[0]:
                v = [x.strip() for x in (r[2] or '').split('|') if x.strip()]
                meta[r[0]] = (v[0] if v else r[0], r[3].strip() or '?',
                              r[7].strip() if len(r) >= 8 else '')

    reftext = RefText()

    def _plain(htm):
        """Card pane HTML -> plain text; match highlight kept as 【…】."""
        if not htm:
            return ''
        s = htm.replace('<mark>', '【').replace('</mark>', '】')
        s = re.sub(r'<[^>]+>', '', s)
        return html.unescape(s)

    def card_html(row, blind=False, bid=''):
        (p, pid, sid, wid, cat, author, title, alen, dens, margin, ncomp,
         band, letters, spans) = row
        trow = con.execute(
            f"SELECT text, {prov_expr} FROM pages WHERE page_id=?",
            (pid,)).fetchone()
        if not trow or not trow[0]:
            return None
        prov = (trow[1] or 'htr') if len(trow) > 1 else 'htr'
        prov_chip = ("" if prov == 'htr' else
                     f"<span class='chip' style='color:#6a1b9a'>טקסט: "
                     f"תעתיק אנושי ({html.escape(prov.upper())})</span>")
        sm, lib, title_nli = meta.get(sid, (sid, '?', ''))
        pn = pnum(pid)
        url = (f"https://genizahsearch.com/browse?sys_id={sid}"
               + (f"&page={pn}" if pn else ""))
        name = f"{author} — {title}" if author else title
        n_wit = len(a_ms.get(wid, ()))
        page_htm, page_slice = snippet(trow[0], spans)
        ref_htm = reftext.passage(wid, page_slice)
        ps = pstream(pid)
        b0, b1 = max(spans, key=lambda s: s[1] - s[0])
        fc, fd = flank_class(ps, b0, b1, wstreams.get(wid, ''))
        fc_txt, fc_col = FLANK_HE[fc]
        n_pgs = len(pages_of.get((sid, wid), []))
        more = (f" · עוד {n_pgs - 1} עמודים בכתב־היד הזה"
                if n_pgs > 1 else "")
        p_show = min(p, prm['display_p_cap'])
        head_p = ("" if blind else
                  f"<span class='p'>{p_chip(p_show, band, alen, prm)}</span>")
        bid_tag = f"<span class='p' style='background:#555'>{bid}</span>" \
            if blind else ""
        stat_line = (f"אורך התאמה <bdi dir='ltr'>{alen}</bdi> אות · "
                     f"מרחק <bdi dir='ltr'>{dens:.2f}</bdi>"
                     + ("" if blind else
                        f" · {BAND_HE.get(band, band)}"
                        + (f" (פער {margin:.2f})" if margin is not None and ncomp else ""))
                     + f" · עדים מחמירים לחיבור: <bdi dir='ltr'>{n_wit}</bdi>{more}")
        ref_pane = (f"<div class='pane'><div class='lbl'>המקבילה במהדורה:</div>"
                    f"<div class='ev ref'>{ref_htm}</div></div>" if ref_htm
                    else "<div class='pane'><div class='lbl'>(המקבילה במהדורה לא אותרה לתצוגה)</div></div>")
        nli_t = (f" <span class='nli'>· קטלוג NLI: {html.escape(title_nli[:60])}</span>"
                 if title_nli else "")
        info = {
            'page_id': pid, 'sys_id': sid, 'shelfmark': sm, 'library': lib,
            'nli_title': title_nli, 'work_id': wid, 'cat': cat,
            'work_name': name, 'p': round(p, 4), 'p_shown': round(p_show, 4),
            'band': band, 'margin': margin, 'n_competitors': ncomp,
            'alen': alen, 'dens': round(dens, 4), 'page_letters': letters,
            'n_work_witnesses_tierA': n_wit, 'n_pages_this_ms': n_pgs,
            'flank_class': fc, 'flank_dist': fd, 'text_provenance': prov,
            'url': url, 'page_snippet': _plain(page_htm),
            'ref_snippet': _plain(ref_htm),
        }
        return info, f"""
<div class='card'>
 <div class='head'>
  {bid_tag}{head_p}
  <a href='{url}' target='_blank'><bdi dir='ltr'><b>{html.escape(sm)}</b>
  ({lib})</bdi></a>
  <span class='work'>[{cat}] {html.escape(name[:75])}</span>{nli_t}
  <span class='chip' style='color:{fc_col}'>{fc_txt}{
    f" <bdi dir='ltr'>({fd:.2f})</bdi>" if fd is not None else ""}</span>
  {prov_chip}
 </div>
 <div class='stats'>{stat_line}</div>
 <div class='panes'>
  <div class='pane'><div class='lbl'>קטע העמוד (גניזה, מודגש = ההתאמה):</div>
   <div class='ev'>{page_htm}</div></div>
  {ref_pane}
 </div>
</div>"""

    CSS = """
 body{font-family:Segoe UI,Arial;max-width:1150px;margin:20px auto;
 padding:0 12px;background:#fafaf7;color:#222}
 .card{background:#fff;border:1px solid #ddd;border-radius:8px;
 margin:10px 0;padding:8px 14px}
 .head{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
 .p{background:#1a5da6;color:#fff;border-radius:6px;padding:2px 8px;
 font-weight:bold} .work{color:#444}
 .chip{font-size:12px;font-weight:bold;border:1px solid currentColor;
 border-radius:10px;padding:1px 8px}
 .stats{font-size:12.5px;color:#666;margin:4px 0}
 .ev{direction:rtl;text-align:right;font-size:14.5px;line-height:1.7;
 white-space:pre-wrap;background:#fcfcf9;border:1px solid #eee;
 border-radius:6px;padding:6px 8px}
 .ev mark{background:#ffe58a} .ev .ctx{color:#aaa}
 .ev.ref{background:#f4f9f4;border-color:#d8e8d8}
 .lbl{font-size:12px;color:#777;margin-top:6px}
 .nli{font-size:12.5px;color:#8a6d3b}
 .panes{display:flex;gap:12px;flex-wrap:wrap}
 .pane{flex:1 1 340px;min-width:0}
 h2{border-bottom:2px solid #1a5da6;padding-bottom:4px;margin-top:28px}
 .note{background:#eef4fb;border:1px solid #cfe0f5;border-radius:8px;
 padding:10px 14px;font-size:14px}
"""

    # ---- main deck (v11: title-gate routing, MAPV2-A) ----
    # The catalog title is a near-perfect router (gold annotation of the v10
    # deck): generic/absent title -> 75% true discoveries; a DIFFERENT
    # specific title -> 85% shared-source leaks (the exceptions are catalog
    # corrections); same/variant title -> 100% already-known. Route to
    # dedicated sections instead of selling everything as discovery.
    from title_gate import TitleGate
    tg = TitleGate({sid: v[2] for sid, v in meta.items()})
    tcache = {}

    def tclass_of(row):
        key = (row[2], row[3])
        if key not in tcache:
            tcache[key] = tg.classify(row[2], f"{row[5]} {row[6]}")
        return tcache[key][0]

    SMALL_BANDS = ('m_003_010', 'm_0_003')

    def reversed_citation(row):
        """Work-side citation formula at the aligned span head while the
        page side is clean -> the EDITION quotes the source the page
        carries directly (blind card #23): a potential find, not a leak."""
        if (row[1], row[3]) in exempt_cite:
            return False            # page side carries the formula too
        ps = pstream(row[1])
        b0, b1 = max(row[13], key=lambda s: s[1] - s[0])[:2]
        ws = wstreams.get(row[3], '')
        sl = ps[b0:b1]
        if not ws or len(sl) < 25:
            return False
        res = partial_ratio_alignment(sl, ws, score_cutoff=45)
        if res is None:
            return False
        wwin = ws[max(0, res.dest_start - 45):res.dest_start + 12]
        return any(nm in wwin for nm in norm_markers)

    used = set()
    disc_rows = [r for r in ms_rows if r[4] != 'Liturgy']
    stat_rows = [r for r in ms_rows if r[4] == 'Liturgy']
    pools = defaultdict(list)
    for r in disc_rows:
        pools[tclass_of(r)].append(r)
    stats_t = {k: len(v) for k, v in pools.items()}
    print(f"title gate over (ms,work) rows: {stats_t}", flush=True)
    known_rows = sorted(pools['same_work'] + pools['name_variant'],
                        key=lambda r: -r[0])
    reversed_items = []
    rev_work = Counter()   # Codex v11 R1 MED: cap the reversed section too
    rev_ms = Counter()

    def fill(rows, lo, hi, cap, exclude_small=False, small_any_p=False,
             divert_reversed=False):
        per_work = Counter()
        per_ms = Counter()
        items = []
        for row in rows:
            key = (row[2], row[3])
            if key in used:
                continue
            small = row[11] in SMALL_BANDS
            if exclude_small and small:
                continue
            in_p = lo <= row[0] < hi
            if not (in_p or (small_any_p and small and row[0] >= 0.2)):
                continue
            if per_work[row[3]] >= prm['per_work_cap'] \
                    or per_ms[row[2]] >= prm.get('per_ms_cap', 99):
                continue
            if divert_reversed and len(reversed_items) < 10 \
                    and rev_work[row[3]] < 2 and rev_ms[row[2]] < 2 \
                    and reversed_citation(row):
                res = card_html(row)
                if res:
                    used.add(key)
                    rev_work[row[3]] += 1
                    rev_ms[row[2]] += 1
                    reversed_items.append((res[0], res[1], key))
                continue
            res = card_html(row)
            if res:
                info, h = res
                used.add(key)
                per_work[row[3]] += 1
                per_ms[row[2]] += 1
                items.append((info, h, key))
            if len(items) >= cap:
                break
        return items

    sections_data = [
        ("תגליות — הקטלוג עדיין לא מזהה · כמעט־ודאי (P ≥ 0.8)",
         fill(pools['generic_or_absent'], 0.8, 1.01, 40,
              exclude_small=True, divert_reversed=True)),
        ("תגליות — סבירות (P 0.5–0.8)",
         fill(pools['generic_or_absent'], 0.5, 0.8, 20,
              exclude_small=True, divert_reversed=True)),
        ("תגליות בסימן שאלה — P 0.2–0.5 או פער־שוליים קטן",
         fill(pools['generic_or_absent'], 0.2, 0.5, 12, small_any_p=True,
              divert_reversed=True)),
        ("הקטלוג מזהה חיבור אחר — לרוב מקור משותף, אך כאן מסתתרים "
         "תיקוני־קטלוג (P ≥ 0.8)",
         fill(pools['different_specific'], 0.8, 1.01, 12,
              exclude_small=True, divert_reversed=True)),
        ("אישורי קטלוג — הקטלוג כבר מזהה כך; המנוע מאשר (P ≥ 0.5)",
         fill(known_rows, 0.5, 1.01, 12)),
        ("תלות ספרותית ידועה — החיבור שבקטלוג מצטט את החיבור הנטען "
         "(או להפך)",
         fill(pools['known_quoter'], 0.8, 1.01, 8)),
    ]
    # reversed-citation section slots after the discovery sections
    sections_data.insert(
        3, ("ציטוט בכיוון הפוך — המהדורה מצטטת מקור שהדף נושא ישירות "
            "(מועמדי־מציאה)", [x for x in reversed_items]))
    st_items = []
    per_work = Counter()
    for row in stat_rows:
        if row[0] < 0.5 or per_work[row[3]] >= 2:
            continue
        res = card_html(row)
        if res:
            per_work[row[3]] += 1
            st_items.append((res[0], res[1], (row[2], row[3])))
        if len(st_items) >= 20:
            break
    sections_data.append((
        "נוסחי קבע — עדים נכונים לרוב אך שגרתיים (מופרדים מרובד התגליות)",
        st_items))

    sections = []
    deck_cards = []          # structured card dump (annotation input)
    for label, items in sections_data:
        cards = []
        for info, h, key in items:
            info['section'] = label
            info['card_no'] = len(deck_cards) + 1
            info['title_class'] = tcache.get(key, ('?', None))[0]
            info['title_evidence'] = tcache.get(key, ('?', None))[1]
            deck_cards.append(info)
            cards.append(h)
        sections.append((label, cards))
        print(f"  {label[:52]}: {len(cards)} cards", flush=True)

    hist = Counter(min(9, int(r[0] * 10)) / 10 for r in ms_rows)
    hist_html = " · ".join(f"{k:.1f}: {v:,}" for k, v in sorted(hist.items()))
    n_a_live = len(a_pairs)
    body = []
    for label, cards in sections:
        if not cards:
            continue
        body.append(f"<h2>{label}</h2>")
        body.extend(cards)
    note = (f"<b>מה זה (v11):</b> רובד התגליות של {args.label} "
            f"({n_pages:,} עמודים עם מועמדים; הרובד המחמיר: {n_a_live:,} "
            f"זיהויים). <b>חדש: נתב הכותרות</b> — כל כרטיס הושווה אוטומטית "
            f"מול כותרת NLI וזיהויי FJMS (עם בקרת שמות־נרדפים): "
            f"מדורי <b>התגליות</b> מכילים רק כתבי־יד שהקטלוג עדיין לא "
            f"מזהה בהם חיבור ספציפי; זיהוי־קיים הופרד ל'אישורי קטלוג'; "
            f"כותרת שמזהה חיבור אחר — למדור חשוד משלה (שם מסתתרים גם "
            f"תיקוני־קטלוג); תלות ספרותית ידועה (ערוך↔ר\"ח וכד') הופרדה. "
            f"<b>P = טיב הקשר הטקסטואלי</b> (חוזק היחס בין הדף למהדורה), "
            f"לא הסתברות עדות. מסנני הציטוטים כוללים כעת גם את משפחת "
            f"הסימנים הערבית־יהודית (לקו', כמא קאל) ופטור־ציטוט שנבדק "
            f"בצד החיבור בנקודת ההתאמה עצמה; ציטוט בכיוון הפוך (המהדורה "
            f"מצטטת את מקור הדף) מנותב למדור מועמדי־מציאה. "
            f"שערי הרקע: קנון חז\"לי, נוסחי קבע, כיסוי־קנוני ≥ "
            f"{int(prm['canon_claim_min_coverage']*100)}%, נדירות ≤ "
            f"{rarity_max} עדים. תווית השוליים בכל כרטיס = מבחן ההקשר "
            f"(ממשיך/אי/קצה) — אינדיקציה בלבד. "
            f"<br><b>התפלגות P אחרי הכל:</b> {hist_html}")
    doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'><head>
<meta charset='utf-8'><title>MAPV2 — רובד התגליות ({html.escape(args.label)})</title>
<style>{CSS}</style></head><body>
<h1>רובד התגליות — {html.escape(args.label)}</h1>
<div class='note'>{note}</div>
{''.join(body)}
</body></html>"""
    open(out_main, 'w', encoding='utf-8').write(doc)
    print(f"wrote {out_main}")
    out_cards = os.path.join(args.outdir, 'mapv2_deck_cards.json')
    json.dump(deck_cards, open(out_cards, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f"wrote {out_cards} ({len(deck_cards)} cards)")

    # ---- blinded deck (stratified band x P, no scores shown) ----
    rng = random.Random(29)
    strata_pool = defaultdict(list)
    for row in disc_rows:
        pb = min(4, int(row[0] * 5))          # 5 P buckets
        strata_pool[(row[11], pb)].append(row)
    cells = sorted(strata_pool)
    picks = []
    quota = max(1, prm['blinded_n'] // max(1, len(cells)))
    for cell in cells:
        pool = strata_pool[cell]
        rng.shuffle(pool)
        picks.extend(pool[:quota])
    rng.shuffle(picks)
    picks = picks[:prm['blinded_n']]
    bcards, key_map = [], {}
    for j, row in enumerate(picks, 1):
        bid = f"B-{j:03d}"
        res = card_html(row, blind=True, bid=bid)
        if res:
            bcards.append(res[1])
            key_map[bid] = {'p': row[0], 'band': row[11], 'page': row[1],
                            'sys': row[2], 'work': row[3], 'alen': row[7],
                            'dens': row[8]}
    bdoc = f"""<!DOCTYPE html><html lang='he' dir='rtl'><head>
<meta charset='utf-8'><title>MAPV2 — חפיסת דירוג עיוורת</title>
<style>{CSS}</style></head><body>
<h1>דירוג עיוור — {len(bcards)} כרטיסים בלי ציון</h1>
<div class='note'>דרג כל כרטיס: נכון / סביר / דליפה / לא ברור.
הציונים הוסתרו בכוונה (המפתח נשמר בנפרד) כדי שהדירוג לא יוטה.</div>
{''.join(bcards)}
</body></html>"""
    open(out_blind, 'w', encoding='utf-8').write(bdoc)
    json.dump(key_map, open(out_key, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f"wrote {out_blind} + key ({len(key_map)} cards)")

    # ---- report ----
    L = ["# MAPV2 deck build report\n"]
    L.append(f"- DB: {args.db}")
    L.append(f"- pages with candidate rows: {n_pages:,}")
    L.append(f"- tier A live: {n_a_live:,} (ms,work); works: {len(a_ms):,}")
    L.append(f"- rarity gate: q{int(prm['rarity_quantile']*100)} of "
             f"witness counts = {quantile(counts, prm['rarity_quantile'])}, "
             f"bounded -> **{rarity_max}**")
    L.append(f"- singleton caps: {prm['singleton_caps']}; display cap "
             f"{prm['display_p_cap']}")
    L.append("\n## Funnel\n")
    for k, v in stats.most_common():
        L.append(f"- {k}: {v:,}")
    L.append(f"- survivors after cheap guards: {len(survivors):,}")
    L.append(f"- kept after canonical+verse guards: {len(kept):,}")
    L.append(f"- (ms, work) aggregated: {len(ms_rows):,}")
    L.append("\n## Title-gate router (v11)\n")
    for k in ('generic_or_absent', 'same_work', 'name_variant',
              'known_quoter', 'different_specific'):
        L.append(f"- {k}: {stats_t.get(k, 0):,}")
    L.append(f"- reversed-citation candidates surfaced: "
             f"{len(reversed_items)}")
    L.append(f"- cite-gate v11: dropped {len(drop_cite):,}, "
             f"exempted-aligned {len(exempt_cite):,}")
    L.append(f"\n## P histogram (post-guard, aggregated)\n{hist_html}")
    L.append(f"\n- total time: {time.time()-t0:.0f}s")
    open(out_report, 'w', encoding='utf-8').write("\n".join(L) + "\n")
    print(f"wrote {out_report}; TOTAL {time.time()-t0:.0f}s")
    con.close()


if __name__ == '__main__':
    main()

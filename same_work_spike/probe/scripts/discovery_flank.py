# -*- coding: utf-8 -*-
"""MAPV2-15m stage 2 — the citation-vs-witness detector (advisory flank pass).

Per match in data/discovery_scored_gated.jsonl (bucket2=='discovery'):
  A (primary)  relocate the matched span in the target work stream, then test
               flank RECONVERGENCE with an edge-gap (flank_align).
  B (support)  whole-page adjusted coverage incl. recovered continuation blocks.
  C (confirm)  a competing NON-canonical, non-equivalent work covering a flank
               (from track1_matches UNION track1_candidates — candidates is
               tier-B only, Codex pre-flight #1).
Verdict -> advisory multiplier (0.45 strong-citation .. 1.10 strong-cont),
disc_score2 unchanged, disc_score2_flank = disc_score2 * multiplier. Fragmentary
pages ABSTAIN (never penalized). RAW signals are stored so calibrate_flank.py
can grid-search thresholds without recompute.

Codex pre-flight fixes folded: spans/page text are re-joined from the DB (the
gated jsonl has NO spans_json, #2); local gap-aware span merge (#3); C = matches
UNION candidates (#1). Does NOT reuse the old broken flank_class as a feature.

Out: data/discovery_scored_flank.jsonl + results/discovery_flank_report.md
Usage: python -X utf8 -u discovery_flank.py [--limit N]
"""
import argparse
import json
import os
import pickle
import sqlite3
import sys
import time
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from flank_align import (WINDOW, classify, flank_dist, gram_index, relocate)
from bib_gate import heb_tokens
from normalize import norm_stream

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
REF = PROBE + r"\data\ref_corpus_v2.pkl"
IN = PROBE + r"\data\discovery_scored_gated.jsonl"
OUT = PROBE + r"\data\discovery_scored_flank.jsonl"
MD = PROBE + r"\results\discovery_flank_report.md"

CANON = {'Bible', 'Bavli', 'Mishnah', 'Yerushalmi', 'Tosefta'}
LITURGY_GENRES = {'פיוט ותפילה', 'שירת ספרד'}
# frozen calibrated cutoffs (calibrate_flank.py -> data/flank_thresholds.json)
_THR = PROBE + r"\data\flank_thresholds.json"
try:
    _t = json.load(open(_THR, encoding='utf-8'))
    CONT_THR, ISLAND_THR = _t['cont_thr'], _t['island_thr']
except (OSError, ValueError, KeyError):
    CONT_THR, ISLAND_THR = 0.42, 0.58
# liturgy is often mis-/un-labelled by genre; detect it from the title too so a
# statutory prayer or piyyut isn't mistaken for 'prose' (which would make a
# liturgical neighbour look like a cross-genre citation competitor).
LITURGY_TITLE = ('תפיל', 'תפל', 'ברכה', 'ברכת', 'סליח', 'פיוט', 'קדוש', 'פתיחה',
                 'הרחבה', 'וידוי', 'קינה', 'קינות', 'קרוב', 'יוצר', 'מעריב',
                 'זולת', 'אופן', 'רהיט', 'פזמון', 'הושענא', 'מזמור', 'זמיר',
                 'שבעתא', 'עבודה', 'סילוק', 'מגן', 'מחיה')


def fam(cat, genre, title=''):
    """Coarse genre family. 'different flanks' is only citation evidence when
    the flank work is a DIFFERENT family — liturgy amid liturgy (or piyyut amid
    piyyut) is an anthology, where each unit is still a witness, not a citation."""
    if cat in CANON:
        return 'canon'
    if cat == 'Targum':
        return 'targum'
    if cat == 'Liturgy' or genre in LITURGY_GENRES \
            or any(k in (title or '') for k in LITURGY_TITLE):
        return 'liturgy'
    return 'prose'
FLANK_OUT = 220          # letters of flank to consider
MERGE_GAP = 15           # local gap-aware span merge (stored spans already <=30)
C_MIN_LETTERS = 90       # competitor must cover >=90 flank letters or >=45%
C_MIN_FRAC = 0.45
MULT = {'target_continuation_strong': 1.10, 'target_continuation_weak': 1.04,
        'mixed_multiwork': 1.00, 'abstain': 1.00,
        'likely_citation_weak': 0.75, 'likely_citation_strong': 0.45}


def _ro():
    return sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro', uri=True)


def merge_spans(spans, gap=MERGE_GAP):
    iv = sorted((int(s[0]), int(s[1])) for s in spans)
    out = []
    for a, b in iv:
        if out and a - out[-1][1] < gap:
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return out


def load_target_spans(pages):
    """(page_id, work_id) -> merged page-span list, from matches then candidates."""
    con = _ro()
    out = {}
    pl = list(pages)
    for tbl in ('track1_matches', 'track1_candidates'):
        for i in range(0, len(pl), 400):
            b = pl[i:i + 400]
            qm = ','.join('?' * len(b))
            for pid, wid, sj in con.execute(
                    f"SELECT page_id, work_id, spans_json FROM {tbl} "
                    f"WHERE page_id IN ({qm})", b):
                k = (pid, wid)
                if k in out:
                    continue
                try:
                    out[k] = merge_spans(json.loads(sj))
                except Exception:
                    pass
    con.close()
    return out


def load_competitors(pages):
    """page_id -> [(work_id, cat, title, merged_spans, strong)] from matches
    UNION candidates. tier-A (track1_matches) rows have no p_same_work -> strong
    by tier; candidates strong iff p_same_work>=0.65 or matched_letters>=100."""
    con = _ro()
    out = defaultdict(list)
    pl = list(pages)
    seen = set()
    for tbl, tierA in (('track1_matches', True), ('track1_candidates', False)):
        cols = ("page_id, work_id, cat, genre, title, matched_letters, spans_json"
                if tierA else
                "page_id, work_id, cat, genre, title, matched_letters, p_same_work, spans_json")
        for i in range(0, len(pl), 400):
            b = pl[i:i + 400]
            qm = ','.join('?' * len(b))
            for row in con.execute(
                    f"SELECT {cols} FROM {tbl} WHERE page_id IN ({qm})", b):
                if tierA:
                    pid, wid, cat, gen, ti, ml, sj = row
                    strong = True
                else:
                    pid, wid, cat, gen, ti, ml, psw, sj = row
                    strong = (psw or 0) >= 0.65 or (ml or 0) >= 100
                if (pid, wid) in seen:
                    continue
                seen.add((pid, wid))
                try:
                    sp = merge_spans(json.loads(sj))
                except Exception:
                    continue
                out[pid].append((wid, cat, gen or '', ti or '', sp, strong))
    con.close()
    return out


def _cover(region, spans):
    """letters of [r0,r1) covered by any span."""
    r0, r1 = region
    tot = 0
    for a, b in spans:
        lo, hi = max(r0, a), min(r1, b)
        if hi > lo:
            tot += hi - lo
    return tot


def _same_family(t_toks, comp_title):
    ct = set(heb_tokens(comp_title))
    return len(t_toks & ct) >= 2


def competitor_on(region, comps, target_wid, target_canon, t_toks):
    """Strong non-equivalent, non-canonical competitor covering `region`.
    Returns (wid, title, cov, cat, genre) or None."""
    r0, r1 = region
    span_len = r1 - r0
    if span_len < 60:
        return None
    for wid, cat, gen, ti, sp, strong in comps:
        if wid == target_wid or not strong:
            continue
        if cat in CANON and not target_canon:
            continue                       # canon competitor never demotes
        if _same_family(t_toks, ti):
            continue
        cov = _cover(region, sp)
        if cov >= C_MIN_LETTERS or cov >= C_MIN_FRAC * span_len:
            return (wid, ti, cov, cat, gen)
    return None


def flank_signals(r, ps, spans, wstream, gpos, comps):
    """Compute the raw flank signals for one match. SHARED by the detector and
    the calibrator so dev/production can never drift."""
    sig = {'reloc_ok': False, 'left_dist': None, 'right_dist': None,
           'page_letters': len(ps), 'adj_cov': 0.0, 'recovered': 0,
           'c_left': None, 'c_right': None,
           'tgt_fam': fam(r.get('cat'), r.get('genre'), r.get('title'))}
    if not (wstream and gpos and spans and ps):
        return sig
    cs, ce = spans[0][0], spans[-1][1]
    span_slice = ps[cs:ce]
    span_letters = sum(b - a for a, b in spans)
    rel = relocate(span_slice, wstream, gpos)
    if not rel:
        return sig
    r0, r1, _red, _na = rel
    sig['reloc_ok'] = True
    pl_f = ps[max(0, cs - FLANK_OUT):cs][::-1]        # reverse -> span-adjacent leads
    wl_f = wstream[max(0, r0 - FLANK_OUT):r0][::-1]
    pr_f = ps[ce:ce + FLANK_OUT]
    wr_f = wstream[r1:r1 + FLANK_OUT]
    sig['left_dist'] = flank_dist(pl_f, wl_f)
    sig['right_dist'] = flank_dist(pr_f, wr_f)
    rec = 0
    if classify(sig['left_dist']) == 'continuation':
        rec += min(len(pl_f), WINDOW)
    if classify(sig['right_dist']) == 'continuation':
        rec += min(len(pr_f), WINDOW)
    sig['recovered'] = rec
    sig['adj_cov'] = (span_letters + rec) / max(1, len(ps))
    tcanon = r.get('cat') in CANON
    ttoks = set(heb_tokens(r.get('title') or ''))
    sig['c_left'] = competitor_on((max(0, cs - FLANK_OUT), cs), comps, r['work_id'], tcanon, ttoks)
    sig['c_right'] = competitor_on((ce, ce + FLANK_OUT), comps, r['work_id'], tcanon, ttoks)
    return sig


def decide(sig, cont=0.42, island=0.58):
    """Verdict + multiplier from stored raw signals. Fragmentary-safe."""
    if not sig['reloc_ok']:
        return 'abstain', 'relocation failed'
    lc = classify(sig['left_dist'], cont, island)
    rc = classify(sig['right_dist'], cont, island)
    cov_ok = (sig['page_letters'] >= 200 and sig['adj_cov'] >= 0.55) or \
        sig['recovered'] >= 120
    cont_side = (lc == 'continuation') or (rc == 'continuation')
    strong_c = sig['c_left'] or sig['c_right']
    # continuation (witness)
    if cont_side or cov_ok:
        if strong_c and cont_side:
            return 'mixed_multiwork', 'continuation + competing work'
        strong = ((lc == 'continuation' and (sig['left_dist'] or 1) <= cont) and
                  sig['page_letters'] >= 120) or \
                 ((rc == 'continuation' and (sig['right_dist'] or 1) <= cont) and
                  sig['page_letters'] >= 120)
        return ('target_continuation_strong' if (cont_side and (strong or cov_ok))
                else 'target_continuation_weak'), 'reconverges to the work'
    # citation needs a NAMED, DIFFERENT-genre competitor on a flank. A
    # same-genre neighbour is an anthology sibling (liturgy amid liturgy), not a
    # citation; bare "flanks differ from the target" can be recension/HTR/
    # anthology -> abstain. Discovery-recall first (never bury a real find on
    # weak evidence); the survivor is human-reviewed anyway.
    tf = sig.get('tgt_fam', 'prose')

    def is_diff(c):                          # c=(wid,title,cov,cat,genre)
        return bool(c) and fam(c[3], c[4], c[1]) != tf

    island_l, island_r = lc == 'island', rc == 'island'
    cl_diff, cr_diff = is_diff(sig['c_left']), is_diff(sig['c_right'])
    parts = []
    if island_l:
        parts.append('left island')
    if island_r:
        parts.append('right island')
    if cl_diff:
        parts.append(f"left={sig['c_left'][1][:24]}")
    if cr_diff:
        parts.append(f"right={sig['c_right'][1][:24]}")
    if (island_l or island_r) and (cl_diff or cr_diff):
        strong = (island_l and island_r) or (cl_diff and cr_diff)
        return ('likely_citation_strong' if strong else 'likely_citation_weak',
                '; '.join(parts))
    return 'abstain', 'flanks differ but no different-genre work identified'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None)
    a = ap.parse_args()
    t0 = time.time()

    rows = [json.loads(l) for l in open(IN, encoding='utf-8')]
    disc = [r for r in rows if r.get('bucket2') == 'discovery']
    if a.limit:
        disc.sort(key=lambda r: -r.get('disc_score2', 0))
        disc = disc[:a.limit]
        keep_ids = {id(r) for r in disc}
    print(f"rows {len(rows)}; discovery to flank {len(disc)}", flush=True)

    ref = {w['id']: w['stream'] for w in pickle.load(open(REF, 'rb'))}
    print(f"ref streams {len(ref)} ({time.time()-t0:.0f}s)", flush=True)

    pages = {r['page_id'] for r in disc}
    con = _ro()
    ptext = {}
    pl = list(pages)
    for i in range(0, len(pl), 400):
        b = pl[i:i + 400]
        qm = ','.join('?' * len(b))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({qm})", b):
            ptext[pid] = norm_stream(tx or '')[0]
    con.close()
    tgt = load_target_spans(pages)
    comps = load_competitors(pages)
    print(f"page streams {len(ptext)}, target-span keys {len(tgt)}, "
          f"pages-with-competitors {len(comps)} ({time.time()-t0:.0f}s)", flush=True)

    # group by work_id -> build gram_index once per work
    by_work = defaultdict(list)
    for r in disc:
        by_work[r['work_id']].append(r)

    vcount = Counter()
    done = 0
    for wid, wrows in by_work.items():
        wstream = ref.get(wid)
        gpos = gram_index(wstream) if wstream else None
        for r in wrows:
            pid = r['page_id']
            ps = ptext.get(pid, '')
            spans = tgt.get((pid, wid), [])
            sig = flank_signals(r, ps, spans, wstream, gpos, comps.get(pid, []))
            verdict, why = decide(sig, CONT_THR, ISLAND_THR)
            mult = MULT[verdict]
            r['flank'] = {
                'verdict': verdict, 'multiplier': mult,
                'left_dist': sig['left_dist'], 'right_dist': sig['right_dist'],
                'reloc': sig['reloc_ok'], 'adj_cov': round(sig['adj_cov'], 3),
                'recovered': sig['recovered'],
                'c_left': sig['c_left'][1][:30] if sig['c_left'] else None,
                'c_right': sig['c_right'][1][:30] if sig['c_right'] else None,
                'why': why}
            r['disc_score2_flank'] = round(r.get('disc_score2', 0.0) * mult, 4)
            vcount[verdict] += 1
            done += 1
        if done and done % 5000 < len(wrows):
            print(f"  {done}/{len(disc)} ({time.time()-t0:.0f}s)", flush=True)

    # write ALL rows (non-disc unchanged, no flank field)
    with open(OUT, 'w', encoding='utf-8') as f:
        emit = disc if a.limit else rows
        for r in emit:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')

    # report
    demoted = [r for r in disc if r.get('flank', {}).get('multiplier', 1) < 1]
    boosted = [r for r in disc if r.get('flank', {}).get('multiplier', 1) > 1]
    L = [f"# Discovery flank pass (MAPV2-15m) — {'limit '+str(a.limit) if a.limit else 'FULL'}",
         "", f"- flanked discovery rows: {len(disc)} ({time.time()-t0:.0f}s)",
         "", "## verdict distribution", ""]
    for v, n in vcount.most_common():
        L.append(f"- {v}: {n} ({100*n//max(1,len(disc))}%) x{MULT[v]}")
    L += ["", f"## effect (advisory)",
          f"- demoted (citation, x<1): {len(demoted)}",
          f"- boosted (continuation, x>1): {len(boosted)}", "",
          "## sample strong-citation demotions", ""]
    for r in sorted([x for x in disc if x['flank']['verdict'] == 'likely_citation_strong'],
                    key=lambda r: -r['disc_score2'])[:15]:
        fl = r['flank']
        L.append(f"- `{r['disc_score2']:.2f}`->`{r['disc_score2_flank']:.2f}` "
                 f"{(r['title'] or '')[:34]} · {fl['why'][:50]}")
    L += ["", "## sample strong continuations (witness)", ""]
    for r in sorted([x for x in disc if x['flank']['verdict'] == 'target_continuation_strong'],
                    key=lambda r: -r['disc_score2_flank'])[:15]:
        L.append(f"- `{r['disc_score2_flank']:.2f}` {(r['title'] or '')[:38]} "
                 f"· {r['sys_id']}")
    open(MD, 'w', encoding='utf-8').write('\n'.join(L) + '\n')
    print('\n'.join(L[:30]))
    print(f"\nwrote {OUT} + {MD}")


if __name__ == '__main__':
    main()

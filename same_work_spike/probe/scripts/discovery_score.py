# -*- coding: utf-8 -*-
"""MAPV2-15k — corpus-wide discovery scoring + ranking.

Every SURFACED match (track1_matches, shadowed_by IS NULL, matched_letters>=40)
gets a discovery-likelihood score so genuine finds float to a SMALL top slice
(Hillel: discovery is the small part; rank it, don't stamp a flat label).

Score axes (Hillel's own):
  confidence  "is it really the work"  = p_same_work (calibrated: size+density+
              margin) gated by match SIZE (bigger span -> truer).
  novelty     "did anyone already know it was here" = catalog-silence (scope
              resolution) x work rarity (few witnesses -> rarer/costlier find).
  penalty     "same-work vs citation/shared" = canonical cat + canon rarity mass
              (a shared classical quotation). FLANK-CONTRAST (island=citation)
              is the decisive citation signal but is computed corpus-wide in a
              SEPARATE pass (needs ref text) and folded on the ranked top —
              see discovery_flank.py.

A Maagarim מסירה witness -> 'known' (this ms is already an edition witness); a
post-1700 reference (late_ref_blocklist) is DROPPED (anachronistic).

Out: data/discovery_scored.jsonl (one row per match, ranked) +
     results/discovery_score_report.md
Usage: python -X utf8 -u discovery_score.py [--limit N] [--out NAME]
"""
import argparse
import json
import math
import os
import sqlite3
import sys
import time
from collections import Counter, defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from canon_rarity import SHARED_TH, CanonRarity
from grader import (WITNESS_HEADS, LITURGY_GENRES, _head, _canon_scores,
                    _load_late_ref, _load_witnesses)
from metadata_scope import ScopeGate

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus_v2.db"
CANON = {'Bible', 'Bavli', 'Mishnah', 'Yerushalmi', 'Tosefta'}
MIN_LETTERS = 40


def _ro():
    return sqlite3.connect('file:' + DB.replace('\\', '/') + '?mode=ro', uri=True)


def load_matches(limit=None):
    con = _ro()
    q = ("SELECT page_id, sys_id, work_id, cat, genre, author, title, mesirah, "
         "matched_letters, best_density, n_spans FROM track1_matches "
         "WHERE shadowed_by IS NULL AND matched_letters >= ?")
    if limit:
        q += f" ORDER BY matched_letters DESC LIMIT {int(limit)}"
    rows = [dict(page_id=r[0], sys_id=str(r[1]), work_id=r[2], cat=r[3],
                 genre=r[4], author=r[5], title=r[6], mesirah=r[7],
                 matched_letters=r[8], best_density=r[9] or 0.0, n_spans=r[10])
            for r in con.execute(q, (MIN_LETTERS,))]
    con.close()
    return rows


def load_psw(pairs):
    """(page_id, work_id) -> (p_same_work, margin_band) from track1_candidates."""
    con = _ro()
    want = set(pairs)
    out = {}
    pages = list({p for p, w in want})
    for i in range(0, len(pages), 400):
        b = pages[i:i + 400]
        qm = ','.join('?' * len(b))
        for pid, wid, psw, mb in con.execute(
                f"SELECT page_id, work_id, p_same_work, margin_band "
                f"FROM track1_candidates WHERE page_id IN ({qm})", b):
            if (pid, wid) in want:
                out[(pid, wid)] = (psw, mb)
    con.close()
    return out


def load_npages():
    """sys_id -> total pages in the corpus (weak scope signal)."""
    con = _ro()
    npg = Counter()
    for (pid,) in con.execute("SELECT page_id FROM pages"):
        npg[pid.split('_', 1)[0]] += 1
    con.close()
    return npg


def _targum_key(work_id):
    """(targum_family, book) for a REF2 Targum work_id, else None. Lets us tell
    'two DIFFERENT Targumim of the SAME book' (Onkelos + Pseudo-Jonathan on
    Exodus; Targum + Targum Sheni on Esther) apart from 'the SAME Targum on
    different books' (Samuel I vs II — same family, distinct two-token book, so
    NOT a sibling)."""
    wid = work_id or ''
    if 'targum' not in wid:
        return None
    rest = wid.split('targum_', 1)[-1]          # tail after 'REF2:targum_'
    if rest.endswith('_targum_sheni'):          # Targum Sheni (of a book)
        return ('sheni', rest[:-len('_targum_sheni')].split('_')[-1])
    for fam in ('onkelos', 'jonathan', 'ketuvim'):
        if rest.startswith(fam + '_'):
            return (fam, rest[len(fam) + 1:])
    return ('?', rest)


_PENT_TARGUM_FAM = {'onkelos', 'jonathan'}


def _catalog_targum_family(nli_title):
    """Pentateuch-Targum family named by an NLI catalogue title, or None.
    Lets us catch a fragment CATALOGUED as one Targum (e.g. אונקלוס) but matched
    to a DIFFERENT one (פסאודו-יונתן) of the same book — the catalogue makes the
    real identity clear, so the labelled Targum is not a discovery."""
    t = nli_title or ''
    tl = t.lower()
    if 'אונקלוס' in t or 'onqelos' in tl or 'onkelos' in tl:
        return 'onkelos'
    if ('יונתן' in t or 'פסאודו' in t or 'pseudo' in tl or 'jonathan' in tl
            or 'ירושלמי' in t or 'palestinian' in tl or 'ניאופיטי' in t
            or 'neofiti' in tl):
        return 'jonathan'          # Palestinian/Ps-Jonathan/Neofiti cluster
    return None


def bucket_of(r):
    """4-bucket label from corpus-available fields (scope resolution already
    encodes the title-class signal via TitleGate)."""
    if r['maagarim_witness']:
        return 'known'
    if r['resolution'] == 'page_resolved_known':
        return 'known'
    # a DIFFERENT Targum of the same book on this ms is a parallel, not a find
    if r.get('targum_parallel'):
        return 'parallel'
    # statutory unit in a liturgical/anthology container -> textual witness
    title = r.get('title') or ''
    head = _head(title.split('—')[-1] if '—' in title else title)
    anon = (not r.get('author')) or ('לא ידוע' in str(r.get('author')))
    if head in WITNESS_HEADS and anon and \
            (r.get('genre') in LITURGY_GENRES or
             r['regime'] == 'homogeneous_anthology'):
        return 'witness'
    if r['cat'] in CANON or r['canon_mass'] >= SHARED_TH:
        return 'other'          # shared classical source / canonical quote
    if r['resolution'] == 'ms_scope_ambiguous':
        return 'discovery'
    return 'other'


def score_row(r):
    """discovery-likelihood in [0,1] + its transparent components."""
    conf = r['p_same_work'] if r['p_same_work'] is not None \
        else min(1.0, r['best_density'] * 2.0)
    size = min(1.0, r['matched_letters'] / 400.0)     # Hillel: bigger = truer
    conf = 0.5 * conf + 0.5 * (conf * size)
    resmap = {'ms_scope_ambiguous': 1.0, 'global_ms_likely': 0.35,
              'page_resolved_known': 0.0}
    nov = resmap.get(r['resolution'], 0.5)
    rarity = 1.0 / (1.0 + math.log10(max(1, r['work_nms'])))  # 1ms->1, 100ms->.33
    nov *= (0.4 + 0.6 * rarity)
    shared = 1.0 if r['cat'] in CANON else \
        min(1.0, r['canon_mass'] / (SHARED_TH * 1.5))
    pen = 1.0 - 0.7 * shared
    disc = conf * nov * pen
    return disc, dict(conf=round(conf, 3), nov=round(nov, 3),
                      rarity=round(rarity, 3), shared=round(shared, 3),
                      size=round(size, 3))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--out', default='discovery_scored')
    a = ap.parse_args()
    t0 = time.time()

    rows = load_matches(a.limit)
    print(f"matches (>= {MIN_LETTERS} letters, non-shadowed): {len(rows)}", flush=True)

    # work rarity = distinct MSS per work over the surfaced set
    work_ms = defaultdict(set)
    for r in rows:
        work_ms[r['work_id']].add(r['sys_id'])
    for r in rows:
        r['work_nms'] = len(work_ms[r['work_id']])

    late = _load_late_ref()
    wit = _load_witnesses()
    for r in rows:
        r['late_ref'] = (f"{late[r['work_id']]['year']} {late[r['work_id']]['title']}"
                         if r['work_id'] in late else None)
        r['maagarim_witness'] = wit.get(r['work_id'], {}).get(r['sys_id'])
    print(f"prep done ({time.time()-t0:.0f}s)", flush=True)

    psw = load_psw([(r['page_id'], r['work_id']) for r in rows])
    for r in rows:
        p = psw.get((r['page_id'], r['work_id']))
        r['p_same_work'], r['margin_band'] = (p if p else (None, None))
    print(f"p_same_work joined ({time.time()-t0:.0f}s)", flush=True)

    cr = CanonRarity()
    cs = _canon_scores([(r['page_id'], r['work_id']) for r in rows], cr)
    for r in rows:
        r['canon_mass'] = cs.get((r['page_id'], r['work_id']), 0.0)
    print(f"canon mass computed ({time.time()-t0:.0f}s)", flush=True)

    npg = load_npages()
    sg = ScopeGate(n_pages=npg)
    for r in rows:
        claim = f"{r.get('author') or ''} — {r.get('title') or ''}"
        sc = sg.scope(r['sys_id'])
        r['regime'] = sc['regime']
        r['resolution'] = sg.resolution(r['sys_id'], claim)
    print(f"scope resolved ({time.time()-t0:.0f}s; {len(sg._cache)} MSS)", flush=True)

    # --- Targum same-book sibling detection (fix B) ---
    # Two DIFFERENT Targumim of the SAME book on one manuscript (Onkelos +
    # Pseudo-Jonathan on Exodus; Targum + Targum Sheni on Esther) share the
    # verse text, so the subordinate one surfaces as a false "discovery". Keep
    # the manuscript's PRIMARY Targum (most matched text); relabel the rest
    # 'parallel'. A lone Targum of a book (Targum Sheni on Esther with no plain
    # Esther Targum on the ms) stays a discovery — no sibling to be parallel to.
    tg_letters = defaultdict(int)      # (sys_id, book, family) -> total letters
    tg_families = defaultdict(set)     # (sys_id, book) -> {family}
    for r in rows:
        tk = _targum_key(r['work_id']) if r['cat'] == 'Targum' else None
        r['_tg'] = tk
        if tk:
            fam, book = tk
            tg_letters[(r['sys_id'], book, fam)] += r['matched_letters']
            tg_families[(r['sys_id'], book)].add(fam)
    tg_primary = {}                    # (sys_id, book) -> primary family
    for (sid, book), fams in tg_families.items():
        if len(fams) >= 2:
            tg_primary[(sid, book)] = max(
                fams, key=lambda f: (tg_letters[(sid, book, f)], f))
    n_parallel = n_cat = 0
    for r in rows:
        tk = r['_tg']
        par = bool(tk and (r['sys_id'], tk[1]) in tg_primary
                   and tk[0] != tg_primary[(r['sys_id'], tk[1])])
        # catalogue-based sibling: the ms is catalogued as a DIFFERENT Pentateuch
        # Targum than the matched work -> the labelled Targum is a parallel
        # (the fragment really is the catalogued one).
        if tk and not par and tk[0] in _PENT_TARGUM_FAM:
            cf = _catalog_targum_family(sg.nli.get(r['sys_id'], ''))
            if cf in _PENT_TARGUM_FAM and cf != tk[0]:
                par = True
                n_cat += 1
        r['targum_parallel'] = par
        n_parallel += par
    print(f"targum siblings -> parallel: {n_parallel} rows "
          f"({len(tg_primary)} multi-targum (ms,book) groups; "
          f"{n_cat} via catalogue-Targum mismatch)", flush=True)

    dropped = 0
    out_rows = []
    for r in rows:
        if r['late_ref']:
            dropped += 1
            continue
        r['bucket'] = bucket_of(r)
        r['disc_score'], r['score_parts'] = score_row(r)
        out_rows.append(r)
    out_rows.sort(key=lambda r: -r['disc_score'])

    keep = ('page_id', 'sys_id', 'work_id', 'cat', 'genre', 'author', 'title',
            'matched_letters', 'best_density', 'p_same_work', 'margin_band',
            'canon_mass', 'work_nms', 'regime', 'resolution',
            'maagarim_witness', 'bucket', 'disc_score', 'score_parts')
    outp = os.path.join(PROBE, 'data', a.out + '.jsonl')
    with open(outp, 'w', encoding='utf-8') as f:
        for r in out_rows:
            f.write(json.dumps({k: r.get(k) for k in keep},
                               ensure_ascii=False) + '\n')

    # ---- report ----
    kept = len(out_rows)
    buck = Counter(r['bucket'] for r in out_rows)
    bands = Counter()
    for r in out_rows:
        s = r['disc_score']
        bands['>=0.5' if s >= .5 else '0.3-0.5' if s >= .3 else
              '0.15-0.3' if s >= .15 else '<0.15'] += 1
    disc_rows = [r for r in out_rows if r['bucket'] == 'discovery']
    hi = [r for r in disc_rows if r['disc_score'] >= 0.3]
    L = [f"# Discovery scoring — corpus-wide ({'FULL' if not a.limit else 'limit '+str(a.limit)})",
         "", f"- matches scored: {kept} (dropped {dropped} anachronistic)",
         f"- runtime {time.time()-t0:.0f}s", "",
         "## bucket mix (labels)", ""]
    for b in ('discovery', 'witness', 'known', 'parallel', 'other'):
        L.append(f"- {b}: {buck[b]} ({100*buck[b]//max(1,kept)}%)")
    L += ["", "## discovery-score bands (ALL matches)", ""]
    for k in ('>=0.5', '0.3-0.5', '0.15-0.3', '<0.15'):
        L.append(f"- {k}: {bands[k]}")
    L += ["", f"## the ranked discovery slice",
          f"- discovery-bucket rows: {len(disc_rows)}",
          f"- **high-confidence discovery (score >= 0.3): {len(hi)}** "
          f"({100*len(hi)//max(1,kept)}% of all matches) <- the small top slice",
          "", "## top 30 discovery candidates (work | ms | letters | score)", ""]
    for r in hi[:30]:
        L.append(f"- `{r['disc_score']:.2f}` {r['matched_letters']}L  "
                 f"{(r['title'] or '')[:38]}  · {r['sys_id']}  ({r['work_nms']} wit)")
    # grouped by work
    byw = defaultdict(list)
    for r in hi:
        byw[(r['work_id'], r['title'])].append(r)
    L += ["", "## discovery slice grouped by work (top 20 works by count)", ""]
    for (wid, ti), rs in sorted(byw.items(), key=lambda x: -len(x[1]))[:20]:
        L.append(f"- **{(ti or wid)[:44]}** — {len(rs)} fragments "
                 f"(best {max(x['disc_score'] for x in rs):.2f})")
    outmd = os.path.join(PROBE, 'results', a.out + '_report.md')
    open(outmd, 'w', encoding='utf-8').write('\n'.join(L) + '\n')
    print('\n'.join(L))
    print(f"\nwrote {outp} + {outmd}")


if __name__ == '__main__':
    main()

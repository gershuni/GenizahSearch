# -*- coding: utf-8 -*-
"""Track-1 identification review tool (Hillel feedback, 2026-07-07).

The testimonies HTML shows aggregates, not evidence. This builds a
grading-style review page (like the Round-3 pair-review tool) showing,
per (manuscript, work) identification, the EXACT matched page span
side-by-side with the corresponding passage of the Maagarim/JA edition.

Title-agreement triage (the user's insight): when the NLI catalog title
~matches the Maagarim work title the identification is almost surely
right -> auto-pass. Human review targets the rest:
  S1 new_witness   — tier 'new?' rows with generic/mismatched titles
                     (the discovery claims)
  S2 title_mismatch— testimony-tier, substantive catalog title that
                     DISAGREES with the identified work
  S3 generic_title — testimony-tier, catalog title generic (פיוט, קטעי
                     גניזה...) — identification adds information
  S4 partial_mismatch — partial-tier mismatches (sample)
  S5 match_control — sample of auto-passed rows (validates auto-pass)
  S6 canonical_control — sample of canonical testimonies

Reference window: re-derived per row by numpy diagonal vote of the page
span's grams against the work stream, then projected back onto the
ORIGINAL Maagarim/JA file text (streams are letters-only; unreadable raw).

Usage: python build_track1_review.py [db_path] [tag]
Out:   review/track1_<tag>_id_review.html
"""
import html
import json
import os
import pickle
import random
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict

import numpy as np
from rapidfuzz.distance import Levenshtein

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from engine_np import _gram_codes, K  # noqa: E402
from normalize import norm_stream  # noqa: E402
from track1_bib import FjmsInfo, load_acronym_equiv, title_bucket2  # noqa: E402

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
REF = ROOT + r"\same_work_spike\probe\data\ref_corpus.pkl"
OUT = ROOT + rf"\same_work_spike\probe\review\track1_{TAG}_id_review.html"

MAAGARIM = r"C:\Users\gersh\Dropbox\דיקטה\מאגרים\AllTextsOnlyText"
JA_DIR = r"C:\Users\gersh\Dropbox\דיקטה\JA\ערבית יהודית מעובד\per_doc"
HEADER_RE = re.compile(r'##[^#]*##')          # must mirror track1_build_ref
CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
T_TESTIMONY, T_PARTIAL = 0.45, 0.15           # mirror track1_testimonies
CITY_LIB = [
    ('Cambridge', 'CUL'), ('Oxford', 'Oxford'), ('Petersburg', 'RNL'),
    ('London', 'BL'), ('New York', 'JTS'), ('Paris', 'AIU'),
    ('Manchester', 'Manchester'), ('Strasbourg', 'Strasbourg'),
    ('Philadelphia', 'Katz'), ('Jerusalem', 'NLI'), ('Budapest', 'Kaufmann'),
    ('Vienna', 'Vienna'), ('Genève', 'Geneva'), ('Geneva', 'Geneva'),
]
CAPS = {'new_witness': 120, 'new_discussed': 30, 'title_mismatch': 100,
        'generic_title': 60, 'partial_mismatch': 40, 'match_control': 30,
        'canonical_control': 20}
random.seed(11)


def mesirah_tier(mesirah, shelfmark, lib):
    if not mesirah:
        return ''
    m_lib = next((code for city, code in CITY_LIB if city in mesirah), None)
    if m_lib and m_lib != lib:
        return 'new?'
    md = set(re.findall(r'\d+', mesirah))
    sd = set(re.findall(r'\d+', shelfmark))
    if md and sd and (sd <= md or md <= sd):
        return 'self?'
    return 'new?'


# ---------------- reference sources ----------------
def build_source_map():
    """work_id -> (kind, path) — mirrors track1_build_ref id derivation."""
    src = {}
    for fn in sorted(os.listdir(MAAGARIM)):
        if fn.endswith('.txt'):
            base = fn.replace('.txt-OnlyText.txt', '')
            parts = base.split('--')
            src[f'M:{parts[-1] if parts else fn}'] = \
                ('M', os.path.join(MAAGARIM, fn))
    for fn in sorted(os.listdir(JA_DIR)):
        if fn.endswith('.txt'):
            src[f'J:{fn[:-4]}'] = ('J', os.path.join(JA_DIR, fn))
    return src


def load_ref_text(kind, path):
    """(original_text, stream, offsets) — same normalization as ref build."""
    if kind == 'M':
        raw = open('\\\\?\\' + path, encoding='utf-8', errors='replace').read()
        text = HEADER_RE.sub(' ', raw)
    else:
        text = open(path, encoding='utf-8', errors='replace').read()
    stream, offs = norm_stream(text)
    return text, stream, offs


def locate_in_work(span_stream, wcodes_sorted, wpos_sorted, band=40):
    """Diagonal-vote the span's grams against the work; return (r0, r1)."""
    g = _gram_codes(span_stream)
    if not len(g):
        return None
    lo = np.searchsorted(wcodes_sorted, g, 'left')
    hi = np.searchsorted(wcodes_sorted, g, 'right')
    cnt = hi - lo
    sel = cnt > 0
    counts = cnt[sel]
    total = int(counts.sum())
    if not total:
        return None
    cum0 = np.cumsum(counts) - counts
    ridx = (np.repeat(lo[sel], counts)
            + (np.arange(total, dtype=np.int64) - np.repeat(cum0, counts)))
    wp = wpos_sorted[ridx]
    pp = np.repeat(np.flatnonzero(sel).astype(np.int64), counts)
    diag = (wp - pp) // band
    bmin = int(diag.min())
    votes = np.bincount((diag - bmin).astype(np.int64))
    b = int(np.argmax(votes))
    inb = (diag - bmin >= b - 1) & (diag - bmin <= b + 1)
    return int(wp[inb].min()), int(wp[inb].max()) + K


def seg3(text, offs, s0, s1, pad=110):
    """(before, match, after) original-text segments for a stream span."""
    if not len(offs):
        return ('', '', '')
    s1 = min(s1, len(offs))
    a = offs[max(0, min(s0, len(offs) - 1))]
    z = offs[s1 - 1] + 1
    return (text[max(0, a - pad):a], text[a:z], text[z:z + pad])


def pnum(pid):
    m = re.search(r'_P(\d+)_', pid)
    return int(m.group(1)) if m else 1


def main():
    t0 = time.time()
    works = pickle.load(open(REF, 'rb'))
    by_wid = {w['id']: w for w in works}
    src_map = build_source_map()
    print(f"ref: {len(works)} works, source paths resolved for "
          f"{sum(1 for w in works if w['id'] in src_map)} "
          f"({time.time() - t0:.0f}s)", flush=True)

    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        import csv as _csv
        r = _csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (variants[0] if variants else row[0],
                                row[3].strip() or '?', title)

    con = sqlite3.connect(DB)
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    live = ("WHERE shadowed_by IS NULL" if 'shadowed_by' in cols else "")
    rows = con.execute(f"""
        SELECT page_id, sys_id, work_id, cat, genre, author, title,
               mesirah, matched_letters, best_density, n_spans, spans_json
        FROM track1_matches {live}""").fetchall()
    plen = {}
    for pid, tx in con.execute(
            "SELECT p.page_id, p.text FROM pages p JOIN (SELECT DISTINCT "
            "page_id FROM track1_matches) t ON p.page_id = t.page_id"):
        plen[pid] = len(norm_stream(tx)[0])
    print(f"match rows: {len(rows):,}; matched pages: {len(plen):,} "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- FJMS enrichment: catalog titles + bibliography (AlmaId==sys_id) --
    all_sys = {r[1] for r in rows}
    fjms = FjmsInfo(all_sys)
    equiv = load_acronym_equiv()
    print(f"fjms: titles for {len(fjms.titles):,} MSS, bib for "
          f"{len(fjms.bib):,} MSS; {len(equiv):,} acronym-equiv tokens "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- aggregate to (manuscript, work); keep the best evidence page ----
    ms_work = {}
    for r in rows:
        cov = r[8] / max(1, plen.get(r[0], 1))
        key = (r[1], r[2])
        a = ms_work.setdefault(key, {
            'pages': 0, 'letters': 0, 'best_cov': 0.0, 'best_d': 1.0,
            'best_row': None, 'best_letters': -1})
        a['pages'] += 1
        a['letters'] += r[8]
        a['best_cov'] = max(a['best_cov'], cov)
        a['best_d'] = min(a['best_d'], r[9])
        if r[8] > a['best_letters']:
            a['best_letters'] = r[8]
            a['best_row'] = r
            a['best_row_cov'] = cov

    cands = []
    triage = Counter()
    for (sid, wid), a in ms_work.items():
        r = a['best_row']
        sm, lib, cat_title = meta.get(sid, (sid, '?', ''))
        cls = ('testimony' if a['best_cov'] >= T_TESTIMONY
               else 'partial' if a['best_cov'] >= T_PARTIAL else 'citation')
        tier = mesirah_tier(r[7], sm, lib) if cls != 'citation' else ''
        cat_titles = [cat_title] + fjms.titles.get(sid, [])
        tb = title_bucket2(cat_titles, r[5], r[6], equiv)
        is_canon = r[3] in CANON_CATS
        triage[(cls, 'canon' if is_canon else 'edited', tb)] += 1
        bib_sig, bib_entry = ('', '')
        if tier == 'new?':
            bib_sig, bib_entry = fjms.bib_signal(sid, r[5], r[6], equiv)
        stratum = None
        if not is_canon and tier == 'new?' and cls in ('testimony',
                                                       'partial'):
            stratum = 'new_discussed' if bib_sig else 'new_witness'
        elif not is_canon and cls == 'testimony' and tb == 'mismatch':
            stratum = 'title_mismatch'
        elif not is_canon and cls == 'testimony' and tb == 'generic':
            stratum = 'generic_title'
        elif not is_canon and cls == 'partial' and tb == 'mismatch':
            stratum = 'partial_mismatch'
        elif not is_canon and cls == 'testimony' and tb == 'match':
            stratum = 'match_control'
        elif is_canon and cls == 'testimony':
            stratum = 'canonical_control'
        if stratum:
            cands.append({
                'stratum': stratum, 'sys': sid, 'wid': wid, 'shelf': sm,
                'lib': lib, 'cat_title': cat_title, 'cls': cls, 'tier': tier,
                'tbucket': tb, 'pages': a['pages'], 'letters': a['letters'],
                'cov': round(a['best_cov'], 3), 'row': r,
                'bib_sig': bib_sig, 'bib_entry': bib_entry,
            })

    by_str = defaultdict(list)
    for c in cands:
        by_str[c['stratum']].append(c)
    sample = []
    for s, cap in CAPS.items():
        pool = by_str.get(s, [])
        if s in ('new_witness', 'title_mismatch', 'new_discussed'):
            pool = sorted(pool, key=lambda c: -c['letters'])[:cap]
        else:
            pool = random.sample(pool, min(cap, len(pool)))
        sample.extend(pool)
    print("strata:", {s: len([c for c in sample if c['stratum'] == s])
                      for s in CAPS}, flush=True)

    # ---- build evidence panes (grouped by work: encode each work once) ----
    items = []
    n_noref = 0
    by_work_rows = defaultdict(list)
    for c in sample:
        by_work_rows[c['wid']].append(c)
    for wi, (wid, group) in enumerate(sorted(by_work_rows.items())):
        w = by_wid.get(wid)
        kind_path = src_map.get(wid)
        wtext = wstream = woffs = None
        wcodes_sorted = wpos_sorted = None
        if w is not None:
            if kind_path:
                try:
                    wtext, wstream, woffs = load_ref_text(*kind_path)
                    if wstream != w['stream']:
                        wtext = None   # drift: fall back to stream display
                except OSError:
                    wtext = None
            codes = _gram_codes(w['stream'])
            if len(codes):
                order = np.argsort(codes, kind='stable')
                wcodes_sorted = codes[order]
                wpos_sorted = np.arange(len(codes), dtype=np.int64)[order]
        for c in group:
            r = c['row']
            page_row = con.execute(
                "SELECT text FROM pages WHERE page_id=?", (r[0],)).fetchone()
            ptext = page_row[0] if page_row else ''
            pstream, poffs = norm_stream(ptext)
            spans = json.loads(r[11])
            p0, p1, dens = max(spans, key=lambda s: s[1] - s[0])
            pseg = seg3(ptext, poffs, int(p0), int(p1))
            rseg, rdens = ('', '', ''), None
            if wcodes_sorted is not None:
                loc = locate_in_work(pstream[int(p0):int(p1)],
                                     wcodes_sorted, wpos_sorted)
                if loc:
                    r0 = max(0, loc[0] - 25)
                    r1 = min(len(w['stream']), loc[1] + 25)
                    alen = max(p1 - p0, r1 - r0)
                    d = Levenshtein.distance(pstream[int(p0):int(p1)],
                                             w['stream'][r0:r1])
                    rdens = round(d / max(1, alen), 3)
                    if wtext is not None:
                        rseg = seg3(wtext, woffs, r0, r1)
                    else:
                        rseg = ('', w['stream'][r0:r1] + '  [letter-stream]',
                                '')
                else:
                    n_noref += 1
            bibs = fjms.bib.get(c['sys'], [])
            bib_list = []
            for pub, art, auth, mtype, ttype, page in bibs[:6]:
                lbl = pub or art
                if art and pub:
                    lbl += f" — {art}"
                if auth:
                    lbl += f" ({auth})"
                tag = ttype if ttype in ('Full', 'Partial') else mtype
                bib_list.append(
                    f"{lbl} [{tag}{', p. ' + page if page else ''}]")
            items.append({
                'id': f"{c['sys']}|{wid}",
                'stratum': c['stratum'], 'tbucket': c['tbucket'],
                'cls': c['cls'], 'tier': c['tier'],
                'bib_sig': c['bib_sig'], 'bib_entry': c['bib_entry'][:110],
                'bib_n': len(bibs), 'bib_list': bib_list,
                'fjms_titles': fjms.titles.get(c['sys'], [])[:3],
                'cov': c['cov'], 'pages': c['pages'],
                'letters': c['letters'], 'span_dens': dens, 'ref_dens': rdens,
                'a': {'pid': r[0], 'sys': c['sys'], 'shelf': c['shelf'],
                      'lib': c['lib'], 'title': c['cat_title'][:120],
                      'page': pnum(r[0]), 'seg': pseg},
                'b': {'wid': wid, 'cat': r[3], 'genre': r[4], 'author': r[5],
                      'work': r[6], 'mesirah': r[7][:120], 'seg': rseg},
            })
        if (wi + 1) % 25 == 0:
            print(f"  works {wi + 1}/{len(by_work_rows)} "
                  f"items={len(items)} ({time.time() - t0:.0f}s)", flush=True)
    con.close()
    fjms.close()

    order = {s: i for i, s in enumerate(CAPS)}
    items.sort(key=lambda d: (order[d['stratum']], -d['letters']))
    tri_lines = []
    for cls in ('testimony', 'partial', 'citation'):
        for grp in ('edited', 'canon'):
            row = {tb: triage.get((cls, grp, tb), 0)
                   for tb in ('match', 'generic', 'mismatch')}
            if sum(row.values()):
                tri_lines.append(f"{cls}/{grp}: match {row['match']:,} · "
                                 f"generic {row['generic']:,} · "
                                 f"mismatch {row['mismatch']:,}")
    print(f"review items: {len(items)} (ref window missing: {n_noref})",
          flush=True)

    doc = TEMPLATE.replace('__DATA__', json.dumps(items, ensure_ascii=False))
    doc = doc.replace('__TRIAGE__', html.escape(' | '.join(tri_lines)))
    doc = doc.replace('__TAG__', TAG)
    open(OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT} ({len(doc) // 1024} KB, {time.time() - t0:.0f}s)")


TEMPLATE = r"""<!DOCTYPE html>
<html lang="he">
<head>
<meta charset="utf-8">
<title>Track-1 ID review — __TAG__</title>
<style>
 body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#17181c;color:#d6d6d6}
 header{position:sticky;top:0;background:#20262b;color:#eee;padding:8px 16px;
        display:flex;gap:14px;align-items:center;flex-wrap:wrap;z-index:5;
        border-bottom:1px solid #33393f}
 header .prog{font-weight:600}
 header select,header button{padding:4px 8px;border-radius:6px;border:1px solid #444;
        background:#2e3138;color:#ddd}
 .tri{font-size:11px;color:#8fa3ad;width:100%}
 .card{max-width:1250px;margin:14px auto;background:#23252c;border-radius:10px;
       box-shadow:0 1px 6px rgba(0,0,0,.5);padding:14px 18px}
 .meta{display:flex;gap:12px;flex-wrap:wrap;font-size:14px;color:#aaa;
       margin-bottom:8px;align-items:center}
 .meta b{color:#fff}
 .badge{background:#3a3d46;color:#ddd;border-radius:10px;padding:1px 9px;font-size:12px}
 .badge.new{background:#1565c0;color:#fff}
 .badge.mis{background:#c62828;color:#fff}
 .badge.gen{background:#ef6c00;color:#fff}
 .cols{display:flex;gap:14px;flex-wrap:wrap}
 .pane{flex:1 1 460px;border:1px solid #3a3d46;border-radius:8px;padding:10px;
       background:#1d1f25}
 .pane h4{margin:0 0 6px;font-size:13px;color:#9aa4ac;direction:rtl;text-align:right}
 .pane h4 b{color:#e8e6df}
 .txt{direction:rtl;text-align:right;font-size:17px;line-height:1.75;
      white-space:pre-wrap;word-break:break-word;max-height:340px;overflow-y:auto;
      color:#e8e6df}
 .txt mark{background:#6b5407;color:#ffe082;padding:0 1px}
 .ctx{color:#6d6d6d}
 .grades{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
 .grades button{padding:8px 12px;border-radius:8px;border:1px solid #4a4d55;
      background:#2e3138;color:#ddd;cursor:pointer;font-size:14px}
 .grades button.sel{background:#2c7d32;color:#fff;border-color:#2c7d32}
 .nav{display:flex;gap:10px;margin-top:10px}
 .nav button{padding:6px 14px;border-radius:8px;border:1px solid #555;
      cursor:pointer;background:#2e3138;color:#ddd}
 a{color:#6fb3e8}
 .hint{font-size:12px;color:#8a8a8a;margin-top:6px}
 details.bib{margin-top:10px;font-size:13px;color:#b8b8b8;direction:rtl;
      text-align:right;background:#1d1f25;border:1px solid #3a3d46;
      border-radius:8px;padding:6px 10px}
 details.bib summary{cursor:pointer;color:#9ec7e8}
 details.bib ul{margin:6px 18px 2px 0;padding:0}
 .fjt{font-size:12px;color:#9aa4ac;direction:rtl;text-align:right;margin-top:8px}
</style>
</head>
<body>
<header>
 <span class="prog" id="prog"></span>
 <select id="filter"></select>
 <button onclick="jumpUngraded()">Next ungraded</button>
 <button onclick="exportGrades()">⬇ Export grades</button>
 <span id="counts" style="font-size:12px"></span>
 <span class="tri">triage (all (MS,work) rows): __TRIAGE__</span>
</header>
<div id="app"></div>
<script>
const DATA = __DATA__;
const GRADES = [
 ["correct","1 · Correct ID / הזיהוי נכון"],
 ["citation","2 · Real match, embedded quote / ציטוט בתוך חיבור אחר"],
 ["formula","3 · Formulaic overlap / חפיפה פורמולרית בלבד"],
 ["wrong","4 · Wrong ID / זיהוי שגוי"],
 ["junk","5 · Junk page / דף פסול"],
 ["unsure","6 · Unsure / לא בטוח"]];
const LS_KEY = "seed029_track1_id_grades_v1";
let grades = JSON.parse(localStorage.getItem(LS_KEY) || "{}");
let idx = 0, filt = "all";

function items(){ return filt==="all" ? DATA : DATA.filter(d=>d.stratum===filt); }
function save(){ localStorage.setItem(LS_KEY, JSON.stringify(grades)); }
function esc(s){ return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;"); }
function paneA(a){
 const [b,m,af] = a.seg;
 const url = `https://genizahsearch.com/browse?sys_id=${a.sys}&page=${a.page}`;
 return `<div class="pane"><h4>כתב־יד: <b>${esc(a.shelf)}</b> · ${a.lib}
   · עמ' ${a.page} · <a href="${url}" target="_blank">פתח ↗</a><br>
   כותרת קטלוג: <b>${esc(a.title)||"—"}</b></h4>
  <div class="txt"><span class="ctx">${esc(b)}</span><mark>${esc(m)}</mark><span class="ctx">${esc(af)}</span></div></div>`;
}
function paneB(b){
 const [bb,m,af] = b.seg;
 const head = `[${b.cat}] ${b.author?esc(b.author)+" — ":""}<b>${esc(b.work)}</b>`
   + (b.genre?` · ${esc(b.genre)}`:"")
   + (b.mesirah?`<br>מסירה: <span style="color:#1565c0">${esc(b.mesirah)}</span>`:"");
 const body = m ? `<span class="ctx">${esc(bb)}</span><mark>${esc(m)}</mark><span class="ctx">${esc(af)}</span>`
                : `<i style="color:#999">ref window not located</i>`;
 return `<div class="pane"><h4>מהדורה: ${head}</h4>
  <div class="txt">${body}</div></div>`;
}
function render(){
 const list = items();
 if(!list.length){ document.getElementById("app").innerHTML =
   "<div class='card'>No items in this filter.</div>"; return; }
 idx = Math.max(0, Math.min(idx, list.length-1));
 const d = list[idx];
 const g = grades[d.id];
 document.getElementById("prog").textContent =
   `${idx+1} / ${list.length}  (${Object.keys(grades).length} graded)`;
 let btns = GRADES.map(([k,lab]) =>
   `<button class="${g===k?'sel':''}" onclick="grade('${k}')">${lab}</button>`).join("");
 const tb = {match:"",generic:"badge gen",mismatch:"badge mis"}[d.tbucket]||"";
 document.getElementById("app").innerHTML = `
  <div class="card">
   <div class="meta">
    <span class="badge">${d.stratum}</span>
    <span class="badge">${d.cls}</span>
    ${d.tier ? `<span class="badge new">tier ${d.tier}</span>` : ``}
    ${d.bib_sig==="transcribed" ? `<span class="badge mis">כבר נדפס</span>` :
      d.bib_sig==="discussed" ? `<span class="badge gen">נדון במחקר</span>` : ``}
    ${d.bib_n ? `<span class="badge">bib ${d.bib_n}</span>` : ``}
    ${d.tbucket!=="match" ? `<span class="${tb||'badge'}">title ${d.tbucket}</span>` : ``}
    <span>coverage <b>${d.cov}</b></span>
    <span><b>${d.pages}</b> pages</span>
    <span><b>${d.letters.toLocaleString()}</b> letters</span>
    <span>span dens <b>${d.span_dens}</b></span>
    ${d.ref_dens!==null ? `<span>window dens <b>${d.ref_dens}</b></span>` : ``}
   </div>
   <div class="cols">${paneA(d.a)}${paneB(d.b)}</div>
   ${d.fjms_titles.length ? `<div class="fjt">כותרות FJMS: ${d.fjms_titles.map(esc).join(" · ")}</div>` : ``}
   ${d.bib_list.length ? `<details class="bib"><summary>ביבליוגרפיה (${d.bib_n})${d.bib_entry ? " — " + esc(d.bib_entry) : ""}</summary><ul>${d.bib_list.map(x=>`<li>${esc(x)}</li>`).join("")}</ul></details>` : ``}
   <div class="grades">${btns}</div>
   <div class="nav">
    <button onclick="move(-1)">← Prev</button>
    <button onclick="move(1)">Next →</button>
   </div>
   <div class="hint">Keys: 1–6 grade &amp; advance · ←/→ navigate. שמאל =
     קטע הדף (HTR); ימין = החלון המקביל במהדורה. ההדגשה = המובאה שהמכונה
     זיהתה. השאלה: האם הדף הזה אכן נושא את החיבור שזוהה?</div>
  </div>`;
}
function grade(k){ const d=items()[idx]; grades[d.id]=k; save(); move(1); }
function move(dd){ idx+=dd; render(); }
function jumpUngraded(){
 const list=items();
 for(let i=0;i<list.length;i++){
   const j=(idx+1+i)%list.length;
   if(!grades[list[j].id]){ idx=j; render(); return; } }
 alert("All graded in this filter!");
}
function exportGrades(){
 const out = DATA.filter(d=>grades[d.id]).map(d=>({
   id:d.id, stratum:d.stratum, tbucket:d.tbucket, cls:d.cls, tier:d.tier,
   cov:d.cov, letters:d.letters, grade:grades[d.id]}));
 const blob = new Blob([JSON.stringify(out,null,1)],{type:"application/json"});
 const a = document.createElement("a");
 a.href = URL.createObjectURL(blob);
 a.download = "track1_id_grades.json"; a.click();
}
document.addEventListener("keydown", e=>{
 if(e.key>="1" && e.key<="6"){ grade(GRADES[+e.key-1][0]); }
 else if(e.key==="ArrowLeft"){ move(1); }   /* RTL-friendly: left = forward */
 else if(e.key==="ArrowRight"){ move(-1); }
});
const strata = ["all",...new Set(DATA.map(d=>d.stratum))];
document.getElementById("filter").innerHTML =
 strata.map(s=>`<option value="${s}">${s} (${s==="all"?DATA.length:DATA.filter(d=>d.stratum===s).length})</option>`).join("");
document.getElementById("filter").onchange = e=>{ filt=e.target.value; idx=0; render(); };
render();
</script>
</body>
</html>"""


if __name__ == '__main__':
    main()

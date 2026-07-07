# -*- coding: utf-8 -*-
"""Build the human-review HTML tool for the SEED-029 probe.

Samples ~270 pairs across 5 strata, recomputes span offsets per pair,
projects them back onto the original HTR text, and emits a single
self-contained review.html (RTL, keyboard grading, localStorage, export).

Output: same_work_spike/probe/review/review.html (+ review_data.json)
"""
import csv
import json
import random
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402
from rapidfuzz.distance import Levenshtein  # noqa: E402

ROOT = r"C:\Genizahsearch"
PAIRS = ROOT + r"\same_work_spike\probe\results\verified_pairs_d50_cap1.json"
TIER1 = ROOT + r"\same_work_spike\probe\results\tier1.json"
BH = ROOT + r"\same_work_spike\probe\data\bh_witnesses.json"
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
OUT_DIR = ROOT + r"\same_work_spike\probe\review"
random.seed(7)

# ---------- shelfmark lookup ----------
def _norm_shelf(s):
    t = re.sub(r'(\d)\.(\d)', r'\1DOT\2', s.replace('/', '.'))
    t = re.sub(r'\W+', '', t).casefold().replace('dot', '.')
    return t[2:] if t.startswith('ms') else t


shelf = {}
shelf_variants = {}  # sys_id -> set of normalized call-number variants
with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
    r = csv.reader(f)
    next(r, None)
    for row in r:
        if len(row) >= 4 and row[0]:
            variants = [v.strip() for v in (row[2] or '').split('|') if v.strip()]
            shelf[row[0]] = variants[0] if variants else row[0]
            shelf_variants[row[0]] = {_norm_shelf(v) for v in variants if v}

# ---------- BH witness map ----------
bh = json.load(open(BH, encoding='utf-8'))
bh_sys = set()
for sig, w in bh['witnesses'].items():
    for sm in w['shelfmarks']:
        bh_sys.update(sm.get('sys_ids', []))

# ---------- load pairs ----------
pairs = json.load(open(PAIRS, encoding='utf-8'))
tier1 = json.load(open(TIER1, encoding='utf-8'))

# ---------- sampling ----------
def is_bh_pair(p):
    return (p['a'].split('_')[0] in bh_sys and p['b'].split('_')[0] in bh_sys)


strata = defaultdict(list)
for p in pairs:
    if p['cls'] == 'duplicate':
        continue
    ln, d = p['len'], p['density']
    if 100 <= ln < 300 and 0.35 <= d <= 0.45 and p['cls'] in ('cross', 'related_new'):
        strata[f"overlap_{p['cls']}"].append(p)
    if p['cls'] == 'cross' and d <= 0.35 and ln >= 300:
        strata['discovery'].append(p)
    if is_bh_pair(p) and 0.30 < d <= 0.42:
        strata['bh_boundary'].append(p)
    if 25 <= ln < 60 and d <= 0.30:
        strata['short_span'].append(p)

sample = []
def take(name, items, n, sort_key=None):
    items = sorted(items, key=sort_key) if sort_key else random.sample(
        items, min(n, len(items)))
    for p in items[:n]:
        sample.append((name, p))


take('overlap_cross', strata['overlap_cross'], 60)
take('overlap_related', strata['overlap_related_new'], 60)
take('discovery', strata['discovery'], 40, sort_key=lambda p: -p['len'])
take('bh_boundary', strata['bh_boundary'], 40)
take('short_span', strata['short_span'], 30)
for p in tier1.get('joins', []):
    sample.append(('join_anomaly', {'a': p['a'], 'b': p['b'],
                                    'len': p['aligned_len'],
                                    'density': p['density'], 'cls': 'tier1_joins'}))

# dedupe (a pair can hit two strata)
seen = set()
final = []
for name, p in sample:
    key = tuple(sorted((p['a'], p['b'])))
    if key in seen:
        continue
    seen.add(key)
    final.append((name, p))
print(f"sampled {len(final)} pairs "
      f"({ {k: len(v) for k, v in strata.items()} })")

# ---------- span recomputation + projection ----------
con = sqlite3.connect(PROBE_DB)
_page_cache = {}


def get_page(pid):
    if pid not in _page_cache:
        row = con.execute("SELECT text FROM pages WHERE page_id=?",
                          (pid,)).fetchone()
        t = unicodedata.normalize('NFC', row[0]) if row else ''
        _page_cache[pid] = (t,) + norm_stream(t)
    return _page_cache[pid]


def pair_extents(sa, sb, k=5, band=20):
    grams_a = defaultdict(list)
    for i in range(len(sa) - k + 1):
        grams_a[sa[i:i + k]].append(i)
    buckets = {}
    for j in range(len(sb) - k + 1):
        for i in grams_a.get(sb[j:j + k], ())[:4]:
            bkt = (i - j) // band
            rec = buckets.get(bkt)
            if rec is None:
                buckets[bkt] = [1, i, i, j, j]
            else:
                rec[0] += 1
                rec[1] = min(rec[1], i)
                rec[2] = max(rec[2], i)
                rec[3] = min(rec[3], j)
                rec[4] = max(rec[4], j)
    best_c, best = 0, None
    for bkt in buckets:
        cl = [buckets[x] for x in (bkt - 1, bkt, bkt + 1) if x in buckets]
        c = sum(r[0] for r in cl)
        if c > best_c:
            best_c, best = c, (
                min(r[1] for r in cl), max(r[2] for r in cl),
                min(r[3] for r in cl), max(r[4] for r in cl))
    return best


def line_agreement(ma, mb):
    """Same-page-photographed-twice detector: fraction of HTR LINES that
    match near-identically IN ORDER between the two matched spans.
    Line breaks are physical-page properties — genuine parallel witnesses
    never agree on them; a re-photographed page must."""
    la = [norm_stream(x)[0] for x in ma.split('\n')]
    lb = [norm_stream(x)[0] for x in mb.split('\n')]
    la = [x for x in la if len(x) >= 10]
    lb = [x for x in lb if len(x) >= 10]
    if min(len(la), len(lb)) < 4:   # short pages: accidental agreement risk
        return 0.0, 0
    j = matched = 0
    for a in la:
        for jj in range(j, min(j + 3, len(lb))):
            b = lb[jj]
            if Levenshtein.normalized_distance(a, b) <= 0.30:
                matched += 1
                j = jj + 1
                break
    return round(matched / max(len(la), len(lb)), 3), matched


def seg3(text, offs, s0, s1, pad=110):
    """Return (before, match, after) original-text segments for stream span."""
    if not len(offs):
        return ('', '', '')
    s1 = min(s1, len(offs))
    a = offs[max(0, min(s0, len(offs) - 1))]
    z = offs[s1 - 1] + 1
    return (text[max(0, a - pad):a], text[a:z], text[z:z + pad])


items = []
for name, p in final:
    ta, sa, oa = get_page(p['a'])
    tb, sb, ob = get_page(p['b'])
    ext = pair_extents(sa, sb)
    if not ext:
        continue
    ia0, ia1, jb0, jb1 = ext
    m = 30
    a0, a1 = max(0, ia0 - m), min(len(sa), ia1 + 5 + m)
    b0, b1 = max(0, jb0 - m), min(len(sb), jb1 + 5 + m)
    dist = Levenshtein.distance(sa[a0:a1], sb[b0:b1])
    dens = round(dist / max(len(sa[a0:a1]), len(sb[b0:b1]), 1), 3)
    sysa, sysb = p['a'].split('_')[0], p['b'].split('_')[0]

    def pnum(pid):
        mm = re.search(r'_P(\d+)_', pid)
        return int(mm.group(1)) if mm else 1

    seg_a = seg3(ta, oa, a0, a1)
    seg_b = seg3(tb, ob, b0, b1)
    agree, n_lines = line_agreement(seg_a[1], seg_b[1])
    same_shelf = bool(shelf_variants.get(sysa, set()) &
                      shelf_variants.get(sysb, set()))
    items.append({
        'id': f"{p['a']}|{p['b']}",
        'stratum': name,
        'cls': p['cls'],
        'len': max(a1 - a0, b1 - b0),
        'density': dens,
        'dup_lines': agree, 'dup_nlines': n_lines,
        'dup_shelf': int(same_shelf),
        'a': {'pid': p['a'], 'sys': sysa, 'shelf': shelf.get(sysa, sysa),
              'page': pnum(p['a']), 'seg': seg_a},
        'b': {'pid': p['b'], 'sys': sysb, 'shelf': shelf.get(sysb, sysb),
              'page': pnum(p['b']), 'seg': seg_b},
    })

import os
os.makedirs(OUT_DIR, exist_ok=True)
json.dump(items, open(OUT_DIR + r"\review_data.json", 'w', encoding='utf-8'),
          ensure_ascii=False)
print(f"built {len(items)} review items")

# ---------- HTML ----------
TEMPLATE = r"""<!DOCTYPE html>
<html lang="he">
<head>
<meta charset="utf-8">
<title>SEED-029 Pair Review</title>
<style>
 body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f4f2ec;color:#222}
 header{position:sticky;top:0;background:#2c3e50;color:#fff;padding:8px 16px;
        display:flex;gap:16px;align-items:center;flex-wrap:wrap;z-index:5}
 header .prog{font-weight:600}
 header select,header button{padding:4px 8px;border-radius:6px;border:none}
 .card{max-width:1200px;margin:14px auto;background:#fff;border-radius:10px;
       box-shadow:0 1px 4px rgba(0,0,0,.15);padding:14px 18px}
 .meta{display:flex;gap:14px;flex-wrap:wrap;font-size:14px;color:#444;
       margin-bottom:8px;align-items:center}
 .meta b{color:#000}
 .badge{background:#eee;border-radius:10px;padding:1px 9px;font-size:12px}
 .cols{display:flex;gap:14px;flex-wrap:wrap}
 .pane{flex:1 1 440px;border:1px solid #ddd;border-radius:8px;padding:10px}
 .pane h4{margin:0 0 6px;font-size:13px;color:#555;direction:ltr}
 .txt{direction:rtl;text-align:right;font-size:17px;line-height:1.75;
      white-space:pre-wrap;word-break:break-word}
 .txt mark{background:#ffe58a;padding:0 1px}
 .ctx{color:#999}
 .grades{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
 .grades button{padding:8px 12px;border-radius:8px;border:1px solid #bbb;
      background:#fafafa;cursor:pointer;font-size:14px}
 .grades button.sel{background:#2c7d32;color:#fff;border-color:#2c7d32}
 .nav{display:flex;gap:10px;margin-top:10px}
 .nav button{padding:6px 14px;border-radius:8px;border:1px solid #888;
      cursor:pointer;background:#fff}
 a{color:#1a5da6}
 .hint{font-size:12px;color:#777;margin-top:6px}
</style>
</head>
<body>
<header>
 <span class="prog" id="prog"></span>
 <select id="filter"></select>
 <button onclick="jumpUngraded()">Next ungraded</button>
 <button onclick="exportGrades()">⬇ Export grades</button>
 <span id="counts" style="font-size:12px"></span>
</header>
<div id="app"></div>
<script>
const DATA = __DATA__;
const GRADES = [
 ["verbatim","1 · Verbatim / זהה"],
 ["near_verbatim","2 · Near-verbatim / כמעט זהה"],
 ["paraphrase","3 · Paraphrase / ניסוח שונה"],
 ["shared_formula","4 · Shared formula / נוסחה משותפת (כתובות, שטרות)"],
 ["topical","5 · Topical only / דמיון נושאי"],
 ["unrelated","6 · Unrelated / לא קשור"],
 ["junk","7 · Junk page / דף פסול"],
 ["canonical","8 · Canonical quote / ציטוט מקרא-חז\"ל-תפילה"],
 ["duplicate_photo","9 · Duplicate photo / אותו דף שצולם פעמיים"]];
const LS_KEY = "seed029_review_grades_v1";
let grades = JSON.parse(localStorage.getItem(LS_KEY) || "{}");
let idx = 0, filt = "all";

function items(){ return filt==="all" ? DATA : DATA.filter(d=>d.stratum===filt); }
function save(){ localStorage.setItem(LS_KEY, JSON.stringify(grades)); }
function esc(s){ return s.replace(/&/g,"&amp;").replace(/</g,"&lt;"); }
function pane(side,label){
 const [b,m,a] = side.seg;
 const url = `https://genizahsearch.com/browse?sys_id=${side.sys}&page=${side.page}`;
 return `<div class="pane"><h4>${label}: <b>${esc(side.shelf)}</b>
   · p.${side.page} · <a href="${url}" target="_blank">open ↗</a>
   <span style="color:#aaa">${side.sys}</span></h4>
  <div class="txt"><span class="ctx">${esc(b)}</span><mark>${esc(m)}</mark><span class="ctx">${esc(a)}</span></div></div>`;
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
 document.getElementById("app").innerHTML = `
  <div class="card">
   <div class="meta">
    <span class="badge">${d.stratum}</span>
    <span class="badge">${d.cls}</span>
    <span><b>${d.len}</b> letters</span>
    <span>density <b>${d.density}</b></span>
    ${d.dup_lines>=0.6 ? `<span class="badge" style="background:#c62828;color:#fff">⚠ same page? line-match ${Math.round(d.dup_lines*100)}% (${d.dup_nlines} lines)</span>` : ``}
    ${d.dup_shelf ? `<span class="badge" style="background:#e65100;color:#fff">⚠ same shelfmark</span>` : ``}
   </div>
   <div class="cols">${pane(d.a,"A")}${pane(d.b,"B")}</div>
   <div class="grades">${btns}</div>
   <div class="nav">
    <button onclick="move(-1)">← Prev</button>
    <button onclick="move(1)">Next →</button>
   </div>
   <div class="hint">Keys: 1–8 grade &amp; advance · ←/→ navigate ·
     the highlighted span is the machine-matched passage; context in gray.
     Rule: grade the RELATIONSHIP of the shared span — 1–3 = same-work /
     citation evidence · 4 = documentary/liturgical boilerplate · 8 =
     scripture/canon quotation · 5–7 = the match is spurious.</div>
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
   id:d.id, stratum:d.stratum, cls:d.cls, len:d.len, density:d.density,
   grade:grades[d.id]}));
 const blob = new Blob([JSON.stringify(out,null,1)],{type:"application/json"});
 const a = document.createElement("a");
 a.href = URL.createObjectURL(blob);
 a.download = "seed029_grades.json"; a.click();
}
document.addEventListener("keydown", e=>{
 if(e.key>="1" && e.key<="9"){ grade(GRADES[+e.key-1][0]); }
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

html = TEMPLATE.replace('__DATA__', json.dumps(items, ensure_ascii=False))
open(OUT_DIR + r"\review.html", 'w', encoding='utf-8').write(html)
print(f"wrote {OUT_DIR}\\review.html ({len(html)//1024} KB)")

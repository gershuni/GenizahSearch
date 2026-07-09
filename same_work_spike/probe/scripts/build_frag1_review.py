# -*- coding: utf-8 -*-
"""FRAG-1 short-bin grading reviewer (Hillel, 2026-07-09).

Builds a dark-mode, keyboard-driven grading page over the REAL FRAG-1
card set, drawn with the SAME seed/config as frag1_truncation.py so it is
consistent with results/frag1_truncation.md. All card selection reuses the
frag1_truncation.py functions directly (build_reference, compute_page_lengths,
sample_unidentified_pages, fetch_streams, query_batch, classify_failure,
sample_track1_pages, make_crops) -- nothing is reimplemented.

Four card types:
  no_reference   -- ~12 real unidentified short pages the classifier put in
                    no_reference_covers_it (THE gate: can Hillel name the work?)
  density_fail   -- ~10 pages with a verifying-but-over-boundary candidate
  ambiguous      -- the 3 ambiguous pages (2+ competing candidates)
  crop_recovered -- ~20 60-100-letter crops the engine re-identified as their
                    true source work W (validates truncation precision)

CONCURRENCY: work_query.py (A2) is writing fullcorpus.db. Every sqlite
connection sets PRAGMA busy_timeout=120000 immediately; reads are light/
batched; no long read transaction; work_query_* tables untouched.

Usage: python -X utf8 -u build_frag1_review.py
Out:   review/frag1_grading.html  (self-contained, embeds DATA const)
       review/frag1_cards.json    (card dataset, for reproducibility)
No pipeline-script edits. No git commit.
"""
import csv
import html
import json
import random
import re
import sqlite3
import time
from collections import defaultdict

import frag1_truncation as F
from normalize import norm_stream, project_span

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
LIBS = ROOT + r"\libraries.csv"
OUT_HTML = PROBE + r"\review\frag1_grading.html"
OUT_JSON = PROBE + r"\review\frag1_cards.json"

N_NO_REF = 12
N_DENSITY = 10
N_CROP = 20
CROP_BINS = [60, 80, 100]      # the recall-knee zone


def connect():
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")   # A2 is writing concurrently
    return con


def pnum(pid):
    m = re.search(r'_P(\d+)_', pid or '')
    return int(m.group(1)) if m else 1


def viewer_url(sys_id, page):
    return f"https://genizahsearch.com/browse?sys_id={sys_id}&page={page}"


def load_lib_meta():
    """sys_id -> (shelfmark, library_code). Mirrors build_track1_review."""
    meta = {}
    with open(LIBS, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                meta[row[0]] = (variants[0] if variants else row[0],
                                row[3].strip() or '?')
    return meta


def best_per_work(cand):
    """cand = [(work_idx, alen, dens)]; -> ranked [(work_idx, dens, alen)]."""
    by_work = {}
    for wi, alen, dens in cand:
        if wi not in by_work or dens < by_work[wi][0]:
            by_work[wi] = (dens, alen)
    return sorted(((wi, d, a) for wi, (d, a) in by_work.items()),
                  key=lambda t: t[1])


def work_label(works, wi):
    w = works[wi]
    author = w.get('author', '')
    title = w.get('title', '')
    return {'work_id': w['id'], 'cat': w.get('cat', ''),
            'author': author, 'title': title}


def ref_text_for(seg_streams, span_tuple, ctx=90):
    """Matched reference span (within-segment coords) + ~ctx letters of flank
    on each side, as letters-only stream. span_tuple = (dens, si, r0, r1) from
    query_batch(want_refspan=True). Returns {before, span, after} or None."""
    if not span_tuple:
        return None
    _dens, si, r0, r1 = span_tuple
    seg = seg_streams[int(si)]
    a = max(0, int(r0) - ctx)
    b = min(len(seg), int(r1) + ctx)
    return {'before': seg[a:int(r0)], 'span': seg[int(r0):int(r1)],
            'after': seg[int(r1):b]}


def cand_render(lbl, density, ref):
    """Uniform per-candidate render record for the HTML comparison view."""
    return {'cat': lbl['cat'], 'author': lbl['author'], 'title': lbl['title'],
            'work_id': lbl['work_id'], 'density': density, 'ref': ref}


def round_robin(items, key_fn):
    """Deterministic round-robin over groups keyed by key_fn (diversity)."""
    groups = defaultdict(list)
    for it in items:
        groups[key_fn(it)].append(it)
    keys = sorted(groups)
    out = []
    i = 0
    progressed = True
    while progressed:
        progressed = False
        for k in keys:
            if i < len(groups[k]):
                out.append(groups[k][i])
                progressed = True
        i += 1
    return out


def main():
    t0 = time.time()
    con = connect()

    works, wid_to_wi, ref_tuple = F.build_reference()
    lib_meta = load_lib_meta()
    page_lengths = F.compute_page_lengths(con)     # cached; no DB scan
    live_ids, canonmask_ids = F.load_live_and_canonmask_ids(con)
    domains, groups, other_idx = F.load_domain_groups()

    def domain_of(sys_id):
        g = F.top_domain_group(sys_id, domains, other_idx)
        return groups[g][0], groups[g][1]          # (EN, HE)

    # ============ unidentified-pool cards (types 1-3) ============
    seg_streams = ref_tuple[0]        # segment texts (letters-only ref)
    sample = F.sample_unidentified_pages(page_lengths, live_ids, canonmask_ids)
    page_ids = [s[0] for s in sample]
    streams_by_pid = F.fetch_streams(con, page_ids)
    streams = [streams_by_pid.get(pid, '') for pid, sid, ln in sample]
    results, diag, refspans = F.query_batch(
        streams, ref_tuple, want_diag=True, want_refspan=True)

    # fetch original page text for the sampled pages (short pages, light)
    orig_text = {}
    for i in range(0, len(page_ids), 400):
        batch = page_ids[i:i + 400]
        ph = ','.join('?' * len(batch))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({ph})",
                batch):
            orig_text[pid] = tx or ''

    noref_pool, dfail_pool, ambig_pool = [], [], []
    for (pid, sid, ln), cand, dg, rs in zip(sample, results, diag, refspans):
        cls, _best = F.classify_failure(dg, cand)
        rec = {'pid': pid, 'sid': sid, 'len': ln, 'cand': cand, 'dg': dg,
               'refspan': rs}
        if cls == 'no_reference_covers_it':
            noref_pool.append(rec)
        elif cls == 'density_fail':
            dfail_pool.append(rec)
        elif cls == 'ambiguous':
            ambig_pool.append(rec)

    # judgeability: longest first (most text for Hillel to read)
    noref_pool.sort(key=lambda r: -r['len'])
    dfail_pool.sort(key=lambda r: -r['len'])
    ambig_pool.sort(key=lambda r: -r['len'])

    cards = []

    def page_card(rec, ctype, extra):
        pid, sid, ln = rec['pid'], rec['sid'], rec['len']
        sm, lib = lib_meta.get(sid, (sid, '?'))
        den, deh = domain_of(sid)
        base = {
            'id': pid, 'type': ctype, 'sys_id': sid, 'shelf': sm, 'lib': lib,
            'domain_en': den, 'domain_he': deh, 'len': ln,
            'page': pnum(pid), 'url': viewer_url(sid, pnum(pid)),
            'orig_text': orig_text.get(pid, '')[:1600],
            'norm_text': streams_by_pid.get(pid, '')[:1200],
            'grams': rec['dg']['grams'], 'hits': rec['dg']['hits'],
            'best_cluster': rec['dg']['best_cluster'],
        }
        base.update(extra)
        return base

    for rec in noref_pool[:N_NO_REF]:
        cards.append(page_card(rec, 'no_reference', {}))

    for rec in dfail_pool[:N_DENSITY]:
        ranked = best_per_work(rec['cand'])
        wi, d, a = ranked[0]
        lbl = work_label(works, wi)
        ref = ref_text_for(seg_streams, rec['refspan'].get(wi))
        cards.append(page_card(rec, 'density_fail', {
            'cand_work': lbl, 'cand_density': round(d, 3),
            'cand_aligned_len': int(a),
            'ref_candidates': [cand_render(lbl, round(d, 3), ref)]}))

    for rec in ambig_pool:      # all 3
        ranked = best_per_work(rec['cand'])[:3]
        cands, ref_candidates = [], []
        for wi, d, a in ranked:
            lbl = work_label(works, wi)
            cands.append({**lbl, 'density': round(d, 3), 'aligned_len': int(a)})
            ref = ref_text_for(seg_streams, rec['refspan'].get(wi))
            ref_candidates.append(cand_render(lbl, round(d, 3), ref))
        cards.append(page_card(rec, 'ambiguous', {
            'cands': cands, 'ref_candidates': ref_candidates}))

    # ============ crop_recovered cards (type 4) ============
    sampled_pages, _shortfall = F.sample_track1_pages(con)
    rnd = random.Random(F.RNG_SEED + 2)             # SAME as frag1 main()
    crops = F.make_crops(sampled_pages, rnd)
    # query directly (want_refspan) instead of run_truncation_experiment;
    # `results` (candidates) are byte-identical either way -> same recovered
    # set, same order, same ids. refspans is additive (gives us ref source).
    crop_streams = [c['stream'] for c in crops]
    cres, _cd, crefspans = F.query_batch(
        crop_streams, ref_tuple, want_diag=False, want_refspan=True)
    for c, cand, rs in zip(crops, cres, crefspans):
        c['candidates'] = cand
        c['true_wi'] = wid_to_wi.get(c['work_id'])
        c['refspan'] = rs

    recovered = []
    for c in crops:
        if c['length_bin'] not in CROP_BINS:
            continue
        true_wi = c['true_wi']
        if true_wi is None:
            continue
        acc = F.accepted_works_at_scale(c['candidates'], 1.0)
        top = F.top_ranked_work(acc)
        if top is not None and top == true_wi:       # genuine recovery
            recovered.append((c, acc[true_wi]))
    # spread evenly across ALL knee bins (60/80/100): per-bin round-robin by
    # work, then interleave the bins so every length is represented.
    per_bin = {}
    for b in CROP_BINS:
        per_bin[b] = round_robin([cd for cd in recovered
                                  if cd[0]['length_bin'] == b],
                                 key_fn=lambda cd: cd[0]['work_id'])
    ordered = []
    i = 0
    while any(i < len(per_bin[b]) for b in CROP_BINS):
        for b in CROP_BINS:
            if i < len(per_bin[b]):
                ordered.append(per_bin[b][i])
        i += 1
    # need original crop text: re-norm the source page once, project the crop
    crop_pids = {cd[0]['page_id'] for cd in ordered[:N_CROP * 3]}
    crop_orig = {}
    plist = list(crop_pids)
    for i in range(0, len(plist), 400):
        batch = plist[i:i + 400]
        ph = ','.join('?' * len(batch))
        for pid, tx in con.execute(
                f"SELECT page_id, text FROM pages WHERE page_id IN ({ph})",
                batch):
            crop_orig[pid] = tx or ''

    n_crop = 0
    for c, dens in ordered:
        if n_crop >= N_CROP:
            break
        pid = c['page_id']
        sid = c['sys_id']
        wi = c['true_wi']
        lbl = work_label(works, wi)
        ref = ref_text_for(seg_streams, c['refspan'].get(wi))
        sm, lib = lib_meta.get(sid, (sid, '?'))
        den, deh = domain_of(sid)
        # project the norm crop back onto the original page text (readable)
        raw = crop_orig.get(pid, '')
        crop_orig_text = ''
        if raw:
            nfc_stream, offs = norm_stream(raw)
            import unicodedata
            crop_orig_text = project_span(
                offs, c['offset'], c['offset'] + c['length_bin'],
                unicodedata.normalize('NFC', raw))
        cards.append({
            'id': f"{pid}|{c['length_bin']}|{c['offset']}",
            'type': 'crop_recovered', 'sys_id': sid, 'shelf': sm, 'lib': lib,
            'domain_en': den, 'domain_he': deh, 'len': c['length_bin'],
            'crop_len': c['length_bin'], 'offset': c['offset'],
            'page': pnum(pid), 'url': viewer_url(sid, pnum(pid)),
            'norm_text': c['stream'], 'orig_text': crop_orig_text[:1000],
            'recovered_work': lbl, 'density': round(dens, 3),
            'ref_candidates': [cand_render(lbl, round(dens, 3), ref)],
        })
        n_crop += 1

    con.close()

    counts = defaultdict(int)
    for c in cards:
        counts[c['type']] += 1
    print(f"cards: {dict(counts)} (total {len(cards)}) "
          f"[noref_pool={len(noref_pool)} dfail_pool={len(dfail_pool)} "
          f"ambig_pool={len(ambig_pool)} recovered={len(recovered)}] "
          f"({time.time() - t0:.0f}s)", flush=True)

    # ---- write JSON dataset ----
    json.dump(cards, open(OUT_JSON, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f"wrote {OUT_JSON}")

    # ---- write HTML ----
    doc = TEMPLATE.replace('__DATA__', json.dumps(cards, ensure_ascii=False))
    doc = doc.replace('__COUNTS__', html.escape(
        ' · '.join(f"{t} {counts[t]}" for t in
                   ('no_reference', 'density_fail', 'ambiguous',
                    'crop_recovered'))))
    open(OUT_HTML, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT_HTML} ({len(doc) // 1024} KB, "
          f"{time.time() - t0:.0f}s total)")


TEMPLATE = r"""<!DOCTYPE html>
<html lang="he">
<head>
<meta charset="utf-8">
<title>FRAG-1 short-bin grading</title>
<style>
 body{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#17181c;color:#d6d6d6}
 header{position:sticky;top:0;background:#20262b;color:#eee;padding:8px 16px;
        display:flex;gap:14px;align-items:center;flex-wrap:wrap;z-index:5;
        border-bottom:1px solid #33393f}
 header .prog{font-weight:600}
 header select,header button{padding:4px 8px;border-radius:6px;border:1px solid #444;
        background:#2e3138;color:#ddd;cursor:pointer}
 .sub{font-size:11px;color:#8fa3ad;width:100%}
 .card{max-width:1100px;margin:14px auto;background:#23252c;border-radius:10px;
       box-shadow:0 1px 6px rgba(0,0,0,.5);padding:14px 18px}
 .meta{display:flex;gap:12px;flex-wrap:wrap;font-size:14px;color:#aaa;
       margin-bottom:8px;align-items:center}
 .meta b{color:#fff}
 .badge{background:#3a3d46;color:#ddd;border-radius:10px;padding:1px 9px;font-size:12px}
 .badge.t0{background:#1565c0;color:#fff}
 .badge.t1{background:#00838f;color:#fff}
 .badge.t2{background:#ef6c00;color:#fff}
 .badge.t3{background:#6a1b9a;color:#fff}
 .cols{display:flex;gap:14px;flex-wrap:wrap}
 .pane{flex:1 1 460px;border:1px solid #3a3d46;border-radius:8px;padding:10px;
       background:#1d1f25}
 .pane h4{margin:0 0 6px;font-size:13px;color:#9aa4ac;direction:rtl;text-align:right}
 .pane h4 b{color:#e8e6df}
 .txt{direction:rtl;text-align:right;font-size:18px;line-height:1.8;
      white-space:pre-wrap;word-break:break-word;max-height:360px;overflow-y:auto;
      color:#e8e6df}
 .norm{direction:rtl;text-align:right;font-size:14px;color:#9aa4ac;
      white-space:pre-wrap;word-break:break-all;line-height:1.6}
 .cand{background:#1d1f25;border:1px solid #3a3d46;border-radius:8px;
      padding:8px 10px;margin-top:6px;direction:rtl;text-align:right;font-size:15px}
 .cand b{color:#e8e6df}
 .cand .d{color:#ef9a9a;font-size:13px}
 .refc{margin-bottom:12px}
 .refc:last-child{margin-bottom:0}
 .candhead{direction:rtl;text-align:right;font-size:13px;color:#9aa4ac;
      margin-bottom:5px}
 .candhead b{color:#e8e6df}
 .candhead .d{color:#ef9a9a}
 .reftxt{font-size:16px;max-height:240px}
 .txt mark{background:#6b5407;color:#ffe082;padding:0 1px}
 .txt .ctx{color:#6d6d6d}
 .grades{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
 .grades button{padding:8px 12px;border-radius:8px;border:1px solid #4a4d55;
      background:#2e3138;color:#ddd;cursor:pointer;font-size:14px}
 .grades button.sel{background:#2c7d32;color:#fff;border-color:#2c7d32}
 .wname{margin-top:10px}
 .wname input,.note textarea{width:100%;box-sizing:border-box;background:#1d1f25;
      color:#e8e6df;border:1px solid #3a3d46;border-radius:6px;padding:7px 9px;
      font-size:14px;direction:rtl;text-align:right}
 .note{margin-top:8px}
 .lbl{font-size:12px;color:#8fa3ad;margin-bottom:3px}
 .nav{display:flex;gap:10px;margin-top:10px}
 .nav button{padding:6px 14px;border-radius:8px;border:1px solid #555;
      cursor:pointer;background:#2e3138;color:#ddd}
 a{color:#6fb3e8}
 .hint{font-size:12px;color:#8a8a8a;margin-top:8px;line-height:1.5}
 details.norm-d{margin-top:8px;font-size:13px;color:#9aa4ac}
 details.norm-d summary{cursor:pointer;color:#9ec7e8}
</style>
</head>
<body>
<header>
 <span class="prog" id="prog"></span>
 <select id="filter"></select>
 <button onclick="jumpUngraded()">Next ungraded</button>
 <button onclick="exportGrades()">⬇ Export grades</button>
 <span id="counts" style="font-size:12px"></span>
 <span class="sub">FRAG-1 short-bin cards: __COUNTS__ — grade with keys 1–N, navigate ←/→</span>
</header>
<div id="app"></div>
<script>
const DATA = __DATA__;
// per-type grade sets: [value, label]. ambiguous is built per-card.
const GRADE_SETS = {
 no_reference: [
  ["known-canonical","1 · חיבור קנוני מוכר / known canonical (reference-gap)"],
  ["known-other","2 · חיבור אחר מוכר / known other work (+name)"],
  ["unknown-new","3 · לא מזוהה — תגלית אפשרית / unknown, possible discovery"],
  ["too-little","4 · קצר/מטושטש מדי / too little to tell"]],
 density_fail: [
  ["candidate-correct","1 · המועמד נכון / candidate correct"],
  ["candidate-wrong","2 · המועמד שגוי / candidate wrong"],
  ["cant-tell","3 · לא ניתן לקבוע / can't tell"]],
 crop_recovered: [
  ["correct-id","1 · הזיהוי נכון / correct ID"],
  ["wrong-id","2 · זיהוי שגוי / wrong ID"],
  ["cant-tell","3 · לא ניתן לקבוע / can't tell"]],
};
const TYPE_BADGE = {no_reference:"t0",density_fail:"t1",ambiguous:"t2",crop_recovered:"t3"};
const LS_KEY = "seed029_frag1_grades_v1";
let store = JSON.parse(localStorage.getItem(LS_KEY) || "{}");
// store[id] = {grade, work_name, note}
let idx = 0, filt = "all";

function items(){ return filt==="all" ? DATA : DATA.filter(d=>d.type===filt); }
function save(){ localStorage.setItem(LS_KEY, JSON.stringify(store)); }
function rec(id){ return store[id] || (store[id]={}); }
function esc(s){ return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;"); }
function nGraded(){ return Object.values(store).filter(v=>v && v.grade).length; }

function gradeSet(d){
 if(d.type==="ambiguous"){
   const gs = d.cands.map((c,i)=>[`pick:${i}`,
     `${i+1} · #${i+1} ${(c.author?c.author+" — ":"")}${c.title} נכון`]);
   gs.push(["none-correct",`${d.cands.length+1} · אף אחד לא נכון / none correct`]);
   gs.push(["cant-tell",`${d.cands.length+2} · לא ברור / can't tell`]);
   return gs;
 }
 return GRADE_SETS[d.type];
}

// right-pane content: one block per candidate, with the matched REFERENCE
// source text (letters-only, matched span <mark>ed, ~90-letter flanks).
function refBlocks(d){
 const cs = d.ref_candidates || [];
 if(!cs.length) return `<i style="color:#999">no candidate</i>`;
 return cs.map((c,i)=>{
   const r = c.ref;
   const body = (r && r.span)
     ? `<span class="ctx">${esc(r.before)}</span><mark>${esc(r.span)}</mark><span class="ctx">${esc(r.after)}</span>`
     : `<i style="color:#999">ref window unavailable</i>`;
   const tag = d.type==="ambiguous" ? `#${i+1} · ` : "";
   return `<div class="refc">
     <div class="candhead">${tag}[${esc(c.cat)}] ${c.author?esc(c.author)+" — ":""}<b>${esc(c.title)}</b>
       · <span class="d">density ${c.density}</span></div>
     <div class="txt reftxt">${body}</div></div>`;
 }).join("");
}

function render(){
 const list = items();
 if(!list.length){ document.getElementById("app").innerHTML =
   "<div class='card'>No cards in this filter.</div>"; return; }
 idx = Math.max(0, Math.min(idx, list.length-1));
 const d = list[idx];
 const r = store[d.id] || {};
 document.getElementById("prog").textContent =
   `${idx+1} / ${list.length}  (${nGraded()} / ${DATA.length} graded)`;
 const gs = gradeSet(d);
 const btns = gs.map(([k,lab]) =>
   `<button class="${r.grade===k?'sel':''}" onclick="grade('${k}')">${esc(lab)}</button>`).join("");
 const bcl = TYPE_BADGE[d.type]||"";
 const stats = (d.grams!==undefined)
   ? `<span>grams <b>${d.grams}</b></span><span>ref-hits <b>${d.hits}</b></span>
      <span>cluster <b>${d.best_cluster}</b></span>` : "";
 const orig = d.orig_text
   ? `<div class="txt">${esc(d.orig_text)}</div>`
   : `<div class="norm">(no original text — letters-only stream below)</div>`;
 const wname = (d.type==="no_reference")
   ? `<div class="wname"><div class="lbl">שם החיבור אם זוהה / work name (for known-*):</div>
      <input id="wn" value="${esc(r.work_name||"")}" placeholder="למשל: סדר רב עמרם / Genesis 12 ..."
      oninput="setWName(this.value)"></div>` : "";
 const fragLabel = d.type==="crop_recovered"
   ? "קטע חתוך (crop) — הקטע לבדיקה" : "טקסט הקטע (הדף)";
 const fragPane = `<div class="pane"><h4>${fragLabel}</h4>${orig}
     <details class="norm-d"><summary>letters-only stream (מה שהמכונה השוותה)</summary>
       <div class="norm">${esc(d.norm_text)}</div></details></div>`;
 let cols;
 if(d.type==="no_reference"){
   cols = `<div class="cols">${fragPane}</div>`;
 } else {
   const rlabel = d.type==="ambiguous"
     ? "מקורות מתחרים במהדורה (letters-only)"
     : "מקור המועמד במהדורה (letters-only)";
   cols = `<div class="cols">${fragPane}
     <div class="pane"><h4>${rlabel}</h4>${refBlocks(d)}</div></div>`;
 }
 document.getElementById("app").innerHTML = `
  <div class="card">
   <div class="meta">
    <span class="badge ${bcl}">${d.type}</span>
    <span>כתב־יד <b>${esc(d.shelf)}</b> · ${d.lib}</span>
    <span>עמ' ${d.page} · <a href="${d.url}" target="_blank">פתח תמונה ↗</a></span>
    <span>תחום: <b>${esc(d.domain_he)}</b> (${esc(d.domain_en)})</span>
    <span>אורך <b>${d.len}</b> אותיות</span>
    ${stats}
   </div>
   ${cols}
   <div class="grades">${btns}</div>
   ${wname}
   <div class="note"><div class="lbl">הערה / note (optional):</div>
     <textarea rows="2" oninput="setNote(this.value)">${esc(r.note||"")}</textarea></div>
   <div class="nav">
    <button onclick="move(-1)">← Prev</button>
    <button onclick="move(1)">Next →</button>
   </div>
   <div class="hint">${hintFor(d.type)}</div>
  </div>`;
}

function hintFor(t){
 if(t==="no_reference") return "הגייט המרכזי: המכונה לא מצאה אף חיבור ייחוס שמתיישר לדף הזה. "
   +"האם אתה מזהה את החיבור? אם כן (קנוני/אחר) — זו פרצת־ייחוס; אם לא — ייתכן שזו תגלית. "
   +"Keys 1–4 grade &amp; advance · ←/→ navigate.";
 if(t==="density_fail") return "המכונה מצאה מועמד אבל מעל סף הקבלה. האם המועמד המוצג נכון? "
   +"Keys 1–3 grade &amp; advance · ←/→ navigate.";
 if(t==="ambiguous") return "שני חיבורים (או יותר) מתחרים בצמידות. בחר את הנכון, או 'אף אחד'. "
   +"Keys 1–N grade &amp; advance · ←/→ navigate.";
 return "קטע קצר (60–100 אות) שנחתך מדף שכבר זוהה, והמכונה זיהתה אותו מחדש. "
   +"האם הקטע אכן מהחיבור המוצג? Keys 1–3 grade &amp; advance · ←/→ navigate.";
}

function grade(k){ const d=items()[idx]; rec(d.id).grade=k; save(); move(1); }
function setWName(v){ const d=items()[idx]; rec(d.id).work_name=v; save(); }
function setNote(v){ const d=items()[idx]; rec(d.id).note=v; save(); }
function move(dd){ idx+=dd; render(); }
function jumpUngraded(){
 const list=items();
 for(let i=0;i<list.length;i++){
   const j=(idx+1+i)%list.length;
   if(!(store[list[j].id]&&store[list[j].id].grade)){ idx=j; render(); return; } }
 alert("All graded in this filter!");
}
function exportGrades(){
 const out = DATA.filter(d=>store[d.id]&&store[d.id].grade).map(d=>{
   const r=store[d.id];
   const o={id:d.id, type:d.type, grade:r.grade};
   if(r.work_name) o.work_name=r.work_name;
   if(r.note) o.note=r.note;
   return o;
 });
 const blob = new Blob([JSON.stringify(out,null,1)],{type:"application/json"});
 const a = document.createElement("a");
 a.href = URL.createObjectURL(blob);
 a.download = "grades_frag1_2026-07-09.json"; a.click();
}
document.addEventListener("keydown", e=>{
 const tag=(e.target.tagName||"").toLowerCase();
 if(tag==="input"||tag==="textarea") return;     // don't hijack typing
 const d=items()[idx]; if(!d) return;
 const gs=gradeSet(d);
 if(/^[1-9]$/.test(e.key)){
   const n=+e.key-1;
   if(n<gs.length){ grade(gs[n][0]); e.preventDefault(); }
 } else if(e.key==="ArrowLeft"){ move(1); }       // RTL: left = forward
 else if(e.key==="ArrowRight"){ move(-1); }
});
const types = ["all",...new Set(DATA.map(d=>d.type))];
document.getElementById("filter").innerHTML =
 types.map(s=>`<option value="${s}">${s} (${s==="all"?DATA.length:DATA.filter(d=>d.type===s).length})</option>`).join("");
document.getElementById("filter").onchange = e=>{ filt=e.target.value; idx=0; render(); };
render();
</script>
</body>
</html>"""


if __name__ == '__main__':
    main()

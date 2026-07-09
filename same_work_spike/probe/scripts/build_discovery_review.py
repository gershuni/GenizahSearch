# -*- coding: utf-8 -*-
"""Discovery grading reviewer (SEED-029) — two discovery card types.

Separate from the FRAG-1 reviewer, but REUSES build_frag1_review.py's UI
(dark mode, 1-N grade+advance, arrow nav, Next-ungraded, type filter,
localStorage autosave, Export→flat JSON) AND its spaced-candidate-source-text
projection (build_id2path / spaced_pieces / the {before,span,after} render).

TYPE 1 "residue_unidentified" (~40): the most-copied UNIDENTIFIED passage
  units (residue_most_copied.py's ranking: labeled=0, continuum unit 367274
  excluded, top-N by distinct witness-MS count). Per card: representative
  passage text (SPACED, from a member's pages.text), witness count, library
  spread, FJMS domain, median length, member shelfmarks + browse links.
TYPE 2 "new_sample" (~40): a STRATIFIED sample of the new? discovery tier
  (live track1_matches rows, non-canonical, coverage>=T_PARTIAL, mesirah_tier
  == 'new?'). One highest-coverage exemplar per distinct identified work, then
  round-robin across FJMS domain groups (representative, not top-N of one
  work). Per card: fragment page text (spaced) side-by-side with the
  identified work's reference source span (spaced, via locate_in_work +
  the spaced projection), page-coverage, shelfmark + browse link.

Selection is deterministic (fixed sort/seed) and a selection AUDIT prints at
the end. Read-only, PRAGMA busy_timeout=120000 defensively; work_query_*
tables untouched. New files only; no git commit.

Usage: python -X utf8 -u build_discovery_review.py
Out:   review/discovery_grading.html  (self-contained, embeds DATA)
       review/discovery_cards.json    (card dataset)
"""
import csv
import html
import json
import sqlite3
import time
import unicodedata
from collections import Counter, defaultdict

import numpy as np

import build_frag1_review as B
import frag1_truncation as F
from build_track1_review import CANON_CATS, locate_in_work
from engine_np import _gram_codes
from normalize import norm_stream
from track1_build_ref import HEADER_RE   # prep Maagarim source exactly as ref build

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\fullcorpus.db"
REF = PROBE + r"\data\ref_corpus.pkl"
LIBS = ROOT + r"\libraries.csv"
OUT_HTML = PROBE + r"\review\discovery_grading.html"
OUT_JSON = PROBE + r"\review\discovery_cards.json"

UNITS_TABLE = "passage_units_accepted_pairs_canonmask"
MEMBERS_TABLE = "passage_unit_members_accepted_pairs_canonmask"
CONTINUUM_UNIT = 367274          # 18,676-MS chain — quarantined per brief
# the committed post-demotion new? discovery queue (track1_testimonies.py:
# aggregate to (ms,work), mesirah_tier THEN FjmsInfo.bib_signal demotion).
# 1,168 rows: edited (non-canonical) + cls=testimony + tier=='new?' (bib empty).
TESTIMONIES_CSV = PROBE + r"\results\track1_full_testimonies.csv"
N_RESIDUE = 40
N_NEW = 40
CTX = 90


def connect():
    con = sqlite3.connect(DB)
    con.execute("PRAGMA busy_timeout=120000")
    return con


def load_lib_meta3():
    """sys_id -> (shelfmark, library_code, catalog_title)."""
    meta = {}
    with open(LIBS, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                vs = [v.strip() for v in (row[2] or '').split('|') if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (vs[0] if vs else row[0], row[3].strip() or '?',
                                title)
    return meta


def prepped_for(id2path, cache, fallback, work_id, expected_stream):
    """Read+prep a work's source EXACTLY as track1_build_ref did; assert its
    stream matches ref_corpus; cache (nfc, offs) or None (=> letters-only)."""
    if work_id in cache:
        return cache[work_id]
    res = None
    ent = id2path.get(work_id)
    if not ent:
        fallback['no_source_path'] += 1
    else:
        kind, path = ent
        try:
            if kind == 'M':
                raw = open('\\\\?\\' + path, encoding='utf-8',
                           errors='replace').read()
                prepped = HEADER_RE.sub(' ', raw)
            else:
                prepped = open(path, encoding='utf-8',
                               errors='replace').read()
            stream, offs = norm_stream(prepped)
            if stream == expected_stream:
                res = (unicodedata.normalize('NFC', prepped), offs)
            else:
                fallback['stream_mismatch'] += 1
        except OSError:
            fallback['file_error'] += 1
    cache[work_id] = res
    return res


def work_gram_index(cache, work):
    wid = work['id']
    if wid in cache:
        return cache[wid]
    codes = _gram_codes(work['stream'])
    if len(codes):
        order = np.argsort(codes, kind='stable')
        res = (codes[order], np.arange(len(codes), dtype=np.int64)[order])
    else:
        res = (None, None)
    cache[wid] = res
    return res


def letters_window(stream, r0, r1, ctx=CTX):
    a = max(0, r0 - ctx)
    b = min(len(stream), r1 + ctx)
    return {'before': stream[a:r0], 'span': stream[r0:r1], 'after': stream[r1:b]}


def spaced_from_page(page_text, s, e):
    """Spaced (before, span, after) for a page-stream span via norm_stream
    offsets + build_frag1_review.spaced_pieces."""
    nfc = unicodedata.normalize('NFC', page_text)
    _stream, offs = norm_stream(page_text)
    return B.spaced_pieces(nfc, offs, int(s), int(e), pad=CTX)


# =====================================================================
# TYPE 1: residue_unidentified
# =====================================================================

def build_residue_cards(con, lib_meta, domains, groups, other_idx):
    t0 = time.time()
    # strictly UNLABELED: labeled=0 alone still admits low-confidence Track-1
    # labels; also require NO Track-1 label at all (COALESCE(t1_label,'')='').
    rows = con.execute(f"""
        SELECT unit, n_pages, n_ms, med_len, libs, t1_label, conf
        FROM {UNITS_TABLE}
        WHERE labeled = 0 AND COALESCE(t1_label, '') = ''
              AND unit != {CONTINUUM_UNIT}
        ORDER BY n_ms DESC, unit ASC
        LIMIT {N_RESIDUE}""").fetchall()
    units = [r[0] for r in rows]
    umeta = {r[0]: {'n_pages': r[1], 'n_ms': r[2], 'med_len': r[3],
                    't1_label': r[5] or '', 'conf': r[6] or ''} for r in rows}
    ph = ','.join('?' * len(units))
    mem = con.execute(
        f"SELECT unit, page_id, sys_id, start, end, cov, role "
        f"FROM {MEMBERS_TABLE} WHERE unit IN ({ph})", units).fetchall()
    members_by_unit = defaultdict(list)
    for u, pid, sid, s, e, cov, role in mem:
        members_by_unit[u].append(
            {'page_id': pid, 'sys_id': sid, 'start': s, 'end': e,
             'cov': cov, 'role': role})

    cards = []
    for u in units:
        ms = members_by_unit[u]
        by_sid = defaultdict(list)
        for m in ms:
            by_sid[m['sys_id']].append(m)
        # library spread (distinct witness MS)
        lib_counts = Counter(lib_meta.get(sid, (sid, '?', ''))[1]
                             for sid in by_sid)
        # modal FJMS domain across member MSS
        dom_counts = Counter(
            groups[F.top_domain_group(sid, domains, other_idx)][0]
            for sid in by_sid)
        top_dom_en = dom_counts.most_common(1)[0][0] if dom_counts else \
            groups[other_idx][0]
        top_dom_he = next((g[1] for g in groups if g[0] == top_dom_en),
                          groups[other_idx][1])
        # representative passage = longest-span member; spaced from its page
        rep = max(ms, key=lambda m: m['end'] - m['start'])
        rrow = con.execute("SELECT text FROM pages WHERE page_id=?",
                           (rep['page_id'],)).fetchone()
        passage = spaced_from_page(rrow[0] if rrow else '',
                                   rep['start'], rep['end'])
        # member shelfmarks (top 6 distinct sys by span length)
        best_by_sid = {}
        for sid, lst in by_sid.items():
            best_by_sid[sid] = max(lst, key=lambda m: m['end'] - m['start'])
        mem_list = []
        for sid in sorted(best_by_sid,
                          key=lambda s: -(best_by_sid[s]['end']
                                          - best_by_sid[s]['start']))[:6]:
            m = best_by_sid[sid]
            sm, lib, cat_title = lib_meta.get(sid, (sid, '?', ''))
            mem_list.append({
                'shelf': sm, 'lib': lib, 'cat_title': (cat_title or '')[:80],
                'page': B.pnum(m['page_id']),
                'url': B.viewer_url(sid, B.pnum(m['page_id']))})
        um = umeta[u]
        cards.append({
            'id': f"residue|{u}", 'type': 'residue_unidentified',
            'unit': u, 'n_ms': um['n_ms'], 'n_pages': um['n_pages'],
            'med_len': um['med_len'], 'domain_en': top_dom_en,
            'domain_he': top_dom_he,
            'lib_spread': lib_counts.most_common(8),
            'weak_t1': um['t1_label'], 'weak_conf': um['conf'],
            'passage_spaced': passage, 'members': mem_list,
        })
    audit = {
        'n': len(cards),
        'witness_range': (min(c['n_ms'] for c in cards),
                          max(c['n_ms'] for c in cards)) if cards else (0, 0),
        'weak_labeled': sum(1 for c in cards if c['weak_t1']),
        'domains': dict(Counter(c['domain_en'] for c in cards).most_common()),
        'libraries': dict(Counter(
            lib for c in cards for lib, _ in c['lib_spread']).most_common()),
    }
    print(f"residue cards: {len(cards)} ({time.time() - t0:.0f}s)", flush=True)
    return cards, audit


# =====================================================================
# TYPE 2: new_sample (stratified new? tier)
# =====================================================================

def select_new_sample(domains, groups, other_idx):
    """Deterministic stratified selection from the COMMITTED post-demotion
    new? discovery queue (track1_full_testimonies.csv). Rule: candidate =
    (ms,work) testimony row with cat NOT in CANON_CATS, cls=='testimony',
    tier=='new?' (bib-demoted 'new?known' rows are ALREADY excluded — this
    reproduces track1_testimonies.py's mesirah_tier + FjmsInfo.bib_signal
    demotion, matching the committed 1,168-row queue). Keep the single
    highest-coverage exemplar per distinct WORK, then round-robin across FJMS
    domain groups (best-coverage first within each group) until N_NEW — so the
    sample spans many works and all represented domains, not top-N of one work.
    """
    rows = list(csv.DictReader(open(TESTIMONIES_CSV, encoding='utf-8-sig')))
    edited_test = [r for r in rows if r['cat'] not in CANON_CATS
                   and r['cls'] == 'testimony']
    queue = [r for r in edited_test if r['tier'] == 'new?']
    n_demoted = sum(1 for r in edited_test if r['tier'] == 'new?known')
    assert all(not r['bib_signal'] for r in queue), "bib-demoted row leaked"
    best_per_work = {}
    for r in queue:
        rec = {'page_id': r['best_page'], 'sys_id': r['sys_id'],
               'work_id': r['work_id'], 'cat': r['cat'], 'author': r['author'],
               'title': r['work'], 'shelf': r['shelfmark'], 'lib': r['lib'],
               'best_cov': float(r['best_cov']),
               'best_density': float(r['best_density']),
               'bib_n': int(r['bib_n'] or 0)}
        cur = best_per_work.get(rec['work_id'])
        if cur is None or (rec['best_cov'], rec['page_id']) > \
                (cur['best_cov'], cur['page_id']):
            best_per_work[rec['work_id']] = rec
    by_dom = defaultdict(list)
    for rec in best_per_work.values():
        g = groups[F.top_domain_group(rec['sys_id'], domains, other_idx)][0]
        by_dom[g].append(rec)
    for g in by_dom:
        by_dom[g].sort(key=lambda r: (-r['best_cov'], r['page_id']))
    dom_order = sorted(by_dom, key=lambda g: (-len(by_dom[g]), g))
    chosen = []
    i = 0
    while len(chosen) < N_NEW and any(i < len(by_dom[g]) for g in dom_order):
        for g in dom_order:
            if len(chosen) >= N_NEW:
                break
            if i < len(by_dom[g]):
                chosen.append(by_dom[g][i])
        i += 1
    return chosen, len(queue), len(best_per_work), n_demoted


def build_new_cards(con, works, wid2work, domains, groups, other_idx, id2path):
    t0 = time.time()
    chosen, n_queue, n_works_total, n_demoted = select_new_sample(
        domains, groups, other_idx)
    gram_cache, prep_cache, fallback = {}, {}, Counter()
    cards = []
    n_ref_spaced = 0
    for rec in chosen:
        pid, sid, wid = rec['page_id'], rec['sys_id'], rec['work_id']
        sm, lib = rec['shelf'], rec['lib']
        g = F.top_domain_group(sid, domains, other_idx)
        den, deh = groups[g][0], groups[g][1]
        prow = con.execute("SELECT text FROM pages WHERE page_id=?",
                           (pid,)).fetchone()
        ptext = prow[0] if prow else ''
        pstream, _poffs = norm_stream(ptext)
        # the page-side matched span (from the live track1_matches row)
        mrow = con.execute(
            "SELECT matched_letters, spans_json FROM track1_matches "
            "WHERE page_id=? AND work_id=? AND shadowed_by IS NULL",
            (pid, wid)).fetchone()
        matched_letters = mrow[0] if mrow else 0
        spans = json.loads(mrow[1]) if mrow and mrow[1] else []
        p0, p1 = (max(spans, key=lambda s: s[1] - s[0])[:2] if spans
                  else (0, min(len(pstream), 200)))
        p0, p1 = int(p0), int(p1)
        frag_spaced = spaced_from_page(ptext, p0, p1)
        # ref-side: locate the page span in the work, then spaced projection
        ref_spaced, ref_letters, ref_dens = None, None, None
        work = wid2work.get(wid)
        if work is not None:
            wcodes, wpos = work_gram_index(gram_cache, work)
            if wcodes is not None and p1 > p0 and p0 < len(pstream):
                loc = locate_in_work(pstream[p0:p1], wcodes, wpos)
                if loc:
                    r0, r1 = loc
                    r0 = max(0, min(r0, len(work['stream'])))
                    r1 = max(r0, min(r1, len(work['stream'])))
                    ref_letters = letters_window(work['stream'], r0, r1)
                    prep = prepped_for(id2path, prep_cache, fallback, wid,
                                       work['stream'])
                    if prep:
                        ref_spaced = B.spaced_pieces(prep[0], prep[1], r0, r1,
                                                     pad=CTX)
        if ref_spaced:
            n_ref_spaced += 1
        cards.append({
            'id': f"new|{pid}|{wid}", 'type': 'new_sample',
            'sys_id': sid, 'shelf': sm, 'lib': lib, 'page': B.pnum(pid),
            'url': B.viewer_url(sid, B.pnum(pid)),
            'domain_en': den, 'domain_he': deh,
            'work': {'work_id': wid, 'cat': rec['cat'],
                     'author': rec['author'], 'title': rec['title']},
            'coverage': round(rec['best_cov'], 3),
            'matched_letters': matched_letters,
            'best_density': rec['best_density'],
            'frag_spaced': frag_spaced,
            'ref_spaced': ref_spaced, 'ref': ref_letters,
        })
    audit = {
        'n': len(cards), 'queue_size': n_queue, 'n_demoted_new_known': n_demoted,
        'distinct_works_in_tier': n_works_total,
        'works_in_sample': len({c['work']['work_id'] for c in cards}),
        'domains': dict(Counter(c['domain_en'] for c in cards).most_common()),
        'coverage_range': (round(min(c['coverage'] for c in cards), 3),
                           round(max(c['coverage'] for c in cards), 3))
        if cards else (0, 0),
        'ref_spaced_ok': n_ref_spaced,
        'fallbacks': dict(fallback),
    }
    print(f"new_sample cards: {len(cards)}; ref-spaced {n_ref_spaced}/"
          f"{len(cards)}; fallbacks={dict(fallback) or 'none'} "
          f"({time.time() - t0:.0f}s)", flush=True)
    return cards, audit


def main():
    t0 = time.time()
    import pickle
    con = connect()
    works = pickle.load(open(REF, 'rb'))
    wid2work = {w['id']: w for w in works}
    lib_meta = load_lib_meta3()
    domains, groups, other_idx = F.load_domain_groups()
    id2path = B.build_id2path()
    print(f"setup: {len(works):,} ref works, {len(lib_meta):,} lib rows, "
          f"{len(id2path):,} source paths ({time.time() - t0:.0f}s)",
          flush=True)

    residue_cards, res_audit = build_residue_cards(
        con, lib_meta, domains, groups, other_idx)
    new_cards, new_audit = build_new_cards(
        con, works, wid2work, domains, groups, other_idx, id2path)
    con.close()

    cards = residue_cards + new_cards
    counts = Counter(c['type'] for c in cards)

    json.dump(cards, open(OUT_JSON, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    doc = TEMPLATE.replace('__DATA__', json.dumps(cards, ensure_ascii=False))
    doc = doc.replace('__COUNTS__', html.escape(
        f"residue_unidentified {counts['residue_unidentified']} · "
        f"new_sample {counts['new_sample']}"))
    open(OUT_HTML, 'w', encoding='utf-8').write(doc)

    # ---- selection audit (confirms both Codex-REVISE fixes) ----
    print("\n===== SELECTION AUDIT =====", flush=True)
    print(f"[residue_unidentified] {res_audit['n']} units; "
          f"witness-MS range {res_audit['witness_range'][0]}-"
          f"{res_audit['witness_range'][1]}")
    print(f"  FIX(MEDIUM): weak/low-conf Track-1-labeled units in set = "
          f"{res_audit['weak_labeled']} (must be 0 — predicate requires "
          f"COALESCE(t1_label,'')='')")
    print(f"  domains: {res_audit['domains']}")
    print(f"  libraries (union of per-unit top-8 spreads): "
          f"{res_audit['libraries']}")
    print(f"[new_sample] {new_audit['n']} cards drawn from the committed "
          f"post-demotion new? queue ({new_audit['queue_size']} rows / "
          f"{new_audit['distinct_works_in_tier']} distinct works); "
          f"{new_audit['works_in_sample']} distinct works in sample")
    print(f"  FIX(BLOCKER): bib-demotion applied — {new_audit['n_demoted_new_known']} "
          f"edited-testimony rows demoted to new?known and EXCLUDED; "
          f"0 in sample by construction (queue = tier=='new?', bib_signal='' "
          f"asserted). queue size {new_audit['queue_size']} == committed 1,168.")
    print(f"  coverage range: {new_audit['coverage_range']}")
    print(f"  domains: {new_audit['domains']}")
    print(f"  ref-spaced rendered: {new_audit['ref_spaced_ok']}/"
          f"{new_audit['n']}; fallbacks={new_audit['fallbacks'] or 'none'}")
    print("===========================\n", flush=True)

    print(f"cards: {dict(counts)} (total {len(cards)})")
    print(f"wrote {OUT_JSON}")
    print(f"wrote {OUT_HTML} ({len(doc) // 1024} KB, {time.time() - t0:.0f}s)")


TEMPLATE = r"""<!DOCTYPE html>
<html lang="he">
<head>
<meta charset="utf-8">
<title>SEED-029 discovery grading</title>
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
 .badge.t0{background:#6a1b9a;color:#fff}
 .badge.t1{background:#1565c0;color:#fff}
 .cols{display:flex;gap:14px;flex-wrap:wrap}
 .pane{flex:1 1 440px;border:1px solid #3a3d46;border-radius:8px;padding:10px;
       background:#1d1f25}
 .pane h4{margin:0 0 6px;font-size:13px;color:#9aa4ac;direction:rtl;text-align:right}
 .pane h4 b{color:#e8e6df}
 .txt{direction:rtl;text-align:right;font-size:17px;line-height:1.8;
      white-space:pre-wrap;word-break:break-word;max-height:320px;overflow-y:auto;
      color:#e8e6df}
 .txt mark{background:#6b5407;color:#ffe082;padding:0 1px}
 .txt .ctx{color:#6d6d6d}
 .members{margin-top:10px;direction:rtl;text-align:right;font-size:13px}
 .members li{margin:2px 0}
 .libs{font-size:12px;color:#9aa4ac}
 .candhead{direction:rtl;text-align:right;font-size:13px;color:#9aa4ac;margin-bottom:5px}
 .candhead b{color:#e8e6df}
 .candhead .d{color:#ef9a9a}
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
</style>
</head>
<body>
<header>
 <span class="prog" id="prog"></span>
 <select id="filter"></select>
 <button onclick="jumpUngraded()">Next ungraded</button>
 <button onclick="exportGrades()">⬇ Export grades</button>
 <span id="counts" style="font-size:12px"></span>
 <span class="sub">SEED-029 discovery cards: __COUNTS__ — grade with keys 1–N, navigate ←/→</span>
</header>
<div id="app"></div>
<script>
const DATA = __DATA__;
const GRADE_SETS = {
 residue_unidentified: [
  ["identify-named","1 · אני מזהה — חיבור בשם / I can name the work"],
  ["known-family-unnamed","2 · משפחה/סוג מוכר, בלי שם / known family, unnamed"],
  ["unknown-new","3 · לא מזוהה — תגלית / unknown, discovery"],
  ["junk-not-text","4 · פסולת / לא טקסט רציף / junk, not real text"],
  ["cant-tell","5 · לא ניתן לקבוע / can't tell"]],
 new_sample: [
  ["confirmed-new-witness","1 · עד חדש מאושר / confirmed new witness"],
  ["correct-work-likely-known","2 · חיבור נכון, כנראה ידוע / correct work, likely already known"],
  ["misidentified","3 · זיהוי שגוי / misidentified"],
  ["cant-tell","4 · לא ניתן לקבוע / can't tell"]],
};
const TYPE_BADGE = {residue_unidentified:"t0", new_sample:"t1"};
const LS_KEY = "seed029_discovery_grades_v1";
let store = JSON.parse(localStorage.getItem(LS_KEY) || "{}");
let idx = 0, filt = "all";

function items(){ return filt==="all" ? DATA : DATA.filter(d=>d.type===filt); }
function save(){ localStorage.setItem(LS_KEY, JSON.stringify(store)); }
function rec(id){ return store[id] || (store[id]={}); }
function esc(s){ return (s||"").replace(/&/g,"&amp;").replace(/</g,"&lt;"); }
function nGraded(){ return Object.values(store).filter(v=>v&&v.grade).length; }
function spacedBody(o){
 if(o && o.span) return `<span class="ctx">${esc(o.before)}</span><mark>${esc(o.span)}</mark><span class="ctx">${esc(o.after)}</span>`;
 return `<i style="color:#999">unavailable</i>`;
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
 const gs = GRADE_SETS[d.type];
 const btns = gs.map(([k,lab]) =>
   `<button class="${r.grade===k?'sel':''}" onclick="grade('${k}')">${esc(lab)}</button>`).join("");
 const bcl = TYPE_BADGE[d.type]||"";
 let inner;
 if(d.type==="residue_unidentified"){
   const libs = d.lib_spread.map(x=>`${x[0]}:${x[1]}`).join(", ");
   const mem = d.members.map(m=>
     `<li><a href="${m.url}" target="_blank">${esc(m.shelf)}</a> (${esc(m.lib)}) עמ' ${m.page}`
     + (m.cat_title?` — <i style="color:#9aa4ac">${esc(m.cat_title)}</i>`:"")+`</li>`).join("");
   const wname = `<div class="wname"><div class="lbl">שם החיבור אם זוהה / work name (for "identify-named"):</div>
      <input value="${esc(r.work_name||"")}" placeholder="למשל: פיוט לר' יהודה הלוי / Karaite siddur ..."
      oninput="setWName(this.value)"></div>`;
   inner = `
    <div class="meta">
     <span class="badge ${bcl}">residue_unidentified</span>
     <span>unit <b>${d.unit}</b></span>
     <span><b>${d.n_ms}</b> witness MSS</span>
     <span><b>${d.n_pages}</b> pages · med <b>${d.med_len}</b> letters</span>
     <span>תחום: <b>${esc(d.domain_he)}</b> (${esc(d.domain_en)})</span>
     ${d.weak_t1?`<span>weak T1 hint: <i>${esc(d.weak_t1)}</i></span>`:``}
    </div>
    <div class="libs">libraries (by witness MS): ${esc(libs)}</div>
    <div class="cols"><div class="pane"><h4>קטע ייצוגי (spaced, from a member page)</h4>
      <div class="txt">${spacedBody(d.passage_spaced)}</div></div></div>
    <div class="pane" style="margin-top:10px"><h4>עדים (member shelfmarks)</h4>
      <ul class="members">${mem}</ul></div>
    ${wname}`;
 } else {
   const w = d.work;
   const refBody = (d.ref_spaced && d.ref_spaced.span)
     ? spacedBody(d.ref_spaced)
     : (d.ref && d.ref.span
        ? spacedBody(d.ref)+` <span style="color:#c98a4b">(letters-only)</span>`
        : `<i style="color:#999">ref window unavailable</i>`);
   inner = `
    <div class="meta">
     <span class="badge ${bcl}">new_sample</span>
     <span>כתב־יד <b>${esc(d.shelf)}</b> · ${d.lib}</span>
     <span>עמ' ${d.page} · <a href="${d.url}" target="_blank">פתח תמונה ↗</a></span>
     <span>תחום: <b>${esc(d.domain_he)}</b> (${esc(d.domain_en)})</span>
     <span>כיסוי דף <b>${(d.coverage*100).toFixed(0)}%</b> (${d.matched_letters} letters)</span>
    </div>
    <div class="cols">
     <div class="pane"><h4>קטע הדף (fragment, spaced)</h4>
       <div class="txt">${spacedBody(d.frag_spaced)}</div></div>
     <div class="pane"><h4>מקור מזוהה (identified work) — spaced</h4>
       <div class="candhead">[${esc(w.cat)}] ${w.author?esc(w.author)+" — ":""}<b>${esc(w.title)}</b>
         · <span class="d">density ${d.best_density}</span></div>
       <div class="txt">${refBody}</div></div>
    </div>`;
 }
 document.getElementById("app").innerHTML = `
  <div class="card">
   ${inner}
   <div class="grades">${btns}</div>
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
 if(t==="residue_unidentified") return "היחידה הכי־מועתקת שאינה מזוהה ע\"י Track-1. "
   +"האם אתה מזהה את החיבור (שם), מכיר את הסוג בלי שם, או שזו תגלית? "
   +"Keys 1–5 grade &amp; advance · ←/→ navigate.";
 return "הדף זוהה כעד חדש (mesirah מצביע על כתב־יד אחר). השווה קטע הדף למקור המזוהה: "
   +"עד חדש מאושר? חיבור נכון אך כנראה כבר ידוע? או זיהוי שגוי? "
   +"Keys 1–4 grade &amp; advance · ←/→ navigate.";
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
 a.download = "grades_discovery_2026-07-09.json"; a.click();
}
document.addEventListener("keydown", e=>{
 const tag=(e.target.tagName||"").toLowerCase();
 if(tag==="input"||tag==="textarea") return;
 const d=items()[idx]; if(!d) return;
 const gs=GRADE_SETS[d.type];
 if(/^[1-9]$/.test(e.key)){
   const n=+e.key-1;
   if(n<gs.length){ grade(gs[n][0]); e.preventDefault(); }
 } else if(e.key==="ArrowLeft"){ move(1); }
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

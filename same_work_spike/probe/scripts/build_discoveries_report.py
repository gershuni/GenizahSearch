# -*- coding: utf-8 -*-
"""Build the SEED-029 discoveries report.

Section 1: human-CONFIRMED same-composition discoveries (Hillel's grades).
Section 2: top UNVERIFIED candidates (machine-filtered: duplicate detectors
           applied, not human-reviewed), honestly labeled.

Aggregated to MANUSCRIPT-pair level (one entry per sys_id pair, best page
pair shown, supporting-pair count). Catalog titles from libraries.csv col 7;
pairs where exactly one side is catalogued are flagged as IDENTIFICATION
candidates.

Output: review/discoveries_report.html + results/discoveries_report.csv
"""
import csv
import json
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict

sys.path.insert(0, r"C:\Genizahsearch\same_work_spike\probe\scripts")
from normalize import norm_stream  # noqa: E402
from rapidfuzz.distance import Levenshtein  # noqa: E402

ROOT = r"C:\Genizahsearch"
GRADES = ROOT + r"\same_work_spike\probe\review\grades_hillel_2026-07-07.json"
REVIEW = ROOT + r"\same_work_spike\probe\review\review_data.json"
ENGINE = ROOT + r"\same_work_spike\probe\results\verified_pairs_d50_cap1.json"
PROBE_DB = ROOT + r"\same_work_spike\probe\data\probe.db"
OUT_HTML = ROOT + r"\same_work_spike\probe\review\discoveries_report.html"
OUT_CSV = ROOT + r"\same_work_spike\probe\results\discoveries_report.csv"

N_CANDIDATES = 60  # unverified section cap (manuscript pairs)

# ---------------- shelfmarks + titles ----------------
shelf, titles, shelf_variants = {}, {}, {}


def _norm_shelf(s):
    t = re.sub(r'(\d)\.(\d)', r'\1DOT\2', s.replace('/', '.'))
    t = re.sub(r'\W+', '', t).casefold().replace('dot', '.')
    return t[2:] if t.startswith('ms') else t


with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
    r = csv.reader(f)
    next(r, None)
    for row in r:
        if len(row) >= 4 and row[0]:
            variants = [v.strip() for v in (row[2] or '').split('|') if v.strip()]
            shelf[row[0]] = variants[0] if variants else row[0]
            shelf_variants[row[0]] = {_norm_shelf(v) for v in variants if v}
            if len(row) > 7 and row[7].strip():
                titles[row[0]] = row[7].strip()

# ---------------- span helpers (as in prep_review_tool) ----------------
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
            best_c, best = c, (min(r[1] for r in cl), max(r[2] for r in cl),
                               min(r[3] for r in cl), max(r[4] for r in cl))
    return best


def seg3(text, offs, s0, s1, pad=110):
    if not len(offs):
        return ('', '', '')
    s1 = min(s1, len(offs))
    a = offs[max(0, min(s0, len(offs) - 1))]
    z = offs[s1 - 1] + 1
    return (text[max(0, a - pad):a], text[a:z], text[z:z + pad])


def line_agreement(ma, mb):
    la = [norm_stream(x)[0] for x in ma.split('\n')]
    lb = [norm_stream(x)[0] for x in mb.split('\n')]
    la = [x for x in la if len(x) >= 10]
    lb = [x for x in lb if len(x) >= 10]
    if min(len(la), len(lb)) < 4:
        return 0.0
    j = matched = 0
    for a in la:
        for jj in range(j, min(j + 3, len(lb))):
            if Levenshtein.normalized_distance(a, lb[jj]) <= 0.30:
                matched += 1
                j = jj + 1
                break
    return matched / max(len(la), len(lb))


def pnum(pid):
    m = re.search(r'_P(\d+)_', pid)
    return int(m.group(1)) if m else 1


def build_entry(pid_a, pid_b):
    ta, sa, oa = get_page(pid_a)
    tb, sb, ob = get_page(pid_b)
    ext = pair_extents(sa, sb)
    if not ext:
        return None
    ia0, ia1, jb0, jb1 = ext
    m = 30
    a0, a1 = max(0, ia0 - m), min(len(sa), ia1 + 5 + m)
    b0, b1 = max(0, jb0 - m), min(len(sb), jb1 + 5 + m)
    dist = Levenshtein.distance(sa[a0:a1], sb[b0:b1])
    dens = round(dist / max(len(sa[a0:a1]), len(sb[b0:b1]), 1), 3)
    seg_a, seg_b = seg3(ta, oa, a0, a1), seg3(tb, ob, b0, b1)
    sysa, sysb = pid_a.split('_')[0], pid_b.split('_')[0]
    return {
        'a': {'pid': pid_a, 'sys': sysa, 'shelf': shelf.get(sysa, sysa),
              'title': titles.get(sysa, ''), 'page': pnum(pid_a), 'seg': seg_a},
        'b': {'pid': pid_b, 'sys': sysb, 'shelf': shelf.get(sysb, sysb),
              'title': titles.get(sysb, ''), 'page': pnum(pid_b), 'seg': seg_b},
        'len': max(a1 - a0, b1 - b0), 'density': dens,
        'line_agree': round(line_agreement(seg_a[1], seg_b[1]), 2),
        'same_shelf': bool(shelf_variants.get(sysa, set()) &
                           shelf_variants.get(sysb, set())),
    }


# ---------------- Section 1: confirmed ----------------
grades = json.load(open(GRADES, encoding='utf-8'))
confirmed_ids = [g['id'] for g in grades
                 if g['stratum'] == 'discovery' and
                 g['grade'] in ('same_text', 'verbatim', 'near_verbatim')]
graded_ids = {g['id'] for g in grades if g['stratum'] == 'discovery'}

by_ms_confirmed = defaultdict(list)  # (sysA,sysB) -> [(pid_a,pid_b)]
for gid in confirmed_ids:
    a, b = gid.split('|')
    key = tuple(sorted((a.split('_')[0], b.split('_')[0])))
    by_ms_confirmed[key].append((a, b))

# ---------------- Section 2: unverified candidates ----------------
engine = json.load(open(ENGINE, encoding='utf-8'))
cand_pairs = [p for p in engine
              if p['cls'] == 'cross' and p['density'] <= 0.35
              and p['len'] >= 300
              and f"{p['a']}|{p['b']}" not in graded_ids
              and f"{p['b']}|{p['a']}" not in graded_ids]
by_ms_cand = defaultdict(list)
for p in cand_pairs:
    key = tuple(sorted((p['a'].split('_')[0], p['b'].split('_')[0])))
    if key in by_ms_confirmed:
        continue
    by_ms_cand[key].append(p)

# rank MS pairs by their best page-pair length
ranked = sorted(by_ms_cand.items(),
                key=lambda kv: -max(p['len'] for p in kv[1]))

sections = {'confirmed': [], 'candidate': []}
for key, pagepairs in by_ms_confirmed.items():
    # best = longest span (recompute each, keep best)
    entries = [e for e in (build_entry(a, b) for a, b in pagepairs) if e]
    if not entries:
        continue
    best = max(entries, key=lambda e: e['len'])
    best['n_pairs'] = len(pagepairs)
    sections['confirmed'].append(best)

n_dropped_dup = 0
for key, pagepairs in ranked:
    if len(sections['candidate']) >= N_CANDIDATES:
        break
    best_p = max(pagepairs, key=lambda p: p['len'])
    e = build_entry(best_p['a'], best_p['b'])
    if not e:
        continue
    if e['line_agree'] >= 0.6 or e['same_shelf']:
        n_dropped_dup += 1
        continue
    e['n_pairs'] = len(pagepairs)
    sections['candidate'].append(e)

sections['confirmed'].sort(key=lambda e: -e['len'])
print(f"confirmed MS-pairs: {len(sections['confirmed'])} "
      f"(from {len(confirmed_ids)} page pairs); "
      f"candidates: {len(sections['candidate'])} "
      f"(dropped {n_dropped_dup} as probable duplicates)")

# ---------------- CSV ----------------
with open(OUT_CSV, 'w', encoding='utf-8-sig', newline='') as f:
    w = csv.writer(f)
    w.writerow(['status', 'shelfmark_a', 'shelfmark_b', 'title_a', 'title_b',
                'identification_candidate', 'span_letters', 'edit_density',
                'supporting_page_pairs', 'sys_id_a', 'sys_id_b',
                'page_a', 'page_b'])
    for status in ('confirmed', 'candidate'):
        for e in sections[status]:
            ident = bool(e['a']['title']) != bool(e['b']['title'])
            w.writerow([status, e['a']['shelf'], e['b']['shelf'],
                        e['a']['title'], e['b']['title'],
                        'YES' if ident else '', e['len'], e['density'],
                        e['n_pairs'], e['a']['sys'], e['b']['sys'],
                        e['a']['page'], e['b']['page']])

# ---------------- HTML ----------------
def esc(s):
    return s.replace('&', '&amp;').replace('<', '&lt;')


def pane(side):
    b, m, a = side['seg']
    url = (f"https://genizahsearch.com/browse?sys_id={side['sys']}"
           f"&page={side['page']}")
    title = f"<div class='ttl'>{esc(side['title'])}</div>" if side['title'] \
        else "<div class='ttl none'>— not catalogued —</div>"
    return (f"<div class='pane'><h4>{esc(side['shelf'])} · p.{side['page']} "
            f"· <a href='{url}' target='_blank'>open ↗</a></h4>{title}"
            f"<div class='txt'><span class='ctx'>{esc(b)}</span>"
            f"<mark>{esc(m)}</mark><span class='ctx'>{esc(a)}</span></div></div>")


def card(e, status):
    ident = bool(e['a']['title']) != bool(e['b']['title'])
    badges = f"<span class='badge'>{e['len']} letters</span>" \
             f"<span class='badge'>density {e['density']}</span>" \
             f"<span class='badge'>{e['n_pairs']} page pair(s)</span>"
    if ident:
        badges += "<span class='badge ident'>🎯 identification candidate</span>"
    return (f"<div class='card {status}'><div class='meta'>{badges}</div>"
            f"<div class='cols'>{pane(e['a'])}{pane(e['b'])}</div></div>")


cards_c = "\n".join(card(e, 'confirmed') for e in sections['confirmed'])
cards_u = "\n".join(card(e, 'candidate') for e in sections['candidate'])
n_ident = sum(1 for st in sections.values() for e in st
              if bool(e['a']['title']) != bool(e['b']['title']))

html = f"""<!DOCTYPE html>
<html lang="he"><head><meta charset="utf-8">
<title>Genizah Shared-Passage Discoveries — SEED-029 probe</title>
<style>
 body{{font-family:Segoe UI,Arial,sans-serif;margin:0;background:#f4f2ec;color:#222}}
 header{{background:#2c3e50;color:#fff;padding:18px 24px}}
 header h1{{margin:0 0 6px;font-size:22px}} header p{{margin:2px 0;font-size:13px;color:#cdd}}
 h2{{max-width:1200px;margin:26px auto 4px;padding:0 12px}}
 .note{{max-width:1200px;margin:0 auto 10px;padding:0 12px;font-size:13px;color:#666}}
 .card{{max-width:1200px;margin:12px auto;background:#fff;border-radius:10px;
   box-shadow:0 1px 4px rgba(0,0,0,.15);padding:12px 16px;border-inline-start:5px solid #2e7d32}}
 .card.candidate{{border-inline-start-color:#e8a33d}}
 .meta{{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:8px}}
 .badge{{background:#eee;border-radius:10px;padding:1px 9px;font-size:12px}}
 .badge.ident{{background:#1a5da6;color:#fff}}
 .cols{{display:flex;gap:12px;flex-wrap:wrap}}
 .pane{{flex:1 1 440px;border:1px solid #ddd;border-radius:8px;padding:10px}}
 .pane h4{{margin:0 0 4px;font-size:13px;color:#333;direction:ltr}}
 .ttl{{direction:rtl;font-size:13px;color:#1a5da6;margin-bottom:6px}}
 .ttl.none{{color:#bbb}}
 .txt{{direction:rtl;text-align:right;font-size:16px;line-height:1.7;
   white-space:pre-wrap;word-break:break-word}}
 .txt mark{{background:#ffe58a;padding:0 1px}} .ctx{{color:#999}}
 a{{color:#9cf}} .card a{{color:#1a5da6}}
</style></head><body>
<header>
 <h1>Genizah Shared-Passage Discoveries</h1>
 <p>SEED-029 feasibility probe · MiDRASH HTR corpus (v0.8, Nov 2025) ·
    pilot of 17,228 pages incl. 10,000 random · seed-and-extend engine ·
    generated 2026-07-07</p>
 <p>{len(sections['confirmed'])} human-confirmed manuscript pairs ·
    {len(sections['candidate'])} machine-filtered unverified candidates ·
    {n_ident} identification candidates (one side uncatalogued) ·
    graded precision: 1 spurious / 164 reviewed pairs</p>
</header>
<h2>✅ Confirmed discoveries (human-reviewed: same composition)</h2>
<div class="note">Each entry = one manuscript pair not previously linked in
 the catalogs used (FJMS joins, GenizahTitleId, PGP); the best-matching page
 pair is shown; highlighted = the machine-matched passage.</div>
{cards_c}
<h2>🟡 Unverified candidates (machine-filtered, NOT human-reviewed)</h2>
<div class="note">Top candidates by span length after duplicate filtering
 (line-break agreement + shelfmark identity). Treat as leads, not claims.</div>
{cards_u}
</body></html>"""
open(OUT_HTML, 'w', encoding='utf-8').write(html)
print(f"wrote {OUT_HTML} ({len(html)//1024} KB) + {OUT_CSV}")

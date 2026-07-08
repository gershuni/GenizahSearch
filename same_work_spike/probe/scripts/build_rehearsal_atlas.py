# -*- coding: utf-8 -*-
"""Browsable HTML atlas of same-unit clusters from the rehearsal run.

Usage: python build_rehearsal_atlas.py [db_path] [tag] [top_n]
Output: review/rehearsal_<tag>_atlas.html

One card per cluster (continuation layer, dup-filtered): member manuscripts
with shelfmark/library/title + browse links, and up to 3 sample aligned
passages (spans projected back onto the original HTR text, highlighted).
"""
import csv
import html
import re
import sqlite3
import sys
from collections import Counter, defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
TOP_N = int(sys.argv[3]) if len(sys.argv) > 3 else 120
TABLE = sys.argv[4] if len(sys.argv) > 4 else "accepted_pairs"
OUT = ROOT + rf"\same_work_spike\probe\review\rehearsal_{TAG}_atlas.html"
CLUSTERS_CSV = ROOT + rf"\same_work_spike\probe\results\rehearsal_{TAG}_clusters.csv"


def load_lib_meta():
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if len(row) >= 4 and row[0]:
                variants = [v.strip() for v in (row[2] or '').split('|')
                            if v.strip()]
                title = row[7].strip() if len(row) >= 8 else ''
                meta[row[0]] = (variants[0] if variants else row[0],
                                row[3].strip() or '?', title)
    return meta


def page_no(page_id):
    m = re.search(r'_P(\d+)_', page_id)
    return int(m.group(1)) if m else 1


def browse_link(sys_id, pid):
    return (f"https://genizahsearch.com/browse?sys_id={sys_id}"
            f"&page={page_no(pid)}")


def seg3(text, offs, s0, s1, pad=90):
    if not len(offs):
        return ('', '', '')
    s1 = min(s1, len(offs))
    a = offs[max(0, min(s0, len(offs) - 1))]
    z = offs[s1 - 1] + 1
    return (text[max(0, a - pad):a], text[a:z], text[z:z + pad])


def main():
    meta = load_lib_meta()
    con = sqlite3.connect(DB)

    # clusters from rehearsal_map's CSV (same-unit layer)
    clusters = defaultdict(list)
    with open(CLUSTERS_CSV, encoding='utf-8-sig', newline='') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            clusters[row[0]].append(row[1])
    top = sorted(clusters.items(), key=lambda kv: -len(kv[1]))[:TOP_N]

    # accepted pairs per ms-pair, best-first
    pair_rows = con.execute(f"""
        SELECT page_a, page_b, sys_a, sys_b, a0, a1, b0, b1,
               aligned_len, density, flank_class
        FROM {TABLE}
        WHERE dup_shelf = 0 AND dup_lines < 0.6
        ORDER BY aligned_len DESC""").fetchall()
    by_ms = defaultdict(list)
    for row in pair_rows:
        key = tuple(sorted((row[2], row[3])))
        if len(by_ms[key]) < 2:
            by_ms[key].append(row)

    page_cache = {}

    def page(pid):
        if pid not in page_cache:
            row = con.execute("SELECT text FROM pages WHERE page_id=?",
                              (pid,)).fetchone()
            text = row[0] if row else ''
            page_cache[pid] = (text,) + tuple([norm_stream(text)[1]])
        return page_cache[pid]

    cards = []
    for ci, (comp, members) in enumerate(top):
        libs = Counter(meta.get(s, ('', '?', ''))[1] for s in members)
        titles = Counter(t for s in members
                         for t in [meta.get(s, ('', '?', ''))[2]] if t)
        tt = ' · '.join(f"{html.escape(t)} ({c})"
                        for t, c in titles.most_common(3))
        mem_rows = []
        MEMBER_CAP = 60
        for s in sorted(members)[:MEMBER_CAP]:
            sm, lib, ti = meta.get(s, (s, '?', ''))
            mem_rows.append(
                f"<tr><td><a href='https://genizahsearch.com/browse?"
                f"sys_id={s}' target='_blank'>{html.escape(sm)}</a></td>"
                f"<td>{html.escape(lib)}</td>"
                f"<td>{html.escape(ti) or '—'}</td></tr>")
        if len(members) > MEMBER_CAP:
            mem_rows.append(
                f"<tr><td colspan='3'>… +{len(members) - MEMBER_CAP:,} more "
                f"manuscripts (full list in the clusters CSV)</td></tr>")
        # sample passages: best pairs inside this cluster
        mset = set(members)
        samples = []
        for key, rows in by_ms.items():
            if key[0] in mset and key[1] in mset and len(samples) < 3:
                pa_id, pb_id, sa, sb, a0, a1, b0, b1, alen, dens, fc = rows[0]
                ta, offa = page(pa_id)
                tb, offb = page(pb_id)
                p1, m1, s1 = seg3(ta, offa, a0, a1)
                p2, m2, s2 = seg3(tb, offb, b0, b1)
                samples.append(f"""
<div class='sample'>
 <div class='meta'>{alen} letters · density {dens:.2f} · {fc} ·
  <a href='{browse_link(sa, pa_id)}' target='_blank'>{html.escape(
      meta.get(sa, (sa,))[0])}</a> ↔
  <a href='{browse_link(sb, pb_id)}' target='_blank'>{html.escape(
      meta.get(sb, (sb,))[0])}</a></div>
 <div class='cols'>
  <div class='txt'>{html.escape(p1)}<mark>{html.escape(m1)}</mark>{
      html.escape(s1)}</div>
  <div class='txt'>{html.escape(p2)}<mark>{html.escape(m2)}</mark>{
      html.escape(s2)}</div>
 </div>
</div>""")
        cards.append(f"""
<details class='card' {'open' if ci < 8 else ''}>
 <summary><b>Cluster {ci + 1}</b> — {len(members)} manuscripts ·
  libraries {html.escape(str(dict(libs.most_common(4))))} ·
  <span class='titles'>{tt or 'no catalog title'}</span></summary>
 <table><tr><th>shelfmark</th><th>library</th><th>title</th></tr>
 {''.join(mem_rows)}</table>
 {''.join(samples)}
</details>""")

    n_ms = sum(len(m) for _, m in top)
    doc = f"""<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>Text-reuse atlas — rehearsal {TAG}</title>
<style>
 body{{font-family:Segoe UI,Arial,sans-serif;max-width:1100px;margin:20px auto;
      padding:0 14px;background:#fafaf7;color:#222}}
 .card{{background:#fff;border:1px solid #ddd;border-radius:8px;margin:10px 0;
       padding:8px 14px}}
 summary{{cursor:pointer;font-size:15px;line-height:1.5}}
 .titles{{color:#0a6}}
 table{{border-collapse:collapse;margin:8px 0;font-size:13px}}
 td,th{{border:1px solid #ddd;padding:3px 8px;text-align:left}}
 .sample{{border-top:1px dashed #ccc;margin-top:8px;padding-top:6px}}
 .meta{{font-size:12px;color:#666;margin-bottom:4px}}
 .cols{{display:flex;gap:12px}}
 .txt{{flex:1;direction:rtl;text-align:right;background:#f6f6f0;
      border-radius:6px;padding:8px;font-size:14px;line-height:1.7;
      white-space:pre-wrap}}
 mark{{background:#ffe08a}}
</style></head><body>
<h1>Text-reuse atlas — 100K-page rehearsal</h1>
<p>Same-unit clusters (continuation layer, duplicate-photography filtered).
Top {len(top)} clusters covering {n_ms} manuscripts. Machine output —
<b>not human-reviewed</b>. Generated by build_rehearsal_atlas.py.</p>
{''.join(cards)}
</body></html>"""
    open(OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT} ({len(top)} clusters, {n_ms} MSS)")
    con.close()


if __name__ == '__main__':
    main()

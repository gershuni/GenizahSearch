# -*- coding: utf-8 -*-
"""Track-1 testimony vs citation classification + new-testimonies report.

Two different products the plain match table conflates (Hillel, 2026-07-07):
1. CITATION: a small fraction of the page matches a work (a quote inside a
   different work) -> mask-only material for the discovery map.
2. TESTIMONY: most of the page agrees with the work -> the page IS a copy —
   a textual witness. Important for canonical works TOO (Genizah Talmud/Bible
   leaves are first-rate witnesses), not just for edited Maagarim/JA works.

Discriminator: page coverage = matched_letters / page_stream_letters.
Thresholds are placed after inspecting the empirical distribution (printed
as a histogram into the report; defaults below marked where the valley sat).

Usage: python track1_testimonies.py [db_path] [tag]
Out: results/track1_<tag>_testimonies.{md,csv}, review/track1_<tag>_testimonies.html
"""
import csv
import html
import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict

from normalize import norm_stream
from track1_bib import FjmsInfo, load_acronym_equiv

ROOT = r"C:\Genizahsearch"
DB = sys.argv[1] if len(sys.argv) > 1 else \
    ROOT + r"\same_work_spike\probe\data\rehearsal.db"
TAG = sys.argv[2] if len(sys.argv) > 2 else "100k"
MD = ROOT + rf"\same_work_spike\probe\results\track1_{TAG}_testimonies.md"
CSV_OUT = ROOT + rf"\same_work_spike\probe\results\track1_{TAG}_testimonies.csv"
HTML_OUT = ROOT + rf"\same_work_spike\probe\review\track1_{TAG}_testimonies.html"

CANON_CATS = {'Bible', 'Mishnah', 'Tosefta', 'Bavli', 'Yerushalmi'}
T_TESTIMONY = 0.45      # coverage >= : page IS a copy of the work
T_PARTIAL = 0.15        # in between: partial / damaged / long extract

CITY_LIB = [
    ('Cambridge', 'CUL'), ('Oxford', 'Oxford'), ('Petersburg', 'RNL'),
    ('London', 'BL'), ('New York', 'JTS'), ('Paris', 'AIU'),
    ('Manchester', 'Manchester'), ('Strasbourg', 'Strasbourg'),
    ('Philadelphia', 'Katz'), ('Jerusalem', 'NLI'), ('Budapest', 'Kaufmann'),
    ('Vienna', 'Vienna'), ('Genève', 'Geneva'), ('Geneva', 'Geneva'),
]


def load_meta():
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


def mesirah_tier(mesirah, shelfmark, lib):
    """'self?' if the edition's source manuscript looks like THIS ms."""
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


P_RE = re.compile(r'_P(\d+)_')


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def evidence_html(con, page_id, spans_json, pad=90):
    """Highlighted best-span snippet from the page's original HTR text."""
    row = con.execute("SELECT text FROM pages WHERE page_id=?",
                      (page_id,)).fetchone()
    if not row:
        return ''
    text = row[0]
    stream, offs = norm_stream(text)
    spans = json.loads(spans_json)
    p0, p1, _ = max(spans, key=lambda s: s[1] - s[0])
    p1 = min(int(p1), len(offs))
    if not len(offs) or p1 <= 0:
        return ''
    a = offs[max(0, min(int(p0), len(offs) - 1))]
    z = offs[p1 - 1] + 1
    return (f"<div class='ev'><span class='ctx'>"
            f"{html.escape(text[max(0, a - pad):a])}</span>"
            f"<mark>{html.escape(text[a:z][:700])}</mark>"
            f"<span class='ctx'>{html.escape(text[z:z + pad])}</span></div>")


def main():
    meta = load_meta()
    con = sqlite3.connect(DB)
    plen = {pid: len(norm_stream(tx)[0]) for pid, tx in
            con.execute("SELECT page_id, text FROM pages")}
    cols = [r[1] for r in con.execute("PRAGMA table_info(track1_matches)")]
    live = ("WHERE shadowed_by IS NULL" if 'shadowed_by' in cols else "")
    rows = con.execute(f"""
        SELECT page_id, sys_id, work_id, cat, genre, author, title,
               mesirah, matched_letters, best_density, n_spans, spans_json
        FROM track1_matches {live}""").fetchall()
    print(f"match rows: {len(rows):,} "
          f"({'shadow-filtered' if live else 'no shadow column'})")

    # ---- per-row coverage + class ----
    hist = defaultdict(Counter)   # cat-group -> coverage bin
    classed = []
    for r in rows:
        cov = r[8] / max(1, plen.get(r[0], 1))
        cls = ('testimony' if cov >= T_TESTIMONY else
               'partial' if cov >= T_PARTIAL else 'citation')
        grp = r[3] if r[3] in CANON_CATS else 'edited'
        hist[grp][round(min(cov, 1.0) * 10) / 10] += 1
        classed.append(r + (round(cov, 3), cls))

    # ---- aggregate to (manuscript, work); keep the BEST-evidence page ----
    ms_work = defaultdict(lambda: {'pages': 0, 'test': 0, 'part': 0,
                                   'cit': 0, 'letters': 0, 'best_d': 1.0,
                                   'best_cov': 0.0, 'best_letters': -1})
    info = {}
    for r in classed:
        key = (r[1], r[2])
        a = ms_work[key]
        a['pages'] += 1
        a['letters'] += r[8]
        a['best_d'] = min(a['best_d'], r[9])
        a['best_cov'] = max(a['best_cov'], r[12])
        a['test' if r[13] == 'testimony' else
          'part' if r[13] == 'partial' else 'cit'] += 1
        if r[8] > a['best_letters']:
            a['best_letters'] = r[8]
            info[key] = r
    ms_rows = []
    for (sid, wid), a in ms_work.items():
        r = info[(sid, wid)]
        sm, lib, cat_title = meta.get(sid, (sid, '?', ''))
        cls = ('testimony' if a['test'] >= 1 and a['best_cov'] >= T_TESTIMONY
               else 'partial' if a['part'] + a['test'] >= 1 else 'citation')
        tier = mesirah_tier(r[7], sm, lib) if cls != 'citation' else ''
        ms_rows.append({
            'sys_id': sid, 'shelfmark': sm, 'lib': lib,
            'catalog_title': cat_title,
            'work_id': wid, 'cat': r[3], 'genre': r[4],
            'author': r[5], 'work': r[6], 'mesirah': r[7],
            'pages': a['pages'], 'p_test': a['test'], 'p_part': a['part'],
            'p_cit': a['cit'], 'letters': a['letters'],
            'best_cov': a['best_cov'], 'best_density': a['best_d'],
            'cls': cls, 'tier': tier,
            'best_page': r[0], 'best_pnum': pnum(r[0]),
            'best_spans': r[11],
        })

    # ---- bib demotion: tier 'new?' already discussed/edited in research
    # (FJMS bibliography per AlmaId; Hillel 2026-07-07) -> 'new?known' ----
    for m in ms_rows:
        m['bib_n'] = 0
        m['bib_signal'] = ''
        m['bib_entry'] = ''
        m['known_channel'] = ''
    new_rows = [m for m in ms_rows if m['tier'] == 'new?']
    if new_rows:
        fj = FjmsInfo({m['sys_id'] for m in new_rows})
        equiv = load_acronym_equiv()
        for m in new_rows:
            sig, entry = fj.bib_signal(m['sys_id'], m['author'], m['work'],
                                       equiv)
            m['bib_n'] = len(fj.bib.get(m['sys_id'], []))
            m['bib_signal'] = sig
            m['bib_entry'] = entry
            if sig:
                m['tier'] = 'new?known'
                m['known_channel'] = 'fjms_bib'
        fj.close()
        print(f"bib demotion: {sum(1 for m in new_rows if m['bib_signal'])}"
              f"/{len(new_rows)} tier-new? rows already in research",
              flush=True)

    # ---- known-witness gate (Map-v2, 4 channels): a (work, manuscript)
    # pair the Academy already knows — via the edition's used mesirot, the
    # website מסירות נוספות tab, or the web msirot list — is not a new
    # discovery. Demote ONLY on confidence='high' (exact shelfmark match;
    # classmark-only stays, flagged upstream). Fail-open when the harvest
    # file is absent (pre-Map-v2 runs behave exactly as before). ----
    kw_path = ROOT + r"\same_work_spike\probe\data\known_witnesses_all.json"
    if os.path.exists(kw_path):
        known = {(k['work_id'], k['sys_id']): k['channel']
                 for k in json.load(open(kw_path, encoding='utf-8'))
                 if k.get('confidence') == 'high'}
        n_gate = 0
        for m in ms_rows:
            if m['tier'] != 'new?':
                continue
            ch = known.get((m['work_id'], m['sys_id']))
            if ch:
                m['tier'] = 'new?known'
                m['known_channel'] = ch
                n_gate += 1
        print(f"known-witness gate: {n_gate} tier-new? rows demoted "
              f"({len(known):,} high-conf known pairs, 4-channel)",
              flush=True)

    canon_test = [m for m in ms_rows
                  if m['cat'] in CANON_CATS and m['cls'] == 'testimony']
    edited_test = [m for m in ms_rows
                   if m['cat'] not in CANON_CATS and m['cls'] == 'testimony']
    lines = [f"# Track-1 testimonies vs citations — '{TAG}'", ""]
    cls_c = Counter(m['cls'] for m in ms_rows)
    lines += [
        f"- (manuscript, work) rows: {len(ms_rows):,} — {dict(cls_c)}",
        f"- **canonical testimonies** (MS is a Bible/Mishnah/Tosefta/Bavli/"
        f"Yerushalmi copy): **{len(canon_test):,} MSS** "
        f"({Counter(m['cat'] for m in canon_test).most_common()})",
        f"- **edited-work testimonies** (Maagarim/JA works): "
        f"**{len(edited_test):,}** (tier: "
        f"{Counter(m['tier'] for m in edited_test).most_common()})", "",
        "## Page-coverage distribution (validates the thresholds)",
    ]
    for grp in sorted(hist):
        lines.append(f"- {grp}: " + " ".join(
            f"{b:.1f}:{hist[grp][b]}" for b in sorted(hist[grp])))
    lines += ["", f"(class thresholds: testimony >= {T_TESTIMONY}, "
              f"partial >= {T_PARTIAL}, else citation)", ""]
    lines.append("## Top edited works by testimony MSS")
    wc = Counter()
    for m in edited_test:
        wc[f"[{m['cat']}] {m['author']} — {m['work']}"[:90]] += 1
    for w, c in wc.most_common(30):
        lines.append(f"- {w}: {c} MSS")

    csv_fields = [k for k in ms_rows[0].keys() if k != 'best_spans']
    with open(CSV_OUT, 'w', encoding='utf-8-sig', newline='') as f:
        w = csv.DictWriter(f, fieldnames=csv_fields, extrasaction='ignore')
        w.writeheader()
        for m in sorted(ms_rows, key=lambda m: (m['cls'] != 'testimony',
                                                -m['letters'])):
            w.writerow(m)

    # ---- HTML: testimonies grouped by work ----
    by_work = defaultdict(list)
    for m in ms_rows:
        if m['cls'] in ('testimony', 'partial'):
            by_work[(m['cat'], m['author'], m['work'], m['mesirah'])].append(m)
    n_ev = 0
    cards = []
    for (cat, author, work, mes), members in sorted(
            by_work.items(), key=lambda kv: -len(kv[1])):
        trs = []
        for m in sorted(members, key=lambda m: -m['letters']):
            url = (f"https://genizahsearch.com/browse?sys_id={m['sys_id']}"
                   f"&page={m['best_pnum']}")
            ev = ''
            if m['tier']:   # evidence inline for the judgment-needed rows
                snippet = evidence_html(con, m['best_page'], m['best_spans'])
                if snippet:
                    n_ev += 1
                    ev = (f"<details class='evd'><summary>עדות (עמ' "
                          f"{m['best_pnum']})</summary>{snippet}</details>")
            trs.append(
                f"<tr class='{m['cls']}'>"
                f"<td><a href='{url}' target='_blank'>"
                f"{html.escape(m['shelfmark'])}</a></td>"
                f"<td>{m['lib']}</td>"
                f"<td>{html.escape(m['catalog_title'][:60]) or '—'}</td>"
                f"<td><a href='{url}' target='_blank'>עמ' "
                f"{m['best_pnum']}</a></td>"
                f"<td>{m['pages']}</td><td>{m['letters']:,}</td>"
                f"<td>{m['best_cov']:.2f}</td><td>{m['cls']}"
                f"{(' · ' + m['tier']) if m['tier'] else ''}{ev}</td></tr>")
        head = f"[{cat}] {author + ' — ' if author else ''}{work}"
        cards.append(
            f"<details {'open' if len(members) > 5 else ''}><summary>"
            f"<b>{html.escape(head)}</b> — {len(members)} MSS"
            f"{(' · מסירה: ' + html.escape(mes[:70])) if mes else ''}"
            f"</summary><table><tr><th>shelfmark</th><th>lib</th>"
            f"<th>catalog title</th><th>best page</th><th>pages</th>"
            f"<th>letters</th><th>cov</th><th>class</th></tr>"
            f"{''.join(trs)}</table></details>")
    print(f"inline evidence snippets: {n_ev:,}")
    doc = f"""<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>Track-1 testimonies — {TAG}</title><style>
 body{{font-family:Segoe UI,Arial;max-width:1150px;margin:20px auto;
 padding:0 12px;background:#fafaf7;color:#222}}
 details{{background:#fff;border:1px solid #ddd;border-radius:8px;
 margin:8px 0;padding:6px 12px}}
 summary{{cursor:pointer}}
 table{{border-collapse:collapse;font-size:13px;margin:6px 0}}
 td,th{{border:1px solid #ddd;padding:3px 8px}}
 tr.testimony td{{background:#eaf7ea}} tr.partial td{{background:#fdf6e3}}
 details.evd{{margin-top:3px;font-size:12px}}
 details.evd summary{{color:#1a5da6;cursor:pointer}}
 .ev{{direction:rtl;text-align:right;font-size:14px;line-height:1.6;
 max-width:640px;white-space:pre-wrap;background:#fff;border:1px solid #ddd;
 border-radius:6px;padding:6px 8px;margin-top:3px}}
 .ev mark{{background:#ffe58a}}
 .ev .ctx{{color:#999}}
</style></head><body>
<h1>Track-1 testimonies — manuscripts carrying known works</h1>
<p>{len(by_work)} works · green rows = testimony (page coverage ≥
{T_TESTIMONY}), yellow = partial (≥ {T_PARTIAL}). Citations excluded
(kept in the CSV). tier 'new?' = the edition's source manuscript appears
to be a DIFFERENT manuscript; 'self?' = likely the edition's own source.
Machine output — not human-reviewed.</p>
{''.join(cards)}</body></html>"""
    open(HTML_OUT, 'w', encoding='utf-8').write(doc)
    open(MD, 'w', encoding='utf-8').write('\n'.join(lines))
    print('\n'.join(lines[:30]))
    print(f"\nwrote {MD}\n      {CSV_OUT}\n      {HTML_OUT}")
    con.close()


if __name__ == '__main__':
    main()

# -*- coding: utf-8 -*-
"""Motif-query growth review — evidence cards for the completion sweep.

Three sections:
  A. identified works that recovered their witness census (DF-cap remedy)
  B. unidentified texts that grew (interleaved Bible+Targum/Tafsir class,
     verse medleys, late hymns, dignitary blessings...)
  C. fragmentary tail — 3-4-witness motifs that gained +1-2 (the prize)

Out: results/motif_query_growth_review.html (dark, RTL, page-anchored
     genizahsearch links, matched-span evidence)
"""
import csv
import html
import json
import re
import sqlite3
from collections import Counter, defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
DB = ROOT + r"\same_work_spike\probe\data\fullcorpus.db"
OUT = ROOT + r"\same_work_spike\probe\results\motif_query_growth_review.html"

P_RE = re.compile(r'_P(\d+)_')


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
    m = P_RE.search(page_id)
    return int(m.group(1)) if m else 1


con = sqlite3.connect(DB)
meta = load_lib_meta()

members = defaultdict(set)
for m, sid in con.execute("SELECT motif, sys_id FROM motif_members_pilot"):
    members[m].add(sid)

pages_of = defaultdict(list)
for m, pid, s, e in con.execute(
        "SELECT motif, page_id, start, end FROM motif_members_pilot"):
    pages_of[m].append((pid, s, e))

hits = defaultdict(list)
for m, pid, sid, letters, d, sj in con.execute(
        "SELECT motif, page_id, sys_id, matched_letters, best_density, "
        "spans_json FROM motif_query_hits"):
    hits[m].append((pid, sid, letters, d, sj))
new_ms = {m: {h[1] for h in v} - members[m] for m, v in hits.items()}
new_ms = {m: v for m, v in new_ms.items() if v}

t1 = defaultdict(Counter)
t1_pages = {}
for pid, author, title in con.execute(
        "SELECT page_id, author, title FROM track1_matches "
        "WHERE shadowed_by IS NULL AND matched_letters >= 150"):
    t1_pages[pid] = f"{author + ' — ' if author else ''}{title}"
for m in new_ms:
    for pid, _, _ in pages_of[m]:
        if pid in t1_pages:
            t1[m][t1_pages[pid]] += 1

_page_cache = {}


def page_view(pid):
    if pid not in _page_cache:
        tx = con.execute("SELECT text FROM pages WHERE page_id=?",
                         (pid,)).fetchone()[0]
        _page_cache[pid] = (tx, *norm_stream(tx))
        if len(_page_cache) > 400:
            _page_cache.pop(next(iter(_page_cache)))
    return _page_cache[pid]


def orig_slice(pid, s, e, cap=420):
    tx, stream, offs = page_view(pid)
    if not len(offs) or s >= len(offs):
        return ''
    e = min(e, len(offs))
    frag = tx[offs[s]:offs[e - 1] + 1]
    return frag[:cap] + ('…' if len(frag) > cap else '')


def esc(x):
    return html.escape(str(x))


def witness_row(pid, sid, letters, d, sj, with_text):
    sm, lib, title = meta.get(sid, (sid, '?', ''))
    url = (f"https://genizahsearch.com/browse?sys_id={sid}"
           f"&page={page_no(pid)}")
    txt = ''
    if with_text:
        spans = json.loads(sj)
        p0, p1 = spans[0][0], max(sp[1] for sp in spans)
        txt = (f"<div class='ev' dir='rtl'>"
               f"{esc(orig_slice(pid, p0, p1, 300))}</div>")
    return (f"<tr><td><a href='{url}' target='_blank'>{esc(sm)}</a></td>"
            f"<td>{esc(lib)}</td><td dir='rtl' class='ti'>{esc(title[:60])}"
            f"</td><td>{letters}</td><td>{d:.2f}</td></tr>"
            + (f"<tr><td colspan='5'>{txt}</td></tr>" if txt else ''))


def card(m, new_sids, label):
    best = max(pages_of[m], key=lambda x: x[2] - x[1])
    pid, s, e = best
    rep = orig_slice(pid, s, e)
    old = len(members[m])
    rows = sorted((h for h in hits[m] if h[1] in new_sids),
                  key=lambda h: h[3])
    body = ''.join(witness_row(*h, with_text=(i < 2))
                   for i, h in enumerate(rows[:8]))
    more = (f"<div class='more'>+ {len(rows) - 8} עדים נוספים</div>"
            if len(rows) > 8 else '')
    return f"""
<div class='card'>
 <div class='hd'><span class='mid'>motif {m}</span>
   <span class='grow'>{old} → {old + len(new_sids)} כ"י
   (+{len(new_sids)})</span>
   <span class='lab'>{esc(label)}</span></div>
 <div class='rep' dir='rtl'>{esc(rep)}</div>
 <table><tr><th>עד חדש</th><th>ספריה</th><th>כותרת NLI</th>
   <th>אותיות</th><th>צפיפות</th></tr>{body}</table>{more}
</div>"""


growth = sorted(((len(v), m) for m, v in new_ms.items()), reverse=True)
ident = [(n, m) for n, m in growth if t1[m]]
unident = [(n, m) for n, m in growth if not t1[m]]
tail = sorted(
    ((n, m) for n, m in growth
     if len(members[m]) <= 4 and n <= 2),
    key=lambda x: min(h[3] for h in hits[x[1]]
                      if h[1] in new_ms[x[1]]))

cards_a = ''.join(card(m, new_ms[m], t1[m].most_common(1)[0][0])
                  for n, m in ident[:15])
cards_b = ''.join(card(m, new_ms[m], 'לא מזוהה (Track-1)')
                  for n, m in unident[:20])
cards_c = ''.join(card(m, new_ms[m],
                       (t1[m].most_common(1)[0][0] if t1[m]
                        else 'לא מזוהה (Track-1)'))
                  for n, m in tail[:15])

tot_new = sum(len(v) for v in new_ms.values())
tail_n = len(tail)
page = f"""<!DOCTYPE html><html lang='he'><head><meta charset='utf-8'>
<title>Motif-query growth review — 2026-07-08</title><style>
body{{background:#111;color:#ddd;font-family:Segoe UI,Arial;margin:0 auto;
 max-width:1050px;padding:20px}}
h1{{font-size:22px}} h2{{font-size:18px;color:#8ecdf7;margin-top:34px;
 border-bottom:1px solid #333;padding-bottom:6px}}
.intro{{background:#1a2230;border:1px solid #2c3e57;border-radius:8px;
 padding:12px 16px;line-height:1.5}}
.card{{background:#1c1c1e;border:1px solid #333;border-radius:8px;
 padding:12px 16px;margin:14px 0}}
.hd{{display:flex;gap:14px;align-items:baseline;flex-wrap:wrap}}
.mid{{color:#888;font-family:monospace}}
.grow{{color:#7ce38b;font-weight:600}}
.lab{{color:#e3b341}}
.rep{{background:#15151a;border-radius:6px;padding:8px 12px;margin:8px 0;
 font-size:17px;line-height:1.7}}
.ev{{background:#132018;border-right:3px solid #2ea043;border-radius:4px;
 padding:6px 10px;margin:2px 0 8px;font-size:15px;line-height:1.6}}
table{{border-collapse:collapse;width:100%;font-size:13px}}
th,td{{border-bottom:1px solid #2a2a2e;padding:4px 8px;text-align:right}}
th{{color:#888;font-weight:500}}
a{{color:#58a6ff;text-decoration:none}}
.ti{{color:#aaa}} .more{{color:#777;font-size:12px;margin-top:4px}}
</style></head><body>
<h1>Motif-as-query — סקירת גידול עדים (2026-07-08)</h1>
<div class='intro'>
12,895 מוטיבים (≥3 כ"י, אורך חציוני ≥100) הופעלו כשאילתות על מלוא
הקורפוס (667,411 עמודים). <b>{len(new_ms):,} מוטיבים גדלו</b>,
<b>{tot_new:,} חברויות (מוטיב, כ"י) חדשות</b>.<br><br>
<b>א.</b> יצירות מזוהות שהשלימו את מפקד העדים שלהן — קורבנות תקרת
ה-DF (קדושת היום למוסף, ברכות חתימה לתקיעות, מעין שבע, "אלו דברים").<br>
<b>ב.</b> טקסטים לא-מזוהים שגדלו — כולל מחלקה שלמה של
<b>מקרא-עם-תרגום/תפסיר משולב</b> (פסוק-פסוק — השילוב מביס גם את
המיסוך הקנוני וגם את Track-1), מחרוזות פסוקים ליטורגיות, פיוטים
מאוחרים (בני היכלא), וברכות לנשיאים.<br>
<b>ג.</b> הזנב הפרגמנטרי — מוטיבים של 3–4 עדים שקיבלו עד אחד או
שניים ({tail_n:,} מוטיבים כאלה; כאן 15 עם הראיות החזקות ביותר).
הקישורים פותחים את העמוד המדויק ב-genizahsearch.
</div>
<h2>א. יצירות מזוהות — המפקד הושלם ({len(ident):,} מוטיבים)</h2>
{cards_a}
<h2>ב. לא מזוהים — מחלקות חדשות ({len(unident):,} מוטיבים)</h2>
{cards_b}
<h2>ג. הזנב הפרגמנטרי — +1/+2 עדים ({tail_n:,} מוטיבים; 15 מובחרים)</h2>
{cards_c}
</body></html>"""

open(OUT, 'w', encoding='utf-8').write(page)
print(f"wrote {OUT}")
print(f"identified grown: {len(ident):,}; unidentified: {len(unident):,}; "
      f"fragmentary tail (+1/+2, old<=4): {tail_n:,}")

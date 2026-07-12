# -*- coding: utf-8 -*-
"""Preview deck for the MAPV2 smoke run — tier-B probability spectrum.

NOT a grading deck (no verdict buttons): a limited-scale PREVIEW for Hillel
to eyeball the new probability-graded discovery tier before the overnight
full-corpus run. Cards are drawn from <smoke db>::track1_candidates,
stratified across P buckets and margin bands, dominated ('not_best') rows
excluded (they are same-prayer edition twins of a better match; the FINAL
margin model will floor most of them out).

Usage: python -X utf8 -u build_smoke_preview.py
Out:   review/mapv2_smoke_preview.html
"""
import html
import json
import re
import sqlite3
from collections import Counter, defaultdict

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
DB = PROBE + r"\data\mapv2_smoke.db"
OUT = PROBE + r"\review\mapv2_smoke_preview.html"

P_RE = re.compile(r'_P(\d+)_')
PER_WORK_CAP = 3
STRATA = [           # (label, min_p, max_p, n_cards, bands or None=any)
    ("P ≥ 0.8 — כמעט־ודאי", 0.8, 1.01, 25, None),
    ("P 0.5–0.8 — סביר", 0.5, 0.8, 20, None),
    ("P 0.2–0.5 — ספק", 0.2, 0.5, 15, None),
    ("P < 0.2 — קרוב לרעש (לניגוד)", 0.05, 0.2, 8, None),
    ("יחידים (singleton) — התאמה בודדת, המועמדים הקשים", 0.05, 1.01, 15,
     {"singleton"}),
]


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else 1


def snippet(text, spans_json, pad=80):
    """Highlighted best-span snippet from the page text (stream offsets)."""
    stream, offs = norm_stream(text)
    spans = json.loads(spans_json)
    p0, p1, _ = max(spans, key=lambda s: s[1] - s[0])
    p1 = min(int(p1), len(offs))
    if not len(offs) or p1 <= 0:
        return ''
    a = offs[max(0, min(int(p0), len(offs) - 1))]
    z = offs[p1 - 1] + 1
    return (f"<span class='ctx'>{html.escape(text[max(0, a - pad):a])}</span>"
            f"<mark>{html.escape(text[a:z][:600])}</mark>"
            f"<span class='ctx'>{html.escape(text[z:z + pad])}</span>")


def main():
    con = sqlite3.connect(DB)
    rows = con.execute("""
        SELECT page_id, sys_id, work_id, cat, author, title,
               best_alen, best_density, margin, n_competitors, margin_band,
               p_same_work, matched_letters, spans_json
        FROM track1_candidates WHERE margin_band != 'not_best'
        ORDER BY p_same_work DESC""").fetchall()
    print(f"non-dominated tier-B rows: {len(rows):,}")

    # tier-A witness count per work (novelty context)
    a_ms = defaultdict(set)
    for wid, sid in con.execute(
            "SELECT work_id, sys_id FROM track1_matches"):
        a_ms[wid].add(sid)

    # shelfmarks
    meta = {}
    import csv
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for r in rd:
            if len(r) >= 4 and r[0]:
                v = [x.strip() for x in (r[2] or '').split('|') if x.strip()]
                meta[r[0]] = (v[0] if v else r[0], r[3].strip() or '?')

    band_he = {
        'singleton': 'התאמה בודדת',
        'm_ge_010': 'מוביל בפער גדול',
        'm_003_010': 'מוביל בפער בינוני',
        'm_0_003': 'מוביל בפער קטן',
    }

    sections = []
    used_pages = set()
    for label, lo, hi, n_cards, bands in STRATA:
        per_work = Counter()
        cards = []
        for r in rows:
            (pid, sid, wid, cat, author, title, alen, dens, margin, ncomp,
             band, p, letters, spans_json) = r
            if not (lo <= p < hi):
                continue
            if bands and band not in bands:
                continue
            if (pid, wid) in used_pages:
                continue
            if per_work[wid] >= PER_WORK_CAP:
                continue
            trow = con.execute("SELECT text FROM pages WHERE page_id=?",
                               (pid,)).fetchone()
            if not trow or not trow[0]:
                continue
            per_work[wid] += 1
            used_pages.add((pid, wid))
            sm, lib = meta.get(sid, (sid, '?'))
            url = (f"https://genizahsearch.com/browse?sys_id={sid}"
                   f"&page={pnum(pid)}")
            name = f"{author} — {title}" if author else title
            ev = snippet(trow[0], spans_json)
            n_wit = len(a_ms.get(wid, ()))
            cards.append(f"""
<div class='card'>
 <div class='head'>
  <span class='p' title='P(same-work)'>P {p:.2f}</span>
  <a href='{url}' target='_blank'><b>{html.escape(sm)}</b></a>
  <span class='lib'>{lib}</span>
  <span class='work'>[{cat}] {html.escape(name[:75])}</span>
 </div>
 <div class='stats'>אורך התאמה {alen} אות · מרחק {dens:.2f} ·
  {band_he.get(band, band)}{f" (פער {margin:.2f})" if margin is not None and ncomp else ""} ·
  {letters} אותיות תואמות · עדים ברמה המחמירה לחיבור זה
  (בתת־הקורפוס): {n_wit}</div>
 <div class='ev'>{ev}</div>
</div>""")
            if len(cards) >= n_cards:
                break
        sections.append((label, cards))
        print(f"  {label}: {len(cards)} cards")

    # P histogram for the header
    hist = dict(con.execute(
        "SELECT ROUND(p_same_work,1), COUNT(*) FROM track1_candidates "
        "WHERE margin_band != 'not_best' GROUP BY 1 ORDER BY 1"))
    n_a = con.execute("SELECT COUNT(*) FROM track1_matches").fetchone()[0]
    n_b = con.execute("SELECT COUNT(*) FROM track1_candidates").fetchone()[0]
    hist_html = " · ".join(f"{k:.1f}: {v:,}" for k, v in hist.items())

    body = []
    for label, cards in sections:
        body.append(f"<h2>{label}</h2>")
        body.extend(cards or ["<p>(אין כרטיסים ברצועה זו)</p>"])
    doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'><head>
<meta charset='utf-8'><title>MAPV2 preview — הספקטרום ההסתברותי (ליטורגיה)</title>
<style>
 body{{font-family:Segoe UI,Arial;max-width:1100px;margin:20px auto;
 padding:0 12px;background:#fafaf7;color:#222}}
 .card{{background:#fff;border:1px solid #ddd;border-radius:8px;
 margin:10px 0;padding:8px 14px}}
 .head{{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}}
 .p{{background:#1a5da6;color:#fff;border-radius:6px;padding:2px 8px;
 font-weight:bold}}
 .lib{{color:#888}} .work{{color:#444}}
 .stats{{font-size:12.5px;color:#666;margin:4px 0}}
 .ev{{direction:rtl;text-align:right;font-size:14.5px;line-height:1.7;
 white-space:pre-wrap;background:#fcfcf9;border:1px solid #eee;
 border-radius:6px;padding:6px 8px}}
 .ev mark{{background:#ffe58a}} .ev .ctx{{color:#aaa}}
 h2{{border-bottom:2px solid #1a5da6;padding-bottom:4px;margin-top:28px}}
 .note{{background:#eef4fb;border:1px solid #cfe0f5;border-radius:8px;
 padding:10px 14px;font-size:14px}}
</style></head><body>
<h1>תצוגה מקדימה — הרובד ההסתברותי (Tier B), תת־קורפוס ליטורגיה</h1>
<div class='note'>
<b>מה זה:</b> ריצת בדיקה על 139,694 עמודי ליטורגיה עם המנוע החדש.
הרובד המחמיר (הצנזוס) לא השתנה — {n_a:,} זיהויים. מתחתיו נשמרו
{n_b:,} מועמדים עם <b>ציון הסתברות</b> (כמה סביר שזה באמת אותו חיבור,
בהינתן אורך ההתאמה והמרחק). כאן מוצגים רק מועמדים שאינם "כפילי מהדורה"
של התאמה טובה יותר. הציונים כאן הם מכיול ה־PILOT — הכיול הסופי ירוץ הלילה.
<br><b>התפלגות P (ללא כפילים):</b> {hist_html}
<br>כל כרטיס: לחיצה על מספר המדף פותחת את העמוד ב־GenizahSearch;
הקטע המודגש = ההתאמה.
</div>
{''.join(body)}
</body></html>"""
    open(OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT}")
    con.close()


if __name__ == '__main__':
    main()

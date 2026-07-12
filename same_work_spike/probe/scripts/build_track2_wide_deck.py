# -*- coding: utf-8 -*-
"""MAPV2-6 product — small-fragment pair review page from track2_wide.db.

Page-vs-page (no reference needed) same-work candidate PAIRS below the strict
boundary, P-stamped by the decoy-calibrated wide tier. Focus: SMALL fragments
(min side <= 300 stream letters) — the population Hillel said future
discoveries live in. Honest labeling: p_local_bucket is bucket-level
precision (local FDR complement), NOT a pair-specific probability.

Usage: python -X utf8 -u build_track2_wide_deck.py
Out:   review/track2_wide_small_fragments.html
"""
import csv
import html
import re
import sqlite3

from normalize import norm_stream

ROOT = r"C:\Genizahsearch"
PROBE = ROOT + r"\same_work_spike\probe"
WIDE = PROBE + r"\data\track2_wide.db"
CORPUS = PROBE + r"\data\fullcorpus_v2.db"
OUT = PROBE + r"\review\track2_wide_small_fragments.html"
P_RE = re.compile(r'_P(\d+)_')
MASK_OV_MAX = 0.30

FLANK_HE = {
    'continuation': ('ההקשר ממשיך משני הצדדים ✓', '#2e7d32'),
    'island': ('אי — השוליים שונים (אפשרי ציטוט משותף)', '#b26a00'),
    'ambig': ('הקשר גבולי', '#8a6d3b'),
    'edge': ('קצה קטע', '#777'),
}


def pnum(pid):
    m = P_RE.search(pid)
    return int(m.group(1)) if m else None


def snip(text, s0, s1, pad=70):
    stream, offs = norm_stream(text)
    s0 = max(0, min(int(s0), len(offs) - 1))
    s1 = min(int(s1), len(offs))
    if not len(offs) or s1 <= 0:
        return ''
    a, z = offs[s0], offs[s1 - 1] + 1
    return (f"<span class='ctx'>{html.escape(text[max(0, a - pad):a])}</span>"
            f"<mark>{html.escape(text[a:z][:500])}</mark>"
            f"<span class='ctx'>{html.escape(text[z:z + pad])}</span>")


def main():
    wide = sqlite3.connect(f"file:{WIDE}?mode=ro", uri=True)
    corp = sqlite3.connect(f"file:{CORPUS}?mode=ro", uri=True)
    meta = {}
    with open(ROOT + r"\libraries.csv", encoding='utf-8-sig', newline='') as f:
        rd = csv.reader(f)
        next(rd, None)
        for r in rd:
            if len(r) >= 4 and r[0]:
                v = [x.strip() for x in (r[2] or '').split('|') if x.strip()]
                meta[r[0]] = (v[0] if v else r[0], r[3].strip() or '?',
                              r[7].strip() if len(r) >= 8 else '')

    base = ("SELECT page_a, page_b, sys_a, sys_b, a0, a1, b0, b1, alen, "
            "dens, minlen, p_local_bucket, q_value, flank_dist, flank_class "
            "FROM track2_wide WHERE stratum <= 1 AND dup_shelf = 0 "
            "AND dup_lines < 0.6 AND mask_ov_a < ? AND mask_ov_b < ? ")
    sections = [
        ("ההקשר ממשיך (המבחן שלך) — הראיה החזקה", base +
         "AND flank_class='continuation' ORDER BY p_local_bucket DESC, "
         "alen DESC LIMIT 20"),
        ("שברי־זעיר (צד קטן ≤ 150 אות) — אוכלוסיית המטרה", base +
         "AND minlen <= 150 ORDER BY p_local_bucket DESC, dens ASC LIMIT 12"),
        ("שאר הזוגות המובילים (אי/קצה — לבדיקת ציטוט משותף)", base +
         "AND flank_class != 'continuation' ORDER BY p_local_bucket DESC, "
         "alen DESC LIMIT 13"),
    ]
    used = set()
    body = []
    n_cards = 0
    for label, q in sections:
        cards = []
        for row in wide.execute(q, (MASK_OV_MAX, MASK_OV_MAX)):
            (pa, pb, sa, sb, a0, a1, b0, b1, alen, dens, minlen,
             pl, qv, fd, fc) = row
            key = frozenset((pa, pb))
            if key in used:
                continue
            used.add(key)
            ta = corp.execute("SELECT text FROM pages WHERE page_id=?",
                              (pa,)).fetchone()
            tb = corp.execute("SELECT text FROM pages WHERE page_id=?",
                              (pb,)).fetchone()
            if not ta or not tb or not ta[0] or not tb[0]:
                continue
            panes = []
            for pid, sid, t, s0, s1 in ((pa, sa, ta[0], a0, a1),
                                        (pb, sb, tb[0], b0, b1)):
                sm, lib, nli = meta.get(sid, (sid, '?', ''))
                pn = pnum(pid)
                url = (f"https://genizahsearch.com/browse?sys_id={sid}"
                       + (f"&page={pn}" if pn else ""))
                nli_t = (f"<div class='lbl'>קטלוג: {html.escape(nli[:70])}"
                         f"</div>" if nli else "")
                panes.append(
                    f"<div class='pane'><div class='lbl'>"
                    f"<a href='{url}' target='_blank'><bdi dir='ltr'>"
                    f"<b>{html.escape(sm)}</b> ({lib})</bdi></a></div>"
                    f"{nli_t}<div class='ev'>{snip(t, s0, s1)}</div></div>")
            fc_txt, fc_col = FLANK_HE.get(fc, (fc, '#777'))
            cards.append(f"""
<div class='card'>
 <div class='head'>
  <span class='p'>דיוק־דלי {min(pl, 0.99):.2f}</span>
  <span class='chip' style='color:{fc_col}'>{fc_txt}{
    f" <bdi dir='ltr'>({fd:.2f})</bdi>" if fd is not None and fd >= 0 else ""}</span>
 </div>
 <div class='stats'>אורך התאמה <bdi dir='ltr'>{alen}</bdi> ·
  מרחק <bdi dir='ltr'>{dens:.2f}</bdi> ·
  צד קטן <bdi dir='ltr'>{minlen}</bdi> אות ·
  q <bdi dir='ltr'>{qv:.3f}</bdi></div>
 <div class='panes'>{''.join(panes)}</div>
</div>""")
        body.append(f"<h2>{label} ({len(cards)})</h2>")
        body.extend(cards)
        n_cards += len(cards)
        print(f"  {label}: {len(cards)}")

    n_all = wide.execute("SELECT COUNT(*) FROM track2_wide").fetchone()[0]
    n_small = wide.execute(
        "SELECT COUNT(*) FROM track2_wide WHERE stratum <= 1").fetchone()[0]
    doc = f"""<!DOCTYPE html><html lang='he' dir='rtl'><head>
<meta charset='utf-8'><title>זוגות שברים קטנים — הרובד הרחב (Track-2)</title>
<style>
 body{{font-family:Segoe UI,Arial;max-width:1150px;margin:20px auto;
 padding:0 12px;background:#fafaf7;color:#222}}
 .card{{background:#fff;border:1px solid #ddd;border-radius:8px;
 margin:10px 0;padding:8px 14px}}
 .head{{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}}
 .p{{background:#4a148c;color:#fff;border-radius:6px;padding:2px 8px;
 font-weight:bold}}
 .chip{{font-size:12px;font-weight:bold;border:1px solid currentColor;
 border-radius:10px;padding:1px 8px}}
 .stats{{font-size:12.5px;color:#666;margin:4px 0}}
 .ev{{direction:rtl;text-align:right;font-size:14.5px;line-height:1.7;
 white-space:pre-wrap;background:#fcfcf9;border:1px solid #eee;
 border-radius:6px;padding:6px 8px}}
 .ev mark{{background:#d9c2f0}} .ev .ctx{{color:#aaa}}
 .lbl{{font-size:12px;color:#777;margin-top:6px}}
 .panes{{display:flex;gap:12px;flex-wrap:wrap}}
 .pane{{flex:1 1 340px;min-width:0}}
 h2{{border-bottom:2px solid #4a148c;padding-bottom:4px;margin-top:28px}}
 .note{{background:#f3ebfb;border:1px solid #ddc9f0;border-radius:8px;
 padding:10px 14px;font-size:14px}}
</style></head><body>
<h1>זוגות "אותו חיבור?" בין שברים קטנים — הרובד הרחב</h1>
<div class='note'>
<b>מה זה:</b> זוגות עמודים שדומים זה לזה מתחת לסף הקבלה המחמיר של
מפת־ההעתקות (ולכן לא הופיעו בה), עם כיול הסתברותי מול פתיונות
(עמודים מעורבבים שהוזרקו לריצה עצמה). כאן רק זוגות שבהם הצד הקטן
≤ 300 אות — האוכלוסייה שביקשת (שברים קטנים). "דיוק־דלי" =
אחוז האמת המשוער של כל הדלי שהזוג יושב בו (לא הסתברות פרטית לזוג;
הכיול שמרני — דליים דלי־ראיות קיבלו חסם עליון). סה"כ ברובד:
{n_all:,} זוגות, מהם {n_small:,} בשכבות הקטנות; כאן {n_cards} לדוגמה.
זוגות מאותה מדף־משפחה (join פיזי) סוננו החוצה, וכן ראיות שרובן
טקסט קנוני ממוסך.
</div>
{''.join(body)}
</body></html>"""
    open(OUT, 'w', encoding='utf-8').write(doc)
    print(f"wrote {OUT} ({n_cards} cards)")
    wide.close()
    corp.close()


if __name__ == '__main__':
    main()

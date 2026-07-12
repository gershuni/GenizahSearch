# -*- coding: utf-8 -*-
"""MAPV2-A — inject the agent annotations into the discovery deck HTML as a
colored verdict banner per card -> mapv2_discovery_deck_annotated.html.
Card N = Nth '<div class='card'>' occurrence (document order == card_no order;
the statutory section is empty in v10).

Usage: python -X utf8 -u mapv2_deck_annotate_html.py
"""
import argparse
import html
import json
import os

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
DECK = os.path.join(PROBE, 'review', 'full_deck', 'mapv2_discovery_deck.html')
_ap = argparse.ArgumentParser()
_ap.add_argument('--tag', default='')
_args = _ap.parse_args()
MERGED = os.path.join(PROBE, 'results', 'deck_annotation' + _args.tag,
                      'merged_annotations.json')
OUT = os.path.join(PROBE, 'review', 'full_deck',
                   'mapv2_discovery_deck_annotated.html')

STYLE = {
    'DISCOVERY': ('תגלית — ידע חדש', '#2e7d32', '#e8f5e9'),
    'CITATION': ('ציטוט', '#e65100', '#fff3e0'),
    'PARALLEL': ('מקבילה ספרותית', '#1565c0', '#e3f2fd'),
    'KNOWN-SAME': ('ידוע בקטלוג — אותו חיבור', '#616161', '#f5f5f5'),
    'KNOWN-DEPENDENCE': ('תלות ספרותית ידועה', '#795548', '#efebe9'),
    'SHARED-SOURCE': ('מקור משותף — דליפה', '#c62828', '#ffebee'),
}
CONF_HE = {'high': 'ביטחון גבוה', 'medium': 'ביטחון בינוני',
           'low': 'ביטחון נמוך'}


def banner(a):
    v = a['verdict']
    label, col, bg = STYLE.get(v, (v, '#333', '#eee'))
    if v == 'CITATION' and a.get('direction') == 'work_quotes_pages_source':
        label, col, bg = ('ציטוט בכיוון הפוך — מועמד-מציאה',
                          '#6a1b9a', '#f3e5f5')
    eq = a.get('name_equation')
    eq_h = (f" · <bdi>{html.escape(str(eq))}</bdi>" if eq else "")
    return (f"<div style='background:{bg};border:1.5px solid {col};"
            f"border-radius:6px;padding:5px 10px;margin:2px 0 6px'>"
            f"<b style='color:{col}'>‹{a['card_no']}› {label}</b>"
            f" <span style='color:#777;font-size:12px'>"
            f"({CONF_HE.get(a.get('confidence'), '?')}"
            f"{', חדש לקטלוג' if a.get('novelty') else ''})</span>{eq_h}"
            f"<div style='font-size:13px;color:#444;margin-top:2px'>"
            f"{html.escape(a.get('reasoning_he', ''))}</div></div>")


def main():
    merged = json.load(open(MERGED, encoding='utf-8'))
    ann = {c['card_no']: c['annotation'] for c in merged
           if c.get('annotation')}
    doc = open(DECK, encoding='utf-8').read()
    parts = doc.split("<div class='card'>")
    assert len(parts) - 1 == len(ann), (len(parts) - 1, len(ann))
    out = [parts[0].replace(
        '<h1>', "<h1>מוער · ", 1)]
    for i, body in enumerate(parts[1:], 1):
        out.append("<div class='card'>" + banner(ann[i]) + body)
    legend = ("<div class='note' style='margin-top:8px'><b>מקרא ההערות:</b> "
              + " · ".join(f"<b style='color:{c}'>{t}</b>"
                           for t, c, _ in STYLE.values())
              + " — סווג ע\"י 4 סוכני Opus לפי הטקסטים + כותרות NLI/FJMS "
                "(בקרת שמות-נרדפים). הדוח המלא: "
                "results/mapv2_deck_annotation.md</div>")
    res = "".join(out)
    res = res.replace("</h1>", "</h1>" + legend, 1)
    open(OUT, 'w', encoding='utf-8').write(res)
    print(f"wrote {OUT} ({len(ann)} banners)")


if __name__ == '__main__':
    main()

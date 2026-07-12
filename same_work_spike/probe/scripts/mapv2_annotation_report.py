# -*- coding: utf-8 -*-
"""MAPV2-A aggregate — merge the 4 agent annotation chunks with the enriched
card dump, compute the honest verdict/novelty statistics, and emit the data
tables for results/mapv2_deck_annotation.md (narrative + recommendations are
appended by the orchestrator).

Usage: python -X utf8 -u mapv2_annotation_report.py
"""
import argparse
import json
import os
from collections import Counter, defaultdict

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
CARDS = os.path.join(PROBE, 'review', 'full_deck',
                     'mapv2_deck_cards_enriched.json')
_ap = argparse.ArgumentParser()
_ap.add_argument('--tag', default='',
                 help="suffix for annotation dir + report (e.g. _v11)")
_ap.add_argument('--chunks', type=int, default=4)
_args = _ap.parse_args()
ANN_DIR = os.path.join(PROBE, 'results', 'deck_annotation' + _args.tag)
OUT = os.path.join(PROBE, 'results',
                   f'mapv2_deck_annotation{_args.tag}.md')
OUT_JSON = os.path.join(ANN_DIR, 'merged_annotations.json')

VORDER = ['DISCOVERY', 'WITNESS', 'CITATION', 'PARALLEL', 'KNOWN-SAME',
          'KNOWN-DEPENDENCE', 'SHARED-SOURCE', 'NO-RELATION']
VHE = {
    'DISCOVERY': 'תגלית (זיהוי חדש)',
    'WITNESS': 'עד נוסח (עדות קטע, לא זיהוי חיבור)',
    'CITATION': 'ציטוט (צד אחד מצטט את השני)',
    'PARALLEL': 'מקבילה ספרותית',
    'KNOWN-SAME': 'ידוע בקטלוג/ביבליוגרפיה (אותו חיבור)',
    'KNOWN-DEPENDENCE': 'תלות ספרותית ידועה',
    'SHARED-SOURCE': 'מקור משותף (דליפה)',
    'NO-RELATION': 'אין קשר',
}


def main():
    cards = {c['card_no']: c for c in json.load(open(CARDS, encoding='utf-8'))}
    ann = {}
    problems = []
    for i in range(1, _args.chunks + 1):
        p = os.path.join(ANN_DIR, f'agent_chunk_{i}.json')
        if not os.path.exists(p):
            problems.append(f"MISSING chunk file: {p}")
            continue
        for a in json.load(open(p, encoding='utf-8')):
            n = a.get('card_no')
            if n is None or n not in cards:
                problems.append(f"chunk {i}: bad card_no {n!r}")
                continue
            a['verdict'] = (a.get('verdict') or '?').upper().strip()
            if n in ann:
                problems.append(f"duplicate annotation for card {n}")
            ann[n] = a
    missing = sorted(set(cards) - set(ann))
    if missing:
        problems.append(f"cards without annotation: {missing}")

    merged = []
    for n in sorted(cards):
        m = dict(cards[n])
        m.pop('page_snippet', None)
        m.pop('ref_snippet', None)
        m['annotation'] = ann.get(n)
        merged.append(m)
    os.makedirs(ANN_DIR, exist_ok=True)
    json.dump(merged, open(OUT_JSON, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)

    rows = [(n, ann[n], cards[n]) for n in sorted(ann)]
    vc = Counter(a['verdict'] for _, a, _ in rows)
    by_sec = defaultdict(Counter)
    for _, a, c in rows:
        by_sec[c['section']][a['verdict']] += 1
        by_sec[c['section']]['n'] += 1
    novel = [(n, a, c) for n, a, c in rows if a.get('novelty')]
    reversed_cites = [(n, a, c) for n, a, c in rows
                      if a['verdict'] == 'CITATION'
                      and a.get('direction') == 'work_quotes_pages_source']
    tr = Counter(a.get('title_relation') or '?' for _, a, _ in rows)
    conf = Counter(a.get('confidence') or '?' for _, a, _ in rows)

    L = ["# MAPV2 — אנוטציה מלאה של חפיסת התגליות (88 כרטיסים)\n"]
    L.append("ארבעה סוכני Opus בלתי-תלויים סיווגו כל כרטיס לפי הטקסטים + "
             "כותרות NLI + זיהויי FJMS, עם בקרת שמות-נרדפים לאותו חיבור. "
             "השאלה המנחה: *האם חוקר ילמד מהכרטיס משהו שאינו כבר בקטלוגים?*\n")
    if problems:
        L.append("## ⚠ בעיות איסוף\n")
        L.extend(f"- {p}" for p in problems)
        L.append("")

    L.append("## התפלגות הפסיקות\n")
    L.append("| פסיקה | n | % |")
    L.append("|---|--|--|")
    total = len(rows)
    for v in VORDER:
        if vc.get(v):
            L.append(f"| {VHE.get(v, v)} ({v}) | {vc[v]} | "
                     f"{vc[v]/total:.0%} |")
    for v in vc:
        if v not in VORDER:
            L.append(f"| {v} (לא בטקסונומיה) | {vc[v]} | {vc[v]/total:.0%} |")
    L.append(f"| **סה\"כ** | **{total}** | |")

    n_disc = vc.get('DISCOVERY', 0)
    n_new = len(novel)
    L.append(f"\n**שורת המחץ:** תגליות אמיתיות: **{n_disc}** "
             f"({n_disc/total:.0%}); כרטיסים עם ידע חדש כלשהו "
             f"(novelty=true, כולל ציטוט-הפוך ומקבילות לא-מובנות-מאליהן): "
             f"**{n_new}** ({n_new/total:.0%}); ציטוט בכיוון הפוך "
             f"(מועמד-מציאה): **{len(reversed_cites)}**.\n")

    L.append("## לפי מדור (רצועת P)\n")
    L.append("| מדור | n | " + " | ".join(v[:9] for v in VORDER) + " |")
    L.append("|---|--|" + "--|" * len(VORDER))
    for sec, cnt in by_sec.items():
        L.append(f"| {sec} | {cnt['n']} | "
                 + " | ".join(str(cnt.get(v, 0)) for v in VORDER) + " |")

    L.append("\n## יחס הכותרות (title_relation)\n")
    L.append("| יחס | n |")
    L.append("|---|--|")
    for k, v in tr.most_common():
        L.append(f"| {k} | {v} |")
    L.append(f"\nביטחון הסוכנים: " +
             ", ".join(f"{k}: {v}" for k, v in conf.most_common()))

    def card_line(n, a, c):
        eq = a.get('name_equation')
        return (f"- **#{n}** [{c['section'].split(' —')[0]}] "
                f"{c['shelfmark']} → {c['work_name'][:60]} "
                f"(P={c['p_shown']:.2f}, {a.get('confidence','?')})"
                + (f" · השוואת שמות: {eq}" if eq else "")
                + f"\n  {a.get('reasoning_he','')}")

    L.append("\n## התגליות (DISCOVERY)\n")
    for n, a, c in rows:
        if a['verdict'] == 'DISCOVERY':
            L.append(card_line(n, a, c))
    if reversed_cites:
        L.append("\n## ציטוט בכיוון הפוך — מועמדי-מציאה\n")
        for n, a, c in reversed_cites:
            L.append(card_line(n, a, c))
    L.append("\n## ידוע-בקטלוג (KNOWN-SAME) — אישורים, לא תגליות\n")
    for n, a, c in rows:
        if a['verdict'] == 'KNOWN-SAME':
            L.append(card_line(n, a, c))
    L.append("\n## תלות ספרותית ידועה (KNOWN-DEPENDENCE)\n")
    for n, a, c in rows:
        if a['verdict'] == 'KNOWN-DEPENDENCE':
            L.append(card_line(n, a, c))

    L.append("\n## טבלת כל הכרטיסים\n")
    L.append("| # | פסיקה | ביטחון | כותרות | חדש? | P | band | חיבור נטען "
             "| כותרת NLI |")
    L.append("|--|---|---|---|---|---|---|---|---|")
    for n, a, c in rows:
        L.append(f"| {n} | {a['verdict']} | {a.get('confidence','?')[:3]} | "
                 f"{(a.get('title_relation') or '?')[:18]} | "
                 f"{'✓' if a.get('novelty') else ''} | {c['p_shown']:.2f} | "
                 f"{c['band']} | {c['work_name'][:42]} | "
                 f"{(c['nli_title'] or '—')[:42]} |")

    open(OUT, 'w', encoding='utf-8').write("\n".join(L) + "\n")
    print(f"wrote {OUT} ({total} annotated) + {OUT_JSON}")
    print("verdicts:", dict(vc))
    print("novelty:", n_new, "reversed:", len(reversed_cites))
    if problems:
        print("PROBLEMS:", problems)


if __name__ == '__main__':
    main()

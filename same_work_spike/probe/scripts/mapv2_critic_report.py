# -*- coding: utf-8 -*-
"""MAPV2-14 — aggregate the Fable critic panel (acting for Hillel) over
deck v13: merge critic_chunk_N.json with the Opus annotations, emit

  - results/mapv2_deck_critic_v13.md  (Hebrew report: agreement, corrected
    section precision, overturns, escalations, catalog corrections)
  - review/full_deck/mapv2_v13_critic_grades.json  (review-page export
    format — the critic's grades standing in for the human export)

Usage: python -X utf8 -u mapv2_critic_report.py [--chunks 5]
"""
import argparse
import json
import os
from collections import Counter, defaultdict

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
_ap = argparse.ArgumentParser()
_ap.add_argument('--chunks', type=int, default=5)
_args = _ap.parse_args()
CDIR = os.path.join(PROBE, 'results', 'deck_critic_v13')
MERGED = os.path.join(PROBE, 'results', 'deck_annotation_v13',
                      'merged_annotations.json')
OUT_MD = os.path.join(PROBE, 'results', 'mapv2_deck_critic_v13.md')
OUT_GR = os.path.join(PROBE, 'review', 'full_deck',
                      'mapv2_v13_critic_grades.json')

GHE = {'discovery': 'תגלית', 'witness': 'עד נוסח', 'citation': 'ציטוט',
       'shared': 'מקור משותף', 'known': 'ידוע', 'formula': 'פורמולה בלבד',
       'norel': 'לא קשור', 'tsarich': 'צ"ע'}


def main():
    cards = {c['card_no']: c for c in
             json.load(open(MERGED, encoding='utf-8'))}
    crit = {}
    problems = []
    for i in range(1, _args.chunks + 1):
        p = os.path.join(CDIR, f'critic_chunk_{i}.json')
        if not os.path.exists(p):
            problems.append(f"MISSING {p}")
            continue
        for a in json.load(open(p, encoding='utf-8')):
            n = a.get('card_no')
            if n not in cards:
                problems.append(f"chunk {i}: bad card_no {n!r}")
                continue
            if n in crit:
                problems.append(f"duplicate card {n}")
            crit[n] = a
    missing = sorted(set(cards) - set(crit))
    if missing:
        problems.append(f"cards without critic review: {missing}")

    rows = [(n, crit[n], cards[n]) for n in sorted(crit)]
    gc = Counter(a['grade'] for _, a, _ in rows)
    n_dis = sum(1 for _, a, _ in rows if not a.get('agree_with_opus'))
    esc = [(n, a, c) for n, a, c in rows if a.get('escalate')]
    corr = [(n, a, c) for n, a, c in rows if a.get('catalog_correction')]

    by_sec = defaultdict(Counter)
    for n, a, c in rows:
        s = c['section'].split(' — ')[0]
        by_sec[s][a['grade']] += 1
        by_sec[s]['n'] += 1

    L = ["# MAPV2 v13 — ביקורת פאנל Fable (במקום הלל)\n"]
    L.append("חמישה סוכני Fable ביקורתיים, מכוילים על הערות הביקורת של הלל "
             "ל-v11, עברו על כל 132 הכרטיסים **עם** פסיקות ה-Opus ביד — "
             "ותפקידם להפריך. כלי נוסף שקיבלו: חיפוש ביבליוגרפי רוחבי בכל "
             "טבלת ה-bibliography (לא רק רשומות כתב־היד).\n")
    if problems:
        L.append("## ⚠ בעיות איסוף\n")
        L.extend(f"- {p}" for p in problems)
        L.append("")

    L.append("## התפלגות ציוני המבקר\n")
    L.append("| ציון | n |")
    L.append("|---|--|")
    for g, v in gc.most_common():
        L.append(f"| {GHE.get(g, g)} ({g}) | {v} |")
    L.append(f"\nחילוקי דעות עם ה-Opus: **{n_dis}/{len(rows)}**; "
             f"הסלמות להלל: **{len(esc)}**; "
             f"מועמדי תיקון קטלוג: **{len(corr)}**.\n")

    L.append("## לפי מדור (ציון המבקר)\n")
    hdr = ['discovery', 'witness', 'citation', 'shared', 'known',
           'formula', 'norel', 'tsarich']
    L.append("| מדור | n | " + " | ".join(hdr) + " |")
    L.append("|---|--|" + "--|" * len(hdr))
    for s, cnt in by_sec.items():
        L.append(f"| {s} | {cnt['n']} | "
                 + " | ".join(str(cnt.get(g, 0)) for g in hdr) + " |")

    L.append("\n## חילוקי הדעות (המבקר מול Opus)\n")
    for n, a, c in rows:
        if a.get('agree_with_opus'):
            continue
        ov = (c.get('annotation') or {}).get('verdict', '?')
        L.append(f"- **#{n}** [{c['section'].split(' — ')[0][:20]}] "
                 f"{c['shelfmark']} → {c['work_name'][:50]}\n"
                 f"  Opus: {ov} ← המבקר: **{GHE.get(a['grade'], a['grade'])}"
                 f"** — {a.get('note_he', '')}")

    L.append("\n## הסלמות — דורש את הלל עצמו\n")
    for n, a, c in esc:
        L.append(f"- **#{n}** {c['shelfmark']} → {c['work_name'][:50]} "
                 f"[{GHE.get(a['grade'], a['grade'])}]\n"
                 f"  {a.get('escalate_reason') or a.get('note_he', '')}")

    if corr:
        L.append("\n## מועמדי תיקון קטלוג\n")
        for n, a, c in corr:
            L.append(f"- **#{n}** {c['shelfmark']} — NLI: "
                     f"\"{(c.get('nli_title') or '—')[:50]}\" → "
                     f"{c['work_name'][:50]}\n  {a.get('note_he', '')}")

    L.append("\n## כל הכרטיסים\n")
    L.append("| # | Opus | מבקר | מסכים? | הערת המבקר |")
    L.append("|--|---|---|---|---|")
    for n, a, c in rows:
        ov = (c.get('annotation') or {}).get('verdict', '?')
        L.append(f"| {n} | {ov} | {a['grade']} | "
                 f"{'✓' if a.get('agree_with_opus') else '✗'} | "
                 f"{a.get('note_he', '')[:90]} |")

    open(OUT_MD, 'w', encoding='utf-8').write("\n".join(L) + "\n")

    # review-page-export-format grades file
    out = [{'card_no': n, 'section': c['section'].split(' — ')[0],
            'shelf': c['shelfmark'], 'work': c['work_name'],
            'opus_verdict': (c.get('annotation') or {}).get('verdict'),
            'grade': a['grade'],
            'note': a.get('note_he') or None,
            'escalate': bool(a.get('escalate')),
            'reviewer': 'fable-critic-panel'}
           for n, a, c in rows]
    json.dump(out, open(OUT_GR, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print(f"wrote {OUT_MD} ({len(rows)} cards) + {OUT_GR}")
    print("grades:", dict(gc))
    print(f"disagreements: {n_dis}, escalations: {len(esc)}, "
          f"catalog corrections: {len(corr)}")
    if problems:
        print("PROBLEMS:", problems)


if __name__ == '__main__':
    main()

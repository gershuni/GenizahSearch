# -*- coding: utf-8 -*-
"""MAPV2-11 — merge Hillel's card-by-card review of the v11 deck
(review/full_deck/mapv2_discovery_deck_annotated_Human_Review.txt) into
merged_annotations.json as a structured `human_review` field, and print the
honest human-verified section numbers.

The verdict classification below is an editorial reading of his Hebrew notes
(done once, by hand); the raw note text is carried verbatim alongside.

Usage: python -X utf8 -u merge_human_review_v11.py
"""
import json
import os
import re

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
TXT = os.path.join(PROBE, 'review', 'full_deck',
                   'mapv2_discovery_deck_annotated_Human_Review.txt')
MERGED = os.path.join(PROBE, 'results', 'deck_annotation_v11',
                      'merged_annotations.json')

# card_no -> hand classification of Hillel's note (2026-07-12 review).
# Classes: discovery_confirmed / known_bib (Friedberg bibliography already
# connects ms<->work) / known_bib_partial (bib covers the work, the specific
# attribution is not there — צ"ע) / witness (valuable passage-level textual
# witness, "עד נוסח") / indirect_witness (secondary use / citation) /
# formula_only / noteworthy (real signal, not a witness) / shared_source /
# known_dependence / rejected
HUMAN = {
    5: 'known_bib',            # + הדמיון ראוי לציון (shared source family)
    6: 'known_bib',
    7: 'witness',
    9: 'indirect_witness',
    12: 'witness',
    13: 'known_dependence',    # + still useful (haggadah may be cut here)
    14: 'witness',
    15: 'known_bib',
    16: 'witness',
    17: 'noteworthy',          # flanks differ -> not a direct witness
    18: 'known_bib',
    19: 'formula_only',
    20: 'discovery_confirmed',   # bib empty; Mosseri catalog ref — לעיין
    21: 'discovery_confirmed',   # JA booklist quoting Otiyot deR. Akiva
    22: 'witness',
    23: 'witness',
    24: 'witness',
    25: 'formula_only',
    26: 'witness',
    27: 'witness',
    28: 'noteworthy',          # shared formula but distinctive
    29: 'witness',
    30: 'known_bib',
    31: 'discovery_confirmed',
    32: 'discovery_confirmed',   # direction: page likely the Arabic SOURCE
    33: 'discovery_confirmed',   # (ref is a תרגום of it)
    34: 'witness',
    35: 'witness',
    36: 'witness',
    37: 'witness',
    38: 'known_bib',
    39: 'known_bib_partial',   # bib mentions exist; פינחס not named — צ"ע
    40: 'known_bib_partial',   # Ibn Jiqatilla not in bib — צ"ע
    41: 'indirect_witness',    # citation ("דרשו")
    42: 'indirect_witness',    # citation; NLI title misleading (correction?)
    43: 'shared_source',
    44: 'witness',             # known-same; useful: seliḥot include THIS one
    45: 'shared_source',       # shared citation
    46: 'witness',
    47: 'known_bib',
    63: 'known_bib',
    65: 'rejected',            # beyond shared citations, flanks dissimilar
}


def parse_notes():
    notes = {}
    for line in open(TXT, encoding='utf-8'):
        m = re.match(r'\s*(\d+)\.\s*(.+)', line.strip())
        if m:
            notes[int(m.group(1))] = m.group(2).strip()
    return notes


def main():
    notes = parse_notes()
    assert set(notes) == set(HUMAN), (
        sorted(set(notes) ^ set(HUMAN)))
    merged = json.load(open(MERGED, encoding='utf-8'))
    by_no = {c['card_no']: c for c in merged}
    for n, cls in HUMAN.items():
        by_no[n]['human_review'] = {
            'reviewer': 'Hillel', 'date': '2026-07-12',
            'class': cls, 'note_he': notes[n]}
    json.dump(merged, open(MERGED, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)

    from collections import Counter
    cc = Counter(HUMAN.values())
    print(f"merged {len(HUMAN)} human verdicts into {MERGED}")
    print("human classes:", dict(cc.most_common()))
    # agreement on the Opus DISCOVERY claims within the reviewed range
    opus_disc = [n for n in HUMAN
                 if (by_no[n].get('annotation') or {}).get('verdict')
                 == 'DISCOVERY']
    conf = [n for n in opus_disc if HUMAN[n] == 'discovery_confirmed']
    bib = [n for n in opus_disc if HUMAN[n].startswith('known_bib')]
    other = [n for n in opus_disc if n not in conf and n not in bib]
    print(f"Opus DISCOVERY cards reviewed: {len(opus_disc)}")
    print(f"  human-confirmed: {len(conf)} {conf}")
    print(f"  killed by Friedberg bibliography: {len(bib)} {bib}")
    print(f"  reclassified other: {len(other)} "
          f"{[(n, HUMAN[n]) for n in other]}")


if __name__ == '__main__':
    main()

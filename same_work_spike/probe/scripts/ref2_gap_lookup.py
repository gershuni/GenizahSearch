# -*- coding: utf-8 -*-
"""REF-2 gap-works lookup: are the residue_naming.md CLEAR/COMPETING works
recoverable from our CLOSED reference universe?

Checks each catalogued work named by the residue clusters against:
  (a) Maagarim filenames  (C:\\Users\\gersh\\Dropbox\\...\\AllTextsOnlyText)
  (b) JA per_doc filenames (+ v1 J: titles)
  (c) ref_corpus v1 titles/authors (id, author, title)
  (d) REF2 new works just ingested (ref_corpus_v2 additions)

REPORT ONLY -- ingests nothing (a human must confirm any match).
Writes ..\\data\\ref2_gap_lookup.json with raw matches for the report.

Run: python -X utf8 -u ref2_gap_lookup.py
"""
import json
import os
import pickle
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
PROBE = os.path.dirname(HERE)
V1_PKL = os.path.join(PROBE, 'data', 'ref_corpus.pkl')
V2_PKL = os.path.join(PROBE, 'data', 'ref_corpus_v2.pkl')
OUT = os.path.join(PROBE, 'data', 'ref2_gap_lookup.json')

MAAGARIM = r"C:\Users\gersh\Dropbox\דיקטה\מאגרים\AllTextsOnlyText"
JA_DIR = r"C:\Users\gersh\Dropbox\דיקטה\JA\ערבית יהודית מעובד\per_doc"

# distinct catalogued works named by the CLEAR/COMPETING residue clusters
# (residue_naming.md 2026-07-09); keys = loose Hebrew substrings, matched
# after stripping apostrophes/geresh/gershayim/quotes.
GAP_WORKS = [
    ("שרח אלמקדמאת (פירוש הפרשיות, דוד בן בועז?)", ["אלמקדמאת"]),
    ("תפסיר אלאלפאט' אלצעבה / שרח אלאלפאץ", ["אלאלפאט", "אלאלפאץ", "אלאלפאצ"]),
    ("כתאב אלאפעאל ד'ואת חרוף אללין (חיוג')", ["חרוף אללין", "דואת חרוף"]),
    ("כתאב אפעאל דואת אלמתלין (חיוג')", ["אלמתלין"]),
    ("כתאב אלמסתלחק (אבן ג'נאח)", ["אלמסתלחק", "מסתלחק"]),
    ("ספור על אסתר / קצת אסתר (סיפור ערבי-יהודי בחרוזים)",
     ["קצת אסתר", "קצה אסתר", "ספור על אסתר", "סיפור אסתר"]),
    ("קצת חנה / ספור על חנה", ["קצת חנה", "קצה חנה", "ספור על חנה", "סיפור חנה"]),
    ("תפסיר רס\"ג (תרגום התורה, סעדיה גאון)", ["תפסיר", "רסג"]),
    ("פירוש יפת בן עלי למקרא", ["יפת"]),
    ("כתאב אלהדאיה", ["אלהדאיה", "הדאיה"]),
    ("כתאב אלטריפות (הל' שחיטה/טריפות, רס\"ג?)", ["אלטריפות", "טריפות"]),
    ("שרוט אלדבאחה (הל' שחיטה קראיות)", ["אלדבאחה", "שרוט"]),
    ("תחכמוני (אלחריזי)", ["תחכמוני"]),
    ("כתאב אלמשתמל עלי אלאצול ואלפצול (אבו אלפרג' הארון)", ["אלמשתמל"]),
    ("כתאב אלכאפי (אבו אלפרג' הארון)", ["אלכאפי"]),
    ("כתר מלכות (אבן גבירול)", ["כתר מלכות"]),
    ("שרח אלאלפאט' אלמתג'אנסה / ספר הענק (משה אבן עזרא)",
     ["מתגאנסה", "הענק", "אלתגניס", "תגניס"]),
    ("כתאב אלאצול / ספר השורשים בערבית (אבן ג'נאח)",
     ["אלאצול", "השרשים", "השורשים"]),
    ("כתאב אלאנואר ואלמראקב (קרקסאני)", ["אלאנואר"]),
    ("ספר מצוות [לוי בן יפת]", ["לוי בן יפת"]),
    ("ספר מצוות [יפת בן דוד אבן צגיר]", ["צגיר"]),
    ("תלכ'יץ תפסיר אבן נוח (אבו אלפרג' הארון)", ["אבן נוח", "תלכיץ"]),
    ("כתאב אלמרשד [שמואל בן משה המערבי]", ["אלמרשד"]),
    ("רסאלה אלתנביה", ["אלתנביה"]),
    ("כתאב אלמועט'ה", ["אלמועטה", "מועטה"]),
    ("דלאלה אלחאירין / מורה נבוכים (רמב\"ם)",
     ["דלאלה", "אלחאירין", "מורה נבוכים", "מורה הנבוכים"]),
    ("סדור מנהג קראים (סידור קראי)", ["קראים", "מנהג קראים"]),
]

_STRIP = dict.fromkeys(map(ord, "'\"`׳״‘’“”."), None)


def norm(s: str) -> str:
    return s.translate(_STRIP)


def find(keys, haystacks):
    """haystacks: list of (label, normalized_text). Return matched labels."""
    out = []
    nkeys = [norm(k) for k in keys]
    for label, txt in haystacks:
        if any(k in txt for k in nkeys):
            out.append(label)
    return out


def main():
    maag = [(fn, norm(fn)) for fn in sorted(os.listdir(MAAGARIM))
            if fn.endswith('.txt')]
    ja = [(fn, norm(fn)) for fn in sorted(os.listdir(JA_DIR))
          if fn.endswith('.txt')]
    print(f'maagarim files: {len(maag)}, JA files: {len(ja)}', flush=True)

    v1 = pickle.load(open(V1_PKL, 'rb'))
    v1_hay = [(f"{w['id']} [{w.get('cat', '')}] "
               f"{w.get('author', '')}--{w.get('title', '')}",
               norm(f"{w.get('author', '')}--{w.get('title', '')}"))
              for w in v1]
    v2 = pickle.load(open(V2_PKL, 'rb'))
    ref2_hay = [(f"{w['id']} [{w['cat']}] {w.get('title', '')}",
                 norm(f"{w.get('author', '')}--{w.get('title', '')}"))
                for w in v2 if w['id'].startswith('REF2:')]
    print(f'v1 works: {len(v1)}, REF2 works: {len(ref2_hay)}', flush=True)

    results = []
    for name, keys in GAP_WORKS:
        m_maag = find(keys, maag)
        m_ja = find(keys, ja)
        m_v1 = find(keys, v1_hay)
        m_ref2 = find(keys, ref2_hay)
        results.append({
            'work': name, 'keys': keys,
            'maagarim_files': m_maag, 'ja_files': m_ja,
            'v1_titles': m_v1, 'ref2_new': m_ref2,
            'n': [len(m_maag), len(m_ja), len(m_v1), len(m_ref2)],
        })
        print(f"\n== {name}  (keys: {', '.join(keys)})")
        print(f"   maagarim={len(m_maag)}  ja={len(m_ja)}  "
              f"v1={len(m_v1)}  ref2={len(m_ref2)}")
        for lab in m_maag[:6]:
            print(f"   [MAAG] {lab}")
        if len(m_maag) > 6:
            print(f"   [MAAG] ... +{len(m_maag) - 6} more")
        for lab in m_ja[:6]:
            print(f"   [JA]   {lab}")
        for lab in m_v1[:6]:
            print(f"   [V1]   {lab}")
        if len(m_v1) > 6:
            print(f"   [V1]   ... +{len(m_v1) - 6} more")
        for lab in m_ref2[:4]:
            print(f"   [REF2] {lab}")

    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=1)
    print(f'\nwrote {OUT}', flush=True)


if __name__ == '__main__':
    main()

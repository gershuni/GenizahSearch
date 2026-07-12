# -*- coding: utf-8 -*-
"""MAPV2-11 — Friedberg bibliography gate.

Hillel's review of the v11 deck (2026-07-12) found the deck's biggest blind
spot: 9 of 17 Opus-labeled "discoveries" were already known in the Friedberg
BIBLIOGRAPHY — the `bibliography` table of fjms_enrichment.db (427K rows,
per-AlmaId), which the pipeline never queried. The title gate saw only
NLI titles + FJMS catalog identifications.

Design (validated on his 12 gold cases):
  - KILL rule = the CLAIMED work/author matches a bib row's title/author
    tokens (>=2 distinct matched claim tokens, at least one non-genre).
    Bib presence alone must NOT kill: T-S Loan 149 (card #21) has 57 bib
    rows yet its booklist-quotes-Otiyot-deR.-Akiva connection is a genuine
    find. TranscriptionType=Full rows likewise only inform, never kill.
  - Everything else is DISPLAY: cards carry the top bib rows so human/Opus
    annotators catch what token matching can't (e.g. English-only rows).

API:
  bg = BibGate()                      # loads + groups bibliography once
  cls, ev = bg.classify(sys_id, "author — work name")
      cls in known_bib / published_full / bib_partial / bib_mentions /
             bib_empty; ev = matched/loudest row summary or None
  bg.display(sys_id, k=3) -> [str]    # top rows for card display

Validate: python -X utf8 -u bib_gate.py --validate
"""
import re
import sqlite3

FJMS_DB = r"C:\Genizahsearch\fist_data\fjms_enrichment.db"

# function words + relational tokens that never anchor a work identity
STOP = {'בר', 'בן', 'בת', 'רבי', 'ר', 'רב', 'של', 'על', 'אל', 'מן', 'עם',
        'לא', 'ידוע', 'מחבר', 'נוסח', 'קטע', 'קטעים', 'חלק', 'אבו', 'מר',
        'לפי', 'פי', 'מאת', 'ליד', 'בין', 'או', 'גם', 'את', 'זה', 'עוד'}
# genre / liturgical-frame words: real signal only alongside a strong token
WEAK = {'פיוט', 'פיוטים', 'פיוטי', 'יוצר', 'יוצרות', 'קדושתא', 'קדושתאות',
        'קרובות', 'קרובה', 'סליחות', 'סליחה', 'קינות', 'קינה', 'תפילה',
        'תפילות', 'ברכה', 'ברכות', 'ברכת', 'מחזור', 'סידור', 'תרגום',
        'מדרש', 'פירוש', 'הלכות', 'שיר', 'שירי', 'שירים', 'שירת', 'תוספת',
        'סדר', 'עבודה', 'שבת', 'שבתות', 'מועדים', 'מועדי', 'שנה', 'השנה',
        'גניזה', 'הגניזה', 'ספר', 'ספרים', 'כתבי', 'יד', 'גאון', 'הגדול',
        'הימים', 'ימים', 'נוראים', 'כיפורים', 'פסח', 'חול', 'קבע', 'קודש'}
# Hebrew token -> transliterations to look for in English bib fields
BRIDGE = {
    'תרגום': ('targum',), 'קרובות': ('qerovot', 'qedushta'),
    'קדושתא': ('qedushta',), 'קדושתאות': ('qedushtaot', 'qedushta'),
    'יוצרות': ('yotserot', 'yotser', 'yozer'), 'פיוט': ('piyyut',),
    'פיוטי': ('piyyut',), 'סליחות': ('selihot', 'seliha'),
    'קינה': ('qinah', 'kinah'), 'קינות': ('qinot', 'kinot'),
    'מחזור': ('mahzor',), 'הגדה': ('haggada', 'hagada'),
    'יהודה': ('yehudah', 'yehuda', 'judah'),
    'יצחק': ('yizhak', 'isaac', 'yitzhak', 'itzhak'),
    'שלמה': ('shelomo', 'solomon'), 'משה': ('moshe', 'moses'),
    'יוסף': ('yosef', 'joseph'), 'אלעזר': ('eleazar', 'elazar'),
    'שמעון': ('shimon', 'simon', 'simeon'),
    'אברהם': ('avraham', 'abraham'), 'שמואל': ('shmuel', 'samuel'),
    'סעדיה': ('saadia', 'saadya', 'saadiah'), 'האיי': ('hai',),
}
_TOKEN_RE = re.compile(r'[א-ת]+')
_PREFIXES = ('ו', 'ה', 'ב', 'ל', 'מ', 'כ', 'ש', 'ד')
_SUFFIXES = ('ותיו', 'אות', 'ות', 'ים', 'ין', 'יהם', 'יו', 'י', 'ה', 'ת')

_MATCH_FIELDS = ('RunningTitleHeb', 'RunningTitle', 'TitleAcronymHeb',
                 'TitleAcronym', 'ArticleName', 'ArticleAuthorHeb',
                 'NoteForDisplay')


def _stem(t):
    for suf in _SUFFIXES:
        if t.endswith(suf) and len(t) - len(suf) >= 3:
            return t[:-len(suf)]
    return t


def _variants(t):
    """Prefix-stripped + stemmed match keys for one token."""
    forms = {t}
    s = t
    for _ in range(2):
        if len(s) > 3 and s[0] in _PREFIXES:
            s = s[1:]
            forms.add(s)
    return forms | {_stem(x) for x in forms if len(_stem(x)) >= 3}


def heb_tokens(s):
    s = (s or '').replace('״', '').replace('׳', '').replace("'", '')
    return [t for t in _TOKEN_RE.findall(s) if len(t) >= 2 and t not in STOP]


def _base(t):
    """Prefix-stripped base form (for WEAK/BRIDGE lookups)."""
    s = t
    for _ in range(2):
        if len(s) > 3 and s[0] in _PREFIXES:
            s = s[1:]
    return s


class BibGate:
    def __init__(self, db=FJMS_DB):
        con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
        self.rows = {}
        for r in con.execute(
                "SELECT AlmaId, RunningTitle, RunningTitleHeb, "
                "TitleAcronymHeb, TitleAcronym, ArticleName, "
                "ArticleAuthorHeb, ArticleAuthorEng, NoteForDisplay, "
                "MentionType, TranscriptionType, TitleYear "
                "FROM bibliography"):
            self.rows.setdefault(str(r[0]), []).append(r[1:])
        con.close()
        self._tok_cache = {}
        self._cls_cache = {}

    def _row_keys(self, sid):
        """Per-row (hebrew_variant_set, english_lower) for a manuscript."""
        if sid not in self._tok_cache:
            out = []
            for r in self.rows.get(sid, []):
                heb = set()
                eng = []
                for v in r[:8]:
                    if not v:
                        continue
                    v = str(v)
                    for t in heb_tokens(v):
                        heb |= _variants(t)
                    eng.append(v.lower())
                out.append((heb, ' '.join(eng)))
            self._tok_cache[sid] = out
        return self._tok_cache[sid]

    def classify(self, sid, claim_text):
        sid = str(sid)
        key = (sid, claim_text)
        if key in self._cls_cache:
            return self._cls_cache[key]
        rows = self.rows.get(sid, [])
        res = ('bib_empty', None)
        if rows:
            claim = heb_tokens(claim_text)
            best = None
            for (heb, eng), raw in zip(self._row_keys(sid), rows):
                matched = set()
                strong = 0
                for t in claim:
                    b = _base(t)
                    hit = bool(_variants(t) & heb) or any(
                        w in eng for w in BRIDGE.get(b, ()))
                    if hit and b not in matched:
                        matched.add(b)
                        if b not in WEAK:
                            strong += 1
                if len(matched) >= 2 and strong >= 1:
                    if best is None or len(matched) > best[0]:
                        best = (len(matched), raw)
            if best:
                res = ('known_bib', self._fmt(best[1]))
            else:
                tt = {str(r[9]) for r in rows}
                if 'Full' in tt:
                    res = ('published_full', self._fmt(
                        next(r for r in rows if str(r[9]) == 'Full')))
                elif 'Partial' in tt:
                    res = ('bib_partial', None)
                else:
                    res = ('bib_mentions', None)
        self._cls_cache[key] = res
        return res

    @staticmethod
    def _fmt(r):
        (rt, rth, tah, ta, art, aah, aae, note, mt, tt, yr) = r
        title = rth or rt or art or tah or ta or note or '?'
        who = aah or aae or ''
        bits = [str(title)[:70]]
        if who:
            bits.append(str(who)[:30])
        if yr:
            bits.append(str(yr))
        tags = '/'.join(x for x in (mt, tt) if x and x != 'None')
        if tags:
            bits.append(tags)
        return ' · '.join(bits)

    def display(self, sid, k=3):
        rows = self.rows.get(str(sid), [])

        def rank(r):
            mt, tt = r[8], str(r[9])
            return (0 if mt == 'Discussion' else 1,
                    0 if tt == 'Full' else 1 if tt == 'Partial' else 2)
        out = []
        seen = set()
        for r in sorted(rows, key=rank):
            f = self._fmt(r)
            base = f.split(' · ')[0]
            if base in seen:
                continue
            seen.add(base)
            out.append(f)
            if len(out) >= k:
                break
        if len({self._fmt(r).split(' · ')[0] for r in rows}) > k:
            out.append(f"(+ עוד {len(rows) - k} רשומות)")
        return out


def _validate():
    import json
    PROBE = r"C:\Genizahsearch\same_work_spike\probe"
    cards = {c['card_no']: c for c in json.load(open(
        PROBE + r"\review\full_deck\mapv2_deck_cards_enriched.json",
        encoding='utf-8'))}
    # gold from Hillel's 2026-07-12 review
    MUST_FIRE = [6, 15, 18, 30, 38, 47]      # bib names the claimed work
    NICE_FIRE = [5, 39, 40, 63]              # partial/English-only — bonus
    MUST_NOT = [20, 21, 31, 32, 33]          # human-confirmed discoveries
    bg = BibGate()
    ok = True
    for group, nos in (('MUST_FIRE', MUST_FIRE), ('NICE_FIRE', NICE_FIRE),
                       ('MUST_NOT', MUST_NOT)):
        for n in nos:
            c = cards[n]
            cls, ev = bg.classify(c['sys_id'], c['work_name'])
            fired = cls == 'known_bib'
            bad = (group == 'MUST_FIRE' and not fired) or \
                  (group == 'MUST_NOT' and fired)
            ok &= not bad
            mark = 'XX' if bad else 'ok'
            print(f"[{mark}] #{n} ({group}) {cls:15s} "
                  f"{c['work_name'][:45]}"
                  + (f" | {ev}" if ev else ""))
    print('VALIDATION', 'PASS' if ok else 'FAIL')


if __name__ == '__main__':
    import sys
    if '--validate' in sys.argv:
        _validate()

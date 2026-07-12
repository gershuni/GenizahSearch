# -*- coding: utf-8 -*-
"""MAPV2-9 title gate — deterministic router comparing a deck claim (work
name + category) against what the catalogs already say about the manuscript
(NLI title line + FJMS GenizahTitleOrgTitle identifications).

Classes (validated against the 88 MAPV2-A gold annotations):
  generic_or_absent   catalog has no specific-work identification
                      -> תגליות section (gold: 75% true discoveries)
  same_work           a catalog title names the claimed work
  name_variant        ... under a known different name (variant lexicon)
                      -> both route to אישורי קטלוג (gold: 100% known)
  known_quoter        catalog names a work in a known quoting relation with
                      the claim -> suppressed (gold: 77% known dependence)
  different_specific  catalog names an unrelated specific work
                      -> "הקטלוג אומר אחרת" suspect section (gold: 85%
                      shared-source; the exceptions are catalog corrections)

Error-direction design: hard scholarly name equations we cannot encode fail
toward different_specific (safe: quarantined, not sold as discovery). The
one direction that would poison the discovery section — a specific title
read as generic — is guarded by a conservative vocabulary test (EVERY token
must be generic for a title to count as generic).

CLI validation:  python -X utf8 -u title_gate.py   (needs
review/full_deck/mapv2_deck_cards_enriched.json + gold annotations)
"""
import json
import os
import re
import sqlite3
from collections import defaultdict

PROBE = r"C:\Genizahsearch\same_work_spike\probe"
FJMS_DB = r"C:\Genizahsearch\fist_data\fjms_enrichment.db"

_NIKUD = re.compile(r'[֑-ׇ]')
_NONWORD = re.compile(r"[^א-תa-zA-Z0-9' ]+")

# tokens that never identify a specific work (Hebrew + English NLI vocab).
# A title is "generic" only if ALL its tokens are here (or connectors).
GENERIC_TOKENS = {
    # fragment/document words
    'קטע', 'קטעים', 'קטעי', 'דף', 'דפים', 'עלה', 'עלים', 'שרידים', 'שריד',
    'מצאי', 'רשימה', 'רשימות', 'לקט', 'ליקוטים', 'קובץ', 'קבץ', 'אוסף',
    'מסמך', 'מסמכים', 'מכתב', 'מכתבים', 'אגרת', 'אגרות', 'טיוטה',
    'חיבור', 'חבור', 'חיבורים', 'ספר', 'ספרים', 'כתב', 'כתבי', 'יד',
    # genre words
    'פיוט', 'פיוטים', 'פיוטי', 'שיר', 'שירים', 'שירה', 'תפילה', 'תפלה',
    'תפילות', 'תפלות', 'ברכה', 'ברכות', 'סידור', 'סדור', 'מחזור', 'קרובה',
    'קרובות', 'סליחות', 'סליחה', 'קינות', 'קינה', 'הושענות', 'יוצרות',
    'זמירות', 'פזמונים', 'בקשות', 'בקשה', 'קבע', 'סדרי', 'סדר',
    'מדרש', 'מדרשים', 'מדרשי', 'אגדה', 'אגדות', 'אגדיים', 'הלכה', 'הלכות',
    'הלכתית', 'הלכתיים', 'פסקים', 'שאלות', 'ותשובות', 'תשובות', 'פרשנות',
    'פירוש', 'פרוש', 'פירושים', 'פרושים', 'באור', 'ביאור', 'ביאורים',
    'דקדוק', 'דקדוקית', 'מילון', 'מלון', 'לקסיקון', 'מסורה', 'טעמים',
    'פילוסופיה', 'קבלה', 'מאגיה', 'מרשמים', 'מאגיים', 'רפואה', 'רפואות',
    'אסטרולוגיה', 'גורלות', 'קמיע', 'קמיעות', 'חשבונות', 'רשימת',
    'ספרות', 'עממית', 'תיעודי', 'חומר', 'תעודות', 'תעודה', 'משפטי',
    'מסמכי', 'גאונים', 'גאונית', 'תרגום', 'תרגומים', 'משלים', 'סיפורים',
    'ספור', 'סיפור', 'מעשיות', 'מעשה', 'דרשות', 'דרשה', 'דרוש',
    'פרקי', 'פרק', 'פרקים', 'מסכת', 'מסכתות', 'עניינים', 'ענייני', 'עניני',
    'שונים', 'שונות', 'לא', 'מזוהה', 'מזוהים', 'זיהוי', 'ראשוני', 'טעון',
    'בדיקה', 'חוזרת', 'שרח', 'תפסיר',
    # language/community words
    'בערבית', 'ערבית', 'יהודית', 'עברית', 'בעברית', 'ארמית', 'בארמית',
    'עברי', 'ערבי', 'ארמי', 'קראית', 'קראים', 'שומרונית', 'לאדינו',
    'תימני', 'תימנית',
    # liturgical calendar words
    'לשבת', 'שבת', 'לחול', 'חול', 'המועד', 'לראש', 'ראש', 'השנה', 'לשנה',
    'ליום', 'יום', 'לימים', 'ימים', 'נוראים', 'הכיפורים', 'כיפור', 'כפור',
    'לפסח', 'פסח', 'לשבועות', 'שבועות', 'לסוכות', 'סוכות', 'לחנוכה',
    'חנוכה', 'לפורים', 'פורים', 'לתענית', 'תענית', 'לרגלים', 'רגלים',
    'קדושתא', 'קדושתאות', 'סילוק', 'סילוקים', 'מעריב', 'שחרית', 'מוסף',
    'מנחה', 'ערבית', 'נעילה', 'עמידה', 'קדושה', 'קדושת', 'שמע', 'קריאת',
    'שירת', 'תלמודית', 'תלמודיים', 'חזל', 'הנהנין', 'השבח', 'המצוות',
    'הגדה', 'הגדות', 'פרשנית', 'עיוניים', 'עיונית',
    # generic scriptural references (a bare book name on a fragment of
    # commentary/midrash does not identify a WORK; Bible cat claims are
    # guarded elsewhere)
    'תורה', 'התורה', 'חומש', 'מקרא', 'תנך', 'נביאים', 'כתובים',
    # misc connectors/qualifiers
    'עם', 'על', 'של', 'או', 'גם', 'and', 'עוד', 'חלק', 'חלקים', 'כרך',
    'נוסח', 'נוסחים', 'נוסחות', 'קדום', 'קדומה', 'מזרחי', 'ספרדי',
    'אשכנזי', 'איטלקי', 'רומני', 'צרפתי',
    # English NLI vocabulary
    'piyyut', 'piyyutim', 'poetry', 'poem', 'poems', 'prayer', 'prayers',
    'common', 'secular', 'liturgical', 'liturgy', 'midrash', 'midrashim',
    'bible', 'biblical', 'commentary', 'commentaries', 'halakha', 'halakhah',
    'halakhic', 'talmud', 'talmudic', 'fragment', 'fragments', 'document',
    'documents', 'documentary', 'letter', 'letters', 'list', 'lists',
    'literature', 'grammar', 'grammatical', 'lexicon', 'dictionary',
    'magic', 'magical', 'medical', 'medicine', 'philosophy', 'philosophical',
    'karaite', 'unidentified', 'miscellaneous', 'material', 'goitein',
    'texts', 'text', 'hebrew', 'arabic', 'aramaic', 'judeo',
}
# author-name particles ignored when matching claim tokens against titles
AUTHOR_STOP = {'בן', 'בר', 'בת', 'אבו', 'בני', 'רבנו', 'רבינו', 'רבי', 'רב',
               'הרב', 'מר', 'גאון', 'הגאון', 'מחבר', 'ידוע', 'מיוחס',
               'טקסט', 'בלבד'}
# very common words that alone cannot anchor a same-work match
BIBLE_BOOKS = {
    'בראשית', 'שמות', 'ויקרא', 'במדבר', 'דברים', 'יהושע', 'שופטים',
    'שמואל', 'מלכים', 'ישעיהו', 'ישעיה', 'ירמיהו', 'ירמיה', 'יחזקאל',
    'הושע', 'יואל', 'עמוס', 'עובדיה', 'יונה', 'מיכה', 'נחום', 'חבקוק',
    'צפניה', 'חגי', 'זכריה', 'מלאכי', 'תהלים', 'תהילים', 'משלי', 'איוב',
    'השירים', 'רות', 'איכה', 'קהלת', 'אסתר', 'דניאל', 'עזרא', 'נחמיה',
    'הימים', 'מגילות', 'מגילת', 'מגלת'}
WEAK_TOKENS = GENERIC_TOKENS | AUTHOR_STOP | BIBLE_BOOKS | {
    'ישראל', 'יהודה', 'יוסף', 'יעקב', 'משה', 'אברהם', 'יצחק', 'דוד',
    'שלמה', 'אליהו', 'לתלמוד', 'התלמוד', 'תלמוד',
    'בבלי', 'ירושלמי', 'למסכת', 'והזהיר', 'לשונות'}

# name-variant lexicon: token(s) seen on one side add the other side's
# tokens before matching. Seeded from the MAPV2-A gold name_equations.
VARIANT_TABLE = [
    ({'הדאיה', 'אלקאר', 'אלקארי'}, {'הוראת', 'הקורא', 'טעמי', 'המקרא'}),
    ({'דלאלה', 'אלחאירין'}, {'מורה', 'נבוכים'}),
    ({'אלענק', 'הענק'}, {'תנו', 'ציץ'}),
    ({'אלערוות'}, {'עריות', 'העריות', 'הישר'}),
    ({'אלאמאנאת'}, {'אמונות', 'ודעות'}),
    ({'אלמסתלחק'}, {'השגות', 'ההשלמה'}),
    ({'כליאת', 'אלכליאת'}, {'כללים'}),
    ({'אגרון', 'אלאגרון'}, {'אגרון'}),
    ({'מגלת', 'מגילת'}, {'מגלת', 'מגילת'}),
]

# known quoting relations: (manuscript-title predicate tokens, claim
# predicate tokens). Match = ANY ms token present AND ANY claim token
# present (normalized substring for multiword entries). Direction-agnostic:
# 'known_quoter' means the overlap is explained by a known dependence.
QUOTER_RULES = [
    # the Arukh quotes R. Hananel / geonic Talmud commentary verbatim
    ({'ערוך'}, {'חננאל', 'הגאונים', 'גאונים', 'פירוש לתלמוד'}),
    # Menorat ha-Maor (either one) is an anthology quoting everything
    ({'מנורת המאור'}, {'מדרש', 'מסכת', 'אבות', 'תשובות', 'תלמוד', 'רבה',
                       'תהלים', 'גאון', 'שאילתות', 'ספרי', 'ספרא',
                       'פסיקתא', 'תנחומא', 'הלכות'}),
    # ms IS a canonical text; claim is a digest/commentary that quotes it
    ({'תלמוד בבלי', 'תלמוד ירושלמי', 'משנה', 'תוספתא'},
     {'הלכות', 'תשובות', 'גאונים', 'גאון', 'שאילתות', 'פירוש', 'פסקי',
      'והזהיר', 'קיצור', 'מדרש'}),
    # ms is Bible; claim is its Targum / verse-anchored work
    ({'מקרא', 'נביאים', 'תורה', 'חומש'}, {'תרגום'}),
    # halakhic digests/codes quoting Talmud, minor tractates, responsa
    ({'הראש', 'פסקי', 'מישרים', 'הלכות גדולות', 'הלכות פסוקות', 'שאילתות',
      'אלפסי', 'אלפס', 'משנה תורה', 'טור', 'סמג', 'אבודרהם', 'כלבו',
      'ארחות חיים', 'אגור', 'שבלי הלקט', 'תניא', 'מרדכי'},
     {'מסכת', 'תלמוד', 'תשובות', 'גאונים', 'גאון', 'מדרש', 'שמחות', 'כלה',
      'דרך ארץ', 'סופרים'}),
    # philosophical works quoting the Guide (and vice versa)
    ({'דלאלה', 'אלחאירין', 'מורה נבוכים'}, {'חטר', 'שאלות'}),
    # Hayyuj is quoted by the Spanish exegetes ("אבו זכריא")
    ({'אלאפעאל', 'חיוג'}, {'בן בלעם', 'אבן בלעם', 'אבן גיקטילה'}),
    # a specific midrash vs a DIFFERENT midrash = shared floating material
    ({'רבה', 'פסיקתא', 'תנחומא', 'ילקוט', 'מכילתא', 'ספרי', 'ספרא',
      'פרקי דרבי אליעזר'},
     {'רבה', 'פסיקתא', 'תנחומא', 'ילקוט', 'מכילתא', 'ספרי', 'ספרא',
      'מדרש'}),
]


def norm_text(s):
    s = _NIKUD.sub('', s or '')
    # a geresh after a SINGLE word-initial Hebrew letter is an abbreviation
    # marker (ר׳חננאל = ר׳ חננאל) — split it so the name stays a token;
    # multi-letter abbreviations (לקו׳) and acronyms (וז"ל) stay merged
    s = re.sub(r"(?<![א-ת])([א-ת])[׳'](?=[א-ת])", r'\1 ', s)
    s = s.replace('"', '').replace("'", '').replace('״', '').replace('׳', '')
    s = _NONWORD.sub(' ', s).lower()
    return ' '.join(s.split())


def tokens(s):
    return set(norm_text(s).split())


_PREFIXES = ('ו', 'ה', 'ב', 'ל', 'מ', 'כ', 'ש')

# genre classes for the generic-title CONFLICT test: a fully-generic title
# still quarantines a claim when its genre contradicts the claim's genre
# (gold cards #23/#55/#83: 'דרשות על התורה' vs Kuzari, 'קובץ בפילוסופיה' vs
# קהלת רבה). Multi-genre titles conflict only if NO class is compatible.
GENRE_OF = {
    'דרשות': 'drash', 'דרשה': 'drash', 'דרוש': 'drash',
    'פילוסופיה': 'philo', 'בפילוסופיה': 'philo', 'קבלה': 'philo',
    'דקדוק': 'gram', 'בדקדוק': 'gram', 'מילון': 'gram', 'מלון': 'gram',
    'לקסיקון': 'gram', 'מסורה': 'gram', 'טעמים': 'gram',
    'פיוט': 'liturgy', 'פיוטים': 'liturgy', 'תפילה': 'liturgy',
    'תפילות': 'liturgy', 'ברכות': 'liturgy', 'סידור': 'liturgy',
    'סדור': 'liturgy', 'מחזור': 'liturgy', 'שיר': 'liturgy',
    'שירים': 'liturgy', 'שירה': 'liturgy', 'שירת': 'liturgy',
    'קינות': 'liturgy', 'סליחות': 'liturgy', 'הושענות': 'liturgy',
    'מדרש': 'midrash', 'מדרשי': 'midrash', 'מדרשים': 'midrash',
    'אגדה': 'midrash', 'אגדות': 'midrash',
    'הלכה': 'halakha', 'הלכות': 'halakha', 'פסקים': 'halakha',
    'תשובות': 'halakha', 'הלכתית': 'halakha',
    'מרשמים': 'magic', 'מאגיים': 'magic', 'קמיע': 'magic',
    'קמיעות': 'magic', 'רפואה': 'med', 'רפואות': 'med',
    'מכתב': 'doc', 'מכתבים': 'doc', 'תעודות': 'doc', 'מסמך': 'doc',
    'מסמכים': 'doc', 'תיעודי': 'doc', 'חשבונות': 'doc',
}
# claim-side genre hints (checked against the claim NAME tokens)
CLAIM_GENRE_OF = dict(GENRE_OF)
CLAIM_GENRE_OF.update({
    'רבה': 'midrash', 'פסיקתא': 'midrash', 'תנחומא': 'midrash',
    'מכילתא': 'midrash', 'ספרי': 'midrash', 'ספרא': 'midrash',
    'הכוזרי': 'philo', 'נבוכים': 'philo', 'אמונות': 'philo',
    'קדושתא': 'liturgy', 'קילוס': 'liturgy', 'הושענא': 'liturgy',
    'ברכת': 'liturgy', 'ברכה': 'liturgy', 'קדיש': 'liturgy',
    'יוצרות': 'liturgy', 'סילוקים': 'liturgy', 'סילוק': 'liturgy',
    'פירוש': 'exeg', 'פרוש': 'exeg', 'תרגום': 'exeg', 'תפסיר': 'exeg',
})
# which title-genres ACCEPT which claim-genres (absence = conflict)
GENRE_OK = {
    'drash': {'drash', 'midrash', 'halakha'},
    'philo': {'philo'},
    'gram': {'gram', 'exeg'},
    'liturgy': {'liturgy'},
    'midrash': {'midrash', 'drash'},
    'halakha': {'halakha', 'drash', 'midrash', 'exeg'},
    'magic': {'magic', 'med'},
    'med': {'med', 'magic'},
    'doc': {'doc'},
}


def genre_conflict(titles, claim_tokens):
    """True when every generic title has genre classes and NONE accepts the
    claim's genre. Unknown genres on either side -> no conflict (fail open)."""
    claim_g = {CLAIM_GENRE_OF[t] for t in claim_tokens if t in CLAIM_GENRE_OF}
    if not claim_g:
        return False
    t_classes = set()
    for t in titles:
        for tk in tokens(t):
            if tk in GENRE_OF:
                t_classes.add(GENRE_OF[tk])
    if not t_classes:
        return False
    return not any(claim_g & GENRE_OK.get(g, {g}) for g in t_classes)


def _is_generic_token(t):
    """Generic test with Hebrew prefix particles stripped (בדקדוק -> דקדוק,
    ופרשנות -> פרשנות). Up to two leading particles."""
    if t in GENERIC_TOKENS or len(t) <= 2 or t.isdigit():
        return True
    cur = t
    for _ in range(2):
        if cur[:1] in _PREFIXES and len(cur) > 3:
            cur = cur[1:]
            if cur in GENERIC_TOKENS:
                return True
        else:
            break
    return False


def is_generic_title(title):
    """True when the title carries NO specific-work signal."""
    tks = tokens(title)
    if not tks:
        return True
    return all(_is_generic_token(t) for t in tks)


def _expand_variants(tks):
    out = set(tks)
    for a, b in VARIANT_TABLE:
        if out & a:
            out |= b
        if out & b:
            out |= a
    return out


def _sig(tks):
    return {t for t in tks if t not in WEAK_TOKENS and len(t) >= 3}


def match_same(claim_tokens, title_tokens):
    """(matched, via_variant). Requires >=2 significant shared tokens, or
    one highly distinctive (len>=5) shared token."""
    a = _sig(claim_tokens)
    b = _sig(title_tokens)
    inter = a & b
    direct = len(inter) >= 2 or any(len(t) >= 5 for t in inter)
    if direct:
        return True, False
    a2 = _expand_variants(claim_tokens)
    b2 = _expand_variants(title_tokens)
    inter2 = _sig(a2) & _sig(b2)
    if len(inter2) >= 2 or any(len(t) >= 5 for t in inter2):
        return True, True
    return False, False


def match_quoter(claim_text, title_text):
    for ms_keys, cl_keys in QUOTER_RULES:
        if any(k in title_text for k in ms_keys) \
                and any(k in claim_text for k in cl_keys):
            # canonical-source rules require the ms to BE the canonical
            # text, not a commentary naming it (פירוש יפת למקרא is not מקרא)
            if ms_keys & {'מקרא', 'נביאים', 'תורה', 'חומש', 'תלמוד בבלי'} \
                    and ('פירוש' in title_text or 'פרוש' in title_text
                         or 'שרח' in title_text):
                continue
            return True
    return False


class TitleGate:
    """Bulk loader + classifier. fjms_titles: sys_id -> [(work, author)]."""

    def __init__(self, nli_titles, fjms_db=FJMS_DB):
        self.nli = nli_titles          # sys_id -> title string
        self.fjms = defaultdict(list)
        if fjms_db and os.path.exists(fjms_db):
            con = sqlite3.connect(
                'file:' + fjms_db.replace('\\', '/') + '?mode=ro', uri=True)
            for sid, w, au in con.execute(
                    "SELECT DISTINCT AlmaId, GenizahTitleOrgTitle, AuthorText"
                    " FROM catalog WHERE GenizahTitleOrgTitle IS NOT NULL"
                    " AND GenizahTitleOrgTitle != ''"):
                self.fjms[str(sid)].append((w, au or ''))
            con.close()

    def titles_of(self, sid):
        sid = str(sid)   # FJMS keys stored as str; robust for int callers
        out = []
        t = self.nli.get(sid)
        if t:
            out.append(t)
        out.extend(f"{w} {au}" for w, au in self.fjms.get(sid, ()))
        return out

    def classify(self, sid, claim_name):
        """-> (cls, evidence_title). claim_name = 'author — title' string."""
        titles = self.titles_of(sid)
        specific = [t for t in titles if not is_generic_title(t)]
        if not specific:
            if titles and genre_conflict(titles, tokens(claim_name)):
                return 'different_specific', 'genre-conflict: ' + titles[0]
            return 'generic_or_absent', None
        ctks = tokens(claim_name)
        ctext = norm_text(claim_name)
        best = None
        for t in specific:
            same, via_var = match_same(ctks, tokens(t))
            if same:
                return ('name_variant' if via_var else 'same_work'), t
        for t in specific:
            if match_quoter(ctext, norm_text(t)):
                best = best or ('known_quoter', t)
        return best or ('different_specific', specific[0])


def _validate():
    """Confusion matrix vs the 88 MAPV2-A gold annotations."""
    enr = json.load(open(os.path.join(
        PROBE, 'review', 'full_deck', 'mapv2_deck_cards_enriched.json'),
        encoding='utf-8'))
    merged = json.load(open(os.path.join(
        PROBE, 'results', 'deck_annotation', 'merged_annotations.json'),
        encoding='utf-8'))
    gold = {c['card_no']: c['annotation'].get('title_relation')
            for c in merged}
    nli = {c['sys_id']: c['nli_title'] for c in enr}
    tg = TitleGate(nli)
    conf = defaultdict(lambda: defaultdict(int))
    SIMPLIFY = {'same_work': 'known', 'name_variant': 'known'}
    bad = []
    for c in enr:
        pred, ev = tg.classify(c['sys_id'], c['work_name'])
        g = gold.get(c['card_no']) or '?'
        conf[g][pred] += 1
        gs = SIMPLIFY.get(g, g)
        ps = SIMPLIFY.get(pred, pred)
        if gs != ps:
            bad.append((c['card_no'], g, pred, c['work_name'][:40],
                        (c['nli_title'] or '-')[:40], (ev or '')[:40]))
    cats = ['generic_or_absent', 'same_work', 'name_variant',
            'known_quoter', 'different_specific']
    hdr = 'gold / pred'
    print(f"{hdr:20s}" + "".join(f"{c[:12]:>14s}" for c in cats))
    for g in cats:
        print(f"{g:20s}" + "".join(f"{conf[g][p]:>14d}" for p in cats))
    n_ok = sum(1 for c in enr) - len(bad)
    print(f"\nagreement (same/variant folded): {n_ok}/{len(enr)}")
    # the two dangerous directions
    d1 = sum(conf[g]['generic_or_absent'] for g in cats
             if g != 'generic_or_absent')
    d2 = conf['generic_or_absent']['different_specific'] \
        + conf['generic_or_absent']['known_quoter'] \
        + conf['generic_or_absent']['same_work'] \
        + conf['generic_or_absent']['name_variant']
    print(f"DANGEROUS specific->generic: {d1}   (lost) generic->specific: {d2}")
    for row in bad:
        print("  MISMATCH #%d gold=%s pred=%s | claim=%s | NLI=%s | ev=%s"
              % row)


if __name__ == '__main__':
    _validate()

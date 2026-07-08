# -*- coding: utf-8 -*-
"""FJMS bibliography/catalog enrichment + translation-aware title matching
for Track-1 identification review (Hillel feedback, 2026-07-07):

1. 'title mismatch is usually just translation' — JA works carry Hebrew
   display titles while catalogs use the Judeo-Arabic title (חובות הלבבות
   vs פראיץ אלקלוב), and authors appear as acronyms (רמב"ם / ריב"ג).
   -> title_bucket2: acronym equivalences from fjms genizah_persons
   (HebDescAc <-> HebDesc) + a hand table of classic JA<->Hebrew title
   pairs + FJMS catalog titles (incl. GenizahTitleOrgTitle normalized
   identifications) as additional catalog-side candidates.
2. 'some new? parallels are already discussed in research' — the FJMS
   bibliography table (427K rows/AlmaId) records publications per MS,
   incl. TranscriptionType Full/Partial (the fragment is ALREADY edited).
   -> load_bib + bib_signal: demote tier 'new?' to 'new?known' when the
   MS has a Full transcription or a bib entry naming the work.
"""
import re
import sqlite3

from rapidfuzz.distance import Levenshtein

FJMS_DB = r"C:\Genizahsearch\fist_data\fjms_enrichment.db"

NIQQUD = re.compile(r'[֑-ׇ]')
_GENERIC_NAME_TOKENS = {'גאון', 'הלוי', 'הכהן', 'רבי', 'ורבי', 'בן', 'אבן',
                        'ידוע', 'מחבר'}

# classic JA <-> Hebrew title pairs (normalized: no niqqud/quotes,
# Hebrew-letter tokens single-spaced). Containment either direction.
TRANSLATION_PAIRS = [
    ('חובות הלבבות', 'פראיץ אלקלוב'),
    ('חובות הלבבות', 'אלהדאיה אלי פראיץ אלקלוב'),
    ('אמונות ודעות', 'אלאמאנאת ואלאעתקאדאת'),
    ('ספר הרקמה', 'כתאב אללמע'),
    ('ספר השרשים', 'כתאב אלאצול'),
    ('המספיק לעובדי השם', 'כפאיה אלעאבדין'),
    ('מורה נבוכים', 'דלאלה אלחאירין'),
    ('מורה הנבוכים', 'דלאלת אלחאירין'),
    ('העיונים והדיונים', 'כתאב אלמחאצרה ואלמדאכרה'),
    ('ספר המאזניים', 'כתאב אלמואזנה'),
    ('ספר המאזנים', 'מואזנת'),
    ('מבוא התלמוד', 'אלמדכל אלי אלתלמוד'),
    ('משפטי שבועות', 'כתאב אלאימאן'),
    ('פירוש המשנה', 'שרח אלמשנה'),
    ('ספר המצוות', 'כתאב אלשראיע'),
    ('ספר המצוות', 'כתאב אלפראיץ'),
    ('תפסיר', 'תרגום ערבי'),
]


def norm_title(s):
    s = NIQQUD.sub('', s or '')
    s = re.sub(r'["\'׳״]', '', s)
    return ' '.join(re.findall(r'[א-ת]+', s))


def heb_tokens(s, min_len=3):
    return [t for t in norm_title(s).split() if len(t) >= min_len]


def tok_eq(a, b):
    if a == b:
        return True
    for x in (a, a[1:] if a[:1] in 'והבלכמש' and len(a) > 3 else a):
        for y in (b, b[1:] if b[:1] in 'והבלכמש' and len(b) > 3 else b):
            if x == y:
                return True
    return (min(len(a), len(b)) >= 4
            and Levenshtein.normalized_distance(a, b) <= 0.25)


def load_acronym_equiv(con=None):
    """token -> set(tokens): author acronym <-> full-name content tokens."""
    own = con is None
    if own:
        con = sqlite3.connect(FJMS_DB)
    equiv = {}
    for acr, full in con.execute(
            "SELECT HebDescAc, HebDesc FROM genizah_persons "
            "WHERE HebDescAc IS NOT NULL AND HebDescAc != ''"):
        a = norm_title(acr).replace(' ', '')
        if len(a) < 3:
            continue
        toks = {t for t in heb_tokens(full, 4)
                if t not in _GENERIC_NAME_TOKENS}
        if not toks:
            continue
        equiv.setdefault(a, set()).update(toks)
        for t in toks:
            equiv.setdefault(t, set()).add(a)
    if own:
        con.close()
    return equiv


def _tokens_match(cat_toks, work_toks, equiv):
    for a in cat_toks:
        ea = {a} | equiv.get(a, set())
        for b in work_toks:
            eb = {b} | equiv.get(b, set())
            if any(tok_eq(x, y) for x in ea for y in eb):
                return True
    return False


def _phrase_match(cat_norm, work_norm):
    for h, j in TRANSLATION_PAIRS:
        if (h in work_norm and j in cat_norm) or \
                (j in work_norm and h in cat_norm) or \
                (h in cat_norm and j in work_norm):
            return True
    return False


GENERIC_TOKENS = {
    'פיוט', 'פיוטים', 'פיוטי', 'תפילה', 'תפלה', 'תפילות', 'תפלות',
    'ברכות', 'ברכה', 'קטע', 'קטעים', 'קטעי', 'גניזה', 'שונות', 'סליחות',
    'סליחה', 'פזמון', 'פזמונים', 'שיר', 'שירים', 'שירה', 'תחינות',
    'זמירות', 'דף', 'דפים', 'תעודות', 'תעודה', 'מסמכים', 'רשימות',
    'רשימה', 'כתבי', 'שרידים', 'שריד', 'עלים', 'עלה', 'לקוטים',
    'ליקוטים', 'לקט', 'ערבית', 'יהודית', 'עברית', 'חבור', 'חיבור',
    'ספרות', 'יפה', 'קבע', 'טקסט',
}
STOP = {'קטע', 'קטעים', 'קטעי', 'גניזה', 'חלק', 'ספר', 'כתב', 'דפים',
        'עם', 'מנהג', 'סדר'}


def title_bucket2(cat_titles, work_author, work_title, equiv):
    """'match' / 'generic' / 'mismatch' over ALL catalog-side titles."""
    work_norm = norm_title(f"{work_author} {work_title}")
    work_toks = [t for t in work_norm.split()
                 if len(t) >= 3 and t not in STOP]
    any_content = False
    for ct in cat_titles:
        cat_norm = norm_title(ct)
        toks = [t for t in cat_norm.split() if len(t) >= 3 and t not in STOP]
        if not toks:
            continue
        any_content = any_content or not all(t in GENERIC_TOKENS
                                             for t in toks)
        if _phrase_match(cat_norm, work_norm) or \
                _tokens_match(toks, work_toks, equiv):
            return 'match'
    return 'mismatch' if any_content else 'generic'


class FjmsInfo:
    """Batched FJMS lookups for a set of sys_ids (AlmaId == sys_id)."""

    def __init__(self, sys_ids, db_path=FJMS_DB):
        self.con = sqlite3.connect(db_path)
        self.titles = {}       # sys -> [distinct FJMS title strings]
        self.bib = {}          # sys -> [(pub, article, author, mtype, ttype, page)]
        ids = list(set(sys_ids))
        for i in range(0, len(ids), 500):
            batch = ids[i:i + 500]
            ph = ','.join('?' * len(batch))
            for sid, t, th, at, gt in self.con.execute(
                    f"SELECT AlmaId, Title, TitleHeb, AuthorText, "
                    f"GenizahTitleOrgTitle FROM catalog "
                    f"WHERE AlmaId IN ({ph})", batch):
                cur = self.titles.setdefault(str(sid), [])
                for v in (t, th, at, gt):
                    if v and v not in cur:
                        cur.append(v)
            for sid, *row in self.con.execute(
                    f"SELECT AlmaId, RunningTitleHeb, RunningTitle, "
                    f"ArticleName, ArticleAuthorHeb, ArticleAuthorEng, "
                    f"MentionType, TranscriptionType, MentionPage "
                    f"FROM bibliography WHERE AlmaId IN ({ph})", batch):
                pub = row[0] or row[1] or ''
                self.bib.setdefault(str(sid), []).append(
                    (pub, row[2] or '', row[3] or row[4] or '',
                     row[5] or '', row[6] or '', row[7] or ''))

    def bib_signal(self, sys_id, work_author, work_title, equiv):
        """('transcribed'|'discussed'|'', best matching entry string)."""
        entries = self.bib.get(sys_id, [])
        work_toks = [t for t in heb_tokens(f"{work_author} {work_title}")
                     if t not in STOP]
        work_norm = norm_title(f"{work_author} {work_title}")
        best = ''
        for pub, art, auth, mtype, ttype, page in entries:
            text = f"{pub} {art}"
            toks = [t for t in heb_tokens(text) if t not in STOP]
            named = _phrase_match(norm_title(text), work_norm) or \
                _tokens_match(toks, work_toks, equiv)
            if named:
                label = f"{pub or art} ({ttype or mtype})"
                if ttype in ('Full', 'Partial'):
                    return 'transcribed', label
                best = best or label
        if best:
            return 'discussed', best
        if any(e[4] == 'Full' for e in entries):
            pub = next(e[0] or e[1] for e in entries if e[4] == 'Full')
            return 'transcribed', f"{pub} (Full)"
        return '', ''

    def close(self):
        self.con.close()

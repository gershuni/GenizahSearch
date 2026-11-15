# -*- coding: utf-8 -*-
"""Normalization of Judeo-Arabic forms for the Blau dictionary sidecar and its lookups.

One function decides when two spellings of a Judeo-Arabic word are the same word:
``normalize_ja``. ``scripts/build_blau_sidecar.py`` keys the sidecar's ``forms_norm`` table
by it, and a corpus token goes through the same function at lookup time, so a dictionary
form matches a token exactly when the two normalize alike. Stdlib only: no Config, no
logger, no genizah_core (``tests/test_no_back_edges_core.py``).

What ``normalize_ja`` drops, and why:

- every combining mark (Unicode category M): Hebrew nikud and cantillation; the
  Judeo-Arabic upper dot U+0307 (ג̇ = ج, כ̇ = خ, ד̇ = ذ, צ̇ = ض, ט̇ = ظ) and diaeresis U+0308
  (ה̈ = ة); Arabic harakat and shadda; and stray marks the source carries (a Gujarati nukta
  on six forms). This is a superset of the ranges the site's "strip diacritics" option
  removes (``SITE_DIACRITIC_RE`` mirrors that regex; ``tests/test_blau_normalize.py``
  proves the superset on every mark the site strips).
- the apostrophe/geresh family (``QUOTE_MARKS``): the HTR corpus marks the same letters
  with an ASCII apostrophe (ג' for ج) far more often than with the dot (164,480 against
  9,256 in a 600,000-line sample of ``Transcriptions.txt``, 2026-11-03), and the search
  path already folds the same characters (``shared.text_normalize.strip_search_diacritics``).
- final letters, folded to the medial form (``FINAL_FOLD``): the dictionary writes forms
  with finals, HTR output does not always, and ``shared.passage_normalize`` folds them too.
- brackets (``BRACKETS``), as transparent: the dictionary writes an optional letter as
  ``חיט(ה̈)`` and the corpus writes reading brackets inside words (``]הנתשנ``), and both
  must key as the plain word.
- everything else that is not a Hebrew letter: punctuation, hyphens, digits, Latin and
  Arabic letters are separators. A form's key is its Hebrew-letter words joined by one
  space, so ``חיט(ה̈)`` keys as ``חיטה`` and ``לא אבד`` as ``לא אבד``.

``normalize_arabic`` is the Arabic-script counterpart for ``arabic_spelling`` (phase 3 of
``docs/plans/BLAU_DICTIONARY_PLAN.md``): NFKC (which folds the presentation forms the site
maps by hand), marks dropped, tatweel dropped, hamza-carrying alefs folded to bare alef.

``candidate_stems`` is the phase-2 heuristic for a clicked corpus word: the word itself,
then with one proclitic layer (ו, פ; ב, ל, כ; אל) and one pronoun suffix peeled. It is not
a lemmatizer; the plan names what it misses (broken plurals, verb inflection).

``NORMALIZER_VERSION`` is an artifact-identity input: it is written to the sidecar's
``meta`` table and ``scripts/verify_blau_sidecar.py`` recomputes every ``forms_norm`` key
with the code that is running, so a sidecar built by other rules fails the gate rather
than silently missing matches. Bump it for ANY change to what this module drops or folds.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from functools import lru_cache

NORMALIZER_VERSION = 1
ANALYSIS_VERSION = 2
SPELLING_DDL = ("CREATE TABLE IF NOT EXISTS spelling_keys (folded TEXT NOT NULL, literal TEXT NOT NULL, "
                "PRIMARY KEY (folded, literal)) WITHOUT ROWID;")


@dataclass(frozen=True)
class WordAnalysis:
    """A search alternative, separate from the reader's literal spelling."""
    key: str
    steps: tuple[str, ...] = ()
    nominal: bool = False


def spelling_key(key: str) -> str:
    """Symmetric single/double yod and waw equivalence; other consonants stay intact."""
    return re.sub(r"י{2,}|ו{2,}", lambda m: m[0][0], key)


def build_spelling_index(conn, keys) -> None:
    """Keep literal vocabulary beside indexed spelling equivalents in a candidate sidecar."""
    conn.execute(SPELLING_DDL)
    conn.executemany("INSERT OR IGNORE INTO spelling_keys VALUES (?, ?)",
                     ((spelling_key(key), key) for key in keys if key))


def spelling_candidates(conn, candidates: tuple[str, ...], *, matres: bool = False) -> tuple[str, ...]:
    """All indexed equivalents, even when another literal candidate already matches."""
    out = list(candidates)
    for key in candidates:
        for (literal,) in conn.execute("SELECT literal FROM spelling_keys WHERE folded = ? ORDER BY literal",
                                       (spelling_key(key),)):
            if literal not in out:
                out.append(literal)
    if matres:
        alternatives = tuple(dict.fromkeys(spelling_key(variant) for key in candidates
                                           for variant in mater_variants(key)))
        if alternatives:
            marks = ','.join('?' * len(alternatives))
            for (literal,) in conn.execute(
                    f'SELECT DISTINCT literal FROM spelling_keys WHERE folded IN ({marks}) ORDER BY literal',
                    alternatives):
                if literal not in out:
                    out.append(literal)
    return tuple(out)


def mater_variants(key: str) -> tuple[str, ...]:
    """One internal yod/waw added or removed; dictionary vocabulary validates each spelling.

    Keep the word's boundaries and all other consonants. Short forms and phrases do not
    support this expansion; doubling is handled separately by ``spelling_key``.
    """
    if not 4 <= len(key) <= 24 or ' ' in key:
        return ()
    out = [key[:i] + key[i + 1:] for i in range(1, len(key) - 1)
           if key[i] in 'וי' and len(key) > 4]
    out += [key[:i] + letter + key[i:] for i in range(1, len(key)) for letter in 'וי'
            if key[i - 1] != letter and key[i] != letter]
    return tuple(dict.fromkeys(out))

# The regex the site's search page applies under "strip diacritics" (js/search.js,
# stripDiacritics, release 1.1): Hebrew points and accents, Arabic harakat and Quranic
# marks, and the combining diacriticals block. Kept verbatim for the parity test; the
# normalizer itself drops every category-M character, which covers all of these.
SITE_DIACRITIC_RE = re.compile(
    "[֑-ֽֿ-ׇً-ٰٟۖ-ۜ۟-ۤ"
    "ۧ-۪ۨ-ۭ̀-ͯ]"
)

# Apostrophe-like marks the corpus writes inside words: ASCII apostrophe and double
# quote (the typed gershayim), Hebrew geresh and gershayim, the curly single quotes.
QUOTE_MARKS = frozenset("'\"׳״‘’")
# Brackets are transparent, not separators: the dictionary writes an optional letter as חיט(ה̈)
# and the corpus writes lacuna/reading brackets inside words (]הנתשנ, the search path's
# _add_bracket_variants). Both spellings must key as the plain word.
BRACKETS = frozenset("()[]")

FINAL_FOLD = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})

_HEB_WORD_RE = re.compile("[א-ת]+")

# Arabic alef with hamza above/below, madda, wasla -> bare alef.
_ALEF_FOLD = str.maketrans({"أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا"})
_TATWEEL = "ـ"
_ARABIC_WORD_RE = re.compile("[ء-يٮ-ۓ]+")


def strip_site_diacritics(text: str) -> str:
    """The site's own "strip diacritics": exactly its regex, nothing more."""
    return SITE_DIACRITIC_RE.sub("", text or "")


def strip_marks(text: str) -> str:
    """NFC, then every combining mark (category Mn, Mc, Me) removed."""
    if not text:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFC", text) if unicodedata.category(ch)[0] != "M")


def fold_finals(text: str) -> str:
    return (text or "").translate(FINAL_FOLD)


def ja_words(text: str) -> list[str]:
    """The Hebrew-letter words of ``text`` after marks, quote marks and finals are folded."""
    if not text:
        return []
    # Category M first, then the site's own ranges (which add three punctuation marks, paseq,
    # sof pasuq and nun hafukha, that would otherwise split a word), then the quote family and
    # the brackets (an optional letter, חיט(ה̈), or a reading mark, ]הנתשנ, never a word break).
    cleaned = SITE_DIACRITIC_RE.sub("", strip_marks(text))
    cleaned = "".join(ch for ch in cleaned if ch not in QUOTE_MARKS and ch not in BRACKETS)
    return _HEB_WORD_RE.findall(fold_finals(cleaned))


def normalize_ja(text: str) -> str:
    """The lookup key of a Judeo-Arabic form or of a corpus token: see the module docstring.

    Idempotent; returns ``""`` for text with no Hebrew letter.
    """
    return " ".join(ja_words(text))


def normalize_arabic(text: str) -> str:
    """The lookup key of an Arabic-script spelling: NFKC, marks and tatweel dropped,
    hamza-carrying alefs folded, Arabic-letter words joined by one space."""
    if not text:
        return ""
    folded = unicodedata.normalize("NFKC", text)
    folded = "".join(ch for ch in folded if unicodedata.category(ch)[0] != "M" and ch != _TATWEEL)
    return " ".join(_ARABIC_WORD_RE.findall(folded.translate(_ALEF_FOLD)))


_ARABIC_TO_JA = str.maketrans({
    "ا": "א", "ب": "ב", "ت": "ת", "ث": "ת", "ج": "ג", "ح": "ח", "خ": "כ", "د": "ד", "ذ": "ד",
    "ر": "ר", "ز": "ז", "س": "ס", "ش": "ש", "ص": "צ", "ض": "צ", "ط": "ט", "ظ": "ט", "ع": "ע",
    "غ": "ג", "ف": "פ", "ق": "ק", "ك": "כ", "ل": "ל", "م": "מ", "ن": "נ", "ه": "ה", "و": "ו",
    "ي": "י", "ى": "י", "ة": "ה", "ء": "א", "ؤ": "ו", "ئ": "י", "ـ": "",
})


def lexical_key(text: str) -> str:
    """Whole-word Arabic/Hebrew correspondence, including mixed-script expressions.

    This supplies candidates, not a claim about a meaning; dictionary vocabulary validates
    them. Literal Arabic spellings and dotted original headings remain available for display.
    """
    text = unicodedata.normalize("NFKC", text or "").translate(_ALEF_FOLD)
    return normalize_ja(text.translate(_ARABIC_TO_JA))


# Phase-2 heuristic (docs/plans/BLAU_DICTIONARY_PLAN.md, §6.3). Order matters: the
# candidates are tried first to last, so the unpeeled word always comes first. The suffixes
# are spelled with medial letters because they are peeled AFTER normalize_ja folded the finals.
CONJUNCTIONS = ("ו", "פ")
PREPOSITIONS = ("ב", "ל", "כ")
ARTICLE = "אל"
PRONOUN_SUFFIXES = ("הא", "המ", "הנ", "כמ", "כנ", "נא", "ני", "ה", "כ", "י")
MIN_STEM_LETTERS = 2


def _peel_prefixes(word: str) -> list[str]:
    out = [word]
    rest = word
    if rest[:1] in CONJUNCTIONS and len(rest) - 1 >= MIN_STEM_LETTERS:
        rest = rest[1:]
        out.append(rest)
    if rest[:1] in PREPOSITIONS and len(rest) - 1 >= MIN_STEM_LETTERS:
        rest = rest[1:]
        out.append(rest)
    if rest.startswith(ARTICLE) and len(rest) - len(ARTICLE) >= MIN_STEM_LETTERS:
        rest = rest[len(ARTICLE):]
        out.append(rest)
    return out


def _peel_suffix(word: str) -> list[str]:
    out = []
    for suffix in PRONOUN_SUFFIXES:
        if word.endswith(suffix) and len(word) - len(suffix) >= MIN_STEM_LETTERS:
            out.append(word[: -len(suffix)])
    return out


def article_candidates(word: str) -> frozenset[str]:
    """The candidates of ``candidate_stems`` reached only by peeling the article אל (and a pronoun
    suffix after it), and the ``elided_article_stems``: the keys of a noun. A word with the article is
    no finite verb (אלקאצ̇י read as stem III of קצו, a verb's senses above the
    noun's), so these keys may be a participle or a verbal noun, never a perfect or an imperfect. The
    word's own key is never one: אלתזם is stem VIII of לזם."""
    base = normalize_ja(word)
    if not base or " " in base:
        return frozenset()
    elided = frozenset(elided_article_stems(base))
    stems = _peel_prefixes(base)
    if len(stems) < 2 or stems[-2] != ARTICLE + stems[-1]:
        return elided
    others = {cand for stem in stems[:-1] for cand in [stem, *_peel_suffix(stem)]}
    return elided | frozenset(cand for cand in [stems[-1], *_peel_suffix(stems[-1])] if cand not in others)


def preposition_candidates(word: str) -> frozenset[str]:
    """The keys of ``word`` left under a preposition (ב, ל or כ, after ו or פ), with a pronoun suffix
    peeled or not: לשגל -> שגל. What follows a preposition is a noun, so an entry such a key names is
    read as the noun before the verb. Keys of three letters or more; never the word's own key."""
    base = normalize_ja(word)
    if not base or " " in base:
        return frozenset()
    found: set[str] = set()
    for stem in (base, base[1:] if base[0] in CONJUNCTIONS else ""):
        if len(stem) > 3 and stem[0] in PREPOSITIONS:
            found.update((stem[1:], *_peel_suffix(stem[1:])))
    return frozenset(found) - {base}


def bare_keys(key: str) -> frozenset[str]:
    """The word's own key and the key under a conjunction (ונצף -> נצף): what the word spells. A
    conjunction leaves the reading as it was, so an entry of that name is the word's own."""
    return frozenset((key, key[1:])) if len(key) > 3 and key[0] in CONJUNCTIONS else frozenset((key,))


#: A stem reached through an article that lost its alif after a preposition has at least this many
#: letters: בלאד (a country) is no ב + (א)ל + אד.
MIN_ELIDED_STEM = 3


def elided_article_stems(word: str) -> list[str]:
    """The stems of a word whose article lost its alif after a preposition
    (בלסלאם, bi-l-salām, reached nothing): ב, ל or כ (after ו or פ), then ל, then the stem --
    בלסלאם -> סלאם, ללנאס -> נאס, ובלסלאמה -> סלאמה, סלאמ. Each of ``MIN_ELIDED_STEM`` letters or more;
    [] for a word with the article written whole (באלסלאם: ``candidate_stems``) or with no preposition.
    Included in ``word_analyses`` for both source and query, alongside intact-word matches."""
    rest = normalize_ja(word)
    if not rest or " " in rest:
        return []
    if rest[:1] in CONJUNCTIONS and len(rest) - 1 >= MIN_STEM_LETTERS:
        rest = rest[1:]
    if rest[:1] not in PREPOSITIONS or rest[1:2] != ARTICLE[1]:   # באלסלאם: the article written whole
        return []
    stem = rest[2:]
    out: list[str] = []
    for cand in [stem, *_peel_suffix(stem)]:
        if len(cand) >= MIN_ELIDED_STEM and cand not in out:
            out.append(cand)
    return out


def candidate_stems(word: str) -> list[str]:
    """Normalized lookup candidates for one corpus word, the word itself first, then with
    one proclitic layer peeled (conjunction, then preposition, then article), then each
    of those with one pronoun suffix peeled. No duplicates; every candidate has at least
    ``MIN_STEM_LETTERS`` letters."""
    return _stems(word, feminine=False)


#: A feminine noun ends in ה, spelled ת before a pronoun suffix: סלאמה, סלאמתך.
FEMININE_ENDING, FEMININE_BEFORE_SUFFIX = "ה", "ת"


def lookup_stems(word: str) -> list[str]:
    """Shared bounded word analyses, also used for source and query expression keys."""
    return [analysis.key for analysis in word_analyses(word)]


@lru_cache(maxsize=32768)
def word_analyses(word: str) -> tuple[WordAnalysis, ...]:
    """Bounded clitic, vocative and feminine analyses, retaining the intact word first.

    Vocabulary lookup validates alternatives; no list of address words is needed. Nominal
    paths must not license finite verbs. Literal and expanded paths remain independent.
    """
    base = normalize_ja(word)
    if not base or " " in base:
        return (WordAnalysis(base),) if base else ()
    out: dict[str, WordAnalysis] = {}

    def add(key: str, steps: tuple[str, ...], nominal: bool = False) -> None:
        if len(key) >= MIN_STEM_LETTERS and key not in out:
            out[key] = WordAnalysis(key, steps, nominal)

    for stem in _peel_prefixes(base):
        prefix = () if stem == base else ("prefix",)
        nominal = base.endswith("אל" + stem)
        for cand in [stem, *_peel_suffix(stem)]:
            steps = prefix + (("suffix",) if cand != stem else ())
            add(cand, steps, nominal)
            if cand.endswith("ת") and len(cand) > MIN_STEM_LETTERS:
                add(cand[:-1] + "ה", (*steps, "feminine"), True)
    for stem in elided_article_stems(base):
        add(stem, ("contracted_article",), True)
        if stem.endswith("ת"):
            add(stem[:-1] + "ה", ("contracted_article", "feminine"), True)
    vocative = base[1:] if base[:1] in CONJUNCTIONS else base
    if vocative.startswith("יא") and len(vocative) >= 4:
        for analysis in word_analyses(vocative[2:]):
            add(analysis.key, ("vocative", *analysis.steps), True)
    # A documented preposition spelling, also used under pronominal suffixes.
    for cand in list(out.values()):
        if cand.key in ("עלא", "עלי"):
            add("עלי" if cand.key == "עלא" else "עלא", (*cand.steps, "spelling"))
    return tuple(out.values())


def nominal_candidates(word: str) -> frozenset[str]:
    return article_candidates(word) | frozenset(a.key for a in query_analyses(word) if a.nominal)


@lru_cache(maxsize=32768)
def query_analyses(word: str) -> tuple[WordAnalysis, ...]:
    """Additional reader-side spellings, without changing the stored dictionary keys."""
    analyses = list(word_analyses(word))
    base = normalize_ja(word)
    bare = base[1:] if base[:1] in CONJUNCTIONS else base
    # A shortened vocative needs an address noun, not just any word beginning in yod.
    address = {'מולאי', 'מולא', 'סידי', 'שיכי', 'שיכ', 'אכי', 'רבי'}
    if bare.startswith('י') and bare[1:] in address:
        analyses.extend(WordAnalysis(a.key, ('vocative', *a.steps), True)
                        for a in word_analyses(bare[1:]))
    alias = {'דאלכ': 'דלכ', 'האדא': 'הדא', 'האדה': 'הדה'}.get(bare)
    if alias:
        analyses.append(WordAnalysis(alias, ('spelling_alias',)))
    known = {a.key for a in analyses}

    def add(key: str, steps: tuple[str, ...], nominal: bool) -> None:
        if len(key) >= MIN_STEM_LETTERS and key not in known:
            known.add(key)
            analyses.append(WordAnalysis(key, steps, nominal))

    # The pronouns as later texts spell them out (בתאעתהו, טהורהי, יסלטנוהו): peeled like the
    # classical ones, the reader's side only (the dictionary's own keys stay as they are).
    if ' ' not in base:
        for stem in _peel_prefixes(base):
            prefix = () if stem == base else ('prefix',)
            nominal = base.endswith(ARTICLE + stem)
            for suffix in LATE_PRONOUN_SUFFIXES:
                if stem.endswith(suffix) and len(stem) - len(suffix) >= MIN_LATE_STEM:
                    cand = stem[:-len(suffix)]
                    add(cand, (*prefix, 'suffix'), nominal)
                    if cand.endswith(FEMININE_BEFORE_SUFFIX) and len(cand) > MIN_LATE_STEM:
                        add(cand[:-1] + FEMININE_ENDING, (*prefix, 'suffix', 'feminine'), True)
    # The nouns that take a long vowel before a pronoun (אכ̇יה, אבוך, חמאהא: אך̇, אב, חם).
    if ' ' not in base:
        for stem in _peel_prefixes(base):
            if stem[:2] in LONG_VOWEL_NOUNS and stem[2:3] in ('ו', 'י', 'א') and stem[3:] in PRONOUN_SUFFIXES:
                add(stem[:2], (*(() if stem == base else ('prefix',)), 'suffix'), True)
    # A doubled waw or yod writes one consonant (מתצוור, יטייב): the single spelling too, for the
    # forms a verb's stem is generated in (the dictionary's own names meet through ``spelling_key``).
    for analysis in list(analyses):
        add(spelling_key(analysis.key), (*analysis.steps, 'spelling'), analysis.nominal)
    return tuple(analyses)


def query_stems(word: str) -> tuple[str, ...]:
    return tuple(dict.fromkeys(a.key for a in query_analyses(word)))


#: The keys of the nouns whose case vowel is written long before a pronoun: father, brother,
#: father-in-law.
LONG_VOWEL_NOUNS = ("אב", "אכ", "חמ")
#: The pronoun suffixes as later Judeo-Arabic spells them out, longest first; what is left of a word
#: they are peeled from has at least ``MIN_LATE_STEM`` letters.
LATE_PRONOUN_SUFFIXES = ("הומ", "הונ", "כומ", "הו", "הי", "כי")
MIN_LATE_STEM = 3
#: The last step of a ``plural_analyses`` key: the sound masculine plural (the first kind there was),
#: the sound feminine plural, the dual, the alif of the indefinite accusative, a broken plural.
PLURAL, FEMININE_PLURAL, DUAL, ACCUSATIVE, BROKEN_PLURAL = 'plural', 'feminine_plural', 'dual', 'accusative', 'broken_plural'
_FEMININE_HEADING = re.compile('ה[\u064b-\u0652]*\u0308[\u064b-\u0652]*$')


def feminine_heading(text: str) -> bool:
    """Does a printed heading's first word end in the feminine ending (ה̈)? Such a noun takes no
    accusative alif: מנהא is "from her", not מנה̈ with one."""
    words = str(text or '').split()
    return bool(words) and bool(_FEMININE_HEADING.search(words[0].rstrip(':,;.')))


def broken_plural_singulars(key: str) -> tuple[str, ...]:
    """The singulars a key may be the broken plural of, by the plural's pattern alone (the letters
    it adds to the consonants); the dictionary's noun headings validate each. For C1 C2 C3 (C4):
    אC1C2אC3 (אבראג: ברג; אפמאמ: פמ), C1C2אC3 (תמאר: תמרה), C1C2וC3 (תכות: תכת; כטוט: כט), C1C2יC3
    (עביד: עבד), C1ואC2C3 (קואדס: קאדוס), C1C2איC3 (כצאיל: כצלה), C1C2אC3C4 (דראדר: דרדור) and
    C1C2אC3יC4 (מחאזיר: מחזור; מראזיב: מרזבה). () for a phrase or any other shape."""
    k = key
    if ' ' in k or not 4 <= len(k) <= 6:
        return ()
    out: list[str] = []

    def three(a: str, b: str, c: str, *more: str) -> None:
        out.extend([a + b + c, a + b + c + 'ה', *more])
        if b == c:
            out.append(a + b)          # a doubled root is named with two letters

    def four(s: str) -> None:
        out.extend([s, s + 'ה', s[:3] + 'ו' + s[3], s[:3] + 'א' + s[3], s[:3] + 'י' + s[3],
                    s[:3] + 'י' + s[3] + 'ה', s[:3] + 'א' + s[3] + 'ה'])

    if len(k) == 5 and k[0] == 'א' and k[3] == 'א':
        a, b, c = k[1], k[2], k[4]
        three(a, b, c, a + b + 'י' + c, a + 'א' + b + c)
    if len(k) == 4 and k[2] == 'א':
        a, b, c = k[0], k[1], k[3]
        three(a, b, c, a + b + 'י' + c, a + b + 'י' + c + 'ה')
    if len(k) == 4 and k[2] == 'ו':
        a, b, c = k[0], k[1], k[3]
        three(a, b, c, a + 'א' + b + c)
    if len(k) == 4 and k[2] == 'י':
        out.append(k[0] + k[1] + k[3])
    if len(k) == 5 and k[1:3] == 'וא':
        a, b, c = k[0], k[3], k[4]
        out.extend([a + 'א' + b + c, a + 'א' + b + c + 'ה', a + 'א' + b + 'ו' + c])
    if len(k) == 5 and k[2:4] == 'אי':
        a, b, c = k[0], k[1], k[4]
        out.extend([a + b + 'י' + c + 'ה', a + b + c + 'ה', a + b + 'א' + c + 'ה', a + b + 'ו' + c + 'ה',
                    a + b + 'י' + c, a + b + 'ו' + c])
    if len(k) == 5 and k[2] == 'א':
        four(k[:2] + k[3:])
    if len(k) == 6 and k[2] == 'א' and k[4] == 'י':
        four(k[:2] + k[3] + k[5])
    return tuple(dict.fromkeys(s for s in out if s != k and len(s) >= MIN_STEM_LETTERS))


def plural_analyses(word: str) -> tuple[WordAnalysis, ...]:
    """The reader's side only: the nouns a word may be an inflected form of -- the sound plurals
    (-ינ, -ונ; -את of a noun with or without ה), the dual (-אנ; -תאנ, -תינ of a noun in ה), the alif
    of the indefinite accusative, and a broken plural's singulars (``broken_plural_singulars``).
    Each key's last step names which; the readers validate every key against a noun's heading."""
    found: dict[str, WordAnalysis] = {}

    def add(key: str, steps: tuple[str, ...], kind: str) -> None:
        if len(key) >= MIN_STEM_LETTERS and key not in found:
            found[key] = WordAnalysis(key, (*steps, kind), True)

    for analysis in query_analyses(word):
        key, steps = analysis.key, analysis.steps
        if ' ' in key:
            continue
        if key.endswith(('ינ', 'ונ')) and len(key) >= 5:
            add(key[:-2], steps, PLURAL)
        if key.endswith('את') and len(key) >= 5:
            add(key[:-2] + FEMININE_ENDING, steps, FEMININE_PLURAL)
            add(key[:-2], steps, FEMININE_PLURAL)
        if key.endswith(('תאנ', 'תינ')) and len(key) >= 6:
            add(key[:-3] + FEMININE_ENDING, steps, DUAL)
        if key.endswith('אנ') and len(key) >= 5:
            add(key[:-2], steps, DUAL)
        if key.endswith('א') and len(key) >= 4:
            add(key[:-1], steps, ACCUSATIVE)
        for singular in broken_plural_singulars(key):
            add(singular, steps, BROKEN_PLURAL)
    return tuple(found.values())


def _stems(word: str, feminine: bool) -> list[str]:
    base = normalize_ja(word)
    if not base or " " in base:
        return [base] if base else []
    seen: list[str] = []
    for stem in _peel_prefixes(base):
        for cand in [stem, *_peel_suffix(stem)]:
            forms = [cand]
            if (feminine and cand != stem and cand.endswith(FEMININE_BEFORE_SUFFIX)
                    and len(cand) > MIN_STEM_LETTERS):
                forms.append(cand[:-1] + FEMININE_ENDING)
            for form in forms:
                if form not in seen:
                    seen.append(form)
    return seen

def analysis_paths(word: str, matched: str) -> tuple[tuple[str, ...], ...]:
    """Retain query transformations and source-side equivalents for match details."""
    key = lexical_key(word)
    paths = []
    # A noun form (``plural_analyses``, beyond the sound masculine plural) explains a match only
    # when no plainer analysis does: the order of what a word reached as written stays as it was.
    nouns = plural_analyses(key)
    plain = (*query_analyses(key), *(a for a in nouns if a.steps[-1] == PLURAL))
    reached = {spelling_key(a.key) for a in plain} | {spelling_key(v) for a in plain for v in mater_variants(a.key)}
    others = () if spelling_key(matched) in reached else tuple(a for a in nouns if a.steps[-1] != PLURAL)
    for analysis in (*plain, *others):
        if analysis.key == matched or spelling_key(analysis.key) == spelling_key(matched):
            steps = analysis.steps
            if analysis.key != matched:
                steps += ("spelling",)
            if any("ء" <= ch <= "ي" for ch in word):
                steps = ("script", *steps)
            paths.append(steps)
        elif spelling_key(matched) in {spelling_key(v) for v in mater_variants(analysis.key)}:
            paths.append((*analysis.steps, 'mater'))
    return tuple(dict.fromkeys(paths)) or (("source_alias",),)


def matching_paths(word: str, literal: str, matched: str) -> tuple[tuple[str, ...], ...]:
    """Both sides of a lexical match, including the source form's transformations."""
    query = analysis_paths(word, matched)
    source = analysis_paths(literal, matched)
    return tuple(dict.fromkeys(q + tuple('source_' + step for step in src if step != 'script')
                               for q in query for src in source))


def evidence_cost(paths: tuple[tuple[str, ...], ...]) -> float:
    """Less direct source forms are weaker evidence than ordinary query clitics."""
    weights = {'prefix': .25, 'suffix': .75, 'feminine': .35, 'vocative': .35,
               'article': .25, 'spelling': .1, 'spelling_alias': .15, 'mater': 2.25, 'script': 0,
               FEMININE_PLURAL: .6, DUAL: 1.0, ACCUSATIVE: 1.0, BROKEN_PLURAL: 2.5,
               # On the dictionary's side a noun form weighs what an unexplained form does, as before.
               'source_' + FEMININE_PLURAL: 1.8, 'source_' + DUAL: 1.8, 'source_' + ACCUSATIVE: 1.8,
               'source_' + BROKEN_PLURAL: 1.8,
               'source_prefix': .6, 'source_suffix': 1.5, 'source_feminine': .5,
               'source_spelling': .1, 'source_script': 0, 'source_form': 1.5,
               'source_alias': 1.8, 'source_source_alias': 1.8}
    return min((sum(weights.get(step, .4) for step in path) for path in paths), default=0)


def grammatical_placeholder(literal: str, head: str) -> bool:
    """A verb's argument label is searchable in its frame, not as that verb's lemma."""
    key = lexical_key(literal)
    first = lexical_key(head).split()[:1]
    return key in {'פלאנ', 'פלאנא', 'פעל'} and first != [key]

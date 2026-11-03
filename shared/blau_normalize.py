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

NORMALIZER_VERSION = 1

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


def candidate_stems(word: str) -> list[str]:
    """Normalized lookup candidates for one corpus word, the word itself first, then with
    one proclitic layer peeled (conjunction, then preposition, then article), then each
    of those with one pronoun suffix peeled. No duplicates; every candidate has at least
    ``MIN_STEM_LETTERS`` letters."""
    base = normalize_ja(word)
    if not base or " " in base:
        return [base] if base else []
    seen: list[str] = []
    for stem in _peel_prefixes(base):
        for cand in [stem, *_peel_suffix(stem)]:
            if cand not in seen:
                seen.append(cand)
    return seen

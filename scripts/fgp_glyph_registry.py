# -*- coding: utf-8 -*-
"""Glyph-name registry for re-reading FGP's PDFs (GS-276; plan ``docs/plans/FGP_PDF_GLYPH_REREAD_PLAN.md`` step 1).

FGP's PDF fonts name their glyphs in ``/Differences`` with names the standard decoders do not know: ``nunfinal``,
``vavholam``, ``tavdages`` (sic), ``alefisolated``, ``lamwithalefisolated``, ``hyphenminus``. This module turns a
name into

* ``form``: the Unicode character(s) the glyph draws, as a font would map them (``vavholam`` -> U+FB4B,
  ``alefisolated`` -> U+FE8D), used to check the name against the glyph's shape (step 3), and
* ``text``: what the transcription stores (decisions 1-2, approved 2026-10-27): Hebrew presentation forms as the
  letter plus its points in canonical order (NFD), Arabic presentation forms as plain Arabic letters (NFKC).

Every name is resolved by an explicit table or a stated rule, never by similarity; a name it cannot resolve returns
``None`` and the glyph is decoded only if its shape alone certifies it (step 3). Changing a table or rule changes
``REGISTRY_VERSION``.
"""
from __future__ import annotations

import functools
import re
import unicodedata

REGISTRY_VERSION = "2026-10-27.1"

HEBREW_LETTERS = {
    "alef": "א", "bet": "ב", "gimel": "ג", "dalet": "ד", "he": "ה", "vav": "ו",
    "zayin": "ז", "het": "ח", "tet": "ט", "yod": "י", "kaffinal": "ך", "kaf": "כ",
    "lamed": "ל", "memfinal": "ם", "mem": "מ", "nunfinal": "ן", "nun": "נ",
    "samekh": "ס", "ayin": "ע", "pefinal": "ף", "pe": "פ", "tsadifinal": "ץ",
    "tsadi": "צ", "qof": "ק", "resh": "ר", "shin": "ש", "tav": "ת",
    # spellings met in FGP's fonts
    "finalkaf": "ך", "finalmem": "ם", "finalnun": "ן", "finalpe": "ף", "finaltsadi": "ץ",
}
HEBREW_MARKS = {  # longest first when parsing
    "dageshandsindot": "ּׂ", "dageshandshindot": "ּׁ", "dageshshindot": "ּׁ",
    "dageshsindot": "ּׂ", "hatafpatah": "ֲ", "hatafsegol": "ֱ", "hatafqamats": "ֳ",
    "dagesh": "ּ", "dages": "ּ", "mapiq": "ּ", "rafe": "ֿ", "holam": "ֹ",
    "patah": "ַ", "qamats": "ָ", "sheva": "ְ", "hiriq": "ִ", "segol": "ֶ",
    "tsere": "ֵ", "qubuts": "ֻ", "qibuts": "ֻ", "shindot": "ׁ", "sindot": "ׂ",
    "meteg": "ֽ",
}
TABLE = {
    # Hebrew marks and punctuation standing alone
    "qibuts": "ֻ", "qubuts": "ֻ", "meteg": "ֽ", "shindot": "ׁ", "sindot": "ׂ",
    "maqaf": "־", "paseq": "׀", "sofpasuq": "׃", "geresh": "׳", "gershayim": "״",
    # Yiddish ligatures (no decomposition in Unicode)
    "vavyod": "ױ", "doublevav": "װ", "doubleyod": "ײ", "yoddbl": "ײ",
    # Arabic single names
    "lam": "ل", "alefmaksura": "ى", "arabicquestionmark": "؟", "arabickaffinal": "ﻚ",
    # Latin
    "hyphenminus": "-", "combiningdotbelow": "̣",
}
ARABIC_BASES = {
    "alefmaksura": "ALEF MAKSURA", "tehmarbuta": "TEH MARBUTA", "alef": "ALEF", "beh": "BEH", "teh": "TEH",
    "theh": "THEH", "jeem": "JEEM", "hah": "HAH", "khah": "KHAH", "dal": "DAL", "thal": "THAL", "reh": "REH",
    "zain": "ZAIN", "seen": "SEEN", "sheen": "SHEEN", "sad": "SAD", "dad": "DAD", "tah": "TAH", "zah": "ZAH",
    "ain": "AIN", "ghain": "GHAIN", "feh": "FEH", "qaf": "QAF", "kaf": "KAF", "lam": "LAM", "meem": "MEEM",
    "noon": "NOON", "heh": "HEH", "waw": "WAW", "yeh": "YEH", "hamza": "HAMZA",
}
ARABIC_MODIFIERS = {"withhamzaabove": "WITH HAMZA ABOVE", "withhamzabelow": "WITH HAMZA BELOW",
                    "withmaddaabove": "WITH MADDA ABOVE"}
ARABIC_MARKS = {"shadda": "SHADDA", "fatha": "FATHA", "kasra": "KASRA", "damma": "DAMMA", "sukun": "SUKUN",
                "fathatan": "FATHATAN", "kasratan": "KASRATAN", "dammatan": "DAMMATAN"}
POSITIONS = ("isolated", "final", "medial", "initial")
NEVER = frozenset({".notdef", "space", "nbspace"})


def _hebrew(name: str) -> str | None:
    """``[final]<letter>[with]<mark>[[and]<mark>...]`` -> letter + marks (the presentation form if one exists)."""
    rest, letter = name, None
    for key in sorted(HEBREW_LETTERS, key=len, reverse=True):
        if rest.startswith(key):
            letter, rest = HEBREW_LETTERS[key], rest[len(key):]
            break
    if letter is None:
        return None
    marks = ""
    while rest:
        rest = rest[4:] if rest.startswith("with") else rest[3:] if rest.startswith("and") else rest
        for key in sorted(HEBREW_MARKS, key=len, reverse=True):
            if rest.startswith(key):
                marks, rest = marks + HEBREW_MARKS[key], rest[len(key):]
                break
        else:
            return None
    seq = unicodedata.normalize("NFC", letter + marks)
    return seq


def _arabic_lookup(text: str) -> str | None:
    try:
        return unicodedata.lookup(text)
    except KeyError:
        return None


def _split_bases(s: str) -> list[str] | None:
    """'lamwithalefmaddaabove' -> ['LAM', 'WITH', 'ALEF WITH MADDA ABOVE'] style tokens, or None."""
    out = []
    while s:
        for key in sorted(ARABIC_MODIFIERS, key=len, reverse=True):
            if s.startswith(key):
                out.append(ARABIC_MODIFIERS[key])
                s = s[len(key):]
                break
        else:
            if s.startswith("with"):
                out.append("WITH")
                s = s[4:]
                continue
            for key in sorted(ARABIC_BASES, key=len, reverse=True):
                if s.startswith(key):
                    out.append(ARABIC_BASES[key])
                    s = s[len(key):]
                    break
            else:
                return None
    return out


def _arabic(name: str) -> str | None:
    """``<letter or ligature><position>`` -> the Arabic presentation form, by its Unicode character name."""
    m = re.fullmatch(r"(.+?)(isolated|final|medial|initial)d?", name)
    if not m:
        return None
    body, pos = m.group(1), m.group(2).upper()
    if body == "allah":
        return _arabic_lookup("ARABIC LIGATURE ALLAH ISOLATED FORM") if pos == "ISOLATED" else None
    if body in ARABIC_MARKS:
        return _arabic_lookup(f"ARABIC {ARABIC_MARKS[body]} {pos} FORM")
    tokens = _split_bases(body)
    if not tokens:
        return None
    if "WITH" in tokens:  # ligature of two letters
        i = tokens.index("WITH")
        a, b = " ".join(tokens[:i]), " ".join(tokens[i + 1:])
        return (_arabic_lookup(f"ARABIC LIGATURE {a} WITH {b} {pos} FORM")
                or _arabic_lookup(f"ARABIC LETTER {a} WITH {b} {pos} FORM"))
    return _arabic_lookup(f"ARABIC LETTER {' '.join(tokens)} {pos} FORM")


@functools.lru_cache(maxsize=None)
def form_of(name: str | None) -> str | None:
    """The character(s) the named glyph draws, or None when no table or rule resolves the name."""
    if not name or name in NEVER:
        return None
    if name in TABLE:
        return TABLE[name]
    if name in HEBREW_LETTERS:
        return HEBREW_LETTERS[name]
    seq = _hebrew(name)
    if seq:
        return seq
    seq = _arabic(name)
    if seq:
        return seq
    from fontTools import agl
    u = agl.toUnicode(name)
    return u or None


def text_of(form: str | None) -> str | None:
    """What the transcription stores for a glyph drawing ``form`` (approved decisions 1 and 2)."""
    if not form:
        return form
    out = []
    for ch in form:
        cp = ord(ch)
        if 0xFB50 <= cp <= 0xFDFF or 0xFE70 <= cp <= 0xFEFF:   # Arabic presentation forms -> letters
            # an isolated vowel-sign form (U+FE70..FE7F) decomposes to SPACE + mark; keep the mark only
            out.append(unicodedata.normalize("NFKC", ch).lstrip(" ") if 0xFE70 <= cp <= 0xFE7F
                       else unicodedata.normalize("NFKC", ch))
        elif 0xFB1D <= cp <= 0xFB4F:                         # Hebrew presentation forms -> letter + points
            out.append(unicodedata.normalize("NFD", ch))
        else:
            out.append(ch)
    return unicodedata.normalize("NFD", "".join(out)) if any(0x0591 <= ord(c) <= 0x05C7 for c in "".join(out)) \
        else "".join(out)


def text_of_name(name: str | None) -> str | None:
    return text_of(form_of(name))

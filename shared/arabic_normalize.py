# -*- coding: utf-8 -*-
"""Judeo-Arabic words read as Arabic, and the keys the Arabic dictionary sidecar is built on.

docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md (§3, §4) is the reasoning; this module is the
one copy of it. ``scripts/build_arabic_sidecar.py`` keys the sidecar's tables by these
functions, ``scripts/verify_arabic_sidecar.py`` recomputes every key with them, and
``scripts/measure_arabic_readings.py`` measured them against the PGP editions. Stdlib only:
no Config, no logger, no genizah_core (``tests/test_no_back_edges_core.py``).

- ``readings``: every Arabic skeleton a Judeo-Arabic word may be. An undotted ג ד ט צ ת כ
  is read both ways (דלך is دلك and ذلك); a marked one (U+0307, or an apostrophe or geresh
  after it, as ``shared/blau_service.py::dot_pattern`` reads the mark) one way, except ג,
  whose mark means ج in Blau and غ in many editions; a final א is also alef maqsura; a
  final ו is also read with the silent alif of the plural (חצ'רו = حضروا).
- ``skeleton``: the key of an Arabic word for the frequency table -- what Judeo-Arabic
  spelling does not distinguish is folded away (harakat, the hamza and its seats, ة/ه,
  ى/ي).
- ``root_key``: the key of a root, so Lane's, Lisān's and a peeled word's spellings meet
  (hamza seats as ``ء``, weak letters as ``W``, a two-letter root doubled).

``NORMALIZER_VERSION`` is an artifact-identity input: it is written to the sidecar's
``meta`` and the gate recomputes every key with the running code, so a sidecar built by
other rules is refused rather than silently missing matches. Bump it for ANY change to
what these functions return.
"""
from __future__ import annotations

import re
import unicodedata
from itertools import product
from typing import Iterator

from shared.blau_normalize import QUOTE_MARKS, fold_finals

NORMALIZER_VERSION = 1

#: Each Hebrew letter (finals folded) -> the Arabic letters it may stand for, the plain
#: reading first. Measured on Blau's own one-word spellings (plan §3): ג 151 غ / 3 ج and
#: ג̇ 404 ج; ד 647 د / 2 ذ and ד̇ 125 ذ; and so on.
LETTERS = {
    "א": ("ا",), "ב": ("ب",), "ג": ("غ", "ج"), "ד": ("د", "ذ"), "ה": ("ه",), "ו": ("و",), "ז": ("ز",),
    "ח": ("ح",), "ט": ("ط", "ظ"), "י": ("ي",), "כ": ("ك", "خ"), "ל": ("ل",), "מ": ("م",), "נ": ("ن",),
    "ס": ("س",), "ע": ("ع",), "פ": ("ف",), "צ": ("ص", "ض"), "ק": ("ق",), "ר": ("ر",), "ש": ("ش",),
    "ת": ("ت", "ث"),
}
#: The letters a dot above turns into a second Arabic letter, and that letter.
DOTTED = {"ג": "ج", "ד": "ذ", "ט": "ظ", "צ": "ض", "ת": "ث", "כ": "خ"}
#: What a MARKED letter may be read as. Blau marks ج (ג̇, 404 of 404), but the PGP
#: editions mark غ as often (בג'יר, ג'ריב): a mark on ג says only "not plain".
DOTTED_READINGS = {letter: (arabic,) for letter, arabic in DOTTED.items()} | {"ג": ("ج", "غ")}
#: A final alef also writes alef maqsura (מוסא = موسى); the skeleton folds ى to ي.
FINAL_ALEF = ("ا", "ي")
MAX_READINGS = 64
DOT_ABOVE = "̇"


def ja_letters(text: str) -> list[tuple[str, bool]]:
    """(letter, dotted) for each Hebrew letter of one word: finals folded, the dot read
    from U+0307 or a quote mark right after a letter of ``DOTTED``; every other mark
    dropped."""
    out: list[tuple[str, bool]] = []
    for ch in unicodedata.normalize("NFC", text or ""):
        if "א" <= ch <= "ת":
            out.append((fold_finals(ch), False))
        elif (ch == DOT_ABOVE or ch in QUOTE_MARKS) and out and out[-1][0] in DOTTED:
            out[-1] = (out[-1][0], True)
    return out


def is_abbreviation(text: str) -> bool:
    """Gershayim, or a quote mark after a letter that takes no dot (ב׳כ׳ר׳, ר')."""
    if "״" in text or '"' in text or "׳׳" in text or "''" in text:
        return True
    prev = ""
    for ch in unicodedata.normalize("NFC", text):
        if ch in QUOTE_MARKS and prev and fold_finals(prev) not in DOTTED:
            return True
        if "א" <= ch <= "ת":
            prev = ch
        elif ch not in QUOTE_MARKS and ch != DOT_ABOVE:
            prev = ""
    return False


def readings(word: str, plural_alif: bool = False) -> list[str]:
    """Every Arabic skeleton a Judeo-Arabic word may be, plain letters first, the last
    letter varying fastest; at most ``MAX_READINGS``.

    ``plural_alif`` (the plan's method v2 and later): Judeo-Arabic mostly omits the
    silent alif after the plural -ū, so a word ending in ו is also read with it."""
    base = _readings(word)
    letters = ja_letters(word)
    if plural_alif and base and len(letters) > 2 and letters[-1][0] == "ו":
        base = base + [r + "ا" for r in base][: MAX_READINGS - len(base)]
    return base


def _readings(word: str) -> list[str]:
    letters = ja_letters(word)
    if not letters:
        return []
    options: list[tuple[str, ...]] = []
    for i, (letter, dotted) in enumerate(letters):
        if dotted:
            options.append(DOTTED_READINGS[letter])
        elif letter == "א" and i == len(letters) - 1 and len(letters) > 1:
            options.append(FINAL_ALEF)
        else:
            options.append(LETTERS.get(letter, ("",)))
    out: list[str] = []
    for combo in product(*options):
        out.append("".join(combo))
        if len(out) >= MAX_READINGS:
            break
    return out


_SKELETON_FOLD = str.maketrans({"أ": "ا", "إ": "ا", "آ": "ا", "ٱ": "ا", "ة": "ه", "ى": "ي", "ؤ": "و",
                                "ئ": "ي", "ء": None, "ـ": None})
_ARABIC_TOKEN_RE = re.compile("[ء-يٱ]+")
_MARKS_RE = re.compile("[ؐ-ًؚ-ٰٟۖ-ۭ]")


def strip_harakat(text: str) -> str:
    """NFC, then the Arabic vowel signs, shadda, sukun, tanwin and Quranic marks removed."""
    return _MARKS_RE.sub("", unicodedata.normalize("NFC", text or ""))


def skeleton(word: str) -> str:
    """The frequency key of an Arabic word: harakat and tatweel dropped, the hamza seats
    and alef forms folded to their carrier, ة to ه, ى to ي, a bare hamza dropped -- the
    distinctions Judeo-Arabic spelling does not make."""
    return strip_harakat(word).translate(_SKELETON_FOLD)


def arabic_tokens(text: str) -> Iterator[str]:
    """Surface Arabic words of a text, harakat dropped (hamza kept)."""
    yield from _ARABIC_TOKEN_RE.findall(strip_harakat(text))


_HAMZA = set("ءأإآؤئ")
_WEAK = set("اويى#")


def root_key(root: str) -> str:
    """A root key, so the dictionaries' spellings meet: Arabic letters only, every hamza
    seat as ``ء``, every weak letter (ا و ي ى, or CAMeL's ``#``) as ``W`` -- Lane writes
    قول, Lisān sometimes قال -- a root never beginning with a long vowel (Lane writes أخذ as
    "Ax*"), and a two-letter root doubled (Lane writes شبب as شب)."""
    letters: list[str] = []
    for ch in root:
        if ch in _HAMZA or (ch == "ا" and not letters):
            letters.append("ء")
        elif ch in _WEAK:
            letters.append("W")
        elif "ء" <= ch <= "ي" and ch not in "ةـ":
            letters.append(ch)
    if len(letters) == 2:
        letters.append(letters[-1])
    return "".join(letters)


def strong_letters(key: str) -> str:
    """The root key's letters that are neither hamza nor weak, doubles collapsed."""
    out: list[str] = []
    for ch in key:
        if ch not in "ءW" and (not out or out[-1] != ch):
            out.append(ch)
    return "".join(out)


#: Buckwalter as the Perseus Lane files write it (``A^`` is an alef with hamza).
_BW = {"'": "ء", "|": "آ", ">": "أ", "&": "ؤ", "<": "إ", "}": "ئ", "A": "ا", "b": "ب", "p": "ة", "t": "ت",
       "v": "ث", "j": "ج", "H": "ح", "x": "خ", "d": "د", "*": "ذ", "r": "ر", "z": "ز", "s": "س", "$": "ش",
       "S": "ص", "D": "ض", "T": "ط", "Z": "ظ", "E": "ع", "g": "غ", "f": "ف", "q": "ق", "k": "ك", "l": "ل",
       "m": "م", "n": "ن", "h": "ه", "w": "و", "Y": "ى", "y": "ي", "{": "ٱ"}


def buckwalter_to_arabic(text: str) -> str:
    """The consonants of a Perseus-Buckwalter string in Arabic script (vowels dropped)."""
    out: list[str] = []
    i = 0
    while i < len(text):
        if text[i] == "A" and text[i + 1:i + 2] == "^":
            out.append("أ")
            i += 2
            continue
        if text[i] in _BW:
            out.append(_BW[text[i]])
        i += 1
    return "".join(out)

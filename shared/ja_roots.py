# -*- coding: utf-8 -*-
"""A typed root, its dots and its verb stems: what Blau's and Friedman's lookups share for looking
a root up and choosing one of its stems.

The private dictionary lookup plan (section 4.6, owner 2026-11-11): the reader types a root, and
possibly a stem ("דלל X", "דלל 10"), and the dictionary's card for that root shows the stems the
dictionary lists as buttons. Nothing here guesses: a stem number is read from the dictionary's
own label, and a typed stem is a stem only when it is written as one.

- **Stem labels.** Both dictionaries label a stem paragraph with a Roman numeral. Friedman's
  labels as recovered from the PDFs are noisy ("IV/II", "II(<IV)", "IV)>I)", "("), and Blau's
  stem rows carry a ``binyan`` column, a ``binyan_shift`` ("IV > I") and the printed head, which
  disagree on 355 rows. ``stem_numbers`` reads a label as every stem number it names, I to X;
  each service takes the union over the places a stem's number is written (plan section 4.1).
- **A typed stem.** ``root_and_stem`` reads "<root> <stem>" (or the stem first, as a right-to-left
  box may be typed): the stem one word, a Roman numeral I to X in either case or a number 1 to 10;
  the root the other words, with a Hebrew letter. Anything else is not a stem lookup.
- **The dot above.** Judeo-Arabic writes ג̇ ד̇ ט̇ צ̇ ת̇ כ̇ for a second Arabic letter; the
  lookup key drops the dot, so דלל and ד̇לל share a key. ``dot_pattern`` keeps it, however it is
  written (U+0307, or an apostrophe or geresh after the letter, as the automatic text and any
  keyboard write it), so a lookup can put the dotted reading first (``dots_agree``). Moved here
  from ``shared/blau_service.py``, which still exports the names, so that Friedman's lookup uses
  the same rule.

``StemPair`` is one (root, stem) pair a dictionary lists, with the unit that holds its paragraph:
the list each service builds (``BlauService.stem_pairs``, ``FriedmanService.stem_pairs``) and the
later generated verb forms reuse (plan section 4.2).

``BorrowedRoot`` is a root another dictionary gives for a looked-up word, for Blau's tab to show
Blau's entry of that root after its own (owner, 2026-11-11: Blau has no list of inflected forms, so
קאל reached no entry of its own; Friedman finds קול through its index and Lane through its
reading). ``borrowed_roots`` reads them off Friedman's and Lane's answers: every root Friedman's
matches name, in its order, then Lane's first root, written in Hebrew letters as Blau spells a
root (``ja_root_of_arabic``).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from shared.arabic_normalize import DOTTED, LETTERS
from shared.blau_normalize import QUOTE_MARKS, fold_finals, normalize_ja
from shared.ja_expression import lookup_key

#: The letters Judeo-Arabic writes with a dot above for a second Arabic letter (finals folded):
#: ג̇ ד̇ ט̇ צ̇ ת̇ כ̇.
DOT_LETTERS = frozenset("גדטצתכ")
DOT_ABOVE = "̇"

#: The stems both dictionaries number, I to X, in order (index + 1 is the number).
STEM_NUMERALS = ("I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X")
_NUMERAL_VALUE = {numeral: n for n, numeral in enumerate(STEM_NUMERALS, 1)}
#: One numeral inside a label, longest first where two could start at the same letter.
_NUMERAL_RE = re.compile(r"X|IX|IV|V?I{1,3}|V")
_HEBREW_LETTER_RE = re.compile(r"[א-ת]")


def dot_pattern(text: str) -> str:
    """The Hebrew letters of ``text`` (finals folded, other marks dropped, words split by one
    space) with ``*`` after each letter of ``DOT_LETTERS`` that carries the dot above: U+0307 as
    the dictionaries write it, or an apostrophe or geresh after the letter as the automatic text
    and a keyboard write it. ``ד'לך`` and ``ד̇לך`` are both ``ד*לכ``; ``ר'`` is ``ר``."""
    out: list[str] = []
    for ch in unicodedata.normalize("NFC", text or ""):
        if "א" <= ch <= "ת":
            out.append(fold_finals(ch))
        elif ch == DOT_ABOVE or ch in QUOTE_MARKS:
            if out and out[-1] in DOT_LETTERS:
                out.append("*")
        elif ch.isspace() and out and out[-1] != " ":
            out.append(" ")
    return "".join(out).strip()


def dots_agree(word_pattern: str, spelling: str) -> bool:
    """The dictionary spelling's first word, dots included, sits inside the looked-up word's
    ``dot_pattern`` (whose proclitics the candidate may have peeled)."""
    first = dot_pattern(spelling).split(" ", 1)[0]
    return bool(first) and first in word_pattern


def stem_numbers(label: object) -> tuple[int, ...]:
    """Every stem number, 1 to 10, that a stem label names, in order and without repeats:
    "IV/II" -> (2, 4), "II(<IV)" -> (2, 4), "IV > I" -> (1, 4), "(" -> ()."""
    found = {_NUMERAL_VALUE[m] for m in _NUMERAL_RE.findall(str(label or "")) if m in _NUMERAL_VALUE}
    return tuple(sorted(found))


def stem_label(number: int) -> str:
    """The Roman numeral of a stem number (1 -> "I"); '' outside 1 to 10."""
    return STEM_NUMERALS[number - 1] if 1 <= int(number) <= len(STEM_NUMERALS) else ""


def parse_stem(token: str) -> int:
    """A typed stem, or 0: one Roman numeral I to X (either case) or a number 1 to 10."""
    token = (token or "").strip()
    if token.isascii() and token.isdigit():
        number = int(token)
        return number if 1 <= number <= len(STEM_NUMERALS) else 0
    return _NUMERAL_VALUE.get(token.upper(), 0) if token.isascii() else 0


def root_and_stem(text: str) -> tuple[str, int]:
    """("<root>", stem) when ``text`` is a root with a typed stem after it (or before it): "דלל X",
    "ד'לל 10", "X דלל". ("", 0) for anything else: one word, a stem alone, a root without a Hebrew
    letter, two words neither of which is a stem."""
    words = (text or "").split()
    if len(words) < 2:
        return "", 0
    for stem_at, rest in ((-1, words[:-1]), (0, words[1:])):
        stem = parse_stem(words[stem_at])
        root = " ".join(rest)
        if stem and _HEBREW_LETTER_RE.search(root) and not any(parse_stem(w) for w in rest):
            return root, stem
    return "", 0


def stem_query(root: str, stem: int = 0) -> str:
    """What the box holds to open ``root`` with ``stem`` chosen ("דלל X"), or the root alone."""
    label = stem_label(stem) if stem else ""
    return f"{root} {label}" if label else root


@dataclass(frozen=True)
class StemPair:
    """One (root, stem) pair a dictionary lists, and the unit holding that stem's paragraph.
    Blau: ``unit_id`` is the ``stems`` row and ``entry_id`` the root (entry) it is under.
    Friedman: ``unit_id`` is the ``binyan`` unit, or the entry itself when the stem is the entry's
    own, and ``entry_id`` the entry."""
    root: str
    root_key: str
    stem: int
    unit_id: str
    entry_id: str


#: The dictionaries a tab borrows a root from, and what each borrowed root says it came from: Blau's
#: tab borrows from Friedman and Lane, Friedman's from Blau and Lane (owner, 2026-11-11).
BORROW_FRIEDMAN = "friedman"
BORROW_LANE = "lane"
BORROW_BLAU = "blau"

#: An Arabic letter in Hebrew letters as Blau spells a root: the plain letter, or the dotted one for
#: the second letter a dot marks (ج ג̇, غ ג, ذ ד̇, ظ ט̇, ض צ̇, ث ת̇, خ כ̇: Blau marks ج, 404 of 404);
#: every hamza seat א, alef maqsura י, ta marbuta ה.
_JA_OF_ARABIC = {arabic: letter for letter, options in LETTERS.items() for arabic in options}
_JA_OF_ARABIC.update({arabic: letter + DOT_ABOVE for letter, arabic in DOTTED.items()})
_JA_OF_ARABIC.update({"أ": "א", "إ": "א", "آ": "א", "ٱ": "א", "ء": "א", "ؤ": "א", "ئ": "א", "ى": "י", "ة": "ה"})


_FINAL_FORMS = {"כ": "ך", "מ": "ם", "נ": "ן", "פ": "ף", "צ": "ץ"}


def with_final_letters(key: str) -> str:
    """A lookup key (finals folded) as it is written: each word's last letter in its final form,
    "סלאמ" -> "סלאם"."""
    return " ".join(word[:-1] + _FINAL_FORMS.get(word[-1], word[-1]) if word else word for word in key.split(" "))


def ja_root_of_arabic(name: str, root_key: str = "") -> str:
    """An Arabic root (Lane's name for it, قول) in Hebrew letters as Blau spells a root: קול, رأى
    ראי, جيأ ג̇יא. A two-letter name whose ``root_key`` is a doubled root (اب under ءبب) gets its
    last letter again, as Blau writes a doubled root (סבב); not a weak one (ذا under ذWW, the
    ``W`` Lane's key writes for و or ي). Letters with no Hebrew counterpart (marks, tatweel) are
    dropped; '' when none is left."""
    letters = [_JA_OF_ARABIC[ch] for ch in unicodedata.normalize("NFC", name or "") if ch in _JA_OF_ARABIC]
    key = root_key or ""
    if len(letters) == 2 and len(key) == 3 and key[1] == key[2] and key[2] != "W":
        letters.append(letters[-1])
    if letters:   # the last letter in its final form, as a root is written (סלם, כ̇צץ̇)
        letters[-1] = _FINAL_FORMS.get(letters[-1][0], letters[-1][0]) + letters[-1][1:]
    return "".join(letters)


#: Where a Friedman head's first spelling ends: the book separates spellings with a slash or a comma
#: (אَצْל/אُצْל/וَצْל, אَך̇, אُכْ̇ת).
_SPELLING_BREAK_RE = re.compile(r"[/,]")


def head_begins_with(head_text: Any, key: str) -> bool:
    """Does a Friedman head's first spelling begin with ``key`` (as many words as the key has)? The
    verb וצל is headed by it; the noun אَצْל/אُצْל/וَצْל lists it as a later spelling, and the entry
    "חוז עِלْמאً" holds עלמא as the verb's complement. (Moved here from ``shared/friedman_service.py``,
    which still exports it, so that ``borrowed_roots`` uses the same rule.)"""
    first = _SPELLING_BREAK_RE.split(str(head_text or ""), 1)[0]
    words = first.split()[:len(key.split())]
    return bool(words) and lookup_key(" ".join(words)) == key


@dataclass(frozen=True)
class BorrowedRoot:
    """A root another dictionary gives for a looked-up word: ``root`` in Hebrew letters (dots
    kept, as that dictionary prints it or as ``ja_root_of_arabic`` writes Lane's), ``source`` the
    dictionary (``BORROW_FRIEDMAN``, ``BORROW_LANE``) and ``evidence`` what the reader is shown
    after the source: Friedman's index form "קאל (קול)", the word and its entry's root "יגי (ג̇יא)",
    Lane's Arabic root "قول"."""
    root: str
    source: str
    evidence: str


def borrowed_roots(friedman: Any = None, lane: Any = None, blau: Any = None) -> tuple[BorrowedRoot, ...]:
    """The roots the other dictionaries' answers give for one word: every entry Blau's OWN cards
    show (not those it borrowed: no root comes back to the dictionary that gave it), by its first
    printed name; the roots Friedman's matches name, in Friedman's order: its index of forms, and an
    entry whose head begins with the word or its base form; then Lane's first root (the root of its
    first reading). Not a Friedman sub-entry, stem paragraph or entry that holds the word further in
    its head (owner, 2026-11-11): such a head is mostly a phrase, the word a verb's complement, so its
    root is the verb's (אלעלמא reached דרס through "דרס עِלْמאً", חצל and "חוז עِלْמאً").
    Blau's tab asks for Friedman's and Lane's, Friedman's for Blau's and Lane's. Each root once per
    dictionary (by its letters and dots); () for an expression, a typed stem or a dictionary not
    served (None). Never a card or match found by verb form (``shared.ja_verb_forms``, PR 3): both
    dictionaries run the same rules, so it is no second dictionary's word for the root, and each
    shows its own. Reads the answers' fields only: no service is imported here."""
    out: list[BorrowedRoot] = []
    if blau is not None and not getattr(blau, "stem", 0):
        seen_blau: set[str] = set()
        for card in getattr(blau, "cards", ()) or ():
            if getattr(card, "borrowed", ()) or getattr(card, "verb_form", ""):
                continue
            name = str(getattr(card, "name", "") or "")
            root = str(getattr(card, "root_label", "") or "").strip() or name
            if not normalize_ja(root) or dot_pattern(root) in seen_blau:
                continue
            seen_blau.add(dot_pattern(root))
            matched = str(getattr(card, "matched_key", "") or "").strip()
            evidence = f"{matched} ({name})" if matched and normalize_ja(matched) != normalize_ja(name) else name
            out.append(BorrowedRoot(root, BORROW_BLAU, evidence))
    seen: set[str] = set()   # by the root's letters and dots: ג̇לל and גלל are two roots
    if friedman is not None and not getattr(friedman, "stem", 0):
        for match in getattr(friedman, "matches", ()) or ():
            via = getattr(match, "via", "")
            matched = str(getattr(match, "matched", "") or "").strip()
            if via != "index":   # the word itself or its base form: an entry it heads only
                unit = getattr(match, "unit", None)
                if via == "verb_form" or getattr(unit, "kind", "") != "entry" \
                        or not head_begins_with(getattr(unit, "head_text", ""), lookup_key(matched)):
                    continue
            root = str(getattr(match, "root", "") or "").strip()
            key = normalize_ja(root)
            if not key or " " in key or dot_pattern(root) in seen:
                continue
            seen.add(dot_pattern(root))
            if via == "index":
                evidence = matched or root   # the index form as the book prints it, "קאל (קול)"
            elif not matched or normalize_ja(matched) == key:
                evidence = root
            else:   # a lookup key (finals folded): written with its final letters, "סלאם"
                evidence = f"{with_final_letters(matched)} ({root})"
            out.append(BorrowedRoot(root, BORROW_FRIEDMAN, evidence))
    roots = getattr(lane, "roots", ()) if lane is not None else ()
    if roots:
        first = roots[0]
        name = str(getattr(first, "name", "") or "")
        root = ja_root_of_arabic(name, str(getattr(first, "root_key", "") or ""))
        if normalize_ja(root):
            out.append(BorrowedRoot(root, BORROW_LANE, name))
    return tuple(out)

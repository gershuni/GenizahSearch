# -*- coding: utf-8 -*-
"""A typed root, its dots and its verb stems: what Blau's and Friedman's lookups share for looking
a root up and choosing one of its stems.

The private dictionary lookup plan (section 4.6): the reader types a root, and
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
Blau's entry of that root after its own (Blau has no list of inflected forms, so
קאל reached no entry of its own; Friedman finds קול through its index and Lane through its
reading). ``borrowed_roots`` reads them off Friedman's and Lane's answers: every root Friedman's
matches name, in its order, then Lane's first root, written in Hebrew letters as Blau spells a
root (``ja_root_of_arabic``).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Optional, Sequence

from shared.arabic_normalize import DOTTED, LETTERS
from shared.blau_normalize import (QUOTE_MARKS, fold_finals, interchange_variants, normalize_ja, query_stems,
                                   spelling_key, spelling_variant)
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


#: The exchange of צ and ט (``shared.blau_normalize.interchange_variants``) is shown only where the
#: dictionary prints the letter reached with the dot: the ض/ظ pair, not ص/ط. Measured on the
#: automatic text (``scripts/measure_dictionary_interchange.py``): 73% of what an exchange reached
#: in Blau without the rule had a ط/ص root, and Blau printed the dot on every ض/ظ root reached.
#: The measurement sets it False to count both rules.
INTERCHANGE_DOTTED_ONLY = True
_EXCHANGED_FOR = {"צ": "ט", "ט": "צ"}
#: The later pass (a spelling the word does not have as written) is fetched this deep before the
#: dotted rule filters it, then four times deeper each round until the page holds ``limit`` valid
#: cards or the dictionary has no more: a valid card behind undotted ones still makes the page.
#: ``LATER_PASS_MAX`` is more than either dictionary's entries, so the widening reaches the end.
LATER_PASS_FETCH = 200
LATER_PASS_MAX = 200 * 4 ** 6


def exchanged_letters(word: str, matched: str) -> Optional[tuple[tuple[str, int], ...]]:
    """The letters ``matched`` has where ``word`` (one of its ``query_stems``) was exchanged to
    reach it, each as (letter, which occurrence of that letter in ``matched``, from 0): טצא
    reaching טטא is (("ט", 1),), the second ט. None when no exchange of the word's stems
    reaches ``matched``."""
    target = spelling_key(matched)
    for stem in query_stems(normalize_ja(word)):
        for variant in interchange_variants(stem):
            if spelling_key(variant) == target:
                return tuple((variant[i], variant[:i].count(variant[i]))
                             for i in range(len(variant)) if variant[i] != stem[i])
    return None


def _dotted_occurrences(spelling: str, places: tuple[tuple[str, int], ...]) -> bool:
    """Does the spelling's first word print each (letter, occurrence) of ``places`` with the dot?"""
    pattern = dot_pattern(spelling).split(" ", 1)[0]
    seen: dict[str, int] = {}
    dotted: set[tuple[str, int]] = set()
    for i, ch in enumerate(pattern):
        if ch == "*" or ch not in _EXCHANGED_FOR:
            continue
        n = seen.get(ch, 0)
        seen[ch] = n + 1
        if pattern[i + 1:i + 2] == "*":
            dotted.add((ch, n))
    return all(place in dotted for place in places)


#: The marks a printed spelling separates alternatives with (עטם/עט̇ם).
_ALTERNATIVES_RE = re.compile(r"[/|]")


def interchange_dotted(word: str, spellings: Sequence[str], matched: str = "") -> bool:
    """Does a dictionary spelling print, with the dot, the letter the ``word``'s צ or ט was
    exchanged for to reach ``matched`` (ט̇ for a word's צ, צ̇ for its ט), at that letter? The
    letter is counted by its occurrence (``exchanged_letters``), so a root spelling reached by a
    form (עט̇ם by עטימ) is read at the same ט, and a dot on an unchanged letter never counts.
    Without ``matched`` (or when no exchange reaches it), any dotted letter of the kind exchanged
    for. Only the spellings that ARE the matched one are read, when any is (a dotted root label
    never vouches for an undotted sub-entry filed under it); ``matched`` may carry its own printed
    form, as an index row's "עט̇ם (כתב)" does. A spelling written with alternatives (עטם/עט̇ם) is
    read alternative by alternative. True for every spelling when the rule is off."""
    if not INTERCHANGE_DOTTED_ONLY:
        return True
    spellings = [part for s in spellings if s for part in _ALTERNATIVES_RE.split(s) if part.strip()]
    form = (matched or "").split(" (", 1)[0].strip()
    key = normalize_ja(form)
    places = exchanged_letters(word, key) if key else None
    if places:
        # ``matched`` is a printed spelling only when it carries marks; a bare key says nothing.
        printed = [s for s in ((form,) if form != key else ()) + tuple(spellings) if s]
        same = [s for s in printed if spelling_key((normalize_ja(s).split(" ", 1) or [""])[0]) == spelling_key(key)]
        return any(_dotted_occurrences(s, places) for s in (same or printed))
    letters = {_EXCHANGED_FOR[ch] for ch in fold_finals(normalize_ja(word)) if ch in _EXCHANGED_FOR}
    patterns = [dot_pattern(s) for s in spellings if s]
    return any(f"{letter}*" in p for p in patterns for letter in letters)


def place_rejects(place: Any, candidates: Sequence[Sequence[str]]) -> Optional[set[tuple[int, str, int]]]:
    """None for a place that passes ``place_dotted``; else (position, key, word) for each key a
    typed word matched only by an exchange whose letter is printed without the dot: that key is
    refused to that word alone (``shared.ja_expression.match_scope``), and the scope is matched
    again, so a later occurrence printed with the dot is still found (חפט אללה ... חפט̇ אללה), and
    another word that has the key as written may still take the token ("עצם עטם", in either order)."""
    if place_dotted(place, candidates):
        return None
    components = tuple(getattr(place, "components", ()) or ())
    positions = tuple(getattr(place, "search_positions", ()) or ()) or tuple(getattr(place, "positions", ()) or ())
    out: set[tuple[int, str, int]] = set()
    for n, (component, pos) in enumerate(zip(components, positions)):
        own = candidates[n] if n < len(candidates) else ()
        if component.matched in own or interchange_dotted(component.word, (component.printed,),
                                                          component.matched.lstrip("~")):
            continue
        # Only the key that failed: another exchanged key of the same printed word (an alternative,
        # טטא/צצ̇א) may still pass; each retry refuses one more key, so the retries end.
        out.add((pos, component.matched, n))
    return out


def place_dotted(place: Any, candidates: Sequence[Sequence[str]]) -> bool:
    """An expression's place (``shared.ja_expression.ExpressionPlace``) reached with צ and ט
    exchanged: does every printed word that a typed word reached only by the exchange print the
    letter exchanged for with the dot, at that letter? ``candidates`` are each typed word's keys
    as written, in the expression's order."""
    components = tuple(getattr(place, "components", ()) or ())
    for n, component in enumerate(components):
        own = candidates[n] if n < len(candidates) else ()
        if component.matched in own:
            continue
        if not interchange_dotted(component.word, (component.printed,), component.matched.lstrip("~")):
            return False
    return True


def exchanged_only(record: Any) -> bool:
    """A card, match or hit reached only with צ and ט exchanged."""
    if getattr(record, "possible_spelling", False):
        return True
    paths = getattr(record, "analyses", ())
    return bool(paths) and all("interchange" in path for path in paths)


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
        if stem and lookup_key(root) and not any(parse_stem(w) for w in rest):
            return (root if _HEBREW_LETTER_RE.search(root) else lookup_key(root)), stem
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
#: tab borrows from Friedman and Lane, Friedman's from Blau and Lane.
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


#: The letters a root may lose or hide in its forms (hollow, weak, hamza, the ta marbuta of a noun's
#: name): ``carries_root`` does not ask for them.
SOFT_LETTERS = frozenset("אויה")


def strong_letters(root_key: str) -> str:
    """A root's letters a form of it keeps, in order: those of ``root_key`` (a lookup key) other than
    ``SOFT_LETTERS`` and spaces, a doubled letter once (מדד: מד)."""
    out: list[str] = []
    for ch in root_key or "":
        if ch in SOFT_LETTERS or not _HEBREW_LETTER_RE.match(ch) or (out and out[-1] == ch):
            continue
        out.append(ch)
    return "".join(out)


def carries_root(key: str, root_key: str) -> bool:
    """Does a lookup key hold a root's strong letters (``strong_letters``), in order? A form of the
    root does (רבאטא of רבט, אלעאלמון of עלם); a word that only stands beside the root in a phrase
    does not: "לה" is written in Blau's entry אכ̇ד̇ for "אכ̇ד̇ לה", "אלקול" in טלע's
    (such words reached those entries as if they were theirs). A root with no strong
    letter (איה̈) is carried by every key."""
    remaining = iter(key or "")
    return all(ch in remaining for ch in strong_letters(root_key))


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
    steps: tuple[str, ...] = ()


def borrowed_roots(friedman: Any = None, lane: Any = None, blau: Any = None,
                   verb_forms: bool = False) -> tuple[BorrowedRoot, ...]:
    """The roots the other dictionaries' answers give for one word: every entry Blau's OWN cards
    show (not those it borrowed: no root comes back to the dictionary that gave it), by its first
    printed name; the roots Friedman's matches name, in Friedman's order: its index of forms, and an
    entry whose head begins with the word or its base form; then Lane's first root (the root of its
    first reading). Not a Friedman sub-entry, stem paragraph or entry that holds the word further in
    its head: such a head is mostly a phrase, the word a verb's complement, so its
    root is the verb's (אלעלמא reached דרס through "דרס עِלْמאً", חצל and "חוז עِלْמאً").
    Blau's tab asks for Friedman's and Lane's, Friedman's for Blau's and Lane's. Each root once per
    dictionary (by its letters and dots); () for an expression, a typed stem or a dictionary not
    served (None). Never a card or match found by verb form (``shared.ja_verb_forms``, PR 3): both
    dictionaries run the same rules, so it is no second dictionary's word for the root, and each
    shows its own. ``verb_forms`` True is Lane's tab asking (``ArabicService.borrow``): Lane lists
    other stems than Blau and Friedman do, so a root one of them reached by verb form is theirs to
    give it. Reads the answers' fields only: no service is imported here."""
    out: list[BorrowedRoot] = []
    if blau is not None and not getattr(blau, "stem", 0):
        seen_blau: set[str] = set()
        for card in getattr(blau, "cards", ()) or ():
            by_form = bool(getattr(card, "verb_form", ""))
            if by_form and not verb_forms or getattr(card, "borrowed", ()) and not by_form:
                continue
            paths = getattr(card, 'analyses', ())
            if paths and all(spelling_variant(path) for path in paths):
                continue
            name = str(getattr(card, "name", "") or "")
            root = str(getattr(card, "root_label", "") or "").strip() or name
            if not normalize_ja(root) or dot_pattern(root) in seen_blau:
                continue
            seen_blau.add(dot_pattern(root))
            matched = str(getattr(card, "matched_key", "") or "").strip()
            evidence = f"{matched} ({name})" if matched and normalize_ja(matched) != normalize_ja(name) else name
            out.append(BorrowedRoot(root, BORROW_BLAU, evidence,
                                    tuple(dict.fromkeys(step for path in getattr(card, "analyses", ()) for step in path))))
    seen: set[str] = set()   # by the root's letters and dots: ג̇לל and גלל are two roots
    if friedman is not None and not getattr(friedman, "stem", 0):
        for match in getattr(friedman, "matches", ()) or ():
            paths = getattr(match, 'analyses', ())
            if paths and all(spelling_variant(path) for path in paths):
                continue
            via = getattr(match, "via", "")
            matched = str(getattr(match, "matched", "") or "").strip()
            if via == "verb_form":
                if not verb_forms:
                    continue
            elif via != "index":   # the word itself or its base form: an entry it heads only
                unit = getattr(match, "unit", None)
                if getattr(unit, "kind", "") != "entry" \
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
            out.append(BorrowedRoot(root, BORROW_FRIEDMAN, evidence,
                                    tuple(dict.fromkeys(step for path in getattr(match, "analyses", ()) for step in path))))
    roots = [r for r in getattr(lane, "roots", ()) if not getattr(r, "borrowed", ())] if lane is not None else []
    if roots:   # Lane's own first root: one it borrowed (``ArabicService.borrow``) is not its word
        first = roots[0]
        name = str(getattr(first, "name", "") or "")
        root = ja_root_of_arabic(name, str(getattr(first, "root_key", "") or ""))
        if normalize_ja(root):
            out.append(BorrowedRoot(root, BORROW_LANE, name, ("script_reading", "root")))
    return tuple(out)

def supported_hollow_perfect(word: str, root: str, stems: Sequence[int]) -> bool:
    """A short hollow perfect supported by an independently supplied root and stem I.

    This ranks a borrowed possibility; it does not enable generated C/E cards.
    Explicit dots continue to distinguish a competing consonant reading.
    """
    key, base = lookup_key(word), lookup_key(root)
    if "*" in dot_pattern(word) or 1 not in stems or len(base) != 3 or base[1] not in "וי":
        return False
    forms = {base[0] + base[2] + ending for ending in ("ת", "תי", "נא", "תם", "תן")}
    return key in forms or key.startswith("ו") and key[1:] in forms

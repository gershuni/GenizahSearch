# -*- coding: utf-8 -*-
"""The forms a verb stem of a three-letter root takes in unvocalized Judeo-Arabic, and which of a
root's stems a written word belongs to.

The private dictionary lookup plan (section 4.2; owner, 2026-11-11: the rules were first used only to
pick the stem of a root already found and open its card on that stem). Generate, do not analyse: for a
root and a stem the dictionary LISTS, every way the stem may be written is spelled out, on the
lookup's own keys (``normalize_ja``: no dots, finals folded), and a word belongs to the stems whose
forms hold one of its keys (``candidate_stems``: the clitics are peeled there, not here).

- **The table** (plan 4.2), for C1 C2 C3: I C1C2C3 / C1אC2C3, מC1C2וC3; II C1C2C3 / מC1C2C3,
  תC1C2יC3; III C1אC2C3 / מC1אC2C3; IV אC1C2C3, imperfect C1C2C3 / מC1C2C3, אC1C2אC3; V תC1C2C3 /
  מתC1C2C3; VI תC1אC2C3 / מתC1אC2C3; VII אנC1C2C3, נC1C2C3 / מנC1C2C3, אנC1C2אC3; VIII אC1תC2C3,
  C1תC2C3 / מC1תC2C3, אC1תC2אC3; IX אC1C2C3, C1C2C3; X אסתC1C2C3, סתC1C2C3 / מסתC1C2C3, אסתC1C2אC3.
- **Weak roots**, every variant beside the regular one: doubled (C2 = C3; the contracted form, דל
  beside דלל, אסתקר beside אסתקרר); hollow (C2 ו or י: קאל, קל-, יקול / יקיל / יקאל, קאיל,
  אסתגאב, אסתגאבה); final weak (C3 ו, י or א: the last letter as א, י, ו or dropped); first ו
  (imperfect of I without it, VIII אתצל, verbal nouns in אי- / אסתי-); first א (VIII אתכד, IV with
  one א); stem VIII assimilation (ז: זד; צ, ט: ט; ד, ת: merged).
- **Endings and prefixes**: perfect ת, תמ, תנ, נא, ו, וא, א, נ, תי, תו; imperfect prefixes י ת נ א
  with ו, וא, ונ, ינ, י, נ; nominal ה, ינ, ונ, את, א, י. A form shorter than three letters is
  dropped (דל, קל), and a verb whose final weak letter is dropped takes an ending (version 2:
  ירמו, not ירמ).

``word_stem`` answers a stem only when exactly one listed stem holds the word: the root itself
(קרר is I and II) and a participle that is also a perfect (טאלב is I and III) choose none. Pure
functions, no database (``switch_kinds`` alone reads the environment); ``VERB_FORMS_VERSION`` names
the rules a measurement ran.

**The cards found by verb form** (plan 4.4 and 6.1; PR 3): ``verb_form_cards`` turns a word's keys
(its own first, then its clitics peeled) into one card per entry they reach, in the plan's order: the
word's own key before a peeled one, a stem with a prefix of its own (III, V to X: ``PREFIXED``) before
I, II and IV, a longer form before a shorter. Each hit has a kind (``hit_kind``), and a card the kind
of its best hit: A own key, prefixed stem; B own key, I/II/IV, four letters or more; C own key,
I/II/IV, three letters; D peeled key, prefixed stem; E peeled, I/II/IV. Which kinds a dictionary
shows is a switch (``switch_kinds``; A, B and D by default since the graded sample, owner
2026-11-11); beside the entries the word itself reaches, the roots are only suggested
(``suggested_roots``).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Iterable, Mapping, Sequence

from shared.ja_roots import dot_pattern, stem_label, with_final_letters

#: 2 (2026-11-11): a verb whose final weak letter is dropped takes an ending (``stem_forms``).
VERB_FORMS_VERSION = 2

#: The letters a weak root has in second or third place (and א as a third).
WEAK = "וי"
PERFECT_ENDINGS = ("", "ת", "תמ", "תנ", "נא", "ו", "וא", "א", "נ", "תי", "תו")
IMPERFECT_PREFIXES = ("י", "ת", "נ", "א")
IMPERFECT_ENDINGS = ("", "ו", "וא", "ונ", "ינ", "י", "נ")
NOMINAL_ENDINGS = ("", "ה", "ינ", "ונ", "את", "א", "י")
MIN_LETTERS = 3


def _after_c1(c2: str, c3: str, weak: bool = True, bare: bool = True) -> tuple[list[str], list[str]]:
    """What follows C1 where it meets C2 directly (stems I, IV, VII, VIII, X): the perfect's and
    the imperfect's, the regular first, then a weak root's variants (``weak``). ``bare`` False leaves
    out the final weak letter dropped (``stem_forms`` uses it to tell those bases apart)."""
    perfect, imperfect = [c2 + c3], [c2 + c3]
    if not weak:
        return perfect, imperfect
    if c2 == c3:                      # doubled: דל beside דלל
        perfect.append(c2)
        imperfect.append(c2)
    if c3 in WEAK + "א":              # final weak: the last letter as א, י, ו or dropped
        perfect += [c2 + "א", c2 + "י", c2 + "ו"] + ([c2] if bare else [])
        imperfect += [c2 + "י", c2 + "ו", c2 + "א"] + ([c2] if bare else [])
    elif c2 in WEAK:                  # hollow: קאל, קל-; יקול, יקיל, יקאל, יקל-
        perfect += ["א" + c3, c3]
        imperfect += ["ו" + c3, "י" + c3, "א" + c3, c3]
    return perfect, imperfect


def _after_long(c2: str, c3: str, weak: bool = True, bare: bool = True) -> list[str]:
    """What follows C1 where C2 is doubled or follows a long vowel (stems II, III, V, VI)."""
    out = [c2 + c3]
    if not weak:
        return out
    if c3 in WEAK + "א":
        out += [c2 + "י", c2 + "א", c2 + "ו"] + ([c2] if bare else [])
    if c2 == c3:
        out.append(c2)
    return out


def _stem_bases(root: str, stem: int, weak: bool = True, assimilation: bool = True,
                bare: bool = True) -> tuple[list[str], list[str], list[str]]:
    """(perfect, imperfect, nominal) bases of one stem of a three-letter root key, before endings.
    ``weak`` and ``assimilation`` off are the gate's mutation runs (plan section 6.5): the weak-root
    variants, and stem VIII's assimilated heads, left out. ``bare`` off leaves out the bases with a
    final weak letter dropped."""
    c1, c2, c3 = root
    perfect_tail, imperfect_tail = _after_c1(c2, c3, weak, bare)
    long_tail = _after_long(c2, c3, weak, bare)
    hollow, final_weak = weak and c2 in WEAK, weak and c3 in WEAK + "א"
    first_w, first_alef, doubled = weak and c1 == "ו", weak and c1 == "א", weak and c2 == c3
    perfect: list[str] = []
    imperfect: list[str] = []
    nominal: list[str] = []
    if stem == 1:
        perfect = [c1 + t for t in perfect_tail]
        imperfect = [c1 + t for t in imperfect_tail]
        if first_w:                   # first ו: the imperfect without it (יצל)
            imperfect.append(c2 + c3)
        nominal = [c1 + "א" + c2 + c3, "מ" + c1 + c2 + "ו" + c3]
        if hollow:
            nominal += [c1 + "אי" + c3, "מ" + c1 + "ו" + c3, "מ" + c1 + "י" + c3]
        if final_weak:
            nominal += [c1 + "א" + c2 + "י", c1 + "א" + c2, "מ" + c1 + c2 + "י"]
        if doubled:
            nominal.append(c1 + "א" + c2)
    elif stem == 2:
        perfect = imperfect = [c1 + t for t in long_tail]
        nominal = ["מ" + c1 + t for t in long_tail] + ["ת" + c1 + c2 + "י" + c3]
    elif stem == 3:
        perfect = imperfect = [c1 + "א" + t for t in long_tail]
        nominal = ["מ" + c1 + "א" + t for t in long_tail]
    elif stem == 4:
        perfect = ["א" + c1 + t for t in perfect_tail]
        imperfect = [c1 + t for t in imperfect_tail]
        nominal = ["מ" + c1 + t for t in imperfect_tail] + ["א" + c1 + c2 + "א" + c3]
        if hollow:
            nominal.append("א" + c1 + "א" + c3 + "ה")
        if first_w:
            nominal.append("אי" + c2 + "א" + c3)
        if first_alef:                # first א: stem IV written with one א
            perfect += [c1 + t for t in perfect_tail]
    elif stem == 5:
        perfect = imperfect = ["ת" + c1 + t for t in long_tail]
        nominal = ["מת" + c1 + t for t in long_tail]
    elif stem == 6:
        perfect = imperfect = ["ת" + c1 + "א" + t for t in long_tail]
        nominal = ["מת" + c1 + "א" + t for t in long_tail]
    elif stem == 7:
        perfect = ["אנ" + c1 + t for t in perfect_tail]
        imperfect = ["נ" + c1 + t for t in perfect_tail]
        nominal = ["מנ" + c1 + t for t in perfect_tail] + ["אנ" + c1 + c2 + "א" + c3]
        if hollow:
            nominal.append("אנ" + c1 + "יא" + c3)
    elif stem == 8:
        if not assimilation:
            heads = [c1 + "ת"]
        elif c1 == "ז":               # assimilation: אזדחם
            heads = ["זד", "זת"]
        elif c1 in "צט":              # אצטלח
            heads = [c1 + "ט", c1 + "ת"]
        elif c1 in "דת":              # merged
            heads = [c1, c1 + "ת"]
        elif c1 in "וא":              # first ו or א: אתצל, אתכד
            heads = ["ת", c1 + "ת"]
        else:
            heads = [c1 + "ת"]
        bases = [h + t for h in heads for t in perfect_tail]
        perfect = ["א" + b for b in bases]
        imperfect = bases
        nominal = ["מ" + b for b in bases] + ["א" + h + c2 + "א" + c3 for h in heads]
        if hollow:
            nominal += ["א" + h + "יא" + c3 for h in heads]
    elif stem == 9:
        perfect, imperfect = ["א" + c1 + c2 + c3], [c1 + c2 + c3]
    elif stem == 10:
        tails = list(dict.fromkeys(imperfect_tail + perfect_tail))
        perfect = ["אסת" + c1 + t for t in perfect_tail]
        imperfect = ["סת" + c1 + t for t in tails]
        nominal = ["מסת" + c1 + t for t in tails] + ["אסת" + c1 + c2 + "א" + c3]
        if hollow:
            nominal.append("אסת" + c1 + "א" + c3 + "ה")
        if first_w:
            nominal.append("אסתי" + c2 + "א" + c3)
    return perfect, imperfect, nominal


@lru_cache(maxsize=16384)
def stem_forms(root: str, stem: int, *, weak: bool = True, assimilation: bool = True,
               min_letters: int = MIN_LETTERS, bare_alone: bool = False) -> frozenset[str]:
    """Every key (no clitics) one stem of a root may be written as. ``root`` is a lookup key of
    three letters (``normalize_ja``); anything else, or a stem outside 1 to 10, has none. The
    keyword arguments are the gate's mutation runs; the lookup uses the defaults.

    A verb whose final weak letter is dropped takes an ending (owner, 2026-11-11; rules version 2):
    רמת, ירמו, ירמון, but not a bare ירמ or אסתקר of קרא, the short form after לם, which mostly
    spells another word (תלתה, "three", read as stem VIII of להו). ``bare_alone`` True is version
    1's rule, the gate's measure of the change."""
    if len(root or "") != 3 or " " in root or not 1 <= int(stem) <= 10:
        return frozenset()
    perfect, imperfect, nominal = _stem_bases(root, int(stem), weak, assimilation)
    bare_perfect: set[str] = set()
    bare_imperfect: set[str] = set()
    if weak and not bare_alone:
        kept = _stem_bases(root, int(stem), weak, assimilation, bare=False)
        bare_perfect, bare_imperfect = set(perfect) - set(kept[0]), set(imperfect) - set(kept[1])
    out: set[str] = set()
    for base in perfect:
        out.update(base + end for end in PERFECT_ENDINGS if end or base not in bare_perfect)
    for base in imperfect:
        out.update(pre + base + end for pre in IMPERFECT_PREFIXES for end in IMPERFECT_ENDINGS
                   if end or base not in bare_imperfect)
    for base in nominal:
        out.update(base + end for end in NOMINAL_ENDINGS)
    return frozenset(form for form in out if len(form) >= min_letters)


def verb_form_index(pairs: Iterable[Any], **rules: Any) -> dict[str, tuple[Any, ...]]:
    """Every form key the pairs a dictionary lists may be written as, and the pairs that give it
    (plan section 4.3: the list a service builds once from its own pairs; ``shared.ja_roots.StemPair``,
    read through ``root_key`` and ``stem``). Pairs of a root that is not three letters give none.
    ``rules`` are ``stem_forms``' keyword arguments (the gate's mutation runs)."""
    index: dict[str, list[Any]] = {}
    for pair in pairs:
        for form in stem_forms(pair.root_key, pair.stem, **rules):
            index.setdefault(form, []).append(pair)
    return {form: tuple(found) for form, found in index.items()}


def word_stems(keys: Iterable[str], root: str, stems: Iterable[int]) -> tuple[int, ...]:
    """The stems among ``stems`` (those the dictionary lists for ``root``) whose forms hold one of
    ``keys`` (the word's lookup keys, its clitics peeled: ``candidate_stems``), in order."""
    keys = {k for k in keys if k}
    return tuple(stem for stem in sorted(set(stems)) if keys & stem_forms(root, stem))


def word_stem(keys: Iterable[str], root: str, stems: Iterable[int]) -> int:
    """The one listed stem the word belongs to, or 0 when none or several do (קרר is I and II;
    טאלב is the participle of I and the perfect of III)."""
    found = word_stems(keys, root, stems)
    return found[0] if len(found) == 1 else 0


# ---------------------------------------------------------------------------
# The cards found by verb form (plan 4.4 and 6.1; PR 3)
# ---------------------------------------------------------------------------

#: The stems with a prefix or an infix of their own (plan 4.4): a card found by one comes first.
PREFIXED = frozenset({3, 5, 6, 7, 8, 9, 10})
#: The kinds of a hit, in the order the plan names them (section 6.1).
KINDS = ("A", "B", "C", "D", "E")
#: How many cards found by verb form a lookup shows (plan 4.4); the rest are counted.
MAX_VERB_FORM_CARDS = 3
#: Beside the cards the word itself reaches, the roots its verb forms give are only suggested, at
#: most this many (owner, 2026-11-11: there a card found by verb form was mostly wrong).
MAX_SUGGESTIONS = 5


def hit_kind(stem: int, own: bool, length: int) -> str:
    """The kind of one hit (plan 6.1): A the word's own key and a prefixed stem; B own key, stem I,
    II or IV, a form of four letters or more; C the same with three letters; D a clitic-peeled key
    and a prefixed stem; E peeled, I, II or IV."""
    if own:
        if stem in PREFIXED:
            return "A"
        return "B" if length >= 4 else "C"
    return "D" if stem in PREFIXED else "E"


def hit_rank(stem: int, own: bool, length: int) -> tuple[bool, bool, int]:
    """Plan 4.4: the word's own key first, then a prefixed stem, then a longer form."""
    return (not own, stem not in PREFIXED, -length)


def parse_kinds(text: Any) -> frozenset[str]:
    """The kinds a switch's value names ("A,B,D"; "a b d"): every letter of ``KINDS`` in it, case
    and separators ignored; anything else dropped. '' or None: none (the default: off)."""
    return frozenset(ch for ch in str(text or "").upper() if ch in KINDS)


def switch_kinds(name: str, default: str) -> frozenset[str]:
    """The kinds an environment switch names, read on every call (``parse_kinds``): ``default``
    while the variable is unset; set and empty, none. Each service names its own switches
    (``BLAU_VERB_FORM_KINDS`` ...)."""
    value = os.environ.get(name)
    return parse_kinds(default if value is None else value)


@dataclass(frozen=True)
class VerbFormCard:
    """One entry a word reached by verb form, with the hits that reached it: (stem, the word's own
    key, the form's length) each, and in ``hit_keys`` the key that made each hit."""
    entry_id: str
    root: str          # as the dictionary prints it (dots kept)
    root_key: str
    hits: tuple[tuple[int, bool, int], ...]
    hit_keys: tuple[str, ...]

    def rank(self) -> tuple[bool, bool, int]:
        return min(hit_rank(*hit) for hit in self.hits)

    def _best(self) -> int:
        return min(range(len(self.hits)), key=lambda n: hit_rank(*self.hits[n]))

    @property
    def kind(self) -> str:
        """The kind of the card's best hit (plan 6.1)."""
        return hit_kind(*self.hits[self._best()])

    @property
    def form(self) -> str:
        """The key of the card's best hit: the word's form that reached the entry."""
        return self.hit_keys[self._best()]

    @property
    def stems(self) -> tuple[int, ...]:
        """The listed stems the word's forms fit, I to X in order."""
        return tuple(sorted({stem for stem, _own, _length in self.hits}))

    @property
    def stem(self) -> int:
        """The stem the card may open on: the one listed stem the word fits, else 0 (as
        ``word_stem``: טאלב fits I and III and opens on none)."""
        stems = self.stems
        return stems[0] if len(stems) == 1 else 0

    def evidence(self) -> str:
        """What the card says reached it: the form, then the root and the stems ("אסתדלו (ד̇לל X)",
        "טאלב (טלב I, III)"): the shape of the book's index line "form (root)" and of the typed
        shortcut "דלל X", so no word of it needs translating. The form is written with its final
        letter ("יסתעמלון", not the key's "יסתעמלונ")."""
        return f"{with_final_letters(self.form)} ({self.root} {', '.join(stem_label(s) for s in self.stems)})"


def verb_form_cards(keys: Sequence[str], index: Mapping[str, Sequence[Any]]) -> list[VerbFormCard]:
    """The entries a word's keys reach by verb form (``index``: a service's ``verb_form_index``, or a
    part of it), one card per entry, in plan 4.4's order (stable on first reach). ``keys[0]`` is the
    word's own key, the rest its clitics peeled (``candidate_stems``). The pairs are read through
    ``entry_id``, ``root``, ``root_key`` and ``stem`` (``shared.ja_roots.StemPair``)."""
    found: dict[str, tuple[Any, list[tuple[int, bool, int]], list[str]]] = {}
    for n, key in enumerate(keys):
        for pair in index.get(key, ()) if key else ():
            _pair, hits, hit_keys = found.setdefault(pair.entry_id, (pair, [], []))
            hit = (int(pair.stem), n == 0, len(key))
            if hit not in hits:
                hits.append(hit)
                hit_keys.append(key)
    cards = [VerbFormCard(entry_id, pair.root, pair.root_key, tuple(hits), tuple(hit_keys))
             for entry_id, (pair, hits, hit_keys) in found.items()]
    return sorted(cards, key=VerbFormCard.rank)


def suggested_roots(keys: Sequence[str], index: Mapping[str, Sequence[Any]], kinds: Iterable[str],
                    shown: Iterable[Any] = (), limit: int = MAX_SUGGESTIONS) -> tuple[tuple[str, int], ...]:
    """The roots a word's verb forms give beside the entries the word itself reached (``shown``, by
    entry id): (the root as printed, the one stem the word fits or 0), in the order the cards would
    come (``verb_form_cards``), of a kind in ``kinds``, each (root with its dots, stem) once, at most
    ``limit``. What both dictionaries draw as the chips "Other possible roots"."""
    kinds, shown = frozenset(kinds), set(shown)
    if not kinds or not index:
        return ()
    out: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for card in verb_form_cards(keys, index):
        if card.kind not in kinds or card.entry_id in shown:
            continue
        key = (dot_pattern(card.root), card.stem)
        if key not in seen:
            seen.add(key)
            out.append((card.root, card.stem))
            if len(out) >= limit:
                break
    return tuple(out)

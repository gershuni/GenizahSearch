# -*- coding: utf-8 -*-
"""The expression search the Judeo-Arabic dictionaries share: keys, the matcher, the tiers.

Several words selected on /browse or typed in the dictionary panel's box are an EXPRESSION, and a
dictionary answers with every scope of its own text (an entry's name, a quotation) holding them IN
ORDER with at most ``gap`` printed words between each consecutive pair. Friedman's dictionary had
this first (``shared/friedman_service.py``, PR #487); Blau's got it next (``shared/blau_service.py``)
and the two key, match and rank one way, here, so a sidecar built by one rule set is read by the
same. What a dictionary adds is its own: which scopes it has, what a unit is, how a hit is shown.

Keys (``word_keys``, ``scope_keys``): a printed or typed word answers to its own key
(``normalize_ja`` after the harakat a PDF stores as presentation forms are dropped) and to its
clitic alternatives -- ``candidate_stems`` of ``MIN_EXPRESSION_STEM`` letters or more, a two-letter
remainder of proclitics alone (ולא -> לא), the preposition under a pronoun suffix (בהא -> ב). A
printed word written with alternatives (דלך/פעל) keys as each; optional letters, (א)קצר, both
ways. The sidecar builders write these rows into a ``tokens`` table and the readers query them,
so ``KEYING_VERSION`` is written to a sidecar's ``meta`` and pinned by its reader: a file keyed by
other rules is refused, never silently missed.

The matcher (``ordered_match``) keeps the tightest occurrence in a scope; the tiers (``tiered``)
put the unit whose own name IS the expression first, then the units whose name (or their entry's)
holds the words in order, then the rest, contiguous before gapped, and show an expression made of
function words only (אללה עליה, מן X) among the rest only where its words stand together: such
places were a false suggestion in 65% of the strangers' quotations they were found in (the review
of Friedman's candidate, 2026-11-08).
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Collection, Hashable, Mapping, Optional, Sequence, TypeVar

from shared.blau_normalize import PRONOUN_SUFFIXES, candidate_stems, normalize_ja

#: The rules below, as a number a sidecar's ``meta.keying_version`` carries: bump it when
#: ``word_keys`` or ``scope_keys`` changes what they write, and rebuild every sidecar's tokens.
KEYING_VERSION = 1

#: The expression search: words per expression, printed words allowed between two of them (the
#: default and the most a reader may ask).
MAX_EXPRESSION_WORDS = 6
MAX_GAP = 5
DEFAULT_GAP = 2

U = TypeVar("U", bound=Hashable)


def clamp_gap(gap: Any) -> int:
    """A gap a reader may ask for: an integer in 0..MAX_GAP, the default for anything else."""
    try:
        value = int(gap)
    except (TypeError, ValueError):
        return DEFAULT_GAP
    return min(max(value, 0), MAX_GAP)


def lookup_key(word: str) -> str:
    """The normalizer's key, after the harakat a PDF stores as presentation forms are dropped."""
    plain = "".join(ch for ch in word if not ("ﱞ" <= ch <= "ﱣ" or "ﹰ" <= ch <= "ﹿ")
                    and not unicodedata.category(ch).startswith("M"))
    return normalize_ja(plain)


#: A clitic alternative shorter than this is not an expression key, except a two-letter
#: remainder of proclitics alone (ולא -> לא, אלהם -> הם, לסד -> סד; the evaluation of 2026-11-08
#: found three of its five real misses here) and the preposition a pronoun suffix was added to
#: (בהא -> ב, מנה -> מן), which lets בה stand for בהא. The article never leaves לה behind on
#: אללה: that remainder would match the name everywhere. The word's own key is always a key.
MIN_EXPRESSION_STEM = 3
PROCLITIC_RE = re.compile(r"^[ופ]?[בלכ]?(?:אל)?$")
NO_ARTICLE_PEEL = frozenset((normalize_ja("אללה"),))
PREPOSITIONS = frozenset(normalize_ja(p) for p in ("ב", "ל", "כ", "פי", "מן", "ען", "מע", "עלי", "אלי", "ענד", "בין", "חתי", "עלא", "לדי"))
OPTIONAL_LETTERS_RE = re.compile(r"\(([א-ת]{1,2})\)")  # a printed head's optional letters, (א)קצר


def short_remainder(word: str, candidate: str) -> bool:
    """A two-letter candidate is a key when only proclitics were peeled off the word; after a
    one-letter proclitic a pronoun (בהא, להם) is not a word, after the article it is (אלהם)."""
    prefix = word[:-2]
    if len(candidate) != 2 or not word.endswith(candidate) or not prefix or not PROCLITIC_RE.match(prefix):
        return False
    if prefix.endswith("אל"):
        return "אל" + candidate not in NO_ARTICLE_PEEL
    return candidate not in PRONOUN_SUFFIXES


#: Words that carry no sense of their own in an expression: prepositions (and their pronoun
#: forms, through ``preposition_stems``), particles, pronouns, and אללה, which opens half the
#: blessings.
FUNCTION_WORDS = frozenset(normalize_ja(w) for w in (
    "ב", "ל", "כ", "פי", "מן", "ען", "מע", "עלי", "אלי", "ענד", "בין", "חתי", "עלא", "לדי", "אללה",
    "אן", "אנ", "מא", "לא", "לם", "לן", "ליס", "אלא", "ולא", "אמא", "הל", "אד", "אדא", "או", "ו", "פ", "תם",
    "קד", "כל", "גיר", "סוא", "דלך", "הדא", "הדה", "תלך", "הו", "הי", "הם", "הן", "אנא", "אנת", "נחן",
    "אנתם", "אלדי", "אלתי", "לאן", "לכן", "כאן", "כיף", "אין", "מתי", "חית", "כמא", "בעד", "קבל", "אלאן",
    "אם", "לו", "בל", "פקט", "הנא", "הנאך", "תחת", "פוק", "דון"))


def preposition_stems(word: str) -> list[str]:
    """The preposition a pronoun suffix was added to (בהא -> ב, עליך -> עלי), whatever its length."""
    return [word[:-len(s)] for s in PRONOUN_SUFFIXES if word.endswith(s) and word[:-len(s)] in PREPOSITIONS]


def is_function_word(keys: Sequence[str]) -> bool:
    """True when some key of a typed word is a function word, or a preposition with a suffix."""
    return any(k in FUNCTION_WORDS or preposition_stems(k) for k in keys)


def word_keys(word: str) -> tuple[str, ...]:
    """The keys one printed or typed word answers to: its own key first, then its clitic
    alternatives (``candidate_stems`` of ``MIN_EXPRESSION_STEM`` letters or more, a two-letter
    remainder of proclitics alone, the preposition under a pronoun suffix), no duplicates; ()
    for a word without Hebrew letters. A printed word that keys as several words (alternatives
    written with a slash, דלך/פעל) gives the keys of each; one with optional letters, (א)קצר,
    keys both ways."""
    spellings = [word] + ([OPTIONAL_LETTERS_RE.sub("", word)] if OPTIONAL_LETTERS_RE.search(word) else [])
    out: list[str] = []
    for spelling in spellings:
        for part in lookup_key(spelling).split():
            for candidate in [part, *candidate_stems(part)]:
                if candidate in out or not (candidate == part or len(candidate) >= MIN_EXPRESSION_STEM
                                            or short_remainder(part, candidate)):
                    continue
                out.append(candidate)
                for stem in preposition_stems(candidate):
                    if stem not in out:
                        out.append(stem)
    return tuple(out)


def scope_keys(text: str) -> list[tuple[int, str, int]]:
    """(position, key, peeled) for every key of every printed word of a quotation or a name:
    the position counts printed words, so a gap is printed words between; ``peeled`` is 0 for
    a word's own key and 1 for a clitic alternative. The builders write these rows and the
    readers query them, so both key one way."""
    out: list[tuple[int, str, int]] = []
    for pos, word in enumerate(text.split()):
        keys = word_keys(word)
        own = set(lookup_key(word).split())
        for key in keys:
            out.append((pos, key, 0 if key in own else 1))
    return out


def ordered_match(tokens: Any, query: Any, gap: int) -> Optional[tuple[int, ...]]:
    """The tightest ordered match of ``query`` in ``tokens`` with at most ``gap`` printed words
    between each consecutive pair: the indices of the matched tokens, or None. Of two matches
    the one spanning fewer printed words wins, wherever each stands (a contiguous occurrence
    after a gapped one is the match: Codex, round 5 of #487), and of equal spans the earlier,
    word by word: with the words repeated (a b b a, query a b a) the first b is the match, not
    the second (Codex, round 6), so the word marked is the earliest that fits.

    ``tokens``: keys in printed order, or (position, key) pairs when a printed word carries
    several keys (its alternatives), sorted by position. ``query``: one key per typed word, or
    a set of keys any of which matches. Two query words need two DISTINCT printed words, so two
    keys of one printed word never satisfy a repeated query word. For plain key lists the
    indices are the positions. No morphology, no sense: the same tokens can hold two senses.
    """
    if not query or not tokens:
        return None
    items = [(t if isinstance(t, tuple) else (i, t)) for i, t in enumerate(tokens)]
    accepts = [frozenset([q]) if isinstance(q, str) else frozenset(q) for q in query]
    # prefix length matched -> the paths (indices into items) still worth extending, all ending
    # before the printed position being read: no kept path starts later than another and ends
    # at least as late (that one is tighter and reaches as far), and at one printed position the
    # first key wins (a word's own key is written before its alternatives). Paths that share a
    # start are all kept until the window drops them: the one ending earlier gives the same
    # span and the earlier word. A printed position is read whole, every key of it, before its
    # new paths join and prune: a path restarting at the word must not take the earlier path
    # away from the word's other keys (Codex, round 7 of #487: בהא בהא, query בהא ב).
    states: dict[int, list[tuple[int, ...]]] = {}
    best: Optional[tuple[int, ...]] = None
    tightest = len(query) - 1   # the span of a contiguous occurrence: nothing beats it

    def where(match: tuple[int, ...]) -> tuple[int, ...]:
        return tuple(items[i][0] for i in match)

    def keep(paths: list[tuple[int, ...]], match: tuple[int, ...]) -> None:
        start, end = items[match[0]][0], items[match[-1]][0]
        here = where(match)
        for path in paths:
            p_start, p_end = items[path[0]][0], items[path[-1]][0]
            if where(path) == here or (p_start > start and p_end >= end):
                return
        paths[:] = [path for path in paths
                    if not (items[path[0]][0] < start and items[path[-1]][0] <= end)]
        paths.append(match)

    groups: list[list[int]] = []
    for i, (pos, _token) in enumerate(items):
        if groups and items[groups[-1][0]][0] == pos:
            groups[-1].append(i)
        else:
            groups.append([i])
    for group in groups:
        pos = items[group[0]][0]
        states = {n: [path for path in paths if pos - items[path[-1]][0] <= gap + 1] for n, paths in states.items()}
        fresh: dict[int, list[tuple[int, ...]]] = {}
        for i in group:
            token = items[i][1]
            for n in sorted(states, reverse=True):
                if token not in accepts[n]:
                    continue
                for path in states[n]:
                    if not 0 < pos - items[path[-1]][0] <= gap + 1:
                        continue
                    match = (*path, i)
                    if n + 1 == len(query):
                        if best is None or (span(items, match), where(match)) < (span(items, best), where(best)):
                            best = match
                            if span(items, best) == tightest:
                                return best
                    else:
                        keep(fresh.setdefault(n + 1, []), match)
            if token in accepts[0]:
                if len(query) == 1:
                    return (i,)
                keep(fresh.setdefault(1, []), (i,))
        for n, paths in fresh.items():
            for match in paths:
                keep(states.setdefault(n, []), match)
        states = {n: paths for n, paths in states.items() if paths}
    return best


def span(items: Sequence[tuple[int, Any]], match: Sequence[int]) -> int:
    """The printed words a match spans, from its first matched word to its last."""
    return items[match[-1]][0] - items[match[0]][0]


@dataclass(frozen=True)
class ExpressionComponent:
    """One typed word of an expression and the printed word that carried it in one place."""
    word: str           # as typed
    key: str            # its key
    printed: str        # the printed word that matched, as printed
    matched: str        # the key they share
    via: str            # "word" (the two keys are the same) or "stem" (a clitic alternative on either side)


@dataclass(frozen=True)
class ExpressionPlace:
    """One scope of a unit (a quotation, or a name) holding the whole expression."""
    scope: str                              # the dictionary's own scope names
    n: int                                  # the scope's number inside the unit (a quotation's, a name's)
    words: tuple[str, ...]                  # the printed words of the scope
    positions: tuple[int, ...]              # the matched printed words, in the expression's order
    components: tuple[ExpressionComponent, ...]
    quotation: Any = None                   # the dictionary's own quotation record (None for a name)
    #: The matched words' positions in the order the search read them, when that is not the
    #: printed order (a Friedman head's bracketed notes come last): the span and the gaps are
    #: the search's, the marks the page's (Codex, round 8 of #487). () when the orders agree.
    search_positions: tuple[int, ...] = ()

    @property
    def span(self) -> int:
        found = self.search_positions or self.positions
        return found[-1] - found[0] if found else 0

    @property
    def contiguous(self) -> bool:
        return self.span == len(self.search_positions or self.positions) - 1


def components_for(items: Sequence[tuple[int, str]], match: Sequence[int], words: Sequence[str],
                   candidates: Sequence[Sequence[str]], printed: Sequence[str]
                   ) -> Optional[tuple[tuple[int, ...], tuple[ExpressionComponent, ...]]]:
    """The matched printed positions and, per typed word, which printed word carried it and how;
    None when the token rows and the text disagree (not this reader's sidecar)."""
    positions = tuple(items[i][0] for i in match)
    if any(p >= len(printed) for p in positions):
        return None
    components = []
    for word, keys, i in zip(words, candidates, match):
        pos, matched = items[i]
        via = "word" if matched == keys[0] and matched in lookup_key(printed[pos]).split() else "stem"
        components.append(ExpressionComponent(word, keys[0], printed[pos], matched, via))
    return positions, tuple(components)


def tiered(places: Mapping[U, Sequence[ExpressionPlace]], exact_keys: Mapping[U, Collection[str]],
           heads_in_order: Collection[U], parents: Mapping[U, Optional[U]], candidates: Sequence[Sequence[str]],
           order: Mapping[U, Any], head_scopes: Collection[str]) -> list[tuple[U, list[ExpressionPlace]]]:
    """The units with their places, ranked: first the unit whose own name IS the expression
    (a place in a ``head_scopes`` scope whose matched keys are one of the unit's ``exact_keys``),
    then the units in ``heads_in_order`` (their name, or their parent's, holds the typed words in
    order within the widest gap, whatever gap the reader chose: a name with the words reversed or
    scattered says nothing about the expression), then the rest; inside a tier by the best place
    (a name before a quotation, contiguous before gapped, the narrower span, the earlier), then
    the dictionary's ``order``. Each unit's places come sorted the same way. An expression of
    function words only is shown in the third tier only where its words stand together."""

    def tier(unit: U, found: Sequence[ExpressionPlace]) -> int:
        # The first tier is the name that IS the expression (one of the unit's keys), not a
        # longer name that merely contains its words: ראובן כתב כטה is not the entry כתב כטה.
        if any(p.scope in head_scopes and " ".join(c.matched for c in p.components) in exact_keys.get(unit, ())
               for p in found):
            return 1
        return 2 if unit in heads_in_order or parents.get(unit) in heads_in_order else 3

    def rank(place: ExpressionPlace) -> tuple:
        return (0 if place.scope in head_scopes else 1, 0 if place.contiguous else 1, place.span, place.n)

    kept: dict[U, list[ExpressionPlace]] = {u: list(f) for u, f in places.items()}
    if all(is_function_word(c) for c in candidates):
        for unit in list(kept):
            if tier(unit, kept[unit]) == 3:
                kept[unit] = [p for p in kept[unit] if p.contiguous]
                if not kept[unit]:
                    del kept[unit]
    for found in kept.values():
        found.sort(key=rank)
    return sorted(kept.items(), key=lambda kv: (tier(kv[0], kv[1]), *rank(kv[1][0]), order.get(kv[0], 0)))

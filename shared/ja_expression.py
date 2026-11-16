# -*- coding: utf-8 -*-
"""The expression search the Judeo-Arabic dictionaries share: keys, the matcher, the tiers.

Several words selected on /browse or typed in the dictionary panel's box are an EXPRESSION, and a
dictionary answers with every scope of its own text (an entry's name, a quotation) holding them in the selected
order with at most ``gap`` printed words between each consecutive pair. Friedman's dictionary had
this first (``shared/friedman_service.py``, PR #487); Blau's got it next (``shared/blau_service.py``)
and the two key, match and rank one way, here, so a sidecar built by one rule set is read by the
same. What a dictionary adds is its own: which scopes it has, what a unit is, how a hit is shown.

Keys (``word_keys``, ``scope_keys``): a printed or typed word answers to its own key
(``normalize_ja`` after the harakat a PDF stores as presentation forms are dropped) and to its
clitic alternatives -- ``lookup_stems`` of ``MIN_EXPRESSION_STEM`` letters or more, a two-letter
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
from functools import lru_cache
from typing import Any, Collection, Hashable, Mapping, Optional, Sequence, TypeVar

from shared.blau_normalize import PRONOUN_SUFFIXES, interchange_variants, lookup_stems, normalize_ja, lexical_key, nominal_candidates, spelling_key, query_stems

#: The rules below, as a number a sidecar's ``meta.keying_version`` carries: bump it when
#: ``word_keys`` or ``scope_keys`` changes what they write, and rebuild every sidecar's tokens.
KEYING_VERSION = 7  # 6 (2026-11-13): a word carries its verb root whatever the kind of the hit (ALL_VERB_KINDS); 7 (2026-11-14): verb forms version 3 (four-letter roots, the verbal nouns of V and VI)

#: The expression search: words per expression, printed words allowed between two of them (the
#: default and the most a reader may ask).
MAX_EXPRESSION_WORDS = 12
MAX_GAP = 20
DEFAULT_GAP = 2

U = TypeVar("U", bound=Hashable)


def clamp_gap(gap: Any) -> int:
    """A gap a reader may ask for: an integer in 0..MAX_GAP, the default for anything else."""
    try:
        value = int(gap)
    except (TypeError, ValueError):
        return DEFAULT_GAP
    return min(max(value, 0), MAX_GAP)


@lru_cache(maxsize=32768)
def lookup_key(word: str) -> str:
    """The normalizer's key, after the harakat a PDF stores as presentation forms are dropped."""
    plain = "".join(ch for ch in word if not ("ﱞ" <= ch <= "ﱣ" or "ﹰ" <= ch <= "ﹿ")
                    and not unicodedata.category(ch).startswith("M"))
    return lexical_key(plain)


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


#: The function words the pronoun ני joins (אנני, לאנני, לכנני); under any other it is no pronoun.
TAKES_NI = frozenset(("אנ", "לאנ", "לכנ", "כאנ"))


def function_reading(key: str, candidate: str) -> bool:
    """Is ``candidate``, a key of the word ``key``, a function word the word may be read as: the
    word itself, or what is left of it under a conjunction, a preposition or a pronoun? Not under
    the pronoun ני unless the function word takes it (מעני is no מע, אלדיני no אלדי)."""
    if candidate not in FUNCTION_WORDS:
        return False
    return not (key.endswith(candidate + 'ני') and candidate not in TAKES_NI)


def preposition_stems(word: str) -> list[str]:
    """The preposition a pronoun suffix was added to (בהא -> ב, עליך -> עלי), whatever its length."""
    return [word[:-len(s)] for s in PRONOUN_SUFFIXES if word.endswith(s) and word[:-len(s)] in PREPOSITIONS]


def is_function_word(keys: Sequence[str]) -> bool:
    """True when some key of a typed word is a function word, or a preposition with a suffix."""
    return any(k in FUNCTION_WORDS or preposition_stems(k) for k in keys)


@lru_cache(maxsize=32768)
def word_keys(word: str) -> tuple[str, ...]:
    """The keys one printed or typed word answers to: its own key first, then its clitic
    alternatives (``lookup_stems`` of ``MIN_EXPRESSION_STEM`` letters or more, a two-letter
    remainder of proclitics alone, the preposition under a pronoun suffix), no duplicates; ()
    for a word without Hebrew letters. A printed word that keys as several words (alternatives
    written with a slash, דלך/פעל) gives the keys of each; one with optional letters, (א)קצר,
    keys both ways."""
    spellings = [word] + ([OPTIONAL_LETTERS_RE.sub("", word)] if OPTIONAL_LETTERS_RE.search(word) else [])
    out: list[str] = []
    for spelling in spellings:
        for part in lookup_key(spelling).split():
            for candidate in [part, *lookup_stems(part)]:
                if candidate in out or not (candidate == part or len(candidate) >= MIN_EXPRESSION_STEM
                                            or short_remainder(part, candidate)):
                    continue
                out.append(candidate)
                for stem in preposition_stems(candidate):
                    if stem not in out:
                        out.append(stem)
    return tuple(out)


#: The kinds of verb-form hit (``shared.ja_verb_forms.hit_kind``) whose root a word carries as an
#: expression key. ``ALL_VERB_KINDS`` adds the bare perfect of three letters (פרח in "פרח בה") and a
#: form under a clitic (ויבני, וטלבת): an expression's other words decide whether the verb is the
#: one meant. Sidecars and queries key with every kind since ``KEYING_VERSION`` 6 (measured on both
#: dictionaries, 2026-11-13); the narrower set is what keying 5 wrote.
EXPRESSION_VERB_KINDS = frozenset("ABD")
ALL_VERB_KINDS = frozenset("ABCDE")


def query_keys(word: str, verb_index=None, kinds: Collection[str] = ALL_VERB_KINDS) -> tuple[str, ...]:
    """``word_keys`` of a word the reader typed, selected or clicked, then the stems its article gives
    when it lost its alif after a preposition (בלסלאם -> סלאמ: ``elided_article_stems``),
    so "יכצך בלסלאם" finds what "יכצך באלסלאם" finds. Only the reader's words: the
    sidecars key the dictionaries' words by ``word_keys`` (``KEYING_VERSION`` unchanged)."""
    original = lookup_stems(lookup_key(word))
    keys = tuple(dict.fromkeys((*word_keys(word), *(k for k in query_stems(lookup_key(word)) if k not in original))))
    extra = []
    if verb_index and not is_function_word(keys):
        from shared.ja_verb_forms import hit_kind, nominal_forms
        nominal = nominal_candidates(lookup_key(word))
        for i, key in enumerate(keys):
            for pair in verb_index.get(key, ()):
                if hit_kind(pair.stem, i == 0, len(key)) not in kinds:
                    continue
                if key in nominal and key not in nominal_forms(pair.root_key, pair.stem):
                    continue
                extra.append(f"@{pair.root_key}:{pair.stem}")
    if lookup_key(word) == "יא":
        extra.append("^יא")
    return (*keys, *dict.fromkeys("~" + spelling_key(k) for k in keys), *dict.fromkeys(extra))


def interchange_candidates(words: Sequence[str], candidates: Sequence[tuple[str, ...]],
                            keys_of) -> Optional[tuple[tuple[str, ...], ...]]:
    """Each word's keys with the keys of its spellings with צ and ט exchanged
    (``interchange_variants``; ``keys_of`` is the reader's ``query_keys``), so an expression
    written with one letter reaches a dictionary that prints the other. None when no word has
    such a spelling with new keys: the expression search runs this pass only after its own."""
    out: list[tuple[str, ...]] = []
    changed = False
    for word, keys in zip(words, candidates):
        extra: list[str] = []
        for variant in interchange_variants(lookup_key(word)):
            extra.extend(k for k in keys_of(variant) if k not in keys and k not in extra)
        changed = changed or bool(extra)
        out.append((*keys, *extra))
    return tuple(out) if changed else None


def scope_keys(text: str, verb_index=None, kinds: Collection[str] = ALL_VERB_KINDS) -> list[tuple[int, str, int]]:
    """(position, key, peeled) for every key of every printed word of a quotation or a name:
    the position counts printed words, so a gap is printed words between; ``peeled`` is 0 for
    a word's own key and 1 for a clitic alternative. The builders write these rows and the
    readers query them, so both key one way."""
    out: list[tuple[int, str, int]] = []
    for pos, word in enumerate(text.split()):
        keys = query_keys(word, verb_index, kinds)
        own = set(lookup_key(word).split())
        for key in keys:
            out.append((pos, key, 0 if key in own else 1))
        if vocative_parts(word):
            out.append((pos, "^יא", 1))
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


class ExpressionLimitError(RuntimeError):
    """A bounded component search ran out of work; never an absence result."""


def unordered_match(tokens: Any, query: Any, gap: int) -> Optional[tuple[int, ...]]:
    """Tightest assignment in any order, using each printed position only once.

    Consecutive selected positions obey the same gap as ordered search. The
    bitmask tracks query components, so aliases of a token cannot satisfy repeats.
    """
    items = [(t if isinstance(t, tuple) else (i, t)) for i, t in enumerate(tokens)]
    accepts = [frozenset([q]) if isinstance(q, str) else frozenset(q) for q in query]
    if not accepts or not items:
        return None
    full = (1 << len(accepts)) - 1
    states = {}
    best = None
    work = 0
    groups = {}
    for i, (pos, key) in enumerate(items):
        groups.setdefault(pos, []).append(i)

    def positions(path):
        return tuple(items[i][0] for i in path if i >= 0)

    def preference(path):
        pp = positions(path)
        return (-min(pp), pp)

    for pos, indices in sorted(groups.items()):
        states = {(mask, end): path for (mask, end), path in states.items() if pos - end <= gap + 1}
        fresh = {}
        for i in indices:
            for q, allowed in enumerate(accepts):
                if items[i][1] not in allowed:
                    continue
                bit = 1 << q
                for (mask, _end), path in [( (0, pos), (-1,) * len(accepts)), *states.items()]:
                    work += 1
                    if work > 2_000_000:
                        raise ExpressionLimitError()
                    if mask & bit:
                        continue
                    changed = list(path)
                    changed[q] = i
                    changed = tuple(changed)
                    key = (mask | bit, pos)
                    if key[0] == full:
                        pp = positions(changed)
                        score = (max(pp) - min(pp), pp)
                        if best is None or score < (max(positions(best)) - min(positions(best)), positions(best)):
                            best = changed
                    elif key not in fresh or preference(changed) < preference(fresh[key]):
                        fresh[key] = changed
        states.update(fresh)
    return best


def vocative_parts(word):
    key = lookup_key(word)
    if key.startswith("ויא"):
        key = key[1:]
    return ("יא", key[2:]) if key.startswith("יא") and len(key) >= 5 else ()


def expression_words(text):
    """Separate an attached vocative for component discovery; keep text for display."""
    return tuple(part for word in text.split() for part in (vocative_parts(word) or (word,)))


def match_scope(items, accepts, gap, ordered, build, keep=None):
    """The place ``build(match)`` makes of the best match of ``accepts`` in one scope's ``items``,
    or None. ``keep(place)``: None to keep it, else (position, key, word) tokens refused to that
    typed word alone; the scope is then matched again with each refused token renamed so that only
    the other words still accept it, until a place is kept or nothing new is refused. ``build``
    always receives indices into the scope's own ``items``."""
    refused: dict[tuple[int, str], frozenset[int]] = {}
    while True:
        if refused:
            def tag(pos, key):
                barred = refused.get((pos, key))
                return f"{key}\x00{','.join(map(str, sorted(barred)))}" if barred else key
            tokens = [(pos, tag(pos, key)) for pos, key in items]
            query = [frozenset(a) | {tag(pos, key) for (pos, key), barred in refused.items()
                                     if key in a and n not in barred}
                     for n, a in enumerate(accepts)]
        else:
            tokens, query = items, accepts
        match = expression_match(tokens, query, gap, ordered)
        if match is None:
            return None
        place = build(match)
        if place is None or keep is None:
            return place
        refusal = keep(place)
        if refusal is None:
            return place
        grew = False
        for pos, key, n in refusal:
            barred = refused.get((pos, key), frozenset())
            if n not in barred:
                refused[(pos, key)] = barred | {n}
                grew = True
        if not grew:
            return None


def expression_match(tokens, query, gap, ordered=True):
    # Only an explicitly segmented vocative can supply two components at one
    # written position. Ordinary spelling alternatives still share one position.
    needs_vocative = any("^יא" in q for q in query)
    split = {pos for pos, key in tokens if key == "^יא"} if needs_vocative else set()
    positioned = sorted((pos + (0 if key == "^יא" else .25) if pos in split else pos, i, key)
                        for i, (pos, key) in enumerate(tokens))
    items = [(pos, key) for pos, _i, key in positioned]
    found = ordered_match(items, query, gap) if ordered else unordered_match(items, query, gap)
    return tuple(positioned[i][1] for i in found) if found is not None else None


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
    steps: tuple[str, ...] = ()


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
        return max(found) - min(found) if found else 0

    @property
    def contiguous(self) -> bool:
        return self.span == len(set(self.search_positions or self.positions)) - 1


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
        via = ("inflection" if matched.startswith("@") else "spelling" if matched.startswith("~") else
               "word" if matched == keys[0] and matched in lookup_key(printed[pos]).split() else "stem")
        components.append(ExpressionComponent(word, keys[0], printed[pos], matched, via,
                                              () if via == "word" else (via,)))
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
        return 2 if (unit in heads_in_order or parents.get(unit) in heads_in_order
                     or any(p.scope == "construction" for p in found)) else 3

    def rank(place: ExpressionPlace) -> tuple:
        inferred = sum(c.via == "inflection" for c in place.components)
        return (0 if place.scope in head_scopes else 1, 0 if place.contiguous else 1, inferred, place.span, place.n)

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


# ---------------------------------------------------------------------------
# The words beside a clicked word
# ---------------------------------------------------------------------------

#: A clicked word is looked up with the words beside it in the text as well: the expressions of
#: this many words that hold it, the longest first. The first size that names a unit is the answer.
CONTEXT_SIZES = (3, 2)
#: What a page may send of the words beside a clicked word, on each side.
MAX_CONTEXT_WORDS = 8


def context_windows(word: str, before: Sequence[str] = (), after: Sequence[str] = (),
                    sizes: Sequence[int] = CONTEXT_SIZES, function_runs: bool = False) -> list[list[str]]:
    """The expressions to try for a word clicked between ``before`` and ``after`` (the words beside
    it in the text, nearest last and first): for each size in ``sizes``, every run of that many
    consecutive words that holds the clicked one, left to right, other than a run of function
    words only ("עלי כל": every dictionary prints it in a hundred places, and each is slow to
    search). [] for no word beside it. ``function_runs``: those runs alone, for the units named
    by exactly such words (אלדי הו), tried when the text gave nothing else."""
    word = " ".join(str(word or "").split())
    if not word or " " in word:
        return []
    widest = max(sizes, default=1) - 1
    left = [w for w in (" ".join(str(b or "").split()) for b in before) if w and " " not in w][-widest:] if widest else []
    right = [w for w in (" ".join(str(a or "").split()) for a in after) if w and " " not in w][:widest]
    words = [*left, word, *right]
    function = [is_function_word(word_keys(w)) for w in words]
    at = len(left)
    out = []
    for size in sizes:
        runs = [" ".join(words[start:start + size])
                for start in range(max(0, at - size + 1), min(at, len(words) - size) + 1)
                if all(function[start:start + size]) == function_runs]
        if runs:
            out.append(runs)
    return out


#: When no unit names a run of ``context_windows`` word for word, the units that hold one with words
#: between, in a name or in a quotation, show instead (ואדאם in "ואדאם סלאמתך"
#: reached nothing, and selecting both words found Friedman's "אדאם אללה סלאמתה"): at most this many
#: units, the longest run first.
MAX_CONTEXT_QUOTED = 3


#: The words of a person's name, which no quoted run holds: "אבו אלפרג", "יוסף בן" are quoted in a dozen
#: entries that have nothing to do with the clicked word.
NAME_WORDS = frozenset(normalize_ja(w) for w in ("בן", "אבן", "בת", "אבו", "אבי", "אבא"))


def quoted_runs(windows: Sequence[Sequence[str]]) -> list[str]:
    """The runs of ``context_windows`` (``windows``) searched with words between, longest first: those
    with no function word and no word of a name (``NAME_WORDS``), which put two words of the text side
    by side in a quotation by chance ("פי אלבר", "ואן יבלגך", "מנהא ואחד" each matched a dozen).
    Measured on four letters, 1,570 clicked words (2026-11-12): any run, a gap of 1, reached
    quotations for 48% of the words in Friedman, about 8 units each; these runs, a gap of 2, 14%."""
    def plain(word: str) -> bool:
        keys = word_keys(word)
        return not is_function_word(keys) and not any(key in NAME_WORDS for key in keys)
    return [run for runs in windows for run in runs if all(plain(word) for word in run.split())]


def governed_verb_run(word: str, after: Sequence[str], verb_index) -> str:
    """A finite verb followed immediately by a preposition with a pronoun (נקף מעה)."""
    if not after or not preposition_stems(lookup_key(after[0])):
        return ''
    from shared.ja_verb_forms import verb_form_cards
    key = lookup_key(word)
    if not is_function_word((key,)) and any(card.finite_for(key) for card in
            verb_form_cards(query_stems(key), verb_index, nominal_candidates(key))):
        return f'{word} {after[0]}'
    return ''


#: How far from a word its preposition may stand in the text for the two to be tried as a governed
#: pair (אלשוק אלעט̇ים אליך: one word between).
GOVERNED_REACH = 3


def _is_preposition(word: str) -> bool:
    key = lookup_key(word)
    return key in PREPOSITIONS or bool(preposition_stems(key))


def governed_runs(word: str, before: Sequence[str], after: Sequence[str]) -> list[str]:
    """A clicked word with the preposition it governs, when other words stand between them in the
    text: "אלשוק אלעט̇ים אליך" is the dictionary's "שוק אליה", "תג̇תמע קלוב אלנאס עליה" its "... עלי".
    A lexical word gives the pair with the first preposition within ``GOVERNED_REACH`` words after
    it; a clicked preposition, the pair with the nearest lexical word that far before it. A pair of
    neighbours is ``context_windows``' already and is left out. [] for a name's word."""
    keys = word_keys(word)
    if not keys or any(k in NAME_WORDS for k in keys):
        return []
    before, after = list(before), list(after)
    def lexical(value: str) -> bool:
        found = word_keys(value)
        return bool(found) and not is_function_word(found) and not any(k in NAME_WORDS for k in found)
    if _is_preposition(word):
        for distance, value in enumerate(reversed(before[-GOVERNED_REACH:])):
            if lexical(value):
                at = len(before) - 1 - distance
                named = _beside_name(before[at - 1] if at else "", before[at + 1] if at + 1 < len(before) else word)
                return [f"{value} {word}"] if distance and not named else []
            if _is_preposition(value):
                break
        return []
    if is_function_word(keys) or _beside_name(before[-1] if before else "", after[0] if after else ""):
        return []
    for distance, value in enumerate(after[:GOVERNED_REACH]):
        if _is_preposition(value):
            return [f"{word} {value}"] if distance else []
    return []


def _beside_name(previous: str, following: str) -> bool:
    """A word of a person's name stands beside the word (אבו אלחסן, עאדי בן): its preposition is
    then a coincidence of a list of names."""
    return any(k in NAME_WORDS for value in (previous, following) for k in word_keys(value))


def verb_keys(word: str, verb_index, kinds: Collection[str] = ALL_VERB_KINDS) -> tuple[str, ...]:
    """The verb roots a FINITE verb is a form of, as its expression keys (``query_keys``'
    "@root:stem"): what a governing word answers to when its preposition stands apart from it. Only
    a perfect or an imperfect of four letters or more, its clitics peeled: a word with the article,
    a participle or a verbal noun (מנזלתנא, נצרהא) and a bare three-letter word governed a chance
    preposition in the measurement of 2026-11-13. ``kinds`` as for ``query_keys``."""
    from shared.blau_normalize import article_candidates
    from shared.ja_verb_forms import hit_kind, nominal_forms
    key = lookup_key(word)
    if not key or " " in key or is_function_word((key,)) or article_candidates(key) or key in NAME_WORDS:
        return ()
    out: list[str] = []
    for n, candidate in enumerate(dict.fromkeys((key, *query_stems(key)))):
        if len(candidate) < 4:
            continue
        for pair in (verb_index or {}).get(candidate, ()):
            if hit_kind(pair.stem, n == 0, len(candidate)) in kinds and candidate not in nominal_forms(pair.root_key, pair.stem):
                out.append(f"@{pair.root_key}:{pair.stem}")
    return tuple(dict.fromkeys(out))


#: The words a head prints for a slot the text fills with its own (פלאן, יפעל, כד̇א): never a word
#: the text must hold.
HEAD_PLACEHOLDERS = frozenset(normalize_ja(w) for w in (
    "פלאן", "פלאנא", "פלאנה", "שי", "אלשי", "שיא", "אלשיא", "פעל", "יפעל", "יפעלו", "תפעל", "אפעל", "נפעל",
    "פעלא", "אלפעל", "כדא", "מכאן", "אלמכאן"))
_OPTIONAL_LETTERS_WORD_RE = re.compile(r"^\([א-ת]{1,2}\)\S")


def _in_order(letters: str, word: str) -> bool:
    found = iter(word)
    return all(letter in found for letter in letters)


def lexical_positions(words: Sequence[str], root: str = "") -> tuple[int, ...]:
    """The positions of a printed head's (or construction's) lexical words: those a text must hold
    for the head to be the expression read there. "נצף מן אלשהר" is נצף and אלשהר; "דכל פיהא" is
    דכל. Left out:

    - the words of a bracketed note ("(ולא רק פי)", "(לה)") and everything up to a colon, the label
      the expression is filed under (נץ: אלנץ, "גמב (<גנב): מן גמב");
    - the entry's ``root`` opening the head when the next word is a form of it (מחן אמתחן בהא);
    - the plural after ר'; an object mark (-ה, -הא); a placeholder (``HEAD_PLACEHOLDERS``); a
      function word or a preposition with its pronoun; anything without a Hebrew letter;
    - the words after a word of alternatives that is not the head's first (ענד אללה/בין ידי אללה,
      מעה/צחבתה/עלי ידה): they belong to the last alternative alone."""
    label = max((i for i, word in enumerate(words) if word.rstrip(")]").endswith(":")), default=-1)
    out: list[int] = []
    depth = 0
    for i, word in enumerate(words):
        note = depth > 0 or (word.startswith(("(", "[")) and not _OPTIONAL_LETTERS_WORD_RE.match(word))
        depth = max(0, depth + word.count("(") + word.count("[") - word.count(")") - word.count("]"))
        if word in ("ר'", "ר׳"):
            break
        if note or i <= label or word[:1] in "-–­=<>+":
            continue
        # of alternatives joined by a slash, one lexical alternative makes the word lexical
        parts = [word_keys(part) for part in word.split("/") if part]
        filler = [not keys or is_function_word(keys) or any(k in HEAD_PLACEHOLDERS for k in keys) for keys in parts]
        if not all(filler):
            out.append(i)
        if "/" in word and i > label + 1 and any(parts):
            break
    if root and len(out) > 1 and out[0] == label + 1:
        first, second = lookup_key(words[out[0]]), lookup_key(words[out[1]])
        if first == lookup_key(root) and first != second and _in_order(first, second):
            out = out[1:]
    return tuple(out)


def names_expression(place: "ExpressionPlace", around: Collection[str] = (), root: str = "", keys=word_keys) -> bool:
    """Do the words matched in a head (``place``) make the head the expression of the text: each
    lexical word of it (``lexical_positions``) is one of the matched words, or stands in the text
    around the click (``around``: the keys of those words; ``keys`` gives a head word's). Two words
    of the text are in a hundred longer heads by chance ("נצף מן" in "נצף מן אלשהר", "דכל פי" in
    "דכל פי חסאב") with no שהר and no חסאב anywhere near."""
    lexical = lexical_positions(place.words, root)
    exact = {position for position, part in zip(place.positions, place.components) if not part.matched.startswith("~")}
    # A word of one letter is an abbreviation or a numeral in a text (ב', ל), never a preposition.
    if any(len(lookup_key(part.word)) < 2 for part in place.components):
        return False
    if not lexical:
        # a head of function words and slots alone (אלי מא יפעל) is named by all its words or none
        return len(set(place.positions)) >= sum(1 for word in place.words if word_keys(word)) > 1
    # One lexical word reached only through a spelling fold (אבקאה for בקוה̈) names nothing.
    held = exact if len(lexical) == 1 else set(place.positions)
    around = set(around)
    def near(word: str) -> bool:
        # the head's accusative (כ̇ירא) is the text's כ̇יר as well
        found = [k for k in keys(word) if not k.startswith("~")]
        return not around.isdisjoint(found) or not around.isdisjoint(
            k[:-1] for k in found if len(k) > 3 and k.endswith("א") and not k.startswith("@"))
    return all(i in held or near(place.words[i]) for i in lexical) and any(i in held for i in lexical)


def fuller_first(hits: Sequence[Any], after: Sequence[str], family=lambda hit: None) -> tuple[Any, ...]:
    """``hits`` (each with ``places``) with the sub-entries of one entry (``family`` gives the
    entry; None: a hit of no family) in the order the text supports: of "עליה" and "עליה בהא" under
    one verb, the second first when a word after the click carries its ב ("ואסתט̇הר עליה בכתב
    אביה"), the first first when none does. A heading's word the run did not match counts against
    it unless one of the next ``SUPPORT_REACH`` words has one of its keys or begins with its
    preposition; a placeholder (פעל), a bracketed note and a word without letters do not count.
    Only hits of one family change places, and only among the places they held."""
    near = list(after)[:SUPPORT_REACH]
    keys = {key for word in near for key in word_keys(word)}
    starts = [lookup_key(word).lstrip("ופ") for word in near]

    def supported(word: str) -> bool:
        found = [k for k in word_keys(word) if not k.startswith(("~", "@"))]
        if not keys.isdisjoint(found):
            return True
        preps = {p for k in found for p in preposition_stems(k)}
        return any(len(start) > len(p) + 2 and start.startswith(p) for p in preps for start in starts)

    def counted(word: str) -> bool:
        found = word_keys(word)
        return bool(found) and not word.startswith(("(", "[", "<")) and not any(k in HEAD_PLACEHOLDERS for k in found)

    def rank(hit) -> tuple[int, int]:
        ranks = []
        for place in hit.places:
            extra = [w for i, w in enumerate(place.words) if i not in place.positions and counted(w)]
            held = sum(1 for w in extra if supported(w))
            ranks.append((len(extra) - held, -held))
        return min(ranks, default=(0, 0))

    slots: dict[Any, list[int]] = {}
    for at, hit in enumerate(hits):
        group = family(hit)
        if group is not None:
            slots.setdefault(group, []).append(at)
    out = list(hits)
    for places in slots.values():
        if len(places) > 1:
            for at, hit in zip(places, sorted((hits[i] for i in places), key=rank)):
                out[at] = hit
    return tuple(out)


#: How many words after a clicked word may support a heading's further word (``fuller_first``).
SUPPORT_REACH = 3
#: The prepositions written onto the next word, and what may stand before them on it.
CLITIC_PREPOSITIONS = ("ב", "ל")
_CLITIC_RE = re.compile(r"^[ופ]?(?:(ב)אל|(ל)(?:אל|ל))(.{3,})$")


def clitic_preposition(word: str) -> str:
    """The preposition written onto a word with the article (באלגאולה, ללשיך: ב, ל), '' when it has
    none: a function word, a preposition's own pronoun form (בהא: ``preposition_stems`` reads that),
    or fewer than three letters after the article. Without the article the letter is as often the
    word's own (בקאר, לוים: measured on 100 documents, 2026-11-13), so that is not read."""
    key = lookup_key(word)
    if " " in key or is_function_word((key,)):
        return ""
    found = _CLITIC_RE.match(key)
    return (found.group(1) or found.group(2)) if found else ""


def clitic_governed(word: str, before: Sequence[str], after: Sequence[str]) -> list[tuple[str, str, str]]:
    """A clicked word with the preposition it governs when the preposition is written onto a later
    word: "יטמע באלגאולה" is the dictionary's "טמע בהא", "אלקיאם באלמבלג" its "קום בהא". Each is
    (the governing word, the word carrying the preposition, the preposition): for a lexical word,
    the first word within ``GOVERNED_REACH`` after it that carries one, unless a preposition of
    its own stands first; for a clicked word that carries one, the nearest lexical word before it."""
    def lexical(value: str) -> bool:
        found = word_keys(value)
        return bool(found) and not is_function_word(found) and not any(k in NAME_WORDS for k in found)
    out: list[tuple[str, str, str]] = []
    before, after = list(before), list(after)
    if lexical(word) and not _beside_name(before[-1] if before else "", ""):
        for value in after[:GOVERNED_REACH]:
            if _is_preposition(value):
                break
            prep = clitic_preposition(value)
            if prep:
                out.append((word, value, prep))
                break
    prep = clitic_preposition(word)
    if prep:
        for value in reversed(list(before)[-GOVERNED_REACH:]):
            if _is_preposition(value):
                break
            if lexical(value):
                out.append((value, word, prep))
                break
    return out


def quoted_gap(gap: int) -> int:
    """The words a quotation may hold between the words of a run: the selection's ``gap``, at most
    ``DEFAULT_GAP`` (a gap widened for one search would match every click's neighbours far apart)."""
    return min(clamp_gap(gap), DEFAULT_GAP)


def component_run(text: str) -> str:
    """A bounded secondary query of lexical components; the full query stays visible."""
    words = text.split()
    content = [w for w in words if not is_function_word(lookup_stems(lookup_key(w)))
               and not any(k in NAME_WORDS for k in word_keys(w))]
    return " ".join(content) if 2 <= len(content) < len(words) <= MAX_EXPRESSION_WORDS else ""


def component_runs(text: str) -> tuple[str, ...]:
    """Bounded partial phrases; returned separately from the full-query answer."""
    words = text.split()
    if not 3 <= len(words) <= MAX_EXPRESSION_WORDS:
        return ()
    content = [w for w in words if not is_function_word(lookup_stems(lookup_key(w)))
               and not any(k in NAME_WORDS for k in word_keys(w))]
    runs = [component_run(text)]
    for size in (3, 2):
        runs.extend(' '.join(content[i:i + size]) for i in range(len(content) - size + 1))
    return tuple(dict.fromkeys(r for r in runs if r and r != text))[:12]


def context_component_runs(word: str, before: Sequence[str], after: Sequence[str]) -> list[str]:
    """Nearest lexical neighbours across intervening function words, anchored on the click."""
    if is_function_word(lookup_stems(lookup_key(word))):
        return []
    def neighbours(values):
        out = []
        for value in values:
            keys = word_keys(value)
            if any(k in NAME_WORDS for k in keys):
                break
            if not is_function_word(lookup_stems(lookup_key(value))):
                out.append(value)
                if len(out) == 2:
                    break
        return out
    left = list(reversed(neighbours(reversed(before[-MAX_CONTEXT_WORDS:]))))
    right = neighbours(after[:MAX_CONTEXT_WORDS])
    return quoted_runs(context_windows(word, left, right))


def context_quality(result: Any) -> tuple:
    """Prefer named constructions and tight source phrases over incidental co-occurrence."""
    def score(place):
        positions = set(place.positions)
        holes = max(positions) - min(positions) + 1 - len(positions) if positions else 0
        scope = 0 if place.scope not in ("quotation", "arabic_quotation", "citation") else 1
        return scope, -len(place.components), holes, sum(c.via != "word" for c in place.components)
    return min((score(p) for h in result.hits for p in h.places), default=(2, 0, 0, 0))


def matching_scopes(conn, candidates, columns, head_only=False):
    """Intersect component scopes in SQLite before materializing their token rows."""
    parts, args = [], []
    restriction = " AND scope NOT IN ('quotation','arabic_quotation','citation')" if head_only else ''
    for keys in candidates:
        keys = sorted(set(keys))
        if not keys:
            return []
        parts.append(f"SELECT {columns} FROM tokens WHERE key IN ({','.join('?' for _ in keys)}){restriction}")
        args.extend(keys)
    return conn.execute(' INTERSECT '.join(parts), args).fetchall() if parts else []

# -*- coding: utf-8 -*-
"""The Arabic layer's method: rank a Judeo-Arabic word's Arabic readings by the word count, and
find the root of a reading. One copy, used by the served lookup (``shared/arabic_service.py``)
and by the gate that measures it (``scripts/measure_arabic_readings.py``), so the numbers in
docs/plans/ARABIC_DICTIONARY_LAYER_PLAN.md (sections 4-6, 7.1) are the numbers of what is served.

Methods (plan section 4): v1 strips affixes; v2 adds the silent plural alif and, when no reading
is attested whole, ranks by the stem; v3 finds the root through Lane's headwords first; v4 (the
served method) answers a closed-class word (prepositions with pronouns, demonstratives,
particles) from the closed-class table first. Moved verbatim from the measuring script
(2026-11-04): the gate's numbers on the built file are unchanged.

Stdlib only; no I/O.
"""
from __future__ import annotations

import math
from collections import Counter
from typing import Optional

from shared.arabic_normalize import DOTTED, ja_letters, readings, root_key, strip_harakat

#: A reading is offered only when it occurs at least this often in the sample.
MIN_COUNT = 2
RANKINGS = ("frequency", "plain", "dotted", "reversed")


#: Method v2's backoff: the clitics peeled from an unattested reading before its
#: stem is counted instead (\u0648\u062a\u0646\u0641\u0630\u0647 -> \u062a\u0646\u0641\u0630).
BACKOFF_PROCLITICS = ("", "\u0648\u0628\u0627\u0644", "\u0641\u0628\u0627\u0644", "\u0648\u0627\u0644", "\u0641\u0627\u0644", "\u0628\u0627\u0644", "\u0644\u0644", "\u0627\u0644", "\u0648\u0628", "\u0641\u0628", "\u0648\u0644", "\u0641\u0644", "\u0648", "\u0641", "\u0628",
                      "\u0644", "\u0643")
BACKOFF_ENCLITICS = ("", "\u0643\u0645\u0627", "\u0647\u0645\u0627", "\u0647\u0627", "\u0647\u0645", "\u0647\u0646", "\u0643\u0645", "\u0643\u0646", "\u0646\u0627", "\u0646\u064a", "\u0647", "\u0643", "\u064a")


def backoff_count(reading: str, counts: Counter) -> int:
    """The count of the commonest stem left when a proclitic and a pronoun are
    peeled from the reading (at least three letters left), 0 when none is counted."""
    best = 0
    for pro in BACKOFF_PROCLITICS:
        if not reading.startswith(pro):
            continue
        for enc in BACKOFF_ENCLITICS:
            if (pro or enc) and reading.endswith(enc):
                stem = reading[len(pro):len(reading) - len(enc)]
                if len(stem) >= 3:
                    best = max(best, counts.get(stem, 0))
    return best


def rank_readings(word: str, counts: Counter, ranking: str = "frequency",
                  version: str = "v1", min_count: int = MIN_COUNT) -> list[tuple[str, int]]:
    """(skeleton, count) of the word's attested readings, best first.

    ``frequency`` -- commonest first (the proposal). The broken rankings the gate
    must reject: ``plain`` -- every undotted letter read plain, as the lookup
    does today; ``dotted`` -- every dottable letter read dotted; ``reversed`` --
    rarest attested reading first.

    ``version`` "v2" adds the plural alif (``readings``) and, when no reading is
    attested as a whole word, ranks them by their stems (``backoff_count``)."""
    later = version in ("v2", "v3", "v4")
    all_readings = readings(word, plural_alif=later)
    if ranking == "plain":
        all_readings = all_readings[:1]
    elif ranking == "dotted":
        letters = ja_letters(word)
        dotted_word = "".join(f"{letter}\u0307" if letter in DOTTED else letter for letter, _ in letters)
        all_readings = readings(dotted_word, plural_alif=later)[:1]
    attested = [(r, counts.get(r, 0)) for r in all_readings if counts.get(r, 0) >= min_count]
    if not attested and later:
        attested = [(r, c) for r, c in ((r, backoff_count(r, counts)) for r in all_readings) if c >= min_count]
    if ranking == "reversed":
        return sorted(attested, key=lambda rc: rc[1])
    order = {r: i for i, r in enumerate(all_readings)}
    return sorted(attested, key=lambda rc: (-rc[1], order[rc[0]]))


def _proclitic_layers(pro: str) -> int:
    """How many proclitics a peeled prefix is: و/ف, then ب/ل/ك, then the article (لل is two)."""
    layers, rest = 0, pro
    if rest[:1] in ("و", "ف"):
        layers, rest = layers + 1, rest[1:]
    if rest.startswith("لل"):
        return layers + 2
    if rest[:1] in ("ب", "ل", "ك"):
        layers, rest = layers + 1, rest[1:]
    return layers + rest.startswith("ال")


def form_roots(reading: str, forms: dict[str, set[str]], prior: Counter,
               counts: Optional[Counter] = None) -> list[str]:
    """Roots of a reading through Lane's headwords: the reading, or the reading with
    a proclitic and a pronoun peeled, found as a headword. Score: half a point per
    proclitic, a point for a pronoun, less a third of the stem's log10 count in the
    sample (بقاءك is بقاء + ك, not ب + قاك); best first."""
    counts = counts or Counter()
    found: dict[str, float] = {}
    for pro in BACKOFF_PROCLITICS:
        if not reading.startswith(pro):
            continue
        for enc in BACKOFF_ENCLITICS:
            if not reading.endswith(enc):
                continue
            stem = reading[len(pro):len(reading) - len(enc)]
            if len(stem) < 2 or stem not in forms:
                continue
            score = (COST_PROCLITIC * _proclitic_layers(pro) + COST_AFFIX * bool(enc)
                     - math.log10(1 + counts.get(stem, 0)) / 3)
            for root in forms[stem]:
                if score < found.get(root, 99.0):
                    found[root] = score
    return sorted(found, key=lambda k: (found[k], -prior.get(k, 0), k))


PROCLITICS = ("وبال", "فبال", "وال", "فال", "بال", "كال", "لل", "ال", "وب", "فب", "ول", "فل", "وك", "فك",
              "و", "ف", "ب", "ل", "ك", "س")
ENCLITICS = ("كما", "هما", "ها", "هم", "هن", "كم", "كن", "نا", "ني", "ه", "ك", "ي")
ENDINGS = ("تما", "تموا", "ون", "ين", "ان", "ات", "تم", "تن", "وا", "ة", "ه", "ت", "ا", "ن", "ي")
PREFIXES = ("است", "مست", "ان", "ي", "ت", "ن", "ا", "أ", "م")
INFIXES = set("اويت")
#: Peeling a proclitic (conjunction, preposition, article) is cheaper than an
#: inflection, and both are cheaper than dropping a letter from inside the stem.
COST_PROCLITIC, COST_AFFIX, COST_INFIX = 0.5, 1.0, 1.5


def strip_roots(surface: str, roots: set[str], prior: Optional[Counter] = None) -> list[str]:
    """Root keys an Arabic word may come from: peel a proclitic, a pronoun, an
    inflectional ending and a verbal or nominal prefix (each optional), drop at
    most one inner long vowel or t, and keep what is a root of Lane or Lisān.
    Cheapest first; a tie goes to the root with the longer dictionary articles."""
    word = strip_harakat(surface)
    stems: list[tuple[float, str]] = []
    for pro in ("",) + PROCLITICS:
        if pro and not word.startswith(pro):
            continue
        if pro == "س" and word[1:2] not in ("ي", "ت", "ن", "أ"):
            continue  # the future marker comes only before an imperfect verb (سلامة is not سـ + لامة)
        a = word[len(pro):]
        for enc in ("",) + ENCLITICS:
            if enc and not a.endswith(enc):
                continue
            b = a[: len(a) - len(enc)] if enc else a
            for end in ("",) + ENDINGS:
                if end and not b.endswith(end):
                    continue
                c = b[: len(b) - len(end)] if end else b
                for pre in ("",) + PREFIXES:
                    if pre and not c.startswith(pre):
                        continue
                    d = c[len(pre):]
                    if len(d) >= 2:
                        cost = (COST_PROCLITIC * bool(pro)
                                + COST_AFFIX * (bool(enc) + bool(end) + bool(pre)))
                        stems.append((cost, d))
    found: dict[str, float] = {}
    for cost, stem in stems:
        candidates = [(0.0, stem)]
        if len(stem) > 3:
            candidates += [(COST_INFIX, stem[:i] + stem[i + 1:]) for i, ch in enumerate(stem) if ch in INFIXES and i]
        for extra, cand in candidates:
            if 2 <= len(cand) <= 4:
                key = root_key(cand)
                if key in roots and cost + extra < found.get(key, 99.0):
                    found[key] = cost + extra
    prior = prior or Counter()
    return sorted(found, key=lambda k: (found[k], -prior.get(k, 0), k))


def closed_class_roots(reading: str, closed: dict[str, list[str]]) -> list[str]:
    """v4: a closed-class word, alone or after a conjunction (و, ف), is the table's: its roots in
    the table's order; [] when the reading is not one."""
    for pro in ("", "و", "ف"):
        if reading.startswith(pro) and reading[len(pro):] in closed:
            return closed[reading[len(pro):]]
    return []


def reading_roots(reading: str, *, surface: dict[str, str], roots: set[str], prior: Counter,
                  forms: Optional[dict[str, set[str]]] = None, counts: Optional[Counter] = None,
                  closed: Optional[dict[str, list[str]]] = None, version: str = "v4") -> list[str]:
    """The roots of one reading, best first, by method ``version``: the closed-class table (v4),
    then Lane's headwords (v3, v4), then stripping, on the reading's commonest spelling in the
    count (``surface``: it carries the hamza and the ة the skeleton folds away)."""
    if closed and version == "v4":
        found = closed_class_roots(reading, closed)
        if found:
            return found
    if version in ("v3", "v4") and forms:
        found = form_roots(reading, forms, prior, counts)
        if found:
            return found
    return strip_roots(surface.get(reading, reading), roots, prior)

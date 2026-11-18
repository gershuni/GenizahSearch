# -*- coding: utf-8 -*-
"""Near spellings of a catalogue word: the "near spellings" option of the catalogue search
(docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md §4.7).

The rule depends on the word's script, because catalogue spellings differ differently in each
(measured on the real file, 2026-11-18, plan §4.7):

- **Hebrew: plene and defective spelling**, one ו or י added or dropped inside the word: the
  Responsa search's ``%word`` (``shared/responsa.py::expand_plene_defective``), so the two
  searches agree. כתובה finds כתבה, תפילין finds תפלין, ירושלים finds ירשלים. The text search's
  Fuzzy rule (two edits from five letters) was measured first and is far too loose for single
  catalogue words: כתובה reached 511 words and 135,943 manuscripts, against 2,216 as typed.
- **Latin letters: transliteration and typing**, the text search's Fuzzy edits (a letter added,
  dropped or replaced, or two neighbours swapped; OSA distance) but from five letters only: one
  edit for 5-8 letters, two from 9. Saadia finds saadya and saadiah (281 manuscripts as typed,
  10,125 with them), Fustat finds fostat, ketubba finds ketuba and ketubbah. Below five letters
  one edit is mostly another word (wine: line, mine, nine), so a short word has none, and neither
  has one longer than ``LATIN_MAX_LETTERS``: two edits of a long word are a few hundred thousand
  strings (measured 2026-11-18: 114,125 in 0.14 s at 9 letters, 311,291 in 0.41 s at 15, 549,956
  in 0.75 s at 20), and an unbounded word (the API takes 1,000 characters) must not cost
  unbounded time and memory.
- Anything else (digits, mixed scripts, Arabic letters) has none.

Every near spelling is a string, not a word: the caller keeps those its own vocabulary holds.
Nothing is cached here (a two-edit set is tens of megabytes); the caller caches what it kept.
The keys are the catalogue's folded keys (``shared/catalog_text_normalize.py``: no nikud, final
letters folded, lower case).
"""
from __future__ import annotations

import re

from rapidfuzz.distance import OSA

from shared.responsa import expand_plene_defective

#: Added after the Fuzzy search switch's tooltip (shared/catalog_text_rows.py::CATALOG_NEAR_TOOLTIP) in
#: Title mode, where the switch also stops reading the query as one phrase (``near_spellings_tooltip``).
TITLE_NEAR_SPELLINGS_TOOLTIP = "Each word anywhere in the title, in any order"

LATIN_LETTERS = "abcdefghijklmnopqrstuvwxyz"
#: Latin words shorter than this have no near spellings.
LATIN_MIN_LETTERS = 5
#: Latin words this long or longer may have two edits; shorter ones one.
LATIN_TWO_EDITS_FROM = 9
#: Latin words longer than this have no near spellings (catalogue words are shorter; the bound
#: keeps the two-edit set at most ~300,000 strings).
LATIN_MAX_LETTERS = 15

_HEBREW_RUN = re.compile("[א-ת]+")
_LATIN_RUN = re.compile("[a-z]+")


def latin_distance(word: str) -> int:
    """The edits a near spelling of the Latin key ``word`` may have: 0 below 5 letters and above
    ``LATIN_MAX_LETTERS``, 1 for 5-8, 2 from 9."""
    if len(word) < LATIN_MIN_LETTERS or len(word) > LATIN_MAX_LETTERS:
        return 0
    return 1 if len(word) < LATIN_TWO_EDITS_FROM else 2


def _edits(word: str, d: int, alphabet: str) -> frozenset[str]:
    found = {word}
    for _ in range(d):
        grown = set(found)
        for w in found:
            for i in range(len(w) + 1):
                if i < len(w):
                    grown.add(w[:i] + w[i + 1:])
                    if i + 1 < len(w):
                        grown.add(w[:i] + w[i + 1] + w[i] + w[i + 2:])
                for ch in alphabet:
                    grown.add(w[:i] + ch + w[i:])
                    if i < len(w):
                        grown.add(w[:i] + ch + w[i + 1:])
        found = grown
    # Two edits can reach a string three apart under OSA (swap two letters, then insert between
    # them); the text search's Fuzzy does not count it, and neither does this.
    return frozenset(w for w in found if w and OSA.distance(word, w, score_cutoff=d) <= d)


def near_spellings(word: str) -> frozenset[str]:
    """Every near spelling of the folded key ``word`` by the rule of its script (module
    docstring), ``word`` itself included; only ``word`` when its script has none."""
    if _HEBREW_RUN.fullmatch(word):
        return frozenset(expand_plene_defective(word))
    if _LATIN_RUN.fullmatch(word):
        d = latin_distance(word)
        return _edits(word, d, LATIN_LETTERS) if d else frozenset({word})
    return frozenset({word})


def near_spellings_tooltip(mode: str, tr) -> str:
    """The Fuzzy search switch's tooltip in search *mode*, through the app's *tr*: in Title
    mode it adds that each word is then looked for on its own, in any order."""
    from shared.catalog_text_rows import CATALOG_NEAR_TOOLTIP
    text = tr(CATALOG_NEAR_TOOLTIP)
    if mode == 'Title':
        text += '. ' + tr(TITLE_NEAR_SPELLINGS_TOOLTIP)
    return text


def distance(word: str, other: str) -> int:
    """The OSA distance between two keys (fewer edits rank first)."""
    return OSA.distance(word, other)

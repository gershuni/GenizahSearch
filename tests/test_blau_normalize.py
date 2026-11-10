# -*- coding: utf-8 -*-
"""shared/blau_normalize.py: the one key under which a Blau dictionary form and a corpus token meet.

Every example below is a spelling the dictionary (release 1.1) or the HTR corpus actually uses:
the Judeo-Arabic dot (ג̇ כ̇ ד̇ צ̇ ט̇), the diaeresis on ה̈ (= ة), nikud on a few headwords, the apostrophe
the corpus writes for the same letters (164,480 against 9,256 dots in a 600,000-line sample of
Transcriptions.txt), final letters, and the brackets/hyphens the dictionary's headwords carry.
"""
from __future__ import annotations

import unicodedata

import pytest

from shared.blau_normalize import (
    NORMALIZER_VERSION,
    SITE_DIACRITIC_RE,
    candidate_stems,
    fold_finals,
    ja_words,
    normalize_arabic,
    normalize_ja,
    strip_marks,
    strip_site_diacritics,
)


@pytest.mark.parametrize("raw, expected", [
    ("אכ̇ד̇", "אכד"),                       # upper dots: خ, ذ
    ("אברה̈", "אברה"),                      # diaeresis: ة
    ("ליכון ד̇לך אבהא להם", "ליכונ דלכ אבהא להמ"),   # finals folded, dot dropped
    ("אֶבִל", "אבל"),                        # nikud on a headword
    ("ג'יד", "גיד"),                        # the corpus's apostrophe for ج
    ("ג׳יד", "גיד"),                        # Hebrew geresh
    ("ר\"ש", "רש"),                         # typed gershayim
    ("צ’מאן", "צמאנ"),                 # curly quote
    ("חיט(ה̈)", "חיטה"),                    # optional letter in brackets: one key
    ("]הנתשנ", "הנתשנ"),                    # the corpus's reading bracket inside a word
    ("נזאלה̈ (נזילה̈)", "נזאלה נזילה"),       # two spellings in one name: two words
    ("מאכול,מצדר", "מאכול מצדר"),           # comma is a separator
    ("אין=אן", "אינ אנ"),
    ("‐", ""),                         # the hyphen placeholder of an empty ja_text
    ("ً - ً", ""),                           # Arabic-only key
    ("أَبَدًا", ""),
    ("  לא   אבד ", "לא אבד"),
    ("", ""),
    (None, ""),
])
def test_normalize_ja(raw, expected):
    assert normalize_ja(raw) == expected


def test_normalize_ja_is_idempotent_and_nfc_insensitive():
    for raw in ("אכ̇ד̇ מנה", "אֶבִל", "ג'יד", "להם"):
        once = normalize_ja(raw)
        assert normalize_ja(once) == once
        assert normalize_ja(unicodedata.normalize("NFD", raw)) == once


def test_ja_words_and_fold_finals():
    assert ja_words("ואכ̇ד̇ת אלכתאב.") == ["ואכדת", "אלכתאב"]
    assert fold_finals("ךםןףץ") == "כמנפצ"


def test_every_mark_the_site_strips_is_dropped_inside_a_word():
    """normalize_ja is a superset of the site's "strip diacritics" (js/search.js): for every code
    point that regex matches, the mark vanishes without splitting the word."""
    site_points = [cp for cp in range(0x0300, 0x0700) if SITE_DIACRITIC_RE.match(chr(cp))]
    assert len(site_points) > 100
    for cp in site_points:
        assert normalize_ja("א" + chr(cp) + "ב") == "אב", f"U+{cp:04X}"
        assert strip_site_diacritics("א" + chr(cp) + "ב") == "אב"


def test_marks_outside_the_site_ranges_are_dropped_too():
    """The source carries a Gujarati nukta (U+0AFE) on six forms; any combining mark goes."""
    assert strip_marks("א૾ב") == "אב"
    assert normalize_ja("א૾ב") == "אב"
    assert strip_site_diacritics("א૾ב") == "א૾ב"      # the site would keep it


@pytest.mark.parametrize("raw, expected", [
    ("أَخَذ", "اخذ"),                        # harakat dropped, hamza alef folded
    ("ﻟﻠﻪ", "لله"),            # presentation forms (the site maps these by hand)
    ("ثُوَيْب", "ثويب"),
    ("كتاـب", "كتاب"),                  # tatweel
    ("إِجارة", "اجارة"),
    ("", ""),
    ("אבד", ""),                             # Hebrew letters are not Arabic words
])
def test_normalize_arabic(raw, expected):
    assert normalize_arabic(raw) == expected


@pytest.mark.parametrize("word, expected", [
    ("ואלכתאב", ["ואלכתאב", "אלכתאב", "כתאב"]),
    ("פאלכתאב", ["פאלכתאב", "אלכתאב", "כתאב"]),
    ("באלכתאב", ["באלכתאב", "אלכתאב", "כתאב"]),
    ("ובאלכתאב", ["ובאלכתאב", "באלכתאב", "אלכתאב", "כתאב"]),
    ("סואלהם", ["סואלהמ", "סואל"]),           # final folded first, then the suffix peeled
    ("סואלה", ["סואלה", "סואל"]),
    ("כתאבה", ["כתאבה", "כתאב", "תאבה", "תאב"]),   # כ is also a preposition: both readings offered
    ("אכ̇ד̇", ["אכד"]),
    ("ללה", ["ללה", "לל", "לה"]),             # never below two letters
    ("לא אבד", ["לא אבד"]),                   # a phrase is looked up whole
    ("", []),
    ("أبد", []),
])
def test_candidate_stems(word, expected):
    assert candidate_stems(word) == expected


def test_candidate_stems_has_no_duplicates_and_starts_with_the_word():
    cands = candidate_stems("והם")
    assert cands[0] == "והמ"
    assert len(cands) == len(set(cands))


def test_normalizer_version_is_an_int():
    assert isinstance(NORMALIZER_VERSION, int) and NORMALIZER_VERSION >= 1

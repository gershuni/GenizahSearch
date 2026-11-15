# -*- coding: utf-8 -*-
"""shared/blau_normalize.py: the one key under which a Blau dictionary form and a corpus token meet.

Every example below is a spelling the dictionary (release 1.1) or the HTR corpus actually uses:
the Judeo-Arabic dot (ג̇ כ̇ ד̇ צ̇ ט̇), the diaeresis on ה̈ (= ة), nikud on a few headwords, the apostrophe
the corpus writes for the same letters (164,480 against 9,256 dots in a 600,000-line sample of
Transcriptions.txt), final letters, and the brackets/hyphens the dictionary's headwords carry.
"""
from __future__ import annotations


def test_query_aliases_do_not_change_stored_dictionary_keys():
    from shared.blau_normalize import query_stems, lookup_stems
    assert 'מולאי' in query_stems('וימולאי')
    assert 'מולאי' not in lookup_stems('וימולאי')
    assert 'דלכ' in query_stems('[דא]לך')
    assert 'דלכ' not in lookup_stems('[דא]לך')
    assert 'כתב' not in query_stems('יכתב'), 'not every initial yod is a vocative'


def test_mater_alternatives_preserve_consonants_boundaries_and_short_words():
    from shared.blau_normalize import mater_variants, analysis_paths, evidence_cost
    assert 'גוכאניה' in mater_variants('גכאניה')
    assert 'תפצל' in mater_variants('תפוצל')
    assert not mater_variants('מני')
    assert not mater_variants('כתאב אחר')
    assert 'כתב' not in mater_variants('כתאב'), 'alif deletion is not a general rule'
    assert ('mater',) in analysis_paths('גכאניה', 'גוכאניה')
    assert evidence_cost((('mater',),)) > evidence_cost((('prefix', 'suffix'),))

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


def test_a_lookup_also_reads_a_feminine_ending_before_a_suffix_as_the_noun_s():
    """סלאמתך and וסעאדתך in a letter's opening reached no entry of סלאמה or
    סעאדה. A lookup reads a ת a pronoun suffix was peeled from also as ה, right after it; never
    a ת the word ends in (בית, סלאמת), never below three letters, and the expression search's
    keys (``candidate_stems``, the sidecars' keying) are unchanged."""
    from shared.blau_normalize import lookup_stems

    assert lookup_stems("סלאמתך") == ["סלאמתכ", "סלאמת", "סלאמה"]
    assert lookup_stems("וסעאדתך") == ["וסעאדתכ", "וסעאדת", "וסעאדה", "סעאדתכ", "סעאדת", "סעאדה"]
    assert lookup_stems("עזתהא") == ["עזתהא", "עזת", "עזה"]
    assert lookup_stems("סלאמת") == ["סלאמת", "סלאמה"]
    assert lookup_stems("בית")[0] == "בית", "retain the literal word alongside noun hypotheses"
    assert "לה" not in lookup_stems("לתך"), "never below three letters"
    assert "סלאמה" not in candidate_stems("סלאמתך")
    for word in ("ואלכתאב", "סואלהם", "כתאבה", "ללה", "לא אבד", "", "أبد"):
        assert lookup_stems(word) == candidate_stems(word), word


def test_an_article_that_lost_its_alif_after_a_preposition_is_peeled_apart():
    """בלסלאם (bi-l-salām) reached nothing: ב, ל or כ (after ו or פ), then the
    article's ל without its alif, then the stem, of three letters or more. Not in ``candidate_stems``,
    which keys the sidecars; a noun's keys (``article_candidates``)."""
    from shared.blau_normalize import article_candidates, elided_article_stems

    assert elided_article_stems("בלסלאם") == ["סלאמ"]
    assert elided_article_stems("ללנאס") == ["נאס"] and elided_article_stems("כלכתאב") == ["כתאב"]
    assert elided_article_stems("ובלסלאמה") == ["סלאמה", "סלאמ"] and elided_article_stems("פבלסלאם") == ["סלאמ"]
    assert elided_article_stems("באלסלאם") == [], "the article written whole: candidate_stems"
    assert elided_article_stems("בלאד") == [], "never below three letters"
    assert elided_article_stems("לסלאם") == [] and elided_article_stems("סלאם") == [], "no preposition before it"
    assert elided_article_stems("וללה") == [] and elided_article_stems("") == [] and elided_article_stems("בל סלאם") == []
    assert "סלאמ" not in candidate_stems("בלסלאם") and article_candidates("בלסלאם") == {"סלאמ"}
    assert article_candidates("ובאלסלאם") == {"סלאמ"}


def test_candidate_stems_has_no_duplicates_and_starts_with_the_word():
    cands = candidate_stems("והם")
    assert cands[0] == "והמ"
    assert len(cands) == len(set(cands))


def test_normalizer_version_is_an_int():
    assert isinstance(NORMALIZER_VERSION, int) and NORMALIZER_VERSION >= 1


def test_demonstrative_alif_is_a_query_alias_without_changing_stored_keys():
    from shared.blau_normalize import query_stems, word_analyses
    for query, target in [('האדא', 'הדא'), ('והאדא', 'הדא'), ('האדה', 'הדה')]:
        assert target in query_stems(query)
        assert target not in {a.key for a in word_analyses(query)}
        assert query_stems(query)[0] == query


def test_the_keys_reached_only_by_peeling_the_article_are_a_noun_s():
    """A word with the article is no finite verb. ``article_candidates`` are the
    candidates reached only by peeling אל (and a pronoun suffix after it); never the word's own key
    (אלתזם is stem VIII of לזם), never a word without the article."""
    from shared.blau_normalize import article_candidates

    assert article_candidates("אלקאצ'י") == {"קאצי", "קאצ"}
    assert article_candidates("ואלסלאם") == {"סלאמ"}
    assert article_candidates("אלתזם") == {"תזמ"} and "אלתזמ" in candidate_stems("אלתזם")
    assert article_candidates("כתאב") == frozenset() and article_candidates("וכתאב") == frozenset()
    assert article_candidates("אלי") == frozenset(), "too short to peel"
    assert article_candidates("") == frozenset() and article_candidates("אל כתאב") == frozenset()
    assert article_candidates("אלכתאב") <= set(candidate_stems("אלכתאב"))

# -*- coding: utf-8 -*-
"""shared/arabic_normalize.py: a Judeo-Arabic word's Arabic readings and the sidecar's keys.

An undotted letter gives both readings and a marked one gives one (except ג, whose mark means
ج in Blau and غ in many editions); a final א is also alef maqsura; a final ו is also read with
the plural's silent alif when asked; the skeleton folds what Judeo-Arabic does not write; the
root keys of Lane, Lisān and a peeled word meet. No dictionary or corpus text is needed.
"""
from __future__ import annotations

from shared.arabic_normalize import (
    NORMALIZER_VERSION,
    arabic_tokens,
    buckwalter_to_arabic,
    is_abbreviation,
    ja_letters,
    readings,
    root_key,
    skeleton,
    strip_harakat,
    strong_letters,
)


def test_an_undotted_letter_is_read_both_ways_and_a_marked_one_once():
    assert readings("דלך") == ["دلك", "دلخ", "ذلك", "ذلخ"], "plain letters first, the last letter varying fastest"
    assert readings("ד'לך") == ["ذلك", "ذلخ"]
    assert readings("ד̇לך") == ["ذلك", "ذلخ"]
    assert readings("ד׳לך") == ["ذلك", "ذلخ"], "the geresh marks the dot as the apostrophe does"
    assert readings("ר'גל") == readings("רגל"), "an apostrophe after ר is an abbreviation, not a dot"
    assert set(readings("ג'יר")) == {"جير", "غير"}, "a mark on ג says only 'not plain'"
    assert ja_letters("אלמד׳") == [("א", False), ("ל", False), ("מ", False), ("ד", True)]
    assert readings("") == [] and readings("abc") == []


def test_a_final_alef_may_be_alef_maqsura_and_a_final_waw_may_carry_the_plural_alif():
    assert "موسي" in readings("מוסא")
    assert "حضروا" not in readings("חצ'רו") and "حضروا" in readings("חצ'רו", plural_alif=True)
    assert readings("או", plural_alif=True) == readings("או"), "a two-letter word is not a plural verb"


def test_the_skeleton_folds_what_judeo_arabic_does_not_write():
    assert skeleton("شيء") == "شي" and skeleton("على") == "علي" and skeleton("مدينة") == "مدينه"
    assert skeleton("أَيْضًا") == "ايضا" and skeleton("ذٰلِكَ") == "ذلك"
    assert skeleton("إلى") == skeleton("الى") == "الي"
    assert strip_harakat("كَتَبَ") == "كتب"
    assert list(arabic_tokens("قال: «الكِتابُ» abc 12 كتب")) == ["قال", "الكتاب", "كتب"]


def test_root_keys_meet_across_the_dictionaries():
    assert root_key(buckwalter_to_arabic("Ax*")) == root_key("أخذ") == "ءخذ", "Lane writes initial hamza as A"
    assert root_key(buckwalter_to_arabic("A^bw")) == root_key("أبو")
    assert root_key("قال") == root_key("قول") == "قWل"
    assert root_key(buckwalter_to_arabic("$b")) == root_key("شبب"), "a geminate root has two letters in Lane"
    assert strong_letters(root_key("قول")) == "قل" and strong_letters(root_key("شبب")) == "شب"


def test_abbreviation_marks_are_told_from_dots():
    assert is_abbreviation("ב׳כ׳ר׳") and is_abbreviation('ר"ת') and is_abbreviation("ר'")
    assert not is_abbreviation("ד'לך") and not is_abbreviation("אלמד׳")


def test_the_normalizer_has_a_version():
    assert isinstance(NORMALIZER_VERSION, int) and NORMALIZER_VERSION >= 1

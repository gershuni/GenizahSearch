# -*- coding: utf-8 -*-
"""scripts/measure_blau_close_spellings.py: the pure parts a measurement rests on.

The swap table is the top HTR confusion pairs, folded and both ways; a close
spelling swaps exactly one letter and peels after the swap, best confusion first;
the alignment pairs a word only where the page follows the edition, and refuses a
page that does not. No corpus or dictionary text is needed.
"""
from __future__ import annotations

from scripts.measure_blau_close_spellings import (
    CLOSE_PAIR_COUNT,
    Measure,
    aligned_pairs,
    close_spellings,
    close_swaps,
)


def test_the_swap_table_is_the_top_htr_pairs_folded_and_both_ways():
    from shared.unified_variants import UNIFIED_VARIANT_PAIRS

    swaps = close_swaps()
    first_source, first_target, _ = UNIFIED_VARIANT_PAIRS[0]
    assert (first_target, 0) in swaps[first_source] and (first_source, 0) in swaps[first_target]
    assert not set(swaps) & set("ךםןףץ"), "finals fold to the medial letter, as normalize_ja does"
    pairs = {frozenset((a, b)) for a, alts in swaps.items() for b, _rank in alts}
    assert len(pairs) == CLOSE_PAIR_COUNT
    for a, alts in swaps.items():
        for b, rank in alts:
            assert (a, rank) in swaps[b], (a, b)
    assert len(close_swaps(3)) < len(swaps)


def test_a_close_spelling_swaps_one_letter_and_peels_after_the_swap():
    keys = frozenset({"אכד", "בלג"})
    assert close_spellings("אכר", keys) == [("אכד", "אכד")]       # ד read as ר
    assert close_spellings("יאכד", keys) == [("אכד", "ואכד")]     # the conjunction ו read as י
    assert close_spellings("אכד", keys, exclude=("אכד",)) == []
    assert close_spellings("אכ", frozenset({"אד"})) == [], "a two-letter word is not tried"
    assert close_spellings("אכר זה", keys) == [], "a phrase is not tried"


def test_the_more_frequent_confusion_comes_first():
    # From מדה: ד->ר (rank 0) reaches מרה; ה->ח (a later rank) reaches מדח.
    assert ("ר", 0) in close_swaps()["ד"]
    assert [stem for stem, _ in close_spellings("מדה", frozenset({"מדח", "מרה"}))] == ["מרה", "מדח"]


def test_alignment_pairs_words_only_where_the_page_follows_the_edition():
    edition = "אלכתאב אלדי כתבתה אלי מולאי אלשיך".split()
    page = "אלכתאב אלדי כתבתה אלר מולאי אלשיך".split()      # one word misread
    pairs = aligned_pairs(page, edition, 0.5)
    assert ("אלר", "אלי") in pairs and ("אלכתאב", "אלכתאב") in pairs and len(pairs) == 6
    assert aligned_pairs("זה טקסט אחר לגמרי בלי קשר".split(), edition, 0.5) is None
    # A run of different length between matches is no pair: the words do not line up.
    uneven = "אלכתאב אלדי כת בתה אלי מולאי אלשיך".split()
    assert ("כת", "כתבתה") not in (aligned_pairs(uneven, edition, 0.5) or [])


def test_the_measure_counts_a_cross_reference_as_its_target():
    measure = Measure({"אכד": frozenset({"e1"}), "חוה": frozenset({"e1"})}, [CLOSE_PAIR_COUNT])
    assert measure.roots("ואכד") == {"e1"}
    assert measure.close("אכר", CLOSE_PAIR_COUNT) == ("e1",)
    assert measure.close("זזזז", CLOSE_PAIR_COUNT) == ()

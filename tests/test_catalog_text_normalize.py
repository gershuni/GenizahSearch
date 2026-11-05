# -*- coding: utf-8 -*-
"""shared/catalog_text_normalize.py: one key for the catalogue text and the query
(docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md §4.2)."""
from __future__ import annotations

import re

from shared.catalog_text_normalize import (
    HEBREW_PREFIXES,
    Term,
    can_expand,
    fts_match,
    hebrew_forms,
    match_spans,
    is_hebrew_text,
    key,
    key_words,
    parse_query,
    tokens,
)


def test_nikud_and_cantillation_are_dropped():
    assert key("כְּתוּבָּה") == "כתובה"
    assert key("שָׁלוֹם֙") == "שלומ"


def test_final_letters_fold_so_any_spelling_meets():
    assert key("שלום") == key("שלומ") == "שלומ"
    assert key("ארץ") == "ארצ"


def test_abbreviation_quote_marks_disappear_whatever_was_typed():
    assert key('רמב"ם') == key("רמב״ם") == key("רמבם") == "רמבמ"
    assert key("ר' יהודה") == "ר יהודה"
    assert key("Sa'adia") == key("Saadia") == "saadia"


def test_latin_case_and_accents_fold():
    assert key("Fusṭāṭ") == key("FUSTAT") == "fustat"
    assert key("Café") == "cafe"


def test_punctuation_and_maqaf_separate_words():
    assert key_words("בית־דין, (ketubba).") == ["בית", "דינ", "ketubba"]
    assert key_words("T-S 12.123") == ["t", "s", "12", "123"]


def test_tokens_point_at_the_original_text():
    text = 'שטר "הכְּתוּבָּה" של פלונית'
    found = [(text[t.start:t.end], t.keys) for t in tokens(text)]
    assert found == [("שטר", ("שטר",)), ("הכְּתוּבָּה", ("הכתובה",)), ("של", ("של",)), ("פלונית", ("פלונית",))]


def test_language_label():
    assert is_hebrew_text("שטר of sale")
    assert not is_hebrew_text("Bill of sale")


def test_hebrew_forms_add_the_prefixes_only_to_hebrew_words():
    forms = hebrew_forms("כתובה")
    assert forms[0] == "כתובה" and len(forms) == 1 + len(HEBREW_PREFIXES)
    assert "הכתובה" in forms and "וכתובה" in forms and "אכתובה" not in forms
    assert hebrew_forms("ב") == ("ב",)
    assert hebrew_forms("cheese") == ("cheese",)
    assert hebrew_forms("12") == ("12",)


def test_parse_query_words_and_phrases():
    assert parse_query("כתובה cheese") == [Term(("כתובה",), False), Term(("cheese",), False)]
    assert parse_query('"marriage contract" Fustat') == [
        Term(("marriage", "contract"), True), Term(("fustat",), False)]
    assert parse_query("“גט פיטורין”") == [Term(("גט", "פיטורינ"), True)]


def test_a_gershayim_inside_a_word_is_not_a_phrase_mark():
    assert parse_query('תשובות רמב"ם') == [Term(("תשובות",), False), Term(("רמבמ",), False)]
    assert parse_query('"תשובות רמב"ם"') == [Term(("תשובות", "רמבמ"), True)]


def test_an_unmatched_quote_leaves_ordinary_words():
    assert parse_query('cheese "wine') == [Term(("cheese",), False), Term(("wine",), False)]


def test_a_query_without_words_is_empty():
    assert parse_query("") == []
    assert parse_query('" ; - * "') == []


def test_fts_match_quotes_every_key_and_adds_only_its_own_operators():
    terms = parse_query("כתובה cheese")
    assert fts_match(terms, expand_prefixes=False) == '"כתובה" AND "cheese"'
    expanded = fts_match(terms, expand_prefixes=True)
    assert expanded.startswith('("כתובה" OR "הכתובה" OR ')
    assert expanded.endswith(') AND "cheese"')
    assert fts_match(parse_query('"marriage contract"'), expand_prefixes=True) == '"marriage contract"'


def test_reader_input_never_reaches_fts5_syntax():
    for query in ('cheese OR wine', 'NEAR(a b)', 'title:cheese', 'chee*', '-wine', '^cheese', 'a + b', "a' OR '1"):
        match = fts_match(parse_query(query), expand_prefixes=True)
        # Outside the double-quoted keys there is nothing but our own AND / OR and parentheses.
        outside = re.sub(r'"[^"]*"', "", match)
        assert set(outside.split()) <= {"AND", "OR", "(", ")"} | {""}, (query, match)
        assert all(re.fullmatch(r"[^\W_]+(?: [^\W_]+)*", k) for k in re.findall(r'"([^"]*)"', match))


def test_can_expand():
    assert can_expand(parse_query("כתובה"))
    assert not can_expand(parse_query("cheese"))
    assert not can_expand(parse_query('"כתובה"'))


def _marked(text, query, expand_prefixes=False):
    spans, anchor = match_spans(text, parse_query(query), expand_prefixes)
    return [text[s:e] for s, e in spans], anchor


def test_single_words_are_marked_wherever_they_stand():
    text = "כתובה ושטר הכתובה"
    assert _marked(text, "כתובה") == (["כתובה"], 0)
    assert _marked(text, "כתובה", expand_prefixes=True) == (["כתובה", "הכתובה"], 0)


def test_a_phrase_is_marked_only_where_its_words_stand_together():
    text = "A marriage gift is listed; the marriage contract follows."
    words, anchor = _marked(text, '"marriage contract"')
    assert words == ["marriage", "contract"]
    assert anchor == text.index("marriage contract")


def test_the_anchor_is_the_first_phrase_match_even_after_a_single_word():
    text = "Cheese and wine. Later: the marriage contract."
    words, anchor = _marked(text, 'cheese "marriage contract"')
    assert words == ["Cheese", "marriage", "contract"]
    assert anchor == text.index("marriage contract")
    assert _marked("nothing here", "cheese") == ([], None)


def test_english_words_are_made_singular_and_nothing_else_is():
    from shared.catalog_text_normalize import english_singular

    assert [english_singular(w) for w in ("letters", "stories", "glasses", "churches", "boxes", "cheeses")] == [
        "letter", "story", "glass", "church", "box", "cheese"]
    for word in ("glass", "exodus", "genesis", "was", "fustat", "12s"):
        assert english_singular(word) == word
    assert key("Ezra's Letters") == "ezra letter"
    assert key("Prayers for the Holidays") == key("prayer for the holiday")
    # Hebrew and Arabic keys pass through untouched, however they end.
    assert english_singular("תפילות") == "תפילות"
    assert key("كتابات") == "كتابات"
    # NFKD splits a hamza seat into its letter and the mark, on both sides: رسائل and رسايل meet.
    assert key("رسائل") == key("رسايل")

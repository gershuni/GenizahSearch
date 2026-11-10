# -*- coding: utf-8 -*-
"""web/pages/help.py::dictionary_bullets names only the dictionaries served: with Blau and Lane
it is the owner's approved release text of 2026-11-05, word for word; Friedman alone promises
neither Blau nor Lane and describes the expression search (Codex, round 5 of #487)."""
from __future__ import annotations

from web.pages.help import dictionary_bullets

APPROVED = {
    'en': (
        "- **Judeo-Arabic dictionaries:** the **עع** button (labelled *Judeo-Arabic dictionaries*) at the "
        "bottom of the text pane turns them on, and the choice is remembered. Click a word of the "
        "transcription to see its entries in Joshua Blau's *Dictionary of Mediaeval Judaeo-Arabic Texts* "
        "([the MiDRASH project's digital edition](https://blau-dict.netlify.app/about.html)) and, in a "
        "second tab, its Arabic readings and root in Lane's *Arabic-English Lexicon*. The box at the top "
        "of the panel takes a typed or corrected word, in Hebrew or Arabic letters. Note: Blau's "
        "dictionary records mainly meanings particular to Judeo-Arabic; for verbs and inflected forms, "
        "type the root or the base form.\n"
        "- **Looking up a word without the switch:** press **D** or **M**, then click a word (or click "
        "it with **Alt** held); or select words and press **D** or **M**. On a phone, select words and "
        "tap **Look up in the dictionary** under them. **Esc** closes the dictionary.\n"
    ),
    'he': (
        "- **מילונים לערבית־יהודית:** הכפתור **עع** (בכותרת \"מילונים לערבית־יהודית\") בתחתית חלונית "
        "הטקסט מפעיל אותם, והבחירה נשמרת. לחיצה על מילה בתעתוק מציגה את ערכיה במילון של יהושע בלאו "
        "לטקסטים ערביים־יהודיים מימי הביניים (במהדורה הדיגיטלית של "
        "[פרויקט MiDRASH](https://blau-dict.netlify.app/about.html)), ובלשונית שנייה את קריאותיה "
        "בערבית ואת שורשה בלקסיקון הערבי-אנגלי של ליין. בתיבה שבראש החלונית אפשר להקליד מילה או "
        "לתקן אותה, באותיות עבריות או ערביות. שימו לב: מילון בלאו מתעד בעיקר הוראות ייחודיות "
        "בערבית-יהודית: כדי לחפש פעלים ונטיות יש להקליד את השורש או את צורת הבסיס.\n"
        "- **חיפוש במילון בלי להפעיל את הכפתור:** לחצו על **D** או **M** (או מ) ואז על מילה (או "
        "לחצו עליה כש-**Alt** לחוץ), או בחרו מילים ולחצו על **D** או **M**. בטלפון, בחרו מילים "
        "והקישו על **חיפוש במילון** שמופיע מתחתיהן. **Esc** סוגר את המילון.\n"
    ),
}


#: The sentence added for Blau's expression search (2026-11-08; a draft for the owner's approval),
#: between the dictionaries sentence and the box sentence of the approved text.
EXPRESSION = {
    'en': (" Select several words, or type them, to search Blau's dictionary for the expression: the "
           "entries whose headword or quotations hold the words in that order, with a choice of how many "
           "words may stand between them."),
    'he': (" בחירה של כמה מילים, או הקלדתן, מחפשת את הביטוי במילון בלאו: הערכים שבראשם או במובאותיהם "
           "המילים מופיעות בסדר הזה, ואפשר לבחור כמה מילים מותרות ביניהן."),
}
BOX = {'en': " The box at the top", 'he': " בתיבה שבראש החלונית"}


def test_blau_and_lane_are_the_approved_text_plus_the_expression_sentence():
    served = {'blau': True, 'lane': True, 'friedman': False}
    for lang in ('en', 'he'):
        expected = APPROVED[lang].replace(BOX[lang], EXPRESSION[lang] + BOX[lang], 1)
        assert expected != APPROVED[lang]
        assert dictionary_bullets(lang, served, {'blau': True, 'friedman': False}) == expected, lang
        # A 1.0.0 Blau sidecar (no tokens) is served without the expression search while the code
        # is on the server ahead of the rebuilt file: the approved text, no promise (Codex on #488).
        assert dictionary_bullets(lang, served, {'blau': False, 'friedman': False}) == APPROVED[lang], lang


def test_friedman_alone_promises_neither_blau_nor_lane():
    en = dictionary_bullets('en', {'blau': False, 'lane': False, 'friedman': True}, {'friedman': True})
    assert "Friedman" in en and "Blau" not in en and "Lane" not in en and "second tab" not in en
    assert "expression" in en and "**Esc** closes the dictionary" in en
    he = dictionary_bullets('he', {'friedman': True}, {'friedman': True})
    assert "פרידמן" in he and "בלאו" not in he and "ליין" not in he and "לשונית" not in he
    assert "הביטוי" in he and "**Esc** סוגר את המילון" in he


def test_all_three_in_the_tab_order_and_none_is_nothing():
    all_three, both = {'blau': True, 'friedman': True, 'lane': True}, {'blau': True, 'friedman': True}
    en = dictionary_bullets('en', all_three, both)
    assert en.index("Blau's") < en.index("Friedman's") < en.index("Lane's") and "two more tabs" in en
    assert "to search Blau's and Friedman's dictionaries for the expression" in en and "Note: Blau's" in en
    assert "to search Friedman's dictionary for the expression" in dictionary_bullets('en', {'friedman': True}, {'friedman': True})
    assert "to search Friedman's dictionary for the expression" in dictionary_bullets('en', all_three, {'friedman': True}),         "Blau served on a 1.0.0 sidecar: Friedman's search alone is named"
    assert "מילוני בלאו ופרידמן" in dictionary_bullets('he', both, both)
    he = dictionary_bullets('he', all_three, both)
    assert he.index("בלאו") < he.index("פרידמן") < he.index("ליין") and "ובשתי לשוניות נוספות" in he
    assert dictionary_bullets('en', {}, {}) == '' and dictionary_bullets('he', {'blau': False}, both) == ''
    lane_only = dictionary_bullets('en', {'lane': True}, both)
    assert "Lane's" in lane_only and "Note: Blau's" not in lane_only and "expression" not in lane_only

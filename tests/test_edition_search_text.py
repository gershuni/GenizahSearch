# -*- coding: utf-8 -*-
"""The search text of a scholarly edition (shared/edition_search_text.py).

Each case states the expected search text by hand, from the edition's marks as the FGP
legends define them (shared/fgp_marks_legend.py) -- never from the cleaner's own output.
"""
import pytest

from shared.edition_search_text import CLEANER_VERSION, SOURCE_FGP, SOURCE_PGP, search_text

CASES = [
    # (name, source, genuzot, original, expected search text)
    ("FGP Word catalogue block", SOURCE_FGP, False,
     "Cambridge, University Library, Taylor-Schechter, Old Series\nT-S C1.35\nלקח טוב\n\n1r\nשלום עליכם\n",
     "שלום עליכם"),
    ("a library line not at the top stays", SOURCE_FGP, False,
     "שלום עליכם\nCambridge University Library\n", "שלום עליכם\nCambridge University Library"),
    ("PGP recto and verso labels", SOURCE_PGP, False,
     "Recto\nשורה ראשונה\nVerso:\nשורה שנייה", "שורה ראשונה\nשורה שנייה"),
    ("PGP side headers with a qualifier", SOURCE_PGP, False,
     "Recto - right margin\nשורה\nVerso. Address.\nשורה שנייה\nVerso (upside down)\nשלישית\n"
     "Recto Margin\nרביעית\nverso, bottom margin\nחמישית",
     "שורה\nשורה שנייה\nשלישית\nרביעית\nחמישית"),
    ("a line that only starts with the word recto stays", SOURCE_PGP, False,
     "recto text written later\nשורה", "recto text written later\nשורה"),
    ("a side header holding Hebrew text stays", SOURCE_PGP, False,
     "Recto - שורה ראשונה\nשנייה", "Recto - שורה ראשונה\nשנייה"),
    ("adjacent headers ending the text", SOURCE_PGP, False, "foo\nRecto\nVerso", "foo"),
    ("adjacent headers around a line", SOURCE_PGP, False, "Recto\nVerso\nשורה\nRecto\n\nVerso:", "שורה"),
    ("only headers", SOURCE_PGP, False, "Recto\nVerso", ""),
    ("blank lines ending the text", SOURCE_FGP, False, "שורה\n\n \n", "שורה"),
    ("Hebrew side labels", SOURCE_FGP, False,
     "צד א' (r צד ימין)\nשורה ראשונה\nע\"ב\nשורה שנייה", "שורה ראשונה\nשורה שנייה"),
    ("line numbers in a numbered text", SOURCE_FGP, False,
     "01 יגיע להרגיע\n02 אדוננו משה\n03 שר השרים\n04", "יגיע להרגיע\nאדוננו משה\nשר השרים"),
    ("dotted line numbers", SOURCE_FGP, False,
     "1. [...] יוחסין\n2. ...] כי\n3. שם", "[...] יוחסין\n...] כי\nשם"),
    ("a number in a text that does not number its lines", SOURCE_FGP, False,
     "12 דינרים\nאחת\nשתיים\nשלוש", "12 דינרים\nאחת\nשתיים\nשלוש"),
    ("editorial heading lines", SOURCE_FGP, False,
     "ציטטות:\nשורה\nלפני הגהה אחרונה\nעוד [פסקה חדשה]שורה", "שורה\nעוד שורה"),
    ("between the lines: letters kept", SOURCE_FGP, True, "אדם אלי אחזמר^ע^ת נבאו", "אדם אלי אחזמרעת נבאו"),
    ("an empty superscript pair", SOURCE_FGP, False, "שלום\n^ ^\n\nעולם", "שלום\nעולם"),
    ("a correction pair keeps the new reading", SOURCE_FGP, True, "אימטויי א(נ){ס}א שם", "אימטויי אסא שם"),
    ("an erasure corrected", SOURCE_FGP, True, "ערכו ((ל)){י}זכר", "ערכו יזכר"),
    ("letters deleted inside a word go", SOURCE_FGP, True, "קדשך שבעו(עו) עמים", "קדשך שבעו עמים"),
    ("a deleted word stays findable", SOURCE_FGP, True, "אחד (מכל) הכבשים", "אחד מכל הכבשים"),
    ("other FGP teams' parentheses restore", SOURCE_FGP, False, "אלאסוי פד(אה וישלח)", "אלאסוי פדאה וישלח"),
    ("PGP parentheses expand", SOURCE_PGP, False, "ורד אהל אל(כא)לק", "ורד אהל אלכאלק"),
    ("alternatives are never joined", SOURCE_FGP, True, "ת לחלום ב/כאש בנעו", "ת לחלום ב כאש בנעו"),
    ("struck letters inside a word go", SOURCE_PGP, False, "עליהם ב⟦ו⟧ל דרהם", "עליהם בל דרהם"),
    ("a struck word stays findable", SOURCE_PGP, False, "דאר אל ⟦נאקה⟧ שם", "דאר אל נאקה שם"),
    ("interlinear additions between \\ and /", SOURCE_FGP, False, "\\אומרין/ הוא", "אומרין הוא"),
    ("an editor's query mark", SOURCE_PGP, False, "יאו[ש]ע(?)הנז׳ לחת", "יאו[ש]ע הנז׳ לחת"),
    ("a colon between words", SOURCE_FGP, False, "יום העשור:וערב", "יום העשור וערב"),
    ("doubtful letters and other Genuzot signs", SOURCE_FGP, True,
     "ל?ח?[....] וא@חד@ =ויאמר= תכת#וב", "לח[....] ואחד ויאמר תכתוב"),
    ("points, brackets, lacuna dots and gershayim untouched", SOURCE_FGP, False,
     "שָׁלוֹם ע[ם] ה..ו רמב\"ם", "שָׁלוֹם ע[ם] ה..ו רמב\"ם"),
    ("control characters and carriage returns", SOURCE_FGP, False, "ב\x0fנועם\r\nשם", "בנועם\nשם"),
]


@pytest.mark.parametrize("name,source,genuzot,original,expected", CASES, ids=[c[0] for c in CASES])
def test_expected_search_text(name, source, genuzot, original, expected):
    assert search_text(original, source, genuzot_marks=genuzot).text == expected


@pytest.mark.parametrize("name,source,genuzot,original,expected", CASES, ids=[c[0] for c in CASES])
def test_every_character_maps_back(name, source, genuzot, original, expected):
    st = search_text(original, source, genuzot_marks=genuzot)
    origs = st.originals()
    assert origs == sorted(origs) and len(set(origs)) == len(origs), "nothing is reordered or doubled"
    for i, ch in enumerate(st.text):
        assert ch == " " or ch == original[st.to_original(i)]


@pytest.mark.parametrize("name,source,genuzot,original,expected", CASES, ids=[c[0] for c in CASES])
def test_idempotent(name, source, genuzot, original, expected):
    once = search_text(original, source, genuzot_marks=genuzot).text
    assert search_text(once, source, genuzot_marks=genuzot).text == once


def test_a_match_maps_to_the_edition_with_its_signs():
    original = "01 קדשך שבעו(עו) עמים\n02 אדם אחזמר^ע^ת נבאו"
    st = search_text(original, SOURCE_FGP, genuzot_marks=True)
    start = st.text.index("שבעו עמים")
    a, b = st.span_to_original(start, start + len("שבעו עמים"))
    assert original[a:b] == "שבעו(עו) עמים"
    start = st.text.index("אחזמרעת")
    a, b = st.span_to_original(start, start + len("אחזמרעת"))
    assert original[a:b] == "אחזמר^ע^ת"


def test_span_from_original():
    original = "Recto\nשורה ראשונה\nVerso\nשורה שנייה"
    st = search_text(original, SOURCE_PGP)
    at = original.index("שורה שנייה")
    s, e = st.span_from_original(at, at + len("שורה שנייה"))
    assert st.text[s:e] == "שורה שנייה"
    assert st.span_from_original(0, len("Recto")) is None


def test_rule_counts_and_version():
    st = search_text("01 א\n02 ב\n03 ג", SOURCE_FGP)
    assert st.rules["numbered_texts"] == 1 and st.rules["line_numbers"] == 3
    assert isinstance(CLEANER_VERSION, int) and CLEANER_VERSION >= 1


def test_genuzot_marks_only_on_fgp():
    with pytest.raises(ValueError):
        search_text("x", SOURCE_PGP, genuzot_marks=True)
    with pytest.raises(ValueError):
        search_text("x", "other")

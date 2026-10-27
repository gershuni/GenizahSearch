"""The Genuzot team's legend of marks (GS-274 option A): who gets it, and what it shows."""

import html

from shared.fgp_marks_legend import PRAYER_MARKS, TALMUD_MARKS, labels, legend_html, uses_genuzot_marks

GENUZOT_EN = "FGP Transcriptions Team - Genuzos"
GENUZOT_HE = "צוות FGP להעתקות -גנוזות"


def _fgp(**kw):
    src = {"source": "fgp", "is_fgp": True, "doc_relation": "Digital Edition"}
    src.update(kw)
    return src


def test_a_genuzot_transcription_gets_the_legend():
    assert uses_genuzot_marks(_fgp(source_credit_en=GENUZOT_EN, source_credit_he=GENUZOT_HE))


def test_any_one_credit_column_is_enough():
    """An old sidecar has only the legacy Hebrew `source_credit`."""
    assert uses_genuzot_marks(_fgp(source_credit=GENUZOT_HE))
    assert uses_genuzot_marks(_fgp(source_credit_he=GENUZOT_HE))
    assert uses_genuzot_marks(_fgp(source_credit_en=GENUZOT_EN))


def test_the_per_row_credits_outrank_the_legacy_one():
    """Codex round 1: in the 2026-06 sidecar the legacy `source_credit` lists every team that
    touched the shelfmark. Row 38 is Yad Harav Herzog's, yet its legacy credit names Genuzot."""
    row_38 = _fgp(source_credit_en="Yad Harav Herzog - FGP sponsored team",
                  source_credit_he="יד הרב הרצוג - צוות FGP",
                  source_credit="Sussmann (Talmud) Catalog; " + GENUZOT_HE + "; Schwab Catalog")
    assert not uses_genuzot_marks(row_38)
    assert uses_genuzot_marks(_fgp(source_credit_en="", source_credit_he=None, source_credit=GENUZOT_HE))


def test_other_teams_translations_and_pgp_do_not():
    assert not uses_genuzot_marks(_fgp(source_credit_he="חיים מיליקובסקי, ראש צוות FGP למדרשי אגדה"))
    assert not uses_genuzot_marks(_fgp(source_credit_en=GENUZOT_EN, doc_relation="Digital Translation"))
    assert not uses_genuzot_marks({"source": "pgp", "doc_relation": "Digital Edition",
                                   "source_credit_en": GENUZOT_EN})
    assert not uses_genuzot_marks(_fgp())
    assert not uses_genuzot_marks(None)
    assert not uses_genuzot_marks({})


# Prayer and piyyut fragments: "סיגלה אקדמית" (2024, image-only PDF), transcribed form by form;
# "plain text" (its first row) has no mark.
LIST_PRAYER = ("(…)", "^…^", "|…|", "=…=", "[....]", "{…}", "{א//ב}", "((…))", "?…?", "?א/ב?", "%…%",
               "<…>", "*…*", "-…-", "$…$", "@…@", "«…»", "&…&", "¿…¿", "##", "#", "ב+", "°")
# Talmud fragments (Hachi Garsinan): the 2014 Word table, rows 3-27, academic column,
# then `@` around a letter, which the owner added (2026-10-27).
LIST_TALMUD = ("(…)", "[…]", "¦…¦", "(…){…}", "[2]…[1]…", "ᵔ", "ᵕ", "א֗", "א֯", "א֯/ב֯", "(א֯){ב֯}/(ב֯){א֯}",
               "....", "*…*", "א?", "§…§", "[1]", "?…?", "#", "##", "-א-", "<…>", "<~>", "<(…)>", "<<…>>", "²",
               "@א@")


def test_at_is_a_piyyut_in_the_prayer_list_and_a_letter_quirk_in_the_talmud_list():
    """Owner, 2026-10-27."""
    prayer = {m.form: m for m in PRAYER_MARKS}
    talmud = {m.form: m for m in TALMUD_MARKS}
    assert prayer["@…@"].he == "פיוט"
    assert talmud["@א@"].he == "חריגות באות"


# The Hebrew meanings, copied from the two sources by hand (Codex round 2: the
# tests compared the page with the module's own table, so a wrong meaning passed).
MEANINGS_PRAYER = ("טקסט מחוק", "בין השיטין", "תוספת בגליון או בין השיטין", "גליון", "קרוע או מטושטש", "תיקון",
                   "תיקון, כשלא ברור מה המקור ומה התיקון", "גרידה", "קריאה מסופקת", "אותיות מסופקות", "שומר דף",
                   "מילוי שורה", "כותרת", "מספר עמוד", "הלכות והוראות", "פיוט", "תוספות בנסיבות מיוחדות",
                   "ערבית יהודית", "שפות זרות", "נקודות מעל מילה", "נקודות מעל אות", "ברוך אתה ה'", "סימון לגליון")
MEANINGS_TALMUD = ("מחיקה", "גליון; בין השיטין; נוספה לאחר השלמת התיבה או השורה", "הוספה בתוך גליון או בין השיטין",
                   "תיקון בגוף כתב היד או על גרידה", "תיקון סדר המילים או סדר האותיות", "חיבור של שתי תיבות לאחת",
                   "הפרדה בין שתי תיבות", "קריאה כמעט ודאית", "קריאה מסופקת", "שתי אפשרויות לקריאה",
                   "תיקון, כשלא ברור מה תוקן למה", "קריעה או טשטוש",
                   "כל דבר שאינו שייך לגוף הטקסט של התלמוד או המשנה: כותרות, הדרן, סליק, קולופון, פירוש וכיוצא בזה",
                   "סימון לגליון", "נוסח אחר, רק שכתוב במפורש (נ\"א וכדומה)", "הערה",
                   "דברים לא מזוהים סביב המילה או האות", "קישוט מעל אות", "קישוט או קו מעל מילה", "מספור עמוד",
                   "שומר דף (<ראובן>) או מילוי שורה (<רא>)", "לא מוגדר",
                   "שומר דף או מילוי שורה השונים מהשורה או מהעמוד הבא", "תחילת השורה הבאה או העמוד הבא קרועה או איננה",
                   "סוף פסקה לא סטנדרטי", "חריגות באות")


def test_each_list_is_its_source_mark_for_mark_in_its_order():
    assert tuple(m.form for m in PRAYER_MARKS) == LIST_PRAYER
    assert tuple(m.form for m in TALMUD_MARKS) == LIST_TALMUD
    assert tuple(m.he for m in PRAYER_MARKS) == MEANINGS_PRAYER
    assert tuple(m.he for m in TALMUD_MARKS) == MEANINGS_TALMUD
    assert all(m.en for m in PRAYER_MARKS + TALMUD_MARKS)


def test_the_legend_is_the_two_headed_lists_and_nothing_else():
    """Owner, 2026-10-27: the two headings and their tables; no notes, no unexplained marks."""
    for lang in ("en", "he"):
        page = legend_html(lang)
        lab = labels(lang)
        assert page.index(html.escape(lab["prayer_heading"])) < page.index(html.escape(lab["talmud_heading"]))
        assert page.count("<table") == 2 and page.count("<h4") == 2
        assert "<p" not in page and "<ul" not in page
        for mark in PRAYER_MARKS + TALMUD_MARKS:
            assert html.escape(mark.form) in page, mark.form
            assert html.escape(mark.meaning(lang)) in page
        assert "<…>" not in page and "&…&" not in page, "a raw mark would be read as markup"


def test_each_language_has_its_own_frame():
    assert legend_html("he").startswith("<div dir='rtl'")
    assert legend_html("en").startswith("<div dir='ltr'")
    assert labels("he")["prayer_heading"] == "סיגלה צוות גנוזות - קטעי תפילה ופיוט"
    assert labels("he")["talmud_heading"] == "סיגלה צוות גנוזות - קטעי תלמוד"
    assert set(labels("he")) == set(labels("en"))

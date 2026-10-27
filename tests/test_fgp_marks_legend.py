"""The Genuzot team's legend of marks (GS-274 option A): who gets it, and what it shows."""

import html

from shared.fgp_marks_legend import (
    MARKS, NOT_IN_LIST, OLDER_LIST, labels, legend_html, not_in_list, uses_genuzot_marks,
)

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


def test_other_teams_translations_and_pgp_do_not():
    assert not uses_genuzot_marks(_fgp(source_credit_he="חיים מיליקובסקי, ראש צוות FGP למדרשי אגדה"))
    assert not uses_genuzot_marks(_fgp(source_credit_en=GENUZOT_EN, doc_relation="Digital Translation"))
    assert not uses_genuzot_marks({"source": "pgp", "doc_relation": "Digital Edition",
                                   "source_credit_en": GENUZOT_EN})
    assert not uses_genuzot_marks(_fgp())
    assert not uses_genuzot_marks(None)
    assert not uses_genuzot_marks({})


def test_the_legend_shows_every_mark_as_typed():
    """The marks are `<`, `&`, `$`: escaped, every one reaches the reader."""
    for lang in ("en", "he"):
        page = legend_html(lang)
        for mark in MARKS + OLDER_LIST:
            assert html.escape(mark.form) in page, mark.form
            assert html.escape(mark.meaning(lang)) in page
        for item in not_in_list(lang):
            assert html.escape(item) in page
        assert "<…>" not in page and "&…&" not in page, "a raw mark would be read as markup"


def test_each_language_has_its_own_frame():
    assert legend_html("he").startswith("<div dir='rtl'")
    assert legend_html("en").startswith("<div dir='ltr'")
    assert labels("he")["title"] == "סימני התעתוק"
    assert labels("en")["title"] == "Transcription marks"
    assert set(labels("he")) == set(labels("en"))
    assert len(not_in_list("he")) == len(NOT_IN_LIST)


def test_every_mark_is_listed_once():
    forms = [m.form for m in MARKS]
    assert len(forms) == len(set(forms))
    assert all(m.en and m.he for m in MARKS + OLDER_LIST)

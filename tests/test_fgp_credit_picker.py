"""pick_fgp_credit — language-appropriate FGP credit selection (2026-06-24).

HE UI prefers ``source_credit_he``, EN UI prefers ``source_credit_en``; either
falls back to the other, then to the legacy single-language ``source_credit``.
"""
from __future__ import annotations

from shared.fgp_service import pick_fgp_credit

_HE = "צוות FGP לספרות ההלכה בערבית-יהודית (דוד סקליר, ראש הצוות)"
_EN = "FGP Judeo-Arabic Halakhic Literature team (David Sklare, Head)"


def test_he_ui_prefers_hebrew():
    src = {"source_credit_he": _HE, "source_credit_en": _EN, "source_credit": "legacy"}
    assert pick_fgp_credit(src, "he") == _HE


def test_en_ui_prefers_english():
    src = {"source_credit_he": _HE, "source_credit_en": _EN, "source_credit": "legacy"}
    assert pick_fgp_credit(src, "en") == _EN


def test_he_falls_back_to_en_then_legacy():
    assert pick_fgp_credit({"source_credit_en": _EN}, "he") == _EN
    assert pick_fgp_credit({"source_credit": "legacy"}, "he") == "legacy"


def test_en_falls_back_to_he_then_legacy():
    assert pick_fgp_credit({"source_credit_he": _HE}, "en") == _HE
    assert pick_fgp_credit({"source_credit": "legacy"}, "en") == "legacy"


def test_none_when_no_credit():
    assert pick_fgp_credit({}, "en") is None
    assert pick_fgp_credit({}, "he") is None


def test_lang_variants_treated_as_hebrew():
    src = {"source_credit_he": _HE, "source_credit_en": _EN}
    for lang in ("he", "he-IL", "HE", "heb"):
        assert pick_fgp_credit(src, lang) == _HE


def test_unknown_lang_defaults_to_english_side():
    src = {"source_credit_he": _HE, "source_credit_en": _EN}
    assert pick_fgp_credit(src, "fr") == _EN
    assert pick_fgp_credit(src, "") == _EN

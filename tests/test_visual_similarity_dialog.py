from shared.genizah_translations import TRANSLATIONS
from web.components.visual_similarity_dialog import (
    _domain_label,
    _domain_options,
    _library_options,
    _normalize_view,
    _pick_preview_image_url,
    _suggestion_thumbnail_url,
)


def test_view_defaults_to_the_list():
    assert _normalize_view('grid') == 'grid'
    for stored in ('list', None, '', 'table', 1):
        assert _normalize_view(stored) == 'list'


def test_thumbnail_is_the_low_resolution_first_image():
    out = _suggestion_thumbnail_url(
        {"alma_id": "990051537270205171", "shelfmark": "T-S NS 158.112", "library_code": "CUL"}
    )

    assert out == "/api/nli_image_by_sysid/990051537270205171?page=0&width=300"


def test_oxford_thumbnail_does_not_use_the_nli_proxy():
    out = _suggestion_thumbnail_url(
        {"alma_id": "990001234560205171", "shelfmark": "MS Heb. e. 93/58", "library_code": "Oxford"}
    )

    assert out and "nli_image_by_sysid" not in out


def test_thumbnails_label_has_hebrew():
    assert TRANSLATIONS.get("Thumbnails")


def test_pick_preview_image_url_prefers_jts_proxy():
    out = _pick_preview_image_url(
        "990053158490205171",
        shelfmark="ENA 2229.14",
        library_code="JTS",
        cached={
            "external_provider": "jts",
            "images_ext": [{"url": "https://figgy.example/canvas/1"}],
        },
    )

    assert out == "/api/jts_image/990053158490205171?page=0&width=600"


def test_pick_preview_image_url_prefers_manchester_proxy():
    out = _pick_preview_image_url(
        "990053737520205171",
        shelfmark="Ms. B 8193",
        library_code="Manchester",
        cached={
            "external_provider": "manchester",
            "images_ext": [{"url": "https://luna.example/canvas/1"}],
        },
    )

    assert out == "/api/manchester_image/990053737520205171?page=0&width=600"


def test_pick_preview_image_url_uses_cambridge_proxy_when_aligned():
    out = _pick_preview_image_url(
        "990051537270205171",
        shelfmark="T-S NS 158.112",
        library_code="CUL",
        cached={
            "external_provider": "cambridge",
            "images_ext": [{"url": "https://cudl.example/canvas/1"}],
            "cambridge_alignment": {"verdict": "aligned"},
        },
    )

    assert out == "/api/cambridge_image/990051537270205171?page=0&width=600"


def test_pick_preview_image_url_falls_back_to_nli_when_cambridge_misaligned():
    out = _pick_preview_image_url(
        "990051537270205171",
        shelfmark="T-S NS 158.112",
        library_code="CUL",
        cached={
            "external_provider": "cambridge",
            "images_ext": [{"url": "https://cudl.example/canvas/1"}],
            "cambridge_alignment": {"verdict": "misaligned"},
        },
    )

    assert out == "/api/nli_image_by_sysid/990051537270205171?page=0&width=600"


def test_pick_preview_image_url_prefers_oxford_thumb_url_when_available():
    out = _pick_preview_image_url(
        "990001234560205171",
        shelfmark="MS Heb. e. 93/58",
        library_code="Oxford",
        cached={"images_ext": [{"thumb_url": "https://bodleian.example/thumb.jpg"}]},
    )

    assert out == "https://bodleian.example/thumb.jpg"


def test_pick_preview_image_url_builds_direct_oxford_image_without_thumb():
    out = _pick_preview_image_url(
        "990001234560205171",
        shelfmark="MS Heb. e. 93/58",
        library_code="Oxford",
        cached={},
    )

    assert out == "https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_e_93_58a.jpg"


def test_domain_label_is_hebrew_only_in_the_hebrew_ui():
    s = {"domain": "Derashot", "domain_heb": "דרשות"}

    assert _domain_label(s, is_heb=True) == "דרשות"
    assert _domain_label(s, is_heb=False) == "Derashot"


def test_domain_label_falls_back_to_english_without_a_hebrew_name():
    assert _domain_label({"domain": "Derashot", "domain_heb": ""}, is_heb=True) == "Derashot"
    assert _domain_label({"domain": "--"}, is_heb=True) == "--"


def test_domain_options_count_each_domain_and_skip_unclassified():
    suggestions = [
        {"domain": "Liturgy", "domain_heb": "תפילה"},
        {"domain": "Derashot", "domain_heb": "דרשות"},
        {"domain": "Liturgy", "domain_heb": "תפילה"},
        {"domain": "--", "domain_heb": ""},
    ]

    assert list(_domain_options(suggestions, is_heb=False).items()) == [
        ("Derashot", "Derashot (1)"), ("Liturgy", "Liturgy (2)"),
    ]
    assert list(_domain_options(suggestions, is_heb=True).items()) == [
        ("Derashot", "דרשות (1)"), ("Liturgy", "תפילה (2)"),
    ]


def test_library_options_count_each_library_in_code_order():
    suggestions = [
        {"library_code": "JTS"},
        {"library_code": "CUL"},
        {"library_code": "CUL"},
        {"library_code": ""},
        {},
    ]

    assert list(_library_options(suggestions).items()) == [
        ("CUL", "CUL (2)"), ("JTS", "JTS (1)"),
    ]

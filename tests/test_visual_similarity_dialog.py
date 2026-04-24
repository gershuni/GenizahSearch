from web.components.visual_similarity_dialog import _pick_preview_image_url


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

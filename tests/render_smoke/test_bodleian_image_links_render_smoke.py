"""The Oxford image header opens each recorded side and its parent volume."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from nicegui import ui

from shared.codicological import CodicologicalManager
from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE, SID, _all_elements, _run_browse, _viewer_image_html, _wait_until,
)
from web.state import state


@pytest.mark.parametrize("shelfmark,part,volume,recto,verso", [
    ("MS heb. g.2/27", "MS. Heb. g. 2", "215", "MS_HEB_g_2_27a.jpg", "MS_HEB_g_2_27b.jpg"),
    ("MS heb. a.4/1", "MS. Heb. a. 4", "6", "MS_HEB_a_4_1a.jpg", None),
    ("MS heb. f.31/14", None, "188", None, None),
])
def test_browse_enriches_and_renders_the_bodleian_links(shelfmark, part, volume, recto, verso):
    manager = CodicologicalManager()
    assert manager.load({SID: {"shelfmark": shelfmark, "oxford_part_id": part}})
    metadata = SimpleNamespace(
        codico_mgr=manager, get_part_for_folio=manager.get_part_for_folio,
        get_part_metadata=manager.get_part_metadata, get_meta_for_id=lambda _: (shelfmark, ""),
        get_library_for_id=lambda _: "Oxford",
    )
    page = replace(OPEN_PAGE, library_code="Oxford", shelfmark=shelfmark, is_oxford=True,
                   is_cambridge=False, external_url=None, p_num=2, total_pages=2, fl_id="FL12345",
                   image_source_info={"nli_fgp": True})
    site = "https://hebrew.bodleian.ox.ac.uk/"
    expected = {"volume": site + "catalog/volume_" + volume}
    if recto:
        expected["recto"] = site + "fragments/full/" + recto
    if verso:
        expected["verso"] = site + "fragments/full/" + verso

    async def driver(user):
        await user.open(f"/browse?sys_id={SID}")

        def destinations():
            return {e.props["data-role"].removeprefix("bodleian-"): e.props["href"]
                    for e in _all_elements(user) if isinstance(e, ui.link)
                    and e.props.get("data-role", "").startswith("bodleian-") and e.props["target"] == "_blank"}

        await _wait_until(lambda: destinations() == expected)
        assert destinations() == expected
        assert page.external_url == (manager.get_part_metadata(part).get("direct_link") or "")
        image = _viewer_image_html(user)
        assert 'data-nli-fl="12345"' in image
        if verso:
            assert expected["verso"] in image
        else:
            assert f"/api/nli_image_by_sysid/{SID}" in image
            assert "handleImageError(this, '" + SID + "', 1, false" in image

    # A pre-upgrade cached part link must not override the fresh local catalogue,
    # including a record whose only surviving target is the volume itself.
    old = {"cambridge_images": [], "external_provider": "", "cambridge_alignment": None,
           "external_url": site + "catalog/volume_999"}
    with patch.object(state, "meta_mgr", metadata), patch(
            "web.components.image_resolution.resolve_external_images", return_value=old):
        _run_browse(page, driver)

"""Oxford reader links use the catalogue's physical folio and full image URLs."""

import pytest

from shared.codicological import CodicologicalManager, bodleian_cache_needs_refresh, get_bodleian_links


@pytest.fixture(scope="module")
def catalogue():
    manager = CodicologicalManager()
    assert manager.load()
    return manager


def links(catalogue, shelfmark, **page):
    part_id = catalogue._resolve_part_by_folio_range(shelfmark)
    return get_bodleian_links(catalogue.get_part_metadata(part_id), shelfmark, **page)


SITE = "https://hebrew.bodleian.ox.ac.uk/"


def test_links_address_the_leaf_and_unanchored_volume(catalogue):
    result = links(catalogue, "MS heb. d.50/19", page_num=2)
    assert result == {
        "volume": SITE + "catalog/volume_75",
        "recto": SITE + "fragments/full/MS_HEB_d_50_19a.jpg",
        "verso": SITE + "fragments/full/MS_HEB_d_50_19b.jpg",
        "current": SITE + "fragments/full/MS_HEB_d_50_19b.jpg",
    }


def test_second_ie_uses_its_own_leaf_even_with_primary_crossref(catalogue):
    crossref = [{"folio_label": "1r", "image_source_name": "MS_HEB_d_50_19a"},
                {"folio_label": "1v", "image_source_name": "MS_HEB_d_50_19b"}]
    result = links(catalogue, "MS heb. d.50/19", folio_offset=1, total_pages=2,
                   page_num=2, folio_images=crossref)
    assert result["current"].endswith("MS_HEB_d_50_20b.jpg")


def test_ordering_and_filename_case_are_preserved(catalogue):
    # Volume 1 starts with 2b in the catalogue. Position zero is not 1a.
    result = links(catalogue, "MS georg. c.1(P)/1")
    assert result["recto"].endswith("MS_Georg_c_1(P)_1a.jpg")
    assert result["verso"].endswith("MS_Georg_c_1(P)_1b.jpg")


def test_unavailable_verso_is_not_fabricated(catalogue):
    result = links(catalogue, "MS heb. a.4/1", page_num=2)
    assert set(result) == {"recto", "volume"}


def test_unlisted_current_image_retains_crossref_confirmed_legacy_link(catalogue):
    crossref = [{"folio_label": "1r", "image_source_name": "MS_HEB_f_21_21a"},
                {"folio_label": "1v", "image_source_name": "MS_HEB_f_21_21b"}]
    result = links(catalogue, "MS heb. f.21/21", total_pages=2, page_num=2, folio_images=crossref)
    assert set(result) == {"volume", "current"}
    assert result["current"].endswith("MS_HEB_f_21_21b.jpg")


def test_leaf_qualifier_is_distinct_from_the_side(catalogue):
    result = links(catalogue, "MS heb. b.12/13a")
    assert result["verso"].endswith("13(a)b.jpg")
    view = links(catalogue, "MS heb. b.12/13a", folio_num="13(b)")
    assert view["verso"].endswith("13(b)b.jpg")


def test_more_than_two_pages_match_original_filenames(catalogue):
    # Real crossref for 990053436560205171. The second leaf is 55(b).
    crossref = [
        {"folio_label": position, "image_source_name": "MS_HEB_e_40_" + label}
        for position, label in zip(("1r", "1v", "2r", "2v"),
                                   ("55(a)a", "55(a)b", "55(b)a", "55(b)b"))
    ]
    result = links(catalogue, "MS heb. e.40/55", total_pages=4, page_num=3, folio_images=crossref)
    assert result["current"] == result["recto"]
    assert result["verso"].endswith("55(b)b.jpg")
    assert set(links(catalogue, "MS heb. e.40/55", total_pages=4, page_num=3)) == {"volume"}
    crossref[2]["folio_label"] = "1r"
    assert set(links(catalogue, "MS heb. e.40/55", total_pages=4, page_num=3,
                     folio_images=crossref)) == {"volume"}


def test_volume_survives_a_missing_part_and_never_prefix_matches(catalogue):
    assert catalogue.get_bodleian_volume_url("MS heb. f.31/14") == SITE + "catalog/volume_188"
    # Volume f.3 is absent. Before exact matching, its folio 19 matched f.30/4.
    assert catalogue._resolve_part_by_folio_range("MS heb. f.3/19") is None
    assert catalogue.get_bodleian_volume_url("MS heb. f.3/19") == ""
    assert get_bodleian_links({}, "MS heb. f.3/19") == {}


def test_cached_wrong_oxford_part_loses_its_images_and_credit(catalogue, monkeypatch):
    from shared import metadata_manager
    from shared.cudl_identity import RESOLVER_REVISION, fresh_external_entry

    sid = "990053460440205171"
    manager = metadata_manager.MetadataManager.__new__(metadata_manager.MetadataManager)
    manager.codico_mgr = catalogue
    manager.csv_bank = {sid: {"library_code": "Oxford", "shelfmark": "MS heb. f.3/19", "title": "Record title"}}
    old_part = catalogue.get_part_metadata("MS. Heb. f. 30/4")
    manager.nli_cache = {sid: {
        "cambridge_resolution": {"revision": RESOLVER_REVISION}, "oxford_part_id": "MS. Heb. f. 30/4",
        "oxford_part_metadata": old_part, "title": old_part["title"], "external_url": old_part["direct_link"],
        "thumb_url": old_part["images"][0]["thumb_url"], "images_ext": old_part["images"],
        "attribution": metadata_manager.OXFORD_IMAGE_CREDIT_EN,
    }}
    monkeypatch.setattr(metadata_manager, "_get_crossref_service", lambda: None)
    monkeypatch.setattr(metadata_manager, "_get_fjms_service", lambda: None)
    monkeypatch.setattr(manager, "fetch_marc_data", lambda _: {})
    monkeypatch.setattr(manager, "fetch_iiif_manifest", lambda *_: {"attribution": "NLI", "canvas_map": {"2": "1r"}})
    assert bodleian_cache_needs_refresh(manager, sid, manager.nli_cache[sid])
    fresh = fresh_external_entry(manager, sid)
    assert fresh["images_ext"] == [] and fresh["images"] == fresh["images_nli"]
    assert fresh["attribution"] == "NLI" and fresh["title"] == "Record title"
    assert not {"oxford_part_id", "oxford_part_metadata", "external_url", "thumb_url"}.intersection(fresh)
    assert not bodleian_cache_needs_refresh(manager, sid, fresh)


def test_only_old_oxford_volume_entries_need_refresh(catalogue):
    from types import SimpleNamespace

    manager = SimpleNamespace(codico_mgr=catalogue,
                              csv_bank={"S": {"library_code": "Oxford", "shelfmark": "MS heb. f.31/14"}})
    assert bodleian_cache_needs_refresh(manager, "S", {})
    assert not bodleian_cache_needs_refresh(manager, "S", {"oxford_part_metadata": {
        "volume_url": SITE + "catalog/volume_188",
    }})
    manager.codico_mgr = CodicologicalManager()  # background catalogue load has not finished
    assert bodleian_cache_needs_refresh(manager, "S", {"oxford_part_id": "MS. Heb. f. 30/4"})
    assert not bodleian_cache_needs_refresh(manager, "S", {"oxford_part_id": "MS. Heb. f. 31/5"})
    manager.csv_bank["S"]["library_code"] = "CUL"
    assert not bodleian_cache_needs_refresh(manager, "S", {})

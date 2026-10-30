"""Structural CUDL identity (shared/cudl_identity.py) and the one Cambridge decision.

Reported 2026-10-29: T-S Misc.36.180 had no Cambridge link on the desktop (the cached
NLI name "Ms. T-S Loan Collection 180" was the only name tried); T-S Ar.18(1).6 and
Or.1080 3.45 had none anywhere (the lossy normalized key differs on the two sides);
T-S NS 318.41 opened T-S NS 318.4.1 (two items, one key); AIU and JTS records opened
Mosseri items. Every name/label below is a real CUDL item unless marked CONSTRUCTED.

The fixture table is built with the importer's own key
(``normalize_shelfmark(cudl_label_to_shelfmark(label))``), so the lookup is tested
against the key production carries.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

import shared.metadata_manager as mm_mod
from shared.browse_map_utils import normalize_shelfmark
from shared.cudl_identity import (
    RESOLVER_REVISION,
    candidate_key,
    cudl_label_to_shelfmark,
    entry_decision_stale,
    fresh_external_entry,
    label_signature,
    name_signature,
    resolution_is_current,
)
from shared.metadata_manager import MetadataManager
from shared.nli_crossref_service import NliCrossrefService

REPO = Path(__file__).resolve().parent.parent

REAL_LABELS = [
    "MS-TS-MISC-00036-00180", "MS-TS-AR-00018-00001-00006", "MS-OR-01080-00003-00045",
    # Both sides of every normalized-key collision among CUL items:
    "MS-TS-NS-00318-00041", "MS-TS-NS-00318-00004-00001",
    "MS-TS-NS-00318-00042", "MS-TS-NS-00318-00004-00002",
    "MS-TS-AR-00007-00031", "MS-TS-AR-00007-00003-00001",
    "MS-OR-01080-00013-00031", "MS-OR-01080-00013-00003-00001",
    # Lossy partners whose own item is NOT in CUDL's list:
    "MS-TS-AR-00048-00211", "MS-MOSSERI-X-00021",
    # Other libraries' shelfmarks spell these:
    "MS-MOSSERI-II-00178", "MS-MOSSERI-X-00037",
    # Same letters and numbers, different order (T-S F 6.1 / T-S 6F1):
    "MS-TS-F-00006-00001", "MS-TS-00006-F-00001",
    "MS-TS-00008-J-A-00002", "MS-TS-NS-00318-00044-B", "MS-MOSSERI-V-00013-00001-K",
    "MS-MOSSERI-III-00013-B", "MS-ADD-00863-00002", "MS-TS-C-00002-00038",
    "MS-OR-01081-00002-00051", "MS-TS-K-00025-00240", "MS-TS-F-00001-00001-00114",
    "MS-TS-MISC-00001-00131-00001", "MS-MOSSERI-II-00259-00004", "MS-MOSSERI-II-00259-00005",
    "MS-TS-NS-J-MINUTE-FRAGMENTS", "MS-TS-00006-JA-00001",
    # Phase 84's CUDL-form classmarks (slash + leading zero, Mosseri label, Or. J):
    "MS-TS-F-00008-00002", "MS-MOSSERI-III-00027-A", "MS-OR-01080-J-00015",
]
# CONSTRUCTED (not a CUDL item): a second label with the structure of MS-TS-00006-JA-00001,
# so one name has two candidates. No such pair exists in CUDL's list today.
CONSTRUCTED_TWIN = "MS-TS-00006-J-A-00001"

NLI_COLUMNS = [
    "LibraryNameEng", "LibraryAbbrev", "LibraryCity", "LibraryNameHeb",
    "CollectionName", "Shelfmark", "InventoryId", "OBBox", "OBVolume",
    "OBFolio", "NLI_AlmaId", "CatalogAbbrev", "CatalogEntry",
    "FGPImageNumberId", "FGPNumber", "ImageName", "ImageSourceName",
    "PartOf", "See", "BifolioWith", "NumFolio", "NumBifolio",
    "Material", "Size", "IsNotGenizah",
]
SID_318_41 = "990051109460205171"
SID_MISC = "990051382160205171"
SID_JTS = "990053345070205171"


def _build_db(path: Path, labels) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE cambridge_manifests (label TEXT NOT NULL, "
                 "manifest_url TEXT NOT NULL, normalized_shelfmark TEXT)")
    for label in labels:
        conn.execute("INSERT INTO cambridge_manifests VALUES (?, ?, ?)", (
            label, f"http://cudl.lib.cam.ac.uk/iiif/{label}",
            normalize_shelfmark(cudl_label_to_shelfmark(label))))
    conn.execute("CREATE INDEX idx_cam_shelfmark ON cambridge_manifests(normalized_shelfmark)")
    conn.execute("CREATE INDEX idx_cam_label ON cambridge_manifests(label)")
    conn.execute(f"CREATE TABLE nli_images ({', '.join(c + ' TEXT' for c in NLI_COLUMNS)})")
    for sid, shelf in ((SID_318_41, "T-S NS 318.41"), (SID_MISC, "T-S Misc.36.180")):
        row = {c: "" for c in NLI_COLUMNS}
        row.update(LibraryNameEng="Cambridge University Library", LibraryAbbrev="CUL",
                   Shelfmark=shelf, NLI_AlmaId=sid, FGPImageNumberId="1", ImageName="1r")
        conn.execute(f"INSERT INTO nli_images VALUES ({', '.join('?' * len(NLI_COLUMNS))})",
                     [row[c] for c in NLI_COLUMNS])
    conn.commit()
    conn.close()


@pytest.fixture
def svc(tmp_path):
    db = tmp_path / "nli_crossref.db"
    _build_db(db, REAL_LABELS)
    return NliCrossrefService(db_path=str(db))


@pytest.fixture
def svc_twin(tmp_path):
    db = tmp_path / "nli_crossref_twin.db"
    _build_db(db, REAL_LABELS + [CONSTRUCTED_TWIN])
    return NliCrossrefService(db_path=str(db))


def _label(res):
    return res["label"] if res["status"] == "match" else None


# --- test_cambridge_structural_identity -------------------------------------------------

@pytest.mark.parametrize("names, lib, expected", [
    (["T-S Ar.18(1).6"], "CUL", "MS-TS-AR-00018-00001-00006"),            # report 2
    (["Or.1080 3.45", "Ms. Or. 1080 3.45"], "CUL", "MS-OR-01080-00003-00045"),  # report 3
    (["T-S NS 318.41"], "CUL", "MS-TS-NS-00318-00041"),                   # was 318.4.1
    (["T-S NS 318.4.1"], "CUL", "MS-TS-NS-00318-00004-00001"),
    (["T-S Ar.7.31"], "CUL", "MS-TS-AR-00007-00031"),                     # was 7.3.1
    (["Or.1080 13.31"], "CUL", "MS-OR-01080-00013-00031"),
    (["Or.1080 13.3.1"], "CUL", "MS-OR-01080-00013-00003-00001"),
    (["T-S F 6.1"], "CUL", "MS-TS-F-00006-00001"),                        # order matters
    (["T-S 6F1"], "CUL", "MS-TS-00006-F-00001"),
    (["T-S 8Ja2"], "CUL", "MS-TS-00008-J-A-00002"),                       # Ja == J-A
    (["T-S NS 318.44b"], "CUL", "MS-TS-NS-00318-00044-B"),
    (["Add. 863, 2"], "CUL", "MS-ADD-00863-00002"),
    (["T-S C 2.038"], "CUL", "MS-TS-C-00002-00038"),                      # zero padding
    (["T-S F1(1).114"], "CUL", "MS-TS-F-00001-00001-00114"),
    (["T-S Misc.1.131(1)"], "CUL", "MS-TS-MISC-00001-00131-00001"),
    (["Cambridge University Library Ms. T-S Misc. 36.180"], "CUL", "MS-TS-MISC-00036-00180"),
    (["T-S Boxes: T-S K25.240"], "CUL", "MS-TS-K-00025-00240"),          # synthetic row
    (["T-S NS J.minute fragments"], "CUL", "MS-TS-NS-J-MINUTE-FRAGMENTS"),  # no numbers
    (["Ms. T-S F 8/002"], "CUL", "MS-TS-F-00008-00002"),
    (["Or. 1080 J 15"], "CUL", "MS-OR-01080-J-00015"),
    (["Ms. III 27A"], "Mosseri", "MS-MOSSERI-III-00027-A"),
    (["Moss. V,13.1K"], "Mosseri", "MS-MOSSERI-V-00013-00001-K"),
    (["Mosseri, Jacques Ms. III 13B"], "Mosseri", "MS-MOSSERI-III-00013-B"),
])
def test_cambridge_structural_identity(svc, names, lib, expected):
    assert _label(svc.resolve_cambridge_record(names, lib)) == expected


@pytest.mark.parametrize("name", [
    "T-S NS 318.41β",      # a character we cannot read: the name names nothing
    "T-S NS 318.4+1",
    "T-S 12.186-188",      # a numeric range is not one item
])
def test_unreadable_names_name_nothing(svc, name):
    assert svc.resolve_cambridge_record([name], "CUL")["status"] == "none"


def test_order_of_letters_and_numbers_is_part_of_identity():
    assert name_signature("T-S F 6.1", "CUL") != name_signature("T-S 6F1", "CUL")
    assert name_signature("T-S F 6.1", "CUL") == label_signature("MS-TS-F-00006-00001")
    assert name_signature("T-S 6F1", "CUL") == label_signature("MS-TS-00006-F-00001")


# --- test_cambridge_unique_lossy_match_is_not_identity ----------------------------------

@pytest.mark.parametrize("names, lib", [
    (["T-S Ar.48.2.11"], "CUL"),        # only Ar.48.211 is in the table: not this item
    (["Moss. X,2.1"], "Mosseri"),       # only X.21 is in the table: not this item
])
def test_cambridge_unique_lossy_match_is_not_identity(svc, names, lib):
    res = svc.resolve_cambridge_record(names, lib)
    assert res["status"] == "none" and res["label"] is None
    assert svc.get_cambridge_manifest_with_bridge(names[0]) is None


def test_one_name_two_items_is_refused_unless_the_catalogue_link_picks_one(svc_twin):
    assert svc_twin.resolve_cambridge_record(["T-S 6Ja1"], "CUL")["status"] == "ambiguous"
    res = svc_twin.resolve_cambridge_record(
        ["T-S 6Ja1"], "CUL", marc_url="https://cudl.lib.cam.ac.uk/view/MS-TS-00006-JA-00001/1")
    assert (res["status"], res["label"], res["source"]) == ("match", "MS-TS-00006-JA-00001", "marc")


# --- library provenance --------------------------------------------------------------------

@pytest.mark.parametrize("name, lib", [("Ms. X 37", "AIU"), ("Ms. II 178", "JTS")])
def test_other_libraries_records_are_never_cambridge(svc, name, lib):
    res = svc.resolve_cambridge_record([name], lib,
                                       marc_url="https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-X-00037")
    assert res["status"] == "not_cambridge" and res["label"] is None
    assert resolution_is_current(res)  # final: needs no table


# --- test_cambridge_all_names_exact_before_fallback ----------------------------------------

def test_main_name_decides_and_other_names_are_kept_as_alternatives(svc):
    res = svc.resolve_cambridge_record(["T-S C2.38", "Ms. Or. 1081 2.51"], "CUL")
    assert res["label"] == "MS-TS-C-00002-00038"
    assert [a["label"] for a in res["alternatives"]] == ["MS-OR-01081-00002-00051"]


def test_other_names_decide_when_the_main_name_names_nothing(svc):
    # Report 1, the desktop's cached name first: the catalogue's other name still finds it.
    res = svc.resolve_cambridge_record(["Ms. T-S Loan Collection 180", "T-S Misc.36.180"], "CUL")
    assert _label(res) == "MS-TS-MISC-00036-00180" and res["matched_name"] == "T-S Misc.36.180"


def test_several_items_link_the_first_and_flag_it(svc):
    res = svc.resolve_cambridge_record(
        ["Ms. P 59 II", "Moss. II,259.4", "Moss. II,259.5"], "Mosseri")
    assert _label(res) == "MS-MOSSERI-II-00259-00004" and res["several_items"] is True
    assert [a["label"] for a in res["alternatives"]] == ["MS-MOSSERI-II-00259-00005"]


def test_second_series_heading_is_not_read_as_first_series(svc):
    assert svc.resolve_cambridge_record(["2nd Series: V 8"], "Mosseri")["status"] == "none"


# --- the NLI catalogue's link ---------------------------------------------------------------

def test_catalogue_link_stands_when_the_names_name_nothing(svc):
    res = svc.resolve_cambridge_record(
        ["T-S Loan Collection 180"], "CUL",
        marc_url="http://cudl.lib.cam.ac.uk/iiif/MS-TS-MISC-00036-00180")
    assert (res["status"], res["label"], res["source"]) == ("match", "MS-TS-MISC-00036-00180", "marc")


def test_catalogue_link_never_overrides_a_names_match(svc):
    res = svc.resolve_cambridge_record(
        ["T-S NS 318.41"], "CUL",
        marc_url="https://cudl.lib.cam.ac.uk/view/MS-TS-NS-00318-00004-00001")
    assert res["label"] == "MS-TS-NS-00318-00041" and res["source"] == "name"
    assert res["marc_label"] == "MS-TS-NS-00318-00004-00001"  # the disagreement stays visible


# --- the key invariant the lookup relies on --------------------------------------------------

def test_candidate_key_is_the_importers_key_for_every_cudl_label():
    """Two labels with one structure share a key; a name's key is the key of the labels it
    can match. Checked over CUDL's whole list, so no item is unreachable by the lookup."""
    labels = [m["label"] for m in json.loads(
        (REPO / "cambridge_genizah.json").read_text(encoding="utf-8"))["manifests"]]
    bad = [lab for lab in labels
           if label_signature(lab) is None
           or candidate_key(label_signature(lab)) != normalize_shelfmark(cudl_label_to_shelfmark(lab))]
    assert not bad, bad[:10]


# --- consumers: availability flag, library link ---------------------------------------------

def test_availability_flag_is_the_decision_not_a_table_probe(svc):
    assert svc.get_image_sources(SID_318_41, normalized_shelfmark="tsns318.41")["cambridge"] is False
    assert svc.get_image_sources(SID_318_41, cambridge=True)["cambridge"] is True


def test_library_link_is_the_item_when_the_decision_named_one(svc):
    assert svc.get_library_viewer_url(SID_MISC)["url"].startswith("https://cudl.lib.cam.ac.uk/search?")
    item = "https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00036-00180"
    assert svc.get_library_viewer_url(SID_MISC, cambridge_viewer_url=item)["url"] == item


# --- test_cambridge_consumer_identity_agreement + test_cambridge_cached_resolution_upgrade --

def _manager(cached, csv_rows, loaded=True):
    mm = MetadataManager.__new__(MetadataManager)
    mm.nli_cache = dict(cached)
    mm.csv_bank = dict(csv_rows)
    mm._csv_bank_loaded = loaded
    return mm


def _enrich(mm, sid, svc, marc=None):
    def _ext(self, url):
        label = url.rstrip("/").rsplit("/", 1)[-1]
        return {"canvases": [{"label": "1r", "url": f"img:{label}", "folio_num": 1, "folio_side": "r"}],
                "metadata": {}, "attribution": f"credit:{label}"}

    with patch.object(MetadataManager, "fetch_marc_data", lambda self, s: dict(marc or {})), \
         patch.object(MetadataManager, "fetch_iiif_manifest", lambda self, s, suffix=1: {}), \
         patch.object(MetadataManager, "fetch_external_iiif_data", _ext), \
         patch.object(MetadataManager, "get_part_for_folio", lambda self, s: None), \
         patch.object(mm_mod, "_get_crossref_service", lambda: svc), \
         patch.object(mm_mod, "_get_fjms_service", lambda: None):
        return mm.enrich_metadata(sid)


def test_a_stale_wrong_cambridge_entry_is_replaced_whole(svc):
    wrong = "MS-TS-NS-00318-00004-00001"
    stale = {"shelfmark": "T-S NS 318.41", "external_provider": "cambridge",
             "external_url": f"http://cudl.lib.cam.ac.uk/view/{wrong}",
             "images_ext": [{"label": "1r", "url": f"img:{wrong}"}],
             "cambridge_alignment": {"verdict": "aligned"}, "attribution": f"credit:{wrong}"}
    mm = _manager({SID_318_41: stale}, {SID_318_41: {
        "shelfmark": "T-S NS 318.41", "library_code": "CUL",
        "call_numbers_raw": ["Ms. T-S NS 318.41", "T-S NS 318.41"]}})
    assert entry_decision_stale(mm, mm.nli_cache[SID_318_41])

    meta = _enrich(mm, SID_318_41, svc)
    right = "MS-TS-NS-00318-00041"
    assert meta["external_url"] == f"https://cudl.lib.cam.ac.uk/view/{right}"
    assert meta["external_provider"] == "cambridge"
    assert [i["url"] for i in meta["images_ext"]] == [f"img:{right}"]
    assert meta["attribution"] == f"credit:{right}"
    assert "cambridge_alignment" not in meta  # the old item's verdict is gone
    assert meta["image_source_info"]["cambridge"] is True
    assert meta["library_viewer_url"]["url"] == f"https://cudl.lib.cam.ac.uk/view/{right}"
    assert meta["cambridge_resolution"]["revision"] == RESOLVER_REVISION
    assert not entry_decision_stale(mm, mm.nli_cache[SID_318_41])


def test_catalogue_name_replaces_a_cached_nli_name(svc):
    """Report 1: the cached NLI name hid the CUDL item; the catalogue's name finds it."""
    mm = _manager({SID_MISC: {"shelfmark": "Ms. T-S Loan Collection 180"}}, {SID_MISC: {
        "shelfmark": "T-S Misc.36.180", "library_code": "CUL",
        "call_numbers_raw": ["Ms. T-S Loan Collection 180", "Ms. T-S Misc. 36.180", "T-S Misc.36.180"]}})
    meta = _enrich(mm, SID_MISC, svc)
    assert meta["shelfmark"] == "T-S Misc.36.180"
    assert meta["external_url"] == "https://cudl.lib.cam.ac.uk/view/MS-TS-MISC-00036-00180"


def test_another_librarys_record_drops_a_cached_cambridge_link(svc):
    stale = {"shelfmark": "Ms. II 178", "external_provider": "cambridge",
             "external_url": "http://cudl.lib.cam.ac.uk/view/MS-MOSSERI-II-00178",
             "images_ext": [{"label": "1", "url": "img:MS-MOSSERI-II-00178"}],
             "attribution": "credit:MS-MOSSERI-II-00178"}
    mm = _manager({SID_JTS: stale}, {SID_JTS: {
        "shelfmark": "Ms. II 178", "library_code": "JTS", "call_numbers_raw": ["Ms. II 178"]}})
    meta = _enrich(mm, SID_JTS, svc,
                   marc={"external_iiif_link": "https://cudl.lib.cam.ac.uk/view/MS-MOSSERI-II-00178"})
    assert meta.get("external_provider") != "cambridge"
    assert "cudl.lib.cam.ac.uk" not in (meta.get("external_url") or "")
    assert not any("MOSSERI" in i["url"] for i in meta.get("images_ext") or [])
    assert "MOSSERI" not in (meta.get("attribution") or "")  # not the Mosseri item's credit
    assert meta["cambridge_resolution"]["status"] == "not_cambridge"


def test_a_catalogue_still_loading_is_not_a_final_no(svc):
    mm = _manager({}, {}, loaded=False)
    meta = _enrich(mm, SID_318_41, svc)
    assert meta["cambridge_resolution"]["status"] == "unavailable"
    assert meta["cambridge_resolution"]["revision"] is None
    assert entry_decision_stale(mm, mm.nli_cache[SID_318_41])  # the next request decides again


def test_image_shortcut_reenriches_an_entry_cached_before_the_decision():
    from web.components.image_resolution import resolve_external_images

    class _MM:
        supports_cambridge_decision = True

        def __init__(self):
            self.nli_cache = {"S": {"images_ext": [{"url": "img:old"}], "external_provider": "cambridge"}}
            self.calls = 0

        def enrich_metadata(self, sid):
            self.calls += 1
            self.nli_cache[sid] = {"images_ext": [{"url": "img:new"}], "external_provider": "cambridge",
                                   "cambridge_resolution": {"revision": RESOLVER_REVISION, "status": "match"}}

    mm = _MM()
    assert resolve_external_images("S", meta_mgr=mm)["cambridge_images"] == [{"url": "img:new"}]
    assert resolve_external_images("S", meta_mgr=mm)["cambridge_images"] == [{"url": "img:new"}]
    assert mm.calls == 1  # current afterwards: the cache serves it


# --- nothing else links a record to CUDL ----------------------------------------------------

def test_only_the_resolver_reads_the_catalogues_cudl_link():
    """MARC 856 is folded into the record's one decision in enrich_metadata. A surface
    that reads it directly can bring back a link the decision refused."""
    offenders = []
    for path in [REPO / "genizah_app.py", *(REPO / "desktop").rglob("*.py"),
                 *(REPO / "web").rglob("*.py"), *(REPO / "shared").rglob("*.py")]:
        if path.name == "metadata_manager.py":
            continue
        if "external_iiif_link" in path.read_text(encoding="utf-8", errors="replace"):
            offenders.append(str(path.relative_to(REPO)))
    assert not offenders, offenders


def test_browse_builds_no_cudl_url_from_a_shelfmark():
    src = (REPO / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
    assert "shelfmark_to_cudl_label" not in src
    assert "cudl.lib.cam.ac.uk/view/{" not in src


# --- review of 94f3954: the cache problems -------------------------------------------------

def test_a_concurrent_reader_never_sees_the_new_decision_beside_the_old_images(svc):
    """enrich_metadata publishes the entry whole. Mutated in place, the entry carried the
    new (current) decision while its images were still the old item's -- and the Puzzle's
    disk cache saved those pixels under the new item's name."""
    wrong, right = "MS-TS-NS-00318-00004-00001", "MS-TS-NS-00318-00041"
    stale = {"shelfmark": "T-S NS 318.41", "external_provider": "cambridge",
             "external_url": f"http://cudl.lib.cam.ac.uk/view/{wrong}",
             "images_ext": [{"label": "1r", "url": f"img:{wrong}"}]}
    mm = _manager({SID_318_41: stale}, {SID_318_41: {
        "shelfmark": "T-S NS 318.41", "library_code": "CUL", "call_numbers_raw": ["T-S NS 318.41"]}})
    seen = []

    def _ext(self, url):
        entry = mm.nli_cache[SID_318_41]  # what another thread reads mid-enrichment
        seen.append((resolution_is_current(entry.get("cambridge_resolution")),
                     [i["url"] for i in entry.get("images_ext") or []]))
        label = url.rstrip("/").rsplit("/", 1)[-1]
        return {"canvases": [{"label": "1r", "url": f"img:{label}"}], "metadata": {}, "attribution": ""}

    with patch.object(MetadataManager, "fetch_marc_data", lambda self, s: {}), \
         patch.object(MetadataManager, "fetch_iiif_manifest", lambda self, s, suffix=1: {}), \
         patch.object(MetadataManager, "fetch_external_iiif_data", _ext), \
         patch.object(MetadataManager, "get_part_for_folio", lambda self, s: None), \
         patch.object(mm_mod, "_get_crossref_service", lambda: svc), \
         patch.object(mm_mod, "_get_fjms_service", lambda: None):
        mm.enrich_metadata(SID_318_41)

    assert seen == [(False, [f"img:{wrong}"])]  # the old entry, still marked stale
    after = mm.nli_cache[SID_318_41]
    assert resolution_is_current(after["cambridge_resolution"])
    assert [i["url"] for i in after["images_ext"]] == [f"img:{right}"]


def test_the_desktop_result_dialog_reenriches_an_entry_cached_before_the_decision():
    pytest.importorskip("PyQt6")
    from desktop.result_dialog import _cached_meta_needs_enrichment

    class _MM:
        supports_cambridge_decision = True

    current = {"revision": RESOLVER_REVISION, "status": "match"}
    mm = _MM()
    # The entry the dialog shows is the one checked -- not a re-read of the cache.
    assert _cached_meta_needs_enrichment(mm, "S", {"marc": {}}) is True                      # before the fix
    assert _cached_meta_needs_enrichment(mm, "S", {"marc": {}, "cambridge_resolution": current}) is False
    assert _cached_meta_needs_enrichment(mm, "S", {"title": "x", "cambridge_resolution": current}) is True
    assert _cached_meta_needs_enrichment(mm, "S", None) is True


def test_every_puzzle_external_image_url_carries_the_decision_revision():
    """The server keys its disk copy by CUDL item, but a browser keeps an unchanged URL's
    bytes (max-age): every /api/puzzle_ext_image URL the page builds carries cv=."""
    import web.pages.puzzle as puzzle

    assert puzzle.EXT_IMAGE_VERSION == f"&cv={RESOLVER_REVISION}"
    src = (REPO / "web" / "pages" / "puzzle.py").read_text(encoding="utf-8")
    py_urls = [ln for ln in src.splitlines() if 'f"/api/puzzle_ext_image?' in ln]
    assert py_urls and all("{EXT_IMAGE_VERSION}" in ln for ln in py_urls)
    js = puzzle.PUZZLE_CANVAS_JS
    assert js.count("'/api/puzzle_ext_image?") == js.count("__EXT_IMAGE_VERSION__") == 3
    assert "PUZZLE_CANVAS_JS.replace('__EXT_IMAGE_VERSION__', EXT_IMAGE_VERSION)" in src


# --- review of a85b805: the decision is checked on the entry the images come from --------

_OLD_ITEM, _NEW_ITEM = "MS-TS-NS-00318-00004-00001", "MS-TS-NS-00318-00041"


def _entries():
    old = {"images_ext": [{"label": "1r", "url": f"https://images.lib.cam.ac.uk/iiif/{_OLD_ITEM}-000-00001.jp2"}],
           "external_provider": "cambridge"}                       # cached before the decision
    new = {"images_ext": [{"label": "1r", "url": f"https://images.lib.cam.ac.uk/iiif/{_NEW_ITEM}-000-00001.jp2"}],
           "external_provider": "cambridge",
           "external_url": f"https://cudl.lib.cam.ac.uk/view/{_NEW_ITEM}",
           "cambridge_resolution": {"revision": RESOLVER_REVISION, "status": "match", "label": _NEW_ITEM,
                                    "viewer_url": f"https://cudl.lib.cam.ac.uk/view/{_NEW_ITEM}"}}
    return old, new


class _PublishedRightAfterRead(dict):
    """An nli_cache whose old entry another thread replaces right after this one reads it:
    a check of the decision on a SECOND read sees the new entry's current revision while
    the reader already holds the old entry's images."""

    def __init__(self, sid, old, new):
        super().__init__({sid: old})
        self._old, self._new = old, new

    def get(self, key, default=None):
        value = super().get(key, default)
        if value is self._old:
            super().__setitem__(key, self._new)
        return value


class _RacingManager:
    supports_cambridge_decision = True

    def __init__(self, sid):
        old, self.new = _entries()
        self.nli_cache = _PublishedRightAfterRead(sid, old, self.new)
        self.enriched = 0

    def enrich_metadata(self, sid):
        self.enriched += 1
        self.nli_cache[sid] = self.new
        return self.new


def test_a_replacement_published_between_read_and_check_cannot_vouch_for_old_images():
    from web.components.image_resolution import resolve_external_images

    mm = _RacingManager("S")
    entry = fresh_external_entry(mm, "S")
    assert [i["url"] for i in entry["images_ext"]] == [i["url"] for i in mm.new["images_ext"]]
    assert mm.enriched == 1

    mm = _RacingManager("S")
    got = resolve_external_images("S", meta_mgr=mm)
    assert _NEW_ITEM in got["cambridge_images"][0]["url"]
    assert got["cambridge_resolution"]["label"] == _NEW_ITEM      # from the same entry
    assert got["external_url"].endswith(_NEW_ITEM)


def _canvases_by_position(monkeypatch):
    """Page N is the entry's Nth canvas, as when nli_crossref.db has no folio rows for the
    record (CI has no sidecar). With the real sidecar this sys_id's folios match none of the
    fixture canvases, so the route takes the NLI fallback, which these tests do not fake."""
    import shared.nli_crossref_service as ncs
    monkeypatch.setattr(ncs, "resolve_cambridge_canvas_for_page", lambda *a, **k: {"degraded": True})


def test_the_image_routes_never_serve_or_keep_the_old_item_under_the_new_decision(tmp_path, monkeypatch):
    """The reported failure: the old image saved on disk under the corrected item's name,
    surviving a cleared memory cache. Real routes; only the network is replaced."""
    from types import SimpleNamespace

    from fastapi import FastAPI
    from starlette.testclient import TestClient

    import shared.puzzle_image_service as pis
    from web.api import init_api_routes
    from web.state import state

    def _fetch(url, **_kw):
        label = _NEW_ITEM if _NEW_ITEM in url else _OLD_ITEM
        return SimpleNamespace(status_code=200, headers={"Content-Type": "image/jpeg"},
                               content=f"jpeg-bytes-of-{label}".encode().ljust(200, b"\0"))

    monkeypatch.setattr(pis, "get_with_checked_redirects", _fetch)
    _canvases_by_position(monkeypatch)
    pis.reset_puzzle_image_service()
    service = pis.get_puzzle_image_service(cache_dir=tmp_path / "puzzle")
    try:
        sid = "990051109460205171"
        app = FastAPI()
        init_api_routes(app_override=app)
        client = TestClient(app)

        monkeypatch.setattr(state, "meta_mgr", _RacingManager(sid))
        got = client.get(f"/api/cambridge_image/{sid}", params={"page": 0})
        assert got.status_code == 200 and _NEW_ITEM.encode() in got.content

        monkeypatch.setattr(state, "meta_mgr", _RacingManager(sid))
        got = client.get("/api/puzzle_ext_image", params={
            "sys_id": sid, "page": 0, "provider": "cambridge", "processed": "false"})
        assert got.status_code == 200 and _NEW_ITEM.encode() in got.content
        on_disk = {p.name: p.read_bytes() for p in (tmp_path / "puzzle").rglob("*") if p.is_file()}
        assert on_disk and all(_NEW_ITEM in name and _NEW_ITEM.encode() in body
                               for name, body in on_disk.items()), on_disk.keys()
    finally:
        pis.reset_puzzle_image_service()


# --- review of 7b17f2c: thumbnail writes and provisional images ---------------------------

def test_a_thumbnail_write_never_puts_back_an_entry_replaced_meanwhile():
    """get_thumbnail sets its fields on the entry current at the moment of writing, in one
    locked step. Modelled: an enrichment lands between its read and its write."""
    from shared.metadata_manager import _BoundedLRUCache

    old, new = _entries()

    class _EnrichmentLandsBeforeTheNextWrite(_BoundedLRUCache):
        pending = None

        def get(self, key, default=None):
            value = super().get(key, default)
            if value is old:
                self.pending = (key, new)
            return value

        def _land(self):
            if self.pending:
                key, entry = self.pending
                self.pending = None
                super().__setitem__(key, entry)

        def __setitem__(self, key, value):
            self._land()
            super().__setitem__(key, value)

        def merge_fields(self, key, fields, default=None):
            self._land()
            return super().merge_fields(key, fields, default)

    mm = MetadataManager.__new__(MetadataManager)
    mm.nli_cache = _EnrichmentLandsBeforeTheNextWrite(data={"S": dict(old, fl_ids=["FL1"])})
    old = mm.nli_cache.get("S")  # the cached object itself (the subclass keys on identity)
    with patch.object(MetadataManager, "_resolve_thumbnail", lambda self, fl_ids, size=320: "thumb:1"):
        assert mm.get_thumbnail("S") == "thumb:1"
    entry = mm.nli_cache.get("S")
    assert entry["cambridge_resolution"]["label"] == _NEW_ITEM          # the enrichment stands
    assert _NEW_ITEM in entry["images_ext"][0]["url"]
    assert entry["thumb_url"] == "thumb:1" and entry["thumb_checked"] is True


class _ProvisionalManager:
    """A record whose Cambridge decision is not final yet (catalogue still loading)."""
    supports_cambridge_decision = True

    def __init__(self, sid):
        _old, new = _entries()
        self.entry = dict(new, cambridge_resolution={"revision": None, "status": "unavailable"})
        self.nli_cache = {sid: self.entry}

    def enrich_metadata(self, sid):
        return self.entry


def test_an_image_made_under_a_provisional_decision_is_kept_by_no_cache(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from fastapi import FastAPI
    from starlette.testclient import TestClient

    import shared.puzzle_image_service as pis
    from web.api import init_api_routes
    from web.state import state

    monkeypatch.setattr(pis, "get_with_checked_redirects", lambda url, **_kw: SimpleNamespace(
        status_code=200, headers={"Content-Type": "image/jpeg"}, content=b"jpeg".ljust(200, b"\0")))
    _canvases_by_position(monkeypatch)
    pis.reset_puzzle_image_service()
    pis.get_puzzle_image_service(cache_dir=tmp_path / "puzzle")
    try:
        sid = "990051109460205171"
        app = FastAPI()
        init_api_routes(app_override=app)
        client = TestClient(app)

        monkeypatch.setattr(state, "meta_mgr", _ProvisionalManager(sid))
        got = client.get(f"/api/cambridge_image/{sid}", params={"page": 0})
        assert got.status_code == 200 and got.headers["cache-control"] == "no-store"
        got = client.get("/api/puzzle_ext_image", params={
            "sys_id": sid, "page": 0, "provider": "cambridge", "processed": "false"})
        assert got.status_code == 200 and got.headers["cache-control"] == "no-store"
        assert not [p for p in (tmp_path / "puzzle").rglob("*") if p.is_file()]

        final = _RacingManager(sid)
        final.nli_cache = {sid: final.new}
        monkeypatch.setattr(state, "meta_mgr", final)
        # Same page: the provisional answer was not kept in memory either.
        got = client.get(f"/api/cambridge_image/{sid}", params={"page": 0})
        assert got.status_code == 200 and "max-age" in got.headers["cache-control"]
    finally:
        pis.reset_puzzle_image_service()

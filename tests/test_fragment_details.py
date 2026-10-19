# -*- coding: utf-8 -*-
"""shared/fragment_details.py: the Compare windows' info line and external links.

One rule for the web Compare modal and the desktop CompareDialog. The lookups
are faked here; nothing reads a sidecar.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from shared import fragment_details as fd

CORPUS = "990001234560205171"
SYNTHETIC = "99" + "0000012345" + "000000"


# --- pure helpers -----------------------------------------------------------

def test_ktiv_url_for_a_corpus_id():
    url = fd.ktiv_url(CORPUS)
    assert url.endswith(f"docId=PNX_MANUSCRIPTS{CORPUS}")
    assert url.startswith("https://www.nli.org.il/")


@pytest.mark.parametrize("sid", ["", None, SYNTHETIC, "970000000100000001"])
def test_no_ktiv_url_for_empty_synthetic_or_local(sid):
    assert fd.ktiv_url(sid) is None


def test_domain_labels_hebrew_with_english_fallback_and_dedup():
    domains = [
        {"domain": "Liturgy", "domain_heb": "ליטורגיה"},
        {"domain": "Bible", "domain_heb": ""},
        {"domain": "Liturgy", "domain_heb": "ליטורגיה"},
        {"domain": "--", "domain_heb": "--"},
        {"domain": "", "domain_heb": "ריק"},
    ]
    assert fd.domain_labels(domains, hebrew=True) == ["ליטורגיה", "Bible"]
    assert fd.domain_labels(domains, hebrew=False) == ["Liturgy", "Bible"]


@pytest.mark.parametrize("w,h,expected", [
    (18.4, 12.6, "18×13 cm"),
    ("20", "10", "20×10 cm"),
    (None, 10, ""),
    (0, 10, ""),
    ("x", 10, ""),
])
def test_dims_text(w, h, expected):
    assert fd.dims_text(w, h) == expected


def test_info_line_orders_and_skips_unknown_parts():
    assert fd.info_line("CUL", ["Liturgy", "Bible"], "1100", 18, 12, "paper") == (
        "CUL · Liturgy, Bible · 1100 · 18×12 cm · paper"
    )
    assert fd.info_line("", [], None, None, None, "") == ""
    assert fd.info_line("BL", None, "  ", 18, None, "") == "BL"


# --- fetch_fragment_details with faked lookups -----------------------------

class _Fjms:
    def is_available(self):
        return True

    def get_domains_for_sys_ids(self, sids):
        return {sids[0]: [{"domain": "Liturgy", "domain_heb": "ליטורגיה"}]}

    def get_catalog(self, sid):
        return {"copy_date": "-99"}   # FJMS's "no date"


class _Crossref:
    def __init__(self, viewer):
        self._viewer = viewer

    def get_library_viewer_url(self, sid):
        return self._viewer


def _patch_sources(monkeypatch, *, viewer=None, pgp_doc=None, fjms=None):
    import shared.document_service as ds
    import shared.fjms_service as fj
    import shared.nli_crossref_service as nc
    monkeypatch.setattr(fj, "get_fjms_service", lambda thread_safe=True: fjms or _Fjms())
    monkeypatch.setattr(nc, "get_nli_crossref_service", lambda thread_safe=True: _Crossref(viewer))
    monkeypatch.setattr(ds, "get_document_for_fragment", lambda sid, page=None: pgp_doc)


def test_fetch_details_links_in_fixed_order(monkeypatch):
    _patch_sources(
        monkeypatch,
        viewer={"url": "https://cudl.example/x", "label": "Cambridge Digital Library"},
        pgp_doc={"pgp_url": "https://geniza.princeton.edu/documents/1/"},
    )
    meta_mgr = SimpleNamespace(csv_bank={CORPUS: {"library_code": "CUL", "title": "Piyyut"}})
    got = fd.fetch_fragment_details(CORPUS, 2, meta_mgr=meta_mgr)

    assert got["library_code"] == "CUL"
    assert got["title"] == "Piyyut"
    assert got["domains"] == [{"domain": "Liturgy", "domain_heb": "ליטורגיה"}]
    assert got["dating"] is None   # "-99" is FJMS's empty date
    assert [ln["key"] for ln in got["links"]] == ["ktiv", "library", "pgp"]
    assert got["links"][1]["label"] == "Cambridge Digital Library"


def test_fetch_details_falls_back_to_the_oxford_part_link(monkeypatch):
    _patch_sources(monkeypatch, viewer=None, pgp_doc=None)

    class _Meta:
        csv_bank: dict = {}

        def get_part_for_folio(self, sid):
            return "P1"

        def get_part_metadata(self, part_id):
            return {"direct_link": "https://digital.bodleian.example/P1"}

    got = fd.fetch_fragment_details(CORPUS, meta_mgr=_Meta())
    assert [ln["key"] for ln in got["links"]] == ["ktiv", "library"]
    assert got["links"][1] == {
        "key": "library", "label": "Oxford Bodleian",
        "url": "https://digital.bodleian.example/P1",
    }


def test_fetch_details_survives_every_source_failing(monkeypatch):
    class _Broken:
        def __getattr__(self, name):
            raise RuntimeError("sidecar gone")

    import shared.document_service as ds
    import shared.fjms_service as fj
    import shared.nli_crossref_service as nc
    monkeypatch.setattr(fj, "get_fjms_service", lambda thread_safe=True: _Broken())
    monkeypatch.setattr(nc, "get_nli_crossref_service", lambda thread_safe=True: _Broken())

    def _raise(*a, **k):
        raise RuntimeError("pgp.db gone")
    monkeypatch.setattr(ds, "get_document_for_fragment", _raise)

    got = fd.fetch_fragment_details(CORPUS, meta_mgr=_Broken())
    assert got["domains"] == [] and got["dating"] is None
    assert [ln["key"] for ln in got["links"]] == ["ktiv"]


def test_fetch_details_for_a_local_document_is_empty():
    got = fd.fetch_fragment_details("970000000100000001")
    assert got == {"library_code": "", "title": "", "domains": [], "dating": None, "links": []}

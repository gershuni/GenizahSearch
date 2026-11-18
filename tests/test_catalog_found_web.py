# -*- coding: utf-8 -*-
"""Where a catalogue search found a manuscript, on the website (web/components/catalog_found.py):
the panel's HTML, the Browse address that carries the search, the gate, and the call sites in
Quick View, the result card's and Quick View's Browse links, Browse and its route. Over the
synthetic sidecar of tests/catalog_text_fixture.py, never the real file."""
from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog_text_fixture import C, build_sidecar  # noqa: E402
import web.components.catalog_found as cf  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]

_FOUND = {"records": [{"label": "FJMS Catalog · Note", "rtl": True,
                       "segments": [("רשומה <זו> (", False), ("טעות", True), (")", False)]},
                      {"label": "PGP description", "rtl": False, "segments": [("A & B", False)]}],
          "count": 5}


@pytest.fixture
def plain_tr(monkeypatch):
    monkeypatch.setattr(cf, "tr", lambda s: s)


def test_the_panel_html_marks_escapes_and_says_what_is_left_out(plain_tr):
    out = cf.found_records_html(_FOUND)
    assert '<mark class="highlight-match">טעות</mark>' in out
    assert "&lt;זו&gt;" in out and "A &amp; B" in out
    assert 'dir="rtl"' in out and 'dir="ltr"' in out
    assert ">FJMS Catalog · Note<" in out and "2 of 5 matching records" in out
    assert cf.found_records_html({"records": [], "count": 0}) == "" and cf.found_records_html(None) == ""


def _row(query="טעות", near=False, translations=True):
    return {"display": {"id": C}, "snippet": "*טעות*",
            "catalogue": {"source": "fjms", "field": "Comment",
                          "searched": {"query": query, "near_spellings": near,
                                       "include_translations": translations}}}


def test_the_browse_address_carries_a_catalogue_results_search():
    assert cf.browse_params(_row()) == "&cq=%D7%98%D7%A2%D7%95%D7%AA"
    assert cf.browse_params(_row("Saadia", near=True, translations=False)) == "&cq=Saadia&cnear=1&ctr=0"
    assert cf.browse_params({"snippet": "*x*"}) == ""
    assert cf.browse_params(_row(query="  ")) == ""


def test_the_address_parameters_leave_the_default_switches_out():
    assert cf.address_params({"query": "טעות", "near_spellings": False, "include_translations": True}) == {
        "cq": "טעות"}
    assert cf.address_params({"query": "x", "near_spellings": True, "include_translations": False}) == {
        "cq": "x", "cnear": "1", "ctr": "0"}
    assert cf.address_params({"query": " "}) == {} and cf.address_params(None) == {}


def test_browse_keeps_the_search_in_its_address_for_the_manuscript_it_opened():
    """Codex review of #51: the page's own address update dropped cq, so a reload lost the panel."""
    update = _function(ast.parse(_source("web/pages/browse.py")), "_update_browser_url")
    assert "address_params" in _called(update)
    src = ast.get_source_segment(_source("web/pages/browse.py"), update)
    assert "catalogue_found['sys_id'] == state.sys_id" in src


def test_the_results_browse_links_carry_the_search_not_a_transcription_phrase():
    from web.pages.search_results import _browse_match_params
    assert _browse_match_params(_row(), "*טעות*") == "&cq=%D7%98%D7%A2%D7%95%D7%AA"
    assert _browse_match_params({"snippet": "a *שלום* b"}, "a *שלום* b") == "&highlight=%D7%A9%D7%9C%D7%95%D7%9D"
    assert _browse_match_params({}, "") == ""


@pytest.fixture
def served(tmp_path_factory, monkeypatch):
    import shared.catalog_text_service as svc
    from shared.catalog_text_service import CatalogTextService
    monkeypatch.setenv("CATALOG_TEXT_SEARCH_ENABLED", "1")
    service = CatalogTextService(db_path=str(build_sidecar(tmp_path_factory.mktemp("catalog_found"))))
    monkeypatch.setattr(svc, "_default_service", service)
    monkeypatch.setattr("web.catalog_assets.catalog_available", lambda: True)
    yield service
    service.close()


@pytest.fixture
def empty_cache(monkeypatch):
    from collections import OrderedDict
    monkeypatch.setattr(cf, "_CACHE", OrderedDict())


def test_quick_view_and_browse_look_up_the_same_note(served, empty_cache):
    from shared.catalog_text_rows import found_records
    lookup = cf.lookup_for_result(_row())
    assert lookup == (C, "טעות", False, True) == cf.lookup_for_browse(C, "טעות", False, True)
    assert cf.cached_matches(lookup) is None
    found, count = cf.run_lookup(lookup)
    assert count == 1 and found[0].field == "Comment"
    assert cf.cached_matches(lookup) == (found, count)
    assert found_records(found, count)["records"][0]["label"] == "FJMS Catalog · Note"
    assert cf.lookup_for_browse(C, "", False, True) is None


def test_the_kept_answers_are_bounded(served, empty_cache, monkeypatch):
    monkeypatch.setattr(cf, "CACHE_SIZE", 2)
    for word in ("טעות", "שטר", "כתובה"):
        cf.run_lookup((C, word, False, True))
    assert list(cf._CACHE) == [(C, "שטר", False, True), (C, "כתובה", False, True)]


def test_nothing_where_the_catalogue_is_not_served(served, monkeypatch):
    monkeypatch.setattr("web.catalog_assets.catalog_available", lambda: False)
    assert cf.lookup_for_result(_row()) is None
    assert cf.lookup_for_browse(C, "טעות", False, True) is None
    assert cf.lookup_for_result({"display": {"id": C}, "snippet": "x"}) is None


def test_the_lookup_runs_off_the_event_loop():
    """Codex review of #51: with near spellings a lookup builds and looks up each word's
    candidates; on the event loop a crafted Browse address could stall every visitor. The page
    side asks only the kept answers; the file is read in run.io_bound."""
    tree = ast.parse(_source("web/components/catalog_found.py"))
    answer = _function(tree, "answer_lookup")
    io_calls = [n for n in ast.walk(answer) if isinstance(n, ast.Call) and getattr(n.func, "attr", None) == "io_bound"]
    assert len(io_calls) == 1 and getattr(io_calls[0].args[0], "id", None) == "run_lookup"
    place = _function(tree, "place_found_panel")
    called = _called(place)
    assert "answer_lookup" in called
    assert not called & {"catalogue_matches", "matches", "run_lookup", "io_bound"}


def test_simultaneous_lookups_share_one_run(monkeypatch, empty_cache):
    """Codex review of #51: callers of the same key while it runs (another visitor, Quick View
    drawn again) wait for the first run instead of building the candidates again; a caller that
    goes away does not stop it."""
    import asyncio

    import nicegui.run

    runs = []

    async def slow_io_bound(func, lookup):
        runs.append(lookup)
        await asyncio.sleep(0.05)
        return (["m"], 1)

    monkeypatch.setattr(nicegui.run, "io_bound", slow_io_bound)
    monkeypatch.setattr(cf, "_IN_FLIGHT", {})
    lookup = (C, "Saadia", True, True)

    async def main():
        first = asyncio.ensure_future(cf.answer_lookup(lookup))
        gone = asyncio.ensure_future(cf.answer_lookup(lookup))
        await asyncio.sleep(0)
        gone.cancel()                                    # a visitor who left
        third = await cf.answer_lookup(lookup)
        return await first, third

    assert asyncio.run(main()) == ((["m"], 1), (["m"], 1))
    assert runs == [lookup] and cf._IN_FLIGHT == {}


# --- the call sites ---------------------------------------------------------------------------

def _source(relative):
    return (ROOT / relative).read_text(encoding="utf-8")


def _function(tree, name):
    found = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    assert len(found) == 1, name
    return found[0]


def _called(node):
    return {getattr(n.func, "id", None) or getattr(n.func, "attr", None)
            for n in ast.walk(node) if isinstance(n, ast.Call)}


def test_quick_view_shows_the_panel_for_its_result():
    tree = ast.parse(_source("web/pages/search_results.py"))
    render = _function(tree, "render_content")
    assert {"lookup_for_result", "place_found_panel"} <= _called(render)


def test_every_browse_link_of_the_results_goes_through_the_helper():
    src = _source("web/pages/search_results.py")
    helper = src.index("def _browse_match_params")
    rest = src[:helper] + src[src.index("def _snippet_match_phrase"):]
    assert "&highlight={quote(" not in rest
    assert src.count("_browse_match_params(result, snippet)") == 4


def test_browse_shows_the_panel_and_its_route_passes_the_search():
    tree = ast.parse(_source("web/pages/browse.py"))
    assert "place_found_panel" in _called(_function(tree, "update_content"))
    assert "lookup_for_browse" in _called(_function(tree, "create_browse_page"))
    route = _function(ast.parse(_source("web/main.py")), "browse_page_route")
    assert {"cq", "cnear", "ctr"} <= {a.arg for a in route.args.args}
    calls = [n for n in ast.walk(route) if isinstance(n, ast.Call)
             and getattr(n.func, "id", None) == "create_browse_page"]
    assert len(calls) == 1 and "catalogue_search" in {kw.arg for kw in calls[0].keywords}

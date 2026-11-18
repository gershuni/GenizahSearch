# -*- coding: utf-8 -*-
"""The catalogue text search as a search mode (catalogue plan §4.3, §4.4, §4.7): the rows
(shared/catalog_text_rows.py), the engine's 'Catalog' mode and % prefix, the research wrapper
running it in its own thread, the Latin-script test, and a search-within step keeping its
switches. Over the synthetic sidecar of tests/catalog_text_fixture.py, never the real file."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog_text_fixture import A, B, C, N, P, build_sidecar  # noqa: E402
from shared import catalog_text_rows as rows_module  # noqa: E402
from shared.catalog_text_rows import (CATALOG_MODE, catalog_options, catalog_search_rows,  # noqa: E402
                                      source_label_parts)
from shared.catalog_text_service import CatalogTextService  # noqa: E402


class _Meta:
    """The two lookups the rows make, over a catalogue that knows every fixture manuscript but
    ``unknown``."""

    def __init__(self, unknown=()):
        self.unknown = set(unknown)
        self.csv_bank = _Bank(self.unknown)

    def get_meta_for_id(self, sys_id):
        return (f"T-S {sys_id[-2:]}", f"title {sys_id[-2:]}")

    def get_library_for_id(self, sys_id):
        return "CUL"


class _Bank:
    def __init__(self, unknown):
        self.unknown = unknown

    def __contains__(self, sys_id):
        return sys_id not in self.unknown

    def __bool__(self):
        return True


@pytest.fixture(scope="module")
def sidecar(tmp_path_factory):
    return build_sidecar(tmp_path_factory.mktemp("catalog_rows"))


@pytest.fixture
def service(sidecar, monkeypatch):
    monkeypatch.setenv("CATALOG_TEXT_SEARCH_ENABLED", "1")
    service = CatalogTextService(db_path=str(sidecar))
    assert service.is_available(), service.unavailable_reason
    yield service
    service.close()


def _ids(rows):
    return [r["display"]["id"] for r in rows]


# --- the rows --------------------------------------------------------------------------

def test_rows_come_in_the_catalogues_order_with_their_source(service):
    rows, info = catalog_search_rows("כתובה", None, _Meta(), service=service)
    assert _ids(rows) == [A, C, B]
    assert info == {"total": 3, "capped": False, "near_forms": [], "available": True}
    first = rows[0]
    assert first["snippet"] == "*כתובה*" and first["full_text"] == "" and first["metadata_only"]
    assert first["catalogue"] == {"source": "fjms", "field": "GenizahTitle", "lang": "he",
                                  "automatic": False, "near": False, "exact": True}
    assert first["display"]["shelfmark"] == "T-S 01" and first["display"]["library_code"] == "CUL"


def test_near_spellings_are_shown_as_written_and_their_rows_marked(service):
    rows, info = catalog_search_rows("כתובה", {"near_spellings": True}, _Meta(), service=service)
    assert _ids(rows) == [A, C, B, N, P]
    assert info["near_forms"] == ["כתבה"]
    assert [r["catalogue"]["near"] for r in rows] == [False, False, False, True, True]


def test_the_translations_switch_reaches_the_service(service):
    with_tr, _ = catalog_search_rows("marriage contract", None, _Meta(), service=service)
    without, _ = catalog_search_rows("marriage contract", {"include_translations": False}, _Meta(),
                                     service=service)
    assert len(with_tr) == 3 and len(without) == 2
    assert any(r["catalogue"]["automatic"] for r in with_tr)
    assert not any(r["catalogue"]["automatic"] for r in without)


def test_a_restriction_keeps_its_manuscripts_only(service):
    rows, info = catalog_search_rows("כתובה", None, _Meta(), restrict_sys_ids={B, C}, service=service)
    assert _ids(rows) == [C, B] and info["total"] == 2


def test_a_manuscript_the_loaded_catalogue_does_not_know_is_skipped(service):
    rows, info = catalog_search_rows("כתובה", None, _Meta(unknown={C}), service=service)
    assert _ids(rows) == [A, B] and info["total"] == 2


def test_the_count_leaves_out_unknown_manuscripts_past_the_cap(service):
    # C ranks after the two rows kept; the count still leaves it out (the API publishes it).
    rows, info = catalog_search_rows("כתובה", None, _Meta(unknown={B}), service=service, max_rows=2)
    assert _ids(rows) == [A, C] and info["total"] == 2 and not info["capped"]


def test_rows_past_the_cap_are_cut_and_said_to_be(service):
    rows, info = catalog_search_rows("כתובה", None, _Meta(), service=service, max_rows=2)
    assert _ids(rows) == [A, C] and info["capped"]


def test_an_unavailable_service_answers_nothing(sidecar, monkeypatch):
    monkeypatch.delenv("CATALOG_TEXT_SEARCH_ENABLED", raising=False)
    service = CatalogTextService(db_path=str(sidecar))
    try:
        assert catalog_search_rows("כתובה", None, _Meta(), service=service) == (
            [], {"total": 0, "capped": False, "near_forms": [], "available": False})
    finally:
        service.close()


def test_the_switches_default_to_translations_on_and_near_spellings_off():
    assert catalog_options(None) == {"include_translations": True, "near_spellings": False}
    assert catalog_options({"near_spellings": 1, "include_translations": 0}) == {
        "include_translations": False, "near_spellings": True}


@pytest.mark.parametrize("source, field, automatic, parts", [
    ("fjms", "FreeDesc", False, ["FJMS Catalog", "Free Description"]),
    ("fjms", "FullText", True, ["FJMS Catalog", "Full catalogue entry", "automatic translation"]),
    ("pgp", "description", False, ["PGP description"]),
    ("libraries", "title_en", True, ["Library catalogue title", "automatic translation"]),
])
def test_the_source_line(source, field, automatic, parts):
    assert source_label_parts(source, field, automatic) == parts


def test_every_label_is_translated():
    from shared.genizah_translations import TRANSLATIONS
    labels = set(rows_module.SOURCE_LABELS.values()) | set(rows_module.FIELD_LABELS.values())
    labels.add(rows_module.AUTOMATIC_LABEL)
    assert not [label for label in labels if label not in TRANSLATIONS]


# --- the engine ------------------------------------------------------------------------

def _engine(meta):
    from shared.search_engine import SearchEngine
    engine = SearchEngine.__new__(SearchEngine)
    engine.meta_mgr = meta
    return engine


@pytest.mark.parametrize("query, expected", [
    ("% cheese", ("Catalog", "cheese")),
    ("%כתובה", ("Catalog", "כתובה")),
    ("%", (None, "%")),
    ("cheese", (None, "cheese")),
])
def test_the_percent_prefix_is_the_catalogue(query, expected):
    assert _engine(_Meta()).parse_query_syntax(query) == expected
    assert CATALOG_MODE == "Catalog"


def test_inside_responsa_percent_keeps_its_responsa_meaning():
    assert _engine(_Meta()).parse_query_syntax("%כתובה", responsa_mode=True) == (None, "%כתובה")


def test_a_prefix_typed_without_a_space_shows_its_modes_options():
    """``%word`` and Enter set the mode list in code, which fires no change event: the page then
    calls the change handler itself, or the catalogue's switches stay hidden (found in testing,
    2026-11-18)."""
    import ast

    path = Path(__file__).resolve().parent.parent / "web" / "pages" / "search.py"
    source = path.read_text(encoding="utf-8")
    run = next(n for n in ast.walk(ast.parse(source))
               if isinstance(n, ast.AsyncFunctionDef) and n.name == "execute_search")
    body = ast.get_source_segment(source, run)
    at = body.index("mode_select.value = mode\n")
    following = body[at:].splitlines()[1:5]
    assert any(line.strip() == "on_mode_change()" for line in following), following


def test_the_catalog_mode_answers_rows_with_its_options(service, monkeypatch):
    import shared.catalog_text_service as svc
    monkeypatch.setattr(svc, "_default_service", service)
    rows = _engine(_Meta()).execute_search("כתובה", "Catalog", 0, catalog_options={"near_spellings": True})
    assert _ids(rows) == [A, C, B, N, P]
    assert rows[0]["catalogue_search"] == {"near_forms": ["כתבה"], "total": 5}


def test_the_catalog_mode_says_when_it_cut_rows(service, monkeypatch):
    import shared.catalog_text_service as svc
    from shared.search_engine import consume_last_search_cutoff
    monkeypatch.setattr(svc, "_default_service", service)
    monkeypatch.setattr(rows_module, "CATALOG_MAX_ROWS", 1)
    consume_last_search_cutoff()
    rows = _engine(_Meta()).execute_search("כתובה", "Catalog", 0)
    assert len(rows) == 1 and consume_last_search_cutoff()["capped"]
    # A search-within completing gets every row.
    rows = _engine(_Meta()).execute_search("כתובה", "Catalog", 0, ids_only=True)
    assert len(rows) == 3 and not consume_last_search_cutoff().get("capped")


def test_a_catalog_search_where_it_is_not_served_answers_nothing(monkeypatch, tmp_path):
    import shared.catalog_text_service as svc
    monkeypatch.setattr(svc, "_default_service", CatalogTextService(db_path=str(tmp_path / "absent.db")))
    assert _engine(_Meta()).execute_search("כתובה", "Catalog", 0) == []


# --- the research wrapper: never a worker ------------------------------------------------

class _FakeEngine:
    def __init__(self):
        self.calls = []

    def execute_search(self, query_str, mode, gap, progress_callback=None, catalog_options=None):
        import threading
        self.calls.append((query_str, mode, catalog_options, threading.get_ident()))
        if progress_callback:
            progress_callback(0, 1)
        return [{"display": {"id": A}}]


def test_a_catalog_search_runs_in_the_calling_thread(monkeypatch):
    import threading
    import web.research_jobs as rj

    def no_worker(*_a, **_k):
        raise AssertionError("a catalogue search must never go to a research worker")
    monkeypatch.setattr(rj, "get_queue", no_worker)
    fake = _FakeEngine()
    rows = rj.IsolatedEngine(fake, "search").execute_search("cheese", "Catalog", 0,
                                                             catalog_options={"near_spellings": True})
    assert rows == [{"display": {"id": A}}]
    assert fake.calls == [("cheese", "Catalog", {"near_spellings": True}, threading.get_ident())]


def test_a_cancelled_catalog_search_raises_as_a_worker_s_does(monkeypatch):
    import threading
    import web.research_jobs as rj
    monkeypatch.setattr(rj, "get_queue", lambda *a, **k: (_ for _ in ()).throw(AssertionError("no worker")))
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(InterruptedError):
        rj.IsolatedEngine(_FakeEngine(), "search").controlled(cancel=cancel).execute_search("cheese", "Catalog", 0)


# --- the search page's helpers -----------------------------------------------------------

@pytest.mark.parametrize("text, latin", [
    ("cheese", True), ("Fusṭāṭ", True), ("marriage contract", True), ("akuo", True),
    ("כתובה", False), ("Maimonides רמב\"ם", False), ("123", False), ("", False), (None, False),
    ("مكتوب", False),
])
def test_latin_script_queries(text, latin):
    from shared.search_box_input import is_latin_script_query
    assert is_latin_script_query(text) is latin


def test_a_search_within_step_keeps_the_catalogue_switches():
    from shared.refinement import RefinementStep, _sources_argument
    step = RefinementStep(query="cheese", mode="Catalog",
                          catalog_options={"near_spellings": True, "include_translations": False})
    again = RefinementStep.from_dict(step.to_dict())
    assert again.catalog_options == {"near_spellings": True, "include_translations": False}
    assert _sources_argument(again) == {"catalog_options": {"near_spellings": True, "include_translations": False}}
    assert _sources_argument(RefinementStep(query="x", mode="literal")) == {}


def test_the_card_names_the_source_in_the_readers_language(monkeypatch):
    import web.pages.search_results as sr
    monkeypatch.setattr(sr, "tr", lambda s: {"FJMS Catalog": "קטלוג FJMS",
                                            "Free Description": "תיאור חופשי",
                                            "Fuzzy": "מקורב"}.get(s, s))
    row = {"catalogue": {"source": "fjms", "field": "FreeDesc", "automatic": False, "near": True}}
    assert sr.catalogue_source_text(row) == "קטלוג FJMS · תיאור חופשי · מקורב"
    assert sr.catalogue_source_text({"snippet": "x"}) == ""

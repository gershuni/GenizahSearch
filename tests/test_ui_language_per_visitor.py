"""The web UI language is each visitor's own, never the previous visitor's.

``web/translations.py::_current_lang`` is one value for the whole process: every
page render sets it from that visitor's saved choice. ``_resolve_ui_language``
used to fall back to it when a visitor had saved nothing, so after one reader
switched to English every new visitor was shown English until some other render
set it back. A visitor with no saved choice now gets their browser's language:
Hebrew when the browser's first choice is Hebrew, English otherwise.
"""

from __future__ import annotations

import io
from types import SimpleNamespace

import pytest
from nicegui.storage import request_contextvar

import web.main as wm
from web.translations import get_language, language_from_accept_language, set_language


@pytest.mark.parametrize("header, expected", [
    ("he-IL,he;q=0.9,en-US;q=0.8,en;q=0.7", "he"),
    ("he", "he"),
    ("HE-il", "he"),
    ("iw-IL,iw;q=0.9", "he"),  # legacy code for Hebrew
    ("en-US,en;q=0.9,he;q=0.8", "en"),  # Hebrew accepted, but not the first choice
    ("en;q=0.5,he;q=0.8", "he"),  # the highest q wins, not the first listed
    ("he;q=0.8,en;q=0.8", "he"),  # a tie goes to the earlier entry
    ("he;q=0,en", "en"),  # q=0 means not acceptable
    ("fr-FR,fr;q=0.9", "en"),
    ("*", "en"),
    ("", "en"),
    (None, "en"),  # no header: most crawlers
    ("he;q=abc", "en"),
    (" , ;q=1", "en"),
])
def test_language_from_accept_language(header, expected):
    assert language_from_accept_language(header) == expected


@pytest.fixture
def visitor(monkeypatch):
    """One visitor: their per-user storage, and their browser's header."""
    data: dict = {}
    monkeypatch.setattr(wm, "safe_user_get", lambda key, default=None: data.get(key, default))
    saved_lang = get_language()
    token = request_contextvar.set(None)

    def browser(header):
        headers = {} if header is None else {"accept-language": header}
        request_contextvar.set(SimpleNamespace(headers=headers))

    yield SimpleNamespace(storage=data, browser=browser)
    request_contextvar.reset(token)
    set_language(saved_lang)


@pytest.mark.parametrize("header, expected", [
    ("he-IL,he;q=0.9", "he"),
    ("en-US,en;q=0.9", "en"),
    ("ru-RU", "en"),
    (None, "en"),
])
@pytest.mark.parametrize("previous_visitor", ["en", "he"])
def test_no_saved_choice_follows_the_browser_not_the_previous_visitor(
        visitor, header, expected, previous_visitor):
    visitor.browser(header)
    set_language(previous_visitor)  # what the last render left behind
    assert wm._resolve_ui_language() == expected


@pytest.mark.parametrize("saved", ["en", "he"])
def test_saved_choice_wins_over_the_browser(visitor, saved):
    visitor.storage["ui_language"] = saved
    visitor.browser("en-US" if saved == "he" else "he-IL")
    assert wm._resolve_ui_language() == saved


def test_unknown_saved_value_follows_the_browser(visitor):
    visitor.storage["ui_language"] = "fr"
    visitor.browser("he-IL")
    set_language("en")
    assert wm._resolve_ui_language() == "he"


def test_outside_a_request_the_default_is_english(visitor):
    set_language("he")
    assert wm._resolve_ui_language() == "en"


@pytest.mark.parametrize("header, previous_visitor, direction", [
    ("he-IL", "en", "rtl"),
    ("en-US", "he", "ltr"),
])
def test_first_paint_direction_follows_the_browser(visitor, header, previous_visitor, direction):
    """The call site: the pre-render script paints this browser's direction,
    not the direction of whoever rendered last."""
    visitor.browser(header)
    set_language(previous_visitor)
    assert f'var dir = "{direction}";' in wm.apply_theme_immediately()


@pytest.mark.parametrize("header, saved, rtl", [
    ("he-IL,he;q=0.9", None, True),
    ("en-US,en;q=0.9", None, False),
    (None, None, False),
    ("en-US", "he", True),  # a saved choice still wins
])
def test_excel_export_sheet_direction_follows_the_same_rule(monkeypatch, header, saved, rtl):
    """The other call site: `/api/export/excel` lays the workbook out in the
    visitor's UI language, which must match what the page showed them."""
    import openpyxl
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from unittest.mock import MagicMock

    from web.api import init_api_routes, state

    bare = FastAPI()

    @bare.middleware("http")
    async def _track_request(request, call_next):  # what NiceGUI's middleware does
        request_contextvar.set(request)
        return await call_next(request)

    init_api_routes(app_override=bare)
    storage = {"export_search_payload": {
        "results": [{
            "uid": "u0",
            "display": {"shelfmark": "T-S 1.1", "title": "t", "id": "9912345678901234",
                        "library_code": "CUL"},
            "raw_header": "header_9912345678901201_IE99_P1",
            "snippet": "a *match*", "full_text": "x", "sort_score": 0.5,
        }],
        "query": "q", "mode": "text", "gap": None, "filters": None,
        "warnings": [], "selected_uids": None,
    }}
    if saved:
        storage["ui_language"] = saved
    monkeypatch.setattr("web.safe_storage.app",
                        SimpleNamespace(storage=SimpleNamespace(user=storage)))
    mgr = MagicMock()
    mgr.get_meta_for_id.return_value = ("T-S 1.1", "t")
    mgr.get_library_for_id.return_value = "CUL"
    mgr.parse_full_id_components.return_value = {
        "sys_id": "9912345678901234", "ie_id": "IE99", "p_num": "1", "fl_id": None}
    saved_mgr, state.meta_mgr = state.meta_mgr, mgr
    try:
        headers = {} if header is None else {"Accept-Language": header}
        r = TestClient(bare).get("/api/export/excel", headers=headers)
    finally:
        state.meta_mgr = saved_mgr
    assert r.status_code == 200, r.text[:300]
    wb = openpyxl.load_workbook(io.BytesIO(r.content))
    # openpyxl reads an unset direction (left to right) back as None.
    assert [bool(ws.sheet_view.rightToLeft) for ws in wb.worksheets] == [rtl] * len(wb.worksheets)

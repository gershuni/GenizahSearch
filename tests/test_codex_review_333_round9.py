"""Regressions for the two Codex findings in round 9 on PR #333 (2026-09-02).

P2 `web/pages/browse.py` — clearing the search phrase on a MANUSCRIPT change was
not enough: paging within the same manuscript kept it, so on a structurally
unalignable manuscript (whole-document FGP sources offered on every folio) a later
folio could default to the edition containing the PREVIOUS folio's hit.

P2 `web/api.py` — the Rosetta call caught only Timeout and ConnectionError.
`TooManyRedirects` / `ChunkedEncodingError` (both `RequestException` subclasses)
escaped `_fetch_nli_image_bytes`, turning a degraded image fetch into a 500.
"""
from __future__ import annotations

import ast
import json
from unittest.mock import patch

import pytest
import requests
from fastapi import FastAPI

FL = "168181477"
PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 21000


def _read(path):
    return open(path, encoding="utf-8").read()


# --------------------------------------------------------------------------
# Folio scoping
# --------------------------------------------------------------------------

class _Page:
    def __init__(self, sys_id, p_num, volume_ie=None):
        self.sys_id = sys_id
        self.p_num = p_num
        self.volume_ie = volume_ie      # part of the scope key since round 10


def _load_scope_helper():
    """`_search_scope_phrase` executed against a stand-in state object."""
    src = _read("web/pages/browse.py")
    i = src.index("    def _search_scope_phrase(page):")
    j = src.index("    def _clear_search_scope_for_new_manuscript(", i)
    body = "\n".join(line[4:] if line.startswith("    ") else line
                     for line in src[i:j].split("\n"))

    class _State:
        highlight_terms = None
        highlight_scope = None
        volume_ie = None                # read by the helper since round 10

    ns = {"state": _State}
    exec(body, ns)
    return ns["_search_scope_phrase"], _State


class TestPhraseIsScopedToItsFolio:
    def test_the_arriving_folio_gets_the_phrase(self):
        f, st = _load_scope_helper()
        st.highlight_terms = "תקום רבה דיניך"
        st.highlight_scope = None
        assert f(_Page("990053489970205171", 2)) == "תקום רבה דיניך"
        assert st.highlight_scope == ("990053489970205171", None, 2)

    def test_another_folio_of_the_same_manuscript_does_not(self):
        f, st = _load_scope_helper()
        st.highlight_terms = "תקום רבה דיניך"
        st.highlight_scope = None
        f(_Page("990053489970205171", 2))                  # claim it
        assert f(_Page("990053489970205171", 1)) is None, (
            "paging within the manuscript must not carry the phrase"
        )
        assert f(_Page("990053489970205171", 5)) is None

    def test_returning_to_the_original_folio_restores_it(self):
        f, st = _load_scope_helper()
        st.highlight_terms = "phrase"
        st.highlight_scope = None
        f(_Page("A", 2))
        assert f(_Page("A", 1)) is None
        assert f(_Page("A", 2)) == "phrase"

    def test_no_phrase_means_no_scope(self):
        f, st = _load_scope_helper()
        st.highlight_terms = None
        st.highlight_scope = None
        assert f(_Page("A", 1)) is None
        assert st.highlight_scope is None

    def test_missing_page_is_safe(self):
        f, st = _load_scope_helper()
        st.highlight_terms = "phrase"
        st.highlight_scope = None
        assert f(None) is None


class TestScopeWiring:
    def test_selector_call_uses_the_helper(self):
        src = _read("web/pages/browse.py")
        assert "must_contain=_search_scope_phrase(page)" in src
        assert "must_contain=state.highlight_terms," not in src

    def test_manuscript_change_resets_the_scope(self):
        src = _read("web/pages/browse.py")
        i = src.index("def _clear_search_scope_for_new_manuscript(")
        body = src[i:i + 900]
        assert "state.highlight_scope = None" in body

    def test_state_declares_the_field(self):
        assert "self.highlight_scope" in _read("web/pages/browse_state.py")

    def test_enrichment_path_is_scoped_too(self):
        src = _read("web/pages/browse_enrichment.py")
        assert "state.highlight_scope" in src
        assert "must_contain=state.highlight_terms,\n" not in src


# --------------------------------------------------------------------------
# Rosetta exception handling
# --------------------------------------------------------------------------

class _Resp:
    def __init__(self, status, content_type, content=b""):
        self.status_code = status
        self.headers = {"Content-Type": content_type}
        self.content = content

    def json(self):
        return json.loads(self.content.decode("utf-8"))


def _manifest():
    return _Resp(200, "application/json", json.dumps({
        "sequences": [{"canvases": [{"images": [{"resource": {
            "service": {"@id": f"https://iiif.nli.org.il/IIIFv21/FL{FL}"}}}]}]}]
    }).encode("utf-8"))


@pytest.fixture(scope="module", autouse=True)
def _init_routes():
    from web.api import init_api_routes
    init_api_routes(app_override=FastAPI())
    with patch("web.api._save_nli_persistent_cache", lambda *a, **kw: None):
        yield


@pytest.fixture(autouse=True)
def _clear_fl_cache():
    from web.api import _api_test_seam
    fn = _api_test_seam.get("fetch_fl_ids_from_nli")
    if fn is not None and fn.__closure__:
        for name, cell in zip(fn.__code__.co_freevars, fn.__closure__):
            if name in ("_nli_cache", "_nli_cache_time") and isinstance(cell.cell_contents, dict):
                cell.cell_contents.clear()
    yield


@pytest.mark.parametrize("exc", [
    requests.exceptions.TooManyRedirects("loop"),
    requests.exceptions.ChunkedEncodingError("truncated"),
    requests.exceptions.RequestException("other"),
])
def test_rosetta_exceptions_do_not_escape(exc):
    import web.api as api_mod

    sys_id = "9900000000000006" + str(abs(hash(type(exc).__name__)) % 90 + 10)
    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
    iiif_prefix = f"https://iiif.nli.org.il/IIIFv21/FL{FL}/full/"

    def fake_get(url, *a, **kw):
        if url == manifest_url:
            return _manifest()
        if url.startswith(iiif_prefix):
            return _Resp(500, "text/xml", b"<error/>")
        raise AssertionError(url)

    with patch.object(api_mod._nli_session, "get", side_effect=fake_get), \
         patch("web.api.requests.get", side_effect=fake_get), \
         patch("web.api.nli_image_get", side_effect=exc), \
         patch("web.api._nli_circuit_is_open", return_value=False):
        got = api_mod._api_test_seam["_fetch_nli_image_bytes"](sys_id, 0)

    assert got is None, "a failed Rosetta attempt degrades to not-found, never a 500"


def test_a_working_rosetta_still_serves_after_the_new_handler():
    import web.api as api_mod
    sys_id = "990000000000000071"
    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
    iiif_prefix = f"https://iiif.nli.org.il/IIIFv21/FL{FL}/full/"

    def fake_get(url, *a, **kw):
        if url == manifest_url:
            return _manifest()
        if url.startswith(iiif_prefix):
            return _Resp(500, "text/xml", b"<error/>")
        raise AssertionError(url)

    with patch.object(api_mod._nli_session, "get", side_effect=fake_get), \
         patch("web.api.requests.get", side_effect=fake_get), \
         patch("web.api.nli_image_get", return_value=_Resp(200, "image/png", PNG)), \
         patch("web.api._nli_circuit_is_open", return_value=False):
        got = api_mod._api_test_seam["_fetch_nli_image_bytes"](sys_id, 0)
    assert got is not None and got[0] == PNG


def test_handler_covers_the_request_exception_base():
    src = _read("web/api.py")
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef) and n.name == "_fetch_nli_image_bytes")
    body = ast.get_source_segment(src, fn)
    tail = body[body.index("dps_func=thumbnail"):]
    assert "except requests.exceptions.RequestException:" in tail

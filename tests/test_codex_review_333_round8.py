"""Regressions for the three Codex findings in round 8 on PR #333 (2026-09-02).

P2 `web/api.py` — Rosetta answers 200 with a ~1615-byte "no image" placeholder
for an FL it cannot deliver. The `nli_image` route has rejected anything under
2000 bytes since Phase 98; the new fallback accepted anything over 200, so it
cached and displayed the placeholder as the manuscript image. (Observed live on
this very manuscript: one FL returns 1615 bytes, another 21734.)

P2 `web/api.py` — a Rosetta 429/5xx recorded no breaker failure, so the loop
could issue an IIIF *and* a Rosetta request for every cached FL id instead of
opening the global breaker.

P2 `web/pages/browse.py` — the initial Oxford label used `page.attribution`,
discarding the shelfmark-prefixed text built for the licence's
"[object] … Image provided by [owner]" form; it appeared only if a fallback later
switched back to Oxford.
"""
from __future__ import annotations

import ast
import json
from unittest.mock import patch

import pytest
from fastapi import FastAPI

FL = "168181477"
PLACEHOLDER = b"\x89PNG\r\n\x1a\n" + b"\x00" * 1600      # ~1.6 KB, like Rosetta's
REAL_THUMB = b"\x89PNG\r\n\x1a\n" + b"\x00" * 21000      # a real thumbnail


def _read(path):
    return open(path, encoding="utf-8").read()


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


def _run(sys_id, rosetta_resp):
    import web.api as api_mod
    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
    iiif_prefix = f"https://iiif.nli.org.il/IIIFv21/FL{FL}/full/"
    recorded = []

    def fake_get(url, *a, **kw):
        if url == manifest_url:
            return _manifest()
        if url.startswith(iiif_prefix):
            return _Resp(500, "text/xml", b"<error/>")
        raise AssertionError(url)

    with patch.object(api_mod._nli_session, "get", side_effect=fake_get), \
         patch("web.api.requests.get", side_effect=fake_get), \
         patch("web.api.nli_image_get", return_value=rosetta_resp), \
         patch("web.api._nli_circuit_is_open", return_value=False), \
         patch("web.api._nli_record_failure",
               side_effect=lambda **kw: recorded.append(kw.get("failure_type"))), \
         patch("web.api._nli_record_success", side_effect=lambda **kw: None):
        got = api_mod._api_test_seam["_fetch_nli_image_bytes"](sys_id, 0)
    return got, recorded


class TestPlaceholderIsRejected:
    def test_the_1615_byte_placeholder_is_not_served(self):
        got, _ = _run("990000000000000031", _Resp(200, "image/png", PLACEHOLDER))
        assert got is None, "Rosetta's no-image placeholder must not become the image"

    def test_a_real_thumbnail_is_served(self):
        got, _ = _run("990000000000000032", _Resp(200, "image/png", REAL_THUMB))
        assert got is not None
        assert got[0] == REAL_THUMB

    def test_one_threshold_constant_for_both_call_sites(self):
        src = _read("web/api.py")
        assert "_ROSETTA_PLACEHOLDER_MAX_BYTES = 2000" in src
        assert src.count("_ROSETTA_PLACEHOLDER_MAX_BYTES") >= 3   # def + both sites
        assert "len(resp.content) > 2000" not in src, "the route must use the constant"


class TestRosettaFailuresFeedTheBreaker:
    @pytest.mark.parametrize("status,expected", [(429, "429"), (503, "5xx"), (500, "5xx")])
    def test_status_is_recorded(self, status, expected):
        got, recorded = _run(f"99000000000000004{status % 10}",
                             _Resp(status, "text/html", b"nope"))
        assert got is None
        assert expected in recorded, f"Rosetta {status} must trip the breaker ({recorded})"

    def test_404_does_not_trip_it(self):
        # D-07: 404 is the caller's business, not a breaker failure.
        got, recorded = _run("990000000000000051", _Resp(404, "text/html", b"nope"))
        assert got is None
        assert recorded == [] or "404" not in recorded


class TestInitialOxfordCreditIsComplete:
    def test_label_uses_the_shelfmark_prefixed_text(self):
        src = _read("web/pages/browse.py")
        i = src.index("credit_text = (")
        window = src[i:i + 400]
        assert "_credit_ox_text" in window, (
            "the initial Oxford label must use the same text as the data attribute"
        )
        assert "page.attribution or _credit_nli_text" not in window

    def test_the_prefixed_text_is_still_what_the_attribute_carries(self):
        src = _read("web/pages/browse.py")
        assert 'data-credit-oxford="{html_module.escape(_credit_ox_text, quote=True)}"' in src

    def test_nli_active_still_wins(self):
        src = _read("web/pages/browse.py")
        i = src.index("credit_text = (")
        window = src[i:i + 400]
        assert "_is_nli_active and page.is_oxford" in window
        assert window.index("_credit_nli_text") < window.index("_credit_ox_text")


def test_module_still_parses():
    ast.parse(_read("web/api.py"))
    ast.parse(_read("web/pages/browse.py"))

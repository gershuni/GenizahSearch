"""Web NLI image proxy: Rosetta thumbnail fallback + Bodleian direct link.

2026-09-02 (debug/oxford-fgp-image-mismatch, owner UAT): NLI's IIIF server
answered HTTP 500 for every FL id of MS heb. g.2/27 at every size while
Rosetta still served a thumbnail of the same FL. The desktop loader already
fell back to Rosetta (desktop/image_loader.py attempt C) and showed an image;
the web proxy returned 404 and the browse page ended in "Image not available".

The proxy helper is closure-encapsulated in ``web.api.init_api_routes``; it is
reached through the Phase 98 test seam exactly like
tests/test_api_nli_breaker_integration.py does.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from fastapi import FastAPI

# One synthetic sys_id per test: the proxy keeps a positive result cache keyed
# on sys_id+page, so a PNG served in one test would satisfy the next.
FL = "168181477"
PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 2500


def _urls(sys_id):
    return (
        f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest",
        f"https://iiif.nli.org.il/IIIFv21/FL{FL}/full/",
        "https://rosetta.nli.org.il/delivery/DeliveryManagerServlet"
        f"?dps_func=thumbnail&dps_pid=FL{FL}",
    )


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


def _fetch(sys_id, page=0):
    from web.api import _api_test_seam
    return _api_test_seam["_fetch_nli_image_bytes"](sys_id, page)


def _intercept(fake_get):
    """The manifest goes through the module's NLI session; images through
    ``requests.get``. Patch both with the same router."""
    import web.api as api_mod
    return (
        patch.object(api_mod._nli_session, "get", side_effect=fake_get),
        patch("web.api.requests.get", side_effect=fake_get),
    )


def _router(sys_id, thumb_status=200, thumb_ct="image/png;charset=UTF-8", thumb_body=PNG_BYTES):
    manifest_url, iiif_prefix, rosetta_thumb = _urls(sys_id)
    calls = []

    def fake_get(url, *a, **kw):
        calls.append(url)
        if url == manifest_url:
            return _manifest()
        if url.startswith(iiif_prefix):
            return _Resp(500, "text/xml", b"<error/>")
        if url == rosetta_thumb:
            return _Resp(thumb_status, thumb_ct, thumb_body)
        raise AssertionError(f"unexpected URL {url}")

    return fake_get, calls


class TestRosettaThumbnailFallback:
    def test_iiif_500_falls_back_to_rosetta_thumbnail(self):
        sid = "990000000000000001"
        fake_get, calls = _router(sid)
        _p1, _p2 = _intercept(fake_get)
        with _p1, _p2:
            got = _fetch(sid)
        assert got is not None, "proxy must not 404 while Rosetta still serves the FL"
        content, ct, fl_id = got
        assert content == PNG_BYTES
        assert ct == "image/png"          # charset parameter stripped
        assert fl_id == FL
        _, iiif_prefix, rosetta_thumb = _urls(sid)
        assert any(u.startswith(iiif_prefix) for u in calls), "IIIF is still tried first"
        assert rosetta_thumb in calls

    def test_rosetta_failure_still_returns_none(self):
        sid = "990000000000000002"
        fake_get, _ = _router(sid, thumb_status=404, thumb_ct="text/html", thumb_body=b"nope")
        _p1, _p2 = _intercept(fake_get)
        with _p1, _p2:
            assert _fetch(sid) is None

    def test_rosetta_html_body_is_not_an_image(self):
        # A challenge/login page with HTTP 200 must not be served as an image.
        sid = "990000000000000003"
        fake_get, _ = _router(sid, thumb_status=200, thumb_ct="text/html;charset=UTF-8", thumb_body=b"<html>" * 100)
        _p1, _p2 = _intercept(fake_get)
        with _p1, _p2:
            assert _fetch(sid) is None


class TestBodleianDirectLinkWiring:
    def test_web_header_offers_direct_link_and_passes_it_to_the_image(self):
        src = open("web/pages/browse.py", encoding="utf-8").read()
        assert "get_oxford_direct_image_url as _ox_direct_url" in src
        assert "tr('Open in Bodleian Libraries'), _ox_direct_link, new_tab=True" in src
        assert 'data-oxford-direct="{_ox_direct_attr}"' in src
        assert "html_module.escape(_ox_direct_link, quote=True)" in src

    def test_js_placeholder_renders_the_link_safely(self):
        js = open("web/static/manuscript_viewer.js", encoding="utf-8").read()
        assert "img.dataset.oxfordDirect" in js
        assert "a.textContent = label" in js          # no innerHTML with the label
        assert "hebrew\\.bodleian\\.ox\\.ac\\.uk" in js  # host-pinned before rendering

    def test_desktop_notice_links_the_failed_bodleian_url(self):
        src = open("desktop/viewers.py", encoding="utf-8").read()
        assert 'startswith("https://hebrew.bodleian.ox.ac.uk/")' in src
        assert 'tr("Open in Bodleian Libraries")' in src
        assert "setOpenExternalLinks(True)" in src

    def test_translation_key_exists_in_hebrew(self):
        src = open("genizah_translations.py", encoding="utf-8").read()
        assert '"Open in Bodleian Libraries"' in src

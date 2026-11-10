"""Regressions for the two Codex findings in round 3 on PR #333 (2026-09-02).

P2 `web/static/manuscript_viewer.js` — the REVERSE fallback: a page explicitly
showing NLI whose NLI image fails retries `/api/oxford_image/`. On success the
footer kept crediting and linking NLI under a Bodleian image.

P2 `web/api.py` — the breaker re-check between the IIIF attempt and the Rosetta
fallback could be tripped by the IIIF failure just recorded, skipping the healthy
Rosetta host in exactly the IIIF-down/Rosetta-up case the fallback exists for.
"""
from __future__ import annotations

import ast
import json
import re
from unittest.mock import patch

import pytest
from fastapi import FastAPI

FL = "168181477"
PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 2500


def _read(path):
    return open(path, encoding="utf-8").read()


class TestReverseFallbackCredit:
    def test_oxford_retry_restores_the_oxford_credit(self):
        js = _read("web/static/manuscript_viewer.js")
        i = js.index("const oxfordUrl = ")
        window = js[i:i + 700]
        assert "switchImageCredit('oxford')" in window, (
            "the Oxford retry's onload must restore the credit it is displaying"
        )

    def test_both_directions_are_covered(self):
        js = _read("web/static/manuscript_viewer.js")
        assert js.count("switchImageCredit('nli')") == 2      # manifest + server proxy
        assert js.count("switchImageCredit('oxford')") == 1   # oxford retry

    def test_helper_reads_both_credit_variants(self):
        js = _read("web/static/manuscript_viewer.js")
        i = js.index("function switchImageCredit(")
        body = js[i:i + 900]
        assert "creditNli" in body and "creditOxford" in body
        assert "linkNli" in body and "linkOxford" in body


class TestBreakerDoesNotSkipRosetta:
    def test_no_breaker_recheck_between_iiif_and_rosetta(self):
        src = _read("web/api.py")
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "_fetch_nli_image_bytes")
        body = ast.get_source_segment(src, fn)
        rosetta_at = body.index("dps_func=thumbnail")
        # the IIIF request that precedes it in the same _try_fl
        iiif_at = body.index("iiif_url = ")
        between = body[iiif_at:rosetta_at]
        assert "_nli_circuit_is_open()" not in between, (
            "the IIIF failure recorded moments earlier can trip the breaker; "
            "re-checking here skips the still-healthy Rosetta host"
        )

    def test_try_fl_still_checks_the_breaker_on_entry(self):
        src = _read("web/api.py")
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "_fetch_nli_image_bytes")
        body = ast.get_source_segment(src, fn)
        inner = body[body.index("def _try_fl("):]
        head = inner[:inner.index("iiif_url = ")]
        assert "_nli_circuit_is_open()" in head, "later FL ids must still short-circuit"


# --- behavioural: the breaker opening on the IIIF 5xx must not lose the image ---

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


def test_rosetta_is_reached_when_the_iiif_failure_opens_the_breaker():
    """The breaker flips open right after the IIIF 5xx is recorded."""
    import web.api as api_mod

    sys_id = "990000000000000011"
    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
    iiif_prefix = f"https://iiif.nli.org.il/IIIFv21/FL{FL}/full/"
    rosetta = ("https://rosetta.nli.org.il/delivery/DeliveryManagerServlet"
               f"?dps_func=thumbnail&dps_pid=FL{FL}")
    state = {"iiif_failed": False}

    def fake_get(url, *a, **kw):
        if url == manifest_url:
            return _manifest()
        if url.startswith(iiif_prefix):
            state["iiif_failed"] = True      # this is the failure that trips it
            return _Resp(500, "text/xml", b"<error/>")
        raise AssertionError(f"unexpected requests.get {url}")

    def fake_nli_get(url, *a, **kw):
        assert url == rosetta, url
        return _Resp(200, "image/png;charset=UTF-8", PNG_BYTES)

    def breaker():
        # closed until the IIIF attempt fails, open afterwards
        return state["iiif_failed"]

    with patch.object(api_mod._nli_session, "get", side_effect=fake_get), \
         patch("web.api.requests.get", side_effect=fake_get), \
         patch("web.api.nli_image_get", side_effect=fake_nli_get), \
         patch("web.api._nli_circuit_is_open", side_effect=breaker):
        got = api_mod._api_test_seam["_fetch_nli_image_bytes"](sys_id, 0)

    assert got is not None, "the breaker opening on the IIIF 5xx must not skip Rosetta"
    assert got[0] == PNG_BYTES
    assert got[1] == "image/png"


def test_rosetta_uses_the_shared_wrapper_not_requests(_clear_fl_cache=None):
    src = _read("web/api.py")
    fn = next(n for n in ast.walk(ast.parse(src))
              if isinstance(n, ast.FunctionDef) and n.name == "_fetch_nli_image_bytes")
    body = ast.get_source_segment(src, fn)
    i = body.index("dps_func=thumbnail")
    assert re.search(r"nli_image_get\(\s*\n?\s*rosetta_thumb", body[i:i + 900])

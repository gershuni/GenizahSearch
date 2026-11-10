"""Regressions for the three Codex findings in round 4 on PR #333 (2026-09-02).

P1 `shared/fgp_service.py` — when the coverage pick failed the folio text check,
the replacement was chosen on similarity alone. `_content_similarity` divides by
the SMALLER token set, so a one-word excerpt sharing one word with the folio
scores 1.0 and would be promoted over V0.8, walking past the low-coverage
demotion (SEED-030) the function exists to enforce.

P2 `web/api.py` — the IIIF timeout / ConnectionError (and so SSLError) handlers
returned before the Rosetta fallback, so an unreachable or TLS-failing IIIF host
produced no image even with Rosetta healthy.

P2 `web/pages/browse.py` — `state.highlight_terms` survived navigation to another
manuscript, so a later manuscript could default to whichever transcription
happened to contain the PREVIOUS manuscript's hit.
"""
from __future__ import annotations

import ast
import json
from unittest.mock import patch

import pytest
import requests
from fastapi import FastAPI

from shared.fgp_service import choose_default_source

FL = "168181477"
PNG_BYTES = b"\x89PNG\r\n\x1a\n" + b"\x00" * 2500


def _read(path):
    return open(path, encoding="utf-8").read()


def _ed(content, source_id):
    return {
        "id": source_id, "content": content, "doc_relation": "Digital Edition",
        "source": "fgp", "is_fgp": True, "text_source": "FGP",
        "c_number": None, "image_side": None, "language": "Hebrew",
    }


FOLIO_HTR = (
    "תקום רבה דיניך וכהדרי אלצדקת עיניך ולבך מביט ועיניך וג במקום אחד "
    "אהובים היום נדמו כמלאכים בקומה זקופה כמעמד מל גנונים אגודים במ דוברים "
    "קדוש וברוך כמ הם לובשי לובן כמ ובמו אין אכילה ושתיה כמ זימון שינה מפרידים"
)
UNRELATED_LONG = " ".join([
    "ברכת מזון אברך לאל אמונה בחלק שבעה ושמונה גנונה מר אש אמנה דיצתה",
    "אנקת שיח שועי בזאת חנוכה קשבת ישעי גדעתה קרן מרשיעי דלתות היכלך",
] * 6)


class TestReplacementMustClearCoverage:
    def test_one_word_sliver_is_not_promoted_over_v08(self):
        # "תקום" alone: similarity 1.0 (divides by the smaller set) but a few
        # letters against a ~200-letter folio -- far below the coverage threshold.
        sliver = "תקום"
        d = choose_default_source([_ed(UNRELATED_LONG, 1), _ed(sliver, 2)], FOLIO_HTR,
                                  full_htr_getter=lambda: FOLIO_HTR)
        assert d["eligible"] is False, (
            f"a {len(sliver)}-char excerpt must not become the default "
            f"(reason={d['reason']}, id={(d.get('source') or {}).get('id')})"
        )
        assert d["reason"] == "demote_no_text_match"

    def test_a_substantial_matching_edition_is_still_promoted(self):
        d = choose_default_source([_ed(UNRELATED_LONG, 1), _ed(FOLIO_HTR, 2)], FOLIO_HTR,
                                  full_htr_getter=lambda: FOLIO_HTR)
        assert d["eligible"] is True
        assert d["source"]["id"] == 2
        assert d["reason"] == "fgp_text_match"
        assert d["ratio"] is not None, "the promoted edition reports its coverage"

    def test_replacement_loop_applies_the_coverage_threshold(self):
        src = _read("shared/fgp_service.py")
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "choose_default_source")
        body = ast.get_source_segment(src, fn)
        i = body.index("best_match, best_sim")
        window = body[i:i + 1400]
        assert "threshold" in window, "the replacement must be coverage-checked"
        assert "_COVERAGE_MIN_HTR_LETTERS" in window


class TestIiifTransportFailuresStillTryRosetta:
    def test_handlers_do_not_return_before_the_fallback(self):
        src = _read("web/api.py")
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "_fetch_nli_image_bytes")
        body = ast.get_source_segment(src, fn)
        head = body[:body.index("dps_func=thumbnail")]
        tail = head[head.index("except requests.exceptions.Timeout"):]
        assert "return None" not in tail, (
            "a timeout/connection/TLS failure must fall through to Rosetta"
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


@pytest.mark.parametrize("exc,label", [
    (requests.exceptions.SSLError("bad chain"), "TLS"),
    (requests.exceptions.ConnectionError("unreachable"), "connection"),
    (requests.exceptions.Timeout("slow"), "timeout"),
])
def test_rosetta_serves_the_image_when_iiif_transport_fails(exc, label):
    import web.api as api_mod

    sys_id = "9900000000000002" + str(abs(hash(label)) % 90 + 10)
    manifest_url = f"https://iiif.nli.org.il/IIIFv21/DOCID/PNX_MANUSCRIPTS{sys_id}-1/manifest"
    iiif_prefix = f"https://iiif.nli.org.il/IIIFv21/FL{FL}/full/"

    def fake_get(url, *a, **kw):
        if url == manifest_url:
            return _manifest()
        if url.startswith(iiif_prefix):
            raise exc
        raise AssertionError(f"unexpected requests.get {url}")

    def fake_nli_get(url, *a, **kw):
        assert "dps_func=thumbnail" in url, url
        return _Resp(200, "image/png;charset=UTF-8", PNG_BYTES)

    with patch.object(api_mod._nli_session, "get", side_effect=fake_get), \
         patch("web.api.requests.get", side_effect=fake_get), \
         patch("web.api.nli_image_get", side_effect=fake_nli_get), \
         patch("web.api._nli_circuit_is_open", return_value=False):
        got = api_mod._api_test_seam["_fetch_nli_image_bytes"](sys_id, 0)

    assert got is not None, f"an IIIF {label} failure must still reach Rosetta"
    assert got[0] == PNG_BYTES


class TestSearchScopeDoesNotFollowTheReader:
    def test_browse_clears_the_phrase_on_a_manuscript_change(self):
        src = _read("web/pages/browse.py")
        assert "def _clear_search_scope_for_new_manuscript(" in src
        i = src.index("def _clear_search_scope_for_new_manuscript(")
        body = src[i:i + 900]
        assert "state.highlight_terms = None" in body
        assert "_url_state['highlight'] = None" in body

    def test_every_manuscript_change_site_clears_it_first(self):
        src = _read("web/pages/browse.py")
        lines = src.split("\n")
        assigns = [k for k, l in enumerate(lines)
                   if l.strip().startswith("state.sys_id = ")
                   and "initial_sys_id" not in l
                   and "bootstrap[" not in l
                   and "page.sys_id" not in l
                   and "meta_page.sys_id" not in l]
        assert assigns, "no navigation assignments found — has the page been refactored?"
        for k in assigns:
            preceding = "\n".join(lines[max(0, k - 3):k])
            assert "_clear_search_scope_for_new_manuscript(" in preceding, (
                f"line {k + 1}: {lines[k].strip()} does not clear the search scope first"
            )

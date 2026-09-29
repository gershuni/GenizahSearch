# -*- coding: utf-8 -*-
"""Sefaria filter texts are cached per exact reference.

The /parallels page (``web.pages.parallels.fetch_sefaria_text``) and the
desktop "Filter Text" dialog (``desktop.filter_text_dialog.SefariaFetchThread``)
keep the Hebrew text of each Sefaria reference in a local cache folder. Every
distinct reference must get its own cache entry: two references whose names
reduce to the same ASCII stem (any two Hebrew references, or two long English
titles that share their first 50 characters) must each be fetched and each
return their own text.

The tests point the home folder at a temp dir (so the real cache is never
touched) and replace ``requests.get`` with a fake that answers per reference
and counts calls.
"""
import json
import os
from urllib.parse import unquote, urlsplit

import pytest
import requests


GENESIS_REF = "בראשית א"
EXODUS_REF = "שמות ב"
LONG_REF_A = "Annotations of Minchat Chinukh on Mishneh Torah, Sabbath"
LONG_REF_B = "Annotations of Minchat Chinukh on Mishneh Torah, Sanhedrin"

TEXTS = {
    GENESIS_REF: "בראשית ברא אלהים",
    EXODUS_REF: "וילך איש מבית לוי",
    LONG_REF_A: "טקסט ראשון לדוגמה",
    LONG_REF_B: "טקסט שני לדוגמה",
}


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload


def _ref_from_url(url):
    path = urlsplit(url).path
    marker = "/texts/"
    return unquote(path[path.index(marker) + len(marker):])


@pytest.fixture
def isolated_cache(tmp_path, monkeypatch):
    """Home folder -> temp dir, and a per-reference fake for requests.get."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))
    calls = []

    def fake_get(url, *args, **kwargs):
        ref = _ref_from_url(url)
        calls.append(ref)
        text = TEXTS.get(ref)
        if text is None:
            return _FakeResponse({}, status_code=404)
        # v3 shape (Tanakh "Text Only") and v2 shape ("he") in one payload.
        return _FakeResponse({"he": text, "versions": [{"language": "he", "text": text}]})

    monkeypatch.setattr(requests, "get", fake_get)
    return home, calls


def _cache_dir(home):
    return os.path.join(str(home), ".genizah_search", "sefaria_cache")


# ---------------------------------------------------------------------------
# Web: web.pages.parallels.fetch_sefaria_text
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("first,second", [
    (GENESIS_REF, EXODUS_REF),
    (LONG_REF_A, LONG_REF_B),
], ids=["hebrew-refs", "long-english-titles"])
def test_web_fetch_returns_each_reference_own_text(isolated_cache, first, second):
    from web.pages.parallels import fetch_sefaria_text

    _home, calls = isolated_cache
    assert fetch_sefaria_text(first) == TEXTS[first]
    assert fetch_sefaria_text(second) == TEXTS[second]
    assert calls.count(first) >= 1 and calls.count(second) >= 1
    assert len(set(calls)) == 2


def test_web_fetch_reuses_cache_for_same_reference(isolated_cache):
    from web.pages.parallels import fetch_sefaria_text

    _home, calls = isolated_cache
    assert fetch_sefaria_text(GENESIS_REF) == TEXTS[GENESIS_REF]
    n = len(calls)
    assert fetch_sefaria_text(GENESIS_REF) == TEXTS[GENESIS_REF]
    assert fetch_sefaria_text("  " + GENESIS_REF + " ") == TEXTS[GENESIS_REF]
    assert len(calls) == n


def test_web_fetch_ignores_old_cache_files(isolated_cache):
    """A file written under the previous naming scheme is never read."""
    from web.pages.parallels import fetch_sefaria_text

    home, calls = isolated_cache
    folder = _cache_dir(home)
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "cache_v2.txt"), "w", encoding="utf-8") as fh:
        fh.write("טקסט ישן")

    assert fetch_sefaria_text(EXODUS_REF) == TEXTS[EXODUS_REF]
    assert calls == [EXODUS_REF]


# ---------------------------------------------------------------------------
# Shared helper
# ---------------------------------------------------------------------------

def test_cache_path_distinct_per_reference_and_stays_in_folder(tmp_path):
    from shared.sefaria_utils import sefaria_cache_path

    folder = str(tmp_path)
    refs = [GENESIS_REF, EXODUS_REF, LONG_REF_A, LONG_REF_B, "../../etc/passwd", "", "a/b\\c"]
    paths = [sefaria_cache_path(r, cache_dir=folder) for r in refs]
    assert len(set(paths)) == len(paths)
    for p in paths:
        assert os.path.dirname(p) == folder
        assert os.path.basename(p).endswith("_v3.txt")
    # Surrounding whitespace does not make a new entry.
    assert sefaria_cache_path(" " + GENESIS_REF, cache_dir=folder) == paths[0]


def test_cache_entry_for_another_reference_is_a_miss(tmp_path):
    """An entry records its reference; a file holding another one is not used."""
    from shared.sefaria_utils import (
        read_sefaria_cache, sefaria_cache_path, write_sefaria_cache)

    folder = str(tmp_path)
    write_sefaria_cache(GENESIS_REF, TEXTS[GENESIS_REF], cache_dir=folder)
    assert read_sefaria_cache(GENESIS_REF, cache_dir=folder) == TEXTS[GENESIS_REF]

    other = sefaria_cache_path(EXODUS_REF, cache_dir=folder)
    with open(other, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(GENESIS_REF) + "\n" + TEXTS[GENESIS_REF])
    assert read_sefaria_cache(EXODUS_REF, cache_dir=folder) == ""


# ---------------------------------------------------------------------------
# Desktop: desktop.filter_text_dialog.SefariaFetchThread
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("first,second", [
    (GENESIS_REF, EXODUS_REF),
    (LONG_REF_A, LONG_REF_B),
], ids=["hebrew-refs", "long-english-titles"])
def test_desktop_fetch_thread_returns_each_reference_own_text(isolated_cache, first, second):
    pytest.importorskip("PyQt6")
    from PyQt6.QtWidgets import QApplication

    _app = QApplication.instance() or QApplication([])  # noqa: F841
    from desktop.filter_text_dialog import SefariaFetchThread

    _home, calls = isolated_cache
    results = {}

    t1 = SefariaFetchThread([first])
    t1.finished.connect(results.update)
    t1.run()
    t2 = SefariaFetchThread([second])
    t2.finished.connect(results.update)
    t2.run()

    assert results == {first: TEXTS[first], second: TEXTS[second]}
    assert calls == [first, second]

    # A second run for the same reference is served from the cache.
    again = {}
    t3 = SefariaFetchThread([first])
    t3.finished.connect(again.update)
    t3.run()
    assert again == {first: TEXTS[first]}
    assert calls == [first, second]

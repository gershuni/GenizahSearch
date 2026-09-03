# -*- coding: utf-8 -*-
"""Debug session oxford-fgp-image-mismatch — sub-issues A & C.

A: Oxford's image host now fronts an "Anubis" bot-challenge that returns
   HTTP 200 with Content-Type text/html instead of JPEG bytes. The desktop
   image loader accepted status_code == 200 unconditionally, so it silently
   downloaded 7KB of HTML, failed to decode it as an image, and only THEN
   surfaced a plain "No Image" (no distinction from a genuine decode error,
   no auto-fallback to the NLI image list that HAS the folio).

C: When the user switches image source (Oxford <-> NLI), `_on_source_changed`
   reused the raw index from the OLD list against the NEW list, resetting to
   0 whenever the new list is shorter than the old index -- e.g. Oxford's
   164-page whole-codex list at index ~53 (folio 27b) resets to NLI index 0
   (folio 27r) instead of the matching 27v.

Both fixes share one pure helper: ``map_matching_image_index`` in
``desktop/widgets/__init__.py``. It never touches Qt state, so it is fully
unit-testable without a QApplication.
"""
from __future__ import annotations

import pytest

from desktop.widgets import map_matching_image_index


def _oxford_g2_images():
    """Real label convention for MS. Heb. g. 2, folios 26-28 (verified
    against oxford_full_db.json: {"label": "27a", ...}, {"label": "27b", ...}).
    """
    images = []
    for folio in range(26, 29):
        images.append({'label': f'{folio}a', 'folio_num': folio})
        images.append({'label': f'{folio}b', 'folio_num': folio})
    return images


def _nli_folio_27_images():
    """NLI's per-part list for folio 27: 2 canvases, recto then verso.
    NLI entries carry only 'label'/'fl_id' -- no folio_num/folio_side."""
    return [
        {'label': 'צד א', 'fl_id': '168181474'},
        {'label': 'צד ב', 'fl_id': '168181475'},
    ]


class TestMapMatchingImageIndexSideMatch:
    """The reported case: a large Oxford list -> a 2-item NLI per-part list."""

    def test_verso_label_maps_to_nli_index_1(self):
        images = _oxford_g2_images()
        # folio 27b is index 3 (26a,26b,27a,27b,...)
        idx = images.index({'label': '27b', 'folio_num': 27})
        assert idx == 3
        nli = _nli_folio_27_images()
        assert map_matching_image_index(images, idx, nli) == 1

    def test_recto_label_maps_to_nli_index_0(self):
        images = _oxford_g2_images()
        idx = images.index({'label': '27a', 'folio_num': 27})
        assert idx == 2
        nli = _nli_folio_27_images()
        assert map_matching_image_index(images, idx, nli) == 0

    def test_does_not_reset_to_zero_for_verso(self):
        """Regression guard for the exact reported symptom: switching FROM
        a page deep in a long list TO a short list must not silently land
        on the short list's first (recto) entry when the source page was
        verso."""
        images = _oxford_g2_images()
        idx = images.index({'label': '27b', 'folio_num': 27})
        nli = _nli_folio_27_images()
        result = map_matching_image_index(images, idx, nli)
        assert result != 0
        assert result == 1


class TestMapMatchingImageIndexFallback:
    def test_proportional_fallback_when_new_list_not_pair(self):
        old = [{'label': str(i)} for i in range(10)]
        new = [{'label': str(i)} for i in range(5)]
        # idx 9 of 10 (last) -> should map to last of new (index 4)
        assert map_matching_image_index(old, 9, new) == 4
        # idx 0 -> first
        assert map_matching_image_index(old, 0, new) == 0
        # idx 4 (middle-ish of 0..9) -> proportional middle of 0..4
        assert map_matching_image_index(old, 4, new) == round(4 / 9 * 4)

    def test_bare_numeric_label_no_side_falls_to_proportional(self):
        old = [{'label': str(i)} for i in range(4)]
        new = [{'label': 'x'}, {'label': 'y'}]
        # No a/b/r/v suffix on old[3]='3' -> proportional, not side-match
        assert map_matching_image_index(old, 3, new) == 1

    def test_empty_new_list_returns_zero(self):
        assert map_matching_image_index(_oxford_g2_images(), 3, []) == 0

    def test_empty_old_list_returns_zero(self):
        assert map_matching_image_index([], 0, _nli_folio_27_images()) == 0

    def test_out_of_range_old_idx_returns_zero(self):
        images = _oxford_g2_images()
        assert map_matching_image_index(images, 999, _nli_folio_27_images()) == 0
        assert map_matching_image_index(images, -1, _nli_folio_27_images()) == 0

    def test_none_old_idx_returns_zero(self):
        assert map_matching_image_index(_oxford_g2_images(), None, _nli_folio_27_images()) == 0

    def test_single_item_old_list_side_suffix_still_maps(self):
        # A single-entry old list with a recognizable side suffix ('1a')
        # should still hit the side-match branch, not the proportional
        # fallback (which would need len(old_list) > 1).
        old = [{'label': '1a'}]
        assert map_matching_image_index(old, 0, [{'label': 'x'}, {'label': 'y'}]) == 0


# ---------------------------------------------------------------------------
# desktop/image_loader.py — Content-Type guard (sub-issue A part 1)
# ---------------------------------------------------------------------------
try:
    from desktop.image_loader import ImageLoaderThread

    IMAGE_LOADER_AVAILABLE = True
except Exception:  # pragma: no cover - environment-dependent (headless QtGui)
    ImageLoaderThread = None
    IMAGE_LOADER_AVAILABLE = False

OXFORD_URL = "https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_g_2_27b.jpg"


class _RespCT:
    """Minimal requests.Response stand-in that carries a headers dict
    (the shared test_desktop_image_loader_breaker._Resp fixture predates
    this and has no .headers -- separate class here to avoid coupling)."""

    def __init__(self, status_code=200, content=b"IMG", content_type="image/jpeg"):
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Type": content_type} if content_type is not None else {}


@pytest.mark.skipif(
    not IMAGE_LOADER_AVAILABLE,
    reason="desktop.image_loader unavailable (PyQt6 QtGui could not load)",
)
class TestDownloadBytesContentTypeGuard:
    def _loader(self):
        loader = ImageLoaderThread.__new__(ImageLoaderThread)
        loader._cancelled = False
        return loader

    def test_200_html_bot_challenge_returns_none(self, monkeypatch):
        """The Bodleian 'Anubis' challenge page: HTTP 200, text/html.
        Must be treated as a failure, not silently returned as image bytes."""
        import desktop.image_loader as il

        monkeypatch.setattr(
            il, "nli_image_get",
            lambda *a, **k: _RespCT(200, b"<html>Making sure you're not a bot!</html>", "text/html"),
        )
        loader = self._loader()
        assert loader._download_bytes(OXFORD_URL, {}) is None

    def test_200_image_content_type_still_returns_content(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(
            il, "nli_image_get",
            lambda *a, **k: _RespCT(200, b"\xff\xd8\xff\xe0JPEGDATA", "image/jpeg"),
        )
        loader = self._loader()
        assert loader._download_bytes(OXFORD_URL, {}) == b"\xff\xd8\xff\xe0JPEGDATA"

    def test_200_missing_content_type_treated_as_image(self, monkeypatch):
        """Some hosts legitimately omit Content-Type; must not be rejected
        (avoid new false-negatives on hosts that never set the header)."""
        import desktop.image_loader as il

        monkeypatch.setattr(
            il, "nli_image_get",
            lambda *a, **k: _RespCT(200, b"DATA", content_type=None),
        )
        loader = self._loader()
        assert loader._download_bytes(OXFORD_URL, {}) == b"DATA"

    def test_200_content_type_with_charset_suffix_still_accepted(self, monkeypatch):
        import desktop.image_loader as il

        monkeypatch.setattr(
            il, "nli_image_get",
            lambda *a, **k: _RespCT(200, b"DATA", "image/jpeg; charset=binary"),
        )
        loader = self._loader()
        assert loader._download_bytes(OXFORD_URL, {}) == b"DATA"


# ---------------------------------------------------------------------------
# desktop/viewers.py::ManuscriptViewerWidget._on_image_load_failed
# (sub-issue A part 2 — auto-fallback wiring)
# ---------------------------------------------------------------------------
# GUI-marked: constructs a real ManuscriptViewerWidget (QWidget subclass).
# Deselected from the main `tests` job (-m "not gui"), run in the dedicated
# gui-tests job. See the `gui` marker docstring in pyproject.toml.

pytestmark_gui = pytest.mark.gui


def _oxford_widget(monkeypatch):
    """Build a ManuscriptViewerWidget wired up as if load_images() had just
    populated it with the reported manuscript (Oxford ext list showing
    folio 27b, an aligned 2-item NLI list for the same part) -- without
    going through load_images()'s real Config/MetadataManager dependencies."""
    import sys
    from PyQt6.QtWidgets import QApplication

    QApplication.instance() or QApplication(sys.argv[:1])
    from desktop.viewers import ManuscriptViewerWidget

    w = ManuscriptViewerWidget()
    w.external_provider = "oxford"
    w.images_ext = _oxford_g2_images()
    w.images_nli = _nli_folio_27_images()
    w.active_list = w.images_ext
    w.current_source = "ext"
    w.current_idx = w.images_ext.index({'label': '27b', 'folio_num': 27})
    w._nli_fallback_active = False
    w._closing = False
    w.combo_source.clear()
    w.combo_source.addItem("Oxford (164 pages)", "ext")
    w.combo_source.addItem("NLI (2 pages)", "nli")
    return w


@pytest.mark.gui
class TestOnImageLoadFailedAutoFallback:
    def test_oxford_failure_switches_to_nli_with_matching_side(self, monkeypatch):
        w = _oxford_widget(monkeypatch)
        gen = w._load_generation

        w._on_image_load_failed(gen)

        assert w.current_source == "nli"
        assert w.active_list is w.images_nli
        # folio 27b (verso) -> NLI index 1 (same mapping as sub-issue C)
        assert w.current_idx == 1
        assert w.combo_source.currentData() == "nli"

    def test_oxford_failure_sets_fallback_flag_and_notice(self, monkeypatch):
        w = _oxford_widget(monkeypatch)
        gen = w._load_generation

        w._on_image_load_failed(gen)

        assert w._nli_fallback_active is True
        # isHidden(), not isVisible() -- the widget is never shown on
        # screen in this headless test, so isVisible() is always False
        # regardless of setVisible(); isHidden() reflects the explicit
        # flag our code sets.
        assert w.lbl_fallback_notice.isHidden() is False
        assert w.lbl_fallback_notice.text()

    def test_stale_generation_is_ignored(self, monkeypatch):
        """A load_failed signal for a page the user has already navigated
        away from must not resurrect it as the active page."""
        w = _oxford_widget(monkeypatch)
        stale_gen = w._load_generation
        w._load_generation += 1  # simulate a newer set_page() in flight

        w._on_image_load_failed(stale_gen)

        assert w.current_source == "ext"
        assert w._nli_fallback_active is False

    def test_no_fallback_when_nli_list_empty(self, monkeypatch):
        w = _oxford_widget(monkeypatch)
        w.images_nli = []
        gen = w._load_generation

        w._on_image_load_failed(gen)

        assert w.current_source == "ext"
        assert w._nli_fallback_active is False

    def test_no_fallback_when_already_active(self, monkeypatch):
        """Second failure after an already-active fallback (e.g. the NLI
        image itself also failed) must not loop -- current_source is
        already 'nli' by then so the guard clause's provider/source check
        fails naturally, but pin the already-active case explicitly."""
        w = _oxford_widget(monkeypatch)
        w._nli_fallback_active = True
        gen = w._load_generation

        w._on_image_load_failed(gen)

        # Falls through to plain "No Image" -- source/list untouched.
        assert w.active_list is w.images_ext
        assert w.current_source == "ext"

    def test_non_oxford_provider_does_not_auto_fallback(self, monkeypatch):
        """Scope guard: sub-issue A is Oxford-only; a Cambridge/CUDL image
        failure must keep showing 'No Image' rather than silently jumping
        to NLI (that surface has its own alignment-based default logic)."""
        w = _oxford_widget(monkeypatch)
        w.external_provider = "cambridge"
        gen = w._load_generation

        w._on_image_load_failed(gen)

        assert w.current_source == "ext"
        assert w._nli_fallback_active is False

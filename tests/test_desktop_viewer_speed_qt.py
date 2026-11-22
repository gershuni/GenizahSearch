# -*- coding: utf-8 -*-
"""The desktop viewer's faster pages (2026-11-22).

- Every IIIF library's page has a 400-px preview (Cambridge had none: the reader waited
  about 3 s for the 2000-px image), loaded through the cached loader; none when the sharp
  image is on disk already.
- A preview arriving after the sharp image does not replace it.
- A page decoded in memory is drawn at once: no debounce, no thread.
- Browse can start the pending page at once (flush_pending_page), before it builds the
  manuscript's panels.
- default_source / default_page_list give the list load_images shows (the prefetch's rule).
Loaders are stand-ins; nothing touches the network.
"""
from __future__ import annotations

import pytest
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtGui import QColor, QImage

from desktop import image_loader, viewers
from desktop.viewers import ManuscriptViewerWidget, default_page_list, default_source

pytestmark = pytest.mark.gui

CUDL = "https://images.lib.cam.ac.uk/iiif/MS-TS-00013-J-00019-00011-000-0000{}.jp2"
STARTED = []


class _Loader(QObject):
    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal()
    finished = pyqtSignal()

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.cancelled = False

    def start(self):
        STARTED.append(self)

    def isRunning(self):
        return False

    def cancel(self):
        self.cancelled = True

    def deleteLater(self):
        pass


def _img(w):
    img = QImage(w, w, QImage.Format.Format_RGB32)
    img.fill(QColor(10, 20, 30))
    return img


@pytest.fixture
def viewer(monkeypatch, tmp_path):
    STARTED.clear()
    monkeypatch.setattr(viewers, "ImageLoaderThread", _Loader)
    monkeypatch.setattr(image_loader.Config, "IMAGE_CACHE_DIR", str(tmp_path))
    image_loader.clear_memory()
    w = ManuscriptViewerWidget()
    shown = []
    real = w.scroll_area.set_image
    w.scroll_area.set_image = lambda pix: (shown.append(pix.width() if pix else None), real(pix))
    w.shown = shown
    yield w
    w._nav_debounce_timer.stop()
    w.close()
    image_loader.clear_memory()


def _cudl_meta(n=2):
    return {'external_provider': 'cambridge',
            'images_ext': [{'label': str(i + 1), 'url': CUDL.format(i + 1)} for i in range(n)]}


def test_a_cambridge_page_has_a_preview_started_with_its_sharp_image(viewer):
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    urls = [t.url for t in STARTED]
    assert f"{CUDL.format(1)}/full/400,/0/default.jpg" in urls, urls
    assert f"{CUDL.format(1)}/full/2000,/0/default.jpg" in urls
    assert urls.index(f"{CUDL.format(1)}/full/400,/0/default.jpg") < urls.index(f"{CUDL.format(1)}/full/2000,/0/default.jpg")


NLI = "https://iiif.nli.org.il/IIIFv21/FL777"


def _nli_meta():
    return {'images_nli': [{'label': '1r', 'url': NLI, 'fl_id': '777'}]}


def test_no_preview_when_the_sharp_image_is_on_disk(viewer, tmp_path):
    full = f"{CUDL.format(1)}/full/2000,/0/default.jpg"
    assert _img(1300).save(image_loader.cache_path_for(full), "JPEG")
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    assert [t.url for t in STARTED][:1] == [full]
    assert not [t for t in STARTED if '/full/400,' in t.url]


def test_no_preview_when_an_nli_page_is_on_disk(viewer, tmp_path):
    path = image_loader.cache_path_for(NLI)
    assert path.startswith(str(tmp_path))
    assert _img(1300).save(path, "JPEG")
    assert image_loader.is_on_disk(NLI)
    viewer.load_images(_nli_meta())
    viewer.flush_pending_page()
    assert not [t for t in STARTED if '/full/400,' in t.url]


@pytest.mark.parametrize("make", ["preview_sized_page", "unreadable"])
def test_a_file_the_loader_would_refuse_does_not_suppress_the_preview(viewer, make):
    """Codex on #98: a preview-sized FL*_v2.jpg (a file from before GS-66, a Rosetta thumbnail)
    or an unreadable file counted as on disk, so no preview started; the loader then refused
    the file and downloaded the page, and the pane stayed at "Loading..." meanwhile."""
    path = image_loader.cache_path_for(f"{NLI}/full/2000,/0/default.jpg")
    if make == "preview_sized_page":
        assert _img(400).save(path, "JPEG")
    else:
        with open(path, "wb") as f:
            f.write(b"x")
    assert not image_loader.is_on_disk(NLI)
    viewer.load_images(_nli_meta())
    viewer.flush_pending_page()
    assert f"{NLI}/full/400,/0/default.jpg" in [t.url for t in STARTED]


def test_a_late_preview_does_not_replace_the_sharp_image(viewer):
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    preview = next(t for t in STARTED if '/full/400,' in t.url)
    full = next(t for t in STARTED if '/full/2000,' in t.url)
    full.image_loaded.emit(_img(1300))
    preview.image_loaded.emit(_img(400))
    assert viewer.shown[-1] == 1300, viewer.shown


def test_a_preview_first_then_the_sharp_image(viewer):
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    preview = next(t for t in STARTED if '/full/400,' in t.url)
    full = next(t for t in STARTED if '/full/2000,' in t.url)
    preview.image_loaded.emit(_img(400))
    full.image_loaded.emit(_img(1300))
    assert viewer.shown[-2:] == [400, 1300]


def test_a_page_in_memory_is_drawn_at_once(viewer):
    full = f"{CUDL.format(2)}/full/2000,/0/default.jpg"
    image_loader._remember(image_loader.cache_path_for(full), _img(1300))
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    STARTED.clear()
    viewer.set_page(1)                       # page 2, decoded in memory
    assert viewer.shown[-1] == 1300, 'not drawn at once'
    assert not viewer._nav_debounce_timer.isActive()
    assert not [t for t in STARTED if t.url in (full, f"{CUDL.format(2)}/full/400,/0/default.jpg")]


def test_flush_starts_the_pending_page_now(viewer):
    viewer.load_images(_cudl_meta())
    assert viewer._nav_debounce_timer.isActive() and not STARTED
    viewer.flush_pending_page()
    assert not viewer._nav_debounce_timer.isActive()
    assert STARTED
    count = len(STARTED)
    viewer.flush_pending_page()              # nothing pending: nothing more
    assert len(STARTED) == count


def test_the_page_asked_for_again_is_not_started_over(viewer):
    """Browse asks for the page again when it renders the page text after load_images: that hid
    the image or its preview ("Loading...") and began the download again (2026-11-22)."""
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    full = next(t for t in STARTED if '/full/2000,' in t.url)
    preview = next(t for t in STARTED if '/full/400,' in t.url)
    preview.image_loaded.emit(_img(400))
    count, shown = len(STARTED), list(viewer.shown)
    viewer.set_page(0)
    assert not viewer._nav_debounce_timer.isActive(), 'started over'
    assert viewer.shown == shown and len(STARTED) == count
    assert viewer.scroll_area._pixmap_item.isVisible(), 'the preview was hidden'
    full.load_failed.emit()                     # failed: asked again, it is tried again
    viewer.set_page(0)
    assert viewer._nav_debounce_timer.isActive()
    viewer.flush_pending_page()
    assert len(STARTED) > count


def test_a_new_list_shows_its_page_even_when_it_is_the_same_image(viewer):
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    count = len(STARTED)
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    assert len(STARTED) > count


def test_another_page_is_started(viewer):
    viewer.load_images(_cudl_meta())
    viewer.flush_pending_page()
    viewer.set_page(1)
    viewer.flush_pending_page()
    assert any(t.url == f"{CUDL.format(2)}/full/2000,/0/default.jpg" for t in STARTED)


def test_a_fitted_image_is_centred_whatever_was_shown_before():
    """The scene's own rect only grows (every image and message it held): a fitted image landed
    off centre, and a preview and its sharp image at different places (2026-11-22)."""
    from PyQt6.QtGui import QPixmap
    from PyQt6.QtTest import QTest
    sa = viewers.ZoomableScrollArea()
    sa.resize(540, 746)
    sa.show()
    QTest.qWait(10)
    try:
        for w, h in ((2400, 3600), (400, 632), (1266, 2000), (900, 400)):
            pix = QPixmap(w, h)
            pix.fill(QColor(10, 20, 30))
            sa.set_status_message("Loading...")
            sa.set_image(pix)
            r = sa.mapFromScene(sa._pixmap_item.sceneBoundingRect()).boundingRect()
            centre = sa.viewport().rect().center()
            assert abs(r.center().x() - centre.x()) <= 2 and abs(r.center().y() - centre.y()) <= 2, (w, h, r)
        sa.rotate_view(90)
        assert sa.sceneRect() == sa._pixmap_item.sceneBoundingRect()
    finally:
        sa.close()


def test_the_credit_line_keeps_its_place_when_hidden(viewer):
    """Browse empties the viewer between two manuscripts: the image grew by that line and
    shrank back, refitted twice."""
    assert viewer.lbl_attribution.sizePolicy().retainSizeWhenHidden()


def test_preview_url_for():
    assert ManuscriptViewerWidget.preview_url_for(CUDL.format(1)) == f"{CUDL.format(1)}/full/400,/0/default.jpg"
    assert ManuscriptViewerWidget.preview_url_for("https://iiif.nli.org.il/IIIFv21/FL1").endswith("/full/400,/0/default.jpg")
    assert ManuscriptViewerWidget.preview_url_for("https://hebrew.bodleian.ox.ac.uk/fragments/full/X_1a.jpg") is None
    assert ManuscriptViewerWidget.preview_url_for(f"{CUDL.format(1)}/full/2000,/0/default.jpg") is None
    assert ManuscriptViewerWidget.preview_url_for("") is None


@pytest.mark.parametrize("meta, expected", [
    (_cudl_meta(), "ext"),
    ({**_cudl_meta(), 'images_nli': [{'url': 'n1'}, {'url': 'n2'}], 'cambridge_alignment': {'verdict': 'aligned'}}, "ext"),
    ({**_cudl_meta(), 'images_nli': [{'url': 'n1'}, {'url': 'n2'}], 'cambridge_alignment': {'verdict': 'misaligned'}}, "nli"),
    ({**_cudl_meta(2), 'images_nli': [{'url': 'n1'}]}, "nli"),                      # legacy count check
    ({'external_provider': 'oxford', 'images_ext': [{'url': 'o.jpg'}], 'images_nli': [{'url': 'n1'}, {'url': 'n2'}]}, "ext"),
    ({'images_nli': [{'url': 'n1'}]}, "nli"),
    ({}, None),
])
def test_default_source_is_the_list_load_images_shows(viewer, meta, expected):
    assert default_source(meta, meta.get('images_ext') or [], meta.get('images_nli') or [],
                          viewers.detect_external_provider(meta)) == expected
    viewer.load_images(meta)
    assert viewer.current_source == expected
    assert default_page_list(meta) == viewer.active_list

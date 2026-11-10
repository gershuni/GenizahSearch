# -*- coding: utf-8 -*-
"""Joins Lab workers: a replaced worker is kept until its QThread finishes, and what it
still emits reaches nobody.

Real JoinWorkbenchWindow / JoinCandidatePane / CandidateCard methods on real QThreads.
The only fakes are the workers' run() bodies (a gate or a sleep instead of the network,
Tantivy or a sidecar database) and the app services the window reads.

Every Lab worker a test creates is also held in KEEP until it has finished, so a worker
the code under test drops makes an assertion fail instead of aborting pytest
("QThread: Destroyed while thread is still running", 0xC0000409). The keeper list is
replaced per test (fresh_orphans), so membership assertions read only this test's
workers and the module list other code relies on is never cleared.
"""
import gc
import re
import sys
import threading
import time
import types
import weakref
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.gui  # real QThreads + a real window: gui bucket only

from PyQt6.QtCore import QCoreApplication, QThread, pyqtSignal  # noqa: E402
from PyQt6.QtGui import QImage  # noqa: E402
from PyQt6.QtTest import QTest  # noqa: E402
from PyQt6.QtWidgets import QApplication, QLabel, QTextBrowser  # noqa: E402

APP = QApplication.instance() or QApplication(sys.argv)

import desktop.gui_threads as gt  # noqa: E402
import desktop.join_workbench as jw  # noqa: E402
from desktop.image_loader import ImageLoaderThread  # noqa: E402

SID = "990000000010205171"
KEEP = []            # every Lab worker a test created, held until it has finished
GATES = []           # every gate a test created; all are opened at teardown
WINDOWS = []
DOWNLOADS = []       # URLs a real ImageLoaderThread tried to download (must stay empty)
_LEFTOVER = []       # threads still running after the teardown wait (never cleared)
GATE = threading.Event()

_LAB_THREADS = ("ImageLoaderThread", "ThumbResolver", "_PageTextWorker",
                "_AnchorLoadWorker", "_KnownJoinsLoadWorker", "ThumbBatchWorker")
_ORIG = {name: getattr(jw, name) for name in _LAB_THREADS}


def _gate():
    g = threading.Event()
    GATES.append(g)
    return g


def _alive(w):
    try:
        return w.isRunning()
    except RuntimeError:
        return False


def _orphans():
    """This test's keeper list (fresh_orphans installed it)."""
    return gt._ORPHANED_WORKERS


def _kept(w):
    return any(x is w for x in _orphans())


def _recording(base, quiet=False):
    """base, recorded in KEEP. quiet: run() does nothing (no network, no sidecar db)."""
    class Recording(base):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            KEEP.append(self)

        if quiet:
            def run(self):
                pass

    Recording.__name__ = base.__name__
    return Recording


@pytest.fixture(autouse=True)
def _private_state(monkeypatch, tmp_path):
    """No test reads or writes the owner's data folder, lists file or image cache."""
    from shared.config import Config
    import shared.lists_manager as lm

    state = str(tmp_path / "state")
    root = Config.INDEX_DIR
    for name, val in list(vars(Config).items()):
        if isinstance(val, str) and val.startswith(root):
            monkeypatch.setattr(Config, name, state + val[len(root):])
    monkeypatch.setattr(Config, "IMAGE_CACHE_DIR", str(tmp_path / "images_cache"))
    monkeypatch.setattr(lm.ListsManager, "LISTS_FILE", str(tmp_path / "lists.pkl"))


@pytest.fixture(autouse=True)
def fresh_orphans(monkeypatch, _private_state):
    """A private keeper list per test; at teardown every gate opens and this test's own
    threads are waited for (bounded) before anything they belong to is dropped."""
    monkeypatch.setattr(gt, "_ORPHANED_WORKERS", [], raising=False)
    errors = []
    monkeypatch.setattr(sys, "excepthook", lambda *a: errors.append(a))
    monkeypatch.setattr(ImageLoaderThread, "_download_bytes",
                        lambda self, url, headers: DOWNLOADS.append(url))
    monkeypatch.setattr(jw, "ImageLoaderThread", _recording(_ORIG["ImageLoaderThread"]))
    monkeypatch.setattr(jw, "ThumbResolver", _recording(_ORIG["ThumbResolver"]))
    monkeypatch.setattr(jw, "_PageTextWorker", _recording(_ORIG["_PageTextWorker"]))
    monkeypatch.setattr(jw, "_AnchorLoadWorker", _recording(_ORIG["_AnchorLoadWorker"]))
    monkeypatch.setattr(jw, "_KnownJoinsLoadWorker",
                        _recording(_ORIG["_KnownJoinsLoadWorker"], quiet=True))
    monkeypatch.setattr(jw, "ThumbBatchWorker", _recording(_ORIG["ThumbBatchWorker"], quiet=True))
    GATE.clear()
    GATES[:] = [GATE]
    DOWNLOADS.clear()
    yield
    for wb in WINDOWS:            # the real closeEvent
        wb.close()
    for g in GATES:
        g.set()
    # Delivering queued results can start more loaders, so settle until a round of
    # event processing leaves nothing running.
    end = time.monotonic() + 10
    while time.monotonic() < end:
        if any(_alive(w) for w in list(KEEP) + list(_orphans())):
            QTest.qWait(10)
            continue
        for _ in range(5):
            QCoreApplication.processEvents()
        if not any(_alive(w) for w in list(KEEP) + list(_orphans())):
            break
    stuck = [w for w in list(KEEP) + list(_orphans()) if _alive(w)]
    _LEFTOVER.extend(stuck)       # never dropped while running, even on a failure
    KEEP.clear()
    WINDOWS.clear()
    assert not stuck, f"{len(stuck)} worker(s) still running after the teardown wait"
    assert not errors, errors
    assert DOWNLOADS == [], DOWNLOADS


# --- fakes -------------------------------------------------------------------------

class FakeMeta:
    def get_thumbnail(self, sid, size=None):
        return f"https://h.invalid/iiif/FL{sid[-6:]}/full/320,/0/default.jpg"

    def enrich_metadata(self, sid):
        return {"images": []}

    def get_meta_for_id(self, sid):
        return ("T-S 1", "")


class FakeSearcher:
    """get_browse_page sleeps delays[page] seconds, or waits for GATE when it is 'gate'."""

    def __init__(self, delays=None):
        self.delays = delays or {}

    def get_browse_page(self, sid, p=None, **kw):
        d = self.delays.get(p, 0.0)
        if d == "gate":
            GATE.wait(10)
        else:
            time.sleep(d)
        return {"text": f"TEXT OF FOLIO {p}", "total_pages": 3}


def _window(searcher=None, meta=None):
    app = MagicMock()
    app.meta_mgr = meta or FakeMeta()
    app.searcher = searcher or FakeSearcher()
    app.corrections_client = None
    wb = jw.JoinWorkbenchWindow(None, app)
    WINDOWS.append(wb)
    return wb


def _candidates(n, prefix="99"):
    from shared.joins_lab import normalize_candidate
    out = []
    for i in range(n):
        sid = f"{prefix}{i:016d}"
        out.append(normalize_candidate({
            "display": {"id": sid, "shelfmark": f"T-S {i}", "title": "",
                        "library_code": "CUL", "img": 1},
            "uid": f"{sid}_P0001", "full_text": "x",
        }))
    return out


class GatedLoader(_ORIG["ImageLoaderThread"]):
    """A download: waits for GATE, or sleeps delays[url-part] seconds. Like the real
    loader, a cancel() that arrives before the download returns yields load_failed.
    The image is 10 x its folio number wide (8 px when the URL names no folio)."""
    delays = {}

    def __init__(self, url):
        super().__init__(url)
        KEEP.append(self)

    def run(self):
        d = next((v for k, v in self.delays.items() if k in self.url), None)
        if d is None:
            GATE.wait(10)
        else:
            time.sleep(d)
        if self._cancelled:
            self.load_failed.emit()
            return
        m = re.search(r"/p(\d+)\.jpg", self.url)
        self.image_loaded.emit(QImage(10 * int(m.group(1)) if m else 8, 8,
                                      QImage.Format.Format_RGB32))


def _pump_until(pred, timeout=5.0, what="condition"):
    end = time.monotonic() + timeout
    while not pred():
        assert time.monotonic() < end, f"timed out waiting for {what}"
        QTest.qWait(10)


def _pump(ms=100):
    end = time.monotonic() + ms / 1000.0
    while time.monotonic() < end:
        QTest.qWait(10)


def _running(kind):
    return [w for w in KEEP if isinstance(w, kind) and _alive(w)]


def _has_pix(label):
    pm = label.pixmap()
    return pm is not None and not pm.isNull()


def _unreferenced(workers, wb):
    """Running workers held by neither the keeper nor one of the window's loader lists."""
    held = list(wb._img_threads) + list(getattr(wb, "_grid_img_threads", []))
    return [w for w in workers
            if _alive(w) and not _kept(w) and not any(h is w for h in held)]


def _anchor(wb, urls):
    wb._anchor_sid = SID
    wb._anchor_images = [{"url": u} if u else {} for u in urls]
    wb._anchor_idx = 0


# --- lifetime: a replaced running worker must stay referenced until it finishes -----

def test_page_turn_keeps_the_running_thumbnail_loaders(monkeypatch):
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    wb = _window()
    pane = wb._candidate_pane
    wb.filtered = _candidates(2 * jw._PER_PAGE)
    pane.render_results()
    _pump_until(lambda: len(_running(GatedLoader)) == jw._PER_PAGE, what="20 thumbnail loaders")
    first_page = _running(GatedLoader)
    pane._next_page()                                   # the real page turn
    assert _unreferenced(first_page, wb) == []


def test_page_turn_keeps_a_running_pool_loader(monkeypatch):
    """A card folio flip or Compare image loads through the 5-slot pool, not the grid
    list: a page turn while it loads must keep that loader too."""
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    monkeypatch.setattr(GatedLoader, "delays", {"/320,/": 0.0})   # thumbnails load at once
    wb = _window()
    pane = wb._candidate_pane
    wb.filtered = _candidates(2 * jw._PER_PAGE)
    pane.render_results()
    _pump_until(lambda: sum(_has_pix(c.img) for c in pane.cards.values()) == jw._PER_PAGE,
                what="the page's thumbnails")
    _pump_until(lambda: not _running(GatedLoader), what="the thumbnail loaders to finish")
    wb._enqueue_image(QLabel("loading…"), "https://h.invalid/POOLX/full/400,/0/default.jpg")
    pool = [w for w in _running(GatedLoader) if "/POOLX/" in w.url]
    assert len(pool) == 1, "the pool image did not start"
    pane._next_page()                                   # the real page turn
    assert _alive(pool[0]) and _kept(pool[0]), "the running pool loader was dropped"
    wb._cancel_images()
    assert wb._img_threads == [] and getattr(wb, "_grid_img_threads", []) == [], (
        "a retired loader is still listed")


def test_page_turn_keeps_the_running_vs_card_text_workers(monkeypatch):
    """A visually-similar card with no text fetches its first page on render; the page
    turn deletes the card, so the card must not be what holds that worker."""
    import dataclasses

    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    wb = _window(FakeSearcher({1: "gate"}))
    pane = wb._candidate_pane
    wb.filtered = [dataclasses.replace(c, via_vs=True, full_text="")
                   for c in _candidates(2 * jw._PER_PAGE)]
    pane.render_results()
    first = _running(_ORIG["_PageTextWorker"])
    assert len(first) == jw._PER_PAGE, f"{len(first)} card text workers started"
    cards = list(pane.cards.values())
    pane._next_page()
    for w in first:
        assert _alive(w) and _kept(w), "a running card text worker was dropped"
        assert not any(v is w for card in cards for v in vars(card).values()), (
            "a card still holds its text worker")


def test_page_turn_keeps_the_running_thumb_resolver():
    started = threading.Event()

    class SlowMeta(FakeMeta):
        def get_thumbnail(self, sid, size=None):
            started.set()
            GATE.wait(10)
            return ""

    wb = _window(meta=SlowMeta())
    pane = wb._candidate_pane
    wb.filtered = _candidates(2 * jw._PER_PAGE)
    pane.render_results()
    first = pane._resolver
    assert started.wait(3)
    pane._next_page()
    assert _alive(first) and _kept(first)


def test_a_retired_resolver_delivers_nothing_to_the_new_cards(monkeypatch):
    """The resolver slot carries no token: only the disconnect stops a result the old
    resolver already queued from starting an old thumbnail on a new card."""
    started_urls = []

    class UrlLoader(GatedLoader):
        def __init__(self, url):
            super().__init__(url)
            started_urls.append(url)

    old_gate = _gate()

    class UrlMeta(FakeMeta):
        def get_thumbnail(self, sid, size=None):
            if sid.startswith("99"):
                old_gate.wait(10)
                return f"https://h.invalid/iiif/FL{sid[-6:]}/full/320,/0/OLD.jpg"
            GATE.wait(10)                                  # the new resolver stays busy
            return f"https://h.invalid/iiif/FL{sid[-6:]}/full/320,/0/NEW.jpg"

    monkeypatch.setattr(jw, "ImageLoaderThread", UrlLoader)
    wb = _window(meta=UrlMeta())
    pane = wb._candidate_pane
    wb.filtered = _candidates(jw._PER_PAGE)               # sids 99...
    pane.render_results()
    r1 = pane._resolver
    old_gate.set()
    assert r1.wait(5000)                                  # its 20 results are QUEUED, not delivered
    wb.filtered = _candidates(jw._PER_PAGE, prefix="88")  # a filter change: a new page-0 list
    started_urls.clear()
    pane.render_results()                                 # re-render page 0 (same card indexes)
    _pump(300)
    assert [u for u in started_urls if u.endswith("OLD.jpg")] == []
    assert r1.receivers(r1.resolved) == 0


def test_a_replaced_known_joins_worker_delivers_nothing_within_one_anchor(monkeypatch):
    """_on_add_as_join reloads the known joins under the SAME generation, so the gen
    check cannot drop the old worker's rows; only the disconnect does."""
    rows1 = [{"other_sid": "990000000000000001", "other_shelf": "T-S OLD", "source": "user"}]
    rows2 = [{"other_sid": "990000000000000002", "other_shelf": "T-S NEW", "source": "user"}]
    k1_gate = _gate()
    plan = [(k1_gate, rows1), (None, rows2)]

    class Joins(_ORIG["_KnownJoinsLoadWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)
            self._gate_rows = plan.pop(0)

        def run(self):
            g, rows = self._gate_rows
            if g is not None:
                g.wait(10)
            self.done.emit(self._gen, rows)

    monkeypatch.setattr(jw, "_KnownJoinsLoadWorker", Joins)
    wb = _window()
    built = []
    orig_build = wb._build_join_row
    monkeypatch.setattr(wb, "_build_join_row",
                        lambda row: (built.append(row["other_shelf"]), orig_build(row))[1])
    wb._anchor_sid = SID
    wb._anchor_res = {"display": {"id": SID, "shelfmark": "T-S 1"}, "uid": SID}
    wb._reload_known_joins(wb._gen)
    k1 = wb._known_joins_worker
    k1_gate.set()
    assert k1.wait(5000)                                  # done(gen, rows1) is queued
    wb._reload_known_joins(wb._gen)                       # same generation
    _pump_until(lambda: built, what="the new rows")
    _pump(200)
    assert built == ["T-S NEW"]
    assert k1.receivers(k1.done) == 0


def test_a_replaced_thumb_batch_delivers_nothing_to_the_new_rows(monkeypatch):
    """Thumbnail results are matched to rows by index under one generation: only the
    disconnect keeps the previous batch's picture off the new row 0."""
    t1_gate = _gate()
    batches = []

    class Thumbs(_ORIG["ThumbBatchWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)
            batches.append(self)
            self._first = len(batches) == 1

        def run(self):
            if self._first:
                t1_gate.wait(10)
                img = QImage(77, 33, QImage.Format.Format_RGB32)
                img.fill(0xFFFF0000)
                self.resolved.emit(self._gen, 0, img)
            else:
                GATE.wait(10)                             # the new batch stays busy

    monkeypatch.setattr(jw, "ThumbBatchWorker", Thumbs)
    wb = _window()
    rows1 = [{"other_sid": "990000000000000001", "other_shelf": "T-S OLD", "source": "user"}]
    rows2 = [{"other_sid": "990000000000000002", "other_shelf": "T-S NEW", "source": "user"}]
    wb._on_known_joins_loaded(wb._gen, rows1)
    t1 = batches[0]
    t1_gate.set()
    assert t1.wait(5000)                                  # resolved(gen, 0, img_old) is queued
    wb._on_known_joins_loaded(wb._gen, rows2)             # same generation: batch t2 starts
    _pump(300)
    assert len(batches) == 2
    assert not _has_pix(wb._join_thumb_labels[0]), "the old batch's picture reached the new row"
    assert t1.receivers(t1.resolved) == 0


def test_an_empty_known_joins_reload_cuts_the_running_thumb_batch(monkeypatch):
    """A reload that leaves no rows starts no batch, but must still retire the old one."""
    class GatedThumbs(_ORIG["ThumbBatchWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)

        def run(self):
            GATE.wait(10)

    monkeypatch.setattr(jw, "ThumbBatchWorker", GatedThumbs)
    wb = _window()
    rows = [{"other_sid": SID, "other_shelf": "T-S 9", "source": "user"}]
    wb._on_known_joins_loaded(wb._gen, rows)
    first = wb._thumb_worker
    wb._on_known_joins_loaded(wb._gen, [])            # same generation, no rows left
    assert _alive(first) and _kept(first), "the running thumbnail batch was dropped"
    assert first.receivers(first.resolved) == 0, "the old batch is still connected"


def test_folio_change_keeps_the_running_text_worker_and_image_loader(monkeypatch):
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    wb = _window(FakeSearcher({2: "gate", 3: "gate"}))
    _anchor(wb, ["https://h/p1.jpg", "https://h/p2.jpg", "https://h/p3.jpg"])
    wb._folio_next()
    first_text, first_img = wb._page_text_worker, wb._img_loader
    wb._folio_next()
    assert _alive(first_text) and _kept(first_text)
    assert _alive(first_img) and _kept(first_img)


def test_reanchor_keeps_the_running_anchor_and_known_joins_workers(monkeypatch):
    class GatedAnchor(_ORIG["_AnchorLoadWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)

        def run(self):
            GATE.wait(10)
            self.done.emit(self._gen, {"images": [], "text": "", "meta": {}})

    class GatedJoins(_ORIG["_KnownJoinsLoadWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)

        def run(self):
            GATE.wait(10)
            self.done.emit(self._gen, [])

    monkeypatch.setattr(jw, "_AnchorLoadWorker", GatedAnchor)
    monkeypatch.setattr(jw, "_KnownJoinsLoadWorker", GatedJoins)
    wb = _window()
    res = {"display": {"id": SID, "shelfmark": "T-S 1", "img": 1}, "uid": SID}
    wb.set_anchor(res)
    first_anchor, first_joins = wb._anchor_worker, wb._known_joins_worker
    wb.set_anchor(dict(res, display={"id": SID[:-1] + "2", "shelfmark": "T-S 2", "img": 1}))
    for w in (first_anchor, first_joins):
        assert _alive(w) and _kept(w)


def test_a_same_anchor_reload_keeps_the_running_known_joins_worker(monkeypatch):
    """_on_add_as_join reloads the known joins without re-anchoring, possibly while the
    previous load still runs."""
    class GatedJoins(_ORIG["_KnownJoinsLoadWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)

        def run(self):
            GATE.wait(10)
            self.done.emit(self._gen, [])

    monkeypatch.setattr(jw, "_KnownJoinsLoadWorker", GatedJoins)
    wb = _window()
    wb._anchor_sid = SID
    wb._anchor_res = {"display": {"id": SID, "shelfmark": "T-S 1"}, "uid": SID}
    wb._reload_known_joins(wb._gen)
    first = wb._known_joins_worker
    wb._reload_known_joins(wb._gen)                       # same generation
    assert wb._known_joins_worker is not first
    assert _alive(first) and _kept(first), "the running known-joins worker was dropped"
    assert first.receivers(first.done) == 0


def test_new_known_joins_rows_keep_the_running_thumb_batch(monkeypatch):
    class GatedThumbs(_ORIG["ThumbBatchWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            KEEP.append(self)

        def run(self):
            GATE.wait(10)

    monkeypatch.setattr(jw, "ThumbBatchWorker", GatedThumbs)
    wb = _window()
    rows = [{"other_sid": SID, "other_shelf": "T-S 9", "source": "user"}]
    wb._on_known_joins_loaded(wb._gen, rows)
    first = wb._thumb_worker
    wb._on_known_joins_loaded(wb._gen, rows)          # _on_add_as_join's reload, same gen
    assert _alive(first) and _kept(first)


def test_card_folio_flip_keeps_the_running_card_text_worker(monkeypatch):
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    wb = _window(FakeSearcher({2: "gate", 3: "gate"}))
    pane = wb._candidate_pane
    wb.filtered = _candidates(3)
    pane.render_results()
    card = pane.cards[0]
    card._card_folio_next()
    card._card_folio_next()
    texts = [w for w in KEEP if isinstance(w, _ORIG["_PageTextWorker"]) and w.sid == card.sid]
    assert [w.p for w in texts] == [2, 3]
    for w in texts:
        assert _alive(w) and _kept(w), f"the page-{w.p} text worker was dropped"
        assert not any(v is w for v in vars(card).values()), "the card still holds its worker"


def test_a_closed_lab_shows_its_thumbnails_when_reopened(monkeypatch):
    """Closing the Lab only hides it (open_join_workbench shows the same window again),
    so the thumbnails still loading must not be cancelled by the close."""
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    wb = _window()
    wb.show()
    pane = wb._candidate_pane
    wb.filtered = _candidates(jw._PER_PAGE)
    pane.render_results()
    _pump_until(lambda: len(_running(GatedLoader)) == jw._PER_PAGE, what="20 thumbnail loaders")
    wb.close()
    GATE.set()
    _pump_until(lambda: not _running(GatedLoader), what="the loaders to finish")
    _pump(100)
    wb.show()
    filled = sum(_has_pix(c.img) for c in pane.cards.values())
    assert filled == jw._PER_PAGE, f"{filled} of {jw._PER_PAGE} cards have their thumbnail"


def test_a_compare_image_requested_while_thumbnails_load_starts_at_once(monkeypatch):
    """Grid thumbnails still all start at once (throughput unchanged), but they no longer
    fill the 5-slot pool: a card flip or Compare image does not wait behind them."""
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    monkeypatch.setattr(GatedLoader, "delays", {"/POOL/": 0.0})
    wb = _window()
    pane = wb._candidate_pane
    wb.filtered = _candidates(jw._PER_PAGE)
    pane.render_results()
    _pump_until(lambda: len(_running(GatedLoader)) == jw._PER_PAGE, what="20 thumbnail loaders")
    lbl = QLabel("loading…")
    wb._enqueue_image(lbl, "https://h.invalid/POOL/full/400,/0/default.jpg")
    assert wb._img_queue == [], "the pool image was queued behind the page's thumbnails"
    _pump_until(lambda: _has_pix(lbl), timeout=2.0, what="the pool image")
    assert len(_running(GatedLoader)) == jw._PER_PAGE, "the thumbnails finished early"
    GATE.set()
    _pump_until(lambda: not _running(GatedLoader), what="the thumbnails to finish")
    _pump(100)
    assert sum(_has_pix(c.img) for c in pane.cards.values()) == jw._PER_PAGE
    assert wb._img_queue == []


# --- late results: an earlier folio must never replace the current one ---------------

def test_late_text_of_an_earlier_folio_does_not_replace_the_current_one():
    wb = _window(FakeSearcher({2: 0.4, 3: 0.02}))
    _anchor(wb, [None, None, None])                   # no image threads in this test
    wb._folio_next()
    QTest.qWait(50)
    wb._folio_next()
    _pump_until(lambda: not _running(_ORIG["_PageTextWorker"]), what="the text workers")
    _pump(100)
    text = wb.anchor_text_browser.toPlainText()
    assert wb._anchor_idx == 2
    assert "TEXT OF FOLIO 3" in text and "TEXT OF FOLIO 2" not in text, text


@pytest.mark.parametrize("third", ["https://h/p3.jpg", None], ids=["p3", "no-image"])
def test_late_image_of_an_earlier_folio_does_not_replace_the_current_one(monkeypatch, third):
    monkeypatch.setattr(jw, "ImageLoaderThread", GatedLoader)
    monkeypatch.setattr(GatedLoader, "delays", {"/p2.jpg": 0.4, "/p3.jpg": 0.02})
    wb = _window()
    _anchor(wb, ["https://h/p1.jpg", "https://h/p2.jpg", third])
    wb._folio_next()
    QTest.qWait(50)
    wb._folio_next()
    _pump_until(lambda: not _running(GatedLoader), what="the image loaders")
    _pump(100)
    label = wb.anchor_img_label
    got = None if wb._anchor_full_pix is None else wb._anchor_full_pix.width()
    if third:
        assert (wb._anchor_idx, got) == (2, 30)
        assert _has_pix(label) and label.text() != jw.tr("No image"), (
            "an earlier folio's late result replaced folio 3's image")
    else:
        assert (wb._anchor_idx, got) == (2, None)
        assert label.text() == jw.tr("No image") and not _has_pix(label), (
            "an earlier folio's image landed under a folio that has none")


# --- no cycle: a released worker is freed without the cycle collector ----------------

class _GatedQThread(QThread):
    done = pyqtSignal(object, int)
    enriched = pyqtSignal(dict)

    def __init__(self, gate):
        super().__init__()
        self._gate = gate

    def cancel(self):
        pass

    def run(self):
        self._gate.wait(10)


def _start_kept(kind, gate, monkeypatch):
    """Start one gated worker and hand it to the retention path under test.
    Returns (worker, reaped) where reaped() says the path has released it."""
    if kind == "keeper":
        w = _GatedQThread(gate)
        w.start()
        jw._keep_until_finished(w)
        wref = weakref.ref(w)
        lists = (_orphans(), jw._ORPHANED_WORKERS)
        return w, lambda: not any(x is wref() for lst in lists for x in lst)
    if kind in ("cross", "enrich"):
        pane = jw.JoinCandidatePane

        class Stub:
            _reap_enrich_worker = pane._reap_enrich_worker
            _reap_retired = getattr(pane, "_reap_retired", None)

        s = Stub()
        s._retired_workers = []
        s._on_enriched = lambda d: None
        s._on_cross_done = lambda r: None
        s._cross_gen = 0
        w = _GatedQThread(gate)
        w.start()
        if kind == "cross":
            s._cross_worker = w
            pane._retire_cross_worker(s)
        else:
            s._enrich_worker = w
            pane._retire_enrich_worker(s)
        assert len(s._retired_workers) == 1
        return w, lambda: s._retired_workers == []
    # CompareDialog's page-text worker, through the real _load_pane_page_text
    created = []

    class GatedPageText(_ORIG["_PageTextWorker"]):
        def __init__(self, *a):
            super().__init__(*a)
            created.append(weakref.ref(self))

        def run(self):
            gate.wait(10)

    monkeypatch.setattr(jw, "_PageTextWorker", GatedPageText)

    class CompareStub:
        _reap_pane_text_worker = jw.CompareDialog._reap_pane_text_worker

    s = CompareStub()
    s.wb = types.SimpleNamespace(_gen=0, searcher=FakeSearcher())
    s._keep_widget = QTextBrowser()
    pane_dict = {"sys_id": SID, "txt": s._keep_widget, "page": 1}
    jw.CompareDialog._load_pane_page_text(s, pane_dict, None)
    assert len(created) == 1 and len(s._pane_text_workers) == 1
    w = created[0]()
    return w, lambda: s._pane_text_workers == []


@pytest.mark.parametrize("kind", ["keeper", "cross", "enrich", "compare"])
def test_a_kept_worker_is_freed_without_the_cycle_collector(kind, monkeypatch):
    """A slot that holds the worker (a default argument or closure) forms worker -> slot
    -> worker: the released worker then lives until the cyclic GC runs, on whatever
    thread triggers it. Released by a weak reference (the keeper) or by id (the Lab's
    reap slots), the last reference drops in the finished slot."""
    gate = _gate()
    gc.collect()
    gc.disable()
    try:
        w, reaped = _start_kept(kind, gate, monkeypatch)
        ref = weakref.ref(w)
        del w
        gate.set()
        _pump_until(lambda: ref() is None or ref().isFinished(), what="the worker to finish")
        _pump_until(reaped, what="the retention path to release the worker")
        _pump(50)
        assert ref() is None, f"{kind}: the released worker is alive until gc.collect()"
    finally:
        gc.enable()


def test_a_finished_worker_is_released_at_once():
    """Most retirements hand over workers that have already finished (every grid
    re-render retires a page of loaded thumbnails, every folio step the previous
    loader). Their finished() has fired, so holding them would grow the keeper for ever."""
    gate = _gate()
    gate.set()
    w = _GatedQThread(gate)
    w.start()
    assert w.wait(5000)
    jw._keep_until_finished(w)
    lists = (_orphans(), jw._ORPHANED_WORKERS)
    assert not any(x is w for lst in lists for x in lst), "a finished worker was kept"


class _FakeSignal:
    def __init__(self):
        self.slots = []

    def connect(self, slot):
        self.slots.append(slot)


class _FakeWorker:
    """All the keeper touches: finished.connect and isRunning."""

    def __init__(self, running):
        self.finished = _FakeSignal()
        self._running = running

    def isRunning(self):
        return self._running


def test_a_late_release_never_drops_a_worker_kept_since():
    """A worker handed over during its finish step is released at once, yet its queued
    finished() is still delivered after it has been freed; a worker kept in between
    usually gets the freed address. The late release must not match that worker."""
    newer = None
    try:
        for _ in range(50):
            early = _FakeWorker(running=False)      # in its finish step
            jw._keep_until_finished(early)
            late_release = early.finished.slots[0]  # its finished() is still queued
            early_id = id(early)
            del early
            newer = _FakeWorker(running=True)
            jw._keep_until_finished(newer)
            if id(newer) == early_id:
                break
            newer._running = False
            for slot in newer.finished.slots:
                slot()
        else:
            pytest.skip("no freed address was reused")
        late_release()                               # the freed worker's finished()
        assert _kept(newer), "a late release dropped a worker kept after it"
    finally:
        if newer is not None:
            newer._running = False
            for slot in newer.finished.slots:
                slot()

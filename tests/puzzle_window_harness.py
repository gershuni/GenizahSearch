# -*- coding: utf-8 -*-
"""Offscreen harness for the REAL desktop.puzzle.PuzzleCanvasWindow.

Not a test module (no test_ prefix): import it from a gui-marked test file,
`import puzzle_window_harness as pwh`, and build one PuzzleEnv per test.

What it replaces, and why nothing else is needed:
- joins.db: a PuzzleService on tmp_path, installed as the singleton. Every
  window method imports get_puzzle_service at CALL time, so patching the module
  attribute reaches them all. No test reaches the real joins_data/joins.db.
- thumbnails: generate_thumbnail is recorded (the fragments it was given) and
  returns env.thumb_result, or raises env.thumb_error; get_puzzle_image_service
  -> None. No save ever fetches an image.
- image/metadata loaders: PuzzleImageLoaderThread / PuzzleMetaLoaderThread are
  replaced IN desktop.puzzle's namespace by fakes that never start a thread and
  emit what the real threads emit (the image loader reports `fl_id or
  image_url`). Each records the slots it was connected with, so a test can
  deliver or fail ONE chosen request, late or out of order.
- prompts: desktop.puzzle._ask / _notify are the one seam for the prompts this
  window asks through; they are recorded and answered from env.answer. The
  static QMessageBox calls the window still makes (publish, export, "Delete
  join?"), QInputDialog and QFileDialog are a tripwire: each raises
  AssertionError("unexpected static dialog: <title>") unless the test put an
  answer for that title in env.static_answers -- an unanswered static modal
  would otherwise block the offscreen run for ever. QDialog.exec (the Save
  dialog) returns env.save_dialog_result.
- the host: a bare QWidget with the attributes the window reads.

Personal state: joins.db and the image cache are on tmp_path. Nothing here
reads or writes session.json, config.pkl, lang.pkl (tests/conftest.py isolates
those), lists.pkl or anything under Config.INDEX_DIR.

Lifetime: windows and hosts are kept in _KEEP for the life of the process
(gui files run one per process). Deleting them mid-run would let a queued
scene.changed / timer callback reach a dead wrapper; see tests/conftest.py on
why teardown must not drain the event queue either.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from PyQt6.QtCore import QBuffer, QByteArray, QEventLoop, QIODevice, Qt, QTimer  # noqa: E402
from PyQt6.QtGui import QPixmap  # noqa: E402
from PyQt6.QtWidgets import (  # noqa: E402
    QApplication, QDialog, QFileDialog, QInputDialog, QMessageBox, QWidget,
)

APP = QApplication.instance() or QApplication(["pytest"])

SB = QMessageBox.StandardButton

_KEEP: list = []

# Harness timer intervals (ms). Nothing depends on the production values
# (500 / 1500); short ones keep the file fast.
DEBOUNCE_MS = 50
AUTOSAVE_MS = 150


def call_catching(fn, *args, **kwargs):
    """Call fn; return the Exception it raised, or None. BaseException that is
    not an Exception (a test's sentinel) is not caught."""
    try:
        fn(*args, **kwargs)
    except Exception as exc:  # noqa: BLE001 -- the caller asserts on it
        return exc
    return None


class FakeSignal:
    def __init__(self):
        self._slots = []

    def connect(self, slot):
        self._slots.append(slot)

    def emit(self, *args):
        for slot in list(self._slots):
            slot(*args)


def _bound_args(signal):
    """The leading arguments bound into the first slot (a functools.partial)."""
    if not signal._slots:
        return ()
    return tuple(getattr(signal._slots[0], "args", ()))


class FakeImageLoader:
    """Stands in for PuzzleImageLoaderThread: records itself, starts nothing."""
    started: list = []

    def __init__(self, fl_id, **kwargs):
        self.fl_id = fl_id
        self.kwargs = dict(kwargs)
        self.image_url = kwargs.get("image_url", "")
        self.image_ready = FakeSignal()
        self.load_failed = FakeSignal()

    @property
    def emit_id(self):
        """What the real thread reports as its id (gui_threads.py)."""
        return self.fl_id or self.image_url

    @property
    def key(self):
        args = _bound_args(self.image_ready)
        return args[0] if args else None

    @property
    def req(self):
        args = _bound_args(self.image_ready)
        return args[1] if len(args) > 1 else None

    def deliver(self, image=None):
        self.image_ready.emit(self.emit_id, image if image is not None else png_bytes())

    def fail(self, error="offline"):
        self.load_failed.emit(self.emit_id, error)

    def start(self):
        FakeImageLoader.started.append(self)

    def isRunning(self):
        return False

    def wait(self, *_a):
        return True


class FakeMetaLoader:
    """Stands in for PuzzleMetaLoaderThread; every one made is in `made`."""
    made: list = []

    def __init__(self, meta_mgr, sys_id, shelfmark=''):
        self.meta_mgr = meta_mgr
        self.sys_id = sys_id
        self.shelfmark = shelfmark
        self.meta_ready = FakeSignal()
        self.meta_failed = FakeSignal()
        FakeMetaLoader.made.append(self)

    def start(self):
        pass

    def isRunning(self):
        return False

    def wait(self, *_a):
        return True


def png_bytes(w=60, h=80):
    pm = QPixmap(w, h)
    pm.fill(Qt.GlobalColor.gray)
    ba = QByteArray()
    buf = QBuffer(ba)
    buf.open(QIODevice.OpenModeFlag.WriteOnly)
    pm.save(buf, "PNG")
    return bytes(ba)


def take_started():
    """The image loaders started since the last call, in start order."""
    started = list(FakeImageLoader.started)
    FakeImageLoader.started.clear()
    return started


def finish_loads(fail_ids=()):
    """Deliver every image requested so far; fail those whose reported id
    (`fl_id or image_url`) is in fail_ids instead."""
    for t in take_started():
        if t.emit_id in fail_ids:
            t.fail()
        else:
            t.deliver()


def pump(ms=0):
    """Run the event loop: once, or for `ms` real milliseconds (timers fire)."""
    if ms <= 0:
        APP.processEvents()
        return
    loop = QEventLoop()
    QTimer.singleShot(ms, loop.quit)
    loop.exec()


class Host(QWidget):
    shelf_model = None
    meta_mgr = None
    corrections_client = None


def fragments(prefix="99000"):
    from shared.puzzle_model import PuzzleFragment
    return [PuzzleFragment(sys_id=prefix + "1", folio_label="1r", fl_id=prefix + "FL1",
                           shelfmark="T-S A 1", x=10.0, y=20.0, rotation=0.0),
            PuzzleFragment(sys_id=prefix + "2", folio_label="1r", fl_id=prefix + "FL2",
                           shelfmark="T-S B 2", x=300.0, y=40.0, rotation=0.0)]


class PuzzleEnv:
    """One real window over one temp joins.db. Build with PuzzleEnv.create()."""

    SB = SB

    @classmethod
    def create(cls, tmp_path, monkeypatch):
        import shared.puzzle_export as pe
        import shared.puzzle_image_service as pis
        import shared.puzzle_service as ps
        import desktop.puzzle as dp
        from shared.config import Config

        env = cls()
        env.dp = dp
        env.svc = ps.PuzzleService(db_path=str(tmp_path / "joins.db"))
        assert env.svc.is_available()
        monkeypatch.setattr(ps, "get_puzzle_service", lambda *a, **k: env.svc)
        monkeypatch.setattr(Config, "IMAGE_CACHE_DIR", str(tmp_path / "images_cache"))

        env.thumb_calls = []         # [(sys_id, folio_label), ...] per generate_thumbnail call
        env.thumb_result = "THUMB"
        env.thumb_error = None

        def _thumb(frags, *_a, **_k):
            env.thumb_calls.append([(f.sys_id, f.folio_label) for f in frags])
            if env.thumb_error is not None:
                raise env.thumb_error
            return env.thumb_result

        monkeypatch.setattr(pe, "generate_thumbnail", _thumb)
        monkeypatch.setattr(pis, "get_puzzle_image_service", lambda *a, **k: None)
        monkeypatch.setattr(dp, "PuzzleImageLoaderThread", FakeImageLoader)
        monkeypatch.setattr(dp, "PuzzleMetaLoaderThread", FakeMetaLoader)

        # The prompt seam. raising=False: on a tree without the seam the
        # window's own static dialogs hit the tripwire below instead.
        env.asks = []                # (title, text, buttons)
        env.answer = SB.Cancel       # a StandardButton, or fn(title, text, buttons)
        env.notices = []             # (kind, title, text)

        def _ask(parent, title, text, buttons):
            env.asks.append((title, text, buttons))
            answer = env.answer
            return answer(title, text, buttons) if callable(answer) else answer

        def _notify(parent, kind, title, text):
            env.notices.append((kind, title, text))

        monkeypatch.setattr(dp, "_ask", _ask, raising=False)
        monkeypatch.setattr(dp, "_notify", _notify, raising=False)

        env.static_calls = []        # (kind, title)
        env.static_answers = {}      # title -> answer, or fn(*args) -> answer

        def _tripwire(kind):
            def _static(parent, title, *args, **kwargs):
                env.static_calls.append((kind, title))
                if title not in env.static_answers:
                    raise AssertionError(f"unexpected static dialog: {title}")
                answer = env.static_answers[title]
                return answer(*args) if callable(answer) else answer
            return staticmethod(_static)

        for name in ("question", "warning", "information", "critical"):
            monkeypatch.setattr(QMessageBox, name, _tripwire(name))
        monkeypatch.setattr(QInputDialog, "getText", _tripwire("getText"))
        monkeypatch.setattr(QInputDialog, "getItem", _tripwire("getItem"))
        monkeypatch.setattr(QFileDialog, "getSaveFileName", _tripwire("getSaveFileName"))

        env.save_dialog_result = QDialog.DialogCode.Rejected

        def _dialog_exec(dlg):
            assert not isinstance(dlg, QMessageBox), "a QMessageBox reached QDialog.exec"
            return env.save_dialog_result

        monkeypatch.setattr(QDialog, "exec", _dialog_exec)

        FakeImageLoader.started.clear()
        FakeMetaLoader.made.clear()
        env.host = Host()
        env.win = dp.PuzzleCanvasWindow(env.host)
        env.win._scene_change_debounce.setInterval(DEBOUNCE_MS)
        env.win._auto_save_timer.setInterval(AUTOSAVE_MS)
        env.win.show()
        _KEEP.append((env.host, env.win))
        pump()
        return env

    def close(self):
        """Test teardown: no timer or queued scene change may reach the next
        test, and the event queue is not drained."""
        win = self.win
        win._auto_save_timer.stop()
        win._scene_change_debounce.stop()
        win._current_doc_id = None
        try:
            win.canvas_view.scene.changed.disconnect()
        except (TypeError, RuntimeError):
            pass
        win.hide()
        FakeImageLoader.started.clear()
        FakeMetaLoader.made.clear()

    # -- timing --

    def settle(self, cap_ms=3000):
        """Pump until neither autosave timer is running (a pending scene
        change is delivered first), with a cap."""
        pump()
        pump()
        waited = 0
        win = self.win
        while (win._scene_change_debounce.isActive() or win._auto_save_timer.isActive()) \
                and waited < cap_ms:
            pump(20)
            waited += 20
        pump()

    # -- building a canvas through the window's own pipeline --

    def add(self, fr):
        self.win.add_fragment(fr.sys_id, fr.shelfmark, fr.folio_label, fr.fl_id)
        finish_loads()
        pump()

    def scratch_pad(self, drag_px=77):
        """Two fragments added the ordinary way, one dragged: nothing else."""
        for fr in fragments():
            self.add(fr)
        item = self.item("990001")
        item.setPos(item.pos().x() + drag_px, item.pos().y())
        pump()

    def save(self, frags, title="saved join", notes="", thumbnail=""):
        from shared.puzzle_model import PuzzleDocument
        return self.svc.save_document(PuzzleDocument(title=title, notes=notes, fragments=frags),
                                      thumbnail_b64=thumbnail)

    def open(self, doc_id, fail_ids=()):
        """Open a saved join as a list click does, deliver its images, and let
        the load-time autosave settle."""
        self.win._load_document(doc_id)
        finish_loads(fail_ids)
        self.settle()

    def item(self, sys_id, label="1r"):
        return self.win._fragment_items[(sys_id, label)]

    def select_only(self, *items):
        self.win.canvas_view.scene.clearSelection()
        for it in items:
            it.setSelected(True)

    def list_item(self, doc_id):
        lst = self.win._docs_list
        for i in range(lst.count()):
            if lst.item(i).data(Qt.ItemDataRole.UserRole) == doc_id:
                return lst.item(i)
        raise AssertionError(f"{doc_id} is not in Saved Joins")

    def click_join(self, doc_id):
        """A real left click on a Saved Joins row (itemClicked -> the handler)."""
        from PyQt6.QtTest import QTest
        lst = self.win._docs_list
        rect = lst.visualItemRect(self.list_item(doc_id))
        QTest.mouseClick(lst.viewport(), Qt.MouseButton.LeftButton, pos=rect.center())
        pump()

    def stored(self, doc_id):
        """(sys_id, rotation, x) per stored fragment, or None if the join is gone."""
        d = self.svc.load_document(doc_id)
        return None if d is None else [(f.sys_id, f.rotation, f.x) for f in d.fragments]

    def stored_doc(self, doc_id):
        return self.svc.load_document(doc_id)

    def stored_thumbnail(self, doc_id):
        row = self.svc._conn.execute(
            "SELECT thumbnail_b64 FROM join_documents WHERE id = ?", (doc_id,)).fetchone()
        return None if row is None else row["thumbnail_b64"]

    def record_saves(self, monkeypatch):
        """Record every save_document call as (doc id, fragment keys, thumbnail)."""
        calls = []
        real = self.svc.save_document

        def _save(doc, thumbnail_b64=None):
            calls.append((doc.id, [(f.sys_id, f.folio_label) for f in doc.fragments],
                          thumbnail_b64))
            return real(doc, thumbnail_b64=thumbnail_b64)

        monkeypatch.setattr(self.svc, "save_document", _save)
        return calls

    def refuse_writes(self):
        """Storage that opens and reads but rejects every write (disk full, a
        lock held past busy_timeout, a read-only joins.db)."""
        self.svc._conn.execute("PRAGMA query_only=ON")

    def allow_writes(self):
        self.svc._conn.execute("PRAGMA query_only=OFF")

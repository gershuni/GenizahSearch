# -*- coding: utf-8 -*-
"""Reading Desk, puzzle images (desktop): the pictures of the saved puzzles of the desk's
fragments, several one under another.

The rules -- which puzzles there are, best first, and which are on show -- are
``shared/reading_desk_puzzles.py``, the website's. Here:

* the reader's own puzzles are read from ``joins.db`` on the UI thread (a small local read,
  through the connection that thread owns; ``PuzzleService`` connections are bound to the
  thread that opened them) and handed to ``PuzzleLookupThread`` as a snapshot;
* ``PuzzleLookupThread`` adds the published puzzles (a network call) through a Supabase client
  of its own, public reads only, never the signed-in client the rest of the app shares;
* ``PuzzlePictureThread`` composes the picture of one of the reader's own puzzles with the
  Puzzle's own export (``shared/puzzle_export.py``); a published puzzle's picture is
  downloaded from its address. One picture is prepared at a time, each kept no larger than
  ``PICTURE_MAX_SIDE``;
* ``PuzzleImagesView`` draws them in the pane's frame: "New Puzzle" above the image column, every
  puzzle found in the list column with a check box (the ones ticked are shown), and in the image
  column each puzzle shown with its name, image controls and "Open in Puzzle", and its picture.

The view never writes a puzzle: opening and arranging go to the Fragment Puzzle window
(``open_requested`` / ``arrange_requested``, connected by the host).
"""
from __future__ import annotations

import io
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional

from PyQt6.QtCore import QSize, QThread, Qt, pyqtSignal
from PyQt6.QtGui import QIcon, QImage, QPixmap
from PyQt6.QtWidgets import (
    QHBoxLayout, QLabel, QListView, QListWidget, QListWidgetItem, QPushButton, QScrollArea, QVBoxLayout,
    QWidget,
)
from PyQt6 import sip

from desktop.gui_threads import _retire_worker
from desktop.reading_desk_images import SIDE_WIDTH, _escape, bounded_image, image_controls
from desktop.viewers import ZoomableScrollArea
from genizah_core import get_logger, tr
from shared.reading_desk_puzzles import (
    choice_of, join_fragments, join_label, own_joins, published_joins, rank_joins, shown_joins,
)

logger = get_logger(__name__)

#: Width the reader's own puzzles are composed at (the Puzzle's PNG export uses 2000).
PICTURE_WIDTH = 2000
#: A puzzle's picture is kept no larger than this on its long side: a join of many fragments
#: composes to a canvas far wider than any one image.
PICTURE_MAX_SIDE = 3000
#: Pictures kept for the session, by puzzle id and save time: coming back is instant.
_PICTURES: "OrderedDict[str, tuple]" = OrderedDict()
_PICTURES_KEPT = 6


def _picture_key(join: Dict[str, Any]) -> str:
    """A puzzle's picture is kept by its id and its version: an own puzzle's save time, a
    published one's arrangement (``revision``; publishing again keeps id, date and address)."""
    return f"{join['id']}@{join.get('revision') or join.get('when') or ''}"


class PublishedPictureThread(QThread):
    """A published puzzle's picture, downloaded afresh: not through the image loader's disk
    cache, which keeps a file by its address for ever, and a puzzle published again keeps its
    address."""

    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal()

    def __init__(self, url: str):
        super().__init__()
        self.url = url
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            import requests
            from genizah_core import Config
            response = requests.get(self.url, headers=dict(Config.HTTP_HEADERS), timeout=(5, 30))
            image = QImage.fromData(response.content) if response.status_code == 200 else QImage()
        except Exception as e:
            logger.info('[ReadingDesk] published puzzle picture %s: %s', self.url, e)
            image = QImage()
        if self._cancelled:
            return
        if image.isNull():
            self.load_failed.emit()
        else:
            self.image_loaded.emit(bounded_image(image, PICTURE_MAX_SIDE))


def remember_picture(key: str, image: QImage, missing: int = 0) -> None:
    _PICTURES[key] = (image, missing)
    _PICTURES.move_to_end(key)
    while len(_PICTURES) > _PICTURES_KEPT:
        _PICTURES.popitem(last=False)


def public_client() -> Any:
    """A Supabase client of the caller's own, for public reads (the published puzzles);
    None when the library or the address is missing. Made on the thread that uses it."""
    try:
        from supabase import create_client
        from shared.supabase_provider import SUPABASE_ANON_KEY, SUPABASE_URL
    except Exception as e:
        logger.debug('[ReadingDesk] no Supabase for published puzzles: %s', e)
        return None
    if not SUPABASE_URL or not SUPABASE_ANON_KEY:
        return None
    return create_client(SUPABASE_URL, SUPABASE_ANON_KEY)


def own_snapshot(sys_ids: List[str], service: Any = None) -> List[Dict[str, Any]]:
    """The reader's own saved puzzles of these fragments, read now, on the calling (UI)
    thread. A joins.db that cannot be read gives none."""
    try:
        if service is None:
            from shared.puzzle_service import get_puzzle_service
            service = get_puzzle_service()
        return own_joins(service, [s for s in dict.fromkeys(sys_ids) if s])
    except Exception:
        logger.exception('[ReadingDesk] saved puzzles lookup')
        return []


class PuzzleLookupThread(QThread):
    """The published puzzles of the desk's fragments, ranked with the reader's own (a
    snapshot taken on the UI thread). ``found`` carries them, best first."""

    found = pyqtSignal(list)

    def __init__(self, sys_ids: List[str], own: List[Dict[str, Any]],
                 client_factory: Optional[Callable[[], Any]] = public_client):
        super().__init__()
        self.sys_ids = list(sys_ids)
        self.own = list(own)
        self.client_factory = client_factory
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        published: List[Dict[str, Any]] = []
        if self.client_factory is not None:
            try:
                client = self.client_factory()
                if client is not None:
                    published = published_joins(client, self.sys_ids)
            except Exception as e:
                logger.info('[ReadingDesk] published puzzles lookup failed: %s', e)
        if not self._cancelled:
            self.found.emit(rank_joins(self.sys_ids, self.own, published))


class _CountingImages:
    """The Puzzle's image service, counting the fragments it had no image for (the export
    leaves them out of the picture without a word)."""

    def __init__(self, service: Any):
        self._service = service
        self.missing = 0

    def resolve_fragment_image(self, *args, **kwargs):
        data = self._service.resolve_fragment_image(*args, **kwargs)
        if data is None:
            self.missing += 1
        return data


class PuzzlePictureThread(QThread):
    """The picture of one of the reader's own puzzles, composed as the Puzzle exports it.
    ``image_loaded`` carries it (no larger than PICTURE_MAX_SIDE) and the number of its
    fragments left out for want of an image."""

    image_loaded = pyqtSignal(QImage, int)
    load_failed = pyqtSignal()

    def __init__(self, join: Dict[str, Any], compose: Optional[Callable[..., Any]] = None):
        super().__init__()
        self.join = join
        self._compose = compose
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True
        self.requestInterruption()

    def run(self) -> None:
        try:
            fragments = join_fragments(self.join)
            if self._compose is not None:
                picture, missing = self._compose(fragments)
            else:
                from shared.puzzle_export import compose_puzzle_export
                from shared.puzzle_image_service import get_puzzle_image_service
                images = _CountingImages(get_puzzle_image_service())
                picture = compose_puzzle_export(fragments, images, export_size=PICTURE_WIDTH, margin=20,
                                                check_cancel=self.isInterruptionRequested)
                missing = images.missing
            if self._cancelled:
                return
            if picture is None:
                self.load_failed.emit()
                return
            buffer = io.BytesIO()
            picture.save(buffer, 'PNG')
            image = QImage.fromData(buffer.getvalue(), 'PNG')
            if image.isNull():
                self.load_failed.emit()
            else:
                self.image_loaded.emit(bounded_image(image, PICTURE_MAX_SIDE), int(missing))
        except InterruptedError:
            return
        except Exception:
            logger.exception('[ReadingDesk] puzzle picture %s', self.join.get('id'))
            if not self._cancelled:
                self.load_failed.emit()


class PublishedPuzzleThread(QThread):
    """A published puzzle's full record (``get_published_join_detail``), fetched off the UI
    thread before it is copied into the reader's own puzzles. ``fetched`` carries it, or
    None when it is gone."""

    fetched = pyqtSignal(object)

    def __init__(self, join_id: str, client_factory: Optional[Callable[[], Any]] = public_client):
        super().__init__()
        self.join_id = join_id
        self.client_factory = client_factory
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        detail = None
        try:
            client = self.client_factory() if self.client_factory else None
            if client is not None:
                from shared.puzzle_publish_service import get_published_join_detail
                detail = get_published_join_detail(client, self.join_id)
        except Exception as e:
            logger.info('[ReadingDesk] published puzzle %s could not be fetched: %s', self.join_id, e)
        if not self._cancelled:
            self.fetched.emit(detail)


class PuzzleImagesView(QWidget):
    """The puzzle images of a desk, in the pane's three cells: this widget is the image column
    (the puzzles on show, one under another); ``head`` goes above it ("New Puzzle") and
    ``side`` in the list column (every puzzle found, a check box each: the ones ticked are
    shown).

    ``state``: the desk (``entries``, ``puzzles_shown``). ``known``: puzzles already found for
    these fragments, drawn at once; else ``begin()`` looks for them -- called by the host once
    it has connected ``found``, which hands the lookup's result over to keep. ``own_lookup`` /
    ``client_factory`` / ``compose``: stand-ins for the saved puzzles, the published ones and
    the composer (tests)."""

    open_requested = pyqtSignal(dict)
    arrange_requested = pyqtSignal()
    found = pyqtSignal(list)

    def __init__(self, state: Any, *, known: Optional[List[Dict[str, Any]]] = None,
                 own_lookup: Callable[[List[str]], List[Dict[str, Any]]] = own_snapshot,
                 client_factory: Optional[Callable[[], Any]] = public_client,
                 compose: Optional[Callable[..., Any]] = None, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._state = state
        self._compose = compose
        self._workers: List[QThread] = []
        self._pictures: List[tuple] = []  # (join, viewer, note) waiting for their picture
        self._picture_worker: Optional[QThread] = None
        self.joins: Optional[List[Dict[str, Any]]] = None
        self.viewers: Dict[str, ZoomableScrollArea] = {}
        self.notes: Dict[str, QLabel] = {}
        self._layout = QVBoxLayout(self)
        self._layout.setContentsMargins(0, 0, 0, 0)
        self._layout.setSpacing(4)
        self._content: Optional[QWidget] = None
        self._own_lookup = own_lookup
        self._client_factory = client_factory

        self.head = QWidget(self)
        head_layout = QHBoxLayout(self.head)
        head_layout.setContentsMargins(4, 0, 4, 0)
        head_layout.addStretch()
        self.new_button = QPushButton(tr('New Puzzle'))
        self.new_button.setObjectName('rd-puzzle-new')
        self.new_button.setToolTip(tr('Arrange the fragments on the desk in another puzzle: another page, or the other side'))
        self.new_button.setStyleSheet("color: #a7d8c0; background: transparent; border: 1px solid #40916c; padding: 2px 8px;")
        self.new_button.clicked.connect(lambda _checked=False: self.arrange_requested.emit())
        self.new_button.setVisible(False)  # until there is a puzzle to add another to
        head_layout.addWidget(self.new_button)

        self.side = QListWidget(self)
        self.side.setObjectName('rd-puzzle-list')
        self.side.setAccessibleName(tr('Puzzles shown'))
        self.side.setToolTip(tr('Choose the puzzles to show.'))
        # As the thumbnails are listed: each puzzle's picture over its name, its box beside.
        self.side.setViewMode(QListView.ViewMode.IconMode)
        self.side.setFlow(QListView.Flow.TopToBottom)
        self.side.setWrapping(False)
        self.side.setMovement(QListView.Movement.Static)
        self.side.setWordWrap(True)
        self.side.setIconSize(QSize(96, 72))
        self.side.setSpacing(4)
        self.side.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.side.setStyleSheet("QListWidget { background: #111827; border: none; color: #e5e7eb; font-size: 10px; }")
        self.side.itemChanged.connect(self._choose)

        if known is not None:
            self._draw(known)
        else:
            self._set_content(self._message(tr('Looking for puzzles of these fragments…')))

    def begin(self) -> None:
        """Look for the puzzles (nothing to do when they were known)."""
        if self.joins is not None or self._workers:
            return
        sys_ids = [e.sys_id for e in self._state.entries]
        worker = PuzzleLookupThread(sys_ids, self._own_lookup(sys_ids), self._client_factory)
        worker.found.connect(self._on_found)
        self._workers.append(worker)
        worker.start()

    # -- lifetime ------------------------------------------------------------

    def shutdown(self) -> None:
        """Every worker let go: nothing it sends from now on reaches this view."""
        for worker in self._workers:
            _retire_worker(worker, *[n for n in ('found', 'image_loaded', 'load_failed') if hasattr(worker, n)])
        self._workers = []
        self._pictures = []
        self._picture_worker = None

    def _on_found(self, joins: list) -> None:
        if sip.isdeleted(self):
            return
        self.found.emit(list(joins))
        self._draw(list(joins))

    # -- drawing -------------------------------------------------------------

    def _message(self, text: str) -> QLabel:
        label = QLabel(text)
        label.setWordWrap(True)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        label.setStyleSheet("color: #cbd5e1; padding: 24px 8px;")
        return label

    def _set_content(self, widget: QWidget) -> None:
        if self._content is not None:
            self._layout.removeWidget(self._content)
            self._content.setParent(None)
            self._content.deleteLater()
        self._content = widget
        self._layout.addWidget(widget, 1)

    def _draw(self, joins: List[Dict[str, Any]]) -> None:
        """The list column and the puzzles on show."""
        self.joins = joins
        self.new_button.setVisible(bool(joins))
        shown_ids = {j['id'] for j in shown_joins(joins, self._state.puzzles_shown)}
        self.side.blockSignals(True)
        self.side.clear()
        for join in joins:
            item = QListWidgetItem(join_label(join, tr))
            item.setData(Qt.ItemDataRole.UserRole, join['id'])
            item.setToolTip(join_label(join, tr))
            item.setSizeHint(QSize(SIDE_WIDTH - 14, 72 + 46))
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignTop)
            # A lone puzzle is always shown: there is nothing to choose.
            flags = Qt.ItemFlag.ItemIsEnabled | (Qt.ItemFlag.ItemIsUserCheckable if len(joins) > 1 else Qt.ItemFlag.NoItemFlags)
            item.setFlags(flags)
            item.setCheckState(Qt.CheckState.Checked if join['id'] in shown_ids else Qt.CheckState.Unchecked)
            cached = _PICTURES.get(_picture_key(join))
            if cached is not None:
                item.setIcon(QIcon(QPixmap.fromImage(cached[0].scaled(
                    self.side.iconSize(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))))
            self.side.addItem(item)
        self.side.blockSignals(False)
        self._draw_shown()

    def _choose(self, _item: QListWidgetItem = None) -> None:
        """A box ticked or cleared: the choice kept with the desk, the puzzles redrawn."""
        if self.joins is None:
            return
        ticked = [self.side.item(i).data(Qt.ItemDataRole.UserRole) for i in range(self.side.count())
                  if self.side.item(i).checkState() == Qt.CheckState.Checked]
        self._state.puzzles_shown = choice_of(self.joins, ticked)
        self._draw_shown()

    def _draw_shown(self) -> None:
        # Pictures still on their way belong to the drawing being replaced.
        self.shutdown()
        self.viewers = {}
        self.notes = {}
        joins = self.joins or []
        if not joins:
            box = QWidget()
            layout = QVBoxLayout(box)
            layout.addWidget(self._message(tr('No puzzle of these fragments yet.')))
            arrange = QPushButton(tr('Arrange in Puzzle'))
            arrange.setObjectName('rd-puzzle-arrange')
            arrange.setStyleSheet("background-color: #40916c; color: white; padding: 3px 10px; border-radius: 3px;")
            arrange.clicked.connect(lambda _checked=False: self.arrange_requested.emit())
            layout.addWidget(arrange, 0, Qt.AlignmentFlag.AlignHCenter)
            layout.addStretch()
            self._set_content(box)
            return
        shown = shown_joins(joins, self._state.puzzles_shown)
        if not shown:
            self._set_content(self._message(tr('Choose the puzzles to show.')))
            return
        stack = QWidget()
        stack_layout = QVBoxLayout(stack)
        stack_layout.setContentsMargins(0, 0, 0, 0)
        stack_layout.setSpacing(6)
        for join in shown:
            self._draw_puzzle(stack_layout, join, alone=len(shown) == 1)
        if len(shown) == 1:
            self._set_content(stack)
        else:
            # One under another, as all the images are: each its own picture and controls.
            scroll = QScrollArea()
            scroll.setObjectName('rd-puzzle-stack')
            scroll.setWidgetResizable(True)
            scroll.setStyleSheet("background: #1a1a2e;")
            stack_layout.addStretch()
            scroll.setWidget(stack)
            self._set_content(scroll)
        self._next_picture()

    def _draw_puzzle(self, layout: QVBoxLayout, join: Dict[str, Any], alone: bool) -> None:
        """One puzzle: its name, the image controls and "Open in Puzzle", then its picture."""
        viewer = ZoomableScrollArea()
        viewer.setObjectName(f"rd-puzzle-{join['id']}")
        if alone:
            viewer.setMinimumHeight(300)
        else:
            viewer.setMinimumHeight(400)
            viewer.setMaximumHeight(600)
        name = QLabel(f"<b style='color: #e5e7eb;'>{_escape(join_label(join, tr))}</b>")
        name.setTextFormat(Qt.TextFormat.RichText)
        name.setToolTip(join_label(join, tr))
        open_button = QPushButton(tr('Open in Puzzle'))
        open_button.setObjectName('rd-puzzle-open')
        open_button.setStyleSheet("color: #a7d8c0; background: transparent; border: none; padding: 1px 6px;")
        open_button.clicked.connect(lambda _checked=False, j=join: self.open_requested.emit(dict(j)))
        layout.addWidget(image_controls(viewer, [name], [open_button]))
        note = QLabel()
        note.setStyleSheet("color: #fbbf24; font-size: 11px; padding: 0 6px;")
        note.setWordWrap(True)
        note.setVisible(False)
        layout.addWidget(note)
        layout.addWidget(viewer, 1 if alone else 0)
        self.viewers[join['id']] = viewer
        self.notes[join['id']] = note

        cached = _PICTURES.get(_picture_key(join))
        if cached is not None:
            self._show_picture(join, viewer, note, cached[0], cached[1])
            return
        viewer.set_status_message(tr('Preparing the puzzle image…'))
        self._pictures.append((join, viewer, note))

    def _show_picture(self, join: Dict[str, Any], viewer: ZoomableScrollArea, note: QLabel,
                      image: QImage, missing: int) -> None:
        if sip.isdeleted(viewer):
            return
        viewer.set_image(QPixmap.fromImage(image))
        if missing:
            total = len(join.get('fragments') or [])
            note.setText(tr("Some fragments' images could not be loaded: {missing} of {total}.").format(
                missing=missing, total=total))
            note.setVisible(True)
        # Its entry in the list column gets the picture too.
        for i in range(self.side.count()):
            item = self.side.item(i)
            if item.data(Qt.ItemDataRole.UserRole) == join['id'] and item.icon().isNull():
                self.side.blockSignals(True)
                item.setIcon(QIcon(QPixmap.fromImage(image.scaled(
                    self.side.iconSize(), Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))))
                self.side.blockSignals(False)

    def _next_picture(self) -> None:
        """Prepare the next picture waiting, one at a time."""
        if self._picture_worker is not None or not self._pictures:
            return
        join, viewer, note = self._pictures.pop(0)
        key = _picture_key(join)
        if join['kind'] == 'own':
            worker: QThread = PuzzlePictureThread(join, self._compose)
        else:
            worker = PublishedPictureThread(join.get('image_url') or '')

        def loaded(image: QImage, missing: int = 0, j=join, v=viewer, n=note, k=key) -> None:
            image = bounded_image(image, PICTURE_MAX_SIDE)
            remember_picture(k, image, missing)
            self._show_picture(j, v, n, image, missing)
            self._picture_done()

        def failed(v=viewer) -> None:
            if not sip.isdeleted(v):
                v.set_status_message(tr('The puzzle image could not be prepared.'))
            self._picture_done()

        worker.image_loaded.connect(loaded)
        worker.load_failed.connect(failed)
        self._workers.append(worker)
        self._picture_worker = worker
        worker.start()

    def _picture_done(self) -> None:
        self._picture_worker = None
        self._next_picture()

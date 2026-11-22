# -*- coding: utf-8 -*-
"""Reading Desk, image side (desktop): the view switcher and the image views.

One frame for three views (``ReadingDeskImagePane``): the image column beside the text, its
controls above it, and a narrow list column on the outer edge with the view switcher above it.
The views (rules in ``shared/reading_desk_views.py``):

* One at a time -- the list is every image's thumbnail; the image column one large image.
  Choosing a thumbnail tells the host (``image_chosen``), which shows that image's text alone.
* All images -- every image of every fragment one under another, a row of controls each; the
  thumbnails scroll to their image, and the text side scrolls in step (the host's scroll sync).
* Puzzle images -- the pictures of the saved puzzles of the desk's fragments
  (``desktop/reading_desk_puzzles.py``), on a desk of two or more fragments; the list is the
  puzzles found, ticked to be shown.

The pane keeps no desk of its own: the host hands it the desk's state (``ReadingDeskState``,
whose ``image_view``, ``image_shown`` and ``puzzles_shown`` hold the reader's choices) and the
image list of every fragment, and redraws it with ``set_desk``. Every image loader it starts is
retired (``desktop.gui_threads._retire_worker``) when it is redrawn or shut down, so nothing a
loader sends afterwards reaches a deleted widget.
"""
from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional

from PyQt6.QtCore import QSize, Qt, pyqtSignal
from PyQt6.QtGui import QIcon, QPixmap
from PyQt6.QtWidgets import (
    QButtonGroup, QGridLayout, QHBoxLayout, QLabel, QListView, QListWidget, QListWidgetItem, QMenu,
    QPushButton, QScrollArea, QSizePolicy, QSlider, QToolButton, QVBoxLayout, QWidget, QWidgetAction,
)
from PyQt6 import sip

from desktop.gui_threads import _retire_worker
from desktop.image_loader import ImageLoaderThread
from desktop.viewers import ZoomableScrollArea
from genizah_core import Config, get_logger, tr
from shared.reading_desk_views import (
    VIEW_ALL, VIEW_JOIN, VIEW_ONE, image_fl_id, image_key, image_view, join_is_offered, page_label, shown_image,
)

logger = get_logger(__name__)

THUMB_SIZE = QSize(96, 120)
#: The list column beside the image: the view switcher above it, the thumbnails or the puzzles in it.
SIDE_WIDTH = 150
#: An image is kept no larger than this on its long side. The loader asks for 2000 px wide, but
#: its fallbacks may answer with the full-resolution scan.
IMAGE_MAX_SIDE = 3000
#: Thumbnails loaded at once. Each is the same 2000 px image the large view shows (disk-cached
#: by ImageLoaderThread), scaled to an icon at once and the full image let go.
THUMB_LOADS_AT_ONCE = 3
_IIIF_FULL = '/full/2000,/0/default.jpg'

_BUTTON_STYLE = "color: white; background: #555; border-radius: 3px; padding: 1px 4px;"
_SEGMENT_STYLE = (
    "QPushButton { color: #d8f3dc; background: #1b4332; border: 1px solid #40916c; padding: 2px 4px;"
    " font-size: 11px; }"
    "QPushButton:checked { color: white; background: #2d6a4f; font-weight: bold; }"
    "QPushButton:hover:!checked { background: #24543f; }"
)


# ---------------------------------------------------------------------------
# The desk's images
# ---------------------------------------------------------------------------

def fragment_image_list(meta: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The images of one fragment, from its metadata cache entry (``meta_mgr.nli_cache``):
    the library's own images when they number as many as NLI's, else NLI's; with neither,
    the image of the fragment's FL id. Each a dict with ``label`` and ``url``."""
    meta = meta or {}
    images_nli = meta.get('images_nli') or []
    images_ext = meta.get('images_ext') or []
    if images_ext and images_nli and len(images_ext) != len(images_nli):
        image_list = images_nli
    else:
        image_list = images_ext or images_nli
    if image_list:
        return list(image_list)
    fl_ids = meta.get('fl_ids') or []
    if isinstance(fl_ids, str):
        fl_ids = [fl_ids]
    for fl in fl_ids:
        digits = re.sub(r"\D", "", str(fl or ""))
        if digits:
            return [{'label': f"FL{digits}", 'url': f"{Config.NLI_IIIF_BASE}/FL{digits}/full/2000,/0/default.jpg"}]
    return []


def full_image_url(url: str) -> str:
    """The address the desk loads an image from: a IIIF image service at 2000 px wide, any
    other address as it is."""
    if url and not url.endswith('.jpg'):
        return f"{url}{_IIIF_FULL}"
    return url or ''


def desk_images(entries: List[Any], image_lists: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Every image of every fragment on the desk, in desk order. ``entries``: the desk's
    ``ReadingDeskEntry``s; ``image_lists``: sys_id -> that fragment's images
    (``fragment_image_list``). A fragment with no images has none here."""
    images: List[Dict[str, Any]] = []
    for frag_idx, entry in enumerate(entries):
        listed = image_lists.get(entry.sys_id) or []
        for index, data in enumerate(listed):
            images.append({
                'key': image_key(entry.sys_id, index), 'sys_id': entry.sys_id, 'shelfmark': entry.shelfmark,
                'frag_idx': frag_idx, 'index': index, 'count': len(listed),
                'label': (data.get('label') or '').strip() or page_label(index, len(listed), tr),
                'url': full_image_url(data.get('url') or ''), 'fl_id': image_fl_id(data),
            })
    return images


def bounded_image(image: Any, max_side: int = IMAGE_MAX_SIDE) -> Any:
    """``image`` scaled down to ``max_side`` on its long side (as it is when smaller)."""
    if image is None or image.isNull() or max(image.width(), image.height()) <= max_side:
        return image
    return image.scaled(max_side, max_side, Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation)


def desk_view(state: Any, document_desk: bool) -> str:
    """The view the desk is in (the reader's, else the default)."""
    sys_ids = [e.sys_id for e in state.entries]
    return image_view(state.image_view or None, document_desk, join_is_offered(sys_ids))


def desk_shown_image(state: Any, image_lists: Dict[str, List[Dict[str, Any]]], document_desk: bool,
                     current_sys_id: str = '') -> Optional[Dict[str, Any]]:
    """The image on show in one at a time (None in the other views): one rule for the image
    side and the text side, which must agree on the image the reader is looking at."""
    if desk_view(state, document_desk) != VIEW_ONE:
        return None
    return shown_image(desk_images(state.entries, image_lists), state.image_shown or None, current_sys_id)


# ---------------------------------------------------------------------------
# One image's controls
# ---------------------------------------------------------------------------

def _small_button(text: str, tip: str, on_click: Callable[[], Any]) -> QPushButton:
    button = QPushButton(text)
    button.setFixedWidth(28)
    button.setStyleSheet(_BUTTON_STYLE)
    button.setToolTip(tip)
    button.setAccessibleName(tip)
    button.clicked.connect(lambda _checked=False: on_click())
    return button


class ImageAdjustButton(QToolButton):
    """Brightness, contrast, gamma and invert behind one button (the controls never take the
    image's room), for one viewer."""

    def __init__(self, viewer: ZoomableScrollArea, parent: Optional[QWidget] = None):
        super().__init__(parent)
        self._viewer = viewer
        self.setText("⚙")
        self.setToolTip(tr("Image Adjustments"))
        self.setAccessibleName(tr("Image Adjustments"))
        self.setStyleSheet(_BUTTON_STYLE)
        self.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        menu = QMenu(self)
        panel = QWidget(menu)
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(8, 6, 8, 6)
        self.sliders: Dict[str, QSlider] = {}
        for name, lo, hi, start in (('Brightness', -100, 100, 0), ('Contrast', -100, 100, 0), ('Gamma', 20, 300, 100)):
            row = QHBoxLayout()
            label = QLabel(tr(name))
            label.setMinimumWidth(70)
            row.addWidget(label)
            slider = QSlider(Qt.Orientation.Horizontal)
            slider.setRange(lo, hi)
            slider.setValue(start)
            slider.setMinimumWidth(160)
            slider.setAccessibleName(tr(name))
            row.addWidget(slider)
            layout.addLayout(row)
            self.sliders[name] = slider
        self.sliders['Brightness'].valueChanged.connect(lambda v: self._viewer.set_adjustments(brightness=v))
        self.sliders['Contrast'].valueChanged.connect(lambda v: self._viewer.set_adjustments(contrast=v))
        self.sliders['Gamma'].valueChanged.connect(lambda v: self._viewer.set_adjustments(gamma=v / 100.0))
        buttons = QHBoxLayout()
        self.invert = QPushButton(tr("Invert Colors"))
        self.invert.setCheckable(True)
        self.invert.toggled.connect(lambda on: self._viewer.set_adjustments(invert=on))
        buttons.addWidget(self.invert)
        buttons.addStretch()
        reset = QPushButton(tr("Reset Image"))
        reset.clicked.connect(self.reset)
        buttons.addWidget(reset)
        layout.addLayout(buttons)
        action = QWidgetAction(menu)
        action.setDefaultWidget(panel)
        menu.addAction(action)
        self.setMenu(menu)

    def reset(self) -> None:
        """Every adjustment back to none, the controls with them."""
        for name, start in (('Brightness', 0), ('Contrast', 0), ('Gamma', 100)):
            slider = self.sliders[name]
            slider.blockSignals(True)
            slider.setValue(start)
            slider.blockSignals(False)
        self.invert.blockSignals(True)
        self.invert.setChecked(False)
        self.invert.blockSignals(False)
        self._viewer.reset_adjustments()


def reset_view(viewer: ZoomableScrollArea) -> None:
    """The image as it was first shown: unturned and fitted to its box."""
    viewer.resetTransform()
    viewer.set_rotation(0)
    viewer._auto_fit_enabled = True
    if not viewer._apply_fit_to_viewport():
        viewer._update_text_pos()


def image_controls(viewer: ZoomableScrollArea, leading: List[QWidget], trailing: Optional[List[QWidget]] = None) -> QWidget:
    """One row: the given widgets, then zoom, rotate, the adjustments and reset view.

    The first leading label (the shelfmark, the puzzle's name) gives way when the pane is
    narrow (cut, its tooltip whole): a long name must not decide how wide the image side is.
    Any further label (the image's own, "1r") keeps its width."""
    row = QWidget()
    layout = QHBoxLayout(row)
    layout.setContentsMargins(4, 2, 4, 2)
    layout.setSpacing(4)
    first_label = True
    for widget in leading:
        if isinstance(widget, QLabel) and first_label:
            first_label = False
            widget.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
            widget.setMinimumWidth(24)
            if not widget.toolTip():
                widget.setToolTip(widget.text())
            layout.addWidget(widget, 1)
        else:
            layout.addWidget(widget)
    layout.addStretch()
    layout.addWidget(_small_button("-", tr("Zoom out"), viewer.zoom_out))
    layout.addWidget(_small_button("+", tr("Zoom in"), viewer.zoom_in))
    layout.addWidget(_small_button("↺", tr("Rotate Left 90"), lambda: viewer.rotate_view(-90)))
    layout.addWidget(_small_button("↻", tr("Rotate Right 90"), lambda: viewer.rotate_view(90)))
    layout.addWidget(ImageAdjustButton(viewer, row))
    layout.addWidget(_small_button("⟲", tr("Reset View"), lambda: reset_view(viewer)))
    for widget in trailing or []:
        layout.addWidget(widget)
    return row


def move_buttons(sys_id: str, index: int, count: int, on_move: Callable[[str, int], Any]) -> List[QPushButton]:
    """Up and down for the fragment at ``index`` of ``count`` on the desk (the order of its
    images, its text and what the Puzzle takes); none on a desk of one, and at either end the
    way out is disabled. Arrows, not words: up is up in either language."""
    if count < 2:
        return []
    up = _small_button("▲", tr("Move up"), lambda: on_move(sys_id, -1))
    down = _small_button("▼", tr("Move down"), lambda: on_move(sys_id, 1))
    up.setObjectName('rd-move-up')
    down.setObjectName('rd-move-down')
    up.setEnabled(index > 0)
    down.setEnabled(index < count - 1)
    return [up, down]


def _name_label(image: Dict[str, Any], current_sys_id: str, on_navigate: Callable[[str], Any]) -> QLabel:
    """The fragment's shelfmark, a link to open it; marked when it is the one browsed."""
    current = image['sys_id'] == current_sys_id
    badge = f" <span style='background: #27ae60; color: white; font-size: 10px;'>&nbsp;{tr('Current')}&nbsp;</span>" if current else ''
    label = QLabel(f"<a href='rd-navigate' style='color: #a7d8c0; font-weight: bold; text-decoration: none;'>"
                   f"{_escape(image['shelfmark'])}</a>{badge}")
    label.setTextFormat(Qt.TextFormat.RichText)
    label.setToolTip(image['shelfmark'])
    label.linkActivated.connect(lambda _link, sid=image['sys_id']: on_navigate(sid))
    return label


def _escape(text: str) -> str:
    return (text or '').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')


# ---------------------------------------------------------------------------
# The pane
# ---------------------------------------------------------------------------

class ReadingDeskImagePane(QWidget):
    """The desk's image side, in one frame for its three views (the owner's layout,
    2026-11-22):

        [image controls     ] [view switcher]
        [                   ] [list         ]
        [ the image(s)      ] [             ]

    The image column lies beside the text; the narrow list column is on the outer edge (in
    Hebrew on the left, mirrored in English). The list is the thumbnails of every image in one
    at a time (choosing one shows it) and in all images (choosing one scrolls to it), and the
    puzzles found, each with a check box, in puzzle images.

    ``image_chosen``: the reader chose another image in one at a time (``state.image_shown``
    is already set); the host shows its text. ``view_changed``: the reader chose another view
    (``state.image_view`` is set); the host redraws its text side. ``navigate_requested`` /
    ``remove_requested``: a fragment's sys_id. ``move_requested``: a fragment's sys_id and -1
    (up) or +1 (down). ``arrange_requested``: the desk's fragments to a new puzzle.
    ``open_puzzle_requested``: a puzzle (a ``find_joins`` dict) to open in the Puzzle.
    """

    image_chosen = pyqtSignal()
    view_changed = pyqtSignal()
    navigate_requested = pyqtSignal(str)
    remove_requested = pyqtSignal(str)
    move_requested = pyqtSignal(str, int)
    arrange_requested = pyqtSignal()
    open_puzzle_requested = pyqtSignal(dict)

    def __init__(self, parent: Optional[QWidget] = None, *, puzzle_options: Optional[Dict[str, Any]] = None):
        """``puzzle_options``: passed to ``PuzzleImagesView`` (stand-ins, in tests)."""
        super().__init__(parent)
        self.setObjectName('rd-image-pane')
        self.setStyleSheet("#rd-image-pane { background: #1a1a2e; }")
        self._puzzle_options = dict(puzzle_options or {})
        self._state: Any = None
        self._image_lists: Dict[str, List[Dict[str, Any]]] = {}
        self._current_sys_id = ''
        self._document_desk = False
        self._loaders: List[ImageLoaderThread] = []
        self._thumb_queue: List[tuple] = []
        self._thumb_running = 0
        self._thumb_items: Dict[str, QListWidgetItem] = {}
        self._main_loader: Optional[ImageLoaderThread] = None
        self._stack_scroll: Optional[QScrollArea] = None
        self._stack_viewers: Dict[str, ZoomableScrollArea] = {}
        self.viewers: List[ZoomableScrollArea] = []
        self.main_viewer: Optional[ZoomableScrollArea] = None
        self.strip: Optional[QListWidget] = None
        self.puzzle_view = None
        self.shown_key: Optional[str] = None
        # The puzzles found for the desk's fragments (sys_ids, puzzles): a redraw for another
        # reason (an image list arriving) reuses them; choosing the view looks afresh.
        self._puzzles_found: Optional[tuple] = None

        grid = QGridLayout(self)
        grid.setContentsMargins(4, 4, 4, 4)
        grid.setHorizontalSpacing(6)
        grid.setVerticalSpacing(4)
        self.head_cell = self._cell('rd-head', QHBoxLayout)
        self.main_cell = self._cell('rd-main', QVBoxLayout)
        self.side_cell = self._cell('rd-side', QVBoxLayout)
        self.side_cell.setFixedWidth(SIDE_WIDTH)

        switcher = QWidget()
        switcher.setObjectName('rd-view-switcher')
        switcher.setFixedWidth(SIDE_WIDTH)
        buttons = QGridLayout(switcher)
        buttons.setContentsMargins(0, 0, 0, 0)
        buttons.setSpacing(0)
        self._view_group = QButtonGroup(self)
        self._view_group.setExclusive(True)
        self.view_buttons: Dict[str, QPushButton] = {}
        for view, text, row, col, span in ((VIEW_ONE, "One at a time", 0, 0, 1), (VIEW_ALL, "All images", 0, 1, 1),
                                           (VIEW_JOIN, "Puzzle images", 1, 0, 2)):
            button = QPushButton(tr(text))
            button.setCheckable(True)
            button.setStyleSheet(_SEGMENT_STYLE)
            button.setObjectName(f'rd-view-{view}')
            button.setToolTip(tr(text))
            button.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)
            button.clicked.connect(lambda _checked=False, v=view: self._choose_view(v))
            self._view_group.addButton(button)
            self.view_buttons[view] = button
            buttons.addWidget(button, row, col, 1, span)

        grid.addWidget(self.head_cell, 0, 0)
        grid.addWidget(switcher, 0, 1)
        grid.addWidget(self.main_cell, 1, 0)
        grid.addWidget(self.side_cell, 1, 1)
        grid.setColumnStretch(0, 1)
        grid.setRowStretch(1, 1)

    @staticmethod
    def _cell(name: str, layout_class) -> QWidget:
        cell = QWidget()
        cell.setObjectName(name)
        layout = layout_class(cell)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        return cell

    @staticmethod
    def _fill(cell: QWidget, widget: Optional[QWidget]) -> None:
        """``widget`` as the cell's only content (what was there is deleted)."""
        layout = cell.layout()
        while layout.count():
            old = layout.takeAt(0).widget()
            if old is not None:
                old.setParent(None)
                old.deleteLater()
        if widget is not None:
            layout.addWidget(widget, 1)

    # -- the host's side ----------------------------------------------------

    def set_desk(self, state: Any, image_lists: Dict[str, List[Dict[str, Any]]], *, current_sys_id: str = '',
                 document_desk: bool = False) -> None:
        """Redraw for the desk ``state`` (its entries and the reader's choices)."""
        self._state = state
        self._image_lists = dict(image_lists)
        self._current_sys_id = current_sys_id or ''
        self._document_desk = bool(document_desk)
        self._rebuild()

    def current_view(self) -> str:
        if self._state is None:
            return VIEW_ALL
        return desk_view(self._state, self._document_desk)

    def stack_scroll_bar(self):
        """The all-images stack's scroll bar, for the text side to scroll in step with (None
        in the other views)."""
        if self._stack_scroll is None or sip.isdeleted(self._stack_scroll):
            return None
        return self._stack_scroll.verticalScrollBar()

    def shutdown(self) -> None:
        """Let every loader go (the desk is being left)."""
        self._retire_loaders()
        if self.puzzle_view is not None and not sip.isdeleted(self.puzzle_view):
            self.puzzle_view.shutdown()

    # -- drawing --------------------------------------------------------------

    def _choose_view(self, view: str) -> None:
        if self._state is None or view == self.current_view():
            self._sync_buttons()
            return
        self._state.image_view = view
        self._puzzles_found = None  # entering the puzzles looks for them afresh
        self._rebuild()
        self.view_changed.emit()

    def _sync_buttons(self) -> None:
        view = self.current_view()
        offered = self._state is not None and join_is_offered(e.sys_id for e in self._state.entries)
        self.view_buttons[VIEW_JOIN].setVisible(offered)
        for name, button in self.view_buttons.items():
            button.setChecked(name == view)

    def _retire_loaders(self) -> None:
        for loader in self._loaders:
            _retire_worker(loader, 'image_loaded', 'load_failed')
        self._loaders = []
        self._thumb_queue = []
        self._thumb_running = 0
        self._main_loader = None

    def _rebuild(self) -> None:
        self._retire_loaders()
        if self.puzzle_view is not None and not sip.isdeleted(self.puzzle_view):
            self.puzzle_view.shutdown()
        self.puzzle_view = None
        self._stack_scroll = None
        self._stack_viewers = {}
        self.viewers = []
        self.main_viewer = None
        self.strip = None
        self.shown_key = None
        self._thumb_items = {}
        for cell in (self.head_cell, self.main_cell, self.side_cell):
            self._fill(cell, None)
        self._sync_buttons()
        if self._state is None:
            return
        images = desk_images(self._state.entries, self._image_lists)
        view = self.current_view()
        if view == VIEW_JOIN:
            self._build_puzzles()
        elif view == VIEW_ONE:
            self._build_one(images)
        else:
            self._build_all(images)

    def _load(self, url: str, on_image: Callable[[Any], None], on_failed: Callable[[], None]) -> ImageLoaderThread:
        loader = ImageLoaderThread(url)
        loader.image_loaded.connect(lambda img: on_image(bounded_image(img)))
        loader.load_failed.connect(on_failed)
        self._loaders.append(loader)
        loader.start()
        return loader

    def _no_images(self) -> QLabel:
        none = QLabel(tr("No images available"))
        none.setStyleSheet("color: #95a5a6; padding: 20px; font-style: italic;")
        none.setAlignment(Qt.AlignmentFlag.AlignCenter)
        return none

    # The list column: every image, under its fragment's name --------------------

    def _build_strip(self, images: List[Dict[str, Any]], on_pick: Callable[[Dict[str, Any]], None]) -> QListWidget:
        strip = QListWidget()
        strip.setObjectName('rd-filmstrip')
        strip.setViewMode(QListView.ViewMode.IconMode)
        strip.setFlow(QListView.Flow.TopToBottom)
        strip.setWrapping(False)
        strip.setMovement(QListView.Movement.Static)
        strip.setIconSize(THUMB_SIZE)
        strip.setSpacing(2)
        strip.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        strip.setStyleSheet(
            "QListWidget { background: #111827; border: none; color: #e5e7eb; font-size: 10px; }"
            "QListWidget::item:selected { background: #2d6a4f; border: 2px solid #52b788; }")
        strip.setAccessibleName(tr("All images"))
        width = SIDE_WIDTH - 12
        last_fragment = None
        for image in images:
            if image['frag_idx'] != last_fragment:
                last_fragment = image['frag_idx']
                heading = QListWidgetItem(image['shelfmark'])
                heading.setFlags(Qt.ItemFlag.NoItemFlags)
                heading.setToolTip(image['shelfmark'])
                heading.setForeground(Qt.GlobalColor.lightGray)
                heading.setSizeHint(QSize(width, 18))
                strip.addItem(heading)
            item = QListWidgetItem(image['label'])
            item.setData(Qt.ItemDataRole.UserRole, image['key'])
            item.setToolTip(f"{image['shelfmark']} {image['label']}")
            item.setSizeHint(QSize(width, THUMB_SIZE.height() + 22))
            item.setTextAlignment(Qt.AlignmentFlag.AlignHCenter | Qt.AlignmentFlag.AlignBottom)
            strip.addItem(item)
            self._thumb_items[image['key']] = item
        by_key = {i['key']: i for i in images}

        def picked(item: QListWidgetItem) -> None:
            image = by_key.get(item.data(Qt.ItemDataRole.UserRole))
            if image is not None:
                on_pick(image)

        strip.itemClicked.connect(picked)
        strip.itemActivated.connect(picked)
        self.strip = strip
        return strip

    # All images ------------------------------------------------------------

    def _build_all(self, images: List[Dict[str, Any]]) -> None:
        count = QLabel(f"{len(images)} {tr('images')}")
        count.setStyleSheet("color: #a7d8c0; font-size: 11px; padding: 0 4px;")
        self._fill(self.head_cell, count)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("background: #1a1a2e;")
        container = QWidget()
        layout = QVBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)
        by_fragment: Dict[int, List[Dict[str, Any]]] = {}
        for image in images:
            by_fragment.setdefault(image['frag_idx'], []).append(image)
        for frag_idx, entry in enumerate(self._state.entries):
            header = QWidget()
            header.setObjectName(f"rd-img-frag-{frag_idx}")
            header.setStyleSheet("background: #34495e;")
            header_layout = QHBoxLayout(header)
            header_layout.setContentsMargins(8, 4, 4, 4)
            name = _name_label({'sys_id': entry.sys_id, 'shelfmark': entry.shelfmark}, self._current_sys_id,
                               self.navigate_requested.emit)
            name.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
            header_layout.addWidget(name, 1)
            for button in move_buttons(entry.sys_id, frag_idx, len(self._state.entries), self.move_requested.emit):
                header_layout.addWidget(button)
            header_layout.addWidget(_small_button("✕", tr("Remove"),
                                                  lambda sid=entry.sys_id: self.remove_requested.emit(sid)))
            layout.addWidget(header)
            fragment_images = by_fragment.get(frag_idx, [])
            if not fragment_images:
                layout.addWidget(self._no_images())
                continue
            for image in fragment_images:
                viewer = ZoomableScrollArea()
                viewer.setObjectName(f"rd-viewer-{frag_idx}-{image['index']}")
                viewer.setMinimumHeight(400)
                viewer.setMaximumHeight(600)
                label = QLabel(f"  {_escape(image['label'])}")
                label.setStyleSheet("color: #bdc3c7; font-size: 11px;")
                layout.addWidget(image_controls(viewer, [label]))
                layout.addWidget(viewer)
                self.viewers.append(viewer)
                self._stack_viewers[image['key']] = viewer
                viewer.set_status_message(tr("Loading..."))

                def loaded(img: Any, v=viewer, key=image['key']) -> None:
                    v.set_image(QPixmap.fromImage(img))
                    self._set_thumb(key, img)  # the list's thumbnail, from the same image

                self._load(image['url'], loaded, lambda v=viewer: v.set_status_message(tr("No Image")))
        layout.addStretch()
        scroll.setWidget(container)
        self._stack_scroll = scroll
        self._fill(self.main_cell, scroll)
        self._fill(self.side_cell, self._build_strip(images, self._scroll_to))

    def _scroll_to(self, image: Dict[str, Any]) -> None:
        """All images: a thumbnail brings its image into view."""
        viewer = self._stack_viewers.get(image['key'])
        if viewer is not None and self._stack_scroll is not None and not sip.isdeleted(viewer):
            self._stack_scroll.ensureWidgetVisible(viewer, 0, 0)

    # One at a time -----------------------------------------------------------

    def _build_one(self, images: List[Dict[str, Any]]) -> None:
        self._fill(self.side_cell, self._build_strip(images, self._choose_image))
        for image in images:
            self._thumb_queue.append((image['key'], image['url']))
        shown = shown_image(images, self._state.image_shown or None, self._current_sys_id)
        if shown is None:
            self._fill(self.main_cell, self._no_images())
        else:
            # The shown image's own load fills its thumbnail; no second download of it.
            self._thumb_queue = [q for q in self._thumb_queue if q[0] != shown['key']]
            self._show_main(shown)
        self._next_thumbs()

    def _show_main(self, image: Dict[str, Any]) -> None:
        """The large image, and its controls above it, for ``image``."""
        if self._main_loader is not None:
            _retire_worker(self._main_loader, 'image_loaded', 'load_failed')
            self._loaders = [w for w in self._loaders if w is not self._main_loader]
            self._main_loader = None
        viewer = ZoomableScrollArea()
        viewer.setObjectName('rd-viewer-shown')
        name = _name_label(image, self._current_sys_id, self.navigate_requested.emit)
        side = QLabel(_escape(image['label']))
        side.setToolTip(image['label'])
        side.setStyleSheet("color: #bdc3c7; font-size: 11px;")
        remove = _small_button("✕", tr("Remove"), lambda sid=image['sys_id']: self.remove_requested.emit(sid))
        moves = move_buttons(image['sys_id'], image['frag_idx'], len(self._state.entries), self.move_requested.emit)
        self._fill(self.head_cell, image_controls(viewer, [name, side], moves + [remove]))
        self._fill(self.main_cell, viewer)
        self.main_viewer = viewer
        self.shown_key = image['key']
        item = self._thumb_items.get(image['key'])
        if item is not None and self.strip is not None:
            self.strip.setCurrentItem(item)
        viewer.set_status_message(tr("Loading..."))

        def loaded(img: Any, v=viewer, key=image['key']) -> None:
            v.set_image(QPixmap.fromImage(img))
            self._set_thumb(key, img)  # its thumbnail too, if it has none yet

        self._main_loader = self._load(image['url'], loaded,
                                       lambda v=viewer: v.set_status_message(tr("No Image")))

    def _choose_image(self, image: Dict[str, Any]) -> None:
        if self._state is None or image['key'] == self.shown_key:
            return
        self._state.image_shown = image['key']
        self._show_main(image)
        self.image_chosen.emit()

    def _next_thumbs(self) -> None:
        """Start thumbnail loads up to THUMB_LOADS_AT_ONCE."""
        while self._thumb_queue and self._thumb_running < THUMB_LOADS_AT_ONCE:
            key, url = self._thumb_queue.pop(0)
            self._thumb_running += 1
            self._load(url, lambda img, k=key: self._thumb_done(k, img), lambda k=key: self._thumb_done(k, None))

    def _thumb_done(self, key: str, image: Any) -> None:
        self._thumb_running = max(0, self._thumb_running - 1)
        self._set_thumb(key, image)
        self._next_thumbs()

    def _set_thumb(self, key: str, image: Any) -> None:
        """The icon of thumbnail ``key``, scaled from ``image`` (the full image is not kept)."""
        item = self._thumb_items.get(key)
        if item is None or image is None or image.isNull() or not item.icon().isNull():
            return
        icon = QPixmap.fromImage(image.scaled(
            THUMB_SIZE, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
        item.setIcon(QIcon(icon))

    # Puzzle images -----------------------------------------------------------

    def _build_puzzles(self) -> None:
        from desktop.reading_desk_puzzles import PuzzleImagesView
        sys_ids = tuple(e.sys_id for e in self._state.entries)
        known = self._puzzles_found[1] if self._puzzles_found and self._puzzles_found[0] == sys_ids else None
        view = PuzzleImagesView(self._state, known=known, **self._puzzle_options)
        view.found.connect(lambda joins, ids=sys_ids: setattr(self, '_puzzles_found', (ids, joins)))
        view.open_requested.connect(self.open_puzzle_requested.emit)
        view.arrange_requested.connect(self.arrange_requested.emit)
        self.puzzle_view = view
        self._fill(self.head_cell, view.head)
        self._fill(self.main_cell, view)
        self._fill(self.side_cell, view.side)
        view.begin()  # after the connections: a quick answer must not miss them

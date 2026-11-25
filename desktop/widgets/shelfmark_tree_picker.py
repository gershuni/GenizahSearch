# -*- coding: utf-8 -*-
"""The desktop "Pick from the shelf" dialog: library, collection, volume, shelfmark, as FGP's
"find a fragment" screen has them (shelf tree picker plan §3.3, D1, D10; handoff DX6).

The columns are the web picker's (``web/components/shelfmark_tree_picker.py``), drawn from the same
pure view model, ``shared.shelfmark_tree_columns.load_view``: the last three levels of the path,
then the shelfmarks of the node chosen last, in 3, else 2, else 1 columns, as many as show the
longest whole. Every list has a filter box and counts; a level with a single child is passed
through; the breadcrumb goes back to any level. Library names are never cut. Names are in Hebrew
where the tree has one and the interface is Hebrew, each written in its own direction.

The contract (plan §10.4), the web picker's too: ``pick_from_shelf`` returns
``{sys_id, shelfmark, placement_id}``, or None when the dialog is closed without a pick; ``start``
is a ``sys_id``, a placement id, a node id or a typed prefix
(``shared.shelfmark_tree_view.start_at``). Single choice: a click, Enter or Space on a shelfmark
picks it and closes the dialog.

Loads run off the UI thread, one at a time; a request made while one runs replaces any request
still waiting, and a result is drawn only when no newer request has been made (a slower, earlier
click or filter never replaces a newer one). Choosing a level (``passed_through``, ``settle``: a
few primary-key reads) is done at once on the UI thread, so clicks apply in the order made.
A filter or "Show more" draws its own list again and nothing else: its box keeps the caret and
what is being typed.

``shelf_tree_service()`` is the desktop's switch: ``SHELFMARK_TREE_ENABLED`` (on unless set off)
AND the file opened and checked (``ShelfTreeOpener`` opens it once, off the UI thread, at start).
Until then, or when the file is missing or refused, hosts show no button and no place on the shelf.

Multiple choice (``mode='multi'``, picker PR 4, plan §10.13; the web dialog's, on the same
widget): a box on every shelfmark and on every node below the libraries, "Select all in <node>"
over the node whose shelfmarks are shown, a chips bar with "+N more" (D11), a running count,
Clear, Save as a list and one confirm. What is chosen is a
``shared.shelfmark_tree_selection.Selection`` (its logic is not repeated here), kept per purpose in
a ``SelectionStore`` the host owns (D12: the desktop keeps it in its session file) until it is
applied or cleared. ``pick_several_from_shelf`` opens it; the host's ``on_confirm(entries, label)``
receives ``[{sys_id, shelfmark, placement_id}]`` and answers None (it took them all) or the ids it
took; what it did not take stays chosen and the dialog stays open. A change is made at once, on
the UI thread, in the order made (two quick ticks both count); what the boxes, chips and count
show is read off the UI thread, the newest answer drawn.
"""
from __future__ import annotations

import copy
import html
from collections import OrderedDict
import logging
import time
import unicodedata
from typing import Callable, Optional

from PyQt6.QtCore import QEvent, QObject, QPoint, QRect, QSize, Qt, QThread, QTimer, pyqtSignal, pyqtSlot
from PyQt6.QtGui import (
    QColor, QFont, QFontMetrics, QImage, QKeySequence, QPalette, QPixmap, QShortcut, QTextBlockFormat,
    QTextCharFormat, QTextCursor, QTextLayout, QTransform,
)
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QButtonGroup, QCheckBox, QComboBox, QDialog, QDialogButtonBox, QFrame,
    QHBoxLayout, QHeaderView, QInputDialog, QLabel, QLineEdit, QListView, QListWidget, QListWidgetItem,
    QPushButton, QScrollArea, QSizePolicy, QStackedWidget, QStyle, QStyledItemDelegate, QStyleOption,
    QStyleOptionViewItem, QTableWidget, QTableWidgetItem, QTextEdit, QToolButton, QVBoxLayout, QWidget,
)

import genizah_core
from desktop.widgets.flow_layout import FlowLayout
from genizah_core import tr
from shared.shelfmark_tree_columns import (NODE_ROWS, WIDE_COLUMNS, leaf_in_view, leaf_tooltip, load_view,
                                           passed_through, settle)
from shared.shelfmark_tree_details import (
    LOOK_MIN, add_joins, field_lines, image_index, image_key, is_microfilm, item_details, look_sharp_width,
    look_width, opening_image, page_image_urls, sized_image_url, thumbnail_url,
)
from shared.shelfmark_tree_rows import (
    COLUMNS, DETAILS_KEY, HEADER_TIPS, SIZE_LABELS, SIZES, STORAGE_KEY, choice_value, details_shown, folds_columns, normal_size, normal_view,
    read_choice, row_details, shown_columns, wanted,
)
from shared.shelfmark_tree_selection import (
    ALL, CHOOSABLE_KINDS, DIRECT, NODE, Chip, Selection, members_function, scope_for,
)
from shared.shelfmark_tree_limits import SelectionLimit, chip_name, refusal_message
from shared.shelfmark_tree_signs import SIGNS, signs_text
from shared.shelfmark_tree_view import PAGE_SIZE, direct_index, display_name, start_at

logger = logging.getLogger(__name__)

#: How long a filter box waits for the typing to pause (ms), as the web's ``debounce=250``.
FILTER_DELAY_MS = 250
#: The narrowest shelfmark column, in pixels (the web's 140 px floor).
LEAF_MIN_WIDTH = 140
#: Shelfmark columns at most (the web's grid: three, else two, else one).
LEAF_COLUMNS = 3
#: How long closing the dialog waits for a load under way before leaving it to finish alone.
STOP_WAIT_MS = 5000
#: The chips the bar draws; the rest are only behind "+N more" (D11; the web's CHIPS_DRAWN).
CHIPS_DRAWN = 40
#: The widest a chip's name is drawn, in pixels (the web's 260 px chip).
CHIP_MAX_WIDTH = 260

_LRM, _RLM = "\u200e", "\u200f"
#: First strong isolate ... pop directional isolate: a name of either direction inside a sentence of
#: the other, kept whole and in its place (the web's <bdi>).
_FSI, _PDI = "\u2068", "\u2069"
_ROW = Qt.ItemDataRole.UserRole
#: A box's look for ``all`` / ``some`` / ``none`` of a node's manuscripts chosen.
_CHECK = {'all': Qt.CheckState.Checked, 'some': Qt.CheckState.PartiallyChecked, 'none': Qt.CheckState.Unchecked}
#: A box is drawn, never toggled by Qt itself: a click is the dialog's to handle (a box Qt toggled
#: and the dialog toggled again would undo itself).
_BOX_FLAGS = Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable
#: A shelfmark row's signs (``shared.shelfmark_tree_signs``): the tuple, in ``SIGNS`` order.
_SIGNS_ROLE = Qt.ItemDataRole.UserRole + 1
#: How each sign is drawn on a row: the search results' green "PGP", a document for a scholarly
#: transcription (the app's document icon, as in "View Document") and a link mark for joins (the two
#: icons in their own colours).
SIGN_MARKS = {'pgp': ('PGP', '#27ae60'), 'edition': ('\U0001F4C4', None), 'join': ('\U0001F517', None)}
#: Pixels around and between the signs.
SIGN_GAP = 4
#: How a join's sources are named in the pane.
_JOIN_SOURCE_NAMES = {'pgp': 'PGP', 'fjms': 'FJMS', 'community': 'Community'}
#: A thumbnail tile's picture (a QPixmap made on the UI thread), or False: the manuscript has none.
_THUMB_ROLE = Qt.ItemDataRole.UserRole + 2
#: A thumbnail tile's title (the catalogue's), whose start is drawn under the shelfmark.
_TITLE_ROLE = Qt.ItemDataRole.UserRole + 3
#: The lines of the title a tile shows.
TILE_TITLE_LINES = 2


def tile_title_lines(text: str, font: QFont, width: int, lines: int = TILE_TITLE_LINES) -> list:
    """The start of a title as a tile shows it: broken into at most ``lines`` lines of ``width`` pixels,
    the last cut with an ellipsis when more of the title is left."""
    text = ' '.join((text or '').split())
    if not text or width <= 0:
        return []
    layout = QTextLayout(text, font)
    layout.beginLayout()
    spans = []
    while len(spans) < lines:
        line = layout.createLine()
        if not line.isValid():
            break
        line.setLineWidth(width)
        spans.append((line.textStart(), line.textLength()))
    layout.endLayout()
    out = [text[start:start + length].strip() for start, length in spans]
    if spans and spans[-1][0] + spans[-1][1] < len(text):
        out[-1] = QFontMetrics(font).elidedText(text[spans[-1][0]:], Qt.TextElideMode.ElideRight, width)
    return [line for line in out if line]


def _title_font(font: QFont) -> QFont:
    small = QFont(font)
    if font.pointSizeF() > 0:
        small.setPointSizeF(font.pointSizeF() * 0.9)
    return small
#: The table's badges, as the search results table colours them (``genizah_app.py``'s results fill).
TABLE_MARKS = {'pgp': ('PGP', '#27ae60'), 'printed': (None, '#dc2626'), 'edition': ('✓', '#b45309'),
               'domain': (None, '#8e44ad')}
#: Thumbnails fetched at once (each may ask NLI for the manuscript's image ids, then its image).
THUMBS_AT_ONCE = 3
#: A picture that did not come (NLI down or busy for now: not "none") is asked for again after this long
#: (past NLI's circuit breaker's window), this many times at most in a picker.
THUMB_RETRY_S = 65
THUMB_TRIES = 3
#: How long the gallery waits for the scrolling to pause before it asks for the tiles in sight (ms).
THUMBS_DELAY_MS = 120
#: How the table's columns first take their widths: the title and the snippet share what is left, the
#: domain has a width of its own, the others are as wide as what they hold, so every column shows in the
#: pane without scrolling sideways. The reader may then resize any of them (owner, 2026-11-22): the widths
#: they set are kept for the rest of the run (``_table_widths``), across nodes, views and pickers.
TABLE_STRETCH = ('title', 'snippet')
TABLE_WIDTHS = {'domain': 120}
#: The share of the room left that the title takes first (the snippet the rest), and their least widths.
TABLE_TITLE_SHARE = 0.4
TABLE_MIN_WIDTH = 120
#: In a narrow pane: the least width of any column (the others shrink in proportion to fit).
TABLE_FLOOR = 40
_table_widths: dict = {}

#: The thumbnails fetched in this run of the app (QImage, by sys_id), shared by the details pane and the
#: gallery of every picker: a manuscript's first image is fetched once (owner, 2026-11-22: clicking the
#: same shelfmark again fetched it again). The oldest leave first. Read and written on the UI thread.
THUMB_CACHE_SIZE = 500
_thumb_cache: "OrderedDict[str, QImage]" = OrderedDict()
#: The width each kept picture was asked for at (by sys_id): a larger tile asks again, once, for a
#: sharper one; an image smaller than asked for (the original is) is never asked for again at that width.
_thumb_width: dict = {}

#: The width a tile's picture is asked for at, by thumbnail size: NLI's 400 (the thumbnail's own) for
#: the small and medium tiles, sharper ones for the large and extra large (the web's Large is 600 too:
#: ``web/components/shelfmark_tree_picker.py::_THUMB_WIDTH``).
THUMB_WIDTH = 400
FETCH_WIDTHS = {'s': THUMB_WIDTH, 'm': THUMB_WIDTH, 'l': 600, 'xl': 800}


def cached_thumbnail(sys_id: str) -> Optional[QImage]:
    image = _thumb_cache.get(sys_id) if sys_id else None
    if image is not None:
        _thumb_cache.move_to_end(sys_id)
    return image


def cached_width(sys_id: str) -> int:
    """The width the kept picture of a manuscript was asked for at (0: none kept)."""
    return _thumb_width.get(sys_id, 0) if sys_id in _thumb_cache else 0


def keep_thumbnail(sys_id: str, image, width: int = THUMB_WIDTH) -> None:
    """Keep a picture; a narrower one never replaces a sharper one kept already."""
    if not sys_id or not isinstance(image, QImage) or image.isNull():
        return
    if cached_width(sys_id) > width:
        _thumb_cache.move_to_end(sys_id)
        return
    _thumb_cache[sys_id] = image
    _thumb_width[sys_id] = width
    _thumb_cache.move_to_end(sys_id)
    while len(_thumb_cache) > THUMB_CACHE_SIZE:
        old, _image = _thumb_cache.popitem(last=False)
        _thumb_width.pop(old, None)


def card_sizes() -> dict:
    """Each thumbnail size's picture, ``(width, height)``: the Join Workbench's cards'
    (``desktop/join_workbench.py::CARD_SIZES``), read when first needed (a large module)."""
    try:
        from desktop.join_workbench import CARD_SIZES
        return {key: (CARD_SIZES[key][1], CARD_SIZES[key][2]) for key in SIZES}
    except Exception:  # noqa: BLE001 -- its numbers as they were written
        return {'s': (188, 100), 'm': (220, 130), 'l': (300, 240), 'xl': (460, 380)}


def load_view_choice() -> tuple:
    """The reader's view and thumbnail size (the app's settings), the list and medium when none is kept."""
    try:
        return read_choice((genizah_core.load_app_config() or {}).get(STORAGE_KEY))
    except Exception:  # noqa: BLE001 -- the default
        return read_choice(None)


def load_details_choice() -> bool:
    """Whether the reader keeps the details pane shown (the app's settings; shown when none is kept)."""
    try:
        return details_shown((genizah_core.load_app_config() or {}).get(DETAILS_KEY))
    except Exception:  # noqa: BLE001 -- the default
        return True


def save_details_choice(shown: bool) -> None:
    try:
        genizah_core.save_app_config({DETAILS_KEY: bool(shown)})
    except Exception:  # noqa: BLE001 -- kept for this session only
        logger.warning("Shelf picker: the details choice could not be saved", exc_info=True)


def save_view_choice(view: str, size: str) -> None:
    try:
        genizah_core.save_app_config({STORAGE_KEY: choice_value(view, size)})
    except Exception:  # noqa: BLE001 -- kept for this session only
        logger.warning("Shelf picker: the view could not be saved", exc_info=True)


class _Staged:
    def __repr__(self) -> str:
        return "STAGED"


#: What a host's ``on_confirm`` answers when it took everything into a step of its own that is not
#: applied yet (the Exclude dialog's own Apply): the picker closes and the choice stays kept until the
#: host consumes it (D12: kept until applied; a crash or a Cancel before the host's Apply loses nothing).
STAGED = _Staged()


def shelf_tree_service():
    """The opened, checked shelf tree when the desktop offers it, else None. Never opens the file:
    ``ShelfTreeOpener`` does, once, off the UI thread."""
    from shared.shelfmark_tree_service import peek_shelfmark_tree_service, shelfmark_tree_enabled
    if not shelfmark_tree_enabled():
        return None
    service = peek_shelfmark_tree_service()
    return service if service is not None and service.is_ready() else None


class ShelfTreeOpener(QThread):
    """Opens and checks the shelf tree file (a quarter of a second: the leaves are counted) and
    says whether it is served. Nothing here can stop the app."""

    opened = pyqtSignal(bool)

    def run(self):
        ok = False
        try:
            from shared.shelfmark_tree_service import get_shelfmark_tree_service, shelfmark_tree_enabled
            if shelfmark_tree_enabled():
                service = get_shelfmark_tree_service()
                ok = service.is_ready()
                if not ok:
                    logger.info("Shelf tree not offered: %s", service.unavailable_reason)
        except Exception as exc:  # noqa: BLE001 -- the shelf is an aid; the app starts without it
            logger.warning("Shelf tree check failed: %s", exc)
        self.opened.emit(ok)


class LiveToolTip(QObject):
    """A widget's tooltip written as it is about to show, by ``compute()``: Browse's previous / next
    name the manuscript they lead to, which depends on state that changes after they are drawn (an
    Oxford Part opened, the place on the shelf read)."""

    def __init__(self, widget: QWidget, compute: Callable[[], str]):
        super().__init__(widget)
        self._compute = compute
        widget.installEventFilter(self)

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.ToolTip:
            try:
                obj.setToolTip(self._compute())
            except Exception as exc:  # noqa: BLE001 -- the tooltip already set stays
                logger.warning("Tooltip not written: %s", exc)
        return False


def own_direction(text: str, rtl: bool) -> str:
    """A name written in its own direction, as the web's ``dir="auto"``: in a right-to-left list a
    name that starts in Latin letters keeps a closing dot or bracket at its end ("T-S Ar."), and a
    Hebrew name does the same in a left-to-right list."""
    if not text:
        return text
    first = next((d for d in map(unicodedata.bidirectional, text) if d in ("L", "R", "AL")), None)
    if rtl and first == "L":
        return f"{_LRM}{text}{_LRM}"
    if not rtl and first in ("R", "AL"):
        return f"{_RLM}{text}{_RLM}"
    return text


def leaf_columns(avail: int, need: int) -> int:
    """Shelfmark columns in ``avail`` pixels when the longest shelfmark shown needs ``need``: three,
    else two, else one (the web's grid)."""
    return max(1, min(LEAF_COLUMNS, max(1, avail) // max(1, need)))


def pick_from_shelf(parent, service, start: Optional[str] = None,
                    title: Optional[str] = None, act_label: Optional[str] = None) -> Optional[dict]:
    """Open the picker and wait: ``{sys_id, shelfmark, placement_id}``, or None (closed). A click
    chooses a shelfmark; ``act_label`` (default "Select") is the button that takes it (DX21)."""
    dialog = ShelfPickerDialog(service, start=start, title=title, parent=parent, act_label=act_label)
    try:
        dialog.exec()
        return dialog.selection()
    finally:
        dialog.deleteLater()


def pick_from_shelf_or_several(parent, service, *, switch_label: Optional[str] = None, purpose: str = '',
                               on_confirm: Optional[Callable[[list, str], object]] = None,
                               store: Optional["SelectionStore"] = None,
                               start: Optional[str] = None, title: Optional[str] = None,
                               confirm_label: Optional[str] = None, lists_mgr=None,
                               on_list_saved: Optional[Callable[[], None]] = None,
                               limit: Optional[SelectionLimit] = None, act_label: Optional[str] = None,
                               actions: Optional[list] = None):
    """The picker of the hosts that open manuscripts (Browse's button and its place on the shelf,
    Ctrl+W, Go to): single choice, a click choosing a shelfmark and ``act_label`` ("Open") taking it;
    with ``switch_label`` a switch in its header (DX16) turns the same open dialog into the
    multiple-choice picker where it is, and back. Off at every opening. ``actions`` (DX22): more
    buttons beside the act button, ``ShelfAction``s, acting on the shelfmark chosen or the ones
    ticked. Waits; answers the outcome: None (closed; what was confirmed in the multiple-choice mode
    went to ``on_confirm``), ``('open', selection)``, or ``(key, entries)`` for an action that closes
    the picker. See ``pick_from_shelf`` and ``pick_several_from_shelf``."""
    dialog = ShelfPickerDialog(service, start=start, title=title, parent=parent, purpose=purpose,
                               on_confirm=on_confirm, confirm_label=confirm_label, store=store,
                               lists_mgr=lists_mgr, on_list_saved=on_list_saved, switch_label=switch_label,
                               limit=limit, act_label=act_label, actions=actions)
    try:
        dialog.exec()
        return dialog.outcome()
    finally:
        dialog.deleteLater()


def pick_several_from_shelf(parent, service, *, purpose: str, on_confirm: Callable[[list, str], object],
                            store: Optional["SelectionStore"] = None, start: Optional[str] = None,
                            title: Optional[str] = None, confirm_label: Optional[str] = None,
                            lists_mgr=None, on_list_saved: Optional[Callable[[], None]] = None,
                            on_staged: Optional[Callable[[frozenset], None]] = None,
                            limit: Optional[SelectionLimit] = None, node_boxes: bool = False) -> bool:
    """Open the picker in its multiple-choice mode and wait. True when the host took everything
    chosen (the confirm closed it); False when it was closed otherwise, what is chosen staying kept
    in ``store`` (D12). See ``ShelfPickerDialog`` for ``on_confirm``."""
    dialog = ShelfPickerDialog(service, start=start, title=title, parent=parent, mode='multi',
                               purpose=purpose, on_confirm=on_confirm, confirm_label=confirm_label,
                               store=store, lists_mgr=lists_mgr, on_list_saved=on_list_saved,
                               on_staged=on_staged, limit=limit, node_boxes=node_boxes)
    try:
        return dialog.exec() == QDialog.DialogCode.Accepted
    finally:
        dialog.deleteLater()


class ShelfAction:
    """A button of the picker beside its act button (DX22: "Add to List", "Add to Puzzle"), acting on
    the shelfmark chosen (single choice) or the ones ticked (multiple choice), which stay ticked.
    ``keep_open``: ``handler(entries, dialog)`` runs with the picker open (it may answer a note to
    show, the limit that kept some out); else the picker closes and its outcome is
    ``(key, entries)``, for the host to act on."""

    def __init__(self, key: str, label: str, keep_open: bool = False,
                 handler: Optional[Callable[[list, "ShelfPickerDialog"], Optional[str]]] = None):
        self.key, self.label, self.keep_open, self.handler = key, label, keep_open, handler


# -- what is chosen, kept (D12) ---------------------------------------------------------------

class SelectionStore:
    """The multiple-choice picker's pending choices, one per purpose (``search_exclude``,
    ``composition_exclude``, ...), kept until applied or cleared (D12). The main window owns one and
    writes it into its session file (``to_state`` / ``load_state``), so a choice outlives closing the
    picker and the app; ``on_change`` asks for that save. Per desktop profile, as the exclusions it
    feeds are. A stored value is only ever read through ``Selection.from_dict``: a stale or broken one
    is an empty choice, never an error."""

    def __init__(self, on_change: Optional[Callable[[], None]] = None):
        self._data: dict = {}
        self._on_change = on_change

    def load(self, purpose: str) -> Selection:
        try:
            return Selection.from_dict(self._data.get(purpose))
        except Exception as exc:  # noqa: BLE001 -- a broken value is an empty choice (Codex: "chips": 1)
            logger.warning("Shelf choice %r not read: %s", purpose, exc)
            return Selection()

    def consume(self, purpose: str, applied_ids, service) -> bool:
        """What a host applied leaves the kept choice (a host that answered ``STAGED`` calls this from
        its own Apply). False, and nothing changed, when the tree cannot be read now."""
        if service is None:
            return False
        selection = self.load(purpose)
        try:
            selection.consume(applied_ids, members_function(service))
        except Exception as exc:  # noqa: BLE001 -- kept as it was; the reader may clear it
            logger.warning("Shelf choice %r not updated: %s", purpose, exc)
            return False
        self.save(purpose, selection)
        return True

    def save(self, purpose: str, selection: Selection) -> bool:
        if selection.chips:
            self._data[purpose] = selection.to_dict()
        else:
            self._data.pop(purpose, None)
        if self._on_change is not None:
            try:
                self._on_change()
            except Exception as exc:  # noqa: BLE001 -- the choice stays in memory; the save comes later
                logger.warning("Shelf choice not saved: %s", exc)
        return True

    def to_state(self) -> dict:
        return {purpose: copy.deepcopy(value) for purpose, value in self._data.items()}

    def load_state(self, raw) -> None:
        self._data = ({p: copy.deepcopy(v) for p, v in raw.items() if isinstance(p, str) and isinstance(v, dict)}
                      if isinstance(raw, dict) else {})


def merge_selections(first: Selection, then: Selection) -> Selection:
    """``first``'s chips, then those of ``then`` not already there (by key): what a host restores when
    a choice it took is given back (D12: a choice not applied stays chosen)."""
    keys = {chip.key for chip in first.chips}
    return Selection(copy.deepcopy(first.chips) + [copy.deepcopy(c) for c in then.chips if c.key not in keys])


#: A manuscript's own library code (``libraries.csv``'s ``library_code``: "CUL", "Katz"), for the
#: chips: set once by the main window (``MetadataManager.get_library_for_id``); None: the library
#: node's code. Read off the UI thread (the chips are named there): it must only read.
library_code_of: Optional[Callable[[str], str]] = None
#: A manuscript's title, for a shelfmark's tooltip (``load_view(title_of=)``, the catalogue's row),
#: the app's catalogue (``MetadataManager``) and its text index's page count
#: (``SearchEngine.page_count``) for the details pane: set once by the main window, read off the UI
#: thread. None: the tooltips have no title and the pane shows what the tree alone knows.
title_of: Optional[Callable[[str], str]] = None
catalogue = None
page_count_of: Optional[Callable[[str], int]] = None
#: The text index's first pages of a manuscript (``SearchEngine.page_texts``), for the pane's preview,
#: and the community client (the window's corrections client: its joins and comments counts, read
#: signed out too), both set by the main window; None: the pane goes without them.
page_texts_of: Optional[Callable[[str], object]] = None
community = None
#: The signs on the shelfmarks' rows (``shared.shelfmark_tree_signs.shelf_signs()``), set by the main
#: window. Loaded off the UI thread when a picker opens (once a process), the rows marked once they
#: are ready; None: no signs.
signs = None


def _signs_of(sys_id: str) -> tuple:
    """A manuscript's signs, once they are loaded (none before)."""
    source = signs
    return tuple(source.of(sys_id)) if source is not None and source.ready and sys_id else ()


def _with_signs(tip: str, marks: tuple) -> str:
    """A shelfmark's tooltip with a line naming its signs ("PGP · Scholarly transcription")."""
    line = signs_text(marks, tr) if marks else ''
    return '\n'.join(x for x in (tip, line) if x)


def _sign_font(font: QFont) -> QFont:
    small = QFont(font)
    if font.pointSizeF() > 0:
        small.setPointSizeF(font.pointSizeF() * 0.85)
    elif font.pixelSize() > 0:
        small.setPixelSize(max(1, int(font.pixelSize() * 0.85)))
    small.setBold(True)
    return small


def signs_room(font: QFont, keys=SIGNS) -> int:
    """The pixels the signs ``keys`` take at the end of a row (all of them by default: a grid keeps
    that room whether a row has signs or not, so the signs coming change no width)."""
    keys = [k for k in keys if k in SIGN_MARKS]     # a sign this app does not draw takes no room
    metrics = QFontMetrics(_sign_font(font))
    return sum(metrics.horizontalAdvance(SIGN_MARKS[k][0]) for k in keys) + SIGN_GAP * (len(keys) + 1)


def _draw_signs(painter, option, marks: tuple, selected: bool = False) -> None:
    """The signs at the row's end side, in reading order after the shelfmark; a shade lighter on a
    chosen row (the results' green on the highlight did not read)."""
    rtl = option.direction == Qt.LayoutDirection.RightToLeft
    marks = tuple(k for k in marks if k in SIGN_MARKS)
    font = _sign_font(option.font)
    metrics = QFontMetrics(font)
    order = list(reversed(marks)) if rtl else list(marks)
    widths = [metrics.horizontalAdvance(SIGN_MARKS[k][0]) for k in order]
    total = sum(widths) + SIGN_GAP * (len(order) - 1)
    rect = option.rect
    x = rect.left() + SIGN_GAP if rtl else rect.right() + 1 - SIGN_GAP - total
    painter.save()
    painter.setFont(font)
    for key, width in zip(order, widths):
        text, colour = SIGN_MARKS[key]
        pen = QColor(colour) if colour else option.palette.color(QPalette.ColorRole.Text)
        painter.setPen(pen.lighter(170) if selected and colour else pen)
        painter.drawText(QRect(x, rect.top(), width, rect.height()),
                         int(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft), text)
        x += width + SIGN_GAP
    painter.restore()


def chip_namer(service, lang: str) -> Callable[[Chip], str]:
    """A chip's name as both apps show it (``shared.shelfmark_tree_limits.chip_name``): its library's
    code (the manuscript's own, ``library_code_of``), then a node's name in the reader's language as
    the tree names it now, or the shelfmark ("CUL T-S 32")."""
    code_of = library_code_of

    def name_of(chip: Chip) -> str:
        try:
            return chip_name(service, chip, lang, code_of=code_of)
        except Exception:  # noqa: BLE001 -- a chip is named as chosen rather than not at all
            return chip.label
    return name_of


def paint_state(service, selection: Selection, node_ids: tuple, list_node: Optional[str], lang: str) -> dict:
    """What the multiple-choice parts show, read from a copy of the selection (blocking: off the UI
    thread): the manuscripts chosen, the chips with their counts, each drawn node's box and the
    "Select all" over the shelfmarks shown. A box shows what its click acts on, the node's
    manuscripts wherever they were chosen; never a guess from shelf positions (Codex, PR 4)."""
    members_of = members_function(service)
    name_of = chip_namer(service, lang)
    chosen = selection.chosen_set(members_of)
    chips = [{'key': chip.key, 'label': name_of(chip), 'node': chip.kind == NODE,
              'count': selection.chip_count(chip, members_of)} for chip in selection.chips]

    def state_of(members: frozenset) -> str:
        hit = members & chosen
        return 'none' if not hit else ('all' if hit == members else 'some')

    # the drawn rows are read once and not kept: the service keeps the chosen nodes' sets
    nodes = {nid: (state_of(service.members_under(nid, keep=False)) if chosen else 'none') for nid in node_ids}
    whole = None
    node = service.node(list_node) if list_node else None
    if node is not None and node.kind in CHOOSABLE_KINDS:
        scope = scope_for(node)
        members = service.members_under(node.id, scope == DIRECT)
        whole = {'id': node.id, 'scope': scope, 'total': len(members), 'name': display_name(node, lang),
                 'state': state_of(members) if chosen else 'none'}
    return {'chosen': frozenset(chosen), 'count': len(chosen), 'chips': chips, 'nodes': nodes, 'whole': whole}


# -- loads off the UI thread ------------------------------------------------------------------

class _Job(QThread):
    """One load. Emits ``done(generation, result)``; an exception is the result when it fails."""

    done = pyqtSignal(int, object)

    def __init__(self, gen: int, fn: Callable[[], object], parent=None):
        super().__init__(parent)
        self.gen = gen
        self._fn = fn

    def run(self):
        try:
            out = self._fn()
        except Exception as exc:  # noqa: BLE001 -- drawn as "not loaded", never raised into Qt
            logger.warning("Shelf picker: a load failed: %s", exc)
            out = exc
        if not self.isInterruptionRequested():
            self.done.emit(self.gen, out)


class _Loads(QObject):
    """At most one job running. A request while one runs waits, replacing any request already
    waiting: only the newest is run next. Jobs are children of the application, not of the dialog,
    so a dialog closed during a load never destroys a running thread (desktop/qthread_exit.py)."""

    def __init__(self, on_done: Callable[[int, object], None], parent=None):
        super().__init__(parent)
        self._on_done = on_done
        self._running: Optional[_Job] = None
        self._waiting = None
        self._stopped = False

    def request(self, gen: int, fn: Callable[[], object]) -> None:
        if self._stopped:
            return
        if self._running is not None:
            self._waiting = (gen, fn)
            return
        self._start(gen, fn)

    def busy(self) -> bool:
        return self._running is not None or self._waiting is not None

    def _start(self, gen: int, fn) -> None:
        job = _Job(gen, fn, QApplication.instance())
        job.done.connect(self._done)
        job.finished.connect(self._finished)
        self._running = job
        job.start()

    @pyqtSlot(int, object)
    def _done(self, gen: int, out) -> None:
        if not self._stopped:
            self._on_done(gen, out)

    @pyqtSlot()
    def _finished(self) -> None:
        job, self._running = self._running, None
        if job is not None:
            job.deleteLater()
        waiting, self._waiting = self._waiting, None
        if waiting is not None and not self._stopped:
            self._start(*waiting)

    def stop(self, wait_ms: int = STOP_WAIT_MS) -> None:
        """No more results; the running job is asked to stop and waited for."""
        self._stopped = True
        self._waiting = None
        job = self._running
        if job is None:
            return
        job.requestInterruption()
        if job.wait(wait_ms):
            self._running = None
            job.deleteLater()
        else:
            # still running (a network call): its parent is the application, settle_running_threads
            # takes it at exit, and it deletes itself when it ends, whatever becomes of its dialog
            job.finished.connect(job.deleteLater)


class _ThumbPool(QObject):
    """Every thumbnail load of a picker, the gallery's and the details pane's: at most
    ``THUMBS_AT_ONCE`` at a time, each ``_thumbnail_image`` on a ``_Job`` (a child of the application),
    one image never fetched twice at once. ``want(sys_ids)`` replaces what the gallery waits for with the
    tiles in sight now (one scrolled away before its turn is not fetched); ``fetch(sys_id)`` is the
    pane's: first in line, kept by ``want`` (the pane's newest replaces an older one not started);
    ``on_image(sys_id, QImage or None, width)`` gets each answer on the UI thread; ``width()`` is the
    width to ask at now (the gallery's size), read as each load starts. ``stop()`` (the dialog
    closing): nothing more is started or delivered, the running loads are asked to stop and none is
    waited for (a network call)."""

    def __init__(self, on_image: Callable[[str, object, int], None], parent=None,
                 skip: Optional[Callable[[str], bool]] = None, width: Optional[Callable[[], int]] = None):
        super().__init__(parent)
        self._on_image = on_image
        self._width = width or (lambda: THUMB_WIDTH)
        self._skip = skip                          # an image known already: not fetched
        self._queue: list = []
        self._first: Optional[str] = None          # the pane's request, ahead of the gallery's
        self._running: dict = {}                   # job -> sys_id
        self._gen = 0
        self._stopped = False

    def want(self, sys_ids) -> None:
        if self._stopped:
            return
        running = set(self._running.values())
        self._queue = [s for s in dict.fromkeys(sys_ids) if s not in running]
        self._pump()

    def fetch(self, sys_id: str) -> None:
        """The details pane's picture: next to start (unless it is under way already)."""
        if self._stopped or not sys_id or sys_id in self.running():
            return
        self._first = sys_id
        self._pump()

    def running(self) -> set:
        return set(self._running.values())

    def _pump(self) -> None:
        while not self._stopped and len(self._running) < THUMBS_AT_ONCE and (self._first or self._queue):
            if self._first:
                sid, self._first = self._first, None
                if sid in self._queue:
                    self._queue.remove(sid)
            else:
                sid = self._queue.pop(0)
            if sid in self.running() or (self._skip is not None and self._skip(sid)):
                continue
            meta, width = catalogue, self._width()
            job = _Job(self._gen, lambda s=sid, m=meta, w=width: (s, _thumbnail_image(s, m, w), w),
                       QApplication.instance())
            job.done.connect(self._done)
            job.finished.connect(self._finished)
            job.finished.connect(job.deleteLater)
            self._running[job] = sid
            job.start()

    @pyqtSlot(int, object)
    def _done(self, gen: int, out) -> None:
        if self._stopped or gen != self._gen or not isinstance(out, tuple):
            return
        self._on_image(*out)

    @pyqtSlot()
    def _finished(self) -> None:
        job = self.sender()
        self._running.pop(job, None)
        self._pump()

    def stop(self) -> None:
        self._stopped = True
        self._gen += 1
        self._queue, self._first = [], None
        for job in list(self._running):
            job.requestInterruption()


# -- the lists --------------------------------------------------------------------------------

def key_tip(text: str, key: str) -> str:
    """A button's tooltip with its key (owner, 2026-11-25): "Zoom in (+)"."""
    return f"{tr(text)} ({key})"


def arrow_keys(rtl: bool) -> tuple:
    """The arrows' names (back, forward) as the keys go: mirrored in Hebrew."""
    return ('\u2192', '\u2190') if rtl else ('\u2190', '\u2192')


class _ZoomImage(QScrollArea):
    """The pane's picture, zoomable: fitted at first; + / - (``zoom_by``), the mouse wheel over it (at
    the pointer) and a drag to move it once zoomed; a double click fits it again. ``zoomed(factor)``
    after each change (the pane asks for a sharper picture past 1). As a QLabel to its callers:
    ``clear``, ``setText``, ``pixmap``."""

    zoomed = pyqtSignal(float)
    MAX_ZOOM = 8.0
    STEP = 1.25

    def __init__(self, width: Optional[int], height: Optional[int], parent=None):
        super().__init__(parent)
        if width and height:
            self.setFixedSize(width, height)
        else:                                      # the quick look's: as large as its place, drawn again on a resize
            self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            self.setMinimumSize(120, 120)
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self._label = QLabel()
        self._label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setWidget(self._label)
        self.setWidgetResizable(False)
        self._source: Optional[QImage] = None
        self._turned: Optional[QImage] = None      # the source as turned (rotation), made once a turn
        self.zoom = 1.0
        self.rotation = 0                          # degrees clockwise: 0, 90, 180, 270
        self._drag = None

    # -- as a label
    def clear(self) -> None:
        self._source, self._turned, self.zoom, self.rotation = None, None, 1.0, 0
        self._label.clear()
        self._label.resize(self.size())
        self.viewport().unsetCursor()

    def setText(self, text: str) -> None:          # noqa: N802 -- QLabel's name
        rotation = self.rotation                   # a page without its picture keeps the turn
        self.clear()
        self.rotation = rotation
        self._label.setText(text)

    def pixmap(self) -> QPixmap:
        return self._label.pixmap()

    # -- the picture
    def set_image(self, image: QImage, keep_zoom: bool = False) -> None:
        """Show ``image``: fitted, or (``keep_zoom``: a sharper copy of the one on show) at the same zoom
        and place."""
        place = self._place() if keep_zoom else None
        self._source, self._turned = image, None
        if not keep_zoom:
            self.zoom = 1.0
        self._draw()
        if place is not None:
            self._go(*place)

    def _shown(self) -> Optional[QImage]:
        """The picture as it is drawn: the source turned by ``rotation`` (another page keeps the turn;
        another shelfmark, ``clear``, does not)."""
        if self._source is None or self._source.isNull() or not self.rotation:
            return self._source
        if self._turned is None:
            self._turned = self._source.transformed(QTransform().rotate(self.rotation),
                                                    Qt.TransformationMode.SmoothTransformation)
        return self._turned

    def rotate_by(self, degrees: int) -> None:
        """Turn the picture a quarter (``degrees`` 90 clockwise, -90 back), fitted again."""
        if self._source is None:
            return
        self.rotation = (self.rotation + degrees) % 360
        self._turned = None
        self.zoom = 1.0
        self._draw()
        self.zoomed.emit(self.zoom)

    def _fit(self) -> float:
        src = self._shown()
        if src is None or src.isNull() or not src.width() or not src.height():
            return 1.0
        return min(self.width() / src.width(), self.height() / src.height())

    def _draw(self) -> None:
        src = self._shown()
        if src is None or src.isNull():
            return
        scale = self._fit() * self.zoom
        size = QSize(max(1, round(src.width() * scale)), max(1, round(src.height() * scale)))
        pixmap = QPixmap.fromImage(src).scaled(size, Qt.AspectRatioMode.KeepAspectRatio,
                                               Qt.TransformationMode.SmoothTransformation)
        self._label.setPixmap(pixmap)
        self._label.resize(pixmap.size().expandedTo(self.size()) if self.zoom <= 1.0 else pixmap.size())
        if self.zoom > 1.0:
            self.viewport().setCursor(Qt.CursorShape.OpenHandCursor)
        else:
            self.viewport().unsetCursor()

    def _place(self, at=None) -> tuple:
        """The point of the picture at ``at`` (the viewport's centre by default), as fractions of it, and
        ``at`` itself."""
        at = at if at is not None else QPoint(self.width() // 2, self.height() // 2)
        w, h = max(1, self._label.width()), max(1, self._label.height())
        x = (self.horizontalScrollBar().value() + at.x()) / w
        y = (self.verticalScrollBar().value() + at.y()) / h
        return x, y, at

    def _go(self, x: float, y: float, at) -> None:
        self.horizontalScrollBar().setValue(round(x * self._label.width() - at.x()))
        self.verticalScrollBar().setValue(round(y * self._label.height() - at.y()))

    def zoom_by(self, factor: float, at=None) -> None:
        if self._source is None:
            return
        zoom = max(1.0, min(self.MAX_ZOOM, self.zoom * factor))
        if abs(zoom - self.zoom) < 1e-6:
            return
        place = self._place(at)
        self.zoom = zoom
        self._draw()
        self._go(*place)
        self.zoomed.emit(self.zoom)

    def reset_zoom(self) -> None:
        if self._source is not None and self.zoom != 1.0:
            self.zoom = 1.0
            self._draw()
            self.zoomed.emit(self.zoom)

    def resizeEvent(self, event):                  # noqa: N802 -- Qt's name
        super().resizeEvent(event)
        if self._source is not None and not self._source.isNull():
            place = self._place() if self.zoom > 1.0 else None
            self._draw()
            if place is not None:
                self._go(*place)

    # -- the mouse
    def wheelEvent(self, event):                   # noqa: N802 -- Qt's name
        if self._source is None:
            super().wheelEvent(event)
            return
        dy = event.angleDelta().y()
        if dy:
            self.zoom_by(self.STEP if dy > 0 else 1 / self.STEP, event.position().toPoint())
        event.accept()

    def mousePressEvent(self, event):              # noqa: N802
        if self.zoom > 1.0 and event.button() == Qt.MouseButton.LeftButton:
            self._drag = (event.position().toPoint(), self.horizontalScrollBar().value(),
                          self.verticalScrollBar().value())
            self.viewport().setCursor(Qt.CursorShape.ClosedHandCursor)
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):               # noqa: N802
        if self._drag is not None:
            start, h, v = self._drag
            delta = event.position().toPoint() - start
            self.horizontalScrollBar().setValue(h - delta.x())
            self.verticalScrollBar().setValue(v - delta.y())
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):            # noqa: N802
        if self._drag is not None:
            self._drag = None
            self.viewport().setCursor(Qt.CursorShape.OpenHandCursor)
            event.accept()
            return
        super().mouseReleaseEvent(event)

    def mouseDoubleClickEvent(self, event):        # noqa: N802
        self.reset_zoom()
        event.accept()


class _PickerLayoutKeys(QObject):
    """Alt+D and Ctrl+F by Windows' virtual key while the picker is the window in front, whatever the
    keyboard's layout makes of the letter (``ShelfPickerDialog._layout_shortcut``). A press a QShortcut
    took never arrives as a key press here, so nothing fires twice."""

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog

    def eventFilter(self, obj, event):             # noqa: N802 -- Qt's name
        if event.type() != QEvent.Type.KeyPress or not isinstance(obj, QWidget):
            return False
        if QApplication.activePopupWidget() is not None or obj.window() is not self.dialog:
            return False
        if self.dialog.look.isVisible():
            return False                           # the quick look's own keys (it covers the lists)
        return self.dialog._layout_shortcut(event)


class _AllBox(QCheckBox):
    """The box that ticks every join partner at once: it shows all, some or none ticked, and a click
    leaves its state to the partners' boxes (``_DetailsPane._sync_all``), not to Qt's cycle."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setTristate(True)

    def nextCheckState(self) -> None:          # noqa: N802 -- Qt's name
        pass


class _LinkButton(QPushButton):
    """A breadcrumb: flat, focusable, pressed by Enter as well as Space."""

    def __init__(self, text: str, parent=None):
        super().__init__(text, parent)
        self.setFlat(True)
        self.setAutoDefault(False)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self.setStyleSheet("QPushButton { color: palette(link); text-decoration: underline; border: none;"
                           " padding: 2px 4px; } QPushButton:focus { outline: 1px dotted palette(link); }")

    def keyPressEvent(self, event):
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            self.click()
            return
        super().keyPressEvent(event)


class _FilterBox(QLineEdit):
    """A list's filter. What is typed is the filter of the list it was typed in at once
    (``on_text(key, text)``: the box may show another level before the typing pauses); only the
    load waits for the pause (``on_load(key)``), or for Enter."""

    def __init__(self, on_text: Callable[[str, str], None], on_load: Callable[[str], None], parent=None):
        super().__init__(parent)
        self.setPlaceholderText(tr("Filter"))
        self.setClearButtonEnabled(True)
        self.key: Optional[str] = None
        self._pending: Optional[str] = None
        self._on_text = on_text
        self._on_load = on_load
        self._timer = QTimer(self)
        self._timer.setSingleShot(True)
        self._timer.setInterval(FILTER_DELAY_MS)
        self._timer.timeout.connect(self.flush)
        self.textEdited.connect(self._typed)
        self.returnPressed.connect(self.flush)
        self.escape_to: Optional[Callable[[], Optional[QWidget]]] = None   # Esc: the list it filters

    def event(self, event):                        # noqa: N802 -- Qt's name
        """Tab (Qt takes it before keyPressEvent) goes from the filter into its list. Alt+D and Ctrl+F by
        the key go to the dialog first: a Hebrew layout's Alt+ג would be typed into the box."""
        if event.type() == QEvent.Type.KeyPress:
            layout_shortcut = getattr(self.window(), '_layout_shortcut', None)
            if layout_shortcut is not None and layout_shortcut(event):
                return True
        if (event.type() == QEvent.Type.KeyPress and event.key() == Qt.Key.Key_Tab
                and not event.modifiers() & ~Qt.KeyboardModifier.KeypadModifier and self.escape_to is not None):
            target = self.escape_to()
            if target is not None and target.isVisible():
                self.flush()
                target.setFocus(Qt.FocusReason.TabFocusReason)
                return True
        return super().event(event)

    def keyPressEvent(self, event):                # noqa: N802 -- Qt's name
        """Esc leaves the filter for its list (pressed there again, it closes the picker)."""
        if event.key() == Qt.Key.Key_Escape and self.escape_to is not None:
            target = self.escape_to()
            if target is not None and target.isVisible():
                self.flush()
                target.setFocus(Qt.FocusReason.ShortcutFocusReason)
                event.accept()
                return
        super().keyPressEvent(event)

    def _typed(self, text: str) -> None:
        if self.key is None:
            return
        self._on_text(self.key, text)
        self._pending = self.key
        self._timer.start()

    def flush(self) -> None:
        self._timer.stop()
        pending, self._pending = self._pending, None
        if pending is not None:
            self._on_load(pending)

    def show_text(self, text: str) -> None:
        """The box of another list now: that list's filter, set only when it differs (setting it
        moves the caret). A load still owed for the list it showed before is asked for now."""
        self.flush()
        if self.text() != text:
            self.setText(text)

    def reset(self) -> None:
        """Empty, with nothing owed: the dialog has cleared every filter (``reveal``)."""
        self._timer.stop()
        self._pending = None
        if self.text():
            self.setText('')


def box_palette(palette: QPalette) -> QPalette:
    """``palette`` with an unticked box that shows. Fusion (the desktop's style) draws a box's outline
    in the window's colour, darkened: on a dark theme that is near black on near black, and a box was
    invisible until ticked (the exclusion pickers' node boxes, the shelfmarks'). On a dark theme the
    outline is drawn from the text's colour instead; a light theme keeps its own. For the boxes in the
    lists (``_RowDelegate``): a check box of its own is filled on a dark theme and shows already."""
    if palette.color(QPalette.ColorRole.Base).lightness() >= 128:
        return palette
    out = QPalette(palette)
    out.setColor(QPalette.ColorRole.Window, palette.color(QPalette.ColorRole.Text))
    return out


class _RowDelegate(QStyledItemDelegate):
    """Rows are chosen whole, and their choice is drawn here: a selected row's cells get the
    highlight behind them, and the style is not told a cell is selected or current. It would mark
    each cell on its own (the Windows 11 style draws a short bar at the start of every selected
    cell: one before the name, another before the count; a focus frame in the current one). Each
    line also gets a little air above and below it."""

    #: Pixels added to a row's height, above and below its text together.
    PAD = 8

    def initStyleOption(self, option, index):
        super().initStyleOption(option, index)
        option.state &= ~QStyle.StateFlag.State_HasFocus

    def paint(self, painter, option, index):
        opt = QStyleOptionViewItem(option)
        selected = bool(opt.state & QStyle.StateFlag.State_Selected)
        if selected:
            painter.fillRect(opt.rect, opt.palette.color(QPalette.ColorRole.Highlight))
            opt.palette.setColor(QPalette.ColorRole.Text, opt.palette.color(QPalette.ColorRole.HighlightedText))
            opt.state &= ~QStyle.StateFlag.State_Selected
        opt.state &= ~QStyle.StateFlag.State_HasFocus
        if index.data(Qt.ItemDataRole.CheckStateRole) is not None:
            opt.palette = box_palette(opt.palette)    # an unticked box shows on a dark theme
        marks = index.data(_SIGNS_ROLE)
        if not marks:
            super().paint(painter, opt, index)
            return
        # a shelfmark's signs at its row's end side: the text keeps out of their room
        room = signs_room(opt.font, marks)
        text = QStyleOptionViewItem(opt)
        text.rect = (opt.rect.adjusted(room, 0, 0, 0) if opt.direction == Qt.LayoutDirection.RightToLeft
                     else opt.rect.adjusted(0, 0, -room, 0))
        super().paint(painter, text, index)
        _draw_signs(painter, opt, marks, selected)

    def sizeHint(self, option, index):
        size = super().sizeHint(option, index)
        return QSize(size.width(), size.height() + self.PAD)


class _TileDelegate(QStyledItemDelegate):
    """A thumbnail's tile: the picture (``_THUMB_ROLE``; a grey place while it comes, a dash when the
    manuscript has none), the shelfmark under it with its signs, the tick box over the picture's
    start corner in the multiple choice, a frame in the highlight's colour when it is the one chosen."""

    def __init__(self, panel: "_LeafPanel"):
        super().__init__(panel.gallery)
        self.panel = panel

    def sizeHint(self, option, index):
        return self.panel.tile_size()

    def paint(self, painter, option, index):
        painter.save()
        rect = option.rect.adjusted(3, 3, -3, -3)
        palette = option.palette
        selected = bool(option.state & QStyle.StateFlag.State_Selected)
        ticked = index.data(Qt.ItemDataRole.CheckStateRole) == Qt.CheckState.Checked
        if ticked:
            painter.fillRect(rect, palette.color(QPalette.ColorRole.AlternateBase))
        pen = painter.pen()
        pen.setColor(palette.color(QPalette.ColorRole.Highlight) if selected or ticked
                     else palette.color(QPalette.ColorRole.Mid))
        pen.setWidth(3 if selected else 1)
        painter.setPen(pen)
        painter.drawRoundedRect(rect, 6, 6)
        width, height = self.panel.picture_size()
        pic = QRect(rect.left() + (rect.width() - width) // 2, rect.top() + 5, width, height)
        picture = index.data(_THUMB_ROLE)
        if isinstance(picture, QPixmap) and not picture.isNull():
            x = pic.left() + (pic.width() - picture.width()) // 2
            y = pic.top() + (pic.height() - picture.height()) // 2
            painter.drawPixmap(x, y, picture)
        else:
            painter.fillRect(pic, palette.color(QPalette.ColorRole.Button))
            if picture is False:                   # none to show: a dash, not an endless grey
                painter.setPen(palette.color(QPalette.ColorRole.PlaceholderText))
                painter.drawText(pic, int(Qt.AlignmentFlag.AlignCenter), '—')
        check = index.data(Qt.ItemDataRole.CheckStateRole)
        if check is not None:
            box = QStyleOptionViewItem(option)
            box.palette = box_palette(palette)
            size = option.widget.style().pixelMetric(QStyle.PixelMetric.PM_IndicatorWidth) if option.widget else 16
            rtl = option.direction == Qt.LayoutDirection.RightToLeft
            box.rect = QRect(pic.right() - size - 4 if rtl else pic.left() + 4, pic.top() + 4, size, size)
            box.state = (box.state & ~QStyle.StateFlag.State_Selected) | (
                QStyle.StateFlag.State_On if check == Qt.CheckState.Checked else QStyle.StateFlag.State_Off)
            painter.fillRect(box.rect.adjusted(-2, -2, 2, 2), palette.color(QPalette.ColorRole.Base))
            style = option.widget.style() if option.widget else QApplication.style()
            style.drawPrimitive(QStyle.PrimitiveElement.PE_IndicatorItemViewItemCheck, box, painter, option.widget)
        line = QFontMetrics(option.font).height()
        caption = QRect(rect.left() + 6, pic.bottom() + 4, rect.width() - 12, line + 4)
        marks = tuple(index.data(_SIGNS_ROLE) or ())
        room = signs_room(option.font, marks) if marks else 0
        text_rect = (caption.adjusted(room, 0, 0, 0) if option.direction == Qt.LayoutDirection.RightToLeft
                     else caption.adjusted(0, 0, -room, 0))
        painter.setPen(palette.color(QPalette.ColorRole.Text))
        text = QFontMetrics(option.font).elidedText(index.data(Qt.ItemDataRole.DisplayRole) or '',
                                                    Qt.TextElideMode.ElideRight, text_rect.width())
        painter.drawText(text_rect, int(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeading), text)
        if marks:
            sign = QStyleOptionViewItem(option)
            sign.rect = caption
            _draw_signs(painter, sign, marks)
        # the start of the title, readable at a glance (owner, 2026-11-22), lined up as it reads
        title = index.data(_TITLE_ROLE) or ''
        if title:
            font = _title_font(option.font)
            painter.setFont(font)
            painter.setPen(palette.color(QPalette.ColorRole.PlaceholderText))
            side = (Qt.AlignmentFlag.AlignRight if _direction_of(title) == Qt.LayoutDirection.RightToLeft
                    else Qt.AlignmentFlag.AlignLeft)
            step = QFontMetrics(font).height()
            top = caption.bottom() + 2
            for n, text in enumerate(tile_title_lines(title, font, caption.width())):
                painter.drawText(QRect(caption.left(), top + n * step, caption.width(), step),
                                 int(Qt.AlignmentFlag.AlignAbsolute | side | Qt.AlignmentFlag.AlignVCenter), text)
        painter.restore()


class _NodeColumn(QWidget):
    """One column of nodes: a title, a filter, the rows (name | count) and "Show more". In the
    multiple-choice mode a narrow first column holds each row's box (below the libraries): a click
    on the box ticks the node, a click on its name opens it."""

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog
        self.key: Optional[str] = None
        self.col: Optional[dict] = None
        self.rows: list = []
        self.chosen_row: Optional[int] = None
        self.name_col, self.count_col = 0, 1      # set_mode: a box column first in multiple choice
        self.tickable = False                      # this column's rows have boxes (multiple choice)
        self.boxes: dict = {}                      # node id -> row
        self.oversized: dict = {}                  # node id -> (tooltip, tooltip when off): too big to tick whole
        self.leaf_rows: dict = {}                  # sys_id -> rows (a filter's shelfmarks)
        self._keep_chosen = False
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        self.title = QLabel()
        self.title.setWordWrap(True)
        bold = self.title.font()
        bold.setBold(True)
        self.title.setFont(bold)
        self.filter = _FilterBox(dialog._filter_typed, dialog._filter_load, self)
        self.table = QTableWidget(0, self.count_col + 1, self)
        self.table.setTabKeyNavigation(False)          # Tab goes to the next list, never the next cell
        self.filter.escape_to = lambda: self.table
        self.table.horizontalHeader().setVisible(False)
        self.table.verticalHeader().setVisible(False)
        self.set_mode()
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setShowGrid(False)
        self.table.setItemDelegate(_RowDelegate(self.table))
        self.table.setWordWrap(True)                    # a library's name is never cut: it wraps
        self.table.setTextElideMode(Qt.TextElideMode.ElideNone)
        self.table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        # A row is as tall as its name at the width the name is drawn at: one line, or two where a
        # long name wraps. Measured again once the columns have taken their widths (the counts'
        # column sizes itself after the rows are filled, and the dialog lays out after it), else
        # rows keep the heights of a narrower first layout ("T-S 6F - 8K" on two lines).
        self._rows_timer = QTimer(self)
        self._rows_timer.setSingleShot(True)
        self._rows_timer.setInterval(0)
        self._rows_timer.timeout.connect(self.size_rows)
        self.table.horizontalHeader().sectionResized.connect(lambda *_a: self._rows_timer.start())
        self.table.viewport().installEventFilter(self)
        self.table.installEventFilter(self)
        self.table.verticalScrollBar().installEventFilter(self)
        self.table.verticalScrollBar().rangeChanged.connect(self._range_changed)
        self.table.clicked.connect(lambda index: dialog._clicked_row(self, index.row(), index.column()))
        self.table.installEventFilter(dialog)
        self.table.viewport().installEventFilter(dialog)
        self.more = QPushButton()
        self.more.setAutoDefault(False)
        self.more.clicked.connect(lambda: dialog._show_more(self.key, NODE_ROWS))
        layout.addWidget(self.title)
        layout.addWidget(self.filter)
        layout.addWidget(self.table, 1)
        layout.addWidget(self.more)

    def set_mode(self) -> None:
        """The table's columns for the dialog's mode: a narrow box column first in multiple choice.
        The rows are filled again by the next draw."""
        multi = self.dialog.multi
        self.name_col = 1 if multi else 0
        self.count_col = self.name_col + 1
        self.table.clearSpans()
        self.table.setColumnCount(self.count_col + 1)
        header = self.table.horizontalHeader()
        if multi:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(self.name_col, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(self.count_col, QHeaderView.ResizeMode.ResizeToContents)

    def eventFilter(self, obj, event):
        kind = event.type()
        if obj is self.table.viewport() and kind == QEvent.Type.Resize:
            self._rows_timer.start()
        elif kind in (QEvent.Type.Wheel, QEvent.Type.MouseButtonPress, QEvent.Type.KeyPress):
            self._keep_chosen = False                  # the reader moves in this list now
        return False

    def size_rows(self) -> None:
        self.table.resizeRowsToContents()
        if self._keep_chosen and self.chosen_row is not None:
            # the chosen row was scrolled to at the old heights: in sight again at the new ones.
            # The table takes the new heights into its scroll range in a timer of its own, and
            # which runs first is the platform's: on Windows the range comes before this call, on
            # Linux after it, against the old range. So also when the range changes (below).
            QTimer.singleShot(0, self._show_chosen)

    def _range_changed(self, *_range) -> None:
        # not while the column is being filled (updates off): the chosen row is the old list's then
        if self._keep_chosen and self.table.updatesEnabled():
            self._show_chosen()

    def _show_chosen(self) -> None:
        item = self.table.item(self.chosen_row, self.name_col) if self.chosen_row is not None else None
        if self._keep_chosen and item is not None:
            self.table.scrollToItem(item, QAbstractItemView.ScrollHint.EnsureVisible)

    def scroll_value(self) -> int:
        return self.table.verticalScrollBar().value()

    def show_column(self, col: dict) -> None:
        self.col = col
        self.key = col['filter_key']
        libs = bool(col.get('libs'))
        # the web's columns: 270 px for the libraries (at least 200), 220 for the others (at least 160)
        self.setMinimumWidth(200 if libs else 160)
        self.setMaximumWidth(270 if libs else 220)
        self.title.setText(own_direction(col['title'], self.isRightToLeft()))
        if self.filter.key != self.key:            # another list: its own filter; the same: left as typed
            self.filter.key = self.key
            self.filter.show_text(self.dialog._filters.get(self.key, ''))
        self.fill(col)

    def fill(self, col: dict) -> None:
        """The rows of the column (and, under a filter with a number, shelfmarks)."""
        self.col = col
        rtl = self.isRightToLeft()
        table = self.table
        multi = self.dialog.multi
        name_col, count_col = self.name_col, self.count_col
        known = self.dialog._paint_state if multi else None    # the last answer, until the next one
        # a whole node is ticked by "Select all" over its shelfmarks (DX23); the exclusion pickers
        # keep a box on every node below the libraries (excluding a whole collection is their point)
        self.tickable = multi and not col.get('libs') and self.dialog._node_boxes
        self.boxes, self.leaf_rows, self.oversized = {}, {}, {}
        table.setUpdatesEnabled(False)
        table.clearSpans()
        table.clearContents()
        table.setRowCount(len(col['rows']))
        self.rows = list(col['rows'])
        chosen = None
        for r, row in enumerate(self.rows):
            if 'sep' in row:
                item = QTableWidgetItem(own_direction(row['sep'], rtl))
                item.setFlags(Qt.ItemFlag.NoItemFlags)
                font = item.font()
                font.setBold(True)
                item.setFont(font)
                table.setItem(r, 0, item)
                table.setSpan(r, 0, 1, count_col + 1)
                continue
            if 'leaf' in row:
                item = QTableWidgetItem(own_direction(row['label'], rtl))
                marks = _signs_of(row['leaf'].sys_id)
                tip = _with_signs(leaf_tooltip(row) or row['label'], marks)
                item.setToolTip(tip)
                item.setData(_ROW, r)
                if marks:
                    item.setData(_SIGNS_ROLE, marks)
                table.setItem(r, name_col, item)
                table.setItem(r, count_col, QTableWidgetItem(''))
                if multi:
                    sid = row['leaf'].sys_id
                    ticked = known is not None and sid in known['chosen']
                    table.setItem(r, 0, self._box(r, 'all' if ticked else 'none', tip))
                    self.leaf_rows.setdefault(sid, []).append(r)
                continue
            name = ('↪ ' if row.get('link') else '') + own_direction(row['label'], rtl)
            if row.get('path'):
                name += '\n' + own_direction(row['path'], rtl)
            item = QTableWidgetItem(name)
            item.setData(_ROW, r)
            item.setToolTip(f"{row['label']} · {row['title']}" if row.get('title') else row['label'])
            font = item.font()
            font.setItalic(bool(row.get('link')))
            font.setBold(bool(row.get('on')))
            item.setFont(font)
            count = QTableWidgetItem(f"{row['count']:,}")
            count.setTextAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignTrailing)
            count.setFlags(count.flags() & ~Qt.ItemFlag.ItemIsEditable)
            table.setItem(r, name_col, item)
            table.setItem(r, count_col, count)
            if multi:
                if self.tickable:
                    state = (known or {}).get('nodes', {}).get(row['id'], 'none')
                    tip = f"{tr('Choose all of it')}: {row['label']}"
                    box = self._box(r, state, tip)
                    limit_n = self.dialog.too_big(row.get('count'))
                    if limit_n is not None:
                        self.oversized[row["id"]] = (tip, tr("Too many to choose here (up to {n})").format(
                            n=f"{limit_n:,}"))
                        _box_off(box, state != 'all', *self.oversized[row["id"]])
                    table.setItem(r, 0, box)
                    self.boxes[row['id']] = r
                else:
                    blank = QTableWidgetItem('')
                    blank.setData(_ROW, r)
                    blank.setFlags(_BOX_FLAGS)
                    table.setItem(r, 0, blank)
            if row.get('on') and chosen is None:
                chosen = r
        table.resizeRowsToContents()
        self._rows_timer.start()                       # again at the widths the rows are drawn at
        table.clearSelection()
        if chosen is not None:
            table.selectRow(chosen)
            table.setCurrentCell(chosen, name_col)
        table.setUpdatesEnabled(True)
        self.chosen_row = chosen
        more = col.get('more') or 0
        self.more.setText(f"{tr('Show more')} ({more:,})")
        self.more.setVisible(bool(more))

    def show_signs(self) -> None:
        """The signs, now loaded, on the shelfmark rows drawn before (a filter's)."""
        for r, row in enumerate(self.rows):
            marks = _signs_of(row['leaf'].sys_id) if 'leaf' in row else ()
            item = self.table.item(r, self.name_col) if marks else None
            if item is not None:
                item.setData(_SIGNS_ROLE, marks)
                item.setToolTip(_with_signs(leaf_tooltip(row) or row['label'], marks))

    @staticmethod
    def _box(row: int, state: str, tip: str) -> QTableWidgetItem:
        box = QTableWidgetItem('')
        box.setData(_ROW, row)
        box.setFlags(_BOX_FLAGS)
        box.setCheckState(_CHECK[state])
        box.setToolTip(tip)
        return box

    def is_box_row(self, row: int) -> bool:
        """A row whose box a click or Space ticks: a node below the libraries, or a shelfmark."""
        return (self.dialog.multi and 0 <= row < len(self.rows)
                and ('leaf' in self.rows[row] or (self.tickable and 'id' in self.rows[row])))

    def paint(self, state: dict) -> None:
        """The boxes, as ``state`` (``paint_state``) has them."""
        for node_id, row in self.boxes.items():
            item = self.table.item(row, 0)
            box_state = state['nodes'].get(node_id, 'none')
            look = _CHECK[box_state]
            if item is not None and item.checkState() != look:
                item.setCheckState(look)
            if item is not None and node_id in self.oversized:
                _box_off(item, box_state != 'all', *self.oversized[node_id])
        for sid, rows in self.leaf_rows.items():
            look = _CHECK['all' if sid in state['chosen'] else 'none']
            for row in rows:
                item = self.table.item(row, 0)
                if item is not None and item.checkState() != look:
                    item.setCheckState(look)

    def scroll_to(self, saved: Optional[int]) -> None:
        self._keep_chosen = saved is None and self.chosen_row is not None
        if saved is not None:
            self.table.verticalScrollBar().setValue(saved)
        elif self.chosen_row is not None:
            self.table.scrollToItem(self.table.item(self.chosen_row, self.name_col),
                                    QAbstractItemView.ScrollHint.EnsureVisible)
        else:
            self.table.scrollToTop()


class _LeafPanel(QWidget):
    """The shelfmarks of the node chosen last: a title with the view switch (VX1: List, Table,
    Thumbnails, and the thumbnails' size), a filter, the shelfmarks in the view chosen and "Show more".
    In the multiple-choice mode each shelfmark has a box, and "Select all in <node>" sits above them.

    The views are a stack: the list (a wrapping grid, as before), the table (the search results table's
    columns, ``shared.shelfmark_tree_rows.COLUMNS``, its details from ``row_details`` through the
    dialog's ``_request_rows``) and the thumbnails (tiles, ``_TileDelegate``, each picture fetched once
    in sight by the dialog's ``_ThumbPool``). Only the view shown is filled; every item holds its leaf
    (``_ROW``), so a click anywhere on it is that shelfmark's, and the dialog asks this panel for the
    leaf at a point, the current one, the marking, the boxes, the focus and the scroll of whichever
    view is shown."""

    VIEW_INDEX = {'list': 0, 'table': 1, 'gallery': 2}

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog
        self.key: Optional[str] = None
        self.leaves: Optional[dict] = None
        self.chosen = None                         # the list's / tiles' item, or the table's row, chosen
        self.items_by_sid: dict = {}               # sys_id -> boxes (list items, tiles, the table's box cells)
        self.table_rows: dict = {}                 # sys_id -> the table's rows
        self.tiles: dict = {}                      # sys_id -> tiles
        self.view, self.size = load_view_choice()
        self._anchor: Optional[str] = None         # a view switch: the shelfmark to bring to the top
        # the place a view was entered with, kept until the reader scrolls it (``_moved``)
        self._entered: Optional[str] = None
        self._moved = False
        self._need = LEAF_MIN_WIDTH
        self._row_height = self.fontMetrics().height() + 12
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        # the title on its line, the switch on the next (beside the title it wrapped it in a narrow pane)
        head = QHBoxLayout()
        head.setSpacing(4)
        self.title = QLabel()
        self.title.setWordWrap(True)
        bold = self.title.font()
        bold.setBold(True)
        self.title.setFont(bold)
        self.view_buttons: dict = {}
        self._view_group = QButtonGroup(self)
        self._view_group.setExclusive(True)
        for key, label, tip in (('list', 'List', 'Show as a list'), ('table', 'Table', 'Show as a table'),
                                ('gallery', 'Thumbnails', 'Show as thumbnails')):
            button = QToolButton(self)
            button.setText(tr(label))
            button.setToolTip(tr(tip))
            button.setAccessibleName(tr(tip))
            button.setCheckable(True)
            button.setAutoRaise(True)
            button.setObjectName(f"shelfView_{key}")
            button.clicked.connect(lambda _checked=False, k=key: self.set_view(k))
            self._view_group.addButton(button)
            self.view_buttons[key] = button
            head.addWidget(button)
        self.size_box = QComboBox(self)
        for key in SIZES:
            self.size_box.addItem(tr(SIZE_LABELS[key]), key)
        self.size_box.setToolTip(tr('Thumbnail size'))
        self.size_box.setAccessibleName(tr('Thumbnail size'))
        self.size_box.activated.connect(lambda i: self.set_size(self.size_box.itemData(i)))
        head.addWidget(self.size_box)
        head.addStretch(1)
        self.filter = _FilterBox(dialog._filter_typed, dialog._filter_load, self)
        self.filter.escape_to = lambda: self.active()
        # "Select all in <node>": the whole node, whatever the filter shows (its count is the node's)
        self.whole = QCheckBox(self)
        self.whole.setTristate(True)
        self.whole.setVisible(False)
        self.whole.clicked.connect(lambda _checked=False: dialog._toggle_whole())
        self.grid = QListWidget(self)
        self.grid.setViewMode(QListView.ViewMode.ListMode)
        self.grid.setFlow(QListView.Flow.LeftToRight)
        self.grid.setWrapping(True)
        self.grid.setResizeMode(QListView.ResizeMode.Adjust)
        self.grid.setUniformItemSizes(True)
        self.grid.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.grid.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.grid.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.grid.setItemDelegate(_RowDelegate(self.grid))
        self.table = QTableWidget(0, 0, self)
        self.table.setTabKeyNavigation(False)          # Tab goes to the next list, never the next cell
        self.table.verticalHeader().setVisible(False)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setShowGrid(False)
        self.table.setWordWrap(False)
        self.table.setTextElideMode(Qt.TextElideMode.ElideRight)
        self.table.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.table.setItemDelegate(_RowDelegate(self.table))
        # every column the reader may resize (owner, 2026-11-22), the widths kept for the run
        self.table.horizontalHeader().setStretchLastSection(False)
        self.table.horizontalHeader().sectionResized.connect(self._column_resized)
        self._sizing = False                       # the widths being set here, not by the reader
        self._fit_timer = QTimer(self)
        self._fit_timer.setSingleShot(True)
        self._fit_timer.setInterval(0)
        self._fit_timer.timeout.connect(self.fit_table)
        self.gallery = QListWidget(self)
        self.gallery.setViewMode(QListView.ViewMode.IconMode)
        self.gallery.setMovement(QListView.Movement.Static)
        self.gallery.setResizeMode(QListView.ResizeMode.Adjust)
        self.gallery.setWrapping(True)
        self.gallery.setUniformItemSizes(True)
        self.gallery.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.gallery.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.gallery.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.gallery.setItemDelegate(_TileDelegate(self))
        self.stack = QStackedWidget(self)
        for widget in (self.grid, self.table, self.gallery):
            self.stack.addWidget(widget)
            widget.itemClicked.connect(lambda item: dialog._clicked_leaf(item))
            widget.installEventFilter(dialog)
            widget.viewport().installEventFilter(dialog)
            widget.viewport().installEventFilter(self)
        # the tiles in sight are asked for once the scrolling pauses
        self._thumb_timer = QTimer(self)
        self._thumb_timer.setSingleShot(True)
        self._thumb_timer.setInterval(THUMBS_DELAY_MS)
        self._thumb_timer.timeout.connect(self.queue_visible)
        self.gallery.verticalScrollBar().valueChanged.connect(lambda _v: self._thumb_timer.start())
        for widget in (self.grid, self.table, self.gallery):     # the reader's own scrolling (not ours)
            widget.verticalScrollBar().actionTriggered.connect(lambda _a: setattr(self, '_moved', True))
        self.more = QPushButton()
        self.more.setAutoDefault(False)
        self.more.clicked.connect(lambda: dialog._show_more(self.key, PAGE_SIZE))
        layout.addWidget(self.title)
        layout.addLayout(head)
        layout.addWidget(self.filter)
        layout.addWidget(self.whole)
        layout.addWidget(self.stack, 1)
        layout.addWidget(self.more)
        self._show_view_parts()

    # -- the view ---------------------------------------------------------------------------

    def _show_view_parts(self) -> None:
        for key, button in self.view_buttons.items():
            button.setChecked(key == self.view)
        self.size_box.setCurrentIndex(SIZES.index(self.size))
        self.size_box.setVisible(self.view == 'gallery')
        self.stack.setCurrentIndex(self.VIEW_INDEX[self.view])

    def set_view(self, view: str) -> None:
        """The switch (VX1): kept in the app's settings; the dialog is drawn again, as the columns it
        shows change (VX2)."""
        view = normal_view(view)
        if view == self.view:
            self._show_view_parts()
            return
        # the reader's place in the view being left: the one it was entered with while the reader has not
        # scrolled it (a table showing all its rows has its first at the top), else what is at its top
        if self._entered is not None and not self._moved:
            place = self._entered
        else:
            place = getattr(self.top_leaf(), 'id', None)
        self.view = view
        save_view_choice(self.view, self.size)
        # its pixel offset means nothing in another view: the next drawing brings that shelfmark to the top
        self.dialog._scroll.pop(self.key, None)
        self._anchor = self._entered = place
        self._moved = False
        if view != 'gallery':
            # the thumbnails still waiting are not fetched (none is in sight now); those under way end
            self.dialog._thumb_pool.want([])
        self._show_view_parts()
        self.dialog._view_changed()

    def set_size(self, size: str) -> None:
        size = normal_size(size)
        if size == self.size:
            return
        self.size = size
        save_view_choice(self.view, self.size)
        if self.leaves is not None and self.view == 'gallery':
            top = self.top_tile()                        # the reader's place, kept across the new sizes
            self.fill(self.leaves)
            if self.dialog._chosen_leaf is not None:     # the tile chosen stays chosen (and current)
                self.mark(self.dialog._chosen_leaf)
            self.gallery.doItemsLayout()
            item = self.tiles.get(top, [None])[0] if top else None
            if item is not None:
                self.gallery.scrollToItem(item, QAbstractItemView.ScrollHint.PositionAtTop)
            else:
                self.gallery.scrollToTop()
            self._thumb_timer.start()

    def top_leaf(self):
        """The shelfmark at the top of the view shown (its first row or tile in sight), or None."""
        view = self.active()
        if isinstance(view, QTableWidget):
            row = view.rowAt(0)
            item = view.item(row, 0) if row >= 0 else None
            return item.data(_ROW) if item is not None else None
        sight = view.viewport().rect()
        for i in range(view.count()):
            item = view.item(i)
            if view.visualItemRect(item).intersects(sight):
                return item.data(_ROW)
        return None

    def _bring_to_top(self, leaf_id: str) -> bool:
        view = self.active()
        if isinstance(view, QTableWidget):
            for r in range(view.rowCount()):
                item = view.item(r, 0)
                if item is not None and getattr(item.data(_ROW), 'id', None) == leaf_id:
                    view.scrollToItem(item, QAbstractItemView.ScrollHint.PositionAtTop)
                    return True
            return False
        view.doItemsLayout()
        for i in range(view.count()):
            item = view.item(i)
            if getattr(item.data(_ROW), 'id', None) == leaf_id:
                view.scrollToItem(item, QAbstractItemView.ScrollHint.PositionAtTop)
                return True
        return False

    def top_tile(self) -> Optional[str]:
        """The sys_id of the first tile in sight (at the gallery's top), or None."""
        sight = self.gallery.viewport().rect()
        for i in range(self.gallery.count()):
            item = self.gallery.item(i)
            if self.gallery.visualItemRect(item).intersects(sight):
                leaf = item.data(_ROW)
                return leaf.sys_id if leaf is not None else None
        return None

    def active(self) -> QAbstractItemView:
        return (self.grid, self.table, self.gallery)[self.VIEW_INDEX[self.view]]

    def views(self) -> tuple:
        return (self.grid, self.table, self.gallery)

    def viewports(self) -> tuple:
        return tuple(view.viewport() for view in self.views())

    def leaf_at(self, viewport, point):
        """The shelfmark under ``point`` in ``viewport`` (any cell of the table), or None."""
        for view in self.views():
            if view.viewport() is viewport:
                item = view.itemAt(point)
                return item.data(_ROW) if item is not None else None
        return None

    def current_leaf(self):
        item = self.active().currentItem()
        return item.data(_ROW) if item is not None else None

    def eventFilter(self, obj, event):
        if event.type() == QEvent.Type.Resize:
            if obj is self.grid.viewport():
                self.fit()
            elif obj is self.gallery.viewport():
                self._thumb_timer.start()
            elif obj is self.table.viewport() and not _table_widths:
                self._fit_timer.start()             # fitted to the pane until the reader sizes a column
        return False

    # -- painting ---------------------------------------------------------------------------

    def paint(self, state: dict) -> None:
        """The boxes and "Select all", as ``state`` (``paint_state``) has them."""
        for sid, items in self.items_by_sid.items():
            look = _CHECK['all' if sid in state['chosen'] else 'none']
            for item in items:
                if item.checkState() != look:
                    item.setCheckState(look)
        whole = state.get('whole')
        shown = self.leaves is not None and whole is not None and whole['id'] == self.leaves['node']
        self.whole.setVisible(bool(shown))
        if shown:
            # the name isolated (a Latin "Schoeyen" in a Hebrew sentence), the count outside it
            name = f"{_FSI}{whole['name']}{_PDI}"
            self.whole.setText(f"{tr('Select all in {name}').format(name=name)} ({whole['total']:,})")
            self.whole.setCheckState(_CHECK[whole['state']])
            limit_n = self.dialog.too_big(whole['total'])
            off = limit_n is not None and whole['state'] != 'all'
            self.whole.setEnabled(not off)
            self.whole.setToolTip(tr("Too many to choose here (up to {n})").format(n=f"{limit_n:,}") if off else '')

    def scroll_value(self) -> int:
        return self.active().verticalScrollBar().value()

    def title_text(self, leaves: dict) -> str:
        return f"{tr('Shelfmarks')} ({leaves['total']:,}) — {own_direction(leaves['title'], self.isRightToLeft())}"

    def show_leaves(self, leaves: dict) -> None:
        self.key = leaves['filter_key']
        if self.filter.key != self.key:
            self.filter.key = self.key
            self.filter.show_text(self.dialog._filters.get(self.key, ''))
        self.fill(leaves)

    def fill(self, leaves: dict) -> None:
        """The shelfmarks in the view shown (the others emptied); then the table's details and the tiles'
        pictures are asked for, off the UI thread."""
        self.leaves = leaves
        # the tiles are replaced: what waited for the old ones is not fetched (the new ones in sight are
        # asked for once drawn); those under way end
        self.dialog._thumb_pool.want([])
        self.title.setText(self.title_text(leaves))
        self.chosen = None
        self.items_by_sid, self.table_rows, self.tiles = {}, {}, {}
        multi = self.dialog.multi
        known = self.dialog._paint_state if multi else None    # the last answer, until the next one
        for view in self.views():
            if view is not self.active():
                if isinstance(view, QTableWidget):
                    view.setRowCount(0)
                else:
                    view.clear()
        if self.view == 'table':
            self._fill_table(leaves, multi, known)
        elif self.view == 'gallery':
            self._fill_gallery(leaves, multi, known)
        else:
            self._fill_list(leaves, multi, known)
        if multi and (known is None or (known.get('whole') or {}).get('id') != leaves['node']):
            self.whole.setVisible(False)          # until the answer for this node says what it holds
        more = leaves.get('more') or 0
        self.more.setText(f"{tr('Show more')} ({more:,})")
        self.more.setVisible(bool(more))
        if self.view == 'table':
            self.dialog._request_rows()
        elif self.view == 'gallery':
            self._thumb_timer.start()

    def _box(self, item, sid: str, known: Optional[dict]) -> None:
        item.setFlags(_BOX_FLAGS)
        item.setCheckState(_CHECK['all' if known is not None and sid in known['chosen'] else 'none'])
        self.items_by_sid.setdefault(sid, []).append(item)

    def _fill_list(self, leaves: dict, multi: bool, known: Optional[dict]) -> None:
        rtl = self.isRightToLeft()
        highlight = self.dialog._highlight
        grid = self.grid
        grid.setUpdatesEnabled(False)
        grid.clear()
        bold = QFont(grid.font())
        bold.setBold(True)
        labels = []
        for row in leaves['rows']:
            leaf = row['leaf']
            item = QListWidgetItem(own_direction(row['label'], rtl))
            item.setData(_ROW, leaf)
            marks = _signs_of(leaf.sys_id)
            item.setToolTip(_with_signs(leaf_tooltip(row) or row['label'], marks))
            if marks:
                item.setData(_SIGNS_ROLE, marks)
            if multi:
                self._box(item, leaf.sys_id, known)
            if leaf.id == highlight:
                item.setFont(bold)
                self.chosen = item
            grid.addItem(item)
            labels.append(row['label'])
        # As many columns as show the longest shelfmark whole (the web's _FIT_GRID_JS).
        bold_metrics = QFontMetrics(bold)
        widest = max((bold_metrics.horizontalAdvance(text) for text in labels), default=0)
        if multi:                                  # the box takes its room too
            widest += self.style().pixelMetric(QStyle.PixelMetric.PM_IndicatorWidth) + 8
        if signs is not None:                      # and the signs theirs, before they come
            widest += signs_room(grid.font())
        self._need = max(LEAF_MIN_WIDTH, widest + 24)
        self._row_height = self.line_height()
        # Every shelfmark as wide as the longest: the grid's items are all the size of its first
        # (uniform sizes), and a short first one ("705") cut the rest to "18..." (Schoeyen)
        hint = QSize(self._need, self._row_height - _RowDelegate.PAD)
        for i in range(grid.count()):
            grid.item(i).setSizeHint(hint)
        self.fit()
        if self.chosen is not None:
            grid.setCurrentItem(self.chosen)
        grid.setUpdatesEnabled(True)

    def table_columns(self) -> dict:
        """Each table column's index: a box column first in the multiple choice."""
        start = 1 if self.dialog.multi else 0
        return {key: i + start for i, (key, _label) in enumerate(COLUMNS)}

    def _fill_table(self, leaves: dict, multi: bool, known: Optional[dict]) -> None:
        rtl = self.isRightToLeft()
        highlight = self.dialog._highlight
        table = self.table
        cols = self.table_columns()
        table.setUpdatesEnabled(False)
        table.clearContents()
        table.setColumnCount(len(cols) + (1 if multi else 0))
        heads = ([''] if multi else []) + [tr(label) for _key, label in COLUMNS]
        table.setHorizontalHeaderLabels(heads)
        header = table.horizontalHeader()
        for key, col in cols.items():
            head = table.horizontalHeaderItem(col)
            if head is not None and key in HEADER_TIPS:
                head.setToolTip(tr(HEADER_TIPS[key]))
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
        if multi:
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        table.setRowCount(len(leaves['rows']))
        details = self.dialog._row_details
        for r, row in enumerate(leaves['rows']):
            leaf = row['leaf']
            sid = leaf.sys_id
            self.table_rows.setdefault(sid, []).append(r)
            for col in range(table.columnCount()):
                cell = QTableWidgetItem('')
                cell.setData(_ROW, leaf)
                cell.setFlags(_BOX_FLAGS)
                table.setItem(r, col, cell)
            if multi:
                self._box(table.item(r, 0), sid, known)
            table.item(r, cols['shelfmark']).setText(own_direction(row['label'], rtl))
            if leaf.id == highlight:
                self.chosen = r
            self._fill_row(r, row, details.get(sid))
        table.setUpdatesEnabled(True)
        if _table_widths:
            self._apply_widths(_table_widths)
        else:
            self._fit_timer.start()                 # once laid out: the pane's width is known then
        if self.chosen is not None:                 # the row selected with it (see ``mark``)
            table.setCurrentCell(self.chosen, cols['shelfmark'])

    def _apply_widths(self, widths: dict) -> None:
        self._sizing = True
        try:
            for key, col in self.table_columns().items():
                if widths.get(key):
                    self.table.setColumnWidth(col, int(widths[key]))
        finally:
            self._sizing = False

    def fit_table(self) -> None:
        """The first widths (``TABLE_STRETCH``, ``TABLE_WIDTHS``): every column in sight; not once the
        reader has resized one."""
        if _table_widths or self.view != 'table' or not self.table.rowCount():
            return
        table, cols = self.table, self.table_columns()
        self._sizing = True
        try:
            room = table.viewport().width() - (table.columnWidth(0) if self.dialog.multi else 0) - 2
            natural = {}
            for key, col in cols.items():
                if key in TABLE_STRETCH:
                    continue
                if key in TABLE_WIDTHS:
                    natural[key] = TABLE_WIDTHS[key]
                else:
                    table.resizeColumnToContents(col)
                    natural[key] = table.columnWidth(col)
            left = room - sum(natural.values())
            if left >= 2 * TABLE_MIN_WIDTH:
                title = max(TABLE_MIN_WIDTH, int(left * TABLE_TITLE_SHARE))
                widths = {**natural, 'title': title, 'snippet': left - title}
            else:
                # a narrow pane: the title and the snippet keep two fifths of it, the others shrink in
                # proportion to the rest
                rest = max(1, room - max(2 * TABLE_FLOOR, int(room * TABLE_TITLE_SHARE)))
                scale = min(1.0, rest / max(1, sum(natural.values())))
                widths = {key: max(TABLE_FLOOR, int(w * scale)) for key, w in natural.items()}
                spare = max(2 * TABLE_FLOOR, room - sum(widths.values()))
                title = max(TABLE_FLOOR, int(spare * TABLE_TITLE_SHARE))
                widths.update(title=title, snippet=max(TABLE_FLOOR, spare - title))
            for key, width in widths.items():
                table.setColumnWidth(cols[key], width)
        finally:
            self._sizing = False

    def _column_resized(self, _index: int, _old: int, _new: int) -> None:
        """The reader resized a column: every width as it is now kept for the run."""
        if self._sizing or self.view != 'table' or not self.table.rowCount():
            return
        for key, col in self.table_columns().items():
            _table_widths[key] = self.table.columnWidth(col)

    def _fill_row(self, r: int, row: dict, det: Optional[dict]) -> None:
        """A table row's cells from ``row_details`` (blank while not known): the shelfmark's join sign
        and tooltip, the title, the snippet, the domains, the pages, and the search table's badges for
        printed, PGP and edition (until the details come, PGP and edition are the row's signs)."""
        table = self.table
        cols = self.table_columns()
        det = det or {}
        leaf = row['leaf']
        marks = _signs_of(leaf.sys_id)
        name = table.item(r, cols['shelfmark'])
        name.setToolTip(_with_signs(leaf_tooltip(row) or row['label'], marks))
        name.setData(_SIGNS_ROLE, ('join',) if 'join' in marks else None)
        rtl = self.isRightToLeft()

        def put(key: str, text: str, tip: str = '', colour: Optional[str] = None, bold: bool = False,
                align: Optional[Qt.AlignmentFlag] = None) -> None:
            cell = table.item(r, cols[key])
            cell.setText(text)
            cell.setToolTip(tip)
            if colour:
                cell.setForeground(QColor(colour))
            if bold:
                font = cell.font()
                font.setBold(True)
                cell.setFont(font)
            if align is not None:
                cell.setTextAlignment(align | Qt.AlignmentFlag.AlignVCenter)

        title = det.get('title') or ''
        put('title', own_direction(title, rtl), title)
        snippet = det.get('snippet') or ''
        put('snippet', snippet, snippet)
        domains = det.get('domains') or []
        put('domain', ', '.join(domains), ', '.join(domains), TABLE_MARKS['domain'][1])
        pages = det.get('pages')
        put('pages', '' if pages is None else f"{pages:,}", align=Qt.AlignmentFlag.AlignTrailing)
        center = Qt.AlignmentFlag.AlignHCenter
        if det.get('printed'):
            from shared.fjms_service import PRINTED_LABEL_EN, PRINTED_LABEL_HE
            put('printed', PRINTED_LABEL_HE if self.dialog._lang == 'he' else PRINTED_LABEL_EN,
                tr(HEADER_TIPS['printed']), TABLE_MARKS['printed'][1], align=center)
        else:
            put('printed', '', align=center)
        pgp = det.get('pgp') if det.get('pgp') is not None else ('pgp' in marks)
        put('pgp', TABLE_MARKS['pgp'][0] if pgp else '', tr(HEADER_TIPS['pgp']) if pgp else '',
            TABLE_MARKS['pgp'][1], bold=True, align=center)
        edition = det.get('edition') if det.get('edition') is not None else ('edition' in marks)
        put('edition', TABLE_MARKS['edition'][0] if edition else '', tr(HEADER_TIPS['edition']) if edition else '',
            TABLE_MARKS['edition'][1], bold=True, align=center)

    def fill_details(self, sys_ids) -> None:
        """The table's rows of these manuscripts filled with their details, now come."""
        if self.view != 'table' or self.leaves is None:
            return
        rows = self.leaves['rows']
        details = self.dialog._row_details
        for sid in sys_ids:
            for r in self.table_rows.get(sid, ()):
                if 0 <= r < len(rows) and r < self.table.rowCount():
                    self._fill_row(r, rows[r], details.get(sid))

    def picture_size(self) -> tuple:
        return card_sizes()[self.size]

    def fetch_width(self) -> int:
        """The width the tiles' pictures are asked for at: sharper ones for the large sizes."""
        return FETCH_WIDTHS.get(self.size, THUMB_WIDTH) if self.view == 'gallery' else THUMB_WIDTH

    def tile_size(self) -> QSize:
        width, height = self.picture_size()
        title = QFontMetrics(_title_font(self.font())).height() * TILE_TITLE_LINES
        return QSize(width + 16, height + self.fontMetrics().height() + title + 24)

    def _fill_gallery(self, leaves: dict, multi: bool, known: Optional[dict]) -> None:
        rtl = self.isRightToLeft()
        highlight = self.dialog._highlight
        gallery = self.gallery
        gallery.setUpdatesEnabled(False)
        gallery.clear()
        tile = self.tile_size()
        gallery.setGridSize(QSize(tile.width() + 6, tile.height() + 6))
        gallery.setIconSize(QSize(*self.picture_size()))
        for row in leaves['rows']:
            leaf = row['leaf']
            item = QListWidgetItem(own_direction(row['label'], rtl))
            item.setData(_ROW, leaf)
            item.setSizeHint(tile)
            item.setData(_TITLE_ROLE, row.get('title') or '')
            marks = _signs_of(leaf.sys_id)
            item.setToolTip(_with_signs(leaf_tooltip(row) or row['label'], marks))
            if marks:
                item.setData(_SIGNS_ROLE, marks)
            if multi:
                self._box(item, leaf.sys_id, known)
            else:
                item.setFlags(_BOX_FLAGS)
            self._put_picture(item, leaf.sys_id)
            if leaf.id == highlight:
                self.chosen = item
            gallery.addItem(item)
            self.tiles.setdefault(leaf.sys_id, []).append(item)
        if self.chosen is not None:
            gallery.setCurrentItem(self.chosen)
        gallery.setUpdatesEnabled(True)

    def _put_picture(self, item, sid: str) -> None:
        known, image = self.dialog.known_thumb(sid)
        if not known:                              # on its way; one that did not come: the dash, till it does
            item.setData(_THUMB_ROLE, False if sid in self.dialog._thumb_tries else None)
            return
        if image is None:
            item.setData(_THUMB_ROLE, False)
            return
        width, height = self.picture_size()
        item.setData(_THUMB_ROLE, QPixmap.fromImage(image).scaled(
            width, height, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))

    def queue_visible(self) -> None:
        """The tiles in sight (and a little past) whose pictures are not here yet, asked for: what was
        waiting and is no longer in sight is dropped."""
        if self.view != 'gallery' or not self.gallery.isVisible():
            return
        sight = self.gallery.viewport().rect().adjusted(0, -100, 0, 100)
        want = []
        for i in range(self.gallery.count()):
            item = self.gallery.item(i)
            leaf = item.data(_ROW)
            if (leaf is not None and self.gallery.visualItemRect(item).intersects(sight)
                    and not self.dialog.thumb_sharp(leaf.sys_id)):
                want.append(leaf.sys_id)
        self.dialog._thumb_pool.want(want)

    def show_picture(self, sid: str) -> None:
        for item in self.tiles.get(sid, ()):
            self._put_picture(item, sid)

    def show_signs(self) -> None:
        """The signs, now loaded, on the shelfmarks drawn before."""
        rows = (self.leaves or {}).get('rows') or []
        if self.view == 'table':
            for r, row in enumerate(rows[:self.table.rowCount()]):
                self._fill_row(r, row, self.dialog._row_details.get(row['leaf'].sys_id))
            return
        view = self.active()
        for i, row in enumerate(rows[:view.count()]):
            marks = _signs_of(row['leaf'].sys_id)
            if marks:
                item = view.item(i)
                item.setData(_SIGNS_ROLE, marks)
                item.setToolTip(_with_signs(leaf_tooltip(row) or row['label'], marks))

    def mark(self, leaf) -> None:
        """The shelfmark a click chose (single choice), shown chosen: the current item or row."""
        if self.view == 'table':
            for r in range(self.table.rowCount()):
                item = self.table.item(r, 0)
                if getattr(item.data(_ROW) if item is not None else None, 'id', None) == leaf.id:
                    # the current cell, which selects the row (rows are chosen whole): Enter and Space read
                    # it (current_leaf). Not selectRow: in a right-to-left table it looks for a column at
                    # the viewport's far edge, finds none where the columns end short of it, and leaves no
                    # row selected or current (owner, 2026-11-22: the line went after a click, Hebrew)
                    self.table.setCurrentCell(r, self.table_columns()['shelfmark'])
                    return
            return
        view = self.active()
        for i in range(view.count()):
            item = view.item(i)
            if getattr(item.data(_ROW), 'id', None) == leaf.id:
                view.setCurrentItem(item)
                return

    def line_height(self) -> int:
        """A one-line row of the node columns beside the grid, measured as their table measures it
        (the style's item size in a table, and the delegate's air): the shelfmarks' rows are as tall.
        A height of their own (two pixels more) made them look taller than the columns' (T-S C4), and
        a list's own measure is taller still in some styles (Windows 11's)."""
        table = self.dialog.node_columns[0].table
        option = QStyleOptionViewItem()
        table.initViewItemOption(option)
        option.font = self.grid.font()
        option.fontMetrics = self.grid.fontMetrics()
        option.text = 'Ag'
        option.features |= QStyleOptionViewItem.ViewItemFeature.HasDisplay
        if self.dialog.multi:                      # the box takes its height too
            option.features |= QStyleOptionViewItem.ViewItemFeature.HasCheckIndicator
        size = table.style().sizeFromContents(QStyle.ContentsType.CT_ItemViewItem, option, QSize(), table)
        return max(size.height(), self.grid.fontMetrics().height()) + _RowDelegate.PAD

    def flow_width(self) -> int:
        """The width the grid lays its shelfmarks out in, the same whether its scroll bar shows or
        not: QListView keeps a scroll bar's room whenever the bar may come (``prepareItemsLayout``).
        The columns were measured on the viewport, wider while the bar was hidden, so they were wider
        than the room Qt fills: with one column the first shelfmark did not fit beside nothing and
        went down a line (T-S C4's blank first line); with three, two fitted, the rows outgrew the
        view and the bar came, the viewport narrowed and three fitted again, the bar went... the
        shelfmarks jumped between the two layouts without end (Schoeyen)."""
        grid = self.grid
        width = grid.maximumViewportSize().width()
        if grid.verticalScrollBarPolicy() == Qt.ScrollBarPolicy.ScrollBarAsNeeded:
            style = grid.style()
            width -= style.pixelMetric(QStyle.PixelMetric.PM_ScrollBarExtent, None, grid.verticalScrollBar())
            if style.styleHint(QStyle.StyleHint.SH_ScrollView_FrameOnlyAroundContents, None, grid):
                option = QStyleOption()
                option.initFrom(grid)
                width -= 2 * style.pixelMetric(QStyle.PixelMetric.PM_DefaultFrameWidth, option, grid)
        return width

    def columns(self) -> int:
        """Shelfmark columns that fit: three, else two, else one."""
        return leaf_columns(self.flow_width() - 2, self._need)

    def fit(self) -> None:
        avail = max(1, self.flow_width() - 2)
        width = max(1, avail // self.columns())
        size = QSize(width, self._row_height)
        if self.grid.gridSize() != size:
            self.grid.setGridSize(size)

    def scroll_to(self, saved: Optional[int]) -> None:
        anchor, self._anchor = self._anchor, None
        if anchor is not None and self._bring_to_top(anchor):   # a view switch keeps the reader's place
            if self.view == 'gallery':
                self._thumb_timer.start()
            return
        view = self.active()
        if isinstance(view, QListWidget):
            view.doItemsLayout()
        if saved is not None:
            view.verticalScrollBar().setValue(saved)
        elif self.chosen is not None:
            if isinstance(self.chosen, int):
                cell = self.table.item(self.chosen, 0)
                if cell is not None:
                    self.table.scrollToItem(cell, QAbstractItemView.ScrollHint.PositionAtCenter)
            else:
                view.scrollToItem(self.chosen, QAbstractItemView.ScrollHint.PositionAtCenter)
        else:
            view.scrollToTop()
        if self.view == 'gallery':
            self._thumb_timer.start()


def _box_off(item: QTableWidgetItem, off: bool, tip: str, limit_tip: str) -> None:
    """A node's box, off (too big to tick whole, not ticked) or on, with the tooltip that says why."""
    flags = _BOX_FLAGS & ~Qt.ItemFlag.ItemIsEnabled if off else _BOX_FLAGS
    if item.flags() != flags:
        item.setFlags(flags)
    item.setToolTip(limit_tip if off else tip)


#: The answer of a picture's fetch that failed for now: asked for again later, never kept as "none".
NOT_COME = object()


def _thumbnail_image(sys_id: str, meta_mgr, width: int = THUMB_WIDTH):
    """The manuscript's picture (``_tile_url``), ``width`` pixels wide, as a QImage (made on the worker:
    never a QPixmap there); None: it has none; ``NOT_COME``: it did not come this time (NLI down or busy,
    its address not found). Asks NLI's image server through its circuit breaker, as the Join Workbench's
    thumbnails do (``desktop/join_workbench.py``)."""
    image, settled = _load_image(_tile_url(sys_id, meta_mgr, width))
    return image if settled else NOT_COME


def _library_of(sys_id: str) -> str:
    """The manuscript's own library code (``library_code_of``), '' when not known."""
    code_of = library_code_of
    try:
        return (code_of(sys_id) if code_of is not None else '') or ''
    except Exception:  # noqa: BLE001 -- not known
        return ''


def _tile_url(sys_id: str, meta_mgr, width: int = THUMB_WIDTH) -> str:
    """The address of a manuscript's tile: its catalogue thumbnail; a microfilm's middle image (the one its
    details and quick look open on), by its image list. '' when neither is found. BLOCKING: off the UI
    thread."""
    if is_microfilm(_library_of(sys_id)):
        pages = page_image_urls(sys_id, meta_mgr, width=width)
        if pages:
            return pages[_opening_of(sys_id, len(pages))]
    return thumbnail_url(sys_id, meta_mgr, width=width)


def _opening_of(sys_id: str, total: int) -> int:
    """The image a manuscript's details and quick look open on (``opening_image``: a microfilm's middle one),
    by its own library code (none: its first)."""
    return opening_image(_library_of(sys_id), total)


def _image_list(sys_id: str, meta) -> tuple:
    """A manuscript's images (NLI's, in order), its tile's address and the one it opens on. BLOCKING
    (the manifest): off the UI thread."""
    pages = page_image_urls(sys_id, meta)
    return pages, _tile_url(sys_id, meta), _opening_of(sys_id, len(pages))


def _image_at(url: str) -> Optional[QImage]:
    """An image by its address as a QImage (on the worker), or None; NLI's through its circuit breaker."""
    return _load_image(url)[0]


def _load_image(url: str) -> tuple:
    """``(QImage or None, settled)``: not settled when the answer is for now only (no address found, NLI down
    or busy, the network), so the image may come another time. One not there or refused (4xx,
    ``nli_refused``: NLI's server is up and will not give it) is a settled None."""
    if not url:
        return None, False
    from shared.nli_circuit_breaker import (
        NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT, is_open, record_failure, record_success,
    )
    from shared.nli_fetch import is_nli_host, nli_image_get, nli_refused
    nli = is_nli_host(url)
    if nli and is_open():                          # NLI down: no network call
        return None, False
    try:
        resp = nli_image_get(url, headers=genizah_core.Config.HTTP_HEADERS,
                             timeout=(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT))
    except Exception:  # noqa: BLE001 -- no image, never a crash
        if nli:
            record_failure(failure_type='connection_error', path='shelf_picker_thumb')
        return None, False
    if resp.status_code != 200:
        busy = (resp.status_code == 429 or 500 <= resp.status_code < 600) and not (nli and nli_refused(resp))
        if nli and busy:
            record_failure(failure_type='429' if resp.status_code == 429 else '5xx', path='shelf_picker_thumb')
        return None, not busy
    image = QImage()
    image.loadFromData(resp.content)
    if image.isNull():                             # a 200 that is no image (an error page): no success
        return None, True
    if nli:
        record_success(path='shelf_picker_thumb')
    return image, True


def _community_facts(sys_id: str, client) -> tuple:
    """The community's join partners of a manuscript (``(sys_id, shelfmark)``) and its comments count,
    through the window's corrections client; ``(partners, None)`` for what could not be read."""
    partners, comments = [], None
    try:
        found = client.get_connected_fragments_by_id(sys_id)
        for join in (getattr(found, 'joins', None) or ()):
            a, b = str(join.document_id_a or ''), str(join.document_id_b or '')
            if a == sys_id:
                partners.append((b, join.fragment_b or ''))
            elif b == sys_id:
                partners.append((a, join.fragment_a or ''))
    except Exception as exc:  # noqa: BLE001 -- the pane goes without them
        logger.debug("Shelf picker: no community joins for %s: %s", sys_id, exc)
    try:
        stats = client.get_document_stats(sys_id) or {}
        if 'comments_count' in stats:
            comments = int(stats['comments_count'] or 0)
    except Exception as exc:  # noqa: BLE001
        logger.debug("Shelf picker: no comments count for %s: %s", sys_id, exc)
    return partners, comments


def _direction_of(text: str) -> Qt.LayoutDirection:
    """A text's own direction, by its first strong letter."""
    first = next((d for d in map(unicodedata.bidirectional, text or '') if d in ("L", "R", "AL")), None)
    return Qt.LayoutDirection.RightToLeft if first in ("R", "AL") else Qt.LayoutDirection.LeftToRight


def _field_html(pairs, rtl: bool) -> str:
    """One line of labelled fields: each label bold, its value plain, the pairs " · " apart."""
    return ' · '.join(f"<b>{html.escape(label)}:</b> {html.escape(own_direction(str(value), rtl))}"
                      for label, value in pairs)


class _DetailsPane(QFrame):
    """DX25, round 4: the shelfmark clicked last, at the end side of the columns, as labelled fields:
    its image when NLI has one; the shelfmark, its other shelfmarks and its title; its text as stored
    (``details['text']``: each page under "Page N", in the text's own direction, in a scrolling box);
    the fields of ``shared.shelfmark_tree_details.field_lines``; its joins, one row a partner (a box
    that ticks it as the lists do, its shelfmark a link that goes to it in the columns, the sources
    small); and its links. The local part is read off the UI thread (``item_details``) and drawn
    first; then the community's joins and comments (fields and joins drawn again) and the image, each
    off the UI thread too; a newer click wins. All of it scrolls: nothing is cut when it is long."""

    WIDTH = 300
    IMAGE_HEIGHT = 220
    #: The text box's height, in lines.
    TEXT_LINES = 11

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog
        self.setFrameShape(QFrame.Shape.StyledPanel)
        self.setFixedWidth(self.WIDTH)
        self.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Expanding)
        self._gen = 0
        self.leaf_id: Optional[str] = None
        self.leaf = None                           # the shelfmark shown (a ShelfLeaf)
        self.pending = None                        # clicked while the reader hides the pane: shown later
        self.pages: list = []                      # the image addresses of the manuscript shown (its pages)
        self._thumb_page: Optional[int] = 0        # which of the pages the thumbnail on show is (None: none)
        self._page_asked: set = set()              # the pages' addresses asked for and not come yet
        self._page_waiting: Optional[str] = None   # the one of them waiting behind a running load
        self.page = 0                              # the one on show (0: the thumbnail, the first)
        self._page_images: "OrderedDict[str, QImage]" = OrderedDict()   # the pages fetched, this pane's
        self.text: Optional[dict] = None           # item_details' answer for the shelfmark shown
        self.join_boxes: dict = {}                 # sys_id -> a join partner's box
        self.join_links: dict = {}                 # sys_id -> a join partner's link
        self._places: dict = {}                    # sys_id -> its first place's ShelfLeaf, or None
        self._text_loads = _Loads(self._text_done, self)
        self._community_loads = _Loads(self._community_done, self)
        self._pages_loads = _Loads(self._pages_done, self)        # the image list (a manifest)
        self._sharp_loads = _Loads(self._sharp_done, self)        # a zoomed picture's sharper copy
        self._sharp: "OrderedDict[tuple, QImage]" = OrderedDict()
        self._sharp_asked: set = set()
        self._page_loads = _Loads(self._page_done, self)          # one page's image at a time
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        self.scroll = QScrollArea(self)
        self.scroll.setWidgetResizable(True)
        self.scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        body = QWidget(self.scroll)
        self.scroll.setWidget(body)
        outer.addWidget(self.scroll)
        layout = QVBoxLayout(body)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(6)
        self.empty = QLabel(tr("Choose a shelfmark to see it here"), body)
        self.empty.setWordWrap(True)
        self.image = _ZoomImage(self.WIDTH - 40, self.IMAGE_HEIGHT, body)
        self.image.setVisible(False)
        self.image.zoomed.connect(self._zoomed)
        # under the picture: zoom out / in, and the manuscript's other images (previous / next and where
        # it is, "2 of 6", shown when it has more than one: the list comes after the text, off the UI thread)
        self.image_bar = QWidget(body)
        bar = QHBoxLayout(self.image_bar)
        bar.setContentsMargins(0, 0, 0, 0)
        bar.setSpacing(2)
        self.page_nav = QWidget(self.image_bar)
        nav = QHBoxLayout(self.page_nav)
        nav.setContentsMargins(0, 0, 0, 0)
        nav.setSpacing(4)
        self.page_prev = QToolButton(self.page_nav)
        self.page_next = QToolButton(self.page_nav)
        for button, step, tip, name in ((self.page_prev, -1, 'Previous image', 'shelfPagePrev'),
                                        (self.page_next, 1, 'Next image', 'shelfPageNext')):
            # the style's own arrow, never a character: a text arrow (‹ ›) is a Unicode mirrored pair
            # and came out pointing the same way twice in Hebrew; the button itself is left-to-right, so
            # the arrow is the one asked for (see _draw_nav)
            button.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
            button.setAutoRaise(True)
            button.setToolTip(tr(tip))
            button.setAccessibleName(tr(tip))
            button.setObjectName(name)
            button.clicked.connect(lambda _c=False, d=step: self.step_page(d))
        self.page_label = QLabel(self.page_nav)
        self.page_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        nav.addWidget(self.page_prev)
        nav.addWidget(self.page_label)
        nav.addWidget(self.page_next)
        self.page_nav.setVisible(False)
        self.rotate_left = QToolButton(self.image_bar)
        self.rotate_right = QToolButton(self.image_bar)
        for button, text, degrees, tip, name in ((self.rotate_left, '\u27f2', -90, 'Rotate left', 'shelfRotateLeft'),
                                                 (self.rotate_right, '\u27f3', 90, 'Rotate right', 'shelfRotateRight')):
            button.setText(text)
            button.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
            button.setAutoRaise(True)
            button.setToolTip(key_tip(tip, 'Shift+R' if degrees < 0 else 'R'))
            button.setAccessibleName(tr(tip))
            button.setObjectName(name)
            button.clicked.connect(lambda _c=False, d=degrees: self.image.rotate_by(d))
        self.zoom_out = QToolButton(self.image_bar)
        self.zoom_in = QToolButton(self.image_bar)
        for button, text, factor, tip, name in ((self.zoom_out, '\u2212', 1 / _ZoomImage.STEP, 'Zoom out', 'shelfZoomOut'),
                                                (self.zoom_in, '+', _ZoomImage.STEP, 'Zoom in', 'shelfZoomIn')):
            button.setText(text)
            button.setAutoRaise(True)
            button.setToolTip(key_tip(tip, '+' if factor > 1 else '-'))
            button.setAccessibleName(tr(tip))
            button.setObjectName(name)
            button.clicked.connect(lambda _c=False, f=factor: self.image.zoom_by(f))
        bar.addStretch(1)
        bar.addWidget(self.page_nav)
        # the quick look: this shelfmark filling the picker, at this page and turn (Q from a list)
        self.look_button = QToolButton(self.image_bar)
        self.look_button.setText('\u26f6')
        self.look_button.setAutoRaise(True)
        self.look_button.setToolTip(f"{tr('Quick look')} (Q)")
        self.look_button.setAccessibleName(tr('Quick look'))
        self.look_button.setObjectName('shelfQuickLookButton')
        self.look_button.clicked.connect(lambda _c=False: self.dialog.quick_look(self.leaf, self))
        bar.addStretch(1)
        bar.addWidget(self.rotate_left)
        bar.addWidget(self.rotate_right)
        bar.addWidget(self.zoom_out)
        bar.addWidget(self.zoom_in)
        bar.addWidget(self.look_button)
        self.image_bar.setVisible(False)
        self.shelfmark = QLabel(body)
        bold = self.shelfmark.font()
        bold.setBold(True)
        bold.setPointSizeF(bold.pointSizeF() * 1.1)
        self.shelfmark.setFont(bold)
        self.shelfmark.setWordWrap(True)
        self.shelfmark.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.shelfmark.setVisible(False)
        self.fields = {name: self._rich_label(body) for name in ('others', 'title')}
        # the text as stored, read-only, scrolling: about eleven lines high
        self.textbox = QTextEdit(body)
        self.textbox.setReadOnly(True)
        self.textbox.setLineWrapMode(QTextEdit.LineWrapMode.WidgetWidth)
        self.textbox.setFixedHeight(self.textbox.fontMetrics().lineSpacing() * self.TEXT_LINES
                                    + 2 * self.textbox.frameWidth()
                                    + 2 * int(self.textbox.document().documentMargin()))
        self.textbox.setVisible(False)
        self.lines_box = QWidget(body)
        self.lines = QVBoxLayout(self.lines_box)
        self.lines.setContentsMargins(0, 0, 0, 0)
        self.lines.setSpacing(2)
        self.lines_box.setVisible(False)
        self.joins_title = self._rich_label(body)
        self.joins_title.setText(f"<b>{html.escape(tr('Joins'))}:</b>")
        # the partners' boxes all at once: a box before "Joins:", ticked when every partner on the shelf is
        # chosen, partly when some are; a click ticks them all (with the shelfmark shown), or, when all
        # are, unticks them (the shelfmark shown stays as it is)
        self.joins_head = QWidget(body)
        head = QHBoxLayout(self.joins_head)
        head.setContentsMargins(0, 0, 0, 0)
        head.setSpacing(6)
        self.joins_all = _AllBox(self.joins_head)
        self.joins_all.setToolTip(tr('Choose all the joins'))
        self.joins_all.setAccessibleName(tr('Choose all the joins'))
        self.joins_all.setObjectName('shelfJoinsAll')
        self.joins_all.clicked.connect(self._all_clicked)
        self.joins_all.setVisible(False)
        head.addWidget(self.joins_all)
        head.addWidget(self.joins_title)
        head.addStretch(1)
        self.joins_head.setVisible(False)
        self.joins_box = QWidget(body)
        self.joins_rows = QVBoxLayout(self.joins_box)
        self.joins_rows.setContentsMargins(0, 0, 0, 0)
        self.joins_rows.setSpacing(2)
        self.joins_box.setVisible(False)
        self.links = self._rich_label(body)
        self.links.setOpenExternalLinks(True)
        for widget in (self.empty, self.image, self.image_bar, self.shelfmark, self.fields['others'], self.fields['title'],
                       self.textbox, self.lines_box, self.joins_head, self.joins_box, self.links):
            layout.addWidget(widget)
        layout.addStretch(1)

    @staticmethod
    def _rich_label(parent) -> QLabel:
        label = QLabel(parent)
        label.setWordWrap(True)
        label.setTextFormat(Qt.TextFormat.RichText)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse
                                      | Qt.TextInteractionFlag.LinksAccessibleByMouse
                                      | Qt.TextInteractionFlag.LinksAccessibleByKeyboard)
        label.setVisible(False)
        return label

    def show_leaf(self, leaf) -> None:
        """Show a shelfmark: its name at once, the rest when read. Hidden by the reader, nothing is read
        (no network call): the shelfmark waits until the pane is shown again."""
        if leaf is None:
            return
        button = getattr(self.dialog, 'details_button', None)
        if button is not None and not button.isChecked():
            self.pending = leaf
            return
        self.pending = None
        self._gen += 1
        gen = self._gen
        self.leaf_id = leaf.id
        self.leaf = leaf
        self.text = None
        self.empty.setVisible(False)
        self.image.clear()
        self.image.setVisible(False)
        self.image_bar.setVisible(False)
        self._no_pages()
        known, image = self.dialog.known_thumb(leaf.sys_id)
        if known and image is not None:            # fetched already (the gallery, an earlier click): at once
            self._show_image(image)
        self.shelfmark.setText(own_direction(leaf.label or leaf.shelfmark or leaf.sys_id, self.isRightToLeft()))
        self.shelfmark.setVisible(True)
        for label in (*self.fields.values(), self.links, self.joins_title):
            label.setVisible(False)
        self.textbox.clear()
        self.textbox.setVisible(False)
        self._clear(self.lines)
        self.lines_box.setVisible(False)
        self._clear(self.joins_rows)
        self.joins_box.setVisible(False)
        self.join_boxes, self.join_links = {}, {}
        self.scroll.verticalScrollBar().setValue(0)
        service, lang = self.dialog._service, self.dialog._lang
        meta, pages_of, texts_of = catalogue, page_count_of, page_texts_of
        self._text_loads.request(gen, lambda: item_details(service, leaf, lang, meta_mgr=meta, page_count=pages_of,
                                                           page_texts=texts_of))

    def clear(self) -> None:
        """No shelfmark: the hint again, as at the opening. What is still being read for the one that
        was shown is not drawn (a newer generation)."""
        self._gen += 1
        self.leaf_id, self.leaf, self.text = None, None, None
        self.pending = None
        self.image.clear()
        self.image.setVisible(False)
        self.image_bar.setVisible(False)
        self._no_pages()
        self.shelfmark.clear()
        self.shelfmark.setVisible(False)
        for label in (*self.fields.values(), self.links, self.joins_title):
            label.setVisible(False)
        self.textbox.clear()
        self.textbox.setVisible(False)
        self._clear(self.lines)
        self.lines_box.setVisible(False)
        self._clear(self.joins_rows)
        self.joins_box.setVisible(False)
        self.join_boxes, self.join_links = {}, {}
        self.empty.setVisible(True)

    def _text_done(self, gen: int, out) -> None:
        if gen != self._gen or isinstance(out, Exception) or not isinstance(out, dict):
            return
        self.text = out
        rtl = self.isRightToLeft()
        name = out['shelfmark'] + (f" ({tr('Part: {part}').format(part=out['part'])})" if out.get('part') else '')
        self.shelfmark.setText(own_direction(name, rtl))
        if out.get('others'):
            self._put('others', [(tr('Other shelfmarks'), ', '.join(out['others']))])
        if out.get('title'):
            self._put('title', [(tr('Title'), out['title'])])
        self._draw_text(out.get('text') or [])
        self._draw_lines()
        self._draw_joins()
        links = [link for link in out.get('links') or () if link.get('url')]
        if links:
            anchors = ' · '.join(
                f'<a href="{html.escape(link["url"], quote=True)}">{html.escape(tr(link.get("label") or link["url"]))}</a>'
                for link in links)
            self.links.setText(f"<b>{html.escape(tr('Links'))}:</b> {anchors}")
            self.links.setVisible(True)
        meta, sid, client = catalogue, out.get('sys_id'), community
        if client is not None and sid:
            self._community_loads.request(gen, lambda: _community_facts(sid, client))
        # asked of the picker's one loader (shared with the gallery: never fetched twice at once); its
        # answer comes back through the dialog (_thumb_done -> show_known_image)
        if meta is not None and sid and not self.dialog.known_thumb(sid)[0]:
            self.dialog._thumb_pool.fetch(sid)
        if meta is not None and sid:
            self._pages_loads.request(gen, lambda: _image_list(sid, meta))

    def _put(self, name: str, pairs: list) -> None:
        label = self.fields[name]
        label.setText(_field_html(pairs, self.isRightToLeft()))
        label.setVisible(True)

    def _draw_text(self, pages: list) -> None:
        """Each page's text as stored (no line folded or cut), under a small "Page N", in its own
        direction; the box scrolls."""
        box = self.textbox
        box.clear()
        if not pages:
            box.setVisible(False)
            return
        head = QTextCharFormat()
        head.setForeground(self.palette().color(QPalette.ColorRole.PlaceholderText))
        if box.font().pointSizeF() > 0:
            head.setFontPointSize(box.font().pointSizeF() * 0.85)
        plain = QTextCharFormat()
        ui = Qt.LayoutDirection.RightToLeft if self.isRightToLeft() else Qt.LayoutDirection.LeftToRight
        cursor = QTextCursor(box.document())
        for i, page in enumerate(pages):
            heading = QTextBlockFormat()
            heading.setLayoutDirection(ui)
            if i:
                heading.setTopMargin(8)
                cursor.insertBlock(heading)
            else:
                cursor.setBlockFormat(heading)
            cursor.insertText(tr('Page {n}').format(n=page.get('page')), head)
            body = QTextBlockFormat()
            body.setLayoutDirection(_direction_of(page.get('text') or ''))
            cursor.insertBlock(body, plain)
            cursor.insertText(page.get('text') or '', plain)
        box.moveCursor(QTextCursor.MoveOperation.Start)
        box.setVisible(True)

    def _draw_lines(self) -> None:
        """The labelled fields (``field_lines``), drawn again when the community's answer comes."""
        self._clear(self.lines)
        rtl = self.isRightToLeft()
        for key, pairs in field_lines(self.text or {}, tr):
            label = self._rich_label(self.lines_box)
            label.setObjectName(f"field_{key}")
            label.setText(_field_html(pairs, rtl))
            label.setVisible(True)
            self.lines.addWidget(label)
        self.lines_box.setVisible(self.lines.count() > 0)

    def place_leaf(self, sys_id: str):
        """A manuscript's first place on the shelf (its ``ShelfLeaf``), or None; read once."""
        if not sys_id:
            return None
        if sys_id not in self._places:
            try:
                placements = self.dialog._service.path_of(sys_id)
            except Exception as exc:  # noqa: BLE001 -- no place: plain text
                logger.debug("Shelf picker: no place for %s: %s", sys_id, exc)
                placements = []
            self._places[sys_id] = placements[0].leaf if placements else None
        return self._places[sys_id]

    def _chosen(self) -> frozenset:
        state = self.dialog._paint_state if self.dialog.multi else None
        return frozenset((state or {}).get('chosen') or ())

    def _draw_joins(self) -> None:
        """One row a partner: a box (where the picker can choose several) that ticks it as the lists'
        box does, its shelfmark a link that goes to it in the columns, its sources small and grey. A
        partner without a sys_id or a place on the shelf is plain text."""
        self._clear(self.joins_rows)
        self.join_boxes, self.join_links = {}, {}
        joins = (self.text or {}).get('joins') or []
        chosen = self._chosen()
        rtl = self.isRightToLeft()
        for join in joins:
            sid = join.get('sys_id') or ''
            name = join.get('shelfmark') or sid
            placed = self.place_leaf(sid) is not None
            row = QWidget(self.joins_box)
            line = QHBoxLayout(row)
            line.setContentsMargins(0, 0, 0, 0)
            line.setSpacing(6)
            if placed and self.dialog._can_multi:
                box = QCheckBox(row)
                box.setToolTip(name)
                box.setAccessibleName(name)
                box.setChecked(sid in chosen)
                box.clicked.connect(lambda checked, s=sid: self.dialog._tick_partner(s, bool(checked)))
                self.join_boxes[sid] = box
                line.addWidget(box)
            link = QLabel(row)
            link.setWordWrap(True)
            if placed:
                link.setTextFormat(Qt.TextFormat.RichText)
                link.setText(f'<a href="{html.escape(sid, quote=True)}">{html.escape(own_direction(name, rtl))}</a>')
                link.setTextInteractionFlags(Qt.TextInteractionFlag.LinksAccessibleByMouse
                                             | Qt.TextInteractionFlag.LinksAccessibleByKeyboard)
                link.linkActivated.connect(lambda _href, s=sid: self.dialog.reveal(s))
                self.join_links[sid] = link
            else:
                link.setText(own_direction(name, rtl))
            line.addWidget(link)
            sources = QLabel(' · '.join(tr(_JOIN_SOURCE_NAMES.get(x, x)) for x in join.get('sources') or ()), row)
            small = sources.font()
            if small.pointSizeF() > 0:
                small.setPointSizeF(small.pointSizeF() * 0.85)
            sources.setFont(small)
            sources.setStyleSheet("color: palette(placeholder-text);")
            line.addWidget(sources)
            line.addStretch(1)
            self.joins_rows.addWidget(row)
        self.joins_title.setVisible(bool(joins))
        self.joins_head.setVisible(bool(joins))
        self.joins_all.setVisible(len(self.join_boxes) > 1)
        self._sync_all()
        self.joins_box.setVisible(bool(joins))

    def paint(self, state: Optional[dict]) -> None:
        """The partners' boxes, as ``state`` (``paint_state``) has the choice; None: none ticked (the
        single choice shows no choice)."""
        chosen = frozenset((state or {}).get('chosen') or ())
        for sid, box in self.join_boxes.items():
            self.set_box(sid, sid in chosen)

    def set_box(self, sys_id: str, on: bool) -> None:
        box = self.join_boxes.get(sys_id)
        if box is not None and box.isChecked() != on:
            box.blockSignals(True)
            box.setChecked(on)
            box.blockSignals(False)
        self._sync_all()

    def _sync_all(self) -> None:
        """The box before "Joins:" as the partners' boxes are: all ticked, some, or none."""
        ticked = [box.isChecked() for box in self.join_boxes.values()]
        state = (Qt.CheckState.Checked if ticked and all(ticked)
                 else Qt.CheckState.PartiallyChecked if any(ticked) else Qt.CheckState.Unchecked)
        if self.joins_all.checkState() != state:
            self.joins_all.setCheckState(state)
        self.joins_all.setToolTip(tr('Untick all the joins') if state == Qt.CheckState.Checked
                                  else tr('Choose all the joins'))

    def _all_clicked(self) -> None:
        """A click on the box before "Joins:": all the partners chosen, or none when all were."""
        self.dialog._tick_partners(self.joins_all.checkState() != Qt.CheckState.Checked)
        self._sync_all()

    def _community_done(self, gen: int, out) -> None:
        """The community's join partners and comments count: added, the fields and joins drawn again."""
        if gen != self._gen or self.text is None or not isinstance(out, tuple):
            return
        partners, comments = out
        add_joins(self.text, partners, 'community')
        if comments is not None:
            self.text['comments'] = comments
        self._draw_lines()
        self._draw_joins()

    @staticmethod
    def _clear(layout) -> None:
        while layout.count():
            item = layout.takeAt(0)
            widget = item.widget() if item is not None else None
            if widget is not None:
                widget.setParent(None)
                widget.deleteLater()

    def show_known_image(self) -> None:
        """The picture of the shelfmark shown, now that it has been fetched."""
        known, image = self.dialog.known_thumb(self.leaf.sys_id) if self.leaf is not None else (False, None)
        if known and image is not None and self.page == self._thumb_page and self.image.pixmap().isNull():
            self._show_image(image)

    def _show_image(self, image: QImage) -> None:
        """A page's picture, fitted (its sharper copy instead when one was fetched for it)."""
        sharp = self._sharp.get(self._sharp_key())
        self.image.set_image(sharp if sharp is not None else image)
        self.image.setVisible(True)
        self.image_bar.setVisible(True)

    # -- zoom: a sharper copy of the picture on show, asked for once it is zoomed in -------------------

    #: The width the sharper copy is asked for at (NLI's image server), and how many a pane keeps.
    SHARP_WIDTH = 1200
    SHARP_CACHE = 12

    def _sharp_key(self) -> Optional[tuple]:
        return (self.leaf.sys_id, self.page) if self.leaf is not None else None

    def _zoomed(self, zoom: float) -> None:
        key = self._sharp_key()
        if zoom <= 1.0 or key is None or key in self._sharp or key in self._sharp_asked:
            return
        sid, page, meta = key[0], key[1], catalogue
        if self.pages and 0 <= page < len(self.pages):
            url = sized_image_url(self.pages[page], self.SHARP_WIDTH)
            fetch = (lambda k=key, u=url: (k, _image_at(u)))
        elif page == self._thumb_page and meta is not None:
            fetch = (lambda k=key, s=sid, m=meta: (k, _image_at(_tile_url(s, m, width=self.SHARP_WIDTH))))
        else:
            return
        self._sharp_asked.add(key)
        self._sharp_loads.request(self._gen, fetch)

    def _sharp_done(self, gen: int, out) -> None:
        if not isinstance(out, tuple):
            return
        key, image = out
        self._sharp_asked.discard(key)
        if gen != self._gen or not isinstance(image, QImage) or image.isNull():
            return
        self._sharp[key] = image
        self._sharp.move_to_end(key)
        while len(self._sharp) > self.SHARP_CACHE:
            self._sharp.popitem(last=False)
        if key == self._sharp_key() and self.image.isVisible():
            self.image.set_image(image, keep_zoom=True)

    # -- the manuscript's other images ----------------------------------------------------------

    #: The pages' images kept by a pane (the newest; a manuscript's pages seen again come at once).
    PAGE_CACHE = 40

    def _no_pages(self) -> None:
        self.pages, self.page = [], 0
        self._thumb_page: Optional[int] = 0        # which of the pages the thumbnail on show is (None: none)
        self._page_asked = set()                   # the pages' addresses asked for and not come yet
        self._page_waiting = None
        self.page_nav.setVisible(False)

    def _pages_done(self, gen: int, out) -> None:
        """The manuscript's image list: the arrows when it has more than one, the ones beside fetched.
        The thumbnail on show is the catalogue record's first image (MARC 907), which need not be the
        list's first (the manifest's, in image-id order, as Browse has it): the pane goes to the list's
        first image (owner, 2026-11-25: it opened on the second), the thumbnail shown until it comes; the
        count and the steps are the list's."""
        if gen != self._gen or not isinstance(out, tuple) or len(out) != 3:
            return
        pages, thumb, start = out
        if not isinstance(pages, list) or len(pages) < 2:
            return
        self.pages = pages
        at = image_index(pages, thumb)
        self._thumb_page = at
        self.page = start if 0 <= start < len(pages) else 0     # a microfilm: its middle (opening_image)
        self._draw_nav()
        if at != self.page:
            self._ask_page(self.page)              # shown when it comes, in the thumbnail's place
        self._prefetch(self.page + 1)

    def _draw_nav(self) -> None:
        total = len(self.pages)
        rtl = self.isRightToLeft()
        # each arrow points away from the label, the way its pages go, as Browse's previous / next
        # manuscript do: in Hebrew previous is on the right pointing right, next on the left pointing left
        right, left = Qt.ArrowType.RightArrow, Qt.ArrowType.LeftArrow
        self.page_prev.setArrowType(right if rtl else left)
        self.page_next.setArrowType(left if rtl else right)
        back, forward = arrow_keys(rtl)
        self.page_prev.setToolTip(key_tip('Previous image', f'Shift+{back}'))
        self.page_next.setToolTip(key_tip('Next image', f'Shift+{forward}'))
        self.page_prev.setEnabled(self.page > 0)
        self.page_next.setEnabled(self.page < total - 1)
        self.page_label.setText(tr('Image {n} of {total}').format(n=self.page + 1, total=total))
        self.page_nav.setVisible(total > 1)

    def step_page(self, step: int) -> None:
        """Previous / next image of the manuscript shown: at once when fetched already, else when it
        comes (the newest step wins); the one after it is fetched meanwhile."""
        if not self.pages:
            return
        page = max(0, min(len(self.pages) - 1, self.page + step))
        if page == self.page:
            return
        self.page = page
        self._draw_nav()
        image = self._page_image(page)
        if image is not None:
            self._show_image(image)
            self._prefetch(page + step)
        else:
            self._ask_page(page)                   # shown when it comes (one under way already: that one)

    def _page_image(self, page: int) -> Optional[QImage]:
        if page == self._thumb_page and self.leaf is not None:
            known, image = self.dialog.known_thumb(self.leaf.sys_id)
            if known and image is not None:
                return image
        image = self._page_images.get(self.pages[page]) if 0 <= page < len(self.pages) else None
        if image is not None:
            self._page_images.move_to_end(self.pages[page])
        return image

    def _prefetch(self, page: int) -> None:
        """The image after the one shown, fetched before it is asked for (one at a time: a step asked
        meanwhile replaces it while it waits)."""
        if 0 <= page < len(self.pages) and page != self.page and self._page_image(page) is None:
            self._ask_page(page)

    def _ask_page(self, page: int) -> None:
        url = self.pages[page]
        if url in self._page_asked:
            return
        if self._page_loads.busy():                # it waits, replacing the one waiting: that one is not asked now
            if self._page_waiting is not None:
                self._page_asked.discard(self._page_waiting)
            self._page_waiting = url
        self._page_asked.add(url)
        self._page_loads.request(self._gen, lambda p=page, u=url: (p, u, _image_at(u)))

    def _page_done(self, gen: int, out) -> None:
        if gen != self._gen or not isinstance(out, tuple):
            return
        page, url, image = out
        self._page_asked.discard(url)
        if url == self._page_waiting:
            self._page_waiting = None
        if isinstance(image, QImage) and not image.isNull():
            self._page_images[url] = image
            self._page_images.move_to_end(url)
            while len(self._page_images) > self.PAGE_CACHE:
                self._page_images.popitem(last=False)
        if page != self.page:
            return
        if image is None:
            self.image.setText(tr('No image'))
            self.image.setVisible(True)
            self.image_bar.setVisible(True)        # the arrows stay: the next page may have one
            return
        self._show_image(image)
        self._prefetch(page + 1)

    def stop(self) -> None:
        # the text is local reads of the tree and the sidecars: waited for, as the lists' loads are (the
        # tree must not be read after its dialog has gone); the image is not, it may take seconds to
        # come over the network (Codex pre-flight: never freeze the close) and it reads no tree
        self._text_loads.stop()
        self._community_loads.stop(wait_ms=0)
        self._pages_loads.stop(wait_ms=0)
        self._page_loads.stop(wait_ms=0)
        self._sharp_loads.stop(wait_ms=0)


# -- the quick look ---------------------------------------------------------------------------

class _QuickLook(QFrame):
    """One shelfmark's picture filling the picker (owner, 2026-11-25), over the lists, which stay where
    they are under it. Opened by Q from a list or the pane's button; Esc (or Q, or its close button) goes
    back to the list, at the shelfmark looked at last.

    The picture shown at once is the one at hand (the tile's, or the pane's page), then the same image
    sized to the window (``look_width``: about 800-1,600 pixels wide, in steps, never every tile's
    size), and once zoomed in a sharper copy (``look_sharp_width``). The arrows go to the previous / next
    shelfmark of the list (mirrored in Hebrew), Ctrl + an arrow to the manuscript's previous / next image;
    the next image and the next shelfmark's are fetched before they are asked for. + / - / 0 zoom, R /
    Shift+R turn a quarter, Space chooses (several at once: ticks), Enter opens, as in the list."""

    CACHE = 30                                     # the pictures kept (the newest), by image and width

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog
        self.setObjectName('shelfQuickLook')
        self.setFrameShape(QFrame.Shape.NoFrame)
        self.setAutoFillBackground(True)
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self._gen = 0
        self.leaf = None                           # the shelfmark shown (a ShelfLeaf)
        self.pages: list = []                      # its images' addresses (as page_image_urls has them)
        self.page = 0                              # the one shown
        self._thumb_page: Optional[int] = 0        # which of them the thumbnail is (None: none of them)
        self.width_asked = LOOK_MIN                # the width the picture shown is asked for at
        self.fit_width = LOOK_MIN                  # ... the width that fills its place (zoomed in: wider)
        self._images: "OrderedDict[tuple, QImage]" = OrderedDict()   # (image_key, width) -> picture
        self._sharp_asked: set = set()
        self._thumb_urls: dict = {}                # sys_id -> the address of the image it opens on (its first)
        self._urls_loads = _Loads(self._urls_done, self)      # a shelfmark's image list
        self._image_loads = _Loads(self._image_done, self)    # the picture wanted now
        self._ahead_loads = _Loads(self._ahead_done, self)    # the next one, before it is wanted
        self._width_timer = QTimer(self)
        self._width_timer.setSingleShot(True)
        self._width_timer.setInterval(300)
        self._width_timer.timeout.connect(self._grown)
        outer = QVBoxLayout(self)
        outer.setContentsMargins(8, 6, 8, 8)
        outer.setSpacing(4)
        bar = QHBoxLayout()
        bar.setSpacing(4)

        def tool(name: str, tip: str, on_click, text: str = '') -> QToolButton:
            button = QToolButton(self)
            if text:
                button.setText(text)
            button.setAutoRaise(True)
            button.setToolTip(tip)
            button.setAccessibleName(tip)
            button.setObjectName(name)
            button.setFocusPolicy(Qt.FocusPolicy.NoFocus)    # the keyboard stays with the quick look
            button.clicked.connect(lambda _c=False: on_click())
            return button

        # the shelfmark and where it is in the list, between its arrows; then the manuscript's images
        self.leaf_prev = tool('shelfLookPrev', tr('Previous shelfmark'), lambda: self.step_leaf(-1))
        self.leaf_next = tool('shelfLookNext', tr('Next shelfmark'), lambda: self.step_leaf(1))
        self._step_loads = _Loads(self._step_done, self)     # past the list's ends: the shelf's order
        self.title = QLabel(self)
        bold = self.title.font()
        bold.setBold(True)
        bold.setPointSizeF(bold.pointSizeF() * 1.15)
        self.title.setFont(bold)
        self.where = QLabel(self)
        self.page_prev = tool('shelfLookPagePrev', tr('Previous image'), lambda: self.step_page(-1))
        self.page_next = tool('shelfLookPageNext', tr('Next image'), lambda: self.step_page(1))
        self.page_label = QLabel(self)
        for button in (self.leaf_prev, self.leaf_next, self.page_prev, self.page_next):
            # the style's own arrows on left-to-right buttons, as the pane's (_DetailsPane._draw_nav)
            button.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        self.image = _ZoomImage(None, None, self)
        self.image.zoomed.connect(self._zoomed)
        self.rotate_left = tool('shelfLookRotateLeft', key_tip('Rotate left', 'Shift+R'), lambda: self.image.rotate_by(-90), '\u27f2')
        self.rotate_right = tool('shelfLookRotateRight', key_tip('Rotate right', 'R'), lambda: self.image.rotate_by(90), '\u27f3')
        self.zoom_out = tool('shelfLookZoomOut', key_tip('Zoom out', '-'), lambda: self.image.zoom_by(1 / _ZoomImage.STEP), '\u2212')
        self.zoom_in = tool('shelfLookZoomIn', key_tip('Zoom in', '+'), lambda: self.image.zoom_by(_ZoomImage.STEP), '+')
        for button in (self.rotate_left, self.rotate_right):
            button.setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        # several at once: the shelfmark's box; one: the picker's own button (Open, Add, ...)
        self.tick = QCheckBox(tr('Chosen'), self)
        self.tick.setObjectName('shelfLookTick')
        self.tick.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.tick.clicked.connect(lambda _c=False: self.choose())
        self.act = QPushButton(self)
        self.act.setObjectName('shelfLookAct')
        self.act.setAutoDefault(False)
        self.act.setFocusPolicy(Qt.FocusPolicy.NoFocus)
        self.act.clicked.connect(lambda _c=False: self.open_it())
        self.close_button = tool('shelfLookClose', key_tip('Back to the list', 'Esc'), self.close_look, '\u2715')
        bar.addWidget(self.leaf_prev)
        bar.addWidget(self.title)
        bar.addWidget(self.where)
        bar.addWidget(self.leaf_next)
        bar.addStretch(1)
        bar.addWidget(self.page_prev)
        bar.addWidget(self.page_label)
        bar.addWidget(self.page_next)
        bar.addStretch(1)
        for widget in (self.rotate_left, self.rotate_right, self.zoom_out, self.zoom_in, self.tick, self.act,
                       self.close_button):
            bar.addWidget(widget)
        outer.addLayout(bar)
        outer.addWidget(self.image, 1)
        self.hide()

    # -- opening and closing

    def open_on(self, leaf, pane: Optional["_DetailsPane"] = None) -> None:
        """Show ``leaf`` filling the picker; from the pane (``pane``, showing that shelfmark) at its page,
        its turn and its image list."""
        if leaf is None:
            return
        self.setGeometry(self.dialog.rect())
        self.show()
        self.raise_()
        self.layout().activate()                   # its picture's size known now, for the width asked
        self.setFocus(Qt.FocusReason.ShortcutFocusReason)
        same = pane is not None and pane.leaf is not None and pane.leaf.id == leaf.id
        self.show_leaf(leaf, pane if same else None)

    def close_look(self) -> None:
        if not self.isVisible():
            return
        leaf = self.leaf
        self.hide()
        self._gen += 1                             # what is still coming is not drawn
        self.dialog._look_closed(leaf)

    def stop(self) -> None:
        self._step_loads.stop()                    # the tree: waited for, as the lists' loads are
        self._urls_loads.stop(wait_ms=0)           # the network, never the tree: not waited for
        self._image_loads.stop(wait_ms=0)
        self._ahead_loads.stop(wait_ms=0)

    # -- the shelfmark shown

    def rows(self) -> list:
        """The shelfmarks of the list under the quick look, in its order (its rows drawn so far)."""
        leaves = self.dialog.leaf_panel.leaves or {}
        return [row['leaf'] for row in leaves.get('rows') or () if row.get('leaf') is not None]

    def _at(self) -> int:
        rows = self.rows()
        return next((i for i, lf in enumerate(rows) if self.leaf is not None and lf.id == self.leaf.id), -1)

    def show_leaf(self, leaf, pane: Optional["_DetailsPane"] = None) -> None:
        self._gen += 1
        gen = self._gen
        self.leaf = leaf
        self.pages, self.page, self._thumb_page = [], 0, 0
        self._sharp_asked = set()
        self.image.clear()
        self.title.setText(own_direction(leaf.label or leaf.shelfmark or leaf.sys_id, self.isRightToLeft()))
        self._draw_where()
        self._draw_nav()
        self.sync_choice()
        start = None
        if pane is not None and pane.pages:        # the pane's list and page: no new request
            self.pages, self.page, self._thumb_page = list(pane.pages), pane.page, pane._thumb_page
            self._draw_nav()
            source = pane.image._source
            if source is not None and not source.isNull():
                start = source
            if pane.image.rotation:
                self.image.rotation = pane.image.rotation
        if start is None:
            known, image = self.dialog.known_thumb(leaf.sys_id)
            start = self._kept(self._thumb_urls.get(leaf.sys_id, '')) or (image if known else None)
        if start is not None:
            self.image.set_image(start)
        else:
            self.image.setText(tr('Loading...'))
        if self.pages:
            self._want(self.page, 0)
            return
        meta, sid = catalogue, leaf.sys_id
        if meta is None:
            if start is None:
                self.image.setText(tr('No image'))
            return
        self._urls_loads.request(gen, lambda: _image_list(sid, meta))

    def _urls_done(self, gen: int, out) -> None:
        if gen != self._gen or not isinstance(out, tuple) or len(out) != 3:
            return
        pages, thumb, start = out
        pages = pages if isinstance(pages, list) else []
        start = start if 0 <= start < len(pages) else 0
        if (pages or thumb) and self.leaf is not None:
            self._thumb_urls[self.leaf.sys_id] = pages[start] if pages else thumb
        if pages:
            # the image the pane opens on (its first; a microfilm's middle): the thumbnail, shown until it
            # comes, may be another
            self.pages, self._thumb_page, self.page = pages, image_index(pages, thumb), start
        elif thumb:
            self.pages, self.page, self._thumb_page = [thumb], 0, 0
        else:
            if self.image.pixmap().isNull():
                self.image.setText(tr('No image'))
            return
        self._draw_nav()
        self._want(self.page, 0)

    def _draw_where(self) -> None:
        """Where it is: "12 of 48" in the list; past the list, the name of its group (a volume, a
        collection). The arrows go on past the list's ends, in the shelf's order."""
        rows, at = self.rows(), self._at()
        if at >= 0:
            self.where.setText(tr('{n} of {total}').format(n=f"{at + 1:,}", total=f"{len(rows):,}"))
        else:
            node = self.dialog._service.node(self.leaf.node_id) if self.leaf is not None else None
            self.where.setText(display_name(node, self.dialog._lang) if node is not None else '')
        rtl = self.isRightToLeft()
        right, left = Qt.ArrowType.RightArrow, Qt.ArrowType.LeftArrow
        self.leaf_prev.setArrowType(right if rtl else left)
        self.leaf_next.setArrowType(left if rtl else right)
        back, forward = arrow_keys(rtl)
        self.leaf_prev.setToolTip(key_tip('Previous shelfmark', back))
        self.leaf_next.setToolTip(key_tip('Next shelfmark', forward))
        self.leaf_prev.setEnabled(self.leaf is not None)
        self.leaf_next.setEnabled(self.leaf is not None)

    def _draw_nav(self) -> None:
        total = len(self.pages)
        rtl = self.isRightToLeft()
        right, left = Qt.ArrowType.RightArrow, Qt.ArrowType.LeftArrow
        self.page_prev.setArrowType(right if rtl else left)
        self.page_next.setArrowType(left if rtl else right)
        back, forward = arrow_keys(rtl)
        self.page_prev.setToolTip(key_tip('Previous image', f'Shift+{back}'))
        self.page_next.setToolTip(key_tip('Next image', f'Shift+{forward}'))
        self.page_prev.setEnabled(self.page > 0)
        self.page_next.setEnabled(self.page < total - 1)
        self.page_label.setText(tr('Image {n} of {total}').format(n=self.page + 1, total=total))
        for widget in (self.page_prev, self.page_label, self.page_next):
            widget.setVisible(total > 1)

    def sync_choice(self) -> None:
        """The box (several at once: ticked as the lists have it) or the picker's own button (one)."""
        dialog = self.dialog
        self.tick.setVisible(dialog.multi)
        act = getattr(dialog, 'act_button', None)
        self.act.setVisible(not dialog.multi and act is not None)
        if act is not None:
            self.act.setText(act.text())
        if dialog.multi and self.leaf is not None:
            chosen = frozenset((dialog._paint_state or {}).get('chosen') or ())
            self.tick.setChecked(self.leaf.sys_id in chosen)

    # -- moving

    def step_leaf(self, step: int) -> None:
        """The list's previous / next shelfmark; past its ends (or once outside it), the shelf's previous /
        next manuscript, into the next or previous group (owner, 2026-11-25)."""
        rows, at = self.rows(), self._at()
        to = at + step
        if at >= 0 and 0 <= to < len(rows):
            self.show_leaf(rows[to])
            return
        leaf, service = self.leaf, self.dialog._service
        if leaf is not None:
            self._step_loads.request(self._gen, lambda lf=leaf, s=step: (s, service.neighbours(lf.id)))

    def _step_done(self, gen: int, out) -> None:
        if gen != self._gen or not self.isVisible() or not isinstance(out, tuple) or len(out) != 2:
            return
        step, pair = out
        target = pair[1 if step > 0 else 0] if isinstance(pair, tuple) and len(pair) == 2 else None
        if target is not None:
            self.show_leaf(target)

    def go_leaf(self, index: int) -> None:
        rows = self.rows()
        if rows:
            self.show_leaf(rows[max(0, min(len(rows) - 1, index))])

    def step_page(self, step: int) -> None:
        page = max(0, min(len(self.pages) - 1, self.page + step))
        if not self.pages or page == self.page:
            return
        self.page = page
        self._draw_nav()
        self._sharp_asked = set()
        self.image.zoom = 1.0                      # another page opens fitted (it keeps the turn)
        self._want(page, step)

    # -- the pictures

    def _box_width(self) -> int:
        """The width that fills the picture's place (device pixels; a quarter turn swaps its sides)."""
        ratio = self.devicePixelRatioF() or 1.0
        w, h = self.image.width() * ratio, self.image.height() * ratio
        source = self.image._source
        aspect = source.width() / source.height() if source is not None and not source.isNull() and source.height() else 0.0
        if self.image.rotation % 180:
            w, h = h, w
        return look_width(w, h, aspect)

    def _kept(self, url: str, width: int = 0) -> Optional[QImage]:
        """A picture of ``url``'s image kept here: at ``width``, or (0) the widest kept."""
        key = image_key(url)
        if not key:
            return None
        if width:
            image = self._images.get((key, width))
            if image is not None:
                self._images.move_to_end((key, width))
            return image
        found = [(w, img) for (k, w), img in self._images.items() if k == key]
        return max(found, key=lambda pair: pair[0])[1] if found else None

    def _keep(self, url: str, width: int, image: QImage) -> None:
        key = (image_key(url), width)
        self._images[key] = image
        self._images.move_to_end(key)
        while len(self._images) > self.CACHE:
            self._images.popitem(last=False)

    def _page_url(self, page: int) -> str:
        return self.pages[page] if 0 <= page < len(self.pages) else ''

    def _want(self, page: int, step: int = 1) -> None:
        """The page's picture at the window's width: at once when kept, else asked for (the newest wins);
        meanwhile any picture of it at hand (a narrower one, the tile's)."""
        url = self._page_url(page)
        if not url:
            return
        width = self.width_asked = self.fit_width = self._box_width()
        image = self._kept(url, width)
        if image is not None:
            self.image.set_image(image, keep_zoom=self.image.zoom > 1.0)
            self._ahead(page + (step or 1))
            return
        at_hand = self._kept(url)
        if at_hand is None and page == self._thumb_page and self.leaf is not None:
            known, thumb = self.dialog.known_thumb(self.leaf.sys_id)
            at_hand = thumb if known else None
        if at_hand is not None:
            self.image.set_image(at_hand, keep_zoom=self.image.zoom > 1.0)
        elif self.image.pixmap().isNull() or step:
            self.image.setText(tr('Loading...'))
        sized = sized_image_url(url, width)
        self._image_loads.request(self._gen, lambda p=page, u=url, w=width, s=sized: (p, u, w, _image_at(s)))

    def _image_done(self, gen: int, out) -> None:
        if not isinstance(out, tuple) or len(out) != 4:
            return
        page, url, width, image = out
        if isinstance(image, QImage) and not image.isNull():
            self._keep(url, width, image)
        if gen != self._gen or page != self.page or url != self._page_url(page):
            return
        if not isinstance(image, QImage) or image.isNull():
            if self.image.pixmap().isNull():
                self.image.setText(tr('No image'))
            return
        shown = self.image._source
        if width < self.width_asked and shown is not None and not shown.isNull() and shown.width() >= image.width():
            return                                 # a sharper one is on show already (a zoom came first)
        self.image.set_image(image, keep_zoom=self.image.zoom > 1.0)
        self._ahead(page + 1)

    def _ahead(self, page: int) -> None:
        """Fetched before it is asked for: the manuscript's next image, else the next shelfmark's
        first (the one it opens on), at the width that fills the place."""
        width = self.fit_width
        if 0 <= page < len(self.pages) and page != self.page:
            url = self._page_url(page)
            if self._kept(url, width) is None:
                sized = sized_image_url(url, width)
                self._ahead_loads.request(self._gen, lambda u=url, w=width, s=sized: (u, w, _image_at(s)))
                return
        rows, at = self.rows(), self._at()
        meta = catalogue
        if meta is None or not 0 <= at < len(rows) - 1:
            return
        sid = rows[at + 1].sys_id
        known = self._thumb_urls.get(sid)
        if known and self._kept(known, width) is not None:
            return

        def fetch(s=sid, m=meta, w=width):
            # the image it opens on (the thumbnail when it has no list)
            pages = page_image_urls(s, m)
            url = pages[_opening_of(s, len(pages))] if pages else thumbnail_url(s, m)
            return (url, w, _image_at(sized_image_url(url, w)) if url else None, s)
        self._ahead_loads.request(self._gen, fetch)

    def _ahead_done(self, _gen: int, out) -> None:
        if not isinstance(out, tuple) or len(out) < 3:
            return
        url, width, image = out[:3]
        if len(out) == 4 and url:
            self._thumb_urls[out[3]] = url
        if url and isinstance(image, QImage) and not image.isNull():
            self._keep(url, width, image)

    def _zoomed(self, zoom: float) -> None:
        """Zoomed in: the picture asked for again sharper, once a page."""
        url = self._page_url(self.page)
        if zoom <= 1.0 or not url:
            return
        width = look_sharp_width(self.width_asked)
        if width <= self.width_asked or (url, width) in self._sharp_asked:
            return
        self._sharp_asked.add((url, width))
        self.width_asked = width
        image = self._kept(url, width)
        if image is not None:
            self.image.set_image(image, keep_zoom=True)
            return
        sized = sized_image_url(url, width)
        page = self.page
        self._image_loads.request(self._gen, lambda p=page, u=url, w=width, s=sized: (p, u, w, _image_at(s)))

    def _grown(self) -> None:
        """The window grew: the picture asked for again when its place wants a wider one."""
        if self.isVisible() and self.pages and self.image.zoom <= 1.0 and self._box_width() > self.width_asked:
            self._want(self.page, 0)

    def resizeEvent(self, event):                  # noqa: N802 -- Qt's name
        super().resizeEvent(event)
        if self.isVisible():
            self._width_timer.start()

    # -- choosing

    def choose(self) -> None:
        """Space / the box: the single choice chooses it (shown so in the list when it closes); several at
        once tick it."""
        if self.leaf is None:
            return
        if self.dialog.multi:
            self.dialog._toggle_leaf(self.leaf)
            self.sync_choice()
        else:
            self.dialog._select(self.leaf)         # chosen as a click chooses it (the pane shows it, under)

    def open_it(self) -> None:
        """Enter / the button: as Enter in the list (one: the picker's button; several: Open)."""
        leaf = self.leaf
        if leaf is None:
            return
        if self.dialog.multi:
            self.dialog._enter_several(leaf)
        else:
            self.hide()
            self.dialog._pick(leaf)

    # -- the keyboard

    #: Windows' virtual keys of Q and R: by the key, whatever letter the layout gives it (Hebrew: ק / ר)
    _VK_Q, _VK_R = 0x51, 0x52

    def keyPressEvent(self, event):                # noqa: N802 -- Qt's name
        key, mods, vk = event.key(), event.modifiers(), event.nativeVirtualKey()
        ctrl = bool(mods & Qt.KeyboardModifier.ControlModifier)
        alt = bool(mods & Qt.KeyboardModifier.AltModifier)
        shift = bool(mods & Qt.KeyboardModifier.ShiftModifier)
        rtl = self.isRightToLeft()
        forward = Qt.Key.Key_Left if rtl else Qt.Key.Key_Right
        back = Qt.Key.Key_Right if rtl else Qt.Key.Key_Left
        event.accept()
        if key == Qt.Key.Key_Escape or (not ctrl and not alt and (key == Qt.Key.Key_Q or vk == self._VK_Q)):
            self.close_look()
        elif key in (forward, back) and not alt:
            step = 1 if key == forward else -1
            if ctrl or shift:                      # Shift or Ctrl: the manuscript's images
                self.step_page(step)
            else:
                self.step_leaf(step)
        elif key in (Qt.Key.Key_Down, Qt.Key.Key_Up, Qt.Key.Key_PageDown, Qt.Key.Key_PageUp) and not ctrl and not alt:
            self.step_page(1 if key in (Qt.Key.Key_Down, Qt.Key.Key_PageDown) else -1)
        elif key in (Qt.Key.Key_Home, Qt.Key.Key_End) and not alt:
            self.go_leaf(0 if key == Qt.Key.Key_Home else len(self.rows()) - 1)
        elif key in (Qt.Key.Key_Plus, Qt.Key.Key_Equal) and not alt:
            self.image.zoom_by(_ZoomImage.STEP)
        elif key == Qt.Key.Key_Minus and not alt:
            self.image.zoom_by(1 / _ZoomImage.STEP)
        elif key == Qt.Key.Key_0 and not alt:
            self.image.reset_zoom()
        elif not ctrl and not alt and (key == Qt.Key.Key_R or vk == self._VK_R):
            self.image.rotate_by(-90 if shift else 90)
        elif key == Qt.Key.Key_Space and not ctrl and not alt:
            self.choose()
        elif key in (Qt.Key.Key_Return, Qt.Key.Key_Enter) and not alt:
            self.open_it()
        elif key == Qt.Key.Key_Tab or key == Qt.Key.Key_Backtab:
            pass                                   # the keyboard stays here until the quick look closes
        else:
            event.ignore()
            super().keyPressEvent(event)

    def focusNextPrevChild(self, _next: bool) -> bool:      # noqa: N802 -- Qt's name
        return False                               # Tab never leaves for the lists under it


# -- the dialog -------------------------------------------------------------------------------

class ShelfPickerDialog(QDialog):
    """See the module docstring. ``selection()`` is the pick, or None.

    Multiple choice (``mode='multi'``): ``purpose`` names the choice kept in ``store`` (a
    ``SelectionStore``; without one it lasts while the dialog is open). The confirm button calls
    ``on_confirm(entries, label)`` with ``[{sys_id, shelfmark, placement_id}]`` in the order chosen and
    a label naming the chips. It answers None when it took them all (they leave the choice and the
    dialog closes), the ids it took (those leave the choice; the dialog closes when nothing is left),
    or ``STAGED``: it took them all into a step of its own that is not applied yet (a host's own
    Apply), so the dialog closes and the choice stays kept until the host consumes it; `on_staged`
    then receives what was chosen again while the confirm was with the host, which the host must not
    consume. Anything chosen while the host works stays chosen. An exception keeps everything. ``lists_mgr``: offer "Save as a list" (``on_list_saved`` runs after)."""

    def __init__(self, service, start: Optional[str] = None, title: Optional[str] = None, parent=None, *,
                 mode: str = 'single', purpose: str = '', on_confirm: Optional[Callable[[list, str], object]] = None,
                 confirm_label: Optional[str] = None, store: Optional[SelectionStore] = None,
                 lists_mgr=None, on_list_saved: Optional[Callable[[], None]] = None,
                 on_staged: Optional[Callable[[frozenset], None]] = None, switch_label: Optional[str] = None,
                 limit: Optional[SelectionLimit] = None, act_label: Optional[str] = None,
                 actions: Optional[list] = None, node_boxes: bool = False):
        super().__init__(parent)
        self.setObjectName("shelfPicker")
        self.multi = mode == 'multi'
        # with a switch (Browse's picker, DX16) the dialog has both modes' parts and changes between
        # them in place (_set_mode): the multiple-choice parts are built, hidden while single
        self._can_multi = self.multi or bool(switch_label)
        if self._can_multi and (not purpose or on_confirm is None):
            raise ValueError("the multiple-choice mode needs a purpose and on_confirm")
        self._purpose = purpose
        self._on_confirm = on_confirm
        self._store = store
        self._lists_mgr = lists_mgr
        self._on_list_saved = on_list_saved
        self._on_staged = on_staged
        # how much may be ticked (shared.shelfmark_tree_limits; None: no limit, an exclusion): checked
        # before a tick is kept, and a node too big to tick whole has its box off
        self._limit = limit
        self._node_boxes = node_boxes              # the exclusion pickers: a box on every node (DX23)
        self._actions = list(actions or ())        # DX22: "Add to List", "Add to Puzzle"
        self._outcome = None                       # (key, entries) of an action that closed the picker
        self._chosen_leaf = None                   # single choice: the shelfmark a click chose (DX21)
        self._since: Optional[set] = None          # chosen while a confirm is with the host (None: none is)
        self._sel = store.load(purpose) if (self._can_multi and store is not None) else Selection()
        self._pruned_once = False                  # the kept choice brought up to the tree (on showing it)
        self._members_of = members_function(service)
        self._paint_state: Optional[dict] = None   # the last answer of paint_state
        self._sgen = 0
        self._wgen = 0
        self._work_then = None                     # (generation, continuation) of the work asked for last
        self._busy = False                         # a confirm or a save is under way
        self._list_retry = None                    # (list id, name) a failed "Save as a list" made
        self._all_chips: Optional[_AllChipsDialog] = None
        self._service = service
        self._start = start
        self._lang = 'he' if genizah_core.CURRENT_LANG == 'he' else 'en'
        self._labels = {'library': tr('Library'), 'others': tr('All other libraries, A-Z'),
                        'in': tr('Shown inside {place}'), 'part': tr('Part: {part}')}
        self._path: tuple = ()
        self._filters: dict = {}
        self._shown: dict = {}
        self._highlight: Optional[str] = None     # the placement marked: the one the picker opened at
        self._view: Optional[dict] = None
        self._gen = 0
        self._whole = True                        # the whole dialog is owed until it is drawn
        self._owed: set = set()                   # ... and so is each list changed meanwhile
        self._scroll: dict = {}                   # each list's scroll, by its filter key
        self._fresh: set = set()                  # lists whose filter changed: shown from their top
        self.crumb_buttons: list = []
        self._focus_after = True                  # where the keyboard goes after a draw
        self._selection: Optional[dict] = None
        self._drawn_path: tuple = ()               # the path the lists on screen were drawn for
        self._eat_release = None                   # a double click's release: its click is not repeated
        self._loads = _Loads(self._loaded, self)
        self._state_loads = _Loads(self._painted, self)    # what the boxes show: the newest wins
        self._work_loads = _Loads(self._worked, self)      # a confirm, a save, the prune on opening
        self._signs_loads = _Loads(self._signs_done, self)  # the rows' signs, once a process
        # the table's details (row_details, by sys_id, in this dialog's one language), read a page at a
        # time by one job that works out again what is missing when it ends (VX5); the thumbnails'
        # pictures (QImage, or None: none), fetched for the tiles in sight (VX7)
        self._row_details: dict = {}
        self._rows_job: Optional[_Job] = None
        self._rows_gen = 0
        self._closing = False
        self._thumbs: dict = {}
        self._thumb_tries: dict = {}                # sys_id -> (the tries that did not come, the last's time)
        self._retry_timer = QTimer(self)
        self._retry_timer.setSingleShot(True)
        self._retry_timer.timeout.connect(self._retry_thumbs)
        # the gallery's loads skip a picture known already or under way in the details pane (one fetch)
        # every thumbnail load, the gallery's and the pane's: an image known already is not fetched
        self._thumb_pool = _ThumbPool(self._thumb_done, self, skip=self.thumb_sharp,
                                      width=lambda: self.leaf_panel.fetch_width())
        self._folded: Optional[bool] = None         # the layout on screen folds its columns (VX2)

        self.setWindowTitle(title or tr("Pick from the shelf"))
        screen = (parent.screen() if parent is not None else None) or QApplication.primaryScreen()
        avail = screen.availableGeometry() if screen is not None else None
        width, height = 1240, 820
        if avail is not None:
            width, height = min(width, int(avail.width() * 0.96)), min(height, int(avail.height() * 0.86))
        self.resize(width, height)

        layout = QVBoxLayout(self)
        heading = QLabel(title or tr("Pick from the shelf"))
        font = heading.font()
        font.setBold(True)
        font.setPointSizeF(font.pointSizeF() * 1.2)
        heading.setFont(font)
        head = QHBoxLayout()
        head.addWidget(heading)
        head.addStretch(1)
        self.switch: Optional[QCheckBox] = None
        if switch_label:
            self.switch = QCheckBox(switch_label, self)
            self.switch.setChecked(self.multi)
            self.switch.toggled.connect(self._switch_toggled)
            head.addWidget(self.switch)
        # the details pane, shown or hidden (the reader's choice, kept): hidden, the shelfmarks and their
        # thumbnails take its room
        self.details_button = QToolButton(self)
        self.details_button.setText(tr("Details"))
        self.details_button.setCheckable(True)
        self.details_button.setChecked(load_details_choice())
        self.details_button.setToolTip(tr("Show or hide the details pane"))
        self.details_button.setAccessibleName(tr("Show or hide the details pane"))
        self.details_button.setObjectName("shelfDetailsToggle")
        self.details_button.toggled.connect(self._details_toggled)
        self.details_button.setToolTip(f"{tr('Show or hide the details pane')} (Alt+D)")
        head.addWidget(self.details_button)
        # the keyboard (owner, 2026-11-25): Alt+D the details pane, Ctrl+F the filter of the list in use
        QShortcut(QKeySequence("Alt+D"), self, activated=self.details_button.toggle)
        # ... and by the key on any layout, wherever the keyboard is in the picker: on the application,
        # as the main window's own (desktop/widgets/side_nav.py::LayoutProofShortcuts), which sees a
        # press before any widget can take it (a Hebrew Ctrl+F did nothing)
        self._layout_keys = _PickerLayoutKeys(self)
        QApplication.instance().installEventFilter(self._layout_keys)
        QShortcut(QKeySequence.StandardKey.Find, self, activated=self._focus_filter)
        # Tab moves between the lists alone (_list_key); F6 / Shift+F6 leave them for the rest of the
        # dialog (the next / previous field outside them) and come back to the list last in use
        QShortcut(QKeySequence("F6"), self, activated=lambda: self._leave_lists(1))
        QShortcut(QKeySequence("Shift+F6"), self, activated=lambda: self._leave_lists(-1))
        layout.addLayout(head)
        self.crumbs_box = QWidget(self)
        self.crumbs = FlowLayout(self.crumbs_box, margin=0, h_spacing=2, v_spacing=2)
        layout.addWidget(self.crumbs_box)
        self.chips_bar: Optional[_ChipsBar] = None
        if self._can_multi:
            self.chips_bar = _ChipsBar(self)
            layout.addWidget(self.chips_bar)
        self.status = QLabel(tr("Loading..."))
        layout.addWidget(self.status)
        body = QHBoxLayout()
        body.setSpacing(10)
        self.node_columns = [_NodeColumn(self) for _ in range(WIDE_COLUMNS)]
        for column in self.node_columns:
            column.setVisible(False)
            column.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
            body.addWidget(column)
        self.leaf_panel = _LeafPanel(self)
        self.leaf_panel.setVisible(False)
        self.leaf_panel.setMinimumWidth(280)
        body.addWidget(self.leaf_panel, 1)
        # the columns keep to the start edge, each its own width; the space after the last is empty
        # while no shelfmarks are shown (DX26; the layout is mirrored in Hebrew)
        self._filler = QWidget(self)
        self._filler.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        body.addWidget(self._filler, 1)
        self.details = _DetailsPane(self)
        body.addWidget(self.details)
        layout.addLayout(body, 1)
        foot = QHBoxLayout()
        self.count_label: Optional[QLabel] = None
        self.note: Optional[QLabel] = None
        self.clear_button = self.save_button = self.confirm_button = None
        self._stretch = None
        if self._can_multi:
            self.count_label = QLabel(tr('{n} manuscripts chosen').format(n=0))
            foot.addWidget(self.count_label)
        # what a tick, a confirm or an action says (a limit), in both modes
        self.note = QLabel('')
        self.note.setWordWrap(True)
        foot.addWidget(self.note, 1)
        if self._can_multi:
            self.clear_button = QPushButton(tr('Clear'))
            self.clear_button.clicked.connect(self._clear)
            foot.addWidget(self.clear_button)
            if lists_mgr is not None:
                self.save_button = QPushButton(tr('Save as a list'))
                self.save_button.clicked.connect(self._save_as_list)
                foot.addWidget(self.save_button)
        self.action_buttons: dict = {}
        for action in self._actions:
            button = QPushButton(action.label, self)
            button.setAutoDefault(False)
            button.clicked.connect(lambda _checked=False, a=action: self._run_action(a))
            foot.addWidget(button)
            self.action_buttons[action.key] = button
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Cancel, self)
        cancel = buttons.button(QDialogButtonBox.StandardButton.Cancel)
        cancel.setText(tr("Cancel"))
        cancel.setAutoDefault(False)
        cancel.setDefault(False)
        buttons.rejected.connect(self.reject)
        # single choice: the button that takes the shelfmark a click chose (DX21; double-click and
        # Enter press it too)
        self.act_button = buttons.addButton(act_label or tr('Select'), QDialogButtonBox.ButtonRole.ActionRole)
        self.act_button.setAutoDefault(False)
        self.act_button.setEnabled(False)
        self.act_button.clicked.connect(lambda _checked=False: self._pick(self._chosen_leaf))
        if self._can_multi:
            self.confirm_button = buttons.addButton(confirm_label or tr('Done'), QDialogButtonBox.ButtonRole.ActionRole)
            self.confirm_button.setAutoDefault(False)
            self.confirm_button.clicked.connect(self._confirm)
        for button in (self.clear_button, self.save_button):
            if button is not None:
                button.setAutoDefault(False)
        foot.addWidget(buttons)
        layout.addLayout(foot)

        # the quick look (owner, 2026-11-25): over everything, hidden until Q or the pane's button
        self.look = _QuickLook(self)

        self._show_mode_parts()
        self._request(first=True)
        self._load_signs()
        if self.multi:
            self._footer()
            self._prune_on_open()

    # -- the contract -----------------------------------------------------------------------

    def selection(self) -> Optional[dict]:
        return self._selection

    def outcome(self):
        """How it closed: ``('open', selection)`` (the act button), ``(key, entries)`` (an action
        that closes it), or None."""
        if self._outcome is not None:
            return self._outcome
        return ('open', self._selection) if self._selection is not None else None

    def path(self) -> tuple:
        return self._path

    def _pick(self, leaf) -> None:
        """Take a shelfmark: in the single choice the act button's work (it closes the picker with
        it), in the multiple choice its box."""
        if leaf is None:
            return
        if self.multi:                             # a shelfmark's box: ticked or unticked
            self._toggle_leaf(leaf)
            self.details.show_leaf(leaf)
            return
        if self._selection is not None:
            return
        self._selection = leaf.as_selection()
        self.accept()

    def _enter_several(self, leaf) -> None:
        """Enter in the multiple choice is the Open button (Space ticks): what is ticked opens; with
        nothing ticked, the shelfmark Enter was pressed on is ticked first (owner, 2026-11-25)."""
        if leaf is None:
            return
        if not len(self._sel):
            self._toggle_leaf(leaf)
        button = getattr(self, 'confirm_button', None)
        if button is not None and button.isEnabled() and len(self._sel):
            self._confirm()

    def _select(self, leaf) -> None:
        """A click on a shelfmark: in the single choice it is chosen (shown so, its details in the
        pane, the act button on); in the multiple choice its box, and its details too (DX21, DX25)."""
        if leaf is None:
            return
        if self.multi:
            self._pick(leaf)
            return
        self._chosen_leaf = leaf
        self.leaf_panel.mark(leaf)
        self.details.show_leaf(leaf)
        self._say('')
        self._footer()

    def _switch_toggled(self, on: bool) -> None:
        """The header's switch: the same dialog changes mode where it is. Not while a confirm or a
        save is with the host (the switch goes back)."""
        if self._busy:
            self.switch.blockSignals(True)
            self.switch.setChecked(self.multi)
            self.switch.blockSignals(False)
            return
        self._set_mode(bool(on))

    def _set_mode(self, multi: bool) -> None:
        """Single or multiple choice, in place: the multiple-choice parts shown or hidden, the lists
        drawn again from the view on screen (boxes or none), the path, filters and scroll kept. The
        choice is the kept one, read at the opening (single choice never changes it)."""
        if multi == self.multi or not self._can_multi:
            return
        self.multi = multi
        self._sgen += 1                            # an answer asked for in the other mode is not drawn
        if not multi:
            # the single choice's shelfmark is the one the pane shows: the one clicked last, in either
            # mode (Codex #87: chosen A, B clicked while several, back to one: Open acted on A)
            self._chosen_leaf = self.details.leaf
        if not multi and self._all_chips is not None:
            self._all_chips.close()
        self._show_mode_parts()
        for column in self.node_columns:
            column.set_mode()
        if self._view is not None:
            self._draw(self._view)
        if multi:
            self._request_state()
            if not self._pruned_once:
                self._prune_on_open()
        else:
            self.details.paint(None)                # a partner's box shows a choice the lists show

    def _show_mode_parts(self) -> None:
        self.act_button.setVisible(not self.multi)
        self._say('')
        if not self._can_multi:
            self._footer()
            return
        for part in (self.chips_bar, self.count_label, self.clear_button, self.save_button,
                     self.confirm_button):
            if part is not None:
                part.setVisible(self.multi)
        if not self.multi:
            self.leaf_panel.whole.setVisible(False)
        self._footer()

    #: Narrower than this, the details pane makes way for the columns (three columns, the shelfmarks
    #: and the pane need about 1,100 pixels; Codex pre-flight).
    DETAILS_MIN_WIDTH = 1100

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._show_details_pane()
        look = getattr(self, 'look', None)
        if look is not None and look.isVisible():
            look.setGeometry(self.rect())

    def _show_details_pane(self) -> None:
        """The pane where the reader keeps it shown and the dialog is wide enough for it."""
        self.details.setVisible(self.details_button.isChecked() and self.width() >= self.DETAILS_MIN_WIDTH)

    def _details_toggled(self, shown: bool) -> None:
        save_details_choice(shown)
        self._show_details_pane()
        pending, self.details.pending = self.details.pending, None
        if shown and pending is not None:
            self.details.show_leaf(pending)        # the shelfmark clicked while it was hidden

    def done(self, result):
        self._closing = True
        QApplication.instance().removeEventFilter(self._layout_keys)
        self._thumb_pool.stop()
        job = self._rows_job
        if job is not None:                        # local reads: waited for as the lists' loads are
            job.requestInterruption()
            if not job.wait(STOP_WAIT_MS):
                job.finished.connect(job.deleteLater)
        self.details.stop()
        self.look.stop()
        self._loads.stop()
        self._state_loads.stop()
        self._work_loads.stop()
        self._signs_loads.stop(wait_ms=0)          # the sidecars, never the tree: not waited for
        if self._all_chips is not None:
            self._all_chips.close()
        super().done(result)

    def chosen(self) -> Selection:
        """What is chosen now (multiple choice)."""
        return self._sel

    def keyPressEvent(self, event):
        if self._layout_shortcut(event):
            return
        # Enter belongs to the list or box that has the focus (handled there), never to a button:
        # an Enter in a filter must not close the dialog.
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            event.accept()
            return
        super().keyPressEvent(event)

    def eventFilter(self, obj, event):
        # A double click is one click. Its second press would land on the list the first click's
        # level has drawn meanwhile and open whatever row is under it there.
        kind = event.type()
        if kind == QEvent.Type.MouseButtonDblClick and obj in self._viewports():
            self._eat_release = obj
            # single choice: a double click on a shelfmark is its act button (DX21); the first click
            # chose it already
            if obj in self.leaf_panel.viewports() and not self.multi:
                leaf = self.leaf_panel.leaf_at(obj, event.position().toPoint())
                if leaf is not None:
                    self._pick(leaf)
            elif not self.multi:
                for column in self.node_columns:
                    if obj is column.table.viewport():
                        row = column.table.rowAt(event.position().toPoint().y())
                        if 0 <= row < len(column.rows) and 'leaf' in column.rows[row]:
                            self._pick(column.rows[row]['leaf'])
            return True
        if kind == QEvent.Type.MouseButtonRelease and obj is self._eat_release:
            self._eat_release = None
            return True
        if kind == QEvent.Type.KeyPress and (self._layout_shortcut(event) or self._list_key(obj, event)):
            return True
        # Enter and Space open the row or pick the shelfmark that has the keyboard's focus.
        if kind == QEvent.Type.KeyPress and event.key() in (
                Qt.Key.Key_Return, Qt.Key.Key_Enter, Qt.Key.Key_Space):
            if obj in self.leaf_panel.views():
                leaf = self.leaf_panel.current_leaf() if obj is self.leaf_panel.active() else None
                if leaf is not None:
                    # single choice: Space chooses, Enter takes (the act button); multiple: Space its
                    # box, Enter the Open button (owner, 2026-11-25)
                    if event.key() == Qt.Key.Key_Space:
                        self._select(leaf)
                    elif self.multi:
                        self._enter_several(leaf)
                    else:
                        self._pick(leaf)
                return True
            for column in self.node_columns:
                if obj is column.table:
                    row = obj.currentRow()
                    if row >= 0:
                        # multiple choice: Space ticks the row's box, Enter opens it (a shelfmark's
                        # row has only its box)
                        if event.key() == Qt.Key.Key_Space and column.is_box_row(row):
                            self._toggle_row(column, row)
                        else:
                            self._open_row(column, row, act=event.key() != Qt.Key.Key_Space)
                    return True
        return super().eventFilter(obj, event)

    # -- the keyboard (owner, 2026-11-25) -------------------------------------------------------------

    def _list_key(self, obj, event) -> bool:
        """The keys a list answers beside moving in it: in a column, the arrow forward opens the row and
        goes into the next list, the arrow back goes to the list before; in any list, Ctrl + an arrow
        is the pane's previous / next image (as the arrows on screen: mirrored in Hebrew) and + / - / 0
        zoom its picture. True when the key was used."""
        column = next((c for c in self.node_columns if obj is c.table), None)
        if column is None and obj not in self.leaf_panel.views():
            return False
        key, mods = event.key(), event.modifiers()
        if key in (Qt.Key.Key_Tab, Qt.Key.Key_Backtab) and not mods & (Qt.KeyboardModifier.ControlModifier
                                                                       | Qt.KeyboardModifier.AltModifier):
            lists = self._shown_lists()
            here = column.table if column is not None else obj
            if here in lists and len(lists) > 1:
                step = -1 if key == Qt.Key.Key_Backtab or mods & Qt.KeyboardModifier.ShiftModifier else 1
                self._focus_list(lists[(lists.index(here) + step) % len(lists)])   # round, from the last to the first
            return True
        rtl = self.isRightToLeft()
        forward = Qt.Key.Key_Left if rtl else Qt.Key.Key_Right
        back = Qt.Key.Key_Right if rtl else Qt.Key.Key_Left
        ctrl = bool(mods & Qt.KeyboardModifier.ControlModifier)
        shift = bool(mods & Qt.KeyboardModifier.ShiftModifier)
        if (ctrl or shift) and not mods & Qt.KeyboardModifier.AltModifier and key in (forward, back):
            # Shift or Ctrl + an arrow: the pane's previous / next image (owner, 2026-11-25: Shift too)
            if self.details.isVisible() and self.details.pages:
                self.details.step_page(1 if key == forward else -1)
            return True
        plain = not (mods & (Qt.KeyboardModifier.ControlModifier | Qt.KeyboardModifier.AltModifier))
        if plain and key in (Qt.Key.Key_Plus, Qt.Key.Key_Equal, Qt.Key.Key_Minus, Qt.Key.Key_0):
            if self.details.isVisible() and self.details.image.isVisible():
                if key == Qt.Key.Key_0:
                    self.details.image.reset_zoom()
                else:
                    step = _ZoomImage.STEP
                    self.details.image.zoom_by(1 / step if key == Qt.Key.Key_Minus else step)
            return True
        if plain and self.leaf_panel.isVisibleTo(self) and self._view_key(event):
            return True
        vk = event.nativeVirtualKey()
        if plain and (key == Qt.Key.Key_Q or vk == _QuickLook._VK_Q) and not mods & Qt.KeyboardModifier.ShiftModifier:
            # Q: the quick look, on the shelfmark the keyboard is on (by the key: a Hebrew layout types ק)
            self.quick_look(self._leaf_in(obj, column), self.details)    # the pane's: at its page and turn
            return True
        if plain and (key == Qt.Key.Key_R or vk == _QuickLook._VK_R):
            # R / Shift+R: the pane's picture turned a quarter, as its buttons (Hebrew: ר)
            if self.details.isVisible() and self.details.image.isVisible():
                shift = bool(mods & Qt.KeyboardModifier.ShiftModifier)
                self.details.image.rotate_by(-90 if shift else 90)
            return True
        if plain and (key == Qt.Key.Key_S or vk == 0x53) and not mods & Qt.KeyboardModifier.ShiftModifier:
            # S: "Open more than one" on or off (by the key too: a Hebrew layout types ד there)
            if self.switch is not None and self.switch.isEnabled():
                self.switch.toggle()
                if self.leaf_panel.isVisibleTo(self):
                    self._focus_list(self.leaf_panel.active())
            return True
        if column is not None and plain and key in (forward, back):
            if key == forward:
                row = column.table.currentRow()
                if row >= 0 and 'leaf' not in (column.rows[row] if row < len(column.rows) else {}):
                    if self._opened_here(column, row):
                        self._focus_after_column(column)       # open already: into its list
                    else:
                        self._open_row(column, row)            # the next list takes the keyboard when drawn
            else:
                self._focus_before_column(column)
            return True
        return False

    def _leaf_in(self, obj, column: Optional["_NodeColumn"]):
        """The shelfmark the keyboard is on in a list (a column's shelfmark row too), or None."""
        if column is not None:
            row = column.table.currentRow()
            data = column.rows[row] if 0 <= row < len(column.rows) else {}
            return data.get('leaf')
        return self.leaf_panel.current_leaf() if obj is self.leaf_panel.active() else None

    def quick_look(self, leaf, pane: Optional["_DetailsPane"] = None) -> None:
        """The quick look on ``leaf`` (from the pane: at its page and turn)."""
        if leaf is not None:
            self.look.open_on(leaf, pane)

    def _look_closed(self, leaf) -> None:
        """Back from the quick look: the list at the shelfmark looked at last, chosen as a click chooses
        it (several at once: shown in the pane, its box as it was), the keyboard in it."""
        panel = self.leaf_panel
        in_list = leaf is not None and any(row.get('leaf') is not None and row['leaf'].id == leaf.id
                                           for row in (panel.leaves or {}).get('rows') or ())
        if in_list:
            if self.multi:
                panel.mark(leaf)
                if self.details.leaf_id != leaf.id:
                    self.details.show_leaf(leaf)
            elif self._chosen_leaf is None or self._chosen_leaf.id != leaf.id:
                self._select(leaf)
            else:
                panel.mark(leaf)
            item = panel.active().currentItem()
            if item is not None:
                panel.active().scrollToItem(item)
        if leaf is not None and not in_list:
            self.reveal(leaf.id)                   # past the list: the picker goes to its group, chosen there
            return
        if panel.isVisibleTo(self) and in_list:
            self._focus_list(panel.active())
        else:
            lists = self._shown_lists()
            last = getattr(self, '_last_list', None)
            if lists:
                self._focus_list(last if last in lists else lists[-1])

    #: Windows' virtual keys of [ and ]: the keys, not the letters a layout gives them (a Hebrew
    #: keyboard types them the other way round)
    _VK_BRACKETS = {0xDB: -1, 0xDD: 1}

    def _view_key(self, event) -> bool:
        """From a list, the shelfmarks' view (owner, 2026-11-25): 1 / 2 / 3 the list, the table, the
        thumbnails; [ / ] the thumbnails smaller / larger. The keyboard stays in the shelfmarks."""
        panel, key = self.leaf_panel, event.key()
        views = {Qt.Key.Key_1: 'list', Qt.Key.Key_2: 'table', Qt.Key.Key_3: 'gallery'}
        if key in views:
            panel.set_view(views[key])
            self._focus_list(panel.active())
            return True
        step = self._VK_BRACKETS.get(event.nativeVirtualKey())
        if step is None:
            step = {Qt.Key.Key_BracketLeft: -1, Qt.Key.Key_BracketRight: 1}.get(key)
        if step is None:
            return False
        if panel.view == 'gallery':
            at = SIZES.index(panel.size) + step
            if 0 <= at < len(SIZES):
                panel.set_size(SIZES[at])
                panel.size_box.setCurrentIndex(at)
        return True

    #: Windows' virtual keys of D and F: Alt+D and Ctrl+F by the key, whatever letter the layout gives it
    #: (a Hebrew keyboard types ג and כ there, and the QShortcuts, which match the letter, never fire)
    _VK_D, _VK_F = 0x44, 0x46

    def _layout_shortcut(self, event) -> bool:
        """Alt+D and Ctrl+F on a keyboard whose layout gives those keys other letters (the QShortcuts
        answer the Latin ones). True when the key was used."""
        key, mods, vk = event.key(), event.modifiers(), event.nativeVirtualKey()
        alt = bool(mods & Qt.KeyboardModifier.AltModifier)
        ctrl = bool(mods & Qt.KeyboardModifier.ControlModifier)
        del key                                    # whatever letter Qt names: the key decides
        if alt and not ctrl and vk == self._VK_D:
            self.details_button.toggle()
            return True
        if ctrl and not alt and vk == self._VK_F:
            self._focus_filter()
            return True
        return False

    def _leave_lists(self, step: int) -> None:
        """F6 (``step`` 1) / Shift+F6 (-1). In a list: the first field after the lists / before them that
        takes the keyboard (the buttons, the pane; the switch, the breadcrumb), the filters passed by. Out
        of the lists: back into the list last in use."""
        lists = self._shown_lists()
        if not lists or self.look.isVisible():
            return
        focus = QApplication.focusWidget()
        if focus not in lists:
            last = getattr(self, '_last_list', None)
            self._focus_list(last if last in lists else lists[0])
            return
        self._last_list = focus
        skip = set(lists) | {c.filter for c in self.node_columns} | {self.leaf_panel.filter}
        widget = lists[-1] if step > 0 else lists[0]
        for _ in range(500):                       # the focus chain is a ring: once round at most
            widget = widget.nextInFocusChain() if step > 0 else widget.previousInFocusChain()
            if widget is None or widget is focus:
                return
            if (widget not in skip and widget.isVisibleTo(self) and widget.isEnabled()
                    and widget.focusPolicy() & Qt.FocusPolicy.TabFocus and self.isAncestorOf(widget)
                    and not any(w.isAncestorOf(widget) for w in lists)):
                widget.setFocus(Qt.FocusReason.TabFocusReason if step > 0 else Qt.FocusReason.BacktabFocusReason)
                return

    def _shown_lists(self) -> list:
        """The lists on screen in their order: the node columns, then the shelfmarks."""
        lists = [c.table for c in self.node_columns if c.isVisibleTo(self)]
        if self.leaf_panel.isVisibleTo(self):
            lists.append(self.leaf_panel.active())
        return lists

    def _opened_here(self, column: "_NodeColumn", row: int) -> bool:
        """The row is the node whose children the next list shows."""
        depth = (column.col or {}).get('depth')
        data = column.rows[row] if 0 <= row < len(column.rows) else {}
        return depth is not None and len(self._path) > depth and self._path[depth] == data.get('id')

    def _focus_after_column(self, column: "_NodeColumn") -> None:
        lists = self._shown_lists()
        if column.table in lists and lists.index(column.table) + 1 < len(lists):
            self._focus_list(lists[lists.index(column.table) + 1])

    def _focus_before_column(self, column: "_NodeColumn") -> None:
        """The list before; from the first on screen, one level up (its list then shows again)."""
        lists = self._shown_lists()
        at = lists.index(column.table) if column.table in lists else -1
        if at > 0:
            self._focus_list(lists[at - 1])
            return
        depth = (column.col or {}).get('depth')
        if depth:
            self.go(tuple(self._path[:depth - 1]))     # _focus_after: the keyboard goes to it when drawn

    def _focus_list(self, view) -> None:
        """The keyboard into a list, on its current row (the first when it has none)."""
        view.setFocus(Qt.FocusReason.ShortcutFocusReason)
        if isinstance(view, QTableWidget) and view.currentRow() < 0 and view.rowCount():
            owner = next((c for c in self.node_columns if c.table is view), None)
            view.setCurrentCell(0, owner.name_col if owner is not None else 0)
        elif isinstance(view, QListWidget) and view.currentRow() < 0 and view.count():
            view.setCurrentRow(0)

    def _focus_filter(self) -> None:
        """Ctrl+F: the filter of the list the keyboard is in (or its filter), else of the last list."""
        if self.look.isVisible():
            return                                 # under the quick look
        focus = QApplication.focusWidget()
        box = next((c.filter for c in self.node_columns
                    if c.isVisibleTo(self) and focus in (c.table, c.filter)), None)
        if box is None and self.leaf_panel.isVisibleTo(self):
            box = self.leaf_panel.filter               # in the shelfmarks, or nowhere in particular
        if box is None:
            shown = [c for c in self.node_columns if c.isVisibleTo(self)]
            box = shown[-1].filter if shown else None
        if box is not None:
            box.setFocus(Qt.FocusReason.ShortcutFocusReason)
            box.selectAll()

    def _viewports(self) -> tuple:
        return (*(c.table.viewport() for c in self.node_columns), *self.leaf_panel.viewports())

    # -- what the reader does ---------------------------------------------------------------

    def _clicked_row(self, column: "_NodeColumn", row: int, col: int = -1) -> None:
        if col == 0 and column.is_box_row(row):    # a box (multiple choice): ticked, never opened
            self._toggle_row(column, row)
            return
        self._open_row(column, row)

    def _clicked_leaf(self, item) -> None:
        self._select(item.data(_ROW))

    def _open_row(self, column: "_NodeColumn", row: int, act: bool = False) -> None:
        """A row clicked (or Enter, ``act``): a node opens; a shelfmark (a filter found it) is chosen,
        or taken with Enter."""
        if not 0 <= row < len(column.rows):
            return
        data = column.rows[row]
        if 'sep' in data:
            return
        if 'leaf' in data:
            if act:
                (self._enter_several if self.multi else self._pick)(data['leaf'])
            else:
                self._select(data['leaf'])
            return
        if data.get('go') is not None:
            self.go(tuple(data['go']))
            return
        self.choose_node(column.col['depth'], data['id'])

    def choose_node(self, depth: int, node_id: str) -> None:
        """A node's row: the path to it, down through any single child, as the web's ``choose_node``.
        The row belongs to the lists on screen, so its path begins as theirs does, whatever a later
        click (not drawn yet) has made of the path meanwhile."""
        self.go(tuple(self._drawn_path[:depth]) + passed_through(self._service, node_id))

    def go(self, path: tuple) -> None:
        self._path = settle(self._service, tuple(path), tuple(self._path))
        self._focus_after = True
        self._request()

    def reveal(self, target: str) -> None:
        """A join partner's link in the pane: the columns go to its place (``start_at``: a sys_id or
        a placement id), its shelfmark is chosen and the pane shows it. The lists' filters are cleared
        (one could hide it)."""
        self._filters = {}
        for part in (*self.node_columns, self.leaf_panel):
            part.filter.reset()                    # a box keeps the list it shows: its text goes too
        self._focus_after = True
        self._request(first=True, start=target, reveal=True)

    def _show_revealed(self, leaf) -> None:
        """The shelfmark a link went to, chosen: in the single choice as a click chooses it; in the
        multiple choice shown and in the pane, never ticked."""
        if not self.multi:
            self._chosen_leaf = leaf
            self._footer()
        self.leaf_panel.mark(leaf)
        self.details.show_leaf(leaf)

    def _tick_partner(self, sys_id: str, on: bool) -> None:
        """A join partner's box in the pane: as its box in the lists, and ticking it ticks the
        shelfmark the pane shows too (the fragments of a join are chosen together; it first, then the
        partner, one change for the limit). Unticking takes out the partner alone. Ticked with the
        multiple choice off, the switch turns it on first, as "Open more than one" would; a box no
        switch can serve goes back."""
        leaf = self.details.place_leaf(sys_id)
        if leaf is not None and not self.multi and on and self.switch is not None:
            self.switch.setChecked(True)               # _switch_toggled -> _set_mode, unless busy
        if leaf is None or not self.multi:
            self.details.paint(self._paint_state if self.multi else None)
            return
        self._say('')
        if on:
            shown = self.details.leaf

            def add(s, m, lf=leaf, own=shown):
                added = bool(own is not None and own.sys_id != lf.sys_id and s.add_leaf(own, m))
                return s.add_leaf(lf, m) or added
            self._change(add)
        else:
            self._change(lambda s, m, sid=sys_id: s.drop_leaf(sid, m))
        try:                                           # the box as the choice is now (a limit may refuse)
            chosen = sys_id in self._sel.chosen_set(self._members_of)
        except Exception:  # noqa: BLE001 -- the next answer draws it
            chosen = on
        self.details.set_box(sys_id, chosen)

    def _tick_partners(self, on: bool) -> None:
        """The pane's "All" / "Clear": every join partner with a place on the shelf ticked (with the
        shelfmark shown, as one partner's box does), or unticked, in one change (the limit counts them
        together). With the multiple choice off, "All" turns it on first."""
        leaves = [lf for lf in (self.details.place_leaf(s) for s in list(self.details.join_boxes)) if lf is not None]
        if not leaves:
            return
        if not self.multi and on and self.switch is not None:
            self.switch.setChecked(True)               # _switch_toggled -> _set_mode, unless busy
        if not self.multi:
            self.details.paint(None)
            return
        self._say('')
        if on:
            shown = self.details.leaf

            def add(s, m, lfs=tuple(leaves), own=shown):
                changed = bool(own is not None and own.sys_id not in {lf.sys_id for lf in lfs}
                               and s.add_leaf(own, m))
                for lf in lfs:
                    changed = s.add_leaf(lf, m) or changed
                return changed
            self._change(add)
        else:
            def drop(s, m, sids=tuple(lf.sys_id for lf in leaves)):
                changed = False
                for sid in sids:
                    changed = s.drop_leaf(sid, m) or changed
                return changed
            self._change(drop)
        try:                                           # the boxes as the choice is now (a limit may refuse)
            chosen = self._sel.chosen_set(self._members_of)
        except Exception:  # noqa: BLE001 -- the next answer draws them
            return
        for lf in leaves:
            self.details.set_box(lf.sys_id, lf.sys_id in chosen)

    def _filter_typed(self, key: str, text: str) -> None:
        self._filters[key] = text or ''
        self._shown.pop(key, None)
        self._fresh.add(key)                       # a new filter's list starts at its top

    def _filter_load(self, key: str) -> None:
        self._request(only=key)

    def set_filter(self, key: str, text: str) -> None:
        """A list's filter, loaded at once (a host or a test; typing goes through the box)."""
        self._filter_typed(key, text)
        self._filter_load(key)

    def _show_more(self, key: Optional[str], base: int) -> None:
        if key is None:
            return
        self._shown[key] = self._shown.get(key, base) + base
        self._request(only=key)

    # -- loading ----------------------------------------------------------------------------

    def _request(self, only: Optional[str] = None, first: bool = False, start: Optional[str] = None,
                 reveal: bool = False) -> None:
        self._gen += 1
        if only is None:
            self._whole = True
        else:
            self._owed.add(only)
        service, lang, labels, titles = self._service, self._lang, self._labels, title_of
        path, filters, shown = tuple(self._path), dict(self._filters), dict(self._shown)
        if first:
            start = self._start if start is None else start

            def job():
                begin = start_at(service, start)
                opened = dict(shown)
                if begin.leaf is not None:
                    index = direct_index(service, begin.leaf)
                    opened[f"l:{begin.leaf.node_id}"] = max(PAGE_SIZE, (index // PAGE_SIZE + 1) * PAGE_SIZE)
                return {'path': begin.path, 'for_path': begin.path, 'leaf': begin.leaf, 'shown': opened,
                        'reveal': reveal,
                        'view': load_view(service, begin.path, filters, opened, lang, labels, title_of=titles)}
        else:
            def job():
                return {'for_path': path, 'view': load_view(service, path, filters, shown, lang, labels,
                                                            title_of=titles)}
        self._loads.request(self._gen, job)

    def _loaded(self, gen: int, out) -> None:
        if gen != self._gen:
            return                                 # a newer request is waiting: it answers for this one
        if isinstance(out, Exception):
            self.status.setText(tr("Not loaded"))
            self.status.setVisible(True)
            return
        opened_at = None                           # the manuscript the picker opened at (not a join's link)
        if 'path' in out:
            self._path = tuple(out['path'])
            self._shown = dict(out['shown'])
            if out['leaf'] is not None:
                self._highlight = out['leaf'].id
                if not out.get('reveal'):
                    opened_at = out['leaf']
        if opened_at is not None and self._chosen_leaf is None:
            # opened at a manuscript: it is the one chosen, as on the web (DX21), drawn so by _draw
            self._chosen_leaf = opened_at
        view = out['view']
        self._drawn_path = tuple(out['for_path'])
        owed, self._owed = self._owed, set()
        if not self._whole and owed and self._draw_lists(view, owed):
            self._view = view
            self._drop_left(view)                  # a filter may have hidden the row it was found in
        else:
            self._whole = False
            self._draw(view)
        if out.get('reveal') and out.get('leaf') is not None:
            self._show_revealed(out['leaf'])
        if opened_at is not None:
            if self.details.leaf is None:          # ... and the pane shows it, in either mode (DX25)
                self.details.show_leaf(opened_at)
            self._footer()
        if self.multi:                             # the rows drawn now: what their boxes show
            self._request_state()

    def _remember_scroll(self) -> None:
        for part in (*self.node_columns, self.leaf_panel):
            if part.isVisibleTo(self) and part.key and part.key not in self._fresh:
                self._scroll[part.key] = part.scroll_value()

    def _restore_scroll(self, part) -> None:
        fresh = part.key in self._fresh
        self._fresh.discard(part.key)
        part.scroll_to(None if fresh else self._scroll.get(part.key))

    def _on_screen(self) -> dict:
        shown = {c.key: c for c in self.node_columns if c.isVisibleTo(self) and c.key}
        if self.leaf_panel.isVisibleTo(self) and self.leaf_panel.key:
            shown[self.leaf_panel.key] = self.leaf_panel
        return shown

    def _draw_lists(self, view: dict, owed: set) -> bool:
        """The lists whose filter or length changed, alone, when they are still on screen; False
        when one is not (then the whole dialog is drawn)."""
        has_leaves = view['leaves'] is not None
        if folds_columns(self.leaf_panel.view, has_leaves) != self._folded:
            return False
        on_screen = self._on_screen()
        self._remember_scroll()
        plan = []
        shown = shown_columns(view['columns'], self.leaf_panel.view, has_leaves, WIDE_COLUMNS)
        for key in owed:
            col = next((c for c in shown if c['filter_key'] == key), None)
            leaves = view['leaves'] if view['leaves'] is not None and view['leaves']['filter_key'] == key else None
            part = on_screen.get(key)
            if part is None or (col is None and leaves is None):
                return False
            if (part is self.leaf_panel) != (leaves is not None):
                return False
            plan.append((part, col if col is not None else leaves))
        for part, data in plan:
            part.fill(data)
            self._restore_scroll(part)
        return True

    def _draw(self, view: dict) -> None:
        """The whole dialog for the path."""
        self._remember_scroll()
        self._view = view
        self.status.setVisible(False)
        self._draw_crumbs(view['crumbs'])
        # the last three; in the table and the thumbnails only the last (VX2, the owner's rule)
        has_leaves = view['leaves'] is not None
        self._folded = folds_columns(self.leaf_panel.view, has_leaves)
        columns = shown_columns(view['columns'], self.leaf_panel.view, has_leaves, WIDE_COLUMNS)
        for i, column in enumerate(self.node_columns):
            if i < len(columns):
                column.show_column(columns[i])
                column.setVisible(True)
                self._restore_scroll(column)
            else:
                column.setVisible(False)
                column.key = None
                column.filter.key = None
        leaves = view['leaves']
        if leaves is not None:
            self.leaf_panel.show_leaves(leaves)
            self.leaf_panel.setVisible(True)
            self._restore_scroll(self.leaf_panel)
        else:
            self.leaf_panel.setVisible(False)
            self.leaf_panel.key = None
            self.leaf_panel.filter.key = None
        self._filler.setVisible(leaves is None)
        self._drop_left(view, columns)
        if self._chosen_leaf is not None:          # the shelfmark chosen stays shown chosen
            self.leaf_panel.mark(self._chosen_leaf)
        if self._focus_after:
            self._focus_after = False
            self._focus_deepest(view)

    def _drop_left(self, view: dict, columns: Optional[list] = None) -> None:
        """A shelfmark left behind (no longer in the lists drawn: ``leaf_in_view``) is no longer in the
        pane, nor chosen: Open would take one the reader no longer sees (owner, 2026-11-22). Asked after
        every drawing, the whole dialog or a list alone (a filter typed: Codex #99). ``columns``: the
        node columns drawn, as the layout shows them (``shown_columns``)."""
        if columns is None:
            columns = shown_columns(view['columns'], self.leaf_panel.view, view['leaves'] is not None,
                                    WIDE_COLUMNS)
        if self.details.leaf is not None and not leaf_in_view(view, self.details.leaf, columns):
            self.details.clear()
        if self._chosen_leaf is not None and not leaf_in_view(view, self._chosen_leaf, columns):
            self._chosen_leaf = None
            self._footer()

    def _focus_deepest(self, view: dict) -> None:
        """The keyboard goes where the next choice is: the chosen node's children, else its shelfmarks."""
        target, owner = None, None
        for column in self.node_columns:
            if column.isVisibleTo(self) and column.col and column.col['depth'] == len(self._path):
                target, owner = column.table, column
        if target is None and self.leaf_panel.isVisibleTo(self):
            target = self.leaf_panel.active()
        if target is None:
            shown = [c for c in self.node_columns if c.isVisibleTo(self)]
            target, owner = (shown[-1].table, shown[-1]) if shown else (None, None)
        if target is not None:
            target.setFocus(Qt.FocusReason.OtherFocusReason)
            if isinstance(target, QTableWidget) and target.currentRow() < 0 and target.rowCount():
                name_col = owner.name_col if owner is not None else 0
                first = next((r for r in range(target.rowCount())
                              if target.item(r, name_col) is not None
                              and target.item(r, name_col).flags() & Qt.ItemFlag.ItemIsEnabled),
                             None)
                if first is not None:
                    target.setCurrentCell(first, name_col)

    def _draw_crumbs(self, crumbs: list) -> None:
        while self.crumbs.count():
            item = self.crumbs.takeAt(0)
            widget = item.widget() if item is not None else None
            if widget is not None:
                widget.deleteLater()
        rtl = self.isRightToLeft()
        self.crumb_buttons = []

        def add_link(text: str, path: tuple):
            button = _LinkButton(own_direction(text, rtl), self.crumbs_box)
            button.clicked.connect(lambda _checked=False, p=path: self.go(p))
            self.crumbs.addWidget(button)
            self.crumb_buttons.append(button)

        add_link(tr("All libraries"), ())
        for k, (i, _nid, name) in enumerate(crumbs):
            # Not shared.shelfmark_tree_view.CRUMB_SEP: a browser mirrors that one character in right-to-left
            # text, but a QLabel takes its direction from its text, and a lone arrow has none, so it is drawn
            # as written: each language writes the arrow that points its way.
            self.crumbs.addWidget(QLabel('‹' if rtl else '›', self.crumbs_box))
            if k == len(crumbs) - 1:
                last = QLabel(own_direction(name, rtl), self.crumbs_box)
                font = last.font()
                font.setBold(True)
                last.setFont(font)
                self.crumbs.addWidget(last)
            else:
                add_link(name, tuple(self._drawn_path[:i + 1]))

    # -- multiple choice --------------------------------------------------------------------

    def _say(self, text: str) -> None:
        if self.note is not None:
            self.note.setText(text)

    def _change(self, fn) -> bool:
        """One change to the choice, ``fn(selection, members_of)``: made at once, on the UI thread,
        in the order made (two quick ticks both count); kept, then shown. While a confirm is with the
        host, what a change makes chosen is noted (``_since``): it stays chosen whatever the host takes
        (Clear, then the same volume again; the web's rule, Codex PR #70). Changes and the host's
        answer never interleave: both run on the UI thread, the worker only reads a frozen copy."""
        try:
            limit = self._limit
            watch = self._since is not None or limit is not None
            before = self._sel.chosen_set(self._members_of) if watch else None
            # with a limit the change is made on a copy, kept only when the limit allows what it adds
            # (removals always pass)
            trial = self._sel.copy() if limit is not None else self._sel
            changed = bool(fn(trial, self._members_of))
            after = trial.chosen_set(self._members_of) if changed and watch else None
            if changed and limit is not None:
                refusal = limit.refusal(before, after)
                if refusal is not None:
                    self._say(refusal_message(refusal, tr))
                    return False
                self._sel = trial
            if changed and self._since is not None:
                self._since |= after - before
        except Exception as exc:  # noqa: BLE001 -- a change that could not be read is not made
            logger.warning("Shelf picker: a change failed: %s", exc)
            self._say(tr("Not loaded"))
            return False
        if changed:
            if self._store is not None:
                self._store.save(self._purpose, self._sel)
            self._request_state()
        return changed

    def _toggle_leaf(self, leaf) -> None:
        """A shelfmark's box, read from the choice as it is now (never from a box drawn earlier)."""
        self._say('')
        self._change(lambda s, m, lf=leaf: s.drop_leaf(lf.sys_id, m) if lf.sys_id in s.chosen_set(m)
                     else s.add_leaf(lf, m))

    def _toggle_node(self, node_id: str, scope: Optional[str]) -> None:
        """A node's box: ticks all of it, or unticks it when all of it is chosen. ``scope`` None: the
        scope "Select all" takes over the node's shelfmarks (``scope_for``)."""
        too_big = []

        def flip(s, m):
            node = self._service.node(node_id)
            if node is None:
                return False
            how = scope or scope_for(node)
            limit = self._limit
            limit_n = (self._limit_n() if limit is not None and limit.node_too_big(node, how, self._service)
                       else None)
            if limit_n is not None and not s.has_node(node.id, how):
                # too big to tick whole: refused before its manuscripts are read (a library is
                # tens of thousands); one ticked whole from before stays to be unticked
                too_big.append(limit_n)
                return False
            if s.node_state(node, how, m) == 'all':
                return s.drop_node(node, how, m)
            return s.add_node(node, how, m)
        self._say('')
        self._change(flip)
        if too_big:
            self._say(tr("Too many to choose here (up to {n})").format(n=f"{too_big[0]:,}"))

    def _limit_reason(self) -> str:
        """Why what is still chosen was not taken, when it is the limit: what is left would pass it
        with what the host holds now (the desk after this confirm), else ''."""
        if self._limit is None:
            return ''
        try:
            refusal = self._limit.refusal((), self._sel.chosen_set(self._members_of))
        except Exception:  # noqa: BLE001 -- the note says what is left without the reason
            return ''
        return refusal_message(refusal, tr) if refusal is not None else ''

    def too_big(self, count) -> Optional[int]:
        """The limit a node of ``count`` manuscripts (known: a row's count, "Select all"'s total)
        passes alone (too big to tick whole), else None."""
        limit = self._limit
        if limit is None or not limit.count_too_big(count):
            return None
        return self._limit_n()

    def _limit_n(self) -> Optional[int]:
        """The number "Too many to choose here (up to {n})" says: the manuscripts the limit allows (a
        node of more than 1,000 manuscripts is past 30 first)."""
        limit = self._limit
        return limit.max_items if limit.max_items is not None else limit.max_pages

    def _toggle_row(self, column: "_NodeColumn", row: int) -> None:
        data = column.rows[row]
        if 'leaf' in data:
            self._toggle_leaf(data['leaf'])
        elif 'id' in data:
            self._toggle_node(data['id'], ALL)

    def _toggle_whole(self) -> None:
        leaves = self.leaf_panel.leaves
        if leaves is not None:
            self._toggle_node(leaves['node'], None)
        if self._paint_state is not None:          # the box shows the answer, not Qt's own turn of it
            self.leaf_panel.paint(self._paint_state)

    def _remove_chip(self, key: str) -> None:
        self._change(lambda s, _m, k=key: s.remove_chip(k))

    def _clear(self) -> None:
        self._say('')
        self._change(lambda s, _m: bool(len(s)) and (s.clear() or True))

    def _request_state(self) -> None:
        """What the boxes, chips and count show, read off the UI thread from a copy of the choice as
        it is now; an answer older than the newest request is never drawn."""
        self._sgen += 1
        gen = self._sgen
        sel = self._sel.copy()
        node_ids = tuple(nid for c in self.node_columns if c.isVisibleTo(self) and c.tickable for nid in c.boxes)
        panel = self.leaf_panel
        list_node = panel.leaves['node'] if panel.isVisibleTo(self) and panel.leaves is not None else None
        service, lang = self._service, self._lang
        self._state_loads.request(gen, lambda: paint_state(service, sel, node_ids, list_node, lang))
        self._footer()

    def _painted(self, gen: int, out) -> None:
        if gen != self._sgen or not self.multi:
            return                                 # a newer change or list asked again: its answer is drawn
        if isinstance(out, Exception):
            self._say(tr("Not loaded"))
            return
        self._paint_state = out
        for column in self.node_columns:
            if column.isVisibleTo(self):
                column.paint(out)
        self.leaf_panel.paint(out)
        self.details.paint(out)
        if self.look.isVisible():
            self.look.sync_choice()
        self.chips_bar.set_chips(out['chips'])
        if self._all_chips is not None:
            self._all_chips.fill(out)
        self._footer()

    def _view_changed(self) -> None:
        """The shelfmarks pane in another view: the whole dialog drawn again from the view on screen
        (the columns shown change, VX2), the boxes asked for again in the multiple choice."""
        if self._view is None:
            return
        self._draw(self._view)
        if self.multi:
            self._request_state()

    def _request_rows(self) -> None:
        """The table's rows that have no details yet, the first page of them read off the UI thread
        (``row_details``); when that job ends, what is still missing is worked out again (a "Show
        more", a filter or another node meanwhile), so no page is left blank. One job at a time."""
        if self._closing or self._rows_job is not None or self.leaf_panel.view != 'table':
            return
        leaves = self.leaf_panel.leaves
        if leaves is None:
            return
        batches = wanted([row['leaf'].sys_id for row in leaves['rows']], self._row_details)
        if not batches:
            return
        batch, lang = batches[0], self._lang
        meta, pages_of, texts_of = catalogue, page_count_of, page_texts_of
        self._rows_gen += 1
        self._rows_batch = batch
        job = _Job(self._rows_gen, lambda: row_details(batch, lang, meta_mgr=meta, page_count=pages_of,
                                                       page_texts=texts_of), QApplication.instance())
        job.done.connect(self._rows_done)
        job.finished.connect(self._rows_finished)
        self._rows_job = job
        job.start()

    @pyqtSlot(int, object)
    def _rows_done(self, _gen: int, out) -> None:
        if self._closing:
            return
        if not isinstance(out, dict):
            # the read failed (row_details guards each source, so something escaped them): its rows stay
            # blank and are not asked for again, or the loader would read the same batch without end
            out = {sid: {} for sid in getattr(self, '_rows_batch', ())}
        self._row_details.update(out)                # kept whatever the table shows now
        self.leaf_panel.fill_details(out)

    @pyqtSlot()
    def _rows_finished(self) -> None:
        job, self._rows_job = self._rows_job, None
        if job is not None:
            job.deleteLater()
        if not self._closing:
            self._request_rows()

    def _thumb_done(self, sys_id: str, image, width: int = THUMB_WIDTH) -> None:
        """A tile's picture (or none), on the UI thread: kept, and drawn on its tiles."""
        if self._closing:
            return
        if image is NOT_COME:
            self._thumb_not_come(sys_id)
            return
        self.thumb_known(sys_id, image, width)
        if image is not None and width < self.leaf_panel.fetch_width():
            # asked before the tiles grew (it was under way, so the larger size's queue left it out):
            # the tiles in sight are asked for again, this one sharper (Codex on #129)
            self.leaf_panel._thumb_timer.start()
        if self.details.leaf is not None and self.details.leaf.sys_id == sys_id and image is not None:
            self.details.show_known_image()

    def _thumb_not_come(self, sys_id: str) -> None:
        """A picture's fetch failed for now: its tile shows none, and it is asked for again in a while
        (``THUMB_RETRY_S``); after ``THUMB_TRIES`` it counts as none in this picker."""
        tries = self._thumb_tries.get(sys_id, (0, 0.0))[0] + 1
        self._thumb_tries[sys_id] = (tries, time.monotonic())
        if tries >= THUMB_TRIES:
            self.thumb_known(sys_id, None)
            return
        self.leaf_panel.show_picture(sys_id)
        if not self._retry_timer.isActive():
            self._retry_timer.start(int(THUMB_RETRY_S * 1000) + 50)

    def _retry_thumbs(self) -> None:
        """The pictures that did not come, asked for again: the tiles in sight and the details'."""
        if self._closing:
            return
        self.leaf_panel.queue_visible()
        leaf = self.details.leaf
        if leaf is not None and catalogue is not None and not self.known_thumb(leaf.sys_id)[0]:
            self._thumb_pool.fetch(leaf.sys_id)
        now = time.monotonic()                      # those that failed later than the first: their turn
        left = [THUMB_RETRY_S - (now - when) for sid, (tries, when) in self._thumb_tries.items()
                if tries < THUMB_TRIES and now - when < THUMB_RETRY_S]
        if left:
            self._retry_timer.start(int(min(left) * 1000) + 50)

    def known_thumb(self, sys_id: str) -> tuple:
        """``(known, QImage or None)``: a manuscript's picture as this dialog knows it (None: it has none),
        else as the run's cache has it (fetched by another picker or the pane); ``(False, None)``: not
        fetched yet."""
        if sys_id in self._thumbs:
            return True, self._thumbs[sys_id]
        image = cached_thumbnail(sys_id)
        if image is not None:
            self._thumbs[sys_id] = image
            return True, image
        return False, None

    def thumb_known(self, sys_id: str, image, width: int = THUMB_WIDTH) -> None:
        """A picture fetched (or none), from the gallery or the pane: kept for both, drawn on its tiles.
        A sharper one already here stays (a narrower answer that came late)."""
        image = image if isinstance(image, QImage) and not image.isNull() else None
        if image is None and self._thumbs.get(sys_id) is not None:
            return                                 # a sharper fetch failed: the picture shown stays
        keep_thumbnail(sys_id, image, width)
        self._thumbs[sys_id] = cached_thumbnail(sys_id) if image is not None else None
        if image is not None:
            self._thumb_tries.pop(sys_id, None)
        self.leaf_panel.show_picture(sys_id)

    def thumb_sharp(self, sys_id: str) -> bool:
        """The picture of a manuscript is here at the width the tiles want now (or it has none): not
        asked for again; nor is one that did not come just now, yet."""
        known, image = self.known_thumb(sys_id)
        tries, when = self._thumb_tries.get(sys_id, (0, 0.0))
        if tries and time.monotonic() - when < THUMB_RETRY_S:
            return True
        return known and (image is None or cached_width(sys_id) >= self.leaf_panel.fetch_width())

    def _load_signs(self) -> None:
        """The rows' signs, read off the UI thread when they are not loaded yet (once a process)."""
        source = signs
        if source is not None and not source.ready:
            self._signs_loads.request(0, source.load)

    def _signs_done(self, _gen: int, out) -> None:
        if isinstance(out, Exception):
            return
        for column in self.node_columns:
            column.show_signs()
        self.leaf_panel.show_signs()

    def _footer(self) -> None:
        if not self.multi:
            self.act_button.setEnabled(self._chosen_leaf is not None)
            for button in self.action_buttons.values():
                button.setEnabled(self._chosen_leaf is not None and not self._busy)
            return
        for button in self.action_buttons.values():
            button.setEnabled(bool(len(self._sel)) and not self._busy)
        if self._paint_state is not None:
            self.count_label.setText(tr('{n} manuscripts chosen').format(n=f"{self._paint_state['count']:,}"))
        has = bool(len(self._sel))
        if self.switch is not None:
            self.switch.setEnabled(not self._busy)
        self.confirm_button.setEnabled(has and not self._busy)
        self.clear_button.setEnabled(has and not self._busy)
        if self.save_button is not None:
            self.save_button.setEnabled(has and not self._busy)

    def _work(self, fn: Callable[[], object], then: Callable[[object], None]) -> None:
        """Blocking work off the UI thread (a confirm's or a save's reading, the prune); ``then(result)``
        runs for the work asked for last only."""
        self._wgen += 1
        self._work_then = (self._wgen, then)
        self._work_loads.request(self._wgen, fn)

    def _worked(self, gen: int, out) -> None:
        pending = self._work_then
        if pending is None or pending[0] != gen:
            return
        self._work_then = None
        pending[1](out)

    def _prune_on_open(self) -> None:
        """A kept choice is brought up to the tree as it is now (a rebuilt file: chips gone, a
        shelfmark written anew). Adopted only when the tree was read and nothing was changed meanwhile."""
        self._pruned_once = True
        if not len(self._sel):
            return
        before = self._sel.to_dict()
        kept = self._sel.copy()
        service = self._service
        self._work(lambda: kept.prune(service), lambda out: self._pruned(before, kept, out))

    def _pruned(self, before: dict, kept: Selection, out) -> None:
        if isinstance(out, Exception) or out is None:
            return                                 # the tree was not read: nothing is dropped
        if self._sel.to_dict() != before or kept.to_dict() == before:
            return
        self._sel = kept
        if self._store is not None:
            self._store.save(self._purpose, kept)
        self._request_state()

    def _resolving(self) -> Callable[[], tuple]:
        """The reading a confirm or a save hands on: the entries and a label naming the chips, from a
        copy of the choice as it is now (a whole collection is tens of thousands of shelfmarks)."""
        frozen = self._sel.copy()
        service, lang = self._service, self._lang
        return lambda: (frozen.resolve_entries(service), frozen.summary(chip_namer(service, lang)))

    def _run_action(self, action: "ShelfAction") -> None:
        """An action's button (DX22): on the shelfmark chosen, or on the ones ticked (read off the UI
        thread, as a confirm's are; they stay ticked)."""
        if self._busy:
            return
        if not self.multi:
            if self._chosen_leaf is not None:
                self._action_on(action, [self._chosen_leaf.as_selection()])
            return
        if not len(self._sel):
            return
        self._busy = True
        self._footer()
        self._work(self._resolving(), lambda out, a=action: self._action_resolved(a, out))

    def _action_resolved(self, action: "ShelfAction", out) -> None:
        self._busy = False
        self._footer()
        if isinstance(out, Exception):
            self._say(tr("Not loaded"))
            return
        entries, _label = out
        if entries:
            self._action_on(action, entries)

    def _action_on(self, action: "ShelfAction", entries: list) -> None:
        if not action.keep_open:
            self._outcome = (action.key, list(entries))
            self.accept()
            return
        try:
            note = action.handler(list(entries), self) if action.handler is not None else None
        except Exception:  # noqa: BLE001 -- the host's failure leaves the picker as it was
            logger.exception("Shelf picker: the action %s failed", action.key)
            note = tr('The choice could not be applied. It is kept; try again.')
        self._say(note or '')

    def _confirm(self) -> None:
        if self._busy or not len(self._sel):
            return
        self._busy = True
        self._since = set()                        # what is chosen from now on is not the host's to take
        self._say('')
        self._footer()
        self._work(self._resolving(), self._confirm_resolved)

    def _confirm_resolved(self, out) -> None:
        """Hand the host what was chosen when the confirm was pressed; what it takes leaves the
        choice, what it does not stays, and so does anything chosen again meanwhile. A failure keeps
        all."""
        close = False
        try:
            if isinstance(out, Exception):
                self._say(tr("Not loaded"))
                return
            entries, label = out
            if not entries:
                return
            try:
                outcome = self._on_confirm(entries, label)
            except Exception:  # noqa: BLE001 -- the host's failure keeps the choice
                logger.exception("Shelf picker: the host could not take the choice")
                self._say(tr('The choice could not be applied. It is kept; try again.'))
                return
            chosen_since, self._since = frozenset(self._since or ()), None
            if outcome is STAGED:                  # taken into the host's own step: kept until applied
                if self._on_staged is not None:
                    self._on_staged(chosen_since)  # the host must not consume what was chosen again
                close = True
                return
            submitted = [e['sys_id'] for e in entries]
            wanted = set(submitted)
            applied = submitted if outcome is None else [s for s in (outcome or ()) if s in wanted]
            # what the reader chose again while the host worked stays chosen: the host took the
            # choice it was sent, not the later one
            applied = [s for s in applied if s not in chosen_since]
            if applied:
                self._change(lambda s, m, a=tuple(applied): s.consume(a, m) or True)
            left = self._sel.count(self._members_of)
            if applied and not left:
                close = True
            elif left:
                note = tr('{n} are still chosen').format(n=f"{left:,}")
                why = self._limit_reason()
                self._say(f"{note} · {why}" if why else note)
        finally:
            self._since = None
            self._busy = False
            self._footer()
            if close:
                self.accept()

    def _save_as_list(self) -> None:
        if self._busy or self._lists_mgr is None or not len(self._sel):
            return
        self._busy = True
        self._say('')
        self._footer()
        self._work(self._resolving(), self._save_resolved)

    def _save_resolved(self, out) -> None:
        """D12: the choice as one of the reader's lists, through the desktop's own list path (a new
        list, then its items at once); it stays chosen."""
        try:
            if isinstance(out, Exception):
                self._say(tr("Not loaded"))
                return
            entries, label = out
            ids = [e['sys_id'] for e in entries]
            if not ids:
                return
            # A retry belongs to the list an earlier try made (the web's rule, Codex PR #73): that list
            # is in the lists already, and a second one of the same name would be saved with it once
            # storage works again. Its items are added again; those already in it are not repeated.
            retained = self._list_retry
            if retained is not None and self._list_exists(retained[0]):
                list_id, name = retained
            elif retained is not None:
                list_id, name = None, retained[1]   # deleted or in the trash meanwhile: made again, same name
            else:
                name, ok = QInputDialog.getText(self, tr('Save as a list'), tr('List name'), text=label[:120])
                name = (name or '').strip()
                if not ok or not name:
                    return
                list_id = None
            failing = getattr(self._lists_mgr, 'saves_failing', None)
            failing = failing if callable(failing) else (lambda: False)
            made = unwritten = False
            new: list = []
            try:
                if list_id is None:
                    list_id = self._lists_mgr.create_list(name)
                    made, unwritten = True, failing()
                    self._list_retry = (list_id, name)       # made: a failure below retries into it
                held = self._list_holds(list_id)
                new = [sid for sid in dict.fromkeys(ids) if sid not in held]
                if new:
                    self._lists_mgr.add_items_bulk(new, list_id, source='shelf')
            except Exception:  # noqa: BLE001 -- the choice stays; the reader is told
                logger.exception("Shelf picker: the list could not be saved")
                self._undo_list_try(list_id, new, made and unwritten)
                self._say(tr('The list could not be saved.'))
                return
            # create_list and add_items_bulk do not report a save that did not reach lists.pkl
            # (a read-only or full profile): the manager's own state does (Codex, PR #73)
            if failing():
                logger.warning("Shelf picker: the list %r was not written to lists.pkl", name)
                # what this try put in memory is taken out again, so a retry writes only the choice as
                # it is then; a list whose creation was not written goes too (Codex, PR #74)
                self._undo_list_try(list_id, new, made and unwritten)
                self._say(tr('The list could not be saved.'))
                return
            self._list_retry = None
            if self._on_list_saved is not None:
                try:
                    self._on_list_saved()
                except Exception as exc:  # noqa: BLE001 -- the list is saved; its panel catches up later
                    logger.warning("Shelf picker: the lists panel was not refreshed: %s", exc)
            self._say(tr('Saved as a list: {name}').format(name=name))
        finally:
            self._busy = False
            self._footer()

    def _list_holds(self, list_id) -> set:
        """The manuscripts a list holds now, as itself or as one of its pages or images."""
        read = getattr(self._lists_mgr, 'get_items_in_list', None)
        try:
            return {it.get('sys_id') for it in read(list_id)} if callable(read) else set()
        except Exception:  # noqa: BLE001 -- read as empty: add_items_bulk does not repeat an entry
            return set()

    def _undo_list_try(self, list_id, added, drop_list: bool) -> None:
        """A Save as a list that did not reach lists.pkl: the entries it added to the list are taken
        out of memory again (those the list held before stay), and a list whose creation was not
        written is dropped; a retry then makes it anew under the name it was given."""
        if list_id is None:
            return
        withdraw = getattr(self._lists_mgr, 'withdraw_from_list', None)
        discard = getattr(self._lists_mgr, 'discard_list', None)
        try:
            if drop_list and callable(discard):
                discard(list_id)
            elif added and callable(withdraw):
                withdraw(added, list_id)
        except Exception as exc:  # noqa: BLE001 -- the next successful save writes the store as it is
            logger.warning("Shelf picker: a failed list save was not undone: %s", exc)

    def _list_exists(self, list_id) -> bool:
        """The list an earlier try made is still one of the reader's lists: not deleted, and not in
        the trash either (delete_list keeps a list's id and sets ``deleted_at``; a retry into it
        would put the manuscripts in a hidden list, Codex PR #73)."""
        lists = getattr(self._lists_mgr, 'data', {}).get('lists') if isinstance(
            getattr(self._lists_mgr, 'data', None), dict) else None
        if lists is None:
            return True
        entry = lists.get(list_id)
        return isinstance(entry, dict) and not entry.get('deleted_at')

    def _show_all_chips(self) -> None:
        """What "+N more" opens: every chip, each with its remove button."""
        if self._all_chips is None:
            dialog = _AllChipsDialog(self)
            dialog.finished.connect(lambda _result, d=dialog: self._all_chips_closed(d))
            self._all_chips = dialog
        if self._paint_state is not None:
            self._all_chips.fill(self._paint_state)
        self._all_chips.open()

    def _all_chips_closed(self, dialog) -> None:
        if self._all_chips is dialog:
            self._all_chips = None
        dialog.deleteLater()


# -- the chips bar (D11) ----------------------------------------------------------------------

class _Chip(QFrame):
    """One chip: its name (cut at CHIP_MAX_WIDTH, whole on hover), a node's count, and a button that
    removes it."""

    def __init__(self, chip: dict, on_remove: Callable[[str], None], parent=None, rtl: bool = False):
        super().__init__(parent)
        self.key = chip['key']
        self.setObjectName("shelfChip")
        self.setStyleSheet("QFrame#shelfChip { border: 1px solid palette(mid); border-radius: 10px; }")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 1, 2, 1)
        layout.setSpacing(4)
        label = chip['label']
        self.name = QLabel(self)
        self.name.setText(self.name.fontMetrics().elidedText(own_direction(label, rtl), Qt.TextElideMode.ElideRight,
                                                             CHIP_MAX_WIDTH))
        self.name.setToolTip(label)
        layout.addWidget(self.name)
        self.count = None
        if chip['node']:
            self.count = QLabel(f"· {chip['count']:,}", self)
            layout.addWidget(self.count)
        self.remove = QToolButton(self)
        self.remove.setText("×")
        self.remove.setAutoRaise(True)
        tip = f"{tr('Remove')}: {label}"
        self.remove.setToolTip(tip)
        self.remove.setAccessibleName(tip)
        self.remove.clicked.connect(lambda _checked=False, k=self.key: on_remove(k))
        layout.addWidget(self.remove)


class _ChipsBar(QWidget):
    """D11: one chip per choice, from any library, on one line, in the order chosen; those that do not
    fit (and any beyond CHIPS_DRAWN) are behind "+N more", which opens the whole list."""

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog
        self.widgets: list = []
        self._extra = 0                            # chips not drawn at all (beyond CHIPS_DRAWN)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)
        self.hint = QLabel(tr('Tick shelfmarks, or a whole volume, to choose them.') if dialog._node_boxes
                           else tr('Tick shelfmarks to choose them.'), self)
        layout.addWidget(self.hint)
        self.more = QPushButton(self)
        self.more.setFlat(True)
        self.more.setAutoDefault(False)
        self.more.setVisible(False)
        self.more.clicked.connect(lambda _checked=False: dialog._show_all_chips())
        layout.addWidget(self.more)
        layout.addStretch(1)
        self.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Fixed)   # never widens the dialog

    def set_chips(self, chips: list) -> None:
        layout = self.layout()
        for widget in self.widgets:
            layout.removeWidget(widget)
            widget.deleteLater()
        self.widgets = []
        self._extra = max(0, len(chips) - CHIPS_DRAWN)
        rtl = self.isRightToLeft()
        for i, chip in enumerate(chips[:CHIPS_DRAWN]):
            widget = _Chip(chip, self.dialog._remove_chip, self, rtl)
            layout.insertWidget(i + 1, widget)     # after the hint
            self.widgets.append(widget)
        self.hint.setVisible(not chips)
        self.fit()

    def shown(self) -> list:
        return [w for w in self.widgets if not w.isHidden()]

    def hidden_count(self) -> int:
        return self._extra + sum(1 for w in self.widgets if w.isHidden())

    def fit(self) -> None:
        """As many chips as fit on the line, in order; the rest counted on "+N more"."""
        spacing = self.layout().spacing()
        needs = [w.sizeHint().width() + spacing for w in self.widgets]
        if not self._extra and sum(needs) <= self.width():
            shown = len(self.widgets)
        else:
            self.more.setText(tr('+{n} more').format(n=len(self.widgets) + self._extra))   # its widest text
            room = self.width() - self.more.sizeHint().width() - spacing
            shown, used = 0, 0
            for need in needs:
                if used + need > room:
                    break
                used += need
                shown += 1
        for i, widget in enumerate(self.widgets):
            widget.setHidden(i >= shown)
        hidden = len(self.widgets) - shown + self._extra
        self.more.setVisible(hidden > 0)
        if hidden:
            self.more.setText(tr('+{n} more').format(n=hidden))

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self.fit()


class _AllChipsDialog(QDialog):
    """What "+N more" opens: every chip, each with its remove button, and the count."""

    def __init__(self, picker: "ShelfPickerDialog"):
        super().__init__(picker)
        self.picker = picker
        self.setWindowTitle(tr('From the shelf'))
        self.resize(560, 420)
        layout = QVBoxLayout(self)
        self.total = QLabel(self)
        bold = self.total.font()
        bold.setBold(True)
        self.total.setFont(bold)
        layout.addWidget(self.total)
        area = QScrollArea(self)
        area.setWidgetResizable(True)
        self.holder = QWidget()
        self.rows = QVBoxLayout(self.holder)
        self.rows.addStretch(1)
        area.setWidget(self.holder)
        layout.addWidget(area, 1)
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Close, self)
        buttons.button(QDialogButtonBox.StandardButton.Close).setText(tr("Close"))
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
        self.chips: list = []

    def fill(self, state: dict) -> None:
        self.total.setText(tr('{n} manuscripts chosen').format(n=f"{state['count']:,}"))
        while self.rows.count() > 1:
            item = self.rows.takeAt(0)
            widget = item.widget() if item is not None else None
            if widget is not None:
                widget.deleteLater()
        self.chips = []
        rtl = self.isRightToLeft()
        for i, chip in enumerate(state['chips']):
            widget = _Chip(chip, self.picker._remove_chip, self.holder, rtl)
            self.rows.insertWidget(i, widget)
            self.chips.append(widget)

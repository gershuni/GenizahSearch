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
import logging
import unicodedata
from typing import Callable, Optional

from PyQt6.QtCore import QEvent, QObject, QRect, QSize, Qt, QThread, QTimer, pyqtSignal, pyqtSlot
from PyQt6.QtGui import (
    QColor, QFont, QFontMetrics, QImage, QPalette, QPixmap, QTextBlockFormat, QTextCharFormat, QTextCursor,
)
from PyQt6.QtWidgets import (
    QAbstractItemView, QApplication, QCheckBox, QDialog, QDialogButtonBox, QFrame, QHBoxLayout,
    QHeaderView, QInputDialog, QLabel, QLineEdit, QListView, QListWidget, QListWidgetItem, QPushButton,
    QScrollArea, QSizePolicy, QStyle, QStyledItemDelegate, QStyleOption, QStyleOptionViewItem,
    QTableWidget, QTableWidgetItem, QTextEdit, QToolButton, QVBoxLayout, QWidget,
)

import genizah_core
from desktop.widgets.flow_layout import FlowLayout
from genizah_core import tr
from shared.shelfmark_tree_columns import NODE_ROWS, WIDE_COLUMNS, leaf_tooltip, load_view, passed_through, settle
from shared.shelfmark_tree_details import add_joins, field_lines, item_details, thumbnail_url
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


# -- the lists --------------------------------------------------------------------------------

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
            # the chosen row was scrolled to at the old heights: in sight again at the new ones,
            # once the table has its scroll range for them (it takes them in on the next turn)
            QTimer.singleShot(0, self._show_chosen)

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
    """The shelfmarks of the node chosen last: a title, a filter, the grid and "Show more". In the
    multiple-choice mode each shelfmark has a box, and "Select all in <node>" sits above the grid."""

    def __init__(self, dialog: "ShelfPickerDialog"):
        super().__init__(dialog)
        self.dialog = dialog
        self.key: Optional[str] = None
        self.leaves: Optional[dict] = None
        self.chosen: Optional[QListWidgetItem] = None
        self.items_by_sid: dict = {}               # sys_id -> grid items (multiple choice)
        self._need = LEAF_MIN_WIDTH
        self._row_height = self.fontMetrics().height() + 12
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        self.title = QLabel()
        self.title.setWordWrap(True)
        bold = self.title.font()
        bold.setBold(True)
        self.title.setFont(bold)
        self.filter = _FilterBox(dialog._filter_typed, dialog._filter_load, self)
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
        self.grid.itemClicked.connect(lambda item: dialog._clicked_leaf(item))
        self.grid.installEventFilter(dialog)
        self.grid.viewport().installEventFilter(dialog)
        self.grid.viewport().installEventFilter(self)
        self.more = QPushButton()
        self.more.setAutoDefault(False)
        self.more.clicked.connect(lambda: dialog._show_more(self.key, PAGE_SIZE))
        layout.addWidget(self.title)
        layout.addWidget(self.filter)
        layout.addWidget(self.whole)
        layout.addWidget(self.grid, 1)
        layout.addWidget(self.more)

    def eventFilter(self, obj, event):
        if obj is self.grid.viewport() and event.type() == QEvent.Type.Resize:
            self.fit()
        return False

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
        return self.grid.verticalScrollBar().value()

    def title_text(self, leaves: dict) -> str:
        return f"{tr('Shelfmarks')} ({leaves['total']:,}) — {own_direction(leaves['title'], self.isRightToLeft())}"

    def show_leaves(self, leaves: dict) -> None:
        self.key = leaves['filter_key']
        if self.filter.key != self.key:
            self.filter.key = self.key
            self.filter.show_text(self.dialog._filters.get(self.key, ''))
        self.fill(leaves)

    def fill(self, leaves: dict) -> None:
        self.leaves = leaves
        self.title.setText(self.title_text(leaves))
        rtl = self.isRightToLeft()
        highlight = self.dialog._highlight
        multi = self.dialog.multi
        known = self.dialog._paint_state if multi else None    # the last answer, until the next one
        grid = self.grid
        grid.setUpdatesEnabled(False)
        grid.clear()
        bold = QFont(grid.font())
        bold.setBold(True)
        self.chosen = None
        self.items_by_sid = {}
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
                item.setFlags(_BOX_FLAGS)
                item.setCheckState(_CHECK['all' if known is not None and leaf.sys_id in known['chosen'] else 'none'])
                self.items_by_sid.setdefault(leaf.sys_id, []).append(item)
            if leaf.id == highlight:
                item.setFont(bold)
                self.chosen = item
            grid.addItem(item)
            labels.append(row['label'])
        if multi and (known is None or (known.get('whole') or {}).get('id') != leaves['node']):
            self.whole.setVisible(False)          # until the answer for this node says what it holds
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
        more = leaves.get('more') or 0
        self.more.setText(f"{tr('Show more')} ({more:,})")
        self.more.setVisible(bool(more))

    def show_signs(self) -> None:
        """The signs, now loaded, on the shelfmarks drawn before."""
        rows = (self.leaves or {}).get('rows') or []
        for i, row in enumerate(rows[:self.grid.count()]):
            marks = _signs_of(row['leaf'].sys_id)
            if marks:
                item = self.grid.item(i)
                item.setData(_SIGNS_ROLE, marks)
                item.setToolTip(_with_signs(leaf_tooltip(row) or row['label'], marks))

    def mark(self, leaf) -> None:
        """The shelfmark a click chose (single choice), shown chosen: the grid's current item."""
        for i in range(self.grid.count()):
            item = self.grid.item(i)
            if getattr(item.data(_ROW), 'id', None) == leaf.id:
                self.grid.setCurrentItem(item)
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
        self.grid.doItemsLayout()
        if saved is not None:
            self.grid.verticalScrollBar().setValue(saved)
        elif self.chosen is not None:
            self.grid.scrollToItem(self.chosen, QAbstractItemView.ScrollHint.PositionAtCenter)
        else:
            self.grid.scrollToTop()


def _box_off(item: QTableWidgetItem, off: bool, tip: str, limit_tip: str) -> None:
    """A node's box, off (too big to tick whole, not ticked) or on, with the tooltip that says why."""
    flags = _BOX_FLAGS & ~Qt.ItemFlag.ItemIsEnabled if off else _BOX_FLAGS
    if item.flags() != flags:
        item.setFlags(flags)
    item.setToolTip(limit_tip if off else tip)


def _thumbnail_image(sys_id: str, meta_mgr) -> Optional[QImage]:
    """The manuscript's first image, small, as a QImage (made on the worker: never a QPixmap there),
    or None. Asks NLI's image server through its circuit breaker, as the Join Workbench's thumbnails
    do (``desktop/join_workbench.py``)."""
    url = thumbnail_url(sys_id, meta_mgr)
    if not url:
        return None
    from shared.nli_circuit_breaker import (
        NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT, is_open, record_failure, record_success,
    )
    from shared.nli_fetch import is_nli_host, nli_image_get
    nli = is_nli_host(url)
    if nli and is_open():                          # NLI down: no network call
        return None
    try:
        resp = nli_image_get(url, headers=genizah_core.Config.HTTP_HEADERS,
                             timeout=(NLI_CONNECT_TIMEOUT, NLI_IMAGE_READ_TIMEOUT))
    except Exception:  # noqa: BLE001 -- no image, never a crash
        if nli:
            record_failure(failure_type='connection_error', path='shelf_picker_thumb')
        return None
    if resp.status_code != 200:
        if nli and (resp.status_code == 429 or 500 <= resp.status_code < 600):
            record_failure(failure_type='429' if resp.status_code == 429 else '5xx', path='shelf_picker_thumb')
        return None
    if nli:
        record_success(path='shelf_picker_thumb')
    image = QImage()
    image.loadFromData(resp.content)
    return None if image.isNull() else image


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
    IMAGE_HEIGHT = 180
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
        self.text: Optional[dict] = None           # item_details' answer for the shelfmark shown
        self.join_boxes: dict = {}                 # sys_id -> a join partner's box
        self.join_links: dict = {}                 # sys_id -> a join partner's link
        self._places: dict = {}                    # sys_id -> its first place's ShelfLeaf, or None
        self._text_loads = _Loads(self._text_done, self)
        self._image_loads = _Loads(self._image_done, self)
        self._community_loads = _Loads(self._community_done, self)
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
        self.image = QLabel(body)
        self.image.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image.setVisible(False)
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
        self.joins_box = QWidget(body)
        self.joins_rows = QVBoxLayout(self.joins_box)
        self.joins_rows.setContentsMargins(0, 0, 0, 0)
        self.joins_rows.setSpacing(2)
        self.joins_box.setVisible(False)
        self.links = self._rich_label(body)
        self.links.setOpenExternalLinks(True)
        for widget in (self.empty, self.image, self.shelfmark, self.fields['others'], self.fields['title'],
                       self.textbox, self.lines_box, self.joins_title, self.joins_box, self.links):
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
        """Show a shelfmark: its name at once, the rest when read."""
        if leaf is None:
            return
        self._gen += 1
        gen = self._gen
        self.leaf_id = leaf.id
        self.leaf = leaf
        self.text = None
        self.empty.setVisible(False)
        self.image.clear()
        self.image.setVisible(False)
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
        if meta is not None and sid:
            self._image_loads.request(gen, lambda: _thumbnail_image(sid, meta))

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

    def _image_done(self, gen: int, out) -> None:
        if gen != self._gen or not isinstance(out, QImage) or out.isNull():
            return
        pixmap = QPixmap.fromImage(out).scaled(self.WIDTH - 40, self.IMAGE_HEIGHT, Qt.AspectRatioMode.KeepAspectRatio,
                                               Qt.TransformationMode.SmoothTransformation)
        self.image.setPixmap(pixmap)
        self.image.setVisible(True)

    def stop(self) -> None:
        # the text is local reads of the tree and the sidecars: waited for, as the lists' loads are (the
        # tree must not be read after its dialog has gone); the image is not, it may take seconds to
        # come over the network (Codex pre-flight: never freeze the close) and it reads no tree
        self._text_loads.stop()
        self._image_loads.stop(wait_ms=0)
        self._community_loads.stop(wait_ms=0)


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
        self.details.setVisible(self.width() >= self.DETAILS_MIN_WIDTH)

    def done(self, result):
        self.details.stop()
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
            if obj is self.leaf_panel.grid.viewport() and not self.multi:
                item = self.leaf_panel.grid.itemAt(event.position().toPoint())
                if item is not None:
                    self._pick(item.data(_ROW))
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
        # Enter and Space open the row or pick the shelfmark that has the keyboard's focus.
        if kind == QEvent.Type.KeyPress and event.key() in (
                Qt.Key.Key_Return, Qt.Key.Key_Enter, Qt.Key.Key_Space):
            if obj is self.leaf_panel.grid:
                item = obj.currentItem()
                if item is not None:
                    # single choice: Space chooses, Enter takes (the act button); multiple: its box
                    if event.key() == Qt.Key.Key_Space:
                        self._select(item.data(_ROW))
                    else:
                        self._pick(item.data(_ROW))
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

    def _viewports(self) -> tuple:
        return (*(c.table.viewport() for c in self.node_columns), self.leaf_panel.grid.viewport())

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
                self._pick(data['leaf'])
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
        if 'path' in out:
            self._path = tuple(out['path'])
            self._shown = dict(out['shown'])
            if out['leaf'] is not None:
                self._highlight = out['leaf'].id
        view = out['view']
        self._drawn_path = tuple(out['for_path'])
        owed, self._owed = self._owed, set()
        if not self._whole and owed and self._draw_lists(view, owed):
            self._view = view
        else:
            self._whole = False
            self._draw(view)
        if out.get('reveal') and out.get('leaf') is not None:
            self._show_revealed(out['leaf'])
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
        on_screen = self._on_screen()
        self._remember_scroll()
        plan = []
        for key in owed:
            col = next((c for c in view['columns'][-WIDE_COLUMNS:] if c['filter_key'] == key), None)
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
        columns = view['columns'][-WIDE_COLUMNS:]
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
        if self._chosen_leaf is not None:          # the shelfmark chosen stays shown chosen
            self.leaf_panel.mark(self._chosen_leaf)
        if self._focus_after:
            self._focus_after = False
            self._focus_deepest(view)

    def _focus_deepest(self, view: dict) -> None:
        """The keyboard goes where the next choice is: the chosen node's children, else its shelfmarks."""
        target, owner = None, None
        for column in self.node_columns:
            if column.isVisibleTo(self) and column.col and column.col['depth'] == len(self._path):
                target, owner = column.table, column
        if target is None and self.leaf_panel.isVisibleTo(self):
            target = self.leaf_panel.grid
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
        self.chips_bar.set_chips(out['chips'])
        if self._all_chips is not None:
            self._all_chips.fill(out)
        self._footer()

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

"""Shared desktop UI widgets and helper functions."""

import re

from PyQt6.QtWidgets import QWidget, QHBoxLayout, QTextEdit, QCompleter
from PyQt6.QtCore import Qt, QRect
from PyQt6.QtGui import QColor, QTextCursor, QTextCharFormat

from genizah_core import tr


class ActionsHoverWidget(QWidget):
    def __init__(self, parent=None, alignment=Qt.AlignmentFlag.AlignCenter):
        super().__init__(parent)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(2, 2, 2, 2)
        layout.setSpacing(4)
        layout.setAlignment(alignment)
        self.buttons = []
        self.always_visible_buttons = set()

    def add_btn(self, btn, always_visible=False):
        self.layout().addWidget(btn)
        self.buttons.append(btn)
        if always_visible:
            self.always_visible_buttons.add(btn)
            btn.setVisible(True)
        else:
            btn.setVisible(False)

    def set_buttons_visible(self, visible):
        for b in self.buttons:
            if b in self.always_visible_buttons:
                b.setVisible(True)
            else:
                b.setVisible(visible)


def _format_add_to_list_label(in_list=False):
    star = "\u2b50" if in_list else "\u2606"
    return f"{star} {tr('List')}"


def apply_find_highlight(text_browser, query):
    if not text_browser:
        return
    if not query:
        text_browser.setExtraSelections([])
        return
    doc = text_browser.document()
    cursor = QTextCursor(doc)
    highlight_format = QTextCharFormat()
    highlight_format.setBackground(QColor("#fff59d"))
    selections = []
    while True:
        cursor = doc.find(query, cursor)
        if cursor.isNull():
            break
        selection = QTextEdit.ExtraSelection()
        selection.cursor = cursor
        selection.format = highlight_format
        selections.append(selection)
    text_browser.setExtraSelections(selections)


def _get_folio_number_from_shelfmark(shelfmark):
    """Extract folio number from Oxford-style shelfmarks only.

    Oxford shelfmarks like "MS. Heb. a. 1/1" or "Bodl. Or. 12/3" contain
    actual folio numbers after the slash. Other libraries (Cambridge, NLI, etc.)
    use classmarks where trailing numbers are not folio references.
    """
    if not shelfmark:
        return None
    upper = shelfmark.upper()
    # Only extract folio from Oxford-style shelfmarks (MS. Heb., Bodl., etc.)
    is_oxford = (
        'MS. HEB' in upper or
        'MS HEB' in upper or
        upper.startswith('BODL') or
        'BODLEIAN' in upper
    )
    if not is_oxford:
        return None
    match = re.search(r'[/.](\d+)\s*$', shelfmark)
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _get_folio_image_index(meta, folio_num, side_offset=0):
    base_idx = _get_initial_image_index(meta, folio_num)
    if side_offset <= 0:
        return base_idx

    images = (meta or {}).get('images_ext') or (meta or {}).get('images') or []
    if not images or base_idx >= len(images):
        return base_idx

    target_folio = images[base_idx].get('folio_num')
    if target_folio is None:
        return base_idx

    label = str(images[base_idx].get('label', '')).lower()
    if label.endswith('b'):
        return base_idx

    next_idx = base_idx + 1
    if next_idx < len(images) and images[next_idx].get('folio_num') == target_folio:
        return next_idx

    for idx, img in enumerate(images):
        if img.get('folio_num') == target_folio and str(img.get('label', '')).lower().endswith('b'):
            return idx

    return base_idx


def _get_folio_side_image_index(meta, folio_num, side):
    """Return images_ext index matching both (folio_num, side), or None.

    This is the side-aware sibling of _get_folio_image_index. Unlike the
    legacy helper (which falls back to the nearest folio when an exact
    index is not available), this returns None when no exact (folio_num,
    side) match exists — so callers can trigger an NLI fallback.

    'side' is 'r' or 'v'. A canvas whose folio_side is None (e.g. a bare
    numeric CUDL label '1' with no r/v suffix) matches only when side
    is 'r' (bare-numeric = recto convention).

    Args:
        meta: Dict carrying 'images_ext' (preferred) or 'images' list.
        folio_num: Integer folio number to match. None returns None.
        side: 'r' or 'v'. Any other value returns None.

    Returns:
        int index into meta['images_ext'] (or 'images'), or None.
    """
    if not meta or folio_num is None or side not in ('r', 'v'):
        return None
    images = (meta or {}).get('images_ext') or (meta or {}).get('images') or []
    # 1. Exact (folio_num, side) match.
    for idx, img in enumerate(images):
        if img.get('folio_num') == folio_num and img.get('folio_side') == side:
            return idx
    # 2. Side-less canvas (folio_side None) matches recto only.
    if side == 'r':
        for idx, img in enumerate(images):
            if img.get('folio_num') == folio_num and not img.get('folio_side'):
                return idx
    return None


def _get_initial_image_index(meta, page_num):
    if page_num is None:
        return 0
    try:
        p_num = int(page_num)
    except (TypeError, ValueError):
        return 0

    images = (meta or {}).get('images_ext') or (meta or {}).get('images') or []
    folio_entries = []
    for idx, img in enumerate(images):
        folio_num = img.get('folio_num')
        if folio_num is None:
            continue
        try:
            folio_entries.append((idx, int(folio_num)))
        except (TypeError, ValueError):
            continue

    if not folio_entries:
        return max(p_num - 1, 0)

    for idx, folio_num in folio_entries:
        if folio_num == p_num:
            return idx

    prior = [(idx, folio_num) for idx, folio_num in folio_entries if folio_num <= p_num]
    if prior:
        return max(prior, key=lambda pair: pair[1])[0]

    return min(folio_entries, key=lambda pair: pair[1])[0]


class ShelfmarkCompleter(QCompleter):
    """
    Custom Completer that normalizes input before matching.
    Input "T-S" -> Normalized "ts" -> Matches model items where UserRole starts with "ts".
    """
    def __init__(self, model, parent=None, valid_keys=None):
        super().__init__(model, parent)
        self.valid_keys = valid_keys or set()

    @staticmethod
    def normalize(text):
        t = re.sub(r'^\s*m[\.\s]*s[\.\s]*\.?\s*', '', text, flags=re.IGNORECASE)
        return re.sub(r"[^\w\./]", "", t).lower()

    def splitPath(self, path):
        return [self.normalize(path)]

    def pathFromIndex(self, index):
        # Return the pretty display text when an item is selected
        return index.data(Qt.ItemDataRole.DisplayRole)

    def complete(self, rect=QRect()):
        # Hide popup if there is an exact match
        text = self.widget().text()
        norm = self.normalize(text)
        if norm in self.valid_keys:
            self.popup().hide()
            return
        super().complete(rect)

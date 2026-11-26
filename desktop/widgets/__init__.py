"""Shared desktop UI widgets and helper functions."""

import html
import re

from PyQt6.QtWidgets import QWidget, QHBoxLayout, QLabel, QMenu, QTextEdit, QToolButton, QCompleter
from PyQt6.QtCore import Qt, QEvent, QRect, QUrl
from PyQt6.QtGui import QColor, QDesktopServices, QTextCursor, QTextCharFormat

from genizah_core import tr
from shared.codicological import get_bodleian_links
from shared.url_hosts import CAMBRIDGE_HOSTS, is_on_host


class LibraryLinkButton(QToolButton):
    """Open the holding library's item, or choose a Bodleian folio side or volume."""

    def __init__(self, icon_text=''):
        super().__init__()
        self._icon_text = icon_text
        self._item_url = ''
        self._library_menu = QMenu(self)
        self._library_menu.aboutToShow.connect(self._fill_library_menu)
        self.clicked.connect(self._open_item)
        self.setToolTip(tr('Open in external library website'))
        self.hide()

    def set_record(self, meta, *, part_meta=None, shelfmark='', provider='', position=None):
        provider = provider or meta.get('external_provider')
        self._part = part_meta or meta.get('oxford_part_metadata') or {}
        self._katz_components = meta.get('katz_component_links') or []
        self._shelfmark = shelfmark or meta.get('shelfmark') or ''
        self._folio_images = meta.get('folio_images') or []
        self._position = position
        # The Cambridge URL has already passed the catalogue identity check.
        self._item_url = meta.get('external_url') or ''
        viewer = meta.get('library_viewer_url') or {}
        if viewer.get('label') == 'Katz Center':
            provider = 'katz'
        if provider in ('manchester', 'jts', 'katz'):
            self._item_url = viewer.get('url') or self._item_url

        if self._part or self._katz_components:
            self.setText(self._icon_text + tr('Katz Center' if self._katz_components else 'Bodleian'))
            self.setMenu(self._library_menu)
            self.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
            self._fill_library_menu()
            self.setVisible(bool(self._library_menu.actions()))
        else:
            self.setMenu(None)
            self.setPopupMode(QToolButton.ToolButtonPopupMode.DelayedPopup)
            if is_on_host(self._item_url, CAMBRIDGE_HOSTS):
                provider = 'cambridge'
            label = {
                'cambridge': 'Cambridge', 'oxford': 'Oxford', 'katz': 'Katz Center',
                'manchester': 'Manchester LUNA', 'jts': 'Princeton Digital Library',
            }.get(provider, 'External Website')
            self.setText(self._icon_text + tr(label))
            self.setVisible(bool(self._item_url))

    def _fill_library_menu(self):
        menu = self._library_menu
        menu.clear()
        if self._katz_components:
            entries = [(item['label'], item['url']) for item in self._katz_components]
        else:
            position = self._position() if self._position else {}
            urls = get_bodleian_links(self._part, self._shelfmark, folio_images=self._folio_images, **position)
            if urls.get('current') in (urls.get('recto'), urls.get('verso')):
                urls.pop('current', None)
            part_url = self._part.get('direct_link')
            if part_url and part_url not in urls.values():
                urls['part'] = part_url
            labels = (('recto', 'Recto'), ('verso', 'Verso'), ('current', 'Open in Bodleian Libraries'),
                      ('volume', 'Whole volume'), ('part', 'Bodleian part'))
            entries = [(tr(label), urls[key]) for key, label in labels if urls.get(key)]
        for label, url in entries:
            action = menu.addAction(label)
            action.triggered.connect(lambda _checked=False, u=url: QDesktopServices.openUrl(QUrl(u)))

    def _open_item(self):
        if self.menu() is None and self._item_url:
            url = self._item_url
            if 'cudl.lib.cam.ac.uk/iiif/' in url:
                url = url.replace('/iiif/', '/view/')
            QDesktopServices.openUrl(QUrl(url))


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


#: Search-hit markers. A `*...*` pair is placed around every regex match by the
#: render paths (browse_render_page, ResultDialog.load_page, ...) BEFORE the text
#: becomes HTML.
_HIGHLIGHT_MARKER_RE = re.compile(r'\*(.*?)\*')


def mark_pattern_hits(text, pattern_str):
    """Wrap every ``pattern_str`` match in ``text`` with ``*...*`` markers.

    The one implementation of the flag heuristic the render paths had each
    open-coded: ``re.MULTILINE`` is added when the pattern carries an explicit
    newline or anchors, because those patterns are written against a whole
    multi-line page. (``MULTILINE`` only affects ``^``/``$``; a search pattern
    still spans a line break through its ``[^\\w...]+`` word separator, which
    matches ``\\n`` like any other non-word character.)

    Literal asterisks in the source text are neutralized to spaces FIRST.
    ``*`` is the marker, so a page that contains one of its own -- a copied
    footnote marker, say -- would otherwise pair up with an inserted marker
    and bold the wrong span, or strand a stray asterisk in the render. This
    is the rule ``shared/search_engine.mark_word_highlights`` already
    follows (Codex round 6 on PR #325, for the same reason in xlsx export);
    ``.replace`` is length-preserving, so nothing downstream shifts.

    Returns ``text`` unchanged -- literal asterisks and all -- when there is
    no pattern, the pattern does not compile, or nothing matched. Text is
    only ever neutralized when a marker is actually being inserted into it,
    so a page with no hit is displayed exactly as it is stored.
    """
    if not text or not pattern_str:
        return text
    flags = re.IGNORECASE
    if '\\n' in pattern_str or pattern_str.startswith('^') or '^\\' in pattern_str:
        flags |= re.MULTILINE
    try:
        marked, hits = re.compile(pattern_str, flags).subn(
            r'*\g<0>*', text.replace('*', ' '))
    except re.error:
        return text
    return marked if hits else text


def text_has_pattern_markers(text, pattern_str):
    """Does ``text`` already carry markers this pattern put there?

    A bare ``'*' in text`` cannot answer that: a page holding a literal
    asterisk of its own would be mistaken for marked text and skipped,
    losing the highlight entirely. A marker pair whose CONTENT the search
    pattern matches end-to-end is our own work; anything else is source
    text (Codex P2, PR #334).
    """
    if not text or not pattern_str:
        return False
    try:
        rx = re.compile(pattern_str, re.IGNORECASE | re.DOTALL)
    except re.error:
        return False
    return any(rx.fullmatch(m.group(1))
               for m in _HIGHLIGHT_MARKER_RE.finditer(text))


def text_lines_to_html(text):
    """Escape page text for ``setHtml`` and turn its newlines into ``<br>``.

    Escaping comes FIRST: Qt's rich-text parser takes every ``<...>`` in the
    text for a tag and drops it with its contents, and the texts carry them --
    FGP's ``<רא>`` line fillers, a PGP editor's added letter in ``מיב<מ>נא``,
    whatever a reader typed into a correction. ``*`` has no HTML meaning, so
    the search-hit markers survive for ``markers_to_bold_html`` afterwards.
    ``ResultDialog._htmlify`` already did this; the Browse paths and the PGP/FGP
    paths did not (found 2026-10-26).
    """
    if not text:
        return text
    return html.escape(text, quote=False).replace('\n', '<br>')


def markers_to_bold_html(html_text, color='red'):
    """Turn `*...*` search-hit markers into red bold.

    Call this on text that has ALREADY had its newlines turned into ``<br>``.
    The regex's ``.`` never matches a literal newline, so a hit that spans a
    line break -- which ``highlight_pattern`` regularly produces, e.g.
    ``אמר\\s*\\n?\\s*רבי`` -- only pairs up once the newline is a ``<br>``.

    Mirrors ``ResultDialog._htmlify`` step 3 exactly; the desktop Browse tab
    had no equivalent at all, so its search hits rendered as literal asterisks
    (found 2026-09-03).
    """
    if not html_text:
        return html_text
    return _HIGHLIGHT_MARKER_RE.sub(
        f"<b style='color:{color};'>\\1</b>", html_text)


#: "Find in text": every match, and the one the arrows are on.
FIND_MATCH_COLOR = "#fff59d"
FIND_CURRENT_COLOR = "#ff9632"


def _find_cursors(doc, query):
    """Every match of ``query`` in ``doc``, in order, as selecting cursors."""
    cursors, cursor = [], QTextCursor(doc)
    while True:
        cursor = doc.find(query, cursor)
        if cursor.isNull():
            return cursors
        cursors.append(cursor)


def _paint_find_matches(text_browser, cursors, current=-1):
    selections = []
    for i, cursor in enumerate(cursors):
        fmt = QTextCharFormat()
        fmt.setBackground(QColor(FIND_CURRENT_COLOR if i == current else FIND_MATCH_COLOR))
        selection = QTextEdit.ExtraSelection()
        selection.cursor = cursor
        selection.format = fmt
        selections.append(selection)
    text_browser.setExtraSelections(selections)


def apply_find_highlight(text_browser, query):
    """Mark every match of ``query`` in ``text_browser``. The render paths call
    this after putting new text in; when a ``FindNavigator`` is attached to the
    view it does the marking, so its count and arrows follow the new text."""
    if not text_browser:
        return
    navigator = getattr(text_browser, '_find_navigator', None)
    if navigator is not None:
        navigator.refresh()
        return
    if not query:
        text_browser.setExtraSelections([])
        return
    _paint_find_matches(text_browser, _find_cursors(text_browser.document(), query))


class FindNavigator(QWidget):
    """The way through the matches of a "Find in text" box (asked for by a
    reader, 2026-11-17: in a large manuscript the marked matches are pages
    apart): a count ("3/12"), the match it is on in orange, and the arrows --
    the buttons, Enter / Shift+Enter and F3 / Shift+F3 in the box.

    Typing goes to the first match from the top of what is on screen, scrolling
    only when it is not in view. New text in the view (another page, another
    version, View All) is marked again with no match chosen ("0/12"), so the
    window stays where the render put it -- at a search hit, say -- until an
    arrow is used. The text cursor is never moved: the view only scrolls.

    Build it with the box, put it in the box's row, and ``attach`` the view
    once it exists."""

    def __init__(self, line_edit, parent=None):
        super().__init__(parent)
        self.line_edit, self.view = line_edit, None
        self.matches, self.current = [], -1
        self._stale = False
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        self.count_label = QLabel()
        layout.addWidget(self.count_label)
        self.btn_prev, self.btn_next = QToolButton(), QToolButton()
        self.btn_prev.setArrowType(Qt.ArrowType.UpArrow)
        self.btn_next.setArrowType(Qt.ArrowType.DownArrow)
        for button, name, keys, slot in (
                (self.btn_prev, tr("Previous match"), "Shift+Enter", self.previous),
                (self.btn_next, tr("Next match"), "Enter", self.next)):
            button.setToolTip(f"{name} ({keys})")
            button.setAccessibleName(name)
            button.clicked.connect(slot)
            layout.addWidget(button)
        line_edit.textChanged.connect(self._on_text_changed)
        line_edit.installEventFilter(self)
        self._update_controls()

    def attach(self, view):
        self.view = view
        view._find_navigator = self
        view.document().contentsChanged.connect(self._mark_stale)
        self.refresh()

    def query(self):
        return self.line_edit.text().strip()

    def refresh(self):
        """New text in the view: mark its matches, none of them chosen."""
        self.current = -1
        self._mark()

    def next(self):
        self._step(1)

    def previous(self):
        self._step(-1)

    # -- inside ---------------------------------------------------------------

    def eventFilter(self, obj, event):
        if obj is self.line_edit and event.type() == QEvent.Type.KeyPress and event.key() in (
                Qt.Key.Key_Return, Qt.Key.Key_Enter, Qt.Key.Key_F3):
            # Taken here, so a dialog's default button never gets the Enter.
            self._step(-1 if event.modifiers() & Qt.KeyboardModifier.ShiftModifier else 1)
            return True
        return super().eventFilter(obj, event)

    def _mark_stale(self):
        self._stale = True

    def _mark(self):
        query = self.query()
        if self.view is not None:
            self.matches = _find_cursors(self.view.document(), query) if query else []
            self._stale = False
            if self.current >= len(self.matches):
                self.current = -1
            _paint_find_matches(self.view, self.matches, self.current)
        self._update_controls()

    def _on_text_changed(self, _text):
        self.current = -1
        self._mark()
        if self.matches:
            self._go(self._from_screen(forward=True))

    def _step(self, direction):
        if self._stale:                     # the text changed under the marks
            self.refresh()
        if not self.matches:
            return
        if self.current < 0:
            self._go(self._from_screen(forward=direction > 0))
        else:
            self._go((self.current + direction) % len(self.matches))

    def _from_screen(self, forward):
        """The first match not above what is on screen, or (backwards) the last
        one not below it; past the end, round to the other end."""
        height = self.view.viewport().height()
        rects = [self.view.cursorRect(c) for c in self.matches]
        if forward:
            return next((i for i, r in enumerate(rects) if r.bottom() >= 0), 0)
        return next((i for i in range(len(rects) - 1, -1, -1) if rects[i].top() <= height),
                    len(rects) - 1)

    def _go(self, index):
        self.current = index
        _paint_find_matches(self.view, self.matches, index)
        self._update_controls()
        rect = self.view.cursorRect(self.matches[index])
        height = self.view.viewport().height()
        if rect.top() < 0 or rect.bottom() > height:
            bar = self.view.verticalScrollBar()
            bar.setValue(bar.value() + rect.center().y() - height // 2)

    def _update_controls(self):
        query, n = self.query(), len(self.matches)
        # Left-to-right mark: "3/12" reads the same in the Hebrew interface.
        self.count_label.setText(f"‎{self.current + 1}/{n}" if query else "")
        self.count_label.setStyleSheet("color: #c62828;" if query and not n else "")
        self.count_label.setVisible(bool(query))
        for button in (self.btn_prev, self.btn_next):
            button.setEnabled(n > 0)


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


def _get_folio_image_index(meta, folio_num=None, side_offset=0, *, page_num=None):
    """Return the image index for (folio_num, side_offset), or positional
    page_num when the caller has no folio.

    260421-aln follow-up: callers previously passed
    ``folio_num if folio_num is not None else self.current_browse_p`` in
    the ``folio_num`` position, which caused the helper to folio-search
    with a value that was actually a 1-indexed page number. New callers
    should pass ``folio_num`` and ``page_num`` separately; the helper
    uses folio_num when available and falls back to positional page_num
    otherwise. side_offset is ignored in the positional branch because
    the page number already encodes the side (odd = recto, even = verso
    for manuscripts that start at recto).
    """
    if folio_num is None and page_num is not None:
        images = (meta or {}).get('images_ext') or (meta or {}).get('images') or []
        try:
            idx = max(int(page_num) - 1, 0)
        except (TypeError, ValueError):
            return 0
        if images and idx >= len(images):
            idx = len(images) - 1
        return idx

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


_SIDE_SUFFIX_RE = re.compile(r'^\d+\s*([abrv])$')


def _side_of_label(label):
    """Return 'r' or 'v' when ``label`` looks like "<folio><a|b|r|v>"
    (Oxford's own convention -- e.g. "27a"/"27b"), else None.

    Restricted to a trailing-digit-then-single-letter pattern (not a bare
    ``.endswith()``) so free-text labels like "Binding" or "Cover" are
    never misread as a side.
    """
    m = _SIDE_SUFFIX_RE.match(str(label or '').strip().lower())
    if not m:
        return None
    return 'v' if m.group(1) in ('b', 'v') else 'r'


def map_matching_image_index(old_list, old_idx, new_list, anchor_folio=None):
    """Map an index from one image list to the best-matching index in a
    different image list, preserving recto/verso side when detectable.

    Used both when the user manually switches image source (Oxford <->
    NLI combo) and when auto-falling back from a dead external source to
    NLI (see debug/oxford-fgp-image-mismatch.md sub-issues A & C). The
    two lists usually have DIFFERENT lengths and no shared join key --
    Oxford's ``images_ext`` entries carry ``folio_num`` + a labeled side
    ("27a"/"27b"), while NLI's ``images_nli`` entries carry only
    ``label``/``fl_id`` in NLI's own (unrelated) labeling convention --
    so this uses whatever signal is available:

    1. When the OLD entry's label carries a recognizable recto/verso
       suffix and the NEW list has exactly 2 entries (the common
       "NLI per-part list = one folio, recto + verso" case), pick index
       0 for recto / 1 for verso.
    2. Otherwise, map by relative position (old_idx / len(old_list))
       scaled onto the new list, clamped to bounds.

    ``anchor_folio`` (optional): the folio the DESTINATION list was last showing.
    Used ONLY when the source list is the position-less two-image case (NLI's
    per-part list: recto + verso of one folio, FL-only labels), where the return
    path is otherwise not invertible. When the source list carries folio numbers of
    its own, the current image wins and the anchor is ignored.

    Returns 0 for empty/invalid inputs -- callers must still check that
    ``new_list`` is non-empty before treating the source switch as valid.
    """
    if not new_list:
        return 0
    if not old_list or old_idx is None or old_idx < 0 or old_idx >= len(old_list):
        return 0

    side = _side_of_label(old_list[old_idx].get('label'))

    # 0. `anchor_folio`: the folio the DESTINATION list was last showing. NLI's
    #    per-part list carries only FL labels, so leaving it has no side or folio
    #    signal of its own and the proportional branch below would map NLI index 0
    #    to Oxford image 0 instead of folio 27a. With the anchor we return to that
    #    folio and use the current index only to choose recto/verso
    #    (Codex P2, 2026-09-02).
    # Restricted to the case it exists for: a TWO-image source list whose entries
    # carry no folio of their own (NLI's per-part FL-only list). There the two
    # entries are the recto and verso of ONE folio, so the destination folio is the
    # anchor and the current index only picks the side. When the old list does carry
    # position information -- both sides exposing whole-codex lists, say -- the
    # reader's current position is the better signal and the anchor would drag them
    # back to the folio they left (Codex P2, 2026-09-02).
    _old_has_position = any(im.get('folio_num') is not None for im in old_list)
    if anchor_folio is not None and len(old_list) == 2 and not _old_has_position:
        if side is None:
            side = 'v' if old_idx == 1 else 'r'
        same_folio = [i for i, im in enumerate(new_list)
                      if im.get('folio_num') == anchor_folio]
        if same_folio:
            if side == 'v' and len(same_folio) > 1:
                return same_folio[1]
            return same_folio[0]

    if side is not None and len(new_list) == 2:
        return 1 if side == 'v' else 0

    if len(old_list) <= 1:
        return 0
    ratio = old_idx / (len(old_list) - 1)
    idx = round(ratio * (len(new_list) - 1))
    return max(0, min(idx, len(new_list) - 1))


def _get_initial_image_index(meta, folio_num=None, *, page_num=None):
    """Find the canvas index for a target folio or transcription page.

    260421-aln follow-up: the old single-parameter signature
    (``page_num``) was a misnomer — callers were passing folio numbers
    (from shelfmark extraction, e.g. ``T-S 12.34.2`` → 2) while the
    body interpreted them semantically as page numbers, but simultaneously
    searched the image list for ``folio_num == p_num``. When a caller
    DID pass a 1-indexed transcription page number as a fallback (because
    the shelfmark had no folio), the function searched images_ext for
    a canvas with matching folio_num — returning the wrong image for
    manuscripts where transcription page N ≠ folio N (e.g. T-S NS 158.112
    where transcription page 3 = folio 2r).

    New signature: either ``folio_num`` (search image list by folio) or
    ``page_num`` (positional 1-indexed), never both. The second positional
    arg is still ``folio_num`` for backward compatibility with existing
    callers, but new callers should be explicit.

    Args:
        meta: metadata dict (uses ``images_ext`` or ``images``).
        folio_num: Folio number extracted from a shelfmark (e.g. 2 from
            "T-S 12.34.2"). Searches the image list for a matching
            ``folio_num``; falls back to the nearest prior folio, then
            the first folio.
        page_num: 1-indexed transcription page number. Returns
            ``max(page_num - 1, 0)`` — positional behavior, no folio
            lookup.

    Returns:
        Canvas index in ``images_ext`` / ``images`` (clamped to list
        bounds implicitly by the positional path; folio search returns
        a valid index into the populated folio_entries list).
    """
    if folio_num is not None:
        try:
            fn = int(folio_num)
        except (TypeError, ValueError):
            return 0

        images = (meta or {}).get('images_ext') or (meta or {}).get('images') or []
        folio_entries = []
        for idx, img in enumerate(images):
            fnum = img.get('folio_num')
            if fnum is None:
                continue
            try:
                folio_entries.append((idx, int(fnum)))
            except (TypeError, ValueError):
                continue

        if not folio_entries:
            return max(fn - 1, 0)

        for idx, fnum in folio_entries:
            if fnum == fn:
                return idx

        prior = [(idx, f) for idx, f in folio_entries if f <= fn]
        if prior:
            return max(prior, key=lambda pair: pair[1])[0]

        return min(folio_entries, key=lambda pair: pair[1])[0]

    if page_num is not None:
        try:
            return max(int(page_num) - 1, 0)
        except (TypeError, ValueError):
            return 0

    return 0


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

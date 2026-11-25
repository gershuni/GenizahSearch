# -*- coding: utf-8 -*-
"""The desktop shelf picker's views of the shelfmarks: a list, a table and thumbnails
(``shared/shelfmark_tree_rows.py``; the private plan SHELFMARK_PICKER_VIEWS_PLAN.md, VX1-VX9).

* The switch is in the shelfmarks pane's header; the list is the default; the choice is kept in the
  app's settings and the next picker opens in it.
* The layout rule: in the table and the thumbnails only the last node column is shown.
* The table: the results table's columns and badges, its details from ``row_details`` read off the UI
  thread, a page at a time, kept per dialog.
* The thumbnails: only the tiles in sight are fetched, at most ``THUMBS_AT_ONCE`` at a time, off the UI
  thread; closing the dialog waits for none of them and none is delivered after.
* Every action in every view: a click on any cell chooses, the button, a double click and Enter take,
  in the multiple choice a click ticks.
* ``_thumbnail_image`` tells NLI's breaker of a success only for bytes that are an image.

The real dialog on the fixture tree; the language pinned, the four profile variables at scratch folders.
"""
from __future__ import annotations

import os
import threading
import time

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import QBuffer, QByteArray, QIODevice, Qt
from PyQt6.QtGui import QColor, QImage, QPixmap
from PyQt6.QtTest import QTest
from PyQt6.QtWidgets import QApplication

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.shelfmark_tree_rows import FIELDS, STORAGE_KEY
from shared.shelfmark_tree_selection import members_function
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
_ROW = Qt.ItemDataRole.UserRole


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    config: dict = {}
    monkeypatch.setattr(genizah_core, "load_app_config", lambda: dict(config))
    monkeypatch.setattr(genizah_core, "save_app_config", lambda new: config.update(new))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED", raising=False)
    for hook in ("title_of", "catalogue", "page_count_of", "library_code_of", "page_texts_of", "community", "signs"):
        monkeypatch.setattr(picker, hook, None)
    # the run's caches (thumbnails, the table's widths) start empty in every test and leave nothing behind
    monkeypatch.setattr(picker, '_thumb_cache', picker.OrderedDict())
    monkeypatch.setattr(picker, '_table_widths', {})
    yield config
    _APP.setLayoutDirection(Qt.LayoutDirection.LeftToRight)


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
    yield service
    service.close()


def _wait(pred, timeout=8.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        _APP.processEvents()
        if pred():
            return True
        time.sleep(0.01)
    _APP.processEvents()
    return pred()


def _open(svc, start="vol:100", width=1240, height=760, **kw):
    dialog = picker.ShelfPickerDialog(svc, start=start, **kw)
    dialog.resize(width, height)
    dialog.show()
    assert _wait(lambda: dialog._view is not None and not dialog._loads.busy()), "the picker never drew"
    return dialog


def _close(dialog):
    dialog.reject()
    dialog.deleteLater()
    _APP.processEvents()


def _shown_columns(dialog) -> int:
    return sum(1 for c in dialog.node_columns if c.isVisibleTo(dialog))


CALLS: list = []


def _fake_rows(sys_ids, lang='en', **_kw):
    ids = list(sys_ids)
    CALLS.append((ids, threading.current_thread() is threading.main_thread()))
    out = {}
    for n, sid in enumerate(ids):
        row = dict.fromkeys(FIELDS)
        row.update({'title': f'Title of {sid}', 'snippet': f'snippet {sid}', 'domains': ['Letters', 'Bible'],
                    'pages': n + 1, 'printed': n == 0, 'pgp': n == 1, 'edition': n == 2})
        out[sid] = row
    return out


def _png(color='#806040', size=(40, 30)) -> bytes:
    image = QImage(size[0], size[1], QImage.Format.Format_RGB32)
    image.fill(QColor(color))
    data = QByteArray()
    buf = QBuffer(data)
    buf.open(QIODevice.OpenModeFlag.WriteOnly)
    image.save(buf, "PNG")
    return bytes(data)


# -- the switch -----------------------------------------------------------------------------------

def test_the_switch_is_in_the_pane_and_the_list_is_the_default(svc):
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        assert [k for k, b in panel.view_buttons.items() if b.isChecked()] == ['list']
        assert panel.active() is panel.grid and not panel.size_box.isVisibleTo(dialog)
        assert all(b.toolTip() for b in panel.view_buttons.values())
        assert _shown_columns(dialog) == 3                     # the list as before: the last three
    finally:
        _close(dialog)


def test_the_table_keeps_the_last_column_its_details_read_once_off_the_ui_thread(svc, monkeypatch):
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    CALLS.clear()
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        panel.view_buttons['table'].click()
        assert _wait(lambda: CALLS and not dialog._rows_job and panel.table.rowCount())
        assert _shown_columns(dialog) == 1 and panel.active() is panel.table
        cols = panel.table_columns()
        heads = [panel.table.horizontalHeaderItem(c).text() for c in cols.values()]
        assert heads == ['Shelfmark', 'Title', 'Snippet', 'Domain', 'Pages', 'Printed', 'PGP', 'Edition']
        ids, on_ui = CALLS[0]
        assert len(CALLS) == 1 and not on_ui
        assert ids == [panel.table.item(r, 0).data(_ROW).sys_id for r in range(panel.table.rowCount())]
        first, second, third = ids[:3]

        def cell(sid, key):
            r = panel.table_rows[sid][0]
            return panel.table.item(r, cols[key])
        assert cell(first, 'title').text() == f'Title of {first}' and cell(first, 'snippet').text() == f'snippet {first}'
        assert cell(first, 'domain').text() == 'Letters, Bible' and cell(first, 'pages').text() == '1'
        assert cell(first, 'printed').text() == 'Printed' and cell(second, 'printed').text() == ''
        assert cell(second, 'pgp').text() == 'PGP' and cell(first, 'pgp').text() == ''
        assert cell(third, 'edition').text() == '✓'
        assert cell(second, 'pgp').foreground().color().name() == '#27ae60'
        # back to the list and to the table: the details are kept, not read again
        panel.view_buttons['list'].click()
        assert _wait(lambda: panel.active() is panel.grid and _shown_columns(dialog) == 3)
        panel.view_buttons['table'].click()
        assert _wait(lambda: panel.table.rowCount())
        assert len(CALLS) == 1 and cell(first, 'title').text() == f'Title of {first}'
    finally:
        _close(dialog)


def test_the_view_is_kept_in_the_settings(svc, _isolated_profile):
    dialog = _open(svc)
    try:
        dialog.leaf_panel.view_buttons['gallery'].click()
        assert _wait(lambda: dialog.leaf_panel.gallery.count())
        assert _isolated_profile[STORAGE_KEY] == {'view': 'gallery', 'size': 'm'}   # kept at the switch
        index = dialog.leaf_panel.size_box.findData('l')
        dialog.leaf_panel.size_box.setCurrentIndex(index)
        dialog.leaf_panel.size_box.activated.emit(index)
    finally:
        _close(dialog)
    assert _isolated_profile[STORAGE_KEY] == {'view': 'gallery', 'size': 'l'}
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        assert panel.view == 'gallery' and panel.size == 'l' and panel.active() is panel.gallery
        assert panel.size_box.isVisibleTo(dialog) and _shown_columns(dialog) == 1
    finally:
        _close(dialog)


# -- the thumbnails ---------------------------------------------------------------------------------

def test_only_the_tiles_in_sight_are_fetched_three_at_a_time(svc, monkeypatch, _isolated_profile):
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'l'}
    state = {'now': 0, 'most': 0, 'asked': [], 'threads': set()}
    lock = threading.Lock()

    def fetch(sid, _meta, *_w):
        with lock:
            state['now'] += 1
            state['most'] = max(state['most'], state['now'])
            state['asked'].append(sid)
            state['threads'].add(threading.current_thread() is threading.main_thread())
        time.sleep(0.15)
        with lock:
            state['now'] -= 1
        image = QImage(60, 40, QImage.Format.Format_RGB32)
        image.fill(QColor('#806040'))
        return image
    monkeypatch.setattr(picker, '_thumbnail_image', fetch)
    dialog = _open(svc, height=520)                            # big tiles, few in sight
    try:
        panel = dialog.leaf_panel
        total = panel.gallery.count()
        assert total > 6
        assert _wait(lambda: state['asked'] and state['now'] == 0 and not dialog._thumb_pool.running(), 10)
        asked = set(state['asked'])
        sight = panel.gallery.viewport().rect().adjusted(0, -100, 0, 100)
        in_sight = {panel.gallery.item(i).data(_ROW).sys_id for i in range(total)
                    if panel.gallery.visualItemRect(panel.gallery.item(i)).intersects(sight)}
        assert asked == in_sight and len(asked) < total        # the others are not fetched
        assert state['most'] <= picker.THUMBS_AT_ONCE and state['threads'] == {False}
        drawn = [panel.gallery.item(i) for i in range(total) if panel.gallery.item(i).data(_ROW).sys_id in asked]
        assert all(isinstance(it.data(picker._THUMB_ROLE), QPixmap) for it in drawn)
        # scrolled to the end: the tiles now in sight are fetched, none twice
        panel.gallery.scrollToBottom()
        assert _wait(lambda: len(state['asked']) > len(asked) and not dialog._thumb_pool.running(), 10)
        assert len(state['asked']) == len(set(state['asked']))
    finally:
        _close(dialog)


def test_closing_waits_for_no_thumbnail_and_none_comes_after(svc, monkeypatch, _isolated_profile):
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'm'}
    release = threading.Event()
    delivered = []

    def slow(sid, _meta, *_w):
        release.wait(5)
        return QImage(10, 10, QImage.Format.Format_RGB32)
    monkeypatch.setattr(picker, '_thumbnail_image', slow)
    dialog = _open(svc)
    try:
        assert _wait(lambda: dialog._thumb_pool.running())
        monkeypatch.setattr(dialog.leaf_panel, 'show_picture', lambda sid: delivered.append(sid))
        started = time.monotonic()
        dialog.reject()
        assert time.monotonic() - started < 1.0                 # the network is not waited for
    finally:
        release.set()
        _wait(lambda: False, 0.5)
        dialog.deleteLater()
        _APP.processEvents()
    assert delivered == []


def test_a_200_that_is_no_image_is_no_success_for_the_breaker(monkeypatch):
    import shared.nli_circuit_breaker as breaker
    import shared.nli_fetch as fetch
    seen = []
    monkeypatch.setattr(breaker, 'is_open', lambda: False)
    monkeypatch.setattr(breaker, 'record_success', lambda **kw: seen.append('success'))
    monkeypatch.setattr(breaker, 'record_failure', lambda **kw: seen.append('failure'))
    monkeypatch.setattr(picker, 'thumbnail_url', lambda sid, meta, **_w: 'https://iiif.nli.org.il/x/full/320,/0/default.jpg')
    monkeypatch.setattr(fetch, 'is_nli_host', lambda url: True)

    class _Resp:
        def __init__(self, content):
            self.status_code, self.content = 200, content
    monkeypatch.setattr(fetch, 'nli_image_get', lambda url, **kw: _Resp(b'<html>busy</html>'))
    assert picker._thumbnail_image('1', object()) is None and seen == []
    monkeypatch.setattr(fetch, 'nli_image_get', lambda url, **kw: _Resp(_png()))
    image = picker._thumbnail_image('1', object())
    assert isinstance(image, QImage) and not image.isNull() and seen == ['success']


# -- every action in every view -----------------------------------------------------------------------

def _point(view, item):
    rect = view.visualItemRect(item)
    return rect.center()


def test_a_click_on_any_cell_chooses_and_the_button_double_click_and_enter_take(svc, monkeypatch):
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: None)
    target = SYS["as2"][2]
    for view in ('table', 'gallery'):
        dialog = _open(svc, act_label="Open")
        try:
            panel = dialog.leaf_panel
            panel.view_buttons[view].click()
            widget = panel.active()
            assert _wait(lambda: (widget.rowCount() if view == 'table' else widget.count()) > 3)
            if view == 'table':
                r = panel.table_rows[target][0]
                item = widget.item(r, panel.table_columns()['snippet'])     # not the shelfmark's cell
            else:
                item = panel.tiles[target][0]
            QTest.mouseClick(widget.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                             _point(widget, item))
            assert dialog._chosen_leaf.sys_id == target and dialog.selection() is None
            assert dialog.details.leaf.sys_id == target                         # the pane shows it
            assert panel.current_leaf().sys_id == target
            dialog.act_button.click()
            assert dialog.selection()["sys_id"] == target
        finally:
            _close(dialog)
        dialog = _open(svc)
        try:
            dialog.leaf_panel.view_buttons[view].click()
            widget = dialog.leaf_panel.active()
            assert _wait(lambda: (widget.rowCount() if view == 'table' else widget.count()) > 3)
            item = (widget.item(dialog.leaf_panel.table_rows[target][0], 1) if view == 'table'
                    else dialog.leaf_panel.tiles[target][0])
            QTest.mouseDClick(widget.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                              _point(widget, item))
            assert dialog.selection()["sys_id"] == target
        finally:
            _close(dialog)
        dialog = _open(svc)
        try:
            dialog.leaf_panel.view_buttons[view].click()
            widget = dialog.leaf_panel.active()
            assert _wait(lambda: (widget.rowCount() if view == 'table' else widget.count()) > 3)
            widget.setFocus()
            item = (widget.item(dialog.leaf_panel.table_rows[target][0], 2) if view == 'table'
                    else dialog.leaf_panel.tiles[target][0])
            widget.setCurrentItem(item)
            QTest.keyClick(widget, Qt.Key.Key_Return)
            assert dialog.selection()["sys_id"] == target
        finally:
            _close(dialog)


def test_ticks_in_the_table_and_the_thumbnails(svc, monkeypatch):
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: None)
    taken = []
    store = picker.SelectionStore()
    a, b = SYS["as2"][1], SYS["as2"][3]
    dialog = _open(svc, mode="multi", purpose="test_views", store=store,
                   on_confirm=lambda entries, label: taken.append([e['sys_id'] for e in entries]))
    try:
        panel = dialog.leaf_panel
        panel.view_buttons['table'].click()
        assert _wait(lambda: panel.table.rowCount() and panel.table.horizontalHeaderItem(0).text() == '')
        r = panel.table_rows[a][0]
        QTest.mouseClick(panel.table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(panel.table, panel.table.item(r, panel.table_columns()['title'])))
        assert _wait(lambda: panel.table.item(r, 0).checkState() == Qt.CheckState.Checked)
        panel.view_buttons['gallery'].click()
        assert _wait(lambda: panel.gallery.count())
        assert _wait(lambda: panel.tiles[a][0].checkState() == Qt.CheckState.Checked)   # kept, drawn ticked
        QTest.mouseClick(panel.gallery.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(panel.gallery, panel.tiles[b][0]))
        assert _wait(lambda: panel.tiles[b][0].checkState() == Qt.CheckState.Checked)
        assert store.load("test_views").count(members_function(svc)) == 2
        dialog.confirm_button.click()
        assert _wait(lambda: taken)
        assert taken == [[a, b]]
    finally:
        _close(dialog)


def test_every_page_of_rows_is_read_one_job_at_a_time(svc, monkeypatch):
    """The loader works out again what is missing when a job ends: a table of several pages (here
    pages of 3) is filled whole, never two jobs at once."""
    from shared.shelfmark_tree_rows import wanted as real_wanted
    monkeypatch.setattr(picker, 'wanted', lambda ids, known: real_wanted(ids, known, size=3))
    running = {'now': 0, 'most': 0}

    def rows(sys_ids, lang='en', **kw):
        running['now'] += 1
        running['most'] = max(running['most'], running['now'])
        time.sleep(0.05)
        running['now'] -= 1
        return _fake_rows(sys_ids, lang, **kw)
    monkeypatch.setattr(picker, 'row_details', rows)
    CALLS.clear()
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        panel.view_buttons['table'].click()
        n = panel.table.rowCount()
        assert n > 6
        title = panel.table_columns()['title']
        assert _wait(lambda: all(panel.table.item(r, title).text() for r in range(n)), 10)
        assert len(CALLS) == -(-n // 3) and running['most'] == 1
    finally:
        _close(dialog)


def test_a_stopped_pool_delivers_nothing(monkeypatch):
    release = threading.Event()
    got = []
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: (release.wait(5), QImage(4, 4, QImage.Format.Format_RGB32))[1])
    pool = picker._ThumbPool(lambda sid, image, *_w: got.append(sid))
    pool.want(['a', 'b', 'c', 'd', 'e'])
    assert pool.running() == {'a', 'b', 'c'}                   # three at a time
    pool.stop()
    release.set()
    _wait(lambda: not pool.running(), 5)
    assert got == [] and not pool.running()
    pool.want(['f'])                                           # nothing is started once stopped
    assert not pool.running()


def test_a_tile_shows_the_start_of_its_title(svc, monkeypatch, _isolated_profile):
    """Owner, 2026-11-22: the start of the title under the shelfmark, readable at a glance."""
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'm'}
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: None)
    monkeypatch.setattr(picker, 'title_of', lambda sid: f'מכתב ממשה אל אחיו על ענייני מסחר {sid}')
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        assert _wait(lambda: panel.gallery.count())
        item = panel.tiles[SYS["as2"][2]][0]
        assert item.data(picker._TITLE_ROLE) == f'מכתב ממשה אל אחיו על ענייני מסחר {SYS["as2"][2]}'
        line = panel.fontMetrics().height()
        assert panel.tile_size().height() >= panel.picture_size()[1] + 3 * line   # room for its two lines
    finally:
        _close(dialog)


def test_the_title_takes_two_lines_at_most_cut_with_an_ellipsis():
    font = _APP.font()
    long = 'שאלות ותשובות של רב האי גאון על ענייני ממונות ודיני נפשות ' * 4
    lines = picker.tile_title_lines(long, font, 160)
    assert len(lines) == 2 and lines[-1].endswith('…')
    assert picker.tile_title_lines('קצר', font, 160) == ['קצר']
    assert picker.tile_title_lines('', font, 160) == [] and picker.tile_title_lines('x', font, 0) == []


# -- Codex on #95 ---------------------------------------------------------------------------------------

def test_leaving_the_gallery_drops_the_thumbnails_waiting(svc, monkeypatch, _isolated_profile):
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 's'}
    release = threading.Event()
    asked = []

    def slow(sid, _meta, *_w):
        asked.append(sid)
        release.wait(5)
        return None
    monkeypatch.setattr(picker, '_thumbnail_image', slow)
    dialog = _open(svc)
    try:
        pool = dialog._thumb_pool
        assert _wait(lambda: len(pool.running()) == picker.THUMBS_AT_ONCE and pool._queue)   # some waiting
        dialog.leaf_panel.view_buttons['list'].click()
        assert pool._queue == []
        release.set()
        assert _wait(lambda: not pool.running())
        _wait(lambda: False, 0.3)
        assert len(asked) == picker.THUMBS_AT_ONCE           # only those under way were fetched
    finally:
        release.set()
        _close(dialog)


def test_the_chosen_tile_stays_chosen_across_a_size_change(svc, monkeypatch, _isolated_profile):
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'm'}
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: None)
    target = SYS["as2"][3]
    dialog = _open(svc, start="vol:100")
    try:
        panel = dialog.leaf_panel
        assert _wait(lambda: panel.gallery.count())
        item = panel.tiles[target][0]
        QTest.mouseClick(panel.gallery.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(panel.gallery, item))
        assert dialog._chosen_leaf.sys_id == target
        index = panel.size_box.findData('l')
        panel.size_box.setCurrentIndex(index)
        panel.size_box.activated.emit(index)
        assert panel.current_leaf() is not None and panel.current_leaf().sys_id == target
        panel.gallery.setFocus()
        QTest.keyClick(panel.gallery, Qt.Key.Key_Return)
        assert dialog.selection()["sys_id"] == target
    finally:
        _close(dialog)


def test_the_row_chosen_in_the_list_is_the_tables_current_one(svc, monkeypatch):
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    target = SYS["as2"][4]
    dialog = _open(svc, start="vol:100")
    try:
        panel = dialog.leaf_panel
        grid_item = next(panel.grid.item(i) for i in range(panel.grid.count())
                         if panel.grid.item(i).data(_ROW).sys_id == target)
        QTest.mouseClick(panel.grid.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(panel.grid, grid_item))
        assert dialog._chosen_leaf.sys_id == target
        panel.view_buttons['table'].click()
        assert _wait(lambda: panel.table.rowCount())
        assert panel.current_leaf() is not None and panel.current_leaf().sys_id == target
        panel.table.setFocus()
        QTest.keyClick(panel.table, Qt.Key.Key_Return)
        assert dialog.selection()["sys_id"] == target
    finally:
        _close(dialog)


def test_a_size_change_keeps_the_readers_place(svc, monkeypatch, _isolated_profile):
    """Codex on #95: scrolled down, a new size keeps the tile at the top at the top (not the gallery's top)."""
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'l'}
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: None)
    dialog = _open(svc, height=520)
    try:
        panel = dialog.leaf_panel
        assert _wait(lambda: panel.gallery.count() > 6)
        panel.gallery.scrollToBottom()
        _APP.processEvents()
        top = panel.top_tile()
        first = panel.gallery.item(0).data(_ROW).sys_id
        assert top is not None and top != first                  # scrolled down
        index = panel.size_box.findData('m')
        panel.size_box.setCurrentIndex(index)
        panel.size_box.activated.emit(index)
        _APP.processEvents()
        assert panel.size == 'm' and panel.gallery.verticalScrollBar().value() > 0
        rect = panel.gallery.visualItemRect(panel.tiles[top][0])
        assert rect.intersects(panel.gallery.viewport().rect())   # still in sight
    finally:
        _close(dialog)


def test_switching_views_keeps_the_readers_place(svc, monkeypatch, _isolated_profile):
    """Codex on #95: scrolled down in the thumbnails, a switch to the table and back keeps the shelfmark
    that was at the top in sight (the table's offset no longer overwrites the gallery's)."""
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'l'}
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: None)
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    dialog = _open(svc, height=520)
    try:
        panel = dialog.leaf_panel
        assert _wait(lambda: panel.gallery.count() > 6)
        panel.gallery.scrollToBottom()
        _APP.processEvents()
        top = panel.top_leaf()
        assert top is not None and top.sys_id != panel.gallery.item(0).data(_ROW).sys_id
        panel.view_buttons['table'].click()
        assert _wait(lambda: panel.table.rowCount())
        r = panel.table_rows[top.sys_id][0]
        assert panel.table.visualItemRect(panel.table.item(r, 1)).intersects(panel.table.viewport().rect())
        panel.view_buttons['gallery'].click()
        assert _wait(lambda: panel.gallery.count())
        _APP.processEvents()
        rect = panel.gallery.visualItemRect(panel.tiles[top.sys_id][0])
        assert rect.intersects(panel.gallery.viewport().rect()) and panel.gallery.verticalScrollBar().value() > 0
    finally:
        _close(dialog)


# -- opened at a manuscript ------------------------------------------------------------------------------

def test_opened_at_a_manuscript_it_is_chosen_and_the_pane_shows_it(svc):
    """Owner, 2026-11-22: as on the web, the shelfmark the picker opens at is the one chosen (Open takes it)
    and the details pane shows it at once."""
    here = SYS["as2"][3]
    dialog = _open(svc, start=here, act_label="Open")
    try:
        assert dialog._chosen_leaf is not None and dialog._chosen_leaf.sys_id == here
        assert dialog.act_button.isEnabled()
        assert _wait(lambda: dialog.details.leaf is not None and dialog.details.leaf.sys_id == here)
        assert dialog.leaf_panel.current_leaf().sys_id == here
        dialog.act_button.click()
        assert dialog.selection()["sys_id"] == here
    finally:
        _close(dialog)
    dialog = _open(svc, start="vol:100", act_label="Open")       # opened at a node: nothing chosen
    try:
        assert dialog._chosen_leaf is None and not dialog.act_button.isEnabled() and dialog.details.leaf is None
    finally:
        _close(dialog)


def test_leaving_a_shelfmark_empties_the_pane_and_the_choice(svc):
    """Owner, 2026-11-22: the pane, and in the single choice the shelfmark chosen, follow the place. A filter
    on the shelfmark's own list keeps them; leaving the list empties the pane and turns Open off (it would
    take a shelfmark no longer in sight); coming back brings neither back by itself."""
    here = SYS["as2"][3]
    dialog = _open(svc, start=here, act_label="Open")
    try:
        assert _wait(lambda: dialog.details.leaf is not None and dialog.details.leaf.sys_id == here)
        path = tuple(dialog.path())
        dialog.set_filter(dialog.leaf_panel.key, "no such shelfmark")
        assert _wait(lambda: not dialog._owed and not dialog._loads.busy())
        assert dialog.leaf_panel.current_leaf() is None                         # hidden by the filter ...
        assert dialog.details.leaf.sys_id == here and dialog._chosen_leaf.sys_id == here   # ... still its place
        assert dialog.act_button.isEnabled()
        dialog.go(())
        assert _wait(lambda: dialog._drawn_path == () and not dialog._loads.busy())
        assert dialog.details.leaf is None and dialog.details.text is None
        assert dialog.details.empty.isVisibleTo(dialog) and not dialog.details.shelfmark.isVisibleTo(dialog)
        assert dialog._chosen_leaf is None and not dialog.act_button.isEnabled()
        dialog.go(path)
        assert _wait(lambda: dialog._drawn_path == path and not dialog._loads.busy())
        assert dialog.details.leaf is None and dialog._chosen_leaf is None and not dialog.act_button.isEnabled()
    finally:
        _close(dialog)
    store = picker.SelectionStore()                  # the multiple choice: the pane alone, nothing is ticked
    dialog = _open(svc, start=here, mode="multi", purpose="test_leave", store=store, on_confirm=lambda *_a: None)
    try:
        assert _wait(lambda: dialog.details.leaf is not None and dialog.details.leaf.sys_id == here)
        dialog.go(())
        assert _wait(lambda: dialog._drawn_path == () and not dialog._loads.busy())
        assert dialog.details.leaf is None and dialog.details.empty.isVisibleTo(dialog)
        assert store.load("test_leave").count(members_function(svc)) == 0
    finally:
        _close(dialog)


def test_the_pane_counts_only_the_columns_on_screen(svc, monkeypatch):
    """Codex #99: a shelfmark a column's filter found is in view only while that column is drawn; in the
    table (and the thumbnails) only the last column is, so the check is asked with the columns on screen."""
    asked = []
    real = picker.leaf_in_view

    def spy(view, leaf, columns=None):
        asked.append(None if columns is None else [c['filter_key'] for c in columns])
        return real(view, leaf, columns)

    monkeypatch.setattr(picker, 'leaf_in_view', spy)
    dialog = _open(svc, start=SYS["as2"][3], act_label="Open")
    try:
        assert _wait(lambda: dialog.details.leaf is not None)
        for view in ('table', 'gallery', 'list'):
            asked.clear()
            dialog.leaf_panel.set_view(view)
            shown = [c.key for c in dialog.node_columns if c.isVisibleTo(dialog)]
            assert asked and all(a == shown for a in asked), (view, asked, shown)
            assert (len(shown) == 1) == (view != 'list')
    finally:
        _close(dialog)


def test_a_filter_that_hides_the_row_a_shelfmark_was_chosen_in_lets_it_go(svc):
    """Codex #99: a shelfmark a column's filter found, chosen there, goes from the pane and the choice when
    the filter changes and its row is no longer drawn (a list drawn alone, not the whole dialog)."""
    dialog = _open(svc, start="col:10", act_label="Open")
    try:
        dialog.set_filter("n:col:10", "T-S AS 2.4")
        column = next(c for c in dialog.node_columns if c.key == "n:col:10")
        assert _wait(lambda: not dialog._owed and not dialog._loads.busy()
                     and any('leaf' in r for r in column.rows))
        row = next(i for i, r in enumerate(column.rows) if 'leaf' in r and r['leaf'].label == "T-S AS 2.4")
        leaf = column.rows[row]['leaf']
        assert leaf.node_id != dialog.leaf_panel.leaves['node']           # found below, not in the list shown
        dialog._open_row(column, row)
        assert dialog._chosen_leaf.id == leaf.id and dialog.details.leaf.id == leaf.id
        assert dialog.act_button.isEnabled()
        dialog.set_filter("n:col:10", "T-S AS 2.9")
        assert _wait(lambda: not dialog._owed and not dialog._loads.busy())
        assert not any(r.get('leaf') is not None and r['leaf'].id == leaf.id for r in column.rows)
        assert dialog.details.leaf is None and dialog._chosen_leaf is None and not dialog.act_button.isEnabled()
    finally:
        _close(dialog)


def test_opened_at_a_manuscript_in_the_multiple_choice_the_pane_shows_it_unticked(svc):
    here = SYS["as2"][3]
    store = picker.SelectionStore()
    dialog = _open(svc, start=here, mode="multi", purpose="test_open_at", store=store, on_confirm=lambda *_a: None)
    try:
        assert _wait(lambda: dialog.details.leaf is not None and dialog.details.leaf.sys_id == here)
        assert store.load("test_open_at").count(members_function(svc)) == 0      # shown, never ticked
    finally:
        _close(dialog)


def test_new_tiles_drop_the_thumbnails_waiting_for_the_old(svc, monkeypatch, _isolated_profile):
    """Codex on #95: a filter (or another node) replaces the tiles; what waited for the old ones is not
    fetched."""
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 's'}
    release = threading.Event()
    asked = []

    def slow(sid, _meta, *_w):
        asked.append(sid)
        release.wait(5)
        return None
    monkeypatch.setattr(picker, '_thumbnail_image', slow)
    dialog = _open(svc)
    try:
        pool, panel = dialog._thumb_pool, dialog.leaf_panel
        assert _wait(lambda: len(pool.running()) == picker.THUMBS_AT_ONCE and pool._queue)
        first = set(pool.running())
        last = panel.gallery.item(panel.gallery.count() - 1).data(_ROW)
        dialog.set_filter(panel.key, last.label)             # the gallery is now that one tile
        assert _wait(lambda: panel.gallery.count() == 1)
        assert all(sid == last.sys_id for sid in pool._queue)   # nothing of the old tiles waits
        release.set()
        assert _wait(lambda: not pool.running() and not pool._queue)
        _wait(lambda: False, 0.4)
        assert set(asked) <= first | {last.sys_id}
    finally:
        release.set()
        _close(dialog)


# -- owner, 2026-11-22: one fetch a thumbnail; resizable columns ----------------------------------------

@pytest.fixture
def _fresh_caches(monkeypatch):
    monkeypatch.setattr(picker, '_thumb_cache', picker.OrderedDict())
    monkeypatch.setattr(picker, '_table_widths', {})


def _image():
    image = QImage(60, 40, QImage.Format.Format_RGB32)
    image.fill(QColor('#806040'))
    return image


def test_the_pane_fetches_a_thumbnail_once_and_shows_it_at_once_after(svc, monkeypatch, _fresh_caches):
    monkeypatch.setattr(picker, 'catalogue', object())
    asked = []
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: asked.append(sid) or _image())
    here = SYS["as2"][3]
    dialog = _open(svc, start=here)
    try:
        assert _wait(lambda: dialog.details.image.isVisible())
        assert asked == [here]
        dialog.details.show_leaf(dialog.details.leaf)          # the same shelfmark again
        assert dialog.details.image.isVisible()                 # at once, before any load
        _wait(lambda: False, 0.4)
        assert asked == [here]                                   # not fetched again
    finally:
        _close(dialog)
    dialog = _open(svc, start=here)                             # another picker of the run
    try:
        assert dialog.details.image.isVisible() or _wait(lambda: dialog.details.image.isVisible(), 1)
        _wait(lambda: False, 0.4)
        assert asked == [here]
    finally:
        _close(dialog)


def test_a_thumbnail_the_gallery_fetched_is_the_panes_at_once(svc, monkeypatch, _fresh_caches, _isolated_profile):
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'm'}
    monkeypatch.setattr(picker, 'catalogue', object())
    asked = []
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: asked.append(sid) or _image())
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        assert _wait(lambda: asked and not dialog._thumb_pool.running())
        sid = asked[0]
        before = len(asked)
        QTest.mouseClick(panel.gallery.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(panel.gallery, panel.tiles[sid][0]))
        assert dialog.details.image.isVisible()                 # at once
        _wait(lambda: False, 0.4)
        assert len(asked) == before                              # the pane fetched nothing
    finally:
        _close(dialog)


def test_the_table_columns_can_be_resized_and_keep_their_widths(svc, monkeypatch, _fresh_caches):
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        panel.view_buttons['table'].click()
        assert _wait(lambda: panel.table.rowCount())
        _wait(lambda: False, 0.2)
        header = panel.table.horizontalHeader()
        cols = panel.table_columns()
        assert all(header.sectionResizeMode(c) == picker.QHeaderView.ResizeMode.Interactive for c in cols.values())
        fitted = sum(panel.table.columnWidth(c) for c in cols.values())
        print('WIDTHS', {k: panel.table.columnWidth(c) for k, c in cols.items()}, panel.table.viewport().width())
        assert fitted <= panel.table.viewport().width() + 4      # every column in sight at first
        header.resizeSection(cols['title'], 333)                 # the reader widens the title
        assert picker._table_widths['title'] == 333
        panel.view_buttons['list'].click()
        panel.view_buttons['table'].click()
        assert _wait(lambda: panel.table.rowCount())
        _wait(lambda: False, 0.2)
        assert panel.table.columnWidth(panel.table_columns()['title']) == 333   # kept
    finally:
        _close(dialog)


def test_a_clicked_table_row_stays_selected_right_to_left(svc, monkeypatch, _isolated_profile):
    """Owner, 2026-11-22 (Hebrew): the selection line went after a click in the table. selectRow looks for a
    column at the far edge of a right-to-left viewport and found none; the row is now selected through its
    current cell."""
    _APP.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    _isolated_profile[STORAGE_KEY] = {'view': 'table', 'size': 'm'}
    monkeypatch.setattr(picker, 'row_details', _fake_rows)
    target = SYS["as2"][2]
    dialog = _open(svc)
    try:
        table = dialog.leaf_panel.table
        assert _wait(lambda: table.rowCount())
        _wait(lambda: False, 0.2)
        r = dialog.leaf_panel.table_rows[target][0]
        QTest.mouseClick(table.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(table, table.item(r, 3)))
        _wait(lambda: False, 0.2)
        assert [i.row() for i in table.selectionModel().selectedRows()] == [r] and table.currentRow() == r
        table.setFocus()
        QTest.keyClick(table, Qt.Key.Key_Return)
        assert dialog.selection()["sys_id"] == target
    finally:
        _close(dialog)


def test_a_picture_under_way_in_the_gallery_is_not_fetched_again_by_the_pane(svc, monkeypatch, _isolated_profile):
    """Codex on #95: a tile clicked while the gallery still fetches its picture starts no second fetch;
    the gallery's answer reaches the pane."""
    _isolated_profile[STORAGE_KEY] = {'view': 'gallery', 'size': 'm'}
    monkeypatch.setattr(picker, 'catalogue', object())
    release = threading.Event()
    asked = []

    def slow(sid, _meta, *_w):
        asked.append(sid)
        release.wait(5)
        return _image()
    monkeypatch.setattr(picker, '_thumbnail_image', slow)
    dialog = _open(svc)
    try:
        panel = dialog.leaf_panel
        assert _wait(lambda: dialog._thumb_pool.running())
        sid = sorted(dialog._thumb_pool.running())[0]
        QTest.mouseClick(panel.gallery.viewport(), Qt.MouseButton.LeftButton, Qt.KeyboardModifier.NoModifier,
                         _point(panel.gallery, panel.tiles[sid][0]))
        assert _wait(lambda: dialog.details.text is not None)      # the pane's text read: its image asked or not
        release.set()
        assert _wait(lambda: dialog.details.image.isVisible())       # the gallery's answer, in the pane
        _wait(lambda: False, 0.3)
        assert asked.count(sid) == 1
    finally:
        release.set()
        _close(dialog)


def test_the_gallery_skips_a_picture_the_pane_is_fetching(monkeypatch):
    got = []
    started = []
    monkeypatch.setattr(picker, '_thumbnail_image', lambda sid, meta, *_w: started.append(sid) or None)
    pool = picker._ThumbPool(lambda sid, image, *_w: got.append(sid), skip=lambda sid: sid == 'busy')
    pool.want(['busy', 'free'])
    assert _wait(lambda: not pool.running())
    assert started == ['free']


# -- Codex on #95's head: one loader for every thumbnail; a failed batch is not read again ------------

def test_the_panes_request_goes_first_and_the_gallerys_own_requests_never_drop_it(monkeypatch):
    release = threading.Event()
    started = []

    def slow(sid, _meta, *_w):
        started.append(sid)
        release.wait(5)
        return None
    monkeypatch.setattr(picker, '_thumbnail_image', slow)
    got = []
    pool = picker._ThumbPool(lambda sid, image, *_w: got.append(sid))
    pool.want(['a', 'b', 'c', 'd', 'e'])
    assert pool.running() == {'a', 'b', 'c'}
    pool.fetch('x')                                           # the pane's, while every slot is busy
    pool.want(['f'])                                          # the gallery scrolled: its queue replaced
    pool.fetch('a')                                           # under way already: not asked twice
    release.set()
    assert _wait(lambda: not pool.running() and len(started) == 5)
    assert started[3] == 'x' and started.count('a') == 1 and set(started) == {'a', 'b', 'c', 'x', 'f'}
    pool.stop()


def test_a_failed_batch_of_table_details_is_not_read_again(svc, monkeypatch, _isolated_profile):
    _isolated_profile[STORAGE_KEY] = {'view': 'table', 'size': 'm'}
    calls = []

    def broken(sys_ids, lang='en', **_kw):
        calls.append(list(sys_ids))
        raise RuntimeError('down')
    monkeypatch.setattr(picker, 'row_details', broken)
    dialog = _open(svc)
    try:
        assert _wait(lambda: calls and not dialog._rows_job)
        _wait(lambda: False, 0.6)
        assert len(calls) == 1                                    # not read again and again
        assert dialog.leaf_panel.table.rowCount()                 # the rows stay, blank
    finally:
        _close(dialog)

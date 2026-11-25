# -*- coding: utf-8 -*-
"""The desktop shelf picker's round 5 (a reader sifting through the shelf, 2026-11-25): the details
pane hidden or shown (the choice kept), its joins ticked or cleared all at once, its image's other
pages behind previous / next, and the extra large thumbnails asked for sharper.

The real dialog on the fixture tree; the profile, the language and the hooks are round 4's.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QImage

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture
from tests.test_shelf_picker_desktop_round4 import _Community, _click_item, _close, _open, _switch_picker, _wait

pytestmark = pytest.mark.gui


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    """Round 4's: the profile in scratch folders, the language pinned, the picker's hooks off."""
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED", raising=False)
    for hook in ("title_of", "catalogue", "page_count_of", "library_code_of", "page_texts_of", "community", "signs"):
        monkeypatch.setattr(picker, hook, None)


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
    yield service
    service.close()


@pytest.fixture(autouse=True)
def _fresh_thumbnails(monkeypatch):
    """The run's thumbnails, empty for each test and left as they were after it."""
    monkeypatch.setattr(picker, "_thumb_cache", picker.OrderedDict())
    monkeypatch.setattr(picker, "_thumb_width", {})


def _image(width):
    image = QImage(width, max(1, width // 2), QImage.Format.Format_RGB32)
    image.fill(0)
    return image


# -- the details pane, hidden or shown --------------------------------------------------------------

def test_the_details_pane_hides_reads_nothing_and_comes_back_with_the_shelfmark_clicked(svc, monkeypatch):
    asked = []
    real = picker.item_details
    monkeypatch.setattr(picker, "item_details", lambda *a, **k: asked.append(a[1].sys_id) or real(*a, **k))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        assert dialog.details_button.isChecked() and pane.isVisible()
        dialog.details_button.click()
        assert not pane.isVisible()
        assert genizah_core.load_app_config().get(picker.DETAILS_KEY) is False
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.pending is not None)
        assert asked == [] and pane.leaf is None                 # hidden: nothing read
        dialog.details_button.click()
        assert pane.isVisible() and pane.pending is None
        assert _wait(lambda: pane.text is not None and pane.text.get("sys_id") == SYS["as2"][0])
        assert asked == [SYS["as2"][0]]
    finally:
        _close(dialog)


def test_a_hidden_pane_stays_hidden_at_the_next_opening(svc):
    genizah_core.save_app_config({picker.DETAILS_KEY: False})
    dialog = _open(svc, start="vol:100")
    try:
        assert not dialog.details_button.isChecked() and not dialog.details.isVisible()
    finally:
        _close(dialog)
    genizah_core.save_app_config({picker.DETAILS_KEY: True})
    dialog = _open(svc, start="vol:100")
    try:
        assert dialog.details_button.isChecked() and dialog.details.isVisible()
    finally:
        _close(dialog)


# -- the joins, all at once ---------------------------------------------------------------------------

def test_the_joins_box_ticks_every_partner_and_again_unticks_them(svc, monkeypatch):
    here, near, far = SYS["as2"][0], SYS["as2"][2], SYS["as10"][0]
    monkeypatch.setattr(picker, "community", _Community([(near, "T-S AS 2.3"), (far, "T-S AS 10.1"),
                                                         ("990000999990205171", "ENA 123.4")]))
    dialog = _switch_picker(svc)
    try:
        pane = dialog.details
        _click_item(dialog, here)
        assert _wait(lambda: set(pane.join_boxes) == {near, far})
        assert pane.joins_all.isVisible() and pane.joins_all.checkState() == Qt.CheckState.Unchecked
        # before "Joins:" (in reading order: its left in English)
        assert pane.joins_all.geometry().right() < pane.joins_title.geometry().left()
        assert not dialog.multi
        pane.joins_all.click()                                   # the multiple choice turned on first
        members = picker.members_function(svc)
        assert dialog.multi and dialog.switch.isChecked()
        assert {here, near, far} <= dialog.chosen().chosen_set(members)
        assert pane.join_boxes[near].isChecked() and pane.join_boxes[far].isChecked()
        assert pane.joins_all.checkState() == Qt.CheckState.Checked
        pane.joins_all.click()                                   # again: the partners out, the shelfmark shown stays
        chosen = dialog.chosen().chosen_set(members)
        assert near not in chosen and far not in chosen and here in chosen
        assert not pane.join_boxes[near].isChecked() and not pane.join_boxes[far].isChecked()
        assert pane.joins_all.checkState() == Qt.CheckState.Unchecked
        pane.join_boxes[far].click()                             # one of two: partly
        assert pane.joins_all.checkState() == Qt.CheckState.PartiallyChecked
        pane.joins_all.click()                                   # partly: a click ticks them all
        chosen = dialog.chosen().chosen_set(members)
        assert near in chosen and far in chosen and pane.joins_all.checkState() == Qt.CheckState.Checked
    finally:
        _close(dialog)


def test_one_partner_has_no_joins_box(svc, monkeypatch):
    far = SYS["as10"][0]
    monkeypatch.setattr(picker, "community", _Community([(far, "T-S AS 10.1")]))
    dialog = _switch_picker(svc)
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: set(pane.join_boxes) == {far})
        assert pane.joins_head.isVisible() and not pane.joins_all.isVisible()
    finally:
        _close(dialog)


# -- the image's other pages ---------------------------------------------------------------------------

class _Meta:
    csv_bank: dict = {}

    def __init__(self, pages, thumb=1):
        self.pages, self.thumb = pages, thumb        # the catalogue record's first image (None: none of them)

    def get_thumbnail(self, sid, size=320):
        fl = f"{sid}{self.thumb}" if self.thumb else f"{sid}0"
        return f"https://iiif.nli.org.il/IIIFv21/FL{fl}/full/400,/0/default.jpg"

    def fetch_volume_manifest(self, sid, suffix):
        return {"images_nli": [{"url": f"https://iiif.nli.org.il/IIIFv21/FL{sid}{p}", "label": str(p),
                                "fl_id": f"{sid}{p}"} for p in range(1, self.pages + 1)]}


def test_previous_and_next_walk_the_manuscript_s_images_the_next_fetched_ahead(svc, monkeypatch):
    fetched = []
    widths = {"/FL{}1/".format(SYS["as2"][0]): 401, "/FL{}2/".format(SYS["as2"][0]): 402,
              "/FL{}3/".format(SYS["as2"][0]): 403}

    def image_at(url):
        fetched.append(url)
        return _image(next(w for key, w in widths.items() if key in url))
    monkeypatch.setattr(picker, "catalogue", _Meta(3))
    monkeypatch.setattr(picker, "_image_at", image_at)
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.page_nav.isVisible())
        assert pane.page_label.text() == "Image 1 of 3"
        assert not pane.page_prev.isEnabled() and pane.page_next.isEnabled()
        assert _wait(lambda: any("FL{}2/".format(SYS["as2"][0]) in u for u in fetched))   # the next, ahead
        assert all("/full/400,/" in u for u in fetched)
        pane.page_next.click()
        assert pane.page_label.text() == "Image 2 of 3"
        assert _wait(lambda: not pane.image.pixmap().isNull())
        assert _wait(lambda: any("FL{}3/".format(SYS["as2"][0]) in u for u in fetched))   # and the one after
        pane.page_next.click()
        assert pane.page_label.text() == "Image 3 of 3" and not pane.page_next.isEnabled()
        pane.page_prev.click()
        pane.page_prev.click()
        assert pane.page_label.text() == "Image 1 of 3" and not pane.page_prev.isEnabled()
        assert len([u for u in fetched if "FL{}2/".format(SYS["as2"][0]) in u]) == 1   # kept, not fetched again
    finally:
        _close(dialog)


def test_the_pane_opens_on_the_list_s_first_image_whatever_the_thumbnail_is(svc, monkeypatch):
    # Seen in the app (T-S C2.13): the thumbnail is the catalogue record's first image, here the list's
    # second; counted as the first, "next" showed the same picture again. Then (owner, 2026-11-25) the pane
    # opened on "Image 2": it goes to the list's first, the thumbnail shown until that comes
    sid, fetched = SYS["as2"][0], []

    def image_at(url):
        fetched.append(url)
        return _image(400 + int(url.split(f"FL{sid}")[1][0]))
    monkeypatch.setattr(picker, "catalogue", _Meta(3, thumb=2))
    monkeypatch.setattr(picker, "_image_at", image_at)
    monkeypatch.setattr(picker, "_thumbnail_image", lambda s, meta, width=400: _image(402))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, sid)
        assert _wait(lambda: pane.page_nav.isVisible())
        assert pane.page == 0 and pane.page_label.text() == "Image 1 of 3"
        assert not pane.page_prev.isEnabled() and pane.page_next.isEnabled()
        assert _wait(lambda: pane.image._source is not None and pane.image._source.width() == 401)
        assert not any(f"FL{sid}2/" in u for u in fetched)      # the thumbnail is image 2: not fetched again
        pane.page_next.click()                                 # the thumbnail, at once
        assert pane.page_label.text() == "Image 2 of 3" and pane.image._source.width() == 402
        pane.page_next.click()
        assert _wait(lambda: pane.image._source.width() == 403)
    finally:
        _close(dialog)


def test_a_thumbnail_the_list_does_not_name_gives_way_to_its_first_image(svc, monkeypatch):
    sid = SYS["as2"][0]
    monkeypatch.setattr(picker, "catalogue", _Meta(2, thumb=None))
    monkeypatch.setattr(picker, "_image_at", lambda url: _image(400 + int(url.split(f"FL{sid}")[1][0])))
    monkeypatch.setattr(picker, "_thumbnail_image", lambda s, meta, width=400: _image(399))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, sid)
        assert _wait(lambda: pane.page_nav.isVisible())
        assert pane.page_label.text() == "Image 1 of 2"
        assert _wait(lambda: pane.image._source is not None and pane.image._source.width() == 401)
        pane.page_next.click()
        assert _wait(lambda: pane.image._source.width() == 402)
        pane.page_prev.click()
        assert pane.image._source.width() == 401                # the list's first, not the thumbnail
    finally:
        _close(dialog)


def test_one_image_shows_no_arrows(svc, monkeypatch):
    monkeypatch.setattr(picker, "catalogue", _Meta(1))
    monkeypatch.setattr(picker, "_image_at", lambda url: _image(400))
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: not pane.image.pixmap().isNull())
        assert _wait(lambda: not pane._pages_loads.busy())
        assert not pane.page_nav.isVisible() and pane.pages == []
    finally:
        _close(dialog)


@pytest.mark.parametrize("rtl", [False, True])
def test_the_page_arrows_are_the_style_s_and_point_away_from_the_label(svc, monkeypatch, rtl):
    # Text arrows (\u2039 / \u203a) are a Unicode mirrored pair: in Hebrew both came out pointing right
    from PyQt6.QtWidgets import QApplication
    monkeypatch.setattr(picker, "catalogue", _Meta(3))
    monkeypatch.setattr(picker, "_image_at", lambda url: _image(400))
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    QApplication.instance().setLayoutDirection(Qt.LayoutDirection.RightToLeft if rtl else Qt.LayoutDirection.LeftToRight)
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.page_nav.isVisible())
        prev, nxt, label = pane.page_prev, pane.page_next, pane.page_label
        assert prev.layoutDirection() == nxt.layoutDirection() == Qt.LayoutDirection.LeftToRight
        assert prev.text() == nxt.text() == ""
        if rtl:      # previous on the right of the label, pointing right; next on its left, pointing left
            assert prev.geometry().left() > label.geometry().right() and prev.arrowType() == Qt.ArrowType.RightArrow
            assert nxt.geometry().right() < label.geometry().left() and nxt.arrowType() == Qt.ArrowType.LeftArrow
        else:
            assert prev.geometry().right() < label.geometry().left() and prev.arrowType() == Qt.ArrowType.LeftArrow
            assert nxt.geometry().left() > label.geometry().right() and nxt.arrowType() == Qt.ArrowType.RightArrow
    finally:
        _close(dialog)
        QApplication.instance().setLayoutDirection(Qt.LayoutDirection.LeftToRight)


# -- zoom -----------------------------------------------------------------------------------------------

def test_the_picture_zooms_with_the_buttons_and_the_wheel_and_a_sharper_copy_comes(svc, monkeypatch):
    from PyQt6.QtCore import QPoint, QPointF
    from PyQt6.QtGui import QWheelEvent
    from PyQt6.QtWidgets import QApplication
    fetched = []

    def image_at(url):
        fetched.append(url)
        return _image(1200 if "/full/1200,/" in url else 400)
    monkeypatch.setattr(picker, "catalogue", _Meta(2))
    monkeypatch.setattr(picker, "_image_at", image_at)
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.image.isVisible() and pane.image_bar.isVisible())
        view = pane.image
        assert view.zoom == 1.0
        fitted = view.pixmap().width()
        assert fitted <= view.width()
        pane.zoom_in.click()
        assert view.zoom == pytest.approx(1.25) and view.pixmap().width() > fitted
        # a sharper copy of this page asked for once, at 1,200 pixels, and put in place at the same zoom
        assert _wait(lambda: any("/full/1200,/" in u for u in fetched))
        assert _wait(lambda: view._source is not None and view._source.width() == 1200)
        assert view.zoom == pytest.approx(1.25)
        pane.zoom_in.click()
        assert len([u for u in fetched if "/full/1200,/" in u]) == 1
        # the wheel over the picture: down zooms out, never below fitted
        center = QPointF(view.width() / 2, view.height() / 2)
        for _ in range(6):
            event = QWheelEvent(center, view.mapToGlobal(center), QPoint(0, 0), QPoint(0, -120),
                                Qt.MouseButton.NoButton, Qt.KeyboardModifier.NoModifier,
                                Qt.ScrollPhase.NoScrollPhase, False)
            QApplication.sendEvent(view.viewport(), event)
        assert view.zoom == 1.0
        event = QWheelEvent(center, view.mapToGlobal(center), QPoint(0, 0), QPoint(0, 120),
                            Qt.MouseButton.NoButton, Qt.KeyboardModifier.NoModifier,
                            Qt.ScrollPhase.NoScrollPhase, False)
        QApplication.sendEvent(view.viewport(), event)
        assert view.zoom == pytest.approx(1.25)
        pane.zoom_out.click()
        pane.zoom_out.click()
        assert view.zoom == 1.0
        # the next page opens fitted
        pane.zoom_in.click()
        pane.page_next.click()
        assert _wait(lambda: pane.page == 1 and view.zoom == 1.0)
    finally:
        _close(dialog)


# -- the thumbnails' sizes -----------------------------------------------------------------------------

def test_the_large_sizes_ask_for_sharper_pictures_and_a_narrower_one_never_replaces_one():
    assert picker.FETCH_WIDTHS == {"s": 400, "m": 400, "l": 600, "xl": 800}
    assert set(picker.FETCH_WIDTHS) == set(picker.SIZES)
    sid = "990000000000001"
    picker.keep_thumbnail(sid, _image(800), 800)
    picker.keep_thumbnail(sid, _image(400), 400)                 # a late narrower answer
    assert picker.cached_width(sid) == 800 and picker.cached_thumbnail(sid).width() == 800


def test_the_gallery_at_extra_large_asks_again_for_a_picture_it_has_at_400(svc, monkeypatch):
    asked = []
    monkeypatch.setattr(picker, "catalogue", object())
    monkeypatch.setattr(picker, "_thumbnail_image",
                        lambda sid, meta, width=400: asked.append((sid, width)) or _image(width))
    dialog = _open(svc, start="vol:100")
    try:
        panel = dialog.leaf_panel
        panel.set_view("gallery")
        panel.set_size("m")
        assert _wait(lambda: asked)
        assert {w for _s, w in asked} == {400}
        # the top tile (it stays at the top, in sight, at the larger size); not the first answered: three
        # load at once, and a tile further along may be out of sight at extra large
        first = panel.top_leaf().sys_id
        assert _wait(lambda: dialog.thumb_sharp(first))
        panel.set_size("xl")
        assert not dialog.thumb_sharp(first)                     # 400 is not sharp enough at extra large
        assert _wait(lambda: (first, 800) in asked)
        assert _wait(lambda: dialog.thumb_sharp(first))
        assert dialog.known_thumb(first)[1].width() == 800
    finally:
        _close(dialog)


# -- the keyboard (owner, 2026-11-25) --------------------------------------------------------------------

def _key(widget, key, mods=Qt.KeyboardModifier.NoModifier):
    from PyQt6.QtTest import QTest
    QTest.keyClick(widget, key, mods)
    _wait(lambda: False, timeout=0.05)


def _focus():
    from PyQt6.QtWidgets import QApplication
    return QApplication.focusWidget()


def test_the_columns_arrows_open_a_row_and_go_back_and_tab_leaves_a_table(svc):
    dialog = _open(svc, start="vol:100")
    try:
        dialog.activateWindow()
        lists = dialog._shown_lists()
        assert len(lists) >= 2
        assert all(not c.table.tabKeyNavigation() for c in dialog.node_columns)
        assert not dialog.leaf_panel.table.tabKeyNavigation()
        first = dialog.node_columns[0]
        # back from the second list: the first
        second = lists[1]
        dialog._focus_list(second)
        _key(second, Qt.Key.Key_Left)
        assert _focus() is first.table
        # forward on the row that is open already: into the list after it
        opened = next(r for r in range(len(first.rows)) if dialog._opened_here(first, r))
        first.table.setCurrentCell(opened, first.name_col)
        _key(first.table, Qt.Key.Key_Right)
        assert _focus() is lists[1]
        # forward on another row: it opens, and the keyboard goes into its list once drawn
        other = next(r for r in range(len(first.rows))
                     if not dialog._opened_here(first, r) and 'id' in first.rows[r] and 'leaf' not in first.rows[r])
        want = first.rows[other]['id']
        dialog._focus_list(first.table)
        first.table.setCurrentCell(other, first.name_col)
        _key(first.table, Qt.Key.Key_Right)
        assert _wait(lambda: want in dialog._path and not dialog._loads.busy())
        assert _wait(lambda: _focus() in dialog._shown_lists() and _focus() is not first.table)
    finally:
        _close(dialog)


def test_ctrl_arrows_step_the_pane_s_images_and_plus_minus_zero_zoom_from_a_list(svc, monkeypatch):
    monkeypatch.setattr(picker, "catalogue", _Meta(3))
    monkeypatch.setattr(picker, "_image_at", lambda url: _image(400))
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.page_nav.isVisible())
        grid = dialog.leaf_panel.grid
        grid.setFocus()
        _key(grid, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        assert pane.page == 1
        _key(grid, Qt.Key.Key_Left, Qt.KeyboardModifier.ControlModifier)
        assert pane.page == 0
        _key(grid, Qt.Key.Key_Plus)
        assert pane.image.zoom == pytest.approx(1.25)
        _key(grid, Qt.Key.Key_Equal)
        assert pane.image.zoom == pytest.approx(1.5625)
        _key(grid, Qt.Key.Key_Minus)
        assert pane.image.zoom == pytest.approx(1.25)
        _key(grid, Qt.Key.Key_0)
        assert pane.image.zoom == 1.0
    finally:
        _close(dialog)


def test_ctrl_arrows_follow_the_arrows_on_screen_in_hebrew(svc, monkeypatch):
    from PyQt6.QtWidgets import QApplication
    monkeypatch.setattr(picker, "catalogue", _Meta(3))
    monkeypatch.setattr(picker, "_image_at", lambda url: _image(400))
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    QApplication.instance().setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.page_nav.isVisible())
        grid = dialog.leaf_panel.grid
        grid.setFocus()
        _key(grid, Qt.Key.Key_Left, Qt.KeyboardModifier.ControlModifier)       # next is on the left
        assert pane.page == 1
        _key(grid, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        assert pane.page == 0
    finally:
        _close(dialog)
        QApplication.instance().setLayoutDirection(Qt.LayoutDirection.LeftToRight)


def test_ctrl_f_goes_to_the_list_s_filter_and_esc_back_and_alt_d_hides_the_pane(svc):
    dialog = _open(svc, start="vol:100")
    try:
        dialog.activateWindow()
        first = dialog.node_columns[0]
        dialog._focus_list(first.table)
        _key(first.table, Qt.Key.Key_F, Qt.KeyboardModifier.ControlModifier)
        assert _focus() is first.filter
        _key(first.filter, Qt.Key.Key_Escape)
        assert _focus() is first.table and dialog.isVisible()           # Esc in a filter never closes
        grid = dialog.leaf_panel.grid
        dialog._focus_list(grid)
        _key(grid, Qt.Key.Key_F, Qt.KeyboardModifier.ControlModifier)
        assert _focus() is dialog.leaf_panel.filter
        _key(dialog.leaf_panel.filter, Qt.Key.Key_Escape)
        assert _focus() is grid
        assert dialog.details_button.isChecked()
        _key(grid, Qt.Key.Key_D, Qt.KeyboardModifier.AltModifier)
        assert not dialog.details_button.isChecked() and not dialog.details.isVisible()
        _key(grid, Qt.Key.Key_D, Qt.KeyboardModifier.AltModifier)
        assert dialog.details_button.isChecked()
    finally:
        _close(dialog)


def test_one_two_three_switch_the_view_and_brackets_the_thumbnails_size_from_a_list(svc, monkeypatch):
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: None)
    dialog = _open(svc, start="vol:100")
    try:
        panel = dialog.leaf_panel
        panel.set_view("list")
        dialog._focus_list(panel.active())
        _key(panel.active(), Qt.Key.Key_2)
        assert panel.view == "table" and _focus() is panel.table
        _key(panel.table, Qt.Key.Key_3)
        assert panel.view == "gallery" and _focus() is panel.gallery
        panel.set_size("m")
        _key(panel.gallery, Qt.Key.Key_BracketRight)
        assert panel.size == "l" and panel.size_box.currentData() == "l"
        _key(panel.gallery, Qt.Key.Key_BracketRight)
        _key(panel.gallery, Qt.Key.Key_BracketRight)              # no larger than extra large
        assert panel.size == "xl"
        _key(panel.gallery, Qt.Key.Key_BracketLeft)
        assert panel.size == "l"
        _key(panel.gallery, Qt.Key.Key_1)
        assert panel.view == "list" and _focus() is panel.grid
        # a node column's list switches the shelfmarks too
        first = dialog.node_columns[0]
        dialog._focus_list(first.table)
        _key(first.table, Qt.Key.Key_3)
        assert panel.view == "gallery"
    finally:
        _close(dialog)


def test_tab_goes_round_the_lists_alone_a_filter_s_tab_into_its_list_and_f6_out_and_back(svc):
    dialog = _open(svc, start="vol:100")
    try:
        dialog.activateWindow()
        lists = dialog._shown_lists()
        assert len(lists) >= 3
        dialog._focus_list(lists[0])
        for want in lists[1:] + [lists[0]]:                      # the filters passed by; round from the last
            _key(_focus(), Qt.Key.Key_Tab)
            assert _focus() is want
        _key(_focus(), Qt.Key.Key_Backtab, Qt.KeyboardModifier.ShiftModifier)
        assert _focus() is lists[-1]
        # Tab in a filter: into its list
        first = dialog.node_columns[0]
        first.filter.setFocus()
        _key(first.filter, Qt.Key.Key_Tab)
        assert _focus() is first.table
        # F6: out of the lists (no list, no filter), then back into the list last in use
        _key(first.table, Qt.Key.Key_F6)
        out = _focus()
        filters = {c.filter for c in dialog.node_columns} | {dialog.leaf_panel.filter}
        assert out is not None and out not in lists and out not in filters and dialog.isAncestorOf(out)
        _key(out, Qt.Key.Key_F6)
        assert _focus() is first.table
        _key(first.table, Qt.Key.Key_F6, Qt.KeyboardModifier.ShiftModifier)
        before = _focus()
        assert before not in lists and before not in filters
    finally:
        _close(dialog)


def test_alt_d_and_ctrl_f_by_the_key_on_a_hebrew_layout(svc):
    # Seen in the app: on a Hebrew keyboard Alt+D is Alt+ג (and Ctrl+F Ctrl+כ); the QShortcuts match the
    # letter, so the dialog answers the key (Windows' virtual key) itself
    from PyQt6.QtCore import QEvent
    from PyQt6.QtGui import QKeyEvent
    from PyQt6.QtWidgets import QApplication

    def hebrew(widget, letter, vk, mods):
        for kind in (QEvent.Type.KeyPress, QEvent.Type.KeyRelease):
            event = QKeyEvent(kind, ord(letter), mods, 0, vk, 0, letter)
            QApplication.sendEvent(widget, event)
        _wait(lambda: False, timeout=0.05)

    dialog = _open(svc, start="vol:100")
    try:
        grid = dialog.leaf_panel.grid
        dialog._focus_list(grid)
        assert dialog.details_button.isChecked()
        hebrew(grid, "ג", 0x44, Qt.KeyboardModifier.AltModifier)           # Alt+ג
        assert not dialog.details_button.isChecked()
        first = dialog.node_columns[0]
        dialog._focus_list(first.table)
        hebrew(first.table, "כ", 0x46, Qt.KeyboardModifier.ControlModifier)  # Ctrl+כ
        assert _focus() is first.filter
        hebrew(first.filter, "ג", 0x44, Qt.KeyboardModifier.AltModifier)    # from a filter too
        assert dialog.details_button.isChecked()
    finally:
        _close(dialog)


@pytest.mark.parametrize("named", [0x05DB, int(Qt.Key.Key_F)])
def test_ctrl_f_by_the_key_from_anywhere_in_the_picker(svc, named):
    # Whatever key Qt names (the Hebrew letter, or F) and wherever the keyboard is (here a button, not a
    # list nor a filter), the application's filter answers the key while the picker is in front
    from PyQt6.QtCore import QEvent
    from PyQt6.QtGui import QKeyEvent
    from PyQt6.QtWidgets import QApplication
    dialog = _open(svc, start="vol:100")
    try:
        dialog.details_button.setFocus()
        event = QKeyEvent(QEvent.Type.KeyPress, named, Qt.KeyboardModifier.ControlModifier, 0, 0x46, 0, "")
        QApplication.sendEvent(dialog.details_button, event)
        assert _focus() is dialog.leaf_panel.filter
    finally:
        _close(dialog)


def test_s_turns_the_multiple_choice_on_and_enter_opens_what_is_ticked(svc):
    # Seen in the app: with several at once, Enter ticked like Space and never opened
    confirmed = []
    dialog = _open(svc, start="vol:100", switch_label="Open more than one", purpose="test",
                   on_confirm=lambda entries, label: confirmed.append([e["sys_id"] for e in entries]))
    try:
        grid = dialog.leaf_panel.grid
        dialog._focus_list(grid)
        assert not dialog.multi
        _key(grid, Qt.Key.Key_S)
        assert dialog.multi and dialog.switch.isChecked()
        grid = dialog.leaf_panel.active()
        assert _wait(lambda: grid.count() > 2)
        grid.setFocus()
        grid.setCurrentRow(1)
        first = grid.item(1).data(Qt.ItemDataRole.UserRole).sys_id
        _key(grid, Qt.Key.Key_Return)                          # nothing ticked: this one, then Open
        assert _wait(lambda: confirmed)
        assert confirmed == [[first]]
        _key(grid, Qt.Key.Key_S)                               # and off again
        assert not dialog.multi
    finally:
        _close(dialog)


def test_the_picture_turns_a_quarter_keeps_its_turn_across_pages_and_not_across_shelfmarks(svc, monkeypatch):
    monkeypatch.setattr(picker, "catalogue", _Meta(2))
    monkeypatch.setattr(picker, "_image_at", lambda url: _image(400))
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, SYS["as2"][0])
        assert _wait(lambda: pane.page_nav.isVisible())
        view = pane.image
        wide = view.pixmap().size()
        assert wide.width() > wide.height()                    # a 400 x 200 picture, fitted
        pane.rotate_right.click()
        assert view.rotation == 90
        tall = view.pixmap().size()
        assert tall.height() > tall.width()
        pane.rotate_left.click()
        pane.rotate_left.click()
        assert view.rotation == 270
        pane.page_next.click()
        assert _wait(lambda: pane.page == 1)
        assert view.rotation == 270                            # another page of it: still turned
        _click_item(dialog, SYS["as2"][1])
        assert _wait(lambda: view.rotation == 0)               # another shelfmark: upright
    finally:
        _close(dialog)


def test_a_picture_under_way_when_the_tiles_grow_is_asked_again_sharper(svc, monkeypatch):
    # Codex on #129: a tile loading at 400 when the size went to extra large was left out of the new queue
    # (under way already) and kept its 400 picture until the next scroll
    import threading
    asked, release = [], threading.Event()

    def thumb(sid, meta, width=400):
        asked.append((sid, width))
        if width == 400:
            release.wait(5)
        return _image(width)
    monkeypatch.setattr(picker, "catalogue", object())
    monkeypatch.setattr(picker, "_thumbnail_image", thumb)
    dialog = _open(svc, start="vol:100")
    try:
        panel = dialog.leaf_panel
        panel.set_view("gallery")
        panel.set_size("m")
        assert _wait(lambda: asked)
        top = panel.top_leaf().sys_id
        assert _wait(lambda: (top, 400) in asked)
        panel.set_size("xl")                          # while the 400 one is still under way
        _wait(lambda: False, timeout=0.3)
        release.set()
        assert _wait(lambda: (top, 800) in asked)
        assert _wait(lambda: dialog.thumb_sharp(top) and dialog.known_thumb(top)[1].width() == 800)
    finally:
        release.set()
        _close(dialog)

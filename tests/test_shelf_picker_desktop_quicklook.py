# -*- coding: utf-8 -*-
"""The desktop shelf picker's quick look (owner, 2026-11-25): one shelfmark's picture filling the
picker, sized to it, its pages and the list's shelfmarks behind the arrows, zoom and turn, and back to
the list at the shelfmark looked at last.

The real dialog on the fixture tree; the profile, the language and the hooks are round 4's.
"""
from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication

import genizah_core
import desktop.widgets.shelfmark_tree_picker as picker
from shared.shelfmark_tree_details import LOOK_MAX, LOOK_MIN, look_sharp_width, look_width
from shared.shelfmark_tree_service import ShelfmarkTreeService
from tests.shelfmark_tree_fixture import SYS, build_fixture
from tests.test_shelf_picker_desktop_round4 import _click_item, _close, _open, _switch_picker, _wait
from tests.test_shelf_picker_desktop_round5 import _Meta, _focus, _image, _key

pytestmark = pytest.mark.gui


@pytest.fixture(autouse=True)
def _isolated_profile(tmp_path, monkeypatch):
    for var in ("USERPROFILE", "HOME", "LOCALAPPDATA", "APPDATA"):
        folder = tmp_path / "profile" / var
        folder.mkdir(parents=True)
        monkeypatch.setenv(var, str(folder))
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.delenv("SHELFMARK_TREE_ENABLED", raising=False)
    for hook in ("title_of", "catalogue", "page_count_of", "library_code_of", "page_texts_of", "community", "signs"):
        monkeypatch.setattr(picker, hook, None)
    monkeypatch.setattr(picker, "_thumb_cache", picker.OrderedDict())
    monkeypatch.setattr(picker, "_thumb_width", {})


@pytest.fixture
def svc(tmp_path):
    db, _libraries, _meta = build_fixture(tmp_path)
    service = ShelfmarkTreeService(str(db), thread_safe=True)
    yield service
    service.close()


@pytest.fixture
def fetched(monkeypatch):
    """Every image address asked for; each answered with a picture as wide as it asks (3:4)."""
    asked = []

    def image_at(url):
        asked.append(url)
        width = int(url.split("/full/")[1].split(",")[0])
        return _image(width)
    monkeypatch.setattr(picker, "catalogue", _Meta(3))
    monkeypatch.setattr(picker, "_image_at", image_at)
    monkeypatch.setattr(picker, "_thumbnail_image", lambda sid, meta, width=400: _image(400))
    return asked


def _look_from_list(dialog, sid):
    _click_item(dialog, sid)
    grid = dialog.leaf_panel.grid
    grid.setFocus()
    _key(grid, Qt.Key.Key_Q)
    look = dialog.look
    assert look.isVisible() and look.leaf is not None and look.leaf.sys_id == sid
    return look


def test_the_width_asked_fills_the_box_in_steps_between_the_tiles_and_the_cap():
    assert look_width(1000, 900, 0.75) == 800           # fitted by its height: 675, stepped up
    assert look_width(500, 2000, 0.75) == 600           # by its width
    assert look_width(100, 100) == LOOK_MIN and look_width(5000, 5000, 1.0) == LOOK_MAX
    assert look_width(1000, 900, 0) == look_width(1000, 900, 0.75)   # unknown: a portrait leaf
    assert look_sharp_width(800) == 1600 and look_sharp_width(1600) == 2000 and look_sharp_width(2000) == 2000


def test_q_opens_it_over_the_picker_sized_to_it_and_esc_goes_back_to_the_list(svc, fetched):
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, SYS["as2"][0])
        assert look.geometry() == dialog.rect()
        assert _focus() is look
        assert _wait(lambda: any(f"FL{SYS['as2'][0]}1/full/{look.width_asked}," in u for u in fetched))
        assert LOOK_MIN <= look.width_asked <= LOOK_MAX and look.width_asked % 200 == 0
        assert _wait(lambda: look.image._source is not None and look.image._source.width() == look.width_asked)
        assert look.where.text() == f"1 of {len(look.rows())}"
        assert look.page_label.text() == "Image 1 of 3"
        _key(look, Qt.Key.Key_Escape)
        assert not look.isVisible() and dialog.isVisible()          # Esc closed the quick look alone
        assert _focus() is dialog.leaf_panel.grid
    finally:
        _close(dialog)


def test_the_arrows_go_through_the_list_and_back_lands_on_the_last_one_chosen(svc, fetched):
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, SYS["as2"][0])
        _key(look, Qt.Key.Key_Right)
        _key(look, Qt.Key.Key_Right)
        assert look.leaf.sys_id == SYS["as2"][2] and look.where.text().startswith("3 of")
        _key(look, Qt.Key.Key_Left)
        assert look.leaf.sys_id == SYS["as2"][1]
        _key(look, Qt.Key.Key_End)
        assert look.leaf.id == look.rows()[-1].id
        _key(look, Qt.Key.Key_Home)
        _key(look, Qt.Key.Key_Right)
        _key(look, Qt.Key.Key_Q)                                     # Q closes it too
        assert not look.isVisible()
        assert dialog.leaf_panel.current_leaf().sys_id == SYS["as2"][1]
        assert dialog._chosen_leaf.sys_id == SYS["as2"][1]           # chosen, as a click chooses it
        assert _wait(lambda: dialog.details.leaf_id == dialog._chosen_leaf.id)
    finally:
        _close(dialog)


def test_ctrl_arrows_and_up_down_step_the_pages_and_the_next_is_fetched_ahead(svc, fetched):
    sid = SYS["as2"][0]
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, sid)
        assert _wait(lambda: any(f"FL{sid}2/full/{look.width_asked}," in u for u in fetched))   # ahead
        _key(look, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        assert look.page == 1 and look.page_label.text() == "Image 2 of 3"
        assert look.leaf.sys_id == sid                               # the same shelfmark
        _key(look, Qt.Key.Key_Down)
        assert look.page == 2 and not look.page_next.isEnabled()
        _key(look, Qt.Key.Key_PageUp)
        assert look.page == 1
        assert len([u for u in fetched if f"FL{sid}2/full/{look.width_asked}," in u]) == 1   # kept
    finally:
        _close(dialog)


def test_the_next_shelfmark_s_picture_is_fetched_ahead(svc, fetched):
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, SYS["as2"][0])
        nxt = SYS["as2"][1]
        # this one's next page first, then the next shelfmark's first image
        _key(look, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        _key(look, Qt.Key.Key_Right, Qt.KeyboardModifier.ControlModifier)
        assert _wait(lambda: any(f"FL{nxt}1/full/{look.width_asked}," in u for u in fetched))
        before = len(fetched)
        _key(look, Qt.Key.Key_Right)
        assert look.leaf.sys_id == nxt
        assert look.image._source is not None and look.image._source.width() == look.width_asked   # at once
        _wait(lambda: False, timeout=0.3)
        assert not any(f"FL{nxt}1/full/{look.width_asked}," in u for u in fetched[before:])
    finally:
        _close(dialog)


def test_zoom_asks_for_a_sharper_copy_once_and_r_turns_it(svc, fetched):
    sid = SYS["as2"][0]
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, sid)
        base = look.width_asked
        assert _wait(lambda: look.image._source is not None and look.image._source.width() == base)
        _key(look, Qt.Key.Key_Plus)
        assert look.image.zoom == pytest.approx(1.25)
        sharp = look_sharp_width(base)
        assert _wait(lambda: look.image._source is not None and look.image._source.width() == sharp)
        assert look.image.zoom == pytest.approx(1.25)                # put in place at the same zoom
        _key(look, Qt.Key.Key_Plus)
        assert len([u for u in fetched if f"/full/{sharp}," in u]) == 1
        _key(look, Qt.Key.Key_0)
        assert look.image.zoom == 1.0
        _key(look, Qt.Key.Key_R)
        assert look.image.rotation == 90
        _key(look, Qt.Key.Key_R, Qt.KeyboardModifier.ShiftModifier)
        assert look.image.rotation == 0
    finally:
        _close(dialog)


def test_the_pane_s_button_opens_it_at_the_pane_s_page_and_turn(svc, fetched):
    sid = SYS["as2"][0]
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, sid)
        assert _wait(lambda: pane.page_nav.isVisible())
        pane.step_page(1)
        assert _wait(lambda: pane.image._source is not None)
        pane.image.rotate_by(90)
        assert pane.image.rotation == 90
        asked = len(fetched)
        pane.look_button.click()
        look = dialog.look
        assert look.isVisible() and look.page == 1 and look.image.rotation == 90
        assert look.page_label.text() == "Image 2 of 3"
        assert _wait(lambda: any(f"FL{sid}2/full/{look.width_asked}," in u for u in fetched[asked:]))
    finally:
        _close(dialog)


def test_several_at_once_space_ticks_it_and_the_box_follows(svc, fetched):
    dialog = _switch_picker(svc)
    try:
        dialog.switch.setChecked(True)
        assert _wait(lambda: dialog.multi and not dialog._loads.busy())
        dialog.leaf_panel.grid.setFocus()
        dialog.leaf_panel.grid.setCurrentRow(0)
        sid = dialog.leaf_panel.current_leaf().sys_id
        _key(dialog.leaf_panel.grid, Qt.Key.Key_Q)
        look = dialog.look
        assert look.isVisible() and look.tick.isVisible() and not look.act.isVisible()
        assert not look.tick.isChecked()
        _key(look, Qt.Key.Key_Space)
        members = picker.members_function(svc)
        assert sid in dialog.chosen().chosen_set(members)
        assert _wait(lambda: look.tick.isChecked())
        _key(look, Qt.Key.Key_Space)
        assert _wait(lambda: not look.tick.isChecked())
        assert sid not in dialog.chosen().chosen_set(members)
    finally:
        _close(dialog)


def test_enter_in_the_single_choice_takes_it(svc, fetched):
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, SYS["as2"][0])
        assert look.act.isVisible() and not look.tick.isVisible()
        _key(look, Qt.Key.Key_Right)
        _key(look, Qt.Key.Key_Return)
        assert dialog.selection() is not None and dialog.selection()["sys_id"] == SYS["as2"][1]
    finally:
        if dialog.isVisible():
            _close(dialog)


def test_tab_and_ctrl_f_stay_with_it_and_the_hebrew_arrows_are_mirrored(svc, fetched):
    QApplication.instance().setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, SYS["as2"][0])
        _key(look, Qt.Key.Key_Tab)
        assert _focus() is look
        _key(look, Qt.Key.Key_F, Qt.KeyboardModifier.ControlModifier)
        assert _focus() is look and look.isVisible()
        _key(look, Qt.Key.Key_Left)                                   # next is on the left
        assert look.leaf.sys_id == SYS["as2"][1]
        assert look.leaf_next.arrowType() == Qt.ArrowType.LeftArrow
        _key(look, Qt.Key.Key_Right)
        assert look.leaf.sys_id == SYS["as2"][0]
    finally:
        QApplication.instance().setLayoutDirection(Qt.LayoutDirection.LeftToRight)
        _close(dialog)


def test_past_the_list_s_end_it_goes_on_in_the_shelf_s_order_and_back_there(svc, fetched):
    # owner, 2026-11-25: past the current group to the next item in the next group, and back
    dialog = _open(svc, start="vol:100")
    try:
        look = _look_from_list(dialog, SYS["as2"][0])
        rows = look.rows()
        last = rows[-1]
        _key(look, Qt.Key.Key_End)
        _, after = svc.neighbours(last.id)
        assert after is not None and all(r.id != after.id for r in rows)
        _key(look, Qt.Key.Key_Right)
        assert _wait(lambda: look.leaf.id == after.id)
        group = svc.node(after.node_id)
        assert look.where.text() == group.name_en                   # its group's name, outside the list
        _key(look, Qt.Key.Key_Left)
        assert _wait(lambda: look.leaf.id == last.id)                 # back into the list
        assert look.where.text() == f"{len(rows)} of {len(rows)}"
        _key(look, Qt.Key.Key_Right)
        assert _wait(lambda: look.leaf.id == after.id)
        _key(look, Qt.Key.Key_Escape)                                 # the picker goes to its group
        assert _wait(lambda: dialog._chosen_leaf is not None and dialog._chosen_leaf.sys_id == after.sys_id
                     and not dialog._loads.busy())
        assert any(r.id == after.id for r in look.rows())
    finally:
        _close(dialog)


def test_shift_arrows_step_the_images_and_the_tooltips_name_the_keys(svc, fetched):
    sid = SYS["as2"][0]
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        look = _look_from_list(dialog, sid)
        _key(look, Qt.Key.Key_Right, Qt.KeyboardModifier.ShiftModifier)
        assert look.page == 1 and look.leaf.sys_id == sid
        assert look.zoom_in.toolTip() == "Zoom in (+)" and look.zoom_out.toolTip() == "Zoom out (-)"
        assert look.rotate_right.toolTip() == "Rotate right (R)" and look.rotate_left.toolTip() == "Rotate left (Shift+R)"
        assert look.page_next.toolTip() == "Next image (Shift+\u2192)"
        assert look.leaf_next.toolTip() == "Next shelfmark (\u2192)"
        _key(look, Qt.Key.Key_Escape)
        assert pane.zoom_in.toolTip() == "Zoom in (+)" and pane.rotate_left.toolTip() == "Rotate left (Shift+R)"
        assert _wait(lambda: pane.page_nav.isVisible())
        assert pane.page_prev.toolTip() == "Previous image (Shift+\u2190)"
        grid = dialog.leaf_panel.grid
        grid.setFocus()
        _key(grid, Qt.Key.Key_Right, Qt.KeyboardModifier.ShiftModifier)
        assert pane.page == 1
    finally:
        _close(dialog)


def test_a_microfilm_opens_on_its_middle_image_in_the_pane_and_the_quick_look(svc, monkeypatch):
    sid = SYS["as2"][0]
    asked = []

    def image_at(url):
        asked.append(url)
        return _image(int(url.split("/full/")[1].split(",")[0]))
    monkeypatch.setattr(picker, "catalogue", _Meta(9))
    monkeypatch.setattr(picker, "_image_at", image_at)
    monkeypatch.setattr(picker, "_thumbnail_image", lambda s, meta, width=400: _image(400))
    monkeypatch.setattr(picker, "library_code_of", lambda s: "RNL")
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, sid)
        assert _wait(lambda: pane.page_nav.isVisible())
        assert pane.page == 4 and pane.page_label.text() == "Image 5 of 9"
        # the tile is that image: shown at once, not asked for again; the one after it is, ahead
        assert _wait(lambda: any(f"FL{sid}6/full/400," in u for u in asked))
        assert not pane.image.pixmap().isNull() and not any(f"FL{sid}5/full/400," in u for u in asked)
        grid = dialog.leaf_panel.grid
        grid.setFocus()
        _key(grid, Qt.Key.Key_Q)                                     # the pane's shelfmark: at its page
        look = dialog.look
        assert look.page == 4 and look.page_label.text() == "Image 5 of 9"
        _key(look, Qt.Key.Key_Right)                                 # another one: its own middle
        assert _wait(lambda: look.leaf.sys_id == SYS["as2"][1] and look.pages)
        assert look.page == 4
    finally:
        _close(dialog)


def test_q_on_the_pane_s_shelfmark_opens_at_the_pane_s_image_and_turn(svc, fetched):
    sid = SYS["as2"][0]
    dialog = _open(svc, start="vol:100")
    try:
        pane = dialog.details
        _click_item(dialog, sid)
        assert _wait(lambda: pane.page_nav.isVisible())
        pane.step_page(1)
        pane.step_page(1)
        assert _wait(lambda: pane.image._source is not None)
        pane.image.rotate_by(-90)
        grid = dialog.leaf_panel.grid
        grid.setFocus()
        _key(grid, Qt.Key.Key_Q)
        look = dialog.look
        assert look.page == 2 and look.page_label.text() == "Image 3 of 3" and look.image.rotation == 270
    finally:
        _close(dialog)

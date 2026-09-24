"""OverflowRow / MenuGroupButton: one-line rows that never push a window off a small screen.

Pinned here: what does not fit moves into the More menu by priority; a button's
own shown/hidden state is never touched by overflow; the menu mirrors each button
(text, enabled, checked) and clicks the real one; RTL mirrors; the minimum width
is the pinned widgets plus More; a group button tracks whether any member applies.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QLabel, QPushButton, QToolButton, QVBoxLayout, QWidget

from desktop.widgets.overflow_row import PINNED, MenuGroupButton, OverflowRow, populate_menu_from_buttons

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


def _btn(text, w=100, h=30):
    b = QPushButton(text)
    b.setFixedSize(w, h)
    return b


def _row(width, rtl=False):
    host = QWidget()
    if rtl:
        host.setLayoutDirection(Qt.LayoutDirection.RightToLeft)
    lay = QVBoxLayout(host)
    lay.setContentsMargins(0, 0, 0, 0)
    row = OverflowRow("More")
    row.more_button.setFixedSize(60, 30)
    lay.addWidget(row)
    host.resize(width, 40)
    return host, row


def _settle(host):
    host.show()
    QApplication.processEvents()
    host.layout().activate()
    QApplication.processEvents()


def test_everything_fits_on_a_wide_row():
    host, row = _row(1000)
    bs = [row.add(_btn(f"b{i}")) for i in range(4)]
    _settle(host)
    assert row.overflowed() == []
    xs = [b.geometry().x() for b in bs]
    assert xs == sorted(xs) and all(b.width() == 100 for b in bs)
    assert not row.rect().intersects(row.more_button.geometry())   # parked outside


def test_narrow_row_moves_the_highest_priority_number_first():
    host, row = _row(400)
    a = row.add(_btn("a"), priority=0)
    b = row.add(_btn("b"), priority=3)
    c = row.add(_btn("c"), priority=1)
    d = row.add(_btn("d"), priority=3)
    _settle(host)
    # 4x100 + 3x6 = 418 > 400: with More (60) we can keep 3 buttons? 3x100+60+3x6 = 378 -> drop one.
    assert row.overflowed() == [d]          # tie on 3: the later one leaves first
    assert row.rect().contains(row.more_button.geometry())
    assert not row.rect().intersects(d.geometry()) and not d.isHidden()   # parked, never hidden
    assert d.focusPolicy() == Qt.FocusPolicy.NoFocus
    for w in (a, b, c):
        assert w.width() == 100


def test_pinned_widgets_never_overflow_and_set_the_minimum():
    host, row = _row(200)
    lbl = QLabel("Pinned")
    lbl.setFixedSize(80, 30)
    row.add(lbl, priority=PINNED)
    row.add(_btn("x"), priority=0)
    row.add(_btn("y"), priority=0)
    _settle(host)
    assert lbl.width() == 80
    assert row.minimumSizeHint().width() == 80 + 6 + 60


def test_hidden_buttons_are_not_offered_and_keep_their_state():
    host, row = _row(180)   # a+b = 206 > 180; a+More = 166 fits
    a = row.add(_btn("a"), priority=0)
    b = row.add(_btn("b"), priority=2)
    c = row.add(_btn("c"), priority=2)
    c.setVisible(False)                     # not applicable for this manuscript
    _settle(host)
    assert c not in row.overflowed()
    assert c.isHidden()
    assert b in row.overflowed() and not b.isHidden()
    c.setVisible(True)                      # its own code shows it again
    _settle(host)
    assert c in row.overflowed() or row.rect().contains(c.geometry())


def test_menu_mirrors_state_and_clicks_the_real_button():
    clicked = []
    b = _btn("Parallels")
    b.clicked.connect(lambda: clicked.append("p"))
    t = _btn("Info")
    t.setCheckable(True)
    t.setChecked(True)
    off = _btn("Catalog (0)")
    off.setEnabled(False)
    gone = _btn("Gone")
    holder = QWidget()
    for w in (b, t, off, gone):
        w.setParent(holder)
    gone.setVisible(False)
    from PyQt6.QtWidgets import QMenu
    m = QMenu()
    assert populate_menu_from_buttons(m, [b, t, off, gone]) == 3
    acts = m.actions()
    assert [a.text() for a in acts] == ["Parallels", "Info", "Catalog (0)"]
    assert acts[1].isCheckable() and acts[1].isChecked()
    assert not acts[2].isEnabled()
    acts[0].trigger()
    assert clicked == ["p"]


def test_symbol_only_tool_button_gets_its_tooltip_as_name():
    from PyQt6.QtWidgets import QMenu
    holder = QWidget()
    cite = QToolButton(holder)
    cite.setText("“”")
    cite.setToolTip("Cite this page")
    m = QMenu()
    populate_menu_from_buttons(m, [cite])
    assert "Cite this page" in m.actions()[0].text()


def test_rtl_row_is_mirrored():
    host, row = _row(1000, rtl=True)
    a = row.add(_btn("a"))
    b = row.add(_btn("b"))
    _settle(host)
    assert a.geometry().right() == row.width() - 1
    assert b.geometry().right() < a.geometry().left()


def test_stretch_item_takes_the_leftover_width():
    host, row = _row(600)
    row.add(_btn("prev"), priority=PINNED)
    lbl = row.add(QLabel("Result 1 of 3"), priority=PINNED, stretch=True)
    row.add(_btn("next"), priority=PINNED)
    _settle(host)
    assert lbl.width() == 600 - 200 - 12


def test_group_button_tracks_whether_any_member_applies():
    host, row = _row(800)
    fj = _btn("Bib FJMS (4)")
    cat = _btn("Catalog (2)")
    fj.setVisible(False)
    cat.setVisible(False)
    grp = MenuGroupButton("Sources", [fj, cat])
    row.add(grp, priority=1)
    _settle(host)
    assert grp.isHidden()
    cat.setVisible(True)                    # metadata arrived
    QApplication.processEvents()
    assert not grp.isHidden()
    grp._fill()
    assert [a.text() for a in grp.menu().actions()] == ["Catalog (2)"]
    cat.setVisible(False)
    QApplication.processEvents()
    assert grp.isHidden()


def test_adding_to_a_row_keeps_a_deliberately_hidden_widget_hidden():
    host, row = _row(800)
    shown = _btn("shown")          # never shown yet: not "hidden on purpose"
    gone = _btn("gone")
    gone.setVisible(False)         # hidden on purpose before it joined the row
    row.add(shown)
    row.add(gone)
    _settle(host)
    assert not shown.isHidden()
    assert gone.isHidden()


def _icon_btn(text, w=None):
    b = QPushButton(text)
    return b


def test_icons_before_menus():
    """Owner, 2026-09-24: shrink labelled icon buttons to their icon first; only
    then move anything into More."""
    host, row = _row(1000)
    bs = [row.add(_icon_btn(t)) for t in ("★ Alpha button", "☆ Beta button",
                                          "♥ Gamma button")]
    _settle(host)
    full = [b.text() for b in bs]
    need_full = sum(b.sizeHint().width() for b in bs)
    host.resize(need_full // 2 + 40, 40)
    _settle(host)
    assert row.overflowed() == []                       # nothing moved to More yet
    short = [b.text() != f for b, f in zip(bs, full)]
    assert any(short), "nothing was shortened"
    # One at a time, least important first (the end of the row, on a tie), and
    # only as many as needed: the shortened ones form a suffix of the row.
    assert short == sorted(short), short
    for b, f, sh in zip(bs, full, short):
        assert b.text() == (f.split()[0] if sh else f)
        assert b.toolTip() == (f if sh else "")         # the label is the tooltip
    host.resize(1000, 40)
    _settle(host)
    assert [b.text() for b in bs] == full               # room again: labels back
    assert [b.toolTip() for b in bs] == ["", "", ""]


def test_a_label_changed_while_shortened_is_the_one_restored():
    host, row = _row(1000)
    other = row.add(_icon_btn("★ A long neighbouring label"))
    t = row.add(_icon_btn("♥ Trans ON"))          # last in the row: shortened first
    _settle(host)
    host.resize((t.sizeHint().width() + other.sizeHint().width()) // 2 + 30, 40)
    _settle(host)
    assert t.text() == "♥"
    t.setText("♥ Trans OFF")                       # the button's own code relabels it
    _settle(host)
    host.resize(1000, 40)
    _settle(host)
    assert t.text() == "♥ Trans OFF"


def test_an_overflowed_button_keeps_its_label_for_the_menu():
    host, row = _row(1000)
    row.add(_btn("keep", w=120), priority=PINNED)
    labels = ["♥ Parallels search", "★ Puzzle board", "♣ Find joins", "♠ Other"]
    for t in labels:
        row.add(_icon_btn(t), priority=3)
    _settle(host)
    host.resize(10, 40)                  # clamps to the row's minimum: pinned + More
    _settle(host)
    over = row.overflowed()
    assert over, "four icons do not fit where only More (60) does"
    for w in over:                       # the menu is built from the button's text
        assert w.text() in labels


def test_group_button_is_never_shortened():
    host, row = _row(1000)
    fj = _btn("Bib FJMS (4)")
    grp = MenuGroupButton("Sources", [fj])
    row.add(grp, priority=1)
    row.add(_icon_btn("♥ Some long label here"))
    _settle(host)
    host.resize(150, 40)
    _settle(host)
    assert grp.text().startswith("Sources")


def test_a_group_folds_into_its_own_menu_only_when_needed():
    host, row = _row(1000)
    row.add(_btn("keep", w=100), priority=PINNED)
    members = [row.add(_btn(f"src{i}", w=100), priority=2, group="Sources") for i in range(3)]
    other = row.add(_btn("other", w=100), priority=3)
    _settle(host)
    grp = row.group_button("Sources")
    assert row.overflowed() == []
    assert not row.rect().intersects(grp.geometry())            # wide: no group menu
    host.resize(100 + 6 + 100 + 6 + 90 + 6 + 90, 40)          # keep + one src + two menus
    _settle(host)
    over = row.overflowed()
    assert other in over                                        # priority 3 leaves first...
    in_group = row.row.overflowed_in("Sources")
    assert in_group and all(m in members for m in in_group)     # ...then sources, into Sources
    assert other in row.row.overflowed_in(None) and other not in in_group
    assert row.rect().intersects(grp.geometry())



def test_a_lazily_filled_tool_button_menu_stays_a_submenu():
    """Codex, #362: the viewer's Joins menu is empty until its aboutToShow fills
    it; in More it must stay a submenu (with the button's own action too, for a
    split button) instead of becoming a plain click."""
    from PyQt6.QtWidgets import QMenu
    holder = QWidget()
    joins = QToolButton(holder)
    joins.setText("\U0001f517")
    joins.setToolTip("View joined fragments")
    joins.setPopupMode(QToolButton.ToolButtonPopupMode.MenuButtonPopup)
    lazy = QMenu(joins)
    lazy.aboutToShow.connect(lambda: lazy.addAction("fragment A") if lazy.isEmpty() else None)
    joins.setMenu(lazy)
    m = QMenu()
    assert populate_menu_from_buttons(m, [joins]) == 2
    acts = m.actions()
    assert acts[0].menu() is None                  # the button's own action
    assert acts[1].menu() is lazy                  # the lazy submenu, still attached
    lazy.aboutToShow.emit()
    assert [a.text() for a in lazy.actions()] == ["fragment A"]

# -*- coding: utf-8 -*-
"""Composition Search conditions: the strip, the dialog and the two result
groups, on real widgets (gate G7 of docs/plans/COMPOSITION_CONDITIONS_PLAN.md).

GUI lane: run with `python scripts/run_gui_tests.py` (one file per process).
Labels are compared with tr(), never with literal text, so the test holds in
either UI language.
"""
from __future__ import annotations

import pytest
from PyQt6.QtWidgets import QApplication, QDialog, QDialogButtonBox, QTreeWidget, QWidget

from desktop import comp_view_filter as _cvf
from desktop.compound_ui import ConditionDialog, ConditionsStrip, condition_summary
from genizah_core import tr
from shared import compound_query as cq
from shared.compound_query import Condition

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

LONG = 'אבגדה' * 12          # 60 letters
SHORT = 'אבג' * 3                       # 9 letters


def _ok(dlg):
    return dlg.buttons.button(QDialogButtonBox.StandardButton.Ok)


def test_dialog_round_trips_a_condition_with_versions():
    # The requirement is fixed from the condition being edited (owner,
    # 2026-10-14) -- there is no requirement combo any more.
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_ADJACENT, (LONG, LONG + LONG), 'HG')
    dlg = ConditionDialog(None, c, floor=40)
    try:
        assert dlg.condition() == c
        assert _ok(dlg).isEnabled()
        assert dlg.lbl_help.isVisibleTo(dlg)          # shown for "must not"
        assert dlg.windowTitle() == tr("Quoting text")
    finally:
        dlg.deleteLater()


def test_dialog_title_and_help_line_follow_the_fixed_requirement():
    dlg = ConditionDialog(None, requirement=cq.REQ_MUST, floor=40)
    try:
        assert not dlg.lbl_help.isVisibleTo(dlg)
        assert dlg.windowTitle() == tr("Search condition")
    finally:
        dlg.deleteLater()

    dlg2 = ConditionDialog(None, requirement=cq.REQ_MUST_NOT, floor=40)
    try:
        assert dlg2.lbl_help.isVisibleTo(dlg2)
        assert dlg2.windowTitle() == tr("Quoting text")
    finally:
        dlg2.deleteLater()


def test_must_not_dialog_defaults_to_anywhere_in_the_manuscript():
    """Owner, 2026-10-14: "Quoting texts..." starts at "Anywhere in the
    manuscript"; "Add condition..." keeps the old first-item default."""
    dlg = ConditionDialog(None, requirement=cq.REQ_MUST_NOT, floor=40)
    try:
        assert dlg.combo_where.currentData() == cq.WHERE_MANUSCRIPT
    finally:
        dlg.deleteLater()

    dlg2 = ConditionDialog(None, requirement=cq.REQ_MUST, floor=40)
    try:
        assert dlg2.combo_where.currentData() == cq.WHERE_SAME_IMAGE
    finally:
        dlg2.deleteLater()


def test_dialog_without_a_fixed_requirement_refuses_to_build():
    with pytest.raises(ValueError):
        ConditionDialog(None, floor=40)


def test_dialog_refuses_an_under_floor_version_and_says_which():
    dlg = ConditionDialog(None, floor=40, requirement=cq.REQ_MUST)
    try:
        assert not _ok(dlg).isEnabled()               # empty
        dlg.edit_text.setPlainText(LONG + '\n\n' + SHORT)
        assert dlg.versions() == [LONG, SHORT]
        assert not _ok(dlg).isEnabled()
        assert tr("Version {} is too short ({} letters; at least {}).").format(
            2, 9, 40) in dlg.lbl_info.text()
        dlg.edit_text.setPlainText(LONG)
        assert _ok(dlg).isEnabled()
    finally:
        dlg.deleteLater()


def _strip(conditions=(), main=1):
    strip = ConditionsStrip(None, floor=lambda: 40, main_searches=lambda: main)
    strip.set_conditions(conditions)
    return strip


def test_strip_rows_count_and_removal():
    a = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (LONG,), 'A')
    b = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, (LONG, LONG), 'B')
    strip = _strip([a, b], main=2)
    seen = []
    strip.changed.connect(seen.append)
    try:
        assert strip._rows_box.count() == 2
        assert strip.lbl_count.text() == tr("{} searches (limit {})").format(5, 25)
        strip._remove(0)
        assert strip.conditions() == [b] and seen == [[b]]
    finally:
        strip.deleteLater()


def test_strip_stops_at_four_and_freezes_outside_letter_level():
    # There is no "+ Add condition" button any more (owner, 2026-10-14): the
    # "Filter texts" menu asks `can_add` / `add_disabled_reason` instead.
    c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (LONG,))
    strip = _strip([c] * 4)
    try:
        assert not strip.can_add()
        assert strip.add_disabled_reason() == tr("At most {} conditions.").format(
            cq.MAX_CONDITIONS)
        strip.set_conditions([c])
        assert strip.can_add()
        assert strip.add_disabled_reason() == ''
        strip.set_available(False, tr("Conditions work only with letter-level search."))
        assert not strip.can_add()
        assert strip.add_disabled_reason() == tr(
            "Conditions work only with letter-level search.")
        assert strip.lbl_disabled.isVisibleTo(strip)
        assert strip.conditions() == [c]              # kept, just frozen
    finally:
        strip.deleteLater()


def test_strip_is_hidden_with_no_conditions_and_shown_with_one():
    strip = _strip()
    try:
        assert strip.isHidden()
        strip.set_conditions([Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (LONG,))])
        assert not strip.isHidden()
        strip.set_conditions([])
        assert strip.isHidden()
    finally:
        strip.deleteLater()


def test_editing_a_row_keeps_its_requirement(monkeypatch):
    def rename_and_accept(dlg):
        # The user edits the row: a new name, the place changed.
        dlg.edit_label.setText('Halakhot Gedolot')
        dlg.combo_where.setCurrentIndex(dlg.combo_where.findData(cq.WHERE_SAME_IMAGE))
        return QDialog.DialogCode.Accepted
    monkeypatch.setattr(ConditionDialog, 'exec', rename_and_accept)
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, (LONG,), 'HG')
    strip = _strip([c])
    seen = []
    strip.changed.connect(seen.append)
    try:
        strip._edit(0)
        [edited] = strip.conditions()
        assert edited.label == 'Halakhot Gedolot'          # the edit landed
        assert edited.where == cq.WHERE_SAME_IMAGE
        assert edited.requirement == cq.REQ_MUST_NOT       # ... and kept the requirement
        assert seen and seen[-1] == [edited]
    finally:
        strip.deleteLater()


def test_a_long_condition_name_elides_instead_of_widening_the_strip():
    name = 'הלכות גדולות, הלכות ברכות, פרק ראשון, מהדורת ורשה עם הגהות ' * 3
    strip = _strip([Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, (LONG,), name)])
    try:
        assert strip.minimumSizeHint().width() < 300
        row_label = strip._rows_box.itemAt(0).widget().layout().itemAt(0).widget()
        assert name.strip() in row_label.toolTip()         # the whole line on hover
    finally:
        strip.deleteLater()


def test_add_condition_owns_the_cap_and_opens_with_the_given_requirement(monkeypatch):
    monkeypatch.setattr(ConditionDialog, 'exec', lambda self: QDialog.DialogCode.Accepted)
    strip = _strip()
    try:
        assert strip.add_condition(cq.REQ_MUST_NOT) is True
        assert strip.conditions()[0].requirement == cq.REQ_MUST_NOT
        assert strip.add_condition(cq.REQ_MUST) is True
        assert strip.conditions()[1].requirement == cq.REQ_MUST
        strip.set_conditions([Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (LONG,))] * 4)
        assert strip.add_condition(cq.REQ_MUST) is False
        assert len(strip.conditions()) == 4
    finally:
        strip.deleteLater()


def test_condition_summary_names_requirement_place_and_text():
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('one two three four five six seven',))
    line = condition_summary(c)
    assert tr("Must not appear") in line and tr("Anywhere in the manuscript") in line
    assert 'one two three four five six ...' in line
    assert tr("({} versions)").format(2) in condition_summary(
        Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, ('a', 'b'), 'HG'))


# -- the two result groups on a real tree --------------------------------------------

import genizah_app  # noqa: E402

APP = genizah_app.GenizahGUI


class _Win:
    _add_compound_groups = APP._add_compound_groups
    _compound_item_reason = APP._compound_item_reason
    _condition_label = APP._condition_label

    def __init__(self, conditions):
        self.comp_tree = QTreeWidget()
        self._comp_view_groups = []
        self._comp_compound = {'conditions': [c.to_dict() for c in conditions]}

    def _sort_comp_items(self, items):
        return list(items)


def _ms(sys_id, states, reasons=None):
    n = len(states)
    oc = cq.PageOutcome(cq._category(states), tuple(states), ((),) * n,
                        tuple(reasons or [''] * n))
    return {'sys_id': sys_id, 'pages': [{'raw_header': f'{sys_id}_IE1_P000001_FL1',
                                         'compound': oc.to_dict()}]}


def test_removed_and_unresolved_groups_are_collapsed_split_and_registered():
    w = _Win([Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('h',), 'HG'),
              Condition(cq.REQ_MUST_NOT, cq.WHERE_MANUSCRIPT, ('r',), 'Rif')])
    removed = [_ms('9901', [cq.REMOVED, cq.SATISFIED]),
               _ms('9902', [cq.SATISFIED, cq.REMOVED]),
               _ms('9903', [cq.REMOVED, cq.SATISFIED])]
    unresolved = [_ms('9904', [cq.UNRESOLVED, cq.SATISFIED], [cq.CUT_STOPPED, ''])]
    added = []

    def add_node(parent, item):
        added.append((parent.text(0), item['sys_id']))

    try:
        w._add_compound_groups(removed, unresolved, add_node, lambda node: None)
        roots = [w.comp_tree.topLevelItem(i) for i in range(w.comp_tree.topLevelItemCount())]
        assert [r.text(0) for r in roots] == [
            tr("Removed by a condition ({})").format(3),
            tr("Not fully checked ({})").format(1)]
        assert all(not r.isExpanded() for r in roots)
        subs = [roots[0].child(i).text(0) for i in range(roots[0].childCount())]
        assert subs == ['HG (2)', 'Rif (1)']
        cats = [(g.category, g.subgroup, len(g.items)) for g in w._comp_view_groups]
        assert cats == [(_cvf.CATEGORY_REMOVED, 'HG', 2), (_cvf.CATEGORY_REMOVED, 'Rif', 1),
                        (_cvf.CATEGORY_UNRESOLVED,
                         f"HG ({tr('stopped')})", 1)]
        assert sorted(s for _p, s in added) == ['9901', '9902', '9903', '9904']
    finally:
        w.comp_tree.deleteLater()


def test_dialog_refuses_a_version_over_the_letter_limit():
    dlg = ConditionDialog(None, floor=40, requirement=cq.REQ_MUST)
    try:
        dlg.edit_text.setPlainText('אב' * (cq.MAX_VERSION_LETTERS // 2 + 1))
        assert not _ok(dlg).isEnabled()
        assert tr("Version {} is too long ({} letters; at most {}).").format(
            1, cq.MAX_VERSION_LETTERS + 2, cq.MAX_VERSION_LETTERS) in dlg.lbl_info.text()
    finally:
        dlg.deleteLater()


# -- the "Filter texts" menu (owner, 2026-10-14) ------------------------------
# One button on the Composition tab, one menu, three fixed-requirement entry
# points, replacing the old "Filter Text" button and the strip's own
# "+ Add condition". genizah_app.py's own methods are borrowed onto a real
# QWidget (QMenu needs a QWidget parent), same pattern as `_Win` above.

class _FilterMenuWin(QWidget):
    _build_comp_filter_menu = APP._build_comp_filter_menu
    _comp_add_condition = APP._comp_add_condition
    _refresh_comp_filter_menu = APP._refresh_comp_filter_menu
    _refresh_comp_conditions_available = APP._refresh_comp_conditions_available

    def __init__(self, method='passage'):
        super().__init__()
        self.comp_conditions_strip = ConditionsStrip(
            self, floor=lambda: 40, main_searches=lambda: 1)
        self.filter_sources = {}
        self.filter_enabled_sources = set()
        self.opened = []
        self._method = method

    def open_filter_dialog(self, col_idx=None):
        self.opened.append(col_idx)

    def _comp_method(self):
        return self._method


def _menu_win(method='passage'):
    win = _FilterMenuWin(method)
    win.comp_filter_menu = win._build_comp_filter_menu()
    return win


def test_filter_texts_menu_has_the_three_items_in_order_with_tooltips():
    win = _menu_win()
    try:
        actions = win.comp_filter_menu.actions()
        assert [a.text() for a in actions] == [
            tr("Quoted texts..."), tr("Quoting texts..."), tr("Add condition...")]
        assert actions[0].toolTip() == tr(
            "For example, Bible verses quoted in the Talmud")
        # Without this Qt never shows an action's tooltip inside a menu.
        assert win.comp_filter_menu.toolTipsVisible()
        # The tooltips of items 2 and 3 are set when the menu opens.
        win.comp_filter_menu.aboutToShow.emit()
        assert actions[1].toolTip() == tr("For example, the Rif quoting the Talmud")
        assert actions[2].toolTip() == tr(
            "For example, show only results where an adjacent image "
            "(before or after) includes a certain text")
    finally:
        win.deleteLater()


def test_item_one_opens_the_quoted_texts_dialog_via_open_filter_dialog():
    win = _menu_win()
    try:
        win.act_comp_quoted_texts.trigger()
        assert win.opened == [False]            # QAction.triggered sends `checked`
    finally:
        win.deleteLater()


class _RealFilterWin(_FilterMenuWin):
    """The window's REAL open_filter_dialog, so item 1 is followed into the
    Quoted texts dialog itself (not a stub)."""
    open_filter_dialog = APP.open_filter_dialog


def test_item_one_reaches_the_real_quoted_texts_dialog_and_keeps_its_sources(monkeypatch):
    import genizah_app
    shown = []

    def accept(dlg):
        shown.append(dlg.windowTitle())
        return QDialog.DialogCode.Accepted
    monkeypatch.setattr(genizah_app.FilterTextDialog, 'exec', accept)
    win = _RealFilterWin()
    win.comp_filter_menu = win._build_comp_filter_menu()
    win.filter_sources = {'Genesis 1': 'בראשית ברא'}
    win.filter_enabled_sources = {'Genesis 1'}
    try:
        win.act_comp_quoted_texts.trigger()
        assert shown == [tr("Quoted texts")]
        assert win.filter_sources == {'Genesis 1': 'בראשית ברא'}
        assert win.filter_enabled_sources == {'Genesis 1'}
    finally:
        win.deleteLater()


def test_item_two_yields_a_must_not_condition_and_item_three_a_must_condition(monkeypatch):
    monkeypatch.setattr(ConditionDialog, 'exec', lambda self: QDialog.DialogCode.Accepted)
    win = _menu_win()
    try:
        win.act_comp_quoting_texts.trigger()
        win.act_comp_add_condition.trigger()
        reqs = [c.requirement for c in win.comp_conditions_strip.conditions()]
        assert reqs == [cq.REQ_MUST_NOT, cq.REQ_MUST]
    finally:
        win.deleteLater()


def test_items_two_and_three_disabled_outside_letter_level_with_the_reason():
    win = _menu_win(method='chunk')
    try:
        win.comp_filter_menu.aboutToShow.emit()
        assert not win.act_comp_quoting_texts.isEnabled()
        assert not win.act_comp_add_condition.isEnabled()
        reason = tr("Conditions work only with letter-level search.")
        assert win.act_comp_quoting_texts.toolTip() == reason
        assert win.act_comp_add_condition.toolTip() == reason
    finally:
        win.deleteLater()


def test_add_condition_refuses_outside_letter_level_even_without_the_menu(monkeypatch):
    monkeypatch.setattr(ConditionDialog, 'exec', lambda self: QDialog.DialogCode.Accepted)
    win = _menu_win(method='chunk')
    try:
        win._refresh_comp_conditions_available()
        assert win.comp_conditions_strip.add_condition(cq.REQ_MUST) is False
        assert win.comp_conditions_strip.conditions() == []
    finally:
        win.deleteLater()


def test_items_two_and_three_disabled_at_four_conditions_with_the_reason():
    win = _menu_win(method='passage')
    try:
        c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (LONG,))
        win.comp_conditions_strip.set_conditions([c] * cq.MAX_CONDITIONS)
        win.comp_filter_menu.aboutToShow.emit()
        assert not win.act_comp_quoting_texts.isEnabled()
        assert not win.act_comp_add_condition.isEnabled()
        reason = tr("At most {} conditions.").format(cq.MAX_CONDITIONS)
        assert win.act_comp_quoting_texts.toolTip() == reason
        assert win.act_comp_add_condition.toolTip() == reason
    finally:
        win.deleteLater()


def test_item_one_shows_the_active_quoted_source_count():
    win = _menu_win()
    try:
        win.comp_filter_menu.aboutToShow.emit()
        assert win.act_comp_quoted_texts.text() == tr("Quoted texts...")
        win.filter_sources = {'a': 'x', 'b': 'y', 'c': 'z'}
        win.filter_enabled_sources = {'a', 'c'}       # 'b' loaded but not enabled
        win.comp_filter_menu.aboutToShow.emit()
        assert win.act_comp_quoted_texts.text() == tr("Quoted texts... ({})").format(2)
    finally:
        win.deleteLater()

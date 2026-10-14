# -*- coding: utf-8 -*-
"""Composition Search conditions: the strip, the dialog and the two result
groups, on real widgets (gate G7 of docs/plans/COMPOSITION_CONDITIONS_PLAN.md).

GUI lane: run with `python scripts/run_gui_tests.py` (one file per process).
Labels are compared with tr(), never with literal text, so the test holds in
either UI language.
"""
from __future__ import annotations

import pytest
from PyQt6.QtWidgets import QApplication, QDialogButtonBox, QTreeWidget

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
    c = Condition(cq.REQ_MUST_NOT, cq.WHERE_ADJACENT, (LONG, LONG + LONG), 'HG')
    dlg = ConditionDialog(None, c, floor=40)
    try:
        assert dlg.condition() == c
        assert _ok(dlg).isEnabled()
        assert dlg.lbl_help.isVisibleTo(dlg)          # shown for "must not"
        dlg.combo_requirement.setCurrentIndex(
            dlg.combo_requirement.findData(cq.REQ_MUST))
        assert not dlg.lbl_help.isVisibleTo(dlg)
    finally:
        dlg.deleteLater()


def test_dialog_refuses_an_under_floor_version_and_says_which():
    dlg = ConditionDialog(None, floor=40)
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
    c = Condition(cq.REQ_MUST, cq.WHERE_SAME_IMAGE, (LONG,))
    strip = _strip([c] * 4)
    try:
        assert not strip.btn_add.isEnabled()
        strip.set_conditions([c])
        assert strip.btn_add.isEnabled()
        strip.set_available(False, tr("Conditions work only with letter-level search."))
        assert not strip.btn_add.isEnabled()
        assert strip.lbl_disabled.isVisibleTo(strip)
        assert strip.conditions() == [c]              # kept, just frozen
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
    dlg = ConditionDialog(None, floor=40)
    try:
        dlg.edit_text.setPlainText('אב' * (cq.MAX_VERSION_LETTERS // 2 + 1))
        assert not _ok(dlg).isEnabled()
        assert tr("Version {} is too long ({} letters; at most {}).").format(
            1, cq.MAX_VERSION_LETTERS + 2, cq.MAX_VERSION_LETTERS) in dlg.lbl_info.text()
    finally:
        dlg.deleteLater()

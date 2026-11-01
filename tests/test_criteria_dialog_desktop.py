# -*- coding: utf-8 -*-
"""Desktop Manuscript features window (desktop/dialogs_filter.py::CriteriaDialog),
PR 1c of docs/plans/CATALOGUE_CRITERIA_PLAN.md (§6.3).

Sections in a list with their controls beside them; lists by count, then
Unknown, Cannot be determined, Not recorded; the only / all-except toggle and
the source choice; a filter box on long lists; the live total, with "of
which N not recorded" for "all except" (rule 2); criteria a 5.0.0 file cannot
answer disabled and dropped with a notice (rule 9); the post-search form
(D20). The values and count workers run synchronously here.
"""
import os
import sys

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtCore import Qt  # noqa: E402
from PyQt6.QtWidgets import QApplication, QLabel  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, B, C, UNIVERSE, build_criteria_sidecar  # noqa: E402

import genizah_core  # noqa: E402
import desktop.dialogs_filter as dialogs_filter  # noqa: E402
import desktop.gui_threads as gui_threads  # noqa: E402
from shared import fjms_service  # noqa: E402
from shared.catalog_criteria import clean_criteria  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

_APP = QApplication.instance() or QApplication([])


class _SyncValues(gui_threads.CriterionValuesWorker):
    def start(self):
        self.run()


class _SyncCount(gui_threads.CriteriaCountWorker):
    def start(self):
        self.run()


def _service(monkeypatch, tmp_path, *, upgrade=True, lang='en'):
    svc = FjmsService(db_path=build_criteria_sidecar(tmp_path / "c.db", upgrade=upgrade))
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    monkeypatch.setattr(gui_threads, 'CriterionValuesWorker', _SyncValues)
    monkeypatch.setattr(gui_threads, 'CriteriaCountWorker', _SyncCount)
    for module in (genizah_core, dialogs_filter):
        monkeypatch.setattr(module, 'CURRENT_LANG', lang)
    return svc


@pytest.fixture
def svc(monkeypatch, tmp_path):
    service = _service(monkeypatch, tmp_path)
    yield service
    service.close()


def _open(saved=None, **kwargs):
    kwargs.setdefault('universe', UNIVERSE)
    return dialogs_filter.CriteriaDialog(None, saved, **kwargs)


def _visit_all(dlg):
    for row in range(dlg.section_list.count()):
        dlg.section_list.setCurrentRow(row)


def _texts(dlg, key):
    widget = dlg._controls[key]['values']
    return [widget.item(i).text() for i in range(widget.count())]


def _tick(dlg, key, value, checked=True):
    widget = dlg._controls[key]['values']
    for i in range(widget.count()):
        if widget.item(i).data(Qt.ItemDataRole.UserRole) == value:
            widget.item(i).setCheckState(Qt.CheckState.Checked if checked else Qt.CheckState.Unchecked)
            return
    raise AssertionError(f"{value!r} is not offered for {key}")


SAVED = {
    'period': {'min': 1000, 'max': 1100, 'not_recorded': True},
    'region': {'values': ['Oriental', '_none'], 'mode': 'except'},
    'material': {'values': ['Paper'], 'source': 'catalogue'},
    'lines': {'min': 20, 'source': 'measured'},
    'margin_left': {'max': 2.5},
}


def test_every_kind_of_criterion_round_trips(svc):
    """Before its values load and after: OK must never lose or change a criterion."""
    dlg = _open(SAVED)
    try:
        expected = clean_criteria(SAVED)[0]
        assert dlg.get_criteria() == expected, "a saved criterion changed before its values loaded"
        _visit_all(dlg)
        assert dlg._controls['region']['loaded'] == 'either'
        assert dlg.get_criteria() == expected, "a saved criterion changed once its values loaded"
    finally:
        dlg.deleteLater()


def test_values_by_count_then_unknown_then_not_recorded(svc):
    dlg = _open()
    try:
        _visit_all(dlg)
        assert _texts(dlg, 'region') == [
            'Oriental (1)', 'Spanish (1)', 'Yemenite (1)', 'Unknown (1)', 'Not recorded (3)']
        assert _texts(dlg, 'attributes') == ['Colophon (1)', 'Illustration (1)', 'Massorah (1)'], (
            "Attributes list only what is present: no Unknown, no Not recorded (D5 follow-up)")
    finally:
        dlg.deleteLater()


def test_ticks_mode_and_source_make_the_criteria(svc):
    dlg = _open()
    try:
        _visit_all(dlg)
        _tick(dlg, 'region', 'Spanish')
        dlg._controls['region']['mode'].setCurrentIndex(1)
        dlg._controls['lines']['min'].setText('20')
        dlg._controls['lines']['max'].setText('25')
        dlg._controls['lines']['source'].setCurrentIndex(
            dlg._controls['lines']['source'].findData('catalogue'))
        assert dlg.get_criteria() == {
            'region': {'values': ['Spanish'], 'mode': 'except'},
            'lines': {'source': 'catalogue', 'min': 20, 'max': 25}}
    finally:
        dlg.deleteLater()


def test_the_live_total_is_the_service_answer_and_names_the_unrecorded(svc):
    """Rule 2: "all except Oriental" keeps the manuscripts with no region, and says how many."""
    criteria = {'region': {'values': ['Oriental'], 'mode': 'except'}}
    dlg = _open(criteria)
    try:
        expected = svc.get_filter_sys_ids(criteria=criteria, universe=UNIVERSE)
        assert len(expected) == 5
        assert dlg.total_label.text() == (
            "5 manuscripts have these features, of which 3 have no Region recorded")
        assert dlg.note_label.text() == (
            "These criteria cover all 6 manuscripts. 2 have no FJMS catalogue record: they match "
            "'Not recorded', 'all except' and measured data (0 also have a catalogue size).")
    finally:
        dlg.deleteLater()


def test_all_except_an_attribute_is_counted(svc):
    """GitHub Codex on #458: Attributes have no "not recorded", so "all
    except" has no "of which" -- and the count must not fail for it."""
    dlg = _open({'attributes': {'values': ['colophon'], 'mode': 'except'}})
    try:
        assert dlg.total_label.text() == "5 manuscripts have these features"
    finally:
        dlg.deleteLater()


def test_the_values_worker_reads_the_chip_labels_off_the_ui_thread(svc, monkeypatch):
    """GitHub Codex on #458: the chips' Hebrew labels (an unscoped read) were
    first read on the UI thread at Apply; the window's worker reads them."""
    read = []
    real = svc.criterion_value_labels
    monkeypatch.setattr(svc, 'criterion_value_labels', lambda key: read.append(key) or real(key))
    dlg = _open()
    try:
        _visit_all(dlg)
        assert {'region', 'style', 'material', 'attributes'} <= set(read)
        assert 'lines' not in read, "ranges have no value labels"
    finally:
        dlg.deleteLater()


def test_the_start_up_warm_up_reads_every_list_label(svc, monkeypatch):
    import genizah_app
    from shared.catalog_criteria import CRITERIA
    read = []
    monkeypatch.setattr(svc, 'criterion_value_labels', read.append)
    genizah_app._warm_criteria_labels(svc)
    assert read == [c.key for c in CRITERIA if c.kind == 'list']


def test_a_count_that_cannot_run_is_an_error_not_a_number(svc):
    """No corpus list: "all except" cannot be counted (corpus_unavailable)."""
    dlg = _open({'region': {'values': ['Oriental'], 'mode': 'except'}}, universe=None)
    try:
        assert dlg.total_label.text() == "The count could not be computed."
    finally:
        dlg.deleteLater()


def test_a_saved_value_the_file_does_not_offer_stays_ticked(svc):
    """Dropping it silently would widen the search. It stays among the values,
    before Unknown and Not recorded."""
    dlg = _open({'region': {'values': ['Byzantine']}})
    try:
        _visit_all(dlg)
        assert _texts(dlg, 'region') == [
            'Oriental (1)', 'Spanish (1)', 'Yemenite (1)', 'Byzantine', 'Unknown (1)', 'Not recorded (3)']
        assert dlg.get_criteria() == {'region': {'values': ['Byzantine'], 'mode': 'only'}}
    finally:
        dlg.deleteLater()


def test_a_5_0_0_file_disables_and_drops_what_needs_the_new_tables(monkeypatch, tmp_path):
    """Rule 9: Lines (in every source mode), Period, Columns, vocalization
    extent and Attributes need criteria_numbers / criteria_values."""
    service = _service(monkeypatch, tmp_path, upgrade=False)
    saved = {'lines': {'min': 20, 'source': 'measured'}, 'attributes': {'values': ['colophon']},
             'region': {'values': ['Spanish']}}
    dlg = _open(saved)
    try:
        assert dlg.dropped_saved_criteria == ['attributes', 'lines']
        assert not dlg.dropped_notice_label.isHidden()
        assert dlg.get_criteria() == {'region': {'values': ['Spanish'], 'mode': 'only'}}
        disabled = {key for key in dlg._controls
                    if not dlg._controls[key]['coverage'].parentWidget().isEnabled()}
        assert disabled == {'period', 'vocal_extent', 'columns', 'lines', 'attributes'}
    finally:
        dlg.deleteLater()
        service.close()


def test_a_bound_is_a_number_or_nothing(svc):
    """Codex, PR 1c round 1: a lone "." was accepted and then dropped the bound."""
    from PyQt6.QtGui import QValidator
    dlg = _open()
    try:
        width, period = dlg._controls['width']['min'].validator(), dlg._controls['period']['min'].validator()
        for text, state in (('.', QValidator.State.Intermediate), ('.5', QValidator.State.Acceptable),
                            ('12.', QValidator.State.Intermediate), ('12.5', QValidator.State.Acceptable),
                            ('', QValidator.State.Acceptable)):
            assert width.validate(text, len(text))[0] == state, text
        assert period.validate('1.', 2)[0] == QValidator.State.Invalid, "a year is whole"
        edit = dlg._controls['width']['min']
        edit.setText('.')
        assert not dlg.ok_btn.isEnabled(), "Apply would drop a bound being typed"
        edit.setText('.5')
        assert dlg.ok_btn.isEnabled() and dlg.get_criteria()['width']['min'] == 0.5
    finally:
        dlg.deleteLater()


def test_support_not_known_drops_nothing(svc, monkeypatch):
    """K-16: a file whose schema could not be read is not known to lack anything."""
    monkeypatch.setattr(svc, 'criteria_support', lambda: fjms_service.FILTER_SUPPORT_UNKNOWN)
    assert dialogs_filter.criteria_unavailable_keys(svc) == frozenset()
    monkeypatch.setattr(svc, 'criteria_support', lambda: fjms_service.FILTER_UNSUPPORTED)
    assert 'lines' in dialogs_filter.criteria_unavailable_keys(svc)


def test_clear_section_and_clear_all(svc):
    dlg = _open(SAVED)
    try:
        _visit_all(dlg)
        dlg.section_list.setCurrentRow(
            [s.key for s in dlg._sections].index('script'))
        dlg._clear_section()
        assert 'region' not in dlg.get_criteria() and 'period' in dlg.get_criteria()
        dlg._clear_all()
        assert dlg.get_criteria() == {}
        dlg._update_count()
        assert dlg.total_label.text() == "No features chosen."
    finally:
        dlg.deleteLater()


def test_a_kept_special_value_keeps_the_special_order(svc):
    """Codex, PR 1c round 2: a saved "Cannot be determined" the file does not
    offer goes between Unknown and Not recorded, not before Unknown."""
    dlg = _open({'region': {'values': ['Cannot be determined', 'Byzantine']}})
    try:
        _visit_all(dlg)
        assert _texts(dlg, 'region') == [
            'Oriental (1)', 'Spanish (1)', 'Yemenite (1)', 'Byzantine', 'Unknown (1)',
            'Cannot be determined', 'Not recorded (3)']
    finally:
        dlg.deleteLater()


def test_the_corpus_note_comes_back_after_clear_all(svc):
    """Codex, PR 1c round 2: read once, it showed only until the first Clear All."""
    dlg = _open({'region': {'values': ['Oriental']}})
    try:
        assert not dlg.note_label.isHidden()
        dlg._clear_all()
        dlg._update_count()
        assert dlg.note_label.isHidden()
        _visit_all(dlg)
        _tick(dlg, 'region', 'Spanish')
        dlg._update_count()
        assert not dlg.note_label.isHidden() and dlg.note_label.text().startswith("These criteria cover")
    finally:
        dlg.deleteLater()


def test_a_long_list_gets_a_filter_box_that_keeps_ticked_values(svc, monkeypatch):
    monkeypatch.setattr(dialogs_filter, '_LONG_LIST', 3)
    dlg = _open({'region': {'values': ['Yemenite']}})
    try:
        _visit_all(dlg)
        controls = dlg._controls['region']
        assert not controls['search'].isHidden()
        assert dlg._controls['style']['search'].isHidden(), "a short list has no filter box"
        controls['search'].setText('span')
        shown = [controls['values'].item(i).text() for i in range(controls['values'].count())
                 if not controls['values'].isRowHidden(i)]
        assert shown == ['Spanish (1)', 'Yemenite (1)'], "a ticked value must stay in sight"
    finally:
        dlg.deleteLater()


def test_hebrew_labels_english_values(monkeypatch, tmp_path):
    """Labels from the catalogue's own Hebrew, or the app's translation for a
    measured value the catalogue does not name; values stay English."""
    service = _service(monkeypatch, tmp_path, lang='he')
    dlg = _open({'material': {'values': ['Vellum'], 'source': 'measured'}})
    try:
        _visit_all(dlg)
        texts = _texts(dlg, 'material')
        assert texts == ['נייר (1)', 'קלף', 'ללא רישום (5)'], (
            "the catalogue's Hebrew; the saved value this source does not offer, before Not recorded")
        assert dlg.get_criteria() == {'material': {'source': 'measured', 'values': ['Vellum'],
                                                   'mode': 'only'}}
        assert dialogs_filter._label(dlg._sections[0]) == 'תקופת העתקה'
    finally:
        dlg.deleteLater()
        service.close()


def test_changing_the_source_reloads_the_values_and_keeps_the_ticks(svc):
    dlg = _open({'material': {'values': ['Vellum'], 'source': 'catalogue'}})
    try:
        _visit_all(dlg)
        assert _texts(dlg, 'material')[:2] == ['Vellum (2)', 'Paper (1)']
        combo = dlg._controls['material']['source']
        combo.setCurrentIndex(combo.findData('measured'))
        assert _texts(dlg, 'material') == ['Paper (1)', 'Vellum', 'Not recorded (5)'], (
            "measured values, the kept tick, then Not recorded")
        assert dlg.get_criteria()['material'] == {'source': 'measured', 'values': ['Vellum'],
                                                  'mode': 'only'}
    finally:
        dlg.deleteLater()


def test_switching_back_to_a_loaded_source_shows_its_values_again(svc, monkeypatch):
    """Each (criterion, source) is read once; switching back must not wait for
    a worker that will never come (the list stayed on "Loading...")."""
    asked = []
    real = _SyncValues.run

    def counting_run(worker):
        asked.extend(worker._needs)
        real(worker)
    monkeypatch.setattr(_SyncValues, 'run', counting_run)
    dlg = _open({'material': {'values': ['Vellum']}})
    try:
        _visit_all(dlg)
        combo = dlg._controls['material']['source']
        for source in ('measured', 'either', 'measured', 'either'):
            combo.setCurrentIndex(combo.findData(source))
            assert dlg._controls['material']['loaded'] == source, f"{source}: still loading"
        assert _texts(dlg, 'material')[:2] == ['Paper (2)', 'Vellum (2)'], "either: catalogue or measured"
        assert asked.count(('material', 'measured')) == 1, "a source was read twice"
        assert dlg.get_criteria()['material']['values'] == ['Vellum']
    finally:
        dlg.deleteLater()


def test_coverage_per_criterion(svc):
    dlg = _open()
    try:
        _visit_all(dlg)
        assert dlg._controls['region']['coverage'].text() == "recorded for 3 of 6"
        assert dlg._controls['lines']['coverage'].text() == "recorded for 4 of 6"
    finally:
        dlg.deleteLater()


def test_the_post_search_form_counts_within_the_results(svc):
    """D20: three sections; "not recorded" and the total within the results;
    a criterion of another section passes through unchanged."""
    results = frozenset({A, B, C})
    saved = {'width': {'min': 11}, 'region': {'values': ['Spanish']}}
    dlg = _open(saved, sections=('lines', 'material', 'measured'), within=results)
    try:
        assert [s.key for s in dlg._sections] == ['lines', 'material', 'measured']
        criteria = dlg.get_criteria()
        assert criteria == clean_criteria(saved)[0]
        kept = svc.match_result_criteria(criteria, results)
        assert dlg.total_label.text() == f"{len(kept)} of the 3 manuscripts in the results match"
        assert dlg.note_label.isHidden()
    finally:
        dlg.deleteLater()


def test_the_dialog_has_no_stray_labels_in_english_when_hebrew(monkeypatch, tmp_path):
    """Every fixed string of the window goes through tr() (Hebrew UI)."""
    service = _service(monkeypatch, tmp_path, lang='he')
    dlg = _open()
    try:
        english = [lbl.text() for lbl in dlg.findChildren(QLabel)
                   if lbl.text() and lbl.text().isascii() and any(c.isalpha() for c in lbl.text())]
        assert english == [], english
    finally:
        dlg.deleteLater()
        service.close()

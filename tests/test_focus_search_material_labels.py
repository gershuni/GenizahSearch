# -*- coding: utf-8 -*-
"""Focus Search "Material (measured)" checkboxes: Hebrew labels, English values.

A user letter (Tishrei 5787) reported the five material options showing in English in
the Hebrew UI: the checkboxes were built with QCheckBox(mat) and never went through tr().
The English name must stay the dict key and the saved `measurement_material` value --
the filter is matched against English data -- so only the DISPLAY text is translated.
"""
import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtWidgets import QApplication  # noqa: E402

import genizah_core  # noqa: E402
import desktop.dialogs_filter as dialogs_filter  # noqa: E402

_APP = QApplication.instance() or QApplication([])

_HE = {'Paper': 'נייר', 'Vellum': 'קלף', 'Papyrus': 'פפירוס', 'Mix': 'מעורב', 'Wood': 'עץ'}


def _dialog(monkeypatch, current_filters):
    # The live filter count starts a worker thread against the FJMS sidecar; it is not
    # what this test is about.
    monkeypatch.setattr(dialogs_filter.PreSearchFilterDialog, '_update_count',
                        lambda self: None)
    return dialogs_filter.PreSearchFilterDialog(None, current_filters)


def test_material_labels_are_hebrew_but_values_stay_english(monkeypatch):
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'he')
    dlg = _dialog(monkeypatch, {'measurement_material': ['Vellum', 'Papyrus']})
    try:
        checks = dlg._meas_material_checks
        assert set(checks) == set(_HE), "dict keys must remain the English values"
        for eng, cb in checks.items():
            assert cb.text() == _HE[eng], f"{eng!r} checkbox shows {cb.text()!r}"
        # Restoring from saved (English) values still ticks the right boxes...
        assert {m for m, cb in checks.items() if cb.isChecked()} == {'Vellum', 'Papyrus'}
        # ...and the round trip saves English, not the Hebrew display text.
        assert dlg.get_filters()['measurement_material'] == ['Vellum', 'Papyrus']
    finally:
        dlg.deleteLater()


def test_material_labels_are_english_in_english_ui(monkeypatch):
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    dlg = _dialog(monkeypatch, {})
    try:
        assert {eng: cb.text() for eng, cb in dlg._meas_material_checks.items()} == {
            eng: eng for eng in _HE}
    finally:
        dlg.deleteLater()

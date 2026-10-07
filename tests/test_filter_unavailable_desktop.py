# -*- coding: utf-8 -*-
"""Desktop: a filter lookup that cannot run is shown as an error (#17).

FilterCountWorker must emit ``failed(reason)`` -- never ``finished(set())``
("No manuscripts match") or ``finished(None)`` ("All manuscripts (no
filters)", and an unrestricted search). The pre-search filter dialog must
show that error with OK disabled, must hide the Line Height controls (or the
whole Measurements group) when the catalog data lacks them, must keep a saved
Line Height value when the data has it, and -- when the catalog's support is
NOT KNOWN -- must keep saved values whatever the controls show and keep OK
disabled until the lookup runs or the user clears them (K-16, K-17). The
worker builds its own FjmsService; GENIZAH_FJMS_DB_PATH points it at a small
real sidecar.
"""
import os
import sys

import pytest

pytestmark = pytest.mark.gui

from PyQt6.QtWidgets import QApplication, QLabel, QPushButton  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fjms_filter_sidecar import build_sidecar, unknown_schema_service  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

_app = QApplication.instance() or QApplication([])


def _run_worker(filters, meta_mgr=None):
    from desktop.gui_threads import FilterCountWorker
    worker = FilterCountWorker(filters, meta_mgr=meta_mgr)
    finished, failed = [], []
    worker.finished.connect(finished.append)
    if hasattr(worker, 'failed'):
        worker.failed.connect(failed.append)
    worker.run()  # synchronously, on this thread
    return finished, failed


@pytest.fixture
def no_lh_sidecar(tmp_path, monkeypatch):
    path = build_sidecar(tmp_path / "no_lh.db", with_line_height=False)
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", path)
    return path


@pytest.fixture
def absent_sidecar(tmp_path, monkeypatch):
    path = str(tmp_path / "absent.db")
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", path)
    yield path
    assert not os.path.exists(path), "the worker must not create a stub sidecar"


# -- FilterCountWorker --

def test_worker_reports_missing_line_height_column_as_failure(no_lh_sidecar):
    finished, failed = _run_worker({'include_mode': True, 'line_height_min': 3.0})
    assert finished == [], f"worker reported {finished!r} as a count; expected a failure"
    assert failed == ['column_missing']


def test_worker_reports_missing_sidecar_as_failure(absent_sidecar):
    finished, failed = _run_worker({'include_mode': True, 'date_from': 1000})
    assert finished == [], (
        f"worker reported {finished!r}; None is shown as 'All manuscripts (no filters)' "
        "and runs the search unrestricted")
    assert failed == ['sidecar_unavailable']


def test_library_only_filter_without_sidecar_keeps_the_library_restriction(absent_sidecar, monkeypatch):
    """A Library filter never needs the sidecar. The old worker returned None
    before it reached the library step, so the search ran over every library
    while the Library chip was shown."""
    monkeypatch.setattr(fjms_service, 'resolve_library_sys_ids',
                        lambda codes, meta_mgr: {'990001', '990002'})
    finished, failed = _run_worker({'include_mode': True, 'library': ['CUL']},
                                   meta_mgr=object())
    assert failed == []
    assert finished == [{'990001', '990002'}], finished


def test_worker_still_counts_when_the_lookup_works(no_lh_sidecar):
    finished, failed = _run_worker({'include_mode': True, 'width_min': 10})
    assert failed == []
    assert finished == [{'990001', '990002'}]


# -- PreSearchFilterDialog --

def _sync_worker_class():
    import desktop.dialogs_filter as dlg_mod
    from desktop.gui_threads import FilterCountWorker

    class _SyncWorker(FilterCountWorker):
        def __init__(self, filters, parent=None, **kw):
            super().__init__(filters, None, **kw)

        def start(self):
            self.run()

    return dlg_mod, _SyncWorker


def test_filter_dialog_shows_error_not_no_match(no_lh_sidecar, monkeypatch):
    """Drive PreSearchFilterDialog._update_count, the dialog's real call site."""
    dlg_mod, sync_worker = _sync_worker_class()
    monkeypatch.setattr(dlg_mod, 'FilterCountWorker', sync_worker)

    class _Bare(dlg_mod.PreSearchFilterDialog):
        def __init__(self):  # skip the real widget tree
            pass

    d = _Bare()
    d._count_generation = 0
    d.count_label = QLabel()
    d.ok_btn = QPushButton()
    d._get_current_filter_dict = lambda: {'include_mode': True, 'line_height_min': 3.0}
    d._update_count()
    text = d.count_label.text()
    assert text != dlg_mod.tr("No manuscripts match"), (
        "the dialog told the user no manuscripts match a filter it could not apply")
    assert text != dlg_mod.tr("All manuscripts (no filters)")
    assert text == dlg_mod.tr("Could not update filter count"), text
    assert d.ok_btn.isEnabled() is False, "OK would store an unknown scope"


class _NoWorker:
    """Stands in for FilterCountWorker so the real dialog starts no thread."""

    def __init__(self, *a, **k):
        class _Sig:
            def connect(self, *a, **k):
                pass
        self.finished = _Sig()
        self.failed = _Sig()

    def start(self):
        pass


def _open_dialog(monkeypatch, saved, svc, worker=_NoWorker):
    import desktop.dialogs_filter as dlg_mod
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    monkeypatch.setattr(dlg_mod, 'FilterCountWorker', worker)
    return dlg_mod.PreSearchFilterDialog(None, current_filters=dict(saved))


SAVED = {'include_mode': True, 'line_height_min': 3.0, 'line_height_max': 6.0, 'width_min': 10.0}


def test_dialog_keeps_a_saved_line_height_when_the_data_has_it(monkeypatch, tmp_path):
    """Opening the dialog and pressing OK must not lose a Line Height filter the
    data can answer. The restore runs before the Measurements group is added to
    the dialog, so a restore guarded on widget visibility loses it every time."""
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=True))
    dlg = _open_dialog(monkeypatch, SAVED, svc)
    out = dlg.get_filters()
    assert out.get('line_height_min') == 3.0 and out.get('line_height_max') == 6.0, out
    assert not dlg.meas_line_height_min.isHidden()


def test_dialog_hides_line_height_when_the_data_lacks_it(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=False))
    dlg = _open_dialog(monkeypatch, SAVED, svc)
    assert dlg.meas_line_height_label.isHidden()
    assert dlg.meas_line_height_min.isHidden() and dlg.meas_line_height_max.isHidden()
    assert not dlg.meas_width_min.isHidden(), "only the Line Height row is hidden"
    out = dlg.get_filters()
    assert 'line_height_min' not in out and 'line_height_max' not in out, out
    assert out.get('width_min') == 10.0


def test_dialog_hides_measurements_when_the_table_is_missing(monkeypatch, tmp_path):
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_measurements=False))
    dlg = _open_dialog(monkeypatch, SAVED, svc)
    assert dlg.meas_group.isHidden()
    out = dlg.get_filters()
    assert not any(k in out for k in ('width_min', 'line_height_min', 'line_height_max')), out


def test_dialog_keeps_saved_values_when_support_is_unknown(monkeypatch, tmp_path):
    """K-16/K-17: the schema could not be read, so nothing is known to be
    missing. The controls are not offered, but the saved values stay in the
    filters -- independent of what is visible."""
    svc = unknown_schema_service(build_sidecar(tmp_path / "s.db", with_line_height=False))
    dlg = _open_dialog(monkeypatch, SAVED, svc)
    assert dlg.meas_group.isHidden()
    out = dlg.get_filters()
    assert (out.get('line_height_min'), out.get('line_height_max'), out.get('width_min')) \
        == (3.0, 6.0, 10.0), out


DROPPED_NOTICE = 'Some saved filters were removed: this catalog data is not available.'


def _visible_notices(dlg):
    return [lbl for lbl in dlg.findChildren(QLabel)
            if lbl.text() == DROPPED_NOTICE and not lbl.isHidden()]


def test_dialog_says_so_when_it_drops_a_saved_line_height(monkeypatch, tmp_path):
    """The reported case: a Line Height value restored while the catalog was
    ABSENT is kept (support unknown); a catalog KNOWN to lack the column then
    opens, and the Focus Search dialog drops the value. It must say so, with
    the notice the restores show -- OK used to lose it silently."""
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    absent = FjmsService(db_path=str(tmp_path / "absent.db"))
    restored, dropped = fjms_service.drop_unavailable_measurement_filters(SAVED, service=absent)
    assert (restored.get('line_height_min'), dropped) == (3.0, []), "the restore was not the reported one"
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=False))
    dlg = _open_dialog(monkeypatch, restored, svc)
    assert len(_visible_notices(dlg)) == 1, "the dialog dropped a saved Line Height silently"
    assert dlg.dropped_saved_filters == ['line_height_max', 'line_height_min']
    out = dlg.get_filters()
    assert 'line_height_min' not in out and out.get('width_min') == 10.0, out


def test_dialog_names_every_measurement_it_drops_when_the_table_is_missing(monkeypatch, tmp_path):
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_measurements=False))
    dlg = _open_dialog(monkeypatch, dict(SAVED, measurement_material=['Paper']), svc)
    assert len(_visible_notices(dlg)) == 1
    assert dlg.dropped_saved_filters == ['line_height_max', 'line_height_min',
                                         'measurement_material', 'width_min']


@pytest.mark.parametrize("case", ["data_has_it", "support_unknown", "nothing_saved_to_drop"])
def test_dialog_shows_no_notice_when_nothing_was_dropped(monkeypatch, tmp_path, case):
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    saved = SAVED
    if case == "data_has_it":
        svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=True))
    elif case == "support_unknown":
        svc = unknown_schema_service(build_sidecar(tmp_path / "s.db", with_line_height=False))
    else:
        svc = FjmsService(db_path=build_sidecar(tmp_path / "s.db", with_line_height=False))
        saved = {'include_mode': True, 'width_min': 10.0, 'line_height_min': 0}
    dlg = _open_dialog(monkeypatch, saved, svc)
    assert _visible_notices(dlg) == []
    assert dlg.dropped_saved_filters == []


def test_dialog_without_a_catalog_blocks_ok_until_the_filters_are_cleared(
        monkeypatch, tmp_path, absent_sidecar):
    """K-17: with no catalog, saved filters are kept, the count says it could
    not run, OK stays disabled; Clear All is the way out."""
    dlg_mod, sync_worker = _sync_worker_class()
    svc = FjmsService(db_path=absent_sidecar)
    dlg = _open_dialog(monkeypatch, SAVED, svc, worker=sync_worker)
    assert dlg.get_filters().get('line_height_min') == 3.0, "a saved value was dropped on a guess"
    assert dlg.count_label.text() == dlg_mod.tr("Could not update filter count")
    assert dlg.ok_btn.isEnabled() is False
    dlg._clear_all()
    assert dlg.ok_btn.isEnabled() is True
    assert not any(k != 'include_mode' for k in dlg.get_filters())

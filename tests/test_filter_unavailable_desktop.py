# -*- coding: utf-8 -*-
"""Desktop: a filter lookup that cannot run is shown as an error (#17).

FilterCountWorker must emit ``failed(reason)`` -- never ``finished(set())``
("No manuscripts match") or ``finished(None)`` ("All manuscripts (no
filters)", and an unrestricted search). The pre-search filter dialog must
show that error with OK disabled; must drop a saved manuscript feature
(catalogue criterion) only when the open file is KNOWN unable to answer it
(a 5.0.0 file and the criteria of the 5.1.0 tables, rule 9 of
docs/plans/CATALOGUE_CRITERIA_PLAN.md), and say so; and -- when the
catalog's support is NOT KNOWN -- must keep saved values and keep OK
disabled until the lookup runs or the user clears them (K-16, K-17). The
worker uses the service singleton or GENIZAH_FJMS_DB_PATH, pointed at a
small real sidecar.
"""
import os
import sqlite3
import sys

import pytest

pytestmark = pytest.mark.gui

from PyQt6.QtWidgets import QApplication, QLabel, QPushButton  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, build_criteria_sidecar  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.fjms_service import FjmsService  # noqa: E402

_app = QApplication.instance() or QApplication([])

# Clean criteria (shared.catalog_criteria.clean_criteria), as the dialog returns them.
LINES = {'lines': {'source': 'either', 'min': 20, 'max': 25}}  # needs the 5.1.0 tables
REGION = {'region': {'values': ['Oriental'], 'mode': 'only'}}  # any file with catalog_fields


def _run_worker(filters, meta_mgr=None):
    from desktop.gui_threads import FilterCountWorker
    worker = FilterCountWorker(filters, meta_mgr=meta_mgr)
    finished, failed = [], []
    worker.finished.connect(finished.append)
    worker.failed.connect(failed.append)
    worker.run()  # synchronously, on this thread
    return finished, failed


def _use(monkeypatch, path):
    """The file both the service singleton (the dialog) and a worker's own
    FjmsService (FilterCountWorker) open."""
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", path)
    svc = FjmsService(db_path=path)
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    return svc


@pytest.fixture
def old_sidecar(tmp_path, monkeypatch):
    """A 5.0.0 file: no criteria_numbers / criteria_values."""
    svc = _use(monkeypatch, build_criteria_sidecar(tmp_path / "old.db", upgrade=False))
    yield svc
    svc.close()


@pytest.fixture
def new_sidecar(tmp_path, monkeypatch):
    svc = _use(monkeypatch, build_criteria_sidecar(tmp_path / "new.db"))
    yield svc
    svc.close()


@pytest.fixture
def absent_sidecar(tmp_path, monkeypatch):
    path = str(tmp_path / "absent.db")
    monkeypatch.setattr(fjms_service, "_default_service", FjmsService(db_path=path))
    monkeypatch.setenv("GENIZAH_FJMS_DB_PATH", path)
    yield path
    assert not os.path.exists(path), "the worker must not create a stub sidecar"


# -- FilterCountWorker --

def test_worker_reports_a_criterion_the_file_lacks_as_failure(old_sidecar):
    finished, failed = _run_worker({'include_mode': True, 'criteria': LINES})
    assert finished == [], f"worker reported {finished!r} as a count; expected a failure"
    assert failed == ['column_missing']


def test_worker_reports_missing_sidecar_as_failure(absent_sidecar):
    finished, failed = _run_worker({'include_mode': True, 'criteria': REGION})
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


def test_worker_still_counts_when_the_lookup_works(old_sidecar):
    finished, failed = _run_worker({'include_mode': True, 'criteria': REGION})
    assert failed == []
    assert finished == [{A}]


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


def test_filter_dialog_shows_error_not_no_match(old_sidecar, monkeypatch):
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
    d._get_current_filter_dict = lambda: {'include_mode': True, 'criteria': LINES}
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


def _open_dialog(monkeypatch, saved, worker=_NoWorker):
    import desktop.dialogs_filter as dlg_mod
    monkeypatch.setattr(dlg_mod, 'FilterCountWorker', worker)
    return dlg_mod.PreSearchFilterDialog(None, current_filters=dict(saved))


SAVED = {'include_mode': True, 'criteria': dict(LINES, **REGION)}


def test_dialog_keeps_saved_criteria_when_the_data_has_them(monkeypatch, new_sidecar):
    """Opening the dialog and pressing OK must not lose a criterion the data can answer."""
    dlg = _open_dialog(monkeypatch, SAVED)
    assert dlg.get_filters()['criteria'] == SAVED['criteria']
    assert dlg.dropped_saved_filters == []


DROPPED_NOTICE = 'Some saved filters were removed: this catalog data is not available.'


def _visible_notices(dlg):
    return [lbl for lbl in dlg.findChildren(QLabel)
            if lbl.text() == DROPPED_NOTICE and not lbl.isHidden()]


def test_dialog_drops_and_names_a_criterion_a_5_0_0_file_cannot_answer(monkeypatch, old_sidecar):
    """Rule 9: Lines needs the 5.1.0 tables, in every source mode; the region
    (catalog_fields) stays. The dialog says so, with the restores' notice."""
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    dlg = _open_dialog(monkeypatch, SAVED)
    assert dlg.dropped_saved_filters == ['lines']
    assert len(_visible_notices(dlg)) == 1, "the dialog dropped a saved criterion silently"
    assert dlg.get_filters()['criteria'] == REGION


class _CriteriaSchemaUnreadable:
    """Wraps a real connection; the question "has this file the 5.1.0 tables?"
    fails (a locked or half-replaced file): the support is then NOT known."""

    def __init__(self, real):
        self._real = real

    def execute(self, sql, params=()):
        if "criteria_numbers" in sql and "sqlite_master" in sql:
            raise sqlite3.OperationalError("database is locked")
        return self._real.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._real, name)


@pytest.mark.parametrize("case", ["data_has_it", "support_unknown", "nothing_to_drop"])
def test_dialog_shows_no_notice_when_nothing_was_dropped(monkeypatch, tmp_path, case):
    """K-16: only a KNOWN absence drops a saved criterion."""
    import genizah_core
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')
    saved = SAVED
    svc = FjmsService(db_path=build_criteria_sidecar(tmp_path / "s.db", upgrade=case == "data_has_it"))
    if case == "support_unknown":
        svc._conn = _CriteriaSchemaUnreadable(svc._conn)
    elif case == "nothing_to_drop":
        saved = {'include_mode': True, 'criteria': REGION}
    monkeypatch.setattr(fjms_service, "_default_service", svc)
    dlg = _open_dialog(monkeypatch, saved)
    assert _visible_notices(dlg) == []
    assert dlg.dropped_saved_filters == []
    assert dlg.get_filters()['criteria'] == saved['criteria']


def test_dialog_without_a_catalog_blocks_ok_until_the_filters_are_cleared(monkeypatch, absent_sidecar):
    """K-17: with no catalog, saved filters are kept, the count says it could
    not run, OK stays disabled; Clear All is the way out."""
    dlg_mod, sync_worker = _sync_worker_class()
    dlg = _open_dialog(monkeypatch, SAVED, worker=sync_worker)
    assert dlg.get_filters().get('criteria') == SAVED['criteria'], "a saved value was dropped on a guess"
    assert dlg.count_label.text() == dlg_mod.tr("Could not update filter count")
    assert dlg.ok_btn.isEnabled() is False
    dlg._clear_all()
    assert dlg.ok_btn.isEnabled() is True
    assert not any(k != 'include_mode' for k in dlg.get_filters())

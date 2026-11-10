"""Desktop pickers for shelfmarks that name several records (catalogue-shelfmark plan, gate 9).

Rows C (Puzzle add box), D (Joins dialog fragment B) and E (Share a Discovery) of
docs/plans/CATALOGUE_SHELFMARK_FORMS_PLAN.md. The resolver worker is replaced by a fake
whose answer the test delivers, so each case is exact about what the user sees:
two options call the picker with numbered labels, choosing adds the chosen record,
cancel adds nothing, and a Puzzle lookup answered after New/Open adds nothing.
"""
import gc
import sys
import time
import types

import pytest

pytestmark = pytest.mark.gui

from PyQt6.QtWidgets import (  # noqa: E402
    QApplication, QComboBox, QDialog, QInputDialog, QLineEdit, QListWidget, QPushButton, QSpinBox, QWidget,
)

sys.path.insert(0, __file__.rsplit('\\', 1)[0].rsplit('/', 1)[0])
import puzzle_window_harness as pwh  # noqa: E402

TWO = [{'sys_id': '990000000000000011', 'shelfmark': 'Moss. VIII,402.1', 'title': ''},
       {'sys_id': '990000000000000012', 'shelfmark': 'Moss. VIII,402.2', 'title': 'Part two'}]


class FakeResolver:
    """Stands in for desktop.gui_threads.ShelfmarkResolveThread."""
    made: list = []

    def __init__(self, meta_mgr, text):
        self.meta_mgr, self.text = meta_mgr, text
        self.resolved, self.failed = pwh.FakeSignal(), pwh.FakeSignal()
        FakeResolver.made.append(self)

    def start(self):
        pass

    def isRunning(self):
        return False

    def wait(self, *_a):
        return True


@pytest.fixture(autouse=True)
def _fresh():
    FakeResolver.made.clear()
    yield


def _wait(condition, timeout=10.0):
    """Deliver queued cross-thread signals until `condition()` holds."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, 'timed out waiting'
        QApplication.processEvents()
        time.sleep(0.01)


# ---------------------------------------------------------------- C: Puzzle

@pytest.fixture
def env(tmp_path, monkeypatch):
    e = pwh.PuzzleEnv.create(tmp_path, monkeypatch)
    monkeypatch.setattr(e.dp, 'ShelfmarkResolveThread', FakeResolver)
    e.host.meta_mgr = types.SimpleNamespace(get_meta_for_id=lambda sid: ('', ''),
                                            get_library_for_id=lambda sid: '')
    e.host._ensure_shelf_map = lambda: None
    e.host._shelf_to_sys = {}
    yield e
    e.close()


def _type_and_add(env, text):
    env.win.shelfmark_input.setText(text)
    env.win._on_add_shelfmark()
    assert len(FakeResolver.made) == 1 and FakeResolver.made[0].text == text
    assert not env.win.btn_add.isEnabled()          # busy while the lookup runs
    return FakeResolver.made[0]


def _labels_seen(env):
    seen = []

    def answer(_text, labels, _current, _editable):
        seen.append(list(labels))
        return labels[1], True
    env.static_answers[env.dp.tr('Shelfmark')] = answer
    return seen


def test_puzzle_two_options_open_the_picker_and_add_the_chosen_one(env):
    seen = _labels_seen(env)
    lookup = _type_and_add(env, 'Mosseri, Jacques, Ms. VIII.402')
    lookup.resolved.emit({'sys_id': None, 'options': TWO, 'selected_shelfmark': None})
    assert seen == [['1. Moss. VIII,402.1', '2. Moss. VIII,402.2 | Part two']]
    assert [m.sys_id for m in pwh.FakeMetaLoader.made] == ['990000000000000012']
    assert env.win.btn_add.isEnabled() and env.win.shelfmark_input.text() == ''


def test_puzzle_cancelled_picker_adds_nothing(env):
    env.static_answers[env.dp.tr('Shelfmark')] = lambda *a: ('', False)
    lookup = _type_and_add(env, 'Mosseri, Jacques, Ms. VIII.402')
    lookup.resolved.emit({'sys_id': None, 'options': TWO, 'selected_shelfmark': None})
    assert pwh.FakeMetaLoader.made == []
    assert env.win.shelfmark_input.text() == 'Mosseri, Jacques, Ms. VIII.402'


@pytest.mark.parametrize('replacement', ['new', 'open_join'])
def test_puzzle_lookup_answered_after_the_canvas_was_replaced_adds_nothing(env, replacement):
    from shared.puzzle_model import PuzzleFragment
    doc_b = env.save([PuzzleFragment(sys_id='990011', folio_label='1r', fl_id='FL11', shelfmark='T-S B 11',
                                     x=10.0, y=20.0)], title='B')
    lookup = _type_and_add(env, 'T-S K 08.026')
    if replacement == 'new':
        env.win._on_new_puzzle()
    else:
        env.open(doc_b)
    lookup.resolved.emit({'sys_id': '990051208260205171', 'options': [], 'selected_shelfmark': 'T-S K8.26'})
    lookup.resolved.emit({'sys_id': None, 'options': TWO, 'selected_shelfmark': None})   # no picker either
    asked = {'990051208260205171'} | {o['sys_id'] for o in TWO}
    assert [m.sys_id for m in pwh.FakeMetaLoader.made if m.sys_id in asked] == []   # opening B loads its own
    assert env.static_calls == []
    assert env.win.btn_add.isEnabled()


def test_puzzle_unique_answer_adds_and_none_says_not_found(env):
    lookup = _type_and_add(env, 'T-S K 08.026')
    lookup.resolved.emit({'sys_id': '990051208260205171', 'options': [], 'selected_shelfmark': 'T-S K8.26'})
    assert [m.sys_id for m in pwh.FakeMetaLoader.made] == ['990051208260205171']
    FakeResolver.made.clear()
    lookup = _type_and_add(env, 'nothing 999')
    lookup.resolved.emit({'sys_id': None, 'options': [], 'selected_shelfmark': None})
    assert env.dp.tr('Shelfmark not found') in env.win.statusBar().currentMessage()


def test_puzzle_quick_map_hit_needs_no_lookup(env):
    env.host._shelf_to_sys = {env.dp.normalize_shelfmark('T-S X 7'): '990077'}
    env.win.shelfmark_input.setText('T-S X 7')
    env.win._on_add_shelfmark()
    assert FakeResolver.made == []
    assert [m.sys_id for m in pwh.FakeMetaLoader.made] == ['990077']


# ---------------------------------------------------------------- D: Joins dialog fragment B

class _Host(QWidget):
    visible = True

    def isVisible(self):
        return self.visible


def _joins_host(monkeypatch, csv_bank=None):
    import desktop.corrections_ui as cu
    import desktop.gui_threads as gt
    monkeypatch.setattr(gt, 'ShelfmarkResolveThread', FakeResolver)
    monkeypatch.setattr(gt, '_keep_until_finished', lambda w: None)
    host = _Host()
    host.frag_a_input, host.frag_b_input = QLineEdit(host), QLineEdit(host)
    host.notes_input, host.type_combo = QLineEdit(host), QComboBox(host)
    host.btn_create = QPushButton(host)
    host.document_id = '990000000000000001'
    host._selected_doc_id_b = None
    host.meta_mgr = types.SimpleNamespace(csv_bank=csv_bank or {}, resolve_system_by_shelfmark=None)
    host.finished = []
    for name in ('create_new_join', '_resolve_frag_b_then_create', '_on_frag_b_resolved', '_normalize_shelfmark'):
        setattr(host, name, types.MethodType(getattr(cu.JoinsDialog, name), host))
    host._finish_create_join = lambda *a: host.finished.append(a)
    return host


def _choose_second(monkeypatch, seen=None):
    import desktop.shelfmark_picker as sp

    def get_item(parent, title, text, labels, current, editable):
        if seen is not None:
            seen.append(list(labels))
        return labels[1], True
    monkeypatch.setattr(sp.QInputDialog, 'getItem', staticmethod(get_item))


def test_joins_fragment_b_picker_sends_the_chosen_record(monkeypatch):
    seen = []
    _choose_second(monkeypatch, seen)
    host = _joins_host(monkeypatch)
    host.frag_a_input.setText('T-S 1.1')
    host.frag_b_input.setText('Mosseri, Jacques, Ms. VIII.402')
    host.create_new_join()
    assert len(FakeResolver.made) == 1 and not host.btn_create.isEnabled()
    FakeResolver.made[0].resolved.emit({'sys_id': None, 'options': TWO, 'selected_shelfmark': None})
    assert seen == [['1. Moss. VIII,402.1', '2. Moss. VIII,402.2 | Part two']]
    assert host.finished == [('T-S 1.1', 'Moss. VIII,402.2', None, None, '990000000000000001', '990000000000000012')]
    assert host.btn_create.isEnabled()


def test_joins_fragment_b_cancel_creates_nothing(monkeypatch):
    import desktop.shelfmark_picker as sp
    monkeypatch.setattr(sp.QInputDialog, 'getItem', staticmethod(lambda *a: ('', False)))
    host = _joins_host(monkeypatch)
    host.frag_a_input.setText('T-S 1.1')
    host.frag_b_input.setText('Mosseri, Jacques, Ms. VIII.402')
    host.create_new_join()
    FakeResolver.made[0].resolved.emit({'sys_id': None, 'options': TWO, 'selected_shelfmark': None})
    assert host.finished == []


def test_joins_fragment_b_not_found_or_failed_creates_by_shelfmark_as_before(monkeypatch):
    host = _joins_host(monkeypatch)
    host.frag_a_input.setText('T-S 1.1')
    host.frag_b_input.setText('Unknown 5')
    host.create_new_join()
    FakeResolver.made[0].resolved.emit({'sys_id': None, 'options': [], 'selected_shelfmark': None})
    host.create_new_join()
    FakeResolver.made[1].failed.emit('boom')
    assert host.finished == [('T-S 1.1', 'Unknown 5', None, None, '990000000000000001', None)] * 2


def test_joins_fragment_b_inline_hit_needs_no_lookup_and_closed_dialog_ignores_answers(monkeypatch):
    host = _joins_host(monkeypatch, csv_bank={'990000000000000044': {'shelfmark': 'T-S 4.4'}})
    host.frag_a_input.setText('T-S 1.1')
    host.frag_b_input.setText('T-S 4.4')
    host.create_new_join()
    assert FakeResolver.made == []
    assert host.finished[-1][-1] == '990000000000000044'
    host.frag_b_input.setText('Unknown 5')
    host.create_new_join()
    host.visible = False                            # closed while the lookup ran
    FakeResolver.made[0].resolved.emit({'sys_id': '990000000000000055', 'options': [], 'selected_shelfmark': 'U 5'})
    assert len(host.finished) == 1


def test_joins_fragment_b_real_worker_survives_until_it_answers(monkeypatch):
    """The real ShelfmarkResolveThread and keeper: a worker handed to the keeper before
    start() was released at once and destroyed while running (0xC0000409)."""
    import desktop.corrections_ui as cu

    def slow_resolve(text):
        time.sleep(0.3)
        return {'sys_id': '990000000000000066', 'options': [], 'selected_shelfmark': 'T-S 6.6'}
    host = _Host()
    host.frag_a_input, host.frag_b_input = QLineEdit(host), QLineEdit(host)
    host.notes_input, host.type_combo, host.btn_create = QLineEdit(host), QComboBox(host), QPushButton(host)
    host.document_id, host._selected_doc_id_b = '990000000000000001', None
    host.meta_mgr = types.SimpleNamespace(csv_bank={}, resolve_system_by_shelfmark=slow_resolve)
    host.finished = []
    for name in ('create_new_join', '_resolve_frag_b_then_create', '_on_frag_b_resolved', '_normalize_shelfmark'):
        setattr(host, name, types.MethodType(getattr(cu.JoinsDialog, name), host))
    host._finish_create_join = lambda *a: host.finished.append(a)
    host.frag_a_input.setText('T-S 1.1')
    host.frag_b_input.setText('T-S 06.006')
    host.create_new_join()
    gc.collect()                                    # nothing but the keeper/dialog holds it
    _wait(lambda: host.finished)
    assert host.finished == [('T-S 1.1', 'T-S 6.6', None, None, '990000000000000001', '990000000000000066')]
    assert host._frag_b_pending is None and host.btn_create.isEnabled()


# ---------------------------------------------------------------- E: Share a Discovery

def _discovery_host(result_for):
    import desktop.corrections_ui as cu
    host = _Host()
    host.shelfmark_input, host.page_input = QLineEdit(host), QSpinBox(host)
    host.shelfmarks_widget, host.shelfmarks_list = QListWidget(host), []
    host.meta_mgr = types.SimpleNamespace(resolve_system_by_shelfmark=result_for)
    for name in ('_add_shelfmark_to_list', '_add_shelfmark_entry'):
        setattr(host, name, types.MethodType(getattr(cu.CreateDiscoveryDialog, name), host))
    return host


def test_discovery_two_parts_of_one_unit_stay_two_entries(monkeypatch):
    import desktop.shelfmark_picker as sp
    picks = iter([0, 1])
    monkeypatch.setattr(sp.QInputDialog, 'getItem',
                        staticmethod(lambda p, t, x, labels, c, e: (labels[next(picks)], True)))
    host = _discovery_host(lambda text: {'sys_id': None, 'options': TWO, 'selected_shelfmark': None})
    for _ in range(2):
        host.shelfmark_input.setText('Mosseri, Jacques, Ms. VIII.402')
        host._add_shelfmark_to_list()
    assert host.shelfmarks_list == [('Moss. VIII,402.1', 0, '990000000000000011'),
                                    ('Moss. VIII,402.2', 0, '990000000000000012')]
    assert host.shelfmark_input.text() == ''


def test_discovery_keeps_distinct_records_that_share_a_shelfmark(monkeypatch):
    import desktop.shelfmark_picker as sp
    same = [{'sys_id': '990001986850205171', 'shelfmark': 'Ms. 1358', 'title': ''},
            {'sys_id': '990001987930205171', 'shelfmark': 'Ms. 1358', 'title': ''}]
    picks = iter([0, 1])
    monkeypatch.setattr(sp.QInputDialog, 'getItem',
                        staticmethod(lambda p, t, x, labels, c, e: (labels[next(picks)], True)))
    host = _discovery_host(lambda text: {'sys_id': None, 'options': same, 'selected_shelfmark': None})
    for _ in range(2):
        host.shelfmark_input.setText('Gaster, Moses Collection, Ms. 1358')
        host._add_shelfmark_to_list()
    assert [e[2] for e in host.shelfmarks_list] == ['990001986850205171', '990001987930205171']


def test_discovery_cancel_adds_nothing_and_keeps_the_text(monkeypatch):
    import desktop.shelfmark_picker as sp
    monkeypatch.setattr(sp.QInputDialog, 'getItem', staticmethod(lambda *a: ('', False)))
    host = _discovery_host(lambda text: {'sys_id': None, 'options': TWO, 'selected_shelfmark': None})
    host.shelfmark_input.setText('Mosseri, Jacques, Ms. VIII.402')
    host._add_shelfmark_to_list()
    assert host.shelfmarks_list == [] and host.shelfmark_input.text() == 'Mosseri, Jacques, Ms. VIII.402'


def test_discovery_exact_answer_as_before_and_same_record_once(monkeypatch):
    monkeypatch.setattr(QInputDialog, 'getItem', staticmethod(lambda *a: pytest.fail('no picker for one record')))
    host = _discovery_host(lambda text: {'sys_id': '990051334060205171', 'options': [], 'selected_shelfmark': 'T-S 12.123'})
    for text in ('T-S 12.123', 'Ms. T-S 12.123'):     # two spellings of one record
        host.shelfmark_input.setText(text)
        host._add_shelfmark_to_list()
    assert host.shelfmarks_list == [('T-S 12.123', 0, '990051334060205171')]


# ---------------------------------------------------------------- F: the desktop Exclude dialog

def _exclude_dialog(monkeypatch):
    import shared.shelfmark_bridge as bridge
    from desktop.dialogs_filter import ExcludeDialog
    from shared.metadata_manager import MetadataManager
    monkeypatch.setattr(bridge, 'lookup_cudl', lambda text: None)
    rows = {
        'CUL1': ('CUL', ['Cambridge University Library Ms. T-S K 8.26', 'T-S K8.26']),
        'CUL2': ('CUL', ['Cambridge University Library Ms. T-S 12.123', 'T-S 12.123']),
        'CUL3': ('CUL', ['Cambridge University Library Ms. T-S AS 31.1', 'T-S AS 31.1']),
        'MOS1': ('Mosseri', ['Moss. VIII,402.1']),
        'MOS2': ('Mosseri', ['Moss. VIII,402.2']),
        'MOS3': ('Mosseri', ['Mosseri, Jacques Ms. IV 366', 'Mosseri, Jacques Ms. IV 367',
                             'Mosseri, Jacques Ms. IV 368']),
    }
    mm = MetadataManager.__new__(MetadataManager)
    mm.csv_bank = {sid: {'shelfmark': min(v, key=len), 'title': '', 'library_code': c, 'call_numbers_raw': v}
                   for sid, (c, v) in rows.items()}
    mm.nli_cache, mm._shelfmark_index = {}, None
    parent = _Host()
    parent.meta_mgr = mm
    dlg = ExcludeDialog(parent)
    _KEEP.append((parent, dlg))     # the parent owns the dialog; keep both alive
    return dlg


_KEEP = []


LINES = ['T-S 12.123', 'Cambridge University Library, Ms. T-S K 08.026', 'Mosseri, Jacques, Ms. VIII.402', 'nothing 999']


def test_exclude_dialog_resolve_button_uses_the_catalogue_fallback(monkeypatch):
    dlg = _exclude_dialog(monkeypatch)
    dlg.shelf_text_area.setPlainText('\n'.join(LINES))
    dlg._resolve_and_show_report()
    assert dlg._catalog_thread is not None and not dlg._btn_apply.isEnabled()   # off the UI thread
    gc.collect()
    _wait(lambda: dlg._catalog_thread is None)
    assert dlg._btn_apply.isEnabled()
    assert dlg._resolved_ids == {'CUL2', 'CUL1'}
    assert [e.status for e in dlg._resolved_entries] == ['found', 'found', 'not_found', 'not_found']
    assert dlg._resolved_unresolved == LINES[2:]          # the ambiguous Mosseri unit stays out
    [source] = dlg.get_exclusion_sources()
    assert source.sys_ids == {'CUL2', 'CUL1'}


def test_exclude_dialog_apply_without_resolve_uses_it_too(monkeypatch):
    dlg = _exclude_dialog(monkeypatch)
    dlg.shelf_text_area.setPlainText('\n'.join(LINES))  # the per-keystroke sync runs, unchanged
    assert dlg._resolved_ids in (None, set(), [])
    dlg.accept()
    assert dlg.result() != QDialog.DialogCode.Accepted      # waits for the lookup first
    _wait(lambda: dlg.result() == QDialog.DialogCode.Accepted)
    [source] = dlg.get_exclusion_sources()
    assert source.sys_ids == {'CUL2', 'CUL1'}


def test_exclude_dialog_never_looks_up_on_the_ui_thread(monkeypatch):
    """Codex implementation round 2: a digit-only line the map misses reached the fallback
    synchronously on Resolve, and text edited during a lookup was looked up afterwards."""
    import threading
    dlg = _exclude_dialog(monkeypatch)
    real = dlg.meta_mgr.resolve_catalog_forms
    threads = []

    def spy(lines, limit=100):
        threads.append(threading.current_thread() is threading.main_thread())
        return real(lines, limit=limit)
    monkeypatch.setattr(dlg.meta_mgr, 'resolve_catalog_forms', spy)
    dlg.shelf_text_area.setPlainText('999999999999999999\nCambridge University Library, Ms. T-S K 08.026')
    dlg._resolve_and_show_report()
    assert dlg._catalog_thread is not None                   # the digit-only line is fetched too
    assert dlg.shelf_text_area.isReadOnly()                  # no edits while it runs
    _wait(lambda: dlg._catalog_thread is None)
    assert not dlg.shelf_text_area.isReadOnly()
    assert dlg._resolved_ids == {'CUL1'}
    dlg.accept()
    _wait(lambda: dlg.result() == QDialog.DialogCode.Accepted)
    assert threads and not any(threads)                     # every lookup ran off the UI thread


def test_exclude_dialog_cancel_during_the_lookup_applies_nothing(monkeypatch):
    dlg = _exclude_dialog(monkeypatch)
    dlg.shelf_text_area.setPlainText('\n'.join(LINES))
    dlg.accept()
    dlg.reject()
    _wait(lambda: dlg._catalog_thread is None)
    assert dlg.result() == QDialog.DialogCode.Rejected

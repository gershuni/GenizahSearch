"""The desktop's offer to rebuild an index of an earlier format (Responsa plan §6.1, "What the
desktop tells a user on an old index"; Phase 3 gives a rebuild something to bring).

Once per index, after startup: "Rebuild now" opens the existing rebuild (its own confirmation
follows); "Later" is remembered for that index, so a rebuilt or replaced index is a new
question; a current index is never asked about; nothing starts a rebuild by itself.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from unittest.mock import patch  # noqa: E402

import pytest  # noqa: E402
from PyQt6.QtWidgets import QApplication, QWidget  # noqa: E402

from genizah_app import GenizahGUI  # noqa: E402
from shared.config import Config  # noqa: E402

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

_APP = QApplication.instance() or QApplication([])


class _Engine:
    index = object()

    def __init__(self, current):
        self.current = current

    def index_format_status(self):
        return {'current': self.current}


@pytest.fixture
def home(tmp_path):
    """An index folder with a committed main index, and the app config beside it."""
    db = tmp_path / "tantivy_db"
    db.mkdir()
    (db / "meta.json").write_text("{}", encoding="utf-8")
    with patch.object(Config, "INDEX_DIR", str(tmp_path)), \
            patch.object(Config, "CONFIG_FILE", str(tmp_path / "config.pkl")):
        yield db


def _window(current=False, answer='later'):
    w = GenizahGUI.__new__(GenizahGUI)
    QWidget.__init__(w)
    w.searcher = _Engine(current)
    w.indexer = object()
    w.asked, w.rebuilds = [], []
    w._ask_rebuild = lambda: w.asked.append(1) or answer
    w.run_indexing = lambda: w.rebuilds.append(1)
    return w


def test_an_old_index_is_offered_a_rebuild_and_later_is_remembered_for_it(home):
    w = _window(answer='later')
    w._offer_index_rebuild()
    assert w.asked == [1] and w.rebuilds == []
    w._offer_index_rebuild()                     # the next launch: the same index, not asked
    assert w.asked == [1]


def test_a_rebuilt_index_is_a_new_question(home):
    w = _window(answer='later')
    w._offer_index_rebuild()
    stamp = os.path.getmtime(home / "meta.json")
    os.utime(home / "meta.json", (stamp + 10, stamp + 10))      # committed again
    w._offer_index_rebuild()
    assert w.asked == [1, 1]


def test_rebuild_now_opens_the_existing_rebuild_and_its_confirmation(home):
    w = _window(answer='rebuild')
    w._offer_index_rebuild()
    assert w.rebuilds == [1]                     # run_indexing: "Start indexing?" follows


@pytest.mark.parametrize('current, indexer', [(True, True), (False, False)])
def test_no_offer_for_a_current_index_or_when_no_build_can_run(home, current, indexer):
    w = _window(current=current)
    if not indexer:
        w.indexer = None
    w._offer_index_rebuild()
    assert w.asked == [] and w.rebuilds == []


def test_the_offer_waits_while_another_window_waits_on_the_user(home):
    w = _window()
    with patch.object(QApplication, "activeModalWidget", return_value=QWidget()), \
            patch("genizah_app.QTimer.singleShot") as later:
        w._offer_index_rebuild()
    assert w.asked == [] and later.call_count == 1


@pytest.mark.parametrize('problem', ['unreadable', 'busy'])
def test_a_config_that_cannot_be_read_means_no_offer(home, problem):
    # Codex round 5 on PR #443: the config reader answers {} for a config.pkl it cannot
    # read, never an exception, so that {} read as "no Later" and the offer was shown.
    (home.parent / "config.pkl").write_bytes(b"not a pickle")
    w = _window()
    if problem == 'busy':                        # held by antivirus or the indexer
        with patch("genizah_core.read_bytes", side_effect=PermissionError("in use")):
            w._offer_index_rebuild()
    else:
        w._offer_index_rebuild()
    assert w.asked == []


# --- the third answer, "Rebuild when I exit", and the close path that runs it ---------------
# (the PR after #443: the offer names three outcomes; the build runs beside the live index.)

from pathlib import Path  # noqa: E402

from PyQt6.QtWidgets import QMessageBox, QPushButton  # noqa: E402

import genizah_app  # noqa: E402
import genizah_core  # noqa: E402
from shared.index_swap import BuildPlan  # noqa: E402

_ROOM = BuildPlan('beside', 15 * 1024 ** 3, 40 * 1024 ** 3, 13 * 1024 ** 3)
_NO_ROOM = BuildPlan('inplace', 15 * 1024 ** 3, 4 * 1024 ** 3, 13 * 1024 ** 3)


@pytest.fixture
def english(monkeypatch):
    monkeypatch.setattr(genizah_core, 'CURRENT_LANG', 'en')


def _config():
    return genizah_core.load_app_config()


def test_rebuild_when_i_exit_is_recorded_for_this_index_and_not_asked_again(home):
    w = _window(answer='on_exit')
    w._offer_index_rebuild()
    assert w.asked == [1] and w.rebuilds == []
    cfg = _config()
    assert cfg[GenizahGUI._REBUILD_ON_EXIT_KEY] == w._index_identity()
    assert GenizahGUI._REBUILD_LATER_KEY not in cfg
    w._offer_index_rebuild()                     # the next launch before any exit: not asked
    assert w.asked == [1]


def test_the_offer_names_three_outcomes_when_the_disk_has_room(home, english):
    w = _window()
    with patch.object(genizah_app, 'plan_build', return_value=_ROOM):
        box, buttons = GenizahGUI._rebuild_offer_box(w)
    assert {b.text() for b in box.buttons()} == {"Rebuild now", "Rebuild when I exit", "Later"}
    assert set(buttons) == {'rebuild', 'on_exit', 'later'}
    # Laid out Rebuild now | Rebuild when I exit | Later: accept, action, reject.
    assert [box.buttonRole(buttons[k]) for k in ('rebuild', 'on_exit', 'later')] == [
        QMessageBox.ButtonRole.AcceptRole, QMessageBox.ButtonRole.ActionRole, QMessageBox.ButtonRole.RejectRole]
    text = box.informativeText()
    for outcome in ("Rebuild now builds the new index beside the current one",
                    "Rebuild when I exit runs the build after you close the program",
                    "Later leaves the index as it is"):
        assert outcome in text
    assert "searching keeps working meanwhile" in text
    assert box.defaultButton() is buttons['later']


def test_without_room_the_offer_has_no_exit_answer_and_says_why(home, english):
    w = _window()
    with patch.object(genizah_app, 'plan_build', return_value=_NO_ROOM):
        box, buttons = GenizahGUI._rebuild_offer_box(w)
    assert {b.text() for b in box.buttons()} == {"Rebuild now", "Later"}
    assert 'on_exit' not in buttons
    text = box.informativeText()
    assert "not enough free disk space" in text and "about 15.0 GB needed, 4.0 GB free" in text
    assert "rebuilds it in place: searching waits until the build ends" in text


@pytest.mark.parametrize('clicked, answer', [('rebuild', 'rebuild'), ('on_exit', 'on_exit'),
                                             ('later', 'later'), (None, 'later')])
def test_each_button_is_its_answer_and_closing_the_window_is_later(home, clicked, answer):
    w = _window()
    real_make = GenizahGUI._rebuild_offer_box

    def make(self):
        box, buttons = real_make(self)
        box._chosen = buttons[clicked] if clicked else None
        return box, buttons

    with patch.object(genizah_app, 'plan_build', return_value=_ROOM), \
            patch.object(QMessageBox, 'exec', lambda box: None), \
            patch.object(QMessageBox, 'clickedButton', lambda box: box._chosen), \
            patch.object(GenizahGUI, '_rebuild_offer_box', make):
        assert GenizahGUI._ask_rebuild(w) == answer


class _Event:
    def __init__(self):
        self.ignored = False

    def ignore(self):
        self.ignored = True


def _record_start(w):
    def start(mode, at_exit=False):
        w.started.append((mode, at_exit))
        w._index_build = {'mode': mode, 'clock': genizah_app.BuildClock(), 'at_exit': at_exit}
    return start


@pytest.fixture(autouse=True)
def _no_modal_boxes():
    """A path a mutation opens must fail, not block on a real box offscreen."""
    with patch.object(QMessageBox, 'information'), patch.object(QMessageBox, 'critical'),             patch.object(QMessageBox, 'question', return_value=QMessageBox.StandardButton.No):
        yield


@pytest.fixture
def at_exit(home, tmp_path):
    """A window whose index has the exit answer recorded, a corpus file to build from, room
    on the disk, and a build that is only recorded, never run."""
    w = _window()
    w.show()
    genizah_core.save_app_config({GenizahGUI._REBUILD_ON_EXIT_KEY: w._index_identity()})
    corpus = tmp_path / "Transcriptions.txt"
    corpus.write_text("x", encoding="utf-8")
    w.started, w.closed = [], []
    w._close_result_dialog = lambda: None
    w.close = lambda: w.closed.append(1) or True
    w.index_rebuild_bar = genizah_app.IndexRebuildBar(w)
    w._start_index_build = _record_start(w)
    with patch.object(Config, 'FILE_V8', str(corpus)), \
            patch.object(genizah_app, 'plan_build', return_value=_ROOM):
        yield w
    QApplication.instance().setQuitOnLastWindowClosed(True)
    dlg = getattr(w, '_exit_rebuild_progress', None)
    if dlg is not None:
        dlg.close()


def test_the_close_hides_the_window_and_runs_the_build_in_its_own_window(at_exit, english):
    w = at_exit
    event = _Event()
    assert w._defer_close_for_index_rebuild(event) is True
    assert event.ignored and w.isHidden()
    assert w.started == [('beside', True)]
    dlg = w._exit_rebuild_progress
    assert dlg is not None and dlg.isVisible()
    assert dlg.labelText().startswith("Rebuilding the search index before closing: 0%, estimating the time left.")
    assert "Cancel keeps the index you have" in dlg.labelText()
    assert QApplication.instance().quitOnLastWindowClosed() is False
    # The answer is spent by this close: a crash now cannot run the build at every close.
    assert _config().get(GenizahGUI._REBUILD_ON_EXIT_KEY) is None
    # A second close while the window is up only waits.
    again = _Event()
    assert w._defer_close_for_index_rebuild(again) is True and again.ignored
    assert w.started == [('beside', True)]
    # Progress goes to that window.
    w._index_build['clock'].started -= 120
    w.on_index_progress(5000, 10000)
    assert dlg.value() == 27 and "27%, about " in dlg.labelText()


def test_when_the_build_ends_the_close_goes_ahead_and_the_program_quits(at_exit):
    w = at_exit
    w._defer_close_for_index_rebuild(_Event())
    with patch.object(QApplication, 'quit') as quit_:
        w.on_index_cancelled()                     # or finished, or failed: the same way out
    assert w.closed == [1] and quit_.call_count == 1
    assert w._exit_rebuild_progress is None and w._exit_rebuild_done
    assert w._defer_close_for_index_rebuild(_Event()) is False      # the close goes through now


def test_cancel_in_the_exit_window_asks_the_build_to_stop_and_keeps_the_index(at_exit, english):
    w = at_exit
    w._defer_close_for_index_rebuild(_Event())

    class _Th:
        cancelled = False

        def isRunning(self):
            return True

        def request_cancel(self):
            self.cancelled = True

    w.ithread = _Th()
    w._exit_rebuild_progress.findChild(QPushButton).click()    # the Cancel button (the window's X does the same)
    assert w.ithread.cancelled
    assert w._exit_rebuild_progress.labelText() == "Cancelling... the index you have is kept."


def test_a_build_already_running_at_close_moves_to_the_exit_window(at_exit, english):
    w = at_exit
    genizah_core.save_app_config({GenizahGUI._REBUILD_ON_EXIT_KEY: None})
    w._start_index_build('inplace')
    w.index_rebuild_bar.begin('inplace')
    event = _Event()
    assert w._defer_close_for_index_rebuild(event) is True and event.ignored
    assert w.started == [('inplace', False)] and w._index_build['at_exit'] is True
    assert not w.index_rebuild_bar.isVisibleTo(w)
    dlg = w._exit_rebuild_progress
    assert "cannot be cancelled" in dlg.labelText()
    assert dlg.findChild(QPushButton) is None                  # no Cancel: its index is already gone


@pytest.mark.parametrize('why', ['no-room', 'current', 'no-corpus'])
def test_the_exit_build_is_skipped_and_the_answer_spent_when_it_cannot_run_safely(at_exit, why):
    w = at_exit
    if why == 'current':
        w.searcher = _Engine(True)
    patches = []
    if why == 'no-room':
        patches.append(patch.object(genizah_app, 'plan_build', return_value=_NO_ROOM))
    if why == 'no-corpus':
        patches.append(patch.object(Config, 'FILE_V8', str(Path(Config.FILE_V8).parent / "missing.txt")))
    for p in patches:
        p.start()
    try:
        event = _Event()
        assert w._defer_close_for_index_rebuild(event) is False
    finally:
        for p in patches:
            p.stop()
    assert not event.ignored and w.started == [] and not w.isHidden()
    assert _config().get(GenizahGUI._REBUILD_ON_EXIT_KEY) is None
    if why != 'current':
        w._offer_index_rebuild()                               # the offer comes back next start
        assert w.asked == [1]


def test_no_exit_build_without_the_answer(at_exit):
    w = at_exit
    genizah_core.save_app_config({GenizahGUI._REBUILD_ON_EXIT_KEY: 'another index'})
    event = _Event()
    assert w._defer_close_for_index_rebuild(event) is False and not event.ignored
    assert w.started == [] and not w.isHidden()


def test_a_failed_exit_build_reports_and_then_the_close_goes_ahead(at_exit):
    w = at_exit
    w._defer_close_for_index_rebuild(_Event())
    order = []
    with patch.object(QMessageBox, 'critical', side_effect=lambda *a: order.append('message')),             patch.object(QApplication, 'quit', side_effect=lambda: order.append('quit')):
        w.close = lambda: order.append('close') or True
        w.on_index_error("disk full")
    assert order == ['message', 'close', 'quit']
    assert w._exit_rebuild_progress is None and w._index_build is None


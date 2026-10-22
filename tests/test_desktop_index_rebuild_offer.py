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


def test_an_unreadable_answer_means_no_offer(home):
    w = _window()
    with patch("genizah_app.load_app_config", side_effect=OSError("busy")):
        w._offer_index_rebuild()
    assert w.asked == []

# -*- coding: utf-8 -*-
"""The desktop Reading Desk's image side (GS-294): the view switcher, one image at a time
beside a strip of thumbnails, all images, the puzzle images, and the text side following the
image on show (desktop/reading_desk_images.py, desktop/reading_desk_puzzles.py, the desk's
methods in genizah_app.py).

Image loaders are replaced by fakes that never start a thread; a test delivers what a loader
would send. Nothing here reaches the network, joins.db or the image cache.
"""
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.gui  # constructs widgets: gui bucket only

from PIL import Image  # noqa: E402
from PyQt6.QtCore import Qt, QThread, pyqtSignal  # noqa: E402
from PyQt6.QtGui import QColor, QImage  # noqa: E402
from PyQt6.QtWidgets import (  # noqa: E402
    QApplication, QComboBox, QInputDialog, QLabel, QListWidget, QPushButton, QTextEdit, QWidget,
)

import desktop.reading_desk_images as rdi  # noqa: E402
import desktop.reading_desk_puzzles as rdp  # noqa: E402
from shared.reading_desk_model import ReadingDeskEntry, ReadingDeskState  # noqa: E402

APP = QApplication.instance() or QApplication([])
_KEEP = []  # widgets kept for the process (gui files run one per process)


class FakeLoader(QThread):
    """ImageLoaderThread's signals, never started."""
    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal()
    made: list = []

    def __init__(self, url):
        super().__init__()
        self.url = url
        self.cancelled = False
        self.started_ = False
        FakeLoader.made.append(self)

    def start(self):
        self.started_ = True

    def cancel(self):
        self.cancelled = True


def _image(w=40, h=60):
    img = QImage(w, h, QImage.Format.Format_RGB32)
    img.fill(QColor('white'))
    return img


@pytest.fixture(autouse=True)
def fake_loaders(monkeypatch):
    """No loader, lookup or composer ever runs on a thread of its own here: a test runs one
    where it is started (``run_now``) or delivers its signals itself."""
    FakeLoader.made = []
    monkeypatch.setattr(rdi, 'ImageLoaderThread', FakeLoader)
    monkeypatch.setattr(rdp, 'PublishedPictureThread', FakeLoader)
    monkeypatch.setattr(rdp.PuzzleLookupThread, 'start', lambda self: None)
    monkeypatch.setattr(rdp.PuzzlePictureThread, 'start', lambda self: None)
    rdp._PICTURES.clear()
    yield


def _nli(fl):
    return {'label': '', 'url': f'https://iiif.nli.org.il/IIIFv21/FL{fl}', 'fl_id': fl}


def _desk(fragments=2):
    state = ReadingDeskState()
    lists = {}
    for n in range(1, fragments + 1):
        sid = f'99000{n}'
        state.entries.append(ReadingDeskEntry(
            sys_id=sid, shelfmark=f'T-S {n}', sequence_order=n,
            pages=[{'p_num': 1, 'text': f'text {n} recto', 'fl_id': f'{n}01'},
                   {'p_num': 2, 'text': f'text {n} verso', 'fl_id': f'{n}02'}]))
        lists[sid] = [dict(_nli(f'{n}01'), label=f'{n}r'), dict(_nli(f'{n}02'), label=f'{n}v')]
    return state, lists


def _pane(state, lists, **kw):
    pane = rdi.ReadingDeskImagePane(puzzle_options=kw.pop('puzzle_options', None))
    pane.resize(600, 700)
    pane.set_desk(state, lists, **kw)
    _KEEP.append(pane)
    return pane


# -- the switcher ----------------------------------------------------------------

def test_puzzle_images_are_offered_only_on_two_fragments_or_more():
    one, lists1 = _desk(1)
    assert _pane(one, lists1).view_buttons['join'].isHidden()
    two, lists2 = _desk(2)
    assert not _pane(two, lists2).view_buttons['join'].isHidden()


def test_the_default_view_follows_the_document_rule():
    state, lists = _desk(2)
    assert _pane(state, lists, document_desk=False).current_view() == 'all'
    state, lists = _desk(2)
    assert _pane(state, lists, document_desk=True).current_view() == 'one'


def test_choosing_a_view_keeps_it_with_the_desk_and_tells_the_host():
    state, lists = _desk(2)
    pane = _pane(state, lists)
    told = []
    pane.view_changed.connect(lambda: told.append(state.image_view))
    pane.view_buttons['one'].click()
    assert state.image_view == 'one' and told == ['one']
    assert pane.main_viewer is not None and pane.stack_scroll_bar() is None
    pane.view_buttons['all'].click()
    assert state.image_view == 'all' and pane.stack_scroll_bar() is not None


# -- the frame: controls above the image, the switcher above the list column ------------

def test_one_at_a_time_puts_the_controls_above_the_image_and_the_thumbnails_beside_it():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=True)
    assert pane.head_cell.findChild(rdi.ImageAdjustButton) is not None
    assert pane.main_cell.findChild(rdi.ZoomableScrollArea) is pane.main_viewer
    assert pane.side_cell.findChild(QListWidget) is pane.strip
    # The list column is the narrow one, as wide as the switcher above it.
    assert pane.side_cell.maximumWidth() == rdi.SIDE_WIDTH == pane.findChild(QWidget, 'rd-view-switcher').maximumWidth()


def test_the_frame_stays_when_the_view_changes():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=False,
                 puzzle_options={'own_lookup': lambda ids: [], 'client_factory': lambda: None})
    assert pane.side_cell.findChild(QListWidget) is pane.strip  # all images: the thumbnails too
    pane.view_buttons['join'].click()
    assert pane.side_cell.findChild(QListWidget) is pane.puzzle_view.side
    assert pane.head_cell.findChild(QPushButton, 'rd-puzzle-new') is pane.puzzle_view.new_button


def test_the_divider_stays_where_it_is_when_the_view_changes():
    """The browse splitter as the app builds it (lists, text, viewer; the text and the viewer
    stretch), the desk's image side placed by the host's own method."""
    import genizah_app
    from PyQt6.QtWidgets import QSplitter
    gui = genizah_app.GenizahGUI
    state, lists = _desk(2)
    splitter = QSplitter()
    panel, text_box, viewer = QWidget(), QWidget(), QWidget()
    text = QTextEdit(text_box)
    for w in (panel, text_box, viewer):
        splitter.addWidget(w)
    splitter.setStretchFactor(0, 0)
    splitter.setStretchFactor(1, 1)
    splitter.setStretchFactor(2, 1)
    panel.setVisible(False)
    _KEEP.append(splitter)
    splitter.resize(1150, 700)  # within any screen: a window is held to the screen's width
    splitter.show()
    APP.processEvents()
    host = SimpleNamespace(browse_splitter=splitter, browse_viewer=viewer, browse_lists_panel=panel,
                           browse_text=text, _browse_rd_image_scroll=None)
    pane = rdi.ReadingDeskImagePane(puzzle_options={'own_lookup': lambda ids: [_own('a'), _own('b')],
                                                    'client_factory': lambda: None})
    gui._browse_rd_place_image_pane(host, pane)
    pane.set_desk(state, lists, document_desk=True)
    APP.processEvents()
    before = splitter.sizes()
    assert before[3] >= 520  # half the room, not the image side's minimum
    for view in ('all', 'join', 'one'):
        pane.view_buttons[view].click()
        APP.processEvents()
        # The window lays the splitter out again (a toolbar or a status line changing size).
        splitter.resize(1151, 700)
        APP.processEvents()
        splitter.resize(1150, 700)
        APP.processEvents()
        assert abs(splitter.sizes()[3] - before[3]) <= 2, (view, before, splitter.sizes())
    # A wider window widens the images too, not only the text.
    splitter.resize(1400, 700)
    APP.processEvents()
    assert splitter.sizes()[3] - before[3] > 80, splitter.sizes()
    splitter.hide()


def test_in_all_images_a_thumbnail_scrolls_to_its_image():
    state, lists = _desk(3)
    pane = _pane(state, lists, document_desk=False)
    pane.resize(700, 600)
    pane.show()
    APP.processEvents()
    bar = pane.stack_scroll_bar()
    assert bar.value() == 0
    last = next(pane.strip.item(i) for i in range(pane.strip.count()) if pane.strip.item(i).data(256) == '990003:1')
    pane.strip.itemClicked.emit(last)
    APP.processEvents()
    assert bar.value() > 0
    # Its thumbnail comes from the stack's own load of the image, not a second download.
    loads = len(FakeLoader.made)
    FakeLoader.made[0].image_loaded.emit(_image())
    assert len(FakeLoader.made) == loads and not pane._thumb_items['990001:0'].icon().isNull()
    pane.hide()


# -- all images ------------------------------------------------------------------

def test_all_images_draws_a_viewer_per_image_with_its_controls():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=False)
    assert len(pane.viewers) == 4
    assert [loader.url for loader in FakeLoader.made] == [
        'https://iiif.nli.org.il/IIIFv21/FL101/full/2000,/0/default.jpg',
        'https://iiif.nli.org.il/IIIFv21/FL102/full/2000,/0/default.jpg',
        'https://iiif.nli.org.il/IIIFv21/FL201/full/2000,/0/default.jpg',
        'https://iiif.nli.org.il/IIIFv21/FL202/full/2000,/0/default.jpg']
    FakeLoader.made[0].image_loaded.emit(_image())
    assert pane.viewers[0]._pixmap is not None and not pane.viewers[0]._pixmap.isNull()


def test_a_fragment_without_images_says_so():
    state, lists = _desk(2)
    lists['990002'] = []
    pane = _pane(state, lists, document_desk=False)
    texts = [w.text() for w in pane.findChildren(QLabel)]
    assert 'No images available' in texts and len(pane.viewers) == 2


# -- one at a time ----------------------------------------------------------------

def test_one_at_a_time_shows_a_strip_of_every_image_and_one_large_image():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=True)
    strip = pane.strip
    keys = [strip.item(i).data(256) for i in range(strip.count())]
    # A heading per fragment (no key), then its images.
    assert keys == [None, '990001:0', '990001:1', None, '990002:0', '990002:1']
    assert pane.shown_key == '990001:0' and pane.viewers == []
    # The large image, then at most three thumbnails at once; the shown one is not fetched twice.
    urls = [loader.url for loader in FakeLoader.made]
    assert urls[0].endswith('FL101/full/2000,/0/default.jpg')
    assert len(urls) == 1 + rdi.THUMB_LOADS_AT_ONCE and not any('FL101' in u for u in urls[1:])


def test_the_image_on_show_starts_at_the_fragment_the_reader_came_from():
    state, lists = _desk(2)
    assert _pane(state, lists, document_desk=True, current_sys_id='990002').shown_key == '990002:0'


def test_choosing_a_thumbnail_shows_it_and_tells_the_host():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=True)
    chosen = []
    pane.image_chosen.connect(lambda: chosen.append(state.image_shown))
    first_viewer = pane.main_viewer
    item = next(pane.strip.item(i) for i in range(pane.strip.count()) if pane.strip.item(i).data(256) == '990002:1')
    pane.strip.itemClicked.emit(item)
    assert chosen == ['990002:1'] and pane.shown_key == '990002:1'
    assert pane.main_viewer is not first_viewer
    assert FakeLoader.made[-1].url.endswith('FL202/full/2000,/0/default.jpg')
    # The large image's loader it replaced is let go, not left to fill a dead viewer.
    assert FakeLoader.made[0].cancelled


def test_thumbnails_load_three_at_a_time_and_get_their_icons():
    state, lists = _desk(3)
    pane = _pane(state, lists, document_desk=True)
    thumbs = FakeLoader.made[1:]
    assert len(thumbs) == 3
    thumbs[0].image_loaded.emit(_image())
    assert len(FakeLoader.made) == 5  # one finished, the next started
    key = '990001:1'
    assert not pane._thumb_items[key].icon().isNull()


def test_a_redraw_lets_every_loader_go():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=True)
    old = list(FakeLoader.made)
    pane.set_desk(state, lists, document_desk=True)
    assert all(loader.cancelled for loader in old)
    old[0].image_loaded.emit(_image())  # late: reaches nothing (no error, no new pixmap)
    assert pane.main_viewer._pixmap is None


def test_shutdown_lets_every_loader_go():
    state, lists = _desk(2)
    pane = _pane(state, lists, document_desk=False)
    pane.shutdown()
    assert FakeLoader.made and all(loader.cancelled for loader in FakeLoader.made)


def test_an_oversized_image_is_kept_at_a_bounded_size():
    big = _image(rdi.IMAGE_MAX_SIDE * 2, 100)
    bounded = rdi.bounded_image(big)
    assert bounded.width() == rdi.IMAGE_MAX_SIDE
    small = _image()
    assert rdi.bounded_image(small) is small


def test_the_adjustments_reach_the_viewer_and_reset():
    viewer = rdi.ZoomableScrollArea()
    button = rdi.ImageAdjustButton(viewer)
    _KEEP.extend([viewer, button])
    button.sliders['Brightness'].setValue(40)
    button.sliders['Gamma'].setValue(150)
    button.invert.setChecked(True)
    assert (viewer._brightness, viewer._gamma, viewer._invert) == (40, 1.5, True)
    button.reset()
    assert (viewer._brightness, viewer._gamma, viewer._invert) == (0, 1.0, False)
    assert button.sliders['Brightness'].value() == 0


def test_a_long_name_does_not_widen_the_image_side():
    # The owner's rule for image controls: they never take the image's room. A shelfmark or a
    # puzzle name cut in a narrow pane, not the pane pushed wider.
    def row_width(name):
        viewer = rdi.ZoomableScrollArea()
        row = rdi.image_controls(viewer, [QLabel(name)], [QPushButton('Open in Puzzle')])
        _KEEP.extend([viewer, row])
        return row.minimumSizeHint().width()
    assert row_width('T-S ' + 'NS 324.' * 30) == row_width('T-S 1')


def test_up_and_down_beside_the_fragments_name():
    state, lists = _desk(3)
    pane = _pane(state, lists, document_desk=True, current_sys_id='990002')
    moved = []
    pane.move_requested.connect(lambda sid, delta: moved.append((sid, delta)))
    up = pane.head_cell.findChild(QPushButton, 'rd-move-up')
    down = pane.head_cell.findChild(QPushButton, 'rd-move-down')
    assert up.isEnabled() and down.isEnabled()
    up.click()
    down.click()
    assert moved == [('990002', -1), ('990002', 1)]
    # All images: on each fragment's header, the first not up and the last not down.
    pane.view_buttons['all'].click()
    ups = [b for b in pane.findChildren(QPushButton, 'rd-move-up')]
    downs = [b for b in pane.findChildren(QPushButton, 'rd-move-down')]
    assert [b.isEnabled() for b in ups] == [False, True, True]
    assert [b.isEnabled() for b in downs] == [True, True, False]


def test_a_desk_of_one_has_no_up_and_down():
    state, lists = _desk(1)
    pane = _pane(state, lists, document_desk=True)
    assert pane.findChildren(QPushButton, 'rd-move-up') == []


# -- the images of a fragment ---------------------------------------------------------

def test_a_libraries_own_images_are_used_when_they_number_as_nlis():
    meta = {'images_nli': [_nli('1'), _nli('2')], 'images_ext': [{'label': 'a', 'url': 'x'}, {'label': 'b', 'url': 'y'}]}
    assert [i['label'] for i in rdi.fragment_image_list(meta)] == ['a', 'b']
    meta['images_ext'] = [{'label': 'a', 'url': 'x'}]
    assert [i['fl_id'] for i in rdi.fragment_image_list(meta)] == ['1', '2']
    assert rdi.fragment_image_list({'fl_ids': ['FL77']})[0]['url'].endswith('FL77/full/2000,/0/default.jpg')
    assert rdi.fragment_image_list({}) == []


def test_unlabelled_images_are_recto_and_verso_or_numbered():
    state, lists = _desk(1)
    lists['990001'] = [_nli('1'), _nli('2')]
    assert [i['label'] for i in rdi.desk_images(state.entries, lists)] == ['Recto', 'Verso']
    lists['990001'] = [_nli('1'), _nli('2'), _nli('3')]
    assert [i['label'] for i in rdi.desk_images(state.entries, lists)] == ['1', '2', '3']


# -- puzzle images ---------------------------------------------------------------

def _own(join_id, title=''):
    return {'kind': 'own', 'id': join_id, 'title': title, 'when': '2026-11-01', 'covers': 2,
            'fragments': [{'sys_id': '990001', 'folio_label': '1r', 'fl_id': 'FL101'},
                          {'sys_id': '990002', 'folio_label': '1r', 'fl_id': 'FL201'}]}


def _composer(missing=0):
    calls = []

    def compose(fragments):
        calls.append([f.sys_id for f in fragments])
        return Image.new('RGBA', (30, 20), (255, 255, 255, 255)), missing
    return compose, calls


@pytest.fixture
def run_now(monkeypatch):
    """Picture threads run where they are started (signals delivered at once)."""
    monkeypatch.setattr(rdp.PuzzlePictureThread, 'start', lambda self: self.run())


def _puzzles(state, joins, **kw):
    view = rdp.PuzzleImagesView(state, known=joins, **kw)
    view.resize(500, 700)
    _KEEP.append(view)
    return view


def test_several_puzzles_are_shown_one_under_another(run_now):
    state, _ = _desk(2)
    compose, calls = _composer()
    view = _puzzles(state, [_own('a', 'recto'), _own('b', 'verso')], compose=compose)
    assert list(view.viewers) == ['a', 'b'] and calls == [['990001', '990002']] * 2
    assert all(not v._pixmap.isNull() for v in view.viewers.values())
    # Each in the list column, ticked, with its picture.
    items = [view.side.item(i) for i in range(view.side.count())]
    assert [i.data(Qt.ItemDataRole.UserRole) for i in items] == ['a', 'b']
    assert all(i.checkState() == Qt.CheckState.Checked and not i.icon().isNull() for i in items)


def test_the_chooser_takes_a_puzzle_off_and_keeps_the_choice(run_now):
    state, _ = _desk(2)
    compose, _calls = _composer()
    view = _puzzles(state, [_own('a'), _own('b')], compose=compose)
    view.side.item(1).setCheckState(Qt.CheckState.Unchecked)
    assert state.puzzles_shown == 'a' and list(view.viewers) == ['a']
    view.side.item(0).setCheckState(Qt.CheckState.Unchecked)
    assert state.puzzles_shown == '-' and view.viewers == {}


def test_a_lone_puzzle_cannot_be_taken_off(run_now):
    state, _ = _desk(2)
    view = _puzzles(state, [_own('a')], compose=_composer()[0])
    assert not view.side.item(0).flags() & Qt.ItemFlag.ItemIsUserCheckable


def test_a_missing_fragment_image_is_said(run_now):
    state, _ = _desk(2)
    compose, _calls = _composer(missing=1)
    view = _puzzles(state, [_own('a')], compose=compose)
    note = view.notes['a']
    assert not note.isHidden() and '1 of 2' in note.text()


def test_pictures_are_prepared_one_at_a_time():
    state, _ = _desk(2)
    compose, calls = _composer()
    view = _puzzles(state, [_own('a'), _own('b')], compose=compose)  # threads not started
    assert len(view._workers) == 1 and len(view._pictures) == 1 and calls == []


def test_a_published_puzzles_picture_is_downloaded():
    state, _ = _desk(2)
    join = {'kind': 'published', 'id': 'p', 'title': '', 'when': '', 'covers': 2, 'author': 'A',
            'image_url': 'https://example.org/p.png', 'revision': 'r1'}
    view = _puzzles(state, [join])
    assert FakeLoader.made[-1].url == 'https://example.org/p.png'
    FakeLoader.made[-1].image_loaded.emit(_image())
    assert not view.viewers['p']._pixmap.isNull()


def test_a_puzzle_published_again_is_fetched_again():
    # Same id, date and address; only the arrangement (its revision) tells the pictures apart.
    state, _ = _desk(2)
    join = {'kind': 'published', 'id': 'p', 'title': '', 'when': '2026-01-01', 'covers': 2, 'author': 'A',
            'image_url': 'https://example.org/p.png', 'revision': 'r1'}
    _puzzles(state, [join])
    FakeLoader.made[-1].image_loaded.emit(_image())
    loads = len(FakeLoader.made)
    _puzzles(state, [join])  # the same: kept for the session
    assert len(FakeLoader.made) == loads
    _puzzles(state, [dict(join, revision='r2')])  # published again
    assert len(FakeLoader.made) == loads + 1
    # The download never goes through the image loader's disk cache, kept by address.
    assert rdp.PublishedPictureThread is FakeLoader and 'ImageLoaderThread' not in vars(rdp)


def test_open_in_puzzle_and_new_puzzle_ask_the_host(run_now):
    state, _ = _desk(2)
    compose, _calls = _composer()
    view = _puzzles(state, [_own('a')], compose=compose)
    opened, arranged = [], []
    view.open_requested.connect(opened.append)
    view.arrange_requested.connect(lambda: arranged.append(True))
    view.findChild(QPushButton, 'rd-puzzle-open').click()
    view.new_button.click()
    assert [j['id'] for j in opened] == ['a'] and arranged == [True]


def test_no_puzzle_offers_to_arrange_one():
    state, _ = _desk(2)
    view = _puzzles(state, [])
    assert view.new_button.isHidden()  # nothing to add another puzzle to
    arranged = []
    view.arrange_requested.connect(lambda: arranged.append(True))
    view.findChild(QPushButton, 'rd-puzzle-arrange').click()
    assert arranged == [True]


def test_the_lookup_reads_own_puzzles_on_the_ui_thread_and_ranks_them():
    state, _ = _desk(2)
    seen = []
    view = rdp.PuzzleImagesView(state, own_lookup=lambda ids: seen.append(ids) or [_own('a')],
                                client_factory=lambda: None, compose=_composer()[0])
    _KEEP.append(view)
    assert seen == []  # nothing is read before the host has connected
    view.begin()
    worker = view._workers[0]
    assert seen == [['990001', '990002']] and worker.own[0]['id'] == 'a'
    found = []
    view.found.connect(found.append)
    worker.run()  # here, not on a thread: the snapshot and no client
    assert [j['id'] for j in found[0]] == ['a']
    assert list(view.viewers) == ['a']


def test_the_pane_reuses_the_puzzles_found_until_the_view_is_chosen_again(monkeypatch):
    state, lists = _desk(2)
    lookups = []
    pane = _pane(state, lists, puzzle_options={'own_lookup': lambda ids: lookups.append(ids) or [],
                                               'client_factory': lambda: None})
    monkeypatch.setattr(rdp.PuzzleLookupThread, 'start', lambda self: self.run())
    pane.view_buttons['join'].click()
    assert len(lookups) == 1
    pane.set_desk(state, lists)  # an image list arrived: no new lookup
    assert len(lookups) == 1
    pane.view_buttons['all'].click()
    pane.view_buttons['join'].click()
    assert len(lookups) == 2


# -- the host: the text follows the image on show, the scroll sync only in all images --

def _host(state, lists, view):
    import genizah_app
    gui = genizah_app.GenizahGUI
    state.image_view = view
    host = SimpleNamespace(
        browse_reading_desk_state=state, browse_reading_desk_active=True, current_browse_sid='',
        browse_rd_count_label=QLabel(), browse_text=QTextEdit(), _browse_rd_document=None,
        _rd_text_sync_handler=None, _rd_image_sync_handler=None, _rd_sync_image_bar=None,
        _browse_rd_syncing=False, _browse_rd_image_scroll=None,
    )
    host._browse_rd_image_lists = lambda: lists
    host._browse_rd_is_document = lambda: False
    host.browse_version_combo = QComboBox()
    host._browse_rd_saved_versions = None
    host.renders = []
    # The host's own methods, as GenizahGUI has them (a full render only recorded).
    for name in ('_browse_rd_shown_image', '_browse_rd_disconnect_sync', '_browse_rd_entry_text',
                 '_browse_rd_source_options', '_browse_rd_sync_version_combo', '_browse_rd_choose_text',
                 '_browse_rd_render_text', '_browse_change_version', '_browse_rd_version_data'):
        setattr(host, name, (lambda n: lambda *a: getattr(gui, n)(host, *a))(name))
    host._browse_rd_source_key = gui._browse_rd_source_key
    host._browse_rd_render = lambda: (host.renders.append(True), gui._browse_rd_render_text(host))
    host.browse_version_combo.currentIndexChanged.connect(host._browse_change_version)
    _KEEP.append(host)
    return gui, host


def _with_sources(state):
    """Fragment 2 has an edition and an English translation."""
    state.entries[1].sources = [
        {'id': 51, 'doc_relation': 'Edition', 'source_scholar': 'A. Editor', 'content': 'edition text 2'},
        {'id': 52, 'doc_relation': 'Translation', 'language': 'English', 'source_scholar': 'T. Ranslator',
         'content': 'translation text 2'},
    ]
    return state


def test_with_one_image_on_show_the_text_is_its_fragments_page_alone():
    state, lists = _desk(2)
    state.image_shown = '990002:1'
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    text = host.browse_text.toPlainText()
    assert 'text 2 verso' in text
    assert 'text 2 recto' not in text and 'text 1' not in text and 'T-S 1' not in text


def test_the_fragment_line_is_in_the_interface_language(monkeypatch):
    import genizah_app
    from shared.genizah_translations import TRANSLATIONS
    monkeypatch.setattr(genizah_app, 'tr', lambda s: TRANSLATIONS.get(s, s))
    state, lists = _desk(2)
    _with_sources(state)
    gui, host = _host(state, lists, 'all')
    host.current_browse_sid = state.entries[1].sys_id  # the fragment Browse is on: its badge
    gui._browse_rd_render_text(host)
    text = host.browse_text.toPlainText()
    assert TRANSLATIONS['Current'] in text and '_html_mod' not in text
    assert '[' + TRANSLATIONS['Change version'] + ']' in text and '[' + TRANSLATIONS['Remove'] + ']' in text
    assert '[change version]' not in text and '[remove]' not in text
    assert TRANSLATIONS['Reading Desk'] in text and '2 ' + TRANSLATIONS['fragments'] in text


def test_with_one_image_on_show_the_text_has_no_fragment_line():
    # Its name and remove are over the image; its version is the chooser above the text.
    state, lists = _desk(2)
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    assert '[Change version]' not in host.browse_text.toPlainText() and '[Remove]' not in host.browse_text.toPlainText()
    gui, host = _host(_with_sources(_desk(2)[0]), lists, 'all')
    gui._browse_rd_render_text(host)
    assert '[Change version]' in host.browse_text.toPlainText() and '[Remove]' in host.browse_text.toPlainText()


def test_the_version_chooser_holds_the_shown_fragments_texts():
    state, lists = _desk(2)
    _with_sources(state)
    state.image_shown = '990002:0'
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    combo = host.browse_version_combo
    labels = [combo.itemText(i) for i in range(combo.count())]
    assert labels == ['Edition: A. Editor', 'Translation (English): T. Ranslator', 'V0.8 (HTR)']
    assert combo.isEnabled() and combo.currentIndex() == 0
    assert 'edition text 2' in host.browse_text.toPlainText()
    combo.setCurrentIndex(1)  # the reader chooses the translation
    text = host.browse_text.toPlainText()
    assert 'translation text 2' in text and 'edition text 2' not in text
    assert state.entries[1].chosen == 'pgp:52'
    combo.setCurrentIndex(2)  # and the automatic transcription, its sources kept
    assert 'text 2 recto' in host.browse_text.toPlainText() and len(state.entries[1].sources) == 2


def test_the_cite_bar_credits_the_text_chosen_on_the_desk():
    # The desk's chooser items speak the browse chooser's vocabulary: an edition chosen there
    # is credited to its editor, not to the automatic transcription (MiDRASH).
    state, lists = _desk(2)
    _with_sources(state)
    state.image_shown = '990002:0'
    gui, host = _host(state, lists, 'one')
    host._browse_pgp_url = None
    gui._browse_rd_render_text(host)
    combo = host.browse_version_combo
    info = gui._credit_version_info(host, combo.currentData())
    assert info['source'] == 'pgp' and info['attribution'] == 'A. Editor'
    combo.setCurrentIndex(1)
    assert gui._credit_version_info(host, combo.currentData())['source'] == 'translation'
    combo.setCurrentIndex(2)
    assert gui._credit_version_info(host, combo.currentData()) is None  # V0.8: MiDRASH


def test_in_all_images_the_version_chooser_is_off_and_released_on_leaving():
    state, lists = _desk(2)
    _with_sources(state)
    state.image_shown = '990002:0'
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    combo = host.browse_version_combo
    assert combo.count() == 3
    state.image_view = 'all'
    gui._browse_rd_render_text(host)
    assert not combo.isEnabled() and combo.count() == 1
    # Leaving: none of the desk's entries stay; the page then shown fills it again.
    gui._browse_rd_release_version_combo(host)
    assert [combo.itemData(i) for i in range(combo.count())] == [{'source': 'original'}]


def test_the_browsed_pages_late_updates_leave_the_desk_alone():
    # A manuscript browsed while the desk is open (or its sources arriving late) must not
    # replace the desk's chooser or its text.
    state, lists = _desk(2)
    _with_sources(state)
    state.image_shown = '990002:0'
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    combo = host.browse_version_combo
    before = ([combo.itemText(i) for i in range(combo.count())], host.browse_text.toPlainText())
    host.current_browse_sid = '990001'
    host.corrections_client = object()  # never reached
    gui._check_document_community_status(host)
    host._update_browse_pgp_button = lambda doc: None
    host._build_pgp_extended_info_html = lambda doc, sys_id=None: ''
    host._update_joins_dropdown = lambda: None
    host.txt_b_extended_info = QTextEdit()
    host.btn_b_ext_info = QPushButton()
    host._browse_enriched_html = ''
    gui._on_browse_pgp_loaded(host, '990001', [{'id': 9, 'doc_relation': 'Edition', 'content': 'other page'}], {})
    assert ([combo.itemText(i) for i in range(combo.count())], host.browse_text.toPlainText()) == before


def test_a_later_puzzle_request_lets_a_pending_one_go():
    from PyQt6.QtCore import QObject

    class Fetch(QThread):
        fetched = pyqtSignal(object)

        def __init__(self):
            super().__init__()
            self.cancelled = False

        def cancel(self):
            self.cancelled = True

    state, lists = _desk(2)
    gui, host = _host(state, lists, 'one')
    pending = Fetch()
    _KEEP.append(pending)
    opened = []
    host._browse_rd_fork_worker = pending
    host._puzzle_window = SimpleNamespace(_load_document=opened.append, start_new_with=lambda f: opened.append(f))
    host._open_puzzle_window = lambda: None
    gui._browse_rd_open_puzzle(host, {'kind': 'own', 'id': 'b'})
    assert pending.cancelled and host._browse_rd_fork_worker is None and opened == ['b']
    again = Fetch()
    _KEEP.append(again)
    host._browse_rd_fork_worker = again
    gui._browse_rd_arrange_in_puzzle(host)
    assert again.cancelled and host._browse_rd_fork_worker is None
    assert QObject  # (imported for the signal class above)


@pytest.mark.parametrize('saved', [False, True])
def test_a_published_puzzle_opens_only_once_its_copy_is_saved(monkeypatch, saved):
    import genizah_app
    import shared.puzzle_service as ps
    saves, warned, opened = [], [], []
    monkeypatch.setattr(ps, 'get_puzzle_service', lambda *a, **k: SimpleNamespace(
        save_document=lambda doc: (saves.append(doc.id), doc.id if saved else None)[1]))
    monkeypatch.setattr(genizah_app.QMessageBox, 'warning', lambda *a: warned.append(a[-1]))
    state, lists = _desk(2)
    gui, host = _host(state, lists, 'one')
    host._browse_rd_fork_worker = None
    host._puzzle_window = SimpleNamespace(_load_document=opened.append)
    host._open_puzzle_window = lambda: opened.append('window')
    detail = {'id': 'pub-1', 'title': 'A join', 'fragments_json': {'fragments': [
        {'sys_id': '990001', 'folio_label': '1r', 'fl_id': 'FL1', 'shelfmark': 'T-S 1'}]}}
    gui._browse_rd_open_published_puzzle(host, detail)
    assert len(saves) == 1 and saves[0] != 'pub-1'  # a copy, under an id of its own
    if saved:
        assert warned == [] and opened == ['window', saves[0]]
    else:
        # The copy could not be saved: said so, and no Puzzle opened on a copy that is not there.
        assert len(warned) == 1 and opened == []


def test_change_version_keeps_the_sources_and_shows_a_translation_beside_an_edition(monkeypatch):
    state, lists = _desk(2)
    _with_sources(state)
    gui, host = _host(state, lists, 'all')
    answers = iter([('Translation (English): T. Ranslator', True), ('V0.8 (HTR)', True),
                    ('Edition: A. Editor', True)])
    monkeypatch.setattr(QInputDialog, 'getItem', staticmethod(lambda *a, **k: next(answers)))
    gui._browse_rd_show_version_dialog(host, '990002', 1)
    assert 'translation text 2' in host.browse_text.toPlainText()
    gui._browse_rd_show_version_dialog(host, '990002', 1)
    assert 'text 2 recto' in host.browse_text.toPlainText() and len(state.entries[1].sources) == 2
    gui._browse_rd_show_version_dialog(host, '990002', 1)
    assert 'edition text 2' in host.browse_text.toPlainText()


def test_moving_a_fragment_reorders_the_desk_and_keeps_the_image_on_show():
    state, lists = _desk(3)
    gui, host = _host(state, lists, 'one')
    state.image_shown = ''  # none chosen: the first image is on show, found by position
    gui._browse_rd_move_entry(host, '990001', 1)
    assert [e.sys_id for e in state.entries] == ['990002', '990001', '990003']
    assert [e.sequence_order for e in state.entries] == [1, 2, 3]  # what adding sorts by
    assert state.image_shown == '990001:0' and host.renders == [True]
    gui._browse_rd_move_entry(host, '990002', -1)  # the first cannot go up
    assert [e.sys_id for e in state.entries] == ['990002', '990001', '990003'] and host.renders == [True]


def test_one_image_on_show_shows_its_page_alone_even_without_a_matching_id():
    # Real data: the images' FL ids are never the text pages' (their own files'); the page goes
    # with its image by number, as in Browse -- not every page of the fragment together.
    state, lists = _desk(2)
    lists['990002'] = [{'label': 'a', 'url': 'https://images.lib.cam.ac.uk/x.jpg'},
                       {'label': 'b', 'url': 'https://iiif.nli.org.il/IIIFv21/FL999'}]
    state.image_shown = '990002:1'
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    text = host.browse_text.toPlainText()
    assert 'text 2 verso' in text and 'text 2 recto' not in text and 'text 1' not in text


def test_an_image_whose_page_has_no_text_shows_none():
    state, lists = _desk(2)
    lists['990002'] = lists['990002'] + [{'label': 'cover', 'url': 'https://iiif.nli.org.il/IIIFv21/FL777'}]
    state.image_shown = '990002:2'
    gui, host = _host(state, lists, 'one')
    gui._browse_rd_render_text(host)
    text = host.browse_text.toPlainText()
    assert 'No text available' in text and 'text 2 recto' not in text and 'text 2 verso' not in text


def test_with_all_images_every_fragments_text_is_shown():
    state, lists = _desk(2)
    gui, host = _host(state, lists, 'all')
    gui._browse_rd_render_text(host)
    text = host.browse_text.toPlainText()
    assert all(f'text {n} {side}' in text for n in (1, 2) for side in ('recto', 'verso'))


def test_the_scroll_sync_binds_to_the_stack_only():
    state, lists = _desk(2)
    gui, host = _host(state, lists, 'all')
    pane = _pane(state, lists)
    host._browse_rd_image_scroll = pane
    gui._browse_rd_setup_sync_scroll(host)
    assert host._rd_sync_image_bar is pane.stack_scroll_bar() and host._rd_image_sync_handler is not None
    pane.view_buttons['one'].click()
    # The delayed set-up after a view change finds no stack and binds nothing.
    gui._browse_rd_setup_sync_scroll(host)
    assert host._rd_image_sync_handler is None and host._rd_sync_image_bar is None

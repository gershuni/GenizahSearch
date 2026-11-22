# -*- coding: utf-8 -*-
"""Render-smoke: the Reading Desk shows a PGP document's edition once, for the whole document.

Before: the desk asked for sources fragment by fragment, so a document on two fragments had
its text under each of them, a fragment shared with another document could show THAT
document's edition, and each text was cut at its "recto"/"verso" headings and dealt out to
every fragment's pages. Now a desk opened for a document shows the document's own sources in
one panel (web/pages/browse_desk_document.py); a hand-built desk is as it was.

Drives the real ``/browse`` over a stubbed browse service and a real SQLite sidecar
(tests/test_reading_desk_document.py), entering the desk through the callback the joins
dialog's "View All Fragments" calls.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    _all_elements, _click, _run_home_smoke, _wait_until,
)
from tests.test_reading_desk_document import (
    DOC, EDITION_ONE, EDITION_TWO, ENGLISH_TRANSLATION, HEBREW_TRANSLATION, OTHER_EDITION,
    SID_A, SID_B, SID_C, sidecar,
)
from web.pages import browse
from web.pages import browse_desk_document as dd
from shared.puzzle_model import PuzzleFragment
from web.pages import browse_desk_images as di
from web.pages import browse_desk_puzzle as dp
from web.services import BrowsePage
from web.translations import using_language

SHELFMARKS = {SID_A: 'Frag A', SID_B: 'Frag B', SID_C: 'Frag C'}
FRAGMENTS = [{'shelfmark': 'Frag A', 'document_id': SID_A}, {'shelfmark': 'Frag B', 'document_id': SID_B}]


def _page(sys_id, p_num=1):
    return BrowsePage(uid=f'{sys_id}-{p_num}', p_num=p_num, text=f'automatic {SHELFMARKS[sys_id]} p{p_num}',
                      full_header='V0.8', total_pages=2, current_idx=p_num, sys_id=sys_id,
                      shelfmark=SHELFMARKS[sys_id], library_code='CUL')


def _service():
    svc = MagicMock()
    svc.is_ready.return_value = True
    svc.get_browse_page.side_effect = lambda sys_id, p_num=None, **_kw: _page(sys_id, p_num or 1)
    svc.get_browse_page_by_fl.return_value = None
    svc.get_full_manuscript.side_effect = lambda sys_id: [_page(sys_id, 1), _page(sys_id, 2)]
    found = MagicMock(sys_id=SID_C, shelfmark='Frag C')
    svc.search_by_shelfmark.return_value = ([found], True)
    return svc


def _run(tmp_path, driver, lang='en', saved=None, orders=None):
    """``driver(user, enter)``: ``enter(fragments, pgpid)`` opens the desk as the joins dialog does.
    ``saved``: a list that receives the reader's choices each time the desk's snapshot is saved;
    ``orders``: one that receives the desk's fragments (their sys_ids, in order) each time."""
    made = []
    real_persist = browse.persist_browse_snapshot

    def persist(state, page=None):
        if saved is not None:
            saved.append(dict(state.reading_desk_selected_sources))
        if orders is not None:
            orders.append([e.get('sys_id') for e in state.reading_desk_entries])
        return real_persist(state, page)

    class Refs(browse.BrowsePageRefs):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            made.append(self)

    async def run(user):
        await user.open(f'/browse?sys_id={SID_A}')
        await _wait_until(lambda: made and made[-1].enter_joined_view is not None
                          and any(isinstance(e, ui.number) for e in _all_elements(user)))

        def enter(fragments, pgpid):
            with user._client:
                made[-1].enter_joined_view(fragments, pgpid)

        await driver(user, enter)

    crossref = MagicMock()
    crossref.is_available.return_value = False
    with sidecar(tmp_path), \
            patch.object(browse, 'BrowsePageRefs', Refs), \
            patch.object(browse, 'persist_browse_snapshot', persist), \
            patch.object(browse, 'get_service', return_value=_service()), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False):
        _run_home_smoke(run, lang=lang)


def _labels(user):
    return [e.text for e in _all_elements(user) if isinstance(e, ui.label)]


def _of(user, kind, marker):
    return [e for e in _all_elements(user) if isinstance(e, kind) and marker in e.classes]


def _view_toggle(user):
    [toggle] = _of(user, ui.toggle, 'rd-document-view')
    return toggle


def _set(user, element, value):
    with user._client:
        element.value = value


def _fragment_selects(user):
    """The per-fragment version selectors of the desk as it was."""
    return [e for e in _all_elements(user) if isinstance(e, ui.select) and 'v08' in (e.options or {})]


def test_a_document_on_two_fragments_shows_its_edition_once(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _of(user, ui.toggle, 'rd-document-view'))
        labels = _labels(user)
        assert labels.count(EDITION_ONE) == 1, labels  # once, whole, its headings and niqqud as stored
        assert OTHER_EDITION not in ' '.join(labels)  # fragment B's other document stays out
        assert not any(text.startswith('automatic ') for text in labels)
        assert f'Document #{DOC}' in labels
        assert not _fragment_selects(user)

        assert _view_toggle(user).options == {
            dd.VIEW_EDITION: 'Edition', dd.VIEW_TRANSLATION: 'Translation', dd.VIEW_FRAGMENTS: 'By fragment'}
        [editions] = _of(user, ui.select, 'rd-document-source')
        assert editions.options == {'1': 'Scholar One, Edition (1976).', '2': 'Scholar Two, unpublished editions.'}
        assert editions.value == '1'  # the selector is the attribution; it is not said twice
        assert not _of(user, ui.row, 'rd-document-attribution')
        [text] = _of(user, ui.label, 'rd-document-text')
        assert text.style.get('direction') == 'rtl'
        [panels] = [e for e in _all_elements(user) if 'reading-desk-panels' in e.classes]
        assert 'rd-document-mode' in panels.classes  # the class the narrow-screen stacking hangs on

    _run(tmp_path, driver)


def test_the_reader_picks_another_edition_or_a_translation(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _of(user, ui.select, 'rd-document-source'))
        _set(user, _of(user, ui.select, 'rd-document-source')[0], '2')
        await _wait_until(lambda: EDITION_TWO in _labels(user))
        assert EDITION_TWO in _labels(user) and EDITION_ONE not in _labels(user)

        _set(user, _view_toggle(user), dd.VIEW_TRANSLATION)
        await _wait_until(lambda: HEBREW_TRANSLATION in _labels(user))
        labels = _labels(user)
        assert HEBREW_TRANSLATION in labels and EDITION_TWO not in labels
        [translations] = _of(user, ui.select, 'rd-document-source')
        assert translations.options == {'3': 'Hebrew: Scholar One, Edition (1976).',
                                        '4': 'English: Scholar Three (2005).'}
        _set(user, translations, '4')
        await _wait_until(lambda: ENGLISH_TRANSLATION in _labels(user))
        [text] = _of(user, ui.label, 'rd-document-text')
        assert text.text == ENGLISH_TRANSLATION and text.style.get('direction') == 'ltr'

        # Back to the edition: the one the reader had chosen.
        _set(user, _view_toggle(user), dd.VIEW_EDITION)
        await _wait_until(lambda: EDITION_TWO in _labels(user))
        assert EDITION_TWO in _labels(user)

    saved = []
    _run(tmp_path, driver, saved=saved)
    # Every choice was saved as it was made (a reload or a change of language restores the
    # snapshot; saved only on entering the desk, it came back to the first edition).
    assert {dd.KEY_EDITION: '2'} in saved
    assert saved[-1] == {dd.KEY_EDITION: '2', dd.KEY_VIEW: dd.VIEW_EDITION, dd.KEY_TRANSLATION: '4'}


def test_by_fragment_brings_back_one_text_per_fragment_and_returns(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _of(user, ui.toggle, 'rd-document-view'))
        _set(user, _view_toggle(user), dd.VIEW_FRAGMENTS)
        await _wait_until(lambda: 'automatic Frag A p1' in _labels(user))
        labels = _labels(user)
        # One image is on show (Frag A's recto): the text is that image's alone -- its
        # fragment, its side -- opening on the automatic text, not the document's edition
        # again, with its version chooser.
        assert [t for t in labels if t.startswith('automatic ')] == ['automatic Frag A p1'], labels
        assert EDITION_ONE not in ' '.join(labels) and OTHER_EDITION not in ' '.join(labels)
        [chooser] = _fragment_selects(user)
        assert chooser.value == 'v08' and len(chooser.options) == 5  # the document's four sources stay on offer
        assert not _of(user, ui.label, 'rd-document-text')

        # Another fragment's image: the text follows it.
        thumbs = _thumbs(user)
        _click(user, thumbs[2])
        await _wait_until(lambda: 'automatic Frag B p1' in _labels(user))
        labels = _labels(user)
        assert [t for t in labels if t.startswith('automatic ')] == ['automatic Frag B p1'], labels
        [chooser] = _fragment_selects(user)
        assert chooser.value == 'v08'
        assert f'{SID_B}?page=0' in _large_images(user)[0]
        # The text side alone was redrawn: the filmstrip is the one the reader clicked in
        # (a full redraw of the desk threw the page to its top).
        assert _thumbs(user)[2] is thumbs[2]
        # The other side of the same fragment: its text, not both sides together.
        _click(user, _thumbs(user)[3])
        await _wait_until(lambda: 'automatic Frag B p2' in _labels(user))
        assert [t for t in _labels(user) if t.startswith('automatic ')] == ['automatic Frag B p2']
        assert f'{SID_B}?page=1' in _large_images(user)[0]

        # A version the reader chose for this fragment stays chosen when the text is redrawn
        # for another image of it (the selector fell back to its default on every click).
        [chooser] = _fragment_selects(user)
        pgp_key = next(k for k, label in chooser.options.items() if label.startswith('PGP Edition: Scholar Two'))
        _set(user, chooser, pgp_key)
        await _wait_until(lambda: EDITION_TWO in _labels(user))
        _click(user, _thumbs(user)[2])
        await _wait_until(lambda: f'{SID_B}?page=0' in _large_images(user)[0])
        [chooser] = _fragment_selects(user)
        assert chooser.value == pgp_key and EDITION_TWO in _labels(user)
        assert saved[-1][SID_B] == pgp_key and saved[-1][di.KEY_IMAGE_SHOWN] == f'{SID_B}:0'
        assert _view_toggle(user).value == dd.VIEW_FRAGMENTS  # the way back stays on screen

        _set(user, _view_toggle(user), dd.VIEW_EDITION)
        await _wait_until(lambda: _of(user, ui.label, 'rd-document-text'))
        assert _labels(user).count(EDITION_ONE) == 1 and not _fragment_selects(user)

    saved = []
    _run(tmp_path, driver, saved=saved)


def test_removing_a_fragment_keeps_the_documents_text_and_says_so(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _of(user, ui.toggle, 'rd-document-view'))
        assert not _of(user, ui.label, 'rd-document-partial')
        remove = [b for b in _all_elements(user) if isinstance(b, ui.button) and b.props.get('icon') == 'close'
                  and 'hover:text-red-500' in b.classes]
        _click(user, remove[-1])  # fragment B
        await _wait_until(lambda: _of(user, ui.label, 'rd-document-partial'))
        [note] = _of(user, ui.label, 'rd-document-partial')
        assert note.text == "1 of this document's 2 fragments are on the desk"
        assert _labels(user).count(EDITION_ONE) == 1 and f'Document #{DOC}' in _labels(user)

    _run(tmp_path, driver)


def test_a_fragment_from_elsewhere_makes_it_a_hand_built_desk_again(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _of(user, ui.toggle, 'rd-document-view'))
        [box] = [e for e in _all_elements(user) if isinstance(e, ui.input)
                 and e.props.get('placeholder') == 'Add by shelfmark...']
        _set(user, box, 'Frag C')
        [add] = [b for b in _all_elements(user) if isinstance(b, ui.button) and b.text == 'Add']
        _click(user, add)
        await _wait_until(lambda: 'Frag C' in _labels(user))
        labels = _labels(user)
        # One text per fragment again; Frag C has no PGP source, so no selector of its own.
        assert 'Frag C' in labels and len(_fragment_selects(user)) == 2, labels
        assert not _of(user, ui.toggle, 'rd-document-view')
        assert 'Reading Desk' in labels and f'Document #{DOC}' not in labels

        _click(user, [b for b in _all_elements(user) if isinstance(b, ui.button)
                      and b.props.get('icon') == 'close' and 'hover:text-red-500' in b.classes][-1])
        await _wait_until(lambda: _of(user, ui.toggle, 'rd-document-view'))
        assert _labels(user).count(EDITION_ONE) == 1 and f'Document #{DOC}' in _labels(user)

    _run(tmp_path, driver)


def _large_images(user):
    return [e.content for e in _all_elements(user) if isinstance(e, ui.html) and 'rd-zoomable' in e.content]


def _thumbs(user):
    return [e for e in _all_elements(user) if 'rd-thumb' in e.classes]


def _image_toggle(user):
    [toggle] = _of(user, ui.toggle, 'rd-image-view')
    return toggle


def test_a_document_desk_shows_one_image_beside_a_filmstrip(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        assert _image_toggle(user).value == di.VIEW_ONE
        assert _image_toggle(user).options == {
            di.VIEW_ONE: 'One at a time', di.VIEW_ALL: 'All images', di.VIEW_JOIN: 'Puzzle images'}
        thumbs = _thumbs(user)
        assert [t.props['aria-label'] for t in thumbs] == [
            'Frag A Recto', 'Frag A Verso', 'Frag B Recto', 'Frag B Verso']
        [large] = _large_images(user)
        assert f'{SID_A}?page=0' in large and 'rd-thumb-on' in thumbs[0].classes
        assert len(_of(user, ui.button, 'rd-image-adjust')) == 1  # one toolbar, not one per image

        _click(user, thumbs[3])
        await _wait_until(lambda: SID_B in _large_images(user)[0])
        [large] = _large_images(user)
        assert f'{SID_B}?page=1' in large and 'id="rd-viewer-1-1"' in large
        assert 'rd-thumb-on' in thumbs[3].classes and 'rd-thumb-on' not in thumbs[0].classes
        assert _labels(user).count(EDITION_ONE) == 1  # the text side did not move

    _run(tmp_path, driver)


def test_all_images_is_the_stack_and_the_choice_is_kept(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        _set(user, _image_toggle(user), di.VIEW_ALL)
        await _wait_until(lambda: len(_large_images(user)) == 4)
        images = _large_images(user)
        assert len(images) == 4 and SID_A in images[0] and SID_B in images[-1]
        assert not _thumbs(user)
        headers = [e.props.get('id') for e in _all_elements(user) if str(e.props.get('id', '')).startswith('rd-img-frag-')]
        assert headers == ['rd-img-frag-0', 'rd-img-frag-1']  # what the per-fragment text scrolls in step with
        assert len(_of(user, ui.button, 'rd-image-adjust')) == 4

        # A change on the text side redraws the desk; the image view stays the reader's.
        _set(user, _view_toggle(user), dd.VIEW_FRAGMENTS)
        await _wait_until(lambda: 'automatic Frag B p2' in _labels(user)
                          and _of(user, ui.toggle, 'rd-image-view') and len(_large_images(user)) == 4)
        assert _image_toggle(user).value == di.VIEW_ALL and len(_large_images(user)) == 4

    _run(tmp_path, driver)


def _headers(user):
    """The fragment names over the images, in the order drawn."""
    return [e.text for e in _all_elements(user) if isinstance(e, ui.label) and 'cursor-pointer' in e.classes
            and e.text in SHELFMARKS.values() and any('rd-img-frag-' in str(p.props.get('id', '')) for p in _ancestors(e))]


def _ancestors(element):
    parent = element.parent_slot.parent if element.parent_slot else None
    while parent is not None:
        yield parent
        parent = parent.parent_slot.parent if parent.parent_slot else None


def test_the_reader_moves_a_fragment_up_or_down_the_desk(tmp_path):
    orders = []

    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        # One at a time: the shown image's fragment has its own up and down beside its name.
        assert len(_of(user, ui.button, 'rd-move-up')) == 1
        _set(user, _image_toggle(user), di.VIEW_ALL)
        await _wait_until(lambda: len(_large_images(user)) == 4)
        assert _headers(user) == ['Frag A', 'Frag B']
        ups, downs = _of(user, ui.button, 'rd-move-up'), _of(user, ui.button, 'rd-move-down')
        # The first cannot go up, the last cannot go down.
        assert [b.enabled for b in ups] == [False, True] and [b.enabled for b in downs] == [True, False]

        _click(user, downs[0])
        await _wait_until(lambda: _headers(user) == ['Frag B', 'Frag A'])
        images = _large_images(user)
        assert SID_B in images[0] and SID_A in images[-1]  # the images follow
        assert orders[-1] == [SID_B, SID_A]  # and the order is kept with the desk

        # The text side by fragment follows too, and has the same buttons.
        _set(user, _view_toggle(user), dd.VIEW_FRAGMENTS)
        await _wait_until(lambda: 'automatic Frag B p2' in _labels(user))
        texts = [t for t in _labels(user) if t.startswith('automatic ')]
        assert texts.index('automatic Frag B p1') < texts.index('automatic Frag A p1')
        text_ups = [b for b in _of(user, ui.button, 'rd-move-up')
                    if any('rd-text-frag-' in str(p.props.get('id', '')) for p in _ancestors(b))]
        assert [b.enabled for b in text_ups] == [False, True]
        _click(user, text_ups[1])  # Frag A back up, from the text side
        await _wait_until(lambda: orders[-1] == [SID_A, SID_B])

        # A desk of one has nothing to order.
        _click(user, [b for b in _all_elements(user) if isinstance(b, ui.button) and b.text == 'Back to Page View'][0])
        enter(FRAGMENTS[:1], DOC)
        await _wait_until(lambda: _thumbs(user))
        assert not _of(user, ui.button, 'rd-move-up') and not _of(user, ui.button, 'rd-move-down')

    _run(tmp_path, driver, orders=orders)


def test_the_image_on_show_stays_on_show_when_its_fragment_moves(tmp_path):
    """One at a time, the image shown when the reader chose none is found by position (the first,
    when the page's own fragment is not on the desk): moving its fragment must not show another."""
    async def driver(user, enter):
        enter([{'shelfmark': 'Frag B', 'document_id': SID_B}, {'shelfmark': 'Frag C', 'document_id': SID_C}], None)
        await _wait_until(lambda: len(_large_images(user)) == 4)  # a hand-built desk: all images
        _set(user, _image_toggle(user), di.VIEW_ONE)
        await _wait_until(lambda: _thumbs(user))
        assert f'{SID_B}?page=0' in _large_images(user)[0]
        down = _of(user, ui.button, 'rd-move-down')[0]  # beside the shown image (the text side has one too)
        _click(user, down)
        await _wait_until(lambda: [t.props['aria-label'] for t in _thumbs(user)][:1] == ['Frag C Recto'])
        assert f'{SID_B}?page=0' in _large_images(user)[0]  # Frag B, now second, is still the one shown

    _run(tmp_path, driver)


def test_each_image_has_its_links_menu_filled_when_opened(tmp_path):
    import asyncio

    from web.pages import browse_desk_links as dl
    asked = []
    complete = {'value': True}

    def resolve(entry):
        asked.append(entry['sys_id'])
        return {'fragment': [(('NLI Ktiv',), f"https://ktiv/{entry['sys_id']}")],
                'pages': {1: [(('Bodleian', 'Verso'), 'https://bodleian/verso.jpg')]},
                'complete': complete['value']}

    def links(user):
        return [(e.text, e.props.get('href')) for e in _all_elements(user) if isinstance(e, ui.link) and 'rd-link' in e.classes]

    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        _set(user, _image_toggle(user), di.VIEW_ALL)
        await _wait_until(lambda: len(_large_images(user)) == 4)
        assert len(_of(user, ui.button, 'rd-links')) == 4  # one beside each image
        assert not asked  # nothing is looked up until a menu is opened
        menus = _of(user, ui.menu, 'rd-links-menu')
        _set(user, menus[1], True)  # Frag A's verso
        await _wait_until(lambda: links(user))
        assert asked == [SID_A]
        assert links(user) == [('Bodleian · Verso', 'https://bodleian/verso.jpg'), ('NLI Ktiv', f'https://ktiv/{SID_A}')]
        _set(user, menus[1], False)
        _set(user, menus[1], True)  # opened again: not looked up again
        await asyncio.sleep(0.5)  # time for a second lookup to run, were one started
        assert asked == [SID_A]
        # A result a source left incomplete: the same menu asks again at its next opening.
        complete['value'] = False
        _set(user, menus[2], True)  # Frag B's recto
        await _wait_until(lambda: SID_B in asked)
        _set(user, menus[2], False)
        _set(user, menus[2], True)
        await _wait_until(lambda: asked.count(SID_B) == 2)
        assert asked == [SID_A, SID_B, SID_B]

    with patch.object(dl, '_resolve', side_effect=resolve):
        _run(tmp_path, driver)


def test_every_control_that_redraws_the_desk_lets_go_of_the_focus(tmp_path):
    """A control the redraw destroys must not keep the focus a click gave it, or the browser takes
    the page to its top (2026-11-22): web/static/reading_desk_place.js acts on ``rd-redraws``."""
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        _set(user, _image_toggle(user), di.VIEW_ALL)
        _set(user, _view_toggle(user), dd.VIEW_FRAGMENTS)
        await _wait_until(lambda: len(_large_images(user)) == 4 and 'automatic Frag B p2' in _labels(user))
        assert 'rd-redraws' in _image_toggle(user).classes and 'rd-redraws' in _view_toggle(user).classes
        for marker in ('rd-move-up', 'rd-move-down', 'rd-remove-fragment'):
            buttons = _of(user, ui.button, marker)
            assert buttons and all('rd-redraws' in b.classes for b in buttons), marker
        # The text side's own remove buttons too.
        removes = [b for b in _all_elements(user) if isinstance(b, ui.button) and b.props.get('icon') == 'close'
                   and any('rd-text-frag-' in str(p.props.get('id', '')) for p in _ancestors(b))]
        assert len(removes) == 2 and all('rd-redraws' in b.classes for b in removes)

    _run(tmp_path, driver)


def test_move_entry_moves_one_place_and_never_off_the_desk():
    entries = [{'sys_id': 'a'}, {'sys_id': 'b'}, {'sys_id': 'c'}]
    ids = lambda moved: [e['sys_id'] for e in moved]  # noqa: E731
    assert ids(di.move_entry(entries, 'a', 1)) == ['b', 'a', 'c']
    assert ids(di.move_entry(entries, 'c', -1)) == ['a', 'c', 'b']
    assert ids(entries) == ['a', 'b', 'c']  # a new list; the desk's own is replaced, not changed
    assert di.move_entry(entries, 'a', -1) is None and di.move_entry(entries, 'c', 1) is None
    assert di.move_entry(entries, 'gone', 1) is None


def test_a_leaf_keeps_recto_and_verso_and_a_longer_fragment_is_numbered():
    with using_language('en'):
        two = di.desk_images([{'sys_id': SID_A, 'shelfmark': 'Frag A', 'pages': []}])
        assert [i['label'] for i in two] == ['Recto', 'Verso']  # no pages known: the two sides of one leaf
        pages = [MagicMock(p_num=n) for n in (1, 2, 3, 4)]
        four = di.desk_images([{'sys_id': SID_A, 'shelfmark': 'Frag A', 'pages': pages}])
        assert [i['label'] for i in four] == ['1', '2', '3', '4']
        assert di.thumbnail_url(four[2]) == f'/api/nli_image_by_sysid/{SID_A}?page=2&width=240'
    assert di.shown_image(four, {di.KEY_IMAGE_SHOWN: f'{SID_A}:3'})['page_index'] == 3
    assert di.shown_image(four, {di.KEY_IMAGE_SHOWN: 'gone:0'})['page_index'] == 0  # a removed fragment's image
    # A Bodleian fragment named as PGP names it: told by its library, its image by the Bodleian's name.
    bodleian = di.desk_images([{'sys_id': SID_A, 'shelfmark': 'Bodl. MS heb. f 56/59', 'pages': []}],
                              library_for=lambda sys_id: 'Oxford')
    assert [i['url'] for i in bodleian] == [
        'https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_56_59a.jpg',
        'https://hebrew.bodleian.ox.ac.uk/fragments/full/MS_HEB_f_56_59b.jpg']
    assert di.thumbnail_url(bodleian[0]) == bodleian[0]['url']
    entries = [{'sys_id': SID_A, 'shelfmark': 'Frag A', 'pages': []}, {'sys_id': SID_B, 'shelfmark': 'Frag B', 'pages': []}]
    shown = di.shown_text_image(entries, {di.KEY_IMAGE_SHOWN: f'{SID_B}:1'}, document_text=True)
    assert (shown['frag_idx'], shown['page_index']) == (1, 1)
    assert di.shown_text_image(entries, {}, document_text=True, current_shelfmark='frag b')['frag_idx'] == 1
    assert di.shown_text_image(entries, {}, document_text=False) is None  # all images: every fragment's text
    # The puzzle images are of several fragments: no one image's text. Where they are not on
    # offer (no Puzzle, or one fragment), the image side falls back and the text side with it.
    in_join = {di.KEY_IMAGE_VIEW: di.VIEW_JOIN}
    assert di.shown_text_image(entries, in_join, document_text=True, can_arrange=True) is None
    assert di.shown_text_image(entries, in_join, document_text=True, can_arrange=False)['frag_idx'] == 0
    assert di.shown_text_image(entries[:1], in_join, document_text=True, can_arrange=True)['frag_idx'] == 0
    assert di.image_view({}, document_text=True) == di.VIEW_ONE
    assert di.image_view({}, document_text=False) == di.VIEW_ALL
    assert di.image_view({di.KEY_IMAGE_VIEW: di.VIEW_ONE}, document_text=False) == di.VIEW_ONE


OWN_JOIN = {'kind': 'own', 'id': 'own-1', 'title': 'My join', 'when': '2026-11-18T10:00:00', 'covers': 2,
            'fragments': [{'sys_id': SID_A, 'folio_label': '1r', 'fl_id': 'FL1', 'x': 1.0, 'y': 2.0}]}
PUBLISHED_JOIN = {'kind': 'published', 'id': 'pub-1', 'title': 'A published join', 'when': '2026-11-01',
                  'covers': 2, 'author': 'A Reader', 'image_url': 'https://example.org/joins/pub-1.png'}


def _join_pictures(user):
    return [e.content for e in _all_elements(user)
            if isinstance(e, ui.html) and f'<img id="{dp.VIEWER_ID}-' in e.content]


def test_puzzle_images_show_every_puzzle_one_under_another_and_lead_to_the_puzzle(tmp_path):
    saved = []

    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        _set(user, _image_toggle(user), di.VIEW_JOIN)
        await _wait_until(lambda: _of(user, ui.select, 'rd-join-choice'))
        [choice] = _of(user, ui.select, 'rd-join-choice')
        assert choice.options == {'own-1': 'My join · Your saved join', 'pub-1': 'A published join · Published by A Reader'}
        # Both shown, one under another, each with its own picture, controls and way to the Puzzle.
        assert choice.value == ['own-1', 'pub-1']
        own, published = _join_pictures(user)
        # The visitor's own join has no picture stored: the browser asks the Puzzle's export route.
        assert f'id="{dp.VIEWER_ID}-0"' in own and 'src=' not in own and 'Preparing the puzzle image' in own
        assert f'id="{dp.VIEWER_ID}-1"' in published and 'src="https://example.org/joins/pub-1.png"' in published
        assert len(_of(user, ui.button, 'rd-join-open')) == 2 and len(_of(user, ui.button, 'rd-image-adjust')) == 2
        assert len(_of(user, ui.button, 'rd-join-new')) == 1  # another puzzle: another page, or the other side
        assert _of(user, ui.scroll_area, 'rd-join-stack')
        assert not _thumbs(user) and not _large_images(user)[2:]  # the puzzles, not the fragments' images
        assert '4 images' not in _labels(user)  # the desk's image count is not the puzzles'
        assert _labels(user).count(EDITION_ONE) == 1  # the text side is as it was

        # By fragment, beside the puzzle images: every fragment's text, not one image's.
        _set(user, _view_toggle(user), dd.VIEW_FRAGMENTS)
        await _wait_until(lambda: 'automatic Frag B p2' in _labels(user) and _of(user, ui.select, 'rd-join-choice'))
        assert [t for t in _labels(user) if t.startswith('automatic ')] == [
            'automatic Frag A p1', 'automatic Frag A p2', 'automatic Frag B p1', 'automatic Frag B p2']
        assert len(_join_pictures(user)) == 2

        # One taken off: the other alone, at the full height, and the choice is kept with the desk.
        [choice] = _of(user, ui.select, 'rd-join-choice')
        _set(user, choice, ['pub-1'])
        await _wait_until(lambda: len(_join_pictures(user)) == 1)
        [published] = _join_pictures(user)
        assert f'id="{dp.VIEWER_ID}-1"' in published and 'pub-1.png' in published
        assert not _of(user, ui.scroll_area, 'rd-join-stack')
        assert saved[-1][dp.KEY_JOIN_SHOWN] == 'pub-1'

        # Every one taken off: none shown, and none it stays.
        [choice] = _of(user, ui.select, 'rd-join-choice')
        _set(user, choice, [])
        await _wait_until(lambda: 'Choose the puzzles to show.' in _labels(user))
        assert not _join_pictures(user) and saved[-1][dp.KEY_JOIN_SHOWN] == dp.NONE_SHOWN

    with patch.object(dp, 'find_joins', return_value=[OWN_JOIN, PUBLISHED_JOIN]):
        _run(tmp_path, driver, saved=saved)


def test_puzzle_images_without_a_saved_join_offer_the_puzzle(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, DOC)
        await _wait_until(lambda: _thumbs(user))
        _set(user, _image_toggle(user), di.VIEW_JOIN)
        await _wait_until(lambda: 'No puzzle of these fragments yet.' in _labels(user))
        assert 'No puzzle of these fragments yet.' in _labels(user)
        assert [b.text for b in _all_elements(user) if isinstance(b, ui.button) and b.text == 'Arrange in Puzzle']
        assert not _of(user, ui.button, 'rd-join-open')

        # A desk of one fragment has no puzzle to show: the view is not offered, and a
        # reader who was in it is back with the images.
        _click(user, [b for b in _all_elements(user) if isinstance(b, ui.button) and b.text == 'Back to Page View'][0])
        enter(FRAGMENTS[:1], DOC)
        await _wait_until(lambda: _thumbs(user))
        assert di.VIEW_JOIN not in _image_toggle(user).options

    with patch.object(dp, 'find_joins', return_value=[]):
        _run(tmp_path, driver)


def test_the_puzzles_shown_are_the_readers_choice_else_the_first_few():
    joins = [{'id': f'j{n}'} for n in range(6)]
    ids = lambda found: [j['id'] for j in found]  # noqa: E731
    assert ids(dp.shown_joins(joins, {})) == ['j0', 'j1', 'j2', 'j3']  # SHOWN_FIRST, best first
    assert dp.SHOWN_FIRST == 4
    # The reader's choice, in the order found (not the order ticked); one no longer found is dropped.
    assert ids(dp.shown_joins(joins, {dp.KEY_JOIN_SHOWN: '\n'.join(('j5', 'gone', 'j1'))})) == ['j1', 'j5']
    # A choice naming none still found is no choice; one cleared on purpose shows none.
    assert ids(dp.shown_joins(joins, {dp.KEY_JOIN_SHOWN: 'gone'})) == ['j0', 'j1', 'j2', 'j3']
    assert dp.shown_joins(joins, {dp.KEY_JOIN_SHOWN: dp.NONE_SHOWN}) == []
    # One puzzle alone has no chooser to bring it back with: it is shown whatever was chosen.
    assert ids(dp.shown_joins(joins[:1], {dp.KEY_JOIN_SHOWN: dp.NONE_SHOWN})) == ['j0']
    assert ids(dp.shown_joins(joins[:1], {dp.KEY_JOIN_SHOWN: 'gone'})) == ['j0']
    # The one join an earlier desk kept (a single id) is still read.
    assert ids(dp.shown_joins(joins, {dp.KEY_JOIN_SHOWN: 'j2'})) == ['j2']
    # A document desk on a narrow screen stacks its cards with no fixed height: the stack of
    # puzzles, like the images' pane, is given one there (flex: 1 alone would be no height).
    narrow = dd.DESK_CSS.split('@media (max-width: 900px)')[1]
    assert '.rd-document-mode .rd-join-stack' in narrow and '.rd-document-mode .rd-image-pane' in narrow
    # Each puzzle's picture goes to its own viewer.
    js = dp.own_picture_js(OWN_JOIN, 'failed', 'too many', viewer_id=f'{dp.VIEWER_ID}-3')
    assert f"getElementById('{dp.VIEWER_ID}-3')" in js and f"rdInitDrag('{dp.VIEWER_ID}-3')" in js
    assert f"getElementById('{dp.VIEWER_ID}')" not in js


def test_find_joins_ranks_and_keeps_each_visitor_to_their_own():
    class Own:
        """A visitor's SavedJoins: the only saved joins the desk may read."""
        docs = {
            'both': MagicMock(id='both', title='Both', updated_at='2026-11-10', fragments=[
                PuzzleFragment(sys_id=SID_A, folio_label='1r', fl_id='FL1'),
                PuzzleFragment(sys_id=SID_B, folio_label='1r', fl_id='FL2')]),
            'one': MagicMock(id='one', title='One only', updated_at='2026-11-17', fragments=[
                PuzzleFragment(sys_id=SID_A, folio_label='1r', fl_id='FL1')]),
        }

        def list_documents_for_fragment(self, sys_id=None, fl_id=None):
            return [k for k, d in self.docs.items() if sys_id in {f.sys_id for f in d.fragments}]

        def load_document(self, doc_id):
            return self.docs.get(doc_id)

    published = {SID_A: [{'id': 'pub', 'title': 'Pub', 'created_at': '2026-11-18', 'author_name': 'R'},
                         {'id': 'both', 'title': 'Both, published', 'created_at': '2026-11-18', 'author_name': 'Me'}],
                 SID_B: [{'id': 'pub', 'title': 'Pub', 'created_at': '2026-11-18', 'author_name': 'R'}]}
    with patch('shared.puzzle_publish_service.get_published_joins_for_fragment',
               side_effect=lambda client, sys_id: published.get(sys_id, [])), \
            patch('shared.puzzle_publish_service.get_published_join_detail',
                  side_effect=lambda client, join_id: {'is_published': True, 'image_url': f'https://x/{join_id}.png'}):
        found = dp.find_joins([SID_A, SID_B], Own(), client=object())
        # Both fragments, the visitor's own first; a join of one fragment of two is not the join.
        assert [(j['id'], j['kind']) for j in found] == [('both', 'own'), ('pub', 'published')]
        assert found[0]['fragments'][0]['sys_id'] == SID_A and found[1]['image_url'] == 'https://x/pub.png'
        # On a desk of one fragment a join holding it is enough.
        assert [j['id'] for j in dp.find_joins([SID_A], Own(), client=None)] == ['one', 'both']
        # The published half failing leaves the visitor's own.
        with patch('shared.puzzle_publish_service.get_published_joins_for_fragment', side_effect=RuntimeError('offline')):
            assert [j['id'] for j in dp.find_joins([SID_A, SID_B], Own(), client=object())] == ['both']
    js = dp.own_picture_js(OWN_JOIN, 'failed', 'too many')
    assert "fetch('/api/puzzle_export'" in js and '"sys_id": "' + SID_A in js and dp.VIEWER_ID in js
    # The export route's limit on fragments is said as what it is, not as a failure.
    assert 'r.status===413' in js and '"too many"' in js and js.index('r.status===413') < js.index('!r.ok')


def test_the_viewer_script_listens_on_the_window_once_not_once_per_image():
    """The filmstrip view redraws its image on every thumbnail and initialises dragging on the
    new one. A window listener added per image and never removed piled up (and kept each
    replaced <img> alive): the script's two window listeners sit behind a once-per-page guard."""
    import inspect
    source = inspect.getsource(browse)
    script = source[source.index('window.rdViewers = {};'):source.index('// Init drag for all existing')]
    guard, init = script.index('if (!window.rdDragBound)'), script.index('window.rdInitDrag = function')
    for event in ("addEventListener('mousemove'", "addEventListener('mouseup'"):
        assert script.count(event) == 1 and guard < script.index(event) < init, event
    assert 'addEventListener' not in script[init:]
    # A redraw of the desk makes its images anew under the same ids: the views kept for the
    # old ones are dropped, not carried over (they made a new image jump on its first zoom).
    assert 'window.rdViewers = window.rdViewers ||' not in source


def _reopen_saved_desk(tmp_path, address):
    """Opens ``address`` in a browser whose saved desk is document DOC on its two fragments,
    with the second edition chosen; returns the errors logged while the desk came back."""
    saved = {'entries': [{'sys_id': SID_A, 'shelfmark': 'Frag A'}, {'sys_id': SID_B, 'shelfmark': 'Frag B'}],
             'pgpid': DOC, 'selected_sources': {dd.KEY_EDITION: '2'}}

    async def driver(user):
        await user.open(address)
        await _wait_until(lambda: _of(user, ui.label, 'rd-document-text'))
        assert _labels(user).count(EDITION_TWO) == 1, _labels(user)
        assert f'Document #{DOC}' in _labels(user)
        assert len(_thumbs(user)) == 4

    crossref = MagicMock()
    crossref.is_available.return_value = False
    real_get = browse.safe_user_get
    with sidecar(tmp_path), \
            patch.object(browse, 'restore_browse_snapshot', return_value=(None, saved)), \
            patch.object(browse, 'safe_user_get',
                         side_effect=lambda key, *a: saved if key == 'reading_desk_state' else real_get(key, *a)), \
            patch.object(browse, 'get_service', return_value=_service()), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=False), \
            patch.object(browse.logger, 'error') as errors:
        _run_home_smoke(driver, lang='en')
    return [str(call.args[0]) for call in errors.call_args_list]


def test_browse_without_an_address_reopens_a_saved_desk(tmp_path):
    """/browse with a saved desk and no sys_id was a server error (the desk was drawn before
    any page was loaded); it now loads the desk's first fragment and comes up with the
    reader's edition -- by restoring the desk, not by the restore failing into a page load."""
    errors = _reopen_saved_desk(tmp_path, '/browse')
    assert not [e for e in errors if 'restoring' in e], errors


def test_an_address_of_one_of_its_fragments_reopens_a_saved_desk(tmp_path):
    """The same with an address (a reload, a change of language): the restore used to raise
    on drawing a desk with no page, be logged as an error, and be rescued by the page load."""
    errors = _reopen_saved_desk(tmp_path, f'/browse?sys_id={SID_B}')
    assert not [e for e in errors if 'restoring' in e], errors


def test_a_hand_built_desk_is_as_it_was(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: len(_fragment_selects(user)) == 2)
        assert len(_fragment_selects(user)) == 2
        assert not _of(user, ui.toggle, 'rd-document-view')
        [panels] = [e for e in _all_elements(user) if 'reading-desk-panels' in e.classes]
        assert 'rd-document-mode' not in panels.classes
        assert 'Reading Desk' in _labels(user)
        # Its images are the stack it always was (now with the switcher above it).
        assert _image_toggle(user).value == di.VIEW_ALL and len(_large_images(user)) == 4

    _run(tmp_path, driver)


def test_the_controls_are_in_hebrew(tmp_path):
    async def driver(user, enter):
        enter(FRAGMENTS[:1], DOC)
        await _wait_until(lambda: _of(user, ui.toggle, 'rd-document-view'))
        assert _view_toggle(user).options == {
            dd.VIEW_EDITION: 'מהדורה', dd.VIEW_TRANSLATION: 'תרגום', dd.VIEW_FRAGMENTS: 'לפי קטע'}
        [note] = _of(user, ui.label, 'rd-document-partial')
        assert note.text == 'בשולחן העיון 1 מתוך 2 קטעי המסמך'
        assert _labels(user).count(EDITION_ONE) == 1  # codepoint for codepoint

    _run(tmp_path, driver, lang='he')


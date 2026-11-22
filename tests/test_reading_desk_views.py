# -*- coding: utf-8 -*-
"""The Reading Desk's image views, the rules both apps follow (shared/reading_desk_views.py):
which view the desk is in, when the puzzle images are offered, which image is on show, the
page labels, and which text page belongs to the image on show (GS-294)."""
from shared.reading_desk_views import (
    VIEW_ALL, VIEW_JOIN, VIEW_ONE, image_fl_id, image_key, image_view, is_document_desk, join_is_offered,
    move_entry, page_label, shown_image, text_page_for_image,
)


# -- which view -----------------------------------------------------------------

def test_the_default_is_one_at_a_time_on_a_documents_desk_and_all_images_elsewhere():
    assert image_view(None, document_desk=True) == VIEW_ONE
    assert image_view(None, document_desk=False) == VIEW_ALL
    assert image_view('', document_desk=True, join_offered=True) == VIEW_ONE


def test_the_readers_choice_wins():
    assert image_view(VIEW_ALL, document_desk=True) == VIEW_ALL
    assert image_view(VIEW_ONE, document_desk=False) == VIEW_ONE
    assert image_view(VIEW_JOIN, document_desk=False, join_offered=True) == VIEW_JOIN


def test_the_puzzle_images_are_the_readers_only_while_offered():
    # A desk taken down to one fragment: the puzzles are gone, the default comes back.
    assert image_view(VIEW_JOIN, document_desk=False, join_offered=False) == VIEW_ALL
    assert image_view(VIEW_JOIN, document_desk=True, join_offered=False) == VIEW_ONE
    assert image_view('nonsense', document_desk=False) == VIEW_ALL


def test_the_puzzle_images_are_offered_on_two_fragments_or_more():
    assert not join_is_offered([])
    assert not join_is_offered(['990001'])
    assert not join_is_offered(['990001', '990001', ''])  # one fragment, named twice
    assert join_is_offered(['990001', '990002'])
    assert not join_is_offered(['990001', '990002'], can_arrange=False)


# -- a document's desk ------------------------------------------------------------

DOC = [{'sys_id': '990001'}, {'sys_id': '990002'}, {'sys_id': '990003'}]


def test_a_desk_of_the_documents_own_fragments_is_its_desk():
    assert is_document_desk(712, ['990001', '990002', '990003'], lambda _p: DOC, lambda _p: True)
    # Removing fragments keeps it the document's desk.
    assert is_document_desk(712, ['990002'], lambda _p: DOC, lambda _p: True)


def test_a_fragment_from_elsewhere_ends_it():
    assert not is_document_desk(712, ['990001', '990009'], lambda _p: DOC, lambda _p: True)


def test_no_document_or_no_text_or_no_sidecar_is_no_documents_desk():
    assert not is_document_desk(None, ['990001'], lambda _p: DOC, lambda _p: True)
    assert not is_document_desk(712, [], lambda _p: DOC, lambda _p: True)
    assert not is_document_desk(712, ['990001'], lambda _p: [], lambda _p: True)
    # The web's rule: a document with no edition and no translation has no text to follow.
    assert not is_document_desk(712, ['990001'], lambda _p: DOC, lambda _p: False)

    def broken(_pgpid):
        raise RuntimeError('pgp.db cannot be read')
    assert not is_document_desk(712, ['990001'], broken, lambda _p: True)


# -- the image on show ------------------------------------------------------------

IMAGES = [
    {'key': image_key('990001', 0), 'sys_id': '990001'},
    {'key': image_key('990001', 1), 'sys_id': '990001'},
    {'key': image_key('990002', 0), 'sys_id': '990002'},
]


def test_the_image_on_show_is_the_readers():
    assert shown_image(IMAGES, '990001:1')['key'] == '990001:1'


def test_else_the_first_of_the_fragment_the_reader_came_from_else_the_first():
    assert shown_image(IMAGES, None, current_sys_id='990002')['key'] == '990002:0'
    assert shown_image(IMAGES, 'gone:3', current_sys_id='990002')['key'] == '990002:0'
    assert shown_image(IMAGES, None)['key'] == '990001:0'
    assert shown_image([], '990001:0') is None


# -- labels -------------------------------------------------------------------------

def test_one_leaf_is_recto_and_verso_and_more_are_numbered():
    assert page_label(0, 2) == 'Recto'
    assert page_label(1, 2) == 'Verso'
    assert page_label(0, 1) == 'Recto'
    assert page_label(2, 5) == '3'
    assert page_label(0, 2, tr=lambda s: s.upper()) == 'RECTO'


# -- the text page of the image on show ----------------------------------------------

PAGES = [{'p_num': 1, 'fl_id': 'FL111', 'text': 'a'}, {'p_num': 2, 'fl_id': '222', 'text': 'b'}]


def test_the_page_is_found_by_the_images_nli_id():
    assert text_page_for_image({'fl_id': '222', 'index': 0}, PAGES) == 1
    assert text_page_for_image({'url': 'https://iiif.nli.org.il/IIIFv21/FL111/full/2000,/0/default.jpg'}, PAGES) == 0


def test_else_by_the_page_number_as_browse_pairs_them():
    # The transcription's pages record their own files' FL ids, not the images' (real data:
    # ENA 2740.3's images are FL157353042/043, its text pages FL157353039/040).
    pages = [{'p_num': 1, 'fl_id': '157353039', 'text': 'recto'}, {'p_num': 2, 'fl_id': '157353040', 'text': 'verso'}]
    assert text_page_for_image({'fl_id': '157353042', 'index': 0}, pages) == 0
    assert text_page_for_image({'fl_id': '157353043', 'index': 1}, pages) == 1
    # A library's own image, no NLI id: its place in the fragment.
    assert text_page_for_image({'url': 'https://images.lib.cam.ac.uk/x.jpg', 'index': 1}, pages) == 1


def test_a_page_with_no_text_does_not_shift_the_rest():
    pages = [{'p_num': 1, 'text': 'one'}, {'p_num': 3, 'text': 'three'}]  # page 2 has no text
    assert text_page_for_image({'index': 2}, pages) == 1
    assert text_page_for_image({'index': 1}, pages) is None  # its page has no text: none shown


def test_an_image_beyond_the_pages_has_no_text_page():
    assert text_page_for_image({'index': 5}, PAGES) is None
    assert text_page_for_image({}, PAGES) is None
    assert text_page_for_image({'fl_id': '111'}, []) is None


def test_the_nli_id_of_an_image():
    assert image_fl_id({'fl_id': 'FL123'}) == '123'
    assert image_fl_id({'url': 'https://iiif.nli.org.il/IIIFv21/FL456'}) == '456'
    assert image_fl_id({'url': 'https://example.org/x.jpg'}) == ''


# -- the desk's order ------------------------------------------------------------------

def test_a_fragment_moves_up_or_down_one_place():
    desk = [{'sys_id': 'a'}, {'sys_id': 'b'}, {'sys_id': 'c'}]
    assert [e['sys_id'] for e in move_entry(desk, 'b', -1)] == ['b', 'a', 'c']
    assert [e['sys_id'] for e in move_entry(desk, 'b', 1)] == ['a', 'c', 'b']
    assert [e['sys_id'] for e in desk] == ['a', 'b', 'c']  # a new list; the desk is not changed


def test_never_off_the_desk():
    desk = [{'sys_id': 'a'}, {'sys_id': 'b'}]
    assert move_entry(desk, 'a', -1) is None
    assert move_entry(desk, 'b', 1) is None
    assert move_entry(desk, 'z', 1) is None


def test_the_desktops_entries_move_too():
    from shared.reading_desk_model import ReadingDeskEntry
    desk = [ReadingDeskEntry(sys_id='a', shelfmark='A'), ReadingDeskEntry(sys_id='b', shelfmark='B')]
    assert [e.sys_id for e in move_entry(desk, 'b', -1)] == ['b', 'a']

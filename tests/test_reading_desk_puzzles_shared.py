# -*- coding: utf-8 -*-
"""The Reading Desk's puzzle images, the rules both apps follow
(shared/reading_desk_puzzles.py): which puzzles of the desk's fragments there are, best first,
and which are on show (GS-294)."""
from types import SimpleNamespace

import pytest

from shared.puzzle_model import PuzzleDocument, PuzzleFragment
from shared.reading_desk_puzzles import (
    NONE_SHOWN, SHOWN_FIRST, choice_of, find_joins, join_fragments, join_label, rank_joins, shown_joins,
)


def _doc(doc_id, sys_ids, when='2026-11-01', title=''):
    return PuzzleDocument(id=doc_id, title=title, updated_at=when,
                          fragments=[PuzzleFragment(sys_id=s, folio_label='1r', fl_id=f'FL{i}', shelfmark=f'T-S {s}')
                                     for i, s in enumerate(sys_ids)])


class _Saved:
    """The saved puzzles' two calls (PuzzleService / the web's SavedJoins)."""

    def __init__(self, docs):
        self.docs = {d.id: d for d in docs}

    def list_documents_for_fragment(self, sys_id=None):
        return [d.id for d in self.docs.values() if any(f.sys_id == sys_id for f in d.fragments)]

    def load_document(self, doc_id):
        return self.docs.get(doc_id)


def _published(join_id, covers, when='2026-10-01', author='A. Reader'):
    return {'kind': 'published', 'id': join_id, 'title': '', 'when': when, 'covers': covers,
            'author': author, 'image_url': f'https://example.org/{join_id}.png'}


def test_own_puzzles_holding_two_of_the_desks_fragments_are_found_best_first():
    saved = _Saved([
        _doc('a', ['1', '2'], when='2026-11-02'),
        _doc('b', ['1', '2', '3'], when='2026-01-01'),
        _doc('c', ['1', '9']),  # one of the desk's: not a puzzle of these fragments
    ])
    found = find_joins(['1', '2', '3'], own=saved)
    assert [j['id'] for j in found] == ['b', 'a']  # most of the desk first
    assert found[0]['kind'] == 'own' and found[0]['covers'] == 3
    assert found[0]['fragments'][0]['sys_id'] == '1'


def test_on_a_desk_of_one_a_puzzle_holding_it_counts():
    assert [j['id'] for j in find_joins(['9'], own=_Saved([_doc('c', ['1', '9'])]))] == ['c']


def test_own_before_published_and_newest_first():
    own = [{'kind': 'own', 'id': 'o', 'when': '2020-01-01', 'covers': 2}]
    published = [_published('p1', 2, when='2026-11-01'), _published('p2', 2, when='2026-11-05'),
                 _published('o', 2)]  # the reader's own, published too: counted once, as theirs
    assert [j['id'] for j in rank_joins(['1', '2'], own, published)] == ['o', 'p2', 'p1']


def test_one_half_failing_leaves_the_other():
    class Broken:
        def list_documents_for_fragment(self, sys_id=None):
            raise RuntimeError('joins.db is locked')

    assert find_joins(['1', '2'], own=Broken()) == []
    # No Supabase client: the reader's own are still found.
    assert [j['id'] for j in find_joins(['1', '2'], own=_Saved([_doc('a', ['1', '2'])]), client=None)] == ['a']


def test_the_first_four_are_shown_until_the_reader_chooses():
    joins = [{'id': str(i)} for i in range(6)]
    assert [j['id'] for j in shown_joins(joins, None)] == ['0', '1', '2', '3']
    assert len(shown_joins(joins, '')) == SHOWN_FIRST


def test_the_readers_choice_in_the_order_found():
    joins = [{'id': str(i)} for i in range(6)]
    assert [j['id'] for j in shown_joins(joins, '5\n1')] == ['1', '5']
    assert choice_of(joins, ['5', '1']) == '1\n5'


def test_a_cleared_list_shows_none_and_a_stale_choice_is_no_choice():
    joins = [{'id': 'a'}, {'id': 'b'}]
    assert choice_of(joins, []) == NONE_SHOWN
    assert shown_joins(joins, NONE_SHOWN) == []
    assert [j['id'] for j in shown_joins(joins, 'deleted-long-ago')] == ['a', 'b']


def test_a_lone_puzzle_is_always_shown():
    # With one puzzle there is no chooser to bring it back with.
    assert [j['id'] for j in shown_joins([{'id': 'a'}], NONE_SHOWN)] == ['a']


def test_publishing_again_changes_the_revision():
    from shared.reading_desk_puzzles import published_revision
    detail = {'fragments_json': {'fragments': [{'sys_id': '1', 'x': 10}]}}
    moved = {'fragments_json': {'fragments': [{'sys_id': '1', 'x': 90}]}}
    assert published_revision(detail) == published_revision(dict(detail))
    assert published_revision(detail) != published_revision(moved)


def test_the_label_says_whose():
    assert join_label({'kind': 'own', 'title': ''}) == 'Your saved join'
    assert join_label({'kind': 'own', 'title': 'T-S 1 + T-S 2'}) == 'T-S 1 + T-S 2 · Your saved join'
    assert join_label({'kind': 'published', 'title': '', 'author': 'A. Reader'}) == 'Published by A. Reader'


def test_an_own_puzzles_fragments_come_back_for_the_composer(caplog):
    join = {'id': 'a', 'fragments': [
        {'sys_id': '1', 'folio_label': '1r', 'fl_id': 'FL1', 'x': 12.5, 'a_key_from_a_newer_save': 1},
        {'folio_label': '1v'},  # no sys_id: cannot be placed
    ]}
    fragments = join_fragments(join)
    assert [(f.sys_id, f.x) for f in fragments] == [('1', 12.5)]
    assert 'cannot be read' in caplog.text  # said, not dropped in silence


@pytest.mark.parametrize('kind', ['own', 'published'])
def test_find_joins_tolerates_a_client_that_fails(kind, monkeypatch):
    import shared.puzzle_publish_service as pps

    def boom(*_a, **_k):
        raise ConnectionError('offline')

    monkeypatch.setattr(pps, 'get_published_joins_for_fragment', boom)
    own = _Saved([_doc('a', ['1', '2'])]) if kind == 'own' else None
    found = find_joins(['1', '2'], own=own, client=SimpleNamespace())
    assert [j['id'] for j in found] == (['a'] if own else [])

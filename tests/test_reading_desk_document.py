# -*- coding: utf-8 -*-
"""The Reading Desk's document mode: which sources a document's desk may show
(web/pages/browse_desk_document.py), over a real SQLite sidecar.

The fixture mirrors what the real sidecar holds: document 100 lies on fragments A and B;
fragment B also belongs to document 200, whose edition sorts FIRST among B's sources -- the
one the per-fragment desk showed under B.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from unittest.mock import patch

import pytest

import shared.document_service as ds
from tests.test_document_service import _create_test_db
from web.pages import browse_desk_document as dd
from web.translations import using_language

SID_A = '990000000000000101'
SID_B = '990000000000000102'
SID_C = '990000000000000103'  # on no document
DOC, OTHER_DOC, BARE_DOC = 100, 200, 300

# Niqqud and a maqaf, in the order the sidecar holds them: the panel must not touch them.
EDITION_ONE = 'recto\n\nבְּרֵאשִׁית בָּרָא\n\nverso\n\nעַל־פְּנֵי תְהוֹם'
EDITION_TWO = 'page a\n\nנוסח שני של המסמך'
HEBREW_TRANSLATION = 'תרגום עברי של המסמך'
ENGLISH_TRANSLATION = 'An English translation of the document'
OTHER_EDITION = 'מהדורה של מסמך אחר'


def build_sidecar(tmp_path) -> str:
    db_path = _create_test_db(str(tmp_path))
    conn = sqlite3.connect(db_path)
    conn.executemany(
        'INSERT INTO documents (pgpid, shelfmark_combined, pgp_url) VALUES (?, ?, ?)',
        [(DOC, 'A + B', 'https://example.org/documents/100/'), (OTHER_DOC, 'B', None), (BARE_DOC, 'A + B', None)])
    conn.executemany(
        'INSERT INTO document_fragments (document_id, sys_id, shelfmark, sequence_order) VALUES (?, ?, ?, ?)',
        [(DOC, SID_A, 'Frag A', 1), (DOC, SID_B, 'Frag B', 2), (OTHER_DOC, SID_B, 'Frag B', 1),
         (BARE_DOC, SID_A, 'Frag A', 1), (BARE_DOC, SID_B, 'Frag B', 2)])
    conn.executemany(
        'INSERT INTO document_sources (id, pgpid, source_scholar, doc_relation, language, content,'
        ' sequence_order, source_direction) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        [(1, DOC, 'Scholar One, Edition (1976).', 'Digital Edition', 'Judaeo-Arabic', EDITION_ONE, 1, 'rtl'),
         (2, DOC, 'Scholar Two, unpublished editions.', 'Digital Edition', 'Judaeo-Arabic', EDITION_TWO, 2, None),
         (3, DOC, 'Scholar One, Edition (1976).', 'Digital Translation', 'Hebrew', HEBREW_TRANSLATION, 1, None),
         (4, DOC, 'Scholar Three (2005).', 'Digital Translation', 'English', ENGLISH_TRANSLATION, 2, None),
         (5, OTHER_DOC, 'Scholar Four.', 'Digital Edition', 'Hebrew', OTHER_EDITION, 0, 'rtl')])
    conn.commit()
    conn.close()
    return db_path


@contextmanager
def sidecar(tmp_path):
    """The module-level document_service functions answer from the fixture."""
    service = ds.PgpService(db_path=build_sidecar(tmp_path), thread_safe=True)
    try:
        with patch.object(ds, '_default_service', service):
            yield service
    finally:
        service.close()


@pytest.fixture
def pgp(tmp_path):
    with sidecar(tmp_path) as service:
        yield service


def _ids(sources):
    return [s['id'] for s in sources]


def test_the_fixture_shows_the_fault_the_per_fragment_desk_had(pgp):
    under_b = pgp.get_all_sources_for_fragment(SID_B)
    assert under_b[0]['pgpid'] == OTHER_DOC  # another document's edition led fragment B's list
    assert {s['pgpid'] for s in under_b} == {DOC, OTHER_DOC}


def test_a_document_desk_offers_only_that_documents_sources(pgp):
    desk = dd.document_desk(DOC, [SID_A, SID_B])
    assert desk['known'] and desk['whole']
    assert _ids(desk['editions']) == [1, 2]
    assert _ids(desk['translations']) == [3, 4]  # apart from the editions
    assert desk['pgp_url'] == 'https://example.org/documents/100/'
    assert (desk['fragments_on_desk'], desk['fragments_total']) == (2, 2)
    assert dd.document_view(desk, {}) == dd.VIEW_EDITION
    assert dd.chosen_source(desk, {}, dd.VIEW_EDITION)['content'] == EDITION_ONE  # whole, headings kept


def test_removing_a_fragment_keeps_the_documents_text(pgp):
    desk = dd.document_desk(DOC, [SID_B])
    assert desk['whole'] and dd.has_document_text(desk)
    assert (desk['fragments_on_desk'], desk['fragments_total']) == (1, 2)
    assert not dd.is_other_desk(desk)


def test_a_fragment_from_outside_the_document_ends_document_mode(pgp):
    desk = dd.document_desk(DOC, [SID_A, SID_B, SID_C])
    assert dd.is_other_desk(desk)
    assert desk['editions'] == [] and desk['translations'] == []
    assert dd.document_view(desk, {}) is None


def test_a_document_without_any_text_keeps_the_per_fragment_desk(pgp):
    desk = dd.document_desk(BARE_DOC, [SID_A, SID_B])
    assert desk['whole'] and not dd.has_document_text(desk)
    assert dd.document_view(desk, {}) is None
    assert not dd.is_other_desk(desk)  # still that document's desk


def test_a_hand_built_desk_and_an_unknown_document_are_not_document_mode(pgp):
    assert dd.document_view(dd.document_desk(None, [SID_A, SID_B]), {}) is None
    unknown = dd.document_desk(999, [SID_A])
    assert not unknown['known'] and not dd.is_other_desk(unknown)
    assert dd.document_view(unknown, {}) is None


def test_the_readers_choices_are_kept_and_a_stale_one_falls_back(pgp):
    desk = dd.document_desk(DOC, [SID_A, SID_B])
    chosen = {dd.KEY_EDITION: '2', dd.KEY_TRANSLATION: '4', dd.KEY_VIEW: dd.VIEW_TRANSLATION}
    assert dd.document_view(desk, chosen) == dd.VIEW_TRANSLATION
    assert dd.chosen_source(desk, chosen, dd.VIEW_EDITION)['id'] == 2
    assert dd.chosen_source(desk, chosen, dd.VIEW_TRANSLATION)['id'] == 4
    assert dd.chosen_source(desk, {dd.KEY_EDITION: '5'}, dd.VIEW_EDITION)['id'] == 1  # 5 is another document's
    assert dd.document_view(desk, {dd.KEY_VIEW: dd.VIEW_FRAGMENTS}) == dd.VIEW_FRAGMENTS


def test_a_document_with_translations_only_opens_on_the_translation(pgp):
    pgp._conn.close()
    conn = sqlite3.connect(pgp._db_path)
    conn.execute("DELETE FROM document_sources WHERE pgpid = ? AND doc_relation LIKE '%Edition%'", (DOC,))
    conn.commit()
    conn.close()
    with patch.object(ds, '_default_service', ds.PgpService(db_path=pgp._db_path)):
        desk = dd.document_desk(DOC, [SID_A, SID_B])
        assert desk['editions'] == [] and _ids(desk['translations']) == [3, 4]
        assert dd.document_view(desk, {}) == dd.VIEW_TRANSLATION
        assert dd.document_view(desk, {dd.KEY_VIEW: dd.VIEW_EDITION}) == dd.VIEW_TRANSLATION


def test_text_direction_and_labels(pgp):
    desk = dd.document_desk(DOC, [SID_A, SID_B])
    one, two = desk['editions']
    hebrew, english = desk['translations']
    assert [dd.text_direction(s) for s in (one, two, hebrew, english)] == ['rtl', 'rtl', 'rtl', 'ltr']
    assert dd.source_label(one, dd.VIEW_EDITION) == 'Scholar One, Edition (1976).'
    with using_language('en'):
        assert dd.source_label(english, dd.VIEW_TRANSLATION) == 'English: Scholar Three (2005).'
    with using_language('he'):
        assert dd.source_label(hebrew, dd.VIEW_TRANSLATION) == 'עברית: Scholar One, Edition (1976).'

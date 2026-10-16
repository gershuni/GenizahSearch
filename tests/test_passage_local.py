# -*- coding: utf-8 -*-
"""Letter-level search for My Library: the LOCAL corpus reader, the second
lifecycle instance, and the scope router.

Everything runs on a temporary LOCAL sidecar built with the indexer's own
schema (`init_sqlite`), so the reader is tested against the real table
shapes, never a copy of them.
"""
from __future__ import annotations

import os
import sqlite3

import pytest

from shared.local_indexer import (_make_full_header, compress_cached_text,
                                  init_sqlite)
from shared.passage_builder import build_index
from shared.passage_index import MAX_RECORD_LETTERS, open_index
from shared.passage_local_corpus import (
    SEGMENT_TARGET_LETTERS, LocalPageTextFetcher, finish_local_rows,
    iter_local_records, parse_local_header, segment_record_id, segment_text,
    split_record_id, write_corpus_signature,
)
from shared.passage_normalize import norm_stream_fast
from shared.passage_parallels import PassageSearcher
from shared.sys_id_patterns import (CORPUS_SYS_ID_RE, LOCAL_SYS_ID_RE,
                                    extract_corpus_sys_id)

SYS_A = '970000000100000001'
SYS_B = '970000000100000002'

PSALM = ('השמים מספרים כבוד אל ומעשה ידיו מגיד הרקיע יום ליום יביע אמר '
         'ולילה ללילה יחוה דעת אין אמר ואין דברים בלי נשמע קולם בכל הארץ '
         'יצא קום ובקצה תבל מליהם לשמש שם אהל בהם והוא כחתן יצא מחפתו ישיש '
         'כגבור לרוץ ארח')
FILLER = ('ויאמר ראובן אל אחיו הלא אמרתי אליכם לאמר אל תחטאו בילד ולא '
          'שמעתם וגם דמו הנה נדרש והם לא ידעו כי שמע יוסף כי המליץ בינתם ')


def _add_file(conn, sys_id, path, pages, folder_id=1):
    conn.execute(
        "INSERT INTO local_files (sys_id, filepath, folder_id, "
        "original_filename, file_extension, page_count, file_size_bytes, "
        "extraction_status, last_indexed_at) VALUES (?,?,?,?,?,?,?,?,?)",
        (sys_id, path, folder_id, os.path.basename(path), '.txt', len(pages),
         100, 'ok', 0.0))
    conn.execute(
        "INSERT INTO processed_files (filepath, mtime, size, sys_id, status) "
        "VALUES (?,?,?,?, 'committed')", (path, 1.0, 100, sys_id))
    for n, text in enumerate(pages, start=1):
        blob, ln = compress_cached_text(text)
        conn.execute(
            "INSERT INTO local_pages (sys_id, uid, page_num, cached_text, "
            "cached_text_codec, cached_text_uncompressed_len) "
            "VALUES (?,?,?,?, 'zstd', ?)",
            (sys_id, f'LOCAL_{sys_id}_P{n}', n, blob, ln))
    file_id = conn.execute('SELECT file_id FROM local_files WHERE sys_id=?',
                           (sys_id,)).fetchone()[0]
    return file_id


@pytest.fixture
def local_db(tmp_path):
    path = str(tmp_path / 'local_index.sqlite3')
    conn = init_sqlite(path)
    conn.execute("INSERT INTO folders (path, added_at, status) "
                 "VALUES ('c:/lib', 0, 'active')")
    fa = _add_file(conn, SYS_A, 'c:/lib/a.txt',
                   [FILLER * 3, FILLER + PSALM + FILLER])
    fb = _add_file(conn, SYS_B, 'c:/lib/b.txt', [PSALM + FILLER * 2])
    conn.commit()
    conn.close()
    return path, fa, fb


# ---------------------------------------------------------------------------
# Namespace.
# ---------------------------------------------------------------------------

def test_local_pattern_is_local_only():
    header = _make_full_header(SYS_A, 3, 7)
    assert LOCAL_SYS_ID_RE.search(header).group(1) == SYS_A
    assert LOCAL_SYS_ID_RE.search('990051620920205171_IE1_P1_FL2') is None
    assert extract_corpus_sys_id(header) is None
    assert CORPUS_SYS_ID_RE.search(header) is None


# ---------------------------------------------------------------------------
# Segmentation.
# ---------------------------------------------------------------------------

def test_short_page_is_one_record_unchanged():
    assert segment_text(PSALM) == [PSALM]


def test_long_page_splits_at_whitespace_under_the_budget():
    text = (FILLER * 1500)
    assert len(norm_stream_fast(text)) >= MAX_RECORD_LETTERS
    pieces = segment_text(text)
    assert len(pieces) > 1
    assert ''.join(pieces) == text
    for p in pieces:
        assert len(norm_stream_fast(p)) <= SEGMENT_TARGET_LETTERS
    # cut only at whitespace: every piece but the last ends in a space
    assert all(p.endswith(' ') for p in pieces[:-1])
    assert segment_text(text) == pieces          # deterministic


def test_whitespace_free_run_is_cut_by_characters():
    text = 'א' * (MAX_RECORD_LETTERS + 10)
    pieces = segment_text(text)
    assert ''.join(pieces) == text
    assert max(len(norm_stream_fast(p)) for p in pieces) <= SEGMENT_TARGET_LETTERS


def test_segment_ids_round_trip():
    header = _make_full_header(SYS_A, 12, 3)
    assert split_record_id(header) == (header, None)
    assert split_record_id(segment_record_id(header, 4)) == (header, 4)
    assert parse_local_header(header) == {'sys_id': SYS_A, 'page': 12,
                                          'file_id': 3}
    assert parse_local_header('990051620920205171_IE1_P1_FL2') is None


# ---------------------------------------------------------------------------
# Reading the sidecar.
# ---------------------------------------------------------------------------

def test_records_are_the_pages_under_their_tantivy_headers(local_db):
    path, fa, fb = local_db
    recs = list(iter_local_records(path))
    assert [r[0] for r in recs] == [
        _make_full_header(SYS_A, 1, fa), _make_full_header(SYS_A, 2, fa),
        _make_full_header(SYS_B, 1, fb)]
    assert recs[2][1] == PSALM + FILLER * 2


def test_reader_never_writes_to_the_sidecar(local_db):
    path, _fa, _fb = local_db

    def _state():
        return [(os.path.getmtime(f), os.path.getsize(f))
                if os.path.exists(f) else None
                for f in (path, path + '-wal')]

    before = _state()
    list(iter_local_records(path))
    write_corpus_signature(path, path + '.sig')
    after = _state()
    # The database file itself is untouched.
    assert after[0] == before[0]
    # SQLite may create an EMPTY -wal beside a WAL-mode database on open (its
    # own housekeeping, even read-only); an existing one is never written.
    assert after[1] == before[1] or (before[1] is None and after[1][1] == 0)


def test_uncommitted_files_are_not_read(local_db):
    path, _fa, _fb = local_db
    conn = sqlite3.connect(path)
    conn.execute("UPDATE processed_files SET status='pending' WHERE sys_id=?",
                 (SYS_B,))
    conn.commit()
    conn.close()
    assert all(SYS_B not in rid for rid, _t in iter_local_records(path))


def test_signature_changes_with_the_library(local_db, tmp_path):
    path, _fa, _fb = local_db
    sig = str(tmp_path / 'sig')
    first = open(write_corpus_signature(path, sig)).read()
    assert open(write_corpus_signature(path, sig)).read() == first
    conn = sqlite3.connect(path)
    conn.execute("UPDATE processed_files SET mtime=2.0 WHERE sys_id=?",
                 (SYS_B,))
    conn.commit()
    conn.close()
    assert open(write_corpus_signature(path, sig)).read() != first


# ---------------------------------------------------------------------------
# End to end: build, search, render.
# ---------------------------------------------------------------------------

class _Doc(dict):
    pass


class _FakeLocalIndex:
    """Just enough of tantivy for `LocalPageTextFetcher`: a phrase query on
    `full_header` resolves to the stored page."""

    def __init__(self, pages):
        self.pages = pages               # header -> (text, shelfmark)

    def parse_query(self, q, _fields):
        return q.split('"')[1]

    def search(self, header, _n):
        class R:
            pass
        r = R()
        r.hits = [(1.0, header)] if header in self.pages else []
        return r

    def doc(self, header):
        text, shelf = self.pages[header]
        return {'full_header': [header], 'content': [text],
                'shelfmark': [shelf]}


class _Engine:
    def __init__(self, pages):
        self.local_index = _FakeLocalIndex(pages)
        self.local_searcher = self.local_index


def _pages(path):
    out = {}
    for rid, text in iter_local_records(path):
        base, _seg = split_record_id(rid)
        out.setdefault(base, '')
        out[base] += text
    return {h: (t, h.split('_')[0] + '.txt') for h, t in out.items()}


def test_build_search_and_render(local_db, tmp_path):
    path, fa, fb = local_db
    idx_dir = str(tmp_path / 'idx')
    build_index(iter_local_records(path), idx_dir, partitions=2)
    index = open_index(idx_dir)
    try:
        fetcher = LocalPageTextFetcher(_Engine(_pages(path)))
        s = PassageSearcher(index=index, text_fetcher=fetcher,
                            sys_id_re=LOCAL_SYS_ID_RE)
        res = finish_local_rows(s.search_composition_logic(PSALM), fetcher)
        rows = res['main'] + res['filtered']
        headers = {r['raw_header'] for r in rows}
        assert _make_full_header(SYS_A, 2, fa) in headers
        assert _make_full_header(SYS_B, 1, fb) in headers
        for r in rows:
            assert r['src_lbl'] == 'LOCAL'
            assert r['uid'].startswith('LOCAL_97')
            assert r['shelfmark'].endswith('.txt')
            assert r['text']
        # restrict_sys_ids is read in the LOCAL namespace
        only_b = s.search_composition_logic(PSALM, restrict_sys_ids={SYS_B})
        assert {r['raw_header'].split('_')[0]
                for r in only_b['main'] + only_b['filtered']} == {SYS_B}
    finally:
        index.close()


# ---------------------------------------------------------------------------
# The two lifecycle slots and the router.
# ---------------------------------------------------------------------------

SYS_G = '990000000000000001'


@pytest.fixture
def slots():
    """Both lifecycle slots, clean before and after (they are module state)."""
    import desktop.passage_lifecycle as pl
    for slot in (pl.SLOT_GENIZAH, pl.SLOT_LOCAL):
        pl.close_passage_state(slot=slot)
    yield pl
    for slot in (pl.SLOT_GENIZAH, pl.SLOT_LOCAL):
        pl.close_passage_state(slot=slot)


def _install(pl, slot, records, idx_dir):
    build_index(records, idx_dir, partitions=2)
    index = open_index(idx_dir)
    assert index is not None
    assert pl.install_passage_state(
        pl.PassageState(index=index, live_dir=idx_dir), slot=slot)


def _genizah_records():
    return [(f'{SYS_G}_IE1_P{n}_FL{n}', FILLER + PSALM + FILLER)
            for n in (1, 2)]


def test_slots_install_and_close_independently(slots, local_db, tmp_path):
    pl = slots
    path, _fa, _fb = local_db
    g0 = pl.current_state_generation(pl.SLOT_GENIZAH)
    _install(pl, pl.SLOT_LOCAL, iter_local_records(path), str(tmp_path / 'l'))
    # Installing My Library moves only its own slot.
    assert pl.passage_available(pl.SLOT_LOCAL)
    assert not pl.passage_available(pl.SLOT_GENIZAH)
    assert pl.current_state_generation(pl.SLOT_GENIZAH) == g0
    l1 = pl.current_state_generation(pl.SLOT_LOCAL)

    _install(pl, pl.SLOT_GENIZAH, _genizah_records(), str(tmp_path / 'g'))
    assert pl.current_state_generation(pl.SLOT_LOCAL) == l1
    # A release authorised for one generation of one slot cannot close the
    # other slot, or a later generation of its own.
    assert pl.close_passage_state(l1 + 7, slot=pl.SLOT_LOCAL) is False
    assert pl.passage_available(pl.SLOT_LOCAL)
    assert pl.close_passage_state(l1, slot=pl.SLOT_LOCAL) is True
    assert not pl.passage_available(pl.SLOT_LOCAL)
    assert pl.passage_available(pl.SLOT_GENIZAH)
    with pytest.raises(ValueError):
        pl.passage_available('elsewhere')


def test_a_lease_on_one_slot_does_not_hold_up_closing_the_other(
        slots, local_db, tmp_path, monkeypatch):
    """The point of separate slots: a search running against the Genizah
    index leaves the My Library index free to close or swap, and the
    Genizah index itself still refuses to close under its reader."""
    import threading

    import shared.passage_parallels as passage_parallels
    pl = slots
    path, _fa, _fb = local_db
    _install(pl, pl.SLOT_LOCAL, iter_local_records(path), str(tmp_path / 'l'))
    _install(pl, pl.SLOT_GENIZAH, _genizah_records(), str(tmp_path / 'g'))
    monkeypatch.setattr(pl, 'LEASE_DRAIN_TIMEOUT_SECONDS', 0.2)
    monkeypatch.setattr(pl, 'LEASE_DRAIN_POLL_SECONDS', 0.02)

    entered, release = threading.Event(), threading.Event()

    def _blocked(self, *a, **k):
        entered.set()
        assert release.wait(timeout=60)
        return {'main': [], 'filtered': []}

    monkeypatch.setattr(passage_parallels.PassageSearcher,
                        'search_composition_logic', _blocked)
    reader = threading.Thread(
        target=lambda: pl.get_passage_searcher(object())
        .search_composition_logic(PSALM))
    reader.start()
    try:
        assert entered.wait(timeout=10), 'the Genizah search never started'
        assert pl.close_passage_state(slot=pl.SLOT_GENIZAH) is False
        assert pl.close_passage_state(slot=pl.SLOT_LOCAL) is True
    finally:
        release.set()
        reader.join(timeout=10)
    assert pl.close_passage_state(slot=pl.SLOT_GENIZAH) is True


def test_local_slot_searches_through_the_router(slots, local_db, tmp_path):
    import desktop.passage_local as passage_local
    pl = slots
    path, fa, fb = local_db
    _install(pl, pl.SLOT_LOCAL, iter_local_records(path), str(tmp_path / 'l'))
    router = passage_local.ScopedPassageSearcher(
        _Engine(_pages(path)), 'widest-40', 'normal', 'normal')
    res = router.search_composition_logic(PSALM, corpus_scope='local')
    rows = res['main'] + res['filtered']
    assert {r['raw_header'] for r in rows} == {
        _make_full_header(SYS_A, 2, fa), _make_full_header(SYS_B, 1, fb)}
    assert all(r['src_lbl'] == 'LOCAL' for r in rows)
    # The slot, not the caller, picks the namespace: a LOCAL restriction
    # works through the adapter with no pattern passed in.
    only_b = router.search_composition_logic(
        PSALM, corpus_scope='local', restrict_sys_ids={SYS_B})
    assert {r['raw_header'].split('_')[0]
            for r in only_b['main'] + only_b['filtered']} == {SYS_B}
    # Nothing is installed in the Genizah slot, and saying so is that
    # slot's job -- the My Library index must not answer for it.
    with pytest.raises(pl.PassageSearchUnavailableError):
        router.search_composition_logic(PSALM, corpus_scope='genizah')


def test_local_slot_unavailable_names_my_library(slots):
    pl = slots
    with pytest.raises(pl.PassageSearchUnavailableError,
                       match='My Library'):
        pl.get_passage_searcher(object(), slot=pl.SLOT_LOCAL) \
            .search_composition_logic(PSALM)


def test_router_sends_conditions_to_the_genizah_index(monkeypatch):
    """`CompoundCompositionThread` calls `search_condition` on whatever
    searcher it holds. The router must answer it, and only from the Genizah
    slot: condition masks are built from Genizah page keys."""
    import desktop.passage_lifecycle as pl
    import desktop.passage_local as pll
    calls = []

    class _Adapter:
        def __init__(self, slot):
            self.slot = slot

        def search_condition(self, text, **kw):
            calls.append((self.slot, text, kw))
            return 'found'

    monkeypatch.setattr(
        pl, 'get_passage_searcher',
        lambda *a, slot=pl.SLOT_GENIZAH, **k: _Adapter(slot))
    r = pll.ScopedPassageSearcher(object(), 'widest-40', 'normal', 'normal')
    assert r.search_condition('x', pages=(1,), sys_ids=('s',),
                              render_limit=3) == 'found'
    assert calls == [(pl.SLOT_GENIZAH, 'x', {
        'pages': (1,), 'sys_ids': ('s',), 'should_stop': None,
        'render_limit': 3})]


def test_router_merges_both_indexes_by_score(monkeypatch):
    import desktop.passage_lifecycle as pl
    import desktop.passage_local as pll

    class _S:
        def __init__(self, rows):
            self.rows = rows

        def search_composition_logic(self, *_a, **_k):
            return {'main': list(self.rows), 'filtered': [],
                    'truncated_to_200': False,
                    'dropped_text_lookup_failures': 1,
                    'duplicate_photography_demoted': 0,
                    'query_report': {}}

    g_row = {'raw_header': '990000000000000001_IE1_P1_FL1', 'score': 50.0}
    l_header = _make_full_header(SYS_A, 1, 1)
    l_row = {'raw_header': l_header, 'score': 80.0}
    monkeypatch.setattr(
        pl, 'get_passage_searcher',
        lambda *a, slot=pl.SLOT_GENIZAH, **k:
            _S([l_row] if slot == pl.SLOT_LOCAL else [g_row]))
    monkeypatch.setattr(pll.LocalPageTextFetcher, 'shelfmark',
                        lambda self, h: 'a.txt')
    r = pll.ScopedPassageSearcher(object(), 'widest-40', 'default',
                                  'default')
    both = r.search_composition_logic('x', corpus_scope='all')
    assert [x['score'] for x in both['main']] == [80.0, 50.0]
    assert both['main'][0]['src_lbl'] == 'LOCAL'
    assert both['dropped_text_lookup_failures'] == 2
    only_g = r.search_composition_logic('x', corpus_scope='genizah')
    assert [x['score'] for x in only_g['main']] == [50.0]

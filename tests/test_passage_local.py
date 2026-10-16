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

    # Every reader opens the sidecar through the one read-only helper, and
    # that helper refuses a write -- so "reads only" is enforced, not just
    # observed on this run.
    import shared.passage_local_corpus as plc
    opened = []
    real_connect = plc._connect_ro

    def _spy(p):
        conn = real_connect(p)
        opened.append(conn)
        return conn

    probe = real_connect(path)
    with pytest.raises(sqlite3.OperationalError):
        probe.execute('CREATE TABLE intruder (x)')
    probe.close()

    before = _state()
    plc._connect_ro = _spy
    try:
        list(iter_local_records(path))
        write_corpus_signature(path, path + '.sig')
        plc.count_local_pages(path)
        plc.local_text_bytes(path)
    finally:
        plc._connect_ro = real_connect
    assert len(opened) == 4
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
    # A pre-search restriction is a set of Genizah ids and applies to
    # Genizah rows only: My Library rows always show, as in chunk search
    # (owner, 2026-10-16) -- not "every My Library row hidden".
    restricted = router.search_composition_logic(
        PSALM, corpus_scope='local', restrict_sys_ids={SYS_G})
    assert {r['raw_header'] for r in
            restricted['main'] + restricted['filtered']} == {
        _make_full_header(SYS_A, 2, fa), _make_full_header(SYS_B, 1, fb)}
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


def test_a_my_library_build_keeps_the_pages_genizah_hygiene_drops(
        slots, local_db, tmp_path, monkeypatch):
    """Built through the real lifecycle with the My Library setting: a page
    with 40-79 Hebrew letters (enough for a match, under the Genizah 80-letter
    floor) is indexed, and so is a page full of catalogue-card words."""
    pl = slots
    path, _fa, _fb = local_db
    short = 'השמים מספרים כבוד אל ומעשה ידיו מגיד הרקיע יום ליום יביע אמר'
    assert 40 <= len(norm_stream_fast(short)) < 80
    card = 'סימן תוכן מחבר שנה הערות ' + PSALM
    records = list(iter_local_records(path)) + [
        (_make_full_header(SYS_A, 9, 1), short),
        (_make_full_header(SYS_A, 10, 1), card)]
    root = str(tmp_path / 'root')
    res = pl.run_build_and_swap(
        root, iter(records), [], path, partitions=2,
        release_live_state=lambda g: pl.close_passage_state(
            g, slot=pl.SLOT_LOCAL),
        slot=pl.SLOT_LOCAL, needed_bytes=0, apply_hygiene=False)
    assert res.status == 'installed', res
    try:
        ids = {res.index.record_id(i) for i in range(res.index.n_records)}
        assert _make_full_header(SYS_A, 9, 1) in ids
        assert _make_full_header(SYS_A, 10, 1) in ids
    finally:
        res.index.close()


def test_my_library_never_demotes_a_page_as_a_duplicate_photograph(
        slots, tmp_path):
    """Two near-identical pages of a user's files are two pages they own,
    not one page photographed twice: both stay in the main results. The
    control -- the same index searched with the Genizah default -- demotes
    one, so the rule does fire on this text."""
    pl = slots
    import desktop.passage_local as passage_local
    words = (PSALM + ' ' + FILLER).split()
    text = '\n'.join(' '.join(words[i:i + 6]) for i in range(0, len(words), 6))
    db = str(tmp_path / 'dup.sqlite3')
    conn = init_sqlite(db)
    conn.execute("INSERT INTO folders (path, added_at, status) "
                 "VALUES ('c:/lib', 0, 'active')")
    fa = _add_file(conn, SYS_A, 'c:/lib/a.pdf', [text])
    fb = _add_file(conn, SYS_B, 'c:/lib/b.pdf', [text])
    conn.commit()
    conn.close()
    _install(pl, pl.SLOT_LOCAL, iter_local_records(db), str(tmp_path / 'l'))
    pages = _pages(db)
    router = passage_local.ScopedPassageSearcher(
        _Engine(pages), 'widest-40', 'normal', 'normal')
    res = router.search_composition_logic(text, corpus_scope='local')
    assert res['duplicate_photography_demoted'] == 0
    assert {r['raw_header'] for r in res['main']} == {
        _make_full_header(SYS_A, 1, fa), _make_full_header(SYS_B, 1, fb)}

    index = open_index(str(tmp_path / 'l'))
    try:
        control = PassageSearcher(index=index,
                                  text_fetcher=LocalPageTextFetcher(
                                      _Engine(pages)),
                                  sys_id_re=LOCAL_SYS_ID_RE)
        assert control.search_composition_logic(text)[
            'duplicate_photography_demoted'] == 1
    finally:
        index.close()


def test_the_my_library_build_worker_turns_hygiene_off(monkeypatch, tmp_path):
    """The call site, not only the parameter: the worker that builds the My
    Library index passes apply_hygiene=False, to the local slot."""
    import desktop.passage_lifecycle as pl
    import desktop.passage_local as passage_local
    import shared.passage_local_corpus as plc
    from desktop.passage_workers import LocalPassageBuildThread
    seen = {}

    def _capture(*a, **k):
        seen.update(k)
        return pl.BuildAndSwapResult(status='cancelled')

    monkeypatch.setattr(pl, 'run_build_and_swap', _capture)
    monkeypatch.setattr(plc, 'count_local_pages', lambda p: 1)
    monkeypatch.setattr(passage_local, 'estimate_local_build_bytes',
                        lambda p: 123)
    monkeypatch.setattr(passage_local, 'refresh_signature',
                        lambda root, cancel_check=None: 'sig')
    LocalPassageBuildThread(str(tmp_path), 'db', lambda g: True).run()
    assert seen['apply_hygiene'] is False
    assert seen['slot'] == pl.SLOT_LOCAL
    assert seen['needed_bytes'] == 123


def test_segments_of_one_page_come_back_as_one_row():
    """A long page that matched in two segments is ONE row, the better one,
    with both segments' matches in its highlight; a page already in 'main'
    is not repeated in 'filtered'."""
    import re

    from shared.passage_parallels import _match_pattern
    from shared.passage_local_corpus import _pattern_literals
    head = _make_full_header(SYS_A, 3, 1)

    class _F:
        def shelfmark(self, h):
            return 'a.txt'

        def chunk_locator(self, h):
            return ''

    res = finish_local_rows({
        'main': [
            {'raw_header': segment_record_id(head, 0), 'score': 90.0,
             'highlight_pattern': _match_pattern(['אבג']),
             'source_highlight_pattern': _match_pattern(['אבג'])},
            {'raw_header': segment_record_id(head, 2), 'score': 40.0,
             'highlight_pattern': _match_pattern(['דהו.(x)|y']),
             'source_highlight_pattern': ''}],
        'filtered': [
            {'raw_header': segment_record_id(head, 1), 'score': 99.0,
             'highlight_pattern': _match_pattern(['זחט']),
             'source_highlight_pattern': ''}],
    }, _F())
    assert [r['raw_header'] for r in res['main']] == [head]
    assert res['filtered'] == []
    row = res['main'][0]
    assert row['score'] == 90.0
    assert set(_pattern_literals(row['highlight_pattern'])) == {
        'אבג', 'דהו.(x)|y', 'זחט'}
    # Still a literal pattern: the special characters stay escaped.
    assert re.search(row['highlight_pattern'], 'דהו.(x)|y').group(0) == \
        'דהו.(x)|y'

    # A stronger segment arriving LATER in the same bucket takes the page's
    # row, in the page's place, still carrying both segments' matches.
    later = finish_local_rows({'main': [
        {'raw_header': segment_record_id(head, 0), 'score': 10.0,
         'text': 'weak', 'highlight_pattern': _match_pattern(['אבג']),
         'source_highlight_pattern': ''},
        {'raw_header': _make_full_header(SYS_B, 1, 2), 'score': 5.0,
         'highlight_pattern': '', 'source_highlight_pattern': ''},
        {'raw_header': segment_record_id(head, 1), 'score': 70.0,
         'text': 'strong', 'highlight_pattern': _match_pattern(['זחט']),
         'source_highlight_pattern': ''}]}, _F())
    assert [r['raw_header'] for r in later['main']] == [
        head, _make_full_header(SYS_B, 1, 2)]
    best = later['main'][0]
    assert (best['score'], best['text']) == (70.0, 'strong')
    assert set(_pattern_literals(best['highlight_pattern'])) == {'אבג', 'זחט'}


def test_rows_carry_their_page_for_the_exports():
    """Word prints `chunk_locator` verbatim and falls back to `p_num`
    (shared/docx_export.py); a My Library row without either printed an
    empty page label."""
    from shared.docx_export import _page_label
    head = _make_full_header(SYS_A, 12, 3)

    class _F:
        def __init__(self, locator):
            self._locator = locator

        def shelfmark(self, h):
            return 'a.pdf'

        def chunk_locator(self, h):
            return self._locator

    for locator, label in (('', 'p. 12'), ('paragraphs 21-40',
                                           'paragraphs 21-40')):
        row = finish_local_rows({'main': [{'raw_header': head, 'score': 1.0}]},
                                _F(locator))['main'][0]
        assert row['p_num'] == 12
        # The export's own row conversion (genizah_app.py, the Word block).
        assert _page_label({'chunk_locator': row.get('chunk_locator', ''),
                            'p_num': row.get('p_num')}) == label


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

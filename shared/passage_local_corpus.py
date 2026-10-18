# -*- coding: utf-8 -*-
"""The My Library ("LOCAL") corpus as passage-index records.

The Genizah letter-level index is built from the transcriptions file
(`shared/passage_corpus.py`). My Library has no such file: its text lives in
the LOCAL SQLite sidecar (`local_pages.cached_text`, zstd), which is also
what the LOCAL Tantivy index is rebuilt from -- so reading it here gives the
passage index exactly the text the LOCAL index stores and displays
(`cached_text == content`, `shared/local_indexer.py` D-06).

Everything here is READ-ONLY with respect to the LOCAL index. The SQLite
sidecar is opened with `mode=ro`, nothing is written to it or to LocalIndex,
and the passage artifact lives in its own directory. The LOCAL index is used
by other tools as it is; building a letter-level index beside it must never
change it.

Record grain is the LOCAL page, and the record id is the page's Tantivy
`full_header` (`{sys_id}_LOCAL_P{n}_F{file_id:04d}`), so a hit resolves to
its page through the same `full_header` lookup the Genizah side uses.

One difference from the Genizah corpus: a printed book's "page" can be far
longer than a manuscript page. The index layout addresses a letter with 16
bits (`shared/passage_index.py::MAX_RECORD_LETTERS`), so a page at or over
that budget is split into SEGMENTS at whitespace, each its own record with a
`_S{i}` suffix on the header. `segment_text` is deterministic, and the text
fetcher re-applies it to the stored page, so a segment's offsets index the
same string at build time and at display time.
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
from typing import Callable, Iterator, Optional

from shared.passage_index import MAX_RECORD_LETTERS, BuildCancelled
from shared.passage_normalize import norm_stream_fast
from shared.sys_id_patterns import LOCAL_SYS_ID_PATTERN

logger = logging.getLogger(__name__)

# A segment is cut once it would pass this many normalized letters. Well under
# MAX_RECORD_LETTERS (65,536) so a cut never has to land mid-word to fit.
SEGMENT_TARGET_LETTERS = 50_000

_SEGMENT_RE = re.compile(r'^(?P<base>[A-Za-z0-9_]+?)_S(?P<seg>\d+)$')
# `{sys_id}_LOCAL_P{page}_F{file_id}` -- shared/local_indexer.py::_make_full_header.
# The sys_id part is the shared LOCAL pattern (group 1), never a copy of it.
_LOCAL_HEADER_RE = re.compile(
    '^' + LOCAL_SYS_ID_PATTERN + r'_LOCAL_P(?P<page>\d+)_F(?P<file_id>\d+)$')
_HEADER_CHARS_RE = re.compile(r'^[A-Za-z0-9_]+$')
_TOKEN_RE = re.compile(r'\S+\s*|\s+')

CANCEL_CHECK_ROWS = 20_000

SIGNATURE_NAME = 'my_library_corpus.sig'


# ---------------------------------------------------------------------------
# Segmentation.
# ---------------------------------------------------------------------------

def segment_text(text: str) -> list:
    """Split `text` into pieces each under the index's per-record letter
    budget, cutting only at whitespace. A page under the budget comes back as
    `[text]` unchanged -- by far the common case (99.9% of pages measured on
    a 2.56M-page library) -- so the split is paid for only where it is
    needed. Concatenating the pieces gives back `text` exactly."""
    if len(norm_stream_fast(text)) < MAX_RECORD_LETTERS:
        return [text]
    pieces = []
    buf: list = []
    letters = 0
    for m in _TOKEN_RE.finditer(text):
        tok = m.group(0)
        n = len(norm_stream_fast(tok))
        if n >= SEGMENT_TARGET_LETTERS:
            # A single whitespace-free run longer than a whole segment (a
            # broken extraction, not prose). Cut it by characters; nothing
            # else can keep it inside the budget.
            if buf:
                pieces.append(''.join(buf))
                buf, letters = [], 0
            step = SEGMENT_TARGET_LETTERS
            for i in range(0, len(tok), step):
                pieces.append(tok[i:i + step])
            continue
        if letters + n > SEGMENT_TARGET_LETTERS and buf:
            pieces.append(''.join(buf))
            buf, letters = [], 0
        buf.append(tok)
        letters += n
    if buf:
        pieces.append(''.join(buf))
    return pieces


def segment_record_id(full_header: str, i: int) -> str:
    return f'{full_header}_S{i}'


def split_record_id(record_id: str) -> tuple:
    """(page full_header, segment index or None)."""
    m = _SEGMENT_RE.match(record_id or '')
    if m and _LOCAL_HEADER_RE.match(m.group('base')):
        return m.group('base'), int(m.group('seg'))
    return record_id, None


def parse_local_header(full_header: str) -> Optional[dict]:
    m = _LOCAL_HEADER_RE.match(full_header or '')
    if not m:
        return None
    return {'sys_id': m.group(1), 'page': int(m.group('page')),
            'file_id': int(m.group('file_id'))}


# ---------------------------------------------------------------------------
# Reading the corpus (read-only).
# ---------------------------------------------------------------------------

def _connect_ro(db_path: str) -> sqlite3.Connection:
    """Read-only by URI, so no code path here can write to the sidecar --
    not even SQLite's own journal housekeeping."""
    uri = 'file:' + os.path.abspath(db_path).replace('\\', '/') + '?mode=ro'
    conn = sqlite3.connect(uri, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def open_local_snapshot(db_path: str) -> sqlite3.Connection:
    """A read-only connection held inside ONE read transaction: every query on
    it sees the library as it was at its first read, whatever a scan or a
    Re-index All commits meanwhile (the sidecar is in WAL mode, so writers are
    not blocked). Close it to end the transaction.

    A build takes its signature and its records from one snapshot, so the
    signature describes exactly the pages it indexed. Read separately, a
    Re-index All that marks every file pending and later restores it
    unchanged could leave a partial index whose signature still matched the
    library, so the freshness check called it current (PR #399 review)."""
    conn = _connect_ro(db_path)
    conn.execute('BEGIN')
    return conn


_PAGES_SQL = """
    SELECT lp.sys_id, lp.page_num, lp.cached_text, lp.cached_text_codec,
           lf.file_id
    FROM local_pages lp
    INNER JOIN processed_files pf ON pf.sys_id = lp.sys_id
    INNER JOIN local_files lf ON lf.sys_id = lp.sys_id
    WHERE pf.status = 'committed'
    ORDER BY lp.sys_id, lp.page_num
"""


def count_local_pages(db_path: str) -> int:
    conn = _connect_ro(db_path)
    try:
        return int(conn.execute("""
            SELECT COUNT(*) FROM local_pages lp
            INNER JOIN processed_files pf ON pf.sys_id = lp.sys_id
            INNER JOIN local_files lf ON lf.sys_id = lp.sys_id
            WHERE pf.status = 'committed'""").fetchone()[0])
    finally:
        conn.close()


def local_text_bytes(db_path: str) -> int:
    """Total UTF-8 bytes of the library's page text, uncompressed. Every
    Hebrew letter is 2 bytes and normalization only removes characters, so
    `bytes // 2` is a strict upper bound on the letters a build can index --
    the same bound the Genizah preflight takes from its file size."""
    conn = _connect_ro(db_path)
    try:
        v = conn.execute('SELECT SUM(cached_text_uncompressed_len) '
                         'FROM local_pages').fetchone()[0]
        return int(v or 0)
    finally:
        conn.close()


def iter_local_records(db_path: str, *,
                       on_page: Optional[Callable[[int], None]] = None,
                       cancel_check: Optional[Callable[[], bool]] = None,
                       conn: Optional[sqlite3.Connection] = None
                       ) -> Iterator[tuple]:
    """Yield (record_id, text) for every committed LOCAL page, in
    (sys_id, page) order, splitting an over-budget page into segments.

    A page with no cached text (a pre-Phase-97 row, re-extracted only by a
    LOCAL re-index) is skipped: re-extracting a source file is the LOCAL
    indexer's job, never this reader's. `on_page(n_pages_read)` reports
    progress; `cancel_check` is polled every CANCEL_CHECK_ROWS pages, so a
    cancel lands within seconds even while the builder is still consuming.
    `conn` is an `open_local_snapshot` the caller owns and closes.
    """
    from shared.local_indexer import _make_full_header, decompress_cached_text

    own = conn is None
    if own:
        conn = _connect_ro(db_path)
    n = 0
    skipped = 0
    try:
        for row in conn.execute(_PAGES_SQL):
            n += 1
            if n % CANCEL_CHECK_ROWS == 0:
                if cancel_check is not None and cancel_check():
                    raise BuildCancelled('reading My Library cancelled')
                if on_page is not None:
                    on_page(n)
            blob = row['cached_text']
            if blob is None:
                skipped += 1
                continue
            if row['cached_text_codec'] == 'zstd':
                text = decompress_cached_text(blob)
            else:
                text = (blob.decode('utf-8', errors='replace')
                        if isinstance(blob, (bytes, bytearray)) else str(blob))
            header = _make_full_header(row['sys_id'], row['page_num'],
                                       row['file_id'])
            pieces = segment_text(text)
            if len(pieces) == 1:
                yield header, text
            else:
                for i, piece in enumerate(pieces):
                    yield segment_record_id(header, i), piece
        if on_page is not None:
            on_page(n)
    finally:
        if own:
            conn.close()
        if skipped:
            logger.info('My Library letter-level build: %d pages had no '
                        'cached text and were skipped', skipped)


def write_corpus_signature(db_path: str, out_path: str,
                           cancel_check: Optional[Callable[[], bool]] = None,
                           conn: Optional[sqlite3.Connection] = None
                           ) -> str:
    """Write a small file that changes whenever the LOCAL corpus does, and
    return its path.

    The passage lifecycle proves an artifact matches its corpus by hashing
    the corpus FILES (`shared/passage_corpus.py::source_manifest`). Hashing
    the multi-GB SQLite sidecar would also change on any unrelated write to
    it (a scan-run row, a WAL checkpoint), so the build and the staleness
    check fingerprint this summary instead: every committed file's path,
    mtime and size, and every page's text length and extraction version.

    Both halves are read from one snapshot: `conn` when the caller passes
    its own `open_local_snapshot` (and closes it), else a fresh one.
    """
    own = conn is None
    if own:
        conn = open_local_snapshot(db_path)
    h = hashlib.sha256()
    try:
        for i, row in enumerate(conn.execute("""
                SELECT pf.sys_id, pf.filepath, pf.mtime, pf.size
                FROM processed_files pf
                WHERE pf.status = 'committed'
                ORDER BY pf.sys_id""")):
            if i % CANCEL_CHECK_ROWS == 0 and cancel_check and cancel_check():
                raise BuildCancelled('My Library signature cancelled')
            h.update(f'F|{row[0]}|{row[1]}|{row[2]}|{row[3]}\n'.encode('utf-8'))
        for i, row in enumerate(conn.execute("""
                SELECT sys_id, page_num, cached_text_uncompressed_len,
                       extraction_format_version
                FROM local_pages ORDER BY sys_id, page_num""")):
            if i % (CANCEL_CHECK_ROWS * 10) == 0 and cancel_check and cancel_check():
                raise BuildCancelled('My Library signature cancelled')
            h.update(f'P|{row[0]}|{row[1]}|{row[2]}|{row[3]}\n'.encode('utf-8'))
    finally:
        if own:
            conn.close()
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    tmp = out_path + '.tmp'
    with open(tmp, 'w', encoding='utf-8', newline='\n') as fh:
        fh.write(h.hexdigest() + '\n')
    os.replace(tmp, out_path)
    return out_path


# ---------------------------------------------------------------------------
# Display: page text for a hit, from the LOCAL Tantivy index.
# ---------------------------------------------------------------------------

class LocalPageTextFetcher:
    """A `PageTextFetcher` (shared/passage_parallels.py) over the LOCAL
    Tantivy index, mirroring `SearchEngine.get_full_text_by_header` for the
    main one. Reads `engine.local_index` / `engine.local_searcher` on every
    call, because a My Library re-index replaces both on the engine.

    Keeps the shelfmark (file name) of each page it fetched, for the result
    rows: LOCAL rows show the file name where Genizah rows show a shelfmark.
    """

    def __init__(self, engine):
        self._engine = engine
        self._shelfmarks: dict = {}
        # The page's own locator ("p. 3", "paragraphs 21-40"), which the
        # Word export prints verbatim; '' when the index has none.
        self._locators: dict = {}

    def _fetch_doc(self, full_header: str):
        if not full_header or not _HEADER_CHARS_RE.match(full_header):
            logger.warning('LocalPageTextFetcher: rejecting a header outside '
                           'the [A-Za-z0-9_]+ character set (value withheld)')
            return None
        index = getattr(self._engine, 'local_index', None)
        searcher = getattr(self._engine, 'local_searcher', None)
        if index is None or searcher is None:
            return None
        try:
            q = index.parse_query(f'full_header:"{full_header}"',
                                  ['full_header'])
            res = searcher.search(q, 5)
            for _score, addr in res.hits:
                doc = searcher.doc(addr)
                if doc['full_header'][0] == full_header:
                    return doc
        except MemoryError:
            raise
        except Exception as e:                               # noqa: BLE001
            logger.warning('LOCAL text fetch failed for %s: %s',
                           full_header, e)
        return None

    def get_full_text_by_header(self, full_header: str) -> Optional[str]:
        base, seg = split_record_id(full_header)
        doc = self._fetch_doc(base)
        if doc is None:
            return None
        text = doc['content'][0]
        try:
            shelf = doc['shelfmark']
            self._shelfmarks[base] = shelf[0] if shelf else ''
        except (KeyError, IndexError, TypeError):
            self._shelfmarks.setdefault(base, '')
        self._locators[base] = self._first(doc, 'chunk_locator')
        if seg is None:
            return text
        pieces = segment_text(text)
        if seg >= len(pieces):
            return None      # the page changed since the build
        return pieces[seg]

    @staticmethod
    def _first(doc, field: str) -> str:
        try:
            value = doc[field]
            return str(value[0]) if value else ''
        except (KeyError, IndexError, TypeError, ValueError):
            return ''

    def chunk_locator(self, full_header: str) -> str:
        """The page's locator as the LOCAL index stores it, '' if none --
        read when the page's text was fetched, never looked up again."""
        base, _seg = split_record_id(full_header)
        return self._locators.get(base, '')

    def shelfmark(self, full_header: str) -> str:
        base, _seg = split_record_id(full_header)
        if base not in self._shelfmarks:
            doc = self._fetch_doc(base)
            shelf = ''
            if doc is not None:
                try:
                    v = doc['shelfmark']
                    shelf = v[0] if v else ''
                except (KeyError, IndexError, TypeError):
                    shelf = ''
            self._shelfmarks[base] = shelf
        return self._shelfmarks[base]


def _pattern_literals(pattern: str) -> list:
    """The literals of a `passage_parallels._match_pattern` alternation
    (`'|'.join(re.escape(lit) ...)`), recovered exactly: split on every `|`
    that is not escaped, and drop each escaping backslash."""
    out, buf, chars = [], [], iter(pattern or '')
    for ch in chars:
        if ch == '\\':
            buf.append(next(chars, ''))
        elif ch == '|':
            out.append(''.join(buf))
            buf = []
        else:
            buf.append(ch)
    if buf:
        out.append(''.join(buf))
    return [lit for lit in out if lit]


def _merge_segment_rows(rows: list, seen: dict, bucket: str) -> list:
    """One row per PAGE. A long page split into segments can match in more
    than one of them; each would otherwise come back as its own row under
    the same page header. One row stands for the page -- within a bucket the
    best-scoring one, and a page already shown in 'main' is never repeated
    in 'filtered' -- and its highlight patterns gain every other segment's
    matches, so the viewer still marks all of them on the page. `seen` maps
    a page header to `(bucket, row)` across both buckets."""
    from shared.passage_parallels import _match_pattern
    out = []
    for row in rows:
        head = row.get('raw_header', '')
        if head not in seen:
            seen[head] = (bucket, row)
            out.append(row)
            continue
        kept_bucket, kept = seen[head]
        replace = (kept_bucket == bucket
                   and float(row.get('score', 0)) > float(kept.get('score', 0)))
        for key in ('highlight_pattern', 'source_highlight_pattern'):
            merged = _match_pattern(_pattern_literals(kept.get(key, ''))
                                    + _pattern_literals(row.get(key, '')))
            kept[key] = row[key] = merged
        if replace:
            # The better segment's row takes the page's place in the list.
            kept.clear()
            kept.update(row)
    return out


def finish_local_rows(result: dict, fetcher: LocalPageTextFetcher) -> dict:
    """Give passage rows from the My Library index the shape the LOCAL chunk
    rows have (`shared/search_engine.py`'s LOCAL composition block): the
    page's own header and unique id, `src_lbl` 'LOCAL' and the file name as
    shelfmark. A segment row is reported against its page, and the segments
    of one page are merged into one row (`_merge_segment_rows`); the
    highlight a row carries is a literal pattern, so it marks the same
    letters on the whole page."""
    from shared.local_indexer import _make_uid

    for bucket in ('main', 'filtered'):
        for row in result.get(bucket) or []:
            base, seg = split_record_id(row.get('raw_header', ''))
            parts = parse_local_header(base)
            row['raw_header'] = base
            if parts is not None:
                row['uid'] = _make_uid(parts['sys_id'], parts['page'])
                # The page, for the exports: Word prints `chunk_locator`
                # verbatim and falls back to `p_num` (shared/docx_export.py).
                row['p_num'] = parts['page']
            row['src_lbl'] = 'LOCAL'
            row['shelfmark'] = fetcher.shelfmark(base)
            row['chunk_locator'] = fetcher.chunk_locator(base)
            if seg is not None:
                row['segment'] = seg
    seen: dict = {}
    for bucket in ('main', 'filtered'):
        if result.get(bucket):
            result[bucket] = _merge_segment_rows(result[bucket], seen, bucket)
    return result

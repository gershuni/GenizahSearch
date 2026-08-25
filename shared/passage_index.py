# -*- coding: utf-8 -*-
"""On-disk format and reader for the passage index.

Contract: docs/specs/passage-matching-algorithm.md sections 4, 5 and 10.

Arrangement C from the spec: the index is persisted over the CORPUS and the
query is streamed through it. That is what makes interactive retrieval
possible; the two research arrangements are O(corpus) per pass.

Files in an index directory
---------------------------
manifest.json      provenance, layout, counts, and every build parameter
gram_offsets.bin   uint64[GRAM_CODE_SPACE + 1]  -- CSR row starts
postings.bin       5 bytes per posting: page(24) | pos(16), little-endian
streams.bin        uint8 per letter, value = ord(letter) - 0x05D0, so 0..26
records.bin        RECORD_DTYPE per corpus record
record_ids.bin     concatenated utf-8 record ids, sliced by records.bin

Why 24/16 and not 25/15. Measured bounds are 948,549 records (20 bits) and a
longest record of 11,809 letters (14 bits). 24/16 leaves 16.7M records (17x)
and 65,535 positions (5.5x); 25/15 would leave 33.5M records but only 32,767
positions, which is 2.8x and would be at risk if a record grain ever
concatenated pages. Same 5-byte payload either way. The research code assumes
20-bit pages, so nothing shipped constrains this choice -- but the bit budget
HAS overflowed once before in this project's history, which is why both bounds
are asserted at build time and recorded in the manifest.

Why streams.bin exists at all. Verification needs each candidate record's
normalized stream, and re-normalizing 948K records per query is not an option.
Display is different: project_span needs ORIGINAL text plus an offset map,
neither of which is stored here, so rendering re-normalizes just the records
actually shown. See the spec's display-span contract.

Fail-closed. `open_index` returns None rather than raising on anything it does
not fully recognise, and it checks declared counts against real file sizes --
a truncated artifact is the failure mode most likely to produce plausible
wrong answers rather than an error.
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Optional

import numpy as np

from shared.passage_normalize import (
    GRAM_CODE_SPACE, HEB_MIN, K, NORMALIZER_VERSION,
)

# Bump when the on-disk layout changes in any way a reader would misread.
LAYOUT_VERSION = 1
SCHEMA_VERSION = 1

PAGE_BITS = 24
POS_BITS = 16
POSTING_BYTES = 5                      # ceil((PAGE_BITS + POS_BITS) / 8)
MAX_RECORDS = 1 << PAGE_BITS           # 16,777,216
MAX_RECORD_LETTERS = 1 << POS_BITS     # 65,536

MANIFEST_NAME = 'manifest.json'
GRAM_OFFSETS_NAME = 'gram_offsets.bin'
POSTINGS_NAME = 'postings.bin'
STREAMS_NAME = 'streams.bin'
RECORDS_NAME = 'records.bin'
RECORD_IDS_NAME = 'record_ids.bin'

RECORD_DTYPE = np.dtype([
    ('stream_off', '<u8'),
    ('n_letters', '<u4'),
    ('id_off', '<u8'),
    ('id_len', '<u2'),
    ('_pad', 'V2'),
])
assert RECORD_DTYPE.itemsize == 24, RECORD_DTYPE.itemsize


class IndexFormatError(Exception):
    """Raised by build-side helpers; the reader never propagates it."""


def require_little_endian() -> None:
    """The packed layouts are little-endian by definition, not by accident."""
    if sys.byteorder != 'little':
        raise IndexFormatError(
            'passage index layout is little-endian; this machine is '
            f'{sys.byteorder}-endian')


def pack_postings(pages: np.ndarray, positions: np.ndarray) -> np.ndarray:
    """(pages, positions) -> uint8 array of POSTING_BYTES per posting.

    Bounds are checked here rather than trusted, because an out-of-range page
    silently aliases onto a different record instead of failing.
    """
    require_little_endian()
    pages = np.asarray(pages, dtype=np.uint64)
    positions = np.asarray(positions, dtype=np.uint64)
    if pages.shape != positions.shape:
        raise IndexFormatError('pages and positions differ in length')
    if pages.size:
        if int(pages.max()) >= MAX_RECORDS:
            raise IndexFormatError(
                f'record index {int(pages.max()):,} exceeds the {PAGE_BITS}-bit '
                f'budget ({MAX_RECORDS:,}); the layout must widen')
        if int(positions.max()) >= MAX_RECORD_LETTERS:
            raise IndexFormatError(
                f'position {int(positions.max()):,} exceeds the {POS_BITS}-bit '
                f'budget ({MAX_RECORD_LETTERS:,}); the layout must widen')
    packed = (pages << np.uint64(POS_BITS)) | positions
    return packed.view(np.uint8).reshape(-1, 8)[:, :POSTING_BYTES].copy()


def unpack_postings(raw: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """uint8 POSTING_BYTES-per-posting array -> (pages, positions)."""
    require_little_endian()
    raw = np.ascontiguousarray(raw, dtype=np.uint8)
    if raw.size % POSTING_BYTES:
        raise IndexFormatError(
            f'posting blob of {raw.size} bytes is not a multiple of '
            f'{POSTING_BYTES}')
    n = raw.size // POSTING_BYTES
    wide = np.zeros((n, 8), dtype=np.uint8)
    wide[:, :POSTING_BYTES] = raw.reshape(n, POSTING_BYTES)
    packed = wide.reshape(-1).view(np.uint64)
    pages = (packed >> np.uint64(POS_BITS)).astype(np.uint32)
    positions = (packed & np.uint64(MAX_RECORD_LETTERS - 1)).astype(np.uint32)
    return pages, positions


def encode_stream(stream: str) -> np.ndarray:
    """Normalized letter stream -> uint8 letter indices (0..26)."""
    if not stream:
        return np.empty(0, dtype=np.uint8)
    a = (np.frombuffer(stream.encode('utf-16-le'), dtype=np.uint16)
         .astype(np.int32) - HEB_MIN)
    if a.size and (a.min() < 0 or a.max() > 26):
        raise IndexFormatError(
            'stream holds a character outside alef..tav; it was not produced '
            'by shared.passage_normalize')
    return a.astype(np.uint8)


def decode_stream(codes: np.ndarray) -> str:
    """uint8 letter indices -> normalized letter stream."""
    a = (np.asarray(codes, dtype=np.uint16) + np.uint16(HEB_MIN))
    return a.astype('<u2').tobytes().decode('utf-16-le')


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def layout_fingerprint() -> dict:
    """The layout facts a reader must agree with before touching a byte."""
    return {
        'schema_version': SCHEMA_VERSION,
        'layout_version': LAYOUT_VERSION,
        'normalizer_version': NORMALIZER_VERSION,
        'page_bits': PAGE_BITS,
        'pos_bits': POS_BITS,
        'posting_bytes': POSTING_BYTES,
        'gram_k': K,
        'gram_code_space': GRAM_CODE_SPACE,
        'byteorder': 'little',
    }


def write_manifest(index_dir: str, payload: dict) -> str:
    """Write manifest.json last, after every data file is closed and sized."""
    manifest = dict(payload)
    manifest['layout'] = layout_fingerprint()
    path = os.path.join(index_dir, MANIFEST_NAME)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as fh:
        json.dump(manifest, fh, ensure_ascii=False, indent=1, sort_keys=True)
    os.replace(tmp, path)
    return path


def _expected_sizes(counts: dict) -> dict:
    return {
        GRAM_OFFSETS_NAME: (GRAM_CODE_SPACE + 1) * 8,
        POSTINGS_NAME: int(counts['n_postings']) * POSTING_BYTES,
        STREAMS_NAME: int(counts['n_letters']),
        RECORDS_NAME: int(counts['n_records']) * RECORD_DTYPE.itemsize,
    }


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------

@dataclass
class PassageIndex:
    """Memory-mapped reader. Construct via `open_index`, never directly."""
    index_dir: str
    manifest: dict
    gram_offsets: np.ndarray
    postings: np.ndarray          # uint8, POSTING_BYTES per posting
    streams: np.ndarray           # uint8 letter indices
    records: np.ndarray           # RECORD_DTYPE
    record_ids: np.ndarray        # uint8 utf-8 blob

    @property
    def n_records(self) -> int:
        return int(self.records.shape[0])

    @property
    def n_postings(self) -> int:
        return int(self.postings.shape[0]) // POSTING_BYTES

    def df(self, code: int) -> int:
        """Postings for one gram code. Not distinct records -- postings."""
        if not 0 <= code < GRAM_CODE_SPACE:
            return 0
        return int(self.gram_offsets[code + 1] - self.gram_offsets[code])

    def dfs(self, codes: np.ndarray) -> np.ndarray:
        """Vectorised `df` -- the query budget needs all of them at once."""
        codes = np.asarray(codes, dtype=np.int64)
        ok = (codes >= 0) & (codes < GRAM_CODE_SPACE)
        out = np.zeros(codes.shape, dtype=np.int64)
        safe = codes[ok]
        out[ok] = (self.gram_offsets[safe + 1].astype(np.int64)
                   - self.gram_offsets[safe].astype(np.int64))
        return out

    def postings_for(self, code: int) -> tuple[np.ndarray, np.ndarray]:
        lo = int(self.gram_offsets[code])
        hi = int(self.gram_offsets[code + 1])
        if hi <= lo:
            return (np.empty(0, dtype=np.uint32),
                    np.empty(0, dtype=np.uint32))
        blob = self.postings[lo * POSTING_BYTES:hi * POSTING_BYTES]
        return unpack_postings(blob)

    def stream(self, record: int) -> str:
        r = self.records[record]
        off = int(r['stream_off'])
        return decode_stream(self.streams[off:off + int(r['n_letters'])])

    def record_id(self, record: int) -> str:
        r = self.records[record]
        off = int(r['id_off'])
        return bytes(self.record_ids[off:off + int(r['id_len'])]).decode('utf-8')


def open_index(index_dir: str) -> Optional[PassageIndex]:
    """Open an index, or return None. Never raises on a bad artifact.

    Fail-closed on: missing files, unparseable or absent manifest, any layout
    mismatch (schema, layout, normalizer, bit budgets, gram width, code space,
    byte order), and any declared count that disagrees with a real file size.
    A truncated artifact is the failure mode most likely to yield plausible
    wrong answers instead of an error, so sizes are checked rather than
    assumed.
    """
    try:
        if sys.byteorder != 'little':
            return None
        mpath = os.path.join(index_dir, MANIFEST_NAME)
        if not os.path.isfile(mpath):
            return None
        with open(mpath, encoding='utf-8') as fh:
            manifest = json.load(fh)
        if manifest.get('layout') != layout_fingerprint():
            return None
        counts = manifest.get('counts') or {}
        for key in ('n_records', 'n_letters', 'n_postings'):
            if not isinstance(counts.get(key), int) or counts[key] < 0:
                return None
        if counts['n_records'] > MAX_RECORDS:
            return None
        for name, expected in _expected_sizes(counts).items():
            path = os.path.join(index_dir, name)
            if not os.path.isfile(path) or os.path.getsize(path) != expected:
                return None
        ids_path = os.path.join(index_dir, RECORD_IDS_NAME)
        if not os.path.isfile(ids_path):
            return None

        def _map(name, dtype, count):
            """Map `count` elements of `name`, or an empty array when count is 0.

            `np.memmap` RAISES on a zero-byte file, and a zero-length section
            is not corruption: `_apply_df_cap` zeroes the histogram bucket of
            every gram over the cap, so a small or low-diversity corpus can
            legitimately produce an empty postings.bin. Such an index is
            valid and simply matches nothing.

            Guarded HERE rather than at the call sites because this is the
            THIRD time the same hazard has been patched ad hoc in this
            codebase -- `record_ids` below (guarded from the first commit,
            so the hazard was known) and `streams` in passage_builder.py --
            and each previous patch left the next call site exposed. The
            result was a zero-postings index that `diagnose_index` called
            "opens cleanly" while `open_index` returned None: exactly the
            contradiction that docstring promises never to produce.
            """
            if not count:
                return np.empty(0, dtype=dtype)
            return np.memmap(os.path.join(index_dir, name), dtype=dtype,
                             mode='r', shape=(count,))

        gram_offsets = _map(GRAM_OFFSETS_NAME, '<u8', GRAM_CODE_SPACE + 1)
        # CSR sanity: monotone, starts at 0, ends at the declared total. A
        # non-monotone offsets array would slice postings from other grams.
        if int(gram_offsets[0]) != 0:
            return None
        if int(gram_offsets[-1]) != counts['n_postings']:
            return None
        # Monotonicity is the part this comment used to promise and the code
        # did not do (PR #324 review). Endpoints and file sizes stay valid
        # under middle-of-array corruption, and `postings_for` slices
        # `postings[start:end]` unguarded: a reversed pair reads as an empty
        # posting list, and an inflated one reads a neighbouring gram's
        # postings as if they were this gram's. Both produce plausible wrong
        # matches rather than the clean fail-closed hide every other check
        # here delivers -- the worst outcome for a research tool.
        #
        # `csr_is_monotone` is chunked (~1 MB transient), so the 114 MB
        # sequential read is the only real cost -- one-off, at open(), and the
        # alternative is serving wrong spans from a corrupt artifact.
        if not csr_is_monotone(gram_offsets):
            return None
        postings = _map(POSTINGS_NAME, np.uint8,
                        counts['n_postings'] * POSTING_BYTES)
        streams = _map(STREAMS_NAME, np.uint8, counts['n_letters'])
        records = _map(RECORDS_NAME, RECORD_DTYPE, counts['n_records'])
        record_ids = _map(RECORD_IDS_NAME, np.uint8,
                          os.path.getsize(ids_path))
        return PassageIndex(index_dir=index_dir, manifest=manifest,
                            gram_offsets=gram_offsets, postings=postings,
                            streams=streams, records=records,
                            record_ids=record_ids)
    except Exception:
        return None


def diagnose_index(index_dir: str) -> str:
    """Why would `open_index(index_dir)` return None? One human sentence.

    `open_index` is deliberately silent: it returns None at a dozen points
    and logs nothing, because on the WEB path a bad artifact must hide
    cleanly rather than announce its internals. That is the right production
    behaviour and it is not changed here. But it leaves an operator running a
    CLI tool with "index failed to open" and no next step, which is how this
    function came to exist (2026-08-24).

    Read-only and never raises: it re-walks the same checks in the same order
    and describes the FIRST one that fails, so the answer always names an
    actionable thing (wrong directory, stale layout, truncated file). Returns
    'opens cleanly' when nothing fails -- callers should treat a passing
    diagnosis with a failing open as a bug report, not a contradiction to
    paper over.

    Kept beside `open_index` on purpose: two copies of these checks in two
    files would drift, and a diagnosis that describes checks the loader no
    longer makes is worse than no diagnosis.
    """
    try:
        if sys.byteorder != 'little':
            return (f'this machine is {sys.byteorder}-endian; the artifact '
                    f'format is little-endian only')
        if not os.path.isdir(index_dir):
            return f'not a directory: {os.path.abspath(index_dir)}'
        mpath = os.path.join(index_dir, MANIFEST_NAME)
        if not os.path.isfile(mpath):
            present = sorted(os.listdir(index_dir))[:12]
            return (f'no {MANIFEST_NAME} in {os.path.abspath(index_dir)} '
                    f'(contains: {", ".join(present) or "nothing"})')
        try:
            with open(mpath, encoding='utf-8') as fh:
                manifest = json.load(fh)
        except Exception as exc:
            return f'{MANIFEST_NAME} is unreadable/unparseable: {exc}'

        want = layout_fingerprint()
        got = manifest.get('layout')
        if got != want:
            if not isinstance(got, dict):
                return f'{MANIFEST_NAME} has no layout fingerprint'
            diffs = [f'{k}: artifact={got.get(k)!r} reader={v!r}'
                     for k, v in want.items() if got.get(k) != v]
            return ('layout mismatch -- the artifact was built by a different '
                    'version and must be rebuilt: ' + '; '.join(diffs))

        counts = manifest.get('counts') or {}
        for key in ('n_records', 'n_letters', 'n_postings'):
            if not isinstance(counts.get(key), int) or counts[key] < 0:
                return f'{MANIFEST_NAME} counts.{key} is missing or invalid'
        if counts['n_records'] > MAX_RECORDS:
            return (f'manifest declares {counts["n_records"]:,} records, '
                    f'above the format maximum {MAX_RECORDS:,}')
        for name, expected in _expected_sizes(counts).items():
            path = os.path.join(index_dir, name)
            if not os.path.isfile(path):
                return f'missing data file: {name}'
            actual = os.path.getsize(path)
            if actual != expected:
                return (f'{name} is {actual:,} bytes but the manifest implies '
                        f'{expected:,} -- truncated or mismatched artifact')
        if not os.path.isfile(os.path.join(index_dir, RECORD_IDS_NAME)):
            return f'missing data file: {RECORD_IDS_NAME}'

        gram_offsets = np.memmap(os.path.join(index_dir, GRAM_OFFSETS_NAME),
                                 dtype='<u8', mode='r',
                                 shape=(GRAM_CODE_SPACE + 1,))
        if int(gram_offsets[0]) != 0:
            return f'{GRAM_OFFSETS_NAME} does not start at 0 (corrupt CSR)'
        if int(gram_offsets[-1]) != counts['n_postings']:
            return (f'{GRAM_OFFSETS_NAME} ends at {int(gram_offsets[-1]):,} '
                    f'but the manifest declares {counts["n_postings"]:,} '
                    f'postings (corrupt CSR)')
        if not csr_is_monotone(gram_offsets):
            return f'{GRAM_OFFSETS_NAME} is not monotone (corrupt CSR)'
        return 'opens cleanly'
    except Exception as exc:  # never raise from a diagnostic
        return f'unexpected error while diagnosing: {exc!r}'


CSR_SCAN_CHUNK = 1 << 20


def csr_is_monotone(gram_offsets) -> bool:
    """True when `gram_offsets` is non-decreasing. Chunked, ~1 MB transient.

    ONE implementation, used by both the build/verify side and `open_index`
    (PR #324 review). The open path added an inline copy of this loop first;
    two versions of one invariant drift, and the whole point of the check is
    that it cannot be quietly weaker in the place that matters most.

    Chunked rather than `np.diff(offsets.astype(np.int64))`: the code space is
    27**5 + 1 entries, so the vectorised form allocates a 114 MB cast plus a
    114 MB difference. That cost is why the original was documented
    "build/verify side only" -- and why the open path could not simply call
    it. Chunking removes the reason for the split.
    """
    prev = None
    for start in range(0, len(gram_offsets), CSR_SCAN_CHUNK):
        block = np.asarray(gram_offsets[start:start + CSR_SCAN_CHUNK])
        if block.size == 0:
            continue
        if prev is not None and block[0] < prev:
            return False
        if block.size > 1 and bool((block[1:] < block[:-1]).any()):
            return False
        prev = block[-1]
    return True


def verify_csr_monotone(gram_offsets: np.ndarray) -> None:
    """Raising form, for the build/verify side."""
    if not csr_is_monotone(gram_offsets):
        raise IndexFormatError('gram_offsets is not monotone')

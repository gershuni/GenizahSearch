# -*- coding: utf-8 -*-
"""Builders for the passage index. Contract: passage-matching-algorithm.md.

Two constructions, one output. They must produce byte-identical artifacts, and
a test asserts that rather than trusting it:

  'scatter'  Counting-sort scatter. Pass 1 writes the streams and an EXACT
             gram histogram; the prefix sum gives every posting its final
             address; then P passes over streams.bin each scatter one
             mass-balanced slice of the output, in RAM, written out once,
             sequentially. No sort of the full key set, and no spool.

  'spool'    The conventional shape: mass-partitioned (gram, payload) keys
             spooled to disk, each partition sorted, CSR written sequentially.
             Costs scratch disk equal to 8 bytes per posting.

MEASURED, on a 60,000-record slice (51,226 indexed, 86.3M postings), P=4:

    batch_grams   scatter wall / RSS      spool wall / RSS      scratch
      1,000,000     66.3s / 630 MB          48.8s / 1.1 GB      659 MB
      4,000,000     88.2s / 820 MB          50.6s / 1.2 GB      659 MB
     16,000,000    108.4s / 1.8 GB          51.0s / 1.8 GB      659 MB

Artifacts were byte-identical in every configuration. `spool` is the default
because it is ~1.7x faster and flat in the RAM knob; `scatter` is kept and
supported because it needs no scratch disk at all, which is the constraint
that may matter on a user's machine. The choice is purely operational -- it
cannot change what gets built.

Scatter loses on time for a structural reason worth stating: it re-derives
every gram once per partition, so its cost is P x derive, and it gets slower
as P rises. That is the external review's prediction, confirmed.

A design that does NOT work, recorded so it is not re-proposed: a single
source-order pass scattering into P buffered gram ranges. A source-order scan
interleaves grams from every range, so a per-range buffer cannot APPEND -- each
record still belongs at a distinct offset inside that range's destination
region, so flushing revisits earlier regions repeatedly. That is random write
dressed up as sequential. Partitioning fixes it only because each partition's
destination slice is held whole in RAM.

Mass balance is what cures skew, not the histogram by itself. Equal code
ranges would be as skewed as Hebrew gram frequency is. Boundaries chosen by
cumulative posting mass make every slice the same size by construction; an
exact histogram only lets you MEASURE skew, and using it to place the
boundaries is what removes it.

Determinism: postings for a gram are ordered by (record index, position).
Records are consumed in corpus order and a stable sort by code preserves that
order within a batch, so the artifact is reproducible byte for byte.
"""
from __future__ import annotations

import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from typing import Callable, Iterable, Optional

import numpy as np

from shared.passage_hygiene import DROP_REASONS, page_filter
from shared.passage_index import (
    GRAM_OFFSETS_NAME, MANIFEST_NAME, MAX_RECORDS, MAX_RECORD_LETTERS,
    POSTINGS_NAME,
    POSTING_BYTES, RECORDS_NAME, RECORD_DTYPE, RECORD_IDS_NAME, STREAMS_NAME,
    BuildCancelled, IndexFormatError, encode_stream, pack_postings,
    require_little_endian, verify_csr_monotone, write_manifest,
)
from shared.passage_normalize import (
    GRAM_CODE_SPACE, K, NORMALIZER_VERSION, gram_codes, norm_stream_fast,
)

EXCLUDED_NAME = 'excluded_records.tsv'
DEFAULT_PARTITIONS = 8
# Batch by GRAMS, not records. Batching 20,000 records meant ~33.8M grams
# per batch at the corpus mean of 1,689 letters/record -- 0.8 GB across
# three uint64 arrays, doubled again by argsort copies. Measured peak RSS
# was 3.0-3.8 GB and did NOT fall as partitions rose, which is what proved
# the in-RAM output slice (51-206 MB) was never the driver. Record counts
# are the wrong unit: record length varies ~100x across this corpus.
# 1M measured best: scatter ran 66s vs 88s at 4M and 108s at 16M on a
# 60K-record slice (smaller arrays stay in cache), and peak RSS fell
# from 1.8 GB to 630 MB. spool is flat in this knob at ~50s.
DEFAULT_BATCH_GRAMS = 1_000_000

# Pass 1's own progress print is every 100k records; measured pass 1 is ~347s
# for 948,549 records, so 100k granularity is ~37s of cancel lag -- too long
# for the desktop close path to wait on. This is a separate, finer cadence
# checked purely for cancellation, not print traffic.
CANCEL_CHECK_RECORDS = 10_000


@dataclass
class BuildStats:
    construction: str = ''
    n_records_seen: int = 0
    n_records_indexed: int = 0
    n_letters: int = 0
    n_postings: int = 0
    max_record_letters: int = 0
    excluded: dict = field(default_factory=dict)
    df_capped_codes: int = 0
    df_capped_postings: int = 0
    distinct_codes: int = 0
    partitions: int = 0
    stride: int = 1
    df_cap: Optional[int] = None
    seconds_pass1: float = 0.0
    seconds_pass2: float = 0.0
    seconds_total: float = 0.0
    peak_slice_bytes: int = 0
    scratch_bytes: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


def _noop(*_a, **_k) -> None:
    pass


def _check_cancel(cancel_check: Optional[Callable[[], bool]]) -> None:
    if cancel_check is not None and cancel_check():
        raise BuildCancelled('passage index build cancelled')


def check_free_space(index_dir: str, needed_bytes: int) -> None:
    """Refuse to start a build that cannot finish. Cheap, and the failure it
    prevents is a half-written multi-GB artifact."""
    os.makedirs(index_dir, exist_ok=True)
    free = shutil.disk_usage(index_dir).free
    if free < needed_bytes:
        raise IndexFormatError(
            f'need ~{needed_bytes / 1e9:.1f} GB free in {index_dir}, '
            f'have {free / 1e9:.1f} GB')


def estimate_artifact_bytes(n_letters: int, stride: int = 1) -> int:
    """Rough total artifact size, for the free-space preflight."""
    postings = max(0, n_letters - K + 1) // max(1, stride)
    return int(postings * POSTING_BYTES
               + (GRAM_CODE_SPACE + 1) * 8
               + n_letters
               + 64 * (1 << 20))


def _pass1(records: Iterable, index_dir: str, *, stride: int,
           apply_hygiene: bool, progress: Callable,
           cancel_check: Optional[Callable[[], bool]] = None) -> tuple:
    """Write streams/records/ids plus the EXACT gram histogram.

    `hist` is uint32: the whole corpus yields ~599M postings, far under a
    uint32 per-code ceiling, and the total is asserted against the emitted
    count before it is trusted.
    """
    require_little_endian()
    hist = np.zeros(GRAM_CODE_SPACE, dtype=np.uint32)
    stats = BuildStats(stride=stride)
    excluded: dict = {r: 0 for r in DROP_REASONS}
    excluded['below_gram_width'] = 0

    rec_rows: list = []
    id_blob = bytearray()
    stream_off = 0
    t0 = time.time()
    pending: list = []
    pending_n = 0

    def _flush_hist():
        nonlocal pending, pending_n
        if not pending:
            return
        codes = np.concatenate(pending)
        pending = []
        pending_n = 0
        if codes.size:
            u, c = np.unique(codes, return_counts=True)
            hist[u] += c.astype(np.uint32)

    streams_path = os.path.join(index_dir, STREAMS_NAME)
    excl_path = os.path.join(index_dir, EXCLUDED_NAME)
    with open(streams_path, 'wb') as sfh, \
            open(excl_path, 'w', encoding='utf-8', newline='\n') as xfh:
        xfh.write('record_id\treason\n')
        for record_id, text in records:
            stats.n_records_seen += 1
            # Checked on every record SEEN, before either `continue` below --
            # a corpus that is mostly or entirely filtered by hygiene or
            # below_gram_width must still be cancellable; gating this on
            # n_records_indexed instead would let such a corpus run pass 1 to
            # completion ignoring Cancel.
            if stats.n_records_seen % CANCEL_CHECK_RECORDS == 0:
                _check_cancel(cancel_check)
            if apply_hygiene:
                reason = page_filter(text)
                if reason is not None:
                    excluded[reason] += 1
                    xfh.write(f'{record_id}\t{reason}\n')
                    continue
            stream = norm_stream_fast(text)
            n_let = len(stream)
            if n_let < K:
                excluded['below_gram_width'] += 1
                xfh.write(f'{record_id}\tbelow_gram_width\n')
                continue
            if n_let >= MAX_RECORD_LETTERS:
                raise IndexFormatError(
                    f'record {record_id} has {n_let:,} letters, at or over the '
                    f'{MAX_RECORD_LETTERS:,} position budget; widen the layout')
            if stats.n_records_indexed >= MAX_RECORDS:
                raise IndexFormatError(
                    f'record count reached the {MAX_RECORDS:,} budget; '
                    f'widen the layout')

            sfh.write(encode_stream(stream).tobytes())
            id_bytes = record_id.encode('utf-8')
            rec_rows.append((stream_off, n_let, len(id_blob), len(id_bytes)))
            id_blob += id_bytes

            codes = gram_codes(stream)
            if stride > 1:
                codes = codes[::stride]
            pending.append(codes.astype(np.int64))
            pending_n += int(codes.size)
            stats.n_postings += int(codes.size)
            stats.n_letters += n_let
            stats.max_record_letters = max(stats.max_record_letters, n_let)
            stream_off += n_let
            stats.n_records_indexed += 1

            if pending_n >= 8_000_000:
                _flush_hist()
            if stats.n_records_seen % 100_000 == 0:
                progress('pass1', stats.n_records_seen,
                         stats.n_records_indexed, time.time() - t0)
        _flush_hist()

    recs = np.zeros(len(rec_rows), dtype=RECORD_DTYPE)
    for i, row in enumerate(rec_rows):
        recs[i] = (row[0], row[1], row[2], row[3], b'\x00\x00')
    recs.tofile(os.path.join(index_dir, RECORDS_NAME))
    with open(os.path.join(index_dir, RECORD_IDS_NAME), 'wb') as fh:
        fh.write(bytes(id_blob))

    total = int(hist.sum(dtype=np.int64))
    if total != stats.n_postings:
        raise IndexFormatError(
            f'histogram totals {total:,} but {stats.n_postings:,} postings '
            f'were emitted -- the two passes would disagree')
    stats.excluded = {k: v for k, v in excluded.items() if v}
    stats.distinct_codes = int(np.count_nonzero(hist))
    stats.seconds_pass1 = round(time.time() - t0, 2)
    return hist, stats


def _apply_df_cap(hist: np.ndarray, df_cap: Optional[int], stats: BuildStats):
    """Zero every code over the cap, and COUNT what that removed.

    This is the one place a silent recall loss could hide: the research
    matcher's raw-posting cap dropped whole gram groups and made matching
    non-monotonic in corpus size, deleting 103 live identifications before
    anyone noticed. So the cap reports both the codes and the postings it
    removed, into the manifest.
    """
    if not df_cap or df_cap <= 0:
        return hist
    over = hist > df_cap
    stats.df_capped_codes = int(over.sum())
    stats.df_capped_postings = int(hist[over].sum(dtype=np.int64))
    hist = hist.copy()
    hist[over] = 0
    return hist


def _csr_offsets(hist: np.ndarray) -> np.ndarray:
    offsets = np.zeros(GRAM_CODE_SPACE + 1, dtype=np.uint64)
    np.cumsum(hist, dtype=np.uint64, out=offsets[1:])
    return offsets


def _mass_partitions(offsets: np.ndarray, n_parts: int) -> list:
    """Code-range boundaries chosen by cumulative posting mass.

    Returns [(code_lo, code_hi, posting_lo, posting_hi), ...]. Ranges are
    contiguous and disjoint, so slices concatenate in code order and the
    output needs no merge.
    """
    total = int(offsets[-1])
    if total == 0:
        return [(0, GRAM_CODE_SPACE, 0, 0)]
    n_parts = max(1, int(n_parts))
    targets = [int(total * (i + 1) / n_parts) for i in range(n_parts)]
    bounds: list = []
    lo_code = 0
    for t in targets:
        hi_code = int(np.searchsorted(offsets, t, side='left'))
        hi_code = min(max(hi_code, lo_code), GRAM_CODE_SPACE)
        if hi_code <= lo_code and lo_code < GRAM_CODE_SPACE:
            continue
        bounds.append((lo_code, hi_code,
                       int(offsets[lo_code]), int(offsets[hi_code])))
        lo_code = hi_code
    if lo_code < GRAM_CODE_SPACE:
        bounds.append((lo_code, GRAM_CODE_SPACE,
                       int(offsets[lo_code]), total))
    return [b for b in bounds if b[1] > b[0]]


def codes_from_letter_indices(chunk: np.ndarray) -> np.ndarray:
    """Base-27 gram codes straight from stored letter indices (0..26).

    Arithmetically identical to gram_codes() on the decoded stream, without a
    decode/encode round trip. Parity with gram_codes() is asserted by test --
    if these two ever diverge, pass 2 writes postings at addresses pass 1
    reserved for different grams, and nothing else would catch it.
    """
    a = np.asarray(chunk, dtype=np.uint64)
    m = a.size - K + 1
    if m <= 0:
        return np.empty(0, dtype=np.uint64)
    c = np.zeros(m, dtype=np.uint64)
    base = np.uint64(27)
    for j in range(K):
        c = c * base + a[j:j + m]
    return c


def _iter_record_grams(recs: np.ndarray, streams: np.ndarray, *,
                       batch_grams: int, stride: int):
    """Yield (codes, record_indices, positions), batched by GRAM COUNT.

    Peak memory is what this controls, and it is the builder's real RAM knob:
    a batch costs roughly `batch_grams * 24` bytes for the three uint64 arrays,
    plus about as much again for sort copies downstream. Batching by record
    count instead let one batch reach 33.8M grams, because record length varies
    by two orders of magnitude across this corpus.

    Stride is applied PER RECORD. Striding a concatenated batch would step
    across record boundaries and index positions pass 1 never counted, so the
    histogram and the scatter would silently disagree.
    """
    codes_l, pages_l, pos_l = [], [], []
    held = 0

    def _flush():
        codes = np.concatenate(codes_l)
        pages = np.concatenate(pages_l)
        poss = np.concatenate(pos_l)
        # Stride is applied per record above; a bug that strides codes but
        # not positions (or vice versa) desyncs the three arrays by however
        # many records were in this batch, not by one -- checked HERE, at
        # batch size, so it is a small clean assertion. Left unchecked it
        # surfaces a frame deeper as a raw numpy shape error, with the
        # multi-million-element CSR/offset arrays of the caller dragged into
        # the traceback.
        assert codes.shape == pages.shape == poss.shape, (
            f'gram batch desynced: codes{codes.shape} pages{pages.shape} '
            f'positions{poss.shape}')
        return codes, pages, poss

    for ri in range(len(recs)):
        off = int(recs[ri]['stream_off'])
        n_let = int(recs[ri]['n_letters'])
        c = codes_from_letter_indices(streams[off:off + n_let])
        if not c.size:
            continue
        pos = np.arange(c.size, dtype=np.uint64)
        if stride > 1:
            c = c[::stride]
            pos = pos[::stride]
        codes_l.append(c)
        pages_l.append(np.full(c.size, ri, dtype=np.uint64))
        pos_l.append(pos)
        held += int(c.size)
        if held >= batch_grams:
            yield _flush()
            codes_l, pages_l, pos_l = [], [], []
            held = 0
    if codes_l:
        yield _flush()


def _pass2_scatter(index_dir: str, offsets: np.ndarray, recs: np.ndarray,
                   streams: np.ndarray, parts: list, *, stride: int,
                   batch_grams: int, progress: Callable,
                   stats: BuildStats,
                   cancel_check: Optional[Callable[[], bool]] = None) -> None:
    """Scatter every posting to its final CSR address, one slice at a time."""
    total = int(offsets[-1])
    postings_path = os.path.join(index_dir, POSTINGS_NAME)
    with open(postings_path, 'wb') as fh:
        fh.truncate(total * POSTING_BYTES)

    cursor = np.zeros(GRAM_CODE_SPACE, dtype=np.uint64)
    t0 = time.time()
    for pi, (c0, c1, o0, o1) in enumerate(parts):
        _check_cancel(cancel_check)
        n_slice = o1 - o0
        out = np.zeros((max(n_slice, 1), POSTING_BYTES), dtype=np.uint8)
        stats.peak_slice_bytes = max(stats.peak_slice_bytes, out.nbytes)
        cursor[c0:c1] = 0
        for codes, pages, poss in _iter_record_grams(
                recs, streams, batch_grams=batch_grams, stride=stride):
            _check_cancel(cancel_check)
            sel = (codes >= c0) & (codes < c1)
            if not sel.any():
                continue
            c = codes[sel]
            p = pages[sel]
            q = poss[sel]
            # A DF-capped code has a zero-width CSR row; its postings are
            # dropped here rather than written past their row.
            width = offsets[c + 1] - offsets[c]
            keep = width > 0
            if not keep.all():
                c, p, q = c[keep], p[keep], q[keep]
                if not c.size:
                    continue
            order = np.argsort(c, kind='stable')
            c, p, q = c[order], p[order], q[order]
            starts = np.flatnonzero(np.r_[True, c[1:] != c[:-1]])
            counts = np.diff(np.r_[starts, c.size])
            ranks = (np.arange(c.size, dtype=np.uint64)
                     - np.repeat(starts.astype(np.uint64), counts))
            u = c[starts]
            base = offsets[u] - np.uint64(o0) + cursor[u]
            dest = (np.repeat(base, counts) + ranks).astype(np.int64)
            out[dest] = pack_postings(p, q).reshape(-1, POSTING_BYTES)
            cursor[u] += counts.astype(np.uint64)

        # Integrity: every CSR row in this partition must be exactly filled.
        # Undercount means postings were dropped; overcount is impossible
        # without having already corrupted a neighbour.
        expected = offsets[c0 + 1:c1 + 1] - offsets[c0:c1]
        if not np.array_equal(cursor[c0:c1], expected):
            bad = int(np.argmax(cursor[c0:c1] != expected))
            raise IndexFormatError(
                f'partition {pi} left code {c0 + bad} with '
                f'{int(cursor[c0 + bad])} postings, expected '
                f'{int(expected[bad])}')
        if n_slice:
            with open(postings_path, 'r+b') as fh:
                fh.seek(o0 * POSTING_BYTES)
                fh.write(out[:n_slice].tobytes())
        del out
        progress('pass2', pi + 1, len(parts), time.time() - t0)
    stats.seconds_pass2 = round(time.time() - t0, 2)


def _pass2_spool(index_dir: str, offsets: np.ndarray, recs: np.ndarray,
                 streams: np.ndarray, parts: list, *, stride: int,
                 batch_grams: int, progress: Callable,
                 stats: BuildStats,
                 cancel_check: Optional[Callable[[], bool]] = None) -> None:
    """Baseline: spool packed keys per partition, sort, write CSR in order.

    Kept so the scatter path is compared against the conventional shape rather
    than asserted better than it. Costs scratch disk equal to 8 bytes per
    posting -- the thing scatter avoids.
    """
    total = int(offsets[-1])
    scratch = os.path.join(index_dir, '_spool')
    os.makedirs(scratch, exist_ok=True)
    part_of = np.zeros(len(parts) + 1, dtype=np.int64)
    edges = np.array([c1 for (_c0, c1, _o0, _o1) in parts], dtype=np.int64)
    t0 = time.time()
    handles = [open(os.path.join(scratch, f'p{i}.bin'), 'wb')
               for i in range(len(parts))]
    try:
        for codes, pages, poss in _iter_record_grams(
                recs, streams, batch_grams=batch_grams, stride=stride):
            _check_cancel(cancel_check)
            width = offsets[codes + 1] - offsets[codes]
            keep = width > 0
            if not keep.all():
                codes, pages, poss = codes[keep], pages[keep], poss[keep]
                if not codes.size:
                    continue
            # Sort key packs the gram code above the payload so that a plain
            # ascending sort within a partition IS CSR order.
            key = ((codes << np.uint64(40))
                   | (pages << np.uint64(16)) | poss)
            which = np.searchsorted(edges, codes.astype(np.int64),
                                    side='right')
            which = np.clip(which, 0, len(parts) - 1)
            order = np.argsort(which, kind='stable')
            key = key[order]
            which = which[order]
            bounds = np.searchsorted(which, np.arange(len(parts) + 1))
            for i in range(len(parts)):
                seg = key[bounds[i]:bounds[i + 1]]
                if seg.size:
                    handles[i].write(seg.tobytes())
                    part_of[i] += seg.size
    finally:
        for h in handles:
            h.close()

    stats.scratch_bytes = sum(
        os.path.getsize(os.path.join(scratch, f'p{i}.bin'))
        for i in range(len(parts)))
    postings_path = os.path.join(index_dir, POSTINGS_NAME)
    written = 0
    with open(postings_path, 'wb') as out_fh:
        for i, (_c0, _c1, o0, o1) in enumerate(parts):
            _check_cancel(cancel_check)
            path = os.path.join(scratch, f'p{i}.bin')
            keys = np.fromfile(path, dtype=np.uint64)
            keys.sort()
            if keys.size != (o1 - o0):
                raise IndexFormatError(
                    f'spool partition {i} holds {keys.size:,} keys, CSR '
                    f'reserves {o1 - o0:,}')
            pages = (keys >> np.uint64(16)) & np.uint64((1 << 24) - 1)
            poss = keys & np.uint64((1 << 16) - 1)
            out_fh.write(pack_postings(pages, poss).tobytes())
            written += keys.size
            del keys, pages, poss
            os.remove(path)
            progress('pass2', i + 1, len(parts), time.time() - t0)
    if written != total:
        raise IndexFormatError(
            f'spool wrote {written:,} postings, CSR declares {total:,}')
    shutil.rmtree(scratch, ignore_errors=True)
    stats.seconds_pass2 = round(time.time() - t0, 2)


def df_band_edges(hist: np.ndarray, n_bands: int = 6) -> list:
    """Log-DF band edges, frozen into the manifest.

    The interactive posting budget reserves capacity across DF bands, and the
    spec requires the band edges be an ARTIFACT property rather than a runtime
    guess -- otherwise the same query returns different results against two
    indexes of the same corpus.
    """
    nz = hist[hist > 0]
    if not nz.size:
        return []
    top = int(nz.max())
    edges: list = []
    e = 1
    while e < top and len(edges) < n_bands - 1:
        e *= 4
        edges.append(int(e))
    return edges


def build_index(records: Iterable, index_dir: str, *,
                construction: str = 'spool',
                partitions: int = DEFAULT_PARTITIONS,
                stride: int = 1,
                df_cap: Optional[int] = None,
                batch_grams: int = DEFAULT_BATCH_GRAMS,
                apply_hygiene: bool = True,
                source_manifest: Optional[list] = None,
                corpus_label: str = '',
                progress: Optional[Callable] = None,
                free_space_bytes: int = 0,
                cancel_check: Optional[Callable[[], bool]] = None) -> BuildStats:
    """Build a passage index into `index_dir`. Returns measured BuildStats.

    manifest.json is written LAST, so an interrupted build leaves a directory
    that `open_index` refuses rather than one it half-reads -- and any
    PRE-EXISTING manifest is deleted FIRST, before a byte of data is touched.

    That second half matters only for a rebuild in place, which is exactly the
    case the "written last" rule does not cover (PR #324 review). Overwriting
    a populated directory leaves the OLD manifest valid-looking while the data
    files beneath it are replaced: the scatter builder truncates postings.bin
    straight to its final size and fills it incrementally, so a same-sized
    rebuild that is interrupted -- or merely opened concurrently -- passes
    every check `open_index` makes and serves zeroed or half-rewritten
    postings as plausible matches. Deleting the manifest first converts that
    window into a clean refusal.

    This is a floor, not the full answer: it makes an interrupted rebuild fail
    closed, but the index is still UNAVAILABLE until the rebuild finishes. A
    staging directory plus an atomic swap is the real fix and belongs with the
    desktop build worker, which is where a rebuild-in-place actually happens.
    """
    if construction not in ('scatter', 'spool'):
        raise IndexFormatError(f'unknown construction {construction!r}')
    require_little_endian()
    os.makedirs(index_dir, exist_ok=True)
    # Preflight FIRST, invalidate second (PR #324 round 3). The first version
    # of this ordering removed the manifest and then ran the free-space check,
    # so a failed preflight -- which touches nothing -- still left a perfectly
    # good existing index unopenable. A refusal to start must leave the world
    # exactly as it found it.
    if free_space_bytes:
        check_free_space(index_dir, free_space_bytes)
    # Invalidate before touching data: a rebuild in place must not overwrite
    # data files under a manifest that still describes the old ones.
    _stale_manifest = os.path.join(index_dir, MANIFEST_NAME)
    if os.path.exists(_stale_manifest):
        os.remove(_stale_manifest)
    progress = progress or _noop
    t_start = time.time()

    hist, stats = _pass1(records, index_dir, stride=stride,
                         apply_hygiene=apply_hygiene, progress=progress,
                         cancel_check=cancel_check)
    stats.construction = construction
    stats.df_cap = df_cap
    raw_postings = stats.n_postings
    hist = _apply_df_cap(hist, df_cap, stats)
    stats.n_postings = raw_postings - stats.df_capped_postings
    bands = df_band_edges(hist)

    offsets = _csr_offsets(hist)
    verify_csr_monotone(offsets)
    if int(offsets[-1]) != stats.n_postings:
        raise IndexFormatError(
            f'CSR total {int(offsets[-1]):,} disagrees with the posting count '
            f'{stats.n_postings:,}')
    del hist

    recs = np.fromfile(os.path.join(index_dir, RECORDS_NAME),
                       dtype=RECORD_DTYPE)
    streams = np.memmap(os.path.join(index_dir, STREAMS_NAME),
                        dtype=np.uint8, mode='r',
                        shape=(stats.n_letters,)) if stats.n_letters else \
        np.empty(0, dtype=np.uint8)
    try:
        parts = _mass_partitions(offsets, partitions)
        stats.partitions = len(parts)
        runner = _pass2_scatter if construction == 'scatter' else _pass2_spool
        runner(index_dir, offsets, recs, streams, parts, stride=stride,
               batch_grams=batch_grams, progress=progress, stats=stats,
               cancel_check=cancel_check)
    finally:
        # Released on EVERY exit, not only success, and closed EXPLICITLY:
        # `del` alone drops only this frame's reference, while the pass-2
        # frame still in the propagating traceback holds `streams` as its own
        # argument. A caller that keeps the exception -- logging it, or
        # re-raising it after cleanup -- therefore keeps the mapping open, and
        # on Windows an open mapping blocks os.rename/rmtree of the staging
        # directory, which is exactly what the cancel path must do next.
        for _mapped in (streams, recs):
            _mm = getattr(_mapped, '_mmap', None)
            if _mm is not None:
                _mm.close()
        del streams
        del recs

    offsets.tofile(os.path.join(index_dir, GRAM_OFFSETS_NAME))
    del offsets

    stats.seconds_total = round(time.time() - t_start, 2)
    _check_cancel(cancel_check)
    write_manifest(index_dir, {
        'corpus': {
            'label': corpus_label,
            'sources': source_manifest or [],
        },
        'counts': {
            'n_records': stats.n_records_indexed,
            'n_letters': stats.n_letters,
            'n_postings': stats.n_postings,
            'max_record_letters': stats.max_record_letters,
            'distinct_codes': stats.distinct_codes,
        },
        'build': {
            'construction': construction,
            'partitions': stats.partitions,
            'stride': stride,
            'df_cap': df_cap,
            'df_capped_codes': stats.df_capped_codes,
            'df_capped_postings': stats.df_capped_postings,
            'batch_grams': batch_grams,
            'hygiene_applied': apply_hygiene,
            'excluded': stats.excluded,
            'normalizer_version': NORMALIZER_VERSION,
        },
        'query': {
            'df_band_edges': bands,
        },
        'timings': {
            'seconds_pass1': stats.seconds_pass1,
            'seconds_pass2': stats.seconds_pass2,
            'seconds_total': stats.seconds_total,
            'peak_slice_bytes': stats.peak_slice_bytes,
            'scratch_bytes': stats.scratch_bytes,
        },
    })
    return stats

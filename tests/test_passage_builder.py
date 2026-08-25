# -*- coding: utf-8 -*-
"""Correctness of the passage-index builders.

Three independent checks, because a posting index is exactly the kind of
artifact that fails plausibly rather than loudly -- a wrong CSR address does
not crash, it returns someone else's postings:

1. BYTE IDENTITY between the two constructions. They share only pass 1, so
   agreeing on postings.bin is strong evidence that neither is inventing
   addresses.
2. BRUTE FORCE. A dict built by walking records in order, with no CSR, no
   partitioning and no packing, is compared against what the index returns for
   every gram code it contains.
3. ORDER. Postings for a code must be sorted by (record, position). That is
   the determinism guarantee the artifact hash depends on.

Plus the accounting the builder is supposed to keep honest: hygiene
exclusions, DF-cap losses, stride, and both bit budgets.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import shared.passage_builder as passage_builder  # noqa: E402
from shared.passage_builder import (  # noqa: E402
    build_index, codes_from_letter_indices,
)
from shared.passage_index import (  # noqa: E402
    GRAM_OFFSETS_NAME, MANIFEST_NAME, MAX_RECORD_LETTERS, POSTINGS_NAME,
    RECORDS_NAME, STREAMS_NAME, BuildCancelled, IndexFormatError,
    diagnose_index, encode_stream, open_index,
)
from shared.passage_normalize import (  # noqa: E402
    gram_codes, norm_stream_fast,
)

ALEF = 0x05D0
DATA_FILES = (POSTINGS_NAME, GRAM_OFFSETS_NAME, STREAMS_NAME, RECORDS_NAME)


def _letters(seq) -> str:
    return ''.join(chr(ALEF + (v % 22)) for v in seq)


def synthetic_records(n_records: int = 40, base_len: int = 260) -> list:
    """Deterministic records that deliberately create hard cases.

    Every record repeats a shared motif, so many gram codes carry postings
    from several records; and each record repeats its own tail, so single codes
    carry several postings from ONE record -- which is the case that breaks a
    naive cursor.
    """
    motif = _letters((i * 7 + 3 for i in range(60)))
    out = []
    for r in range(n_records):
        body = _letters(((r + 1) * (i + 1) for i in range(base_len)))
        tail = _letters((i * 5 for i in range(30)))
        text = f'{body[:80]} {motif} {body[80:]} {tail} {tail} {motif}'
        out.append((f'rec{r:04d}', text))
    return out


def brute_force_postings(records, *, stride: int = 1) -> dict:
    """code -> [(record_index, position), ...] in (record, position) order."""
    table: dict = {}
    for ri, (_rid, text) in enumerate(records):
        stream = norm_stream_fast(text)
        codes = gram_codes(stream)
        positions = np.arange(codes.size)
        if stride > 1:
            codes = codes[::stride]
            positions = positions[::stride]
        for code, pos in zip(codes.tolist(), positions.tolist()):
            table.setdefault(code, []).append((ri, pos))
    return table


def file_digest(path: str) -> str:
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for block in iter(lambda: fh.read(1 << 20), b''):
            h.update(block)
    return h.hexdigest()


def digests(index_dir: str) -> dict:
    return {name: file_digest(os.path.join(index_dir, name))
            for name in DATA_FILES}


def assert_matches_brute_force(idx, records, *, stride: int = 1):
    expected = brute_force_postings(records, stride=stride)
    total = 0
    for code, want in expected.items():
        pages, positions = idx.postings_for(code)
        # Checked BEFORE any content comparison, and as plain ints/tuples --
        # a desynced pair (e.g. stride applied to one of codes/positions but
        # not the other, upstream in _iter_record_grams) must fail on a cheap
        # shape mismatch here, not fall through into zip()/list equality
        # where pytest's failure report would repr the full arrays (and, one
        # frame up, the memmapped `idx` itself) -- that repr is what has
        # produced a Windows access violation instead of a clean assertion.
        assert pages.shape == positions.shape, (
            f'code {code}: pages{pages.shape} != positions{positions.shape}')
        assert pages.shape[0] == len(want), (
            f'code {code}: {pages.shape[0]} postings, expected {len(want)}')
        got = list(zip(pages.tolist(), positions.tolist()))
        assert got == want, f'code {code}: {got[:6]} != {want[:6]}'
        total += len(want)
    assert total == idx.n_postings, (total, idx.n_postings)
    # Every code the index claims to hold must be one brute force found.
    nz = np.flatnonzero(np.diff(idx.gram_offsets.astype(np.int64)))
    assert set(nz.tolist()) == set(expected), 'code sets differ'
    return total


# ---------------------------------------------------------------------------
# The two constructions must agree, and both must match brute force.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('construction', ['scatter', 'spool'])
def test_build_matches_brute_force(tmp_path, construction):
    records = synthetic_records()
    d = str(tmp_path / construction)
    stats = build_index(records, d, construction=construction,
                        partitions=5, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None, 'freshly built index must open'
    assert stats.n_records_indexed == len(records)
    n = assert_matches_brute_force(idx, records)
    assert n > 5000, f'only {n} postings -- fixture too small to be evidence'


@pytest.mark.parametrize('partitions', [1, 2, 3, 7, 64])
def test_constructions_are_byte_identical(tmp_path, partitions):
    """Different constructions AND different partition counts, one artifact."""
    records = synthetic_records()
    ref = None
    for construction in ('scatter', 'spool'):
        d = str(tmp_path / f'{construction}-{partitions}')
        build_index(records, d, construction=construction,
                    partitions=partitions, apply_hygiene=False)
        got = digests(d)
        if ref is None:
            ref = got
        else:
            assert got == ref, f'{construction} p={partitions} diverged'


def test_partition_count_does_not_change_the_artifact(tmp_path):
    records = synthetic_records()
    ref = None
    for partitions in (1, 4, 11, 200):
        d = str(tmp_path / f'p{partitions}')
        stats = build_index(records, d, construction='scatter',
                            partitions=partitions, apply_hygiene=False)
        assert stats.partitions >= 1
        got = digests(d)
        if ref is None:
            ref = got
        else:
            assert got == ref, f'partitions={partitions} changed the artifact'


def test_postings_are_ordered_by_record_then_position(tmp_path):
    records = synthetic_records()
    d = str(tmp_path / 'ord')
    build_index(records, d, construction='scatter', partitions=4,
                apply_hygiene=False)
    idx = open_index(d)
    checked = 0
    nz = np.flatnonzero(np.diff(idx.gram_offsets.astype(np.int64)))
    for code in nz.tolist():
        pages, positions = idx.postings_for(int(code))
        if pages.size < 2:
            continue
        keys = (pages.astype(np.int64) << 20) | positions.astype(np.int64)
        assert (np.diff(keys) > 0).all(), f'code {code} out of order'
        checked += 1
    assert checked > 200, f'only {checked} multi-posting codes examined'


def test_rebuild_is_reproducible(tmp_path):
    records = synthetic_records()
    a = str(tmp_path / 'a')
    b = str(tmp_path / 'b')
    build_index(records, a, partitions=3, apply_hygiene=False)
    build_index(records, b, partitions=3, apply_hygiene=False)
    assert digests(a) == digests(b)


# ---------------------------------------------------------------------------
# Accounting the builder must keep honest.
# ---------------------------------------------------------------------------

def test_hygiene_exclusions_are_counted_and_listed(tmp_path):
    keep = synthetic_records(n_records=6)
    # Precedence matters and is pinned here: `short` is tested BEFORE
    # target_sheet and library_stamp, so a stamp page under 80 letters is
    # reported as short, not as a stamp. Both of these therefore clear 80
    # letters while staying under the 400-letter ceiling those two rules need.
    drop = [
        ('too-short', 'אבג'),
        ('stamp', 'בית הספרים הלאומי ' + 'אבגדה' * 30),
        ('card', 'סימן תוכן מחבר שנה הערות ' + 'אבגדה' * 30),
    ]
    d = str(tmp_path / 'hyg')
    stats = build_index(keep + drop, d, partitions=2, apply_hygiene=True)
    assert stats.n_records_seen == 9
    assert stats.n_records_indexed == 6
    assert stats.excluded.get('short') == 1, stats.excluded
    assert stats.excluded.get('library_stamp') == 1, stats.excluded
    assert stats.excluded.get('target_sheet') == 1, stats.excluded
    rows = open(os.path.join(d, 'excluded_records.tsv'),
                encoding='utf-8').read().splitlines()
    assert rows[0] == 'record_id\treason'
    assert len(rows) == 4, rows
    assert sum(stats.excluded.values()) == stats.n_records_seen - \
        stats.n_records_indexed


def test_df_cap_reports_exactly_what_it_removed(tmp_path):
    records = synthetic_records()
    plain = str(tmp_path / 'plain')
    capped = str(tmp_path / 'capped')
    s0 = build_index(records, plain, partitions=3, apply_hygiene=False)
    s1 = build_index(records, capped, partitions=3, apply_hygiene=False,
                     df_cap=5)
    assert s1.df_capped_codes > 0, 'cap removed nothing -- test is vacuous'
    assert s1.n_postings == s0.n_postings - s1.df_capped_postings
    idx = open_index(capped)
    assert idx.n_postings == s1.n_postings
    # No surviving code may exceed the cap, and dropped codes must be empty.
    widths = np.diff(idx.gram_offsets.astype(np.int64))
    assert widths.max() <= 5
    full = open_index(plain)
    full_widths = np.diff(full.gram_offsets.astype(np.int64))
    over = np.flatnonzero(full_widths > 5)
    assert over.size == s1.df_capped_codes
    assert widths[over].sum() == 0


@pytest.mark.parametrize('stride', [1, 2, 3])
def test_stride_matches_brute_force_and_is_applied_per_record(tmp_path, stride):
    records = synthetic_records()
    d = str(tmp_path / f's{stride}')
    stats = build_index(records, d, partitions=3, apply_hygiene=False,
                        stride=stride)
    idx = open_index(d)
    assert_matches_brute_force(idx, records, stride=stride)
    if stride > 1:
        plain = build_index(records, str(tmp_path / 'plain'), partitions=3,
                            apply_hygiene=False)
        assert stats.n_postings < plain.n_postings


# ---------------------------------------------------------------------------
# Bit budgets and refusal behaviour.
# ---------------------------------------------------------------------------

def test_over_long_record_fails_the_build_loudly(tmp_path):
    """A record past the position budget must stop the build, not alias."""
    long_text = 'אבגדה' * (MAX_RECORD_LETTERS // 5 + 10)
    with pytest.raises(IndexFormatError, match='position budget'):
        build_index([('huge', long_text)], str(tmp_path / 'over'),
                    apply_hygiene=False)


def test_unknown_construction_is_refused(tmp_path):
    with pytest.raises(IndexFormatError, match='unknown construction'):
        build_index(synthetic_records(2), str(tmp_path / 'x'),
                    construction='magic', apply_hygiene=False)


def test_missing_manifest_makes_the_index_unopenable(tmp_path):
    d = str(tmp_path / 'nomanifest')
    build_index(synthetic_records(4), d, apply_hygiene=False)
    assert open_index(d) is not None
    os.remove(os.path.join(d, 'manifest.json'))
    assert open_index(d) is None, 'manifest-less directory must not open'


def test_truncated_postings_file_is_refused(tmp_path):
    """The failure most likely to return plausible wrong answers."""
    d = str(tmp_path / 'trunc')
    build_index(synthetic_records(8), d, apply_hygiene=False)
    assert open_index(d) is not None
    path = os.path.join(d, POSTINGS_NAME)
    with open(path, 'r+b') as fh:
        fh.truncate(os.path.getsize(path) - 5)
    assert open_index(d) is None, 'truncated postings must not open'


def test_layout_mismatch_is_refused(tmp_path):
    import json
    d = str(tmp_path / 'layout')
    build_index(synthetic_records(4), d, apply_hygiene=False)
    mpath = os.path.join(d, 'manifest.json')
    m = json.load(open(mpath, encoding='utf-8'))
    m['layout']['normalizer_version'] += 1
    json.dump(m, open(mpath, 'w', encoding='utf-8'))
    assert open_index(d) is None, 'normalizer bump must invalidate the index'


def test_band_edges_are_recorded_and_ascending(tmp_path):
    d = str(tmp_path / 'bands')
    build_index(synthetic_records(), d, partitions=3, apply_hygiene=False)
    idx = open_index(d)
    edges = idx.manifest['query']['df_band_edges']
    assert edges == sorted(edges) and len(set(edges)) == len(edges)
    assert all(e > 0 for e in edges)


def test_code_reconstruction_matches_gram_codes():
    """Pass 2 re-derives codes from stored letters; if that ever diverges from
    pass 1's gram_codes, postings land at addresses reserved for other grams
    and nothing else in the suite would notice."""
    for _rid, text in synthetic_records(6):
        stream = norm_stream_fast(text)
        a = gram_codes(stream)
        b = codes_from_letter_indices(encode_stream(stream))
        assert a.shape == b.shape and (a == b).all()


# ---------------------------------------------------------------------------
# Real corpus.
# ---------------------------------------------------------------------------

TRANSCRIPTIONS = os.environ.get(
    'GENIZAH_TRANSCRIPTIONS', r'C:\GenizahSearch\Transcriptions.txt')


def _real_records(limit: int):
    if not os.path.exists(TRANSCRIPTIONS):
        pytest.skip(f'corpus absent: {TRANSCRIPTIONS}')
    import itertools

    from shared.passage_corpus import iter_records
    return list(itertools.islice(iter_records(TRANSCRIPTIONS), limit))


def test_real_corpus_slice_both_constructions_agree(tmp_path):
    """Synthetic fixtures cannot produce real Hebrew gram-frequency skew, and
    skew is exactly what mass-balanced partitioning exists to handle."""
    records = _real_records(3000)
    ref = None
    stats_by = {}
    for construction in ('scatter', 'spool'):
        d = str(tmp_path / construction)
        stats_by[construction] = build_index(
            records, d, construction=construction, partitions=6)
        got = digests(d)
        if ref is None:
            ref = got
            idx = open_index(d)
            assert idx is not None
            kept = [r for r in records
                    if r[0] not in _excluded_ids(d)]
            assert_matches_brute_force(idx, kept)
            assert idx.n_postings > 500_000, idx.n_postings
        else:
            assert got == ref, 'real-corpus artifacts diverged'
    a, b = stats_by['scatter'], stats_by['spool']
    assert a.n_postings == b.n_postings
    assert a.n_records_indexed == b.n_records_indexed
    assert a.scratch_bytes == 0, 'scatter must not spool'
    assert b.scratch_bytes > 0, 'spool baseline must actually spool'


def _excluded_ids(index_dir: str) -> set:
    path = os.path.join(index_dir, 'excluded_records.tsv')
    out = set()
    with open(path, encoding='utf-8') as fh:
        next(fh, None)
        for line in fh:
            out.add(line.split('\t', 1)[0])
    return out


def test_real_corpus_record_ids_and_streams_round_trip(tmp_path):
    records = _real_records(1200)
    d = str(tmp_path / 'rt')
    build_index(records, d, partitions=3)
    idx = open_index(d)
    dropped = _excluded_ids(d)
    kept = [(rid, txt) for rid, txt in records if rid not in dropped]
    assert idx.n_records == len(kept)
    for i, (rid, txt) in enumerate(kept):
        assert idx.record_id(i) == rid
        assert idx.stream(i) == norm_stream_fast(txt)


def test_non_monotone_csr_offsets_are_refused(tmp_path):
    """PR #324 review, P2: the comment promised monotonicity; the code checked
    only the first and last entry.

    A middle-of-array corruption leaves both endpoints valid and every file
    size unchanged, so nothing else in `open_index` notices. `postings_for`
    then slices `postings[start:end]` unguarded, so a reversed pair reads as
    an empty posting list and an inflated one reads a NEIGHBOURING gram's
    postings as this gram's -- plausible wrong matches instead of the clean
    fail-closed hide every other check here delivers.
    """
    import numpy as np

    from shared.passage_index import GRAM_OFFSETS_NAME

    d = str(tmp_path / 'csr')
    build_index(synthetic_records(8), d, apply_hygiene=False)
    assert open_index(d) is not None, 'fixture must open before corruption'

    path = os.path.join(d, GRAM_OFFSETS_NAME)
    offsets = np.fromfile(path, dtype='<u8')

    # Find a strictly increasing step somewhere in the MIDDLE and reverse it,
    # leaving offsets[0] and offsets[-1] untouched.
    steps = np.flatnonzero(np.diff(offsets[1:-1]) > 0)
    assert steps.size, 'fixture has no interior step to corrupt'
    i = int(steps[steps.size // 2]) + 1
    original = offsets[i]
    offsets[i] = offsets[i + 1] + np.uint64(1)      # break monotonicity
    assert offsets[i] != original
    assert int(offsets[0]) == 0, 'first entry must stay valid'
    offsets.tofile(path)

    assert open_index(d) is None, (
        'non-monotone CSR offsets must fail closed -- endpoints and file '
        'sizes are still valid, so this is the only check that can catch it'
    )


def test_a_rebuild_in_place_invalidates_the_old_manifest_first(tmp_path):
    """PR #324 review: "manifest written LAST" only protects a FRESH build.

    Rebuilding over a populated directory left the OLD manifest valid while
    the data files under it were replaced. The scatter builder truncates
    postings.bin straight to its final size and fills it incrementally, so a
    same-sized rebuild that is interrupted -- or merely opened concurrently --
    passed every check `open_index` makes and served zeroed or half-rewritten
    postings as plausible matches.
    """
    from shared.passage_index import MANIFEST_NAME

    d = str(tmp_path / 'rebuild')
    build_index(synthetic_records(8), d, apply_hygiene=False)
    assert open_index(d) is not None, 'fixture must open before the rebuild'

    class _Boom(RuntimeError):
        pass

    def _records_that_die_midway():
        for i, rec in enumerate(synthetic_records(8)):
            if i == 4:
                raise _Boom('interrupted mid-rebuild')
            yield rec

    with pytest.raises(_Boom):
        build_index(_records_that_die_midway(), d, apply_hygiene=False)

    assert not os.path.exists(os.path.join(d, MANIFEST_NAME)), (
        'the stale manifest survived an interrupted rebuild'
    )
    assert open_index(d) is None, (
        'an interrupted rebuild must fail closed, not open under the manifest '
        'that described the PREVIOUS contents'
    )


def test_a_failed_space_preflight_leaves_the_old_index_openable(tmp_path):
    """PR #324 round 3: the round-2 fix deleted the manifest and THEN ran the
    free-space check, so a refused rebuild -- which touches no data at all --
    still left a perfectly good existing index unopenable. A refusal to start
    must leave the world exactly as it found it."""
    d = str(tmp_path / 'preflight')
    build_index(synthetic_records(6), d, apply_hygiene=False)
    assert open_index(d) is not None

    with pytest.raises(IndexFormatError):
        build_index(synthetic_records(6), d, apply_hygiene=False,
                    free_space_bytes=10 ** 18)  # a petabyte: always refused

    assert open_index(d) is not None, (
        'the refused rebuild destroyed the manifest of the index it never '
        'touched'
    )


# ---------------------------------------------------------------------------
# diagnose_index: `open_index` is silent by design, so the explainer beside it
# must stay TRUE to it. Every case here asserts both halves -- the artifact
# really fails to open, AND the sentence names the actual cause. A diagnosis
# that drifts from the loader is worse than none.
# ---------------------------------------------------------------------------

def _healthy_index(tmp_path, name='ok'):
    d = str(tmp_path / name)
    build_index(synthetic_records(), d, partitions=3, apply_hygiene=False)
    assert open_index(d) is not None
    return d


def test_diagnose_says_opens_cleanly_for_a_healthy_index(tmp_path):
    assert diagnose_index(_healthy_index(tmp_path)) == 'opens cleanly'


def test_diagnose_names_a_missing_directory(tmp_path):
    msg = diagnose_index(str(tmp_path / 'not-there'))
    assert 'not a directory' in msg and 'not-there' in msg


def test_diagnose_names_a_directory_without_a_manifest(tmp_path):
    empty = tmp_path / 'empty'
    empty.mkdir()
    assert open_index(str(empty)) is None
    assert f'no {MANIFEST_NAME}' in diagnose_index(str(empty))


def test_diagnose_names_the_mismatched_layout_field(tmp_path):
    d = _healthy_index(tmp_path, 'stale')
    mpath = os.path.join(d, MANIFEST_NAME)
    with open(mpath, encoding='utf-8') as fh:
        manifest = json.load(fh)
    manifest['layout']['normalizer_version'] = 999
    with open(mpath, 'w', encoding='utf-8') as fh:
        json.dump(manifest, fh)
    assert open_index(d) is None
    msg = diagnose_index(d)
    assert 'layout mismatch' in msg
    assert 'normalizer_version' in msg and '999' in msg


def test_diagnose_names_the_truncated_file(tmp_path):
    d = _healthy_index(tmp_path, 'cut')
    victim = os.path.join(d, STREAMS_NAME)
    with open(victim, 'r+b') as fh:
        fh.truncate(os.path.getsize(victim) - 8)
    assert open_index(d) is None
    msg = diagnose_index(d)
    assert STREAMS_NAME in msg and 'truncated' in msg


def test_diagnose_never_raises_on_garbage(tmp_path):
    d = _healthy_index(tmp_path, 'garbage')
    with open(os.path.join(d, MANIFEST_NAME), 'w', encoding='utf-8') as fh:
        fh.write('{ not json at all')
    assert open_index(d) is None
    assert 'unparseable' in diagnose_index(d)
    # Non-paths must not blow up either -- this is a diagnostic, not a gate.
    assert isinstance(diagnose_index(''), str)


# ---------------------------------------------------------------------------
# A zero-postings index is valid, not corrupt.
# ---------------------------------------------------------------------------

def _all_capped_index(tmp_path, name='allcapped'):
    """Build a REAL index whose every gram is removed by df_cap.

    Two records that are a repeating five-letter cycle, so the corpus holds
    only five distinct 5-grams, each far above `df_cap=1`. `_apply_df_cap`
    zeroes every histogram bucket and pass 2 writes a legitimately empty
    postings.bin. Nothing here is corrupted by hand -- this is what an
    ordinary df_cap on a low-diversity corpus produces.
    """
    d = str(tmp_path / name)
    text = _letters([i % 5 for i in range(300)])
    build_index([('rec0000', text), ('rec0001', text)], d,
                partitions=1, apply_hygiene=False, df_cap=1)
    assert os.path.getsize(os.path.join(d, POSTINGS_NAME)) == 0, (
        'fixture is wrong: df_cap did not empty the postings'
    )
    return d


def test_a_zero_postings_index_opens(tmp_path):
    """`np.memmap` raises on a zero-byte file, and `open_index` guarded only
    ONE of its four mapped sections. A build whose every gram was capped away
    is a valid index that matches nothing -- it must open, not fail closed."""
    idx = open_index(_all_capped_index(tmp_path))
    assert idx is not None, 'a legitimately empty index failed to open'
    assert idx.n_records == 2
    assert len(idx.postings) == 0


def test_the_diagnosis_and_the_loader_agree_on_a_zero_postings_index(tmp_path):
    """The defect this pins: `diagnose_index` returned 'opens cleanly' while
    `open_index` returned None -- exactly the contradiction diagnose_index's
    own docstring promises never to produce, and the reason it exists."""
    d = _all_capped_index(tmp_path, 'agree')
    opens = open_index(d) is not None
    says_opens = diagnose_index(d) == 'opens cleanly'
    assert opens == says_opens, (
        f'diagnose_index says opens={says_opens} but open_index says '
        f'opens={opens}'
    )


def test_a_zero_postings_index_answers_a_query_with_no_matches(tmp_path):
    """Opening is only half of it. An index that matches nothing must SAY so,
    not raise on the first search."""
    from shared.passage_search import search_passage
    from shared.passage_policy import get_preset
    idx = open_index(_all_capped_index(tmp_path, 'query'))
    hits, _report = search_passage(idx, _letters(range(60)),
                                   get_preset('widest-40'))
    assert hits == []


def test_a_truncated_file_is_still_caught(tmp_path):
    """The zero-length guard must not swallow real truncation: a file that is
    short but NOT empty is corruption, and still has to fail closed."""
    d = _healthy_index(tmp_path, 'shortened')
    victim = os.path.join(d, POSTINGS_NAME)
    with open(victim, 'r+b') as fh:
        fh.truncate(os.path.getsize(victim) - 16)
    assert open_index(d) is None
    assert 'truncated' in diagnose_index(d)


# ---------------------------------------------------------------------------
# Phase 146 Task 2a: cancel_check plumbing.
#
# Every case below drives build_index() through its PUBLIC signature only --
# no private pass1/pass2 function is imported directly. Determinism about
# WHICH checkpoint fires comes from shaping the input rather than instrumenting
# the internals: `batch_grams` set far above the corpus's total gram count
# forces exactly ONE batch per _iter_record_grams() call, which makes every
# cancel_check() call in a build countable and orderable by hand. A record
# count under CANCEL_CHECK_RECORDS additionally guarantees pass 1 makes ZERO
# calls of its own, so the first observed call is always pass 2's.
# ---------------------------------------------------------------------------

_HUGE_BATCH = 1 << 40  # forces a single batch per partition; never spools twice


def _cancel_after(n):
    """False for the first n-1 calls, True from the n-th call on."""
    calls = {'i': 0}

    def cancel():
        calls['i'] += 1
        return calls['i'] >= n

    cancel.calls = calls
    return cancel


def _actual_partitions(tmp_path, records, construction, requested,
                       batch_grams):
    """_mass_partitions can emit a different count than requested (measured:
    3 requested, 10-record fixture, actually produced 4 -- collapsed or split
    ranges are legitimate, see test_partition_count_does_not_change_the_artifact).
    Tests that count cancel_check() calls per partition need the REAL number,
    not the request, so they probe it with an uncancelled build first."""
    d = str(tmp_path / f'probe-{construction}')
    stats = build_index(records, d, construction=construction,
                        partitions=requested, apply_hygiene=False,
                        batch_grams=batch_grams)
    shutil.rmtree(d)
    return stats.partitions


def test_cancel_check_none_is_a_noop_for_every_existing_caller(tmp_path):
    """The parameter is additive: an explicit cancel_check=None must build the
    byte-identical artifact a caller that never passes the argument at all
    gets -- for both constructions."""
    records = synthetic_records()
    for construction in ('scatter', 'spool'):
        implicit = str(tmp_path / f'{construction}-implicit')
        explicit = str(tmp_path / f'{construction}-explicit')
        build_index(records, implicit, construction=construction,
                    partitions=3, apply_hygiene=False)
        build_index(records, explicit, construction=construction,
                    partitions=3, apply_hygiene=False, cancel_check=None)
        assert digests(implicit) == digests(explicit), construction


def test_cancel_check_stops_pass1(tmp_path, monkeypatch):
    """Pass 1's cancel cadence is its OWN constant, finer than the 100k
    progress print -- proven here by shrinking it far below the fixture."""
    monkeypatch.setattr(passage_builder, 'CANCEL_CHECK_RECORDS', 3)
    records = synthetic_records(n_records=20)
    d = str(tmp_path / 'cancel-pass1')
    cancel = _cancel_after(1)
    with pytest.raises(BuildCancelled):
        passage_builder.build_index(records, d, partitions=2,
                                    apply_hygiene=False, cancel_check=cancel)
    assert cancel.calls['i'] == 1
    assert not os.path.exists(os.path.join(d, MANIFEST_NAME))
    # RECORDS_NAME is written only after pass 1's record loop runs to
    # completion (build_index.py, after the streams.bin `with` block closes).
    # Its absence is proof the cancellation landed INSIDE pass 1 -- pass 2
    # cannot even start without it, so a cancel firing on a later checkpoint
    # (e.g. the 100k progress cadence, or a pass-2 batch) would have let pass
    # 1 finish and this file would exist.
    assert not os.path.exists(os.path.join(d, RECORDS_NAME))
    # No memmap was ever opened this early -- pass 1 alone must not need the
    # try/finally release, but the directory still has to be clean to delete.
    shutil.rmtree(d)


def test_cancel_check_stops_pass1_when_every_record_is_filtered(
        tmp_path, monkeypatch):
    """Records dropped by hygiene or below_gram_width `continue` past the
    indexing work -- if the cancel check were gated on n_records_indexed
    instead of n_records_seen, a corpus that filters every record would run
    pass 1 to completion ignoring Cancel."""
    monkeypatch.setattr(passage_builder, 'CANCEL_CHECK_RECORDS', 3)
    records = [(f'short{i:04d}', 'א') for i in range(20)]  # all below_gram_width
    d = str(tmp_path / 'cancel-pass1-all-filtered')
    cancel = _cancel_after(1)
    with pytest.raises(BuildCancelled):
        passage_builder.build_index(records, d, partitions=2,
                                    apply_hygiene=False, cancel_check=cancel)
    assert cancel.calls['i'] == 1
    # records.bin is written only once pass 1's loop has run to completion, so
    # its absence is what proves the cancellation fired DURING pass 1 rather
    # than at a later checkpoint -- which is the whole claim here, since an
    # all-filtered corpus reaches every later checkpoint regardless.
    assert not os.path.exists(os.path.join(d, RECORDS_NAME))
    assert not os.path.exists(os.path.join(d, MANIFEST_NAME))
    shutil.rmtree(d)


@pytest.mark.parametrize('construction', ['scatter', 'spool'])
def test_cancel_mid_build_releases_the_memmap_for_immediate_deletion(
        tmp_path, construction):
    """The critical Windows case: a BuildCancelled raised from INSIDE pass 2
    (after streams.bin is memmapped) must not pin the staging directory.
    The caught exception is held across the delete on purpose. A caller that
    logs the cancellation keeps the traceback, the traceback keeps
    build_index's frame, and the frame keeps `streams` mapped -- which is the
    only condition under which the builder's own `del streams` is
    load-bearing. A bare `pytest.raises` would drop the traceback the instant
    the block exits and the mapping would close regardless, proving nothing."""
    records = synthetic_records(n_records=10)
    d = str(tmp_path / f'cancel-mid-{construction}')
    cancel = _cancel_after(1)  # first pass-2 call, whichever loop makes it
    with pytest.raises(BuildCancelled) as excinfo:
        build_index(records, d, construction=construction, partitions=3,
                    apply_hygiene=False, batch_grams=_HUGE_BATCH,
                    cancel_check=cancel)
    assert not os.path.exists(os.path.join(d, MANIFEST_NAME))
    assert excinfo.traceback, 'the frame chain must still be referenced here'
    shutil.rmtree(d)  # must not raise on Windows
    assert not os.path.exists(d)
    assert excinfo.value is not None  # excinfo outlives the delete


def test_cancel_in_spool_spooling_loop(tmp_path):
    """cancel_after(1) with a single-batch corpus fires on the SPOOLING loop's
    one and only iteration, before any partition file is sorted/written."""
    records = synthetic_records(n_records=10)
    d = str(tmp_path / 'cancel-spool-spooling')
    cancel = _cancel_after(1)
    with pytest.raises(BuildCancelled):
        build_index(records, d, construction='spool', partitions=3,
                    apply_hygiene=False, batch_grams=_HUGE_BATCH,
                    cancel_check=cancel)
    scratch = os.path.join(d, '_spool')
    # The spooling loop never got past its first (and only) batch, so no
    # partition's spool file was ever opened for read in the sort/write loop.
    assert os.path.isdir(scratch)
    shutil.rmtree(d)


def test_cancel_in_spool_partition_sort_write_loop(tmp_path):
    """cancel_after(2): call 1 is the spooling loop's single batch (passes),
    call 2 is the FIRST partition of the sort/write loop -- proven distinct
    from the spooling-loop case by cancelling one call later."""
    records = synthetic_records(n_records=10)
    n_parts = _actual_partitions(tmp_path, records, 'spool', 3, _HUGE_BATCH)
    d = str(tmp_path / 'cancel-spool-partition')
    cancel = _cancel_after(2)
    with pytest.raises(BuildCancelled):
        build_index(records, d, construction='spool', partitions=3,
                    apply_hygiene=False, batch_grams=_HUGE_BATCH,
                    cancel_check=cancel)
    assert cancel.calls['i'] == 2
    # Spooling completed in full (it was call 1, and passed): every
    # partition's spool file was written before the sort/write loop started.
    scratch = os.path.join(d, '_spool')
    spool_files = [f for f in os.listdir(scratch) if f.startswith('p')]
    assert len(spool_files) == n_parts, spool_files
    shutil.rmtree(d)


def test_cancel_in_scatter_partition_loop(tmp_path):
    """cancel_after(1): the OUTER per-partition loop's check fires before that
    partition's inner batch loop is entered at all."""
    records = synthetic_records(n_records=10)
    d = str(tmp_path / 'cancel-scatter-partition')
    cancel = _cancel_after(1)
    with pytest.raises(BuildCancelled):
        build_index(records, d, construction='scatter', partitions=3,
                    apply_hygiene=False, batch_grams=_HUGE_BATCH,
                    cancel_check=cancel)
    assert cancel.calls['i'] == 1
    shutil.rmtree(d)


def test_cancel_in_scatter_batch_loop(tmp_path):
    """cancel_after(2): call 1 is partition 0's outer check (passes), call 2
    is that same partition's INNER batch loop -- scatter's equivalent of
    spool's spooling loop, one call later than the outer checkpoint above."""
    records = synthetic_records(n_records=10)
    d = str(tmp_path / 'cancel-scatter-batch')
    cancel = _cancel_after(2)
    with pytest.raises(BuildCancelled):
        build_index(records, d, construction='scatter', partitions=3,
                    apply_hygiene=False, batch_grams=_HUGE_BATCH,
                    cancel_check=cancel)
    assert cancel.calls['i'] == 2
    shutil.rmtree(d)


@pytest.mark.parametrize('construction', ['spool', 'scatter'])
def test_cancel_fires_once_more_before_write_manifest(tmp_path, construction):
    """Every mid-build checkpoint has to pass for this one to be reachable at
    all -- proof that the "once more before write_manifest" checkpoint is a
    SEPARATE call, not a side effect of the last partition's own check."""
    records = synthetic_records(n_records=10)
    n_parts = _actual_partitions(tmp_path, records, construction, 3,
                                 _HUGE_BATCH)
    # spool: 1 spooling-loop batch + n_parts sort/write checks.
    # scatter: n_parts x (outer per-partition check + 1 inner batch check).
    n_calls_before_manifest = (1 + n_parts if construction == 'spool'
                               else 2 * n_parts)
    d = str(tmp_path / f'cancel-final-{construction}')
    cancel = _cancel_after(n_calls_before_manifest + 1)
    with pytest.raises(BuildCancelled):
        build_index(records, d, construction=construction, partitions=3,
                    apply_hygiene=False, batch_grams=_HUGE_BATCH,
                    cancel_check=cancel)
    assert cancel.calls['i'] == n_calls_before_manifest + 1
    # Every data file pass 2 writes is already on disk; only the manifest,
    # written last, is missing.
    assert os.path.exists(os.path.join(d, POSTINGS_NAME))
    assert os.path.exists(os.path.join(d, GRAM_OFFSETS_NAME))
    assert not os.path.exists(os.path.join(d, MANIFEST_NAME))
    shutil.rmtree(d)

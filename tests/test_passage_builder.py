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
import sys

import numpy as np
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_builder import (  # noqa: E402
    build_index, codes_from_letter_indices,
)
from shared.passage_index import (  # noqa: E402
    GRAM_OFFSETS_NAME, MANIFEST_NAME, MAX_RECORD_LETTERS, POSTINGS_NAME,
    RECORDS_NAME, STREAMS_NAME, IndexFormatError, diagnose_index,
    encode_stream, open_index,
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

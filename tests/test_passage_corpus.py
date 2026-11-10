# -*- coding: utf-8 -*-
"""Phase 146 Task 2b: cancel_check plumbing in the corpus hasher.

A full SHA-256 pass over the ~1.47 GB corpus is otherwise uninterruptible,
which would defeat both the build Cancel button and the app-close drain.
"""
from __future__ import annotations

import hashlib

import pytest

from shared.passage_corpus import sha256_file, source_manifest
from shared.passage_index import BuildCancelled


def _cancel_after(n):
    calls = {'i': 0}

    def cancel():
        calls['i'] += 1
        return calls['i'] >= n

    cancel.calls = calls
    return cancel


def test_sha256_file_matches_hashlib_with_no_cancel_check(tmp_path):
    """cancel_check omitted (and cancel_check=None) must not change the
    result -- the existing, un-cancellable caller stays byte-for-byte."""
    p = tmp_path / 'corpus.txt'
    p.write_bytes(b'abc' * 5000)
    want = hashlib.sha256(p.read_bytes()).hexdigest()
    assert sha256_file(str(p)) == want
    assert sha256_file(str(p), cancel_check=None) == want


def test_sha256_file_cancels_between_chunks(tmp_path):
    p = tmp_path / 'corpus.txt'
    p.write_bytes(b'x' * 100)
    # chunk=10 over 100 bytes -> 10 reads if uninterrupted; cancel on the 2nd
    # check proves the loop is interruptible mid-file, not just at EOF.
    cancel = _cancel_after(2)
    with pytest.raises(BuildCancelled):
        sha256_file(str(p), chunk=10, cancel_check=cancel)
    assert cancel.calls['i'] == 2


def test_sha256_file_checks_before_the_very_first_read(tmp_path):
    """An immediate cancel must fire without reading any data -- proof the
    check is not appended only after the loop already has bytes in hand."""
    p = tmp_path / 'corpus.txt'
    p.write_bytes(b'x' * 100)
    cancel = _cancel_after(1)
    with pytest.raises(BuildCancelled):
        sha256_file(str(p), chunk=10, cancel_check=cancel)
    assert cancel.calls['i'] == 1


def test_source_manifest_propagates_cancel_check(tmp_path):
    """source_manifest must thread its cancel_check into every sha256_file()
    call, not just accept the keyword and drop it."""
    a = tmp_path / 'a.txt'
    b = tmp_path / 'b.txt'
    a.write_bytes(b'a' * 100)
    b.write_bytes(b'b' * 100)
    cancel = _cancel_after(1)
    with pytest.raises(BuildCancelled):
        source_manifest([str(a), str(b)], cancel_check=cancel)


def test_source_manifest_with_no_cancel_check_is_unaffected(tmp_path):
    a = tmp_path / 'a.txt'
    a.write_bytes(b'a' * 100)
    out = source_manifest([str(a)])
    assert out == [{
        'path': 'a.txt',
        'bytes': 100,
        'sha256': hashlib.sha256(b'a' * 100).hexdigest(),
    }]

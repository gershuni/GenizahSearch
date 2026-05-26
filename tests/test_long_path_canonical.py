# -*- coding: utf-8 -*-
"""Phase 97.1 — verify _canonical_filepath applies the Windows long-path prefix.

Regression for: paths > 260 chars without `\\\\?\\` prefix fail with [WinError 3]
on `os.stat`/`GetFileAttributesEx`, even though `os.walk` enumerated them fine.
Debug session: `.planning/debug/phase-97-freeze-winerror-3.md`.
"""
import os
import sys
import tempfile

import pytest

from shared.local_sys_id import _canonical_filepath, _WIN_LONG_PATH_PREFIX


pytestmark = pytest.mark.skipif(
    sys.platform != "win32",
    reason="Long-path \\\\?\\ prefix is Windows-only behavior",
)


def _make_long_path(tmp_dir: str, target_chars: int) -> str:
    """Build a directory + filename combination that exceeds target_chars."""
    # Fill segment ~50 chars, mixing Latin + Hebrew so the test covers
    # Unicode normalisation paths the production paths exercise.
    seg = "longpath_שלום_" + "a" * 30   # ~45 chars
    cur = tmp_dir
    while len(cur) < target_chars - 80:
        cur = os.path.join(cur, seg)
        # Use abspath rather than makedirs in the long form — we'll
        # makedirs(exist_ok=True) below once.
    os.makedirs(cur, exist_ok=True)
    fname = "f_" + "b" * 60 + ".pdf"   # ~65 chars
    return os.path.join(cur, fname)


def test_canonical_filepath_prefixes_long_path(tmp_path):
    long_path = _make_long_path(str(tmp_path), target_chars=270)
    # sanity: we actually built something long
    assert len(long_path) > 260, (
        f"test fixture path is only {len(long_path)} chars — "
        f"need >260 to trigger MAX_PATH"
    )

    # Some Windows builds need the prefix to *write* a long-path file too;
    # use the canonical form for the actual write.
    canonical_write = _canonical_filepath(long_path)
    assert canonical_write.startswith(_WIN_LONG_PATH_PREFIX), (
        f"_canonical_filepath did NOT apply \\\\?\\ prefix to a "
        f"{len(canonical_write)}-char path: {canonical_write!r}"
    )

    # Write a small file via the canonical path; then `os.stat` it.
    with open(canonical_write, "wb") as f:
        f.write(b"hello\n")

    st = os.stat(canonical_write)
    assert st.st_size > 0


def test_canonical_filepath_skips_short_path(tmp_path):
    short_path = os.path.join(str(tmp_path), "short.pdf")
    canonical = _canonical_filepath(short_path)
    assert not canonical.startswith(_WIN_LONG_PATH_PREFIX), (
        f"short path should NOT be prefixed: {canonical!r}"
    )


def test_canonical_filepath_idempotent_for_already_prefixed():
    """If the input already has \\\\?\\, do not double-prefix."""
    if sys.platform != "win32":
        pytest.skip("Windows-only")
    with tempfile.TemporaryDirectory() as td:
        long_path = _make_long_path(td, target_chars=270)
        canonical_a = _canonical_filepath(long_path)
        # Feed the prefixed form back in
        canonical_b = _canonical_filepath(canonical_a)
        assert canonical_b == canonical_a, (
            f"double-prefix mismatch: {canonical_a!r} -> {canonical_b!r}"
        )
        # Specifically, no `\\?\\\?\` chain
        assert not canonical_b.startswith(_WIN_LONG_PATH_PREFIX * 2)

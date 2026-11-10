# -*- coding: utf-8 -*-
"""Phase 95 D-42: _canonical_filepath() helper normalization tests.

Tests:
  - Drive-letter casing normalization (Windows: C: == c:)
  - Separator normalization (/ == \\)
  - Relative paths become absolute
  - Missing files do not raise (strict=False)
  - UNC paths (Windows-only)
  - Junction idempotent (Windows-only, admin-gated)
"""
from __future__ import annotations

import os
import platform
import subprocess

import pytest

from shared.local_sys_id import _canonical_filepath

_IS_WINDOWS = platform.system() == "Windows"


def test_drive_letter_casing():
    """D-42: C:/x/y.pdf and c:/x/y.pdf produce the same canonical path (normcase)."""
    if not _IS_WINDOWS:
        pytest.skip("Drive-letter casing test only meaningful on Windows")
    upper = _canonical_filepath("C:/Users/x/y.pdf")
    lower = _canonical_filepath("c:/Users/x/y.pdf")
    assert upper == lower, f"Drive-letter casing mismatch: {upper!r} != {lower!r}"


def test_separator_normalization():
    """D-42: Forward slashes and backslashes resolve to the same canonical path."""
    if not _IS_WINDOWS:
        pytest.skip("Separator normalization test only meaningful on Windows")
    fwd = _canonical_filepath("C:/Users/x/y.pdf")
    bwd = _canonical_filepath("C:\\Users\\x\\y.pdf")
    assert fwd == bwd, f"Separator mismatch: {fwd!r} != {bwd!r}"


def test_relative_to_absolute():
    """D-42: Relative path is resolved to an absolute path string."""
    result = _canonical_filepath("foo.pdf")
    assert os.path.isabs(result), f"Expected absolute path, got: {result!r}"


def test_strict_false_handles_missing_files():
    """D-42: Missing files do not raise (Path.resolve(strict=False))."""
    try:
        result = _canonical_filepath("/does/not/exist/at/all.pdf")
    except FileNotFoundError:
        pytest.fail("_canonical_filepath raised FileNotFoundError on missing path")
    assert isinstance(result, str), f"Expected str, got: {type(result)}"


def test_unc_path():
    """D-42: UNC paths (\\\\server\\share\\file.pdf) normalize consistently."""
    if not _IS_WINDOWS:
        pytest.skip("UNC path test only meaningful on Windows")
    unc = _canonical_filepath("\\\\server\\share\\file.pdf")
    assert isinstance(unc, str), f"Expected str, got: {type(unc)}"
    # The share component should survive normalization
    assert "server" in unc or "share" in unc, (
        f"UNC share component lost after normalization: {unc!r}"
    )


def test_junction_idempotent(tmp_path):
    """D-42: junction-linked path and target path resolve to the same canonical string.

    Skipped if junction creation requires admin privileges or fails for any reason.
    """
    if not _IS_WINDOWS:
        pytest.skip("Junction test only meaningful on Windows")

    target_dir = tmp_path / "target"
    target_dir.mkdir()
    target_file = target_dir / "file.pdf"
    target_file.write_bytes(b"%PDF test")

    junction_dir = tmp_path / "junction_link"

    # Create junction via mklink /J (requires no admin on Windows 10+)
    try:
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(junction_dir), str(target_dir)],
            capture_output=True,
            timeout=10,
        )
        if result.returncode != 0:
            pytest.skip("junction unavailable in this CI (mklink /J failed)")
    except Exception:
        pytest.skip("junction unavailable in this CI")

    junction_file = junction_dir / "file.pdf"
    canonical_via_junction = _canonical_filepath(junction_file)
    canonical_via_target = _canonical_filepath(target_file)

    assert canonical_via_junction == canonical_via_target, (
        f"Junction and target produce different canonical paths:\n"
        f"  junction: {canonical_via_junction!r}\n"
        f"  target:   {canonical_via_target!r}"
    )

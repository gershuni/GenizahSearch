# -*- coding: utf-8 -*-
"""Phase 97 Wave B — C-05 zip-bomb defense tests for XLSX/DOCX.

Tests:
- test_zip_bomb_defense_via_monkeypatched_infolist: _check_zip_bomb rejects a forged
  600 MB uncompressed-size zip without reading any actual bytes (Codex MEDIUM #1 fixture).

Note: The main XLSX text-extraction tests land in Wave C (plan 97-03). Only the
zip-bomb pre-check is tested here because _check_zip_bomb is added in Wave B.
"""
from __future__ import annotations

import zipfile
from types import SimpleNamespace

import pytest

from shared.local_indexer import _check_zip_bomb, _MAX_UNCOMPRESSED_BYTES


def test_zip_bomb_defense_via_monkeypatched_infolist(monkeypatch, tmp_path):
    """_check_zip_bomb rejects a zip whose central-directory reports 600 MB uncompressed.

    Codex MEDIUM #1 fix: ZipInfo.file_size = 600*1024*1024 BEFORE writestr() is
    OVERWRITTEN by Python's zip writer at writestr() time with the actual byte length.
    Correct approach: create a real (small, valid) zip, then monkeypatch infolist()
    to return synthesized ZipInfo records claiming the large uncompressed size.
    """
    # Create a real (small, valid) zip so ZipFile() opens OK.
    real_zip = tmp_path / "fake.xlsx"
    with zipfile.ZipFile(real_zip, "w") as zf:
        zf.writestr("[Content_Types].xml", b"<x/>")

    # Patch infolist to return a forged record claiming 600 MB uncompressed.
    def fake_infolist(self):
        return [SimpleNamespace(file_size=600 * 1024 * 1024, compress_size=2048)]

    monkeypatch.setattr(zipfile.ZipFile, "infolist", fake_infolist)

    reason = _check_zip_bomb(str(real_zip))

    assert reason is not None, "_check_zip_bomb should return a reason string for a 600 MB claim"
    assert "uncompressed size" in reason, f"Expected 'uncompressed size' in reason, got: {reason!r}"
    assert str(_MAX_UNCOMPRESSED_BYTES) in reason, (
        f"Expected limit {_MAX_UNCOMPRESSED_BYTES} in reason, got: {reason!r}"
    )
    # Also verify the claimed size is reported
    assert str(600 * 1024 * 1024) in reason, (
        f"Expected claimed size 629145600 in reason, got: {reason!r}"
    )

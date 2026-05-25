# -*- coding: utf-8 -*-
"""Phase 97 R-03: zstd cached_text compression/decompression round-trip tests."""
from __future__ import annotations


def test_compress_roundtrip_hebrew_english():
    """compress_cached_text + decompress_cached_text round-trips Hebrew+English text."""
    from shared.local_indexer import compress_cached_text, decompress_cached_text

    original = "שלום hello עולם 12345"
    compressed, uncompressed_len = compress_cached_text(original)
    assert isinstance(compressed, bytes)
    assert len(compressed) > 0

    recovered = decompress_cached_text(compressed)
    assert recovered == original


def test_compress_returns_uncompressed_len():
    """Second tuple element equals len(text.encode('utf-8'))."""
    from shared.local_indexer import compress_cached_text

    text = "שלום world"
    compressed, uncompressed_len = compress_cached_text(text)
    assert uncompressed_len == len(text.encode("utf-8"))


def test_decompress_handles_empty_string():
    """Round-trip on empty string works without error."""
    from shared.local_indexer import compress_cached_text, decompress_cached_text

    compressed, uncompressed_len = compress_cached_text("")
    assert uncompressed_len == 0
    recovered = decompress_cached_text(compressed)
    assert recovered == ""

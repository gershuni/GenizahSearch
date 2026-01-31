# -*- coding: utf-8 -*-
"""
Canonical Text Filter - Pre-screens chunks against Bible/Mishnah/Talmud.

Loads canonical texts from Maagarim and creates a fast lookup set
to filter out common biblical/talmudic passages before expensive searches.
"""

import os
import re
import pickle
from typing import Set, Optional
from pathlib import Path

from .config import CORPORA, OUTPUT_DIR


# Patterns to identify canonical texts in Maagarim filenames
CANONICAL_PATTERNS = [
    'מחבר לא ידוע--מקרא',      # Bible
    'מחבר לא ידוע--משנה',      # Mishnah
    'מחבר לא ידוע--תלמוד',     # Talmud
]

# Cache file for the fingerprint set
CANONICAL_CACHE = os.path.join(OUTPUT_DIR, 'canonical_fingerprints.pkl')


def normalize_chunk(text: str) -> str:
    """Normalize text for comparison - remove nikud, punctuation, normalize spaces."""
    # Remove nikud and taamim
    text = re.sub(r'[\u05B0-\u05BD\u05BF-\u05C7\u0591-\u05AF]', '', text)
    # Remove punctuation and special chars
    text = re.sub(r'[^\w\s\u0590-\u05FF]', '', text)
    # Normalize whitespace
    text = ' '.join(text.split())
    return text.strip().lower()


def extract_chunks(text: str, chunk_size: int = 5) -> Set[str]:
    """Extract normalized n-word chunks from text."""
    words = text.split()
    chunks = set()

    for i in range(len(words) - chunk_size + 1):
        chunk = ' '.join(words[i:i + chunk_size])
        normalized = normalize_chunk(chunk)
        if len(normalized) > 5:  # Skip very short chunks
            chunks.add(normalized)

    return chunks


def build_canonical_fingerprints(chunk_size: int = 5, save_cache: bool = True) -> Set[str]:
    """
    Build a set of fingerprints from canonical texts (Bible, Mishnah, Talmud).

    Returns:
        Set of normalized chunk strings for fast lookup
    """
    maagarim_path = CORPORA.get('maagarim', {}).get('path', '')
    if not os.path.exists(maagarim_path):
        print(f"Warning: Maagarim path not found: {maagarim_path}")
        return set()

    fingerprints = set()
    files_processed = 0

    print("Building canonical text fingerprints...")
    print(f"Scanning: {maagarim_path}")

    for filename in os.listdir(maagarim_path):
        # Check if this is a canonical text
        is_canonical = any(pattern in filename for pattern in CANONICAL_PATTERNS)
        if not is_canonical:
            continue

        filepath = os.path.join(maagarim_path, filename)
        if not os.path.isfile(filepath):
            continue

        try:
            with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()

            # Clean the content (remove headers, markers)
            content = re.sub(r'##[^#]*##', ' ', content)  # Remove headers
            content = re.sub(r'>>\s*', ' ', content)  # Remove content markers
            content = re.sub(r'\$[^$]*\$', ' ', content)  # Remove section markers

            # Extract chunks
            chunks = extract_chunks(content, chunk_size)
            fingerprints.update(chunks)
            files_processed += 1

            if files_processed % 20 == 0:
                print(f"  Processed {files_processed} files, {len(fingerprints):,} fingerprints...")

        except Exception as e:
            print(f"  Error reading {filename}: {e}")

    print(f"\nCompleted: {files_processed} files, {len(fingerprints):,} unique fingerprints")

    # Save cache
    if save_cache:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        with open(CANONICAL_CACHE, 'wb') as f:
            pickle.dump(fingerprints, f)
        print(f"Cache saved to: {CANONICAL_CACHE}")

    return fingerprints


def load_canonical_fingerprints() -> Set[str]:
    """Load fingerprints from cache or build if not exists."""
    if os.path.exists(CANONICAL_CACHE):
        print(f"Loading canonical fingerprints from cache...")
        with open(CANONICAL_CACHE, 'rb') as f:
            fingerprints = pickle.load(f)
        print(f"Loaded {len(fingerprints):,} fingerprints")
        return fingerprints
    else:
        return build_canonical_fingerprints()


class CanonicalFilter:
    """
    Filter for pre-screening chunks against canonical texts.

    Usage:
        filter = CanonicalFilter()

        for chunk in chunks:
            if filter.is_canonical(chunk):
                continue  # Skip - it's biblical/talmudic
            # ... do expensive search ...
    """

    def __init__(self, auto_load: bool = True):
        self.fingerprints: Set[str] = set()
        self.enabled = True

        if auto_load:
            self.load()

    def load(self):
        """Load fingerprints from cache or build."""
        self.fingerprints = load_canonical_fingerprints()
        self.enabled = len(self.fingerprints) > 0

    def build(self, chunk_size: int = 5):
        """Force rebuild of fingerprints."""
        self.fingerprints = build_canonical_fingerprints(chunk_size)
        self.enabled = len(self.fingerprints) > 0

    def is_canonical(self, chunk: str) -> bool:
        """
        Check if a chunk matches canonical texts.

        Returns:
            True if the chunk is found in Bible/Mishnah/Talmud
        """
        if not self.enabled:
            return False

        normalized = normalize_chunk(chunk)
        return normalized in self.fingerprints

    def filter_chunks(self, chunks: list) -> list:
        """
        Filter a list of chunks, removing canonical ones.

        Args:
            chunks: List of chunk dictionaries with 'text' key

        Returns:
            Filtered list with canonical chunks removed
        """
        if not self.enabled:
            return chunks

        return [c for c in chunks if not self.is_canonical(c.get('text', ''))]

    def stats(self) -> dict:
        """Get filter statistics."""
        return {
            'enabled': self.enabled,
            'fingerprint_count': len(self.fingerprints),
            'cache_path': CANONICAL_CACHE,
            'cache_exists': os.path.exists(CANONICAL_CACHE),
        }


# Singleton instance
_filter_instance: Optional[CanonicalFilter] = None


def get_canonical_filter() -> CanonicalFilter:
    """Get the singleton canonical filter instance."""
    global _filter_instance
    if _filter_instance is None:
        _filter_instance = CanonicalFilter()
    return _filter_instance


# CLI for building/testing
if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'build':
        build_canonical_fingerprints()
    else:
        # Test the filter
        cf = CanonicalFilter()
        print(f"\nFilter stats: {cf.stats()}")

        # Test some chunks
        test_chunks = [
            "בראשית ברא אלהים את השמים",  # Genesis - should match
            "ואהבת את יי אלהיך",  # Deut 6:5 - should match
            "אלמעבר עאקלא צאלחא צאדקא באלטבע",  # Judeo-Arabic - should NOT match
        ]

        print("\nTest results:")
        for chunk in test_chunks:
            result = cf.is_canonical(chunk)
            print(f"  '{chunk[:40]}...' -> {'CANONICAL' if result else 'NOVEL'}")

#!/usr/bin/env python3
"""
Build Tantivy index on the server from Transcriptions.txt

Usage: python build_index.py
"""
import os
import sys

# Set up the index directory before importing genizah_core
# This ensures the portable path is used
os.makedirs(os.path.join(os.path.dirname(__file__), "Genizah_Index"), exist_ok=True)

from genizah_core import Indexer, MetadataManager, Config

def progress_callback(current, total, message=""):
    """Print progress to console."""
    if total > 0:
        pct = (current / total) * 100
        print(f"\r[{pct:5.1f}%] {message} ({current:,}/{total:,})", end="", flush=True)
    else:
        print(f"\r{message}", end="", flush=True)

def main():
    print("=" * 60)
    print("  Genizah Search Index Builder")
    print("=" * 60)
    print()

    # Show configuration
    print(f"Index directory: {Config.INDEX_DIR}")
    print(f"Transcriptions V0.8: {Config.FILE_V8}")
    print(f"Transcriptions V0.7: {Config.FILE_V7}")
    print()

    # Check for input files
    if not os.path.exists(Config.FILE_V8):
        print(f"ERROR: Transcriptions file not found: {Config.FILE_V8}")
        print("Please ensure 'Transcriptions.txt' is in the project root.")
        sys.exit(1)

    v8_size = os.path.getsize(Config.FILE_V8) / (1024 * 1024)
    print(f"Found V0.8 file: {v8_size:.1f} MB")

    if os.path.exists(Config.FILE_V7):
        v7_size = os.path.getsize(Config.FILE_V7) / (1024 * 1024)
        print(f"Found V0.7 file: {v7_size:.1f} MB")
    else:
        print("V0.7 file not found (optional, skipping)")

    print()
    print("Starting index build...")
    print()

    # Initialize metadata manager and indexer
    meta_mgr = MetadataManager()
    indexer = Indexer(meta_mgr)

    # Build the index
    try:
        indexer.create_index(progress_callback=progress_callback)
        print()
        print()
        print("=" * 60)
        print("  Index build completed successfully!")
        print("=" * 60)
        print(f"Index location: {os.path.join(Config.INDEX_DIR, 'tantivy_db')}")
    except Exception as e:
        print()
        print(f"ERROR: Index build failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Build Tantivy indexes on the server from Transcriptions.txt

Usage:
    python build_index.py          # Build both indexes
    python build_index.py main     # Build main index only
    python build_index.py lab      # Build lab index only
"""
import os
import sys

# Set up the index directory before importing genizah_core
# This ensures the portable path is used
os.makedirs(os.path.join(os.path.dirname(__file__), "Genizah_Index"), exist_ok=True)

from genizah_core import Indexer, MetadataManager, LabEngine, Config

def progress_callback(current, total, message=""):
    """Print progress to console."""
    if total > 0:
        pct = (current / total) * 100
        print(f"\r[{pct:5.1f}%] {message} ({current:,}/{total:,})", end="", flush=True)
    else:
        print(f"\r{message}", end="", flush=True)

def build_main_index():
    """Build the main Tantivy search index."""
    print("=" * 60)
    print("  Building MAIN Index (tantivy_db)")
    print("=" * 60)
    print()

    meta_mgr = MetadataManager()
    indexer = Indexer(meta_mgr)

    try:
        indexer.create_index(progress_callback=progress_callback)
        print()
        print()
        print("Main index completed!")
        print(f"Location: {os.path.join(Config.INDEX_DIR, 'tantivy_db')}")
        return True
    except Exception as e:
        print()
        print(f"ERROR: Main index build failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def build_lab_index():
    """Build the lab/parallels index."""
    print()
    print("=" * 60)
    print("  Building LAB Index (lab_index)")
    print("=" * 60)
    print()

    lab_engine = LabEngine()

    try:
        lab_engine.rebuild_lab_index(progress_callback=progress_callback)
        print()
        print()
        print("Lab index completed!")
        print(f"Location: {Config.LAB_INDEX_DIR}")
        return True
    except Exception as e:
        print()
        print(f"ERROR: Lab index build failed: {e}")
        import traceback
        traceback.print_exc()
        return False

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

    # Determine which indexes to build
    build_main = True
    build_lab = True

    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "main":
            build_lab = False
        elif arg == "lab":
            build_main = False

    success = True

    if build_main:
        if not build_main_index():
            success = False

    if build_lab:
        if not build_lab_index():
            success = False

    print()
    print("=" * 60)
    if success:
        print("  All indexes built successfully!")
    else:
        print("  Some indexes failed to build.")
    print("=" * 60)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

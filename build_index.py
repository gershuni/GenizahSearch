#!/usr/bin/env python3
"""
Build Tantivy indexes on the server from Transcriptions.txt

Usage:
    python build_index.py          # Build both indexes
    python build_index.py main     # Build main index only
    python build_index.py lab      # Build lab index only

Building the main index into a NEW folder, while the live one keeps serving
(docs/guides/DEPLOYMENT_TECHNICAL.md, "Rebuild the main index (format 2)"):
    python build_index.py main --index-dir /home/ubuntu/GenizahSearch/Genizah_Index.next \\
        --corpus-v8 Transcriptions.txt --v08-only \\
        --caches-from Genizah_Index

--index-dir moves every path the build writes (tantivy_db, browse_map.pkl, caches)
into that folder; the live Genizah_Index is not touched. --no-seams and
--no-reversed-field build today's format-1 index (no marker).
"""
import argparse
import json
import os
import sys
import time

# Set up the index directory before importing genizah_core
# This ensures the portable path is used
os.makedirs(os.path.join(os.path.dirname(__file__), "Genizah_Index"), exist_ok=True)

from genizah_core import Indexer, MetadataManager, VariantManager, LabEngine, Config


def progress_callback(current, total, message=""):
    """Print progress to console."""
    if total > 0:
        pct = (current / total) * 100
        print(f"\r[{pct:5.1f}%] {message} ({current:,}/{total:,})", end="", flush=True)
    else:
        print(f"\r{message}", end="", flush=True)

def build_main_index(seams=True, reversed_field=True, seam_words=None):
    """Build the main Tantivy search index."""
    print("=" * 60)
    print("  Building MAIN Index (tantivy_db)")
    print("=" * 60)
    print()

    meta_mgr = MetadataManager()
    indexer = Indexer(meta_mgr)

    try:
        kwargs = {'seams': seams, 'reversed_field': reversed_field}
        if seam_words:
            kwargs['seam_words'] = seam_words
        indexer.create_index(progress_callback=progress_callback, **kwargs)
        print()
        print()
        print("Main index completed!")
        print(f"Location: {os.path.join(Config.INDEX_DIR, 'tantivy_db')}")
        print("Build: " + json.dumps(indexer.last_build, ensure_ascii=False))
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

    meta_mgr = MetadataManager()
    var_mgr = VariantManager()
    lab_engine = LabEngine(meta_mgr, var_mgr)

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
    ap = argparse.ArgumentParser(description="Build the GenizahSearch Tantivy indexes.")
    ap.add_argument('which', nargs='?', default='both', choices=['both', 'main', 'lab'])
    ap.add_argument('--index-dir', help='build into this folder instead of Config.INDEX_DIR')
    ap.add_argument('--corpus-v8', help='Transcriptions.txt (default: next to the code)')
    ap.add_argument('--corpus-v7', help='AllGenizah_OLD.txt (default: next to the code)')
    ap.add_argument('--v08-only', action='store_true',
                    help='leave the V0.7 file out even if it exists (production publishes V0.8 only)')
    ap.add_argument('--caches-from',
                    help="with --index-dir: copy metadata_cache.pkl and nli_cache.pkl from this folder "
                         "(the live Genizah_Index) first; they decide the shelfmark field")
    ap.add_argument('--no-seams', action='store_true', help='no seam index (format 1 unless the field is on)')
    ap.add_argument('--no-reversed-field', action='store_true', help='no reversed-words field')
    ap.add_argument('--seam-words', type=int, default=None, help='words on each side of a seam')
    args = ap.parse_args()

    if args.index_dir:
        Config.relocate_index_dir(args.index_dir)
        if args.caches_from:
            Config.copy_shelfmark_caches(args.caches_from)
    if args.corpus_v8:
        Config.FILE_V8 = os.path.abspath(args.corpus_v8)
    if args.corpus_v7:
        Config.FILE_V7 = os.path.abspath(args.corpus_v7)
    if args.v08_only:
        Config.FILE_V7 = ''

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
    build_main = args.which in ('both', 'main')
    build_lab = args.which in ('both', 'lab')

    success = True
    started = time.time()

    if build_main:
        if not build_main_index(seams=not args.no_seams, reversed_field=not args.no_reversed_field,
                                seam_words=args.seam_words):
            success = False

    if build_lab:
        if not build_lab_index():
            success = False

    print()
    print("=" * 60)
    if success:
        print(f"  All indexes built successfully! ({time.time() - started:.0f} s)")
    else:
        print("  Some indexes failed to build.")
    print("=" * 60)

    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()

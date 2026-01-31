# -*- coding: utf-8 -*-
"""
Corpus Mapper - Main CLI Entry Point

מפה קשרים בין מאגרי טקסט לכתבי יד הגניזה
Maps connections between text corpora and Genizah manuscripts

Usage:
    python -m corpus_mapper discover [--corpus ja|maagarim|all] [--limit N]
    python -m corpus_mapper configure
    python -m corpus_mapper test [--corpus ja|maagarim] [--limit N]
    python -m corpus_mapper run [--corpus ja|maagarim|all] [--limit N]
    python -m corpus_mapper stats
"""

import argparse
import sys
import os

# Ensure parent directory is in path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def cmd_discover(args):
    """Discover special symbols in corpora."""
    from corpus_mapper.symbol_discovery import run_discovery

    corpus_ids = None if args.corpus == 'all' else [args.corpus]
    report_path = run_discovery(corpus_ids, limit=args.limit)

    print(f"\n{'='*60}")
    print("  Symbol discovery complete!")
    print(f"  Report saved to: {report_path}")
    print("  Next step: Run 'python -m corpus_mapper configure'")
    print('='*60)


def cmd_configure(args):
    """Interactive configuration of cleaning rules."""
    from corpus_mapper.interactive_config import run_configuration
    run_configuration()


def cmd_test(args):
    """Run a quick test on a few files."""
    from corpus_mapper.runner import run_test

    corpus = args.corpus if args.corpus != 'all' else 'ja'
    limit = args.limit or 3

    print(f"\nRunning test on {corpus} corpus ({limit} files)...")
    run_test(corpus, limit=limit)


def cmd_run(args):
    """Run full corpus mapping."""
    from corpus_mapper.runner import CorpusRunner

    runner = CorpusRunner(min_score=args.min_score)

    print(f"\n{'='*60}")
    print(f"  Starting corpus mapping")
    print(f"  Corpus: {args.corpus}")
    print(f"  Min score: {args.min_score}")
    if args.limit:
        print(f"  Limit: {args.limit} files")
    print('='*60)

    try:
        if args.corpus == 'all':
            runner.run_all(limit=args.limit, resume=not args.no_resume)
        else:
            runner.run_corpus(args.corpus, limit=args.limit, resume=not args.no_resume)
    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving checkpoint...")
        runner.request_stop()


def cmd_stats(args):
    """Show database statistics."""
    from corpus_mapper.runner import ResultsDatabase
    from corpus_mapper.config import RESULTS_DB

    if not os.path.exists(RESULTS_DB):
        print("No results database found. Run 'corpus_mapper run' first.")
        return

    db = ResultsDatabase()
    stats = db.get_stats()

    print(f"\n{'='*60}")
    print("  Corpus Mapping Statistics")
    print('='*60)
    print(f"\n  Total matches: {stats['total_matches']}")

    if stats['by_corpus']:
        print("\n  By corpus:")
        for corpus, info in stats['by_corpus'].items():
            print(f"    {corpus}: {info['count']} matches (avg score: {info['avg_score']:.1f})")


def cmd_export(args):
    """Export results to various formats."""
    from corpus_mapper.runner import ResultsDatabase
    from corpus_mapper.config import OUTPUT_DIR
    import sqlite3
    import json
    import csv

    db = ResultsDatabase()

    output_file = os.path.join(OUTPUT_DIR, f"corpus_matches.{args.format}")

    with sqlite3.connect(db.db_path) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('''
            SELECT * FROM corpus_matches
            ORDER BY score DESC
            LIMIT ?
        ''', (args.limit or 100000,))
        rows = [dict(row) for row in cursor.fetchall()]

    if args.format == 'json':
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
    elif args.format == 'csv':
        if rows:
            with open(output_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

    print(f"Exported {len(rows)} matches to: {output_file}")


def cmd_unique(args):
    """Export unique parallels (filtering out common biblical/rabbinic texts)."""
    from corpus_mapper.runner import ResultsDatabase
    from corpus_mapper.config import OUTPUT_DIR

    db = ResultsDatabase()

    output_file = os.path.join(OUTPUT_DIR, f"unique_parallels.txt")
    db.export_unique_parallels(
        output_file,
        max_ms_matches=args.max_ms,
        min_score=args.min_score
    )

    # Also show summary
    results = db.get_unique_parallels(args.max_ms, args.min_score, limit=10)

    print(f"\n{'='*60}")
    print(f"  UNIQUE PARALLELS (max {args.max_ms} MS matches)")
    print(f"  Filtering out common texts (biblical, rabbinic, etc.)")
    print('='*60)
    print(f"\nExported to: {output_file}")
    print(f"\nTop 10 unique parallels:")

    for i, r in enumerate(results, 1):
        print(f"\n  #{i} ({r['ms_count']} MSs, score {r['max_score']:,.0f})")
        print(f"     {r['source_text'][:60]}...")


def main():
    parser = argparse.ArgumentParser(
        description='Corpus Mapper - Map text corpora to Genizah manuscripts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Step 1: Discover symbols in your corpus
  python -m corpus_mapper discover --corpus ja --limit 10

  # Step 2: Configure cleaning rules interactively
  python -m corpus_mapper configure

  # Step 3: Test on a few files
  python -m corpus_mapper test --corpus ja --limit 3

  # Step 4: Run full mapping (can run overnight)
  python -m corpus_mapper run --corpus all

  # Check progress
  python -m corpus_mapper stats
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Commands')

    # Discover command
    discover_parser = subparsers.add_parser('discover', help='Discover special symbols in corpora')
    discover_parser.add_argument('--corpus', choices=['ja', 'maagarim', 'all'], default='all')
    discover_parser.add_argument('--limit', type=int, help='Limit files to scan')

    # Configure command
    configure_parser = subparsers.add_parser('configure', help='Configure cleaning rules interactively')

    # Test command
    test_parser = subparsers.add_parser('test', help='Quick test run')
    test_parser.add_argument('--corpus', choices=['ja', 'maagarim', 'all'], default='ja')
    test_parser.add_argument('--limit', type=int, default=3)

    # Run command
    run_parser = subparsers.add_parser('run', help='Run full corpus mapping')
    run_parser.add_argument('--corpus', choices=['ja', 'maagarim', 'all'], default='all')
    run_parser.add_argument('--limit', type=int, help='Limit files to process')
    run_parser.add_argument('--min-score', type=float, default=300, help='Minimum match score')
    run_parser.add_argument('--no-resume', action='store_true', help='Start fresh')

    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show statistics')

    # Export command
    export_parser = subparsers.add_parser('export', help='Export results')
    export_parser.add_argument('--format', choices=['json', 'csv'], default='json')
    export_parser.add_argument('--limit', type=int, help='Limit rows to export')

    # Unique parallels command
    unique_parser = subparsers.add_parser('unique', help='Export unique parallels (filter common texts)')
    unique_parser.add_argument('--max-ms', type=int, default=10,
                               help='Max MS matches (higher = more common, default 10)')
    unique_parser.add_argument('--min-score', type=int, default=5000,
                               help='Minimum score threshold (default 5000)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    commands = {
        'discover': cmd_discover,
        'configure': cmd_configure,
        'test': cmd_test,
        'run': cmd_run,
        'stats': cmd_stats,
        'export': cmd_export,
        'unique': cmd_unique,
    }

    commands[args.command](args)


if __name__ == '__main__':
    main()

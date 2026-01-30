# -*- coding: utf-8 -*-
"""
Corpus Mapper Runner - Batch processing with multiprocessing and checkpointing.

This script runs composition searches on external corpora against
the Genizah manuscript database, saving results to SQLite.

Features:
- Multiprocessing for parallel chunk searching
- Configurable chunk size and variant modes
- Integration with libraries.csv for title matching
- Checkpointing for resume capability
"""

import os
import sys
import json
import time
import sqlite3
import logging
import csv
import multiprocessing as mp
from functools import partial
from datetime import datetime
from typing import Dict, List, Any, Optional, Iterator, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ProcessPoolExecutor, as_completed

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .config import (
    CORPORA, OUTPUT_DIR, CHECKPOINTS_DIR, LOGS_DIR, RESULTS_DB,
    DEFAULT_MIN_SCORE, DEFAULT_CHUNK_SIZE, DEFAULT_BATCH_SIZE, ensure_dirs
)
from .parsers import JAParser, MaagarimParser
from .text_cleaner import get_cleaner


# ============================================================================
# Configuration - EDIT THESE FOR YOUR NEEDS
# ============================================================================

# Default settings optimized for thorough search
SEARCH_CONFIG = {
    'chunk_size': 5,           # Small chunks = more matches (was 15)
    'chunk_overlap': 2,        # Overlap between chunks
    'min_score': 500,          # Higher threshold for small chunks (was 300)
    'mode': 'variants',        # 'variants', 'variants_extended', 'variants_maximum'
    'num_workers': 4,          # Number of parallel processes
    'batch_size': 50,          # Files per checkpoint
}


# ============================================================================
# Logging
# ============================================================================

def setup_logging():
    """Configure logging to file and console."""
    ensure_dirs()
    log_file = os.path.join(LOGS_DIR, f"runner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)


# ============================================================================
# Libraries CSV Integration
# ============================================================================

class LibrariesDB:
    """Interface to libraries.csv for title/shelfmark lookups."""

    def __init__(self, csv_path: str = None):
        self.csv_path = csv_path
        self.data = {}  # sys_id -> {title, shelfmark, oxford_part_id}
        self._load()

    def _find_csv(self) -> Optional[str]:
        """Find libraries.csv in common locations."""
        candidates = [
            self.csv_path,
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'libraries.csv'),
            r'C:\GenizahSearch\libraries.csv',
            os.path.expanduser('~/GenizahSearch/libraries.csv'),
        ]
        for path in candidates:
            if path and os.path.exists(path):
                return path
        return None

    def _load(self):
        """Load libraries.csv into memory."""
        csv_path = self._find_csv()
        if not csv_path:
            logging.warning("libraries.csv not found - title matching disabled")
            return

        try:
            with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f)
                next(reader)  # Skip header

                for row in reader:
                    if not row or len(row) < 3:
                        continue

                    sys_id = ''.join(ch for ch in str(row[0]) if ch.isdigit())
                    oxford_part_id = row[1].strip() if len(row) > 1 else ''

                    # Parse shelfmarks (column 2)
                    shelfmarks = row[2].split('|') if len(row) > 2 else []
                    shelfmark = shelfmarks[0].strip() if shelfmarks else ''

                    # Title is column 6
                    title = row[6].strip() if len(row) > 6 else ''

                    self.data[sys_id] = {
                        'title': title,
                        'shelfmark': shelfmark,
                        'oxford_part_id': oxford_part_id,
                    }

            logging.info(f"Loaded {len(self.data)} records from libraries.csv")

        except Exception as e:
            logging.error(f"Failed to load libraries.csv: {e}")

    def get_info(self, sys_id: str) -> Dict[str, str]:
        """Get title and shelfmark for a system ID."""
        # Normalize sys_id
        clean_id = ''.join(ch for ch in str(sys_id) if ch.isdigit())
        return self.data.get(clean_id, {'title': '', 'shelfmark': '', 'oxford_part_id': ''})

    def match_title(self, sys_id: str, source_title: str) -> Optional[float]:
        """
        Check if source title matches the manuscript's cataloged title.
        Returns similarity score (0-1) or None if no title available.
        """
        info = self.get_info(sys_id)
        if not info['title'] or not source_title:
            return None

        # Simple word overlap matching
        title_words = set(info['title'].lower().split())
        source_words = set(source_title.lower().split())

        if not title_words or not source_words:
            return None

        overlap = len(title_words & source_words)
        max_len = max(len(title_words), len(source_words))

        return overlap / max_len if max_len > 0 else 0


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class SearchResult:
    """A single search result/match."""
    # Source info
    source_corpus: str
    source_file: str
    source_author: str
    source_title: str
    source_ref: str
    source_text: str

    # Match info
    ms_id: str
    ms_shelfmark: str
    ms_snippet: str
    ms_title: str  # From libraries.csv
    score: float
    title_match_score: float  # Similarity between source and ms titles
    match_type: str = 'parallel'


@dataclass
class ChunkTask:
    """A chunk to be searched (for multiprocessing)."""
    corpus_id: str
    file_path: str
    author: str
    title: str
    chunk_text: str
    chunk_ref: str


# ============================================================================
# Database
# ============================================================================

class ResultsDatabase:
    """SQLite database for storing search results."""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or RESULTS_DB
        ensure_dirs()
        self._init_db()

    def _init_db(self):
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS corpus_matches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_corpus TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    source_author TEXT,
                    source_title TEXT,
                    source_ref TEXT,
                    source_text TEXT,
                    ms_id TEXT NOT NULL,
                    ms_shelfmark TEXT,
                    ms_snippet TEXT,
                    ms_title TEXT,
                    score REAL NOT NULL,
                    title_match_score REAL,
                    match_type TEXT DEFAULT 'parallel',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_corpus, source_file, source_ref, ms_id)
                )
            ''')

            conn.execute('CREATE INDEX IF NOT EXISTS idx_source ON corpus_matches(source_corpus, source_file)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ms ON corpus_matches(ms_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_score ON corpus_matches(score DESC)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_title_match ON corpus_matches(title_match_score DESC)')

            # Checkpoints table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corpus_id TEXT NOT NULL,
                    last_file TEXT NOT NULL,
                    files_processed INTEGER NOT NULL,
                    total_matches INTEGER NOT NULL,
                    config_json TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(corpus_id)
                )
            ''')

            conn.commit()

    def save_results(self, results: List[SearchResult]):
        """Save a batch of results to the database."""
        if not results:
            return

        with sqlite3.connect(self.db_path) as conn:
            conn.executemany('''
                INSERT OR REPLACE INTO corpus_matches
                (source_corpus, source_file, source_author, source_title,
                 source_ref, source_text, ms_id, ms_shelfmark, ms_snippet,
                 ms_title, score, title_match_score, match_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (r.source_corpus, r.source_file, r.source_author, r.source_title,
                 r.source_ref, r.source_text, r.ms_id, r.ms_shelfmark, r.ms_snippet,
                 r.ms_title, r.score, r.title_match_score, r.match_type)
                for r in results
            ])
            conn.commit()

    def save_checkpoint(self, corpus_id: str, last_file: str, files_processed: int,
                       total_matches: int, config: dict = None):
        """Save a checkpoint for resuming."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO checkpoints
                (corpus_id, last_file, files_processed, total_matches, config_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (corpus_id, last_file, files_processed, total_matches,
                  json.dumps(config) if config else None, datetime.now()))
            conn.commit()

    def get_checkpoint(self, corpus_id: str) -> Optional[Dict[str, Any]]:
        """Get the last checkpoint for a corpus."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT last_file, files_processed, total_matches, config_json FROM checkpoints WHERE corpus_id = ?',
                (corpus_id,)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'last_file': row[0],
                    'files_processed': row[1],
                    'total_matches': row[2],
                    'config': json.loads(row[3]) if row[3] else None
                }
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM corpus_matches')
            total = cursor.fetchone()[0]

            cursor = conn.execute('''
                SELECT source_corpus, COUNT(*), AVG(score), AVG(title_match_score)
                FROM corpus_matches
                GROUP BY source_corpus
            ''')
            by_corpus = {
                row[0]: {
                    'count': row[1],
                    'avg_score': row[2],
                    'avg_title_match': row[3]
                }
                for row in cursor.fetchall()
            }

            # Top title matches
            cursor = conn.execute('''
                SELECT source_title, ms_title, ms_shelfmark, score, title_match_score
                FROM corpus_matches
                WHERE title_match_score > 0.3
                ORDER BY title_match_score DESC
                LIMIT 10
            ''')
            top_title_matches = [
                {
                    'source_title': row[0],
                    'ms_title': row[1],
                    'ms_shelfmark': row[2],
                    'score': row[3],
                    'title_match': row[4]
                }
                for row in cursor.fetchall()
            ]

        return {
            'total_matches': total,
            'by_corpus': by_corpus,
            'top_title_matches': top_title_matches
        }


# ============================================================================
# Worker Functions (for multiprocessing)
# ============================================================================

# Global search engine for worker processes
_worker_engine = None
_worker_libs_db = None


def _init_worker():
    """Initialize search engine in worker process."""
    global _worker_engine, _worker_libs_db

    from genizah_core import LabEngine
    _worker_engine = LabEngine()
    _worker_libs_db = LibrariesDB()


def _search_chunk_worker(task: Tuple, config: dict) -> List[SearchResult]:
    """
    Worker function to search a single chunk.
    Called in parallel by ProcessPoolExecutor.
    """
    global _worker_engine, _worker_libs_db

    corpus_id, file_path, author, title, chunk_text, chunk_ref = task

    try:
        results = _worker_engine.lab_composition_search(
            chunk_text,
            chunk_size=config['chunk_size'],
            mode=config['mode']
        )

        matches = []
        for item in results.get('main', []):
            score = item.get('score', 0)
            if score < config['min_score']:
                continue

            ms_id = item.get('uid', '')

            # Get title info from libraries.csv
            ms_info = _worker_libs_db.get_info(ms_id) if _worker_libs_db else {}
            ms_title = ms_info.get('title', '')

            # Calculate title match score
            title_match = _worker_libs_db.match_title(ms_id, title) if _worker_libs_db else None

            result = SearchResult(
                source_corpus=corpus_id,
                source_file=os.path.basename(file_path),
                source_author=author,
                source_title=title,
                source_ref=chunk_ref,
                source_text=chunk_text[:500],
                ms_id=ms_id,
                ms_shelfmark=item.get('raw_header', ''),
                ms_snippet=item.get('ms_snippet', '')[:500] if item.get('ms_snippet') else '',
                ms_title=ms_title,
                score=score,
                title_match_score=title_match or 0.0,
            )
            matches.append(result)

        return matches

    except Exception as e:
        logging.warning(f"Worker error on chunk: {e}")
        return []


# ============================================================================
# Main Runner
# ============================================================================

class CorpusRunner:
    """Main runner for batch corpus searching with multiprocessing."""

    def __init__(self, config: dict = None):
        self.config = config or SEARCH_CONFIG.copy()
        self.db = ResultsDatabase()
        self.libs_db = LibrariesDB()
        self.logger = setup_logging()
        self._stop_requested = False

        self.logger.info(f"Runner config: chunk_size={self.config['chunk_size']}, "
                        f"min_score={self.config['min_score']}, "
                        f"mode={self.config['mode']}, "
                        f"workers={self.config['num_workers']}")

    def request_stop(self):
        """Request graceful stop at next checkpoint."""
        self._stop_requested = True
        self.logger.info("Stop requested - will save checkpoint and exit")

    def _collect_chunks(self, doc, corpus_id: str) -> List[Tuple]:
        """Collect all chunks from a document for parallel processing."""
        chunks = []

        for chunk in doc.iter_chunks(
            chunk_size=self.config['chunk_size'],
            overlap=self.config.get('chunk_overlap', 2)
        ):
            # Build source reference
            if corpus_id == 'ja':
                chunk_ref = f"p.{chunk.get('page', '?')}, lines {chunk.get('start_line', '?')}-{chunk.get('end_line', '?')}"
            else:
                chunk_ref = f"section {chunk.get('section_idx', '?')}: {chunk.get('header', '')[:50]}"

            task = (
                corpus_id,
                doc.file_path,
                getattr(doc, 'author', ''),
                getattr(doc, 'title', getattr(doc, 'composition', '')),
                chunk['text'],
                chunk_ref
            )
            chunks.append(task)

        return chunks

    def run_corpus(self, corpus_id: str, limit: int = None, resume: bool = True):
        """
        Run search on an entire corpus using multiprocessing.
        """
        corpus_config = CORPORA.get(corpus_id)
        if not corpus_config:
            raise ValueError(f"Unknown corpus: {corpus_id}")

        self.logger.info(f"Starting corpus: {corpus_config['name']}")

        # Get parser
        if corpus_id == 'ja':
            parser = JAParser()
        else:
            parser = MaagarimParser()

        # Check for resume
        start_after = None
        files_processed = 0
        total_matches = 0

        if resume:
            checkpoint = self.db.get_checkpoint(corpus_id)
            if checkpoint:
                start_after = checkpoint['last_file']
                files_processed = checkpoint['files_processed']
                total_matches = checkpoint['total_matches']
                self.logger.info(f"Resuming from checkpoint: {files_processed} files processed")

        # Process files
        pending_results = []
        skip_until_found = start_after is not None
        last_file_path = ''

        # Create process pool
        num_workers = self.config.get('num_workers', 4)
        self.logger.info(f"Starting {num_workers} worker processes...")

        with ProcessPoolExecutor(max_workers=num_workers, initializer=_init_worker) as executor:

            for doc in parser.iter_documents(limit=limit):
                # Handle resume
                if skip_until_found:
                    if doc.file_path == start_after:
                        skip_until_found = False
                    continue

                # Check for stop request
                if self._stop_requested:
                    self.logger.info("Stop requested - saving and exiting")
                    break

                files_processed += 1
                last_file_path = doc.file_path

                self.logger.info(f"Processing [{files_processed}]: {os.path.basename(doc.file_path)}")

                # Collect all chunks from this document
                chunks = self._collect_chunks(doc, corpus_id)
                self.logger.info(f"  {len(chunks)} chunks to search")

                # Submit all chunks to worker pool
                futures = []
                for chunk_task in chunks:
                    future = executor.submit(_search_chunk_worker, chunk_task, self.config)
                    futures.append(future)

                # Collect results
                file_matches = 0
                for future in as_completed(futures):
                    if self._stop_requested:
                        break
                    try:
                        matches = future.result(timeout=60)
                        pending_results.extend(matches)
                        file_matches += len(matches)
                        total_matches += len(matches)
                    except Exception as e:
                        self.logger.warning(f"Future error: {e}")

                self.logger.info(f"  Found {file_matches} matches")

                # Save batch if needed
                if len(pending_results) >= 500:
                    self.db.save_results(pending_results)
                    pending_results = []

                # Checkpoint every batch_size files
                if files_processed % self.config['batch_size'] == 0:
                    self.db.save_results(pending_results)
                    pending_results = []
                    self.db.save_checkpoint(corpus_id, doc.file_path, files_processed,
                                           total_matches, self.config)
                    self.logger.info(f"Checkpoint: {files_processed} files, {total_matches} matches")

        # Final save
        if pending_results:
            self.db.save_results(pending_results)
        self.db.save_checkpoint(corpus_id, last_file_path, files_processed, total_matches, self.config)

        self.logger.info(f"Completed: {files_processed} files, {total_matches} matches")
        return {'files': files_processed, 'matches': total_matches}

    def run_all(self, limit: int = None, resume: bool = True):
        """Run on all corpora."""
        results = {}
        for corpus_id in CORPORA.keys():
            if self._stop_requested:
                break
            results[corpus_id] = self.run_corpus(corpus_id, limit=limit, resume=resume)
        return results


# ============================================================================
# CLI
# ============================================================================

def run_test(corpus_id: str = 'ja', limit: int = 2):
    """Quick test run on a few files."""
    print(f"\n{'='*60}")
    print(f"  TEST RUN: {corpus_id} corpus, {limit} files")
    print(f"  Config: chunk_size=5, min_score=400, workers=2")
    print('='*60)

    config = SEARCH_CONFIG.copy()
    config['min_score'] = 400  # Lower for testing
    config['num_workers'] = 2

    runner = CorpusRunner(config=config)
    result = runner.run_corpus(corpus_id, limit=limit, resume=False)

    print(f"\nResults: {result}")
    print(f"Database: {RESULTS_DB}")

    # Show some results
    db = ResultsDatabase()
    stats = db.get_stats()
    print(f"\nTotal matches in DB: {stats['total_matches']}")

    if stats.get('top_title_matches'):
        print("\nTop title matches:")
        for m in stats['top_title_matches'][:5]:
            print(f"  {m['source_title'][:30]} -> {m['ms_title'][:30]} ({m['title_match']:.2f})")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run corpus mapping with multiprocessing')
    parser.add_argument('--corpus', choices=['ja', 'maagarim', 'all'], default='all',
                        help='Which corpus to process')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of files')
    parser.add_argument('--test', action='store_true',
                        help='Run quick test (2 files)')
    parser.add_argument('--no-resume', action='store_true',
                        help='Start fresh, ignore checkpoints')

    # Search configuration
    parser.add_argument('--chunk-size', type=int, default=SEARCH_CONFIG['chunk_size'],
                        help=f"Chunk size in words (default: {SEARCH_CONFIG['chunk_size']})")
    parser.add_argument('--min-score', type=float, default=SEARCH_CONFIG['min_score'],
                        help=f"Minimum match score (default: {SEARCH_CONFIG['min_score']})")
    parser.add_argument('--mode', choices=['variants', 'variants_extended', 'variants_maximum'],
                        default=SEARCH_CONFIG['mode'],
                        help=f"Variant mode (default: {SEARCH_CONFIG['mode']})")
    parser.add_argument('--workers', type=int, default=SEARCH_CONFIG['num_workers'],
                        help=f"Number of parallel workers (default: {SEARCH_CONFIG['num_workers']})")

    args = parser.parse_args()

    if args.test:
        run_test(args.corpus if args.corpus != 'all' else 'ja', limit=2)
    else:
        config = {
            'chunk_size': args.chunk_size,
            'min_score': args.min_score,
            'mode': args.mode,
            'num_workers': args.workers,
            'batch_size': SEARCH_CONFIG['batch_size'],
            'chunk_overlap': 2,
        }

        print(f"\n{'='*60}")
        print(f"  CORPUS MAPPER - Multiprocessing Mode")
        print(f"  Corpus: {args.corpus}")
        print(f"  Chunk size: {config['chunk_size']} words")
        print(f"  Min score: {config['min_score']}")
        print(f"  Mode: {config['mode']}")
        print(f"  Workers: {config['num_workers']}")
        print('='*60)

        runner = CorpusRunner(config=config)

        try:
            if args.corpus == 'all':
                runner.run_all(limit=args.limit, resume=not args.no_resume)
            else:
                runner.run_corpus(args.corpus, limit=args.limit, resume=not args.no_resume)
        except KeyboardInterrupt:
            print("\n\nInterrupted! Saving checkpoint...")
            runner.request_stop()

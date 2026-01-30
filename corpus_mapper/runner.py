# -*- coding: utf-8 -*-
"""
Corpus Mapper Runner - Batch processing with checkpointing.

This script runs composition searches on external corpora against
the Genizah manuscript database, saving results to SQLite.
"""

import os
import sys
import json
import time
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Iterator
from dataclasses import dataclass, asdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .config import (
    CORPORA, OUTPUT_DIR, CHECKPOINTS_DIR, LOGS_DIR, RESULTS_DB,
    DEFAULT_MIN_SCORE, DEFAULT_CHUNK_SIZE, DEFAULT_BATCH_SIZE, ensure_dirs
)
from .parsers import JAParser, MaagarimParser
from .text_cleaner import get_cleaner


# Setup logging
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
    score: float
    match_type: str = 'parallel'


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
                    score REAL NOT NULL,
                    match_type TEXT DEFAULT 'parallel',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_corpus, source_file, source_ref, ms_id)
                )
            ''')

            conn.execute('CREATE INDEX IF NOT EXISTS idx_source ON corpus_matches(source_corpus, source_file)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_ms ON corpus_matches(ms_id)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_score ON corpus_matches(score DESC)')

            # Checkpoints table
            conn.execute('''
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    corpus_id TEXT NOT NULL,
                    last_file TEXT NOT NULL,
                    files_processed INTEGER NOT NULL,
                    total_matches INTEGER NOT NULL,
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
                 score, match_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (r.source_corpus, r.source_file, r.source_author, r.source_title,
                 r.source_ref, r.source_text, r.ms_id, r.ms_shelfmark, r.ms_snippet,
                 r.score, r.match_type)
                for r in results
            ])
            conn.commit()

    def save_checkpoint(self, corpus_id: str, last_file: str, files_processed: int, total_matches: int):
        """Save a checkpoint for resuming."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO checkpoints
                (corpus_id, last_file, files_processed, total_matches, updated_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (corpus_id, last_file, files_processed, total_matches, datetime.now()))
            conn.commit()

    def get_checkpoint(self, corpus_id: str) -> Optional[Dict[str, Any]]:
        """Get the last checkpoint for a corpus."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT last_file, files_processed, total_matches FROM checkpoints WHERE corpus_id = ?',
                (corpus_id,)
            )
            row = cursor.fetchone()
            if row:
                return {
                    'last_file': row[0],
                    'files_processed': row[1],
                    'total_matches': row[2]
                }
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT COUNT(*) FROM corpus_matches')
            total = cursor.fetchone()[0]

            cursor = conn.execute('''
                SELECT source_corpus, COUNT(*), AVG(score)
                FROM corpus_matches
                GROUP BY source_corpus
            ''')
            by_corpus = {row[0]: {'count': row[1], 'avg_score': row[2]} for row in cursor.fetchall()}

        return {'total_matches': total, 'by_corpus': by_corpus}


class CorpusRunner:
    """Main runner for batch corpus searching."""

    def __init__(self, min_score: float = None, chunk_size: int = None, batch_size: int = None):
        self.min_score = min_score or DEFAULT_MIN_SCORE
        self.chunk_size = chunk_size or DEFAULT_CHUNK_SIZE
        self.batch_size = batch_size or DEFAULT_BATCH_SIZE
        self.db = ResultsDatabase()
        self.logger = setup_logging()
        self.search_engine = None
        self._stop_requested = False

    def _init_search_engine(self):
        """Initialize the Genizah search engine."""
        if self.search_engine is not None:
            return

        self.logger.info("Initializing Genizah search engine...")

        # Import and initialize
        try:
            from genizah_core import LabEngine, Config

            # Check if transcriptions file exists
            if not os.path.exists(Config.FILE_V8):
                raise FileNotFoundError(f"Transcriptions file not found: {Config.FILE_V8}")

            self.search_engine = LabEngine()
            self.logger.info("Search engine initialized successfully")

        except ImportError as e:
            self.logger.error(f"Failed to import genizah_core: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to initialize search engine: {e}")
            raise

    def request_stop(self):
        """Request graceful stop at next checkpoint."""
        self._stop_requested = True
        self.logger.info("Stop requested - will save checkpoint and exit")

    def _search_chunk(self, text: str) -> List[Dict[str, Any]]:
        """
        Search a text chunk against the Genizah corpus.

        Returns list of matches with score >= min_score.
        """
        if not self.search_engine:
            self._init_search_engine()

        try:
            results = self.search_engine.lab_composition_search(
                text,
                chunk_size=self.chunk_size,
                mode='variants'
            )

            matches = []
            for item in results.get('main', []):
                if item.get('score', 0) >= self.min_score:
                    matches.append({
                        'ms_id': item.get('uid', ''),
                        'ms_shelfmark': item.get('raw_header', ''),
                        'ms_snippet': item.get('ms_snippet', ''),
                        'score': item.get('score', 0)
                    })

            return matches

        except Exception as e:
            self.logger.warning(f"Search error: {e}")
            return []

    def run_corpus(self, corpus_id: str, limit: int = None, resume: bool = True):
        """
        Run search on an entire corpus.

        Args:
            corpus_id: 'ja' or 'maagarim'
            limit: Optional limit on files
            resume: Whether to resume from checkpoint
        """
        corpus_config = CORPORA.get(corpus_id)
        if not corpus_config:
            raise ValueError(f"Unknown corpus: {corpus_id}")

        self.logger.info(f"Starting corpus: {corpus_config['name']}")
        self._init_search_engine()

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
            file_matches = 0

            self.logger.info(f"Processing [{files_processed}]: {os.path.basename(doc.file_path)}")

            # Search each chunk
            for chunk in doc.iter_chunks(chunk_size=self.chunk_size):
                if self._stop_requested:
                    break

                matches = self._search_chunk(chunk['text'])

                for match in matches:
                    # Build source reference
                    if corpus_id == 'ja':
                        source_ref = f"p.{chunk.get('page', '?')}, lines {chunk.get('start_line', '?')}-{chunk.get('end_line', '?')}"
                    else:
                        source_ref = f"section {chunk.get('section_idx', '?')}: {chunk.get('header', '')[:50]}"

                    result = SearchResult(
                        source_corpus=corpus_id,
                        source_file=os.path.basename(doc.file_path),
                        source_author=getattr(doc, 'author', ''),
                        source_title=getattr(doc, 'title', getattr(doc, 'composition', '')),
                        source_ref=source_ref,
                        source_text=chunk['text'][:500],
                        ms_id=match['ms_id'],
                        ms_shelfmark=match['ms_shelfmark'],
                        ms_snippet=match['ms_snippet'][:500] if match['ms_snippet'] else '',
                        score=match['score']
                    )
                    pending_results.append(result)
                    file_matches += 1
                    total_matches += 1

            # Save batch if needed
            if len(pending_results) >= 100:
                self.db.save_results(pending_results)
                pending_results = []

            # Checkpoint every batch_size files
            if files_processed % self.batch_size == 0:
                self.db.save_results(pending_results)
                pending_results = []
                self.db.save_checkpoint(corpus_id, doc.file_path, files_processed, total_matches)
                self.logger.info(f"Checkpoint: {files_processed} files, {total_matches} matches")

            # Small delay to prevent overheating
            time.sleep(0.05)

        # Final save
        if pending_results:
            self.db.save_results(pending_results)
        self.db.save_checkpoint(corpus_id, doc.file_path if 'doc' in dir() else '', files_processed, total_matches)

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


def run_test(corpus_id: str = 'ja', limit: int = 3):
    """
    Quick test run on a few files.

    Args:
        corpus_id: Which corpus to test
        limit: Number of files to process
    """
    print(f"\n{'='*60}")
    print(f"  TEST RUN: {corpus_id} corpus, {limit} files")
    print('='*60)

    runner = CorpusRunner(min_score=200)  # Lower threshold for testing
    result = runner.run_corpus(corpus_id, limit=limit, resume=False)

    print(f"\nResults: {result}")
    print(f"Database: {RESULTS_DB}")

    # Show some results
    db = ResultsDatabase()
    stats = db.get_stats()
    print(f"Total matches in DB: {stats['total_matches']}")


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run corpus mapping')
    parser.add_argument('--corpus', choices=['ja', 'maagarim', 'all'], default='all',
                        help='Which corpus to process')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of files (for testing)')
    parser.add_argument('--test', action='store_true',
                        help='Run quick test (3 files)')
    parser.add_argument('--no-resume', action='store_true',
                        help='Start fresh, ignore checkpoints')
    parser.add_argument('--min-score', type=float, default=DEFAULT_MIN_SCORE,
                        help=f'Minimum score threshold (default: {DEFAULT_MIN_SCORE})')

    args = parser.parse_args()

    if args.test:
        run_test(args.corpus if args.corpus != 'all' else 'ja', limit=3)
    else:
        runner = CorpusRunner(min_score=args.min_score)

        if args.corpus == 'all':
            runner.run_all(limit=args.limit, resume=not args.no_resume)
        else:
            runner.run_corpus(args.corpus, limit=args.limit, resume=not args.no_resume)

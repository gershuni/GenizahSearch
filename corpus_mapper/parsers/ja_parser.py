# -*- coding: utf-8 -*-
"""
Judeo-Arabic (Friedberg) Parser.

Parses JSON files from the Friedberg Judeo-Arabic corpus.
Format:
{
  "AuthorName": "...",
  "TitleName": "...",
  "Content": [
    {"PageNumber": N, "rows": [{"LineNumber": N, "Text": "..."}]}
  ]
}
"""

import os
import json
import glob
from typing import Dict, List, Any, Iterator, Optional
from dataclasses import dataclass, field

from ..config import CORPORA
from ..text_cleaner import get_cleaner


@dataclass
class JADocument:
    """Represents a parsed Judeo-Arabic document."""
    file_path: str
    author: str
    author_full: str
    title: str
    title_original: str
    description: str
    publisher: str
    editor: str
    pages: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def file_id(self) -> str:
        """Extract file ID from path (e.g., '8' from '8.JSON')."""
        return os.path.splitext(os.path.basename(self.file_path))[0]

    def get_full_text(self, cleaned: bool = True) -> str:
        """Get all text from the document."""
        lines = []
        for page in self.pages:
            for row in page.get('rows', []):
                lines.append(row.get('text', ''))

        text = '\n'.join(lines)

        if cleaned:
            cleaner = get_cleaner()
            text, _ = cleaner.clean_ja_text(text)

        return text

    def iter_chunks(self, chunk_size: int = 15, overlap: int = 7, cleaned: bool = True) -> Iterator[Dict[str, Any]]:
        """
        Iterate over text chunks for searching.

        Args:
            chunk_size: Number of words per chunk
            overlap: Number of words to overlap between chunks
            cleaned: Whether to clean the text first

        Yields:
            Dict with chunk info: {text, page, start_line, end_line, word_offset}
        """
        # Collect all lines with their positions
        all_lines = []
        for page in self.pages:
            page_num = page.get('page_number', 0)
            for row in page.get('rows', []):
                line_num = row.get('line_number', 0)
                text = row.get('text', '')
                if cleaned:
                    cleaner = get_cleaner()
                    text, _ = cleaner.clean_ja_text(text)
                all_lines.append({
                    'page': page_num,
                    'line': line_num,
                    'text': text
                })

        # Combine into words with position tracking
        words = []
        for line_info in all_lines:
            line_words = line_info['text'].split()
            for word in line_words:
                words.append({
                    'word': word,
                    'page': line_info['page'],
                    'line': line_info['line']
                })

        # Generate chunks
        step = chunk_size - overlap
        for i in range(0, max(1, len(words) - chunk_size + 1), step):
            chunk_words = words[i:i + chunk_size]
            if not chunk_words:
                continue

            yield {
                'text': ' '.join(w['word'] for w in chunk_words),
                'page': chunk_words[0]['page'],
                'start_line': chunk_words[0]['line'],
                'end_line': chunk_words[-1]['line'],
                'word_offset': i
            }


class JAParser:
    """Parser for Judeo-Arabic JSON corpus."""

    def __init__(self, corpus_path: str = None):
        self.corpus_path = corpus_path or CORPORA['ja']['path']

    def parse_file(self, filepath: str) -> Optional[JADocument]:
        """
        Parse a single JSON file.

        Args:
            filepath: Path to the JSON file

        Returns:
            JADocument or None if parsing fails
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract metadata
            doc = JADocument(
                file_path=filepath,
                author=data.get('AuthorName', ''),
                author_full=data.get('FullName', ''),
                title=data.get('TitleName', ''),
                title_original=data.get('OriginalName', ''),
                description=data.get('Description', ''),
                publisher=data.get('Publisher', ''),
                editor=data.get('Editor', '')
            )

            # Extract content
            for page_data in data.get('Content', []):
                page = {
                    'page_number': page_data.get('PageNumber', 0),
                    'rows': []
                }
                for row_data in page_data.get('rows', []):
                    page['rows'].append({
                        'line_number': row_data.get('LineNumber', 0),
                        'text': row_data.get('Text', '')
                    })
                doc.pages.append(page)

            return doc

        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return None

    def iter_files(self, limit: int = None) -> Iterator[str]:
        """
        Iterate over all JSON files in the corpus.

        Args:
            limit: Optional limit on number of files

        Yields:
            File paths
        """
        pattern = os.path.join(self.corpus_path, '*.JSON')
        files = sorted(glob.glob(pattern), key=lambda x: int(os.path.splitext(os.path.basename(x))[0]))

        for i, filepath in enumerate(files):
            if limit and i >= limit:
                break
            yield filepath

    def iter_documents(self, limit: int = None) -> Iterator[JADocument]:
        """
        Iterate over all parsed documents.

        Args:
            limit: Optional limit on number of documents

        Yields:
            JADocument instances
        """
        for filepath in self.iter_files(limit):
            doc = self.parse_file(filepath)
            if doc:
                yield doc

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the corpus."""
        files = list(self.iter_files())
        total_pages = 0
        total_lines = 0
        authors = set()
        titles = []

        for filepath in files[:10]:  # Sample first 10 for speed
            doc = self.parse_file(filepath)
            if doc:
                total_pages += len(doc.pages)
                for page in doc.pages:
                    total_lines += len(page.get('rows', []))
                authors.add(doc.author)
                titles.append(doc.title)

        return {
            'total_files': len(files),
            'sample_pages': total_pages,
            'sample_lines': total_lines,
            'unique_authors': len(authors),
            'sample_titles': titles[:5]
        }


def parse_ja_file(filepath: str) -> Optional[JADocument]:
    """Convenience function to parse a single JA file."""
    parser = JAParser()
    return parser.parse_file(filepath)


if __name__ == '__main__':
    # Test the parser
    parser = JAParser()
    stats = parser.get_stats()
    print("JA Corpus Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Parse first document
    for doc in parser.iter_documents(limit=1):
        print(f"\nFirst document: {doc.title}")
        print(f"  Author: {doc.author}")
        print(f"  Pages: {len(doc.pages)}")

        # Show first chunk
        for chunk in doc.iter_chunks(chunk_size=15):
            print(f"\n  First chunk (page {chunk['page']}):")
            print(f"    {chunk['text'][:200]}...")
            break

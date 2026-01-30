# -*- coding: utf-8 -*-
"""
Maagarim (Academy Historical Dictionary) Parser.

Parses TXT files from the Maagarim corpus.
Filename format: author--composition--date--genre--id-OnlyText.txt

Content format:
## title | source: manuscript info ##
>> text line
>> text line
"""

import os
import re
import glob
from typing import Dict, List, Any, Iterator, Optional, Tuple
from dataclasses import dataclass, field

from ..config import CORPORA
from ..text_cleaner import get_cleaner


@dataclass
class MaagarimSection:
    """A section within a Maagarim document (marked by ## header ##)."""
    header: str
    manuscript_info: str
    lines: List[str] = field(default_factory=list)

    @property
    def text(self) -> str:
        """Get combined text of all lines."""
        return '\n'.join(self.lines)


@dataclass
class MaagarimDocument:
    """Represents a parsed Maagarim document."""
    file_path: str
    author: str
    composition: str
    date: str
    genre: str
    document_id: str
    sections: List[MaagarimSection] = field(default_factory=list)

    @classmethod
    def from_filename(cls, filepath: str) -> 'MaagarimDocument':
        """Parse metadata from filename."""
        basename = os.path.basename(filepath)
        # Remove -OnlyText.txt suffix
        name = re.sub(r'-OnlyText\.txt$', '', basename, flags=re.IGNORECASE)

        # Split by --
        parts = name.split('--')

        # Handle various formats
        author = parts[0] if len(parts) > 0 else ''
        composition = parts[1] if len(parts) > 1 else ''
        date = parts[2] if len(parts) > 2 else ''
        genre = parts[3] if len(parts) > 3 else ''
        doc_id = parts[4] if len(parts) > 4 else ''

        # Clean up ID (remove .txt if present)
        doc_id = re.sub(r'\.txt$', '', doc_id, flags=re.IGNORECASE)

        return cls(
            file_path=filepath,
            author=author.strip(),
            composition=composition.strip(),
            date=date.strip(),
            genre=genre.strip(),
            document_id=doc_id.strip()
        )

    def get_full_text(self, cleaned: bool = True) -> str:
        """Get all text from all sections."""
        lines = []
        for section in self.sections:
            lines.extend(section.lines)

        text = '\n'.join(lines)

        if cleaned:
            cleaner = get_cleaner()
            text, _ = cleaner.clean_maagarim_text(text)

        return text

    def iter_chunks(self, chunk_size: int = 15, overlap: int = 7, cleaned: bool = True) -> Iterator[Dict[str, Any]]:
        """
        Iterate over text chunks for searching.

        Yields:
            Dict with chunk info: {text, section_idx, header, word_offset}
        """
        for section_idx, section in enumerate(self.sections):
            text = section.text
            if cleaned:
                cleaner = get_cleaner()
                text, _ = cleaner.clean_maagarim_text(text)

            words = text.split()
            step = chunk_size - overlap

            for i in range(0, max(1, len(words) - chunk_size + 1), step):
                chunk_words = words[i:i + chunk_size]
                if not chunk_words:
                    continue

                yield {
                    'text': ' '.join(chunk_words),
                    'section_idx': section_idx,
                    'header': section.header,
                    'manuscript_info': section.manuscript_info,
                    'word_offset': i
                }


class MaagarimParser:
    """Parser for Maagarim TXT corpus."""

    # Patterns for parsing
    HEADER_PATTERN = re.compile(r'^##([^|]*)\|?\s*(?:המסירה:\s*)?([^#]*)##\s*$')
    LINE_PATTERN = re.compile(r'^>>\s*(.*)$')

    def __init__(self, corpus_path: str = None):
        self.corpus_path = corpus_path or CORPORA['maagarim']['path']

    def parse_file(self, filepath: str) -> Optional[MaagarimDocument]:
        """
        Parse a single TXT file.

        Args:
            filepath: Path to the TXT file

        Returns:
            MaagarimDocument or None if parsing fails
        """
        try:
            doc = MaagarimDocument.from_filename(filepath)

            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            current_section = None

            for line in content.split('\n'):
                line = line.strip()

                # Check for header
                header_match = self.HEADER_PATTERN.match(line)
                if header_match:
                    # Save previous section
                    if current_section and current_section.lines:
                        doc.sections.append(current_section)

                    # Start new section
                    current_section = MaagarimSection(
                        header=header_match.group(1).strip(),
                        manuscript_info=header_match.group(2).strip()
                    )
                    continue

                # Check for content line
                line_match = self.LINE_PATTERN.match(line)
                if line_match and current_section is not None:
                    text = line_match.group(1).strip()
                    if text:
                        current_section.lines.append(text)

            # Don't forget the last section
            if current_section and current_section.lines:
                doc.sections.append(current_section)

            return doc

        except Exception as e:
            print(f"Error parsing {filepath}: {e}")
            return None

    def iter_files(self, limit: int = None) -> Iterator[str]:
        """
        Iterate over all TXT files in the corpus.

        Args:
            limit: Optional limit on number of files

        Yields:
            File paths
        """
        pattern = os.path.join(self.corpus_path, '*-OnlyText.txt')
        files = sorted(glob.glob(pattern))

        for i, filepath in enumerate(files):
            if limit and i >= limit:
                break
            yield filepath

    def iter_documents(self, limit: int = None) -> Iterator[MaagarimDocument]:
        """
        Iterate over all parsed documents.

        Args:
            limit: Optional limit on number of documents

        Yields:
            MaagarimDocument instances
        """
        for filepath in self.iter_files(limit):
            doc = self.parse_file(filepath)
            if doc:
                yield doc

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the corpus."""
        files = list(self.iter_files())
        total_sections = 0
        total_lines = 0
        authors = set()
        genres = set()
        dates = set()

        for filepath in files[:50]:  # Sample first 50 for speed
            doc = self.parse_file(filepath)
            if doc:
                total_sections += len(doc.sections)
                for section in doc.sections:
                    total_lines += len(section.lines)
                authors.add(doc.author)
                genres.add(doc.genre)
                if doc.date:
                    dates.add(doc.date)

        return {
            'total_files': len(files),
            'sample_sections': total_sections,
            'sample_lines': total_lines,
            'unique_authors': len(authors),
            'unique_genres': len(genres),
            'date_range': sorted(dates)[:5] if dates else []
        }


def parse_maagarim_file(filepath: str) -> Optional[MaagarimDocument]:
    """Convenience function to parse a single Maagarim file."""
    parser = MaagarimParser()
    return parser.parse_file(filepath)


if __name__ == '__main__':
    # Test the parser
    parser = MaagarimParser()
    stats = parser.get_stats()
    print("Maagarim Corpus Stats:")
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # Parse first document
    for doc in parser.iter_documents(limit=1):
        print(f"\nFirst document:")
        print(f"  Author: {doc.author}")
        print(f"  Composition: {doc.composition}")
        print(f"  Date: {doc.date}")
        print(f"  Genre: {doc.genre}")
        print(f"  Sections: {len(doc.sections)}")

        if doc.sections:
            print(f"\n  First section header: {doc.sections[0].header}")
            print(f"  Manuscript: {doc.sections[0].manuscript_info}")
            print(f"  Lines: {len(doc.sections[0].lines)}")

        # Show first chunk
        for chunk in doc.iter_chunks(chunk_size=15):
            print(f"\n  First chunk:")
            print(f"    {chunk['text'][:200]}...")
            break

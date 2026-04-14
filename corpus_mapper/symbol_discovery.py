# -*- coding: utf-8 -*-
"""
Symbol Discovery - Scans corpora to find all special symbols and patterns.

This script analyzes the corpus files and generates a report of all
special symbols, their frequency, and example contexts.
"""

import os
import re
import json
import glob
from collections import defaultdict, Counter
from typing import Dict, List, Any

from .config import CORPORA, SYMBOL_REPORT_FILE, ensure_dirs


# Regex patterns to detect special constructs
PATTERN_TYPES = {
    # Brackets and their contents
    'curly_braces': r'\{[^}]*\}',           # {text}
    'square_brackets': r'\[[^\]]*\]',        # [text]
    'parentheses': r'\([^)]*\)',             # (text)
    'angle_brackets': r'<[^>]*>',            # <text>
    'double_angle': r'<<[^>]*>>',            # <<text>>

    # Combined patterns
    'plus_angle': r'\+<[^>]*>',              # +<text>
    'tilde_braces': r'~\{[^}]*\}',           # ~{text}
    'plus_braces': r'\+\{[^}]*\}',           # +{text}

    # Special markers
    'double_hash': r'##[^#]*##',             # ##text##
    'double_greater': r'>>\s*',              # >> (line start)
    'dollar_markers': r'\$[^$]*\$',          # $text$
    'pipe_markers': r'\|',                   # |
    'question_marks': r'\?[^\s?]*\?',        # ?text?

    # Numbered references
    'superscript_nums': r'<\d+>',            # <70>
    'subscript_refs': r'<[^>]*\d+>',         # <text70>

    # Hebrew-specific
    'nikud': r'[\u05B0-\u05BD\u05BF-\u05C7]', # ניקוד
    'taamim': r'[\u0591-\u05AF]',            # טעמים
    'maqaf': r'\u05BE',                      # מקף ־

    # Special characters
    'geresh': r'[\u05F3\u05F4\']',           # גרש ׳ וגרשיים ״
}


class SymbolDiscovery:
    """Discovers and catalogs special symbols in corpus files."""

    def __init__(self):
        self.results = {
            'ja': defaultdict(lambda: {'count': 0, 'examples': []}),
            'maagarim': defaultdict(lambda: {'count': 0, 'examples': []})
        }
        self.raw_symbols = {
            'ja': Counter(),
            'maagarim': Counter()
        }

    def scan_ja_file(self, filepath: str) -> Dict[str, List[str]]:
        """Scan a single Judeo-Arabic JSON file."""
        found = defaultdict(list)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extract all text content
            texts = []
            if 'Content' in data:
                for page in data['Content']:
                    if 'rows' in page:
                        for row in page['rows']:
                            if 'Text' in row:
                                texts.append(row['Text'])

            # Scan for patterns
            full_text = '\n'.join(texts)
            for pattern_name, pattern in PATTERN_TYPES.items():
                matches = re.findall(pattern, full_text)
                if matches:
                    found[pattern_name].extend(matches[:5])  # Keep up to 5 examples

            # Also find any other non-Hebrew/Arabic characters
            other_chars = re.findall(r'[^\u0590-\u05FF\u0600-\u06FFa-zA-Z0-9\s.,;:!?\-\'"()]', full_text)
            if other_chars:
                found['other_special'] = list(set(other_chars))[:20]

        except Exception as e:
            print(f"Error scanning {filepath}: {e}")

        return found

    def scan_maagarim_file(self, filepath: str) -> Dict[str, List[str]]:
        """Scan a single Maagarim TXT file."""
        found = defaultdict(list)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            # Scan for patterns
            for pattern_name, pattern in PATTERN_TYPES.items():
                matches = re.findall(pattern, content)
                if matches:
                    found[pattern_name].extend(matches[:5])

            # Find line structure patterns
            lines = content.split('\n')
            for line in lines[:50]:  # Sample first 50 lines
                if line.startswith('##'):
                    found['header_line'].append(line[:100])
                elif line.startswith('>>'):
                    found['content_line'].append(line[:100])

            # Find other special characters
            other_chars = re.findall(r'[^\u0590-\u05FF\u0600-\u06FFa-zA-Z0-9\s.,;:!?\-\'"()\[\]{}<>|#$@~+]', content)
            if other_chars:
                found['other_special'] = list(set(other_chars))[:20]

        except Exception as e:
            print(f"Error scanning {filepath}: {e}")

        return found

    def scan_corpus(self, corpus_id: str, limit: int = None, progress_callback=None):
        """
        Scan an entire corpus for special symbols.

        Args:
            corpus_id: 'ja' or 'maagarim'
            limit: Optional limit on number of files to scan
            progress_callback: Optional callback(current, total)
        """
        corpus = CORPORA.get(corpus_id)
        if not corpus:
            raise ValueError(f"Unknown corpus: {corpus_id}")

        # Find all files
        pattern = os.path.join(corpus['path'], corpus['pattern'])
        files = glob.glob(pattern)

        if limit:
            files = files[:limit]

        total = len(files)
        print(f"Scanning {total} files in {corpus['name']}...")

        all_found = defaultdict(lambda: {'count': 0, 'examples': set()})

        for i, filepath in enumerate(files):
            if progress_callback:
                progress_callback(i + 1, total)
            elif i % 100 == 0:
                print(f"  Progress: {i}/{total}")

            # Scan file based on type
            if corpus_id == 'ja':
                found = self.scan_ja_file(filepath)
            else:
                found = self.scan_maagarim_file(filepath)

            # Aggregate results
            for pattern_name, examples in found.items():
                all_found[pattern_name]['count'] += len(examples)
                for ex in examples:
                    if len(all_found[pattern_name]['examples']) < 10:
                        all_found[pattern_name]['examples'].add(ex[:100] if isinstance(ex, str) else str(ex))

        # Convert sets to lists for JSON serialization
        self.results[corpus_id] = {
            k: {
                'count': v['count'],
                'examples': list(v['examples'])
            }
            for k, v in all_found.items()
        }

        return self.results[corpus_id]

    def generate_report(self) -> Dict[str, Any]:
        """Generate a comprehensive report of all discovered symbols."""
        report = {
            'summary': {},
            'corpora': {}
        }

        for corpus_id, data in self.results.items():
            corpus_info = CORPORA.get(corpus_id, {})
            report['corpora'][corpus_id] = {
                'name': corpus_info.get('name', corpus_id),
                'name_he': corpus_info.get('name_he', corpus_id),
                'patterns': {}
            }

            # Sort patterns by count
            sorted_patterns = sorted(data.items(), key=lambda x: x[1]['count'], reverse=True)

            for pattern_name, info in sorted_patterns:
                if info['count'] > 0:
                    report['corpora'][corpus_id]['patterns'][pattern_name] = {
                        'count': info['count'],
                        'examples': info['examples'],
                        'suggested_action': self._suggest_action(pattern_name)
                    }

        return report

    def _suggest_action(self, pattern_name: str) -> str:
        """Suggest a default action for a pattern type."""
        suggestions = {
            'curly_braces': 'remove_brackets_keep_content',
            'square_brackets': 'remove_entirely',  # Usually corrections/variants
            'parentheses': 'ask_user',
            'angle_brackets': 'remove_brackets_keep_content',
            'double_hash': 'extract_as_metadata',
            'double_greater': 'remove_marker',
            'dollar_markers': 'remove_entirely',
            'nikud': 'remove',
            'taamim': 'remove',
            'maqaf': 'replace_with_space',
            'question_marks': 'remove_markers_keep_content',
            'plus_angle': 'remove_entirely',  # Usually additions
            'superscript_nums': 'remove_entirely',
        }
        return suggestions.get(pattern_name, 'ask_user')

    def save_report(self, filepath: str = None):
        """Save the report to a JSON file."""
        if filepath is None:
            ensure_dirs()
            filepath = SYMBOL_REPORT_FILE

        report = self.generate_report()

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        print(f"Report saved to: {filepath}")
        return filepath


def run_discovery(corpus_ids: List[str] = None, limit: int = None):
    """
    Run symbol discovery on specified corpora.

    Args:
        corpus_ids: List of corpus IDs to scan (default: all)
        limit: Optional limit on files per corpus (for testing)

    Returns:
        Path to the generated report
    """
    if corpus_ids is None:
        corpus_ids = list(CORPORA.keys())

    discovery = SymbolDiscovery()

    for corpus_id in corpus_ids:
        print(f"\n{'='*50}")
        print(f"Scanning: {CORPORA[corpus_id]['name']}")
        print('='*50)
        discovery.scan_corpus(corpus_id, limit=limit)

    return discovery.save_report()


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Discover special symbols in corpora')
    parser.add_argument('--corpus', choices=['ja', 'maagarim', 'all'], default='all',
                        help='Which corpus to scan')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of files (for testing)')

    args = parser.parse_args()

    corpus_ids = None if args.corpus == 'all' else [args.corpus]
    run_discovery(corpus_ids, args.limit)

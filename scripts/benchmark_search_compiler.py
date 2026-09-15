"""Compare distinct generated queries with an optional previous adapter source.

Run with --previous path/to/search_regex.py to compare an earlier implementation.
Use --match to also compare small synthetic matching samples. This makes no
real-index throughput claims; compilation is measured separately from matching.
"""

import argparse
import importlib.util
import json
from pathlib import Path
import re
import statistics
import sys
import time
from types import SimpleNamespace

import regex

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from shared import search_engine, search_regex  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--previous', type=Path)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--case', choices=('exact', 'gaps', 'variants', 'responsa'), default='exact')
    parser.add_argument('--match', action='store_true', help='Also match small synthetic inputs; stdlib matching has no timeout.')
    args = parser.parse_args()
    compilers = {'stdlib': re.compile, 'compact': search_regex.compile}
    if args.previous:
        spec = importlib.util.spec_from_file_location('previous_adapter', args.previous)
        previous = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(previous)
        compilers['previous'] = previous.compile

    engine = object.__new__(search_engine.SearchEngine)
    def variants(term, mode, limit):
        return [term + chr(0x5D0 + i) for i in range(8)] if args.case == 'variants' else [term]

    engine.var_mgr = SimpleNamespace(get_variants=variants)
    tokens = [''.join(chr(0x5D0 + (n // 22**i) % 22) for i in range(4)) for n in range(120)]
    chunks = [tokens[i:i + 10] for i in range(111)]
    results = {name: [] for name in compilers}
    matching = {name: [] for name in compilers}
    initialization = {}
    documents = [' '.join(tokens), '\n'.join(reversed(tokens)), ('\u05E1\u05D9\u05DE\u05DF ' * 200) + ' '.join(tokens[:10])]
    if args.case == 'gaps':
        # Long overlapping gap candidates can cause exponential stdlib matching.
        # Keep this optional parity sample small; measure compilation separately.
        documents = [' '.join(tokens[:10]), '\n'.join(reversed(tokens[:10])), '\u05E1\u05D9\u05DE\u05DF ' * 10]
    expected_spans = None
    original = search_engine.compile_search_regex
    try:
        for name, compiler in compilers.items():
            started = time.perf_counter()
            compiler(r'\w')
            initialization[name] = time.perf_counter() - started
        for iteration in range(args.rounds):
            names = list(compilers)
            if iteration % 2:
                names.reverse()
            for name in names:
                re.purge()
                regex.purge()
                search_engine.compile_search_regex = compilers[name]
                started = time.perf_counter()
                patterns = [engine.build_regex_pattern(
                    chunk, 'exact', 5 if args.case == 'gaps' else 0,
                    responsa_components=([{'regex_terms': [term]} for term in chunk]
                                         if args.case == 'responsa' else None),
                    responsa_options={'within_document': True},
                ) for chunk in chunks]
                elapsed = time.perf_counter() - started
                assert all(pattern is not None for pattern in patterns)
                results[name].append(elapsed)
                print(f'{args.case}: round {iteration + 1} {name} compiled in {elapsed:.3f}s', file=sys.stderr, flush=True)
                if not args.match:
                    continue
                started = time.perf_counter()
                spans = []
                for pattern in patterns:
                    for document in documents:
                        match = pattern.search(document)
                        spans.append((match.span(), match.groups()) if match else None)
                matching[name].append(time.perf_counter() - started)
                if expected_spans is None:
                    expected_spans = spans
                assert spans == expected_spans, name
        print(json.dumps({
            'python': sys.version, 'regex': regex.__version__,
            'distinct_chunks': len(chunks), 'words_per_chunk': 10,
            'case': args.case,
            'initialization_seconds': initialization,
            'round_seconds': results,
            'median_seconds': {name: statistics.median(values) for name, values in results.items()},
            'matching_round_seconds': matching,
        }, indent=2))
    finally:
        search_engine.compile_search_regex = original


if __name__ == '__main__':
    main()

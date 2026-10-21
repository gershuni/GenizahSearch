# -*- coding: utf-8 -*-
"""Phase 0 of docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md: measurements, no product code.

One subcommand per item of plan section 7 ("Phase 0"). Each writes
<out-dir>/<item>.json and <out-dir>/<item>.log (both UTF-8); the console gets
ASCII only, so Hebrew never reaches a cp1252 terminal. Everything here is
READ-ONLY on the index, the corpus file and browse_map.pkl.

Items (plan section 7, Phase 0):
  scan             items 1 + 7: one pass over Transcriptions.txt -- every distinct
                   content_search word (folded, hebword class), the reversed sorted
                   list on disk, and the Hebrew-block punctuation kept inside tokens
  wordlist         item 1: load the reversed list, binary-search two endings, and
                   time term_set_query against regex_query('.*ending') on the index
  patterns         item 2: bracket/mark-tolerant regex_query patterns, and the
                   tantivy-fst / Rust-regex size limit
  scored-terms     item 3: a boolean OR of 600 and 1,200 term_query clauses
  missing-pages    item 4: the pages a brute-force scan finds for `*nim *lim` that the
                   index path (two leading-star lookups + the letters-only check) misses
  verify-tolerant  item 4, closing check: the bracket- and mark-tolerant stage 1 admits them?
  topology         item 5: junctions inside whole-manuscript and part documents that
                   are not two consecutive pages of one volume and one transcription
  desktop-timings  item 6: execute_search in Responsa mode, as the desktop calls it,
                   with the `regex` module (desktop) and stdlib `re` (production worker)
  punct            item 7: the punctuation counts, read back from scan.json
  marks            items 2 + 7: distinct words holding Hebrew marks / other-script marks
  token-only       item 7, exhaustive: every character kept inside a token that ends a word
                   for Exact's predicate (maqaf, sof pasuq, join controls, ...), by count

Run (Windows, the owner's laptop):
  python -I -X utf8 scripts/measure_responsa_wildcards.py scan ^
      --corpus-file C:\\GenizahSearch\\Transcriptions.txt --out-dir _tmp\\responsa_wildcards_phase0
  python -I -X utf8 scripts/measure_responsa_wildcards.py desktop-timings --out-dir ...

Linux worker (the owner's run): the same commands with --index-dir /path/to/tantivy_db
and --corpus-file /path/to/Transcriptions.txt; `desktop-timings` measures both regex
engines there too (`--engines re` for the production route only).

Exit codes: 0 done; 2 required data missing (never a silent skip).
"""
from __future__ import annotations

import argparse
import bisect
import json
import os
import platform
import re
import sys
import time
import unicodedata
from contextlib import nullcontext

import regex  # the regex module: \\p{..} classes, and its \\w is Rust's (Alphabetic, M, Nd, Pc, Join_Control)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import tantivy  # noqa: E402

from shared.search_tokenizer import HEBWORD_TOKENIZER_PATTERN, register_search_tokenizers  # noqa: E402
from shared.sys_id_patterns import CORPUS_SYS_ID_RE  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402

EXIT_OK, EXIT_MISSING_DATA = 0, 2

# The tokenizer's own character class, compiled with the regex module (its \w agrees
# with the Rust regex crate's: Alphabetic, marks, decimal digits, connector punctuation,
# join controls; NOT the superscript digits Python's re counts as \w).
HEBWORD_RE = regex.compile(HEBWORD_TOKENIZER_PATTERN)

# Item 7: Hebrew-block punctuation the tokenizer keeps inside a token (plan section 4 names
# maqaf as the one documented gap; the other three are the question).
HEBREW_PUNCT = {
    '\u05BE': 'maqaf U+05BE',
    '\u05C0': 'paseq U+05C0',
    '\u05C3': 'sof pasuq U+05C3',
    '\u05C6': 'nun hafukha U+05C6',
}
PUNCT_TOKEN_RE = regex.compile(
    HEBWORD_TOKENIZER_PATTERN[:-1] + '*[\u05BE\u05C0\u05C3\u05C6]' + HEBWORD_TOKENIZER_PATTERN[:-1] + '*')

NARROW_ENDING, BROAD_ENDING = 'כהנים', 'נים'
STAR_WORD_1, STAR_WORD_2 = '*נים', '*לים'
STEM_1, STEM_2 = 'כהנ', 'גדול'          # #X# stems for items 2 and 3
FINITE_WORD = 'כהנים'


# --------------------------------------------------------------------------------------
# The hand-written oracle (plan section 8, gate 1): Exact's word predicate and a letters-only
# star, written here on purpose and NOT imported from shared/search_engine.py or the compiler.
# --------------------------------------------------------------------------------------

# Copied from the plan's section 4 (Exact's _IN_WORD_QUOTES), not imported: the oracle must
# not depend on the engine module it checks.
_IN_WORD_QUOTES = frozenset('\'"\u05F3\u05F4\u2018\u2019[]')


def _looked_past(ch):
    """Marks, in-word quotes and brackets belong to the word they sit in and are transparent."""
    return ch in _IN_WORD_QUOTES or unicodedata.category(ch)[0] == 'M'


def _continues_word(ch):
    """A word goes on through letters and decimal digits of any script and connector punctuation."""
    cat = unicodedata.category(ch)
    return cat[0] == 'L' or cat in ('Nd', 'Pc')


# A maximal run of word characters and looked-past characters. A run made only of
# looked-past characters (e.g. the lacuna mark `][`) is not a word.
WORD_RUN_RE = regex.compile(r"[\p{L}\p{Nd}\p{Pc}\p{M}'\"\u05F3\u05F4\u2018\u2019\[\]]+")
# The looked-past characters alone, for removing them from a text at C speed.
LOOKED_PAST_RE = regex.compile(r"[\p{M}'\"\u05F3\u05F4\u2018\u2019\[\]]")


def semantic_words(folded_text):
    """The whole words of a page by Exact's predicate, in order."""
    return [m.group() for m in WORD_RUN_RE.finditer(folded_text) if any(map(_continues_word, m.group()))]


_STAR_CACHE = {}


def _star_regex(pattern):
    """`*` = any sequence of letters; everything else literal. Applied to the word's letters only."""
    compiled = _STAR_CACHE.get(pattern)
    if compiled is None:
        compiled = re.compile('.*'.join(re.escape(part) for part in pattern.split('*')), re.DOTALL)
        _STAR_CACHE[pattern] = compiled
    return compiled


def star_word(pattern, run):
    """Does the whole word *run* match *pattern* (letters-only star, marks/quotes/brackets
    passed over, digits and connectors never consumed)?"""
    core = ''.join(ch for ch in run if not _looked_past(ch))
    if not core:
        return False
    for ch in core:
        if unicodedata.category(ch)[0] != 'L':
            return False
    return _star_regex(pattern).fullmatch(core) is not None


def first_pair(folded_text, p1, p2):
    """The first two consecutive whole words matching p1 then p2 (gap 0), or None."""
    words = semantic_words(folded_text)
    for i in range(len(words) - 1):
        if star_word(p1, words[i]) and star_word(p2, words[i + 1]):
            return words[i], words[i + 1]
    return None


# --------------------------------------------------------------------------------------
# Plumbing
# --------------------------------------------------------------------------------------

class Log:
    def __init__(self, out_dir, item):
        os.makedirs(out_dir, exist_ok=True)
        self.path = os.path.join(out_dir, f'{item}.log')
        self.json_path = os.path.join(out_dir, f'{item}.json')
        self._f = open(self.path, 'a', encoding='utf-8')
        self.started = time.time()
        self(f'=== {item} started {time.strftime("%Y-%m-%d %H:%M:%S")} ===')

    def __call__(self, msg):
        line = f'[{time.strftime("%H:%M:%S")}] {msg}'
        self._f.write(line + '\n')
        self._f.flush()
        print(line.encode('ascii', 'backslashreplace').decode('ascii'), flush=True)

    def finish(self, record):
        record = dict(record)
        record.setdefault('machine', machine_info())
        record['wall_seconds'] = round(time.time() - self.started, 1)
        record['finished'] = time.strftime('%Y-%m-%d %H:%M:%S')
        with open(self.json_path, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=1)
        self(f'wrote {self.json_path} ({record["wall_seconds"]} s)')


def machine_info():
    info = {
        'host': platform.node(),
        'os': platform.platform(),
        'python': sys.version.split()[0],
        'tantivy': getattr(tantivy, '__version__', '?'),
        'regex_module': getattr(regex, '__version__', '?'),
        'cpu_count': os.cpu_count(),
    }
    try:
        import psutil
        info['ram_gib'] = round(psutil.virtual_memory().total / 2 ** 30, 1)
    except Exception:
        pass
    return info


def rss_mib():
    try:
        import psutil
        return psutil.Process().memory_info().rss / 2 ** 20
    except Exception:
        return None


def timed(fn, *args, **kwargs):
    started = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, time.perf_counter() - started


def require(path, what):
    if not path or not os.path.exists(path):
        print(f'MISSING  {what}: {path}', flush=True)
        sys.exit(EXIT_MISSING_DATA)


def open_index(index_dir):
    require(index_dir, 'tantivy index')
    idx = tantivy.Index.open(index_dir)
    register_search_tokenizers(idx)
    idx.reload()
    return idx, idx.searcher()


def q_and(*queries):
    return tantivy.Query.boolean_query([(tantivy.Occur.Must, q) for q in queries])


def q_pages(idx):
    return idx.parse_query('scope:page', ['content'])


def q_v08(idx):
    return idx.parse_query('source:"V0.8"', ['content'])


def count_hits(searcher, query):
    return searcher.search(query, 1, count=True).count


def hit_addresses(searcher, query, limit):
    """{(segment_ord, doc)} of the first *limit* hits: a page set that needs no document fetch."""
    return {(addr.segment_ord, addr.doc) for _, addr in searcher.search(query, limit).hits}


def iter_pages(corpus_file):
    """(header, text) for every V0.8 page, with the text built as shared/indexer.py builds
    the page document: each line stripped, joined with newlines (the file's last page is
    joined with spaces, as the indexer's tail does)."""
    with open(corpus_file, encoding='utf-8') as f:
        header, lines = None, []
        for line in f:
            line = line.strip()
            if line.startswith('==>'):
                if header is not None and lines:
                    yield header, '\n'.join(lines)
                header = line.replace('==>', '').replace('<==', '').strip()
                lines = []
            else:
                lines.append(line)
        if header is not None and lines:
            yield header, ' '.join(lines)


_UID_RE = re.compile(r'(IE\d+_P\d+_FL\d+)')
_PNUM_RE = re.compile(r'_P(\d+)_')


def header_uid(header):
    m = _UID_RE.search(header)
    return m.group(1) if m else None


def header_sys_id(header):
    m = CORPUS_SYS_ID_RE.search(header)
    return m.group(1) if m else None


def header_page(header):
    m = _PNUM_RE.search(header)
    return int(m.group(1)) if m else None


def reversed_list_path(out_dir):
    return os.path.join(out_dir, 'reversed_words.txt')


def grammatical_forms(stem):
    """The 600 forms of `#stem#`: 24 prefixes x 25 suffixes (shared/responsa.py's lists)."""
    from shared.responsa import expand_grammatical_prefixes, expand_grammatical_suffixes
    forms = []
    seen = set()
    for suffixed in expand_grammatical_suffixes(stem):
        for form in expand_grammatical_prefixes(suffixed):
            if form not in seen:
                seen.add(form)
                forms.append(form)
    return forms


def prefix_forms(word):
    from shared.responsa import expand_grammatical_prefixes
    return list(dict.fromkeys(expand_grammatical_prefixes(word)))


# --------------------------------------------------------------------------------------
# scan: items 1 + 7 (one pass over the corpus file)
# --------------------------------------------------------------------------------------

def cmd_scan(args):
    log = Log(args.out_dir, 'scan')
    require(args.corpus_file, 'corpus file (Transcriptions.txt)')
    words = set()
    tokens_folded = tokens_raw = pages = pages_without_uid = chars = 0
    punct_tokens = {name: 0 for name in HEBREW_PUNCT.values()}
    punct_samples = {name: [] for name in HEBREW_PUNCT.values()}
    punct_pages = {name: set() for name in HEBREW_PUNCT.values()}
    rss0 = rss_mib()
    started = time.perf_counter()
    for header, text in iter_pages(args.corpus_file):
        pages += 1
        if header_uid(header) is None:
            pages_without_uid += 1
        chars += len(text)
        folded = strip_search_diacritics(text)
        toks = HEBWORD_RE.findall(folded)
        tokens_folded += len(toks)
        words.update(toks)
        if not args.no_raw_count:
            tokens_raw += len(HEBWORD_RE.findall(text))
        for tok in PUNCT_TOKEN_RE.findall(folded):
            for ch, name in HEBREW_PUNCT.items():
                if ch in tok:
                    punct_tokens[name] += 1
                    punct_pages[name].add(header_uid(header) or header)
                    if len(punct_samples[name]) < 5 and tok not in punct_samples[name]:
                        punct_samples[name].append(tok)
        if pages % 100_000 == 0:
            log(f'{pages:,} pages, {tokens_folded:,} tokens, {len(words):,} distinct words, '
                f'{time.perf_counter() - started:.0f} s')
    scan_seconds = time.perf_counter() - started
    log(f'scan done: {pages:,} pages ({pages_without_uid:,} without a uid), {chars:,} chars, '
        f'{tokens_folded:,} folded tokens, {tokens_raw:,} raw tokens, {len(words):,} distinct words, '
        f'{scan_seconds:.1f} s')

    # Item 7: distinct terms containing each character (the token counts above are occurrences).
    punct_terms = {name: sum(1 for w in words if ch in w) for ch, name in HEBREW_PUNCT.items()}
    # Other Hebrew-block punctuation and marks inside tokens, for completeness.
    other_hebrew_nonletters = {}
    for w in words:
        for ch in w:
            if '\u0590' <= ch <= '\u05FF' and not ('\u05D0' <= ch <= '\u05EA'):
                other_hebrew_nonletters[ch] = other_hebrew_nonletters.get(ch, 0) + 1
    other = sorted(((f'U+{ord(ch):04X} {unicodedata.name(ch, "?")} ({unicodedata.category(ch)})', n)
                    for ch, n in other_hebrew_nonletters.items()), key=lambda x: -x[1])

    # Item 1: the reversed, sorted word list.
    started = time.perf_counter()
    rev = sorted(w[::-1] for w in words)
    sort_seconds = time.perf_counter() - started
    path = reversed_list_path(args.out_dir)
    started = time.perf_counter()
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        for r in rev:
            f.write(r)
            f.write('\n')
    write_seconds = time.perf_counter() - started
    size = os.path.getsize(path)
    rss1 = rss_mib()
    log(f'reversed list: {len(rev):,} entries, sort {sort_seconds:.1f} s, write {write_seconds:.1f} s, '
        f'{size / 2 ** 20:.1f} MiB on disk')
    log.finish({
        'corpus_file': args.corpus_file,
        'corpus_bytes': os.path.getsize(args.corpus_file),
        'pages': pages, 'pages_without_uid': pages_without_uid, 'chars': chars,
        'tokens_folded': tokens_folded, 'tokens_raw': (None if args.no_raw_count else tokens_raw),
        'distinct_words': len(words),
        'scan_seconds': round(scan_seconds, 1),
        'reversed_list': {
            'path': path, 'entries': len(rev), 'sort_seconds': round(sort_seconds, 1),
            'write_seconds': round(write_seconds, 1), 'bytes_on_disk': size,
            'build_seconds_total': round(scan_seconds + sort_seconds + write_seconds, 1),
            'process_rss_mib_before': rss0, 'process_rss_mib_after': rss1,
        },
        'punctuation': {
            name: {'token_occurrences': punct_tokens[name], 'distinct_terms': punct_terms[name],
                   'pages': len(punct_pages[name]), 'samples': punct_samples[name]}
            for name in HEBREW_PUNCT.values()
        },
        'other_hebrew_block_non_letters_in_terms': other[:40],
    })
    return EXIT_OK


def cmd_marks(args):
    """Items 2 and 7, the size of the mark question: how many distinct content_search words hold a
    Hebrew mark (U+0591-05C7), a mark of another script, or both -- and how many of the latter
    hold a Hebrew letter at all. Reads the reversed word list."""
    log = Log(args.out_dir, 'marks')
    path = reversed_list_path(args.out_dir)
    require(path, 'reversed word list (run `scan` first)')
    rev, _ = _load_reversed(path, log)
    hebrew = other = both = other_with_hebrew_letter = 0
    by_char = {}
    samples = {}
    for r in rev:
        h = o = False
        for ch in r:
            if unicodedata.category(ch)[0] == 'M':
                if '֑' <= ch <= 'ׇ':
                    h = True
                else:
                    o = True
                    by_char[ch] = by_char.get(ch, 0) + 1
                    if len(samples.setdefault(ch, [])) < 3:
                        samples[ch].append(r[::-1])
        hebrew += h
        other += o
        both += h and o
        if o and any('א' <= ch <= 'ת' for ch in r):
            other_with_hebrew_letter += 1
    top = [{'char': f'U+{ord(ch):04X} {unicodedata.name(ch, "?")}', 'words': n, 'samples': samples[ch]}
           for ch, n in sorted(by_char.items(), key=lambda kv: -kv[1])[:12]]
    log(f'{len(rev):,} distinct words: {hebrew:,} hold a Hebrew mark, {other:,} a mark of another script '
        f'({other_with_hebrew_letter:,} of those hold a Hebrew letter), {both:,} both')
    for row in top:
        log(f'  {row["char"]}: {row["words"]:,} words, e.g. {row["samples"]}')
    log.finish({'distinct_words': len(rev), 'with_hebrew_mark': hebrew, 'with_other_script_mark': other,
                'other_script_mark_and_hebrew_letter': other_with_hebrew_letter, 'both': both, 'top_other_marks': top})
    return EXIT_OK


def cmd_token_only(args):
    """Item 7, exhaustive: every character the tokenizer keeps inside a token that Exact's predicate
    neither continues a word through nor looks past -- so a word boundary to the check, inside one
    index term (maqaf and sof pasuq are two; join controls U+200C/U+200D, letter-numbers and
    alphabetic symbols are the others Rust's \\w admits; Codex round 3 on PR #429). Counts distinct
    content_search words, from the reversed word list."""
    log = Log(args.out_dir, 'token-only')
    path = reversed_list_path(args.out_dir)
    require(path, 'reversed word list (run `scan` first)')
    rev, _ = _load_reversed(path, log)
    by_char = {}
    samples = {}
    words_with_any = words_with_any_and_hebrew = 0
    for r in rev:
        hit = False
        for ch in r:
            if _continues_word(ch) or _looked_past(ch):
                continue
            hit = True
            by_char[ch] = by_char.get(ch, 0) + 1
            if len(samples.setdefault(ch, [])) < 3:
                samples[ch].append(r[::-1])
        if hit:
            words_with_any += 1
            if any('א' <= ch <= 'ת' for ch in r):
                words_with_any_and_hebrew += 1
    rows = [{'char': f'U+{ord(ch):04X} {unicodedata.name(ch, "?")} ({unicodedata.category(ch)})', 'words': n,
             'samples': samples[ch]} for ch, n in sorted(by_char.items(), key=lambda kv: -kv[1])]
    log(f'{len(rev):,} distinct words: {words_with_any:,} hold a token-only boundary character '
        f'({words_with_any_and_hebrew:,} of them also a Hebrew letter); {len(rows)} distinct characters')
    for row in rows[:25]:
        log(f'  {row["char"]}: {row["words"]:,} words, e.g. {row["samples"]}')
    log.finish({'distinct_words': len(rev), 'words_with_token_only_char': words_with_any,
                'words_with_token_only_char_and_hebrew_letter': words_with_any_and_hebrew, 'characters': rows})
    return EXIT_OK


def cmd_punct(args):
    """Item 7 read back from scan.json (the scan does the counting)."""
    path = os.path.join(args.out_dir, 'scan.json')
    require(path, 'scan.json (run `scan` first)')
    with open(path, encoding='utf-8') as f:
        record = json.load(f)
    log = Log(args.out_dir, 'punct')
    for name, row in record['punctuation'].items():
        log(f'{name}: {row["token_occurrences"]:,} tokens, {row["distinct_terms"]:,} terms, '
            f'{row["pages"]:,} pages; samples {row["samples"]}')
    log.finish({'from': path, 'tokens_folded': record['tokens_folded'], 'punctuation': record['punctuation'],
                'other_hebrew_block_non_letters_in_terms': record['other_hebrew_block_non_letters_in_terms']})
    return EXIT_OK


# --------------------------------------------------------------------------------------
# wordlist: item 1, the lookups
# --------------------------------------------------------------------------------------

def _load_reversed(path, log):
    rss0 = rss_mib()
    started = time.perf_counter()
    with open(path, encoding='utf-8') as f:
        rev = f.read().split('\n')
    if rev and rev[-1] == '':
        rev.pop()
    seconds = time.perf_counter() - started
    rss1 = rss_mib()
    log(f'loaded {len(rev):,} reversed words in {seconds:.2f} s; RSS {rss0:.0f} -> {rss1:.0f} MiB')
    return rev, {'load_seconds': round(seconds, 2), 'entries': len(rev),
                 'rss_mib_before': rss0, 'rss_mib_after': rss1,
                 'in_memory_mib_rss_delta': (None if rss0 is None else round(rss1 - rss0, 1)),
                 'python_objects_mib': round((sys.getsizeof(rev) + sum(sys.getsizeof(r) for r in rev)) / 2 ** 20, 1)}


def words_ending_with(rev, ending):
    """Binary search of the reversed list: every word that ends with *ending*."""
    key = ending[::-1]
    lo = bisect.bisect_left(rev, key)
    hi = bisect.bisect_left(rev, key + '\U0010FFFF')
    return [r[::-1] for r in rev[lo:hi]]


def cmd_wordlist(args):
    log = Log(args.out_dir, 'wordlist')
    path = reversed_list_path(args.out_dir)
    require(path, 'reversed word list (run `scan` first)')
    rev, load = _load_reversed(path, log)
    idx, searcher = open_index(args.index_dir)
    schema = idx.schema
    pages_q = q_pages(idx)
    v08_pages, t_v08 = timed(hit_addresses, searcher, q_and(pages_q, q_v08(idx)), 3_000_000)
    log(f'V0.8 page docs: {len(v08_pages):,} ({t_v08:.2f} s)')
    results = {}
    for ending in (NARROW_ENDING, BROAD_ENDING):
        started = time.perf_counter()
        words = words_ending_with(rev, ending)
        bisect_seconds = time.perf_counter() - started
        log(f'ending {ending!r}: {len(words):,} words, binary search + slice {bisect_seconds * 1000:.1f} ms')
        ts_query, t_build = timed(tantivy.Query.term_set_query, schema, 'content_search', words)
        ts_pages, t_ts = timed(hit_addresses, searcher, q_and(ts_query, pages_q), 3_000_000)
        log(f'  term_set_query: {len(ts_pages):,} pages, build {t_build * 1000:.1f} ms, search+collect {t_ts:.2f} s')
        rx_query = tantivy.Query.regex_query(schema, 'content_search', '.*' + ending)
        rx_pages, t_rx = timed(hit_addresses, searcher, q_and(rx_query, pages_q), 3_000_000)
        log(f'  regex_query(.*{ending}): {len(rx_pages):,} pages, search+collect {t_rx:.2f} s')
        # The same two lookups on V0.8 pages only, counted (limit 1, count=True), as the plan's
        # section 3.4 compared them -- one more leading-star run per ending.
        v08_q = q_v08(idx)
        ts_v08_count, t_ts_v08 = timed(count_hits, searcher, q_and(ts_query, pages_q, v08_q))
        rx_v08_count, t_rx_v08 = timed(count_hits, searcher, q_and(rx_query, pages_q, v08_q))
        log(f'  V0.8 pages, count only: term set {ts_v08_count:,} in {t_ts_v08:.2f} s, regex {rx_v08_count:,} in {t_rx_v08:.2f} s')
        ts_v08, rx_v08 = ts_pages & v08_pages, rx_pages & v08_pages
        only_rx = rx_v08 - ts_v08
        only_ts = ts_v08 - rx_v08
        log(f'  V0.8 pages: term set {len(ts_v08):,}, regex {len(rx_v08):,}, identical={ts_v08 == rx_v08} '
            f'(regex only {len(only_rx):,}, term set only {len(only_ts):,}); all pages identical={ts_pages == rx_pages} '
            f'(regex only {len(rx_pages - ts_pages):,}, V0.7 among them)')
        examples = []
        for seg, doc in list(only_rx)[:5]:
            d = searcher.doc(tantivy.DocAddress(seg, doc)).to_dict()
            folded = strip_search_diacritics(d['content'][0])
            toks = sorted({t for t in HEBWORD_RE.findall(folded) if t.endswith(ending)})
            examples.append({'uid': d['unique_id'][0], 'source': d['source'][0], 'tokens': toks[:5]})
        results[ending] = {
            'words': len(words), 'bisect_ms': round(bisect_seconds * 1000, 2),
            'term_set_build_ms': round(t_build * 1000, 1), 'term_set_search_collect_s': round(t_ts, 2),
            'regex_search_collect_s': round(t_rx, 2),
            'v08_count_only': {'term_set_pages': ts_v08_count, 'term_set_s': round(t_ts_v08, 2),
                               'regex_pages': rx_v08_count, 'regex_s': round(t_rx_v08, 2)},
            'pages_all_sources': {'term_set': len(ts_pages), 'regex': len(rx_pages),
                                  'regex_only': len(rx_pages - ts_pages), 'term_set_only': len(ts_pages - rx_pages)},
            'pages_v08': {'term_set': len(ts_v08), 'regex': len(rx_v08), 'identical': ts_v08 == rx_v08,
                          'regex_only': len(only_rx), 'term_set_only': len(only_ts)},
            'regex_only_examples_v08': examples,
            'sample_words': words[:10],
        }
    log.finish({'index_dir': args.index_dir, 'reversed_list': {'path': path, 'bytes_on_disk': os.path.getsize(path), **load},
                'v08_pages': len(v08_pages), 'endings': results})
    return EXIT_OK


# --------------------------------------------------------------------------------------
# patterns: item 2
# --------------------------------------------------------------------------------------

T_BRACKETS = r'[\[\]]*'
# Brackets + the Hebrew marks (points and te'amim, U+0591-05C7): a small class, cheap in DFA states.
T_BRACKETS_HEBREW_MARKS = r'[\[\]\u0591-\u05C7]*'
# Brackets + every Unicode mark (category M), the class the oracle looks past: content_search keeps
# all marks outside U+0300-036F (Hebrew points and te'amim, Arabic harakat in Judeo-Arabic words,
# ...). A Hebrew-only class would miss `\u05D0\u064E\u05D1` (Codex round 2 on PR #429) -- but the all-marks class
# costs so many DFA states that only a single-branch pattern builds with it (measured in
# cmd_patterns), so both classes are measured and the plan carries both.
T_BRACKETS_MARKS = r'[\[\]\p{M}]*'
TOLERANCES = (('brackets+all marks', T_BRACKETS_MARKS), ('brackets+Hebrew marks', T_BRACKETS_HEBREW_MARKS),
              ('brackets', T_BRACKETS), ('none', ''))


def tolerant(word, between):
    return between.join(re.escape(ch) for ch in word)


def alternation(forms, between, edges=True):
    body = '(?:' + '|'.join(tolerant(f, between) for f in forms) + ')'
    return (between + body + between) if edges else body


def _pattern_case(log, idx, searcher, name, pattern, plain_query, plain_label, examples_from_forms=None):
    schema = idx.schema
    pages_q = q_pages(idx)
    row = {'name': name, 'pattern_chars': len(pattern), 'pattern_head': pattern[:120]}
    try:
        query, t_build = timed(tantivy.Query.regex_query, schema, 'content_search', pattern)
    except Exception as exc:  # noqa: BLE001 -- the error text is the measurement
        row['build_error'] = f'{type(exc).__name__}: {exc}'
        log(f'{name}: BUILD ERROR {row["build_error"]}')
        return row
    row['build_ms'] = round(t_build * 1000, 2)
    _, t_rx_count = timed(count_hits, searcher, q_and(query, pages_q))
    rx_pages, t_rx = timed(hit_addresses, searcher, q_and(query, pages_q), 3_000_000)
    row['regex_pages'] = len(rx_pages)
    row['regex_count_s'] = round(t_rx_count, 3)
    row['regex_search_collect_s'] = round(t_rx, 3)
    _, t_plain_count = timed(count_hits, searcher, q_and(plain_query, pages_q))
    plain_pages, t_plain = timed(hit_addresses, searcher, q_and(plain_query, pages_q), 3_000_000)
    row['plain'] = {'label': plain_label, 'pages': len(plain_pages), 'count_s': round(t_plain_count, 3),
                    'search_collect_s': round(t_plain, 3)}
    only_rx = rx_pages - plain_pages
    row['regex_only_pages'] = len(only_rx)
    row['plain_only_pages'] = len(plain_pages - rx_pages)
    examples = []
    if examples_from_forms is not None:
        # Why the pattern reaches pages the plain forms do not: a bracket at the token's edge
        # (today's query adds those as extra terms), a bracket inside the word, or a mark.
        forms = set(examples_from_forms)
        checker = regex.compile(pattern)
        kinds = {}
        for seg, doc in list(only_rx)[:2000]:
            d = searcher.doc(tantivy.DocAddress(seg, doc)).to_dict()
            toks = sorted({t for t in HEBWORD_RE.findall(strip_search_diacritics(d['content'][0]))
                           if checker.fullmatch(t) and t not in forms})
            page_kinds = set()
            for t in toks:
                inner = t.strip('[]')
                if any(unicodedata.category(ch)[0] == 'M' for ch in t):
                    page_kinds.add('mark inside the token')
                elif '[' in inner or ']' in inner:
                    page_kinds.add('bracket inside the word')
                elif inner != t:
                    page_kinds.add('bracket at the token edge only')
                else:
                    page_kinds.add('other')
            key = ' + '.join(sorted(page_kinds)) or 'no token found'
            kinds[key] = kinds.get(key, 0) + 1
            if len(examples) < 5:
                examples.append({'uid': d['unique_id'][0], 'tokens': toks[:5]})
        row['regex_only_kinds'] = kinds
    row['regex_only_examples'] = examples
    log(f'{name}: {len(pattern):,} chars, build {t_build * 1000:.1f} ms, count {t_rx_count:.2f} s, '
        f'search+collect {t_rx:.2f} s, {len(rx_pages):,} pages; {plain_label}: {len(plain_pages):,} pages, '
        f'count {t_plain_count:.2f} s, search+collect {t_plain:.2f} s; pattern-only {len(only_rx):,}')
    return row


def cmd_patterns(args):
    log = Log(args.out_dir, 'patterns')
    idx, searcher = open_index(args.index_dir)
    schema = idx.schema
    rows = []

    def rx(p):
        return tantivy.Query.regex_query(schema, 'content_search', p)

    def ts(forms):
        return tantivy.Query.term_set_query(schema, 'content_search', list(forms))

    # (a) a star word, trailing star, with each tolerance class between the letters
    for t_label, between in TOLERANCES[:3]:
        rows.append(_pattern_case(log, idx, searcher, f'a1 trailing star, {t_label}',
                                  tolerant(STEM_1, between) + between + '.*', rx(STEM_1 + '.*'), f'regex {STEM_1}.*'))
    # (a) the leading-star form
    for t_label, between in TOLERANCES[:3]:
        rows.append(_pattern_case(log, idx, searcher, f'a2 leading star, {t_label}',
                                  '.*' + tolerant(FINITE_WORD, between) + between, rx('.*' + FINITE_WORD),
                                  f'regex .*{FINITE_WORD}'))
    # (b) a finite word: its 24 prefix forms in one alternation
    forms24 = prefix_forms(FINITE_WORD)
    for t_label, between in TOLERANCES[:2]:
        rows.append(_pattern_case(log, idx, searcher, f'b {len(forms24)} prefix forms, {t_label}',
                                  alternation(forms24, between), ts(forms24), f'term_set {len(forms24)} forms',
                                  examples_from_forms=forms24))
    # (c) 200 stem alternatives, what spelling variants produce
    try:
        from shared.variants import VariantManager
        variants = list(dict.fromkeys(VariantManager().get_variants(FINITE_WORD, 'variants_extended', limit=200)))
        variants_source = 'VariantManager.get_variants(variants_extended, limit=200)'
    except Exception as exc:  # noqa: BLE001
        variants, variants_source = [], f'VariantManager unavailable: {exc}'
    for t_label, between in TOLERANCES[:2]:
        if variants:
            rows.append(_pattern_case(log, idx, searcher, f'c {len(variants)} spelling variants, {t_label}',
                                      alternation(variants, between), ts(variants),
                                      f'term_set {len(variants)} variants', examples_from_forms=variants))
    # (d) the 600 forms of #X#
    forms600 = grammatical_forms(STEM_1)
    for t_label, between in TOLERANCES[:3]:
        rows.append(_pattern_case(log, idx, searcher, f'd {len(forms600)} forms of #{STEM_1}#, {t_label}',
                                  alternation(forms600, between), ts(forms600), f'term_set {len(forms600)} forms',
                                  examples_from_forms=forms600))

    # The size limit: grow an alternation until regex_query raises, for three kinds of list and
    # with/without the tolerance classes (the limit is on DFA states, so structure matters).
    pools = []
    rev_path = reversed_list_path(args.out_dir)
    if os.path.exists(rev_path):
        rev, _ = _load_reversed(rev_path, log)
        corpus_words = words_ending_with(rev, BROAD_ENDING)
        del rev
        pools.append((f'distinct corpus words ending in {BROAD_ENDING} ({len(corpus_words):,} available)', corpus_words))
    else:
        log('no word list (run `scan` first): the corpus-word probe is skipped')
    if variants:
        pools.append((f'spelling variants of {FINITE_WORD} ({variants_source})', variants))
    pools.append((f'grammatical forms: #{STEM_1}# then #{STEM_2}# (24 x 25 each)', forms600 + grammatical_forms(STEM_2)))
    probes = []
    for label, pool in pools:
        for between_label, between in TOLERANCES:
            probes.append(_probe_limit(log, idx, searcher, f'{label}; tolerance {between_label}', pool, between))
    log.finish({'index_dir': args.index_dir, 'tolerance_classes': dict(TOLERANCES),
                'variants_source': variants_source, 'cases': rows, 'size_limit_probes': probes})
    return EXIT_OK


def _probe_limit(log, idx, searcher, label, pool, between):
    """The largest alternation of *pool*'s first N entries that regex_query accepts: doubling from
    8, then a bisection to the exact N; the first error text; a search with the largest."""
    schema = idx.schema

    def build(n):
        pattern = alternation(pool[:n], between, edges=bool(between))
        step = {'alternatives': n, 'pattern_chars': len(pattern)}
        try:
            _, t_build = timed(tantivy.Query.regex_query, schema, 'content_search', pattern)
            step['build_s'] = round(t_build, 4)
        except Exception as exc:  # noqa: BLE001 -- the error text is the measurement
            step['error'] = f'{type(exc).__name__}: {exc}'
        return step

    probe = {'label': label, 'available': len(pool), 'steps': []}
    last_ok, first_bad = 0, None
    n = 8
    while True:
        n = min(n, len(pool))
        step = build(n)
        probe['steps'].append(step)
        if 'error' in step:
            first_bad = n
            break
        last_ok = n
        if n >= len(pool):
            break
        n *= 2
    if first_bad is not None:
        lo, hi = last_ok, first_bad
        while hi - lo > 1:
            mid = (lo + hi) // 2
            step = build(mid)
            probe['steps'].append(step)
            if 'error' in step:
                hi = mid
            else:
                lo = mid
        probe['largest_ok_alternatives'] = lo
        probe['smallest_failing_alternatives'] = hi
        probe['first_error'] = next(s['error'] for s in probe['steps'] if 'error' in s)
    else:
        probe['largest_ok_alternatives'] = last_ok
        probe['note'] = f'no error: the whole list of {len(pool):,} builds'
    lo = probe['largest_ok_alternatives']
    if lo:
        pattern = alternation(pool[:lo], between, edges=bool(between))
        probe['largest_ok_pattern_chars'] = len(pattern)
        query, t_build = timed(tantivy.Query.regex_query, schema, 'content_search', pattern)
        pages, t_rx = timed(count_hits, searcher, q_and(query, q_pages(idx)))
        probe['largest_ok'] = {'build_s': round(t_build, 4), 'pages': pages, 'count_s': round(t_rx, 3)}
    log(f'size probe [{label}]: largest ok {lo:,} of {len(pool):,}'
        + (f', first failure at {probe["smallest_failing_alternatives"]:,}: {probe["first_error"]}' if first_bad else ' (no error)')
        + (f'; largest builds in {probe["largest_ok"]["build_s"]:.3f} s, {probe["largest_ok"]["pages"]:,} pages in '
           f'{probe["largest_ok"]["count_s"]:.2f} s ({probe["largest_ok_pattern_chars"]:,} chars)' if lo else ''))
    return probe


# --------------------------------------------------------------------------------------
# scored-terms: item 3
# --------------------------------------------------------------------------------------

def cmd_scored_terms(args):
    log = Log(args.out_dir, 'scored-terms')
    idx, searcher = open_index(args.index_dir)
    schema = idx.schema
    pages_q = q_pages(idx)
    forms1, forms2 = grammatical_forms(STEM_1), grammatical_forms(STEM_2)

    def or_terms(forms):
        return tantivy.Query.boolean_query(
            [(tantivy.Occur.Should, tantivy.Query.term_query(schema, 'content_search', f)) for f in forms])

    def run(name, build):
        query, t_build = timed(build)
        full = q_and(query, pages_q)
        result, t_search = timed(searcher.search, full, 10, count=True)
        scores = [round(s, 4) for s, _ in result.hits]
        row = {'name': name, 'build_ms': round(t_build * 1000, 2), 'search_s': round(t_search, 3),
               'candidates': result.count, 'top10_scores': scores, 'distinct_top10_scores': len(set(scores))}
        log(f'{name}: build {t_build * 1000:.1f} ms, search {t_search:.3f} s, {result.count:,} candidates, '
            f'top-10 scores {scores} ({row["distinct_top10_scores"]} distinct)')
        return row

    rows = [
        run(f'OR of {len(forms1)} term_query (#{STEM_1}#)', lambda: or_terms(forms1)),
        run(f'term_set_query of the same {len(forms1)} forms', lambda: tantivy.Query.term_set_query(schema, 'content_search', forms1)),
        run(f'OR {len(forms1)} (#{STEM_1}#) AND OR {len(forms2)} (#{STEM_2}#): {len(forms1) + len(forms2)} term_query',
            lambda: q_and(or_terms(forms1), or_terms(forms2))),
        run(f'OR of {len(forms1) + len(forms2)} term_query (both lists, one OR)', lambda: or_terms(forms1 + forms2)),
        run(f'term_set_query of {len(forms1) + len(forms2)} forms', lambda: tantivy.Query.term_set_query(schema, 'content_search', forms1 + forms2)),
    ]
    # The second run of the first query, warm.
    rows.append(run(f'OR of {len(forms1)} term_query (#{STEM_1}#), second run', lambda: or_terms(forms1)))
    log.finish({'index_dir': args.index_dir, 'stems': [STEM_1, STEM_2], 'forms': [len(forms1), len(forms2)], 'runs': rows})
    return EXIT_OK


# --------------------------------------------------------------------------------------
# missing-pages: item 4
# --------------------------------------------------------------------------------------

def _classify_token(word, tokens):
    """Why the tokenizer's token(s) covering *word* do not match `.*ending` as whole terms."""
    if not tokens:
        return 'no token covers the word'
    if len(tokens) > 1:
        return 'the tokenizer split the word into %d tokens' % len(tokens)
    tok = tokens[0]
    names = []
    for ch, name in HEBREW_PUNCT.items():
        if ch in tok:
            names.append(name)
    if names:
        return 'token holds ' + ', '.join(names) + (' (joins two words)' if tok != word else '')
    if tok and tok[-1] in '[]':
        return 'bracket glued at the token end'
    if tok and unicodedata.category(tok[-1])[0] == 'M':
        return 'mark after the last letter of the token'
    inner = tok.strip('[]')
    if '[' in inner or ']' in inner:
        return 'bracket inside the word'
    if any(unicodedata.category(ch)[0] == 'M' for ch in inner):
        return 'mark inside the word'
    if tok != word:
        extra = sorted({ch for ch in tok if ch not in word})
        return 'token joins neighbouring characters: ' + ', '.join(
            f'U+{ord(c):04X} {unicodedata.name(c, "?")}' for c in extra)
    return 'other'


def _non_letters(s):
    return [f'U+{ord(c):04X} {unicodedata.name(c, "?")} ({unicodedata.category(c)})'
            for c in dict.fromkeys(s) if unicodedata.category(c)[0] != 'L']


def diagnose_missing(idx, searcher, uid, header, file_text, pair):
    out = {'uid': uid, 'sys_id': header_sys_id(header), 'page': header_page(header),
           'words': list(pair), 'non_letters_in_words': [_non_letters(w) for w in pair]}
    q = idx.parse_query(f'unique_id:"{uid}" AND scope:page AND source:"V0.8"', ['content'])
    hits = searcher.search(q, 5, count=True)
    out['index_page_docs'] = hits.count
    if hits.count == 0:
        out['cause'] = 'page not in the index'
        return out
    doc = searcher.doc(hits.hits[0][1]).to_dict()
    content = doc['content'][0]
    out['index_text_equals_file'] = content == file_text
    folded = strip_search_diacritics(content)
    tokens = [(m.start(), m.end(), m.group()) for m in HEBWORD_RE.finditer(folded)]
    rust1 = regex.compile('.*' + STAR_WORD_1[1:])
    rust2 = regex.compile('.*' + STAR_WORD_2[1:])
    has1 = any(rust1.fullmatch(t) for _, _, t in tokens)
    has2 = any(rust2.fullmatch(t) for _, _, t in tokens)
    out['index_tokens_match'] = {STAR_WORD_1: has1, STAR_WORD_2: has2}
    if has1 and has2:
        out['cause'] = 'unexplained: the index holds a token for each word'
        return out
    pos = folded.find(pair[0])
    causes = []
    for label, word, has in (('first', pair[0], has1), ('second', pair[1], has2)):
        if has:
            continue
        wpos = folded.find(word, max(pos, 0)) if pos >= 0 else folded.find(word)
        covering = [t for s, e, t in tokens if wpos >= 0 and s < wpos + len(word) and e > wpos]
        out[f'{label}_word_tokens'] = covering
        cause = _classify_token(word, covering)
        out[f'{label}_word_cause'] = cause
        causes.append(f'{label} word: {cause}')
    out['cause'] = '; '.join(causes) if causes else 'other'
    return out


def cmd_missing_pages(args):
    log = Log(args.out_dir, 'missing-pages')
    require(args.corpus_file, 'corpus file (Transcriptions.txt)')
    idx, searcher = open_index(args.index_dir)
    schema = idx.schema
    # Stage 1: two leading-star lookups ANDed, V0.8 page docs.
    q = q_and(tantivy.Query.regex_query(schema, 'content_search', '.*' + STAR_WORD_1[1:]),
              tantivy.Query.regex_query(schema, 'content_search', '.*' + STAR_WORD_2[1:]),
              q_pages(idx), q_v08(idx))
    result, t_stage1 = timed(searcher.search, q, 3_000_000)
    hits = result.hits
    log(f'stage 1: {len(hits):,} candidate V0.8 pages in {t_stage1:.2f} s')
    # Stage 2: fetch each page and run the letters-only check.
    index_found = {}
    chars = 0
    started = time.perf_counter()
    t_fetch = 0.0
    for i, (_, addr) in enumerate(hits):
        t0 = time.perf_counter()
        doc = searcher.doc(addr).to_dict()
        t_fetch += time.perf_counter() - t0
        content = doc['content'][0]
        chars += len(content)
        pair = first_pair(strip_search_diacritics(content), STAR_WORD_1, STAR_WORD_2)
        if pair is not None:
            index_found[doc['unique_id'][0]] = {'header': doc['full_header'][0], 'pair': pair}
        if (i + 1) % 20_000 == 0:
            log(f'  stage 2: {i + 1:,} checked, {len(index_found):,} pass, {time.perf_counter() - started:.0f} s')
    t_stage2 = time.perf_counter() - started
    log(f'stage 2: {len(index_found):,} pages pass, {chars:,} chars read, {t_stage2:.1f} s (fetch {t_fetch:.1f} s)')
    # Brute force over the file. The prefilter must not be narrower than the oracle: it looks for
    # the two endings in the page's text with every looked-past character removed (marks, quotes,
    # brackets), exactly what star_word compares against -- a literal test would skip `נ[י]ם` and
    # `נִים` (Codex review of PR #429).
    brute = {}
    pages = prefiltered = duplicates = 0
    started = time.perf_counter()
    for header, text in iter_pages(args.corpus_file):
        pages += 1
        folded = strip_search_diacritics(text)
        core_text = LOOKED_PAST_RE.sub('', folded)
        if STAR_WORD_1[1:] not in core_text or STAR_WORD_2[1:] not in core_text:
            continue
        prefiltered += 1
        pair = first_pair(folded, STAR_WORD_1, STAR_WORD_2)
        if pair is not None:
            uid = header_uid(header)
            if uid in brute:
                duplicates += 1
            brute[uid] = {'header': header, 'pair': pair, 'text': text}
        if pages % 200_000 == 0:
            log(f'  brute force: {pages:,} pages, {len(brute):,} matches, {time.perf_counter() - started:.0f} s')
    t_brute = time.perf_counter() - started
    log(f'brute force: {pages:,} pages, {prefiltered:,} prefiltered, {len(brute):,} matching pages '
        f'({duplicates} duplicate uids), {t_brute:.1f} s')
    missing = sorted(set(brute) - set(index_found), key=lambda u: (brute[u]['header']))
    extra = sorted(set(index_found) - set(brute))
    log(f'brute force only: {len(missing):,}; index path only: {len(extra):,}')
    diagnoses = []
    for uid in missing:
        row = brute[uid]
        d = diagnose_missing(idx, searcher, uid, row['header'], row['text'], row['pair'])
        diagnoses.append(d)
        log(f'  missing {d["sys_id"]} p{d["page"]} {uid}: {d["cause"]} | words {d["words"]}')
    causes = {}
    for d in diagnoses:
        causes[d['cause']] = causes.get(d['cause'], 0) + 1
    extra_rows = [{'uid': u, 'header': index_found[u]['header'], 'pair': list(index_found[u]['pair'])} for u in extra[:20]]
    log.finish({
        'index_dir': args.index_dir, 'corpus_file': args.corpus_file, 'query': f'{STAR_WORD_1} {STAR_WORD_2}',
        'stage1': {'candidates': len(hits), 'seconds': round(t_stage1, 2)},
        'stage2': {'pages': len(index_found), 'seconds': round(t_stage2, 1), 'fetch_seconds': round(t_fetch, 1), 'chars': chars},
        'brute_force': {'pages_scanned': pages, 'prefiltered': prefiltered, 'pages': len(brute), 'duplicate_uids': duplicates,
                        'seconds': round(t_brute, 1)},
        'brute_only': len(missing), 'index_only': len(extra), 'causes': causes,
        'missing_pages': diagnoses, 'index_only_pages': extra_rows,
    })
    return EXIT_OK


def cmd_verify_tolerant(args):
    """Item 4, closing check: does the bracket- and mark-tolerant stage 1 of section 5.1
    (`.*נ[\\[\\]\\u0591-\\u05C7]*י...ם[...]*`: the tolerance class between the letters and at the
    end) admit every page the plain stage 1 missed? Reads missing-pages.json."""
    log = Log(args.out_dir, 'verify-tolerant')
    path = os.path.join(args.out_dir, 'missing-pages.json')
    require(path, 'missing-pages.json (run `missing-pages` first)')
    with open(path, encoding='utf-8') as f:
        record = json.load(f)
    missing = [m['uid'] for m in record['missing_pages']]
    idx, searcher = open_index(args.index_dir)
    schema = idx.schema
    tolerant_q = q_and(
        tantivy.Query.regex_query(schema, 'content_search',
                                  '.*' + tolerant(STAR_WORD_1[1:], T_BRACKETS_MARKS) + T_BRACKETS_MARKS),
        tantivy.Query.regex_query(schema, 'content_search',
                                  '.*' + tolerant(STAR_WORD_2[1:], T_BRACKETS_MARKS) + T_BRACKETS_MARKS),
        q_pages(idx), q_v08(idx))
    candidates, seconds = timed(hit_addresses, searcher, tolerant_q, 3_000_000)
    log(f'tolerant stage 1: {len(candidates):,} candidate V0.8 pages in {seconds:.2f} s '
        f'(plain stage 1: {record["stage1"]["candidates"]:,} in {record["stage1"]["seconds"]} s)')
    found = {}
    for uid in missing:
        q = idx.parse_query(f'unique_id:"{uid}" AND scope:page AND source:"V0.8"', ['content'])
        addrs = {(a.segment_ord, a.doc) for _, a in searcher.search(q, 5).hits}
        found[uid] = bool(addrs & candidates)
        log(f'  {uid}: {"admitted" if found[uid] else "STILL MISSING"}')
    log.finish({'index_dir': args.index_dir, 'tolerant_candidates': len(candidates), 'seconds': round(seconds, 2),
                'plain_candidates': record['stage1']['candidates'], 'missing_pages_admitted': found,
                'all_admitted': all(found.values()) if found else None})
    return EXIT_OK


# --------------------------------------------------------------------------------------
# topology: item 5
# --------------------------------------------------------------------------------------

_IE_RE = re.compile(r'(IE\d+)')


def _junction_kinds(a, b):
    kinds = []
    if (a.get('source') or '') != (b.get('source') or ''):
        kinds.append('transcription change (V0.8 -> V0.7 in one document)')
    if (a.get('sys_id') or '') != (b.get('sys_id') or ''):
        kinds.append('manuscript (sys_id) change')
    ie_a = _IE_RE.search(a.get('uid') or a.get('full_header') or '')
    ie_b = _IE_RE.search(b.get('uid') or b.get('full_header') or '')
    same_ie = bool(ie_a and ie_b and ie_a.group(1) == ie_b.group(1))
    if ie_a and ie_b and not same_ie:
        kinds.append('volume (IE) change')
    if not kinds:
        pa, pb = a.get('p_num'), b.get('p_num')
        if pa is None or pb is None:
            kinds.append('page number missing')
        elif pb != pa + 1:
            kinds.append('page number jump forward' if pb > pa else 'page number back or repeated')
    return kinds


def cmd_topology(args):
    log = Log(args.out_dir, 'topology')
    idx, searcher = open_index(args.index_dir)
    schema = idx.schema
    record = {'index_dir': args.index_dir, 'scopes': {}}
    bm_path = args.browse_map
    if bm_path and os.path.exists(bm_path):
        record['browse_map'] = {'path': bm_path, 'bytes': os.path.getsize(bm_path)}
        log(f'browse_map.pkl: {os.path.getsize(bm_path):,} bytes (a 37-byte stub replaced it once; this is the real one)'
            if os.path.getsize(bm_path) > 1_000_000 else f'browse_map.pkl is only {os.path.getsize(bm_path)} bytes: NOT trusted')
    for scope in ('system', 'part'):
        started = time.perf_counter()
        hits = searcher.search(tantivy.Query.term_query(schema, 'scope', scope), 3_000_000).hits
        docs = junctions = pages = 0
        clean = 0
        kinds_count = {}
        examples = {}
        first_source = {}
        docs_with_odd = 0
        max_pages = 0
        for i, (_, addr) in enumerate(hits):
            doc = searcher.doc(addr).to_dict()
            try:
                bounds = json.loads(doc['boundaries'][0]) if doc.get('boundaries') and doc['boundaries'][0] else []
            except Exception:  # noqa: BLE001
                bounds = []
            docs += 1
            pages += len(bounds)
            max_pages = max(max_pages, len(bounds))
            src = doc['source'][0] if doc.get('source') else ''
            first_source[src] = first_source.get(src, 0) + 1
            odd = False
            for a, b in zip(bounds, bounds[1:]):
                junctions += 1
                kinds = _junction_kinds(a, b)
                if not kinds:
                    clean += 1
                    continue
                odd = True
                key = ' + '.join(kinds)
                kinds_count[key] = kinds_count.get(key, 0) + 1
                if len(examples.setdefault(key, [])) < 3:
                    examples[key].append({'doc': doc['unique_id'][0],
                                          'from': {'uid': a.get('uid'), 'p_num': a.get('p_num'), 'source': a.get('source'), 'sys_id': a.get('sys_id')},
                                          'to': {'uid': b.get('uid'), 'p_num': b.get('p_num'), 'source': b.get('source'), 'sys_id': b.get('sys_id')}})
            if odd:
                docs_with_odd += 1
            if (i + 1) % 50_000 == 0:
                log(f'  {scope}: {i + 1:,} docs, {junctions:,} junctions, {time.perf_counter() - started:.0f} s')
        seconds = time.perf_counter() - started
        not_clean = junctions - clean
        log(f'{scope}: {docs:,} docs, {pages:,} page slots (max {max_pages:,} per doc), {junctions:,} junctions, '
            f'{clean:,} consecutive, {not_clean:,} not ({100.0 * not_clean / max(junctions, 1):.2f}%), '
            f'{docs_with_odd:,} docs with at least one; first source {first_source}; {seconds:.1f} s')
        for key, n in sorted(kinds_count.items(), key=lambda kv: -kv[1]):
            log(f'    {n:,}  {key}')
        record['scopes'][scope] = {'docs': docs, 'page_slots': pages, 'max_pages_per_doc': max_pages, 'junctions': junctions,
                                   'consecutive': clean, 'not_consecutive': not_clean, 'docs_with_a_non_consecutive_junction': docs_with_odd,
                                   'first_source': first_source, 'kinds': kinds_count, 'examples': examples, 'seconds': round(seconds, 1)}
    log.finish(record)
    return EXIT_OK


# --------------------------------------------------------------------------------------
# desktop-timings: item 6
# --------------------------------------------------------------------------------------

PHASE1_QUERIES = [
    ('#כהנים #גדולים', {}),
    ('#כהנ# גדולים', {}),
    ('כהנים גדול', {}),
    ('ספר', {}),
    ('כהנים [2] גדולים', {}),
    ('כהנים גדולים', {'bidirectional': True}),
    ('כהנים AND גדולים', {}),
    ('|כהנים גדולים', {}),
    ('כהנים גדולים -אהרן', {}),
]
PHASE2_QUERIES = [
    ('*כהנים *גדולים', {}),
    ('*נים *לים', {}),
    ('כ*נים גדולים', {}),
    ('*כ*ה*נ*ים גדולים', {}),
]


class _Deadline:
    """A progress callback that stops the search after *seconds*, as the desktop's Stop does
    (the engine swallows InterruptedError and returns the rows so far, marked interrupted)."""

    def __init__(self, seconds):
        self.end = time.monotonic() + seconds
        self.fired = False
        self.calls = 0

    def __call__(self, current, total):
        self.calls += 1
        if time.monotonic() > self.end:
            self.fired = True
            raise InterruptedError('measurement deadline')


def cmd_desktop_timings(args):
    # A run of one engine gets its own files (desktop-timings-re.json), so runs do not overwrite each other.
    suffix = '' if set(args.engines) == {'regex', 're'} else '-' + '-'.join(args.engines)
    log = Log(args.out_dir, 'desktop-timings' + suffix)
    from unittest.mock import patch
    from shared.config import Config
    index_dir = os.path.dirname(args.index_dir.rstrip('/\\'))
    with patch.object(Config, 'INDEX_DIR', index_dir):
        from shared.metadata_manager import MetadataManager
        from shared.variants import VariantManager
        import shared.search_engine as se
        from shared.search_regex import isolated_matching
        started = time.perf_counter()
        meta = MetadataManager()
        meta._load_heavy_caches_bg()
        # worker_mode=True: no background fl_id index build competing for the CPU
        # (the desktop starts one at launch and it is finished before the user searches);
        # open_local=False: the LOCAL side-index is not consulted at corpus_scope='genizah'.
        engine = se.SearchEngine(meta, VariantManager(), worker_mode=True, open_local=False)
        log(f'engine ready in {time.perf_counter() - started:.1f} s; SEARCH_LIMIT={Config.SEARCH_LIMIT}')
        # Factories: a @contextmanager object can be entered once, so one is made per query.
        engines = {'regex': nullcontext, 're': lambda: isolated_matching(native=True)}
        rows = []
        warm = engine.execute_search('ספר', 'literal', 0, corpus_scope='genizah')
        se.consume_last_search_cutoff()
        log(f'warm-up Exact ספר: {len(warm):,} rows')
        for engine_name in args.engines:
            for phase, queries in (('phase1', PHASE1_QUERIES), ('phase2', PHASE2_QUERIES)):
                for query, extra in queries:
                    options = {'responsa_mode': True, 'variants': False, 'ja': False, 'flex_spacing': False,
                               'bidirectional': False, 'variant_mode': 'exact'}
                    options.update(extra)
                    deadline = _Deadline(args.query_timeout)
                    row = {'engine': engine_name, 'phase': phase, 'query': query, 'options': extra}
                    se.consume_last_search_cutoff()
                    se._consume_last_responsa_downgrade_meta()
                    started = time.perf_counter()
                    try:
                        with engines[engine_name]():
                            results = engine.execute_search(query, 'exact', 0, progress_callback=deadline,
                                                            responsa_options=options, corpus_scope='genizah')
                        row['rows'] = len(results)
                    except Exception as exc:  # noqa: BLE001 -- the message is the measurement
                        row['error'] = f'{type(exc).__name__}: {exc}'
                    row['seconds'] = round(time.perf_counter() - started, 2)
                    cutoff = se.consume_last_search_cutoff()
                    row['capped_N_plus'] = bool(cutoff.get('capped'))
                    row['interrupted'] = bool(cutoff.get('interrupted'))
                    row['stopped_by_deadline'] = deadline.fired
                    row['downgrade'] = se._consume_last_responsa_downgrade_meta()
                    rows.append(row)
                    log(f'{engine_name:5} {phase} {query!r} {extra or ""}: '
                        + (f'{row["rows"]:,} rows' if 'rows' in row else row['error'])
                        + f', {row["seconds"]} s, N+={row["capped_N_plus"]}, interrupted={row["interrupted"]}'
                        + (f', downgrade={row["downgrade"]}' if row['downgrade'] else ''))
    log.finish({'index_dir': args.index_dir, 'search_limit': Config.SEARCH_LIMIT, 'query_timeout_s': args.query_timeout,
                'engines': args.engines, 'runs': rows})
    return EXIT_OK


# --------------------------------------------------------------------------------------

def main(argv=None):
    from shared.config import Config
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('item', choices=['scan', 'punct', 'marks', 'token-only', 'wordlist', 'patterns', 'scored-terms',
                                     'missing-pages', 'verify-tolerant', 'topology', 'desktop-timings'])
    ap.add_argument('--index-dir', default=os.path.join(Config.INDEX_DIR, 'tantivy_db'))
    ap.add_argument('--corpus-file', default=os.environ.get('GENIZAH_CORPUS_FILE') or Config.FILE_V8)
    ap.add_argument('--browse-map', default=Config.BROWSE_MAP)
    ap.add_argument('--out-dir', default=os.path.join(REPO_ROOT, '_tmp', 'responsa_wildcards_phase0'))
    ap.add_argument('--no-raw-count', action='store_true', help='scan: skip the raw (unfolded) token count')
    ap.add_argument('--engines', nargs='+', choices=['regex', 're'], default=['regex', 're'],
                    help='desktop-timings: regex = the desktop route, re = the production worker route')
    ap.add_argument('--query-timeout', type=float, default=600.0, help='desktop-timings: stop a query after this many seconds')
    args = ap.parse_args(argv)
    handler = {
        'scan': cmd_scan, 'punct': cmd_punct, 'marks': cmd_marks, 'token-only': cmd_token_only,
        'wordlist': cmd_wordlist, 'patterns': cmd_patterns,
        'scored-terms': cmd_scored_terms, 'missing-pages': cmd_missing_pages, 'verify-tolerant': cmd_verify_tolerant,
        'topology': cmd_topology, 'desktop-timings': cmd_desktop_timings,
    }[args.item]
    return handler(args)


if __name__ == '__main__':
    sys.exit(main())

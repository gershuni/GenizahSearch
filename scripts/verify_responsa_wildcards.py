# -*- coding: utf-8 -*-
"""Ground truth for Responsa search on the real data (docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md,
section 8, gate 1), one query set per phase.

The oracle here is independent of the engine: it does not import build_regex_pattern or the
word compiler (shared/responsa_match.py), and it never runs the engine's patterns. It reads
Transcriptions.txt page by page, folds each page as the index does (strip_search_diacritics),
takes a bracket-free query's text with its brackets out (the one bracket rule, plan section 4),
cuts the page into whole words by Exact's predicate (letters, decimal digits and connector
punctuation go on a word; marks, in-word quotes and brackets are passed over; any other
character ends it) -- not into index tokens -- and applies the contract's word rules: every
word a whole word, the first and last included; `#` the grammatical lists; `[N]` up to N
words between; `AND` every word somewhere on the page; a line group on one line, its words
separated by whitespace, `|` its start. Each witness is the page and the span of its first
acceptable match, mapped back to the page's original characters and trimmed to its first and
last letter.

The engine runs the same queries (SearchEngine.execute_search, corpus_scope 'genizah', the
production worker's stdlib-re route, SEARCH_LIMIT raised so nothing is cut). Witnesses are
compared in both directions: a missing witness fails, an extra one fails, a wrong span on a
page that also has a valid match fails. Differences of a documented kind are listed by count
and do not fail (plan section 4: maqaf and sof pasuq, kept inside a token, where Exact's
predicate ends a word; a foreign mark inside a Hebrew word; and the whole-manuscript
documents' kinds -- an AND whose words sit on different pages.

Phase 3 (`--phase 3`): a star word is matched by the oracle's own reading of the contract
(``StarWord``: any letters and marks for a star, never a digit, `_`, space or punctuation; a
letter before a star in its final or medial form). **Crossings are compared too, seam-aware:**
for every two V0.8 pages that follow each other in one volume (same sys_id and IE, page number
+ 1), the first acceptable match that runs from one into the other, on the pages joined by a
line break -- a crossing row only where the first page has no match of its own (first wins).
The engine's crossing rows are compared both ways; the rows a whole-manuscript document makes at
a junction the seams do not hold (into the V0.7 copy, between pages that do not follow each
other) and a match over more than two pages are listed by count. A crossing the engine lacks
fails for a star query; for finite words it is listed (they read whole manuscripts, first match
only, until decision 10).

A synthetic crossing fixture, ordered pages of one manuscript with their physical order and
source, is indexed in a temporary directory and checked the same way, crossing witnesses
included. tests/test_verify_responsa_wildcards.py runs the whole gate on a small corpus built
by the real Indexer, as format 2 and as format 1.

--mutation NAME runs the same check against a deliberately broken engine, to show the gate can
fail (each must exit 1): accept-off (no whole-word acceptance), raw-prefix-tree (one prefix
tree for forms of different lengths), guard-500 (the old 500-term limit), no-crossing (whole-
manuscript documents and seams skipped), star-list (star retrieval back to Phase 1's fixed
suffix list), no-star-windows (a star query's whole manuscripts read whole, first match only).

Run (Phase 1; `--phase 3` for the star queries; the index of either format):
  python -I -X utf8 scripts/verify_responsa_wildcards.py ^
      --index-dir C:\\Users\\gersh\\Genizah_Tantivy_Index\\tantivy_db ^
      --corpus-file C:\\GenizahSearch\\Transcriptions.txt --out-dir _tmp\\responsa_verify

Exit codes: 0 every witness agrees (documented differences only); 1 a difference; 2 the index or
the corpus file is missing -- the gate FAILS then, it never skips.
"""
from __future__ import annotations

import argparse
import bisect
import json
import os
import re
import shutil
import sys
import tempfile
import time
import unicodedata
from collections import Counter, defaultdict
from unittest.mock import patch

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import regex  # noqa: E402  -- \p{M} for the pre-test (the engine's own patterns are never used)

from shared.text_normalize import strip_search_diacritics  # noqa: E402
# The grammatical lists are data (the contract's `#`, plan section 4); the oracle builds the
# forms from them itself.
from shared.responsa import GRAMMATICAL_PREFIXES, GRAMMATICAL_SUFFIXES  # noqa: E402

EXIT_OK, EXIT_FAILED, EXIT_MISSING_DATA = 0, 1, 2

# --------------------------------------------------------------------------------------
# Exact's predicate (plan section 4), written out here, not imported
# --------------------------------------------------------------------------------------

_IN_WORD_QUOTES = frozenset('\'"\u05F3\u05F4\u2018\u2019[]')
_SOFIT = {'\u05DD': '\u05DE', '\u05DF': '\u05E0', '\u05E5': '\u05E6', '\u05E3': '\u05E4', '\u05DA': '\u05DB'}
MAQAF, SOF_PASUQ = '\u05BE', '\u05C3'
# The looked-past characters, removed at C speed for the pre-test only.
_LOOKED_PAST_RE = regex.compile(r"[\p{M}'\"\u05F3\u05F4\u2018\u2019\[\]]")


def looked_past(ch):
    return ch in _IN_WORD_QUOTES or unicodedata.category(ch)[0] == 'M'


def continues(ch):
    cat = unicodedata.category(ch)
    return cat[0] == 'L' or cat in ('Nd', 'Pc')


def words_of(text):
    """[(core, start, end)]: the whole words of *text*, each its characters from its first to
    its last word character (a run of looked-past characters alone is not a word)."""
    out = []
    i, n = 0, len(text)
    while i < n:
        if continues(text[i]) or looked_past(text[i]):
            j = i
            while j < n and (continues(text[j]) or looked_past(text[j])):
                j += 1
            k0, k1 = i, j
            while k0 < k1 and not continues(text[k0]):
                k0 += 1
            while k1 > k0 and not continues(text[k1 - 1]):
                k1 -= 1
            if k0 < k1:
                out.append((text[k0:k1], k0, k1))
            i = j
        else:
            i += 1
    return out


# --------------------------------------------------------------------------------------
# The query set and its word rules
# --------------------------------------------------------------------------------------

def prefix_forms(word):
    return list(dict.fromkeys(p + word for p in GRAMMATICAL_PREFIXES))


def suffix_forms(word):
    out = []
    for s in GRAMMATICAL_SUFFIXES:
        if s and word and word[-1] in _SOFIT:
            out.append(word[:-1] + _SOFIT[word[-1]] + s)
        else:
            out.append(word + s)
    return list(dict.fromkeys(out))


# A star (plan §4, Phase 3): any letters of any script, and the marks between them -- the
# folded, bracket-free text has no quotes or brackets left to look past -- never a digit,
# `_`, space or punctuation. Written here from the contract, not taken from the compiler.
_STAR = r'[\p{L}\p{M}]*'
_FINAL_MEDIAL = dict(list(_SOFIT.items()) + [(v, k) for k, v in _SOFIT.items()])


class StarWord:
    """One query word with a star, as a set of word cores: ``core in StarWord('כ*נים')``.
    Its letters between stars must stand as typed; a letter right before a star may take its
    final or medial form (`שלום*` holds `שלומות`, `שלומ*` holds `שלום`)."""

    def __init__(self, token):
        runs = re.sub(r'\*+', '*', token).split('*')
        parts = []
        for i, run in enumerate(runs):
            if i:
                parts.append(_STAR)
            if not run:
                continue
            if i < len(runs) - 1 and run[-1] in _FINAL_MEDIAL:
                parts.append(regex.escape(run[:-1]) + '[' + run[-1] + _FINAL_MEDIAL[run[-1]] + ']')
            else:
                parts.append(regex.escape(run))
        self.token = token
        self.runs = [r for r in runs if r]
        self._rx = regex.compile(''.join(parts))

    def __contains__(self, core):
        return self._rx.fullmatch(core) is not None

    def __repr__(self):
        return f'StarWord({self.token!r})'


def forms_of(token):
    """The forms of one query word: `#X` the prefix list, `X#` the suffix list, `#X#` both;
    a word with a star, a StarWord (no `#` with it in these query sets)."""
    if '*' in token:
        if '#' in token or '%' in token:
            raise ValueError(f'the oracle reads a star word without # or %: {token!r}')
        return StarWord(token)
    pre, suf = token.startswith('#'), token.endswith('#') and len(token) > 1
    word = token.strip('#')
    forms = [word]
    if pre:
        forms = [f for w in forms for f in prefix_forms(w)]
    if suf:
        forms = [f for w in forms for f in suffix_forms(w)]
    return frozenset(forms)


class Query:
    """One query of the set, parsed by the oracle's own reading of the syntax."""

    def __init__(self, name, text, options=None):
        self.name, self.text, self.options = name, text, dict(options or {})
        self.bidirectional = bool(self.options.get('bidirectional'))
        tokens = text.split()
        self.and_query = 'AND' in tokens
        self.negated = [t[1:] for t in tokens if t.startswith('-') and len(t) > 1]
        self.line_start = any(t.startswith('|') for t in tokens)
        self.words, self.gaps = [], []
        pending_gap = 0
        for t in tokens:
            if t == 'AND' or (t.startswith('-') and len(t) > 1):
                continue
            m = re.fullmatch(r'\[(\d+)\]', t)
            if m:
                pending_gap = int(m.group(1))
                continue
            if self.words:
                self.gaps.append(pending_gap)
            pending_gap = 0
            self.words.append(forms_of(t.lstrip('|')))
        self.brackets = False   # no query here holds a bracket of its own

    @property
    def sequence(self):
        """A run of words (not AND, not a line query): its matches can cross a page break."""
        return not self.and_query and not self.line_start and len(self.words) > 1

    def letters_needed(self):
        """For a cheap pre-test: one string per word that every form of it contains."""
        out = []
        for forms in self.words:
            if isinstance(forms, StarWord):
                out.append(max(forms.runs, key=len))
                continue
            common = min(forms, key=len)
            # the stem: the longest substring every form shares, found by shrinking the shortest
            stem = next((common[i:j] for size in range(len(common), 0, -1)
                         for i in range(0, len(common) - size + 1) for j in [i + size]
                         if all(common[i:j] in f for f in forms)), '')
            out.append(stem)
        return out


PHASE1 = [
    Query('hash-hash', '#כהנים #גדולים'),
    Query('hash-both', '#כהנ# גדולים'),
    Query('last-word', 'כהנים גדול'),
    Query('lone-word', 'ספר'),
    Query('gap-2', 'כהנים [2] גדולים'),
    Query('bidirectional', 'כהנים גדולים', {'bidirectional': True}),
    Query('and', 'כהנים AND גדולים'),
    Query('line-start', '|כהנים גדולים'),
    Query('negation', 'כהנים גדולים -אהרן'),
]
# Phase 3, first PR (plan §7, "Phase 3, settled before building"): a star at the end of a word
# or inside it. A leading star keeps Phase 1's meaning there; its queries come with the second.
PHASE3 = PHASE1 + [
    Query('star-end-pair', 'מאמ* קור*'),
    Query('star-end', 'כהנ* גדול*'),
    Query('star-inside', 'כ*נים גדולים'),
    Query('star-several', 'כ*ה*נ*ים גדולים'),
]
QUERY_SETS = {1: PHASE1, 3: PHASE3}


def _seq_match(words, gaps, q_words, start_index, line_only):
    """The span of q_words matched from words[start_index] on, or None. Gaps are greedy (the
    most words between first), as the engine's separators are."""
    first = words[start_index]
    if first[0] not in q_words[0]:
        return None

    def rest(k, i):
        if k == len(q_words):
            return words[i - 1][2]
        gap = gaps[k - 1]
        for skip in range(gap, -1, -1):
            j = i + skip
            if j >= len(words):
                continue
            if line_only and any(w[3] != words[i - 1][3] for w in words[i:j + 1]):
                continue
            if words[j][0] in q_words[k]:
                end = rest(k + 1, j + 1)
                if end is not None:
                    return end
        return None

    end = rest(1, start_index + 1)
    return None if end is None else (first[1], end)


def oracle_page(q, text):
    """The span of the first acceptable match of *q* on *text* (folded, brackets out), or None."""
    words = words_of(text)
    if q.line_start:
        lines = [0] + [m.end() for m in re.finditer('\n', text)]
        words = [(c, s, e, bisect.bisect_right(lines, s) - 1) for c, s, e in words]
    else:
        words = [(c, s, e, 0) for c, s, e in words]
    if q.and_query:
        if not all(any(w[0] in forms for w in words) for forms in q.words):
            return None
        for w in words:
            if any(w[0] in forms for forms in q.words):
                return w[1], w[2]
        return None
    orders = [(q.words, q.gaps)]
    if q.bidirectional and len(q.words) > 1:
        orders.append((list(reversed(q.words)), list(reversed(q.gaps))))
    for i in range(len(words)):
        for q_words, gaps in orders:
            if q.line_start:
                # The first word opens its line (only whitespace before it on the line);
                # the words of the group are separated by whitespace only.
                line_start_pos = text.rfind('\n', 0, words[i][1]) + 1
                if ''.join(ch for ch in text[line_start_pos:words[i][1]] if not looked_past(ch)).strip():
                    continue
            span = _seq_match(words, gaps, q_words, i, q.line_start)
            if span is None:
                continue
            if q.line_start and not _whitespace_only_between(text, words, i, span):
                continue
            return span
    return None


def _whitespace_only_between(text, words, i, span):
    seg = text[span[0]:span[1]]
    inside = [w for w in words if span[0] <= w[1] and w[2] <= span[1]]
    for a, b in zip(inside, inside[1:]):
        if text[a[2]:b[1]].strip():
            return False
    return '\n' not in seg


def oracle_crossing(q, a_raw, b_raw):
    """The first acceptable match of *q* that runs from page *a_raw* into the page that
    follows it, *b_raw* -- the pages joined by a line break, as the seams and the
    whole-manuscript documents join them -- as ({0: span on a, 1: span on b}) in each
    page's raw characters, trimmed to letters; or None."""
    joined = a_raw + '\n' + b_raw
    text, idx = transformed(joined, strip_brackets=not q.brackets)
    cs = len(transformed(a_raw + '\n', strip_brackets=not q.brackets)[0])
    words = [(c, st, e, 0) for c, st, e in words_of(text)]
    orders = [(q.words, q.gaps)]
    if q.bidirectional and len(q.words) > 1:
        orders.append((list(reversed(q.words)), list(reversed(q.gaps))))
    for i in range(len(words)):
        if words[i][1] >= cs:
            break
        for q_words, gaps in orders:
            span = _seq_match(words, gaps, q_words, i, False)
            if span is None or not span[0] < cs < span[1]:
                continue
            s, e = idx[span[0]], idx[span[1] - 1] + 1
            a_len = len(a_raw)
            return {0: letters_trim(a_raw, s, a_len), 1: letters_trim(b_raw, 0, e - a_len - 1)}
    return None


_PAGE_KEY_RE = re.compile(r'(99\d{16})_IE(\d+)_P(\d+)_FL')


def page_key(header):
    """(sys_id, IE, page number) of a V0.8 header, or None."""
    m = _PAGE_KEY_RE.search(header)
    return (m.group(1), m.group(2), int(m.group(3))) if m else None


def follows(prev_header, header):
    """*header*'s page comes right after *prev_header*'s in one volume of one manuscript
    (the seams' junctions, plan §6.1 point 3; the oracle reads V0.8 only)."""
    a, b = page_key(prev_header), page_key(header)
    return bool(a and b and a[0] == b[0] and a[1] == b[1] and b[2] == a[2] + 1)


# --------------------------------------------------------------------------------------
# Reading the corpus
# --------------------------------------------------------------------------------------

def iter_pages(corpus_file):
    """(header, text) of every V0.8 page, the text built as the indexer builds a page document."""
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


def transformed(raw, strip_brackets=True):
    """(text, index map): *raw* folded as the index folds it, brackets out; map[i] is the raw
    index of the text's character i."""
    keep, idx = [], []
    for i, ch in enumerate(raw):
        if strip_search_diacritics(ch) == '':
            continue
        if strip_brackets and ch in '[]':
            continue
        keep.append(ch)
        idx.append(i)
    return ''.join(keep), idx


def letters_trim(raw, s, e):
    """(s, e) narrowed to the first and last word character inside it (a whole-manuscript
    page's span may reach the line break that joins it to the next page)."""
    e = min(e, len(raw))
    while s < e and not continues(raw[s]):
        s += 1
    while e > s and not continues(raw[e - 1]):
        e -= 1
    return s, e


def to_raw(raw, idx, span):
    s, e = span
    return letters_trim(raw, idx[s], idx[e - 1] + 1)


def classify_missing(raw, span):
    """Why a page's witness may be beyond the engine's reach (plan section 4), or None."""
    lo = raw.rfind(' ', 0, span[0]) + 1
    hi = raw.find(' ', span[1])
    hi = len(raw) if hi < 0 else hi
    token = raw[lo:hi].replace('\n', ' ')
    if MAQAF in token or SOF_PASUQ in token:
        return 'maqaf or sof pasuq inside the token (plan section 4)'
    if any(unicodedata.category(ch)[0] == 'M' and not ('\u0591' <= ch <= '\u05C7') for ch in token):
        return 'a mark of another script inside the word (plan section 4)'
    between = raw[span[0]:span[1]]
    if any(unicodedata.category(ch) in ('No', 'Nl') for ch in between):
        return 'a number character between the words (the separator is not a word character)'
    return None


def cached_oracle(args, queries, log):
    """The oracle's witnesses, from a cache next to the log when the corpus file and the
    queries are the same (the mutation runs change only the engine)."""
    stat = os.stat(args.corpus_file)
    key = {'corpus': os.path.abspath(args.corpus_file), 'size': stat.st_size, 'mtime': stat.st_mtime,
           'oracle': os.stat(os.path.abspath(__file__)).st_mtime,
           'queries': [[q.name, q.text, q.options] for q in queries]}
    path = os.path.join(args.out_dir, f'oracle-phase{args.phase}.json')
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            cached = json.load(f)
        if cached.get('key') == key:
            log(f'oracle witnesses from {path}')
            out = {}
            for name, entries in cached['found'].items():
                if name.startswith('_terms_'):
                    out[name] = {uid: ([tuple(sp) for sp in spans], raw) for uid, (spans, raw) in entries.items()}
                elif name.startswith('_cross_'):
                    out[name] = {uid: ({u: tuple(sp) for u, sp in pages.items()}, raws)
                                 for uid, (pages, raws) in entries.items()}
                else:
                    out[name] = {uid: (tuple(span), raw) for uid, (span, raw) in entries.items()}
            return out
    log(f'oracle over {args.corpus_file}')
    found = oracle_scan(args.corpus_file, queries, log)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump({'key': key, 'found': found}, f, ensure_ascii=False)
    return found


def oracle_scan(corpus_file, queries, log):
    """{query name: {uid: (raw span, raw text)}} over every V0.8 page, one pass."""
    found = {q.name: {} for q in queries}
    terms = {q.name: {} for q in queries if q.and_query}
    crossings = {q.name: {} for q in queries if q.sequence}
    needed = {q.name: q.letters_needed() + q.negated for q in queries}
    started = time.perf_counter()
    pages = 0
    prev = None          # (header, uid, raw, letters) of the page before, in file order
    for header, raw in iter_pages(corpus_file):
        pages += 1
        if pages % 100_000 == 0:
            log(f'  oracle: {pages:,} pages, {time.perf_counter() - started:.0f} s')
        m = _UID_RE.search(header)
        if not m:
            prev = None
            continue
        uid = m.group(1)
        if prev is not None and follows(prev[0], header):
            # Crossings: the page before and this one, joined (seam-aware: only pages that
            # follow each other in one volume).
            joined_letters = prev[3] + '\n' + _LOOKED_PAST_RE.sub('', strip_search_diacritics(raw))
            for q in queries:
                if not q.sequence or not all(sn in joined_letters for sn in q.letters_needed()):
                    continue
                pair = oracle_crossing(q, prev[2], raw)
                if pair is None:
                    continue
                if q.negated and any(w.lower() in (prev[2] + '\n' + raw).lower() for w in q.negated):
                    continue
                crossings[q.name][prev[1]] = ({prev[1]: pair[0], uid: pair[1]}, [prev[2], raw])
        letters = None
        for q in queries:
            if letters is None:
                letters = _LOOKED_PAST_RE.sub('', strip_search_diacritics(raw))
            needed = q.letters_needed()
            if q.and_query and any(s in letters for s in needed):
                text, idx = transformed(raw, strip_brackets=not q.brackets)
                spans = [to_raw(raw, idx, (w[1], w[2])) for w in words_of(text)
                         if any(w[0] in forms for forms in q.words)]
                if spans:
                    terms[q.name][uid] = (spans, raw)
            if not all(s in letters for s in needed):
                continue
            text, idx = transformed(raw, strip_brackets=not q.brackets)
            span = oracle_page(q, text)
            if span is None:
                continue
            if q.negated and any(w.lower() in raw.lower() for w in q.negated):
                continue
            found[q.name][uid] = (to_raw(raw, idx, span), raw)
        prev = (header, uid, raw, letters if letters is not None
                else _LOOKED_PAST_RE.sub('', strip_search_diacritics(raw)))
    log(f'  oracle: {pages:,} pages in {time.perf_counter() - started:.0f} s')
    for name, pages_terms in terms.items():
        found['_terms_' + name] = pages_terms
    for name, pairs in crossings.items():
        # A page with a match of its own shows that match: its crossing is no row (first
        # wins, plan §5.4).
        found['_cross_' + name] = {uid: v for uid, v in pairs.items() if uid not in found[name]}
    return found


# --------------------------------------------------------------------------------------
# The engine side
# --------------------------------------------------------------------------------------

def row_witnesses(rows):
    """({uid: span} within one page, [crossing rows]) of V0.8 result rows; spans in the page's
    raw characters, trimmed to letters."""
    within, crossing, unlocated = {}, [], []
    for r in rows:
        if (r.get('display') or {}).get('source') != 'V0.8':
            continue
        highlights = r.get('page_highlights') or []
        if r.get('scope') != 'page' and highlights:
            # A V0.8 page and its V0.7 copy share a uid: a page is its uid and source.
            pages = {(h['uid'], h.get('source') or 'V0.8') for h in highlights}
            if len(pages) > 1:
                crossing.append(r)
                continue
            h = highlights[0]
            within[r['uid']] = ('aggregate', tuple(h['span']))
            continue
        span = _span_from_snippet(r.get('raw_file_hl') or '', r.get('full_text') or '')
        if span is None:
            unlocated.append(r['uid'])
            continue
        within[r['uid']] = ('page', span)
    return within, crossing, unlocated


def _span_from_snippet(hl, text):
    a = hl.find('*')
    b = hl.find('*', a + 1)
    if a < 0 or b < 0:
        return None
    left, marked = hl[:a], hl[a + 1:b]
    safe = text.replace('*', ' ')
    pos = safe.find(left + marked)
    if pos < 0:
        return None
    s = pos + len(left)
    return letters_trim(text, s, s + len(marked))


MUTATIONS = ('accept-off', 'raw-prefix-tree', 'guard-500', 'no-crossing', 'star-list', 'no-star-windows')


def mutation_patches(name, se, rm):
    """The patches of one named mutation (each must turn the gate red)."""
    if name is None:
        return []
    if name == 'accept-off':
        return [patch.object(rm.CompiledResponsa, 'accept', lambda self, text, m: True)]
    if name == 'raw-prefix-tree':
        return [patch.object(se, '_variant_alternation', lambda forms, inserter=se.MARK_TOLERANT_INSERTER:
                             se._prefix_tree(forms, inserter))]
    if name == 'guard-500':
        from shared.config import Config
        return [patch.object(Config, 'MAX_EXPANDED_TERMS', 500)]
    if name == 'no-crossing':
        import tantivy
        original = rm.CompiledResponsa.retrieval

        def pages_only(self, index, *args, **kwargs):
            # The lookup restricted to page documents: no whole-manuscript document, so
            # no match across a page break.
            query = original(self, index, *args, **kwargs)
            return tantivy.Query.boolean_query([(tantivy.Occur.Must, query), (
                tantivy.Occur.Must, index.parse_query('scope:page', ['content']))])
        return [patch.object(rm.CompiledResponsa, 'retrieval', pages_only)]
    if name == 'star-list':
        # Star retrieval back to Phase 1's fixed list: `X*` looks up X with its 25 grammatical
        # suffixes, a star inside a word looks up nothing. Loses `מאמתי קורין` for `מאמ* קור*`.
        import tantivy
        from shared.responsa import expand_grammatical_suffixes

        def fixed_list(star_forms, schema, field, tolerance):
            terms = [f for sf in star_forms if len(sf.runs) == 2 and sf.runs[1] == ('',)
                     for stem in sf.runs[0] for f in expand_grammatical_suffixes(stem)]
            return [tantivy.Query.term_set_query(schema, field, terms)] if terms else []
        return [patch.object(rm, 'star_queries', fixed_list)]
    if name == 'no-star-windows':
        # A star query's crossings on an index without seams read whole, first match only
        # (today's route for finite words), not break by break: loses a crossing that
        # follows a match inside an earlier page of the same manuscript.
        real = se.SearchEngine._responsa_crossings

        def whole(self, *args, **kwargs):
            plan = real(self, *args, **kwargs)
            plan.windows = False
            return plan
        return [patch.object(se.SearchEngine, '_responsa_crossings', whole)]
    raise ValueError(name)


def run_engine(engine, q, se, log):
    from shared.config import Config
    from shared.search_regex import isolated_matching
    options = dict({'responsa_mode': True, 'variants': False, 'ja': False, 'flex_spacing': False,
                    'bidirectional': False, 'variant_mode': 'exact'}, **q.options)
    se.consume_last_search_cutoff()
    started = time.perf_counter()
    # Nothing is cut: every candidate page, whole-manuscript document and seam is read, and
    # a star query's whole manuscripts on an index without seams too (the website's limits
    # are for its time limit; the oracle compares the whole answer).
    with patch.object(Config, 'SEARCH_LIMIT', 5_000_000), patch.object(Config, 'RESPONSA_PAGE_LIMIT', 5_000_000), \
            patch.object(Config, 'RESPONSA_STAR_AGGREGATE_LIMIT', 5_000_000), isolated_matching(native=True):
        rows = engine.execute_search(q.text, 'exact', 0, responsa_options=options, corpus_scope='genizah')
    cutoff = se.consume_last_search_cutoff()
    return rows, time.perf_counter() - started, cutoff


def _p_num(uid):
    m = re.search(r'_P(\d+)_', uid or '')
    return int(m.group(1)) if m else -1


def compare_crossings(q, crosses, crossing, result):
    """Engine crossing rows (V0.8) against the oracle's crossings between pages that follow
    each other (seam-aware, ``oracle_crossing``), in both directions. A row the oracle has
    no pair for is a documented kind when the whole-manuscript document made it: a junction
    into the V0.7 copy, between pages that do not follow each other (§3.6.5: the seams hold
    neither), or a match over more than two pages (the oracle reads pairs); listed by count.
    A crossing the engine lacks fails for a star query; for finite words it is listed: they
    read the whole-manuscript documents, which give their first match only (today's route
    until decision 10 moves them to the seams)."""
    seen = set()
    for r in crossing:
        hl = r.get('page_highlights') or []
        start = r['uid']
        want = crosses.get(start)
        if want is not None:
            pages, raws = want
            raw_of = dict(zip(pages, raws))
            got = {h['uid']: letters_trim(raw_of[h['uid']], *h['span']) for h in hl if h['uid'] in raw_of}
            if len(hl) == 2 and got == pages:
                result['crossing_matched'] += 1
                seen.add(start)
                continue
            seen.add(start)
            result['failures']['wrong crossing'].append({'uid': start, 'oracle': pages,
                                                         'engine': [(h['uid'], h['span']) for h in hl]})
            continue
        if any((h.get('source') or 'V0.8') != 'V0.8' for h in hl):
            result['expected']['a crossing into the V0.7 copy (whole-manuscript document)'] += 1
        elif len({(h['uid'], h.get('source')) for h in hl}) > 2:
            result['expected']['a match over more than two pages (the oracle reads pairs)'] += 1
        elif len(hl) == 2 and _p_num(hl[1]['uid']) != _p_num(hl[0]['uid']) + 1:
            result['expected']['a junction between pages that do not follow each other '
                               '(whole-manuscript document)'] += 1
        else:
            result['failures']['extra crossing'].append({'uid': start, 'engine': [(h['uid'], h['span']) for h in hl]})
    star = any(isinstance(w, StarWord) for w in q.words)
    for uid, (pages, _raws) in crosses.items():
        if uid in seen:
            continue
        if star:
            result['failures']['missing crossing'].append({'uid': uid, 'oracle': pages})
        else:
            result['expected']['a crossing finite words did not reach (a whole-manuscript document '
                               'gives its first match only)'] += 1


def compare(q, oracle, rows, terms=None, crosses=None):
    """The query's verdict: matched, missing, extra and wrong-span witnesses, by class."""
    within, crossing, unlocated = row_witnesses(rows)
    result = {'oracle': len(oracle), 'engine_within_page': len(within), 'engine_crossing': len(crossing),
              'oracle_crossing': len(crosses or {}), 'crossing_matched': 0,
              'matched': 0, 'failures': defaultdict(list), 'expected': Counter()}
    for uid, (span, raw) in oracle.items():
        got = within.get(uid)
        if got is None:
            why = classify_missing(raw, span)
            if why:
                result['expected'][why] += 1
            else:
                result['failures']['missing'].append({'uid': uid, 'span': span, 'text': raw[span[0]:span[1]]})
            continue
        if got[1] != span:
            result['failures']['wrong span'].append({'uid': uid, 'oracle': span, 'engine': got[1],
                                                     'oracle_text': raw[span[0]:span[1]]})
            continue
        result['matched'] += 1
    for uid, got in within.items():
        if uid in oracle:
            continue
        if got[0] == 'aggregate' and q.and_query and got[1] in (terms or {}).get(uid, ([], ''))[0]:
            # Every word on some page of the manuscript, the row marking a whole-word
            # occurrence of one of them on its own page (the oracle checks the mark;
            # the other pages may be the manuscript's V0.7 copy, which it does not read).
            result['expected']['AND with its words on different pages (whole-manuscript document)'] += 1
            continue
        result['failures']['extra'].append({'uid': uid, 'engine': got})
    for uid in unlocated:
        result['failures']['span not located in the row'].append({'uid': uid})
    if crosses is not None:
        compare_crossings(q, crosses, crossing, result)
    elif crossing:
        result['expected']['a match across a page break (not compared: not a sequence)'] += len(crossing)
    result['failures'] = dict(result['failures'])
    return result


# --------------------------------------------------------------------------------------
# The crossing fixture: ordered pages of one manuscript, indexed in a temporary directory
# --------------------------------------------------------------------------------------

FIXTURE = [   # (uid, text), in physical order; one manuscript, one transcription (V0.8)
    ('IE9_P001_FL1', 'פתיחה\nוהיו הכהנים'),
    ('IE9_P002_FL2', 'הגדולים עומדים\nשם'),
    ('IE9_P003_FL3', 'אחר כך כהנים גדולים באו'),
]
FIXTURE_QUERY = Query('crossing-fixture', '#כהנים #גדולים')


def build_fixture(root):
    import tantivy
    from shared.indexer import Indexer, build_main_schema
    from shared.search_tokenizer import register_search_tokenizers
    db = os.path.join(root, 'tantivy_db')
    os.makedirs(db)
    idx = tantivy.Index(build_main_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    headers = [f'990000000900000001_{uid} 990000000900000001' for uid, _ in FIXTURE]

    def add(uid, text, scope, header, bounds=''):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source='V0.8',
            full_header=header, shelfmark=header, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))
    parts, bounds, cursor = [], [], 0
    for n, ((uid, text), header) in enumerate(zip(FIXTURE, headers), 1):
        add(uid, text, 'page', header)
        start = cursor
        parts.append(text)
        cursor += len(text)
        if n != len(FIXTURE):
            parts.append('\n')
            cursor += 1
        bounds.append({'uid': uid, 'p_num': n, 'full_header': header, 'source': 'V0.8',
                       'sys_id': '990000000900000001', 'start': start, 'end': cursor})
    add('sys:990000000900000001', ''.join(parts), 'system', headers[0], json.dumps(bounds, ensure_ascii=False))
    w.commit()
    w.wait_merging_threads()


class _FixtureMeta:
    def get_display_data(self, header, source):
        return {'shelfmark': header, 'title': '', 'img': '1', 'source': source, 'id': header.split()[-1],
                'library_code': ''}

    def parse_full_id_components(self, header):
        return {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}


def fixture_oracle(q):
    """Within-page witnesses and crossings of the fixture: its pages in physical order, a
    crossing being a match whose words run from one page into the next."""
    within, crossings = {}, []
    for uid, raw in FIXTURE:
        text, idx = transformed(raw)
        span = oracle_page(q, text)
        if span:
            within[uid] = to_raw(raw, idx, span)
    for (u1, a), (u2, b) in zip(FIXTURE, FIXTURE[1:]):
        joined = a + '\n' + b
        text, idx = transformed(joined)
        span = oracle_page(q, text)
        if span:
            s, e = to_raw(joined, idx, span)
            if s < len(a) < e:
                crossings.append({u1: (s, len(a.rstrip())), u2: (0, e - len(a) - 1)})
    return within, crossings


def check_fixture(se, log, mutation):
    from shared.config import Config
    root = tempfile.mkdtemp(prefix='responsa_verify_')
    try:
        build_fixture(root)
        with patch.object(Config, 'INDEX_DIR', root), patch.object(se.SearchEngine, 'start_fl_id_index_build'):
            engine = se.SearchEngine(_FixtureMeta(), _variant_manager(), worker_mode=True, open_local=False)
        within, crossings = fixture_oracle(FIXTURE_QUERY)
        rows, _secs, _cut = run_engine(engine, FIXTURE_QUERY, se, log)
        got_within, got_cross, _ = row_witnesses(rows)
        failures = []
        for uid, span in within.items():
            if got_within.get(uid, (None, None))[1] != span:
                failures.append(f'within-page witness {uid} {span}: engine {got_within.get(uid)}')
        for uid in got_within:
            if uid not in within:
                failures.append(f'extra within-page witness {uid}')
        got_spans = [{h['uid']: letters_trim(dict(FIXTURE)[h['uid']], *h['span']) for h in r['page_highlights']}
                     for r in got_cross]
        for c in crossings:
            if c not in got_spans:
                failures.append(f'crossing witness {c}: engine {got_spans}')
        for c in got_spans:
            if c not in crossings:
                failures.append(f'extra crossing {c}')
        engine.close_index()
        return {'within': len(within), 'crossings': len(crossings), 'failures': failures}
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _variant_manager():
    from shared.variants import VariantManager
    return VariantManager()


# --------------------------------------------------------------------------------------

class Log:
    def __init__(self, out_dir, name):
        os.makedirs(out_dir, exist_ok=True)
        self.path = os.path.join(out_dir, name + '.log')
        self.f = open(self.path, 'w', encoding='utf-8')

    def __call__(self, text):
        self.f.write(text + '\n')
        self.f.flush()
        print(text.encode('ascii', 'replace').decode('ascii'), flush=True)


def main(argv=None, meta_factory=None):
    """*meta_factory*: the metadata manager for the real index (tests pass a stand-in)."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--index-dir', required=True, help='the tantivy_db directory')
    ap.add_argument('--corpus-file', required=True, help='Transcriptions.txt (V0.8)')
    ap.add_argument('--phase', type=int, default=1, choices=sorted(QUERY_SETS))
    ap.add_argument('--out-dir', default=os.path.join(REPO_ROOT, '_tmp', 'responsa_verify'))
    ap.add_argument('--query', action='append', help='only these query names (default: the whole set)')
    ap.add_argument('--mutation', choices=MUTATIONS, help='check a deliberately broken engine (must exit 1)')
    args = ap.parse_args(argv)
    for path, what in ((args.index_dir, 'tantivy index'), (args.corpus_file, 'corpus file')):
        if not path or not os.path.exists(path):
            print(f'MISSING  {what}: {path} -- the gate fails; it never skips', flush=True)
            return EXIT_MISSING_DATA
    name = f'phase{args.phase}' + (f'-{args.mutation}' if args.mutation else '')
    log = Log(args.out_dir, name)
    queries = [q for q in QUERY_SETS[args.phase] if not args.query or q.name in args.query]

    from shared.config import Config
    import shared.responsa_match as rm
    import shared.search_engine as se
    patches = mutation_patches(args.mutation, se, rm)
    for p in patches:
        p.start()
    try:
        fixture = check_fixture(se, log, args.mutation)
        log(f'crossing fixture: {fixture["within"]} within-page and {fixture["crossings"]} crossing witnesses; '
            f'{len(fixture["failures"])} failures')
        for f in fixture['failures']:
            log('  FAIL ' + f)

        oracle = cached_oracle(args, queries, log)

        from shared.metadata_manager import MetadataManager
        with patch.object(Config, 'INDEX_DIR', os.path.dirname(args.index_dir.rstrip('/\\'))), \
                patch.object(se.SearchEngine, 'start_fl_id_index_build'):
            if meta_factory is None:
                meta = MetadataManager()
                meta._load_heavy_caches_bg()
            else:
                meta = meta_factory()
            engine = se.SearchEngine(meta, _variant_manager(), worker_mode=True, open_local=False)
            log(f'index format: {engine.index_format_status()}')
        results = {}
        failed = bool(fixture['failures'])
        for q in queries:
            rows, secs, cutoff = run_engine(engine, q, se, log)
            verdict = compare(q, oracle[q.name], rows, oracle.get('_terms_' + q.name),
                              oracle.get('_cross_' + q.name))
            verdict.update({'query': q.text, 'options': q.options, 'seconds': round(secs, 1),
                            'rows': len(rows), 'cutoff': cutoff})
            n_fail = sum(len(v) for v in verdict['failures'].values())
            failed = failed or n_fail > 0 or bool(cutoff.get('capped'))
            log(f'{q.name} {q.text!r} {q.options or ""}: oracle {verdict["oracle"]:,} + '
                f'{verdict["oracle_crossing"]:,} across a break, engine '
                f'{verdict["engine_within_page"]:,} within one page + {verdict["engine_crossing"]:,} across a break, '
                f'matched {verdict["matched"]:,} + {verdict["crossing_matched"]:,}, failures {n_fail}, {secs:.1f} s'
                + (', N+ (cut off)' if cutoff.get('capped') else '')
                + (' (partial: crossings left out by rule)' if cutoff.get('partial') else ''))
            for kind, n in verdict['expected'].items():
                log(f'    expected: {n:,} x {kind}')
            for kind, items in verdict['failures'].items():
                log(f'    FAIL {kind}: {len(items):,}; first: {items[:3]}')
            verdict['expected'] = dict(verdict['expected'])
            results[q.name] = verdict
        engine.close_index()
    finally:
        for p in reversed(patches):
            p.stop()
    with open(os.path.join(args.out_dir, name + '.json'), 'w', encoding='utf-8') as f:
        json.dump({'phase': args.phase, 'mutation': args.mutation, 'fixture': fixture, 'queries': results},
                  f, ensure_ascii=False, indent=1, default=str)
    log('RESULT: ' + ('FAILED' if failed else 'PASSED'))
    return EXIT_FAILED if failed else EXIT_OK


if __name__ == '__main__':
    sys.exit(main())

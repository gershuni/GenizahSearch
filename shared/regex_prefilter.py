r"""Lossless Tantivy candidate query for Regex-mode searches.

Contract: build_candidate_query(pattern, index, ...) builds a formula F over the
tokens of the indexed ``content`` field such that every document whose text
the regex can match satisfies F. F is built only from constraints the regex
REQUIRES; anything uncertain is weakened to TRUE (all documents), never
guessed. The regex itself still decides the final hit list.

Why a constraint is sound
-------------------------
The regex runs on T = the stored content (or the content with square brackets
removed, when the pattern has none). The ``content`` field is tokenized by
hebword: a token is a maximal run of token characters (tantivy's own analyzer
decides which characters those are; we ask it, per character). Brackets are
token characters, so removing them never moves a token boundary: each maximal
token-character run of T is the bracket-free form of exactly one indexed
token.

A "run" is a stretch of consecutive pattern atoms that can only match token
characters (a literal token character, or a class all of whose members are
token characters, with case variants included). Any text the run matches lies
inside ONE token, so "some token contains a substring matching the run" is a
necessary condition. When the atom just before the run can only match a
separator (space, comma, ``\s``) or is ``^``/``\A``, the run starts at a token
start; likewise at the end. Zero-width assertions (``\b``, ``\B``,
lookarounds) consume nothing, so runs continue across them. Everything else
(``.``, negated classes, back-references, mixed classes) breaks a run with NO
anchoring, which is sound whatever that atom matches.

Alternation becomes OR (TRUE if any branch is TRUE); a repeat with min 0 is
TRUE; a group repeated at least once contributes its body's formula. Dropping
an AND conjunct only weakens F, so the query is kept small by choosing a few
conjuncts.
"""
from __future__ import annotations

import functools
import re
import warnings
from dataclasses import dataclass
from re import _constants as C
from re import _parser as sre_parse

import tantivy

from shared.search_regex import matched_chars as _matched_chars
from shared.search_tokenizer import HEBWORD_TOKENIZER_PATTERN

MAX_CLASS = 512          # larger classes are not expanded (treated as unknown)
MAX_CONJUNCTS = 2        # AND constraints sent to Tantivy
MAX_FULL_PASSES = 1      # constraints not anchored at a token start scan the whole term dictionary (~4 s each on 2.2M docs)
MIN_TOK_WEIGHT = 4       # a run must pin at least ~2 exact characters to be worth a term-dictionary pass
MAX_DISJUNCTS = 8        # an OR with more branches is dropped (TRUE)
REPEAT_FLOOR = 4         # x{lo,hi} -> x{min(lo,4),} : a superset, keeps DFAs small
BRACKETS = frozenset("[]")


@functools.lru_cache(maxsize=1)
def _analyzer():
    return tantivy.TextAnalyzerBuilder(
        tantivy.Tokenizer.regex(HEBWORD_TOKENIZER_PATTERN)).build()


@functools.lru_cache(maxsize=1 << 16)
def is_token_char(ch: str) -> bool:
    return _analyzer().analyze(ch) == [ch]


def matched_chars(atom_src: str) -> frozenset:
    # Both engines the search may use, IGNORECASE as the search compiles it.
    return _matched_chars(atom_src, re.IGNORECASE)


def _esc(code: int) -> str:
    return "\\U%08x" % code


_CAT_SRC = {
    C.CATEGORY_DIGIT: r"\d", C.CATEGORY_NOT_DIGIT: r"\D",
    C.CATEGORY_SPACE: r"\s", C.CATEGORY_NOT_SPACE: r"\S",
    C.CATEGORY_WORD: r"\w", C.CATEGORY_NOT_WORD: r"\W",
}


def _atom_src(op, av):
    """Python-regex source for one single-character atom, or None if 'big'."""
    if op is C.LITERAL:
        return _esc(av)
    if op is C.IN:
        parts = []
        for iop, iav in av:
            if iop is C.NEGATE:
                return None
            if iop is C.LITERAL:
                parts.append(_esc(iav))
            elif iop is C.RANGE:
                lo, hi = iav
                if hi - lo > 4096:
                    return None
                parts.append(_esc(lo) + "-" + _esc(hi))
            elif iop is C.CATEGORY:
                if iav not in (C.CATEGORY_SPACE, C.CATEGORY_DIGIT):
                    return None
                parts.append(_CAT_SRC[iav])
            else:
                return None
        return "[" + "".join(parts) + "]"
    return None  # ANY, NOT_LITERAL, ...


def _rust_class(chars) -> str:
    codes = sorted(ord(c) for c in chars)
    out, i = [], 0
    while i < len(codes):
        j = i
        while j + 1 < len(codes) and codes[j + 1] == codes[j] + 1:
            j += 1
        out.append("\\x{%x}" % codes[i] + ("" if i == j else "-\\x{%x}" % codes[j]))
        i = j + 1
    return "[" + "".join(out) + "]"


# ---------------------------------------------------------------- formula
@dataclass(frozen=True)
class Tok:
    frag: str      # Rust regex for the run (brackets interleaved when needed)
    start: bool    # the run begins at a token start
    end: bool      # the run ends at a token end
    weight: int    # required characters in the run (selectivity heuristic)

    def term_regex(self) -> str:
        return ("" if self.start else ".*") + self.frag + ("" if self.end else ".*")


@dataclass(frozen=True)
class And:
    parts: tuple


@dataclass(frozen=True)
class Or:
    parts: tuple


TRUE = None


def _and(parts):
    flat = []
    for p in parts:
        if p is TRUE:
            continue
        flat.extend(p.parts if isinstance(p, And) else [p])
    if not flat:
        return TRUE
    return flat[0] if len(flat) == 1 else And(tuple(flat))


def _or(parts):
    if any(p is TRUE for p in parts) or not parts:
        return TRUE
    return parts[0] if len(parts) == 1 else Or(tuple(parts))


class _Unsupported(Exception):
    pass


class _Builder:
    def __init__(self, stripped: bool, anchors: bool):
        self.stripped = stripped          # text matched has brackets removed
        self.anchors = anchors            # field tokenizer is hebword
        self.B = r"[\[\]]*" if stripped else ""

    # classify one single-char atom -> ('atom', frag) | 'sep' | 'break'
    def _char_atom(self, op, av):
        src = _atom_src(op, av)
        if src is None:
            return "break", None
        chars = matched_chars(src)
        if self.stripped:
            chars = chars - BRACKETS     # T never contains a bracket
        if not chars:
            return "break", None
        if len(chars) <= MAX_CLASS and all(is_token_char(c) for c in chars):
            weight = 2 if len(chars) <= 3 else (1 if len(chars) <= 8 else 0)
            return "atom", (_rust_class(chars) + self.B, weight)
        if len(chars) <= MAX_CLASS and not any(is_token_char(c) for c in chars):
            return ("sep" if self.anchors else "break"), None
        return "break", None

    def _pure_run(self, seq):
        """If every element of seq is a token atom (or zero-width), return
        (frag, weight); else None. Used to fold small alternations into runs."""
        items = self._items(seq)
        frag, w = [], 0
        for kind, val, wt in items:
            if kind == "zero":
                continue
            if kind != "atom":
                return None
            frag.append(val)
            w += wt
        return "".join(frag), w

    def _items(self, seq):
        out = []
        for op, av in seq:
            if op in (C.LITERAL, C.IN, C.ANY, C.NOT_LITERAL):
                kind, val = self._char_atom(op, av)
                out.append((kind, val[0], val[1]) if kind == "atom" else (kind, None, 0))
            elif op is C.AT:
                if av in (C.AT_BEGINNING, C.AT_BEGINNING_STRING, C.AT_END,
                          C.AT_END_STRING, C.AT_BEGINNING_LINE, C.AT_END_LINE):
                    out.append(("sep" if self.anchors else "break", None, 0))
                else:                      # \b \B: zero width, run continues
                    out.append(("zero", None, 0))
            elif op in (C.ASSERT, C.ASSERT_NOT):
                out.append(("zero", None, 0))
            elif op is C.SUBPATTERN:
                _group, add_flags, del_flags, p = av
                if (add_flags | del_flags) & (re.ASCII | re.LOCALE):
                    raise _Unsupported("scoped ASCII/LOCALE flags")
                out.extend(self._items(p))       # flatten: runs merge through it
            elif op is C.ATOMIC_GROUP:
                out.extend(self._items(av))      # atomic only narrows matches
            elif op in (C.MAX_REPEAT, C.MIN_REPEAT, C.POSSESSIVE_REPEAT):
                lo, hi, body = av
                run = self._pure_run(body)
                if run is not None and run[0]:
                    floor = min(lo, REPEAT_FLOOR)
                    out.append(("atom", "(?:%s){%d,}" % (run[0], floor), run[1] * floor))
                    continue
                sub = self._items(body)
                if lo >= 1 and sub and all(k == "sep" for k, _, _ in sub if k != "zero") \
                        and any(k == "sep" for k, _, _ in sub):
                    out.append(("sep", None, 0))
                    continue
                if lo >= 1:
                    out.append(("f", self._seq_formula(body), 0))
                else:
                    out.append(("break", None, 0))
            elif op is C.BRANCH:
                _, branches = av
                runs = [self._pure_run(b) for b in branches]
                if all(r is not None for r in runs) and len(branches) <= MAX_DISJUNCTS:
                    w = min(r[1] for r in runs)
                    out.append(("atom", "(?:%s)" % "|".join(r[0] for r in runs), w))
                    continue
                out.append(("f", _or([self._seq_formula(b) for b in branches]), 0))
            elif op is C.GROUPREF_EXISTS:
                out.append(("break", None, 0))
            else:                                # GROUPREF, FAILURE, ...
                out.append(("break", None, 0))
        return out

    def _seq_formula(self, seq):
        parts, cur, weight = [], [], 0
        start = False

        def close(end):
            if cur and weight >= MIN_TOK_WEIGHT:
                parts.append(Tok(self.B + "".join(cur), start, end, weight))

        for kind, val, wt in self._items(seq):
            if kind == "zero":
                continue
            if kind == "atom":
                cur.append(val)
                weight += wt
                continue
            close(end=(kind == "sep"))
            cur, weight = [], 0
            start = kind == "sep"
            if kind == "f":
                parts.append(val)
        close(end=False)
        return _and(parts)


def _score(f):
    if isinstance(f, Tok):
        return f.weight * 10 + (15 if f.start else 0) + (5 if f.end else 0)
    if isinstance(f, Or):
        return min(_score(p) for p in f.parts)
    if isinstance(f, And):
        return max(_score(p) for p in f.parts)
    return 0


def _passes(f):
    """Full term-dictionary passes needed (a start-anchored run is a cheap prefix walk)."""
    if f is TRUE:
        return 0
    if isinstance(f, Tok):
        return 0 if f.start else 1
    if isinstance(f, Or) and all(isinstance(p, Tok) for p in f.parts):
        return 0 if all(p.start for p in f.parts) else 1   # merged into one regex
    return sum(_passes(p) for p in f.parts)


def _budget(f, conjuncts=MAX_CONJUNCTS, passes=MAX_FULL_PASSES):
    """Weaken f (never strengthen) to a small, cheap query."""
    if f is TRUE or isinstance(f, Tok):
        return f if _passes(f) <= passes else TRUE
    if isinstance(f, Or):
        if len(f.parts) > MAX_DISJUNCTS:
            return TRUE
        g = _or([_budget(p, 1, passes) for p in f.parts])
        return g if _passes(g) <= passes else TRUE
    chosen, used = [], 0
    for p in sorted(f.parts, key=lambda p: (_passes(p) > 0, -_score(p))):
        if len(chosen) == conjuncts:
            break
        if _passes(p) and any(isinstance(c, Tok) and c.start and c.weight >= 6 for c in chosen):
            break   # a selective prefix constraint is already in; skip the ~4 s pass
        q = _budget(p, 1, passes - used)
        if q is TRUE:
            continue
        chosen.append(q)
        used += _passes(q)
    return _and(chosen)


def _has_literal_brace(seq) -> bool:
    """True when the parsed pattern contains '{' as a plain literal outside a
    character class. The regex module reads 'x{e<=1}', 'x{i<=1}', 'x{d}' as
    fuzzy-match constraints, while stdlib re (what this module parses) reads
    them as literal text, so the structure parsed here would be wrong."""
    for op, av in seq:
        if op is C.LITERAL and av == 0x7B:
            return True
        if op is C.SUBPATTERN:
            if _has_literal_brace(av[-1]):
                return True
        elif op in (C.MAX_REPEAT, C.MIN_REPEAT, C.POSSESSIVE_REPEAT):
            if _has_literal_brace(av[2]):
                return True
        elif op is C.BRANCH:
            if any(_has_literal_brace(b) for b in av[1]):
                return True
        elif op in (C.ASSERT, C.ASSERT_NOT):
            if _has_literal_brace(av[1]):
                return True
        elif op is C.ATOMIC_GROUP:
            if _has_literal_brace(av):
                return True
        elif op is C.GROUPREF_EXISTS:
            if _has_literal_brace(av[1]) or (av[2] is not None and _has_literal_brace(av[2])):
                return True
    return False


_SET_OPERATORS = frozenset("-&~|")


def _set_syntax_diverges(pattern: str) -> bool:
    """True when a character class in the pattern holds a '[' or a doubled set
    operator ('--', '&&', '~~', '||').

    The regex module reads '[:alpha:]' / '[:^digit:]' inside a class as a POSIX
    class (anywhere in it, not only at its start); stdlib re reads the same '['
    as a literal and closes the class at the POSIX class's ']', so
    'ש[a[:alpha:]]ום' needs the text ']ום' in re's parse and matches 'שלום' in
    regex's. stdlib warns ("Possible nested set") only for '[[' at a class's
    start and for doubled operators, and catching that warning is process-wide
    state, so the source is read here: a class opens at an unescaped '[', a
    '\\' escapes the next character, and a ']' first in the class (after an
    optional '^') is a literal, as both engines read it. Conservative: any '['
    inside a class counts, even where the engines agree (it costs speed, never
    a match)."""
    i, n = 0, len(pattern)
    while i < n:
        ch = pattern[i]
        if ch == "\\":
            i += 2
            continue
        i += 1
        if ch != "[":
            continue
        if i < n and pattern[i] == "^":
            i += 1
        if i < n and pattern[i] == "]":
            i += 1
        while i < n:
            ch = pattern[i]
            if ch == "\\":
                i += 2
                continue
            if ch == "]":
                i += 1
                break
            if ch == "[":
                return True
            if ch in _SET_OPERATORS and i + 1 < n and pattern[i + 1] == ch:
                return True
            i += 1
    return False


def extract_formula(pattern: str, *, stripped: bool, anchors: bool = True):
    # The in-process matcher is the regex module (shared/search_regex.py
    # compile()); native research workers use stdlib re. The structure below
    # comes from stdlib's parser, so any pattern the two engines may read
    # differently gets no prefilter: all documents are candidates.
    if _set_syntax_diverges(pattern):
        return TRUE   # e.g. 'ש[a[:alpha:]]ום': a POSIX class in regex, a literal '[' in re
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            parsed = sre_parse.parse(pattern, re.IGNORECASE)
    except re.error:
        return TRUE
    if any(issubclass(w.category, FutureWarning) for w in caught):
        return TRUE   # e.g. '[[:alpha:]]': a POSIX class in regex, a nested set warning in re
    if _has_literal_brace(parsed):
        return TRUE   # e.g. '(?:שלום){e<=1}': a fuzzy constraint in regex, literal text in re
    if parsed.state.flags & (re.ASCII | re.LOCALE):
        return TRUE
    try:
        f = _Builder(stripped, anchors)._seq_formula(parsed)
    except (_Unsupported, RecursionError):
        return TRUE
    return _budget(f)


def formula_to_query(f, schema, field="content"):
    """Tantivy Query for formula f; None means 'all documents'."""
    if f is TRUE:
        return None
    if isinstance(f, Tok):
        return tantivy.Query.regex_query(schema, field, f.term_regex())
    if isinstance(f, Or):
        if all(isinstance(p, Tok) for p in f.parts):   # one FST pass
            alt = "|".join("(?:%s)" % p.term_regex() for p in f.parts)
            return tantivy.Query.regex_query(schema, field, alt)
        subs = [formula_to_query(p, schema, field) for p in f.parts]
        if any(s is None for s in subs):
            return None
        return tantivy.Query.boolean_query([(tantivy.Occur.Should, s) for s in subs])
    subs = [q for q in (formula_to_query(p, schema, field) for p in f.parts) if q is not None]
    if not subs:
        return None
    if len(subs) == 1:
        return subs[0]
    return tantivy.Query.boolean_query([(tantivy.Occur.Must, s) for s in subs])


# ---------------------------------------------------------------- index side
_PROBE = "\"Ab,c'd[e] f\""


def tokenizer_kind(index, field="content"):
    """'hebword', 'unsplit' (whitespace: splits on whitespace only, so each
    token holds whole hebword tokens and no line break), or None when the
    field's tokenizer is not one this prefilter understands. 'raw' is None:
    its single token spans line breaks, which a term regex '.*X.*' does not
    cross.

    tantivy-py exposes no schema introspection, so parse a probe phrase with the
    field's own analyzer and read the terms back. Not cached: the probe costs
    microseconds and a reopened index may use another tokenizer."""
    try:
        text = repr(index.parse_query(_PROBE, [field]))
    except Exception:
        return None
    terms = re.findall(r'Term\(field=\d+, type=Str, "((?:[^"\\]|\\.)*)"\)', text)
    if terms == ["Ab", "c'd[e]", "f"]:
        return "hebword"
    if terms == ["Ab,c'd[e]", "f"]:
        return "unsplit"
    return None


def build_candidate_query(pattern, index, *, stripped, field="content", kind=None):
    """Return (tantivy.Query or None, formula). None means every document."""
    kind = kind if kind is not None else tokenizer_kind(index, field)
    if kind is None:
        return None, TRUE
    f = extract_formula(pattern, stripped=stripped, anchors=(kind == "hebword"))
    try:
        return formula_to_query(f, index.schema, field), f
    except ValueError:        # e.g. the term regex exceeds tantivy's automaton limits
        return None, TRUE


def describe(f) -> str:
    if f is TRUE:
        return "TRUE(all docs)"
    if isinstance(f, Tok):
        return "tok/%s/" % f.term_regex()
    j = " AND " if isinstance(f, And) else " OR "
    return "(" + j.join(describe(p) for p in f.parts) + ")"

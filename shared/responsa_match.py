# -*- coding: utf-8 -*-
"""The Responsa word compiler: one meaning of a query on every search path.

docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md, Phase 1 (§4, §5.1-5.3). Every Responsa
pipeline -- the main search, the line-break search, My Library (LOCAL), both together
(ALL) and the complete-match pass (``ids_only``) -- turns its parsed components into
the same three things here:

* **retrieval**, per index (each with its own schema and content_search flag): every
  finite form as a scored term (``component_query_string``, the clause the query string
  always had), ORed with a bracket- and mark-tolerant ``regex_query`` of the same forms,
  so a page holding ``של[ו]ם`` is a candidate for ``שלום`` (§5.1);
* **check**, the stage-2 pattern: every word bounded by Exact's whole-word predicate
  (``_starts_word`` / ``_ends_word``), the first and last included (§4), inside each
  ``AND`` lookahead and in both ``bidirectional`` branches; ``#X#``'s forms as
  length-grouped prefix trees (``_variant_alternation``), never the raw tree;
* **highlight**, the pattern a row carries for the viewers: the same words with
  fixed-width lookarounds that approximate those edges (``compile_search_regex``
  validates with Python ``re``, which has no variable-width lookbehind).

Which rows exist is decided by ``accept``: Exact's predicate on the span of every word
the match holds (a named group per word). The check pattern's own lookarounds only help
the regex engine backtrack to an acceptable occurrence: its end lookahead is the
predicate itself, its start lookbehinds are a lenient approximation (they never reject
what the predicate accepts), and the first word has none, so the pattern keeps re's
fast scan for its first characters (a leading lookbehind made a search 5x slower,
2026-10-21). Where the pattern itself places a word -- a line's ``.*``, an ``AND``
lookahead, a ``[N]`` gap -- a place the predicate rejects would hide one it accepts
(the regex commits to one place and only ``accept`` sees the edge), so after such a
rejection the text is searched on with the exact pattern (``_CheckPattern``).

Stars keep today's meaning in Phase 1 (the fixed lists of ``#X`` / ``X#`` in retrieval,
``\\S*`` in the check); the shapes that cannot mean what the help says are refused here,
in one place (``refuse_unsupported_stars``), so Phase 3 can lift them.
"""

import functools
import re
import unicodedata
from dataclasses import dataclass, field
from typing import List, Optional

from shared.config import Config
from shared.responsa import (
    ResponsaComponent,
    _SOFIT_TO_NORMAL,
    _tr,
    expand_grammatical_prefixes,
    expand_grammatical_suffixes,
    expand_judeo_arabic,
    expand_plene_defective,
)
from shared.search_regex import Pattern, compile as compile_search_regex


# ==============================================================================
#  Refused star shapes (Phase 1)
# ==============================================================================

STAR_REFUSAL_CODE = 'responsa_unsupported_star'
STAR_REFUSAL_TEXT = (
    "Responsa search takes a * only at the start or the end of a word (*word, word*, *word*). "
    "A * inside a word, a word made only of *s, and other shapes with several *s are not "
    "supported yet. For any one word, use a gap such as [1]."
)


class ResponsaQueryRefused(ValueError):
    """A query the engine does not search yet: refused with a message, never zero rows.

    A ValueError, so every surface that already shows a query error (the desktop
    search thread, the website, the API's 400) shows this one; ``code`` names it for
    the API and MCP (``shared/api_errors.py``)."""

    code = STAR_REFUSAL_CODE

    def __init__(self, message=None, code=None):
        super().__init__(message or _tr(STAR_REFUSAL_TEXT))
        if code:
            self.code = code


def _star_word_ok(word):
    """One star at the start, one at the end, or one at each end of a word with
    letters of its own (``*X*`` is rewritten as ``#X#``); no star at all."""
    if '*' not in word:
        return True
    core = word.strip('*')
    if not core or '*' in core:
        return False
    lead = len(word) - len(word.lstrip('*'))
    trail = len(word) - len(word.rstrip('*'))
    return lead <= 1 and trail <= 1


def refuse_unsupported_stars(components):
    """Raise ResponsaQueryRefused when a component holds a star shape Phase 1 cannot
    search as the help documents it: a star inside a word (``כ*נים``), a word of stars
    only (``*``, ``**``), or several stars that are not one at each end (``*א*ב*``).

    Today the first returns nothing (the star is searched as a literal ``*``), the
    second matches any text, the third finds what it finds by chance (plan §2); the
    one place to lift them is here (Phase 3)."""
    for comp in components:
        if comp.wildcard == 'pattern':
            if not _star_word_ok(comp.wildcard_pattern or ''):
                raise ResponsaQueryRefused()
            continue
        if comp.inline_pattern and '*' in comp.inline_pattern:
            raise ResponsaQueryRefused()
        for word in comp.words:
            if '*' in word:
                raise ResponsaQueryRefused()


def rewrite_both_end_stars(components):
    """``*X*`` -> ``#X#`` in place (today's meaning until Phase 3); True if any was."""
    rewritten = False
    for comp in components:
        if (comp.wildcard == 'pattern' and comp.wildcard_pattern
                and comp.wildcard_pattern.startswith('*') and comp.wildcard_pattern.endswith('*')
                and comp.wildcard_pattern.count('*') == 2):
            stem = comp.wildcard_pattern.strip('*')
            if stem:
                comp.words = [stem]
                comp.wildcard = None
                comp.wildcard_pattern = None
                comp.grammatical_prefixes = True
                comp.grammatical_suffixes = True
                rewritten = True
    return rewritten


# ==============================================================================
#  Expansion: the literal part first, in one order for retrieval and check
# ==============================================================================

class _OverLimit(Exception):
    """The expansion passed the guard's limit (raised early, before it is complete)."""


def _inline_forms(pattern, limit=None):
    """``אירו(ס/ש)ין`` -> ``['אירוסין', 'אירושין']``: every choice of every group, in
    order (an empty alternative is the group left out). Raises _OverLimit as soon as
    there are more than *limit* forms (default ``Config.MAX_EXPANDED_TERMS``): each
    group multiplies them, so ``a(a/b)(a/b)...`` within the 1,000-character query cap
    spelt out would exhaust the process's memory before the guard counted it (Codex
    review of PR #432)."""
    limit = Config.MAX_EXPANDED_TERMS if limit is None else limit
    forms = ['']
    i = 0
    while i < len(pattern):
        if pattern[i] == '(':
            close = pattern.find(')', i)
            if close == -1:
                forms = [f + pattern[i] for f in forms]
                i += 1
                continue
            alternatives = list(dict.fromkeys(pattern[i + 1:close].split('/')))
            out = {}
            for f in forms:
                for a in alternatives:
                    out[f + a] = None
                    if len(out) > limit:
                        raise _OverLimit()
            forms = list(out)
            i = close + 1
        else:
            forms = [f + pattern[i] for f in forms]
            i += 1
    return list(dict.fromkeys(f for f in forms if f))


def literal_forms(comp: ResponsaComponent, limit=None) -> List[str]:
    """The component's own words: an OR group's words, or an inline alternation spelt
    out -- the compiler expands both on both sides (plan §4; today the inline branch
    returned before every other expansion, Codex 2.16)."""
    if comp.inline_pattern:
        return _inline_forms(comp.inline_pattern, limit)
    return [w for w in comp.words if w]


class _Budget:
    def __init__(self, limit):
        self.limit = limit
        self.used = 0

    def take(self, n):
        self.used += n
        if self.limit is not None and self.used > self.limit:
            raise _OverLimit()


def _variants(var_mgr, word, mode, limit):
    # Lazy: shared.search_engine imports this module.
    from shared.search_engine import _variants_noting_cutoff
    return _variants_noting_cutoff(var_mgr, word, mode, limit)


def expand_stems(comp, variants_on, ja_on, variant_mode, var_mgr, budget=None):
    """The component's finite forms: its words, then plene/defective (``%``), the
    grammatical prefixes and suffixes (``#``), Judeo-Arabic, spelling variants --
    the order the main path always used, now the one order for retrieval and check.
    For a star word these are the stems the star attaches to."""
    forms = literal_forms(comp, budget.limit if budget is not None else None)

    def step(fn):
        nonlocal forms
        out = []
        for w in forms:
            out.extend(fn(w))
        forms = list(dict.fromkeys(out))

    if comp.plene_defective:
        step(expand_plene_defective)
    if comp.grammatical_prefixes:
        step(expand_grammatical_prefixes)
    if comp.grammatical_suffixes:
        step(expand_grammatical_suffixes)
    if ja_on:
        step(expand_judeo_arabic)
    if variants_on and var_mgr is not None:
        out = []
        for w in forms:
            try:
                out.extend(_variants(var_mgr, w, variant_mode, 200))
            except MemoryError:
                raise
            except Exception:
                out.append(w)   # Variant expansion failed for this word; use original
            if budget is not None and budget.limit is not None and len(out) > budget.limit:
                raise _OverLimit()
        forms = list(dict.fromkeys(out))
    if budget is not None:
        budget.take(len(retrieval_terms(comp, forms)))
    return forms


def _star_list(comp, stems):
    """Today's retrieval list of a star word (Phase 1; Phase 3 replaces it): ``*X``
    -> the 24 prefix forms, ``X*`` -> the 25 suffix forms and the stem with its final
    letter made non-final -- of every stem, not only the typed word (Codex 2.17: the
    check accepted a variant's prefixed form the lookup never asked for)."""
    out = []
    for w in stems:
        if comp.wildcard == 'prefix':
            out.extend(expand_grammatical_prefixes(w))
        elif comp.wildcard == 'suffix':
            out.extend(expand_grammatical_suffixes(w))
            if w and w[-1] in _SOFIT_TO_NORMAL:
                out.append(w[:-1] + _SOFIT_TO_NORMAL[w[-1]])
    return out


def retrieval_terms(comp, stems):
    """Every form the lookup asks for: the stems, and a star word's list."""
    return list(dict.fromkeys(list(stems) + _star_list(comp, stems)))


# ==============================================================================
#  The guard (plan §5.3): real cost, one cascade, every path
# ==============================================================================

def expand_under_guard(components, variants_on, ja_on, variant_mode, var_mgr):
    """Expand *components* (positive ones; negated words are letters to exclude) under
    ``Config.MAX_EXPANDED_TERMS``, the real number of terms the lookup holds -- forms,
    and a star word's list -- counted as they are made, not estimated.

    Measured 2026-10-21 on the local index (2,220,293 docs): an OR of 1,200 scored
    terms ran in 0.6-0.8 s, 4,800 in 2.9-3.7 s, 9,600 in 5.5-6.0 s. So ``#X# #Y#``
    (1,200) keeps every suffix -- the old 500-term estimate cut ``#X#``'s (600) --
    while ``#X# #Y#`` with Judeo-Arabic (9,600) still steps down.

    Over the limit, the same cascade as always: variants to basic, variants off,
    Judeo-Arabic off, plene/defective off, suffixes off, prefixes off, then a
    ValueError. Returns ``(stems per component, warning or None, effective options)``;
    components are changed in place by the last three steps, as before."""
    limit = Config.MAX_EXPANDED_TERMS
    state = {'variants_on': bool(variants_on), 'ja_on': bool(ja_on), 'variant_mode': variant_mode}
    warnings = []

    def attempt():
        budget = _Budget(limit)
        return [expand_stems(c, state['variants_on'], state['ja_on'], state['variant_mode'],
                             var_mgr, budget) for c in components]

    def try_now():
        try:
            return attempt()
        except _OverLimit:
            return None

    steps = []
    if state['variants_on'] and variant_mode != 'variants':
        steps.append(('variant_mode', _tr("Variant mode downgraded to basic (30 pairs)")))
    if state['variants_on']:
        steps.append(('variants_on', _tr("Spelling variants disabled")))
    if state['ja_on']:
        steps.append(('ja_on', _tr("Judeo-Arabic expansion disabled")))
    for flag, text in (('plene_defective', "Plene/defective expansion disabled"),
                       ('grammatical_suffixes', "Grammatical suffix expansion disabled"),
                       ('grammatical_prefixes', "Grammatical prefix expansion disabled")):
        if any(getattr(c, flag) for c in components):
            steps.append((flag, _tr(text)))

    stems = try_now()
    for what, text in steps:
        if stems is not None:
            break
        if what == 'variant_mode':
            state['variant_mode'] = 'variants'
        elif what in ('variants_on', 'ja_on'):
            state[what] = False
        else:
            for c in components:
                setattr(c, what, False)
        warnings.append(text)
        stems = try_now()
    if stems is None:
        raise guard_refusal(limit)
    return stems, ('; '.join(warnings) if warnings else None), state


GUARD_REFUSAL_CODE = 'responsa_query_too_large'


def guard_refusal(limit=None):
    """The query is over the guard's limit after every step down: refused with its
    message and its code -- a 400 on the API, not the 500 a plain ValueError became
    there (Codex review of PR #432). Still a ValueError for the apps' handlers."""
    limit = Config.MAX_EXPANDED_TERMS if limit is None else limit
    return ResponsaQueryRefused(
        _tr("Query exceeds the limit of {limit} expanded terms (estimated {count} terms). "
            "Please simplify your query by using fewer OR-group alternatives or "
            "removing the # (grammatical prefixes) modifier from some terms.").format(
                limit=limit, count=f'{limit}+'),
        code=GUARD_REFUSAL_CODE)


# ==============================================================================
#  Word edges (Exact's predicate) as regex
# ==============================================================================

_IN_WORD_QUOTES = '\'"׳״‘’[]'


def _class(points):
    """A character class of *points* (sorted code points), as ranges."""
    out = []
    i = 0
    while i < len(points):
        first = last = points[i]
        i += 1
        while i < len(points) and points[i] == last + 1:
            last = points[i]
            i += 1
        out.append(re.escape(chr(first)) + ('' if last == first else '-' + re.escape(chr(last))))
    return '[' + ''.join(out) + ']'


@functools.lru_cache(maxsize=1)
def edge_classes():
    """(continues, looked past): ``_continues_word``'s characters (letters, decimal
    digits, connector punctuation) and ``_looked_past``'s (marks, in-word quotes and
    brackets), over the Basic Multilingual Plane, as explicit classes -- the same in
    ``re`` and in the ``regex`` module, whose ``\\w`` differs from re's (marks). A
    character beyond the BMP is in neither, so a pattern bounded with them never
    rejects what the predicate accepts there; ``accept`` decides. ~0.2 s, once."""
    cont, past = [], []
    for cp in range(0x10000):
        cat = unicodedata.category(chr(cp))
        if cat[0] == 'L' or cat in ('Nd', 'Pc'):
            cont.append(cp)
        elif cat[0] == 'M' or chr(cp) in _IN_WORD_QUOTES:
            past.append(cp)
    past = sorted(set(past) | {ord(c) for c in _IN_WORD_QUOTES})
    return _class(cont), _class(past)


# Start lookbehinds look past up to this many marks, quotes or brackets: Python's
# lookbehind is fixed-width, so the run is spelt out per length.
_LOOKBEHIND_RUN = 3


def start_edge(strict=False):
    """Before a bounded word. Lenient (the check): rejects only a word whose
    neighbour, past at most _LOOKBEHIND_RUN looked-past characters, continues a word
    -- which the predicate rejects too. Strict (the highlight): also rejects a longer
    run of looked-past characters, so a highlight is never marked where the predicate
    says no (it may then be missing)."""
    cont, past = edge_classes()
    parts = ['(?<!' + cont + past * n + ')' for n in range(_LOOKBEHIND_RUN + 1)]
    if strict:
        parts.append('(?<!' + past * (_LOOKBEHIND_RUN + 1) + ')')
    return ''.join(parts)


def end_edge():
    """After a bounded word: Exact's ``_ends_word`` exactly (a lookahead may be any
    length)."""
    cont, past = edge_classes()
    return '(?!' + past + '*' + cont + ')'


def exact_start(first=None):
    """Before a bounded word, exactly: Exact's ``_starts_word`` as text the pattern
    reads -- the run of looked-past characters before the word (part of the match), and
    before the run a character that neither continues a word nor is looked past, or
    none. For the exact pattern only (``_CheckPattern``): its large classes are tried
    wherever the pattern backtracks. *first*: the characters the word can begin with,
    tried first."""
    cont, past = edge_classes()
    guard = ('(?=' + past + '*[' + ''.join(re.escape(c) for c in sorted(first)) + '])'
             if first else '')
    return guard + '(?<![' + cont[1:-1] + past[1:-1] + '])' + past + '*'


# ==============================================================================
#  The compiled query
# ==============================================================================

@dataclass
class WordPlan:
    """One positive component, compiled."""
    component: ResponsaComponent
    stems: List[str]                      # finite forms (a star word's stems)
    flex_patterns: List[str] = field(default_factory=list)

    @property
    def star(self) -> Optional[str]:
        return self.component.wildcard if self.component.wildcard in ('prefix', 'suffix', 'pattern') else None

    @property
    def bounds(self):
        """(start bounded, end bounded): a star relaxes only its own side (§4)."""
        if self.star == 'pattern':
            pattern = self.component.wildcard_pattern or ''
            return not pattern.startswith('*'), not pattern.endswith('*')
        return self.star != 'prefix', self.star != 'suffix'

    @property
    def original_words(self):
        return list(self.component.words)

    def terms(self):
        return retrieval_terms(self.component, self.stems)

    def first_chars(self, flex_spacing=False):
        """The characters the word's own pattern can begin with (its forms'; with flex
        spacing, its typed words' too), or None when it can begin with anything."""
        if self.star == 'pattern':
            pattern = self.component.wildcard_pattern or ''
            return {pattern[0]} if pattern and pattern[0] != '*' else None
        if self.star == 'prefix':
            return None
        chars = {f[0] for f in self.stems if f}
        if flex_spacing:
            chars |= {w[0] for w in self.component.words if w}
        return chars or None

    def tantivy_dict(self):
        """The component dict build_tantivy_query / component_query_string read."""
        return {
            'tantivy_terms': self.stems,
            'regex_terms': self.stems,
            'original_words': self.original_words,
            'wildcard_stems': self.stems,
            'wildcard': self.component.wildcard,
            'wildcard_pattern': self.component.wildcard_pattern,
            'flex_patterns': self.flex_patterns,
            'inline_pattern': None,
        }

    def body(self, flex_spacing, inserter=None):
        """The word's own pattern, without edges."""
        from shared.search_engine import _build_wildcard_regex, _variant_alternation, MARK_TOLERANT_INSERTER
        if self.star:
            return _build_wildcard_regex({
                'wildcard': self.star, 'wildcard_pattern': self.component.wildcard_pattern,
                'regex_terms': sorted(set(self.stems), key=len, reverse=True)})
        forms = sorted(set(self.stems), key=len, reverse=True)
        alternatives = [_variant_alternation(forms, inserter=inserter or MARK_TOLERANT_INSERTER)] if forms else []
        if flex_spacing:
            alternatives.extend(fp for fp in self.flex_patterns if fp not in alternatives)
        return '(?:' + '|'.join(alternatives) + ')'


def plans_from_dicts(component_dicts):
    """WordPlans from the component dicts build_regex_pattern has always taken
    (``regex_terms``, ``wildcard``, ``inline_pattern``, ``flex_patterns`` ...). A
    component with nothing to match is left out, as before."""
    plans = []
    for d in component_dicts:
        comp = ResponsaComponent(words=list(d.get('original_words') or []),
                                 wildcard=d.get('wildcard'),
                                 wildcard_pattern=d.get('wildcard_pattern'),
                                 inline_pattern=d.get('inline_pattern'))
        if comp.inline_pattern:
            try:
                stems = _inline_forms(comp.inline_pattern)
            except _OverLimit:
                raise guard_refusal() from None
        else:
            stems = [t for t in (d.get('regex_terms') or []) if t]
        if comp.wildcard == 'pattern' and not comp.wildcard_pattern:
            continue
        if not stems and comp.wildcard != 'pattern' and not d.get('flex_patterns'):
            continue
        plans.append(WordPlan(component=comp, stems=stems,
                              flex_patterns=list(d.get('flex_patterns') or [])))
    return plans


def _separator(gap_val, flex):
    """The text between two words: non-word characters, with up to *gap_val* words."""
    if gap_val == 0:
        return r'[^\w֐-׿\']*' if flex else r'[^\w֐-׿\']+'
    return rf'(?:[^\w֐-׿\']+{Config.WORD_TOKEN_PATTERN}){{0,{gap_val}}}[^\w֐-׿\']+'


@dataclass
class CompiledResponsa:
    """A Responsa query compiled for retrieval, check and highlight."""
    plans: List[WordPlan]
    negated_words: List[str]
    warning: Optional[str]
    options: dict                          # effective variants_on / ja_on / variant_mode
    check_pattern: str
    highlight_pattern: str
    groups: list                           # (group name, start bounded, end bounded)
    flex_spacing: bool = False
    flags: int = re.IGNORECASE
    exact_pattern: str = ''                # the check with exact starts (_CheckPattern)

    @property
    def expanded_count(self):
        return sum(len(p.stems) for p in self.plans)

    @property
    def has_star(self):
        return any(p.star for p in self.plans)

    def compile_check(self):
        """The check pattern, compiled: its ``search`` returns only a match ``accept``
        takes (``_CheckPattern``)."""
        return _CheckPattern(self)

    def accept(self, text, m):
        """Exact's whole-word predicate on every word the match holds (§5.2): the
        rows that exist. *m* is a match of the check pattern in *text*."""
        from shared.search_engine import _starts_word, _ends_word
        for name, bounded_start, bounded_end in self.groups:
            s, e = m.span(name)
            if s < 0:
                continue
            if bounded_start and not _starts_word(text, s):
                return False
            if bounded_end and not _ends_word(text, e):
                return False
        return True

    def word_span(self, m):
        """(start, end) from the first word the match holds to its last -- a line
        query's match is its whole line(s), its words are what the row marks."""
        spans = [m.span(name) for name, _bs, _be in self.groups]
        spans = [sp for sp in spans if sp[0] >= 0]
        if not spans:
            return m.span()
        return min(s for s, _e in spans), max(e for _s, e in spans)

    def retrieval(self, index, field='content', has_content_search=False, flex_spacing=False):
        """The lookup on *index* (its own schema; plan §5.1, Codex 2.4): per word its
        scored terms on *field*, ORed -- on the content field -- with a bracket- and
        mark-tolerant pattern of its forms; the words ANDed."""
        return retrieval_query(self.plans, index, field=field,
                               has_content_search=has_content_search, flex_spacing=flex_spacing)


class _CheckPattern(Pattern):
    """The check pattern, as shared/search_regex.py compiles it, whose ``search``
    returns the first match ``accept`` takes, or None.

    The fast pattern matches every place the predicate accepts (its start lookbehinds
    are lenient), so nothing acceptable starts before its first match. Where it places
    a word itself -- a line's ``.*``, an ``AND`` lookahead, a ``[N]`` gap -- it commits
    to one place: a rejected one (``beta x[[[[beta``, a run of brackets longer than the
    lookbehinds read) hid the acceptable ``beta`` before it (Codex review of PR #432).
    So once ``accept`` rejects a match, the text is searched on from that match with
    the exact pattern, which backtracks past such a place. A sequence's first word is
    unbounded in both: the scan's next start reaches its next occurrence."""

    def __init__(self, compiled):
        self.__dict__.update(compile_search_regex(compiled.check_pattern, compiled.flags).__dict__)
        self._responsa = compiled
        self._exact = None

    def _exact_regex(self):
        if self._exact is None:
            self._exact = compile_search_regex(
                self._responsa.exact_pattern or self._responsa.check_pattern, self._responsa.flags)
        return self._exact

    def search(self, string, pos=0, endpos=None):
        m = super().search(string, pos, endpos)
        if m is None or self._responsa.accept(string, m):
            return m
        exact = self._exact_regex()
        m = exact.search(string, m.start(), endpos)
        while m is not None and not self._responsa.accept(string, m):
            m = exact.search(string, m.start() + 1, endpos)
        return m


def _names(prefix, n):
    return [f'{prefix}{i}' for i in range(n)]


def _word_piece(plan, name, flex, bound_start, bound_end, strict, inserter=None, exact=False):
    body = plan.body(flex, inserter=inserter)
    piece = ''
    if bound_start:
        # The start lookbehinds' classes are large: try them only where the word can
        # begin. In the regex module a line query (^.*word) and an AND lookahead
        # ([\s\S]*word) reach them at every position they backtrack through
        # (2026-10-22: 6.8 -> 85.6 s and 11.3 -> 103.3 s on the desktop without this).
        first = plan.first_chars(flex)
        if exact:
            piece += exact_start(first)
        else:
            if first:
                piece += '(?=[' + ''.join(re.escape(c) for c in sorted(first)) + '])'
            piece += start_edge(strict)
    piece += f'(?P<{name}>{body})' if name else f'(?:{body})'
    if bound_end:
        piece += end_edge()
    return piece


def _sequence(plans, names, gaps, max_gap, flex, strict, inserter=None, exact=False):
    """Words joined by separators. The first word gets no start lookbehind in the
    check (*strict* False): ``accept`` tests it, and a leading lookbehind would cost
    re its fast scan. With flex spacing only the joined unit's outer edges are
    bounded, so ``שלוםעולם`` still matches ``שלום עולם`` (§4)."""
    out = []
    last = len(plans) - 1
    groups = []
    for i, plan in enumerate(plans):
        bs, be = plan.bounds
        if flex:
            bs = bs and i == 0
            be = be and i == last
        if i:
            gap = gaps[i - 1] if gaps and i - 1 < len(gaps) and gaps[i - 1] is not None else max_gap
            out.append(_separator(gap, flex))
        name = names[i] if names else None
        out.append(_word_piece(plan, name, flex, bs and (strict or i > 0), be, strict, inserter, exact))
        if name:
            groups.append((name, bs, be))
    return ''.join(out), groups


def build_patterns(plans, max_gap, per_pair_gaps=None, *, flex_spacing=False, bidirectional=False,
                   within_document=False, highlight_brackets=False, exact_starts=False):
    """(check pattern, highlight pattern, groups) for the word sequence *plans*.

    *highlight_brackets*: the query has no brackets of its own, so the row's text is
    matched with its brackets out; the highlight -- run on the original text by the
    viewers -- then lets brackets stand between letters too, so ``של[ו]ם`` is marked
    for ``שלום``. *exact_starts*: the check's exact pattern instead (``exact_start``
    before every bounded word the check bounds; ``_CheckPattern``), and no highlight."""
    from shared.search_engine import _FUZZY_INSERTER
    hl_inserter = _FUZZY_INSERTER if highlight_brackets else None

    def one(strict, named, exact=False):
        inserter = hl_inserter if strict else None
        if within_document:
            # At the earliest acceptable word, every word must occur in the rest of the
            # text (any order or distance): each one bounded inside its lookahead.
            groups = []
            req = []
            for i, plan in enumerate(plans):
                bs, be = plan.bounds
                name = f'ra{i}' if named else None
                req.append(r'(?=[\s\S]*' + _word_piece(plan, name, flex_spacing, bs, be, strict, inserter, exact) + ')')
                if name:
                    groups.append((name, bs, be))
            term_alts = []
            plain_alts = []
            for i, plan in enumerate(plans):
                bs, be = plan.bounds
                name = f'rt{i}' if named else None
                term_alts.append(_word_piece(plan, name, flex_spacing, bs, be, strict, inserter, exact))
                plain_alts.append(_word_piece(plan, None, flex_spacing, bs, be, strict, inserter, exact))
                if name:
                    groups.append((name, bs, be))
            term = '(?:' + '|'.join(term_alts) + ')'
            lead = '(?:' + '|'.join(plain_alts) + ')'
            return '(?=' + lead + ')' + ''.join(req) + term, groups
        forward, groups = _sequence(plans, _names('rf', len(plans)) if named else None,
                                    per_pair_gaps, max_gap, flex_spacing, strict, inserter, exact)
        if bidirectional and len(plans) >= 2:
            reversed_gaps = list(reversed(per_pair_gaps)) if per_pair_gaps else per_pair_gaps
            backward, back_groups = _sequence(list(reversed(plans)),
                                              _names('rb', len(plans)) if named else None,
                                              reversed_gaps, max_gap, flex_spacing, strict, inserter, exact)
            return f'(?:{forward})|(?:{backward})', groups + back_groups
        return forward, groups

    if exact_starts:
        exact, groups = one(strict=False, named=True, exact=True)
        return exact, None, groups
    check, groups = one(strict=False, named=True)
    highlight, _ = one(strict=True, named=False)
    return check, highlight, groups


def compile_components(components, responsa_options, var_mgr, max_gap, per_pair_gaps=None,
                       query_has_brackets=False):
    """Compile parsed *components* (one query, no line groups) into a CompiledResponsa.

    Refuses the unsupported star shapes, rewrites ``*X*`` as ``#X#`` (with today's
    warning), splits off the negated words (letters to exclude), and expands the rest
    under the guard."""
    from shared.search_engine import _make_flex_spacing_pattern
    opts = responsa_options or {}
    refuse_unsupported_stars(components)
    warnings = []
    if rewrite_both_end_stars(components):
        warnings.append(_tr("*word* rewritten as #word# (prefix + suffix expansion)"))
    positive = [c for c in components if not c.negated]
    negated = negated_words(components)
    stems, guard_warning, state = expand_under_guard(
        positive, opts.get('variants', False), opts.get('ja', False),
        opts.get('variant_mode', 'exact'), var_mgr)
    if guard_warning:
        warnings.append(guard_warning)
    flex = bool(opts.get('flex_spacing', False))
    plans = []
    for comp, forms in zip(positive, stems):
        plans.append(WordPlan(component=comp, stems=forms,
                              flex_patterns=[_make_flex_spacing_pattern(w) for w in comp.words] if flex else []))
    check = highlight = ''
    groups = []
    exact = ''
    if plans:
        shape = dict(flex_spacing=flex, bidirectional=bool(opts.get('bidirectional', False)),
                     within_document=bool(opts.get('within_document', False)))
        check, highlight, groups = build_patterns(
            plans, max_gap, per_pair_gaps, highlight_brackets=not query_has_brackets, **shape)
        exact = build_patterns(plans, max_gap, per_pair_gaps, exact_starts=True, **shape)[0]
    return CompiledResponsa(plans=plans, negated_words=negated,
                            warning='; '.join(warnings) if warnings else None,
                            options=state, check_pattern=check, highlight_pattern=highlight,
                            groups=groups, flex_spacing=flex, exact_pattern=exact)


def _line_word_separator(gap):
    """Between two words of one line (never crosses a line break): whitespace, with
    up to *gap* words between (CR HIGH-6, [N] word gaps in line mode)."""
    if not gap:
        return r'[^\S\n]+'
    return r'(?:[^\S\n]+\S+){0,' + str(int(gap)) + r'}[^\S\n]+'


def compile_line_groups(line_groups, line_gaps, responsa_options, var_mgr, query_has_brackets=False):
    """Compile a line-break query (``_parse_line_break_query``'s groups) through the
    same steps as compile_components: the refused star shapes, ``*X*`` -> ``#X#``,
    negated words split off as letters to exclude, one guard over every word, and
    every word bounded by Exact's predicate -- each group's edges included, while a
    ``|`` anchor still means the line's start or end (§4). Its pattern is MULTILINE.

    *line_groups* are changed in place (negated components leave their group). A
    group left with no word drops out with the line gap before it."""
    opts = responsa_options or {}
    every = [c for g in line_groups for c in g.components]
    refuse_unsupported_stars(every)
    warnings = []
    if rewrite_both_end_stars(every):
        warnings.append(_tr("*word* rewritten as #word# (prefix + suffix expansion)"))
    negated = negated_words(every)
    kept_groups, kept_gaps = [], []
    for gi, group in enumerate(line_groups):
        comps, gaps = [], []
        pending = None
        for ci, comp in enumerate(group.components):
            gap_before = group.word_gaps[ci - 1] if ci and ci - 1 < len(group.word_gaps) else None
            if comp.negated:
                pending = pending if pending is not None else gap_before
                continue
            if comps:
                gaps.append(gap_before if gap_before is not None else pending)
            comps.append(comp)
            pending = None
        if not comps:
            continue
        group.components, group.word_gaps = comps, gaps
        if kept_groups:
            kept_gaps.append(line_gaps[gi - 1] if gi - 1 < len(line_gaps) else None)
        kept_groups.append(group)
    positive = [c for g in kept_groups for c in g.components]
    stems, guard_warning, state = expand_under_guard(
        positive, opts.get('variants', False), opts.get('ja', False),
        opts.get('variant_mode', 'exact'), var_mgr)
    if guard_warning:
        warnings.append(guard_warning)
    plans = [WordPlan(component=c, stems=f) for c, f in zip(positive, stems)]
    by_group, k = [], 0
    for group in kept_groups:
        by_group.append(plans[k:k + len(group.components)])
        k += len(group.components)
    check, highlight, groups = build_line_patterns(kept_groups, by_group, kept_gaps, query_has_brackets)
    exact = build_line_patterns(kept_groups, by_group, kept_gaps, query_has_brackets, exact_starts=True)[0]
    return CompiledResponsa(plans=plans, negated_words=negated,
                            warning='; '.join(warnings) if warnings else None,
                            options=state, check_pattern=check, highlight_pattern=highlight,
                            groups=groups, flags=re.IGNORECASE | re.MULTILINE, exact_pattern=exact)


# The looked-past characters a line's edge word commonly carries: brackets, quotes,
# U+0300-036F and the Hebrew block's marks (build_line_patterns).
_LINE_EDGE_PAST = ("[\\[\\]\"'\u05F3\u05F4\u2018\u2019\u0300-\u036F"
                   "\u0591-\u05BD\u05BF\u05C1\u05C2\u05C4\u05C5\u05C7]")


def build_line_patterns(line_groups, plans_by_group, line_gaps, query_has_brackets=False,
                        exact_starts=False):
    """(check, highlight, groups) of a line-break query: each group one line pattern,
    its words bounded and named, joined by line breaks with the [|N] gaps.
    *exact_starts*: the check's exact pattern instead, and no highlight (as
    build_patterns)."""
    from shared.search_engine import _FUZZY_INSERTER
    hl_inserter = None if query_has_brackets else _FUZZY_INSERTER
    kept_groups, by_group, kept_gaps = line_groups, plans_by_group, line_gaps
    past = edge_classes()[1]

    def one(strict, named, exact=False):
        groups = []
        line_patterns = []
        for gi, (group, gplans) in enumerate(zip(kept_groups, by_group)):
            word_pat = ''
            for ci, plan in enumerate(gplans):
                if ci:
                    word_pat += _line_word_separator(group.word_gaps[ci - 1] if ci - 1 < len(group.word_gaps) else None)
                bs, be = plan.bounds
                name = f'rl{gi}_{ci}' if named else None
                word_pat += _word_piece(plan, name, False, bs, be, strict, hl_inserter if strict else None, exact)
                if name:
                    groups.append((name, bs, be))
            # A | anchor is the line's start or end; the quotes, brackets and marks of
            # the edge word are part of it (Exact's predicate looks past them). The
            # check spells out the common ones: with the full class there, the regex
            # module (the desktop) scanned every position (a line query: 6.8 -> 85.6 s,
            # 2026-10-22); a line opened by another script's mark is the cost.
            edge = past if strict else _LINE_EDGE_PAST
            lead = r'^\s*' + edge + '*' if group.line_start else r'^.*'
            tail = edge + r'*\s*$' if group.line_end else r'.*$'
            line_patterns.append(lead + word_pat + tail)
        parts = [line_patterns[0]] if line_patterns else []
        for i in range(1, len(line_patterns)):
            gap = kept_gaps[i - 1] if i - 1 < len(kept_gaps) and kept_gaps[i - 1] is not None else 0
            parts.append(r'\n' if gap == 0 else r'\n(?:.*\n){' + str(gap) + r'}')
            parts.append(line_patterns[i])
        return ''.join(parts), groups

    if not any(by_group):
        return '', '', []
    if exact_starts:
        exact, groups = one(strict=False, named=True, exact=True)
        return exact, None, groups
    check, groups = one(strict=False, named=True)
    # The viewers compile a row's pattern without flags: MULTILINE goes with it.
    highlight = '(?m:' + one(strict=True, named=False)[0] + ')'
    return check, highlight, groups


def negated_words(components):
    """The letters a ``-word`` excludes: the word, and its plene/defective spellings
    with ``%`` -- as the main path always took them."""
    out = []
    for comp in components:
        if not comp.negated:
            continue
        for w in comp.words:
            out.append(w)
            if comp.plene_defective:
                out.extend(expand_plene_defective(w))
    return out


# ==============================================================================
#  Retrieval
# ==============================================================================

def component_query_string(comp, flex_spacing=False):
    """One component's scored clause, as the Responsa query string always had it: the
    typed word boosted x5, other-length forms x3, a star word's list, the typed word's
    edge-bracket forms, and with flex spacing its split pairs. '' when it has none."""
    from shared.search_engine import _add_bracket_variants
    tantivy_terms = comp.get('tantivy_terms', [])
    original_words = set(comp.get('original_words', []))
    if not tantivy_terms:
        return ''
    clean_vars = []
    seen = set()
    for t in tantivy_terms:
        t_clean = t.replace('"', '')
        if not t_clean or t_clean in seen:
            continue
        seen.add(t_clean)
        if t_clean in original_words:
            clean_vars.append(f'"{t_clean}"^5')
        elif any(len(t_clean) != len(ow) for ow in original_words):
            clean_vars.append(f'"{t_clean}"^3')
        else:
            clean_vars.append(f'"{t_clean}"')
    wildcard = comp.get('wildcard')
    if wildcard in ('prefix', 'suffix'):
        star = ResponsaComponent(words=[], wildcard=wildcard)
        for form in _star_list(star, comp.get('wildcard_stems') or comp.get('original_words', [])):
            form = form.replace('"', '')
            if form and form not in seen:
                seen.add(form)
                clean_vars.append(f'"{form}"')
    for w in comp.get('original_words', []):
        for bv in _add_bracket_variants(w):
            if bv != w and bv not in seen:
                seen.add(bv)
                clean_vars.append(f'"{bv}"')
    flex_split_clauses = []
    if flex_spacing:
        for w in comp.get('original_words', []):
            if len(w) >= 3:
                for i in range(1, len(w)):
                    flex_split_clauses.append(f'("{w[:i]}" AND "{w[i:]}")')
    if not (clean_vars or flex_split_clauses):
        return ''
    return f'({" OR ".join(clean_vars + flex_split_clauses)})'


# Between the letters of a form in the tolerant lookup: brackets, and the Hebrew
# block's marks (its category-M code points, not its punctuation -- maqaf, paseq,
# sof pasuq and nun hafukha end a word for Exact). On content_search the quotes and
# U+0300-036F are folded out already; on an old index's content field they are not.
_T_CONTENT_SEARCH = r'[\[\]֑-ׇֽֿׁׂׅׄ]*'
_T_CONTENT = (r'[\[\]֑-ׇֽֿׁׂׅׄ'
              r'̀-ͯ"\'׳״‘’]*')
# The tokenizer's class (shared/search_tokenizer.py): a form with any other
# character is never one token, and its scored clause covers it.
_TOKEN_RE = re.compile("[\\w֐-׿̀-ͯ'\"\\[\\]]+")
_RUST_META = set('\\.+*?()|[]{}^$#&-~')
# Pages the pattern alone reaches get this constant; the others keep BM25 (§5.1).
PATTERN_SCORE = 0.5
# The first chunk tried; one that exceeds tantivy-fst's 1,000 DFA states is halved
# (one word's 600 #X# forms fit; 125 spelling variants do; §3.6.2).
_PATTERN_CHUNK = 600


def _rust_escape(ch):
    return '\\' + ch if ch in _RUST_META else ch


def tolerant_patterns(forms, tolerance):
    """The tolerant lookup patterns of *forms* (already folded), each one alternation."""
    return tolerance + '(?:' + '|'.join(tolerance.join(_rust_escape(c) for c in f) for f in forms) + ')' + tolerance


def tolerant_queries(forms, schema, field, tolerance):
    """``regex_query``s over *forms*, split until each fits tantivy-fst's size limit."""
    import tantivy
    from shared.text_normalize import strip_search_diacritics
    folded = [strip_search_diacritics(f) if field == 'content_search' else f for f in forms]
    folded = list(dict.fromkeys(f for f in folded if f and _TOKEN_RE.fullmatch(f)))
    out = []

    def build(chunk):
        if not chunk:
            return
        try:
            out.append(tantivy.Query.regex_query(schema, field, tolerant_patterns(chunk, tolerance)))
        except ValueError as exc:
            if 'size limit' not in str(exc) or len(chunk) == 1:
                if len(chunk) == 1:
                    return      # its scored term still finds the bare form
                raise
            mid = len(chunk) // 2
            build(chunk[:mid])
            build(chunk[mid:])

    for i in range(0, len(folded), _PATTERN_CHUNK):
        build(folded[i:i + _PATTERN_CHUNK])
    return out


def retrieval_query(plans, index, field='content', has_content_search=False, flex_spacing=False):
    """The candidates for *plans* on *index*: per word, its scored clause on *field*,
    ORed (on the content field only -- position fields hold whitespace tokens) with
    the tolerant pattern of its forms on content_search, or on content when the
    index has no content_search field; the words ANDed."""
    import tantivy
    should, must = tantivy.Occur.Should, tantivy.Occur.Must
    per_word = []
    for plan in plans:
        clause = component_query_string(plan.tantivy_dict(), flex_spacing)
        if not clause:
            continue
        alternatives = [(should, index.parse_query(clause, [field]))]
        if field == 'content':
            pfield = 'content_search' if has_content_search else 'content'
            tolerance = _T_CONTENT_SEARCH if has_content_search else _T_CONTENT
            for q in tolerant_queries(plan.terms(), index.schema, pfield, tolerance):
                alternatives.append((should, tantivy.Query.const_score_query(q, PATTERN_SCORE)))
        per_word.append((must, tantivy.Query.boolean_query(alternatives)))
    if not per_word:
        return None
    return tantivy.Query.boolean_query(per_word)

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

**Stars** (Phase 3, plan §4 and §7 "Phase 3, settled before building"): ``*`` is any
letters of any script inside one word, zero included, anywhere in the word and as often as
typed (``*נים``, ``כ*נים``, ``*כ*ה*נ*ים``). A star word is a whole word bounded at both ends:
the star takes the letters (and the looked-past marks, quotes and brackets) up to the
word's own edge, never a digit, ``_``, space or punctuation. Its letters between stars are
*runs*, each expanded on its own (``StarForm``); the check and the lookup write one
alternation per run, so a word's size is the sum of its runs' forms. Retrieval per form:
a forward pattern when the word begins with a letter, the reversed-words field when it
begins with a star and ends with a letter (format 2), else the slow ``.*`` pattern. Only a
word made of stars alone is refused (``refuse_unsupported_stars``, decision 4).
"""

import functools
import re
import sys
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
from shared.text_normalize import strip_search_diacritics


# ==============================================================================
#  Refused star shapes (decision 4) and short star words
# ==============================================================================

STAR_REFUSAL_CODE = 'responsa_unsupported_star'
STAR_REFUSAL_TEXT = (
    "A * alone, or a word made only of *s, matches every word. "
    "For any one word, use a gap such as [1]."
)
STAR_PUNCTUATION_TEXT = (
    "A word with * holds letters only: a * never reaches past a space or punctuation. "
    "Write the punctuation as a separate word, or leave it out."
)
SHORT_STAR_TEXT = (
    "A * word with only one or two letters of its own matches very many words: "
    "the list may be cut short (N+)."
)


class ResponsaQueryRefused(ValueError):
    """A query the engine does not search: refused with a message, never zero rows.

    A ValueError, so every surface that already shows a query error (the desktop
    search thread, the website, the API's 400) shows this one; ``code`` names it for
    the API and MCP (``shared/api_errors.py``)."""

    code = STAR_REFUSAL_CODE

    def __init__(self, message=None, code=None):
        super().__init__(message or _tr(STAR_REFUSAL_TEXT))
        if code:
            self.code = code


def _one_star(form):
    """``**`` is ``*``: several stars in a row take the same letters as one."""
    return re.sub(r'\*+', '*', form)


def is_bare_star(form):
    """A word made of stars alone (``*``, ``**``): it matches every word."""
    return '*' in form and not form.replace('*', '')


def _typed_or_raw(comp):
    """Every form of *comp* as typed, for the refusals: its typed forms, or -- an
    inline alternation too large to spell out, which the guard refuses anyway -- its
    words."""
    try:
        return typed_forms(comp, Config.MAX_EXPANDED_TERMS)
    except _OverLimit:
        return list(comp.words)


def refuse_unsupported_stars(components):
    """Raise ResponsaQueryRefused for a star shape the engine does not search: a word
    of stars alone, positive or negated, alone or in an OR group (decision 4: it
    matches every word; the message suggests a gap such as ``[1]``), and a star word
    whose letters hold punctuation (a star never reaches past it, and the lookup reads
    one index token per word). Every other position and number of stars is searched
    (Phase 3; Phase 1 refused all but ``*X``, ``X*`` and ``*X*`` here)."""
    for comp in components:
        for form in _typed_or_raw(comp):
            if is_bare_star(form):
                raise ResponsaQueryRefused()
            if '*' in form and any(run and not _TOKEN_RE.fullmatch(strip_search_diacritics(run) or 'x')
                                   for run in form.split('*')):
                raise ResponsaQueryRefused(_tr(STAR_PUNCTUATION_TEXT))


def short_star_warning(plans):
    """Decision 4: a star word with one or two letters of its own runs, with a warning
    that its list may be cut ("N+"); None when no word is that short."""
    if any(sf.own_letters <= 2 for p in plans for sf in p.starred):
        return _tr(SHORT_STAR_TEXT)
    return None


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


def typed_forms(comp: ResponsaComponent, limit=None) -> List[str]:
    """The component's forms as typed, with a ``*`` wherever the user put one: its words,
    an OR group's words, an inline alternation spelt out; ``*word`` / ``word*`` (the
    parser's prefix and suffix wildcards, an OR group's trailing star) and character
    patterns as they were typed. Stars in a row are one."""
    if comp.wildcard == 'pattern' and comp.wildcard_pattern:
        base = [comp.wildcard_pattern]
    else:
        base = literal_forms(comp, limit)
        if comp.wildcard == 'prefix':
            base = ['*' + f for f in base]
        elif comp.wildcard == 'suffix':
            base = [f + '*' for f in base]
    return list(dict.fromkeys(_one_star(f) for f in base if f))


@dataclass(frozen=True)
class StarForm:
    """One typed star word, expanded: per run of letters between its stars (in order;
    ``('',)`` for the empty run before a leading or after a trailing star), that run's
    forms. Its forms are every combination of one form per run, stars between; the
    check and the lookup write them as one alternation per run (plan §7, Phase 3 point
    2), so its cost is the sum of the runs' forms, not the product."""
    typed: str
    runs: tuple

    @property
    def leading(self):
        return self.typed.startswith('*')

    @property
    def trailing(self):
        return self.typed.endswith('*')

    @property
    def own_letters(self):
        """The characters typed besides the stars (decision 4's "letters of its own")."""
        return len(self.typed.replace('*', ''))

    @property
    def cost(self):
        """Stems in its pattern alternations: what the guard counts (plan §5.3)."""
        return sum(len(r) for r in self.runs if r != ('',))

    @property
    def max_chars(self):
        """Its longest form's characters, stars aside (``_bounded_forms``)."""
        return sum(max((len(f) for f in r), default=0) for r in self.runs)

    def __str__(self):
        return self.typed


def _expand_star_form(comp, typed, ja_on, variants_on, variant_mode, var_mgr, room):
    """*typed* (a form with stars) expanded run by run, in the order finite forms are
    (plan §7, Phase 3 point 2): ``%`` and spelling variants on every run, ``#``'s
    prefixes and Judeo-Arabic on the first run when the word begins with a letter, its
    suffixes on the last when it ends with one -- a leading star already takes every
    prefix, a trailing one every suffix. Raises _OverLimit past *room* forms."""
    segments = typed.split('*')
    runs = []
    used = 0
    for i, seg in enumerate(segments):
        if not seg:
            runs.append(('',))
            continue
        forms = [seg]

        def step(fn, forms):
            out = {}
            for w in forms:
                for f in fn(w):
                    out[f] = None
                if used + len(out) > room:
                    raise _OverLimit()
            return list(out)

        if comp.plene_defective:
            forms = step(expand_plene_defective, forms)
        if i == 0 and comp.grammatical_prefixes:
            forms = step(expand_grammatical_prefixes, forms)
        if i == len(segments) - 1 and comp.grammatical_suffixes:
            forms = step(expand_grammatical_suffixes, forms)
        if i == 0 and ja_on:
            forms = step(expand_judeo_arabic, forms)
        if variants_on and var_mgr is not None:
            def spellings(w):
                try:
                    return _variants(var_mgr, w, variant_mode, 200)
                except MemoryError:
                    raise
                except Exception:
                    return [w]   # Variant expansion failed for this run; use it as typed
            forms = step(spellings, forms)
        used += len(forms)
        runs.append(tuple(f for f in forms if f) or ('',))
    return StarForm(typed=typed, runs=tuple(runs))


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
    """The component's forms: its words, then plene/defective (``%``), the grammatical
    prefixes and suffixes (``#``), Judeo-Arabic, spelling variants -- the order the main
    path always used, now the one order for retrieval and check. A form with a star
    becomes a StarForm, expanded run by run (``_expand_star_form``); the others stay
    strings.

    Every step stops (_OverLimit) as soon as the forms pass what is left of the
    guard's limit (*budget*; without one, ``Config.MAX_EXPANDED_TERMS``): each step
    multiplies them -- ``#X#`` by about 600 -- so ``#א(ב/ג)...#`` with 12 groups
    (4,096 forms) built some 2.5 million before they were counted (Codex review of
    PR #432)."""
    if budget is not None and budget.limit is not None:
        room = budget.limit - budget.used
    else:
        room = Config.MAX_EXPANDED_TERMS
    typed = typed_forms(comp, room)
    forms = [f for f in typed if '*' not in f]
    starred = [f for f in typed if '*' in f]

    def step(fn):
        nonlocal forms
        out = {}
        for w in forms:
            for f in fn(w):
                out[f] = None
            if len(out) > room:
                raise _OverLimit()
        forms = list(out)

    if forms:
        if comp.plene_defective:
            step(expand_plene_defective)
        if comp.grammatical_prefixes:
            step(expand_grammatical_prefixes)
        if comp.grammatical_suffixes:
            step(expand_grammatical_suffixes)
        if ja_on:
            step(expand_judeo_arabic)
    if forms and variants_on and var_mgr is not None:
        out = {}
        for w in forms:
            try:
                found = _variants(var_mgr, w, variant_mode, 200)
            except MemoryError:
                raise
            except Exception:
                found = [w]   # Variant expansion failed for this word; use original
            for f in found:
                out[f] = None
            if len(out) > room:
                raise _OverLimit()
        forms = list(out)
    stars = []
    for typed_star in starred:
        sf = _expand_star_form(comp, typed_star, ja_on, variants_on, variant_mode, var_mgr,
                               room - len(forms) - term_cost(stars))
        stars.append(sf)
    result = forms + stars
    if budget is not None:
        budget.take(term_cost(result))
    return result


def term_cost(forms):
    """The terms *forms* put in the lookup: one per finite form, a StarForm's runs' forms
    (plan §5.3: terms in a term set, stems in one pattern alternation)."""
    return sum(f.cost if isinstance(f, StarForm) else 1 for f in forms)


def retrieval_terms(comp, stems):
    """Every form the lookup asks for (finite forms and StarForms), once each. Phase 1
    added a star word's fixed prefix or suffix list here; a star is a pattern now."""
    return list(dict.fromkeys(stems))


# ==============================================================================
#  The guard (plan §5.3): real cost, one cascade, every path
# ==============================================================================

def expand_under_guard(components, variants_on, ja_on, variant_mode, var_mgr):
    """Expand *components* (positive ones; negated words are letters to exclude) under
    ``Config.MAX_EXPANDED_TERMS``, the real number of terms the lookup holds -- forms,
    and a star word's runs' forms (``term_cost``) -- counted as they are made, not
    estimated.

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
PATTERN_REFUSAL_TEXT = (
    "This query is too long for Responsa search. Use fewer or shorter words "
    "(Composition Search takes a longer passage)."
)


GUARD_REFUSAL_TEXT = (
    "Query exceeds the limit of {limit} expanded terms (estimated {count} terms). "
    "Please simplify your query by using fewer OR-group alternatives or "
    "removing the # (grammatical prefixes) modifier from some terms."
)


def guard_refusal(limit=None):
    """The query is over the guard's limit after every step down: refused with its
    message and its code -- a 400 on the API, not the 500 a plain ValueError became
    there (Codex review of PR #432). Still a ValueError for the apps' handlers."""
    limit = Config.MAX_EXPANDED_TERMS if limit is None else limit
    return ResponsaQueryRefused(
        _tr(GUARD_REFUSAL_TEXT).format(limit=limit, count=f'{limit}+'),
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


@functools.lru_cache(maxsize=1)
def star_class():
    """What a star takes (plan §4): letters of any script and the looked-past
    characters (marks, in-word quotes, brackets), over all of Unicode (the planes past
    the first too: Codex review of PR #443), as one explicit class -- never a digit,
    ``_``, space or punctuation, which Exact's predicate would let a word go on through
    or end it at. The same in ``re`` and the ``regex`` module (their ``\\w`` differ).
    Built once, in about 0.1 s."""
    points = set()
    for cp in range(sys.maxunicode + 1):
        cat = unicodedata.category(chr(cp))
        if cat[0] in ('L', 'M'):
            points.add(cp)
    points |= {ord(c) for c in _IN_WORD_QUOTES}
    return _class(sorted(points))


# A final letter and its medial form: a letter right before a star takes either (plan §7,
# Phase 3 point 3): שלום* finds שלומות, שלומ* finds שלום.
_FINAL_MEDIAL = dict(list(_SOFIT_TO_NORMAL.items()) + [(v, k) for k, v in _SOFIT_TO_NORMAL.items()])


def _before_star(forms):
    """*forms* of a run a star follows, each also with its last letter's other form."""
    out = list(forms)
    for f in forms:
        if f and f[-1] in _FINAL_MEDIAL:
            out.append(f[:-1] + _FINAL_MEDIAL[f[-1]])
    return list(dict.fromkeys(out))


def star_body(sf, inserter, anchor=True):
    """The check's pattern of a StarForm: one alternation per run (mark-tolerant,
    length-grouped prefix trees, as finite forms), the star class between. A word that
    begins with a star starts where no letter or looked-past character stands before
    it (*anchor*, where the word's start is bounded): the star takes them, so the
    predicate's word start is the only start, and the regex does not retry inside a
    word at every letter (``(?<!...)``, one character)."""
    from shared.search_engine import _variant_alternation
    star = star_class() + '*'
    parts = []
    last = len(sf.runs) - 1
    if sf.leading and anchor:
        cont, past = edge_classes()
        parts.append('(?<![' + cont[1:-1] + past[1:-1] + '])')
    for i, run in enumerate(sf.runs):
        if i:
            parts.append(star)
        forms = [f for f in run if f]
        if not forms:
            continue
        if i < last:
            forms = _before_star(forms)
        forms = sorted(set(forms), key=len, reverse=True)
        parts.append('(?:' + _variant_alternation(forms, inserter=inserter) + ')')
    return ''.join(parts)


# ==============================================================================
#  The compiled query
# ==============================================================================

@dataclass
class WordPlan:
    """One positive component, compiled. *stems*: its forms -- strings for the finite
    ones, StarForms for the words with a star."""
    component: ResponsaComponent
    stems: List
    flex_patterns: List[str] = field(default_factory=list)

    @property
    def finite(self):
        return [f for f in self.stems if not isinstance(f, StarForm)]

    @property
    def starred(self):
        return [f for f in self.stems if isinstance(f, StarForm)]

    @property
    def star(self) -> bool:
        """Whether a form of the word holds a star."""
        return any(isinstance(f, StarForm) for f in self.stems)

    @property
    def bounds(self):
        """(start bounded, end bounded): both, always. A star takes the letters up to
        the word's own edge, so ``*נים`` is a word that ends in נים and both its ends are
        checked like any word's (plan §7, Phase 3 point 1)."""
        return True, True

    @property
    def original_words(self):
        """The words typed without a star: the scored clause boosts them."""
        if self.component.wildcard in ('prefix', 'suffix', 'pattern'):
            return []
        return [w for w in self.component.words if '*' not in w]

    def terms(self):
        return retrieval_terms(self.component, self.stems)

    def first_chars(self, flex_spacing=False):
        """The characters the word's own pattern can begin with (its forms'; with flex
        spacing, its typed words' too), or None when it can begin with anything."""
        chars = {f[0] for f in self.finite if f}
        for sf in self.starred:
            if sf.leading:
                return None
            first = sf.runs[0]
            forms = _before_star(first) if len(sf.runs) > 1 else list(first)
            chars |= {f[0] for f in forms if f}
        if flex_spacing:
            chars |= {w[0] for w in self.original_words if w}
        return chars or None

    def tantivy_dict(self):
        """The component dict build_tantivy_query / component_query_string read: the
        finite forms (a star word is looked up by its pattern, ``star_queries``)."""
        finite = self.finite
        return {
            'tantivy_terms': finite,
            'regex_terms': finite,
            'original_words': self.original_words,
            'wildcard_stems': finite,
            'wildcard': None,
            'wildcard_pattern': None,
            'flex_patterns': self.flex_patterns,
            'inline_pattern': None,
        }

    def body(self, flex_spacing, inserter=None, anchor=True):
        """The word's own pattern, without edges. *anchor*: its start is a word's start
        (``star_body``)."""
        from shared.search_engine import _variant_alternation, MARK_TOLERANT_INSERTER
        inserter = inserter or MARK_TOLERANT_INSERTER
        alternatives = []
        forms = sorted(set(self.finite), key=len, reverse=True)
        if forms:
            alternatives.append(_variant_alternation(forms, inserter=inserter))
        alternatives.extend(star_body(sf, inserter, anchor) for sf in self.starred)
        if flex_spacing:
            alternatives.extend(fp for fp in self.flex_patterns if fp not in alternatives)
        return '(?:' + '|'.join(alternatives) + ')'


def _dict_star_forms(d):
    """The StarForms of a legacy component dict (``wildcard`` prefix / suffix / pattern
    and its ``regex_terms``)."""
    wildcard, terms = d.get('wildcard'), [t for t in (d.get('regex_terms') or []) if t]
    if wildcard == 'prefix' and terms:
        return [StarForm(typed='*' + terms[0], runs=(('',), tuple(terms)))]
    if wildcard == 'suffix' and terms:
        return [StarForm(typed=terms[0] + '*', runs=(tuple(terms), ('',)))]
    if wildcard == 'pattern' and d.get('wildcard_pattern'):
        typed = _one_star(d['wildcard_pattern'])
        return [StarForm(typed=typed, runs=tuple((seg,) if seg else ('',) for seg in typed.split('*')))]
    return []


def plans_from_dicts(component_dicts):
    """WordPlans from the component dicts build_regex_pattern has always taken
    (``regex_terms``, ``wildcard``, ``inline_pattern``, ``flex_patterns`` ...). A
    component with nothing to match is left out, as before."""
    plans = []
    for d in component_dicts:
        comp = ResponsaComponent(words=list(d.get('original_words') or []),
                                 inline_pattern=d.get('inline_pattern'))
        if comp.inline_pattern:
            try:
                stems = _inline_forms(comp.inline_pattern)
            except _OverLimit:
                raise guard_refusal() from None
        elif d.get('wildcard') in ('prefix', 'suffix', 'pattern'):
            stems = _dict_star_forms(d)
        else:
            stems = [t for t in (d.get('regex_terms') or []) if t]
        if not stems and not d.get('flex_patterns'):
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
        """The finite forms searched (the results line's "expanded terms")."""
        return sum(len(p.finite) for p in self.plans)

    @property
    def pattern_count(self):
        """The star words searched, each one pattern (the results line's "patterns",
        plan §5.7)."""
        return sum(len(p.starred) for p in self.plans)

    @property
    def has_star(self):
        return any(p.star for p in self.plans)

    @property
    def all_star(self):
        """Every word holds a star and none a finite form: the rows have no score to
        order by (decision 2)."""
        return bool(self.plans) and all(p.star and not p.finite for p in self.plans)

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

    def retrieval(self, index, field='content', has_content_search=False, flex_spacing=False,
                  bidirectional=False, within_document=False, reversed_field=None):
        """The lookup on *index* (its own schema; plan §5.1, Codex 2.4): per word its
        scored terms ORed with a bracket- and mark-tolerant pattern of its forms, the
        words ANDed; a position *field* also requires its edge word there
        (retrieval_query). *reversed_field*: the index's reversed-words field, when it
        has one (a leading star is looked up there)."""
        return retrieval_query(self.plans, index, field=field,
                               has_content_search=has_content_search, flex_spacing=flex_spacing,
                               bidirectional=bidirectional, within_document=within_document,
                               reversed_field=reversed_field)


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


def _bounded_forms(plans):
    """Refuse a query with a form longer than ``Config.MAX_RESPONSA_FORM_CHARS``:
    its tolerant lookup cannot be built (tantivy-fst's size limit), and without it
    the bare form's scored term misses its bracketed spellings, which the check
    accepts -- zero rows, not the page (Codex review of PR #432). Checked when the
    query is compiled, before any handler that would turn an error into no rows."""
    if any((t.max_chars if isinstance(t, StarForm) else len(t)) > Config.MAX_RESPONSA_FORM_CHARS
           for p in plans for t in p.terms()):
        raise ResponsaQueryRefused(_tr(PATTERN_REFUSAL_TEXT), code=GUARD_REFUSAL_CODE)


def _bounded(*patterns):
    """Refuse a query whose patterns are too large to compile in good time
    (``Config.MAX_RESPONSA_PATTERN_CHARS``): compiling cannot be interrupted, and 500
    one-letter words fit the query and term limits yet made 12 MB of pattern (Codex
    review of PR #432). Checked after the strings are built (milliseconds), before
    anything compiles them."""
    if max(len(p or '') for p in patterns) > Config.MAX_RESPONSA_PATTERN_CHARS:
        raise ResponsaQueryRefused(_tr(PATTERN_REFUSAL_TEXT), code=GUARD_REFUSAL_CODE)


def _names(prefix, n):
    return [f'{prefix}{i}' for i in range(n)]


def _word_piece(plan, name, flex, bound_start, bound_end, strict, inserter=None, exact=False,
                anchor=None):
    # *anchor*: the word begins a word (default: *bound_start*); the check's first word
    # is bounded by ``accept`` without a lookbehind of its own.
    body = plan.body(flex, inserter=inserter, anchor=bound_start if anchor is None else anchor)
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
        out.append(_word_piece(plan, name, flex, bs and (strict or i > 0), be, strict, inserter, exact,
                               anchor=bs))
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
        _bounded(exact)
        return exact, None, groups
    check, groups = one(strict=False, named=True)
    highlight, _ = one(strict=True, named=False)
    _bounded(check, highlight)
    return check, highlight, groups


def compile_components(components, responsa_options, var_mgr, max_gap, per_pair_gaps=None,
                       query_has_brackets=False):
    """Compile parsed *components* (one query, no line groups) into a CompiledResponsa.

    Refuses a bare star (decision 4), splits off the negated words (letters to
    exclude), and expands the rest under the guard. A star word is searched as typed:
    ``*X*`` is no longer rewritten as ``#X#`` (Phase 1 did, with a warning)."""
    from shared.search_engine import _make_flex_spacing_pattern
    opts = responsa_options or {}
    refuse_unsupported_stars(components)
    warnings = []
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
        plan = WordPlan(component=comp, stems=forms)
        # A star never reaches past a space (§4), so a star word has no split form.
        if flex:
            plan.flex_patterns = [_make_flex_spacing_pattern(w) for w in plan.original_words]
        plans.append(plan)
    short = short_star_warning(plans)
    if short:
        warnings.append(short)
    check = highlight = ''
    groups = []
    _bounded_forms(plans)
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


def _line_word_separator(gap, flex=False):
    """Between two words of one line (never crosses a line break): whitespace, with
    up to *gap* words between (CR HIGH-6, [N] word gaps in line mode); with flex
    spacing and no gap, none at all (שלוםעולם, as _separator)."""
    if not gap:
        return r'[^\S\n]*' if flex else r'[^\S\n]+'
    return r'(?:[^\S\n]+\S+){0,' + str(int(gap)) + r'}[^\S\n]+'


def _line_flex_pattern(word):
    """_make_flex_spacing_pattern for a line query: spaces between the letters, never
    a line break -- a word split over two lines is not on one line."""
    return r'[^\S\n]*'.join(re.escape(ch) for ch in word)


def compile_line_groups(line_groups, line_gaps, responsa_options, var_mgr, query_has_brackets=False):
    """Compile a line-break query (``_parse_line_break_query``'s groups) through the
    same steps as compile_components: a bare star refused, star words searched as typed,
    negated words split off as letters to exclude, one guard over every word, and
    every word bounded by Exact's predicate -- each group's edges included, while a
    ``|`` anchor still means the line's start or end (§4). Its pattern is MULTILINE.

    *line_groups* are changed in place (negated components leave their group). A
    group left with no word drops out with the line gap before it."""
    opts = responsa_options or {}
    every = [c for g in line_groups for c in g.components]
    refuse_unsupported_stars(every)
    warnings = []
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
    # Flex spacing holds in a line query too (Codex review of PR #432).
    flex = bool(opts.get('flex_spacing', False))
    plans = []
    for c, f in zip(positive, stems):
        plan = WordPlan(component=c, stems=f)
        if flex:
            plan.flex_patterns = [_line_flex_pattern(w) for w in plan.original_words]
        plans.append(plan)
    short = short_star_warning(plans)
    if short:
        warnings.append(short)
    _bounded_forms(plans)
    by_group, k = [], 0
    for group in kept_groups:
        by_group.append(plans[k:k + len(group.components)])
        k += len(group.components)
    check, highlight, groups = build_line_patterns(kept_groups, by_group, kept_gaps, query_has_brackets,
                                                   flex_spacing=flex)
    exact = build_line_patterns(kept_groups, by_group, kept_gaps, query_has_brackets, exact_starts=True,
                                flex_spacing=flex)[0]
    return CompiledResponsa(plans=plans, negated_words=negated,
                            warning='; '.join(warnings) if warnings else None,
                            options=state, check_pattern=check, highlight_pattern=highlight,
                            groups=groups, flex_spacing=flex, flags=re.IGNORECASE | re.MULTILINE,
                            exact_pattern=exact)


# The looked-past characters a line's edge word commonly carries: brackets, quotes,
# U+0300-036F and the Hebrew block's marks (build_line_patterns).
_LINE_EDGE_PAST = ("[\\[\\]\"'\u05F3\u05F4\u2018\u2019\u0300-\u036F"
                   "\u0591-\u05BD\u05BF\u05C1\u05C2\u05C4\u05C5\u05C7]")


def build_line_patterns(line_groups, plans_by_group, line_gaps, query_has_brackets=False,
                        exact_starts=False, flex_spacing=False):
    """(check, highlight, groups) of a line-break query: each group one line pattern,
    its words bounded and named, joined by line breaks with the [|N] gaps.
    *exact_starts*: the check's exact pattern instead, and no highlight (as
    build_patterns). *flex_spacing*: a group's words as _sequence joins them -- only
    its outer edges bounded, spaces optional within and between them."""
    from shared.search_engine import _FUZZY_INSERTER
    hl_inserter = None if query_has_brackets else _FUZZY_INSERTER
    kept_groups, by_group, kept_gaps = line_groups, plans_by_group, line_gaps
    past = edge_classes()[1]

    def one(strict, named, exact=False):
        groups = []
        line_patterns = []
        for gi, (group, gplans) in enumerate(zip(kept_groups, by_group)):
            word_pat = ''
            last = len(gplans) - 1
            for ci, plan in enumerate(gplans):
                if ci:
                    word_pat += _line_word_separator(
                        group.word_gaps[ci - 1] if ci - 1 < len(group.word_gaps) else None, flex_spacing)
                bs, be = plan.bounds
                if flex_spacing:
                    bs = bs and ci == 0
                    be = be and ci == last
                name = f'rl{gi}_{ci}' if named else None
                word_pat += _word_piece(plan, name, flex_spacing, bs, be, strict,
                                        hl_inserter if strict else None, exact)
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
        _bounded(exact)
        return exact, None, groups
    check, groups = one(strict=False, named=True)
    # The viewers compile a row's pattern without flags: MULTILINE goes with it.
    highlight = '(?m:' + one(strict=True, named=False)[0] + ')'
    _bounded(check, highlight)
    return check, highlight, groups


class StarExclusion(str):
    """A negated word with a star inside (``-כ*נים``): a page is excluded where letters
    matching it stand anywhere, inside a word or not (decision 7's letters-anywhere
    rule, the star as letters; plan §7, Phase 3 point 5). A str, so the lists of
    excluded words it travels in still print and join. *runs*: each run's forms (with
    ``%``, its plene/defective spellings), the typed run alone by default."""

    def __new__(cls, value, runs=None):
        obj = super().__new__(cls, value)
        obj.runs = tuple(runs) if runs is not None else tuple((seg,) for seg in value.split('*'))
        return obj

    def found_in(self, text):
        rx = getattr(self, '_rx', None)
        if rx is None:
            pieces = ['(?:' + '|'.join(re.escape(f) for f in sorted(forms, key=len, reverse=True)) + ')'
                      for forms in self.runs]
            rx = self._rx = re.compile((star_class() + '*').join(pieces), re.IGNORECASE)
        return rx.search(text) is not None


def negated_words(components):
    """The letters a ``-word`` excludes: the word, and its plene/defective spellings
    with ``%`` -- as the main path always took them. A star at a word's edge adds
    nothing to letters found anywhere (``-*כהנים`` is ``-כהנים``); a star inside it
    makes a StarExclusion."""
    out = []
    for comp in components:
        if not comp.negated:
            continue
        for w in _typed_or_raw(comp):
            core = w.strip('*')
            if not core:
                continue
            if '*' in core:
                # %'s spellings on every run between the stars, as for a positive star
                # word (point 2; Codex review of PR #443).
                runs = [tuple(dict.fromkeys([seg] + (expand_plene_defective(seg)
                                                     if comp.plene_defective and seg else [])))
                        for seg in core.split('*')]
                out.append(StarExclusion(core, runs))
                continue
            out.append(core)
            if comp.plene_defective:
                out.extend(expand_plene_defective(core))
    return out


# ==============================================================================
#  Retrieval
# ==============================================================================

def component_query_string(comp, flex_spacing=False):
    """One component's scored clause, as the Responsa query string always had it: the
    typed word boosted x5, other-length forms x3, the typed word's edge-bracket forms,
    and with flex spacing its split pairs. '' when it has none. A star word's forms
    are not here: its pattern finds them (``star_queries``; Phase 1 listed 24 prefixes
    or 25 suffixes)."""
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


def tolerant_patterns(forms, tolerance, edges=None):
    """The tolerant lookup patterns of *forms* (already folded), each one alternation;
    *edges* (lead, trail) before and after it in place of *tolerance*."""
    lead, trail = (tolerance, tolerance) if edges is None else edges
    return lead + '(?:' + '|'.join(tolerance.join(_rust_escape(c) for c in f) for f in forms) + ')' + trail


def tolerant_queries(forms, schema, field, tolerance, edges=None):
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
            out.append(tantivy.Query.regex_query(schema, field, tolerant_patterns(chunk, tolerance, edges)))
        except ValueError as exc:
            if 'size limit' not in str(exc) or len(chunk) == 1:
                if len(chunk) == 1:
                    # Past the size limit alone: compile refuses such a form
                    # (_bounded_forms), so only a longer cap would reach this.
                    return
                raise
            mid = len(chunk) // 2
            build(chunk[:mid])
            build(chunk[mid:])

    for i in range(0, len(folded), _PATTERN_CHUNK):
        build(folded[i:i + _PATTERN_CHUNK])
    return out


def star_pattern(sf, tolerance, fold=True, reverse=False):
    """The ``regex_query`` pattern of StarForm *sf* over whole tokens: each run's forms
    (a run a star follows also with its last letter's other form) with *tolerance*
    between letters, ``.*`` for a star -- wider than the check, which retrieval may be,
    never narrower (§4) -- and *tolerance* at an end that is a letter. *fold*: folded as
    ``content_search`` is. *reverse*: on the reversed-words field, every form written
    backwards and the runs in reverse order, so a leading star becomes a trailing one
    and the term-dictionary walk is pruned by the word's last letters (§6.1 point 5).
    Case-insensitive (``(?i)``), as the check is: the index keeps a token's case
    (Codex review of PR #442; Hebrew has none, so its patterns are unchanged)."""
    last = len(sf.runs) - 1
    pieces = []
    for i, run in enumerate(sf.runs):
        forms = [f for f in run if f]
        if forms and i < last:
            forms = _before_star(forms)
        if fold:
            forms = [strip_search_diacritics(f) for f in forms]
        pieces.append(list(dict.fromkeys(f for f in forms if f)))
    if reverse:
        pieces = [[f[::-1] for f in p] for p in reversed(pieces)]
    out = []
    for i, forms in enumerate(pieces):
        if i:
            out.append('.*')
        if forms:
            out.append('(?:' + '|'.join(tolerance.join(_rust_escape(c) for c in f) for f in forms) + ')')
    return '(?i)' + (tolerance if pieces[0] else '') + ''.join(out) + (tolerance if pieces[-1] else '')


def _split_largest_run(sf):
    """*sf* as two StarForms whose runs' forms together are its own (its largest run
    halved), or None when every run has one form."""
    i = max(range(len(sf.runs)), key=lambda k: len(sf.runs[k]))
    run = sf.runs[i]
    if len(run) < 2:
        return None
    mid = len(run) // 2
    return [StarForm(sf.typed, sf.runs[:i] + (half,) + sf.runs[i + 1:]) for half in (run[:mid], run[mid:])]


def star_route(sf, reversed_field):
    """How StarForm *sf* is looked up (plan §7, Phase 3 point 6): 'forward' when it
    begins with a letter (the walk is pruned by it), 'reversed' when it begins with a
    star and ends with a letter and the index has the reversed-words field, else 'scan'
    (``.*`` first: the whole term dictionary; both ends starred, or no reversed field)."""
    if not sf.leading:
        return 'forward'
    if not sf.trailing and reversed_field:
        return 'reversed'
    return 'scan'


def star_queries(star_forms, schema, field, tolerance, reversed_field=None):
    """``regex_query``s that find every document holding a token one of *star_forms*
    can be, each within tantivy-fst's size limit: a form whose pattern is too large is
    split by halving its largest run (decision 5: several queries, ORed by the caller).
    *field*: content_search (or content on an index without it); *reversed_field*: the
    index's reversed-words field (built from content_search), or None."""
    import tantivy
    out = []

    def build(sf):
        route = star_route(sf, reversed_field)
        if route == 'reversed':
            on_field, pattern = reversed_field, star_pattern(sf, _T_CONTENT_SEARCH, True, reverse=True)
        else:
            on_field = field
            pattern = star_pattern(sf, tolerance, fold=field == 'content_search')
        try:
            out.append(tantivy.Query.regex_query(schema, on_field, pattern))
        except ValueError as exc:
            halves = _split_largest_run(sf) if 'size limit' in str(exc) else None
            if halves is None:
                # One form per run and still past the limit: retrieval would be
                # narrower than the check, so the query is refused, never silently cut.
                raise ResponsaQueryRefused(_tr(PATTERN_REFUSAL_TEXT), code=GUARD_REFUSAL_CODE) from exc
            for half in halves:
                build(half)

    for sf in star_forms:
        pending = [sf]
        while pending:                 # a run past _PATTERN_CHUNK forms is split first
            cur = pending.pop()
            if max(len(r) for r in cur.runs) > _PATTERN_CHUNK:
                pending.extend(_split_largest_run(cur))
            else:
                build(cur)
    return out


# The position fields whose edge word is a query's first word (the others: its last).
_FIRST_WORD_FIELDS = ('content_head', 'line_starts')
# A position field's token around its edge word (Indexer._validate_position_match):
# at a start only brackets come before the word, at an end only brackets after it;
# on the other side, whatever the token goes on with. Anchored, the lookup walks
# only the tokens that begin so (line_starts holds every line's first token twice,
# L3:word too: an unanchored .*word.* took 13-20 s there on the real index). The
# end side's lead is the punctuation a token opens with, not any text: אחר,שלום at
# a line's end is missed, as the position field always missed it.
_FIRST_WORD_EDGES = (r'[\[\]]*', '.*')
_LAST_WORD_EDGES = ('[\\[\\]()<>{}"\'״׳‘’.,:;!?\\-־׃׀/|*+=_#%$@·…]*', r'[\[\]]*')


def retrieval_query(plans, index, field='content', has_content_search=False, flex_spacing=False,
                    bidirectional=False, within_document=False, reversed_field=None):
    """The candidates for *plans* on *index*: per word, its scored clause on the
    content field, ORed with the tolerant pattern of its forms on content_search (or
    on content when the index has no content_search field), and its star forms'
    patterns (``star_queries``; *reversed_field*: the index's reversed-words field,
    where a leading star is looked up, or None); the words ANDed. Star patterns score
    a constant, so the finite words' scores order the rows (decision 2).

    A position search (*field* one of the position fields) also requires its edge
    word there: the first word at a start, the last at an end, either one when the
    order may be reversed. Only that word: the others need not be any line's first
    or last token (כהנים גדולים opening a line: 3 rows -> 39 on the real index). A
    position field holds raw whitespace tokens (שלום, and של[ו]ם), so its pattern
    takes the form with what may stand beside it there (_FIRST_WORD_EDGES); the
    position and the whole word are checked on each candidate (Codex review of PR
    #432). The content lookup alone would read every page with the words: כהנים at
    a page's end took 0.1 -> 15.7 s for the same 54 rows. A star word is not looked up
    on a position field: its pattern runs on the content field only."""
    import tantivy
    should, must = tantivy.Occur.Should, tantivy.Occur.Must

    def lookup(plan, on_field):
        clause = component_query_string(plan.tantivy_dict(), flex_spacing)
        alternatives = [(should, index.parse_query(clause, [on_field]))] if clause else []
        if on_field == 'content':
            pfield = 'content_search' if has_content_search else 'content'
            tolerance = _T_CONTENT_SEARCH if has_content_search else _T_CONTENT
            patterns = tolerant_queries(plan.finite, index.schema, pfield, tolerance)
            patterns += star_queries(plan.starred, index.schema, pfield, tolerance,
                                     reversed_field if has_content_search else None)
        elif not clause:
            return None
        else:
            patterns = tolerant_queries(
                plan.finite, index.schema, on_field, _T_CONTENT,
                _FIRST_WORD_EDGES if on_field in _FIRST_WORD_FIELDS else _LAST_WORD_EDGES)
        for q in patterns:
            alternatives.append((should, tantivy.Query.const_score_query(q, PATTERN_SCORE)))
        return tantivy.Query.boolean_query(alternatives) if alternatives else None

    per_word = [(must, q) for q in (lookup(p, 'content') for p in plans) if q is not None]
    if not per_word:
        return None
    if field != 'content' and not within_document:
        first = field in _FIRST_WORD_FIELDS
        edges = [plans[0] if first else plans[-1]]
        if bidirectional and len(plans) > 1:
            edges.append(plans[-1] if first else plans[0])
        edge_queries = [q for q in (lookup(p, field) for p in edges) if q is not None]
        if edge_queries and field in ('line_starts', 'line_ends'):
            # A line opened (or closed) by a lacuna, "] שלום", keeps only "]" there:
            # any bracket-only token lets the page through to the check.
            edge_queries.append(tantivy.Query.regex_query(index.schema, field, r'[\[\]]+'))
        if len(edge_queries) == 1:
            per_word.append((must, edge_queries[0]))
        elif edge_queries:
            per_word.append((must, tantivy.Query.boolean_query([(should, q) for q in edge_queries])))
    return tantivy.Query.boolean_query(per_word)

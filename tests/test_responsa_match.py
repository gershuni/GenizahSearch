"""The Responsa word compiler (shared/responsa_match.py), unit by unit: plan §8 gate 2.

Every expectation is written by hand. Each pattern is run on both regex runtimes the app
uses: stdlib ``re`` (the production workers) and the ``regex`` module (the desktop and the
Joins Lab adapter), through shared/search_regex.py as the engine compiles it.
"""
import re

import pytest
import regex

import shared.search_engine as se
from shared.responsa import parse_responsa_query, extract_per_pair_gaps, _parse_line_break_query
from shared.responsa_match import (
    ResponsaQueryRefused, compile_components, compile_line_groups, refuse_unsupported_stars,
    _inline_forms, expand_stems,
)
from shared.search_regex import compile as compile_search_regex

RUNTIMES = ['re', 'regex']


def _compiled(query, **opts):
    comps = parse_responsa_query(query)
    return compile_components(comps, dict({'responsa_mode': True}, **opts), None, 0,
                              extract_per_pair_gaps(query),
                              query_has_brackets=se._query_has_brackets(query))


def _rx(pattern, flags, runtime):
    if runtime == 're':
        return re.compile(pattern, flags)
    return regex.compile(pattern, (regex.I if flags & re.I else 0) | (regex.M if flags & re.M else 0) | regex.V0)


def accepted(query, text, runtime='re', **opts):
    """The text of the first match the row would use (membership), or None."""
    c = _compiled(query, **opts)
    rx = _rx(c.check_pattern, c.flags, runtime)
    m = rx.search(text)
    while m is not None and not c.accept(text, m):
        m = rx.search(text, m.start() + 1)
    return m.group(0) if m else None


def highlighted(query, text, runtime='re', **opts):
    """Every span the row's highlight pattern marks."""
    c = _compiled(query, **opts)
    rx = _rx(c.highlight_pattern, re.I, runtime)
    return [m.group(0) for m in rx.finditer(text)]


# --- The parser and the refusal (every star position) -----------------------------------

@pytest.mark.parametrize('query, wildcard, words', [
    ('*כהנים', 'prefix', ['כהנים']),
    ('כהנים*', 'suffix', ['כהנים']),
    ('*כהנים*', 'pattern', ['*כהנים*']),
    ('#כהנים', None, ['כהנים']),
    ('%#כהנים#', None, ['כהנים']),
    ('-כהנים', None, ['כהנים']),
    ('(כהן/לוי)', None, ['כהן', 'לוי']),
    ('(כהן/לוי)*', 'suffix', ['כהן', 'לוי']),
])
def test_the_parser_reads_every_supported_shape(query, wildcard, words):
    comp, = parse_responsa_query(query)
    assert comp.wildcard == wildcard
    assert comp.words == words
    refuse_unsupported_stars([comp])        # supported: no refusal


@pytest.mark.parametrize('query', ['כ*נים', '*', '**', '***', '*כ*נים', 'כ*נים*', '*כ*ה*נ*ים*',
                                   '(כ*ן/לוי)', 'א(ב/ג)*ד', '#כ*נים', '-כ*נים', '%כ*ן'])
def test_every_unsupported_star_shape_is_refused(query):
    with pytest.raises(ResponsaQueryRefused) as info:
        _compiled(query)
    assert info.value.code == 'responsa_unsupported_star'


def test_the_refusal_covers_line_groups():
    groups, gaps = _parse_line_break_query('|א | כ*ן')
    with pytest.raises(ResponsaQueryRefused):
        compile_line_groups(groups, gaps, {'responsa_mode': True}, None)


def test_both_end_stars_are_still_hash_both_sides():
    c = _compiled('*כהנ*')
    assert c.warning and c.plans[0].component.grammatical_prefixes and c.plans[0].component.grammatical_suffixes


# --- Expansion: one order, OR groups and inline alternation spelt out --------------------

def test_inline_alternation_is_every_choice():
    assert _inline_forms('אירו(ס/ש)ין') == ['אירוסין', 'אירושין']
    assert _inline_forms('א(ב/)ג(ד/ה)') == ['אבגד', 'אבגה', 'אגד', 'אגה']


def test_hash_applies_to_an_inline_alternation():
    comp, = parse_responsa_query('#אירו(ס/ש)ין')
    stems = expand_stems(comp, False, False, 'exact', None)
    assert {'אירוסין', 'והאירושין', 'לאירוסין'} <= set(stems)


def test_hash_applies_to_every_word_of_an_or_group():
    comp, = parse_responsa_query('#(כהן/לוי)')
    stems = set(expand_stems(comp, False, False, 'exact', None))
    assert {'הכהן', 'והלוי', 'כהן', 'לוי'} <= stems


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_a_short_form_that_prefixes_a_longer_one_is_matched(runtime):
    # #כהנ#: כהני (suffix י) begins כהנים (suffix ים). One prefix tree for both
    # lengths would end כהני only where no form goes on (Codex 6.1).
    assert accepted('#כהנ#', 'שם כהני העם', runtime) == 'כהני'
    assert accepted('#כהנ#', 'שם כהנים העם', runtime) == 'כהנים'
    assert accepted('#כהנ#', 'שם והכהניהם העם', runtime) == 'והכהניהם'


# --- Whole words: the first and last word, and inside -----------------------------------

@pytest.mark.parametrize('runtime', RUNTIMES)
@pytest.mark.parametrize('text, want', [
    ('כהנים גדול.', 'כהנים גדול'),
    ('כהנים גדולים', None),            # the last word goes on
    ('הכהנים גדול', None),             # the first word began before
    ('כהנים "גדול"', 'כהנים "גדול'),   # a quote is part of the word, the edge is past it
    ('כהנים גדול_ים', None),           # connector punctuation goes on a word
    ('כהנים גדול‿ים', None),      # a Pc character other than _
    ('כהנים גדול־ים', 'כהנים גדול'),  # maqaf ends a word for Exact
    ('כהנים גדול1', None),             # a decimal digit goes on a word
    ('כהנים גדול²', 'כהנים גדול'),     # a superscript digit is not one (category No)
])
def test_the_edge_words_are_whole_words(runtime, text, want):
    got = accepted('כהנים גדול', text, runtime)
    assert (got.rstrip('"') if got else got) == (want.rstrip('"') if want else want)


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_every_and_operand_is_bounded(runtime):
    assert accepted('ראובן AND שמעון', 'ראובן אמר לשמעון', runtime, within_document=True) is None
    assert accepted('ראובן AND שמעון', 'ראובני אמר שמעון', runtime, within_document=True) is None
    assert accepted('ראובן AND שמעון', 'שמעון אמר: ראובן!', runtime, within_document=True) == 'שמעון'


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_both_bidirectional_branches_are_bounded(runtime):
    assert accepted('אבג דהו', 'דהו אבג', runtime, bidirectional=True) == 'דהו אבג'
    assert accepted('אבג דהו', 'דהו אבגד', runtime, bidirectional=True) is None
    assert accepted('אבג דהו', 'ודהו אבג', runtime, bidirectional=True) is None


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_reversed_per_pair_gaps(runtime):
    # forward: א [2] ב [0] ג; backward reads ג [0] ב [2] א
    q = 'אאא [2] בבב גגג'
    assert accepted(q, 'גגג בבב x y אאא', runtime, bidirectional=True) == 'גגג בבב x y אאא'
    assert accepted(q, 'גגג x בבב אאא', runtime, bidirectional=True) is None


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_flex_spacing_bounds_the_joined_unit_only(runtime):
    assert accepted('שלום עולם', 'אמר שלוםעולם כאן', runtime, flex_spacing=True) == 'שלוםעולם'
    assert accepted('שלום עולם', 'אמר שלוםעולמים כאן', runtime, flex_spacing=True) is None
    assert accepted('שלום עולם', 'אמר בשלוםעולם כאן', runtime, flex_spacing=True) is None
    # a typed word split by a space stays valid
    assert accepted('שלום', 'אמר של ום כאן', runtime, flex_spacing=True) == 'של ום'


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_a_gap_backtracks_to_the_whole_word(runtime):
    # The greedy gap first reaches תורה inside תורהו; the end edge sends it back to
    # the whole word right after ספר.
    assert accepted('ספר [2] תורה', 'ספר תורה תורהו', runtime) == 'ספר תורה'


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_a_star_relaxes_only_its_own_side(runtime):
    assert accepted('*כהנים', 'אלכהנים', runtime) == 'אלכהנים'
    assert accepted('*כהנים', 'אלכהנימ', runtime) is None
    assert accepted('גדול*', 'גדולים', runtime) == 'גדולים'
    assert accepted('גדול*', 'הגדולים', runtime) is None


def test_line_groups_are_bounded_and_anchored():
    groups, gaps = _parse_line_break_query('|ראשונה שורה | שנייה')
    c = compile_line_groups(groups, gaps, {'responsa_mode': True}, None)
    rx = compile_search_regex(c.check_pattern, c.flags)

    def ok(text):
        m = rx.search(text)
        while m is not None and not c.accept(text, m):
            m = rx.search(text, m.start() + 1)
        return m is not None
    assert ok('ראשונה שורה\nשנייה כאן')
    assert ok('"ראשונה שורה\nשנייה')          # the edge word's quote is part of it
    assert not ok('ראשונה שורות\nשנייה')      # the group's last word goes on
    assert not ok('x ראשונה שורה\nשנייה')     # | is the start of the line
    assert not ok('ראשונה שורה\nשנייהא')


def test_a_negated_line_word_is_excluded_not_required():
    groups, gaps = _parse_line_break_query('|ראשונה -אהרן | שנייה')
    c = compile_line_groups(groups, gaps, {'responsa_mode': True}, None)
    assert c.negated_words == ['אהרן']
    assert [p.stems for p in c.plans] == [['ראשונה'], ['שנייה']]


# --- Quotes and brackets: raw and folded --------------------------------------------------

@pytest.mark.parametrize('runtime', RUNTIMES)
def test_quotes_inside_a_word_are_tolerated_raw(runtime):
    assert accepted('רת', 'אמר ר"ת כאן', runtime) == 'ר"ת'
    assert accepted('רת', "אמר ר'ת כאן", runtime) == "ר'ת"


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_the_bracket_free_text_is_what_matches(runtime):
    # The engine matches a bracket-free query on the text with its brackets out.
    text = se._strip_brackets('אמר של[ו]ם עליכם')
    assert accepted('שלום', text, runtime) == 'שלום'
    assert accepted('שלום', se._strip_brackets('אמר ב[שלום'), runtime) is None


# --- The highlight pattern: fixed-width edges (where it and membership differ) ------------

@pytest.mark.parametrize('runtime', RUNTIMES)
@pytest.mark.parametrize('text, member, marked', [
    ('[ספר תורה', 'ספר', ['ספר']),                 # a bracket before: both say a word
    ('x[ספר תורה', None, []),                      # a letter past the bracket: both say no
    ('"ספר תורה', 'ספר', ['ספר']),
    ('x"ספר תורה', None, []),
    ('́ספר תורה', 'ספר', ['ספר']),            # a combining mark before the word
    ('תורה־ספר', 'ספר', ['ספר']),             # maqaf: a boundary for both
    ('תורה‿ספר', None, []),                   # a Pc character: goes on the word
    ('x[[[[ספר', None, []),                        # four looked-past chars: both say no
    (' [[[[ספר', 'ספר', []),                       # ... but the predicate looks past any
                                                   # run; the highlight's lookbehind spells out
                                                   # three, so this one is not marked
])
def test_where_the_highlight_and_membership_agree_and_differ(runtime, text, member, marked):
    assert accepted('ספר', text, runtime) == member
    assert highlighted('ספר', text, runtime) == marked


@pytest.mark.parametrize('runtime', RUNTIMES)
def test_the_highlight_marks_a_bracketed_form_where_it_stands(runtime):
    assert highlighted('שלום', 'אמר של[ו]ם עליכם', runtime) == ['של[ו]ם']
    assert highlighted('שלום עליכם', 'אמר של[ו]ם עליכם', runtime) == ['של[ו]ם עליכם']


def test_a_line_highlight_carries_its_multiline_flag():
    groups, gaps = _parse_line_break_query('|ראשונה | שנייה')
    c = compile_line_groups(groups, gaps, {'responsa_mode': True}, None)
    # The viewers compile a row's pattern without flags.
    assert re.search(c.highlight_pattern, 'x\nראשונה\nשנייה', re.I)


# --- The retrieval gaps the contract names (§4), pinned ----------------------------------

def test_the_token_only_boundaries_are_maqaf_and_sof_pasuq():
    """The check takes a word next to maqaf or sof pasuq; the index keeps both inside the
    token, so a finite lookup misses the page (193 + 271 of 159.8M tokens, §3.6.7)."""
    from shared.search_tokenizer import HEBWORD_TOKENIZER_PATTERN
    token = regex.compile(HEBWORD_TOKENIZER_PATTERN)
    for ch in ('־', '׃'):
        text = f'אבג{ch}דהו'
        assert token.fullmatch(text), 'one token'
        assert accepted('אבג', text) == 'אבג', 'a whole word for the check'
    # A foreign mark inside a Hebrew word: looked past by the predicate, kept by
    # content_search, outside the lookup's Hebrew-mark tolerance (5 words, §3.6.2).
    from shared.responsa_match import _T_CONTENT_SEARCH
    assert 'َ' not in _T_CONTENT_SEARCH

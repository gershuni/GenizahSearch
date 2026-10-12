"""Desktop native matching: engine-generated patterns match with stdlib re.

docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md section 4, gates 1-4, 6, 8 and 9 (the
timed regressions of gate 5 are in test_native_matching_timing.py, the gap
limit of gate 10 in test_native_matching_gap.py). Routing is asserted with
Mocks on the two engines' objects, never by a catastrophic input: a wrong route
must fail an assertion, not hang the run.
"""

import ast
import re
import time
from contextlib import nullcontext
from pathlib import Path
from re import _constants, _parser
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from shared import search_engine, search_regex
from shared.search_engine import (
    SearchEngine, _INSERTER_CLASS, _SEPARATOR_CLASS, _compile_typed_term, _fuzzy_matcher,
    _gap_separator, _native_eligible_forms, _shared_chars_re, _tree_cost_ns,
    make_mark_tolerant_pattern,
)
from shared.search_regex import (
    SearchBudgetExceeded, cancel_check, compile_generated, isolated_matching, search_budget,
)

ROOT = Path(__file__).resolve().parents[1]
W1, W2 = 'שלום', 'עולם'
# Word 1 everywhere, word 2 nowhere: every occurrence matches the first part and
# fails on the second. The regex route needs well over a millisecond for it.
LONG_PAGE = (W1 + ' ') * 48_000


def _engine(forms=lambda term: [term, term + 'א'], level='maximum'):
    """The stub engine of test_search_budget_engine.py, with a variant level."""
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = SimpleNamespace(
        get_variants=lambda term, mode, limit=200: list(forms(term)),
        variant_level=lambda mode: level,
    )
    return engine


def _two_word_maximum():
    return _engine().build_regex_pattern([W1, W2], 'variants', 0)


def _spy_engines(pattern):
    """Both engines' objects replaced by Mocks: (stdlib, regex module)."""
    pattern._stdlib = Mock(name='stdlib')
    pattern._compiled = Mock(name='regex')
    return pattern._stdlib, pattern._compiled


@pytest.fixture
def tight_regex_timeout(monkeypatch):
    monkeypatch.setenv('GENIZAH_REGEX_TIMEOUT_SECONDS', '0.001')
    monkeypatch.delenv('GENIZAH_SEARCH_BUDGET_SECONDS', raising=False)


# --- Gate 1: a long page no longer aborts the search --------------------------

def test_the_long_page_exhausts_the_regex_route(tight_regex_timeout):
    # The premise of the gate below: on the regex route this page times out.
    pattern = _two_word_maximum()
    regex_route = search_regex.compile(pattern.pattern, pattern.flags)
    with pytest.raises(SearchBudgetExceeded):
        regex_route.search(LONG_PAGE)


@pytest.mark.parametrize('context', ['no budget', 'desktop search_budget()'])
def test_a_long_page_is_matched_natively_within_the_regex_timeout(tight_regex_timeout, context):
    with search_budget() if context != 'no budget' else nullcontext():
        pattern = _two_word_maximum()
        # Before native matching this raised SearchBudgetExceeded.
        assert pattern.search(LONG_PAGE) is None
    assert pattern._generated is True
    assert len(LONG_PAGE) <= pattern.native_max_chars
    assert pattern._compiled is None      # the regex object was never needed


def test_windows_workers_keep_the_regex_route_without_a_timeout(tight_regex_timeout):
    # Selection, not timeout: the worker's regex object runs with no timeout,
    # and stdlib re (which a Python watchdog thread cannot stop) is never used.
    with isolated_matching(native=False):
        pattern = _two_word_maximum()
        assert pattern._generated is False
        regex_object = pattern._compiled
        pattern._stdlib = Mock(name='stdlib')
        pattern._compiled = Mock(wraps=regex_object)
        assert pattern.search(LONG_PAGE) is None
    pattern._stdlib.search.assert_not_called()
    call = pattern._compiled.search.call_args
    assert call.args == (LONG_PAGE,)
    assert call.kwargs['timeout'] is None and call.kwargs['concurrent'] is True


def test_linux_workers_match_any_size_natively():
    with isolated_matching(native=True):
        pattern = _two_word_maximum()
        assert pattern._generated is True
        subject = 'א' * 10_000_000
        assert len(subject) > pattern.native_max_chars
        stdlib, regex_object = _spy_engines(pattern)
        pattern.search(subject)
    stdlib.search.assert_called_once_with(subject, pos=0)
    regex_object.search.assert_not_called()


# --- Gate 2: which patterns are generated, and where their calls go -----------

def _responsa(components, per_pair_gaps=None, **options):
    return _engine().build_regex_pattern(
        None, 'exact', 0, responsa_components=components, responsa_options=options,
        per_pair_gaps=per_pair_gaps)


_TWO_COMPONENTS = [{'regex_terms': [W1]}, {'regex_terms': [W2]}]
_SUFFIX_WILDCARD = {'regex_terms': [W1], 'wildcard': 'suffix', 'wildcard_pattern': None,
                    'flex_patterns': [], 'inline_pattern': None, 'original_words': [W1]}

NOT_GENERATED = {
    'Regex mode': lambda: _engine().build_regex_pattern(['(a|aa)+$'], 'Regex', 0),
    'Responsa within_document': lambda: _responsa(_TWO_COMPONENTS, within_document=True),
    'Responsa wildcard': lambda: _responsa([_SUFFIX_WILDCARD, {'regex_terms': [W2]}]),
    'Responsa flex spacing': lambda: _responsa(_TWO_COMPONENTS, flex_spacing=True),
    'Responsa per-pair gap': lambda: _responsa(_TWO_COMPONENTS, per_pair_gaps=[1]),
    'gap 1': lambda: _engine().build_regex_pattern([W1, W2], 'variants', 1),
    'Basic multi-form': lambda: _engine(level='basic').build_regex_pattern([W1, W2], 'variants', 0),
    '66-char form at Maximum': lambda: _engine(forms=lambda t: [t]).build_regex_pattern(
        ['א' * 66], 'variants', 0),
    'apostrophe in a form': lambda: _engine().build_regex_pattern(["ש'לום", W2], 'variants', 0),
    'iota form': lambda: _engine().build_regex_pattern(['ש\u03b9ם', W2], 'variants', 0),
    'Latin aa/Aa': lambda: _engine(forms=lambda t: ['aa', 'Aa']).build_regex_pattern(
        ['aa'], 'variants', 0),
    "a form with '.'": lambda: _engine(forms=lambda t: [t, t + '.' + t]).build_regex_pattern(
        ['אב'], 'variants', 0),
    'an empty form': lambda: _engine(forms=lambda t: [t, '']).build_regex_pattern(
        [W1], 'variants', 0),
    "parts ['א', '.', 'ב']": lambda: _engine().build_regex_pattern(['א', '.', 'ב'], 'variants', 0),
}

_RICH_FORMS = lambda t: [t, t + 'א', 'ו' + t, 'ה' + t, t[:-1] + 'ה', 'וה' + t]  # noqa: E731

GENERATED = {
    'literal single form': lambda: _engine(forms=lambda t: [t]).build_regex_pattern(
        [W1, W2], 'literal', 0),
    'Basic single form': lambda: _engine(forms=lambda t: [t], level='basic').build_regex_pattern(
        [W1, W2], 'variants', 0),
    'Extended two words gap 0': lambda: _engine(_RICH_FORMS, 'extended').build_regex_pattern(
        [W1, W2], 'variants', 0),
    'Maximum two words gap 0': _two_word_maximum,
    'composition chunk': lambda: _engine(forms=lambda t: [t]).build_regex_pattern(
        ['ויאמר', 'משה', 'אל', 'העם', 'אל', 'תיראו'], 'exact', 0),
}


@pytest.mark.parametrize('name', list(NOT_GENERATED))
def test_ineligible_shapes_keep_the_regex_route(name):
    pattern = NOT_GENERATED[name]()
    assert pattern is not None
    assert pattern._generated is False
    stdlib, regex_object = _spy_engines(pattern)
    pattern.search('שלום עולם')
    regex_object.search.assert_called_once()
    stdlib.search.assert_not_called()


@pytest.mark.parametrize('name', list(GENERATED))
def test_eligible_shapes_match_natively(name):
    pattern = GENERATED[name]()
    assert pattern._generated is True
    stdlib, regex_object = _spy_engines(pattern)
    pattern.search('שלום עולם')
    stdlib.search.assert_called_once_with('שלום עולם', pos=0)
    regex_object.search.assert_not_called()


def test_fuzzy_typed_terms_route_by_their_form():
    assert _compile_typed_term('123')._generated is True
    assert _compile_typed_term('abc')._generated is False
    # The call site: a Fuzzy word that is not plain Hebrew letters.
    matcher = _fuzzy_matcher(['123', 'abc'], 0)
    assert [rx._generated for rx in matcher._typed_rx] == [True, False]


def test_an_expired_budget_raises_before_any_match():
    pattern = _two_word_maximum()
    stdlib, regex_object = _spy_engines(pattern)
    # (Had the call not raised, the budget's exit would: the Mocks tell.)
    with pytest.raises(SearchBudgetExceeded):
        with search_budget(0.001):
            time.sleep(0.05)          # run out (Windows' monotonic ticks every 15.6 ms)
            pattern.search('שלום עולם')
    stdlib.search.assert_not_called()
    regex_object.search.assert_not_called()


def test_a_finite_budget_still_matches_natively():
    pattern = _two_word_maximum()
    stdlib, regex_object = _spy_engines(pattern)
    with search_budget(5):
        pattern.search('שלום עולם')
    stdlib.search.assert_called_once_with('שלום עולם', pos=0)
    regex_object.search.assert_not_called()


def test_sub_never_matches_natively():
    pattern = _two_word_maximum()
    stdlib, regex_object = _spy_engines(pattern)
    pattern.sub('x', 'שלום עולם')
    stdlib.sub.assert_not_called()
    call = regex_object.sub.call_args
    assert call.args == ('x', 'שלום עולם')
    assert call.kwargs['concurrent'] is True and call.kwargs['count'] == 0


@pytest.mark.parametrize('route', ['native', 'regex'])
def test_a_stop_lands_before_either_route(route):
    pattern = _two_word_maximum()
    # A page over the cap would take the regex route.
    subject = 'שלום עולם' if route == 'native' else 'א' * (pattern.native_max_chars + 1)
    stdlib, regex_object = _spy_engines(pattern)
    with cancel_check(lambda: True):
        with pytest.raises(InterruptedError):
            pattern.search(subject)
    stdlib.search.assert_not_called()
    regex_object.search.assert_not_called()
    with cancel_check(lambda: False):
        pattern.search(subject)
    used = stdlib if route == 'native' else regex_object
    assert used.search.call_count == 1
    assert stdlib.search.call_count + regex_object.search.call_count == 1


# --- Gate 3: the dispatcher ---------------------------------------------------

CAP_COST_NS = 50_000      # HOLD_BUDGET_S over 50 us: a cap of exactly 20,000 characters


def _capped(parts=1):
    pattern = compile_generated(
        _gap_separator(0).join(make_mark_tolerant_pattern('אב') for _ in range(parts)),
        re.IGNORECASE, cost_per_start_ns=CAP_COST_NS, parts=parts,
        shared_chars=_shared_chars_re())
    assert pattern._generated is True and pattern.native_max_chars == 20_000
    # The lazy regex compile reads the validated stdlib object's pattern and flags.
    stdlib = pattern._stdlib
    pattern._stdlib = Mock(wraps=stdlib, pattern=stdlib.pattern, flags=stdlib.flags)
    return pattern


@pytest.fixture
def regex_compiles(monkeypatch):
    """Every regex-module compile, as a Mock wrapping the real object."""
    compiles = []
    real = search_regex._regex.compile

    def spy(*args, **kwargs):
        compiles.append(Mock(wraps=real(*args, **kwargs)))
        return compiles[-1]

    monkeypatch.setattr(search_regex._regex, 'compile', spy)
    return compiles


def test_one_char_over_the_cap_takes_the_regex_route_compiled_once(regex_compiles):
    pattern = _capped()
    subject = 'א' * 20_001
    pattern.search(subject)
    pattern.search(subject)
    assert len(regex_compiles) == 1
    assert regex_compiles[0].search.call_count == 2
    pattern._stdlib.search.assert_not_called()


def test_at_the_cap_matches_natively_and_compiles_nothing(regex_compiles):
    pattern = _capped()
    subject = 'א' * 20_000
    pattern.search(subject)
    pattern._stdlib.search.assert_called_once_with(subject, pos=0)
    assert regex_compiles == []
    assert pattern._compiled is None


@pytest.mark.parametrize('length, pos, endpos, native', [
    (30_000, 5_000, 25_000, True),      # the range, not the string, is measured
    (30_000, 10_000, None, True),
    (30_000, 9_999, None, False),
    (30_000, 0, 20_001, False),
    (20_000, 0, 10**9, True),           # endpos past the end counts to the end
])
def test_the_cap_measures_the_searched_range(regex_compiles, length, pos, endpos, native):
    pattern = _capped()
    subject = 'א' * length
    pattern.search(subject, pos, endpos)
    assert pattern._stdlib.search.called is native
    assert len(regex_compiles) == (0 if native else 1)


@pytest.mark.parametrize('parts', [1, 2, 3, 4, 5, 6])
def test_a_shared_character_cap_halves_per_part_after_two(parts):
    pattern = _capped(parts)
    if parts == 1:
        assert pattern._shared_chars is None          # a single part has no boundary
    else:
        assert pattern._shared_chars is _shared_chars_re()
    assert pattern.native_max_chars_shared == 20_000 >> max(0, parts - 2)


def _shared_subject(length):
    """*length* characters with a shared character ('"') between two letters."""
    return 'אב"אב' + 'א' * (length - 5)


@pytest.mark.parametrize('parts, length, native', [
    (3, 10_000, True),        # within the shared cap (20,000 >> 1)
    (3, 10_001, False),       # over it, though within the plain cap
    (2, 20_000, True),        # at two parts the shared cap is the plain cap
    (2, 20_001, False),
    (1, 20_000, True),        # a single part ignores shared characters
])
def test_a_shared_character_applies_the_shared_cap(regex_compiles, parts, length, native):
    pattern = _capped(parts)
    subject = _shared_subject(length)
    pattern.search(subject)
    assert pattern._stdlib.search.called is native
    assert len(regex_compiles) == (0 if native else 1)


def test_without_a_shared_character_the_plain_cap_applies(regex_compiles):
    pattern = _capped(parts=3)
    subject = 'אב אב' + 'א' * (10_001 - 5)
    pattern.search(subject)
    pattern._stdlib.search.assert_called_once_with(subject, pos=0)
    assert regex_compiles == []


@pytest.mark.parametrize('mark', ['\u05f3', '\u05f4'])
def test_geresh_and_gershayim_are_not_shared(regex_compiles, mark):
    pattern = _capped(parts=3)
    subject = 'אב' + mark + 'אב' + 'א' * (10_001 - 5)
    pattern.search(subject)
    pattern._stdlib.search.assert_called_once_with(subject, pos=0)
    assert regex_compiles == []


def test_an_expired_budget_raises_before_the_lazy_compile(regex_compiles):
    pattern = _capped()
    with pytest.raises(SearchBudgetExceeded):
        with search_budget(0.001):
            time.sleep(0.05)          # run out (Windows' monotonic ticks every 15.6 ms)
            pattern.search('א' * 20_001)
    assert regex_compiles == []
    assert pattern._compiled is None


# --- Gate 4: the cost model and the shared set --------------------------------

@pytest.mark.parametrize('forms, cost', [
    (['שלום'], 20 * 4 + 5 * 4),                      # one branch per level
    (['שלום', 'שלומא'], 100 + 125),                  # two lengths, two trees
    (['אב', 'אג'], 20 * 2 + 5 * (1 + 2)),            # two siblings at the second level
    (['אבג', 'אבד', 'אהו'], 20 * 3 + 5 * (1 + 2 + 2)),
])
def test_tree_cost_of_hand_built_forms(forms, cost):
    assert _tree_cost_ns(forms) == cost


def test_the_shared_set_is_the_two_classes_intersection():
    every = search_regex._every_character()
    inserter = set(re.findall(_INSERTER_CLASS, every, re.IGNORECASE))
    separator = set(re.findall(_SEPARATOR_CLASS, every, re.IGNORECASE))
    shared = set(_shared_chars_re().findall(every))
    assert shared == inserter & separator
    assert {'"', '\u2018', '\u2019', '\u0300'} <= shared
    assert not {'\u05f3', '\u05f4', "'"} & shared


INELIGIBLE_FORMS = {
    'empty': '', '65 chars': 'א' * 65, 'newline': 'א\nב', 'apostrophe': "א'ב",
    'double quote': 'א"ב', 'dot': 'א.ב', 'bracket': 'א[ב', 'space': 'א ב',
    'Greek iota': 'א\u03b9ב', 'Latin a': 'אaב',
}
ELIGIBLE_FORMS = ['שלום', '123', 'אבג', 'א' * 64]


@pytest.mark.parametrize('name', list(INELIGIBLE_FORMS))
def test_one_ineligible_form_makes_the_part_ineligible(name):
    form = INELIGIBLE_FORMS[name]
    assert _native_eligible_forms([form]) is False
    assert _native_eligible_forms(ELIGIBLE_FORMS + [form]) is False


@pytest.mark.parametrize('form', ELIGIBLE_FORMS)
def test_plain_forms_are_eligible(form):
    assert _native_eligible_forms([form]) is True


def test_build_regex_pattern_passes_parts_cost_and_the_shared_class(monkeypatch):
    seen = []
    real = search_engine.compile_generated_regex

    def spy(pattern, flags=0, **kwargs):
        seen.append(kwargs)
        return real(pattern, flags, **kwargs)

    monkeypatch.setattr(search_engine, 'compile_generated_regex', spy)
    words = [W1, W2, 'דבר']
    assert _engine().build_regex_pattern(words, 'variants', 0)._generated is True
    (kwargs,) = seen
    assert kwargs['parts'] == 3
    assert kwargs['shared_chars'] is _shared_chars_re()
    assert kwargs['cost_per_start_ns'] == sum(_tree_cost_ns([w, w + 'א']) for w in words)


def test_eight_thousand_siblings_get_a_small_cap():
    # Codex's CJK case: 8,000 two-character forms branching at the root.
    forms = [chr(0x4E00 + i) + 'ב' for i in range(8000)]
    pattern = _engine(forms=lambda t: forms).build_regex_pattern([forms[0]], 'variants', 0)
    assert pattern._generated is True
    assert search_regex.NATIVE_MIN_SUBJECT_CHARS <= pattern.native_max_chars < 30_000
    stdlib, regex_object = _spy_engines(pattern)
    pattern.search(forms[0][0] * 250_000)
    regex_object.search.assert_called_once()
    stdlib.search.assert_not_called()


def test_the_nineteen_part_quote_subject_takes_the_regex_route():
    # Codex's 2**parts case: '"' is both an inserter and a separator character,
    # so each א" can be read inside a part or between two. Within the plain cap,
    # over the shared one (smaller by 2**17).
    forms = {'א': ['אא', 'א'], 'בב': ['בב']}
    pattern = _engine(forms=lambda t: forms[t]).build_regex_pattern(
        ['א'] * 18 + ['בב'], 'variants', 0)
    assert pattern._generated is True
    subject = 'א"' * 256 + 'ג'
    assert pattern.native_max_chars_shared < len(subject) <= pattern.native_max_chars
    stdlib, regex_object = _spy_engines(pattern)
    pattern.search(subject)
    regex_object.search.assert_called_once()
    stdlib.search.assert_not_called()


# --- Gate 6: the grammar of generated patterns --------------------------------

def _items(node):
    """Every (opcode, argument) in a parsed pattern, nested ones included."""
    if isinstance(node, _parser.SubPattern):
        for op, av in node.data:
            yield op, av
            yield from _items(av)
    elif isinstance(node, (list, tuple)):
        for child in node:
            yield from _items(child)


def _class_items(class_pattern, flags):
    ((_repeat, (_min, _max, body)),) = _parser.parse(class_pattern, flags).data
    ((op, items),) = body.data
    assert op is _constants.IN
    return items


_REPEATS = (_constants.MAX_REPEAT, _constants.MIN_REPEAT, _constants.POSSESSIVE_REPEAT)


@pytest.mark.parametrize('words', [1, 2, 3, 4])
@pytest.mark.parametrize('shape', ['exact', 'basic single', 'extended', 'maximum'])
def test_generated_patterns_repeat_only_the_two_classes(shape, words):
    engine = {
        'exact': lambda: _engine(forms=lambda t: [t]),
        'basic single': lambda: _engine(forms=lambda t: [t], level='basic'),
        'extended': lambda: _engine(_RICH_FORMS, 'extended'),
        'maximum': lambda: _engine(_RICH_FORMS, 'maximum'),
    }[shape]()
    pattern = engine.build_regex_pattern([W1, W2, 'דבר', 'אמת'][:words],
                                         'exact' if shape == 'exact' else 'variants', 0)
    assert pattern._generated is True
    separator = _class_items(_SEPARATOR_CLASS + '+', pattern.flags)
    inserter = _class_items(_INSERTER_CLASS + '*', pattern.flags)
    unbounded = []
    for op, av in _items(_parser.parse(pattern.pattern, pattern.flags)):
        assert op not in (_constants.GROUPREF, _constants.GROUPREF_EXISTS,
                          _constants.ASSERT, _constants.ASSERT_NOT)
        if op in _REPEATS and av[1] == _constants.MAXREPEAT:
            ((body_op, items),) = av[2].data
            assert body_op is _constants.IN and items in (separator, inserter)
            unbounded.append('separator' if items == separator else 'inserter')
    # The walk saw the pattern: every letter boundary has an inserter, every
    # word boundary a separator.
    assert unbounded.count('separator') == words - 1
    assert unbounded.count('inserter') > 0


# --- Gate 8: only the engine builds generated patterns ------------------------

_GENERATED_NAMES = {'compile_generated', 'compile_generated_regex'}


def _files_referencing_generated_compile():
    paths = [path for top in ('shared', 'web', 'desktop')
             for path in sorted((ROOT / top).rglob('*.py'))] + [ROOT / 'genizah_app.py']
    found = set()
    for path in paths:
        source = path.read_text(encoding='utf-8')
        if 'compile_generated' not in source:     # every match below spells it
            continue
        for node in ast.walk(ast.parse(source, filename=str(path))):
            if (isinstance(node, ast.Name) and node.id in _GENERATED_NAMES
                    or isinstance(node, ast.Attribute) and node.attr in _GENERATED_NAMES
                    or isinstance(node, ast.alias)
                    and (node.name in _GENERATED_NAMES or node.asname in _GENERATED_NAMES)
                    or isinstance(node, ast.Constant) and node.value in _GENERATED_NAMES):
                found.add(path.relative_to(ROOT).as_posix())
    return found


def test_only_the_engine_compiles_generated_patterns():
    # A generated pattern is matched with stdlib re, which holds the GIL and
    # cannot be stopped: only build_regex_pattern vouches for a shape.
    assert _files_referencing_generated_compile() == {'shared/search_engine.py'}


# --- Gate 9: the native route and the regex route agree -----------------------

PARITY_TEXTS = [
    'שלום עולם', 'שלוםא עולם', 'שלום דבר עולם', 'שלום\nעולם',
    'שָׁלוֹם עולם', ']שלום[ עולם', 'שלום\u0307 עולם',
    'שלום\u200cעולם', 'שלום_עולם', 'שלום²עולם', 'nothing matches',
    'ושלום עולמא', 'השלום  עולם', 'שלוה עולם', 'של\u05f4ום עולם', 'שלום\u05f3 עולם',
    'של"ום עולם', 'שלום"עולם', 'שלו\u2019ם עולם',
]


def _span_groups(match):
    return (match.span(), match.groups()) if match else None


@pytest.mark.parametrize('level', ['extended', 'maximum'])
@pytest.mark.parametrize('text', PARITY_TEXTS)
def test_the_native_route_agrees_with_the_regex_route(level, text):
    generated = _engine(_RICH_FORMS, level).build_regex_pattern([W1, W2], 'variants', 0)
    assert generated._generated is True
    regex_route = search_regex.compile(generated.pattern, generated.flags)
    assert regex_route._generated is False and regex_route._compiled is not None
    expected = _span_groups(regex_route.search(text))
    assert _span_groups(generated.search(text)) == expected
    assert _span_groups(generated._stdlib.search(text)) == expected

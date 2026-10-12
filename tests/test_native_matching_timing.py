"""Timed regressions for native matching (docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md
section 4, gate 5).

A native (stdlib re) call holds the GIL and cannot be stopped, so each case runs
in a child process under an external timeout: a regression that makes a call
super-linear is killed, not waited for. Each asserts the route (generated, and
the regex object never compiled) and a wall time of at most twice what the cost
model predicts for the subject -- HOLD_BUDGET_S at the cap -- measured in the
child around the one search call.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from shared.search_regex import HOLD_BUDGET_S

pytestmark = pytest.mark.slow

ROOT = str(Path(__file__).resolve().parents[1])
MARGIN = 2.0

_CHILD = r'''
import json, sys, time
from contextlib import nullcontext
sys.path.insert(0, sys.argv[1])
from types import SimpleNamespace
from shared.search_engine import SearchEngine
from shared import search_regex

ALEF, BET, GIMEL = chr(0x5D0), chr(0x5D1), chr(0x5D2)


def build(forms_by_term, terms):
    engine = SearchEngine.__new__(SearchEngine)
    engine.var_mgr = SimpleNamespace(
        get_variants=lambda term, mode, limit=200: list(forms_by_term[term]),
        variant_level=lambda mode: 'maximum')
    return engine.build_regex_pattern(terms, 'variants', 0)


def degenerate():
    # One part, forms alef*n+bet for n in 3..20: 18 trees, 225 levels in all; a
    # run of alef walks every tree to its last level at every start.
    forms = [ALEF * n + BET for n in range(3, 21)]
    return build({forms[0]: forms}, [forms[0]])


def degenerate_at_cap():
    pattern = degenerate()
    return pattern, ALEF * pattern.native_max_chars, 'plain'


def degenerate_million_linux_worker():
    pattern = degenerate()
    return pattern, ALEF * 1_000_000, 'plain'


PREFIXES = ['', 'ו', 'ה', 'ב', 'כ', 'ל', 'מ', 'ש', 'וה', 'וב', 'ול', 'ומ', 'וש', 'שה',
            'מה', 'כש', 'לה', 'בה', 'וכ', 'שב']
SWAPS = {'א': 'הע', 'ה': 'אח', 'ע': 'א', 'ח': 'הכ', 'ו': 'י', 'י': 'ו', 'ת': 'ט',
         'ט': 'ת', 'כ': 'קח', 'ק': 'כ', 'ס': 'שצ', 'ש': 'ס', 'צ': 'ס', 'ב': 'ו', 'ם': 'ן',
         'ן': 'ם', 'ר': 'ד', 'ד': 'ר', 'מ': 'נ', 'נ': 'מ', 'ג': 'כ', 'ז': 'צ', 'פ': 'ב'}


def real_shape_forms(word):
    # Prefixes times up to two letter swaps or mater insertions: the shape of a
    # Maximum word (trees of several lengths, wide at the prefixes).
    spellings = {word}
    for _ in range(2):
        grown = set(spellings)
        for s in spellings:
            for i, c in enumerate(s):
                for r in SWAPS.get(c, ''):
                    grown.add(s[:i] + r + s[i + 1:])
                for m in 'וי':
                    grown.add(s[:i] + m + s[i:])
        spellings = grown
    return sorted({p + s for p in PREFIXES for s in spellings})


def real_shape_four_words():
    words = ['אתה', 'זכור', 'מעשה', 'עולם']
    pattern = build({w: real_shape_forms(w) for w in words}, words)
    # Three of the four words everywhere, the fourth nowhere.
    size = min(160_000, pattern.native_max_chars)
    return pattern, ('ואתה זכור מעשה ' * size)[:size], 'plain'


def siblings_at_cap():
    # Codex's CJK case: 8,000 siblings at the root, two levels.
    forms = [chr(0x4E00 + i) + BET for i in range(8000)]
    pattern = build({forms[0]: forms}, [forms[0]])
    return pattern, (forms[-1][0] + GIMEL) * (pattern.native_max_chars // 2), 'plain'


def shared_at_shared_cap(parts):
    # A '"' at every boundary: each can be read inside a part or between two.
    pattern = build({ALEF: [ALEF * 2, ALEF], BET * 2: [BET * 2]},
                    [ALEF] * (parts - 1) + [BET * 2])
    size = pattern.native_max_chars_shared
    return pattern, ((ALEF + '"') * size)[:size], 'shared'


CASES = {
    'degenerate_at_cap': (degenerate_at_cap, nullcontext),
    'degenerate_million_linux_worker': (
        degenerate_million_linux_worker, lambda: search_regex.isolated_matching(native=True)),
    'real_shape_four_words': (real_shape_four_words, nullcontext),
    'siblings_at_cap': (siblings_at_cap, nullcontext),
    'shared_at_shared_cap_6': (lambda: shared_at_shared_cap(6), nullcontext),
    'shared_at_shared_cap_10': (lambda: shared_at_shared_cap(10), nullcontext),
}

make, context = CASES[sys.argv[2]]
with context():
    pattern, subject, cap_kind = make()
    started = time.perf_counter()
    found = pattern.search(subject)
    elapsed = time.perf_counter() - started
print(json.dumps({
    'generated': pattern._generated, 'native': pattern._compiled is None,
    'found': found is not None, 'chars': len(subject), 'elapsed': elapsed,
    'cap': pattern.native_max_chars if cap_kind == 'plain' else pattern.native_max_chars_shared,
}))
'''


def _run(case, timeout):
    """The child's report for *case*; fails (killing it) past *timeout* seconds."""
    try:
        done = subprocess.run(
            [sys.executable, '-I', '-X', 'utf8', '-c', _CHILD, ROOT, case],
            capture_output=True, text=True, encoding='utf-8', timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail(f'{case}: the child did not finish within {timeout} s and was killed')
    assert done.returncode == 0, done.stderr
    return json.loads(done.stdout.strip().splitlines()[-1])


def _assert_native_and_timely(report):
    assert report['generated'] is True
    assert report['native'] is True           # the regex object was never compiled
    assert report['found'] is False
    predicted = HOLD_BUDGET_S * report['chars'] / report['cap']
    assert report['elapsed'] < MARGIN * predicted, report


def test_degenerate_run_at_its_cap():
    report = _run('degenerate_at_cap', timeout=30)
    assert report['cap'] == int(1e9 / (25 * 225))       # 20 ns a level + 5 a sibling, 225 levels
    assert report['chars'] == report['cap']
    _assert_native_and_timely(report)


def test_degenerate_run_on_a_million_chars_in_a_linux_worker():
    # No cap in a supervised Linux worker: a million characters, about 5.6 cap
    # lengths, still native and linear.
    report = _run('degenerate_million_linux_worker', timeout=40)
    assert report['chars'] == 1_000_000 > report['cap']
    _assert_native_and_timely(report)


def test_real_shape_four_word_pattern():
    report = _run('real_shape_four_words', timeout=40)
    assert report['cap'] >= 100_000           # the order of a real 4-word Maximum query's
    assert report['chars'] == min(160_000, report['cap'])
    _assert_native_and_timely(report)


def test_eight_thousand_siblings_at_their_cap():
    report = _run('siblings_at_cap', timeout=30)
    assert report['cap'] < 30_000
    _assert_native_and_timely(report)


@pytest.mark.parametrize('parts', [6, 10])
def test_shared_characters_at_the_shared_cap(parts):
    report = _run(f'shared_at_shared_cap_{parts}', timeout=30)
    assert report['chars'] == report['cap'] > 0
    _assert_native_and_timely(report)

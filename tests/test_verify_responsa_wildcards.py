"""The Responsa ground-truth gate (scripts/verify_responsa_wildcards.py; plan §8 gate 1) run on a
small corpus built by the real Indexer, as format 2 (seams) and as format 1, so its logic --
the oracle's reading of the contract, its seam-aware crossing witnesses, the documented
differences it lists by count -- is proven here, and the real-data run is the local session's.

Each corpus page is written for a query of the Phase 1 and Phase 3 sets (the comments say
which). The gate must pass on both formats, fail under each named mutation that touches what the
corpus holds, and fail -- never skip -- when the index or the corpus file is missing.
"""
import os
import sys

import pytest

from tests.responsa_index_fixture import FakeMeta, build

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, 'scripts'))
import verify_responsa_wildcards as gate  # noqa: E402


def sid(n):
    return f"99000000{n:03d}0205171"


MANUSCRIPTS = {
    201: ["פתיחה מאמתי", "קורין סוף"],                  # מאמ* קור*: across the break
    202: ["אמר כהנים גדולים עמדו"],                    # the Phase 1 pair and every star query, inside
    203: ["כהנים גדול."],                              # כהנים גדול
    204: ["ספר תורה"],                                 # ספר
    205: ["כהנים גדולים בשורה\nעוד"],                  # |כהנים גדולים
    206: ["שם כוהנים", "גדולים שם"],                   # כ*נים / כ*ה*נ*ים גדולים: across the break
    207: ["אחד כהנים", "גדולים שני"],                   # finite and star words across the break
    208: ["תחילה מאמרו"],                              # V0.8's last page; the V0.7 copy follows
    209: ["כהנים גדולים", "שם כהנים", "גדולים סוף"],    # inside p1, then across p2 | p3
    210: ["ואהרן כהנים גדולים"],                       # -אהרן hides it
    211: ["אמר מאמתי קורין את"],                       # מאמ* קור*: inside a page
    212: ["כהנים שם הכהנים גדול."],                    # כהנים גדול: no (the first word began before)
    213: ["שם אלכהנים אלגדולים"],                      # *כהנים *גדולים: letters no prefix list has
    214: ["כהנים", "כהנים", "גדולים"],                 # כהנ* [1] גדול*: p1 | p3, and p2 | p3 apart
}
V08 = [(sid(n), 3000 + n, p, text) for n, pages in MANUSCRIPTS.items() for p, text in enumerate(pages, 1)]
V07 = [(sid(208), 3208, 1, "קוראים כאן")]          # format 1 joins it to 208's V0.8 page


@pytest.fixture(scope='module')
def indexes(tmp_path_factory):
    base = tmp_path_factory.mktemp('verify_gate')
    roots = {'2': build(base / 'format2', V08, V07),
             '1': build(base / 'format1', V08, V07, seams=False, reversed_field=False)}
    return {fmt: {'index': os.path.join(root, 'tantivy_db'), 'corpus': os.path.join(root, 'v8.txt')}
            for fmt, root in roots.items()}


def run_gate(indexes, fmt, tmp_path, *extra):
    paths = indexes[fmt]
    return gate.main(['--index-dir', paths['index'], '--corpus-file', paths['corpus'],
                      '--out-dir', str(tmp_path / f'out{fmt}'), *extra], meta_factory=FakeMeta)


@pytest.mark.parametrize('phase', ['1', '3'])
@pytest.mark.parametrize('fmt', ['2', '1'])
def test_the_gate_passes_on_both_formats(indexes, tmp_path, fmt, phase):
    assert run_gate(indexes, fmt, tmp_path, '--phase', phase) == gate.EXIT_OK


def _verdicts(tmp_path, fmt, phase='3'):
    import json
    with open(tmp_path / f'out{fmt}' / f'phase{phase}.json', encoding='utf-8') as f:
        return json.load(f)['queries']


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_the_gate_compares_the_crossings_and_lists_the_known_kinds(indexes, tmp_path, fmt):
    assert run_gate(indexes, fmt, tmp_path, '--phase', '3') == gate.EXIT_OK
    v = _verdicts(tmp_path, fmt)
    # מאמ* קור*: 211 inside its page; 201 across its break, read from the seams (format 2)
    # or from the break windows (format 1), where 208's V0.8 page joined to its V0.7 copy is
    # a junction the seams do not hold.
    assert v['star-end-pair']['matched'] == 1 and v['star-end-pair']['crossing_matched'] == 1
    v07 = v['star-end-pair']['expected'].get('a crossing into the V0.7 copy (whole-manuscript document)', 0)
    assert v07 == (1 if fmt == '1' else 0)
    # כהנ* גדול*: 207's, 209's p2 | p3 and 214's p2 | p3 crossings.
    assert v['star-end']['crossing_matched'] == 3
    # Finite words: on format 2 the seams give 207's, 209's p2 | p3 and 214's (decision 10);
    # on format 1 a whole manuscript gives its first match only, so 209's is listed by count.
    if fmt == '2':
        assert v['hash-hash']['crossing_matched'] == 3 and v['hash-hash']['expected'] == {}
    else:
        assert v['hash-hash']['crossing_matched'] == 2
        assert v['hash-hash']['expected'] == {
            'a crossing finite words did not reach (a whole-manuscript document gives its first match only)': 1}
    assert v['star-several']['crossing_matched'] == 4          # 206, 207, 209's and 214's p2 | p3
    # כהנ* [1] גדול*: 214's p1 runs over p2 into p3 (more than two pages, listed) and its
    # p2's own crossing into p3 is still found (Codex review of PR #443); 207 and 209 too.
    assert v['star-gap']['crossing_matched'] == 3
    assert v['star-gap']['expected'] == {'a match over more than two pages (the oracle reads pairs)': 1}
    # Leading stars: 202, 205, 209's p1, 210 and 213 inside a page; 207, 209's and 214's
    # p2 | p3 across a break (206's כוהנים is not *כהנים).
    assert v['star-start']['matched'] == 5 and v['star-start']['crossing_matched'] == 3
    assert v['star-start-endings']['crossing_matched'] == 4    # 206, 207, 209's and 214's p2 | p3
    assert v['star-start-several']['crossing_matched'] == 4


@pytest.mark.parametrize('fmt, mutation', [
    ('2', 'star-list'), ('1', 'star-list'),
    ('2', 'no-crossing'), ('1', 'no-crossing'),
    ('1', 'no-star-windows'),
    ('2', 'accept-off'),
    ('2', 'leading-prefix-list'), ('1', 'leading-prefix-list'),
    ('2', 'finite-crossings-whole'),
])
def test_the_gate_fails_under_each_mutation(indexes, tmp_path, fmt, mutation):
    assert run_gate(indexes, fmt, tmp_path, '--phase', '3', '--mutation', mutation) == gate.EXIT_FAILED


@pytest.mark.parametrize('missing', ['index', 'corpus'])
def test_the_gate_fails_never_skips_without_its_data(indexes, tmp_path, missing):
    paths = dict(indexes['2'])
    paths[missing] = str(tmp_path / 'not-there')
    assert gate.main(['--index-dir', paths['index'], '--corpus-file', paths['corpus'],
                      '--out-dir', str(tmp_path / 'out'), '--phase', '3']) == gate.EXIT_MISSING_DATA


def test_the_oracle_reads_a_star_by_the_contract():
    w = gate.StarWord('כ*נים')
    assert 'כהנים' in w and 'כנים' in w and 'כוהנים' in w
    assert 'הכהנים' not in w and 'כ1נים' not in w and 'כ_נים' not in w
    assert 'כֹּהֲנִים' not in w                       # a literal run is typed letters only
    assert 'כֹּהֲנִים' in gate.StarWord('כ*ה*נ*ים')      # a star takes marks
    assert 'שלומות' in gate.StarWord('שלום*') and 'שלום' in gate.StarWord('שלומ*')

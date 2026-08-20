# -*- coding: utf-8 -*-
"""Parity of shared/passage_normalize.py against the research implementation.

Why this test exists. Every calibrated constant in
docs/specs/passage-matching-algorithm.md -- both acceptance boundaries, the
Stage-0 thresholds, the DF-banding recall findings -- was measured against the
normalizer in the gitignored research tree. If this port differs from that one
by a single character class, those constants silently stop applying and no
other test in the suite would notice. So parity is asserted directly, on real
corpus text, against the original code.

Structure:
  * The FIXTURE tests always run. They pin expected output literally, so this
    file is not vacuous where the research tree is absent (a worktree, CI).
  * The PARITY tests import the original modules by absolute path and skip
    with a loud reason when those are missing.
  * The CORPUS parity test samples pages from several byte offsets, not just
    the head: Transcriptions.txt is catalog-ordered, so head-only sampling
    would only exercise one library's hands.

Overrides: GENIZAH_PROBE_SCRIPTS, GENIZAH_TRANSCRIPTIONS.
"""
from __future__ import annotations

import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_normalize import (  # noqa: E402
    GRAM_CODE_SPACE, K, NORMALIZER_VERSION, gram_codes, nfc, norm_stream,
    norm_stream_fast, project_span,
)

PROBE_SCRIPTS = os.environ.get(
    'GENIZAH_PROBE_SCRIPTS',
    r'C:\GenizahSearch\same_work_spike\probe\scripts')
TRANSCRIPTIONS = os.environ.get(
    'GENIZAH_TRANSCRIPTIONS', r'C:\GenizahSearch\Transcriptions.txt')

# Deliberately nasty: nikud, cantillation, the Judeo-Arabic upper dot, all five
# finals, brackets, maqaf, geresh/gershayim, curly quotes, Latin, digits, and
# several whitespace species including a non-breaking space and a newline.
FIXTURES = [
    'בְּרוּךֶ אַתָּה [יי] אלהינו מלך העולם, הזן אותנו צ̇מאן ואת־העולם כלו...',
    'ךםןףץ',
    'א\u0591ב\u05c1ג\u0307ד',
    'שלום\u00a0עולם\nשורה שניה\tטאב',
    'abc123 שלום ABC ...!?',
    'ר\u05f3 יוסי בן חלפתא ז\u05f4ל',
    'א”ב’ג',
    '',
    '   \n\t  ',
    'א',
    'אבגד',
    'אבגדה',
]

_FOLD_MAP = {ord('ך'): 'כ', ord('ם'): 'מ', ord('ן'): 'נ',
             ord('ף'): 'פ', ord('ץ'): 'צ'}


def _load(mod_name: str):
    """Load a research module by absolute path, or skip loudly."""
    path = os.path.join(PROBE_SCRIPTS, mod_name + '.py')
    if not os.path.exists(path):
        pytest.skip(f'research tree absent: {path} -- parity NOT verified here')
    if PROBE_SCRIPTS not in sys.path:
        sys.path.insert(0, PROBE_SCRIPTS)
    spec = importlib.util.spec_from_file_location('probe_' + mod_name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sample_pages(n_per_offset: int = 60, n_offsets: int = 8) -> list:
    """Page texts sampled at evenly spaced byte offsets across the corpus."""
    if not os.path.exists(TRANSCRIPTIONS):
        pytest.skip(f'corpus absent: {TRANSCRIPTIONS} -- parity NOT verified '
                    f'on real text')
    size = os.path.getsize(TRANSCRIPTIONS)
    pages = []
    with open(TRANSCRIPTIONS, 'rb') as fh:
        for i in range(n_offsets):
            fh.seek(int(size * i / n_offsets))
            fh.readline()  # discard the partial line we landed in
            blob = fh.read(400_000).decode('utf-8', errors='ignore')
            cur = []
            got = 0
            for line in blob.splitlines():
                if line.startswith('==>') and line.rstrip().endswith('<=='):
                    if cur:
                        pages.append('\n'.join(cur))
                        got += 1
                        if got >= n_per_offset:
                            break
                    cur = []
                else:
                    cur.append(line)
    assert pages, 'sampled no pages -- the corpus layout is not what we assume'
    return pages


# --------------------------------------------------------------------------
# Fixture behaviour: always runs, pins output literally.
# --------------------------------------------------------------------------

def test_normalizer_version_is_pinned():
    assert NORMALIZER_VERSION == 1


def test_finals_are_folded_not_dropped():
    assert norm_stream_fast('ךםןףץ') == 'כמנפצ'


def test_marks_punctuation_latin_digits_whitespace_all_dropped():
    got = norm_stream_fast(FIXTURES[0])
    assert got == 'ברוכאתהייאלהינומלכהעולמהזנאותנוצמאנואתהעולמכלו', got
    assert norm_stream_fast('abc123 שלום ABC ...!?') == 'שלומ'
    assert norm_stream_fast('   \n\t  ') == ''
    assert norm_stream_fast('א֑בׁג̇ד') == 'אבגד'
    assert norm_stream_fast('שלום עולם\nשורה שניה\tטאב') == \
        'שלומעולמשורהשניהטאב'


def test_geresh_gershayim_and_curly_quotes_dropped():
    assert norm_stream_fast('ר׳ יוסי בן חלפתא ז״ל') == 'ריוסיבנחלפתאזל'
    assert norm_stream_fast('א”ב’ג') == 'אבג'


@pytest.mark.parametrize('text', FIXTURES)
def test_fast_and_full_paths_agree(text):
    """The builder path and the display path must be the same function."""
    assert norm_stream_fast(text) == norm_stream(text)[0]


@pytest.mark.parametrize('text', FIXTURES)
def test_offsets_point_at_the_letter_they_claim(text):
    stream, offs = norm_stream(text)
    src = nfc(text).translate(_FOLD_MAP)
    assert len(offs) == len(stream)
    for i, ch in enumerate(stream):
        assert src[offs[i]] == ch, (i, ch, offs[i])


def test_project_span_recovers_original_wording():
    text = 'בְּרוּךֶ אַתָּה [יי] אלהינו'
    stream, offs = norm_stream(text)
    out = project_span(offs, 0, 7, nfc(text))
    assert out.startswith('בְּרוּךֶ'), out
    assert 'אַתָּה' in out, out


def test_project_span_edge_cases_do_not_raise():
    _, offs = norm_stream('אבגדה')
    assert project_span(offs, 0, 0, 'אבגדה') == ''
    assert project_span(offs, 9, 12, 'אבגדה') == ''
    assert project_span(offs, 3, 1, 'אבגדה') == ''
    _, empty_offs = norm_stream('')
    assert project_span(empty_offs, 0, 5, '') == ''


def test_gram_count_and_code_space():
    assert len(gram_codes('אבגד')) == 0          # shorter than K
    assert len(gram_codes('אבגדה')) == 1         # exactly K
    assert len(gram_codes('אבגדהו')) == 2
    stream = norm_stream_fast(FIXTURES[0])
    g = gram_codes(stream)
    assert len(g) == len(stream) - K + 1
    assert int(g.max()) < GRAM_CODE_SPACE
    assert GRAM_CODE_SPACE == 27 ** 5 == 14_348_907


# --------------------------------------------------------------------------
# Parity against the research implementation.
# --------------------------------------------------------------------------

@pytest.mark.parametrize('text', FIXTURES)
def test_fixture_parity_with_research_normalizer(text):
    probe = _load('normalize')
    exp_stream, exp_offs = probe.norm_stream(text)
    got_stream, got_offs = norm_stream(text)
    assert got_stream == exp_stream
    assert list(got_offs) == list(exp_offs)
    assert norm_stream_fast(text) == exp_stream


def test_corpus_parity_with_research_normalizer():
    """Stream, offsets and back-projection, on real pages from the corpus."""
    probe = _load('normalize')
    pages = _sample_pages()
    checked_letters = 0
    for text in pages:
        exp_stream, exp_offs = probe.norm_stream(text)
        got_stream, got_offs = norm_stream(text)
        assert got_stream == exp_stream, text[:120]
        assert list(got_offs) == list(exp_offs), text[:120]
        assert norm_stream_fast(text) == exp_stream, text[:120]
        if len(exp_stream) > 95:
            src = nfc(text)
            assert (project_span(got_offs, 5, 90, src)
                    == probe.project_span(exp_offs, 5, 90, src))
        checked_letters += len(exp_stream)
    # Guard against a silently tiny sample passing as green.
    assert len(pages) >= 50, f'only {len(pages)} pages sampled'
    assert checked_letters >= 20_000, f'only {checked_letters} letters compared'
    print(f'\nparity: {len(pages)} pages, {checked_letters:,} letters')


def test_corpus_parity_of_gram_codes():
    engine = _load('engine_np')
    pages = _sample_pages(n_per_offset=25, n_offsets=4)
    total = 0
    for text in pages:
        stream = norm_stream_fast(text)
        exp = engine._gram_codes(stream)
        got = gram_codes(stream)
        assert got.dtype == exp.dtype
        assert got.shape == exp.shape
        assert (got == exp).all(), text[:120]
        total += len(got)
    assert total >= 10_000, f'only {total} grams compared'
    print(f'\ngram parity: {len(pages)} pages, {total:,} grams')


def test_research_k_and_base_match_the_spec():
    engine = _load('engine_np')
    assert engine.K == K == 5
    assert int(engine.BASE) == 27
    assert engine.HEB_MIN == 0x05D0

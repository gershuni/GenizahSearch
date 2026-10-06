# -*- coding: utf-8 -*-
"""A stopped Lab search says so through the cut-off signal (time limit, 2026-10-07).

The website's time limit (Config.WEB_SEARCH_TIME_LIMIT) stops a search through the
worker's progress callback (shared/research_worker.py::_time_limited); the engine
returns what it had checked. Every consumer of the result -- /search's "N+", a
search within, the API's results_cut_off -- learns that the list is incomplete
from the cut-off signal (shared/search_engine.py::consume_last_search_cutoff).
lab_search returned its rows and said nothing, so a timed-out Lab search looked
complete; lab_composition_search said 'partial' only in its own payload.

These tests run the REAL LabEngine over a real (tiny) Lab index built by its own
rebuild_lab_index, and stop it from a progress callback at each of its stop
paths: the fast and the deep (batched) scan, and the My Library (LOCAL) loop.
"""
from __future__ import annotations

import gc
import re

import pytest

tantivy = pytest.importorskip("tantivy")

from shared import search_engine as se  # noqa: E402
from shared.config import Config  # noqa: E402

PHRASE = 'ויאמר משה אל העם אל תיראו התיצבו וראו את ישועת יהוה'
FILLER = 'ספר תורה נביאים וכתובים משנה ותלמוד מדרש ואגדה'
# A composition of many chunks (the Lab composition search reads 15 words at a time).
COMPOSITION = ' '.join([PHRASE, FILLER, PHRASE, FILLER, PHRASE, FILLER, PHRASE])
N_DOCS = 12


class _Meta:
    def extract_unique_id(self, line):
        m = re.search(r'IE\d+_P\d+_FL\d+', line)
        return m.group(0) if m else None

    def get_shelfmark_from_header(self, header):
        return header

    def get_display_data(self, header, source):
        return {'shelfmark': header, 'title': '', 'img': '1', 'source': source,
                'id': header.split('_')[0], 'library_code': ''}


@pytest.fixture(scope='module')
def lab(tmp_path_factory):
    from shared.lab_engine import LabEngine
    from shared.lab_settings import LabSettings
    root = tmp_path_factory.mktemp('lab_stop')
    corpus = root / 'Transcriptions.txt'
    with corpus.open('w', encoding='utf-8') as f:
        for n in range(N_DOCS):
            f.write(f'==> 99{n:016d}_IE{n}_P{n}_FL{n} <==\n{COMPOSITION}\n{FILLER} {n}\n')
    patches = {'LAB_INDEX_DIR': str(root / 'lab_index'), 'FILE_V8': str(corpus),
               'FILE_V7': str(root / 'none.txt'), 'LAB_WEIGHTS_FILE': str(root / 'none.json'),
               'LOCAL_LAB_INDEX_DIR': str(root / 'no_local_lab')}
    saved = {name: getattr(Config, name) for name in patches}
    saved_load = LabSettings.load
    LabSettings.load = lambda self: None
    try:
        for name, value in patches.items():
            setattr(Config, name, value)
        engine = LabEngine(_Meta(), None, settings=LabSettings())
        assert engine.rebuild_lab_index() == N_DOCS
        assert engine.lab_searcher is not None
        yield engine
    finally:
        LabSettings.load = saved_load
        for name, value in saved.items():
            setattr(Config, name, value)
    engine = None
    gc.collect()


@pytest.fixture
def local_lab(lab):
    """The same index as the engine's My Library (LOCAL) Lab index."""
    lab._local_lab_index, lab.local_lab_searcher = lab.lab_index, lab.lab_searcher
    yield lab
    lab._local_lab_index = lab.local_lab_searcher = None


def _stop_after(n_numeric=None, n_text=None):
    """A progress callback that raises the engine's Stop on its n-th numeric
    (current, total) call, or on its n-th text-status call (the deep scan's)."""
    seen = {'numeric': 0, 'text': 0}

    def progress(*args):
        kind = 'text' if args and isinstance(args[0], str) else 'numeric'
        seen[kind] += 1
        limit = n_text if kind == 'text' else n_numeric
        if limit is not None and seen[kind] >= limit:
            raise InterruptedError('Search time limit reached')
    return progress


def _run(call):
    se.consume_last_search_cutoff()
    result = call()
    return result, se.consume_last_search_cutoff()


def test_a_whole_lab_search_is_not_cut_off(lab):
    rows, cutoff = _run(lambda: lab.lab_search(PHRASE, progress_callback=lambda *a: None))
    assert len(rows) == N_DOCS
    assert cutoff == {'capped': False, 'interrupted': False}


def test_a_stopped_lab_search_keeps_its_rows_and_says_it_was_stopped(lab):
    # The fast scan ticks every fifth document: stopped at the second tick, the
    # first five are kept.
    rows, cutoff = _run(lambda: lab.lab_search(PHRASE, progress_callback=_stop_after(n_numeric=2)))
    assert 0 < len(rows) < N_DOCS
    assert cutoff['interrupted'] is True


def test_a_stopped_deep_lab_search_says_it_was_stopped(lab):
    rows, cutoff = _run(lambda: lab.lab_search(PHRASE, deep_scan=True,
                                               progress_callback=_stop_after(n_text=1)))
    assert isinstance(rows, list)
    assert cutoff['interrupted'] is True


def test_a_stopped_my_library_lab_search_says_it_was_stopped(local_lab):
    rows, cutoff = _run(lambda: local_lab.lab_search(PHRASE, corpus_scope='local',
                                                     progress_callback=_stop_after(n_numeric=2)))
    assert 0 < len(rows) < N_DOCS
    assert all(r['display']['source'] == 'LOCAL' for r in rows)
    assert cutoff['interrupted'] is True


def test_a_whole_lab_composition_search_is_not_cut_off(lab):
    result, cutoff = _run(lambda: lab.lab_composition_search(COMPOSITION, progress_callback=lambda *a: None))
    assert result['main'] and result['partial'] is False
    assert cutoff == {'capped': False, 'interrupted': False}


@pytest.mark.parametrize('deep', [False, True])
def test_a_stopped_lab_composition_search_keeps_its_rows_and_says_so(lab, deep):
    # Stopped at the third chunk: the first two chunks' matches are kept.
    stop = _stop_after(n_text=3) if deep else _stop_after(n_numeric=3)
    result, cutoff = _run(lambda: lab.lab_composition_search(COMPOSITION, deep_scan=deep,
                                                             progress_callback=stop))
    assert result['partial'] is True
    assert result['main'], 'the matches of the chunks searched before the stop are kept'
    assert cutoff['interrupted'] is True


def test_a_stopped_my_library_lab_composition_search_says_so(local_lab):
    result, cutoff = _run(lambda: local_lab.lab_composition_search(
        COMPOSITION, corpus_scope='local', progress_callback=_stop_after(n_numeric=3)))
    assert result['partial'] is True
    assert cutoff['interrupted'] is True

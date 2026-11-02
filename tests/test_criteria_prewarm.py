# -*- coding: utf-8 -*-
"""The Manuscript features counts, warmed when the server starts (web/main.py
calls web/components/criteria_panel.py::prewarm_criteria in its background
pre-warm, v9.13.0): afterwards every value list and "recorded for N" count the
panel asks for comes from the cache, with no query, so the first visitor after
a restart does not wait 1-8 s per section.

Over the real FjmsService and the criteria fixture sidecar (tests/criteria_sidecar.py).
"""
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import UNIVERSE, build_criteria_sidecar  # noqa: E402

from shared import fjms_service  # noqa: E402
from shared.catalog_criteria import CRITERIA  # noqa: E402
from shared.fjms_service import FilterUnavailable, FjmsService  # noqa: E402
from web.components.criteria_panel import prewarm_criteria  # noqa: E402


class _Meta:
    """MetadataManager.corpus_sys_ids: one frozenset, or FilterUnavailable before libraries.csv is read."""

    def __init__(self, corpus):
        self.corpus = corpus
        self.timeouts = []

    def corpus_sys_ids(self, timeout=30.0):
        self.timeouts.append(timeout)
        if self.corpus is None:
            raise FilterUnavailable('corpus_unavailable', 'the list of all manuscripts is not loaded yet')
        return self.corpus


@pytest.fixture(scope='module')
def new_file(tmp_path_factory):
    return build_criteria_sidecar(tmp_path_factory.mktemp('w') / 'f.db')


def _panel_reads(svc, universe):
    """What the panel asks for, section by section: each criterion's coverage
    with the corpus note beside it ("recorded for N of M", _load_coverage)
    and, for a list, its values, for every source it offers; then the note
    under the total (_load_note)."""
    for criterion in CRITERIA:
        for source in ('either', *criterion.sources):
            svc.get_criterion_coverage(criterion.key, universe, source)
            svc.criteria_corpus_note(universe)
            if criterion.kind == 'list':
                svc.get_criterion_values(criterion.key, universe, source)
    svc.criteria_corpus_note(universe)


def _no_query(*_a, **_k):
    pytest.fail('the panel queried the sidecar after the warm-up')


def test_after_the_warm_up_the_panel_runs_no_query(new_file, monkeypatch):
    svc = FjmsService(db_path=new_file)
    meta = _Meta(UNIVERSE)
    assert prewarm_criteria(svc, meta) is True
    # it waits for libraries.csv as long as the start needs, not the panel's 30 s
    assert meta.timeouts == [300.0]
    monkeypatch.setattr(fjms_service, 'criterion_values', _no_query)
    monkeypatch.setattr(fjms_service, '_recorded_count', _no_query)
    # the corpus note's width query (GitHub Codex on PR 461)
    monkeypatch.setattr(svc, 'get_filter_sys_ids', _no_query)
    _panel_reads(svc, meta.corpus_sys_ids)       # the panel hands the service the callable


def _counted(calls, real, pause=0.2):
    def slow(*a, **k):
        calls.append(1)
        time.sleep(pause)                        # long enough for the others to arrive
        return real(*a, **k)
    return slow


def _together(read, n=4):
    """``read()`` from n threads at once; their answers."""
    answers, errors = [], []

    def run():
        try:
            answers.append(read())
        except Exception as e:                   # pragma: no cover - reported below
            errors.append(e)
    threads = [threading.Thread(target=run) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert not errors, errors
    assert len(answers) == n
    return answers


@pytest.mark.parametrize('read', ['coverage', 'values', 'note'])
def test_readers_during_the_warm_up_share_one_build(new_file, monkeypatch, read):
    """A visitor who opens the panel while the start is still building it
    waits for that build rather than running the same corpus-wide query
    beside it (GitHub Codex on PR 461): one query, however many ask."""
    svc = FjmsService(db_path=new_file)
    svc.catalogued_ids()                         # cached already; not what is counted
    calls = []
    if read == 'coverage':
        monkeypatch.setattr(fjms_service, '_recorded_count',
                            _counted(calls, fjms_service._recorded_count))
        answers = _together(lambda: svc.get_criterion_coverage('material', UNIVERSE))
    elif read == 'values':
        monkeypatch.setattr(fjms_service, 'criterion_values',
                            _counted(calls, fjms_service.criterion_values))
        answers = _together(lambda: svc.get_criterion_values('material', UNIVERSE))
    else:
        monkeypatch.setattr(svc, 'get_filter_sys_ids', _counted(calls, svc.get_filter_sys_ids))
        answers = _together(lambda: svc.criteria_corpus_note(UNIVERSE))
    assert len(calls) == 1
    assert all(a == answers[0] for a in answers)


def test_without_the_warm_up_the_panel_queries(new_file, monkeypatch):
    """The check above can fail: a cold service does query."""
    svc = FjmsService(db_path=new_file)
    monkeypatch.setattr(fjms_service, 'criterion_values', _no_query)
    monkeypatch.setattr(fjms_service, '_recorded_count', _no_query)
    with pytest.raises(pytest.fail.Exception):
        _panel_reads(svc, _Meta(UNIVERSE).corpus_sys_ids)


def test_nothing_to_warm_before_the_metadata_or_the_corpus(new_file):
    svc = FjmsService(db_path=new_file)
    assert prewarm_criteria(svc, None) is False
    with pytest.raises(FilterUnavailable):        # web/main.py logs it; the panel computes on first use
        prewarm_criteria(svc, _Meta(None))

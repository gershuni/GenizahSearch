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
    and, for a list, its values, for every source it offers."""
    for criterion in CRITERIA:
        for source in ('either', *criterion.sources):
            svc.get_criterion_coverage(criterion.key, universe, source)
            if criterion.kind == 'list':
                svc.get_criterion_values(criterion.key, universe, source)


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
    _panel_reads(svc, meta.corpus_sys_ids)       # the panel hands the service the callable


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

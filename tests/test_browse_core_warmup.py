"""Browse core budget during cold start.

2026-09-02: the first ``get_browse_page`` in a fresh process cost 20 s on the dev
box (cold browse map: 133 MB pickle + repair scan), later calls 20-110 ms. Every
``/api/browse`` request in that window burned the 2 s core budget and answered
504 ``core_timeout`` -- with a full traceback per request in the log -- and the
requests queued behind the loading thread 504'd one after another.

``_fetch_core`` now widens the budget while the provider reports it is not warm,
and logs the timeout as one WARNING line.
"""
from __future__ import annotations

import asyncio
import logging
import time

import pytest

from shared import browse_service as bs


class _Provider:
    def __init__(self, warm, delay):
        self._warm = warm
        self._delay = delay
        self.calls = 0

    def is_warm(self):
        return self._warm

    def get_browse_page(self, sys_id, p_num=None, volume_ie=None):
        self.calls += 1
        time.sleep(self._delay)
        return {"uid": f"{sys_id}_P{p_num}", "text": "x"}

    def get_browse_page_by_fl(self, fl_id, sys_id=None):
        return self.get_browse_page(sys_id, 1)


class _LegacyProvider(_Provider):
    """A provider without ``is_warm`` -- must keep the old strict budget."""
    is_warm = None  # attribute exists but is not callable -> getattr returns None


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def _fresh_executor():
    bs.reset_browse_executor_state()
    yield
    bs.reset_browse_executor_state()


def test_cold_provider_gets_the_warmup_budget(monkeypatch):
    monkeypatch.setenv("SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT", "2.0")
    prov = _Provider(warm=False, delay=0.3)
    page = _run(bs._fetch_core(prov, "990000000000000001", 2, None, None, timeout=0.05))
    assert page == {"uid": "990000000000000001_P2", "text": "x"}


def test_warm_provider_keeps_the_strict_budget(monkeypatch):
    monkeypatch.setenv("SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT", "2.0")
    prov = _Provider(warm=True, delay=0.3)
    with pytest.raises(bs.APIError) as ei:
        _run(bs._fetch_core(prov, "990000000000000002", 2, None, None, timeout=0.05))
    assert ei.value.http_status == 504
    assert "core_timeout" in str(ei.value.code if hasattr(ei.value, "code") else ei.value)


def test_provider_without_is_warm_is_treated_as_warm(monkeypatch):
    monkeypatch.setenv("SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT", "2.0")
    prov = _LegacyProvider(warm=False, delay=0.3)
    with pytest.raises(bs.APIError):
        _run(bs._fetch_core(prov, "990000000000000003", 2, None, None, timeout=0.05))


def test_warmup_budget_never_shrinks_the_configured_budget(monkeypatch):
    # warm-up value smaller than the normal budget -> normal budget stays.
    monkeypatch.setenv("SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT", "0.01")
    prov = _Provider(warm=False, delay=0.1)
    page = _run(bs._fetch_core(prov, "990000000000000004", 2, None, None, timeout=1.0))
    assert page is not None


def test_timeout_logs_one_warning_line_without_traceback(caplog):
    prov = _Provider(warm=True, delay=0.3)
    with caplog.at_level(logging.WARNING, logger=bs.logger.name):
        with pytest.raises(bs.APIError):
            _run(bs._fetch_core(prov, "990000000000000005", 2, None, None, timeout=0.05))
    recs = [r for r in caplog.records if "core_timeout" in r.getMessage()]
    assert len(recs) == 1
    assert recs[0].levelno == logging.WARNING
    assert recs[0].exc_info is None, "an expected, handled timeout must not dump a traceback"
    assert "990000000000000005" in recs[0].getMessage()


def test_web_provider_reports_warm_state_from_the_shared_browse_map(monkeypatch):
    from shared.search_engine import SearchEngine
    from web import services
    from web.state import state

    svc = services.GenizahService.__new__(services.GenizahService)
    monkeypatch.setattr(state, "searcher", object(), raising=False)
    monkeypatch.setattr(SearchEngine, "_shared_browse_map", None, raising=False)
    assert svc.is_warm() is False
    monkeypatch.setattr(SearchEngine, "_shared_browse_map", {"x": []}, raising=False)
    assert svc.is_warm() is True
    monkeypatch.setattr(state, "searcher", None, raising=False)
    assert svc.is_warm() is True  # nothing to wait for

# -*- coding: utf-8 -*-
"""SEED-021 — image-fetch observability + viewer-polish audit tests.

Covers:
  #23 — fetch_fl_ids_from_nli persists a LOCK-CONSISTENT cache snapshot
        (the snapshot is copied inside the same critical section as the update,
        passed into _persist_positive_cache_snapshot).
  #36 — nli_image logs non-200/non-429/non-5xx statuses (404 / unexpected 4xx)
        at debug for both the IIIF and Rosetta fetch handlers.
  #37 — resolve_external_images warning includes exc_info (stack trace).
  #38 — resolve_external_images warns when invoked on the asyncio event loop.

State isolation for the web.api closures is provided by the module fixtures
below (mirrors tests/test_api_nli_breaker_integration.py).
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import patch

import pytest
from fastapi import FastAPI


# ─── web.api closure seam fixtures (mirrors test_api_nli_breaker_integration) ────

class _MockResponse:
    def __init__(self, status_code=200, content=b'', headers=None, text='', json_data=None):
        self.status_code = status_code
        self.content = content
        self.headers = headers or {}
        self.text = text
        self._json_data = json_data or {}

    def json(self):
        return self._json_data


@pytest.fixture(scope='module', autouse=True)
def _initialize_api_routes():
    from web.api import init_api_routes
    bare = FastAPI()
    init_api_routes(app_override=bare)
    yield


@pytest.fixture(autouse=True)
def _clear_nli_in_memory_cache():
    from web.api import _api_test_seam
    fn = _api_test_seam.get('fetch_fl_ids_from_nli')
    if fn is not None and fn.__closure__:
        closure_vars = dict(zip(fn.__code__.co_freevars, fn.__closure__))
        nli_cache = closure_vars.get('_nli_cache')
        nli_cache_time = closure_vars.get('_nli_cache_time')
        nli_cache_lock = closure_vars.get('_nli_cache_lock')
        if nli_cache is not None and nli_cache_time is not None and nli_cache_lock is not None:
            with nli_cache_lock.cell_contents:
                nli_cache.cell_contents.clear()
                nli_cache_time.cell_contents.clear()
    yield


def _seam(name):
    from web.api import _api_test_seam
    return _api_test_seam[name]


# ─── #36 — nli_image non-200/non-429/non-5xx status logging ─────────────────────

class TestNliImageStatusLogging:
    """nli_image previously fell through 404/unexpected statuses silently."""

    def test_iiif_404_logs_debug_and_falls_through(self, caplog):
        nli_image = _seam('nli_image')
        # IIIF 404 → debug log + fall through to Rosetta (also 404 here → final 404).
        responses = [
            _MockResponse(status_code=404),  # IIIF
            _MockResponse(status_code=404),  # Rosetta
        ]

        def _fake_get(url, *a, **kw):
            return responses.pop(0)

        with caplog.at_level(logging.DEBUG, logger='web.api'):
            with patch('web.api.requests.get', side_effect=_fake_get):
                resp = nli_image('12345')

        assert resp.status_code == 404
        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert 'IIIF non-success status 404' in msgs
        assert 'Rosetta non-success status 404' in msgs

    def test_iiif_unexpected_3xx_logged(self, caplog):
        nli_image = _seam('nli_image')
        responses = [
            _MockResponse(status_code=302),  # IIIF unexpected redirect status
            _MockResponse(status_code=404),  # Rosetta
        ]

        def _fake_get(url, *a, **kw):
            return responses.pop(0)

        with caplog.at_level(logging.DEBUG, logger='web.api'):
            with patch('web.api.requests.get', side_effect=_fake_get):
                nli_image('99999')

        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert 'IIIF non-success status 302' in msgs

    def test_429_and_5xx_do_not_emit_fallthrough_debug(self, caplog):
        """429/5xx are recorded as breaker failures, NOT the new fall-through log."""
        nli_image = _seam('nli_image')
        responses = [
            _MockResponse(status_code=429),  # IIIF — breaker failure path
            _MockResponse(status_code=503),  # Rosetta — breaker failure path
        ]

        def _fake_get(url, *a, **kw):
            return responses.pop(0)

        with caplog.at_level(logging.DEBUG, logger='web.api'):
            with patch('web.api.requests.get', side_effect=_fake_get):
                nli_image('55555')

        msgs = " ".join(r.getMessage() for r in caplog.records)
        assert 'non-success status' not in msgs


# ─── #23 — lock-consistent persisted snapshot ───────────────────────────────────

class TestPersistSnapshotConsistency:
    """The persisted snapshot must reflect the cache AT update time, copied
    inside the same critical section — not a re-locked read that could
    interleave with a concurrent mutation."""

    def test_snapshot_reflects_update_under_lock(self):
        captured = {}

        def _capture_save(cache_snapshot, cache_time_snapshot):
            captured['cache'] = dict(cache_snapshot)
            captured['time'] = dict(cache_time_snapshot)

        manifest = _MockResponse(
            status_code=200,
            json_data={'sequences': [{'canvases': [
                {'images': [{'resource': {'service': {
                    '@id': 'https://iiif.nli.org.il/IIIFv21/FL7734473/info.json'}}}]},
            ]}]},
        )
        with patch('web.api._save_nli_persistent_cache', side_effect=_capture_save):
            # fetch_fl_ids_from_nli fetches the IIIF manifest via _nli_session.get.
            with patch('web.api._nli_session.get', return_value=manifest):
                fl_ids = _seam('fetch_fl_ids_from_nli')('sysid_snap_test')

        assert fl_ids == ['7734473']
        # The persisted snapshot must already contain the just-resolved entry —
        # proving the snapshot was copied AFTER the in-lock update (consistent).
        assert captured['cache'].get('sysid_snap_test') == ['7734473']
        assert 'sysid_snap_test' in captured['time']

    def test_persist_helper_accepts_passed_snapshot(self):
        """The writer accepts an explicit pre-copied snapshot (the consistent path)."""
        captured = {}

        def _capture_save(cache_snapshot, cache_time_snapshot):
            captured['cache'] = cache_snapshot
            captured['time'] = cache_time_snapshot

        snap = ({'k': ['1', '2']}, {'k': 123.0})
        with patch('web.api._save_nli_persistent_cache', side_effect=_capture_save):
            _seam('_persist_positive_cache_snapshot')(snap)

        assert captured['cache'] == {'k': ['1', '2']}
        assert captured['time'] == {'k': 123.0}

    def test_persist_helper_self_locks_when_no_snapshot(self):
        """Backward-compat: called with no arg, it still takes its own copy."""
        captured = {}

        def _capture_save(cache_snapshot, cache_time_snapshot):
            captured['called'] = True

        with patch('web.api._save_nli_persistent_cache', side_effect=_capture_save):
            _seam('_persist_positive_cache_snapshot')()

        assert captured.get('called') is True


# ─── #37 / #38 — resolve_external_images observability ──────────────────────────

class _FakeMetaMgrRaises:
    def __init__(self):
        self.nli_cache: dict = {}

    def enrich_metadata(self, sys_id):
        raise RuntimeError("NLI unreachable")


class TestResolveExternalImagesObservability:
    def test_enrich_failure_warning_has_exc_info(self, caplog):
        """#37: the degraded-path warning must carry stack info (exc_info)."""
        from web.components.image_resolution import resolve_external_images

        mgr = _FakeMetaMgrRaises()
        with caplog.at_level(logging.WARNING, logger='web.components.image_resolution'):
            result = resolve_external_images('990025143260205171', meta_mgr=mgr)

        # Degrades gracefully.
        assert result['cambridge_images'] == []
        # The warning record must include exception info (traceback attached).
        warn_records = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and 'enrich_metadata error' in r.getMessage()
        ]
        assert warn_records, "expected an enrich_metadata error warning"
        assert any(r.exc_info is not None for r in warn_records), (
            "the enrich_metadata warning must attach exc_info (#37)"
        )

    def test_warns_when_called_on_event_loop(self, caplog):
        """#38: invoking on the asyncio event loop thread emits a warning."""
        from web.components.image_resolution import resolve_external_images

        mgr = _FakeMetaMgrRaises()

        async def _run_on_loop():
            with caplog.at_level(logging.WARNING, logger='web.components.image_resolution'):
                # Runs synchronously on the running event loop → guard must fire.
                resolve_external_images('990025143260205171', meta_mgr=mgr)
            return [r.getMessage() for r in caplog.records]

        messages = asyncio.run(_run_on_loop())
        joined = " ".join(messages)
        assert 'event-loop thread' in joined, (
            "resolve_external_images must warn when run on the event loop (#38)"
        )

    def test_no_event_loop_warning_off_loop(self, caplog):
        """#38: when NOT on the event loop (normal sync/io_bound), no guard warning."""
        from web.components.image_resolution import resolve_external_images

        mgr = _FakeMetaMgrRaises()
        with caplog.at_level(logging.WARNING, logger='web.components.image_resolution'):
            resolve_external_images('990025143260205171', meta_mgr=mgr)

        joined = " ".join(r.getMessage() for r in caplog.records)
        assert 'event-loop thread' not in joined


# ─── #42 / #43 — desktop join_workbench viewer polish (source-level) ─────────────
# GUI construction segfaults headless on Windows (see memory
# feedback_full_suite_testing_windows); assert against the source instead.

import pathlib  # noqa: E402

_JW_SRC = (
    pathlib.Path(__file__).resolve().parent.parent / 'desktop' / 'join_workbench.py'
).read_text(encoding='utf-8')


class TestDesktopViewerPolishSource:
    def test_compare_pane_has_zoom_percent_label(self):
        """#42: the Compare pane builds a zoom-% label wired into the pane dict."""
        assert '"zoom_lbl": zoom_lbl' in _JW_SRC
        # The label is updated to a percentage on render.
        assert 'zoom_lbl.setText(f"{int(round(z * 100))}%")' in _JW_SRC

    def test_zoom_label_updated_in_render_pane_image(self):
        """#42: render path keeps the zoom-% label in sync on each zoom change."""
        idx = _JW_SRC.index('def _render_pane_image')
        body = _JW_SRC[idx:idx + 1200]
        assert 'zoom_lbl' in body and 'setText' in body

    def test_candidate_card_has_animated_loading_placeholder(self):
        """#43: candidate card runs a QTimer-driven animated loading placeholder."""
        assert '_loading_timer' in _JW_SRC
        assert 'def _tick_loading_anim' in _JW_SRC
        assert 'def _start_loading_anim' in _JW_SRC
        # Animation reuses the existing tr("loading…") key (no new untranslated key).
        assert 'base = tr("loading…")' in _JW_SRC

    def test_qtimer_imported(self):
        """#43: QTimer must be imported for the animator to construct."""
        assert 'QTimer' in _JW_SRC

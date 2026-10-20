# -*- coding: utf-8 -*-
"""crash_log.txt is written to the data folder, not next to the program.

An installed copy lives under Program Files, which is read-only; the write failed
and was swallowed, so with telemetry declined an unhandled exception left no
trace. The file now goes to Config.INDEX_DIR, next to faulthandler_dump.txt.
"""
from __future__ import annotations

import os
import sys
import traceback

import genizah_app


def _fire_crash_hook(monkeypatch, index_dir):
    monkeypatch.setattr(genizah_app.Config, 'INDEX_DIR', str(index_dir))
    original = sys.excepthook
    try:
        genizah_app._setup_crash_handler()
        # Keep the console quiet: only the file write is under test.
        monkeypatch.setattr(sys, '__excepthook__', lambda *a: None)
        monkeypatch.setattr(traceback, 'print_exception', lambda *a, **k: None)
        try:
            raise ValueError('boom for the crash log')
        except ValueError as exc:
            sys.excepthook(type(exc), exc, exc.__traceback__)
    finally:
        sys.excepthook = original


def test_the_crash_log_goes_to_the_data_folder(monkeypatch, tmp_path):
    _fire_crash_hook(monkeypatch, tmp_path)
    log = tmp_path / 'crash_log.txt'
    assert log.exists()
    text = log.read_text(encoding='utf-8')
    assert 'boom for the crash log' in text
    assert 'ValueError' in text


def test_nothing_is_written_next_to_the_program(monkeypatch, tmp_path):
    beside = os.path.join(os.path.dirname(genizah_app.__file__), 'crash_log.txt')
    before = os.path.getsize(beside) if os.path.exists(beside) else None
    _fire_crash_hook(monkeypatch, tmp_path)
    after = os.path.getsize(beside) if os.path.exists(beside) else None
    assert after == before

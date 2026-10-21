# -*- coding: utf-8 -*-
"""The desktop's Source (מקור) column shows when the rows come from more than
one source, decided from the rows (OPEN_ISSUES P2 "Desktop hides the Source
column although the index holds V0.7", 2026-10-21).

It was decided by whether AllGenizah_OLD.txt sits beside the exe. The
installed app's folder has no such file, but the index it opens was built
from the repo's copy and returns V0.7 pages (44 of 405 for `*כהנים *גדולים`),
which then had no visible marker.

Driven through GenizahGUI.on_search_finished on the stub window of
tests/test_exclusion_surfaces.py.
"""
from __future__ import annotations

import pytest

from shared.config import Config
from tests.test_exclusion_surfaces import (A, B, _personal_state_in_tmp,  # noqa: F401
                                           _res, _search, slot_errors, window)

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6


def _with_source(row, source):
    row["display"] = dict(row["display"], source=source)
    return row


def _v07_file(monkeypatch, tmp_path, present):
    path = tmp_path / "AllGenizah_OLD.txt"
    if present:
        path.write_text("==> header <==\ntext\n", encoding="utf-8")
    monkeypatch.setattr(Config, "FILE_V7", str(path))


def test_v07_rows_show_the_column_with_no_v07_file(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    _v07_file(monkeypatch, tmp_path, present=False)
    _search(window, [_with_source(_res(A, 1), "V0.7"), _res(B, 1)])
    assert not window.results_table.isColumnHidden(window.COL_SRC)


def test_v08_rows_alone_hide_the_column_beside_a_v07_file(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    _v07_file(monkeypatch, tmp_path, present=True)
    _search(window, [_res(A, 1), _res(B, 1)])
    assert window.results_table.isColumnHidden(window.COL_SRC)


def test_local_rows_still_show_the_column(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    _v07_file(monkeypatch, tmp_path, present=False)
    _search(window, [_with_source(_res(A, 1), "LOCAL"), _res(B, 1)])
    assert not window.results_table.isColumnHidden(window.COL_SRC)


def test_the_next_search_decides_again(window, monkeypatch, tmp_path):  # noqa: F811 (the imported fixture)
    _v07_file(monkeypatch, tmp_path, present=False)
    _search(window, [_with_source(_res(A, 1), "V0.7")])
    _search(window, [_res(B, 1)])
    assert window.results_table.isColumnHidden(window.COL_SRC)

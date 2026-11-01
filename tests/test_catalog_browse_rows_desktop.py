# -*- coding: utf-8 -*-
"""Desktop Catalog tab rows (PR 1a of docs/plans/CATALOGUE_CRITERIA_PLAN.md):
the date column shows the copy span over a manuscript's records ('944–1100',
D18), an undated manuscript an empty cell, and a row from before the span
existed its start year."""
from types import SimpleNamespace

import pytest

pytestmark = pytest.mark.gui  # imports PyQt6: gui bucket only

from PyQt6.QtWidgets import QApplication, QLabel, QPushButton, QTableWidget  # noqa: E402

import genizah_app  # noqa: E402

_APP = QApplication.instance() or QApplication([])
GUI = genizah_app.GenizahGUI


# The tab's column numbers and page size, as GenizahGUI's Catalog tab sets them.
COLUMNS = dict(_CAT_COL_ACTIONS=0, _CAT_COL_SHELF=1, _CAT_COL_LIBRARY=2,
               _CAT_COL_DOMAIN=3, _CAT_COL_IDENT=4, _CAT_COL_DATE=5)


def _host():
    table = QTableWidget()
    table.setColumnCount(len(COLUMNS))
    return SimpleNamespace(
        catalog_results_table=table, meta_mgr=None,
        _CATALOG_PAGE_SIZE=50, _catalog_current_page=0, **COLUMNS,
        _catalog_count_label=QLabel(), _catalog_page_label=QLabel(),
        _catalog_prev_btn=QPushButton(), _catalog_next_btn=QPushButton(),
        _create_action_button=lambda text, tip, callback, parent=None: QPushButton(text),
        _catalog_update_chips=lambda: None, _catalog_update_text_summary=lambda: None,
    )


def test_the_date_column_shows_the_copy_span():
    host = _host()
    rows = [
        {'sys_id': '990000000010205171', 'copy_date': '944', 'copy_span': '944–1100'},
        {'sys_id': '990000000020205171', 'copy_date': '-99', 'copy_span': ''},
        {'sys_id': '990000000030205171', 'copy_date': '1200'},
    ]
    GUI._catalog_on_async_refresh_done(host, {'data': {'results': rows, 'total': 3}})
    table = host.catalog_results_table
    assert [table.item(i, COLUMNS['_CAT_COL_DATE']).text() for i in range(3)] == ['944–1100', '', '1200']

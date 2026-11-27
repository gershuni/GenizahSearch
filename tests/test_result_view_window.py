# -*- coding: utf-8 -*-
"""GS-9 in the window: the Search tab's filters, counts, "Load more" and export decide over
the whole result list, not the rows loaded so far (desktop/result_view.py).

A 1,200-result search loads 500 rows. A filter read the table, so a match at result 900
did not show, the counts said what the loaded rows said, and Export wrote the loaded rows.
"""
import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtTest import QTest

import genizah_app as app
from genizah_app import tr
from tests.test_exclusion_surfaces import (_personal_state_in_tmp, _search, slot_errors,  # noqa: F401
                                           window)

pytestmark = pytest.mark.gui  # imports genizah_app, which imports PyQt6


def _sid(i):
    return f"99{i:016d}"


def _r(i, page=1):
    sid = _sid(i)
    return {"display": {"id": sid, "source": "V0.8", "img": str(page), "shelfmark": "", "title": ""},
            "snippet": f"*word* {i}/{page}", "full_text": "text", "uid": f"{sid}_{page}",
            "raw_header": f"{sid}_P{page}", "raw_file_hl": "", "highlight_pattern": "word",
            "scope": "genizah", "score": 1.0}


def _many(n):
    return [_r(i) for i in range(n)]


def _drawn(w):
    t = w.results_table
    return [t.item(r, w.COL_SYS_ID).text() for r in range(t.rowCount()) if not t.isRowHidden(r)]


def _showing(visible, total):
    return tr("Showing {} of {} results").format(visible, total)


def test_a_match_beyond_the_loaded_rows_shows_and_is_counted(window):  # noqa: F811 (the imported fixture)
    """The only result whose shelfmark matches is result 900 (the test metadata names
    result i "T-S <last three digits of its id>")."""
    w = window
    _search(w, _many(1200))
    assert w.results_table.rowCount() <= 500 and w.results_loaded < 1200
    w.results_filters = {w.COL_SHELF: {"text": "T-S 900"}}
    w._apply_results_table_filters()
    assert _drawn(w) == [_sid(900)], "the match beyond the loaded rows is not in the table"
    assert w.status_label.text().startswith(_showing(1, 1200))


def test_a_filter_set_before_the_search_never_leaves_an_empty_table(window):  # noqa: F811 (the imported fixture)
    w = window
    w.results_filters = {w.COL_SNIPPET: {"text": "/7"}}
    _search(w, [_r(i) for i in range(600)] + [_r(600, page=7)])
    assert _drawn(w) == [_sid(600)]
    assert w.status_label.text().startswith(_showing(1, 601))


def test_export_writes_every_kept_result_not_only_the_loaded_rows(window):  # noqa: F811 (the imported fixture)
    w = window
    w.excluded_sys_ids = {_sid(3)}
    _search(w, _many(1200))
    assert w.results_table.rowCount() < 1200
    rows, ticked = w._results_to_export()
    assert not ticked and len(rows) == 1199
    assert [r["uid"] for r in rows] == [r["uid"] for r in _many(1200) if r["display"]["id"] != _sid(3)]


def test_ticked_rows_are_what_export_writes(window):  # noqa: F811 (the imported fixture)
    w = window
    _search(w, _many(30))
    w.results_table.item(4, w.COL_CHECKBOX).setCheckState(Qt.CheckState.Checked)
    rows, ticked = w._results_to_export()
    assert ticked and [r["uid"] for r in rows] == [_r(4)["uid"]]


def test_a_loosened_filter_draws_again_and_keeps_the_ticks(window, monkeypatch):  # noqa: F811 (the imported fixture)
    """Codex F4 on the plan: building the table again must not untick what was ticked."""
    monkeypatch.setattr(app, "BATCH_SIZE", 20)
    w = window
    w.results_filters = {w.COL_SNIPPET: {"text": "word* 1", "exclude": True}}   # results 1 and 10-19 out
    _search(w, _many(30))
    assert _sid(1) not in _drawn(w) and _sid(25) in _drawn(w)
    for sid in (_sid(2), _sid(25)):
        row = _drawn(w).index(sid)
        w.results_table.item(row, w.COL_CHECKBOX).setCheckState(Qt.CheckState.Checked)
    w.results_filters = {}
    w._apply_results_table_filters()
    assert _drawn(w)[:3] == [_sid(0), _sid(1), _sid(2)], "the results let in again are not in order"
    ticked = [w.results_table.item(r, w.COL_SYS_ID).text() for r in range(w.results_table.rowCount())
              if w.results_table.item(r, w.COL_CHECKBOX).checkState() == Qt.CheckState.Checked]
    assert ticked == [_sid(2), _sid(25)]


def test_a_shelfmark_filter_waits_for_metadata_not_yet_fetched(window):  # noqa: F811 (the imported fixture)
    """Codex F3 on the plan: a result whose shelfmark is not known yet passes, so it is
    drawn and its fetch starts; the fetched shelfmark then decides."""
    w = window
    unknown = {_sid(1), _sid(2)}
    real = w.meta_mgr.get_meta_for_id
    w.meta_mgr.get_meta_for_id = lambda sid: (
        ("Unknown", "") if sid in unknown and sid not in w.meta_mgr.nli_cache
        else (w.meta_mgr.nli_cache[sid]["shelfmark"], "") if sid in w.meta_mgr.nli_cache else real(sid))
    w.start_metadata_loading = lambda ids: None
    w.results_filters = {w.COL_SHELF: {"text": "ENA"}}
    _search(w, _many(5))
    assert _drawn(w) == [_sid(1), _sid(2)], "a result with no metadata yet was dropped unfetched"
    assert w._results_to_export()[0] == [], "drawn rows not yet known to match were exported"
    w.results_table.item(0, w.COL_CHECKBOX).setCheckState(Qt.CheckState.Checked)   # Codex round 2 on #150
    assert w._results_to_export() == ([], True), "a ticked row still loading was exported"
    w.results_table.item(0, w.COL_CHECKBOX).setCheckState(Qt.CheckState.Unchecked)
    w.meta_mgr.nli_cache.update({_sid(1): {"shelfmark": "ENA 1"}, _sid(2): {"shelfmark": "T-S 2"}})
    for sid in unknown:
        w._result_texts().forget(sid)
    w._apply_results_table_filters()
    assert _drawn(w) == [_sid(1)]


def test_a_result_not_loaded_whose_details_are_unknown_is_fetched_and_held_back(window):  # noqa: F811 (the imported fixture)
    """Codex on #150: only a drawn row started a fetch, so a result beyond the loaded rows
    whose shelfmark is unknown passed the filter for ever, counted and exported."""
    w = window
    late = _sid(900)
    real = w.meta_mgr.get_meta_for_id
    w.meta_mgr.get_meta_for_id = lambda sid: (
        (w.meta_mgr.nli_cache[sid]["shelfmark"], "") if sid in w.meta_mgr.nli_cache
        else ("Unknown", "") if sid == late else real(sid))
    asked = []
    w.start_metadata_loading = lambda ids: asked.extend(ids)
    _search(w, _many(1200))
    w.results_filters = {w.COL_SHELF: {"text": "T-S"}}           # every result but the unknown one
    w._apply_results_table_filters()
    assert late not in _drawn(w), "the test needs the unknown result beyond the loaded rows"
    assert late in asked, "nothing fetches the unknown result beyond the loaded rows"
    assert f"(1 {tr('still loading')})" in w.status_label.text()
    rows, _ticked = w._results_to_export()
    assert late not in [r["display"]["id"] for r in rows], "a result not known to match was exported"
    w.meta_mgr.nli_cache[late] = {"shelfmark": "ENA 900"}         # its fetch lands: no match
    w._result_texts().forget(late)
    w._apply_results_table_filters()
    assert tr("still loading") not in w.status_label.text()
    assert late not in [r["display"]["id"] for r in w._results_to_export()[0]]
    assert w._current_result_view().count == len(w._results_to_export()[0]) == 1199


def test_a_running_load_is_never_cancelled_to_fetch_the_pending_ones(window, monkeypatch):  # noqa: F811 (the imported fixture)
    """Codex round 3 on #150: starting a load cancels the running one and waits for it on the
    window's thread. The pending results wait for the running load to end instead."""
    from types import SimpleNamespace
    w = window
    late = _sid(900)
    real = w.meta_mgr.get_meta_for_id
    w.meta_mgr.get_meta_for_id = lambda sid: ("Unknown", "") if sid == late else real(sid)
    asked = []
    w.start_metadata_loading = lambda ids: asked.append(list(ids))
    monkeypatch.setattr(app, "_keep_until_finished", lambda worker: None)
    _search(w, _many(1200))
    w.meta_loader = SimpleNamespace(isRunning=lambda: True, sids=[_sid(1)])   # the first batch's load
    w.results_filters = {w.COL_SHELF: {"text": "T-S"}}
    w._apply_results_table_filters()
    assert asked == [], "a running load was replaced (cancelled and waited for)"
    w.meta_cached_count = w.meta_to_fetch_count = w.meta_progress_current = 0
    w.on_meta_finished(False)
    assert asked == [[late]], "the pending result was not fetched once the load ended"


def test_a_burst_of_metadata_runs_one_pass(window, monkeypatch):  # noqa: F811 (the imported fixture)
    w = window
    _search(w, _many(5))
    w.meta_cached_count = w.meta_to_fetch_count = w.meta_progress_current = 0
    passes = []
    real = app.GenizahGUI._apply_results_table_filters
    monkeypatch.setattr(app.GenizahGUI, "_apply_results_table_filters",
                        lambda self: passes.append(1) or real(self))
    for i in range(5):
        w.on_meta_progress(i + 1, 5, _sid(i))
    assert passes == []
    QTest.qWait(300)
    assert passes == [1]


def test_a_snippet_filter_looks_up_no_title(window, monkeypatch):  # noqa: F811 (the imported fixture)
    w = window
    _search(w, _many(50))
    looked_up = []
    monkeypatch.setattr(app, "_resolve_display_title", lambda sid, title, **k: looked_up.append(sid) or title)
    w.results_filters = {w.COL_SNIPPET: {"text": "/1"}}
    w._apply_results_table_filters()
    assert looked_up == []


def test_the_translations_setting_filters_the_titles_again(window, monkeypatch):  # noqa: F811 (the imported fixture)
    """Codex F5 on the plan."""
    w = window
    config = {"show_translations": False}
    monkeypatch.setattr(app, "load_app_config", lambda: dict(config))
    monkeypatch.setattr(app, "_resolve_display_title",
                        lambda sid, title, show_translations=None, **k:
                        "English" if (config["show_translations"] if show_translations is None
                                      else show_translations) else "עברית")
    w.results_filters = {w.COL_TITLE: {"text": "English"}}
    _search(w, _many(3))
    assert _drawn(w) == []
    config["show_translations"] = True
    w._refresh_search_titles()
    assert _drawn(w) == [_sid(0), _sid(1), _sid(2)]


def test_all_terms_and_a_column_filter_together(window, monkeypatch):  # noqa: F811 (the imported fixture)
    w = window
    monkeypatch.setattr(app, "compute_all_terms_filter", lambda chain: {_r(i)["uid"] for i in range(0, 600, 2)})
    monkeypatch.setattr(app, "enrich_snippet_with_chain_terms", lambda s, c, q: s)
    _search(w, _many(600))
    w.results_filters = {w.COL_SNIPPET: {"text": "/1"}}       # every result's snippet ends "/1"
    w.refinement_chain = [object(), object()]
    w._all_terms_filter = True
    w._apply_all_terms_filter_and_rerender()
    assert w.status_label.text().startswith(_showing(300, 600))
    assert w.status_label.text().endswith(f" ({tr('Only results with all terms')})")
    w.results_filters = {w.COL_SNIPPET: {"text": "8/1"}}
    w._apply_results_table_filters()
    assert w.status_label.text().startswith(_showing(60, 600))   # 8, 18, ..., 598

"""Regression: desktop search history must never persist full result snapshots.

Older builds stored ``results[:5000]`` (and ``filtered_results[:5000]`` for
composition) per history entry. With a 20-entry limit this grew
``search_history.json`` to ~778 MB, and every search loaded + rewrote the whole
file on the UI thread — a ~20-30s freeze on *every* search, independent of the
current result count (root cause of the v7.16 LOCAL-search freeze).

History now stores only lightweight metadata (query, params, filters,
exclusions); clicking an entry re-runs the search. These tests pin that
invariant in ``shared/session_persistence.py``.
"""
import json

import pytest


@pytest.fixture
def sp(tmp_path, monkeypatch):
    import shared.session_persistence as session_persistence
    monkeypatch.setattr(
        session_persistence, "HISTORY_FILE",
        str(tmp_path / "search_history.json"),
    )
    return session_persistence


def _file_size(sp):
    import os
    return os.path.getsize(sp.HISTORY_FILE)


def test_add_history_entry_drops_result_snapshot(sp):
    sp.add_history_entry("regular", {
        "query": "אבגד",
        "result_count": 27022,
        "search_params": {"mode_index": 0},
        "state": {
            # A caller that (wrongly) passes a huge snapshot must be defended
            # against — the file must stay tiny regardless.
            "results": [
                {"display": {"id": str(i)}, "full_text": "x" * 1000}
                for i in range(5000)
            ],
            "excluded_sys_ids": ["1", "2"],
        },
    })
    entries = sp.get_history("regular")
    assert len(entries) == 1
    state = entries[0]["state"]
    assert "results" not in state, "result snapshot leaked into history"
    # Lightweight fields are preserved.
    assert state.get("excluded_sys_ids") == ["1", "2"]
    size = _file_size(sp)
    assert size < 50_000, f"history file unexpectedly large: {size} bytes"


def test_add_comp_history_entry_drops_results_and_filtered(sp):
    sp.add_history_entry("composition", {
        "query": "title",
        "result_count": 100,
        "search_params": {},
        "state": {
            "source_text": "some source text",
            "results": [{"x": 1}] * 5000,
            "filtered_results": [{"y": 2}] * 5000,
        },
    })
    state = sp.get_history("composition")[0]["state"]
    assert "results" not in state
    assert "filtered_results" not in state
    # source_text is kept so the entry can re-run the composition search.
    assert state.get("source_text") == "some source text"


def test_load_history_file_self_heals_legacy_bloat(sp):
    # Simulate a legacy bloated file written by an older build.
    legacy = {
        "regular": [{
            "query": "q",
            "result_count": 5000,
            "search_params": {},
            "state": {"results": [{"a": 1}] * 5000, "excluded_sys_ids": ["7"]},
        }],
        "composition": [{
            "query": "c",
            "result_count": 10,
            "search_params": {},
            "state": {"source_text": "t", "results": [{"a": 1}] * 10,
                      "filtered_results": [{"b": 2}] * 10},
        }],
    }
    with open(sp.HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(legacy, f)

    data = sp._load_history_file()
    assert "results" not in data["regular"][0]["state"]
    assert data["regular"][0]["state"]["excluded_sys_ids"] == ["7"]
    assert "results" not in data["composition"][0]["state"]
    assert "filtered_results" not in data["composition"][0]["state"]
    assert data["composition"][0]["state"]["source_text"] == "t"


def test_strip_helper_reports_whether_it_stripped(sp):
    clean = {"regular": [{"state": {"excluded_sys_ids": []}}], "composition": []}
    assert sp._strip_history_result_snapshots(clean) is False
    bloated = {"regular": [{"state": {"results": [1, 2, 3]}}], "composition": []}
    assert sp._strip_history_result_snapshots(bloated) is True
    assert "results" not in bloated["regular"][0]["state"]

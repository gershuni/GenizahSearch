# -*- coding: utf-8 -*-
"""The HTR side of a substituted page in the private review viewer.

Pinned: the HTR pane is offered only when `htr_page` exists AND the stamped
row count equals the count the attach pass recorded (fail-closed, like the
card grain); the page endpoint returns the stored text UNCHANGED, because the
row's offsets index that exact string; and the page markup carries the
feature flag the JavaScript reads.
"""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
import serve_v3_review as sv  # noqa: E402


class _Fake:
    """The members `_has_htr` / `_api_htr_page` touch."""

    def __init__(self, con):
        self.con = con
        self.sent = None

    def _query(self, con, name, sql, params=()):
        return self.con.execute(sql, params)

    def _conn(self):
        return self.con

    def _send(self, obj, ctype="application/json", raw=None, status=200):
        self.sent = obj

    def _has_htr(self, con):
        return sv.Handler._has_htr(self, con)

    @staticmethod
    def _one(q, key, default=""):
        return (q.get(key) or [default])[0]


class _Con:
    """A connection whose close() the handler may call without ending the
    test's own view of the database."""

    def __init__(self, con):
        self._c = con

    def execute(self, *a):
        return self._c.execute(*a)

    def close(self):
        pass


def _db(tmp_path, *, stamped, meta_rows, with_table=True, text="+פסוק~ 5~ שלום"):
    p = str(tmp_path / "v.db")
    con = sqlite3.connect(p)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY, "
                "htr_align_status TEXT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    if with_table:
        con.execute("""CREATE TABLE htr_page(page_id TEXT PRIMARY KEY, sys_id TEXT,
            search_text_source TEXT, substitution_score REAL, search_text_n_chars INT,
            htr_text TEXT, htr_n_chars INT, htr_file_char_start INT,
            htr_file_char_end INT, nfc_ok INT, in_review_set INT)""")
        con.execute("INSERT INTO htr_page VALUES ('pg1','99',' fgp',88.5,120,?,?,1000,1014,1,1)",
                    (text, len(text)))
    con.executemany("INSERT INTO review_row VALUES (?,?)",
                    [("e%d" % i, "exact" if i < stamped else None) for i in range(5)])
    if meta_rows is not None:
        con.execute("INSERT INTO meta VALUES ('htr_realign.rows', ?)", (str(meta_rows),))
    con.commit()
    return _Con(con)


@pytest.fixture(autouse=True)
def _reset_probe():
    sv.Handler._htr_table = None
    yield
    sv.Handler._htr_table = None


def test_current_pass_is_offered(tmp_path):
    con = _db(tmp_path, stamped=3, meta_rows=3)
    assert sv.Handler._has_htr(_Fake(con), con) is True


def test_no_table_means_off(tmp_path):
    con = _db(tmp_path, stamped=3, meta_rows=3, with_table=False)
    assert sv.Handler._has_htr(_Fake(con), con) is False


def test_stamp_count_drift_is_refused(tmp_path):
    """A row was re-stamped or added after the pass: the recorded count no
    longer matches, so no address from that pass is trusted."""
    con = _db(tmp_path, stamped=4, meta_rows=3)
    assert sv.Handler._has_htr(_Fake(con), con) is False


def test_zero_stamps_is_off_even_with_a_table(tmp_path):
    con = _db(tmp_path, stamped=0, meta_rows=0)
    assert sv.Handler._has_htr(_Fake(con), con) is False


def test_page_endpoint_returns_the_stored_text_unchanged(tmp_path):
    """No display cleaning: the row's htr_page_char_* index THIS string."""
    text = "+פסוק~ 5~ שלום עליכם"
    con = _db(tmp_path, stamped=3, meta_rows=3, text=text)
    f = _Fake(con)
    sv.Handler._api_htr_page(f, {"page_id": ["pg1"]})
    assert f.sent["text"] == text
    assert f.sent["file_start"] == 1000 and f.sent["file_end"] == 1014
    assert f.sent["source"].strip() == "fgp" and f.sent["score"] == 88.5
    # what the display cleaner would have done to it -- proves the check bites
    assert sv.clean_display_markers(text) != text


def test_page_endpoint_names_an_unsubstituted_page(tmp_path):
    con = _db(tmp_path, stamped=3, meta_rows=3)
    f = _Fake(con)
    sv.Handler._api_htr_page(f, {"page_id": ["nope"]})
    assert "not substituted" in f.sent["error"]


def test_page_endpoint_is_closed_when_the_pass_is_stale(tmp_path):
    con = _db(tmp_path, stamped=4, meta_rows=3)
    f = _Fake(con)
    sv.Handler._api_htr_page(f, {"page_id": ["pg1"]})
    assert "error" in f.sent and "text" not in f.sent


def test_page_markup_carries_the_flag():
    html_on = sv.render_page({}, {}, "https://x", "off", htr_ok=True)
    html_off = sv.render_page({}, {}, "https://x", "off")
    assert "const HTR_OK = true;" in html_on
    assert "const HTR_OK = false;" in html_off
    assert "__HTR_OK__" not in html_on and "__HTR_OK__" not in html_off
    # the JavaScript that draws the address must reference every status once
    for st in ("exact", "realigned_htr", "realign_uncertain", "ambiguous"):
        assert '"%s"' % st in html_on
    assert json.dumps("HTR text of this page")[1:-1] in html_on

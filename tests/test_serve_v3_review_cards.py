# -*- coding: utf-8 -*-
"""Card-grain plumbing in the private review viewer.

Two things here are worth pinning: a stale card projection must NEVER be
offered as current, and the clause builder must survive an EMPTY filter (the
viewer's `_where` returns "" when nothing is filtered, so a naive
" AND ..." concatenation produced invalid SQL).
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
import serve_v3_review as sv  # noqa: E402


def test_and_composes_onto_an_empty_where():
    assert sv.Handler._and("", "a = 1") == "WHERE a = 1"
    assert sv.Handler._and("WHERE x", "a = 1") == "WHERE x AND a = 1"
    assert sv.Handler._and("WHERE x") == "WHERE x"
    assert sv.Handler._and("", None, "") == ""
    assert sv.Handler._and("", "a", "b") == "WHERE a AND b"


class _Fake:
    """The two members `_has_cards` touches."""

    def __init__(self, con):
        self.con = con

    def _query(self, con, name, sql, params=()):
        return self.con.execute(sql, params)


def _db(tmp_path, *, members, registry_root, card_root, rows, with_table=True):
    p = str(tmp_path / "v.db")
    con = sqlite3.connect(p)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    if with_table:
        con.execute("CREATE TABLE card_member(evidence_id TEXT)")
    con.executemany("INSERT INTO review_row VALUES (?)",
                    [("e%d" % i,) for i in range(rows)])
    con.executemany("INSERT INTO meta VALUES (?,?)", [
        ("card_grain.members", str(members)),
        ("card_grain.registry_pins_sha256", card_root),
        ("work_registry.pins_sha256", registry_root)])
    con.commit()
    return con


@pytest.fixture(autouse=True)
def _reset_probe():
    sv.Handler._card_grain = None
    yield
    sv.Handler._card_grain = None


def test_current_projection_is_offered(tmp_path):
    con = _db(tmp_path, members=3, registry_root="aa", card_root="aa", rows=3)
    assert sv.Handler._has_cards(_Fake(con), con) is True


def test_projection_behind_review_row_is_refused(tmp_path):
    """Rows were added after the cards were built: some row belongs to no
    card, and a card view would silently omit it."""
    con = _db(tmp_path, members=3, registry_root="aa", card_root="aa", rows=4)
    assert sv.Handler._has_cards(_Fake(con), con) is False


def test_projection_from_an_older_registry_is_refused(tmp_path):
    """The registry was rebuilt (identities changed) but the cards were not."""
    con = _db(tmp_path, members=3, registry_root="bb", card_root="aa", rows=3)
    assert sv.Handler._has_cards(_Fake(con), con) is False


def test_absent_projection_is_refused(tmp_path):
    con = _db(tmp_path, members=3, registry_root="aa", card_root="aa", rows=3,
              with_table=False)
    assert sv.Handler._has_cards(_Fake(con), con) is False


def test_card_sorts_do_not_offer_a_row_only_key():
    """A sort key with no card equivalent would silently fall back to the
    default, so the card grain must not offer one."""
    assert "pages" not in sv.Handler.CARD_SORT_SQL
    assert "witnesses" in sv.Handler.CARD_SORT_SQL
    assert "witnesses" not in sv.SORT_SQL
    # every card sort orders by a column of `card`, never of the fat table
    for expr in sv.Handler.CARD_SORT_SQL.values():
        assert "r." not in expr


def test_page_offers_no_card_toggle_without_a_projection():
    """A v3-era file, or one before the attach script ran, keeps the tool
    exactly as it was."""
    off = sv.render_page({}, {}, "http://x", "off", cards_ok=False)
    on = sv.render_page({}, {}, "http://x", "off", cards_ok=True)
    assert "const CARDS_OK = false;" in off
    assert "const CARDS_OK = true;" in on
    assert "__CARDS_OK__" not in off and "__CARDS_OK__" not in on

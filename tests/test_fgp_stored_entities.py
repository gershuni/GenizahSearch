"""GS-60: FGP's stored HTML entities are decoded once, where a row is read, for every surface."""

import json
import sqlite3

from shared.fgp_service import _row_to_fgp_source, decode_stored_entities


def test_the_margin_sign_and_a_double_encoding_are_decoded():
    # Candidate rows 651 and 8767, verbatim.
    assert decode_stored_entities("והשקה את&deg;=ת= כל") == "והשקה את°=ת= כל"
    assert decode_stored_entities("Pass&amp;#40age&amp;#41:") == "Pass(age):"


def test_the_genuzot_marks_and_loose_ampersands_are_left_alone():
    for text in ("&ערבית&", "&תמת&@", "R&D", "a & b", "&deg without a semicolon", "&notanentity;"):
        assert decode_stored_entities(text) == text
    assert decode_stored_entities("") == ""
    assert decode_stored_entities(None) is None


def test_a_row_is_decoded_in_its_content_and_its_sections():
    con = sqlite3.connect(":memory:")
    con.row_factory = sqlite3.Row
    con.execute("create table t (id, content, sections, doc_relation)")
    con.execute("insert into t values (1, ?, ?, 'Digital Edition')",
                ("שנה&deg;= הלכות=", json.dumps([{"page_num": 1, "text": "ני&deg; ^ש^"}])))
    src = _row_to_fgp_source(con.execute("select * from t").fetchone())
    assert src["content"] == "שנה°= הלכות="
    assert src["sections"][0]["text"] == "ני° ^ש^"

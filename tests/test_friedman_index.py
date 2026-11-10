"""Rows of the recovered index of forms: line reading, column order, binding."""
from __future__ import annotations

import json

import pytest

from scripts import friedman_index as fi
from scripts import prepare_friedman_dictionary as prep


def test_rows_read_mirrored_brackets_dots_and_several_roots():
    row = fi.parse_line("אגל )אג˜ל, ג˜לל(")
    assert row["form"] == "אגל" and row["form_key"] == "אגל"
    assert [r["dotted"] for r in row["roots"]] == ["אג̇ל", "ג̇לל"]
    assert [r["key"] for r in row["roots"]] == ["אגל", "גלל"], "the shared key loses the dot; the dotted key keeps it"
    row = fi.parse_line("אגצא )גצ˜ ו/גצ˜ י(")
    assert [(r["dotted"], r["alternatives"]) for r in row["roots"]] == [("גצ̇ו", 2), ("גצ̇י", 2)]
    assert fi.parse_line("אבריסם") == {"form": "אבריסם", "form_key": "אבריסמ", "roots": [], "line": "אבריסם"}
    assert fi.parse_line("זיר אכואן )זרכ˜ ואן(")["roots"][0]["dotted"] == "זרכ̇ואנ"[:-1] + "ן"
    assert fi.parse_line("מפתח ערכים ושורשיהם") is None and fi.parse_line("[969]") is None
    assert fi.parse_line(")כפי שבאים בכתבי יד רבים(, שהמעיין") is None


def line(text, x0, x1, y):
    return {"bbox": [x0, y, x1, y + 10], "spans": [{"font": "Advhadasa-n", "size": 10.0, "text": text}]}


def page(number, *lines):
    return {"pdf_page": number, "text": "", "blocks": [{"lines": list(lines)}]}


def extraction():
    first = page(1, line("מפתח ערכים ושורשיהם", 300, 440, 60),
                 line("הרשימה כוללת מבחר ערכים הנדפסים בלי סימנים", 116, 442, 130),
                 line("אבד )אבד(", 400, 442, 200), line("אגל )אג˜ל, ג˜לל(", 395, 442, 212),
                 line("אדאה )אדו(", 280, 329, 200), line("אבריסם", 170, 215, 200), line("[969]", 270, 288, 630))
    second = page(2, line("אוחד )וחד(", 404, 442, 101), line("[970]", 270, 288, 630))
    return {"source_id": "Value_Keys", "source_sha256": prep.INDEX_RECOVERY_SHA256,
            "letter_recovery": {"status": "letter_recovery_candidate_not_lexical_mapping", "pages": [first, second]}}


def test_columns_are_read_right_to_left_and_the_introduction_is_skipped():
    result = fi.build_rows(extraction())
    assert [(r["form"], r["column"], r["printed_page"], r["order"]) for r in result["rows"]] == [
        ("אבד", 0, "969", 0), ("אגל", 0, "969", 1), ("אדאה", 1, "969", 2), ("אבריסם", 2, "969", 3), ("אוחד", 0, "970", 0)]
    assert result["summary"] == {"rows": 5, "with_roots": 4, "several_roots": 1, "distinct_forms": 5, "distinct_roots": 5,
                                 "dotted_roots": 1, "lines_skipped": 1, "order_inversions": 1}
    assert result["rows"][0]["url"].endswith("Value_Keys.pdf#page=1")


def test_unpinned_source_is_refused():
    data = extraction()
    data["source_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="pinned"):
        fi.build_rows(data)


def test_build_and_verify_bind_the_rows_to_the_retained_extraction(tmp_path):
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    prep.write_json(candidate / "Value_Keys.json", extraction())
    assert fi.build(tmp_path)["rows"] == 5
    assert fi.verify(tmp_path)["verified"]
    stored = json.loads((candidate / "index_forms.json").read_text(encoding="utf-8"))
    stored["rows"][0]["roots"][0]["dotted"] = "אבג"
    prep.write_json(candidate / "index_forms.json", stored)
    with pytest.raises(ValueError, match="differ"):
        fi.verify(tmp_path)
    prep.write_json(candidate / "Value_Keys.json", {**extraction(), "changed": True})
    with pytest.raises(ValueError, match="not bound"):
        fi.verify(tmp_path)

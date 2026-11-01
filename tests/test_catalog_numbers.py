"""shared/catalog_numbers.py: the catalogue's free-text line and column counts
(ruling D7) and copy-date spans (ruling D18) of
docs/plans/CATALOGUE_CRITERIA_PLAN.md."""
import json
import re
import sqlite3
from pathlib import Path

import pytest

from shared.catalog_numbers import copy_span, parse_number_text, span_label

GOLDEN = Path(__file__).parent / "fixtures" / "catalog_numbers_golden.json"
REAL_SIDECAR = Path(__file__).resolve().parent.parent / "fist_data" / "fjms_enrichment.db"


@pytest.mark.parametrize("text, expected", [
    ("13", [(13, 13)]),
    (" 13 ", [(13, 13)]),
    ("19-21", [(19, 21)]),
    ("22,25", [(22, 22), (25, 25)]),
    ("16–31", [(16, 31)]),             # en dash
    ("5—7", [(5, 7)]),                  # em dash
    ("19­-24", [(19, 24)]),            # soft hyphen before the dash
    ("22-­24", [(22, 24)]),            # soft hyphen after it
    ("8-9-12", [(8, 12)]),                  # a chain is min..max
    ("22-20 ", [(20, 22)]),                 # reversed
    ("15 (-16)", [(15, 16)]),               # parenthesised end
    ("5-", [(5, 5)]),                       # trailing dash
    ("7,3,", [(3, 3), (7, 7)]),             # trailing comma
    ("0", []),                              # blank in Lieberman / Danzig
    ("0-0", []),
    ("0-25", [(25, 25)]),                   # zero end dropped
    ("26-0", [(26, 26)]),
    ("1117", [(1117, 1117)]),               # over 80: kept
    ("16,20-25,16-18,20-22", [(16, 16), (16, 18), (20, 22), (20, 25)]),
    ("17,6,17", [(6, 6), (17, 17)]),        # repeats once
    ("", []),
    (None, []),
])
def test_parse_number_text_rules(text, expected):
    assert parse_number_text(text) == expected


def test_parse_number_text_matches_the_golden_table():
    """Every distinct non-plain line/column text of the 5.0.0 catalogue,
    with the intervals it was reviewed to give. A parser change shows up
    here as a diff of the reviewed table."""
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert set(golden) == {"lines", "columns"}
    assert len(golden["lines"]) > 2000 and len(golden["columns"]) > 10
    wrong = {
        text: (parse_number_text(text), [tuple(iv) for iv in want])
        for table in golden.values() for text, want in table.items()
        if parse_number_text(text) != [tuple(iv) for iv in want]
    }
    assert not wrong, list(wrong.items())[:10]


@pytest.mark.skipif(not REAL_SIDECAR.is_file() or REAL_SIDECAR.stat().st_size < 1_000_000,
                    reason="the real FJMS sidecar is not here")
def test_golden_table_covers_every_form_in_the_real_sidecar():
    """A new export with a form the table has never seen fails here, so the
    new form is reviewed before it is parsed silently."""
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    conn = sqlite3.connect(f"file:{REAL_SIDECAR.as_posix()}?mode=ro", uri=True)
    try:
        for key, column in (("lines", "NumRow"), ("columns", "NumColumn")):
            texts = {t for (t,) in conn.execute(
                f"SELECT DISTINCT {column} FROM catalog "
                f"WHERE {column} IS NOT NULL AND TRIM({column}) <> ''")}
            unseen = {t for t in texts if not re.fullmatch(r"\d+", t.strip())} - set(golden[key])
            assert not unseen, sorted(unseen)[:20]
    finally:
        conn.close()


@pytest.mark.parametrize("start, end, expected", [
    ("1050", None, (1050, 1050)),
    ("1050", "", (1050, 1050)),
    ("944", "1100", (944, 1100)),
    ("1027", "8", (1027, 1027)),            # end before start: the start alone
    ("1300", "1300", (1300, 1300)),
    ("0", "1200", None),                    # end only: no date
    ("-99", "1200", None),
    (None, "1200", None),
    ("", None, None),
    ("abc", None, None),
])
def test_copy_span(start, end, expected):
    assert copy_span(start, end) == expected


def test_span_label():
    assert span_label((944, 1100)) == "944–1100"
    assert span_label((1050, 1050)) == "1050"
    assert span_label(None) == ""

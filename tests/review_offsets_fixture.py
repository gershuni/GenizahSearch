# -*- coding: utf-8 -*-
"""Build a tiny, self-contained review DB for the offset-verifier gate.

Not a test module (no `test_` prefix, so pytest does not collect it).

WHY THIS EXISTS
---------------
`tests/test_verify_v3_review_offsets.py` proves that
`scripts/verify_v3_review_offsets.py` can actually FAIL: each test breaks one
stored coordinate and asserts a non-zero exit. That guarantee is worth keeping
exactly as it is.

What was not worth keeping is the price. Every mutation test copied the real
`discovery_data/discovery-v5-REVIEW.db` -- 3.45 GB, 519,382 rows -- and then ran
the verifier over every row of it as a subprocess. Measured 2026-09-09: 3,396
seconds for six tests, **66.5% of the entire non-GUI suite's module time**. And
because the file is `skipif(DB is None)`, it costs CI nothing and the owner
everything: CI has no review DB, so it skips there and runs in full only on the
one machine that has the data.

So the mutation matrix now runs against the DB built here: a handful of rows
over a handful of characters. Same verifier, same subprocess, same exit-code
assertions, same three mutation classes -- in about a second.

WHAT THIS DOES **NOT** DO
-------------------------
It does not verify the real artifact. A small mutation matrix proves the
verifier rejects bad coordinates; it says nothing about whether 519,382
production rows are correct. That check still has to happen -- see
`scripts/verify_review_artifact.py` and the `slow`-marked real-data test -- and
this fixture is not a substitute for it.

EVERY OFFSET BELOW IS HAND-CHECKABLE
------------------------------------
The point of a fixture for an offset oracle is that its expected coordinates are
specified INDEPENDENTLY of both the builder and the verifier. Nothing here calls
either. The source text is short enough to index by eye, and the table in
`_SOURCE_LAYOUT` below states every character position explicitly.
"""
from __future__ import annotations

import json
import os
import sqlite3

# ---------------------------------------------------------------------------
# The M-source witness text, and its character positions, stated exhaustively.
#
#   index : 0 1 2 3 4 5 6 7 8 9  10 11 12 13 14 15 16 17 18
#   char  : #  # H E A D E R #  #  א  ב  ג  ד  ה  ו  ז  ח  ט
#           \_____ cut by M_HEADER _____/  \___ the letter stream ___/
#
# The verifier's oracle cuts `##...##` for kind 'M', so the stream is the nine
# Hebrew letters and `positions` is [10, 11, 12, 13, 14, 15, 16, 17, 18].
# A stream index i therefore sits at character 10 + i.
# ---------------------------------------------------------------------------
_HEADER = "##HEADER##"                     # 10 characters, all cut
_LETTERS = "אבגדהוזחט"                     # 9 letters, characters 10..18
SOURCE_TEXT = _HEADER + _LETTERS

_FIRST_LETTER_CHAR = len(_HEADER)          # 10

_SOURCE_LAYOUT = {                         # stream index -> character index
    i: _FIRST_LETTER_CHAR + i for i in range(len(_LETTERS))
}

# Two rows in ONE witness, so the "two rows swap their loci" mutation has a
# same-witness pair to swap (that test requires two rows with different starts
# under the same witness_id).
#
#   ev1: stream[1:4] == 'בגד' -> chars [11, 14)
#   ev2: stream[5:8] == 'וזח' -> chars [15, 18)
#
# Both spans are longer than one letter, which the mutation helper requires
# (`ref_char_end > ref_char_start + 1`).
_REF_ROWS = [
    ("ev1", 1, 4, "בגד"),
    ("ev2", 5, 8, "וזח"),
]

# ---------------------------------------------------------------------------
# The manuscript-side corpus file, also stated exhaustively.
#
#   0..9   A B C D E F G H I J      (filler)
#   10..14 א ב ג ד ה                 <- ev1's file span [10, 15)
#   15..19 K L M N O                (filler)
#   20..24 ו ז ח ט י                 <- ev2's file span [20, 25)
#   25..29 P Q R S T                (filler)
#
# The verifier slices the corpus file at [file_char_start, file_char_end) and
# compares it with `ms_match`, so these two must agree exactly.
# ---------------------------------------------------------------------------
CORPUS_TEXT = ("ABCDEFGHIJ" + "אבגדה" + "KLMNO" + "וזחטי" + "PQRST")

_MS_ROWS = {
    "ev1": (10, 15, "אבגדה"),
    "ev2": (20, 25, "וזחטי"),
}

_SOURCE_FILE_ID = "sf_fixture_m"
_WITNESS_ID = "wit_fixture_m"
_REF_ID = "M:fixture-1"

# Schema mirrors discovery-v5-REVIEW.db for every column the verifier reads,
# plus the NOT NULL columns it does not. Deliberately NOT the full production
# DDL: a fixture that reproduces 60 unused columns rots the moment one changes,
# and the verifier's own queries are the contract that matters here.
_DDL = """
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);

CREATE TABLE source_file (
  id          TEXT PRIMARY KEY,
  kind        TEXT NOT NULL,
  masked      INTEGER NOT NULL,
  ref_id      TEXT NOT NULL,
  display_ref TEXT,
  UNIQUE(kind, ref_id)
);

CREATE TABLE reference_witness (
  witness_id     TEXT PRIMARY KEY,
  work_id        TEXT NOT NULL,
  raw_id         TEXT NOT NULL,
  source_file_id TEXT REFERENCES source_file(id),
  w_shift        INTEGER NOT NULL DEFAULT 0,
  w_is_stream    INTEGER NOT NULL DEFAULT 0,
  UNIQUE(work_id, raw_id)
);

CREATE TABLE review_row (
  evidence_id     TEXT PRIMARY KEY,
  sys_id          TEXT NOT NULL,
  page_id         TEXT NOT NULL,
  work_id         TEXT NOT NULL,
  ms_match        TEXT,
  ref_match       TEXT,
  page_char_start INTEGER, page_char_end INTEGER,
  file_char_start INTEGER, file_char_end INTEGER,
  ms_provenance_status TEXT,
  w_start INTEGER, w_end INTEGER,
  ref_char_start INTEGER, ref_char_end INTEGER,
  ref_provenance_status TEXT,
  witness_id TEXT
);
"""


def expected_ref_offsets(w_start: int, w_end: int) -> tuple:
    """The (ref_char_start, ref_char_end) a correct row must carry.

    Derived from `_SOURCE_LAYOUT` alone -- no builder, no verifier, no oracle.
    `ref_char_end` is one PAST the last letter, matching the verifier's
    `pos[b - 1] + 1`.
    """
    return (_SOURCE_LAYOUT[w_start], _SOURCE_LAYOUT[w_end - 1] + 1)


def build(root) -> dict:
    """Write the fixture under `root` (a pathlib.Path) and return its paths.

    Returns a dict with `db`, `sourcekeys`, `corpus` and `source` -- everything
    `scripts/verify_v3_review_offsets.py` needs to run hermetically, with no
    reference to the owner's real data or key file.
    """
    root = os.fspath(root)
    src_dir = os.path.join(root, "sources")
    os.makedirs(src_dir, exist_ok=True)

    source_path = os.path.join(src_dir, "fixture_m_source.txt")
    with open(source_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(SOURCE_TEXT)

    corpus_path = os.path.join(root, "corpus.txt")
    with open(corpus_path, "w", encoding="utf-8", newline="") as fh:
        fh.write(CORPUS_TEXT)

    # The masked-source id->path map, in the same shape as the real
    # sourcekeys.json but pointing only at the fixture file above.
    keys_path = os.path.join(root, "sourcekeys.json")
    with open(keys_path, "w", encoding="utf-8") as fh:
        json.dump({_REF_ID: source_path}, fh, ensure_ascii=False)

    db_path = os.path.join(root, "review-fixture.db")
    if os.path.exists(db_path):
        os.remove(db_path)
    con = sqlite3.connect(db_path)
    con.executescript(_DDL)
    con.execute("INSERT INTO meta VALUES ('schema', 'v3-review/2')")
    con.execute(
        "INSERT INTO source_file (id, kind, masked, ref_id, display_ref) "
        "VALUES (?, 'M', 1, ?, NULL)", (_SOURCE_FILE_ID, _REF_ID))
    con.execute(
        "INSERT INTO reference_witness "
        "(witness_id, work_id, raw_id, source_file_id, w_shift, w_is_stream) "
        "VALUES (?, 'w000001', ?, ?, 0, 0)",
        (_WITNESS_ID, _REF_ID, _SOURCE_FILE_ID))

    for eid, w_start, w_end, ref_match in _REF_ROWS:
        ref_start, ref_end = expected_ref_offsets(w_start, w_end)
        file_start, file_end, ms_match = _MS_ROWS[eid]
        con.execute(
            "INSERT INTO review_row (evidence_id, sys_id, page_id, work_id, "
            " ms_match, ref_match, page_char_start, page_char_end, "
            " file_char_start, file_char_end, ms_provenance_status, "
            " w_start, w_end, ref_char_start, ref_char_end, "
            " ref_provenance_status, witness_id) "
            "VALUES (?, '990000000000000001', ?, 'w000001', ?, ?, 0, ?, ?, ?, "
            "        'ok', ?, ?, ?, ?, 'ok', ?)",
            (eid, "page_" + eid, ms_match, ref_match,
             len(ms_match), file_start, file_end,
             w_start, w_end, ref_start, ref_end, _WITNESS_ID))
    con.commit()
    con.close()

    return {"db": db_path, "sourcekeys": keys_path,
            "corpus": corpus_path, "source": source_path}

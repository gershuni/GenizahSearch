# -*- coding: utf-8 -*-
"""Every gate in `scripts/package_review_artifact.py`, proven able to FAIL.

A packaging gate that cannot fail is decoration: it would print PASS over a
torn database, a masking leak, or the key file sitting in the bundle. So each
check here is handed the state it exists to refuse.

The live-writer check is exercised for real rather than here: it fired on a
running process, then had to be fixed because it matched ITS OWN command line
(`--also scripts/serve_v3_review.py`) and reported the packaging run as a
writer.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
import package_review_artifact as pkg  # noqa: E402

QUIET = (lambda *a: None)


def _db(path, *, orphan=False):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE parent(id INTEGER PRIMARY KEY)")
    con.execute("CREATE TABLE child(id INTEGER, pid INTEGER REFERENCES parent(id))")
    con.execute("INSERT INTO parent VALUES (1)")
    con.execute("INSERT INTO child VALUES (1, 1)")
    if orphan:
        # FKs are OFF by default, so this lands and only shows up in
        # foreign_key_check -- exactly the shape a merge bug leaves behind
        con.execute("INSERT INTO child VALUES (2, 999)")
    con.commit()
    con.close()
    return path


def test_side_file_refuses(tmp_path):
    p = _db(str(tmp_path / "a.db"))
    open(p + "-journal", "wb").write(b"x")
    with pytest.raises(pkg.GateError, match="side files present"):
        pkg.no_side_files([p], QUIET)


def test_no_side_file_passes(tmp_path):
    p = _db(str(tmp_path / "b.db"))
    pkg.no_side_files([p], QUIET)          # must not raise


def test_foreign_key_violation_refuses(tmp_path):
    p = _db(str(tmp_path / "c.db"), orphan=True)
    with pytest.raises(pkg.GateError, match="foreign-key violations"):
        pkg.integrity(p, QUIET, "c.db")


def test_clean_db_passes_integrity(tmp_path):
    p = _db(str(tmp_path / "d.db"))
    pkg.integrity(p, QUIET, "d.db")


def test_missing_pattern_file_refuses(tmp_path, monkeypatch):
    """A scan with no patterns is a FALSE GREEN, so an unset env var is a
    failure, never a skip."""
    monkeypatch.delenv("MASKING_SCAN_PATTERNS_FILE", raising=False)
    p = _db(str(tmp_path / "e.db"))
    with pytest.raises(pkg.GateError, match="is not set"):
        pkg.masking([p], QUIET)


def test_planted_pattern_hit_refuses(tmp_path, monkeypatch):
    pat = tmp_path / "patterns.txt"
    pat.write_text("SECRETCORPUSNAME\n", encoding="utf-8")
    monkeypatch.setenv("MASKING_SCAN_PATTERNS_FILE", str(pat))
    leak = tmp_path / "note.md"
    leak.write_text("this mentions SECRETCORPUSNAME in passing\n", encoding="utf-8")
    with pytest.raises(pkg.GateError, match="masking hit"):
        pkg.masking([str(leak)], QUIET)


def test_clean_file_passes_masking(tmp_path, monkeypatch):
    pat = tmp_path / "patterns.txt"
    pat.write_text("SECRETCORPUSNAME\n", encoding="utf-8")
    monkeypatch.setenv("MASKING_SCAN_PATTERNS_FILE", str(pat))
    ok = tmp_path / "note.md"
    ok.write_text("nothing restricted here\n", encoding="utf-8")
    pkg.masking([str(ok)], QUIET)


def test_key_file_in_the_bundle_refuses(tmp_path):
    f = tmp_path / "sourcekeys.json"
    f.write_text("{}", encoding="utf-8")
    with pytest.raises(pkg.GateError, match="key file is in the bundle"):
        pkg.key_file_absent([str(f)], QUIET)


def test_key_file_beside_the_bundle_refuses(tmp_path):
    """Not in the list, but in a directory the bundle is drawn from -- one
    `git add -A` or one drag-and-drop away from travelling with it."""
    (tmp_path / "sourcekeys.json").write_text("{}", encoding="utf-8")
    p = _db(str(tmp_path / "f.db"))
    with pytest.raises(pkg.GateError, match="sits in a bundle directory"):
        pkg.key_file_absent([p], QUIET)


def test_manifest_records_size_and_hash(tmp_path):
    p = tmp_path / "x.txt"
    p.write_text("hello", encoding="utf-8")
    text = pkg.manifest([str(p)], QUIET)
    assert "x.txt" in text and "size   5 bytes" in text
    # sha256("hello")
    assert ("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
            in text)


def test_a_missing_bundle_file_refuses(tmp_path):
    p = _db(str(tmp_path / "g.db"))
    with pytest.raises(pkg.GateError, match="bundle file missing"):
        pkg.run(p, [str(tmp_path / "nope.md")], say=QUIET)

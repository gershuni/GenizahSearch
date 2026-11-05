# -*- coding: utf-8 -*-
"""scripts/sample_fjms_translations.py: the audit sample of FJMS automatic translations, drawn
either from the translation table or (the population search meets) from the built sidecar."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog_text_fixture import C, D, H, M, build_sidecar  # noqa: E402
from scripts import sample_fjms_translations as sampler  # noqa: E402


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("sampler")
    sidecar = build_sidecar(root)
    return root / "inputs" / "fjms_enrichment.db", sidecar


def test_the_table_population_leaves_out_people_and_bibliography(built):
    fjms, _ = built
    rows = sampler.draw(fjms, n=7, seed=1)
    assert len(rows) == 7 and len({r["id"] for r in rows}) == 7
    assert not [r for r in rows if r["field"] in sampler.EXCLUDED_FIELDS]
    with pytest.raises(SystemExit):
        sampler.draw(fjms, n=8, seed=1)


def test_the_sidecar_population_is_exactly_what_search_indexes(built):
    fjms, sidecar = built
    con = sqlite3.connect(f"file:{sidecar.resolve().as_posix()}?mode=ro", uri=True)
    indexed = con.execute("SELECT COUNT(*) FROM records WHERE source = 'fjms' AND automatic = 1").fetchone()[0]
    con.close()
    rows = sampler.draw_from_sidecar(fjms, sidecar, n=indexed, seed=1)
    got = sorted((r["sys_id"], r["field"], r["translation"]) for r in rows)
    # The duplicate of A's title, the unknown work, the orphan title and the unknown author never
    # entered the sidecar, so they cannot be drawn; the shared work title counts once per manuscript.
    assert got == sorted([
        (D, "FreeDesc", "Marriage contract (ketubba) of Sitt al-Dar"),
        (H, "GenizahTitleEngTitle", "Questions and Answers"),
        (M, "GenizahTitleEngTitle", "Questions and Answers"),
        (C, "AuthorText", "Nathan ben Nahorai"),
    ])
    assert len({r["record_id"] for r in rows}) == indexed
    with pytest.raises(SystemExit):
        sampler.draw_from_sidecar(fjms, sidecar, n=indexed + 1, seed=1)


def test_a_record_is_traced_to_the_lowest_matching_translation_id(tmp_path):
    # H carries two work titles whose translations give the same text; the builder keeps the
    # translation with the lower id, whatever order the catalogue lists the titles in.
    root = tmp_path
    inputs_dir = root / "inputs"
    from catalog_text_fixture import build_args, write_inputs
    from scripts.build_catalog_text_sidecar import main as build_main

    inputs = write_inputs(inputs_dir, extra_translations=[("4000", "GenizahTitleEngTitle", "Questions and Answers")])
    con = sqlite3.connect(inputs["fjms"])
    # Title 4000 is listed for H BEFORE title 3000 (rowid order), but its translation's id is higher.
    con.execute("CREATE TABLE catalog2 AS SELECT * FROM catalog WHERE 0")
    con.execute("INSERT INTO catalog2 (AlmaId, GenizahTitleOrgTitle, GenizahTitleId) VALUES (?, 'שו\"ת', 4000)", (H,))
    con.execute("INSERT INTO catalog2 SELECT * FROM catalog")
    con.execute("DROP TABLE catalog")
    con.execute("ALTER TABLE catalog2 RENAME TO catalog")
    con.commit()
    low = con.execute("SELECT MIN(id) FROM fjms_translations WHERE translated_text = 'Questions and Answers'").fetchone()[0]
    con.close()
    out = root / "catalog_text.db"
    assert build_main(build_args(inputs, out)) == 0
    side = sqlite3.connect(f"file:{out.resolve().as_posix()}?mode=ro", uri=True)
    n = side.execute("SELECT COUNT(*) FROM records WHERE source = 'fjms' AND automatic = 1").fetchone()[0]
    side.close()
    rows = sampler.draw_from_sidecar(inputs["fjms"], out, n=n, seed=1)
    h_rows = [r for r in rows if r["sys_id"] == H and r["field"] == "GenizahTitleEngTitle"]
    assert len(h_rows) == 1 and h_rows[0]["id"] == low


def test_a_sidecar_from_another_fjms_file_is_refused(built, tmp_path):
    fjms, sidecar = built
    other = tmp_path / "other.db"
    other.write_bytes(fjms.read_bytes())
    con = sqlite3.connect(other)
    con.execute("UPDATE fjms_translations SET translated_text = 'something else'")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="not built from this FJMS file"):
        sampler.draw_from_sidecar(other, sidecar, n=1, seed=1)


def test_main_writes_the_sample(built, tmp_path):
    fjms, sidecar = built
    out = tmp_path / "sample.jsonl"
    assert sampler.main(["--db", str(fjms), "--sidecar", str(sidecar), "--out", str(out), "--n", "2"]) == 0
    assert len(out.read_text(encoding="utf-8").splitlines()) == 2
    assert sampler.main(["--db", str(tmp_path / "absent.db"), "--out", str(out)]) == 2

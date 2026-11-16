# -*- coding: utf-8 -*-
"""The editions index builder (shared/editions_indexer.py) and its gate
(scripts/verify_editions_index.py), on small sidecars built here -- never the real ones."""
import json
import os
import pickle
import re
import sqlite3
import sys

import pytest

tantivy = pytest.importorskip("tantivy")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

from shared.editions_indexer import (  # noqa: E402
    BROWSE_MAP_FILENAME, DB_DIRNAME, META_FILENAME, build_generation, read_generation)
from shared.index_format import read_index_format  # noqa: E402

GENUZOT = "FGP Transcriptions Team - Genuzos"
MS_A = "990000000010205171"   # two pages, two NLI images: FGP per-image rows
MS_B = "990000000020205171"   # no pages: an FGP whole-document row
MS_C = "990000000030205171"   # PGP join, first fragment
MS_D = "990000000040205171"   # PGP join, second fragment
MS_E = "990000000050205171"   # two pages: a PGP text linked as verso
MS_F = "990000000060205171"   # one page: a PGP text with no side
PAGES = {
    MS_A: ["IE100_P000001_FL101", "IE100_P000002_FL102"],
    MS_C: ["IE300_P000001_FL301"],
    MS_D: ["IE400_P000001_FL401", "IE400_P000002_FL402"],
    MS_E: ["IE500_P000001_FL501", "IE500_P000002_FL502"],
    MS_F: ["IE600_P000001_FL601"],
}
IMAGES = {MS_A: [{"fgp_image_number_id": "11", "folio_label": "1r"},
                 {"fgp_image_number_id": "12", "folio_label": "1v"}]}

FGP_ROWS = [
    # id, sys_id, c_number, doc_relation, content, credit_en, image_side, page_info
    (1, MS_A, "C11", "Digital Edition", "01 שָׁלוֹם לכל אחינו\n02 הזקנים הישרים הכשרים\n03 אשר בסורא", GENUZOT, "1r", "recto"),
    (2, MS_A, "C12", "Digital Edition", "קדשך שבעו(עו) עמים יראו\nחיל אחז ישבי בלשת", GENUZOT, "1v", "verso"),
    (3, MS_A, "C12", "Digital Edition", "קדשך שבעו", GENUZOT, "1v", "verso"),        # shorter scan of 2
    (4, MS_A, None, "Digital Edition", "כל הטקסט של כתב היד כולו", GENUZOT, None, None),  # whole-doc: dropped
    (5, MS_A, "C13", "Digital Edition", "01 שָׁלוֹם לכל אחינו\n02 הזקנים הישרים הכשרים\n03 אשר בסורא", GENUZOT, "2r", None),
    (6, MS_A, "C11", "Digital Translation", "a translation", GENUZOT, "1r", "recto"),
    (7, MS_B, None, "Digital Edition", "Cambridge, University Library\nT-S 1\nכותר\n\nאדם אחזמר^ע^ת נבאו כל", "Other team", None, None),
]
PGP_SOURCES = [
    # id, pgpid, doc_relation, content, sections
    (101, 1, "Digital Edition", "בתאריך תאמן ועשרין אב\nשידך ואירס מר משה", None),
    (102, 2, "Digital Edition", "ואלשרוט בינהם אן יכון\nאלמקדם כמסה עשר דינאר", None),
    (103, 2, "Digital Translation", "a translation", None),
    (104, 2, "Edition ; Translation", "אלמאוחר סתין דינאר\nכתובה וקנין", None),
    (105, 3, "Digital Edition", "פי דלך ואן לזם אל\nבית דין ואמא", None),
]
PGP_FRAGMENTS = [
    # document_id, sys_id, page_info
    (1, MS_C, None), (1, MS_D, None),
    (2, MS_E, "verso"),
    (3, MS_F, None),
]


def make_fgp(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE fgp_transcriptions (id INTEGER PRIMARY KEY, sys_id TEXT, c_number TEXT, "
                "doc_relation TEXT, language TEXT, content TEXT, sections TEXT, page_info TEXT, "
                "image_side TEXT, folio_num INTEGER, source_credit TEXT, source_credit_he TEXT, "
                "source_credit_en TEXT, text_source TEXT, image_id TEXT)")
    for rid, sid, cnum, rel, content, credit, side, page_info in FGP_ROWS:
        con.execute("INSERT INTO fgp_transcriptions (id, sys_id, c_number, doc_relation, content, "
                    "source_credit_en, image_side, page_info, text_source, folio_num) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (rid, sid, cnum, rel, content, credit, side, page_info, "gate3:docx",
                     int(side[0]) if side else None))
    con.execute("CREATE TABLE fgp_meta (key TEXT, value TEXT)")
    con.executemany("INSERT INTO fgp_meta VALUES (?,?)",
                    [("version", "9.9.9"), ("built_at", "2026-11-15"), ("rows", "7")])
    con.commit()
    con.close()


def make_pgp(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE document_sources (id INTEGER PRIMARY KEY, pgpid INTEGER, source_scholar TEXT, "
                "doc_relation TEXT, language TEXT, content TEXT, content_length INTEGER, source_url TEXT, "
                "notes TEXT, sequence_order INTEGER, sections TEXT)")
    con.execute("CREATE TABLE document_fragments (id INTEGER PRIMARY KEY, document_id INTEGER, sys_id TEXT, "
                "shelfmark TEXT, sequence_order INTEGER, page_info TEXT)")
    for sid, pgpid, rel, content, sections in PGP_SOURCES:
        con.execute("INSERT INTO document_sources (id, pgpid, doc_relation, content, sections) VALUES (?,?,?,?,?)",
                    (sid, pgpid, rel, content, sections))
    for doc, sid, page_info in PGP_FRAGMENTS:
        con.execute("INSERT INTO document_fragments (document_id, sys_id, page_info) VALUES (?,?,?)",
                    (doc, sid, page_info))
    con.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    con.executemany("INSERT INTO meta VALUES (?,?)", [("version", "1.1.0"), ("upstream_commit", "abc")])
    con.commit()
    con.close()


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    d = tmp_path_factory.mktemp("editions-inputs")
    fgp, pgp, bm = str(d / "fgp.db"), str(d / "pgp.db"), str(d / "browse_map.pkl")
    make_fgp(fgp)
    make_pgp(pgp)
    browse_map = {sid: [{"uid": u, "p_num": i + 1, "full_header": f"{sid}_{u}", "ie_id": u.split("_")[0]}
                        for i, u in enumerate(uids)] for sid, uids in PAGES.items()}
    with open(bm, "wb") as f:
        pickle.dump(browse_map, f)
    # Stands in for the NLI crossref as an input file (its images come from IMAGES).
    nli = d / "nli_crossref.db"
    nli.write_bytes(b"fixture")
    return {"fgp_db": fgp, "pgp_db": pgp, "browse_map": bm, "nli_db": str(nli)}


@pytest.fixture(scope="module")
def generation(inputs, tmp_path_factory):
    out = str(tmp_path_factory.mktemp("editions-out") / "Editions.new")
    meta = build_generation(out, fgp_db=inputs["fgp_db"], pgp_db=inputs["pgp_db"],
                            browse_map_path=inputs["browse_map"], nli_db=inputs["nli_db"],
                            folio_images_for=lambda sid: [dict(i) for i in IMAGES.get(sid, [])],
                            progress=lambda m: None)
    index = tantivy.Index.open(os.path.join(out, DB_DIRNAME))
    from shared.search_tokenizer import register_search_tokenizers
    register_search_tokenizers(index)
    index.reload()
    docs = {}
    searcher = index.searcher()
    for _s, addr in searcher.search(tantivy.Query.all_query(), limit=100).hits:
        d = searcher.doc(addr)
        docs[d["unique_id"][0]] = d
    return {"dir": out, "meta": meta, "index": index, "docs": docs}


def test_canonical_set(generation):
    assert set(generation["docs"]) == {
        "ED:fgp:1", "ED:fgp:2", "ED:fgp:7",               # 3: shorter scan; 4: whole-doc; 5: twin of 1
        "ED:pgp:101:" + MS_C, "ED:pgp:101:" + MS_D,        # a join: once per fragment
        "ED:pgp:102:" + MS_E, "ED:pgp:104:" + MS_E,        # "Edition ; Translation" is an edition
        "ED:pgp:105:" + MS_F,
    }
    _meta, payload = read_generation(generation["dir"])
    entry = next(e for e in payload["map"][MS_A] if e["uid"] == "ED:fgp:1")
    assert entry["also_ids"] == [5]
    assert generation["meta"]["counts"]["documents_by_source"] == {"fgp": 3, "pgp": 5}


def test_located_headers_copy_the_page_uid(generation):
    docs = generation["docs"]
    assert docs["ED:fgp:1"]["full_header"][0] == f"{MS_A}_ED_fgp_1_IE100_P000001_FL101"
    assert docs["ED:fgp:2"]["full_header"][0] == f"{MS_A}_ED_fgp_2_IE100_P000002_FL102"
    assert docs["ED:pgp:102:" + MS_E]["full_header"][0].endswith("_IE500_P000002_FL502")


def test_unlocated_is_never_page_zero(generation):
    docs = generation["docs"]
    for uid in ("ED:fgp:7", "ED:pgp:101:" + MS_D):
        header = docs[uid]["full_header"][0]
        assert re.search(r"_P\d+_", header) is None, header
        bounds = json.loads(docs[uid]["boundaries"][0])
        assert [b["uid"] for b in bounds] == [None]
    # A joined text is not placed even on a one-page fragment: it spans fragments.
    bounds = json.loads(docs["ED:pgp:101:" + MS_C]["boundaries"][0])
    assert [b["uid"] for b in bounds] == [None]


def test_a_one_page_manuscript_places_a_plain_text(generation):
    bounds = json.loads(generation["docs"]["ED:pgp:105:" + MS_F]["boundaries"][0])
    assert [b["uid"] for b in bounds] == ["IE600_P000001_FL601"]


def test_boundaries_cover_the_text(generation):
    for uid, d in generation["docs"].items():
        bounds = json.loads(d["boundaries"][0])
        text = d["content"][0]
        assert bounds[0]["start"] == 0 and bounds[-1]["end"] == len(text), uid
        for a, b in zip(bounds, bounds[1:]):
            assert a["end"] == b["start"], uid


def test_search_text_is_indexed(generation):
    docs = generation["docs"]
    assert docs["ED:fgp:2"]["content"][0] == "קדשך שבעו עמים יראו\nחיל אחז ישבי בלשת"
    assert docs["ED:fgp:7"]["content"][0] == "אדם אחזמרעת נבאו כל"   # catalogue block and ^ gone
    index = generation["index"]
    hits = index.searcher().search(index.parse_query('content:"שבעו עמים יראו"', ["content"]), 5).hits
    assert [index.searcher().doc(a)["unique_id"][0] for _s, a in hits] == ["ED:fgp:2"]


def test_position_fields_are_folded(generation):
    index = generation["index"]
    # Line 1 of ED:fgp:1 starts with a pointed word, its line number stripped.
    q = index.parse_query('line_starts:"L1:שלום"', ["line_starts"])
    hits = index.searcher().search(q, 5).hits
    assert [index.searcher().doc(a)["unique_id"][0] for _s, a in hits] == ["ED:fgp:1"]
    q = index.parse_query('line_ends:"L2:הכשרים"', ["line_ends"])
    assert len(index.searcher().search(q, 5).hits) == 1


def test_generation_files_share_one_build(generation):
    out = generation["dir"]
    marker = read_index_format(os.path.join(out, DB_DIRNAME))
    meta, payload = read_generation(out)
    assert marker["reversed_field"] and not marker.get("seams")
    assert marker["build_id"] == meta["build_id"] == payload["build_id"]
    assert os.path.isfile(os.path.join(out, BROWSE_MAP_FILENAME))
    assert os.path.isfile(os.path.join(out, META_FILENAME))
    assert meta["inputs"]["fgp"]["version"] == "9.9.9" and meta["inputs"]["browse_map"]["sha256"]


def test_build_refuses_to_overwrite(inputs, generation):
    with pytest.raises(FileExistsError):
        build_generation(generation["dir"], fgp_db=inputs["fgp_db"], pgp_db=inputs["pgp_db"],
                         browse_map_path=inputs["browse_map"], folio_images_for=lambda s: [])


def test_the_meta_records_the_inputs_as_they_were_before_the_build_read_them(tmp_path):
    """A sidecar replaced while the build reads (the desktop's sidecar update) must leave the
    generation stale against the new file, never stamped with it (Codex review of PR #25)."""
    from shared.editions_indexer import file_fingerprint
    fgp, pgp, bm = str(tmp_path / "fgp.db"), str(tmp_path / "pgp.db"), str(tmp_path / "bm.pkl")
    make_fgp(fgp)
    make_pgp(pgp)
    with open(bm, "wb") as f:
        pickle.dump({}, f)
    before = file_fingerprint(fgp)

    def progress(msg):
        if msg == "reading PGP editions":      # the FGP editions are read: the file changes
            con = sqlite3.connect(fgp)
            con.execute("UPDATE fgp_transcriptions SET content = content || ' עוד' WHERE id = 1")
            con.commit()
            con.close()
    meta = build_generation(str(tmp_path / "Editions.new"), fgp_db=fgp, pgp_db=pgp, browse_map_path=bm,
                            folio_images_for=lambda s: [dict(i) for i in IMAGES.get(s, [])], progress=progress)
    assert file_fingerprint(fgp) != before
    assert meta["inputs"]["fgp"]["file"] == before


def test_build_script_requires_the_nli_crossref(inputs, tmp_path):
    import build_editions_index
    code = build_editions_index.main([
        "--out", str(tmp_path / "gen"), "--fgp-db", inputs["fgp_db"], "--pgp-db", inputs["pgp_db"],
        "--browse-map", inputs["browse_map"], "--nli-db", str(tmp_path / "absent.db")])
    assert code == 2 and not (tmp_path / "gen").exists()


def test_build_refuses_a_wrong_nli_crossref(inputs, tmp_path):
    import build_editions_index
    from shared.editions_indexer import check_nli_crossref
    not_sqlite = inputs["nli_db"]                      # a non-empty file that is no database
    no_table = str(tmp_path / "no_table.db")
    con = sqlite3.connect(no_table)
    con.execute("CREATE TABLE other (x)")
    con.commit()
    con.close()
    empty_table = str(tmp_path / "empty_table.db")
    con = sqlite3.connect(empty_table)
    con.execute("CREATE TABLE nli_images (NLI_AlmaId, FGPImageNumberId, FGPNumber, ImageName, "
                "ImageSourceName, Shelfmark)")
    con.commit()
    con.close()
    for bad in (not_sqlite, no_table, empty_table):
        with pytest.raises(ValueError):
            check_nli_crossref(bad)
        out = tmp_path / ("gen-" + os.path.basename(bad))
        code = build_editions_index.main([
            "--out", str(out), "--fgp-db", inputs["fgp_db"], "--pgp-db", inputs["pgp_db"],
            "--browse-map", inputs["browse_map"], "--nli-db", bad])
        assert code == 2 and not out.exists()
    with pytest.raises(ValueError):
        build_generation(str(tmp_path / "lib"), fgp_db=inputs["fgp_db"], pgp_db=inputs["pgp_db"],
                         browse_map_path=inputs["browse_map"], nli_db=None)


def test_gate_placement_floor(generation):
    import verify_editions_index as gate
    assert not gate.check_placement(generation["meta"]).ok     # the fixture places a handful
    ok = {"counts": {"located": {s: {"all": n, "part": 0} for s, n in gate.PLACED_CENSUS.items()}}}
    assert gate.check_placement(ok).ok


def test_gate_fails_without_the_nli_crossref(inputs, generation):
    import verify_editions_index as gate
    from shared.editions_indexer import input_stamp
    expected = dict(gate.expected_fgp(inputs["fgp_db"]))
    expected.update(gate.expected_pgp(inputs["pgp_db"]))
    stamp = input_stamp(inputs["fgp_db"], inputs["pgp_db"], inputs["browse_map"], None)
    membership = gate.run_checks(generation["dir"], expected, stamp, checks=("membership",))[0]
    assert not membership.ok and any("NLI crossref" in f for f in membership.failures)


def test_gate_passes_and_can_fail(inputs, generation, tmp_path):
    import verify_editions_index as gate
    from shared.editions_indexer import input_stamp
    expected = dict(gate.expected_fgp(inputs["fgp_db"]))
    expected.update(gate.expected_pgp(inputs["pgp_db"]))
    stamp = input_stamp(inputs["fgp_db"], inputs["pgp_db"], inputs["browse_map"], inputs["nli_db"])
    results = {r.name.split(" ")[0]: r for r in gate.run_checks(generation["dir"], expected, stamp,
                                                                  checks=("membership", "content", "readiness"))}
    assert all(r.ok for r in results.values()), [r.line() for r in results.values()]

    broken = str(tmp_path / "broken")
    docs = gate._docs_from_expected(expected, inputs["fgp_db"], inputs["pgp_db"],
                                    drop=lambda r: r["uid"] == "ED:fgp:7")
    build_generation(broken, fgp_db=inputs["fgp_db"], pgp_db=inputs["pgp_db"],
                     browse_map_path=inputs["browse_map"], documents=docs, folio_images_for=lambda s: [])
    membership = gate.run_checks(broken, expected, None, checks=("membership",))[0]
    assert not membership.ok and any("ED:fgp:7" in f for f in membership.failures)

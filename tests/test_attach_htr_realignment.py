# -*- coding: utf-8 -*-
"""Gate tests for scripts/attach_htr_realignment.py.

A miniature Transcriptions.txt, offsets index, corpus and review db. The green
fixture pins the five statuses and proves every stored file address slices
back to the matched letters; every gate is then handed the state it exists to
refuse.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
pytest.importorskip("rapidfuzz")
try:
    import attach_htr_realignment  # noqa: E402,F401
except ImportError as e:  # the spike normalizer is a gitignored tree
    pytest.skip("spike normalizer not available: %s" % e, allow_module_level=True)

from attach_htr_realignment import GateError, align_row, build, stream_pages  # noqa: E402

HTR = {
    # an ordinary HTR page: indexed in page_offsets, used for the self-check
    "p_htr": "בראשית ברא אלהים את השמים ואת הארץ\nוהארץ היתה תהו ובהו וחשך על פני תהום",
    # substituted pages: the HTR text stands in the file, the search text was FGP/PGP
    "p_exact": "שורה ראשונה של הדף\nויאמר אלהים יהי אור ויהי אור וירא אלהים את האור כי טוב\nשורה אחרונה",
    "p_fuzzy": "כותרת\nויאמר אלהים יהי רקיע בתוך המים ויהי מבדיל בין מים למים ויעש אלהים את הרקיע\nסוף",
    "p_noisy": "]ואמד אלחים יקוו המימ מתחח השמימ אל מקומ אחד ותראה היבשח ויחי כנ[",
    "p_twice": "ויקרא אלהים לאור יום ולחשך קרא לילה\nויקרא אלהים לאור יום ולחשך קרא לילה",
    # a Latin editorial note with a combining mark: NFC shortens the page
    "p_nfc": "note: café\nויהי ערב ויהי בקר יום אחד",
}
SUBS = {"p_exact": "fgp", "p_fuzzy": "fgp", "p_noisy": "pgp", "p_twice": "fgp",
        "p_nfc": "fgp"}


def write_file(path, pages):
    parts = []
    for pid, text in pages.items():
        parts.append("==> %s <==\n%s\n" % (pid, text))
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write("".join(parts))


def make_all(tmp_path, htr=HTR, subs=SUBS, rows=None, htr_n_override=None,
             offsets_override=None, size_delta=0):
    src = str(tmp_path / "Transcriptions.txt")
    write_file(src, htr)
    if size_delta:
        with open(src, "a", encoding="utf-8") as fh:
            fh.write("x" * size_delta)
    st = os.stat(src)
    pages, n = stream_pages(src, set(htr))
    # offsets index: the HTR page(s), plus the source contract
    idx = str(tmp_path / "transcriptions_index.db")
    con = sqlite3.connect(idx)
    con.execute("CREATE TABLE page_offsets(page_id TEXT PRIMARY KEY, "
                "file_char_start INT, file_char_end INT, n_chars INT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    for pid in htr:
        if pid in subs:
            continue
        t, a, b = pages[pid]
        if offsets_override and pid in offsets_override:
            a, b = offsets_override[pid]
        con.execute("INSERT INTO page_offsets VALUES (?,?,?,?)", (pid, a, b, b - a))
    for k, v in (("src_size_bytes", st.st_size), ("src_mtime", int(st.st_mtime)),
                 ("records_streamed", n)):
        con.execute("INSERT INTO meta VALUES (?,?)", (k, str(v)))
    con.commit()
    con.close()
    # corpus: every page, substituted ones with the search text and htr_n_chars
    corpus = str(tmp_path / "fullcorpus_v2.db")
    con = sqlite3.connect(corpus)
    con.execute("CREATE TABLE pages(page_id TEXT PRIMARY KEY, sys_id TEXT, "
                "n_chars INT, text TEXT, provenance TEXT, fgp_id INT, "
                "fgp_score REAL, htr_n_chars INT)")
    for pid, text in htr.items():
        prov = subs.get(pid, "htr")
        htr_n = len(text)
        if htr_n_override and pid in htr_n_override:
            htr_n = htr_n_override[pid]
        search = ("FGP " + text) if prov != "htr" else text
        con.execute("INSERT INTO pages VALUES (?,?,?,?,?,?,?,?)",
                    (pid, "99" + pid, len(search), search, prov,
                     7 if prov != "htr" else None,
                     91.5 if prov != "htr" else None, htr_n))
    con.commit()
    con.close()
    # review db
    db = str(tmp_path / "review.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY, page_id TEXT, "
                "ms_match TEXT, ms_provenance_status TEXT, page_char_start INT, "
                "page_char_end INT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    if rows is None:
        rows = [
            ("e_exact", "p_exact", "ויאמר אלהים יהי אור ויהי אור", "offsets_missing"),
            # one letter off (רקיע -> רקיא) and a dropped word: still > 90
            ("e_fuzzy", "p_fuzzy", "ויאמר אלהים יהי רקיא בתוך המים ויהי מבדיל בין מים למים", "offsets_missing"),
            # the human text is clean; the HTR is very noisy
            ("e_noisy", "p_noisy", "ויאמר אלהים יקוו המים מתחת השמים אל מקום אחד ותראה היבשה ויהי כן", "offsets_missing"),
            ("e_twice", "p_twice", "ויקרא אלהים לאור יום", "offsets_missing"),
            ("e_short", "p_exact", "כי טוב", "offsets_missing"),
            ("e_nfc", "p_nfc", "ויהי ערב ויהי בקר", "offsets_missing"),
            ("e_ok", "p_htr", "בראשית ברא אלהים", "ok"),
        ]
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,NULL,NULL)", rows)
    con.commit()
    con.close()
    return dict(db=db, src=src, idx=idx, corpus=corpus, pages=pages)


def q(db, sql, *a):
    con = sqlite3.connect(db)
    try:
        return con.execute(sql, a).fetchall()
    finally:
        con.close()


def file_text(src):
    with open(src, encoding="utf-8", errors="replace") as fh:
        return fh.read()


def letters(s):
    from normalize import norm_stream
    return norm_stream(s)[0]


def test_green_statuses_and_every_file_address_slices_back(tmp_path):
    f = make_all(tmp_path)
    counts = build(f["db"], f["src"], f["idx"], f["corpus"])
    assert counts == {"exact": 2, "realigned_htr": 1, "realign_uncertain": 1,
                      "ambiguous": 1, "unalignable": 1}
    got = {r[0]: r[1:] for r in q(
        f["db"], "SELECT evidence_id, htr_align_status, htr_align_score, "
        "htr_page_char_start, htr_page_char_end, htr_file_char_start, "
        "htr_file_char_end FROM review_row")}
    assert got["e_exact"][0] == "exact" and got["e_exact"][1] == 100.0
    assert got["e_fuzzy"][0] == "realigned_htr" and got["e_fuzzy"][1] >= 90
    assert got["e_noisy"][0] == "realign_uncertain" and got["e_noisy"][1] < 90
    assert got["e_twice"][0] == "ambiguous" and got["e_twice"][2] is None
    assert got["e_short"][0] == "unalignable" and got["e_short"][2] is None
    # the untouched row
    assert got["e_ok"] == (None, None, None, None, None, None)
    # every file address slices the FILE back to exactly the aligned letters
    text = file_text(f["src"])
    ms = {r[0]: r[1] for r in q(f["db"], "SELECT evidence_id, ms_match FROM review_row")}
    for ev in ("e_exact", "e_fuzzy"):
        st, sc, pa, pb, fa, fb = got[ev]
        assert fa is not None and fb > fa
        assert letters(text[fa:fb]) == letters(ms[ev]) if ev == "e_exact" else True
        # the file slice and the page slice are the same characters
        page = f["pages"][q(f["db"], "SELECT page_id FROM review_row WHERE evidence_id=?", ev)[0][0]][0]
        assert text[fa:fb] == page[pa:pb]
    # the uncertain row still carries its best window, honestly labelled
    st, sc, pa, pb, fa, fb = got["e_noisy"]
    assert fa is not None and text[fa:fb] == f["pages"]["p_noisy"][0][pa:pb]


def test_htr_page_table_holds_every_substituted_page(tmp_path):
    f = make_all(tmp_path)
    build(f["db"], f["src"], f["idx"], f["corpus"])
    rows = {r[0]: r[1:] for r in q(
        f["db"], "SELECT page_id, search_text_source, substitution_score, "
        "htr_text, htr_n_chars, htr_file_char_start, htr_file_char_end, nfc_ok, "
        "in_review_set FROM htr_page")}
    assert set(rows) == set(SUBS)
    text = file_text(f["src"])
    for pid, (src_kind, score, htr_text, n, fa, fb, nfc_ok, in_set) in rows.items():
        assert src_kind == SUBS[pid] and score == 91.5
        assert htr_text == HTR[pid] and n == len(HTR[pid])
        assert in_set == 1
        if pid == "p_nfc":
            assert nfc_ok == 0 and fa is None and fb is None
        else:
            assert nfc_ok == 1 and text[fa:fb] == HTR[pid]
    meta = dict(q(f["db"], "SELECT key, value FROM meta WHERE key LIKE 'htr_realign.%'"))
    assert meta["htr_realign.rows"] == "6"
    assert meta["htr_realign.substituted_pages"] == "5"
    assert meta["htr_realign.nfc_shift_pages"] == "1"
    assert q(f["db"], "SELECT value FROM meta WHERE key='doc.htr_realign'")


def test_nfc_shift_page_keeps_page_offsets_but_no_file_address(tmp_path):
    f = make_all(tmp_path)
    build(f["db"], f["src"], f["idx"], f["corpus"])
    (st, pa, pb, fa, fb), = q(
        f["db"], "SELECT htr_align_status, htr_page_char_start, htr_page_char_end, "
        "htr_file_char_start, htr_file_char_end FROM review_row WHERE evidence_id='e_nfc'")
    assert st == "exact" and pa is not None and pb > pa
    assert fa is None and fb is None
    import unicodedata
    nfc = unicodedata.normalize("NFC", HTR["p_nfc"])
    assert letters(nfc[pa:pb]) == letters("ויהי ערב ויהי בקר")


def test_rerun_is_idempotent(tmp_path):
    f = make_all(tmp_path)
    a = build(f["db"], f["src"], f["idx"], f["corpus"])
    before = q(f["db"], "SELECT * FROM review_row ORDER BY evidence_id")
    b = build(f["db"], f["src"], f["idx"], f["corpus"])
    assert a == b
    assert q(f["db"], "SELECT * FROM review_row ORDER BY evidence_id") == before
    cols = [r[1] for r in q(f["db"], "PRAGMA table_info(review_row)")]
    assert cols.count("htr_align_status") == 1


def test_refuses_a_changed_source_file(tmp_path):
    f = make_all(tmp_path)
    with open(f["src"], "a", encoding="utf-8") as fh:
        fh.write("\n")
    with pytest.raises(GateError, match="size"):
        build(f["db"], f["src"], f["idx"], f["corpus"])
    assert "htr_align_status" not in [r[1] for r in q(f["db"], "PRAGMA table_info(review_row)")]


def test_refuses_htr_length_drift(tmp_path):
    f = make_all(tmp_path, htr_n_override={"p_fuzzy": 3})
    with pytest.raises(GateError, match="htr_n_chars"):
        build(f["db"], f["src"], f["idx"], f["corpus"])


def test_refuses_a_substituted_page_absent_from_the_file(tmp_path):
    htr = dict(HTR)
    subs = dict(SUBS)
    subs["p_ghost"] = "fgp"
    f = make_all(tmp_path, htr=htr, subs=subs)
    # the corpus knows p_ghost as substituted, the file has no such record
    con = sqlite3.connect(f["corpus"])
    con.execute("INSERT INTO pages VALUES ('p_ghost','99g',10,'FGP text','fgp',1,80.0,300)")
    con.commit()
    con.close()
    with pytest.raises(GateError, match="absent from the file"):
        build(f["db"], f["src"], f["idx"], f["corpus"])


def test_refuses_a_row_on_a_page_the_corpus_calls_htr(tmp_path):
    rows = [("e_bad", "p_htr", "בראשית ברא אלהים", "offsets_missing")]
    f = make_all(tmp_path, rows=rows)
    with pytest.raises(GateError, match="not call substituted"):
        build(f["db"], f["src"], f["idx"], f["corpus"])


def test_refuses_when_the_selfcheck_span_disagrees(tmp_path):
    f = make_all(tmp_path, offsets_override={"p_htr": (1, 10)})
    with pytest.raises(GateError, match="self-check"):
        build(f["db"], f["src"], f["idx"], f["corpus"])


def test_refuses_a_live_writer_marker(tmp_path):
    f = make_all(tmp_path)
    open(f["db"] + "-journal", "w").close()
    with pytest.raises(GateError, match="unfinished transaction"):
        build(f["db"], f["src"], f["idx"], f["corpus"])


def test_status_is_decided_on_the_stored_rounded_score(monkeypatch):
    """89.96 is stored and shown as 90.0, so it must also be classified as 90.0:
    the reader must never see '90.0' beside 'uncertain'."""
    import attach_htr_realignment as m
    from normalize import norm_stream
    from rapidfuzz.fuzz import ratio
    h, hoffs = norm_stream("אבגד הוזח טיכל מנסע")
    q = "אבגדהוזחטיכמ"                    # one letter off the page: not verbatim

    class _Al:
        src_start, src_end = 0, len(q)
        dest_start, dest_end = 0, 12
        score = ratio(q, h[0:12])         # the honest score of that window

    monkeypatch.setattr(m, "partial_ratio_alignment", lambda a, b: _Al())
    monkeypatch.setattr(m, "SCORE_TOLERANCE", 100.0)   # isolate the rounding rule
    for raw, shown, status in ((89.96, 90.0, "realigned_htr"),
                               (89.94, 89.9, "realign_uncertain")):
        _Al.score = raw
        st, sc, pa, pb, let = m.align_row(q, h, hoffs)
        assert (sc, st) == (shown, status)


def test_a_window_that_does_not_reproduce_its_score_is_refused(monkeypatch):
    """The content check: an in-bounds but unrelated window (what an
    argument-order bug would produce) is refused even though it slices back
    perfectly through hoffs."""
    import attach_htr_realignment as m
    from normalize import norm_stream
    page = "אבגד הוזח טיכל מנסע פצקר שתאב גדהו"
    h, hoffs = norm_stream(page)
    q = "פצקרשתאבגדהוז"                    # the tail of the page plus one letter: not verbatim
    assert h.find(q) < 0

    class _Wrong:                          # claims a high score for the wrong span
        src_start, src_end = 0, len(q)
        dest_start, dest_end = 0, 12       # the HEAD of the page, unrelated to q
        score = 95.0

    monkeypatch.setattr(m, "partial_ratio_alignment", lambda a, b: _Wrong())
    with pytest.raises(GateError, match="does not reproduce its own score"):
        m.align_row(q, h, hoffs)
    # and the genuine aligner on the same inputs is accepted, at its true window
    monkeypatch.undo()
    st, sc, pa, pb, let = m.align_row(q, h, hoffs)
    assert st in ("realigned_htr", "realign_uncertain")
    assert norm_stream(page[pa:pb])[0] == let
    assert let.startswith("פצקר")


def test_align_row_never_guesses():
    from normalize import norm_stream
    page = "אבגד הוזח טיכל מנסע אבגד הוזח טיכל פצקר שתאב"
    h, hoffs = norm_stream(page)
    # occurs twice -> ambiguous, no offsets
    assert align_row("אבגדהוזחטיכל", h, hoffs)[0] == "ambiguous"
    # too short -> unalignable, whatever the page says
    assert align_row("אבגדהוזחט", h, hoffs)[0] == "unalignable"
    # once -> exact with offsets that slice back
    st, sc, pa, pb, let = align_row("טיכלפצקרשתאב", h, hoffs)
    assert st == "exact" and sc == 100.0
    assert norm_stream(page[pa:pb])[0] == "טיכלפצקרשתאב" == let

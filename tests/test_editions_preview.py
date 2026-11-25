"""The first rows of a search with the scholarly editions, while it runs (2026-11-25).

With the editions the list is a fusion of two legs, folded: what its first result is depends on
rows found later. The early rows must still be the start of the finished list, as they are
without the editions -- so ``GroupedPrefix`` hands over only the results no later row can move.
Here: against the finished list on thousands of made-up searches, at every step of each, and
through the real engine on a small pair of indexes. The same comparison on the real indexes is
a script of the PR's, run by hand.
"""
import copy
import gc
import os
import random
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

import shared.editions_preview as ep  # noqa: E402
import shared.editions_search as es  # noqa: E402
import shared.search_engine as se  # noqa: E402
from shared.edition_search_text import SOURCE_FGP  # noqa: E402
from shared.editions_indexer import build_generation  # noqa: E402
from shared.editions_search import _fold_clusters, _fold_linked, _group_ranked, group_results  # noqa: E402
from tests.responsa_index_fixture import build, engine as make_engine, uid  # noqa: E402
from tests.test_editions_search import _doc, _sidecars  # noqa: E402


# --- made-up searches ------------------------------------------------------------------------

def _main(page, sid, source="V0.8"):
    return {"uid": page, "display": {"id": sid, "source": source}, "snippet": page}


def _edition(kind, eid, sid, page=None, span=(0, 4)):
    uid = f"ED:{kind}:{eid}" + (f":{sid}" if kind == "pgp" else "")
    return {"uid": uid, "group_key": page or uid, "display": {"id": sid, "source": f"ED_{kind}"}, "snippet": uid,
            "edition": {"kind": kind, "id": str(eid), "uid": uid, "version": f"{kind}:{eid}", "sys_id": sid,
                        "span": list(span)}}


def _search(rng, small):
    """One search: the main leg's hits in the order they are read (a page may come as V0.7 and as
    V0.8, some only across a page break, at the end), and the editions leg's rows."""
    n_sids = rng.randint(1, 4 if small else 25)
    pages = {f"9{s:03d}": [f"9{s:03d}_p{n}" for n in range(rng.randint(1, 5))] for s in range(n_sids)}
    all_pages = [(p, sid) for sid, ps in pages.items() for p in ps]
    hits = []
    for page, sid in all_pages:
        if rng.random() < 0.6:
            hits.append(_main(page, sid))
        if rng.random() < 0.3:
            hits.append(_main(page, sid, "V0.7"))
    rng.shuffle(hits)
    page_hits = len(hits)
    for page, sid in rng.sample(all_pages, rng.randint(0, min(3, len(all_pages)))):
        hits.append(_main(page, sid))
    editions, seen = [], set()
    for eid in range(rng.randint(0, 6 if small else 40)):
        kind = rng.choice(("fgp", "pgp"))
        for sid in rng.sample(sorted(pages), min(len(pages), rng.choice((1, 1, 1, 2, 3)) if kind == "pgp" else 1)):
            for page in rng.sample([None, *pages[sid]], rng.choice((1, 1, 2)) if pages[sid] else 1):
                row = _edition(kind, eid, sid, page, span=rng.choice(((0, 4), (0, 4), (9, 13))))
                if (row["group_key"], row["uid"]) not in seen:
                    seen.add((row["group_key"], row["uid"]))
                    editions.append(row)
    rng.shuffle(editions)
    return hits, page_hits, editions


def _order(hits, page_hits, n):
    return (0, n) if n < page_hits else (1, n - page_hits)


def _shape(rows):
    return [(r["uid"], r["snippet"], [m["uid"] for m in r["members"]], r["sources"], r.get("folded"), r.get("also_in"),
             r["edition_match"], r["page_identified"]) for r in rows]


def _steps(hits, page_hits, editions, every=1):
    """(the early rows so far) as the engine reads hit after hit, answering what is wanted."""
    prefix = ep.GroupedPrefix(editions)
    for n, row in enumerate(hits):
        if n % every == 0:
            while True:
                pages, sids, page_sids = prefix.wanted()
                if not (pages or sids):
                    break
                read = []
                for m in range(n, len(hits)):
                    h = hits[m]
                    if h["uid"] in pages or h["display"]["id"] in sids:
                        # A V0.8 page row ranks no lower than every hit between giving a row.
                        both_pages = n < page_hits and m < page_hits
                        read.append((_order(hits, page_hits, m), prefix.settled + m - n + 1 if both_pages else None, h))
                prefix.checked(read)
            yield prefix.rows(50)
        prefix.add_main(row, _order(hits, page_hits, n))


def _finished(hits, editions):
    leg = se.SearchEngine._deduplicate(se.SearchEngine.__new__(se.SearchEngine), hits, first_wins=True)
    return group_results(leg, editions)


@pytest.mark.parametrize("small", [True, False], ids=["small", "large"])
def test_the_early_rows_are_the_start_of_the_finished_list_at_every_step(small):
    rng = random.Random(11 if small else 12)
    shown = folded = joined = 0
    for _ in range(1500 if small else 150):
        hits, page_hits, editions = _search(rng, small)
        final = _shape(_finished(hits, editions))
        before = 0
        for rows in _steps(hits, page_hits, editions):
            early = _shape(rows)
            assert early == final[:len(early)], (hits, editions)
            assert len(early) >= before, "a later call begins with what an earlier one gave"
            before = len(early)
        shown += before
        folded += sum(bool(r[4]) for r in final[:before])
        joined += sum(len(r[2]) > 1 for r in final[:before])
    # The comparison is worth something only if rows were shown, folded and joined ones too.
    assert shown > 500 and folded > 50 and joined > 100, (shown, folded, joined)


def test_a_page_both_legs_found_is_shown_before_its_main_row_is_settled():
    """The page the best edition is on is far down the main leg: its result is second by the
    editions alone, and above both first rows whatever the page ranks."""
    main = [_main(f"991_p{n}", "991") for n in range(60)]
    editions = [_edition("fgp", 1, "991", page="991_p50")]
    steps = list(_steps(main, len(main), editions))
    first = next(rows for rows in steps if rows)
    assert [m["uid"] for m in first[0]["members"]] == ["ED:fgp:1", "991_p50"]
    assert len([rows for rows in steps if not rows]) < 5


def test_nothing_is_shown_on_a_guess():
    main = [_main(f"991_p{n}", "991") for n in range(60)]
    editions = [_edition("fgp", 1, "991", page="991_p50")]
    prefix = ep.GroupedPrefix(editions)
    prefix.add_main(main[0], (0, 0))
    # Whether the edition's page matches in the main index is not known yet.
    assert prefix.rows(50) == []


def test_a_tie_keeps_the_fusions_own_order():
    # Rank 1 of each leg, on different pages: equal scores, the main leg's first.
    main = [_main("991_p1", "991"), _main("991_p2", "991"), _main("991_p3", "991")]
    editions = [_edition("fgp", 1, "992", page="992_p1"), _edition("fgp", 2, "993", page="993_p1")]
    last = list(_steps([*main, _main("991_p4", "991")], 4, editions))[-1]
    assert [r["uid"] for r in last][:4] == ["991_p1", "992_p1", "991_p2", "993_p1"]


def test_check_preview_says_when_the_early_rows_were_not_the_start():
    rows = [{"uid": "a"}, {"uid": "b"}]
    with patch.object(ep.LOGGER, "error") as said:
        assert ep.check_preview(["a"], rows) and not said.called
        assert not ep.check_preview(["b"], rows) and "not its first rows" in said.call_args.args[0]


def test_what_the_prefix_stops_at_is_asked_for():
    """A page that matters from far down the editions: found when the prefix reaches it."""
    editions = [_edition("fgp", n, f"8{n:03d}", page=f"8{n:03d}_p1") for n in range(200)]
    main = [_main("8199_p1", "8199"), *(_main(f"991_p{n}", "991") for n in range(400))]
    prefix = ep.GroupedPrefix(editions)
    for n, row in enumerate(main[:300]):
        prefix.add_main(row, (0, n))
    with patch.object(ep, "_LOOKUP_REACH", 0):
        assert prefix.rows(50) == [] and prefix.asks
        pages, sids, _page_sids = prefix.wanted()
        assert pages == {"8199_p1"} and not sids
        prefix.checked([])
        assert [r["uid"] for r in prefix.rows(50)][:1] == ["8199_p1"]


# --- folding only what can fold --------------------------------------------------------------

def test_folding_passes_over_the_rows_that_cannot_fold_and_gives_the_same_results():
    rng = random.Random(7)
    for n in range(800):
        hits, _pages, editions = _search(rng, n % 2 == 0)
        leg = se.SearchEngine._deduplicate(se.SearchEngine.__new__(se.SearchEngine), hits, first_wins=True)
        rows = _group_ranked(enumerate(leg, 1), enumerate(editions, 1), [(1, {"uid": "L1", "display": {"id": "97"}})])
        whole = [[id(r) for r in c] for c in _fold_linked(rows)]
        assert [[id(r) for r in c] for c in _fold_clusters(rows)] == whole
    main = [_main(f"991_p{n}", "991") for n in range(500)]
    with patch.object(es, "_fold_linked", side_effect=_fold_linked) as looked_at:
        group_results(main, [_edition("fgp", 1, "992", page="992_p1")])
        # The automatic rows of a manuscript with no unplaced edition are not looked at ...
        assert len(looked_at.call_args.args[0]) == 1
        group_results(main, [_edition("fgp", 1, "991")])
        assert len(looked_at.call_args.args[0]) == 501       # ... those of one that has, are


def test_the_collector_is_paused_while_rows_are_grouped_and_on_again_after(monkeypatch):
    from shared import gc_pause
    during = []
    fold = es.fold_same_text
    monkeypatch.setattr(es, "fold_same_text", lambda rows: during.append(gc.isenabled()) or fold(rows))
    assert gc.isenabled()
    se.SearchEngine.merge_search_legs({"rows": [_main("991_p1", "991")], "leg": {}}, None)
    assert during == [False] and gc.isenabled()
    with gc_pause.gc_paused():
        with gc_pause.gc_paused():
            assert not gc.isenabled()
        assert not gc.isenabled(), "the outer pause still holds"
    assert gc.isenabled()
    gc.disable()
    try:
        with gc_pause.gc_paused():
            pass
        assert not gc.isenabled(), "a collector that was off stays off"
    finally:
        gc.enable()


# --- through the engine ----------------------------------------------------------------------
#
# 40 manuscripts of two pages, built by the real Indexer (so with seams): the phrase on one page,
# or begun on the first page and ended on the second; editions of some of them, on the page, on a
# page the phrase only starts on, and on no named page.

WORD, NEXT, FILL = "שלום", "עליכם", "עוד"
R = {"responsa_mode": True, "variants": False, "ja": False, "flex_spacing": False, "bidirectional": False,
     "variant_mode": "exact"}
QUERIES = [(WORD, "literal", {}), (f"{WORD} {NEXT}", "literal", {}), (f"{WORD} {NEXT}", "variants", {}),
           (WORD, "fuzzy", {}), (WORD, "responsa", {"responsa_options": R}),
           (f"{WORD} {NEXT}", "responsa", {"responsa_options": R}), ("של.ם", "Regex", {}),
           (WORD, "literal", {"text_position": "line_start"}),
           (WORD, "literal", {"restrict_sys_ids": {f"99000000{n:03d}0205171" for n in range(5, 30)}})]
IDS = [f"{m}:{q}:{'+'.join(k)}" for q, m, k in QUERIES]


@pytest.fixture(scope="module")
def shelf(tmp_path_factory):
    base = tmp_path_factory.mktemp("editions-preview")
    main, docs = [], []
    for n in range(40):
        sid = f"99000000{n:03d}0205171"
        # A different length gives a different score: the hits are not in shelf order.
        fill = " ".join([FILL] * (3 * n % 41))
        if n % 3 == 0:
            first, second = f"{FILL} {fill} {WORD}", f"{NEXT} {FILL}"        # across the page break
        elif n % 3 == 1:
            first, second = f"{WORD} {NEXT} {fill}", FILL
        else:
            first, second = f"{WORD} {fill} {NEXT}", f"{WORD} {NEXT} {fill}"
        main += [(sid, 100 + n, 1, first), (sid, 100 + n, 2, second)]
        if n % 4 == 0:
            docs.append(_doc(SOURCE_FGP, 300 + n, sid, f"{WORD} {NEXT} {fill}".strip(), [(WORD, uid(100 + n, 1))]))
        if n % 5 == 0:
            docs.append(_doc(SOURCE_FGP, 400 + n, sid, f"{WORD} {NEXT} {FILL} {fill}".strip(), [(WORD, None)]))
    root = build(base / "main", main)
    fgp, pgp = _sidecars(str(base))
    gen = str(base / "Editions")
    build_generation(gen, fgp_db=fgp, pgp_db=pgp, browse_map_path=os.path.join(root, "browse_map.pkl"),
                     folio_images_for=lambda s: [], documents=docs, progress=lambda m: None)
    browse = patch.object(se.Config, "BROWSE_MAP", os.path.join(root, "browse_map.pkl"))
    browse.start()
    se.SearchEngine._shared_browse_map = None
    eng = make_engine(root)
    assert eng.attach_editions(gen) and eng._seams_ready()
    try:
        yield eng
    finally:
        eng.close_index()
        se.SearchEngine._shared_browse_map = None
        browse.stop()


@pytest.fixture
def eager(monkeypatch):
    """The early rows worked out at every row found."""
    monkeypatch.setattr(se, "_FEED_FIRST", 1)
    monkeypatch.setattr(se, "_PREVIEW_AFTER_S", 0.0)


def _run(eng, query, mode, kw, **more):
    return eng.execute_search(query, mode, 0, corpus_scope="genizah", sources="all", **copy.deepcopy(kw), **more)


@pytest.mark.parametrize("query, mode, kw", QUERIES, ids=IDS)
def test_a_search_with_editions_shows_the_start_of_its_list_while_it_runs(shelf, eager, query, mode, kw):
    early = []
    with patch.object(ep.LOGGER, "error") as flaw, patch.object(se.LOGGER, "exception") as failed:
        rows = _run(shelf, query, mode, kw, preview_callback=lambda r: early.append(copy.deepcopy(r)))
    flaw.assert_not_called()
    failed.assert_not_called()
    cut = se.consume_last_search_cutoff()
    assert rows == _run(shelf, query, mode, kw), "the finished list is the one without early rows"
    assert cut == se.consume_last_search_cutoff()
    assert early, "early rows while the search runs"
    for r in early:
        assert r == [{k: v for k, v in row.items() if not k.startswith("responsa_")} for row in rows[:len(r)]]
    assert [len(r) for r in early] == sorted({len(r) for r in early}), "each one longer than the one before"
    assert len(early[-1]) > 3 and any(len(r["members"]) > 1 for r in early[-1])


def test_the_matches_across_a_page_break_are_read_ahead(shelf, eager):
    """An edition on a page the phrase only starts on: its page's row comes from a seam, last of
    all, and its result is shown with it before that."""
    early = []
    _run(shelf, f"{WORD} {NEXT}", "literal", {}, preview_callback=early.append)
    crossing = {uid(100 + n, 1) for n in range(0, 40, 12)}
    assert any(m["uid"] in crossing and m["scope"] == "seam" for r in early[-1] for m in r["members"])


def test_the_hits_read_ahead_are_read_once(shelf, eager):
    """Their rows are kept for their turn: reading ahead adds no work to the search."""
    def read(**more):
        with patch.object(shelf, "_highlight_pair", side_effect=shelf._highlight_pair) as built:
            _run(shelf, WORD, "literal", {}, **more)
        return built.call_count
    assert read(preview_callback=lambda r: None) == read()


@pytest.mark.parametrize("query, mode, kw", [
    (WORD, "literal", {"exclude_words": [FILL]}), (f"{WORD} -{FILL}", "responsa", {"responsa_options": R}),
    ("של*", "responsa", {"responsa_options": R}), (WORD, "literal", {"ids_only": True}),
], ids=["exclude_words", "minus_word", "all_star", "ids_only"])
def test_no_early_rows_where_a_later_step_drops_or_reorders_them(shelf, eager, query, mode, kw):
    early = []
    assert _run(shelf, query, mode, kw, preview_callback=early.append) == _run(shelf, query, mode, kw)
    assert early == []


def test_the_legs_of_two_workers_show_the_same_early_rows(shelf, eager):
    """The website: the main leg's worker shows them once the editions leg's rows are there."""
    whole = _run(shelf, WORD, "literal", {})
    editions = shelf.execute_search_leg(WORD, "literal", 0, "editions", corpus_scope="genizah")
    asked, early = [], []

    def there():
        asked.append(1)
        return editions["rows"] if len(asked) > 2 else None
    with patch.object(ep, "_POLL_SECONDS", 0.0):
        main = shelf.execute_search_leg(WORD, "literal", 0, "main", corpus_scope="genizah",
                                        preview_callback=lambda r: early.append(copy.deepcopy(r)), edition_rows=there)
    rows, _warning = se.SearchEngine.merge_search_legs(main, editions)
    assert rows == whole and early and early[-1] == rows[:len(early[-1])]
    # Never there (the editions leg failed, or waits in its queue): no early rows, the same list.
    early.clear()
    alone = shelf.execute_search_leg(WORD, "literal", 0, "main", corpus_scope="genizah",
                                     preview_callback=early.append, edition_rows=lambda: None)
    assert alone["rows"] == main["rows"] and early == []


def test_a_fault_in_the_early_rows_costs_only_them(shelf, eager):
    with patch.object(ep.GroupedPrefix, "rows", side_effect=RuntimeError("fault")), \
            patch.object(se.LOGGER, "exception") as said:
        rows = _run(shelf, WORD, "literal", {}, preview_callback=lambda r: None)
    assert said.called and rows == _run(shelf, WORD, "literal", {})


def test_a_stop_while_reading_ahead_leaves_no_row_out_of_its_place(shelf, eager):
    """A stopped search's rows are in their order, whatever was being read ahead."""
    whole = [r["uid"] for r in shelf.execute_search_leg(WORD, "literal", 0, "main", corpus_scope="genizah")["rows"]]
    editions = shelf.execute_search_leg(WORD, "literal", 0, "editions", corpus_scope="genizah")["rows"]
    for stop_at in range(1, 40):
        calls = []

        def progress(*_a):
            calls.append(1)
            if len(calls) > stop_at:
                raise InterruptedError
        with patch.object(se, "_PROGRESS_TICK_EVERY", 1):
            part = shelf.execute_search_leg(WORD, "literal", 0, "main", corpus_scope="genizah",
                                            progress_callback=progress, preview_callback=lambda r: None,
                                            edition_rows=lambda: editions)["rows"]
        se.consume_last_search_cutoff()
        assert [r["uid"] for r in part] == whole[:len(part)], stop_at


def test_a_refused_crossing_query_is_the_searchs_error_also_when_read_ahead(shelf, eager):
    """Not a fault of the early rows, after which the search would go on without its crossings
    (Codex review of dev PR #128)."""
    editions = shelf.execute_search_leg(f"{WORD} {NEXT}", "responsa", 0, "editions", corpus_scope="genizah",
                                        responsa_options=dict(R))["rows"]
    asked = []

    def refuse(*_a):
        asked.append(1)
        raise se.ResponsaQueryRefused("refused")
    with patch.object(shelf, "_responsa_seam_query", side_effect=refuse), pytest.raises(se.ResponsaQueryRefused):
        shelf.execute_search_leg(f"{WORD} {NEXT}", "responsa", 0, "main", corpus_scope="genizah",
                                 responsa_options=dict(R), preview_callback=lambda r: None,
                                 edition_rows=lambda: editions)
    assert asked == [1]


@pytest.mark.parametrize("crossings", [f"{WORD} {NEXT}", WORD], ids=["seams", "none"])
def test_a_stop_is_met_before_the_crossings_are_read_ahead(shelf, eager, crossings):
    """The second query cannot be stopped: the checkpoint before it holds for the preview too
    (Codex review of dev PR #128)."""
    editions = shelf.execute_search_leg(crossings, "literal", 0, "editions", corpus_scope="genizah")["rows"]
    asked, ticks = [], []
    real = se._top_hits

    def top_hits(searcher, query, limit):
        asked.append(searcher is shelf.seam_searcher)
        return real(searcher, query, limit)

    def progress(*_a):
        ticks.append(len(asked))
        if len(ticks) > 1:          # the first tick is the first page's; the stop comes after it
            raise InterruptedError
    with patch.object(se, "_top_hits", top_hits), patch.object(se, "_PROGRESS_TICK_EVERY", 10**9), \
            patch.object(se, "_PROGRESS_TICK_SECONDS", 10**9):
        shelf.execute_search_leg(crossings, "literal", 0, "main", corpus_scope="genizah", progress_callback=progress,
                                 preview_callback=lambda r: None, edition_rows=lambda: editions)
    stopped = se.consume_last_search_cutoff()["interrupted"]
    assert (True in asked) is False
    assert stopped is (crossings != WORD)


# --- the workers' files ----------------------------------------------------------------------

def test_the_edition_rows_are_left_whole_and_only_the_first_with_their_texts(tmp_path, monkeypatch):
    from shared import research_worker as rw
    monkeypatch.setattr(rw, "HANDED_WITH_TEXT", 2)
    rows = [dict(_edition("fgp", n, "991"), full_text="t" * n, raw_file_hl="h") for n in range(4)]
    path = tmp_path / "editions.pkl"
    assert rw.read_editions_result(path) is None
    result = {"value": {"rows": rows, "leg": {"warning": None}}, "cutoff": {"capped": True}}
    rw.write_editions_result(path, result)
    left = rw.read_editions_result(path)
    assert left["cutoff"] == {"capped": True} and left["value"]["leg"] == {"warning": None}
    assert result["value"]["rows"] is rows and "full_text" in rows[3], "the result itself is as it was"
    read = left["value"]["rows"]
    assert read[:2] == rows[:2]
    assert read[2:] == [{k: v for k, v in r.items() if k not in ("full_text", "raw_file_hl")} for r in rows[2:]]
    assert [p.name for p in tmp_path.iterdir()] == ["editions.pkl"]
    rw.write_editions_result(tmp_path / "gone" / "editions.pkl", result)      # the search is over: no error


@pytest.mark.parametrize("where", ["light rows", "saving"])
def test_no_memory_for_the_handed_rows_costs_only_the_early_rows(tmp_path, monkeypatch, where):
    # Codex review of dev PR #128: the editions leg is complete by then, its rows still to be saved.
    from shared import research_worker as rw

    class Full(dict):
        def items(self):
            raise MemoryError
    monkeypatch.setattr(rw, "HANDED_WITH_TEXT", 0)
    rows = [Full(uid="P1")] if where == "light rows" else [{"uid": "P1"}]
    with patch.object(rw.pickle, "dump", side_effect=MemoryError if where == "saving" else None):
        rw.write_editions_result(tmp_path / "editions.pkl", {"value": {"rows": rows}})
    assert list(tmp_path.iterdir()) == []


def test_the_editions_worker_leaves_its_rows_and_the_main_one_reads_them(tmp_path, monkeypatch):
    import pickle
    from shared import research_worker as rw
    monkeypatch.setenv("GENIZAH_RESEARCH_MEMORY_MB", "64")
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "1")
    rows, given = [_edition("fgp", 1, "991")], {}

    class Engine:
        searcher = object()

        def __init__(self, *a, **k):
            pass

        def execute_search_leg(self, **kw):
            given.update(kw)
            if kw["leg"] == "main":
                kw["preview_callback"](kw["edition_rows"]())
            return {"rows": rows if kw["leg"] == "editions" else [], "leg": {}}

    def run(leg, **payload):
        root = tmp_path / leg
        root.mkdir(exist_ok=True)
        with patch("shared.search_engine.SearchEngine", Engine), patch("shared.lab_engine.LabEngine"), \
                patch("shared.variants.VariantManager"), \
                patch("shared.editions_search.open_editions_engine", lambda *a: Engine()):
            rw.run_query(root, {"kind": "search", "method": "execute_search_leg", **payload,
                                "arguments": {"leg": leg, "mode": "literal", "corpus_scope": "genizah"}},
                         lambda *a: None, meta=object())
        with rw.open_output(root / "output.pkl") as stream:
            assert "error" not in pickle.load(stream)
        return root
    shared = {"preview": True, "editions_file": str(tmp_path / "editions.pkl")}
    run("editions", **shared)
    assert "preview_callback" not in given
    assert rw.read_editions_result(shared["editions_file"])["value"]["rows"] == rows
    root = run("main", **shared)
    assert pickle.loads((root / "preview.pkl").read_bytes()) == {"sequence": 1, "rows": rows}


@pytest.mark.parametrize("letters, plain_max, allowed_mb, plain", [
    (10, 1000, "512", True), (4000, 1000, "512", False),
    # What the parent takes (GENIZAH_RESEARCH_RESULT_MB) holds too (Codex review of dev PR #128):
    # a repetitive result it would refuse plain fits it compressed, as before.
    (3 * 1024**2, 256 * 1024**2, "1", False), (3 * 1024**2, 256 * 1024**2, "4", True),
], ids=["plain", "compressed", "past_the_parents_limit", "within_it"])
def test_a_result_is_saved_plain_up_to_a_size_and_read_either_way(tmp_path, monkeypatch, letters, plain_max,
                                                                  allowed_mb, plain):
    import json
    import pickle
    from shared import research_worker as rw
    monkeypatch.setattr(rw, "PLAIN_OUTPUT_MAX_BYTES", plain_max)
    monkeypatch.setenv("GENIZAH_RESEARCH_MEMORY_MB", "64")
    monkeypatch.setenv("GENIZAH_RESEARCH_RESULT_MB", allowed_mb)
    rows = [{"uid": "P1", "full_text": "t" * letters}]
    done = []

    class Engine:
        searcher = object()

        def __init__(self, *a, **k):
            pass

        def execute_search(self, **kw):
            return rows
    with patch("shared.search_engine.SearchEngine", Engine), patch("shared.lab_engine.LabEngine"), \
            patch("shared.variants.VariantManager"):
        rw.run_query(tmp_path, {"kind": "search", "method": "execute_search",
                                "arguments": {"mode": "literal", "corpus_scope": "genizah"}}, lambda *a: None,
                     meta=object(), done=lambda: done.append((tmp_path / "output-size.json").exists()))
    assert done == [True], "called once the result is saved"
    assert ((tmp_path / "output.pkl").read_bytes()[:2] != b"\x1f\x8b") is plain
    assert (tmp_path / "output.pkl").stat().st_size <= int(allowed_mb) * 1024**2
    with rw.open_output(tmp_path / "output.pkl") as stream:
        assert pickle.load(stream)["value"] == rows
    assert json.loads((tmp_path / "output-size.json").read_text(encoding="utf-8"))["expanded_bytes"] > 0

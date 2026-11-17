# -*- coding: utf-8 -*-
"""Searching the scholarly editions beside the automatic transcriptions (editions plan, PR 2):
the leaf engine, the coordinator, one result per page, locating, positions, the source choice.

A small main index built by the real Indexer (tests/responsa_index_fixture.py) and an editions
generation built by shared/editions_indexer.py from documents written here, with boundaries
naming the main index's pages -- never the real sidecars.
"""
import copy
import json
import os
import sqlite3
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

from shared.config import Config  # noqa: E402
from shared.edition_search_text import SOURCE_FGP, SOURCE_PGP, search_text  # noqa: E402
from shared.editions_indexer import (  # noqa: E402
    BROWSE_MAP_FILENAME, META_FILENAME, EditionDoc, build_generation)
from shared.editions_search import (  # noqa: E402
    SOURCES_ALL, check_generation, editions_served_to, group_results, normalize_sources,
    results_summary)
from shared.search_engine import (SearchEngine, _consume_last_responsa_downgrade,  # noqa: E402
                                  consume_last_search_cutoff)
from tests.responsa_index_fixture import build, engine as make_engine, uid  # noqa: E402

MS1 = "990000000110205171"   # two pages
MS2 = "990000000120205171"   # one page, an unlocated edition
MS3 = "990000000130205171"   # one page, a partly located edition
MS4 = "990000000140205171"   # one page, positions
P11, P12, P21, P31, P41 = uid(11, 1), uid(11, 2), uid(12, 1), uid(13, 1), uid(14, 1)
R = {"responsa_mode": True}

MAIN = [
    (MS1, 11, 1, "שלום עליכם רבותי\nואחר כך"),
    (MS1, 11, 2, "ברוך הבא לכם"),
    (MS2, 12, 1, "אין כאן דבר"),
    (MS3, 13, 1, "אבג דהו"),
    (MS4, 14, 1, "דף ריק"),
]


def _doc(kind, eid, sys_id, text, parts):
    """An edition document whose search text is *text* (unchanged by the cleaner), placed by
    *parts*: [(first words of the part, page uid or None)], each part running to the next."""
    st = search_text(text, kind)
    assert st.text == text, st.text
    starts = [text.index(words) for words, _page in parts]
    bounds = []
    for i, (start, (_w, page)) in enumerate(zip(starts, parts)):
        end = starts[i + 1] if i + 1 < len(starts) else len(text)
        bounds.append({"start": start, "end": end, "uid": page, "p_num": None,
                       "full_header": f"{sys_id}_{page}" if page else "",
                       "source": "FGP" if kind == SOURCE_FGP else "PGP", "sys_id": sys_id})
    return EditionDoc(kind=kind, edition_id=eid, sys_id=sys_id, search=st, boundaries=bounds,
                      credit={"en": f"Team {eid}", "he": f"Team {eid}"})


DOCS = [
    # Two editions of MS1's first page, one pointed.
    _doc(SOURCE_FGP, 1, MS1, "שָׁלוֹם עליכם רבותי\nדבר אחר", [("שָׁלוֹם", P11)]),
    _doc(SOURCE_FGP, 2, MS1, "שלום עליכם סוף", [("שלום", P11)]),
    # One stored PGP text on both pages of MS1: recto, then verso.
    _doc(SOURCE_PGP, 10, MS1, "שלום עליכם אחד\nשלום עליכם שתיים", [("שלום עליכם אחד", P11),
                                                                    ("שלום עליכם שתיים", P12)]),
    # Placed nowhere.
    _doc(SOURCE_FGP, 4, MS2, "שלום עליכם בלי דף שלום שלום", [("שלום", None)]),
    # Partly located: a located part, then an unlocated one, then a located one again.
    _doc(SOURCE_FGP, 5, MS3, "ראשון שני\nשלישי רביעי\nחמישי שישי",
         [("ראשון", P31), ("שלישי", None), ("חמישי", P31)]),
    # Positions: a punctuated first word, a pointed last word.
    _doc(SOURCE_FGP, 6, MS4, '"פתח דבר כאן\nסוף הַדָּבָר', [('"פתח', P41)]),
    # Fuzzy: a near spelling first, the word itself later on the same page.
    _doc(SOURCE_FGP, 8, MS4, "ירושלמי ועוד ירושלים", [("ירושלמי", P41)]),
]


def _sidecars(d):
    fgp, pgp = os.path.join(d, "fgp.db"), os.path.join(d, "pgp.db")
    con = sqlite3.connect(fgp)
    con.execute("CREATE TABLE fgp_meta (key TEXT, value TEXT)")
    con.execute("INSERT INTO fgp_meta VALUES ('version', '9.9.9')")
    con.commit()
    con.close()
    con = sqlite3.connect(pgp)
    con.execute("CREATE TABLE meta (key TEXT, value TEXT)")
    con.execute("INSERT INTO meta VALUES ('version', '1.1.0')")
    con.commit()
    con.close()
    return fgp, pgp


@pytest.fixture(scope="module")
def world(tmp_path_factory):
    base = tmp_path_factory.mktemp("editions-search")
    root = build(base / "main", MAIN)
    fgp, pgp = _sidecars(str(base))
    gen = str(base / "Editions")
    build_generation(gen, fgp_db=fgp, pgp_db=pgp, browse_map_path=os.path.join(root, "browse_map.pkl"),
                     folio_images_for=lambda s: [], documents=DOCS, progress=lambda m: None)
    browse = patch.object(Config, "BROWSE_MAP", os.path.join(root, "browse_map.pkl"))
    browse.start()
    SearchEngine._shared_browse_map = None
    eng = make_engine(root)
    plain = make_engine(root)            # today's engine: no editions attached
    assert eng.attach_editions(gen)
    try:
        yield {"eng": eng, "plain": plain, "gen": gen, "root": root}
    finally:
        eng.close_index()
        plain.close_index()
        SearchEngine._shared_browse_map = None
        browse.stop()


def _search(eng, query, mode="literal", **kw):
    kw.setdefault("corpus_scope", "genizah")
    rows = eng.execute_search(query, mode, kw.pop("gap", 0), **kw)
    return rows


def _members(rows):
    """{group uid: [(member uid, its source label)]}."""
    return {r["uid"]: [m["uid"] for m in r["members"]] for r in rows}


def _private_keys(rows):
    found = set()
    for r in rows:
        for x in [r] + list(r.get("members") or []):
            found |= {k for k in x if k in ("_matches", "_page_text", "_excluded")}
    return found


# --- the source choice -----------------------------------------------------------------------

CASES = [
    ("שלום עליכם", "literal", {}),
    ("שלום", "variants", {}),
    ("שלום", "fuzzy", {}),
    ("של.ם", "Regex", {}),
    ("שלום עליכם", "literal", {"responsa_options": R}),
    ("של*", "literal", {"responsa_options": R}),
    ("רבותי | ואחר", "literal", {"responsa_options": R}),
    ("שלום", "literal", {"text_position": "line_start"}),
    ("שלום עליכם", "literal", {"exclude_words": ["רבותי"]}),
    ("שלום", "literal", {"restrict_sys_ids": {MS1}}),
    ("שלום עליכם", "literal", {"ids_only": True}),
    ("שלום", "literal", {"corpus_scope": "all"}),
]


@pytest.mark.parametrize("query, mode, kw", CASES, ids=[f"{m}:{q}:{sorted(k)}" for q, m, k in CASES])
def test_automatic_only_is_todays_search(world, query, mode, kw):
    today = _search(world["plain"], query, mode, **dict(kw))
    today_cut = consume_last_search_cutoff()
    for sources in (None, "automatic"):
        assert _search(world["eng"], query, mode, sources=sources, **dict(kw)) == today
        assert consume_last_search_cutoff() == today_cut
    # "Automatic and editions" on an engine with no editions index is today's search too.
    assert _search(world["plain"], query, mode, sources=SOURCES_ALL, **dict(kw)) == today


def test_automatic_only_never_reads_the_editions(world):
    with patch.object(world["eng"]._editions, "_execute_search_leg",
                      side_effect=AssertionError("editions read")):
        _search(world["eng"], "שלום", sources="automatic")


def test_previews_only_without_editions(world, monkeypatch):
    import shared.search_engine as se
    monkeypatch.setattr(se, "_PREVIEW_ROWS", 1)     # a preview as soon as one row is found
    seen = []
    _search(world["eng"], "ב", "Regex", sources="automatic", preview_callback=seen.append)
    assert seen, "today's previews"
    seen.clear()
    _search(world["eng"], "ב", "Regex", sources="all", preview_callback=seen.append)
    assert seen == []


def test_my_library_rows_fused_before_exclusion(world, monkeypatch):
    """Today's order: My Library's rows are fused, then the exclusion step reads them too --
    and with editions the coordinator keeps it (plan §3.3 item 4)."""
    eng = world["eng"]
    local = [{"uid": "L1", "display": {"source": "LOCAL", "id": "9700001"}, "full_text": "שלום עליכם רבותי",
              "snippet": "*שלום עליכם*", "scope": "page"},
             {"uid": "L2", "display": {"source": "LOCAL", "id": "9700002"}, "full_text": "שלום עליכם",
              "snippet": "*שלום עליכם*", "scope": "page"}]
    monkeypatch.setattr(eng, "local_searcher", object(), raising=False)
    monkeypatch.setattr(eng, "_query_local_index", lambda *a, **k: [dict(r) for r in local])
    monkeypatch.setattr(eng, "_local_forms_query", lambda *a, **k: None)
    for sources in ("automatic", "all"):
        uids = [r["uid"] for r in eng.execute_search("שלום עליכם", "literal", 0, corpus_scope="all",
                                                     exclude_words=["רבותי"], sources=sources)]
        assert "L2" in uids and "L1" not in uids, (sources, uids)
    rows = eng.execute_search("שלום עליכם", "literal", 0, corpus_scope="all", sources="all")
    assert [r for r in rows if r["uid"] == "L1"][0].get("members") is None    # never grouped


def test_editions_only_runs_no_main_leg(world):
    eng = world["eng"]
    with patch.object(eng, "_execute_search_leg", side_effect=AssertionError("main read")):
        rows = _search(eng, "שלום עליכם", sources="editions")
    assert rows and all(m["edition"] for r in rows for m in r["members"])


def test_editions_only_without_an_editions_index_is_empty(world):
    from shared.search_engine import consume_editions_unavailable
    consume_editions_unavailable()
    assert _search(world["plain"], "שלום", sources="editions") == []
    # ... and says the editions were not read, which a worker hands on (Codex review of PR #18).
    assert consume_editions_unavailable() is True
    assert consume_editions_unavailable() is False                       # said once
    assert _search(world["plain"], "שלום", sources="all")                # today's rows ...
    assert consume_editions_unavailable() is True                        # ... said too
    for sources in (None, "automatic"):
        _search(world["plain"], "שלום", sources=sources)
        assert consume_editions_unavailable() is False
    _search(world["plain"], "x", "Title", sources="all", metadata_sys_ids=[])
    assert consume_editions_unavailable() is False                       # Title reads no editions
    _search(world["eng"], "שלום", sources="editions")
    assert consume_editions_unavailable() is False                       # read


def test_title_and_shelfmark_never_group(world):
    with patch.object(world["eng"]._editions, "_execute_search_leg",
                      side_effect=AssertionError("editions read")):
        _search(world["eng"], "x", "Title", sources="all", metadata_sys_ids=[])


def test_match_text_maps_spans_back():
    """The text the check runs on (points folded, brackets out) and its map back: for brackets
    it agrees with the main path's _unstripped_span on every span; a span over pointed letters
    comes back with the points between them and on its last letter; the maqaf is a space."""
    import random

    from shared.editions_search import MatchText
    from shared.search_engine import _strip_brackets, _unstripped_span
    rnd = random.Random(7)
    for _ in range(300):
        text = "".join(rnd.choice("אבג [] ד") for _ in range(rnd.randint(0, 30)))
        mt = MatchText(text, True)
        assert mt.text == _strip_brackets(text)
        for _ in range(10):
            s = rnd.randint(0, len(mt.text))
            e = rnd.randint(s, len(mt.text))
            if e > s:
                assert mt.span(s, e) == _unstripped_span(text, s, e), (text, s, e)
    pointed = "אמר שָׁלוֹם [עֲ]לֵיכֶם בְּנֵי־אָדָם"
    mt = MatchText(pointed, True)
    assert mt.text == "אמר שלום עליכם בני אדם"
    for word, original in (("שלום", "שָׁלוֹם"), ("עליכם", "[עֲ]לֵיכֶם"), ("בני", "בְּנֵי"), ("אדם", "אָדָם")):
        at = mt.text.index(word)
        a, b = mt.span(at, at + len(word))
        assert pointed[a:b] == original, word
        assert mt.to_match(a) == at
    assert MatchText("שלום [ע]", False).text == "שלום [ע]"      # a query with brackets keeps them


def test_normalize_sources():
    assert normalize_sources(None) == "automatic"
    assert normalize_sources("ALL") == "all"
    with pytest.raises(ValueError):
        normalize_sources("both")


# --- one result per page ---------------------------------------------------------------------

def test_one_result_per_page(world):
    rows = _search(world["eng"], "שלום עליכם", sources="all")
    groups = _members(rows)
    pgp = f"ED:pgp:10:{MS1}"
    # The automatic hit and the three editions of page 1: one result, the automatic text first.
    assert groups[P11][0] == P11 and set(groups[P11]) == {P11, "ED:fgp:1", "ED:fgp:2", pgp}
    # One stored PGP text matching on recto and verso: two members, in two results.
    assert groups[P12] == [pgp]
    # An unlocated edition hit is a result of its own, "page not identified".
    assert groups["ED:fgp:4"] == ["ED:fgp:4"]
    by_uid = {r["uid"]: r for r in rows}
    assert by_uid["ED:fgp:4"]["page_identified"] is False
    # ... and has no page number: never the header parser's "Unknown", which the website's card
    # read as one and stopped rendering the page's cards at.
    assert all(m["display"]["img"] == "" for m in by_uid["ED:fgp:4"]["members"])
    assert by_uid["ED:fgp:4"]["display"]["img"] == ""
    assert by_uid[P11]["display"]["img"] and by_uid[P12]["display"]["img"]
    assert by_uid[P11]["page_identified"] is True and by_uid[P12]["page_identified"] is True
    assert set(by_uid[P11]["sources"]) == {"automatic", "fgp_edition", "pgp_edition"}
    assert by_uid[P11]["edition"] is None and by_uid[P12]["edition"]["kind"] == "pgp"
    assert results_summary(rows) == {"results": 3, "with_edition": 3}
    assert not _private_keys(rows)


def test_edition_records(world):
    rows = _search(world["eng"], "שלום עליכם", sources="editions")
    members = {(m["uid"], r["uid"]): m for r in rows for m in r["members"]}
    m = members[("ED:fgp:1", P11)]
    e = m["edition"]
    assert e["version"] == "fgp:1" and e["kind"] == "fgp" and e["id"] == "1" and e["sys_id"] == MS1
    assert e["located"] is True and e["page_uid"] == P11 and e["credit"] == {"en": "Team 1", "he": "Team 1"}
    assert e["build_id"] and e["cleaner_version"]
    text = m["full_text"]
    assert text[e["span"][0]:e["span"][1]] == "שָׁלוֹם עליכם"
    assert m["raw_header"] == f"{MS1}_ED_fgp_1_{P11}" and m["display"]["source"] == "FGP"
    pgp_verso = members[(f"ED:pgp:10:{MS1}", P12)]
    assert pgp_verso["raw_header"] == f"{MS1}_ED_pgp_10_{P12}"
    assert pgp_verso["edition"]["version"] == "pgp:10"
    unlocated = members[("ED:fgp:4", "ED:fgp:4")]
    assert unlocated["edition"]["located"] is False and unlocated["raw_header"] == f"{MS2}_ED_fgp_4"
    assert "_P" not in unlocated["raw_header"]


def test_grouping_under_ids_only(world):
    rows = _search(world["eng"], "שלום עליכם", sources="all", ids_only=True)
    assert {r["uid"] for r in rows} == {P11, P12, "ED:fgp:4"}
    assert all(set(r) == {"uid", "display", "scope"} for r in rows)
    assert {r["display"]["id"] for r in rows} == {MS1, MS2}


def test_search_within_keeps_a_page_found_in_an_edition(world):
    # Step 1 finds page 1 in its automatic text; step 2's words are only in an edition of it.
    first = {r["uid"] for r in _search(world["eng"], "רבותי", sources="automatic")}
    second = {r["uid"] for r in _search(world["eng"], "סוף", sources="all", restrict_sys_ids={MS1})}
    assert P11 in first and P11 in second


def test_exclusion_is_judged_per_member(world):
    rows = _search(world["eng"], "שלום עליכם", sources="all", exclude_words=["סוף"])
    groups = _members(rows)
    assert "ED:fgp:2" not in groups[P11] and "ED:fgp:1" in groups[P11]
    # A group whose only member is excluded goes.
    rows = _search(world["eng"], "שלום עליכם", sources="editions", exclude_words=["שתיים"])
    assert P12 not in _members(rows)
    ids = _search(world["eng"], "שלום עליכם", sources="editions", exclude_words=["שתיים"], ids_only=True)
    assert {r["uid"] for r in ids} == set(_members(rows))


def test_restriction(world):
    rows = _search(world["eng"], "שלום עליכם", sources="all", restrict_sys_ids={MS2})
    assert _members(rows) == {"ED:fgp:4": ["ED:fgp:4"]}


# --- locating ----------------------------------------------------------------------------------

def test_locating_is_strict_on_the_first_character(world):
    eng = world["eng"]
    # A passage wholly in the unlocated part stays unlocated.
    assert _members(_search(eng, "רביעי", sources="editions")) == {"ED:fgp:5": ["ED:fgp:5"]}
    # A match starting in the unlocated part and running into a located one is unlocated.
    rows = _search(eng, "רביעי חמישי", sources="editions")
    assert _members(rows) == {"ED:fgp:5": ["ED:fgp:5"]}
    assert rows[0]["members"][0]["cross_page"] is True
    # A match starting in a located part and running into the next is on the first, cross-page.
    rows = _search(eng, "שני שלישי", sources="editions")
    assert _members(rows) == {P31: ["ED:fgp:5"]}
    assert rows[0]["members"][0]["cross_page"] is True and rows[0]["page_identified"] is True


# --- every mode, line breaks, positions --------------------------------------------------------

@pytest.mark.parametrize("query, mode, kw", [
    ("שלום עליכם רבותי", "literal", {}),
    ("עליכם רבותי", "variants", {}),
    ("רבותי", "fuzzy", {}),
    ("עליכ. רבות.", "Regex", {}),
    ("עליכם רבותי", "literal", {"responsa_options": R}),
    ("רבות*", "literal", {"responsa_options": R}),
])
def test_every_mode_finds_the_edition_member(world, query, mode, kw):
    rows = _search(world["eng"], query, mode, sources="editions", **kw)
    assert ("ED:fgp:1", P11) in {(m["uid"], r["uid"]) for r in rows for m in r["members"]}, rows


def test_fuzzy_shows_the_word_itself_on_a_page_that_holds_it(world):
    rows = _search(world["eng"], "ירושלים", "fuzzy", sources="editions")
    m = next(m for r in rows for m in r["members"] if m["uid"] == "ED:fgp:8")
    a, b = m["edition"]["span"]
    assert m["full_text"][a:b] == "ירושלים"


def test_line_break(world):
    rows = _search(world["eng"], "רבותי | דבר", sources="editions", responsa_options=R)
    assert _members(rows) == {P11: ["ED:fgp:1"]}
    # Lines flattened to spaces would not hold it as a line break: the same words on one line do not.
    assert _search(world["eng"], "עליכם | סוף", sources="editions", responsa_options=R) == []


def test_line_positions_on_pointed_and_punctuated_words(world):
    eng = world["eng"]
    # A pointed first word, found by its plain spelling at the start of line 1.
    rows = _search(eng, "L1:שלום", sources="editions", text_position="line_start")
    assert "ED:fgp:1" in {m["uid"] for r in rows for m in r["members"]}
    # A punctuated first word of the text.
    rows = _search(eng, "פתח", sources="editions", text_position="start")
    assert _members(rows) == {P41: ["ED:fgp:6"]}
    # A pointed last word, at the end of its line and of the text.
    for position in ("line_end", "end"):
        rows = _search(eng, "הדבר", sources="editions", text_position=position)
        assert _members(rows) == {P41: ["ED:fgp:6"]}, position
    rows = _search(eng, "L2:הדבר", sources="editions", text_position="line_end")
    assert _members(rows) == {P41: ["ED:fgp:6"]}
    # The intended line only.
    assert _search(eng, "L1:הדבר", sources="editions", text_position="line_end") == []
    # Not at a position it does not hold.
    assert _search(eng, "דבר", sources="editions", text_position="start") == []


# --- ordering, warnings, cut-offs, cancellation --------------------------------------------------

def test_all_star_orders_groups_by_matches(world):
    rows = _search(world["eng"], "של*", sources="all", responsa_options=R)
    # The unlocated edition holds three matches: the most, so first, whatever its rank.
    assert rows[0]["uid"] == "ED:fgp:4"
    assert {r["uid"] for r in rows} >= {P11, P12, "ED:fgp:4"}
    assert not _private_keys(rows)
    assert rows[0]["responsa_expanded_count"] is not None


def test_cut_off_from_either_leg(world, monkeypatch):
    monkeypatch.setattr(Config, "SEARCH_LIMIT", 1)
    _search(world["eng"], "שלום", sources="editions")
    assert consume_last_search_cutoff()["capped"] is True


def test_a_stopped_first_leg_starts_no_second(world):
    eng = world["eng"]

    def stop(*_a):
        raise InterruptedError("stop")
    with patch.object(eng._editions, "_execute_search_leg",
                      side_effect=AssertionError("second leg after a stop")):
        _search(eng, "שלום", sources="all", progress_callback=stop)
    assert consume_last_search_cutoff()["interrupted"] is True


def test_the_editions_phase_is_named(world):
    phases = []
    _search(world["eng"], "שלום", sources="all", phase_callback=phases.append)
    assert phases == ["editions_search"]


def test_warning_said_once(world):
    _consume_last_responsa_downgrade()
    rows = _search(world["eng"], "שלום עליכם", sources="all", responsa_options=R)
    assert rows and rows[0].get("responsa_pattern_count") is not None


def test_group_results_ranks_and_local_rows():
    main = [{"uid": "A", "display": {"source": "V0.8"}}, {"uid": "B", "display": {"source": "V0.8"}}]
    eds = [{"uid": "ED:fgp:9", "group_key": "B", "display": {"source": "FGP"},
            "edition": {"kind": "fgp", "uid": "ED:fgp:9"}}]
    local = [{"uid": "L", "display": {"source": "LOCAL"}}]
    rows = group_results(main, eds, local)
    # B: found by both legs, fused first; then A and the local row at equal rank, Genizah first.
    assert [r["uid"] for r in rows] == ["B", "A", "L"]
    # Members in rank order: the edition ranked first in its leg, the page second in its own.
    assert [m["uid"] for m in rows[0]["members"]] == ["ED:fgp:9", "B"]


def _ed(kind, eid, sys_id, page=None, shelf=None):
    """An edition member on *sys_id*: on *page* (its group key) or, without one, unlocated."""
    uid = f"ED:{kind}:{eid}:{sys_id}" if kind == "pgp" else f"ED:{kind}:{eid}"
    return {"uid": uid, "group_key": page or uid,
            "display": {"id": sys_id, "shelfmark": shelf or sys_id, "source": kind.upper()},
            "edition": {"kind": kind, "uid": uid, "version": f"{kind}:{eid}", "sys_id": sys_id}}


def test_the_same_text_is_shown_once():
    """The duplicates of a joined document's search (v10.0.0): a PGP edition indexed once per
    fragment, and editions whose page was not identified, each a result of its own."""
    eds = [_ed("fgp", 1, "990A", shelf="T-S 24.18"),            # unlocated FGP of A
           _ed("pgp", 7, "990A", shelf="T-S 24.18"),            # one PGP text on A, B, C, unlocated
           _ed("pgp", 7, "990B", shelf="T-S 16.223"),
           _ed("pgp", 7, "990C", shelf="T-S 12.634"),
           _ed("pgp", 8, "990A", page="990A_p1", shelf="T-S 24.18"),   # another PGP text, on a page
           _ed("pgp", 8, "990C", page="990C_p1", shelf="T-S 12.634"),
           _ed("pgp", 8, "990B", shelf="T-S 16.223")]
    rows = group_results([], eds)
    assert len(rows) == 1
    row = rows[0]
    # The result whose page was identified leads, at the first result's place.
    assert row["uid"] == "990A_p1" and row["page_identified"] is True
    assert sorted({m["edition"]["version"] for m in row["members"]}) == ["fgp:1", "pgp:7", "pgp:8"]
    assert len(row["members"]) == 3
    assert [a["shelfmark"] for a in row["also_in"]] == ["T-S 16.223", "T-S 12.634"]
    assert row["folded"] == 6
    # ids_only (completing a step) keeps every match.
    assert len(group_results([], eds, ids_only=True)) == 7


def test_the_same_text_is_one_result_in_every_order():
    """T-S 24.18 shown twice (2026-11-17): the same seven rows, ranked in another order, were
    two results -- the unplaced FGP and PGP-7 copies in one, PGP-8's located pages in another --
    because an unplaced PGP-8 copy on B ranked before any result held PGP 8 and started its
    own, and B's PGP-7 copy then went there by manuscript. Before the fix 3,296 of the 5,040
    orders gave two or three results."""
    import itertools
    eds = [_ed("fgp", 1, "990A"), _ed("pgp", 7, "990A"), _ed("pgp", 7, "990B"),
           _ed("pgp", 7, "990C"), _ed("pgp", 8, "990A", page="990A_p1"),
           _ed("pgp", 8, "990C", page="990C_p1"), _ed("pgp", 8, "990B")]
    seen = set()
    for order in itertools.permutations(eds):
        rows = group_results([], copy.deepcopy(list(order)))
        assert len(rows) == 1, [r["uid"] for r in rows]
        row = rows[0]
        assert row["page_identified"] is True and row["uid"] in ("990A_p1", "990C_p1")
        assert sorted(m["edition"]["version"] for m in row["members"]) == ["fgp:1", "pgp:7", "pgp:8"]
        assert row["folded"] == 6
        assert {row["display"]["id"]} | {a["sys_id"] for a in row["also_in"]} == {"990A", "990B", "990C"}
        seen.add(row["uid"])
    # Which identified page leads follows the ranking.
    assert seen == {"990A_p1", "990C_p1"}


def test_a_page_whose_own_text_matched_leads_the_joined_result():
    """A located edition page and a page whose automatic text matched, the two holding one hit:
    one result in either order, led by the page whose own text matched (it is never folded into
    another), at the place of the better-ranked of the two."""
    from shared.editions_search import fold_same_text
    main = [{"uid": "990B_p1", "display": {"id": "990B", "source": "V0.8"}}]
    edition_page = group_results([], [_ed("pgp", 7, "990A", page="990A_p1")])[0]
    matched_page = group_results(main, [_ed("pgp", 7, "990B", page="990B_p1")])[0]
    other = group_results([], [_ed("fgp", 3, "990D")])[0]
    for order in ([edition_page, other, matched_page], [matched_page, other, edition_page]):
        rows = fold_same_text(copy.deepcopy(order))
        assert [r["uid"] for r in rows] == ["990B_p1", "ED:fgp:3"]
        assert [a["sys_id"] for a in rows[0]["also_in"]] == ["990A"]
    # Two pages whose own text matched stay two results, however they are linked.
    second = group_results([{"uid": "990A_p1", "display": {"id": "990A", "source": "V0.8"}}],
                           [_ed("pgp", 7, "990A", page="990A_p1")])[0]
    assert [r["uid"] for r in fold_same_text([second, matched_page])] == ["990A_p1", "990B_p1"]


def test_a_page_whose_automatic_text_matched_keeps_its_result():
    main = [{"uid": "990A_p1", "display": {"id": "990A", "source": "V0.8"}},
            {"uid": "990B_p1", "display": {"id": "990B", "source": "V0.8"}}]
    eds = [_ed("pgp", 7, "990A", page="990A_p1"), _ed("pgp", 7, "990B", page="990B_p1"),
           _ed("pgp", 7, "990C"), _ed("fgp", 3, "990D")]
    rows = group_results(main, eds)
    # Both pages matched in their own text: two results; C's copy of the same PGP text folds into
    # the first; D's FGP edition is another text on another manuscript.
    assert [r["uid"] for r in rows] == ["990A_p1", "990B_p1", "ED:fgp:3"]
    assert [a["sys_id"] for a in rows[0]["also_in"]] == ["990C"]
    assert "also_in" not in rows[1] and "folded" not in rows[2]
    # B's unplaced copy goes to B's own result, not under A (B would be named twice).
    rows = group_results(main, eds[:2] + [_ed("pgp", 7, "990B"), _ed("pgp", 7, "990C")])
    assert [a["sys_id"] for a in rows[0]["also_in"]] == ["990C"]
    assert rows[1]["folded"] == 1 and "also_in" not in rows[1]


def test_an_unplaced_second_match_of_an_edition_is_not_folded_by_manuscript():
    """Codex review of PR #36: another match of the same edition on one manuscript is a find of
    its own, page identified or not; another edition there still folds."""
    first, second = _ed("pgp", 7, "990A"), _ed("pgp", 7, "990A")
    first["edition"]["span"], second["edition"]["span"] = [0, 9], [40, 49]
    second["uid"] = second["group_key"] = "ED:pgp:7:990A#2"     # its own group, as the leaf keys it
    fgp = _ed("fgp", 1, "990A")
    fgp["edition"]["span"] = [5, 14]
    rows = group_results([], [first, second, fgp])
    assert len(rows) == 2 and rows[0]["folded"] == 1 and "folded" not in rows[1]
    assert {m["edition"]["version"] for m in rows[0]["members"]} == {"pgp:7", "fgp:1"}
    # A located row of the manuscript with another match of the unplaced row's edition stays.
    placed = _ed("pgp", 7, "990A", page="990A_p1")
    placed["edition"]["span"] = [40, 49]
    assert len(group_results([], [first, placed])) == 2


def test_a_located_hit_joins_its_manuscripts_unplaced_hit_folded_elsewhere():
    """Codex review of PR #36: B's unplaced copy went into A's result by hit; B's located FGP
    hit joins that result rather than standing apart."""
    rows = group_results([], [_ed("pgp", 7, "990A"), _ed("pgp", 7, "990B"), _ed("fgp", 1, "990B", page="990B_p1")])
    assert len(rows) == 1 and rows[0]["uid"] == "990B_p1"
    assert [a["sys_id"] for a in rows[0]["also_in"]] == ["990A"]
    # Never into a result whose own page matched: a located hit on another page stays.
    main = [{"uid": "990A_p1", "display": {"id": "990A", "source": "V0.8"}}]
    rows = group_results(main, [_ed("pgp", 7, "990A"), _ed("fgp", 1, "990A", page="990A_p5")])
    assert [r["uid"] for r in rows] == ["990A_p1", "990A_p5"]


def test_a_result_takes_one_located_page_by_manuscript_alone():
    """Codex review of PR #36: A and B share an unplaced hit; B's located FGP joins; A's located
    page of another edition shares no hit with it and stays a result of its own."""
    rows = group_results([], [_ed("pgp", 7, "990A"), _ed("pgp", 7, "990B"),
                              _ed("fgp", 1, "990B", page="990B_p1"), _ed("fgp", 2, "990A", page="990A_p3")])
    assert [r["uid"] for r in rows] == ["990B_p1", "990A_p3"]
    # The same on one manuscript: a second located page of another edition stays apart.
    rows = group_results([], [_ed("pgp", 7, "990A"), _ed("fgp", 1, "990A", page="990A_p1"),
                              _ed("pgp", 9, "990A", page="990A_p5")])
    assert [r["uid"] for r in rows] == ["990A_p1", "990A_p5"]


def test_a_located_hit_joins_only_through_a_located_rows_hit():
    """Codex review of PR #36: located A/p1 holds hit X; unplaced B/X and B/Y join it; a located
    C/p1 with hit Y shares no hit with A's located row and stays a result, while a located D/p1
    with X (the same text on another fragment's page) still folds."""
    rows = group_results([], [_ed("pgp", 7, "990A", page="990A_p1"), _ed("pgp", 7, "990B"),
                              _ed("fgp", 3, "990B"), _ed("fgp", 3, "990C", page="990C_p1"),
                              _ed("pgp", 7, "990D", page="990D_p1")])
    assert [r["uid"] for r in rows] == ["990A_p1", "990C_p1"]
    assert [a["sys_id"] for a in rows[0]["also_in"]] == ["990B", "990D"]


def _page(sys_id, page, *eds):
    """A located row holding several editions on one page (group_results groups them)."""
    return [_ed(kind, eid, sys_id, page=page) for kind, eid in eds]


def test_a_located_page_with_two_hits_never_bridges_two_pages():
    """Codex review of PR #36: pages of X, of X+Y, of Y. The X+Y page joins the X page; the Y
    page shares no hit with the X page and stays a result of its own."""
    eds = _page("990A", "990A_p1", ("pgp", 7)) + _page("990B", "990B_p1", ("pgp", 7), ("fgp", 3)) \
        + _page("990C", "990C_p1", ("fgp", 3))
    assert [r["uid"] for r in group_results([], eds)] == ["990A_p1", "990C_p1"]


def test_joining_never_bridges_two_pages_in_any_order():
    """Codex review of the order-independent fold: pages of X, X+Y, Y+Z, Z. The order decides
    which pages go together (two results or three), but in no order are two pages that share no
    hit one result."""
    import itertools
    pages = [_page("990A", "990A_p1", ("pgp", 7)), _page("990B", "990B_p1", ("pgp", 7), ("fgp", 3)),
             _page("990C", "990C_p1", ("fgp", 3), ("pgp", 9)), _page("990D", "990D_p1", ("pgp", 9))]
    apart = [{"990A", "990C"}, {"990A", "990D"}, {"990B", "990D"}]
    counts = set()
    for order in itertools.permutations(pages):
        rows = group_results([], copy.deepcopy([e for page in order for e in page]))
        counts.add(len(rows))
        for row in rows:
            held = {row["display"]["id"]} | {a["sys_id"] for a in row.get("also_in") or ()}
            assert not any(pair <= held for pair in apart), (row["uid"], held)
    assert counts <= {2, 3}


def test_two_pages_whose_own_text_matched_are_never_compared(monkeypatch):
    """Codex review of the order-independent fold: many pages whose own text matched, all
    holding one hit, cost no comparison at all (they can never be one result)."""
    import shared.editions_search as es
    calls = []
    real = es._can_join
    monkeypatch.setattr(es, "_can_join", lambda a, b: calls.append(1) or real(a, b))
    main = [{"uid": f"990{i}_p1", "display": {"id": f"990{i}", "source": "V0.8"}} for i in range(300)]
    eds = []
    for i in range(300):
        ed = _ed("pgp", 7, f"990{i}", page=f"990{i}_p1")
        ed["edition"]["span"] = [0, 9]
        eds.append(ed)
    rows = group_results(main, eds)
    assert len(rows) == 300 and not calls


def test_a_located_hit_tries_every_result_holding_one_of_its_hits():
    """Codex review of PR #36: located X, unplaced X+Y (joins X's result), located Z; a located
    Y+Z page is refused by X's result (no hit in common with its located row) and joins Z's."""
    eds = _page("990A", "990A_p1", ("pgp", 7)) + [_ed("pgp", 7, "990B"), _ed("fgp", 3, "990B")] \
        + _page("990C", "990C_p1", ("pgp", 9)) + _page("990D", "990D_p1", ("fgp", 3), ("pgp", 9))
    rows = group_results([], eds)
    assert [r["uid"] for r in rows] == ["990A_p1", "990C_p1"]
    assert [a["sys_id"] for a in rows[1]["also_in"]] == ["990D"]


def test_list_bulk_add_and_replay_take_the_folded_fragments():
    from shared.editions_search import row_sys_ids
    row = {"display": {"id": "990A"}, "also_in": [{"sys_id": "990B"}, {"sys_id": "990A"}, {}]}
    assert row_sys_ids(row) == ["990A", "990B"]
    assert row_sys_ids({"display": {}}) == [] and row_sys_ids(None) == []

    from shared.refinement import RefinementStep, replay_chain

    class Searcher:
        def execute_search(self, *a, **kw):
            return [dict(row)]
    step = RefinementStep(query="x", mode="literal", result_count=1)
    replay_chain([step], Searcher(), None)
    assert step._result_sys_ids == {"990A", "990B"}


def test_search_within_counts_the_folded_fragments():
    from shared.editions_search import result_sys_ids
    rows = group_results([], [_ed("pgp", 7, "990A", page="990A_p1"), _ed("pgp", 7, "990B"),
                              _ed("pgp", 7, "990C")])
    assert len(rows) == 1
    assert result_sys_ids(rows) == {"990A", "990B", "990C"}
    assert result_sys_ids([{"display": {"id": "990D"}}, {"display": {}}, "x"]) == {"990D"}


def test_a_second_match_of_an_edition_stays_a_result():
    first, second = _ed("pgp", 7, "990A", page="990A_p1"), _ed("pgp", 7, "990A", page="990A_p2")
    first["edition"]["span"], second["edition"]["span"] = [0, 9], [40, 49]
    assert [r["uid"] for r in group_results([], [first, second])] == ["990A_p1", "990A_p2"]


# --- the engine's lifecycle and re-reading rows ----------------------------------------------------

def test_variant_settings_bind_the_child(world):
    view = copy.copy(world["eng"])
    view.var_mgr = object()
    child = view._editions_for_request()
    assert child is not world["eng"]._editions and child.var_mgr is view.var_mgr
    assert world["eng"]._editions_for_request() is world["eng"]._editions


def test_rows_are_read_through_the_engine_that_holds_them(world):
    eng = world["eng"]
    assert eng.get_full_text_by_id("ED:fgp:2") == "שלום עליכם סוף"
    assert eng.get_full_text_by_header(f"{MS1}_ED_pgp_10_{P12}") == "שלום עליכם אחד\nשלום עליכם שתיים"
    assert eng.get_full_text_by_id("ED:fgp:404") is None
    assert world["plain"].get_full_text_by_id("ED:fgp:2") is None
    assert eng.get_full_text_by_id(P12) == "ברוך הבא לכם"


def test_format_status_needs_no_seams(world):
    status = world["eng"]._editions.index_format_status()
    assert status["current"] is True and status["seams"] is False


def test_close_releases_the_child_and_reload_reopens_it(world):
    e = make_engine(world["root"])
    try:
        assert e.attach_editions(world["gen"]) and e.editions_ready()
        e.close_index()                      # a rebuild closes the main index first
        assert not e.editions_ready() and e._editions is None
        with patch.object(Config, "INDEX_DIR", world["root"]):
            assert e.reload_index()
        assert e.editions_ready()
        e.close_editions()                   # a promotion closes the child alone
        assert not e.editions_ready() and e.searcher is not None
        assert e.reload_editions() and e.editions_ready()
        e.detach_editions()
        with patch.object(Config, "INDEX_DIR", world["root"]):
            e.reload_index()
        assert not e.editions_ready()
    finally:
        e.close_index()


def test_promotion_takes_the_lease(tmp_path):
    from shared.editions_search import promote_with_lease
    from shared.local_index_leases import index_leases
    live, new = tmp_path / "Editions", tmp_path / "Editions.new"
    live.mkdir()
    (live / "old").write_text("1", encoding="utf-8")
    new.mkdir()
    (new / "new").write_text("2", encoding="utf-8")
    calls = []
    real = index_leases

    def spy(paths, *, exclusive=False):
        calls.append(([os.path.basename(p) for p in paths], exclusive))
        return real(paths, exclusive=exclusive)
    with patch("shared.local_index_leases.index_leases", spy):
        promote_with_lease(str(new), str(live))
    assert calls == [(["Editions"], True)]
    assert (live / "new").exists() and (tmp_path / "Editions.prev" / "old").exists() and not new.exists()


def test_the_resolver_reads_the_original_edition(tmp_path):
    from shared.editions_search import EditionResolver
    fgp, pgp = str(tmp_path / "fgp.db"), str(tmp_path / "pgp.db")
    con = sqlite3.connect(fgp)
    con.execute("CREATE TABLE fgp_transcriptions (id INTEGER PRIMARY KEY, sys_id TEXT, content TEXT, "
                "doc_relation TEXT, source_credit TEXT, source_credit_en TEXT, source_credit_he TEXT, text_source TEXT)")
    con.execute("INSERT INTO fgp_transcriptions VALUES (7, ?, ?, 'Digital Edition', NULL, ?, ?, 'gate3:docx')",
                (MS1, "01 קדשך שבעו(עו) עמים\n02 שם\n03 סוף", "FGP Transcriptions Team - Genuzos", "צוות גנוזות"))
    con.commit()
    con.close()
    con = sqlite3.connect(pgp)
    con.execute("CREATE TABLE document_sources (id INTEGER PRIMARY KEY, pgpid INTEGER, source_scholar TEXT, content TEXT)")
    con.execute("INSERT INTO document_sources VALUES (10, 3, 'Moshe Gil', 'Recto\nשורה')")
    con.commit()
    con.close()
    resolver = EditionResolver(fgp_db=fgp, pgp_db=pgp)
    ed = resolver.resolve("fgp:7")
    assert ed["original"].startswith("01 קדשך") and ed["search_text"] == "קדשך שבעו עמים\nשם\nסוף"
    assert ed["credit"] == {"en": "FGP Transcriptions Team - Genuzos", "he": "צוות גנוזות"}
    at = ed["search_text"].index("שבעו עמים")
    a, b = ed["search"].span_to_original(at, at + len("שבעו עמים"))
    assert ed["original"][a:b] == "שבעו(עו) עמים"         # the match, with the edition's own signs
    pgp_ed = resolver.resolve(f"ED:pgp:10:{MS1}")
    assert pgp_ed["search_text"] == "שורה" and pgp_ed["credit"]["en"] == "Moshe Gil"
    assert resolver.resolve("fgp:404") is None and resolver.resolve("nonsense") is None


# --- readiness and the switch ---------------------------------------------------------------------

def test_readiness_refuses_an_incoherent_generation(world, tmp_path):
    import shutil
    ok, reason, _m, _p = check_generation(world["gen"])
    assert ok, reason
    bad = str(tmp_path / "Editions")
    shutil.copytree(world["gen"], bad)
    with open(os.path.join(bad, META_FILENAME), encoding="utf-8") as f:
        meta = json.load(f)
    meta["counts"]["documents"] += 1
    with open(os.path.join(bad, META_FILENAME), "w", encoding="utf-8") as f:
        json.dump(meta, f)
    ok, reason, _m, _p = check_generation(bad)
    assert not ok and "documents" in reason
    meta["counts"]["documents"] -= 1
    meta["build_id"] = "another"
    with open(os.path.join(bad, META_FILENAME), "w", encoding="utf-8") as f:
        json.dump(meta, f)
    assert check_generation(bad)[0] is False
    e = make_engine(world["root"])
    try:
        assert e.attach_editions(bad) is False and not e.editions_ready()
        assert e.attach_editions(str(tmp_path / "missing")) is False
    finally:
        e.close_index()
    os.remove(os.path.join(bad, BROWSE_MAP_FILENAME))
    assert check_generation(bad)[0] is False


def test_switch(monkeypatch):
    monkeypatch.delenv("EDITIONS_SEARCH_ENABLED", raising=False)
    assert editions_served_to(True) is False
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "editors")
    assert editions_served_to(True) is True and editions_served_to(False) is False
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "1")
    assert editions_served_to(False) is True


def test_switch_on_with_no_index_is_todays_search(world, monkeypatch, tmp_path):
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "1")
    e = make_engine(world["root"])
    try:
        assert e.attach_editions(str(tmp_path / "Editions")) is False
        assert _search(e, "שלום", sources="all") == _search(world["plain"], "שלום")
        assert _search(e, "שלום", sources="editions") == []
    finally:
        e.close_index()


def test_worker_reads_editions_only_when_asked(monkeypatch):
    from shared.research_worker import wants_editions
    payload = {"kind": "search", "method": "execute_search", "arguments": {"sources": "all"}}
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "editors")
    assert wants_editions(payload) is True
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "0")
    assert wants_editions(payload) is False
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "1")
    assert wants_editions({**payload, "arguments": {"sources": "automatic"}}) is False
    # Read as execute_search reads it (Codex review of PR #16).
    assert wants_editions({**payload, "arguments": {"sources": " ALL "}}) is True
    assert wants_editions({**payload, "arguments": {"sources": "EDITIONS"}}) is True
    assert wants_editions({**payload, "arguments": {"sources": "both"}}) is False
    assert wants_editions({**payload, "arguments": {}}) is False
    assert wants_editions({**payload, "kind": "lab"}) is False


def test_search_within_replays_the_source_choice():
    from shared.refinement import RefinementStep, replay_chain
    calls = []

    class Searcher:
        def execute_search(self, query, mode, gap, **kwargs):
            calls.append(kwargs.get("sources", "<none>"))
            return [{"uid": "u", "display": {"id": "990001"}}]

    step = RefinementStep(query="x", mode="literal", sources="editions")
    assert RefinementStep.from_dict(step.to_dict()).sources == "editions"
    assert RefinementStep.from_dict({"query": "x", "mode": "literal"}).sources is None
    replay_chain([step, RefinementStep(query="y", mode="literal")], Searcher(), None)
    assert calls == ["editions", "<none>"]


# --- the website's route: each leg in a research worker of its own (PR 3) ------------------------

def _legs(world, query, mode, kw, *, leaf=None):
    """The two legs as two research workers return them (pickled across the process boundary),
    merged in the web process."""
    import pickle
    kw = dict(kw)
    kw.setdefault("corpus_scope", "genizah")
    gap = kw.pop("gap", 0)
    main = world["eng"].execute_search_leg(query, mode, gap, "main", **kw)
    editions = (leaf or world["eng"]).execute_search_leg(query, mode, gap, "editions", **kw)
    main, editions = pickle.loads(pickle.dumps(main)), pickle.loads(pickle.dumps(editions))
    return SearchEngine.merge_search_legs(main, editions, ids_only=kw.get("ids_only", False))


@pytest.mark.parametrize("query, mode, kw", CASES + [
    ("רבות*", "literal", {"responsa_options": R}),
    ("שלום עליכם רבותי", "literal", {}),
])
def test_the_parallel_legs_return_the_coordinators_rows(world, query, mode, kw):
    """Each leg in a worker of its own, merged in the web process, gives what the in-process
    coordinator gives for the same search, row for row (the owner's parallel worker, §3.10)."""
    expected = _search(world["eng"], query, mode, sources="all", **copy.deepcopy(kw))
    _consume_last_responsa_downgrade()
    consume_last_search_cutoff()
    rows, warning = _legs(world, query, mode, copy.deepcopy(kw))
    assert rows == expected
    assert warning == (expected[0].get("responsa_warning") if expected else None)


def test_the_editions_worker_opens_the_editions_alone(world):
    """The editions leg's worker opens the leaf engine only (no main index to load) and returns
    what the attached child returns."""
    from shared.editions_search import open_editions_engine
    eng = world["eng"]
    leaf = open_editions_engine(eng.meta_mgr, eng.var_mgr, world["gen"])
    try:
        for query, mode, kw in [("שלום", "literal", {}), ("של*", "literal", {"responsa_options": R})]:
            expected = _search(eng, query, mode, sources="all", **copy.deepcopy(kw))
            rows, _ = _legs(world, query, mode, kw, leaf=leaf)
            assert rows == expected
        with pytest.raises(ValueError):
            leaf.execute_search_leg("שלום", "literal", 0, "main")
    finally:
        leaf.close_index()


def test_a_failed_editions_leg_leaves_the_main_rows(world):
    main = world["eng"].execute_search_leg("שלום", "literal", 0, "main", corpus_scope="genizah")
    rows, _ = SearchEngine.merge_search_legs(main, None)
    plain = world["plain"].execute_search("שלום", "literal", 0, corpus_scope="genizah")
    assert [r["uid"] for r in rows] == [r["uid"] for r in plain]
    assert all(r["sources"] == ["automatic"] for r in rows)


def test_a_leg_refuses_what_has_no_editions(world):
    eng = world["eng"]
    for mode, scope in (("Title", "genizah"), ("literal", "local")):
        with pytest.raises(ValueError):
            eng.execute_search_leg("שלום", mode, 0, "main", corpus_scope=scope)
    with pytest.raises(ValueError):
        eng.execute_search_leg("שלום", "literal", 0, "sideways")
    with pytest.raises(ValueError):
        world["plain"].execute_search_leg("שלום", "literal", 0, "editions")


def test_the_worker_reads_the_editions_only_in_the_editions_leg(monkeypatch):
    from shared.research_worker import wants_editions
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "1")
    leg = {"kind": "search", "method": "execute_search_leg"}
    assert wants_editions({**leg, "arguments": {"leg": "editions"}}) is True
    assert wants_editions({**leg, "arguments": {"leg": "main"}}) is False
    monkeypatch.setenv("EDITIONS_SEARCH_ENABLED", "0")
    assert wants_editions({**leg, "arguments": {"leg": "editions"}}) is False

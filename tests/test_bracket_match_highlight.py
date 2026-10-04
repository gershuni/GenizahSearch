"""A row whose accepted occurrence holds a bracket marks that occurrence (Codex review of PR #375).

Membership is decided on the text with its brackets out; the snippet then looks for the
match again in the original text, where של[ו]ם is not שלום. When the original's only
occurrences were ones the whole-word rule rejects (inside בשלום), the row fell back to
the first of them and marked it. Now the accepted occurrence is mapped back onto the
original text, brackets included.
"""
import gc
import os
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

from shared.config import Config  # noqa: E402
from shared.indexer import Indexer  # noqa: E402
from shared.search_tokenizer import register_search_tokenizers  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402
from shared.variants import VariantManager  # noqa: E402
import shared.search_engine as se  # noqa: E402

W, ON = "שלום", "עולם"
PAGES = {
    # The embedded pair comes first; the whole-word one needs its bracket out.
    "rejected_then_bracketed": f"{W} אבג ב{W} {ON} דהו של[ו]ם {ON} זחט",
    "plain": f"אבג {W} {ON} דהו",
}


def _schema():
    b = tantivy.SchemaBuilder()
    b.add_text_field("unique_id", stored=True)
    b.add_text_field("content", stored=True, tokenizer_name="hebword")
    for f in ("content_head", "content_tail", "line_starts", "line_ends"):
        b.add_text_field(f, stored=False, tokenizer_name="whitespace")
    b.add_text_field("content_search", stored=False, tokenizer_name="hebword")
    for f in ("source", "full_header", "shelfmark", "scope", "boundaries"):
        b.add_text_field(f, stored=True)
    return b.build()


class _Meta:
    def get_display_data(self, header, source):
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header.split()[-1], "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("bracket_hl")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    for n, (uid, text) in enumerate(PAGES.items()):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
            full_header=f"IE_{uid} 99{n}", shelfmark=uid, scope="page", boundaries="",
            **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    eng = None
    gc.collect()


def test_the_snippet_marks_the_bracketed_occurrence_not_the_rejected_one(engine):
    rows = {r["uid"]: r for r in engine.execute_search(f"{W} {ON}", "variants", 0, corpus_scope="genizah")}
    row = rows["rejected_then_bracketed"]
    for text in (row["snippet"], row["raw_file_hl"]):
        assert f"*של[ו]ם {ON}*" in text and f"ב*{W}" not in text, text


def test_an_original_whole_word_occurrence_is_still_the_one_marked(engine):
    rows = {r["uid"]: r for r in engine.execute_search(f"{W} {ON}", "variants", 0, corpus_scope="genizah")}
    assert f"אבג *{W} {ON}*" in rows["plain"]["snippet"]


@pytest.mark.parametrize("text, start, end, want", [
    ("אב[ג]ד", 1, 3, (1, 5)),        # ב[ג]: the pair kept whole
    ("א[בג]ד", 0, 2, (0, 3)),        # א[ב: a pair the match splits stays split (no ג)
    ("[א]בג", 0, 1, (1, 2)),         # a leading bracket stays outside the first letter
    ("אבג", 0, 3, (0, 3)),           # no brackets: the same offsets
    ("ש[ל]ום עולם", 0, 4, (0, 6)),
    ("א[בג]ד", 2, 4, (3, 6)),        # ג]ד: likewise (no ב)
    ("[אב]ג", 0, 3, (0, 5)),         # [אב]ג: the pair it closes, opened right at its edge
])
def test_unstripped_span(text, start, end, want):
    assert se._unstripped_span(text, start, end) == want
    # The same letters, never one outside the match.
    assert se._strip_brackets(text[want[0]:want[1]]) == se._strip_brackets(text)[start:end]


# --- Search-within over an Oxford part scans the stripped text (Codex review, round 7) ---
# The part's first match is a clean one on a page of a manuscript outside the restriction;
# the one inside matches only with its bracket out. The scan for an in-scope match went on
# in the original text, where it never finds ש[לו]ם, so the row was lost. Regex mode
# reaches this path (the whole-word modes take multi-word matches on a part from the
# crossing search, and single words from the pages).

ON2 = "על"


def _aggregate(pages):
    import json
    text, bounds, cursor = [], [], 0
    for i, (uid, sid, t) in enumerate(pages):
        start = cursor
        text.append(t)
        cursor += len(t)
        if i != len(pages) - 1:
            text.append("\n")
            cursor += 1
        bounds.append({"uid": uid, "p_num": i + 1, "full_header": f"IE_{uid} {sid}",
                       "source": "V0.8", "sys_id": sid, "start": start, "end": cursor})
    return "".join(text), json.dumps(bounds, ensure_ascii=False)


PART_INSIDE = [("a1", "A", f"אבג {W} {ON2} דהו"), ("b1", "B", f"דבר ש[לו]ם {ON2} דהו")]
PART_CROSSING = [("c1", "C", f"אבג {W} {ON2} דהו"), ("d1", "D", f"זחט ש[לו]ם"), ("d2", "D", f"{ON2} טוב")]
# No page docs of its own: its rows come from the part alone. The in-scope bracketed
# occurrence comes first, a clean one after it, and a bracket earlier shifts offsets.
PART_SOLO = [("g1", "G", "[אבג] דהו"), ("h1", "H", f"דבר ש[לו]ם {ON2} זחט {W} {ON2}")]


@pytest.fixture(scope="module")
def part_engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("bracket_part")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, sid, text, scope, bounds=""):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
            full_header=f"IE_{uid} {sid}", shelfmark=uid, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))
    for name, pages in (("inside", PART_INSIDE), ("crossing", PART_CROSSING)):
        for uid, sid, text in pages:
            add(uid, sid, text, "page")
        add(f"part:{name}", pages[0][1], *_aggregate(pages)[:1], "part", _aggregate(pages)[1])
    add("part:solo", "G", *_aggregate(PART_SOLO)[:1], "part", _aggregate(PART_SOLO)[1])
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    browse = {}
    for uid, sid, _t in PART_INSIDE + PART_CROSSING + PART_SOLO:
        browse.setdefault(sid, []).append({"uid": uid})
    eng._load_browse_map = lambda: browse
    yield eng
    eng = None
    gc.collect()


@pytest.mark.parametrize("restrict, page", [("B", "b1"), ("D", "d1")])
def test_search_within_finds_a_bracketed_match_on_a_part_after_an_outside_one(part_engine, restrict, page):
    rows = part_engine.execute_search(rf"{W}\s+{ON2}", "Regex", 0, corpus_scope="genizah",
                                      restrict_sys_ids={restrict})
    assert page in {r["uid"] for r in rows}, "the in-scope occurrence needs its bracket out"
    (row,) = [r for r in rows if r["uid"] == page]
    # The whole occurrence, brackets and the page break included.
    assert f"*ש[לו]ם{chr(10) if page == 'd1' else ' '}{ON2}*" in row["raw_file_hl"], row["raw_file_hl"]


def test_search_within_takes_the_first_in_scope_occurrence_from_the_stripped_text(part_engine):
    rows = part_engine.execute_search(rf"{W}\s+{ON2}", "Regex", 0, corpus_scope="genizah",
                                      restrict_sys_ids={"H"})
    (row,) = [r for r in rows if r["uid"] == "h1"]
    assert f"דבר *ש[לו]ם {ON2}* זחט" in row["raw_file_hl"], row["raw_file_hl"]


def test_ids_only_finds_it_too(part_engine):
    rows = part_engine.execute_search(rf"{W}\s+{ON2}", "Regex", 0, corpus_scope="genizah",
                                      restrict_sys_ids={"B"}, ids_only=True)
    assert "b1" in {r["uid"] for r in rows}


# --- Composition marks a bracketed match whole (Codex review sweep, round 7) -------------

def test_composition_marks_a_match_found_only_with_its_bracket_out(tmp_path):
    root = tmp_path / "comp"
    db = root / "tantivy_db"
    db.mkdir(parents=True)
    idx = tantivy.Index(_schema(), path=str(db))
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    text = f"פתח ש[לום דהו טוב {W}"      # a clean שלום later lets the candidate through
    w.add_document(tantivy.Document(
        unique_id="p", content=text, content_search=strip_search_diacritics(text), source="V0.8",
        full_header="IE_p 990", shelfmark="p", scope="page", boundaries="",
        **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    res = eng.search_composition_logic(f"פתח {W}", 2, 10 ** 9, "literal", corpus_scope="genizah")
    (item,) = res["main"] + res["filtered"]
    assert item["text"].startswith("*פתח ש[לום*"), item["text"]
    eng = None
    gc.collect()


def test_my_library_composition_marks_it_whole_too(tmp_path):
    from shared.local_indexer import build_local_schema
    root = tmp_path / "comp_main"
    (root / "tantivy_db").mkdir(parents=True)
    main = tantivy.Index(_schema(), path=str(root / "tantivy_db"))
    register_search_tokenizers(main)
    main.writer(heap_size=50_000_000, num_threads=1).commit()
    local = tantivy.Index(build_local_schema(), path=str(tmp_path))
    register_search_tokenizers(local)
    w = local.writer(heap_size=50_000_000, num_threads=1)
    text = f"פתח ש[לום דהו טוב {W}"
    w.add_document(tantivy.Document(
        unique_id="loc", content=text, content_search=strip_search_diacritics(text), source="LOCAL",
        full_header="loc_LOCAL_P1_F1", shelfmark="loc", scope="page", boundaries="",
        **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    local.reload()
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    eng.local_index, eng.local_searcher, eng._local_has_content_search = local, local.searcher(), True
    res = eng.search_composition_logic(f"פתח {W}", 2, 10 ** 9, "literal", corpus_scope="local")
    (item,) = res["main"] + res["filtered"]
    assert item["text"].startswith("*פתח ש[לום*"), item["text"]
    eng = None
    gc.collect()

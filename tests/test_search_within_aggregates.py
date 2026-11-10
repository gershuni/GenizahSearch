"""Search-within keeps hits that come from whole-manuscript (system) and Oxford-part docs.

End-to-end through ``SearchEngine.execute_search`` on a tiny main-schema index.
Before 2026-09-30 every aggregate hit was dropped under ``restrict_sys_ids``:
its uid is ``sys:``/``part:`` and was compared against PAGE uids. So a phrase
running across a page break was never found within results. Separately, an
Oxford part carries only its first page's header, so the <=500-manuscript query
filter excluded a part whose match lies in another manuscript.

Each case runs with the restriction below 500 manuscripts (Tantivy-level
filter) and above it (per-document check only). Plan:
docs/plans/SEARCH_UNCAPPED_STREAMING_PLAN.md, stage 0c / gate G2c.
"""
import gc
import json
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

# Phrase 1 crosses the page break inside manuscript 990009 (system doc).
T1, T2 = "תעודדו", "תענגו"  # תעודדו תענגו
# Phrase 2 lives in an Oxford part spanning 990007 (first page) and 990008.
B1, B2 = "ברוך", "הבא"                            # ברוך הבא
X, Y, Z = "עמוד", "ראשון", "סוף"  # עמוד ראשון סוף
# Phrase 3 crosses the break in 990010 with a gap word (Y) before the break.
G1, G2 = "גמרא", "תוספות"  # גמרא תוספות


def _page(uid, sid, text):
    return {"uid": uid, "sid": sid, "header": f"IE_{uid} {sid}", "text": text}


SYS_PAGES = [_page("p1", "990009", f"{X} {Y} {T1}"), _page("p2", "990009", f"{T2} {X} {Z}")]
PART_PAGES = [_page("q1", "990007", f"{B1} {B2} {X}"),       # first match: outside 990008
              _page("q2", "990008", f"{X} {Y} {B1}"),        # second match starts here...
              _page("q3", "990008", f"{B2} {Z}")]            # ...and ends here
GAP_PAGES = [_page("g1", "990010", f"{X} {G1} {Y}"), _page("g2", "990010", f"{G2} {Z}")]
BROWSE_MAP = {"990009": [{"uid": "p1"}, {"uid": "p2"}], "990007": [{"uid": "q1"}],
              "990008": [{"uid": "q2"}, {"uid": "q3"}], "990010": [{"uid": "g1"}, {"uid": "g2"}]}
MANY = {f"99{n:07d}" for n in range(1000, 1501)}  # 501 unrelated ids: forces the >500 path


def _aggregate(pages):
    text, bounds, cursor = [], [], 0
    for i, p in enumerate(pages):
        start = cursor
        text.append(p["text"])
        cursor += len(p["text"])
        if i != len(pages) - 1:
            text.append("\n")
            cursor += 1
        bounds.append({"uid": p["uid"], "p_num": i + 1, "full_header": p["header"],
                       "source": "V0.8", "sys_id": p["sid"], "start": start, "end": cursor})
    return "".join(text), json.dumps(bounds, ensure_ascii=False)


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
def run(tmp_path_factory):
    root = tmp_path_factory.mktemp("aggidx")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, header, scope, bounds=""):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=header, shelfmark=header, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))

    for p in SYS_PAGES + PART_PAGES + GAP_PAGES:
        add(p["uid"], p["text"], p["header"], "page")
    add("sys:990009", *_aggregate(SYS_PAGES)[:1], SYS_PAGES[0]["header"], "system",
        _aggregate(SYS_PAGES)[1])
    add("sys:990010", _aggregate(GAP_PAGES)[0], GAP_PAGES[0]["header"], "system",
        _aggregate(GAP_PAGES)[1])
    add("part:MS. Heb. x. 1/1", _aggregate(PART_PAGES)[0], PART_PAGES[0]["header"], "part",
        _aggregate(PART_PAGES)[1])
    w.commit()
    w.wait_merging_threads()
    w = idx = None  # release before cleanup; `add` closes over w

    patcher = patch.object(Config, "INDEX_DIR", str(root))
    patcher.start()
    engine = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    engine._load_browse_map = lambda: BROWSE_MAP

    def _run(query, restrict=None, responsa=False, gap=0):
        rows = engine.execute_search(
            query, "literal", gap, restrict_sys_ids=restrict, corpus_scope="genizah",
            responsa_options={"responsa_mode": True} if responsa else None)
        return [r["uid"] for r in rows]

    yield _run
    patcher.stop()
    engine = None  # release the mmap before tmp cleanup; `_run` closes over it
    gc.collect()


SYS_PHRASE = f"{T1} {T2}"
PART_PHRASE = f"{B1} {B2}"


def test_unrestricted_baseline(run):
    assert run(SYS_PHRASE) == ["p1"]            # mapped to the page the match starts on
    # q1 from its own page doc; q2 is the part's match across the q2|q3 break. Before
    # 2026-09-30 an aggregate gave only its FIRST match (q1 again), so the cross-page
    # q2 was found only under search-within.
    assert run(PART_PHRASE) == ["q1", "q2"]


@pytest.mark.parametrize("extra", [set(), MANY], ids=["under500", "over500"])
def test_cross_page_system_hit_survives_search_within(run, extra):
    assert run(SYS_PHRASE, restrict={"990009"} | extra) == ["p1"]


@pytest.mark.parametrize("extra", [set(), MANY], ids=["under500", "over500"])
def test_part_hit_in_a_later_manuscript_survives(run, extra):
    # The part's header is 990007's; its second occurrence starts in 990008.
    assert run(PART_PHRASE, restrict={"990008"} | extra) == ["q2"]


@pytest.mark.parametrize("extra", [set(), MANY], ids=["under500", "over500"])
def test_restriction_still_excludes_other_manuscripts(run, extra):
    assert run(SYS_PHRASE, restrict={"990008"} | extra) == []
    assert run(PART_PHRASE, restrict={"990009"} | extra) == []


@pytest.mark.parametrize("extra", [set(), MANY], ids=["under500", "over500"])
def test_line_break_path_keeps_cross_page_system_hit(run, extra):
    query = f"{T1} | {T2}"                      # T1 on one line, T2 on the next
    assert run(query, responsa=True) == ["p1"]
    assert run(query, restrict={"990009"} | extra, responsa=True) == ["p1"]
    assert run(query, restrict={"990008"} | extra, responsa=True) == []


def test_a_crossing_with_a_gap_word_before_the_break_is_found(run):
    # The window holds (terms - 1) * (gap + 1) words on each side: here G1 and the
    # gap word Y. A window of (terms - 1) words held only Y, so the crossing was lost.
    assert run(f"{G1} {G2}", gap=1) == ["g1"]
    assert run(f"{G1} {G2}", gap=0) == []      # Y stands between them

"""execute_search(preview_callback=...) hands the desktop the first result rows early.

The preview must be exactly the first _PREVIEW_ROWS rows of the final list (same uids,
same order) -- the table shows it while the search runs and the finished handler then
rebuilds from the full list -- so it is offered only where nothing after the loop can
drop or move those rows: Genizah scope, no exclude_words, not Responsa.
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

W = "שלום"          # שלום
F = "עוד"                # עוד
N_V8 = 70


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
                "id": header, "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


def _build(root, n_v8):
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, source):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source=source, full_header=uid, shelfmark=uid, scope="page", boundaries="",
            **Indexer._extract_position_fields(text)))

    for n in range(n_v8):
        # Different lengths give different scores, so hit order is not doc order.
        add(f"u{n:03d}", f"{W} " + " ".join([F] * (n % 9)), "V0.8")
    for n in range(0, 10, 2):
        add(f"u{n:03d}", f"{W} {F}", "V0.7")          # V0.7 duplicates of some V0.8 uids
    add("v7only", f"{W}", "V0.7")                      # a V0.7-only page: goes to the end
    # A whole-manuscript doc that outscores every page (the word 30 times) and maps
    # to a page with no page doc of its own: its row exists only through the
    # aggregate. Literal single-word search skips aggregates; variants keeps them.
    text = " ".join([W] * 30)
    w.add_document(tantivy.Document(
        unique_id="sys:990009", content=text, content_search=text, source="V0.8",
        full_header="sysonly", shelfmark="sysonly", scope="system",
        boundaries=json.dumps([{"uid": "sysonly", "p_num": 1, "full_header": "sysonly",
                                "source": "V0.8", "sys_id": "990009", "start": 0, "end": len(text)}]),
        **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()


@pytest.fixture(scope="module")
def engines(tmp_path_factory):
    made = {}
    for name, n in (("many", N_V8), ("few", 20)):
        root = tmp_path_factory.mktemp(f"preview_{name}")
        _build(root, n)
        with patch.object(Config, "INDEX_DIR", str(root)):
            made[name] = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield made
    made.clear()
    gc.collect()


def _run(engine, **kw):
    calls = []
    kw.setdefault("corpus_scope", "genizah")
    final = engine.execute_search(W, "literal", 0, preview_callback=calls.append, **kw)
    return calls, final


def test_the_preview_is_the_first_rows_of_the_final_list(engines):
    calls, final = _run(engines["many"])
    assert len(calls) == 1, "the preview is handed over exactly once"
    preview = calls[0]
    assert len(preview) == se._PREVIEW_ROWS
    assert [r["uid"] for r in preview] == [r["uid"] for r in final[:se._PREVIEW_ROWS]]
    assert all(r["display"]["source"] == "V0.8" for r in preview)
    assert final[-1]["uid"] == "v7only"                # V0.7-only rows still go last


@pytest.mark.parametrize("kw", [
    {"exclude_words": [F]},
    {"corpus_scope": "all"},
    {"responsa_options": {"responsa_mode": True}},
], ids=["exclude_words", "local_fusion", "responsa"])
def test_no_preview_where_a_later_step_could_drop_or_move_rows(engines, kw):
    calls, _final = _run(engines["many"], **kw)
    assert calls == []


def test_no_preview_when_the_whole_result_is_smaller_and_fast(engines):
    calls, final = _run(engines["few"])
    assert calls == [] and len(final) >= 20


def _assert_growing_prefixes(calls, final):
    uids = [[r["uid"] for r in rows] for rows in calls]
    final_uids = [r["uid"] for r in final]
    for before, after in zip(uids, uids[1:]):
        assert len(after) > len(before) and after[:len(before)] == before, "a preview must extend the last"
    for u in uids:
        assert u == final_uids[:len(u)], "every preview is the start of the final list"


def test_a_slow_run_keeps_previewing_the_rows_found_so_far(engines, monkeypatch):
    # Every _PREVIEW_AFTER_S (0 here) with new rows: the rows so far. The few-row
    # search never reaches _PREVIEW_ROWS, which is the variants / fuzzy case.
    monkeypatch.setattr(se, "_PREVIEW_AFTER_S", 0.0)
    calls, final = _run(engines["few"])
    assert len(calls) >= 2
    _assert_growing_prefixes(calls, final)


def test_previews_grow_until_the_full_first_page(engines, monkeypatch):
    monkeypatch.setattr(se, "_PREVIEW_AFTER_S", 0.0)
    calls, final = _run(engines["many"])
    _assert_growing_prefixes(calls, final)
    assert len(calls[-1]) == se._PREVIEW_ROWS, "the last preview is the full first page"


def test_previews_are_spaced_by_the_interval(engines, monkeypatch):
    monkeypatch.setattr(se, "_PREVIEW_AFTER_S", 3600.0)
    calls, _final = _run(engines["few"])
    assert calls == [], "no early preview before the interval has passed"


def test_first_wins_keeps_the_first_row_and_position_per_uid():
    eng = se.SearchEngine.__new__(se.SearchEngine)
    page = {"uid": "a", "scope": "page", "display": {"source": "V0.8"}}
    other = {"uid": "b", "scope": "page", "display": {"source": "V0.8"}}
    agg = {"uid": "a", "scope": "system", "display": {"source": "V0.8"}}
    rows = [page, other, agg]
    assert eng._deduplicate(rows, first_wins=True) == [page, other]
    assert eng._deduplicate(rows) == [agg, other]        # Responsa keeps the last-row rule


def test_page_docs_are_processed_before_whole_manuscript_docs(engines):
    # The aggregate outscores every page, so the old single mixed query handled
    # it first; its row now follows every page row.
    final = engines["many"].execute_search(W, "variants", 0, corpus_scope="genizah")
    v8 = [r["uid"] for r in final if r["display"]["source"] == "V0.8"]
    assert "sysonly" in v8 and v8[-1] == "sysonly"


def test_search_thread_passes_the_callback_and_emits_the_preview():
    from desktop.gui_threads import SearchThread

    rows = [{"uid": "x", "display": {"source": "V0.8", "id": "x"}}]

    class Fake:
        def execute_search(self, *a, preview_callback=None, **kw):
            preview_callback(rows)
            return rows

    t = SearchThread(Fake(), "q", "literal", 0)
    got = []
    t.preview_signal.connect(got.append)
    t.run()                       # same thread: the signal is delivered directly
    assert got == [rows]

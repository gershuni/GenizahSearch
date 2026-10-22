"""The index format of Responsa plan Phase 2 (docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md
§6.1): the seam index and the reversed-words field, built by the real Indexer from a
small corpus, and how the engine opens them.

- format detection: an index without the marker (every index before Phase 2), a seam
  index of another build, a missing seam marker -- all searched by today's routes;
- the reversed-words lookup finds the documents a ``.*X`` regex lookup finds;
- isolation (gate d): every reader of the main index -- searches in every mode, the
  document counts, the text fetches, composition -- gives on the new index exactly
  what it gives on the old one, and never a seam; except the matches across a page break
  of a phrase (Exact, Variants, Fuzzy, a Responsa sequence), which come from the seams on
  the new index (decision 10, Phase 3): the same rows, read from a seam, and none at a
  junction no seam holds.
"""
import json
import os
import re
import shutil
from unittest.mock import patch

import pytest
import tantivy

from shared.config import Config
from shared.index_format import INDEX_FORMAT_FILE, SEAM_FORMAT_FILE
from shared.indexer import Indexer
from shared.metadata_manager import MetadataManager
from shared.search_engine import SearchEngine
from shared.variants import VariantManager

A, B = "990000000110205171", "990000000220205171"

V08 = [
    (A, 1001, 1, "פתיחה שלום כהנים"),
    (A, 1001, 2, "גדולים באו לעיר\nשורה שנייה"),
    (A, 1001, 3, "סוף [כהנים] שלום"),
    (B, 2001, 1, "אחר אלכהנים גדולים מו\"רנו כֹּהֲנִים"),
]
V07 = [
    (A, 1001, 1, "ישן כהנים"),
    (A, 1001, 2, "גדולים ישן נתקנים["),
]


class FakeCodico:
    part_to_folios = {}

    def load(self, csv_bank=None):
        return True

    def _get_folio_number(self, shelfmark):
        return 0


class FakeMeta:
    """The metadata the indexer and the engine ask for, with the real header parsers."""
    meta_map = {}
    csv_bank = {}
    extract_unique_id = MetadataManager.extract_unique_id
    parse_full_id_components = MetadataManager.parse_full_id_components
    parse_header_smart = MetadataManager.parse_header_smart

    def __init__(self):
        self.codico_mgr = FakeCodico()

    def _load_csv_bank(self):
        pass

    def get_shelfmark_from_header(self, header):
        return ""

    def get_display_data(self, header, source):
        sys_id = self.parse_header_smart(header)[0]
        return {"shelfmark": header, "title": "", "img": "1", "source": source, "id": sys_id,
                "library_code": ""}


def write_corpus(root):
    v8, v7 = os.path.join(root, "v8.txt"), os.path.join(root, "v7.txt")
    with open(v8, "w", encoding="utf-8") as f:
        for sid, ie, p, text in V08:
            f.write(f"==> {sid}_IE{ie}_P{p:06d}_FL{ie}{p} <==\n{text}\n")
    with open(v7, "w", encoding="utf-8") as f:
        for sid, ie, p, text in V07:
            f.write(f"### Q:\\ERC\\x\\{sid}\\IE{ie}\\IE{ie}_P{p:06d}_FL{ie}{p}-reco.xml - TEST\n{text}\n")
    return v8, v7


def build(root, **kwargs):
    """The index (and browse map) under *root*, built by Indexer.create_index."""
    os.makedirs(root, exist_ok=True)
    v8, v7 = write_corpus(root)
    with patch.object(Config, "INDEX_DIR", str(root)), \
            patch.object(Config, "BROWSE_MAP", os.path.join(str(root), "browse_map.pkl")), \
            patch.object(Config, "FILE_V8", v8), patch.object(Config, "FILE_V7", v7):
        ix = Indexer(FakeMeta())
        ix.create_index(**kwargs)
    return str(root)


def engine(root):
    with patch.object(Config, "INDEX_DIR", str(root)), \
            patch.object(SearchEngine, "start_fl_id_index_build"):
        return SearchEngine(FakeMeta(), VariantManager(), worker_mode=True, open_local=False)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    base = tmp_path_factory.mktemp("index_format")
    new = build(base / "new")
    old = build(base / "old", seams=False, reversed_field=False)
    # The two builds hold the same browse map; the engine reads it lazily, so the path
    # stays on the fixture's for the module (never the real one).
    browse = patch.object(Config, "BROWSE_MAP", os.path.join(new, "browse_map.pkl"))
    browse.start()
    SearchEngine._shared_browse_map = None
    engines = {"new": engine(new), "old": engine(old)}
    try:
        yield {"new": new, "old": old, "engines": engines}
    finally:
        for e in engines.values():
            e.close_index()
        SearchEngine._shared_browse_map = None
        browse.stop()


# --- the format ---------------------------------------------------------------------

def test_the_new_build_writes_its_marker_and_the_seams(built):
    db = os.path.join(built["new"], "tantivy_db")
    marker = json.load(open(os.path.join(db, INDEX_FORMAT_FILE), encoding="utf-8"))
    assert marker["format"] == 2 and marker["reversed_field"] == "content_rev"
    # V0.8 A p1>p2, p2>p3; V0.7 A p1>p2. Not V0.8 p3 > V0.7 p1, the restart.
    assert marker["seams"]["count"] == 3 and marker["seams"]["words_each_side"] == 30
    assert built["engines"]["new"].index_format_status() == {
        "format": 2, "reversed_words": True, "seams": True, "seam_words": 30, "current": True}


def test_an_index_without_the_marker_is_an_old_index(built):
    assert not os.path.exists(os.path.join(built["old"], "tantivy_db", INDEX_FORMAT_FILE))
    eng = built["engines"]["old"]
    assert eng.index_format_status() == {
        "format": 1, "reversed_words": False, "seams": False, "seam_words": 0, "current": False}
    assert eng.reversed_ending_query("כהנים") is None
    assert eng.reversed_ending_query("כהנים", seams=True) is None


def _copy(src, dst):
    shutil.copytree(src, dst)
    return str(dst)


@pytest.mark.parametrize("damage", ["other_build", "no_seam_marker", "no_seam_dir"])
def test_seams_of_another_build_or_missing_are_not_used(built, tmp_path, damage):
    root = _copy(built["new"], tmp_path / "copy")
    seams = os.path.join(root, "tantivy_db", "seams")
    if damage == "other_build":
        path = os.path.join(seams, SEAM_FORMAT_FILE)
        data = json.load(open(path, encoding="utf-8"))
        data["build_id"] = "0" * 32
        json.dump(data, open(path, "w", encoding="utf-8"))
    elif damage == "no_seam_marker":
        os.remove(os.path.join(seams, SEAM_FORMAT_FILE))
    else:
        shutil.rmtree(seams)
    eng = engine(root)
    try:
        status = eng.index_format_status()
        assert status["seams"] is False and status["current"] is False
        assert status["reversed_words"] is True       # the field is in the main schema
        assert eng.reversed_ending_query("כהנים", seams=True) is None
    finally:
        eng.close_index()


def test_a_marker_naming_a_field_the_schema_lacks_is_not_believed(built, tmp_path):
    root = _copy(built["old"], tmp_path / "copy")
    with open(os.path.join(root, "tantivy_db", INDEX_FORMAT_FILE), "w", encoding="utf-8") as f:
        json.dump({"format": 2, "build_id": "x", "reversed_field": "content_rev", "seams": None}, f)
    eng = engine(root)
    try:
        assert eng.index_format_status()["reversed_words"] is False
        assert eng.reversed_ending_query("כהנים") is None
    finally:
        eng.close_index()


def test_seams_without_the_reversed_field_offer_no_reversed_lookup(tmp_path):
    # build_index.py --no-reversed-field keeps the seams: their lookup must say "no field"
    # (None, so a caller keeps today's route), not run an empty query (Codex review of
    # PR #440).
    root = build(tmp_path / "seams_only", reversed_field=False)
    eng = engine(root)
    try:
        status = eng.index_format_status()
        assert status["seams"] is True and status["reversed_words"] is False
        assert eng.reversed_ending_query("כהנים") is None
        assert eng.reversed_ending_query("כהנים", seams=True) is None
    finally:
        eng.close_index()


@pytest.mark.parametrize("words", [0, -1, True, 2.5])
def test_a_seam_window_that_is_not_a_positive_number_is_refused_before_anything_is_deleted(
        built, tmp_path, words):
    # A window under 1 would build seams the marker then calls invalid (Codex review of
    # PR #440); the refusal comes before the old index is removed.
    root = _copy(built["new"], tmp_path / "copy")
    before = sorted(os.listdir(os.path.join(root, "tantivy_db")))
    with pytest.raises(ValueError):
        build(root, seam_words=words)
    assert sorted(os.listdir(os.path.join(root, "tantivy_db"))) == before


def test_close_index_releases_the_seams_so_a_rebuild_can_delete_them(built, tmp_path):
    root = _copy(built["new"], tmp_path / "copy")
    eng = engine(root)
    assert eng.seam_searcher is not None
    eng.close_index()
    assert eng.seam_searcher is None and eng.seam_index is None
    shutil.rmtree(os.path.join(root, "tantivy_db"))      # Windows refuses while a reader holds it


# --- the reversed-words lookup ---------------------------------------------------------

def _uids(searcher, query):
    return sorted((searcher.doc(a)["unique_id"][0], searcher.doc(a)["source"][0])
                  for _, a in searcher.search(query, 1000).hits)


def _oracle(searcher, ending, query):
    """The documents of *query* holding a word that ends with *ending* once the
    characters the lookups look past (brackets, Hebrew marks: _T_CONTENT_SEARCH) are
    removed from it -- each word as the hebword tokenizer cuts the folded text, read
    from the stored text, not from the index."""
    from shared.index_seams import hebword_analyzer
    from shared.responsa_match import _T_CONTENT_SEARCH
    from shared.text_normalize import strip_search_diacritics
    looked_past = re.compile(_T_CONTENT_SEARCH)
    analyzer = hebword_analyzer()
    out = []
    for _, a in searcher.search(query, 1000).hits:
        doc = searcher.doc(a)
        tokens = analyzer.analyze(strip_search_diacritics(doc["content"][0]))
        if any(looked_past.sub("", t).endswith(ending) for t in tokens):
            out.append((doc["unique_id"][0], doc["source"][0]))
    return sorted(out)


# ורנו: the field is built from the folded text, where מו"רנו is מורנו (from the raw text it
# would be the token ונר"ום, which does not start with ונרו, and no other word ends so).
# כהנים: [כהנים] and כֹּהֲנִים end with it once the brackets and points are looked past; a
# plain prefix of םינהכ misses both (Codex review of PR #438). נים: נתקנים[ too.
@pytest.mark.parametrize("ending", ["כהנים", "נים", "ים", "שלום", "לום", "ורנו"])
def test_the_reversed_lookup_finds_every_word_with_the_ending(built, ending):
    eng = built["engines"]["new"]
    pages = eng.index.parse_query("scope:page", ["content"])
    rev = tantivy.Query.boolean_query([(tantivy.Occur.Must, eng.reversed_ending_query(ending)),
                                       (tantivy.Occur.Must, pages)])
    want = _oracle(eng.searcher, ending, pages)
    assert _uids(eng.searcher, rev) == want and want
    seam_rev = _uids(eng.seam_searcher, eng.reversed_ending_query(ending, seams=True))
    assert seam_rev == _oracle(eng.seam_searcher, ending, tantivy.Query.all_query())


def test_the_fixture_has_the_words_a_plain_prefix_misses(built):
    eng = built["engines"]["new"]
    plain = tantivy.Query.regex_query(eng.index.schema, "content_rev", "םינהכ.*")
    assert len(_uids(eng.searcher, eng.reversed_ending_query("כהנים"))) > len(_uids(eng.searcher, plain))


def test_the_reversed_lookup_takes_a_folded_ending(built):
    eng = built["engines"]["new"]
    plain = _uids(eng.searcher, eng.reversed_ending_query("כהנים"))
    assert _uids(eng.searcher, eng.reversed_ending_query("כה\"נים")) == plain


# --- isolation (gate d): what reads the main index sees no seam -------------------------

RESPONSA = {"responsa_mode": True, "variants": False, "ja": False, "flex_spacing": False,
            "bidirectional": False, "variant_mode": "exact"}

SEARCHES = [
    ("exact one word", "כהנים", "literal", {}),
    ("regex", "כהנים\\s+גדולים", "Regex", {}),
    ("responsa and", "שלום AND ישן", "literal", {"responsa_options": RESPONSA}),
    ("responsa line", "|כהנים", "literal", {"responsa_options": RESPONSA}),
    ("position start", "פתיחה", "literal", {"text_position": "start"}),
]
# Decision 10 (plan §7, Phase 3 points 7 and 12): a phrase's matches across a page break
# come from the seams on the new index. Each of these finds A's p1 | p2 crossing.
CROSSINGS = [
    ("exact across a break", "כהנים גדולים", "literal", {}),
    ("variants", "כהנים גדולים", "variants", {}),
    ("fuzzy", "כהנים גדולים", "fuzzy", {}),
    ("responsa", "כהנים גדולים", "literal", {"responsa_options": RESPONSA}),
    ("responsa star", "*כהנים גדולים", "literal", {"responsa_options": RESPONSA}),
    ("responsa bidirectional", "גדולים כהנים", "literal",
     {"responsa_options": dict(RESPONSA, bidirectional=True)}),
    ("ids_only", "כהנים גדולים", "literal", {"ids_only": True}),
]


def _row_keys(eng, query, mode, kwargs):
    """The rows, as a sorted list: documents of equal score come in segment order, and
    which segment a document lands in is up to the writer's threads, build by build."""
    rows = eng.execute_search(query, mode, 0, corpus_scope="genizah", **kwargs)
    return sorted((r.get("uid"), (r.get("display") or {}).get("source"), r.get("scope"),
                   r.get("snippet"), r.get("cross_page"),
                   repr([(h.get("uid"), h.get("span")) for h in r.get("page_highlights") or []]))
                  for r in rows)


def _rows(eng, query, mode, kwargs):
    return [repr(k) for k in _row_keys(eng, query, mode, kwargs)]


@pytest.mark.parametrize("name, query, mode, kwargs", SEARCHES, ids=[s[0] for s in SEARCHES])
def test_every_search_gives_on_the_new_index_what_it_gives_on_the_old(built, name, query, mode, kwargs):
    new = _rows(built["engines"]["new"], query, mode, kwargs)
    old = _rows(built["engines"]["old"], query, mode, kwargs)
    assert new == old
    assert not any("seam" in row for row in new)


@pytest.mark.parametrize("name, query, mode, kwargs", CROSSINGS, ids=[s[0] for s in CROSSINGS])
def test_a_crossing_comes_from_a_seam_on_the_new_index_and_nothing_else_changes(built, name, query, mode,
                                                                                 kwargs):
    old = _row_keys(built["engines"]["old"], query, mode, kwargs)
    new = _row_keys(built["engines"]["new"], query, mode, kwargs)
    assert [k[2] for k in old].count("system") == 1          # A's p1 | p2, its whole manuscript
    # The same rows, the crossing's scope a seam; and its snippet the seam's text, which
    # ends with A's V0.8 pages (the whole manuscript ran on into its V0.7 copy).
    assert [k[:3] + k[4:] for k in new] == sorted(
        k[:2] + ("seam" if k[2] == "system" else k[2],) + k[4:] for k in old)
    seam_row = next(k for k in new if k[2] == "seam")
    if seam_row[3]:                                          # ids_only rows carry no snippet
        assert seam_row[3].endswith("סוף [כהנים] שלום")


def test_the_seams_leave_out_the_false_junction_of_the_v07_restart(built):
    # A's last V0.8 page and its V0.7 copy's first page: one whole-manuscript document on
    # the old index, a junction no seam holds (§6.1 point 3).
    assert [k[2] for k in _row_keys(built["engines"]["old"], "שלום ישן", "literal", {})] == ["system"]
    assert _row_keys(built["engines"]["new"], "שלום ישן", "literal", {}) == []


def test_the_cases_find_something(built):
    # The comparisons above are not of empty lists.
    eng = built["engines"]["new"]
    assert _rows(eng, "כהנים", "literal", {})
    assert _rows(eng, "שלום AND ישן", "literal", {"responsa_options": RESPONSA})


def test_document_counts_and_scopes_are_unchanged(built):
    new, old = built["engines"]["new"], built["engines"]["old"]
    assert new.searcher.num_docs == old.searcher.num_docs == len(V08) + len(V07) + 2
    assert new._every_doc_has_scope() and old._every_doc_has_scope()


def _docs(eng, field, value):
    """The stored content of the main index's documents whose *field* is *value*."""
    q = eng.index.parse_query(f'{field}:"{value}"', [field])
    return {eng.searcher.doc(a)["content"][0] for _, a in eng.searcher.search(q, 50).hits
            if eng.searcher.doc(a)[field][0] == value}


def _seam_texts(eng):
    return {eng.seam_searcher.doc(a)["content"][0]
            for _, a in eng.seam_searcher.search(tantivy.Query.all_query(), 1000).hits}


@pytest.mark.parametrize("n", [1, 2, 3])
def test_text_fetches_return_a_document_of_the_main_index(built, n):
    # Page 1's header is also its whole-manuscript document's header, so a fetch by
    # header may return either (Codex's table, §6: "can return an aggregate by
    # accident", unchanged here); never a seam.
    new, old = built["engines"]["new"], built["engines"]["old"]
    header = f"{A}_IE1001_P{n:06d}_FL1001{n}"
    allowed = _docs(old, "full_header", header)
    assert allowed and new.get_full_text_by_header(header) in allowed
    uid = header.split("_", 1)[1]
    assert new.get_full_text_by_id(uid) in _docs(old, "unique_id", uid)
    assert not (allowed & _seam_texts(new))


def test_best_text_for_a_manuscript_is_a_main_document(built):
    new, old = built["engines"]["new"], built["engines"]["old"]
    text = new._get_best_text_for_id(A)[0]
    assert text and text in {t for _s, _ie, _p, t in V08} | _docs(old, "unique_id", f"sys:{A}")


def test_composition_search_is_unchanged(built):
    new, old = built["engines"]["new"], built["engines"]["old"]

    def run(eng):
        res = eng.search_composition_logic("שלום כהנים גדולים באו לעיר", 3, 100, "literal")
        return sorted(json.dumps(item, sort_keys=True, ensure_ascii=False, default=str)
                      for item in res["main"] + res["filtered"])

    got = run(new)
    assert got == run(old) and got
    assert not any('"uid": "seam:' in item for item in got)

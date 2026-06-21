"""SEED-006 — Hebrew search tokenizer + diacritic-fold retrieval.

Two-stage fix for punctuation-/diacritic-attached words being unretrievable:

* Stage 1: a shared ``hebword`` regex tokenizer on the searchable ``content``
  field (both indexes) so ``בסגן,`` is tokenized ``בסגן`` (comma is a boundary)
  while ``[סגן`` keeps its bracket and ``צ̇מאן`` keeps its combining dot.
* Stage 2: an additive, non-stored ``content_search`` field
  (= ``strip_search_diacritics(content)``) used as a lower-weighted OR fallback
  so ``צמאן`` / ``צ'מאן`` retrieve the corpus form ``צ̇מאן``.

These tests build TINY throwaway tantivy indexes (no 2 GB corpus) that exercise
the REAL code paths: ``build_local_schema``, ``register_search_tokenizers``,
``strip_search_diacritics``, ``_add_bracket_variants``,
``SearchEngine.build_tantivy_query`` and the ``_index_has_field`` compat gate.

The five user-confirmed invariants (each asserted below):
  1. ``[סגן`` -> ONLY ``[סגן`` (bracket-in-query = exact).
  2. bare ``סגן`` -> also finds ``[סגן`` / prefixed forms ON pages that contain
     ``סגן``; NO prefix expansion (never retrieves a ``בסגן``-only page).
  3. ``בסגן`` -> finds ``בסגן,``.
  4. ``צמאן`` / ``צ'מאן`` -> finds ``צ̇מאן`` (U+0307).
  5. results DISPLAY original text (stored ``content`` keeps dots/brackets).
"""

import inspect
import os
from types import SimpleNamespace

import tantivy

from genizah_core import (
    Indexer,
    SearchEngine,
    _add_bracket_variants,
    _index_has_field,
    strip_search_diacritics,
)
from shared.local_indexer import build_local_schema
from shared.search_tokenizer import (
    HEBWORD_TOKENIZER_NAME,
    HEBWORD_TOKENIZER_PATTERN,
    register_search_tokenizers,
)

# --- Hebrew fixtures (escaped so the source stays ASCII-clean) -------------
SAGAN = "סגן"                     # סגן
B_SAGAN = "ב" + SAGAN                       # בסגן  (prefixed)
BRACKET_SAGAN = "[" + SAGAN                      # [סגן  (reconstruction marker)
TSAMAN_CLEAN = "צמאן"        # צמאן
TSAMAN_DOT = "צ̇מאן"    # צ̇מאן (U+0307 Judeo-Arabic dot)
TSAMAN_GERESH = "צ'מאן"      # צ'מאן (ASCII apostrophe)
AMAR_GERESH = "אמ'"                    # אמ'


# Tiny corpus exercising every class of attached word.
DOCS = {
    "comma": "מצותה " + B_SAGAN + ", הקשו",  # מצותה בסגן, הקשו
    "plain": "עוד " + SAGAN + " כאן",                        # עוד סגן כאן
    "paren": "טקסט (" + SAGAN + ") פנימי",     # טקסט (סגן) פנימי
    "bracket": "זה " + BRACKET_SAGAN + " בסוגריים",  # זה [סגן בסוגריים
    "prefix": "רק " + B_SAGAN + " בלבד",                      # רק בסגן בלבד
    "dot": TSAMAN_DOT + " בכתב",                                       # צ̇מאן בכתב
    "clean": TSAMAN_CLEAN + " ברור",                                   # צמאן ברור
    "geresh": AMAR_GERESH + " פה",                                               # אמ' פה
}


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _build_index(tmp_path, docs=DOCS, with_content_search=True):
    """Build a tiny on-disk index using the REAL local schema + tokenizers.

    Populates ``content`` with original text and ``content_search`` with the
    diacritic-folded form, exactly as the engine's add-document sites do. When
    *with_content_search* is False the field is simply left empty (simulating an
    OLD index for the compat-gate test — the field still exists in the schema,
    so for a true "missing field" test we build a bespoke schema instead).
    """
    os.makedirs(str(tmp_path), exist_ok=True)
    idx = tantivy.Index(build_local_schema(), path=str(tmp_path))
    register_search_tokenizers(idx)
    writer = idx.writer(heap_size=15_000_000)
    for uid, text in docs.items():
        pos = Indexer._extract_position_fields(text)
        writer.add_document(
            tantivy.Document(
                unique_id=[uid],
                content=[text],
                content_search=[strip_search_diacritics(text) if with_content_search else ""],
                content_head=[pos["content_head"]],
                content_tail=[pos["content_tail"]],
                line_starts=[pos["line_starts"]],
                line_ends=[pos["line_ends"]],
                source=["LOCAL"],
                full_header=[uid],
                shelfmark=[uid],
                scope=["page"],
                boundaries=[""],
                scan_run_id=[""],
                chunk_locator=[""],
            )
        )
    writer.commit()
    idx.reload()
    return idx


def _stub_engine(has_content_search=True):
    """A minimal object exposing exactly what build_tantivy_query touches."""

    class _Var:
        def get_variants(self, term, mode, limit=200):
            return [term]  # deterministic: exact only

    return SimpleNamespace(var_mgr=_Var(), _has_content_search=has_content_search)


def _genizah_retrieve(idx, raw_query, mode="exact"):
    """Mirror the main word-search retrieval layer (Genizah path).

    Replicates execute_search: strip diacritics, split, build the Tantivy query
    WITH the content_search fallback, then parse against the ``content`` default
    field (content_search is reached via its explicit field clause).
    """
    stripped = strip_search_diacritics(raw_query)
    terms = stripped.split()
    t_query = SearchEngine.build_tantivy_query(
        _stub_engine(), terms, mode, content_search_field="content_search"
    )
    q = idx.parse_query(t_query, ["content"])
    s = idx.searcher()
    return {s.doc(a)["unique_id"][0] for _sc, a in s.search(q, 50).hits}


def _genizah_ranked(idx, raw_query, mode="exact"):
    """Like _genizah_retrieve but returns (uid, score) ordered by score desc."""
    stripped = strip_search_diacritics(raw_query)
    t_query = SearchEngine.build_tantivy_query(
        _stub_engine(), stripped.split(), mode, content_search_field="content_search"
    )
    q = idx.parse_query(t_query, ["content"])
    s = idx.searcher()
    return [(s.doc(a)["unique_id"][0], sc) for sc, a in s.search(q, 50).hits]


def _local_retrieve(idx, raw_query, has_content_search=True):
    """Mirror _query_local_index's field-less parse over the LOCAL field list."""
    fields = ["content", "content_head", "content_tail"]
    if has_content_search:
        fields.append("content_search")
    stripped = strip_search_diacritics(raw_query)
    q = idx.parse_query(stripped, fields)
    s = idx.searcher()
    return {s.doc(a)["unique_id"][0] for _sc, a in s.search(q, 50).hits}


# ---------------------------------------------------------------------------
# 1. hebword tokenizer behaviour
# ---------------------------------------------------------------------------

class TestHebwordTokenizer:
    def _analyze(self, text):
        ta = tantivy.TextAnalyzerBuilder(
            tantivy.Tokenizer.regex(HEBWORD_TOKENIZER_PATTERN)
        ).build()
        return ta.analyze(text)

    def test_comma_is_a_boundary(self):
        # The reported bug: בסגן, must tokenize to בסגן (clean).
        assert self._analyze("מצותה " + B_SAGAN + ",") == [
            "מצותה",
            B_SAGAN,
        ]

    def test_combining_dot_kept(self):
        assert self._analyze(TSAMAN_DOT) == [TSAMAN_DOT]

    def test_apostrophe_kept(self):
        assert self._analyze(AMAR_GERESH) == [AMAR_GERESH]

    def test_brackets_kept(self):
        assert self._analyze(BRACKET_SAGAN) == [BRACKET_SAGAN]

    def test_parens_and_period_and_slash_split(self):
        assert self._analyze("(" + SAGAN + ")") == [SAGAN]
        assert self._analyze("a/b.c") == ["a", "b", "c"]

    def test_colon_splits_so_position_fields_must_stay_whitespace(self):
        # L{n}:word markers would be shattered by hebword -> they stay on
        # whitespace; content (no markers) splitting the colon is harmless.
        assert self._analyze("L3:שלום") == ["L3", "שלום"]

    def test_pattern_has_no_literal_whitespace(self):
        # HIGH-1 (codex): the pattern must not contain a literal space, or it
        # would stop splitting on spaces.
        assert " " not in HEBWORD_TOKENIZER_PATTERN
        assert HEBWORD_TOKENIZER_NAME == "hebword"


# ---------------------------------------------------------------------------
# 2. _add_bracket_variants gating (invariants 1 & 2)
# ---------------------------------------------------------------------------

class TestBracketVariantGating:
    def test_bare_term_expands(self):
        v = _add_bracket_variants(SAGAN)
        assert BRACKET_SAGAN in v and SAGAN + "]" in v

    def test_bracketed_term_not_expanded(self):
        # [סגן is an exact query -> returned unchanged (no bare/other forms).
        assert _add_bracket_variants(BRACKET_SAGAN) == [BRACKET_SAGAN]

    def test_closing_bracket_term_not_expanded(self):
        assert _add_bracket_variants(SAGAN + "]") == [SAGAN + "]"]


# ---------------------------------------------------------------------------
# 3. build_tantivy_query content_search fallback clause
# ---------------------------------------------------------------------------

class TestBuildTantivyQueryContentSearch:
    def test_clause_added_when_field_supplied(self):
        q = SearchEngine.build_tantivy_query(
            _stub_engine(), [TSAMAN_CLEAN], "exact", content_search_field="content_search"
        )
        assert f'content_search:"{TSAMAN_CLEAN}"^0.5' in q
        # exact-content boost preserved (ranking invariant)
        assert f'"{TSAMAN_CLEAN}"^5' in q

    def test_no_clause_without_field(self):
        q = SearchEngine.build_tantivy_query(_stub_engine(), [TSAMAN_CLEAN], "exact")
        assert "content_search" not in q

    def test_clause_term_is_diacritic_folded(self):
        # geresh query term -> folded for the content_search clause.
        q = SearchEngine.build_tantivy_query(
            _stub_engine(), [AMAR_GERESH], "exact", content_search_field="content_search"
        )
        assert 'content_search:"אמ"^0.5' in q  # אמ (geresh stripped)

    def test_bracket_query_clause_keeps_bracket_no_expansion(self):
        q = SearchEngine.build_tantivy_query(
            _stub_engine(), [BRACKET_SAGAN], "exact", content_search_field="content_search"
        )
        assert f'content_search:"{BRACKET_SAGAN}"^0.5' in q
        # no bare/other-bracket forms leaked in
        assert f'"{SAGAN}]"' not in q and f'"]{SAGAN}"' not in q


# ---------------------------------------------------------------------------
# 4. End-to-end invariants (Genizah retrieval layer)
# ---------------------------------------------------------------------------

class TestInvariantsGenizah:
    def test_inv3_comma_word_retrievable(self, tmp_path):
        # בסגן must now find the בסגן, (comma) doc — the reported bug.
        hits = _genizah_retrieve(_build_index(tmp_path), B_SAGAN)
        assert "comma" in hits
        assert "prefix" in hits  # also legitimately contains בסגן

    def test_inv1_bracket_query_is_exact(self, tmp_path):
        hits = _genizah_retrieve(_build_index(tmp_path), BRACKET_SAGAN)
        assert hits == {"bracket"}

    def test_inv2_bare_query_loose_but_no_prefix_expansion(self, tmp_path):
        hits = _genizah_retrieve(_build_index(tmp_path), SAGAN)
        # finds clean סגן, parenthesised סגן, and the [סגן doc (bracket variant)
        assert {"plain", "paren", "bracket"} <= hits
        # NO prefix expansion: never retrieves a בסגן-only page from a סגן query
        assert "comma" not in hits
        assert "prefix" not in hits

    def test_inv4_diacritic_fold_all_forms(self, tmp_path):
        idx = _build_index(tmp_path)
        for q in (TSAMAN_CLEAN, TSAMAN_GERESH, TSAMAN_DOT):
            hits = _genizah_retrieve(idx, q)
            assert "dot" in hits, f"query {q!r} did not retrieve the צ̇מאן doc"

    def test_inv5_display_keeps_original_text(self, tmp_path):
        # Stored content is the ORIGINAL (dot + bracket visible).
        idx = _build_index(tmp_path)
        s = idx.searcher()
        q = idx.parse_query('unique_id:"dot"', ["unique_id"])
        doc = s.doc(s.search(q, 1).hits[0][1])
        assert TSAMAN_DOT in doc["content"][0]
        q2 = idx.parse_query('unique_id:"bracket"', ["unique_id"])
        doc2 = s.doc(s.search(q2, 1).hits[0][1])
        assert BRACKET_SAGAN in doc2["content"][0]

    def test_ranking_exact_above_fold(self, tmp_path):
        # MEDIUM-7: the clean (exact) doc must rank above the dot-only fold match.
        ranked = _genizah_ranked(_build_index(tmp_path), TSAMAN_CLEAN)
        order = [uid for uid, _ in ranked]
        assert "clean" in order and "dot" in order
        assert order.index("clean") < order.index("dot")


# ---------------------------------------------------------------------------
# 5. End-to-end invariants (LOCAL retrieval layer — field-less fan-out)
# ---------------------------------------------------------------------------

class TestInvariantsLocal:
    def test_inv3_comma_word_retrievable(self, tmp_path):
        hits = _local_retrieve(_build_index(tmp_path), B_SAGAN)
        assert "comma" in hits

    def test_inv4_diacritic_fold(self, tmp_path):
        idx = _build_index(tmp_path)
        for q in (TSAMAN_CLEAN, TSAMAN_GERESH, TSAMAN_DOT):
            assert "dot" in _local_retrieve(idx, q), q

    def test_geresh_abbreviation_retrievable(self, tmp_path):
        # אמ' typed with/without geresh both reach the אמ' doc via content_search.
        idx = _build_index(tmp_path)
        assert "geresh" in _local_retrieve(idx, "אמ")       # אמ
        assert "geresh" in _local_retrieve(idx, AMAR_GERESH)          # אמ'


# ---------------------------------------------------------------------------
# 6. Position regression (HIGH-4) — line_starts L{n}:word still works
# ---------------------------------------------------------------------------

class TestPositionFieldsRegression:
    def test_line_start_marker_query_still_works(self, tmp_path):
        # Multi-line doc; an L{n}:word query must hit the right line via the
        # whitespace-tokenized line_starts field (NOT shattered by a colon).
        docs = {
            "multi": "שלום עולם\n"  # שלום עולם
                     "עליכם רבים",  # עליכם רבים
        }
        idx = _build_index(tmp_path, docs=docs)
        s = idx.searcher()
        # L2:עליכם — word at start of line 2
        marker = "L2:עליכם"
        q = idx.parse_query(f'line_starts:"{marker}"', ["line_starts"])
        hits = {s.doc(a)["unique_id"][0] for _sc, a in s.search(q, 5).hits}
        assert hits == {"multi"}
        # The marker for a wrong line must NOT match.
        bad = idx.parse_query('line_starts:"L1:עליכם"', ["line_starts"])
        assert len(s.search(bad, 5).hits) == 0


# ---------------------------------------------------------------------------
# 7. Compat gate — querying content_search against an OLD index never crashes
# ---------------------------------------------------------------------------

class TestCompatGate:
    def _old_schema_index(self, tmp_path):
        """An index WITHOUT a content_search field (pre-SEED-006 shape)."""
        os.makedirs(str(tmp_path), exist_ok=True)
        b = tantivy.SchemaBuilder()
        b.add_text_field("unique_id", stored=True, tokenizer_name="raw")
        b.add_text_field("content", stored=True, tokenizer_name="whitespace")
        idx = tantivy.Index(b.build(), path=str(tmp_path))
        register_search_tokenizers(idx)
        w = idx.writer(heap_size=15_000_000)
        w.add_document(tantivy.Document(unique_id=["a"], content=[SAGAN]))
        w.commit()
        idx.reload()
        return idx

    def test_index_has_field_detects_presence(self, tmp_path):
        new_idx = _build_index(tmp_path / "new")
        old_idx = self._old_schema_index(tmp_path / "old")
        assert _index_has_field(new_idx, "content_search") is True
        assert _index_has_field(old_idx, "content_search") is False

    def test_query_without_clause_works_on_old_index(self, tmp_path):
        # When the gate is off, build_tantivy_query omits content_search, so the
        # query parses cleanly against an index that lacks the field.
        old_idx = self._old_schema_index(tmp_path)
        t_query = SearchEngine.build_tantivy_query(
            _stub_engine(has_content_search=False), [SAGAN], "exact"
        )
        q = old_idx.parse_query(t_query, ["content"])  # must not raise
        assert len(old_idx.searcher().search(q, 5).hits) == 1

    def test_local_fields_omit_content_search_when_absent(self, tmp_path):
        old_idx = self._old_schema_index(tmp_path)
        # has_content_search=False -> fields exclude content_search -> no crash.
        q = old_idx.parse_query(SAGAN, ["content"])
        assert len(old_idx.searcher().search(q, 5).hits) == 1

    def test_missing_field_error_wording_is_pinned(self, tmp_path):
        # L4: _index_has_field keys on 'does not exist' / 'not defined' in the
        # tantivy ValueError text. Pin that wording so a future tantivy reword
        # fails THIS test loudly (else the gate would fail OPEN — emit a
        # content_search clause against a fieldless index and crash at runtime).
        old_idx = self._old_schema_index(tmp_path)
        import pytest
        with pytest.raises(ValueError) as exc:
            old_idx.parse_query('content_search:"x"', ["content_search"])
        msg = str(exc.value).lower()
        assert ("does not exist" in msg) or ("not defined" in msg), (
            f"tantivy missing-field wording changed: {exc.value!r} — update "
            "_index_has_field in genizah_core.py to match."
        )


# ---------------------------------------------------------------------------
# 7b. Real call site — drive the actual _query_local_index (not a mirror).
#     Addresses L1: the helpers above re-implement retrieval; this exercises
#     the genuine LOCAL method + its metacharacter-strip fallback, incl. the
#     LOCAL bracket-exactness that leans on the regex backstop.
# ---------------------------------------------------------------------------

class TestRealLocalQueryCallSite:
    def _engine(self, idx):
        eng = SearchEngine.__new__(SearchEngine)  # bypass __init__ (no real indexes)
        eng.local_index = idx
        eng.local_searcher = idx.searcher()
        eng._local_has_content_search = True
        eng._my_library_tab_ref = None       # is_searchable gate defaults True
        eng._last_local_query_regex = None
        eng.var_mgr = _stub_engine().var_mgr  # get_variants -> [term]
        return eng

    def _search(self, eng, raw_query, mode="exact", gap=0):
        """Replicate execute_search's corpus_scope=='local' branch exactly."""
        q = strip_search_diacritics(raw_query) if mode != "Regex" else raw_query
        terms = [q] if mode == "Regex" else q.split()
        regex = eng.build_regex_pattern(terms, mode, gap)
        hits = eng._query_local_index(q, mode, gap, regex=regex)
        return {h["uid"] for h in hits}

    def test_comma_word_via_real_method(self, tmp_path):
        eng = self._engine(_build_index(tmp_path))
        assert "comma" in self._search(eng, B_SAGAN)

    def test_diacritic_fold_via_real_method(self, tmp_path):
        eng = self._engine(_build_index(tmp_path))
        assert "dot" in self._search(eng, TSAMAN_CLEAN)
        assert "dot" in self._search(eng, TSAMAN_GERESH)  # geresh-typed too

    def test_local_bracket_query_no_false_positives(self, tmp_path):
        # Invariant 1 on LOCAL = "exact-or-nothing" (never over-match).
        # LOCAL [סגן goes through the pre-existing metacharacter-strip fallback
        # (the parser can't take a literal '['), which rewrites the query to
        # bare סגן; the regex backstop (literal '[') then drops the bare/
        # parenthesised סגן docs. The bracket doc itself is NOT retrievable on
        # LOCAL (its hebword token is '[סגן', and LOCAL does no bracket-variant
        # expansion — a documented pre-existing limitation, unchanged by SEED-006).
        # What matters: NO false positives leak in.
        eng = self._engine(_build_index(tmp_path))
        hits = self._search(eng, BRACKET_SAGAN)
        assert hits.isdisjoint({"plain", "paren", "prefix", "comma"})


# ---------------------------------------------------------------------------
# 7c. P2 — ASCII double-quote Hebrew abbreviations (רמב"ם) are retrievable
#     and never emit invalid Tantivy syntax.
# ---------------------------------------------------------------------------

RAMBAM_QUOTE = 'רמב"ם'       # ASCII double-quote (typed gershayim substitute)
RAMBAM_GERSHAYIM = "רמב״ם"   # Hebrew gershayim U+05F4
RAMBAM_CLEAN = "רמבם"        # no separator


class TestAsciiQuoteAbbreviation:
    def test_strip_folds_ascii_double_quote(self):
        # The ASCII " folds exactly like the Hebrew gershayim ".
        assert strip_search_diacritics(RAMBAM_QUOTE) == RAMBAM_CLEAN
        assert strip_search_diacritics(RAMBAM_GERSHAYIM) == RAMBAM_CLEAN

    def test_clean_query_retrieves_ascii_quote_doc(self, tmp_path):
        # Corpus stores רמב"ם (ASCII quote); a clean רמבם query must reach it
        # through the content_search fold.
        idx = _build_index(tmp_path, docs={"rambam": RAMBAM_QUOTE + " כתב"})
        assert "rambam" in _genizah_retrieve(idx, RAMBAM_CLEAN)
        assert "rambam" in _local_retrieve(idx, RAMBAM_CLEAN)

    def test_quoted_query_retrieves_ascii_quote_doc(self, tmp_path):
        # Typing the abbreviation WITH the quote must also retrieve (and not
        # raise a Tantivy parse error).
        idx = _build_index(tmp_path, docs={"rambam": RAMBAM_QUOTE + " כתב"})
        assert "rambam" in _genizah_retrieve(idx, RAMBAM_QUOTE)

    def test_build_query_strips_quote_no_invalid_syntax(self):
        # A raw " inside a term must not survive into a quoted clause as
        # "רמב"ם" (which Tantivy rejects). build_tantivy_query strips it.
        q = SearchEngine.build_tantivy_query(
            _stub_engine(), [RAMBAM_QUOTE], "exact",
            content_search_field="content_search",
        )
        assert '"' + RAMBAM_QUOTE + '"' not in q   # no raw quoted term
        assert RAMBAM_CLEAN in q                     # folded form present

    def test_build_query_quote_term_parses(self, tmp_path):
        # End-to-end: the produced query string parses cleanly against a real index.
        idx = _build_index(tmp_path, docs={"rambam": RAMBAM_QUOTE + " כתב"})
        q = SearchEngine.build_tantivy_query(
            _stub_engine(), [RAMBAM_QUOTE], "exact",
            content_search_field="content_search",
        )
        idx.parse_query(q, ["content"])  # must not raise


# ---------------------------------------------------------------------------
# 8. Source guards — both schemas conform; position fields stay whitespace
# ---------------------------------------------------------------------------

class TestSchemaSourceGuards:
    def test_local_schema_content_is_hebword(self):
        src = inspect.getsource(build_local_schema)
        assert 'add_text_field("content", stored=True, tokenizer_name="hebword")' in src
        assert 'add_text_field("content_search"' in src
        assert 'tokenizer_name="hebword"' in src.split('content_search')[1][:80]

    def test_local_schema_position_fields_stay_whitespace(self):
        src = inspect.getsource(build_local_schema)
        for field in ("content_head", "content_tail", "line_starts", "line_ends"):
            assert f'add_text_field("{field}", stored=False, tokenizer_name="whitespace")' in src

    def test_create_index_content_is_hebword_with_content_search(self):
        src = inspect.getsource(Indexer.create_index)
        assert 'add_text_field("content", stored=True, tokenizer_name="hebword")' in src
        assert 'add_text_field("content_search"' in src

    def test_create_index_position_fields_stay_whitespace(self):
        src = inspect.getsource(Indexer.create_index)
        for field in ("content_head", "content_tail", "line_starts", "line_ends"):
            assert f'add_text_field("{field}", stored=False, tokenizer_name="whitespace")' in src

    def test_create_index_populates_content_search(self):
        src = inspect.getsource(Indexer.create_index)
        assert src.count("content_search=strip_search_diacritics(") >= 2

    def test_add_bracket_variants_is_bracket_gated(self):
        src = inspect.getsource(_add_bracket_variants)
        assert "'[' in term or ']' in term" in src

    def test_query_local_index_appends_content_search_gated(self):
        src = inspect.getsource(SearchEngine._query_local_index)
        assert "_local_has_content_search" in src
        assert '_fields.append("content_search")' in src

    def test_main_word_search_gates_content_search_on_position(self):
        # The fallback must be disabled when text_position is set.
        src = inspect.getsource(SearchEngine.execute_search)
        assert "not text_position" in src and "_has_content_search" in src

    def test_local_composition_has_content_search_parity(self):
        # M1: the Phase-110 LOCAL composition/parallels hook is the 4th retrieval
        # site — it must also fan across content_search (gated) and fold the chunk.
        src = inspect.getsource(SearchEngine.search_composition_logic)
        assert '_local_fields_scl.append("content_search")' in src
        assert "strip_search_diacritics(_w) for _w in _chunk_scl" in src

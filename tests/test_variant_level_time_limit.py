"""Extended and Maximum stop at the time limit, and their regex is fast (2026-10-11).

A Maximum search on the real index ran past the website's 180 s limit and its 60 s
grace, so the research worker was killed. Measured: compiling the one regex took
about 22 s (the flat alternation of 19,000 spellings, 904,000 characters, with the
stdlib pattern compiled twice), each candidate page took about a second to search,
and the time limit -- which acts only at a progress tick -- was checked once per 200
candidates. A composition search built every chunk's regex before its first tick.

Now: Extended and Maximum write each word's spellings as prefix trees
(_variant_alternation), which match the same texts; Basic keeps its pattern; the
candidate loop also ticks at the next candidate once _PROGRESS_TICK_SECONDS have
passed, and before the aggregate query; and a composition chunk's plan is built
after that chunk's tick (and a My Library chunk that cannot be built still fails
the search).
"""
import gc
import json
import os
import random
import re
from unittest.mock import patch

import pytest

tantivy = pytest.importorskip("tantivy")

from shared import search_regex  # noqa: E402
from shared.config import Config  # noqa: E402
from shared.indexer import Indexer  # noqa: E402
from shared.lab_settings import LabSettings  # noqa: E402
from shared.search_tokenizer import register_search_tokenizers  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402
from shared.variants import VariantManager  # noqa: E402
import shared.search_engine as se  # noqa: E402

DIAERESIS, DOT = "̈", "̇"


def _flat(forms):
    """build_regex_pattern's alternation before the prefix trees (Basic's still)."""
    ordered = sorted(set(forms), key=len, reverse=True)
    return "|".join(se.make_mark_tolerant_pattern(re.escape(f)) for f in ordered)


def _compile(alternation):
    return re.compile(f"({alternation})", re.IGNORECASE)


# --- the prefix trees match what the flat alternation matched ---------------------

def _random_forms(rng, letters, n, marks=""):
    forms = set()
    while len(forms) < n:
        word = "".join(rng.choice(letters) for _ in range(rng.randint(1, 6)))
        if marks and rng.random() < 0.4:
            at = rng.randint(1, len(word))
            word = word[:at] + rng.choice(marks) + word[at:]
        forms.add(word)
    return sorted(forms)


def _random_text(rng, forms, letters, marks):
    pieces = []
    for _ in range(rng.randint(1, 8)):
        roll = rng.random()
        if roll < 0.5:
            word = rng.choice(forms)
            if marks and rng.random() < 0.3:      # a mark or quote between its letters
                at = rng.randint(1, len(word))
                word = word[:at] + rng.choice(marks) + word[at:]
            pieces.append(word)
        else:
            pieces.append("".join(rng.choice(letters + marks) for _ in range(rng.randint(1, 4))))
        pieces.append(rng.choice([" ", "", "\n", "."]))
    return "".join(pieces)


@pytest.mark.parametrize("seed", range(6))
def test_letters_only_forms_give_the_same_spans(seed):
    # Without marks, the forms found at one place are prefixes of one another: the
    # flat alternation's longest-first order gives one span, and so must the trees.
    rng = random.Random(seed)
    letters = "אבגדהוכלמש"
    forms = _random_forms(rng, letters, 60)
    flat, tree = _compile(_flat(forms)), _compile(se._variant_alternation(_sorted(forms)))
    for _ in range(300):
        text = _random_text(rng, forms, letters, "")
        assert ([(m.span(), m.span(1)) for m in flat.finditer(text)]
                == [(m.span(), m.span(1)) for m in tree.finditer(text)]), text


def _sorted(forms):
    return sorted(set(forms), key=len, reverse=True)


@pytest.mark.parametrize("seed", range(6))
def test_forms_with_marks_match_the_same_texts_from_the_same_places(seed):
    # The website's custom pairs (ה̈=ה, ת̇=ת ...) put marks inside forms. The trees
    # match the same texts, and every match starts where the flat one's does.
    rng = random.Random(seed)
    letters, marks = "אבגדהת", DIAERESIS + DOT + "'׳"
    forms = _random_forms(rng, letters, 60, marks=DIAERESIS + DOT)
    flat, tree = _compile(_flat(forms)), _compile(se._variant_alternation(_sorted(forms)))
    for _ in range(300):
        text = _random_text(rng, forms, letters, marks)
        for start in range(len(text) + 1):
            f, t = flat.match(text, start), tree.match(text, start)
            assert (f is None) == (t is None), (text, start)
        assert bool(flat.fullmatch(text)) == bool(tree.fullmatch(text)), text


@pytest.mark.parametrize("seed", range(4))
def test_forms_with_marks_give_a_span_the_flat_alternation_could_give(seed):
    # Among forms of one length the flat order was set order. Where two of them
    # match at one place, the tree's span must be the flat alternation's with SOME
    # form first in its length group -- here followed by a separator and a second
    # word, so a failed continuation makes both backtrack.
    rng = random.Random(100 + seed)
    letters, marks = "אבגדהת", DIAERESIS + DOT + "'׳"
    forms = _sorted(_random_forms(rng, letters, 40, marks=DIAERESIS + DOT))
    rest = se._gap_separator(0) + "(אב|ב)"

    def compiled(alternation):
        return re.compile(f"({alternation}){rest}", re.IGNORECASE)

    def with_first(form):
        group = [f for f in forms if len(f) == len(form) and f != form]
        ordered = [f for f in forms if len(f) > len(form)] + [form] + group \
            + [f for f in forms if len(f) < len(form)]
        return compiled("|".join(se.make_mark_tolerant_pattern(re.escape(f)) for f in ordered))

    tree = compiled(se._variant_alternation(forms))
    orderings = [with_first(f) for f in forms]
    checked = 0
    for _ in range(150):
        text = _random_text(rng, forms + ["אב"], letters, marks)
        for start in range(len(text) + 1):
            t = tree.match(text, start)
            if t is None:
                assert not any(o.match(text, start) for o in orderings), (text, start)
                continue
            spans = {(m.span(), m.span(1)) for m in (o.match(text, start) for o in orderings) if m}
            assert (t.span(), t.span(1)) in spans, (text, start)
            checked += 1
    assert checked


def test_long_or_newline_forms_keep_the_flat_alternation():
    # A tree nests a group per character (regex's compiler fails between 100 and
    # 200 levels; the renderer recursed into RecursionError on a 502-character
    # form), and the old tokenizer does not read a newline as a character.
    # Forms of one length sharing a prefix: a tree would differ from the flat join.
    long_forms = ["א" * (se._PREFIX_TREE_MAX_FORM + 1), "א" * se._PREFIX_TREE_MAX_FORM + "ב"]
    newline_forms = ["א\nב", "א\nג"]
    for forms in (long_forms, newline_forms):
        assert se._variant_alternation(forms) == "|".join(
            se.make_mark_tolerant_pattern(re.escape(f)) for f in forms)
    at_cap = ["א" * se._PREFIX_TREE_MAX_FORM, "א" * (se._PREFIX_TREE_MAX_FORM - 1) + "ב"]
    assert se._variant_alternation(at_cap) != "|".join(
        se.make_mark_tolerant_pattern(re.escape(f)) for f in at_cap), "forms at the cap get a tree"


def test_a_very_long_word_at_extended_still_searches():
    settings = LabSettings()
    settings.variant_pairs_count = 70
    settings.variant_max_changes = 2
    settings.custom_variants = {"aa=ab": True}
    term = "x" * 500 + "aa"
    regex = _engine_with(VariantManager(settings=settings)).build_regex_pattern([term], "variants_extended", 0)
    assert regex.search(term).span() == (0, 502)
    assert regex.search("x" * 500 + "ab").span() == (0, 502)


def test_a_longer_form_is_still_tried_first():
    forms = ["אב", "אבג", "אבגד" + DIAERESIS, "ב"]
    tree = _compile(se._variant_alternation(_sorted(forms)))
    assert tree.search("אבג").span() == (0, 3)
    assert tree.search("אבגד" + DIAERESIS).span() == (0, 5)
    assert tree.search("x אב y").span() == (2, 4)


def _manager(pairs, changes):
    settings = LabSettings()
    settings.variant_pairs_count = pairs
    settings.variant_max_changes = changes
    settings.custom_variants = {f"ה{DIAERESIS}=ה": True, f"ת{DOT}=ת": True}
    return VariantManager(settings=settings)


def _engine_with(manager):
    engine = se.SearchEngine.__new__(se.SearchEngine)
    engine.var_mgr = manager
    return engine


def test_basic_keeps_its_pattern():
    manager = _manager(30, 1)
    terms = ["והמשפטים", "אשר"]
    built = _engine_with(manager).build_regex_pattern(terms, "variants", 0).pattern
    expected = se._gap_separator(0).join(
        "(" + _flat(manager.get_variants(t, "variants", limit=Config.REGEX_VARIANTS_LIMIT) + [t]) + ")"
        for t in terms)
    assert built == expected


@pytest.mark.parametrize("pairs, mode", [(70, "variants_extended"), (150, "variants_maximum"),
                                         (150, "variants")])
def test_extended_and_maximum_use_the_trees(pairs, mode):
    # The level is the pairs count's, whatever the mode string (a composition search
    # runs mode 'variants' with 70 or 150 pairs).
    manager = _manager(pairs, 2)
    engine = _engine_with(manager)
    terms = ["אתה", "זכור"]
    tree = engine.build_regex_pattern(terms, mode, 0)
    flat = se._gap_separator(0).join(
        "(" + _flat(manager.get_variants(t, mode, limit=Config.REGEX_VARIANTS_LIMIT) + [t]) + ")"
        for t in terms)
    assert len(tree.pattern) < len(flat) / 2
    flat = re.compile(flat, re.IGNORECASE)
    for text in ("אתה זכור", "עתה זכור", "את" + DOT + "ה זכור", "אתה" + DIAERESIS + " זכר", "x אתה, זכור y"):
        f, t = flat.search(text), tree.search(text)
        assert (f and f.span()) == (t and t.span()), text


def test_search_regex_compiles_the_stdlib_pattern_once():
    # The second compile of the same pattern missed re's cache (its flags now hold
    # UNICODE): at Maximum it cost several seconds of the time limit.
    digits = str(random.random()).replace(".", "")           # a pattern not in re's cache
    real = re.compile
    with patch.object(search_regex.re, "compile", side_effect=real) as spy:
        compiled = search_regex.compile(f"(?:אבג|דהו){digits}", re.IGNORECASE)
    assert spy.call_count == 1
    assert compiled._stdlib.search(f"אבג{digits}") and compiled.search(f"דהו{digits}")


# --- the time limit is checked while candidates are slow -------------------------

W, B1, B2 = "שלום", "ברוך", "הבא"


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


PAGES = [(f"p{n}", f"אבג {W} דהו", "page", "") for n in range(8)]
AGG_TEXT = f"אבג {B1}\n{B2} דהו"
AGGS = [("sys:1", AGG_TEXT, "system", json.dumps([
    {"uid": "a1", "p_num": 1, "full_header": "a1", "source": "V0.8", "sys_id": "1",
     "start": 0, "end": len(f"אבג {B1}") + 1},
    {"uid": "b1", "p_num": 2, "full_header": "b1", "source": "V0.8", "sys_id": "1",
     "start": len(f"אבג {B1}") + 1, "end": len(AGG_TEXT)}]))]


def _build_index(path, docs):
    os.makedirs(path)
    idx = tantivy.Index(_schema(), path=path)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)
    for uid, text, scope, bounds in docs:
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text), source="V0.8",
            full_header=f"IE_{uid} 99{len(uid)}", shelfmark=uid, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))
    w.commit()
    w.wait_merging_threads()
    idx.reload()
    return idx


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("variant_limit"))
    _build_index(os.path.join(root, "tantivy_db"), PAGES + AGGS)
    with patch.object(Config, "INDEX_DIR", root):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    eng = None
    gc.collect()


def _stop_on_call(n):
    calls = []

    def progress(i, total):
        calls.append((i, total))
        if len(calls) >= n:
            raise InterruptedError("Search time limit reached")
    return progress, calls


def test_slow_candidates_still_reach_the_time_limit(engine):
    # 200 candidates between ticks took 209 s at Maximum; a tick by time stops it.
    progress, calls = _stop_on_call(3)
    se.consume_last_search_cutoff()
    with patch.object(se, "_PROGRESS_TICK_EVERY", 1000), patch.object(se, "_PROGRESS_TICK_SECONDS", 0):
        rows = engine.execute_search(W, "literal", 0, corpus_scope="genizah", progress_callback=progress)
    assert [i for i, _ in calls] == [0, 1, 2]
    assert len(rows) == 2, "the candidates checked before the limit are kept"
    assert se.consume_last_search_cutoff()["interrupted"]


def test_fast_candidates_tick_by_count_only(engine):
    progress, calls = _stop_on_call(3)
    with patch.object(se, "_PROGRESS_TICK_EVERY", 1000), patch.object(se, "_PROGRESS_TICK_SECONDS", 1e9):
        rows = engine.execute_search(W, "literal", 0, corpus_scope="genizah", progress_callback=progress)
    assert calls == [(0, len(PAGES))]
    assert len(rows) == len(PAGES)


def test_the_aggregate_query_waits_for_a_checkpoint(engine):
    # The phrase has no page docs; the whole-manuscript query is a second native
    # query that cannot be stopped, so the limit is checked before it runs.
    progress, calls = _stop_on_call(1)
    queries = []
    real = se._top_hits

    def counting(*args, **kwargs):
        queries.append(1)
        return real(*args, **kwargs)
    se.consume_last_search_cutoff()
    with patch.object(se, "_top_hits", side_effect=counting):
        rows = engine.execute_search(f"{B1} {B2}", "literal", 0, corpus_scope="genizah",
                                     progress_callback=progress)
    assert rows == [] and len(queries) == 1 and calls == [(0, 0)]
    assert se.consume_last_search_cutoff()["interrupted"]
    queries.clear()
    with patch.object(se, "_top_hits", side_effect=counting):
        rows = engine.execute_search(f"{B1} {B2}", "literal", 0, corpus_scope="genizah")
    assert len(queries) == 2 and rows, "unstopped, the aggregate still gives its row"


def test_a_composition_chunk_is_prepared_after_its_checkpoint(engine):
    # An Extended or Maximum chunk regex takes seconds: preparing every chunk before
    # the first checkpoint ran past the limit (0-1 results, or the worker killed).
    text = " ".join([f"אבג {W} דהו"] * 4)          # 12 words, 11 chunks of 2
    progress, calls = _stop_on_call(4)
    built = []
    real = se.SearchEngine.build_regex_pattern

    def counting(self, *args, **kwargs):
        built.append(1)
        return real(self, *args, **kwargs)
    se.consume_last_search_cutoff()
    with patch.object(se.SearchEngine, "build_regex_pattern", counting), \
            patch.object(se, "_PROGRESS_TICK_SECONDS", 1e9):
        result = engine.search_composition_logic(text, 2, float("inf"), "literal",
                                                 progress_callback=progress)
    assert len(built) == 3, "only the chunks reached before the stop are prepared"
    assert result["partial"] is True and result["main"]
    assert se.consume_last_search_cutoff()["interrupted"]


def test_a_composition_chunk_with_slow_documents_reaches_the_time_limit(engine):
    # The stop comes at the chunk's first document, and the chunk's broad error
    # handler must not swallow it (InterruptedError is an OSError).
    text = " ".join([f"אבג {W} דהו"] * 4)
    progress, calls = _stop_on_call(2)
    se.consume_last_search_cutoff()
    with patch.object(se, "_PROGRESS_TICK_SECONDS", 0):
        result = engine.search_composition_logic(text, 2, float("inf"), "literal",
                                                 progress_callback=progress)
    assert [i for i, _ in calls] == [0, 0]
    assert result["partial"] is True and result["main"] == []
    assert se.consume_last_search_cutoff()["interrupted"]


@pytest.mark.parametrize("scope", ["genizah", "local"])
def test_a_chunk_that_cannot_be_prepared_fails_the_composition_search(engine, tmp_path, scope):
    # Prepared lazily, a My Library chunk is built inside the LOCAL block, whose
    # broad handler logs and reports empty results. A build error must still fail
    # the search, as when every chunk was prepared first (Codex review, 2026-10-11).
    local = _build_index(str(tmp_path / "local"), [("l1", f"אבג {W} דהו", "page", "")])
    saved = (getattr(engine, "local_index", None), getattr(engine, "local_searcher", None))
    engine.local_index, engine.local_searcher = local, local.searcher()

    def broken(self, *args, **kwargs):
        raise ValueError("chunk regex could not be built")
    try:
        with patch.object(se.SearchEngine, "build_regex_pattern", broken), \
                pytest.raises(ValueError, match="could not be built"):
            engine.search_composition_logic(f"אבג {W} דהו", 2, float("inf"), "literal",
                                            corpus_scope=scope)
    finally:
        engine.local_index, engine.local_searcher = saved

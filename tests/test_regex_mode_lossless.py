"""Regex mode must never lose a document the regex matches.

These tests drive the real entry point, ``SearchEngine.execute_search(..., 'Regex')``,
against small Tantivy indexes built with the production schema and the
production ``hebword`` tokenizer registration. For every pattern the expected
result is computed by brute force: run the same compiled regex over every
document text, exactly as the engine's post-filter does. The engine's result
must equal that set -- a smaller set means the candidate query dropped a true
match.
"""
import json
import random
import re
from types import SimpleNamespace

import pytest
import tantivy

from shared import search_engine as se
from shared.config import Config
from shared.indexer import Indexer, build_main_schema as _main_schema
from shared.local_indexer import build_local_schema
from shared.search_engine import SearchEngine, _query_has_brackets, _strip_brackets
from shared.search_regex import compile as compile_search_regex
from shared.search_tokenizer import register_search_tokenizers
from shared.search_engine import strip_search_diacritics


DOCS = {
    "vshalom": "ברכה ושלום לכם",            # שלום inside the token ושלום
    "shalomo": "שלומו של אדם",               # שלום as a token prefix
    "sade": "הלך אל השדה",                   # the other branch of שלום|שדה
    "hamelech": "אמר המלך דוד",              # ה?מלך with the article attached
    "shalvah": "שלוה ושקט",                  # שלו.
    "melachim": "ספר מלכים א",               # מלכ[יו]ם
    "malkom": "אל מלכום",                    # מלכ[יו]ם, other class member
    "bracketed": "ברכת [ש]לום לכם",          # regex runs on the bracket-free text
    "bracketed_run": "ברכת [ש]לום לכם",      # same text in a multi-page (continuous) document
    "comma": "שלום, עליכם",                  # separator inside the literal
    "latin": "Rabbi TESTING case",           # Latin literal, IGNORECASE
    "ishrael": "ובני ישראל כולם",
    "none": "טקסט אחר לגמרי",
}

PATTERNS = [
    "שלום|שדה", "מלכ[יו]ם", "שלו.", "ה?מלך", r"\bא[א-ת]{3}\b", r"^אמר",
    r"\bישראל\b", "test", r"(של)ום \1", r"שלום(?=, )", "ש.ל.ם", "שלום.*",
    r"שלום\s*,\s*עליכם", "ישראל$", r"[^א-ת]ל",
]


def _is_continuous(uid):
    # Multi-page ("continuous") documents build their row from the match span,
    # so a match that exists only in the bracket-free text is still a result.
    return uid.endswith("_run")


def _add(writer, uid, text, *, local=False):
    pos = Indexer._extract_position_fields(text)
    continuous = _is_continuous(uid) and not local
    fields = dict(
        unique_id=[uid], content=[text], content_search=[strip_search_diacritics(text)],
        content_head=[pos["content_head"]], content_tail=[pos["content_tail"]],
        line_starts=[pos["line_starts"]], line_ends=[pos["line_ends"]],
        source=["LOCAL" if local else "V0.8"], full_header=[uid], shelfmark=[uid],
        scope=["system" if continuous else "page"],
        boundaries=[json.dumps([{"start": 0, "end": len(text), "uid": uid, "p_num": 1,
                                 "full_header": uid, "source": "V0.8"}]) if continuous else ""],
    )
    if local:
        fields.update(scan_run_id=[""], chunk_locator=[""])
    writer.add_document(tantivy.Document(**fields))


def _index(docs, schema, *, local=False):
    idx = tantivy.Index(schema)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=15_000_000)
    for uid, text in docs.items():
        _add(w, uid, text, local=local)
    w.commit()
    idx.reload()
    return idx


def _engine(main=None, local=None):
    eng = SearchEngine.__new__(SearchEngine)   # no real indexes / metadata
    eng.index = main
    eng.searcher = main.searcher() if main is not None else None
    eng.local_index = local
    eng.local_searcher = local.searcher() if local is not None else None
    eng._has_content_search = True
    eng._local_has_content_search = True
    eng._my_library_tab_ref = None
    eng._last_local_query_regex = None
    eng.var_mgr = SimpleNamespace(get_variants=lambda term, mode, limit=200: [term])
    eng.meta_mgr = SimpleNamespace(
        get_display_data=lambda header, source: {"source": source, "id": header})
    return eng


def _expected(pattern, docs, *, strip):
    """Pages the engine's own post-filter accepts, by brute force over every text.

    Main index: the regex is tried on the bracket-free text (when the pattern has
    no brackets). A single-page row is then built from a highlight on the stored
    text, so it needs both to match; a continuous document's row is built from
    the match span and needs only the first. My Library: the stored text only.
    """
    rx = compile_search_regex(pattern, re.IGNORECASE)
    use_strip = strip and not _query_has_brackets(pattern)
    return {uid for uid, text in docs.items()
            if rx.search(_strip_brackets(text) if use_strip else text)
            and (rx.search(text) or (strip and _is_continuous(uid)))}


@pytest.fixture(scope="module")
def main_engine():
    return _engine(main=_index(DOCS, _main_schema()))


@pytest.mark.parametrize("pattern", PATTERNS)
def test_regex_search_returns_every_matching_page(main_engine, pattern):
    found = {r["uid"] for r in main_engine.execute_search(
        pattern, "Regex", 0, corpus_scope="genizah")}
    assert found == _expected(pattern, DOCS, strip=True)


@pytest.mark.parametrize("pattern", PATTERNS)
def test_regex_search_on_my_library_returns_every_matching_document(pattern):
    # The LOCAL ("My Library") index matches the regex against the stored text
    # as is (no bracket removal).
    eng = _engine(local=_index(DOCS, build_local_schema(), local=True))
    found = {r["uid"] for r in eng.execute_search(pattern, "Regex", 0, corpus_scope="local")}
    assert found == _expected(pattern, DOCS, strip=False)


# A seeded random corpus and random patterns: any drop is a lost match.
ALPHABET = list("שלומהכ") + [" ", ",", "[", "]", "ָ", "."]
ATOMS = ["ש", "ל", "ו", "ם", "ה", "מ", "כ", ".", "[לו]", r"\s", " ", ","]


def _random_pattern(rnd, depth=0):
    parts = []
    for _ in range(rnd.randint(1, 4)):
        r = rnd.random()
        if r < 0.12 and depth < 2:
            parts.append("(" + _random_pattern(rnd, depth + 1) + "|" + _random_pattern(rnd, depth + 1) + ")")
        elif r < 0.2:
            parts.append(rnd.choice([r"\b", "^", "$"]))
        else:
            parts.append(rnd.choice(ATOMS) + rnd.choice(["", "", "", "?", "*", "+", "{2}"]))
    return "".join(parts)


def test_random_patterns_never_lose_a_match():
    rnd = random.Random(20260928)
    docs = {f"d{i}" + ("_run" if i % 2 else ""):
            "".join(rnd.choice(ALPHABET) for _ in range(rnd.randint(3, 25)))
            for i in range(300)}
    eng = _engine(main=_index(docs, _main_schema()))
    lost = []
    for _ in range(150):
        p = _random_pattern(rnd)
        try:
            re.compile(p)
        except re.error:
            continue
        found = {r["uid"] for r in eng.execute_search(p, "Regex", 0, corpus_scope="genizah")}
        missing = _expected(p, docs, strip=True) - found
        if missing:
            lost.append((p, len(missing)))
    assert not lost, f"{len(lost)} patterns lost matches, e.g. {lost[:5]}"


def _cut_off(eng, pattern, **kwargs):
    se.consume_last_search_cutoff()
    rows = eng.execute_search(pattern, "Regex", 0, corpus_scope="genizah", **kwargs)
    return rows, se.consume_last_search_cutoff()


def test_cut_off_is_reported(monkeypatch):
    # Five pages match; the engine reads 2 candidates. The count must say it stopped
    # early ("N+"), and the complete set (ids_only, what search-within completes a
    # step with) holds all five.
    docs = {f"p{i}": f"שלו{c} ועוד" for i, c in enumerate("םהתנא")}
    eng = _engine(main=_index(docs, _main_schema()))
    monkeypatch.setattr(Config, "SEARCH_LIMIT", 2)
    rows, cut = _cut_off(eng, "שלו.")
    assert len(rows) == 2 and cut["capped"] is True
    rows, cut = _cut_off(eng, "שלו.", ids_only=True)
    assert {r["uid"] for r in rows} == set(docs) and cut["capped"] is False


def test_complete_scan_reports_no_cut_off():
    eng = _engine(main=_index(DOCS, _main_schema()))
    _rows, cut = _cut_off(eng, "שלו.")
    assert cut["capped"] is False


def test_candidate_query_is_selective(main_engine):
    # Guards the tests above against a prefilter that silently widened to
    # "every document": 'שלו.' needs a token containing ש-ל-ו (brackets
    # allowed between letters), which 6 of the 13 pages have.
    from shared.regex_prefilter import build_candidate_query
    query, _formula = build_candidate_query("שלו.", main_engine.index, stripped=True)
    assert query is not None
    assert main_engine.searcher.search(query, 100).count == 6


@pytest.mark.parametrize("tokenizer, expected", [
    ("hebword", "hebword"), ("whitespace", "unsplit"), ("raw", None), ("default", None)])
def test_content_tokenizer_is_recognised(tokenizer, expected):
    from shared import regex_prefilter
    b = tantivy.SchemaBuilder()
    b.add_text_field("content", stored=True, tokenizer_name=tokenizer)
    idx = tantivy.Index(b.build())
    register_search_tokenizers(idx)
    assert regex_prefilter.tokenizer_kind(idx) == expected


def test_regex_mode_has_no_query_string_form():
    # The lossy whole-word AND string must not come back through another caller.
    with pytest.raises(ValueError):
        SearchEngine.build_tantivy_query(None, ["שלום|שדה"], "Regex")


def test_regex_with_a_text_position_keeps_its_candidates():
    # A position search reads its candidates from the content field too (the
    # position is checked on each candidate): the first word המלך holds the
    # pattern's required run מלך inside a longer token.
    docs = {"starts": "המלך אמר לעבדיו", "later": "אמר המלך לעבדיו", "none": "טקסט אחר"}
    eng = _engine(main=_index(docs, _main_schema()))
    found = {r["uid"] for r in eng.execute_search(
        "ה?מלך", "Regex", 0, corpus_scope="genizah", text_position="start")}
    assert found == {"starts"}

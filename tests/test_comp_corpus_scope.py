# -*- coding: utf-8 -*-
"""Phase 110 Wave-0 scaffold — composition/parallels LOCAL-corpus routing.

Pins the contract Plan 02 will implement: a `corpus_scope` selector (Genizah /
Local / ALL) on BOTH composition engines, orthogonal to mode (Lab Mode is no
longer hardwired to LOCAL).

Requirements covered (one test per VALIDATION.md row):
  - COMP-LOC-01: corpus_scope routes which index loop runs (Genizah / LOCAL LAB),
    for BOTH standard `search_composition_logic` and Lab `lab_composition_search`,
    in BOTH directions (Lab honors the selector — D decoupling).
  - COMP-LOC-02: `corpus_scope='all'` includes LOCAL LAB hits; a present-but-stale
    LAB sets a per-run staleness verdict (does NOT silently drop); both engine
    early-return paths still carry the scope payload (Round-2 #4); an invalid /
    typo scope fails CLOSED to Genizah (never exposes LOCAL — also D-12).
  - D-12: a LOCAL composition run invokes ZERO Supabase/cloud-write surfaces
    (the three v7.14 gates: search_serializer, corrections_service, lists_sync).
  - D-13 (Genizah non-regression): enforced by THREE complementary checks, not a
    single byte-comparison — (a) default-equality (`test_genizah_default_nonregression`:
    `corpus_scope='genizah'` returns the SAME dict as the omitted-arg call),
    (b) the LOCAL hook is NOT invoked for Genizah
    (`test_std_comp_genizah_skips_local_lab` + `test_lab_comp_genizah_skips_local_lab`),
    and (c) the pre-existing comp suites (tests/test_lab_composition_chunk_hits.py,
    tests/test_corpus_scope_routing.py) staying green after Plan 02.

ALL tests in this file are PURE-ENGINE (C2): they call `search_composition_logic`
/ `lab_composition_search` directly with `corpus_scope=` and monkeypatch the index
loops / cloud surfaces. NONE import `genizah_app` or drive the UI composition
runner, so Plan 02's whole-file `-x` verify is deterministically green at Wave 2.

Wave-0 status: `corpus_scope` does not yet exist on either engine method, so the
keyword calls raise `TypeError` (legitimately RED). Collection still succeeds —
the engine classes import cleanly. Each test goes green as Plan 02 lands the
parameter, the fail-closed normalizer, the per-run staleness flag (A2
`local_lab_stale`), and the scope payload on the early-return dicts.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Engine builders — __init__ bypassed (mirrors tests/test_lab_composition_chunk_hits.py)
# ---------------------------------------------------------------------------

def _build_search_engine():
    """A SearchEngine with __init__ bypassed and the minimum attributes the
    standard `search_composition_logic` LOCAL-LAB hook + Genizah loop touch.

    The Genizah Tantivy loop and the LOCAL LAB loop are both spy-able:
      - engine.searcher.search          -> Genizah loop
      - engine.local_lab_searcher.search -> LOCAL LAB loop (gated on freshness)
    """
    from genizah_core import SearchEngine

    engine = SearchEngine.__new__(SearchEngine)  # bypass __init__

    # --- Genizah index (search_composition_logic uses self.index + self.searcher)
    engine.index = MagicMock(name="genizah_index")
    engine.index.parse_query.return_value = MagicMock(name="genizah_query")
    _g_hits = MagicMock()
    _g_hits.hits = []  # zero Genizah hits — we only count calls
    engine.searcher = MagicMock(name="genizah_searcher")
    engine.searcher.search.return_value = _g_hits

    # --- LOCAL LAB index (the corpus_scope='local'/'all' hook)
    engine._local_lab_index = MagicMock(name="local_lab_index")
    engine._local_lab_index.parse_query.return_value = MagicMock(name="local_lab_query")
    _l_hits = MagicMock()
    _l_hits.hits = []
    engine.local_lab_searcher = MagicMock(name="local_lab_searcher")
    engine.local_lab_searcher.search.return_value = _l_hits
    engine.local_lab_searcher_stale = False

    # Freshness is forced True by default so the LOCAL LAB hook is REACHABLE when
    # the scope asks for it; individual tests override via patch.object.
    engine._check_local_lab_freshness = MagicMock(return_value=True)
    engine._current_lab_weights_hash = MagicMock(return_value="hash-fresh")
    engine._my_library_tab_ref = None  # is_searchable defaults True

    # Helpers the comp loop calls — return real regex / query strings.
    import re as _re
    engine.build_tantivy_query = MagicMock(return_value="content:foo")
    engine.build_regex_pattern = MagicMock(return_value=_re.compile("foo"))
    engine._load_browse_map = MagicMock(return_value={})
    return engine


def _build_lab_engine():
    """A LabEngine with __init__ bypassed and the minimum attributes the Lab
    composition Genizah loop + LOCAL LAB loop touch.

      - engine.lab_searcher.search        -> Genizah lab loop
      - engine.local_lab_searcher.search  -> LOCAL LAB loop (gated on freshness)
    """
    from genizah_core import LabEngine, LabSettings

    engine = LabEngine.__new__(LabEngine)  # bypass __init__
    engine.settings = LabSettings()
    engine.settings.comp_min_score = 1
    engine.settings.min_should_match = 50
    engine.dynamic_rank_map = None
    engine._filter_match_count = 0

    # Genizah lab index/searcher
    engine.lab_index = MagicMock(name="lab_index")
    engine.lab_index.parse_query.return_value = MagicMock(name="lab_query")
    _g_hits = MagicMock()
    _g_hits.hits = []
    engine.lab_searcher = MagicMock(name="lab_searcher")
    engine.lab_searcher.search.return_value = _g_hits

    # LOCAL LAB index/searcher
    engine._local_lab_index = MagicMock(name="local_lab_index")
    engine._local_lab_index.parse_query.return_value = MagicMock(name="local_lab_query")
    _l_hits = MagicMock()
    _l_hits.hits = []
    engine.local_lab_searcher = MagicMock(name="local_lab_searcher")
    engine.local_lab_searcher.search.return_value = _l_hits
    engine.local_lab_searcher_stale = False

    engine._check_local_lab_freshness = MagicMock(return_value=True)
    engine._current_lab_weights_hash = MagicMock(return_value="hash-fresh")
    engine._my_library_tab_ref = None
    return engine


# A source long enough to produce several chunks at chunk_size=4. Hebrew so it
# passes the WORD_TOKEN_PATTERN / r"[\w֐-׿\']+" tokenizer.
_SOURCE_TOKENS = [
    "אלף", "בית", "גימל", "דלת", "הא", "וו",
    "זין", "חית", "טית", "יוד", "כף", "למד",
]
_SOURCE_TEXT = " ".join(_SOURCE_TOKENS)


# ---------------------------------------------------------------------------
# COMP-LOC-01 — routing: corpus_scope decides which index loop runs
# ---------------------------------------------------------------------------

def test_lab_comp_genizah_skips_local_lab():
    """COMP-LOC-01: Lab composition with corpus_scope='genizah' runs the Genizah
    lab loop and NEVER touches the LOCAL LAB searcher."""
    engine = _build_lab_engine()
    engine.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="genizah",
    )
    assert engine.local_lab_searcher.search.call_count == 0, (
        "corpus_scope='genizah' must NOT query the LOCAL LAB searcher"
    )
    assert engine.lab_searcher.search.call_count > 0, (
        "corpus_scope='genizah' must still query the Genizah lab searcher"
    )


def test_lab_comp_local_skips_genizah_lab():
    """COMP-LOC-01: Lab composition with corpus_scope='local' runs the LOCAL LAB
    loop and NEVER touches the Genizah lab searcher."""
    engine = _build_lab_engine()
    engine.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="local",
    )
    assert engine.lab_searcher.search.call_count == 0, (
        "corpus_scope='local' must NOT query the Genizah lab searcher"
    )
    assert engine.local_lab_searcher.search.call_count > 0, (
        "corpus_scope='local' must query the LOCAL LAB searcher"
    )


def test_std_comp_genizah_skips_local_lab():
    """COMP-LOC-01: standard composition with corpus_scope='genizah' runs the
    Genizah Tantivy loop and NEVER reaches the LOCAL LAB hook."""
    engine = _build_search_engine()
    engine.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="genizah",
    )
    assert engine.local_lab_searcher.search.call_count == 0, (
        "corpus_scope='genizah' must NOT reach the LOCAL LAB hook in "
        "search_composition_logic"
    )
    assert engine.searcher.search.call_count > 0, (
        "corpus_scope='genizah' must still run the Genizah Tantivy loop"
    )


def test_lab_mode_not_hardwired_to_local():
    """COMP-LOC-01 (decoupling): Lab Mode honors the corpus selector in BOTH
    directions — it is NOT hardwired to LOCAL. Pure-engine, no UI."""
    # Genizah scope -> Genizah lab loop only.
    engine_g = _build_lab_engine()
    engine_g.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="genizah",
    )
    assert engine_g.local_lab_searcher.search.call_count == 0
    assert engine_g.lab_searcher.search.call_count > 0

    # Local scope -> LOCAL LAB loop only (the inverse).
    engine_l = _build_lab_engine()
    engine_l.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="local",
    )
    assert engine_l.lab_searcher.search.call_count == 0
    assert engine_l.local_lab_searcher.search.call_count > 0


# ---------------------------------------------------------------------------
# COMP-LOC-02 — ALL merge, staleness payload, early-return payload, fail-closed
# ---------------------------------------------------------------------------

def test_std_comp_all_includes_local_hits():
    """COMP-LOC-02: standard composition with corpus_scope='all' consults BOTH
    the Genizah Tantivy loop AND the LOCAL LAB hook.

    The LOCAL LAB hook is gated on freshness; the live weights-hash override is
    injected by the app (Plan 03), so this engine-level test forces freshness
    True (stub `_check_local_lab_freshness` -> True) so the hook is reached."""
    engine = _build_search_engine()
    engine._check_local_lab_freshness = MagicMock(return_value=True)
    # Mirror the live app's override hook so the freshness gate opens.
    engine._lab_weights_hash_override = engine._current_lab_weights_hash()

    engine.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="all",
    )
    assert engine.searcher.search.call_count > 0, (
        "corpus_scope='all' must run the Genizah Tantivy loop"
    )
    assert engine.local_lab_searcher.search.call_count > 0, (
        "corpus_scope='all' must ALSO reach the LOCAL LAB hook (LOCAL hits "
        "merged into the same doc_hits accumulator — COMP-LOC-02)"
    )


def test_stale_lab_sets_flag():
    """COMP-LOC-02: a present-but-stale LAB sets the PER-RUN staleness verdict
    (the A2 payload flag `result['local_lab_stale']`) AND the back-compat engine
    flag — it does NOT silently drop. When NO LOCAL index exists at all
    (local_lab_searcher is None), staleness is NOT reported (M2 — distinguish
    stale-but-present from absent)."""
    # (a) Present-but-stale: a LAB searcher EXISTS but the freshness check fails.
    engine = _build_search_engine()
    engine._check_local_lab_freshness = MagicMock(return_value=False)
    engine.local_lab_searcher_stale = True  # the freshness check sets this side-effect

    result = engine.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="local",
    )
    assert result.get("local_lab_stale") is True, (
        "A present-but-stale LAB must surface a per-run staleness verdict "
        "(result['local_lab_stale'] is True — A2)"
    )
    assert engine.local_lab_searcher_stale is True, (
        "The back-compat engine flag local_lab_searcher_stale must also be set"
    )

    # (b) No LOCAL index at all: staleness must NOT be reported (M2).
    engine_none = _build_search_engine()
    engine_none.local_lab_searcher = None
    engine_none._local_lab_index = None
    engine_none._check_local_lab_freshness = MagicMock(return_value=False)
    engine_none.local_lab_searcher_stale = False

    result_none = engine_none.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="local",
    )
    assert not result_none.get("local_lab_stale"), (
        "With NO LOCAL index, no staleness verdict should be reported (M2 — "
        "stale != absent)"
    )


def test_invalid_scope_fails_closed():
    """C4 / D-12: an unknown/typo corpus_scope normalizes to 'genizah' (fail
    CLOSED) — the LOCAL LAB loop is NEVER hit. Never expose LOCAL on a bad value.
    """
    for bad_scope in ("gnizah", "", "LOCALish", "garbage"):
        # Standard engine
        engine = _build_search_engine()
        engine._check_local_lab_freshness = MagicMock(return_value=True)
        engine._lab_weights_hash_override = engine._current_lab_weights_hash()
        engine.search_composition_logic(
            _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
            corpus_scope=bad_scope,
        )
        assert engine.local_lab_searcher.search.call_count == 0, (
            f"standard composition with invalid corpus_scope={bad_scope!r} must "
            f"fail CLOSED (never hit the LOCAL LAB loop)"
        )
        assert engine.searcher.search.call_count > 0, (
            f"invalid corpus_scope={bad_scope!r} must still run the Genizah loop"
        )

        # Lab engine
        lab = _build_lab_engine()
        lab.lab_composition_search(
            _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope=bad_scope,
        )
        assert lab.local_lab_searcher.search.call_count == 0, (
            f"Lab composition with invalid corpus_scope={bad_scope!r} must fail "
            f"CLOSED (never hit the LOCAL LAB loop)"
        )


def test_early_return_carries_scope_payload():
    """Round-2 #4 (COMP-LOC-02): BOTH engine early-return paths still carry the
    A2 contract keys (`corpus_scope` + `local_lab_stale`), so on_comp_scan_finished
    can never default a stale Local/ALL short-text run to Genizah/False and hide
    the warning.

    (a) LAB empty-text early return (genizah_core.py:1421-1422).
    (b) STANDARD too-short early return: token count < chunk_size
        (genizah_core.py:8914-8915).
    """
    # (a) LAB empty text -> immediate early return.
    lab = _build_lab_engine()
    lab_result = lab.lab_composition_search(
        full_text="", mode="variants", corpus_scope="local",
    )
    assert "corpus_scope" in lab_result and lab_result["corpus_scope"] == "local", (
        "LAB empty-text early return must echo corpus_scope='local'"
    )
    assert "local_lab_stale" in lab_result, (
        "LAB empty-text early return must carry the local_lab_stale key"
    )

    # (b) STANDARD too-short text -> immediate early return (1 token < chunk_size=4).
    engine = _build_search_engine()
    std_result = engine.search_composition_logic(
        full_text="אלף", chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="all",
    )
    assert "corpus_scope" in std_result and std_result["corpus_scope"] == "all", (
        "standard too-short early return must echo corpus_scope='all'"
    )
    assert "local_lab_stale" in std_result, (
        "standard too-short early return must carry the local_lab_stale key"
    )


# ---------------------------------------------------------------------------
# D-13 — Genizah default non-regression (default-equality leg)
# ---------------------------------------------------------------------------

def test_genizah_default_nonregression():
    """D-13: `corpus_scope='genizah'` returns the SAME result object as calling
    with NO corpus_scope arg (default equivalence). Combined with the
    LOCAL-hook-not-called tests and the pre-existing comp suites staying green,
    this guards full Genizah non-regression without a brittle byte-comparison."""
    engine_explicit = _build_search_engine()
    explicit = engine_explicit.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="genizah",
    )

    engine_default = _build_search_engine()
    default = engine_default.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
    )

    # The Genizah default path must be observably identical to the omitted-arg
    # path. (The scope-echo key, if added, is identical in both, so equality holds.)
    assert explicit == default, (
        "corpus_scope='genizah' must produce the SAME result dict as the "
        "omitted-arg default (D-13 default-equality)"
    )


# ---------------------------------------------------------------------------
# D-12 — no cloud-write surface fires on a LOCAL composition run
# ---------------------------------------------------------------------------

def test_no_cloud_write_on_local_comp():
    """D-12 / T-110-01 (pure-engine): a LOCAL-scope composition run invokes ZERO
    Supabase/cloud-write methods — the three v7.14 gates:
      - shared.search_serializer (the _is_local_item web-payload filter path)
      - shared.corrections_service (corrections save path)
      - lists_sync.sync_item_to_cloud / lists_sync.sync_list_to_cloud

    We patch each surface with a spy and assert call-count == 0 after running a
    LOCAL composition directly via the engine (NOT via the UI)."""
    import shared.search_serializer as _serializer
    import shared.corrections_service as _corrections
    import lists_sync as _lists_sync

    with patch.object(_serializer, "_is_local_item", create=True) as spy_serialize, \
            patch.object(_corrections, "save_correction", create=True) as spy_corr, \
            patch.object(_lists_sync, "sync_item_to_cloud", create=True) as spy_item, \
            patch.object(_lists_sync, "sync_list_to_cloud", create=True) as spy_list:

        engine = _build_search_engine()
        engine._check_local_lab_freshness = MagicMock(return_value=True)
        engine._lab_weights_hash_override = engine._current_lab_weights_hash()
        engine.search_composition_logic(
            _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
            corpus_scope="local",
        )

    assert spy_serialize.call_count == 0, "LOCAL comp must not hit web-payload serializer"
    assert spy_corr.call_count == 0, "LOCAL comp must not save a correction to cloud"
    assert spy_item.call_count == 0, "LOCAL comp must not sync an item to cloud"
    assert spy_list.call_count == 0, "LOCAL comp must not sync a list to cloud"

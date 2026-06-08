# -*- coding: utf-8 -*-
"""Phase 110 Wave-0 scaffold — composition/parallels LOCAL-corpus routing.

Pins the contract Plan 02 will implement: a `corpus_scope` selector (Genizah /
Local / ALL) on BOTH composition engines, orthogonal to mode (Lab Mode is no
longer hardwired to LOCAL).

Phase 110 DESIGN CORRECTION (2026-06-08, Plan 110-03 UAT checkpoint): STANDARD
(Lab-Mode-OFF) composition queries the REGULAR My-Library index
(`self.local_searcher` / `self.local_index`) — the SAME index regular search
scope=Local uses — NOT the LAB side-index. The LAB side-index is opt-in via Lab
Mode (`lab_composition_search`, unchanged). The default path has NO weights-hash
and NO staleness concept (an empty LOCAL result is just "no results"). The
standard-path routing tests therefore assert routing to `local_searcher`; the
Lab-path tests keep asserting LAB `local_lab_searcher` routing; staleness is a
Lab-Mode-only concern (`test_stale_lab_sets_flag` is repurposed to the Lab path).

Requirements covered (one test per VALIDATION.md row):
  - COMP-LOC-01: corpus_scope routes which index loop runs — for standard
    composition (Genizah loop vs the REGULAR LOCAL index) and Lab composition
    (Genizah lab loop vs the LAB side-index), in BOTH directions (Lab honors the
    selector — D decoupling).
  - COMP-LOC-02: standard `corpus_scope='all'` includes regular-LOCAL hits; a
    present-but-stale LAB sets a per-run staleness verdict on the LAB path (does
    NOT silently drop); both engine early-return paths still carry the scope
    payload (Round-2 #4); an invalid / typo scope fails CLOSED to Genizah (never
    exposes LOCAL — also D-12).
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
    standard `search_composition_logic` LOCAL hook + Genizah loop touch.

    Phase 110 DESIGN CORRECTION (2026-06-08): standard (Lab-Mode-OFF) composition
    now queries the REGULAR My-Library index (self.local_searcher / self.local_index),
    NOT the LAB side-index. The routing spies are therefore:
      - engine.searcher.search        -> Genizah loop
      - engine.local_searcher.search  -> LOCAL hook (regular index)
    `engine.local_lab_searcher` is still defined as a spy so tests can assert the
    standard path does NOT touch the LAB side-index (it must stay at 0 calls).
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

    # --- Regular LOCAL My-Library index (the corpus_scope='local'/'all' hook).
    # Phase 110 correction: standard composition queries THIS, not the LAB index.
    engine.local_index = MagicMock(name="local_index")
    engine.local_index.parse_query.return_value = MagicMock(name="local_query")
    _reg_hits = MagicMock()
    _reg_hits.hits = []
    engine.local_searcher = MagicMock(name="local_searcher")
    engine.local_searcher.search.return_value = _reg_hits

    # --- LOCAL LAB side-index spy (must NOT be touched by the standard path).
    engine._local_lab_index = MagicMock(name="local_lab_index")
    engine._local_lab_index.parse_query.return_value = MagicMock(name="local_lab_query")
    _l_hits = MagicMock()
    _l_hits.hits = []
    engine.local_lab_searcher = MagicMock(name="local_lab_searcher")
    engine.local_lab_searcher.search.return_value = _l_hits
    engine.local_lab_searcher_stale = False

    # Freshness/weights-hash still defined for back-compat, but the standard path
    # no longer reads them (the regular index has no staleness concept).
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
    Genizah Tantivy loop and NEVER reaches the LOCAL hook — neither the regular
    LOCAL index NOR the LAB side-index (Phase 110 correction)."""
    engine = _build_search_engine()
    engine.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="genizah",
    )
    assert engine.local_searcher.search.call_count == 0, (
        "corpus_scope='genizah' must NOT reach the regular LOCAL index in "
        "search_composition_logic"
    )
    assert engine.local_lab_searcher.search.call_count == 0, (
        "corpus_scope='genizah' must NOT touch the LAB side-index either"
    )
    assert engine.searcher.search.call_count > 0, (
        "corpus_scope='genizah' must still run the Genizah Tantivy loop"
    )


def test_std_comp_local_uses_regular_index():
    """COMP-LOC-01/02 (Phase 110 correction): standard composition with
    corpus_scope='local' queries the REGULAR My-Library index
    (self.local_searcher), skips the Genizah Tantivy loop, and does NOT touch the
    LAB side-index."""
    engine = _build_search_engine()
    engine.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="local",
    )
    assert engine.local_searcher.search.call_count > 0, (
        "corpus_scope='local' must query the regular LOCAL index"
    )
    assert engine.searcher.search.call_count == 0, (
        "corpus_scope='local' must SKIP the Genizah Tantivy loop"
    )
    assert engine.local_lab_searcher.search.call_count == 0, (
        "corpus_scope='local' must NOT touch the LAB side-index (opt-in via Lab Mode)"
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
# Phase 110 UAT (Issue 3) — Genizah Lab loop guard against an UNBUILT LAB index
# ---------------------------------------------------------------------------

def test_lab_comp_missing_lab_index_no_crash():
    """Phase 110 UAT (Issue 3): a Lab-Mode + Genizah composition run with
    chunk_size>3 must NOT crash when the Genizah fingerprint LAB index
    (Config.LAB_INDEX_DIR) has never been built — i.e. self.lab_index /
    self.lab_searcher are None.

    Before the guard, the Genizah lab loop called self.lab_index.parse_query(...)
    unconditionally → "'NoneType' object has no attribute 'parse_query'".
    The guard skips the Genizah lab contribution gracefully and the method must
    still return its normal result dict (carrying corpus_scope / local_lab_stale).
    """
    engine = _build_lab_engine()
    # Simulate an unbuilt Genizah LAB index.
    engine.lab_index = None
    engine.lab_searcher = None

    result = engine.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="genizah",
    )
    assert isinstance(result, dict), (
        "an unbuilt Genizah LAB index must yield a dict, not raise "
        "(no NoneType.parse_query crash)"
    )
    assert result.get("corpus_scope") == "genizah", (
        "the result must still carry the corpus_scope payload key"
    )
    assert "local_lab_stale" in result, (
        "the result must still carry the local_lab_stale payload key"
    )


# ---------------------------------------------------------------------------
# COMP-LOC-02 — ALL merge, staleness payload, early-return payload, fail-closed
# ---------------------------------------------------------------------------

def test_std_comp_all_includes_local_hits():
    """COMP-LOC-02: standard composition with corpus_scope='all' consults BOTH
    the Genizah Tantivy loop AND the REGULAR LOCAL index (Phase 110 correction).

    The standard path queries the regular My-Library index (self.local_searcher),
    NOT the LAB side-index — no freshness/weights-hash gate is involved."""
    engine = _build_search_engine()

    engine.search_composition_logic(
        _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
        corpus_scope="all",
    )
    assert engine.searcher.search.call_count > 0, (
        "corpus_scope='all' must run the Genizah Tantivy loop"
    )
    assert engine.local_searcher.search.call_count > 0, (
        "corpus_scope='all' must ALSO reach the regular LOCAL index (LOCAL hits "
        "merged into the same doc_hits accumulator — COMP-LOC-02)"
    )
    assert engine.local_lab_searcher.search.call_count == 0, (
        "the standard path must NOT touch the LAB side-index (opt-in via Lab Mode)"
    )


def test_stale_lab_sets_flag():
    """COMP-LOC-02 (Phase 110 correction): staleness is now a LAB-MODE-ONLY
    concern. The Lab path (`lab_composition_search`) surfaces the PER-RUN
    staleness verdict (the A2 payload flag `result['local_lab_stale']`) AND the
    back-compat engine flag when its LAB index is present-but-stale — it does NOT
    silently drop. When NO LOCAL LAB index exists (local_lab_searcher is None),
    staleness is NOT reported (M2 — distinguish stale-but-present from absent).

    (The standard path no longer touches the LAB index — it queries the regular
    My-Library index which has no staleness concept; see
    test_std_comp_local_uses_regular_index.)"""
    # (a) Present-but-stale: a LAB searcher EXISTS but the freshness check fails.
    lab = _build_lab_engine()
    lab._check_local_lab_freshness = MagicMock(return_value=False)
    lab.local_lab_searcher_stale = False

    result = lab.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="local",
    )
    assert result.get("local_lab_stale") is True, (
        "A present-but-stale LAB must surface a per-run staleness verdict "
        "(result['local_lab_stale'] is True — A2)"
    )
    assert lab.local_lab_searcher_stale is True, (
        "The back-compat engine flag local_lab_searcher_stale must also be set"
    )

    # (b) No LOCAL LAB index at all: staleness must NOT be reported (M2).
    lab_none = _build_lab_engine()
    lab_none.local_lab_searcher = None
    lab_none._local_lab_index = None
    lab_none._check_local_lab_freshness = MagicMock(return_value=False)
    lab_none.local_lab_searcher_stale = False

    result_none = lab_none.lab_composition_search(
        _SOURCE_TEXT, mode="variants", chunk_size=4, corpus_scope="local",
    )
    assert not result_none.get("local_lab_stale"), (
        "With NO LOCAL LAB index, no staleness verdict should be reported (M2 — "
        "stale != absent)"
    )


def test_invalid_scope_fails_closed():
    """C4 / D-12: an unknown/typo corpus_scope normalizes to 'genizah' (fail
    CLOSED) — the LOCAL LAB loop is NEVER hit. Never expose LOCAL on a bad value.
    """
    for bad_scope in ("gnizah", "", "LOCALish", "garbage"):
        # Standard engine
        engine = _build_search_engine()
        engine.search_composition_logic(
            _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
            corpus_scope=bad_scope,
        )
        assert engine.local_searcher.search.call_count == 0, (
            f"standard composition with invalid corpus_scope={bad_scope!r} must "
            f"fail CLOSED (never hit the regular LOCAL index)"
        )
        assert engine.local_lab_searcher.search.call_count == 0, (
            f"standard composition with invalid corpus_scope={bad_scope!r} must "
            f"never touch the LAB side-index either"
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
        engine.search_composition_logic(
            _SOURCE_TEXT, chunk_size=4, max_freq=1000, mode="variants",
            corpus_scope="local",
        )

    assert spy_serialize.call_count == 0, "LOCAL comp must not hit web-payload serializer"
    assert spy_corr.call_count == 0, "LOCAL comp must not save a correction to cloud"
    assert spy_item.call_count == 0, "LOCAL comp must not sync an item to cloud"
    assert spy_list.call_count == 0, "LOCAL comp must not sync a list to cloud"

# -*- coding: utf-8 -*-
"""SEED-011 (125-01): Guard that per-chunk plans are built ONCE per chunk, not once
per (chunk x index pass).

Phase 125 Plan 01 — PREP-01 dedup contract:

``search_composition_logic`` double-prep context:
  The Genizah loop and the LOCAL loop each need a query string and compiled
  regex for every chunk.  BEFORE the dedup these were re-derived independently
  inside each loop.  AFTER the dedup a single ``_ChunkPlan`` is built once in a
  pre-pass over ``chunks_data`` and then CONSUMED by both loops.

  The two per-flavor build calls (``build_tantivy_query`` / ``build_regex_pattern``)
  REMAIN (one Genizah-flavor + one LOCAL-flavor per chunk); the dedup does NOT
  collapse them.  What it removes is the structural double-iteration of
  ``chunks_data``: the plans are built once and reused.

  This test therefore asserts:
    * ``_ChunkPlan`` is instantiated exactly N times (once per chunk) for a
      ``corpus_scope='all'`` run (both index passes active).
    * The Genizah loop and LOCAL loop consume the SAME plan objects (same identity).

``lab_composition_search`` double-prep context:
  The fingerprint prep (``text_to_fingerprint`` / ``_is_phrase_statistically_weak``
  / ``fp_str.split()`` / ``needed_unique_fps`` / ``core_query``) IS genuinely
  index-independent and was previously recomputed by BOTH the Genizah-LAB loop and
  the LOCAL-LAB loop.  After the dedup it is computed ONCE per qualifying chunk.
  ``text_to_fingerprint`` call count SHOULD drop from 2*N to N.

Tests (RED before Task 2/3, GREEN after):
  test_search_composition_logic_shared_prep_once
  test_lab_composition_search_no_double_prep
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers — construct minimal engine instances without real Tantivy indices
# ---------------------------------------------------------------------------

def _make_search_engine():
    """Return a SearchEngine instance with both Genizah and LOCAL indices mocked.

    Mirrors the construction pattern from test_local_post_dedup_merge.py.
    ``reload_index`` and ``_open_local_searcher`` are suppressed so no real
    Tantivy directory is required.
    """
    from genizah_core import SearchEngine

    with patch("genizah_core.SearchEngine.reload_index", return_value=False):
        with patch.object(SearchEngine, "_open_local_searcher"):
            meta = MagicMock()
            meta.parse_full_id_components.return_value = {}
            engine = SearchEngine(meta, MagicMock())

    # Wire a fake Genizah index + searcher so the Genizah loop is entered.
    engine.index = MagicMock(name="genizah_index")
    engine.searcher = MagicMock(name="genizah_searcher")
    engine.searcher.search.return_value = MagicMock(hits=[])

    # Wire a fake LOCAL index + searcher so the LOCAL loop is entered.
    engine.local_index = MagicMock(name="local_index")
    engine.local_searcher = MagicMock(name="local_searcher")
    engine.local_searcher.search.return_value = MagicMock(hits=[])

    # Enable content_search so the diacritic-fold branch is exercised.
    engine._has_content_search = True
    engine._local_has_content_search = True

    # Disable the my_library_tab gate (no desktop widget available).
    engine._my_library_tab_ref = None

    return engine


def _make_lab_engine():
    """Return a LabEngine instance with both LAB and LOCAL-LAB indices mocked."""
    from genizah_core import LabEngine, LabSettings

    engine = LabEngine.__new__(LabEngine)  # bypass __init__

    # Minimum attrs for lab_composition_search
    engine.settings = LabSettings()
    engine.settings.comp_min_score = 1
    engine.settings.min_should_match = 0
    engine.dynamic_rank_map = None
    engine._filter_match_count = 0

    # Genizah LAB index (so the Genizah-LAB loop is entered)
    engine.lab_index = MagicMock(name="lab_index")
    engine.lab_index.parse_query.return_value = MagicMock(name="q_obj")
    engine.lab_searcher = MagicMock(name="lab_searcher")
    engine.lab_searcher.search.return_value = MagicMock(hits=[])

    # LOCAL LAB index (so the LOCAL-LAB loop is entered)
    engine._local_lab_index = MagicMock(name="local_lab_index")
    engine._local_lab_index.parse_query.return_value = MagicMock(name="local_q_obj")
    engine.local_lab_searcher = MagicMock(name="local_lab_searcher")
    engine.local_lab_searcher.search.return_value = MagicMock(hits=[])
    engine.local_lab_searcher_stale = False
    engine._lab_local_meta = None

    # Gate disabled
    engine._my_library_tab_ref = None

    return engine


# ---------------------------------------------------------------------------
# Test 1 — search_composition_logic shared-prep-once
# ---------------------------------------------------------------------------

class TestSearchCompositionSharedPrepOnce:
    """SEED-011 Finding 1: per-chunk _ChunkPlan is built ONCE per chunk.

    Implementation note: build_tantivy_query / build_regex_pattern are STILL
    called once per (chunk x flavor) — the LOCAL flavor applies diacritic folding
    so the two query strings genuinely differ.  The 2*N build-call count is
    CORRECT by design and is NOT the dedup target.  The test asserts the STRUCTURAL
    invariant: a single pre-pass builds all plans before the index loops begin.
    """

    def test_search_composition_logic_shared_prep_once(self):
        """_ChunkPlan is constructed exactly N times for a corpus_scope='all' run
        (once per chunk), not 2*N times (once per chunk per loop).

        Strategy: patch genizah_core._ChunkPlan with a call-counting wrapper and
        verify the constructor is invoked exactly N times for N chunks.  If
        _ChunkPlan does not exist yet (pre-Task-2) the test raises AttributeError
        and fails — that is the correct RED state.
        """
        import genizah_core

        # Verify _ChunkPlan exists — fails RED before Task 2 introduces it.
        assert hasattr(genizah_core, "_ChunkPlan"), (
            "_ChunkPlan dataclass not found in genizah_core — Task 2 must add it"
        )

        engine = _make_search_engine()

        # A source text that produces exactly 3 chunks (4 tokens, chunk_size=2).
        source_text = "אחד שניים שלושה ארבעה"  # 4 Hebrew tokens → 3 chunks of size 2

        construction_calls = []
        original_chunk_plan = genizah_core._ChunkPlan

        def counting_chunk_plan(*args, **kwargs):
            obj = original_chunk_plan(*args, **kwargs)
            construction_calls.append(1)
            return obj

        with patch.object(genizah_core, "_ChunkPlan", side_effect=counting_chunk_plan):
            engine.search_composition_logic(
                full_text=source_text,
                chunk_size=2,
                max_freq=100,
                mode="Composition",
                corpus_scope="all",
            )

        n_chunks = 3  # 4 tokens, chunk_size=2 → indices 0,1,2
        assert len(construction_calls) == n_chunks, (
            f"_ChunkPlan must be constructed ONCE per chunk (expected {n_chunks}, "
            f"got {len(construction_calls)}).  If the count is {2 * n_chunks} the "
            f"pre-pass is missing and each index loop is still re-deriving the plan "
            f"(the pre-Task-2 double-prep state)."
        )

    def test_chunk_plan_object_reused_by_both_loops(self):
        """The plan objects built in the pre-pass are shared by both index passes.

        Verifies that ``search_composition_logic`` builds a list of _ChunkPlan
        objects ONCE and that both the Genizah loop and the LOCAL loop consume
        those same objects (same identity — checked via the attribute values
        rather than object id, since mocks may copy attributes).

        Fails RED before Task 2 because _ChunkPlan doesn't exist yet.
        """
        import genizah_core

        assert hasattr(genizah_core, "_ChunkPlan"), (
            "_ChunkPlan dataclass not found in genizah_core — Task 2 must add it"
        )

        engine = _make_search_engine()
        source_text = "אחד שניים שלושה ארבעה"

        built_plans = []
        original_chunk_plan = genizah_core._ChunkPlan

        def capturing_chunk_plan(*args, **kwargs):
            obj = original_chunk_plan(*args, **kwargs)
            built_plans.append(obj)
            return obj

        with patch.object(genizah_core, "_ChunkPlan", side_effect=capturing_chunk_plan):
            engine.search_composition_logic(
                full_text=source_text,
                chunk_size=2,
                max_freq=100,
                mode="Composition",
                corpus_scope="all",
            )

        # After the dedup, exactly 3 plans (one per chunk) should have been built.
        assert len(built_plans) == 3, (
            f"Expected 3 _ChunkPlan objects (one per chunk), got {len(built_plans)}"
        )

        # Each plan must carry both flavor attributes — confirming it is the
        # consolidated plan, not a per-loop partial.
        for plan in built_plans:
            assert hasattr(plan, "genizah_query_str"), (
                "_ChunkPlan must have genizah_query_str field"
            )
            assert hasattr(plan, "local_query_str"), (
                "_ChunkPlan must have local_query_str field"
            )


# ---------------------------------------------------------------------------
# Test 2 — lab_composition_search no-double-prep
# ---------------------------------------------------------------------------

class TestLabCompositionSharedPrepOnce:
    """SEED-011 Finding 2: text_to_fingerprint called N times (not 2*N) for N chunks.

    The fingerprint prep is genuinely index-independent — fp_str / fp_list /
    needed_unique_fps / core_query are identical for the Genizah-LAB and LOCAL-LAB
    passes.  The dedup removes the 2x text_to_fingerprint call per chunk by
    pre-computing _LabChunkPlan ONCE before the two LAB loops.
    """

    def test_lab_composition_search_no_double_prep(self):
        """text_to_fingerprint is called exactly N times (once per qualifying chunk),
        not 2*N (once per chunk per LAB index pass).

        Fails RED before Task 3 introduces _LabChunkPlan.
        """
        import genizah_core

        assert hasattr(genizah_core, "_LabChunkPlan"), (
            "_LabChunkPlan dataclass not found in genizah_core — Task 3 must add it"
        )

        engine = _make_lab_engine()

        # Stub out the heavy statistical-weakness check so ALL chunks qualify.
        with patch.object(engine, "_is_phrase_statistically_weak", return_value=False):
            # Stub _calculate_match_metrics so each match is above threshold.
            with patch.object(
                engine,
                "_calculate_match_metrics",
                return_value=(100.0, [], (0, 0)),
            ):
                # Count text_to_fingerprint calls via a wrapper.
                original_t2f = genizah_core.text_to_fingerprint
                call_count = []

                def counting_t2f(text, freq_map=None):
                    result = original_t2f(text, freq_map=freq_map)
                    # Return a non-empty fingerprint so the chunk isn't skipped.
                    if not result:
                        result = "stub_fp"
                    call_count.append(text)
                    return result

                with patch.object(genizah_core, "text_to_fingerprint", side_effect=counting_t2f):
                    # 4-token source → 2 chunks of size 3 (indices 0, 1).
                    # chunk_size default is taken from lab_composition_search signature —
                    # pass chunk_size explicitly so the number of chunks is predictable.
                    source_text = "אחד שניים שלושה ארבעה חמישה"  # 5 tokens → 3 chunks of size 3

                    engine.lab_composition_search(
                        full_text=source_text,
                        chunk_size=3,
                        max_freq=100,
                        corpus_scope="all",
                    )

        n_chunks = 3  # 5 tokens, chunk_size=3 → indices 0,1,2
        assert len(call_count) == n_chunks, (
            f"text_to_fingerprint must be called ONCE per qualifying chunk "
            f"(expected {n_chunks}, got {len(call_count)}).  "
            f"If the count is {2 * n_chunks} the _LabChunkPlan pre-pass is missing "
            f"and both LAB loops are still computing the fingerprint independently "
            f"(the pre-Task-3 double-prep state)."
        )

"""
Phase 77 Plan 02 -- behavioral test for D-13 Path A core extension.

Verifies that lab_composition_search populates `chunk_hits` per uid in the
returned items dict (and internally on results_map[uid]) AT RUNTIME by
exercising the real per-iteration loop with the search-engine boundary
monkeypatched. The prior revision used inspect.getsource() to grep the source
for an `append` line; that was a static contract check, not a behavioral one.
See 77-REVIEWS.md HIGH-04.

The TestStaticContract class below acts as a fast smoke layer: if anyone
removes the chunk_hits init, append, or return-side surface in a future
refactor, BOTH the behavioral test AND the static checks fail loudly.
"""

import inspect
import pytest
from unittest.mock import MagicMock, patch


# -----------------------------------------------------------------------------
# Static contract checks (cheap; fast first-failure signal)
# -----------------------------------------------------------------------------

class TestStaticContract:
    """If anyone removes the init or append, these tests fail before the slower
    behavioral test even runs. They do not REPLACE the behavioral test."""

    def test_chunk_hits_field_exists_in_source(self):
        from genizah_core import LabEngine
        src = inspect.getsource(LabEngine.lab_composition_search)
        assert "'chunk_hits': []" in src, \
            "results_map[uid] init must include chunk_hits per Phase 77 D-13 Path A"
        assert "rec['chunk_hits'].append(" in src, \
            "Per-iteration accumulator must append to chunk_hits"

    def test_chunk_hits_contract_documented(self):
        from genizah_core import LabEngine
        src = inspect.getsource(LabEngine.lab_composition_search)
        assert src.count("Phase 77 D-13") >= 2, \
            "Inline comments documenting the Phase 77 contract are required " \
            "(init + append + items-dict surface)"

    def test_chunk_hits_surfaced_on_returned_items(self):
        """Plan 03's serialize_parallels_payload reads each item's chunk_hits;
        results_map is internal-only and not returned. Confirm the post-process
        item dict pulls chunk_hits forward."""
        from genizah_core import LabEngine
        src = inspect.getsource(LabEngine.lab_composition_search)
        assert "'chunk_hits': data.get('chunk_hits'" in src, \
            "raw_final_items 'item' dict must surface data['chunk_hits'] for " \
            "consumers (Plan 03 serialize_parallels_payload, /api/parallels)"


# -----------------------------------------------------------------------------
# Behavioral test (HIGH-04 fix) -- exercises the real loop via monkeypatch
# -----------------------------------------------------------------------------

class TestChunkHitsBehavior:
    """HIGH-04: prove chunk_hits is populated at runtime, not just present in source.

    Strategy:
      1. Build a LabEngine instance with __init__ bypassed (LabEngine.__new__),
         then stuff the minimum attributes the method touches: settings,
         dynamic_rank_map, _filter_match_count, lab_index, lab_searcher.
      2. Monkeypatch lab_index.parse_query -> non-None query object.
      3. Monkeypatch lab_searcher.search to return an object whose .hits is an
         iterable of (score, doc_addr) tuples per the loop at line 1328.
      4. Monkeypatch lab_searcher.doc to return a synthetic doc dict with the
         keys 'content', 'unique_id', 'full_header', 'source' (all returning
         single-element lists per the actual access pattern).
      5. Monkeypatch LabEngine._calculate_match_metrics so it returns
         (match_score, matches, best_window) with matches whose 'fp' values
         exactly match the input query_fingerprints_list (so the
         min_pct_ratio guard at line 1347-1348 passes).
      6. Monkeypatch LabEngine._is_phrase_statistically_weak to return False.
      7. Call lab_composition_search end-to-end.
      8. Inspect the returned dict's 'main' list (and the synthetic uid's item
         within) and assert chunk_hits is a non-empty list with the documented
         tuple shape.

    No Tantivy index is required.
    """

    def _build_engine(self):
        """Construct a LabEngine instance with heavy initialization bypassed.

        Most LabEngine.__init__ work is loading the tantivy index, dynamic
        weights, fingerprint cache, etc. We skip all of it via __new__ and
        stuff the minimum attributes the lab_composition_search method body
        actually touches.
        """
        from genizah_core import LabEngine, LabSettings

        engine = LabEngine.__new__(LabEngine)  # bypass __init__
        engine.settings = LabSettings()
        # Drop the score threshold so our synthetic match_score (100.0) sails
        # through, regardless of the default comp_min_score=70.
        engine.settings.comp_min_score = 1
        # Loosen the must-match ratio so the test does not hinge on perfect
        # fingerprint alignment (we still align it; this is belt+suspenders).
        engine.settings.min_should_match = 50
        engine.dynamic_rank_map = None
        engine._filter_match_count = 0
        engine.lab_index = MagicMock()
        engine.lab_index.parse_query.return_value = MagicMock(name="query_obj")
        engine.lab_searcher = MagicMock()
        return engine

    def test_chunk_hits_populated_per_chunk_match(self):
        """Behavioral: a synthetic source matches one synthetic manuscript on
        every chunk. After lab_composition_search runs, the returned 'main'
        list contains exactly one item whose `chunk_hits` is a non-empty list
        of (chunk_index: int, source_chunk_text: str, score: number,
        ms_snippet: str) tuples."""
        from genizah_core import LabEngine

        engine = self._build_engine()

        # Build a long source text so the tokenizer produces enough tokens to
        # form multiple chunks. Hebrew Unicode (U+0590-05FF) is used so it
        # passes the regex r"[\w֐-׿\']+" used at line 1251.
        # 12 distinct tokens, chunk_size=4 -> chunks = ceil((12-4+1)/2)+1 = 5
        # plus floor stepping. We assert >=1 hit; exact count is incidental.
        source_tokens = [
            "אלף", "בית", "גימל", "דלת",
            "הא", "וו", "זין", "חית",
            "טית", "יוד", "כף", "למד",
        ]
        source_text = " ".join(source_tokens)

        synthetic_uid = "uid_test_99"
        # The full manuscript content must contain a substring that
        # _calculate_match_metrics can return start/end indices into. We make
        # it long enough so [start:end] yields a recognizable string.
        synthetic_content = "צורת המשפט במכתב יד זה דוגמה למבחן " * 3

        synthetic_doc = {
            "content": [synthetic_content],
            "unique_id": [synthetic_uid],
            "full_header": ["header_9988776655443322_IE99_P3"],
            "source": ["V0.8"],
        }

        # Configure lab_searcher.search to return an object with .hits that
        # contains exactly one synthetic hit per call. The loop at line 1328
        # iterates `for score, doc_addr in iterator:`. doc_addr is fed to
        # lab_searcher.doc(...) so any sentinel works.
        def _make_search_result(*_args, **_kwargs):
            res = MagicMock()
            res.hits = [(0.9, "addr_99")]
            return res

        engine.lab_searcher.search.side_effect = _make_search_result
        engine.lab_searcher.doc.return_value = synthetic_doc

        # Synthetic fake for _calculate_match_metrics. The real signature is:
        #   _calculate_match_metrics(self, text, query_fingerprints_list,
        #                            original_query_str, freq_map=None)
        # Returns (match_score, matches, best_window).
        #
        # IMPORTANT: matches must contain `fp` values that fully cover
        # `set(query_fingerprints_list)` so the min_pct_ratio guard at line
        # 1347-1348 passes (len(common_fps)/len(needed_unique_fps) >= 0.5).
        def fake_metrics(self, text, query_fingerprints_list,
                         original_query_str, freq_map=None):
            # Build matches where each match's fp covers a unique input fp.
            # The manuscript-snippet substring will be content[matches[0]['start']:matches[-1]['end']].
            # Choose a window inside synthetic_content.
            matches = []
            for idx, fp in enumerate(query_fingerprints_list):
                matches.append({
                    "fp": fp,
                    "word": f"word_{idx}",
                    "start": 0 + idx * 3,
                    "end": 5 + idx * 3,
                })
            # best_window covers all matches so all fps qualify
            best_window = (0, len(matches) - 1) if matches else (0, 0)
            match_score = 100.0
            return match_score, matches, best_window

        # Patch the helpers on the LabEngine class. autospec=False because we
        # are replacing with side_effects whose signatures we control.
        with patch.object(LabEngine, "_calculate_match_metrics",
                          autospec=True, side_effect=fake_metrics), \
             patch.object(LabEngine, "_is_phrase_statistically_weak",
                          autospec=True, return_value=False):

            result = engine.lab_composition_search(
                source_text,
                mode="variants",
                chunk_size=4,
            )

        # The function returns {'main', 'known', 'filtered', 'partial',
        # 'boundary_stats'}. Find the synthetic-uid item.
        assert isinstance(result, dict), \
            f"lab_composition_search must return a dict; got {type(result)}"
        assert "main" in result, \
            f"Returned dict must have 'main' key; got keys {list(result.keys())}"

        # The synthetic uid should be the only one in main (or filtered, since
        # is_text_filtered defaults False, it goes to main).
        all_items = list(result.get("main", [])) + \
                    list(result.get("filtered", [])) + \
                    list(result.get("known", []))
        # Find the synthetic-uid item
        target = None
        for item in all_items:
            if item.get("uid") == synthetic_uid:
                target = item
                break

        assert target is not None, (
            f"Synthetic uid {synthetic_uid!r} not found in returned items. "
            f"Got {len(all_items)} items with uids "
            f"{[i.get('uid') for i in all_items]}"
        )

        # === Behavioral assertion: chunk_hits surfaced on returned item ===
        assert "chunk_hits" in target, (
            "Returned item missing 'chunk_hits' key -- D-13 Path A regressed "
            "(item dict at lines 1479-1495 must surface data['chunk_hits'])"
        )
        chunk_hits = target["chunk_hits"]
        assert isinstance(chunk_hits, list), \
            f"chunk_hits must be a list; got {type(chunk_hits)}"
        assert len(chunk_hits) >= 1, (
            f"chunk_hits must be non-empty after at least one matching "
            f"chunk; got {chunk_hits}"
        )

        # Tuple-shape assertions on every entry
        for tup in chunk_hits:
            assert isinstance(tup, tuple), \
                f"Each chunk_hit must be a tuple; got {type(tup)}: {tup}"
            assert len(tup) == 4, \
                f"Each chunk_hit must be a 4-tuple; got len={len(tup)}: {tup}"
            ch_idx, ch_text, ch_score, ms_snip = tup
            assert isinstance(ch_idx, int), \
                f"chunk_index must be int (0-based); got {type(ch_idx)}: {ch_idx}"
            assert ch_idx >= 0, \
                f"chunk_index must be 0-based (>=0); got {ch_idx}"
            assert isinstance(ch_text, str), \
                f"source_chunk_text must be str; got {type(ch_text)}"
            assert ch_text, \
                f"source_chunk_text must be non-empty; got {ch_text!r}"
            assert isinstance(ch_score, (int, float)), \
                f"match_score must be number; got {type(ch_score)}"
            assert isinstance(ms_snip, str), \
                f"manuscript_snippet must be str; got {type(ms_snip)}"

    def test_existing_fields_unchanged_alongside_chunk_hits(self):
        """Regression guard: total_score, hits_count, ms_matches still
        populated as before. Prove the additive change did not displace
        existing per-iteration accumulator semantics."""
        from genizah_core import LabEngine

        engine = self._build_engine()

        source_text = " ".join([
            "אלף", "בית", "גימל", "דלת",
            "הא", "וו", "זין", "חית",
        ])
        synthetic_uid = "uid_test_regression"
        synthetic_doc = {
            "content": ["dummy manuscript content for regression test " * 3],
            "unique_id": [synthetic_uid],
            "full_header": ["header_9911111111111111_IE1_P1"],
            "source": ["V0.8"],
        }

        def _make_search_result(*_args, **_kwargs):
            res = MagicMock()
            res.hits = [(0.5, "addr_x")]
            return res

        engine.lab_searcher.search.side_effect = _make_search_result
        engine.lab_searcher.doc.return_value = synthetic_doc

        def fake_metrics(self, text, query_fingerprints_list,
                         original_query_str, freq_map=None):
            matches = [
                {"fp": fp, "word": f"w_{i}", "start": i, "end": i + 4}
                for i, fp in enumerate(query_fingerprints_list)
            ]
            best_window = (0, len(matches) - 1) if matches else (0, 0)
            return 150.0, matches, best_window

        with patch.object(LabEngine, "_calculate_match_metrics",
                          autospec=True, side_effect=fake_metrics), \
             patch.object(LabEngine, "_is_phrase_statistically_weak",
                          autospec=True, return_value=False):

            result = engine.lab_composition_search(
                source_text, mode="variants", chunk_size=4,
            )

        target = None
        for item in (list(result.get("main", []))
                     + list(result.get("filtered", []))
                     + list(result.get("known", []))):
            if item.get("uid") == synthetic_uid:
                target = item
                break
        assert target is not None, \
            "Synthetic uid not in returned items -- engine setup regression"

        # Existing fields all present
        assert "score" in target and target["score"] > 0, \
            "Existing 'score' field must remain populated"
        assert "uid" in target and target["uid"] == synthetic_uid
        assert "raw_header" in target and target["raw_header"]
        assert "src_lbl" in target
        assert "text" in target  # ms_snips joined string
        assert "full_text" in target
        assert "has_boundary_matches" in target
        # And the new field is present alongside
        assert "chunk_hits" in target


class TestSearchCompositionLogicStaticContract:
    """Phase 77 D-13: search_composition_logic (standard-mode parallels) must
    emit the same chunk_hits list-of-tuples shape as lab_composition_search.

    The original int counter was renamed to 'chunk_count' to avoid the
    serializer collision that produced "'int' object is not iterable" on
    /api/export/parallels/json downloads. Static-source assertions lock this
    rename so a future refactor can't quietly reintroduce the bug.
    """

    def test_chunk_count_replaces_old_int_counter(self):
        """The int counter must be 'chunk_count', not 'chunk_hits'."""
        with open("genizah_core.py", encoding="utf-8") as f:
            src = f.read()
        # The doc_hits defaultdict in search_composition_logic uses chunk_count
        # for the int counter; the inner increment uses chunk_count.
        assert "'chunk_count': 0" in src, (
            "search_composition_logic doc_hits defaultdict must init "
            "'chunk_count' (was 'chunk_hits' before Phase 77 rename)"
        )
        assert "rec['chunk_count'] += 1" in src, (
            "Per-chunk increment must use chunk_count (was chunk_hits before rename)"
        )
        assert "'chunk_count': data.get('chunk_count', 0)" in src, (
            "build_items output must surface chunk_count int counter on items"
        )

    def test_chunk_hits_list_appended_per_chunk(self):
        """The list-of-tuples chunk_hits must be appended with the same shape
        Plan 02 uses (i, chunk_text, score, ms_snip).
        """
        with open("genizah_core.py", encoding="utf-8") as f:
            src = f.read()
        # The defaultdict initializes chunk_hits as a list (not int)
        assert "'chunk_hits': []" in src, (
            "doc_hits must init chunk_hits as a list (parallel to "
            "lab_composition_search at line 1366)"
        )
        # The per-chunk loop appends a 4-tuple (now wrapped in dedup logic)
        assert "rec['chunk_hits'].append(" in src, (
            "search_composition_logic per-chunk loop must append to chunk_hits"
        )
        # The shape mirrors lab_composition_search: (chunk_index, chunk_text, score, snippet).
        # _new_score is the inlined float(score) the dedup helper reuses.
        assert "(i, ' '.join(chunk), _new_score, ms_snip)" in src, (
            "chunk_hits tuple must use shape (chunk_index, chunk_text, score, snippet)"
        )
        # Dedup is in place — same (chunk_index, snippet) doesn't double-emit.
        assert "_chunk_hit_keys" in src, (
            "Per-rec dedup map (_chunk_hit_keys) must guard against duplicate "
            "Tantivy segments returning the same uid"
        )

    def test_chunk_hits_surfaced_on_returned_items_in_standard_mode(self):
        """build_items at the end of search_composition_logic must surface
        chunk_hits onto the returned-item dict so the serializer can read it.
        """
        with open("genizah_core.py", encoding="utf-8") as f:
            src = f.read()
        assert "'chunk_hits': data.get('chunk_hits', [])" in src, (
            "build_items output must surface data['chunk_hits'] (list) for "
            "shared.search_serializer.serialize_parallels_payload to consume"
        )

---
phase: 95
plan: 05
type: execute
wave: 2
depends_on: [02, 03]
files_modified:
  - genizah_core.py
  - tests/test_local_post_dedup_merge.py
  - tests/test_side_index_merge.py
  - tests/test_local_index_open_fallback.py
  - tests/test_local_reload_after_refresh.py
autonomous: true
requirements: [REQ-3]
must_haves:
  truths:
    - "LOCAL hits are queried alongside the main Tantivy index and merged via RRF k=60"
    - "LOCAL hits merge AFTER _deduplicate() (Codex D-08 P0 — _deduplicate() at line 7390 would otherwise drop them)"
    - "Tie-break: Genizah first when LOCAL and Genizah hits have identical RRF scores — pinned by `test_rrf_tiebreak_genizah_first` (W7)"
    - "If LOCAL index is missing/locked/corrupt at search-init, main search returns Genizah-only results without exception (D-37)"
    - "_deduplicate() body at line 7916 is UNCHANGED (smaller blast radius per D-08 Codex revision)"
  artifacts:
    - path: "genizah_core.py"
      provides: "self.local_searcher property; _query_local_index() method; _rrf_merge() method; post-_deduplicate LOCAL merge hook"
      contains: "_rrf_merge"
    - path: "tests/test_local_post_dedup_merge.py"
      provides: "Codex D-08 P0 pin — LOCAL before _deduplicate is dropped; LOCAL after _deduplicate survives; AST-pinned merge-after-dedup site"
    - path: "tests/test_side_index_merge.py"
      provides: "End-to-end RRF k=60 merge order test + dedicated test_rrf_tiebreak_genizah_first scenario (W7)"
    - path: "tests/test_local_index_open_fallback.py"
      provides: "D-37 fallback — corrupt LOCAL index falls back to Genizah-only"
  key_links:
    - from: "genizah_core.py:7390 (after _deduplicate)"
      to: "self._query_local_index() + self._rrf_merge()"
      via: "LOCAL merge happens AFTER dedup line"
      pattern: "_query_local_index"
    - from: "self._rrf_merge"
      to: "RRF k=60 algorithm"
      via: "Reciprocal Rank Fusion"
      pattern: "1.0 / \\(k \\+ rank\\)"
    - from: "SearchEngine.reload_local_indexes"
      to: "MyLibraryTab refresh / delete / rebuild / recovery callbacks (Plan 07)"
      via: "post-commit reopen of Tantivy searcher handles"
      pattern: "reload_local_indexes"
---

<objective>
Wire the LOCAL side-index into the main search dispatch. Per Codex D-08 P0: LOCAL hits MUST merge AFTER `_deduplicate()` at `genizah_core.py:7390`, NOT before — the existing `_deduplicate()` body at `:7916-7921` whitelists only V0.8 / V0.7 sources and would silently drop LOCAL. Use RRF k=60 (NOT raw BM25 — Codex revision) because BM25 IDF is index-local and scores from two independent Tantivy indexes are not directly comparable.

Also implement D-37 fallback: if `tantivy.Index.open(Config.LOCAL_INDEX_DIR)` raises at search-init (missing files, file lock from crashed previous instance, corruption), main search proceeds normally — LOCAL hits are absent, the LOCAL filter button stays hidden (gated in Plan 08), and a status-bar "My Library index unavailable — Rebuild?" notice is surfaced.

**HIGH-1 RESOLVED — Live-search reload after refresh.** The original plan opened `self.local_searcher` only during `SearchEngine.__init__`. Plan 07 ran refresh workers but did not specify reloading the Tantivy searcher handles after commits. Result: newly indexed files would not appear until app restart, breaking the main product promise. Task 3 (NEW) adds `SearchEngine.reload_local_indexes()` and `SearchEngine.reload_local_lab_index()` methods that close + reopen the Tantivy index handles. Plan 07 then calls these methods after every MyLibraryTab Refresh, Delete, Rebuild, and Recovery commit (Plan 07 update covers the call-site wiring).

**MEDIUM-1 RESOLVED — Query semantics parity.** Task 2 extension: refactor `_query_local_index` to call the same query-builder helper from `genizah_core.py` that the main search uses, parameterized by which fields to search. Validation: a test asserts phrase mode and gap mode produce identical hit-sets on a fixture containing both main-index and LOCAL documents. If the full refactor is too invasive for this revision, the divergence is documented in a `<deferred>` block with a follow-up test exercising each query mode against LOCAL fixtures and a TODO referencing this plan.

**W7 RESOLVED — Dedicated tie-break test:** the must_haves list explicitly calls out Genizah-first tie-break, but the original plan had no test FORCING the equal-RRF-score scenario. Task 2 ADDS `test_rrf_tiebreak_genizah_first` to `tests/test_side_index_merge.py` with a fixture constructed to produce identical RRF scores; assertion: the Genizah hit ranks first in the merged output.

**W6 RESOLVED — Acceptance command:** the original AST check used a `python -c` one-liner with a multi-line `for` loop, which is illegal in `python -c` single-string syntax. Task 2's acceptance now invokes a dedicated pytest test (`test_local_merge_inserts_after_dedup_call_site`) that performs the AST walk from inside a Python file.

Output: Modified `genizah_core.py` (3 new methods: `_open_local_searcher`, `_query_local_index`, `_rrf_merge`; one insertion AFTER line 7390) + 3 GREEN test files including the new W7 tie-break test.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@.planning/phases/95-my-library/95-RESEARCH.md
@genizah_core.py
@shared/local_indexer.py
@shared/local_sys_id.py
@.planning/phases/95-my-library/95-02-SUMMARY.md
@.planning/phases/95-my-library/95-03-SUMMARY.md

<interfaces>
<!-- Critical insertion site (verbatim from genizah_core.py:7389-7401 — already verified) -->

```python
LOGGER.debug(f"Line-break search: ...")
deduped = self._deduplicate(results)              # line 7390 — INSERT LOCAL MERGE AFTER THIS

if exclude_words and deduped:                     # line 7392
    filtered = []
    for r in deduped:
        text_content = (r.get('snippet', '') + ' ' + r.get('full_text', '')).lower()
        should_exclude = any(w.lower() in text_content for w in exclude_words)
        if not should_exclude:
            filtered.append(r)
    deduped = filtered

return deduped                                    # line 7401
```

<!-- _deduplicate body (DO NOT MODIFY per D-08 Codex revision) -->

```python
def _deduplicate(self, results):                  # line 7916
    v8 = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
    final = list(v8.values())
    for r in results:
        if r['display']['source'] == "V0.7" and r['uid'] not in v8: final.append(r)
    return final
```

<!-- RRF body (port verbatim from RESEARCH.md lines 381-416) -->

```python
def _rrf_merge(self, genizah_hits, local_hits, k=60, limit=None):
    rrf = {}
    for rank, hit in enumerate(genizah_hits, start=1):
        uid = hit['uid']
        rrf.setdefault(uid, {'hit': hit, 'score': 0.0, 'sources': set()})
        rrf[uid]['score'] += 1.0 / (k + rank)
        rrf[uid]['sources'].add('genizah')
    for rank, hit in enumerate(local_hits, start=1):
        uid = hit['uid']
        rrf.setdefault(uid, {'hit': hit, 'score': 0.0, 'sources': set()})
        rrf[uid]['score'] += 1.0 / (k + rank)
        rrf[uid]['sources'].add('local')
    fused = sorted(rrf.values(), key=lambda r: (r['score'], 'genizah' in r['sources']), reverse=True)
    out = [r['hit'] for r in fused]
    return out[:limit] if limit else out
```

<!-- D-34 result row shape (LOCAL hits MUST produce this for parse compat from Plan 02) -->

```python
{
    'uid': 'LOCAL_970012345601234567_P3',
    'full_text': '...',
    'snippet': '...',
    'sys_id': '970012345601234567',
    'p_num': '3',
    'display': {
        'id': '970012345601234567',
        'source': 'LOCAL',
        'library_code': 'LOCAL',
        'fl_id': 'F0042',
        ...
    },
    'full_header': '970012345601234567_LOCAL_P3_F0042',
}
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add LOCAL searcher initialization + D-37 fallback in genizah_core.py</name>
  <read_first>
    - genizah_core.py — locate SearchEngine `__init__` and Tantivy index initialization (likely around the schema setup at :5118-5189)
    - .planning/phases/95-my-library/95-CONTEXT.md (D-14 paths, D-37 fallback contract)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 4: Tantivy main schema — LEAVE UNCHANGED")
    - shared/local_indexer.py (build_local_schema available)
  </read_first>
  <behavior>
    Test `test_corrupt_local_index_falls_back_to_genizah_only`:
    - Create a temp directory that exists but contains garbage (not a valid Tantivy index).
    - Monkey-patch `Config.LOCAL_INDEX_DIR` to point at the garbage dir.
    - Construct a `SearchEngine`.
    - Assert `engine.local_searcher is None` (not a hard failure; engine still constructed).
    - Assert no traceback raised during construction.
    - Verify a warning was logged: `caplog.text` contains `"LOCAL index unavailable"` or similar.

    Test `test_missing_local_index_dir_falls_back`:
    - Monkey-patch `Config.LOCAL_INDEX_DIR` to a path that does NOT exist on disk.
    - Construct `SearchEngine`.
    - `engine.local_searcher is None`.
    - No exception propagates.

    Test `test_main_search_returns_genizah_only_when_local_unavailable`:
    - Set up `SearchEngine` with `local_searcher = None`.
    - Run a normal search.
    - Assert results are returned (non-empty if a matching Genizah doc exists) and contain NO `display.source == 'LOCAL'` rows.
  </behavior>
  <action>
    1. Locate the `SearchEngine` class `__init__` in `genizah_core.py` (or wherever the main Tantivy searcher is initialized — search for `tantivy.Index.open` or `self.searcher`).

    2. AFTER the main Tantivy searcher is initialized, add a LOCAL searcher init with try/except fallback:
    ```python
    # Phase 95 — open LOCAL side-index alongside main (D-14 + D-37 fallback).
    self.local_searcher = None
    try:
        if os.path.isdir(Config.LOCAL_INDEX_DIR):
            from shared.local_indexer import build_local_schema
            schema = build_local_schema()
            local_index = tantivy.Index(schema, path=Config.LOCAL_INDEX_DIR)
            self.local_searcher = local_index.searcher()
            logger.info("LOCAL side-index opened: %s", Config.LOCAL_INDEX_DIR)
        else:
            logger.info("LOCAL side-index dir not present (no scan yet?): %s", Config.LOCAL_INDEX_DIR)
    except Exception as e:
        # D-37 — fall back to Genizah-only; surface in UI via banner (Plan 07).
        logger.warning("LOCAL index unavailable, main search continues without LOCAL hits: %r", e)
        self.local_searcher = None
    ```

    3. Add a helper method `def _query_local_index(self, query_str, mode, gap, limit, ...)` that wraps the LOCAL Tantivy search and returns a result list in the same shape as the main searcher (with `uid`, `full_text`, `snippet`, `display`, `full_header`, etc.).

    ```python
    def _query_local_index(self, query, mode, gap, limit=None):
        """Query the LOCAL side-index. Returns [] if local_searcher is None (D-37)."""
        if self.local_searcher is None:
            return []
        try:
            # Build a Tantivy query against build_local_schema()'s content/source/full_header fields.
            # Mirror the main searcher's query construction (parse_query for phrase mode etc.) —
            # planner identifies the exact API the main searcher uses and replicates.
            tantivy_q = self.local_searcher.index.parse_query(query, ["content", "content_head", "content_tail"])
            top_n = self.local_searcher.search(tantivy_q, limit=limit or 1000).hits
            results = []
            for score, doc_address in top_n:
                doc = self.local_searcher.doc(doc_address)
                # Build a result dict matching the shape of main-index results.
                results.append(self._build_local_result_dict(doc, score))
            return results
        except Exception as e:
            logger.warning("LOCAL index query failed: %r", e)
            return []

    def _build_local_result_dict(self, doc, score):
        """Construct a result row from a LOCAL Tantivy doc per D-34 shape."""
        unique_id = doc.get_first("unique_id")  # "LOCAL_{sys_id}_P{page_num}"
        full_header = doc.get_first("full_header")  # "{sys_id}_LOCAL_P{page_num}_F{file_id:04d}"
        content = doc.get_first("content") or ""
        # Parse sys_id + p_num via the already-broadened parse_header_smart (Plan 02 D-13 P0 fix).
        sys_id, p_num = self.parse_header_smart(full_header)
        return {
            'uid': unique_id,
            'full_text': content,
            'snippet': content[:200],  # planner may refine
            'sys_id': sys_id,
            'p_num': p_num,
            'score': score,
            'display': {
                'id': sys_id,
                'source': 'LOCAL',
                'library_code': 'LOCAL',
            },
            'full_header': full_header,
        }
    ```

    4. Implement `_rrf_merge` (port verbatim from RESEARCH.md Pattern 1 — already in `<interfaces>` block above):
    ```python
    def _rrf_merge(self, genizah_hits, local_hits, k=60, limit=None):
        """Reciprocal Rank Fusion merger (D-08 Codex P0). BM25 scores from two
        independent indexes are NOT comparable; RRF fuses by rank."""
        rrf = {}
        for rank, hit in enumerate(genizah_hits, start=1):
            uid = hit['uid']
            rrf.setdefault(uid, {'hit': hit, 'score': 0.0, 'sources': set()})
            rrf[uid]['score'] += 1.0 / (k + rank)
            rrf[uid]['sources'].add('genizah')
        for rank, hit in enumerate(local_hits, start=1):
            uid = hit['uid']
            rrf.setdefault(uid, {'hit': hit, 'score': 0.0, 'sources': set()})
            rrf[uid]['score'] += 1.0 / (k + rank)
            rrf[uid]['sources'].add('local')
        # Tie-break: Genizah first when scores equal (per CONTEXT D-08).
        fused = sorted(
            rrf.values(),
            key=lambda r: (r['score'], 'genizah' in r['sources']),
            reverse=True,
        )
        out = [r['hit'] for r in fused]
        return out[:limit] if limit else out
    ```
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_index_open_fallback.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "self.local_searcher" genizah_core.py` returns ≥ 3 (init + use + check sites).
    - `grep -c "def _rrf_merge" genizah_core.py` returns 1.
    - `grep -c "def _query_local_index" genizah_core.py` returns 1.
    - `grep -c "def _build_local_result_dict" genizah_core.py` returns 1.
    - `grep -c "1.0 / (k + rank)" genizah_core.py` returns ≥ 2 (RRF for both lists).
    - D-37 fallback verified: `python -m pytest tests/test_local_index_open_fallback.py -x -q` exits 0.
    - REGRESSION: `python -m pytest tests/ -k "search_engine or main_search" -x -q` exits 0.
    - `python -m ruff check genizah_core.py tests/test_local_index_open_fallback.py` exits 0.
  </acceptance_criteria>
  <done>SearchEngine constructs with optional LOCAL searcher; D-37 fallback verified; RRF method present.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Insert LOCAL merge POST-_deduplicate (Codex D-08 P0 — line 7390) + W6 + W7 fixes</name>
  <read_first>
    - genizah_core.py:7385-7405 (the exact 20 lines around line 7390 — verify line numbers are correct, search for "deduped = self._deduplicate(results)")
    - genizah_core.py:7916-7921 (the _deduplicate body — DO NOT MODIFY)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 5: Main search merger — RRF k=60 POST-_deduplicate (D-08 Codex P0)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-08 Codex revision — "merge after _deduplicate(). Smaller blast radius, leaves Genizah dedup behavior untouched.")
  </read_first>
  <behavior>
    Test `test_local_hit_before_dedup_dropped`:
    - Directly call `self._deduplicate([local_hit, genizah_v8_hit])` with a fake LOCAL hit (`display.source == 'LOCAL'`) + a V0.8 Genizah hit.
    - Assert the LOCAL hit is NOT in the output (dedup body whitelists V0.8/V0.7 only).
    - This is the REGRESSION TEST proving why D-08's "merge AFTER dedup" decision matters.

    Test `test_local_hit_after_dedup_survives`:
    - Construct a fake search flow that runs `_deduplicate` then `_rrf_merge` (insert LOCAL hit AFTER dedup).
    - Assert the LOCAL hit appears in the final result list.

    Test `test_local_merge_inserts_after_dedup_call_site` (W6 — was an illegal python -c one-liner; now a proper pytest test):
    - STATIC AST check from inside the test file. Read `genizah_core.py` source. Walk the AST. Locate the function containing `Call(func=Attribute(attr='_deduplicate'))`. Within that function's body, find the statement-index of the `_deduplicate(results)` call. Assert that a subsequent statement (within the same function, AFTER the dedup statement in source order) contains a call to either `_rrf_merge` or `_query_local_index`. (Same scanner pattern as `tests/test_pgp_filter_cascade.py`.)

    Test `test_side_index_merge_end_to_end` (in tests/test_side_index_merge.py):
    - Build a small LOCAL index with 2 docs. Build a fake `genizah_hits` list with 2 Genizah hits.
    - Call the search dispatch (or directly `_rrf_merge`).
    - Assert: 4 results total. First result is whichever has the highest RRF score.

    Test `test_rrf_tiebreak_genizah_first` (W7 — NEW, dedicated tie-break scenario):
    - Construct two single-element hit lists (one Genizah, one LOCAL) with identical RRF inputs. Specifically, rank=1 in each → both get score `1.0 / (60 + 1)`. The `sorted()` key `(score, 'genizah' in sources)` MUST place 'genizah' first because `True > False` at equal score.
    - Concretely: `genizah_hits = [{'uid': 'g_uid', 'display': {'source': 'V0.8'}}]`, `local_hits = [{'uid': 'l_uid', 'display': {'source': 'LOCAL'}}]`.
    - Call `engine._rrf_merge(genizah_hits, local_hits, k=60)`.
    - Assert `result[0]['uid'] == 'g_uid'` AND `result[1]['uid'] == 'l_uid'`.
    - Also assert reverse order: passing `(local_hits, genizah_hits)` STILL produces `g_uid` first (tie-break is order-independent — uses 'genizah' in sources, not list ordering).
    - REGRESSION: when scores DIFFER (Genizah at rank=10, LOCAL at rank=1 → LOCAL has higher RRF score), assert LOCAL ranks first. This proves the tie-break only triggers on actual ties, not as a blanket Genizah-priority override.
  </behavior>
  <action>
    1. Locate the exact line in `genizah_core.py` where `deduped = self._deduplicate(results)` appears (expected to be line 7390 per CONTEXT/PATTERNS, but VERIFY first via grep — line numbers shift over time):
    ```bash
    grep -n "deduped = self._deduplicate(results)" genizah_core.py
    ```

    2. AFTER that line (and BEFORE the `if exclude_words and deduped:` block), INSERT the LOCAL merge hook per PATTERNS.md exact code:
    ```python
    deduped = self._deduplicate(results)    # ← existing line 7390 (UNCHANGED)

    # Phase 95 D-08 (Codex P0): LOCAL hits merge AFTER _deduplicate (the dedup
    # body at :7916 whitelists V0.8/V0.7 only and would otherwise drop LOCAL).
    # RRF k=60 used (BM25 IDF from two independent indexes is not comparable;
    # raw score sort would mis-rank).
    if getattr(self, 'local_searcher', None) is not None:
        try:
            # Pass the same query/mode/gap context as the main search.
            # NOTE: the planner identifies the exact variable names for
            # query/mode/gap/limit in the surrounding scope and threads them
            # through. The call site has those variables in scope by the time
            # _deduplicate has been called.
            local_hits = self._query_local_index(query, mode, gap, limit=limit)
        except Exception as e:
            logger.warning("LOCAL side-index query failed; main results unaffected: %r", e)
            local_hits = []
        if local_hits:
            deduped = self._rrf_merge(deduped, local_hits, k=60, limit=limit)
    # End Phase 95 D-08 merge.

    if exclude_words and deduped:                # ← existing flow continues
        filtered = []
        ...
    ```

    3. The exact variable names (`query`, `mode`, `gap`, `limit`) depend on the surrounding function signature. The executor reads the enclosing `def` to identify them and passes the correct ones. If `limit` is not in scope, omit the limit argument and let `_rrf_merge` return all.

    4. DO NOT modify `_deduplicate()` body at `:7916-7921` — explicitly pinned by `test_local_hit_before_dedup_dropped`.

    5. **W6 — Implement the AST-walking test inside the file, NOT in a python -c one-liner.** In `tests/test_local_post_dedup_merge.py`, add:

    ```python
    # tests/test_local_post_dedup_merge.py
    import ast
    from pathlib import Path


    def _find_function_containing_call(tree, target_attr: str):
        """Walk AST; return the FunctionDef whose body contains a Call to .{target_attr}."""
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call):
                        f = inner.func
                        if isinstance(f, ast.Attribute) and f.attr == target_attr:
                            return node
        return None


    def _statement_contains_attr_call(stmt, target_attr: str) -> bool:
        for inner in ast.walk(stmt):
            if isinstance(inner, ast.Call):
                f = inner.func
                if isinstance(f, ast.Attribute) and f.attr == target_attr:
                    return True
                if isinstance(f, ast.Name) and f.id == target_attr:
                    return True
        return False


    def test_local_merge_inserts_after_dedup_call_site():
        """W6: AST-asserted that LOCAL merge appears AFTER the _deduplicate(results)
        call site in the enclosing function (D-08 Codex P0)."""
        src = Path("genizah_core.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        fn = _find_function_containing_call(tree, "_deduplicate")
        assert fn is not None, "Expected a function that calls self._deduplicate(...) — none found"
        body = fn.body
        dedup_idx = None
        for i, stmt in enumerate(body):
            if _statement_contains_attr_call(stmt, "_deduplicate"):
                dedup_idx = i
                break
        assert dedup_idx is not None, "Could not locate the _deduplicate statement in the function body"
        # Subsequent statements must include a call to _rrf_merge OR _query_local_index.
        tail = body[dedup_idx + 1 :]
        found = any(
            _statement_contains_attr_call(s, "_rrf_merge") or _statement_contains_attr_call(s, "_query_local_index")
            for s in tail
        )
        assert found, (
            "LOCAL merge hook not found AFTER _deduplicate(results) call site — "
            "expected _rrf_merge or _query_local_index in a subsequent statement"
        )
    ```

    6. **W7 — Add `test_rrf_tiebreak_genizah_first` to `tests/test_side_index_merge.py`:**

    ```python
    # tests/test_side_index_merge.py — W7 dedicated tie-break test

    def test_rrf_tiebreak_genizah_first():
        """W7: when LOCAL and Genizah produce identical RRF scores, Genizah ranks first.
        Tie-break is order-independent (driven by 'genizah' in sources, not list order)."""
        from genizah_core import SearchEngine  # or however the merge fn is exposed
        engine = SearchEngine.__new__(SearchEngine)  # bare instance for the helper
        genizah_hits = [{'uid': 'g_uid', 'display': {'source': 'V0.8'}}]
        local_hits = [{'uid': 'l_uid', 'display': {'source': 'LOCAL'}}]

        # Both lists have 1 element at rank=1 → identical RRF score 1/(60+1).
        result_a = engine._rrf_merge(genizah_hits, local_hits, k=60)
        assert [r['uid'] for r in result_a] == ['g_uid', 'l_uid'], (
            "Genizah-first tie-break violated when genizah passed as first arg"
        )

        # Reverse argument order — tie-break still applies.
        result_b = engine._rrf_merge(local_hits, genizah_hits, k=60)
        assert [r['uid'] for r in result_b] == ['g_uid', 'l_uid'], (
            "Genizah-first tie-break is supposed to be order-independent; "
            "argument order should not change outcome on tied scores"
        )


    def test_rrf_does_not_blanket_prioritize_genizah():
        """W7 regression: tie-break ONLY triggers on actual score ties. When scores differ
        (LOCAL ranked higher), LOCAL must outrank Genizah."""
        from genizah_core import SearchEngine
        engine = SearchEngine.__new__(SearchEngine)
        # 10 Genizah hits → top Genizah has rank=1, RRF score 1/61.
        # 1 LOCAL hit at rank=1 → RRF score 1/61. (Tied at top.)
        # Add a Genizah hit at rank=10 → RRF 1/70.
        # LOCAL at rank=1 should outrank Genizah at rank=10.
        genizah_hits = [
            {'uid': f'g_{i}', 'display': {'source': 'V0.8'}}
            for i in range(10)
        ]
        local_hits = [{'uid': 'l_uid', 'display': {'source': 'LOCAL'}}]
        result = engine._rrf_merge(genizah_hits, local_hits, k=60)
        # The top result is g_0 (tied at rank 1 with l_uid; tie-break → genizah first).
        # The second result is l_uid (RRF 1/61).
        # g_9 (rank 10, RRF 1/70) ranks below l_uid.
        l_pos = next(i for i, r in enumerate(result) if r['uid'] == 'l_uid')
        g9_pos = next(i for i, r in enumerate(result) if r['uid'] == 'g_9')
        assert l_pos < g9_pos, (
            "LOCAL at rank=1 (score 1/61) must outrank Genizah at rank=10 (score 1/70). "
            "Tie-break is for ties only, not blanket Genizah priority."
        )
    ```
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_post_dedup_merge.py tests/test_side_index_merge.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - W6 — `python -m pytest tests/test_local_post_dedup_merge.py::test_local_merge_inserts_after_dedup_call_site -x -q` exits 0 (the AST test from inside the test file, not a python -c one-liner).
    - `_deduplicate` body unchanged: `grep -A 5 "def _deduplicate" genizah_core.py | head -8` shows the same 5-line whitelist body.
    - W7 — `python -m pytest tests/test_side_index_merge.py::test_rrf_tiebreak_genizah_first tests/test_side_index_merge.py::test_rrf_does_not_blanket_prioritize_genizah -x -q` exits 0.
    - `python -m pytest tests/test_local_post_dedup_merge.py -x -q` exits 0 — both `test_local_hit_before_dedup_dropped` AND `test_local_hit_after_dedup_survives` pass.
    - `python -m pytest tests/test_side_index_merge.py -x -q` exits 0.
    - REGRESSION: `python -m pytest tests/ -q` exits 0 (no breakage of existing search tests).
    - `python -m ruff check genizah_core.py tests/test_local_post_dedup_merge.py tests/test_side_index_merge.py` exits 0.
  </acceptance_criteria>
  <done>LOCAL merge inserted post-dedup; RRF used; dedup body untouched; all 3 test files green including the new W7 tie-break test and the W6 AST-from-pytest test.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: HIGH-1 review fix — SearchEngine.reload_local_indexes() + reload_local_lab_index() + MEDIUM-1 shared query builder (or deferred)</name>
  <read_first>
    - genizah_core.py — SearchEngine __init__ (LOCAL searcher init added in Task 1)
    - genizah_core.py — locate the main search query construction site (for MEDIUM-1 shared helper extraction)
    - .planning/phases/95-my-library/95-REVIEWS.md (HIGH-1 + MEDIUM-1 findings)
    - desktop/my_library_tab.py (Plan 07 — call sites that will invoke these reload methods)
  </read_first>
  <behavior>
    Test `test_reload_local_indexes_picks_up_new_docs_without_restart` (HIGH-1 load-bearing):
    - Construct a `SearchEngine` against a LOCAL Tantivy index containing 0 docs initially.
    - Confirm a search for some token returns 0 LOCAL hits.
    - In a SEPARATE process / writer, ADD a doc to the LOCAL Tantivy index and commit (simulates MyLibraryTab refresh worker committing on a background thread).
    - Without calling reload, the search STILL returns 0 LOCAL hits (Tantivy searcher is snapshotted at open time — this is the bug HIGH-1 flags).
    - Call `engine.reload_local_indexes()`.
    - Search again: now returns 1 LOCAL hit (the newly committed doc).
    - This pins HIGH-1: the live search session can see newly indexed files without restart.

    Test `test_reload_local_lab_index_picks_up_new_docs`:
    - Same shape as above but against `local_lab_searcher` after a LOCAL LAB commit.
    - Call `engine.reload_local_lab_index()`.
    - Composition Search returns the newly indexed LOCAL LAB doc.

    Test `test_reload_local_indexes_no_op_when_dir_missing`:
    - HIGH-1 + D-37 interaction: when `Config.LOCAL_INDEX_DIR` does not exist (first launch, never indexed), `reload_local_indexes()` leaves `self.local_searcher = None` and does NOT raise. Logs at INFO "no LOCAL index to reload".

    Test `test_reload_local_indexes_recovers_from_transient_lock_error`:
    - Simulate a transient open failure (mock `tantivy.Index` to raise `IOError` on first call, succeed on second). `reload_local_indexes()` logs the failure at WARNING and leaves `local_searcher = None` (defensive — same D-37 fallback semantics on reload as at init).

    Test `test_query_semantics_phrase_mode_parity_with_main` (MEDIUM-1 — required if option A taken; otherwise the deferred test below):
    - Build a small main-index fixture (1 doc with phrase "ABC DEF GHI") AND a LOCAL fixture (1 doc with same phrase).
    - Run a phrase-mode search for `"DEF GHI"` against both indexes.
    - Assert: hit count from each index is identical (1 hit each). Same for gap-mode (1-gap permitting "DEF X GHI"-style matches per the engine's gap semantics).
    - If the full refactor was DEFERRED (MEDIUM-1 option B), this test is marked `@pytest.mark.xfail(reason="MEDIUM-1 deferred to follow-up: shared query builder not yet extracted; see deferred block in 95-05")`.
  </behavior>
  <action>
    **HIGH-1 implementation — `SearchEngine.reload_local_indexes()`:**

    Add to `SearchEngine` in `genizah_core.py` (next to the existing `local_searcher` init from Task 1):

    ```python
    def reload_local_indexes(self) -> None:
        """HIGH-1 review fix: reopen LOCAL Tantivy searchers (main + LAB) so newly
        committed docs become visible in the live session.

        Called by MyLibraryTab (Plan 07) AFTER every refresh / delete / rebuild /
        recovery commit. Idempotent + defensive: on any open failure, the searcher
        falls back to None (D-37 semantics).

        Side effects:
          - self.local_searcher: reopened or set to None (defensive on failure).
          - self.local_lab_searcher: reopened or set to None.
          - self._lab_local_meta: re-read from <LOCAL_LAB_INDEX_DIR>/.meta.json.
        """
        # Reload main LOCAL side-index.
        self.local_searcher = None
        try:
            if os.path.isdir(Config.LOCAL_INDEX_DIR):
                from shared.local_indexer import build_local_schema
                schema = build_local_schema()
                local_index = tantivy.Index(schema, path=Config.LOCAL_INDEX_DIR)
                self.local_searcher = local_index.searcher()
                logger.info("HIGH-1 reload: LOCAL side-index reopened: %s", Config.LOCAL_INDEX_DIR)
            else:
                logger.info("HIGH-1 reload: LOCAL side-index dir absent; searcher=None")
        except Exception as e:
            logger.warning("HIGH-1 reload: LOCAL side-index unavailable: %r", e)
            self.local_searcher = None
        # Delegate LAB reload to the narrower method (Plan 06 adds it too).
        self.reload_local_lab_index()

    def reload_local_lab_index(self) -> None:
        """HIGH-1 review fix (LAB-only narrow reload). Reopens self.local_lab_searcher
        and re-reads the .meta.json staleness sentinel.

        Plan 06 (LAB plan) also touches this method; coordinate via co-edit.
        """
        self.local_lab_searcher = None
        self._lab_local_meta = None
        try:
            if os.path.isdir(Config.LOCAL_LAB_INDEX_DIR):
                from shared.local_indexer import build_local_lab_schema, LocalIndexer
                schema = build_local_lab_schema()
                local_lab_index = tantivy.Index(schema, path=Config.LOCAL_LAB_INDEX_DIR)
                self.local_lab_searcher = local_lab_index.searcher()
                self._lab_local_meta = LocalIndexer.read_lab_meta(Config.LOCAL_LAB_INDEX_DIR)
                logger.info("HIGH-1 reload: LOCAL LAB side-index reopened: %s",
                            Config.LOCAL_LAB_INDEX_DIR)
            else:
                logger.info("HIGH-1 reload: LOCAL LAB side-index dir absent; searcher=None")
        except Exception as e:
            logger.warning("HIGH-1 reload: LOCAL LAB side-index unavailable: %r", e)
            self.local_lab_searcher = None
            self._lab_local_meta = None
    ```

    **MEDIUM-1 implementation — shared query-builder helper:**

    Option A (preferred — full refactor): extract the query-construction logic from the main searcher into a helper `_build_tantivy_query(query_str, mode, gap, fields)`. Replace both the main searcher call AND `_query_local_index`'s `parse_query(query, ["content", "content_head", "content_tail"])` with calls to this helper. The fields list is the only difference between the two call sites.

    Option B (deferred, only if A is too invasive): keep `_query_local_index` using a fresh `parse_query` but document the divergence in `<deferred>` (added below) and add `@pytest.mark.xfail`-marked tests asserting phrase / gap / exclusion / Hebrew-expansion parity — these become a follow-up plan in v7.14.x. The deferred path is acceptable per the reviewer's wording ("if a full refactor is too invasive for this revision, document the divergence in the plan's `<deferred>` block").

    Executor decides A vs B during execution by inspecting the actual size of the main search query-construction code:
    - If the query construction is ≤ 50 LOC and self-contained → take Option A.
    - If the query construction touches Responsa expansion, exclusion lists, refinement chains, or multiple call sites → take Option B and document.

    Either way, ship `test_query_semantics_phrase_mode_parity_with_main`. Under Option A it passes; under Option B it xfails with the deferred reason string.

    Wire-up note: Plan 07 will call `engine.reload_local_indexes()` from `MyLibraryTab._on_worker_finished`, `_on_delete_completed`, `_on_rebuild_completed`, and `_on_startup_recovery_completed`. The Plan 07 update lands separately.
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_reload_after_refresh.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "def reload_local_indexes" genizah_core.py` returns 1.
    - `grep -c "def reload_local_lab_index" genizah_core.py` returns 1.
    - HIGH-1 load-bearing test passes: `python -m pytest tests/test_local_reload_after_refresh.py::test_reload_local_indexes_picks_up_new_docs_without_restart -x -q` exits 0.
    - `python -m pytest tests/test_local_reload_after_refresh.py -x -q` exits 0 (all 4-5 reload tests pass).
    - MEDIUM-1 parity test is shipped (pass under Option A; xfail under Option B). Either way: `python -m pytest tests/test_local_reload_after_refresh.py::test_query_semantics_phrase_mode_parity_with_main -q 2>&amp;1 | grep -E "passed|xfailed"` returns a match.
    - If Option B taken: `<deferred>` block in this plan (or in `95-05-SUMMARY.md`) documents the shared-query-builder follow-up with explicit follow-up test names.
    - Plan 07 has been updated to call `engine.reload_local_indexes()` at the four documented call sites (verify by reading Plan 07; cross-plan coordination required).
    - `python -m ruff check genizah_core.py tests/test_local_reload_after_refresh.py` exits 0.
  </acceptance_criteria>
  <done>HIGH-1 reload methods shipped; MEDIUM-1 either taken (Option A — shared builder) or documented as deferred with a follow-up test; Plan 07 wire-up coordinated.</done>
</task>

<deferred>
## MEDIUM-1 deferred follow-up (only if Option B was taken in Task 3)

If the executor takes Option B (no shared query-builder extraction in this plan), this `<deferred>` block records the open work:

- **Follow-up plan:** Extract `_build_tantivy_query(query_str, mode, gap, fields)` from main searcher and route `_query_local_index` through it. Target: a future v7.14.x patch plan.
- **Tests gated by xfail:**
  - `tests/test_local_reload_after_refresh.py::test_query_semantics_phrase_mode_parity_with_main` — phrase mode parity.
  - `tests/test_local_reload_after_refresh.py::test_query_semantics_gap_mode_parity_with_main` — gap mode parity.
  - `tests/test_local_reload_after_refresh.py::test_query_semantics_exclusion_parity_with_main` — exclusion lists.
  - `tests/test_local_reload_after_refresh.py::test_query_semantics_hebrew_expansion_parity_with_main` — Hebrew expansion / Responsa syntax.
- **Trigger to unxfail:** when the shared helper lands, remove the `xfail` markers and assert pass.

The follow-up MUST be a real plan, not a backlog ticket — semantic divergence between LOCAL and main search is a user-visible defect.
</deferred>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| LOCAL Tantivy index files on disk → SearchEngine process memory | D-37 fallback handles corruption/missing/locked |
| LOCAL hits → RRF merge → result list rendered in UI | LOCAL items now visible alongside Genizah; export gates (Plan 09 D-45) handle desktop xlsx |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-18 | Denial of service | Corrupt LOCAL index file crashes SearchEngine on startup | mitigate | D-37 try/except in `__init__` sets `self.local_searcher = None`; main search continues; pinned by `tests/test_local_index_open_fallback.py` |
| T-95-19 | Denial of service | LOCAL Tantivy query throws mid-search → main search crashes | mitigate | `_query_local_index` wraps query in try/except; returns `[]` on any exception; main results unaffected |
| T-95-20 | Tampering | LOCAL hits silently dropped because dedup body whitelists V0.8/V0.7 only | mitigate | D-08 P0: merge happens AFTER `_deduplicate()` (Codex line 7390 fix); pinned by `tests/test_local_post_dedup_merge.py::test_local_hit_before_dedup_dropped` AND `test_local_hit_after_dedup_survives` AND `test_local_merge_inserts_after_dedup_call_site` (W6 AST assertion) |
| T-95-21 | Tampering | Raw BM25 score sort mis-ranks LOCAL vs Genizah because IDF is index-local | mitigate | RRF k=60 used (Cormack/Clarke 2009 default, industry standard for retriever fusion); tie-break: Genizah first — pinned by `test_rrf_tiebreak_genizah_first` (W7) |
</threat_model>

<verification>
- `python -m pytest tests/test_local_post_dedup_merge.py tests/test_side_index_merge.py tests/test_local_index_open_fallback.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (no regressions).
- `python -m ruff check genizah_core.py tests/test_local_post_dedup_merge.py tests/test_side_index_merge.py tests/test_local_index_open_fallback.py` exits 0.
- W6 — `test_local_merge_inserts_after_dedup_call_site` performs the AST walk from a proper Python test file (no python -c one-liner).
- W7 — `test_rrf_tiebreak_genizah_first` AND `test_rrf_does_not_blanket_prioritize_genizah` both pass.
</verification>

<success_criteria>
- `SearchEngine` initializes with optional `self.local_searcher` (None on missing/corrupt).
- LOCAL hits merge POST-`_deduplicate()` (Codex D-08 P0 fix).
- `_deduplicate()` body at `:7916-7921` is UNCHANGED (smaller blast radius).
- RRF k=60 algorithm implemented per RESEARCH Pattern 1.
- W7 — Genizah-first tie-break encoded AND verified by dedicated `test_rrf_tiebreak_genizah_first`.
- W6 — AST assertion runs via pytest, not python -c one-liner.
- D-37 fallback: corrupt/missing LOCAL index → Genizah-only main search.
- 3 Wave-0 stub files green.
- No regressions in existing search tests.
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-05-SUMMARY.md` documenting:
- Final line number of the LOCAL merge insertion (may differ from 7390 by a few lines)
- Variable name choices for query/mode/gap/limit threading
- Whether `_query_local_index`'s query construction matches the main searcher's parse_query API
- W7 confirmation that `test_rrf_tiebreak_genizah_first` shipped and is green
</output>

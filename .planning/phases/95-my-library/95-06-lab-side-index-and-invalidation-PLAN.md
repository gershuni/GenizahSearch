---
phase: 95
plan: 06
type: execute
wave: 3
depends_on: [02, 03, 05]
files_modified:
  - shared/local_indexer.py
  - genizah_core.py
  - tests/test_local_lab_invalidation.py
autonomous: true
requirements: [REQ-3, REQ-6]
must_haves:
  truths:
    - "shared/local_indexer.py emits LOCAL LAB side-index alongside LOCAL main side-index in same indexer run (D-09)"
    - "LOCAL LAB metadata file <LOCAL_LAB_INDEX_DIR>/.meta.json captures weights_hash + lab_schema_version + last_built_at"
    - "lab_composition_search queries BOTH lab_index AND local_lab_index when local_lab_index is available"
    - "Custom fingerprint scoring path preserved for LAB merging (NOT BM25, NOT RRF — per D-09 explicit)"
    - "On weights_hash mismatch, local_lab_index query is skipped and a 'stale, rebuild?' signal is emitted (D-38)"
    - "Same pattern applied to search_composition_logic at genizah_core.py:7923 (non-LAB Composition Search) per REQ-6 three-surface coverage"
    - "Fingerprint helpers _compute_fingerprint_dyn / _compute_fingerprint_static / _normalize_text are LOCKED to Option C (callback injection from genizah_core.py SearchEngine into LocalIndexer.build_lab_side_index)"
  artifacts:
    - path: "shared/local_indexer.py"
      provides: "build_lab_side_index() method on LocalIndexer; write .meta.json with weights_hash; accepts callback functions for fingerprint computation (Option C)"
      contains: "weights_hash"
    - path: "genizah_core.py"
      provides: "lab_composition_search + search_composition_logic extension; local_lab_searcher init + invalidation check; passes SearchEngine-bound fingerprint helpers as callbacks (Option C)"
      contains: "local_lab_searcher"
    - path: "tests/test_local_lab_invalidation.py"
      provides: "D-38 weights_hash invalidation test"
  key_links:
    - from: "LocalIndexer.build_lab_side_index"
      to: "<LOCAL_LAB_INDEX_DIR>/.meta.json"
      via: "writes weights_hash on completion"
      pattern: "weights_hash"
    - from: "genizah_core.py:lab_composition_search"
      to: "self.local_lab_searcher (None on stale or missing)"
      via: "weights_hash check before query"
      pattern: "stale"
    - from: "genizah_core.py:SearchEngine._compute_fingerprint_dyn / _compute_fingerprint_static / _normalize_text"
      to: "LocalIndexer.build_lab_side_index(fingerprint_dyn_fn=..., fingerprint_static_fn=..., normalize_text_fn=...)"
      via: "callback injection — Option C locked (W5)"
      pattern: "fingerprint_dyn_fn="
---

<objective>
Add the LOCAL LAB side-index (D-09) and its invalidation contract (D-38) so that Composition Search and Parallels can include LOCAL hits. Key constraint from CONTEXT D-09 (Codex revision): `fingerprint_dyn` depends on the current LAB `dynamic_rank_map`; if LAB weights change in settings OR main LAB rebuilds, the LOCAL LAB side-index becomes silently stale (wrong fingerprints → wrong Composition matches).

D-38 invalidation triggers: persist `weights_hash = sha256(json.dumps(current_lab_weights, sort_keys=True))` + `lab_schema_version` to `<LOCAL_LAB_INDEX_DIR>/.meta.json` at build time. At Composition/Parallels query time, compare current LAB weights_hash to stored value; if mismatch, skip LOCAL LAB query AND surface non-modal banner `"My Library LAB index out of date — Rebuild?"`.

D-09 mandates NOT using RRF and NOT raw BM25 for LAB merging — the existing custom fingerprint scoring path is preserved; LOCAL LAB hits flow through the same scoring with the same `target_field` (`fingerprint_dyn` or static) and same `target_map`. Merger = concat + sort by `sort_score` desc + Genizah-first tie-break.

**W5 RESOLVED — Fingerprint helper factoring: Option C LOCKED.**

Three factoring options were considered for `_compute_fingerprint_dyn` / `_compute_fingerprint_static` / `_normalize_text`:
- Option A: Make them static methods on `SearchEngine` and import from `genizah_core`.
- Option B: Move to `shared/lab_fingerprint.py`.
- **Option C (LOCKED):** Pass them as callback functions into `LocalIndexer.build_lab_side_index`.

Option A is struck (forces `shared/local_indexer.py` to import `SearchEngine`, creating a circular dependency risk between core and shared). Option B is struck (Plan 06 must not refactor `genizah_core.py` beyond the LAB extension hooks — too wide a blast radius for a Wave-3 plan; backlog item if desired). Option C is the smallest blast radius and matches the existing pattern of helper-injection used elsewhere in `shared/local_indexer.py` (e.g., `cancel_check` callback in Plan 03).

**I14 RESOLVED — `search_composition_logic` line number:** the planner has grepped and confirmed the function definition is at **`genizah_core.py:7923`** (`def search_composition_logic(self, full_text, chunk_size, max_freq, mode, filter_text=None, progress_callback=None, ...)`). Task 2 modifies this exact function.

Output: Extended `shared/local_indexer.py` (LAB-build method with callback signature per Option C) + extended `genizah_core.py` (`lab_composition_search` + `search_composition_logic@:7923` both query LOCAL LAB; weights_hash check + banner signal; LAB-engine code passes SearchEngine fingerprint helpers as callbacks) + 1 GREEN test file.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/phases/95-my-library/95-CONTEXT.md
@.planning/phases/95-my-library/95-PATTERNS.md
@shared/local_indexer.py
@genizah_core.py
@.planning/phases/95-my-library/95-03-SUMMARY.md
@.planning/phases/95-my-library/95-05-SUMMARY.md

<interfaces>
<!-- Main LAB schema + builder (genizah_core.py:742-790 — already loaded in PATTERNS.md) -->

```python
# rebuild_lab_index() body (excerpt):
builder = tantivy.SchemaBuilder()
builder.add_text_field("unique_id", stored=True)
builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace")
builder.add_text_field(self.LAB_FINGERPRINT_FIELD, stored=False, tokenizer_name="simple")
builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")
builder.add_text_field("full_header", stored=True)
builder.add_text_field("shelfmark", stored=True)
builder.add_text_field("source", stored=True)
builder.add_text_field("content", stored=True, tokenizer_name="simple")
schema = builder.build()
index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
self._ensure_lab_tokenizers(index)
writer = index.writer(heap_size=50_000_000)
```

<!-- lab_composition_search entry point (genizah_core.py:1292-1325) -->

```python
def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None,
                            excluded_ids=None, filter_text=None, deep_scan=False, scan_limit=50000,
                            boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                            min_boundary_matches=0, min_delimiter_distance=3):
    """..."""
    ...
    use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
    target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD
    target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ
```

<!-- search_composition_logic entry point — I14 RESOLVED: line 7923 (verified via grep) -->

```python
def search_composition_logic(self, full_text, chunk_size, max_freq, mode, filter_text=None, progress_callback=None, ...):
    # genizah_core.py:7923 — non-LAB Composition Search path
    # Task 2 of this plan extends this function with the same LOCAL LAB query
    # pattern (post Wave-0 stub turned GREEN by `test_local_lab_invalidation.py`).
```

<!-- D-38 metadata file structure -->

```json
{
  "weights_hash": "<sha256 hex>",
  "lab_schema_version": 1,
  "last_built_at": "2026-05-21T12:34:56Z"
}
```
</interfaces>
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add build_lab_side_index() method to shared/local_indexer.py — Option C callbacks LOCKED (W5)</name>
  <read_first>
    - shared/local_indexer.py (from Plan 03 — has build_local_lab_schema and main side-index build)
    - genizah_core.py:742-790 (rebuild_lab_index — fingerprint computation template; source of the closures injected as callbacks)
    - .planning/phases/95-my-library/95-PATTERNS.md ("shared/local_indexer.py — LAB side-index builder (D-09)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-09, D-38 — weights_hash invalidation)
  </read_first>
  <behavior>
    Test in `tests/test_local_lab_invalidation.py`:
    - `test_lab_meta_json_written_on_build` — call `LocalIndexer.build_lab_side_index(lab_engine_or_weights)`; verify `<LOCAL_LAB_INDEX_DIR>/.meta.json` exists; verify it has keys `weights_hash`, `lab_schema_version`, `last_built_at`.
    - `test_lab_meta_weights_hash_deterministic` — given same weights dict, `build_lab_side_index` writes the same `weights_hash`. Different weights → different hash.
    - `test_lab_meta_versioning` — `lab_schema_version` is an int ≥ 1; future schema bumps invalidate the index.
    - `test_build_lab_side_index_callback_signature_option_c` — assert `build_lab_side_index`'s signature has the three callback parameters `fingerprint_dyn_fn`, `fingerprint_static_fn`, `normalize_text_fn` (W5 — Option C LOCKED).
  </behavior>
  <action>
    Add the following method to `LocalIndexer` class in `shared/local_indexer.py`. **W5 — Option C LOCKED: fingerprint helpers are passed as callback functions.** Options A and B are STRUCK.

    ```python
    def build_lab_side_index(
        self,
        lab_weights: dict,
        *,
        fingerprint_dyn_fn,         # Callable[[str, dict], str] — computes fingerprint_dyn from content + dynamic_rank_map
        fingerprint_static_fn,      # Callable[[str], str] — computes static fingerprint from content
        normalize_text_fn,          # Callable[[str], str] — normalizes content for text_normalized field
        lab_schema_version: int = 1,
        dynamic_rank_map=None,
    ):
        """Build LOCAL LAB side-index (D-09) alongside the main LOCAL side-index.

        W5 LOCKED — Option C: fingerprint helpers are passed as callback functions
        from genizah_core.py's SearchEngine. This keeps shared/local_indexer.py
        free of any genizah_core import (no circular-dep risk) and avoids a
        broader refactor of SearchEngine. Option A (static methods on SearchEngine)
        and Option B (move helpers to shared/lab_fingerprint.py) are STRUCK.

        Per D-09: same indexing run produces both side-indexes in sync. This method
        is called from within scan_all() (or at the end of it) — never independently
        of main side-index build.

        Per D-38: writes <LOCAL_LAB_INDEX_DIR>/.meta.json with:
          - weights_hash = sha256(json.dumps(lab_weights, sort_keys=True))
          - lab_schema_version = current version
          - last_built_at = ISO 8601 timestamp

        Arguments:
            lab_weights: dict of current LAB dynamic weights (used for weights_hash).
            fingerprint_dyn_fn: callable that returns fingerprint_dyn for given content + map.
            fingerprint_static_fn: callable that returns static fingerprint for given content.
            normalize_text_fn: callable that returns normalized text.
            lab_schema_version: schema version int (incremented when LAB schema changes).
            dynamic_rank_map: dict mapping tokens to ranks (for fingerprint_dyn_fn).
        """
        import json
        import datetime

        # Build the LOCAL LAB Tantivy index — fingerprint_dyn computed using lab_weights.
        schema = build_local_lab_schema()
        lab_index = tantivy.Index(schema, path=self._lab_index_dir)
        writer = lab_index.writer(heap_size=50_000_000)

        # For each indexed file (SELECT from local_files), compute the LAB
        # representation per page (using injected fingerprint helpers — Option C).
        for sys_id, uid, page_num, file_id, content in self._iterate_lab_source_rows():
            fingerprint_dyn = fingerprint_dyn_fn(content, dynamic_rank_map)
            fingerprint_static = fingerprint_static_fn(content)
            text_normalized = normalize_text_fn(content)
            full_header = _make_full_header(sys_id, page_num, file_id)
            doc = tantivy.Document()
            doc.add_text("unique_id", uid)
            doc.add_text("content", content)
            doc.add_text("text_normalized", text_normalized)
            doc.add_text("fingerprint", fingerprint_static)
            doc.add_text("fingerprint_dyn", fingerprint_dyn)
            doc.add_text("full_header", full_header)
            doc.add_text("shelfmark", sys_id)  # synthetic shelfmark for LOCAL
            doc.add_text("source", "LOCAL")
            writer.add_document(doc)

        writer.commit()
        writer.wait_merging_threads()

        # Write .meta.json (D-38).
        weights_hash = hashlib.sha256(
            json.dumps(lab_weights, sort_keys=True).encode("utf-8")
        ).hexdigest()
        meta = {
            "weights_hash": weights_hash,
            "lab_schema_version": lab_schema_version,
            "last_built_at": datetime.datetime.utcnow().isoformat() + "Z",
        }
        meta_path = os.path.join(self._lab_index_dir, ".meta.json")
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)

        logger.info("LOCAL LAB side-index built: %d pages, weights_hash=%s",
                    self._count_lab_pages(), weights_hash[:8])
    ```

    **W5 — Critical: NO Option A/B fallback.** The function signature uses keyword-only callback parameters (`*,` separator). If a caller omits any callback, the function raises `TypeError` at call time — fail-fast contract. The Qt-side caller in Plan 07 wires the LAB engine and passes the SearchEngine-bound closures.

    Add a helper to expose `weights_hash` from the meta file:
    ```python
    @staticmethod
    def read_lab_meta(lab_index_dir: str) -> Optional[dict]:
        """Read .meta.json for LOCAL LAB invalidation check (D-38)."""
        meta_path = os.path.join(lab_index_dir, ".meta.json")
        if not os.path.exists(meta_path):
            return None
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("LOCAL LAB meta read failed: %r", e)
            return None
    ```
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_lab_invalidation.py -x -q -k "meta_json_written or weights_hash_deterministic or versioning or callback_signature"</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "def build_lab_side_index" shared/local_indexer.py` returns 1.
    - `grep -c "fingerprint_dyn_fn\\|fingerprint_static_fn\\|normalize_text_fn" shared/local_indexer.py` returns ≥ 3 (W5 — Option C signature locked).
    - `grep -c "def read_lab_meta" shared/local_indexer.py` returns 1.
    - `grep -c "weights_hash" shared/local_indexer.py` returns ≥ 3.
    - `grep -c "lab_schema_version" shared/local_indexer.py` returns ≥ 2.
    - `grep -c "\\.meta\\.json" shared/local_indexer.py` returns ≥ 2.
    - First 4 tests in `tests/test_local_lab_invalidation.py` pass (including W5 signature check).
    - `python -m ruff check shared/local_indexer.py` exits 0.
  </acceptance_criteria>
  <done>build_lab_side_index method shipped with Option C callback signature LOCKED; .meta.json written; read_lab_meta helper added.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Extend lab_composition_search + search_composition_logic@7923 with LOCAL LAB query (D-09 + D-38) — I14 RESOLVED</name>
  <read_first>
    - genizah_core.py:1292-1349 (lab_composition_search — exact entry block in PATTERNS.md interfaces)
    - genizah_core.py:7923 (search_composition_logic — non-LAB composition path, I14 PINNED)
    - .planning/phases/95-my-library/95-PATTERNS.md ("Modification 6: lab_composition_search — query both LAB indexes (D-09)")
    - .planning/phases/95-my-library/95-CONTEXT.md (D-09 + D-38 — weights_hash check; banner signal)
    - shared/local_indexer.py (read_lab_meta + build_local_lab_schema available)
  </read_first>
  <behavior>
    Test `test_weights_hash_mismatch_triggers_banner`:
    - Set up `self.local_lab_searcher` via mock or temp index.
    - Write `.meta.json` with `weights_hash="stale_hash"`.
    - Set current LAB weights such that current weights_hash != "stale_hash".
    - Call `lab_composition_search(...)`.
    - Assert `engine.local_lab_searcher_stale == True` (or whatever flag/signal is exposed).
    - Assert the LAB query returns only Genizah LAB results (no LOCAL).
    - If signal-based: verify `local_lab_stale_signal.emit` was called (use a `MagicMock`).

    Test `test_weights_hash_match_local_results_included`:
    - Set up `.meta.json` with `weights_hash` matching current LAB weights.
    - Call `lab_composition_search`.
    - Assert LOCAL LAB hits are present in results (concat + sort by `sort_score`).
    - Assert tie-break: Genizah first when sort_score is equal.

    Test `test_local_lab_missing_falls_back_to_main_only` (D-37 mirror for LAB):
    - `Config.LOCAL_LAB_INDEX_DIR` does not exist.
    - `self.local_lab_searcher is None`.
    - Call `lab_composition_search`; assert results returned, no LOCAL hits, no exception.

    Test `test_search_composition_logic_extends_local_lab_query`:
    - I14 — pin the line number: assert via AST that the function `search_composition_logic` at `genizah_core.py` contains a call to `self.local_lab_searcher` or `self._query_local_lab_index` (the extension hook added in Task 2).
  </behavior>
  <action>
    1. In `genizah_core.py` SearchEngine `__init__` (likely same place where you added `self.local_searcher` in Plan 05), ADD parallel init for `self.local_lab_searcher`:
    ```python
    # Phase 95 D-09 — open LOCAL LAB side-index alongside main LAB.
    self.local_lab_searcher = None
    self.local_lab_searcher_stale = False
    try:
        if os.path.isdir(Config.LOCAL_LAB_INDEX_DIR):
            from shared.local_indexer import build_local_lab_schema, LocalIndexer
            schema = build_local_lab_schema()
            local_lab_index = tantivy.Index(schema, path=Config.LOCAL_LAB_INDEX_DIR)
            self.local_lab_searcher = local_lab_index.searcher()
            # D-38 — check weights_hash freshness.
            meta = LocalIndexer.read_lab_meta(Config.LOCAL_LAB_INDEX_DIR)
            self._lab_local_meta = meta
            logger.info("LOCAL LAB side-index opened: %s", Config.LOCAL_LAB_INDEX_DIR)
    except Exception as e:
        logger.warning("LOCAL LAB index unavailable: %r", e)
        self.local_lab_searcher = None
        self._lab_local_meta = None
    ```

    2. Add a helper to compute current LAB weights_hash:
    ```python
    def _current_lab_weights_hash(self) -> str:
        """Compute hash of current LAB weights for D-38 staleness check."""
        import hashlib, json
        weights_dict = {
            "dynamic_rank_map": self.dynamic_rank_map if self.dynamic_rank_map else None,
            "use_dynamic_weights": getattr(self.settings, "use_dynamic_weights", False),
            # Add any other LAB-affecting settings here.
        }
        return hashlib.sha256(
            json.dumps(weights_dict, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
    ```

    3. Add a helper to check freshness and emit a stale signal:
    ```python
    def _check_local_lab_freshness(self) -> bool:
        """Return True if LOCAL LAB index is fresh; False if stale or missing.
        Side effect: sets self.local_lab_searcher_stale and (optionally) emits signal."""
        if self.local_lab_searcher is None:
            return False
        meta = self._lab_local_meta
        if not meta:
            self.local_lab_searcher_stale = True
            return False
        current_hash = self._current_lab_weights_hash()
        if meta.get("weights_hash") != current_hash:
            self.local_lab_searcher_stale = True
            logger.info("LOCAL LAB index stale (weights changed); banner surface required")
            return False
        self.local_lab_searcher_stale = False
        return True
    ```

    4. Modify `lab_composition_search` (around `:1292+`) to query the LOCAL LAB index when fresh:
    - After the existing main-LAB scoring loop produces `genizah_lab_hits`, IF `_check_local_lab_freshness()` returns True:
      - Run the same scoring loop against `self.local_lab_searcher` with same `target_field` + `target_map`.
      - Concat results.
      - Sort by `sort_score` desc; Genizah-first tie-break (`(r['sort_score'], r['display']['source'] != 'LOCAL')` — True > False, so non-LOCAL ranks higher on tie).
    - If `_check_local_lab_freshness()` returns False but `self.local_lab_searcher is not None`, log the staleness signal — the UI banner is surfaced in Plan 07 (MyLibraryTab connects to this state).

    5. Apply the SAME extension pattern to `search_composition_logic` at **`genizah_core.py:7923`** (I14 — verified line number). The function signature is:
    ```python
    def search_composition_logic(self, full_text, chunk_size, max_freq, mode, filter_text=None, progress_callback=None, ...):
    ```
    REQ-6 covers both surfaces. The non-LAB path uses different scoring; the LOCAL extension mirrors whatever the local equivalent is. Add the LOCAL LAB query hook AFTER the main scoring loop, IF `_check_local_lab_freshness()` is True. Concat + sort with Genizah-first tie-break.

    DO NOT use RRF or BM25 here — preserve the existing custom fingerprint scoring path per CONTEXT D-09 explicit.

    6. **Where SearchEngine passes Option C callbacks to LocalIndexer (W5 wire-up):**

    The LAB-engine code path (`rebuild_lab_index` flow, or the new "rebuild LOCAL LAB" trigger called when MyLibraryTab runs Refresh) does:

    ```python
    # In SearchEngine — example trigger to build LOCAL LAB alongside main LAB rebuild
    def rebuild_local_lab_index(self, local_indexer):
        """Trigger LOCAL LAB rebuild via the LocalIndexer, passing fingerprint
        helpers as callbacks (W5 — Option C). Called from MyLibraryTab Refresh
        (Plan 07) and Tools→Rebuild LAB (per D-38)."""
        lab_weights = {
            "dynamic_rank_map": self.dynamic_rank_map,
            "use_dynamic_weights": getattr(self.settings, "use_dynamic_weights", False),
        }
        local_indexer.build_lab_side_index(
            lab_weights=lab_weights,
            fingerprint_dyn_fn=self._compute_fingerprint_dyn,        # bound method
            fingerprint_static_fn=self._compute_fingerprint_static,  # bound method
            normalize_text_fn=self._normalize_text,                   # bound method
            lab_schema_version=1,
            dynamic_rank_map=self.dynamic_rank_map,
        )
    ```

    The executor identifies the existing helper names in `genizah_core.py` (run `grep -nE "def _compute_fingerprint|def _normalize_text" genizah_core.py`). If any of those helpers don't exist under those exact names yet, the executor adds thin wrappers around whatever code in `rebuild_lab_index` does the equivalent computation (no logic change — pure extract-method refactor).
  </action>
  <verify>
    <automated>python -m pytest tests/test_local_lab_invalidation.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "self.local_lab_searcher" genizah_core.py` returns ≥ 3 (init + queries in lab_composition_search + search_composition_logic).
    - `grep -c "_current_lab_weights_hash\\|_check_local_lab_freshness" genizah_core.py` returns ≥ 2.
    - `grep -c "local_lab_searcher_stale" genizah_core.py` returns ≥ 2.
    - `grep -c "fingerprint_dyn_fn=" genizah_core.py` returns ≥ 1 (W5 — callback wire-up at the rebuild call site).
    - LAB scoring path NOT replaced with RRF: `grep -c "_rrf_merge.*lab\\|lab.*_rrf_merge" genizah_core.py` returns 0 (RRF only used for MAIN search per Plan 05; LAB preserves custom scoring per D-09).
    - I14 — `search_composition_logic` at `genizah_core.py:7923` contains the LOCAL LAB extension hook. Verify via AST or `grep -n "def search_composition_logic" genizah_core.py` returns line 7923 (or document the new line if shifted).
    - `python -m pytest tests/test_local_lab_invalidation.py -x -q` exits 0.
    - REGRESSION: `python -m pytest tests/ -k "lab_composition or composition_search or search_composition_logic" -x -q` exits 0.
    - `python -m ruff check genizah_core.py tests/test_local_lab_invalidation.py` exits 0.
  </acceptance_criteria>
  <done>Composition Search + Parallels query LOCAL LAB when fresh; weights_hash mismatch flags stale state; D-09 custom-scoring path preserved; W5 Option C callbacks wired; I14 — line 7923 search_composition_logic extended.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| LAB weights (settings panel) → LOCAL LAB fingerprints | Weight changes invalidate fingerprints; D-38 weights_hash detects |
| LOCAL LAB Tantivy index files on disk → SearchEngine | Same D-37 fallback applies (corrupt/missing) |
| SearchEngine fingerprint helpers (bound methods) → LocalIndexer callbacks | W5 Option C: callbacks injected at build time; no circular import |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-95-22 | Tampering (silent) | LAB weights change → LOCAL LAB fingerprints silently wrong → wrong Composition matches | mitigate | D-38 weights_hash invalidation contract; `_check_local_lab_freshness()` compares hashes; stale → skip query + banner via signal |
| T-95-23 | Denial of service | LOCAL LAB index corruption blocks main LAB Composition Search | mitigate | D-37-mirror try/except; sets `self.local_lab_searcher = None`; main LAB continues |
| T-95-24 | Information disclosure | LOCAL LAB fingerprint data on disk (cleartext) | accept | D-33 disclosure in Help (Plan 09); same trust model as main LOCAL index — OS disk encryption is user's responsibility |
| T-95-34 | Tampering | Future contributor passes wrong fingerprint callback signature, breaking LOCAL LAB silently | mitigate | W5 — `build_lab_side_index` keyword-only callback args (`*,` separator); omission raises TypeError at call time (fail-fast); signature pinned by `test_build_lab_side_index_callback_signature_option_c` |
</threat_model>

<verification>
- `python -m pytest tests/test_local_lab_invalidation.py -x -q` exits 0.
- `python -m pytest tests/ -q` exits 0 (no regressions).
- `python -m ruff check shared/local_indexer.py genizah_core.py tests/test_local_lab_invalidation.py` exits 0.
- LOCAL LAB index builds .meta.json with valid JSON: `python -c "import json; json.load(open('.../LocalLabIndex/.meta.json'))" exits 0` (when a fixture-built index exists in test env).
- W5 — Option C callback signature pinned: `build_lab_side_index` rejects calls missing any of `fingerprint_dyn_fn`, `fingerprint_static_fn`, `normalize_text_fn`.
- I14 — `search_composition_logic` extension lives in the function at `genizah_core.py:7923` (or new line if shifted).
</verification>

<success_criteria>
- `LocalIndexer.build_lab_side_index` shipped with **Option C callback signature LOCKED (W5)**; writes `.meta.json` with `weights_hash` + `lab_schema_version` + `last_built_at`.
- `SearchEngine` initializes optional `self.local_lab_searcher` with D-37 fallback.
- `lab_composition_search` queries BOTH lab_index AND local_lab_index when fresh.
- `search_composition_logic` at **`genizah_core.py:7923` (I14)** extended same way per REQ-6.
- Weights_hash mismatch → `self.local_lab_searcher_stale = True`; banner surface deferred to Plan 07 UI.
- Custom fingerprint scoring path PRESERVED (NOT RRF, NOT BM25 per D-09).
- `SearchEngine.rebuild_local_lab_index` wires bound-method callbacks into `build_lab_side_index` (W5 — Option C wire-up).
- 1 Wave-0 stub file (test_local_lab_invalidation.py) GREEN.
</success_criteria>

<output>
After completion, create `.planning/phases/95-my-library/95-06-SUMMARY.md` documenting:
- **W5 confirmation: Option C locked.** Document the callback wire-up at the rebuild call site.
- Whether `_ensure_lab_tokenizers` was extracted to shared or kept on SearchEngine (planner discretion)
- The exact LAB-affecting settings list included in `_current_lab_weights_hash`
- Whether the staleness banner uses a Qt signal or a flag polled by MyLibraryTab
- I14 confirmation: final line number of `search_composition_logic` after edit (may differ from 7923 by a few lines)
</output>

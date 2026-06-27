---
status: diagnosed
trigger: "Regular (non-composition) search cancel is slow (~20s delay). Composition search cancel was fixed (GAP-4 in 42-04) but regular search was not addressed."
created: 2026-03-01T00:00:00Z
updated: 2026-03-01T12:00:00Z
---

## Current Focus

hypothesis: CONFIRMED - Three independent cancel-blocking bottlenecks in the regular search path
test: Full code trace of execute_search, web progress_cb, and post-search enrichment
expecting: N/A - root cause confirmed
next_action: Return diagnosis

## Symptoms

expected: Cancel button should stop regular search immediately and show partial results (like composition search after GAP-4 fix)
actual: ~20s delay between pressing cancel and seeing partial results
errors: none
reproduction: Start a regular search on a broad query (e.g., common Hebrew word in Variants mode), press cancel
started: Always (composition cancel was fixed in 42-04 but regular was not addressed)

## Eliminated

- hypothesis: progress_callback modulo too large (i % 50)
  evidence: Code already changed to i % 5 at genizah_core.py:5864 and :5648. The i%5 callback frequency is adequate.
  timestamp: 2026-03-01T12:00:00Z

## Evidence

- timestamp: 2026-03-01T12:00:00Z
  checked: web/pages/search.py cancel mechanism (lines 1253-1256, 2161-2164)
  found: cancel_search() sets search_state.is_cancelled=True. progress_cb checks is_cancelled and raises InterruptedError. This is correct and functional.
  implication: The cancel signal mechanism itself is sound

- timestamp: 2026-03-01T12:00:00Z
  checked: genizah_core.py execute_search hit-loop cancel (lines 5862-5925)
  found: The for loop at line 5863 calls progress_callback every 5 hits (i%5==0) at line 5864. InterruptedError is caught at line 5923, sets was_interrupted=True, returns partial results. This works correctly.
  implication: The hit-processing loop itself is cancel-responsive (max 5 hit latency)

- timestamp: 2026-03-01T12:00:00Z
  checked: genizah_core.py BLOCKING Tantivy call (line 5850)
  found: self.searcher.search(query, Config.SEARCH_LIMIT) where SEARCH_LIMIT=50000. This is a BLOCKING C-extension call to Tantivy that retrieves and scores up to 50,000 document pointers. No cancel check is possible during this call. For broad queries (common Hebrew words), this single call can take 5-20 seconds.
  implication: BOTTLENECK #1 - The Tantivy search phase is completely un-cancellable

- timestamp: 2026-03-01T12:00:00Z
  checked: genizah_core.py pre-loop variant computation (line 5834)
  found: _get_or_compute_variants(terms, mode) at line 5834 computes up to 8000 variants per term (Config.REGEX_VARIANTS_LIMIT). First call is expensive; subsequent calls are cached. No cancel check.
  implication: BOTTLENECK #2 (first search only) - Variant precomputation is un-cancellable

- timestamp: 2026-03-01T12:00:00Z
  checked: web/pages/search.py post-search enrichment (lines 2231-2283)
  found: After run_core_search returns (whether cancelled or not), FOUR enrichment queries run in parallel (lines 2250-2255: domains, transcriptions, catalog counts, printed IDs) PLUS a sequential domain hierarchy fetch (lines 2278-2283). These run on ALL partial results with no cancel check. No early-out if was_cancelled.
  implication: BOTTLENECK #3 - Post-search enrichment adds delay after cancel is processed

- timestamp: 2026-03-01T12:00:00Z
  checked: genizah_core.py _execute_batched_search (lines 777-782) for lab_search path
  found: progress_callback is wrapped in try/except Exception: pass at lines 779-782. InterruptedError is a subclass of Exception, so cancel InterruptedError is SILENTLY SWALLOWED. Lab/deep-scan searches via web are completely un-cancellable.
  implication: BONUS BUG - Lab mode web cancel is completely broken (separate from main issue)

## Resolution

root_cause: |
  Three independent cancel-blocking bottlenecks cause the ~20s delay:

  BOTTLENECK 1 (DOMINANT, ~5-20s): Tantivy search call is un-cancellable
    File: genizah_core.py line 5850
    Code: res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
    Why:  SEARCH_LIMIT=50000. This is a blocking C-extension call to the Tantivy
          search engine. For broad queries on the 217K document index, scoring and
          ranking 50K candidates takes 5-20 seconds. There is no way to interrupt
          a Tantivy search in progress -- it must complete before the hit-processing
          loop (which HAS cancel checks) can begin.

  BOTTLENECK 2 (first search only, ~1-5s): Variant precomputation
    File: genizah_core.py line 5834
    Code: self._get_or_compute_variants(terms, mode)
    Why:  Generates up to 8000 variants per search term. Cached after first call,
          but the first search in a session for a given term+mode is un-cancellable.

  BOTTLENECK 3 (post-cancel, ~1-3s): Enrichment queries run unconditionally
    File: web/pages/search.py lines 2250-2283
    Code: asyncio.gather(domains, transcriptions, catalog_counts, printed_ids) + hierarchy
    Why:  After execute_search returns partial results (cancel worked in the loop),
          four enrichment queries still run on all partial results. No check for
          was_cancelled to skip enrichment.

  BONUS BUG: Lab mode cancel completely broken
    File: genizah_core.py lines 779-782
    Code: try: progress_callback(i, total_hits) except Exception: pass
    Why:  InterruptedError is caught and silently swallowed, making lab/deep-scan
          web cancel completely non-functional.

  The i%5 modulo was already fixed (previously i%50). The hit-processing loop
  itself is adequately responsive. The problem is the phases BEFORE and AFTER
  the loop that have no cancel awareness.

fix:
verification:
files_changed: []

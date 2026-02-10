# Phase 17: Integration Testing & Polish - Research

**Researched:** 2026-02-10
**Domain:** Cross-app integration testing, search regression testing, performance profiling, edge case validation
**Confidence:** HIGH

## Summary

Phase 17 is the quality gate for the Responsa Search milestone. Its core mission is verifying that the same Responsa query produces identical results in both the web (NiceGUI) and desktop (PyQt6) apps, that edge cases are handled gracefully, that performance is acceptable on the full corpus, and that existing search modes are unaffected by the Responsa additions.

The codebase is well-positioned for this phase. All Responsa search logic resides in `genizah_core.py` (shared between both apps, per XAPP-02). Both apps build an identical `responsa_options` dict and pass it through to the same `SearchEngine.execute_search()` method. This means cross-app parity testing reduces to verifying that both apps construct the same `responsa_options` dict for the same UI state, and that the shared core produces deterministic results. There are already 135 passing Responsa tests across `test_responsa_core.py` (unit tests for parser, expansion, explosion guard) and `test_responsa_integration.py` (integration tests for Tantivy query building, regex building, pipeline routing). Phase 17 adds higher-level tests that exercise the full pipeline with real-world queries and verify behavior at the boundaries.

The main challenge is performance testing. The project's Tantivy index (`Genizah_Index/`) contains ~217,000 manuscript records. A Responsa query with variants + JA enabled can expand a single word into hundreds of terms. The success criterion requires queries to complete in under 5 seconds. Testing this requires access to the full index (not just mocked), which means some tests must be marked as requiring the index and skipped in CI if unavailable.

**Primary recommendation:** Structure Phase 17 into three plans: (1) cross-app parity tests and edge case tests (pure Python, no index needed), (2) regression tests for existing search modes (mocked index, verifying behavior unchanged), and (3) performance benchmarks (requires real index, documents observed timing).

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| pytest | 9.0.2 | Test framework | Already used for all 304 tests in the project |
| unittest.mock | stdlib | Mocking SearchEngine, Tantivy index | Already used extensively in `test_responsa_integration.py` |
| time | stdlib | Performance timing measurements | For the <5 second benchmark |
| re | stdlib | Regex pattern validation | Already used in Responsa tests |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pytest-benchmark | N/A | Optional formal benchmarking | Only if performance tests need repeatable statistical data (defer unless needed) |
| pytest markers | built-in | `@pytest.mark.slow`, `@pytest.mark.requires_index` | For skipping performance tests in CI |

### No New Dependencies Required
All testing uses pytest and stdlib. No new packages needed.

**Installation:**
```bash
# No new dependencies needed
```

## Architecture Patterns

### Recommended Test File Structure
```
tests/
├── test_responsa_core.py          # EXISTING: 67 unit tests (parser, expansion, guard)
├── test_responsa_integration.py   # EXISTING: 68 integration tests (tantivy/regex building, pipeline)
├── test_responsa_parity.py        # NEW: Cross-app parity verification (Phase 17)
├── test_responsa_edge_cases.py    # NEW: Edge case handling (Phase 17)
├── test_responsa_regression.py    # NEW: Non-Responsa mode regression (Phase 17)
├── test_responsa_performance.py   # NEW: Performance benchmarks (Phase 17, requires index)
└── conftest.py                    # EXISTING: path setup
```

### Pattern 1: Cross-App Parity Test (Core Pattern)
**What:** Verify both apps would produce identical results by testing at the shared `execute_search()` interface
**When to use:** For every XAPP-01 test case
**Why this works:** Both apps call `SearchEngine.execute_search(query, mode, gap, responsa_options=...)`. If the same inputs produce the same outputs, parity is guaranteed regardless of UI framework.

```python
# Pattern: Test that identical inputs produce identical outputs
class TestCrossAppParity:
    """Verify XAPP-01: identical results for the same Responsa query."""

    def _build_web_responsa_options(self, variants=False, ja=False,
                                     flex_spacing=False, bidirectional=False):
        """Build responsa_options dict matching web app's construction."""
        return {
            'responsa_mode': True,
            'variants': variants,
            'ja': ja,
            'flex_spacing': flex_spacing,
            'bidirectional': bidirectional,
            'variant_mode': 'variants' if variants else 'exact',
        }

    def _build_desktop_responsa_options(self, variants=False, ja=False,
                                         flex_spacing=False, bidirectional=False):
        """Build responsa_options dict matching desktop app's construction."""
        return {
            'responsa_mode': True,
            'variants': variants,
            'ja': ja,
            'flex_spacing': flex_spacing,
            'bidirectional': bidirectional,
            'variant_mode': 'variants' if variants else 'exact',
        }

    def test_options_dicts_identical(self):
        """Web and desktop build identical responsa_options dicts."""
        for variants in [True, False]:
            for ja in [True, False]:
                web = self._build_web_responsa_options(variants=variants, ja=ja)
                desk = self._build_desktop_responsa_options(variants=variants, ja=ja)
                assert web == desk, f"Mismatch: variants={variants}, ja={ja}"
```

### Pattern 2: Edge Case Test with Mocked Engine
**What:** Test edge cases without needing a real Tantivy index
**When to use:** For empty queries, single-char terms, special characters, etc.

```python
class TestEdgeCases:
    def _make_engine(self):
        """Create SearchEngine with mocked dependencies."""
        meta_mgr = MagicMock()
        var_mgr = MagicMock()
        var_mgr.get_variants = MagicMock(
            side_effect=lambda term, mode, limit=200: [term]
        )
        with patch.object(SearchEngine, 'reload_index', return_value=False):
            engine = SearchEngine(meta_mgr, var_mgr)
        return engine

    def test_empty_query_returns_empty(self):
        engine = self._make_engine()
        result = engine.execute_search(
            '', 'exact', 0,
            responsa_options={'responsa_mode': True, 'variants': False,
                            'ja': False, 'flex_spacing': False,
                            'bidirectional': False, 'variant_mode': 'exact'}
        )
        assert result == []
```

### Pattern 3: Regression Test for Non-Responsa Modes
**What:** Verify existing modes produce the same Tantivy query and regex as before
**When to use:** For all 7 non-Responsa search modes

```python
class TestNonResponsaModesRegression:
    """Verify existing modes are unchanged when Responsa is OFF."""

    def test_exact_mode_unchanged(self):
        engine = _make_search_engine()
        # Build query the old way
        result_old = engine.build_tantivy_query(['test'], 'exact')
        # Build query the new way (responsa_components=None)
        result_new = engine.build_tantivy_query(['test'], 'exact',
                                                 responsa_components=None)
        assert result_old == result_new

    def test_variants_mode_unchanged(self):
        engine = _make_search_engine()
        result = engine.build_tantivy_query(['test'], 'variants')
        assert '"test"^5' in result
```

### Pattern 4: Performance Test with Real Index
**What:** Measure search time on full corpus
**When to use:** Only when `Genizah_Index/` exists

```python
import pytest
import time

@pytest.mark.requires_index
class TestPerformance:
    """Performance benchmarks -- requires real Tantivy index."""

    @pytest.fixture(autouse=True)
    def skip_without_index(self):
        """Skip if Genizah_Index directory does not exist."""
        import os
        index_path = os.path.join(os.path.dirname(__file__), '..', 'Genizah_Index')
        if not os.path.isdir(index_path):
            pytest.skip("Genizah_Index not available")

    def test_responsa_with_variants_ja_under_5_seconds(self, real_engine):
        start = time.perf_counter()
        results = real_engine.execute_search(
            '#shalom', 'exact', 3,
            responsa_options={
                'responsa_mode': True, 'variants': True, 'ja': True,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'variants'
            }
        )
        elapsed = time.perf_counter() - start
        assert elapsed < 5.0, f"Search took {elapsed:.2f}s, exceeds 5s limit"
```

### Anti-Patterns to Avoid
- **Testing UI rendering in Phase 17:** This phase tests core search logic parity, NOT whether checkboxes render correctly. UI rendering is verified manually/in UAT, not in automated integration tests.
- **Duplicating existing test coverage:** 135 tests already cover parser, expansion, explosion guard, regex/tantivy building. Phase 17 should NOT re-test these -- it should test at a higher level (end-to-end query -> results).
- **Requiring network for tests:** All tests should work offline. Mock Supabase, IIIF, etc.
- **Creating flaky performance tests:** Performance tests should use generous margins. If target is 5s, assert < 10s in automated tests and document 5s as the manual acceptance threshold.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Test engine setup | Custom SearchEngine init per test | `_make_search_engine()` helper from `test_responsa_integration.py` | Already handles mocking, proven pattern |
| Performance measurement | Custom timing framework | `time.perf_counter()` with simple assertions | stdlib, reliable, no dependency |
| Skipping tests without index | Custom import guards | `@pytest.mark.requires_index` + conftest skip logic | Standard pytest pattern |
| Test data (Hebrew text) | Generating random Hebrew | Use real Hebrew terms from the project (e.g., `Config.GRAMMATICAL_PREFIXES` list) | Realistic, validates actual behavior |
| Regression baselines | Snapshot files | Inline assertions on known outputs (query strings, regex patterns) | Simpler, self-documenting |

**Key insight:** Phase 17's value is in the TEST DESIGN, not in tooling. The right test cases catch real bugs; fancy frameworks do not. Keep it simple: pytest + assertions.

## Common Pitfalls

### Pitfall 1: False Parity from Mocked-Only Tests
**What goes wrong:** Tests with mocked Tantivy/searcher pass but real execution diverges because both apps build `responsa_options` slightly differently.
**Why it happens:** Web uses `mode_select.value == 'responsa'` while desktop uses `self.mode_combo.currentIndex() == self.MODE_RESPONSA`. The dict construction code is duplicated, not shared.
**How to avoid:** Explicitly test that both apps' `responsa_options` construction code produces identical dicts for every combination of checkbox states. Also test with a real index if available.
**Warning signs:** Tests pass in CI but users report different results between apps.

### Pitfall 2: Regex Timeout in Performance Tests
**What goes wrong:** A complex Responsa query with flex spacing generates a massive regex that takes >30 seconds on long documents, causing the test to hang.
**Why it happens:** `\s*` per character creates catastrophic backtracking on non-matching text.
**How to avoid:** Set a per-test timeout (`@pytest.mark.timeout(30)`). For performance tests, use queries that are representative but not pathological. The explosion guard should prevent most worst-case scenarios, but flex spacing + long documents can still be slow.
**Warning signs:** Test suite hangs on specific Hebrew queries with flex spacing enabled.

### Pitfall 3: Non-Deterministic Result Ordering
**What goes wrong:** Parity test asserts `results_web == results_desktop` but they differ in order even though the same documents appear.
**Why it happens:** Tantivy scoring can vary with document stats, and deduplication order depends on hit order.
**How to avoid:** Compare result SETS (by uid or shelfmark), not ordered lists. Or sort both results by a stable key before comparison.
**Warning signs:** Flaky tests that sometimes pass and sometimes fail.

### Pitfall 4: Testing Internal Implementation Instead of Behavior
**What goes wrong:** Tests assert on the exact regex pattern string, which breaks when implementation changes (e.g., alternation order).
**Why it happens:** Testing implementation details rather than behavior.
**How to avoid:** Test BEHAVIOR: "this text matches the pattern" and "this text does NOT match the pattern". Don't assert `pattern == '(A|B|C)'` -- assert `re.search(pattern, 'text with A')` is not None.
**Warning signs:** Tests break after harmless refactoring.

### Pitfall 5: Missing the `#` Conflict Test
**What goes wrong:** User types `#T-S 12.1` expecting Shelfmark search but gets Responsa prefix expansion because Responsa mode is on.
**Why it happens:** `#` means different things in different modes. The bypass logic in `parse_query_syntax()` must work correctly.
**How to avoid:** Explicitly test: (1) `#word` in Responsa mode -> prefix expansion (not Shelfmark), (2) `#word` in non-Responsa mode -> Shelfmark search, (3) switching between modes doesn't leave stale state.
**Warning signs:** Existing tests for this pass (they do, in `test_responsa_integration.py::TestParseQuerySyntaxResponsaBypass`), but no test verifies the full end-to-end with execute_search.

### Pitfall 6: Explosion Guard Warning Not Reaching UI
**What goes wrong:** The explosion guard triggers and downgrades options, but the warning message never appears in the UI.
**Why it happens:** The warning is attached to `results[0]['responsa_warning']` but the UI code doesn't read it.
**How to avoid:** Test the full chain: (1) trigger explosion guard, (2) verify `results[0]['responsa_warning']` exists, (3) verify UI code reads and displays it. Steps 1-2 are automated; step 3 is manual UAT.
**Warning signs:** Both apps already implement this (verified in code review -- web: `search.py:1737`, desktop: `genizah_app.py:13618`).

### Pitfall 7: Flex Spacing Minimum Length Not Enforced
**What goes wrong:** User enables flex spacing on a single-character query, creating a trivial `\s*` regex that matches everything.
**Why it happens:** Success criteria #4 says "min 3 chars" for flex spacing but this may not be enforced in the parser.
**How to avoid:** Test that flex spacing is ignored (or raises a warning) for terms shorter than 3 characters. Verify the core engine enforces this minimum.
**Warning signs:** Single-character flex-spaced query returns all documents.

## Code Examples

### Example 1: Cross-App Parity Test Structure
```python
# Source: Derived from web/pages/search.py:1660 and genizah_app.py:13406
import pytest
from unittest.mock import MagicMock, patch
from genizah_core import SearchEngine, parse_responsa_query, ResponsaComponent

class TestCrossAppParity:
    """XAPP-01: Identical results for the same Responsa query in both apps."""

    def _make_engine_with_hits(self, content_texts):
        """Create SearchEngine that returns specified documents."""
        meta_mgr = MagicMock()
        var_mgr = MagicMock()
        var_mgr.get_variants = MagicMock(
            side_effect=lambda term, mode, limit=200: [term]
        )
        with patch.object(SearchEngine, 'reload_index', return_value=False):
            engine = SearchEngine(meta_mgr, var_mgr)
        # Set up mock index/searcher
        engine.index = MagicMock()
        engine.searcher = MagicMock()
        engine.index.parse_query = MagicMock(return_value=MagicMock())
        # Mock hits
        hits = [(1.0, MagicMock()) for _ in content_texts]
        mock_result = MagicMock()
        mock_result.hits = hits
        engine.searcher.search = MagicMock(return_value=mock_result)
        # Mock doc retrieval
        for i, text in enumerate(content_texts):
            pass  # Would set up doc mock per hit
        return engine

    def test_web_desktop_options_identical_all_off(self):
        """Both apps produce same options when all checkboxes off."""
        web_opts = {
            'responsa_mode': True, 'variants': False, 'ja': False,
            'flex_spacing': False, 'bidirectional': False, 'variant_mode': 'exact'
        }
        desktop_opts = {
            'responsa_mode': True, 'variants': False, 'ja': False,
            'flex_spacing': False, 'bidirectional': False, 'variant_mode': 'exact'
        }
        assert web_opts == desktop_opts

    def test_web_desktop_options_identical_all_on(self):
        """Both apps produce same options when all checkboxes on."""
        web_opts = {
            'responsa_mode': True, 'variants': True, 'ja': True,
            'flex_spacing': True, 'bidirectional': True, 'variant_mode': 'variants'
        }
        desktop_opts = {
            'responsa_mode': True, 'variants': True, 'ja': True,
            'flex_spacing': True, 'bidirectional': True, 'variant_mode': 'variants'
        }
        assert web_opts == desktop_opts
```

### Example 2: Edge Case Test for Empty/Short Queries
```python
# Source: Derived from success criteria #4
class TestEdgeCases:
    def test_empty_query_returns_empty(self):
        """Empty query returns empty list, no crash."""
        engine = _make_search_engine()
        result = engine.execute_search(
            '', 'exact', 0,
            responsa_options={
                'responsa_mode': True, 'variants': False, 'ja': False,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'exact'
            }
        )
        assert result == []

    def test_whitespace_only_query_returns_empty(self):
        """Whitespace-only query returns empty list."""
        engine = _make_search_engine()
        result = engine.execute_search(
            '   ', 'exact', 0,
            responsa_options={
                'responsa_mode': True, 'variants': False, 'ja': False,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'exact'
            }
        )
        assert result == []

    def test_hash_in_responsa_mode_not_shelfmark(self):
        """# is prefix expansion in Responsa mode, not Shelfmark."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#test', responsa_mode=True)
        assert mode is None  # NOT 'Shelfmark'
        assert clean == '#test'

    def test_hash_without_responsa_is_shelfmark(self):
        """# is Shelfmark when Responsa mode OFF."""
        engine = _make_search_engine()
        mode, clean = engine.parse_query_syntax('#test', responsa_mode=False)
        assert mode == 'Shelfmark'
```

### Example 3: Regression Test for Existing Modes
```python
# Source: Success criteria #5
class TestExistingModesUnchanged:
    """All non-Responsa modes work when Responsa mode is OFF."""

    MODES = [
        ('exact', 'test'),
        ('variants', 'test'),
        ('fuzzy', 'test'),
        ('Regex', 'test.*'),
        ('Shelfmark', 'T-S 12.1'),
        ('Title', 'Genesis'),
    ]

    @pytest.mark.parametrize("mode,query", MODES)
    def test_mode_produces_valid_query(self, mode, query):
        """Each non-Responsa mode builds a valid Tantivy query."""
        engine = _make_search_engine()
        if mode in ['Shelfmark', 'Title']:
            # These are metadata modes, not Tantivy
            return
        terms = [query] if mode == 'Regex' else query.split()
        result = engine.build_tantivy_query(terms, mode)
        assert result is not None
        assert len(result) > 0

    @pytest.mark.parametrize("mode,query", MODES)
    def test_mode_produces_valid_regex(self, mode, query):
        """Each non-Responsa mode builds a valid regex pattern."""
        engine = _make_search_engine()
        if mode in ['Shelfmark', 'Title']:
            return
        terms = [query] if mode == 'Regex' else query.split()
        result = engine.build_regex_pattern(terms, mode, 0)
        assert result is not None
```

### Example 4: Explosion Guard Cascade Verification
```python
# Source: Success criteria #2
class TestExplosionGuardIntegration:
    """Verify the cascade triggers correctly for complex queries."""

    def test_cascade_with_real_word(self):
        """Real Hebrew word with full expansion triggers cascade."""
        from genizah_core import ResponsaComponent, _apply_explosion_guard
        # A word with #prefix + JA + max variants = should exceed 500
        components = [
            ResponsaComponent(words=["shalom"], grammatical_prefixes=True),
            ResponsaComponent(words=["olam"], grammatical_prefixes=True),
            ResponsaComponent(words=["torah"], grammatical_prefixes=True),
        ]
        var_mgr = MagicMock()
        var_mgr.get_variants = MagicMock(
            side_effect=lambda t, m, limit=200: [f"{t}_v{i}" for i in range(50)]
        )
        expanded, warning, opts = _apply_explosion_guard(
            components,
            variants_on=True,
            ja_on=True,
            var_mgr=var_mgr,
            variant_mode='variants_maximum'
        )
        assert warning is not None  # Should have triggered cascade
```

### Example 5: Performance Benchmark Pattern
```python
# Source: Success criteria #3
import time
import pytest

@pytest.mark.requires_index
def test_responsa_search_performance(real_engine):
    """Responsa query with variants + JA completes in <5 seconds."""
    queries = [
        '#\u05e9\u05dc\u05d5\u05dd',        # #shalom - single word with prefixes
        '#\u05e9\u05dc\u05d5\u05dd \u05e2\u05d5\u05dc\u05dd',  # #shalom olam - two words
        '#\u05ea\u05d5\u05e8\u05d4 [3] #\u05de\u05e9\u05d4',  # #torah [3] #moshe - with gap
    ]
    for query in queries:
        start = time.perf_counter()
        results = real_engine.execute_search(
            query, 'exact', 3,
            responsa_options={
                'responsa_mode': True, 'variants': True, 'ja': True,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'variants'
            }
        )
        elapsed = time.perf_counter() - start
        # Automated test uses generous 10s margin; manual UAT uses 5s
        assert elapsed < 10.0, (
            f"Query '{query}' took {elapsed:.2f}s (>10s limit). "
            f"Returned {len(results)} results."
        )
```

## Existing Test Coverage Map

Understanding what Phase 17 does NOT need to re-test:

| Area | Existing Tests | File | Count |
|------|---------------|------|-------|
| Parser (parse_responsa_query) | All syntax variants, OR groups, wildcards, inline alternation | test_responsa_core.py | 18 |
| Prefix expansion | Basic, single, double, no duplicates | test_responsa_core.py | 5 |
| Suffix expansion | Plural, possessive, combined | test_responsa_core.py | 8 |
| Plene/defective | Add/remove vav/yod, boundaries | test_responsa_core.py | 8 |
| Judeo-Arabic | 8 forms, no assimilation | test_responsa_core.py | 7 |
| Explosion guard | Under/over limit, cascade, error | test_responsa_core.py | 7 |
| Gap notation | [N] parsing, multiple gaps, zero gap | test_responsa_core.py | 6 |
| Tabular syntax gen | All modifiers, OR groups, negation | test_responsa_core.py | 14 |
| Negation parsing | Simple, with mods, roundtrip | test_responsa_core.py | 11 |
| Tantivy query (Responsa) | OR groups, boosting, AND join | test_responsa_integration.py | 5 |
| Regex pattern (Responsa) | Wildcards, flex spacing, bidirectional | test_responsa_integration.py | 11 |
| Query syntax bypass | All prefix shortcuts bypassed | test_responsa_integration.py | 6 |
| Execute search pipeline | Parameter acceptance, warning, expansion order | test_responsa_integration.py | 8 |
| Per-pair gaps | Different distances, None fallback, bidirectional | test_responsa_integration.py | 4 |
| **Total existing** | | | **135** |

**What Phase 17 adds:**

| Area | New Tests | Purpose |
|------|-----------|---------|
| Cross-app parity | 8-12 | Verify XAPP-01 at execute_search level |
| Edge cases | 10-15 | Empty, short, special chars, mode switching |
| Regression (non-Responsa) | 6-10 | All existing modes unchanged |
| Explosion guard E2E | 3-5 | Cascade with realistic queries |
| Performance | 3-5 | Timing benchmarks (optional, requires index) |
| **Total new** | **~30-47** | |

## Success Criteria Mapping

Each success criterion maps to specific test categories:

| # | Criterion | Test Approach | Index Needed? |
|---|-----------|---------------|---------------|
| 1 | Same query = same results (web vs desktop) | Compare `responsa_options` dict construction, then verify `execute_search()` is deterministic with same inputs | No (mocked) |
| 2 | Explosion guard triggers correctly | Parameterized tests with increasing complexity until cascade fires | No (mocked) |
| 3 | Performance <5s on full corpus | `time.perf_counter()` around real `execute_search()` calls | YES |
| 4a | Empty query handled | `execute_search('')` returns `[]` | No |
| 4b | Single-char terms with flex spacing (min 3) | Test flex pattern generation for 1-2 char terms | No |
| 4c | `#` in Shelfmark vs Responsa | `parse_query_syntax('#test', responsa_mode=True/False)` | No |
| 5 | Existing modes unchanged | Parameterized tests for all 7 non-Responsa modes | No (mocked) |

## Cross-App Options Construction Analysis

Critical finding: Both apps construct `responsa_options` identically. The web app (search.py:1660-1668) and desktop app (genizah_app.py:13406-13414) both build:

```python
{
    'responsa_mode': True,
    'variants': <checkbox_value>,
    'ja': <checkbox_value>,
    'flex_spacing': <checkbox_value>,
    'bidirectional': <checkbox_value>,
    'variant_mode': 'variants' if <variants_checked> else 'exact',
}
```

Both apps then pass this dict to `SearchEngine.execute_search()`, which is shared code in `genizah_core.py`. Since the dict construction is identical and the execution path is shared, parity is architecturally guaranteed. Tests should verify this structural guarantee holds.

**Key difference to test:** The desktop uses `self.mode_combo.currentIndex() == self.MODE_RESPONSA` (integer comparison) while the web uses `mode_select.value == 'responsa'` (string comparison). Both are correct but test that mode detection logic doesn't diverge.

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| No Responsa tests | 135 unit + integration tests | Phase 14 (Feb 2026) | Solid base for Phase 17 |
| Separate web/desktop search logic | Shared `genizah_core.py` for all search | Architecture decision | Cross-app parity is structural, not behavioral |
| No explosion guard | MAX_EXPANDED_TERMS=500 with cascade | Phase 14 | Phase 17 validates cascade with realistic queries |
| No performance benchmarks | Manual testing only | Never formalized | Phase 17 adds first automated benchmarks |

## Open Questions

1. **Performance test environment**
   - What we know: The `Genizah_Index/` directory is required for performance tests. It is NOT in the git repo (too large). It exists on the developer's machine.
   - What's unclear: Should performance tests be automated (and skipped in CI) or purely manual UAT?
   - Recommendation: Create automated tests with `@pytest.mark.requires_index` that skip gracefully when index is unavailable. Document manual performance UAT checklist separately.

2. **Flex spacing minimum length enforcement**
   - What we know: Success criteria #4 says "single-character terms with flex spacing (min 3 chars)."
   - What's unclear: Is this minimum already enforced in `_make_flex_spacing_pattern()` or does it need to be added?
   - Recommendation: Write a test that verifies flex spacing is either (a) not applied or (b) produces a reasonable pattern for 1-2 character terms. If enforcement is missing, add it as part of this phase.

3. **"Polish" scope**
   - What we know: Phase is called "Integration Testing & Polish" but the success criteria only mention testing. No polish items are specified.
   - What's unclear: Are there UI polish items? Bug fixes? Error message improvements?
   - Recommendation: Treat "polish" as bug fixes discovered during testing. If tests reveal issues, fix them inline. No separate polish tasks unless bugs are found.

## Sources

### Primary (HIGH confidence)
- `genizah_core.py` lines 5438-5697 -- full Responsa `execute_search()` pipeline
- `genizah_core.py` lines 4983-5078 -- `build_tantivy_query()` Responsa branch
- `genizah_core.py` lines 5079-5350 -- `build_regex_pattern()` Responsa branch
- `genizah_core.py` lines 5355-5376 -- `parse_query_syntax()` Responsa bypass
- `web/pages/search.py` lines 1659-1688 -- web `responsa_options` construction and search execution
- `genizah_app.py` lines 13405-13427 -- desktop `responsa_options` construction and SearchThread creation
- `gui_threads.py` lines 25-49 -- SearchThread class with `responsa_options` parameter
- `tests/test_responsa_core.py` -- 67 unit tests (all passing)
- `tests/test_responsa_integration.py` -- 68 integration tests (all passing)
- `.planning/REQUIREMENTS.md` -- XAPP-01 requirement definition

### Secondary (HIGH confidence)
- `.planning/phases/14-responsa-core-engine/14-RESEARCH.md` -- Phase 14 architecture decisions
- `.planning/phases/15-search-ui/15-RESEARCH.md` -- Phase 15 UI wiring patterns
- `.planning/phases/16-tabular-query-builder/16-RESEARCH.md` -- Phase 16 parser extensions

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- uses only pytest/stdlib, no new dependencies
- Architecture: HIGH -- verified against actual source code, both apps' code inspected line-by-line
- Pitfalls: HIGH -- derived from actual code analysis, specific line references
- Test design: HIGH -- builds on existing 135-test foundation, follows established patterns
- Performance testing: MEDIUM -- depends on index availability, thresholds may need tuning

**Research date:** 2026-02-10
**Valid until:** 2026-03-10 (stable domain, all dependent phases implemented)

"""
Performance benchmarks for Responsa search queries on the real Tantivy index.

These tests require the Genizah_Index/ directory (containing the Tantivy database)
to be present at the project root. If the index is not available, ALL tests in this
file are skipped gracefully.

PERFORMANCE THRESHOLD RATIONALE:
---------------------------------
The ROADMAP success criterion says Responsa queries with variants + JA should
complete in "<5 seconds". This is the MANUAL UAT target representing expected
performance on a typical developer machine under normal load.

Automated CI/test environments have variable system load, background processes,
and potentially slower I/O, so tests use a GENEROUS 10-second ceiling to avoid
flaky failures.

Dual threshold summary:
  - 5s  = Manual UAT target (what we expect in practice, verified during UAT)
  - 10s = Automated test ceiling (generous margin for CI variability)

If a test takes >5s but <10s, it passes automation but signals investigation
may be needed. If >10s, something is genuinely wrong.
"""

import os
import sys
import time
import pytest
from pathlib import Path

# Determine project root (tests/ is one level below root)
ROOT_DIR = Path(__file__).resolve().parent.parent

# Module-level skip if Genizah_Index is unavailable
pytestmark = pytest.mark.skipif(
    not os.path.isdir(os.path.join(ROOT_DIR, 'Genizah_Index')),
    reason="Genizah_Index not available -- performance tests require the real Tantivy index"
)


@pytest.fixture(scope="module")
def real_engine():
    """Create a real SearchEngine with real MetadataManager and VariantManager.

    This fixture loads the actual Tantivy index for performance benchmarking.
    Scoped to module so the index is loaded once and reused across all tests.
    """
    # Ensure project root is on sys.path
    root_str = str(ROOT_DIR)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)

    from genizah_core import SearchEngine, MetadataManager, VariantManager

    # Check index directory one more time (belt and suspenders)
    index_path = os.path.join(ROOT_DIR, 'Genizah_Index')
    if not os.path.isdir(index_path):
        pytest.skip("Genizah_Index not available")

    meta_mgr = MetadataManager()
    var_mgr = VariantManager()
    engine = SearchEngine(meta_mgr, var_mgr)

    if engine.searcher is None:
        pytest.skip("Tantivy index failed to load")

    engine.reload_index()
    yield engine


def _format_timing(query, elapsed, result_count):
    """Format timing output with UAT target comparison."""
    if elapsed < 5:
        status = "(WITHIN 5s UAT target)"
    else:
        status = "(ABOVE 5s UAT target, within 10s automated ceiling)"
    return f"Query '{query}': {elapsed:.2f}s, {result_count} results {status}"


class TestPerformanceBenchmarks:
    """Performance benchmarks for Responsa queries on the full corpus.

    Each test uses time.perf_counter() around execute_search() calls.
    The 10-second automated ceiling is generous to avoid flaky failures;
    the 5-second UAT target is what we expect in practice.
    """

    def test_simple_responsa_no_expansions(self, real_engine, capsys):
        """Baseline: Simple Responsa query with all checkboxes OFF.

        Uses Hebrew 'shalom' with responsa_mode=True but no variants, JA, or
        flex spacing. This is the simplest Responsa query path.

        Automated ceiling: 10s. UAT target: <5s.
        """
        query = "\u05e9\u05dc\u05d5\u05dd"  # shalom
        start = time.perf_counter()
        results = real_engine.execute_search(
            query, 'exact', 0,
            responsa_options={
                'responsa_mode': True, 'variants': False, 'ja': False,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'exact'
            }
        )
        elapsed = time.perf_counter() - start

        print(_format_timing(query, elapsed, len(results)))
        # Automated ceiling: 10 seconds (generous for CI variability)
        assert elapsed < 10.0, (
            f"Simple Responsa query took {elapsed:.2f}s, exceeds 10s automated ceiling. "
            f"Returned {len(results)} results."
        )

    def test_responsa_with_prefix_expansion(self, real_engine, capsys):
        """Responsa with prefix expansion: ~25 prefix forms should still be fast.

        Uses Hebrew '#shalom' which triggers grammatical prefix expansion
        (ha-, ve-, u-, be-, le-, she-, ke-, mi-, etc.)

        Automated ceiling: 10s. UAT target: <5s.
        """
        query = "#\u05e9\u05dc\u05d5\u05dd"  # #shalom
        start = time.perf_counter()
        results = real_engine.execute_search(
            query, 'exact', 0,
            responsa_options={
                'responsa_mode': True, 'variants': False, 'ja': False,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'exact'
            }
        )
        elapsed = time.perf_counter() - start

        print(_format_timing(query, elapsed, len(results)))
        # Automated ceiling: 10 seconds
        assert elapsed < 10.0, (
            f"Prefix expansion query took {elapsed:.2f}s, exceeds 10s automated ceiling. "
            f"Returned {len(results)} results."
        )

    def test_responsa_with_variants_and_ja(self, real_engine, capsys):
        """Primary performance criterion: Responsa with variants + JA enabled.

        This produces the most term expansion and is the ROADMAP's primary
        performance criterion: "<5 seconds for Responsa query with variants + JA."

        Automated ceiling: 10s. UAT target: <5s.
        """
        query = "#\u05e9\u05dc\u05d5\u05dd"  # #shalom
        start = time.perf_counter()
        results = real_engine.execute_search(
            query, 'exact', 0,
            responsa_options={
                'responsa_mode': True, 'variants': True, 'ja': True,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'variants'
            }
        )
        elapsed = time.perf_counter() - start

        print(_format_timing(query, elapsed, len(results)))
        # Automated ceiling: 10 seconds
        assert elapsed < 10.0, (
            f"Variants + JA query took {elapsed:.2f}s, exceeds 10s automated ceiling. "
            f"Returned {len(results)} results."
        )

    def test_two_component_query_with_gap(self, real_engine, capsys):
        """Two-component query with gap: '#shalom [3] olam'.

        Tests multi-component query with per-pair gap, variants enabled.

        Automated ceiling: 10s. UAT target: <5s.
        """
        # #shalom [3] olam  (shalom with prefixes, 3-word gap, olam plain)
        query = "#\u05e9\u05dc\u05d5\u05dd [3] \u05e2\u05d5\u05dc\u05dd"
        start = time.perf_counter()
        results = real_engine.execute_search(
            query, 'exact', 1,
            responsa_options={
                'responsa_mode': True, 'variants': True, 'ja': False,
                'flex_spacing': False, 'bidirectional': False,
                'variant_mode': 'variants'
            }
        )
        elapsed = time.perf_counter() - start

        print(_format_timing(query, elapsed, len(results)))
        # Automated ceiling: 10 seconds
        assert elapsed < 10.0, (
            f"Two-component gap query took {elapsed:.2f}s, exceeds 10s automated ceiling. "
            f"Returned {len(results)} results."
        )

    def test_non_responsa_baseline(self, real_engine, capsys):
        """Non-Responsa baseline for comparison: plain exact mode search.

        Same Hebrew word 'shalom' but with plain exact mode (no Responsa).
        This provides a baseline to detect if Responsa adds unreasonable overhead.

        Automated ceiling: 3s (exact mode should be fast).
        """
        query = "\u05e9\u05dc\u05d5\u05dd"  # shalom
        start = time.perf_counter()
        results = real_engine.execute_search(
            query, 'exact', 0,
            responsa_options=None  # No Responsa -- existing path
        )
        elapsed = time.perf_counter() - start

        if elapsed < 3:
            status = "(WITHIN 3s exact-mode target)"
        else:
            status = "(ABOVE 3s exact-mode target)"
        print(f"Query '{query}' (exact, no Responsa): {elapsed:.2f}s, {len(results)} results {status}")

        # Non-Responsa exact search should be fast: 3 second ceiling
        assert elapsed < 3.0, (
            f"Non-Responsa exact search took {elapsed:.2f}s, exceeds 3s ceiling. "
            f"Returned {len(results)} results."
        )

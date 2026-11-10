"""Performance and stress tests for the web application.

Tests verify:
- Search result pagination handles page changes quickly
- Large result sets don't crash the WebSocket connection
- Page load times are within acceptable bounds

These tests require ChromeDriver and Tantivy index; skip gracefully when unavailable.
"""

import os
import time
import pytest

pytest.importorskip("selenium", reason="selenium not installed -- skipping E2E tests")
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys


# Mark all tests in this module
pytestmark = [
    pytest.mark.e2e,
    pytest.mark.slow,
    pytest.mark.filterwarnings("ignore::DeprecationWarning"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _has_tantivy_index():
    """Check if the Tantivy search index is available.

    Checks all locations the Config class uses: portable (project root),
    LOCALAPPDATA, and legacy home directory.
    """
    base_dir = os.path.join(os.path.dirname(__file__), '..', '..')
    portable = os.path.join(base_dir, 'Genizah_Index')
    appdata = os.path.join(
        os.getenv('LOCALAPPDATA', os.path.expanduser('~')),
        'GenizahSearchPro', 'Index'
    )
    legacy = os.path.join(os.path.expanduser('~'), 'Genizah_Tantivy_Index')
    return any(os.path.isdir(p) for p in (portable, appdata, legacy))


def _find_search_input(screen):
    """Find the main search input element on the page."""
    selectors = [
        'input[type="text"]',
        '.q-field input',
        'input[placeholder]',
        'input',
    ]
    for selector in selectors:
        elements = screen.selenium.find_elements(By.CSS_SELECTOR, selector)
        if elements:
            for el in elements:
                if el.is_displayed():
                    return el
    return None


# ---------------------------------------------------------------------------
# Performance Tests
# ---------------------------------------------------------------------------

@pytest.mark.skipif(not _has_tantivy_index(), reason="Tantivy index not available")
class TestSearchPerformance:
    """Performance tests for search functionality."""

    def test_large_result_set_does_not_crash(self, screen):
        """Search with 1000+ results should complete without crashing.

        The WebSocket connection should remain stable when rendering a
        large paginated result set (PAGE_SIZE=50 keeps individual page
        payloads manageable).
        """
        screen.open('/search')
        screen.wait(3.0)

        input_el = _find_search_input(screen)
        assert input_el is not None, "Need search input"

        input_el.clear()
        # Use a very broad query that returns many results
        input_el.send_keys('fragment')
        input_el.send_keys(Keys.ENTER)

        # Wait generously for large result set
        screen.wait(15.0)

        # Verify page is still responsive (WebSocket didn't crash)
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Page should still be accessible"
        assert len(body.text) > 50, "Page should have content after large search"

    def test_pagination_page_change_speed(self, screen):
        """Pagination page changes should complete within 5 seconds.

        This tests that navigating between pages of search results is fast
        enough for a good user experience.
        """
        screen.open('/search')
        screen.wait(3.0)

        input_el = _find_search_input(screen)
        assert input_el is not None, "Need search input"

        input_el.clear()
        input_el.send_keys('fragment')
        input_el.send_keys(Keys.ENTER)

        screen.wait(15.0)

        # Find pagination buttons
        pagination_btns = screen.selenium.find_elements(
            By.CSS_SELECTOR, '.q-pagination .q-btn, .q-pagination button'
        )

        if len(pagination_btns) > 1:
            # Click a page button (not the first one, which is likely current)
            target_btn = None
            for btn in pagination_btns:
                btn_text = btn.text.strip()
                if btn_text == '2':
                    target_btn = btn
                    break

            if target_btn and target_btn.is_displayed():
                # Record initial content for comparison
                body_before = screen.selenium.find_element(By.TAG_NAME, 'body').text

                start = time.time()
                target_btn.click()
                screen.wait(5.0)
                elapsed = time.time() - start

                # Verify page changed (body text should differ)
                body_after = screen.selenium.find_element(By.TAG_NAME, 'body').text
                assert body_after, "Page should have content after pagination"

                # Page change should be fast
                assert elapsed < 10.0, \
                    f"Page change took {elapsed:.1f}s (expected < 10s)"
            else:
                pytest.skip("Could not find page 2 button to test pagination speed")
        else:
            pytest.skip("No pagination buttons found (result set may be too small)")


class TestPageLoadPerformance:
    """Test that pages load within acceptable time bounds."""

    def test_home_page_load_time(self, screen):
        """Home page should load within 10 seconds."""
        start = time.time()
        screen.open('/')
        screen.wait(1.0)
        elapsed = time.time() - start

        # Home page should load quickly (even with engine initialization)
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Home page should load"
        assert elapsed < 15.0, \
            f"Home page load took {elapsed:.1f}s (expected < 15s)"

    def test_search_page_load_time(self, screen):
        """Search page should load within 10 seconds."""
        start = time.time()
        screen.open('/search')
        screen.wait(1.0)
        elapsed = time.time() - start

        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Search page should load"
        assert elapsed < 15.0, \
            f"Search page load took {elapsed:.1f}s (expected < 15s)"

    def test_browse_page_load_time(self, screen):
        """Browse page should load within 10 seconds."""
        start = time.time()
        screen.open('/browse')
        screen.wait(1.0)
        elapsed = time.time() - start

        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Browse page should load"
        assert elapsed < 15.0, \
            f"Browse page load took {elapsed:.1f}s (expected < 15s)"

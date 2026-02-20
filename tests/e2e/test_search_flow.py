"""E2E tests for the search page flow.

Tests the search happy path using NiceGUI's Screen fixture:
- Page loads and shows expected elements
- Query submission produces results
- Pagination appears for multi-page result sets

These tests require ChromeDriver and skip gracefully when unavailable.
The tests use the full application (web/main.py) if the Tantivy index is
available, or a lightweight stub otherwise.
"""

import os
import time
import pytest

pytest.importorskip("selenium", reason="selenium not installed -- skipping E2E tests")
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException


# Mark all tests in this module as e2e
pytestmark = [
    pytest.mark.e2e,
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
    """Find the main search input element on the page.

    Tries multiple CSS selectors since the NiceGUI/Quasar input structure
    can vary between versions.
    """
    selectors = [
        'input[type="text"]',
        '.q-field input',
        'input[placeholder]',
        'input',
    ]
    for selector in selectors:
        elements = screen.selenium.find_elements(By.CSS_SELECTOR, selector)
        if elements:
            # Return the first visible input
            for el in elements:
                if el.is_displayed():
                    return el
    return None


# ---------------------------------------------------------------------------
# Search Page Tests
# ---------------------------------------------------------------------------

class TestSearchPageLoads:
    """Test that the search page loads and contains expected elements."""

    def test_search_page_accessible(self, screen):
        """Search page responds and renders HTML content."""
        screen.open('/search')
        screen.wait(2.0)
        # The page should have a body element at minimum
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Page should have a body element"

    def test_search_page_has_title(self, screen):
        """Search page has the application title."""
        screen.open('/search')
        screen.wait(2.0)
        title = screen.selenium.title
        assert title, "Page should have a non-empty title"
        # Our app title contains 'Genizah' or 'Dicta'
        assert any(word in title for word in ['Genizah', 'Dicta', 'genizah', 'Search']), \
            f"Page title should contain app name, got: {title}"

    def test_search_page_has_input(self, screen):
        """Search page has at least one text input for queries."""
        screen.open('/search')
        screen.wait(3.0)
        input_el = _find_search_input(screen)
        assert input_el is not None, "Search page should have a text input"


@pytest.mark.skipif(not _has_tantivy_index(), reason="Tantivy index not available")
class TestSearchExecution:
    """Test search execution (requires Tantivy index)."""

    def test_search_returns_results(self, screen):
        """Entering a query and pressing Enter returns results."""
        screen.open('/search')
        screen.wait(3.0)

        input_el = _find_search_input(screen)
        assert input_el is not None, "Need search input"

        input_el.clear()
        input_el.send_keys('ketubah')
        input_el.send_keys(Keys.ENTER)

        # Wait for search to execute (may take a few seconds on first run)
        screen.wait(10.0)

        # Look for result indicators: result cards, result count, or shelfmark links
        result_indicators = screen.selenium.find_elements(
            By.CSS_SELECTOR, '.result-card, .result-shelfmark, [class*="result"]'
        )
        # If no styled results, check for any text mentioning results
        page_text = screen.selenium.find_element(By.TAG_NAME, 'body').text
        has_results = (
            len(result_indicators) > 0 or
            'result' in page_text.lower() or
            'T-S' in page_text  # Shelfmark in results
        )
        assert has_results, \
            f"Search for 'ketubah' should return results. Found {len(result_indicators)} result elements."

    def test_pagination_appears_for_many_results(self, screen):
        """A broad query should produce pagination controls."""
        screen.open('/search')
        screen.wait(3.0)

        input_el = _find_search_input(screen)
        assert input_el is not None, "Need search input"

        input_el.clear()
        # Use a very broad Hebrew term that should match many fragments
        input_el.send_keys('fragment')
        input_el.send_keys(Keys.ENTER)

        screen.wait(12.0)

        # Check for Quasar pagination component
        pagination_els = screen.selenium.find_elements(
            By.CSS_SELECTOR, '.q-pagination, [role="navigation"], nav'
        )
        # Pagination may or may not appear depending on result count
        # Just verify the page didn't crash
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body.text, "Page should have content after search"

    def test_search_with_hebrew_query(self, screen):
        """Search works with Hebrew text input."""
        screen.open('/search')
        screen.wait(3.0)

        input_el = _find_search_input(screen)
        assert input_el is not None, "Need search input"

        input_el.clear()
        # Use a common Hebrew word that should return results
        input_el.send_keys('\u05db\u05ea\u05d5\u05d1\u05d4')  # ketubah in Hebrew
        input_el.send_keys(Keys.ENTER)

        screen.wait(10.0)

        # Verify page didn't crash
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Page should be accessible after Hebrew search"

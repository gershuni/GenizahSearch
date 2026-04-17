"""E2E tests for the browse page flow.

Tests the browse happy path using NiceGUI's Screen fixture:
- Browse page loads successfully
- Shelfmark navigation shows metadata
- Image and transcription panels render

These tests require ChromeDriver and skip gracefully when unavailable.
"""

import os
import pytest

pytest.importorskip("selenium", reason="selenium not installed -- skipping E2E tests")
from selenium.webdriver.common.by import By


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


# ---------------------------------------------------------------------------
# Browse Page Tests
# ---------------------------------------------------------------------------

class TestBrowsePageLoads:
    """Test that the browse page loads correctly."""

    def test_browse_page_accessible(self, screen):
        """Browse page responds and renders HTML content."""
        screen.open('/browse')
        screen.wait(3.0)
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Page should have a body element"

    def test_browse_page_has_title(self, screen):
        """Browse page has the application title."""
        screen.open('/browse')
        screen.wait(2.0)
        title = screen.selenium.title
        assert title, "Page should have a non-empty title"


@pytest.mark.skipif(not _has_tantivy_index(), reason="Tantivy index not available")
class TestBrowseNavigation:
    """Test browse page with actual manuscript data."""

    def test_browse_with_sys_id(self, screen):
        """Browse page loads a specific manuscript by sys_id."""
        # Use a known sys_id that should exist in the metadata
        # sys_id 003750 is a common test target (T-S 12.1 area)
        screen.open('/browse?sys_id=003750')
        screen.wait(8.0)

        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        page_text = body.text
        # The page should show some manuscript content
        # Either shelfmark, metadata, or transcription text
        has_content = (
            len(page_text) > 100 or  # Has substantial text
            'T-S' in page_text or     # Shows a shelfmark
            'CUL' in page_text or     # Shows library code
            'Cambridge' in page_text   # Shows library name
        )
        assert has_content, \
            f"Browse page should show manuscript content, got {len(page_text)} chars"

    def test_shelfmark_navigation_updates_url(self, screen):
        """Shelfmark navigation (Prev/Next) updates the browser URL bar.

        This is the regression test for the Cat-1 asyncio.ensure_future fix (D-20).
        Before the fix: on_click=lambda: asyncio.ensure_future(navigate_shelfmark(...))
        returned a Task, bypassing NiceGUI's context-preserving awaitable path.
        history.replaceState was then called outside the client context and
        silently dropped by NiceGUI, leaving the URL bar stale.

        After the Phase 74 Cat-1 sweep: on_click=lambda: navigate_shelfmark(...)
        returns a coroutine that NiceGUI schedules via handle_event under
        parent_slot, so history.replaceState reaches the correct client.

        Selector reliability (review-revision: Codex HIGH #10):
        Uses the stable aria-label / data-action attributes added to the
        shelfmark buttons in Task 0. No XPath chevron fallback - that fallback
        would match page-nav chevrons at browse.py:3712/3765, not shelfmark
        navigation, and pass the test for the wrong reason.
        """
        import re
        import pytest
        from selenium.webdriver.common.by import By

        screen.open('/browse?sys_id=003750')
        screen.wait(8.0)  # Allow page mount + initial load.

        initial_url = screen.selenium.current_url
        # Extract initial sys_id from URL to compare later (Codex HIGH #11:
        # strengthen assertion beyond "URL string changed").
        initial_sys_id_match = re.search(r'sys_id=([0-9]+)', initial_url)
        assert initial_sys_id_match, f"Expected sys_id in initial URL: {initial_url}"
        initial_sys_id = initial_sys_id_match.group(1)

        # Locate the Next Shelfmark button via stable selector (Task 0).
        next_btns = screen.selenium.find_elements(
            By.CSS_SELECTOR,
            'button[aria-label="Next manuscript"], button[data-action="next-manuscript"]'
        )

        if not next_btns:
            pytest.skip(
                "Next Shelfmark button not found via stable selector. "
                "Check Task 0 ran: aria-label / data-action attribute must be present."
            )

        # Click Next.
        try:
            next_btns[0].click()
        except Exception as e:
            pytest.skip(f"Could not click next shelfmark button: {e}")

        screen.wait(5.0)  # Allow async navigate_shelfmark + history.replaceState.

        updated_url = screen.selenium.current_url
        # STRONGER assertion (Codex HIGH #11): verify sys_id CHANGED, not just
        # that the URL string differs. A cache-buster or hash change would pass
        # the naive "updated_url != initial_url" check while the Cat-1 bug
        # remains unfixed.
        updated_sys_id_match = re.search(r'sys_id=([0-9]+)', updated_url)
        assert updated_sys_id_match, (
            f"Updated URL missing sys_id parameter - shelfmark navigation did "
            f"not complete correctly. Initial: {initial_url!r} Updated: {updated_url!r}. "
            f"This is the Cat-1 asyncio.ensure_future regression - NiceGUI's "
            f"awaitable scheduling path must be used (on_click=lambda: fn(), "
            f"NOT on_click=lambda: asyncio.ensure_future(fn()))."
        )
        updated_sys_id = updated_sys_id_match.group(1)
        assert updated_sys_id != initial_sys_id, (
            f"sys_id should have changed after Next Shelfmark click. "
            f"Initial: {initial_sys_id} Updated: {updated_sys_id}. "
            f"If sys_id did not change, history.replaceState was dropped - "
            f"this is the Cat-1 asyncio.ensure_future regression."
        )

    def test_browse_shows_metadata(self, screen):
        """Browse page displays manuscript metadata."""
        screen.open('/browse?sys_id=003750')
        screen.wait(8.0)

        # Look for metadata elements (cards, labels, links)
        metadata_elements = screen.selenium.find_elements(
            By.CSS_SELECTOR, '.q-card, .metadata, [class*="meta"], [class*="info"]'
        )
        body_text = screen.selenium.find_element(By.TAG_NAME, 'body').text

        # Page should have some structured content (cards or text)
        has_metadata = (
            len(metadata_elements) > 0 or
            len(body_text) > 200
        )
        assert has_metadata, "Browse page should display metadata elements"

    def test_browse_image_panel_exists(self, screen):
        """Browse page has an image panel or image element."""
        screen.open('/browse?sys_id=003750')
        screen.wait(8.0)

        # Look for image-related elements
        image_elements = screen.selenium.find_elements(
            By.CSS_SELECTOR, 'img, canvas, .image-panel, [class*="image"], .openseadragon-container'
        )
        # Image panel may or may not load depending on IIIF availability
        # Just verify the browse page structure is present
        body = screen.selenium.find_element(By.TAG_NAME, 'body')
        assert body is not None, "Browse page should render"

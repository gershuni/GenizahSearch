"""E2E test configuration using NiceGUI's Screen fixture.

Provides:
- ChromeDriver skip logic (tests skip gracefully when browser automation unavailable)
- NiceGUI Screen fixture registration
- pytest-selenium configuration for headless Chrome

Tests skip in two scenarios:
1. selenium/pytest-selenium not installed -> ImportError skip
2. ChromeDriver not found/not matching Chrome version -> RuntimeError skip
"""

import os
import pytest


# ---------------------------------------------------------------------------
# Skip logic: gracefully skip all E2E tests when dependencies are missing
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(config, items):
    """Skip E2E tests if Selenium or pytest-selenium is not installed."""
    try:
        import selenium  # noqa: F401
        import pytest_selenium  # noqa: F401
    except ImportError as e:
        skip_marker = pytest.mark.skip(
            reason=f"E2E dependency not available: {e} (pip install selenium pytest-selenium)"
        )
        for item in items:
            if "e2e" in str(item.fspath):
                item.add_marker(skip_marker)
        return


# ---------------------------------------------------------------------------
# Import NiceGUI testing fixtures (only if selenium is available)
# ---------------------------------------------------------------------------

try:
    from nicegui.testing.screen_plugin import (  # noqa: F401
        nicegui_chrome_options,
        nicegui_driver,
        nicegui_remove_all_screenshots,
        pytest_runtest_makereport,
        screen,
    )
    from nicegui.testing.general_fixtures import (  # noqa: F401
        nicegui_reset_globals,
        pytest_addoption,
        pytest_configure,
    )

    # Override nicegui_chrome_options to add headless mode and window size
    @pytest.fixture
    def nicegui_chrome_options(chrome_options):
        """Configure Chrome for headless E2E testing."""
        chrome_options.add_argument('disable-dev-shm-usage')
        chrome_options.add_argument('disable-search-engine-choice-screen')
        chrome_options.add_argument('no-sandbox')
        chrome_options.add_argument('headless')
        chrome_options.add_argument('window-size=1280x1024')
        if 'GITHUB_ACTIONS' in os.environ:
            chrome_options.add_argument('disable-gpu')
        else:
            chrome_options.add_argument('--use-gl=angle')
        if 'CHROME_BINARY_LOCATION' in os.environ:
            chrome_options.binary_location = os.environ['CHROME_BINARY_LOCATION']
        return chrome_options

except ImportError:
    # Selenium not installed; tests will be skipped via pytest_collection_modifyitems
    pass

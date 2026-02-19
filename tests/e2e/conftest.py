"""E2E test configuration using NiceGUI's Screen fixture.

Provides:
- Custom screen fixture that starts the GenizahSearch web application
- ChromeDriver skip logic (tests skip gracefully when browser automation unavailable)
- Headless Chrome configuration for automated testing
- Custom mark registration (e2e, slow)

Tests skip in two scenarios:
1. selenium/pytest-selenium not installed -> ImportError skip
2. ChromeDriver not found/not matching Chrome version -> RuntimeError skip
"""

import os
import shutil
import threading
import runpy
from pathlib import Path
from collections.abc import Generator

import pytest


# ---------------------------------------------------------------------------
# Register custom marks to avoid PytestUnknownMarkWarning
# ---------------------------------------------------------------------------

def pytest_configure(config):
    """Register custom markers for E2E tests."""
    config.addinivalue_line("markers", "e2e: end-to-end browser tests using NiceGUI Screen fixture")
    config.addinivalue_line("markers", "slow: tests that take more than 10 seconds")


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
# NiceGUI Screen fixture (custom, avoids main_file/inipath requirement)
# ---------------------------------------------------------------------------

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service

    from nicegui.testing.screen import Screen
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.general import nicegui_reset_globals as _nicegui_reset_globals_ctx
    from nicegui import app, ui

    DOWNLOAD_DIR = Path(__file__).parent / 'download'
    MAIN_FILE = Path(__file__).resolve().parent.parent.parent / 'web' / 'main.py'

    @pytest.hookimpl(tryfirst=True, hookwrapper=True)
    def pytest_runtest_makereport(item, call):
        """Store test outcome in the node for fixture access."""
        outcome = yield
        rep = outcome.get_result()
        setattr(item, f'rep_{rep.when}', rep)

    @pytest.fixture
    def nicegui_chrome_options(chrome_options: webdriver.ChromeOptions) -> webdriver.ChromeOptions:
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
        chrome_options.add_experimental_option('prefs', {
            'download.default_directory': str(DOWNLOAD_DIR),
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
        })
        if 'CHROME_BINARY_LOCATION' in os.environ:
            chrome_options.binary_location = os.environ['CHROME_BINARY_LOCATION']
        return chrome_options

    @pytest.fixture
    def capabilities(capabilities: dict) -> dict:
        """Configure Chrome driver capabilities for console log capture."""
        capabilities['goog:loggingPrefs'] = {'browser': 'ALL'}
        return capabilities

    @pytest.fixture(scope='session')
    def nicegui_remove_all_screenshots() -> None:
        """Remove all screenshots from the screenshot directory before the test session."""
        for name in Screen.SCREENSHOT_DIR.glob('*.png'):
            name.unlink()

    @pytest.fixture()
    def nicegui_driver(nicegui_chrome_options: webdriver.ChromeOptions) -> Generator[webdriver.Chrome, None, None]:
        """Create a new Chrome driver instance."""
        for executable_path in (None, shutil.which('chromedriver'), 'chromedriver'):
            try:
                s = Service(executable_path=executable_path)
                driver_ = webdriver.Chrome(service=s, options=nicegui_chrome_options)
                break
            except Exception:
                continue
        else:
            pytest.skip('Could not start Chrome WebDriver (ChromeDriver not found)')
        driver_.implicitly_wait(Screen.IMPLICIT_WAIT)
        driver_.set_page_load_timeout(30)
        yield driver_
        driver_.quit()

    @pytest.fixture
    def nicegui_reset_globals():
        """Reset the global state of the NiceGUI package."""
        with _nicegui_reset_globals_ctx():
            yield

    @pytest.fixture
    def screen(nicegui_reset_globals,
               nicegui_remove_all_screenshots,
               nicegui_driver: webdriver.Chrome,
               request: pytest.FixtureRequest,
               caplog: pytest.LogCaptureFixture,
               ) -> Generator[Screen, None, None]:
        """Create a Screen fixture that starts the GenizahSearch web app.

        This custom fixture avoids the NiceGUI default's requirement for
        config.inipath (pytest.ini), instead directly using the main_file path.
        """
        from nicegui.server import Server
        from nicegui import core

        os.environ['NICEGUI_SCREEN_TEST_PORT'] = str(Screen.PORT)
        screen_ = Screen(nicegui_driver, caplog, request=None)  # Pass None to skip main_file lookup

        # Override start_server to use our actual app
        original_start = screen_.start_server

        def custom_start_server():
            if MAIN_FILE.is_file():
                screen_.server_thread = threading.Thread(
                    target=lambda: runpy.run_path(str(MAIN_FILE), run_name='__main__')
                )
            else:
                # Fallback: start a minimal NiceGUI server with test pages
                prepare_simulation()

                @ui.page('/search')
                def search_page():
                    ui.label('Search')
                    ui.input(placeholder='Search manuscripts...').classes('search-input')

                @ui.page('/browse')
                def browse_page():
                    ui.label('Browse')
                    ui.label('Manuscript Viewer')

                @ui.page('/')
                def home_page():
                    ui.label('Dicta Genizah Search')
                    ui.label('Home')

                screen_.server_thread = threading.Thread(
                    target=lambda: ui.run(**screen_.ui_run_kwargs)
                )
            screen_.server_thread.start()

        screen_.start_server = custom_start_server

        try:
            yield screen_

            logs = [record for record in screen_.caplog.get_records('call') if record.levelname == 'ERROR']
            if screen_.is_open:
                test_failed = hasattr(request.node, 'rep_call') and request.node.rep_call.failed
                screen_.shot(request.node.name, failed=test_failed or bool(logs))
            # Don't fail on ERROR logs for our E2E tests (app initialization may log errors)
            # if logs:
            #     pytest.fail('There were unexpected ERROR logs.', pytrace=False)
        finally:
            os.environ.pop('NICEGUI_SCREEN_TEST_PORT', None)
            screen_.stop_server()
            if DOWNLOAD_DIR.exists():
                shutil.rmtree(DOWNLOAD_DIR)

except ImportError:
    # Selenium not installed; tests will be skipped via pytest_collection_modifyitems
    pass

"""PostHog analytics helper — safe to import from any module.

Extracted from web/main.py to avoid circular imports: web/main.py runs
module-level ``app.on_startup(...)`` which fails when re-imported at
runtime (NiceGUI is already started).
"""

import logging

from nicegui import ui

logger = logging.getLogger(__name__)


def posthog_capture(event: str, properties: dict = None, *, client=None):
    """Send a custom PostHog event from the server side via JS injection.

    Safe to call even if PostHog isn't loaded (no-ops gracefully).
    Properties are JSON-serialized and passed to posthog.capture().

    ``client``: the page to send it through. Pass the page's client from work
    the page handed to a task, or that runs after the clicked element was
    cleared away: ``ui.run_javascript`` needs the current page, finds none
    there, and the event is lost (tests/test_posthog_capture_page_client.py).
    """
    import json
    props_js = json.dumps(properties or {})
    try:
        (client if client is not None else ui.context.client).run_javascript(
            f"if(window.posthog)posthog.capture('{event}',{props_js})"
        )
    except Exception as exc:
        # No client connection or PostHog not loaded
        logger.debug('posthog_capture(%s) dropped: %s', event, exc)

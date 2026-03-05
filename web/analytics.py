"""PostHog analytics helper — safe to import from any module.

Extracted from web/main.py to avoid circular imports: web/main.py runs
module-level ``app.on_startup(...)`` which fails when re-imported at
runtime (NiceGUI is already started).
"""

from nicegui import ui


def posthog_capture(event: str, properties: dict = None):
    """Send a custom PostHog event from the server side via JS injection.

    Safe to call even if PostHog isn't loaded (no-ops gracefully).
    Properties are JSON-serialized and passed to posthog.capture().
    """
    import json
    props_js = json.dumps(properties or {})
    try:
        ui.run_javascript(
            f"if(window.posthog)posthog.capture('{event}',{props_js})"
        )
    except Exception:
        pass  # No client connection or PostHog not loaded

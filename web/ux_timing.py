"""User-perceived latency, measured in the visitor's browser (PostHog ``ux_timing``).

$web_vitals sees only the first page load: in-page NiceGUI updates (a Browse page
turn, a search's first rows, a save) are websocket round trips it never times.
Here the browser marks the user's action (a click or Enter on an element tagged
``data-ux="<workflow>"``), the server reports when the update is sent, and
web/static/js/ux_timing.js measures to the next paint, so one event carries
total = transport + server + render, with the page image's load time apart.

``ux_done`` must be given the page's own client: the work it reports usually
runs in a task the page started, or after the clicked element was cleared away,
where ``ui.context`` has no page (tests/test_posthog_capture_page_client.py).

Env: UX_TIMING_ENABLED (default on; '0' turns it off) and UX_TIMING_SAMPLE_RATE
(share of page loads that send events, default 1.0). Nothing is sent without
POSTHOG_API_KEY. See docs/guides/ENV_VARS.md.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

#: The workflows measured. A trigger element carries ``data-ux=<one of these>``.
WORKFLOWS = frozenset({
    'browse_page',        # a Browse page turn, or the first page of a /browse load
    'result_quick_view',  # a search result opened in the Quick View dialog
    'search_results',     # a regular search, to its first visible rows
    'save_list_add',      # add to a list
    'save_comment',       # post a comment
})
OUTCOMES = frozenset({'ok', 'preview', 'partial', 'zero', 'error', 'duplicate'})

_JS_PATH = Path(__file__).resolve().parent / 'static' / 'js' / 'ux_timing.js'
_EVENT_VERSION = 1


def enabled() -> bool:
    return (bool(os.environ.get('POSTHOG_API_KEY'))
            and os.environ.get('UX_TIMING_ENABLED', '1').strip().lower() not in ('0', 'false', 'no', 'off'))


def sample_rate() -> float:
    try:
        rate = float(os.environ.get('UX_TIMING_SAMPLE_RATE', '1.0'))
    except ValueError:
        return 1.0
    return min(max(rate, 0.0), 1.0)


def pending_wait_ms() -> dict:
    """How long the browser keeps an action's start, where 60 s is too short.

    A website search may run to Config.WEB_SEARCH_TIME_LIMIT before its first
    rows; dropping those would leave exactly the slow searches unmeasured.
    """
    from shared.config import Config
    return {'search_results': (int(Config.WEB_SEARCH_TIME_LIMIT) + 30) * 1000}


def head_html() -> str:
    """The <head> snippet that loads the browser side, or '' when switched off."""
    if not enabled():
        return ''
    try:
        version = hashlib.sha256(_JS_PATH.read_bytes()).hexdigest()[:10]
    except OSError:
        logger.warning('ux_timing: %s is missing; not measuring', _JS_PATH)
        return ''
    config = json.dumps({'rate': sample_rate(), 'v': _EVENT_VERSION, 'wait': pending_wait_ms()})
    return (f'<script>window.GS_UX={config};</script>\n'
            f'<script src="/static/js/ux_timing.js?v={version}" defer></script>\n')


def ux_done(client, workflow: str, outcome: str, *, server_ms: float | None = None,
            nav: bool = False, img_selector: str | None = None) -> None:
    """Tell ``client``'s browser that ``workflow`` has just finished on the server.

    ``server_ms``: the server's own time for it. ``nav``: the work was started by
    the page's URL, not by a click, so the browser measures from the start of the
    navigation. ``img_selector``: an image whose load is reported separately.
    Never raises: measuring must not break a page.
    """
    if workflow not in WORKFLOWS or outcome not in OUTCOMES:
        logger.warning('ux_timing: unknown workflow/outcome %r/%r; not sent', workflow, outcome)
        return
    if client is None or not enabled():
        return
    args = [workflow, outcome, None if server_ms is None else round(server_ms, 1),
            bool(nav), img_selector]
    try:
        client.run_javascript('window.gsUx&&gsUx.done(' + ','.join(json.dumps(a) for a in args) + ')')
    except Exception as exc:  # the page is gone
        logger.debug('ux_timing(%s) not sent: %s', workflow, exc)


def ux_ignored(client, workflow: str) -> None:
    """Tell ``client``'s browser that the server turned the latest ``workflow``
    action away (a second Enter while a search runs), so the measurement keeps
    the start of the action still in progress. Never raises."""
    if workflow not in WORKFLOWS or client is None or not enabled():
        return
    try:
        client.run_javascript('window.gsUx&&gsUx.ignored&&gsUx.ignored(' + json.dumps(workflow) + ')')
    except Exception as exc:  # the page is gone
        logger.debug('ux_timing(%s) ignored not sent: %s', workflow, exc)
